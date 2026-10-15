<?php

declare(strict_types=1);

namespace RocketMQ\Remoting;

use RocketMQ\Client\Exceptions\RemotingCommandException;
use RocketMQ\Client\Exceptions\RemotingConnectException;
use RocketMQ\Client\Exceptions\RemotingSendRequestException;
use RocketMQ\Client\Exceptions\RemotingTimeoutException;
use RocketMQ\Client\Logger;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;

/**
 * Persistent-socket client: remoting frame send/receive with the NameServer / broker.
 *
 * Provides: connection management (lazy connect + reuse + close recycling), invokeSync /
 * invokeAsync / invokeOneway, opaque-mapped callback dispatch, timeout control, GO_AWAY
 * reconnect-and-resend, and immediate failure of in-flight requests on a broken connection
 * (failFast).
 *
 * PHP has no threads (see php/PORTING.md for the async model):
 *  - ``invokeAsync`` = register the pending table (opaque -> PendingResponse) + non-blocking
 *    write, returning immediately;
 *  - ``waitResponses(timeout)`` pumps responses with ``stream_select``, dispatches callbacks and
 *    sweeps timeouts;
 *  - ``invokeSync`` pumps internally until done (same frame-receive path).
 */
final class RemotingClient
{
    public const MAX_FRAME_LENGTH = 16 * 1024 * 1024;

    private int $connectTimeoutMillis;
    private int $invokeTimeoutMillis;

    /** Defaults to **true**: on broker ResponseCode.GO_AWAY(1500), resend once over a new
     * connection; when disabled, GO_AWAY is reported directly as a send failure. */
    private bool $enableReconnectForGoAway;

    /** Whether to connect to the broker over TLS. The explicit constructor argument wins,
     * otherwise the ROCKETMQ_TLS_ENABLE environment variable is read (this port has no runtime
     * system properties; env is the only switch entry). */
    private bool $tlsEnable;

    /**
     * TLS details (all keys optional; values are local file paths):
     *   - caCert:     CA certificate path. When given, the broker certificate chain and hostname
     *                 are **truly verified** (strict mode); when absent, self-signed certs are
     *                 trusted (permissive mode, the default).
     *   - clientCert: client certificate path (required when the broker enables mutual TLS).
     *   - clientKey:  client private key path (may be omitted when the cert embeds the key).
     *   - serverName: SNI/hostname verification override (defaults to the host of the connect address).
     *
     * @var array{caCert?:string, clientCert?:string, clientKey?:string, serverName?:string}
     */
    private array $tlsOptions;

    private ?SocksProxyConfig $socksProxy = null;

    private bool $closed = false;

    /** @var array<string, resource> addr -> connection */
    private array $conns = [];

    /** @var array<string, string> addr -> receive buffer holding an incomplete frame */
    private array $inBuffers = [];

    /** @var array<int, PendingResponse> opaque -> in-flight request */
    private array $pending = [];

    /** @var array<int, callable(RemotingCommand, string): ?RemotingCommand> broker-initiated request handlers */
    private array $processors = [];

    /** @var list<RPCHook> */
    private array $rpcHooks = [];

    /** @var list<string> namesrv address list (round-robin / failover) */
    private array $nameServerList = [];
    private int $nameServerIndex = 0;

    /**
     * @param list<string>|null $nameServers initial namesrv addresses (may also be set later via updateNameServerAddressList)
     */
    public function __construct(
        int $connectTimeoutMillis = 3000,
        int $invokeTimeoutMillis = 15000,
        ?bool $tlsEnable = null,
        bool $enableReconnectForGoAway = true,
        ?array $nameServers = null,
        ?array $tlsOptions = null,
        ?SocksProxyConfig $socksProxy = null,
    ) {
        $this->connectTimeoutMillis = $connectTimeoutMillis;
        $this->invokeTimeoutMillis = $invokeTimeoutMillis;
        $this->enableReconnectForGoAway = $enableReconnectForGoAway;
        if ($tlsEnable === null) {
            $tlsEnable = in_array(strtolower(trim((string)getenv('ROCKETMQ_TLS_ENABLE'))), ['1', 'true', 'yes'], true);
        }
        $this->tlsEnable = (bool)$tlsEnable;
        $this->tlsOptions = $tlsOptions ?? [];
        $this->socksProxy = $socksProxy;
        if ($nameServers !== null) {
            $this->nameServerList = array_values($nameServers);
        }
    }

    /** Monotonic clock in milliseconds, dedicated to timeout decisions: immune to system clock
     * steps, steadier than wall clock. */
    public static function monoMillis(): float
    {
        return hrtime(true) / 1e6;
    }

    // ---------- Connection management ----------

    /** Sets/clears the SOCKS proxy (null clears). Takes effect only on the **next connect**;
     * existing connections are unaffected. */
    public function setSocksProxy(?SocksProxyConfig $proxy): void
    {
        $this->socksProxy = $proxy;
    }

    public function getSocksProxy(): ?SocksProxyConfig
    {
        return $this->socksProxy;
    }

    /**
     * @return resource
     */
    private function getOrCreateConn(string $addr)
    {
        $conn = $this->conns[$addr] ?? null;
        if ($conn !== null && is_resource($conn)) {
            return $conn;
        }
        $conn = $this->createConn($addr);
        $this->conns[$addr] = $conn;
        $this->inBuffers[$addr] = '';
        return $conn;
    }

    /**
     * @return resource
     */
    private function createConn(string $addr)
    {
        [$host, $port] = self::parseAddr($addr);
        $ctx = stream_context_create([
            'ssl' => [
                // Defaults for the connect phase (permissive mode): trust the broker's
                // self-signed certificate, skip hostname verification, present no client
                // certificate (works out of the box against a PERMISSIVE broker).
                'verify_peer' => false,
                'verify_peer_name' => false,
                'allow_self_signed' => true,
            ],
        ]);
        $timeoutSec = $this->connectTimeoutMillis / 1000.0;
        $proxy = $this->socksProxy;
        if ($proxy !== null && $proxy->isConfigured()) {
            [$pHost, $pPort] = self::parseAddr((string)$proxy->getAddr());
            $conn = @stream_socket_client(
                sprintf('tcp://%s:%s', $pHost, $pPort),
                $errno,
                $errstr,
                $timeoutSec,
                STREAM_CLIENT_CONNECT,
                $ctx
            );
            if ($conn === false) {
                throw new RemotingConnectException(sprintf('%s (socks proxy %s)', $addr, $proxy->getAddr()));
            }
            try {
                SocksProxyConnector::handshake($conn, $host, (int)$port, $proxy, $timeoutSec);
            } catch (\Throwable $e) {
                if (is_resource($conn)) {
                    @fclose($conn);
                }
                throw new RemotingConnectException(sprintf('%s (socks proxy %s: %s)', $addr, $proxy->getAddr(), $e->getMessage()));
            }
        } else {
            $conn = @stream_socket_client(
                sprintf('tcp://%s:%s', $host, $port),
                $errno,
                $errstr,
                $timeoutSec,
                STREAM_CLIENT_CONNECT,
                $ctx
            );
            if ($conn === false) {
                throw new RemotingConnectException($addr);
            }
        }
        if ($this->tlsEnable) {
            // TLS must wrap the entire stream and complete its handshake before any RocketMQ
            // frame (the broker side lives at the same layer). We build the plain socket first,
            // then enable_crypto; the frame format is unchanged after the handshake.
            $caCert = $this->tlsOptions['caCert'] ?? null;
            if ($caCert !== null && $caCert !== '') {
                // With a CA => strict mode, real verification: certificate chain + hostname.
                stream_context_set_option($conn, 'ssl', 'verify_peer', true);
                stream_context_set_option($conn, 'ssl', 'verify_peer_name', true);
                stream_context_set_option($conn, 'ssl', 'allow_self_signed', false);
                stream_context_set_option($conn, 'ssl', 'cafile', $caCert);
                $serverName = $this->tlsOptions['serverName'] ?? null;
                if ($serverName !== null && $serverName !== '') {
                    stream_context_set_option($conn, 'ssl', 'peer_name', $serverName);
                }
            } else {
                // Permissive mode (the default): trust the broker's self-signed certificate,
                // skip hostname verification, present no client certificate (works out of the
                // box against a PERMISSIVE broker).
                stream_context_set_option($conn, 'ssl', 'verify_peer', false);
                stream_context_set_option($conn, 'ssl', 'verify_peer_name', false);
                stream_context_set_option($conn, 'ssl', 'allow_self_signed', true);
            }
            $clientCert = $this->tlsOptions['clientCert'] ?? null;
            if ($clientCert !== null && $clientCert !== '') {
                // mTLS: a broker with mutual authentication enabled demands a client certificate
                // during the handshake; missing it fails the handshake outright.
                stream_context_set_option($conn, 'ssl', 'local_cert', $clientCert);
                $clientKey = $this->tlsOptions['clientKey'] ?? null;
                if ($clientKey !== null && $clientKey !== '') {
                    stream_context_set_option($conn, 'ssl', 'local_pk', $clientKey);
                }
            }
            $crypto = @stream_socket_enable_crypto($conn, true, STREAM_CRYPTO_METHOD_TLS_CLIENT);
            if ($crypto !== true) {
                $reason = error_get_last()['message'] ?? 'unknown error';
                fclose($conn);
                throw new RemotingConnectException(sprintf('%s (tls handshake: %s)', $addr, $reason));
            }
        }
        stream_set_blocking($conn, false);
        // TCP_NODELAY (best effort: set only when ext-sockets is available)
        if (function_exists('socket_set_option') && defined('IPPROTO_TCP') && defined('TCP_NODELAY')) {
            @socket_set_option($conn, IPPROTO_TCP, TCP_NODELAY, 1);
        }
        return $conn;
    }

    /** @return array{0: string, 1: string} [host, port] (compatible with IPv6 literals ``[::1]:9876``) */
    public static function parseAddr(string $addr): array
    {
        if (str_starts_with($addr, '[')) {
            $rest = substr($addr, 1);
            $pos = strpos($rest, ']');
            if ($pos !== false) {
                $host = substr($rest, 0, $pos);
                $port = ltrim(substr($rest, $pos), ']:');
                return [$host, $port];
            }
        }
        $pos = strrpos($addr, ':');
        if ($pos === false) {
            return [$addr, ''];
        }
        return [substr($addr, 0, $pos), substr($addr, $pos + 1)];
    }

    /**
     * Tears down the connection to addr: remove it from the connection table first, then close
     * the socket. In-flight requests fail immediately as send failures (PHP has no dedicated read
     * thread, so failFast hangs off the close point).
     */
    public function closeChannel(string $addr): void
    {
        $conn = $this->conns[$addr] ?? null;
        if ($conn === null) {
            return;
        }
        unset($this->conns[$addr], $this->inBuffers[$addr]);
        $this->failFast($addr, $conn);
        if (is_resource($conn)) {
            @fclose($conn);
        }
    }

    public function isChannelWritable(string $addr): bool
    {
        $conn = $this->conns[$addr] ?? null;
        return $conn !== null && is_resource($conn);
    }

    // ---------- RPC hooks ----------

    /**
     * Runs the RPC hooks in order before sending.
     *
     * **Must be called before cmd.encode()**: the ACL hook writes AccessKey/Signature into
     * ext_fields, and Signature covers the full ext_fields + body after makeCustomHeaderToNet()
     * — i.e. the exact content that goes on the wire. Injecting after encode() is too late.
     */
    private function applyBeforeRequestHooks(string $addr, RemotingCommand $cmd): void
    {
        foreach ($this->rpcHooks as $hook) {
            $hook->doBeforeRequest($addr, $cmd);
        }
    }

    private function applyAfterResponseHooks(string $addr, ?RemotingCommand $request, ?RemotingCommand $response): void
    {
        if ($this->rpcHooks === [] || $request === null) {
            return;
        }
        foreach ($this->rpcHooks as $hook) {
            try {
                $hook->doAfterResponse($addr, $request, $response);
            } catch (\Throwable) {
                // Ignore hook exceptions; they must not affect response dispatch
            }
        }
    }

    public function registerRpcHook(RPCHook $hook): void
    {
        $this->rpcHooks[] = $hook;
    }

    public function unregisterRpcHook(RPCHook $hook): void
    {
        foreach ($this->rpcHooks as $i => $h) {
            if ($h === $hook) {
                unset($this->rpcHooks[$i]);
            }
        }
        $this->rpcHooks = array_values($this->rpcHooks);
    }

    // ---------- Request sending ----------

    /**
     * Writes the command onto the connection (hooks -> encode -> full write).
     * Returns the connection resource used (for bookkeeping: failFast claims in-flight requests
     * by resource identity).
     *
     * @return resource
     */
    private function send(string $addr, RemotingCommand $cmd)
    {
        $this->applyBeforeRequestHooks($addr, $cmd);
        $conn = $this->getOrCreateConn($addr);
        $data = $cmd->encode();
        try {
            $this->writeAll($conn, $data, $this->invokeTimeoutMillis / 1000.0);
        } catch (\Throwable $e) {
            $this->closeChannel($addr);
            throw new RemotingSendRequestException($addr, $e->getMessage());
        }
        return $conn;
    }

    private function writeAll($conn, string $data, float $budgetSec): void
    {
        $total = strlen($data);
        $written = 0;
        $deadline = self::monoMillis() + $budgetSec * 1000.0;
        while ($written < $total) {
            $now = self::monoMillis();
            if ($now >= $deadline) {
                throw new \RuntimeException('write timeout');
            }
            $r = [];
            $w = [$conn];
            $e = [];
            $remainingUs = (int)min(($deadline - $now) * 1000.0, 500000.0);
            $n = @stream_select($r, $w, $e, 0, $remainingUs);
            if ($n === false) {
                throw new \RuntimeException('stream_select failed on write');
            }
            if ($n === 0 || $w === []) {
                continue;
            }
            $chunk = @fwrite($conn, substr($data, $written));
            if ($chunk === false || $chunk === 0) {
                // On non-blocking sockets 0 bytes may be a momentarily full buffer; the deadline is the backstop
                if (feof($conn)) {
                    throw new \RuntimeException('connection closed on write');
                }
                continue;
            }
            $written += $chunk;
        }
    }

    /**
     * Writes a response back to the broker.
     *
     * Must **not** go through send(): send() runs the RPC hooks (ACL signs requests), and a
     * response must not and does not carry a signature. Write failures are only logged: this
     * response corresponds to a broker-side ``Broker2Client.callClient`` timeout, and the frame
     * receive path must not be broken because of it.
     */
    private function writeResponse(string $addr, RemotingCommand $cmd): void
    {
        try {
            $conn = $this->getOrCreateConn($addr);
            $this->writeAll($conn, $cmd->encode(), $this->invokeTimeoutMillis / 1000.0);
        } catch (\Throwable $e) {
            Logger::warning(sprintf('remoting: failed to write response (code=%s) to %s: %s', $cmd->code, $addr, $e->getMessage()));
        }
    }

    // ---------- Synchronous RPC ----------

    /** Synchronous RPC: wait for the response, judge timeout, including GO_AWAY reconnect-and-resend. */
    public function invokeSync(string $addr, RemotingCommand $request, ?int $timeoutMillis = null): RemotingCommand
    {
        $timeout = $timeoutMillis ?? $this->invokeTimeoutMillis;
        $started = self::monoMillis();
        $response = $this->invokeOnce($addr, $request, $timeout);
        if ($response->code !== ResponseCode::GO_AWAY) {
            return $response;
        }
        return $this->handleGoAway($addr, $request, $timeout, $started, $response);
    }

    /**
     * The GO_AWAY settlement: with the switch off, error out directly; otherwise reconnect and
     * resend once — a second GO_AWAY throws. The sync and async paths share the same rule, and
     * the resend count must match too (once).
     */
    private function handleGoAway(string $addr, RemotingCommand $request, int $timeout, float $started, RemotingCommand $response): RemotingCommand
    {
        if (!$this->enableReconnectForGoAway) {
            throw new RemotingSendRequestException($addr, 'Receive GO_AWAY from channel ' . $addr);
        }
        Logger::info("remoting: receive GO_AWAY from {$addr}, reconnect and retry once");
        $this->closeChannel($addr);
        // The resend only spends the remaining budget: the total timeout is counted from before
        // the first send, and one resend must not double the budget.
        $spent = (int)(self::monoMillis() - $started);
        $retryTimeout = max(1, $timeout - $spent);
        $retry = self::retryRequest($request);
        $response = $this->invokeOnce($addr, $retry, $retryTimeout);
        if ($response->code === ResponseCode::GO_AWAY) {
            throw new RemotingSendRequestException($addr, 'Receive GO_AWAY twice in request from channel ' . $addr);
        }
        return $response;
    }

    /**
     * A copy of the request for GO_AWAY resend: same code / header / body, only the opaque changes.
     *
     * The opaque must be **new**: the old entry was already removed from the in-flight table, and
     * reusing it would misroute the response to another request.
     * ext_fields is copied by value — the ACL signature (AccessKey/Signature) lives there, and
     * the resend must carry the original signature, so the copy must not be re-derived from
     * custom_header (deriving loses those two entries).
     */
    private static function retryRequest(RemotingCommand $request): RemotingCommand
    {
        $retry = new RemotingCommand(code: $request->code, customHeader: $request->customHeader);
        $retry->language = $request->language;
        $retry->version = $request->version;
        $retry->flag = $request->flag;
        $retry->remark = $request->remark;
        $retry->extFields = $request->extFields;
        $retry->body = $request->body;
        $retry->serializeTypeCurrentRpc = $request->serializeTypeCurrentRpc;
        $retry->cachedHeader = $request->cachedHeader;
        return $retry;
    }

    private function invokeOnce(string $addr, RemotingCommand $request, int $timeout): RemotingCommand
    {
        $future = new PendingResponse($request->opaque, $timeout, null, null, $addr, $request);
        $this->pending[$request->opaque] = $future;
        try {
            $future->conn = $this->send($addr, $request);
        } catch (\Throwable $e) {
            unset($this->pending[$request->opaque]);
            throw $e;
        }
        $this->pumpForFuture($future, $timeout);
        if ($future->response !== null) {
            return $future->response;
        }
        unset($this->pending[$request->opaque]);
        // When the connection dropped, failFast already set the cause and woke me: settle here
        // as a "send failure" instead of waiting out the full timeout and reporting a timeout.
        // The two paths differ in retryability (see the producer's retry classification).
        if ($future->cause !== null) {
            throw $future->cause;
        }
        throw new RemotingTimeoutException($addr, $timeout);
    }

    // ---------- Asynchronous RPC ----------

    /**
     * Asynchronous invocation: register the pending table + non-blocking write, then **return
     * immediately** (see php/PORTING.md for the async model). The caller pumps with
     * waitResponses() afterwards; Producer's asyncSend pumps internally to completion after the
     * callbacks are registered.
     *
     * Callback contract (success / failure dichotomy, mutually exclusive):
     *  - response received normally -> ``onSuccess(response)``
     *  - no response past timeout -> ``onFailure(RemotingTimeoutException)``
     *  - connection broken before the response -> ``onFailure(RemotingSendRequestException)`` (failFast)
     * Each callback fires **exactly once** (the done flag decides).
     */
    public function invokeAsync(string $addr, RemotingCommand $request, ?\Closure $onSuccess, ?\Closure $onFailure, ?int $timeoutMillis = null): void
    {
        $timeout = $timeoutMillis ?? $this->invokeTimeoutMillis;
        $future = new PendingResponse($request->opaque, $timeout, $onSuccess, $onFailure, $addr, $request);
        $this->pending[$request->opaque] = $future;
        try {
            $future->conn = $this->send($addr, $request);
        } catch (\Throwable $e) {
            unset($this->pending[$request->opaque]);
            throw $e;
        }
    }

    /**
     * Pumps all in-flight requests until the pending table empties or the timeout hits.
     * This port has no IO thread: frame reception, callback dispatch and timeout sweeping are all
     * driven by this call.
     */
    public function waitResponses(int $timeoutMillis): void
    {
        $deadline = self::monoMillis() + $timeoutMillis;
        while ($this->pending !== []) {
            $now = self::monoMillis();
            if ($now >= $deadline) {
                break;
            }
            $this->sweepExpired();
            if ($this->pending === []) {
                break;
            }
            $watch = $this->watchableConns();
            if ($watch === []) {
                // No waitable connection (already closed by failFast etc.): yield briefly and let timeout sweeping settle
                usleep(5000);
                continue;
            }
            $r = array_values($watch);
            $w = [];
            $e = [];
            $remainingUs = (int)min(($deadline - self::monoMillis()) * 1000.0, 500000.0);
            $n = @stream_select($r, $w, $e, 0, $remainingUs);
            if ($n === false) {
                break;
            }
            if ($n === 0) {
                continue;
            }
            foreach ($r as $conn) {
                $addr = $this->addrOfConn($conn);
                if ($addr === null) {
                    continue;
                }
                $this->readFrom($addr, $conn);
            }
        }
        $this->sweepExpired();
    }

    /** @return array<string, resource> addr -> conn (only connections bound to in-flight requests) */
    private function watchableConns(): array
    {
        $watch = [];
        foreach ($this->pending as $f) {
            if ($f->conn !== null && is_resource($f->conn)) {
                $watch[$f->addr] = $f->conn;
            }
        }
        return $watch;
    }

    /**
     * Pumps inbound frames on **all** connections — the difference from waitResponses: it does
     * not require the connection to carry in-flight requests.
     *
     * The Request-Reply waiting period is exactly this scenario: the send long since completed
     * (pending emptied), but the broker **pushes** the reply back via
     * PUSH_REPLY_MESSAGE_TO_CLIENT(326). waitResponses, which only watches pending connections,
     * returns right after pending empties, and the pushed reply keeps sitting in the kernel
     * buffer unread — measured on a live cluster, the broker-side callClient then times out with
     * "push reply message to ... fail", while the requester only reads it on the next send, by
     * which time the future has long been swept for timeout ("not matched any request").
     */
    public function pumpIncoming(int $timeoutMillis): void
    {
        $deadline = self::monoMillis() + $timeoutMillis;
        while (true) {
            if (self::monoMillis() >= $deadline) {
                break;
            }
            $watch = $this->conns;
            if ($watch === []) {
                usleep(5000);
                continue;
            }
            $r = array_values($watch);
            $w = [];
            $e = [];
            $remainingUs = (int)min(($deadline - self::monoMillis()) * 1000.0, 500000.0);
            $n = @stream_select($r, $w, $e, 0, $remainingUs);
            if ($n === false) {
                break;
            }
            if ($n === 0) {
                continue;
            }
            foreach ($r as $conn) {
                $addr = $this->addrOfConn($conn);
                if ($addr === null) {
                    continue;
                }
                $this->readFrom($addr, $conn);
            }
        }
    }

    private function addrOfConn($conn): ?string
    {
        foreach ($this->pending as $f) {
            if ($f->conn === $conn) {
                return $f->addr;
            }
        }
        foreach ($this->conns as $addr => $c) {
            if ($c === $conn) {
                return $addr;
            }
        }
        return null;
    }

    /** Sync path only: pump until this future gets a response / fails / times out. */
    private function pumpForFuture(PendingResponse $future, int $timeoutMillis): void
    {
        $deadline = self::monoMillis() + $timeoutMillis;
        while ($future->response === null && $future->cause === null) {
            $remainingMs = $deadline - self::monoMillis();
            if ($remainingMs <= 0) {
                break;
            }
            if ($future->conn === null || !is_resource($future->conn)) {
                break;
            }
            $r = [$future->conn];
            $w = [];
            $e = [];
            $n = @stream_select($r, $w, $e, 0, (int)min($remainingMs * 1000.0, 500000.0));
            if ($n === false) {
                break;
            }
            if ($n === 0) {
                continue;
            }
            if (!$this->readFrom($future->addr, $future->conn)) {
                break;
            }
        }
    }

    // ---------- Frame reception ----------

    /**
     * Reads one chunk from the connection and dispatches frames as far as they assemble.
     * Returning false means the connection is EOF/broken (the future has been marked by failFast).
     */
    private function readFrom(string $addr, $conn): bool
    {
        $chunk = @fread($conn, 65536);
        if ($chunk === false || $chunk === '') {
            // WARNING, measured on Windows: after the peer closes, stream_select keeps reporting
            // readable while fread returns **false** (not an empty string), with feof() true.
            // Earlier code treated only an empty string as EOF and false as "no data for now",
            // so the receive loop spun until timeout before settling (it should have failFasted
            // immediately). Conversely, a transient failure of a non-blocking read (TLS
            // handshake/decryption jitter) must not be treated as a dead connection either —
            // hence feof decides.
            if (feof($conn)) {
                // The peer closed: failFast immediately, dooming the in-flight requests on this connection
                $this->closeChannel($addr);
                return false;
            }
            return true; // no data for now on a non-blocking socket
        }
        $buf = ($this->inBuffers[$addr] ?? '') . $chunk;
        while (strlen($buf) >= 4) {
            $totalLen = RocketMQSerializable::unpackSignedInt(substr($buf, 0, 4));
            if ($totalLen <= 0 || $totalLen > self::MAX_FRAME_LENGTH) {
                // An untrustworthy length field means the stream is corrupt: drop the whole buffer,
                // do not try to realign frame boundaries
                $buf = '';
                break;
            }
            if (strlen($buf) < 4 + $totalLen) {
                break;
            }
            $frame = substr($buf, 0, 4 + $totalLen);
            $buf = substr($buf, 4 + $totalLen);
            // dispatch may closeChannel (GO_AWAY / failed response write); confirm the connection is still alive first
            if (!is_resource($conn) || ($this->conns[$addr] ?? null) !== $conn) {
                break;
            }
            $this->dispatchFrame($frame, $addr);
        }
        if (isset($this->conns[$addr]) && $this->conns[$addr] === $conn) {
            $this->inBuffers[$addr] = $buf;
        }
        return true;
    }

    /** Frame dispatch (public so tests/upper layers can inject frames). */
    public function dispatchFrame(string $frame, string $addr): void
    {
        try {
            $cmd = RemotingCommand::decode($frame);
        } catch (\Throwable) {
            return;
        }
        if ($cmd->isResponseType()) {
            $this->deliverResponse($cmd, $addr);
            return;
        }
        // Non-response command: fall back to "should have been the peer's response but lacks the
        // response flag" first, then handle as a broker-initiated request.
        if (isset($this->pending[$cmd->opaque])) {
            $this->deliverResponse($cmd, $addr);
            return;
        }
        $handler = $this->processors[$cmd->code] ?? null;
        if ($handler === null) {
            Logger::debug(sprintf('no processor registered for request code %s (opaque=%s) from %s', $cmd->code, $cmd->opaque, $addr));
            return;
        }
        // The handler **may** return a response command; whatever it returns is written back to
        // the peer under the request's opaque.
        // The typical responder is PUSH_REPLY_MESSAGE_TO_CLIENT(326). Transaction check 39 is
        // oneway; returning null suffices.
        try {
            $response = $handler($cmd, $addr);
        } catch (\Throwable $e) {
            Logger::warning(sprintf('processor for request code %s raised: %s', $cmd->code, $e->getMessage()));
            $response = RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'process request fail');
        }
        if ($response !== null && !$cmd->isOnewayRPC()) {
            $response->opaque = $cmd->opaque;
            $this->writeResponse($addr, $response);
        }
    }

    private function deliverResponse(RemotingCommand $cmd, string $addr): void
    {
        $future = $this->pending[$cmd->opaque] ?? null;
        if ($future === null) {
            return;
        }
        unset($this->pending[$cmd->opaque]);
        $this->applyAfterResponseHooks($addr, $future->request, $cmd);

        // GO_AWAY follows the same rule as the sync path: reconnect and resend once; a second
        // GO_AWAY reports the error via callback.
        if ($cmd->code === ResponseCode::GO_AWAY
            && !$future->goAwayRetried
            && $this->enableReconnectForGoAway) {
            $future->goAwayRetried = true;
            $spent = (int)(self::monoMillis() - $future->beginAt);
            $retryTimeout = max(1, $future->timeoutMillis - $spent);
            $this->closeChannel($addr);
            $retry = self::retryRequest($future->request ?? new RemotingCommand($cmd->code));
            $retryFuture = new PendingResponse($retry->opaque, $retryTimeout, $future->onSuccess, $future->onFailure, $addr, $retry);
            $retryFuture->goAwayRetried = true;
            $this->pending[$retry->opaque] = $retryFuture;
            try {
                $retryFuture->conn = $this->send($addr, $retry);
            } catch (\Throwable $e) {
                unset($this->pending[$retry->opaque]);
                $this->fireFailure($retryFuture, $e);
            }
            return;
        }

        $future->response = $cmd;
        if (!$future->done && $future->onSuccess !== null) {
            $future->done = true;
            try {
                ($future->onSuccess)($cmd);
            } catch (\Throwable $e) {
                // An exception thrown inside a callback must not take down the frame receive
                // path: log it and keep receiving the remaining in-flight requests.
                Logger::warning('remoting: invoke callback raised: ' . $e->getMessage());
            }
        }
    }

    // ---------- In-flight request timeout sweep ----------

    private function sweepExpired(): void
    {
        // Predicate: beginAt + timeoutMillis + 1000 <= now. The 1s grace is headroom for
        // "the response is already on its way" — a response arriving within 1s after timeout is
        // still delivered as success.
        $graceMillis = 1000.0;
        $now = self::monoMillis();
        foreach ($this->pending as $opaque => $f) {
            if ($now - $f->beginAt <= $f->timeoutMillis + $graceMillis) {
                continue;
            }
            unset($this->pending[$opaque]);
            $this->fireFailure($f, new RemotingTimeoutException($f->addr, $f->timeoutMillis));
        }
    }

    // ---------- Fail in-flight requests immediately on disconnect (failFast) ----------

    /**
     * The connection ``conn`` is already closed: doom all of its in-flight requests that have no
     * response yet as send failures.
     *
     * RemotingSendRequestException is raised here (not a timeout) — the async send retry
     * classification branches on the exception type, and a wrong type means wrong semantics.
     * Requests are claimed by connection **resource identity**, not by address: the same address
     * may already have a new connection (GO_AWAY reconnect-and-resend, closeChannel after a write
     * failure), and the old settlement must not hurt the new connection.
     */
    private function failFast(string $addr, $conn): int
    {
        $doomed = [];
        foreach ($this->pending as $opaque => $f) {
            if ($f->conn === $conn) {
                unset($this->pending[$opaque]);
                $doomed[] = $f;
            }
        }
        foreach ($doomed as $f) {
            $f->sendRequestOk = false;
            $this->fireFailure($f, new RemotingSendRequestException($addr, 'connection closed'));
        }
        if ($doomed !== []) {
            Logger::warning(sprintf('remoting: connection to %s closed, %d in-flight request(s) failed fast', $addr, count($doomed)));
        }
        return count($doomed);
    }

    private function fireFailure(PendingResponse $f, \Throwable $error): void
    {
        $f->cause = $error;
        if (!$f->done && $f->onFailure !== null) {
            $f->done = true;
            try {
                ($f->onFailure)($error);
            } catch (\Throwable $e) {
                Logger::warning('remoting: failure callback raised: ' . $e->getMessage());
            }
        }
    }

    // ---------- oneway ----------

    public function invokeOneway(string $addr, RemotingCommand $request): void
    {
        $request->markOnewayRPC();
        $this->send($addr, $request);
    }

    // ---------- Broker-initiated request handlers ----------

    /**
     * Registers a broker-initiated request handler: a dispatch table keyed by requestCode.
     *
     * The handler signature is ``handler(cmd, addr) -> ?RemotingCommand``: returning non-null
     * writes that command back as the response (the framework fills opaque with the request's
     * opaque); returning null means no response is needed (oneway requests, such as the
     * transaction check CHECK_TRANSACTION_STATE(39)).
     */
    public function registerProcessor(int $requestCode, callable $handler): void
    {
        $this->processors[$requestCode] = $handler;
    }

    public function unregisterProcessor(int $requestCode): void
    {
        unset($this->processors[$requestCode]);
    }

    // ---------- namesrv address management (round-robin / failover) ----------

    public function updateNameServerAddressList(array $addrs): void
    {
        if ($addrs === $this->nameServerList) {
            return;
        }
        $this->nameServerList = array_values($addrs);
    }

    /** @return list<string> */
    public function getNameServerAddressList(): array
    {
        return $this->nameServerList;
    }

    /** Round-robins one namesrv, advancing the cursor each call; returns null on an empty list, leaving error reporting to the caller. */
    public function chooseNameServer(): ?string
    {
        if ($this->nameServerList === []) {
            return null;
        }
        $addr = $this->nameServerList[$this->nameServerIndex % count($this->nameServerList)];
        $this->nameServerIndex = ($this->nameServerIndex + 1) % max(1, count($this->nameServerList));
        return $addr;
    }

    // ---------- Shutdown ----------

    public function shutdown(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        foreach (array_keys($this->conns) as $addr) {
            $this->closeChannel($addr);
        }
        $this->pending = [];
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }
}

/**
 * The wait slot of one in-flight request.
 *
 * A synchronous waiter reads ``response`` / ``cause``; an async waiter relies on the
 * ``onSuccess`` / ``onFailure`` callbacks (fired exactly once; the ``done`` flag decides).
 */
final class PendingResponse
{
    /** @var resource|null the connection the request was actually written on (failFast claims by resource identity) */
    public $conn = null;
    public ?RemotingCommand $response = null;
    public ?\Throwable $cause = null;
    public bool $sendRequestOk = false;
    public bool $done = false;
    public bool $goAwayRetried = false;
    public float $beginAt;

    public function __construct(
        public int $opaque,
        public int $timeoutMillis,
        public ?\Closure $onSuccess,
        public ?\Closure $onFailure,
        public string $addr,
        public ?RemotingCommand $request = null,
    ) {
        $this->beginAt = RemotingClient::monoMillis();
    }
}
