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
 * socket 长连接客户端（对应 org.apache.rocketmq.remoting.netty.NettyRemotingClient 的核心能力，
 * 移植自 remoting/client.py）。
 *
 * 提供：连接管理（惰性建连 + 复用 + 关闭回收）、invokeSync/invokeAsync/invokeOneway、
 * opaque 映射回调分发、超时控制、GO_AWAY 换连接重发、
 * 连接断开时立刻失败该连接上的在途请求（对应 Java failFast）。
 *
 * PHP 无线程（见 php/PORTING.md 异步模型）：
 *  - ``invokeAsync`` = 注册 pending 表（opaque → PendingResponse）+ 非阻塞写，立即返回；
 *  - ``waitResponses(timeout)`` 用 ``stream_select`` 泵响应并分发回调、清理超时；
 *  - ``invokeSync`` 内部泵到完成（同一套收帧路径）。
 */
final class RemotingClient
{
    public const MAX_FRAME_LENGTH = 16 * 1024 * 1024;

    private int $connectTimeoutMillis;
    private int $invokeTimeoutMillis;

    /** 对应 Java NettyClientConfig.enableReconnectForGoAway（默认 **true**）：
     * 收到 ResponseCode.GO_AWAY(1500) 时换一条连接重发一次。 */
    private bool $enableReconnectForGoAway;

    /** TLS（对应 Java NettyRemotingClient 的 isUseTLS / tls.enable）。显式参数优先，
     * 否则读 ROCKETMQ_TLS_ENABLE（Java 是 JVM 系统属性 -Dtls.enable，这里等价为 env）。 */
    private bool $tlsEnable;

    /**
     * TLS 细项（对应 Java TlsSystemConfig 的 certPath 族属性；键均可选）：
     *   - caCert:     CA 证书路径。给了就**真校验**服务端证书链 + 主机名
     *                 （tls.test.mode.enable=false 口径）；不给则信任自签（test-mode，默认）。
     *   - clientCert: 客户端证书路径（mTLS，对应 tls.client.certPath + authClient=true）。
     *   - clientKey:  客户端私钥路径（对应 tls.client.keyPath；cert 内含私钥时可省）。
     *   - serverName: SNI/主机名校验覆盖（默认用连接地址的 host）。
     *
     * @var array{caCert?:string, clientCert?:string, clientKey?:string, serverName?:string}
     */
    private array $tlsOptions;

    private bool $closed = false;

    /** @var array<string, resource> addr → 连接 */
    private array $conns = [];

    /** @var array<string, string> addr → 未凑满整帧的接收缓冲 */
    private array $inBuffers = [];

    /** @var array<int, PendingResponse> opaque → 在途请求 */
    private array $pending = [];

    /** @var array<int, callable(RemotingCommand, string): ?RemotingCommand> broker 主动请求处理器 */
    private array $processors = [];

    /** @var list<RPCHook> */
    private array $rpcHooks = [];

    /** @var list<string> namesrv 地址表（轮询/故障切换） */
    private array $nameServerList = [];
    private int $nameServerIndex = 0;

    /**
     * @param list<string>|null $nameServers 初始 namesrv 地址（也可后置 updateNameServerAddressList）
     */
    public function __construct(
        int $connectTimeoutMillis = 3000,
        int $invokeTimeoutMillis = 15000,
        ?bool $tlsEnable = null,
        bool $enableReconnectForGoAway = true,
        ?array $nameServers = null,
        ?array $tlsOptions = null,
    ) {
        $this->connectTimeoutMillis = $connectTimeoutMillis;
        $this->invokeTimeoutMillis = $invokeTimeoutMillis;
        $this->enableReconnectForGoAway = $enableReconnectForGoAway;
        if ($tlsEnable === null) {
            $tlsEnable = in_array(strtolower(trim((string)getenv('ROCKETMQ_TLS_ENABLE'))), ['1', 'true', 'yes'], true);
        }
        $this->tlsEnable = (bool)$tlsEnable;
        $this->tlsOptions = $tlsOptions ?? [];
        if ($nameServers !== null) {
            $this->nameServerList = array_values($nameServers);
        }
    }

    /** 超时判定专用单调时钟毫秒（对应 Java System.currentTimeMillis 口径，取更稳的 monotonic）。 */
    public static function monoMillis(): float
    {
        return hrtime(true) / 1e6;
    }

    // ---------- 连接管理 ----------

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
                // 对应 Java tls.test.mode.enable 默认 true：信任 broker 的自签证书、
                // 不校验主机名、不带客户端证书（PERMISSIVE broker 即配即通）。
                'verify_peer' => false,
                'verify_peer_name' => false,
                'allow_self_signed' => true,
            ],
        ]);
        $timeoutSec = $this->connectTimeoutMillis / 1000.0;
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
        if ($this->tlsEnable) {
            // 对应 Java pipeline.addFirst(SslHandler)：TLS 包住整个流，在任何 RocketMQ
            // 帧之前完成握手。Python 侧是先建 plain socket 再 wrap_socket，等价路径。
            $caCert = $this->tlsOptions['caCert'] ?? null;
            if ($caCert !== null && $caCert !== '') {
                // 有 CA ⇒ 真校验（tls.test.mode.enable=false 口径）：证书链 + 主机名。
                stream_context_set_option($conn, 'ssl', 'verify_peer', true);
                stream_context_set_option($conn, 'ssl', 'verify_peer_name', true);
                stream_context_set_option($conn, 'ssl', 'allow_self_signed', false);
                stream_context_set_option($conn, 'ssl', 'cafile', $caCert);
                $serverName = $this->tlsOptions['serverName'] ?? null;
                if ($serverName !== null && $serverName !== '') {
                    stream_context_set_option($conn, 'ssl', 'peer_name', $serverName);
                }
            } else {
                // test-mode（Java tls.test.mode.enable 默认 true）：信任 broker 的自签证书、
                // 不校验主机名、不带客户端证书（PERMISSIVE broker 即配即通）。
                stream_context_set_option($conn, 'ssl', 'verify_peer', false);
                stream_context_set_option($conn, 'ssl', 'verify_peer_name', false);
                stream_context_set_option($conn, 'ssl', 'allow_self_signed', true);
            }
            $clientCert = $this->tlsOptions['clientCert'] ?? null;
            if ($clientCert !== null && $clientCert !== '') {
                // mTLS：对应 Java tls.client.certPath/keyPath（broker 端
                // tls.client.authServer=true 时会要求出示）。
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
        // TCP_NODELAY（尽力而为：ext-sockets 可用才设置）
        if (function_exists('socket_set_option') && defined('IPPROTO_TCP') && defined('TCP_NODELAY')) {
            @socket_set_option($conn, IPPROTO_TCP, TCP_NODELAY, 1);
        }
        return $conn;
    }

    /** @return array{0: string, 1: string} [host, port]（IPv6 字面量 ``[::1]:9876`` 兼容） */
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
     * 摘掉到 addr 的连接（对应 Java closeChannel 的 removeChannel + channel.close）。
     * 在途请求立即按发送失败处理（PHP 单线程里没有独立读线程可做 failFast）。
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

    // ---------- RPC 钩子 ----------

    /**
     * 发送前依次执行 RPC 钩子（对应 Java NettyRemotingAbstract#doBeforeRpcHooks）。
     *
     * **必须在 cmd.encode() 之前调用**：ACL 钩子要把 AccessKey/Signature 写进
     * ext_fields，而 Signature 覆盖的是 makeCustomHeaderToNet() 之后的完整
     * ext_fields + body —— 即真正会上线的那份内容。encode() 之后再注入就晚了。
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
                // 忽略钩子异常，不影响响应分发
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

    // ---------- 请求发送 ----------

    /**
     * 把命令写到连接上（钩子 → encode → 全量写出）。
     * 返回所用的连接资源（记账用：failFast 按资源身份认领在途请求）。
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
                // 非阻塞下 0 字节可能是缓冲瞬时满，靠 deadline 兜底
                if (feof($conn)) {
                    throw new \RuntimeException('connection closed on write');
                }
                continue;
            }
            $written += $chunk;
        }
    }

    /**
     * 把响应写回 broker。
     *
     * **不能**走 send()：send() 会执行 RPC 钩子（ACL 会给请求加签），而响应报文
     * 不需要也不能带签名。写失败只记日志：这条响应对应 broker 侧
     * ``Broker2Client.callClient`` 超时，不该让收帧路径因此中断。
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

    // ---------- 同步 RPC ----------

    /** 同步 RPC，含 GO_AWAY 换连接重发（对应 Java NettyRemotingClient#invokeImpl:828-873）。 */
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
     * GO_AWAY 的收口：开关关掉直接报错；否则换连接重发一次，第二次还是 GO_AWAY 就抛。
     * 同步与异步两条路共用同一套判定，重发次数也必须一样（一次）。
     */
    private function handleGoAway(string $addr, RemotingCommand $request, int $timeout, float $started, RemotingCommand $response): RemotingCommand
    {
        if (!$this->enableReconnectForGoAway) {
            throw new RemotingSendRequestException($addr, 'Receive GO_AWAY from channel ' . $addr);
        }
        Logger::info("remoting: receive GO_AWAY from {$addr}, reconnect and retry once");
        $this->closeChannel($addr);
        // 重发只花剩余预算：Java 用同一个 Stopwatch 的 elapsed 扣减 timeoutMillis。
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
     * GO_AWAY 重发用的请求副本（对应 Java 的 createRequestCommand + setBody + setExtFields）。
     *
     * 必须是**新** opaque：旧的那条已经在途表里摘掉了，复用会让响应错配到别的请求。
     * ext_fields 按值复制——ACL 签名（AccessKey/Signature）就存在这里，重发要带原签名，
     * 所以副本不能从 custom_header 重新推导（推导会丢掉签名那两项）。
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
        // 连接断掉时 failFast 已经置好 cause 并唤醒了我：按 Java 的口径报"发送失败"，
        // 而不是等满 timeout 再报超时。两条路的可重试性不一样（见 producer 的重试分类）。
        if ($future->cause !== null) {
            throw $future->cause;
        }
        throw new RemotingTimeoutException($addr, $timeout);
    }

    // ---------- 异步 RPC ----------

    /**
     * 异步调用：注册 pending 表 + 非阻塞写后**立即返回**（见 php/PORTING.md 异步模型）。
     * 调用方随后用 waitResponses() 泵；Producer 的 asyncSend 会在回调注册后内部泵到完成。
     *
     * 回调契约（与 Java InvokeCallback 的 operationSucceed / operationFail 二分等价）：
     *  - 正常收到响应 → ``onSuccess(response)``
     *  - 超过 timeout 仍无响应 → ``onFailure(RemotingTimeoutException)``
     *  - 连接在响应之前断开 → ``onFailure(RemotingSendRequestException)``（failFast）
     * 回调**恰好触发一次**（done 标志定胜负，对应 Java executeCallbackOnlyOnce）。
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
     * 泵所有在途请求直到 pending 表空或超时（对应 Java scanResponseTable + IO 线程）。
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
                // 没有可等连接（连接已被 failFast 关闭等）：短暂让路，等超时清理收口
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

    /** @return array<string, resource> addr → conn（只收在途请求绑定的连接） */
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

    /** 同步路径专用：泵到该 future 收到响应 / 失败 / 超时。 */
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

    // ---------- 收帧 ----------

    /**
     * 从连接读一段数据并尽量凑帧分发。
     * 返回 false 表示连接已 EOF/损坏（future 已被 failFast 标记）。
     */
    private function readFrom(string $addr, $conn): bool
    {
        $chunk = @fread($conn, 65536);
        if ($chunk === false || $chunk === '') {
            // ⚠ Windows 上实测：对端 close 后 stream_select 会一直报 readable，而 fread 返回
            // **false**（不是空串），此时 feof() 为 true。早先只把空串当 EOF，false 被当成
            // "暂时无数据"，结果收帧循环空转到超时才收口（本该立刻 failFast）。
            // 反过来，非阻塞读的瞬时失败（TLS 握手/解密抖动）也不能当死连接，故以 feof 定夺。
            if (feof($conn)) {
                // 对端关了：立刻判死这条连接上的在途请求（对应 Java failFast）
                $this->closeChannel($addr);
                return false;
            }
            return true; // 非阻塞下暂时无数据
        }
        $buf = ($this->inBuffers[$addr] ?? '') . $chunk;
        while (strlen($buf) >= 4) {
            $totalLen = RocketMQSerializable::unpackSignedInt(substr($buf, 0, 4));
            if ($totalLen <= 0 || $totalLen > self::MAX_FRAME_LENGTH) {
                // 流已坏，丢弃整个缓冲（Python 同口径 buf.clear()）
                $buf = '';
                break;
            }
            if (strlen($buf) < 4 + $totalLen) {
                break;
            }
            $frame = substr($buf, 0, 4 + $totalLen);
            $buf = substr($buf, 4 + $totalLen);
            // dispatch 可能 closeChannel（GO_AWAY / 写响应失败），先确认连接还活着
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

    /** 收帧分发（public 供测试/上层注入帧使用）。 */
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
        // 非响应命令：先按「本应是对端响应却没带响应标志」兜底，再按 broker 主动请求处理。
        if (isset($this->pending[$cmd->opaque])) {
            $this->deliverResponse($cmd, $addr);
            return;
        }
        $handler = $this->processors[$cmd->code] ?? null;
        if ($handler === null) {
            Logger::debug(sprintf('no processor registered for request code %s (opaque=%s) from %s', $cmd->code, $cmd->opaque, $addr));
            return;
        }
        // 处理器**可以**返回一个响应命令（对应 Java NettyRequestProcessor#processRequest 的返回值）。
        // 需要返回的典型是 PUSH_REPLY_MESSAGE_TO_CLIENT(326)。事务回查 39 是 oneway，返回 null 即可。
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

        // GO_AWAY 与同步路径同口径：换连接重发一次，第二次仍是 GO_AWAY 则回调报错。
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
                // 回调里抛出的异常不能带走收帧路径（对应 Java 的 try-catch + warn）
                Logger::warning('remoting: invoke callback raised: ' . $e->getMessage());
            }
        }
    }

    // ---------- 在途请求超时清理（对应 Java scanResponseTable） ----------

    private function sweepExpired(): void
    {
        // 与 Java 同式：beginTimestamp + timeoutMillis + 1000 <= now。这 1s 宽限是给
        // "响应已经在路上"留的余量——超时后 1s 内到达的响应仍按成功投递。
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

    // ---------- 连接断开时立刻失败在途请求（对应 Java failFast / requestFail） ----------

    /**
     * ``conn`` 这条连接已经关了：把它名下还没响应的在途请求全部判为发送失败。
     *
     * Java 报 RemotingSendRequestException（不是超时），异步发送的重试分类按类型分流，
     * 错类型等于错语义。按连接**资源身份**认领，不按地址：同地址可能已经换了新连接
     * （GO_AWAY 换连接重发、写失败后的 closeChannel），旧收尾不能误伤新连接。
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

    // ---------- broker 主动请求处理器 ----------

    /**
     * 注册 broker 主动请求处理器（对应 Java NettyRemotingServer 的 processor 表）。
     *
     * handler 签名 ``handler(cmd, addr) -> ?RemotingCommand``：**返回非 null 就会把该命令
     * 作为响应写回**（opaque 由框架填成请求的 opaque），返回 null 表示不需要响应
     * （oneway 请求，如事务回查 CHECK_TRANSACTION_STATE(39)）。
     */
    public function registerProcessor(int $requestCode, callable $handler): void
    {
        $this->processors[$requestCode] = $handler;
    }

    public function unregisterProcessor(int $requestCode): void
    {
        unset($this->processors[$requestCode]);
    }

    // ---------- namesrv 地址管理（轮询/故障切换） ----------

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

    /** 轮询取一个 namesrv；列表为空返回 null（对应 Java getAndCreateNameserverChannel 的轮询语义）。 */
    public function chooseNameServer(): ?string
    {
        if ($this->nameServerList === []) {
            return null;
        }
        $addr = $this->nameServerList[$this->nameServerIndex % count($this->nameServerList)];
        $this->nameServerIndex = ($this->nameServerIndex + 1) % max(1, count($this->nameServerList));
        return $addr;
    }

    // ---------- 收尾 ----------

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
 * 一次在途请求（对应 Java/Python ``ResponseFuture``）。
 *
 * 同步等待方读 ``response`` / ``cause``；异步方通过 ``onSuccess`` / ``onFailure``
 * 回调（恰好触发一次，``done`` 标志定胜负）。
 */
final class PendingResponse
{
    /** @var resource|null 请求真正写出去时所用的连接（failFast 按资源身份认领） */
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
