<?php

declare(strict_types=1);

/**
 * Transport connection-lifecycle suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientTransportContract.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Five behaviours of the remoting layer, each measured against a socket this script owns, and
 * each asserted on two things at once: the connection table afterwards, and the log
 * line that has to explain it. "The frame vanished and nothing was said" is the defect under
 * test here, so a leg that only checks the table proves half of it.
 *
 *   1. a frame that does not decode -- and a length prefix outside the accepted range, which is
 *      the same event one level earlier -- is an ERROR naming the peer, and the connection goes
 *      the way a peer-EOF connection goes; the peer's next valid frame is served on a fresh one.
 *   2. a response whose opaque no request is waiting on is dropped, with a WARNING naming the
 *      opaque and the address, and the connection stays up.
 *   3. the writable question is about the outbound side, not about having connected: no
 *      connection => true, a connection with room => true, a connection whose send buffer is
 *      full => false.
 *   4. a connection idle past the limit is closed by the sweep while one whose idle time merely
 *      reaches the limit survives; reads and writes both re-start the idle clock; a non-positive
 *      configured limit restores the default instead of switching the sweep off, because the
 *      sweep is always installed; and the two pumps drive it.
 *   5. a synchronous wait that runs out closes the connection it went out on by default, and
 *      keeps it when that is turned off; an expired in-flight entry logs one line per entry.
 *
 * Single-threaded PHP cannot answer a request and wait for it in the same breath, so where a leg
 * needs a peer that has already said something, the bytes go into the accepted socket first and
 * the client reads them on its next pump. The connection table, the idle stamps and the private
 * expiry sweep are reached through reflection.
 */

require_once __DIR__ . '/../bootstrap.php';

putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_transport_logs_' . getmypid());

use RocketMQ\Client\Exceptions\RemotingTimeoutException;
use RocketMQ\Client\Logger;
use RocketMQ\Remoting\PendingResponse;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\RemotingClient;

final class RunClientTransportContract
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    /** @var list<string> the lines the client logger emitted while the current leg ran */
    private array $logged = [];

    public function check(bool $condition, string $label): void
    {
        if ($condition) {
            $this->passed++;
            return;
        }
        $this->failures[] = $label;
        fwrite(STDERR, "FAIL: $label\n");
    }

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        if ($expected === $actual) {
            $this->passed++;
            return;
        }
        $this->failures[] = $label;
        fwrite(STDERR, sprintf(
            "FAIL: %s\n  expected: %s\n  actual:   %s\n",
            $label,
            var_export($expected, true),
            var_export($actual, true),
        ));
    }

    public function summary(): int
    {
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }

    // ---------------- log capture ----------------

    private function captureStart(): void
    {
        $this->logged = [];
        // The handler receives one rendered line, "[date] LEVEL: message"; the level is part of
        // the text, so a leg that wants only the warnings filters on the needle it expects.
        Logger::setHandler(function (string $line): void {
            $this->logged[] = $line;
        });
    }

    private function captureStop(): void
    {
        Logger::setHandler(null);
    }

    /** @return list<string> the captured lines whose text contains $needle */
    private function loggedWith(string $needle): array
    {
        return array_values(array_filter($this->logged, static fn(string $l): bool => str_contains($l, $needle)));
    }

    // ---------------- reflection ----------------

    private function prop(object $o, string $name): mixed
    {
        $p = new ReflectionProperty(RemotingClient::class, $name);
        $p->setAccessible(true);
        return $p->getValue($o);
    }

    private function setProp(object $o, string $name, mixed $value): void
    {
        $p = new ReflectionProperty(RemotingClient::class, $name);
        $p->setAccessible(true);
        $p->setValue($o, $value);
    }

    /** @return array<string, mixed> addr => connection */
    private function conns(RemotingClient $c): array
    {
        return (array)$this->prop($c, 'conns');
    }

    /** @return array<string, float> addr => idle stamp in monotonic milliseconds */
    private function stamps(RemotingClient $c): array
    {
        return (array)$this->prop($c, 'lastActivityMillis');
    }

    private function hasConn(RemotingClient $c, string $addr): bool
    {
        return array_key_exists($addr, $this->conns($c));
    }

    private function ageStamp(RemotingClient $c, string $addr, float $millis): void
    {
        $stamps = $this->stamps($c);
        $stamps[$addr] = $this->monoMillis() - $millis;
        $this->setProp($c, 'lastActivityMillis', $stamps);
    }

    /**
     * Stage several idle stamps at once, each measured back from one clock reading, so a sweep
     * invoked with that same reading sees exactly the idle times named here.
     *
     * @param array<string, int> $millisAgo addr => how long before $now it last carried traffic
     */
    private function stageStamps(RemotingClient $c, int $now, array $millisAgo): void
    {
        $stamps = $this->stamps($c);
        foreach ($millisAgo as $addr => $ago) {
            $stamps[$addr] = (float)($now - $ago);
        }
        $this->setProp($c, 'lastActivityMillis', $stamps);
    }

    private function monoMillis(): float
    {
        $m = new ReflectionMethod(RemotingClient::class, 'monoMillis');
        $m->setAccessible(true);
        return (float)$m->invoke(null);
    }

    private function sweepExpired(RemotingClient $c): void
    {
        $m = new ReflectionMethod(RemotingClient::class, 'sweepExpired');
        $m->setAccessible(true);
        $m->invoke($c);
    }

    // ---------------- loopback peer ----------------

    /** @return array{0: resource, 1: string} [server, addr] */
    private static function listen(): array
    {
        $server = @stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            throw new \RuntimeException("cannot start test peer: $errstr");
        }
        return [$server, (string)stream_socket_get_name($server, false)];
    }

    private static function readExact($stream, int $n): string
    {
        $buf = '';
        while (strlen($buf) < $n) {
            $chunk = @fread($stream, $n - strlen($buf));
            if ($chunk === false || $chunk === '') {
                return $buf;
            }
            $buf .= $chunk;
        }
        return $buf;
    }

    /** One request frame off the peer side, or null when the peer hung up first. */
    private static function readFrame($conn): ?RemotingCommand
    {
        $head = self::readExact($conn, 4);
        if (strlen($head) < 4) {
            return null;
        }
        $total = RocketMQSerializable::unpackSignedInt($head);
        if ($total <= 0) {
            return null;
        }
        $rest = self::readExact($conn, $total);
        if (strlen($rest) < $total) {
            return null;
        }
        return RemotingCommand::decode($head . $rest);
    }

    /**
     * A frame inside a legal length prefix whose own header length does not fit inside it.
     * RemotingCommand::decode raises on this: the peer and this client no longer agree on where
     * the next frame starts, which is exactly the case the contract says is fatal to the socket.
     */
    private static function undecodableFrame(): string
    {
        return pack('N', 8) . pack('N', 0x0FFFFFFF) . 'junk';
    }

    /** A length prefix of -1: the reader cannot even tell how many bytes belong to the frame. */
    private static function badLengthFrame(): string
    {
        return pack('N', -1);
    }

    private static function responseFor(int $opaque): RemotingCommand
    {
        $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $resp->opaque = $opaque;
        return $resp;
    }

    /** Bring one connection up, accepted on the peer side, after a oneway handshake. */
    private function connectOne(RemotingClient $client, string $addr, $server)
    {
        $client->invokeOneway($addr, RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT));
        $conn = @stream_socket_accept($server, 5);
        if ($conn === false) {
            throw new \RuntimeException('test peer: accept failed');
        }
        stream_set_blocking($conn, true);
        self::readFrame($conn);
        return $conn;
    }

    // ==================================================================== T1

    /**
     * A frame that does not decode is an ERROR naming the peer, and the connection leaves the
     * table -- not "frame dropped, socket kept".
     */
    private function testUndecodableFrameClosesAndNamesThePeer(): void
    {
        [$server, $addr] = self::listen();
        $client = new RemotingClient(1000, 3000);
        $conn = $this->connectOne($client, $addr, $server);
        $this->check($this->hasConn($client, $addr), 'decode leg: the connection is in the table before the frame');

        fwrite($conn, self::undecodableFrame());
        $this->captureStart();
        $client->pumpIncoming(300);
        $this->captureStop();

        $this->check(!$this->hasConn($client, $addr),
            'decode leg: an undecodable frame takes the connection out of the table');
        $this->check(!array_key_exists($addr, $this->stamps($client)),
            'decode leg: the closed connection leaves no idle stamp behind');
        $errors = $this->loggedWith('cannot decode frame');
        $this->checkSame(1, count($errors), 'decode leg: exactly one error line for the undecodable frame');
        $this->check(isset($errors[0]) && str_contains($errors[0], $addr),
            'decode leg: the error names the peer the frame came from');
        $this->check(isset($errors[0]) && str_contains($errors[0], 'closing the connection'),
            'decode leg: the error says the connection is going, not merely that the frame is bad');

        @fclose($conn);
        @fclose($server);
        $client->shutdown();
    }

    /** The point of closing: the next valid frame from that peer is served on a fresh socket. */
    private function testNextValidFrameIsServedOnAFreshConnection(): void
    {
        [$server, $addr] = self::listen();
        $client = new RemotingClient(1000, 3000);
        $conn = $this->connectOne($client, $addr, $server);
        $firstConnection = $this->conns($client)[$addr] ?? null;

        fwrite($conn, self::undecodableFrame());
        $this->captureStart();
        $client->pumpIncoming(300);
        $this->captureStop();
        $this->check(!$this->hasConn($client, $addr), 'reuse leg: the broken stream was dropped');
        @fclose($conn);

        $got = null;
        $failed = null;
        $req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        $client->invokeAsync($addr, $req,
            static function (RemotingCommand $resp) use (&$got): void { $got = $resp; },
            static function (\Throwable $e) use (&$failed): void { $failed = $e; },
            4000);
        $fresh = @stream_socket_accept($server, 5);
        $this->check($fresh !== false, 'reuse leg: the client connected again instead of reusing the dead stream');
        if ($fresh !== false) {
            stream_set_blocking($fresh, true);
            $srvCmd = self::readFrame($fresh);
            $this->check($srvCmd !== null && !$srvCmd->isResponseType(),
                'reuse leg: the fresh connection carries the new request');
            fwrite($fresh, self::responseFor($req->opaque)->encode());
            $client->waitResponses(1500);
            $this->check($got !== null && $failed === null,
                'reuse leg: the frame written on the fresh connection reaches the waiting request');
            $this->checkSame(ResponseCode::SUCCESS, $got?->code,
                'reuse leg: and it is the answer, not a leftover of the broken stream');
            $this->check(($this->conns($client)[$addr] ?? null) !== $firstConnection,
                'reuse leg: the connection serving it is a different socket');
            @fclose($fresh);
        }
        @fclose($server);
        $client->shutdown();
    }

    /** A length prefix outside the accepted range is the same failure one level earlier. */
    private function testBadFrameLengthTakesTheConnectionDown(): void
    {
        [$server, $addr] = self::listen();
        $client = new RemotingClient(1000, 3000);
        $conn = $this->connectOne($client, $addr, $server);

        fwrite($conn, self::badLengthFrame());
        $this->captureStart();
        $client->pumpIncoming(300);
        $this->captureStop();

        $this->check(!$this->hasConn($client, $addr),
            'length leg: an out-of-range length prefix closes the connection');
        $errors = $this->loggedWith('bad frame length');
        $this->checkSame(1, count($errors), 'length leg: one error line for the bad length prefix');
        $this->check(isset($errors[0]) && str_contains($errors[0], $addr),
            'length leg: the error names the peer');
        $this->checkSame(0, count($this->loggedWith('cannot decode frame')),
            'length leg: a broken length prefix is reported once, at the level that caught it');

        @fclose($conn);
        @fclose($server);
        $client->shutdown();
    }

    // ==================================================================== T2

    /** A response nobody is waiting for is dropped with a line, and the connection survives. */
    private function testUnmatchedResponseWarnsAndKeepsTheConnection(): void
    {
        [$server, $addr] = self::listen();
        $client = new RemotingClient(1000, 3000);
        $conn = $this->connectOne($client, $addr, $server);

        $req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        $strayOpaque = $req->opaque + 99999;
        // Both answers go on the wire before the request does: the stray one has to be dropped
        // without disturbing the real one behind it.
        fwrite($conn, self::responseFor($strayOpaque)->encode() . self::responseFor($req->opaque)->encode());

        $got = null;
        $failed = null;
        $this->captureStart();
        $client->invokeAsync($addr, $req,
            static function (RemotingCommand $resp) use (&$got): void { $got = $resp; },
            static function (\Throwable $e) use (&$failed): void { $failed = $e; },
            2000);
        $client->waitResponses(1500);
        $this->captureStop();

        $warns = $this->loggedWith('does not match any in-flight request');
        $this->checkSame(1, count($warns), 'unmatched leg: the dropped response is logged once');
        $this->check(isset($warns[0]) && str_contains($warns[0], (string)$strayOpaque),
            'unmatched leg: the warning names the opaque nobody was waiting for');
        $this->check(isset($warns[0]) && str_contains($warns[0], $addr),
            'unmatched leg: and the address it arrived from');
        $this->check($got !== null && $failed === null,
            'unmatched leg: the real answer still reaches the caller after the stray one');
        $this->check($this->hasConn($client, $addr),
            'unmatched leg: a correctly framed stray response keeps the connection');

        @fclose($conn);
        @fclose($server);
        $client->shutdown();
    }

    // ==================================================================== T3

    private function testWritableIsTrueWithoutAConnection(): void
    {
        $client = new RemotingClient(1000, 3000);
        $this->checkSame(true, $client->isChannelWritable('127.0.0.1:1'),
            'writable leg: an address with no connection is writable -- the next send connects');
        $this->checkSame([], $this->conns($client), 'writable leg: and answering the question created nothing');
        $client->shutdown();
    }

    private function testWritableForALiveConnectionWithRoom(): void
    {
        [$server, $addr] = self::listen();
        $client = new RemotingClient(1000, 3000);
        $conn = $this->connectOne($client, $addr, $server);
        $this->checkSame(true, $client->isChannelWritable($addr),
            'writable leg: a live connection whose send buffer has room is writable');
        @fclose($conn);
        @fclose($server);
        $client->shutdown();
    }

    /**
     * A socket the client holds that has no send-buffer room is the one unwritable answer.
     *
     * A connected pair whose buffers are clamped small, rather than loopback TCP: a TCP socket
     * that a peer never reads keeps acknowledging, so the writer always ends up with a few bytes
     * of room again and "full" is not a stable state to assert on. Here the fill stops the moment
     * the write would block, and the probe agrees.
     */
    private function testNotWritableWhileTheSendBufferIsFull(): void
    {
        $size = 8192;
        [$ours, $theirs] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        if ($ours === false || $theirs === false) {
            $this->check(false, 'saturated leg: could not create the connected pair');
            return;
        }
        socket_set_option(socket_import_stream($ours), SOL_SOCKET, SO_SNDBUF, $size);
        socket_set_option(socket_import_stream($theirs), SOL_SOCKET, SO_RCVBUF, $size);
        stream_set_blocking($ours, false);
        stream_set_blocking($theirs, false);

        // The peer end is never read: the bytes stack up until this end has no room left.
        $filled = 0;
        $payload = str_repeat('x', $size);
        $stopped_after_fill = false;
        for ($attempt = 0; $attempt < 256; $attempt++) {
            $n = @fwrite($ours, $payload);
            if ($n === false || $n === 0) {
                $stopped_after_fill = true;
                break;
            }
            $filled += $n;
        }
        $this->check($filled > 0, 'saturated leg: the socket accepted at least one write before filling');
        $this->check($stopped_after_fill, 'saturated leg: the socket filled until a write could not go out');

        $client = new RemotingClient(1000, 3000);
        $addr = '127.0.0.1:19999';
        $conns = $this->conns($client);
        $conns[$addr] = $ours;
        $this->setProp($client, 'conns', $conns);
        $this->checkSame(false, $client->isChannelWritable($addr),
            'saturated leg: a live connection with a full send buffer is the unwritable case');

        $drained = 0;
        while ($drained < $filled) {
            $chunk = @fread($theirs, 65536);
            if ($chunk === false || $chunk === '') {
                break;
            }
            $drained += strlen($chunk);
        }
        $this->check($drained > 0, 'saturated leg: the peer side drained what was queued');
        $this->checkSame(true, $client->isChannelWritable($addr),
            'saturated leg: once there is room again the same connection is writable');

        $client->shutdown();
        if (is_resource($ours)) {
            @fclose($ours);
        }
        if (is_resource($theirs)) {
            @fclose($theirs);
        }
    }

    // ==================================================================== T4

    /**
     * The sweep closes the connection that is past the limit only, warns about it, and takes its
     * stamp with it.
     *
     * Both stamps and the clock reading are staged, so the boundary -- idle == limit -- is exact
     * instead of a race against the wall clock.
     */
    private function testIdleSweepClosesQuietAndKeepsBusy(): void
    {
        [$quietServer, $quietAddr] = self::listen();
        [$busyServer, $busyAddr] = self::listen();
        $client = new RemotingClient(1000, 3000, channelIdleTimeoutSeconds: 2);
        $quietConn = $this->connectOne($client, $quietAddr, $quietServer);
        $busyConn = $this->connectOne($client, $busyAddr, $busyServer);
        $this->checkSame(2, $client->getChannelIdleTimeoutSeconds(),
            'idle leg: a positive limit is kept as configured');

        $t = (int)ceil($this->monoMillis());
        $this->stageStamps($client, $t, [$quietAddr => 2001, $busyAddr => 2000]);

        $this->captureStart();
        $closed = $client->sweepIdleConnections($t);
        $this->captureStop();

        $this->checkSame(1, $closed, 'idle leg: exactly one connection was reaped');
        $this->check(!$this->hasConn($client, $quietAddr),
            'idle leg: the connection idle past the limit is gone from the table');
        $this->check(!array_key_exists($quietAddr, $this->stamps($client)),
            'idle leg: and it leaves no stamp behind');
        $this->check($this->hasConn($client, $busyAddr),
            'idle leg: idle time exactly equal to the limit is not past it');
        $warns = $this->loggedWith('idle for');
        $this->checkSame(1, count($warns), 'idle leg: the close is logged once');
        $this->check(isset($warns[0]) && str_contains($warns[0], $quietAddr),
            'idle leg: the warning names the address it closed');
        $this->check(array_key_exists($busyAddr, $this->stamps($client)),
            'idle leg: the surviving connection keeps its stamp for the next sweep');

        // One millisecond more and the survivor is past the limit too.
        $this->stageStamps($client, $t, [$busyAddr => 2001]);
        $this->captureStart();
        $this->checkSame(1, $client->sweepIdleConnections($t),
            'idle leg: the connection past the limit on the next pass is reaped then');
        $this->captureStop();

        @fclose($quietConn);
        @fclose($busyConn);
        @fclose($quietServer);
        @fclose($busyServer);
        $client->shutdown();
    }

    /**
     * A write or a read restarts the idle clock, so a busy connection is never the idle one.
     *
     * The idle limit is deliberately long here: the pumps sweep on entry, and a leg that ages the
     * stamp past a short limit would be reaped before the traffic it means to measure arrives.
     * What the leg asserts is the stamp itself -- that the write and the read moved it.
     */
    private function testTrafficRefreshesTheIdleStamp(): void
    {
        [$server, $addr] = self::listen();
        $client = new RemotingClient(1000, 3000, channelIdleTimeoutSeconds: 600);
        $conn = $this->connectOne($client, $addr, $server);

        $this->ageStamp($client, $addr, 300000.0);
        $client->invokeOneway($addr, RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT));
        $stamps = $this->stamps($client);
        $this->check($stamps[$addr] > $this->monoMillis() - 1000.0,
            'stamp leg: a completed write re-starts the idle clock');
        $this->checkSame(0, $client->sweepIdleConnections(),
            'stamp leg: so the connection the client just wrote on is not swept');

        $this->ageStamp($client, $addr, 300000.0);
        // A well-formed frame nobody asked for: the read alone has to re-start the clock.
        fwrite($conn, self::responseFor(424242)->encode());
        $this->captureStart();
        $client->pumpIncoming(300);
        $this->captureStop();
        $this->check($this->hasConn($client, $addr),
            'stamp leg: bytes off the socket keep the connection');
        $stamps = $this->stamps($client);
        $this->check($stamps[$addr] > $this->monoMillis() - 1000.0,
            'stamp leg: and the stamp really moved forward, not just the table');
        $this->checkSame(0, $client->sweepIdleConnections(),
            'stamp leg: the refreshed connection is not swept on the next pass');

        @fclose($conn);
        @fclose($server);
        $client->shutdown();
    }

    /** The limit is a widening knob, never an off switch. */
    private function testIdleLimitNeverDisablesTheSweep(): void
    {
        $default = (new RemotingClient(1000, 3000));
        $this->checkSame(RemotingClient::DEFAULT_CHANNEL_IDLE_TIMEOUT_SECONDS,
            $default->getChannelIdleTimeoutSeconds(),
            'idle default: an untouched client carries the 120 s default');
        $default->shutdown();

        foreach ([0, -1, -120] as $bad) {
            $client = new RemotingClient(1000, 3000, channelIdleTimeoutSeconds: $bad);
            $this->checkSame(RemotingClient::DEFAULT_CHANNEL_IDLE_TIMEOUT_SECONDS,
                $client->getChannelIdleTimeoutSeconds(),
                "idle default: a configured {$bad} s restores the default instead of disabling the sweep");
            $client->shutdown();
        }

        $client = new RemotingClient(1000, 3000);
        $client->setChannelIdleTimeoutSeconds(0);
        $this->checkSame(RemotingClient::DEFAULT_CHANNEL_IDLE_TIMEOUT_SECONDS,
            $client->getChannelIdleTimeoutSeconds(),
            'idle default: the setter clamps a non-positive value the same way');
        $client->setChannelIdleTimeoutSeconds(30);
        $this->checkSame(30, $client->getChannelIdleTimeoutSeconds(),
            'idle default: a positive value set later is honoured');
        $client->shutdown();
    }

    /** The sweep is driven from the two pumps, not only from the explicit call. */
    private function testPumpsDriveTheIdleSweep(): void
    {
        [$server, $addr] = self::listen();
        $client = new RemotingClient(1000, 3000, channelIdleTimeoutSeconds: 1);
        $conn = $this->connectOne($client, $addr, $server);
        $this->ageStamp($client, $addr, 5000.0);
        $client->pumpIncoming(50);
        $this->check(!$this->hasConn($client, $addr),
            'pump leg: pumpIncoming reaps an expired connection on entry');
        @fclose($conn);
        @fclose($server);

        [$server2, $addr2] = self::listen();
        $conn2 = $this->connectOne($client, $addr2, $server2);
        $this->ageStamp($client, $addr2, 5000.0);
        $client->waitResponses(50);
        $this->check(!$this->hasConn($client, $addr2),
            'pump leg: waitResponses reaps an expired connection too');
        @fclose($conn2);
        @fclose($server2);
        $client->shutdown();
    }

    // ==================================================================== T5

    /** One line per expired entry, and each caller's failure callback still fires exactly once. */
    private function testExpiredEntriesWarnOneEach(): void
    {
        [$server, $addr] = self::listen();
        $client = new RemotingClient(1000, 3000);
        $conn = $this->connectOne($client, $addr, $server);

        $fired = [];
        foreach ([21, 22] as $opaque) {
            $req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
            $req->opaque = $opaque;
            $client->invokeAsync($addr, $req,
                static function (RemotingCommand $resp) use (&$fired, $opaque): void { $fired[] = "ok-$opaque"; },
                static function (\Throwable $e) use (&$fired, $opaque): void { $fired[] = "fail-$opaque"; },
                100);
            $pending = $this->prop($client, 'pending');
            $pending[$opaque]->beginAt = $this->monoMillis() - 5000.0;
            $this->setProp($client, 'pending', $pending);
        }

        $this->captureStart();
        $this->sweepExpired($client);
        $this->captureStop();

        $warns = $this->loggedWith('expired');
        $this->checkSame(2, count($warns), 'expiry leg: one warning per reclaimed entry');
        $this->check(isset($warns[0]) && str_contains($warns[0], $addr),
            'expiry leg: the warning names the address the request went to');
        $this->checkSame(['fail-21', 'fail-22'], $fired,
            'expiry leg: each expired caller gets exactly one failure callback');
        $this->checkSame([], $this->prop($client, 'pending'), 'expiry leg: the table is left empty');
        $this->check($this->hasConn($client, $addr),
            'expiry leg: reclaiming a request is not a connection event -- the socket stays');

        $this->captureStart();
        $this->sweepExpired($client);
        $this->captureStop();
        $this->checkSame([], $this->logged,
            'expiry leg: a second sweep over an empty table logs nothing new');

        @fclose($conn);
        @fclose($server);
        $client->shutdown();
    }

    // ==================================================================== T6

    /**
     * One sync wait that runs out against a peer that accepted and never answered.
     *
     * @return array{threw: ?\Throwable, alive: bool, closeWarn: list<string>, timeoutWarn: list<string>, conn: resource, server: resource, client: RemotingClient}
     */
    private function syncTimeoutInto(bool $closeOnTimeout, int $timeoutMillis): array
    {
        [$server, $addr] = self::listen();
        $client = new RemotingClient(1000, 3000, closeChannelOnSyncTimeout: $closeOnTimeout);
        $conn = $this->connectOne($client, $addr, $server);
        $this->checkSame($closeOnTimeout, $client->isCloseChannelOnSyncTimeout(),
            'sync-timeout leg: the knob reads back as configured');

        $this->captureStart();
        $threw = null;
        try {
            $client->invokeSync($addr, RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT), $timeoutMillis);
        } catch (\Throwable $e) {
            $threw = $e;
        }
        $result = [
            'threw' => $threw,
            'alive' => $this->hasConn($client, $addr),
            'closeWarn' => $this->loggedWith('closing the connection'),
            'timeoutWarn' => $this->loggedWith('wait response from'),
            'conn' => $conn,
            'server' => $server,
            'client' => $client,
        ];
        $this->captureStop();
        return $result;
    }

    private function testSyncTimeoutClosesTheConnectionByDefault(): void
    {
        $r = $this->syncTimeoutInto(true, 300);
        $this->check($r['threw'] instanceof RemotingTimeoutException,
            'sync-timeout leg: the caller still gets the timeout it asked for');
        $this->checkSame(false, $r['alive'],
            'sync-timeout leg: by default the connection the wait ran out on is closed');
        $this->checkSame(1, count($r['closeWarn']), 'sync-timeout leg: the close is logged');
        $this->checkSame(1, count($r['timeoutWarn']), 'sync-timeout leg: and so is the timeout itself');
        @fclose($r['conn']);
        @fclose($r['server']);
        $r['client']->shutdown();
    }

    private function testSyncTimeoutKeepsTheConnectionWhenTurnedOff(): void
    {
        $r = $this->syncTimeoutInto(false, 300);
        $this->check($r['threw'] instanceof RemotingTimeoutException,
            'sync-timeout off leg: the timeout is still reported to the caller');
        $this->checkSame(true, $r['alive'],
            'sync-timeout off leg: with the knob off the socket is kept -- the timeout is a caller-side event');
        $this->checkSame(0, count($r['closeWarn']), 'sync-timeout off leg: no close is logged');
        $this->checkSame(1, count($r['timeoutWarn']), 'sync-timeout off leg: the timeout line still appears');
        @fclose($r['conn']);
        @fclose($r['server']);
        $r['client']->shutdown();
    }

    /**
     * The two floors of the close decision, on the only budget this port can hand the wait.
     *
     * This transport does not subtract the connect cost, so the time still available when the
     * wait began is the whole timeout. The floors then leave exactly one shape that keeps the
     * socket: a budget of zero, which no wait can ever be answered on.
     */
    private function testSyncTimeoutCloseRuleFloors(): void
    {
        $r = $this->syncTimeoutInto(true, 0);
        $this->check($r['threw'] instanceof RemotingTimeoutException,
            'floor leg: a zero budget still reports the timeout to the caller');
        $this->checkSame(true, $r['alive'],
            'floor leg: a zero budget is at both floors, so the connection is kept');
        $this->checkSame(0, count($r['closeWarn']), 'floor leg: and nothing is closed');
        @fclose($r['conn']);
        @fclose($r['server']);
        $r['client']->shutdown();

        $r2 = $this->syncTimeoutInto(true, 101);
        $this->checkSame(false, $r2['alive'],
            'floor leg: one millisecond above the floor is a close, not a keep');
        @fclose($r2['conn']);
        @fclose($r2['server']);
        $r2['client']->shutdown();
    }

    // ==================================================================== entry

    public function run(): int
    {
        $this->testUndecodableFrameClosesAndNamesThePeer();
        $this->testNextValidFrameIsServedOnAFreshConnection();
        $this->testBadFrameLengthTakesTheConnectionDown();
        $this->testUnmatchedResponseWarnsAndKeepsTheConnection();
        $this->testWritableIsTrueWithoutAConnection();
        $this->testWritableForALiveConnectionWithRoom();
        $this->testNotWritableWhileTheSendBufferIsFull();
        $this->testIdleSweepClosesQuietAndKeepsBusy();
        $this->testTrafficRefreshesTheIdleStamp();
        $this->testIdleLimitNeverDisablesTheSweep();
        $this->testPumpsDriveTheIdleSweep();
        $this->testExpiredEntriesWarnOneEach();
        $this->testSyncTimeoutClosesTheConnectionByDefault();
        $this->testSyncTimeoutKeepsTheConnectionWhenTurnedOff();
        $this->testSyncTimeoutCloseRuleFloors();
        return $this->summary();
    }
}

exit((new RunClientTransportContract())->run());
