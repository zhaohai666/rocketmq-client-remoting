<?php

declare(strict_types=1);

/**
 * Channel-active re-registration contract (no phpunit, no real cluster).
 *
 * Run: php tests/RunChannelActiveReRegistration.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Two layers, measured separately:
 *
 * 1. The transport fires a registered listener once per NEW connection -- a reused connection
 *    fires nothing and a torn-down one fires again on the next dial. A listener that throws
 *    neither breaks the invoke nor the listeners registered after it; it is logged.
 *
 * 2. The instance re-registers every group it owns when a connection to a known broker comes
 *    up: the beat carries the group and wakes the consumer's rebalance. An unknown address, an
 *    empty consumer table and the switch off all make it a no-op.
 */

require_once __DIR__ . '/../bootstrap.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Logger;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\TopicRouteData;
use RocketMQ\Remoting\RemotingClient;

/** Instance double: records every sync RPC and seeds the route table directly. */
final class ChannelActiveInstance extends MQClientInstance
{
    /** @var list<array{addr: string, code: int}> */
    public array $requests = [];

    protected function invokeSync(string $addr, RemotingCommand $request, ?int $timeoutMillis = null): RemotingCommand
    {
        $this->requests[] = ['addr' => $addr, 'code' => $request->code];
        return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }

    public function seedRoute(string $brokerName, string $addr): void
    {
        $route = new TopicRouteData();
        $route->queueDatas = [new QueueData($brokerName, 1, 1, PermName::PERM_READ | PermName::PERM_WRITE, 0)];
        $route->brokerDatas = [new BrokerData('DefaultCluster', $brokerName, [MixAll::MASTER_ID => $addr])];
        $this->topicRouteTable['Tt'] = $route;
    }
}

final class RunChannelActiveReRegistration
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        if ($expected === $actual) {
            $this->passed++;
        } else {
            $detail = sprintf("%s\n  expected: %s\n  actual:   %s", $label, var_export($expected, true), var_export($actual, true));
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$detail}\n");
        }
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

    /** @return array{0: resource, 1: string} a listening socket that never answers, and its address */
    private function silentPeer(): array
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            throw new RuntimeException("cannot listen: {$errstr}");
        }
        return [$server, (string) stream_socket_get_name($server, false)];
    }

    private function heartbeatRequest(): RemotingCommand
    {
        return RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT, null);
    }

    // ================================================================ transport

    private function testTransportPerConnection(): void
    {
        [$server, $addr] = $this->silentPeer();
        // A timed-out sync must keep the socket, so the second invoke reuses it on purpose.
        $rc = new RemotingClient(invokeTimeoutMillis: 150, closeChannelOnSyncTimeout: false);
        $events = [];
        $rc->registerChannelListener(static function (string $seen) use (&$events): void {
            $events[] = $seen;
        });
        try {
            try {
                $rc->invokeSync($addr, $this->heartbeatRequest(), 150);
            } catch (Throwable $e) {
                // The peer never answers; the timeout is the expected outcome.
            }
            $this->checkSame([$addr], $events, 'a new connection fires the listener once, with its address');

            try {
                $rc->invokeSync($addr, $this->heartbeatRequest(), 150);
            } catch (Throwable $e) {
                // Same timeout, same live socket.
            }
            $this->checkSame(1, count($events), 'reusing the connection fires nothing');

            $rc->closeChannel($addr);
            try {
                $rc->invokeSync($addr, $this->heartbeatRequest(), 150);
            } catch (Throwable $e) {
                // The fresh dial times out as well.
            }
            $this->checkSame(2, count($events), 'the next dial fires again');
            $this->checkSame($addr, $events[1] ?? null, 'the new event carries the same address');
        } finally {
            $rc->shutdown();
            @fclose($server);
        }
    }

    private function testThrowingListenerIsContained(): void
    {
        [$server, $addr] = $this->silentPeer();
        $rc = new RemotingClient(invokeTimeoutMillis: 150, closeChannelOnSyncTimeout: false);
        $lines = [];
        Logger::setLevel(Logger::DEBUG);
        Logger::setHandler(static function (string $line) use (&$lines): void {
            $lines[] = $line;
        });
        $seen = 0;
        $rc->registerChannelListener(static function (string $unused): void {
            throw new RuntimeException('listener blew up');
        });
        $rc->registerChannelListener(static function (string $unused) use (&$seen): void {
            $seen++;
        });
        try {
            try {
                $rc->invokeSync($addr, $this->heartbeatRequest(), 150);
            } catch (Throwable $e) {
                // The peer never answers; the listener must not have broken this path either way.
            }
            $this->checkSame(1, $seen, 'the listener after the throwing one still runs');
            $warns = array_values(array_filter($lines, static fn (string $l): bool => str_contains($l, 'channel listener')));
            $this->checkSame(1, count($warns), 'the failure is logged once');
            $this->checkSame(true, isset($warns[0]) && str_contains((string) $warns[0], $addr), 'the log names the address');
            $this->checkSame(true, isset($warns[0]) && str_contains((string) $warns[0], 'listener blew up'), 'the log carries the listener error');
        } finally {
            Logger::setHandler(null);
            $rc->shutdown();
            @fclose($server);
        }
    }

    // ================================================================ instance

    /** @return bool the consumer's pending-rebalance flag */
    private function rebalanceFlag(DefaultMQPushConsumer $consumer): bool
    {
        $prop = new ReflectionProperty(DefaultMQPushConsumer::class, 'rebalanceNow');
        $prop->setAccessible(true);
        return (bool) $prop->getValue($consumer);
    }

    private function clearRebalanceFlag(DefaultMQPushConsumer $consumer): void
    {
        $prop = new ReflectionProperty(DefaultMQPushConsumer::class, 'rebalanceNow');
        $prop->setAccessible(true);
        $prop->setValue($consumer, false);
    }

    private function testInstanceReRegistration(): void
    {
        $brokerAddr = '10.0.0.1:10911';
        $inst = new ChannelActiveInstance('cid@channel-active', ['127.0.0.1:1'], heartbeatIntervalMillis: 60_000);
        $inst->seedRoute('b1', $brokerAddr);
        $consumer = new DefaultMQPushConsumer('GID_Channel');
        $consumer->subscribe('Tt', '*');
        $inst->registerConsumer('GID_Channel', $consumer);

        $this->checkSame(true, $inst->isEnableHeartbeatChannelEventListener(), 'the switch defaults to on');

        $inst->onChannelActive($brokerAddr);
        $this->checkSame(1, count($inst->requests), 'a known broker gets exactly one re-registration beat');
        $this->checkSame($brokerAddr, $inst->requests[0]['addr'] ?? null, 'the beat goes to the address that came up');
        $this->checkSame(RequestCode::HEART_BEAT, $inst->requests[0]['code'] ?? null, 'the re-registration is a HEART_BEAT');
        $this->checkSame(true, $this->rebalanceFlag($consumer), 'the re-registration wakes rebalance');

        $inst->onChannelActive('127.0.0.1:1');
        $this->checkSame(1, count($inst->requests), 'an address that is not a known broker re-registers nothing');
        $this->checkSame(true, $this->rebalanceFlag($consumer), 'and leaves the already-woken rebalance alone');

        $this->clearRebalanceFlag($consumer);
        $inst->setEnableHeartbeatChannelEventListener(false);
        $inst->onChannelActive($brokerAddr);
        $this->checkSame(1, count($inst->requests), 'the switch off suppresses a later event');
        $this->checkSame(false, $this->rebalanceFlag($consumer), 'and wakes no rebalance');

        $inst->setEnableHeartbeatChannelEventListener(true);
        $inst->onChannelActive($brokerAddr);
        $this->checkSame(2, count($inst->requests), 'the switch back on beats again');
        $this->checkSame(true, $this->rebalanceFlag($consumer), 'and wakes rebalance again');

        $inst->unregisterConsumer('GID_Channel');
        $inst->onChannelActive($brokerAddr);
        $this->checkSame(2, count($inst->requests), 'an empty consumer table re-registers nothing');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testTransportPerConnection();
        $this->testThrowingListenerIsContained();
        $this->testInstanceReRegistration();
        return $this->summary();
    }
}

exit((new RunChannelActiveReRegistration())->run());
