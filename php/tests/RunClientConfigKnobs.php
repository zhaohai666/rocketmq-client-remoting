<?php

declare(strict_types=1);

/**
 * ClientConfig knob self-test (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientConfigKnobs.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Covers the three knobs whose effects only show up on the wire:
 *
 * 1. decodeReadBody -- the pull response is decoded body by body; the switch off leaves each
 *    message a header-only shell (queue offsets and properties survive, bodies do not) while
 *    the batch still comes up whole. Both directions sit next to each other in one run.
 * 2. mqClientApiTimeout -- what the heartbeat RPC is actually invoked with: the default 3000,
 *    a configured value, and an explicit argument that wins over both.
 * 3. enableConcurrentHeartbeat -- one round with the switch on dispatches through the worker
 *    pool, the serial control runs the same round without ever creating it.
 *
 * Every RPC here is answered by a subclass override of the instance's single synchronous exit
 * point, which also records each request's address, code and timeout -- the timeout assertions
 * read recorded state, never a clock.
 */

require_once __DIR__ . '/../bootstrap.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PullStatus;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;

/** Instance double that records every sync RPC and answers per script. */
final class KnobInstance extends MQClientInstance
{
    /** @var list<array{addr: string, code: int, timeout: ?int}> */
    public array $requests = [];

    /** @var callable(RemotingCommand, string): ?RemotingCommand|null */
    public $handler = null;

    protected function invokeSync(string $addr, RemotingCommand $request, ?int $timeoutMillis = null): RemotingCommand
    {
        $this->requests[] = ['addr' => $addr, 'code' => $request->code, 'timeout' => $timeoutMillis];
        $h = $this->handler;
        $resp = $h !== null ? $h($request, $addr) : null;
        return $resp ?? RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }

    /** Whether the concurrent-heartbeat worker pool exists (private field, read by reflection). */
    public function heartbeatPoolExists(): bool
    {
        $prop = new ReflectionProperty(MQClientInstance::class, 'heartbeatExecutor');
        $prop->setAccessible(true);
        return $prop->getValue($this) !== null;
    }

    /** Seeds the broker address table directly; the heartbeat round walks this table. */
    public function seedBrokerAddrs(array $addrs): void
    {
        $prop = new ReflectionProperty(MQClientInstance::class, 'brokerAddrTable');
        $prop->setAccessible(true);
        $prop->setValue($this, ['broker-0' => [0 => $addrs[0]]]);
    }
}

final class RunClientConfigKnobs
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

    // ================================================================ 1. decodeReadBody

    private function pullResponse(array $messages): RemotingCommand
    {
        $body = '';
        foreach ($messages as $m) {
            $body .= MessageDecoder::encodeMessageExt($m);
        }
        $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $resp->extFields['nextBeginOffset'] = '13';
        $resp->extFields['minOffset'] = '0';
        $resp->extFields['maxOffset'] = '13';
        $resp->body = $body;
        return $resp;
    }

    private function pullStream(): array
    {
        $messages = [];
        foreach ([10 => 'one', 11 => 'two', 12 => 'three'] as $offset => $body) {
            $m = new MessageExt(topic: 'KnobTopic', body: $body);
            $m->setQueueId(0);
            $m->setQueueOffset($offset);
            $m->setBornTimestamp(1_700_000_000_000);
            $m->setStoreTimestamp(1_700_000_000_000);
            $m->putProperty('TAGS', 'Tag' . ($offset - 9));
            $messages[] = $m;
        }
        return $messages;
    }

    private function testDecodeReadBody(): void
    {
        $inst = new KnobInstance('knob-decode-on-' . uniqid(), [], decodeReadBody: true);
        $inst->handler = fn (RemotingCommand $req): RemotingCommand => $this->pullResponse($this->pullStream());

        $result = $inst->pullMessage(
            'GID_knob', new MessageQueue('KnobTopic', 'broker-0', 0), 0, 32, 0, 0, null, 0, null,
            addr: '127.0.0.1:10911', timeoutMillis: 3000,
        );
        $this->checkSame(PullStatus::FOUND, $result->status, 'pull status=FOUND with the decode switch on');
        $this->checkSame(['one', 'two', 'three'], array_map(fn ($m) => $m->body, $result->msgFoundList),
            'bodies decoded with the switch on');
        $this->checkSame([10, 11, 12], array_map(fn ($m) => $m->getQueueOffset(), $result->msgFoundList),
            'queue offsets intact with the switch on');
        $this->checkSame(['Tag1', 'Tag2', 'Tag3'], array_map(fn ($m) => $m->getProperty('TAGS'), $result->msgFoundList),
            'properties intact with the switch on');

        $off = new KnobInstance('knob-decode-off-' . uniqid(), [], decodeReadBody: false);
        $off->handler = fn (RemotingCommand $req): RemotingCommand => $this->pullResponse($this->pullStream());
        $result = $off->pullMessage(
            'GID_knob', new MessageQueue('KnobTopic', 'broker-0', 0), 0, 32, 0, 0, null, 0, null,
            addr: '127.0.0.1:10911', timeoutMillis: 3000,
        );
        $this->checkSame(3, count($result->msgFoundList),
            'the batch still comes up whole with the switch off');
        $this->checkSame([null, null, null], array_map(fn ($m) => $m->body, $result->msgFoundList),
            'bodies are none with the switch off');
        $this->checkSame([10, 11, 12], array_map(fn ($m) => $m->getQueueOffset(), $result->msgFoundList),
            'queue offsets survive with the switch off');
        $this->checkSame(['Tag1', 'Tag2', 'Tag3'], array_map(fn ($m) => $m->getProperty('TAGS'), $result->msgFoundList),
            'properties survive with the switch off');
    }

    // ================================================================ 2. mqClientApiTimeout

    private function testMqClientApiTimeout(): void
    {
        $inst = new KnobInstance('knob-timeout-' . uniqid(), []);
        $inst->seedBrokerAddrs(['127.0.0.1:10911']);

        $hb = new HeartbeatData('knob-timeout-client');
        $inst->sendHeartbeat('127.0.0.1:10911', $hb);
        $this->checkSame(3000, $inst->requests[0]['timeout'],
            'the heartbeat runs under the default 3000 budget');

        $inst->setMqClientApiTimeout(1234);
        $inst->sendHeartbeat('127.0.0.1:10911', $hb);
        $this->checkSame(1234, $inst->requests[1]['timeout'],
            'the configured budget reaches the heartbeat RPC');

        $inst->sendHeartbeat('127.0.0.1:10911', $hb, 777);
        $this->checkSame(777, $inst->requests[2]['timeout'],
            'an explicit argument wins over the configured budget');

        // The round entry passes its argument through the same way.
        $inst->registerProducer('PG_knob');
        $inst->heartbeatOnce(555);
        $this->checkSame(555, $inst->requests[3]['timeout'],
            'heartbeatOnce hands its explicit budget to every beat');
    }

    // ================================================================ 3. enableConcurrentHeartbeat

    private function testEnableConcurrentHeartbeat(): void
    {
        // Control: the serial round beats the broker without creating the pool.
        $serial = new KnobInstance('knob-heartbeat-serial-' . uniqid(), []);
        $serial->seedBrokerAddrs(['127.0.0.1:10911']);
        $serial->registerProducer('PG_knob');
        $this->checkSame(1, $serial->heartbeatOnce(), 'serial round beats the broker once');
        $this->checkSame(false, $serial->heartbeatPoolExists(),
            'the serial round must not create the heartbeat pool');
        $this->checkSame([RequestCode::HEART_BEAT], array_map(fn ($r) => $r['code'], $serial->requests),
            'the serial round sent exactly one beat');

        // The concurrent round dispatches through the pool: reverting the dispatch
        // decision leaves the pool uncreated and this check red.
        $concurrent = new KnobInstance('knob-heartbeat-concurrent-' . uniqid(), []);
        $concurrent->seedBrokerAddrs(['127.0.0.1:10911']);
        $concurrent->registerProducer('PG_knob');
        $concurrent->setEnableConcurrentHeartbeat(true);
        $this->checkSame(1, $concurrent->heartbeatOnce(), 'concurrent round beats the broker once');
        $this->checkSame(true, $concurrent->heartbeatPoolExists(),
            'the concurrent round must dispatch through the heartbeat pool');
        $this->checkSame([RequestCode::HEART_BEAT], array_map(fn ($r) => $r['code'], $concurrent->requests),
            'the concurrent round sent exactly one beat');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testDecodeReadBody();
        $this->testMqClientApiTimeout();
        $this->testEnableConcurrentHeartbeat();
        return $this->summary();
    }
}

exit((new RunClientConfigKnobs())->run());
