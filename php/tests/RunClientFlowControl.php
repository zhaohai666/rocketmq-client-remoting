<?php

declare(strict_types=1);

/**
 * Push-consumer cache flow-control contract suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientFlowControl.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Three contracts are pinned here:
 *
 * 1. Cache gates compare **strictly greater than** their thresholds. A buffer that has reached a
 *    threshold exactly is still allowed to pull, so every gate test sits next to its boundary
 *    control: threshold messages must not trip, threshold+1 messages must.
 * 2. A trip arms a short pause on that queue -- 50 ms, this client's own backlog being the reason
 *    -- and the pause is honoured by the pull round: while it is unpaid the broker receives no pull
 *    request for that queue at all, and the request reappears the moment the pause is due. The
 *    queue with an empty buffer is the paired control: it pulls on the very same round, and a
 *    second queue keeps pulling while the first one sits in its own pause.
 * 3. A throttled queue trips on every round, so only the 1st trip warns while every trip keeps its
 *    debug line; both are read from the log handler, and the modulo itself is pinned through the
 *    predicate the production code calls.
 * 4. A gate judges **one queue's own buffer** and nothing else: the two topic-level budgets are not
 *    consulted at pull time (they are split into the per-queue knobs during rebalance), so the
 *    sibling queues of a topic never add up to a pause.
 *
 * The gates are driven by seeding the queue's cached messages and calling the gate directly, and a
 * pull round is driven one queue at a time against an instance double that records the requests it
 * is asked to send -- so no assertion depends on how fast a listener drains or on a clock running
 * past a sleep.
 */

require_once __DIR__ . '/../bootstrap.php';
// message_queue_key() lives in a functions-only include: require it rather than depend on some
// earlier class load pulling the file in by accident.
require_once __DIR__ . '/../src/Remoting/Protocol/AdminBodies.php';

// The suite emits debug and warn lines on purpose; keep the files out of the repository.
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_fc_logs_' . getmypid());
// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Logger;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PopResult;
use RocketMQ\Client\PopStatus;
use RocketMQ\Client\PullResult;
use RocketMQ\Client\PullStatus;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Remoting\Protocol\ResponseCode;
use function RocketMQ\Remoting\Protocol\message_queue_key;

/**
 * Instance double for the pull round: it records every pull this client asks for and answers each
 * one with a live two-message batch, so "the gate stopped the request" and "the request went out"
 * are both read from recorded state.
 */
final class FcInstance extends MQClientInstance
{
    /** @var list<array{topic:string,group:string,queueId:int,offset:int}> */
    public array $pulls = [];

    /** @var list<array{topic:string,group:string,queueId:int}> */
    public array $pops = [];

    /** When set, the next pull answers with this broker response code instead of FOUND. */
    public ?int $failNextWith = null;

    public function pullMessage(
        string $consumerGroup,
        MessageQueue $mq,
        int $queueOffset,
        int $maxMsgNums,
        int $sysFlag,
        int $commitOffset,
        ?string $subscription,
        int $subVersion,
        ?string $expressionType,
        int $timeoutMillis = 30000,
        int $maxMsgBytes = -1,
        int $suspendTimeoutMillis = 15000,
        ?string $addr = null,
        int $requestSource = 0,
        ?int $brokerId = null,
    ): PullResult {
        $this->pulls[] = [
            'topic' => $mq->topic,
            'group' => $consumerGroup,
            'queueId' => $mq->queueId,
            'offset' => $queueOffset,
        ];
        if ($this->failNextWith !== null) {
            $code = $this->failNextWith;
            $this->failNextWith = null;
            throw new MQBrokerException($code, 'broker says no');
        }
        $made = [];
        foreach ([0, 1] as $offset) {
            $m = new MessageExt(topic: $mq->topic, body: 'fc-' . $offset);
            $m->setQueueOffset($offset);
            $m->setStoreSize(1024);
            $made[] = $m;
        }
        return new PullResult(
            PullStatus::FOUND,
            nextBeginOffset: $queueOffset + 2,
            minOffset: 0,
            maxOffset: 100,
            msgFoundList: $made,
            suggestWhichBrokerId: MixAll::MASTER_ID,
        );
    }

    public function popMessage(
        string $consumerGroup,
        string $topic,
        int $queueId = -1,
        int $maxMsgNums = 32,
        int $invisibleTime = 60000,
        int $pollTime = 0,
        int $initMode = 0,
        ?string $exp = null,
        ?string $expType = null,
        bool $order = false,
        ?string $brokerName = null,
        int $timeoutMillis = 10000,
        ?string $addr = null,
    ): PopResult {
        $this->pops[] = ['topic' => $topic, 'group' => $consumerGroup, 'queueId' => $queueId];
        return new PopResult(PopStatus::POLLING_NOT_FOUND);
    }

    public function pullCount(): int
    {
        return count($this->pulls);
    }

    public function popCount(): int
    {
        return count($this->pops);
    }
}

final class RunClientFlowControl
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    /** @var list<string> */
    private array $captured = [];

    public function check(bool $cond, string $label): void
    {
        if ($cond) {
            $this->passed++;
        } else {
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$label}\n");
        }
    }

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

    // ---------------- reflection helpers ----------------

    private function prop(DefaultMQPushConsumer $c, string $name): mixed
    {
        $p = new \ReflectionProperty(DefaultMQPushConsumer::class, $name);
        $p->setAccessible(true);
        return $p->getValue($c);
    }

    private function setProp(DefaultMQPushConsumer $c, string $name, mixed $value): void
    {
        $p = new \ReflectionProperty(DefaultMQPushConsumer::class, $name);
        $p->setAccessible(true);
        $p->setValue($c, $value);
    }

    private function call(DefaultMQPushConsumer $c, string $name, array $args): mixed
    {
        $m = new \ReflectionMethod(DefaultMQPushConsumer::class, $name);
        $m->setAccessible(true);
        return $m->invokeArgs($c, $args);
    }

    // ---------------- message helpers ----------------

    /** @param list<int> $offsets */
    private function msgs(array $offsets, int $storeSize = 1024): array
    {
        $out = [];
        foreach ($offsets as $offset) {
            $m = new MessageExt(topic: 'GateTopic', body: 'x');
            $m->queueOffset = (int) $offset;
            $m->storeSize = $storeSize;
            $out[] = $m;
        }
        return $out;
    }

    /** Seed one queue's cached messages and ask the gate whether it would pause the pull. */
    private function hit(DefaultMQPushConsumer $c, MessageQueue $mq, array $messages): bool
    {
        $key = message_queue_key($mq);
        $this->setProp($c, 'pending', [$key => $messages]);
        return (bool) $this->call($c, 'flowControlHit', [$mq, $key]);
    }

    /**
     * Ask the gate about one queue while leaving the buffer state of every other queue untouched --
     * the only way to tell "this queue is full" apart from "this topic is full".
     */
    private function gate(DefaultMQPushConsumer $c, MessageQueue $mq, string $key): bool
    {
        return (bool) $this->call($c, 'flowControlHit', [$mq, $key]);
    }

    private function gateConsumer(string $group): DefaultMQPushConsumer
    {
        $c = new DefaultMQPushConsumer($group);
        $mq = new MessageQueue('GateTopic', 'b-fc', 0);
        $this->setProp($c, 'mqMap', [message_queue_key($mq) => $mq]);
        return $c;
    }

    /** Collect the lines the client logs from now on (both tiers pass; the handler replaces the sink). */
    private function captureLogs(int $level): void
    {
        Logger::setLevel($level);
        $this->captured = [];
        Logger::setHandler(function (string $line): void {
            $this->captured[] = $line;
        });
    }

    private function releaseLogs(): void
    {
        Logger::setHandler(null);
        Logger::setLevel(Logger::INFO);
    }

    /** @return list<string> captured lines carrying this log tier's marker */
    private function linesOf(string $tier): array
    {
        return array_values(array_filter(
            $this->captured,
            static fn(string $l): bool => str_contains($l, $tier . ':')
        ));
    }

    private function lastDebugContains(string $needle): bool
    {
        foreach (array_reverse($this->linesOf('DEBUG')) as $line) {
            if (str_contains($line, 'flow control')) {
                return str_contains($line, $needle);
            }
        }
        return false;
    }

    // ================================================================ 1. configuration surface

    /**
     * The numbers the gates and the pause are built from, asserted without touching the network:
     * the buffer thresholds, the POP wait-ack threshold the pull path shares the shape with, the
     * 50 ms own-cache pause and the once-per-1000 warn cadence.
     */
    private function testConfigurationSurface(): void
    {
        $c = new DefaultMQPushConsumer('GID-FC-CFG');
        $this->checkSame(1000, $c->pullThresholdForQueue, 'pullThresholdForQueue defaults to 1000');
        $this->checkSame(100, $c->pullThresholdSizeForQueue, 'pullThresholdSizeForQueue defaults to 100 MiB');
        $this->checkSame(2000, $c->consumeConcurrentlyMaxSpan, 'consumeConcurrentlyMaxSpan defaults to 2000');
        $this->checkSame(-1, $c->pullThresholdForTopic, 'pullThresholdForTopic defaults to unlimited');
        $this->checkSame(-1, $c->pullThresholdSizeForTopic, 'pullThresholdSizeForTopic defaults to unlimited');
        $this->checkSame(96, $c->popThresholdForQueue, 'popThresholdForQueue defaults to 96');
        $this->checkSame(0, $c->flowControlTriggeredCount(), 'a fresh consumer has tripped no gate');

        $consts = (new \ReflectionClass(DefaultMQPushConsumer::class))->getConstants();
        $this->checkSame(50, $consts['PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL'] ?? null,
            'own-cache pause is 50 ms');
        $this->checkSame(1000, $consts['FLOW_CONTROL_WARN_EVERY'] ?? null,
            'the warn cadence is every 1000th trip');
    }

    // ================================================================ 2. the count gate is strictly greater

    /**
     * A threshold reads as "how many messages may sit in the buffer": reaching it exactly still
     * pulls. Reverting the gate to a greater-or-equal comparison reddens the exactly-at-threshold
     * check, which is the paired control for the trip check right below it.
     */
    private function testCountGateIsStrictlyGreater(): void
    {
        $c = $this->gateConsumer('GID-FC-COUNT');
        $mq = new MessageQueue('GateTopic', 'b-fc', 0);
        $c->pullThresholdForQueue = 2;

        $this->checkSame(false, $this->hit($c, $mq, $this->msgs([0])), 'one message under threshold 2 pulls');
        $this->checkSame(false, $this->hit($c, $mq, $this->msgs([0, 1])),
            'exactly the threshold 2 is still allowed to pull (strictly greater)');
        $this->checkSame(true, $this->hit($c, $mq, $this->msgs([0, 1, 2])),
            'one message past threshold 2 trips the count gate');

        // The clamp keeps a zero threshold meaningful: it behaves like 1, so it trips on the
        // second message rather than on the first.
        $c->pullThresholdForQueue = 0;
        $this->checkSame(false, $this->hit($c, $mq, $this->msgs([0])),
            'a zero threshold clamps to one, so a single message still pulls');
        $this->checkSame(true, $this->hit($c, $mq, $this->msgs([0, 1])),
            'a zero threshold trips on the second message, not the first');
    }

    // ================================================================ 3. the size gate is in MiB and strictly greater

    private function testSizeGateIsStrictlyGreater(): void
    {
        $c = $this->gateConsumer('GID-FC-SIZE');
        $mq = new MessageQueue('GateTopic', 'b-fc', 0);
        $c->pullThresholdForQueue = 1000;
        $c->pullThresholdSizeForQueue = 1;
        $mib = 1024 * 1024;

        $this->checkSame(false, $this->hit($c, $mq, $this->msgs([0], $mib)),
            'exactly 1 MiB is still allowed under a 1 MiB threshold (strictly greater)');
        $this->checkSame(true, $this->hit($c, $mq, $this->msgs([0, 1], intdiv($mib, 2) + 1)),
            'one byte past 1 MiB trips the size gate');
        // Control: the same backlog under a disabled gate never trips.
        $c->pullThresholdSizeForQueue = 0;
        $this->checkSame(false, $this->hit($c, $mq, $this->msgs([0, 1, 2], 50 * $mib)),
            'a zero size threshold disables the gate');
    }

    // ================================================================ 4. the span gate is strictly greater

    private function testSpanGateIsStrictlyGreater(): void
    {
        $c = $this->gateConsumer('GID-FC-SPAN');
        $mq = new MessageQueue('GateTopic', 'b-fc', 0);
        $c->consumeConcurrentlyMaxSpan = 10;

        $this->checkSame(false, $this->hit($c, $mq, $this->msgs([0, 10])),
            'a span of exactly maxSpan 10 is allowed (strictly greater)');
        $this->checkSame(true, $this->hit($c, $mq, $this->msgs([0, 11])),
            'a span past maxSpan 10 trips the span gate');
        $this->checkSame(true, $this->hit($c, $mq, $this->msgs([11, 0, 4])),
            'the span is max minus min, whatever order the buffer holds');
        // Control: a huge span with the gate switched off.
        $c->consumeConcurrentlyMaxSpan = 0;
        $this->checkSame(false, $this->hit($c, $mq, $this->msgs([0, 1_000_000_000])),
            'a zero maxSpan disables the span gate');
    }

    // ================================================================ 5. which gate answers first

    /**
     * The three per-queue gates share one round, and the reason written to the log decides which
     * one is credited: count, then size, then span. Every case is a captured debug line, so the
     * precedence is read from recorded state rather than inferred.
     */
    private function testGatePrecedence(): void
    {
        $this->captureLogs(Logger::DEBUG);
        try {
            $mq = new MessageQueue('GateTopic', 'b-fc', 0);
            $mib = 1024 * 1024;

            $c = $this->gateConsumer('GID-FC-ORDER');
            $c->pullThresholdForQueue = 1;
            $c->pullThresholdSizeForQueue = 1;
            $c->consumeConcurrentlyMaxSpan = 1;
            $this->hit($c, $mq, $this->msgs([0, 1], 2 * $mib));
            $this->check($this->lastDebugContains('count=2'),
                'with every gate exceeded the count gate is the one credited');

            $c2 = $this->gateConsumer('GID-FC-ORDER2');
            $c2->pullThresholdForQueue = 1000;
            $c2->pullThresholdSizeForQueue = 1;
            $c2->consumeConcurrentlyMaxSpan = 1;
            $this->hit($c2, $mq, $this->msgs([0], 2 * $mib));
            $this->check($this->lastDebugContains('size=2.0MB'),
                'with only the size gate exceeded the size reason is credited');

            $c3 = $this->gateConsumer('GID-FC-ORDER3');
            $c3->pullThresholdForQueue = 1000;
            $c3->pullThresholdSizeForQueue = 100;
            $c3->consumeConcurrentlyMaxSpan = 10;
            $this->hit($c3, $mq, $this->msgs([0, 50], 1024));
            $this->check($this->lastDebugContains('span=50'),
                'with only the span gate exceeded the span reason is credited');

            // Control: a round where nothing is exceeded logs no flow-control line at all.
            $this->captured = [];
            $this->hit($c3, $mq, $this->msgs([0, 5], 1024));
            $this->checkSame([], $this->linesOf('DEBUG'), 'a round inside every threshold logs nothing');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 6. a trip arms the 50 ms pause

    /**
     * The pause is the second half of the contract: the trip must leave a due time on exactly the
     * queue that tripped, 50 ms out, and a round that does not trip must leave no pause armed.
     */
    private function testTripArmsThePause(): void
    {
        $c = $this->gateConsumer('GID-FC-PAUSE');
        $mq = new MessageQueue('GateTopic', 'b-fc', 0);
        $key = message_queue_key($mq);
        $c->pullThresholdForQueue = 1;

        $before = microtime(true);
        $this->checkSame(true, $this->hit($c, $mq, $this->msgs([0, 1])), 'the count gate trips');
        $until = (array) $this->prop($c, 'flowControlUntil');
        $this->check(isset($until[$key]), 'the tripped queue carries a pause deadline');
        $window = (float) ($until[$key] ?? 0.0) - $before;
        $this->check(abs($window - 0.05) < 0.01, "the pause is 50 ms, measured {$window}s");
        $this->checkSame(1, $c->flowControlTriggeredCount(), 'one trip, one counted flow-control hit');

        // Control: the same queue one message lighter arms nothing and counts nothing.
        $c2 = $this->gateConsumer('GID-FC-PAUSE2');
        $c2->pullThresholdForQueue = 1;
        $this->checkSame(false, $this->hit($c2, $mq, $this->msgs([0])), 'under the threshold the gate passes');
        $this->checkSame([], (array) $this->prop($c2, 'flowControlUntil'),
            'a round that does not trip arms no pause');
        $this->checkSame(0, $c2->flowControlTriggeredCount(), 'a round that does not trip counts nothing');
    }

    // ================================================================ 7. the warn cadence

    /**
     * A throttled queue trips on every round, so the warn is throttled too: the 1st trip and then
     * every 1000th. The modulo is pinned through the predicate itself, and the wiring is pinned by
     * driving the gate 1000 and 1001 times and counting the warn lines the client emits.
     */
    private function testWarnCadence(): void
    {
        $this->checkSame(true, DefaultMQPushConsumer::isFlowControlWarnTrip(1), 'the first trip warns');
        $this->checkSame(false, DefaultMQPushConsumer::isFlowControlWarnTrip(2), 'the second trip stays quiet');
        $this->checkSame(false, DefaultMQPushConsumer::isFlowControlWarnTrip(1000), 'trip 1000 stays quiet');
        $this->checkSame(true, DefaultMQPushConsumer::isFlowControlWarnTrip(1001), 'trip 1001 warns again');
        $this->checkSame(true, DefaultMQPushConsumer::isFlowControlWarnTrip(2001), 'trip 2001 warns again');
        $this->checkSame(false, DefaultMQPushConsumer::isFlowControlWarnTrip(2000), 'trip 2000 stays quiet');

        $this->captureLogs(Logger::DEBUG);
        try {
            $c = $this->gateConsumer('GID-FC-WARN');
            $mq = new MessageQueue('GateTopic', 'b-fc', 0);
            $c->pullThresholdForQueue = 1;
            for ($i = 0; $i < 1000; $i++) {
                $this->hit($c, $mq, $this->msgs([0, 1]));
            }
            $this->checkSame(1000, $c->flowControlTriggeredCount(), 'the gate ran 1000 rounds');
            $this->checkSame(1, count($this->linesOf('WARNING')),
                '1000 trips leave exactly one warn line (the first)');
            $this->checkSame(1000, count($this->linesOf('DEBUG')),
                'every one of the 1000 trips keeps its debug line');

            $this->captured = [];
            $this->hit($c, $mq, $this->msgs([0, 1]));
            $warns = $this->linesOf('WARNING');
            $this->checkSame(1, count($warns), 'the 1001st trip is the next warn');
            $this->check(isset($warns[0]) && str_contains($warns[0], 'flowControlTimes=1001'),
                'the warn carries the trip counter it fired on');
            $this->checkSame(1, count($this->linesOf('DEBUG')),
                'the 1001st trip still logs a debug line');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 8. the pause on the pull round

    /**
     * A consumer wired to the instance double, holding the given queues with a resolved offset: the
     * pull round is then driven one call at a time, so a round is decided by the client's own gate
     * and pause table rather than by a rebalance or a clock.
     *
     * @return array{0:DefaultMQPushConsumer,1:MessageQueue,2:string}
     */
    private function pullConsumer(FcInstance $inst, string $group, string $topic, int ...$queueIds): array
    {
        $c = new DefaultMQPushConsumer($group);
        $c->subscribe($topic);
        $this->setProp($c, 'started', true);
        $c->mqClient = $inst;
        $mqMap = [];
        $offsets = [];
        foreach ($queueIds as $queueId) {
            $mq = new MessageQueue($topic, 'b-fc', $queueId);
            $mqMap[message_queue_key($mq)] = $mq;
            $offsets[message_queue_key($mq)] = 100;
        }
        $this->setProp($c, 'mqMap', $mqMap);
        $this->setProp($c, 'offsetTable', $offsets);
        $keys = array_keys($mqMap);
        return [$c, $mqMap[$keys[0]], (string) $keys[0]];
    }

    /** Put one queue into the buffer state a pull round should start from, leaving pauses alone. */
    private function setPending(DefaultMQPushConsumer $c, string $key, array $pending): void
    {
        $all = (array) $this->prop($c, 'pending');
        $all[$key] = $pending;
        $this->setProp($c, 'pending', $all);
        $this->setProp($c, 'idleUntil', []);
        $this->setProp($c, 'suspendedUntil', []);
    }

    /** The same, with the queue's pause deadline set (or cleared) explicitly. */
    private function seed(DefaultMQPushConsumer $c, string $key, array $pending, ?float $pauseUntil): void
    {
        $this->setPending($c, $key, $pending);
        $this->setProp($c, 'flowControlUntil', $pauseUntil === null ? [] : [$key => $pauseUntil]);
    }

    private function pullOnce(DefaultMQPushConsumer $c, MessageQueue $mq, string $key, float $now): bool
    {
        return (bool) $this->call($c, 'pullOnce', [$mq, $key, false, $now]);
    }

    /**
     * The full pause contract on the pull round: a buffer past its threshold blocks the request
     * before it leaves, an unpaid pause blocks it too (with the buffer already drained, so only the
     * pause can be what stopped it), and the deadline itself lets exactly one request through.
     */
    private function testPauseBlocksThePull(): void
    {
        $inst = new FcInstance('fc-wire-' . uniqid(), []);
        [$c, $mq, $key] = $this->pullConsumer($inst, 'GID-FC-WIRE', 'FCPause', 0);
        $c->pullThresholdForQueue = 1;
        $c->pullThresholdSizeForQueue = 100;

        // 1) Buffer past the threshold: the gate stops the round and arms the pause.
        $this->seed($c, $key, $this->msgs([0, 1]), null);
        $this->checkSame(false, $this->pullOnce($c, $mq, $key, microtime(true)),
            'a full buffer stops the round');
        $this->checkSame(0, $inst->pullCount(), 'a stopped round sends no pull request at all');
        $until = (array) $this->prop($c, 'flowControlUntil');
        $this->check(isset($until[$key]), 'the stopped queue armed a pause');
        $this->checkSame(1, $c->flowControlTriggeredCount(), 'the stopped round counted one trip');
        $due = (float) $until[$key];

        // 2) Buffer drained, pause unpaid: only the pause can be what stops this round, and it
        //    still sends nothing.
        $this->seed($c, $key, [], $due);
        $this->checkSame(false, $this->pullOnce($c, $mq, $key, $due - 0.001),
            'a queue inside its 50 ms pause is skipped even with an empty buffer');
        $this->checkSame(0, $inst->pullCount(), 'the unpaid pause sends no pull request');

        // 3) The deadline itself opens the gate again: the pull is re-scheduled at 50 ms, not parked.
        $this->seed($c, $key, [], $due);
        $this->checkSame(true, $this->pullOnce($c, $mq, $key, $due),
            'once the pause is due the queue pulls again');
        $this->checkSame(
            [['topic' => 'FCPause', 'group' => 'GID-FC-WIRE', 'queueId' => 0, 'offset' => 100]],
            $inst->pulls,
            'the resumed round sent exactly one real pull request, from the queue offset',
        );
    }

    /**
     * Paired control for the leg above: the same driven round, an empty buffer and no armed pause
     * -- so the request the blocked queue never sent is what this one sends.
     */
    private function testQueueWithoutPausePulls(): void
    {
        $inst = new FcInstance('fc-ctrl-' . uniqid(), []);
        [$c, $mq, $key] = $this->pullConsumer($inst, 'GID-FC-CTRL', 'FCControl', 0);
        $c->pullThresholdForQueue = 1;
        $this->seed($c, $key, [], null);
        $this->checkSame(true, $this->pullOnce($c, $mq, $key, microtime(true)),
            'a quiet queue pulls on the same round');
        $this->checkSame(1, $inst->pullCount(), 'the control queue sent exactly one pull request');
        $this->checkSame([], (array) $this->prop($c, 'flowControlUntil'), 'a round that pulls arms no pause');
        $this->checkSame(0, $c->flowControlTriggeredCount(), 'a round that pulls trips no gate');
    }

    /**
     * The pause belongs to one queue: a second queue of the same consumer keeps pulling while the
     * first sits in its own pause, which is what a per-queue re-scheduled request looks like.
     */
    private function testPauseIsPerQueue(): void
    {
        $inst = new FcInstance('fc-pq-' . uniqid(), []);
        [$c, $busy, $busyKey] = $this->pullConsumer($inst, 'GID-FC-PQ', 'FCPerQueue', 0, 1);
        $c->pullThresholdForQueue = 1;
        $idleMq = new MessageQueue('FCPerQueue', 'b-fc', 1);
        $idleKey = message_queue_key($idleMq);

        $this->seed($c, $busyKey, $this->msgs([0, 1]), null);
        $this->checkSame(false, $this->pullOnce($c, $busy, $busyKey, microtime(true)),
            'the busy queue is stopped by its own buffer');
        $until = (array) $this->prop($c, 'flowControlUntil');
        $this->check(isset($until[$busyKey]), 'the busy queue armed its own pause');
        $this->check(!isset($until[$idleKey]), 'the idle queue armed no pause');

        $insidePause = (float) ($until[$busyKey] ?? microtime(true)) - 0.001;
        $this->setPending($c, $idleKey, []);
        $this->checkSame(false, $this->pullOnce($c, $busy, $busyKey, $insidePause),
            'the busy queue is still inside its pause');
        $this->checkSame(true, $this->pullOnce($c, $idleMq, $idleKey, $insidePause),
            'the idle queue pulls while the other one is paused');
        $this->checkSame(1, $inst->pullCount(), 'exactly one request reached the broker');
        $this->checkSame([1], array_column($inst->pulls, 'queueId'),
            'the one request that went out carried the idle queue');
    }

    // ================================================================ 9. the broker's own pause

    /**
     * FLOW_CONTROL(215) is the broker being busy, not this client's cache being full, so its retry
     * is shorter than the backoff for every other failed request. The mapping is pinned as a pure
     * value, and the wiring by driving one real round whose request the double answers with that
     * code and reading the deadline the client armed.
     */
    private function testBrokerBackoffMapping(): void
    {
        $this->checkSame(215, ResponseCode::FLOW_CONTROL, 'the wire code the branch keys on');
        $this->checkSame(20, DefaultMQPushConsumer::PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
            'the broker pause is 20 ms');
        $this->checkSame(50, DefaultMQPushConsumer::PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL,
            'the own-cache pause is 50 ms');
        $this->checkSame(500, DefaultMQPushConsumer::PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION,
            'the general error pause is 500 ms');
        $this->checkSame(20, DefaultMQPushConsumer::pullBackoffMillisForResponseCode(215),
            'a 215 answer retries early');
        // Paired control: only 215 gets the early retry, so collapsing the branch onto one number
        // (or keying it on the wrong code) reddens this.
        foreach ([1, 2, 24, 25, 26, 500, 3000, null] as $code) {
            $this->checkSame(500, DefaultMQPushConsumer::pullBackoffMillisForResponseCode($code),
                'every other outcome (' . var_export($code, true) . ') keeps the general pause');
        }
    }

    /** Drive one round whose request the broker answers with $code, and read back the deadline. */
    private function brokerBackoffRound(string $group, int $code): float
    {
        $inst = new FcInstance('fc-bk-' . uniqid(), []);
        [$c, $mq, $key] = $this->pullConsumer($inst, $group, 'FCBroker', 0);
        $this->seed($c, $key, [], null);
        $inst->failNextWith = $code;
        $before = microtime(true);
        $this->checkSame(false, $this->pullOnce($c, $mq, $key, $before),
            "a $code answer does not hand the round any message");
        $this->checkSame(1, $inst->pullCount(), "the round really asked the broker ($code)");
        $idle = (array) $this->prop($c, 'idleUntil');
        $this->check(isset($idle[$key]), "the $code round armed a pause on its own queue");
        $delta = (float) $idle[$key] - $before;
        // Still inside the pause: the next round must send nothing.
        $this->checkSame(false, $this->pullOnce($c, $mq, $key, $before + $delta / 2),
            "a queue inside the $code pause is skipped");
        $this->checkSame(1, $inst->pullCount(), "the unpaid $code pause keeps the request off the wire");
        return $delta;
    }

    private function testBrokerBackoffArmsThePause(): void
    {
        $throttled = $this->brokerBackoffRound('GID-FC-BROKER-215', ResponseCode::FLOW_CONTROL);
        $this->check($throttled > 0.015 && $throttled < 0.1,
            'a 215 answer pauses for ~20 ms, not for the general 500 ms (' . $throttled . ')');

        // Paired control: any other broker error keeps the long backoff, so a branch that answers
        // 20 ms for everything is caught here rather than above.
        $other = $this->brokerBackoffRound('GID-FC-BROKER-25', ResponseCode::SYSTEM_ERROR);
        $this->check($other > 0.4 && $other < 0.8,
            'any other broker error keeps the ~500 ms backoff (' . $other . ')');
    }

    // ================================================================ 10. the POP gate on the wire

    /**
     * The POP wait-ack gate is the same event as a throttled pull -- this client's own backlog --
     * so it feeds the same counter, arms the same 50 ms pause and is honoured by the next pop round.
     *
     * @return array{0:DefaultMQPushConsumer,1:MessageQueue,2:string,3:FcInstance}
     */
    private function popConsumer(FcInstance $inst, string $group, string $topic, int $waitAck): array
    {
        $c = new DefaultMQPushConsumer($group);
        $c->subscribe($topic);
        $this->setProp($c, 'started', true);
        $c->mqClient = $inst;
        $mq = new MessageQueue($topic, 'b-fc', 0);
        $key = message_queue_key($mq);
        $pq = new \RocketMQ\Client\PopProcessQueue();
        $pq->incFoundMsg($waitAck);
        $this->setProp($c, 'mqMap', [$key => $mq]);
        $this->setProp($c, 'popQueues', [$key => $pq]);
        $this->setProp($c, 'subscriptionData', [
            $topic => new \RocketMQ\Common\SubscriptionData($topic, '*'),
        ]);
        return [$c, $mq, $key, $inst];
    }

    private function testPopGateArmsThePause(): void
    {
        $inst = new FcInstance('fc-pop-' . uniqid(), []);
        [$c, $mq, $key] = $this->popConsumer($inst, 'GID-FC-POP', 'FCPop', 2);
        $c->popThresholdForQueue = 1;
        $before = microtime(true);
        $this->call($c, 'popOnce', [$mq, $key, $before]);
        $this->checkSame(0, $inst->popCount(), 'a throttled pop round sends no pop request at all');
        $this->checkSame(1, $c->flowControlTriggeredCount(),
            'the POP gate feeds the same flow-control counter as the pull gates');
        $until = (array) $this->prop($c, 'flowControlUntil');
        $this->check(isset($until[$key]), 'the POP gate armed the shared pause');
        $delta = (float) $until[$key] - $before;
        $this->check($delta > 0.015 && $delta < 0.1,
            'the POP gate pauses with the cache-flow-control 50 ms (' . $delta . ')');

        // The pause holds the next round back even after the debt is gone: only the pause stops it.
        // popOnce reads the live clock, so the deadline is seeded rather than simulated by an argument.
        $this->setProp($c, 'popQueues', [$key => new \RocketMQ\Client\PopProcessQueue()]);
        $this->setProp($c, 'flowControlUntil', [$key => microtime(true) + 5.0]);
        $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);
        $this->checkSame(0, $inst->popCount(), 'an unpaid pause skips the queue with no wait-ack debt');

        $this->setProp($c, 'flowControlUntil', [$key => microtime(true) - 0.001]);
        $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);
        $this->checkSame(1, $inst->popCount(), 'once the pause is paid the queue pops again');

        // Paired control: the same driven round with the debt exactly at the threshold pops right away.
        $inst2 = new FcInstance('fc-pop-ok-' . uniqid(), []);
        [$c2, $mq2, $key2] = $this->popConsumer($inst2, 'GID-FC-POP-OK', 'FCPopOk', 1);
        $c2->popThresholdForQueue = 1;
        $this->call($c2, 'popOnce', [$mq2, $key2, microtime(true)]);
        $this->checkSame(1, $inst2->popCount(),
            'a debt of exactly the threshold is still allowed to pop (>, not >=)');
        $this->checkSame(0, $c2->flowControlTriggeredCount(), 'a round that does not trip counts nothing');
    }

    // ================================================================ 11. the gate never sums siblings

    /**
     * A topic budget is split over the owned queues at rebalance, so the pull-time gate reads ONLY
     * the queue's own buffer. Four queues holding the divided share of a 1000-message budget (250
     * each, 1000 in total across the topic) therefore all keep pulling, and one queue at 251 pauses
     * on its own count gate while its three siblings at 250 do not.
     *
     * Reverting the gate to a pull-time sum of the topic's queues reddens the first leg (the four
     * buffers reach the topic budget exactly) and the closing control leg (the same 1000 messages
     * under a wide-open per-queue gate, which the removed `topicCount`/`topicSize` branch would pause
     * on every one of the four queues).
     */
    private function testGateNeverSumsTheQueuesOfATopic(): void
    {
        $this->captureLogs(Logger::DEBUG);
        try {
            $mqMap = [];
            $pending = [];
            $held = [];
            foreach ([0, 1, 2, 3] as $i) {
                $mq = new MessageQueue('FCSiblings', 'b-fc', $i);
                $key = message_queue_key($mq);
                $mqMap[$key] = $mq;
                $pending[$key] = $this->msgs(range(0, 249));
                $held[$i] = [$mq, $key];
            }

            // The knobs exactly as the rebalance division leaves them for a 1000-message budget over
            // four queues; both topic knobs stay set, because the gate must not read them any more.
            $c = $this->siblingConsumer('GID-FC-NO-TOPIC-SUM', $mqMap, $pending, 250, 25);
            $this->checkSame(1000, array_sum(array_map('count', $pending)),
                'the topic really holds all 1000 cached messages, spread over four queues');

            foreach ($held as $i => [$mq, $key]) {
                $this->checkSame(false, $this->gate($c, $mq, $key),
                    "queue $i at its divided share of 250 keeps pulling: the gate judges this queue alone");
            }
            $this->checkSame(0, $c->flowControlTriggeredCount(),
                'the whole topic budget cached across four queues trips no gate at all');

            // One queue one message past its share pauses, on its own count ...
            $pending[$held[1][1]] = $this->msgs(range(0, 250));
            $this->setProp($c, 'pending', $pending);
            $this->checkSame(true, $this->gate($c, $held[1][0], $held[1][1]),
                'a queue one message past its divided share pauses');
            $this->check($this->lastDebugContains('count=251'),
                'the paused queue is paused by its own 251, not by a topic total of 1001');

            // ... while the three siblings still at their share keep pulling.
            foreach ([0, 2, 3] as $i) {
                $this->checkSame(false, $this->gate($c, $held[$i][0], $held[$i][1]),
                    "queue $i stayed at its share, and a full sibling does not spread to it");
            }
            $this->checkSame(1, $c->flowControlTriggeredCount(),
                'exactly one queue tripped, so no sibling total was ever consulted');

            // Paired control: the same four 250-message buffers with the per-queue gate wide open and
            // both topic budgets still enabled. Nothing is summed at pull time, so nothing trips --
            // the removed aggregation branch would have paused all four queues here.
            $open = $this->siblingConsumer(
                'GID-FC-NO-TOPIC-SUM-OPEN', $mqMap, $pending, 100000, 0);
            foreach ($held as $i => [$mq, $key]) {
                $this->checkSame(false, $this->gate($open, $mq, $key),
                    "queue $i pulls with a 1000-message topic budget and no summing ($i)");
            }
            $this->checkSame(0, $open->flowControlTriggeredCount(),
                'an enabled topic knob alone cannot pause a queue that is inside its own gate');
        } finally {
            $this->releaseLogs();
        }
    }

    /** A consumer owning four queues of one topic, each buffering 250 messages. */
    private function siblingConsumer(
        string $group,
        array $mqMap,
        array $pending,
        int $countGate,
        int $sizeGate,
    ): DefaultMQPushConsumer {
        $c = new DefaultMQPushConsumer($group);
        $c->pullThresholdForTopic = 1000;
        $c->pullThresholdSizeForTopic = 100;
        $c->pullThresholdForQueue = $countGate;
        $c->pullThresholdSizeForQueue = $sizeGate;
        // The span gate is switched off so that only the message count can ever be the reason here.
        $c->consumeConcurrentlyMaxSpan = 0;
        $this->setProp($c, 'mqMap', $mqMap);
        $this->setProp($c, 'pending', $pending);
        return $c;
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testConfigurationSurface();
        $this->testCountGateIsStrictlyGreater();
        $this->testSizeGateIsStrictlyGreater();
        $this->testSpanGateIsStrictlyGreater();
        $this->testGatePrecedence();
        $this->testTripArmsThePause();
        $this->testWarnCadence();
        $this->testPauseBlocksThePull();
        $this->testQueueWithoutPausePulls();
        $this->testPauseIsPerQueue();
        $this->testBrokerBackoffMapping();
        $this->testBrokerBackoffArmsThePause();
        $this->testPopGateArmsThePause();
        $this->testGateNeverSumsTheQueuesOfATopic();
        return $this->summary();
    }
}

exit((new RunClientFlowControl())->run());
