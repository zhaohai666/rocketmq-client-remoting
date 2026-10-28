<?php

declare(strict_types=1);

/**
 * Push-consumer cache flow-control contract suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientFlowControl.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * These contracts are pinned here:
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
 * 5. The POP wait-ack gate is the **same event** as a throttled pull, so it feeds the same counter,
 *    arms the same pause and warns on the same cadence -- only its log wording differs, on both
 *    tiers: the wait-ack debt as a count, the pop threshold it breached and the queue's current
 *    offset, ending in the pause of the pop request. The pull texts the pull suites assert on are
 *    asserted whole next to them and stay untouched.
 * 6. The **lite pull** path has the same contract on its own cache, with one difference that is
 *    deliberate: its four gates judge per-queue accumulators the single global buffer maintains
 *    (aggregate count, this queue's count, this queue's stored MiB, this queue's offset span), all
 *    strictly greater, and the thresholds are plain budgets -- no clamping, so a threshold of 0
 *    trips on the first cached message. Its refusals arm the same 50 ms per-queue pause, feed three
 *    counters of their own on the same warn cadence, and a pull answered FLOW_CONTROL(215) re-schedules
 *    that queue 20 ms later. Here, too, nothing asserts on a real sleep: the armed deadline and the
 *    requests the broker double recorded are what the assertions read.
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

use RocketMQ\Client\DefaultLitePullConsumer;
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
 * An orderly listener, used only to put a consumer in ordered-consumption mode: the cache gate's
 * span leg is what is under test here, never what the listener does with a batch.
 */
final class FcOrderlyListener implements RocketMQ\Client\MessageListenerOrderly
{
    public function consumeMessage(array $msgs, RocketMQ\Client\ConsumeOrderlyContext $context): RocketMQ\Client\ConsumeOrderlyStatus
    {
        return RocketMQ\Client\ConsumeOrderlyStatus::SUCCESS;
    }
}

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
            // A 1 KiB body on a 1-byte store record: the accumulators the lite gates read are the
            // body sums, so a gate that still measured the store record would see nothing here.
            $m = new MessageExt(topic: $mq->topic, body: 'fc-' . str_repeat('x', 1021));
            // Re-stamped with the queue the response belongs to, exactly as the real pull decoder
            // does: the lite path keys its cache accumulators off the message's own queue, so an
            // unstamped double would describe a queue that does not exist.
            $m->brokerName = $mq->brokerName;
            $m->queueId = $mq->queueId;
            $m->setQueueOffset($offset);
            $m->setStoreSize(1);
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

    public function getMinOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        return 0;
    }

    public function getMaxOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        // Matches the maxOffset a pull result carries here, so a seek inside the seeded range is legal
        // and no offset RPC ever leaves this process.
        return 100;
    }

    public function queryConsumerOffset(
        string $consumerGroup,
        MessageQueue $mq,
        int $timeoutMillis = 5000,
        ?string $addr = null,
        bool $setZeroIfNotFound = false,
    ): ?int {
        // Nothing was ever committed: the fixture answers the question instead of dialling out.
        return null;
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

    /**
     * Cached messages for the push gates. The body carries the bytes the size gate reads, while
     * ``storeSize`` is deliberately a different number: a gate that still measured the store record
     * would read 1 byte per message here and never trip, which is what reddens it.
     *
     * @param list<int> $offsets
     */
    private function msgs(array $offsets, int $bodyBytes = 1024, int $storeSize = 1): array
    {
        $out = [];
        foreach ($offsets as $offset) {
            $m = new MessageExt(topic: 'GateTopic', body: str_repeat('x', max(1, $bodyBytes)));
            $m->queueOffset = (int) $offset;
            $m->storeSize = $storeSize;
            $out[] = $m;
        }
        return $out;
    }

    /**
     * Stage a queue's held set -- the pulled-but-not-yet-dispatched buffer plus the batches already
     * handed to the listener and not settled yet -- and ask the gate whether it would pause.
     *
     * @param list<MessageExt> $buffered
     * @param list<MessageExt> $withListener
     */
    private function hitHeld(
        DefaultMQPushConsumer $c,
        MessageQueue $mq,
        array $buffered,
        array $withListener,
    ): bool {
        $key = message_queue_key($mq);
        $this->setProp($c, 'pending', [$key => $buffered]);
        $this->setProp($c, 'inflightMsgs', $withListener === [] ? [] : [$key => $withListener]);
        return (bool) $this->call($c, 'flowControlHit', [$mq, $key]);
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

        // A zero threshold is not a disable switch: the comparison is the plain one, so the very
        // first cached message already exceeds it. Which values a user may set at all is the
        // start-up config check's business, not the gate's.
        $zero = $this->gateConsumer('GID-FC-COUNT-ZERO');
        $zero->pullThresholdForQueue = 0;
        $this->checkSame(true, $this->hit($zero, $mq, $this->msgs([0])),
            'a zero threshold switches nothing off: the first cached message already exceeds it');
        $this->checkSame(1, $zero->flowControlTriggeredCount(),
            'the zero-threshold refusal is counted like any other trip');
    }

    // ================================================================ 2b. the held set is the cache plus what the listener still owes

    /**
     * The gate judges everything this queue still owes, not just the buffer: messages pulled and not
     * yet dispatched plus the batches already handed to the listener that have not settled. A budget
     * of 2 with 2 in the buffer and 1 with the listener is 3 held, so the round pauses; the same
     * buffer with the listener's batch settled is 2 held, so it pulls.
     */
    private function testHeldSetIncludesWhatTheListenerStillOwes(): void
    {
        $this->captureLogs(Logger::DEBUG);
        try {
            $mq = new MessageQueue('GateTopic', 'b-fc', 0);

            $c = $this->gateConsumer('GID-FC-HELD');
            $c->pullThresholdForQueue = 2;
            $this->checkSame(true, $this->hitHeld($c, $mq, $this->msgs([0, 1]), $this->msgs([2])),
                'two buffered plus one still with the listener is three held, so the round pauses');
            $this->check($this->lastDebugContains('count=3'),
                'the refusal names the whole held set, not just the buffer');

            // Paired control: the identical buffer with nothing outstanding at the listener.
            $settled = $this->gateConsumer('GID-FC-HELD-SETTLED');
            $settled->pullThresholdForQueue = 2;
            $this->checkSame(false, $this->hitHeld($settled, $mq, $this->msgs([0, 1]), []),
                'the same buffer with the listener batch settled is inside the budget and pulls');

            // A message present in both accounts is one message, not two.
            $shared = $this->gateConsumer('GID-FC-HELD-DEDUP');
            $shared->pullThresholdForQueue = 2;
            $batch = $this->msgs([7]);
            $this->checkSame(false, $this->hitHeld($shared, $mq, $batch, $batch),
                'the same message counted in the buffer and at the listener is still one held');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 3. the size gate counts whole MiB of message body

    /**
     * The size gate measures the **message body**, never the store record, and compares whole MiB
     * after an integer division: a backlog of 1.9 MiB is still one whole MiB and stays inside a 1 MiB
     * budget, while two whole MiB do not. The mirror control puts a big store record on small bodies,
     * which is exactly the pair that reddens a gate still reading ``storeSize``.
     */
    private function testSizeGateCountsWholeMiBOfBody(): void
    {
        $mib = 1024 * 1024;
        $mq = new MessageQueue('GateTopic', 'b-fc', 0);

        $c = $this->gateConsumer('GID-FC-SIZE');
        $c->pullThresholdForQueue = 1000;
        $c->pullThresholdSizeForQueue = 1;
        $this->checkSame(false, $this->hit($c, $mq, $this->msgs([0], $mib)),
            'exactly 1 MiB of body is still allowed under a 1 MiB budget (strictly greater)');
        $this->checkSame(false, $this->hit($c, $mq, $this->msgs([0], $mib + 1)),
            'one byte past 1 MiB is still 1 whole MiB, so the budget tolerates it (integer division)');
        $this->checkSame(true, $this->hit($c, $mq, $this->msgs([0, 1], $mib)),
            'two whole MiB of body trip a 1 MiB budget');

        // Paired control: the same queue on a wide store record and a small body. A gate reading the
        // store record would refuse this round; the body says one byte.
        $body = $this->gateConsumer('GID-FC-SIZE-BODY');
        $body->pullThresholdForQueue = 1000;
        $body->pullThresholdSizeForQueue = 1;
        $this->checkSame(false, $this->hit($body, $mq, $this->msgs([0, 1], 1, 50 * $mib)),
            'the gate sizes the message body, never the store record');

        // A zero size budget is not a switch either: one whole MiB of body exceeds it.
        $zero = $this->gateConsumer('GID-FC-SIZE-ZERO');
        $zero->pullThresholdForQueue = 1000;
        $zero->pullThresholdSizeForQueue = 0;
        $this->checkSame(true, $this->hit($zero, $mq, $this->msgs([0], $mib)),
            'a zero size budget switches nothing off: a whole MiB of body exceeds it');
        $this->checkSame(false, $this->hit($zero, $mq, $this->msgs([0], 1024)),
            'the same zero budget still tolerates a fraction of a MiB (control)');
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
        // A zero maxSpan is not a switch either: any cache that spans more than nothing exceeds it.
        $zero = $this->gateConsumer('GID-FC-SPAN-ZERO');
        $zero->consumeConcurrentlyMaxSpan = 0;
        $this->checkSame(true, $this->hit($zero, $mq, $this->msgs([0, 1_000_000_000])),
            'a zero maxSpan switches nothing off: a two-offset cache already exceeds it');
        $this->checkSame(false, $this->hit($zero, $mq, $this->msgs([5])),
            'the same zero budget tolerates a single cached offset, which spans nothing (control)');
    }

    // ================================================================ 4b. ordered consumption never runs the span leg

    /**
     * The span gate is a concurrent-consumption device: with the same cache and the same budget, a
     * concurrent consumer refuses the round and an orderly one pulls. Judging one cache twice is
     * what makes the exception observable -- either leg alone would also pass if the leg simply
     * stopped being skipped.
     */
    private function testSpanGateIsSkippedWhenConsumingOrderly(): void
    {
        $mq = new MessageQueue('GateTopic', 'b-fc', 0);

        $concurrent = $this->gateConsumer('GID-FC-ORDERLY-NO');
        $concurrent->pullThresholdForQueue = 1000;
        $concurrent->pullThresholdSizeForQueue = 100;
        $concurrent->consumeConcurrentlyMaxSpan = 10;
        $this->checkSame(true, $this->hit($concurrent, $mq, $this->msgs([0, 50])),
            'a concurrent consumer with a too-long span pauses');

        // The identical cache, budget and queue, on an orderly listener.
        $orderly = $this->gateConsumer('GID-FC-ORDERLY-YES');
        $orderly->pullThresholdForQueue = 1000;
        $orderly->pullThresholdSizeForQueue = 100;
        $orderly->consumeConcurrentlyMaxSpan = 10;
        $orderly->messageListener = new FcOrderlyListener();
        $this->checkSame(false, $this->hit($orderly, $mq, $this->msgs([0, 50])),
            'the same span never gates a queue consumed orderly');
        $this->checkSame(0, $orderly->flowControlTriggeredCount(),
            'the skipped leg arms no pause and moves no counter');
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
            $this->check($this->lastDebugContains('size=2MB'),
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
        $this->checkSame(1000, DefaultMQPushConsumer::DEFAULT_FAILED_REQUEST_BACKOFF_MILLIS,
            'the configured failed-round backoff defaults to 1000 ms');
        $this->checkSame(1000, (new DefaultMQPushConsumer('GID-FC-BACKOFF-DEFAULT'))->getFailedRequestBackoffMillis(),
            'a fresh consumer carries that default');
        $consts = (new \ReflectionClass(DefaultMQPushConsumer::class))->getConstants();
        $this->check(!isset($consts['PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION']),
            'the general error pause is the knob now, not a second hardcoded number');
        $this->checkSame(20, DefaultMQPushConsumer::pullBackoffMillisForResponseCode(215),
            'a 215 answer retries early');
        // Paired control: only 215 gets the early retry, so collapsing the branch onto one number
        // (or keying it on the wrong code) reddens this.
        foreach ([1, 2, 24, 25, 26, 500, 3000, null] as $code) {
            $this->checkSame(1000, DefaultMQPushConsumer::pullBackoffMillisForResponseCode($code),
                'every other outcome (' . var_export($code, true) . ') keeps the configured backoff');
        }
        // The value the consumer armed with is what the mapping returns, so a schedule built from a
        // moved knob cannot quietly keep using the default.
        $this->checkSame(20, DefaultMQPushConsumer::pullBackoffMillisForResponseCode(215, 700),
            'a moved knob still leaves the 215 arm at 20 ms');
        foreach ([1, 25, null] as $code) {
            $this->checkSame(700, DefaultMQPushConsumer::pullBackoffMillisForResponseCode($code, 700),
                'every other outcome (' . var_export($code, true) . ') takes the configured 700 ms');
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
            'a 215 answer pauses for ~20 ms, not for the configured failed-round backoff (' . $throttled . ')');

        // Paired control: any other broker error keeps the long backoff, so a branch that answers
        // 20 ms for everything is caught here rather than above.
        $other = $this->brokerBackoffRound('GID-FC-BROKER-25', ResponseCode::SYSTEM_ERROR);
        $this->check($other > 0.9 && $other < 1.4,
            'any other broker error keeps the ~1000 ms configured backoff (' . $other . ')');
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
        $sets = $this->popQueueSet($inst, $group, $topic, [0 => $waitAck]);
        return [$sets[0][0], $sets[0][1], $sets[0][2], $inst];
    }

    /**
     * The same wiring over several queues of one topic, each with its own wait-ack debt, so a POP
     * round can be judged queue by queue.
     *
     * @param array<int,int> $waitAckByQueueId
     * @return list<array{0:DefaultMQPushConsumer,1:MessageQueue,2:string}>
     */
    private function popQueueSet(FcInstance $inst, string $group, string $topic, array $waitAckByQueueId): array
    {
        $c = new DefaultMQPushConsumer($group);
        $c->subscribe($topic);
        $this->setProp($c, 'started', true);
        $c->mqClient = $inst;
        $mqMap = [];
        $pops = [];
        $sets = [];
        foreach ($waitAckByQueueId as $queueId => $waitAck) {
            $mq = new MessageQueue($topic, 'b-fc', (int) $queueId);
            $key = message_queue_key($mq);
            $pq = new \RocketMQ\Client\PopProcessQueue();
            $pq->incFoundMsg($waitAck);
            $mqMap[$key] = $mq;
            $pops[$key] = $pq;
            $sets[] = [$c, $mq, $key];
        }
        $this->setProp($c, 'mqMap', $mqMap);
        $this->setProp($c, 'popQueues', $pops);
        $this->setProp($c, 'subscriptionData', [
            $topic => new \RocketMQ\Common\SubscriptionData($topic, '*'),
        ]);
        return $sets;
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

    /**
     * A POP trip must read differently from a pull trip on both tiers: it names the wait-ack debt as
     * a count, the pop threshold it breached and the queue's current offset the round stood on, and
     * it pauses the pop rather than a pull. Every text here is asserted whole -- the message after
     * the "[time] TIER: " prefix, read from the log handler -- so the wording is pinned from recorded
     * state, never from a sleep window and never through a substring helper.
     */
    private function testPopTripNamesTheDebtInTheLog(): void
    {
        $this->captureLogs(Logger::DEBUG);
        try {
            // Leg 1: the queue has a current offset, and both tiers must carry it.
            $inst = new FcInstance('fc-pop-word-' . uniqid(), []);
            [$c, $mq, $key] = $this->popQueueSet($inst, 'GID-FC-POP-WORD', 'FCPopWord', [0 => 3])[0];
            $c->popThresholdForQueue = 2;
            $this->setProp($c, 'offsetTable', [$key => 777]);
            $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);

            $debug = $this->linesOf('DEBUG');
            $this->checkSame(1, count($debug), 'one pop trip logs exactly one debug line');
            $this->checkSame(
                "flow control: queue $mq the messages waiting to ack (3) exceeds the pop threshold 2,"
                    . ' at offset 777, pause pop',
                $this->messageOf((string) ($debug[0] ?? '')),
                'the pop debug line carries the debt, the pop threshold, the queue offset and the pop pause',
            );
            $this->check(!str_contains((string) ($debug[0] ?? ''), 'waitAckCount='),
                'the bare wait-ack count reason no longer reaches the log');
            $this->check(!str_contains((string) ($debug[0] ?? ''), 'pause pull'),
                'a pop debug line does not claim to pause a pull');
            $warns = $this->linesOf('WARNING');
            $this->checkSame(1, count($warns), 'the first pop trip warns');
            $this->checkSame(
                "flow control: queue $mq the messages waiting to ack (3) exceeds the pop threshold 2"
                    . ' at offset 777, so do flow control, pause pop for 50 ms (flowControlTimes=1)',
                $this->messageOf((string) ($warns[0] ?? '')),
                'the pop warn line carries the debt, the threshold, the queue offset and the trip it fired on',
            );

            // Leg 2: a queue whose offset this client never resolved states that instead of inventing 0.
            $this->captured = [];
            $inst2 = new FcInstance('fc-pop-word2-' . uniqid(), []);
            [$c2, $mq2, $key2] = $this->popQueueSet($inst2, 'GID-FC-POP-WORD2', 'FCPopWord2', [0 => 3])[0];
            $c2->popThresholdForQueue = 2;
            $this->call($c2, 'popOnce', [$mq2, $key2, microtime(true)]);
            $noOffset = $this->linesOf('DEBUG');
            $this->checkSame(1, count($noOffset), 'the offsetless pop trip logs one debug line');
            $this->checkSame(
                "flow control: queue $mq2 the messages waiting to ack (3) exceeds the pop threshold 2,"
                    . ' at offset unknown, pause pop',
                $this->messageOf((string) ($noOffset[0] ?? '')),
                'a pop debug line without a resolved offset says so',
            );
            $noOffsetWarns = $this->linesOf('WARNING');
            $this->checkSame(1, count($noOffsetWarns), 'the offsetless pop trip warns too');
            $this->checkSame(
                "flow control: queue $mq2 the messages waiting to ack (3) exceeds the pop threshold 2"
                    . ' at offset unknown, so do flow control, pause pop for 50 ms (flowControlTimes=1)',
                $this->messageOf((string) ($noOffsetWarns[0] ?? '')),
                'a pop warn line without a resolved offset says so',
            );

            // Paired control: the pull path keeps its exact wording, counter and cadence shared.
            $this->captured = [];
            $pullMq = new MessageQueue('GateTopic', 'b-fc', 0);
            $c3 = $this->gateConsumer('GID-FC-PULL-WORD');
            $c3->pullThresholdForQueue = 1;
            $c3->pullThresholdSizeForQueue = 100;
            $c3->consumeConcurrentlyMaxSpan = 2000;
            $this->hit($c3, $pullMq, $this->msgs([0, 1]));
            $pullDebug = $this->linesOf('DEBUG');
            $this->checkSame(1, count($pullDebug), 'one pull trip still logs exactly one debug line');
            $this->checkSame(
                "flow control: queue $pullMq count=2, pause pull",
                $this->messageOf((string) ($pullDebug[0] ?? '')),
                'the pull debug line keeps its wording untouched',
            );
            $pullWarns = $this->linesOf('WARNING');
            $this->checkSame(1, count($pullWarns), 'the first pull trip still warns');
            $this->checkSame(
                "flow control: queue $pullMq count=2 exceeds its cache threshold,"
                    . ' pause pull for 50 ms (flowControlTimes=1)',
                $this->messageOf((string) ($pullWarns[0] ?? '')),
                'the pull warn line keeps its wording untouched',
            );
        } finally {
            $this->releaseLogs();
        }
    }

    /** The message of a captured line, with its "[timestamp] TIER: " prefix and line feed stripped. */
    private function messageOf(string $line): string
    {
        foreach (['DEBUG', 'INFO', 'WARNING', 'ERROR'] as $tier) {
            $needle = $tier . ': ';
            $pos = strpos($line, $needle);
            if ($pos !== false) {
                return rtrim(substr($line, $pos + strlen($needle)), "\n");
            }
        }
        return rtrim($line, "\n");
    }

    /**
     * One counter, two paths: the pop trip and the pull trip of the same consumer add up, so the
     * warn cadence and any diagnostic reading the counter see one budget, not two.
     */
    private function testPopAndPullTripsShareOneCounter(): void
    {
        $inst = new FcInstance('fc-pop-counter-' . uniqid(), []);
        [$c, $mq, $key] = $this->popQueueSet($inst, 'GID-FC-POP-COUNTER', 'FCPopCounter', [0 => 2])[0];
        $c->popThresholdForQueue = 1;
        $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);
        $this->checkSame(1, $c->flowControlTriggeredCount(), 'the pop trip is the first hit on the counter');

        // The very same consumer then trips a pull gate: no second counter can absorb it.
        $c->pullThresholdForQueue = 1;
        $c->pullThresholdSizeForQueue = 100;
        $c->consumeConcurrentlyMaxSpan = 2000;
        $this->setProp($c, 'flowControlUntil', []);
        $this->checkSame(true, $this->hit($c, $mq, $this->msgs([0, 1])), 'the pull gate trips too');
        $this->checkSame(2, $c->flowControlTriggeredCount(),
            'the pull trip lands on the counter the pop trip already used');

        // Control: a consumer whose only trip is a pull trip still reads 1.
        $c2 = $this->gateConsumer('GID-FC-PULL-ONLY');
        $c2->pullThresholdForQueue = 1;
        $c2->pullThresholdSizeForQueue = 100;
        $c2->consumeConcurrentlyMaxSpan = 2000;
        $this->hit($c2, new MessageQueue('GateTopic', 'b-fc', 0), $this->msgs([0, 1]));
        $this->checkSame(1, $c2->flowControlTriggeredCount(),
            'a pull-only consumer counts one trip, so the shared counter is not inflated');
    }

    /**
     * The warn cadence is fed by pop trips too: the predicate picks the 1st trip and then every
     * 1000th after it, and a pop trip that lands on the next warn threshold really emits the warn.
     * The counter is seeded rather than driven 1000 times, so no case waits on a clock.
     */
    private function testPopTripsFeedTheWarnCadence(): void
    {
        $this->checkSame(true, DefaultMQPushConsumer::isFlowControlWarnTrip(1),
            'the predicate picks a first pop trip');
        $this->checkSame(false, DefaultMQPushConsumer::isFlowControlWarnTrip(2),
            'the predicate keeps a second pop trip at debug level');
        $this->checkSame(false, DefaultMQPushConsumer::isFlowControlWarnTrip(1000),
            'trip 1000 is not the warn trip; the cadence counts from the first');
        $this->checkSame(true, DefaultMQPushConsumer::isFlowControlWarnTrip(1001),
            'trip 1001 is the next warn trip a pop path can land on');

        $this->captureLogs(Logger::DEBUG);
        try {
            $inst = new FcInstance('fc-pop-cadence-' . uniqid(), []);
            [$c, $mq, $key] = $this->popQueueSet($inst, 'GID-FC-POP-CADENCE', 'FCPopCadence', [0 => 2])[0];
            $c->popThresholdForQueue = 1;
            $this->setProp($c, 'flowControlTriggered', 1000);
            $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);
            $this->checkSame(1001, $c->flowControlTriggeredCount(),
                'the seeded counter let the pop trip land on 1001');
            $warns = $this->linesOf('WARNING');
            $this->checkSame(1, count($warns),
                'a pop trip on the warn threshold warns just like a pull trip');
            $this->checkSame(
                "flow control: queue $mq the messages waiting to ack (2) exceeds the pop threshold 1"
                    . ' at offset unknown, so do flow control, pause pop for 50 ms (flowControlTimes=1001)',
                $this->messageOf((string) ($warns[0] ?? '')),
                'the pop warn line carries the shared trip counter it fired on',
            );
            $this->checkSame(1, count($this->linesOf('DEBUG')),
                'a pop trip between two warns still logs its debug line');

            // The next pop trip is one past the warn threshold: debug only.
            $this->captured = [];
            $this->setProp($c, 'flowControlUntil', []);
            $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);
            $this->checkSame(1002, $c->flowControlTriggeredCount(), 'the counter keeps counting up');
            $this->checkSame(0, count($this->linesOf('WARNING')),
                'trip 1002 on the pop path stays at debug level');
            $this->checkSame(1, count($this->linesOf('DEBUG')),
                'the quiet pop trip still leaves its debug line');
        } finally {
            $this->releaseLogs();
        }
    }

    /**
     * The pop pause is armed per queue: one queue past its own wait-ack threshold is skipped while
     * its sibling, which holds nothing at all, pops on the very same round.
     */
    private function testPopPauseIsPerQueue(): void
    {
        $inst = new FcInstance('fc-pop-perq-' . uniqid(), []);
        $sets = $this->popQueueSet($inst, 'GID-FC-POP-PERQ', 'FCPopPerQ', [0 => 3, 1 => 0]);
        $c = $sets[0][0];
        $c->popThresholdForQueue = 2;
        [$mqA, $keyA] = [$sets[0][1], $sets[0][2]];
        [$mqB, $keyB] = [$sets[1][1], $sets[1][2]];

        $this->call($c, 'popOnce', [$mqA, $keyA, microtime(true)]);
        $this->call($c, 'popOnce', [$mqB, $keyB, microtime(true)]);

        $this->checkSame(1, $inst->popCount(),
            'the throttled queue sends nothing while its empty sibling pops on the same round');
        $this->checkSame(
            [['topic' => 'FCPopPerQ', 'group' => 'GID-FC-POP-PERQ', 'queueId' => 1]],
            $inst->pops,
            'the request that went out is the sibling queue with no wait-ack debt',
        );
        $until = (array) $this->prop($c, 'flowControlUntil');
        $this->check(isset($until[$keyA]), 'only the over-threshold queue armed a pause');
        $this->check(!isset($until[$keyB]), 'the empty sibling armed no pause');
        $this->checkSame(1, $c->flowControlTriggeredCount(),
            'one over-threshold queue of two is one trip, not two');

        // The armed pause holds only its own queue back: driven again inside the window queue A
        // sends nothing again and the counter does not move, while queue B pops a second time.
        $this->call($c, 'popOnce', [$mqA, $keyA, microtime(true)]);
        $this->checkSame(1, $inst->popCount(), 'queue A is still inside its own pop pause');
        $this->checkSame(1, $c->flowControlTriggeredCount(),
            'a round skipped by the pause is not a fresh trip');
        $this->setProp($c, 'idleUntil', []);
        $this->call($c, 'popOnce', [$mqB, $keyB, microtime(true)]);
        $this->checkSame(2, $inst->popCount(), 'the sibling keeps popping on its own rounds');
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
        // The span budget is set wide rather than to zero: a zero maxSpan is a real budget, not a
        // switch, and these buffers span 249 offsets. Only the message count can be the reason here.
        $c->consumeConcurrentlyMaxSpan = 1000;
        $this->setProp($c, 'mqMap', $mqMap);
        $this->setProp($c, 'pending', $pending);
        return $c;
    }

    // ================================================================ 12. the LitePull cache gates

    /** The lite path's own queue key: a bare topic + brokerName + queueId concatenation. */
    private function liteKey(MessageQueue $mq): string
    {
        return $mq->topic . $mq->brokerName . (string) $mq->queueId;
    }

    private function liteProp(DefaultLitePullConsumer $c, string $name): mixed
    {
        $p = new \ReflectionProperty(DefaultLitePullConsumer::class, $name);
        $p->setAccessible(true);
        return $p->getValue($c);
    }

    private function setLiteProp(DefaultLitePullConsumer $c, string $name, mixed $value): void
    {
        $p = new \ReflectionProperty(DefaultLitePullConsumer::class, $name);
        $p->setAccessible(true);
        $p->setValue($c, $value);
    }

    private function callLite(DefaultLitePullConsumer $c, string $name, array $args): mixed
    {
        $m = new \ReflectionMethod(DefaultLitePullConsumer::class, $name);
        $m->setAccessible(true);
        return $m->invokeArgs($c, $args);
    }

    /**
     * A lite consumer whose queues are already assigned with a resolved cursor, driven one queue's
     * round at a time: every request the round wants to send is recorded by the instance double and
     * nothing here consults a rebalance or a wall clock. ``autoCommit`` is off so that ``poll()``
     * and ``shutdown()`` stay on recorded state as well.
     *
     * @return array{0:DefaultLitePullConsumer,1:array<string,MessageQueue>,2:list<string>}
     */
    private function liteConsumer(FcInstance $inst, string $group, string $topic, int ...$queueIds): array
    {
        $c = new DefaultLitePullConsumer($group);
        $c->subscribe($topic);
        $c->setAutoCommit(false);
        $this->setLiteProp($c, 'started', true);
        $this->setLiteProp($c, 'running', true);
        $c->mqClient = $inst;
        $assigned = [];
        $nextOffset = [];
        $keys = [];
        foreach ($queueIds as $queueId) {
            $mq = new MessageQueue($topic, 'b-fc', $queueId);
            $key = $this->liteKey($mq);
            $assigned[$key] = $mq;
            $nextOffset[$key] = 100;
            $keys[] = $key;
        }
        $this->setLiteProp($c, 'assigned', $assigned);
        $this->setLiteProp($c, 'nextOffset', $nextOffset);
        return [$c, $assigned, $keys];
    }

    /**
     * One cached message of one queue, stamped with that queue's identity. The size accumulator the
     * gates read is built from the body, so ``$bodyBytes`` is the number under test and the store
     * record is set to 1 -- a gate that measured the store record would see 1 byte per message here.
     */
    private function liteMsg(MessageQueue $mq, int $offset, int $bodyBytes, string $body = ''): MessageExt
    {
        $m = new MessageExt(topic: $mq->topic, body: $body === '' ? str_repeat('x', max(1, $bodyBytes)) : $body);
        $m->brokerName = $mq->brokerName;
        $m->queueId = $mq->queueId;
        $m->setQueueOffset($offset);
        $m->setStoreSize(1);
        return $m;
    }

    /**
     * Put messages into the consumer's cache through the buffer's own append step -- the same one a
     * live pull result goes through -- so the accumulators the gates read are filled by the buffer.
     * Each call is one pull round, which is what the aggregate gate counts.
     *
     * @param list<int> $offsets
     */
    private function liteSeed(DefaultLitePullConsumer $c, MessageQueue $mq, array $offsets, int $bodyBytes = 1024): void
    {
        $made = [];
        foreach ($offsets as $offset) {
            $made[] = $this->liteMsg($mq, (int) $offset, $bodyBytes);
        }
        $this->callLite($c, 'bufferAppend', [$made]);
    }

    /** Drive one queue's pull round. */
    private function litePullOnce(DefaultLitePullConsumer $c, MessageQueue $mq): bool
    {
        return (bool) $this->callLite($c, 'pullOne', [$mq]);
    }

    /** @return array<string,float> the queues this consumer has parked on a cache gate */
    private function litePauseMap(DefaultLitePullConsumer $c): array
    {
        return (array) $this->liteProp($c, 'cacheFlowControlUntil');
    }

    private function litePauseUntil(DefaultLitePullConsumer $c, string $key): ?float
    {
        $until = $this->litePauseMap($c);
        return isset($until[$key]) ? (float) $until[$key] : null;
    }

    /** @return array<string,float> the queues parked after a failed request */
    private function liteIdleMap(DefaultLitePullConsumer $c): array
    {
        return (array) $this->liteProp($c, 'pullIdleUntil');
    }

    /** Park (or clear) one queue's cache-gate deadline explicitly, so a round is judged by it alone. */
    private function liteSetPause(DefaultLitePullConsumer $c, string $key, ?float $until): void
    {
        $this->setLiteProp($c, 'cacheFlowControlUntil', $until === null ? [] : [$key => $until]);
    }

    /**
     * The numbers and the surface the lite gates are built from, asserted without a network: the
     * four budgets and their defaults, plain setter/getter round-trips with **no clamping**, the
     * three counters a fresh consumer has not moved, and the pause length -- which the lite path
     * shares with the push path instead of restating a value of its own.
     */
    private function testLiteConfigurationSurface(): void
    {
        $c = new DefaultLitePullConsumer('GID-LITE-CFG');
        $this->checkSame(10000, $c->getPullThresholdForAll(), 'pullThresholdForAll defaults to 10000 messages');
        $this->checkSame(1000, $c->getPullThresholdForQueue(), 'pullThresholdForQueue defaults to 1000 messages');
        $this->checkSame(100, $c->getPullThresholdSizeForQueue(), 'pullThresholdSizeForQueue defaults to 100 MiB');
        $this->checkSame(2000, $c->getConsumeMaxSpan(), 'consumeMaxSpan defaults to 2000 offsets');
        $this->checkSame(0, $c->flowControlForAllTimes(), 'a fresh lite consumer tripped no aggregate gate');
        $this->checkSame(0, $c->flowControlForQueueTimes(), 'a fresh lite consumer tripped no count or size gate');
        $this->checkSame(0, $c->flowControlForSpanTimes(), 'a fresh lite consumer tripped no span gate');
        // The batch size is the aggregate gate's multiplier, so its default is the budget of one
        // round; the push path asks for a whole 32 and stays there.
        $this->checkSame(10, $c->pullBatchSize, 'the lite pull batch defaults to 10 messages');
        $this->checkSame(32, (new DefaultMQPushConsumer('GID-LITE-CFG-PUSH'))->pullBatchSize,
            'the push path keeps its own 32-message pull batch');

        $c->setPullThresholdForAll(7);
        $c->setPullThresholdForQueue(0);
        $c->setPullThresholdSizeForQueue(3);
        $c->setConsumeMaxSpan(11);
        $this->checkSame([7, 0, 3, 11], [
            $c->getPullThresholdForAll(),
            $c->getPullThresholdForQueue(),
            $c->getPullThresholdSizeForQueue(),
            $c->getConsumeMaxSpan(),
        ], 'the four lite budgets round-trip through their accessors');
        $this->checkSame(0, $c->getPullThresholdForQueue(),
            'a zero count budget is stored as given: the budgets are never clamped');

        $consts = (new \ReflectionClass(DefaultLitePullConsumer::class))->getConstants();
        $this->checkSame(50, DefaultMQPushConsumer::PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL,
            'the own-cache pause the lite gates arm is 50 ms');
        $this->check(!isset($consts['PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL']),
            'the lite path reuses the shared 50 ms pause constant instead of declaring a second value');
    }

    /**
     * The count gate is strictly greater and judged on this queue's own cached messages: a buffer
     * that reached the budget exactly still pulls, one message more does not. The ladder runs on
     * real pull results, so the messages the gate counts are the ones the buffer actually holds.
     */
    private function testLiteCountGateIsStrictlyGreater(): void
    {
        $inst = new FcInstance('lite-count-' . uniqid(), []);
        [$c, $mqMap, $keys] = $this->liteConsumer($inst, 'GID-LITE-COUNT', 'LiteCount', 0);
        $mq = $mqMap[$keys[0]];
        $key = $keys[0];
        $c->setPullThresholdForQueue(2);

        // Round one: nothing cached, so the request goes out and two messages land in the buffer.
        $this->checkSame(true, $this->litePullOnce($c, $mq), 'an empty lite cache pulls');
        $this->checkSame(1, $inst->pullCount(), 'the empty-cache round put its pull on the wire');
        $this->checkSame(2, $c->cachedMessageCountOf($mq), 'the pull result is what the queue accumulator counts');
        $this->checkSame(1, $c->cachedOffsetSpanOf($mq), 'the span is the buffer\'s own highest minus lowest offset');

        // Boundary control: exactly the budget (2 of 2 allowed) still pulls.
        $this->checkSame(true, $this->litePullOnce($c, $mq),
            'a lite queue holding exactly the count budget is still allowed to pull (strictly greater)');
        $this->checkSame(2, $inst->pullCount(), 'the exactly-at-budget round really sent its request');
        $this->checkSame([], $this->litePauseMap($c), 'a round that pulls arms no pause');

        // One message past it: 4 cached against a budget of 2.
        $before = microtime(true);
        $this->checkSame(false, $this->litePullOnce($c, $mq), 'one message past the count budget stops the round');
        $this->checkSame(2, $inst->pullCount(), 'a refused round sends no pull request at all');
        $until = $this->litePauseUntil($c, $key);
        $this->check($until !== null, 'the refused lite queue parked on its own cache gate');
        $window = (float) $until - $before;
        $this->check($window > 0.04 && $window < 0.1, "the lite refusal re-schedules the round in 50 ms, measured {$window}s");
        $this->checkSame(1, $c->flowControlForQueueTimes(), 'the count gate feeds the shared count/size counter');
        $this->checkSame(0, $c->flowControlForAllTimes(), 'a count-gate refusal is not an aggregate refusal');
        $this->checkSame(0, $c->flowControlForSpanTimes(), 'a count-gate refusal is not a span refusal');

        // A zero budget is a legal configuration and means "trip on the first cached message";
        // the control next to it is the same single message under a budget of one.
        $hot = new FcInstance('lite-zero-' . uniqid(), []);
        [$zc, $zMap, $zKeys] = $this->liteConsumer($hot, 'GID-LITE-ZERO', 'LiteZero', 0);
        $zc->setPullThresholdForQueue(0);
        $this->liteSeed($zc, $zMap[$zKeys[0]], [0]);
        $this->checkSame(false, $this->litePullOnce($zc, $zMap[$zKeys[0]]),
            'a zero count budget trips on the first cached message (no clamping)');
        $this->checkSame(0, $hot->pullCount(), 'the zero-budget refusal sent no request');

        $mild = new FcInstance('lite-one-' . uniqid(), []);
        [$mc, $mMap, $mKeys] = $this->liteConsumer($mild, 'GID-LITE-ONE', 'LiteOne', 0);
        $mc->setPullThresholdForQueue(1);
        $this->liteSeed($mc, $mMap[$mKeys[0]], [0]);
        $this->checkSame(true, $this->litePullOnce($mc, $mMap[$mKeys[0]]),
            'one cached message against a budget of one still pulls (control)');
        $this->checkSame(1, $mild->pullCount(), 'the control round sent its request');
    }

    /**
     * The size gate measures the message **body**, never the store record, and compares whole MiB
     * after an integer division: a large store record around a small body leaves the round pulling,
     * while a body backlog of threshold+1 whole MiB refuses it. The two mirror legs are the pair --
     * a gate reading ``storeSize`` reddens on the first and passes the second the wrong way round.
     */
    private function testLiteSizeGateUsesBodyBytes(): void
    {
        $mib = 1024 * 1024;

        // One small body inside a huge store record: only a body measurement lets this round out.
        $inst = new FcInstance('lite-size-' . uniqid(), []);
        [$c, $mqMap, $keys] = $this->liteConsumer($inst, 'GID-LITE-SIZE', 'LiteSize', 0);
        $mq = $mqMap[$keys[0]];
        $c->setPullThresholdForQueue(1000);
        $c->setPullThresholdSizeForQueue(1);
        $this->liteSeed($c, $mq, [0], 1);
        $this->checkSame(1, $c->cachedByteSizeOf($mq), 'the accumulator sums the body bytes of the cache');
        $this->checkSame(true, $this->litePullOnce($c, $mq),
            'a one-byte body does not refuse the round, whatever its store record says');
        $this->checkSame(1, $inst->pullCount(), 'the body-size refusal-free round sent its request');

        // Mirror: the same 1 MiB budget against a body that overflows it.
        $inst2 = new FcInstance('lite-size-trip-' . uniqid(), []);
        [$c2, $map2, $keys2] = $this->liteConsumer($inst2, 'GID-LITE-SIZE-TRIP', 'LiteSizeTrip', 0);
        $mq2 = $map2[$keys2[0]];
        $c2->setPullThresholdForQueue(1000);
        $c2->setPullThresholdSizeForQueue(1);
        $this->liteSeed($c2, $mq2, [0, 1], $mib);
        $this->checkSame(2 * $mib, $c2->cachedByteSizeOf($mq2),
            'the accumulator is the sum of the cached bodies');
        $this->checkSame(false, $this->litePullOnce($c2, $mq2),
            'a backlog of threshold+1 whole MiB refuses the round');
        $this->checkSame(0, $inst2->pullCount(), 'the whole-MiB refusal sent no request');

        // Integer division leg: one byte past a whole MiB is still that MiB.
        $inst3 = new FcInstance('lite-size-int-' . uniqid(), []);
        [$c3, $map3, $keys3] = $this->liteConsumer($inst3, 'GID-LITE-SIZE-INT', 'LiteSizeInt', 0);
        $mq3 = $map3[$keys3[0]];
        $c3->setPullThresholdForQueue(1000);
        $c3->setPullThresholdSizeForQueue(1);
        $this->liteSeed($c3, $mq3, [0], $mib + 1);
        $this->checkSame(true, $this->litePullOnce($c3, $mq3),
            'one byte past 1 MiB is still 1 whole MiB, so a 1 MiB budget still pulls (integer division)');
        $this->checkSame(1, $inst3->pullCount(), 'the below-a-whole-MiB round sent its request');
    }

    /**
     * The span gate is highest minus lowest **cached** offset of this queue: exactly the budget
     * pulls, one offset more refuses, the order the buffer holds the messages in is irrelevant, and
     * a sibling queue's long span is none of this queue's business.
     */
    private function testLiteSpanGateIsStrictlyGreater(): void
    {
        $inst = new FcInstance('lite-span-' . uniqid(), []);
        [$c, $mqMap, $keys] = $this->liteConsumer($inst, 'GID-LITE-SPAN', 'LiteSpan', 0, 1);
        $mqA = $mqMap[$keys[0]];
        $mqB = $mqMap[$keys[1]];
        $c->setPullThresholdForQueue(1000);
        $c->setPullThresholdSizeForQueue(100);
        $c->setConsumeMaxSpan(10);

        // Queue A sits exactly on the span budget; queue B spans past it.
        $this->liteSeed($c, $mqA, [0, 10]);
        $this->liteSeed($c, $mqB, [0, 11]);
        $this->checkSame(10, $c->cachedOffsetSpanOf($mqA), 'the span is max minus min of this queue\'s cache');
        $this->checkSame(true, $this->litePullOnce($c, $mqA),
            'a cached span of exactly consumeMaxSpan is allowed to pull (strictly greater)');
        $this->checkSame(1, $inst->pullCount(), 'the exactly-at-span round sent its request');
        $this->checkSame(0, $c->flowControlForSpanTimes(), 'a round at the span budget trips no counter');

        $this->checkSame(false, $this->litePullOnce($c, $mqB),
            'a span one offset past consumeMaxSpan refuses the round');
        $this->checkSame(1, $inst->pullCount(), 'the span refusal sent no new request');
        $this->checkSame(1, $c->flowControlForSpanTimes(), 'the span gate has its own counter');
        $this->checkSame(0, $c->flowControlForQueueTimes(), 'a span refusal does not feed the count/size counter');
        $this->check($this->litePauseUntil($c, $this->liteKey($mqB)) !== null,
            'the span refusal parked queue B alone');
        $this->check($this->litePauseUntil($c, $this->liteKey($mqA)) === null,
            'the queue that pulled armed no pause');

        // The span is max minus min, whatever order the buffer holds, and it is this queue's own:
        // a wide-spaced queue never puts a tight one's span up.
        $inst2 = new FcInstance('lite-span2-' . uniqid(), []);
        [$c2, $map2, $keys2] = $this->liteConsumer($inst2, 'GID-LITE-SPAN2', 'LiteSpan2', 0);
        $mq2 = $map2[$keys2[0]];
        $c2->setPullThresholdForQueue(1000);
        $c2->setConsumeMaxSpan(10);
        $this->liteSeed($c2, $mq2, [11, 0, 4]);
        $this->checkSame(11, $c2->cachedOffsetSpanOf($mq2),
            'the span is the highest minus the lowest cached offset, not the buffer order');
        $this->checkSame(false, $this->litePullOnce($c2, $mq2),
            'the unordered backlog spans past the budget and is refused');
        $this->checkSame(0, $inst2->pullCount(), 'the unordered span refusal sent no request');

        // Control: an empty and a single-message cache both span 0, so even a zero span budget
        // (never clamped) leaves them alone.
        $inst3 = new FcInstance('lite-span3-' . uniqid(), []);
        [$c3, $map3, $keys3] = $this->liteConsumer($inst3, 'GID-LITE-SPAN3', 'LiteSpan3', 0);
        $mq3 = $map3[$keys3[0]];
        $c3->setPullThresholdForQueue(1000);
        $c3->setConsumeMaxSpan(0);
        $this->checkSame(0, $c3->cachedOffsetSpanOf($mq3), 'an empty cache spans 0');
        $this->liteSeed($c3, $mq3, [500]);
        $this->checkSame(0, $c3->cachedOffsetSpanOf($mq3), 'a single cached message spans 0');
        $this->checkSame(true, $this->litePullOnce($c3, $mq3),
            'a zero span budget is not breached by a single-message cache, so the round pulls');
        $this->checkSame(1, $inst3->pullCount(), 'the empty-span control sent its request');
    }

    /**
     * The aggregate gate is a capacity projection, not a sum of what happens to be cached: every
     * pull round still waiting to be handed to the application budgets a whole ``pullBatchSize``,
     * and it is that projection that is compared with ``pullThresholdForAll``. Three rounds against a
     * 20-message budget at a batch of 10 therefore refuse every queue while the buffers hold only six
     * messages -- no per-queue budget is anywhere near its limit -- and the very same three rounds
     * against a 30-message budget pull everywhere.
     */
    private function testLiteAggregateGateProjectsRoundsTimesBatch(): void
    {
        $inst = new FcInstance('lite-all-' . uniqid(), []);
        [$c, $mqMap, $keys] = $this->liteConsumer($inst, 'GID-LITE-ALL', 'LiteAll', 0, 1, 2);
        $c->setPullThresholdForQueue(1000);
        $c->setPullThresholdSizeForQueue(100);
        $c->setConsumeMaxSpan(2000);
        $c->setPullBatchSize(10);
        $c->setPullThresholdForAll(20);
        foreach ($keys as $i => $key) {
            $this->liteSeed($c, $mqMap[$key], [$i * 10, $i * 10 + 1]);
        }
        $this->checkSame(3, $c->pendingPullRounds(), 'three append rounds are pending across the three queues');
        $this->checkSame(6, $c->cachedMessageCount(),
            'only six messages are actually cached, so a sum of the buffer would never refuse');

        foreach ($keys as $i => $key) {
            $this->checkSame(false, $this->litePullOnce($c, $mqMap[$key]),
                "queue $i is refused by the projection of the pending rounds");
        }
        $this->checkSame(0, $inst->pullCount(), 'an aggregate refusal sends no request for any queue');
        $this->checkSame(3, $c->flowControlForAllTimes(),
            'the aggregate gate counts one trip per refused queue on its own counter');
        $this->checkSame(0, $c->flowControlForQueueTimes(),
            'the per-queue count/size counter stayed untouched, so no buffer total was read');
        $this->checkSame(0, $c->flowControlForSpanTimes(),
            'the span counter stayed untouched on an aggregate refusal');
        $this->checkSame(3, count($this->litePauseMap($c)), 'each refused queue parked on its own deadline');

        // The multiplier is the pull batch size: the same three rounds under a batch of 6 are a
        // projection of 18, inside the same 20-message budget, and every queue pulls.
        $c->setPullBatchSize(6);
        $this->liteSetPause($c, $this->liteKey($mqMap[$keys[0]]), null);
        $this->checkSame(true, $this->litePullOnce($c, $mqMap[$keys[0]]),
            'three rounds at a batch of 6 project 18, inside the same budget the batch of 10 breached');

        // Paired control: the same three rounds and the same batch of 10 against a 30-message budget,
        // judged without sending a request -- a round that succeeds projects a fourth round of its own.
        $inst2 = new FcInstance('lite-all-ok-' . uniqid(), []);
        [$c2, $map2, $keys2] = $this->liteConsumer($inst2, 'GID-LITE-ALL-OK', 'LiteAllOk', 0, 1, 2);
        $c2->setPullThresholdForQueue(1000);
        $c2->setPullThresholdSizeForQueue(100);
        $c2->setConsumeMaxSpan(2000);
        $c2->setPullBatchSize(10);
        $c2->setPullThresholdForAll(30);
        foreach ($keys2 as $i => $key) {
            $this->liteSeed($c2, $map2[$key], [$i * 10, $i * 10 + 1]);
        }
        $this->checkSame(30, $c2->pendingPullRounds() * $c2->pullBatchSize,
            'the projection is exactly the threshold here: strictly greater, so it still pulls');
        foreach ($keys2 as $i => $key) {
            $this->checkSame(false, $this->liteGate($c2, $map2[$key]),
                "queue $i passes the aggregate gate with its rounds projected at the budget ($i)");
        }
        $this->checkSame(0, $c2->flowControlForAllTimes(), 'a projection at the budget trips nothing');

        // The projection is the buffer's own bookkeeping: the rounds retire as the batches they
        // carried are handed over, and a round still partly in the buffer keeps counting a batch.
        $drainInst = new FcInstance('lite-drain-' . uniqid(), []);
        [$dc, $dmap, $dkeys] = $this->liteConsumer($drainInst, 'GID-LITE-DRAIN', 'LiteDrain', 0);
        $dmq = $dmap[$dkeys[0]];
        $this->liteSeed($dc, $dmq, [0, 1, 2]);
        $this->checkSame(1, $dc->pendingPullRounds(), 'one append round is pending');
        $this->checkSame(3, $dc->cachedMessageCount(), 'and it carried three messages into the buffer');
        $this->callLite($dc, 'bufferTake', [1]);
        $this->checkSame(1, $dc->pendingPullRounds(),
            'a round with messages still in the buffer keeps its projected batch');
        $this->callLite($dc, 'bufferTake', [2]);
        $this->checkSame(0, $dc->pendingPullRounds(), 'the round retires once its last message left');
        $this->checkSame(0, $dc->cachedMessageCount(), 'and the buffer emptied with it');
    }

    /** Judge one queue's cache gates without running its round. */
    private function liteGate(DefaultLitePullConsumer $c, MessageQueue $mq): bool
    {
        return (bool) $this->callLite($c, 'cacheFlowControlHit', [$mq, $this->liteKey($mq)]);
    }

    /**
     * Gates two and three judge **only this queue**: a hot sibling never stops a cold one, for the
     * count gate and for the size gate alike.
     */
    private function testLiteGatesJudgeOneQueueOnly(): void
    {
        // Count gate: queue 0 three cached against a budget of 2, queue 1 one cached.
        $inst = new FcInstance('lite-sib-count-' . uniqid(), []);
        [$c, $mqMap, $keys] = $this->liteConsumer($inst, 'GID-LITE-SIB-COUNT', 'LiteSibCount', 0, 1);
        $c->setPullThresholdForQueue(2);
        $this->liteSeed($c, $mqMap[$keys[0]], [0, 1, 2]);
        $this->liteSeed($c, $mqMap[$keys[1]], [0]);
        $this->checkSame(false, $this->litePullOnce($c, $mqMap[$keys[0]]),
            'the hot queue is refused by its own count budget');
        $this->checkSame(true, $this->litePullOnce($c, $mqMap[$keys[1]]),
            'the cold sibling still pulls: the count budget is never summed over queues');
        $this->checkSame([1], array_column($inst->pulls, 'queueId'),
            'the one request that left carried the cold sibling');
        $until = $this->litePauseMap($c);
        $this->check(isset($until[$keys[0]]), 'the hot queue parked on its own deadline');
        $this->check(!isset($until[$keys[1]]), 'the pulling sibling armed no deadline');

        // Size gate: queue 0 3 MiB cached against a 2 MiB budget, queue 1 1 MiB.
        $mib = 1024 * 1024;
        $inst2 = new FcInstance('lite-sib-size-' . uniqid(), []);
        [$c2, $map2, $keys2] = $this->liteConsumer($inst2, 'GID-LITE-SIB-SIZE', 'LiteSibSize', 0, 1);
        $c2->setPullThresholdForQueue(1000);
        $c2->setPullThresholdSizeForQueue(2);
        $this->liteSeed($c2, $map2[$keys2[0]], [0, 1], intdiv(3 * $mib, 2));
        $this->liteSeed($c2, $map2[$keys2[1]], [0], $mib);
        $this->checkSame(false, $this->litePullOnce($c2, $map2[$keys2[0]]),
            'the hot queue is refused by its own size budget');
        $this->checkSame(true, $this->litePullOnce($c2, $map2[$keys2[1]]),
            'the cold sibling still pulls: the size budget is never summed over queues');
        $this->checkSame([1], array_column($inst2->pulls, 'queueId'),
            'the one request that left carried the sibling inside its size budget');
        $this->checkSame(1, $c2->flowControlForQueueTimes(),
            'one hot queue of two is one trip on the shared counter');
    }

    /**
     * Which gate answers a round is decided by the order the contract fixes -- aggregate, count,
     * size, span -- and it is read from the counter that moved plus the credited log line, so a
     * re-ordered gate does not slip through.
     */
    private function testLiteGateOrder(): void
    {
        $this->captureLogs(Logger::DEBUG);
        try {
            $mib = 1024 * 1024;
            $backlog = [0, 20, 40, 60];

            $inst = new FcInstance('lite-order-' . uniqid(), []);
            [$c, $map, $keys] = $this->liteConsumer($inst, 'GID-LITE-ORDER', 'LiteOrder', 0);
            $mq = $map[$keys[0]];
            $c->setPullThresholdForAll(3);
            $c->setPullThresholdForQueue(2);
            $c->setPullThresholdSizeForQueue(1);
            $c->setConsumeMaxSpan(10);
            $this->liteSeed($c, $mq, $backlog, 2 * $mib);
            $this->checkSame(false, $this->litePullOnce($c, $mq), 'a round past every budget is refused');
            $this->checkSame([1, 0, 0], [
                $c->flowControlForAllTimes(),
                $c->flowControlForQueueTimes(),
                $c->flowControlForSpanTimes(),
            ], 'with every budget breached the aggregate gate is the one credited');
            $this->checkSame(0, $inst->pullCount(), 'the credited refusal sent no request');

            // Same backlog, aggregate budget out of the way: the count gate answers.
            $c->setPullThresholdForAll(10000);
            $this->liteSetPause($c, $this->liteKey($mq), null);
            $this->captured = [];
            $this->checkSame(false, $this->litePullOnce($c, $mq), 'the count gate answers once the aggregate passes');
            $this->checkSame([1, 1, 0], [
                $c->flowControlForAllTimes(),
                $c->flowControlForQueueTimes(),
                $c->flowControlForSpanTimes(),
            ], 'the count gate moved the shared count/size counter and no other');
            $this->check($this->lastDebugContains('the cached message count exceeds the threshold 2'),
                'the credited reason is the count budget');

            // Count budget out of the way too: the size gate answers.
            $c->setPullThresholdForQueue(1000);
            $this->liteSetPause($c, $this->liteKey($mq), null);
            $this->captured = [];
            $this->litePullOnce($c, $mq);
            $this->check($this->lastDebugContains('the cached message size exceeds the threshold 1 MiB'),
                'with only the size budget breached the size reason is credited');

            // Size budget out of the way: the span gate answers last.
            $c->setPullThresholdSizeForQueue(100);
            $this->liteSetPause($c, $this->liteKey($mq), null);
            $this->captured = [];
            $this->litePullOnce($c, $mq);
            $this->check($this->lastDebugContains("the queue's messages, span too long"),
                'with only the span budget breached the span reason is credited');

            // Control: a round inside all four budgets logs nothing at all.
            $inst2 = new FcInstance('lite-order-ok-' . uniqid(), []);
            [$c2, $map2, $keys2] = $this->liteConsumer($inst2, 'GID-LITE-ORDER-OK', 'LiteOrderOk', 0);
            $c2->setPullThresholdForAll(10);
            $c2->setPullThresholdForQueue(10);
            $c2->setPullThresholdSizeForQueue(10);
            $c2->setConsumeMaxSpan(100);
            $this->liteSeed($c2, $map2[$keys2[0]], [0, 1], 1024);
            $this->captured = [];
            $this->checkSame(true, $this->litePullOnce($c2, $map2[$keys2[0]]),
                'a queue inside every budget pulls');
            $this->checkSame([], $this->linesOf('DEBUG'), 'a round inside every budget logs no flow-control line');
        } finally {
            $this->releaseLogs();
        }
    }

    /**
     * Every refusal logs the queue, what was measured, the budget it breached and the counter it
     * moved -- the same text on both tiers -- and the warn rides the push path's cadence: the 1st
     * trip, then every 1000th after it. The counters are seeded rather than driven a thousand
     * rounds, so no case waits on a clock.
     */
    private function testLiteTripWordingAndWarnCadence(): void
    {
        $mib = 1024 * 1024;
        $this->captureLogs(Logger::DEBUG);
        try {
            // ---- the count gate, trip 1: one debug line and one warn line, worded in full ----
            $inst = new FcInstance('lite-word-count-' . uniqid(), []);
            [$c, $map, $keys] = $this->liteConsumer($inst, 'GID-LITE-WORD-COUNT', 'LiteWord', 0);
            $mq = $map[$keys[0]];
            $c->setPullThresholdForQueue(2);
            $c->setPullThresholdSizeForQueue(100);
            $c->setConsumeMaxSpan(2000);
            $this->liteSeed($c, $mq, [0, 1, 2, 3]);
            $this->litePullOnce($c, $mq);
            $debug = $this->linesOf('DEBUG');
            $this->checkSame(1, count($debug), 'one count-gate refusal logs exactly one debug line');
            $expected = "flow control: queue $mq the cached message count exceeds the threshold 2,"
                . ' so do flow control, minOffset=0, maxOffset=3, count=4, size=0 MiB,'
                . ' flowControlTimes=1, pause pull for 50 ms';
            $this->checkSame($expected, $this->messageOf((string) ($debug[0] ?? '')),
                'the count-gate debug line carries the queue, the measurement, the budget and the counter');
            $warns = $this->linesOf('WARNING');
            $this->checkSame(1, count($warns), 'the first count-gate trip warns');
            $this->checkSame($expected, $this->messageOf((string) ($warns[0] ?? '')),
                'the count-gate warn line reads as the contract words it');

            // ---- the size gate on the same shared counter ----
            $this->captured = [];
            $inst2 = new FcInstance('lite-word-size-' . uniqid(), []);
            [$c2, $map2, $keys2] = $this->liteConsumer($inst2, 'GID-LITE-WORD-SIZE', 'LiteSizeWord', 0);
            $mq2 = $map2[$keys2[0]];
            $c2->setPullThresholdForQueue(1000);
            $c2->setPullThresholdSizeForQueue(1);
            $this->liteSeed($c2, $mq2, [7, 9], 2 * $mib);
            $this->litePullOnce($c2, $mq2);
            $expected2 = "flow control: queue $mq2 the cached message size exceeds the threshold 1 MiB,"
                . ' so do flow control, minOffset=7, maxOffset=9, count=2, size=4 MiB,'
                . ' flowControlTimes=1, pause pull for 50 ms';
            $sizeDebug = $this->linesOf('DEBUG');
            $this->checkSame($expected2, $this->messageOf((string) ($sizeDebug[0] ?? '')),
                'the size-gate debug line names the MiB budget and the measured MiB backlog');
            $this->checkSame($expected2, $this->messageOf((string) ($this->linesOf('WARNING')[0] ?? '')),
                'the size-gate warn line reads as the contract words it');

            // ---- the span gate ----
            $this->captured = [];
            $inst3 = new FcInstance('lite-word-span-' . uniqid(), []);
            [$c3, $map3, $keys3] = $this->liteConsumer($inst3, 'GID-LITE-WORD-SPAN', 'LiteSpanWord', 0);
            $mq3 = $map3[$keys3[0]];
            $c3->setPullThresholdForQueue(1000);
            $c3->setConsumeMaxSpan(10);
            $this->liteSeed($c3, $mq3, [0, 60]);
            $this->litePullOnce($c3, $mq3);
            $expected3 = 'flow control: queue ' . $mq3 . " the queue's messages, span too long,"
                . ' so do flow control, minOffset=0, maxOffset=60, maxSpan=60, flowControlTimes=1,'
                . ' pause pull for 50 ms';
            $this->checkSame($expected3, $this->messageOf((string) ($this->linesOf('DEBUG')[0] ?? '')),
                'the span-gate debug line names the span it measured and the offsets it spanned');
            $this->checkSame($expected3, $this->messageOf((string) ($this->linesOf('WARNING')[0] ?? '')),
                'the span-gate warn line reads as the contract words it');

            // ---- the aggregate gate ----
            $this->captured = [];
            $inst4 = new FcInstance('lite-word-all-' . uniqid(), []);
            [$c4, $map4, $keys4] = $this->liteConsumer($inst4, 'GID-LITE-WORD-ALL', 'LiteAllWord', 0, 1);
            $c4->setPullThresholdForQueue(1000);
            $c4->setPullThresholdForAll(3);
            $this->liteSeed($c4, $map4[$keys4[0]], [0, 1]);
            $this->liteSeed($c4, $map4[$keys4[1]], [0, 1, 2, 3]);
            $this->litePullOnce($c4, $map4[$keys4[0]]);
            $expected4 = 'flow control: queue ' . $map4[$keys4[0]]
                . ' the consume request count exceeds threshold 1, so do flow control,'
                . ' consume request count=2, flowControlTimes=1, pause pull for 50 ms';
            $this->checkSame($expected4, $this->messageOf((string) ($this->linesOf('DEBUG')[0] ?? '')),
                'the aggregate line states the threshold in rounds the batch budget fits, and the rounds pending');
            $this->checkSame($expected4, $this->messageOf((string) ($this->linesOf('WARNING')[0] ?? '')),
                'the aggregate gate warns on its first trip too');

            // ---- the cadence on each of the three counters ----
            // Each case breaches exactly one budget, so the counter whose trip lands on 1001 is
            // unambiguous: aggregate total, this queue's count, this queue's span.
            $cadenceCases = [
                ['flowControlForAllTimes', 'consume request count=1', static function (DefaultLitePullConsumer $x): void {
                    $x->setPullThresholdForAll(3);
                    $x->setPullThresholdForQueue(1000);
                    $x->setPullThresholdSizeForQueue(100);
                    $x->setConsumeMaxSpan(2000);
                }],
                ['flowControlForQueueTimes', 'count=4', static function (DefaultLitePullConsumer $x): void {
                    $x->setPullThresholdForAll(10000);
                    $x->setPullThresholdForQueue(2);
                    $x->setPullThresholdSizeForQueue(100);
                    $x->setConsumeMaxSpan(2000);
                }],
                ['flowControlForSpanTimes', 'maxSpan=60', static function (DefaultLitePullConsumer $x): void {
                    $x->setPullThresholdForAll(10000);
                    $x->setPullThresholdForQueue(1000);
                    $x->setPullThresholdSizeForQueue(100);
                    $x->setConsumeMaxSpan(10);
                }],
            ];
            foreach ($cadenceCases as [$counter, $needle, $configure]) {
                $this->captured = [];
                $inst5 = new FcInstance('lite-cadence-' . $counter . '-' . uniqid(), []);
                [$c5, $map5, $keys5] = $this->liteConsumer($inst5, 'GID-LITE-CADENCE', 'LiteCadence', 0);
                $mq5 = $map5[$keys5[0]];
                $configure($c5);
                $this->liteSeed($c5, $mq5, [0, 20, 40, 60]);
                $this->setLiteProp($c5, $counter, 1000);
                $this->checkSame(false, $this->litePullOnce($c5, $mq5), "the round that trips $counter is refused");
                $this->checkSame(1001, (int) $this->liteProp($c5, $counter),
                    "the seeded counter let a $counter trip land on 1001");
                $warns5 = $this->linesOf('WARNING');
                $this->checkSame(1, count($warns5), "a $counter trip on 1001 is a warn trip");
                $this->check(isset($warns5[0]) && str_contains($warns5[0], 'flowControlTimes=1001'),
                    "the 1001st $counter warn carries the counter it fired on");
                $this->check(isset($warns5[0]) && str_contains($warns5[0], $needle),
                    "the 1001st $counter warn still names the measurement ($needle)");
                $this->checkSame(1, count($this->linesOf('DEBUG')), "the 1001st $counter trip keeps its debug line");

                $this->captured = [];
                $this->liteSetPause($c5, $this->liteKey($mq5), null);
                $this->litePullOnce($c5, $mq5);
                $this->checkSame(1002, (int) $this->liteProp($c5, $counter), 'the counter keeps counting up');
                $this->checkSame(0, count($this->linesOf('WARNING')), 'trip 1002 stays at debug level');
                $this->checkSame(1, count($this->linesOf('DEBUG')), 'the quiet trip still logs its debug line');
            }

            // A thousand trips from a cold counter leave exactly one warn (the first).
            $this->captured = [];
            $inst6 = new FcInstance('lite-thousand-' . uniqid(), []);
            [$c6, $map6, $keys6] = $this->liteConsumer($inst6, 'GID-LITE-THOUSAND', 'LiteThousand', 0);
            $mq6 = $map6[$keys6[0]];
            $c6->setPullThresholdForQueue(1);
            $this->liteSeed($c6, $mq6, [0, 1]);
            for ($i = 0; $i < 1000; $i++) {
                $this->liteSetPause($c6, $this->liteKey($mq6), null);
                $this->litePullOnce($c6, $mq6);
            }
            $this->checkSame(1000, $c6->flowControlForQueueTimes(), 'the lite count gate ran 1000 refused rounds');
            $this->checkSame(1, count($this->linesOf('WARNING')),
                '1000 lite trips leave exactly one warn line (the first)');
            $this->checkSame(1000, count($this->linesOf('DEBUG')),
                'every one of the 1000 lite trips keeps its debug line');
        } finally {
            $this->releaseLogs();
        }
    }

    /**
     * A refusal is a re-scheduled round, not a parked queue: the unpaid 50 ms deadline keeps the
     * request off the wire even once the backlog is gone, and the deadline itself lets exactly one
     * request through. The recovery is asserted, not just the refusal.
     */
    private function testLitePauseBlocksAndRecovers(): void
    {
        $inst = new FcInstance('lite-pause-' . uniqid(), []);
        [$c, $map, $keys] = $this->liteConsumer($inst, 'GID-LITE-PAUSE', 'LitePause', 0);
        $mq = $map[$keys[0]];
        $key = $keys[0];
        $c->setPullThresholdForQueue(1);
        $this->liteSeed($c, $mq, [0, 1]);
        $cursorBefore = $c->pullCursorOf($mq);

        $began = microtime(true);
        $this->checkSame(false, $this->litePullOnce($c, $mq), 'a full lite cache stops the round');
        $this->checkSame(0, $inst->pullCount(), 'the stopped round sent no pull request at all');
        $due = $this->litePauseUntil($c, $key);
        $this->check($due !== null, 'the stopped lite queue armed a pause');
        $this->check($due > $began && $due < $began + 0.1,
            'the armed deadline is the shared 50 ms cache pause, measured from the round that refused');
        $this->checkSame($cursorBefore, $c->pullCursorOf($mq), 'a refusal does not move the pull cursor');
        $this->checkSame(2, $c->cachedMessageCountOf($mq), 'a refusal leaves the messages cached');
        $this->checkSame(-1, $c->consumeCursorOf($mq), 'a refusal commits nothing: the consume cursor never moved');
        $this->checkSame(-1, $c->pendingCommitOf($mq), 'a refusal leaves no pending commit behind either');

        // The app drains the buffer; only the unpaid deadline can be what stops the next round now.
        $this->checkSame(2, count($c->poll(0)), 'the application drained the lite buffer');
        $this->checkSame(0, $c->cachedMessageCount(), 'the drained buffer leaves no accumulator behind');
        $this->checkSame(0, $c->cachedMessageCountOf($mq), 'and this queue reports an empty cache');
        $this->checkSame(false, $this->litePullOnce($c, $mq),
            'a lite queue inside its 50 ms pause is skipped even with an empty buffer');
        $this->checkSame(0, $inst->pullCount(), 'the unpaid pause sent no pull request');
        $this->checkSame(1, $c->flowControlForQueueTimes(), 'a round skipped by the pause is not a fresh trip');

        // Once the pause is due the queue pulls again: the gate cannot deadlock the consumer. The
        // deadline is moved to the past rather than waited out, so the round is judged by it alone.
        $this->liteSetPause($c, $key, microtime(true) - 0.001);
        $this->checkSame(true, $this->litePullOnce($c, $mq), 'once the pause is due the lite queue pulls again');
        $this->checkSame(1, $inst->pullCount(), 'the resumed round sent exactly one real pull request');
        $this->checkSame(
            [['topic' => 'LitePause', 'group' => 'GID-LITE-PAUSE', 'queueId' => 0, 'offset' => 100]],
            $inst->pulls,
            'the resumed request carries the queue cursor this round never touched while it was parked',
        );
        $this->checkSame(1, $c->flowControlForQueueTimes(), 'a round that pulls trips no gate again');

        // Paired control: the same queue with an empty cache and no armed pause pulls on its first round.
        $inst2 = new FcInstance('lite-pause-ok-' . uniqid(), []);
        [$c2, $map2, $keys2] = $this->liteConsumer($inst2, 'GID-LITE-PAUSE-OK', 'LitePauseOk', 0);
        $this->checkSame(true, $this->litePullOnce($c2, $map2[$keys2[0]]),
            'the control queue with nothing cached and no pause pulls on the same round');
        $this->checkSame(1, $inst2->pullCount(), 'the control sent exactly one request');
        $this->checkSame([], $this->litePauseMap($c2), 'a round that pulls arms no pause');
    }

    /**
     * The accumulators are the buffer's own bookkeeping: a live pull result feeds them, ``poll()``
     * taking messages away empties them, ``seek()`` pruning subtracts exactly the pruned messages,
     * a revoked queue loses its record and its cached messages together, and a boundary offset that
     * lost its last message is re-picked. Across all of it the per-queue sums equal the buffer.
     */
    private function testLiteCacheAccumulatorsFollowTheBuffer(): void
    {
        $mib = 1024 * 1024;

        // A real pull result lands in the accumulators, not only in the buffer.
        $inst = new FcInstance('lite-track-' . uniqid(), []);
        [$c, $map, $keys] = $this->liteConsumer($inst, 'GID-LITE-TRACK', 'LiteTrack', 0);
        $mq = $map[$keys[0]];
        $this->checkSame(true, $this->litePullOnce($c, $mq), 'the round pulled two messages into the cache');
        $this->checkSame(2, $c->cachedMessageCountOf($mq), 'the append updated this queue\'s count');
        $this->checkSame(2048, $c->cachedByteSizeOf($mq), 'the append updated this queue\'s body bytes');
        $this->checkSame(2, $c->cachedMessageCount(), 'the aggregate reads the same buffer');

        // poll() taking them away is the exact inverse.
        $handed = $c->poll(0);
        $this->checkSame(2, count($handed), 'poll handed the cached messages to the application');
        $this->checkSame(0, $c->cachedMessageCountOf($mq), 'the removal subtracted the count');
        $this->checkSame(0, $c->cachedByteSizeOf($mq), 'the removal subtracted the body bytes');
        $this->checkSame(0, $c->cachedMessageCount(), 'no accumulator outlives the buffer');

        // seek() pruning subtracts exactly the pruned messages, boundaries included.
        $this->liteSeed($c, $mq, [0, 1, 2, 3, 4], $mib);
        $this->checkSame(4, $c->cachedOffsetSpanOf($mq), 'five cached offsets span 4');
        $c->seek($mq, 2);
        $this->checkSame(3, $c->cachedMessageCountOf($mq), 'seek pruned the two messages below the new offset');
        $this->checkSame(3, $c->cachedMessageCount(), 'the aggregate followed the pruned buffer');
        $this->checkSame(3 * $mib, $c->cachedByteSizeOf($mq), 'the pruned messages\' stored bytes left too');
        $this->checkSame(2, $c->cachedOffsetSpanOf($mq), 'the span is recomputed over what the buffer really holds');

        // The lowest boundary re-picks when its last message leaves: removal comes off the head of
        // the buffer, so the first message out is the one holding the cached offset 2.
        $this->checkSame(2, $this->liteMinOffset($c, $mq), 'the pruned buffer left its lowest boundary at 2');
        $taken = $this->callLite($c, 'bufferTake', [1]);
        $this->checkSame(1, count($taken), 'one message leaves the buffer');
        $this->checkSame(2, $taken[0]->queueOffset, 'and it is the head of the buffer, the lowest cached offset');
        $this->checkSame(3, $this->liteMinOffset($c, $mq), 'the lowest cached offset moved up with its last message');
        $this->checkSame(4, $this->liteMaxOffset($c, $mq), 'taking the lowest away never disturbs the highest');
        $this->checkSame(1, $c->cachedOffsetSpanOf($mq), 'so the span is the two boundaries the buffer still holds');

        // Two messages sharing one cached offset are two cached messages, and handing only one of
        // them over keeps both boundaries: the third message of offset 9 is still there.
        $this->liteSeed($c, $mq, [9]);
        $this->checkSame(9, $this->liteMaxOffset($c, $mq), 'a higher append moves the highest boundary');
        $this->checkSame(6, $c->cachedOffsetSpanOf($mq), 'the span is the two boundaries, 9 down to 3');
        $this->liteSeed($c, $mq, [9]);
        $this->checkSame(4, $c->cachedMessageCountOf($mq), 'the same offset twice is two cached messages');
        $this->checkSame(6, $c->cachedOffsetSpanOf($mq), 'and a duplicate offset does not move the span');
        $this->callLite($c, 'bufferTake', [2]);
        $this->checkSame(2, $c->cachedMessageCountOf($mq), 'the two single messages left the buffer');
        $this->checkSame(9, $this->liteMinOffset($c, $mq),
            'both boundaries re-picked onto the offset that still holds its duplicates');
        $this->checkSame(0, $c->cachedOffsetSpanOf($mq), 'and a cache of one repeated offset spans nothing');
        $this->callLite($c, 'bufferTake', [1]);
        $this->checkSame(1, $c->cachedMessageCountOf($mq), 'one of the two duplicates left');
        $this->checkSame(9, $this->liteMaxOffset($c, $mq),
            'the boundaries survive a partial removal of the offset they sit on');
        $this->checkSame(0, $c->cachedOffsetSpanOf($mq), 'and the span stays 0 across it');
        $this->callLite($c, 'bufferTake', [1]);
        $this->checkSame(0, $c->cachedMessageCountOf($mq), 'the queue is empty once its last message is gone');
        $this->checkSame(0, $c->cachedByteSizeOf($mq), 'and its accumulator is dropped rather than left at zero');
        $this->checkSame(0, $c->cachedMessageCount(), 'the aggregate and the buffer emptied together');

        // Revoking the queue drops its record, its messages and its armed pause in one step. The
        // shrink is the public assign path: a consumer that subscribed cannot also assign, and a
        // revocation in subscribe mode is driven by the rebalance round, not by the caller.
        $revokeInst = new FcInstance('lite-revoke-' . uniqid(), []);
        $rc = new DefaultLitePullConsumer('GID-LITE-REVOKE');
        $rc->setSubExpressionForAssign('LiteRevoke', '*');
        $rc->setAutoCommit(false);
        $this->setLiteProp($rc, 'started', true);
        $this->setLiteProp($rc, 'running', true);
        $rc->mqClient = $revokeInst;
        $rmq = new MessageQueue('LiteRevoke', 'b-fc', 0);
        $kept = new MessageQueue('LiteRevoke', 'b-fc', 1);
        $rc->assign([$rmq, $kept]);
        $this->liteSeed($rc, $rmq, [20, 21]);
        $this->liteSetPause($rc, $this->liteKey($rmq), microtime(true) + 5.0);
        $rc->assign([$kept]);
        $this->checkSame(0, $rc->cachedMessageCountOf($rmq), 'a revoked queue keeps no accumulator');
        $this->checkSame(0, $rc->cachedMessageCount(), 'and its cached messages left the buffer with it');
        $this->check($this->litePauseUntil($rc, $this->liteKey($rmq)) === null,
            'a revoked queue keeps no armed pause either');
        $this->checkSame([], $this->litePauseMap($rc), 'no stale deadline survives the revocation');
    }

    /** The queue's lowest cached offset as the accumulator holds it (0 while nothing is cached). */
    private function liteMinOffset(DefaultLitePullConsumer $c, MessageQueue $mq): int
    {
        return (int) (($this->liteProp($c, 'queueCache')[$this->liteKey($mq)] ?? [])['min'] ?? 0);
    }

    private function liteMaxOffset(DefaultLitePullConsumer $c, MessageQueue $mq): int
    {
        return (int) (($this->liteProp($c, 'queueCache')[$this->liteKey($mq)] ?? [])['max'] ?? 0);
    }

    /**
     * A broker throttle on this path is not an error: a pull answered FLOW_CONTROL(215) re-schedules
     * that queue in 20 ms -- the push path's broker pause, through the shared code-to-wait mapping --
     * while some other code keeps the lite path's existing handling, which is to say it arms nothing
     * and the next tick asks again. Both legs in one test, and neither is a cache-gate trip.
     */
    private function testLiteBrokerThrottleArmsShortPause(): void
    {
        $this->checkSame(215, ResponseCode::FLOW_CONTROL, 'the wire code the lite branch keys on');

        $inst = new FcInstance('lite-215-' . uniqid(), []);
        [$c, $map, $keys] = $this->liteConsumer($inst, 'GID-LITE-215', 'LiteThrottle', 0);
        $mq = $map[$keys[0]];
        $key = $keys[0];
        $inst->failNextWith = ResponseCode::FLOW_CONTROL;
        $before = microtime(true);
        $this->checkSame(false, $this->litePullOnce($c, $mq), 'a 215 answer hands the round no message');
        $this->checkSame(1, $inst->pullCount(), 'the 215 round really asked the broker');
        $idle = $this->liteIdleMap($c);
        $this->check(isset($idle[$key]), 'the 215 answer parked this queue alone');
        $window = (float) $idle[$key] - $before;
        $this->check($window > 0.015 && $window < 0.05,
            "a 215 answer re-schedules the lite round in ~20 ms, measured {$window}s");
        $this->check($this->litePauseUntil($c, $key) === null,
            'a broker throttle is not a cache-gate trip, so the cache pause stays armed elsewhere');
        $this->checkSame([0, 0, 0], [
            $c->flowControlForAllTimes(),
            $c->flowControlForQueueTimes(),
            $c->flowControlForSpanTimes(),
        ], 'a 215 answer feeds none of the cache-gate counters');
        $this->checkSame(false, $this->litePullOnce($c, $mq), 'a queue inside the 20 ms pause is skipped');
        $this->checkSame(1, $inst->pullCount(), 'the unpaid 20 ms pause keeps the request off the wire');
        $this->setLiteProp($c, 'pullIdleUntil', [$key => microtime(true) - 0.001]);
        $this->checkSame(true, $this->litePullOnce($c, $mq), 'once the 20 ms pause is due the queue pulls again');
        $this->checkSame(2, $inst->pullCount(), 'the resumed 215 queue put its request back on the wire');

        // Paired control: any other failure code keeps the lite path as it was -- logged, nothing
        // armed, and the very next round asks again.
        $inst2 = new FcInstance('lite-busy-' . uniqid(), []);
        [$c2, $map2, $keys2] = $this->liteConsumer($inst2, 'GID-LITE-BUSY', 'LiteBusy', 0);
        $mq2 = $map2[$keys2[0]];
        $inst2->failNextWith = ResponseCode::SYSTEM_BUSY;
        $this->checkSame(false, $this->litePullOnce($c2, $mq2), 'a SYSTEM_BUSY answer hands the round no message');
        $this->checkSame(1, $inst2->pullCount(), 'the SYSTEM_BUSY round really asked the broker');
        $this->checkSame([], $this->liteIdleMap($c2),
            'any other failure code arms no short pause: the lite path keeps its own handling');
        $this->checkSame([], $this->litePauseMap($c2), 'and no cache pause either');
        $this->checkSame(true, $this->litePullOnce($c2, $mq2),
            'the next round of the failed queue asks again at once');
        $this->checkSame(2, $inst2->pullCount(), 'the control queue sent its second request');
    }

    /**
     * A gate never blocks: a refusal only leaves a deadline behind, so a consumer whose queue is
     * parked on a gate stops as promptly as one that is pulling. Nothing here waits on the pause --
     * the deadline is deliberately seeded well past the round, and the claim is that shutdown and
     * the following tick return without spending it.
     */
    private function testLiteGateDoesNotBlockShutdown(): void
    {
        $inst = new FcInstance('lite-shutdown-' . uniqid(), []);
        [$c, $map, $keys] = $this->liteConsumer($inst, 'GID-LITE-SHUTDOWN', 'LiteStop', 0, 1);
        $mq = $map[$keys[0]];
        $mq2 = $map[$keys[1]];
        $c->setPullThresholdForQueue(1);
        $this->liteSeed($c, $mq, [0, 1]);
        $this->checkSame(false, $this->litePullOnce($c, $mq), 'queue 0 is parked on its cache gate');
        $this->checkSame(0, $inst->pullCount(), 'the refused queue never reached the broker');

        // Park that queue a full minute out and time the next round: a refusal is a deadline read,
        // never a wait, so the round is over as soon as it has looked at the map.
        $this->liteSetPause($c, $keys[0], microtime(true) + 60.0);
        $this->check($this->litePauseUntil($c, $keys[0]) > microtime(true) + 59.0,
            'the parked deadline is a full minute out, far past any round this test runs');
        $began = microtime(true);
        $this->checkSame(false, $this->litePullOnce($c, $mq), 'a round inside a minute-long pause is skipped');
        $roundCost = microtime(true) - $began;
        $this->check($roundCost < 0.02,
            "a skipped round returns at once instead of spending the pause it read, it took {$roundCost}s");
        $this->checkSame(0, $inst->pullCount(), 'the skipped round sent no request either');

        $began = microtime(true);
        $c->shutdown();
        $stopCost = microtime(true) - $began;
        $this->checkSame(false, $c->isRunning(), 'shutdown stops the lite consumer');
        $this->check($stopCost < 0.05, "shutdown did not wait on the armed pause, it took {$stopCost}s");

        $began = microtime(true);
        $c->tick();
        $tickCost = microtime(true) - $began;
        $this->checkSame(0, $inst->pullCount(), 'a tick after shutdown sends no request for any queue');
        $this->check($tickCost < 0.05,
            "a tick with a minute-long pause still armed returns at once, it took {$tickCost}s");

        // Stopping with the gate tripped leaves nothing half-done: the refused queue still holds its
        // cached messages (the application never got them), the queue that never pulled holds none,
        // and the armed deadline is left where it was rather than paid out on the way.
        $this->checkSame(2, $c->cachedMessageCountOf($mq),
            'shutdown does not drain the cache a gate was protecting');
        $this->checkSame(0, $c->cachedMessageCountOf($mq2), 'the queue that never pulled caches nothing');
        $this->checkSame(2, $c->cachedMessageCount(), 'the aggregate is those two messages and no more');
        $this->checkSame(0, $c->flowControlForSpanTimes(), 'the refusal was a count trip, not a span trip');
        $this->check($this->litePauseUntil($c, $keys[0]) !== null,
            'the armed deadline survives the shutdown: the gate never waited for it, it just left it');

        // Paired control: a consumer whose queues are inside their budgets has nothing to park and
        // nothing to escape, and it stops on the very same round.
        $inst2 = new FcInstance('lite-shutdown-ok-' . uniqid(), []);
        [$c3, $map3, $keys3] = $this->liteConsumer($inst2, 'GID-LITE-SHUTDOWN-OK', 'LiteStopOk', 0, 1);
        $this->checkSame([], $this->litePauseMap($c3), 'an unparked consumer armed no deadline at all');
        $began = microtime(true);
        $c3->shutdown();
        $this->check(microtime(true) - $began < 0.05, 'the control consumer stops just as promptly');
        $this->checkSame(false, $c3->isRunning(), 'and it is stopped too');
        $this->checkSame(0, $inst2->pullCount(), 'the control sent no request before stopping');
        $this->checkSame([], array_filter(
            array_map(static fn(MessageQueue $q): bool => $c3->cachedMessageCountOf($q) > 0, array_values($map3)),
        ), 'and no queue of the control kept any cache behind');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testConfigurationSurface();
        $this->testCountGateIsStrictlyGreater();
        $this->testHeldSetIncludesWhatTheListenerStillOwes();
        $this->testSizeGateCountsWholeMiBOfBody();
        $this->testSpanGateIsStrictlyGreater();
        $this->testSpanGateIsSkippedWhenConsumingOrderly();
        $this->testGatePrecedence();
        $this->testTripArmsThePause();
        $this->testWarnCadence();
        $this->testPauseBlocksThePull();
        $this->testQueueWithoutPausePulls();
        $this->testPauseIsPerQueue();
        $this->testBrokerBackoffMapping();
        $this->testBrokerBackoffArmsThePause();
        $this->testPopGateArmsThePause();
        $this->testPopTripNamesTheDebtInTheLog();
        $this->testPopAndPullTripsShareOneCounter();
        $this->testPopTripsFeedTheWarnCadence();
        $this->testPopPauseIsPerQueue();
        $this->testGateNeverSumsTheQueuesOfATopic();
        $this->testLiteConfigurationSurface();
        $this->testLiteCountGateIsStrictlyGreater();
        $this->testLiteSizeGateUsesBodyBytes();
        $this->testLiteSpanGateIsStrictlyGreater();
        $this->testLiteAggregateGateProjectsRoundsTimesBatch();
        $this->testLiteGatesJudgeOneQueueOnly();
        $this->testLiteGateOrder();
        $this->testLiteTripWordingAndWarnCadence();
        $this->testLitePauseBlocksAndRecovers();
        $this->testLiteCacheAccumulatorsFollowTheBuffer();
        $this->testLiteBrokerThrottleArmsShortPause();
        $this->testLiteGateDoesNotBlockShutdown();
        return $this->summary();
    }
}

exit((new RunClientFlowControl())->run());
