<?php

declare(strict_types=1);

/**
 * Topic-level cache budget suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientTopicThreshold.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * The contract pinned here: a topic-level budget (``pullThresholdForTopic`` /
 * ``pullThresholdSizeForTopic``, -1 = off) is enforced by being **divided into the per-queue knob at
 * rebalance**, and never by summing the queues of a topic at pull time. So:
 *
 * 1. the division splits each enabled budget over the queues this consumer owns right now -- ALL of
 *    them, across every topic -- read from the live per-queue pull-state table (``mqMap`` on the
 *    classic pull path, ``popQueues`` under POP), floors each quotient at 1, and moves only the knob
 *    that budget belongs to;
 * 2. the numerator is always the topic budget, so the same queue count re-derives the same number
 *    while a grown set re-divides the budget (1000/8 = 125, not the 250/8 = 31 the previous round
 *    would feed back);
 * 3. it runs on a rebalance round that CHANGED the owned set, AFTER that round rebuilt the pull
 *    state, so a shrinking round divides by the count it produced;
 * 4. with nothing owned, and with both budgets off, the per-queue knobs stay exactly as the user set
 *    them.
 *
 * Every positive case sits next to a control that goes red when the division is reverted to a
 * pull-time sum, dropped, moved before the state update, or fed the wrong number. State is read
 * back from the knobs, the live pull-state tables and the captured INFO lines -- never from a sleep.
 */

require_once __DIR__ . '/../bootstrap.php';

// This port must never write logs under $HOME: pin the sink to a temp dir, and read the lines this
// suite asserts on through a handler instead of from a file.
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_tt_logs_' . getmypid());
// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Logger;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PopProcessQueue;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\MessageRequestMode;

/**
 * Instance double for the rebalance legs: it serves a mutable queue set per topic, answers the
 * consumer-list query with this single client (so the local strategy takes everything it serves) and
 * keeps every offset RPC off the network. What it records -- not a clock -- decides the assertions.
 */
final class TtInstance extends MQClientInstance
{
    /** @var array<string,list<MessageQueue>> topic => the queues the route serves this round */
    public array $routes = [];

    /** @var list<string> the clientIds GET_CONSUMER_LIST_BY_GROUP answers with */
    public array $peers = [];

    public function getTopicSubscribeInfo(string $topic): array
    {
        return $this->routes[$topic] ?? [];
    }

    /** @return list<string> */
    public function getConsumerIdListByGroup(string $topic, string $consumerGroup, ?int $timeoutMillis = null): ?array
    {
        return $this->peers;
    }

    public function queryConsumerOffset(
        string $consumerGroup,
        MessageQueue $mq,
        int $timeoutMillis = 5000,
        ?string $addr = null,
        bool $setZeroIfNotFound = false,
    ): ?int {
        return null;
    }

    public function getMaxOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        return 0;
    }

    public function updateConsumerOffset(
        string $consumerGroup,
        MessageQueue $mq,
        int $commitOffset,
        int $timeoutMillis = 5000,
        ?string $addr = null,
    ): void {
        // Revocation persists a consumed offset here; the fixture has none, so nothing is expected.
        // Overridden anyway so that no offset RPC can ever leave this process.
    }
}

final class RunClientTopicThreshold
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

    // ---------------- fixture helpers ----------------

    /** @return list<MessageQueue> $count queues of one broker, numbered from $firstId */
    private function queues(string $topic, string $broker, int $count, int $firstId = 0): array
    {
        $out = [];
        for ($i = $firstId; $i < $firstId + $count; $i++) {
            $out[] = new MessageQueue($topic, $broker, $i);
        }
        return $out;
    }

    /**
     * The queue key the client itself uses for its per-queue tables (topic + broker + queueId), so a
     * fixture-seeded table and a table a rebalance round built can be compared by key.
     */
    private function keyOf(MessageQueue $mq): string
    {
        return $mq->topic . $mq->brokerName . (string) $mq->queueId;
    }

    /** @return array<string,MessageQueue> the pull-state table entries a rebalance would leave */
    private function stateOf(array $mqs): array
    {
        $out = [];
        foreach ($mqs as $mq) {
            $out[$this->keyOf($mq)] = $mq;
        }
        return $out;
    }

    /**
     * Give the consumer $n queues with live pull state, the way a rebalance round leaves them, and
     * return how many the table now holds (a repeated call grows the set).
     */
    private function ownQueues(DefaultMQPushConsumer $c, int $n, string $topic = 'TtTopic'): int
    {
        $table = (array) $this->prop($c, 'mqMap');
        foreach ($this->stateOf($this->queues($topic, 'b-tt', $n)) as $key => $mq) {
            $table[$key] = $mq;
        }
        $this->setProp($c, 'mqMap', $table);
        return count($table);
    }

    /** The POP variant: the live per-queue state sits in the POP table instead. */
    private function ownPopQueues(DefaultMQPushConsumer $c, int $n, string $topic = 'TtTopic'): int
    {
        $table = (array) $this->prop($c, 'popQueues');
        foreach ($this->queues($topic, 'b-tt', $n) as $mq) {
            $table[$this->keyOf($mq)] = new PopProcessQueue();
        }
        $this->setProp($c, 'popQueues', $table);
        return count($table);
    }

    /** A consumer with both budgets off and two hand-set knobs, so a stray division is visible. */
    private function configured(string $group): DefaultMQPushConsumer
    {
        $c = new DefaultMQPushConsumer($group);
        $c->pullThresholdForQueue = 4242;
        $c->pullThresholdSizeForQueue = 43;
        $c->pullThresholdForTopic = -1;
        $c->pullThresholdSizeForTopic = -1;
        return $c;
    }

    /** Run the division exactly as rebalance does. */
    private function divide(DefaultMQPushConsumer $c): void
    {
        $this->call($c, 'divideTopicThresholds', []);
    }

    /** The INFO lines a division wrote (the only ones this suite asserts on). */
    private function divisionLines(): array
    {
        return array_values(array_filter(
            $this->captured,
            static fn(string $l): bool => str_contains($l, 'INFO:')
                && str_contains($l, 'is changed from')
        ));
    }

    /** Whether the division wrote an INFO line carrying this exact wording. */
    private function sawDivisionLine(string $needle): bool
    {
        foreach ($this->divisionLines() as $line) {
            if (str_contains($line, $needle)) {
                return true;
            }
        }
        return false;
    }

    private function captureLogs(): void
    {
        Logger::setLevel(Logger::DEBUG);
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

    /** A consumer wired to the rebalance double: started, subscribed, and owning what the route serves. */
    private function rebalancing(TtInstance $inst, string $group, string $topic): DefaultMQPushConsumer
    {
        $c = new DefaultMQPushConsumer($group);
        $c->clientId = $group . '-cid';
        $inst->peers = [$c->clientId];
        $c->subscribe($topic);
        $this->setProp($c, 'started', true);
        $c->mqClient = $inst;
        return $c;
    }

    // ================================================================ 1. each enabled budget divides

    /**
     * An enabled budget is split over the owned queues and only the knob it belongs to moves, with
     * the INFO line naming the old and the new number. Reverting the division (or letting one budget
     * write the other knob) reddens one of these legs.
     */
    private function testEachEnabledBudgetDividesOverTheOwnedQueues(): void
    {
        $this->captureLogs();
        try {
            $c = $this->configured('GID-TT-SPLIT');
            $c->pullThresholdForTopic = 1000;
            $this->checkSame(4, $this->ownQueues($c, 4), 'four queues hold live pull state');

            $this->divide($c);
            $this->checkSame(250, $c->pullThresholdForQueue,
                'the 1000-message budget becomes 250 per queue over 4 owned queues');
            $this->checkSame(43, $c->pullThresholdSizeForQueue,
                'the byte budget is off, so the user byte knob is not divided');
            $this->checkSame(1000, $c->pullThresholdForTopic,
                'the topic budget itself stays the budget: it is the numerator, not a cursor');
            $this->check($this->sawDivisionLine('The pullThresholdForQueue is changed from 4242 to 250'),
                'the count division reports old and new number');
            $this->checkSame(1, count($this->divisionLines()),
                'the off budget logs no byte division line at all');

            // The byte budget divides over the same table, and leaves the count knob alone.
            $this->captured = [];
            $c->pullThresholdSizeForTopic = 100;
            $this->divide($c);
            $this->checkSame(25, $c->pullThresholdSizeForQueue,
                'the 100 MB budget becomes 25 MB per queue over the same 4 queues');
            $this->checkSame(250, $c->pullThresholdForQueue, 'the count knob already written stays put');
            $this->check($this->sawDivisionLine('The pullThresholdSizeForQueue is changed from 43 to 25'),
                'the size division reports old and new number in MB');
            $this->checkSame(1, count($this->divisionLines()), 'one write, one INFO line');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 2. both budgets off

    /**
     * Paired control for the whole contract: with both budgets off nothing is divided, so the user's
     * per-queue values survive any queue count. An unconditional division reddens this.
     */
    private function testBothBudgetsOffLeaveTheKnobsUntouched(): void
    {
        $this->captureLogs();
        try {
            $c = $this->configured('GID-TT-OFF');
            $this->ownQueues($c, 4);
            $this->divide($c);
            $this->checkSame(4242, $c->pullThresholdForQueue,
                'an off count budget must not touch the count knob');
            $this->checkSame(43, $c->pullThresholdSizeForQueue,
                'an off size budget must not touch the size knob');
            $this->checkSame([], $this->divisionLines(), 'no division, no INFO line');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 3. the floor

    /**
     * A budget smaller than the queue count still leaves a usable gate: the quotient is floored at 1,
     * never 0 -- 0 per queue would mean the budget cannot be honoured at all, and a floor of 1 is the
     * most the split can promise.
     */
    private function testABudgetSmallerThanTheQueueCountFloorsAtOne(): void
    {
        $this->captureLogs();
        try {
            $c = $this->configured('GID-TT-FLOOR');
            $c->pullThresholdForTopic = 3;
            $this->ownQueues($c, 4);
            $this->divide($c);
            $this->checkSame(1, $c->pullThresholdForQueue, 'the 3/4 quotient floors at 1, never 0');
            $this->check($this->sawDivisionLine('The pullThresholdForQueue is changed from 4242 to 1'),
                'the floored quotient is reported like any other division');

            // Same for the byte budget, in MB.
            $c = $this->configured('GID-TT-FLOOR2');
            $c->pullThresholdSizeForTopic = 1;
            $this->ownQueues($c, 4);
            $this->divide($c);
            $this->checkSame(1, $c->pullThresholdSizeForQueue, 'the 1 MB/4 quotient floors at 1 MB');
            $this->checkSame(4242, $c->pullThresholdForQueue, 'the off count budget stays the user value');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 4. owning nothing

    /**
     * With no queue owned there is no divisor, so both knobs stay exactly as the user set them rather
     * than collapsing to a quotient. Without the guard the division below would die on a zero
     * divisor instead of writing these numbers.
     */
    private function testOwningNoQueueKeepsBothKnobsAsTheUserSetThem(): void
    {
        $c = $this->configured('GID-TT-NO-QUEUE');
        $c->pullThresholdForTopic = 1000;
        $c->pullThresholdSizeForTopic = 100;
        $this->checkSame(0, count((array) $this->prop($c, 'mqMap')), 'nothing owns live pull state');

        $this->divide($c);
        $this->checkSame(4242, $c->pullThresholdForQueue, 'no owned queue, so the count knob is untouched');
        $this->checkSame(43, $c->pullThresholdSizeForQueue, 'no owned queue, so the size knob is untouched');

        // The POP table is the state under POP, so an empty POP table is the same no-divisor case.
        $pop = $this->configured('GID-TT-NO-POP-QUEUE');
        $pop->popMode = true;
        $pop->pullThresholdForTopic = 1000;
        $pop->pullThresholdSizeForTopic = 100;
        $this->ownQueues($pop, 4);
        $this->divide($pop);
        $this->checkSame(4242, $pop->pullThresholdForQueue,
            'under POP an empty POP state table owns nothing, whatever the pull table holds');
    }

    // ================================================================ 5. idempotence + the divisor

    /**
     * The numerator is always the topic budget, so the same queue count re-derives the same gate
     * (writing nothing a second time), and a grown set is divided from the budget rather than from
     * the gate the previous round wrote: 1000/8 = 125, not 250/8 = 31.
     */
    private function testDivisionRepeatsAndTracksTheQueueCount(): void
    {
        $this->captureLogs();
        try {
            $c = $this->configured('GID-TT-REPEAT');
            $c->pullThresholdForTopic = 1000;
            $c->pullThresholdSizeForTopic = 100;
            $this->ownQueues($c, 4);

            $this->divide($c);
            $this->checkSame(250, $c->pullThresholdForQueue, '1000 over 4 queues is 250');
            $this->checkSame(25, $c->pullThresholdSizeForQueue, '100 MB over 4 queues is 25 MB');

            $this->captured = [];
            $this->divide($c);
            $this->checkSame(250, $c->pullThresholdForQueue,
                're-dividing writes the same number: the budget is the numerator, not the knob');
            $this->checkSame(25, $c->pullThresholdSizeForQueue, 'and the byte knob repeats too');
            $this->checkSame([], $this->divisionLines(),
                'an unchanged number is not written, so it logs nothing');

            $this->checkSame(8, $this->ownQueues($c, 8), 'four more queues join the live state');
            $this->divide($c);
            $this->checkSame(125, $c->pullThresholdForQueue, 'the grown set gives 1000/8 = 125, not 250/8');
            $this->checkSame(12, $c->pullThresholdSizeForQueue, 'and 100/8 = 12 MB, not 25/8');
            $this->check($this->sawDivisionLine('The pullThresholdForQueue is changed from 250 to 125'),
                'the INFO line carries the previous quotient as the number it replaced');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 6. across all topics

    /**
     * The divisor is every queue this client owns with live pull state, whatever topic it belongs to:
     * a topic budget is a budget for this client as a whole. Two queues per topic over two topics
     * therefore divide by 4, and a per-topic divisor would write 500 here.
     */
    private function testTheDivisorCountsQueuesAcrossAllTopics(): void
    {
        $c = $this->configured('GID-TT-ACROSS-TOPICS');
        $c->pullThresholdForTopic = 1000;
        $this->checkSame(2, $this->ownQueues($c, 2, 'TtTopicA'), 'two queues of the first topic');
        $this->checkSame(4, $this->ownQueues($c, 2, 'TtTopicB'), 'plus two queues of a second topic');

        $this->divide($c);
        $this->checkSame(250, $c->pullThresholdForQueue,
            '1000 over all 4 owned queues, not over the 2 queues of one topic');
    }

    // ================================================================ 7. which table is the divisor

    /**
     * Under POP the live per-queue state is the POP table, so that is the divisor -- and here the two
     * tables disagree on purpose. Paired control: the identical fixture with POP off must divide by
     * the pull table instead, so reading one table for both modes cannot pass both legs.
     */
    private function testPopModeDividesByThePopStateTable(): void
    {
        $c = $this->configured('GID-TT-POP-TABLE');
        $c->popMode = true;
        $c->pullThresholdForTopic = 1000;
        $this->checkSame(8, $this->ownQueues($c, 8), 'the pull table holds 8 queues');
        $this->checkSame(4, $this->ownPopQueues($c, 4), 'the POP table holds 4 live queues');

        $this->divide($c);
        $this->checkSame(250, $c->pullThresholdForQueue,
            '1000 over the 4 queues of the live POP state, not over the 8 of the pull table');

        $pull = $this->configured('GID-TT-PULL-TABLE');
        $pull->pullThresholdForTopic = 1000;
        $this->ownQueues($pull, 8);
        $this->ownPopQueues($pull, 4);
        $this->divide($pull);
        $this->checkSame(125, $pull->pullThresholdForQueue,
            'the same tables without POP divide by the pull table instead');
    }

    // ================================================================ 8. the rebalance wiring

    /**
     * The division belongs to the round that CHANGED the owned set and runs AFTER that round rebuilt
     * the pull state:
     *   - the first round splits the budget over the queues it owns;
     *   - an identical round owns the same queues, so it must leave a hand-set knob alone;
     *   - an equal-size swap (one queue given up on the first broker, one taken on the second) changed
     *     the SET while keeping the count, so it re-divides -- a length comparison would miss it;
     *   - a shrink divides by the count THIS round produced. Dividing before the state update would
     *     still see the four old queues and write 250.
     */
    private function testRebalanceDividesOnlyOnAChangedAssignmentAndAfterTheStateUpdate(): void
    {
        $topic = 'TtRebalance';
        $inst = new TtInstance('tt-inst-' . uniqid(), []);
        $c = $this->rebalancing($inst, 'GID-TT-REBALANCE', $topic);
        $c->pullThresholdForTopic = 1000;
        $c->pullThresholdSizeForTopic = 100;
        $c->pullThresholdForQueue = 1000;
        $c->pullThresholdSizeForQueue = 100;
        $inst->routes[$topic] = $this->queues($topic, 'b1', 4);

        $c->doRebalance();
        $this->checkSame(4, $c->assignedQueueCount(), 'the round owns the four queues the route serves');
        $this->checkSame(4, count((array) $this->prop($c, 'mqMap')), 'and all four hold live pull state');
        $this->checkSame(250, $c->pullThresholdForQueue, 'the budget is split over what this round owns');
        $this->checkSame(25, $c->pullThresholdSizeForQueue, 'both enabled budgets divide in the same round');

        // Paired control: the identical round changes nothing, so it does not divide either.
        $c->pullThresholdForQueue = 777;
        $c->doRebalance();
        $this->checkSame(777, $c->pullThresholdForQueue,
            'an unchanged assignment must leave a hand-set knob alone: the division is change-gated');
        $this->checkSame(25, $c->pullThresholdSizeForQueue, 'and the other knob keeps its divided number');

        // An equal-size swap: 3 queues on the first broker plus 1 on the second, so the COUNT is still
        // 4 while the SET changed.
        $inst->routes[$topic] = array_merge(
            $this->queues($topic, 'b1', 3),
            $this->queues($topic, 'b2', 1),
        );
        $c->doRebalance();
        $this->checkSame(4, count((array) $this->prop($c, 'mqMap')), 'the swap leaves four live queues');
        $this->checkSame(250, $c->pullThresholdForQueue,
            'an equal-size swap re-divides: the change test compares the set, not the count');

        // A shrink divides by the count this round produced (1000/2), not by the four it started from.
        $c->pullThresholdForQueue = 777;
        $inst->routes[$topic] = $this->queues($topic, 'b1', 2);
        $c->doRebalance();
        $this->checkSame(2, count((array) $this->prop($c, 'mqMap')), 'the shrink leaves two live queues');
        $this->checkSame(500, $c->pullThresholdForQueue,
            '1000 over the two queues left, so the division ran after the state update');
        $this->checkSame(50, $c->pullThresholdSizeForQueue, 'the byte budget follows the same new count');
        $live = array_keys((array) $this->prop($c, 'mqMap'));
        sort($live);
        $kept = array_keys($this->stateOf($this->queues($topic, 'b1', 2)));
        sort($kept);
        $this->checkSame($kept, $live,
            'the two given-up queues left the pull state, so the divisor this round saw was 2');
    }

    // ================================================================ 9. the POP assignment wiring

    /**
     * The broker-side POP assignment owns the queue set on that leg (doRebalance delegates to it and
     * returns), so it divides the same way: over the POP table, on a changed set only, after the POP
     * state was rebuilt.
     */
    private function testPopAssignmentDividesOnAChangedAssignment(): void
    {
        $topic = 'TtPopAssign';
        $c = new DefaultMQPushConsumer('GID-TT-POP-ASSIGN');
        $c->popMode = true;
        $c->clientRebalance = false;
        $c->pullThresholdForTopic = 1000;
        $c->pullThresholdSizeForTopic = 100;
        $c->pullThresholdForQueue = 1000;
        $c->pullThresholdSizeForQueue = 100;
        $this->checkSame(true, $c->usesBrokerPopAssignment(),
            'this fixture really takes the broker-side POP leg');

        $answer = function (int $n) use ($topic): array {
            $out = [];
            foreach ($this->queues($topic, 'b1', $n) as $mq) {
                $out[] = ['mq' => $mq, 'mode' => MessageRequestMode::POP];
            }
            return $out;
        };

        $c->applyPopAssignment($topic, $answer(4));
        $this->checkSame(4, $c->popQueueCount(), 'the answer became four live POP queues');
        $this->checkSame(250, $c->pullThresholdForQueue, 'the budget splits over the POP queues taken');
        $this->checkSame(25, $c->pullThresholdSizeForQueue, 'the byte budget splits in the same round');

        // Paired control: the same answer again changes nothing, so nothing is re-divided.
        $c->pullThresholdForQueue = 777;
        $c->applyPopAssignment($topic, $answer(4));
        $this->checkSame(777, $c->pullThresholdForQueue,
            'an unchanged POP answer must not divide: a hand-set knob survives');

        // A shrunken answer divides by the POP state this round rebuilt.
        $c->applyPopAssignment($topic, $answer(2));
        $this->checkSame(2, $c->popQueueCount(), 'the shrunken answer leaves two POP queues');
        $this->checkSame(500, $c->pullThresholdForQueue, '1000 over the two queues this round kept');
        $this->checkSame(50, $c->pullThresholdSizeForQueue, '100 MB over the same two queues');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testEachEnabledBudgetDividesOverTheOwnedQueues();
        $this->testBothBudgetsOffLeaveTheKnobsUntouched();
        $this->testABudgetSmallerThanTheQueueCountFloorsAtOne();
        $this->testOwningNoQueueKeepsBothKnobsAsTheUserSetThem();
        $this->testDivisionRepeatsAndTracksTheQueueCount();
        $this->testTheDivisorCountsQueuesAcrossAllTopics();
        $this->testPopModeDividesByThePopStateTable();
        $this->testRebalanceDividesOnlyOnAChangedAssignmentAndAfterTheStateUpdate();
        $this->testPopAssignmentDividesOnAChangedAssignment();
        return $this->summary();
    }
}

exit((new RunClientTopicThreshold())->run());
