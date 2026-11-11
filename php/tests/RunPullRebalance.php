<?php

declare(strict_types=1);

/**
 * Pull-side rebalance contract suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunPullRebalance.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Java's pull consumer is not the "no rebalance" shape this port used to document:
 * ``DefaultMQPullConsumer.java:97`` declares ``enableRebalance = true`` and the shared
 * ``RebalanceService`` walks the registered topics, allocates this instance's queues and calls
 * ``messageQueueChanged(topic, mqAll, mqDivided)``. This port has no background thread, so the
 * round is ``tickRebalance()``, driven by the caller's loop with an explicit clock.
 *
 * The assertions pin the three things that are easy to get wrong:
 *
 *  * ``enableRebalance == false`` must still report **balanced**
 *    (``DefaultMQPullConsumerImpl#tryRebalance`` :398-402) -- reporting "unbalanced" would pin
 *    the loop at minInterval and burn a round-trip every second for nothing.
 *  * the listener fires **only when the allocation changed** and receives
 *    ``(topic, mqAll, mqDivided)`` -- the arguments ``RebalancePullImpl`` :44-53 passes.
 *  * a throwing listener must not escape: Java logs and swallows it (:49-51) so one bad
 *    callback cannot kill the loop and freeze every allocation.
 *
 * The fake client answers only the two reads a rebalance needs (route, consumer list); nothing
 * here goes near a socket, and every clock value is handed in rather than slept through.
 */

require_once __DIR__ . '/../bootstrap.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');
// The debug/warning rounds log on purpose; keep the files out of the repository.
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_pullreb_logs_' . getmypid());

use RocketMQ\Client\AllocateMessageQueueAveragely;
use RocketMQ\Client\AllocateMessageQueueStrategy;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\MessageQueueListener;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\MessageModel;

const PRB_TOPIC = 'TopicPullReb';

/** Instance double: the two reads a rebalance performs, scripted and counted. */
final class PrbInstance extends MQClientInstance
{
    /** @var list<MessageQueue> */
    public array $subscribe = [];

    /** @var list<string>|null null = "consumer list unavailable" */
    public ?array $cids = [];

    public int $subscribeCalls = 0;

    public function getTopicSubscribeInfo(string $topic): array
    {
        $this->subscribeCalls++;
        return array_values(array_filter(
            $this->subscribe,
            static fn(MessageQueue $mq): bool => $mq->topic === $topic,
        ));
    }

    public function getConsumerIdListByGroup(string $topic, string $consumerGroup, ?int $timeoutMillis = null): ?array
    {
        return $this->cids === null ? null : array_values($this->cids);
    }
}

/** Records each callback as (topic, mqAll keys, mqDivided keys), sets kept for order-free compares. */
final class PrbRecorder implements MessageQueueListener
{
    /** @var list<array{0:string,1:list<string>,2:list<string>}> */
    public array $calls = [];

    public function messageQueueChanged(string $topic, array $mqAll, array $mqDivided): void
    {
        $this->calls[] = [$topic, $this->keys($mqAll), $this->keys($mqDivided)];
    }

    /** @param list<MessageQueue> $mqs @return list<string> */
    private function keys(array $mqs): array
    {
        $keys = array_map(
            static fn(MessageQueue $mq): string => $mq->brokerName . '#' . $mq->queueId,
            $mqs,
        );
        sort($keys);
        return $keys;
    }
}

/** A listener that violates the contract on purpose. */
final class PrbBoom implements MessageQueueListener
{
    public function messageQueueChanged(string $topic, array $mqAll, array $mqDivided): void
    {
        throw new \RuntimeException('listener exploded');
    }
}

/** Always hands out nothing: pins the "nothing -> nothing is not a change" rule. */
final class PrbEmptyStrategy implements AllocateMessageQueueStrategy
{
    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        return [];
    }

    public function getName(): string
    {
        return 'EMPTY';
    }
}

/** Wraps the real strategy so a test can prove it was (not) consulted. */
final class PrbCountingStrategy implements AllocateMessageQueueStrategy
{
    public int $calls = 0;

    public function __construct(private AllocateMessageQueueStrategy $inner)
    {
    }

    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        $this->calls++;
        return $this->inner->allocate($consumerGroup, $currentCID, $mqAll, $cidAll);
    }

    public function getName(): string
    {
        return $this->inner->getName();
    }
}

final class RunPullRebalance
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

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

    // ---------------- helpers ----------------

    /** @param list<MessageQueue> $queues */
    private function setPrivate(object $o, string $name, mixed $value): void
    {
        $p = new \ReflectionProperty(DefaultMQPullConsumer::class, $name);
        $p->setAccessible(true);
        $p->setValue($o, $value);
    }

    /** @return list<MessageQueue> */
    private function queues(int $n, string $topic = PRB_TOPIC, string $broker = 'broker-a'): array
    {
        $out = [];
        for ($i = 0; $i < $n; $i++) {
            $out[] = new MessageQueue($topic, $broker, $i);
        }
        return $out;
    }

    /**
     * A consumer wired to the instance double, marked started (the rebalance's gate) with TOPIC
     * registered -- exactly the state Java's shared RebalanceService requires.
     *
     * @param list<MessageQueue> $mqAll
     * @param list<string>|null $cidAll
     * @return array{0:DefaultMQPullConsumer,1:PrbInstance}
     */
    private function consumer(
        array $mqAll,
        ?array $cidAll,
        string $model = MessageModel::CLUSTERING,
        string $clientId = 'cid-A',
        ?MessageQueueListener $listener = null,
    ): array {
        $c = new DefaultMQPullConsumer('PG_PullReb');
        $c->clientId = $clientId;
        $c->messageModel = $model;
        $inst = new PrbInstance('prb-' . uniqid(), []);
        $inst->subscribe = $mqAll;
        $inst->cids = $cidAll;
        $c->mqClient = $inst;
        $this->setPrivate($c, 'started', true);
        $c->registerTopic(PRB_TOPIC);
        if ($listener !== null) {
            $c->setMessageQueueListener($listener);
        }
        return [$c, $inst];
    }

    /** @param list<MessageQueue> $queues @return list<int> */
    private function queueIds(array $queues): array
    {
        $ids = array_map(static fn(MessageQueue $mq): int => $mq->queueId, $queues);
        sort($ids);
        return $ids;
    }

    // ================================================================ 1. switch

    private function testEnableRebalanceDefaultsToJavaTrue(): void
    {
        $c = new DefaultMQPullConsumer('PG_PullReb');
        // Java DefaultMQPullConsumer.java:97 `private boolean enableRebalance = true`.
        $this->checkSame(true, $c->isEnableRebalance(), 'enableRebalance defaults to true');
    }

    private function testSetterRoundTrips(): void
    {
        $c = new DefaultMQPullConsumer('PG_PullReb');
        $c->setEnableRebalance(false);
        $this->checkSame(false, $c->isEnableRebalance(), 'setEnableRebalance(false) turns the switch off');
        $c->setEnableRebalance(true);
        $this->checkSame(true, $c->isEnableRebalance(), 'setEnableRebalance(true) turns it back on');
    }

    private function testWaitAndMinIntervalAreClampedConfigurable(): void
    {
        $c = new DefaultMQPullConsumer('PG_PullReb');
        $this->checkSame(20000, $c->getRebalanceWaitMillis(), 'wait interval defaults to 20000 ms');
        $this->checkSame(1000, $c->getRebalanceMinIntervalMillis(), 'min interval defaults to 1000 ms');
        $c->setRebalanceWaitMillis(0);
        $c->setRebalanceMinIntervalMillis(-5);
        $this->checkSame(1, $c->getRebalanceWaitMillis(), 'a zero wait period would spin the loop, so it clamps to 1 ms');
        $this->checkSame(1, $c->getRebalanceMinIntervalMillis(), 'a negative min interval clamps to 1 ms');
    }

    // ================================================================ 2. switch off

    private function testDisabledReportsBalancedAndNeverAllocates(): void
    {
        [$c] = $this->consumer($this->queues(4), ['cid-A', 'cid-B']);
        $strategy = new PrbCountingStrategy($c->allocateMessageQueueStrategy);
        $c->setAllocateMessageQueueStrategy($strategy);
        $c->setEnableRebalance(false);

        // Java tryRebalance returns **true** when the switch is off (:398-402).
        $this->checkSame(true, $c->doRebalance(), 'switch off reports balanced, not endless-unbalanced');
        $this->checkSame(0, $strategy->calls, 'no allocation may run while the switch is off');
        $this->checkSame([], $c->getRebalanceTable(), 'nothing may be cached either');
    }

    private function testDisabledKeepsFetchInBalanceOnTheSpot(): void
    {
        [$c] = $this->consumer($this->queues(4), ['cid-A', 'cid-B']);
        $this->setPrivate($c, 'rebalanceTable', [PRB_TOPIC => []]); // a stale table must be ignored while off
        $c->setEnableRebalance(false);
        $this->checkSame([0, 1], $this->queueIds($c->fetchMessageQueuesInBalance(PRB_TOPIC)),
            'switch off falls back to on-the-spot averaging');
    }

    // ================================================================ 3. broadcasting

    private function testBroadcastingFirstRoundNotifiesWithAllQueues(): void
    {
        $rec = new PrbRecorder();
        [$c] = $this->consumer($this->queues(3), ['cid-A'], MessageModel::BROADCASTING, 'cid-A', $rec);
        $this->checkSame(false, $c->doRebalance(), 'the first round changes the empty table');
        $this->checkSame(1, count($rec->calls), 'the first round notifies exactly once');
        [$topic, $mqAll, $divided] = $rec->calls[0];
        $this->checkSame(PRB_TOPIC, $topic, 'the callback names the topic');
        $this->checkSame(['broker-a#0', 'broker-a#1', 'broker-a#2'], $mqAll, 'broadcasting hands every queue as mqAll');
        $this->checkSame($mqAll, $divided, 'broadcasting hands the same set as mqDivided');
    }

    private function testBroadcastingSecondRoundIsBalancedAndSilent(): void
    {
        $rec = new PrbRecorder();
        [$c] = $this->consumer($this->queues(3), ['cid-A'], MessageModel::BROADCASTING, 'cid-A', $rec);
        $c->doRebalance();
        $this->checkSame(true, $c->doRebalance(), 'nothing changed, so the second round is balanced');
        $this->checkSame(1, count($rec->calls), 'messageQueueChanged fires only on a real change');
    }

    // ================================================================ 4. clustering

    private function testThisInstanceTakesItsShare(): void
    {
        [$c] = $this->consumer($this->queues(4), ['cid-A', 'cid-B']);
        $c->doRebalance();
        $this->checkSame([0, 1], $this->queueIds($c->getRebalanceTable()[PRB_TOPIC]),
            'averagely: A (sorted first) takes the first half');
    }

    private function testTwoInstancesGetDisjointShares(): void
    {
        [$a] = $this->consumer($this->queues(5), ['cid-A', 'cid-B'], MessageModel::CLUSTERING, 'cid-A');
        [$b] = $this->consumer($this->queues(5), ['cid-A', 'cid-B'], MessageModel::CLUSTERING, 'cid-B');
        $a->doRebalance();
        $b->doRebalance();
        $sa = $this->queueIds($a->getRebalanceTable()[PRB_TOPIC]);
        $sb = $this->queueIds($b->getRebalanceTable()[PRB_TOPIC]);
        $this->checkSame([], array_values(array_intersect($sa, $sb)),
            'the same queue must never be owned twice');
        $both = array_merge($sa, $sb);
        sort($both);
        $this->checkSame([0, 1, 2, 3, 4], $both, 'together the two instances cover every queue');
    }

    private function testNewQueueTriggersExactlyOneNotification(): void
    {
        $rec = new PrbRecorder();
        [$c, $inst] = $this->consumer($this->queues(2), ['cid-A'], MessageModel::CLUSTERING, 'cid-A', $rec);
        $c->doRebalance();
        $this->checkSame(1, count($rec->calls), 'the initial allocation notifies once');

        $inst->subscribe = $this->queues(3); // a queue was added
        $this->checkSame(false, $c->doRebalance(), 'a new queue makes the round unbalanced');
        $this->checkSame(2, count($rec->calls), 'the new queue notifies exactly once more');
        [, $mqAll, $divided] = $rec->calls[1];
        $this->checkSame(['broker-a#0', 'broker-a#1', 'broker-a#2'], $mqAll, 'the callback reports the widened mqAll');
        $this->checkSame($mqAll, $divided, 'a single-consumer group owns every queue');
    }

    private function testMissingConsumerIdListIsBalancedAndSilent(): void
    {
        $rec = new PrbRecorder();
        [$c] = $this->consumer($this->queues(3), null, MessageModel::CLUSTERING, 'cid-A', $rec);
        // Java (:300-302) only warns; it does not notify and leaves balanced at true.
        $this->checkSame(true, $c->doRebalance(), 'a missing consumer list still reports balanced');
        $this->checkSame([], $rec->calls, 'no consumer list -> say nothing, keep the old allocation');
        $this->checkSame([], $c->getRebalanceTable(), 'the table stays untouched');
    }

    private function testFirstRoundWithAnEmptyShareIsSilentAndBalanced(): void
    {
        // Java's updateProcessQueueTableInRebalance counts only table additions/removals as
        // a change: "nothing -> nothing" is no change, so an empty opening share fires no
        // callback and the round counts as balanced.
        $rec = new PrbRecorder();
        [$c] = $this->consumer($this->queues(3), ['cid-A', 'cid-B'], MessageModel::CLUSTERING, 'cid-A', $rec);
        $real = $c->allocateMessageQueueStrategy;
        $c->setAllocateMessageQueueStrategy(new PrbEmptyStrategy());

        $this->checkSame(true, $c->doRebalance(), 'nothing -> nothing is a balanced round');
        $this->checkSame([], $rec->calls, 'an empty opening share must not fire the listener');
        $this->checkSame([], $c->getRebalanceTable()[PRB_TOPIC], 'the table keeps the topic with an empty share');

        // The share appearing later IS a change (the table grew from nothing).
        $c->setAllocateMessageQueueStrategy($real);
        $this->checkSame(false, $c->doRebalance(), 'growing from nothing to a share is a change');
        $this->checkSame(1, count($rec->calls), 'the share appearing later notifies exactly once');
    }

    private function testVanishedTopicClearsTableAndNotifiesEmpty(): void
    {
        $rec = new PrbRecorder();
        [$c, $inst] = $this->consumer($this->queues(2), ['cid-A'], MessageModel::CLUSTERING, 'cid-A', $rec);
        $c->doRebalance();
        $this->checkSame(1, count($rec->calls), 'the first round notifies');

        $inst->subscribe = [];
        // Java (:286-294) notifies with two empty sets and then `break`s, leaving balanced true.
        $this->checkSame(true, $c->doRebalance(), 'an absent topic is not unbalanced');
        $this->checkSame([], $c->getRebalanceTable()[PRB_TOPIC], 'the vanished topic clears its share');
        [$topic, $mqAll, $divided] = $rec->calls[1];
        $this->checkSame(PRB_TOPIC, $topic, 'the empty notification still names the topic');
        $this->checkSame([], $mqAll, 'the vanished topic reports an empty mqAll');
        $this->checkSame([], $divided, 'the vanished topic reports an empty mqDivided');
    }

    // ================================================================ 5. listener safety

    private function testAThrowingListenerDoesNotEscape(): void
    {
        [$c] = $this->consumer($this->queues(2), ['cid-A'], MessageModel::CLUSTERING, 'cid-A', new PrbBoom());
        // Java swallows it (RebalancePullImpl:49-51); the call must simply return.
        $this->checkSame(false, $c->doRebalance(), 'the throwing listener does not escape the round');
        $this->checkSame(2, count($c->getRebalanceTable()[PRB_TOPIC]), 'the table is still updated');
    }

    private function testSameQueuesInAnotherOrderDoNotNotify(): void
    {
        $rec = new PrbRecorder();
        [$c, $inst] = $this->consumer($this->queues(3), ['cid-A'], MessageModel::CLUSTERING, 'cid-A', $rec);
        $c->doRebalance();
        $this->checkSame(1, count($rec->calls), 'the initial allocation notifies once');

        $inst->subscribe = array_reverse($this->queues(3));
        $this->checkSame(true, $c->doRebalance(), 'the same set in another order is not a change');
        $this->checkSame(1, count($rec->calls), 'an order-only difference stays silent');
    }

    // ================================================================ 6. fetch in balance

    private function testTableWinsOverOnTheSpotComputation(): void
    {
        [$c] = $this->consumer($this->queues(4), ['cid-A', 'cid-B']);
        $this->setPrivate($c, 'rebalanceTable', [
            PRB_TOPIC => [new MessageQueue(PRB_TOPIC, 'broker-a', 3)],
        ]);
        $this->checkSame([3], $this->queueIds($c->fetchMessageQueuesInBalance(PRB_TOPIC)),
            "the rebalance's product is authoritative");
    }

    private function testTableAnswerIsSortedAndIsolatedFromCaller(): void
    {
        [$c] = $this->consumer($this->queues(4), ['cid-A']);
        $this->setPrivate($c, 'rebalanceTable', [
            PRB_TOPIC => [
                new MessageQueue(PRB_TOPIC, 'broker-b', 1),
                new MessageQueue(PRB_TOPIC, 'broker-a', 2),
            ],
        ]);
        $got = $c->fetchMessageQueuesInBalance(PRB_TOPIC);
        $this->checkSame(['broker-a#2', 'broker-b#1'], array_map(
            static fn(MessageQueue $mq): string => $mq->brokerName . '#' . $mq->queueId,
            $got,
        ), 'the table answer comes back in house sort order');
        $got = [];
        $this->checkSame(2, count($c->getRebalanceTable()[PRB_TOPIC]),
            'callers must not mutate the table');
    }

    private function testGetRebalanceTableReturnsACopy(): void
    {
        [$c] = $this->consumer($this->queues(2), ['cid-A']);
        $c->doRebalance();
        $snapshot = $c->getRebalanceTable();
        $snapshot[PRB_TOPIC] = [];
        $this->checkSame(2, count($c->getRebalanceTable()[PRB_TOPIC]),
            'clearing the snapshot leaves the stored table untouched');
    }

    private function testUnfilledTopicStillComputesOnTheSpot(): void
    {
        [$c] = $this->consumer($this->queues(4), ['cid-A', 'cid-B']);
        $this->checkSame([], $c->getRebalanceTable(), 'nothing has been rebalanced yet');
        $this->checkSame([0, 1], $this->queueIds($c->fetchMessageQueuesInBalance(PRB_TOPIC)),
            'an unfilled topic computes its share on the spot');
    }

    // ================================================================ 7. register listener + topic

    private function testRegisterMessageQueueListenerRegistersBoth(): void
    {
        $c = new DefaultMQPullConsumer('PG_PullReb');
        $rec = new PrbRecorder();
        $c->registerMessageQueueListener(PRB_TOPIC, $rec);
        $this->checkSame([PRB_TOPIC], $c->getRegisterTopics(), 'the topic joins the rebalance walk');
        $this->checkSame($rec, $c->messageQueueListener, 'the listener is installed by the same call');
    }

    private function testNullListenerKeepsTheCurrentOne(): void
    {
        $c = new DefaultMQPullConsumer('PG_PullReb');
        $rec = new PrbRecorder();
        $c->setMessageQueueListener($rec);
        $c->registerMessageQueueListener(PRB_TOPIC, null);
        $this->checkSame([PRB_TOPIC], $c->getRegisterTopics(), 'the topic is still registered');
        $this->checkSame($rec, $c->messageQueueListener, 'Java only overwrites the listener when non-null');
    }

    private function testBlankTopicIsIgnoredEntirely(): void
    {
        $c = new DefaultMQPullConsumer('PG_PullReb');
        $rec = new PrbRecorder();
        $c->registerMessageQueueListener('   ', $rec);
        $this->checkSame([], $c->getRegisterTopics(), 'a blank topic never joins the walk');
        $this->checkSame(null, $c->messageQueueListener, 'a blank topic installs nothing');
    }

    // ================================================================ 8. tick pacing

    private function testTickBeforeStartDoesNothing(): void
    {
        $c = new DefaultMQPullConsumer('PG_PullReb');
        $inst = new PrbInstance('prb-' . uniqid(), []);
        $c->mqClient = $inst;
        $c->registerTopic(PRB_TOPIC);
        $this->checkSame(true, $c->tickRebalance(1000.0), 'before start the beat reports balanced');
        $this->checkSame(0, $inst->subscribeCalls, 'before start no round is attempted');
        $this->checkSame([], $c->getRebalanceTable(), 'before start the table stays empty');
    }

    private function testFirstBeatRunsImmediatelyThenWaitsMinInterval(): void
    {
        [$c, $inst] = $this->consumer($this->queues(3), ['cid-A']);
        $this->checkSame(false, $c->tickRebalance(1000.0),
            'the first beat runs at once and reports the changed allocation');
        $this->checkSame(1, $inst->subscribeCalls, 'the first beat really consulted the route');

        // Unbalanced -> the next beat is due at 1000 + 1000 ms = 1001 s.
        $this->checkSame(false, $c->tickRebalance(1000.5),
            'a beat before the min interval reports the previous answer');
        $this->checkSame(1, $inst->subscribeCalls, 'and runs no round');

        $this->checkSame(true, $c->tickRebalance(1001.0),
            'the confirmation round at minInterval is balanced');
        $this->checkSame(2, $inst->subscribeCalls, 'the confirmation round did run');
    }

    private function testBalancedRoundSchedulesTheFullWaitInterval(): void
    {
        [$c, $inst] = $this->consumer($this->queues(2), ['cid-A']);
        $c->tickRebalance(2000.0);      // unbalanced, due again at 2001
        $this->checkSame(true, $c->tickRebalance(2001.0), 'the confirmation round is balanced');

        $this->checkSame(true, $c->tickRebalance(2020.999), 'a beat just before the wait interval is idle');
        $this->checkSame(2, $inst->subscribeCalls, 'no round was run just before the wait interval');

        $c->tickRebalance(2021.0);
        $this->checkSame(3, $inst->subscribeCalls, 'the round runs once the wait interval has elapsed');
    }

    private function testDisabledTickKeepsBalancedAndSilent(): void
    {
        [$c, $inst] = $this->consumer($this->queues(2), ['cid-A']);
        $c->setEnableRebalance(false);
        $this->checkSame(true, $c->tickRebalance(3000.0), 'a disabled rebalance reports balanced');
        $this->checkSame(0, $inst->subscribeCalls, 'a disabled rebalance never reads the route');
        $this->checkSame([], $c->getRebalanceTable(), 'a disabled rebalance caches nothing');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testEnableRebalanceDefaultsToJavaTrue();
        $this->testSetterRoundTrips();
        $this->testWaitAndMinIntervalAreClampedConfigurable();

        $this->testDisabledReportsBalancedAndNeverAllocates();
        $this->testDisabledKeepsFetchInBalanceOnTheSpot();

        $this->testBroadcastingFirstRoundNotifiesWithAllQueues();
        $this->testBroadcastingSecondRoundIsBalancedAndSilent();

        $this->testThisInstanceTakesItsShare();
        $this->testTwoInstancesGetDisjointShares();
        $this->testNewQueueTriggersExactlyOneNotification();
        $this->testMissingConsumerIdListIsBalancedAndSilent();
        $this->testFirstRoundWithAnEmptyShareIsSilentAndBalanced();
        $this->testVanishedTopicClearsTableAndNotifiesEmpty();

        $this->testAThrowingListenerDoesNotEscape();
        $this->testSameQueuesInAnotherOrderDoNotNotify();

        $this->testTableWinsOverOnTheSpotComputation();
        $this->testTableAnswerIsSortedAndIsolatedFromCaller();
        $this->testGetRebalanceTableReturnsACopy();
        $this->testUnfilledTopicStillComputesOnTheSpot();

        $this->testRegisterMessageQueueListenerRegistersBoth();
        $this->testNullListenerKeepsTheCurrentOne();
        $this->testBlankTopicIsIgnoredEntirely();

        $this->testTickBeforeStartDoesNothing();
        $this->testFirstBeatRunsImmediatelyThenWaitsMinInterval();
        $this->testBalancedRoundSchedulesTheFullWaitInterval();
        $this->testDisabledTickKeepsBalancedAndSilent();

        return $this->summary();
    }
}

exit((new RunPullRebalance())->run());
