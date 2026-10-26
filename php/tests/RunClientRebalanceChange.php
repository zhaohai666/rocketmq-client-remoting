<?php

declare(strict_types=1);

/**
 * Assignment-change package suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientRebalanceChange.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * A rebalance round that moved one topic's queue set pays a fixed package, in this order and for the
 * moved topics only:
 *
 * 1. the moved topics' subscription version is re-stamped with the current wall clock, each with its
 *    own "<topic> rebalance changed, also update version: <old>, <new>" INFO line;
 * 2. the two topic-level cache budgets are re-split over the queue count THIS round produced;
 * 3. one heartbeat goes out to every broker immediately, carrying the version just stamped;
 * 4. the application's queue listener hears about the moved topics only, each with that topic's full
 *    route set and this instance's share of it.
 *
 * A round that changed nothing pays nothing at all, and a change is a change of the per-topic queue
 * SET: an equal-size swap onto another broker moved, a re-ordering of the same queues did not.
 *
 * Every positive leg sits next to a control that goes red when the package is reverted to the old
 * shape -- restamping every subscription, beating only on the periodic timer, telling the listener
 * about every topic each round, or detecting a change by counting queues. State is read back from the
 * subscription table, the recorded heartbeat payloads, the listener's call log and the captured
 * INFO lines; nothing here waits on a clock.
 */

require_once __DIR__ . '/../bootstrap.php';

// This port must never write logs under $HOME: pin the sink to a temp dir, and read the lines this
// suite asserts on through a handler instead of from a file.
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_rc_logs_' . getmypid());
// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Logger;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\MessageRequestMode;

const RC_BROKER_A = 'b1';
const RC_BROKER_B = 'b2';

/**
 * Instance double for the rebalance legs. It serves a mutable queue set per topic, answers the
 * consumer-list query with this single client, records every heartbeat it is asked to send together
 * with the payload it was handed, and keeps every offset RPC off the network. What it records -- not
 * a clock -- decides the assertions.
 */
final class RcInstance extends MQClientInstance
{
    /** @var array<string,list<MessageQueue>> topic => the queues the route serves this round */
    public array $routes = [];

    /** @var list<string> the clientIds GET_CONSUMER_LIST_BY_GROUP answers with */
    public array $peers = [];

    /** @var list<string> every address a heartbeat was sent to, in order */
    public array $heartbeatAddrs = [];

    /** @var list<HeartbeatData> the payload handed to each recorded heartbeat */
    public array $heartbeats = [];

    /** When set, every heartbeat throws, to drive the round that could not notify the brokers. */
    public bool $failHeartbeats = false;

    public function getTopicSubscribeInfo(string $topic): array
    {
        return $this->routes[$topic] ?? [];
    }

    /** @return list<string> */
    public function getConsumerIdListByGroup(string $topic, string $consumerGroup, ?int $timeoutMillis = null): ?array
    {
        return $this->peers;
    }

    public function getAllBrokerAddrs(): array
    {
        // Two brokers, master and slave: the package is one immediate round to EVERY one of them,
        // so the count of recorded addresses says what a single-broker fixture could not.
        return ['127.0.0.1:10911', '127.0.0.1:10912'];
    }

    public function sendHeartbeat(string $addr, HeartbeatData $heartbeatData, ?int $timeoutMillis = null): void
    {
        if ($this->failHeartbeats) {
            throw new \RuntimeException('fixture broker refused the heartbeat');
        }
        $this->heartbeatAddrs[] = $addr;
        $this->heartbeats[] = $heartbeatData;
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
        // A revoked queue persists its offset here; the fixture has none, so nothing is expected.
        // Overridden anyway so that no offset RPC can ever leave this process.
    }

    /** The subVersion this client's last recorded heartbeat carries for $topic, or null. */
    public function stampedVersion(string $topic): ?int
    {
        $last = $this->heartbeats[count($this->heartbeats) - 1] ?? null;
        if ($last === null) {
            return null;
        }
        foreach ($last->consumerDataSet as $cd) {
            foreach ($cd->subscriptionDataSet as $sub) {
                if ($sub->topic === $topic) {
                    return $sub->subVersion;
                }
            }
        }
        return null;
    }
}

/** Records every queue-listener callback so the "moved topics only" rule is read, not inferred. */
final class RcListener implements RocketMQ\Client\MessageQueueListener
{
    /** @var list<array{topic:string,all:int,divided:int}> */
    public array $calls = [];

    public bool $boom = false;

    public function messageQueueChanged(string $topic, array $mqAll, array $mqDivided): void
    {
        $this->calls[] = ['topic' => $topic, 'all' => count($mqAll), 'divided' => count($mqDivided)];
        if ($this->boom) {
            throw new \RuntimeException('the application listener said no');
        }
    }

    /** @return list<string> the topics it was told about, in the order it was told */
    public function topics(): array
    {
        return array_column($this->calls, 'topic');
    }

    public function reset(): void
    {
        $this->calls = [];
    }
}

final class RunClientRebalanceChange
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

    private function prop(object $c, string $name): mixed
    {
        $p = new \ReflectionProperty($c::class, $name);
        $p->setAccessible(true);
        return $p->getValue($c);
    }

    private function setProp(object $c, string $name, mixed $value): void
    {
        $p = new \ReflectionProperty($c::class, $name);
        $p->setAccessible(true);
        $p->setValue($c, $value);
    }

    private function call(object $c, string $name, array $args): mixed
    {
        $m = new \ReflectionMethod($c::class, $name);
        $m->setAccessible(true);
        return $m->invokeArgs($c, $args);
    }

    // ---------------- log capture ----------------

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

    /** @return list<string> captured lines carrying this tier's marker */
    private function linesOf(string $tier): array
    {
        return array_values(array_filter(
            $this->captured,
            static fn(string $l): bool => str_contains($l, $tier . ':')
        ));
    }

    /** @return list<string> the INFO lines that re-stamped a subscription version */
    private function versionLines(): array
    {
        return array_values(array_filter(
            $this->linesOf('INFO'),
            static fn(string $l): bool => str_contains($l, 'rebalance changed, also update version'),
        ));
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
     * A started push consumer wired to the double, subscribed to $topics with an old, unmistakable
     * subscription version on each, so a re-stamp is observable against the sentinel it replaced.
     *
     * @param list<string> $topics
     */
    private function pushConsumer(RcInstance $inst, string $group, array $topics, int $sentinel = 111): DefaultMQPushConsumer
    {
        $c = new DefaultMQPushConsumer($group);
        $c->clientId = $group . '-cid';
        $inst->peers = [$c->clientId];
        foreach ($topics as $topic) {
            $c->subscribe($topic);
            $c->subscriptionData[$topic]->subVersion = $sentinel;
        }
        $this->setProp($c, 'started', true);
        $c->mqClient = $inst;
        return $c;
    }

    /** The version the consumer's own subscription table holds for $topic. */
    private function versionOf(DefaultMQPushConsumer $c, string $topic): int
    {
        return (int) $c->subscriptionData[$topic]->subVersion;
    }

    /** @return array{0:DefaultMQPushConsumer,1:RcInstance,2:RcListener} a two-topic consumer and its doubles */
    private function twoTopicPush(string $group, string $topicA, string $topicB): array
    {
        $inst = new RcInstance('rc-' . $group . '-' . uniqid(), []);
        $c = $this->pushConsumer($inst, $group, [$topicA, $topicB]);
        $listener = new RcListener();
        $c->setMessageQueueListener($listener);
        $inst->routes[$topicA] = $this->queues($topicA, RC_BROKER_A, 2);
        $inst->routes[$topicB] = $this->queues($topicB, RC_BROKER_A, 2);
        return [$c, $inst, $listener];
    }

    // ================================================================ 1. only the moved topic is re-stamped

    /**
     * Growing one topic's queue set moves that topic alone, so that topic's version is the only one
     * re-stamped -- and it is stamped with the wall clock, not with a counter. The untouched sibling
     * keeps its sentinel, which is the control: a package that re-stamped every subscription would
     * move both.
     */
    private function testOnlyTheMovedTopicGetsANewVersion(): void
    {
        $this->captureLogs();
        try {
            [$c, $inst] = $this->twoTopicPush('GID-RC-STAMP', 'RcStampA', 'RcStampB');
            $c->doRebalance();
            $this->checkSame(4, $c->assignedQueueCount(), 'the first round owns two queues of each topic');
            $stampA = $this->versionOf($c, 'RcStampA');
            $stampB = $this->versionOf($c, 'RcStampB');
            $this->check($stampA > 111, 'the round that first took the queues stamped its version forward');
            $this->checkSame($stampA, $stampB,
                'both topics were stamped in the same round, from the same clock');

            // Now move one topic only. Each version is pinned to its own unmistakable sentinel first,
            // so "moved" is a real change of value rather than a wall-clock collision between legs.
            $c->subscriptionData['RcStampA']->subVersion = 111;
            $c->subscriptionData['RcStampB']->subVersion = 222;
            $this->captured = [];
            $inst->routes['RcStampA'] = $this->queues('RcStampA', RC_BROKER_A, 3);
            $c->doRebalance();

            $moved = $this->versionOf($c, 'RcStampA');
            $this->check($moved > 222, 'the moved topic was re-stamped with the current wall clock');
            $this->check($moved > 111, 'the stamp replaced the sentinel, it did not increment it');
            $this->checkSame(222, $this->versionOf($c, 'RcStampB'),
                'the topic whose set did not move keeps its old version');
            $this->checkSame(1, count($this->versionLines()),
                'exactly one version line, for the one topic that moved');
            $this->check($this->sawVersionLine('RcStampA rebalance changed, also update version: 111, ' . $moved),
                'the version line reads as old then new, as the contract words it');
        } finally {
            $this->releaseLogs();
        }
    }

    private function sawVersionLine(string $needle): bool
    {
        foreach ($this->versionLines() as $line) {
            if (str_contains($line, $needle)) {
                return true;
            }
        }
        return false;
    }

    // ================================================================ 2. an unchanged round pays nothing

    /**
     * A second round over the same routes is the control for the whole package: no version moves, no
     * heartbeat leaves, the listener hears nothing. Reverting the change gate to "every round" reddens
     * each of these four legs.
     */
    private function testAnUnchangedRoundPaysNothing(): void
    {
        $this->captureLogs();
        try {
            [$c, $inst, $listener] = $this->twoTopicPush('GID-RC-SILENT', 'RcSilA', 'RcSilB');
            $c->pullThresholdForTopic = 1000;
            $c->doRebalance();
            $this->check($inst->heartbeatAddrs !== [], 'the round that took the queues beat');
            $this->checkSame(2, count($listener->topics()),
                'and both topics moved into this instance at once');

            // Pin the knobs a stray package would move, so the silent round is judged on its own work.
            $c->pullThresholdForQueue = 777;
            $versionA = $this->versionOf($c, 'RcSilA');
            $beats = count($inst->heartbeatAddrs);
            $calls = count($listener->calls);

            // The identical round: same routes, same consumer list, same everything.
            $this->captured = [];
            $c->doRebalance();
            $this->checkSame($versionA, $this->versionOf($c, 'RcSilA'),
                'an unchanged round leaves the subscription version where it is');
            $this->checkSame($beats, count($inst->heartbeatAddrs),
                'an unchanged round sends no heartbeat to any broker');
            $this->checkSame($calls, count($listener->calls),
                'an unchanged round tells the listener nothing');
            $this->checkSame(777, $c->pullThresholdForQueue,
                'an unchanged round does not re-split the topic budget either');
            $this->checkSame([], $this->versionLines(), 'an unchanged round logs no version line');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 3. the heartbeat carries the new version

    /**
     * The reason the beat is immediate is the version: a broker comparing fingerprints only sees the
     * re-shared subscription if the heartbeat that carries it goes out on this round. So the recorded
     * payload is read -- the moved topic carries the new stamp, the sibling still carries its old one.
     */
    private function testTheImmediateHeartbeatCarriesTheNewVersion(): void
    {
        $this->captureLogs();
        try {
            [$c, $inst] = $this->twoTopicPush('GID-RC-BEAT', 'RcBeatA', 'RcBeatB');
            $c->doRebalance();
            $this->checkSame(2, count($inst->heartbeatAddrs),
                'one round of the package beats every broker once, master and slave alike');
            $this->checkSame(
                ['127.0.0.1:10911', '127.0.0.1:10912'],
                $inst->heartbeatAddrs,
                'the immediate beat is the usual all-broker round, not a single address',
            );
            $this->checkSame(1, $c->heartbeatCount(), 'the consumer counts the round as one successful beat');

            $versionB = $this->versionOf($c, 'RcBeatB');
            $inst->heartbeatAddrs = [];
            $inst->heartbeats = [];
            $inst->routes['RcBeatA'] = array_merge(
                $this->queues('RcBeatA', RC_BROKER_A, 2),
                $this->queues('RcBeatA', RC_BROKER_B, 1),
            );
            $c->doRebalance();
            $this->checkSame(2, count($inst->heartbeatAddrs), 'the moving round beat again, on both brokers');
            $this->checkSame($this->versionOf($c, 'RcBeatA'), $inst->stampedVersion('RcBeatA'),
                'the heartbeat carries the version this round just stamped');
            $this->checkSame($versionB, $inst->stampedVersion('RcBeatB'),
                'and the sibling rides along with the version it already had');
        } finally {
            $this->releaseLogs();
        }
    }

    /** A heartbeat the brokers refuse must not break the round the listener still has to hear about. */
    private function testAFailedBeatDoesNotSkipTheRestOfTheRound(): void
    {
        $this->captureLogs();
        try {
            [$c, $inst, $listener] = $this->twoTopicPush('GID-RC-BEATFAIL', 'RcFailA', 'RcFailB');
            $c->doRebalance();
            $listener->reset();
            $c->subscriptionData['RcFailA']->subVersion = 333;
            $c->subscriptionData['RcFailB']->subVersion = 333;
            $inst->failHeartbeats = true;
            $inst->routes['RcFailA'] = $this->queues('RcFailA', RC_BROKER_A, 3);
            $c->doRebalance();
            $this->check($this->versionOf($c, 'RcFailA') > 333,
                'the version was still re-stamped although no broker took the heartbeat');
            $this->checkSame(333, $this->versionOf($c, 'RcFailB'),
                'and only the topic that moved was re-stamped');
            $this->checkSame(1, $c->heartbeatCount(),
                'the refused round added no successful beat to the consumer\'s own count');
            $this->checkSame(['RcFailA'], $listener->topics(),
                'a refused heartbeat does not cancel the application side of the round');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 4. a swap of the same size is a move

    /**
     * The change test compares each topic's queue SET. Giving up one queue on the first broker and
     * taking one on the second keeps every count -- the number of queues, the number of topics, the
     * divided budget -- and still moved the assignment, so the whole package runs. A re-ordering of
     * the same queues is the control: nothing moved, so nothing is paid.
     */
    private function testAnEqualSizeSwapIsAMoveAndAReorderIsNot(): void
    {
        $this->captureLogs();
        try {
            $topic = 'RcSwapA';
            $sibling = 'RcSwapB';
            $inst = new RcInstance('rc-swap-' . uniqid(), []);
            $c = $this->pushConsumer($inst, 'GID-RC-SWAP', [$topic, $sibling]);
            $listener = new RcListener();
            $c->setMessageQueueListener($listener);
            $c->pullThresholdForTopic = 1000;
            $inst->routes[$topic] = $this->queues($topic, RC_BROKER_A, 4);
            $inst->routes[$sibling] = $this->queues($sibling, RC_BROKER_A, 1);
            $c->doRebalance();
            $this->checkSame(200, $c->pullThresholdForQueue, '1000 over the five queues this round owns');
            $listener->reset();

            // Same count for this topic, different broker: 3 queues on the first broker plus 1 on the
            // second, so every count -- queues, topics, divided budget -- is exactly as it was.
            $c->subscriptionData[$topic]->subVersion = 333;
            $c->subscriptionData[$sibling]->subVersion = 444;
            $inst->routes[$topic] = array_merge(
                $this->queues($topic, RC_BROKER_A, 3),
                $this->queues($topic, RC_BROKER_B, 1, 3),
            );
            $this->captured = [];
            $c->doRebalance();
            $this->checkSame(5, $c->assignedQueueCount(), 'the swap left the same five queues owned');
            $this->check($this->versionOf($c, $topic) > 333,
                'an equal-size swap re-stamped: the change test compares the set, not the count');
            $this->checkSame(444, $this->versionOf($c, $sibling),
                'the untouched sibling of a swap round keeps its own version');
            $this->checkSame([['topic' => $topic, 'all' => 4, 'divided' => 4]], $listener->calls,
                'and the listener was told about the topic whose set swapped brokers');

            // Control: the very same queues handed over in a different order.
            $version = $this->versionOf($c, $topic);
            $beats = count($inst->heartbeatAddrs);
            $listener->reset();
            $this->captured = [];
            $inst->routes[$topic] = array_reverse($inst->routes[$topic]);
            $c->doRebalance();
            $this->checkSame($version, $this->versionOf($c, $topic),
                'a re-ordering of the same queue set is not a change: nothing was re-stamped');
            $this->checkSame($beats, count($inst->heartbeatAddrs), 'a re-ordering sent no heartbeat');
            $this->checkSame([], $listener->topics(), 'a re-ordering told the listener nothing');
            $this->checkSame([], $this->versionLines(), 'a re-ordering logged no version line');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 5. the listener hears the moved topics only

    /**
     * The queue listener is an application callback, so its arguments are part of the contract: the
     * topic that moved, that topic's full route set, and this instance's share of it. A topic that did
     * not move is not reported, and a listener that throws does not lose the round its sibling already
     * took.
     */
    private function testTheListenerHearsMovedTopicsWithItsQueues(): void
    {
        $this->captureLogs();
        try {
            [$c, $inst, $listener] = $this->twoTopicPush('GID-RC-LISTEN', 'RcLisA', 'RcLisB');
            $c->doRebalance();
            $this->checkSame(
                [['topic' => 'RcLisA', 'all' => 2, 'divided' => 2],
                 ['topic' => 'RcLisB', 'all' => 2, 'divided' => 2]],
                $listener->calls,
                'the first round reports each topic it moved, with the full set and the whole share',
            );

            $listener->reset();
            $inst->routes['RcLisB'] = $this->queues('RcLisB', RC_BROKER_B, 4);
            $c->doRebalance();
            $this->checkSame([['topic' => 'RcLisB', 'all' => 4, 'divided' => 4]], $listener->calls,
                'only the topic whose set grew is reported, with its own new numbers');

            // A listener that throws on one topic still gets the other topic its round moved.
            $listener->reset();
            $listener->boom = true;
            $inst->routes['RcLisA'] = $this->queues('RcLisA', RC_BROKER_A, 5);
            $inst->routes['RcLisB'] = $this->queues('RcLisB', RC_BROKER_B, 5);
            $c->doRebalance();
            $this->checkSame(2, count($listener->calls),
                'a throwing listener does not swallow the second moved topic of the round');
            $this->checkSame(10, $c->assignedQueueCount(),
                'the round completed and took both grown sets even though the callback failed');
            $this->check($this->sawDebugContaining('message queue listener failed for RcLisA'),
                'the failed callback is recorded rather than dropped');
            $listener->boom = false;

            // No listener wired at all is the ordinary case, and it must not disturb the round.
            $plain = $this->pushConsumer($inst, 'GID-RC-LISTEN-NONE', ['RcLisA']);
            $plain->doRebalance();
            $this->check($plain->assignedQueueCount() > 0,
                'a consumer with no listener still takes its queues');
        } finally {
            $this->releaseLogs();
        }
    }

    private function sawDebugContaining(string $needle): bool
    {
        foreach ($this->linesOf('DEBUG') as $line) {
            if (str_contains($line, $needle)) {
                return true;
            }
        }
        return false;
    }

    // ================================================================ 6. the POP assignment round pays the same

    /**
     * Under broker-side POP assignment the queue set arrives from QUERY_ASSIGNMENT instead of a local
     * allocation, and that round is the one that can change ownership -- so it pays the same package:
     * the version is re-stamped, the beat goes out, the listener hears the topic. A repeated identical
     * answer changes nothing and pays nothing.
     */
    private function testThePopAssignmentRoundPaysThePackageOnce(): void
    {
        $this->captureLogs();
        try {
            $topic = 'RcPopA';
            $inst = new RcInstance('rc-pop-' . uniqid(), []);
            $c = $this->pushConsumer($inst, 'GID-RC-POP', [$topic]);
            $c->popMode = true;
            $listener = new RcListener();
            $c->setMessageQueueListener($listener);

            $popQueues = fn(int $n): array => array_map(
                static fn(MessageQueue $mq): array => ['mq' => $mq, 'mode' => MessageRequestMode::POP],
                $this->queues($topic, RC_BROKER_A, $n),
            );

            $c->applyPopAssignment($topic, $popQueues(2));
            $this->checkSame(2, $c->assignedQueueCount(), 'the broker handed this instance two POP queues');
            $first = $this->versionOf($c, $topic);
            $this->check($first > 111, 'the POP round that took the queues re-stamped the version');
            $this->checkSame(2, count($inst->heartbeatAddrs), 'and beat to every broker on that round');
            $this->checkSame([$topic], $listener->topics(), 'and told the listener about the assigned topic');

            // Paired control: the broker answers again with the very same set.
            $beats = count($inst->heartbeatAddrs);
            $listener->reset();
            $this->captured = [];
            $c->applyPopAssignment($topic, $popQueues(2));
            $this->checkSame($first, $this->versionOf($c, $topic),
                'the same POP answer is not a change: no re-stamp');
            $this->checkSame($beats, count($inst->heartbeatAddrs), 'the same POP answer sends no heartbeat');
            $this->checkSame([], $listener->topics(), 'the same POP answer tells the listener nothing');

            // The answer that gives one queue back is a change even though the topic is the same one.
            $c->subscriptionData[$topic]->subVersion = 555;
            $this->captured = [];
            $c->applyPopAssignment($topic, $popQueues(1));
            $this->check($this->versionOf($c, $topic) > 555,
                'a POP shrink re-stamped: the set moved, the topic did not have to change');
            $this->checkSame([$topic], $listener->topics(), 'and the listener heard the shrunk share');
            $this->checkSame(1, $listener->calls[0]['divided'], 'with the one queue this instance now owns');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 7. the lite consumer's own rebalance

    /**
     * The lite path has no topic budget to re-split and no version to re-share, but its queue listener
     * is on the same rule: the topics whose assigned set moved, this round, with the full route set and
     * this consumer's share -- and a revoked queue leaves no cache behind.
     */
    private function testTheLiteRebalanceTellsMovedTopicsOnly(): void
    {
        $this->captureLogs();
        try {
            $topicA = 'RcLiteA';
            $topicB = 'RcLiteB';
            $inst = new RcInstance('rc-lite-' . uniqid(), []);
            $c = new DefaultLitePullConsumer('GID-RC-LITE');
            $c->clientId = 'rc-lite-cid';
            $inst->peers = [$c->clientId];
            $c->subscribe($topicA);
            $c->subscribe($topicB);
            $inst->routes[$topicA] = $this->queues($topicA, RC_BROKER_A, 2);
            $inst->routes[$topicB] = $this->queues($topicB, RC_BROKER_A, 2);
            $this->setProp($c, 'started', true);
            $c->mqClient = $inst;
            $listener = new RcListener();
            $c->setMessageQueueListener($listener);

            $this->call($c, 'rebalance', []);
            $this->checkSame([$topicA, $topicB], $listener->topics(),
                'the first round reports each topic it assigned');

            $listener->reset();
            $this->call($c, 'rebalance', []);
            $this->checkSame([], $listener->topics(), 'an identical lite round reports nothing');

            // Grow one topic; the given-up queue of the other topic is untouched, so only one report.
            $inst->routes[$topicA] = array_merge(
                $this->queues($topicA, RC_BROKER_A, 2),
                $this->queues($topicA, RC_BROKER_B, 2, 2),
            );
            $this->call($c, 'rebalance', []);
            $this->checkSame([$topicA], $listener->topics(),
                'the lite listener hears the topic whose set grew, and no other');
            $this->checkSame(4, $listener->calls[0]['divided'], 'with the four queues this consumer now owns');
            $this->checkSame(4, $listener->calls[0]['all'],
                'and the callback carries that topic\'s full route set beside this share of it');

            // Give two of them back: the set moved again, and the queues this consumer no longer owns
            // leave no cursor and no cache record behind.
            $givenUp = new MessageQueue($topicA, RC_BROKER_B, 2);
            $c->seek($givenUp, 77);
            $listener->reset();
            $inst->routes[$topicA] = $this->queues($topicA, RC_BROKER_A, 2);
            $this->call($c, 'rebalance', []);
            $this->checkSame([$topicA], $listener->topics(), 'a lite shrink is reported like a lite growth');
            $this->checkSame(2, $listener->calls[0]['divided'], 'with the two queues that are left');
            $this->checkSame(0, $c->cachedMessageCountOf($givenUp),
                'a queue this consumer gave back keeps no cache record');
            $nextOffset = (array) $this->prop($c, 'nextOffset');
            $this->check(
                !array_key_exists($givenUp->topic . $givenUp->brokerName . (string) $givenUp->queueId, $nextOffset),
                'and its pull cursor went with it',
            );
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ 8. the two rules are one package, in order

    /**
     * The budget re-split and the version re-stamp happen on the same round and the same trigger: one
     * moved topic. Pinning them together is what stops either half from being moved onto the periodic
     * timer or onto a different change test -- the divided knob and the new version are both read after
     * the single round, and neither moves on the round after it.
     */
    private function testOneMovedTopicDrivesBothTheStampAndTheDivision(): void
    {
        $this->captureLogs();
        try {
            $topic = 'RcBoth';
            $inst = new RcInstance('rc-both-' . uniqid(), []);
            $c = $this->pushConsumer($inst, 'GID-RC-BOTH', [$topic]);
            $c->pullThresholdForTopic = 1000;
            $c->pullThresholdSizeForTopic = 100;
            $inst->routes[$topic] = $this->queues($topic, RC_BROKER_A, 4);

            $c->doRebalance();
            $this->checkSame(250, $c->pullThresholdForQueue, '1000 over four queues');
            $this->checkSame(25, $c->pullThresholdSizeForQueue, '100 MiB over four queues');

            $inst->routes[$topic] = $this->queues($topic, RC_BROKER_A, 8);
            $c->subscriptionData[$topic]->subVersion = 666;
            $c->doRebalance();
            $this->checkSame(125, $c->pullThresholdForQueue, 'the grown set re-split the same budget');
            $this->checkSame(12, $c->pullThresholdSizeForQueue, 'and the byte budget with it');
            $this->check($this->versionOf($c, $topic) > 666, 'on the same round, from the same trigger');

            $this->checkSame(2, $c->heartbeatCount(), 'two moved rounds, so two all-broker beats');
            $this->checkSame(4, count($inst->heartbeatAddrs),
                'each beat went to master and slave alike');
            $this->checkSame($inst->stampedVersion($topic), $this->versionOf($c, $topic),
                'and the beat that went out carried the divided round\'s version');
        } finally {
            $this->releaseLogs();
        }
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testOnlyTheMovedTopicGetsANewVersion();
        $this->testAnUnchangedRoundPaysNothing();
        $this->testTheImmediateHeartbeatCarriesTheNewVersion();
        $this->testAFailedBeatDoesNotSkipTheRestOfTheRound();
        $this->testAnEqualSizeSwapIsAMoveAndAReorderIsNot();
        $this->testTheListenerHearsMovedTopicsWithItsQueues();
        $this->testThePopAssignmentRoundPaysThePackageOnce();
        $this->testTheLiteRebalanceTellsMovedTopicsOnly();
        $this->testOneMovedTopicDrivesBothTheStampAndTheDivision();
        return $this->summary();
    }
}

exit((new RunClientRebalanceChange())->run());
