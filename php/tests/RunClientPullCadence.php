<?php

declare(strict_types=1);

/**
 * Pull round pacing contract suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientPullCadence.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * A pull round in this port is a short poll: the request carries ``pollTime = 0``, so an answer costs no
 * broker suspend budget and comes back at once. Two knobs pace the queue after a round, and each has
 * exactly one job:
 *
 * 1. **A round that delivered rows is paced by ``pullInterval``** (default 0). Above zero that round arms
 *    the cadence on its queue and reports "not now", so the next round waits for it; at 0 the very same
 *    queue re-pulls immediately, inside the same ``tick()``. What decides "delivered" is the list the
 *    round hands over -- after the client-side tag pass and the filter hooks -- which is the same list
 *    ``PULL_TPS`` counts, so the two figures cannot drift apart.
 * 2. **A round that delivered nothing keeps this port's idle guard ``pullIntervalMillis``** (default
 *    1000 ms), because re-polling an empty short poll at once would turn every idle queue into a hot
 *    loop. This includes a FOUND round whose every row the pre-delivery filter dropped: nothing was
 *    handed over, so it is an empty round and the cadence does not apply to it.
 *
 * The arms that are neither of those stay exactly as they are and are pinned here as controls: the
 * broker's own ``FLOW_CONTROL``(215) at 20 ms, the configured failed-round backoff for every other
 * failed round (default 1000 ms, snapshotted when the consumer starts), and the paused consumer, whose
 * round ends before a request is built and arms nothing.
 *
 * The three waits are deliberately three different numbers in every leg -- 50 ms cadence, 300 ms idle
 * guard, 1000 ms (or 640 ms) failed-round backoff -- so a branch that took the wrong one cannot pass.
 * Nothing asserts on a fixed sleep window: each leg drives one round against an instance double that
 * answers as scripted and records the requests it was asked to send, then reads the due time the round
 * left behind. Where a later round has to be inside or past that due time, the leg hands the recorded
 * deadline to the round instead of waiting for it.
 */

require_once __DIR__ . '/../bootstrap.php';
// message_queue_key() lives in a functions-only include: require it rather than depend on some
// earlier class load pulling the file in by accident.
require_once __DIR__ . '/../src/Remoting/Protocol/AdminBodies.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');
// The failed-round legs log on the debug tier on purpose; keep the files out of the repository.
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_pc_logs_' . getmypid());

use RocketMQ\Client\ConsumerStatsManager;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PullResult;
use RocketMQ\Client\PullStatus;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\UtilAll;
use RocketMQ\Remoting\Protocol\ResponseCode;
use function RocketMQ\Remoting\Protocol\message_queue_key;

/** The three waits, kept apart on purpose. */
const PC_CADENCE_MILLIS = 50;
const PC_IDLE_GUARD_MILLIS = 300;
const PC_FAILED_BACKOFF_MILLIS = 640;

/** Instance double: scripted pull answers, and a record of every request that left the client. */
final class PcInstance extends MQClientInstance
{
    /** @var list<int> one entry per pull request that went out */
    public array $sent = [];

    /** @var list<PullResult|\Throwable> consumed one per pull */
    public array $pullScript = [];

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
        $this->sent[] = $queueOffset;
        $next = array_shift($this->pullScript);
        if ($next instanceof \Throwable) {
            throw $next;
        }
        return $next instanceof PullResult
            ? $next
            : new PullResult(PullStatus::NO_NEW_MSG, nextBeginOffset: $queueOffset);
    }
}

final class RunClientPullCadence
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

    // ---------------- reflection helpers ----------------

    private function prop(object $o, string $name): mixed
    {
        $p = new ReflectionProperty(DefaultMQPushConsumer::class, $name);
        $p->setAccessible(true);
        return $p->getValue($o);
    }

    private function setProp(object $o, string $name, mixed $value): void
    {
        $p = new ReflectionProperty(DefaultMQPushConsumer::class, $name);
        $p->setAccessible(true);
        $p->setValue($o, $value);
    }

    private function call(object $o, string $name, array $args): mixed
    {
        $m = new ReflectionMethod(DefaultMQPushConsumer::class, $name);
        $m->setAccessible(true);
        return $m->invoke($o, ...$args);
    }

    private function key(MessageQueue $mq): string
    {
        return (string) message_queue_key($mq);
    }

    /** When the queue's next round is allowed, in seconds since the epoch, or null when nothing holds it. */
    private function dueAt(DefaultMQPushConsumer $c, string $key): ?float
    {
        $idle = (array) $this->prop($c, 'idleUntil');
        return isset($idle[$key]) ? (float) $idle[$key] : null;
    }

    /** The wait the last round armed on this queue, in seconds, or null when nothing was armed. */
    private function armedWait(DefaultMQPushConsumer $c, string $key, float $roundStart): ?float
    {
        $due = $this->dueAt($c, $key);
        return $due === null ? null : $due - $roundStart;
    }

    /** Is the armed wait this number, within a window that leaves the neighbouring arms apart? */
    private function within(?float $seconds, float $expected, float $tolerance = 0.05): bool
    {
        return $seconds !== null && abs($seconds - $expected) < $tolerance;
    }

    // ---------------- consumers wired for one round ----------------

    /**
     * One pull queue wired for a round: running, subscribed with ``$expression``, its cursor resolved and
     * its stats on the books. The three pacing numbers are set to three different values --
     * ``pullInterval`` 50 ms, ``pullIntervalMillis`` 300 ms and the failed-round backoff 640 ms -- so a
     * round that armed the wrong one is caught by the number itself, not by a comment.
     *
     * @return array{0:DefaultMQPushConsumer,1:MessageQueue,2:string,3:PcInstance}
     */
    private function pullQueue(
        string $group,
        string $topic,
        string $expression = '*',
        int $cadenceMillis = PC_CADENCE_MILLIS,
        int $idleGuardMillis = PC_IDLE_GUARD_MILLIS,
        ?int $failedBackoffMillis = PC_FAILED_BACKOFF_MILLIS,
    ): array {
        $inst = new PcInstance('pc-' . uniqid(), []);
        $c = new DefaultMQPushConsumer($group);
        $c->subscribe($topic, $expression);
        $c->pullInterval = $cadenceMillis;
        $c->pullIntervalMillis = $idleGuardMillis;
        if ($failedBackoffMillis !== null) {
            $c->setFailedRequestBackoffMillis($failedBackoffMillis);
        }
        $mq = new MessageQueue($topic, 'b-pc', 0);
        $key = $this->key($mq);
        $c->mqClient = $inst;
        $this->setProp($c, 'mqMap', [$key => $mq]);
        $this->setProp($c, 'offsetTable', [$key => 0]);
        $this->setProp($c, 'statsManager', new ConsumerStatsManager());
        $this->setProp($c, 'started', true);
        return [$c, $mq, $key, $inst];
    }

    /** A row as the pull path hands it over: the pull confirms by offset, so no ack bookkeeping. */
    private function row(string $topic, int $offset, ?string $tag = null): MessageExt
    {
        $m = new MessageExt(topic: $topic, body: 'pc-' . $offset);
        $m->queueId = 0;
        $m->queueOffset = $offset;
        $m->brokerName = 'b-pc';
        $m->msgId = sprintf('PC-%d', $offset);
        $m->bornTimestamp = UtilAll::currentTimeMillis();
        if ($tag !== null) {
            $m->setTags($tag);
        }
        return $m;
    }

    /** @param list<MessageExt> $rows */
    private function found(string $topic, int $nextOffset, array $rows): PullResult
    {
        return new PullResult(
            PullStatus::FOUND,
            nextBeginOffset: $nextOffset,
            minOffset: 0,
            maxOffset: $nextOffset,
            msgFoundList: $rows,
        );
    }

    /** @return list<int> the queue offsets the buffered rows carry, in order */
    private function pendingOffsets(DefaultMQPushConsumer $c, string $key): array
    {
        return array_map(
            static fn(MessageExt $m): int => $m->queueOffset,
            (array) (($this->prop($c, 'pending')[$key] ?? [])),
        );
    }

    /**
     * Drive exactly one round whose request the double answers with ``$answer``, from a clean slate so
     * the wait read back is the one this round armed.
     *
     * @return array{0:bool,1:?float,2:int} the round's verdict, the wait it armed, the requests it sent
     */
    private function round(
        DefaultMQPushConsumer $c,
        MessageQueue $mq,
        string $key,
        mixed $answer,
        ?float $now = null,
    ): array {
        /** @var PcInstance $inst */
        $inst = $c->mqClient;
        $this->setProp($c, 'idleUntil', []);
        $this->setProp($c, 'flowControlUntil', []);
        $this->setProp($c, 'suspendedUntil', []);
        $inst->pullScript = [$answer];
        $inst->sent = [];
        $before = $now ?? microtime(true);
        $verdict = (bool) $this->call($c, 'pullOnce', [$mq, $key, false, $before]);
        return [$verdict, $this->armedWait($c, $key, $before), count($inst->sent)];
    }

    // ================================================================ 1. the two knobs, the two roles

    /** Untouched, the cadence is off and the idle guard is this port's own polling interval. */
    private function testTheDefaultsKeepTheTwoRolesApart(): void
    {
        $c = new DefaultMQPushConsumer('GID-PC-DEFAULTS');
        $this->checkSame(0, $c->pullInterval, 'a fresh consumer asks for no cadence on a delivered round');
        $this->checkSame(1000, $c->pullIntervalMillis,
            "and keeps this port's 1000 ms idle guard for a round that delivered nothing");
        $this->checkSame(1000, DefaultMQPushConsumer::DEFAULT_FAILED_REQUEST_BACKOFF_MILLIS,
            'the failed-round backoff is a third number, not one of the two pacing knobs');
        $this->checkSame(20, DefaultMQPushConsumer::PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
            'the broker throttle stays at its own 20 ms');
    }

    // ================================================================ 2. a delivered round takes the cadence

    /**
     * ``pullInterval`` above zero: the round that handed rows over reports "not now" and defers its
     * queue by exactly that number -- not by the idle guard, not by the failed-round backoff -- and the
     * following rounds honour the deadline it wrote.
     */
    private function testDeliveredRoundArmsTheCadence(): void
    {
        [$c, $mq, $key, $inst] = $this->pullQueue('GID-PC-CADENCE', 'PcCadence');
        [$verdict, $armed, $requests] = $this->round(
            $c,
            $mq,
            $key,
            $this->found('PcCadence', 12, [$this->row('PcCadence', 10), $this->row('PcCadence', 11)]),
        );
        $this->checkSame(false, $verdict, 'a delivered round with a cadence ends this queue for the tick');
        $this->check($this->within($armed, 0.05, 0.02),
            'the delivered round defers its queue by pullInterval, not by the idle guard (' . var_export($armed, true) . ')');
        $this->checkSame(1, $requests, 'a deferred delivered round still sent its one request');
        $this->checkSame([10, 11], $this->pendingOffsets($c, $key),
            'CONTROL: the rows of the deferred round were buffered for delivery all the same');

        /** @var ConsumerStatsManager $stats */
        $stats = $this->prop($c, 'statsManager');
        $tps = $stats->topicAndGroupPullTps->find('PcCadence@GID-PC-CADENCE');
        $this->checkSame(2, $tps?->getValue(),
            'the throughput figure of that round is the same list its pacing was judged on');

        // Inside the cadence: nothing goes out, and the deadline is left where the round put it.
        $due = $this->dueAt($c, $key);
        $this->check($due !== null, 'the delivered round recorded a next allowed round');
        if ($due === null) {
            $this->check(false, 'the inside-cadence leg needs the deadline the delivered round armed');
            $this->check(false, 'the due-time leg needs the deadline the delivered round armed');
            return;
        }
        [$insideVerdict, , $insideRequests] = $this->roundInside($c, $mq, $key, $due - 0.001);
        $this->checkSame(false, $insideVerdict, 'a round of the same queue inside its cadence reports "not now"');
        $this->checkSame(0, $insideRequests, 'and it keeps the request off the wire while the cadence is unpaid');

        // Past it: the very same queue pulls again.
        [$dueVerdict, , $dueRequests] = $this->roundInside($c, $mq, $key, $due);
        $this->checkSame(1, $dueRequests, 'once the cadence is due the same queue pulls again');
        $this->checkSame(false, $dueVerdict, 'the scripted empty round at the due time ends the queue again');
        $guard = $this->dueAt($c, $key) === null ? null : $this->dueAt($c, $key) - microtime(true);
        $this->check($this->within($guard, 0.3),
            'and it is the idle guard that holds the queue now, not the cadence');
    }

    /** One round handed a clock of its own, with the scripted answer kept for the due-time leg. */
    private function roundInside(DefaultMQPushConsumer $c, MessageQueue $mq, string $key, float $now): array
    {
        /** @var PcInstance $inst */
        $inst = $c->mqClient;
        $inst->pullScript = [new PullResult(PullStatus::NO_NEW_MSG, nextBeginOffset: 12, minOffset: 0, maxOffset: 12)];
        $inst->sent = [];
        $verdict = (bool) $this->call($c, 'pullOnce', [$mq, $key, false, $now]);
        return [$verdict, $this->armedWait($c, $key, $now), count($inst->sent)];
    }

    // ================================================================ 3. no cadence: the queue re-pulls at once

    /**
     * With ``pullInterval`` at 0 a delivered round is allowed to run again immediately, which is what a
     * round of the loop does: it asks for the next round of that very queue inside the same ``tick()``.
     */
    private function testDeliveredRoundWithoutCadenceRunsAgain(): void
    {
        [$c, $mq, $key] = $this->pullQueue('GID-PC-NOCADENCE', 'PcNoCadence', cadenceMillis: 0);
        [$verdict, $armed, $requests] = $this->round(
            $c,
            $mq,
            $key,
            $this->found('PcNoCadence', 21, [$this->row('PcNoCadence', 20)]),
        );
        $this->checkSame(true, $verdict, 'a delivered round with no cadence asks for another round at once');
        $this->checkSame(null, $armed, 'and it leaves no wait behind, whatever the idle guard says');
        $this->checkSame(1, $requests, 'the immediate round sent exactly one request');

        // The same contract at the round level: one tick runs every round the queue is allowed to make.
        [$c2, $mq2, $key2, $inst2] = $this->pullQueue('GID-PC-NOCADENCE-TICK', 'PcNoCadenceTick', cadenceMillis: 0);
        $c2->maxPullsPerQueuePerTick = 3;
        $inst2->pullScript = [
            $this->found('PcNoCadenceTick', 31, [$this->row('PcNoCadenceTick', 30)]),
            $this->found('PcNoCadenceTick', 32, [$this->row('PcNoCadenceTick', 31)]),
            $this->found('PcNoCadenceTick', 33, [$this->row('PcNoCadenceTick', 32)]),
        ];
        $this->setProp($c2, 'idleUntil', []);
        $this->call($c2, 'pullRound', [microtime(true)]);
        $this->checkSame(3, count($inst2->sent),
            'CONTROL: with no cadence one tick keeps re-pulling the queue that delivers');
        $this->checkSame([30, 31, 32], $this->pendingOffsets($c2, $key2),
            'and every delivered batch of that tick was buffered');

        // Paired control: the same three delivered rounds, but a cadence above zero stops after the first.
        [$c3, $mq3, $key3, $inst3] = $this->pullQueue('GID-PC-CADENCE-TICK', 'PcCadenceTick');
        $c3->maxPullsPerQueuePerTick = 3;
        $inst3->pullScript = [
            $this->found('PcCadenceTick', 41, [$this->row('PcCadenceTick', 40)]),
            $this->found('PcCadenceTick', 42, [$this->row('PcCadenceTick', 41)]),
            $this->found('PcCadenceTick', 43, [$this->row('PcCadenceTick', 42)]),
        ];
        $this->setProp($c3, 'idleUntil', []);
        $this->call($c3, 'pullRound', [microtime(true)]);
        $this->checkSame(1, count($inst3->sent),
            'a cadence above zero stops the queue after the first delivered round of the tick');
        $this->checkSame([40], $this->pendingOffsets($c3, $key3), 'and the rows it buffered are that round only');
    }

    // ================================================================ 4. the branch keys on what was handed over

    /**
     * A FOUND round whose every row the pre-delivery filter removes handed nothing over, so it is an
     * empty round: the idle guard, not the cadence. This is the leg that reddens when the branch reads
     * the broker's raw list instead of the list the round delivers -- and the throughput figure reads
     * the very same list, which is asserted beside it.
     */
    private function testFilterEmptiedRoundTakesTheIdleGuard(): void
    {
        [$c, $mq, $key] = $this->pullQueue('GID-PC-FILTERED', 'PcFiltered', 'TagA');
        [$verdict, $armed, $requests] = $this->round(
            $c,
            $mq,
            $key,
            $this->found('PcFiltered', 52, [
                $this->row('PcFiltered', 50, tag: 'TagB'),
                $this->row('PcFiltered', 51, tag: 'TagB'),
            ]),
        );
        $this->checkSame(false, $verdict, 'a round whose batch the filter removed asks for no next round');
        $this->check($this->within($armed, 0.3),
            'the emptied round takes the idle guard, not the cadence (' . var_export($armed, true) . ')');
        $this->checkSame(1, $requests, 'the emptied round still sent its one request');
        $this->checkSame([], $this->pendingOffsets($c, $key), 'and nothing was buffered for delivery');

        /** @var ConsumerStatsManager $stats */
        $stats = $this->prop($c, 'statsManager');
        $this->checkSame(null, $stats->topicAndGroupPullTps->find('PcFiltered@GID-PC-FILTERED'),
            'the throughput figure counts nothing for that round, which is the list the branch keyed on');

        // Paired control: one row of the same batch survives, and the round is a delivered one again.
        [$c2, $mq2, $key2] = $this->pullQueue('GID-PC-PARTLY', 'PcPartly', 'TagA');
        [$verdict2, $armed2] = $this->round(
            $c2,
            $mq2,
            $key2,
            $this->found('PcPartly', 63, [
                $this->row('PcPartly', 61, tag: 'TagB'),
                $this->row('PcPartly', 62, tag: 'TagA'),
            ]),
        );
        $this->checkSame(false, $verdict2, 'a round that kept one row is a delivered round');
        $this->check($this->within($armed2, 0.05, 0.02),
            'and it is the cadence that holds it, not the idle guard');
        /** @var ConsumerStatsManager $stats2 */
        $stats2 = $this->prop($c2, 'statsManager');
        $tps2 = $stats2->topicAndGroupPullTps->find('PcPartly@GID-PC-PARTLY');
        $this->checkSame(1, $tps2?->getValue(), 'the throughput it counted is the row that survived');
        $this->checkSame([62], $this->pendingOffsets($c2, $key2),
            'the same single row is what the round handed over, and what its pacing was judged on');
    }

    // ================================================================ 5. the rounds that found nothing

    /**
     * The empty statuses are the idle guard's own territory, whatever the cadence is set to: a round
     * that found nothing, one whose tags matched nothing at the broker, and a FOUND round that came back
     * with an empty list all re-poll on this port's polling interval.
     */
    private function testEmptyRoundsKeepTheIdleGuard(): void
    {
        $legs = [
            'NO_NEW_MSG' => new PullResult(PullStatus::NO_NEW_MSG, nextBeginOffset: 71, minOffset: 0, maxOffset: 71),
            'NO_MATCHED_MSG' => new PullResult(PullStatus::NO_MATCHED_MSG, nextBeginOffset: 72, minOffset: 0, maxOffset: 72),
            'FOUND with no rows' => new PullResult(
                PullStatus::FOUND,
                nextBeginOffset: 73,
                minOffset: 0,
                maxOffset: 73,
                msgFoundList: [],
            ),
        ];
        $n = 0;
        foreach ($legs as $name => $answer) {
            $n++;
            [$c, $mq, $key] = $this->pullQueue('GID-PC-EMPTY-' . $n, 'PcEmpty' . $n);
            [$verdict, $armed, $requests] = $this->round($c, $mq, $key, $answer);
            $this->checkSame(false, $verdict, "a $name round ends this queue for the tick");
            $this->check($this->within($armed, 0.3),
                "a $name round waits this client's own polling interval, not the cadence");
            $this->checkSame(1, $requests, "a $name round sent exactly one request");
            $this->checkSame([], $this->pendingOffsets($c, $key), "a $name round buffered nothing");
        }
    }

    /** The guard is honoured as written: at 0 an empty round is due again at once. */
    private function testIdleGuardAtZeroRePollsAtOnce(): void
    {
        [$c, $mq, $key] = $this->pullQueue('GID-PC-GUARD0', 'PcGuard0', idleGuardMillis: 0);
        [$verdict, $armed] = $this->round(
            $c,
            $mq,
            $key,
            new PullResult(PullStatus::NO_NEW_MSG, nextBeginOffset: 81, minOffset: 0, maxOffset: 81),
        );
        $this->checkSame(false, $verdict, 'an empty round still ends this queue for the tick');
        $this->check($armed !== null && $armed < 0.01,
            'with the idle guard at 0 its deadline is already due, whatever the cadence is set to');
    }

    // ================================================================ 6. the arms that are not pacing

    /**
     * The failed rounds and the paused consumer are untouched by the pacing knobs: the broker's own
     * throttle keeps its 20 ms, every other failed round keeps the configured backoff the schedule was
     * built from, and a paused round ends before a request is built, so it arms nothing.
     */
    private function testTheOtherArmsAreUntouched(): void
    {
        // FLOW_CONTROL(215): the broker's own window, 20 ms, whatever the pacing knobs say.
        [$c, $mq, $key] = $this->pullQueue('GID-PC-THROTTLED', 'PcThrottled');
        [$verdict, $armed, $requests] = $this->round(
            $c,
            $mq,
            $key,
            new MQBrokerException(ResponseCode::FLOW_CONTROL, 'throttled'),
        );
        $this->checkSame(false, $verdict, 'a 215 round ends this queue for the tick');
        $this->check($this->within($armed, 0.02, 0.01),
            'a 215 round still arms its own 20 ms, not the cadence and not the guard');
        $this->checkSame(1, $requests, 'the 215 round really asked the broker');

        // Any other failure: the configured failed-round backoff, which is a third number here.
        [$c2, $mq2, $key2] = $this->pullQueue('GID-PC-FAILED', 'PcFailed');
        [$verdict2, $armed2, $requests2] = $this->round(
            $c2,
            $mq2,
            $key2,
            new MQBrokerException(ResponseCode::SYSTEM_ERROR, 'broker says no'),
        );
        $this->checkSame(false, $verdict2, 'a failed round ends this queue for the tick');
        $this->check($this->within($armed2, 0.64),
            'a failed round still arms the configured backoff, not the pacing knobs');
        $this->checkSame(1, $requests2, 'the failed round really asked the broker');

        // And with the knob left untouched it arms the default it was built from.
        [$c3, $mq3, $key3] = $this->pullQueue(
            'GID-PC-FAILED-DEFAULT',
            'PcFailedDefault',
            failedBackoffMillis: null,
        );
        [, $armed3] = $this->round($c3, $mq3, $key3, new MQBrokerException(ResponseCode::SYSTEM_ERROR, 'no'));
        $this->check($this->within($armed3, 1.0),
            'CONTROL: an untouched consumer still arms its 1000 ms failed-round backoff');

        // A paused round never reaches the request, so it paces nothing at all.
        [$c4, $mq4, $key4, $inst4] = $this->pullQueue('GID-PC-PAUSED', 'PcPaused');
        $c4->suspend();
        $this->setProp($c4, 'idleUntil', []);
        $before = microtime(true);
        $this->checkSame(false, (bool) $this->call($c4, 'pullOnce', [$mq4, $key4, false, $before]),
            'a paused consumer ends the round without pulling');
        $this->checkSame(0, count($inst4->sent), 'CONTROL: the paused round put nothing on the wire');
        $this->checkSame(null, $this->armedWait($c4, $key4, $before),
            'CONTROL: a paused round arms neither the cadence nor the idle guard');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testTheDefaultsKeepTheTwoRolesApart();
        $this->testDeliveredRoundArmsTheCadence();
        $this->testDeliveredRoundWithoutCadenceRunsAgain();
        $this->testFilterEmptiedRoundTakesTheIdleGuard();
        $this->testEmptyRoundsKeepTheIdleGuard();
        $this->testIdleGuardAtZeroRePollsAtOnce();
        $this->testTheOtherArmsAreUntouched();
        return $this->summary();
    }
}

exit((new RunClientPullCadence())->run());
