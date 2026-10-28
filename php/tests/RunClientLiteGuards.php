<?php

declare(strict_types=1);

/**
 * Lite pull consumer misuse guards (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientLiteGuards.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * The lite surface answers misuse with an error instead of a plausible-looking result:
 *
 * 1. assign() with no queue is refused. Taken literally it would revoke every queue the consumer
 *    holds, which is exactly what a route lookup that came back empty does by accident; a refused
 *    call also leaves the mode and the assignment alone.
 * 2. subscribe() with a blank topic is refused rather than registered as a topic named "".
 * 3. setSubExpressionForAssign() refuses a blank expression -- it would quietly mean "everything",
 *    and the caller would never learn its filter did not apply -- refuses a blank topic, which would
 *    otherwise key the expression on "", and refuses to be called once the consumer is started,
 *    because a running consumer has already told the broker which expression this topic has.
 * 4. poll() and committed() refuse a consumer that never started. An empty poll or an unknown
 *    committed offset is exactly what a queue at its tail looks like, which is how a missing
 *    start() stays hidden forever. Once running, a negative poll timeout is refused too -- and the
 *    state check comes first, so an unstarted consumer is reported as that rather than as a
 *    timeout problem.
 * 5. subscribe() and assign() are mutually exclusive: whichever the consumer claimed first holds,
 *    and the other is refused without changing the mode or the assignment.
 * 6. seek() refuses a queue the consumer does not hold (with the rebalancing wording in subscribe
 *    mode and the plain wording in assign mode), refuses a consumer that never started, and refuses
 *    an offset outside the queue's [minOffset, maxOffset] -- leaving the cursors where they were.
 *
 * The started flag is set through reflection where a case needs "already running": starting for
 * real would need a cluster, and none of these guards reads the wire.
 */

require_once __DIR__ . '/../bootstrap.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\MessageSelector;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Client\Exceptions\MQClientException;

/** Answers the two offset reads seek performs and dials nothing, so a range case stays offline. */
final class LiteGuardInstance extends MQClientInstance
{
    public function getMinOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        return 10;
    }

    public function getMaxOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        return 20;
    }
}

final class RunClientLiteGuards
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        if ($expected === $actual) {
            $this->passed++;
        } else {
            $this->failures[] = $label;
            fwrite(STDERR, sprintf("FAIL: %s\n  expected: %s\n  actual:   %s\n",
                $label, var_export($expected, true), var_export($actual, true)));
        }
    }

    /** Runs $call and reports whether it raised $class carrying $needle in its message. */
    private function checkThrows(string $class, string $needle, callable $call, string $label): void
    {
        try {
            $call();
        } catch (\Throwable $e) {
            $isClass = $e instanceof $class;
            $saysWhy = str_contains($e->getMessage(), $needle);
            if ($isClass && $saysWhy) {
                $this->passed++;
                return;
            }
            $this->failures[] = $label;
            fwrite(STDERR, sprintf("FAIL: %s\n  got: %s: %s (wanted %s containing %s)\n",
                $label, $e::class, $e->getMessage(), $class, $needle));
            return;
        }
        $this->failures[] = $label;
        fwrite(STDERR, "FAIL: {$label}\n  no exception at all\n");
    }

    /** Queues this consumer holds, as stable labels for comparison. */
    private function labels(DefaultLitePullConsumer $c): array
    {
        return array_map(
            static fn (MessageQueue $q): string => $q->brokerName . '@' . $q->topic . '-' . $q->queueId,
            $c->assignment());
    }

    private function markStarted(DefaultLitePullConsumer $c, bool $started): void
    {
        $prop = new \ReflectionProperty(DefaultLitePullConsumer::class, 'started');
        $prop->setAccessible(true);
        $prop->setValue($c, $started);
    }

    private function consumer(): DefaultLitePullConsumer
    {
        $c = new DefaultLitePullConsumer('GID_lite_guards');
        $c->setNamesrvAddr('127.0.0.1:9876');
        $c->setAutoCommit(false);
        return $c;
    }

    // ================================================================ 1. assign

    private function testAssignRefusesAnEmptyList(): void
    {
        $c = $this->consumer();
        $mq = new MessageQueue('T', 'broker-0', 0);

        $this->checkThrows(\InvalidArgumentException::class, 'can not be null or empty',
            fn () => $c->assign([]), 'assign([]) is refused instead of revoking every held queue');

        $c->assign([$mq]);
        $this->checkSame(['broker-0@T-0'], $this->labels($c), 'a non-empty assign is accepted');

        $this->checkThrows(\InvalidArgumentException::class, 'can not be null or empty',
            fn () => $c->assign([]), 'assign([]) is still refused once queues are held');
        $this->checkSame(['broker-0@T-0'], $this->labels($c),
            'a refused assign leaves the assignment alone');
    }

    // ================================================================ 2. subscribe

    private function testSubscribeRefusesABlankTopic(): void
    {
        $c = $this->consumer();
        $this->checkThrows(\InvalidArgumentException::class, 'Topic can not be null or empty',
            fn () => $c->subscribe('   '), 'subscribe(" ") is refused');
        $this->checkSame([], $c->subscription, 'a refused subscribe registers nothing');
    }

    // ================================================================ 3. assign expression

    private function testAssignExpressionIsRefusedOutLoud(): void
    {
        $c = $this->consumer();
        $this->checkThrows(\InvalidArgumentException::class, 'subExpression can not be null or empty',
            fn () => $c->setSubExpressionForAssign('T', '  '),
            'a blank assign expression is refused instead of meaning "everything"');
        $this->checkSame([], $c->subscriptionData, 'a refused assign expression registers no subscription');

        $this->checkThrows(\InvalidArgumentException::class, 'Topic can not be null or empty',
            fn () => $c->setSubExpressionForAssign('   ', 'TagA'),
            'a blank topic is refused instead of keying an expression on ""');
        $this->checkThrows(\InvalidArgumentException::class, 'Topic can not be null or empty',
            fn () => $c->setSubExpressionForAssignWithSelector('   ', MessageSelector::byTag('TagA')),
            'the selector flavour refuses a blank topic too');
        $this->checkSame([], $c->subscriptionData, 'a refused blank topic registers no subscription');

        $this->markStarted($c, true);
        $this->checkThrows(MQClientException::class, 'only can be called before start',
            fn () => $c->setSubExpressionForAssign('T', 'TagA'),
            'the assign expression is a pre-start setting');
        $this->checkThrows(MQClientException::class, 'only can be called before start',
            fn () => $c->setSubExpressionForAssign('T', '*'),
            'even "*" is refused after start: the broker already knows the old expression');
    }

    // ================================================================ 4. poll / committed

    private function testPollAndCommittedRefuseAnUnstartedConsumer(): void
    {
        $c = $this->consumer();
        $mq = new MessageQueue('T', 'broker-0', 0);

        $this->checkThrows(MQClientException::class, 'not running',
            fn () => $c->poll(), 'poll() on a consumer that never started refuses');
        $this->checkThrows(MQClientException::class, 'not running',
            fn () => $c->poll(-1), 'the state check comes before the timeout check');
        $this->checkThrows(MQClientException::class, 'not running',
            fn () => $c->committed($mq), 'committed() on a consumer that never started refuses');

        $this->markStarted($c, true);
        $this->checkThrows(\InvalidArgumentException::class, 'Timeout must not be negative',
            fn () => $c->poll(-1), 'a running consumer still refuses a negative timeout');
    }

    // ================================================================ 5. mode exclusivity

    private function testModesAreMutuallyExclusive(): void
    {
        $mq = new MessageQueue('T', 'broker-0', 0);

        $sub = $this->consumer();
        $sub->subscribe('T');
        $sub->subscribe('T2');
        $this->checkSame(2, count($sub->subscription),
            'the first claim on the consumer is subscribe, and it stays open to more topics');
        $this->checkThrows(MQClientException::class, 'Subscribe and assign are mutually exclusive',
            fn () => $sub->assign([$mq]), 'assign after subscribe is refused');
        $this->checkThrows(MQClientException::class, 'Subscribe and assign are mutually exclusive',
            fn () => $sub->setSubExpressionForAssign('T', '*'),
            'an assign expression after subscribe is refused too');
        $this->checkSame(false, $sub->isAssignMode(), 'the refused calls left the consumer in subscribe mode');
        $this->checkSame([], $sub->assignment(), 'a refused assign adds no queue');

        $assign = $this->consumer();
        $assign->setSubExpressionForAssign('T', '*');
        $this->checkThrows(MQClientException::class, 'Subscribe and assign are mutually exclusive',
            fn () => $assign->subscribe('T'), 'subscribe after assign is refused');
        $this->checkSame(true, $assign->isAssignMode(), 'the refused subscribe left the consumer in assign mode');
        $this->checkSame([], $assign->subscription, 'a refused subscribe registers no subscription');
    }

    // ================================================================ 6. seek

    /** Put one queue into the assigned set without a rebalance round. */
    private function holdQueue(DefaultLitePullConsumer $c, MessageQueue $mq): void
    {
        $prop = new \ReflectionProperty(DefaultLitePullConsumer::class, 'assigned');
        $prop->setAccessible(true);
        $prop->setValue($c, [$mq->topic . $mq->brokerName . (string) $mq->queueId => $mq]);
    }

    private function testSeekRefusesWhatItCannotHonour(): void
    {
        $mq = new MessageQueue('T', 'broker-0', 0);
        $stranger = new MessageQueue('T', 'broker-0', 1);

        // Subscribe mode: a queue this consumer does not hold names the rebalance explicitly.
        $sub = $this->consumer();
        $sub->subscribe('T');
        $this->checkThrows(MQClientException::class, 'not in assigned list, may be rebalancing, message queue',
            fn () => $sub->seek($stranger, 15), 'subscribe mode names the rebalance that may own the queue');

        // Assign mode: the same refusal, without blaming a rebalance this mode never runs.
        $assign = $this->consumer();
        $assign->setSubExpressionForAssign('T', '*');
        $assign->assign([$mq]);
        $this->checkThrows(
            MQClientException::class,
            "not in assigned list, message queue: MessageQueue(topic='T', broker='broker-0', qid=1)",
            fn () => $assign->seek($stranger, 15),
            'assign mode does not claim a rebalance is in progress',
        );
        $this->checkSame(-1, $assign->pullCursorOf($mq), 'a refused seek leaves the held queue\'s cursor alone');

        // Membership is answered before the running state...
        $this->checkThrows(MQClientException::class, 'not in assigned list',
            fn () => $assign->seek(new MessageQueue('T', 'broker-0', 2), 15),
            'an unheld queue is reported as unheld even on a consumer that never started');

        // ...and the running state before the range, which needs the broker's own bounds.
        $this->holdQueue($assign, $mq);
        $this->checkThrows(MQClientException::class, 'not running',
            fn () => $assign->seek($mq, 15), 'a consumer that never started cannot seek');

        $assign->mqClient = new LiteGuardInstance('lite-guard-' . uniqid(), []);
        $this->markStarted($assign, true);

        $this->checkThrows(
            MQClientException::class,
            'Seek offset illegal, seek offset = 9, min offset = 10, max offset = 20',
            fn () => $assign->seek($mq, 9),
            'an offset below the queue\'s min is refused with all three numbers',
        );
        $this->checkThrows(
            MQClientException::class,
            'Seek offset illegal, seek offset = 21, min offset = 10, max offset = 20',
            fn () => $assign->seek($mq, 21),
            'an offset past the queue\'s max is refused with all three numbers',
        );
        $this->checkSame(-1, $assign->pullCursorOf($mq), 'neither refused seek moved the cursor');

        $assign->seek($mq, 10);
        $this->checkSame(10, $assign->pullCursorOf($mq), 'the min offset itself is legal');
        $assign->seek($mq, 20);
        $this->checkSame(20, $assign->pullCursorOf($mq), 'the max offset itself is legal');
        $this->checkSame(20, $assign->consumeCursorOf($mq), 'a legal seek dials the consume cursor too');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testAssignRefusesAnEmptyList();
        $this->testSubscribeRefusesABlankTopic();
        $this->testAssignExpressionIsRefusedOutLoud();
        $this->testPollAndCommittedRefuseAnUnstartedConsumer();
        $this->testModesAreMutuallyExclusive();
        $this->testSeekRefusesWhatItCannotHonour();
        return $this->summary();
    }

    private function summary(): int
    {
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }
}

exit((new RunClientLiteGuards())->run());
