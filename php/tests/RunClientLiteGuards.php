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
 *
 * The started flag is set through reflection where a case needs "already running": starting for
 * real would need a cluster, and none of these guards reads the wire.
 */

require_once __DIR__ . '/../bootstrap.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\MessageSelector;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Client\Exceptions\MQClientException;

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

    // ================================================================ entry

    public function run(): int
    {
        $this->testAssignRefusesAnEmptyList();
        $this->testSubscribeRefusesABlankTopic();
        $this->testAssignExpressionIsRefusedOutLoud();
        $this->testPollAndCommittedRefuseAnUnstartedConsumer();
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
