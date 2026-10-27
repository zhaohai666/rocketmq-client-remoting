<?php

declare(strict_types=1);

/**
 * Failed-round backoff knob contract suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientRequestBackoff.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * One knob decides how long a queue waits before its pull (or pop) request is re-issued after a round
 * that failed for any reason other than the broker's own flow control. Four legs are pinned here, and
 * each one is asserted on the pull path and on the POP path of the same consumer:
 *
 * 1. **The default is 1000 ms**, on the getter, on the shared constant the pure mappings fall back to,
 *    and on a round that actually armed a wait without anyone setting the knob.
 * 2. **A FLOW_CONTROL(215) answer still yields 20 ms**, however the knob is set: the broker throttled
 *    that request on its own window, which is a different event from a failed round.
 * 3. **Setting the knob changes the wait both paths take** -- a non-215 error answer on either path and
 *    a POLLING_FULL pop all move to the configured number, and a knob moved *after* the consumer is
 *    running leaves the running schedule alone, because the schedule was built from the configured value.
 * 4. **A non-215 error and a POLLING_FULL round both take the knob value**, including the two arms that
 *    never put a request on the wire: a round taken while the consumer is not running, and a round whose
 *    queue has no subscription to pull or pop with.
 *
 * The waits that are not this knob stay pinned next to them as controls: this client's own cache-threshold
 * gate (50 ms) and the round that simply found nothing (this client's own polling interval).
 *
 * Nothing asserts on a fixed sleep window: every case drives one round against an instance double that
 * records its requests and answers as scripted, then reads the due time the round left behind.
 */

require_once __DIR__ . '/../bootstrap.php';
// message_queue_key() lives in a functions-only include: require it rather than depend on some
// earlier class load pulling the file in by accident.
require_once __DIR__ . '/../src/Remoting/Protocol/AdminBodies.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');
// The failed-round legs log on the debug tier on purpose; keep the files out of the repository.
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_rb_logs_' . getmypid());

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PopProcessQueue;
use RocketMQ\Client\PopResult;
use RocketMQ\Client\PopStatus;
use RocketMQ\Client\PullResult;
use RocketMQ\Client\PullStatus;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\ResponseCode;
use function RocketMQ\Remoting\Protocol\message_queue_key;

/** Instance double: scripted pull/pop answers, and a record of what left the client. */
final class RbInstance extends MQClientInstance
{
    /** @var list<int> one entry per request that went out */
    public array $sent = [];

    /** @var list<PullResult|\Throwable> consumed one per pull */
    public array $pullScript = [];

    /** @var list<PopResult|\Throwable> consumed one per pop */
    public array $popScript = [];

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
        $this->sent[] = ResponseCode::SUCCESS;
        $next = array_shift($this->pullScript);
        if ($next instanceof \Throwable) {
            throw $next;
        }
        return $next instanceof PullResult
            ? $next
            : new PullResult(PullStatus::NO_NEW_MSG, nextBeginOffset: $queueOffset);
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
        $this->sent[] = ResponseCode::SUCCESS;
        $next = array_shift($this->popScript);
        if ($next instanceof \Throwable) {
            throw $next;
        }
        return $next instanceof PopResult ? $next : new PopResult(PopStatus::POLLING_NOT_FOUND);
    }
}

/** Listener that never sees a batch in these legs: it exists so the POP round has one to call. */
final class RbSilentListener implements MessageListenerConcurrently
{
    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

final class RunClientRequestBackoff
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

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

    public function check(bool $condition, string $label): void
    {
        if ($condition) {
            $this->passed++;
            return;
        }
        $this->failures[] = $label;
        fwrite(STDERR, "FAIL: $label\n");
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

    /** The wait the last round armed on this queue, in seconds, or null when nothing was armed. */
    private function armedWait(DefaultMQPushConsumer $c, string $key, float $before): ?float
    {
        $idle = (array) $this->prop($c, 'idleUntil');
        return isset($idle[$key]) ? (float) $idle[$key] - $before : null;
    }

    // ---------------- consumers wired for one round ----------------

    /**
     * One pull queue of a consumer that finished its setup with the knob at $backoffMillis: the round
     * arms the wait that value produced, exactly as a started consumer would.
     *
     * @return array{0:DefaultMQPushConsumer,1:MessageQueue,2:string,3:RbInstance}
     */
    private function pullQueue(string $group, string $topic, ?int $backoffMillis, bool $running = true): array
    {
        $inst = new RbInstance('rb-pull-' . uniqid(), []);
        $c = new DefaultMQPushConsumer($group);
        $c->subscribe($topic);
        if ($backoffMillis !== null) {
            $c->setFailedRequestBackoffMillis($backoffMillis);
        }
        $mq = new MessageQueue($topic, 'b-rb', 0);
        $key = $this->key($mq);
        $c->mqClient = $inst;
        $this->setProp($c, 'mqMap', [$key => $mq]);
        $this->setProp($c, 'offsetTable', [$key => 0]);
        $this->setProp($c, 'started', $running);
        return [$c, $mq, $key, $inst];
    }

    /**
     * The POP counterpart: pop mode on, one live queue, the same knob taken before the consumer was
     * marked running.
     *
     * @return array{0:DefaultMQPushConsumer,1:MessageQueue,2:string,3:RbInstance}
     */
    private function popQueue(string $group, string $topic, ?int $backoffMillis, bool $running = true): array
    {
        $inst = new RbInstance('rb-pop-' . uniqid(), []);
        $c = new DefaultMQPushConsumer($group);
        $c->subscribe($topic);
        $c->popMode = true;
        $c->messageListener = new RbSilentListener();
        if ($backoffMillis !== null) {
            $c->setFailedRequestBackoffMillis($backoffMillis);
        }
        $mq = new MessageQueue($topic, 'b-rb', 0);
        $key = $this->key($mq);
        $c->mqClient = $inst;
        $pq = new PopProcessQueue();
        $this->setProp($c, 'mqMap', [$key => $mq]);
        $this->setProp($c, 'popQueues', [$key => $pq]);
        $this->setProp($c, 'started', $running);
        return [$c, $mq, $key, $inst];
    }

    /** Start the queue from a clean slate, so the wait read back is the one this round armed. */
    private function clearWaits(DefaultMQPushConsumer $c): void
    {
        $this->setProp($c, 'idleUntil', []);
        $this->setProp($c, 'flowControlUntil', []);
        $this->setProp($c, 'suspendedUntil', []);
    }

    /** Drive one pull round whose request the double answers with $answer. @return ?float armed wait */
    private function drivePull(DefaultMQPushConsumer $c, MessageQueue $mq, string $key, mixed $answer): ?float
    {
        /** @var RbInstance $inst */
        $inst = $c->mqClient;
        $this->clearWaits($c);
        $inst->pullScript = [$answer];
        $inst->sent = [];
        $before = microtime(true);
        $this->checkSame(false, (bool) $this->call($c, 'pullOnce', [$mq, $key, false, $before]),
            'the failed pull round ends this queue for the tick');
        $this->checkSame(1, count($inst->sent), 'the pull round put exactly one request on the wire');
        return $this->armedWait($c, $key, $before);
    }

    /** Drive one pop round whose request the double answers with $answer. @return ?float armed wait */
    private function drivePop(DefaultMQPushConsumer $c, MessageQueue $mq, string $key, mixed $answer): ?float
    {
        /** @var RbInstance $inst */
        $inst = $c->mqClient;
        $this->clearWaits($c);
        $inst->popScript = [$answer];
        $inst->sent = [];
        $before = microtime(true);
        $this->checkSame(false, (bool) $this->call($c, 'popOnce', [$mq, $key, $before]),
            'the failed pop round ends this queue for the tick');
        $this->checkSame(1, count($inst->sent), 'the pop round put exactly one request on the wire');
        return $c->nextPopAllowedAt($key) === null
            ? null
            : $c->nextPopAllowedAt($key) - $before;
    }

    private function within(?float $seconds, float $expected): bool
    {
        return $seconds !== null && abs($seconds - $expected) < 0.05;
    }

    // ================================================================ 1. the default is 1000 ms

    private function testDefaultIsOneSecond(): void
    {
        $c = new DefaultMQPushConsumer('GID-RB-DEFAULT');
        $this->checkSame(1000, DefaultMQPushConsumer::DEFAULT_FAILED_REQUEST_BACKOFF_MILLIS,
            'the configured failed-round backoff defaults to 1000 ms');
        $this->checkSame(1000, $c->getFailedRequestBackoffMillis(),
            'a fresh consumer carries that default');
        $this->checkSame(1000, DefaultMQPushConsumer::pullBackoffMillisForResponseCode(ResponseCode::SYSTEM_ERROR),
            'the pull mapping falls back to the same default');
        $this->checkSame(1000, DefaultMQPushConsumer::popIdleBackoffMillisForStatus(PopStatus::POLLING_FULL, 250),
            'the pop mapping falls back to the same default');

        $consts = (new ReflectionClass(DefaultMQPushConsumer::class))->getConstants();
        $this->check(!isset($consts['PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION']),
            'the pull path no longer carries a hardcoded 500 ms error constant');
        $this->check(!isset($consts['POP_TIME_DELAY_MILLS_WHEN_POLLING_FULL']),
            'the pop path no longer carries a hardcoded 3000 ms polling-full constant');

        // And a round that nobody configured arms the default.
        [$cp, $mq, $key] = $this->pullQueue('GID-RB-DEFAULT-PULL', 'RbDefaultPull', null);
        $this->check($this->within(
            $this->drivePull($cp, $mq, $key, new MQBrokerException(ResponseCode::SYSTEM_ERROR, 'broker says no')),
            1.0),
            'an untouched pull consumer arms 1000 ms for a failed round');
        [$cq, $mq2, $key2] = $this->popQueue('GID-RB-DEFAULT-POP', 'RbDefaultPop', null);
        $this->check($this->within(
            $this->drivePop($cq, $mq2, $key2, new MQBrokerException(ResponseCode::SYSTEM_ERROR, 'broker says no')),
            1.0),
            'an untouched POP consumer arms 1000 ms for a failed round');

        // Control: the waits that are not this knob keep their own numbers.
        $this->checkSame(50, DefaultMQPushConsumer::PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL,
            "this client's own cache gate still arms its 50 ms pause");
        $this->checkSame(20, DefaultMQPushConsumer::PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
            "the broker's own throttle still arms its 20 ms pause");
    }

    // ================================================================ 2. 215 stays at 20 ms

    private function testBrokerFlowControlKeepsItsOwnPause(): void
    {
        $this->checkSame(20, DefaultMQPushConsumer::pullBackoffMillisForResponseCode(ResponseCode::FLOW_CONTROL),
            'a 215 answer maps to 20 ms at the default knob');
        $this->checkSame(20, DefaultMQPushConsumer::pullBackoffMillisForResponseCode(ResponseCode::FLOW_CONTROL, 640),
            'and to 20 ms with the knob moved: a throttle is not a failed round');

        foreach ([[null, 0.02], [640, 0.02]] as [$knob, $expected]) {
            [$c, $mq, $key] = $this->pullQueue('GID-RB-215-PULL' . (string) $knob, 'RbThrottlePull', $knob);
            $this->check($this->within(
                $this->drivePull($c, $mq, $key, new MQBrokerException(ResponseCode::FLOW_CONTROL, 'throttled')),
                $expected),
                'a pull answered 215 waits ' . $expected * 1000 . ' ms (knob ' . var_export($knob, true) . ')');
            [$c2, $mq2, $key2] = $this->popQueue('GID-RB-215-POP' . (string) $knob, 'RbThrottlePop', $knob);
            $this->check($this->within(
                $this->drivePop($c2, $mq2, $key2, new MQBrokerException(ResponseCode::FLOW_CONTROL, 'throttled')),
                $expected),
                'a pop answered 215 waits ' . $expected * 1000 . ' ms (knob ' . var_export($knob, true) . ')');
        }
    }

    // ================================================================ 3. the knob moves both paths

    private function testKnobMovesBothPaths(): void
    {
        $legs = [
            ['pull error', fn(array $q) => $this->drivePull($q[0], $q[1], $q[2],
                new MQBrokerException(ResponseCode::SYSTEM_ERROR, 'broker says no'))],
            ['pop error', fn(array $q) => $this->drivePop($q[0], $q[1], $q[2],
                new MQBrokerException(ResponseCode::SYSTEM_ERROR, 'broker says no'))],
            ['pop timeout error', fn(array $q) => $this->drivePop($q[0], $q[1], $q[2],
                new MQBrokerException(ResponseCode::TOPIC_NOT_EXIST, 'not created yet'))],
            ['pop polling full', fn(array $q) => $this->drivePop($q[0], $q[1], $q[2],
                new PopResult(PopStatus::POLLING_FULL))],
        ];
        foreach ([640, 2500] as $knob) {
            foreach ($legs as [$name, $drive]) {
                $build = str_contains($name, 'pull') ? 'pullQueue' : 'popQueue';
                $q = $this->{$build}('GID-RB-KNOB-' . strtoupper(str_replace(' ', '', $name)) . $knob,
                    'RbKnob' . strtoupper(str_replace(' ', '', $name)) . $knob, $knob);
                $this->check($this->within($drive($q), $knob / 1000.0),
                    "the $name round takes the configured $knob ms backoff");
            }
        }

        // The round that merely found nothing does NOT follow the knob: it stays on this client's own
        // polling interval, which is what keeps the two arms apart.
        [$c, $mq, $key, $inst] = $this->popQueue('GID-RB-KNOB-IDLE', 'RbKnobIdle', 640);
        $c->popIntervalMillis = 250;
        $inst->popScript = [new PopResult(PopStatus::POLLING_NOT_FOUND)];
        $before = microtime(true);
        $this->call($c, 'popOnce', [$mq, $key, $before]);
        $this->check($this->within($c->nextPopAllowedAt($key) - $before, 0.25),
            'a pop that found nothing still waits this client\'s own polling interval, not the knob');

        // A knob moved after the consumer was set up leaves the running schedule alone: the wait the
        // rounds arm stays the number the schedule was built from.
        $this->checkSame(640, $c->getFailedRequestBackoffMillis(), 'the knob itself reads the new value');
        $c->setFailedRequestBackoffMillis(9999);
        $this->checkSame(9999, $c->getFailedRequestBackoffMillis(),
            'the getter reports what the knob was set to last');
        $due = $this->drivePop($c, $mq, $key, new MQBrokerException(ResponseCode::SYSTEM_ERROR, 'broker says no'));
        $this->check($this->within($due, 0.64),
            'the running schedule still arms the 640 ms it was built from, not the post-start value');
    }

    // ================================================================ 4. the arms that never reach the wire

    /**
     * The two arms that answer before a request is built -- the consumer is not running, and the queue
     * has no subscription to work with -- are failed rounds too, so they arm the same configured wait
     * on both the pull path and the POP path.
     */
    private function testArmsWithoutABrokerAnswer(): void
    {
        // A round taken while the consumer is not running.
        [$c, $mq, $key] = $this->pullQueue('GID-RB-NOSTART-PULL', 'RbNoStartPull', 640, running: false);
        $before = microtime(true);
        $this->checkSame(false, (bool) $this->call($c, 'pullOnce', [$mq, $key, false, $before]),
            'a pull round outside the running consumer ends here');
        $this->check($this->within($this->armedWait($c, $key, $before), 0.64),
            'and it arms the configured backoff instead of retrying on every tick');

        [$c2, $mq2, $key2] = $this->popQueue('GID-RB-NOSTART-POP', 'RbNoStartPop', 640, running: false);
        $before2 = microtime(true);
        $this->checkSame(false, (bool) $this->call($c2, 'popOnce', [$mq2, $key2, $before2]),
            'a pop round outside the running consumer ends here');
        $this->check($this->within($c2->nextPopAllowedAt($key2) - $before2, 0.64),
            'and it arms the configured backoff too');

        // A queue whose topic this consumer never subscribed to.
        [$c3, , $key3] = $this->pullQueue('GID-RB-NOSUB-PULL', 'RbNoSubPull', 640);
        $orphan = new MessageQueue('RbNeverSubscribed', 'b-rb', 0);
        $orphanKey = $this->key($orphan);
        $before3 = microtime(true);
        $this->checkSame(false, (bool) $this->call($c3, 'pullOnce', [$orphan, $orphanKey, false, $before3]),
            'a pull round with no subscription for the queue ends here');
        $this->check($this->within($this->armedWait($c3, $orphanKey, $before3), 0.64),
            'and the unsubscribed queue arms the configured backoff');
        $this->checkSame(null, $this->armedWait($c3, $key3, $before3),
            'control: the queue that was subscribed armed nothing on that round');

        [$c4, , $key4] = $this->popQueue('GID-RB-NOSUB-POP', 'RbNoSubPop', 640);
        $orphan4 = new MessageQueue('RbNeverSubscribedPop', 'b-rb', 0);
        $orphan4Key = $this->key($orphan4);
        $this->setProp($c4, 'popQueues', [$orphan4Key => new PopProcessQueue()]);
        $before4 = microtime(true);
        $this->checkSame(false, (bool) $this->call($c4, 'popOnce', [$orphan4, $orphan4Key, $before4]),
            'a pop round with no subscription for the queue ends here');
        $this->check($this->within($c4->nextPopAllowedAt($orphan4Key) - $before4, 0.64),
            'and the unsubscribed queue arms the configured backoff');
        $this->checkSame(null, $c4->nextPopAllowedAt($key4),
            'control: the queue that was subscribed armed nothing on that round');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testDefaultIsOneSecond();
        $this->testBrokerFlowControlKeepsItsOwnPause();
        $this->testKnobMovesBothPaths();
        $this->testArmsWithoutABrokerAnswer();
        return $this->summary();
    }
}

exit((new RunClientRequestBackoff())->run());
