<?php

declare(strict_types=1);

/**
 * POP delivery contract suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientPopDelivery.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Four contracts are pinned here, all of them inside the caller-driven ``tick()`` model of this port:
 *
 * 1. **The retry budget is spent before delivery, on the POP path only.** A popped row whose reconsume
 *    times is strictly greater than this consumer's effective budget (its own ``maxReconsumeTimes``, or
 *    16 while that is left unset) never reaches the listener: it is logged and acked exactly like a row
 *    the tag filter rejected, wait-ack bookkeeping included. The boundary sits on the limit itself, so
 *    the row at the budget is delivered and only the row one past it is dropped. The pull path is the
 *    paired control: the same rows, same consumer settings, are delivered there untouched, because a pull
 *    advances the offset and a row skipped there would vanish without any send-back. The guard the
 *    listener's own result feeds stays in place next to this early drop, so a row delivered at exactly
 *    the budget and then refused is still redelivered through it rather than acked.
 * 2. **A POP retry topic name is rewritten to the ordinary topic it carries before the listener sees the
 *    batch**: ``%RETRY%<group>+Topic`` and ``%RETRY%<group>_Topic`` both become ``Topic``, while exactly
 *    ``%RETRY%<group>`` and an ordinary topic keep their name. With a namespace the name is unwrapped,
 *    normalised, and unwrapped again. The rewrite sits at the concurrent delivery entry, so pull and POP
 *    batches alike go through it, and the ack of a rewritten row still names the retry topic. The orderly
 *    entry is the paired control: it delivers the name it locked.
 * 3. **The POP round's re-schedule is judged per status after the pre-delivery step, and is state, not a
 *    sleep**: ``nextPopAllowedAt()`` says when the queue may pop again. A round that handed messages to
 *    the listener waits for ``pullInterval`` (and only when that is above zero -- otherwise it pops
 *    again at once); a round that popped everything away is allowed again at once and records no delivery
 *    TPS; ``POLLING_FULL`` and any status without a rule of its own wait 3000 ms, while a round that
 *    simply found nothing waits for this client's own polling interval, since this port's pop carries no
 *    suspend budget at the broker. TPS counts the messages delivered, not the messages popped; RT is the
 *    round's own cost. The error path keeps its own mapping -- FLOW_CONTROL(215) 20 ms, anything else
 *    the general error backoff -- and is asserted untouched next to the status arms so a change of the
 *    error branch cannot pass as a status rule.
 * 4. **The pull round's telemetry follows the same two figures as the POP round's.** ``PULL_RT`` is
 *    recorded by a FOUND round and by no other round -- a round that carried no message says nothing about
 *    the latency of this topic's deliveries, while a FOUND round records its cost even when no row came
 *    back -- and ``PULL_TPS`` is fed the rows that survive the pre-delivery filter, never the rows the
 *    broker handed over, because a row this client dropped was never delivered. Each leg asserts both
 *    halves, so a change to one figure cannot pass as a change to the other.
 *
 * Nothing here asserts on a fixed sleep window. Every case drives one round against an instance double
 * that records the requests it is asked to send, reads the due time the round left behind, and where the
 * observable is "the queue pops again", the pause is paid by moving that recorded due time rather than
 * by waiting.
 */

require_once __DIR__ . '/../bootstrap.php';

// The suite logs budget drops and filter drops on purpose; keep the files out of the repository.
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_popd_logs_' . getmypid());
// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\ChangeInvisibleTimeResult;
use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\ConsumeOrderlyContext;
use RocketMQ\Client\ConsumeOrderlyStatus;
use RocketMQ\Client\ConsumerDefaults;
use RocketMQ\Client\ConsumerStatsManager;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Logger;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MessageListenerOrderly;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PopProcessQueue;
use RocketMQ\Client\PopResult;
use RocketMQ\Client\PopStatus;
use RocketMQ\Client\PullResult;
use RocketMQ\Client\PullStatus;
use RocketMQ\Common\FilterAPI;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\UtilAll;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\ResponseCode;

/**
 * The delivery recorder for the concurrent listener: it keeps what the listener was actually handed
 * (topics, offsets, reconsume times) and, in the same breath, the state of the queue's wait-ack
 * accumulator and of the broker double's ack log at that moment. Those two snapshots are what tell a
 * pre-delivery drop apart from a drop made after the listener ran.
 */
final class PdConcurrentListener implements MessageListenerConcurrently
{
    /** @var list<array{topics:list<string>, offsets:list<int>, reconsume:list<int>, waitAck:int, acks:int}> */
    public array $calls = [];

    public bool $refuse = false;

    public function __construct(
        public ?PopProcessQueue $pq = null,
        public ?PdInstance $inst = null,
    ) {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        $topics = [];
        $offsets = [];
        $reconsume = [];
        foreach ($msgs as $m) {
            $topics[] = (string) $m->topic;
            $offsets[] = $m->queueOffset;
            $reconsume[] = $m->getReconsumeTimes();
        }
        $this->calls[] = [
            'topics' => $topics,
            'offsets' => $offsets,
            'reconsume' => $reconsume,
            'waitAck' => $this->pq?->waitAckCount() ?? -1,
            'acks' => $this->inst === null ? -1 : count($this->inst->acks),
        ];
        return $this->refuse
            ? ConsumeConcurrentlyStatus::RECONSUME_LATER
            : ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    /** Every topic this listener has been handed across all calls, flattened. */
    public function allTopics(): array
    {
        $out = [];
        foreach ($this->calls as $call) {
            foreach ($call['topics'] as $t) {
                $out[] = $t;
            }
        }
        return $out;
    }

    public function deliveryCount(): int
    {
        $n = 0;
        foreach ($this->calls as $call) {
            $n += count($call['topics']);
        }
        return $n;
    }
}

/** Orderly listener: records the topic names it was handed and accepts the batch. */
final class PdOrderlyListener implements MessageListenerOrderly
{
    /** @var list<list<string>> */
    public array $calls = [];

    public function consumeMessage(array $msgs, ConsumeOrderlyContext $context): ConsumeOrderlyStatus
    {
        $this->calls[] = array_map(static fn(MessageExt $m): string => (string) $m->topic, $msgs);
        return ConsumeOrderlyStatus::SUCCESS;
    }
}

/**
 * Instance double for a POP consumer: it answers a scripted round per pop, records every ack and every
 * invisible-time extension it is asked to send, and can answer one round with a broker error code. A
 * whole POP contract is then readable from recorded state -- which rows were acked, which were handed to
 * the listener, how many requests went out -- with no socket and no clock.
 */
final class PdInstance extends MQClientInstance
{
    /** @var list<array{topic:string,group:string,queueId:int}> */
    public array $pops = [];

    /** @var list<array{topic:string,group:string,queueId:int,offset:int,extraInfo:string}> */
    public array $acks = [];

    /** @var list<array{topic:string,queueId:int,offset:int,invisibleTime:int}> */
    public array $delays = [];

    /** @var list<array{topic:string,queueId:int,offset:int}> */
    public array $pulls = [];

    /** @var list<PopResult|\Throwable> consumed one per pop, last entry repeats when exhausted */
    public array $popScript = [];

    /** @var list<PullResult|\Throwable> consumed one per pull */
    public array $pullScript = [];

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
        $next = array_shift($this->popScript);
        if ($next instanceof \Throwable) {
            throw $next;
        }
        return $next instanceof PopResult ? $next : new PopResult(PopStatus::POLLING_NOT_FOUND);
    }

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
        $this->pulls[] = ['topic' => $mq->topic, 'queueId' => $mq->queueId, 'offset' => $queueOffset];
        $next = array_shift($this->pullScript);
        if ($next instanceof \Throwable) {
            throw $next;
        }
        return $next instanceof PullResult
            ? $next
            : new PullResult(PullStatus::NO_NEW_MSG, nextBeginOffset: $queueOffset);
    }

    public function ackMessage(
        string $consumerGroup,
        string $topic,
        int $queueId,
        string $extraInfo,
        int $offset,
        ?string $brokerName = null,
        int $timeoutMillis = 30000,
        ?string $addr = null,
    ): int {
        $this->acks[] = [
            'topic' => $topic,
            'group' => $consumerGroup,
            'queueId' => $queueId,
            'offset' => $offset,
            'extraInfo' => $extraInfo,
            'brokerName' => (string) $brokerName,
        ];
        return ResponseCode::SUCCESS;
    }

    public function changeInvisibleTime(
        string $consumerGroup,
        string $topic,
        int $queueId,
        string $extraInfo,
        int $offset,
        int $invisibleTime,
        ?string $brokerName = null,
        int $timeoutMillis = 30000,
        ?string $addr = null,
    ): ChangeInvisibleTimeResult {
        $this->delays[] = [
            'topic' => $topic,
            'queueId' => $queueId,
            'offset' => $offset,
            'invisibleTime' => $invisibleTime,
        ];
        return new ChangeInvisibleTimeResult(ResponseCode::SUCCESS);
    }

    public function popAckAddrFor(string $brokerName, string $topic, int $queueId): string
    {
        return '127.0.0.1:10911';
    }

    /** @return list<int> queue offsets the double was asked to ack, in order */
    public function ackedOffsets(): array
    {
        return array_map(static fn(array $a): int => $a['offset'], $this->acks);
    }
}

final class RunClientPopDelivery
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

    // ---------------- fixtures ----------------

    /** The queue key the production round keys its per-queue state on. */
    private function key(MessageQueue $mq): string
    {
        return $mq->topic . $mq->brokerName . (string) $mq->queueId;
    }

    /**
     * A popped row: its own offset as both CK offsets, a live popTime so the batch is not judged timed
     * out, the reconsume count the budget is judged on, and the POP_CK the ack is built from.
     */
    private function popped(
        string $topic,
        int $queueId,
        int $offset,
        int $reconsumeTimes,
        string $brokerName = 'b-pop',
        ?string $tag = null,
    ): MessageExt {
        $m = new MessageExt(topic: $topic, body: 'pop-' . $offset);
        $m->queueId = $queueId;
        $m->queueOffset = $offset;
        $m->brokerName = $brokerName;
        $m->msgId = sprintf('MSG-%d-%d', $queueId, $offset);
        $m->bornTimestamp = UtilAll::currentTimeMillis();
        $m->setReconsumeTimes($reconsumeTimes);
        if ($tag !== null) {
            $m->setTags($tag);
        }
        $m->putProperty(
            MessageConst::PROPERTY_POP_CK,
            ExtraInfoUtil::buildExtraInfo(
                $offset,
                $m->bornTimestamp,
                60000,
                0,
                $topic,
                $brokerName,
                $queueId,
                $offset,
            ),
        );
        return $m;
    }

    /** A pulled row: same shape, no POP_CK, since the pull path confirms with offsets and not with acks. */
    private function pulled(
        string $topic,
        int $queueId,
        int $offset,
        int $reconsumeTimes,
        ?string $tag = null,
    ): MessageExt {
        $m = new MessageExt(topic: $topic, body: 'pull-' . $offset);
        $m->queueId = $queueId;
        $m->queueOffset = $offset;
        $m->brokerName = 'b-pop';
        $m->msgId = sprintf('PULL-%d-%d', $queueId, $offset);
        $m->bornTimestamp = UtilAll::currentTimeMillis();
        $m->setReconsumeTimes($reconsumeTimes);
        if ($tag !== null) {
            $m->setTags($tag);
        }
        return $m;
    }

    /**
     * One POP queue wired to the double: started, with its subscription, its wait-ack bookkeeping, its
     * stats set and a listener that records what it was handed. Nothing here talks to a socket.
     *
     * @return array{0:DefaultMQPushConsumer,1:MessageQueue,2:string,3:PopProcessQueue,4:PdConcurrentListener,5:PdInstance}
     */
    private function popConsumer(
        string $group,
        string $topic,
        string $expression = '*',
        int $queueId = 0,
        ?PdInstance $inst = null,
    ): array {
        $inst ??= new PdInstance('popd-' . uniqid(), []);
        $c = new DefaultMQPushConsumer($group);
        $c->popMode = true;
        $this->setProp($c, 'started', true);
        $c->mqClient = $inst;
        $mq = new MessageQueue($topic, 'b-pop', $queueId);
        $key = $this->key($mq);
        $pq = new PopProcessQueue();
        $listener = new PdConcurrentListener($pq, $inst);
        $c->messageListener = $listener;
        $c->consumeMessageBatchMaxSize = 32;
        $this->setProp($c, 'mqMap', [$key => $mq]);
        $this->setProp($c, 'popQueues', [$key => $pq]);
        $this->setProp($c, 'subscriptionData', [
            $topic => FilterAPI::buildSubscriptionData($topic, $expression),
        ]);
        $this->setProp($c, 'statsManager', new ConsumerStatsManager());
        return [$c, $mq, $key, $pq, $listener, $inst];
    }

    /** The same wiring for the pull path: one queue, a cursor already resolved, no POP bookkeeping. */
    private function pullConsumer(string $group, string $topic, string $expression = '*'): array
    {
        $inst = new PdInstance('popd-pull-' . uniqid(), []);
        $c = new DefaultMQPushConsumer($group);
        $this->setProp($c, 'started', true);
        $c->mqClient = $inst;
        $mq = new MessageQueue($topic, 'b-pop', 0);
        $key = $this->key($mq);
        $listener = new PdConcurrentListener(null, $inst);
        $c->messageListener = $listener;
        $c->consumeMessageBatchMaxSize = 32;
        $this->setProp($c, 'mqMap', [$key => $mq]);
        $this->setProp($c, 'offsetTable', [$key => 0]);
        $this->setProp($c, 'subscriptionData', [
            $topic => FilterAPI::buildSubscriptionData($topic, $expression),
        ]);
        $this->setProp($c, 'statsManager', new ConsumerStatsManager());
        return [$c, $mq, $key, $listener, $inst];
    }

    // ---------------- log capture ----------------

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

    /** @return list<string> the messages of every captured line of this tier */
    private function messagesOf(string $tier): array
    {
        return array_map(fn(string $l): string => $this->messageOf($l), $this->linesOf($tier));
    }

    // ================================================================ 1. the pure rules

    /**
     * The numbers and the name shapes, asserted without any round at all: the budget this consumer
     * measures against, the wait each POP status hands to the next ``tick()``, and the retry-topic names
     * the rewrite recognises. Every arm sits next to the arm it could be confused with, so a mapping
     * mistake is caught here as well as on the wire.
     */
    private function testPureRules(): void
    {
        $c = new DefaultMQPushConsumer('GID-PD-PURE');
        $this->checkSame(-1, $c->maxReconsumeTimes, 'the retry budget is unset by default');
        $this->checkSame(16, ConsumerDefaults::MAX_RECONSUME_TIMES_WHEN_UNSET,
            'an unset retry budget means 16 reconsumes');
        $this->checkSame(16, $this->call($c, 'effectiveMaxReconsumeTimes', []),
            'a consumer that never set a budget measures against 16');
        $c->maxReconsumeTimes = 3;
        $this->checkSame(3, $this->call($c, 'effectiveMaxReconsumeTimes', []),
            'a consumer that set a budget measures against its own number');
        $c->maxReconsumeTimes = 0;
        $this->checkSame(0, $this->call($c, 'effectiveMaxReconsumeTimes', []),
            'a budget of zero stays zero instead of falling back to the default');

        $this->checkSame(3000, DefaultMQPushConsumer::POP_TIME_DELAY_MILLS_WHEN_POLLING_FULL,
            'POLLING_FULL hands the next pop a 3000 ms wait');
        $this->checkSame(3000, DefaultMQPushConsumer::popIdleBackoffMillisForStatus(PopStatus::POLLING_FULL, 250),
            'a busy-polling broker is not paced by this client\'s own interval');
        $this->checkSame(3000, DefaultMQPushConsumer::popIdleBackoffMillisForStatus(PopStatus::POLLING_FULL, 0),
            'even a client that polls back to back waits out POLLING_FULL');
        $this->checkSame(250, DefaultMQPushConsumer::popIdleBackoffMillisForStatus(PopStatus::POLLING_NOT_FOUND, 250),
            'a round that found nothing waits this client\'s own polling interval');
        $this->checkSame(250, DefaultMQPushConsumer::popIdleBackoffMillisForStatus(PopStatus::NO_NEW_MSG, 250),
            'a round told there is no new message waits the same interval');
        $this->checkSame(250, DefaultMQPushConsumer::popIdleBackoffMillisForStatus(PopStatus::FOUND, 250),
            'a FOUND round that delivered nothing takes the round-that-found-nothing wait');
        $this->checkSame(0, DefaultMQPushConsumer::popIdleBackoffMillisForStatus(PopStatus::POLLING_NOT_FOUND, 0),
            'control: a client polling back to back is held back by nothing but a busy broker');

        $this->checkSame('Topic', ExtraInfoUtil::parsePopRetryTopic('%RETRY%GID+Topic', 'GID'),
            'the V2 name is rewritten to what follows the +');
        $this->checkSame('Topic', ExtraInfoUtil::parsePopRetryTopic('%RETRY%GID_Topic', 'GID'),
            'the V1 name is rewritten to what follows the _');
        $this->checkSame('%RETRY%GID', ExtraInfoUtil::parsePopRetryTopic('%RETRY%GID', 'GID'),
            'exactly the group retry topic carries no ordinary topic to extract, so it stays');
        $this->checkSame('Topic', ExtraInfoUtil::parsePopRetryTopic('Topic', 'GID'),
            'an ordinary topic stays');
        $this->checkSame('%RETRY%Other_Topic', ExtraInfoUtil::parsePopRetryTopic('%RETRY%Other_Topic', 'GID'),
            'another group\'s retry topic is not this consumer\'s to rewrite');
        $this->checkSame('%RETRY%GID_', ExtraInfoUtil::parsePopRetryTopic('%RETRY%GID_', 'GID'),
            'an empty tail is not a topic name, so the retry name stays');
        $this->checkSame('%RETRY%GID+', ExtraInfoUtil::parsePopRetryTopic('%RETRY%GID+', 'GID'),
            'control: the same holds for the V2 shape with an empty tail');
        $this->checkSame('A_B', ExtraInfoUtil::parsePopRetryTopic('%RETRY%GID+A_B', 'GID'),
            'the + right after the group wins, so a topic containing _ is read whole');
        $this->checkSame('', ExtraInfoUtil::parsePopRetryTopic('', 'GID'), 'an empty name stays empty');
        $this->checkSame('%RETRY%GID_Topic', ExtraInfoUtil::parsePopRetryTopic('%RETRY%GID_Topic', ''),
            'without a group name there is nothing to strip');
    }

    // ================================================================ 2. the retry budget drops before delivery

    /**
     * The boundary itself: the row that has used exactly the budget is delivered, the row one past it is
     * not. The dropped row is acked -- recorded by the broker double before the listener ever ran, with
     * the same CK the row came with -- and the wait-ack accumulator is left holding only what is actually
     * waiting. The drop is logged by name, whole.
     */
    private function testBudgetDropAtTheBoundary(): void
    {
        $this->captureLogs(Logger::DEBUG);
        try {
            [$c, $mq, $key, $pq, $listener, $inst] = $this->popConsumer('GID-PD-BUDGET', 'PdBudget');
            $c->maxReconsumeTimes = 3;
            $atLimit = $this->popped('PdBudget', 0, 100, 3);
            $onePast = $this->popped('PdBudget', 0, 101, 4);
            $inst->popScript = [new PopResult(PopStatus::FOUND, [$atLimit, $onePast])];

            $verdict = (bool) $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);

            $this->checkSame(1, count($listener->calls), 'one POP round hands the listener one batch');
            $this->checkSame([100], $listener->calls[0]['offsets'],
                'the row at the budget is delivered, so the guard is strictly greater than the budget');
            $this->checkSame(101, $inst->acks[0]['offset'],
                'the row one past the budget is the round\'s first ack, so it was never delivered');
            $this->checkSame([101, 100], $inst->ackedOffsets(),
                'the dropped row is acked first, and the delivered one follows once the listener accepts it');
            $this->checkSame(1, $listener->calls[0]['acks'],
                'the drop was acked before the listener ran, not after it');
            $this->checkSame(1, $listener->calls[0]['waitAck'],
                'the wait-ack accumulator keeps only what is still waiting for an ack');
            $this->checkSame(0, $inst->acks[0]['queueId'],
                'the ack names the queue the row was popped from');
            $this->checkSame('PdBudget', $inst->acks[0]['topic'],
                'an ordinary topic is acked under its own name');
            $this->checkSame(
                (string) $onePast->getProperty(MessageConst::PROPERTY_POP_CK),
                $inst->acks[0]['extraInfo'],
                'the ack carries the CK the row was popped with',
            );
            $this->checkSame(1, count($inst->pops), 'the round sent exactly one pop request');
            $this->checkSame(true, $verdict,
                'a round that delivered something leaves the next pop to the interval rule');
            $this->checkSame([3], $listener->calls[0]['reconsume'],
                'control: what reached the listener is the row at the budget, counted by its own reconsume times');
            $this->checkSame(0, count($inst->delays),
                'a dropped row is acked outright, not extended in invisible time');

            $infos = $this->messagesOf('INFO');
            $this->checkSame(['reconsume times has reached 3, so ack msg=MSG-0-101'], $infos,
                'the drop is logged once, with the budget it ran out against and the message itself');
        } finally {
            $this->releaseLogs();
        }
    }

    /**
     * The two ways the same guard could be wrong without the boundary case noticing: a budget set wider
     * than the rows (nothing may be dropped at all) and a budget left unset (the default of 16 applies to
     * both the drop and the number in the log).
     */
    private function testBudgetFollowsTheConsumersOwnNumber(): void
    {
        $this->captureLogs(Logger::DEBUG);
        try {
            // Leg 1: a budget wide enough for both rows -- the same pair goes through untouched.
            [$c1, $mq1, $key1, , $listener1, $inst1] = $this->popConsumer('GID-PD-WIDE', 'PdWide');
            $c1->maxReconsumeTimes = 5;
            $inst1->popScript = [new PopResult(PopStatus::FOUND, [
                $this->popped('PdWide', 0, 200, 3),
                $this->popped('PdWide', 0, 201, 4),
            ])];
            $this->call($c1, 'popOnce', [$mq1, $key1, microtime(true)]);
            $this->checkSame([200, 201], $listener1->calls[0]['offsets'],
                'with a budget both rows still clear, nothing is dropped');
            $this->checkSame(0, $listener1->calls[0]['acks'],
                'neither row was acked while the listener still had nothing to answer for');
            $this->checkSame([200, 201], $inst1->ackedOffsets(),
                'both rows reach the broker as acks only after the listener accepted them');
            $this->checkSame(0, count($this->linesOf('INFO')), 'a round that drops nothing logs no drop');

            // Leg 2: the unset budget -- 16 is the number the rows are measured and logged against.
            [$c2, $mq2, $key2, , $listener2, $inst2] = $this->popConsumer('GID-PD-DEFAULT', 'PdDefault');
            $inst2->popScript = [new PopResult(PopStatus::FOUND, [
                $this->popped('PdDefault', 0, 300, 16),
                $this->popped('PdDefault', 0, 301, 17),
            ])];
            $this->call($c2, 'popOnce', [$mq2, $key2, microtime(true)]);
            $this->checkSame([300], $listener2->calls[0]['offsets'],
                'an unset budget lets the sixteenth reconsume through and stops at the seventeenth');
            $this->checkSame(301, $inst2->acks[0]['offset'], 'the seventeenth is acked away');
            $this->checkSame([301, 300], $inst2->ackedOffsets(),
                'and the sixteenth that the listener delivered is acked behind it');
            $this->checkSame(['reconsume times has reached 16, so ack msg=MSG-0-301'], $this->messagesOf('INFO'),
                'the log names the default budget the guard fell back to');
        } finally {
            $this->releaseLogs();
        }
    }

    /**
     * The paired control for the placement of the guard: the very same rows, on the pull path, with a
     * consumer that would drop the second one in POP mode. A pull advances the offset, so a row skipped
     * before delivery would be gone with no send-back and no ack -- nothing may be dropped here, and
     * nothing may be acked either.
     */
    private function testPullDeliveryNeverPreDropsOnTheBudget(): void
    {
        $this->captureLogs(Logger::DEBUG);
        try {
            [$c, $mq, $key, $listener, $inst] = $this->pullConsumer('GID-PD-PULL', 'PdPull');
            $c->maxReconsumeTimes = 3;
            $c->pullBatchSize = 32;
            $inst->pullScript = [new PullResult(
                PullStatus::FOUND,
                nextBeginOffset: 102,
                minOffset: 0,
                maxOffset: 102,
                msgFoundList: [
                    $this->pulled('PdPull', 0, 100, 3),
                    $this->pulled('PdPull', 0, 101, 4),
                ],
            )];
            $this->call($c, 'pullOnce', [$mq, $key, false, microtime(true)]);
            $this->checkSame(2, count($this->prop($c, 'pending')[$key] ?? []),
                'both rows are buffered for delivery, the one past the budget included');

            $this->call($c, 'dispatchRound', []);
            $this->checkSame(1, count($inst->pulls), 'the pull round asked the broker once');
            $this->checkSame([100, 101], $listener->calls[0]['offsets'],
                'the pull path delivers the row past its budget too: only POP pre-drops');
            $this->checkSame([], $inst->ackedOffsets(),
                'the pull path sends no ack for the row a POP round would have dropped');
            $this->checkSame(0, count($this->linesOf('INFO')),
                'and it logs no budget drop, because it made none');
            $this->checkSame(102, (int) ($this->prop($c, 'offsetTable')[$key] ?? -1),
                'the pull cursor moved past both rows, so dropping one before delivery would lose it');
        } finally {
            $this->releaseLogs();
        }
    }

    /**
     * The early guard is an addition: the check the listener's own result feeds is still there. A row
     * delivered at exactly the budget and then refused by the listener is redelivered later through that
     * check, and is not acked away by the early one.
     */
    private function testPostListenerBudgetCheckStillEnforced(): void
    {
        [$c, $mq, $key, , $listener, $inst] = $this->popConsumer('GID-PD-POST', 'PdPost');
        $c->maxReconsumeTimes = 3;
        $listener->refuse = true;
        $atLimit = $this->popped('PdPost', 0, 400, 3);
        $inst->popScript = [new PopResult(PopStatus::FOUND, [$atLimit])];
        $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);

        $this->checkSame([400], $listener->calls[0]['offsets'],
            'the row at the budget was delivered rather than dropped early');
        $this->checkSame([], $inst->ackedOffsets(),
            'a refused batch is not acked, so the early drop did not swallow it');
        $this->checkSame(1, count($inst->delays),
            'the listener-side budget check is still the one that reschedules the refused row');
        $this->checkSame(400, $inst->delays[0]['offset'],
            'and it reschedules the row the listener refused');
    }

    // ================================================================ 3. the retry topic name is restored

    /**
     * The four names the rewrite is defined on, delivered through a real POP round: the two shapes that
     * carry an ordinary topic are rewritten before the listener runs, and the two that do not keep their
     * name. Every ack is rebuilt from the delivery name the row ended with plus the retry flag its own CK
     * carries, so the rewrite of a shaped name lands the ack back on the queue the row came from; the
     * names the rewrite leaves alone are re-wrapped by that same rebuild, and only a live broker says
     * whether it takes them.
     */
    private function testPopRetryTopicNormalisedBeforeDelivery(): void
    {
        $cases = [
            ['%RETRY%GID-PD-RT+OrdinaryTopic', 'OrdinaryTopic', '%RETRY%GID-PD-RT+OrdinaryTopic',
                'the V2 name is rewritten at the delivery entry'],
            ['%RETRY%GID-PD-RT_OrdinaryTopic', 'OrdinaryTopic', '%RETRY%GID-PD-RT_OrdinaryTopic',
                'the V1 name is rewritten at the delivery entry'],
            ['%RETRY%GID-PD-RT', '%RETRY%GID-PD-RT', '%RETRY%GID-PD-RT_%RETRY%GID-PD-RT',
                'exactly the group retry topic is delivered under its own name'],
            ['OrdinaryTopic', 'OrdinaryTopic', 'OrdinaryTopic',
                'control: an ordinary topic is untouched by the rewrite'],
            ['%RETRY%Other+OrdinaryTopic', '%RETRY%Other+OrdinaryTopic',
                '%RETRY%GID-PD-RT+%RETRY%Other+OrdinaryTopic',
                "control: another group's retry topic is not rewritten by this consumer"],
        ];
        foreach ($cases as $i => [$topic, $expected, $ackedTopic, $label]) {
            [$c, $mq, $key, , $listener, $inst] = $this->popConsumer('GID-PD-RT', $topic);
            $inst->popScript = [new PopResult(PopStatus::FOUND, [$this->popped($topic, 0, 10 + $i, 0)])];
            $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);
            $this->checkSame([$expected], $listener->allTopics(), $label);
            $this->checkSame([$ackedTopic], array_map(static fn(array $a): string => $a['topic'], $inst->acks),
                $label . ' -- and the ack is rebuilt from the delivery name with the CK retry flag');
        }
    }

    /**
     * The same rule with the namespace this consumer was configured with: the name is unwrapped, the
     * ordinary topic taken out of it, and the wrap taken off the result, so the listener sees the plain
     * topic it would see without any namespace at all.
     */
    private function testNamespacedPopRetryTopicNormalised(): void
    {
        $cases = [
            ['%RETRY%NS%GID-PD-NS+NS%OrdinaryTopic', 'OrdinaryTopic'],
            ['%RETRY%NS%GID-PD-NS_NS%OrdinaryTopic', 'OrdinaryTopic'],
            ['NS%OrdinaryTopic', 'OrdinaryTopic'],
            ['%RETRY%NS%GID-PD-NS', '%RETRY%GID-PD-NS'],
        ];
        foreach ($cases as $i => [$topic, $expected]) {
            [$c, $mq, $key, , $listener, $inst] = $this->popConsumer('NS%GID-PD-NS', $topic);
            $c->namespace = 'NS';
            $inst->popScript = [new PopResult(PopStatus::FOUND, [$this->popped($topic, 0, 20 + $i, 0)])];
            $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);
            $this->checkSame([$expected], $listener->allTopics(),
                sprintf('a namespaced %s reaches the listener as %s', $topic, $expected));
        }
    }

    /**
     * Where the rewrite is allowed to happen. Both concurrent entries carry it -- the POP batch above
     * and a pulled batch -- and the orderly entry does not: it consumes the queue under the name that
     * queue has, so it must be handed exactly that name. The two legs differ only in the listener, so a
     * rewrite that moved up into the shared restore would be caught by the orderly leg.
     */
    private function testNormalisationIsConcurrentOnly(): void
    {
        $retryTopic = '%RETRY%GID-PD-ENTRY+OrdinaryTopic';

        // Leg 1: a pulled batch, delivered through the concurrent entry.
        [$c, $mq, $key] = $this->pullConsumer('GID-PD-ENTRY', $retryTopic);
        $listener = new PdConcurrentListener();
        $c->messageListener = $listener;
        $this->call($c, 'consumeBatchHeld', [$key, $mq, [$this->pulled($retryTopic, 0, 500, 0)]]);
        $this->checkSame([$retryTopic], array_map(static fn(MessageExt $m): string => (string) $m->topic, [$this->pulled($retryTopic, 0, 500, 0)]),
            'control: the fixture hands over the retry-shaped name it was built with');
        $this->checkSame(['OrdinaryTopic'], $listener->allTopics(),
            'the pull path\'s concurrent entry rewrites the retry topic the same way');

        // Leg 2: the same batch through the orderly entry of the same consumer.
        $orderly = new PdOrderlyListener();
        $c->messageListener = $orderly;
        $again = $this->pulled($retryTopic, 0, 501, 0);
        $this->call($c, 'consumeBatchHeld', [$key, $mq, [$again]]);
        $this->checkSame(1, count($orderly->calls), 'the orderly entry ran the batch');
        $this->checkSame([$retryTopic], $orderly->calls[0],
            'the orderly entry never rewrites the retry topic: it delivers the name of the queue it locked');
    }

    // ================================================================ 4. the round's re-schedule is state

    /**
     * The three empty-handed statuses apart, read from the due time the round left behind instead of from
     * a clock: a busy-polling broker is worth 3000 ms, a round that found nothing is worth this client's
     * own polling interval, and a round that popped everything away is worth nothing at all. The
     * intervals are deliberately all different numbers, so an arm that took the wrong one cannot pass.
     */
    private function testRescheduleByStatus(): void
    {
        // Leg 1: POLLING_FULL.
        [$c, $mq, $key] = $this->popConsumer('GID-PD-RESCHED', 'PdResched');
        $c->popIntervalMillis = 250;
        $c->pullInterval = 1500;
        /** @var PdInstance $inst */
        $inst = $c->mqClient;
        $inst->popScript = [new PopResult(PopStatus::POLLING_FULL)];
        $before = microtime(true);
        $verdict = (bool) $this->call($c, 'popOnce', [$mq, $key, $before]);
        $due = $c->nextPopAllowedAt($key);
        $this->checkSame(false, $verdict, 'a POLLING_FULL round ends this queue\'s popping for the tick');
        $this->check($due !== null, 'a POLLING_FULL round left a due time behind');
        $this->check($due !== null && abs(($due - $before) - 3.0) < 0.05,
            'POLLING_FULL defers the next pop by 3000 ms, not by this client\'s polling interval');

        // Leg 2: POLLING_NOT_FOUND -- the client's own interval, a different number from both neighbours.
        [$c2, $mq2, $key2] = $this->popConsumer('GID-PD-RESCHED2', 'PdResched2');
        $c2->popIntervalMillis = 250;
        $c2->pullInterval = 1500;
        $inst2 = $c2->mqClient;
        $inst2->popScript = [new PopResult(PopStatus::POLLING_NOT_FOUND)];
        $before2 = microtime(true);
        $this->checkSame(false, (bool) $this->call($c2, 'popOnce', [$mq2, $key2, $before2]),
            'a round that found nothing also ends this queue for the tick');
        $due2 = $c2->nextPopAllowedAt($key2);
        $this->check($due2 !== null && abs(($due2 - $before2) - 0.25) < 0.05,
            'a POP that found nothing waits this client\'s own polling interval ('
                . ($due2 === null ? 'none' : round(($due2 - $before2) * 1000)) . ' ms)');

        // Leg 3: NO_NEW_MSG, the other no-message status, same wait as its neighbour.
        [$c3, $mq3, $key3] = $this->popConsumer('GID-PD-RESCHED3', 'PdResched3');
        $c3->popIntervalMillis = 250;
        $inst3 = $c3->mqClient;
        $inst3->popScript = [new PopResult(PopStatus::NO_NEW_MSG)];
        $before3 = microtime(true);
        $this->call($c3, 'popOnce', [$mq3, $key3, $before3]);
        $due3 = $c3->nextPopAllowedAt($key3);
        $this->check($due3 !== null && abs(($due3 - $before3) - 0.25) < 0.05,
            'NO_NEW_MSG is paced exactly like a round that found nothing, not like a busy broker');

        // Leg 4: FOUND with nothing left after the pre-delivery step -- allowed again at once.
        [$c4, $mq4, $key4, $pq4, $listener4, $inst4] = $this->popConsumer('GID-PD-RESCHED4', 'PdResched4', 'TagA');
        $c4->popIntervalMillis = 250;
        $inst4->popScript = [new PopResult(PopStatus::FOUND, [
            $this->popped('PdResched4', 0, 600, 0, tag: 'TagB'),
            $this->popped('PdResched4', 0, 601, 0, tag: 'TagB'),
        ])];
        $this->captureLogs(Logger::DEBUG);
        try {
            $verdict4 = (bool) $this->call($c4, 'popOnce', [$mq4, $key4, microtime(true)]);
            $this->checkSame(true, $verdict4,
                'a round whose whole batch was dropped before delivery asks for the next pop at once');
            $this->checkSame(null, $c4->nextPopAllowedAt($key4),
                'and it leaves no wait behind: nothing of that batch is waiting for an ack');
            $this->checkSame([], $listener4->calls, 'nothing was delivered');
            $this->checkSame([600, 601], $inst4->ackedOffsets(),
                'everything popped was acked, since none of it can be consumed');
            $this->checkSame(0, $pq4->waitAckCount(),
                'and the wait-ack accumulator is back to empty, so nothing holds the next round');
            $this->checkSame(['pop filter dropped 2 of 2 messages (acked)'], $this->messagesOf('INFO'),
                'the tag filter reports its own drop, and the budget guard logs none');
        } finally {
            $this->releaseLogs();
        }
    }

    /**
     * A round that delivered something is paced by ``pullInterval``, and only when that is above zero:
     * with a budget of zero the next pop is asked for at once. The interval the round is measured against
     * is the pull one and not the polling one, so the two are deliberately different numbers here.
     */
    private function testRescheduleAfterDelivery(): void
    {
        // Leg 1: pullInterval above zero.
        [$c, $mq, $key, , $listener, $inst] = $this->popConsumer('GID-PD-PULLINT', 'PdPullInt');
        $c->popIntervalMillis = 250;
        $c->pullInterval = 1500;
        $inst->popScript = [new PopResult(PopStatus::FOUND, [$this->popped('PdPullInt', 0, 700, 0)])];
        $before = microtime(true);
        $verdict = (bool) $this->call($c, 'popOnce', [$mq, $key, $before]);
        $this->checkSame(false, $verdict, 'a delivered round with a pull interval ends this queue for the tick');
        $due = $c->nextPopAllowedAt($key);
        $this->check($due !== null && abs(($due - $before) - 1.5) < 0.05,
            'a delivered round defers the next pop by pullInterval, not by the polling interval');
        $this->checkSame(1, count($inst->pops), 'one pop request went out for a deferred round');

        // Leg 2: control, pullInterval zero -- allowed again at once.
        [$c2, $mq2, $key2, , , $inst2] = $this->popConsumer('GID-PD-PULLINT0', 'PdPullInt0');
        $c2->popIntervalMillis = 250;
        $c2->pullInterval = 0;
        $inst2->popScript = [new PopResult(PopStatus::FOUND, [$this->popped('PdPullInt0', 0, 701, 0)])];
        $this->checkSame(true, (bool) $this->call($c2, 'popOnce', [$mq2, $key2, microtime(true)]),
            'with no pull interval a delivered round asks for the next pop immediately');
        $this->checkSame(null, $c2->nextPopAllowedAt($key2),
            'and leaves no wait behind, whatever the polling interval says');
    }

    /**
     * The re-schedule is not just a number written down: the round obeys it. ``popRound()`` is asked for
     * twice, and the queue that is inside its wait sends nothing on the second one -- then the same queue
     * pops again the moment the recorded due time is paid. Bounded rounds, no sleeping.
     */
    private function testDeferredStateHoldsTheNextRound(): void
    {
        [$c, $mq, $key, , , $inst] = $this->popConsumer('GID-PD-HOLD', 'PdHold');
        $c->popIntervalMillis = 250;
        $inst->popScript = [
            new PopResult(PopStatus::POLLING_FULL),
            new PopResult(PopStatus::POLLING_FULL),
            new PopResult(PopStatus::POLLING_FULL),
        ];
        $this->call($c, 'popRound', [microtime(true)]);
        $this->checkSame(1, count($inst->pops), 'the first round of the tick pops the queue once');
        $this->call($c, 'popRound', [microtime(true)]);
        $this->checkSame(1, count($inst->pops),
            'the next round of the same tick sends nothing: the queue is inside its own wait');

        // Pay the wait by moving the recorded due time, never by waiting.
        $this->setProp($c, 'idleUntil', [$key => microtime(true) - 0.001]);
        $this->call($c, 'popRound', [microtime(true)]);
        $this->checkSame(2, count($inst->pops), 'once the wait is due the very same queue pops again');
    }

    /**
     * What a round is counted as: the round's cost is recorded whenever the broker found messages, while
     * the delivered count is the TPS -- the rows that reached the listener, not the rows the pop handed
     * over. A round that dropped everything before delivery therefore records a round and no traffic.
     */
    private function testStatsCountTheDeliveredRowsNotThePoppedOnes(): void
    {
        // Leg 1: three popped, one of them dropped by the budget.
        [$c, $mq, $key, , , $inst] = $this->popConsumer('GID-PD-STATS', 'PdStats');
        $c->maxReconsumeTimes = 2;
        $inst->popScript = [new PopResult(PopStatus::FOUND, [
            $this->popped('PdStats', 0, 800, 0),
            $this->popped('PdStats', 0, 801, 1),
            $this->popped('PdStats', 0, 802, 3),
        ])];
        $this->call($c, 'popOnce', [$mq, $key, microtime(true)]);
        /** @var ConsumerStatsManager $stats */
        $stats = $this->prop($c, 'statsManager');
        $tps = $stats->topicAndGroupPullTps->find('PdStats@GID-PD-STATS');
        $rt = $stats->topicAndGroupPullRt->find('PdStats@GID-PD-STATS');
        $this->checkSame(2, $tps?->getValue(), 'TPS counts the rows delivered, not the rows popped');
        $this->checkSame(1, $tps?->getTimes(), 'one round adds one TPS sample');
        $this->checkSame(1, $rt?->getTimes(), 'the same round records its own cost once');

        // Leg 2: control, nothing survives the pre-delivery step -- a round, and no traffic.
        [$c2, $mq2, $key2, , , $inst2] = $this->popConsumer('GID-PD-STATS2', 'PdStats2', 'TagA');
        $inst2->popScript = [new PopResult(PopStatus::FOUND, [
            $this->popped('PdStats2', 0, 803, 0, tag: 'TagB'),
            $this->popped('PdStats2', 0, 804, 0, tag: 'TagB'),
        ])];
        $this->call($c2, 'popOnce', [$mq2, $key2, microtime(true)]);
        /** @var ConsumerStatsManager $stats2 */
        $stats2 = $this->prop($c2, 'statsManager');
        $this->checkSame(1, $stats2->topicAndGroupPullRt->find('PdStats2@GID-PD-STATS2')?->getTimes(),
            'a round that delivered nothing still recorded its cost');
        $this->checkSame(null, $stats2->topicAndGroupPullTps->find('PdStats2@GID-PD-STATS2'),
            'and it recorded no delivered traffic at all');

        // Leg 3: control, an empty POP records neither.
        [$c3, $mq3, $key3, , , $inst3] = $this->popConsumer('GID-PD-STATS3', 'PdStats3');
        $inst3->popScript = [new PopResult(PopStatus::POLLING_NOT_FOUND)];
        $this->call($c3, 'popOnce', [$mq3, $key3, microtime(true)]);
        /** @var ConsumerStatsManager $stats3 */
        $stats3 = $this->prop($c3, 'statsManager');
        $this->checkSame(null, $stats3->topicAndGroupPullRt->find('PdStats3@GID-PD-STATS3'),
            'a round that found no messages is not a round with a pull cost');
    }

    /**
     * The error path is its own rule and stays where it was: FLOW_CONTROL(215) is worth 20 ms because
     * the broker throttled this request, anything else the general error backoff. Both are asserted with
     * the status waits set to entirely different numbers, so an error branch that fell through to the
     * status table would be caught rather than looking like a working re-schedule.
     */
    private function testErrorBackoffUntouched(): void
    {
        $this->checkSame(20, DefaultMQPushConsumer::pullBackoffMillisForResponseCode(ResponseCode::FLOW_CONTROL),
            'a throttled request is retried after 20 ms');
        $this->checkSame(500, DefaultMQPushConsumer::pullBackoffMillisForResponseCode(ResponseCode::SYSTEM_ERROR),
            'any other broker error keeps the general error backoff');

        $legs = [
            [ResponseCode::FLOW_CONTROL, 0.02, 'a POP answered FLOW_CONTROL(215) waits 20 ms'],
            [ResponseCode::SYSTEM_ERROR, 0.5, 'a POP answered any other error waits the general error backoff'],
            [ResponseCode::TOPIC_NOT_EXIST, 0.5, 'control: a not-yet-created topic is not a throttling answer'],
        ];
        foreach ($legs as [$code, $expectedSeconds, $label]) {
            [$c, $mq, $key] = $this->popConsumer('GID-PD-ERR' . $code, 'PdErr' . $code);
            $c->popIntervalMillis = 250;
            $c->pullInterval = 1500;
            /** @var PdInstance $inst */
            $inst = $c->mqClient;
            $inst->popScript = [new MQBrokerException($code, 'broker says no')];
            $before = microtime(true);
            $verdict = (bool) $this->call($c, 'popOnce', [$mq, $key, $before]);
            $due = $c->nextPopAllowedAt($key);
            $this->checkSame(false, $verdict, $label . ': the round ends here');
            $this->check($due !== null && abs(($due - $before) - $expectedSeconds) < 0.05,
                $label . ' (' . ($due === null ? 'none' : round(($due - $before) * 1000)) . ' ms)');
        }
    }

    // ================================================================ 5. the pull round's telemetry

    /**
     * The pull path's round telemetry, pinned on both halves at once -- the counterpart of the POP round
     * above, since both surfaces read out the same two figures:
     *
     * - **PULL_RT belongs to a FOUND round and to no other round.** A round the broker answered with
     *   ``NO_NEW_MSG`` or ``NO_MATCHED_MSG`` carried no message, so whatever its wall time was, it says
     *   nothing about the latency of the deliveries this topic actually makes -- and idle rounds are the
     *   overwhelming majority, so folding them in would dilute the figure into noise. A FOUND round
     *   records its cost even when no row came back with it: the status is what the rule is gated on.
     * - **PULL_TPS counts the rows that survive the pre-delivery filter, not the rows the broker handed
     *   over.** A row the subscription's tag pass removed was never buffered, never reached the listener
     *   and never became a consume sample, so counting it would inflate this topic's throughput by
     *   exactly the rows this client itself dropped.
     *
     * The two rules are independent, so each leg asserts both halves: a leg that only watched the traffic
     * figure would still pass if the cost rule moved, and the reverse. Every leg runs one round -- or a
     * FOUND round followed by a non-FOUND one on the same queue, to show the second adds nothing to a
     * sample already on the books -- against the recording instance double, with no socket and no clock.
     */
    private function testPullRoundTelemetryRecordsOnlyWhatIsDelivered(): void
    {
        // Leg 1: a FOUND round whose every row is dropped by the subscription's tag: a cost, no traffic.
        [$c1, $mq1, $key1, $listener1, $inst1] = $this->pullConsumer('GID-PD-PSTATS-DROP', 'PdPullDrop', 'TagA');
        $c1->pullBatchSize = 32;
        $inst1->pullScript = [new PullResult(
            PullStatus::FOUND,
            nextBeginOffset: 902,
            minOffset: 0,
            maxOffset: 902,
            msgFoundList: [
                $this->pulled('PdPullDrop', 0, 900, 0, tag: 'TagB'),
                $this->pulled('PdPullDrop', 0, 901, 0, tag: 'TagB'),
            ],
        )];
        $this->call($c1, 'pullOnce', [$mq1, $key1, false, microtime(true)]);
        /** @var ConsumerStatsManager $stats1 */
        $stats1 = $this->prop($c1, 'statsManager');
        $rt1 = $stats1->topicAndGroupPullRt->find('PdPullDrop@GID-PD-PSTATS-DROP');
        $this->checkSame(1, count($inst1->pulls), 'the round sent exactly one pull request');
        $this->check($rt1 !== null,
            'a FOUND round records its own cost even when the filter removed every row it brought back');
        $this->checkSame(1, $rt1?->getTimes(), 'one round adds one cost sample, whatever became of its rows');
        $this->check($rt1 !== null && $rt1->getValue() >= 0, 'and the cost it records is the round trip');
        $this->checkSame(null, $stats1->topicAndGroupPullTps->find('PdPullDrop@GID-PD-PSTATS-DROP'),
            'a round that delivered nothing adds nothing to the throughput of this topic');
        $this->checkSame([], $this->prop($c1, 'pending')[$key1] ?? [],
            'the filtered rows were never buffered for delivery, which is why they count as no traffic');
        $this->checkSame([], $listener1->calls, 'and the listener was handed nothing');
        $this->checkSame(902, (int) ($this->prop($c1, 'offsetTable')[$key1] ?? -1),
            'the cursor still moved past the dropped rows: a pull confirms by offset, not by delivery');

        // Leg 2: a partial drop -- three popped, the tag pass removes one, so the round books two.
        [$c2, $mq2, $key2, $listener2, $inst2] = $this->pullConsumer('GID-PD-PSTATS-PART', 'PdPullPart', 'TagA');
        $c2->pullBatchSize = 32;
        $inst2->pullScript = [new PullResult(
            PullStatus::FOUND,
            nextBeginOffset: 913,
            minOffset: 0,
            maxOffset: 913,
            msgFoundList: [
                $this->pulled('PdPullPart', 0, 910, 0, tag: 'TagA'),
                $this->pulled('PdPullPart', 0, 911, 0, tag: 'TagB'),
                $this->pulled('PdPullPart', 0, 912, 0, tag: 'TagA'),
            ],
        )];
        $this->call($c2, 'pullOnce', [$mq2, $key2, false, microtime(true)]);
        /** @var ConsumerStatsManager $stats2 */
        $stats2 = $this->prop($c2, 'statsManager');
        $tps2 = $stats2->topicAndGroupPullTps->find('PdPullPart@GID-PD-PSTATS-PART');
        $this->checkSame(2, $tps2?->getValue(),
            'throughput is the rows left after the filter, not the three the broker handed over');
        $this->checkSame(1, $tps2?->getTimes(), 'the dropped row leaves the round with a single sample');
        $this->checkSame(1, $stats2->topicAndGroupPullRt->find('PdPullPart@GID-PD-PSTATS-PART')?->getTimes(),
            'and the same round recorded its cost once, on the way in');
        $this->checkSame([910, 912], $this->offsetsOf($this->prop($c2, 'pending')[$key2] ?? []),
            'the buffered rows are exactly the two that were counted');
        $this->call($c2, 'dispatchRound', []);
        $this->checkSame([[910, 912]], array_map(
            static fn(array $call): array => $call['offsets'],
            $listener2->calls,
        ), 'control: what the listener is handed is the same two rows, so the count is a delivery count');

        // Leg 3: control -- nothing is filtered out, so the round books every row it popped.
        [$c3, $mq3, $key3, , $inst3] = $this->pullConsumer('GID-PD-PSTATS-ALL', 'PdPullAll');
        $c3->pullBatchSize = 32;
        $inst3->pullScript = [new PullResult(
            PullStatus::FOUND,
            nextBeginOffset: 923,
            minOffset: 0,
            maxOffset: 923,
            msgFoundList: [
                $this->pulled('PdPullAll', 0, 920, 0),
                $this->pulled('PdPullAll', 0, 921, 0),
                $this->pulled('PdPullAll', 0, 922, 0),
            ],
        )];
        $this->call($c3, 'pullOnce', [$mq3, $key3, false, microtime(true)]);
        /** @var ConsumerStatsManager $stats3 */
        $stats3 = $this->prop($c3, 'statsManager');
        $this->checkSame(3, $stats3->topicAndGroupPullTps->find('PdPullAll@GID-PD-PSTATS-ALL')?->getValue(),
            'with an open subscription the round books all three popped rows: the filter is not simply shrinking the count');
        $this->checkSame(1, $stats3->topicAndGroupPullRt->find('PdPullAll@GID-PD-PSTATS-ALL')?->getTimes(),
            'and one cost sample for one round');

        // Leg 4: a FOUND round followed by a non-FOUND one on the same queue -- the second round must add
        // nothing at all, to either half. This is the leg that fails the moment the cost stops being
        // status-gated.
        [$c4, $mq4, $key4, , $inst4] = $this->pullConsumer('GID-PD-PSTATS-IDLE', 'PdPullIdle');
        $c4->pullBatchSize = 32;
        $inst4->pullScript = [
            new PullResult(
                PullStatus::FOUND,
                nextBeginOffset: 930,
                minOffset: 0,
                maxOffset: 930,
                msgFoundList: [$this->pulled('PdPullIdle', 0, 929, 0)],
            ),
            new PullResult(PullStatus::NO_NEW_MSG, nextBeginOffset: 930, minOffset: 0, maxOffset: 930),
        ];
        $this->call($c4, 'pullOnce', [$mq4, $key4, false, microtime(true)]);
        /** @var ConsumerStatsManager $stats4 */
        $stats4 = $this->prop($c4, 'statsManager');
        $this->checkSame(1, $stats4->topicAndGroupPullRt->find('PdPullIdle@GID-PD-PSTATS-IDLE')?->getTimes(),
            'the FOUND round of the pair recorded its cost');
        $this->call($c4, 'pullOnce', [$mq4, $key4, false, microtime(true)]);
        $this->checkSame(2, count($inst4->pulls), 'the idle round really ran: the broker was asked again');
        $this->checkSame(1, $stats4->topicAndGroupPullRt->find('PdPullIdle@GID-PD-PSTATS-IDLE')?->getTimes(),
            'a round that found no new message added no cost sample: the figure stays the latency of the rounds that carried something');
        $this->checkSame(1, $stats4->topicAndGroupPullTps->find('PdPullIdle@GID-PD-PSTATS-IDLE')?->getValue(),
            'and it added no traffic either: the throughput of the pair is the one row the FOUND round delivered');
        $this->checkSame(1, $stats4->topicAndGroupPullTps->find('PdPullIdle@GID-PD-PSTATS-IDLE')?->getTimes(),
            'the idle round left the delivered-sample count of the pair untouched');

        // Leg 5: the other non-FOUND statuses, each on a fresh queue -- neither half of the telemetry.
        foreach ([PullStatus::NO_NEW_MSG, PullStatus::NO_MATCHED_MSG] as $status) {
            [$c5, $mq5, $key5, , $inst5] = $this->pullConsumer('GID-PD-PSTATS-' . $status->name, 'PdPull' . $status->name);
            $c5->pullBatchSize = 32;
            $inst5->pullScript = [new PullResult($status, nextBeginOffset: 940, minOffset: 0, maxOffset: 950)];
            $this->call($c5, 'pullOnce', [$mq5, $key5, false, microtime(true)]);
            /** @var ConsumerStatsManager $stats5 */
            $stats5 = $this->prop($c5, 'statsManager');
            $this->checkSame(1, count($inst5->pulls),
                'a ' . $status->name . ' round ran against the broker');
            $this->checkSame(null, $stats5->topicAndGroupPullRt->find('PdPull' . $status->name . '@GID-PD-PSTATS-' . $status->name),
                'a ' . $status->name . ' round records no pull cost');
            $this->checkSame(null, $stats5->topicAndGroupPullTps->find('PdPull' . $status->name . '@GID-PD-PSTATS-' . $status->name),
                'and no pull traffic either: an idle round is absent from both figures');
        }

        // Leg 6: a FOUND round that came back with an empty list -- the status gates the cost, not the rows.
        [$c6, $mq6, $key6, , $inst6] = $this->pullConsumer('GID-PD-PSTATS-EMPTY', 'PdPullEmpty');
        $c6->pullBatchSize = 32;
        $inst6->pullScript = [new PullResult(PullStatus::FOUND, nextBeginOffset: 960, minOffset: 0, maxOffset: 960)];
        $this->call($c6, 'pullOnce', [$mq6, $key6, false, microtime(true)]);
        /** @var ConsumerStatsManager $stats6 */
        $stats6 = $this->prop($c6, 'statsManager');
        $this->checkSame(1, $stats6->topicAndGroupPullRt->find('PdPullEmpty@GID-PD-PSTATS-EMPTY')?->getTimes(),
            'a FOUND round with no row in hand is still a round this client paid for');
        $this->checkSame(null, $stats6->topicAndGroupPullTps->find('PdPullEmpty@GID-PD-PSTATS-EMPTY'),
            'and it books no throughput, since it delivered nothing');
    }

    /**
     * @param list<MessageExt> $msgs
     * @return list<int>
     */
    private function offsetsOf(array $msgs): array
    {
        return array_map(static fn(MessageExt $m): int => $m->queueOffset, $msgs);
    }

    // ================================================================ runner

    public function run(): int
    {
        $this->testPureRules();
        $this->testBudgetDropAtTheBoundary();
        $this->testBudgetFollowsTheConsumersOwnNumber();
        $this->testPullDeliveryNeverPreDropsOnTheBudget();
        $this->testPostListenerBudgetCheckStillEnforced();
        $this->testPopRetryTopicNormalisedBeforeDelivery();
        $this->testNamespacedPopRetryTopicNormalised();
        $this->testNormalisationIsConcurrentOnly();
        $this->testRescheduleByStatus();
        $this->testRescheduleAfterDelivery();
        $this->testDeferredStateHoldsTheNextRound();
        $this->testStatsCountTheDeliveredRowsNotThePoppedOnes();
        $this->testPullRoundTelemetryRecordsOnlyWhatIsDelivered();
        $this->testErrorBackoffUntouched();
        return $this->summary();
    }
}

exit((new RunClientPopDelivery())->run());
