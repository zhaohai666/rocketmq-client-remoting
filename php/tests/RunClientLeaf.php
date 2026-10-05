<?php

declare(strict_types=1);

/**
 * Client 叶子模块纯 PHP assert 风格自测（不依赖 phpunit）。
 *
 * 覆盖：SendResult / ConsumerResult / TopAddressing / Latency / Metrics / Backpressure /
 * Hook / Validators / ConsumeExecutor / ConsumerStats / RequestReply / TraceContextPropagator。
 *
 * 运行：C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunClientLeaf.php
 * 全部通过输出 "ALL TESTS PASSED (N checks)"，任一失败列出明细并以非零码退出。
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ChangeInvisibleTimeResult;
use RocketMQ\Client\CheckForbiddenContext;
use RocketMQ\Client\CheckForbiddenHook;
use RocketMQ\Client\ClientMetrics;
use RocketMQ\Client\CommunicationMode;
use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\ConsumeExecutor;
use RocketMQ\Client\ConsumeMessageContext;
use RocketMQ\Client\ConsumeMessageHook;
use RocketMQ\Client\ConsumeOrderlyContext;
use RocketMQ\Client\ConsumeOrderlyStatus;
use RocketMQ\Client\ConsumeReturnType;
use RocketMQ\Client\ConsumerStatsManager;
use RocketMQ\Client\CorrelationIdUtil;
use RocketMQ\Client\DefaultTopAddressing;
use RocketMQ\Client\EndTransactionContext;
use RocketMQ\Client\EndTransactionHook;
use RocketMQ\Client\Exceptions\ClientErrorCode;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\FairSemaphore;
use RocketMQ\Client\FaultItem;
use RocketMQ\Client\FilterMessageContext;
use RocketMQ\Client\FilterMessageHook;
use RocketMQ\Client\LatencyFaultToleranceImpl;
use RocketMQ\Client\Logger;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MessageUtil;
use RocketMQ\Client\MQFaultStrategy;
use RocketMQ\Client\Pending;
use RocketMQ\Client\PopResult;
use RocketMQ\Client\PopStatus;
use RocketMQ\Client\PullResult;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\RejectedExecutionError;
use RocketMQ\Client\RequestCallback;
use RocketMQ\Client\RequestFutureHolder;
use RocketMQ\Client\RequestReply;
use RocketMQ\Client\RequestResponseFuture;
use RocketMQ\Client\SendMessageContext;
use RocketMQ\Client\SendMessageHook;
use RocketMQ\Client\SendResult;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\StatsItem;
use RocketMQ\Client\StatsItemSet;
use RocketMQ\Client\StatsSnapshot;
use RocketMQ\Client\TraceContextPropagator;
use RocketMQ\Client\Validators;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MessageType;
use RocketMQ\Common\MixAll;
use RocketMQ\Remoting\Protocol\ConsumeStatus;
use RocketMQ\Remoting\Protocol\ResponseCode;

use function RocketMQ\Client\consumeStatusName;

// ==================================================================== 测试替身

/** 覆写 httpGet 注入 mock 响应的 DefaultTopAddressing。 */
final class MockTopAddressing extends DefaultTopAddressing
{
    /** @var list<string|\Throwable> */
    public array $responses;
    /** @var list<string> */
    public array $seenUrls = [];

    /** @param list<string|\Throwable> $responses */
    public function __construct(array $responses, string $wsAddr = 'http://mock/rocketmq/nsaddr', string $unitName = '', ?array $para = null)
    {
        $this->responses = $responses;
        parent::__construct($wsAddr, $unitName, $para);
    }

    public function httpGet(string $url, float $timeoutSeconds): ?string
    {
        $this->seenUrls[] = $url;
        $r = array_shift($this->responses);
        if ($r instanceof \Throwable) {
            throw $r;
        }
        return $r;
    }
}

/** MQFaultStrategy.selectOneMessageQueue 依赖的 TopicPublishInfo 替身。 */
final class FakeTopicPublishInfo
{
    public int $idx = 0;
    public int $resetCount = 0;

    /** @param list<MessageQueue> $queues */
    public function __construct(public array $queues)
    {
    }

    public function resetIndex(): void
    {
        $this->idx = 0;
        $this->resetCount++;
    }

    public function selectOneMessageQueue(callable ...$filters): ?MessageQueue
    {
        $n = count($this->queues);
        if ($n === 0) {
            return null;
        }
        for ($i = 0; $i < $n; $i++) {
            $mq = $this->queues[($this->idx + $i) % $n];
            $ok = true;
            foreach ($filters as $f) {
                if (!$f($mq)) {
                    $ok = false;
                    break;
                }
            }
            if ($ok) {
                return $mq;
            }
        }
        return null;
    }
}

/** 记录回调是否被调用的 RequestCallback 实现。 */
final class RecordingRequestCallback implements RequestCallback
{
    public int $successCount = 0;
    public int $exceptionCount = 0;
    public ?Message $lastResponse = null;
    public ?\Throwable $lastCause = null;

    public function onSuccess(?Message $responseMessage): void
    {
        $this->successCount++;
        $this->lastResponse = $responseMessage;
    }

    public function onException(?\Throwable $cause): void
    {
        $this->exceptionCount++;
        $this->lastCause = $cause;
    }
}

/** 记录调用的 SendMessageHook。 */
final class RecordingSendHook implements SendMessageHook
{
    public int $before = 0;
    public int $after = 0;

    public function hookName(): string
    {
        return 'send-hook';
    }

    public function sendMessageBefore(SendMessageContext $context): void
    {
        $this->before++;
    }

    public function sendMessageAfter(SendMessageContext $context): void
    {
        $this->after++;
    }
}

/** 记录调用的 ConsumeMessageHook。 */
final class RecordingConsumeHook implements ConsumeMessageHook
{
    public int $before = 0;
    public int $after = 0;

    public function hookName(): string
    {
        return 'consume-hook';
    }

    public function consumeMessageBefore(ConsumeMessageContext $context): void
    {
        $this->before++;
    }

    public function consumeMessageAfter(ConsumeMessageContext $context): void
    {
        $this->after++;
    }
}

/** 记录调用的 EndTransactionHook。 */
final class RecordingEndTxHook implements EndTransactionHook
{
    public int $calls = 0;

    public function hookName(): string
    {
        return 'end-tx-hook';
    }

    public function endTransaction(EndTransactionContext $context): void
    {
        $this->calls++;
    }
}

/** 记录调用的 CheckForbiddenHook。 */
final class RecordingForbiddenHook implements CheckForbiddenHook
{
    public int $calls = 0;

    public function hookName(): string
    {
        return 'forbidden-hook';
    }

    public function checkForbidden(CheckForbiddenContext $context): void
    {
        $this->calls++;
    }
}

/** 记录调用的 FilterMessageHook。 */
final class RecordingFilterHook implements FilterMessageHook
{
    public int $calls = 0;

    public function hookName(): string
    {
        return 'filter-hook';
    }

    public function filterMessage(FilterMessageContext $context): void
    {
        $this->calls++;
    }
}

// ==================================================================== runner

final class RunClientLeaf
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

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

    public function checkClose(float $expected, float $actual, float $tolerance, string $label): void
    {
        $this->check(abs($expected - $actual) <= $tolerance, sprintf('%s (expected≈%s, actual=%s)', $label, $expected, $actual));
    }

    /** @param class-string<\Throwable> $exceptionClass */
    public function checkThrows(callable $fn, string $exceptionClass, string $label): void
    {
        $e = $this->capture($fn);
        if ($e === null) {
            $this->check(false, "{$label} (no exception thrown)");
            return;
        }
        $this->check($e instanceof $exceptionClass, sprintf('%s (got %s: %s)', $label, get_class($e), $e->getMessage()));
    }

    public function capture(callable $fn): ?\Throwable
    {
        try {
            $fn();
            return null;
        } catch (\Throwable $e) {
            return $e;
        }
    }

    /** 断言抛出 MQClientException 且 responseCode / 文案符合。 */
    public function checkClientError(callable $fn, ?int $responseCode, string $msgSubstr, string $label): void
    {
        $e = $this->capture($fn);
        if (!$e instanceof MQClientException) {
            $this->check(false, sprintf('%s (got %s)', $label, $e === null ? 'no exception' : get_class($e)));
            return;
        }
        $ok = $e->responseCode === $responseCode && str_contains($e->getMessage(), $msgSubstr);
        if (!$ok) {
            $this->check(false, sprintf(
                '%s (code=%s msg=%s)',
                $label,
                var_export($e->responseCode, true),
                $e->getMessage()
            ));
            return;
        }
        $this->passed++;
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

    // ==================================================================== SendResult

    private function testSendResult(): void
    {
        $this->checkSame(0, SendStatus::SEND_OK->value, 'SendStatus.SEND_OK=0');
        $this->checkSame(1, SendStatus::FLUSH_DISK_TIMEOUT->value, 'SendStatus.FLUSH_DISK_TIMEOUT=1');
        $this->checkSame(2, SendStatus::FLUSH_SLAVE_TIMEOUT->value, 'SendStatus.FLUSH_SLAVE_TIMEOUT=2');
        $this->checkSame(3, SendStatus::SLAVE_NOT_AVAILABLE->value, 'SendStatus.SLAVE_NOT_AVAILABLE=3');

        $this->checkSame(SendStatus::SEND_OK, SendStatus::fromCode(0), 'fromCode(0)');
        $this->checkSame(SendStatus::SLAVE_NOT_AVAILABLE, SendStatus::fromCode(3), 'fromCode(3)');
        $this->checkSame(SendStatus::SEND_OK, SendStatus::fromCode(99), 'fromCode(未知)兜底 SEND_OK');
        $this->checkSame(SendStatus::SEND_OK, SendStatus::fromCode(-1), 'fromCode(-1)兜底 SEND_OK');

        $r = new SendResult();
        $this->checkSame(SendStatus::SEND_OK, $r->getSendStatus(), 'SendResult 默认 sendStatus');
        $this->checkSame(null, $r->getMsgId(), 'SendResult 默认 msgId=null');
        $this->checkSame(null, $r->getMessageQueue(), 'SendResult 默认 messageQueue=null');
        $this->checkSame(0, $r->getQueueOffset(), 'SendResult 默认 queueOffset=0');
        $this->checkSame(null, $r->getTransactionId(), 'SendResult 默认 transactionId=null');
        $this->checkSame(null, $r->getOffsetMsgId(), 'SendResult 默认 offsetMsgId=null');
        $this->checkSame(null, $r->getRegionId(), 'SendResult 默认 regionId=null');
        $this->check(true === $r->isTraceOn(), 'SendResult 默认 traceOn=true');
        $this->checkSame(null, $r->getRecallHandle(), 'SendResult 默认 recallHandle=null');

        $mq = new MessageQueue('TopicTest', 'broker-a', 3);
        $r2 = new SendResult(SendStatus::FLUSH_DISK_TIMEOUT, 'MSG-1', $mq, 17, 'TX-1', 'OFF-1', 'REG-1');
        $this->checkSame(SendStatus::FLUSH_DISK_TIMEOUT, $r2->getSendStatus(), 'SendResult ctor sendStatus');
        $this->checkSame('MSG-1', $r2->getMsgId(), 'SendResult ctor msgId');
        $this->check($r2->getMessageQueue() === $mq, 'SendResult ctor messageQueue 同引用');
        $this->checkSame(17, $r2->getQueueOffset(), 'SendResult ctor queueOffset');
        $this->checkSame('TX-1', $r2->getTransactionId(), 'SendResult ctor transactionId');
        $this->checkSame('OFF-1', $r2->getOffsetMsgId(), 'SendResult ctor offsetMsgId');
        $this->checkSame('REG-1', $r2->getRegionId(), 'SendResult ctor regionId');

        $r2->setTransactionId('TX-2');
        $this->checkSame('TX-2', $r2->getTransactionId(), 'setTransactionId');
        $r2->setRegionId('REG-2');
        $this->checkSame('REG-2', $r2->getRegionId(), 'setRegionId');
        $r2->setRecallHandle('RH');
        $this->checkSame('RH', $r2->getRecallHandle(), 'setRecallHandle');
        $r2->setTraceOn(false);
        $this->check(false === $r2->isTraceOn(), 'setTraceOn(false)');
        // 参数类型为 ?string，传 null 合法（Java 字段可空同此）：清空 transactionId
        $this->check($this->capture(static fn() => $r2->setTransactionId(null)) === null, 'setTransactionId(null) 不抛（?string 合法）');
        $this->checkSame(null, $r2->getTransactionId(), 'setTransactionId(null) 清空字段');
        $this->check(str_contains((string) $r2, 'SendResult [sendStatus='), '__toString 形态');
        $this->check(str_contains((string) $r2, 'MSG-1'), '__toString 含 msgId');
    }

    // ==================================================================== ConsumerResult

    private function testConsumerResult(): void
    {
        // 枚举取值
        $this->checkSame(0, PullStatus::FOUND->value, 'PullStatus.FOUND=0');
        $this->checkSame(3, PullStatus::OFFSET_ILLEGAL->value, 'PullStatus.OFFSET_ILLEGAL=3');
        $this->checkSame(PullStatus::NO_MATCHED_MSG, PullStatus::fromCode(2), 'PullStatus.fromCode(2)');
        $this->checkSame(PullStatus::FOUND, PullStatus::fromCode(88), 'PullStatus.fromCode(未知)兜底');

        $this->checkSame(0, ConsumeReturnType::SUCCESS->value, 'ConsumeReturnType.SUCCESS=0');
        $this->checkSame(1, ConsumeReturnType::TIME_OUT->value, 'ConsumeReturnType.TIME_OUT=1');
        $this->checkSame(2, ConsumeReturnType::EXCEPTION->value, 'ConsumeReturnType.EXCEPTION=2');
        $this->checkSame(3, ConsumeReturnType::RETURNNULL->value, 'ConsumeReturnType.RETURNNULL=3');
        $this->checkSame(4, ConsumeReturnType::FAILED->value, 'ConsumeReturnType.FAILED=4');

        $this->checkSame(0, PopStatus::FOUND->value, 'PopStatus.FOUND=0');
        $this->checkSame(3, PopStatus::POLLING_NOT_FOUND->value, 'PopStatus.POLLING_NOT_FOUND=3');

        $this->checkSame(0, ConsumeConcurrentlyStatus::CONSUME_SUCCESS->value, 'ConsumeConcurrentlyStatus.CONSUME_SUCCESS=0');
        $this->checkSame(1, ConsumeConcurrentlyStatus::RECONSUME_LATER->value, 'ConsumeConcurrentlyStatus.RECONSUME_LATER=1');

        $this->checkSame(0, ConsumeOrderlyStatus::SUCCESS->value, 'ConsumeOrderlyStatus.SUCCESS=0');
        $this->checkSame(1, ConsumeOrderlyStatus::ROLLBACK->value, 'ConsumeOrderlyStatus.ROLLBACK=1');
        $this->checkSame(2, ConsumeOrderlyStatus::COMMIT->value, 'ConsumeOrderlyStatus.COMMIT=2');
        $this->checkSame(3, ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT->value, 'ConsumeOrderlyStatus.SUSPEND=3');

        // PullResult
        $p = new PullResult(PullStatus::NO_NEW_MSG);
        $this->checkSame(PullStatus::NO_NEW_MSG, $p->status, 'PullResult.status');
        $this->checkSame(0, $p->nextBeginOffset, 'PullResult 默认 nextBeginOffset');
        $this->checkSame(0, $p->minOffset, 'PullResult 默认 minOffset');
        $this->checkSame(0, $p->maxOffset, 'PullResult 默认 maxOffset');
        $this->checkSame([], $p->msgFoundList, 'PullResult 默认 msgFoundList=[]');
        $this->checkSame(null, $p->suggestWhichBrokerId, 'PullResult 默认 suggestWhichBrokerId=null');
        $msgs = [(new MessageExt())];
        $p2 = new PullResult(PullStatus::FOUND, 100, 5, 200, $msgs, 1);
        $this->checkSame(100, $p2->nextBeginOffset, 'PullResult ctor nextBeginOffset');
        $this->checkSame(1, $p2->suggestWhichBrokerId, 'PullResult ctor suggestWhichBrokerId');
        $this->checkSame(1, count($p2->msgFoundList), 'PullResult ctor msgFoundList');
        $this->check(str_contains((string) $p2, 'PullStatus.FOUND'), 'PullResult __toString');

        // PopResult
        $pop = new PopResult(PopStatus::POLLING_FULL);
        $this->checkSame(PopStatus::POLLING_FULL, $pop->status, 'PopResult.status');
        $this->checkSame([], $pop->msgFoundList, 'PopResult 默认 msgFoundList=[]');
        $this->checkSame(0, $pop->restNum, 'PopResult 默认 restNum');
        $this->checkSame(null, $pop->startOffsetInfo, 'PopResult 默认 startOffsetInfo');
        $pop2 = new PopResult(PopStatus::FOUND, $msgs, 3, 111, 222, 999, 'so', 'mo', 'oc');
        $this->checkSame(3, $pop2->restNum, 'PopResult ctor restNum');
        $this->checkSame(111, $pop2->popTime, 'PopResult ctor popTime');
        $this->checkSame(999, $pop2->reviveQid, 'PopResult ctor reviveQid');
        $this->checkSame('oc', $pop2->orderCountInfo, 'PopResult ctor orderCountInfo');

        // ChangeInvisibleTimeResult
        $ok = new ChangeInvisibleTimeResult(0, 10, 20, 999, 'ck');
        $this->check(true === $ok->success, 'ChangeInvisibleTimeResult responseCode=0 → success=true');
        $this->checkSame('ck', $ok->extraInfo, 'ChangeInvisibleTimeResult extraInfo');
        $bad = new ChangeInvisibleTimeResult(21);
        $this->check(false === $bad->success, 'ChangeInvisibleTimeResult responseCode!=0 → success=false');
        $this->checkSame(0, $bad->popTime, 'ChangeInvisibleTimeResult 默认 popTime');

        // 上下文
        $cc = new ConsumeConcurrentlyContext();
        $this->checkSame(null, $cc->messageQueue, 'ConsumeConcurrentlyContext 默认 mq=null');
        $this->checkSame(0, $cc->delayLevelWhenNextConsume, 'ConsumeConcurrentlyContext 默认 delay=0');
        $this->checkSame((1 << 31) - 1, $cc->ackIndex, 'ConsumeConcurrentlyContext 默认 ackIndex=Integer.MAX_VALUE');
        $cc2 = new ConsumeConcurrentlyContext($mq = new MessageQueue('T', 'b', 0));
        $this->check($cc2->messageQueue === $mq, 'ConsumeConcurrentlyContext ctor mq');

        $oc = new ConsumeOrderlyContext();
        $this->check(true === $oc->autoCommit, 'ConsumeOrderlyContext 默认 autoCommit=true');
        $this->checkSame(-1, $oc->suspendCurrentQueueTimeMillis, 'ConsumeOrderlyContext 默认 suspend=-1');
        $this->checkSame(null, $oc->messageQueue, 'ConsumeOrderlyContext 默认 mq=null');

        // consumeStatusName（Java status.toString() 形态 = 裸枚举成员名）
        $this->checkSame('CONSUME_SUCCESS', consumeStatusName(ConsumeConcurrentlyStatus::CONSUME_SUCCESS), 'consumeStatusName 并发成功');
        $this->checkSame('RECONSUME_LATER', consumeStatusName(ConsumeConcurrentlyStatus::RECONSUME_LATER), 'consumeStatusName 并发重投');
        $this->checkSame('SUSPEND_CURRENT_QUEUE_A_MOMENT', consumeStatusName(ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT), 'consumeStatusName 顺序挂起');
        $this->checkSame('None', consumeStatusName(null), 'consumeStatusName null 兜底');
        $this->checkSame('raw', consumeStatusName('raw'), 'consumeStatusName 非枚举兜底 str');

        // 监听器契约（interface）
        $listener = new class implements MessageListenerConcurrently {
            public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
            {
                return ConsumeConcurrentlyStatus::RECONSUME_LATER;
            }
        };
        $this->check($listener instanceof MessageListenerConcurrently, '监听器 implements MessageListenerConcurrently');
        $this->checkSame(
            ConsumeConcurrentlyStatus::RECONSUME_LATER,
            $listener->consumeMessage([], new ConsumeConcurrentlyContext()),
            '监听器 consumeMessage 返回状态'
        );
    }

    // ==================================================================== TopAddressing

    private function testTopAddressing(): void
    {
        $this->checkSame('jmenv.tbsite.net', DefaultTopAddressing::DEFAULT_NAMESRV_ADDR_LOOKUP, 'DEFAULT_NAMESRV_ADDR_LOOKUP');
        $this->checkSame('nsaddr', DefaultTopAddressing::DEFAULT_DOMAIN_SUBGROUP, 'DEFAULT_DOMAIN_SUBGROUP');

        // clearNewLine
        $this->checkSame('abc', DefaultTopAddressing::clearNewLine("  abc  "), 'clearNewLine trim');
        $this->checkSame('abc', DefaultTopAddressing::clearNewLine("abc\r\ndef"), 'clearNewLine 截断 \\r');
        $this->checkSame('abc', DefaultTopAddressing::clearNewLine("abc\ndef"), 'clearNewLine 截断 \\n');
        $this->checkSame('abc', DefaultTopAddressing::clearNewLine('abc'), 'clearNewLine 无换行');

        // getWsAddr
        $this->checkSame('http://jmenv.tbsite.net:8080/rocketmq/nsaddr', DefaultTopAddressing::getWsAddr('jmenv.tbsite.net'), 'getWsAddr 默认端口');
        $this->checkSame('http://host:9999/rocketmq/nsaddr', DefaultTopAddressing::getWsAddr('host:9999'), 'getWsAddr 自带端口不追加');
        $this->checkSame('http://d:8080/rocketmq/sub', DefaultTopAddressing::getWsAddr('d', 'sub'), 'getWsAddr 指定 subgroup');
        $this->checkSame('http://d:8080/rocketmq/nsaddr', DefaultTopAddressing::getWsAddr('d', ''), 'getWsAddr 空 subgroup 回落默认');

        // buildUrl
        $this->checkSame('http://mock/rocketmq/nsaddr', (new MockTopAddressing([]))->buildUrl(), 'buildUrl 无 unit/para');
        $this->checkSame('http://mock/rocketmq/nsaddr-u?nofix=1', (new MockTopAddressing([], 'http://mock/rocketmq/nsaddr', 'u'))->buildUrl(), 'buildUrl 仅 unit');
        $this->checkSame('http://mock/rocketmq/nsaddr?k=v', (new MockTopAddressing([], 'http://mock/rocketmq/nsaddr', '', ['k' => 'v']))->buildUrl(), 'buildUrl 仅 para');
        $this->checkSame('http://mock/rocketmq/nsaddr-u?nofix=1&k=v&k2=v2', (new MockTopAddressing([], 'http://mock/rocketmq/nsaddr', 'u', ['k' => 'v', 'k2' => 'v2']))->buildUrl(), 'buildUrl unit+para');
        $this->checkSame('http://mock/rocketmq/nsaddr?k=v', (new MockTopAddressing([], 'http://mock/rocketmq/nsaddr', '   ', ['k' => 'v']))->buildUrl(), 'buildUrl 空白 unit 视为空');

        // 构造：domain / 环境变量
        putenv('ROCKETMQ_NAMESRV_DOMAIN=');
        $this->checkSame('', (new DefaultTopAddressing())->wsAddr, '未配置 domain → wsAddr 空');
        $this->check(false === DefaultTopAddressing::isConfigured(), 'isConfigured 未配置 false');
        putenv('ROCKETMQ_NAMESRV_DOMAIN=envdomain');
        $this->check(true === DefaultTopAddressing::isConfigured(), 'isConfigured 已配置 true');
        $this->checkSame('http://envdomain:8080/rocketmq/nsaddr', (new DefaultTopAddressing())->wsAddr, 'env domain 生效');
        putenv('ROCKETMQ_NAMESRV_DOMAIN=');
        $this->checkSame('http://explicit:8080/rocketmq/nsaddr', (new DefaultTopAddressing(null, '', null, 3000, 'explicit'))->wsAddr, '显式 domain 优先于 env');
        $this->checkSame('http://given/rocketmq/nsaddr', (new DefaultTopAddressing('http://given/rocketmq/nsaddr'))->wsAddr, 'ws_addr 直给');

        // fetchNsAddr
        $this->checkSame(null, (new DefaultTopAddressing(''))->fetchNsAddr(), 'wsAddr 空 → fetchNsAddr null');
        $mock = new MockTopAddressing(["addr1:9876\r\nrest"], 'http://mock/ns');
        $this->checkSame('addr1:9876', $mock->fetchNsAddr(), 'fetchNsAddr 截断换行');
        $this->checkSame('http://mock/ns', $mock->seenUrls[0], 'fetchNsAddr 使用 buildUrl');

        $mock2 = new MockTopAddressing([new \RuntimeException('boom')], 'http://mock/ns');
        $this->checkSame(null, $mock2->fetchNsAddr(false), 'fetchNsAddr 异常静默 → null');
        $this->checkSame(null, $mock2->fetchNsAddr(true), 'fetchNsAddr 异常 verbose → null');

        // fetchAndApply：地址变化才应用
        $mock3 = new MockTopAddressing(['A;B', 'A;B', 'C'], 'http://mock/ns');
        $this->checkSame('A;B', $mock3->fetchAndApply(), 'fetchAndApply 首次应用');
        $this->checkSame('A;B', $mock3->nsAddr, 'fetchAndApply 缓存 nsAddr');
        $this->checkSame(null, $mock3->fetchAndApply(), 'fetchAndApply 相同 → null');
        $this->checkSame('C', $mock3->fetchAndApply(), 'fetchAndApply 变化 → 应用新值');
        $this->checkSame(null, (new MockTopAddressing(['   '], 'http://mock/ns'))->fetchAndApply(), 'fetchAndApply 空白地址不应用');
    }

    // ==================================================================== Latency

    private function testLatency(): void
    {
        // FaultItem
        $fi = new FaultItem('b1');
        $this->checkSame('b1', $fi->name, 'FaultItem.name');
        $this->checkSame(0.0, $fi->currentLatency, 'FaultItem 默认 currentLatency');
        $this->checkSame(0.0, $fi->startTimestamp, 'FaultItem 默认 startTimestamp');
        $this->check(true === $fi->reachableFlag, 'FaultItem 默认 reachable');
        $this->check(true === $fi->isAvailable(), 'FaultItem 默认 available');
        $this->check(true === $fi->isReachable(), 'FaultItem 默认 reachable');
        $fi->updateNotAvailableDuration(5000.0);
        $this->check($fi->startTimestamp > FaultItem::nowMillis(), 'FaultItem 隔离期起点在未来');
        $this->check(false === $fi->isAvailable(), 'FaultItem 隔离期内不可用');
        $before = $fi->startTimestamp;
        $fi->updateNotAvailableDuration(0.0);
        $this->checkSame($before, $fi->startTimestamp, 'updateNotAvailableDuration(0) 不改变');
        $fi->reachableFlag = false;
        $this->check(false === $fi->isReachable(), 'FaultItem reachableFlag=false');

        // FaultItem::__toString
        $fiStr = new FaultItem('bx');
        $fiStr->currentLatency = 12.0;
        $fiStr->reachableFlag = false;
        $fiStrText = (string) $fiStr;
        $this->check(str_contains($fiStrText, 'FaultItem{name=bx') && str_contains($fiStrText, 'reachable=False'), 'FaultItem __toString 形态');

        // LatencyFaultToleranceImpl
        $tol = new LatencyFaultToleranceImpl();
        $this->check(true === $tol->isAvailable('none'), '无记录 → isAvailable true');
        $this->check(true === $tol->isReachable('none'), '无记录 → isReachable true');
        $this->checkSame(null, $tol->getFaultItem('none'), '无记录 → getFaultItem null');
        $tol->updateFaultItem('b2', 12.0, 0.0, false);
        $item = $tol->getFaultItem('b2');
        $this->check($item instanceof FaultItem, 'updateFaultItem 建项');
        $this->checkSame(12.0, $item->currentLatency, 'updateFaultItem 写 latency');
        $this->check(false === $tol->isReachable('b2'), 'updateFaultItem 写 reachable=false');
        $this->check(true === $tol->isAvailable('b2'), 'dur=0 → 仍可用');
        $tol->updateFaultItem('b2', 99.0, 5000.0, true);
        $this->checkSame(99.0, $tol->getFaultItem('b2')->currentLatency, 'updateFaultItem 更新 latency');
        $this->check(true === $tol->isReachable('b2'), 'updateFaultItem 更新 reachable=true');
        $this->check(false === $tol->isAvailable('b2'), 'dur>0 → 不可用');
        $tol->remove('b2');
        $this->checkSame(null, $tol->getFaultItem('b2'), 'remove 摘除');
        $this->check(true === $tol->isAvailable('b2'), 'remove 后恢复可用');

        // MQFaultStrategy 常量与配置
        $this->checkSame([50, 100, 550, 1800, 3000, 5000, 15000], MQFaultStrategy::LATENCY_MAX, 'LATENCY_MAX 表');
        $this->checkSame([0, 0, 2000, 5000, 6000, 10000, 30000], MQFaultStrategy::NOT_AVAILABLE_DURATION, 'NOT_AVAILABLE_DURATION 表');
        $st = new MQFaultStrategy();
        $this->check(false === $st->isSendLatencyFaultEnable(), '默认关闭');
        $this->checkSame([50, 100, 550, 1800, 3000, 5000, 15000], $st->latencyMax, 'latencyMax 副本');
        $this->checkSame([0, 0, 2000, 5000, 6000, 10000, 30000], $st->notAvailableDuration, 'notAvailableDuration 副本');
        $st->setSendLatencyFaultEnable(true);
        $this->check(true === $st->isSendLatencyFaultEnable(), 'setSendLatencyFaultEnable(true)');
        $this->check($st->getLatencyFaultTolerance() instanceof LatencyFaultToleranceImpl, 'latencyFaultTolerance getter');

        // 关闭时 updateFaultItem 为 no-op
        $off = new MQFaultStrategy(false);
        $off->updateFaultItem('b', 99999.0, true, true);
        $this->checkSame(null, $off->getLatencyFaultTolerance()->getFaultItem('b'), '关闭时 updateFaultItem no-op');

        // computeNotAvailableDuration 档位（隔离场景固定按 10000ms 算档）
        // 注意：isolation=true 会把 latency 强制为 10000 → dur=10000，必然被隔离；
        //       所以「未隔离」档位必须走 isolation=false 直接把真实 latency 喂给分档表。
        $cases = [
            [0.0, false, 0.0],      // 0 < 50 → 无档位
            [49.0, false, 0.0],     // < 50 → 无档位
            [50.0, false, 0.0],     // idx0 → 0
            [100.0, false, 0.0],    // idx1 → 0
            [550.0, false, 2000.0], // idx2
            [1800.0, false, 5000.0],// idx3
            [3000.0, false, 6000.0],// idx4
            [5000.0, false, 10000.0],// idx5
            [15000.0, false, 30000.0],// idx6
            [999999.0, false, 30000.0],
            [1.0, true, 10000.0],   // isolation → latency=10000 → idx5 → 10000
        ];
        foreach ($cases as $i => [$latency, $isolation, $expectedDuration]) {
            $s = new MQFaultStrategy(true);
            $s->updateFaultItem('b', $latency, $isolation, true);
            $item = $s->getLatencyFaultTolerance()->getFaultItem('b');
            $this->check($item !== null, "档位[$i] 建项");
            if ($expectedDuration > 0) {
                $this->check($item->startTimestamp > FaultItem::nowMillis(), "档位[$i] 被隔离");
                $this->check(false === $s->getLatencyFaultTolerance()->isAvailable('b'), "档位[$i] 不可用");
            } else {
                $this->checkSame(0.0, $item->startTimestamp, "档位[$i] 未隔离");
                $this->check(true === $s->getLatencyFaultTolerance()->isAvailable('b'), "档位[$i] 可用");
            }
        }

        // 选队列：关闭时按 broker_filter
        $q1 = new MessageQueue('T', 'b1', 0);
        $q2 = new MessageQueue('T', 'b2', 0);
        $tp = new FakeTopicPublishInfo([$q1, $q2]);
        $off2 = new MQFaultStrategy(false);
        $this->checkSame('b1', $off2->selectOneMessageQueue($tp, null)->brokerName, '关闭时选队首');
        $this->checkSame('b2', $off2->selectOneMessageQueue($tp, 'b1')->brokerName, '关闭时按 lastBrokerName 过滤');
        $tp->queues = [$q1];
        $this->checkSame('b1', $off2->selectOneMessageQueue($tp, 'b1')->brokerName, '关闭时过滤不中回退普通轮询');

        // 选队列：开启时优先 available
        $tp2 = new FakeTopicPublishInfo([$q1, $q2]);
        $on = new MQFaultStrategy(true);
        // 隔离 b1（不可用），b2 仍可用
        $on->updateFaultItem('b1', 10000.0, false, true);
        $on->updateFaultItem('b2', 1.0, false, true);
        $picked = $on->selectOneMessageQueue($tp2, null, true);
        $this->checkSame('b2', $picked->brokerName, '开启时跳过不可用 broker');
        $this->checkSame(1, $tp2->resetCount, 'resetIndex=true 调用 resetIndex');

        // 都不可用 → 退化到 reachable（b1 reachable=true, b2 reachable=false）
        $tp3 = new FakeTopicPublishInfo([$q1, $q2]);
        $on2 = new MQFaultStrategy(true);
        $on2->updateFaultItem('b1', 10000.0, false, true);  // 不可用，reachable
        $on2->updateFaultItem('b2', 10000.0, false, false); // 不可用，不可达
        $this->checkSame('b1', $on2->selectOneMessageQueue($tp3, null)->brokerName, '都不可用时退化到 reachable');

        // 都不可用且都不可达 → 退化普通轮询
        $tp4 = new FakeTopicPublishInfo([$q1, $q2]);
        $on3 = new MQFaultStrategy(true);
        $on3->updateFaultItem('b1', 10000.0, false, false);
        $on3->updateFaultItem('b2', 10000.0, false, false);
        $picked4 = $on3->selectOneMessageQueue($tp4, null);
        $this->check($picked4 !== null, '都不可达 → 普通轮询兜底非 null');
    }

    // ==================================================================== Metrics

    private function testMetrics(): void
    {
        $m = new ClientMetrics();
        $snap = $m->snapshot();
        $this->checkSame(12, count($snap), 'snapshot 12 个键');
        $this->checkSame(0, $snap['sendCount'], '初始 sendCount=0');
        $this->checkSame(0, $snap['sendFailureCount'], '初始 sendFailureCount=0');
        $this->checkSame(0.0, $snap['sendRTAvg'], '初始 sendRTAvg=0.0');
        $this->checkSame(0.0, $snap['sendRTSum'], '初始 sendRTSum=0.0');
        $this->checkSame(0, $snap['consumeCount'], '初始 consumeCount=0');
        $this->checkSame(0.0, $snap['consumeRTAvg'], '初始 consumeRTAvg=0.0');

        $start = $m->recordSendStart();
        $this->check(is_float($start) && $start > 0, 'recordSendStart 返回毫秒时间戳');

        $base = microtime(true) * 1000.0;
        $m->recordSendSuccess($base - 10.0);
        $m->recordSendSuccess($base - 20.0);
        $snap = $m->snapshot();
        $this->checkSame(2, $snap['sendCount'], '两次成功 sendCount=2');
        $this->checkSame(0, $snap['sendFailureCount'], '无失败 sendFailureCount=0');
        $this->checkClose(20.0, $snap['sendRTMax'], 5.0, 'sendRTMax≈20');
        $this->checkClose(10.0, $snap['sendRTMin'], 5.0, 'sendRTMin≈10');
        $this->checkClose(30.0, $snap['sendRTSum'], 8.0, 'sendRTSum≈30');
        $this->checkClose(15.0, $snap['sendRTAvg'], 5.0, 'sendRTAvg≈15');

        $m->recordSendFailure($base - 5.0);
        $snap = $m->snapshot();
        $this->checkSame(1, $snap['sendFailureCount'], '失败计入 sendFailureCount');
        $this->checkSame(2, $snap['sendCount'], '失败不计入 sendCount');
        $this->checkClose(5.0, $snap['sendRTMin'], 5.0, '失败也更新 RTMin');
        $this->checkClose(20.0, $snap['sendRTMax'], 5.0, '失败未抬升 RTMax');

        $m->recordConsumeSuccess($base - 7.0);
        $m->recordConsumeFailure($base - 3.0);
        $snap = $m->snapshot();
        $this->checkSame(1, $snap['consumeCount'], 'consumeCount=1');
        $this->checkSame(1, $snap['consumeFailureCount'], 'consumeFailureCount=1');
        $this->checkClose(7.0, $snap['consumeRTMax'], 5.0, 'consumeRTMax≈7');
        $this->checkClose(3.0, $snap['consumeRTMin'], 5.0, 'consumeRTMin≈3');
        $this->checkClose(10.0, $snap['consumeRTSum'], 8.0, 'consumeRTSum≈10');
        // 失败不计入 consumeCount（与 Python/Java 一致）：sum≈10 / count=1 → avg≈10
        $this->checkClose(10.0, $snap['consumeRTAvg'], 5.0, 'consumeRTAvg≈10');

        $this->check($m->recordConsumeStart() > 0, 'recordConsumeStart 返回毫秒时间戳');

        // ClientMetrics::__toString（snapshot 的 JSON 形态）
        $this->check(str_contains((string) $m, 'ClientMetrics{"sendCount"'), 'ClientMetrics __toString 形态');
    }

    // ==================================================================== Backpressure

    private function testBackpressure(): void
    {
        $this->checkSame(10, FairSemaphore::MIN_ASYNC_SEND_NUM, 'MIN_ASYNC_SEND_NUM=10');
        $this->checkSame(1024 * 1024, FairSemaphore::MIN_ASYNC_SEND_SIZE, 'MIN_ASYNC_SEND_SIZE=1M');

        // Pending（_Pending 等价类）
        $pend = new Pending(7);
        $this->checkSame(7, $pend->permits, 'Pending.permits');

        $s = new FairSemaphore(5);
        $this->checkSame(5, $s->totalPermits(), 'totalPermits 初始');
        $this->checkSame(5, $s->availablePermits(), 'availablePermits 初始');

        $this->check(true === $s->tryAcquire(3, 1000), 'tryAcquire(3) 成功');
        $this->checkSame(2, $s->availablePermits(), '拿 3 个后剩 2');
        $this->check(false === $s->tryAcquire(3, 1000), '额度不足 → 拒绝');
        $this->checkSame(2, $s->availablePermits(), '拒绝后额度不变');
        $this->check(true === $s->tryAcquire(2, 0), 'tryAcquire(2) 正好用尽');
        $this->checkSame(0, $s->availablePermits(), '用尽后剩 0');
        $this->check(false === $s->tryAcquire(1, 0), '无额度 → 拒绝');
        $this->check(true === $s->tryAcquire(0, 0), '申请 0 个恒成功');

        $s->release(0);
        $this->checkSame(0, $s->availablePermits(), 'release(0) 无操作');
        $s->release(-5);
        $this->checkSame(0, $s->availablePermits(), 'release(负数) 无操作');
        $s->release(2);
        $this->checkSame(2, $s->availablePermits(), 'release(2) 归还');
        $s->release(10);
        $this->checkSame(12, $s->availablePermits(), 'release 可超过总量');

        // setTotalPermits 平移（在途份数原样保留）
        $s2 = new FairSemaphore(5);
        $s2->tryAcquire(2, 0);              // 在途 2，free 3
        $s2->setTotalPermits(3);            // free += 3-5 = -2 → 1
        $this->checkSame(3, $s2->totalPermits(), 'setTotalPermits 更新总量');
        $this->checkSame(1, $s2->availablePermits(), 'setTotalPermits 平移空闲额度');
        $this->check(false === $s2->tryAcquire(2, 0), '缩容后额度不足拒绝');
        $this->check(true === $s2->tryAcquire(1, 0), '缩容后剩余额度可用');

        // 负空闲额度（Java new Semaphore(负数) 同样接受）
        $s3 = new FairSemaphore(2);
        $s3->tryAcquire(2, 0);              // free 0
        $s3->setTotalPermits(0);            // free += 0-2 = -2
        $this->checkSame(-2, $s3->availablePermits(), 'setTotalPermits 得负空闲额度');
        $s3->release(3);
        $this->checkSame(1, $s3->availablePermits(), '归还把负额度拉回正数');
    }

    // ==================================================================== Hook

    private function testHook(): void
    {
        $this->checkSame('SYNC', CommunicationMode::SYNC, 'CommunicationMode.SYNC');
        $this->checkSame('ASYNC', CommunicationMode::ASYNC, 'CommunicationMode.ASYNC');
        $this->checkSame('ONEWAY', CommunicationMode::ONEWAY, 'CommunicationMode.ONEWAY');

        $sc = new SendMessageContext();
        $this->checkSame(null, $sc->producer, 'SendMessageContext.producer 默认 null');
        $this->checkSame('', $sc->producerGroup, 'SendMessageContext.producerGroup 默认空');
        $this->checkSame(null, $sc->message, 'SendMessageContext.message 默认 null');
        $this->checkSame(null, $sc->mq, 'SendMessageContext.mq 默认 null');
        $this->checkSame('', $sc->brokerAddr, 'SendMessageContext.brokerAddr 默认空');
        $this->checkSame(null, $sc->communicationMode, 'SendMessageContext.communicationMode 默认 null');
        $this->checkSame(null, $sc->sendResult, 'SendMessageContext.sendResult 默认 null');
        $this->checkSame(null, $sc->exception, 'SendMessageContext.exception 默认 null');
        $this->checkSame(null, $sc->mqTraceContext, 'SendMessageContext.mqTraceContext 默认 null');
        $this->checkSame(MessageType::NORMAL_MSG, $sc->msgType, 'SendMessageContext.msgType 默认 NORMAL_MSG');
        $this->checkSame('', $sc->namespace, 'SendMessageContext.namespace 默认空');

        $cm = new ConsumeMessageContext();
        $this->checkSame('', $cm->consumerGroup, 'ConsumeMessageContext 默认 group 空');
        $this->checkSame([], $cm->msgList, 'ConsumeMessageContext 默认 msgList=[]');
        $this->check(true === $cm->success, 'ConsumeMessageContext 默认 success=true');
        $this->checkSame(null, $cm->status, 'ConsumeMessageContext 默认 status=null');
        $msg = new MessageExt();
        $mq = new MessageQueue('T', 'b', 0);
        $cm2 = new ConsumeMessageContext('G', [$msg], $mq);
        $this->checkSame('G', $cm2->consumerGroup, 'ConsumeMessageContext ctor group');
        $this->checkSame(1, count($cm2->msgList), 'ConsumeMessageContext ctor msgList');
        $this->check($cm2->mq === $mq, 'ConsumeMessageContext ctor mq');

        $et = new EndTransactionContext();
        $this->checkSame('', $et->producerGroup, 'EndTransactionContext.producerGroup 默认空');
        $this->checkSame(null, $et->msgId, 'EndTransactionContext.msgId 默认 null');
        $this->check(false === $et->fromTransactionCheck, 'EndTransactionContext.fromTransactionCheck 默认 false');

        $cf = new CheckForbiddenContext();
        $this->checkSame('', $cf->nameSrvAddr, 'CheckForbiddenContext.nameSrvAddr 默认空');
        $this->checkSame(null, $cf->arg, 'CheckForbiddenContext.arg 默认 null');
        $this->check(false === $cf->unitMode, 'CheckForbiddenContext.unitMode 默认 false');
        $this->checkSame(null, $cf->sendResult, 'CheckForbiddenContext 无 sendResult');

        $fm = new FilterMessageContext();
        $this->checkSame([], $fm->msgList, 'FilterMessageContext 默认 msgList=[]');
        $fm2 = new FilterMessageContext('G', [$msg], $mq);
        $this->checkSame('G', $fm2->consumerGroup, 'FilterMessageContext ctor group');
        $this->checkSame(1, count($fm2->msgList), 'FilterMessageContext ctor msgList');
        $this->check(false === $fm2->unitMode, 'FilterMessageContext.unitMode 默认 false');

        // 钩子接口可用
        $sh = new RecordingSendHook();
        $sh->sendMessageBefore($sc);
        $sh->sendMessageAfter($sc);
        $this->check($sh instanceof SendMessageHook && $sh->before === 1 && $sh->after === 1, 'SendMessageHook 接口');
        $this->checkSame('send-hook', $sh->hookName(), 'SendMessageHook.hookName');

        $ch = new RecordingConsumeHook();
        $ch->consumeMessageBefore($cm);
        $ch->consumeMessageAfter($cm);
        $this->check($ch instanceof ConsumeMessageHook && $ch->before === 1 && $ch->after === 1, 'ConsumeMessageHook 接口');

        $eh = new RecordingEndTxHook();
        $eh->endTransaction($et);
        $this->check($eh instanceof EndTransactionHook && $eh->calls === 1, 'EndTransactionHook 接口');

        $fh = new RecordingForbiddenHook();
        $fh->checkForbidden($cf);
        $this->check($fh instanceof CheckForbiddenHook && $fh->calls === 1, 'CheckForbiddenHook 接口');

        $flh = new RecordingFilterHook();
        $flh->filterMessage($fm);
        $this->check($flh instanceof FilterMessageHook && $flh->calls === 1, 'FilterMessageHook 接口');
    }

    // ==================================================================== Validators

    private function testValidators(): void
    {
        $this->checkSame(255, Validators::CHARACTER_MAX_LENGTH, 'CHARACTER_MAX_LENGTH=255');
        $this->checkSame(DIRECTORY_SEPARATOR, Validators::FILE_SEPARATOR, 'FILE_SEPARATOR=DIRECTORY_SEPARATOR');

        // checkGroup
        $this->check($this->capture(static fn() => Validators::checkGroup('GID_valid-1')) === null, 'checkGroup 合法不抛');
        $this->checkClientError(static fn() => Validators::checkGroup(null), null, 'the specified group is blank', 'checkGroup null');
        $this->checkClientError(static fn() => Validators::checkGroup(''), null, 'the specified group is blank', 'checkGroup 空串');
        $this->checkClientError(static fn() => Validators::checkGroup('   '), null, 'the specified group is blank', 'checkGroup 空白');
        $this->checkClientError(static fn() => Validators::checkGroup(str_repeat('a', 121)), null, 'longer than group max length: 120', 'checkGroup 超长(121)');
        $this->check($this->capture(static fn() => Validators::checkGroup(str_repeat('a', 120))) === null, 'checkGroup 长度 120 合法');
        $this->checkClientError(static fn() => Validators::checkGroup('gid$bad'), null, 'contains illegal characters', 'checkGroup 非法字符');
        $this->checkClientError(static fn() => Validators::checkGroup('组'), null, 'contains illegal characters', 'checkGroup 中文非法字符');

        // checkTopic
        $this->check($this->capture(static fn() => Validators::checkTopic('TopicTest_1')) === null, 'checkTopic 合法不抛');
        $this->checkClientError(static fn() => Validators::checkTopic(null), null, 'The specified topic is blank', 'checkTopic null');
        $this->checkClientError(static fn() => Validators::checkTopic('  '), null, 'The specified topic is blank', 'checkTopic 空白');
        $this->checkClientError(static fn() => Validators::checkTopic(str_repeat('a', 128)), null, 'longer than topic max length 127', 'checkTopic 超长(128)');
        $this->check($this->capture(static fn() => Validators::checkTopic(str_repeat('a', 127))) === null, 'checkTopic 长度 127 合法');
        $this->checkClientError(static fn() => Validators::checkTopic('topic$bad'), null, 'contains illegal characters', 'checkTopic 非法字符');
        // 中文字符长度按 UTF-8 码点计数（Python len() 语义）：
        // 60 个中文 = 60 码点 ≤ 127 → 走字符表分支；若按字节(180)会误判超长
        $this->checkClientError(static fn() => Validators::checkTopic(str_repeat('中', 60)), null, 'contains illegal characters', 'checkTopic 60 中文按码点不超长');
        $this->checkClientError(static fn() => Validators::checkTopic(str_repeat('中', 130)), null, 'longer than topic max length 127', 'checkTopic 130 中文按码点超长');

        // isSystemTopic
        $this->check($this->capture(static fn() => Validators::isSystemTopic('NormalTopic')) === null, 'isSystemTopic 普通不抛');
        $this->checkClientError(static fn() => Validators::isSystemTopic('TBW102'), null, 'is conflict with system topic', 'isSystemTopic TBW102');
        $this->checkClientError(static fn() => Validators::isSystemTopic('rmq_sys_x'), null, 'is conflict with system topic', 'isSystemTopic 前缀');

        // isNotAllowedSendTopic
        $this->check($this->capture(static fn() => Validators::isNotAllowedSendTopic('NormalTopic')) === null, 'isNotAllowedSendTopic 普通不抛');
        $this->check($this->capture(static fn() => Validators::isNotAllowedSendTopic('%RETRY%GID')) === null, 'isNotAllowedSendTopic 放行 %RETRY%');
        $this->checkClientError(static fn() => Validators::isNotAllowedSendTopic('SCHEDULE_TOPIC_XXXX'), null, 'is forbidden', 'isNotAllowedSendTopic 禁发');

        // checkMessage
        $this->checkClientError(static fn() => Validators::checkMessage(null, 100), ResponseCode::MESSAGE_ILLEGAL, 'the message is null', 'checkMessage null');
        $this->check($this->capture(static fn() => Validators::checkMessage(new Message('TopicTest', 'body'), 100)) === null, 'checkMessage 合法不抛');
        $this->checkClientError(
            static fn() => Validators::checkMessage(new Message('', 'body'), 100),
            null,
            'The specified topic is blank',
            'checkMessage topic 空白'
        );
        $this->checkClientError(
            static fn() => Validators::checkMessage(new Message('SCHEDULE_TOPIC_XXXX', 'body'), 100),
            null,
            'is forbidden',
            'checkMessage 禁发 topic'
        );
        $nullBody = new Message('TopicTest', 'x');
        $nullBody->body = null;
        $this->checkClientError(static fn() => Validators::checkMessage($nullBody, 100), ResponseCode::MESSAGE_ILLEGAL, 'the message body is null', 'checkMessage body null');
        $this->checkClientError(static fn() => Validators::checkMessage(new Message('TopicTest', ''), 100), ResponseCode::MESSAGE_ILLEGAL, 'the message body length is zero', 'checkMessage body 空');
        $this->checkClientError(
            static fn() => Validators::checkMessage(new Message('TopicTest', str_repeat('x', 11)), 10),
            ResponseCode::MESSAGE_ILLEGAL,
            'the message body size over max value, MAX: 10',
            'checkMessage body 超限'
        );
        $this->check($this->capture(static fn() => Validators::checkMessage(new Message('TopicTest', str_repeat('x', 10)), 10)) === null, 'checkMessage body 恰好等于上限合法');

        $lmqBad = new Message('TopicTest', 'x');
        $lmqBad->putProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH, 'a' . Validators::FILE_SEPARATOR . 'b');
        $this->checkClientError(static fn() => Validators::checkMessage($lmqBad, 100), ResponseCode::MESSAGE_ILLEGAL, 'can not contains', 'checkMessage LMQ 路径含分隔符');
        $lmqOk = new Message('TopicTest', 'x');
        $lmqOk->putProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH, 'ab');
        $this->check($this->capture(static fn() => Validators::checkMessage($lmqOk, 100)) === null, 'checkMessage LMQ 无分隔符合法');
    }

    // ==================================================================== ConsumeExecutor

    private function testConsumeExecutor(): void
    {
        $this->check(is_subclass_of(RejectedExecutionError::class, \RuntimeException::class), 'RejectedExecutionError extends RuntimeException');

        $ex = new ConsumeExecutor(1, 3);
        $this->checkSame(1, $ex->getCorePoolSize(), 'getCorePoolSize');
        $this->checkSame(3, $ex->getMaxPoolSize(), 'getMaxPoolSize');
        $this->checkSame(0, $ex->workerCount(), '初始 workerCount=0');
        $this->checkSame(0, $ex->queuedCount(), '初始 queuedCount=0');

        $this->checkSame(0, (new ConsumeExecutor(-5, -3))->getCorePoolSize(), 'core 负值钳到 0');
        $this->checkSame(0, (new ConsumeExecutor(-5, -3))->getMaxPoolSize(), 'max 钳到 core');
        $this->checkSame(5, (new ConsumeExecutor(5, 2))->getMaxPoolSize(), 'max < core 抬到 core');

        // submit 仅入队，pump 才执行
        $log = [];
        $ex->submit(static function () use (&$log): void {
            $log[] = 'a';
        });
        $this->checkSame(1, $ex->queuedCount(), 'submit 入队');
        $this->checkSame(1, $ex->workerCount(), 'submit 后 worker=core');
        $this->checkSame(0, count($log), 'pump 前任务未执行');
        $this->check(true === $ex->pumpOnce(), 'pumpOnce 执行一条');
        $this->checkSame(['a'], $log, 'pumpOnce 执行了任务');
        $this->check(false === $ex->pumpOnce(), '空队列 pumpOnce 返回 false');

        // 带参任务 + FIFO
        $ex2 = new ConsumeExecutor(1, 1);
        $argSeen = null;
        $ex2->submit(static function (string $s) use (&$argSeen): void {
            $argSeen = $s;
        }, 'x');
        $order = [];
        $ex2->submit(static function () use (&$order): void {
            $order[] = 1;
        });
        $ex2->submit(static function () use (&$order): void {
            $order[] = 2;
        });
        $ex2->pumpAll();
        $this->checkSame('x', $argSeen, 'submit 传递参数');
        $this->checkSame([1, 2], $order, 'pumpAll FIFO');

        // 任务异常不杀 worker
        $ex3 = new ConsumeExecutor(1, 1);
        $ex3->submit(static function (): void {
            throw new \RuntimeException('boom');
        });
        $ex3->submit(static function () use (&$order): void {
            $order[] = 'after';
        });
        $ex3->pumpAll();
        $this->checkSame(1, $ex3->handlerExceptionCount(), '任务异常被计数');
        $this->checkSame('after', $order[count($order) - 1], '异常不阻断后续任务');
        $this->checkSame(1, $ex3->workerCount(), '异常不杀 worker');

        // setCorePoolSize
        $this->checkThrows(static fn() => $ex->setCorePoolSize(-1), \ValueError::class, 'setCorePoolSize(-1) → ValueError');
        $ex4 = new ConsumeExecutor(1, 3);
        $ex4->submit(static function (): void {});
        $ex4->submit(static function (): void {});
        $ex4->submit(static function (): void {});
        $this->checkSame(1, $ex4->workerCount(), '无界队列只有 core 个 worker');
        $ex4->setCorePoolSize(3);
        $this->checkSame(3, $ex4->getCorePoolSize(), 'setCorePoolSize 更新 core');
        $this->checkSame(3, $ex4->workerCount(), 'setCorePoolSize 按队列长度补足 worker');
        $ex4->setCorePoolSize(5);
        $this->checkSame(5, $ex4->getMaxPoolSize(), 'setCorePoolSize 超过 max 时抬升 max');

        // 有界队列满则拒绝
        $bounded = new ConsumeExecutor(1, 1, 60.0, 'p', 1);
        $bounded->submit(static function (): void {});
        $this->checkThrows(static fn() => $bounded->submit(static function (): void {}), RejectedExecutionError::class, '有界队列满 → RejectedExecutionError');

        // 有界队列满但可开非 core 线程
        $bounded2 = new ConsumeExecutor(1, 3, 60.0, 'p', 1);
        $bounded2->submit(static function (): void {});
        $bounded2->submit(static function (): void {});
        $bounded2->submit(static function (): void {});
        $this->checkSame(3, $bounded2->workerCount(), '队列满且可生长 → 开非 core 线程');
        $e = $this->capture(static fn() => $bounded2->submit(static function (): void {}));
        $this->check($e instanceof RejectedExecutionError && str_contains($e->getMessage(), 'queue is full (1)'), '到达 max 后拒绝并报队列大小');
        // 空转 pump 退掉超编 worker
        $bounded2->pumpAll();
        $this->checkSame(1, $bounded2->workerCount(), 'keep-alive 空转退掉超编 worker');

        // shutdown
        $ex5 = new ConsumeExecutor(1, 1);
        $ex5->shutdown(false);
        $this->checkThrows(static fn() => $ex5->submit(static function (): void {}), RejectedExecutionError::class, 'shutdown 后 submit 被拒');
        $ex5->shutdown(false); // 幂等

        $done = [];
        $ex6 = new ConsumeExecutor(1, 1);
        $ex6->submit(static function () use (&$done): void {
            $done[] = 1;
        });
        $ex6->submit(static function () use (&$done): void {
            $done[] = 2;
        });
        $ex6->shutdown(true);
        $this->checkSame([1, 2], $done, 'shutdown(wait=true) 跑完队列');
        $this->checkSame(0, $ex6->queuedCount(), 'shutdown(wait=true) 队列清空');
        $this->checkSame(0, $ex6->workerCount(), 'shutdown(wait=true) worker 归零');
    }

    // ==================================================================== ConsumerStats

    private function testConsumerStats(): void
    {
        $this->checkSame(10.0, ConsumerStatsManager::SAMPLING_INTERVAL_SECONDS, 'SAMPLING_INTERVAL_SECONDS=10');
        $this->checkSame(600.0, ConsumerStatsManager::HOUR_SAMPLING_INTERVAL_SECONDS, 'HOUR_SAMPLING_INTERVAL_SECONDS=600');
        $this->checkSame(60, ConsumerStatsManager::MINUTE_LIST_MAX, 'MINUTE_LIST_MAX=60');
        $this->checkSame(60, ConsumerStatsManager::HOUR_LIST_MAX, 'HOUR_LIST_MAX=60');

        // StatsSnapshot 默认
        $ss = new StatsSnapshot();
        $this->checkSame(0, $ss->sum, 'StatsSnapshot.sum 默认 0');
        $this->checkSame(0.0, $ss->tps, 'StatsSnapshot.tps 默认 0.0');
        $this->checkSame(0.0, $ss->avgpt, 'StatsSnapshot.avgpt 默认 0.0');
        $this->checkSame(0, $ss->times, 'StatsSnapshot.times 默认 0');
        $this->check(str_contains((string) $ss, 'StatsSnapshot(sum=0'), 'StatsSnapshot __toString 形态');

        // computeStatsData
        $empty = StatsItem::computeStatsData([]);
        $this->checkSame(0, $empty->sum, 'computeStatsData([]) sum=0');
        $this->checkSame(0.0, $empty->tps, 'computeStatsData([]) tps=0');

        $one = StatsItem::computeStatsData([[1000, 50, 5]]);
        $this->checkSame(0, $one->sum, 'computeStatsData 单点 sum=0');
        $this->checkSame(0.0, $one->tps, 'computeStatsData 单点 tps=0');
        $this->checkSame(0, $one->times, 'computeStatsData 单点 times=0');

        $two = StatsItem::computeStatsData([[1000, 10, 1], [3000, 30, 3]]);
        $this->checkSame(20, $two->sum, 'computeStatsData sum=last-first');
        $this->checkSame(10.0, $two->tps, 'computeStatsData tps=sum*1000/span');
        $this->checkSame(2, $two->times, 'computeStatsData times=times 差');
        $this->checkSame(10.0, $two->avgpt, 'computeStatsData avgpt=sum/times');

        $sameTs = StatsItem::computeStatsData([[1000, 10, 1], [1000, 20, 2]]);
        $this->checkSame(10, $sameTs->sum, 'span=0 时 sum 仍算');
        $this->checkSame(0.0, $sameTs->tps, 'span=0 时 tps=0（不除零）');
        $this->checkSame(10.0, $sameTs->avgpt, 'span=0 时 avgpt 正常');

        $noTimes = StatsItem::computeStatsData([[1000, 10, 0], [2000, 20, 0]]);
        $this->checkSame(10, $noTimes->sum, 'timesDiff=0 sum');
        $this->checkSame(0, $noTimes->times, 'timesDiff=0 times=0');
        $this->checkSame(0.0, $noTimes->avgpt, 'timesDiff=0 avgpt=0');

        // StatsItem 累计与采样
        $item = new StatsItem('PULL_RT', 'T@G');
        $this->checkSame('PULL_RT', $item->statsName, 'StatsItem.statsName');
        $this->checkSame('T@G', $item->statsKey, 'StatsItem.statsKey');
        $item->addValue(10, 2);
        $item->addValue(5, 1);
        $this->checkSame(15, $item->getValue(), 'StatsItem 累计 value');
        $this->checkSame(3, $item->getTimes(), 'StatsItem 累计 times');
        $this->checkSame(0, $item->getMinuteSampleCount(), '采样前分钟链空');
        $item->sample();
        usleep(2000);
        $item->addValue(15, 1);
        $item->sample();
        $this->checkSame(2, $item->getMinuteSampleCount(), '两次 sample → 2 点');
        $snap = $item->getStatsDataInMinute();
        $this->checkSame(15, $snap->sum, '分钟快照 sum=窗口增量');
        $this->checkSame(1, $snap->times, '分钟快照 times 增量');
        $this->checkSame(15.0, $snap->avgpt, '分钟快照 avgpt');
        $this->check($snap->tps > 0, '分钟快照 tps>0');
        $this->checkSame(0, $item->getStatsDataInHour()->sum, '小时链未采样 → sum=0');

        // 采样链溢出（超过 MAX 丢最旧）
        $over = new StatsItem('X', 'k');
        for ($i = 0; $i < 65; $i++) {
            $over->sample();
        }
        $this->checkSame(60, $over->getMinuteSampleCount(), '分钟链溢出截断到 60');
        for ($i = 0; $i < 65; $i++) {
            $over->sampleHour();
        }
        $this->checkSame(60, $over->getHourSampleCount(), '小时链溢出截断到 60');

        // StatsItemSet
        $set = new StatsItemSet('PULL_TPS');
        $this->checkSame('PULL_TPS', $set->statsName, 'StatsItemSet.statsName');
        $this->checkSame(null, $set->find('nope'), 'StatsItemSet.find 未命中 null');
        $a = $set->getAndCreate('k1');
        $this->check($set->getAndCreate('k1') === $a, 'getAndCreate 幂等');
        $set->addValue('k2', 7, 3);
        $this->checkSame([7, 3], [$set->find('k2')->getValue(), $set->find('k2')->getTimes()], 'StatsItemSet.addValue');
        $this->checkSame(['k1', 'k2'], $set->keys(), 'StatsItemSet.keys 顺序');
        $set->sampleAll();
        $this->checkSame(1, $set->find('k2')->getMinuteSampleCount(), 'sampleAll 采样全部');
        $set->sampleHourAll();
        $this->checkSame(1, $set->find('k2')->getHourSampleCount(), 'sampleHourAll 采样全部');

        // ConsumerStatsManager：key = topic@group
        $mgr = new ConsumerStatsManager();
        $mgr->start();
        $mgr->start(); // 幂等
        $mgr->incPullRt('G', 'T', 20);
        $this->checkSame(20, $mgr->topicAndGroupPullRt->find('T@G')->getValue(), 'incPullRt 落到 topic@group');
        $mgr->incPullTps('G', 'T', 4);
        $mgr->incConsumeRt('G', 'T', 30);
        $mgr->incConsumeOkTps('G', 'T', 8);
        $mgr->incConsumeFailedTps('G', 'T', 2);
        $this->checkSame(4, $mgr->topicAndGroupPullTps->find('T@G')->getValue(), 'incPullTps 累计');
        $this->checkSame(30, $mgr->topicAndGroupConsumeRt->find('T@G')->getValue(), 'incConsumeRt 累计');

        $mgr->sampleOnce();
        usleep(2000);
        $mgr->incPullRt('G', 'T', 20);
        $mgr->incPullTps('G', 'T', 4);
        $mgr->incConsumeRt('G', 'T', 30);
        $mgr->incConsumeOkTps('G', 'T', 8);
        $mgr->incConsumeFailedTps('G', 'T', 2);
        $mgr->sampleOnce();

        $cs = $mgr->consumeStatus('G', 'T');
        $this->check($cs instanceof ConsumeStatus, 'consumeStatus 返回 ConsumeStatus');
        $this->checkSame(20.0, $cs->pullRt, 'consumeStatus.pullRt=avgpt');
        $this->checkSame(30.0, $cs->consumeRt, 'consumeStatus.consumeRt=avgpt');
        $this->check($cs->pullTps > 0, 'consumeStatus.pullTps>0');
        $this->check($cs->consumeOkTps > 0, 'consumeStatus.consumeOkTps>0');
        $this->check($cs->consumeFailedTps > 0, 'consumeStatus.consumeFailedTps>0');
        $this->checkSame(0, $cs->consumeFailedMsgs, 'consumeFailedMsgs 取小时窗口（未采样=0）');

        $miss = $mgr->consumeStatus('NOGROUP', 'NOTOPIC');
        $this->checkSame(0.0, $miss->pullRt, 'consumeStatus 无数据 pullRt=0');

        // consumeFailedMsgs 取 failed 的 hour 窗口 sum（跨窗口取数）
        $mgr2 = new ConsumerStatsManager();
        $failed = $mgr2->topicAndGroupConsumeFailedTps->getAndCreate('T@G');
        $failed->addValue(5, 1);
        $failed->sampleHour();
        usleep(2000);
        $failed->addValue(7, 1);
        $failed->sampleHour();
        $cs2 = $mgr2->consumeStatus('G', 'T');
        $this->checkSame(7, $cs2->consumeFailedMsgs, 'consumeFailedMsgs=hour 窗口 sum');
        $mgr->shutdown();
    }

    // ==================================================================== RequestReply

    private function testRequestReply(): void
    {
        $this->checkSame(3000, RequestResponseFuture::DEFAULT_REQUEST_TIMEOUT_MILLIS, 'DEFAULT_REQUEST_TIMEOUT_MILLIS=3000');

        $f = new RequestResponseFuture('cid-1', 3000);
        $this->checkSame('cid-1', $f->correlationId, 'RequestResponseFuture.correlationId');
        $this->checkSame(3000, $f->timeoutMillis, 'RequestResponseFuture.timeoutMillis');
        $this->checkSame(null, $f->requestCallback, '默认 callback=null');
        $this->checkSame(null, $f->responseMsg, '默认 responseMsg=null');
        $this->check(true === $f->sendRequestOk, '默认 sendRequestOk=true');
        $this->checkSame(null, $f->cause, '默认 cause=null');
        $this->check($f->beginTimestamp > 0, 'beginTimestamp 已设置');
        $this->check(false === $f->isTimeout(), '未超时');
        $this->checkSame(null, $f->waitResponseMessage(100), '未投递时 wait 返回 null');

        $msg = new Message('T', 'r');
        $f->putResponseMessage($msg);
        $this->check($f->waitResponseMessage(100) === $msg, 'putResponseMessage 后 wait 取到');

        $this->check(true === (new RequestResponseFuture('c', -1))->isTimeout(), 'timeout<0 → isTimeout');
        $this->check(false === (new RequestResponseFuture('c', 100000))->isTimeout(), 'timeout 大 → 未超时');

        // executeRequestCallback 只触发一次
        $cb = new RecordingRequestCallback();
        $f2 = new RequestResponseFuture('c2', 3000, $cb);
        $f2->putResponseMessage($msg);
        $f2->executeRequestCallback();
        $f2->executeRequestCallback();
        $this->checkSame(1, $cb->successCount, '成功回调只触发一次');
        $this->checkSame(0, $cb->exceptionCount, '成功路径无异常回调');
        $this->check($cb->lastResponse === $msg, '成功回调收到 response');

        // sendRequestOk=false → onException(cause)
        $cb2 = new RecordingRequestCallback();
        $f3 = new RequestResponseFuture('c3', 3000, $cb2);
        $f3->sendRequestOk = false;
        $cause = new \RuntimeException('send failed');
        $f3->cause = $cause;
        $f3->executeRequestCallback();
        $this->checkSame(1, $cb2->exceptionCount, 'sendRequestOk=false → onException');
        $this->check($cb2->lastCause === $cause, 'onException 收到 cause');

        // cause 非空即使 sendRequestOk=true 也走异常
        $cb3 = new RecordingRequestCallback();
        $f4 = new RequestResponseFuture('c4', 3000, $cb3);
        $f4->cause = new \RuntimeException('x');
        $f4->executeRequestCallback();
        $this->checkSame(1, $cb3->exceptionCount, 'cause 非空 → onException');
        $this->checkSame(0, $cb3->successCount, 'cause 非空不走成功');

        // callback 为空时 no-op
        $f5 = new RequestResponseFuture('c5', 3000);
        $this->check($this->capture(static fn() => $f5->executeRequestCallback()) === null, '无 callback → no-op');

        // RequestFutureHolder
        $h = new RequestFutureHolder();
        $h->putRequest('c', $f);
        $this->check($h->getRequest('c') === $f, 'putRequest/getRequest');
        $this->checkSame(null, $h->getRequest('nope'), 'getRequest 未命中 null');
        $this->check($h->removeRequest('c') === $f, 'removeRequest 返回被摘项');
        $this->checkSame(null, $h->removeRequest('c'), 'removeRequest 二次 null');

        $cb4 = new RecordingRequestCallback();
        $f6 = new RequestResponseFuture('c6', 3000, $cb4);
        $h->putRequest('c6', $f6);
        $got = $h->putResponse('c6', $msg);
        $this->check($got === $f6, 'putResponse 返回被填充 future');
        $this->check($f6->responseMsg === $msg, 'putResponse 填充 responseMsg');
        $this->checkSame(null, $h->getRequest('c6'), 'putResponse 后从表移除');
        $this->checkSame(1, $cb4->successCount, 'putResponse 触发成功回调');
        $this->checkSame(null, $h->putResponse('c6', $msg), '重复 putResponse 返回 null');

        $this->check(RequestFutureHolder::getInstance() === RequestFutureHolder::getInstance(), 'getInstance 单例');

        // CorrelationIdUtil
        $cid = CorrelationIdUtil::createCorrelationId();
        $this->check(preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/', $cid) === 1, 'createCorrelationId 是 v4 UUID');
        $this->check(CorrelationIdUtil::createCorrelationId() !== $cid, 'createCorrelationId 每次不同');

        // MessageUtil
        $plain = new Message('T', 'b');
        $this->check(false === MessageUtil::isReplyMessage($plain), 'isReplyMessage 普通 false');
        $plain->putProperty(MessageConst::PROPERTY_MESSAGE_TYPE, MixAll::REPLY_MESSAGE_FLAG);
        $this->check(true === MessageUtil::isReplyMessage($plain), 'isReplyMessage reply true');

        $this->checkClientError(
            static fn(): Message => MessageUtil::createReplyMessage(null, 'x'),
            ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION,
            'requestMessage cannot be null',
            'createReplyMessage null'
        );
        $noCluster = new Message('T', 'b');
        $this->checkClientError(
            static fn(): Message => MessageUtil::createReplyMessage($noCluster, 'x'),
            ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION,
            'property[CLUSTER] is null',
            'createReplyMessage 无 CLUSTER'
        );

        $req = new Message('ReqTopic', 'req-body');
        $req->putProperty(MessageConst::PROPERTY_CLUSTER, 'DefaultCluster');
        $req->putProperty(MessageConst::PROPERTY_CORRELATION_ID, 'cid-9');
        $req->putProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT, 'client-9');
        $req->putProperty(MessageConst::PROPERTY_MESSAGE_TTL, '5000');
        $reply = MessageUtil::createReplyMessage($req, 'reply-body');
        $this->checkSame('DefaultCluster_REPLY_TOPIC', $reply->topic, 'createReplyMessage topic');
        $this->checkSame('reply-body', $reply->body, 'createReplyMessage body');
        $this->checkSame(MixAll::REPLY_MESSAGE_FLAG, $reply->getProperty(MessageConst::PROPERTY_MESSAGE_TYPE), 'createReplyMessage MSG_TYPE=reply');
        $this->checkSame('cid-9', $reply->getProperty(MessageConst::PROPERTY_CORRELATION_ID), 'createReplyMessage CORRELATION_ID');
        $this->checkSame('client-9', $reply->getProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT), 'createReplyMessage REPLY_TO_CLIENT');
        $this->checkSame('5000', $reply->getProperty(MessageConst::PROPERTY_MESSAGE_TTL), 'createReplyMessage TTL');
        $this->check(true === MessageUtil::isReplyMessage($reply), 'createReplyMessage 结果 isReplyMessage');

        // reactCallback
        $called = null;
        RequestReply::reactCallback(static function (?Message $m, ?\Throwable $c) use (&$called): void {
            $called = [$m, $c];
        }, $f6);
        $this->check(is_array($called) && $called[0] === $msg && $called[1] === null, 'reactCallback 传 (response, cause)');
        $this->check($this->capture(static fn() => RequestReply::reactCallback(null, $f6)) === null, 'reactCallback(null) no-op');
    }

    // ==================================================================== TraceContextPropagator

    private function testTraceContext(): void
    {
        $this->checkSame('traceparent', TraceContextPropagator::TRACE_CONTEXT_PROPERTY, 'TRACE_CONTEXT_PROPERTY');
        $this->checkSame('tracestate', TraceContextPropagator::TRACE_STATE_PROPERTY, 'TRACE_STATE_PROPERTY');

        $tp = TraceContextPropagator::generateTraceparent();
        $this->check(preg_match('/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/', $tp) === 1, 'generateTraceparent 格式');
        $this->check(true === TraceContextPropagator::isValidTraceparent($tp), '生成值合法');
        $this->check(TraceContextPropagator::generateTraceparent() !== $tp, 'generateTraceparent 随机');

        $this->check(false === TraceContextPropagator::isValidTraceparent(null), 'null 非法');
        $this->check(false === TraceContextPropagator::isValidTraceparent(''), '空串非法');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-abc'), '段数不足非法');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-abc-def-01-extra'), '段数过多非法');
        $this->check(false === TraceContextPropagator::isValidTraceparent('ff-' . str_repeat('a', 32) . '-' . str_repeat('b', 16) . '-01'), 'version=ff 非法');
        $this->check(false === TraceContextPropagator::isValidTraceparent('zzz-' . str_repeat('a', 32) . '-' . str_repeat('b', 16) . '-01'), 'version 非法长度/字符');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-' . str_repeat('0', 32) . '-' . str_repeat('b', 16) . '-01'), 'trace-id 全 0 非法');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-' . str_repeat('a', 32) . '-' . str_repeat('0', 16) . '-01'), 'parent-id 全 0 非法');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-' . str_repeat('a', 31) . '-' . str_repeat('b', 16) . '-01'), 'trace-id 长度错非法');
        $this->check(true === TraceContextPropagator::isValidTraceparent(strtoupper($tp)), '大写 hex 宽松接受');
        $this->check(true === TraceContextPropagator::isValidTraceparent('00-' . str_repeat('a', 32) . '-' . str_repeat('b', 16) . '-00'), 'flags 全 0 合法');
        $this->check(true === TraceContextPropagator::isValidTraceparent('01-' . str_repeat('a', 32) . '-' . str_repeat('b', 16) . '-01'), '非 00 的 2 位 hex version 合法');
        $this->check(true === TraceContextPropagator::isValidTraceparent('  ' . $tp . '  '), '外层空白容忍');

        // childTraceparent
        $child = TraceContextPropagator::childTraceparent($tp);
        $this->check($child !== null && $child !== $tp, 'childTraceparent 换 parent-id');
        $this->check(true === TraceContextPropagator::isValidTraceparent($child), 'child 合法');
        $this->checkSame(explode('-', $tp)[1], explode('-', (string) $child)[1], 'child 保持 trace-id');
        $this->checkSame(null, TraceContextPropagator::childTraceparent(null), 'childTraceparent(null) → null');
        $this->checkSame(null, TraceContextPropagator::childTraceparent('bad'), 'childTraceparent(非法) → null');

        // injectTraceContext
        $m = new Message('T', 'b');
        $injected = TraceContextPropagator::injectTraceContext($m);
        $this->check(true === TraceContextPropagator::isValidTraceparent($injected), '注入值合法');
        $this->checkSame($injected, $m->getProperty(TraceContextPropagator::TRACE_CONTEXT_PROPERTY), '注入写回属性');
        $this->checkSame($injected, TraceContextPropagator::injectTraceContext($m), '已有值不覆盖');
        $m2 = new Message('T', 'b');
        $m2->putProperty(TraceContextPropagator::TRACE_CONTEXT_PROPERTY, 'custom-tp');
        $this->checkSame('custom-tp', TraceContextPropagator::injectTraceContext($m2), '上游上下文优先');

        // extractTraceparent
        $ext = new MessageExt();
        $ext->putProperty(TraceContextPropagator::TRACE_CONTEXT_PROPERTY, 'tp-value');
        $this->checkSame('tp-value', TraceContextPropagator::extractTraceparent($ext), 'extractTraceparent 取值');
        $this->checkSame(null, TraceContextPropagator::extractTraceparent(new MessageExt()), 'extractTraceparent 无值 null');
        $ext2 = new MessageExt();
        $ext2->putProperty(TraceContextPropagator::TRACE_CONTEXT_PROPERTY, '');
        $this->checkSame(null, TraceContextPropagator::extractTraceparent($ext2), 'extractTraceparent 空串 null');

        // env 开关
        putenv('ROCKETMQ_TRACE_CONTEXT_ENABLE');
        $this->check(false === TraceContextPropagator::traceContextEnabledFromEnv(), 'env 未设 → false');
        putenv('ROCKETMQ_TRACE_CONTEXT_ENABLE=1');
        $this->check(true === TraceContextPropagator::traceContextEnabledFromEnv(), 'env=1 → true');
        putenv('ROCKETMQ_TRACE_CONTEXT_ENABLE=TRUE');
        $this->check(true === TraceContextPropagator::traceContextEnabledFromEnv(), 'env=TRUE → true');
        putenv('ROCKETMQ_TRACE_CONTEXT_ENABLE=Yes');
        $this->check(true === TraceContextPropagator::traceContextEnabledFromEnv(), 'env=Yes → true');
        putenv('ROCKETMQ_TRACE_CONTEXT_ENABLE=0');
        $this->check(false === TraceContextPropagator::traceContextEnabledFromEnv(), 'env=0 → false');
        putenv('ROCKETMQ_TRACE_CONTEXT_ENABLE=no');
        $this->check(false === TraceContextPropagator::traceContextEnabledFromEnv(), 'env=no → false');
        putenv('ROCKETMQ_TRACE_CONTEXT_ENABLE');
    }

    public function run(): int
    {
        Logger::setHandler(static function (string $line): void {}); // 静音（含任务异常日志）
        $this->testSendResult();
        $this->testConsumerResult();
        $this->testTopAddressing();
        $this->testLatency();
        $this->testMetrics();
        $this->testBackpressure();
        $this->testHook();
        $this->testValidators();
        $this->testConsumeExecutor();
        $this->testConsumerStats();
        $this->testRequestReply();
        $this->testTraceContext();
        return $this->summary();
    }
}

exit((new RunClientLeaf())->run());
