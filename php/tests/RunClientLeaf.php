<?php

declare(strict_types=1);

/**
 * Client leaf-module pure-PHP assert-style self-test (no phpunit dependency).
 *
 * Covers: SendResult / ConsumerResult / TopAddressing / Latency / Metrics / Backpressure /
 * Hook / Validators / ConsumeExecutor / ConsumerStats / RequestReply / TraceContextPropagator.
 *
 * Run: C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunClientLeaf.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
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

// ==================================================================== test doubles

/** DefaultTopAddressing that overrides httpGet to inject mock responses. */
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

/** Fake TopicPublishInfo used by MQFaultStrategy.selectOneMessageQueue. */
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

/** RequestCallback implementation recording whether callbacks fired. */
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

/** Recording SendMessageHook. */
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

/** Recording ConsumeMessageHook. */
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

/** Recording EndTransactionHook. */
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

/** Recording CheckForbiddenHook. */
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

/** Recording FilterMessageHook. */
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

    /** Asserts that MQClientException is thrown with matching responseCode / message. */
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
        $this->checkSame(SendStatus::SEND_OK, SendStatus::fromCode(99), 'fromCode(unknown) falls back to SEND_OK');
        $this->checkSame(SendStatus::SEND_OK, SendStatus::fromCode(-1), 'fromCode(-1) falls back to SEND_OK');

        $r = new SendResult();
        $this->checkSame(SendStatus::SEND_OK, $r->getSendStatus(), 'SendResult default sendStatus');
        $this->checkSame(null, $r->getMsgId(), 'SendResult default msgId=null');
        $this->checkSame(null, $r->getMessageQueue(), 'SendResult default messageQueue=null');
        $this->checkSame(0, $r->getQueueOffset(), 'SendResult default queueOffset=0');
        $this->checkSame(null, $r->getTransactionId(), 'SendResult default transactionId=null');
        $this->checkSame(null, $r->getOffsetMsgId(), 'SendResult default offsetMsgId=null');
        $this->checkSame(null, $r->getRegionId(), 'SendResult default regionId=null');
        $this->check(true === $r->isTraceOn(), 'SendResult default traceOn=true');
        $this->checkSame(null, $r->getRecallHandle(), 'SendResult default recallHandle=null');

        $mq = new MessageQueue('TopicTest', 'broker-a', 3);
        $r2 = new SendResult(SendStatus::FLUSH_DISK_TIMEOUT, 'MSG-1', $mq, 17, 'TX-1', 'OFF-1', 'REG-1');
        $this->checkSame(SendStatus::FLUSH_DISK_TIMEOUT, $r2->getSendStatus(), 'SendResult ctor sendStatus');
        $this->checkSame('MSG-1', $r2->getMsgId(), 'SendResult ctor msgId');
        $this->check($r2->getMessageQueue() === $mq, 'SendResult ctor messageQueue same reference');
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
        // Parameter type is ?string, passing null is legal (nullable protocol field): clears transactionId
        $this->check($this->capture(static fn() => $r2->setTransactionId(null)) === null, 'setTransactionId(null) does not throw (?string is legal)');
        $this->checkSame(null, $r2->getTransactionId(), 'setTransactionId(null) clears the field');
        $this->check(str_contains((string) $r2, 'SendResult [sendStatus='), '__toString shape');
        $this->check(str_contains((string) $r2, 'MSG-1'), '__toString contains msgId');
    }

    // ==================================================================== ConsumerResult

    private function testConsumerResult(): void
    {
        // Enum values
        $this->checkSame(0, PullStatus::FOUND->value, 'PullStatus.FOUND=0');
        $this->checkSame(3, PullStatus::OFFSET_ILLEGAL->value, 'PullStatus.OFFSET_ILLEGAL=3');
        $this->checkSame(PullStatus::NO_MATCHED_MSG, PullStatus::fromCode(2), 'PullStatus.fromCode(2)');
        $this->checkSame(PullStatus::FOUND, PullStatus::fromCode(88), 'PullStatus.fromCode(unknown) falls back');

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
        $this->checkSame(0, $p->nextBeginOffset, 'PullResult default nextBeginOffset');
        $this->checkSame(0, $p->minOffset, 'PullResult default minOffset');
        $this->checkSame(0, $p->maxOffset, 'PullResult default maxOffset');
        $this->checkSame([], $p->msgFoundList, 'PullResult default msgFoundList=[]');
        $this->checkSame(null, $p->suggestWhichBrokerId, 'PullResult default suggestWhichBrokerId=null');
        $msgs = [(new MessageExt())];
        $p2 = new PullResult(PullStatus::FOUND, 100, 5, 200, $msgs, 1);
        $this->checkSame(100, $p2->nextBeginOffset, 'PullResult ctor nextBeginOffset');
        $this->checkSame(1, $p2->suggestWhichBrokerId, 'PullResult ctor suggestWhichBrokerId');
        $this->checkSame(1, count($p2->msgFoundList), 'PullResult ctor msgFoundList');
        $this->check(str_contains((string) $p2, 'PullStatus.FOUND'), 'PullResult __toString');

        // PopResult
        $pop = new PopResult(PopStatus::POLLING_FULL);
        $this->checkSame(PopStatus::POLLING_FULL, $pop->status, 'PopResult.status');
        $this->checkSame([], $pop->msgFoundList, 'PopResult default msgFoundList=[]');
        $this->checkSame(0, $pop->restNum, 'PopResult default restNum');
        $this->checkSame(null, $pop->startOffsetInfo, 'PopResult default startOffsetInfo');
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
        $this->checkSame(0, $bad->popTime, 'ChangeInvisibleTimeResult default popTime');

        // Contexts
        $cc = new ConsumeConcurrentlyContext();
        $this->checkSame(null, $cc->messageQueue, 'ConsumeConcurrentlyContext default mq=null');
        $this->checkSame(0, $cc->delayLevelWhenNextConsume, 'ConsumeConcurrentlyContext default delay=0');
        $this->checkSame((1 << 31) - 1, $cc->ackIndex, 'ConsumeConcurrentlyContext default ackIndex=Integer.MAX_VALUE');
        $cc2 = new ConsumeConcurrentlyContext($mq = new MessageQueue('T', 'b', 0));
        $this->check($cc2->messageQueue === $mq, 'ConsumeConcurrentlyContext ctor mq');

        $oc = new ConsumeOrderlyContext();
        $this->check(true === $oc->autoCommit, 'ConsumeOrderlyContext default autoCommit=true');
        $this->checkSame(-1, $oc->suspendCurrentQueueTimeMillis, 'ConsumeOrderlyContext default suspend=-1');
        $this->checkSame(null, $oc->messageQueue, 'ConsumeOrderlyContext default mq=null');

        // consumeStatusName (shape = bare enum member name)
        $this->checkSame('CONSUME_SUCCESS', consumeStatusName(ConsumeConcurrentlyStatus::CONSUME_SUCCESS), 'consumeStatusName concurrent success');
        $this->checkSame('RECONSUME_LATER', consumeStatusName(ConsumeConcurrentlyStatus::RECONSUME_LATER), 'consumeStatusName concurrent re-consume');
        $this->checkSame('SUSPEND_CURRENT_QUEUE_A_MOMENT', consumeStatusName(ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT), 'consumeStatusName orderly suspend');
        $this->checkSame('None', consumeStatusName(null), 'consumeStatusName null fallback');
        $this->checkSame('raw', consumeStatusName('raw'), 'consumeStatusName non-enum fallback str');

        // Listener contract (interface)
        $listener = new class implements MessageListenerConcurrently {
            public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
            {
                return ConsumeConcurrentlyStatus::RECONSUME_LATER;
            }
        };
        $this->check($listener instanceof MessageListenerConcurrently, 'listener implements MessageListenerConcurrently');
        $this->checkSame(
            ConsumeConcurrentlyStatus::RECONSUME_LATER,
            $listener->consumeMessage([], new ConsumeConcurrentlyContext()),
            'listener consumeMessage returns a status'
        );
    }

    // ==================================================================== TopAddressing

    private function testTopAddressing(): void
    {
        $this->checkSame('jmenv.tbsite.net', DefaultTopAddressing::DEFAULT_NAMESRV_ADDR_LOOKUP, 'DEFAULT_NAMESRV_ADDR_LOOKUP');
        $this->checkSame('nsaddr', DefaultTopAddressing::DEFAULT_DOMAIN_SUBGROUP, 'DEFAULT_DOMAIN_SUBGROUP');

        // clearNewLine
        $this->checkSame('abc', DefaultTopAddressing::clearNewLine("  abc  "), 'clearNewLine trim');
        $this->checkSame('abc', DefaultTopAddressing::clearNewLine("abc\r\ndef"), 'clearNewLine cuts at \\r');
        $this->checkSame('abc', DefaultTopAddressing::clearNewLine("abc\ndef"), 'clearNewLine cuts at \\n');
        $this->checkSame('abc', DefaultTopAddressing::clearNewLine('abc'), 'clearNewLine without newline');

        // getWsAddr
        $this->checkSame('http://jmenv.tbsite.net:8080/rocketmq/nsaddr', DefaultTopAddressing::getWsAddr('jmenv.tbsite.net'), 'getWsAddr default port');
        $this->checkSame('http://host:9999/rocketmq/nsaddr', DefaultTopAddressing::getWsAddr('host:9999'), 'getWsAddr keeps an explicit port, no append');
        $this->checkSame('http://d:8080/rocketmq/sub', DefaultTopAddressing::getWsAddr('d', 'sub'), 'getWsAddr explicit subgroup');
        $this->checkSame('http://d:8080/rocketmq/nsaddr', DefaultTopAddressing::getWsAddr('d', ''), 'getWsAddr empty subgroup falls back to default');

        // buildUrl
        $this->checkSame('http://mock/rocketmq/nsaddr', (new MockTopAddressing([]))->buildUrl(), 'buildUrl without unit/para');
        $this->checkSame('http://mock/rocketmq/nsaddr-u?nofix=1', (new MockTopAddressing([], 'http://mock/rocketmq/nsaddr', 'u'))->buildUrl(), 'buildUrl unit only');
        $this->checkSame('http://mock/rocketmq/nsaddr?k=v', (new MockTopAddressing([], 'http://mock/rocketmq/nsaddr', '', ['k' => 'v']))->buildUrl(), 'buildUrl para only');
        $this->checkSame('http://mock/rocketmq/nsaddr-u?nofix=1&k=v&k2=v2', (new MockTopAddressing([], 'http://mock/rocketmq/nsaddr', 'u', ['k' => 'v', 'k2' => 'v2']))->buildUrl(), 'buildUrl unit+para');
        $this->checkSame('http://mock/rocketmq/nsaddr?k=v', (new MockTopAddressing([], 'http://mock/rocketmq/nsaddr', '   ', ['k' => 'v']))->buildUrl(), 'buildUrl whitespace unit treated as empty');

        // Constructor: domain / env var
        putenv('ROCKETMQ_NAMESRV_DOMAIN=');
        $this->checkSame('', (new DefaultTopAddressing())->wsAddr, 'no domain configured → wsAddr empty');
        $this->check(false === DefaultTopAddressing::isConfigured(), 'isConfigured false when unconfigured');
        putenv('ROCKETMQ_NAMESRV_DOMAIN=envdomain');
        $this->check(true === DefaultTopAddressing::isConfigured(), 'isConfigured true when configured');
        $this->checkSame('http://envdomain:8080/rocketmq/nsaddr', (new DefaultTopAddressing())->wsAddr, 'env domain takes effect');
        putenv('ROCKETMQ_NAMESRV_DOMAIN=');
        $this->checkSame('http://explicit:8080/rocketmq/nsaddr', (new DefaultTopAddressing(null, '', null, 3000, 'explicit'))->wsAddr, 'explicit domain wins over env');
        $this->checkSame('http://given/rocketmq/nsaddr', (new DefaultTopAddressing('http://given/rocketmq/nsaddr'))->wsAddr, 'ws_addr given directly');

        // fetchNsAddr
        $this->checkSame(null, (new DefaultTopAddressing(''))->fetchNsAddr(), 'empty wsAddr → fetchNsAddr null');
        $mock = new MockTopAddressing(["addr1:9876\r\nrest"], 'http://mock/ns');
        $this->checkSame('addr1:9876', $mock->fetchNsAddr(), 'fetchNsAddr truncates at newline');
        $this->checkSame('http://mock/ns', $mock->seenUrls[0], 'fetchNsAddr uses buildUrl');

        $mock2 = new MockTopAddressing([new \RuntimeException('boom')], 'http://mock/ns');
        $this->checkSame(null, $mock2->fetchNsAddr(false), 'fetchNsAddr exception silent → null');
        $this->checkSame(null, $mock2->fetchNsAddr(true), 'fetchNsAddr exception verbose → null');

        // fetchAndApply: applies only on address change
        $mock3 = new MockTopAddressing(['A;B', 'A;B', 'C'], 'http://mock/ns');
        $this->checkSame('A;B', $mock3->fetchAndApply(), 'fetchAndApply applies on first call');
        $this->checkSame('A;B', $mock3->nsAddr, 'fetchAndApply caches nsAddr');
        $this->checkSame(null, $mock3->fetchAndApply(), 'fetchAndApply same value → null');
        $this->checkSame('C', $mock3->fetchAndApply(), 'fetchAndApply changed value → applies the new one');
        $this->checkSame(null, (new MockTopAddressing(['   '], 'http://mock/ns'))->fetchAndApply(), 'fetchAndApply blank address not applied');
    }

    // ==================================================================== Latency

    private function testLatency(): void
    {
        // FaultItem
        $fi = new FaultItem('b1');
        $this->checkSame('b1', $fi->name, 'FaultItem.name');
        $this->checkSame(0.0, $fi->currentLatency, 'FaultItem default currentLatency');
        $this->checkSame(0.0, $fi->startTimestamp, 'FaultItem default startTimestamp');
        $this->check(true === $fi->reachableFlag, 'FaultItem default reachable');
        $this->check(true === $fi->isAvailable(), 'FaultItem default available');
        $this->check(true === $fi->isReachable(), 'FaultItem default reachable');
        $fi->updateNotAvailableDuration(5000.0);
        $this->check($fi->startTimestamp > FaultItem::nowMillis(), 'FaultItem isolation start lies in the future');
        $this->check(false === $fi->isAvailable(), 'FaultItem unavailable during isolation');
        $before = $fi->startTimestamp;
        $fi->updateNotAvailableDuration(0.0);
        $this->checkSame($before, $fi->startTimestamp, 'updateNotAvailableDuration(0) does not change anything');
        $fi->reachableFlag = false;
        $this->check(false === $fi->isReachable(), 'FaultItem reachableFlag=false');

        // FaultItem::__toString
        $fiStr = new FaultItem('bx');
        $fiStr->currentLatency = 12.0;
        $fiStr->reachableFlag = false;
        $fiStrText = (string) $fiStr;
        $this->check(str_contains($fiStrText, 'FaultItem{name=bx') && str_contains($fiStrText, 'reachable=False'), 'FaultItem __toString shape');

        // LatencyFaultToleranceImpl
        $tol = new LatencyFaultToleranceImpl();
        $this->check(true === $tol->isAvailable('none'), 'no record → isAvailable true');
        $this->check(true === $tol->isReachable('none'), 'no record → isReachable true');
        $this->checkSame(null, $tol->getFaultItem('none'), 'no record → getFaultItem null');
        $tol->updateFaultItem('b2', 12.0, 0.0, false);
        $item = $tol->getFaultItem('b2');
        $this->check($item instanceof FaultItem, 'updateFaultItem creates an entry');
        $this->checkSame(12.0, $item->currentLatency, 'updateFaultItem writes latency');
        $this->check(false === $tol->isReachable('b2'), 'updateFaultItem writes reachable=false');
        $this->check(true === $tol->isAvailable('b2'), 'dur=0 → still available');
        $tol->updateFaultItem('b2', 99.0, 5000.0, true);
        $this->checkSame(99.0, $tol->getFaultItem('b2')->currentLatency, 'updateFaultItem updates latency');
        $this->check(true === $tol->isReachable('b2'), 'updateFaultItem updates reachable=true');
        $this->check(false === $tol->isAvailable('b2'), 'dur>0 → unavailable');
        $tol->remove('b2');
        $this->checkSame(null, $tol->getFaultItem('b2'), 'remove deletes the entry');
        $this->check(true === $tol->isAvailable('b2'), 'available again after remove');

        // MQFaultStrategy constants and configuration
        $this->checkSame([50, 100, 550, 1800, 3000, 5000, 15000], MQFaultStrategy::LATENCY_MAX, 'LATENCY_MAX table');
        $this->checkSame([0, 0, 2000, 5000, 6000, 10000, 30000], MQFaultStrategy::NOT_AVAILABLE_DURATION, 'NOT_AVAILABLE_DURATION table');
        $st = new MQFaultStrategy();
        $this->check(false === $st->isSendLatencyFaultEnable(), 'default off');
        $this->checkSame([50, 100, 550, 1800, 3000, 5000, 15000], $st->latencyMax, 'latencyMax copy');
        $this->checkSame([0, 0, 2000, 5000, 6000, 10000, 30000], $st->notAvailableDuration, 'notAvailableDuration copy');
        $st->setSendLatencyFaultEnable(true);
        $this->check(true === $st->isSendLatencyFaultEnable(), 'setSendLatencyFaultEnable(true)');
        $this->check($st->getLatencyFaultTolerance() instanceof LatencyFaultToleranceImpl, 'latencyFaultTolerance getter');

        // updateFaultItem is a no-op when disabled
        $off = new MQFaultStrategy(false);
        $off->updateFaultItem('b', 99999.0, true, true);
        $this->checkSame(null, $off->getLatencyFaultTolerance()->getFaultItem('b'), 'updateFaultItem no-op when disabled');

        // computeNotAvailableDuration tiers (isolation scenario always tiers on 10000ms)
        // Note: isolation=true forces latency to 10000 → dur=10000, always isolated;
        //       so the "not isolated" tiers must run with isolation=false, feeding the real
        //       latency straight into the tier table.
        $cases = [
            [0.0, false, 0.0],      // 0 < 50 → no tier
            [49.0, false, 0.0],     // < 50 → no tier
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
            $this->check($item !== null, "tier[$i] entry created");
            if ($expectedDuration > 0) {
                $this->check($item->startTimestamp > FaultItem::nowMillis(), "tier[$i] isolated");
                $this->check(false === $s->getLatencyFaultTolerance()->isAvailable('b'), "tier[$i] unavailable");
            } else {
                $this->checkSame(0.0, $item->startTimestamp, "tier[$i] not isolated");
                $this->check(true === $s->getLatencyFaultTolerance()->isAvailable('b'), "tier[$i] available");
            }
        }

        // Queue selection: when disabled, honors broker_filter
        $q1 = new MessageQueue('T', 'b1', 0);
        $q2 = new MessageQueue('T', 'b2', 0);
        $tp = new FakeTopicPublishInfo([$q1, $q2]);
        $off2 = new MQFaultStrategy(false);
        $this->checkSame('b1', $off2->selectOneMessageQueue($tp, null)->brokerName, 'when disabled picks the first queue');
        $this->checkSame('b2', $off2->selectOneMessageQueue($tp, 'b1')->brokerName, 'when disabled filters by lastBrokerName');
        $tp->queues = [$q1];
        $this->checkSame('b1', $off2->selectOneMessageQueue($tp, 'b1')->brokerName, 'when disabled falls back to plain round-robin when the filter misses');

        // Queue selection: when enabled prefers available
        $tp2 = new FakeTopicPublishInfo([$q1, $q2]);
        $on = new MQFaultStrategy(true);
        // Isolate b1 (unavailable), b2 stays available
        $on->updateFaultItem('b1', 10000.0, false, true);
        $on->updateFaultItem('b2', 1.0, false, true);
        $picked = $on->selectOneMessageQueue($tp2, null, true);
        $this->checkSame('b2', $picked->brokerName, 'when enabled skips unavailable brokers');
        $this->checkSame(1, $tp2->resetCount, 'resetIndex=true invokes resetIndex');

        // All unavailable → degrade to reachable (b1 reachable=true, b2 reachable=false)
        $tp3 = new FakeTopicPublishInfo([$q1, $q2]);
        $on2 = new MQFaultStrategy(true);
        $on2->updateFaultItem('b1', 10000.0, false, true);  // unavailable, reachable
        $on2->updateFaultItem('b2', 10000.0, false, false); // unavailable, unreachable
        $this->checkSame('b1', $on2->selectOneMessageQueue($tp3, null)->brokerName, 'all unavailable degrades to reachable');

        // All unavailable and unreachable → degrade to plain round-robin
        $tp4 = new FakeTopicPublishInfo([$q1, $q2]);
        $on3 = new MQFaultStrategy(true);
        $on3->updateFaultItem('b1', 10000.0, false, false);
        $on3->updateFaultItem('b2', 10000.0, false, false);
        $picked4 = $on3->selectOneMessageQueue($tp4, null);
        $this->check($picked4 !== null, 'all unreachable → plain round-robin fallback is non-null');
    }

    // ==================================================================== Metrics

    private function testMetrics(): void
    {
        $m = new ClientMetrics();
        $snap = $m->snapshot();
        $this->checkSame(12, count($snap), 'snapshot has 12 keys');
        $this->checkSame(0, $snap['sendCount'], 'initial sendCount=0');
        $this->checkSame(0, $snap['sendFailureCount'], 'initial sendFailureCount=0');
        $this->checkSame(0.0, $snap['sendRTAvg'], 'initial sendRTAvg=0.0');
        $this->checkSame(0.0, $snap['sendRTSum'], 'initial sendRTSum=0.0');
        $this->checkSame(0, $snap['consumeCount'], 'initial consumeCount=0');
        $this->checkSame(0.0, $snap['consumeRTAvg'], 'initial consumeRTAvg=0.0');

        $start = $m->recordSendStart();
        $this->check(is_float($start) && $start > 0, 'recordSendStart returns a millisecond timestamp');

        $base = microtime(true) * 1000.0;
        $m->recordSendSuccess($base - 10.0);
        $m->recordSendSuccess($base - 20.0);
        $snap = $m->snapshot();
        $this->checkSame(2, $snap['sendCount'], 'two successes sendCount=2');
        $this->checkSame(0, $snap['sendFailureCount'], 'no failures sendFailureCount=0');
        $this->checkClose(20.0, $snap['sendRTMax'], 5.0, 'sendRTMax≈20');
        $this->checkClose(10.0, $snap['sendRTMin'], 5.0, 'sendRTMin≈10');
        $this->checkClose(30.0, $snap['sendRTSum'], 8.0, 'sendRTSum≈30');
        $this->checkClose(15.0, $snap['sendRTAvg'], 5.0, 'sendRTAvg≈15');

        $m->recordSendFailure($base - 5.0);
        $snap = $m->snapshot();
        $this->checkSame(1, $snap['sendFailureCount'], 'failure counted in sendFailureCount');
        $this->checkSame(2, $snap['sendCount'], 'failure not counted in sendCount');
        $this->checkClose(5.0, $snap['sendRTMin'], 5.0, 'failure also updates RTMin');
        $this->checkClose(20.0, $snap['sendRTMax'], 5.0, 'failure did not raise RTMax');

        $m->recordConsumeSuccess($base - 7.0);
        $m->recordConsumeFailure($base - 3.0);
        $snap = $m->snapshot();
        $this->checkSame(1, $snap['consumeCount'], 'consumeCount=1');
        $this->checkSame(1, $snap['consumeFailureCount'], 'consumeFailureCount=1');
        $this->checkClose(7.0, $snap['consumeRTMax'], 5.0, 'consumeRTMax≈7');
        $this->checkClose(3.0, $snap['consumeRTMin'], 5.0, 'consumeRTMin≈3');
        $this->checkClose(10.0, $snap['consumeRTSum'], 8.0, 'consumeRTSum≈10');
        // Failures are not counted in consumeCount (stats count successes only): sum≈10 / count=1 → avg≈10
        $this->checkClose(10.0, $snap['consumeRTAvg'], 5.0, 'consumeRTAvg≈10');

        $this->check($m->recordConsumeStart() > 0, 'recordConsumeStart returns a millisecond timestamp');

        // ClientMetrics::__toString (JSON form of the snapshot)
        $this->check(str_contains((string) $m, 'ClientMetrics{"sendCount"'), 'ClientMetrics __toString shape');
    }

    // ==================================================================== Backpressure

    private function testBackpressure(): void
    {
        $this->checkSame(10, FairSemaphore::MIN_ASYNC_SEND_NUM, 'MIN_ASYNC_SEND_NUM=10');
        $this->checkSame(1024 * 1024, FairSemaphore::MIN_ASYNC_SEND_SIZE, 'MIN_ASYNC_SEND_SIZE=1M');

        // Pending (equivalent class of _Pending)
        $pend = new Pending(7);
        $this->checkSame(7, $pend->permits, 'Pending.permits');

        $s = new FairSemaphore(5);
        $this->checkSame(5, $s->totalPermits(), 'totalPermits initial');
        $this->checkSame(5, $s->availablePermits(), 'availablePermits initial');

        $this->check(true === $s->tryAcquire(3, 1000), 'tryAcquire(3) succeeds');
        $this->checkSame(2, $s->availablePermits(), '2 left after taking 3');
        $this->check(false === $s->tryAcquire(3, 1000), 'insufficient permits → rejected');
        $this->checkSame(2, $s->availablePermits(), 'permits unchanged after rejection');
        $this->check(true === $s->tryAcquire(2, 0), 'tryAcquire(2) exactly exhausts');
        $this->checkSame(0, $s->availablePermits(), '0 left after exhaustion');
        $this->check(false === $s->tryAcquire(1, 0), 'no permits → rejected');
        $this->check(true === $s->tryAcquire(0, 0), 'acquiring 0 always succeeds');

        $s->release(0);
        $this->checkSame(0, $s->availablePermits(), 'release(0) is a no-op');
        $s->release(-5);
        $this->checkSame(0, $s->availablePermits(), 'release(negative) is a no-op');
        $s->release(2);
        $this->checkSame(2, $s->availablePermits(), 'release(2) returns permits');
        $s->release(10);
        $this->checkSame(12, $s->availablePermits(), 'release may exceed the total');

        // setTotalPermits shifts the total (in-flight share preserved as-is)
        $s2 = new FairSemaphore(5);
        $s2->tryAcquire(2, 0);              // 2 in flight, 3 free
        $s2->setTotalPermits(3);            // free += 3-5 = -2 → 1
        $this->checkSame(3, $s2->totalPermits(), 'setTotalPermits updates the total');
        $this->checkSame(1, $s2->availablePermits(), 'setTotalPermits shifts the free permits');
        $this->check(false === $s2->tryAcquire(2, 0), 'rejected after shrink due to insufficient permits');
        $this->check(true === $s2->tryAcquire(1, 0), 'remaining permits usable after shrink');

        // Negative free permits (semaphore accepts a negative margin)
        $s3 = new FairSemaphore(2);
        $s3->tryAcquire(2, 0);              // free 0
        $s3->setTotalPermits(0);            // free += 0-2 = -2
        $this->checkSame(-2, $s3->availablePermits(), 'setTotalPermits yields negative free permits');
        $s3->release(3);
        $this->checkSame(1, $s3->availablePermits(), 'release pulls the negative margin back positive');
    }

    // ==================================================================== Hook

    private function testHook(): void
    {
        $this->checkSame('SYNC', CommunicationMode::SYNC, 'CommunicationMode.SYNC');
        $this->checkSame('ASYNC', CommunicationMode::ASYNC, 'CommunicationMode.ASYNC');
        $this->checkSame('ONEWAY', CommunicationMode::ONEWAY, 'CommunicationMode.ONEWAY');

        $sc = new SendMessageContext();
        $this->checkSame(null, $sc->producer, 'SendMessageContext.producer default null');
        $this->checkSame('', $sc->producerGroup, 'SendMessageContext.producerGroup default empty');
        $this->checkSame(null, $sc->message, 'SendMessageContext.message default null');
        $this->checkSame(null, $sc->mq, 'SendMessageContext.mq default null');
        $this->checkSame('', $sc->brokerAddr, 'SendMessageContext.brokerAddr default empty');
        $this->checkSame(null, $sc->communicationMode, 'SendMessageContext.communicationMode default null');
        $this->checkSame(null, $sc->sendResult, 'SendMessageContext.sendResult default null');
        $this->checkSame(null, $sc->exception, 'SendMessageContext.exception default null');
        $this->checkSame(null, $sc->mqTraceContext, 'SendMessageContext.mqTraceContext default null');
        $this->checkSame(MessageType::NORMAL_MSG, $sc->msgType, 'SendMessageContext.msgType default NORMAL_MSG');
        $this->checkSame('', $sc->namespace, 'SendMessageContext.namespace default empty');

        $cm = new ConsumeMessageContext();
        $this->checkSame('', $cm->consumerGroup, 'ConsumeMessageContext default group empty');
        $this->checkSame([], $cm->msgList, 'ConsumeMessageContext default msgList=[]');
        $this->check(true === $cm->success, 'ConsumeMessageContext default success=true');
        $this->checkSame(null, $cm->status, 'ConsumeMessageContext default status=null');
        $msg = new MessageExt();
        $mq = new MessageQueue('T', 'b', 0);
        $cm2 = new ConsumeMessageContext('G', [$msg], $mq);
        $this->checkSame('G', $cm2->consumerGroup, 'ConsumeMessageContext ctor group');
        $this->checkSame(1, count($cm2->msgList), 'ConsumeMessageContext ctor msgList');
        $this->check($cm2->mq === $mq, 'ConsumeMessageContext ctor mq');

        $et = new EndTransactionContext();
        $this->checkSame('', $et->producerGroup, 'EndTransactionContext.producerGroup default empty');
        $this->checkSame(null, $et->msgId, 'EndTransactionContext.msgId default null');
        $this->check(false === $et->fromTransactionCheck, 'EndTransactionContext.fromTransactionCheck default false');

        $cf = new CheckForbiddenContext();
        $this->checkSame('', $cf->nameSrvAddr, 'CheckForbiddenContext.nameSrvAddr default empty');
        $this->checkSame(null, $cf->arg, 'CheckForbiddenContext.arg default null');
        $this->check(false === $cf->unitMode, 'CheckForbiddenContext.unitMode default false');
        $this->checkSame(null, $cf->sendResult, 'CheckForbiddenContext has no sendResult');

        $fm = new FilterMessageContext();
        $this->checkSame([], $fm->msgList, 'FilterMessageContext default msgList=[]');
        $fm2 = new FilterMessageContext('G', [$msg], $mq);
        $this->checkSame('G', $fm2->consumerGroup, 'FilterMessageContext ctor group');
        $this->checkSame(1, count($fm2->msgList), 'FilterMessageContext ctor msgList');
        $this->check(false === $fm2->unitMode, 'FilterMessageContext.unitMode default false');

        // Hook interfaces usable
        $sh = new RecordingSendHook();
        $sh->sendMessageBefore($sc);
        $sh->sendMessageAfter($sc);
        $this->check($sh instanceof SendMessageHook && $sh->before === 1 && $sh->after === 1, 'SendMessageHook interface');
        $this->checkSame('send-hook', $sh->hookName(), 'SendMessageHook.hookName');

        $ch = new RecordingConsumeHook();
        $ch->consumeMessageBefore($cm);
        $ch->consumeMessageAfter($cm);
        $this->check($ch instanceof ConsumeMessageHook && $ch->before === 1 && $ch->after === 1, 'ConsumeMessageHook interface');

        $eh = new RecordingEndTxHook();
        $eh->endTransaction($et);
        $this->check($eh instanceof EndTransactionHook && $eh->calls === 1, 'EndTransactionHook interface');

        $fh = new RecordingForbiddenHook();
        $fh->checkForbidden($cf);
        $this->check($fh instanceof CheckForbiddenHook && $fh->calls === 1, 'CheckForbiddenHook interface');

        $flh = new RecordingFilterHook();
        $flh->filterMessage($fm);
        $this->check($flh instanceof FilterMessageHook && $flh->calls === 1, 'FilterMessageHook interface');
    }

    // ==================================================================== Validators

    private function testValidators(): void
    {
        $this->checkSame(255, Validators::CHARACTER_MAX_LENGTH, 'CHARACTER_MAX_LENGTH=255');
        $this->checkSame(DIRECTORY_SEPARATOR, Validators::FILE_SEPARATOR, 'FILE_SEPARATOR=DIRECTORY_SEPARATOR');

        // checkGroup
        $this->check($this->capture(static fn() => Validators::checkGroup('GID_valid-1')) === null, 'checkGroup valid, no throw');
        $this->checkClientError(static fn() => Validators::checkGroup(null), null, 'the specified group is blank', 'checkGroup null');
        $this->checkClientError(static fn() => Validators::checkGroup(''), null, 'the specified group is blank', 'checkGroup empty string');
        $this->checkClientError(static fn() => Validators::checkGroup('   '), null, 'the specified group is blank', 'checkGroup whitespace');
        $this->checkClientError(static fn() => Validators::checkGroup(str_repeat('a', 121)), null, 'longer than group max length: 120', 'checkGroup too long (121)');
        $this->check($this->capture(static fn() => Validators::checkGroup(str_repeat('a', 120))) === null, 'checkGroup length 120 is legal');
        $this->checkClientError(static fn() => Validators::checkGroup('gid$bad'), null, 'contains illegal characters', 'checkGroup illegal characters');
        $this->checkClientError(static fn() => Validators::checkGroup('组'), null, 'contains illegal characters', 'checkGroup CJK illegal character');

        // checkTopic
        $this->check($this->capture(static fn() => Validators::checkTopic('TopicTest_1')) === null, 'checkTopic valid, no throw');
        $this->checkClientError(static fn() => Validators::checkTopic(null), null, 'The specified topic is blank', 'checkTopic null');
        $this->checkClientError(static fn() => Validators::checkTopic('  '), null, 'The specified topic is blank', 'checkTopic whitespace');
        $this->checkClientError(static fn() => Validators::checkTopic(str_repeat('a', 128)), null, 'longer than topic max length 127', 'checkTopic too long (128)');
        $this->check($this->capture(static fn() => Validators::checkTopic(str_repeat('a', 127))) === null, 'checkTopic length 127 is legal');
        $this->checkClientError(static fn() => Validators::checkTopic('topic$bad'), null, 'contains illegal characters', 'checkTopic illegal characters');
        // CJK character length counts UTF-8 code points:
        // 60 CJK chars = 60 code points ≤ 127 → takes the character-table branch; counting bytes (180) would wrongly flag it as too long
        $this->checkClientError(static fn() => Validators::checkTopic(str_repeat('中', 60)), null, 'contains illegal characters', 'checkTopic 60 CJK chars not too long by code point');
        $this->checkClientError(static fn() => Validators::checkTopic(str_repeat('中', 130)), null, 'longer than topic max length 127', 'checkTopic 130 CJK chars too long by code point');

        // isSystemTopic
        $this->check($this->capture(static fn() => Validators::isSystemTopic('NormalTopic')) === null, 'isSystemTopic normal topic, no throw');
        $this->checkClientError(static fn() => Validators::isSystemTopic('TBW102'), null, 'is conflict with system topic', 'isSystemTopic TBW102');
        $this->checkClientError(static fn() => Validators::isSystemTopic('rmq_sys_x'), null, 'is conflict with system topic', 'isSystemTopic prefix');

        // isNotAllowedSendTopic
        $this->check($this->capture(static fn() => Validators::isNotAllowedSendTopic('NormalTopic')) === null, 'isNotAllowedSendTopic normal topic, no throw');
        $this->check($this->capture(static fn() => Validators::isNotAllowedSendTopic('%RETRY%GID')) === null, 'isNotAllowedSendTopic allows %RETRY%');
        $this->checkClientError(static fn() => Validators::isNotAllowedSendTopic('SCHEDULE_TOPIC_XXXX'), null, 'is forbidden', 'isNotAllowedSendTopic forbidden');

        // checkMessage
        $this->checkClientError(static fn() => Validators::checkMessage(null, 100), ResponseCode::MESSAGE_ILLEGAL, 'the message is null', 'checkMessage null');
        $this->check($this->capture(static fn() => Validators::checkMessage(new Message('TopicTest', 'body'), 100)) === null, 'checkMessage valid, no throw');
        $this->checkClientError(
            static fn() => Validators::checkMessage(new Message('', 'body'), 100),
            null,
            'The specified topic is blank',
            'checkMessage blank topic'
        );
        $this->checkClientError(
            static fn() => Validators::checkMessage(new Message('SCHEDULE_TOPIC_XXXX', 'body'), 100),
            null,
            'is forbidden',
            'checkMessage forbidden topic'
        );
        $nullBody = new Message('TopicTest', 'x');
        $nullBody->body = null;
        $this->checkClientError(static fn() => Validators::checkMessage($nullBody, 100), ResponseCode::MESSAGE_ILLEGAL, 'the message body is null', 'checkMessage body null');
        $this->checkClientError(static fn() => Validators::checkMessage(new Message('TopicTest', ''), 100), ResponseCode::MESSAGE_ILLEGAL, 'the message body length is zero', 'checkMessage body empty');
        $this->checkClientError(
            static fn() => Validators::checkMessage(new Message('TopicTest', str_repeat('x', 11)), 10),
            ResponseCode::MESSAGE_ILLEGAL,
            'the message body size over max value, MAX: 10',
            'checkMessage body over limit'
        );
        $this->check($this->capture(static fn() => Validators::checkMessage(new Message('TopicTest', str_repeat('x', 10)), 10)) === null, 'checkMessage body exactly at the limit is legal');

        $lmqBad = new Message('TopicTest', 'x');
        $lmqBad->putProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH, 'a' . Validators::FILE_SEPARATOR . 'b');
        $this->checkClientError(static fn() => Validators::checkMessage($lmqBad, 100), ResponseCode::MESSAGE_ILLEGAL, 'can not contains', 'checkMessage LMQ path contains a separator');
        $lmqOk = new Message('TopicTest', 'x');
        $lmqOk->putProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH, 'ab');
        $this->check($this->capture(static fn() => Validators::checkMessage($lmqOk, 100)) === null, 'checkMessage LMQ without separator is legal');
    }

    // ==================================================================== ConsumeExecutor

    private function testConsumeExecutor(): void
    {
        $this->check(is_subclass_of(RejectedExecutionError::class, \RuntimeException::class), 'RejectedExecutionError extends RuntimeException');

        $ex = new ConsumeExecutor(1, 3);
        $this->checkSame(1, $ex->getCorePoolSize(), 'getCorePoolSize');
        $this->checkSame(3, $ex->getMaxPoolSize(), 'getMaxPoolSize');
        $this->checkSame(0, $ex->workerCount(), 'initial workerCount=0');
        $this->checkSame(0, $ex->queuedCount(), 'initial queuedCount=0');

        $this->checkSame(0, (new ConsumeExecutor(-5, -3))->getCorePoolSize(), 'negative core clamped to 0');
        $this->checkSame(0, (new ConsumeExecutor(-5, -3))->getMaxPoolSize(), 'max clamped to core');
        $this->checkSame(5, (new ConsumeExecutor(5, 2))->getMaxPoolSize(), 'max < core raised to core');

        // submit only enqueues; pump executes
        $log = [];
        $ex->submit(static function () use (&$log): void {
            $log[] = 'a';
        });
        $this->checkSame(1, $ex->queuedCount(), 'submit enqueues');
        $this->checkSame(1, $ex->workerCount(), 'worker=core after submit');
        $this->checkSame(0, count($log), 'task not executed before pump');
        $this->check(true === $ex->pumpOnce(), 'pumpOnce executes one task');
        $this->checkSame(['a'], $log, 'pumpOnce ran the task');
        $this->check(false === $ex->pumpOnce(), 'pumpOnce returns false on an empty queue');

        // Task with args + FIFO
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
        $this->checkSame('x', $argSeen, 'submit passes arguments');
        $this->checkSame([1, 2], $order, 'pumpAll FIFO');

        // Task exceptions do not kill the worker
        $ex3 = new ConsumeExecutor(1, 1);
        $ex3->submit(static function (): void {
            throw new \RuntimeException('boom');
        });
        $ex3->submit(static function () use (&$order): void {
            $order[] = 'after';
        });
        $ex3->pumpAll();
        $this->checkSame(1, $ex3->handlerExceptionCount(), 'task exception counted');
        $this->checkSame('after', $order[count($order) - 1], 'exception does not block subsequent tasks');
        $this->checkSame(1, $ex3->workerCount(), 'exception does not kill the worker');

        // setCorePoolSize
        $this->checkThrows(static fn() => $ex->setCorePoolSize(-1), \ValueError::class, 'setCorePoolSize(-1) → ValueError');
        $ex4 = new ConsumeExecutor(1, 3);
        $ex4->submit(static function (): void {});
        $ex4->submit(static function (): void {});
        $ex4->submit(static function (): void {});
        $this->checkSame(1, $ex4->workerCount(), 'unbounded queue keeps only core workers');
        $ex4->setCorePoolSize(3);
        $this->checkSame(3, $ex4->getCorePoolSize(), 'setCorePoolSize updates core');
        $this->checkSame(3, $ex4->workerCount(), 'setCorePoolSize tops up workers per queue length');
        $ex4->setCorePoolSize(5);
        $this->checkSame(5, $ex4->getMaxPoolSize(), 'setCorePoolSize raises max when exceeding it');

        // Full bounded queue rejects
        $bounded = new ConsumeExecutor(1, 1, 60.0, 'p', 1);
        $bounded->submit(static function (): void {});
        $this->checkThrows(static fn() => $bounded->submit(static function (): void {}), RejectedExecutionError::class, 'full bounded queue → RejectedExecutionError');

        // Full bounded queue but non-core threads may spawn
        $bounded2 = new ConsumeExecutor(1, 3, 60.0, 'p', 1);
        $bounded2->submit(static function (): void {});
        $bounded2->submit(static function (): void {});
        $bounded2->submit(static function (): void {});
        $this->checkSame(3, $bounded2->workerCount(), 'queue full and growable → spawns non-core threads');
        $e = $this->capture(static fn() => $bounded2->submit(static function (): void {}));
        $this->check($e instanceof RejectedExecutionError && str_contains($e->getMessage(), 'queue is full (1)'), 'rejects at max and reports the queue size');
        // Idle pumping retires over-provisioned workers
        $bounded2->pumpAll();
        $this->checkSame(1, $bounded2->workerCount(), 'keep-alive idling retires over-provisioned workers');

        // shutdown
        $ex5 = new ConsumeExecutor(1, 1);
        $ex5->shutdown(false);
        $this->checkThrows(static fn() => $ex5->submit(static function (): void {}), RejectedExecutionError::class, 'submit rejected after shutdown');
        $ex5->shutdown(false); // idempotent

        $done = [];
        $ex6 = new ConsumeExecutor(1, 1);
        $ex6->submit(static function () use (&$done): void {
            $done[] = 1;
        });
        $ex6->submit(static function () use (&$done): void {
            $done[] = 2;
        });
        $ex6->shutdown(true);
        $this->checkSame([1, 2], $done, 'shutdown(wait=true) drains the queue');
        $this->checkSame(0, $ex6->queuedCount(), 'shutdown(wait=true) empties the queue');
        $this->checkSame(0, $ex6->workerCount(), 'shutdown(wait=true) zeroes the workers');
    }

    // ==================================================================== ConsumerStats

    private function testConsumerStats(): void
    {
        $this->checkSame(10.0, ConsumerStatsManager::SAMPLING_INTERVAL_SECONDS, 'SAMPLING_INTERVAL_SECONDS=10');
        $this->checkSame(600.0, ConsumerStatsManager::HOUR_SAMPLING_INTERVAL_SECONDS, 'HOUR_SAMPLING_INTERVAL_SECONDS=600');
        $this->checkSame(60, ConsumerStatsManager::MINUTE_LIST_MAX, 'MINUTE_LIST_MAX=60');
        $this->checkSame(60, ConsumerStatsManager::HOUR_LIST_MAX, 'HOUR_LIST_MAX=60');

        // StatsSnapshot defaults
        $ss = new StatsSnapshot();
        $this->checkSame(0, $ss->sum, 'StatsSnapshot.sum default 0');
        $this->checkSame(0.0, $ss->tps, 'StatsSnapshot.tps default 0.0');
        $this->checkSame(0.0, $ss->avgpt, 'StatsSnapshot.avgpt default 0.0');
        $this->checkSame(0, $ss->times, 'StatsSnapshot.times default 0');
        $this->check(str_contains((string) $ss, 'StatsSnapshot(sum=0'), 'StatsSnapshot __toString shape');

        // computeStatsData
        $empty = StatsItem::computeStatsData([]);
        $this->checkSame(0, $empty->sum, 'computeStatsData([]) sum=0');
        $this->checkSame(0.0, $empty->tps, 'computeStatsData([]) tps=0');

        $one = StatsItem::computeStatsData([[1000, 50, 5]]);
        $this->checkSame(0, $one->sum, 'computeStatsData single point sum=0');
        $this->checkSame(0.0, $one->tps, 'computeStatsData single point tps=0');
        $this->checkSame(0, $one->times, 'computeStatsData single point times=0');

        $two = StatsItem::computeStatsData([[1000, 10, 1], [3000, 30, 3]]);
        $this->checkSame(20, $two->sum, 'computeStatsData sum=last-first');
        $this->checkSame(10.0, $two->tps, 'computeStatsData tps=sum*1000/span');
        $this->checkSame(2, $two->times, 'computeStatsData times=times diff');
        $this->checkSame(10.0, $two->avgpt, 'computeStatsData avgpt=sum/times');

        $sameTs = StatsItem::computeStatsData([[1000, 10, 1], [1000, 20, 2]]);
        $this->checkSame(10, $sameTs->sum, 'span=0 still computes sum');
        $this->checkSame(0.0, $sameTs->tps, 'span=0 tps=0 (no division by zero)');
        $this->checkSame(10.0, $sameTs->avgpt, 'span=0 avgpt normal');

        $noTimes = StatsItem::computeStatsData([[1000, 10, 0], [2000, 20, 0]]);
        $this->checkSame(10, $noTimes->sum, 'timesDiff=0 sum');
        $this->checkSame(0, $noTimes->times, 'timesDiff=0 times=0');
        $this->checkSame(0.0, $noTimes->avgpt, 'timesDiff=0 avgpt=0');

        // StatsItem accumulation and sampling
        $item = new StatsItem('PULL_RT', 'T@G');
        $this->checkSame('PULL_RT', $item->statsName, 'StatsItem.statsName');
        $this->checkSame('T@G', $item->statsKey, 'StatsItem.statsKey');
        $item->addValue(10, 2);
        $item->addValue(5, 1);
        $this->checkSame(15, $item->getValue(), 'StatsItem accumulates value');
        $this->checkSame(3, $item->getTimes(), 'StatsItem accumulates times');
        $this->checkSame(0, $item->getMinuteSampleCount(), 'minute chain empty before sampling');
        $item->sample();
        usleep(2000);
        $item->addValue(15, 1);
        $item->sample();
        $this->checkSame(2, $item->getMinuteSampleCount(), 'two samples → 2 points');
        $snap = $item->getStatsDataInMinute();
        $this->checkSame(15, $snap->sum, 'minute snapshot sum=window delta');
        $this->checkSame(1, $snap->times, 'minute snapshot times delta');
        $this->checkSame(15.0, $snap->avgpt, 'minute snapshot avgpt');
        $this->check($snap->tps > 0, 'minute snapshot tps>0');
        $this->checkSame(0, $item->getStatsDataInHour()->sum, 'hour chain unsampled → sum=0');

        // Sample-chain overflow (beyond MAX drops the oldest)
        $over = new StatsItem('X', 'k');
        for ($i = 0; $i < 65; $i++) {
            $over->sample();
        }
        $this->checkSame(60, $over->getMinuteSampleCount(), 'minute chain overflow truncated to 60');
        for ($i = 0; $i < 65; $i++) {
            $over->sampleHour();
        }
        $this->checkSame(60, $over->getHourSampleCount(), 'hour chain overflow truncated to 60');

        // StatsItemSet
        $set = new StatsItemSet('PULL_TPS');
        $this->checkSame('PULL_TPS', $set->statsName, 'StatsItemSet.statsName');
        $this->checkSame(null, $set->find('nope'), 'StatsItemSet.find miss → null');
        $a = $set->getAndCreate('k1');
        $this->check($set->getAndCreate('k1') === $a, 'getAndCreate idempotent');
        $set->addValue('k2', 7, 3);
        $this->checkSame([7, 3], [$set->find('k2')->getValue(), $set->find('k2')->getTimes()], 'StatsItemSet.addValue');
        $this->checkSame(['k1', 'k2'], $set->keys(), 'StatsItemSet.keys order');
        $set->sampleAll();
        $this->checkSame(1, $set->find('k2')->getMinuteSampleCount(), 'sampleAll samples everything');
        $set->sampleHourAll();
        $this->checkSame(1, $set->find('k2')->getHourSampleCount(), 'sampleHourAll samples everything');

        // ConsumerStatsManager: key = topic@group
        $mgr = new ConsumerStatsManager();
        $mgr->start();
        $mgr->start(); // idempotent
        $mgr->incPullRt('G', 'T', 20);
        $this->checkSame(20, $mgr->topicAndGroupPullRt->find('T@G')->getValue(), 'incPullRt lands under topic@group');
        $mgr->incPullTps('G', 'T', 4);
        $mgr->incConsumeRt('G', 'T', 30);
        $mgr->incConsumeOkTps('G', 'T', 8);
        $mgr->incConsumeFailedTps('G', 'T', 2);
        $this->checkSame(4, $mgr->topicAndGroupPullTps->find('T@G')->getValue(), 'incPullTps accumulates');
        $this->checkSame(30, $mgr->topicAndGroupConsumeRt->find('T@G')->getValue(), 'incConsumeRt accumulates');

        $mgr->sampleOnce();
        usleep(2000);
        $mgr->incPullRt('G', 'T', 20);
        $mgr->incPullTps('G', 'T', 4);
        $mgr->incConsumeRt('G', 'T', 30);
        $mgr->incConsumeOkTps('G', 'T', 8);
        $mgr->incConsumeFailedTps('G', 'T', 2);
        $mgr->sampleOnce();

        $cs = $mgr->consumeStatus('G', 'T');
        $this->check($cs instanceof ConsumeStatus, 'consumeStatus returns ConsumeStatus');
        $this->checkSame(20.0, $cs->pullRt, 'consumeStatus.pullRt=avgpt');
        $this->checkSame(30.0, $cs->consumeRt, 'consumeStatus.consumeRt=avgpt');
        $this->check($cs->pullTps > 0, 'consumeStatus.pullTps>0');
        $this->check($cs->consumeOkTps > 0, 'consumeStatus.consumeOkTps>0');
        $this->check($cs->consumeFailedTps > 0, 'consumeStatus.consumeFailedTps>0');
        $this->checkSame(0, $cs->consumeFailedMsgs, 'consumeFailedMsgs reads the hour window (unsampled=0)');

        $miss = $mgr->consumeStatus('NOGROUP', 'NOTOPIC');
        $this->checkSame(0.0, $miss->pullRt, 'consumeStatus without data pullRt=0');

        // consumeFailedMsgs reads the failed hour-window sum (cross-window read)
        $mgr2 = new ConsumerStatsManager();
        $failed = $mgr2->topicAndGroupConsumeFailedTps->getAndCreate('T@G');
        $failed->addValue(5, 1);
        $failed->sampleHour();
        usleep(2000);
        $failed->addValue(7, 1);
        $failed->sampleHour();
        $cs2 = $mgr2->consumeStatus('G', 'T');
        $this->checkSame(7, $cs2->consumeFailedMsgs, 'consumeFailedMsgs=hour-window sum');
        $mgr->shutdown();
    }

    // ==================================================================== RequestReply

    private function testRequestReply(): void
    {
        $this->checkSame(3000, RequestResponseFuture::DEFAULT_REQUEST_TIMEOUT_MILLIS, 'DEFAULT_REQUEST_TIMEOUT_MILLIS=3000');

        $f = new RequestResponseFuture('cid-1', 3000);
        $this->checkSame('cid-1', $f->correlationId, 'RequestResponseFuture.correlationId');
        $this->checkSame(3000, $f->timeoutMillis, 'RequestResponseFuture.timeoutMillis');
        $this->checkSame(null, $f->requestCallback, 'default callback=null');
        $this->checkSame(null, $f->responseMsg, 'default responseMsg=null');
        $this->check(true === $f->sendRequestOk, 'default sendRequestOk=true');
        $this->checkSame(null, $f->cause, 'default cause=null');
        $this->check($f->beginTimestamp > 0, 'beginTimestamp is set');
        $this->check(false === $f->isTimeout(), 'not timed out');
        $this->checkSame(null, $f->waitResponseMessage(100), 'wait returns null when not delivered');

        $msg = new Message('T', 'r');
        $f->putResponseMessage($msg);
        $this->check($f->waitResponseMessage(100) === $msg, 'wait retrieves after putResponseMessage');

        $this->check(true === (new RequestResponseFuture('c', -1))->isTimeout(), 'timeout<0 → isTimeout');
        $this->check(false === (new RequestResponseFuture('c', 100000))->isTimeout(), 'large timeout → not timed out');

        // executeRequestCallback fires only once
        $cb = new RecordingRequestCallback();
        $f2 = new RequestResponseFuture('c2', 3000, $cb);
        $f2->putResponseMessage($msg);
        $f2->executeRequestCallback();
        $f2->executeRequestCallback();
        $this->checkSame(1, $cb->successCount, 'success callback fires only once');
        $this->checkSame(0, $cb->exceptionCount, 'success path has no exception callback');
        $this->check($cb->lastResponse === $msg, 'success callback received the response');

        // sendRequestOk=false → onException(cause)
        $cb2 = new RecordingRequestCallback();
        $f3 = new RequestResponseFuture('c3', 3000, $cb2);
        $f3->sendRequestOk = false;
        $cause = new \RuntimeException('send failed');
        $f3->cause = $cause;
        $f3->executeRequestCallback();
        $this->checkSame(1, $cb2->exceptionCount, 'sendRequestOk=false → onException');
        $this->check($cb2->lastCause === $cause, 'onException received the cause');

        // Non-null cause routes to the exception path even when sendRequestOk=true
        $cb3 = new RecordingRequestCallback();
        $f4 = new RequestResponseFuture('c4', 3000, $cb3);
        $f4->cause = new \RuntimeException('x');
        $f4->executeRequestCallback();
        $this->checkSame(1, $cb3->exceptionCount, 'non-null cause → onException');
        $this->checkSame(0, $cb3->successCount, 'non-null cause skips the success path');

        // No-op when callback is empty
        $f5 = new RequestResponseFuture('c5', 3000);
        $this->check($this->capture(static fn() => $f5->executeRequestCallback()) === null, 'no callback → no-op');

        // RequestFutureHolder
        $h = new RequestFutureHolder();
        $h->putRequest('c', $f);
        $this->check($h->getRequest('c') === $f, 'putRequest/getRequest');
        $this->checkSame(null, $h->getRequest('nope'), 'getRequest miss → null');
        $this->check($h->removeRequest('c') === $f, 'removeRequest returns the removed entry');
        $this->checkSame(null, $h->removeRequest('c'), 'second removeRequest → null');

        $cb4 = new RecordingRequestCallback();
        $f6 = new RequestResponseFuture('c6', 3000, $cb4);
        $h->putRequest('c6', $f6);
        $got = $h->putResponse('c6', $msg);
        $this->check($got === $f6, 'putResponse returns the filled future');
        $this->check($f6->responseMsg === $msg, 'putResponse fills responseMsg');
        $this->checkSame(null, $h->getRequest('c6'), 'putResponse removes the entry from the table');
        $this->checkSame(1, $cb4->successCount, 'putResponse triggers the success callback');
        $this->checkSame(null, $h->putResponse('c6', $msg), 'duplicate putResponse returns null');

        $this->check(RequestFutureHolder::getInstance() === RequestFutureHolder::getInstance(), 'getInstance singleton');

        // CorrelationIdUtil
        $cid = CorrelationIdUtil::createCorrelationId();
        $this->check(preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/', $cid) === 1, 'createCorrelationId is a v4 UUID');
        $this->check(CorrelationIdUtil::createCorrelationId() !== $cid, 'createCorrelationId differs each call');

        // MessageUtil
        $plain = new Message('T', 'b');
        $this->check(false === MessageUtil::isReplyMessage($plain), 'isReplyMessage false for a normal message');
        $plain->putProperty(MessageConst::PROPERTY_MESSAGE_TYPE, MixAll::REPLY_MESSAGE_FLAG);
        $this->check(true === MessageUtil::isReplyMessage($plain), 'isReplyMessage true for a reply');

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
            'createReplyMessage without CLUSTER'
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
        $this->check(true === MessageUtil::isReplyMessage($reply), 'createReplyMessage result isReplyMessage');

        // reactCallback
        $called = null;
        RequestReply::reactCallback(static function (?Message $m, ?\Throwable $c) use (&$called): void {
            $called = [$m, $c];
        }, $f6);
        $this->check(is_array($called) && $called[0] === $msg && $called[1] === null, 'reactCallback passes (response, cause)');
        $this->check($this->capture(static fn() => RequestReply::reactCallback(null, $f6)) === null, 'reactCallback(null) no-op');
    }

    // ==================================================================== TraceContextPropagator

    private function testTraceContext(): void
    {
        $this->checkSame('traceparent', TraceContextPropagator::TRACE_CONTEXT_PROPERTY, 'TRACE_CONTEXT_PROPERTY');
        $this->checkSame('tracestate', TraceContextPropagator::TRACE_STATE_PROPERTY, 'TRACE_STATE_PROPERTY');

        $tp = TraceContextPropagator::generateTraceparent();
        $this->check(preg_match('/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/', $tp) === 1, 'generateTraceparent format');
        $this->check(true === TraceContextPropagator::isValidTraceparent($tp), 'generated value is valid');
        $this->check(TraceContextPropagator::generateTraceparent() !== $tp, 'generateTraceparent is random');

        $this->check(false === TraceContextPropagator::isValidTraceparent(null), 'null invalid');
        $this->check(false === TraceContextPropagator::isValidTraceparent(''), 'empty string invalid');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-abc'), 'too few segments invalid');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-abc-def-01-extra'), 'too many segments invalid');
        $this->check(false === TraceContextPropagator::isValidTraceparent('ff-' . str_repeat('a', 32) . '-' . str_repeat('b', 16) . '-01'), 'version=ff invalid');
        $this->check(false === TraceContextPropagator::isValidTraceparent('zzz-' . str_repeat('a', 32) . '-' . str_repeat('b', 16) . '-01'), 'version wrong length/characters');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-' . str_repeat('0', 32) . '-' . str_repeat('b', 16) . '-01'), 'all-zero trace-id invalid');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-' . str_repeat('a', 32) . '-' . str_repeat('0', 16) . '-01'), 'all-zero parent-id invalid');
        $this->check(false === TraceContextPropagator::isValidTraceparent('00-' . str_repeat('a', 31) . '-' . str_repeat('b', 16) . '-01'), 'wrong trace-id length invalid');
        $this->check(true === TraceContextPropagator::isValidTraceparent(strtoupper($tp)), 'uppercase hex accepted leniently');
        $this->check(true === TraceContextPropagator::isValidTraceparent('00-' . str_repeat('a', 32) . '-' . str_repeat('b', 16) . '-00'), 'all-zero flags legal');
        $this->check(true === TraceContextPropagator::isValidTraceparent('01-' . str_repeat('a', 32) . '-' . str_repeat('b', 16) . '-01'), 'non-00 two-hex-digit version legal');
        $this->check(true === TraceContextPropagator::isValidTraceparent('  ' . $tp . '  '), 'outer whitespace tolerated');

        // childTraceparent
        $child = TraceContextPropagator::childTraceparent($tp);
        $this->check($child !== null && $child !== $tp, 'childTraceparent swaps the parent-id');
        $this->check(true === TraceContextPropagator::isValidTraceparent($child), 'child is valid');
        $this->checkSame(explode('-', $tp)[1], explode('-', (string) $child)[1], 'child keeps the trace-id');
        $this->checkSame(null, TraceContextPropagator::childTraceparent(null), 'childTraceparent(null) → null');
        $this->checkSame(null, TraceContextPropagator::childTraceparent('bad'), 'childTraceparent(invalid) → null');

        // injectTraceContext
        $m = new Message('T', 'b');
        $injected = TraceContextPropagator::injectTraceContext($m);
        $this->check(true === TraceContextPropagator::isValidTraceparent($injected), 'injected value is valid');
        $this->checkSame($injected, $m->getProperty(TraceContextPropagator::TRACE_CONTEXT_PROPERTY), 'injection written back to the property');
        $this->checkSame($injected, TraceContextPropagator::injectTraceContext($m), 'existing value not overwritten');
        $m2 = new Message('T', 'b');
        $m2->putProperty(TraceContextPropagator::TRACE_CONTEXT_PROPERTY, 'custom-tp');
        $this->checkSame('custom-tp', TraceContextPropagator::injectTraceContext($m2), 'upstream context takes precedence');

        // extractTraceparent
        $ext = new MessageExt();
        $ext->putProperty(TraceContextPropagator::TRACE_CONTEXT_PROPERTY, 'tp-value');
        $this->checkSame('tp-value', TraceContextPropagator::extractTraceparent($ext), 'extractTraceparent reads the value');
        $this->checkSame(null, TraceContextPropagator::extractTraceparent(new MessageExt()), 'extractTraceparent no value → null');
        $ext2 = new MessageExt();
        $ext2->putProperty(TraceContextPropagator::TRACE_CONTEXT_PROPERTY, '');
        $this->checkSame(null, TraceContextPropagator::extractTraceparent($ext2), 'extractTraceparent empty string → null');

        // env switch
        putenv('ROCKETMQ_TRACE_CONTEXT_ENABLE');
        $this->check(false === TraceContextPropagator::traceContextEnabledFromEnv(), 'env unset → false');
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
        Logger::setHandler(static function (string $line): void {}); // silence (including task exception logs)
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
