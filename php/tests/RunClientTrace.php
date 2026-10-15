<?php

declare(strict_types=1);

/*
 * =====================================================================================
 * Message trace self-test: pure assert style (no phpunit); on success prints
 * `ALL TESTS PASSED (N checks)`, on failure exits non-zero.
 * -------------------------------------------------------------------------------------
 * Depends on php/src/Client/{Hook,SendResult,ConsumerResult}.php; this suite **uses the real
 * implementations directly**. The conditional stubs in namespace RocketMQ\Client below are only
 * a fallback for when those real files are missing: during live/full regression runs they exist,
 * and class_exists / interface_exists / enum_exists hit the real definitions so all stubs are
 * skipped. **No real source file is modified**.
 * Note: in Hook.php SendMessageHook/ConsumeMessageHook/EndTransactionHook are
 * **interfaces**, so their fallback checks must use interface_exists (class_exists would return
 * false for an interface and cause a name-collision redeclaration).
 * -------------------------------------------------------------------------------------
 * Name conflict (resolved): php/src/Client/TraceContext.php (the W3C traceparent helper)
 * previously shared **the same RocketMQ\Client\TraceContext name** with the message-trace
 * context TraceContext,
 * and a direct PSR-4 hit would let the former always win. The W3C one has been renamed to
 * `RocketMQ\Client\TraceContextPropagator` (file renamed accordingly), so this suite **no longer
 * needs** to require Trace.php explicitly and goes through the real autoload path (closer to
 * production).
 * =====================================================================================
 */
namespace RocketMQ\Client {
    require_once __DIR__ . '/../bootstrap.php';

    if (!class_exists(SendMessageContext::class)) {
        class SendMessageContext
        {
            public mixed $producer = null;
            public string $producerGroup = '';
            public mixed $message = null;
            public mixed $mq = null;
            public string $brokerAddr = '';
            public string $bornHost = '';
            public mixed $communicationMode = null;
            public mixed $sendResult = null;
            public ?\Throwable $exception = null;
            public mixed $mqTraceContext = null;
            public mixed $props = null;
            public \RocketMQ\Common\MessageType $msgType = \RocketMQ\Common\MessageType::NORMAL_MSG;
            public string $namespace = '';
        }
    }

    if (!interface_exists(SendMessageHook::class)) {
        class SendMessageHook
        {
            public function hookName(): string
            {
                return '';
            }

            public function sendMessageBefore(SendMessageContext $context): void
            {
            }

            public function sendMessageAfter(SendMessageContext $context): void
            {
            }
        }
    }

    if (!class_exists(ConsumeMessageContext::class)) {
        class ConsumeMessageContext
        {
            public mixed $mq = null;
            public bool $success = true;
            public ?string $status = null;
            public mixed $mqTraceContext = null;
            public mixed $props = null;
            public mixed $accessChannel = null;

            /** @param list<mixed> $msgList */
            public function __construct(
                public string $consumerGroup = '',
                public array $msgList = [],
                mixed $mq = null,
            ) {
                $this->mq = $mq;
            }
        }
    }

    if (!interface_exists(ConsumeMessageHook::class)) {
        class ConsumeMessageHook
        {
            public function hookName(): string
            {
                return '';
            }

            public function consumeMessageBefore(ConsumeMessageContext $context): void
            {
            }

            public function consumeMessageAfter(ConsumeMessageContext $context): void
            {
            }
        }
    }

    if (!class_exists(EndTransactionContext::class)) {
        class EndTransactionContext
        {
            public string $producerGroup = '';
            public mixed $message = null;
            public string $brokerAddr = '';
            public ?string $msgId = null;
            public ?string $transactionId = null;
            public mixed $transactionState = null;
            public bool $fromTransactionCheck = false;
            public string $namespace = '';
        }
    }

    if (!interface_exists(EndTransactionHook::class)) {
        class EndTransactionHook
        {
            public function hookName(): string
            {
                return '';
            }

            public function endTransaction(EndTransactionContext $context): void
            {
            }
        }
    }

    if (!enum_exists(SendStatus::class)) {
        enum SendStatus: int
        {
            case SEND_OK = 0;
            case FLUSH_DISK_TIMEOUT = 1;
            case FLUSH_SLAVE_TIMEOUT = 2;
            case SLAVE_NOT_AVAILABLE = 3;
        }
    }

    if (!enum_exists(ConsumeReturnType::class)) {
        enum ConsumeReturnType: int
        {
            case SUCCESS = 0;
            case TIME_OUT = 1;
            case EXCEPTION = 2;
            case RETURNNULL = 3;
            case FAILED = 4;
        }
    }
}

namespace {

    use RocketMQ\Client\AccessChannel;
    use RocketMQ\Client\AsyncTraceDispatcher;
    use RocketMQ\Client\ConsumeMessageContext;
    use RocketMQ\Client\ConsumeMessageTraceHook;
    use RocketMQ\Client\EndTransactionContext;
    use RocketMQ\Client\EndTransactionTraceHook;
    use RocketMQ\Client\SendMessageContext;
    use RocketMQ\Client\SendMessageTraceHook;
    use RocketMQ\Client\SendStatus;
    use RocketMQ\Client\TraceBean;
    use RocketMQ\Client\TraceConstants;
    use RocketMQ\Client\TraceContext;
    use RocketMQ\Client\TraceDataEncoder;
    use RocketMQ\Client\TraceDispatcherType;
    use RocketMQ\Client\TraceTransferBean;
    use RocketMQ\Client\TraceType;
    use RocketMQ\Common\MessageConst;
    use RocketMQ\Common\MessageExt;
    use RocketMQ\Common\MessageQueue;
    use RocketMQ\Common\MessageType;
    use RocketMQ\Common\MixAll;

    const SOH = "\x01";
    const STX = "\x02";

    const MSG_ID_1 = 'AC1400A1F0A018B4AAC2A1B2C3D4E5F6';
    const MSG_ID_2 = 'AC1400A1F0A018B4AAC2A1B2C3D4E5F7';
    const OFFSET_MSG_ID = 'AC1400A1000027100000000000000001';
    const BODY = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; // 42 bytes

    // ---- Fixed vectors of the trace-log encoding (do not hand-edit; changing them breaks
    // compatibility with the trace console) ----
    // define() turns them into global constants: class methods cannot see file-level $variables
    // but can see global constants.
    define('EXPECTED_PUB', implode(SOH, [
        'Pub', '1700000000000', 'DefaultRegion', 'GID_test', 'TopicTest',
        MSG_ID_1, 'TagA', 'KeyA KeyB', '127.0.0.1:10911', '42', '7', '0',
        OFFSET_MSG_ID, 'true',
    ]) . STX);
    define('EXPECTED_SUB_BEFORE', implode(SOH, [
        'SubBefore', '1700000000000', 'DefaultRegion', 'CID_test', 'REQ-SUB-001',
        MSG_ID_1, '2', 'KeyA KeyB',
    ]) . STX
        . implode(SOH, [
            'SubBefore', '1700000000000', 'DefaultRegion', 'CID_test', 'REQ-SUB-001',
            MSG_ID_2, '0', 'KeyC',
        ]) . STX);
    define('EXPECTED_SUB_AFTER', implode(SOH, [
        'SubAfter', 'REQ-SUB-001', MSG_ID_1, '11', 'false',
        'KeyA KeyB', '2', '1700000000000', 'CID_test',
    ]) . STX);
    define('EXPECTED_END_TRANSACTION', implode(SOH, [
        'EndTransaction', '1700000000000', 'DefaultRegion',
        'GID_test', 'TopicTest', MSG_ID_1, 'TagA', 'KeyA KeyB',
        '127.0.0.1:10911', '0', 'TRAN-001', 'COMMIT_MESSAGE',
        'false',
    ]) . STX);
    define('EXPECTED_RECALL', implode(SOH, [
        'Recall', '1700000000000', 'DefaultRegion', 'GID_test',
        'TopicTest', MSG_ID_1, 'true',
    ]) . STX);

    // ---- Test doubles (no cluster/thread dependency) ----

    /** Double: records send calls only, no network. */
    final class FakeTraceProducer
    {
        /** @var list<array{0:string,1:?string,2:?string}> */
        public array $sent = [];
        public bool $stopped = false;

        public function topicPublishInfo(string $topic): mixed
        {
            throw new \RuntimeException('no route in unit test');
        }

        public function send(mixed $msg, int $timeout = 0): mixed
        {
            $this->sent[] = [$msg->topic, $msg->body, $msg->getKeys()];
            return null;
        }

        public function sendBySelector(mixed $msg, mixed $selector, mixed $arg, int $timeout = 0): mixed
        {
            $this->sent[] = [$msg->topic, $msg->body, $msg->getKeys()];
            return null;
        }

        public function shutdown(): void
        {
            $this->stopped = true;
        }
    }

    /** Double: records append calls only. */
    final class CapturingDispatcher
    {
        /** @var list<TraceContext> */
        public array $appended = [];

        public function getTraceTopicName(): string
        {
            return MixAll::TRACE_TOPIC;
        }

        public function clientId(): string
        {
            return 'CID@1';
        }

        public function append(mixed $ctx): bool
        {
            $this->appended[] = $ctx;
            return true;
        }
    }

    /**
     * Test-double SendResult: the hook accesses SendResult duck-typed by property (no type
     * constraint), so this suite does not depend on the exact constructor signature of the
     * parallel sub-task's SendResult.
     */
    final class TestSendResult
    {
        public function __construct(
            public SendStatus $sendStatus = SendStatus::SEND_OK,
            public ?string $msgId = null,
            public ?string $offsetMsgId = null,
            public ?string $regionId = null,
            public bool $traceOn = true,
        ) {
        }
    }

    final class RunClientTrace
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

        /** @param class-string<\Throwable> $exceptionClass */
        public function checkThrows(callable $fn, string $exceptionClass, string $label): void
        {
            try {
                $fn();
                $this->check(false, "{$label} (no exception thrown)");
            } catch (\Throwable $e) {
                $this->check($e instanceof $exceptionClass, sprintf('%s (got %s: %s)', $label, get_class($e), $e->getMessage()));
            }
        }

        // ============================================================ Construction helpers

        private static function bean(
            string $msgId = MSG_ID_1,
            string $keys = 'KeyA KeyB',
            int $retryTimes = 2
        ): TraceBean {
            $b = new TraceBean();
            $b->topic = 'TopicTest';
            $b->msgId = $msgId;
            $b->offsetMsgId = OFFSET_MSG_ID;
            $b->tags = 'TagA';
            $b->keys = $keys;
            $b->storeHost = '127.0.0.1:10911';
            $b->storeTime = 1700000000123;
            $b->retryTimes = $retryTimes;
            $b->bodyLength = 42;
            $b->msgType = MessageType::NORMAL_MSG;
            $b->transactionId = 'TRAN-001';
            $b->transactionState = 'COMMIT_MESSAGE';
            return $b;
        }

        private static function pubContext(
            string $topic = 'TopicTest',
            string $msgId = MSG_ID_1,
            string $region = 'DefaultRegion'
        ): TraceContext {
            $ctx = new TraceContext();
            $ctx->traceType = TraceType::PUB;
            $ctx->timeStamp = 1700000000000;
            $ctx->regionId = $region;
            $ctx->groupName = 'GID_test';
            $ctx->costTime = 7;
            $ctx->isSuccess = true;
            $bean = self::bean(msgId: $msgId);
            $bean->topic = $topic;
            $ctx->traceBeans = [$bean];
            return $ctx;
        }

        private static function makeDispatcher(): AsyncTraceDispatcher
        {
            $d = new AsyncTraceDispatcher('GID_test', TraceDispatcherType::PRODUCE, 10, null, null);
            $d->traceProducer = new FakeTraceProducer();
            return $d;
        }

        private static function sendContext(
            string $topic = 'TopicTest',
            bool $traceOn = true,
            ?string $region = 'DefaultRegion'
        ): SendMessageContext {
            $ctx = new SendMessageContext();
            $ctx->producerGroup = 'GID_test';
            $ctx->brokerAddr = '127.0.0.1:10911';
            $msg = new MessageExt($topic, BODY, 'TagA', 'KeyA');
            $msg->setKeys('KeyA KeyB');
            $ctx->message = $msg;
            $ctx->mq = new MessageQueue($topic, 'broker-a', 0);
            $ctx->msgType = MessageType::NORMAL_MSG;
            $ctx->sendResult = new TestSendResult(
                SendStatus::SEND_OK,
                MSG_ID_1,
                OFFSET_MSG_ID,
                $region,
                $traceOn
            );
            return $ctx;
        }

        private static function consumeContext(
            string $topic = 'TopicTest',
            ?string $traceOn = null,
            ?string $region = 'DefaultRegion'
        ): ConsumeMessageContext {
            $msg = new MessageExt($topic, BODY, 'TagA', 'KeyA');
            $msg->msgId = MSG_ID_1;
            $msg->storeTimestamp = 1700000000000;
            $msg->storeSize = 42;
            $msg->reconsumeTimes = 1;
            if ($region !== null) {
                $msg->putProperty(MessageConst::PROPERTY_MSG_REGION, $region);
            }
            if ($traceOn !== null) {
                $msg->putProperty(MessageConst::PROPERTY_TRACE_SWITCH, $traceOn);
            }
            $ctx = new ConsumeMessageContext('CID_test', [$msg], new MessageQueue($topic, 'broker-a', 0));
            $ctx->props = ['ConsumeContextType' => 'SUCCESS'];
            $ctx->success = true;
            return $ctx;
        }

        // ============================================================ Constants and enums

        private function testConstantsAndEnums(): void
        {
            $this->checkSame("\x01", TraceConstants::CONTENT_SPLITOR, 'CONTENT_SPLITOR=\\x01');
            $this->checkSame("\x02", TraceConstants::FIELD_SPLITOR, 'FIELD_SPLITOR=\\x02');
            $this->checkSame('_INNER_TRACE_PRODUCER', TraceConstants::GROUP_NAME_PREFIX, 'GROUP_NAME_PREFIX');
            $this->checkSame('PID_CLIENT_INNER_TRACE_PRODUCER', TraceConstants::TRACE_INSTANCE_NAME, 'TRACE_INSTANCE_NAME');
            $this->checkSame('rmq_sys_TRACE_DATA_', TraceConstants::TRACE_TOPIC_PREFIX, 'TRACE_TOPIC_PREFIX');
            $this->checkSame('To_', TraceConstants::TO_PREFIX, 'TO_PREFIX');
            $this->checkSame('From_', TraceConstants::FROM_PREFIX, 'FROM_PREFIX');
            $this->checkSame('EndTransaction', TraceConstants::END_TRANSACTION, 'END_TRANSACTION constant');
            $this->checkSame('rocketmq', TraceConstants::ROCKETMQ_SERVICE, 'ROCKETMQ_SERVICE');
            $this->checkSame('rocketmq.success', TraceConstants::ROCKETMQ_SUCCESS, 'ROCKETMQ_SUCCESS');
            $this->checkSame('rocketmq.tags', TraceConstants::ROCKETMQ_TAGS, 'ROCKETMQ_TAGS');
            $this->checkSame('rocketmq.keys', TraceConstants::ROCKETMQ_KEYS, 'ROCKETMQ_KEYS');
            $this->checkSame('rocketmq.store_host', TraceConstants::ROCKETMQ_STORE_HOST, 'ROCKETMQ_STORE_HOST');
            $this->checkSame('rocketmq.body_length', TraceConstants::ROCKETMQ_BODY_LENGTH, 'ROCKETMQ_BODY_LENGTH');
            $this->checkSame('rocketmq.mgs_id', TraceConstants::ROCKETMQ_MSG_ID, 'ROCKETMQ_MSG_ID (the protocol field name carries the mgs typo)');
            $this->checkSame('rocketmq.mgs_type', TraceConstants::ROCKETMQ_MSG_TYPE, 'ROCKETMQ_MSG_TYPE (the protocol field name carries the mgs typo)');
            $this->checkSame('rocketmq.region_id', TraceConstants::ROCKETMQ_REGION_ID, 'ROCKETMQ_REGION_ID');
            $this->checkSame('rocketmq.transaction_id', TraceConstants::ROCKETMQ_TRANSACTION_ID, 'ROCKETMQ_TRANSACTION_ID');
            $this->checkSame('rocketmq.transaction_state', TraceConstants::ROCKETMQ_TRANSACTION_STATE, 'ROCKETMQ_TRANSACTION_STATE');
            $this->checkSame('rocketmq.is_from_transaction_check', TraceConstants::ROCKETMQ_IS_FROM_TRANSACTION_CHECK, 'ROCKETMQ_IS_FROM_TRANSACTION_CHECK');
            $this->checkSame('rocketmq.retry_times', TraceConstants::ROCKETMQ_RETRY_TIMERS, 'ROCKETMQ_RETRY_TIMERS');

            $this->checkSame('Pub', TraceType::PUB->value, 'TraceType.PUB');
            $this->checkSame('Recall', TraceType::RECALL->value, 'TraceType.RECALL');
            $this->checkSame('SubBefore', TraceType::SUB_BEFORE->value, 'TraceType.SUB_BEFORE');
            $this->checkSame('SubAfter', TraceType::SUB_AFTER->value, 'TraceType.SUB_AFTER');
            $this->checkSame('EndTransaction', TraceType::END_TRANSACTION->value, 'TraceType.END_TRANSACTION');
            $this->checkSame(5, count(TraceType::cases()), 'TraceType has 5 entries');

            $this->checkSame('LOCAL', AccessChannel::LOCAL->value, 'AccessChannel.LOCAL');
            $this->checkSame('CLOUD', AccessChannel::CLOUD->value, 'AccessChannel.CLOUD');

            $this->checkSame('PRODUCE', TraceDispatcherType::PRODUCE->value, 'TraceDispatcherType.PRODUCE');
            $this->checkSame('CONSUME', TraceDispatcherType::CONSUME->value, 'TraceDispatcherType.CONSUME');

            // Common constants/enums the encoding semantics depend on
            $this->checkSame('RMQ_SYS_TRACE_TOPIC', MixAll::TRACE_TOPIC, 'MixAll.TRACE_TOPIC');
            $this->checkSame('DefaultRegion', MixAll::DEFAULT_TRACE_REGION_ID, 'MixAll.DEFAULT_TRACE_REGION_ID');
            $this->checkSame(' ', MessageConst::KEY_SEPARATOR, 'MessageConst.KEY_SEPARATOR');
            $this->checkSame('TRACE_ON', MessageConst::PROPERTY_TRACE_SWITCH, 'MessageConst.PROPERTY_TRACE_SWITCH');
            $this->checkSame('MSG_REGION', MessageConst::PROPERTY_MSG_REGION, 'MessageConst.PROPERTY_MSG_REGION');
            $this->checkSame([0, 1, 2, 3, 4], [
                MessageType::NORMAL_MSG->value, MessageType::TRANS_MSG_HALF->value,
                MessageType::TRANS_MSG_COMMIT->value, MessageType::DELAY_MSG->value,
                MessageType::ORDER_MSG->value,
            ], 'MessageType value order = protocol enum ordinals');
            $this->checkSame('Normal', MessageType::NORMAL_MSG->shortName(), 'MessageType.NORMAL_MSG.shortName');
            $this->checkSame('Trans', MessageType::TRANS_MSG_HALF->shortName(), 'MessageType.TRANS_MSG_HALF.shortName');
            $this->checkSame('TransCommit', MessageType::TRANS_MSG_COMMIT->shortName(), 'MessageType.TRANS_MSG_COMMIT.shortName');
            $this->checkSame('Delay', MessageType::DELAY_MSG->shortName(), 'MessageType.DELAY_MSG.shortName');
            $this->checkSame('Order', MessageType::ORDER_MSG->shortName(), 'MessageType.ORDER_MSG.shortName');
        }

        private function testDefaults(): void
        {
            $tb = new TraceTransferBean();
            $this->checkSame('', $tb->transData, 'TraceTransferBean.transData defaults to empty string');
            $this->checkSame([], $tb->transKey, 'TraceTransferBean.transKey defaults to empty set');

            $a = new TraceContext();
            $b = new TraceContext();
            $this->check($a->requestId !== $b->requestId, 'TraceContext.requestId is unique');
            $this->checkSame(32, strlen($a->requestId), 'TraceContext.requestId length 32');
            $this->checkSame(true, $a->isSuccess, 'TraceContext.isSuccess defaults to true');
            $this->checkSame([], $a->traceBeans, 'TraceContext.traceBeans defaults to empty');
            $this->check(abs($a->timeStamp - \RocketMQ\Common\UtilAll::currentTimeMillis()) < 5000, 'TraceContext.timeStamp is approximately the current millis');
            $this->checkSame(AccessChannel::LOCAL, $a->accessChannelOrLocal(), 'accessChannelOrLocal defaults to LOCAL');
            $a->accessChannel = AccessChannel::CLOUD;
            $this->checkSame(AccessChannel::CLOUD, $a->accessChannelOrLocal(), 'accessChannelOrLocal respects an explicit CLOUD');

            $bean = new TraceBean();
            $this->checkSame('', $bean->topic, 'TraceBean.topic defaults to empty');
            $this->check($bean->storeHost !== '' && $bean->storeHost === $bean->clientHost, 'TraceBean storeHost/clientHost default to the same LOCAL_ADDRESS');
            $this->checkSame($bean->storeHost, TraceBean::localAddress(), 'TraceBean.localAddress() matches the default value');
            $this->checkSame(null, $bean->msgType, 'TraceBean.msgType defaults to null');
            $this->checkSame(false, $bean->fromTransactionCheck, 'TraceBean.fromTransactionCheck defaults to false');

            $this->check($a->__toString() !== '', 'TraceContext.__toString renders');
        }

        // ============================================================ Encoding (byte-exact)

        private function testEncodePubByteExact(): void
        {
            $expected = implode(SOH, [
                'Pub', '1700000000000', 'DefaultRegion', 'GID_test', 'TopicTest',
                MSG_ID_1, 'TagA', 'KeyA KeyB', '127.0.0.1:10911', '42', '7', '0',
                OFFSET_MSG_ID, 'true',
            ]) . STX;

            $ctx = new TraceContext();
            $ctx->traceType = TraceType::PUB;
            $ctx->timeStamp = 1700000000000;
            $ctx->regionId = 'DefaultRegion';
            $ctx->groupName = 'GID_test';
            $ctx->costTime = 7;
            $ctx->isSuccess = true;
            $ctx->requestId = 'REQ-PUB-001';
            $ctx->traceBeans = [self::bean()];

            $tb = TraceDataEncoder::encoderFromContextBean($ctx);
            $this->checkSame($expected, $tb->transData, 'PUB encoding byte-exact = the fixed vector');
            $keys = array_keys($tb->transKey);
            sort($keys, SORT_STRING);
            $this->checkSame([MSG_ID_1, 'KeyA', 'KeyB'], $keys, 'PUB transKey = {msgId, KeyA, KeyB}');
        }

        private function testEncodeSubBeforeMultiContextByteExact(): void
        {
            $expected = implode(SOH, [
                'SubBefore', '1700000000000', 'DefaultRegion', 'CID_test', 'REQ-SUB-001',
                MSG_ID_1, '2', 'KeyA KeyB',
            ]) . STX
                . implode(SOH, [
                    'SubBefore', '1700000000000', 'DefaultRegion', 'CID_test', 'REQ-SUB-001',
                    MSG_ID_2, '0', 'KeyC',
                ]) . STX;

            $ctx = new TraceContext();
            $ctx->traceType = TraceType::SUB_BEFORE;
            $ctx->timeStamp = 1700000000000;
            $ctx->regionId = 'DefaultRegion';
            $ctx->groupName = 'CID_test';
            $ctx->requestId = 'REQ-SUB-001';
            $ctx->traceBeans = [self::bean(), self::bean(msgId: MSG_ID_2, keys: 'KeyC', retryTimes: 0)];

            $tb = TraceDataEncoder::encoderFromContextBean($ctx);
            $this->checkSame($expected, $tb->transData, 'SUB_BEFORE multi-context byte-exact = the fixed vector');
            $keys = array_keys($tb->transKey);
            sort($keys, SORT_STRING);
            $this->checkSame([MSG_ID_1, MSG_ID_2, 'KeyA', 'KeyB', 'KeyC'], $keys, 'SUB_BEFORE transKey merges both entries');
        }

        private function testEncodeSubAfterByteExact(): void
        {
            $expectedLocal = implode(SOH, [
                'SubAfter', 'REQ-SUB-001', MSG_ID_1, '11', 'false',
                'KeyA KeyB', '2', '1700000000000', 'CID_test',
            ]) . STX;
            $expectedCloud = implode(SOH, [
                'SubAfter', 'REQ-SUB-001', MSG_ID_1, '11', 'false', 'KeyA KeyB', '2',
            ]) . STX;

            $ctx = new TraceContext();
            $ctx->traceType = TraceType::SUB_AFTER;
            $ctx->timeStamp = 1700000000000;
            $ctx->groupName = 'CID_test';
            $ctx->requestId = 'REQ-SUB-001';
            $ctx->costTime = 11;
            $ctx->isSuccess = false;
            $ctx->contextCode = 2;
            $ctx->accessChannel = AccessChannel::LOCAL;
            $ctx->traceBeans = [self::bean()];
            $this->checkSame($expectedLocal, TraceDataEncoder::encoderFromContextBean($ctx)->transData, 'SUB_AFTER(LOCAL) byte-exact = the fixed vector (with timestamp+group)');

            $ctx->accessChannel = AccessChannel::CLOUD;
            $this->checkSame($expectedCloud, TraceDataEncoder::encoderFromContextBean($ctx)->transData, 'SUB_AFTER(CLOUD) drops the timestamp+group segments');

            // accessChannel null -> handled as LOCAL (a deliberate robustness fallback: default to
            // LOCAL instead of throwing on a null)
            $ctx->accessChannel = null;
            $this->checkSame($expectedLocal, TraceDataEncoder::encoderFromContextBean($ctx)->transData, 'SUB_AFTER(accessChannel=null) handled as LOCAL');
        }

        private function testEncodeEndTransactionAndRecallByteExact(): void
        {
            $endTx = new TraceContext();
            $endTx->traceType = TraceType::END_TRANSACTION;
            $endTx->timeStamp = 1700000000000;
            $endTx->regionId = 'DefaultRegion';
            $endTx->groupName = 'GID_test';
            $endTx->traceBeans = [self::bean()];
            $this->checkSame(
                EXPECTED_END_TRANSACTION,
                TraceDataEncoder::encoderFromContextBean($endTx)->transData,
                'END_TRANSACTION encoding byte-exact = the fixed vector'
            );

            $recall = new TraceContext();
            $recall->traceType = TraceType::RECALL;
            $recall->timeStamp = 1700000000000;
            $recall->regionId = 'DefaultRegion';
            $recall->groupName = 'GID_test';
            $recall->isSuccess = true;
            $recall->traceBeans = [self::bean()];
            $this->checkSame(
                EXPECTED_RECALL,
                TraceDataEncoder::encoderFromContextBean($recall)->transData,
                'RECALL encoding byte-exact = the fixed vector'
            );

            $this->checkSame(null, TraceDataEncoder::encoderFromContextBean(null), 'encoderFromContextBean(null) → null');
        }

        private function testEncodeSpecialCharsAndChinese(): void
        {
            $ctx = new TraceContext();
            $ctx->traceType = TraceType::PUB;
            $ctx->timeStamp = 1700000000000;
            $ctx->regionId = 'DefaultRegion';
            $ctx->groupName = 'GID_中文';
            $ctx->costTime = 7;
            $ctx->isSuccess = true;
            $bean = self::bean();
            $bean->topic = '主题';
            $bean->tags = '标签';
            $bean->keys = '键一 键二';
            $ctx->traceBeans = [$bean];

            $expected = implode(SOH, [
                'Pub', '1700000000000', 'DefaultRegion', 'GID_中文', '主题',
                MSG_ID_1, '标签', '键一 键二', '127.0.0.1:10911', '42', '7', '0',
                OFFSET_MSG_ID, 'true',
            ]) . STX;
            $tb = TraceDataEncoder::encoderFromContextBean($ctx);
            $this->checkSame($expected, $tb->transData, 'PUB Chinese/special chars stay byte-identical in UTF-8');
            $keys = array_keys($tb->transKey);
            sort($keys, SORT_STRING);
            $this->checkSame([MSG_ID_1, '键一', '键二'], $keys, 'Chinese keys split on spaces');

            // Chinese round-trip decoding
            $back = TraceDataEncoder::decoderFromTraceDataString($tb->transData);
            $this->checkSame(1, count($back), 'Chinese PUB decodes 1 record');
            $this->checkSame('主题', $back[0]->traceBeans[0]->topic, 'Chinese topic round-trips');
            $this->checkSame('GID_中文', $back[0]->groupName, 'Chinese groupName round-trips');
            $this->checkSame('键一 键二', $back[0]->traceBeans[0]->keys, 'Chinese keys round-trip');
        }

        // ============================================================ wire_split / decoding

        private function testWireSplitSemantics(): void
        {
            $this->checkSame(['a'], TraceDataEncoder::wireSplit('a' . STX, STX), 'wireSplit drops the trailing empty string');
            $this->checkSame([], TraceDataEncoder::wireSplit(STX, STX), 'wireSplit(single separator) -> []');
            $this->checkSame(['a', 'b'], TraceDataEncoder::wireSplit('a' . STX . 'b' . STX, STX), 'wireSplit drops the trailing empty across multiple segments');
            $this->checkSame(['a', '', 'b'], TraceDataEncoder::wireSplit('a' . SOH . SOH . 'b', SOH), 'wireSplit keeps middle empty strings');
            // PHP's native explode keeps the trailing empty string — exactly why it cannot be used directly
            $this->checkSame(['a', ''], explode(STX, 'a' . STX), 'native explode keeps the trailing empty string (control)');
        }

        private function testDecodeRoundTrips(): void
        {
            // PUB round-trip
            $ctx = new TraceContext();
            $ctx->traceType = TraceType::PUB;
            $ctx->timeStamp = 1700000000000;
            $ctx->regionId = 'DefaultRegion';
            $ctx->groupName = 'GID_test';
            $ctx->costTime = 7;
            $ctx->isSuccess = true;
            $ctx->traceBeans = [self::bean()];
            $decoded = TraceDataEncoder::decoderFromTraceDataString(
                TraceDataEncoder::encoderFromContextBean($ctx)->transData
            );
            $this->checkSame(1, count($decoded), 'PUB round-trip record count');
            $got = $decoded[0];
            $this->checkSame(TraceType::PUB, $got->traceType, 'PUB round-trip traceType');
            $this->checkSame(1700000000000, $got->timeStamp, 'PUB round-trip timeStamp');
            $this->checkSame('DefaultRegion', $got->regionId, 'PUB round-trip regionId');
            $this->checkSame('GID_test', $got->groupName, 'PUB round-trip groupName');
            $this->checkSame(7, $got->costTime, 'PUB round-trip costTime');
            $this->checkSame(true, $got->isSuccess, 'PUB round-trip isSuccess');
            $bean = $got->traceBeans[0];
            $this->checkSame(['TopicTest', MSG_ID_1, 'TagA', 'KeyA KeyB', '127.0.0.1:10911'], [
                $bean->topic, $bean->msgId, $bean->tags, $bean->keys, $bean->storeHost,
            ], 'PUB round-trip bean five fields');
            $this->checkSame(42, $bean->bodyLength, 'PUB round-trip bodyLength');
            $this->checkSame(OFFSET_MSG_ID, $bean->offsetMsgId, 'PUB round-trip offsetMsgId');
            $this->checkSame(MessageType::NORMAL_MSG, $bean->msgType, 'PUB round-trip msgType');
            $this->checkSame(TraceBean::localAddress(), $bean->clientHost, 'PUB decoding lands clientHost on LOCAL_ADDRESS');

            // SUB_BEFORE round-trip: requestId / retryTimes
            $sb = new TraceContext();
            $sb->traceType = TraceType::SUB_BEFORE;
            $sb->timeStamp = 1700000000000;
            $sb->regionId = 'DefaultRegion';
            $sb->groupName = 'CID_test';
            $sb->requestId = 'REQ-SUB-001';
            $sb->traceBeans = [self::bean(), self::bean(msgId: MSG_ID_2, keys: 'KeyC', retryTimes: 0)];
            $dec = TraceDataEncoder::decoderFromTraceDataString(TraceDataEncoder::encoderFromContextBean($sb)->transData);
            $this->checkSame(2, count($dec), 'SUB_BEFORE round-trip record count');
            $this->checkSame([MSG_ID_1, MSG_ID_2], array_map(static fn($c) => $c->traceBeans[0]->msgId, $dec), 'SUB_BEFORE round-trip msgId order');
            $this->checkSame([2, 0], array_map(static fn($c) => $c->traceBeans[0]->retryTimes, $dec), 'SUB_BEFORE round-trip retryTimes');
            $this->checkSame('REQ-SUB-001', $dec[0]->requestId, 'SUB_BEFORE round-trip requestId');
            $this->checkSame('CID_test', $dec[1]->groupName, 'SUB_BEFORE round-trip groupName');

            // SUB_AFTER round-trip (with contextCode + legacy branch)
            $sa = new TraceContext();
            $sa->traceType = TraceType::SUB_AFTER;
            $sa->timeStamp = 1700000000000;
            $sa->groupName = 'CID_test';
            $sa->requestId = 'REQ-SUB-001';
            $sa->costTime = 11;
            $sa->isSuccess = false;
            $sa->contextCode = 4;
            $sa->accessChannel = AccessChannel::LOCAL;
            $sa->traceBeans = [self::bean()];
            $gotSa = TraceDataEncoder::decoderFromTraceDataString(
                TraceDataEncoder::encoderFromContextBean($sa)->transData
            )[0];
            $this->checkSame(4, $gotSa->contextCode, 'SUB_AFTER round-trip contextCode');
            $this->checkSame(false, $gotSa->isSuccess, 'SUB_AFTER round-trip isSuccess');
            $this->checkSame(1700000000000, $gotSa->timeStamp, 'SUB_AFTER round-trip timeStamp');
            $this->checkSame('CID_test', $gotSa->groupName, 'SUB_AFTER round-trip groupName');

            // With only 7 segments the legacy version must not read timestamp/group
            $legacy = implode(SOH, ['SubAfter', 'REQ', MSG_ID_1, '5', 'true', 'KeyA', '0']);
            $gotLegacy = TraceDataEncoder::decoderFromTraceDataString($legacy)[0];
            $this->checkSame(0, $gotLegacy->contextCode, 'legacy SubAfter contextCode=0');
            $this->check($gotLegacy->timeStamp > 1700000000000, 'legacy SubAfter falls back to the current-time default');
            $this->checkSame('', $gotLegacy->groupName, 'legacy SubAfter groupName empty');

            // END_TRANSACTION round-trip
            $endTx = new TraceContext();
            $endTx->traceType = TraceType::END_TRANSACTION;
            $endTx->timeStamp = 1700000000000;
            $endTx->regionId = 'DefaultRegion';
            $endTx->groupName = 'GID_test';
            $endTx->traceBeans = [self::bean()];
            $gotEnd = TraceDataEncoder::decoderFromTraceDataString(
                TraceDataEncoder::encoderFromContextBean($endTx)->transData
            )[0];
            $this->checkSame(TraceType::END_TRANSACTION, $gotEnd->traceType, 'END_TRANSACTION round-trip traceType');
            $this->checkSame('TRAN-001', $gotEnd->traceBeans[0]->transactionId, 'END_TRANSACTION round-trip transactionId');
            $this->checkSame('COMMIT_MESSAGE', $gotEnd->traceBeans[0]->transactionState, 'END_TRANSACTION round-trip transactionState(string)');
            $this->checkSame(false, $gotEnd->traceBeans[0]->fromTransactionCheck, 'END_TRANSACTION round-trip fromTransactionCheck');
            $this->checkSame(MessageType::NORMAL_MSG, $gotEnd->traceBeans[0]->msgType, 'END_TRANSACTION round-trip msgType');
        }

        private function testDecodeRobustness(): void
        {
            $this->checkSame([], TraceDataEncoder::decoderFromTraceDataString(''), 'decoding an empty string -> []');
            $this->checkSame([], TraceDataEncoder::decoderFromTraceDataString(null), 'decoding null -> []');
            $this->checkSame([], TraceDataEncoder::decoderFromTraceDataString('Bogus' . SOH . '1' . STX), 'unknown record type -> []');
            $this->checkSame([], TraceDataEncoder::decoderFromTraceDataString(STX . STX), 'bare separators -> []');

            // SubBefore without keys: missing fields fall back to an empty string instead of an out-of-bounds error
            $raw = implode(SOH, ['SubBefore', '1700000000000', 'DefaultRegion', 'GID_trace_live',
                'REQ-001', MSG_ID_1, '0', '']) . STX;
            $records = TraceDataEncoder::decoderFromTraceDataString($raw);
            $this->checkSame(1, count($records), 'keyless SubBefore is decodable');
            $this->checkSame('', $records[0]->traceBeans[0]->keys, 'keyless SubBefore keys fall back to an empty string');
            $this->checkSame(0, $records[0]->traceBeans[0]->retryTimes, 'keyless SubBefore retryTimes=0');
            $this->checkSame(MSG_ID_1, $records[0]->traceBeans[0]->msgId, 'keyless SubBefore msgId');

            // One broken record must not destroy the whole trace message
            $broken = implode(SOH, ['SubAfter', 'REQ-9', MSG_ID_2]) . STX;   // too few segments
            $goodPub = TraceDataEncoder::encoderFromContextBean(self::pubContext())->transData;
            $goodSub = implode(SOH, ['SubBefore', '1700000000000', 'R', 'G', 'REQ-1', MSG_ID_1, '0', 'K']) . STX;
            $recs = TraceDataEncoder::decoderFromTraceDataString($goodPub . $broken . $goodSub);
            $this->checkSame(
                [TraceType::PUB, TraceType::SUB_BEFORE],
                array_map(static fn($c) => $c->traceType, $recs),
                'a broken record skips only itself (the rest still decode)'
            );

            // Unknown records are ignored wholesale; known records still decode
            $raw2 = implode(SOH, ['SomethingNew', '1', '2']) . STX
                . TraceDataEncoder::encoderFromContextBean(self::recallContext())->transData;
            $recs2 = TraceDataEncoder::decoderFromTraceDataString($raw2);
            $this->checkSame([TraceType::RECALL], array_map(static fn($c) => $c->traceType, $recs2), 'unknown records are ignored');
        }

        private static function recallContext(): TraceContext
        {
            $recall = new TraceContext();
            $recall->traceType = TraceType::RECALL;
            $recall->timeStamp = 1700000000000;
            $recall->regionId = 'DefaultRegion';
            $recall->groupName = 'GID_test';
            $recall->isSuccess = true;
            $recall->traceBeans = [self::bean()];
            return $recall;
        }

        // ============================================================ Dispatcher

        private function testDispatcherDefaults(): void
        {
            $d = self::makeDispatcher();
            $this->checkSame(MixAll::TRACE_TOPIC, $d->traceTopicName, 'dispatcher default traceTopic');
            $this->checkSame(10, $d->batchNum, 'dispatcher batchNum defaults to 10');
            $this->checkSame(128000, $d->maxMsgSize, 'dispatcher maxMsgSize defaults to 128000');
            $this->checkSame(2048, AsyncTraceDispatcher::MAX_QUEUE_SIZE, 'dispatcher queue cap 2048');

            $d2 = new AsyncTraceDispatcher('GID_test', TraceDispatcherType::PRODUCE, 50, null, null);
            $d2->traceProducer = new FakeTraceProducer();
            $this->checkSame(20, $d2->batchNum, 'batchNum capped at 20');

            $this->check(str_contains($d->genGroupNameForTrace(), TraceConstants::GROUP_NAME_PREFIX), 'the internal producer group name contains GROUP_NAME_PREFIX');
            $this->check(str_contains($d->genGroupNameForTrace(), '-PRODUCE-'), 'the internal producer group name contains -PRODUCE-');
            $dConsume = new AsyncTraceDispatcher('CID_test', TraceDispatcherType::CONSUME);
            $this->check(str_contains($dConsume->genGroupNameForTrace(), '-CONSUME-'), 'the internal producer group name contains -CONSUME-');

            $custom = new AsyncTraceDispatcher('GID_test', TraceDispatcherType::PRODUCE, 10, 'MyTraceTopic');
            $custom->traceProducer = new FakeTraceProducer();
            $this->checkSame('MyTraceTopic', $custom->getTraceTopicName(), 'custom traceTopic');
        }

        private function testDispatcherAppendAndFlush(): void
        {
            $d = self::makeDispatcher();
            $this->checkSame(true, $d->append(self::pubContext()), 'append enqueues successfully');
            $d->flush();
            $this->checkSame(0, count($d->traceContextQueue), 'flush drains the queue');
            $this->checkSame(1, count($d->traceProducer->sent), 'flush triggers exactly one send');
            $this->checkSame(MixAll::TRACE_TOPIC, $d->traceProducer->sent[0][0], 'trace message topic');
            $this->checkSame(EXPECTED_PUB, $d->traceProducer->sent[0][1], 'trace message body = the PUB encoding');
            $keys = explode(' ', (string) $d->traceProducer->sent[0][2]);
            sort($keys, SORT_STRING);
            $this->checkSame([MSG_ID_1, 'KeyA', 'KeyB'], $keys, 'trace message keys = raw msgId + business keys');
        }

        private function testDispatcherSkipsInvalidContexts(): void
        {
            $d = self::makeDispatcher();
            $noRegion = self::pubContext(region: '');
            $this->check($noRegion->traceBeans !== [], 'precondition: noRegion still has beans');
            $d->append($noRegion);
            $empty = self::pubContext();
            $empty->traceBeans = [];
            $d->append($empty);
            $d->flush();
            $this->checkSame([], $d->traceProducer->sent, 'a context with no region or no beans is not sent');
        }

        private function testDispatcherCloudTopic(): void
        {
            $d = self::makeDispatcher();
            $ctx = self::pubContext();
            $ctx->accessChannel = AccessChannel::CLOUD;
            $d->append($ctx);
            $d->flush();
            $this->checkSame('rmq_sys_TRACE_DATA_DefaultRegion', $d->traceProducer->sent[0][0], 'the CLOUD channel uses TRACE_TOPIC_PREFIX+regionId');
        }

        private function testDispatcherGroupsByBusinessTopic(): void
        {
            $d = self::makeDispatcher();
            $d->append(self::pubContext(topic: 'TopicA'));
            $d->append(self::pubContext(topic: 'TopicB'));
            $d->append(self::pubContext(topic: 'TopicA'));
            $d->flush();
            $this->checkSame(2, count($d->traceProducer->sent), 'grouped by (business topic, trace topic) -> 2 groups');
            $counts = array_map(
                static fn($s) => substr_count((string) $s[1], STX),
                $d->traceProducer->sent
            );
            sort($counts);
            $this->checkSame([1, 2], $counts, 'TopicA group holds 2 records, TopicB group 1 (counted by STX)');
        }

        private function testDispatcherSplitsOverMaxSize(): void
        {
            $d = self::makeDispatcher();
            // Threshold set to 1: a single record's encoded length is always >= 1, so every record
            // triggers one split-send
            $d->maxMsgSize = 1;
            $this->checkSame(1, $d->maxMsgSize, 'threshold semantics self-check: with maxMsgSize=1 every record exceeds it');
            for ($i = 0; $i < 3; $i++) {
                $d->append(self::pubContext());
            }
            $d->flush();
            $this->checkSame(3, count($d->traceProducer->sent), 'an oversized body is split-sent record by record, 3 times');
            foreach ($d->traceProducer->sent as $i => $s) {
                $this->checkSame(EXPECTED_PUB, $s[1], "split chunk {$i} body is a single-record encoding");
            }
        }

        private function testDispatcherQueueFullDiscards(): void
        {
            $d = self::makeDispatcher();
            $ctx = self::pubContext();
            for ($i = 0; $i < AsyncTraceDispatcher::MAX_QUEUE_SIZE; $i++) {
                $d->append($ctx);
            }
            $this->checkSame(false, $d->append($ctx), 'append returns false once the queue is full');
            $this->checkSame(1, $d->discardCount, 'the discard counter increments by 1 when the queue is full');
            $this->checkSame(AsyncTraceDispatcher::MAX_QUEUE_SIZE, count($d->traceContextQueue), 'the queue length is capped at 2048');
        }

        private function testDispatcherShutdownFlushes(): void
        {
            $d = self::makeDispatcher();
            $d->append(self::pubContext());
            $d->isStarted = true;
            $d->stopped = true;
            $d->shutdown();
            $this->checkSame(0, count($d->traceContextQueue), 'flush drains the queue before shutdown');
            $this->checkSame(true, $d->traceProducer->stopped, 'shutdown closes the internal producer');
            $this->checkSame(true, $d->stopped, 'shutdown sets stopped');
        }

        // ============================================================ Send hook

        private function testSendHookRoundTrip(): void
        {
            $d = new CapturingDispatcher();
            $hook = new SendMessageTraceHook($d);
            $this->checkSame('SendMessageTraceHook', $hook->hookName(), 'send hook name');

            $ctx = self::sendContext();
            $hook->sendMessageBefore($ctx);
            $this->check($ctx->mqTraceContext instanceof TraceContext, 'before writes mqTraceContext');
            $this->checkSame(TraceType::PUB, $ctx->mqTraceContext->traceType, 'before traceType=PUB');
            $this->checkSame('GID_test', $ctx->mqTraceContext->groupName, 'before groupName');
            $this->checkSame('TopicTest', $ctx->mqTraceContext->traceBeans[0]->topic, 'before bean.topic');
            $this->checkSame('TagA', $ctx->mqTraceContext->traceBeans[0]->tags, 'before bean.tags');
            $this->checkSame('KeyA KeyB', $ctx->mqTraceContext->traceBeans[0]->keys, 'before bean.keys');
            $this->checkSame(strlen(BODY), $ctx->mqTraceContext->traceBeans[0]->bodyLength, 'before bean.bodyLength');
            $this->checkSame('127.0.0.1:10911', $ctx->mqTraceContext->traceBeans[0]->storeHost, 'before bean.storeHost=brokerAddr');
            $this->checkSame(MessageType::NORMAL_MSG, $ctx->mqTraceContext->traceBeans[0]->msgType, 'before bean.msgType');

            $hook->sendMessageAfter($ctx);
            $this->checkSame(1, count($d->appended), 'after enqueues 1 record');
            $got = $d->appended[0];
            $this->checkSame(TraceType::PUB, $got->traceType, 'after traceType=PUB');
            $this->checkSame('DefaultRegion', $got->regionId, 'after regionId comes from the SendResult');
            $this->checkSame(true, $got->isSuccess, 'after isSuccess(SEND_OK)');
            $this->check($got->costTime >= 0, 'after costTime >= 0');
            $this->checkSame(MSG_ID_1, $got->traceBeans[0]->msgId, 'after bean.msgId=SendResult.msgId');
            $this->checkSame(OFFSET_MSG_ID, $got->traceBeans[0]->offsetMsgId, 'after bean.offsetMsgId');
            $this->check($got->traceBeans[0]->storeTime >= $got->timeStamp, 'after storeTime >= timeStamp');
        }

        private function testSendHookSkipsTraceTopicItself(): void
        {
            $d = new CapturingDispatcher();
            $hook = new SendMessageTraceHook($d);
            $ctx = self::sendContext(topic: MixAll::TRACE_TOPIC);
            $hook->sendMessageBefore($ctx);
            $this->checkSame(null, $ctx->mqTraceContext, 'the trace topic itself builds no context in before');
            $hook->sendMessageAfter($ctx);
            $this->checkSame([], $d->appended, 'the trace topic itself does not enqueue in after');
        }

        private function testSendHookSkipsTraceOffOrNoRegion(): void
        {
            $d = new CapturingDispatcher();
            $hook = new SendMessageTraceHook($d);

            $off = self::sendContext(traceOn: false);
            $hook->sendMessageBefore($off);
            $hook->sendMessageAfter($off);
            $this->checkSame([], $d->appended, 'broker traceOn=false does not enqueue');

            $noRegion = self::sendContext(region: null);
            $hook->sendMessageBefore($noRegion);
            $hook->sendMessageAfter($noRegion);
            $this->checkSame([], $d->appended, 'regionId=null does not enqueue');
        }

        private function testSendHookMarksFailureStatus(): void
        {
            $d = new CapturingDispatcher();
            $hook = new SendMessageTraceHook($d);
            $ctx = self::sendContext();
            $ctx->sendResult->sendStatus = SendStatus::FLUSH_DISK_TIMEOUT;
            $hook->sendMessageBefore($ctx);
            $hook->sendMessageAfter($ctx);
            $this->checkSame(false, $d->appended[0]->isSuccess, 'non SEND_OK -> isSuccess=false');
        }

        private function testSendHookRequiresBefore(): void
        {
            $d = new CapturingDispatcher();
            $hook = new SendMessageTraceHook($d);
            $ctx = self::sendContext(); // no before run -> mqTraceContext stays null
            $hook->sendMessageAfter($ctx);
            $this->checkSame([], $d->appended, 'after does not enqueue when before was not run');
        }

        // ============================================================ Consume hook

        private function testConsumeHookBeforeAndAfterShareRequestId(): void
        {
            $d = new CapturingDispatcher();
            $hook = new ConsumeMessageTraceHook($d);
            $this->checkSame('ConsumeMessageTraceHook', $hook->hookName(), 'consume hook name');

            $ctx = self::consumeContext();
            $hook->consumeMessageBefore($ctx);
            $this->checkSame(1, count($d->appended), 'consume before enqueues 1 record');
            $before = $d->appended[0];
            $this->checkSame(TraceType::SUB_BEFORE, $before->traceType, 'before traceType=SubBefore');
            $this->checkSame('CID_test', $before->groupName, 'before groupName');
            $this->checkSame('DefaultRegion', $before->regionId, 'before regionId comes from the message property');
            $this->checkSame(MSG_ID_1, $before->traceBeans[0]->msgId, 'before bean.msgId');
            $this->checkSame(1, $before->traceBeans[0]->retryTimes, 'before bean.retryTimes');
            $this->checkSame(42, $before->traceBeans[0]->bodyLength, 'before bean.bodyLength=storeSize');
            $this->checkSame(1700000000000, $before->traceBeans[0]->storeTime, 'before bean.storeTime');

            $hook->consumeMessageAfter($ctx);
            $this->checkSame(2, count($d->appended), 'consume after enqueues 1 more record');
            $after = $d->appended[1];
            $this->checkSame(TraceType::SUB_AFTER, $after->traceType, 'after traceType=SubAfter');
            $this->checkSame($before->requestId, $after->requestId, 'before/after share the requestId');
            $this->checkSame(true, $after->isSuccess, 'after isSuccess');
            $this->check($after->costTime >= 0, 'after costTime >= 0');
            $this->checkSame(0, $after->contextCode, 'after contextCode(SUCCESS ordinal=0)');
        }

        private function testConsumeHookContextCodeFromProps(): void
        {
            $d = new CapturingDispatcher();
            $hook = new ConsumeMessageTraceHook($d);
            $ctx = self::consumeContext();
            $ctx->props = ['ConsumeContextType' => 'FAILED'];
            $ctx->success = false;
            $hook->consumeMessageBefore($ctx);
            $hook->consumeMessageAfter($ctx);
            $this->checkSame(4, $d->appended[1]->contextCode, 'contextCode: FAILED ordinal=4 (enum looked up by name)');
            $this->checkSame(false, $d->appended[1]->isSuccess, 'after isSuccess=false');

            // An unknown name degrades silently to 0 (no crash)
            $ctx2 = self::consumeContext();
            $ctx2->props = ['ConsumeContextType' => 'NO_SUCH_TYPE'];
            $hook->consumeMessageBefore($ctx2);
            $hook->consumeMessageAfter($ctx2);
            $this->checkSame(0, $d->appended[3]->contextCode, 'an unknown contextType degrades silently to 0');
        }

        private function testConsumeHookSkipsTraceOff(): void
        {
            $d = new CapturingDispatcher();
            $hook = new ConsumeMessageTraceHook($d);
            $ctx = self::consumeContext(traceOn: 'false');
            $hook->consumeMessageBefore($ctx);
            $this->checkSame([], $d->appended, 'message TRACE_ON=false -> before writes no trace');
            $hook->consumeMessageAfter($ctx);
            $this->checkSame([], $d->appended, 'no trace in before -> none in after either');
        }

        private function testConsumeHookAfterWithoutBefore(): void
        {
            $d = new CapturingDispatcher();
            $hook = new ConsumeMessageTraceHook($d);
            $ctx = self::consumeContext();
            $hook->consumeMessageAfter($ctx);
            $this->checkSame([], $d->appended, 'after does not enqueue when before was not run');
        }

        // ============================================================ End-transaction hook

        private function testEndTransactionHook(): void
        {
            $d = new CapturingDispatcher();
            $hook = new EndTransactionTraceHook($d);
            $this->checkSame('EndTransactionTraceHook', $hook->hookName(), 'endTransaction hook name');

            $msg = new MessageExt('TopicTest', BODY, 'TagA', 'KeyA');
            $msg->putProperty(MessageConst::PROPERTY_MSG_REGION, 'RegionX');
            $ctx = new EndTransactionContext();
            $ctx->producerGroup = 'GID_test';
            $ctx->message = $msg;
            $ctx->brokerAddr = '127.0.0.1:10911';
            $ctx->msgId = MSG_ID_1;
            $ctx->transactionId = 'TRAN-001';
            $ctx->transactionState = 'COMMIT_MESSAGE';
            $ctx->fromTransactionCheck = false;
            $hook->endTransaction($ctx);

            $this->checkSame(1, count($d->appended), 'endTransaction enqueues 1 record');
            $got = $d->appended[0];
            $this->checkSame(TraceType::END_TRANSACTION, $got->traceType, 'endTransaction traceType');
            $this->checkSame('GID_test', $got->groupName, 'endTransaction groupName');
            $this->checkSame('RegionX', $got->regionId, 'endTransaction regionId comes from the message property');
            $bean = $got->traceBeans[0];
            $this->checkSame(MessageType::TRANS_MSG_COMMIT, $bean->msgType, 'endTransaction msgType=TRANS_MSG_COMMIT');
            $this->checkSame('CID@1', $bean->clientHost, 'endTransaction clientHost=dispatcher.clientId()');
            $this->checkSame(MSG_ID_1, $bean->msgId, 'endTransaction msgId');
            $this->checkSame('TRAN-001', $bean->transactionId, 'endTransaction transactionId');
            $this->checkSame('COMMIT_MESSAGE', $bean->transactionState, 'endTransaction transactionState');
            $this->checkSame(false, $bean->fromTransactionCheck, 'endTransaction fromTransactionCheck');
            $this->checkSame('TagA', $bean->tags, 'endTransaction tags');
            $this->checkSame('KeyA', $bean->keys, 'endTransaction keys');

            // No MSG_REGION -> falls to DefaultRegion
            $msg2 = new MessageExt('TopicTest', BODY, 'TagA', 'KeyA');
            $ctx2 = new EndTransactionContext();
            $ctx2->producerGroup = 'GID_test';
            $ctx2->message = $msg2;
            $ctx2->brokerAddr = '127.0.0.1:10911';
            $ctx2->msgId = MSG_ID_1;
            $ctx2->transactionId = 'TRAN-002';
            $ctx2->transactionState = 'ROLLBACK_MESSAGE';
            $ctx2->fromTransactionCheck = true;
            $hook->endTransaction($ctx2);
            $this->checkSame(MixAll::DEFAULT_TRACE_REGION_ID, $d->appended[1]->regionId, 'endTransaction with no region -> DefaultRegion');
            $this->checkSame(true, $d->appended[1]->traceBeans[0]->fromTransactionCheck, 'endTransaction fromTransactionCheck=true');

            // The trace topic itself is not traced
            $msg3 = new MessageExt(MixAll::TRACE_TOPIC, BODY);
            $ctx3 = new EndTransactionContext();
            $ctx3->producerGroup = 'GID_test';
            $ctx3->message = $msg3;
            $hook->endTransaction($ctx3);
            $this->checkSame(2, count($d->appended), 'endTransaction on the trace topic itself does not enqueue');
        }

        // ============================================================ Teardown

        public function run(): int
        {
            $this->testConstantsAndEnums();
            $this->testDefaults();
            $this->testEncodePubByteExact();
            $this->testEncodeSubBeforeMultiContextByteExact();
            $this->testEncodeSubAfterByteExact();
            $this->testEncodeEndTransactionAndRecallByteExact();
            $this->testEncodeSpecialCharsAndChinese();
            $this->testWireSplitSemantics();
            $this->testDecodeRoundTrips();
            $this->testDecodeRobustness();
            $this->testDispatcherDefaults();
            $this->testDispatcherAppendAndFlush();
            $this->testDispatcherSkipsInvalidContexts();
            $this->testDispatcherCloudTopic();
            $this->testDispatcherGroupsByBusinessTopic();
            $this->testDispatcherSplitsOverMaxSize();
            $this->testDispatcherQueueFullDiscards();
            $this->testDispatcherShutdownFlushes();
            $this->testSendHookRoundTrip();
            $this->testSendHookSkipsTraceTopicItself();
            $this->testSendHookSkipsTraceOffOrNoRegion();
            $this->testSendHookMarksFailureStatus();
            $this->testSendHookRequiresBefore();
            $this->testConsumeHookBeforeAndAfterShareRequestId();
            $this->testConsumeHookContextCodeFromProps();
            $this->testConsumeHookSkipsTraceOff();
            $this->testConsumeHookAfterWithoutBefore();
            $this->testEndTransactionHook();

            if ($this->failures === []) {
                printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
                return 0;
            }
            printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
            return 1;
        }
    }

    exit((new RunClientTrace())->run());
}
