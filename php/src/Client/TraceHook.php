<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageType;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\UtilAll;
use RocketMQ\Remoting\Protocol\NamespaceUtil;

/**
 * 消息轨迹钩子（对应 org.apache.rocketmq.client.trace.hook 包，
 * 移植自 python/client/trace_hook.py）。
 *
 *   - SendMessageTraceHook    ← SendMessageTraceHookImpl
 *   - ConsumeMessageTraceHook ← ConsumeMessageTraceHookImpl
 *   - EndTransactionTraceHook ← EndTransactionTraceHookImpl
 *
 * 两条硬性约定：
 *   1. **轨迹消息本身不再被追踪**：before/after 都先看 topic 是否以轨迹 topic 开头，
 *      是则直接 return（否则轨迹会自我复制）。
 *   2. **是否落轨迹由 broker 说了算**：发送侧看 SendResult 的 regionId / traceOn
 *      （由 SEND 响应头的 MSG_REGION / TRACE_ON 解析而来，broker 默认 traceOn=true）；
 *      消费侧看消息属性 TRACE_ON 是否为 "false"。
 *
 * ⚠ 依赖其它子任务移植的 `php/src/Client/Hook.php`（SendMessageHook / ConsumeMessageHook /
 *   EndTransactionHook 三个**接口**）与 `php/src/Client/SendResult.php`（SendResult /
 *   SendStatus）。按该文件的实际形态，三个轨迹钩子均 `implements` 对应接口。
 */
final class SendMessageTraceHook implements SendMessageHook
{
    public function __construct(private mixed $localDispatcher)
    {
    }

    public function hookName(): string
    {
        return 'SendMessageTraceHook';
    }

    public function sendMessageBefore(SendMessageContext $context): void
    {
        if ($context === null || $context->message === null) {
            return;
        }
        $topic = $context->message->topic ?? '';
        if (str_starts_with($topic, $this->localDispatcher->getTraceTopicName())) {
            return;
        }
        $traceContext = new TraceContext();
        $context->mqTraceContext = $traceContext;
        $traceContext->traceType = TraceType::PUB;
        $traceContext->groupName = NamespaceUtil::withoutNamespace($context->producerGroup ?? '');
        $bean = new TraceBean();
        $bean->topic = NamespaceUtil::withoutNamespace($topic);
        $bean->tags = $context->message->getTags() ?? '';
        $bean->keys = $context->message->getKeys() ?? '';
        $bean->storeHost = $context->brokerAddr ?? '';
        $body = $context->message->getBody();
        $bean->bodyLength = ($body !== null && $body !== '') ? strlen($body) : 0;
        $bean->msgType = $context->msgType;
        $traceContext->traceBeans = [$bean];
    }

    public function sendMessageAfter(SendMessageContext $context): void
    {
        if ($context === null || $context->message === null) {
            return;
        }
        $topic = $context->message->topic ?? '';
        if (str_starts_with($topic, $this->localDispatcher->getTraceTopicName())) {
            return;
        }
        if ($context->mqTraceContext === null) {
            return;
        }
        if ($context->sendResult === null) {
            return;
        }
        $result = $context->sendResult;
        // broker 侧 traceOn=false 或没带回 region 时不落库（对齐 Java）
        if ($result->regionId === null || !$result->traceOn) {
            return;
        }
        $traceContext = $context->mqTraceContext;
        if ($traceContext->traceBeans === []) {
            return;
        }
        $bean = $traceContext->traceBeans[0];
        $costTime = (int) ((UtilAll::currentTimeMillis() - $traceContext->timeStamp)
            / count($traceContext->traceBeans));
        $traceContext->costTime = $costTime;
        $traceContext->isSuccess = $result->sendStatus === SendStatus::SEND_OK;
        $traceContext->regionId = $result->regionId;
        $bean->msgId = $result->msgId ?? '';
        $bean->offsetMsgId = $result->offsetMsgId ?? '';
        $bean->storeTime = $traceContext->timeStamp + intdiv($costTime, 2);
        $this->localDispatcher->append($traceContext);
    }
}

/**
 * 消费侧轨迹钩子（SubBefore / SubAfter 两条记录共用同一个 requestId）。
 */
final class ConsumeMessageTraceHook implements ConsumeMessageHook
{
    /** 对应 Java MixAll.CONSUME_CONTEXT_TYPE（props 里存放 ConsumeReturnType 的名字）。 */
    private const CONSUME_CONTEXT_TYPE = 'ConsumeContextType';

    public function __construct(private mixed $localDispatcher)
    {
    }

    public function hookName(): string
    {
        return 'ConsumeMessageTraceHook';
    }

    public function consumeMessageBefore(ConsumeMessageContext $context): void
    {
        if ($context === null || $context->msgList === []) {
            return;
        }
        $traceContext = new TraceContext();
        $context->mqTraceContext = $traceContext;
        $traceContext->traceType = TraceType::SUB_BEFORE;
        $traceContext->groupName = NamespaceUtil::withoutNamespace($context->consumerGroup ?? '');
        $beans = [];
        foreach ($context->msgList as $msg) {
            if ($msg === null) {
                continue;
            }
            $regionId = $msg->getProperty(MessageConst::PROPERTY_MSG_REGION);
            $traceOn = $msg->getProperty(MessageConst::PROPERTY_TRACE_SWITCH);
            if ($traceOn !== null && $traceOn === 'false') {
                continue;
            }
            $bean = new TraceBean();
            $bean->topic = NamespaceUtil::withoutNamespace($msg->topic ?? '');
            $bean->msgId = $msg->msgId ?? '';
            $bean->tags = $msg->getTags() ?? '';
            $bean->keys = $msg->getKeys() ?? '';
            $bean->storeTime = $msg->storeTimestamp;
            $bean->bodyLength = $msg->storeSize;
            $bean->retryTimes = $msg->reconsumeTimes;
            $traceContext->regionId = $regionId ?? '';
            $beans[] = $bean;
        }
        if ($beans !== []) {
            $traceContext->traceBeans = $beans;
            $traceContext->timeStamp = UtilAll::currentTimeMillis();
            $this->localDispatcher->append($traceContext);
        }
    }

    public function consumeMessageAfter(ConsumeMessageContext $context): void
    {
        if ($context === null || $context->msgList === []) {
            return;
        }
        $subBefore = $context->mqTraceContext;
        if ($subBefore === null || $subBefore->traceBeans === []) {
            return;
        }
        $subAfter = new TraceContext();
        $subAfter->traceType = TraceType::SUB_AFTER;
        $subAfter->regionId = $subBefore->regionId;
        $subAfter->groupName = NamespaceUtil::withoutNamespace($subBefore->groupName);
        $subAfter->requestId = $subBefore->requestId;
        $subAfter->accessChannel = $context->accessChannel;
        $subAfter->isSuccess = $context->success;
        $subAfter->costTime = (int) ((UtilAll::currentTimeMillis() - $subBefore->timeStamp)
            / count($context->msgList));
        $subAfter->traceBeans = $subBefore->traceBeans;
        $props = $context->props;
        if (is_array($props) && $props !== []) {
            // props[CONSUME_CONTEXT_TYPE] 是 ConsumeReturnType 的**名字**（Java 用
            // ConsumeReturnType.valueOf(contextType) 按名查），其 ordinal 就是轨迹里的
            // contextCode（Java ConsumeMessageTraceHookImpl:113）。
            // ⚠ 必须按名查；按值查（名字→整数）必然失败并静默退化成 SUCCESS。
            $contextType = $props[self::CONSUME_CONTEXT_TYPE] ?? null;
            if ($contextType !== null) {
                $code = self::consumeReturnTypeCode((string) $contextType);
                if ($code !== null) {
                    $subAfter->contextCode = $code;
                }
            }
        }
        $this->localDispatcher->append($subAfter);
    }

    /** 按名查 ConsumeReturnType 的 ordinal（值）；未定义枚举或缺名返回 null。 */
    private static function consumeReturnTypeCode(string $name): ?int
    {
        if (!enum_exists(ConsumeReturnType::class)) {
            return null;
        }
        foreach (ConsumeReturnType::cases() as $case) {
            if ($case->name === $name) {
                return $case->value;
            }
        }
        return null;
    }
}

/**
 * 事务收尾轨迹钩子（对应 EndTransactionTraceHookImpl）。
 *
 * broker 的事务回查（fromTransactionCheck=true）也会走这里，
 * 所以「客户端主动提交」和「回查后提交」两种情况都能在轨迹里看到。
 */
final class EndTransactionTraceHook implements EndTransactionHook
{
    public function __construct(private mixed $localDispatcher)
    {
    }

    public function hookName(): string
    {
        return 'EndTransactionTraceHook';
    }

    public function endTransaction(EndTransactionContext $context): void
    {
        if ($context === null || $context->message === null) {
            return;
        }
        $topic = $context->message->topic ?? '';
        if (str_starts_with($topic, $this->localDispatcher->getTraceTopicName())) {
            return;
        }
        $msg = $context->message;
        $tuxeContext = new TraceContext();
        $tuxeContext->traceType = TraceType::END_TRANSACTION;
        $tuxeContext->groupName = NamespaceUtil::withoutNamespace($context->producerGroup ?? '');
        $bean = new TraceBean();
        $bean->topic = NamespaceUtil::withoutNamespace($topic);
        $bean->tags = $msg->getTags() ?? '';
        $bean->keys = $msg->getKeys() ?? '';
        $bean->storeHost = $context->brokerAddr ?? '';
        $bean->msgType = MessageType::TRANS_MSG_COMMIT;
        $bean->clientHost = $this->localDispatcher->clientId();
        $bean->msgId = $context->msgId ?? '';
        $bean->transactionState = $context->transactionState;
        $bean->transactionId = $context->transactionId;
        $bean->fromTransactionCheck = (bool) $context->fromTransactionCheck;
        $regionId = $msg->getProperty(MessageConst::PROPERTY_MSG_REGION);
        $tuxeContext->regionId = ($regionId !== null && $regionId !== '')
            ? $regionId
            : MixAll::DEFAULT_TRACE_REGION_ID;
        $tuxeContext->traceBeans = [$bean];
        $tuxeContext->timeStamp = UtilAll::currentTimeMillis();
        $this->localDispatcher->append($tuxeContext);
    }
}
