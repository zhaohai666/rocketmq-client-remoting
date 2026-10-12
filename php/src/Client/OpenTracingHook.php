<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;

final class OpenTracingHook implements SendMessageHook, ConsumeMessageHook
{
    /** 跨进程传播用的消息属性名（JSON 编码的 tracer carrier）。 */
    public const CARRIER_PROP = 'OT_CARRIER';

    public const OP_SEND = 'RocketMQ/send';
    public const OP_CONSUME = 'RocketMQ/consume';

    /** @var \SplObjectStorage<object,object> Message → 在途发送 span */
    private \SplObjectStorage $sendSpans;

    /** @var \SplObjectStorage<object,object> ConsumeMessageContext → 在途消费 span */
    private \SplObjectStorage $consumeSpans;

    /**
     * @param object $tracer 满足 TracingTracer duck-type 的对象：
     *   - startSpan(string $operationName, array $options = []): object（options 支持
     *     `tags` => array、`refs` => list<object>，返回对象需满足 setTag/log/finish）
     *   - extract(array $carrier): ?object（SpanContext-like，无则 null）
     *   - inject(object $spanContext, array &$carrier): void
     */
    public function __construct(private readonly object $tracer)
    {
        $this->sendSpans = new \SplObjectStorage();
        $this->consumeSpans = new \SplObjectStorage();
    }

    public function hookName(): string
    {
        return 'OpenTracingHook';
    }

    // ------------------------------------------------ 发送侧

    public function sendMessageBefore(SendMessageContext $context): void
    {
        $msg = $context->message;
        if ($msg === null || $this->sendSpans->contains($msg)) {
            return; // 同一消息重复进入（客户端重试发送）不叠加 span
        }
        /** @var array<string,mixed> $carrier */
        $carrier = [];
        $prevCtx = $this->extractCarrier($msg);
        $options = [
            'tags' => [
                'messaging.system' => 'rocketmq',
                'messaging.destination' => $msg->getTopic(),
                'messaging.operation' => 'send',
                'component' => 'rocketmq-php-client',
            ],
        ];
        if ($prevCtx !== null) {
            $options['refs'] = [$prevCtx];
        }
        $span = $this->tracer->startSpan(self::OP_SEND, $options);
        $span->setTag('producerGroup', $context->producerGroup);
        if ($context->brokerAddr !== '') {
            $span->setTag('brokerAddr', $context->brokerAddr);
        }
        // context 注入 message properties，随消息上 broker（zipkin/OTLP 之外的带内传播）
        $carrier = [];
        $this->tracer->inject($prevCtx ?? $this->spanContextOf($span), $carrier);
        if ($carrier !== []) {
            $msg->setUserProperty(self::CARRIER_PROP, json_encode($carrier, JSON_UNESCAPED_UNICODE));
        }
        $this->sendSpans[$msg] = $span;
    }

    public function sendMessageAfter(SendMessageContext $context): void
    {
        $msg = $context->message;
        if ($msg === null || !$this->sendSpans->contains($msg)) {
            return;
        }
        $span = $this->sendSpans[$msg];
        $this->sendSpans->detach($msg);
        if ($context->exception !== null) {
            $span->setTag('error', true);
            $span->log(['event' => 'error', 'error.object' => $context->exception::class, 'message' => $context->exception->getMessage()]);
        } else {
            $sr = $context->sendResult;
            if ($sr !== null) {
                $span->setTag('sendStatus', $sr->sendStatus->name ?? (string) $sr->sendStatus);
                if ($sr->msgId !== null) {
                    $span->setTag('msgId', $sr->msgId);
                }
                $span->setTag('queueOffset', $sr->queueOffset);
            }
        }
        $span->finish();
    }

    // ------------------------------------------------ 消费侧

    public function consumeMessageBefore(ConsumeMessageContext $context): void
    {
        if ($this->consumeSpans->contains($context)) {
            return;
        }
        $options = [
            'tags' => [
                'messaging.system' => 'rocketmq',
                'messaging.operation' => 'receive',
                'messaging.consumer.group' => $context->consumerGroup,
                'component' => 'rocketmq-php-client',
            ],
        ];
        $parent = null;
        foreach ($context->msgList as $m) {
            $parent = $this->extractCarrier($m);
            if ($parent !== null) {
                break; // 首个带 carrier 的消息决定 span 的父子关系
            }
        }
        if ($parent !== null) {
            $options['refs'] = [$parent];
        }
        $span = $this->tracer->startSpan(self::OP_CONSUME, $options);
        if ($context->mq !== null) {
            $span->setTag('mq', $context->mq->topic . '#' . $context->mq->queueId . '@' . $context->mq->brokerName);
        }
        $this->consumeSpans[$context] = $span;
    }

    public function consumeMessageAfter(ConsumeMessageContext $context): void
    {
        if (!$this->consumeSpans->contains($context)) {
            return;
        }
        $span = $this->consumeSpans[$context];
        $this->consumeSpans->detach($context);
        $span->setTag('success', $context->success);
        if ($context->status !== null) {
            $span->setTag('status', $context->status);
        }
        $span->finish();
    }

    /** 从消息属性里解出上一次注入的 tracer carrier。 */
    private function extractCarrier(Message $msg): ?object
    {
        $raw = $msg->getUserProperty(self::CARRIER_PROP);
        if ($raw === null || $raw === '') {
            return null;
        }
        $decoded = json_decode($raw, true);
        if (!is_array($decoded) || $decoded === []) {
            return null;
        }
        return $this->tracer->extract($decoded);
    }

    /**
     * 发送侧没有上游 carrier 时，从刚开的 span 拿 context（有 getContext/spanContext
     * 方法的对象才取得到；没有就传 span 本身，由 tracer 决定怎么处理）。
     */
    private function spanContextOf(object $span): object
    {
        if (method_exists($span, 'context')) {
            $ctx = $span->context();
            if (is_object($ctx)) {
                return $ctx;
            }
        }
        if (method_exists($span, 'getContext')) {
            $ctx = $span->getContext();
            if (is_object($ctx)) {
                return $ctx;
            }
        }
        return $span;
    }
}
