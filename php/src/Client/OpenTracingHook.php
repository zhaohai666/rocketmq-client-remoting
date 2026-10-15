<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;

final class OpenTracingHook implements SendMessageHook, ConsumeMessageHook
{
    /** Message property name for cross-process propagation (JSON-encoded tracer carrier). */
    public const CARRIER_PROP = 'OT_CARRIER';

    public const OP_SEND = 'RocketMQ/send';
    public const OP_CONSUME = 'RocketMQ/consume';

    /** @var \SplObjectStorage<object,object> Message -> in-flight send span */
    private \SplObjectStorage $sendSpans;

    /** @var \SplObjectStorage<object,object> ConsumeMessageContext -> in-flight consume span */
    private \SplObjectStorage $consumeSpans;

    /**
     * @param object $tracer an object satisfying the TracingTracer duck-type:
     *   - startSpan(string $operationName, array $options = []): object (options supports
     *     `tags` => array, `refs` => list<object>; the returned object must satisfy setTag/log/finish)
     *   - extract(array $carrier): ?object (SpanContext-like, null if none)
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

    // ------------------------------------------------ Send side

    public function sendMessageBefore(SendMessageContext $context): void
    {
        $msg = $context->message;
        if ($msg === null || $this->sendSpans->contains($msg)) {
            return; // the same message re-entering (client retry) does not stack spans
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
        // context injected into message properties, travels with the message to the broker (in-band propagation besides zipkin/OTLP)
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

    // ------------------------------------------------ Consume side

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
                break; // the first message carrying a carrier decides the span's parent-child relation
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

    /** Extract the previously injected tracer carrier from the message properties. */
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
     * When the send side has no upstream carrier, take the context from the just-opened span
     * (only objects with a getContext/spanContext method qualify; otherwise pass the span
     * itself and let the tracer decide how to handle it).
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
