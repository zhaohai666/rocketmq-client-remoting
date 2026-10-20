<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;

/**
 * OpenTracing hook trio for the message lifecycle (Pub / SubBefore / EndTransaction), reporting
 * with the documented trace key set (see TraceConstants) and the ``Topic <name> <Operation>``
 * operation-name style (see TraceType).
 *
 * Registration is through the ordinary hook lists, so every method really runs:
 *   - Producer::registerSendMessageHook()     -> sendMessageBefore / sendMessageAfter
 *   - Producer::registerEndTransactionHook()  -> endTransaction
 *   - PushConsumer::registerConsumeMessageHook() -> consumeMessageBefore / consumeMessageAfter
 *
 * Nothing here touches the message body or its properties except the one carrier property
 * written on the send side (CARRIER_PROP), which is what the cross-process link rides on.
 */
final class OpenTracingHook implements SendMessageHook, ConsumeMessageHook, EndTransactionHook
{
    /** Message property name for cross-process propagation (JSON-encoded tracer carrier). */
    public const CARRIER_PROP = 'OT_CARRIER';

    /** Operation-name style: ``Topic <name> <Operation>`` (Operation is the trace type value). */
    public const OP_NAME_TEMPLATE = 'Topic %s %s';

    /** Extra keys of the documented set that have no dedicated constant (same ``rocketmq.`` namespace). */
    public const TAG_PRODUCER_GROUP = 'rocketmq.producer_group';
    public const TAG_CONSUMER_GROUP = 'rocketmq.consumer_group';
    public const TAG_BROKER_ADDR = 'rocketmq.broker_addr';
    public const TAG_SEND_STATUS = 'rocketmq.send_status';
    public const TAG_CONSUME_STATUS = 'rocketmq.status';
    public const TAG_QUEUE_OFFSET = 'rocketmq.queue_offset';
    public const TAG_MESSAGE_QUEUE = 'rocketmq.mq';

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

    /** ``Topic <name> <Operation>``: the operation part is the trace-type value (Pub / SubBefore / EndTransaction). */
    public static function operationName(string $topic, TraceType $traceType): string
    {
        return sprintf(self::OP_NAME_TEMPLATE, $topic, $traceType->value);
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
                TraceConstants::ROCKETMQ_SERVICE => TraceConstants::ROCKETMQ_SERVICE,
                TraceConstants::ROCKETMQ_TAGS => (string) ($msg->getTags() ?? ''),
                TraceConstants::ROCKETMQ_KEYS => (string) ($msg->getKeys() ?? ''),
                TraceConstants::ROCKETMQ_BODY_LENGTH => strlen((string) $msg->getBody()),
                TraceConstants::ROCKETMQ_MSG_TYPE => $context->msgType->name,
                self::TAG_PRODUCER_GROUP => $context->producerGroup,
            ],
        ];
        if ($prevCtx !== null) {
            $options['refs'] = [$prevCtx];
        }
        $span = $this->tracer->startSpan(self::operationName($msg->getTopic(), TraceType::PUB), $options);
        if ($context->brokerAddr !== '') {
            $span->setTag(self::TAG_BROKER_ADDR, $context->brokerAddr);
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
        $span->setTag(TraceConstants::ROCKETMQ_SUCCESS, $context->exception === null);
        if ($context->exception !== null) {
            $span->setTag('error', true);
            $span->log(['event' => 'error', 'error.object' => $context->exception::class, 'message' => $context->exception->getMessage()]);
        } else {
            $sr = $context->sendResult;
            if ($sr !== null) {
                $span->setTag(self::TAG_SEND_STATUS, $sr->sendStatus->name ?? (string) $sr->sendStatus);
                if ($sr->msgId !== null) {
                    $span->setTag(TraceConstants::ROCKETMQ_MSG_ID, $sr->msgId);
                }
                $span->setTag(self::TAG_QUEUE_OFFSET, $sr->queueOffset);
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
        $first = $context->msgList[0] ?? null;
        $topic = $first instanceof MessageExt
            ? $first->topic
            : (string) ($context->mq?->topic ?? '');
        $options = [
            'tags' => [
                TraceConstants::ROCKETMQ_SERVICE => TraceConstants::ROCKETMQ_SERVICE,
                TraceConstants::ROCKETMQ_RETRY_TIMERS => (int) ($first?->reconsumeTimes ?? 0),
                self::TAG_CONSUMER_GROUP => $context->consumerGroup,
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
        $span = $this->tracer->startSpan(self::operationName($topic, TraceType::SUB_BEFORE), $options);
        if ($context->mq !== null) {
            $span->setTag(self::TAG_MESSAGE_QUEUE, $context->mq->topic . '#' . $context->mq->queueId . '@' . $context->mq->brokerName);
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
        $span->setTag(TraceConstants::ROCKETMQ_SUCCESS, $context->success);
        if ($context->status !== null) {
            $span->setTag(self::TAG_CONSUME_STATUS, $context->status);
        }
        $span->finish();
    }

    // ------------------------------------------------ Transaction commit / rollback side

    /**
     * One closed span per END_TRANSACTION leg: the transaction identity is reported with the
     * documented keys, and the message is left untouched (no carrier is written here -- the send
     * side already injected it).
     */
    public function endTransaction(EndTransactionContext $context): void
    {
        $topic = (string) ($context->message?->getTopic() ?? '');
        $state = $context->transactionState;
        $span = $this->tracer->startSpan(
            self::operationName($topic, TraceType::END_TRANSACTION),
            [
                'tags' => [
                    TraceConstants::ROCKETMQ_SERVICE => TraceConstants::ROCKETMQ_SERVICE,
                    self::TAG_PRODUCER_GROUP => $context->producerGroup,
                    TraceConstants::ROCKETMQ_TRANSACTION_ID => (string) ($context->transactionId ?? ''),
                    TraceConstants::ROCKETMQ_TRANSACTION_STATE => $state instanceof \BackedEnum
                        ? $state->name
                        : (string) ($state ?? ''),
                    TraceConstants::ROCKETMQ_IS_FROM_TRANSACTION_CHECK => $context->fromTransactionCheck,
                ],
            ],
        );
        if ($context->msgId !== null) {
            $span->setTag(TraceConstants::ROCKETMQ_MSG_ID, $context->msgId);
        }
        if ($context->brokerAddr !== '') {
            $span->setTag(self::TAG_BROKER_ADDR, $context->brokerAddr);
        }
        $span->setTag(TraceConstants::ROCKETMQ_SUCCESS, true);
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
