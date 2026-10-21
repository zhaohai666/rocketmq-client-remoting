<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\Protocol\NamespaceUtil;

/**
 * OpenTracing hook trio for the message lifecycle, one hook instance covering the three
 * reference legs:
 *   - send:        span ``To_<topic>``, kind producer, kind+service+destination+tags+keys+
 *                  store_host+msg_type+body_length at open; success / msg_id / region_id at close
 *   - consume:     one span per message, ``From_<topic>``, kind consumer; msg_id / tags / keys /
 *                  body_length (store size) / retry_times / region_id at open; success at close
 *   - transaction: span ``EndTransaction`` with the transaction identity keys, closed immediately
 *
 * Cross-process propagation rides on the message properties themselves: the upstream context is
 * extracted from the property map and the just-started span's context is injected back into it,
 * so whichever keys the tracer's injector writes travel in-band with the message.
 *
 * Registration is through the ordinary hook lists, so every method really runs:
 *   - Producer::registerSendMessageHook()     -> sendMessageBefore / sendMessageAfter
 *   - Producer::registerEndTransactionHook()  -> endTransaction
 *   - PushConsumer::registerConsumeMessageHook() -> consumeMessageBefore / consumeMessageAfter
 */
final class OpenTracingHook implements SendMessageHook, ConsumeMessageHook, EndTransactionHook
{
    public const SPAN_KIND = 'span.kind';
    public const SPAN_KIND_PRODUCER = 'producer';
    public const SPAN_KIND_CONSUMER = 'consumer';
    public const PEER_SERVICE = 'peer.service';
    public const MESSAGE_BUS_DESTINATION = 'message_bus.destination';

    /** msg_type of the transaction leg: the commit state is the only state this leg reports. */
    public const TRANS_MSG_COMMIT = 'Trans_msg_Commit';

    /** @var \SplObjectStorage<object,object> Message -> in-flight send span */
    private \SplObjectStorage $sendSpans;

    /** @var \SplObjectStorage<object,list<object>> ConsumeMessageContext -> its per-message spans */
    private \SplObjectStorage $consumeSpans;

    /**
     * @param object $tracer an object satisfying the TracingTracer duck-type:
     *   - startSpan(string $operationName, array $options = []): object (options supports
     *     `tags` => array, `refs` => list<object>; the returned object must satisfy setTag/log/finish)
     *   - extract(array $carrier): ?object (SpanContext-like, null if none; the carrier is the
     *     message's property map)
     *   - inject(object $spanContext, array &$carrier): void (writes the propagation keys into
     *     the property map, which the hook then writes back onto the message)
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
        $props = $msg->getProperties();
        $upstream = $this->tracer->extract($props);
        $options = ['tags' => [self::SPAN_KIND => self::SPAN_KIND_PRODUCER]];
        if ($upstream !== null) {
            $options['refs'] = [$upstream];
        }
        $span = $this->tracer->startSpan(TraceConstants::TO_PREFIX . $msg->getTopic(), $options);
        $span->setTag(self::PEER_SERVICE, TraceConstants::ROCKETMQ_SERVICE);
        $span->setTag(self::MESSAGE_BUS_DESTINATION, $msg->getTopic());
        $span->setTag(TraceConstants::ROCKETMQ_TAGS, (string) ($msg->getTags() ?? ''));
        $span->setTag(TraceConstants::ROCKETMQ_KEYS, (string) ($msg->getKeys() ?? ''));
        $span->setTag(TraceConstants::ROCKETMQ_STORE_HOST, $context->brokerAddr);
        $span->setTag(TraceConstants::ROCKETMQ_MSG_TYPE, $context->msgType->name);
        $span->setTag(TraceConstants::ROCKETMQ_BODY_LENGTH, $msg->getBody() === null ? 0 : strlen($msg->getBody()));
        // The just-started (child) span's context is what travels: the downstream leg must
        // parent onto THIS send, not onto whatever upstream produced the incoming context.
        $this->tracer->inject($this->spanContextOf($span), $props);
        foreach ($props as $key => $value) {
            if (($msg->getProperties()[$key] ?? null) !== $value) {
                $msg->setUserProperty($key, (string) $value);
            }
        }
        $this->sendSpans[$msg] = $span;
    }

    public function sendMessageAfter(SendMessageContext $context): void
    {
        $msg = $context->message;
        if ($msg === null || !$this->sendSpans->contains($msg)) {
            return;
        }
        $sr = $context->sendResult;
        // No result, or no region id on it: there is nothing to report and the span is left open,
        // exactly like the documented close-out flow (a missing region id skips the report).
        if ($sr === null || $sr->regionId === null) {
            return;
        }
        $span = $this->sendSpans[$msg];
        $this->sendSpans->detach($msg);
        $span->setTag(TraceConstants::ROCKETMQ_SUCCESS, $sr->sendStatus === SendStatus::SEND_OK);
        $span->setTag(TraceConstants::ROCKETMQ_MSG_ID, $sr->msgId);
        $span->setTag(TraceConstants::ROCKETMQ_REGION_ID, $sr->regionId);
        $span->finish();
    }

    // ------------------------------------------------ Consume side

    public function consumeMessageBefore(ConsumeMessageContext $context): void
    {
        if ($this->consumeSpans->contains($context) || $context->msgList === []) {
            return;
        }
        $spans = [];
        foreach ($context->msgList as $msg) {
            if (!$msg instanceof MessageExt) {
                continue;
            }
            $upstream = $this->tracer->extract($msg->getProperties());
            $options = ['tags' => [self::SPAN_KIND => self::SPAN_KIND_CONSUMER]];
            if ($upstream !== null) {
                $options['refs'] = [$upstream];
            }
            $span = $this->tracer->startSpan(TraceConstants::FROM_PREFIX . $msg->getTopic(), $options);
            $span->setTag(self::PEER_SERVICE, TraceConstants::ROCKETMQ_SERVICE);
            $span->setTag(self::MESSAGE_BUS_DESTINATION, NamespaceUtil::withoutNamespace($msg->getTopic()));
            $span->setTag(TraceConstants::ROCKETMQ_MSG_ID, $msg->msgId);
            $span->setTag(TraceConstants::ROCKETMQ_TAGS, (string) ($msg->getTags() ?? ''));
            $span->setTag(TraceConstants::ROCKETMQ_KEYS, (string) ($msg->getKeys() ?? ''));
            $span->setTag(TraceConstants::ROCKETMQ_BODY_LENGTH, $msg->getStoreSize());
            $span->setTag(TraceConstants::ROCKETMQ_RETRY_TIMERS, $msg->reconsumeTimes);
            $span->setTag(TraceConstants::ROCKETMQ_REGION_ID, $msg->getProperty(MessageConst::PROPERTY_MSG_REGION));
            $spans[] = $span;
        }
        $context->mqTraceContext = $spans;
        $this->consumeSpans[$context] = $spans;
    }

    public function consumeMessageAfter(ConsumeMessageContext $context): void
    {
        if (!$this->consumeSpans->contains($context)) {
            return;
        }
        $spans = $this->consumeSpans[$context];
        $this->consumeSpans->detach($context);
        foreach ($spans as $span) {
            $span->setTag(TraceConstants::ROCKETMQ_SUCCESS, $context->success);
            $span->finish();
        }
    }

    // ------------------------------------------------ Transaction commit / rollback side

    /**
     * One closed span per END_TRANSACTION leg: the transaction identity is reported with the
     * documented keys, and the message is left untouched (no injection on this leg -- the send
     * side already put the context on the properties).
     */
    public function endTransaction(EndTransactionContext $context): void
    {
        $msg = $context->message;
        $upstream = $this->tracer->extract($msg === null ? [] : $msg->getProperties());
        $options = ['tags' => [self::SPAN_KIND => self::SPAN_KIND_PRODUCER]];
        if ($upstream !== null) {
            $options['refs'] = [$upstream];
        }
        $span = $this->tracer->startSpan(TraceConstants::END_TRANSACTION, $options);
        $span->setTag(self::PEER_SERVICE, TraceConstants::ROCKETMQ_SERVICE);
        $span->setTag(self::MESSAGE_BUS_DESTINATION, $msg?->getTopic() ?? '');
        $span->setTag(TraceConstants::ROCKETMQ_TAGS, (string) ($msg?->getTags() ?? ''));
        $span->setTag(TraceConstants::ROCKETMQ_KEYS, (string) ($msg?->getKeys() ?? ''));
        $span->setTag(TraceConstants::ROCKETMQ_STORE_HOST, $context->brokerAddr);
        $span->setTag(TraceConstants::ROCKETMQ_MSG_ID, $context->msgId);
        $span->setTag(TraceConstants::ROCKETMQ_MSG_TYPE, self::TRANS_MSG_COMMIT);
        $span->setTag(TraceConstants::ROCKETMQ_TRANSACTION_ID, (string) ($context->transactionId ?? ''));
        $span->setTag(
            TraceConstants::ROCKETMQ_TRANSACTION_STATE,
            $context->transactionState instanceof \BackedEnum
                ? $context->transactionState->name
                : (string) ($context->transactionState ?? ''),
        );
        $span->setTag(TraceConstants::ROCKETMQ_IS_FROM_TRANSACTION_CHECK, $context->fromTransactionCheck);
        $span->finish();
    }

    /**
     * The context of the just-started span, for inject: only objects exposing a
     * context()/getContext() method qualify; otherwise the span itself is passed and the
     * tracer decides how to handle it.
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
