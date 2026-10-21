<?php

declare(strict_types=1);

/*
 * =====================================================================================
 * OpenTracingHook self-test: pure assert style (no phpunit); on success prints
 * `ALL TESTS PASSED (N checks)`, on failure exits non-zero.
 * -------------------------------------------------------------------------------------
 * Verifies the hook's contract:
 *   1) Send side: one span ``To_<topic>`` opened with span.kind=producer and the documented
 *      rocketmq.* tag set; the just-started span's context is injected into the message
 *      properties (in-band propagation), so a pre-existing property survives and only the
 *      propagation keys are added; repeating before on the same message (client retry) neither
 *      stacks spans nor re-injects;
 *   2) Send side after: rocketmq.success / rocketmq.mgs_id / rocketmq.region_id + finish exactly
 *      once; a send result without a region id reports nothing and leaves the span open; the
 *      exception path takes the same no-report shape; repeated after is idempotent;
 *   3) Consume side: one span per message ``From_<topic>`` (span.kind=consumer) with the
 *      per-message identity tags (msg_id / body_length = store size / retry_times / region_id);
 *      child-of the upstream context when one rides on the properties; before/after idempotence;
 *   4) EndTransaction leg: one closed ``EndTransaction`` span with the transaction identity
 *      (mgs_type is the commit marker, no success key), extract over the property map but no
 *      injection, message left byte-identical;
 *   5) Executability: the hook really runs through the producer's own hook executors
 *      (registerSendMessageHook / registerEndTransactionHook); without those registrations the
 *      same calls record nothing (control).
 * Every span must report ONLY the documented keys: each block cross-checks the opened tag keys
 * against an allow-list.
 * tracer/span use FakeTracer/FakeSpan, satisfying the duck type from the
 * OpenTracingHook docblock:
 *   startSpan(string, array{tags?,refs?}):object / extract(array):?object /
 *   inject(object, array&):void + the span's setTag/log/finish/context().
 * =====================================================================================
 */
namespace RocketMQ\Client {
    require_once __DIR__ . '/../bootstrap.php';

    use RocketMQ\Common\Message;
    use RocketMQ\Common\MessageConst;
    use RocketMQ\Common\MessageExt;
    use RocketMQ\Common\MessageType;

    // ---- FakeTracer / FakeSpan / FakeSpanContext (kept in this file's namespace, not in src/) ----

    final class FakeSpanContext
    {
        public function __construct(public string $tag = 'ctx')
        {
        }
    }

    final class FakeSpan
    {
        /** @var array<string,mixed> */
        public array $tags = [];
        /** @var list<array<string,mixed>> */
        public array $logs = [];
        public int $finishCount = 0;

        public function __construct(public string $name, public FakeTracer $tracer)
        {
        }

        public function setTag(string $key, mixed $value): self
        {
            $this->tags[$key] = $value;
            return $this;
        }

        /** @param array<string,mixed> $fields */
        public function log(array $fields): self
        {
            $this->logs[] = $fields;
            $this->tracer->calls[] = 'log';
            return $this;
        }

        public function context(): FakeSpanContext
        {
            return new FakeSpanContext($this->name);
        }

        public function finish(): void
        {
            $this->finishCount++;
            $this->tracer->calls[] = "finish:{$this->name}";
        }
    }

    final class FakeTracer
    {
        /** @var list<string> */
        public array $calls = [];
        /** @var array<string,list<array<string,mixed>>> op => the options of each startSpan call */
        public array $spanOpts = [];
        /** @var list<FakeSpan> every span ever produced by startSpan (in time order) */
        public array $spans = [];
        /** extract return value (simulates the SpanContext restored from the properties); null = no upstream */
        public ?object $nextExtract = null;
        public int $extractCalls = 0;
        public int $injectCalls = 0;
        /** @var list<object> the span contexts handed to inject (order preserved) */
        public array $injected = [];
        /** @var list<array<string,string>> the carriers handed to extract (the property maps) */
        public array $extractCarriers = [];
        /** What inject writes into the carrier (simulates a real tracer's propagation keys) */
        public array $carrierValue = ['traceparent' => '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'];

        public function startSpan(string $operationName, array $options = []): FakeSpan
        {
            $this->calls[] = "startSpan:{$operationName}";
            $this->spanOpts[$operationName][] = $options;
            $span = new FakeSpan($operationName, $this);
            $this->spans[] = $span;
            return $span;
        }

        /** @param array<string,mixed> $carrier */
        public function extract(array $carrier): ?object
        {
            $this->calls[] = 'extract';
            $this->extractCalls++;
            $this->extractCarriers[] = $carrier;
            return $this->nextExtract;
        }

        /** @param array<string,mixed> $carrier */
        public function inject(object $spanContext, array &$carrier): void
        {
            $this->calls[] = 'inject';
            $this->injectCalls++;
            $this->injected[] = $spanContext;
            foreach ($this->carrierValue as $k => $v) {
                $carrier[$k] = $v;
            }
        }
    }

    // ---- Cases ----

    $passed = 0;
    $failed = 0;
    $check = function (bool $ok, string $name, string $detail = '') use (&$passed, &$failed): void {
        echo ($ok ? 'PASS ' : 'FAIL ') . $name . ($detail !== '' ? " | {$detail}" : '') . PHP_EOL;
        if ($ok) {
            $passed++;
        } else {
            $failed++;
        }
    };
    $count = function (array $calls, string $needle): int {
        return count(array_values(array_filter($calls, static fn(string $c): bool => $c === $needle)));
    };

    $mkSendCtx = static function (): SendMessageContext {
        $ctx = new SendMessageContext();
        $ctx->producerGroup = 'GID_OT';
        $ctx->brokerAddr = '127.0.0.1:10911';
        return $ctx;
    };

    // The span names are literals on purpose: deriving them from the production constants would
    // make a renamed operation invisible to this file.
    $opSend = 'To_OT_Topic';
    $opConsume = 'From_OT_Topic';
    $opEndTx = 'EndTransaction';

    // 1) hookName
    $tracer = new FakeTracer();
    $hook = new OpenTracingHook($tracer);
    $check($hook->hookName() === 'OpenTracingHook', 'hookName');

    // 2) Send-side before: span + kind + documented tag set + context injected into the properties
    $tracer = new FakeTracer();
    $hook = new OpenTracingHook($tracer);
    $ctx = $mkSendCtx();
    $ctx->message = new Message('OT_Topic', 'hello-1', 'TagA', 'key-1');
    $ctx->msgType = MessageType::ORDER_MSG;
    $hook->sendMessageBefore($ctx);
    $msg = $ctx->message;
    $check($count($tracer->calls, 'startSpan:' . $opSend) === 1, 'send before opens exactly 1 send span');
    $opts = $tracer->spanOpts[$opSend][0] ?? [];
    $tags = $opts['tags'] ?? [];
    $check(($tags[OpenTracingHook::SPAN_KIND] ?? '') === OpenTracingHook::SPAN_KIND_PRODUCER,
        'send span opens with span.kind=producer', json_encode($tags));
    $span0 = $tracer->spans[0] ?? null;
    $allTags = ($span0->tags ?? []) + $tags;
    $check(($allTags[OpenTracingHook::PEER_SERVICE] ?? '') === TraceConstants::ROCKETMQ_SERVICE
        && ($allTags[OpenTracingHook::MESSAGE_BUS_DESTINATION] ?? '') === 'OT_Topic'
        && ($allTags[TraceConstants::ROCKETMQ_TAGS] ?? '') === 'TagA'
        && ($allTags[TraceConstants::ROCKETMQ_KEYS] ?? '') === 'key-1'
        && ($allTags[TraceConstants::ROCKETMQ_STORE_HOST] ?? '') === '127.0.0.1:10911'
        && ($allTags[TraceConstants::ROCKETMQ_MSG_TYPE] ?? '') === 'ORDER_MSG'
        && ($allTags[TraceConstants::ROCKETMQ_BODY_LENGTH] ?? -1) === strlen('hello-1'),
        'send span carries the documented tag set', json_encode($allTags));
    $documentedSendKeys = [
        OpenTracingHook::SPAN_KIND,
        OpenTracingHook::PEER_SERVICE,
        OpenTracingHook::MESSAGE_BUS_DESTINATION,
        TraceConstants::ROCKETMQ_TAGS,
        TraceConstants::ROCKETMQ_KEYS,
        TraceConstants::ROCKETMQ_STORE_HOST,
        TraceConstants::ROCKETMQ_MSG_TYPE,
        TraceConstants::ROCKETMQ_BODY_LENGTH,
    ];
    $extra = array_diff(array_keys($allTags), $documentedSendKeys);
    $check($extra === [], 'send span reports only the documented key set', json_encode($extra));
    $check($tracer->extractCalls === 1 && $tracer->injectCalls === 1,
        'send before extracts the upstream and injects exactly once',
        "extract={$tracer->extractCalls} inject={$tracer->injectCalls}");
    $check(($tracer->injected[0]->tag ?? '') === $opSend,
        'the just-started span context is what gets injected', json_encode(array_map(
            static fn(object $c): string => $c->tag, $tracer->injected)));
    $check(($msg->getUserProperty('traceparent') ?? '') === $tracer->carrierValue['traceparent'],
        'the propagation keys land on the message properties',
        (string) $msg->getUserProperty('traceparent'));

    // 2b) Upstream control: a context restored from the properties parents the span, but the
    //     child context (not the upstream) is what travels downstream.
    $tracerUp = new FakeTracer();
    $upCtx = new FakeSpanContext('upstream-ctx');
    $tracerUp->nextExtract = $upCtx;
    $hookUp = new OpenTracingHook($tracerUp);
    $ctxUp = $mkSendCtx();
    $ctxUp->message = new Message('OT_Topic', 'upstream');
    $hookUp->sendMessageBefore($ctxUp);
    $optsUp = $tracerUp->spanOpts[$opSend][0] ?? [];
    $check(($optsUp['refs'][0] ?? null) === $upCtx && ($tracerUp->injected[0]->tag ?? '') === $opSend,
        'upstream: span refs the extracted context, but the child context is injected');

    // 3) Repeating before on the same message (client retry): no span stacking, no re-injection
    $propBefore = $msg->getUserProperty('traceparent');
    $hook->sendMessageBefore($ctx);
    $check($count($tracer->calls, 'startSpan:' . $opSend) === 1 && $tracer->injectCalls === 1
        && $tracer->extractCalls === 1
        && $msg->getUserProperty('traceparent') === $propBefore,
        'repeated before is idempotent (retry neither stacks spans nor re-injects)');

    // 4) Send-side after success: tags + finish once
    $ctx->sendResult = new SendResult(SendStatus::SEND_OK, 'OT_MSGID_1', null, 7, null, null, 'DefaultRegion');
    $hook->sendMessageAfter($ctx);
    $check($count($tracer->calls, 'finish:' . $opSend) === 1, 'send after finish exactly once');
    $check(($span0->tags[TraceConstants::ROCKETMQ_SUCCESS] ?? null) === true
        && ($span0->tags[TraceConstants::ROCKETMQ_MSG_ID] ?? '') === 'OT_MSGID_1'
        && ($span0->tags[TraceConstants::ROCKETMQ_REGION_ID] ?? '') === 'DefaultRegion',
        'send after records success/msg_id/region_id', json_encode($span0->tags ?? []));

    // 4b) Repeated after: the span is already detached, so nothing else is finished or tagged.
    $tagsAfter = $span0->tags;
    $tracer->calls = [];
    $hook->sendMessageAfter($ctx);
    $check($tracer->calls === [] && $span0->tags === $tagsAfter,
        'repeated send after records nothing (span detached bookkeeping)');

    // 5) Send result without a region id: nothing is reported and the span stays open
    $tracerR = new FakeTracer();
    $hookR = new OpenTracingHook($tracerR);
    $ctxR = $mkSendCtx();
    $ctxR->message = new Message('OT_Topic', 'region-less');
    $hookR->sendMessageBefore($ctxR);
    $regionSpan = $tracerR->spans[0] ?? null;
    $ctxR->sendResult = new SendResult(SendStatus::SEND_OK, 'OT_MSGID_2', null, 8);
    $hookR->sendMessageAfter($ctxR);
    $check($regionSpan !== null && $regionSpan->finishCount === 0
        && !array_key_exists(TraceConstants::ROCKETMQ_SUCCESS, $regionSpan->tags)
        && $count($tracerR->calls, 'finish:' . $opSend) === 0,
        'region-less send result: nothing reported, span left open', json_encode($regionSpan?->tags ?? []));

    // 5b) Exception path: the same no-report shape
    $ctxR->exception = new \RuntimeException('connect refused');
    $hookR->sendMessageAfter($ctxR);
    $check($regionSpan->finishCount === 0 && !array_key_exists(TraceConstants::ROCKETMQ_SUCCESS, $regionSpan->tags),
        'exception path: no report, span left open', json_encode($regionSpan->tags));

    // 6) Consume side: one span per message with the per-message identity tags
    $tracer3 = new FakeTracer();
    $hook3 = new OpenTracingHook($tracer3);
    $in1 = new MessageExt('OT_Topic', 'payload-1');
    $in1->msgId = 'OT_MSGID_C1';
    $in1->reconsumeTimes = 3;
    $in1->setStoreSize(137);
    $in1->setUserProperty(MessageConst::PROPERTY_MSG_REGION, 'RegionHangzhou');
    $in2 = new MessageExt('OT_Topic', 'payload-2');
    $in2->msgId = 'OT_MSGID_C2';
    $in2->reconsumeTimes = 1;
    $in2->setStoreSize(58);
    $cctx = new ConsumeMessageContext('GID_OT', [$in1, $in2], null);
    $hook3->consumeMessageBefore($cctx);
    $cOpts1 = $tracer3->spanOpts[$opConsume][0] ?? [];
    $cOpts2 = $tracer3->spanOpts[$opConsume][1] ?? [];
    $check($count($tracer3->calls, 'startSpan:' . $opConsume) === 2 && $tracer3->extractCalls === 2,
        'consume before opens one span per message and extracts per message',
        "spans={$count($tracer3->calls, 'startSpan:' . $opConsume)} extract={$tracer3->extractCalls}");
    $cSpan1 = $tracer3->spans[0] ?? null;
    $cSpan2 = $tracer3->spans[1] ?? null;
    $check(($cOpts1['tags'][OpenTracingHook::SPAN_KIND] ?? '') === OpenTracingHook::SPAN_KIND_CONSUMER
        && ($cOpts2['tags'][OpenTracingHook::SPAN_KIND] ?? '') === OpenTracingHook::SPAN_KIND_CONSUMER,
        'consume spans open with span.kind=consumer');
    $check(($cSpan1?->tags[TraceConstants::ROCKETMQ_MSG_ID] ?? '') === 'OT_MSGID_C1'
        && ($cSpan1?->tags[TraceConstants::ROCKETMQ_BODY_LENGTH] ?? -1) === 137
        && ($cSpan1?->tags[TraceConstants::ROCKETMQ_RETRY_TIMERS] ?? -1) === 3
        && ($cSpan1?->tags[TraceConstants::ROCKETMQ_REGION_ID] ?? '') === 'RegionHangzhou'
        && ($cSpan1?->tags[OpenTracingHook::PEER_SERVICE] ?? '') === TraceConstants::ROCKETMQ_SERVICE
        && ($cSpan1?->tags[OpenTracingHook::MESSAGE_BUS_DESTINATION] ?? '') === 'OT_Topic',
        'first consume span carries its message identity', json_encode($cSpan1?->tags ?? []));
    $check(($cSpan2?->tags[TraceConstants::ROCKETMQ_MSG_ID] ?? '') === 'OT_MSGID_C2'
        && ($cSpan2?->tags[TraceConstants::ROCKETMQ_BODY_LENGTH] ?? -1) === 58
        && ($cSpan2?->tags[TraceConstants::ROCKETMQ_RETRY_TIMERS] ?? -1) === 1
        && $cSpan2 !== null
        && array_key_exists(TraceConstants::ROCKETMQ_REGION_ID, $cSpan2->tags)
        && $cSpan2->tags[TraceConstants::ROCKETMQ_REGION_ID] === null,
        'second consume span carries its own identity (no region property => null region tag)',
        json_encode($cSpan2?->tags ?? []));
    $documentedConsumeKeys = [
        OpenTracingHook::SPAN_KIND,
        OpenTracingHook::PEER_SERVICE,
        OpenTracingHook::MESSAGE_BUS_DESTINATION,
        TraceConstants::ROCKETMQ_MSG_ID,
        TraceConstants::ROCKETMQ_TAGS,
        TraceConstants::ROCKETMQ_KEYS,
        TraceConstants::ROCKETMQ_BODY_LENGTH,
        TraceConstants::ROCKETMQ_RETRY_TIMERS,
        TraceConstants::ROCKETMQ_REGION_ID,
    ];
    $extra = array_diff(array_keys(($cSpan1?->tags ?? []) + ($cOpts1['tags'] ?? [])), $documentedConsumeKeys);
    $check($extra === [], 'consume span reports only the documented key set', json_encode($extra));
    $stored = $cctx->mqTraceContext;
    $check(is_array($stored) && count($stored) === 2
        && ($stored[0] ?? null) === $cSpan1 && ($stored[1] ?? null) === $cSpan2,
        'the per-message spans are handed back on the context (mqTraceContext)');

    // 7) Consume before idempotence: repeated calls do not stack spans
    $hook3->consumeMessageBefore($cctx);
    $check($count($tracer3->calls, 'startSpan:' . $opConsume) === 2 && $tracer3->extractCalls === 2,
        'consume before is idempotent');

    // 8) Consume after: success tag + finish per span; repeated after is idempotent
    $cctx->success = true;
    $hook3->consumeMessageAfter($cctx);
    $hook3->consumeMessageAfter($cctx);
    $check($count($tracer3->calls, 'finish:' . $opConsume) === 2,
        'consume after finishes each span exactly once (repeated calls idempotent)');
    $check(($cSpan1?->tags[TraceConstants::ROCKETMQ_SUCCESS] ?? null) === true
        && ($cSpan2?->tags[TraceConstants::ROCKETMQ_SUCCESS] ?? null) === true,
        'consume after records success on every span');

    // 9) Consume side without an upstream: extract is attempted (the properties are the carrier)
    //    but the span opens without refs
    $tracer4 = new FakeTracer();
    $hook4 = new OpenTracingHook($tracer4);
    $plain = new MessageExt('OT_Topic', 'no-carrier');
    $cctx4 = new ConsumeMessageContext('GID_OT', [$plain], null);
    $hook4->consumeMessageBefore($cctx4);
    $cOpts4 = $tracer4->spanOpts[$opConsume][0] ?? [];
    $check(!isset($cOpts4['refs']) && $count($tracer4->calls, 'startSpan:' . $opConsume) === 1
        && $tracer4->extractCalls === 1,
        'message without upstream: span opened, no refs, extract attempted over the property map');

    // 10) Cross-process closed loop: propagation keys injected on the send side => a brand-new
    //     MessageExt (simulating peer-side decoding) => consume side child-of the same context
    $tracer5 = new FakeTracer();
    $hook5 = new OpenTracingHook($tracer5);
    $sendCtx5 = $mkSendCtx();
    $sendCtx5->message = new Message('OT_Topic', 'round-trip');
    $hook5->sendMessageBefore($sendCtx5);
    $carrierRaw = (string) $sendCtx5->message->getUserProperty('traceparent');
    // ---- the "cross-process" boundary: the peer receives a bare property string ----
    $downstream = new MessageExt('OT_Topic', 'round-trip');
    $downstream->setUserProperty('traceparent', $carrierRaw);
    $ctx5 = new FakeSpanContext('round-trip-ctx');
    $tracer5->nextExtract = $ctx5;
    $cctx5 = new ConsumeMessageContext('GID_OT_Down', [$downstream], null);
    $hook5->consumeMessageBefore($cctx5);
    $cOpts5 = $tracer5->spanOpts[$opConsume][0] ?? [];
    $check(($cOpts5['refs'][0] ?? null) === $ctx5
        && (($tracer5->extractCarriers[1]['traceparent'] ?? '') === $carrierRaw),
        'cross-process closed loop: downstream extracts the key the send side injected');

    // 11) EndTransaction leg: one closed span with the transaction identity, message untouched
    $tracer6 = new FakeTracer();
    $hook6 = new OpenTracingHook($tracer6);
    $txMsg = new Message('OT_Topic', 'tx-body', 'TagT', 'keyT');
    $txMsg->setUserProperty('PRE_EXISTING', 'keep-me');
    $propsBefore = $txMsg->properties;
    $txCtx = new EndTransactionContext();
    $txCtx->producerGroup = 'GID_OT';
    $txCtx->message = $txMsg;
    $txCtx->brokerAddr = '127.0.0.1:10912';
    $txCtx->msgId = 'OT_MSGID_TX';
    $txCtx->transactionId = 'HALF_OT_1';
    $txCtx->transactionState = 'CommitMessage';
    $txCtx->fromTransactionCheck = true;
    $hook6->endTransaction($txCtx);
    $txSpan = $tracer6->spans[0] ?? null;
    $txOpts = $tracer6->spanOpts[$opEndTx][0] ?? [];
    $txAllTags = ($txSpan?->tags ?? []) + ($txOpts['tags'] ?? []);
    $check($count($tracer6->calls, 'startSpan:' . $opEndTx) === 1
        && $count($tracer6->calls, 'finish:' . $opEndTx) === 1,
        'end transaction opens and closes exactly one span');
    $check(($txAllTags[OpenTracingHook::SPAN_KIND] ?? '') === OpenTracingHook::SPAN_KIND_PRODUCER
        && ($txAllTags[OpenTracingHook::MESSAGE_BUS_DESTINATION] ?? '') === 'OT_Topic'
        && ($txAllTags[TraceConstants::ROCKETMQ_TAGS] ?? '') === 'TagT'
        && ($txAllTags[TraceConstants::ROCKETMQ_KEYS] ?? '') === 'keyT'
        && ($txAllTags[TraceConstants::ROCKETMQ_STORE_HOST] ?? '') === '127.0.0.1:10912'
        && ($txAllTags[TraceConstants::ROCKETMQ_MSG_ID] ?? '') === 'OT_MSGID_TX'
        && ($txAllTags[TraceConstants::ROCKETMQ_MSG_TYPE] ?? '') === OpenTracingHook::TRANS_MSG_COMMIT
        && ($txAllTags[TraceConstants::ROCKETMQ_TRANSACTION_ID] ?? '') === 'HALF_OT_1'
        && ($txAllTags[TraceConstants::ROCKETMQ_TRANSACTION_STATE] ?? '') === 'CommitMessage'
        && ($txAllTags[TraceConstants::ROCKETMQ_IS_FROM_TRANSACTION_CHECK] ?? null) === true
        && !array_key_exists(TraceConstants::ROCKETMQ_SUCCESS, $txAllTags)
        && !isset($txOpts['refs']),
        'end transaction span carries the transaction identity (no success key, no refs)',
        json_encode($txAllTags));
    $documentedTxKeys = [
        OpenTracingHook::SPAN_KIND,
        OpenTracingHook::PEER_SERVICE,
        OpenTracingHook::MESSAGE_BUS_DESTINATION,
        TraceConstants::ROCKETMQ_TAGS,
        TraceConstants::ROCKETMQ_KEYS,
        TraceConstants::ROCKETMQ_STORE_HOST,
        TraceConstants::ROCKETMQ_MSG_ID,
        TraceConstants::ROCKETMQ_MSG_TYPE,
        TraceConstants::ROCKETMQ_TRANSACTION_ID,
        TraceConstants::ROCKETMQ_TRANSACTION_STATE,
        TraceConstants::ROCKETMQ_IS_FROM_TRANSACTION_CHECK,
    ];
    $extra = array_diff(array_keys($txAllTags), $documentedTxKeys);
    $check($extra === [], 'end transaction span reports only the documented key set', json_encode($extra));
    $check($tracer6->extractCalls === 1 && $tracer6->injectCalls === 0
        && ($tracer6->extractCarriers[0] ?? []) === $txMsg->getProperties(),
        'end transaction extracts over the property map and never injects');
    $check($txMsg->properties === $propsBefore,
        'end transaction leaves the message byte-identical', json_encode($txMsg->properties));

    // 12) Executability: the hook really runs through the producer's own hook executors.
    //     Control: the same executor calls on an unregistered producer record nothing.
    $tracer7 = new FakeTracer();
    $producer = new DefaultMQProducer('GID_OT_EXEC');
    $producer->registerSendMessageHook(new OpenTracingHook($tracer7));
    $producer->registerEndTransactionHook(new OpenTracingHook($tracer7));
    $execCtx = $mkSendCtx();
    $execCtx->producerGroup = 'GID_OT_EXEC';
    $execCtx->message = new Message('OT_Topic', 'hooked');
    $producer->executeSendMessageHookBefore($execCtx);
    $execCtx->sendResult = new SendResult(SendStatus::SEND_OK, 'OT_MSGID_EXEC', null, 1, null, null, 'DefaultRegion');
    $producer->executeSendMessageHookAfter($execCtx);
    $execTx = new EndTransactionContext();
    $execTx->producerGroup = 'GID_OT_EXEC';
    $execTx->message = $execCtx->message;
    $execTx->brokerAddr = '127.0.0.1:10911';
    $execTx->msgId = 'OT_MSGID_EXEC_TX';
    $execTx->transactionId = 'HALF_OT_EXEC';
    $execTx->transactionState = 'CommitMessage';
    $execTx->fromTransactionCheck = false;
    $producer->executeEndTransactionHook($execTx);
    $check($count($tracer7->calls, 'startSpan:' . $opSend) === 1
        && $count($tracer7->calls, 'finish:' . $opSend) === 1
        && $count($tracer7->calls, 'startSpan:' . $opEndTx) === 1
        && $count($tracer7->calls, 'finish:' . $opEndTx) === 1,
        'producer executors drive the send legs and the transaction leg', json_encode($tracer7->calls));

    $tracerIdle = new FakeTracer();
    $bare = new DefaultMQProducer('GID_OT_BARE');
    $bareCtx = $mkSendCtx();
    $bareCtx->message = new Message('OT_Topic', 'unhooked');
    $bare->executeSendMessageHookBefore($bareCtx);
    $bare->executeSendMessageHookAfter($bareCtx);
    $bare->executeEndTransactionHook($execTx);
    $check($tracerIdle->calls === [] && ($bareCtx->message->getUserProperty('traceparent') ?? '') === '',
        'control: an unregistered producer records nothing and writes no propagation key',
        json_encode($tracerIdle->calls));


    echo PHP_EOL;
    if ($failed === 0) {
        printf("ALL TESTS PASSED (%d checks)%s", $passed, PHP_EOL);
        exit(0);
    }
    printf("TESTS FAILED: %d/%d%s", $failed, $passed + $failed, PHP_EOL);
    exit(1);
}
