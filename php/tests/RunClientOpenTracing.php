<?php

declare(strict_types=1);

/*
 * =====================================================================================
 * OpenTracingHook self-test: pure assert style (no phpunit); on success prints
 * `ALL TESTS PASSED (N checks)`, on failure exits non-zero (style copied from
 * tests/RunClientTrace.php).
 * -------------------------------------------------------------------------------------
 * Verifies five things:
 *   1) Send side: one span named ``Topic <topic> Pub`` + the documented rocketmq.* tags + carrier
 *      injected into the OT_CARRIER property (in-band propagation); repeating before on the same
 *      message (client retry) neither stacks spans nor re-injects, and no other message property
 *      or the body shifts;
 *   2) Send side after: rocketmq.success / rocketmq.mgs_id / rocketmq.send_status /
 *      rocketmq.queue_offset + finish exactly once; exception => error tag + log + finish;
 *      repeated after is idempotent (SplObjectStorage bookkeeping);
 *   3) Consume side: extract from the first message carrying a carrier => child-of refs =>
 *      startSpan(``Topic <topic> SubBefore``); no carrier => no refs; before/after idempotence
 *      matches the send side; after writes rocketmq.success / rocketmq.status;
 *   4) EndTransaction leg: ``Topic <topic> EndTransaction`` span with the transaction identity
 *      keys, and the message left byte-identical (no carrier written on this leg);
 *   5) Executability: the hook satisfies all three hook interfaces and really runs through the
 *      producer's own hook executors (registerSendMessageHook / registerEndTransactionHook);
 *      without those registrations the same calls record nothing (control).
 * tracer/span use FakeTracer/FakeSpan, satisfying the duck type from the
 * OpenTracingHook docblock:
 *   startSpan(string, array{tags?,refs?}):object / extract(array):?object /
 *   inject(object, array&):void + the span's setTag/log/finish/context().
 * =====================================================================================
 */
namespace RocketMQ\Client {
    require_once __DIR__ . '/../bootstrap.php';

    use RocketMQ\Common\Message;
    use RocketMQ\Common\MessageExt;
    use RocketMQ\Common\MessageQueue;
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
        /** extract return value (simulates the SpanContext restored from a carrier); null = no upstream in the carrier */
        public ?object $nextExtract = null;
        public int $extractCalls = 0;
        public int $injectCalls = 0;
        /** What inject writes into the carrier (simulates a real tracer's serialization) */
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
            return $this->nextExtract;
        }

        /** @param array<string,mixed> $carrier */
        public function inject(object $spanContext, array &$carrier): void
        {
            $this->calls[] = 'inject';
            $this->injectCalls++;
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

    // The three span names are literals on purpose: deriving them from the production helper
    // would make a renamed operation or trace type invisible to this file.
    $opSend = 'Topic OT_Topic Pub';
    $opConsume = 'Topic OT_Topic SubBefore';
    $opEndTx = 'Topic OT_Topic EndTransaction';

    // 1) hookName
    $tracer = new FakeTracer();
    $hook = new OpenTracingHook($tracer);
    $check($hook->hookName() === 'OpenTracingHook', 'hookName');

    // 1b) The operation-name style itself: ``Topic <name> <trace type value>``
    $check(OpenTracingHook::operationName('T1', TraceType::PUB) === 'Topic T1 Pub'
        && OpenTracingHook::operationName('T1', TraceType::SUB_BEFORE) === 'Topic T1 SubBefore'
        && OpenTracingHook::operationName('T1', TraceType::END_TRANSACTION) === 'Topic T1 EndTransaction',
        'operation name is "Topic <topic> <trace type>"',
        OpenTracingHook::operationName('T1', TraceType::SUB_AFTER));

    // 2) Send-side before: span + tags + carrier injection
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
    $check(($tags[TraceConstants::ROCKETMQ_SERVICE] ?? '') === TraceConstants::ROCKETMQ_SERVICE
        && ($tags[TraceConstants::ROCKETMQ_TAGS] ?? '') === 'TagA'
        && ($tags[TraceConstants::ROCKETMQ_KEYS] ?? '') === 'key-1'
        && ($tags[TraceConstants::ROCKETMQ_BODY_LENGTH] ?? -1) === strlen('hello-1')
        && ($tags[TraceConstants::ROCKETMQ_MSG_TYPE] ?? '') === 'ORDER_MSG'
        && ($tags[OpenTracingHook::TAG_PRODUCER_GROUP] ?? '') === 'GID_OT',
        'send span carries the documented tag set', json_encode($tags));
    $span0 = $tracer->spans[0] ?? null;
    $check($span0 !== null && ($span0->tags[OpenTracingHook::TAG_BROKER_ADDR] ?? '') === '127.0.0.1:10911',
        'send span tags the broker address', json_encode($span0->tags ?? []));
    $check(($msg->getUserProperty(OpenTracingHook::CARRIER_PROP) ?? '') !== '', 'carrier injected into the OT_CARRIER property');
    $carrierDecoded = json_decode((string) $msg->getUserProperty(OpenTracingHook::CARRIER_PROP), true);
    $check(is_array($carrierDecoded) && ($carrierDecoded['traceparent'] ?? '') === $tracer->carrierValue['traceparent'],
        'OT_CARRIER property is the JSON written by inject', (string) $msg->getUserProperty(OpenTracingHook::CARRIER_PROP));
    $check($tracer->injectCalls === 1, 'inject exactly once');

    // 2b) Control: with no broker address in the context the tag is absent rather than empty,
    //     and the message properties other than the carrier are untouched.
    $tracerNoBroker = new FakeTracer();
    $hookNoBroker = new OpenTracingHook($tracerNoBroker);
    $ctxNoBroker = new SendMessageContext();
    $ctxNoBroker->producerGroup = 'GID_OT';
    $ctxNoBroker->message = new Message('OT_Topic', 'hello-nb');
    $hookNoBroker->sendMessageBefore($ctxNoBroker);
    $nbSpan = $tracerNoBroker->spans[0] ?? null;
    $nbTags = $tracerNoBroker->spanOpts[$opSend][0]['tags'] ?? [];
    $check($nbSpan !== null && !array_key_exists(OpenTracingHook::TAG_BROKER_ADDR, $nbSpan->tags)
        && ($nbTags[TraceConstants::ROCKETMQ_TAGS] ?? 'x') === ''
        && ($nbTags[TraceConstants::ROCKETMQ_KEYS] ?? 'x') === ''
        && ($nbTags[TraceConstants::ROCKETMQ_MSG_TYPE] ?? '') === 'NORMAL_MSG'
        && ($nbTags[TraceConstants::ROCKETMQ_BODY_LENGTH] ?? -1) === strlen('hello-nb'),
        'an empty context reports empty tag values and no broker address',
        json_encode($nbTags) . ' +' . json_encode($nbSpan->tags ?? []));

    // 3) Repeating before on the same message (client retry): no span stacking, no re-injection
    $propBefore = $msg->getUserProperty(OpenTracingHook::CARRIER_PROP);
    $hook->sendMessageBefore($ctx);
    $check($count($tracer->calls, 'startSpan:' . $opSend) === 1 && $tracer->injectCalls === 1
        && $msg->getUserProperty(OpenTracingHook::CARRIER_PROP) === $propBefore,
        'repeated before is idempotent (retry neither stacks spans nor re-injects)');

    // 4) Send-side after success: tags + finish once
    $ctx->sendResult = new SendResult(SendStatus::SEND_OK, 'OT_MSGID_1', null, 7);
    $hook->sendMessageAfter($ctx);
    $check($count($tracer->calls, 'finish:' . $opSend) === 1, 'send after finish exactly once');
    $sendSpan = $tracer->spans[0] ?? null;
    $check($sendSpan !== null && ($sendSpan->tags[OpenTracingHook::TAG_SEND_STATUS] ?? '') === 'SEND_OK'
        && ($sendSpan->tags[TraceConstants::ROCKETMQ_MSG_ID] ?? '') === 'OT_MSGID_1'
        && ($sendSpan->tags[OpenTracingHook::TAG_QUEUE_OFFSET] ?? -1) === 7
        && ($sendSpan->tags[TraceConstants::ROCKETMQ_SUCCESS] ?? null) === true,
        'send after records send_status/msg_id/queue_offset/success', json_encode($sendSpan->tags ?? []));

    // 4b) Repeated after: the span is already detached, so nothing else is finished or tagged.
    $tagsAfter = $sendSpan->tags;
    $tracer->calls = [];
    $hook->sendMessageAfter($ctx);
    $check($tracer->calls === [] && $sendSpan->tags === $tagsAfter,
        'repeated send after records nothing (span detached bookkeeping)');

    // 5) Send-side after exception: error tag + log (new message, new context)
    $tracer2 = new FakeTracer();
    $hook2 = new OpenTracingHook($tracer2);
    $ctx2 = $mkSendCtx();
    $ctx2->message = new Message('OT_Topic', 'hello-2');
    $hook2->sendMessageBefore($ctx2);
    $ctx2->exception = new \RuntimeException('connect refused');
    $hook2->sendMessageAfter($ctx2);
    $check($count($tracer2->calls, 'finish:' . $opSend) === 1, 'exception path finish exactly once');
    $errSpan = $tracer2->spans[0] ?? null;
    $check($errSpan !== null && ($errSpan->tags['error'] ?? false) === true
        && ($errSpan->tags[TraceConstants::ROCKETMQ_SUCCESS] ?? null) === false
        && ($errSpan->logs[0]['error.object'] ?? '') === \RuntimeException::class,
        'exception path error tag + log(error.object)', json_encode($errSpan->tags ?? []) . ' logs=' . json_encode($errSpan->logs ?? []));

    // 6) Consume side: extract from message => child-of refs => consume span
    $tracer3 = new FakeTracer();
    $parent = new FakeSpanContext('parent-ctx');
    $tracer3->nextExtract = $parent;
    $hook3 = new OpenTracingHook($tracer3);
    $in = new MessageExt('OT_Topic', 'hello-1');
    $in->reconsumeTimes = 2;
    $in->setUserProperty(OpenTracingHook::CARRIER_PROP, json_encode($tracer3->carrierValue));
    $cctx = new ConsumeMessageContext('GID_OT', [$in], new MessageQueue('OT_Topic', 'broker-a', 0));
    $hook3->consumeMessageBefore($cctx);
    $check($tracer3->extractCalls === 1, 'consume before extracts from the first message carrying a carrier');
    $cOpts = $tracer3->spanOpts[$opConsume][0] ?? [];
    $check(($cOpts['refs'][0] ?? null) === $parent, 'consume span refs=child-of(upstream context)');
    $cSpan = $tracer3->spans[0] ?? null;
    $cTags = $cOpts['tags'] ?? [];
    $check(($cTags[TraceConstants::ROCKETMQ_SERVICE] ?? '') === TraceConstants::ROCKETMQ_SERVICE
        && ($cTags[OpenTracingHook::TAG_CONSUMER_GROUP] ?? '') === 'GID_OT'
        && ($cTags[TraceConstants::ROCKETMQ_RETRY_TIMERS] ?? -1) === 2
        && ($cSpan?->tags[OpenTracingHook::TAG_MESSAGE_QUEUE] ?? '') === 'OT_Topic#0@broker-a',
        'consume span tags (group/retry times/mq)', json_encode($cTags));

    // 7) Consume before idempotence: repeated calls do not stack spans
    $hook3->consumeMessageBefore($cctx);
    $check($count($tracer3->calls, 'startSpan:' . $opConsume) === 1 && $tracer3->extractCalls === 1,
        'consume before is idempotent');

    // 8) Consume after: success/status tags + finish; repeated after is idempotent
    $cctx->success = true;
    $cctx->status = 'CONSUME_SUCCESS';
    $hook3->consumeMessageAfter($cctx);
    $hook3->consumeMessageAfter($cctx);
    $check($count($tracer3->calls, 'finish:' . $opConsume) === 1, 'consume after finish exactly once (repeated calls idempotent)');
    $check(($cSpan?->tags[TraceConstants::ROCKETMQ_SUCCESS] ?? null) === true
        && ($cSpan?->tags[OpenTracingHook::TAG_CONSUME_STATUS] ?? '') === 'CONSUME_SUCCESS',
        'consume after records success/status', json_encode($cSpan->tags ?? []));

    // 9) Consume side without carrier: no extract call at all and no refs
    $tracer4 = new FakeTracer();
    $hook4 = new OpenTracingHook($tracer4);
    $plain = new MessageExt('OT_Topic', 'no-carrier');
    $cctx4 = new ConsumeMessageContext('GID_OT', [$plain], null);
    $hook4->consumeMessageBefore($cctx4);
    $cOpts4 = $tracer4->spanOpts[$opConsume][0] ?? [];
    $check(!isset($cOpts4['refs']) && $count($tracer4->calls, 'startSpan:' . $opConsume) === 1
        && $tracer4->extractCalls === 0,
        'message without carrier: span opened but no refs and no extract');

    // 10) Cross-process closed loop: properties injected on the send side => brand-new Message
    //     (simulating peer-side decoding) => consume side gets the same context
    $tracer5 = new FakeTracer();
    $hook5 = new OpenTracingHook($tracer5);
    $sendCtx5 = $mkSendCtx();
    $sendCtx5->message = new Message('OT_Topic', 'round-trip');
    $hook5->sendMessageBefore($sendCtx5);
    $carrierRaw = (string) $sendCtx5->message->getUserProperty(OpenTracingHook::CARRIER_PROP);
    // ---- the "cross-process" boundary: the peer receives a bare property string ----
    $downstream = new MessageExt('OT_Topic', 'round-trip');
    $downstream->setUserProperty(OpenTracingHook::CARRIER_PROP, $carrierRaw);
    $ctx5 = new FakeSpanContext('round-trip-ctx');
    $tracer5->nextExtract = $ctx5;
    $cctx5 = new ConsumeMessageContext('GID_OT_Down', [$downstream], null);
    $hook5->consumeMessageBefore($cctx5);
    $cOpts5 = $tracer5->spanOpts[$opConsume][0] ?? [];
    $check(($cOpts5['refs'][0] ?? null) === $ctx5, 'cross-process closed loop: downstream extract gets the upstream context');

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
    $txTags = $tracer6->spanOpts[$opEndTx][0]['tags'] ?? [];
    $check($count($tracer6->calls, 'startSpan:' . $opEndTx) === 1
        && $count($tracer6->calls, 'finish:' . $opEndTx) === 1,
        'end transaction opens and closes exactly one span');
    $check(($txTags[TraceConstants::ROCKETMQ_TRANSACTION_ID] ?? '') === 'HALF_OT_1'
        && ($txTags[TraceConstants::ROCKETMQ_TRANSACTION_STATE] ?? '') === 'CommitMessage'
        && ($txTags[TraceConstants::ROCKETMQ_IS_FROM_TRANSACTION_CHECK] ?? null) === true
        && ($txTags[OpenTracingHook::TAG_PRODUCER_GROUP] ?? '') === 'GID_OT'
        && ($txSpan?->tags[TraceConstants::ROCKETMQ_MSG_ID] ?? '') === 'OT_MSGID_TX'
        && ($txSpan?->tags[OpenTracingHook::TAG_BROKER_ADDR] ?? '') === '127.0.0.1:10912'
        && ($txSpan?->tags[TraceConstants::ROCKETMQ_SUCCESS] ?? null) === true,
        'end transaction span carries the transaction identity', json_encode($txTags) . ' +' . json_encode($txSpan->tags ?? []));
    $check($txMsg->properties === $propsBefore,
        'end transaction leaves the message byte-identical (no carrier on this leg)', json_encode($txMsg->properties));

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
    $execCtx->sendResult = new SendResult(SendStatus::SEND_OK, 'OT_MSGID_EXEC', null, 1);
    $producer->executeSendMessageHookAfter($execCtx);
    $execTx = new EndTransactionContext();
    $execTx->producerGroup = 'GID_OT_EXEC';
    $execTx->message = $execCtx->message;
    $execTx->transactionId = 'HALF_OT_EXEC';
    $producer->executeEndTransactionHook($execTx);
    $check($count($tracer7->calls, 'startSpan:' . $opSend) === 1
        && $count($tracer7->calls, 'finish:' . $opSend) === 1
        && $count($tracer7->calls, 'startSpan:' . $opEndTx) === 1,
        'producer executors drive send before/after and end transaction', json_encode($tracer7->calls));

    $tracerIdle = new FakeTracer();
    $bare = new DefaultMQProducer('GID_OT_BARE');
    $bareCtx = $mkSendCtx();
    $bareCtx->message = new Message('OT_Topic', 'unhooked');
    $bare->executeSendMessageHookBefore($bareCtx);
    $bare->executeSendMessageHookAfter($bareCtx);
    $bare->executeEndTransactionHook($execTx);
    $check($tracerIdle->calls === [] && ($bareCtx->message->getUserProperty(OpenTracingHook::CARRIER_PROP) ?? '') === '',
        'control: an unregistered producer records nothing and writes no carrier', json_encode($tracerIdle->calls));


    echo PHP_EOL;
    if ($failed === 0) {
        printf("ALL TESTS PASSED (%d checks)%s", $passed, PHP_EOL);
        exit(0);
    }
    printf("TESTS FAILED: %d/%d%s", $failed, $passed + $failed, PHP_EOL);
    exit(1);
}
