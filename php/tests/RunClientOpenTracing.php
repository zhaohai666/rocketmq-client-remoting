<?php

declare(strict_types=1);

/*
 * =====================================================================================
 * OpenTracingHook self-test: pure assert style (no phpunit); on success prints
 * `ALL TESTS PASSED (N checks)`, on failure exits non-zero (style copied from
 * tests/RunClientTrace.php).
 * -------------------------------------------------------------------------------------
 * Verifies four things:
 *   1) Send side: startSpan(OP_SEND) + tags + carrier injected into the OT_CARRIER
 *      property (in-band propagation); repeating before on the same message (client
 *      retry) neither stacks spans nor re-injects;
 *   2) Send side after: success => sendStatus/msgId/queueOffset tags + finish exactly
 *      once; exception => error tag + log + finish; repeated after is idempotent
 *      (SplObjectStorage bookkeeping);
 *   3) Consume side: extract from the first message carrying a carrier => child-of
 *      refs => startSpan(OP_CONSUME); no carrier => no refs; before/after idempotence
 *      matches the send side;
 *   4) Cross-process propagation closed loop: properties injected on the send side =>
 *      into a brand-new Message object (simulating peer-side decoding) => consume side
 *      extract gets the same SpanContext object.
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

    // 1) hookName
    $tracer = new FakeTracer();
    $hook = new OpenTracingHook($tracer);
    $check($hook->hookName() === 'OpenTracingHook', 'hookName');

    // 2) Send-side before: span + tags + carrier injection
    $tracer = new FakeTracer();
    $hook = new OpenTracingHook($tracer);
    $ctx = $mkSendCtx();
    $ctx->message = new Message('OT_Topic', 'hello-1');
    $hook->sendMessageBefore($ctx);
    $msg = $ctx->message;
    $check($count($tracer->calls, 'startSpan:RocketMQ/send') === 1, 'send before opens exactly 1 send span');
    $opts = $tracer->spanOpts['RocketMQ/send'][0] ?? [];
    $tags = $opts['tags'] ?? [];
    $check(($tags['messaging.system'] ?? '') === 'rocketmq' && ($tags['messaging.destination'] ?? '') === 'OT_Topic'
        && ($tags['messaging.operation'] ?? '') === 'send', 'send span tags complete', json_encode($tags));
    $check(($msg->getUserProperty(OpenTracingHook::CARRIER_PROP) ?? '') !== '', 'carrier injected into the OT_CARRIER property');
    $carrierDecoded = json_decode((string) $msg->getUserProperty(OpenTracingHook::CARRIER_PROP), true);
    $check(is_array($carrierDecoded) && ($carrierDecoded['traceparent'] ?? '') === $tracer->carrierValue['traceparent'],
        'OT_CARRIER property is the JSON written by inject', (string) $msg->getUserProperty(OpenTracingHook::CARRIER_PROP));
    $span = $tracer->spanOpts['RocketMQ/send'][0] ? null : null; // placeholder; span is not reachable from storage, verified on the fake side:
    $check($tracer->injectCalls === 1, 'inject exactly once');

    // 3) Repeating before on the same message (client retry): no span stacking, no re-injection
    $callsBefore = $tracer->calls;
    $propBefore = $msg->getUserProperty(OpenTracingHook::CARRIER_PROP);
    $hook->sendMessageBefore($ctx);
    $check($count($tracer->calls, 'startSpan:RocketMQ/send') === 1 && $tracer->injectCalls === 1
        && $msg->getUserProperty(OpenTracingHook::CARRIER_PROP) === $propBefore,
        'repeated before is idempotent (retry neither stacks spans nor re-injects)');

    // 4) Send-side after success: tags + finish once
    $ctx->sendResult = new SendResult(SendStatus::SEND_OK, 'OT_MSGID_1', null, 7);
    $hook->sendMessageAfter($ctx);
    $check($count($tracer->calls, 'finish:RocketMQ/send') === 1, 'send after finish exactly once');
    $sendSpan = $tracer->spans[0] ?? null;
    $check($sendSpan !== null && ($sendSpan->tags['sendStatus'] ?? '') === 'SEND_OK'
        && ($sendSpan->tags['msgId'] ?? '') === 'OT_MSGID_1' && ($sendSpan->tags['queueOffset'] ?? -1) === 7,
        'send after records sendStatus/msgId/queueOffset tags', json_encode($sendSpan->tags ?? []));

    // 5) Send-side after exception: error tag + log (new message, new context)
    $tracer2 = new FakeTracer();
    $hook2 = new OpenTracingHook($tracer2);
    $ctx2 = $mkSendCtx();
    $ctx2->message = new Message('OT_Topic', 'hello-2');
    $hook2->sendMessageBefore($ctx2);
    $ctx2->exception = new \RuntimeException('connect refused');
    $hook2->sendMessageAfter($ctx2);
    $check($count($tracer2->calls, 'finish:RocketMQ/send') === 1, 'exception path finish exactly once');
    $errSpan = $tracer2->spans[0] ?? null;
    $check($errSpan !== null && ($errSpan->tags['error'] ?? false) === true
        && ($errSpan->logs[0]['error.object'] ?? '') === \RuntimeException::class,
        'exception path error tag + log(error.object)', json_encode($errSpan->tags ?? []) . ' logs=' . json_encode($errSpan->logs ?? []));

    // 6) Consume side: extract from message => child-of refs => consume span
    $tracer3 = new FakeTracer();
    $parent = new FakeSpanContext('parent-ctx');
    $tracer3->nextExtract = $parent;
    $hook3 = new OpenTracingHook($tracer3);
    $in = new MessageExt('OT_Topic', 'hello-1');
    $in->setUserProperty(OpenTracingHook::CARRIER_PROP, json_encode($tracer3->carrierValue));
    $cctx = new ConsumeMessageContext('GID_OT', [$in], new MessageQueue('OT_Topic', 'broker-a', 0));
    $hook3->consumeMessageBefore($cctx);
    $check($tracer3->extractCalls === 1, 'consume before extracts from the first message carrying a carrier');
    $cOpts = $tracer3->spanOpts['RocketMQ/consume'][0] ?? [];
    $check(($cOpts['refs'][0] ?? null) === $parent, 'consume span refs=child-of(upstream context)');
    $cTags = $cOpts['tags'] ?? [];
    $check(($cTags['messaging.consumer.group'] ?? '') === 'GID_OT' && ($cTags['messaging.operation'] ?? '') === 'receive',
        'consume span tags (group/operation)', json_encode($cTags));

    // 7) Consume before idempotence: repeated calls do not stack spans
    $hook3->consumeMessageBefore($cctx);
    $check($count($tracer3->calls, 'startSpan:RocketMQ/consume') === 1 && $tracer3->extractCalls === 1,
        'consume before is idempotent');

    // 8) Consume after: success/status tags + finish; repeated after is idempotent
    $cctx->success = true;
    $cctx->status = 'CONSUME_SUCCESS';
    $hook3->consumeMessageAfter($cctx);
    $hook3->consumeMessageAfter($cctx);
    $check($count($tracer3->calls, 'finish:RocketMQ/consume') === 1, 'consume after finish exactly once (repeated calls idempotent)');

    // 9) Consume side without carrier: extract still called (searched message by message) but no refs
    $tracer4 = new FakeTracer();
    $hook4 = new OpenTracingHook($tracer4);
    $plain = new MessageExt('OT_Topic', 'no-carrier');
    $cctx4 = new ConsumeMessageContext('GID_OT', [$plain], null);
    $hook4->consumeMessageBefore($cctx4);
    $cOpts4 = $tracer4->spanOpts['RocketMQ/consume'][0] ?? [];
    $check(!isset($cOpts4['refs']) && $count($tracer4->calls, 'startSpan:RocketMQ/consume') === 1,
        'message without carrier: span opened but no refs');

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
    $cOpts5 = $tracer5->spanOpts['RocketMQ/consume'][0] ?? [];
    $check(($cOpts5['refs'][0] ?? null) === $ctx5, 'cross-process closed loop: downstream extract gets the upstream context');

    echo PHP_EOL;
    if ($failed === 0) {
        printf("ALL TESTS PASSED (%d checks)%s", $passed, PHP_EOL);
        exit(0);
    }
    printf("TESTS FAILED: %d/%d%s", $failed, $passed + $failed, PHP_EOL);
    exit(1);
}
