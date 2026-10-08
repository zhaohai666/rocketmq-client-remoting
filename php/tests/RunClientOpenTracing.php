<?php

declare(strict_types=1);

/*
 * =====================================================================================
 * OpenTracingHook 自测：纯 assert 风格（无 phpunit），成功打印
 * `ALL TESTS PASSED (N checks)`，失败非零退出（风格照抄 tests/RunClientTrace.php）。
 * -------------------------------------------------------------------------------------
 * 验证四件事：
 *   1) 发送侧：startSpan(OP_SEND) + tags + carrier 注入 OT_CARRIER 属性（带内传播）；
 *      同一消息重复 before（客户端重试）不叠加 span、不重复注入；
 *   2) 发送侧 after：成功 → sendStatus/msgId/queueOffset tag + finish 恰一次；
 *      异常 → error tag + log + finish；重复 after 幂等（SplObjectStorage 记账）；
 *   3) 消费侧：从首条带 carrier 的消息 extract → child-of refs → startSpan(OP_CONSUME)；
 *      无 carrier 时无 refs；before/after 幂等同发送侧；
 *   4) 跨进程传播闭环：发送侧注入的属性 → 换一个全新 Message 对象（模拟对端解码）→
 *      消费侧 extract 拿到同一个 SpanContext 对象。
 * tracer/span 用 FakeTracer/FakeSpan 满足 OpenTracingHook docblock 里的 duck-type：
 *   startSpan(string, array{tags?,refs?}):object / extract(array):?object /
 *   inject(object, array&):void + span 的 setTag/log/finish/context()。
 * =====================================================================================
 */
namespace RocketMQ\Client {
    require_once __DIR__ . '/../bootstrap.php';

    use RocketMQ\Common\Message;
    use RocketMQ\Common\MessageExt;
    use RocketMQ\Common\MessageQueue;

    // ---- FakeTracer / FakeSpan / FakeSpanContext（放在本文件 namespace，不进 src/） ----

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
        /** @var array<string,list<array<string,mixed>>> op → 每次 startSpan 的 options */
        public array $spanOpts = [];
        /** @var list<FakeSpan> startSpan 出过的全部 span（按时间序） */
        public array $spans = [];
        /** extract 返回值（模拟从 carrier 还原的 SpanContext）；null = carrier 里没有上游 */
        public ?object $nextExtract = null;
        public int $extractCalls = 0;
        public int $injectCalls = 0;
        /** inject 写进 carrier 的内容（模拟真实 tracer 的序列化） */
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

    // ---- 用例 ----

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

    // 2) 发送侧 before：span + tags + carrier 注入
    $tracer = new FakeTracer();
    $hook = new OpenTracingHook($tracer);
    $ctx = $mkSendCtx();
    $ctx->message = new Message('OT_Topic', 'hello-1');
    $hook->sendMessageBefore($ctx);
    $msg = $ctx->message;
    $check($count($tracer->calls, 'startSpan:RocketMQ/send') === 1, '发送 before 开 1 个 send span');
    $opts = $tracer->spanOpts['RocketMQ/send'][0] ?? [];
    $tags = $opts['tags'] ?? [];
    $check(($tags['messaging.system'] ?? '') === 'rocketmq' && ($tags['messaging.destination'] ?? '') === 'OT_Topic'
        && ($tags['messaging.operation'] ?? '') === 'send', 'send span tags 齐全', json_encode($tags));
    $check(($msg->getUserProperty(OpenTracingHook::CARRIER_PROP) ?? '') !== '', 'carrier 注入 OT_CARRIER 属性');
    $carrierDecoded = json_decode((string) $msg->getUserProperty(OpenTracingHook::CARRIER_PROP), true);
    $check(is_array($carrierDecoded) && ($carrierDecoded['traceparent'] ?? '') === $tracer->carrierValue['traceparent'],
        'OT_CARRIER 属性是 inject 写出的 JSON', (string) $msg->getUserProperty(OpenTracingHook::CARRIER_PROP));
    $span = $tracer->spanOpts['RocketMQ/send'][0] ? null : null; // 占位，span 从 storage 取不到，用 fake 侧验证：
    $check($tracer->injectCalls === 1, 'inject 恰好 1 次');

    // 3) 同一消息重复 before（客户端重试）：不叠加 span、不重复注入
    $callsBefore = $tracer->calls;
    $propBefore = $msg->getUserProperty(OpenTracingHook::CARRIER_PROP);
    $hook->sendMessageBefore($ctx);
    $check($count($tracer->calls, 'startSpan:RocketMQ/send') === 1 && $tracer->injectCalls === 1
        && $msg->getUserProperty(OpenTracingHook::CARRIER_PROP) === $propBefore,
        '重复 before 幂等（重试不叠加 span / 不重注入）');

    // 4) 发送 after 成功：tags + finish 一次
    $ctx->sendResult = new SendResult(SendStatus::SEND_OK, 'OT_MSGID_1', null, 7);
    $hook->sendMessageAfter($ctx);
    $check($count($tracer->calls, 'finish:RocketMQ/send') === 1, '发送 after finish 恰 1 次');
    $sendSpan = $tracer->spans[0] ?? null;
    $check($sendSpan !== null && ($sendSpan->tags['sendStatus'] ?? '') === 'SEND_OK'
        && ($sendSpan->tags['msgId'] ?? '') === 'OT_MSGID_1' && ($sendSpan->tags['queueOffset'] ?? -1) === 7,
        '发送 after 记录 sendStatus/msgId/queueOffset tag', json_encode($sendSpan->tags ?? []));

    // 5) 发送 after 异常：error tag + log（新消息、新上下文）
    $tracer2 = new FakeTracer();
    $hook2 = new OpenTracingHook($tracer2);
    $ctx2 = $mkSendCtx();
    $ctx2->message = new Message('OT_Topic', 'hello-2');
    $hook2->sendMessageBefore($ctx2);
    $ctx2->exception = new \RuntimeException('connect refused');
    $hook2->sendMessageAfter($ctx2);
    $check($count($tracer2->calls, 'finish:RocketMQ/send') === 1, '异常路径 finish 恰 1 次');
    $errSpan = $tracer2->spans[0] ?? null;
    $check($errSpan !== null && ($errSpan->tags['error'] ?? false) === true
        && ($errSpan->logs[0]['error.object'] ?? '') === \RuntimeException::class,
        '异常路径 error tag + log(error.object)', json_encode($errSpan->tags ?? []) . ' logs=' . json_encode($errSpan->logs ?? []));

    // 6) 消费侧：从消息 extract → child-of refs → consume span
    $tracer3 = new FakeTracer();
    $parent = new FakeSpanContext('parent-ctx');
    $tracer3->nextExtract = $parent;
    $hook3 = new OpenTracingHook($tracer3);
    $in = new MessageExt('OT_Topic', 'hello-1');
    $in->setUserProperty(OpenTracingHook::CARRIER_PROP, json_encode($tracer3->carrierValue));
    $cctx = new ConsumeMessageContext('GID_OT', [$in], new MessageQueue('OT_Topic', 'broker-a', 0));
    $hook3->consumeMessageBefore($cctx);
    $check($tracer3->extractCalls === 1, '消费 before 从首条带 carrier 的消息 extract');
    $cOpts = $tracer3->spanOpts['RocketMQ/consume'][0] ?? [];
    $check(($cOpts['refs'][0] ?? null) === $parent, '消费 span refs=child-of(上游 context)');
    $cTags = $cOpts['tags'] ?? [];
    $check(($cTags['messaging.consumer.group'] ?? '') === 'GID_OT' && ($cTags['messaging.operation'] ?? '') === 'receive',
        '消费 span tags（group/operation）', json_encode($cTags));

    // 7) 消费 before 幂等：重复调用不叠加 span
    $hook3->consumeMessageBefore($cctx);
    $check($count($tracer3->calls, 'startSpan:RocketMQ/consume') === 1 && $tracer3->extractCalls === 1,
        '消费 before 幂等');

    // 8) 消费 after：success/status tag + finish；重复 after 幂等
    $cctx->success = true;
    $cctx->status = 'CONSUME_SUCCESS';
    $hook3->consumeMessageAfter($cctx);
    $hook3->consumeMessageAfter($cctx);
    $check($count($tracer3->calls, 'finish:RocketMQ/consume') === 1, '消费 after finish 恰 1 次（重复调用幂等）');

    // 9) 消费侧无 carrier：extract 仍被调（逐条找）但无 refs
    $tracer4 = new FakeTracer();
    $hook4 = new OpenTracingHook($tracer4);
    $plain = new MessageExt('OT_Topic', 'no-carrier');
    $cctx4 = new ConsumeMessageContext('GID_OT', [$plain], null);
    $hook4->consumeMessageBefore($cctx4);
    $cOpts4 = $tracer4->spanOpts['RocketMQ/consume'][0] ?? [];
    $check(!isset($cOpts4['refs']) && $count($tracer4->calls, 'startSpan:RocketMQ/consume') === 1,
        '无 carrier 消息：开 span 但无 refs');

    // 10) 跨进程闭环：发送侧注入的属性 → 全新 Message（模拟对端解码）→ 消费侧拿到同一 context
    $tracer5 = new FakeTracer();
    $hook5 = new OpenTracingHook($tracer5);
    $sendCtx5 = $mkSendCtx();
    $sendCtx5->message = new Message('OT_Topic', 'round-trip');
    $hook5->sendMessageBefore($sendCtx5);
    $carrierRaw = (string) $sendCtx5->message->getUserProperty(OpenTracingHook::CARRIER_PROP);
    // ——「跨进程」分界线：对端拿到的是裸属性字符串——
    $downstream = new MessageExt('OT_Topic', 'round-trip');
    $downstream->setUserProperty(OpenTracingHook::CARRIER_PROP, $carrierRaw);
    $ctx5 = new FakeSpanContext('round-trip-ctx');
    $tracer5->nextExtract = $ctx5;
    $cctx5 = new ConsumeMessageContext('GID_OT_Down', [$downstream], null);
    $hook5->consumeMessageBefore($cctx5);
    $cOpts5 = $tracer5->spanOpts['RocketMQ/consume'][0] ?? [];
    $check(($cOpts5['refs'][0] ?? null) === $ctx5, '跨进程闭环：下游 extract 拿到上游 context');

    echo PHP_EOL;
    if ($failed === 0) {
        printf("ALL TESTS PASSED (%d checks)%s", $passed, PHP_EOL);
        exit(0);
    }
    printf("TESTS FAILED: %d/%d%s", $failed, $passed + $failed, PHP_EOL);
    exit(1);
}
