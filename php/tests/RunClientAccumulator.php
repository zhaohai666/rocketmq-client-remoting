<?php

declare(strict_types=1);

// 先装 autoloader，再判 SendResult 是否已由并行子任务落地 —— 顺序不能反，否则真实类缺席时
// 会误判并声明替身，导致稍后真实文件被 require 时 "name is already in use"。
namespace {
    require_once __DIR__ . '/../bootstrap.php';
}

// ============================================================================================
// SendResult 兜底定义（仅当并行子任务尚未落地 php/src/Client/SendResult.php 时生效）。
//
// 按约定本文件**不**创建 php/src/Client/SendResult.php：那是并行子任务的产物。这里只是在
// 自测运行时，若真实类缺席，补一个同形状（Java/Python 构造器顺序 + camelCase 公有属性）的
// 替身，好让本套件在当前工作区也能独立跑通；真实类一旦落地，class_exists 命中，本替身自动失效。
// ============================================================================================
namespace RocketMQ\Client {
    if (!class_exists(\RocketMQ\Client\SendResult::class)) {
        final class SendResult
        {
            public function __construct(
                public mixed $sendStatus = null,
                public ?string $msgId = null,
                public ?\RocketMQ\Common\MessageQueue $messageQueue = null,
                public int $queueOffset = 0,
                public ?string $transactionId = null,
                public ?string $offsetMsgId = null,
                public ?string $regionId = null,
                public bool $traceOn = true,
                public ?string $recallHandle = null,
            ) {
            }

            public function getSendStatus(): mixed
            {
                return $this->sendStatus;
            }

            public function getMsgId(): ?string
            {
                return $this->msgId;
            }

            public function getMessageQueue(): ?\RocketMQ\Common\MessageQueue
            {
                return $this->messageQueue;
            }

            public function getQueueOffset(): int
            {
                return $this->queueOffset;
            }

            public function getOffsetMsgId(): ?string
            {
                return $this->offsetMsgId;
            }
        }
    }
}

namespace {
    use RocketMQ\Client\AggregateKey;
    use RocketMQ\Client\InternalSendCallback;
    use RocketMQ\Client\ProduceAccumulator;
    use RocketMQ\Client\SendResult;
    use RocketMQ\Common\Message;
    use RocketMQ\Common\MessageBatch;
    use RocketMQ\Common\MessageQueue;

    /**
     * ProduceAccumulator（自动攒批）纯 PHP assert 风格自测（不依赖 phpunit，不连真机）。
     *
     * 运行：C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunClientAccumulator.php
     * 全部通过输出 "ALL TESTS PASSED (N checks)"，任一失败列出明细并以非零码退出。
     *
     * 单线程适配：蓝本的守卫线程 → 显式 pump（tick/flush/flushAll）；"当前时间"与发送函数都注入。
     */
    final class RunClientAccumulator
    {
        private int $passed = 0;

        /** @var list<string> */
        private array $failures = [];

        private int $now = 1_000_000;

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
                $detail = sprintf(
                    "%s\n  expected: %s\n  actual:   %s",
                    $label,
                    var_export($expected, true),
                    var_export($actual, true)
                );
                $this->failures[] = $label;
                fwrite(STDERR, "FAIL: {$detail}\n");
            }
        }

        /**
         * @param callable(): mixed         $fn
         * @param class-string<\Throwable>  $exceptionClass
         */
        public function checkThrows(callable $fn, string $exceptionClass, string $label): void
        {
            try {
                $fn();
                $this->check(false, "{$label} (no exception thrown)");
            } catch (\Throwable $e) {
                $this->check(
                    $e instanceof $exceptionClass,
                    sprintf('%s (got %s: %s)', $label, get_class($e), $e->getMessage())
                );
            }
        }

        public function summary(): int
        {
            if ($this->failures === []) {
                printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
                return 0;
            }
            printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
            return 1;
        }

        private function sendOkStatus(): mixed
        {
            return class_exists(\RocketMQ\Client\SendStatus::class)
                ? \RocketMQ\Client\SendStatus::SEND_OK
                : null;
        }

        /** 注入的假发送函数：记账 + 按配置回一份批量 SendResult（含可选的失败） */
        private function fakeSender(
            string $msgIds = 'single-id',
            string $offsetIds = 'off-single',
            int $queueOffset = 0,
            int $failAt = -1,
        ): FakeSender {
            return new FakeSender($msgIds, $offsetIds, $queueOffset, $failAt, $this->sendOkStatus());
        }

        private function clock(): \Closure
        {
            return fn(): int => $this->now;
        }

        // ==================================================================== 归并键

        private function testAggregateKeyEqualityAndHash(): void
        {
            $m = new Message('TopicA', 'x', 'TagA');
            $same = new Message('TopicA', 'y', 'TagA');

            $k1 = AggregateKey::ofMessage($m);
            $k2 = AggregateKey::ofMessage($same);
            $this->check($k1->equals($k2), 'AggregateKey: 同 topic/tag 相等');
            $this->checkSame($k1->keyString(), $k2->keyString(), 'AggregateKey: 相等的键 keyString 相同');

            $otherTag = AggregateKey::ofMessage(new Message('TopicA', 'y', 'TagB'));
            $this->check(!$k1->equals($otherTag), 'AggregateKey: tag 不同不相等');
            $this->check($k1->keyString() !== $otherTag->keyString(), 'AggregateKey: tag 不同 keyString 不同');

            $otherTopic = AggregateKey::ofMessage(new Message('TopicB', 'y', 'TagA'));
            $this->check(!$k1->equals($otherTopic), 'AggregateKey: topic 不同不相等');
            $this->check($k1->keyString() !== $otherTopic->keyString(), 'AggregateKey: topic 不同 keyString 不同');

            // 指定 mq 与不指定 mq 不合并
            $mq = new MessageQueue('TopicA', 'broker-a', 0);
            $withMq = AggregateKey::ofMessageWithMq($m, $mq);
            $this->check(!$k1->equals($withMq), 'AggregateKey: 带 mq 与不带 mq 不相等');
            $this->check($k1->keyString() !== $withMq->keyString(), 'AggregateKey: 带/不带 mq keyString 不同');
            $withMq2 = AggregateKey::ofMessageWithMq($same, new MessageQueue('TopicA', 'broker-a', 0));
            $this->check($withMq->equals($withMq2), 'AggregateKey: 同 mq 相等（MessageQueue.equals）');
            $this->checkSame($withMq->keyString(), $withMq2->keyString(), 'AggregateKey: 同 mq keyString 相同');
            $this->check(!$withMq->equals(AggregateKey::ofMessageWithMq($m, new MessageQueue('TopicA', 'broker-a', 1))), 'AggregateKey: queueId 不同不相等');

            // 空 tag（""）与缺省 tag（null）不是一回事（构造器会把 "" 折叠成缺省，故显式 setTags("")）
            $nullTag = AggregateKey::ofMessage(new Message('TopicA', 'x'));
            $emptyMsg = new Message('TopicA', 'x');
            $emptyMsg->setTags('');
            $emptyTag = AggregateKey::ofMessage($emptyMsg);
            $this->check(!$nullTag->equals($emptyTag), 'AggregateKey: tag=null 与 tag="" 不相等');
            $this->check($nullTag->keyString() !== $emptyTag->keyString(), 'AggregateKey: tag=null 与 tag="" keyString 不同');

            // waitStoreMsgOK 参与分区；且"没设过 WAIT"缺省即 true
            $w = new Message('TopicA', 'x', 'TagA');
            $w->setWaitStoreMsgOk(false);
            $kW = AggregateKey::ofMessage($w);
            $this->check(!$k1->equals($kW), 'AggregateKey: waitStoreMsgOK 不同不相等');
            $plain = new Message('TopicA', 'x', 'TagA');
            $this->checkSame(null, $plain->getWaitStoreMsgOk(), '普通消息未写 WAIT 属性');
            $this->checkSame(true, AggregateKey::ofMessage($plain)->waitStoreMsgOk, 'AggregateKey: WAIT 缺省即 true');
            $this->checkSame(true, AggregateKey::ofMessageWithMq($plain, $mq)->waitStoreMsgOk, 'ofMessageWithMq 的 WAIT 缺省即 true');
        }

        // ==================================================================== 参数与闸门

        private function testParamsAndByteGate(): void
        {
            $acc = new ProduceAccumulator('params', $this->fakeSender(), $this->clock());
            $this->checkSame(10, $acc->getBatchMaxDelayMs(), '默认 holdMs=10');
            $this->checkSame(32 * 1024, $acc->getBatchMaxBytes(), '默认 holdSize=32KB');
            $this->checkSame(32 * 1024 * 1024, $acc->getTotalHoldSize(), '默认 totalHoldSize=32MB');
            // Java 的 getTotalBatchMaxBytes 实际返回 holdSize（上游笔误，照抄）
            $this->checkSame(32 * 1024, $acc->getTotalBatchMaxBytes(), 'getTotalBatchMaxBytes 返回 holdSize');
            $this->checkSame(0, $acc->currentlyHoldSize(), '初始 currentlyHoldSize=0');

            $acc->batchMaxDelayMs(1);
            $this->checkSame(1, $acc->getBatchMaxDelayMs(), 'batchMaxDelayMs 下界 1 生效');
            $acc->batchMaxDelayMs(30 * 1000);
            $this->checkSame(30 * 1000, $acc->getBatchMaxDelayMs(), 'batchMaxDelayMs 上界 30s 生效');
            $this->checkThrows(fn() => $acc->batchMaxDelayMs(0), \InvalidArgumentException::class, 'batchMaxDelayMs(0) 抛异常');
            $this->checkThrows(fn() => $acc->batchMaxDelayMs(30 * 1000 + 1), \InvalidArgumentException::class, 'batchMaxDelayMs(>30s) 抛异常');

            $acc->batchMaxBytes(1);
            $acc->batchMaxBytes(2 * 1024 * 1024);
            $this->checkSame(2 * 1024 * 1024, $acc->getBatchMaxBytes(), 'batchMaxBytes 上界 2MB 生效');
            $this->checkThrows(fn() => $acc->batchMaxBytes(0), \InvalidArgumentException::class, 'batchMaxBytes(0) 抛异常');
            $this->checkThrows(fn() => $acc->batchMaxBytes(2 * 1024 * 1024 + 1), \InvalidArgumentException::class, 'batchMaxBytes(>2MB) 抛异常');

            $acc->totalBatchMaxBytes(1);
            $this->checkSame(1, $acc->getTotalHoldSize(), 'totalBatchMaxBytes 生效');
            $this->checkThrows(fn() => $acc->totalBatchMaxBytes(0), \InvalidArgumentException::class, 'totalBatchMaxBytes(0) 抛异常');

            // 全局字节闸门
            $acc->totalBatchMaxBytes(10);
            $msg10 = new Message('T', '1234567890'); // 10 字节
            $this->check($acc->tryAddMessage($msg10), '闸门：额度内放行');
            $this->checkSame(10, $acc->currentlyHoldSize(), '闸门：放行即记账');
            $this->check(!$acc->tryAddMessage(new Message('T', 'x')), '闸门：额度满拒绝');
            $this->checkSame(10, $acc->currentlyHoldSize(), '闸门：拒绝后额度不变');
            $acc->releaseHold(10);
            $this->checkSame(0, $acc->currentlyHoldSize(), '闸门：归还后额度为 0');
            $this->check($acc->tryAddMessage(new Message('T', 'x')), '闸门：归还后可再放行');
            $acc->releaseHold(1);
            $acc->totalBatchMaxBytes(5);
            $this->check($acc->tryAddMessage(new Message('T', '')), '闸门：空 body 放行');
            $this->checkSame(0, $acc->currentlyHoldSize(), '闸门：空 body 不记账');
        }

        // ==================================================================== 阈值触发 flush

        /**
         * 累积到 holdSize 阈值触发 flush。
         *
         * ⚠ 蓝本（python/client/produce_accumulator.py）**没有** MaxMessageNum / 条数阈值，
         * 只有两条判据：`messagesSize > holdSize`（本批字节）与 `now >= createTime + holdMs`。
         * 任务书里的 "MaxMessageNum / MaxMessageSize" 在本文件对应 holdSize（字节闸门），此处按蓝本实现。
         */
        private function testAccumulateToHoldSizeThresholdTriggersFlush(): void
        {
            $sender = $this->fakeSender('id-0,id-1,id-2,id-3', 'off-0,off-1,off-2,off-3', 100);
            $acc = new ProduceAccumulator('threshold', $sender, $this->clock());
            $acc->batchMaxBytes(16); // 严格大于才 ready：16 不触发，>16 触发

            $key = new AggregateKey('T', null, true, null);
            $batch = $acc->getOrCreateSyncBatch($key);
            $msgs = [];
            for ($i = 0; $i < 4; $i++) {
                $msgs[] = new Message('T', str_repeat('x', 5)); // 每条 5 字节
                $batch->add($msgs[$i]);
            }
            // 3 条 15 字节 < 16，不 ready；第 4 条 20 > 16，ready
            $this->checkSame(20, $batch->messagesSize, '累积后 messagesSize 记账');
            $this->check($batch->readyToSend(), 'messagesSize > holdSize 触发 readyToSend');

            $this->checkSame(0, count($sender->sent), 'tick 前未发送');
            $acc->tick();
            $this->checkSame(1, count($sender->sent), 'tick 触发 flush');
            $this->check($batch->closed, 'flush 后批次置 closed');
            $this->checkSame(4, $batch->count, '批次含 4 条消息');
            $this->checkSame(100, $batch->sendResultAt(0)->queueOffset, '拆条 queueOffset 起点');
            $this->checkSame(103, $batch->sendResultAt(3)->queueOffset, '拆条 queueOffset 递增');

            // 未达阈值时不 ready 也不 flush
            $sender2 = $this->fakeSender('z', 'zo', 0);
            $acc2 = new ProduceAccumulator('threshold-2', $sender2, $this->clock());
            $acc2->batchMaxBytes(1000);
            $b2 = $acc2->getOrCreateSyncBatch($key);
            $b2->add(new Message('T', str_repeat('y', 5)));
            $this->check(!$b2->readyToSend(), '未达 holdSize 且未到 HoldMs → 不 ready');
            $acc2->tick();
            $this->checkSame(0, count($sender2->sent), '未就绪 tick 不发送');
            $this->checkSame(1, count($acc2->syncSendBatchesSnapshot()), '未就绪批次仍留在表内');
        }

        // ==================================================================== HoldMs 到期

        private function testTickFlushesOnHoldMsExpiry(): void
        {
            $sender = $this->fakeSender('h1', 'o1', 1);
            $this->now = 2_000_000;
            $acc = new ProduceAccumulator('holdms', $sender, $this->clock());
            $acc->batchMaxDelayMs(50);

            $key = new AggregateKey('T', null, true, null);
            $batch = $acc->getOrCreateSyncBatch($key);
            $batch->add(new Message('T', 'zz'));
            $this->checkSame(2_000_000, $batch->createTime, 'accumulation.createTime 取自注入时钟');

            $this->check(!$batch->readyToSend(), '刚入队未到期');
            $this->check(!$batch->readyToSend(2_000_049), '差 1ms 未到期');
            $this->check($batch->readyToSend(2_000_050), '到 HoldMs 边界即 ready');

            $acc->tick();
            $this->checkSame(0, count($sender->sent), 'tick(未到点) 不发送');
            $this->now = 2_000_050;
            $acc->tick();
            $this->checkSame(1, count($sender->sent), 'tick(到点) 发送');

            // 显式 tick($nowMs) 覆盖时钟
            $sender2 = $this->fakeSender('h2', 'o2', 2);
            $acc2 = new ProduceAccumulator('holdms-2', $sender2, $this->clock());
            $acc2->batchMaxDelayMs(30);
            $key2 = new AggregateKey('T', null, true, null);
            $b2 = $acc2->getOrCreateSyncBatch($key2);
            $b2->add(new Message('T', 'q'));
            $acc2->tick($b2->createTime + 30);
            $this->checkSame(1, count($sender2->sent), 'tick($nowMs) 显式到期触发');
        }

        // ==================================================================== 超大单条消息

        private function testOversizedSingleMessage(): void
        {
            // 单条 body > holdSize → 立即 readyToSend，作为单条批次发出
            $sender = $this->fakeSender('big', 'big-off', 7);
            $acc = new ProduceAccumulator('oversize', $sender, $this->clock());
            $acc->batchMaxBytes(8);
            $key = new AggregateKey('T', null, true, null);
            $batch = $acc->getOrCreateSyncBatch($key);
            $batch->add(new Message('T', str_repeat('b', 100)));
            $this->check($batch->readyToSend(), '单条超 holdSize → readyToSend');
            $acc->tick();
            $this->checkSame(1, count($sender->sent), '超大单条消息单独成批发出');
            $this->checkSame(1, count($sender->sent[0][0]->getMessages()), '该批仅含 1 条子消息');

            // 全局闸门：超大消息只被放行一次
            $gate = new ProduceAccumulator('oversize-gate', $this->fakeSender(), $this->clock());
            $gate->totalBatchMaxBytes(64);
            $big = new Message('T', str_repeat('c', 100));
            $this->check($gate->tryAddMessage($big), '超大消息首次放行（quota 尚空）');
            $this->checkSame(100, $gate->currentlyHoldSize(), '超大消息按实际字节记账');
            $this->check(!$gate->tryAddMessage(new Message('T', 'x')), '额度已被占满 → 后续拒绝（退回直发）');
        }

        // ==================================================================== 批量拆条映射

        private function testSplitSendResultsMapping(): void
        {
            // 一个 batch 一份结果 → 拆回每条消息各自的 SendResult（含 queueOffset 递增）
            $sender = $this->fakeSender('id-0,id-1,id-2,id-3,id-4', 'off-0,off-1,off-2,off-3,off-4', 100);
            $acc = new ProduceAccumulator('split', $sender, $this->clock());
            $key = new AggregateKey('T', null, true, null);
            $batch = $acc->getOrCreateSyncBatch($key);
            $msgs = [];
            for ($i = 0; $i < 5; $i++) {
                $msgs[] = new Message('T', str_repeat('m', $i + 1));
                $batch->add($msgs[$i]);
            }
            $batch->sendSync();
            $this->checkSame(1, count($sender->sent), '同步：5 条同键合成 1 批');

            $sentBatch = $sender->sent[0][0];
            $this->check($sentBatch instanceof MessageBatch, '发出的是 MessageBatch');
            $this->checkSame(null, $sender->sent[0][1], '未指定 mq → 透传 null（由 producer 选队列）');
            $this->checkSame(null, $sender->sent[0][2], '同步路径无回调');
            // 单线程顺序确定 → 批 body 与 reference 逐字节一致
            $this->checkSame(MessageBatch::generateFromList($msgs)->encode(), $sentBatch->getBody(), '批 body 与 reference 一致');
            $this->checkSame('', $sentBatch->getKeys(), '同步无 keys → 批级 KEYS=""');

            $prev = null;
            for ($i = 0; $i < 5; $i++) {
                $r = $batch->sendResultAt($i);
                $this->checkSame("id-$i", $r->msgId, "拆条 msgId[$i]");
                $this->checkSame("off-$i", $r->offsetMsgId, "拆条 offsetMsgId[$i]");
                $this->checkSame(100 + $i, $r->queueOffset, "拆条 queueOffset=100+$i");
                if ($prev !== null) {
                    $this->check($r !== $prev, "拆条 $i 是独立对象（非共享）");
                }
                $prev = $r;
            }

            // 不含逗号：所有下标共享同一个 SendResult 对象（就地共享，不复制）
            $sharedSender = $this->fakeSender('single-only', 'off-single', 9);
            $acc2 = new ProduceAccumulator('shared', $sharedSender, $this->clock());
            $b2 = $acc2->getOrCreateSyncBatch(new AggregateKey('T', null, true, null));
            for ($i = 0; $i < 3; $i++) {
                $b2->add(new Message('T', 's'));
            }
            $b2->sendSync();
            $this->check($b2->sendResultAt(0) === $b2->sendResultAt(1) && $b2->sendResultAt(1) === $b2->sendResultAt(2), '不含逗号：所有下标共享同一对象');
            $this->checkSame(9, $b2->sendResultAt(0)->queueOffset, '共享对象 queueOffset 不递增');

            // 条数对不上 → InvalidArgumentException("sendResult is illegal")
            $badSender = $this->fakeSender('a,b', 'x,y', 0);
            $acc3 = new ProduceAccumulator('illegal', $badSender, $this->clock());
            $b3 = $acc3->getOrCreateSyncBatch(new AggregateKey('T', null, true, null));
            // 真实调用链里是 producer 先 tryAddMessage 记额度、批次发完再归还；这里补上记账。
            $acc3->tryAddMessage(new Message('T', 'kkkkk')); // 记 5 字节
            for ($i = 0; $i < 5; $i++) {
                $b3->add(new Message('T', 'k')); // 5×1 = 5 字节
            }
            $this->checkThrows(fn() => $b3->sendSync(), \InvalidArgumentException::class, '拆条条数不符抛异常');
            $this->checkSame(0, $acc3->currentlyHoldSize(), '拆条异常仍归还全局额度（finally）');

            // null 结果
            $acc4 = new ProduceAccumulator('null-result', $this->fakeSender(), $this->clock());
            $b4 = $acc4->getOrCreateSyncBatch(new AggregateKey('T', null, true, null));
            $b4->add(new Message('T', 'n'));
            $this->checkThrows(fn() => $b4->splitSendResults(null), \InvalidArgumentException::class, 'sendResult 为 null 抛异常');
        }

        // ==================================================================== 异步路径

        private function testAsyncBatchAndCallbackOrder(): void
        {
            $sender = $this->fakeSender('a,b,c,d,e', 'p,q,r,s,t', 7);
            $this->now = 3_000_000;
            $acc = new ProduceAccumulator('async', $sender, $this->clock());
            $acc->batchMaxDelayMs(50);

            /** @var array<int, array{0: ?SendResult, 1: ?\Throwable}> $results */
            $results = [];
            $msgs = [];
            for ($i = 0; $i < 5; $i++) {
                $msg = new Message('T', str_repeat('a', $i + 1));
                $msgs[] = $msg;
                $this->check($acc->tryAddMessage($msg), "异步闸门放行 $i");
                $acc->sendAsync($msg, function (?SendResult $r, ?\Throwable $e) use (&$results, $i): void {
                    $results[$i] = [$r, $e];
                });
            }
            $this->checkSame(0, count($sender->sent), '异步未就绪不发送');

            $this->now += 50;
            $acc->tick();
            $this->checkSame(1, count($sender->sent), 'tick 触发异步 flush');
            $this->checkSame(5, count($results), '5 个回调各被触发一次');
            $this->checkSame(
                ['a', 'b', 'c', 'd', 'e'],
                array_map(static fn(array $x): ?string => $x[0]?->msgId, array_values($results)),
                '异步回调按入队顺序拿到各自 msgId'
            );
            $this->checkSame(
                [7, 8, 9, 10, 11],
                array_map(static fn(array $x): int => $x[0]->queueOffset, array_values($results)),
                '异步回调 queueOffset 递增'
            );
            $this->check(array_reduce($results, static fn(bool $c, array $x): bool => $c && $x[1] === null, true), '异步无错误回调');
            $this->checkSame('', $sender->sent[0][0]->getKeys(), '异步批不收集 keys → KEYS=""');
            $this->checkSame(0, $acc->currentlyHoldSize(), '异步发完归还额度');

            // 批量应答含逗号但条数对不上 → onSuccess 内部抛错转全体回调 onException
            $badSender = $this->fakeSender('a,b', 'x,y', 0);
            $acc2 = new ProduceAccumulator('async-illegal', $badSender, $this->clock());
            $errs = [];
            for ($i = 0; $i < 3; $i++) {
                $acc2->sendAsync(new Message('T', 'k'), function (?SendResult $r, ?\Throwable $e = null) use (&$errs): void {
                    $errs[] = $e;
                });
            }
            $acc2->flushAll();
            $this->checkSame(3, count($errs), '拆条非法：3 个回调都被触发');
            $this->check(array_reduce($errs, static fn(bool $c, ?\Throwable $e): bool => $c && $e instanceof \InvalidArgumentException, true), '拆条非法：回调收到 InvalidArgumentException');

            // 发送本身抛异常（发起阶段）→ 全体回调 onException，且**不归还**额度（Java 遗漏，照抄）
            $throwSender = new FakeSender('x', 'y', 0, 0, $this->sendOkStatus(), true);
            $acc3 = new ProduceAccumulator('async-throw', $throwSender, $this->clock());
            $msg3 = new Message('T', 'zzz');
            $this->check($acc3->tryAddMessage($msg3), '发起异常用例：先记额度');
            $holdBefore = $acc3->currentlyHoldSize();
            $err3 = null;
            $acc3->sendAsync($msg3, function (?SendResult $r, ?\Throwable $e = null) use (&$err3): void {
                $err3 = $e;
            });
            $acc3->flushAll();
            $this->check($err3 instanceof \Throwable, '发起阶段异常 → 回调 onException');
            $this->checkSame($holdBefore, $acc3->currentlyHoldSize(), '发起阶段异常**不**归还额度（Java 遗漏，照抄）');
        }

        // ==================================================================== 守卫防重入

        private function testGuardPreventsReentry(): void
        {
            $acc = new ProduceAccumulator('guard', $this->fakeSender(), $this->clock());
            $g = $acc->guardSync;

            $this->check($g->tryEnter('k'), 'guard：首次进入成功');
            $this->check(!$g->tryEnter('k'), 'guard：同 key 在途时拒绝进入（防重入）');
            $this->check($g->isInFlight('k'), 'guard：isInFlight 为真');
            $g->leave('k');
            $this->check($g->tryEnter('k'), 'guard：leave 后可再次进入');
            $g->leave('k');

            $this->checkSame('Client_guard_GuardForSyncSend', $acc->guardSync->name, 'sync 守卫名');
            $this->checkSame('Client_guard_GuardForAsyncSend', $acc->guardAsync->name, 'async 守卫名');

            // 发送回调里重入 flush(同 key)：在途集合挡住，不会二次触发
            $reentered = 0;
            $acc2 = null;
            $key2 = null;
            $sender2 = function (MessageBatch $batch, ?MessageQueue $mq, ?InternalSendCallback $cb) use (&$acc2, &$key2, &$reentered): ?SendResult {
                $reentered++;
                $acc2->flush($key2); // 同 key 重入
                $r = new SendResult($this->sendOkStatus(), 'single', $mq, 0, null, 'o', null);
                if ($cb !== null) {
                    $cb->onSuccess($r);
                }
                return $r;
            };
            $acc2 = new ProduceAccumulator('reentry', $sender2, $this->clock());
            $key2 = new AggregateKey('T', null, true, null);
            $acc2->getOrCreateSyncBatch($key2)->add(new Message('T', 'r'));
            $acc2->flush($key2);
            $this->checkSame(1, $reentered, '同 key 重入 flush 被守卫挡住，发送仅 1 次');
        }

        // ==================================================================== 空批次清理 / 表内留存

        private function testGuardCleanupAndTableRetention(): void
        {
            // 空批次（无人 add）：tick 直接置 closed + 摘表，不发任何东西
            $sender = $this->fakeSender();
            $acc = new ProduceAccumulator('cleanup', $sender, $this->clock());
            $key = new AggregateKey('T', null, true, null);
            $empty = $acc->getOrCreateSyncBatch($key);
            $this->checkSame(1, count($acc->syncSendBatchesSnapshot()), '空批次已在表内');
            $acc->tick();
            $this->check($empty->closed, 'tick 后空批次置 closed');
            $this->checkSame(0, count($acc->syncSendBatchesSnapshot()), 'tick 摘除空批次');
            $this->checkSame(0, count($sender->sent), '空批次不发送');

            // 发完的批次 messagesSize>0 → 留在表里，直到下一次同键 send 拿它并 add 返回 -1 才摘
            $acc2 = new ProduceAccumulator('retain', $this->fakeSender('one', 'o1', 3), $this->clock());
            $r1 = $acc2->send(new Message('T', 'a'));
            $this->checkSame('one', $r1->msgId, 'send 返回本条自己的结果');
            $this->checkSame(1, count($acc2->syncSendBatchesSnapshot()), '发完的批次留在表里');
            $this->check($acc2->syncSendBatchesSnapshot()[0]->closed, '留表的批次是 closed');
            $oldBatch = $acc2->syncSendBatchesSnapshot()[0];
            $acc2->tick();
            $this->checkSame(1, count($acc2->syncSendBatchesSnapshot()), 'tick 不摘除已发送（size>0）的批次');

            // 下一次同键 send：摘掉 closed → 新建 → 再发一批
            $acc2->send(new Message('T', 'b'));
            $snap = $acc2->syncSendBatchesSnapshot();
            $this->checkSame(1, count($snap), '二次 send 后该 key 仍只占 1 个表项');
            $this->check($snap[0] !== $oldBatch, '二次 send 换成了新的 accumulation 对象');
            $this->check($snap[0]->closed, '二次 send 的新批次也已 closed');
        }

        // ==================================================================== 同步入口 / 指定 mq / keys

        private function testSyncEntryAndKeys(): void
        {
            $sender = $this->fakeSender('one', 'o1', 5);
            $acc = new ProduceAccumulator('sync-entry', $sender, $this->clock());
            $r = $acc->send(new Message('T', 'hello'));
            $this->checkSame('one', $r->msgId, '同步 send msgId');
            $this->checkSame('o1', $r->offsetMsgId, '同步 send offsetMsgId');
            $this->checkSame(5, $r->queueOffset, '同步 send queueOffset');
            $this->checkSame(1, count($sender->sent), '同步 send 发出一批');

            // 指定 mq 原样透传
            $mq = new MessageQueue('T', 'broker-pinned', 2);
            $acc->sendWithMq(new Message('T', 'hi'), $mq);
            $this->checkSame(2, count($sender->sent), 'sendWithMq 单独成批（mq 参与 key）');
            $this->check($sender->sent[1][1] === $mq, 'sendWithMq 把 mq 透传给 sender');

            // 同步批次收集子消息 keys 的并集（空格 join）
            $kSender = $this->fakeSender('kk', 'koff', 0);
            $acc3 = new ProduceAccumulator('keys', $kSender, $this->clock());
            $key = new AggregateKey('T', null, true, null);
            $b = $acc3->getOrCreateSyncBatch($key);
            $b->add(new Message('T', 'aa', null, 'k1 k2'));
            $b->add(new Message('T', 'bbb', null, 'k2 k3'));
            $b->add(new Message('T', 'c', null, 'k4 ')); // 尾随空格 → 尾部空段被丢弃
            $acc3->flush($key);
            $keys = explode(' ', $kSender->sent[0][0]->getKeys());
            sort($keys);
            $this->checkSame(['k1', 'k2', 'k3', 'k4'], $keys, '同步批 KEYS = 子消息 keys 并集（空格 join，尾空段丢弃）');

            // tag 透传到批
            $tSender = $this->fakeSender('t', 'to', 0);
            $accT = new ProduceAccumulator('tag', $tSender, $this->clock());
            $accT->send(new Message('T', 'x', 'TagX'));
            $this->checkSame('TagX', $tSender->sent[0][0]->getTags(), 'tag 透传到 MessageBatch.TAGS');
        }

        // ==================================================================== 进程级复用 / 重启

        private function testReuseAndRestart(): void
        {
            ProduceAccumulator::clearSharedInstances();
            $a = ProduceAccumulator::getOrCreateProduceAccumulator('cid-accum-1');
            $b = ProduceAccumulator::getOrCreateProduceAccumulator('cid-accum-1');
            $c = ProduceAccumulator::getOrCreateProduceAccumulator('cid-accum-2');
            $this->check($a === $b, '同 clientId 复用同一累加器');
            $this->check($a !== $c, '不同 clientId 得到不同累加器');
            $this->checkSame('cid-accum-1', $a->instanceName, 'instanceName = clientId');
            ProduceAccumulator::clearSharedInstances();

            // start → shutdown → start 幂等可重复
            $acc = new ProduceAccumulator('restart', $this->fakeSender('r', 'ro', 0), $this->clock());
            $acc->start();
            $this->check($acc->isStarted(), 'start 后 isStarted');
            $acc->shutdown();
            $this->check(!$acc->isStarted(), 'shutdown 后未 started');
            $acc->start();
            $this->check($acc->isStarted(), '可重复 start');
            $r = $acc->send(new Message('T', 'r'));
            $this->checkSame('r', $r->msgId, '重启后仍能发送');
            $this->checkSame(max(1, intdiv(10, 2)), $acc->guardSync->sleepTimeMs(), '守卫轮询间隔 = max(1, holdMs/2)');
        }

        // ==================================================================== 分区隔离

        private function testDifferentTagsDoNotMerge(): void
        {
            $sender = $this->fakeSender('a', 'ao', 0);
            $acc = new ProduceAccumulator('partition', $sender, $this->clock());
            $acc->batchMaxDelayMs(3000);
            $kA = AggregateKey::ofMessage(new Message('T', 'aa', 'TagA'));
            $kB = AggregateKey::ofMessage(new Message('T', 'bb', 'TagB'));
            $bA = $acc->getOrCreateSyncBatch($kA);
            $bB = $acc->getOrCreateSyncBatch($kB);
            $this->check($bA !== $bB, '不同 tag → 不同 accumulation');
            $bA->add(new Message('T', 'aa', 'TagA'));
            $bB->add(new Message('T', 'bb', 'TagB'));
            $this->checkSame(2, count($acc->syncSendBatchesSnapshot()), '两张表项并存');
            $acc->flush($kA);
            $acc->flush($kB);
            $this->checkSame(2, count($sender->sent), '不同 tag 各发一批');
            $tags = array_map(static fn(array $x): ?string => $x[0]->getTags(), $sender->sent);
            sort($tags);
            $this->checkSame(['TagA', 'TagB'], $tags, '两批各自的 TAGS 正确');
        }

        public function run(): int
        {
            $this->testAggregateKeyEqualityAndHash();
            $this->testParamsAndByteGate();
            $this->testAccumulateToHoldSizeThresholdTriggersFlush();
            $this->testTickFlushesOnHoldMsExpiry();
            $this->testOversizedSingleMessage();
            $this->testSplitSendResultsMapping();
            $this->testAsyncBatchAndCallbackOrder();
            $this->testGuardPreventsReentry();
            $this->testGuardCleanupAndTableRetention();
            $this->testSyncEntryAndKeys();
            $this->testReuseAndRestart();
            $this->testDifferentTagsDoNotMerge();
            return $this->summary();
        }
    }

    /** 注入用的假发送函数：只记账 + 回一份可控的批量 SendResult。 */
    final class FakeSender
    {
        /** @var list<array{0: MessageBatch, 1: ?MessageQueue, 2: ?InternalSendCallback}> */
        public array $sent = [];

        public function __construct(
            private string $msgIds = 'single-id',
            private string $offsetIds = 'off-single',
            private int $queueOffset = 0,
            private int $failAt = -1,
            private mixed $status = null,
            private bool $throwOnSend = false,
        ) {
        }

        public function __invoke(MessageBatch $batch, ?MessageQueue $mq, ?InternalSendCallback $callback): ?SendResult
        {
            $this->sent[] = [$batch, $mq, $callback];
            $index = count($this->sent) - 1;

            if ($this->throwOnSend) {
                // 模拟 sendDirect 在发起阶段就抛（还没回调）——Java 外层 catch 只通知回调、不归还额度
                throw new \RuntimeException('fake sendDirect threw');
            }
            if ($this->failAt >= 0 && $index === $this->failAt) {
                if ($callback !== null) {
                    $callback->onException(new \RuntimeException('fake send failure'));
                }
                return null;
            }

            $result = new SendResult($this->status, $this->msgIds, $mq, $this->queueOffset, null, $this->offsetIds, null);
            if ($callback !== null) {
                $callback->onSuccess($result);
            }
            return $result;
        }
    }

    exit((new RunClientAccumulator())->run());
}
