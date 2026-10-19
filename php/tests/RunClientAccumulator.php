<?php

declare(strict_types=1);

// install the autoloader first, then check whether SendResult has already landed from the parallel
// subtask — the order must not be reversed, otherwise a missing real class is misjudged and a stub
// gets declared, causing "name is already in use" when the real file is required later.
namespace {
    require_once __DIR__ . '/../bootstrap.php';
}

// ============================================================================================
// SendResult fallback definition (only effective while the parallel subtask has not yet landed php/src/Client/SendResult.php).
//
// By convention this file does **not** create php/src/Client/SendResult.php: that belongs to the
// parallel subtask. This only patches in a same-shaped stub (constructor parameter order + camelCase
// public properties) at self-test runtime if the real class is absent, so the suite also runs
// standalone in the current workspace; once the real class lands, class_exists hits and this stub
// becomes inert automatically.
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
     * ProduceAccumulator (auto batching) pure-PHP assert-style self test (no phpunit, no real cluster).
     *
     * Run: php tests/RunClientAccumulator.php
     * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
     *
     * Single-thread adaptation: background guard thread -> explicit pump (tick/flush/flushAll);
     * "current time" and the send function are both injected.
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

        /** Injected fake send function: bookkeeping + returns a batch SendResult per config (with optional failure) */
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

        // ==================================================================== aggregate key

        private function testAggregateKeyEqualityAndHash(): void
        {
            $m = new Message('TopicA', 'x', 'TagA');
            $same = new Message('TopicA', 'y', 'TagA');

            $k1 = AggregateKey::ofMessage($m);
            $k2 = AggregateKey::ofMessage($same);
            $this->check($k1->equals($k2), 'AggregateKey: same topic/tag are equal');
            $this->checkSame($k1->keyString(), $k2->keyString(), 'AggregateKey: equal keys have the same keyString');

            $otherTag = AggregateKey::ofMessage(new Message('TopicA', 'y', 'TagB'));
            $this->check(!$k1->equals($otherTag), 'AggregateKey: different tag not equal');
            $this->check($k1->keyString() !== $otherTag->keyString(), 'AggregateKey: different tag different keyString');

            $otherTopic = AggregateKey::ofMessage(new Message('TopicB', 'y', 'TagA'));
            $this->check(!$k1->equals($otherTopic), 'AggregateKey: different topic not equal');
            $this->check($k1->keyString() !== $otherTopic->keyString(), 'AggregateKey: different topic different keyString');

            // specifying mq vs not specifying mq does not merge
            $mq = new MessageQueue('TopicA', 'broker-a', 0);
            $withMq = AggregateKey::ofMessageWithMq($m, $mq);
            $this->check(!$k1->equals($withMq), 'AggregateKey: with mq and without mq not equal');
            $this->check($k1->keyString() !== $withMq->keyString(), 'AggregateKey: with/without mq different keyString');
            $withMq2 = AggregateKey::ofMessageWithMq($same, new MessageQueue('TopicA', 'broker-a', 0));
            $this->check($withMq->equals($withMq2), 'AggregateKey: same mq equal (MessageQueue.equals)');
            $this->checkSame($withMq->keyString(), $withMq2->keyString(), 'AggregateKey: same mq same keyString');
            $this->check(!$withMq->equals(AggregateKey::ofMessageWithMq($m, new MessageQueue('TopicA', 'broker-a', 1))), 'AggregateKey: different queueId not equal');

            // empty tag ("") and absent tag (null) are not the same thing (the constructor folds "" into absent, hence the explicit setTags(""))
            $nullTag = AggregateKey::ofMessage(new Message('TopicA', 'x'));
            $emptyMsg = new Message('TopicA', 'x');
            $emptyMsg->setTags('');
            $emptyTag = AggregateKey::ofMessage($emptyMsg);
            $this->check(!$nullTag->equals($emptyTag), 'AggregateKey: tag=null and tag="" not equal');
            $this->check($nullTag->keyString() !== $emptyTag->keyString(), 'AggregateKey: tag=null and tag="" different keyString');

            // waitStoreMsgOK participates in partitioning; and "WAIT never set" defaults to true
            $w = new Message('TopicA', 'x', 'TagA');
            $w->setWaitStoreMsgOk(false);
            $kW = AggregateKey::ofMessage($w);
            $this->check(!$k1->equals($kW), 'AggregateKey: different waitStoreMsgOK not equal');
            $plain = new Message('TopicA', 'x', 'TagA');
            $this->checkSame(null, $plain->getWaitStoreMsgOk(), 'normal message has no WAIT property written');
            $this->checkSame(true, AggregateKey::ofMessage($plain)->waitStoreMsgOk, 'AggregateKey: WAIT defaults to true');
            $this->checkSame(true, AggregateKey::ofMessageWithMq($plain, $mq)->waitStoreMsgOk, 'ofMessageWithMq WAIT defaults to true');
        }

        // ==================================================================== params and byte gate

        private function testParamsAndByteGate(): void
        {
            $acc = new ProduceAccumulator('params', $this->fakeSender(), $this->clock());
            $this->checkSame(10, $acc->getBatchMaxDelayMs(), 'default holdMs=10');
            $this->checkSame(32 * 1024, $acc->getBatchMaxBytes(), 'default holdSize=32KB');
            $this->checkSame(32 * 1024 * 1024, $acc->getTotalHoldSize(), 'default totalHoldSize=32MB');
            // getTotalBatchMaxBytes actually returns holdSize (keep this known behavior, do not change)
            $this->checkSame(32 * 1024, $acc->getTotalBatchMaxBytes(), 'getTotalBatchMaxBytes returns holdSize');
            $this->checkSame(0, $acc->currentlyHoldSize(), 'initial currentlyHoldSize=0');

            $acc->batchMaxDelayMs(1);
            $this->checkSame(1, $acc->getBatchMaxDelayMs(), 'batchMaxDelayMs lower bound 1 effective');
            $acc->batchMaxDelayMs(30 * 1000);
            $this->checkSame(30 * 1000, $acc->getBatchMaxDelayMs(), 'batchMaxDelayMs upper bound 30s effective');
            $this->checkThrows(fn() => $acc->batchMaxDelayMs(0), \InvalidArgumentException::class, 'batchMaxDelayMs(0) throws');
            $this->checkThrows(fn() => $acc->batchMaxDelayMs(30 * 1000 + 1), \InvalidArgumentException::class, 'batchMaxDelayMs(>30s) throws');

            $acc->batchMaxBytes(1);
            $acc->batchMaxBytes(2 * 1024 * 1024);
            $this->checkSame(2 * 1024 * 1024, $acc->getBatchMaxBytes(), 'batchMaxBytes upper bound 2MB effective');
            $this->checkThrows(fn() => $acc->batchMaxBytes(0), \InvalidArgumentException::class, 'batchMaxBytes(0) throws');
            $this->checkThrows(fn() => $acc->batchMaxBytes(2 * 1024 * 1024 + 1), \InvalidArgumentException::class, 'batchMaxBytes(>2MB) throws');

            $acc->totalBatchMaxBytes(1);
            $this->checkSame(1, $acc->getTotalHoldSize(), 'totalBatchMaxBytes effective');
            $this->checkThrows(fn() => $acc->totalBatchMaxBytes(0), \InvalidArgumentException::class, 'totalBatchMaxBytes(0) throws');

            // global byte gate
            $acc->totalBatchMaxBytes(10);
            $msg10 = new Message('T', '1234567890'); // 10 bytes
            $this->check($acc->tryAddMessage($msg10), 'gate: within quota accepted');
            $this->checkSame(10, $acc->currentlyHoldSize(), 'gate: acceptance records the bytes');
            $this->check(!$acc->tryAddMessage(new Message('T', 'x')), 'gate: quota full rejects');
            $this->checkSame(10, $acc->currentlyHoldSize(), 'gate: quota unchanged after rejection');
            $acc->releaseHold(10);
            $this->checkSame(0, $acc->currentlyHoldSize(), 'gate: quota 0 after release');
            $this->check($acc->tryAddMessage(new Message('T', 'x')), 'gate: accepted again after release');
            $acc->releaseHold(1);
            $acc->totalBatchMaxBytes(5);
            $this->check($acc->tryAddMessage(new Message('T', '')), 'gate: empty body accepted');
            $this->checkSame(0, $acc->currentlyHoldSize(), 'gate: empty body records nothing');
        }

        // ==================================================================== threshold-triggered flush

        /**
         * Accumulation up to the holdSize threshold triggers flush.
         *
         * ⚠ The accumulator has **no** MaxMessageNum / count threshold, only two criteria:
         * `messagesSize > holdSize` (batch bytes) and `now >= createTime + holdMs` (time gate).
         * So this suite always sets expectations by holdSize (the byte gate), never by count.
         */
        private function testAccumulateToHoldSizeThresholdTriggersFlush(): void
        {
            $sender = $this->fakeSender('id-0,id-1,id-2,id-3', 'off-0,off-1,off-2,off-3', 100);
            $acc = new ProduceAccumulator('threshold', $sender, $this->clock());
            $acc->batchMaxBytes(16); // ready only when strictly greater: 16 does not trigger, >16 does

            $key = new AggregateKey('T', null, true, null);
            $batch = $acc->getOrCreateSyncBatch($key);
            $msgs = [];
            for ($i = 0; $i < 4; $i++) {
                $msgs[] = new Message('T', str_repeat('x', 5)); // 5 bytes each
                $batch->add($msgs[$i]);
            }
            // 3 messages 15 bytes < 16, not ready; the 4th makes 20 > 16, ready
            $this->checkSame(20, $batch->messagesSize, 'messagesSize recorded after accumulation');
            $this->check($batch->readyToSend(), 'messagesSize > holdSize triggers readyToSend');

            $this->checkSame(0, count($sender->sent), 'nothing sent before tick');
            $acc->tick();
            $this->checkSame(1, count($sender->sent), 'tick triggers flush');
            $this->check($batch->closed, 'batch closed after flush');
            $this->checkSame(4, $batch->count, 'batch contains 4 messages');
            $this->checkSame(100, $batch->sendResultAt(0)->queueOffset, 'split queueOffset starting point');
            $this->checkSame(103, $batch->sendResultAt(3)->queueOffset, 'split queueOffset increments');

            // below threshold: neither ready nor flushed
            $sender2 = $this->fakeSender('z', 'zo', 0);
            $acc2 = new ProduceAccumulator('threshold-2', $sender2, $this->clock());
            $acc2->batchMaxBytes(1000);
            $b2 = $acc2->getOrCreateSyncBatch($key);
            $b2->add(new Message('T', str_repeat('y', 5)));
            $this->check(!$b2->readyToSend(), 'below holdSize and before HoldMs -> not ready');
            $acc2->tick();
            $this->checkSame(0, count($sender2->sent), 'not-ready tick sends nothing');
            $this->checkSame(1, count($acc2->syncSendBatchesSnapshot()), 'not-ready batch stays in the table');
        }

        // ==================================================================== HoldMs expiry

        private function testTickFlushesOnHoldMsExpiry(): void
        {
            $sender = $this->fakeSender('h1', 'o1', 1);
            $this->now = 2_000_000;
            $acc = new ProduceAccumulator('holdms', $sender, $this->clock());
            $acc->batchMaxDelayMs(50);

            $key = new AggregateKey('T', null, true, null);
            $batch = $acc->getOrCreateSyncBatch($key);
            $batch->add(new Message('T', 'zz'));
            $this->checkSame(2_000_000, $batch->createTime, 'accumulation.createTime taken from the injected clock');

            $this->check(!$batch->readyToSend(), 'not due right after enqueue');
            $this->check(!$batch->readyToSend(2_000_049), '1ms short of due');
            $this->check($batch->readyToSend(2_000_050), 'ready exactly at the HoldMs boundary');

            $acc->tick();
            $this->checkSame(0, count($sender->sent), 'tick (not yet due) sends nothing');
            $this->now = 2_000_050;
            $acc->tick();
            $this->checkSame(1, count($sender->sent), 'tick (due) sends');

            // explicit tick($nowMs) overrides the clock
            $sender2 = $this->fakeSender('h2', 'o2', 2);
            $acc2 = new ProduceAccumulator('holdms-2', $sender2, $this->clock());
            $acc2->batchMaxDelayMs(30);
            $key2 = new AggregateKey('T', null, true, null);
            $b2 = $acc2->getOrCreateSyncBatch($key2);
            $b2->add(new Message('T', 'q'));
            $acc2->tick($b2->createTime + 30);
            $this->checkSame(1, count($sender2->sent), 'tick($nowMs) explicit expiry triggers');
        }

        // ==================================================================== oversized single message

        private function testOversizedSingleMessage(): void
        {
            // single body > holdSize -> immediately readyToSend, sent as a single-message batch
            $sender = $this->fakeSender('big', 'big-off', 7);
            $acc = new ProduceAccumulator('oversize', $sender, $this->clock());
            $acc->batchMaxBytes(8);
            $key = new AggregateKey('T', null, true, null);
            $batch = $acc->getOrCreateSyncBatch($key);
            $batch->add(new Message('T', str_repeat('b', 100)));
            $this->check($batch->readyToSend(), 'single over holdSize -> readyToSend');
            $acc->tick();
            $this->checkSame(1, count($sender->sent), 'oversized single message sent as its own batch');
            $this->checkSame(1, count($sender->sent[0][0]->getMessages()), 'the batch contains exactly 1 sub-message');

            // global gate: the oversized message is admitted only once
            $gate = new ProduceAccumulator('oversize-gate', $this->fakeSender(), $this->clock());
            $gate->totalBatchMaxBytes(64);
            $big = new Message('T', str_repeat('c', 100));
            $this->check($gate->tryAddMessage($big), 'oversized message admitted the first time (quota still empty)');
            $this->checkSame(100, $gate->currentlyHoldSize(), 'oversized message recorded at actual bytes');
            $this->check(!$gate->tryAddMessage(new Message('T', 'x')), 'quota already full -> later attempts rejected (fall back to direct send)');
        }

        // ==================================================================== batch split-result mapping

        private function testSplitSendResultsMapping(): void
        {
            // one batch one result -> split back into per-message SendResults (with queueOffset increments)
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
            $this->checkSame(1, count($sender->sent), 'sync: 5 same-key messages form 1 batch');

            $sentBatch = $sender->sent[0][0];
            $this->check($sentBatch instanceof MessageBatch, 'what goes out is a MessageBatch');
            $this->checkSame(null, $sender->sent[0][1], 'no mq specified -> null passed through (producer picks the queue)');
            $this->checkSame(null, $sender->sent[0][2], 'sync path has no callback');
            // single-threaded order is deterministic -> batch body byte-identical to the reference
            $this->checkSame(MessageBatch::generateFromList($msgs)->encode(), $sentBatch->getBody(), 'batch body identical to the reference');
            $this->checkSame('', $sentBatch->getKeys(), 'sync without keys -> batch-level KEYS=""');

            $prev = null;
            for ($i = 0; $i < 5; $i++) {
                $r = $batch->sendResultAt($i);
                $this->checkSame("id-$i", $r->msgId, "split msgId[$i]");
                $this->checkSame("off-$i", $r->offsetMsgId, "split offsetMsgId[$i]");
                $this->checkSame(100 + $i, $r->queueOffset, "split queueOffset=100+$i");
                if ($prev !== null) {
                    $this->check($r !== $prev, "split $i is an independent object (not shared)");
                }
                $prev = $r;
            }

            // no commas: all indexes share the same SendResult object (shared in place, not copied)
            $sharedSender = $this->fakeSender('single-only', 'off-single', 9);
            $acc2 = new ProduceAccumulator('shared', $sharedSender, $this->clock());
            $b2 = $acc2->getOrCreateSyncBatch(new AggregateKey('T', null, true, null));
            for ($i = 0; $i < 3; $i++) {
                $b2->add(new Message('T', 's'));
            }
            $b2->sendSync();
            $this->check($b2->sendResultAt(0) === $b2->sendResultAt(1) && $b2->sendResultAt(1) === $b2->sendResultAt(2), 'no commas: all indexes share one object');
            $this->checkSame(9, $b2->sendResultAt(0)->queueOffset, 'shared object queueOffset does not increment');

            // count mismatch -> InvalidArgumentException("sendResult is illegal")
            $badSender = $this->fakeSender('a,b', 'x,y', 0);
            $acc3 = new ProduceAccumulator('illegal', $badSender, $this->clock());
            $b3 = $acc3->getOrCreateSyncBatch(new AggregateKey('T', null, true, null));
            // in the real call chain the producer records the quota via tryAddMessage first and the batch returns it after sending; record it here too.
            $acc3->tryAddMessage(new Message('T', 'kkkkk')); // records 5 bytes
            for ($i = 0; $i < 5; $i++) {
                $b3->add(new Message('T', 'k')); // 5x1 = 5 bytes
            }
            $this->checkThrows(fn() => $b3->sendSync(), \InvalidArgumentException::class, 'split count mismatch throws');
            $this->checkSame(0, $acc3->currentlyHoldSize(), 'split exception still returns the global quota (finally)');

            // null result
            $acc4 = new ProduceAccumulator('null-result', $this->fakeSender(), $this->clock());
            $b4 = $acc4->getOrCreateSyncBatch(new AggregateKey('T', null, true, null));
            $b4->add(new Message('T', 'n'));
            $this->checkThrows(fn() => $b4->splitSendResults(null), \InvalidArgumentException::class, 'null sendResult throws');
        }

        // ==================================================================== async path

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
                $this->check($acc->tryAddMessage($msg), "async gate admits $i");
                $acc->sendAsync($msg, function (?SendResult $r, ?\Throwable $e) use (&$results, $i): void {
                    $results[$i] = [$r, $e];
                });
            }
            $this->checkSame(0, count($sender->sent), 'async not ready sends nothing');

            $this->now += 50;
            $acc->tick();
            $this->checkSame(1, count($sender->sent), 'tick triggers async flush');
            $this->checkSame(5, count($results), '5 callbacks each fired once');
            $this->checkSame(
                ['a', 'b', 'c', 'd', 'e'],
                array_map(static fn(array $x): ?string => $x[0]?->msgId, array_values($results)),
                'async callbacks receive their own msgId in enqueue order'
            );
            $this->checkSame(
                [7, 8, 9, 10, 11],
                array_map(static fn(array $x): int => $x[0]->queueOffset, array_values($results)),
                'async callbacks queueOffset increments'
            );
            $this->check(array_reduce($results, static fn(bool $c, array $x): bool => $c && $x[1] === null, true), 'async has no error callbacks');
            $this->checkSame('', $sender->sent[0][0]->getKeys(), 'async batch does not collect keys -> KEYS=""');
            $this->checkSame(0, $acc->currentlyHoldSize(), 'async returns the quota after sending');

            // batch reply contains commas but the count mismatches -> onSuccess throws internally, converted to onException for every callback
            $badSender = $this->fakeSender('a,b', 'x,y', 0);
            $acc2 = new ProduceAccumulator('async-illegal', $badSender, $this->clock());
            $errs = [];
            for ($i = 0; $i < 3; $i++) {
                $acc2->sendAsync(new Message('T', 'k'), function (?SendResult $r, ?\Throwable $e = null) use (&$errs): void {
                    $errs[] = $e;
                });
            }
            $acc2->flushAll();
            $this->checkSame(3, count($errs), 'illegal split: all 3 callbacks fired');
            $this->check(array_reduce($errs, static fn(bool $c, ?\Throwable $e): bool => $c && $e instanceof \InvalidArgumentException, true), 'illegal split: callbacks receive InvalidArgumentException');

            // the send itself throws (initiation phase) -> all callbacks onException, and the quota is **not** returned (keep this known behavior)
            $throwSender = new FakeSender('x', 'y', 0, 0, $this->sendOkStatus(), true);
            $acc3 = new ProduceAccumulator('async-throw', $throwSender, $this->clock());
            $msg3 = new Message('T', 'zzz');
            $this->check($acc3->tryAddMessage($msg3), 'initiation-exception case: record quota first');
            $holdBefore = $acc3->currentlyHoldSize();
            $err3 = null;
            $acc3->sendAsync($msg3, function (?SendResult $r, ?\Throwable $e = null) use (&$err3): void {
                $err3 = $e;
            });
            $acc3->flushAll();
            $this->check($err3 instanceof \Throwable, 'initiation-phase exception -> callback onException');
            $this->checkSame($holdBefore, $acc3->currentlyHoldSize(), 'initiation-phase exception does **not** return the quota (known behavior)');
        }

        // ==================================================================== guard prevents re-entry

        private function testGuardPreventsReentry(): void
        {
            $acc = new ProduceAccumulator('guard', $this->fakeSender(), $this->clock());
            $g = $acc->guardSync;

            $this->check($g->tryEnter('k'), 'guard: first enter succeeds');
            $this->check(!$g->tryEnter('k'), 'guard: same key in flight is rejected (re-entry prevention)');
            $this->check($g->isInFlight('k'), 'guard: isInFlight true');
            $g->leave('k');
            $this->check($g->tryEnter('k'), 'guard: can enter again after leave');
            $g->leave('k');

            $this->checkSame('Client_guard_GuardForSyncSend', $acc->guardSync->name, 'sync guard name');
            $this->checkSame('Client_guard_GuardForAsyncSend', $acc->guardAsync->name, 'async guard name');

            // re-enter flush (same key) from within the send callback: the in-flight set blocks it, no second trigger
            $reentered = 0;
            $acc2 = null;
            $key2 = null;
            $sender2 = function (MessageBatch $batch, ?MessageQueue $mq, ?InternalSendCallback $cb) use (&$acc2, &$key2, &$reentered): ?SendResult {
                $reentered++;
                $acc2->flush($key2); // same-key re-entry
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
            $this->checkSame(1, $reentered, 'same-key re-entrant flush blocked by the guard, send happens exactly once');
        }

        // ==================================================================== empty-batch cleanup / table retention

        private function testGuardCleanupAndTableRetention(): void
        {
            // empty batch (nobody adds): tick marks it closed + removes it from the table, sends nothing
            $sender = $this->fakeSender();
            $acc = new ProduceAccumulator('cleanup', $sender, $this->clock());
            $key = new AggregateKey('T', null, true, null);
            $empty = $acc->getOrCreateSyncBatch($key);
            $this->checkSame(1, count($acc->syncSendBatchesSnapshot()), 'empty batch is in the table');
            $acc->tick();
            $this->check($empty->closed, 'empty batch closed after tick');
            $this->checkSame(0, count($acc->syncSendBatchesSnapshot()), 'tick removes the empty batch');
            $this->checkSame(0, count($sender->sent), 'empty batch sends nothing');

            // sent batches with messagesSize>0 -> stay in the table until the next same-key send takes it and add returns -1
            $acc2 = new ProduceAccumulator('retain', $this->fakeSender('one', 'o1', 3), $this->clock());
            $r1 = $acc2->send(new Message('T', 'a'));
            $this->checkSame('one', $r1->msgId, 'send returns this message own result');
            $this->checkSame(1, count($acc2->syncSendBatchesSnapshot()), 'sent batch stays in the table');
            $this->check($acc2->syncSendBatchesSnapshot()[0]->closed, 'the retained batch is closed');
            $oldBatch = $acc2->syncSendBatchesSnapshot()[0];
            $acc2->tick();
            $this->checkSame(1, count($acc2->syncSendBatchesSnapshot()), 'tick does not remove already-sent (size>0) batches');

            // next same-key send: closed removed -> fresh one created -> another batch sent
            $acc2->send(new Message('T', 'b'));
            $snap = $acc2->syncSendBatchesSnapshot();
            $this->checkSame(1, count($snap), 'after the second send the key still occupies 1 table entry');
            $this->check($snap[0] !== $oldBatch, 'the second send swapped in a new accumulation object');
            $this->check($snap[0]->closed, 'the new batch from the second send is also closed');
        }

        // ==================================================================== sync entry / explicit mq / keys

        private function testSyncEntryAndKeys(): void
        {
            $sender = $this->fakeSender('one', 'o1', 5);
            $acc = new ProduceAccumulator('sync-entry', $sender, $this->clock());
            $r = $acc->send(new Message('T', 'hello'));
            $this->checkSame('one', $r->msgId, 'sync send msgId');
            $this->checkSame('o1', $r->offsetMsgId, 'sync send offsetMsgId');
            $this->checkSame(5, $r->queueOffset, 'sync send queueOffset');
            $this->checkSame(1, count($sender->sent), 'sync send sends one batch');

            // explicit mq passed through as-is
            $mq = new MessageQueue('T', 'broker-pinned', 2);
            $acc->sendWithMq(new Message('T', 'hi'), $mq);
            $this->checkSame(2, count($sender->sent), 'sendWithMq forms its own batch (mq joins the key)');
            $this->check($sender->sent[1][1] === $mq, 'sendWithMq passes mq through to the sender');

            // sync batches collect the union of sub-message keys (space-joined)
            $kSender = $this->fakeSender('kk', 'koff', 0);
            $acc3 = new ProduceAccumulator('keys', $kSender, $this->clock());
            $key = new AggregateKey('T', null, true, null);
            $b = $acc3->getOrCreateSyncBatch($key);
            $b->add(new Message('T', 'aa', null, 'k1 k2'));
            $b->add(new Message('T', 'bbb', null, 'k2 k3'));
            $b->add(new Message('T', 'c', null, 'k4 ')); // trailing space -> trailing empty segment dropped
            $acc3->flush($key);
            $keys = explode(' ', $kSender->sent[0][0]->getKeys());
            sort($keys);
            $this->checkSame(['k1', 'k2', 'k3', 'k4'], $keys, 'sync batch KEYS = union of sub-message keys (space-joined, trailing empty dropped)');

            // tag passed through to the batch
            $tSender = $this->fakeSender('t', 'to', 0);
            $accT = new ProduceAccumulator('tag', $tSender, $this->clock());
            $accT->send(new Message('T', 'x', 'TagX'));
            $this->checkSame('TagX', $tSender->sent[0][0]->getTags(), 'tag passed through to MessageBatch.TAGS');
        }

        // ==================================================================== process-level reuse / restart

        private function testReuseAndRestart(): void
        {
            ProduceAccumulator::clearSharedInstances();
            $a = ProduceAccumulator::getOrCreateProduceAccumulator('cid-accum-1');
            $b = ProduceAccumulator::getOrCreateProduceAccumulator('cid-accum-1');
            $c = ProduceAccumulator::getOrCreateProduceAccumulator('cid-accum-2');
            $this->check($a === $b, 'same clientId reuses the same accumulator');
            $this->check($a !== $c, 'different clientIds get different accumulators');
            $this->checkSame('cid-accum-1', $a->instanceName, 'instanceName = clientId');
            ProduceAccumulator::clearSharedInstances();

            // start -> shutdown -> start idempotently repeatable
            $acc = new ProduceAccumulator('restart', $this->fakeSender('r', 'ro', 0), $this->clock());
            $acc->start();
            $this->check($acc->isStarted(), 'isStarted after start');
            $acc->shutdown();
            $this->check(!$acc->isStarted(), 'not started after shutdown');
            $acc->start();
            $this->check($acc->isStarted(), 'can start again');
            $r = $acc->send(new Message('T', 'r'));
            $this->checkSame('r', $r->msgId, 'still sends after restart');
            $this->checkSame(max(1, intdiv(10, 2)), $acc->guardSync->sleepTimeMs(), 'guard poll interval = max(1, holdMs/2)');
        }

        // ==================================================================== partition isolation

        private function testDifferentTagsDoNotMerge(): void
        {
            $sender = $this->fakeSender('a', 'ao', 0);
            $acc = new ProduceAccumulator('partition', $sender, $this->clock());
            $acc->batchMaxDelayMs(3000);
            $kA = AggregateKey::ofMessage(new Message('T', 'aa', 'TagA'));
            $kB = AggregateKey::ofMessage(new Message('T', 'bb', 'TagB'));
            $bA = $acc->getOrCreateSyncBatch($kA);
            $bB = $acc->getOrCreateSyncBatch($kB);
            $this->check($bA !== $bB, 'different tags -> different accumulations');
            $bA->add(new Message('T', 'aa', 'TagA'));
            $bB->add(new Message('T', 'bb', 'TagB'));
            $this->checkSame(2, count($acc->syncSendBatchesSnapshot()), 'two table entries coexist');
            $acc->flush($kA);
            $acc->flush($kB);
            $this->checkSame(2, count($sender->sent), 'different tags each send one batch');
            $tags = array_map(static fn(array $x): ?string => $x[0]->getTags(), $sender->sent);
            sort($tags);
            $this->checkSame(['TagA', 'TagB'], $tags, 'each batch carries its own TAGS');
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

    /** Fake send function for injection: bookkeeping only + returns a controllable batch SendResult. */
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
                // simulate sendDirect throwing at the initiation phase (before any callback) — the outer catch only notifies callbacks, does not return the quota
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
