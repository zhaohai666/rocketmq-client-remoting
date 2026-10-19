<?php

declare(strict_types=1);

/**
 * Client allocation strategies (Allocation.php) pure-PHP assert-style self test (no phpunit).
 *
 * Covers: AllocationHelper (mqSortKey / safeHookName / clientSideTagFilter /
 * executeFilterHooks / filterMessagesForDelivery / strategyName /
 * wireQueueKeyString / wireSplit), MessageSelector, MessageQueueListener,
 * the six AllocateMessageQueueStrategy implementations (AVG / AVG_BY_CIRCLE / CONFIG /
 * CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY), and the consistent-hash ring
 * primitives (MD5Hash / ClientNode / VirtualNode / ConsistentHashRouter).
 *
 * Run: php tests/RunClientAllocation.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\AllocationHelper;
use RocketMQ\Client\AllocateMachineRoomNearby;
use RocketMQ\Client\AllocateMessageQueueAveragely;
use RocketMQ\Client\AllocateMessageQueueAveragelyByCircle;
use RocketMQ\Client\AllocateMessageQueueByConfig;
use RocketMQ\Client\AllocateMessageQueueByMachineRoom;
use RocketMQ\Client\AllocateMessageQueueConsistentHash;
use RocketMQ\Client\AllocateMessageQueueStrategy;
use RocketMQ\Client\ClientNode;
use RocketMQ\Client\ConsistentHashRouter;
use RocketMQ\Client\FilterMessageContext;
use RocketMQ\Client\FilterMessageHook;
use RocketMQ\Client\HashFunction;
use RocketMQ\Client\Logger;
use RocketMQ\Client\MachineRoomResolver;
use RocketMQ\Client\MD5Hash;
use RocketMQ\Client\MessageQueueListener;
use RocketMQ\Client\MessageSelector;
use RocketMQ\Client\Node;
use RocketMQ\Client\VirtualNode;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\SubscriptionData;

// ==================================================================== test doubles

/** FilterMessageHook whose hookName() and filterMessage() both throw (tests safeHookName fallback + exception swallowing). */
final class BrokenHook implements FilterMessageHook
{
    public int $calls = 0;

    public function hookName(): string
    {
        throw new RuntimeException('hookName boom');
    }

    public function filterMessage(FilterMessageContext $context): void
    {
        $this->calls++;
        throw new RuntimeException('filterMessage boom');
    }
}

/** FilterMessageHook that records calls normally. */
final class RecordingFilterHook2 implements FilterMessageHook
{
    public int $calls = 0;
    public ?FilterMessageContext $last = null;

    public function hookName(): string
    {
        return 'recording-filter-hook';
    }

    public function filterMessage(FilterMessageContext $context): void
    {
        $this->calls++;
        $this->last = $context;
    }
}

/** Hook that strips every message carrying the given tag from msgList (simulates pruning). */
final class PruningFilterHook implements FilterMessageHook
{
    public function __construct(private readonly string $tagToRemove)
    {
    }

    public function hookName(): string
    {
        return 'pruning-filter-hook';
    }

    public function filterMessage(FilterMessageContext $context): void
    {
        $kept = [];
        foreach ($context->msgList as $m) {
            if ($m->getTags() !== $this->tagToRemove) {
                $kept[] = $m;
            }
        }
        $context->msgList = $kept;
    }
}

/** MessageQueueListener double: records the most recent callback. */
final class RecordingListener implements MessageQueueListener
{
    public ?array $last = null;

    public function messageQueueChanged(string $topic, array $mqAll, array $mqDivided): void
    {
        $this->last = [$topic, $mqAll, $mqDivided];
    }
}

/** Duck-typed strategy with only allocate (no getName; tests strategyName fallback). */
final class DuckStrategy
{
    public function allocate(string $g, string $cid, array $mqAll, array $cidAll): array
    {
        return $mqAll;
    }
}

/** Constant hash (every key lands on the same hash); used to test TreeMap.put override semantics. */
final class ConstantHash implements HashFunction
{
    public function __construct(private readonly int $value)
    {
    }

    public function hash(string $key): int
    {
        return $this->value;
    }
}

/** Hash = ord of the first character (enough spread, test-controllable). */
final class FirstCharHash implements HashFunction
{
    public function hash(string $key): int
    {
        return ord($key[0]);
    }
}

/** Machine room resolver double: broker names take the prefix before "room@xxx", consumers use a lookup table. */
final class StaticRoomResolver implements MachineRoomResolver
{
    /** @param array<string, string> $consumerRooms */
    public function __construct(private readonly array $consumerRooms, private readonly ?string $forceNullRoomForBroker = null)
    {
    }

    public function brokerDeployIn(MessageQueue $messageQueue): string
    {
        $pos = strpos($messageQueue->brokerName, '@');
        $room = $pos === false ? $messageQueue->brokerName : substr($messageQueue->brokerName, 0, $pos);
        if ($this->forceNullRoomForBroker !== null && $messageQueue->brokerName === $this->forceNullRoomForBroker) {
            return '';
        }
        return $room;
    }

    public function consumerDeployIn(string $clientId): string
    {
        return $this->consumerRooms[$clientId] ?? '';
    }
}

/** Records the arguments the inner strategy received (verifies MACHINE_ROOM_NEARBY grouping). */
final class RecordingInnerStrategy implements AllocateMessageQueueStrategy
{
    /** @var list<array{group: string, cid: string, mqAll: list<MessageQueue>, cidAll: list<string>}> */
    public array $calls = [];

    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        $this->calls[] = ['group' => $consumerGroup, 'cid' => $currentCID, 'mqAll' => $mqAll, 'cidAll' => $cidAll];
        return $mqAll; // pass everything through to make group assertions easy
    }

    public function getName(): string
    {
        return 'RECORDING';
    }
}

// ==================================================================== runner

final class RunClientAllocation
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

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
            $detail = sprintf("%s\n  expected: %s\n  actual:   %s", $label, var_export($expected, true), var_export($actual, true));
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$detail}\n");
        }
    }

    /** @param class-string<\Throwable> $exceptionClass */
    public function checkThrows(callable $fn, string $exceptionClass, string $label): void
    {
        $e = $this->capture($fn);
        if ($e === null) {
            $this->check(false, "{$label} (no exception thrown)");
            return;
        }
        $this->check($e instanceof $exceptionClass, sprintf('%s (got %s: %s)', $label, get_class($e), $e->getMessage()));
    }

    public function capture(callable $fn): ?\Throwable
    {
        try {
            $fn();
            return null;
        } catch (\Throwable $e) {
            return $e;
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

    // ==================================================================== helpers

    /** @return list<MessageQueue> */
    private function mqs(int $n, string $topic = 'TopicTest', string $broker = 'broker-a'): array
    {
        $out = [];
        for ($i = 0; $i < $n; $i++) {
            $out[] = new MessageQueue($topic, $broker, $i);
        }
        return $out;
    }

    /** @return list<MessageQueue> */
    private function mqsBrokers(array $spec): array
    {
        $out = [];
        foreach ($spec as [$topic, $broker, $qid]) {
            $out[] = new MessageQueue($topic, $broker, $qid);
        }
        return $out;
    }

    /** Extracts the queueId list from a result, for assertions. */
    private function qids(array $mqs): array
    {
        $out = [];
        foreach ($mqs as $m) {
            $out[] = $m->queueId;
        }
        return $out;
    }

    private function msgWithTags(?string $tags): MessageExt
    {
        $m = new MessageExt();
        if ($tags !== null) {
            $m->putProperty('TAGS', $tags);
        }
        return $m;
    }

    private function subWithTags(array $tags, bool $classFilterMode = false): SubscriptionData
    {
        $sub = new SubscriptionData('TopicTest', 'TagA || TagB');
        foreach ($tags as $t) {
            $sub->addTag($t);
        }
        $sub->classFilterMode = $classFilterMode;
        return $sub;
    }

    // ==================================================================== mqSortKey

    private function testMqSortKey(): void
    {
        $mq = new MessageQueue('T', 'b', 3);
        $this->checkSame(['T', 'b', 3], AllocationHelper::mqSortKey($mq), 'mqSortKey returns the (topic, brokerName, queueId) triple');

        // shuffle then sort by key -> topic -> brokerName -> queueId (three-level lexicographic comparison)
        $list = [
            new MessageQueue('T2', 'b1', 0),
            new MessageQueue('T1', 'b2', 5),
            new MessageQueue('T1', 'b1', 9),
            new MessageQueue('T1', 'b1', 2),
            new MessageQueue('T1', 'b0', 0),
        ];
        usort($list, fn (MessageQueue $a, MessageQueue $b) => $a->compareTo($b));
        usort($list, fn (MessageQueue $a, MessageQueue $b) => AllocationHelper::mqSortKey($a) <=> AllocationHelper::mqSortKey($b));
        $this->checkSame(
            [['T1', 'b0', 0], ['T1', 'b1', 2], ['T1', 'b1', 9], ['T1', 'b2', 5], ['T2', 'b1', 0]],
            array_map(fn (MessageQueue $m) => [$m->topic, $m->brokerName, $m->queueId], $list),
            'mqSortKey sorts topic -> brokerName -> queueId'
        );

        // total order agrees with MessageQueue.compareTo (spot-check random-ish combinations)
        $pairs = [
            [new MessageQueue('A', 'b', 0), new MessageQueue('B', 'a', 9)],
            [new MessageQueue('A', 'a', 0), new MessageQueue('A', 'b', 0)],
            [new MessageQueue('A', 'b', 1), new MessageQueue('A', 'b', 2)],
            [new MessageQueue('A', 'b', 2), new MessageQueue('A', 'b', 2)],
        ];
        foreach ($pairs as $i => [$x, $y]) {
            $this->checkSame(
                $x->compareTo($y) <=> 0,
                AllocationHelper::mqSortKey($x) <=> AllocationHelper::mqSortKey($y),
                "mqSortKey total order agrees with compareTo #{$i}"
            );
        }
    }

    // ==================================================================== safeHookName

    private function testSafeHookName(): void
    {
        $ok = new RecordingFilterHook2();
        $this->checkSame('recording-filter-hook', AllocationHelper::safeHookName($ok), 'safeHookName returns hookName normally');

        $broken = new BrokenHook();
        $this->checkSame(BrokenHook::class, AllocationHelper::safeHookName($broken), 'safeHookName falls back to class name when hookName throws');
    }

    // ==================================================================== clientSideTagFilter

    private function testClientSideTagFilter(): void
    {
        $sub = $this->subWithTags(['TagA', 'TagB']);

        $this->checkSame([], AllocationHelper::clientSideTagFilter($sub, []), 'empty message list returned as-is');
        $this->checkSame([], AllocationHelper::clientSideTagFilter(null, []), 'sub=null + empty list returned as-is');

        $msgs = [$this->msgWithTags('TagA'), $this->msgWithTags('TagC'), $this->msgWithTags(null), $this->msgWithTags('TagB')];
        $out = AllocationHelper::clientSideTagFilter($sub, $msgs);
        $this->checkSame(['TagA', 'TagB'], array_map(fn (MessageExt $m) => $m->getTags(), $out), 'keeps only messages whose tag is in tagsSet and non-null');
        $this->checkSame(4, count($msgs), 'clientSideTagFilter does not modify the input array');

        // SUB_ALL (empty tagsSet) does not filter
        $subAll = new SubscriptionData('TopicTest', '*');
        $outAll = AllocationHelper::clientSideTagFilter($subAll, $msgs);
        $this->checkSame($msgs, $outAll, 'empty tagsSet (SUB_ALL) does not filter');

        // class filter mode does not filter
        $subClass = $this->subWithTags(['TagA'], true);
        $outClass = AllocationHelper::clientSideTagFilter($subClass, $msgs);
        $this->checkSame($msgs, $outClass, 'classFilterMode=true does not filter');

        // sub=null does not filter
        $this->checkSame($msgs, AllocationHelper::clientSideTagFilter(null, $msgs), 'sub=null does not filter');

        // everything filtered out
        $onlyC = [$this->msgWithTags('TagC')];
        $this->checkSame([], AllocationHelper::clientSideTagFilter($sub, $onlyC), 'all unmatched -> empty list');
    }

    // ==================================================================== filter hooks

    private function testFilterHooks(): void
    {
        // normal execution
        $ctx = new FilterMessageContext('G', [], null);
        $hook = new RecordingFilterHook2();
        AllocationHelper::executeFilterHooks([$hook], $ctx);
        $this->checkSame(1, $hook->calls, 'executeFilterHooks executes hooks');

        // exceptions swallowed + logged as error; later hooks still run
        $broken = new BrokenHook();
        $hook2 = new RecordingFilterHook2();
        $lines = [];
        Logger::setHandler(static function (string $line) use (&$lines): void {
            $lines[] = $line;
        });
        AllocationHelper::executeFilterHooks([$broken, $hook2], $ctx);
        $this->checkSame(1, $broken->calls, 'filterMessage called and exception swallowed (calls incremented before throw, process not interrupted)');
        $this->checkSame(1, $hook2->calls, 'hook exception swallowed, subsequent hooks still run');
        $this->checkSame(1, count($lines), 'hook exception logs one error line');
        $this->check(str_contains($lines[0] ?? '', 'execute hook error'), 'error log prefixed with execute hook error');
        $this->check(str_contains($lines[0] ?? '', BrokenHook::class), 'error log includes fallback hook name (class name)');
        Logger::setHandler(static function (string $line): void {}); // restore silence

        // empty hook list is a no-op
        $this->capture(fn () => AllocationHelper::executeFilterHooks([], $ctx));
        $this->check(true, 'executeFilterHooks empty list is a no-op without throwing');
    }

    // ==================================================================== filterMessagesForDelivery

    private function testFilterMessagesForDelivery(): void
    {
        $sub = $this->subWithTags(['TagA', 'TagB']);
        $mq = new MessageQueue('TopicTest', 'broker-a', 0);

        // pure tag filtering, no hooks
        $msgs = [$this->msgWithTags('TagA'), $this->msgWithTags('TagX')];
        $out = AllocationHelper::filterMessagesForDelivery('G', [], $mq, $sub, $msgs);
        $this->checkSame(['TagA'], array_map(fn (MessageExt $m) => $m->getTags(), $out), 'no hooks: tag filtering only');

        // hook receives context: group/mq/unitMode/msgList
        $hook = new RecordingFilterHook2();
        $out = AllocationHelper::filterMessagesForDelivery('G1', [$hook], $mq, $sub, $msgs, true);
        $this->checkSame(1, $hook->calls, 'hook runs when filter result is non-empty');
        $this->checkSame('G1', $hook->last->consumerGroup, 'FilterMessageContext.consumerGroup');
        $this->checkSame($mq, $hook->last->mq, 'FilterMessageContext.mq');
        $this->check(true === $hook->last->unitMode, 'FilterMessageContext.unitMode default parameter passed through');
        $this->checkSame(['TagA'], array_map(fn (MessageExt $m) => $m->getTags(), $hook->last->msgList), 'hook receives the filtered list');
        $this->checkSame(['TagA'], array_map(fn (MessageExt $m) => $m->getTags(), $out), 'return value = filtered result');

        // unitMode defaults to false
        $hook->last = null;
        AllocationHelper::filterMessagesForDelivery('G', [$hook], $mq, $sub, $msgs);
        $this->check(false === $hook->last->unitMode, 'unitMode defaults to false');

        // hook prunes messages: return value reflects hook changes (mutable msgList semantics)
        $prune = new PruningFilterHook('TagA');
        $out = AllocationHelper::filterMessagesForDelivery('G', [$prune], $mq, $sub, $msgs);
        $this->checkSame([], $out, 'return value follows hook pruning (mutable msgList)');

        // filtered to empty -> hooks not run
        $hook2 = new RecordingFilterHook2();
        $out = AllocationHelper::filterMessagesForDelivery('G', [$hook2], $mq, $sub, [$this->msgWithTags('TagX')]);
        $this->checkSame(0, $hook2->calls, 'tag filter leaves nothing -> hooks not run');
        $this->checkSame([], $out, 'empty input -> empty output');

        // empty message list -> hooks not run
        $out = AllocationHelper::filterMessagesForDelivery('G', [$hook2], $mq, $sub, []);
        $this->checkSame(0, $hook2->calls, 'empty msgs -> hooks not run');
    }

    // ==================================================================== MessageSelector

    private function testMessageSelector(): void
    {
        $s = new MessageSelector();
        $this->checkSame('TAG', $s->type, 'MessageSelector defaults to type=TAG');
        $this->checkSame('*', $s->expression, 'MessageSelector defaults to expression="*"');

        $tag = MessageSelector::byTag('TagA');
        $this->checkSame('TAG', $tag->type, 'byTag type=TAG');
        $this->checkSame('TagA', $tag->expression, 'byTag expression');

        $sql = MessageSelector::bySql('a > 1');
        $this->checkSame('SQL92', $sql->type, 'bySql type=SQL92');
        $this->checkSame('a > 1', $sql->expression, 'bySql expression');

        $this->checkSame('TAG', \RocketMQ\Common\ExpressionType::TAG, 'ExpressionType.TAG constant');
        $this->checkSame('SQL92', \RocketMQ\Common\ExpressionType::SQL92, 'ExpressionType.SQL92 constant');
    }

    // ==================================================================== listener / strategy interface

    private function testListenerAndStrategyName(): void
    {
        $listener = new RecordingListener();
        $mqAll = $this->mqs(2);
        $listener->messageQueueChanged('T', $mqAll, [$mqAll[0]]);
        $this->checkSame(['T', $mqAll, [$mqAll[0]]], $listener->last, 'MessageQueueListener callback arguments passed through');

        $avg = new AllocateMessageQueueAveragely();
        $this->checkSame('AVG', AllocationHelper::strategyName($avg), 'strategyName uses getName');
        $this->checkSame('RECORDING', AllocationHelper::strategyName(new RecordingInnerStrategy()), 'strategyName for interface implementations');

        $duck = new DuckStrategy();
        $this->checkSame(DuckStrategy::class, AllocationHelper::strategyName($duck), 'strategyName without getName -> class name (duck typing)');

        $exploder = new class () implements AllocateMessageQueueStrategy {
            public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
            {
                throw new MQClientException('boom', 1);
            }

            public function getName(): string
            {
                throw new RuntimeException('getName boom');
            }
        };
        // PHP 8.3 anonymous class name = implemented interface FQCN + "@anonymous" + file line number
        $this->check(str_contains(AllocationHelper::strategyName($exploder), '@anonymous'), 'strategyName falls back to class name (anonymous class name) when getName throws');
    }

    // ==================================================================== AVG

    private function testAveragely(): void
    {
        $s = new AllocateMessageQueueAveragely();
        $this->checkSame('AVG', $s->getName(), 'AVG.getName');

        // guards (return [] instead of throwing on invalid input)
        $this->checkSame([], $s->allocate('G', '', $this->mqs(8), ['c0', 'c1']), 'AVG empty currentCID -> []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0']), 'AVG empty mqAll -> []');
        $this->checkSame([], $s->allocate('G', 'c0', $this->mqs(8), []), 'AVG empty cidAll -> []');
        $this->checkSame([], $s->allocate('G', 'cX', $this->mqs(8), ['c0', 'c1']), 'AVG currentCID not in cidAll -> []');

        // 8 mq / 3 cid: mod=2 avg=2 -> [0-2],[3-5],[6-7]
        $mqAll = $this->mqs(8);
        $this->checkSame([0, 1, 2], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 8/3 c0 -> 0,1,2');
        $this->checkSame([3, 4, 5], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 8/3 c1 -> 3,4,5');
        $this->checkSame([6, 7], $this->qids($s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 8/3 c2 -> 6,7');

        // 6/3 evenly divisible
        $mqAll = $this->mqs(6);
        $this->checkSame([0, 1], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 6/3 c0 -> 0,1');
        $this->checkSame([2, 3], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 6/3 c1 -> 2,3');
        $this->checkSame([4, 5], $this->qids($s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 6/3 c2 -> 4,5');

        // 5/5: one each
        $mqAll = $this->mqs(5);
        foreach (range(0, 4) as $i) {
            $this->checkSame([$i], $this->qids($s->allocate('G', "c{$i}", $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), "AVG 5/5 c{$i} -> single queue");
        }

        // 2/5: avg=0 -> first 2 consumers get one each, the rest empty
        $mqAll = $this->mqs(2);
        $this->checkSame([0], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), 'AVG 2/5 c0 -> 0');
        $this->checkSame([1], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), 'AVG 2/5 c1 -> 1');
        $this->checkSame([], $s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4']), 'AVG 2/5 c2 -> []');
        $this->checkSame([], $s->allocate('G', 'c4', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4']), 'AVG 2/5 c4 -> []');

        // 1/3: avg=0, mod=1
        $mqAll = $this->mqs(1);
        $this->checkSame([0], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 1/3 c0 -> 0');
        $this->checkSame([], $s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2']), 'AVG 1/3 c1 -> []');

        // shards are disjoint and their union is complete (8/3)
        $mqAll = $this->mqs(8);
        $all = [];
        foreach (['c0', 'c1', 'c2'] as $cid) {
            foreach ($this->qids($s->allocate('G', $cid, $mqAll, ['c0', 'c1', 'c2'])) as $q) {
                $all[] = $q;
            }
        }
        sort($all);
        $this->checkSame(range(0, 7), $all, 'AVG 8/3: union of three consumers = all queues with no overlap');
    }

    // ==================================================================== AVG_BY_CIRCLE

    private function testAveragelyByCircle(): void
    {
        $s = new AllocateMessageQueueAveragelyByCircle();
        $this->checkSame('AVG_BY_CIRCLE', $s->getName(), 'AVG_BY_CIRCLE.getName');

        $this->checkSame([], $s->allocate('G', '', $this->mqs(8), ['c0']), 'CIRCLE empty currentCID -> []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0']), 'CIRCLE empty mqAll -> []');
        $this->checkSame([], $s->allocate('G', 'c0', $this->mqs(8), []), 'CIRCLE empty cidAll -> []');
        $this->checkSame([], $s->allocate('G', 'cX', $this->mqs(8), ['c0']), 'CIRCLE currentCID not in cidAll -> []');

        // 8/3 round-robin: c0 -> 0,3,6; c1 -> 1,4,7; c2 -> 2,5
        $mqAll = $this->mqs(8);
        $this->checkSame([0, 3, 6], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'CIRCLE 8/3 c0 -> 0,3,6');
        $this->checkSame([1, 4, 7], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])), 'CIRCLE 8/3 c1 -> 1,4,7');
        $this->checkSame([2, 5], $this->qids($s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2'])), 'CIRCLE 8/3 c2 -> 2,5');

        // 2/5
        $mqAll = $this->mqs(2);
        $this->checkSame([0], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), 'CIRCLE 2/5 c0 -> 0');
        $this->checkSame([1], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), 'CIRCLE 2/5 c1 -> 1');
        $this->checkSame([], $s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4']), 'CIRCLE 2/5 c2 -> []');

        // differs from AVG result (same 8/3): confirm the two algorithms actually diverge
        $avg = new AllocateMessageQueueAveragely();
        $circle = new AllocateMessageQueueAveragelyByCircle();
        $mqAll = $this->mqs(8);
        $this->check(
            $this->qids($avg->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2']))
                !== $this->qids($circle->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])),
            'AVG and CIRCLE give c1 different shards at 8/3'
        );
    }

    // ==================================================================== BY_CONFIG

    private function testByConfig(): void
    {
        $s = new AllocateMessageQueueByConfig();
        $this->checkSame('CONFIG', $s->getName(), 'CONFIG.getName');
        $this->checkSame([], $s->allocate('G', 'c0', $this->mqs(4), ['c0']), 'CONFIG default empty list');

        // no checks: empty group / empty cidAll still returns the configured value
        $cfg = [new MessageQueue('T', 'b', 7)];
        $s2 = new AllocateMessageQueueByConfig($cfg);
        $this->checkSame([7], $this->qids($s2->allocate('', 'c0', [], [])), 'CONFIG does no checks, returns config with empty references');
        $this->checkSame([7], $this->qids($s2->allocate('G', 'not-in-cids', [], [])), 'CONFIG returns config even when currentCID not in cidAll');

        // return value isolated from the internal list: external mutation does not affect later allocate
        $cfg2 = [new MessageQueue('T', 'b', 1), new MessageQueue('T', 'b', 2)];
        $s3 = new AllocateMessageQueueByConfig($cfg2);
        $r1 = $s3->allocate('G', 'c0', [], []);
        $r1[] = new MessageQueue('T', 'b', 99); // mutate the return value
        $r2 = $s3->allocate('G', 'c0', [], []);
        $this->checkSame(2, count($r2), 'CONFIG returns a copy; external mutation does not leak into the internal list');
        $this->checkSame([1, 2], $this->qids($r2), 'CONFIG returns consistent results across calls');
    }

    // ==================================================================== consistent-hash ring primitives

    private function testHashPrimitives(): void
    {
        // MD5Hash: first 4 bytes big-endian (checked byte-by-byte against known vectors)
        $h = new MD5Hash();
        // md5("foo")   = acbd18db4cc2f85cedef654fccc4a4d8 -> 0xACBD18DB
        $this->checkSame(0xACBD18DB, $h->hash('foo'), 'MD5Hash("foo")=0xACBD18DB (first 4 bytes big-endian)');
        // md5("")      = d41d8cd98f00b204e9800998ecf8427e -> 0xD41D8CD9
        $this->checkSame(0xD41D8CD9, $h->hash(''), 'MD5Hash("")=0xD41D8CD9');
        // md5("hello") = 5d41402abc4b2a76b9719d911017c592 -> 0x5D41402A
        $this->checkSame(0x5D41402A, $h->hash('hello'), 'MD5Hash("hello")=0x5D41402A');
        // md5("MessageQueue [topic=T, brokerName=b, queueId=0]") = 218a9c35... -> 0x218A9C35
        $this->checkSame(0x218A9C35, $h->hash('MessageQueue [topic=T, brokerName=b, queueId=0]'), 'MD5Hash (queue key-string shape) vector');
        $this->checkSame(2898073819, $h->hash('foo'), 'MD5Hash result is an unsigned 32-bit decimal (0xACBD18DB)');
        $this->check(true === $h->hash('foo') >= 0, 'MD5Hash is always non-negative');

        // ClientNode
        $n = new ClientNode('cid-1');
        $this->checkSame('cid-1', $n->getKey(), 'ClientNode.getKey=clientId');
        $this->check(true === $n instanceof Node, 'ClientNode implements Node');

        // VirtualNode
        $v = new VirtualNode($n, 3);
        $this->checkSame('cid-1-3', $v->getKey(), 'VirtualNode key = physical key + "-" + index');
        $this->check(true === $v instanceof Node, 'VirtualNode implements Node');
        $this->checkSame('cid-1', $v->getPhysicalNode()->getKey(), 'VirtualNode.getPhysicalNode');
        $this->check(true === $v->isVirtualNodeOf($n), 'isVirtualNodeOf matches by physical key');
        $this->check(false === $v->isVirtualNodeOf(new ClientNode('cid-2')), 'isVirtualNodeOf different physical node -> false');
        $v0 = new VirtualNode($n, 0);
        $this->checkSame('cid-1-0', $v0->getKey(), 'VirtualNode index 0 naming');
    }

    // ==================================================================== ConsistentHashRouter

    private function testConsistentHashRouter(): void
    {
        // empty ring -> null
        $router = new ConsistentHashRouter([], 3, new FirstCharHash());
        $this->checkSame(null, $router->routeNode('anything'), 'empty ring routeNode -> null');
        $this->checkSame(null, (new ConsistentHashRouter())->routeNode('x'), 'default construction (no nodes) routeNode -> null');

        // negative vNodeCount -> ValueError (constructor guard rejects negative values)
        $router = new ConsistentHashRouter();
        $this->checkThrows(fn () => $router->addNode(new ClientNode('c'), -1), \ValueError::class, 'addNode vNodeCount<0 -> ValueError');

        // basic routing: which physical node's arc the hash falls into
        $r = new ConsistentHashRouter([new ClientNode('a'), new ClientNode('b'), new ClientNode('c')], 5, new FirstCharHash());
        $node = $r->routeNode('a-0'); // hash=ord('a')=97, ring has a-0 with hash=97 -> goes to a (ordered ring takes first >= target, inclusive)
        $this->checkSame('a', $node->getKey(), 'routeNode endpoint equality maps to itself (first >= hit is inclusive)');
        $node = $r->routeNode('b-9'); // hash=ord('b')=98 -> goes to b
        $this->checkSame('b', $node->getKey(), 'routeNode hits node b');
        $node = $r->routeNode('z-1'); // hash=ord('z')=122 > whole ring -> wraps around to the first node
        $this->checkSame('a', $node->getKey(), 'routeNode wraps around to firstKey past the end');

        // routing determinism
        $this->checkSame($r->routeNode('b-9')->getKey(), $r->routeNode('b-9')->getKey(), 'routeNode is deterministic');

        // total virtual nodes & existingReplicas (MD5Hash has no collisions, replica count is countable)
        $r2 = new ConsistentHashRouter([], 0, new MD5Hash());
        $a = new ClientNode('a');
        $r2->addNode($a, 4);
        $this->checkSame(4, $r2->getExistingReplicas($a), 'existingReplicas=4 after addNode');
        $r2->addNode($a, 2); // new replicas continue numbering (i + existingReplicas)
        $this->checkSame(6, $r2->getExistingReplicas($a), 'second addNode continues numbering -> 6');
        $r2->addNode(new ClientNode('b'), 3);
        $this->checkSame(6, $r2->getExistingReplicas($a), 'another node joining does not affect a replica count');
        $this->checkSame(3, $r2->getExistingReplicas(new ClientNode('b')), 'b has 3 replicas');

        // removeNode
        $r2->removeNode($a);
        $this->checkSame(0, $r2->getExistingReplicas($a), 'removeNode clears all virtual nodes of the physical node');
        $this->checkSame(3, $r2->getExistingReplicas(new ClientNode('b')), 'removeNode does not touch other nodes');
        $r2->removeNode($a); // removing again is a no-op
        $this->checkSame(3, $r2->getExistingReplicas(new ClientNode('b')), 'removeNode is idempotent');

        // same-hash override (ordered map: later value overrides, ring position unchanged)
        $r3 = new ConsistentHashRouter([], 1, new ConstantHash(42));
        $r3->addNode(new ClientNode('p1'), 1);
        $r3->addNode(new ClientNode('p2'), 1);
        $node = $r3->routeNode('whatever');
        $this->checkSame('p2', $node->getKey(), 'same hash: later entry overrides (TreeMap.put semantics)');
        $this->checkSame(0, $r3->getExistingReplicas(new ClientNode('p1')), 'after same-hash override p1 replicas are replaced');
        $this->checkSame(1, $r3->getExistingReplicas(new ClientNode('p2')), 'after same-hash override p2 holds the position');

        // ring size = node count x vNodeCount (when the hash has no collisions)
        $r4 = new ConsistentHashRouter(
            [new ClientNode('c0'), new ClientNode('c1'), new ClientNode('c2')],
            10,
            new MD5Hash()
        );
        $node = $r4->routeNode('MessageQueue [topic=T, brokerName=b, queueId=0]');
        $this->check($node !== null && in_array($node->getKey(), ['c0', 'c1', 'c2'], true), 'routing result is always one of the ring physical nodes');

        // default constructor: MD5Hash; single node + 1 virtual node -> every key routes to it
        $r5 = new ConsistentHashRouter([new ClientNode('only')], 1);
        $this->checkSame('only', $r5->routeNode('k')->getKey(), 'single-node ring routes any key');
        $r5b = new ConsistentHashRouter([new ClientNode('only')], 0);
        $this->checkSame(null, $r5b->routeNode('k'), 'vNodeCount=0 -> empty ring -> null');
    }

    // ==================================================================== wireQueueKeyString / wireSplit

    private function testWireFormatCompat(): void
    {
        // queue key-string format MessageQueue [topic=…, brokerName=…, queueId=…], fixed character-by-character
        $this->checkSame(
            'MessageQueue [topic=TopicTest, brokerName=broker-a, queueId=3]',
            AllocationHelper::wireQueueKeyString(new MessageQueue('TopicTest', 'broker-a', 3)),
            'wireQueueKeyString fixed character-by-character'
        );
        $this->checkSame(
            'MessageQueue [topic=, brokerName=, queueId=0]',
            AllocationHelper::wireQueueKeyString(new MessageQueue('', '', 0)),
            'wireQueueKeyString empty fields'
        );

        // wireSplit: semantics table measured on JDK 17 (see docblock)
        $this->checkSame(['room1'], AllocationHelper::wireSplit('room1@', '@'), 'wireSplit "room1@" -> ["room1"] (trailing empty segment removed)');
        $this->checkSame(['room1', 'b'], AllocationHelper::wireSplit('room1@b@', '@'), 'wireSplit "room1@b@" -> 2 segments (trailing empty segment removed)');
        $this->checkSame([], AllocationHelper::wireSplit('@', '@'), 'wireSplit "@" -> []');
        $this->checkSame([''], AllocationHelper::wireSplit('', '@'), 'wireSplit "" returns [""] as-is (Pattern#split early return)');
        $this->checkSame(['broker-a'], AllocationHelper::wireSplit('broker-a', '@'), 'wireSplit no separator hit -> whole string as-is');
        $this->checkSame(['a', '', 'b'], AllocationHelper::wireSplit('a@@b', '@'), 'wireSplit keeps interior empty segments');
        $this->checkSame([], AllocationHelper::wireSplit('@@', '@'), 'wireSplit "@@" -> []');
        $this->checkSame(['a', 'b'], AllocationHelper::wireSplit('a@b', '@'), 'wireSplit regular two segments');
        $this->checkSame(['a', 'b', 'c'], AllocationHelper::wireSplit('a@b@c', '@'), 'wireSplit three segments');
        $this->checkSame(['', 'b'], AllocationHelper::wireSplit('@b', '@'), 'wireSplit keeps the leading empty segment');
    }

    // ==================================================================== CONSISTENT_HASH

    private function testConsistentHash(): void
    {
        $s = new AllocateMessageQueueConsistentHash();
        $this->checkSame('CONSISTENT_HASH', $s->getName(), 'CONSISTENT_HASH.getName');

        // constructor guards
        $this->checkThrows(fn () => new AllocateMessageQueueConsistentHash(-1), \ValueError::class, 'CONSISTENT_HASH virtualNodeCnt<0 -> ValueError');
        $this->check(true === (new \ReflectionClass(AllocateMessageQueueConsistentHash::class))->getMethod('__construct')->getNumberOfParameters() >= 1, 'CONSISTENT_HASH constructor parameter exists');
        try {
            new AllocateMessageQueueConsistentHash(-5);
        } catch (\ValueError $e) {
            $this->check(str_contains($e->getMessage(), 'illegal virtualNodeCnt'), 'ValueError message contains illegal virtualNodeCnt');
        }

        // guards
        $mqAll = $this->mqs(8);
        $this->checkSame([], $s->allocate('G', '', $mqAll, ['c0', 'c1']), 'CONSISTENT_HASH empty currentCID -> []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0', 'c1']), 'CONSISTENT_HASH empty mqAll -> []');
        $this->checkSame([], $s->allocate('G', 'c0', $mqAll, []), 'CONSISTENT_HASH empty cidAll -> []');
        $this->checkSame([], $s->allocate('G', 'cX', $mqAll, ['c0', 'c1']), 'CONSISTENT_HASH currentCID not in cidAll -> []');

        // basic allocation properties: deterministic, disjoint, union complete
        $mqAll = $this->mqs(8);
        $cids = ['c0', 'c1', 'c2'];
        $buckets = [];
        foreach ($cids as $cid) {
            $buckets[$cid] = $this->qids((new AllocateMessageQueueConsistentHash())->allocate('G', $cid, $mqAll, $cids));
        }
        $union = [];
        foreach ($buckets as $b) {
            foreach ($b as $q) {
                $union[] = $q;
            }
        }
        sort($union);
        $this->checkSame(range(0, 7), $union, 'CONSISTENT_HASH 8 queues 3 consumers: union complete');
        $this->checkSame(count($union), count(array_unique($union)), 'CONSISTENT_HASH no overlap (each queue has exactly one owner)');
        $this->checkSame(
            $this->qids((new AllocateMessageQueueConsistentHash())->allocate('G', 'c1', $mqAll, $cids)),
            $buckets['c1'],
            'CONSISTENT_HASH deterministic (same args, same result)'
        );

        // stability: adding a consumer only "takes away" from existing shards; existing ownership is unchanged (consistency-hash stability criterion)
        $mqAll = $this->mqs(20);
        $three = ['c0', 'c1', 'c2'];
        $four = ['c0', 'c1', 'c2', 'c3'];
        foreach (['c0', 'c1', 'c2'] as $cid) {
            $before = $this->qids((new AllocateMessageQueueConsistentHash())->allocate('G', $cid, $mqAll, $three));
            $after = $this->qids((new AllocateMessageQueueConsistentHash())->allocate('G', $cid, $mqAll, $four));
            $this->check(
                $after === array_values(array_intersect($before, $after)) && count($after) <= count($before),
                "CONSISTENT_HASH: after c3 joins, {$cid} keeps only a subset of its previous queues"
            );
        }
        $c3 = $this->qids((new AllocateMessageQueueConsistentHash())->allocate('G', 'c3', $mqAll, $four));
        $this->check(count($c3) > 0, 'CONSISTENT_HASH new consumer c3 gets queues');

        // custom hash injection really takes effect
        $constMq = $this->mqs(3);
        $allConst = new AllocateMessageQueueConsistentHash(1, new ConstantHash(7));
        $got = $this->qids($allConst->allocate('G', 'c1', $constMq, ['c0', 'c1']));
        $this->checkSame([0, 1, 2], $got, 'custom ConstantHash: single landing point on the ring, all queues go to the later entrant');

        // default virtual node count = 10 (default configuration)
        $ref = new \ReflectionClass(AllocateMessageQueueConsistentHash::class);
        $ctor = $ref->getMethod('__construct');
        $this->checkSame(10, $ctor->getParameters()[0]->getDefaultValue(), 'CONSISTENT_HASH default virtualNodeCnt=10');

        // single consumer: everything goes to itself
        $solo = new AllocateMessageQueueConsistentHash();
        $this->checkSame(range(0, 4), $this->qids($solo->allocate('G', 'only', $this->mqs(5), ['only'])), 'CONSISTENT_HASH single consumer takes everything');
    }

    // ==================================================================== MACHINE_ROOM

    private function testByMachineRoom(): void
    {
        $s = new AllocateMessageQueueByMachineRoom(['room1']);
        $this->checkSame('MACHINE_ROOM', $s->getName(), 'MACHINE_ROOM.getName');

        $mqAll = $this->mqsBrokers([
            ['T', 'room1@broker-a', 0],
            ['T', 'room1@broker-b', 1],
            ['T', 'room2@broker-c', 2],
            ['T', 'broker-x', 3],       // no @ -> not eligible
            ['T', 'room1@', 4],         // consumeridcs split yields 1 segment -> not eligible
            ['T', 'room1@b@', 5],       // consumeridcs split yields 2 segments -> eligible
            ['T', 'room3@broker-d', 6], // room not in consumeridcs -> not eligible
        ]);

        // 4 eligible entries (0,1,5 + …), 3 consumers: mod=1 rem=1
        $this->checkSame([0], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM c0 -> first eligible queue + remainder');
        $this->checkSame([1], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM c1');
        $this->checkSame([5], $this->qids($s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM c2');

        // verify the eligible set = {0,1,5} exactly: one consumer takes all
        $this->checkSame([0, 1, 5], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0'])), 'MACHINE_ROOM eligible set = room1@ with 2 segments after wireSplit');

        // rem ownership criterion: rem > currentIndex (remainder goes to the first rem consumers)
        $s2 = new AllocateMessageQueueByMachineRoom(['r']);
        $mqAll2 = $this->mqsBrokers([
            ['T', 'r@b0', 0], ['T', 'r@b1', 1], ['T', 'r@b2', 2], ['T', 'r@b3', 3], ['T', 'r@b4', 4],
        ]);
        // 5 entries / 3 consumers: mod=1 rem=2 -> c0:[p0,p3] c1:[p1,p4] c2:[p2]
        $this->checkSame([0, 3], $this->qids($s2->allocate('G', 'c0', $mqAll2, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM rem criterion c0 -> [p0,p3]');
        $this->checkSame([1, 4], $this->qids($s2->allocate('G', 'c1', $mqAll2, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM rem criterion c1 -> [p1,p4]');
        $this->checkSame([2], $this->qids($s2->allocate('G', 'c2', $mqAll2, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM rem criterion c2 -> [p2]');

        // guards
        $this->checkSame([], $s->allocate('G', '', $mqAll, ['c0']), 'MACHINE_ROOM empty currentCID -> []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0']), 'MACHINE_ROOM empty mqAll -> []');
        $this->checkSame([], $s->allocate('G', 'c0', $mqAll, []), 'MACHINE_ROOM empty cidAll -> []');
        $this->checkSame([], $s->allocate('G', 'cX', $mqAll, ['c0']), 'MACHINE_ROOM currentCID not in cidAll -> []');

        // empty consumeridcs -> nothing allocated (defaults to an empty result instead of a null pointer)
        $sEmpty = new AllocateMessageQueueByMachineRoom();
        $this->checkSame([], $sEmpty->allocate('G', 'c0', $mqAll, ['c0']), 'MACHINE_ROOM unconfigured consumeridcs -> []');
        $this->checkSame([], $sEmpty->getConsumeridcs(), 'MACHINE_ROOM default consumeridcs is empty');

        // get/set
        $this->checkSame(['room1'], $s->getConsumeridcs(), 'getConsumeridcs');
        $s->setConsumeridcs(['r9', 'r8']);
        $this->checkSame(['r9', 'r8'], $s->getConsumeridcs(), 'setConsumeridcs overwrites');

        // reallocate after switching rooms
        $s->setConsumeridcs(['room2']);
        $this->checkSame([2], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0'])), 'eligible set follows setConsumeridcs after switching rooms');
    }

    // ==================================================================== MACHINE_ROOM_NEARBY

    private function testMachineRoomNearby(): void
    {
        // constructor guards: null arguments (non-nullable type declarations -> TypeError)
        $inner = new AllocateMessageQueueAveragely();
        $resolver = new StaticRoomResolver(['c0' => 'A', 'c1' => 'B']);
        $this->checkThrows(
            fn () => new AllocateMachineRoomNearby(null, $resolver),
            \TypeError::class,
            'NEARBY strategy=null -> TypeError'
        );
        $this->checkThrows(
            fn () => new AllocateMachineRoomNearby($inner, null),
            \TypeError::class,
            'NEARBY resolver=null -> TypeError'
        );

        $s = new AllocateMachineRoomNearby($inner, $resolver);
        $this->checkSame('MACHINE_ROOM_NEARBY-AVG', $s->getName(), 'NEARBY.getName = prefix + inner strategy name');
        $this->checkSame(
            'MACHINE_ROOM_NEARBY-CONSISTENT_HASH',
            (new AllocateMachineRoomNearby(new AllocateMessageQueueConsistentHash(), $resolver))->getName(),
            'NEARBY.getName follows the inner strategy name'
        );

        // guards
        $mqAll = $this->mqsBrokers([
            ['T', 'A@b0', 0], ['T', 'A@b1', 1], ['T', 'B@b0', 2], ['T', 'B@b1', 3],
        ]);
        $this->checkSame([], $s->allocate('G', '', $mqAll, ['c0', 'c1']), 'NEARBY empty currentCID -> []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0']), 'NEARBY empty mqAll -> []');
        $this->checkSame([], $s->allocate('G', 'c0', $mqAll, []), 'NEARBY empty cidAll -> []');
        $this->checkSame([], $s->allocate('G', 'cX', $mqAll, ['c0', 'c1']), 'NEARBY currentCID not in cidAll -> []');

        // basic grouping: room-A queues are only divided among room-A consumers
        // c0(A): AVG(2 mqs, [c0]) -> mq0,mq1; c1(B): AVG(2 mqs, [c1]) -> mq2,mq3
        $this->checkSame([0, 1], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1'])), 'NEARBY c0 only gets room-A queues');
        $this->checkSame([2, 3], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1'])), 'NEARBY c1 only gets room-B queues');

        // multiple consumers in the same room: four queues A@b0..3, c0/c2 both in A
        $resolver2 = new StaticRoomResolver(['c0' => 'A', 'c1' => 'B', 'c2' => 'A']);
        $s2 = new AllocateMachineRoomNearby($inner, $resolver2);
        $mqAll2 = $this->mqsBrokers([
            ['T', 'A@b0', 0], ['T', 'A@b1', 1], ['T', 'A@b2', 2], ['T', 'A@b3', 3], ['T', 'B@b0', 4],
        ]);
        $got0 = $this->qids($s2->allocate('G', 'c0', $mqAll2, ['c0', 'c1', 'c2']));
        $got2 = $this->qids($s2->allocate('G', 'c2', $mqAll2, ['c0', 'c1', 'c2']));
        $this->checkSame([0, 1], $got0, 'NEARBY 4 queues 2 consumers in room A: c0 -> [0,1] (AVG 4/2)');
        $this->checkSame([2, 3], $got2, 'NEARBY c2 -> [2,3]');
        $got1 = $this->qids($s2->allocate('G', 'c1', $mqAll2, ['c0', 'c1', 'c2']));
        $this->checkSame([4], $got1, 'NEARBY c1 only gets room-B queues');

        // room with no active consumer -> falls back to allocating among all consumers (union complete)
        $resolver3 = new StaticRoomResolver(['c0' => 'A', 'c1' => 'B']);
        $s3 = new AllocateMachineRoomNearby($inner, $resolver3);
        $mqAll3 = $this->mqsBrokers([
            ['T', 'A@b0', 0], ['T', 'A@b1', 1],
            ['T', 'B@b0', 2],
            ['T', 'C@b0', 3], ['T', 'C@b1', 4], // room C has no consumers
        ]);
        $got0 = $this->qids($s3->allocate('G', 'c0', $mqAll3, ['c0', 'c1']));
        $got1 = $this->qids($s3->allocate('G', 'c1', $mqAll3, ['c0', 'c1']));
        // c0: group A AVG(2 mqs,[c0])=[0,1] + group C AVG(2 mqs,[c0,c1]) c0=[3] -> [0,1,3]
        // c1: group B AVG(1 mq,[c1])=[2] + group C c1=[4] -> [2,4]
        $this->checkSame([0, 1, 3], $got0, 'NEARBY queues in owner-less room C fall back to all consumers (c0)');
        $this->checkSame([2, 4], $got1, 'NEARBY queues in owner-less room C fall back to all consumers (c1)');
        $union = array_merge($got0, $got1);
        sort($union);
        $this->checkSame(range(0, 4), $union, 'NEARBY full union complete (including owner-less room)');

        // grouping verification: the mqAll/cidAll the inner strategy received really were split by room
        $rec = new RecordingInnerStrategy();
        $s4 = new AllocateMachineRoomNearby($rec, $resolver);
        $s4->allocate('G', 'c0', $mqAll, ['c0', 'c1']);
        $this->checkSame(1, count($rec->calls), 'NEARBY inner strategy called once (own-room hit)');
        $this->checkSame([0, 1], $this->qids($rec->calls[0]['mqAll']), 'NEARBY inner strategy receives the room-A queue group');
        $this->checkSame(['c0'], $rec->calls[0]['cidAll'], 'NEARBY inner strategy receives the same-room consumer group');
        $this->checkSame('G', $rec->calls[0]['group'], 'NEARBY inner strategy gets consumerGroup passed through');
        $this->checkSame('c0', $rec->calls[0]['cid'], 'NEARBY inner strategy gets currentCID passed through');

        // resolver returns an empty room -> ValueError (rethrown as-is, preserving rebalance's existing allocation)
        $badResolver = new StaticRoomResolver(['c0' => 'A'], 'X@b9');
        $s5 = new AllocateMachineRoomNearby($inner, $badResolver);
        $this->checkThrows(
            fn () => $s5->allocate('G', 'c0', [new MessageQueue('T', 'X@b9', 0)], ['c0']),
            \ValueError::class,
            'NEARBY broker-side room empty -> ValueError'
        );
        try {
            $s5->allocate('G', 'c0', [new MessageQueue('T', 'X@b9', 0)], ['c0']);
        } catch (\ValueError $e) {
            $this->check(str_contains($e->getMessage(), 'Machine room is null for mq'), 'ValueError message for the mq side');
            $this->check(str_contains($e->getMessage(), 'MessageQueue [topic=T, brokerName=X@b9, queueId=0]'), 'ValueError message includes the queue key-string shape');
        }
        $s6 = new AllocateMachineRoomNearby($inner, new StaticRoomResolver([]));
        $this->checkThrows(
            fn () => $s6->allocate('G', 'c0', [new MessageQueue('T', 'A@b', 0)], ['c0']),
            \ValueError::class,
            'NEARBY consumer-side room empty -> ValueError'
        );
        try {
            $s6->allocate('G', 'c0', [new MessageQueue('T', 'A@b', 0)], ['c0']);
        } catch (\ValueError $e) {
            $this->check(str_contains($e->getMessage(), 'Machine room is null for consumer id c0'), 'ValueError message for the consumer side');
        }

        // rooms iterated in lexicographic order (TreeMap semantics): out-of-order insertion, deterministic owner-less fallback across runs
        $got0b = $this->qids($s3->allocate('G', 'c0', $mqAll3, ['c0', 'c1']));
        $this->checkSame($got0, $got0b, 'NEARBY deterministic result (rooms iterated lexicographically)');
    }

    // ==================================================================== run

    public function run(): int
    {
        Logger::setHandler(static function (string $line): void {}); // silence [BUG]/error logs
        $this->testMqSortKey();
        $this->testSafeHookName();
        $this->testClientSideTagFilter();
        $this->testFilterHooks();
        $this->testFilterMessagesForDelivery();
        $this->testMessageSelector();
        $this->testListenerAndStrategyName();
        $this->testAveragely();
        $this->testAveragelyByCircle();
        $this->testByConfig();
        $this->testHashPrimitives();
        $this->testConsistentHashRouter();
        $this->testWireFormatCompat();
        $this->testConsistentHash();
        $this->testByMachineRoom();
        $this->testMachineRoomNearby();
        return $this->summary();
    }
}

exit((new RunClientAllocation())->run());
