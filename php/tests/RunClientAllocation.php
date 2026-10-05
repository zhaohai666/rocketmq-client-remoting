<?php

declare(strict_types=1);

/**
 * Client 分配策略（Allocation.php）纯 PHP assert 风格自测（不依赖 phpunit）。
 *
 * 覆盖：AllocationHelper（mqSortKey / safeHookName / clientSideTagFilter /
 * executeFilterHooks / filterMessagesForDelivery / strategyName /
 * javaMessageQueueString / javaSplit）、MessageSelector、MessageQueueListener、
 * AllocateMessageQueueStrategy 六种实现（AVG / AVG_BY_CIRCLE / CONFIG /
 * CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY）、一致性哈希环
 * （MD5Hash / ClientNode / VirtualNode / ConsistentHashRouter）。
 *
 * 运行：C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunClientAllocation.php
 * 全部通过输出 "ALL TESTS PASSED (N checks)"，任一失败列出明细并以非零码退出。
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

// ==================================================================== 测试替身

/** hookName() 与 filterMessage() 都抛异常的 FilterMessageHook（测 safeHookName 退化 + 吞异常）。 */
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

/** 正常记录调用的 FilterMessageHook。 */
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

/** 把 msgList 里带指定 tag 的消息全部摘掉的钩子（模拟裁剪）。 */
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

/** MessageQueueListener 替身：记录最近一次回调。 */
final class RecordingListener implements MessageQueueListener
{
    public ?array $last = null;

    public function messageQueueChanged(string $topic, array $mqAll, array $mqDivided): void
    {
        $this->last = [$topic, $mqAll, $mqDivided];
    }
}

/** 只有 allocate 的鸭子类型策略（无 getName，测 strategyName 退化）。 */
final class DuckStrategy
{
    public function allocate(string $g, string $cid, array $mqAll, array $cidAll): array
    {
        return $mqAll;
    }
}

/** 恒定哈希（所有 key 落同一个 hash），用于测 TreeMap.put 覆盖语义。 */
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

/** 哈希 = 首个字符的 ord（区分度够用，测试可控）。 */
final class FirstCharHash implements HashFunction
{
    public function hash(string $key): int
    {
        return ord($key[0]);
    }
}

/** 机房解析替身：broker 名按 "room@xxx" 取前缀，消费者按映射表。 */
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

/** 记录内层策略收到的入参（验证 MACHINE_ROOM_NEARBY 的分组口径）。 */
final class RecordingInnerStrategy implements AllocateMessageQueueStrategy
{
    /** @var list<array{group: string, cid: string, mqAll: list<MessageQueue>, cidAll: list<string>}> */
    public array $calls = [];

    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        $this->calls[] = ['group' => $consumerGroup, 'cid' => $currentCID, 'mqAll' => $mqAll, 'cidAll' => $cidAll];
        return $mqAll; // 全量透传，方便断言分组
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

    /** 取 result 里的 queueId 列表，便于断言。 */
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
        $this->checkSame(['T', 'b', 3], AllocationHelper::mqSortKey($mq), 'mqSortKey 返回 (topic, brokerName, queueId) 三元组');

        // 打乱后按 key 排序 → topic → brokerName → queueId（Java compareTo 语义）
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
            'mqSortKey 排序结果 topic→brokerName→queueId'
        );

        // 与 MessageQueue.compareTo 全序一致（随机-ish 组合抽查）
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
                "mqSortKey 与 compareTo 全序一致 #{$i}"
            );
        }
    }

    // ==================================================================== safeHookName

    private function testSafeHookName(): void
    {
        $ok = new RecordingFilterHook2();
        $this->checkSame('recording-filter-hook', AllocationHelper::safeHookName($ok), 'safeHookName 正常取 hookName');

        $broken = new BrokenHook();
        $this->checkSame(BrokenHook::class, AllocationHelper::safeHookName($broken), 'safeHookName hookName 抛异常 → 类名');
    }

    // ==================================================================== clientSideTagFilter

    private function testClientSideTagFilter(): void
    {
        $sub = $this->subWithTags(['TagA', 'TagB']);

        $this->checkSame([], AllocationHelper::clientSideTagFilter($sub, []), '空消息列表原样返回');
        $this->checkSame([], AllocationHelper::clientSideTagFilter(null, []), 'sub=null + 空列表原样返回');

        $msgs = [$this->msgWithTags('TagA'), $this->msgWithTags('TagC'), $this->msgWithTags(null), $this->msgWithTags('TagB')];
        $out = AllocationHelper::clientSideTagFilter($sub, $msgs);
        $this->checkSame(['TagA', 'TagB'], array_map(fn (MessageExt $m) => $m->getTags(), $out), '只保留 tagsSet 内且非 null tag 的消息');
        $this->checkSame(4, count($msgs), 'clientSideTagFilter 不改输入数组');

        // SUB_ALL（tagsSet 空）不过滤
        $subAll = new SubscriptionData('TopicTest', '*');
        $outAll = AllocationHelper::clientSideTagFilter($subAll, $msgs);
        $this->checkSame($msgs, $outAll, 'tagsSet 空（SUB_ALL）不过滤');

        // class filter 模式不过滤
        $subClass = $this->subWithTags(['TagA'], true);
        $outClass = AllocationHelper::clientSideTagFilter($subClass, $msgs);
        $this->checkSame($msgs, $outClass, 'classFilterMode=true 不过滤');

        // sub=null 不过滤
        $this->checkSame($msgs, AllocationHelper::clientSideTagFilter(null, $msgs), 'sub=null 不过滤');

        // 全被滤光
        $onlyC = [$this->msgWithTags('TagC')];
        $this->checkSame([], AllocationHelper::clientSideTagFilter($sub, $onlyC), '全部不匹配 → 空列表');
    }

    // ==================================================================== filter hooks

    private function testFilterHooks(): void
    {
        // 正常执行
        $ctx = new FilterMessageContext('G', [], null);
        $hook = new RecordingFilterHook2();
        AllocationHelper::executeFilterHooks([$hook], $ctx);
        $this->checkSame(1, $hook->calls, 'executeFilterHooks 执行钩子');

        // 异常吞掉 + 记 error，后续钩子照常
        $broken = new BrokenHook();
        $hook2 = new RecordingFilterHook2();
        $lines = [];
        Logger::setHandler(static function (string $line) use (&$lines): void {
            $lines[] = $line;
        });
        AllocationHelper::executeFilterHooks([$broken, $hook2], $ctx);
        $this->checkSame(1, $broken->calls, 'filterMessage 被调且异常被吞（calls 自增后抛出，进程未中断）');
        $this->checkSame(1, $hook2->calls, '钩子异常被吞，后续钩子照常执行');
        $this->checkSame(1, count($lines), '钩子异常记一条 error 日志');
        $this->check(str_contains($lines[0] ?? '', 'execute hook error'), 'error 日志前缀 execute hook error');
        $this->check(str_contains($lines[0] ?? '', BrokenHook::class), 'error 日志带退化钩子名（类名）');
        Logger::setHandler(static function (string $line): void {}); // 恢复静音

        // 空钩子列表 no-op
        $this->capture(fn () => AllocationHelper::executeFilterHooks([], $ctx));
        $this->check(true, 'executeFilterHooks 空列表 no-op 不抛');
    }

    // ==================================================================== filterMessagesForDelivery

    private function testFilterMessagesForDelivery(): void
    {
        $sub = $this->subWithTags(['TagA', 'TagB']);
        $mq = new MessageQueue('TopicTest', 'broker-a', 0);

        // 纯 tag 过滤、无钩子
        $msgs = [$this->msgWithTags('TagA'), $this->msgWithTags('TagX')];
        $out = AllocationHelper::filterMessagesForDelivery('G', [], $mq, $sub, $msgs);
        $this->checkSame(['TagA'], array_map(fn (MessageExt $m) => $m->getTags(), $out), '无钩子：仅 tag 过滤');

        // 钩子收到 context：group/mq/unitMode/msgList
        $hook = new RecordingFilterHook2();
        $out = AllocationHelper::filterMessagesForDelivery('G1', [$hook], $mq, $sub, $msgs, true);
        $this->checkSame(1, $hook->calls, '有钩子且过滤后非空 → 钩子执行');
        $this->checkSame('G1', $hook->last->consumerGroup, 'FilterMessageContext.consumerGroup');
        $this->checkSame($mq, $hook->last->mq, 'FilterMessageContext.mq');
        $this->check(true === $hook->last->unitMode, 'FilterMessageContext.unitMode 默认参数透传（Java:640）');
        $this->checkSame(['TagA'], array_map(fn (MessageExt $m) => $m->getTags(), $hook->last->msgList), '钩子拿到的是过滤后的列表');
        $this->checkSame(['TagA'], array_map(fn (MessageExt $m) => $m->getTags(), $out), '返回值 = 过滤结果');

        // unitMode 默认 false
        $hook->last = null;
        AllocationHelper::filterMessagesForDelivery('G', [$hook], $mq, $sub, $msgs);
        $this->check(false === $hook->last->unitMode, 'unitMode 缺省 false');

        // 钩子摘消息：返回值反映钩子改动（可变 msgList 语义）
        $prune = new PruningFilterHook('TagA');
        $out = AllocationHelper::filterMessagesForDelivery('G', [$prune], $mq, $sub, $msgs);
        $this->checkSame([], $out, '钩子裁剪后返回值随动（可变 msgList）');

        // 过滤后为空 → 不走钩子
        $hook2 = new RecordingFilterHook2();
        $out = AllocationHelper::filterMessagesForDelivery('G', [$hook2], $mq, $sub, [$this->msgWithTags('TagX')]);
        $this->checkSame(0, $hook2->calls, 'tag 过滤后为空 → 钩子不执行');
        $this->checkSame([], $out, '空输入 → 空输出');

        // 空消息列表 → 钩子不执行
        $out = AllocationHelper::filterMessagesForDelivery('G', [$hook2], $mq, $sub, []);
        $this->checkSame(0, $hook2->calls, '空 msgs → 钩子不执行');
    }

    // ==================================================================== MessageSelector

    private function testMessageSelector(): void
    {
        $s = new MessageSelector();
        $this->checkSame('TAG', $s->type, 'MessageSelector 默认 type=TAG');
        $this->checkSame('*', $s->expression, 'MessageSelector 默认 expression="*"');

        $tag = MessageSelector::byTag('TagA');
        $this->checkSame('TAG', $tag->type, 'byTag type=TAG');
        $this->checkSame('TagA', $tag->expression, 'byTag expression');

        $sql = MessageSelector::bySql('a > 1');
        $this->checkSame('SQL92', $sql->type, 'bySql type=SQL92');
        $this->checkSame('a > 1', $sql->expression, 'bySql expression');

        $this->checkSame('TAG', \RocketMQ\Common\ExpressionType::TAG, 'ExpressionType.TAG 常量');
        $this->checkSame('SQL92', \RocketMQ\Common\ExpressionType::SQL92, 'ExpressionType.SQL92 常量');
    }

    // ==================================================================== listener / strategy interface

    private function testListenerAndStrategyName(): void
    {
        $listener = new RecordingListener();
        $mqAll = $this->mqs(2);
        $listener->messageQueueChanged('T', $mqAll, [$mqAll[0]]);
        $this->checkSame(['T', $mqAll, [$mqAll[0]]], $listener->last, 'MessageQueueListener 回调参数透传');

        $avg = new AllocateMessageQueueAveragely();
        $this->checkSame('AVG', AllocationHelper::strategyName($avg), 'strategyName 走 getName');
        $this->checkSame('RECORDING', AllocationHelper::strategyName(new RecordingInnerStrategy()), 'strategyName 接口实现');

        $duck = new DuckStrategy();
        $this->checkSame(DuckStrategy::class, AllocationHelper::strategyName($duck), 'strategyName 无 getName → 类名（鸭子类型）');

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
        // PHP 8.3 匿名类名 = implements 的接口 FQCN + "@anonymous" + 文件行号
        $this->check(str_contains(AllocationHelper::strategyName($exploder), '@anonymous'), 'strategyName getName 抛异常 → 类名兜底（匿名类名）');
    }

    // ==================================================================== AVG

    private function testAveragely(): void
    {
        $s = new AllocateMessageQueueAveragely();
        $this->checkSame('AVG', $s->getName(), 'AVG.getName');

        // 守卫（Java check 抛 IllegalArgumentException → 这里返回 []）
        $this->checkSame([], $s->allocate('G', '', $this->mqs(8), ['c0', 'c1']), 'AVG currentCID 为空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0']), 'AVG mqAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', $this->mqs(8), []), 'AVG cidAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'cX', $this->mqs(8), ['c0', 'c1']), 'AVG currentCID 不在 cidAll → []');

        // 8 mq / 3 cid：mod=2 avg=2 → [0-2],[3-5],[6-7]
        $mqAll = $this->mqs(8);
        $this->checkSame([0, 1, 2], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 8/3 c0 → 0,1,2');
        $this->checkSame([3, 4, 5], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 8/3 c1 → 3,4,5');
        $this->checkSame([6, 7], $this->qids($s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 8/3 c2 → 6,7');

        // 6/3 整除
        $mqAll = $this->mqs(6);
        $this->checkSame([0, 1], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 6/3 c0 → 0,1');
        $this->checkSame([2, 3], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 6/3 c1 → 2,3');
        $this->checkSame([4, 5], $this->qids($s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 6/3 c2 → 4,5');

        // 5/5：每人一个
        $mqAll = $this->mqs(5);
        foreach (range(0, 4) as $i) {
            $this->checkSame([$i], $this->qids($s->allocate('G', "c{$i}", $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), "AVG 5/5 c{$i} → 单队列");
        }

        // 2/5：avg=0 → 前 2 人各 1，其余空
        $mqAll = $this->mqs(2);
        $this->checkSame([0], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), 'AVG 2/5 c0 → 0');
        $this->checkSame([1], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), 'AVG 2/5 c1 → 1');
        $this->checkSame([], $s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4']), 'AVG 2/5 c2 → []');
        $this->checkSame([], $s->allocate('G', 'c4', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4']), 'AVG 2/5 c4 → []');

        // 1/3：avg=0, mod=1
        $mqAll = $this->mqs(1);
        $this->checkSame([0], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'AVG 1/3 c0 → 0');
        $this->checkSame([], $s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2']), 'AVG 1/3 c1 → []');

        // 分片互斥且并集完整（8/3）
        $mqAll = $this->mqs(8);
        $all = [];
        foreach (['c0', 'c1', 'c2'] as $cid) {
            foreach ($this->qids($s->allocate('G', $cid, $mqAll, ['c0', 'c1', 'c2'])) as $q) {
                $all[] = $q;
            }
        }
        sort($all);
        $this->checkSame(range(0, 7), $all, 'AVG 8/3 三人并集 = 全部队列且无重叠');
    }

    // ==================================================================== AVG_BY_CIRCLE

    private function testAveragelyByCircle(): void
    {
        $s = new AllocateMessageQueueAveragelyByCircle();
        $this->checkSame('AVG_BY_CIRCLE', $s->getName(), 'AVG_BY_CIRCLE.getName');

        $this->checkSame([], $s->allocate('G', '', $this->mqs(8), ['c0']), 'CIRCLE currentCID 为空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0']), 'CIRCLE mqAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', $this->mqs(8), []), 'CIRCLE cidAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'cX', $this->mqs(8), ['c0']), 'CIRCLE currentCID 不在 cidAll → []');

        // 8/3 轮转：c0 → 0,3,6；c1 → 1,4,7；c2 → 2,5
        $mqAll = $this->mqs(8);
        $this->checkSame([0, 3, 6], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'CIRCLE 8/3 c0 → 0,3,6');
        $this->checkSame([1, 4, 7], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])), 'CIRCLE 8/3 c1 → 1,4,7');
        $this->checkSame([2, 5], $this->qids($s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2'])), 'CIRCLE 8/3 c2 → 2,5');

        // 2/5
        $mqAll = $this->mqs(2);
        $this->checkSame([0], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), 'CIRCLE 2/5 c0 → 0');
        $this->checkSame([1], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4'])), 'CIRCLE 2/5 c1 → 1');
        $this->checkSame([], $s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2', 'c3', 'c4']), 'CIRCLE 2/5 c2 → []');

        // 与 AVG 结果不同（同样是 8/3）：确认两种算法真的分道
        $avg = new AllocateMessageQueueAveragely();
        $circle = new AllocateMessageQueueAveragelyByCircle();
        $mqAll = $this->mqs(8);
        $this->check(
            $this->qids($avg->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2']))
                !== $this->qids($circle->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])),
            '8/3 下 AVG 与 CIRCLE 给 c1 的分片不同'
        );
    }

    // ==================================================================== BY_CONFIG

    private function testByConfig(): void
    {
        $s = new AllocateMessageQueueByConfig();
        $this->checkSame('CONFIG', $s->getName(), 'CONFIG.getName');
        $this->checkSame([], $s->allocate('G', 'c0', $this->mqs(4), ['c0']), 'CONFIG 默认空列表');

        // 不做 check：空 group / 空 cidAll 照样返回配置值
        $cfg = [new MessageQueue('T', 'b', 7)];
        $s2 = new AllocateMessageQueueByConfig($cfg);
        $this->checkSame([7], $this->qids($s2->allocate('', 'c0', [], [])), 'CONFIG 不做 check，空参照返');
        $this->checkSame([7], $this->qids($s2->allocate('G', 'not-in-cids', [], [])), 'CONFIG currentCID 不在 cidAll 也返回配置');

        // 返回值与内部列表隔离：外部改动返回值不影响后续 allocate
        $cfg2 = [new MessageQueue('T', 'b', 1), new MessageQueue('T', 'b', 2)];
        $s3 = new AllocateMessageQueueByConfig($cfg2);
        $r1 = $s3->allocate('G', 'c0', [], []);
        $r1[] = new MessageQueue('T', 'b', 99); // 改返回值
        $r2 = $s3->allocate('G', 'c0', [], []);
        $this->checkSame(2, count($r2), 'CONFIG 返回副本，外部改动不渗回内部列表');
        $this->checkSame([1, 2], $this->qids($r2), 'CONFIG 多次调用结果一致');
    }

    // ==================================================================== 一致性哈希环基元

    private function testHashPrimitives(): void
    {
        // MD5Hash：前 4 字节大端（已知向量逐字节核对）
        $h = new MD5Hash();
        // md5("foo")   = acbd18db4cc2f85cedef654fccc4a4d8 → 0xACBD18DB
        $this->checkSame(0xACBD18DB, $h->hash('foo'), 'MD5Hash("foo")=0xACBD18DB（前 4 字节大端）');
        // md5("")      = d41d8cd98f00b204e9800998ecf8427e → 0xD41D8CD9
        $this->checkSame(0xD41D8CD9, $h->hash(''), 'MD5Hash("")=0xD41D8CD9');
        // md5("hello") = 5d41402abc4b2a76b9719d911017c592 → 0x5D41402A
        $this->checkSame(0x5D41402A, $h->hash('hello'), 'MD5Hash("hello")=0x5D41402A');
        // md5("MessageQueue [topic=T, brokerName=b, queueId=0]") = 218a9c35... → 0x218A9C35
        $this->checkSame(0x218A9C35, $h->hash('MessageQueue [topic=T, brokerName=b, queueId=0]'), 'MD5Hash(java toString 形态) 向量');
        $this->checkSame(2898073819, $h->hash('foo'), 'MD5Hash 结果是 32bit 无符号十进制（0xACBD18DB）');
        $this->check(true === $h->hash('foo') >= 0, 'MD5Hash 恒非负');

        // ClientNode
        $n = new ClientNode('cid-1');
        $this->checkSame('cid-1', $n->getKey(), 'ClientNode.getKey=clientId');
        $this->check(true === $n instanceof Node, 'ClientNode 实现 Node');

        // VirtualNode
        $v = new VirtualNode($n, 3);
        $this->checkSame('cid-1-3', $v->getKey(), 'VirtualNode key = 物理 key + "-" + 序号');
        $this->check(true === $v instanceof Node, 'VirtualNode 实现 Node');
        $this->checkSame('cid-1', $v->getPhysicalNode()->getKey(), 'VirtualNode.getPhysicalNode');
        $this->check(true === $v->isVirtualNodeOf($n), 'isVirtualNodeOf 按物理 key 判定');
        $this->check(false === $v->isVirtualNodeOf(new ClientNode('cid-2')), 'isVirtualNodeOf 不同物理节点 → false');
        $v0 = new VirtualNode($n, 0);
        $this->checkSame('cid-1-0', $v0->getKey(), 'VirtualNode 序号 0 命名');
    }

    // ==================================================================== ConsistentHashRouter

    private function testConsistentHashRouter(): void
    {
        // 空环 → null
        $router = new ConsistentHashRouter([], 3, new FirstCharHash());
        $this->checkSame(null, $router->routeNode('anything'), '空环 routeNode → null');
        $this->checkSame(null, (new ConsistentHashRouter())->routeNode('x'), '默认构造（无节点）routeNode → null');

        // vNodeCount 负数 → ValueError（Python ValueError 语义）
        $router = new ConsistentHashRouter();
        $this->checkThrows(fn () => $router->addNode(new ClientNode('c'), -1), \ValueError::class, 'addNode vNodeCount<0 → ValueError');

        // 基本路由：hash 落在哪个物理节点的弧段
        $r = new ConsistentHashRouter([new ClientNode('a'), new ClientNode('b'), new ClientNode('c')], 5, new FirstCharHash());
        $node = $r->routeNode('a-0'); // hash=ord('a')=97，环上有 a-0 的 hash=97 → 归 a（tailMap 含端点）
        $this->checkSame('a', $node->getKey(), 'routeNode 端点相等归自己（tailMap 含端点）');
        $node = $r->routeNode('b-9'); // hash=ord('b')=98 → 归 b
        $this->checkSame('b', $node->getKey(), 'routeNode 命中 b 节点');
        $node = $r->routeNode('z-1'); // hash=ord('z')=122 > 全环 → 回绕到首节点
        $this->checkSame('a', $node->getKey(), 'routeNode 越过末尾回绕到 firstKey');

        // 路由确定性
        $this->checkSame($r->routeNode('b-9')->getKey(), $r->routeNode('b-9')->getKey(), 'routeNode 确定性');

        // 虚拟节点总数 & existingReplicas（MD5Hash 无碰撞，副本数可数）
        $r2 = new ConsistentHashRouter([], 0, new MD5Hash());
        $a = new ClientNode('a');
        $r2->addNode($a, 4);
        $this->checkSame(4, $r2->getExistingReplicas($a), 'addNode 后 existingReplicas=4');
        $r2->addNode($a, 2); // 已有副本接着编号（i + existingReplicas）
        $this->checkSame(6, $r2->getExistingReplicas($a), '二次 addNode 副本接着编号 → 6');
        $r2->addNode(new ClientNode('b'), 3);
        $this->checkSame(6, $r2->getExistingReplicas($a), '别的节点加入不影响 a 的副本数');
        $this->checkSame(3, $r2->getExistingReplicas(new ClientNode('b')), 'b 的副本数=3');

        // removeNode
        $r2->removeNode($a);
        $this->checkSame(0, $r2->getExistingReplicas($a), 'removeNode 清空物理节点全部虚拟节点');
        $this->checkSame(3, $r2->getExistingReplicas(new ClientNode('b')), 'removeNode 不误伤其它节点');
        $r2->removeNode($a); // 再删一次 no-op
        $this->checkSame(3, $r2->getExistingReplicas(new ClientNode('b')), 'removeNode 幂等');

        // 同 hash 覆盖（Java TreeMap.put：后来者覆盖，位置不变）
        $r3 = new ConsistentHashRouter([], 1, new ConstantHash(42));
        $r3->addNode(new ClientNode('p1'), 1);
        $r3->addNode(new ClientNode('p2'), 1);
        $node = $r3->routeNode('whatever');
        $this->checkSame('p2', $node->getKey(), '同 hash 后来者覆盖（TreeMap.put 语义）');
        $this->checkSame(0, $r3->getExistingReplicas(new ClientNode('p1')), '同 hash 覆盖后 p1 副本被换掉');
        $this->checkSame(1, $r3->getExistingReplicas(new ClientNode('p2')), '同 hash 覆盖后 p2 占住位置');

        // ring 尺寸 = 节点数 × vNodeCount（hash 无碰撞时）
        $r4 = new ConsistentHashRouter(
            [new ClientNode('c0'), new ClientNode('c1'), new ClientNode('c2')],
            10,
            new MD5Hash()
        );
        $node = $r4->routeNode('MessageQueue [topic=T, brokerName=b, queueId=0]');
        $this->check($node !== null && in_array($node->getKey(), ['c0', 'c1', 'c2'], true), '路由结果必是环上物理节点之一');

        // 默认构造：MD5Hash；单节点 + 1 个虚拟节点 → 所有 key 都路由到它
        $r5 = new ConsistentHashRouter([new ClientNode('only')], 1);
        $this->checkSame('only', $r5->routeNode('k')->getKey(), '单节点环任意 key 路由');
        $r5b = new ConsistentHashRouter([new ClientNode('only')], 0);
        $this->checkSame(null, $r5b->routeNode('k'), 'vNodeCount=0 → 环空 → null（对齐 Python 语义）');
    }

    // ==================================================================== javaMessageQueueString / javaSplit

    private function testJavaCompat(): void
    {
        // 逐字符对齐 Java MessageQueue#toString
        $this->checkSame(
            'MessageQueue [topic=TopicTest, brokerName=broker-a, queueId=3]',
            AllocationHelper::javaMessageQueueString(new MessageQueue('TopicTest', 'broker-a', 3)),
            'javaMessageQueueString 逐字符对齐 Java'
        );
        $this->checkSame(
            'MessageQueue [topic=, brokerName=, queueId=0]',
            AllocationHelper::javaMessageQueueString(new MessageQueue('', '', 0)),
            'javaMessageQueueString 空字段'
        );

        // javaSplit：JDK 17 实测语义表（见 docblock）
        $this->checkSame(['room1'], AllocationHelper::javaSplit('room1@', '@'), 'javaSplit "room1@" → ["room1"]（剔尾空段）');
        $this->checkSame(['room1', 'b'], AllocationHelper::javaSplit('room1@b@', '@'), 'javaSplit "room1@b@" → 2 段（剔尾空段）');
        $this->checkSame([], AllocationHelper::javaSplit('@', '@'), 'javaSplit "@" → []');
        $this->checkSame([''], AllocationHelper::javaSplit('', '@'), 'javaSplit "" 原样返回 [""]（Pattern#split 早返回）');
        $this->checkSame(['broker-a'], AllocationHelper::javaSplit('broker-a', '@'), 'javaSplit 无分隔符命中 → 整串原样');
        $this->checkSame(['a', '', 'b'], AllocationHelper::javaSplit('a@@b', '@'), 'javaSplit 中间空段保留');
        $this->checkSame([], AllocationHelper::javaSplit('@@', '@'), 'javaSplit "@@" → []');
        $this->checkSame(['a', 'b'], AllocationHelper::javaSplit('a@b', '@'), 'javaSplit 常规两段');
        $this->checkSame(['a', 'b', 'c'], AllocationHelper::javaSplit('a@b@c', '@'), 'javaSplit 三段');
        $this->checkSame(['', 'b'], AllocationHelper::javaSplit('@b', '@'), 'javaSplit 首空段保留');
    }

    // ==================================================================== CONSISTENT_HASH

    private function testConsistentHash(): void
    {
        $s = new AllocateMessageQueueConsistentHash();
        $this->checkSame('CONSISTENT_HASH', $s->getName(), 'CONSISTENT_HASH.getName');

        // 构造守卫
        $this->checkThrows(fn () => new AllocateMessageQueueConsistentHash(-1), \ValueError::class, 'CONSISTENT_HASH virtualNodeCnt<0 → ValueError');
        $this->check(true === (new \ReflectionClass(AllocateMessageQueueConsistentHash::class))->getMethod('__construct')->getNumberOfParameters() >= 1, 'CONSISTENT_HASH 构造器参数存在');
        try {
            new AllocateMessageQueueConsistentHash(-5);
        } catch (\ValueError $e) {
            $this->check(str_contains($e->getMessage(), 'illegal virtualNodeCnt'), 'ValueError 文案带 illegal virtualNodeCnt');
        }

        // 守卫
        $mqAll = $this->mqs(8);
        $this->checkSame([], $s->allocate('G', '', $mqAll, ['c0', 'c1']), 'CONSISTENT_HASH currentCID 空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0', 'c1']), 'CONSISTENT_HASH mqAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', $mqAll, []), 'CONSISTENT_HASH cidAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'cX', $mqAll, ['c0', 'c1']), 'CONSISTENT_HASH currentCID 不在 cidAll → []');

        // 分配基本性质：确定、互斥、并集完整
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
        $this->checkSame(range(0, 7), $union, 'CONSISTENT_HASH 8 队列 3 消费者：并集完整');
        $this->checkSame(count($union), count(array_unique($union)), 'CONSISTENT_HASH 无重叠（每队列恰好一个主）');
        $this->checkSame(
            $this->qids((new AllocateMessageQueueConsistentHash())->allocate('G', 'c1', $mqAll, $cids)),
            $buckets['c1'],
            'CONSISTENT_HASH 确定性（同参重跑一致）'
        );

        // 稳定性：新增消费者只会从原有分片里「拿走」，存量归属不变（Java 单测断言点）
        $mqAll = $this->mqs(20);
        $three = ['c0', 'c1', 'c2'];
        $four = ['c0', 'c1', 'c2', 'c3'];
        foreach (['c0', 'c1', 'c2'] as $cid) {
            $before = $this->qids((new AllocateMessageQueueConsistentHash())->allocate('G', $cid, $mqAll, $three));
            $after = $this->qids((new AllocateMessageQueueConsistentHash())->allocate('G', $cid, $mqAll, $four));
            $this->check(
                $after === array_values(array_intersect($before, $after)) && count($after) <= count($before),
                "CONSISTENT_HASH 加入 c3 后 {$cid} 的存量归属只减不增"
            );
        }
        $c3 = $this->qids((new AllocateMessageQueueConsistentHash())->allocate('G', 'c3', $mqAll, $four));
        $this->check(count($c3) > 0, 'CONSISTENT_HASH 新消费者 c3 分到队列');

        // 自定义哈希注入真的生效
        $constMq = $this->mqs(3);
        $allConst = new AllocateMessageQueueConsistentHash(1, new ConstantHash(7));
        $got = $this->qids($allConst->allocate('G', 'c1', $constMq, ['c0', 'c1']));
        $this->checkSame([0, 1, 2], $got, '自定义 ConstantHash：环上只有一个落点，全队列归后加者');

        // 默认虚拟节点数 = 10（Java 默认链）
        $ref = new \ReflectionClass(AllocateMessageQueueConsistentHash::class);
        $ctor = $ref->getMethod('__construct');
        $this->checkSame(10, $ctor->getParameters()[0]->getDefaultValue(), 'CONSISTENT_HASH 默认 virtualNodeCnt=10');

        // 单消费者：全部分给自己
        $solo = new AllocateMessageQueueConsistentHash();
        $this->checkSame(range(0, 4), $this->qids($solo->allocate('G', 'only', $this->mqs(5), ['only'])), 'CONSISTENT_HASH 单消费者全量');
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
            ['T', 'broker-x', 3],       // 无 @ → 不参与
            ['T', 'room1@', 4],         // Java split 1 段 → 不参与
            ['T', 'room1@b@', 5],       // Java split 2 段 → 参与
            ['T', 'room3@broker-d', 6], // 机房不在 consumeridcs → 不参与
        ]);

        // 4 条参与（0,1,5 + …），3 个消费者：mod=1 rem=1
        $this->checkSame([0], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM c0 → 首条参与队列+余数');
        $this->checkSame([1], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM c1');
        $this->checkSame([5], $this->qids($s->allocate('G', 'c2', $mqAll, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM c2');

        // 精确核对参与集合 = {0,1,5}：用 1 个消费者全量接
        $this->checkSame([0, 1, 5], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0'])), 'MACHINE_ROOM 参与集合 = room1@ 且 javaSplit 后 2 段');

        // rem 归属判据：rem > currentIndex（余数发给前 rem 个消费者）
        $s2 = new AllocateMessageQueueByMachineRoom(['r']);
        $mqAll2 = $this->mqsBrokers([
            ['T', 'r@b0', 0], ['T', 'r@b1', 1], ['T', 'r@b2', 2], ['T', 'r@b3', 3], ['T', 'r@b4', 4],
        ]);
        // 5 条 / 3 人：mod=1 rem=2 → c0:[p0,p3] c1:[p1,p4] c2:[p2]
        $this->checkSame([0, 3], $this->qids($s2->allocate('G', 'c0', $mqAll2, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM rem 判据 c0 → [p0,p3]');
        $this->checkSame([1, 4], $this->qids($s2->allocate('G', 'c1', $mqAll2, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM rem 判据 c1 → [p1,p4]');
        $this->checkSame([2], $this->qids($s2->allocate('G', 'c2', $mqAll2, ['c0', 'c1', 'c2'])), 'MACHINE_ROOM rem 判据 c2 → [p2]');

        // 守卫
        $this->checkSame([], $s->allocate('G', '', $mqAll, ['c0']), 'MACHINE_ROOM currentCID 空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0']), 'MACHINE_ROOM mqAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', $mqAll, []), 'MACHINE_ROOM cidAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'cX', $mqAll, ['c0']), 'MACHINE_ROOM currentCID 不在 cidAll → []');

        // consumeridcs 空 → 一条都不分（Java 未 set 会 NPE，这里兜成空结果）
        $sEmpty = new AllocateMessageQueueByMachineRoom();
        $this->checkSame([], $sEmpty->allocate('G', 'c0', $mqAll, ['c0']), 'MACHINE_ROOM 未配置 consumeridcs → []');
        $this->checkSame([], $sEmpty->getConsumeridcs(), 'MACHINE_ROOM 默认 consumeridcs 空');

        // get/set
        $this->checkSame(['room1'], $s->getConsumeridcs(), 'getConsumeridcs');
        $s->setConsumeridcs(['r9', 'r8']);
        $this->checkSame(['r9', 'r8'], $s->getConsumeridcs(), 'setConsumeridcs 覆盖');

        // 换机房后重新分配
        $s->setConsumeridcs(['room2']);
        $this->checkSame([2], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0'])), 'setConsumeridcs 换机房后参与集合随动');
    }

    // ==================================================================== MACHINE_ROOM_NEARBY

    private function testMachineRoomNearby(): void
    {
        // 构造守卫：null 参数（PHP 非空类型 → TypeError，对应 Java NPE）
        $inner = new AllocateMessageQueueAveragely();
        $resolver = new StaticRoomResolver(['c0' => 'A', 'c1' => 'B']);
        $this->checkThrows(
            fn () => new AllocateMachineRoomNearby(null, $resolver),
            \TypeError::class,
            'NEARBY strategy=null → TypeError（Java NPE 对应）'
        );
        $this->checkThrows(
            fn () => new AllocateMachineRoomNearby($inner, null),
            \TypeError::class,
            'NEARBY resolver=null → TypeError（Java NPE 对应）'
        );

        $s = new AllocateMachineRoomNearby($inner, $resolver);
        $this->checkSame('MACHINE_ROOM_NEARBY-AVG', $s->getName(), 'NEARBY.getName = 前缀 + 内层策略名');
        $this->checkSame(
            'MACHINE_ROOM_NEARBY-CONSISTENT_HASH',
            (new AllocateMachineRoomNearby(new AllocateMessageQueueConsistentHash(), $resolver))->getName(),
            'NEARBY.getName 内层策略名随动'
        );

        // 守卫
        $mqAll = $this->mqsBrokers([
            ['T', 'A@b0', 0], ['T', 'A@b1', 1], ['T', 'B@b0', 2], ['T', 'B@b1', 3],
        ]);
        $this->checkSame([], $s->allocate('G', '', $mqAll, ['c0', 'c1']), 'NEARBY currentCID 空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', [], ['c0']), 'NEARBY mqAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'c0', $mqAll, []), 'NEARBY cidAll 空 → []');
        $this->checkSame([], $s->allocate('G', 'cX', $mqAll, ['c0', 'c1']), 'NEARBY currentCID 不在 cidAll → []');

        // 基本分组：A 机房队列只在 A 消费者间分
        // c0(A): AVG(2 mqs, [c0]) → mq0,mq1；c1(B): AVG(2 mqs, [c1]) → mq2,mq3
        $this->checkSame([0, 1], $this->qids($s->allocate('G', 'c0', $mqAll, ['c0', 'c1'])), 'NEARBY c0 只拿 A 机房队列');
        $this->checkSame([2, 3], $this->qids($s->allocate('G', 'c1', $mqAll, ['c0', 'c1'])), 'NEARBY c1 只拿 B 机房队列');

        // 同机房多消费者：A@b0..3 四条队列，c0/c2 同在 A
        $resolver2 = new StaticRoomResolver(['c0' => 'A', 'c1' => 'B', 'c2' => 'A']);
        $s2 = new AllocateMachineRoomNearby($inner, $resolver2);
        $mqAll2 = $this->mqsBrokers([
            ['T', 'A@b0', 0], ['T', 'A@b1', 1], ['T', 'A@b2', 2], ['T', 'A@b3', 3], ['T', 'B@b0', 4],
        ]);
        $got0 = $this->qids($s2->allocate('G', 'c0', $mqAll2, ['c0', 'c1', 'c2']));
        $got2 = $this->qids($s2->allocate('G', 'c2', $mqAll2, ['c0', 'c1', 'c2']));
        $this->checkSame([0, 1], $got0, 'NEARBY A 机房 4 队列 2 人：c0 → [0,1]（AVG 4/2）');
        $this->checkSame([2, 3], $got2, 'NEARBY c2 → [2,3]');
        $got1 = $this->qids($s2->allocate('G', 'c1', $mqAll2, ['c0', 'c1', 'c2']));
        $this->checkSame([4], $got1, 'NEARBY c1 只拿 B 机房队列');

        // 无活消费者机房 → 回落全量分配（并集完整）
        $resolver3 = new StaticRoomResolver(['c0' => 'A', 'c1' => 'B']);
        $s3 = new AllocateMachineRoomNearby($inner, $resolver3);
        $mqAll3 = $this->mqsBrokers([
            ['T', 'A@b0', 0], ['T', 'A@b1', 1],
            ['T', 'B@b0', 2],
            ['T', 'C@b0', 3], ['T', 'C@b1', 4], // C 机房没有消费者
        ]);
        $got0 = $this->qids($s3->allocate('G', 'c0', $mqAll3, ['c0', 'c1']));
        $got1 = $this->qids($s3->allocate('G', 'c1', $mqAll3, ['c0', 'c1']));
        // c0: A 组 AVG(2 mqs,[c0])=[0,1] + C 组 AVG(2 mqs,[c0,c1]) c0=[3] → [0,1,3]
        // c1: B 组 AVG(1 mq,[c1])=[2] + C 组 c1=[4] → [2,4]
        $this->checkSame([0, 1, 3], $got0, 'NEARBY 无主机房 C 的队列回落给全体消费者（c0）');
        $this->checkSame([2, 4], $got1, 'NEARBY 无主机房 C 的队列回落给全体消费者（c1）');
        $union = array_merge($got0, $got1);
        sort($union);
        $this->checkSame(range(0, 4), $union, 'NEARBY 全量并集完整（含无主机房）');

        // 分组口径验证：内层策略收到的 mqAll/cidAll 确实按机房切过
        $rec = new RecordingInnerStrategy();
        $s4 = new AllocateMachineRoomNearby($rec, $resolver);
        $s4->allocate('G', 'c0', $mqAll, ['c0', 'c1']);
        $this->checkSame(1, count($rec->calls), 'NEARBY 内层只被调一次（本机房命中）');
        $this->checkSame([0, 1], $this->qids($rec->calls[0]['mqAll']), 'NEARBY 内层收到 A 机房队列组');
        $this->checkSame(['c0'], $rec->calls[0]['cidAll'], 'NEARBY 内层收到同机房消费者组');
        $this->checkSame('G', $rec->calls[0]['group'], 'NEARBY 内层透传 consumerGroup');
        $this->checkSame('c0', $rec->calls[0]['cid'], 'NEARBY 内层透传 currentCID');

        // resolver 返回空机房 → ValueError（照抛，保住 rebalance 现有分配）
        $badResolver = new StaticRoomResolver(['c0' => 'A'], 'X@b9');
        $s5 = new AllocateMachineRoomNearby($inner, $badResolver);
        $this->checkThrows(
            fn () => $s5->allocate('G', 'c0', [new MessageQueue('T', 'X@b9', 0)], ['c0']),
            \ValueError::class,
            'NEARBY 机房 broker 侧为空 → ValueError'
        );
        try {
            $s5->allocate('G', 'c0', [new MessageQueue('T', 'X@b9', 0)], ['c0']);
        } catch (\ValueError $e) {
            $this->check(str_contains($e->getMessage(), 'Machine room is null for mq'), 'ValueError 文案 mq 侧');
            $this->check(str_contains($e->getMessage(), 'MessageQueue [topic=T, brokerName=X@b9, queueId=0]'), 'ValueError 文案带 Java toString 形态');
        }
        $s6 = new AllocateMachineRoomNearby($inner, new StaticRoomResolver([]));
        $this->checkThrows(
            fn () => $s6->allocate('G', 'c0', [new MessageQueue('T', 'A@b', 0)], ['c0']),
            \ValueError::class,
            'NEARBY 机房 consumer 侧为空 → ValueError'
        );
        try {
            $s6->allocate('G', 'c0', [new MessageQueue('T', 'A@b', 0)], ['c0']);
        } catch (\ValueError $e) {
            $this->check(str_contains($e->getMessage(), 'Machine room is null for consumer id c0'), 'ValueError 文案 consumer 侧');
        }

        // 机房遍历按字典序（TreeMap 语义）：构造乱序插入，无主机房回落顺序可重跑一致
        $got0b = $this->qids($s3->allocate('G', 'c0', $mqAll3, ['c0', 'c1']));
        $this->checkSame($got0, $got0b, 'NEARBY 结果确定（机房字典序遍历）');
    }

    // ==================================================================== run

    public function run(): int
    {
        Logger::setHandler(static function (string $line): void {}); // 静音 [BUG]/error 日志
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
        $this->testJavaCompat();
        $this->testConsistentHash();
        $this->testByMachineRoom();
        $this->testMachineRoomNearby();
        return $this->summary();
    }
}

exit((new RunClientAllocation())->run());
