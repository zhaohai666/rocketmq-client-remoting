<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\ExpressionType;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\SubscriptionData;

/**
 * 队列分配策略 + 消费前过滤。
 *
 * 文件内含：
 *  - AllocationHelper：模块级辅助（队列排序键、钩子取名、客户端二次 tag 过滤、过滤钩子执行、
 *    投递前过滤、策略取名、环上队列 key 字符串、broker 名切分）
 *  - MessageSelector / MessageQueueListener / AllocateMessageQueueStrategy 接口族
 *  - AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY
 *  - 一致性哈希环（HashFunction / MD5Hash / Node / ClientNode / VirtualNode /
 *    ConsistentHashRouter）
 */

/**
 * 分配与过滤共用的一组模块级辅助函数。
 *
 * 分工：
 *  - mqSortKey()                 队列排序键
 *  - safeHookName()              钩子取名（取不到退化成类名）
 *  - clientSideTagFilter()       客户端二次 tag 过滤
 *  - executeFilterHooks()        依次执行过滤钩子
 *  - filterMessagesForDelivery() 投递前过滤（tag 复核 + 钩子）
 *  - strategyName()              策略取名（日志用）
 *  - wireQueueKeyString()        一致性哈希环上代表队列的字符串
 *  - wireSplit()                 按分隔符切分（裁掉末尾空段）
 */
final class AllocationHelper
{
    /**
     * 队列排序键：topic → brokerName → queueId 逐段比较。
     *
     * rebalance 必须先把 mqAll/cidAll 排序再做分配；顺序不一致会让同组实例算出
     * 不同的分配结果（同一队列被两个实例同时消费）。
     *
     * @return array{0: string, 1: string, 2: int}
     */
    public static function mqSortKey(MessageQueue $mq): array
    {
        return [$mq->topic, $mq->brokerName, $mq->queueId];
    }

    /**
     * 取钩子名；hookName() 抛异常时退化成类名（取名绝不影响过滤本身）。
     */
    public static function safeHookName(FilterMessageHook $hook): string
    {
        try {
            return (string) $hook->hookName();
        } catch (\Throwable) {
            return get_class($hook);
        }
    }

    /**
     * 客户端二次 tag 过滤。
     *
     * broker 侧只按 tag 的**哈希（codeSet）**过滤，存在哈希碰撞误放，所以经典 remoting
     * 协议的消费端要再按 tag 原文核一遍。守卫「tagsSet 非空 && !classFilterMode」意味着：
     * 订阅 `"*"`（SUB_ALL）时不过滤 —— 所以 FilterAPI::buildSubscriptionData
     * 对 SUB_ALL 必须保持 tagsSet 为空（那里有详细注释）。
     *
     * @param list<MessageExt>|null $msgs
     * @return list<MessageExt>
     */
    public static function clientSideTagFilter(?SubscriptionData $sub, ?array $msgs): array
    {
        if ($msgs === null || $msgs === [] || $sub === null || $sub->tagsSet === [] || $sub->classFilterMode) {
            return $msgs ?? [];
        }
        $out = [];
        foreach ($msgs as $m) {
            $tags = $m->getTags();
            if ($tags !== null && in_array($tags, $sub->tagsSet, true)) {
                $out[] = $m;
            }
        }
        return $out;
    }

    /**
     * 依次执行过滤钩子，**单个钩子的异常只记 error、一律吞掉**。
     *
     * 与 send/consume 钩子不同：过滤钩子失败不能影响消费，只是该次过滤不生效。
     *
     * @param list<FilterMessageHook> $hookList
     */
    public static function executeFilterHooks(array $hookList, FilterMessageContext $context): void
    {
        foreach ($hookList as $hook) {
            try {
                $hook->filterMessage($context);
            } catch (\Throwable $e) {
                Logger::error(sprintf('execute hook error. hookName=%s: %s', self::safeHookName($hook), $e->getMessage()));
            }
        }
    }

    /**
     * 投递前过滤 = 客户端二次 tag 过滤 + FilterMessageHook（拉取/POP/pull 三处共用）。
     *
     * 钩子拿到的是**可变的** msgList；被摘掉的消息由调用方决定处置方式：
     * 拉取路径 = 静默跳过（位点照常推进，不 ack）；
     * POP 路径 = 必须立刻 ack，否则 invisibleTime 到期后会复活重投。
     *
     * @param list<FilterMessageHook> $hookList
     * @param list<MessageExt> $msgs
     * @return list<MessageExt>
     */
    public static function filterMessagesForDelivery(
        string $consumerGroup,
        array $hookList,
        ?MessageQueue $mq,
        ?SubscriptionData $sub,
        array $msgs,
        bool $unitMode = false
    ): array {
        $out = self::clientSideTagFilter($sub, $msgs);
        if ($out !== [] && $hookList !== []) {
            $context = new FilterMessageContext($consumerGroup, $out, $mq);
            $context->unitMode = $unitMode;    // 钩子上下文要带上单元化标记
            self::executeFilterHooks($hookList, $context);
            $out = $context->msgList;
        }
        return $out;
    }

    /**
     * 取策略名用于日志。
     *
     * 策略既可以传接口实现（一定有 getName()），也可以传只实现了 allocate 的鸭子类型对象，
     * 所以缺 getName 或它抛异常时退化成类名，绝不因为取名字而打断 rebalance。
     */
    public static function strategyName(object $strategy): string
    {
        if (method_exists($strategy, 'getName')) {
            try {
                return (string) $strategy->getName();
            } catch (\Throwable) {
                // 日志取值不该影响 rebalance
            }
        }
        return get_class($strategy);
    }

    /**
     * 一致性哈希环上代表一条队列的 key（哈希的是这个字符串，不是 MessageQueue::__toString）。
     *
     * ⚠ 必须逐字符是 "MessageQueue [topic=.., brokerName=.., queueId=..]"：
     * 本端口 MessageQueue::__toString 是另一种写法，拿它去哈希会得到完全不同的环，
     * 同组实例之间也就算不出同一条环。
     */
    public static function wireQueueKeyString(MessageQueue $mq): string
    {
        return sprintf('MessageQueue [topic=%s, brokerName=%s, queueId=%d]', $mq->topic, $mq->brokerName, $mq->queueId);
    }

    /**
     * 按「裁掉末尾空段」的语义切分字符串（等价于 limit=0 的 split）。
     *
     * PHP 原生 explode 保留尾空段，两者对 broker 名的切分结果因此不同，而
     * AllocateMessageQueueByMachineRoom 恰好按「段数 == 2」判合法，差异会直接改变
     * 一条队列参不参与分配：
     *
     * | 输入 | wireSplit | 原生 explode |
     * |---|---|---|
     * | `"room1@"` | `["room1"]`（1 段，剔除） | `["room1", ""]`（2 段，误收） |
     * | `"room1@b@"` | `["room1", "b"]`（2 段，收） | `["room1", "b", ""]`（3 段，误剔） |
     * | `"@"` | `[]` | `["", ""]` |
     * | `""` / `"broker-a"` | 无分隔符命中时**整串原样返回**，哪怕是空串 | 同 |
     *
     * 最后一行的早返回不能省：没命中分隔符时必须原样返回，所以不能无条件裁尾
     * （否则 `""` 会变成 `[]`）。
     *
     * @return list<string>
     */
    public static function wireSplit(string $text, string $sep): array
    {
        $parts = explode($sep, $text);
        if (count($parts) === 1) {
            return $parts; // 没命中分隔符：整串原样返回
        }
        while ($parts !== [] && end($parts) === '') {
            array_pop($parts);
        }
        return array_values($parts);
    }
}

final class MessageSelector
{
    public function __construct(
        public string $type = ExpressionType::TAG,
        public string $expression = '*',
    ) {
    }

    public static function byTag(string $tag): self
    {
        return new self(ExpressionType::TAG, $tag);
    }

    public static function bySql(string $sql): self
    {
        return new self(ExpressionType::SQL92, $sql);
    }
}

interface MessageQueueListener
{
    /** @param list<MessageQueue> $mqAll @param list<MessageQueue> $mqDivided */
    public function messageQueueChanged(string $topic, array $mqAll, array $mqDivided): void;
}

interface AllocateMessageQueueStrategy
{
    /**
     * @param list<MessageQueue> $mqAll
     * @param list<string> $cidAll
     * @return list<MessageQueue>
     */
    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array;

    public function getName(): string;
}

final class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy
{
    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        if ($currentCID === '' || $mqAll === [] || $cidAll === []) {
            return [];
        }
        $index = array_search($currentCID, $cidAll, true);
        if ($index === false) {
            Logger::info(sprintf(
                '[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s',
                $consumerGroup,
                $currentCID,
                json_encode($cidAll)
            ));
            return [];
        }
        $index = (int) $index;
        $mod = count($mqAll) % count($cidAll);
        $averageSize = intdiv(count($mqAll), count($cidAll));
        if ($averageSize === 0) {
            return $index < count($mqAll) ? [$mqAll[$index]] : [];
        }
        if ($mod > 0 && $index < $mod) {
            $startIndex = $index * ($averageSize + 1);
            $endIndex = $startIndex + $averageSize + 1;
        } else {
            $startIndex = $mod * ($averageSize + 1) + ($index - $mod) * $averageSize;
            $endIndex = $startIndex + $averageSize;
        }
        return array_slice($mqAll, $startIndex, $endIndex - $startIndex);
    }

    public function getName(): string
    {
        return 'AVG';
    }
}

final class AllocateMessageQueueAveragelyByCircle implements AllocateMessageQueueStrategy
{
    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        if ($currentCID === '' || $mqAll === [] || $cidAll === []) {
            return [];
        }
        $index = array_search($currentCID, $cidAll, true);
        if ($index === false) {
            Logger::info(sprintf(
                '[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s',
                $consumerGroup,
                $currentCID,
                json_encode($cidAll)
            ));
            return [];
        }
        $index = (int) $index;
        $result = [];
        for ($i = $index, $n = count($mqAll), $step = count($cidAll); $i < $n; $i += $step) {
            $result[] = $mqAll[$i];
        }
        return $result;
    }

    public function getName(): string
    {
        return 'AVG_BY_CIRCLE';
    }
}

final class AllocateMessageQueueByConfig implements AllocateMessageQueueStrategy
{
    /** @var list<MessageQueue> */
    private array $messageQueueList;

    /** @param list<MessageQueue>|null $messageQueueList */
    public function __construct(?array $messageQueueList = null)
    {
        $this->messageQueueList = $messageQueueList ?? [];
    }

    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        return $this->messageQueueList;
    }

    public function getName(): string
    {
        return 'CONFIG';
    }
}


interface HashFunction
{
    public function hash(string $key): int;
}

final class MD5Hash implements HashFunction
{
    public function hash(string $key): int
    {
        $digest = md5($key, true);
        $value = 0;
        for ($i = 0; $i < 4; $i++) {
            $value = ($value << 8) | ord($digest[$i]);
        }
        return $value; // 32bit 无符号，PHP int 64bit 不会溢出
    }
}

interface Node
{
    public function getKey(): string;
}

final class ClientNode implements Node
{
    public function __construct(public readonly string $clientId)
    {
    }

    public function getKey(): string
    {
        return $this->clientId;
    }
}

final class VirtualNode implements Node
{
    public function __construct(
        private readonly Node $physicalNode,
        public readonly int $replicaIndex,
    ) {
    }

    public function getKey(): string
    {
        return sprintf('%s-%d', $this->physicalNode->getKey(), $this->replicaIndex);
    }

    public function isVirtualNodeOf(Node $pNode): bool
    {
        return $this->physicalNode->getKey() === $pNode->getKey();
    }

    public function getPhysicalNode(): Node
    {
        return $this->physicalNode;
    }
}

final class ConsistentHashRouter
{
    private HashFunction $hashFunction;

    private array $ring = [];

    /** @var list<int> 升序 hash key 列表（对应 TreeMap 的 keySet） */
    private array $keys = [];

    /** @param list<Node>|null $pNodes */
    public function __construct(?array $pNodes = null, int $vNodeCount = 0, ?HashFunction $hashFunction = null)
    {
        $this->hashFunction = $hashFunction ?? new MD5Hash();
        if ($pNodes !== null) {
            foreach ($pNodes as $pNode) {
                $this->addNode($pNode, $vNodeCount);
            }
        }
    }

    public function addNode(Node $pNode, int $vNodeCount): void
    {
        if ($vNodeCount < 0) {
            throw new \ValueError(sprintf('illegal virtual node counts :%d', $vNodeCount));
        }
        $existingReplicas = $this->getExistingReplicas($pNode);
        for ($i = 0; $i < $vNodeCount; $i++) {
            $vNode = new VirtualNode($pNode, $i + $existingReplicas);
            $key = $this->hashFunction->hash($vNode->getKey());
            if (!isset($this->ring[$key])) {
                $this->insertKeySorted($key);
            }
            $this->ring[$key] = $vNode;
        }
    }

    public function removeNode(Node $pNode): void
    {
        foreach ($this->keys as $k) {
            if ($this->ring[$k]->isVirtualNodeOf($pNode)) {
                unset($this->ring[$k]);
            }
        }
        $this->keys = array_values(array_filter(
            $this->keys,
            fn (int $k): bool => isset($this->ring[$k])
        ));
    }

    public function routeNode(string $objectKey): ?Node
    {
        if ($this->ring === []) {
            return null;
        }
        $index = $this->bisectLeft($this->keys, $this->hashFunction->hash($objectKey));
        if ($index === count($this->keys)) {
            $index = 0;
        }
        return $this->ring[$this->keys[$index]]->getPhysicalNode();
    }

    public function getExistingReplicas(Node $pNode): int
    {
        $count = 0;
        foreach ($this->ring as $vNode) {
            if ($vNode->isVirtualNodeOf($pNode)) {
                $count++;
            }
        }
        return $count;
    }

    /** @param list<int> $keys */
    private function insertKeySorted(int $key): void
    {
        $pos = $this->bisectLeft($this->keys, $key);
        array_splice($this->keys, $pos, 0, [$key]);
    }

    /**
     * 二分查找：返回 keys 中第一个 >= target 的下标（升序数组）。
     *
     * @param list<int> $keys
     */
    private function bisectLeft(array $keys, int $target): int
    {
        $lo = 0;
        $hi = count($keys);
        while ($lo < $hi) {
            $mid = ($lo + $hi) >> 1;
            if ($keys[$mid] < $target) {
                $lo = $mid + 1;
            } else {
                $hi = $mid;
            }
        }
        return $lo;
    }
}

final class AllocateMessageQueueConsistentHash implements AllocateMessageQueueStrategy
{
    public function __construct(
        private readonly int $virtualNodeCnt = 10,
        private readonly ?HashFunction $customHashFunction = null,
    ) {
        if ($virtualNodeCnt < 0) {
            throw new \ValueError(sprintf('illegal virtualNodeCnt :%d', $virtualNodeCnt));
        }
    }

    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        if ($currentCID === '' || $mqAll === [] || $cidAll === []) {
            return [];
        }
        if (!in_array($currentCID, $cidAll, true)) {
            Logger::info(sprintf(
                '[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s',
                $consumerGroup,
                $currentCID,
                json_encode($cidAll)
            ));
            return [];
        }
        $cidNodes = [];
        foreach ($cidAll as $cid) {
            $cidNodes[] = new ClientNode($cid);
        }
        if ($this->customHashFunction !== null) {
            $router = new ConsistentHashRouter($cidNodes, $this->virtualNodeCnt, $this->customHashFunction);
        } else {
            $router = new ConsistentHashRouter($cidNodes, $this->virtualNodeCnt);
        }
        $result = [];
        foreach ($mqAll as $mq) {
            $node = $router->routeNode(AllocationHelper::wireQueueKeyString($mq));
            if ($node !== null && $node->getKey() === $currentCID) {
                $result[] = $mq;
            }
        }
        return $result;
    }

    public function getName(): string
    {
        return 'CONSISTENT_HASH';
    }
}

final class AllocateMessageQueueByMachineRoom implements AllocateMessageQueueStrategy
{
    /** set 语义：key = 机房名，value 同名占位。 @var array<string, string> */
    private array $consumeridcs = [];

    /** @param list<string>|null $consumeridcs */
    public function __construct(?array $consumeridcs = null)
    {
        if ($consumeridcs !== null) {
            foreach ($consumeridcs as $c) {
                $this->consumeridcs[$c] = $c;
            }
        }
    }

    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        if ($currentCID === '' || $mqAll === [] || $cidAll === []) {
            return [];
        }
        $currentIndex = array_search($currentCID, $cidAll, true);
        if ($currentIndex === false) {
            Logger::info(sprintf(
                '[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s',
                $consumerGroup,
                $currentCID,
                json_encode($cidAll)
            ));
            return [];
        }
        $currentIndex = (int) $currentIndex;
        $premqAll = [];
        foreach ($mqAll as $mq) {
            $temp = AllocationHelper::wireSplit($mq->brokerName, '@');
            if (count($temp) === 2 && isset($this->consumeridcs[$temp[0]])) {
                $premqAll[] = $mq;
            }
        }
        $mod = intdiv(count($premqAll), count($cidAll));
        $rem = count($premqAll) % count($cidAll);
        $startIndex = $mod * $currentIndex;
        $result = array_slice($premqAll, $startIndex, $mod);
        if ($rem > $currentIndex) {
            $result[] = $premqAll[$currentIndex + $mod * count($cidAll)];
        }
        return $result;
    }

    public function getName(): string
    {
        return 'MACHINE_ROOM';
    }

    /** @return list<string> */
    public function getConsumeridcs(): array
    {
        return array_values($this->consumeridcs);
    }

    /** @param list<string> $consumeridcs */
    public function setConsumeridcs(array $consumeridcs): void
    {
        $this->consumeridcs = [];
        foreach ($consumeridcs as $c) {
            $this->consumeridcs[$c] = $c;
        }
    }
}

interface MachineRoomResolver
{
    public function brokerDeployIn(MessageQueue $messageQueue): string;

    public function consumerDeployIn(string $clientId): string;
}

final class AllocateMachineRoomNearby implements AllocateMessageQueueStrategy
{
    public function __construct(
        private readonly AllocateMessageQueueStrategy $allocateMessageQueueStrategy,
        private readonly MachineRoomResolver $machineRoomResolver,
    ) {
    }

    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        if ($currentCID === '' || $mqAll === [] || $cidAll === []) {
            return [];
        }
        if (!in_array($currentCID, $cidAll, true)) {
            Logger::info(sprintf(
                '[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s',
                $consumerGroup,
                $currentCID,
                json_encode($cidAll)
            ));
            return [];
        }

        /** @var array<string, list<MessageQueue>> $mr2Mq */
        $mr2Mq = [];
        foreach ($mqAll as $mq) {
            $room = $this->machineRoomResolver->brokerDeployIn($mq);
            if ($room !== '') {
                $mr2Mq[$room][] = $mq;
            } else {
                throw new \ValueError(sprintf(
                    'Machine room is null for mq %s',
                    AllocationHelper::wireQueueKeyString($mq)
                ));
            }
        }
        /** @var array<string, list<string>> $mr2C */
        $mr2C = [];
        foreach ($cidAll as $cid) {
            $room = $this->machineRoomResolver->consumerDeployIn($cid);
            if ($room !== '') {
                $mr2C[$room][] = $cid;
            } else {
                throw new \ValueError(sprintf('Machine room is null for consumer id %s', $cid));
            }
        }

        /** @var list<MessageQueue> $allocateResults */
        $allocateResults = [];
        // 1. 本消费者所在机房的队列：只在同机房消费者之间分
        $currentMachineRoom = $this->machineRoomResolver->consumerDeployIn($currentCID);
        $mqInThisMachineRoom = $mr2Mq[$currentMachineRoom] ?? null;
        unset($mr2Mq[$currentMachineRoom]);
        $consumerInThisMachineRoom = $mr2C[$currentMachineRoom] ?? [];
        if ($mqInThisMachineRoom !== null && $mqInThisMachineRoom !== []) {
            array_push(
                $allocateResults,
                ...$this->allocateMessageQueueStrategy->allocate(
                    $consumerGroup,
                    $currentCID,
                    $mqInThisMachineRoom,
                    $consumerInThisMachineRoom
                )
            );
        }
        // 2. 没有活消费者的机房：队列不能没人消费，交给全部消费者
        $rooms = array_keys($mr2Mq);
        sort($rooms, SORT_STRING);
        foreach ($rooms as $room) {
            if (!isset($mr2C[$room])) {
                array_push(
                    $allocateResults,
                    ...$this->allocateMessageQueueStrategy->allocate(
                        $consumerGroup,
                        $currentCID,
                        $mr2Mq[$room],
                        $cidAll
                    )
                );
            }
        }
        return $allocateResults;
    }

    public function getName(): string
    {
        return sprintf('MACHINE_ROOM_NEARBY-%s', AllocationHelper::strategyName($this->allocateMessageQueueStrategy));
    }
}
