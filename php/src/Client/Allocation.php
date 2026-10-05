<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\ExpressionType;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\SubscriptionData;

/**
 * 队列分配策略 + 消费前过滤（移植自 consumer.py 第 89-620 行，对应
 * org.apache.rocketmq.client.consumer 里的分配策略部分与 Java common 包的
 * consistenthash 一致性哈希环）。
 *
 * 文件内含：
 *  - AllocationHelper：consumer.py 的模块级 helper（_mq_sort_key / _safe_hook_name /
 *    client_side_tag_filter / execute_filter_hooks / filter_messages_for_delivery /
 *    _strategy_name / _java_message_queue_string / _java_split）
 *  - MessageSelector / MessageQueueListener / AllocateMessageQueueStrategy 接口族
 *  - AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY
 *  - 一致性哈希环（HashFunction / MD5Hash / Node / ClientNode / VirtualNode /
 *    ConsistentHashRouter）
 */

/**
 * consumer.py 模块级 helper 的静态收容所。
 *
 * 对应关系：
 *  - mqSortKey()               ← _mq_sort_key
 *  - safeHookName()            ← _safe_hook_name
 *  - clientSideTagFilter()     ← client_side_tag_filter
 *  - executeFilterHooks()      ← execute_filter_hooks
 *  - filterMessagesForDelivery() ← filter_messages_for_delivery
 *  - strategyName()            ← _strategy_name
 *  - javaMessageQueueString()  ← _java_message_queue_string
 *  - javaSplit()               ← _java_split
 */
final class AllocationHelper
{
    /**
     * 队列排序键，语义对齐 Java MessageQueue.compareTo：topic → brokerName → queueId。
     *
     * Java rebalance 会先把 mqAll/cidAll 排序再分配；顺序不一致会让不同实例算出
     * 不同的分配结果（同一队列被两个实例同时消费）。
     *
     * @return array{0: string, 1: string, 2: int}
     */
    public static function mqSortKey(MessageQueue $mq): array
    {
        return [$mq->topic, $mq->brokerName, $mq->queueId];
    }

    /**
     * 取钩子名；hookName() 抛异常时退化成类名（Python 侧同语义）。
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
     * 客户端二次 tag 过滤（对应 Java PullAPIWrapper.processPullResult:113-122）。
     *
     * broker 侧是按 tag 的**哈希（codeSet）**过滤的，存在哈希碰撞误放；Java 因此让客户端
     * 再按字符串核一遍。守卫 `!tagsSet.isEmpty() && !isClassFilterMode` 意味着：
     * 订阅 `"*"`（SUB_ALL）时不过滤 —— 所以 FilterAPI.build_subscription_data
     * 对 SUB_ALL 必须保持 tags_set 为空（那里有详细注释）。
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
     * 依次执行过滤钩子，**异常一律吞掉**（Java PullAPIWrapper.executeHook:171-178 记 error）。
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
     * 拉取路径 = 静默跳过（位点照常推进，不 ack，Java 亦然）；
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
            $context->unitMode = $unitMode;    // Java DefaultMQPushConsumerImpl:640
            self::executeFilterHooks($hookList, $context);
            $out = $context->msgList;
        }
        return $out;
    }

    /**
     * 取策略名用于日志。
     *
     * Java 侧策略是接口实现、必有 getName()；Python 允许业务方鸭子类型地传一个只有
     * allocate 的自定义对象，所以缺 getName 时退化成类名而不是抛 Error。
     * （PHP 侧同样接受任意 object，接口实现之外还能喂鸭子类型对象。）
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
     * 对应 Java `MessageQueue#toString`（一致性哈希要哈希**它**，不是 __toString）。
     *
     * ⚠ 必须逐字符等于 Java 的 "MessageQueue [topic=.., brokerName=.., queueId=..]"：
     * PHP 的 MessageQueue::__toString 是另一种写法，拿它去哈希会得到完全不同的环。
     */
    public static function javaMessageQueueString(MessageQueue $mq): string
    {
        return sprintf('MessageQueue [topic=%s, brokerName=%s, queueId=%d]', $mq->topic, $mq->brokerName, $mq->queueId);
    }

    /**
     * 对应 Java `String#split(String)`（limit=0）：**丢掉末尾的空段**。
     *
     * PHP 的 explode 保留尾空段，两边对 broker 名的切分结果因此不同，而
     * AllocateMessageQueueByMachineRoom 恰好按 `length == 2` 判合法，差异会直接改变
     * 一条队列参不参与分配：
     *
     * | 输入 | Java | PHP 原生 explode |
     * |---|---|---|
     * | `"room1@"` | `["room1"]`（1 段，剔除） | `["room1", ""]`（2 段，误收） |
     * | `"room1@b@"` | `["room1", "b"]`（2 段，收） | `["room1", "b", ""]`（3 段，误剔） |
     * | `"@"` | `[]` | `["", ""]` |
     * | `""` / `"broker-a"` | 无分隔符命中时**整串原样返回**，哪怕是空串 | 同 |
     *
     * 最后一行是 Java `Pattern#split` 里 "If no match was found, return this" 那个早返回，
     * 所以不能无条件裁尾（否则 `""` 会变成 `[]`）。以上取值用 JDK 17 实测核对过。
     *
     * @return list<string>
     */
    public static function javaSplit(string $text, string $sep): array
    {
        $parts = explode($sep, $text);
        if (count($parts) === 1) {
            return $parts; // 没命中分隔符：Java 原样返回整串
        }
        while ($parts !== [] && end($parts) === '') {
            array_pop($parts);
        }
        return array_values($parts);
    }
}

/**
 * 消息选择器（对应 Java MessageSelector）。
 */
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

/**
 * 队列变更监听器（对应 Java MessageQueueListener）。
 *
 * Python 是 raise NotImplementedError 的契约类；PHP 用 interface 表达
 * （Python MessageQueueListener.message_queue_changed ↔ 本方法）。
 *
 * @phpstan-require-implements MessageQueueListener
 */
interface MessageQueueListener
{
    /** @param list<MessageQueue> $mqAll @param list<MessageQueue> $mqDivided */
    public function messageQueueChanged(string $topic, array $mqAll, array $mqDivided): void;
}

/**
 * 队列分配策略接口（对应 Java AllocateMessageQueueStrategy）。
 *
 * 与 Java 的有意差异：Java 的 ``AbstractAllocateMessageQueueStrategy#check`` 在非法入参时抛
 * ``IllegalArgumentException``，这里一律返回空列表——rebalance 是后台周期任务，
 * 一条脏入参不该把消费者打挂。
 */
interface AllocateMessageQueueStrategy
{
    /**
     * @param list<MessageQueue> $mqAll
     * @param list<string> $cidAll
     * @return list<MessageQueue>
     */
    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array;

    /**
     * 对应 Java ``AllocateMessageQueueStrategy#getName``：算法名（AVG 等）。
     */
    public function getName(): string;
}

/**
 * 平均分配（对应 Java AllocateMessageQueueAveragely）。
 */
final class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy
{
    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        // 守卫顺序与 Java AbstractAllocateMessageQueueStrategy#check 一致：currentCID →
        // mqAll → cidAll。唯一偏差：Java 这三种情况都抛 IllegalArgumentException，
        // 这里只返回空列表（rebalance 是后台周期任务，脏入参不该把消费者打挂）；
        // currentCID 不在 cidAll 时保留 Java 那条 [BUG] info 日志。
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

/**
 * 环形平均分配（对应 Java AllocateMessageQueueAveragelyByCircle）。
 */
final class AllocateMessageQueueAveragelyByCircle implements AllocateMessageQueueStrategy
{
    public function allocate(string $consumerGroup, string $currentCID, array $mqAll, array $cidAll): array
    {
        // 守卫与 Averagely 同款（Java AbstractAllocateMessageQueueStrategy#check），同样只返回空列表。
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

/**
 * 按显式配置分配（对应 Java AllocateMessageQueueByConfig）。
 *
 * 与 Java 的有意差异：Java 直接 ``return this.messageQueueList``，未配置时是 ``null``；
 * 这里构造器规整成空列表、allocate 返回列表副本。两边的 ``allocate`` 都**不**做
 * check，所以空 group / 空 cid_all 也照样返回配置值。
 */
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

// ------------------------------------------------------ 一致性哈希环（Java common 包）
//
// 对应 org.apache.rocketmq.common.consistenthash.{HashFunction, Node, VirtualNode,
// ConsistentHashRouter}。只有 AllocateMessageQueueConsistentHash 用它，但整套照搬而不做
// "等价改写"：环上的落点由 MD5 取字节的方式、虚拟节点命名、tailMap 含端点这三处细节
// 共同决定，任何一处不同都会让全体队列换主，与 Java 客户端混跑时表现为重复/漏消费。

/**
 * 对应 Java `HashFunction#hash(String)`。自定义哈希用它注入策略。
 */
interface HashFunction
{
    public function hash(string $key): int;
}

/**
 * 对应 Java `ConsistentHashRouter.MD5Hash`：MD5 摘要的**前 4 字节**按大端拼成整数。
 *
 * ⚠ 只取前 4 字节（不是完整 128 bit），换成其它取法就与 Java 不是同一个环。
 */
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

/**
 * 对应 Java `Node#getKey`：环上可寻址的东西，物理或虚拟都行。
 */
interface Node
{
    public function getKey(): string;
}

/**
 * 对应 Java `AllocateMessageQueueConsistentHash.ClientNode`：key 就是 clientId。
 */
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

/**
 * 对应 Java `VirtualNode`：key = 物理节点 key + "-" + 副本序号。
 */
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

/**
 * 对应 Java `ConsistentHashRouter`：把节点哈希成环，路由到顺时针最近的物理节点。
 *
 * Java 用 `TreeMap<Long, VirtualNode>`；这里用「升序 key 列表 + hash 表」等价替代。
 * `routeNode` 要的是 `tailMap(hashVal).firstKey()`，而 TreeMap 的 tailMap **含端点**，
 * 所以等价的查找是 bisect_left（相等的 hash 归自己），环空或越过末尾时回绕到首节点。
 */
final class ConsistentHashRouter
{
    private HashFunction $hashFunction;

    /** @var array<int, VirtualNode> hash → 虚拟节点（同 hash 后者覆盖，同 Java TreeMap.put） */
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
        // 对应 Java `#addNode`：已有副本要接着编号，否则同一个物理节点的 v 个虚拟节点
        // 会全落在同一个 hash 上（Java 分两次 addNode 时靠 i + existingReplicas 区分）。
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
            // Java 是 TreeMap.put：同 hash 时后来者覆盖，位置不变
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
            $index = 0; // 越过环的末尾 → 回绕到第一个（Java 的 ring.firstKey()）
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
     * Python bisect_left：返回 keys 中第一个 >= target 的下标（升序数组，二分）。
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

/**
 * 一致性哈希分配（对应 Java `AllocateMessageQueueConsistentHash`，`getName()` 为
 * `CONSISTENT_HASH`）。
 *
 * 与 AVG / AVG_BY_CIRCLE 的差别不是"分得均不均"，而是**稳定性**：队列数或消费者数变化时，
 * 只有落在新增/移除节点之间弧段上的队列会换主（Java 单测
 * `AllocateMessageQueueConsitentHashTest` 正是断言这一点），AVG 则会把所有人的分界整体挪掉。
 *
 * 守卫口径同其它策略：Java 的 `check` 抛 IllegalArgumentException，这里返回空列表。
 * 唯一保留抛的是构造函数里的 `virtualNodeCnt < 0`（Java 也在构造时抛，且不属于 rebalance
 * 后台路径）。
 */
final class AllocateMessageQueueConsistentHash implements AllocateMessageQueueStrategy
{
    /**
     * 对应 Java 三个构造函数的链：默认 10 个虚拟节点、默认 MD5Hash。
     */
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
            $node = $router->routeNode(AllocationHelper::javaMessageQueueString($mq));
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

/**
 * 按机房分配（对应 Java `AllocateMessageQueueByMachineRoom`，`getName()` 为
 * `MACHINE_ROOM`，注释里的场景是"支付宝逻辑机房"）。
 *
 * 约定 broker 名写成 `<机房>@<brokerName>`，只有前缀落在 `consumeridcs` 里的队列参与
 * 分配，然后在这些队列内部再做一次"平均分配"（分片算法与 AVG 逐行相同，但 rem 的归属
 * 判据是 `rem > currentIndex`，即余数队列发给前 rem 个消费者）。
 *
 * ⚠ broker 名的切分走 AllocationHelper::javaSplit()，不是 PHP 原生 explode：Java 会丢掉
 * 末尾空段，`"room1@"` 在 Java 是 1 段（不参与分配）、`"room1@b@"` 是 2 段（参与）。
 *
 * ⚠ Java 的 `consumeridcs` 字段没有默认值，没 set 就 `contains` → NPE；这里默认空集合，
 * 表现为"一条都不分"，与本端口一贯的"守卫返回空结果"口径一致。
 */
final class AllocateMessageQueueByMachineRoom implements AllocateMessageQueueStrategy
{
    /** set 语义（对应 Python set）：key = 机房名，value 同名占位。 @var array<string, string> */
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
            $temp = AllocationHelper::javaSplit($mq->brokerName, '@');
            if (count($temp) === 2 && isset($this->consumeridcs[$temp[0]])) {
                $premqAll[] = $mq;
            }
        }
        $mod = intdiv(count($premqAll), count($cidAll)); // Java 是 int 除法
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

/**
 * 对应 Java `AllocateMachineRoomNearby.MachineRoomResolver`：告诉策略"谁在哪个机房"。
 *
 * Java 注释明确写了两个方法**都不能返回 null**（否则该机房视为空，队列会被撤走）；
 * Python 侧以返回空串表示 null，这里同样以空串触发异常。
 */
interface MachineRoomResolver
{
    public function brokerDeployIn(MessageQueue $messageQueue): string;

    public function consumerDeployIn(string $clientId): string;
}

/**
 * 机房就近分配（对应 Java `AllocateMachineRoomNearby`）。
 *
 * 代理模式：先按机房把队列和消费者各自分组，
 * 1. 本消费者所在机房的队列只分给**同机房**的消费者（用内层策略）；
 * 2. 那些**机房里没有任何活消费者**的队列，交给所有消费者按内层策略瓜分——
 *    否则它们就没人消费了。
 *
 * `getName()` 是 `"MACHINE_ROOM_NEARBY" + "-" + 内层策略名`（Java 同），因为日志里
 * 必须能看出实际用的是哪个分配算法。
 *
 * 两个构造参数缺失时 Java 抛 NullPointerException；PHP 由非空类型标注在传入 null 时抛
 * TypeError 达成同样效果（调用期即失败，不属于 rebalance 后台静默路径）。
 * resolver 给出空机房时 Python 抛 ValueError —— 这里**照抛**：静默返回空列表等于把整个
 * topic 的队列撤走，而 rebalance 抓住异常时反而会保住现有分配，与 Java 行为一致。
 */
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

        // 按机房分组。Java 用 TreeMap ⇒ 机房名**字典序**遍历，这里同样排序，
        // 否则同名机房的处理顺序会随插入顺序变（结果集是并集，顺序也会进日志/断言）。
        /** @var array<string, list<MessageQueue>> $mr2Mq */
        $mr2Mq = [];
        foreach ($mqAll as $mq) {
            $room = $this->machineRoomResolver->brokerDeployIn($mq);
            if ($room !== '') {
                $mr2Mq[$room][] = $mq;
            } else {
                throw new \ValueError(sprintf(
                    'Machine room is null for mq %s',
                    AllocationHelper::javaMessageQueueString($mq)
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
