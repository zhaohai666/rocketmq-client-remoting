<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\ExpressionType;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\SubscriptionData;

/**
 * Queue allocation strategies + pre-consume filtering.
 *
 * This file contains:
 *  - AllocationHelper: module-level helpers (queue sort key, hook naming, client-side
 *    secondary tag filtering, filter hook execution, pre-delivery filtering, strategy
 *    naming, on-ring queue key string, broker name splitting)
 *  - MessageSelector / MessageQueueListener / AllocateMessageQueueStrategy interface family
 *  - AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY
 *  - Consistent hash ring (HashFunction / MD5Hash / Node / ClientNode / VirtualNode /
 *    ConsistentHashRouter)
 */

/**
 * A set of module-level helper functions shared by allocation and filtering.
 *
 * Division of labor:
 *  - mqSortKey()                 queue sort key
 *  - safeHookName()              hook naming (falls back to the class name)
 *  - clientSideTagFilter()       client-side secondary tag filtering
 *  - executeFilterHooks()        execute filter hooks in order
 *  - filterMessagesForDelivery() pre-delivery filtering (tag re-check + hooks)
 *  - strategyName()              strategy naming (for logs)
 *  - wireQueueKeyString()        the string representing a queue on the consistent hash ring
 *  - wireSplit()                 split by separator (trailing empty segments trimmed)
 */
final class AllocationHelper
{
    /**
     * Queue sort key: topic -> brokerName -> queueId, compared segment by segment.
     *
     * rebalance must sort mqAll/cidAll before allocating; inconsistent ordering makes
     * instances in the same group compute different allocation results (the same queue
     * consumed by two instances at once).
     *
     * @return array{0: string, 1: string, 2: int}
     */
    public static function mqSortKey(MessageQueue $mq): array
    {
        return [$mq->topic, $mq->brokerName, $mq->queueId];
    }

    /**
     * Take the hook name; degrade to the class name when hookName() throws (naming must never affect filtering itself).
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
     * Client-side secondary tag filtering.
     *
     * The broker side only filters by the tag **hash (codeSet)**, which allows false
     * positives from hash collisions, so the classic remoting protocol's consume side
     * must check the tag text again. The guard "tagsSet non-empty && !classFilterMode"
     * means: no filtering when subscribed to `"*"` (SUB_ALL) -- so
     * FilterAPI::buildSubscriptionData must keep tagsSet empty for SUB_ALL
     * (detailed comments there).
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
     * Execute the filter hooks in order; **a single hook's exception is only logged as error and always swallowed**.
     *
     * Unlike send/consume hooks: a filter hook failure must not affect consumption, it just
     * means that filtering pass does not take effect.
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
     * Pre-delivery filtering = client-side secondary tag filtering + FilterMessageHook (shared by the pull/POP/pull three call sites).
     *
     * The hooks receive a **mutable** msgList; removed messages are disposed of by the
     * caller: the pull path = silently skip (offset advances as usual, no ack);
     * the POP path = must ack immediately, otherwise the message revives and is redelivered
     * after invisibleTime expires.
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
            $context->unitMode = $unitMode;    // the hook context must carry the unitized marker
            self::executeFilterHooks($hookList, $context);
            $out = $context->msgList;
        }
        return $out;
    }

    /**
     * Take the strategy name for logs.
     *
     * A strategy can be passed either as an interface implementation (always has getName())
     * or as a duck-typed object that only implements allocate, so when getName is missing
     * or it throws, degrade to the class name; never interrupt rebalance just to get a name.
     */
    public static function strategyName(object $strategy): string
    {
        if (method_exists($strategy, 'getName')) {
            try {
                return (string) $strategy->getName();
            } catch (\Throwable) {
                // taking a log value should not affect rebalance
            }
        }
        return get_class($strategy);
    }

    /**
     * The key representing a queue on the consistent hash ring (this string is hashed, not MessageQueue::__toString).
     *
     * WARNING: it must be exactly "MessageQueue [topic=.., brokerName=.., queueId=..]"
     * character for character: this port's MessageQueue::__toString is a different
     * wording; hashing it yields a completely different ring, and instances in the same
     * group would not compute the same ring.
     */
    public static function wireQueueKeyString(MessageQueue $mq): string
    {
        return sprintf('MessageQueue [topic=%s, brokerName=%s, queueId=%d]', $mq->topic, $mq->brokerName, $mq->queueId);
    }

    /**
     * Split a string with "trailing empty segments trimmed" semantics (equivalent to a limit=0 split).
     *
     * PHP's native explode keeps trailing empty segments, so the two differ when splitting
     * a broker name, and AllocateMessageQueueByMachineRoom judges validity exactly by
     * "segment count == 2", so the difference directly changes whether a queue
     * participates in allocation:
     *
     * | input | wireSplit | native explode |
     * |---|---|---|
     * | `"room1@"` | `["room1"]` (1 segment, excluded) | `["room1", ""]` (2 segments, wrongly included) |
     * | `"room1@b@"` | `["room1", "b"]` (2 segments, included) | `["room1", "b", ""]` (3 segments, wrongly excluded) |
     * | `"@"` | `[]` | `["", ""]` |
     * | `""` / `"broker-a"` | when no separator matches, **the whole string is returned as-is**, even if empty | same |
     *
     * The last row's early return cannot be omitted: when no separator matches the whole
     * string must be returned as-is, so trailing segments cannot be trimmed unconditionally
     * (otherwise `""` would become `[]`).
     *
     * @return list<string>
     */
    public static function wireSplit(string $text, string $sep): array
    {
        $parts = explode($sep, $text);
        if (count($parts) === 1) {
            return $parts; // no separator match: return the whole string as-is
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
        return $value; // 32bit unsigned; PHP's 64bit int does not overflow
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

    /** @var list<int> ascending hash key list (corresponding to TreeMap's keySet) */
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
     * Binary search: returns the index of the first element >= target in keys (ascending array).
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
    /** set semantics: key = machine room name, value = same-name placeholder. @var array<string, string> */
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
        // 1. Queues in the machine room where this consumer resides: distributed only among consumers in the same machine room
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
        // 2. Machine rooms with no live consumers: queues cannot be left unconsumed, hand them to all consumers
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
