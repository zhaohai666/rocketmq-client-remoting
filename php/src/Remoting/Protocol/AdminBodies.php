<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\TopicConfig;

/**
 * Response bodies dedicated to the admin side (the management structures in the protocol):
 *
 * TopicStatsTable / TopicOffset / ConsumeStats / OffsetWrapper /
 * TopicConfigSerializeWrapper / ConsumeQueueData / QueryConsumeQueueResponseBody.
 *
 * ⚠ The key trap: the Map keys of these classes are **MessageQueue**, and the broker-side serializer
 * inlines the keys directly as JSON objects, producing **illegal JSON**, for example:
 *     {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:{...}}}
 * So they must be parsed with RemotingSerializable::fastjsonLoads(), and then the string keys have to be
 * restored into MessageQueue. The field names and structures were all confirmed by measuring real
 * responses — do not change them from memory.
 *
 * PHP cannot use objects as array keys: every map keyed by MessageQueue is stored uniformly as
 * ``list<array{mq: MessageQueue, value: mixed}>``, while the JSON output still uses messageQueueKey() to
 * inline the object keys.
 */

/**
 * Restores the MessageQueue from the MessageQueue inline object key written by the broker.
 *
 * Returning null means that this key is not an inline object (for instance an ordinary string key such as
 * ``"0"`` or ``"G1"``).
 */
function parse_message_queue_key(string $key): ?MessageQueue
{
    $d = RemotingSerializable::decodeMessageQueueKey($key);
    if ($d === null) {
        return null;
    }
    return new MessageQueue(
        (string)($d['topic'] ?? ''),
        (string)($d['brokerName'] ?? ''),
        (int)($d['queueId'] ?? 0),
    );
}

/** Serializes a MessageQueue into a fastjson2-style inline object key (the keys in alphabetical order). */
function message_queue_key(MessageQueue $mq): string
{
    return sprintf('{"brokerName":"%s","queueId":%d,"topic":"%s"}', $mq->brokerName, $mq->queueId, $mq->topic);
}

/**
 * Generic: parses a map keyed by MessageQueue (fastjson2 non-string keys).
 * Only the keys that can be restored into a MessageQueue are taken in.
 *
 * @return list<array{mq: MessageQueue, value: mixed}>
 */
function decode_message_queue_map(?array $raw): array
{
    $result = [];
    foreach ($raw ?? [] as $k => $v) {
        $mq = parse_message_queue_key((string)$k);
        if ($mq !== null) {
            $result[] = ['mq' => $mq, 'value' => $v];
        }
    }
    return $result;
}

// ---------------------------------------------------------------- TopicStatsTable

/** The minimum / maximum offset of one topic. */
final class TopicOffset
{
    public function __construct(
        public int $minOffset = 0,
        public int $maxOffset = 0,
        public int $lastUpdateTimestamp = 0,
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'minOffset' => $this->minOffset,
            'maxOffset' => $this->maxOffset,
            'lastUpdateTimestamp' => $this->lastUpdateTimestamp,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            (int)($d['minOffset'] ?? 0),
            (int)($d['maxOffset'] ?? 0),
            (int)($d['lastUpdateTimestamp'] ?? 0),
        );
    }
}

/**
 * The per-queue send/receive statistics table of a topic.
 *
 * Probe output: {"offsetTable":{<MessageQueue>:{...}},"topicPutTps":0.0}
 */
final class TopicStatsTable
{
    /** @var list<array{mq: MessageQueue, value: TopicOffset}> */
    public array $offsetTable = [];
    public float $topicPutTps = 0.0;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $table = [];
        foreach ($this->offsetTable as ['mq' => $mq, 'value' => $v]) {
            $table[message_queue_key($mq)] = $v->toDict();
        }
        return [
            // An inline object key is not legal JSON, so the fastjson compatible path is mandatory
            'offsetTable' => (object)$table,
            'topicPutTps' => $this->topicPutTps,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $t = new self();
        foreach (decode_message_queue_map($d['offsetTable'] ?? null) as ['mq' => $mq, 'value' => $v]) {
            $t->offsetTable[] = ['mq' => $mq, 'value' => TopicOffset::fromDict((array)$v)];
        }
        $t->topicPutTps = (float)($d['topicPutTps'] ?? 0.0);
        return $t;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

// ---------------------------------------------------------------- ConsumeStats

/** The broker / consumer offsets of a single queue plus the last update time. */
final class OffsetWrapper
{
    public function __construct(
        public int $brokerOffset = 0,
        public int $consumerOffset = 0,
        public int $lastTimestamp = 0,
        public int $pullOffset = 0,
    ) {
    }

    /** The lag: brokerOffset - consumerOffset. */
    public function lag(): int
    {
        return $this->brokerOffset - $this->consumerOffset;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'brokerOffset' => $this->brokerOffset,
            'consumerOffset' => $this->consumerOffset,
            'lastTimestamp' => $this->lastTimestamp,
            'pullOffset' => $this->pullOffset,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            (int)($d['brokerOffset'] ?? 0),
            (int)($d['consumerOffset'] ?? 0),
            (int)($d['lastTimestamp'] ?? 0),
            (int)($d['pullOffset'] ?? 0),
        );
    }
}

/**
 * The overall consumption statistics of a consumer group.
 *
 * Probe output: {"consumeTps":1.5,"offsetTable":{<MessageQueue>:<OffsetWrapper>}}
 */
final class ConsumeStats
{
    /** @var list<array{mq: MessageQueue, value: OffsetWrapper}> */
    public array $offsetTable = [];
    public float $consumeTps = 0.0;

    public function totalLag(): int
    {
        $sum = 0;
        foreach ($this->offsetTable as ['value' => $ow]) {
            $sum += $ow->lag();
        }
        return $sum;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $table = [];
        foreach ($this->offsetTable as ['mq' => $mq, 'value' => $v]) {
            $table[message_queue_key($mq)] = $v->toDict();
        }
        return [
            'offsetTable' => (object)$table,
            'consumeTps' => $this->consumeTps,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $cs = new self();
        foreach (decode_message_queue_map($d['offsetTable'] ?? null) as ['mq' => $mq, 'value' => $v]) {
            $cs->offsetTable[] = ['mq' => $mq, 'value' => OffsetWrapper::fromDict((array)$v)];
        }
        $cs->consumeTps = (float)($d['consumeTps'] ?? 0.0);
        return $cs;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

// ---------------------------------------------------------------- TopicConfigSerializeWrapper

/**
 * The full topic configuration table on the broker plus the data version.
 *
 * Probe output: {"dataVersion":{...},"topicConfigTable":{"Topic":{"attributes":{},...}}}
 */
final class TopicConfigSerializeWrapper
{
    /** @var array<string, TopicConfig> */
    public array $topicConfigTable = [];
    /** @var array<string, mixed> */
    public array $dataVersion = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'dataVersion' => (object)$this->dataVersion,
            'topicConfigTable' => (object)array_map(static fn(TopicConfig $v): array => $v->toDict(), $this->topicConfigTable),
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $w = new self();
        foreach ((array)($d['topicConfigTable'] ?? []) as $k => $v) {
            $w->topicConfigTable[(string)$k] = TopicConfig::fromDict((array)$v);
        }
        $w->dataVersion = (array)($d['dataVersion'] ?? []);
        return $w;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

// ---------------------------------------------------------------- QueryConsumeQueue

/**
 * The raw content of one ConsumeQueue entry (read out directly when troubleshooting).
 *
 * Probe / source fields: physicOffset, physicSize, tagsCode, extendDataJson, bitMap, eval, msg
 */
final class ConsumeQueueData
{
    public function __construct(
        public int $physicOffset = 0,
        public int $physicSize = 0,
        public int $tagsCode = 0,
        public ?string $extendDataJson = null,
        public ?string $bitMap = null,
        public bool $eval = false,
        public ?string $msg = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'physicOffset' => $this->physicOffset,
            'physicSize' => $this->physicSize,
            'tagsCode' => $this->tagsCode,
            'eval' => $this->eval,
            'bitMap' => $this->bitMap,
        ];
        // null fields are not serialized: writing a null key would change how the broker interprets the default
        if ($this->extendDataJson !== null) {
            $d['extendDataJson'] = $this->extendDataJson;
        }
        if ($this->msg !== null) {
            $d['msg'] = $this->msg;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            (int)($d['physicOffset'] ?? 0),
            (int)($d['physicSize'] ?? 0),
            (int)($d['tagsCode'] ?? 0),
            isset($d['extendDataJson']) ? (string)$d['extendDataJson'] : null,
            isset($d['bitMap']) ? (string)$d['bitMap'] : null,
            (bool)($d['eval'] ?? false),
            isset($d['msg']) ? (string)$d['msg'] : null,
        );
    }
}

// ---------------------------------------------------------------- Controller sync state

/**
 * One replica as the controller sees it.
 *
 * The boxed fields stay nullable end to end: the controller omits whatever it does not
 * know, and writing a zero in its place would be indistinguishable from a real id 0.
 */
final class ReplicaIdentity
{
    public function __construct(
        public ?string $brokerName = null,
        public ?int $brokerId = null,
        public ?string $brokerAddress = null,
        public ?bool $alive = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [];
        if ($this->brokerName !== null) {
            $d['brokerName'] = $this->brokerName;
        }
        if ($this->brokerId !== null) {
            $d['brokerId'] = $this->brokerId;
        }
        if ($this->brokerAddress !== null) {
            $d['brokerAddress'] = $this->brokerAddress;
        }
        if ($this->alive !== null) {
            $d['alive'] = $this->alive;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            isset($d['brokerName']) ? (string)$d['brokerName'] : null,
            isset($d['brokerId']) ? (int)$d['brokerId'] : null,
            isset($d['brokerAddress']) ? (string)$d['brokerAddress'] : null,
            isset($d['alive']) ? (bool)$d['alive'] : null,
        );
    }
}

/** The master view plus the in-sync set of one broker. */
final class ReplicasInfo
{
    public ?int $masterBrokerId = null;
    public ?string $masterAddress = null;
    public ?int $masterEpoch = null;
    public ?int $syncStateSetEpoch = null;
    /** @var list<ReplicaIdentity>|null */
    public ?array $inSyncReplicas = null;
    /** @var list<ReplicaIdentity>|null */
    public ?array $notInSyncReplicas = null;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [];
        if ($this->masterBrokerId !== null) {
            $d['masterBrokerId'] = $this->masterBrokerId;
        }
        if ($this->masterAddress !== null) {
            $d['masterAddress'] = $this->masterAddress;
        }
        if ($this->masterEpoch !== null) {
            $d['masterEpoch'] = $this->masterEpoch;
        }
        if ($this->syncStateSetEpoch !== null) {
            $d['syncStateSetEpoch'] = $this->syncStateSetEpoch;
        }
        if ($this->inSyncReplicas !== null) {
            $d['inSyncReplicas'] = array_map(static fn(ReplicaIdentity $r): array => $r->toDict(), $this->inSyncReplicas);
        }
        if ($this->notInSyncReplicas !== null) {
            $d['notInSyncReplicas'] = array_map(static fn(ReplicaIdentity $r): array => $r->toDict(), $this->notInSyncReplicas);
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $r = new self();
        $r->masterBrokerId = isset($d['masterBrokerId']) ? (int)$d['masterBrokerId'] : null;
        $r->masterAddress = isset($d['masterAddress']) ? (string)$d['masterAddress'] : null;
        $r->masterEpoch = isset($d['masterEpoch']) ? (int)$d['masterEpoch'] : null;
        $r->syncStateSetEpoch = isset($d['syncStateSetEpoch']) ? (int)$d['syncStateSetEpoch'] : null;
        $r->inSyncReplicas = isset($d['inSyncReplicas']) && is_array($d['inSyncReplicas'])
            ? array_values(array_map(static fn(array $x): ReplicaIdentity => ReplicaIdentity::fromDict($x), $d['inSyncReplicas']))
            : null;
        $r->notInSyncReplicas = isset($d['notInSyncReplicas']) && is_array($d['notInSyncReplicas'])
            ? array_values(array_map(static fn(array $x): ReplicaIdentity => ReplicaIdentity::fromDict($x), $d['notInSyncReplicas']))
            : null;
        return $r;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

/**
 * CONTROLLER_GET_SYNC_STATE_DATA(1006) response: the per-broker replicas state keyed by
 * broker name.
 */
final class BrokerReplicasInfo
{
    /** @var array<string, ReplicasInfo> */
    public array $replicasInfoTable = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'replicasInfoTable' => (object)array_map(static fn(ReplicasInfo $r): array => $r->toDict(), $this->replicasInfoTable),
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        foreach ((array)($d['replicasInfoTable'] ?? []) as $brokerName => $info) {
            $b->replicasInfoTable[(string)$brokerName] = is_array($info)
                ? ReplicasInfo::fromDict($info)
                : new ReplicasInfo();
        }
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

// ---------------------------------------------------------------- RocksDB cold check

/**
 * CHECK_ROCKSDB_CQ_WRITE_PROGRESS(354) result.
 *
 * checkStatus is the CheckStatus ordinal: 0 CHECK_OK, 1 CHECK_NOT_OK, 2 CHECK_IN_PROGRESS,
 * 3 CHECK_ERROR.
 */
final class CheckRocksdbCqWriteResult
{
    public ?string $checkResult = null;
    public int $checkStatus = 0;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = ['checkStatus' => $this->checkStatus];
        if ($this->checkResult !== null) {
            $d['checkResult'] = $this->checkResult;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $r = new self();
        $r->checkResult = isset($d['checkResult']) ? (string)$d['checkResult'] : null;
        $r->checkStatus = (int)($d['checkStatus'] ?? 0);
        return $r;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

// ---------------------------------------------------------------- Lite admin info

/** One lag entry of the lite group lag top-k lists. */
final class LiteLagInfo
{
    public function __construct(
        public ?string $liteTopic = null,
        public int $lagCount = 0,
        public int $earliestUnconsumedTimestamp = -1,
        public int $consumerOffset = -1,
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'lagCount' => $this->lagCount,
            'earliestUnconsumedTimestamp' => $this->earliestUnconsumedTimestamp,
            'consumerOffset' => $this->consumerOffset,
        ];
        if ($this->liteTopic !== null) {
            $d['liteTopic'] = $this->liteTopic;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            isset($d['liteTopic']) ? (string)$d['liteTopic'] : null,
            (int)($d['lagCount'] ?? 0),
            (int)($d['earliestUnconsumedTimestamp'] ?? -1),
            (int)($d['consumerOffset'] ?? -1),
        );
    }
}

/** GET_BROKER_LITE_INFO(200074) response: the broker's lite-store footprint. */
final class GetBrokerLiteInfoResponseBody
{
    public ?string $storeType = null;
    public int $maxLmqNum = 0;
    public int $currentLmqNum = 0;
    public int $liteSubscriptionCount = 0;
    public int $orderInfoCount = 0;
    public int $cqTableSize = 0;
    public int $offsetTableSize = 0;
    public int $eventMapSize = 0;
    /** @var array<string, int> */
    public array $topicMeta = [];
    /** @var array<string, list<string>> */
    public array $groupMeta = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'maxLmqNum' => $this->maxLmqNum,
            'currentLmqNum' => $this->currentLmqNum,
            'liteSubscriptionCount' => $this->liteSubscriptionCount,
            'orderInfoCount' => $this->orderInfoCount,
            'cqTableSize' => $this->cqTableSize,
            'offsetTableSize' => $this->offsetTableSize,
            'eventMapSize' => $this->eventMapSize,
            'topicMeta' => (object)$this->topicMeta,
            'groupMeta' => (object)array_map(static fn(array $v): array => array_values($v), $this->groupMeta),
        ];
        if ($this->storeType !== null) {
            $d['storeType'] = $this->storeType;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->storeType = isset($d['storeType']) ? (string)$d['storeType'] : null;
        $b->maxLmqNum = (int)($d['maxLmqNum'] ?? 0);
        $b->currentLmqNum = (int)($d['currentLmqNum'] ?? 0);
        $b->liteSubscriptionCount = (int)($d['liteSubscriptionCount'] ?? 0);
        $b->orderInfoCount = (int)($d['orderInfoCount'] ?? 0);
        $b->cqTableSize = (int)($d['cqTableSize'] ?? 0);
        $b->offsetTableSize = (int)($d['offsetTableSize'] ?? 0);
        $b->eventMapSize = (int)($d['eventMapSize'] ?? 0);
        foreach ((array)($d['topicMeta'] ?? []) as $k => $v) {
            $b->topicMeta[(string)$k] = (int)$v;
        }
        foreach ((array)($d['groupMeta'] ?? []) as $k => $v) {
            $b->groupMeta[(string)$k] = array_values(array_map(strval(...), (array)$v));
        }
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

/** GET_PARENT_TOPIC_INFO(200075) response. */
final class GetParentTopicInfoResponseBody
{
    public ?string $topic = null;
    public int $ttl = 0;
    /** @var list<string> */
    public array $groups = [];
    public int $lmqNum = 0;
    public int $liteTopicCount = 0;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'ttl' => $this->ttl,
            'groups' => array_values($this->groups),
            'lmqNum' => $this->lmqNum,
            'liteTopicCount' => $this->liteTopicCount,
        ];
        if ($this->topic !== null) {
            $d['topic'] = $this->topic;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->topic = isset($d['topic']) ? (string)$d['topic'] : null;
        $b->ttl = (int)($d['ttl'] ?? 0);
        $b->groups = array_values(array_map(strval(...), (array)($d['groups'] ?? [])));
        $b->lmqNum = (int)($d['lmqNum'] ?? 0);
        $b->liteTopicCount = (int)($d['liteTopicCount'] ?? 0);
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

/** GET_LITE_TOPIC_INFO(200076) response. */
final class GetLiteTopicInfoResponseBody
{
    public ?string $parentTopic = null;
    public ?string $liteTopic = null;
    /** @var list<array<string, mixed>> */
    public array $subscriber = [];
    public ?TopicOffset $topicOffset = null;
    public bool $shardingToBroker = false;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'subscriber' => $this->subscriber,
            'shardingToBroker' => $this->shardingToBroker,
        ];
        if ($this->parentTopic !== null) {
            $d['parentTopic'] = $this->parentTopic;
        }
        if ($this->liteTopic !== null) {
            $d['liteTopic'] = $this->liteTopic;
        }
        if ($this->topicOffset !== null) {
            $d['topicOffset'] = $this->topicOffset->toDict();
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->parentTopic = isset($d['parentTopic']) ? (string)$d['parentTopic'] : null;
        $b->liteTopic = isset($d['liteTopic']) ? (string)$d['liteTopic'] : null;
        $b->subscriber = array_values((array)($d['subscriber'] ?? []));
        $b->topicOffset = isset($d['topicOffset']) && is_array($d['topicOffset'])
            ? TopicOffset::fromDict($d['topicOffset'])
            : null;
        $b->shardingToBroker = (bool)($d['shardingToBroker'] ?? false);
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

/** GET_LITE_CLIENT_INFO(200077) response. */
final class GetLiteClientInfoResponseBody
{
    public ?string $parentTopic = null;
    public ?string $group = null;
    public ?string $clientId = null;
    public int $lastAccessTime = 0;
    public int $lastConsumeTime = 0;
    public int $liteTopicCount = 0;
    /** @var list<string> */
    public array $liteTopicSet = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'lastAccessTime' => $this->lastAccessTime,
            'lastConsumeTime' => $this->lastConsumeTime,
            'liteTopicCount' => $this->liteTopicCount,
            'liteTopicSet' => array_values($this->liteTopicSet),
        ];
        if ($this->parentTopic !== null) {
            $d['parentTopic'] = $this->parentTopic;
        }
        if ($this->group !== null) {
            $d['group'] = $this->group;
        }
        if ($this->clientId !== null) {
            $d['clientId'] = $this->clientId;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->parentTopic = isset($d['parentTopic']) ? (string)$d['parentTopic'] : null;
        $b->group = isset($d['group']) ? (string)$d['group'] : null;
        $b->clientId = isset($d['clientId']) ? (string)$d['clientId'] : null;
        $b->lastAccessTime = (int)($d['lastAccessTime'] ?? 0);
        $b->lastConsumeTime = (int)($d['lastConsumeTime'] ?? 0);
        $b->liteTopicCount = (int)($d['liteTopicCount'] ?? 0);
        $b->liteTopicSet = array_values(array_map(strval(...), (array)($d['liteTopicSet'] ?? [])));
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

/** GET_LITE_GROUP_INFO(200078) response. */
final class GetLiteGroupInfoResponseBody
{
    public ?string $group = null;
    public ?string $parentTopic = null;
    public ?string $liteTopic = null;
    public int $earliestUnconsumedTimestamp = -1;
    public int $totalLagCount = 0;
    public ?OffsetWrapper $liteTopicOffsetWrapper = null;
    /** @var list<LiteLagInfo>|null */
    public ?array $lagCountTopK = null;
    /** @var list<LiteLagInfo>|null */
    public ?array $lagTimestampTopK = null;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'earliestUnconsumedTimestamp' => $this->earliestUnconsumedTimestamp,
            'totalLagCount' => $this->totalLagCount,
        ];
        if ($this->group !== null) {
            $d['group'] = $this->group;
        }
        if ($this->parentTopic !== null) {
            $d['parentTopic'] = $this->parentTopic;
        }
        if ($this->liteTopic !== null) {
            $d['liteTopic'] = $this->liteTopic;
        }
        if ($this->liteTopicOffsetWrapper !== null) {
            $d['liteTopicOffsetWrapper'] = $this->liteTopicOffsetWrapper->toDict();
        }
        if ($this->lagCountTopK !== null) {
            $d['lagCountTopK'] = array_map(static fn(LiteLagInfo $l): array => $l->toDict(), $this->lagCountTopK);
        }
        if ($this->lagTimestampTopK !== null) {
            $d['lagTimestampTopK'] = array_map(static fn(LiteLagInfo $l): array => $l->toDict(), $this->lagTimestampTopK);
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->group = isset($d['group']) ? (string)$d['group'] : null;
        $b->parentTopic = isset($d['parentTopic']) ? (string)$d['parentTopic'] : null;
        $b->liteTopic = isset($d['liteTopic']) ? (string)$d['liteTopic'] : null;
        $b->earliestUnconsumedTimestamp = (int)($d['earliestUnconsumedTimestamp'] ?? -1);
        $b->totalLagCount = (int)($d['totalLagCount'] ?? 0);
        $b->liteTopicOffsetWrapper = isset($d['liteTopicOffsetWrapper']) && is_array($d['liteTopicOffsetWrapper'])
            ? OffsetWrapper::fromDict($d['liteTopicOffsetWrapper'])
            : null;
        $b->lagCountTopK = isset($d['lagCountTopK']) && is_array($d['lagCountTopK'])
            ? array_values(array_map(static fn(array $x): LiteLagInfo => LiteLagInfo::fromDict($x), $d['lagCountTopK']))
            : null;
        $b->lagTimestampTopK = isset($d['lagTimestampTopK']) && is_array($d['lagTimestampTopK'])
            ? array_values(array_map(static fn(array $x): LiteLagInfo => LiteLagInfo::fromDict($x), $d['lagTimestampTopK']))
            : null;
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

// ---------------------------------------------------------------- Batch operation results

/** Success / failure address lists of a broker fan-out operation. */
final class BrokerOperatorResult
{
    /** @var list<string> */
    public array $successList = [];
    /** @var list<string> */
    public array $failureList = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'successList' => array_values($this->successList),
            'failureList' => array_values($this->failureList),
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $r = new self();
        $r->successList = array_values(array_map(strval(...), (array)($d['successList'] ?? [])));
        $r->failureList = array_values(array_map(strval(...), (array)($d['failureList'] ?? [])));
        return $r;
    }
}

/** Result codes of the admin batch operations; the numbers are the protocol values. */
final class AdminToolsResultCodeEnum
{
    public const SUCCESS = 200;
    public const REMOTING_ERROR = -1001;
    public const MQ_BROKER_ERROR = -1002;
    public const MQ_CLIENT_ERROR = -1003;
    public const INTERRUPT_ERROR = -1004;
    public const TOPIC_ROUTE_INFO_NOT_EXIST = -2001;
    public const CONSUMER_NOT_ONLINE = -2002;
    public const BROADCAST_CONSUMPTION = -2003;
}

/**
 * The envelope every *Concurrent fan-out (except the message tracking one) returns:
 * ``success``/``code``/``errorMsg`` describe the batch, ``data`` carries the merged value.
 */
final class AdminToolResult
{
    /** @param mixed $data */
    public function __construct(
        public bool $success,
        public int $code,
        public ?string $errorMsg = null,
        public mixed $data = null,
    ) {
    }

    /** @param mixed $data */
    public static function success(mixed $data = null): self
    {
        return new self(true, AdminToolsResultCodeEnum::SUCCESS, 'success', $data);
    }

    /** @param mixed $data */
    public static function failure(int $code, ?string $errorMsg, mixed $data = null): self
    {
        return new self(false, $code, $errorMsg, $data);
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'success' => $this->success,
            'code' => $this->code,
        ];
        if ($this->errorMsg !== null) {
            $d['errorMsg'] = $this->errorMsg;
        }
        if ($this->data !== null) {
            $d['data'] = $this->data;
        }
        return $d;
    }
}

/**
 * The response content of a direct ConsumeQueue query.
 *
 * Probe output: {"filterData":"*","maxQueueIndex":88,"minQueueIndex":1,"subscriptionData":{...}}
 * (queueData does not appear when it is null)
 */
final class QueryConsumeQueueResponseBody
{
    /** @var array<string, mixed>|null */
    public ?array $subscriptionData = null;
    public ?string $filterData = null;
    /** @var list<ConsumeQueueData>|null */
    public ?array $queueData = null;
    public int $maxQueueIndex = 0;
    public int $minQueueIndex = 0;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'maxQueueIndex' => $this->maxQueueIndex,
            'minQueueIndex' => $this->minQueueIndex,
        ];
        if ($this->subscriptionData !== null) {
            $d['subscriptionData'] = (object)$this->subscriptionData;
        }
        if ($this->filterData !== null) {
            $d['filterData'] = $this->filterData;
        }
        if ($this->queueData !== null) {
            $d['queueData'] = array_map(static fn(ConsumeQueueData $q): array => $q->toDict(), $this->queueData);
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->subscriptionData = isset($d['subscriptionData']) && is_array($d['subscriptionData']) ? $d['subscriptionData'] : null;
        $b->filterData = isset($d['filterData']) ? (string)$d['filterData'] : null;
        $raw = $d['queueData'] ?? null;
        $b->queueData = is_array($raw) && $raw !== []
            ? array_map(static fn(array $x): ConsumeQueueData => ConsumeQueueData::fromDict($x), $raw)
            : null;
        $b->maxQueueIndex = (int)($d['maxQueueIndex'] ?? 0);
        $b->minQueueIndex = (int)($d['minQueueIndex'] ?? 0);
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}
