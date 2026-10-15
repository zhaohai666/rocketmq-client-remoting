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
