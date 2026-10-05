<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\TopicConfig;

/**
 * 管理端专用响应体（对应 org.apache.rocketmq.remoting.protocol.admin.* 与 body.* 中的管理类，
 * 移植自 protocol/admin_body.py）：
 *
 * TopicStatsTable / TopicOffset / ConsumeStats / OffsetWrapper /
 * TopicConfigSerializeWrapper / ConsumeQueueData / QueryConsumeQueueResponseBody。
 *
 * ⚠ 关键坑：这几个类的 Map 键是 **MessageQueue**，fastjson2 会把键直接内联成 JSON 对象，
 * 产出**非法 JSON**，例如：
 *     {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:{...}}}
 * 所以必须用 RemotingSerializable::fastjsonLoads() 解析，再把字符串键还原成 MessageQueue。
 * 字段名与结构均由 Java 探针实测确认。
 *
 * PHP 无法用对象做数组键：以 MessageQueue 为键的 map 统一存成
 * ``list<array{mq: MessageQueue, value: mixed}>``，JSON 输出仍用 messageQueueKey() 内联对象键。
 */

/**
 * 把 fastjson2 写出的 MessageQueue 内联对象键还原成 MessageQueue。
 *
 * 返回 null 表示这个键不是内联对象（例如 ``"0"``、``"G1"`` 这类普通字符串键）。
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

/** 把 MessageQueue 序列化成 fastjson2 风格的内联对象键（键按字母序）。 */
function message_queue_key(MessageQueue $mq): string
{
    return sprintf('{"brokerName":"%s","queueId":%d,"topic":"%s"}', $mq->brokerName, $mq->queueId, $mq->topic);
}

/**
 * 通用：解析以 MessageQueue 为键的 map（fastjson2 非字符串键）。
 * 只有能还原成 MessageQueue 的键才收进来。
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

/** 对应 org.apache.rocketmq.remoting.protocol.admin.TopicOffset。 */
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
 * 对应 org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable。
 *
 * 探针输出：{"offsetTable":{<MessageQueue>:{...}},"topicPutTps":0.0}
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
            // 内联对象键不是合法 JSON，必须走 fastjson 兼容路径
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

/** 对应 org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper。 */
final class OffsetWrapper
{
    public function __construct(
        public int $brokerOffset = 0,
        public int $consumerOffset = 0,
        public int $lastTimestamp = 0,
        public int $pullOffset = 0,
    ) {
    }

    /** 与 Java OffsetWrapper.getLag() 一致：brokerOffset - consumerOffset。 */
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
 * 对应 org.apache.rocketmq.remoting.protocol.admin.ConsumeStats。
 *
 * 探针输出：{"consumeTps":1.5,"offsetTable":{<MessageQueue>:<OffsetWrapper>}}
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
 * 对应 org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper。
 *
 * 探针输出：{"dataVersion":{...},"topicConfigTable":{"Topic":{"attributes":{},...}}}
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
 * 对应 org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData。
 *
 * 探针/源码字段：physicOffset, physicSize, tagsCode, extendDataJson, bitMap, eval, msg
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
        // 同 Java：null 字段不序列化
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
 * 对应 org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody。
 *
 * 探针输出：{"filterData":"*","maxQueueIndex":88,"minQueueIndex":1,"subscriptionData":{...}}
 * （queueData 为 null 时不出现）
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
