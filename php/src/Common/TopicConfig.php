<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * TopicConfig：创建/更新 topic 时上报给 broker 的配置载体。
 *
 * 字段与默认值以 broker 5.x 的实际解析口径为准（实测确认，勿凭记忆改）：
 * ``new TopicConfig("t")`` → readQueueNums=16, writeQueueNums=16, perm=6,
 * topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}。
 * ``attributes`` **会被序列化**——协议里它是普通字段，不是内部字段。
 */
final class TopicConfig
{
    /** broker 侧默认读队列数 */
    public const DEFAULT_READ_QUEUE_NUMS = 16;
    /** broker 侧默认写队列数 */
    public const DEFAULT_WRITE_QUEUE_NUMS = 16;
    /** PermName.PERM_READ | PermName.PERM_WRITE */
    public const DEFAULT_PERM = 6;

    public function __construct(
        public string $topicName = '',
        public int $readQueueNums = self::DEFAULT_READ_QUEUE_NUMS,
        public int $writeQueueNums = self::DEFAULT_WRITE_QUEUE_NUMS,
        public int $perm = self::DEFAULT_PERM,
        public string $topicFilterType = TopicFilterType::SINGLE_TAG,
        public int $topicSysFlag = 0,
        public bool $order = false,
        /** @var array<string, string> */
        public array $attributes = [],
    ) {
    }

    public function encode(): string
    {
        return json_encode($this->toDict(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'topicName' => $this->topicName,
            'readQueueNums' => $this->readQueueNums,
            'writeQueueNums' => $this->writeQueueNums,
            'perm' => $this->perm,
            'topicFilterType' => $this->topicFilterType,
            'topicSysFlag' => $this->topicSysFlag,
            'order' => $this->order,
            // 协议里 attributes 是 Map（默认 {}）：PHP 空 array 会编成 []，
            // broker 按 Map 解析时报 "expect '{', but '['" —— 空表必须输出对象。
            'attributes' => $this->attributes === [] ? new \stdClass() : $this->attributes,
        ];
    }

    /** @param array<string, mixed> $data */
    public static function fromDict(array $data): self
    {
        return new self(
            topicName: (string) ($data['topicName'] ?? ''),
            readQueueNums: (int) ($data['readQueueNums'] ?? self::DEFAULT_READ_QUEUE_NUMS),
            writeQueueNums: (int) ($data['writeQueueNums'] ?? self::DEFAULT_WRITE_QUEUE_NUMS),
            perm: (int) ($data['perm'] ?? self::DEFAULT_PERM),
            topicFilterType: (string) ($data['topicFilterType'] ?? TopicFilterType::SINGLE_TAG),
            topicSysFlag: (int) ($data['topicSysFlag'] ?? 0),
            order: (bool) ($data['order'] ?? false),
            attributes: (array) ($data['attributes'] ?? []),
        );
    }

    public static function decode(string $jsonStr): self
    {
        return self::fromDict(
            json_decode($jsonStr, true, 512, JSON_THROW_ON_ERROR)
        );
    }

    public function __toString(): string
    {
        return sprintf(
            'TopicConfig[topicName=%s, readQueueNums=%d, writeQueueNums=%d, perm=%s]',
            $this->topicName,
            $this->readQueueNums,
            $this->writeQueueNums,
            PermName::permToString($this->perm)
        );
    }
}
