<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * TopicConfig（对应 org.apache.rocketmq.common.TopicConfig，移植自 topic_config.py）。
 *
 * 字段与默认值以 Java 5.x 为准（探针实测，勿凭记忆改）：
 * ``new TopicConfig("t")`` → readQueueNums=16, writeQueueNums=16, perm=6,
 * topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}。
 * ``attributes`` **会被序列化**（Java 的 ``getAttributes()`` 没有 serialize=false）。
 */
final class TopicConfig
{
    /** org.apache.rocketmq.common.TopicConfig.defaultReadQueueNums */
    public const DEFAULT_READ_QUEUE_NUMS = 16;
    /** org.apache.rocketmq.common.TopicConfig.defaultWriteQueueNums */
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
            'attributes' => $this->attributes,
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
