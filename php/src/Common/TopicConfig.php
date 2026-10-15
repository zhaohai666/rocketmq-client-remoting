<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * TopicConfig: the config payload reported to the broker when creating/updating a topic.
 *
 * Field names and default values follow the broker 5.x's actual parsing (confirmed by testing; do not change from memory):
 * ``new TopicConfig("t")`` -> readQueueNums=16, writeQueueNums=16, perm=6,
 * topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}.
 * ``attributes`` **is serialized** — in the protocol it is a normal field, not an internal one.
 */
final class TopicConfig
{
    /** Default read queue count on the broker side */
    public const DEFAULT_READ_QUEUE_NUMS = 16;
    /** Default write queue count on the broker side */
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
            // In the protocol attributes is a Map (default {}): a PHP empty array encodes to [],
            // and the broker parsing it as a Map would report "expect '{', but '['" — an empty table must output an object.
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
