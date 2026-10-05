<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * SubscriptionData（对应 org.apache.rocketmq.common.filter.SubscriptionData，
 * 移植自 subscription_data.py）。
 */
class SubscriptionData
{
    public bool $classFilterMode = false;

    public ?string $topic;

    public ?string $subString;

    /** @var list<string> 唯一 tag 集合（对应 Python set） */
    public array $tagsSet = [];

    /** @var list<int> 唯一 tag hash 集合（对应 Python set） */
    public array $codeSet = [];

    public int $subVersion;

    public string $expressionType = ExpressionType::TAG;

    public ?string $filterClassSource = null;

    public function __construct(?string $topic = null, ?string $subString = null)
    {
        $this->topic = $topic;
        $this->subString = $subString;
        $this->subVersion = UtilAll::currentTimeMillis();
    }

    public function getTopic(): ?string
    {
        return $this->topic;
    }

    public function setTopic(string $topic): void
    {
        $this->topic = $topic;
    }

    public function getSubString(): ?string
    {
        return $this->subString;
    }

    public function setSubString(?string $subString): void
    {
        $this->subString = $subString;
    }

    /** @return list<string> */
    public function getTagsSet(): array
    {
        return $this->tagsSet;
    }

    /** @param list<string> $tagsSet */
    public function setTagsSet(array $tagsSet): void
    {
        $this->tagsSet = $tagsSet;
    }

    /** @return list<int> */
    public function getCodeSet(): array
    {
        return $this->codeSet;
    }

    public function getSubVersion(): int
    {
        return $this->subVersion;
    }

    public function setSubVersion(int $subVersion): void
    {
        $this->subVersion = $subVersion;
    }

    public function getExpressionType(): string
    {
        return $this->expressionType;
    }

    public function setExpressionType(string $expressionType): void
    {
        $this->expressionType = $expressionType;
    }

    public function getFilterClassSource(): ?string
    {
        return $this->filterClassSource;
    }

    public function setFilterClassSource(?string $source): void
    {
        $this->filterClassSource = $source;
    }

    public function isClassFilterMode(): bool
    {
        return $this->classFilterMode;
    }

    public function setClassFilterMode(bool $mode): void
    {
        $this->classFilterMode = $mode;
    }

    /** 集合语义：去重添加 tag（对应 Python set.add）。 */
    public function addTag(string $tag): void
    {
        if (!in_array($tag, $this->tagsSet, true)) {
            $this->tagsSet[] = $tag;
        }
    }

    /** 集合语义：去重添加 tag hash（对应 Python set.add）。 */
    public function addCode(int $code): void
    {
        if (!in_array($code, $this->codeSet, true)) {
            $this->codeSet[] = $code;
        }
    }

    public function equals(self $other): bool
    {
        if ($this->classFilterMode !== $other->classFilterMode) {
            return false;
        }
        if ($this->topic !== $other->topic) {
            return false;
        }
        if ($this->subString !== $other->subString) {
            return false;
        }
        if ($this->expressionType !== $other->expressionType) {
            return false;
        }
        $a = $this->tagsSet;
        $b = $other->tagsSet;
        sort($a);
        sort($b);
        if ($a !== $b) {
            return false;
        }
        $a = $this->codeSet;
        $b = $other->codeSet;
        sort($a);
        sort($b);
        return $a === $b;
    }

    /**
     * Java 字段名（camelCase），tagsSet/codeSet 转 list 才能 JSON 序列化。
     *
     * 注意 filterClassSource 在 Java 里是 @JSONField(serialize=false) —— **不序列化**。
     *
     * @return array<string, mixed>
     */
    public function toDict(): array
    {
        $tags = $this->tagsSet;
        sort($tags);
        $codes = $this->codeSet;
        sort($codes);
        return [
            'classFilterMode' => $this->classFilterMode,
            'topic' => $this->topic,
            'subString' => $this->subString,
            'tagsSet' => $tags,
            'codeSet' => $codes,
            'subVersion' => $this->subVersion,
            'expressionType' => $this->expressionType,
        ];
    }

    public function __toString(): string
    {
        return sprintf(
            'SubscriptionData [topic=%s, subString=%s, tagsSet=%s]',
            $this->topic,
            $this->subString,
            json_encode($this->tagsSet)
        );
    }
}
