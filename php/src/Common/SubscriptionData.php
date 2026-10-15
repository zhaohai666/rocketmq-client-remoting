<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Subscription data: the topic / subString / tagsSet / codeSet / expression type parsed from one
 * subscription; both the heartbeat and the pull report the broker using this structure.
 */
class SubscriptionData
{
    public bool $classFilterMode = false;

    public ?string $topic;

    public ?string $subString;

    /** @var list<string> the set of distinct tags (the array is used as a set, addTag handles dedup) */
    public array $tagsSet = [];

    /** @var list<int> the set of distinct tag hashes, in one-to-one correspondence with tagsSet */
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

    /** Set semantics: add a tag with dedup. */
    public function addTag(string $tag): void
    {
        if (!in_array($tag, $this->tagsSet, true)) {
            $this->tagsSet[] = $tag;
        }
    }

    /** Set semantics: add a tag hash with dedup. */
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
     * Emits the field table used for reporting: the keys use the protocol field names (camelCase),
     * tagsSet/codeSet have to appear as JSON arrays.
     *
     * Note that filterClassSource **does not take part in serialization** — in the protocol it is an
     * internal client-side field, and including it would bloat the whole heartbeat packet.
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
