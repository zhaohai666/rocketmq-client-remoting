<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * User-side message model: topic / body / ordered property table / flag / delay level.
 */
class Message
{
    /** @var array<string, string> Ordered property table */
    public array $properties = [];

    public ?string $transactionId = null;

    public function __construct(
        public string $topic = '',
        public ?string $body = '',
        ?string $tags = null,
        ?string $keys = null,
        public int $flag = 0,
    ) {
        if ($this->body === null) {
            $this->body = '';
        }
        if ($tags !== null && $tags !== '') {
            $this->properties['TAGS'] = $tags;
        }
        if ($keys !== null && $keys !== '') {
            $this->properties['KEYS'] = $keys;
        }
    }

    // ---- Properties ----

    public function setTags(string $tags): void
    {
        $this->properties['TAGS'] = $tags;
    }

    public function getTags(): ?string
    {
        return $this->properties['TAGS'] ?? null;
    }

    public function setKeys(string $keys): void
    {
        $this->properties['KEYS'] = $keys;
    }

    public function getKeys(): ?string
    {
        return $this->properties['KEYS'] ?? null;
    }

    public function setDelayTimeLevel(int $level): void
    {
        $this->properties['DELAY'] = (string) $level;
    }

    public function getDelayTimeLevel(): ?string
    {
        return $this->properties['DELAY'] ?? null;
    }

    public function setWaitStoreMsgOk(bool $ok): void
    {
        $this->properties['WAIT'] = $ok ? 'true' : 'false';
    }

    public function getWaitStoreMsgOk(): ?string
    {
        return $this->properties['WAIT'] ?? null;
    }

    public function setUserProperty(string $name, string $value): void
    {
        $this->properties[$name] = $value;
    }

    public function getUserProperty(string $name): ?string
    {
        return $this->properties[$name] ?? null;
    }

    public function putProperty(string $name, string $value): void
    {
        $this->properties[$name] = $value;
    }

    public function removeProperty(string $name): void
    {
        unset($this->properties[$name]);
    }

    public function getProperty(string $name): ?string
    {
        return $this->properties[$name] ?? null;
    }

    public function clearProperty(): void
    {
        $this->properties = [];
    }

    // ---- getter / setter (property names are protocol field names) ----

    public function getTopic(): string
    {
        return $this->topic;
    }

    public function setTopic(string $topic): void
    {
        $this->topic = $topic;
    }

    public function getBody(): ?string
    {
        return $this->body;
    }

    public function setBody(?string $body): void
    {
        $this->body = $body;
    }

    public function getFlag(): int
    {
        return $this->flag;
    }

    public function setFlag(int $flag): void
    {
        $this->flag = $flag;
    }

    /** @return array<string, string> */
    public function getProperties(): array
    {
        return $this->properties;
    }

    /** @param array<string, string> $properties */
    public function setProperties(array $properties): void
    {
        $this->properties = $properties;
    }

    public function getTransactionId(): ?string
    {
        return $this->transactionId;
    }

    public function setTransactionId(?string $transactionId): void
    {
        $this->transactionId = $transactionId;
    }

    /**
     * The WAIT property defaults to true; any other value is truthy only if it is the case-insensitive ``"true"``
     * (the broker side uses this criterion to decide whether to wait for disk flush before responding).
     *
     * WARNING: Do not write ``getWaitStoreMsgOk() === "true"``. This class's constructor does **not** pre-write ``WAIT``,
     * so "property absent" is the **normal case, not an edge case**: judging with ``=== "true"`` would turn ordinary
     * messages into ``WAIT=false``, and ``MessageBatch`` would then be sent with ``WAIT=false`` ——
     * the broker would respond ``SEND_OK`` without waiting for disk flush, silently degrading durability.
     */
    public static function isWaitStoreMsgOk(Message $msg): bool
    {
        $value = $msg->getWaitStoreMsgOk();
        if ($value === null) {
            return true;
        }
        return strtolower($value) === 'true';
    }
}
