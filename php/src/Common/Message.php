<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 用户侧消息模型：topic / body / 有序属性表 / flag / 延迟等级。
 */
class Message
{
    /** @var array<string, string> 有序属性表 */
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

    // ---- 属性 ----

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

    // ---- getter / setter（属性名即协议字段名）----

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
     * WAIT 属性缺省即 true，其余值只有忽略大小写的 ``"true"`` 算真
     * （broker 侧就按这个口径决定是否等刷盘再回包）。
     *
     * ⚠ 别写成 ``getWaitStoreMsgOk() === "true"``。本类构造器**不**预写 ``WAIT``，
     * 所以"属性缺省"是**常态而不是边角**：按 ``=== "true"`` 判会让普通消息变成
     * ``WAIT=false``，``MessageBatch`` 就会以 ``WAIT=false`` 下发 ——
     * broker 不等刷盘就回 ``SEND_OK``，持久性静默降级。
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
