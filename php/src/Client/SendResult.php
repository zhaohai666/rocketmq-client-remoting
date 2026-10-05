<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageQueue;

/**
 * 发送状态（对应 org.apache.rocketmq.client.producer.SendStatus，移植自 send_result.py）。
 *
 * 整型值与 Java 枚举声明顺序一致，broker 在 SEND 响应体里按该 ordinal 回传。
 */
enum SendStatus: int
{
    case SEND_OK = 0;
    case FLUSH_DISK_TIMEOUT = 1;
    case FLUSH_SLAVE_TIMEOUT = 2;
    case SLAVE_NOT_AVAILABLE = 3;

    /** 对应 ``SendStatus.from_code``：未知码兜底为 SEND_OK（Java 同此）。 */
    public static function fromCode(int $code): self
    {
        return self::tryFrom($code) ?? self::SEND_OK;
    }
}

/**
 * 发送结果（对应 org.apache.rocketmq.client.producer.SendResult）。
 *
 * 属性 public 且 camelCase；getter/setter 与 Java/Python 同名逐一对应。
 */
final class SendResult
{
    /**
     * 轨迹开关：来自 SEND 响应头的 TRACE_ON（broker 默认 true）。
     * Java 的判据是「extFields.get("TRACE_ON") != "false"」，所以默认 true。
     */
    public bool $traceOn = true;

    /**
     * 对应 Java SendResult.recallHandle：只有定时/延迟消息才非 null，
     * 是 broker 发的撤回句柄，原样传给 producer.recallMessage() 才能撤回。
     */
    public ?string $recallHandle = null;

    public function __construct(
        public SendStatus $sendStatus = SendStatus::SEND_OK,
        public ?string $msgId = null,
        public ?MessageQueue $messageQueue = null,
        public int $queueOffset = 0,
        public ?string $transactionId = null,
        public ?string $offsetMsgId = null,
        public ?string $regionId = null,
    ) {
    }

    public function getRecallHandle(): ?string
    {
        return $this->recallHandle;
    }

    public function setRecallHandle(?string $recallHandle): void
    {
        $this->recallHandle = $recallHandle;
    }

    public function isTraceOn(): bool
    {
        return $this->traceOn;
    }

    public function setTraceOn(bool $traceOn): void
    {
        $this->traceOn = $traceOn;
    }

    public function getRegionId(): ?string
    {
        return $this->regionId;
    }

    public function setRegionId(?string $regionId): void
    {
        $this->regionId = $regionId;
    }

    public function getSendStatus(): SendStatus
    {
        return $this->sendStatus;
    }

    public function getMsgId(): ?string
    {
        return $this->msgId;
    }

    public function getMessageQueue(): ?MessageQueue
    {
        return $this->messageQueue;
    }

    public function getQueueOffset(): int
    {
        return $this->queueOffset;
    }

    public function getTransactionId(): ?string
    {
        return $this->transactionId;
    }

    public function setTransactionId(?string $transactionId): void
    {
        $this->transactionId = $transactionId;
    }

    public function getOffsetMsgId(): ?string
    {
        return $this->offsetMsgId;
    }

    public function __toString(): string
    {
        return sprintf(
            'SendResult [sendStatus=%s, msgId=%s, offsetMsgId=%s, messageQueue=%s, queueOffset=%d, transactionId=%s]',
            'SendStatus.' . $this->sendStatus->name,
            $this->msgId,
            $this->offsetMsgId,
            $this->messageQueue !== null ? (string) $this->messageQueue : 'null',
            $this->queueOffset,
            $this->transactionId,
        );
    }
}
