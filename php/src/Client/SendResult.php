<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageQueue;

enum SendStatus: int
{
    case SEND_OK = 0;
    case FLUSH_DISK_TIMEOUT = 1;
    case FLUSH_SLAVE_TIMEOUT = 2;
    case SLAVE_NOT_AVAILABLE = 3;

    public static function fromCode(int $code): self
    {
        return self::tryFrom($code) ?? self::SEND_OK;
    }
}

final class SendResult
{
    public bool $traceOn = true;

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
