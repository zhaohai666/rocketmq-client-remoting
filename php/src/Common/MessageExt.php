<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 对应 org.apache.rocketmq.common.message.MessageExt（拉取到的消息，
 * 移植自 message.py 的 MessageExt）。
 */
class MessageExt extends Message
{
    public int $queueId = 0;
    public int $storeSize = 0;
    public int $queueOffset = 0;
    public int $sysFlag = 0;
    public int $bornTimestamp = 0;
    public ?string $bornHost = null;
    public int $bornHostPort = 0;
    public int $storeTimestamp = 0;
    public ?string $storeHost = null;
    public int $storeHostPort = 0;
    public ?string $msgId = null;
    public int $commitLogOffset = 0;
    public int $bodyCrc = 0;
    public int $reconsumeTimes = 0;
    public int $preparedTransactionOffset = 0;
    public ?string $brokerName = null;
    public ?string $offsetMsgId = null;
    public ?string $msgType = null;

    public function getQueueId(): int
    {
        return $this->queueId;
    }

    public function setQueueId(int $queueId): void
    {
        $this->queueId = $queueId;
    }

    public function getStoreSize(): int
    {
        return $this->storeSize;
    }

    public function setStoreSize(int $size): void
    {
        $this->storeSize = $size;
    }

    public function getQueueOffset(): int
    {
        return $this->queueOffset;
    }

    public function setQueueOffset(int $queueOffset): void
    {
        $this->queueOffset = $queueOffset;
    }

    public function getSysFlag(): int
    {
        return $this->sysFlag;
    }

    public function setSysFlag(int $sysFlag): void
    {
        $this->sysFlag = $sysFlag;
    }

    public function getBornTimestamp(): int
    {
        return $this->bornTimestamp;
    }

    public function setBornTimestamp(int $bornTimestamp): void
    {
        $this->bornTimestamp = $bornTimestamp;
    }

    public function getBornHost(): ?string
    {
        return $this->bornHost;
    }

    public function setBornHost(?string $bornHost): void
    {
        $this->bornHost = $bornHost;
    }

    public function getStoreTimestamp(): int
    {
        return $this->storeTimestamp;
    }

    public function setStoreTimestamp(int $ts): void
    {
        $this->storeTimestamp = $ts;
    }

    public function getStoreHost(): ?string
    {
        return $this->storeHost;
    }

    public function setStoreHost(?string $storeHost): void
    {
        $this->storeHost = $storeHost;
    }

    public function getMsgId(): ?string
    {
        return $this->msgId;
    }

    public function setMsgId(?string $msgId): void
    {
        $this->msgId = $msgId;
    }

    public function getCommitLogOffset(): int
    {
        return $this->commitLogOffset;
    }

    public function setCommitLogOffset(int $offset): void
    {
        $this->commitLogOffset = $offset;
    }

    public function getBodyCrc(): int
    {
        return $this->bodyCrc;
    }

    public function setBodyCrc(int $crc): void
    {
        $this->bodyCrc = $crc;
    }

    public function getReconsumeTimes(): int
    {
        return $this->reconsumeTimes;
    }

    public function setReconsumeTimes(int $n): void
    {
        $this->reconsumeTimes = $n;
    }

    public function getPreparedTransactionOffset(): int
    {
        return $this->preparedTransactionOffset;
    }

    public function setPreparedTransactionOffset(int $offset): void
    {
        $this->preparedTransactionOffset = $offset;
    }

    public function setBrokerName(?string $brokerName): void
    {
        $this->brokerName = $brokerName;
    }

    public function getBrokerName(): ?string
    {
        return $this->brokerName;
    }

    public function getOffsetMsgId(): ?string
    {
        return $this->offsetMsgId;
    }

    public function setOffsetMsgId(?string $msgId): void
    {
        $this->offsetMsgId = $msgId;
    }

    public function getMsgType(): ?string
    {
        return $this->msgType;
    }

    public function setMsgType(?string $msgType): void
    {
        $this->msgType = $msgType;
    }

    public function getBornHostString(): ?string
    {
        if ($this->bornHost !== null && $this->bornHost !== '' && $this->bornHostPort !== 0) {
            return sprintf('%s:%d', $this->bornHost, $this->bornHostPort);
        }
        return $this->bornHost;
    }

    public function getStoreHostString(): ?string
    {
        if ($this->storeHost !== null && $this->storeHost !== '' && $this->storeHostPort !== 0) {
            return sprintf('%s:%d', $this->storeHost, $this->storeHostPort);
        }
        return $this->storeHost;
    }

    public function __toString(): string
    {
        return sprintf(
            "MessageExt(topic='%s', msgId='%s', queueOffset=%d, body=%d bytes)",
            $this->topic,
            $this->msgId,
            $this->queueOffset,
            strlen($this->body ?? '')
        );
    }
}
