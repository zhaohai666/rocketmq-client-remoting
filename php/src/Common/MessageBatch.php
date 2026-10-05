<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 批量消息（对应 org.apache.rocketmq.common.message.MessageBatch，移植自 message.py）。
 *
 * 没有自己的序列化字段，body 由 ``encode()`` 生成：
 * ``MessageDecoder::encodeMessages($messages)`` 的拼接结果（每条为 6 段轻量格式）。
 */
final class MessageBatch extends Message
{
    /** @var Message[] */
    public array $messages = [];

    /** @param list<Message> $messages */
    public function __construct(array $messages = [])
    {
        parent::__construct();
        $this->messages = array_values($messages);
    }

    /** 对应 Java MessageBatch.encode()。 */
    public function encode(): string
    {
        return MessageDecoder::encodeMessages($this->messages);
    }

    /** @return list<Message> */
    public function getMessages(): array
    {
        return $this->messages;
    }

    /**
     * 对应 Java MessageBatch.generateFromList。
     *
     * 约束：非空；同一 topic；同一 waitStoreMsgOK；不允许延时消息；不允许重试 topic。
     * Java 抛 UnsupportedOperationException / IllegalArgumentException，
     * 这里统一映射为 PHP 的 InvalidArgumentException（对应 Python 的 ValueError）。
     *
     * @param list<Message> $messages
     */
    public static function generateFromList(array $messages): self
    {
        if ($messages === []) {
            throw new \InvalidArgumentException('messages must not be null or empty');
        }

        $messageList = [];
        $first = null;
        foreach ($messages as $message) {
            $delayLevel = $message->getDelayTimeLevel();
            if ($delayLevel !== null && (int) $delayLevel > 0) {
                throw new \InvalidArgumentException('Delayed messages are not supported for batching');
            }
            if (str_starts_with($message->getTopic(), MixAll::RETRY_GROUP_TOPIC_PREFIX)) {
                throw new \InvalidArgumentException('Retry Group is not supported for batching');
            }
            if ($first === null) {
                $first = $message;
            } else {
                if ($first->getTopic() !== $message->getTopic()) {
                    throw new \InvalidArgumentException('The topic of the messages in one batch should be the same');
                }
                if ($first->getWaitStoreMsgOk() !== $message->getWaitStoreMsgOk()) {
                    throw new \InvalidArgumentException('The waitStoreMsgOK of the messages in one batch should be the same');
                }
            }
            $messageList[] = $message;
        }

        $batch = new self($messageList);
        $batch->setTopic($first->getTopic());
        // Java generateFromList:70 是 ``batch.setWaitStoreMsgOK(first.isWaitStoreMsgOK())``：
        // **属性缺省即 true**。曾经写成 ``first.get_wait_store_msg_ok() == "true"`` ——
        // 缺省（None）被判成 False，于是普通消息（``Message.__init__`` 不写 WAIT）组成的
        // 批量会以 ``WAIT=false`` 下发：broker 不等刷盘就回 SEND_OK，持久性静默降级。
        $batch->setWaitStoreMsgOk(self::isWaitStoreMsgOk($first));
        $batch->setBody($batch->encode());
        return $batch;
    }
}
