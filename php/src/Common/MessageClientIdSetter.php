<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 客户端消息唯一 ID（对应 org.apache.rocketmq.common.message.MessageClientIDSetter，
 * 移植自 message_client_id_setter.py）。
 *
 * 用途：
 *   * ``createUniqId()`` 生成 32 位十六进制唯一 ID（IP + PID + 类哈希 + 当月毫秒 + 自增）；
 *   * ``setUniqId($msg)``  发送前把 UNIQ_KEY 写到消息属性上（Java 在
 *     ``DefaultMQProducerImpl.sendKernelImpl`` 里对**非批量**消息调用；批量消息在
 *     ``DefaultMQProducer.batch():1176/1179`` 里逐条写、再给批量自身写一个）；
 *   * ``getUniqId($msg)``  取 UNIQ_KEY —— SendResult.msgId、消息轨迹的 msgId、
 *     事务消息的 transactionId 都用它。
 *
 * ⚠ 没有它的话 SendResult.msgId 只能退化成 broker 的 offsetMsgId（含 commitlog 偏移），
 * 与 Java 的语义不同，且轨迹里的 msgId 与消费侧对不上。
 */
final class MessageClientIdSetter
{
    public static function createUniqId(): string
    {
        return InnerIdGenerator::createUniqId();
    }

    /**
     * UNIQ_KEY 缺失时才写入（Java MessageClientIDSetter.setUniqID）。
     */
    public static function setUniqId(?Message $msg): void
    {
        if ($msg === null) {
            return;
        }
        if ($msg->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) === null) {
            MessageAccessor::putProperty(
                $msg,
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                self::createUniqId()
            );
        }
    }

    public static function getUniqId(?Message $msg): ?string
    {
        if ($msg === null) {
            return null;
        }
        return $msg->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }
}
