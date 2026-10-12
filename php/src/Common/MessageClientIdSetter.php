<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 客户端消息唯一 ID（UNIQ_KEY）的生成与读写。
 *
 * 用途：
 *   * ``createUniqId()`` 生成 32 位十六进制唯一 ID（IP + PID + 类哈希 + 当月毫秒 + 自增）；
 *   * ``setUniqId($msg)``  发送前把 UNIQ_KEY 写到消息属性上：**非批量**消息在发送
 *     内核里写一次；批量消息既逐条写、再给批量整体写一个；
 *   * ``getUniqId($msg)``  取 UNIQ_KEY —— SendResult.msgId、消息轨迹的 msgId、
 *     事务消息的 transactionId 都用它。
 *
 * ⚠ 没有它的话 SendResult.msgId 只能退化成 broker 的 offsetMsgId（含 commitlog 偏移），
 * 那既不是按 key 查询用的 ID，轨迹里的 msgId 也和消费侧对不上。
 */
final class MessageClientIdSetter
{
    public static function createUniqId(): string
    {
        return InnerIdGenerator::createUniqId();
    }

    /**
     * UNIQ_KEY 缺失时才写入：已有值绝不覆盖，否则重试会换掉 msgId。
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
