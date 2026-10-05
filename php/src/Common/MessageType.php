<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 消息类型（对应 org.apache.rocketmq.common.message.MessageType，移植自 message_type.py）。
 *
 * Java 侧 TraceBean 的 ``msgType`` 在编码轨迹时用的是 **ordinal()**（见
 * TraceDataEncoder 的 Pub/EndTransaction 分支），所以这里的整型值必须与 Java 的枚举
 * 声明顺序严格一致（PHP 的 backed value 即 Java ordinal）：Normal_Msg=0,
 * Trans_Msg_Half=1, Trans_msg_Commit=2, Delay_Msg=3, Order_Msg=4。
 */
enum MessageType: int
{
    case NORMAL_MSG = 0;
    case TRANS_MSG_HALF = 1;
    case TRANS_MSG_COMMIT = 2;
    case DELAY_MSG = 3;
    case ORDER_MSG = 4;

    public function shortName(): string
    {
        return match ($this) {
            self::NORMAL_MSG => 'Normal',
            self::TRANS_MSG_HALF => 'Trans',
            self::TRANS_MSG_COMMIT => 'TransCommit',
            self::DELAY_MSG => 'Delay',
            self::ORDER_MSG => 'Order',
        };
    }

    public static function getByShortName(string $shortName): self
    {
        return match ($shortName) {
            'Normal' => self::NORMAL_MSG,
            'Trans' => self::TRANS_MSG_HALF,
            'TransCommit' => self::TRANS_MSG_COMMIT,
            'Delay' => self::DELAY_MSG,
            'Order' => self::ORDER_MSG,
            default => self::NORMAL_MSG,
        };
    }
}
