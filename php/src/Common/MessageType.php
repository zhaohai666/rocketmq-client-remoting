<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Message type.
 *
 * The msgType in trace reporting uses the **enum ordinal** (the broker side decodes the trace by ordinal to restore the
 * Pub / EndTransaction branches), so the integer values must strictly match the declaration order in the protocol:
 * Normal_Msg=0, Trans_Msg_Half=1, Trans_msg_Commit=2, Delay_Msg=3, Order_Msg=4.
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
