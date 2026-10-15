<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Unique message ID generator: the byte layout implementation of UNIQ_KEY.
 */
final class InnerIdGenerator
{
    private static int $counter = 0;

    /**
     * Generates a 32-character hex unique ID: IP(4|16B) + PID(2B) + class-loading hash(4B) + millis within
     * the current hour(4B) + auto-increment(2B).
     */
    public static function createUniqId(): string
    {
        $pid = UtilAll::getPid();
        $ip = MixAll::getIpStr();
        self::$counter = (self::$counter + 1) & 0xFFFF;
        $counter = self::$counter;

        $result = '';
        $bin = @inet_pton($ip);
        if ($bin === false) {
            // When the local IP is illegal, UNIQ_KEY must not silently carry a bad address
            throw new \ValueError(sprintf('invalid local ip: %s', $ip));
        }
        $result .= $bin;
        $result .= pack('n', $pid & 0xFFFF);
        // This segment is a process-level salt that only has to be constant within the same process: on
        // the protocol side it is an arbitrary value such as a class-loading hash anyway, so a
        // deterministic crc32 is used here.
        $result .= pack('N', crc32('RocketMQClient') & 0xFFFFFFFF);
        $now = new \DateTimeImmutable('now');
        $monthMs = ((((int) $now->format('G')) * 60 + (int) $now->format('i')) * 60 + (int) $now->format('s')) * 1000
            + (int) $now->format('v');
        $result .= pack('N', $monthMs);
        $result .= pack('n', $counter);
        return UtilAll::bytes2String($result);
    }
}
