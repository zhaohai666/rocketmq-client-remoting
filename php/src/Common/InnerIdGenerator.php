<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 消息唯一 ID 生成器（对应用户端 MessageClientIDSetter，移植自 util_all.py 的 InnerIdGenerator）。
 */
final class InnerIdGenerator
{
    private static int $counter = 0;

    /**
     * 生成 32 位十六进制唯一 ID：IP(4|16B) + PID(2B) + 类加载hash(4B) + 当月毫秒(4B) + 自增(2B)。
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
            // Python inet_pton 对非法地址抛 OSError，这里同样不让它静默通过
            throw new \ValueError(sprintf('invalid local ip: %s', $ip));
        }
        $result .= $bin;
        $result .= pack('n', $pid & 0xFFFF);
        // Python 用 abs(hash("RocketMQClient"))——进程级随机盐、同进程内恒定；
        // PHP 无等价物，改用确定性 crc32（该字段只需进程内稳定，Java 侧本来就是
        // classloader hashCode 这类任意值）。
        $result .= pack('N', crc32('RocketMQClient') & 0xFFFFFFFF);
        $now = new \DateTimeImmutable('now');
        $monthMs = ((((int) $now->format('G')) * 60 + (int) $now->format('i')) * 60 + (int) $now->format('s')) * 1000
            + (int) $now->format('v');
        $result .= pack('N', $monthMs);
        $result .= pack('n', $counter);
        return UtilAll::bytes2String($result);
    }
}
