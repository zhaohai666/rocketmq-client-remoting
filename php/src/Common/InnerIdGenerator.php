<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 消息唯一 ID 生成器：UNIQ_KEY 的字节布局实现。
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
            // 本机 IP 非法时不能让 UNIQ_KEY 静默带上坏地址
            throw new \ValueError(sprintf('invalid local ip: %s', $ip));
        }
        $result .= $bin;
        $result .= pack('n', $pid & 0xFFFF);
        // 这一段是进程级盐，只要求同进程内恒定：协议侧它本来就是类加载
        // 哈希这类任意值，所以这里用确定性 crc32。
        $result .= pack('N', crc32('RocketMQClient') & 0xFFFFFFFF);
        $now = new \DateTimeImmutable('now');
        $monthMs = ((((int) $now->format('G')) * 60 + (int) $now->format('i')) * 60 + (int) $now->format('s')) * 1000
            + (int) $now->format('v');
        $result .= pack('N', $monthMs);
        $result .= pack('n', $counter);
        return UtilAll::bytes2String($result);
    }
}
