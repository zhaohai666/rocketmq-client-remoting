<?php

declare(strict_types=1);

namespace RocketMQ\Common;

use RocketMQ\Client\Exceptions\MQClientException;

/**
 * 定时消息的撤回句柄：broker 发回来的凭据，撤回时原样带回。
 *
 * 句柄不是客户端自己造的：发送带 ``TIMER_DELIVER_MS`` / ``TIMER_DELAY_MS`` / ``TIMER_DELAY_SEC``
 * 的定时消息时，broker 在 ``SendMessageProcessor#attachRecallHandle`` 里把这个句柄挂到
 * SEND 响应头的 ``recallHandle`` 字段上返回，客户端只负责原样带回去调用 ``recallMessage``。
 * 因此普通消息的响应里根本没有这个字段。
 *
 * 编码格式由服务端约定锁死：``base64url("v1 <topic> <brokerName> <timestampStr> <messageId>")``，
 * 5 段、空格分隔。
 *
 * 两处刻意的处理：
 *   * 编码带 ``=`` 填充（服务端回包里的句柄就是带填充的形式）；解码则带/不带填充
 *     都吃——只发无填充句柄的写方留下的消息也要能撤回。
 *   * 解码失败直接抛 ``MQClientException``，文案固定为
 *     ``"recall handle is invalid"``：调用方按这条文案匹配错误，不能改。
 */
final class RecallMessageHandle
{
    public const SEPARATOR = ' ';
    public const VERSION_1 = 'v1';
    public const INVALID_HANDLE = 'recall handle is invalid';

    /**
     * 拼句柄：5 段空格连接后 base64url 编码，**带** ``=`` 填充。
     */
    public static function buildHandle(string $topic, string $brokerName, string $timestampStr, string $messageId): string
    {
        $raw = implode(self::SEPARATOR, [self::VERSION_1, $topic, $brokerName, $timestampStr, $messageId]);
        return self::b64UrlEncode($raw);
    }

    /**
     * 解句柄：任何非法输入都抛 ``MQClientException``，见类注释的容忍度说明。
     */
    public static function decodeHandle(string $handle): HandleV1
    {
        self::ensureExceptions();
        if ($handle === '') {
            throw new MQClientException(self::INVALID_HANDLE);
        }
        $raw = self::b64UrlDecode($handle);
        if ($raw === null) {
            throw new MQClientException(self::INVALID_HANDLE);
        }
        if (!preg_match('//u', $raw)) {
            // 非法 UTF-8 序列同样给固定文案
            throw new MQClientException(self::INVALID_HANDLE);
        }
        $items = explode(self::SEPARATOR, $raw);
        if (count($items) < 5 || $items[0] !== self::VERSION_1) {
            throw new MQClientException(self::INVALID_HANDLE);
        }
        // 只取 1..4 段，多余的分段直接忽略（"v1 t b ts id extra" 仍然合法）。
        return new HandleV1($items[1], $items[2], $items[3], $items[4]);
    }

    private static function b64UrlEncode(string $raw): string
    {
        // 保留 "=" 填充：服务端回包里的句柄就带填充
        return strtr(base64_encode($raw), '+/', '-_');
    }

    /**
     * base64url 解码：编码侧带 ``=`` 填充，解码侧兼容带/不带填充两种输入。
     */
    private static function b64UrlDecode(string $handle): ?string
    {
        $padded = $handle . str_repeat('=', (4 - strlen($handle) % 4) % 4);
        $std = strtr($padded, '-_', '+/');
        $raw = base64_decode($std, true);
        return $raw === false ? null : $raw;
    }

    /** 触发异常类加载；bootstrap.php 的 classmap fallback 会命中 Client/Exceptions.php。 */
    private static function ensureExceptions(): void
    {
        class_exists(MQClientException::class);
    }
}
