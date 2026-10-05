<?php

declare(strict_types=1);

namespace RocketMQ\Common;

use RocketMQ\Client\Exceptions\MQClientException;

/**
 * 定时消息的撤回句柄（对应 org.apache.rocketmq.common.producer.RecallMessageHandle，
 * 移植自 recall_message_handle.py）。
 *
 * 句柄不是客户端自己造的：发送带 ``TIMER_DELIVER_MS`` / ``TIMER_DELAY_MS`` / ``TIMER_DELAY_SEC``
 * 的定时消息时，broker 在 ``SendMessageProcessor#attachRecallHandle`` 里把这个句柄挂到
 * SEND 响应头的 ``recallHandle`` 字段上返回，客户端只负责原样带回去调用 ``recallMessage``。
 * 因此普通消息的响应里根本没有这个字段。
 *
 * 编码格式与 Java 完全一致：``base64url("v1 <topic> <brokerName> <timestampStr> <messageId>")``，
 * 5 段、空格分隔。
 *
 * 与 Java 的两处显式差异：
 *   * Java ``buildHandle`` 用 ``Base64.getUrlEncoder()``（**带** ``=`` 填充），
 *     ``decodeHandle`` 用 ``getUrlDecoder()``（严格，无填充串会抛错）。这里编码同样带填充，
 *     解码则两种都吃：其他客户端移植版用无填充解码器，只发无填充句柄的客户端写下的
 *     消息也要能撤回。
 *   * Java 解码失败抛 ``DecoderException``，``DefaultMQProducerImpl#recallMessage`` 再包成
 *     ``MQClientException(e.getMessage())``。PHP 侧直接抛 ``MQClientException``，
 *     文案仍是 Java 的 ``"recall handle is invalid"``。
 */
final class RecallMessageHandle
{
    public const SEPARATOR = ' ';
    public const VERSION_1 = 'v1';
    public const INVALID_HANDLE = 'recall handle is invalid';

    /**
     * 对应 Java ``RecallMessageHandle.buildHandle``（带 ``=`` 填充，与 Java 一致）。
     */
    public static function buildHandle(string $topic, string $brokerName, string $timestampStr, string $messageId): string
    {
        $raw = implode(self::SEPARATOR, [self::VERSION_1, $topic, $brokerName, $timestampStr, $messageId]);
        return self::b64UrlEncode($raw);
    }

    /**
     * 对应 Java ``RecallMessageHandle.decodeHandle``，见类注释的容忍度差异。
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
            // Python UnicodeDecodeError -> 同样给固定文案
            throw new MQClientException(self::INVALID_HANDLE);
        }
        $items = explode(self::SEPARATOR, $raw);
        if (count($items) < 5 || $items[0] !== self::VERSION_1) {
            throw new MQClientException(self::INVALID_HANDLE);
        }
        // Java 取 items[1..4]，多余的分段直接忽略（"v1 t b ts id extra" 仍然合法）。
        return new HandleV1($items[1], $items[2], $items[3], $items[4]);
    }

    private static function b64UrlEncode(string $raw): string
    {
        // 保留 "=" 填充（Java Base64.getUrlEncoder() / Python urlsafe_b64encode 同样带填充）
        return strtr(base64_encode($raw), '+/', '-_');
    }

    /**
     * base64url 解码：编码侧带 ``=`` 填充（与 Java getUrlEncoder 一致），解码侧
     * 兼容带/不带填充两种输入。
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
