<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;

final class TraceContextPropagator
{
    /** W3C 键名（小写，与 OTel / HTTP 头一致） */
    public const TRACE_CONTEXT_PROPERTY = 'traceparent';
    public const TRACE_STATE_PROPERTY = 'tracestate';

    private const ALL_HEX = '0123456789abcdef';

    /** 生成合法的根 traceparent：``00-<32hex>-<16hex>-01``（记录采样）。 */
    public static function generateTraceparent(): string
    {
        return sprintf('00-%s-%s-01', self::hex(32), self::hex(16));
    }

    /** 按 W3C 语法与"不全 0"规则校验。宽松接受大写 hex（转发不重写）。 */
    public static function isValidTraceparent(?string $value): bool
    {
        if ($value === null || $value === '') {
            return false;
        }
        $parts = explode('-', trim($value));
        if (count($parts) !== 4) {
            return false;
        }
        [$version, $traceId, $parentId, $flags] = $parts;

        if ($version !== '00' && !(strlen($version) === 2 && self::allHex($version))) {
            return false;
        }
        if ($version === 'ff') {
            return false;
        }
        if (strlen($traceId) !== 32 || strlen($parentId) !== 16 || strlen($flags) !== 2) {
            return false;
        }
        foreach ([[$traceId, true], [$parentId, true], [$flags, false]] as [$part, $disallowZero]) {
            $low = strtolower($part);
            if (!self::allHex($low)) {
                return false;
            }
            if ($disallowZero && $low === str_repeat('0', strlen($low))) {
                return false;
            }
        }
        return true;
    }

    /** 同一 trace-id 下生成子 span（换 parent-id）；parent 非法返回 null。 */
    public static function childTraceparent(?string $parent): ?string
    {
        if (!self::isValidTraceparent($parent)) {
            return null;
        }
        $parts = explode('-', trim((string) $parent));
        return sprintf('00-%s-%s-01', strtolower($parts[1]), self::hex(16));
    }

    /**
     * 消息没有 traceparent 属性时注入根上下文；返回（注入后的）值。
     *
     * 已有值**不覆盖**——上游传播进来的上下文优先。
     */
    public static function injectTraceContext(Message $message): string
    {
        $existing = $message->getProperty(self::TRACE_CONTEXT_PROPERTY);
        if ($existing !== null && $existing !== '') {
            return $existing;
        }
        $tp = self::generateTraceparent();
        $message->putProperty(self::TRACE_CONTEXT_PROPERTY, $tp);
        return $tp;
    }

    /** 从消息属性里取出 traceparent（未注入/为空返回 null）。 */
    public static function extractTraceparent(MessageExt $msg): ?string
    {
        $value = $msg->getProperty(self::TRACE_CONTEXT_PROPERTY);
        return ($value !== null && $value !== '') ? $value : null;
    }

    /** env ``ROCKETMQ_TRACE_CONTEXT_ENABLE``（对齐其它开关的 env 惯例）。 */
    public static function traceContextEnabledFromEnv(): bool
    {
        $raw = getenv('ROCKETMQ_TRACE_CONTEXT_ENABLE');
        if ($raw === false) {
            return false;
        }
        return in_array(strtolower(trim((string) $raw)), ['1', 'true', 'yes'], true);
    }

    /** 对应 Python ``secrets.token_hex(n // 2)``：n 位十六进制。 */
    private static function hex(int $n): string
    {
        return bin2hex(random_bytes(intdiv($n, 2)));
    }

    private static function allHex(string $s): bool
    {
        $len = strlen($s);
        for ($i = 0; $i < $len; $i++) {
            if (strpos(self::ALL_HEX, $s[$i]) === false) {
                return false;
            }
        }
        return true;
    }
}
