<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;

/**
 * W3C Trace Context（traceparent）透传，移植自 trace_context.py。
 *
 * 格式（W3C Trace Context）：``traceparent: 00-<trace-id 32hex>-<parent-id 16hex>-<flags 2hex>``
 *
 * * trace-id / parent-id 不能全 0；version 00 时 flags 任意两位 hex；
 * * 生产侧：消息没有 traceparent 属性时注入一个根 span 上下文（opt-in，
 *   enableTraceContext=true）——键名沿用 W3C 的小写 traceparent；已有值**不覆盖**；
 * * 消费侧：从 MessageExt.properties 里取出，供业务做父子 span 关联。
 *
 * Python 侧是模块级函数（``trace_context.py``），这里落成静态方法（autoload 友好）。
 *
 * ⚠ 命名：Python 的实现方式没有「类」这一层，所以模块名 ``trace_context`` 直译会是
 * ``TraceContext``；但 PHP 无模块隔离，而 Java 的 ``org.apache.rocketmq.client.trace.TraceContext``
 * 已占用该名字（见 Trace.php，消息轨迹上下文）。因此这里改名为
 * ``TraceContextPropagator``（传播器），语义上更准确且与轨迹上下文区分。
 */
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
