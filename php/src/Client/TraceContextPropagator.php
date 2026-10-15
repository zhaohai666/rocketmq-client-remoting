<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;

final class TraceContextPropagator
{
    /** W3C key names (lowercase, consistent with OTel / HTTP headers) */
    public const TRACE_CONTEXT_PROPERTY = 'traceparent';
    public const TRACE_STATE_PROPERTY = 'tracestate';

    private const ALL_HEX = '0123456789abcdef';

    /** Generate a valid root traceparent: ``00-<32hex>-<16hex>-01`` (record sampled). */
    public static function generateTraceparent(): string
    {
        return sprintf('00-%s-%s-01', self::hex(32), self::hex(16));
    }

    /** Validate per W3C syntax and the "not all zeros" rule. Accepts uppercase hex loosely (forwarding does not rewrite). */
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

    /** Generate a child span under the same trace-id (swap the parent-id); returns null if the parent is invalid. */
    public static function childTraceparent(?string $parent): ?string
    {
        if (!self::isValidTraceparent($parent)) {
            return null;
        }
        $parts = explode('-', trim((string) $parent));
        return sprintf('00-%s-%s-01', strtolower($parts[1]), self::hex(16));
    }

    /**
     * Inject the root context when the message has no traceparent property; returns the
     * (injected) value.
     *
     * An existing value is **not** overwritten -- the context propagated from upstream wins.
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

    /** Take the traceparent from the message properties (null when not injected / empty). */
    public static function extractTraceparent(MessageExt $msg): ?string
    {
        $value = $msg->getProperty(self::TRACE_CONTEXT_PROPERTY);
        return ($value !== null && $value !== '') ? $value : null;
    }

    /** env ``ROCKETMQ_TRACE_CONTEXT_ENABLE`` (follows the env convention of the other switches). */
    public static function traceContextEnabledFromEnv(): bool
    {
        $raw = getenv('ROCKETMQ_TRACE_CONTEXT_ENABLE');
        if ($raw === false) {
            return false;
        }
        return in_array(strtolower(trim((string) $raw)), ['1', 'true', 'yes'], true);
    }

    /** Generate an n-digit hex random string. */
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
