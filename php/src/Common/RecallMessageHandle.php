<?php

declare(strict_types=1);

namespace RocketMQ\Common;

use RocketMQ\Client\Exceptions\MQClientException;

/**
 * The recall handle of a scheduled message: a credential returned by the broker that is passed back
 * verbatim when recalling.
 *
 * The handle is not created by the client: when sending a scheduled message carrying
 * ``TIMER_DELIVER_MS`` / ``TIMER_DELAY_MS`` / ``TIMER_DELAY_SEC``, the broker attaches this handle to the
 * SEND response header field ``recallHandle`` inside ``SendMessageProcessor#attachRecallHandle``; the
 * client only has to pass it back verbatim to call ``recallMessage``. Ordinary message responses simply
 * do not carry this field.
 *
 * The encoding format is fixed by the server-side contract:
 * ``base64url("v1 <topic> <brokerName> <timestampStr> <messageId>")``, 5 space-separated segments.
 *
 * Two deliberate choices:
 *   * Encoding keeps the ``=`` padding (the handle in server responses is in the padded form); decoding
 *     accepts both padded and unpadded input — messages left by writers that only send unpadded handles
 *     must be recallable too.
 *   * A decoding failure throws ``MQClientException`` directly, with the fixed message
 *     ``"recall handle is invalid"``: callers match the error on this exact message, so it must not be
 *     changed.
 */
final class RecallMessageHandle
{
    public const SEPARATOR = ' ';
    public const VERSION_1 = 'v1';
    public const INVALID_HANDLE = 'recall handle is invalid';

    /**
     * Builds the handle: 5 segments joined by spaces then base64url encoded, **with** ``=`` padding.
     */
    public static function buildHandle(string $topic, string $brokerName, string $timestampStr, string $messageId): string
    {
        $raw = implode(self::SEPARATOR, [self::VERSION_1, $topic, $brokerName, $timestampStr, $messageId]);
        return self::b64UrlEncode($raw);
    }

    /**
     * Decodes the handle: any illegal input throws ``MQClientException``, see the tolerance notes in the
     * class comment.
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
            // An illegal UTF-8 sequence also gets the fixed message
            throw new MQClientException(self::INVALID_HANDLE);
        }
        $items = explode(self::SEPARATOR, $raw);
        if (count($items) < 5 || $items[0] !== self::VERSION_1) {
            throw new MQClientException(self::INVALID_HANDLE);
        }
        // Only segments 1..4 are taken, any extra segments are ignored outright
        // ("v1 t b ts id extra" is still legal).
        return new HandleV1($items[1], $items[2], $items[3], $items[4]);
    }

    private static function b64UrlEncode(string $raw): string
    {
        // Keep the "=" padding: the handle in server responses does carry padding
        return strtr(base64_encode($raw), '+/', '-_');
    }

    /**
     * base64url decoding: the encoding side emits ``=`` padding, the decoding side accepts both padded
     * and unpadded input.
     */
    private static function b64UrlDecode(string $handle): ?string
    {
        $padded = $handle . str_repeat('=', (4 - strlen($handle) % 4) % 4);
        $std = strtr($padded, '-_', '+/');
        $raw = base64_decode($std, true);
        return $raw === false ? null : $raw;
    }

    /** Triggers loading of the exception class; the classmap fallback in bootstrap.php hits Client/Exceptions.php. */
    private static function ensureExceptions(): void
    {
        class_exists(MQClientException::class);
    }
}
