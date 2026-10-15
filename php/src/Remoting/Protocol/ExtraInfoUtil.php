<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MixAll;

/**
 * Encoding and decoding of the extraInfo of POP mode (commonly called the CK string).
 *
 * The segment order, the separators and the compatibility rules of the string are all fixed by the
 * broker-side parsing code, and this class implements exactly the shape it accepts.
 *
 * The CK string is the core credential of POP mode: the broker **does not** write the ``POP_CK`` property
 * for messages of ordinary topics in the POP response (it only does so on the retry-topic re-encoding
 * path), so the client has to reconstruct the string itself from the ``startOffsetInfo`` /
 * ``msgOffsetInfo`` of the response header and send it back as the ``extraInfo`` of ACK /
 * CHANGE_MESSAGE_INVISIBLETIME. The format is 8 segments separated by **spaces**:
 *
 * ``ckQueueOffset popTime invisibleTime reviveQid retryFlag brokerName queueId [msgQueueOffset]``
 *
 * Note that the separator is a space and not a comma — getting it wrong makes the broker fail to parse
 * it silently.
 */
final class ExtraInfoUtil
{
    /** The in-segment separator; it **must** be a space — the broker splits on spaces, and with any
     * other character the whole string cannot be decoded */
    public const KEY_SEPARATOR = ' ';
    /** The separator between queues */
    public const QUEUE_SEPARATOR = ';';
    /** The separator between multiple offsets of the same queue in ``msgOffsetInfo`` */
    public const OFFSET_SEPARATOR = ',';

    public const NORMAL_TOPIC = '0';
    public const RETRY_TOPIC = '1';
    public const RETRY_TOPIC_V2 = '2';
    public const QUEUE_OFFSET = 'qo';

    /** The fixed revive queue id used for orderly consumption: 999 is the sentinel value in the protocol */
    public const POP_ORDER_REVIVE_QUEUE = 999;

    private const POP_RETRY_SEPARATOR_V1 = '_';
    private const POP_RETRY_SEPARATOR_V2 = '+';

    /** The V1-shaped POP retry topic name: ``%RETRY%<cid>_<topic>``. */
    public static function buildPopRetryTopicV1(string $topic, string $cid): string
    {
        return MixAll::RETRY_GROUP_TOPIC_PREFIX . $cid . self::POP_RETRY_SEPARATOR_V1 . $topic;
    }

    /** The V2-shaped POP retry topic name: ``%RETRY%<cid>+<topic>``. */
    public static function buildPopRetryTopicV2(string $topic, string $cid): string
    {
        return MixAll::RETRY_GROUP_TOPIC_PREFIX . $cid . self::POP_RETRY_SEPARATOR_V2 . $topic;
    }

    /** Picks the shape by the switch: with ``enableRetryTopicV2`` off (the default) it goes V1. */
    public static function buildPopRetryTopic(string $topic, string $cid, bool $enableRetryV2 = false): string
    {
        return $enableRetryV2 ? self::buildPopRetryTopicV2($topic, $cid) : self::buildPopRetryTopicV1($topic, $cid);
    }

    /** The ``%RETRY%`` prefix plus a ``+`` means a V2 retry topic. */
    public static function isPopRetryTopicV2(?string $retryTopic): bool
    {
        if ($retryTopic === null || $retryTopic === '') {
            return false;
        }
        return str_starts_with($retryTopic, MixAll::RETRY_GROUP_TOPIC_PREFIX)
            && strpos($retryTopic, self::POP_RETRY_SEPARATOR_V2) !== false;
    }

    /**
     * Splits the segments following the broker-side splitting semantics: **the trailing empty strings are
     * dropped**.
     * This is not nitpicking: ``"a b "`` splits into the two segments ``["a","b"]``, and the segment count
     * check depends on it.
     *
     * @return list<string>
     */
    private static function splitDropTrailing(string $value, string $sep): array
    {
        $parts = explode($sep, $value);
        while ($parts !== [] && end($parts) === '') {
            array_pop($parts);
        }
        return $parts;
    }

    /** Splits the CK string by spaces. */
    public static function split(?string $extraInfo): array
    {
        if ($extraInfo === null) {
            throw new \InvalidArgumentException('split extraInfo is null');
        }
        return self::splitDropTrailing($extraInfo, self::KEY_SEPARATOR);
    }

    private static function require(?array $segments, int $need, string $what): void
    {
        if ($segments === null || count($segments) < $need) {
            throw new \InvalidArgumentException(sprintf('%s fail, extraInfoStrs length %d', $what, $segments === null ? 0 : count($segments)));
        }
    }

    public static function getCkQueueOffset(?array $segments): int
    {
        self::require($segments, 1, 'getCkQueueOffset');
        return (int)$segments[0];
    }

    public static function getPopTime(?array $segments): int
    {
        self::require($segments, 2, 'getPopTime');
        return (int)$segments[1];
    }

    public static function getInvisibleTime(?array $segments): int
    {
        self::require($segments, 3, 'getInvisibleTime');
        return (int)$segments[2];
    }

    public static function getReviveQid(?array $segments): int
    {
        self::require($segments, 4, 'getReviveQid');
        return (int)$segments[3];
    }

    public static function getRetry(?array $segments): string
    {
        self::require($segments, 5, 'getRetry');
        return $segments[4];
    }

    public static function getBrokerName(?array $segments): string
    {
        self::require($segments, 6, 'getBrokerName');
        return $segments[5];
    }

    public static function getQueueId(?array $segments): int
    {
        self::require($segments, 7, 'getQueueId');
        return (int)$segments[6];
    }

    public static function getQueueOffset(?array $segments): int
    {
        self::require($segments, 8, 'getQueueOffset');
        return (int)$segments[7];
    }

    /**
     * Determines retryFlag from the shape of the topic.
     * The order matters: check V2 (containing ``+``) first, then the ``%RETRY%`` prefix (V1).
     */
    public static function retryOfTopic(string $topic): string
    {
        if (self::isPopRetryTopicV2($topic)) {
            return self::RETRY_TOPIC_V2;
        }
        if (str_starts_with($topic, MixAll::RETRY_GROUP_TOPIC_PREFIX)) {
            return self::RETRY_TOPIC;
        }
        return self::NORMAL_TOPIC;
    }

    /**
     * Builds the CK string. Passing ``$msgQueueOffset`` yields 8 segments, not passing it yields 7.
     * The broker accepts both lengths: the ACK scenario must use the 8-segment version, one segment
     * short loses the queue offset.
     */
    public static function buildExtraInfo(int $ckQueueOffset, int $popTime, int $invisibleTime, int $reviveQid, string $topic, string $brokerName, int $queueId, ?int $msgQueueOffset = null): string
    {
        $parts = [
            (string)$ckQueueOffset,
            (string)$popTime,
            (string)$invisibleTime,
            (string)$reviveQid,
            self::retryOfTopic($topic),
            $brokerName,
            (string)$queueId,
        ];
        if ($msgQueueOffset !== null) {
            $parts[] = (string)$msgQueueOffset;
        }
        return implode(self::KEY_SEPARATOR, $parts);
    }

    /**
     * Parses ``startOffsetInfo``, whose shape is ``"0 3 0;0 2 0"``.
     * The key is ``"<retryFlag>@<queueId>"``, the value is the start offset of this pop for that queue.
     *
     * @return array<string, int>|null
     */
    public static function parseStartOffsetInfo(?string $startOffsetInfo): ?array
    {
        if ($startOffsetInfo === null || $startOffsetInfo === '') {
            return null;
        }
        $out = [];
        $segments = !str_contains($startOffsetInfo, self::QUEUE_SEPARATOR)
            ? [$startOffsetInfo]
            : self::splitDropTrailing($startOffsetInfo, self::QUEUE_SEPARATOR);
        foreach ($segments as $one) {
            $parts = explode(self::KEY_SEPARATOR, $one);
            if (count($parts) !== 3) {
                throw new \InvalidArgumentException('parse startOffsetInfo error, ' . $startOffsetInfo);
            }
            $key = $parts[0] . '@' . $parts[1];
            if (array_key_exists($key, $out)) {
                throw new \InvalidArgumentException('parse startOffsetInfo error, duplicate, ' . $startOffsetInfo);
            }
            $out[$key] = (int)$parts[2];
        }
        return $out;
    }

    /**
     * Parses ``msgOffsetInfo``, whose shape is ``"0 3 0,1,2;0 2 0"``.
     * The key is the same as in parseStartOffsetInfo, the value is the list of offsets of the messages
     * popped this time for that queue.
     *
     * @return array<string, list<int>>|null
     */
    public static function parseMsgOffsetInfo(?string $msgOffsetInfo): ?array
    {
        if ($msgOffsetInfo === null || $msgOffsetInfo === '') {
            return null;
        }
        $out = [];
        $segments = !str_contains($msgOffsetInfo, self::QUEUE_SEPARATOR)
            ? [$msgOffsetInfo]
            : self::splitDropTrailing($msgOffsetInfo, self::QUEUE_SEPARATOR);
        foreach ($segments as $one) {
            $parts = explode(self::KEY_SEPARATOR, $one);
            if (count($parts) !== 3) {
                throw new \InvalidArgumentException('parse msgOffsetInfo error, ' . $msgOffsetInfo);
            }
            $key = $parts[0] . '@' . $parts[1];
            if (array_key_exists($key, $out)) {
                throw new \InvalidArgumentException('parse msgOffsetInfo error, duplicate, ' . $msgOffsetInfo);
            }
            $out[$key] = array_map(intval(...), self::splitDropTrailing($parts[2], self::OFFSET_SEPARATOR));
        }
        return $out;
    }

    /**
     * Parses ``orderCountInfo`` (used for orderly consumption), the third segment is the count.
     *
     * @return array<string, int>|null
     */
    public static function parseOrderCountInfo(?string $orderCountInfo): ?array
    {
        if ($orderCountInfo === null || $orderCountInfo === '') {
            return null;
        }
        $out = [];
        $segments = !str_contains($orderCountInfo, self::QUEUE_SEPARATOR)
            ? [$orderCountInfo]
            : self::splitDropTrailing($orderCountInfo, self::QUEUE_SEPARATOR);
        foreach ($segments as $one) {
            $parts = explode(self::KEY_SEPARATOR, $one);
            if (count($parts) !== 3) {
                throw new \InvalidArgumentException('parse orderCountInfo error, ' . $orderCountInfo);
            }
            $key = $parts[0] . '@' . $parts[1];
            if (array_key_exists($key, $out)) {
                throw new \InvalidArgumentException('parse orderCountInfo error, duplicate, ' . $orderCountInfo);
            }
            $out[$key] = (int)$parts[2];
        }
        return $out;
    }

    public static function getStartOffsetInfoMapKey(string $topic, int|string $key): string
    {
        return self::retryOfTopic($topic) . '@' . (string)$key;
    }

    public static function getQueueOffsetKeyValueKey(int|string $queueId, int|string $queueOffset): string
    {
        return self::QUEUE_OFFSET . (string)$queueId . '%' . (string)$queueOffset;
    }

    public static function getQueueOffsetMapKey(string $topic, int|string $queueId, int|string $queueOffset): string
    {
        return self::retryOfTopic($topic) . '@' . self::getQueueOffsetKeyValueKey($queueId, $queueOffset);
    }

    /** A reviveQid of 999 means orderly consumption. */
    public static function isOrder(array $segments): bool
    {
        return self::getReviveQid($segments) === self::POP_ORDER_REVIVE_QUEUE;
    }

    /** Restores the real topic from retryFlag (strips the %RETRY% prefix and the cid segment). */
    public static function getRealTopic(string $topic, string $cid, string $retry): string
    {
        if ($retry === self::NORMAL_TOPIC) {
            return $topic;
        }
        if ($retry === self::RETRY_TOPIC) {
            return self::buildPopRetryTopicV1($topic, $cid);
        }
        if ($retry === self::RETRY_TOPIC_V2) {
            return self::buildPopRetryTopicV2($topic, $cid);
        }
        throw new \InvalidArgumentException('getRetry fail, format is wrong');
    }
}
