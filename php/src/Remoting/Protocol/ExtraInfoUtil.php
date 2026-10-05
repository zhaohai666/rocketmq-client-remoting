<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MixAll;

/**
 * POP 模式的 extraInfo（俗称 CK 串）编解码（移植自 protocol/extra_info.py）。
 *
 * 逐条移植 Java ``org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil``。
 *
 * CK 串是 POP 模式的核心凭据：broker 在 POP 响应里**不**给普通 topic 的消息写
 * ``POP_CK`` 属性（只在 retry topic 重编码路径写），客户端必须自己用响应头的
 * ``startOffsetInfo`` / ``msgOffsetInfo`` 反构出这个串，再作为 ACK /
 * CHANGE_MESSAGE_INVISIBLETIME 的 ``extraInfo`` 回传。格式是 8 段、**空格**分隔：
 *
 * ``ckQueueOffset popTime invisibleTime reviveQid retryFlag brokerName queueId [msgQueueOffset]``
 *
 * 注意分隔符是空格而不是逗号 —— 用错会导致 broker 静默解析失败。
 */
final class ExtraInfoUtil
{
    /** 段内分隔符；**必须**是空格，对应 Java ``MessageConst.KEY_SEPARATOR`` */
    public const KEY_SEPARATOR = ' ';
    /** 队列之间的分隔符 */
    public const QUEUE_SEPARATOR = ';';
    /** ``msgOffsetInfo`` 里同一队列多条 offset 的分隔符 */
    public const OFFSET_SEPARATOR = ',';

    public const NORMAL_TOPIC = '0';
    public const RETRY_TOPIC = '1';
    public const RETRY_TOPIC_V2 = '2';
    public const QUEUE_OFFSET = 'qo';

    /** 顺序消费用的固定 revive 队列号，对应 Java ``KeyBuilder.POP_ORDER_REVIVE_QUEUE`` */
    public const POP_ORDER_REVIVE_QUEUE = 999;

    private const POP_RETRY_SEPARATOR_V1 = '_';
    private const POP_RETRY_SEPARATOR_V2 = '+';

    /** ``%RETRY%<cid>_<topic>``（Java ``KeyBuilder.buildPopRetryTopicV1``）。 */
    public static function buildPopRetryTopicV1(string $topic, string $cid): string
    {
        return MixAll::RETRY_GROUP_TOPIC_PREFIX . $cid . self::POP_RETRY_SEPARATOR_V1 . $topic;
    }

    /** ``%RETRY%<cid>+<topic>``（Java ``KeyBuilder.buildPopRetryTopicV2``）。 */
    public static function buildPopRetryTopicV2(string $topic, string $cid): string
    {
        return MixAll::RETRY_GROUP_TOPIC_PREFIX . $cid . self::POP_RETRY_SEPARATOR_V2 . $topic;
    }

    /** Java ``buildPopRetryTopic``：``enableRetryTopicV2`` 关（默认）走 V1。 */
    public static function buildPopRetryTopic(string $topic, string $cid, bool $enableRetryV2 = false): string
    {
        return $enableRetryV2 ? self::buildPopRetryTopicV2($topic, $cid) : self::buildPopRetryTopicV1($topic, $cid);
    }

    /** Java ``KeyBuilder.isPopRetryTopicV2``：``%RETRY%`` 前缀且含 ``+``。 */
    public static function isPopRetryTopicV2(?string $retryTopic): bool
    {
        if ($retryTopic === null || $retryTopic === '') {
            return false;
        }
        return str_starts_with($retryTopic, MixAll::RETRY_GROUP_TOPIC_PREFIX)
            && strpos($retryTopic, self::POP_RETRY_SEPARATOR_V2) !== false;
    }

    /**
     * 模拟 Java ``String.split``：**丢弃末尾空串**。
     * 这个差异是真实的：Java ``"a b ".split(" ")`` 得到 ``["a","b"]``，段数校验依赖它。
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

    /** 按空格切分 CK 串（Java ``ExtraInfoUtil.split``）。 */
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
     * 由 topic 形状判定 retryFlag（Java ``ExtraInfoUtil.getRetry(topic)``）。
     * 顺序很重要：先判 V2（含 ``+``），再判 ``%RETRY%`` 前缀（V1）。
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
     * 拼 CK 串。传 ``$msgQueueOffset`` 得到 8 段，不传得到 7 段。
     * 对应 Java 的两个 ``buildExtraInfo`` 重载。ACK 场景用 8 段版本。
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
     * 解析 ``startOffsetInfo``，形如 ``"0 3 0;0 2 0"``。
     * key 是 ``"<retryFlag>@<queueId>"``，value 是该队列本次弹出的起始 offset。
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
     * 解析 ``msgOffsetInfo``，形如 ``"0 3 0,1,2;0 2 0"``。
     * key 同 parseStartOffsetInfo，value 是该队列本次弹出的各条消息 offset 列表。
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
     * 解析 ``orderCountInfo``（顺序消费用），第三段是计数。
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

    /** reviveQid 是 999 表示顺序消费（Java ``ExtraInfoUtil.isOrder``）。 */
    public static function isOrder(array $segments): bool
    {
        return self::getReviveQid($segments) === self::POP_ORDER_REVIVE_QUEUE;
    }

    /** 由 retryFlag 还原真实 topic（Java ``ExtraInfoUtil.getRealTopic``）。 */
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
