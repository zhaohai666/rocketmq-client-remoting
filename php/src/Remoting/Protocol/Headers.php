<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\BoundaryType;

/**
 * 全部请求/响应头（对应 org.apache.rocketmq.remoting.protocol.header.*，移植自 protocol/headers.py）。
 *
 * 每个 header 提供 toExtFields()（仅输出非 null 字段，与 Java makeCustomHeaderToNet 行为一致）
 * 和 fromExtFields()（把 ext_fields 映射回字段）。
 *
 * ⚠ ext key 必须逐字等于 Java 侧的字段名：broker 用 fastjson2 按 Java 属性名反序列化，
 * 错一个字母就**静默丢字段**（不报错、行为静默退化）。
 */
abstract class CommandCustomHeader
{
    /** @return array<string, mixed> */
    abstract public function toExtFields(): array;

    public function checkFields(): void
    {
    }

    /**
     * 过滤 null，并把 bool 规范成 Java ``Boolean.toString`` 的小写形式。
     *
     * 为什么必须显式转换：``RemotingCommand`` 落 ext_fields 时统一走 ``str(v)``，
     * Python 的 ``str(False)`` 是 ``"False"``，而 Java 写的是 ``"false"``。
     * 这里统一成小写，与 C++ 侧 ``putOptBool`` 完全一致。
     *
     * @param array<string, mixed> $fields
     * @return array<string, mixed>
     */
    protected static function ext(array $fields): array
    {
        $out = [];
        foreach ($fields as $k => $v) {
            if ($v === null) {
                continue;
            }
            $out[$k] = is_bool($v) ? ($v ? 'true' : 'false') : $v;
        }
        return $out;
    }

    protected static function i(mixed $v): ?int
    {
        return $v === null ? null : (int)$v;
    }

    protected static function l(mixed $v): ?int
    {
        return $v === null ? null : (int)$v;
    }

    protected static function b(mixed $v): ?bool
    {
        if ($v === null) {
            return null;
        }
        return in_array(strtolower((string)$v), ['true', '1'], true);
    }
}

// ---------------- 发送消息 ----------------

final class SendMessageRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $producerGroup = null,
        public ?string $topic = null,
        public ?string $defaultTopic = null,
        public ?int $defaultTopicQueueNums = null,
        public ?int $queueId = null,
        public ?int $sysFlag = null,
        public ?int $bornTimestamp = null,
        public ?int $flag = null,
        public ?string $properties = null,
        public ?int $reconsumeTimes = null,
        public ?bool $unitMode = null,
        public ?int $maxReconsumeTimes = null,
        public ?bool $batch = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'producerGroup' => $this->producerGroup,
            'topic' => $this->topic,
            'defaultTopic' => $this->defaultTopic,
            'defaultTopicQueueNums' => $this->defaultTopicQueueNums,
            'queueId' => $this->queueId,
            'sysFlag' => $this->sysFlag,
            'bornTimestamp' => $this->bornTimestamp,
            'flag' => $this->flag,
            'properties' => $this->properties,
            'reconsumeTimes' => $this->reconsumeTimes,
            'unitMode' => $this->unitMode,
            'maxReconsumeTimes' => $this->maxReconsumeTimes,
            'batch' => $this->batch,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->producerGroup = isset($ext['producerGroup']) ? (string)$ext['producerGroup'] : null;
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->defaultTopic = isset($ext['defaultTopic']) ? (string)$ext['defaultTopic'] : null;
        $this->defaultTopicQueueNums = self::i($ext['defaultTopicQueueNums'] ?? null);
        $this->queueId = self::i($ext['queueId'] ?? null);
        $this->sysFlag = self::i($ext['sysFlag'] ?? null);
        $this->bornTimestamp = self::l($ext['bornTimestamp'] ?? null);
        $this->flag = self::i($ext['flag'] ?? null);
        $this->properties = isset($ext['properties']) ? (string)$ext['properties'] : null;
        $this->reconsumeTimes = self::i($ext['reconsumeTimes'] ?? null);
        $this->unitMode = self::b($ext['unitMode'] ?? null);
        $this->maxReconsumeTimes = self::i($ext['maxReconsumeTimes'] ?? null);
        $this->batch = self::b($ext['batch'] ?? null);
    }
}

/** 短字段名编码（producerGroup->a ...），与 Java V2 严格一致。 */
final class SendMessageRequestHeaderV2 extends CommandCustomHeader
{
    public function __construct(
        public ?string $producerGroup = null,
        public ?string $topic = null,
        public ?string $defaultTopic = null,
        public ?int $defaultTopicQueueNums = null,
        public ?int $queueId = null,
        public ?int $sysFlag = null,
        public ?int $bornTimestamp = null,
        public ?int $flag = null,
        public ?string $properties = null,
        public ?int $reconsumeTimes = null,
        public ?bool $unitMode = null,
        public ?int $maxReconsumeTimes = null,
        public ?bool $batch = null,
        // Java SendMessageRequestHeaderV2 还有 private String n; // brokerName
        public ?string $brokerName = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'a' => $this->producerGroup,
            'b' => $this->topic,
            'c' => $this->defaultTopic,
            'd' => $this->defaultTopicQueueNums,
            'e' => $this->queueId,
            'f' => $this->sysFlag,
            'g' => $this->bornTimestamp,
            'h' => $this->flag,
            'i' => $this->properties,
            'j' => $this->reconsumeTimes,
            'k' => $this->unitMode,
            'l' => $this->maxReconsumeTimes,
            'm' => $this->batch,
            'n' => $this->brokerName,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->producerGroup = isset($ext['a']) ? (string)$ext['a'] : null;
        $this->topic = isset($ext['b']) ? (string)$ext['b'] : null;
        $this->defaultTopic = isset($ext['c']) ? (string)$ext['c'] : null;
        $this->defaultTopicQueueNums = self::i($ext['d'] ?? null);
        $this->queueId = self::i($ext['e'] ?? null);
        $this->sysFlag = self::i($ext['f'] ?? null);
        $this->bornTimestamp = self::l($ext['g'] ?? null);
        $this->flag = self::i($ext['h'] ?? null);
        $this->properties = isset($ext['i']) ? (string)$ext['i'] : null;
        $this->reconsumeTimes = self::i($ext['j'] ?? null);
        $this->unitMode = self::b($ext['k'] ?? null);
        $this->maxReconsumeTimes = self::i($ext['l'] ?? null);
        $this->batch = self::b($ext['m'] ?? null);
        $this->brokerName = isset($ext['n']) ? (string)$ext['n'] : null;
    }
}

/**
 * broker → 请求方 的 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 请求头。
 *
 * 对应 Java ``ReplyMessageRequestHeader``（字段与 ``SendMessageRequestHeader`` 高度重合，
 * 但多了 bornHost/storeHost/storeTimestamp）。请求方收到后据此 + body 还原出真正的应答 MessageExt。
 */
final class ReplyMessageRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $producerGroup = null,
        public ?string $topic = null,
        public ?string $defaultTopic = null,
        public ?int $defaultTopicQueueNums = null,
        public ?int $queueId = null,
        public ?int $sysFlag = null,
        public ?int $bornTimestamp = null,
        public ?int $flag = null,
        public ?string $properties = null,
        public ?int $reconsumeTimes = null,
        public ?bool $unitMode = null,
        public ?string $bornHost = null,
        public ?string $storeHost = null,
        public ?int $storeTimestamp = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'producerGroup' => $this->producerGroup,
            'topic' => $this->topic,
            'defaultTopic' => $this->defaultTopic,
            'defaultTopicQueueNums' => $this->defaultTopicQueueNums,
            'queueId' => $this->queueId,
            'sysFlag' => $this->sysFlag,
            'bornTimestamp' => $this->bornTimestamp,
            'flag' => $this->flag,
            'properties' => $this->properties,
            'reconsumeTimes' => $this->reconsumeTimes,
            'unitMode' => $this->unitMode,
            'bornHost' => $this->bornHost,
            'storeHost' => $this->storeHost,
            'storeTimestamp' => $this->storeTimestamp,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->producerGroup = isset($ext['producerGroup']) ? (string)$ext['producerGroup'] : null;
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->defaultTopic = isset($ext['defaultTopic']) ? (string)$ext['defaultTopic'] : null;
        $this->defaultTopicQueueNums = self::i($ext['defaultTopicQueueNums'] ?? null);
        $this->queueId = self::i($ext['queueId'] ?? null);
        $this->sysFlag = self::i($ext['sysFlag'] ?? null);
        $this->bornTimestamp = self::l($ext['bornTimestamp'] ?? null);
        $this->flag = self::i($ext['flag'] ?? null);
        $this->properties = isset($ext['properties']) ? (string)$ext['properties'] : null;
        $this->reconsumeTimes = self::i($ext['reconsumeTimes'] ?? null);
        $this->unitMode = self::b($ext['unitMode'] ?? null);
        $this->bornHost = isset($ext['bornHost']) ? (string)$ext['bornHost'] : null;
        $this->storeHost = isset($ext['storeHost']) ? (string)$ext['storeHost'] : null;
        $this->storeTimestamp = self::l($ext['storeTimestamp'] ?? null);
    }
}

final class SendMessageResponseHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $msgId = null,
        public ?int $queueId = null,
        public ?int $queueOffset = null,
        public ?string $transactionId = null,
        public ?string $batchUniqId = null,
        // 只给定时/延迟消息：broker 的 SendMessageProcessor#attachRecallHandle 看到
        // TIMER_OUT_MS + REAL_TOPIC 才挂上，普通消息恒为 null。
        public ?string $recallHandle = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'msgId' => $this->msgId,
            'queueId' => $this->queueId,
            'queueOffset' => $this->queueOffset,
            'transactionId' => $this->transactionId,
            'batchUniqId' => $this->batchUniqId,
            'recallHandle' => $this->recallHandle,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->msgId = isset($ext['msgId']) ? (string)$ext['msgId'] : null;
        $this->queueId = self::i($ext['queueId'] ?? null);
        $this->queueOffset = self::l($ext['queueOffset'] ?? null);
        $this->transactionId = isset($ext['transactionId']) ? (string)$ext['transactionId'] : null;
        $this->batchUniqId = isset($ext['batchUniqId']) ? (string)$ext['batchUniqId'] : null;
        $this->recallHandle = isset($ext['recallHandle']) ? (string)$ext['recallHandle'] : null;
    }
}

/**
 * 对应 RecallMessageRequestHeader。
 *
 * ⚠ Java 侧继承 ``TopicRequestHeader`` → ``RpcRequestHeader``，父类字段 ``bname`` 的
 * **反射名就是 bname**（不是 brokerName）：写成 ``brokerName`` 会被 broker 静默丢掉。
 */
final class RecallMessageRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $producerGroup = null,
        public ?string $topic = null,
        public ?string $recallHandle = null,
        public ?string $bname = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'producerGroup' => $this->producerGroup,
            'topic' => $this->topic,
            'recallHandle' => $this->recallHandle,
            'bname' => $this->bname,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->producerGroup = isset($ext['producerGroup']) ? (string)$ext['producerGroup'] : null;
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->recallHandle = isset($ext['recallHandle']) ? (string)$ext['recallHandle'] : null;
        $this->bname = isset($ext['bname']) ? (string)$ext['bname'] : null;
    }
}

/** 对应 RecallMessageResponseHeader：Java 只有一个字段 ``msgId``。 */
final class RecallMessageResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?string $msgId = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['msgId' => $this->msgId]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->msgId = isset($ext['msgId']) ? (string)$ext['msgId'] : null;
    }
}

// ---------------- 拉取消息 ----------------

final class PullMessageRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $topic = null,
        public ?string $liteTopic = null,
        public ?int $queueId = null,
        public ?int $queueOffset = null,
        public ?int $maxMsgNums = null,
        public ?int $sysFlag = null,
        public ?int $commitOffset = null,
        public ?int $suspendTimeoutMillis = null,
        public ?string $subscription = null,
        public ?int $subVersion = null,
        public ?string $expressionType = null,
        public ?int $maxMsgBytes = null,
        public ?int $requestSource = null,
        public ?string $proxyFrowardClientId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'consumerGroup' => $this->consumerGroup,
            'topic' => $this->topic,
            'liteTopic' => $this->liteTopic,
            'queueId' => $this->queueId,
            'queueOffset' => $this->queueOffset,
            'maxMsgNums' => $this->maxMsgNums,
            'sysFlag' => $this->sysFlag,
            'commitOffset' => $this->commitOffset,
            'suspendTimeoutMillis' => $this->suspendTimeoutMillis,
            'subscription' => $this->subscription,
            'subVersion' => $this->subVersion,
            'expressionType' => $this->expressionType,
            'maxMsgBytes' => $this->maxMsgBytes,
            'requestSource' => $this->requestSource,
            'proxyFrowardClientId' => $this->proxyFrowardClientId,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->liteTopic = isset($ext['liteTopic']) ? (string)$ext['liteTopic'] : null;
        $this->queueId = self::i($ext['queueId'] ?? null);
        $this->queueOffset = self::l($ext['queueOffset'] ?? null);
        $this->maxMsgNums = self::i($ext['maxMsgNums'] ?? null);
        $this->sysFlag = self::i($ext['sysFlag'] ?? null);
        $this->commitOffset = self::l($ext['commitOffset'] ?? null);
        $this->suspendTimeoutMillis = self::l($ext['suspendTimeoutMillis'] ?? null);
        $this->subscription = isset($ext['subscription']) ? (string)$ext['subscription'] : null;
        $this->subVersion = self::l($ext['subVersion'] ?? null);
        $this->expressionType = isset($ext['expressionType']) ? (string)$ext['expressionType'] : null;
        $this->maxMsgBytes = self::i($ext['maxMsgBytes'] ?? null);
        $this->requestSource = self::i($ext['requestSource'] ?? null);
        $this->proxyFrowardClientId = isset($ext['proxyFrowardClientId']) ? (string)$ext['proxyFrowardClientId'] : null;
    }
}

final class PullMessageResponseHeader extends CommandCustomHeader
{
    public function __construct(
        public ?int $nextBeginOffset = null,
        public ?int $minOffset = null,
        public ?int $maxOffset = null,
        public ?int $suggestWhichBrokerId = null,
        public ?int $topicSysFlag = null,
        public ?int $groupSysFlag = null,
        public ?int $forbiddenType = null,
        public ?int $offsetDelta = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'nextBeginOffset' => $this->nextBeginOffset,
            'minOffset' => $this->minOffset,
            'maxOffset' => $this->maxOffset,
            'suggestWhichBrokerId' => $this->suggestWhichBrokerId,
            'topicSysFlag' => $this->topicSysFlag,
            'groupSysFlag' => $this->groupSysFlag,
            'forbiddenType' => $this->forbiddenType,
            'offsetDelta' => $this->offsetDelta,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->nextBeginOffset = self::l($ext['nextBeginOffset'] ?? null);
        $this->minOffset = self::l($ext['minOffset'] ?? null);
        $this->maxOffset = self::l($ext['maxOffset'] ?? null);
        $this->suggestWhichBrokerId = self::l($ext['suggestWhichBrokerId'] ?? null);
        $this->topicSysFlag = self::i($ext['topicSysFlag'] ?? null);
        $this->groupSysFlag = self::i($ext['groupSysFlag'] ?? null);
        $this->forbiddenType = self::i($ext['forbiddenType'] ?? null);
        $this->offsetDelta = self::l($ext['offsetDelta'] ?? null);
    }
}

// ---------------- 偏移量 ----------------

final class QueryConsumerOffsetRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $topic = null,
        public ?int $queueId = null,
        public ?bool $setZeroIfNotFound = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'consumerGroup' => $this->consumerGroup,
            'topic' => $this->topic,
            'queueId' => $this->queueId,
            'setZeroIfNotFound' => $this->setZeroIfNotFound,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->queueId = self::i($ext['queueId'] ?? null);
        $this->setZeroIfNotFound = self::b($ext['setZeroIfNotFound'] ?? null);
    }
}

final class QueryConsumerOffsetResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?int $offset = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['offset' => $this->offset]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->offset = self::l($ext['offset'] ?? null);
    }
}

final class UpdateConsumerOffsetRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $topic = null,
        public ?int $queueId = null,
        public ?int $commitOffset = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'consumerGroup' => $this->consumerGroup,
            'topic' => $this->topic,
            'queueId' => $this->queueId,
            'commitOffset' => $this->commitOffset,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->queueId = self::i($ext['queueId'] ?? null);
        $this->commitOffset = self::l($ext['commitOffset'] ?? null);
    }
}

final class UpdateConsumerOffsetResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?int $offset = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['offset' => $this->offset]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->offset = self::l($ext['offset'] ?? null);
    }
}

final class GetMaxOffsetRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?int $queueId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic, 'queueId' => $this->queueId]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->queueId = self::i($ext['queueId'] ?? null);
    }
}

final class GetMaxOffsetResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?int $offset = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['offset' => $this->offset]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->offset = self::l($ext['offset'] ?? null);
    }
}

final class GetMinOffsetRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?int $queueId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic, 'queueId' => $this->queueId]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->queueId = self::i($ext['queueId'] ?? null);
    }
}

final class GetMinOffsetResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?int $offset = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['offset' => $this->offset]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->offset = self::l($ext['offset'] ?? null);
    }
}

final class SearchOffsetRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?int $queueId = null,
        public ?int $timestamp = null,
        // Java 该字段是 @CFNullable：为 null 时**不写键**（Java 的
        // MQClientAPIImpl 已废弃的 5 参 searchOffset 就是这种形状）。
        public ?BoundaryType $boundaryType = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'topic' => $this->topic,
            'queueId' => $this->queueId,
            'timestamp' => $this->timestamp,
            // 入网文本是枚举名大写（Java makeCustomHeaderToNet 的 Enum.toString()），
            // 不是 BoundaryType.getName() 的小写名。
            'boundaryType' => $this->boundaryType?->value,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->queueId = self::i($ext['queueId'] ?? null);
        $this->timestamp = self::l($ext['timestamp'] ?? null);
        $value = $ext['boundaryType'] ?? null;
        // 缺键 ⇒ null（Java 端 getBoundaryType() 自行回落 LOWER）；
        // 有键但值不认识 ⇒ Java BoundaryType.getType 的宽松语义，给 LOWER。
        $this->boundaryType = $value !== null ? BoundaryType::getType((string)$value) : null;
    }
}

final class SearchOffsetResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?int $offset = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['offset' => $this->offset]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->offset = self::l($ext['offset'] ?? null);
    }
}

final class GetEarliestMsgStoretimeRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?int $queueId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic, 'queueId' => $this->queueId]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->queueId = self::i($ext['queueId'] ?? null);
    }
}

final class GetEarliestMsgStoretimeResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?int $timestamp = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['timestamp' => $this->timestamp]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->timestamp = self::l($ext['timestamp'] ?? null);
    }
}

// ---------------- 查询消息 ----------------

/**
 * 对应 QueryMessageRequestHeader。
 *
 * ``indexType`` 取 MessageConst.INDEX_KEY_TYPE("K") / INDEX_UNIQUE_TYPE("U") /
 * INDEX_TAG_TYPE("T")；broker 侧为空时默认按 "K"（普通 key 索引）查。
 */
final class QueryMessageRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?string $key = null,
        public ?int $maxNum = null,
        public ?int $beginTimestamp = null,
        public ?int $endTimestamp = null,
        public ?string $indexType = null,
        public ?string $lastKey = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'topic' => $this->topic,
            'key' => $this->key,
            'maxNum' => $this->maxNum,
            'beginTimestamp' => $this->beginTimestamp,
            'endTimestamp' => $this->endTimestamp,
            'indexType' => $this->indexType,
            'lastKey' => $this->lastKey,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->key = isset($ext['key']) ? (string)$ext['key'] : null;
        $this->maxNum = self::i($ext['maxNum'] ?? null);
        $this->beginTimestamp = self::l($ext['beginTimestamp'] ?? null);
        $this->endTimestamp = self::l($ext['endTimestamp'] ?? null);
        $this->indexType = isset($ext['indexType']) ? (string)$ext['indexType'] : null;
        $this->lastKey = isset($ext['lastKey']) ? (string)$ext['lastKey'] : null;
    }
}

final class QueryMessageResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?int $indexLastUpdateTimestamp = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['indexLastUpdateTimestamp' => $this->indexLastUpdateTimestamp]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->indexLastUpdateTimestamp = self::l($ext['indexLastUpdateTimestamp'] ?? null);
    }
}

final class ViewMessageRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?int $offset = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['offset' => $this->offset]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->offset = self::l($ext['offset'] ?? null);
    }
}

final class ViewMessageResponseHeader extends CommandCustomHeader
{
    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return [];
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
    }
}

// ---------------- 心跳 / 注销 ----------------

final class HeartbeatRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $clientId = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['clientID' => $this->clientId]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->clientId = isset($ext['clientID']) ? (string)$ext['clientID'] : null;
    }
}

final class UnregisterClientRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $clientId = null,
        public ?string $producerGroup = null,
        public ?string $consumerGroup = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'clientID' => $this->clientId,
            'producerGroup' => $this->producerGroup,
            'consumerGroup' => $this->consumerGroup,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->clientId = isset($ext['clientID']) ? (string)$ext['clientID'] : null;
        $this->producerGroup = isset($ext['producerGroup']) ? (string)$ext['producerGroup'] : null;
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
    }
}

// ---------------- 消费管理 ----------------

final class ConsumerSendMsgBackRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?int $offset = null,
        public ?string $group = null,
        public ?int $delayLevel = null,
        public ?string $originMsgId = null,
        public ?string $originTopic = null,
        public ?bool $unitMode = null,
        public ?int $maxReconsumeTimes = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'offset' => $this->offset,
            'group' => $this->group,
            'delayLevel' => $this->delayLevel,
            'originMsgId' => $this->originMsgId,
            'originTopic' => $this->originTopic,
            'unitMode' => $this->unitMode,
            'maxReconsumeTimes' => $this->maxReconsumeTimes,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->offset = self::l($ext['offset'] ?? null);
        $this->group = isset($ext['group']) ? (string)$ext['group'] : null;
        $this->delayLevel = self::i($ext['delayLevel'] ?? null);
        $this->originMsgId = isset($ext['originMsgId']) ? (string)$ext['originMsgId'] : null;
        $this->originTopic = isset($ext['originTopic']) ? (string)$ext['originTopic'] : null;
        $this->unitMode = self::b($ext['unitMode'] ?? null);
        $this->maxReconsumeTimes = self::i($ext['maxReconsumeTimes'] ?? null);
    }
}

final class GetConsumerListByGroupRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $consumerGroup = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['consumerGroup' => $this->consumerGroup]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
    }
}

final class GetConsumerListByGroupResponseHeader extends CommandCustomHeader
{
    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return [];
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
    }
}

final class NotifyConsumerIdsChangedRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $consumerGroup = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['consumerGroup' => $this->consumerGroup]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
    }
}

final class GetConsumerConnectionListRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $consumerGroup = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['consumerGroup' => $this->consumerGroup]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
    }
}

/** GET_CONSUMER_STATUS_FROM_CLIENT(221) 的请求头。 */
final class GetConsumerStatusRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?string $group = null,
        public ?string $clientAddr = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic, 'group' => $this->group, 'clientAddr' => $this->clientAddr]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->group = isset($ext['group']) ? (string)$ext['group'] : null;
        $this->clientAddr = isset($ext['clientAddr']) ? (string)$ext['clientAddr'] : null;
    }
}

final class GetConsumerRunningInfoRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $clientId = null,
        public ?bool $jstackEnabled = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'consumerGroup' => $this->consumerGroup,
            'clientId' => $this->clientId,
            'jstackEnable' => $this->jstackEnabled,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
        $this->clientId = isset($ext['clientId']) ? (string)$ext['clientId'] : null;
        $this->jstackEnabled = self::b($ext['jstackEnable'] ?? null);
    }
}

final class ConsumeMessageDirectlyResultRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $clientId = null,
        public ?string $msgId = null,
        public ?string $brokerName = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'consumerGroup' => $this->consumerGroup,
            'clientId' => $this->clientId,
            'msgId' => $this->msgId,
            'brokerName' => $this->brokerName,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
        $this->clientId = isset($ext['clientId']) ? (string)$ext['clientId'] : null;
        $this->msgId = isset($ext['msgId']) ? (string)$ext['msgId'] : null;
        $this->brokerName = isset($ext['brokerName']) ? (string)$ext['brokerName'] : null;
    }
}

final class ResetOffsetRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?string $group = null,
        public ?int $timestamp = null,
        public ?bool $isForce = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'topic' => $this->topic,
            'group' => $this->group,
            'timestamp' => $this->timestamp,
            'isForce' => $this->isForce,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->group = isset($ext['group']) ? (string)$ext['group'] : null;
        $this->timestamp = self::l($ext['timestamp'] ?? null);
        $this->isForce = self::b($ext['isForce'] ?? null);
    }
}

// ---------------- 锁 ----------------

final class LockBatchMqRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $clientId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['consumerGroup' => $this->consumerGroup, 'clientId' => $this->clientId]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
        $this->clientId = isset($ext['clientId']) ? (string)$ext['clientId'] : null;
    }
}

final class UnlockBatchMqRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $clientId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['consumerGroup' => $this->consumerGroup, 'clientId' => $this->clientId]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
        $this->clientId = isset($ext['clientId']) ? (string)$ext['clientId'] : null;
    }
}

// ---------------- 事务 ----------------

/**
 * 对应 EndTransactionRequestHeader。
 *
 * ⚠ 继承 RpcRequestHeader 的 brokerName 字段在 Java 里**反射名是 ``bname``**
 * （setter 为 setBrokerName，但字段声明名是 bname）。写错键 broker 会静默丢字段。
 */
final class EndTransactionRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?string $producerGroup = null,
        public ?int $tranStateTableOffset = null,
        public ?int $commitLogOffset = null,
        public ?int $commitOrRollback = null,
        public ?bool $fromTransactionCheck = null,
        public ?string $msgId = null,
        public ?string $transactionId = null,
        public ?string $bname = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'topic' => $this->topic,
            'producerGroup' => $this->producerGroup,
            'tranStateTableOffset' => $this->tranStateTableOffset,
            'commitLogOffset' => $this->commitLogOffset,
            'commitOrRollback' => $this->commitOrRollback,
            'fromTransactionCheck' => $this->fromTransactionCheck,
            'msgId' => $this->msgId,
            'transactionId' => $this->transactionId,
            'bname' => $this->bname,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->producerGroup = isset($ext['producerGroup']) ? (string)$ext['producerGroup'] : null;
        $this->tranStateTableOffset = self::l($ext['tranStateTableOffset'] ?? null);
        $this->commitLogOffset = self::l($ext['commitLogOffset'] ?? null);
        $this->commitOrRollback = self::i($ext['commitOrRollback'] ?? null);
        $this->fromTransactionCheck = self::b($ext['fromTransactionCheck'] ?? null);
        $this->msgId = isset($ext['msgId']) ? (string)$ext['msgId'] : null;
        $this->transactionId = isset($ext['transactionId']) ? (string)$ext['transactionId'] : null;
        $this->bname = isset($ext['bname']) ? (string)$ext['bname'] : null;
    }
}

final class EndTransactionResponseHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $msgId = null,
        public ?string $transactionId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['msgId' => $this->msgId, 'transactionId' => $this->transactionId]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->msgId = isset($ext['msgId']) ? (string)$ext['msgId'] : null;
        $this->transactionId = isset($ext['transactionId']) ? (string)$ext['transactionId'] : null;
    }
}

/**
 * 对应 CheckTransactionStateRequestHeader。
 *
 * ⚠ 同样继承 RpcRequestHeader：brokerName 反射名是 ``bname``。
 */
final class CheckTransactionStateRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?int $tranStateTableOffset = null,
        public ?int $commitLogOffset = null,
        public ?string $msgId = null,
        public ?string $transactionId = null,
        public ?string $offsetMsgId = null,
        public ?string $bname = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'topic' => $this->topic,
            'tranStateTableOffset' => $this->tranStateTableOffset,
            'commitLogOffset' => $this->commitLogOffset,
            'msgId' => $this->msgId,
            'transactionId' => $this->transactionId,
            'offsetMsgId' => $this->offsetMsgId,
            'bname' => $this->bname,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->tranStateTableOffset = self::l($ext['tranStateTableOffset'] ?? null);
        $this->commitLogOffset = self::l($ext['commitLogOffset'] ?? null);
        $this->msgId = isset($ext['msgId']) ? (string)$ext['msgId'] : null;
        $this->transactionId = isset($ext['transactionId']) ? (string)$ext['transactionId'] : null;
        $this->offsetMsgId = isset($ext['offsetMsgId']) ? (string)$ext['offsetMsgId'] : null;
        $this->bname = isset($ext['bname']) ? (string)$ext['bname'] : null;
    }
}

final class CheckTransactionStateResponseHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $groupName = null,
        public ?int $transactionState = null,
        public ?int $offset = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'groupName' => $this->groupName,
            'transactionState' => $this->transactionState,
            'offset' => $this->offset,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->groupName = isset($ext['groupName']) ? (string)$ext['groupName'] : null;
        $this->transactionState = self::i($ext['transactionState'] ?? null);
        $this->offset = self::l($ext['offset'] ?? null);
    }
}

// ---------------- 管理 / 查询 ----------------

final class GetAllTopicConfigRequestHeader extends CommandCustomHeader
{
    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return [];
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
    }
}

final class GetAllTopicConfigResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?string $dataVersion = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['dataVersion' => $this->dataVersion]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->dataVersion = isset($ext['dataVersion']) ? (string)$ext['dataVersion'] : null;
    }
}

final class GetTopicConfigRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $topic = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
    }
}

/**
 * 对应 CreateTopicRequestHeader。
 *
 * ⚠ broker 的 checkFields() 会把 ``topicFilterType`` 解析成枚举，**为空直接抛
 * RemotingCommandException("topicFilterType = [null] value invalid")**，
 * 所以哪怕只想建普通 topic，也必须显式下发 topicFilterType。
 */
final class CreateTopicRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?string $defaultTopic = null,
        public ?int $readQueueNums = null,
        public ?int $writeQueueNums = null,
        public ?int $perm = null,
        public ?string $topicFilterType = null,
        public ?int $topicSysFlag = null,
        public ?bool $order = null,
        // AttributeParser.parseToString 格式："k1=v1,k2" （值为空时只留 key）
        public ?string $attributes = null,
        public ?bool $force = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'topic' => $this->topic,
            'defaultTopic' => $this->defaultTopic,
            'readQueueNums' => $this->readQueueNums,
            'writeQueueNums' => $this->writeQueueNums,
            'perm' => $this->perm,
            'topicFilterType' => $this->topicFilterType,
            'topicSysFlag' => $this->topicSysFlag,
            'order' => $this->order,
            'attributes' => $this->attributes,
            'force' => $this->force,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->defaultTopic = isset($ext['defaultTopic']) ? (string)$ext['defaultTopic'] : null;
        $this->readQueueNums = self::i($ext['readQueueNums'] ?? null);
        $this->writeQueueNums = self::i($ext['writeQueueNums'] ?? null);
        $this->perm = self::i($ext['perm'] ?? null);
        $this->topicFilterType = isset($ext['topicFilterType']) ? (string)$ext['topicFilterType'] : null;
        $this->topicSysFlag = self::i($ext['topicSysFlag'] ?? null);
        $this->order = self::b($ext['order'] ?? null);
    }
}

final class DeleteTopicRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $topic = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
    }
}

final class GetTopicStatsInfoRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $topic = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
    }
}

final class GetConsumeStatsRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $topic = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['consumerGroup' => $this->consumerGroup, 'topic' => $this->topic]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
    }
}

final class GetAllSubscriptionGroupConfigRequestHeader extends CommandCustomHeader
{
    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return [];
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
    }
}

final class GetSubscriptionGroupConfigRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $group = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['group' => $this->group]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->group = isset($ext['group']) ? (string)$ext['group'] : null;
    }
}

final class InterviewGetConsumerStatusRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $topic = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['consumerGroup' => $this->consumerGroup, 'topic' => $this->topic]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->consumerGroup = isset($ext['consumerGroup']) ? (string)$ext['consumerGroup'] : null;
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
    }
}

final class GetTopicsByClusterRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $cluster = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['cluster' => $this->cluster]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->cluster = isset($ext['cluster']) ? (string)$ext['cluster'] : null;
    }
}

final class GetBrokerConfigResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?string $version = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['version' => $this->version]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->version = isset($ext['version']) ? (string)$ext['version'] : null;
    }
}

final class GetTopicConfigResponseHeader extends CommandCustomHeader
{
    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return [];
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
    }
}

final class GetSubscriptionGroupResponseHeader extends CommandCustomHeader
{
    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return [];
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
    }
}

final class GetTopicListResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?string $brokerAddr = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['brokerAddr' => $this->brokerAddr]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->brokerAddr = isset($ext['brokerAddr']) ? (string)$ext['brokerAddr'] : null;
    }
}

// ---------------- namesrv ----------------

final class RegisterBrokerRequestHeader extends CommandCustomHeader
{
    public bool $compressed = false;
    public int $bodyCrc32 = 0;

    public function __construct(
        public ?string $brokerName = null,
        public ?string $brokerAddr = null,
        public ?string $clusterName = null,
        public ?string $haServerAddr = null,
        public ?int $brokerId = null,
        public ?int $heartbeatTimeoutMillis = null,
        public ?bool $enableActingMaster = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'brokerName' => $this->brokerName,
            'brokerAddr' => $this->brokerAddr,
            'clusterName' => $this->clusterName,
            'haServerAddr' => $this->haServerAddr,
            'brokerId' => $this->brokerId,
            'heartbeatTimeoutMillis' => $this->heartbeatTimeoutMillis,
            'enableActingMaster' => $this->enableActingMaster,
            'compressed' => $this->compressed,
            'bodyCrc32' => $this->bodyCrc32,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->brokerName = isset($ext['brokerName']) ? (string)$ext['brokerName'] : null;
        $this->brokerAddr = isset($ext['brokerAddr']) ? (string)$ext['brokerAddr'] : null;
        $this->clusterName = isset($ext['clusterName']) ? (string)$ext['clusterName'] : null;
        $this->haServerAddr = isset($ext['haServerAddr']) ? (string)$ext['haServerAddr'] : null;
        $this->brokerId = self::i($ext['brokerId'] ?? null);
        $this->heartbeatTimeoutMillis = self::i($ext['heartbeatTimeoutMillis'] ?? null);
        $ea = $ext['enableActingMaster'] ?? null;
        $this->enableActingMaster = $ea !== null ? (bool)$ea : null;
        $this->compressed = ($ext['compressed'] ?? null) !== null ? (bool)$ext['compressed'] : false;
        $this->bodyCrc32 = (int)(($ext['bodyCrc32'] ?? 0) ?: 0);
    }
}

final class RegisterBrokerResponseHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $haServerAddr = null,
        public ?string $masterAddr = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['haServerAddr' => $this->haServerAddr, 'masterAddr' => $this->masterAddr]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->haServerAddr = isset($ext['haServerAddr']) ? (string)$ext['haServerAddr'] : null;
        $this->masterAddr = isset($ext['masterAddr']) ? (string)$ext['masterAddr'] : null;
    }
}

final class UnRegisterBrokerRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $brokerName = null,
        public ?string $brokerAddr = null,
        public ?string $clusterName = null,
        public ?int $brokerId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'brokerName' => $this->brokerName,
            'brokerAddr' => $this->brokerAddr,
            'clusterName' => $this->clusterName,
            'brokerId' => $this->brokerId,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->brokerName = isset($ext['brokerName']) ? (string)$ext['brokerName'] : null;
        $this->brokerAddr = isset($ext['brokerAddr']) ? (string)$ext['brokerAddr'] : null;
        $this->clusterName = isset($ext['clusterName']) ? (string)$ext['clusterName'] : null;
        $this->brokerId = self::i($ext['brokerId'] ?? null);
    }
}

final class GetRouteInfoRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?bool $acceptStandardJsonOnly = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic, 'acceptStandardJsonOnly' => $this->acceptStandardJsonOnly]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $v = $ext['acceptStandardJsonOnly'] ?? null;
        $this->acceptStandardJsonOnly = $v !== null ? (bool)$v : null;
    }
}

final class PutKVConfigRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $namespace = null,
        public ?string $key = null,
        public ?string $value = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['namespace' => $this->namespace, 'key' => $this->key, 'value' => $this->value]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->namespace = isset($ext['namespace']) ? (string)$ext['namespace'] : null;
        $this->key = isset($ext['key']) ? (string)$ext['key'] : null;
        $this->value = isset($ext['value']) ? (string)$ext['value'] : null;
    }
}

final class GetKVConfigRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $namespace = null,
        public ?string $key = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['namespace' => $this->namespace, 'key' => $this->key]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->namespace = isset($ext['namespace']) ? (string)$ext['namespace'] : null;
        $this->key = isset($ext['key']) ? (string)$ext['key'] : null;
    }
}

final class GetKVConfigResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?string $value = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['value' => $this->value]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->value = isset($ext['value']) ? (string)$ext['value'] : null;
    }
}

final class DeleteKVConfigRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $namespace = null,
        public ?string $key = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['namespace' => $this->namespace, 'key' => $this->key]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->namespace = isset($ext['namespace']) ? (string)$ext['namespace'] : null;
        $this->key = isset($ext['key']) ? (string)$ext['key'] : null;
    }
}

final class GetKVListByNamespaceRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $namespace = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['namespace' => $this->namespace]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->namespace = isset($ext['namespace']) ? (string)$ext['namespace'] : null;
    }
}

final class RegisterTopicRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $topic = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
    }
}

final class RegisterOrderTopicRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?string $orderTopicString = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic, 'orderTopicString' => $this->orderTopicString]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->orderTopicString = isset($ext['orderTopicString']) ? (string)$ext['orderTopicString'] : null;
    }
}

final class DeleteTopicFromNamesrvRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $topic = null,
        public ?string $clusterName = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['topic' => $this->topic, 'clusterName' => $this->clusterName]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->topic = isset($ext['topic']) ? (string)$ext['topic'] : null;
        $this->clusterName = isset($ext['clusterName']) ? (string)$ext['clusterName'] : null;
    }
}

final class GetBrokerMemberGroupRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $clusterName = null,
        public ?string $brokerName = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['clusterName' => $this->clusterName, 'brokerName' => $this->brokerName]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->clusterName = isset($ext['clusterName']) ? (string)$ext['clusterName'] : null;
        $this->brokerName = isset($ext['brokerName']) ? (string)$ext['brokerName'] : null;
    }
}

final class WipeWritePermOfBrokerRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $brokerName = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['brokerName' => $this->brokerName]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->brokerName = isset($ext['brokerName']) ? (string)$ext['brokerName'] : null;
    }
}

final class WipeWritePermOfBrokerResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?int $wipeTopicCount = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['wipeTopicCount' => $this->wipeTopicCount]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->wipeTopicCount = self::i($ext['wipeTopicCount'] ?? null);
    }
}

final class AddWritePermOfBrokerRequestHeader extends CommandCustomHeader
{
    public function __construct(public ?string $brokerName = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['brokerName' => $this->brokerName]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->brokerName = isset($ext['brokerName']) ? (string)$ext['brokerName'] : null;
    }
}

final class AddWritePermOfBrokerResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?int $addTopicCount = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['addTopicCount' => $this->addTopicCount]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->addTopicCount = self::i($ext['addTopicCount'] ?? null);
    }
}

final class BrokerHeartbeatRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $clusterName = null,
        public ?string $brokerAddr = null,
        public ?string $brokerName = null,
        public ?int $brokerId = null,
        public ?int $epoch = null,
        public ?int $maxOffset = null,
        public ?int $confirmOffset = null,
        public ?int $heartbeatTimeoutMills = null,
        public ?int $electionPriority = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'clusterName' => $this->clusterName,
            'brokerAddr' => $this->brokerAddr,
            'brokerName' => $this->brokerName,
            'brokerId' => $this->brokerId,
            'epoch' => $this->epoch,
            'maxOffset' => $this->maxOffset,
            'confirmOffset' => $this->confirmOffset,
            'heartbeatTimeoutMills' => $this->heartbeatTimeoutMills,
            'electionPriority' => $this->electionPriority,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->clusterName = isset($ext['clusterName']) ? (string)$ext['clusterName'] : null;
        $this->brokerAddr = isset($ext['brokerAddr']) ? (string)$ext['brokerAddr'] : null;
        $this->brokerName = isset($ext['brokerName']) ? (string)$ext['brokerName'] : null;
        $this->brokerId = self::i($ext['brokerId'] ?? null);
        $this->epoch = self::i($ext['epoch'] ?? null);
        $this->maxOffset = self::i($ext['maxOffset'] ?? null);
        $this->confirmOffset = self::i($ext['confirmOffset'] ?? null);
        $this->heartbeatTimeoutMills = self::i($ext['heartbeatTimeoutMills'] ?? null);
        $this->electionPriority = self::i($ext['electionPriority'] ?? null);
    }
}

final class QueryDataVersionRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $brokerName = null,
        public ?string $brokerAddr = null,
        public ?string $clusterName = null,
        public ?int $brokerId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'brokerName' => $this->brokerName,
            'brokerAddr' => $this->brokerAddr,
            'clusterName' => $this->clusterName,
            'brokerId' => $this->brokerId,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->brokerName = isset($ext['brokerName']) ? (string)$ext['brokerName'] : null;
        $this->brokerAddr = isset($ext['brokerAddr']) ? (string)$ext['brokerAddr'] : null;
        $this->clusterName = isset($ext['clusterName']) ? (string)$ext['clusterName'] : null;
        $this->brokerId = self::i($ext['brokerId'] ?? null);
    }
}

final class QueryDataVersionResponseHeader extends CommandCustomHeader
{
    public function __construct(public ?bool $changed = null)
    {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext(['changed' => $this->changed]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $v = $ext['changed'] ?? null;
        $this->changed = $v !== null ? (bool)$v : null;
    }
}

// ---------------- POP 模式（5.x 轻量消费） ----------------
//
// 另外注意 `order` / `suspend` 这两个字段 Java 用的是**非空** Boolean/boolean
// （`Boolean order = Boolean.FALSE`、`private boolean suspend = false`），
// `encodeHeader` 只会跳过 null，所以它们**总是**出现在报文里 —— 这里也不做 null 过滤。

/** Java ``PopMessageRequestHeader``（RequestCode.POP_MESSAGE = 200050）。 */
final class PopMessageRequestHeader extends CommandCustomHeader
{
    public bool $order = false;

    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $topic = null,
        public ?int $queueId = null,
        public ?int $maxMsgNums = null,
        public ?int $invisibleTime = null,
        public ?int $pollTime = null,
        // bornTime 必须是**当前毫秒时间戳**：broker 校验
        // `now - bornTime - pollTime > 500` 就直接回 POLLING_TIMEOUT(210)。
        public ?int $bornTime = null,
        // 0 = MIN（从最小位点开始，能消费历史），1 = MAX（只拿新消息）
        public ?int $initMode = null,
        public ?string $expType = null,
        public ?string $exp = null,
        public ?string $attemptId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'consumerGroup' => $this->consumerGroup,
            'topic' => $this->topic,
            'queueId' => $this->queueId,
            'maxMsgNums' => $this->maxMsgNums,
            'invisibleTime' => $this->invisibleTime,
            'pollTime' => $this->pollTime,
            'bornTime' => $this->bornTime,
            'initMode' => $this->initMode,
            'expType' => $this->expType,
            'exp' => $this->exp,
            'order' => $this->order,
            'attemptId' => $this->attemptId,
        ]);
    }
}

/**
 * Java ``PopMessageResponseHeader``。
 *
 * ``startOffsetInfo`` / ``msgOffsetInfo`` / ``orderCountInfo`` 是编码过的
 * 字符串（空格做字段分隔、分号做队列分隔），用 ExtraInfoUtil 解析。
 */
final class PopMessageResponseHeader extends CommandCustomHeader
{
    public function __construct(
        public ?int $popTime = null,
        public ?int $invisibleTime = null,
        public ?int $reviveQid = null,
        public ?int $restNum = null,
        public ?string $startOffsetInfo = null,
        public ?string $msgOffsetInfo = null,
        public ?string $orderCountInfo = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'popTime' => $this->popTime,
            'invisibleTime' => $this->invisibleTime,
            'reviveQid' => $this->reviveQid,
            'restNum' => $this->restNum,
            'startOffsetInfo' => $this->startOffsetInfo,
            'msgOffsetInfo' => $this->msgOffsetInfo,
            'orderCountInfo' => $this->orderCountInfo,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->popTime = self::l($ext['popTime'] ?? null);
        $this->invisibleTime = self::l($ext['invisibleTime'] ?? null);
        $this->reviveQid = self::i($ext['reviveQid'] ?? null);
        $this->restNum = self::l($ext['restNum'] ?? null);
        $this->startOffsetInfo = isset($ext['startOffsetInfo']) ? (string)$ext['startOffsetInfo'] : null;
        $this->msgOffsetInfo = isset($ext['msgOffsetInfo']) ? (string)$ext['msgOffsetInfo'] : null;
        $this->orderCountInfo = isset($ext['orderCountInfo']) ? (string)$ext['orderCountInfo'] : null;
    }
}

/**
 * Java ``AckMessageRequestHeader``（RequestCode.ACK_MESSAGE = 200051）。
 *
 * ``offset`` 是 **consumeQueue offset**（即 CK 串第 8 段 / msgQueueOffset），
 * 不是 commitlog offset —— 传错会被 broker 回 NO_MESSAGE。
 */
final class AckMessageRequestHeader extends CommandCustomHeader
{
    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $topic = null,
        public ?int $queueId = null,
        public ?string $extraInfo = null,
        public ?int $offset = null,
        public ?string $liteTopic = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'consumerGroup' => $this->consumerGroup,
            'topic' => $this->topic,
            'queueId' => $this->queueId,
            'extraInfo' => $this->extraInfo,
            'offset' => $this->offset,
            'liteTopic' => $this->liteTopic,
        ]);
    }
}

/** Java ``ChangeInvisibleTimeRequestHeader``（CHANGE_MESSAGE_INVISIBLETIME = 200053）。 */
final class ChangeInvisibleTimeRequestHeader extends CommandCustomHeader
{
    public bool $suspend = false;

    public function __construct(
        public ?string $consumerGroup = null,
        public ?string $topic = null,
        public ?int $queueId = null,
        public ?string $extraInfo = null,
        public ?int $offset = null,
        public ?int $invisibleTime = null,
        public ?string $liteTopic = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'consumerGroup' => $this->consumerGroup,
            'topic' => $this->topic,
            'queueId' => $this->queueId,
            'extraInfo' => $this->extraInfo,
            'offset' => $this->offset,
            'invisibleTime' => $this->invisibleTime,
            'liteTopic' => $this->liteTopic,
            'suspend' => $this->suspend,
        ]);
    }
}

/**
 * Java ``ChangeInvisibleTimeResponseHeader``。
 *
 * 返回的是**新的** ``invisibleTime`` / ``popTime`` / ``reviveQid``；
 * 客户端要用它们重建 extraInfo 供后续 ACK 使用。
 */
final class ChangeInvisibleTimeResponseHeader extends CommandCustomHeader
{
    public function __construct(
        public ?int $popTime = null,
        public ?int $invisibleTime = null,
        public ?int $reviveQid = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toExtFields(): array
    {
        return self::ext([
            'popTime' => $this->popTime,
            'invisibleTime' => $this->invisibleTime,
            'reviveQid' => $this->reviveQid,
        ]);
    }

    /** @param array<string, mixed> $ext */
    public function fromExtFields(array $ext): void
    {
        $this->popTime = self::l($ext['popTime'] ?? null);
        $this->invisibleTime = self::l($ext['invisibleTime'] ?? null);
        $this->reviveQid = self::i($ext['reviveQid'] ?? null);
    }
}
