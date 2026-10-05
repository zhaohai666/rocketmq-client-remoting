<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\InnerIdGenerator;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageType;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\UtilAll;

/**
 * 消息轨迹（对应 org.apache.rocketmq.client.trace 包 + client.AccessChannel，
 * 移植自 python/client/trace.py）。
 *
 * 包含：
 *   - TraceConstants    —— 常量（org.apache.rocketmq.client.trace.TraceConstants）
 *   - TraceType         —— Pub / Recall / SubBefore / SubAfter / EndTransaction
 *   - AccessChannel     —— LOCAL / CLOUD（org.apache.rocketmq.client.AccessChannel）
 *   - TraceBean         —— 轨迹里被追踪的那条消息
 *   - TraceContext      —— 一次追踪上下文（Pub/SubBefore/... 各一份）
 *   - TraceTransferBean —— 编码结果：transData + transKey
 *   - TraceDataEncoder  —— 文本编解码（与 Java 逐字节一致）
 *
 * ⚠ 两个必须保留的 Java 语义差异（否则编解码对不上）：
 *   1. CONTENT_SPLITOR = \x01、FIELD_SPLITOR = \x02，编码时**每条记录末尾补
 *      FIELD_SPLITOR**；解码用 Java String.split 语义（**丢弃末尾空串**）切分
 *      —— 见 TraceDataEncoder::javaSplit()。
 *   2. PHP 的 explode 与 Python 的 str.split 一样保留末尾空串，直接切会多出一个
 *      空段并把字段整体错位。本模块一律走 javaSplit()。
 */
final class TraceConstants
{
    /** 对应 org.apache.rocketmq.client.trace.TraceConstants。 */
    public const GROUP_NAME_PREFIX = '_INNER_TRACE_PRODUCER';
    public const CONTENT_SPLITOR = "\x01";
    public const FIELD_SPLITOR = "\x02";
    public const TRACE_INSTANCE_NAME = 'PID_CLIENT_INNER_TRACE_PRODUCER';
    public const TRACE_TOPIC_PREFIX = 'rmq_sys_TRACE_DATA_';
    public const TO_PREFIX = 'To_';
    public const FROM_PREFIX = 'From_';
    public const END_TRANSACTION = 'EndTransaction';

    public const ROCKETMQ_SERVICE = 'rocketmq';
    public const ROCKETMQ_SUCCESS = 'rocketmq.success';
    public const ROCKETMQ_TAGS = 'rocketmq.tags';
    public const ROCKETMQ_KEYS = 'rocketmq.keys';
    public const ROCKETMQ_STORE_HOST = 'rocketmq.store_host';
    public const ROCKETMQ_BODY_LENGTH = 'rocketmq.body_length';
    public const ROCKETMQ_MSG_ID = 'rocketmq.mgs_id';
    public const ROCKETMQ_MSG_TYPE = 'rocketmq.mgs_type';
    public const ROCKETMQ_REGION_ID = 'rocketmq.region_id';
    public const ROCKETMQ_TRANSACTION_ID = 'rocketmq.transaction_id';
    public const ROCKETMQ_TRANSACTION_STATE = 'rocketmq.transaction_state';
    public const ROCKETMQ_IS_FROM_TRANSACTION_CHECK = 'rocketmq.is_from_transaction_check';
    public const ROCKETMQ_RETRY_TIMERS = 'rocketmq.retry_times';
}

/**
 * 对应 org.apache.rocketmq.client.trace.TraceType（枚举名即线上字段第 1 段）。
 */
enum TraceType: string
{
    case PUB = 'Pub';
    case RECALL = 'Recall';
    case SUB_BEFORE = 'SubBefore';
    case SUB_AFTER = 'SubAfter';
    case END_TRANSACTION = 'EndTransaction';
}

/**
 * 对应 org.apache.rocketmq.client.AccessChannel。
 *
 * 只在 SubAfter 编码时起作用：非 CLOUD 才追加 timestamp + groupName 两段
 * （Java TraceDataEncoder:208）。
 */
enum AccessChannel: string
{
    case LOCAL = 'LOCAL';
    case CLOUD = 'CLOUD';
}

/**
 * 对应 org.apache.rocketmq.client.trace.TraceBean。
 */
final class TraceBean
{
    public string $topic = '';
    public string $msgId = '';
    public string $offsetMsgId = '';
    public string $tags = '';
    public string $keys = '';
    public string $storeHost;
    public string $clientHost;
    public int $storeTime = 0;
    public int $retryTimes = 0;
    public int $bodyLength = 0;
    public ?MessageType $msgType = null;
    /** LocalTransactionState（枚举或裸串）；编码时取其 name（见 encoder）。 */
    public mixed $transactionState = null;
    public ?string $transactionId = null;
    public bool $fromTransactionCheck = false;

    public function __construct()
    {
        // 对应 Java TraceBean 静态块里的 LOCAL_ADDRESS（storeHost/clientHost 默认值）
        $local = self::localAddress();
        $this->storeHost = $local;
        $this->clientHost = $local;
    }

    /**
     * 对应 Java TraceBean 静态块里的 LOCAL_ADDRESS，移植自 trace.py 的 _local_address()。
     *
     * IPv4 直接用；非 IPv4 时按 Java 的 inet_pton(AF_INET6) 形态逐 2 字节 hex 以 ':' 连接
     * （这不是标准 IPv6 记法，但与该字段的既有线上形态一致）；失败则原样返回。
     */
    public static function localAddress(): string
    {
        static $cached = null;
        if ($cached !== null) {
            return $cached;
        }
        $ip = MixAll::getIpStr();
        if (UtilAll::isIpv4($ip)) {
            return $cached = $ip;
        }
        try {
            $packed = @inet_pton($ip);
            if ($packed === false) {
                throw new \ValueError(sprintf('invalid address: %s', $ip));
            }
            $parts = [];
            for ($i = 0; $i < 16; $i += 2) {
                $parts[] = bin2hex(substr($packed, $i, 2));
            }
            return $cached = implode(':', $parts);
        } catch (\Throwable) {
            return $cached = $ip;
        }
    }
}

/**
 * 对应 org.apache.rocketmq.client.trace.TraceContext。
 *
 * requestId 默认取 InnerIdGenerator::createUniqId()，与 Java 一致 ——
 * SubBefore 与 SubAfter 共用一个 requestId，是控制台把一次消费前后串起来的关键。
 */
class TraceContext
{
    public ?TraceType $traceType = null;
    public int $timeStamp;
    public string $regionId = '';
    public string $regionName = '';
    public string $groupName = '';
    public int $costTime = 0;
    public bool $isSuccess = true;
    public string $requestId;
    public int $contextCode = 0;
    public ?AccessChannel $accessChannel = null;
    /** @var list<TraceBean> */
    public array $traceBeans = [];

    public function __construct()
    {
        $this->timeStamp = UtilAll::currentTimeMillis();
        $this->requestId = InnerIdGenerator::createUniqId();
    }

    /** Java 属性名 accessChannel/isSuccess 的别名，便于移植时逐字对照。 */
    public function accessChannelOrLocal(): AccessChannel
    {
        return $this->accessChannel ?? AccessChannel::LOCAL;
    }

    /** 对应 Java TraceContext.toString。 */
    public function __toString(): string
    {
        $beans = '';
        foreach ($this->traceBeans as $b) {
            $beans .= sprintf('%s_%s_', $b->msgId, $b->topic);
        }
        return sprintf(
            'TraceContext{%s_%s_%s_%s_%s}',
            $this->traceType?->value ?? '',
            $this->groupName,
            $this->regionId,
            $this->isSuccess ? 'True' : 'False',
            $beans
        );
    }
}

/**
 * 对应 org.apache.rocketmq.client.trace.TraceTransferBean。
 *
 * transKey 是集合语义（Python set），这里用 `array<string,true>` 表达。
 */
final class TraceTransferBean
{
    public string $transData = '';
    /** @var array<string,true> */
    public array $transKey = [];
}

/**
 * 对应 org.apache.rocketmq.client.trace.TraceDataEncoder。
 *
 * 编码结果的字段顺序**逐字节对齐 Java**，回归守卫是 Python/Java 官方实现打印出来的
 * 固定字符串（见自测 RunClientTrace.php 的 EXPECTED_* 向量）。
 */
final class TraceDataEncoder
{
    /**
     * 复刻 Java String.split：**丢弃末尾的空串**（移植自 trace.py 的 java_split）。
     *
     * PHP 的 explode 与 Python 的 str.split 一样保留末尾空串，而编码结果末尾正好补了
     * FIELD_SPLITOR，所以直接切会多出一段空记录；对内容段（CONTENT_SPLITOR）同理。
     *
     * @return list<string>
     */
    public static function javaSplit(string $value, string $sep): array
    {
        $parts = explode($sep, $value);
        while ($parts !== [] && end($parts) === '') {
            array_pop($parts);
        }
        return $parts;
    }

    /**
     * 把线上轨迹文本解回 TraceContext 列表（对应 Java decoderFromTraceDataString）。
     *
     * @return list<TraceContext>
     */
    public static function decoderFromTraceDataString(?string $traceData): array
    {
        $res = [];
        if ($traceData === null || $traceData === '') {
            return $res;
        }
        foreach (self::javaSplit($traceData, TraceConstants::FIELD_SPLITOR) as $context) {
            if ($context === '') {
                continue;
            }
            try {
                $ctx = self::decodeContext($context);
            } catch (\Throwable $e) {
                // 有意偏离 Java（Java 此处会把异常抛给调用方，整条轨迹消息全丢）：
                // 单条坏记录只跳过自己，其余记录照常解出。
                Logger::warning(sprintf('decode trace context failed: %s, context=%s', $e->getMessage(), $context));
                continue;
            }
            if ($ctx !== null) {
                $res[] = $ctx;
            }
        }
        return $res;
    }

    /** 解一条轨迹记录（一段 FIELD_SPLITOR 之内）。无法识别时返回 null。 */
    private static function decodeContext(string $context): ?TraceContext
    {
        $line = self::javaSplit($context, TraceConstants::CONTENT_SPLITOR);
        if ($line === []) {
            return null;
        }
        $kind = $line[0];
        if ($kind === TraceType::PUB->value) {
            $ctx = new TraceContext();
            $ctx->traceType = TraceType::PUB;
            $ctx->timeStamp = (int) self::at($line, 1);
            $ctx->regionId = self::at($line, 2);
            $ctx->groupName = self::at($line, 3);
            $bean = new TraceBean();
            $bean->topic = self::at($line, 4);
            $bean->msgId = self::at($line, 5);
            $bean->tags = self::at($line, 6);
            $bean->keys = self::at($line, 7);
            $bean->storeHost = self::at($line, 8);
            $bean->bodyLength = (int) self::at($line, 9);
            $ctx->costTime = (int) self::at($line, 10);
            // MessageType::from 对越界值抛 ValueError（与 Python MessageType(int) 一致），
            // 由 decoderFromTraceDataString 捕获后跳过该条记录。
            $bean->msgType = MessageType::from((int) self::at($line, 11));
            $n = count($line);
            if ($n === 13) {                                        // 老版本：无 offsetMsgId
                $ctx->isSuccess = self::at($line, 12) === 'true';
            } elseif ($n === 14) {
                $bean->offsetMsgId = self::at($line, 12);
                $ctx->isSuccess = self::at($line, 13) === 'true';
            }
            if ($n >= 15) {                                         // 兼容更老版本
                $bean->offsetMsgId = self::at($line, 12);
                $ctx->isSuccess = self::at($line, 13) === 'true';
                $bean->clientHost = self::at($line, 14);
            }
            $ctx->traceBeans = [$bean];
            return $ctx;
        }
        if ($kind === TraceType::SUB_BEFORE->value) {
            $ctx = new TraceContext();
            $ctx->traceType = TraceType::SUB_BEFORE;
            $ctx->timeStamp = (int) self::at($line, 1);
            $ctx->regionId = self::at($line, 2);
            $ctx->groupName = self::at($line, 3);
            $ctx->requestId = self::at($line, 4);
            $bean = new TraceBean();
            $bean->msgId = self::at($line, 5);
            $bean->retryTimes = (int) self::at($line, 6);
            // 无 keys 的消息会缺这段，见 atOr() 注释
            $bean->keys = self::atOr($line, 7);
            $ctx->traceBeans = [$bean];
            return $ctx;
        }
        if ($kind === TraceType::SUB_AFTER->value) {
            $ctx = new TraceContext();
            $ctx->traceType = TraceType::SUB_AFTER;
            $ctx->requestId = self::at($line, 1);
            $bean = new TraceBean();
            $bean->msgId = self::at($line, 2);
            $bean->keys = self::at($line, 5);
            $ctx->traceBeans = [$bean];
            $ctx->costTime = (int) self::at($line, 3);
            $ctx->isSuccess = self::at($line, 4) === 'true';
            $n = count($line);
            if ($n >= 7) {
                $ctx->contextCode = (int) self::at($line, 6);
            }
            if ($n >= 9) {                                          // 兼容老版本
                $ctx->timeStamp = (int) self::at($line, 7);
                $ctx->groupName = self::at($line, 8);
            }
            return $ctx;
        }
        if ($kind === TraceType::END_TRANSACTION->value) {
            $ctx = new TraceContext();
            $ctx->traceType = TraceType::END_TRANSACTION;
            $ctx->timeStamp = (int) self::at($line, 1);
            $ctx->regionId = self::at($line, 2);
            $ctx->groupName = self::at($line, 3);
            $bean = new TraceBean();
            $bean->topic = self::at($line, 4);
            $bean->msgId = self::at($line, 5);
            $bean->tags = self::at($line, 6);
            $bean->keys = self::at($line, 7);
            $bean->storeHost = self::at($line, 8);
            $bean->msgType = MessageType::from((int) self::at($line, 9));
            $bean->transactionId = self::at($line, 10);
            // 解码存**原始字符串**（Java 存 LocalTransactionState 枚举名，两边都取字符串形态）
            $bean->transactionState = self::at($line, 11);
            $bean->fromTransactionCheck = self::at($line, 12) === 'true';
            $ctx->traceBeans = [$bean];
            return $ctx;
        }
        if ($kind === TraceType::RECALL->value) {
            $ctx = new TraceContext();
            $ctx->traceType = TraceType::RECALL;
            $ctx->timeStamp = (int) self::at($line, 1);
            $ctx->regionId = self::at($line, 2);
            $ctx->groupName = self::at($line, 3);
            $bean = new TraceBean();
            $bean->topic = self::at($line, 4);
            $bean->msgId = self::at($line, 5);
            $ctx->isSuccess = self::at($line, 6) === 'true';
            $ctx->traceBeans = [$bean];
            return $ctx;
        }
        return null;
    }

    /**
     * 按下标取段；越界抛异常（对齐 Java/Python 直接 line[i] 的语义）。
     *
     * 这条抛异常是**有意**的：decoderFromTraceDataString 靠它把段数不足的坏记录
     * 跳过（Python 参考实现同样抛 IndexError）。
     */
    private static function at(array $line, int $index): string
    {
        if (!array_key_exists($index, $line)) {
            throw new \OutOfRangeException(sprintf('trace field index %d out of range', $index));
        }
        return $line[$index];
    }

    /**
     * 按下标取段，越界返回 ''。
     *
     * ⚠ 这是**有意偏离 Java** 的一处健壮性处理：Java decoderFromTraceDataString
     * 直接 line[7]，而 SubBefore 的加密串形如
     * `SubBefore\x01ts\x01region\x01group\x01reqId\x01msgId\x01retryTimes\x01<keys>\x01\x02`：
     * 当被消费的消息**没有 keys** 时，<keys> 为空，Java String.split 会连同末尾的
     * CONTENT_SPLITOR 一起丢弃 → 数组只剩 7 段 → line[7] 抛
     * ArrayIndexOutOfBoundsException。轨迹读取方不该因为一条无 key 的合法记录就崩，
     * 所以这里改为「缺段当空串」，其余字段顺序与 Java 逐字保持一致。
     */
    private static function atOr(array $line, int $index): string
    {
        return $line[$index] ?? '';
    }

    /**
     * 把 TraceContext 编成可发送的文本（对应 Java encoderFromContextBean）。
     */
    public static function encoderFromContextBean(?TraceContext $ctx): ?TraceTransferBean
    {
        if ($ctx === null) {
            return null;
        }
        $SOH = TraceConstants::CONTENT_SPLITOR;
        $STX = TraceConstants::FIELD_SPLITOR;
        $tb = new TraceTransferBean();
        $t = $ctx->traceType;
        if ($t === TraceType::PUB) {
            $bean = self::firstBean($ctx);
            $sb = [
                $t->value, (string) $ctx->timeStamp, $ctx->regionId, $ctx->groupName,
                $bean->topic, $bean->msgId, $bean->tags, $bean->keys,
                $bean->storeHost, (string) $bean->bodyLength, (string) $ctx->costTime,
                (string) ($bean->msgType?->value ?? 0),
                $bean->offsetMsgId, self::boolStr($ctx->isSuccess),
            ];
            $tb->transData = implode($SOH, $sb) . $STX;
        } elseif ($t === TraceType::SUB_BEFORE) {
            foreach ($ctx->traceBeans as $bean) {
                $sb = [
                    $t->value, (string) $ctx->timeStamp, $ctx->regionId, $ctx->groupName,
                    $ctx->requestId, $bean->msgId, (string) $bean->retryTimes, $bean->keys,
                ];
                $tb->transData .= implode($SOH, $sb) . $STX;
            }
        } elseif ($t === TraceType::SUB_AFTER) {
            foreach ($ctx->traceBeans as $bean) {
                $sb = [
                    $t->value, $ctx->requestId, $bean->msgId, (string) $ctx->costTime,
                    self::boolStr($ctx->isSuccess), $bean->keys, (string) $ctx->contextCode,
                ];
                // Java：非 CLOUD 才补 timestamp + groupName（accessChannel 为 null 时
                // Java 会 NPE，这里按 LOCAL 处理 —— 唯一一处有意的健壮性偏离）
                if ($ctx->accessChannelOrLocal() !== AccessChannel::CLOUD) {
                    $sb[] = (string) $ctx->timeStamp;
                    $sb[] = $ctx->groupName;
                }
                $tb->transData .= implode($SOH, $sb) . $STX;
            }
        } elseif ($t === TraceType::END_TRANSACTION) {
            $bean = self::firstBean($ctx);
            $sb = [
                $t->value, (string) $ctx->timeStamp, $ctx->regionId, $ctx->groupName,
                $bean->topic, $bean->msgId, $bean->tags, $bean->keys, $bean->storeHost,
                (string) ($bean->msgType?->value ?? 0),
                $bean->transactionId ?? '', self::stateName($bean->transactionState),
                self::boolStr($bean->fromTransactionCheck),
            ];
            $tb->transData = implode($SOH, $sb) . $STX;
        } elseif ($t === TraceType::RECALL) {
            $bean = self::firstBean($ctx);
            $sb = [
                $t->value, (string) $ctx->timeStamp, $ctx->regionId, $ctx->groupName,
                $bean->topic, $bean->msgId, self::boolStr($ctx->isSuccess),
            ];
            $tb->transData = implode($SOH, $sb) . $STX;
        }
        // 收集 keys：msgId + 按空格拆开的业务 keys（Java split(KEY_SEPARATOR)）
        foreach ($ctx->traceBeans as $bean) {
            $tb->transKey[$bean->msgId] = true;
            if ($bean->keys !== '') {
                foreach (explode(MessageConst::KEY_SEPARATOR, $bean->keys) as $k) {
                    $tb->transKey[$k] = true;
                }
            }
        }
        return $tb;
    }

    private static function firstBean(TraceContext $ctx): TraceBean
    {
        if ($ctx->traceBeans === []) {
            throw new \OutOfRangeException('traceBeans is empty');
        }
        return $ctx->traceBeans[0];
    }

    private static function boolStr(bool $b): string
    {
        return $b ? 'true' : 'false';
    }

    /**
     * 事务状态名（Java getTransactionState().name()；Python getattr(state,"name",None) or str(state)）。
     */
    private static function stateName(mixed $state): string
    {
        if ($state instanceof \UnitEnum) {
            return $state->name;
        }
        if ($state === null) {
            // 对齐 Python 的 str(None)（Java 此处会 NPE，但编码前必有状态）
            return 'None';
        }
        if (is_object($state) && isset($state->name) && is_string($state->name)) {
            return $state->name;
        }
        return (string) $state;
    }
}
