<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\InnerIdGenerator;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageType;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\UtilAll;

final class TraceConstants
{
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

enum TraceType: string
{
    case PUB = 'Pub';
    case RECALL = 'Recall';
    case SUB_BEFORE = 'SubBefore';
    case SUB_AFTER = 'SubAfter';
    case END_TRANSACTION = 'EndTransaction';
}

enum AccessChannel: string
{
    case LOCAL = 'LOCAL';
    case CLOUD = 'CLOUD';
}

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
    /** LocalTransactionState (enum or bare string); take its name when encoding (see encoder). */
    public mixed $transactionState = null;
    public ?string $transactionId = null;
    public bool $fromTransactionCheck = false;

    public function __construct()
    {
        $local = self::localAddress();
        $this->storeHost = $local;
        $this->clientHost = $local;
    }

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

    public function accessChannelOrLocal(): AccessChannel
    {
        return $this->accessChannel ?? AccessChannel::LOCAL;
    }

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

final class TraceTransferBean
{
    public string $transData = '';
    /** @var array<string,true> */
    public array $transKey = [];
}

final class TraceDataEncoder
{
    public static function wireSplit(string $value, string $sep): array
    {
        $parts = explode($sep, $value);
        while ($parts !== [] && end($parts) === '') {
            array_pop($parts);
        }
        return $parts;
    }

    public static function decoderFromTraceDataString(?string $traceData): array
    {
        $res = [];
        if ($traceData === null || $traceData === '') {
            return $res;
        }
        foreach (self::wireSplit($traceData, TraceConstants::FIELD_SPLITOR) as $context) {
            if ($context === '') {
                continue;
            }
            try {
                $ctx = self::decodeContext($context);
            } catch (\Throwable $e) {
                Logger::warning(sprintf('decode trace context failed: %s, context=%s', $e->getMessage(), $context));
                continue;
            }
            if ($ctx !== null) {
                $res[] = $ctx;
            }
        }
        return $res;
    }

    /** Decode one trace record (within one FIELD_SPLITOR segment). Returns null when unrecognizable. */
    private static function decodeContext(string $context): ?TraceContext
    {
        $line = self::wireSplit($context, TraceConstants::CONTENT_SPLITOR);
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
            // MessageType::from throws ValueError on out-of-range values;
            // caught by decoderFromTraceDataString, which then skips this record.
            $bean->msgType = MessageType::from((int) self::at($line, 11));
            $n = count($line);
            if ($n === 13) {                                        // old version: no offsetMsgId
                $ctx->isSuccess = self::at($line, 12) === 'true';
            } elseif ($n === 14) {
                $bean->offsetMsgId = self::at($line, 12);
                $ctx->isSuccess = self::at($line, 13) === 'true';
            }
            if ($n >= 15) {                                         // compatible with older versions
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
            // Messages without keys lack this segment; see the atOr() comment
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
            if ($n >= 9) {                                          // compatible with older versions
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

    private static function at(array $line, int $index): string
    {
        if (!array_key_exists($index, $line)) {
            throw new \OutOfRangeException(sprintf('trace field index %d out of range', $index));
        }
        return $line[$index];
    }

    private static function atOr(array $line, int $index): string
    {
        return $line[$index] ?? '';
    }

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

    private static function stateName(mixed $state): string
    {
        if ($state instanceof \UnitEnum) {
            return $state->name;
        }
        if ($state === null) {
            return 'None';
        }
        if (is_object($state) && isset($state->name) && is_string($state->name)) {
            return $state->name;
        }
        return (string) $state;
    }
}
