<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Global constants and general utilities: topic/group name prefixes, clientId construction, VIP channel port
 * conversion, and properties text <-> map conversion.
 */
final class MixAll
{
    public const NAMESRV_ADDR_PROPERTY = 'rocketmq.namesrv.addr';
    public const NAMESRV_ADDR_ENV = 'NAMESRV_ADDR';
    /**
     * Response ext field: the broker understood the fingerprint-bearing heartbeat body, so later
     * beats to this address may omit the subscription set.
     */
    public const IS_SUPPORT_HEART_BEAT_V2 = 'IS_SUPPORT_HEART_BEAT_V2';
    /// Response ext field: the fingerprint the broker holds for one of the groups differs from the
    /// one just sent, so the subscriptions have to go out again.
    public const IS_SUB_CHANGE = 'IS_SUB_CHANGE';
    public const MESSAGE_COMPRESS_LEVEL = 'rocketmq.message.compressLevel';
    public const DEFAULT_TOPIC = 'TBW102';
    public const BENCHMARK_TOPIC = 'BenchmarkTest';
    public const DEFAULT_PRODUCER_GROUP = 'DEFAULT_PRODUCER';
    public const DEFAULT_CONSUMER_GROUP = 'DEFAULT_CONSUMER';
    public const CLIENT_INNER_PRODUCER_GROUP = 'CLIENT_INNER_PRODUCER';
    public const SELF_TEST_PRODUCER_GROUP = 'SELF_TEST_P_GROUP';
    public const SELF_TEST_CONSUMER_GROUP = 'SELF_TEST_C_GROUP';
    public const SCHEDULE_CONSUMER_GROUP = 'SCHEDULE_CONSUMER';
    public const ONS_HTTP_PROXY_GROUP = 'CID_ONS-HTTP-PROXY';
    public const CID_ONSAPI_PERMISSION_GROUP = 'CID_ONSAPI_PERMISSION';
    public const CID_ONSAPI_OWNER_GROUP = 'CID_ONSAPI_OWNER';
    public const CID_ONSAPI_PULL_GROUP = 'CID_ONSAPI_PULL';
    public const CID_SYS_RMQ_TRANS = 'CID_SYS_RMQ_TRANS';
    public const ONS_ADDR = 'ONS_ADDR';
    public const CID_RMQ_SYS_PREFIX = 'CID_RMQ_SYS_';
    public const CID_ONSAPI_PREFIX = 'CID_ONSAPI_';
    public const CID_SDK_SYNC_PREFIX = 'CID_SDK_SYNC_';
    public const CID_SDK_ASYNC_PREFIX = 'CID_SDK_ASYNC_';
    public const CID_SDK_PROXY_PREFIX = 'CID_SDK_PROXY_';
    public const PROXY_NAME = 'MQProxy';
    public const DEFAULT_PRODUCER_GROUP_AND_STREAM = 'DEFAULT_PRODUCER_AND_STREAM';

    public const RETRY_GROUP_TOPIC_PREFIX = '%RETRY%';
    public const DLQ_GROUP_TOPIC_PREFIX = '%DLQ%';
    public const REPLY_TOPIC_PREFIX = '%REPLY%';
    /** Request-Reply: reply topic name = <cluster>_REPLY_TOPIC */
    public const REPLY_TOPIC_POSTFIX = 'REPLY_TOPIC';
    /** Request-Reply: the MSG_TYPE property value of a reply message */
    public const REPLY_MESSAGE_FLAG = 'reply';
    public const SYSTEM_TOPIC_PREFIX = 'rmq_sys_';
    public const TOOLS_CONSUMER_GROUP = 'TOOLS_CONSUMER';
    public const FILTERSRV_CONSUMER_GROUP = 'FILTERSRV_CONSUMER';
    public const MONITOR_CONSUMER_GROUP = '__MONITOR_CONSUMER';
    public const CLIENT_INNER_CONSUMER_GROUP = 'CLIENT_INNER_CONSUMER';
    public const SELF_TEST_CONSUMER_GROUP2 = 'SELF_TEST_C_GROUP2';
    public const ONS_NAMESPACE = 'namespace';
    /**
     * WARNING: This is the **key name** of an extFields entry (value is "true"/"false"), not a numeric flag bit.
     * The broker reads this key to decide whether this message query goes through uniqKey (INDEX_UNIQUE_TYPE);
     * writing the number 1 would make the broker unable to read the key, silently degrading to a msgId-based query.
     */
    public const UNIQUE_MSG_QUERY_FLAG = '_UNIQUE_KEY_QUERY';
    public const TRACE_TOPIC = 'RMQ_SYS_TRACE_TOPIC';
    public const REAL_TRACE_TOPIC = 'rmq_sys_TRACE_DATA';
    /** Placeholder region value in traces: used when the SEND response header carries no MSG_REGION */
    public const DEFAULT_TRACE_REGION_ID = 'DefaultRegion';
    public const TRANS_STAT_PROGRESS_TOPIC = 'RMQ_SYS_TRANS_OP_HALF_TOPIC';
    public const RMQ_SYS_TRANS_HALF_TOPIC = 'RMQ_SYS_TRANS_HALF_TOPIC';
    public const RMQ_SYS_TRANS_OP_HALF_TOPIC = 'RMQ_SYS_TRANS_OP_HALF_TOPIC';
    public const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = 'RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC';
    public const RMQ_SYS_TRANS_CHECK_MAX_TIME = 15;
    public const TRANS_CHECK_MAX_TIME = 15;
    public const UNIT_PREFIX = 'unit_';
    /**
     * extFields key name `ReqT`, whose value is the RequestType enum code. Written by the streaming request's
     * RPC hook before sending, so the broker can identify the request type.
     */
    public const REQ_T = 'ReqT';
    /**
     * extFields key name `nsd`, whose value is always the string `"true"`, written by NamespaceRpcHook,
     * meaning "this request carries the 5.x new namespace" (serverless instance).
     */
    public const RPC_REQUEST_HEADER_NAMESPACED_FIELD = 'nsd';
    /**
     * extFields key name `ns`, whose value is the namespaceV2 body. Paired with the flag bit above,
     * the broker uses it to resolve the request to the corresponding instance — this is the **server-side** namespace
     * mechanism, distinct from the `namespace` field (client side prepends `namespace%` to the topic name). Do not mix them.
     */
    public const RPC_REQUEST_HEADER_NAMESPACE_FIELD = 'ns';
    /**
     * The stream suffix of clientId is concatenated with the RequestType enum **name** (not the code),
     * so the suffix is always "@STREAM".
     */
    public const STREAM_REQUEST_TYPE = 'STREAM';
    public const LMQ_PREFIX = '%LMQ%';
    public const LMQ_QUEUE_ID = 0;
    public const DEFAULT_TOPIC_QUEUE_NUMS = 4;
    public const DEFAULT_TOPIC_READ_QUEUE_NUMS = 4;
    public const DEFAULT_TOPIC_WRITE_QUEUE_NUMS = 4;
    public const MAX_TOPIC_LENGTH = 127;
    public const MAX_GROUP_LENGTH = 255;
    public const CHARACTER_MAX_LENGTH = 255;
    public const PULL_THRESHOLD_LEVEL_HIGH = 1;
    public const PULL_THRESHOLD_LEVEL_MEDIUM = 2;
    public const PULL_THRESHOLD_LEVEL_LOW = 3;
    public const PULL_TIMEOUT_MILLIS_HIGH = 30000;
    public const PULL_TIMEOUT_MILLIS_MEDIUM = 20000;
    public const PULL_TIMEOUT_MILLIS_LOW = 10000;
    public const LOG_STATS_TOPIC = 'LOG_STATS_TOPIC';

    public const W_HELPER = 'HELPER';
    public const W_EXPIRY_DATE = 'EXPIRY_DATE';
    public const W_AVATAR = 'AVATAR';
    public const W_REGION_ID = 'REGION_ID';

    public const MASTER_ID = 0;
    public const DEFAULT_CENTER = 'DEFAULT_CENTER';

    public const NAMESPACE_PATTERN = '^[%s]{4}[a-zA-Z0-9_-]+$';

    /** PermName.PERM_READ | PERM_WRITE */
    public const READ_PERM_BY_DEFAULT = 4 | 2;

    // ---------------- clientId ----------------
    // Default value when instanceName is not explicitly configured
    public const DEFAULT_INSTANCE_NAME = 'DEFAULT';

    /** Predefined groups shipped with the framework: Admin filters them out when fetching "user subscription groups" */
    private const PREDEFINE_GROUP_SET = [
        self::DEFAULT_CONSUMER_GROUP,
        self::DEFAULT_PRODUCER_GROUP,
        self::TOOLS_CONSUMER_GROUP,
        self::SCHEDULE_CONSUMER_GROUP,
        self::FILTERSRV_CONSUMER_GROUP,
        self::MONITOR_CONSUMER_GROUP,
        self::CLIENT_INNER_PRODUCER_GROUP,
        self::SELF_TEST_PRODUCER_GROUP,
        self::SELF_TEST_CONSUMER_GROUP,
        self::ONS_HTTP_PROXY_GROUP,
        self::CID_ONSAPI_PERMISSION_GROUP,
        self::CID_ONSAPI_OWNER_GROUP,
        self::CID_ONSAPI_PULL_GROUP,
        self::CID_SYS_RMQ_TRANS,
    ];

    /** In-process cache for cachedIpStr(): the IP is probed only once and then never changes. */
    private static ?string $cachedIpStr = null;

    /** Characters treated as whitespace in properties text (used for separator judgment in string2Properties). */
    private const PROP_WS = " \t\f";

    public static function getRetryTopic(string $consumerGroup): string
    {
        return self::RETRY_GROUP_TOPIC_PREFIX . $consumerGroup;
    }

    public static function isRetryTopic(?string $topic): bool
    {
        return $topic !== null && str_starts_with($topic, self::RETRY_GROUP_TOPIC_PREFIX);
    }

    public static function getDlqTopic(string $consumerGroup): string
    {
        return self::DLQ_GROUP_TOPIC_PREFIX . $consumerGroup;
    }

    public static function isDlqTopic(?string $topic): bool
    {
        return $topic !== null && str_starts_with($topic, self::DLQ_GROUP_TOPIC_PREFIX);
    }

    /**
     * VIP channel address conversion: port - 2.
     *
     * If the port cannot be parsed, return as-is — a bad address should not crash the caller.
     */
    public static function brokerVipChannel(bool $isChange, string $brokerAddr): string
    {
        if (!$isChange) {
            return $brokerAddr;
        }
        $pos = strrpos($brokerAddr, ':');
        if ($pos === false) {
            return $brokerAddr;
        }
        $host = substr($brokerAddr, 0, $pos);
        $port = substr($brokerAddr, $pos + 1);
        if ($host === '' || $port === '' || !ctype_digit($port)) {
            return $brokerAddr;
        }
        return sprintf('%s:%d', $host, (int) $port - 2);
    }

    /**
     * Reply topic name = `<cluster>_REPLY_TOPIC`.
     *
     * The reply message of Request-Reply is sent to this topic (the broker writes the cluster name into the message's
     * CLUSTER property, and the replying side builds this topic from it). Note this is **NOT** the `%REPLY%<topic>` prefix
     * in the console (that is a separate mechanism); this method is only for request-reply.
     */
    public static function getReplyTopic(string $clusterName): string
    {
        return sprintf('%s_%s', $clusterName, self::REPLY_TOPIC_POSTFIX);
    }

    public static function getBrokerCircuitBreakerConsumeGroup(): string
    {
        return 'BROKER_CIRCUIT_BREAKER';
    }

    public static function getBrokerCircuitBreakerTopic(): string
    {
        return 'BROKER_CIRCUIT_BREAKER_TOPIC';
    }

    public static function isSysTopic(?string $topic): bool
    {
        return $topic !== null && str_starts_with($topic, self::SYSTEM_TOPIC_PREFIX);
    }

    /** LMQ topics start with %LMQ%. */
    public static function isLmq(?string $lmqMetaData): bool
    {
        return $lmqMetaData !== null && str_starts_with($lmqMetaData, self::LMQ_PREFIX);
    }

    /** System consumer groups start with the CID_RMQ_SYS_ prefix. */
    public static function isSysConsumerGroup(?string $consumerGroup): bool
    {
        return $consumerGroup !== null && str_starts_with($consumerGroup, self::CID_RMQ_SYS_PREFIX);
    }

    /** Whether the group name falls in the predefined group table. */
    public static function isPredefinedGroup(?string $consumerGroup): bool
    {
        return $consumerGroup !== null && in_array($consumerGroup, self::PREDEFINE_GROUP_SET, true);
    }

    public static function resetRetryAndDlqTopic(?string $topic): ?string
    {
        if ($topic === null) {
            return null;
        }
        if (self::isRetryTopic($topic)) {
            return substr($topic, strlen(self::RETRY_GROUP_TOPIC_PREFIX));
        }
        if (self::isDlqTopic($topic)) {
            return substr($topic, strlen(self::DLQ_GROUP_TOPIC_PREFIX));
        }
        return $topic;
    }

    public static function compareAndIncreaseNamespace(string $instanceName, ?string $namespace): string
    {
        if ($namespace === null || $namespace === '') {
            return $instanceName;
        }
        if (str_starts_with($instanceName, $namespace)) {
            return $instanceName;
        }
        $namespacePrefix = sprintf('%%%s%%', $namespace);
        if (str_starts_with($instanceName, $namespacePrefix)) {
            return $instanceName;
        }
        return sprintf('%%%s%%%%%s', $namespace, $instanceName);
    }

    public static function createUniqName(string $prefix): string
    {
        // 32-char hex random string: the source of UNIQ_KEY's global uniqueness
        return $prefix . bin2hex(random_bytes(16));
    }

    public static function getIpStr(): string
    {
        if (function_exists('socket_create')) {
            $sock = @socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
            if ($sock !== false) {
                if (@socket_connect($sock, '8.8.8.8', 80)) {
                    $addr = '';
                    $port = 0;
                    if (@socket_getsockname($sock, $addr, $port) && $addr !== '') {
                        socket_close($sock);
                        return $addr;
                    }
                }
                socket_close($sock);
            }
        }
        return '127.0.0.1';
    }

    public static function pid(): int
    {
        return UtilAll::getPid();
    }

    /**
     * In-process local IP probed only once.
     *
     * The local IP is fixed the first time the client uses it and then never changes — this keeps the clientId stable;
     * re-probing every time is both slow and may change the clientId of the same client after a network interface change.
     */
    public static function cachedIpStr(): string
    {
        if (self::$cachedIpStr === null) {
            self::$cachedIpStr = self::getIpStr();
        }
        return self::$cachedIpStr;
    }

    /**
     * clientId format: `ip@instanceName[@unitName][@STREAM]`.
     *
     * Both suffixes are optional and in a fixed order: unitName first, then STREAM.
     * - unitName: **only trim when checking for emptiness**, but concatenate using the original value (so a unitName with
     *   trailing spaces enters the clientId as-is; do not trim the value here).
     * - STREAM: appended with the enum name `STREAM` when `enableStreamRequestType` is true;
     *   its purpose is explained in the `clientIdFor` comment.
     */
    public static function buildMqClientId(
        string $clientIp,
        string $instanceName,
        ?string $unitName = null,
        bool $enableStreamRequestType = false
    ): string {
        $cid = sprintf('%s@%s', $clientIp, $instanceName);
        if ($unitName !== null && trim($unitName) !== '') {
            // Trim for emptiness check, but concatenate the original value: changing this would alter the clientId
            $cid = sprintf('%s@%s', $cid, $unitName);
        }
        if ($enableStreamRequestType) {
            $cid = sprintf('%s@%s', $cid, self::STREAM_REQUEST_TYPE);
        }
        return $cid;
    }

    /**
     * Replace the default name `DEFAULT` with `<pid>#<nanoTime>`, otherwise return as-is.
     *
     * This step is the source of clientId uniqueness: without it, two clients in the same process would compute the same
     * clientId (the old implementation used second-level timestamps, which inevitably collide within the same second).
     * Only called in the `start()` of producers (non-`CLIENT_INNER_PRODUCER`) and CLUSTERING consumers;
     * the conditions are handled by each facade.
     */
    public static function changeInstanceNameToPid(string $instanceName): string
    {
        if ($instanceName === self::DEFAULT_INSTANCE_NAME) {
            // hrtime is monotonic with an arbitrary origin; used only to guarantee no repetition within the same process
            return sprintf('%d#%d', self::pid(), hrtime(true));
        }
        return $instanceName;
    }

    /**
     * Default clientId when not explicitly configured: `<local IP>@<instanceName>[@<unitName>][@STREAM]`.
     *
     * The caller must first run `changeInstanceNameToPid` under the same conditions
     * (producers unconditionally, consumers only in CLUSTERING).
     *
     * The `enableStreamRequestType` suffix is not just cosmetic: it prevents clients of different roles from accidentally
     * sharing the same MQClientInstance — pull/lite consumers always pass true, so they and push consumers with the same
     * instanceName naturally land on different clientIds.
     */
    public static function clientIdFor(
        string $instanceName,
        ?string $unitName = null,
        bool $enableStreamRequestType = false
    ): string {
        return self::buildMqClientId(self::cachedIpStr(), $instanceName, $unitName, $enableStreamRequestType);
    }

    // ---------------- Properties <-> String ----------------

    /**
     * properties -> text: each entry "key=value\n", skipping null values.
     *
     * @param array<string, string|null>|null $properties
     */
    public static function properties2String(?array $properties, bool $isSort = false): string
    {
        if ($properties === null) {
            return '';
        }
        $items = $properties;
        if ($isSort) {
            ksort($items, SORT_STRING);
        }
        $buf = '';
        foreach ($items as $k => $v) {
            if ($v !== null) {
                $buf .= sprintf("%s=%s\n", $k, $v);
            }
        }
        return $buf;
    }

    /**
     * Text -> properties: parse according to the standard properties text format (the KV string exported by the broker
     * uses exactly this format, and no rule can be omitted).
     *
     * Rules:
     *   - Skip empty lines and ``#`` / ``!`` comment lines;
     *   - A line-ending **unescaped** ``\`` means continuation; the leading whitespace of the next line is discarded;
     *   - Key and value are separated by the **first** ``=``, ``:`` or **whitespace** (whitespace is a valid separator too!);
     *   - Whitespace before/after the separator is skipped; trailing whitespace of the value is preserved (not trimmed).
     *
     * Note: backslash escapes like ``\t \n \uXXXX`` are not part of parsing — they do not appear in broker config exports,
     * and forcing them in would easily get the semantics wrong.
     *
     * @return array<string, string>
     */
    public static function string2Properties(?string $text): array
    {
        if ($text === null) {
            return [];
        }
        $result = [];
        // First merge continuation lines into logical lines
        $logicalLines = [];
        $pending = null;
        foreach (preg_split('/\r\n|\r|\n/', $text) as $raw) {
            $line = $raw;
            if ($pending !== null) {
                $line = $pending . ltrim($line);
                $pending = null;
            }
            // Count trailing backslashes: an odd number means continuation
            $trailing = strlen($line) - strlen(rtrim($line, '\\'));
            if ($trailing % 2 === 1) {
                $pending = substr($line, 0, -1);
                continue;
            }
            $logicalLines[] = $line;
        }
        if ($pending !== null) {
            $logicalLines[] = $pending;
        }

        foreach ($logicalLines as $line) {
            $stripped = trim($line);
            if ($stripped === '' || $stripped[0] === '#' || $stripped[0] === '!') {
                continue;
            }
            $n = strlen($line);
            $i = 0;
            while ($i < $n && strpos(self::PROP_WS, $line[$i]) !== false) {
                $i++;
            }
            $keyStart = $i;
            while ($i < $n && $line[$i] !== '=' && $line[$i] !== ':'
                && strpos(self::PROP_WS, $line[$i]) === false) {
                $i++;
            }
            $key = substr($line, $keyStart, $i - $keyStart);
            // Skip whitespace before the separator
            while ($i < $n && strpos(self::PROP_WS, $line[$i]) !== false) {
                $i++;
            }
            // Optional '=' / ':' and the whitespace after it
            if ($i < $n && ($line[$i] === '=' || $line[$i] === ':')) {
                $i++;
                while ($i < $n && strpos(self::PROP_WS, $line[$i]) !== false) {
                    $i++;
                }
            }
            $result[$key] = (string) substr($line, $i);
        }
        return $result;
    }
}
