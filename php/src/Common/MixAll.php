<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 全局常量与通用工具：topic/组名前缀、clientId 构造、VIP 通道端口换算、
 * properties 文本与 map 的互转。
 */
final class MixAll
{
    public const NAMESRV_ADDR_PROPERTY = 'rocketmq.namesrv.addr';
    public const NAMESRV_ADDR_ENV = 'NAMESRV_ADDR';
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
    /** Request-Reply：应答 topic 名 = <cluster>_REPLY_TOPIC */
    public const REPLY_TOPIC_POSTFIX = 'REPLY_TOPIC';
    /** Request-Reply：应答消息的 MSG_TYPE 属性值 */
    public const REPLY_MESSAGE_FLAG = 'reply';
    public const SYSTEM_TOPIC_PREFIX = 'rmq_sys_';
    public const TOOLS_CONSUMER_GROUP = 'TOOLS_CONSUMER';
    public const FILTERSRV_CONSUMER_GROUP = 'FILTERSRV_CONSUMER';
    public const MONITOR_CONSUMER_GROUP = '__MONITOR_CONSUMER';
    public const CLIENT_INNER_CONSUMER_GROUP = 'CLIENT_INNER_CONSUMER';
    public const SELF_TEST_CONSUMER_GROUP2 = 'SELF_TEST_C_GROUP2';
    public const ONS_NAMESPACE = 'namespace';
    /**
     * ⚠ 这是 extFields 的**键名**（值为 "true"/"false"），不是数字标志位。
     * broker 按该键取值决定这次查消息走不走 uniqKey（INDEX_UNIQUE_TYPE）；
     * 写成数字 1 会让 broker 读不到键，静默退化成按 msgId 查。
     */
    public const UNIQUE_MSG_QUERY_FLAG = '_UNIQUE_KEY_QUERY';
    public const TRACE_TOPIC = 'RMQ_SYS_TRACE_TOPIC';
    public const REAL_TRACE_TOPIC = 'rmq_sys_TRACE_DATA';
    /** 轨迹里的 region 占位值：SEND 响应头没带 MSG_REGION 时用它 */
    public const DEFAULT_TRACE_REGION_ID = 'DefaultRegion';
    public const TRANS_STAT_PROGRESS_TOPIC = 'RMQ_SYS_TRANS_OP_HALF_TOPIC';
    public const RMQ_SYS_TRANS_HALF_TOPIC = 'RMQ_SYS_TRANS_HALF_TOPIC';
    public const RMQ_SYS_TRANS_OP_HALF_TOPIC = 'RMQ_SYS_TRANS_OP_HALF_TOPIC';
    public const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = 'RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC';
    public const RMQ_SYS_TRANS_CHECK_MAX_TIME = 15;
    public const TRANS_CHECK_MAX_TIME = 15;
    public const UNIT_PREFIX = 'unit_';
    /**
     * extFields 的键名 `ReqT`，值是 RequestType 的枚举 code，由流式请求的
     * RPC hook 在发请求前写入，broker 据此识别请求类型。
     */
    public const REQ_T = 'ReqT';
    /**
     * extFields 的键名 `nsd`，值恒为字符串 `"true"`，由 NamespaceRpcHook 写入，
     * 含义是「这笔请求带 5.x 新命名空间」（serverless 实例）。
     */
    public const RPC_REQUEST_HEADER_NAMESPACED_FIELD = 'nsd';
    /**
     * extFields 的键名 `ns`，值是 namespaceV2 本体。与上面那个标记位成对出现，
     * broker 侧据此把请求解析到对应实例——这是**服务端**命名空间机制，和 `namespace`
     * 字段（客户端把 `namespace%` 拼进 topic 名）是两套东西，别混。
     */
    public const RPC_REQUEST_HEADER_NAMESPACE_FIELD = 'ns';
    /**
     * clientId 的流式后缀拼的是 RequestType 的枚举**名**（不是 code），
     * 所以后缀恒为 "@STREAM"。
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
    // instanceName 未显式配置时的默认值
    public const DEFAULT_INSTANCE_NAME = 'DEFAULT';

    /** 框架自带的预置组：Admin 取「用户订阅组」时按此表把它们滤掉 */
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

    /** cachedIpStr() 的进程内缓存：IP 只探测一次，之后不再变化。 */
    private static ?string $cachedIpStr = null;

    /** properties 文本认作空白的字符（string2Properties 的分隔判断用）。 */
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
     * VIP 通道地址换算：端口 - 2。
     *
     * 端口不可解析时原样返回——一个坏地址不该让调用方直接崩。
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
     * 应答 topic 名 = `<cluster>_REPLY_TOPIC`。
     *
     * Request-Reply 的应答消息就发到这个 topic 上（broker 会把 cluster 名写进
     * 消息的 CLUSTER 属性，应答方据此拼出该 topic）。注意这**不是**控制台里那个
     * `%REPLY%<topic>` 前缀（那是另一套东西），本方法只用于 request-reply。
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

    /** LMQ topic 以 %LMQ% 开头。 */
    public static function isLmq(?string $lmqMetaData): bool
    {
        return $lmqMetaData !== null && str_starts_with($lmqMetaData, self::LMQ_PREFIX);
    }

    /** 系统消费组以 CID_RMQ_SYS_ 前缀开头。 */
    public static function isSysConsumerGroup(?string $consumerGroup): bool
    {
        return $consumerGroup !== null && str_starts_with($consumerGroup, self::CID_RMQ_SYS_PREFIX);
    }

    /** 组名是否落在预置组表里。 */
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
        // 32 位十六进制随机串：UNIQ_KEY 全局唯一性的来源
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
     * 进程内只探测一次的本机 IP。
     *
     * 本机 IP 在客户端第一次用到时就定下来，之后不再变——clientId 因此稳定；
     * 每次都重新探测既慢，又可能在网卡变化后让同一客户端换掉 clientId。
     */
    public static function cachedIpStr(): string
    {
        if (self::$cachedIpStr === null) {
            self::$cachedIpStr = self::getIpStr();
        }
        return self::$cachedIpStr;
    }

    /**
     * clientId 格式：`ip@instanceName[@unitName][@STREAM]`。
     *
     * 两段后缀都是可选的，且顺序固定：先 unitName 再 STREAM。
     * - unitName：**只在判空时用 trim**，拼接用原值（所以尾随空格的 unitName
     *   会原样进 clientId，这里不能顺手把值也 trim 掉）。
     * - STREAM：`enableStreamRequestType` 为真时追加枚举名 `STREAM`，
     *   它的存在意义见 `clientIdFor` 的注释。
     */
    public static function buildMqClientId(
        string $clientIp,
        string $instanceName,
        ?string $unitName = null,
        bool $enableStreamRequestType = false
    ): string {
        $cid = sprintf('%s@%s', $clientIp, $instanceName);
        if ($unitName !== null && trim($unitName) !== '') {
            // 判空用 trim，拼接用原值：改了就会换 clientId
            $cid = sprintf('%s@%s', $cid, $unitName);
        }
        if ($enableStreamRequestType) {
            $cid = sprintf('%s@%s', $cid, self::STREAM_REQUEST_TYPE);
        }
        return $cid;
    }

    /**
     * 默认名 `DEFAULT` 换成 `<pid>#<nanoTime>`，其余原样返回。
     *
     * 这一步是 clientId 唯一性的来源：不换的话同进程里两个客户端会算出同一个
     * clientId（旧实现用秒级时间戳，同一秒内必撞）。只在生产者（非
     * `CLIENT_INNER_PRODUCER`）和 CLUSTERING 消费者的 `start()` 里调用，
     * 条件由各 facade 把。
     */
    public static function changeInstanceNameToPid(string $instanceName): string
    {
        if ($instanceName === self::DEFAULT_INSTANCE_NAME) {
            // hrtime 单调、原点任意，只用来保证同进程内不重复
            return sprintf('%d#%d', self::pid(), hrtime(true));
        }
        return $instanceName;
    }

    /**
     * 未显式配置 clientId 时的默认口径：`<本机 IP>@<instanceName>[@<unitName>][@STREAM]`。
     *
     * 调用方要按同样的条件先跑过 `changeInstanceNameToPid`
     * （生产者无条件，消费者仅 CLUSTERING）。
     *
     * `enableStreamRequestType` 这段后缀不只是好看：它就是为了防止不同角色的客户端
     * 意外复用同一个 MQClientInstance —— 拉取/轻量消费者恒为 true，所以同一个
     * instanceName 的它们与推送消费者天然落在不同的 clientId 上。
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
     * properties -> 文本：每条 "key=value\n"，null 值跳过。
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
     * 文本 -> properties：按标准 properties 文本格式解析（broker 导出的 KV 串
     * 就是这个格式，规则一条都不能省）。
     *
     * 规则：
     *   - 跳过空行与 ``#`` / ``!`` 注释行；
     *   - 行尾**未转义**的 ``\`` 表示续行，下一行的前导空白被丢弃；
     *   - 键与值以**第一个** ``=``、``:`` 或**空白**分隔（空白也是合法分隔符！）；
     *   - 分隔符前后的空白被跳过；值的**尾部**空白保留（不去尾空白）。
     *
     * 注：``\t \n \uXXXX`` 这类反斜杠转义不参与解析——broker 配置导出里不出现，
     * 硬做反而容易把语义弄错。
     *
     * @return array<string, string>
     */
    public static function string2Properties(?string $text): array
    {
        if ($text === null) {
            return [];
        }
        $result = [];
        // 先把续行合并成逻辑行
        $logicalLines = [];
        $pending = null;
        foreach (preg_split('/\r\n|\r|\n/', $text) as $raw) {
            $line = $raw;
            if ($pending !== null) {
                $line = $pending . ltrim($line);
                $pending = null;
            }
            // 统计行尾反斜杠个数：奇数表示续行
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
            // 跳过分隔符前的空白
            while ($i < $n && strpos(self::PROP_WS, $line[$i]) !== false) {
                $i++;
            }
            // 可选的 '=' / ':' 及其后的空白
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
