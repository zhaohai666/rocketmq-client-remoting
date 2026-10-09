<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\ClientErrorCode;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\BoundaryType;
use RocketMQ\Common\ExpressionType;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageAccessor;
use RocketMQ\Common\MessageBatch;
use RocketMQ\Common\MessageClientIdSetter;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MessageSysFlag;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PullSysFlag;
use RocketMQ\Common\SubscriptionData;
use RocketMQ\Common\TopicFilterType;
use RocketMQ\Remoting\Protocol\AckMessageRequestHeader;
use RocketMQ\Remoting\Protocol\ChangeInvisibleTimeRequestHeader;
use RocketMQ\Remoting\Protocol\ChangeInvisibleTimeResponseHeader;
use RocketMQ\Remoting\Protocol\CheckClientRequestBody;
use RocketMQ\Remoting\Protocol\ClusterInfo;
use RocketMQ\Remoting\Protocol\ConsumeMessageDirectlyResult;
use RocketMQ\Remoting\Protocol\ConsumeMessageDirectlyResultRequestHeader;
use RocketMQ\Remoting\Protocol\CreateTopicRequestHeader;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\GetConsumerListByGroupRequestHeader;
use RocketMQ\Remoting\Protocol\GetConsumerListByGroupResponseBody;
use RocketMQ\Remoting\Protocol\GetConsumerRunningInfoRequestHeader;
use RocketMQ\Remoting\Protocol\GetConsumerStatusBody;
use RocketMQ\Remoting\Protocol\GetConsumerStatusRequestHeader;
use RocketMQ\Remoting\Protocol\GetMaxOffsetRequestHeader;
use RocketMQ\Remoting\Protocol\GetMaxOffsetResponseHeader;
use RocketMQ\Remoting\Protocol\GetMinOffsetRequestHeader;
use RocketMQ\Remoting\Protocol\GetMinOffsetResponseHeader;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\LockBatchMqRequestHeader;
use RocketMQ\Remoting\Protocol\LockBatchRequestBody;
use RocketMQ\Remoting\Protocol\LockBatchResponseBody;
use RocketMQ\Remoting\Protocol\NotifyConsumerIdsChangedRequestHeader;
use RocketMQ\Remoting\Protocol\PopMessageRequestHeader;
use RocketMQ\Remoting\Protocol\PopMessageResponseHeader;
use RocketMQ\Remoting\Protocol\PullMessageRequestHeader;
use RocketMQ\Remoting\Protocol\PullMessageResponseHeader;
use RocketMQ\Remoting\Protocol\QueryConsumerOffsetRequestHeader;
use RocketMQ\Remoting\Protocol\QueryConsumerOffsetResponseHeader;
use RocketMQ\Remoting\Protocol\QueryMessageRequestHeader;
use RocketMQ\Remoting\Protocol\RecallMessageRequestHeader;
use RocketMQ\Remoting\Protocol\RecallMessageResponseHeader;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\ReplyMessageRequestHeader;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResetOffsetBody;
use RocketMQ\Remoting\Protocol\ResetOffsetBodyForC;
use RocketMQ\Remoting\Protocol\ResetOffsetRequestHeader;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\SearchOffsetRequestHeader;
use RocketMQ\Remoting\Protocol\SearchOffsetResponseHeader;
use RocketMQ\Remoting\Protocol\SendMessageRequestHeaderV2;
use RocketMQ\Remoting\Protocol\SendMessageResponseHeader;
use RocketMQ\Remoting\Protocol\TopicList;
use RocketMQ\Remoting\Protocol\TopicRouteData;
use RocketMQ\Remoting\Protocol\UnlockBatchMqRequestHeader;
use RocketMQ\Remoting\Protocol\UnlockBatchRequestBody;
use RocketMQ\Remoting\Protocol\UnregisterClientRequestHeader;
use RocketMQ\Remoting\Protocol\UpdateConsumerOffsetRequestHeader;
use RocketMQ\Remoting\RemotingClient;
use RocketMQ\Remoting\NamespaceRpcHook;
use RocketMQ\Remoting\StreamTypeRPCHook;

/**
 * MQClientInstance：RocketMQ 客户端核心编排（对应
 * org.apache.rocketmq.client.impl.factory.MQClientInstance 与 MQClientAPIImpl 的核心调用面）。
 *
 * 职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
 * 消息发送（SEND_MESSAGE_V2 / SEND_BATCH_MESSAGE / SEND_REPLY_MESSAGE_V2）、拉取（PULL_MESSAGE）、
 * POP（POP_MESSAGE / ACK_MESSAGE / CHANGE_MESSAGE_INVISIBLETIME）、offset 查询/更新、心跳、
 * 管理类 API（创建/删除 Topic、集群信息等）。
 *
 * 本文件同时容纳 TopicPublishInfo 与 MQClientInstance 两个类（对应 Python 同模块）。
 *
 * 单线程适配：Python 版的 _namesrv_refresh_loop / _adjust_thread_pool_loop /
 * _route_refresh_loop 三个后台线程，以及 ConsumerStatsManager 的采样线程，全部收敛为
 * 调用方驱动的显式 ``tick()``（内部按时间戳判定到期，可外部注入当前时间）；原先
 * 「丢到后台线程」的 resetOffset 改为进入内部 pending 队列，由 ``tick()`` 执行。
 */

/**
 * 对应 org.apache.rocketmq.client.impl.producer.TopicPublishInfo。
 */
final class TopicPublishInfo
{
    public bool $orderTopic = false;

    /** @var list<MessageQueue> */
    public array $msgQueueList = [];

    public ?TopicRouteData $topicRouteData = null;

    private int $index = 0;

    public function ok(): bool
    {
        return count($this->msgQueueList) > 0;
    }

    public function resetIndex(): void
    {
        $this->index = 0;
    }

    /**
     * 轮询选队列（对应 Java TopicPublishInfo.selectOneMessageQueue）。
     *
     * ``$filters`` 为可调用 ``f(mq): bool``，全部通过才选中。无过滤器时固定返回一个
     * 轮询队列（永不返回 null）；带过滤器且一轮内无匹配时返回 null，由调用方退化选择。
     *
     * @param callable(MessageQueue): bool ...$filters
     */
    public function selectOneMessageQueue(callable ...$filters): ?MessageQueue
    {
        // Python: with self._lock（PHP 单线程顺序执行，临界区语义不变）
        if ($this->msgQueueList === []) {
            throw new MQClientException('no message queue for publish info');
        }
        $n = count($this->msgQueueList);
        if ($filters === []) {
            $mq = $this->msgQueueList[$this->index % $n];
            $this->index++;
            return $mq;
        }
        for ($i = 0; $i < $n; $i++) {
            $mq = $this->msgQueueList[$this->index % $n];
            $this->index++;
            $all = true;
            foreach ($filters as $f) {
                if (!$f($mq)) {
                    $all = false;
                    break;
                }
            }
            if ($all) {
                return $mq;
            }
        }
        return null;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'orderTopic' => $this->orderTopic,
            'messageQueueList' => array_map(
                static fn(MessageQueue $q): array => [
                    'topic' => $q->topic,
                    'brokerName' => $q->brokerName,
                    'queueId' => $q->queueId,
                ],
                $this->msgQueueList,
            ),
        ];
    }
}

/**
 * 对应 org.apache.rocketmq.client.impl.factory.MQClientInstance（含 MQClientAPIImpl 调用面）。
 */
class MQClientInstance
{
    /** 采样周期（Java ConsumerStatsManager 的 scheduleAtFixedRate 参数，10s）。 */
    public const STATS_SAMPLE_INTERVAL_MILLIS = 10_000;

    /** 动态 NameServer 刷新：Java initialDelay = 10s。 */
    public const NAMESRV_REFRESH_INITIAL_DELAY_MILLIS = 10_000;
    /** 动态 NameServer 刷新：Java 周期 = 2 分钟。 */
    public const NAMESRV_REFRESH_INTERVAL_MILLIS = 120_000;

    /** 线程弹性巡检：Java initialDelay = 1 分钟。 */
    public const ADJUST_POOL_INITIAL_DELAY_MILLIS = 60_000;
    /** 线程弹性巡检：Java 周期 = 1 分钟。 */
    public const ADJUST_POOL_INTERVAL_MILLIS = 60_000;

    /** 路由刷新：Java initialDelay = 10ms。 */
    public const ROUTE_REFRESH_INITIAL_DELAY_MILLIS = 10;

    /** 对应 Python 的 INSTANCE_MAP（clientId → 实例）。 */
    public static array $instanceMap = [];

    public string $clientId;

    /** @var list<string> */
    public array $nameServerAddrs;

    /**
     * 在用 topic 的路由刷新周期（对应 Java ClientConfig.pollNameServerInterval，默认 30000ms）。
     * 只在 start() 时取一次（Java scheduledExecutorService 同理）。
     */
    public int $pollNameServerInterval;

    /**
     * VIP 通道开关（对应 Java ClientConfig.vipChannelEnabled，默认 false）：true 时
     * 发往 broker 的**发送 / 拉取 / POP / 指配**请求走 VIP 端口（端口 - 2）。
     * Java 在 MQClientAPIImpl 的每个 invoke 点做 ``MixAll.brokerVIPChannel``，本端口
     * 集中在 ``vipAddrFor()`` 一处套用（Admin 有自己独立的同名开关）。
     */
    public bool $vipChannelEnabled = false;

    /**
     * 5.x 新命名空间（对应 Java ClientConfig.namespaceV2）：非空时**每笔**请求都带
     * `nsd=true` / `ns=<该值>` 两个扩展头（由 {@see NamespaceRpcHook} 写入），broker
     * 据此把请求解析到对应的 serverless 实例。它和拼 topic 名的 `namespace` 是两套机制。
     *
     * ⚠ 钩子在构造时就装好、值在每笔请求里现读（Java 传的是 clientConfig 对象，同语义），
     * 所以 start() 之后再改这个字段一样生效——别把「非空才注册」当成优化。
     */
    public string $namespaceV2 = '';

    public RemotingClient $remotingClient;

    /** @var array<string, TopicRouteData> */
    public array $topicRouteTable = [];

    /** @var array<string, TopicPublishInfo> */
    public array $topicPublishInfoTable = [];

    /**
     * 对应 Java brokerAddrTable：按 brokerName 平的一张贴表，每次刷到任一条路由就整批覆盖。
     *
     * @var array<string, array<int, string>>
     */
    public array $brokerAddrTable = [];

    /** Python threading.RLock：PHP 单线程顺序执行，临界区语义保留（仅注释）。 */
    public bool $topicRouteLockHeld = false;

    public DefaultTopAddressing $topAddressing;

    public ConsumerStatsManager $consumerStatsManager;

    /** 可注入的「当前时间」闭包（毫秒），便于测试。为空时用 microtime。 */
    public ?\Closure $clock = null;

    private bool $started = false;

    /** @var array<string, bool> 本客户端「在用」的 topic 集合 */
    private array $topicsInUse = [];

    /** @var array<string, object> consumerGroup → 消费者实例 */
    private array $consumerTable = [];

    private int $consumerIdsChangedCount = 0;

    /** @var list<callable> 单线程下的「后台任务」延迟队列（原 Python 另起线程做的事） */
    private array $pendingActions = [];

    private bool $namesrvRefreshEnabled = false;
    private ?int $namesrvRefreshNextRunMillis = null;
    private ?int $adjustPoolNextRunMillis = null;
    private ?int $routeRefreshNextRunMillis = null;
    private ?int $statsSampleNextRunMillis = null;
    /** RequestFutureHolder TTL 清扫（Java RequestHouseKeepingService：initial 3s，period 1s）。 */
    private ?int $requestScanNextRunMillis = null;
    public const REQUEST_SCAN_INITIAL_DELAY_MILLIS = 3_000;
    public const REQUEST_SCAN_INTERVAL_MILLIS = 1_000;

    /**
     * @param list<string> $nameServerAddrs
     */
    public function __construct(
        string $clientId,
        array $nameServerAddrs,
        int $connectTimeoutMillis = 3000,
        int $invokeTimeoutMillis = 15000,
        ?bool $tlsEnable = null,
        bool $enableStreamRequestType = false,
        string $namespaceV2 = '',
        ?string $unitName = null,
        int $pollNameServerInterval = 30000,
        ?array $tlsOptions = null,
        bool $vipChannelEnabled = false,
    ) {
        $this->clientId = $clientId;
        $this->nameServerAddrs = array_values($nameServerAddrs);
        $this->pollNameServerInterval = $pollNameServerInterval;
        $this->vipChannelEnabled = $vipChannelEnabled;
        $this->namespaceV2 = $namespaceV2;
        $this->remotingClient = new RemotingClient($connectTimeoutMillis, $invokeTimeoutMillis, $tlsEnable, tlsOptions: $tlsOptions);
        // 对应 Java MQClientAPIImpl:329-335 的装链顺序：
        //   Namespace → Stream → 用户 rpcHook（ACL 签名）→（本端口无 DynamicalExtField）
        // namespace 钩子**无条件注册**且排在最前：nsd/ns 必须在算签名之前进 extFields，
        // 否则开鉴权的 broker 验签会多出未签名字段而拒签；钩子自身在 namespaceV2 为空时
        // 一个字段都不写（与 Java 同），所以空值注册没有代价。
        $this->remotingClient->registerRpcHook(new NamespaceRpcHook(\Closure::fromCallable([$this, 'namespaceV2Value'])));
        // stream 钩子同样必须注册在用户 rpcHook 之前，这样 ReqT 才会被算进 ACL 签名内容
        // （"Inject stream rpc hook first to make reserve field signature"）。facade 都是在
        // 构造完本实例之后才注册 rpcHook。
        if ($enableStreamRequestType) {
            $this->remotingClient->registerRpcHook(new StreamTypeRPCHook());
        }

        // Request-Reply：broker 用 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 把应答推回来。
        $this->remotingClient->registerProcessor(
            RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT,
            \Closure::fromCallable([$this, 'processReplyMessage']),
        );
        // broker 主动请求 220/221/307/309（对应 Java ClientRemotingProcessor）：
        // 客户端实例级注册，处理时按 consumerGroup 找到对应消费者实例。
        $this->remotingClient->registerProcessor(
            RequestCode::RESET_CONSUMER_CLIENT_OFFSET,
            \Closure::fromCallable([$this, 'processResetOffset']),
        );
        $this->remotingClient->registerProcessor(
            RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT,
            \Closure::fromCallable([$this, 'processGetConsumerStatus']),
        );
        $this->remotingClient->registerProcessor(
            RequestCode::GET_CONSUMER_RUNNING_INFO,
            \Closure::fromCallable([$this, 'processGetConsumerRunningInfo']),
        );
        $this->remotingClient->registerProcessor(
            RequestCode::CONSUME_MESSAGE_DIRECTLY,
            \Closure::fromCallable([$this, 'processConsumeMessageDirectly']),
        );
        // NOTIFY_CONSUMER_IDS_CHANGED(40)：消费组成员变化时 broker 沿长连接反向推过来。
        $this->remotingClient->registerProcessor(
            RequestCode::NOTIFY_CONSUMER_IDS_CHANGED,
            \Closure::fromCallable([$this, 'processNotifyConsumerIdsChanged']),
        );

        // 动态 name server（Java MQClientAPIImpl.topAddressing）。未配置
        // ROCKETMQ_NAMESRV_DOMAIN 时 wsAddr 为空串 → fetch 是 no-op。
        $this->topAddressing = new DefaultTopAddressing(unitName: $unitName ?? '');
        // 消费统计（Java MQClientFactory.getConsumerStatsManager，实例级共享）
        $this->consumerStatsManager = new ConsumerStatsManager();

        self::$instanceMap[$clientId] = $this;
    }

    public static function getInstance(string $clientId): ?self
    {
        return self::$instanceMap[$clientId] ?? null;
    }

    public static function removeInstance(string $clientId): void
    {
        unset(self::$instanceMap[$clientId]);
    }

    /**
     * 当前生效的 namespaceV2——{@see NamespaceRpcHook} 每笔请求回调本方法取值，
     * 与 Java「钩子实时读 clientConfig.getNamespaceV2()」同语义：start() 之后改
     * 本字段，后续请求立即跟着变（对照 Remoting/NamespaceRpcHook 的注释）。
     */
    public function namespaceV2Value(): string
    {
        return $this->namespaceV2;
    }

    // ---------------- 时间 ----------------

    public function currentTimeMillis(): int
    {
        if ($this->clock !== null) {
            return (int) ($this->clock)();
        }
        return (int) floor(microtime(true) * 1000.0);
    }

    // ---------------- 生命周期 ----------------

    /**
     * 同步初始化（对应 Java MQClientInstance.start 的同步部分；PHP 不启后台线程）。
     *
     * ①置 started；②启动消费统计；③动态 NameServer 首次取址（取不到直接报 10004）；
     * ④排定各 pump 的首次到期时刻；⑤同步做一次初始化「泵」：首轮路由刷新 + 一次心跳 +
     * CHECK_CLIENT_CONFIG(46) 校验（后者 best-effort，异常只记日志）。
     *
     * 之后由调用方周期调用 ``tick()`` 推进 namesrv 刷新 / 路由刷新 / 线程巡检 / 统计采样。
     */
    public function start(): void
    {
        $this->started = true;
        $this->consumerStatsManager->start();

        $dynamicNs = $this->nameServerAddrs === []
            && $this->topAddressing->wsAddr !== '';
        if ($dynamicNs) {
            $this->fetchNameServerAddr();
            if ($this->nameServerAddrs === []) {
                // Java 此步不报错（取不到照样启动），本端口在 start 就失败（更可预期），
                // 码值仍用 10004 NO_NAME_SERVER_EXCEPTION。
                throw new MQClientException(
                    sprintf(
                        'name server address is not set and address server (%s) returned none',
                        $this->topAddressing->wsAddr,
                    ),
                    ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
                );
            }
            // 周期刷新（Java scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min)）
            $this->namesrvRefreshEnabled = true;
        }

        $now = $this->currentTimeMillis();
        $this->routeRefreshNextRunMillis = $now + self::ROUTE_REFRESH_INITIAL_DELAY_MILLIS;
        $this->adjustPoolNextRunMillis = $now + self::ADJUST_POOL_INITIAL_DELAY_MILLIS;
        $this->namesrvRefreshNextRunMillis = $this->namesrvRefreshEnabled
            ? $now + self::NAMESRV_REFRESH_INITIAL_DELAY_MILLIS
            : null;
        $this->statsSampleNextRunMillis = $now + self::STATS_SAMPLE_INTERVAL_MILLIS;
        $this->requestScanNextRunMillis = $now + self::REQUEST_SCAN_INITIAL_DELAY_MILLIS;

        $this->pumpStartupOnce();
    }

    public function shutdown(): void
    {
        $this->started = false;
        $this->namesrvRefreshEnabled = false;
        $this->namesrvRefreshNextRunMillis = null;
        $this->adjustPoolNextRunMillis = null;
        $this->routeRefreshNextRunMillis = null;
        $this->statsSampleNextRunMillis = null;
        $this->requestScanNextRunMillis = null;
        $this->pendingActions = [];
        $this->consumerStatsManager->shutdown();
        $this->remotingClient->shutdown();
    }

    public function isStarted(): bool
    {
        return $this->started;
    }

    /**
     * 显式事件泵（替换 Python 的后台线程）。调用方（通常是主循环 / 测试）周期调用。
     *
     * @param int|null $nowMillis 可注入的当前时间（毫秒），为空时用内部时钟。
     */
    public function tick(?int $nowMillis = null): void
    {
        $now = $nowMillis ?? $this->currentTimeMillis();

        // 原「后台线程」延迟队列（如 resetOffset 触发的 rebalance）
        $this->runPendingActions();

        if (!$this->started) {
            return;
        }

        // ①动态 NameServer 刷新
        if ($this->namesrvRefreshEnabled
            && $this->namesrvRefreshNextRunMillis !== null
            && $now >= $this->namesrvRefreshNextRunMillis) {
            $this->namesrvRefreshOnce();
            $this->namesrvRefreshNextRunMillis = $now + self::NAMESRV_REFRESH_INTERVAL_MILLIS;
        }

        // ②线程弹性巡检
        if ($this->adjustPoolNextRunMillis !== null && $now >= $this->adjustPoolNextRunMillis) {
            $this->adjustThreadPool();
            $this->adjustPoolNextRunMillis = $now + self::ADJUST_POOL_INTERVAL_MILLIS;
        }

        // ③路由刷新（周期 = pollNameServerInterval）
        if ($this->routeRefreshNextRunMillis !== null && $now >= $this->routeRefreshNextRunMillis) {
            $this->routeRefreshOnce();
            $this->routeRefreshNextRunMillis = $now + $this->pollNameServerInterval;
        }

        // ④消费统计采样
        if ($this->statsSampleNextRunMillis !== null && $now >= $this->statsSampleNextRunMillis) {
            $this->consumerStatsManager->sampleOnce();
            $this->statsSampleNextRunMillis = $now + self::STATS_SAMPLE_INTERVAL_MILLIS;
        }

        // ⑤RequestFutureHolder TTL 清扫（Java RequestHouseKeepingService 单线程
        //   scheduleAtFixedRate(scanExpiredRequest, 3s, 1s)；PHP 单线程下没有并发，
        //   只需保证同一请求的超时路径与应答路径只有一条生效 —— putResponse 用
        //   「摘到才负责」的 remove 语义，天然互斥）。
        if ($this->requestScanNextRunMillis !== null && $now >= $this->requestScanNextRunMillis) {
            $this->requestScanNextRunMillis = $now + self::REQUEST_SCAN_INTERVAL_MILLIS;
            try {
                RequestFutureHolder::getInstance()->scanExpiredRequest();
            } catch (\Throwable $e) {
                Logger::warning('scan RequestFutureTable exception: ' . $e->getMessage());
            }
        }
    }

    /** tick() 的别名（用户要求的替代命名）。 */
    public function pumpOnce(?int $nowMillis = null): void
    {
        $this->tick($nowMillis);
    }

    /** 动态 NameServer 刷新一轮（对应 Python _namesrv_refresh_loop 循环体）。 */
    public function namesrvRefreshOnce(): void
    {
        try {
            $this->fetchNameServerAddr();
        } catch (\Throwable $e) {
            Logger::debug('fetchNameServerAddr exception: ' . $e->getMessage());
        }
    }

    /** 路由刷新一轮（对应 Python _route_refresh_loop 循环体）。 */
    public function routeRefreshOnce(): void
    {
        foreach (array_keys($this->topicsInUse) as $topic) {
            try {
                $this->updateTopicRouteInfoFromNameServer($topic);
            } catch (\Throwable $e) {
                Logger::debug(sprintf('route refresh failed for %s: %s', $topic, $e->getMessage()));
            }
        }
    }

    /**
     * start() 的同步「首泵」：首轮路由刷新 + 一次心跳 + CHECK_CLIENT_CONFIG(46)（best-effort）。
     */
    private function pumpStartupOnce(): void
    {
        $this->routeRefreshOnce();
        $this->heartbeatOnce();
        try {
            $this->checkClientInBroker();
        } catch (\Throwable $e) {
            Logger::warning('checkClientInBroker on start failed: ' . $e->getMessage());
        }
    }

    /** 向所有已知 broker 发一次心跳（默认空 HeartbeatData，仅 clientId）。 */
    public function heartbeatOnce(int $timeoutMillis = 5000): void
    {
        $data = new HeartbeatData($this->clientId);
        foreach ($this->getAllBrokerAddrs() as $addr) {
            try {
                $this->sendHeartbeat($addr, $data, $timeoutMillis);
            } catch (\Throwable $e) {
                Logger::debug(sprintf('heartbeatOnce failed, addr=%s: %s', $addr, $e->getMessage()));
            }
        }
    }

    /** 执行并清空延迟动作队列（原 Python 的「丢到后台线程」）。 */
    public function runPendingActions(): void
    {
        while ($this->pendingActions !== []) {
            $action = array_shift($this->pendingActions);
            try {
                $action();
            } catch (\Throwable $e) {
                Logger::warning('pending action failed: ' . $e->getMessage());
            }
        }
    }

    private function enqueueAction(callable $action): void
    {
        $this->pendingActions[] = $action;
    }

    // ---------------- 动态 NameServer ----------------

    /**
     * 对应 Java MQClientAPIImpl.fetchNameServerAddr：地址变化才应用。
     */
    public function fetchNameServerAddr(): ?string
    {
        if ($this->topAddressing->wsAddr === '') {
            return null;
        }
        $changed = $this->topAddressing->fetchAndApply();
        if ($changed !== null && $changed !== '') {
            $addrs = [];
            foreach (explode(';', $changed) as $a) {
                $a = trim($a);
                if ($a !== '') {
                    $addrs[] = $a;
                }
            }
            $this->updateNameServerAddressList($addrs);
        }
        return $changed;
    }

    /** @param list<string> $addrs */
    public function updateNameServerAddressList(array $addrs): void
    {
        if ($addrs !== []) {
            $this->nameServerAddrs = array_values($addrs);
        }
    }

    // ---------------- 消费者注册 / 线程巡检 ----------------

    public function adjustThreadPool(): void
    {
        foreach ($this->consumerTable as $group => $consumer) {
            if ($consumer === null) {
                continue;
            }
            try {
                $consumer->adjustThreadPool();
            } catch (\Throwable $e) {
                Logger::debug(sprintf('adjustThreadPool failed for group %s: %s', $group, $e->getMessage()));
            }
        }
    }

    public function registerConsumer(string $group, object $consumer): void
    {
        $this->consumerTable[$group] = $consumer;
    }

    public function unregisterConsumer(string $group): void
    {
        unset($this->consumerTable[$group]);
    }

    public function findConsumer(string $group): ?object
    {
        return $this->consumerTable[$group] ?? null;
    }

    // ---------------- CHECK_CLIENT_CONFIG(46) ----------------

    /**
     * 对应 Java MQClientInstance#checkClientInBroker:534。
     *
     * 只把**非 TAG**（SQL92 / CLASS_FILTER）表达式发给 broker 校验。某消费者「无订阅」时是
     * ``return`` 而不是 ``continue``（Java 源码如此）；查不到路由时跳过该订阅。
     */
    public function checkClientInBroker(): void
    {
        foreach ($this->consumerTable as $group => $consumer) {
            $subs = $consumer !== null ? $consumer->subscriptions() : null;
            if ($subs === null || $subs === [] || $subs === false) {
                // 对齐 Java 的 return：不是 continue。
                return;
            }
            $this->checkSubscriptionsInBroker($group, $subs);
        }
    }

    /**
     * @param iterable<SubscriptionData> $subs
     */
    public function checkSubscriptionsInBroker(string $group, iterable $subs): void
    {
        foreach ($subs as $sub) {
            // Java ExpressionType.isTagType：null / "" / "TAG" 都算 TAG，一律跳过。
            if ($sub === null) {
                continue;
            }
            $exprType = $sub->getExpressionType();
            if ($exprType === '' || $exprType === ExpressionType::TAG) {
                continue;
            }
            $addr = $this->findBrokerAddrByTopic($sub->getTopic() ?? '');
            if ($addr === null) {
                continue;
            }
            try {
                $this->checkClientConfig($addr, $group, $this->clientId, $sub);
            } catch (MQClientException $e) {
                throw $e;
            } catch (\Throwable $e) {
                throw new MQClientException(
                    sprintf(
                        'Check client in broker error, maybe because you use %s to filter '
                        . 'message, but server has not been upgraded to support!This error would '
                        . 'not affect the launch of consumer, but may has impact on message '
                        . 'receiving if you have use the new features which are not supported by '
                        . 'server, please check the log!',
                        $exprType,
                    ),
                    null,
                    $e,
                );
            }
        }
    }

    // ---------------- 40 NOTIFY_CONSUMER_IDS_CHANGED ----------------

    public function consumerIdsChangedCount(): int
    {
        return $this->consumerIdsChangedCount;
    }

    public function rebalanceImmediately(): void
    {
        foreach ($this->consumerTable as $consumer) {
            if ($consumer === null) {
                continue;
            }
            $wake = [$consumer, 'rebalanceImmediately'];
            if (!is_callable($wake)) {
                $wake = [$consumer, 'rebalance_immediately'];
            }
            if (!is_callable($wake)) {
                continue;
            }
            try {
                $wake();
            } catch (\Throwable $e) {
                Logger::warning('rebalance_immediately failed: ' . $e->getMessage());
            }
        }
    }

    /**
     * 对应 Java ClientRemotingProcessor#notifyConsumerIdsChanged。broker 用 invokeOneway 发，
     * Java 返回 null ⇒ 不回包。group 只用于日志，整组一起唤醒。
     */
    public function processNotifyConsumerIdsChanged(RemotingCommand $cmd, string $addr): ?RemotingCommand
    {
        $header = new NotifyConsumerIdsChangedRequestHeader();
        $header->fromExtFields($cmd->extFields);
        $this->consumerIdsChangedCount++;
        Logger::info(sprintf(
            "receive broker's notification[%s], the consumer group: %s changed, rebalance immediately",
            $addr,
            (string) $header->consumerGroup,
        ));
        $this->rebalanceImmediately();
        return null;
    }

    // ---------------- broker 主动请求处理（ClientRemotingProcessor） ----------------

    /**
     * RESET_CONSUMER_CLIENT_OFFSET(220)：broker 用 invokeOneway 发，无需应答。
     *
     * 重置逻辑里会触发 rebalance（lock/unlock 等 invokeSync），不能在本轮同步跑
     * —— 原 Python 丢后台线程，这里进 pending 队列由下次 tick() 执行。
     */
    public function processResetOffset(RemotingCommand $cmd, string $addr): ?RemotingCommand
    {
        $header = new ResetOffsetRequestHeader();
        $header->fromExtFields($cmd->extFields);
        $group = (string) $header->group;
        $consumer = $group !== '' ? $this->findConsumer($group) : null;
        if ($consumer === null) {
            Logger::warning(sprintf('RESET_CONSUMER_CLIENT_OFFSET: no consumer for group=%s', $group));
            return null;
        }
        $topic = (string) $header->topic;
        // PHP 端 offset 表用 ResetOffsetBody 原生的 list<array{mq,offset}> 形态
        // （MessageQueue 是对象，不能作数组键）。
        /** @var list<array{mq: MessageQueue, offset: int}> $offsetTable */
        $offsetTable = [];
        if ($cmd->body !== null && $cmd->body !== '') {
            try {
                $offsetTable = ResetOffsetBody::decode($cmd->body)->offsetTable;
            } catch (\Throwable $e) {
                Logger::warning('RESET_CONSUMER_CLIENT_OFFSET: not map-form body: ' . $e->getMessage());
            }
            if ($offsetTable === []) {
                try {
                    foreach (ResetOffsetBodyForC::decode($cmd->body)->offsetTable as $e) {
                        $offsetTable[] = [
                            'mq' => new MessageQueue($e->topic, $e->brokerName, $e->queueId),
                            'offset' => $e->offset,
                        ];
                    }
                } catch (\Throwable $e) {
                    Logger::warning('RESET_CONSUMER_CLIENT_OFFSET: bad body: ' . $e->getMessage());
                    return null;
                }
            }
        }

        $this->enqueueAction(function () use ($consumer, $topic, $offsetTable, $group): void {
            try {
                $consumer->resetOffset($topic, $offsetTable);
            } catch (\Throwable $e) {
                Logger::warning(sprintf(
                    'reset offset failed (group=%s topic=%s): %s',
                    $group,
                    $topic,
                    $e->getMessage(),
                ));
            }
        });
        return null;
    }

    /** GET_CONSUMER_STATUS_FROM_CLIENT(221)：返回已消费位点表。 */
    public function processGetConsumerStatus(RemotingCommand $cmd, string $addr): ?RemotingCommand
    {
        $header = new GetConsumerStatusRequestHeader();
        $header->fromExtFields($cmd->extFields);
        $group = (string) $header->group;
        $consumer = $group !== '' ? $this->findConsumer($group) : null;
        if ($consumer === null) {
            return RemotingCommand::createResponseCommand(
                ResponseCode::SYSTEM_ERROR,
                sprintf('no consumer for group=%s', $group),
            );
        }
        $status = $consumer->getConsumerStatus((string) $header->topic);
        $body = new GetConsumerStatusBody();
        $body->messageQueueTable = is_array($status) ? array_values($status) : [];
        $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $resp->body = $body->encode();
        return $resp;
    }

    /** GET_CONSUMER_RUNNING_INFO(307)：返回本消费者运行信息。 */
    public function processGetConsumerRunningInfo(RemotingCommand $cmd, string $addr): ?RemotingCommand
    {
        $header = new GetConsumerRunningInfoRequestHeader();
        $header->fromExtFields($cmd->extFields);
        $group = (string) $header->consumerGroup;
        $consumer = $group !== '' ? $this->findConsumer($group) : null;
        if ($consumer === null) {
            return RemotingCommand::createResponseCommand(
                ResponseCode::SYSTEM_ERROR,
                sprintf('no consumer for group=%s', $group),
            );
        }
        $info = $consumer->consumerRunningInfo();
        $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $resp->body = $info->encode();
        return $resp;
    }

    /** CONSUME_MESSAGE_DIRECTLY(309)：broker 把一条消息推下来，要求本地真实消费一次。 */
    public function processConsumeMessageDirectly(RemotingCommand $cmd, string $addr): ?RemotingCommand
    {
        $header = new ConsumeMessageDirectlyResultRequestHeader();
        $header->fromExtFields($cmd->extFields);
        $group = (string) $header->consumerGroup;
        $consumer = $group !== '' ? $this->findConsumer($group) : null;
        if ($consumer === null) {
            return RemotingCommand::createResponseCommand(
                ResponseCode::SYSTEM_ERROR,
                sprintf('no consumer for group=%s', $group),
            );
        }
        if ($cmd->body === null || $cmd->body === '') {
            return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'empty message body');
        }
        $msg = MessageDecoder::decodeMessage($cmd->body, true, true, true, false);
        if ($msg === null) {
            return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'decode message failed');
        }
        $result = $consumer->consumeMessageDirectly($msg, $header->brokerName);
        $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $resp->body = $result->encode();
        return $resp;
    }

    // ---------------- 底层 invoke ----------------

    private function invokeSync(string $addr, RemotingCommand $request, ?int $timeoutMillis = null): RemotingCommand
    {
        return $this->remotingClient->invokeSync($addr, $request, $timeoutMillis);
    }

    private function checkResponse(RemotingCommand $response): RemotingCommand
    {
        if ($response->code === ResponseCode::SUCCESS) {
            return $response;
        }
        throw new MQBrokerException($response->code, $response->remark ?? '');
    }

    // ---------------- 路由管理 ----------------

    /**
     * 拉取并落库 topic 路由。
     *
     * ``isDefault`` 对应 Java updateTopicRouteInfoFromNameServer(topic, isDefault, producer)：
     * **只有生产者**在真实路由拉不到时才回退到 TBW102 为新 topic 合成发布信息。
     */
    public function updateTopicRouteInfoFromNameServer(string $topic, int $timeoutMillis = 5000, bool $isDefault = false): bool
    {
        if ($this->nameServerAddrs === []) {
            // Java 这里只 log.warn 并返回 false；本端口路由拉取是拉不到就抛，直接把同一个码带上。
            throw new MQClientException(
                'name server address list is empty',
                ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
            );
        }

        $fetch = function (string $t) use ($timeoutMillis): ?TopicRouteData {
            $request = RemotingCommand::createRequestCommand(RequestCode::GET_ROUTEINFO_BY_TOPIC, null);
            $request->extFields['topic'] = $t;
            $lastExc = null;
            foreach ($this->nameServerAddrs as $nsAddr) {
                try {
                    $response = $this->invokeSync($nsAddr, $request, $timeoutMillis);
                } catch (\Throwable $e) {
                    // 连接/发送/超时级别失败 → 换下一个 name server 再试。
                    $lastExc = $e;
                    continue;
                }
                // 拿到应答：无论 SUCCESS 还是 TOPIC_NOT_EXIST，都是终局答复，不再轮询。
                if ($response->code === ResponseCode::SUCCESS && $response->body !== null && $response->body !== '') {
                    return TopicRouteData::decode($response->body);
                }
                return null;
            }
            if ($lastExc !== null && !($lastExc instanceof MQBrokerException)) {
                throw $lastExc;
            }
            return null;
        };

        $route = $fetch($topic);
        if ($route === null && $isDefault && $topic !== MixAll::DEFAULT_TOPIC) {
            $route = $fetch(MixAll::DEFAULT_TOPIC);
            if ($route !== null) {
                // 新 topic 由 broker 用 default_topic_queue_nums 建队列，按 broker 实际创建数裁剪。
                $cap = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
                foreach ($route->queueDatas as $qd) {
                    $qd->writeQueueNums = min($qd->writeQueueNums, $cap);
                    $qd->readQueueNums = min($qd->readQueueNums, $cap);
                }
            }
        }
        if ($route === null) {
            return false;
        }

        // Python: with self.topic_route_lock（PHP 单线程顺序执行）
        $this->topicRouteTable[$topic] = $route;
        foreach ($route->getBrokerDatas() as $bd) {
            $this->brokerAddrTable[$bd->brokerName] = $bd->brokerAddrs;
        }
        $publish = $this->topicPublishInfoTable[$topic] ??= new TopicPublishInfo();
        $publish->orderTopic = $route->orderTopicConf !== null;
        $publish->topicRouteData = $route;
        $publish->msgQueueList = $route->getAllMessageQueue($topic);
        return true;
    }

    public function getTopicPublishInfo(string $topic, bool $isDefault = false): TopicPublishInfo
    {
        $info = $this->topicPublishInfoTable[$topic] ?? null;
        if ($info !== null && $info->ok()) {
            return $info;
        }
        $this->updateTopicRouteInfoFromNameServer($topic, 5000, $isDefault);
        $info = $this->topicPublishInfoTable[$topic] ?? null;
        if ($info === null || !$info->ok()) {
            throw new MQClientException(sprintf('Can not find Message Queue for topic: %s', $topic));
        }
        return $info;
    }

    public function getTopicRouteData(string $topic): ?TopicRouteData
    {
        $route = $this->topicRouteTable[$topic] ?? null;
        if ($route !== null) {
            return $route;
        }
        try {
            $this->updateTopicRouteInfoFromNameServer($topic);
        } catch (\Throwable) {
            // 对齐 Python：补拉失败静默，返回缓存（此时为 null）
        }
        return $this->topicRouteTable[$topic] ?? null;
    }

    /**
     * 本实例订阅该 topic 时应看到的全部队列（Java RebalanceImpl.topicSubscribeInfoTable）。
     *
     * @return list<MessageQueue>
     */
    public function getTopicSubscribeInfo(string $topic): array
    {
        $route = $this->getTopicRouteData($topic);
        if ($route === null) {
            return [];
        }
        return $route->getAllSubscribeMessageQueue($topic);
    }

    public static function findBrokerAddrInRoute(TopicRouteData $route, string $brokerName): ?string
    {
        foreach ($route->getBrokerDatas() as $brokerData) {
            if ($brokerData->brokerName === $brokerName) {
                return $brokerData->selectBrokerAddr();
            }
        }
        return null;
    }

    /**
     * 对应 Java MQClientInstance#findBrokerAddrByTopic:1390：从路由里随机挑一个 broker
     * （优先 master 地址）；查不到返回 null（不抛）。
     */
    public function findBrokerAddrByTopic(string $topic): ?string
    {
        $route = $this->getTopicRouteData($topic);
        if ($route === null) {
            return null;
        }
        $brokers = $route->getBrokerDatas();
        if ($brokers === []) {
            return null;
        }
        return $brokers[array_rand($brokers)]->selectBrokerAddr();
    }

    // ---------------- 消息发送 ----------------

    /**
     * VIP 通道换算（Java 每个invoke 点的 ``MixAll.brokerVIPChannel(isVip, addr)``）。
     * 关闭或端口不可解析时原样返回；**只在发请求前套用一次**（端口会 -2，重复套用会
     * 连减多次 —— 调用方不得对同一地址二次换算）。
     */
    public function vipAddrFor(string $addr): string
    {
        return MixAll::brokerVipChannel($this->vipChannelEnabled, $addr);
    }

    /**
     * Java 侧「只要主」的地址解析：查发布地址（只认 brokerId=0）→ 查不到按 topic 刷一次路由
     * → 重查 → 仍查不到抛 "The broker[X] not exist"。
     */
    public function publishAddrFor(string $brokerName, string $topic): string
    {
        $addr = $this->findBrokerAddressInPublish($brokerName);
        if ($addr === null || $addr === '') {
            $this->updateTopicRouteInfoFromNameServer($topic);
            $addr = $this->findBrokerAddressInPublish($brokerName);
        }
        if ($addr === null || $addr === '') {
            throw new MQClientException(sprintf('The broker[%s] not exist', $brokerName));
        }
        return $addr;
    }

    public function sendMessage(
        string $producerGroup,
        Message $msg,
        MessageQueue $mq,
        int $timeoutMillis = 3000,
        int $sysFlag = 0,
        bool $unitMode = false,
        ?string $defaultTopic = null,
        ?int $defaultTopicQueueNums = null,
    ): SendResult {
        return $this->sendMessageToAddr(
            $producerGroup,
            $msg,
            $mq,
            $this->publishAddrFor($mq->brokerName, $mq->topic),
            $timeoutMillis,
            $sysFlag,
            $unitMode,
            $defaultTopic,
            $defaultTopicQueueNums,
        );
    }

    public function sendMessageToAddr(
        string $producerGroup,
        Message $msg,
        MessageQueue $mq,
        string $addr,
        int $timeoutMillis = 3000,
        int $sysFlag = 0,
        bool $unitMode = false,
        ?string $defaultTopic = null,
        ?int $defaultTopicQueueNums = null,
    ): SendResult {
        $request = $this->buildSendRequestInternal(
            $producerGroup,
            $msg,
            $mq,
            $timeoutMillis,
            $sysFlag,
            $unitMode,
            $defaultTopic,
            $defaultTopicQueueNums,
        );
        // Java MQClientAPIImpl.sendMessage —— 发送路径走 VIP 通道换算
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
        return $this->parseSendResponse($response, $msg, $mq);
    }

    public function sendMessageOneway(
        string $producerGroup,
        Message $msg,
        MessageQueue $mq,
        string $addr,
        int $timeoutMillis = 3000,
        int $sysFlag = 0,
        bool $unitMode = false,
        ?string $defaultTopic = null,
        ?int $defaultTopicQueueNums = null,
    ): void {
        $request = $this->buildSendRequestInternal(
            $producerGroup,
            $msg,
            $mq,
            $timeoutMillis,
            $sysFlag,
            $unitMode,
            $defaultTopic,
            $defaultTopicQueueNums,
        );
        $request->markOnewayRPC();
        $this->remotingClient->invokeOneway($this->vipAddrFor($addr), $request);
    }

    /** 只**构建** SEND_MESSAGE 请求对象、不发送（异步发送链需要跨重试复用同一请求）。 */
    public function buildSendRequest(
        string $producerGroup,
        Message $msg,
        MessageQueue $mq,
        int $timeoutMillis = 3000,
        int $sysFlag = 0,
        bool $unitMode = false,
        ?string $defaultTopic = null,
        ?int $defaultTopicQueueNums = null,
    ): RemotingCommand {
        return $this->buildSendRequestInternal(
            $producerGroup,
            $msg,
            $mq,
            $timeoutMillis,
            $sysFlag,
            $unitMode,
            $defaultTopic,
            $defaultTopicQueueNums,
        );
    }

    /**
     * 异步发出一个已构建好的发送请求（对应 Java MQClientAPIImpl#sendMessageAsync）。
     *
     * ``$onComplete`` 形如 ``function (?SendResult $result, ?\Throwable $error): void``，
     * 恰好回调一次。响应解析也放在这一层做（Java processSendResponse 在 operationSucceed 里调）。
     *
     * @param callable(?SendResult, ?\Throwable): void $onComplete
     */
    public function sendMessageAsync(
        string $addr,
        RemotingCommand $request,
        Message $msg,
        MessageQueue $mq,
        int $timeoutMillis,
        callable $onComplete,
    ): void {
        $onSuccess = function (?RemotingCommand $response) use ($onComplete, $msg, $mq): void {
            try {
                $onComplete($this->parseSendResponse($response, $msg, $mq), null);
            } catch (\Throwable $parseError) {
                $onComplete(null, $parseError);
            }
        };
        $onFailure = function (\Throwable $error) use ($onComplete): void {
            $onComplete(null, $error);
        };
        $this->remotingClient->invokeAsync(
            $this->vipAddrFor($addr),
            $request,
            \Closure::fromCallable($onSuccess),
            \Closure::fromCallable($onFailure),
            $timeoutMillis,
        );
    }

    private function buildSendRequestInternal(
        string $producerGroup,
        Message $msg,
        MessageQueue $mq,
        int $timeoutMillis = 3000,
        int $sysFlag = 0,
        bool $unitMode = false,
        ?string $defaultTopic = null,
        ?int $defaultTopicQueueNums = null,
    ): RemotingCommand {
        // 非批量消息在发请求前补 UNIQ_KEY；批量消息在 MessageBatch 构造时已逐条写好。
        if (!($msg instanceof MessageBatch)) {
            MessageClientIdSetter::setUniqId($msg);
        }
        $header = new SendMessageRequestHeaderV2();
        $header->producerGroup = $producerGroup;
        $header->topic = $msg->topic;
        $header->defaultTopic = $defaultTopic ?? MixAll::DEFAULT_TOPIC;
        $header->defaultTopicQueueNums = $defaultTopicQueueNums ?? MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
        $header->queueId = $mq->queueId;
        $header->sysFlag = $sysFlag;
        $header->bornTimestamp = $this->currentTimeMillis();
        $header->flag = $msg->flag;
        $header->properties = MessageDecoder::messageProperties2String($msg->properties);
        $header->reconsumeTimes = 0;
        $header->unitMode = $unitMode;
        // ⚠ maxReconsumeTimes 只在「发往 %RETRY% 且消息带 MAX_RECONSUME_TIMES 属性」时才下发。
        $header->maxReconsumeTimes = null;
        // 发往 %RETRY%<group> 时把重试属性抬进请求头（broker 读的是 requestHeader）。
        if (str_starts_with((string) $header->topic, MixAll::RETRY_GROUP_TOPIC_PREFIX)) {
            $reconsumeTimes = MessageAccessor::getReconsumeTime($msg);
            if ($reconsumeTimes !== null) {
                $header->reconsumeTimes = (int) $reconsumeTimes;
                MessageAccessor::clearProperty($msg, MessageConst::PROPERTY_RECONSUME_TIME);
            }
            $maxReconsumeTimes = MessageAccessor::getMaxReconsumeTimes($msg);
            if ($maxReconsumeTimes !== null) {
                $header->maxReconsumeTimes = (int) $maxReconsumeTimes;
                MessageAccessor::clearProperty($msg, MessageConst::PROPERTY_MAX_RECONSUME_TIMES);
            }
        }
        $header->batch = $msg instanceof MessageBatch;
        // V2 的 brokerName 键是单字母 `n`。
        $header->brokerName = $mq->brokerName !== '' ? $mq->brokerName : null;

        // reply 优先于 batch（对齐 Java MQClientAPIImpl.sendMessage:550-563）。
        if (MessageUtil::isReplyMessage($msg)) {
            $code = RequestCode::SEND_REPLY_MESSAGE_V2;
        } elseif ($header->batch) {
            $code = RequestCode::SEND_BATCH_MESSAGE;
        } else {
            $code = RequestCode::SEND_MESSAGE_V2;
        }
        $request = RemotingCommand::createRequestCommand($code, $header);
        $request->body = self::encodeBody($msg);
        return $request;
    }

    /** 对应 Java MQClientAPIImpl.sendMessage：request.setBody(msg.getBody())。 */
    public static function encodeBody(Message $msg): string
    {
        return $msg->getBody() ?? '';
    }

    public function parseSendResponse(RemotingCommand $response, Message $msg, MessageQueue $mq): SendResult
    {
        $statusMap = [
            ResponseCode::SUCCESS => SendStatus::SEND_OK,
            ResponseCode::FLUSH_DISK_TIMEOUT => SendStatus::FLUSH_DISK_TIMEOUT,
            ResponseCode::FLUSH_SLAVE_TIMEOUT => SendStatus::FLUSH_SLAVE_TIMEOUT,
            ResponseCode::SLAVE_NOT_AVAILABLE => SendStatus::SLAVE_NOT_AVAILABLE,
        ];
        if (!array_key_exists($response->code, $statusMap)) {
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }
        $header = new SendMessageResponseHeader();
        $header->fromExtFields($response->extFields);
        // msgId = 客户端唯一 ID（批量用批量自身 UNIQ_KEY）；offsetMsgId = 响应头 msgId；
        // regionId = 响应头 MSG_REGION（缺省回落 DefaultRegion）；traceOn = TRACE_ON != "false"。
        $uniqMsgId = MessageClientIdSetter::getUniqId($msg) ?: $header->batchUniqId;
        $result = new SendResult(
            sendStatus: $statusMap[$response->code],
            msgId: $uniqMsgId ?: $header->msgId,
            messageQueue: new MessageQueue(
                $mq->topic,
                $mq->brokerName,
                $header->queueId ?? $mq->queueId,
            ),
            queueOffset: $header->queueOffset ?? 0,
            transactionId: $header->transactionId,
            offsetMsgId: $header->msgId,
        );
        $ext = $response->extFields;
        $regionId = $ext[MessageConst::PROPERTY_MSG_REGION] ?? null;
        $result->regionId = ($regionId !== null && $regionId !== '')
            ? (string) $regionId
            : MixAll::DEFAULT_TRACE_REGION_ID;
        $result->traceOn = (string) ($ext[MessageConst::PROPERTY_TRACE_SWITCH] ?? '') !== 'false';
        $result->recallHandle = $header->recallHandle;
        return $result;
    }

    // ---------------- 定时消息撤回 ----------------

    /** RECALL_MESSAGE(370)，对应 Java MQClientAPIImpl#recallMessage。 */
    public function recallMessage(string $addr, RecallMessageRequestHeader $header, int $timeoutMillis): string
    {
        $request = RemotingCommand::createRequestCommand(RequestCode::RECALL_MESSAGE, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        if ($response->code === ResponseCode::SUCCESS) {
            $respHeader = new RecallMessageResponseHeader();
            $respHeader->fromExtFields($response->extFields);
            if ($respHeader->msgId === null || $respHeader->msgId === '') {
                throw new MQBrokerException(
                    $response->code,
                    sprintf('recall message response has no msgId, addr %s', $addr),
                );
            }
            return $respHeader->msgId;
        }
        throw new MQBrokerException($response->code, $response->remark ?? '');
    }

    // ---------------- Request-Reply：接收 broker 推回的应答 ----------------

    /**
     * 处理 PUSH_REPLY_MESSAGE_TO_CLIENT(326)：把应答交给等待中的 request()。
     *
     * 与 Java 一样**必须回一个响应**（broker 侧是 invokeSync，不回会超时）。
     */
    public function processReplyMessage(RemotingCommand $cmd, string $addr): ?RemotingCommand
    {
        $header = new ReplyMessageRequestHeader();
        $header->fromExtFields($cmd->extFields);
        try {
            $body = $cmd->body ?? '';
            // sysFlag 里带压缩标志时要先解压：326 推的是裸包，不走消息解码路径。
            if (MessageSysFlag::isCompressed($header->sysFlag ?? 0)) {
                $body = MessageDecoder::decompressBody(
                    $body,
                    MessageSysFlag::getCompressionType($header->sysFlag ?? 0),
                );
            }
            $msg = new MessageExt(topic: $header->topic ?? '', body: $body);
            $msg->queueId = $header->queueId ?? 0;
            $msg->storeTimestamp = $header->storeTimestamp ?? 0;
            $msg->flag = $header->flag ?? 0;
            $msg->bornTimestamp = $header->bornTimestamp ?? 0;
            $msg->reconsumeTimes = $header->reconsumeTimes ?? 0;
            if ($header->bornHost !== null && $header->bornHost !== '') {
                $msg->bornHost = $header->bornHost;
            }
            if ($header->storeHost !== null && $header->storeHost !== '') {
                $msg->storeHost = $header->storeHost;
            }
            $msg->properties = MessageDecoder::string2MessageProperties($header->properties);
            $msg->properties[MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME] = (string) $this->currentTimeMillis();
            $correlationId = $msg->properties[MessageConst::PROPERTY_CORRELATION_ID] ?? null;
            if ($correlationId === null
                || RequestFutureHolder::getInstance()->putResponse($correlationId, $msg) === null) {
                // 查不到是正常情况（请求已超时 / 应答重复），Java 此处也是 warn。
                Logger::warning(sprintf(
                    'receive reply message, but not matched any request, CorrelationId: %s, reply from host: %s',
                    (string) $correlationId,
                    (string) $header->bornHost,
                ));
            }
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        } catch (\Throwable $e) {
            Logger::warning('unknown err when receiveReplyMsg: ' . $e->getMessage());
            return RemotingCommand::createResponseCommand(
                ResponseCode::SYSTEM_ERROR,
                sprintf('process reply message fail: %s', $e->getMessage()),
            );
        }
    }

    // ---------------- 消息拉取 ----------------

    public function pullMessage(
        string $consumerGroup,
        MessageQueue $mq,
        int $queueOffset,
        int $maxMsgNums,
        int $sysFlag,
        int $commitOffset,
        ?string $subscription,
        int $subVersion,
        ?string $expressionType,
        int $timeoutMillis = 30000,
        int $maxMsgBytes = -1,
        int $suspendTimeoutMillis = 15000,
        ?string $addr = null,
        int $requestSource = 0,
        ?int $brokerId = null,
    ): PullResult {
        $slave = false;
        if ($addr === null) {
            $route = $this->getTopicRouteData($mq->topic);
            if ($route === null) {
                throw new MQClientException(sprintf('No route info of this topic: %s', $mq->topic));
            }
            if ($brokerId !== null) {
                $brokerData = null;
                foreach ($route->brokerDatas as $bd) {
                    if ($bd->brokerName === $mq->brokerName) {
                        $brokerData = $bd;
                        break;
                    }
                }
                if ($brokerData === null) {
                    throw new MQClientException(sprintf('Broker %s not exist', $mq->brokerName));
                }
                [$addr, $slave] = self::findBrokerAddrInSubscribe($brokerData->brokerAddrs, $brokerId);
                if ($addr === null) {
                    throw new MQClientException(sprintf('Broker %s not exist', $mq->brokerName));
                }
            } else {
                $addr = self::findBrokerAddrInRoute($route, $mq->brokerName);
                if ($addr === null) {
                    throw new MQClientException(
                        sprintf('Broker %s not found in route of topic %s', $mq->brokerName, $mq->topic),
                    );
                }
            }
        }
        if ($slave) {
            // Java pullKernelImpl:219-221：从节点上位点提交没有意义，清 COMMIT_OFFSET 位。
            $sysFlag = PullSysFlag::clearCommitOffsetFlag($sysFlag);
        }
        $header = new PullMessageRequestHeader();
        $header->consumerGroup = $consumerGroup;
        $header->topic = $mq->topic;
        $header->queueId = $mq->queueId;
        $header->queueOffset = $queueOffset;
        $header->maxMsgNums = $maxMsgNums;
        $header->sysFlag = $sysFlag;
        $header->commitOffset = $commitOffset;
        $header->suspendTimeoutMillis = $suspendTimeoutMillis;
        $header->subscription = $subscription;
        $header->subVersion = $subVersion;
        $header->expressionType = $expressionType;
        $header->maxMsgBytes = $maxMsgBytes;
        $header->requestSource = $requestSource;
        // lite pull 位决定请求码 LITE_PULL_MESSAGE(361) vs PULL_MESSAGE(11)。
        $code = PullSysFlag::hasLitePullFlag($sysFlag)
            ? RequestCode::LITE_PULL_MESSAGE
            : RequestCode::PULL_MESSAGE;
        $request = RemotingCommand::createRequestCommand($code, $header);
        // Java MQClientAPIImpl.pullMessage —— 拉取路径同样走 VIP 通道换算
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);

        if ($response->code === ResponseCode::SUCCESS) {
            $status = PullStatus::FOUND;
        } elseif ($response->code === ResponseCode::PULL_NOT_FOUND) {
            $status = PullStatus::NO_NEW_MSG;
        } elseif ($response->code === ResponseCode::PULL_OFFSET_MOVED) {
            $status = PullStatus::OFFSET_ILLEGAL;
        } elseif ($response->code === ResponseCode::PULL_RETRY_IMMEDIATELY) {
            $status = PullStatus::NO_MATCHED_MSG;
        } else {
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }

        $respHeader = new PullMessageResponseHeader();
        $respHeader->fromExtFields($response->extFields);
        $found = [];
        if ($response->body !== null && $response->body !== '') {
            $found = MessageDecoder::decodeMessages($response->body);
            foreach ($found as $m) {
                $m->brokerName = $mq->brokerName;
                $m->queueId = $mq->queueId;
            }
        }
        return new PullResult(
            $status,
            $respHeader->nextBeginOffset ?? 0,
            $respHeader->minOffset ?? 0,
            $respHeader->maxOffset ?? 0,
            $found,
            $respHeader->suggestWhichBrokerId,
        );
    }

    /**
     * 对应 Java MQClientInstance#findBrokerAddressInSubscribe：按 brokerId 选地址。
     *
     * @param array<int, string> $brokerAddrs
     * @return array{0: ?string, 1: bool} [addr, isSlave]
     */
    public static function findBrokerAddrInSubscribe(array $brokerAddrs, int $brokerId, bool $onlyThisBroker = false): array
    {
        if ($brokerAddrs === []) {
            return [null, false];
        }
        $addr = $brokerAddrs[$brokerId] ?? null;
        if ($addr !== null) {
            return [$addr, $brokerId !== MixAll::MASTER_ID];
        }
        if ($brokerId !== MixAll::MASTER_ID) {
            $addr = $brokerAddrs[$brokerId + 1] ?? null;
            if ($addr !== null) {
                return [$addr, true];
            }
        }
        if (!$onlyThisBroker) {
            $firstId = min(array_keys($brokerAddrs));
            return [$brokerAddrs[$firstId], $firstId !== MixAll::MASTER_ID];
        }
        return [null, false];
    }

    // ---------------- POP 模式（5.x 轻量消费） ----------------

    /**
     * QUERY_ASSIGNMENT(400)：向 broker 要本组的队列指配（Java MQClientAPIImpl
     * #queryAssignment，body 是 QueryAssignmentRequestBody JSON）。
     *
     * 返回值语义与 Java 一致：SUCCESS 时返回指配列表（可为空表），**非 SUCCESS 抛
     * MQBrokerException**。是否把空表当"无效结果"由调用方（RebalanceImpl）判定。
     *
     * @return list<array{mq: MessageQueue, mode: MessageRequestMode}>
     */
    public function queryAssignment(
        string $topic,
        string $consumerGroup,
        string $clientId,
        string $strategyName,
        string $messageModel,
        int $timeoutMillis = 3000,
        ?string $addr = null,
    ): array {
        if ($addr === null) {
            // Java RebalanceImpl 走 mQClientFactory.queryAssignment → 找本 topic 任一
            // master；找不到直接抛 no route。
            $addr = $this->findBrokerAddrByTopic($topic)
                ?? throw new MQClientException(sprintf('No route info of this topic: %s', $topic));
        }
        $body = new QueryAssignmentRequestBody();
        $body->topic = $topic;
        $body->consumerGroup = $consumerGroup;
        $body->clientId = $clientId;
        $body->messageModel = $messageModel;
        $body->strategyName = $strategyName;
        $request = RemotingCommand::createRequestCommand(RequestCode::QUERY_ASSIGNMENT, null);
        $request->body = $body->encode();
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }
        $out = [];
        if ($response->body !== null && $response->body !== '') {
            foreach (QueryAssignmentResponseBody::decode($response->body)->messageQueueAssignments as $a) {
                if ($a->messageQueue === null) {
                    continue;
                }
                $out[] = ['mq' => $a->messageQueue, 'mode' => $a->mode];
            }
        }
        return $out;
    }

    /**
     * POP 弹取消息（RequestCode.POP_MESSAGE = 200050）。
     *
     * 与 pull 的语义差别：不需要提交位点、消费完成后用 ackMessage 确认；不 ack 的消息在
     * invisibleTime 之后被 broker 复活重投；queueId = -1 表示弹该 topic 的所有队列。
     */
    public function popMessage(
        string $consumerGroup,
        string $topic,
        int $queueId = -1,
        int $maxMsgNums = 32,
        int $invisibleTime = 60000,
        int $pollTime = 0,
        int $initMode = 0,
        ?string $exp = null,
        ?string $expType = null,
        bool $order = false,
        ?string $brokerName = null,
        int $timeoutMillis = 10000,
        ?string $addr = null,
    ): PopResult {
        if ($brokerName === null || $brokerName === '') {
            $route = $this->getTopicRouteData($topic);
            if ($route === null) {
                throw new MQClientException(sprintf('No route info of this topic: %s', $topic));
            }
            $brokers = $route->getBrokerDatas();
            if ($brokers === []) {
                throw new MQClientException(sprintf('No broker in route of topic: %s', $topic));
            }
            $brokerName = $brokers[0]->brokerName;
        }
        if ($addr === null) {
            // Java PullAPIWrapper#popAsync:369-373：只认主。
            $addr = $this->publishAddrFor($brokerName, $topic);
        }

        $header = new PopMessageRequestHeader();
        $header->consumerGroup = $consumerGroup;
        $header->topic = $topic;
        $header->queueId = $queueId;
        $header->maxMsgNums = $maxMsgNums;
        $header->invisibleTime = $invisibleTime;
        $header->pollTime = $pollTime;
        $header->bornTime = $this->currentTimeMillis();
        $header->initMode = $initMode;
        $header->expType = $expType;
        $header->exp = $exp;
        $header->order = $order;
        $request = RemotingCommand::createRequestCommand(RequestCode::POP_MESSAGE, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);

        // 响应码映射照 Java MQClientAPIImpl.processPopResponse。
        if ($response->code === ResponseCode::SUCCESS) {
            $status = PopStatus::FOUND;
        } elseif ($response->code === ResponseCode::POLLING_FULL) {
            $status = PopStatus::POLLING_FULL;
        } elseif ($response->code === ResponseCode::POLLING_TIMEOUT) {
            $status = PopStatus::POLLING_NOT_FOUND;
        } elseif ($response->code === ResponseCode::PULL_NOT_FOUND) {
            $status = PopStatus::POLLING_NOT_FOUND;
        } else {
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }

        $respHeader = new PopMessageResponseHeader();
        $respHeader->fromExtFields($response->extFields);

        /** @var list<MessageExt> $found */
        $found = [];
        if ($status === PopStatus::FOUND && $response->body !== null && $response->body !== '') {
            $found = MessageDecoder::decodeMessages($response->body);
            // 在改写 topic 之前反构 POP_CK（retryFlag 从消息原始 topic 推出来）。
            $this->stampPopCk($found, $brokerName, $respHeader);
        }
        // 统一盖 brokerName，并把 topic 还原成请求的 topic。
        foreach ($found as $m) {
            $m->brokerName = $brokerName;
            $m->topic = $topic;
        }

        return new PopResult(
            $status,
            $found,
            $respHeader->restNum ?? 0,
            $respHeader->popTime ?? 0,
            $respHeader->invisibleTime ?? 0,
            $respHeader->reviveQid ?? 0,
            $respHeader->startOffsetInfo,
            $respHeader->msgOffsetInfo,
            $respHeader->orderCountInfo,
        );
    }

    /** startOffsetInfo / msgOffsetInfo / sortMap 的查表 key（对应 Java getStartOffsetInfoMapKey 规则）。 */
    private static function popQueueMapKey(MessageExt $m): string
    {
        $ck = $m->getProperty(MessageConst::PROPERTY_POP_CK);
        if ($ck !== null && $ck !== '') {
            return ExtraInfoUtil::getRetry(ExtraInfoUtil::split($ck)) . '@' . $m->queueId;
        }
        return ExtraInfoUtil::getStartOffsetInfoMapKey($m->topic, $m->queueId);
    }

    /**
     * 给 POP 出来的消息反构 POP_CK 与 1ST_POP_TIME。逐条对齐 Java
     * MQClientAPIImpl.processPopResponse。
     *
     * @param list<MessageExt> $found
     */
    private function stampPopCk(array $found, string $brokerName, PopMessageResponseHeader $respHeader): void
    {
        $popTime = $respHeader->popTime ?? 0;
        $invisibleTime = $respHeader->invisibleTime ?? 0;
        $reviveQid = $respHeader->reviveQid ?? 0;

        if ($respHeader->startOffsetInfo === null || $respHeader->startOffsetInfo === '') {
            // Java startOffsetInfo == null 分支：用消息自身 queueOffset 当 ckQueueOffset 拼 7 段，
            // 再手工补一段凑成 8 段。
            $perQueue = [];
            foreach ($found as $m) {
                $key = (string) $m->topic . (string) $m->queueId;
                if (!array_key_exists($key, $perQueue)) {
                    $perQueue[$key] = ExtraInfoUtil::buildExtraInfo(
                        $m->queueOffset,
                        $popTime,
                        $invisibleTime,
                        $reviveQid,
                        $m->topic,
                        $brokerName,
                        $m->queueId,
                    );
                }
                $m->putProperty(
                    MessageConst::PROPERTY_POP_CK,
                    $perQueue[$key] . ExtraInfoUtil::KEY_SEPARATOR . (string) $m->queueOffset,
                );
            }
        } else {
            $startMap = ExtraInfoUtil::parseStartOffsetInfo($respHeader->startOffsetInfo) ?? [];
            $msgMap = ExtraInfoUtil::parseMsgOffsetInfo($respHeader->msgOffsetInfo) ?? [];

            $sortedOffsets = [];
            foreach ($found as $m) {
                $queueKey = self::popQueueMapKey($m);
                $sortedOffsets[$queueKey][] = $m->queueOffset;
            }
            foreach ($sortedOffsets as &$offsets) {
                sort($offsets);
            }
            unset($offsets);

            foreach ($found as $m) {
                // retry topic 弹回来的消息 broker 已经写好 POP_CK，不能覆盖。
                if ($m->getProperty(MessageConst::PROPERTY_POP_CK) !== null) {
                    continue;
                }
                $queueKey = self::popQueueMapKey($m);
                $startOffset = $startMap[$queueKey] ?? null;
                $offsets = $msgMap[$queueKey] ?? null;
                if ($startOffset === null || $offsets === null || $offsets === []) {
                    continue;
                }
                $index = array_search($m->queueOffset, $sortedOffsets[$queueKey], true);
                if ($index === false) {
                    continue;
                }
                if ($index >= count($offsets)) {
                    continue;
                }
                $m->putProperty(
                    MessageConst::PROPERTY_POP_CK,
                    ExtraInfoUtil::buildExtraInfo(
                        $startOffset,
                        $popTime,
                        $invisibleTime,
                        $reviveQid,
                        $m->topic,
                        $brokerName,
                        $m->queueId,
                        $offsets[$index],
                    ),
                );
            }
        }

        // Java computeIfAbsent：只在缺失时补。
        foreach ($found as $m) {
            if (!array_key_exists(MessageConst::PROPERTY_FIRST_POP_TIME, $m->properties)) {
                $m->putProperty(MessageConst::PROPERTY_FIRST_POP_TIME, (string) $popTime);
            }
        }
    }

    /** 按 topic（可选再按 brokerName）解析 broker 地址。 */
    private function addrFor(string $topic, ?string $brokerName = null): string
    {
        $route = $this->getTopicRouteData($topic);
        if ($route === null) {
            throw new MQClientException(sprintf('No route info of this topic: %s', $topic));
        }
        if ($brokerName !== null && $brokerName !== '') {
            $addr = self::findBrokerAddrInRoute($route, $brokerName);
            if ($addr === null) {
                throw new MQClientException(
                    sprintf('Broker %s not found in route of topic %s', $brokerName, $topic),
                );
            }
            return $addr;
        }
        $brokers = $route->getBrokerDatas();
        if ($brokers === []) {
            throw new MQClientException(sprintf('No broker in route of topic: %s', $topic));
        }
        $addr = $brokers[0]->selectBrokerAddr();
        if ($addr === null) {
            throw new MQClientException(sprintf('No available broker addr for topic: %s', $topic));
        }
        return $addr;
    }

    /**
     * 确认一条 POP 消息（RequestCode.ACK_MESSAGE = 200051）。返回 broker 响应码。
     */
    public function ackMessage(
        string $consumerGroup,
        string $topic,
        int $queueId,
        string $extraInfo,
        int $offset,
        ?string $brokerName = null,
        int $timeoutMillis = 3000,
        ?string $addr = null,
    ): int {
        if ($brokerName === null) {
            $brokerName = ExtraInfoUtil::getBrokerName(ExtraInfoUtil::split($extraInfo));
        }
        if ($addr === null) {
            $addr = $this->addrFor($topic, $brokerName);
        }
        $header = new AckMessageRequestHeader();
        $header->consumerGroup = $consumerGroup;
        $header->topic = $topic;
        $header->queueId = $queueId;
        $header->extraInfo = $extraInfo;
        $header->offset = $offset;
        $request = RemotingCommand::createRequestCommand(RequestCode::ACK_MESSAGE, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        return $response->code;
    }

    /**
     * 延长 POP 消息的不可见时间（CHANGE_MESSAGE_INVISIBLETIME = 200053）。
     */
    public function changeInvisibleTime(
        string $consumerGroup,
        string $topic,
        int $queueId,
        string $extraInfo,
        int $offset,
        int $invisibleTime,
        ?string $brokerName = null,
        int $timeoutMillis = 3000,
        ?string $addr = null,
    ): ChangeInvisibleTimeResult {
        if ($brokerName === null) {
            $brokerName = ExtraInfoUtil::getBrokerName(ExtraInfoUtil::split($extraInfo));
        }
        if ($addr === null) {
            $addr = $this->addrFor($topic, $brokerName);
        }
        $header = new ChangeInvisibleTimeRequestHeader();
        $header->consumerGroup = $consumerGroup;
        $header->topic = $topic;
        $header->queueId = $queueId;
        $header->extraInfo = $extraInfo;
        $header->offset = $offset;
        $header->invisibleTime = $invisibleTime;
        $request = RemotingCommand::createRequestCommand(RequestCode::CHANGE_MESSAGE_INVISIBLETIME, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);

        $respHeader = new ChangeInvisibleTimeResponseHeader();
        $respHeader->fromExtFields($response->extFields);

        $newExtra = null;
        if ($response->code === ResponseCode::SUCCESS) {
            $newExtra = ExtraInfoUtil::buildExtraInfo(
                $offset,
                $respHeader->popTime ?? 0,
                $respHeader->invisibleTime ?? 0,
                $respHeader->reviveQid ?? 0,
                $topic,
                $brokerName,
                $queueId,
                $offset,
            );
        }
        return new ChangeInvisibleTimeResult(
            $response->code,
            $respHeader->popTime ?? 0,
            $respHeader->invisibleTime ?? 0,
            $respHeader->reviveQid ?? 0,
            $newExtra,
        );
    }

    // ---------------- Offset 查询/更新 ----------------

    /**
     * setZeroIfNotFound 默认 false：Java 的 fetchConsumeOffsetFromBroker 从不设置该字段，
     * 新消费组因此回 QUERY_NOT_FOUND（null）而非 0。
     */
    public function queryConsumerOffset(
        string $consumerGroup,
        MessageQueue $mq,
        int $timeoutMillis = 5000,
        ?string $addr = null,
        bool $setZeroIfNotFound = false,
    ): ?int {
        if ($addr === null) {
            $addr = $this->consumerOffsetAddr($mq);
        }
        $header = new QueryConsumerOffsetRequestHeader();
        $header->consumerGroup = $consumerGroup;
        $header->topic = $mq->topic;
        $header->queueId = $mq->queueId;
        $header->setZeroIfNotFound = $setZeroIfNotFound;
        $request = RemotingCommand::createRequestCommand(RequestCode::QUERY_CONSUMER_OFFSET, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        if ($response->code === ResponseCode::QUERY_NOT_FOUND) {
            return null;
        }
        $this->checkResponse($response);
        $respHeader = new QueryConsumerOffsetResponseHeader();
        $respHeader->fromExtFields($response->extFields);
        return $respHeader->offset;
    }

    public function updateConsumerOffset(
        string $consumerGroup,
        MessageQueue $mq,
        int $commitOffset,
        int $timeoutMillis = 5000,
        ?string $addr = null,
    ): void {
        if ($addr === null) {
            $addr = $this->brokerAddr($mq);
        }
        $header = new UpdateConsumerOffsetRequestHeader();
        $header->consumerGroup = $consumerGroup;
        $header->topic = $mq->topic;
        $header->queueId = $mq->queueId;
        $header->commitOffset = $commitOffset;
        $request = RemotingCommand::createRequestCommand(RequestCode::UPDATE_CONSUMER_OFFSET, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        $this->checkResponse($response);
    }

    public function getMaxOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        if ($addr === null) {
            $addr = $this->publishAddrInAdmin($mq);
        }
        $header = new GetMaxOffsetRequestHeader();
        $header->topic = $mq->topic;
        $header->queueId = $mq->queueId;
        $request = RemotingCommand::createRequestCommand(RequestCode::GET_MAX_OFFSET, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        $this->checkResponse($response);
        $respHeader = new GetMaxOffsetResponseHeader();
        $respHeader->fromExtFields($response->extFields);
        return $respHeader->offset ?? 0;
    }

    public function getMinOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        if ($addr === null) {
            $addr = $this->publishAddrInAdmin($mq);
        }
        $header = new GetMinOffsetRequestHeader();
        $header->topic = $mq->topic;
        $header->queueId = $mq->queueId;
        $request = RemotingCommand::createRequestCommand(RequestCode::GET_MIN_OFFSET, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        $this->checkResponse($response);
        $respHeader = new GetMinOffsetResponseHeader();
        $respHeader->fromExtFields($response->extFields);
        return $respHeader->offset ?? 0;
    }

    /**
     * boundaryType 默认 LOWER（Java 同签名重载直接传 BoundaryType.LOWER）；
     * 传 null 就不写 boundaryType 字段。
     */
    public function searchOffsetByTimestamp(
        MessageQueue $mq,
        int $timestamp,
        int $timeoutMillis = 5000,
        ?string $addr = null,
        ?BoundaryType $boundaryType = BoundaryType::LOWER,
    ): int {
        if ($addr === null) {
            $addr = $this->publishAddrInAdmin($mq);
        }
        $header = new SearchOffsetRequestHeader();
        $header->topic = $mq->topic;
        $header->queueId = $mq->queueId;
        $header->timestamp = $timestamp;
        $header->boundaryType = $boundaryType;
        $request = RemotingCommand::createRequestCommand(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        $this->checkResponse($response);
        $respHeader = new SearchOffsetResponseHeader();
        $respHeader->fromExtFields($response->extFields);
        return $respHeader->offset ?? 0;
    }

    /**
     * 按 key 查消息（对应 Java MQClientAPIImpl.queryMessage）。
     *
     * uniqKey=true 时额外下发 extFields["_UNIQUE_KEY_QUERY"]="true"。
     */
    public function queryMessage(
        string $topic,
        string $key,
        int $maxNum,
        int $beginTimestamp,
        int $endTimestamp,
        int $timeoutMillis = 15000,
        ?string $addr = null,
        ?string $indexType = null,
        bool $uniqKey = false,
    ): ?string {
        if ($addr === null) {
            $addr = $this->brokerAddrForTopic($topic);
        }
        $header = new QueryMessageRequestHeader();
        $header->topic = $topic;
        $header->key = $key;
        $header->maxNum = $maxNum;
        $header->beginTimestamp = $beginTimestamp;
        $header->endTimestamp = $endTimestamp;
        $header->indexType = $indexType;
        $request = RemotingCommand::createRequestCommand(RequestCode::QUERY_MESSAGE, $header);
        if ($uniqKey) {
            $request->extFields[MixAll::UNIQUE_MSG_QUERY_FLAG] = 'true';
        }
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        if ($response->code === ResponseCode::QUERY_NOT_FOUND) {
            return null;
        }
        $this->checkResponse($response);
        return $response->body;
    }

    /**
     * 对应 Java MQAdminImpl.queryMessage：查该 topic 所有 broker 并合并去重后的消息。
     *
     * @return list<MessageExt>
     */
    public function queryMessageAllBrokers(
        string $topic,
        string $key,
        int $maxNum,
        int $beginTimestamp,
        int $endTimestamp,
        ?string $indexType = null,
        bool $uniqKey = false,
        int $timeoutMillis = 15000,
    ): array {
        $messages = [];
        $route = $this->getTopicRouteData($topic);
        if ($route === null) {
            return $messages;
        }
        foreach ($route->getBrokerDatas() as $brokerData) {
            $addr = $brokerData->selectBrokerAddr();
            if ($addr === null || $addr === '') {
                continue;
            }
            try {
                $body = $this->queryMessage(
                    $topic,
                    $key,
                    $maxNum,
                    $beginTimestamp,
                    $endTimestamp,
                    $timeoutMillis,
                    $addr,
                    $indexType,
                    $uniqKey,
                );
            } catch (\Throwable) {
                continue;
            }
            if ($body === null || $body === '') {
                continue;
            }
            foreach (MessageDecoder::decodeMessages($body) as $m) {
                $m->brokerName = $brokerData->brokerName;
                if ($uniqKey) {
                    if ($m->msgId === $key) {
                        $messages[] = $m;
                    }
                } else {
                    $keys = $m->getKeys();
                    if ($keys !== null && $keys !== '') {
                        foreach (explode(MessageConst::KEY_SEPARATOR, $keys) as $k) {
                            if ($k === $key && $m->topic === $topic) {
                                $messages[] = $m;
                                break;
                            }
                        }
                    }
                }
            }
        }
        usort($messages, static fn(MessageExt $a, MessageExt $b): int => ($a->queueOffset ?? 0) <=> ($b->queueOffset ?? 0));
        return $maxNum > 0 ? array_slice($messages, 0, $maxNum) : $messages;
    }

    private function brokerAddrForTopic(string $topic): string
    {
        $route = $this->getTopicRouteData($topic);
        if ($route === null) {
            throw new MQClientException(sprintf('No route info of this topic: %s', $topic));
        }
        $brokers = $route->getBrokerDatas();
        if ($brokers === []) {
            throw new MQClientException(sprintf('No broker in route of topic: %s', $topic));
        }
        $addr = $brokers[0]->selectBrokerAddr();
        if ($addr === null) {
            throw new MQClientException(sprintf('No available broker addr for topic: %s', $topic));
        }
        return $addr;
    }

    // ---------------- 队列锁（顺序消费） ----------------

    /**
     * 批量锁队列（对应 Java MQClientAPIImpl.lockBatchMQ，RequestCode.LOCK_BATCH_MQ）。
     *
     * @param list<MessageQueue> $mqs
     * @return list<MessageQueue>
     */
    public function lockBatchMq(string $consumerGroup, string $clientId, array $mqs, int $timeoutMillis = 1000): array
    {
        $lockOk = [];
        $byBroker = [];
        foreach ($mqs as $mq) {
            $byBroker[$mq->brokerName][] = $mq;
        }
        foreach ($byBroker as $brokerName => $brokerMqs) {
            // Java RebalanceImpl#lock 走 findBrokerAddressInSubscribe(brokerName, MASTER_ID, true)
            // —— 只认主、不刷路由，拿不到就整台跳过。
            $addr = $this->findBrokerAddressInPublish($brokerName);
            if ($addr === null) {
                continue;
            }
            $body = new LockBatchRequestBody();
            $body->consumerGroup = $consumerGroup;
            $body->clientId = $clientId;
            $body->mqSet = array_map(
                static fn(MessageQueue $m): array => [
                    'topic' => $m->topic,
                    'brokerName' => $m->brokerName,
                    'queueId' => $m->queueId,
                ],
                $brokerMqs,
            );
            $request = RemotingCommand::createRequestCommand(RequestCode::LOCK_BATCH_MQ, new LockBatchMqRequestHeader());
            $request->body = $body->encode();
            try {
                $response = $this->invokeSync($addr, $request, $timeoutMillis);
                $this->checkResponse($response);
                $rb = LockBatchResponseBody::decode($response->body ?? '');
                foreach ($rb->lockOkMqSet as $d) {
                    $lockOk[] = new MessageQueue(
                        (string) ($d['topic'] ?? ''),
                        (string) ($d['brokerName'] ?? ''),
                        (int) ($d['queueId'] ?? 0),
                    );
                }
            } catch (\Throwable $e) {
                Logger::warning(sprintf('lock_batch_mq failed for broker %s: %s', $brokerName, $e->getMessage()));
            }
        }
        return $lockOk;
    }

    /**
     * 批量解锁队列（对应 Java MQClientAPIImpl.unlockBatchMQ，RequestCode.UNLOCK_BATCH_MQ）。
     *
     * @param list<MessageQueue> $mqs
     */
    public function unlockBatchMq(string $consumerGroup, string $clientId, array $mqs, int $timeoutMillis = 1000): void
    {
        $byBroker = [];
        foreach ($mqs as $mq) {
            $byBroker[$mq->brokerName][] = $mq;
        }
        foreach ($byBroker as $brokerName => $brokerMqs) {
            $addr = $this->findBrokerAddressInPublish($brokerName);
            if ($addr === null) {
                continue;
            }
            $body = new UnlockBatchRequestBody();
            $body->consumerGroup = $consumerGroup;
            $body->clientId = $clientId;
            $body->mqSet = array_map(
                static fn(MessageQueue $m): array => [
                    'topic' => $m->topic,
                    'brokerName' => $m->brokerName,
                    'queueId' => $m->queueId,
                ],
                $brokerMqs,
            );
            $request = RemotingCommand::createRequestCommand(RequestCode::UNLOCK_BATCH_MQ, new UnlockBatchMqRequestHeader());
            $request->body = $body->encode();
            try {
                $response = $this->invokeSync($addr, $request, $timeoutMillis);
                $this->checkResponse($response);
            } catch (\Throwable $e) {
                Logger::warning(sprintf('unlock_batch_mq failed for broker %s: %s', $brokerName, $e->getMessage()));
            }
        }
    }

    // ---------------- 心跳 / 注销 ----------------

    public function sendHeartbeat(string $addr, HeartbeatData $heartbeatData, int $timeoutMillis = 5000): void
    {
        $request = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT, null);
        $request->body = $heartbeatData->encode();
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        $this->checkResponse($response);
    }

    /** UNREGISTER_CLIENT(35)：把本 clientId 从这台 broker 摘掉。 */
    public function unregisterClient(
        string $addr,
        string $clientId,
        ?string $producerGroup,
        ?string $consumerGroup,
        int $timeoutMillis = 3000,
    ): void {
        $header = new UnregisterClientRequestHeader();
        $header->clientId = $clientId;
        // 空串会被 broker 当成「有这个组」并白做一轮 unregister，故空白按「没这个组」处理。
        $pg = trim((string) $producerGroup);
        $header->producerGroup = $pg !== '' ? $pg : null;
        $cg = trim((string) $consumerGroup);
        $header->consumerGroup = $cg !== '' ? $cg : null;
        $request = RemotingCommand::createRequestCommand(RequestCode::UNREGISTER_CLIENT, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        $this->checkResponse($response);
    }

    /**
     * CHECK_CLIENT_CONFIG(46)：让 broker 校验一份订阅表达式。
     */
    public function checkClientConfig(
        string $brokerAddr,
        string $consumerGroup,
        string $clientId,
        SubscriptionData $subscriptionData,
        int $timeoutMillis = 3000,
    ): void {
        $request = RemotingCommand::createRequestCommand(RequestCode::CHECK_CLIENT_CONFIG, null);
        $body = new CheckClientRequestBody();
        $body->clientId = $clientId;
        $body->group = $consumerGroup;
        $body->subscriptionData = $subscriptionData;
        $request->body = $body->encode();
        $response = $this->invokeSync($brokerAddr, $request, $timeoutMillis);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQClientException($response->remark ?? '', $response->code);
        }
    }

    // ---------------- 管理类 API ----------------

    public function getBrokerClusterInfo(int $timeoutMillis = 10000): ClusterInfo
    {
        $request = RemotingCommand::createRequestCommand(RequestCode::GET_BROKER_CLUSTER_INFO, null);
        foreach ($this->nameServerAddrs as $nsAddr) {
            try {
                $response = $this->invokeSync($nsAddr, $request, $timeoutMillis);
                if ($response->code === ResponseCode::SUCCESS && $response->body !== null && $response->body !== '') {
                    return ClusterInfo::decode($response->body);
                }
            } catch (\Throwable) {
                continue;
            }
        }
        throw new MQClientException('Failed to get broker cluster info from name server');
    }

    public function getAllTopicListFromNameServer(int $timeoutMillis = 10000): TopicList
    {
        $request = RemotingCommand::createRequestCommand(RequestCode::GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null);
        foreach ($this->nameServerAddrs as $nsAddr) {
            try {
                $response = $this->invokeSync($nsAddr, $request, $timeoutMillis);
                if ($response->code === ResponseCode::SUCCESS && $response->body !== null && $response->body !== '') {
                    return TopicList::decode($response->body);
                }
            } catch (\Throwable) {
                continue;
            }
        }
        throw new MQClientException('Failed to get all topic list from name server');
    }

    public function createTopicInBroker(
        string $brokerAddr,
        string $defaultTopic,
        string $topic,
        int $readQueueNums = 4,
        int $writeQueueNums = 4,
        int $perm = 6,
        int $topicSysFlag = 0,
        string $topicFilterType = TopicFilterType::SINGLE_TAG,
        bool $order = false,
        ?string $attributes = null,
        int $timeoutMillis = 5000,
        int $retryTimes = 5,
    ): void {
        $header = new CreateTopicRequestHeader();
        $header->topic = $topic;
        $header->defaultTopic = $defaultTopic;
        $header->readQueueNums = $readQueueNums;
        $header->writeQueueNums = $writeQueueNums;
        $header->perm = $perm;
        $header->topicFilterType = $topicFilterType;
        $header->topicSysFlag = $topicSysFlag;
        $header->order = $order;
        // Java: AttributeParser.parseToString(map) —— 空 map 输出 ""，不是 null
        $header->attributes = $attributes ?? '';
        $header->force = false;
        $request = RemotingCommand::createRequestCommand(RequestCode::UPDATE_AND_CREATE_TOPIC, $header);

        $attempts = max(1, $retryTimes);
        $lastExc = null;
        for ($attempt = 0; $attempt < $attempts; $attempt++) {
            try {
                $response = $this->invokeSync($brokerAddr, $request, $timeoutMillis);
                $this->checkResponse($response);
                return;
            } catch (MQBrokerException $e) {
                throw $e;
            } catch (\Throwable $e) {
                $lastExc = $e;
                if ($attempt === $attempts - 1) {
                    throw $e;
                }
            }
        }
        if ($lastExc !== null) {
            throw $lastExc;
        }
    }

    public function createTopicInRoute(
        string $topic,
        int $readQueueNums = 4,
        int $writeQueueNums = 4,
        int $perm = 6,
        int $topicSysFlag = 0,
        ?string $attributes = null,
        int $timeoutMillis = 5000,
    ): void {
        $route = $this->getTopicRouteData(MixAll::DEFAULT_TOPIC);
        if ($route === null) {
            throw new MQClientException(sprintf('No route info of default topic %s', MixAll::DEFAULT_TOPIC));
        }
        $createdAtLeastOnce = false;
        $lastExc = null;
        foreach ($route->getBrokerDatas() as $brokerData) {
            $addr = $brokerData->selectBrokerAddr();
            if ($addr === null || $addr === '') {
                continue;
            }
            try {
                $this->createTopicInBroker(
                    $addr,
                    MixAll::DEFAULT_TOPIC,
                    $topic,
                    $readQueueNums,
                    $writeQueueNums,
                    $perm,
                    $topicSysFlag,
                    attributes: $attributes,
                    timeoutMillis: $timeoutMillis,
                );
                $createdAtLeastOnce = true;
            } catch (\Throwable $e) {
                $lastExc = $e;
            }
        }
        if (!$createdAtLeastOnce && $lastExc !== null) {
            throw new MQClientException('create new topic failed', null, $lastExc);
        }
    }

    public function deleteTopicInBroker(string $brokerAddr, string $topic, int $timeoutMillis = 5000): void
    {
        $request = RemotingCommand::createRequestCommand(RequestCode::DELETE_TOPIC_IN_BROKER, null);
        $request->extFields['topic'] = $topic;
        $response = $this->invokeSync($brokerAddr, $request, $timeoutMillis);
        $this->checkResponse($response);
    }

    public function deleteTopicInNamesrv(string $topic, int $timeoutMillis = 5000): void
    {
        $request = RemotingCommand::createRequestCommand(RequestCode::DELETE_TOPIC_IN_NAMESRV, null);
        $request->extFields['topic'] = $topic;
        foreach ($this->nameServerAddrs as $nsAddr) {
            try {
                $response = $this->invokeSync($nsAddr, $request, $timeoutMillis);
                $this->checkResponse($response);
                return;
            } catch (\Throwable) {
                continue;
            }
        }
        throw new MQClientException(sprintf('Failed to delete topic %s in name server', $topic));
    }

    public function getConsumerListByGroup(
        string $consumerGroup,
        int $timeoutMillis = 5000,
        ?string $addr = null,
    ): GetConsumerListByGroupResponseBody {
        if ($addr === null) {
            throw new MQClientException('broker addr required for get consumer list');
        }
        $header = new GetConsumerListByGroupRequestHeader();
        $header->consumerGroup = $consumerGroup;
        $request = RemotingCommand::createRequestCommand(RequestCode::GET_CONSUMER_LIST_BY_GROUP, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        $this->checkResponse($response);
        if ($response->body !== null && $response->body !== '') {
            return GetConsumerListByGroupResponseBody::decode($response->body);
        }
        return new GetConsumerListByGroupResponseBody();
    }

    // ---------------- 工具 ----------------

    /**
     * 位点读取的地址口径：只认主 → 刷一次路由 → 重查时放宽（可落到从节点，位点是 HA 同一份）。
     */
    private function consumerOffsetAddr(MessageQueue $mq): string
    {
        $addr = $this->findBrokerAddressInPublish($mq->brokerName);
        if ($addr === null || $addr === '') {
            $this->updateTopicRouteInfoFromNameServer($mq->topic);
            $addr = $this->brokerAddrOf($mq->brokerName);
        }
        if ($addr === null || $addr === '') {
            throw new MQClientException(sprintf('The broker[%s] not exist', $mq->brokerName));
        }
        return $addr;
    }

    /** Java findBrokerAddressInAdmin 口径：主优先、没主退一台从节点。 */
    private function brokerAddr(MessageQueue $mq): string
    {
        $route = $this->getTopicRouteData($mq->topic);
        if ($route === null) {
            throw new MQClientException(sprintf('No route info of this topic: %s', $mq->topic));
        }
        $addr = self::findBrokerAddrInRoute($route, $mq->brokerName);
        if ($addr === null) {
            throw new MQClientException(
                sprintf('Broker %s not found in route of topic %s', $mq->brokerName, $mq->topic),
            );
        }
        return $addr;
    }

    /** Java MQAdminImpl 的 offset 查询口径：只打主，主没了就报错。 */
    private function publishAddrInAdmin(MessageQueue $mq): string
    {
        $addr = $this->findBrokerAddressInPublish($mq->brokerName);
        if ($addr === null || $addr === '') {
            $this->updateTopicRouteInfoFromNameServer($mq->topic);
            $addr = $this->findBrokerAddressInPublish($mq->brokerName);
        }
        if ($addr === null || $addr === '') {
            throw new MQClientException(sprintf('The broker[%s] not exist', $mq->brokerName));
        }
        return $addr;
    }

    /**
     * ``publishAddrInAdmin`` 的公开出口：供拉模式消费者的 earliestMsgStoreTime 复用
     * （Python 端 consumer.py 直接复用了实例的私有 ``_publish_addr_in_admin``，
     * PHP 私有不可见，纯增量补这个公开壳子，逻辑不走样）。
     */
    public function adminAddrFor(MessageQueue $mq): string
    {
        return $this->publishAddrInAdmin($mq);
    }

    /** 从 topic_route_table 里找该 brokerName 的地址（主优先、没主退任意一台）。 */
    public function brokerAddrOf(string $brokerName): ?string
    {
        foreach ($this->topicRouteTable as $route) {
            $addr = self::findBrokerAddrInRoute($route, $brokerName);
            if ($addr !== null && $addr !== '') {
                return $addr;
            }
        }
        return null;
    }

    /**
     * 对应 Java MQClientInstance#findBrokerAddressInPublish:1295-1305：**只**从
     * brokerAddrTable 取 brokerId=0 的地址，没有返回 null。
     */
    public function findBrokerAddressInPublish(string $brokerName): ?string
    {
        $addrs = $this->brokerAddrTable[$brokerName] ?? null;
        if ($addrs === null || $addrs === []) {
            return null;
        }
        return $addrs[MixAll::MASTER_ID] ?? null;
    }

    /** @return list<string> */
    public function getRouteOfAllBrokers(): array
    {
        $addrs = [];
        foreach ($this->topicRouteTable as $route) {
            foreach ($route->getBrokerDatas() as $brokerData) {
                $a = $brokerData->selectBrokerAddr();
                if ($a !== null && $a !== '' && !in_array($a, $addrs, true)) {
                    $addrs[] = $a;
                }
            }
        }
        return $addrs;
    }

    /**
     * 路由里出现过的每一台 broker 地址（主 + 从），不去重到「一个 brokerName 一台」。
     *
     * @return list<string>
     */
    public function getAllBrokerAddrs(): array
    {
        $addrs = [];
        foreach ($this->topicRouteTable as $route) {
            foreach ($route->getBrokerDatas() as $brokerData) {
                foreach ($brokerData->brokerAddrs as $a) {
                    if ($a !== null && $a !== '' && !in_array($a, $addrs, true)) {
                        $addrs[] = $a;
                    }
                }
            }
        }
        return $addrs;
    }

    /**
     * 查询消费组内所有 clientId（对应 Java MQClientInstance.findConsumerIdList）。
     *
     * @return list<string>|null
     */
    public function getConsumerIdListByGroup(string $topic, string $consumerGroup, int $timeoutMillis = 5000): ?array
    {
        try {
            $addr = $this->brokerAddrForTopic($topic);
        } catch (MQClientException $e) {
            Logger::debug(sprintf('get_consumer_id_list_by_group: no broker for topic %s: %s', $topic, $e->getMessage()));
            return null;
        }
        $header = new GetConsumerListByGroupRequestHeader();
        $header->consumerGroup = $consumerGroup;
        $request = RemotingCommand::createRequestCommand(RequestCode::GET_CONSUMER_LIST_BY_GROUP, $header);
        try {
            $response = $this->invokeSync($addr, $request, $timeoutMillis);
        } catch (\Throwable $e) {
            Logger::debug(sprintf(
                'get_consumer_id_list_by_group failed, %s %s: %s',
                $addr,
                $consumerGroup,
                $e->getMessage(),
            ));
            return null;
        }
        if ($response->code !== ResponseCode::SUCCESS || $response->body === null) {
            return null;
        }
        try {
            $body = GetConsumerListByGroupResponseBody::decode($response->body);
        } catch (\Throwable $e) {
            Logger::debug('get_consumer_id_list_by_group decode failed: ' . $e->getMessage());
            return null;
        }
        return array_values($body->consumerIdList);
    }

    /** 向所有已知 broker（主 + 从）注销本 clientId（对应 Java MQClientInstance.unregisterClient）。 */
    public function unregisterClientAllBrokers(
        string $clientId,
        ?string $producerGroup,
        ?string $consumerGroup,
        int $timeoutMillis = 3000,
    ): void {
        foreach ($this->getAllBrokerAddrs() as $addr) {
            try {
                $this->unregisterClient($addr, $clientId, $producerGroup, $consumerGroup, $timeoutMillis);
            } catch (\Throwable $e) {
                Logger::debug(sprintf('unregister_client failed, addr=%s: %s', $addr, $e->getMessage()));
            }
        }
    }

    // ---------------- 在用 topic 登记 ----------------

    /** 登记需要在后台周期刷新路由的 topic（对应 Java 的订阅/发布 topic 列表）。 */
    public function registerTopicInUse(string $topic): void
    {
        if ($topic !== '') {
            $this->topicsInUse[$topic] = true;
        }
    }

    /** @return list<string> */
    public function getTopicsInUse(): array
    {
        return array_keys($this->topicsInUse);
    }
}
