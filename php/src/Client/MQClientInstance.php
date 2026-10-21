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
use RocketMQ\Remoting\Protocol\BatchAckMessageRequestBody;
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
use RocketMQ\Remoting\Protocol\HeartbeatV2Result;
use RocketMQ\Remoting\Protocol\LockBatchMqRequestHeader;
use RocketMQ\Remoting\Protocol\LockBatchRequestBody;
use RocketMQ\Remoting\Protocol\LockBatchResponseBody;
use RocketMQ\Remoting\Protocol\MessageRequestMode;
use RocketMQ\Remoting\Protocol\NotifyConsumerIdsChangedRequestHeader;
use RocketMQ\Remoting\Protocol\PopMessageRequestHeader;
use RocketMQ\Remoting\Protocol\PopMessageResponseHeader;
use RocketMQ\Remoting\Protocol\PullMessageRequestHeader;
use RocketMQ\Remoting\Protocol\PullMessageResponseHeader;
use RocketMQ\Remoting\Protocol\ProducerData;
use RocketMQ\Remoting\Protocol\QueryAssignmentRequestBody;
use RocketMQ\Remoting\Protocol\QueryAssignmentResponseBody;
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
use RocketMQ\Remoting\SocksProxyConfig;
use RocketMQ\Remoting\StreamTypeRPCHook;

/**
 * MQClientInstance: the RocketMQ client's core orchestration, concentrating the protocol
 * call surface with the name server / broker.
 *
 * Responsibilities: NameServer address management, Topic route fetching and caching, Broker
 * address resolution, message sending (SEND_MESSAGE_V2 / SEND_BATCH_MESSAGE /
 * SEND_REPLY_MESSAGE_V2), pulling (PULL_MESSAGE), POP (POP_MESSAGE / ACK_MESSAGE /
 * CHANGE_MESSAGE_INVISIBLETIME), offset query/update, heartbeat, admin APIs
 * (create/delete Topic, cluster info, etc.).
 *
 * This file also hosts both the TopicPublishInfo and MQClientInstance classes.
 *
 * Single-threaded model: this port has no threads; the periodic tasks -- dynamic NameServer
 * refresh, thread-pool elasticity patrol, route refresh, consume stats sampling, and the
 * registration heartbeat -- are all converged into caller-driven explicit ``tick()`` (expiry
 * is judged internally by timestamp, with the current time injectable); the resetOffset that
 * used to be "dropped onto a background thread" now enters an internal pending queue,
 * executed by ``tick()``.
 */

/**
 * The publish-side view of a topic: the route queue list + a round-robin cursor.
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
     * Round-robin queue selection.
     *
     * ``$filters`` are callables ``f(mq): bool``; all must pass for selection. With no
     * filters a round-robin queue is always returned (never null); with filters and no
     * match in a full round, null is returned and the caller degrades the selection.
     *
     * @param callable(MessageQueue): bool ...$filters
     */
    public function selectOneMessageQueue(callable ...$filters): ?MessageQueue
    {
        // PHP single-threaded sequential execution: the cursor increment and read cannot be
        // preempted, so the round-robin sequence stays contiguous.
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
 * Client instance: reused by clientId, hosting the route cache, broker address table,
 * periodic tasks and the protocol call surface.
 */
class MQClientInstance
{
    /** Consume stats sampling period, 10s. */
    public const STATS_SAMPLE_INTERVAL_MILLIS = 10_000;

    /** Dynamic NameServer refresh: first delay 10s. */
    public const NAMESRV_REFRESH_INITIAL_DELAY_MILLIS = 10_000;
    /** Dynamic NameServer refresh: period 2 minutes. */
    public const NAMESRV_REFRESH_INTERVAL_MILLIS = 120_000;

    /** Thread elasticity patrol: first delay 1 minute. */
    public const ADJUST_POOL_INITIAL_DELAY_MILLIS = 60_000;
    /** Thread elasticity patrol: period 1 minute. */
    public const ADJUST_POOL_INTERVAL_MILLIS = 60_000;

    /** Route refresh: first delay 10ms. */
    public const ROUTE_REFRESH_INITIAL_DELAY_MILLIS = 10;

    /** Registration refresh (heartbeat) period: 30000ms. */
    public const HEARTBEAT_INTERVAL_MILLIS = 30_000;

    /** Default timeout of the internal client-to-broker/name-server API calls, 3000ms. */
    public const DEFAULT_MQ_CLIENT_API_TIMEOUT_MILLIS = 3000;

    public const QUERY_ASSIGNMENT_TIMEOUT_MILLIS = 3000;

    public const POP_ASSIGNMENT_INTERVAL_MILLIS = 20_000;

    /** Process-level instance table: clientId -> instance. */
    public static array $instanceMap = [];

    public string $clientId;

    /** @var list<string> */
    public array $nameServerAddrs;

    /**
     * Route refresh period for in-use topics, default 30000ms.
     * Read only once at start(); changing this field at runtime does not change the refresh cadence.
     */
    public int $pollNameServerInterval;

    /**
     * Registration refresh (heartbeat) period, default HEARTBEAT_INTERVAL_MILLIS.
     * Same reading rule as pollNameServerInterval: taken once at start(), so a facade's
     * heartbeatIntervalMillis decides the cadence of the beats this instance pumps.
     */
    public int $heartbeatIntervalMillis;

    /**
     * VIP channel switch (default false): when true, the broker port is shifted to the dedicated
     * fast port (port - 2) for **sends** and for the offset / lock / offset-search / message-query
     * / consumer-list / client-config / broker-admin family. This port applies it in one place,
     * ``vipAddrFor()``, rather than recomputing at every call site (Admin has its own independent
     * switch of the same name). Heartbeats, unregistration, pulls, pops, acks, invisible-time
     * changes, recalls and every name-server request are never shifted.
     */
    public bool $vipChannelEnabled = false;

    /**
     * Heartbeat fingerprint path (default false). When on, each round hashes its subscription set
     * into ``heartbeatFingerprint`` and, once a broker has answered IS_SUPPORT_HEART_BEAT_V2 and
     * still holds that fingerprint, sends it the subscription-free body instead.
     */
    public bool $useHeartbeatV2 = false;

    /**
     * Decode-read-body switch (default true). When off, a PULL / LITE_PULL / POP response is still
     * walked frame by frame (the decoder cursor advances normally), but each message's body bytes
     * are skipped without being copied, CRC-checked or decompressed: the returned messages are
     * header-only, with their queue offsets and properties intact.
     */
    public bool $decodeReadBody = true;

    /**
     * Timeout (milliseconds) of the internal API calls this instance issues on its own behalf:
     * heartbeat V1/V2, CHECK_CLIENT_CONFIG, route fetches from the name server, UNREGISTER_CLIENT
     * and GET_CONSUMER_LIST_BY_GROUP. It is the fallback only -- an explicit per-call timeout
     * always wins.
     */
    public int $mqClientApiTimeout = self::DEFAULT_MQ_CLIENT_API_TIMEOUT_MILLIS;

    /**
     * Concurrent heartbeat dispatch (default false). On (and with the fingerprint path off), one
     * round fans one HEART_BEAT per broker out over the pool created by start() and waits for
     * every task. The fingerprint path wins when both switches are on.
     */
    public bool $enableConcurrentHeartbeat = false;

    /** Worker count of the concurrent heartbeat pool, default = available processors. */
    public int $concurrentHeartbeatThreadPoolSize;

    private ?ConsumeExecutor $heartbeatExecutor = null;

    /** @var array<string, true> broker addresses that answered IS_SUPPORT_HEART_BEAT_V2 at least once */
    private array $supportV2BrokerAddrs = [];

    /** @var array<string, int> broker address => the fingerprint that round left there */
    private array $brokerAddrHeartbeatFingerprint = [];

    /**
     * 5.x new namespace: when non-empty, **every** request carries the two extension
     * headers `nsd=true` / `ns=<value>` (written by {@see NamespaceRpcHook}), by which the
     * broker resolves the request to the corresponding serverless instance. It is a
     * separate mechanism from `namespace`, which prefixes topic names.
     *
     * WARNING: the hook is installed at construction and the value is read fresh per
     * request, so changing this field after start() takes effect too -- do not treat
     * "register only when non-empty" as an optimization.
     */
    public string $namespaceV2 = '';

    public RemotingClient $remotingClient;

    /** @var array<string, TopicRouteData> */
    public array $topicRouteTable = [];
    /** @var array<string, TopicPublishInfo> */
    public array $topicPublishInfoTable = [];

    /**
     * A flat table keyed by brokerName, wholesale overwritten whenever any route is refreshed.
     *
     * @var array<string, array<int, string>>
     */
    public array $brokerAddrTable = [];

    public array $topicEndPointsTable = [];

    /** PHP single-threaded sequential execution: route table reads/writes do not span requests; here only the mutex-semantics marker is kept. */
    public bool $topicRouteLockHeld = false;

    public DefaultTopAddressing $topAddressing;

    public ConsumerStatsManager $consumerStatsManager;

    /** Injectable "current time" closure (milliseconds), for tests. Uses microtime when empty. */
    public ?\Closure $clock = null;

    private bool $started = false;

    /** @var array<string, bool> the set of topics "in use" by this client */
    private array $topicsInUse = [];

    /** @var array<string, object> consumerGroup -> consumer instance */
    private array $consumerTable = [];

    /** @var array<string, bool> producerGroup -> registered flag */
    private array $producerTable = [];

    /** @var list<string> broker addresses the current registration was last beaten to (sorted) */
    private array $brokersBeaten = [];

    /** Successful heartbeat rounds (each round may cover several brokers). */
    private int $heartbeatCount = 0;

    private int $consumerIdsChangedCount = 0;

    /** @var list<callable> the delayed queue of "background tasks" under a single thread (actions that should not block the caller) */
    private array $pendingActions = [];

    private ?MQFaultStrategy $faultStrategy = null;

    private array $popAssignmentPollers = [];

    private bool $namesrvRefreshEnabled = false;
    private ?int $namesrvRefreshNextRunMillis = null;
    private ?int $adjustPoolNextRunMillis = null;
    private ?int $routeRefreshNextRunMillis = null;
    private ?int $statsSampleNextRunMillis = null;
    private ?int $heartbeatNextRunMillis = null;
    /** RequestFutureHolder TTL sweep: first 3s, period 1s. */
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
        bool $useHeartbeatV2 = false,
        ?SocksProxyConfig $socksProxy = null,
        int $heartbeatIntervalMillis = self::HEARTBEAT_INTERVAL_MILLIS,
        bool $decodeReadBody = true,
        int $mqClientApiTimeout = self::DEFAULT_MQ_CLIENT_API_TIMEOUT_MILLIS,
        bool $enableConcurrentHeartbeat = false,
        ?int $concurrentHeartbeatThreadPoolSize = null,
    ) {
        $this->clientId = $clientId;
        $this->nameServerAddrs = array_values($nameServerAddrs);
        $this->pollNameServerInterval = $pollNameServerInterval;
        $this->heartbeatIntervalMillis = $heartbeatIntervalMillis > 0
            ? $heartbeatIntervalMillis
            : self::HEARTBEAT_INTERVAL_MILLIS;
        $this->vipChannelEnabled = $vipChannelEnabled;
        $this->useHeartbeatV2 = $useHeartbeatV2;
        $this->decodeReadBody = $decodeReadBody;
        $this->mqClientApiTimeout = $mqClientApiTimeout > 0
            ? $mqClientApiTimeout
            : self::DEFAULT_MQ_CLIENT_API_TIMEOUT_MILLIS;
        $this->enableConcurrentHeartbeat = $enableConcurrentHeartbeat;
        $this->concurrentHeartbeatThreadPoolSize = $concurrentHeartbeatThreadPoolSize !== null
            && $concurrentHeartbeatThreadPoolSize > 0
            ? $concurrentHeartbeatThreadPoolSize
            : self::availableProcessors();
        $this->namespaceV2 = $namespaceV2;
        $this->remotingClient = new RemotingClient($connectTimeoutMillis, $invokeTimeoutMillis, $tlsEnable, tlsOptions: $tlsOptions, socksProxy: $socksProxy);
        $this->remotingClient->registerRpcHook(new NamespaceRpcHook(\Closure::fromCallable([$this, 'namespaceV2Value'])));
        // The stream hook must also be registered before the user rpcHook, so that ReqT is
        // counted in the ACL signature content ("Inject stream rpc hook first to make
        // reserve field signature"). Facades all register rpcHook only after this
        // instance is constructed.
        if ($enableStreamRequestType) {
            $this->remotingClient->registerRpcHook(new StreamTypeRPCHook());
        }

        // Request-Reply: the broker pushes the reply back via PUSH_REPLY_MESSAGE_TO_CLIENT(326).
        $this->remotingClient->registerProcessor(
            RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT,
            \Closure::fromCallable([$this, 'processReplyMessage']),
        );
        // Broker-initiated requests 220/221/307/309:
        // registered at the client instance level; on handling, the corresponding consumer instance is found by consumerGroup.
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
        // NOTIFY_CONSUMER_IDS_CHANGED(40): when consumer group members change, the broker pushes it back along the long connection.
        $this->remotingClient->registerProcessor(
            RequestCode::NOTIFY_CONSUMER_IDS_CHANGED,
            \Closure::fromCallable([$this, 'processNotifyConsumerIdsChanged']),
        );

        // Dynamic name server (DefaultTopAddressing pulls the address list by domain). When
        // ROCKETMQ_NAMESRV_DOMAIN is not configured, wsAddr is an empty string -> fetch is a no-op.
        $this->topAddressing = new DefaultTopAddressing(unitName: $unitName ?? '');
        // Consume stats: shared at instance level; multiple producers/consumers share the same stats.
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
     * The currently effective namespaceV2 -- {@see NamespaceRpcHook} calls this method for the
     * value per request, i.e. the hook reads the field in real time: after start(), changing
     * this field immediately changes subsequent requests
     * (cf. the comment on Remoting/NamespaceRpcHook).
     */
    public function namespaceV2Value(): string
    {
        return $this->namespaceV2;
    }

    // ---------------- Configuration setters ----------------

    /** Gate body materialisation of PULL / LITE_PULL / POP responses (takes effect on the next decode). */
    public function setDecodeReadBody(bool $decodeReadBody): void
    {
        $this->decodeReadBody = $decodeReadBody;
    }

    public function isDecodeReadBody(): bool
    {
        return $this->decodeReadBody;
    }

    /** Fallback timeout of the internal API calls; an explicit per-call timeout still wins. */
    public function setMqClientApiTimeout(int $timeoutMillis): void
    {
        $this->mqClientApiTimeout = $timeoutMillis;
    }

    public function getMqClientApiTimeout(): int
    {
        return $this->mqClientApiTimeout;
    }

    public function setEnableConcurrentHeartbeat(bool $enable): void
    {
        $this->enableConcurrentHeartbeat = $enable;
    }

    public function isEnableConcurrentHeartbeat(): bool
    {
        return $this->enableConcurrentHeartbeat;
    }

    /** Resize the concurrently dispatching heartbeat pool when it already exists. */
    public function setConcurrentHeartbeatThreadPoolSize(int $size): void
    {
        $this->concurrentHeartbeatThreadPoolSize = $size > 0 ? $size : self::availableProcessors();
        $this->heartbeatExecutor?->setCorePoolSize($this->concurrentHeartbeatThreadPoolSize);
    }

    public function getConcurrentHeartbeatThreadPoolSize(): int
    {
        return $this->concurrentHeartbeatThreadPoolSize;
    }

    /** The worker count of the live heartbeat pool (0 before start()); for observation. */
    public function heartbeatExecutorSize(): int
    {
        return $this->heartbeatExecutor?->getCorePoolSize() ?? 0;
    }

    /**
     * The PHP stand-in for the runtime's available-processor count: PHP exposes no core count, so
     * the same 4-core stand-in the send executors use applies (RMQ_CPU_CORES overrides it).
     */
    public static function availableProcessors(): int
    {
        $raw = getenv('RMQ_CPU_CORES');
        if ($raw !== false && (int) $raw > 0) {
            return (int) $raw;
        }
        return 4;
    }

    // ---------------- Time ----------------

    public function currentTimeMillis(): int
    {
        if ($this->clock !== null) {
            return (int) ($this->clock)();
        }
        return (int) floor(microtime(true) * 1000.0);
    }

    // ---------------- Lifecycle ----------------

    /**
     * Synchronous initialization (PHP starts no background threads; all periodic tasks are pumped by the caller).
     *
     * (1) set started; (2) start consume stats; (3) dynamic NameServer first address fetch
     * (report 10004 directly if unavailable); (4) schedule each pump's first expiry, the
     * heartbeat pump included; (5) synchronously run one initialization "pump": first route
     * refresh + one heartbeat + CHECK_CLIENT_CONFIG(46) validation (the latter best-effort;
     * exceptions are only logged).
     *
     * Afterwards the caller periodically calls ``tick()`` to advance namesrv refresh /
     * route refresh / thread patrol / stats sampling.
     */
    public function start(): void
    {
        $this->started = true;
        $this->consumerStatsManager->start();
        // The concurrent-heartbeat pool is part of the started instance: one worker per configured
        // slot, created once here and reused by every round until shutdown().
        if ($this->heartbeatExecutor === null) {
            $this->heartbeatExecutor = new ConsumeExecutor(
                $this->concurrentHeartbeatThreadPoolSize,
                $this->concurrentHeartbeatThreadPoolSize,
                0.0,
                'client-heartbeat',
                0,
            );
        }

        $dynamicNs = $this->nameServerAddrs === []
            && $this->topAddressing->wsAddr !== '';
        if ($dynamicNs) {
            $this->fetchNameServerAddr();
            if ($this->nameServerAddrs === []) {
                // Failing to get the address fails at start() (more predictable);
                // the code value still uses 10004 NO_NAME_SERVER_EXCEPTION.
                throw new MQClientException(
                    sprintf(
                        'name server address is not set and address server (%s) returned none',
                        $this->topAddressing->wsAddr,
                    ),
                    ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
                );
            }
            // Periodic refresh: after the first 10s, once every 2 minutes
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
        // The broker forgot everything this clientId registered when the previous session's
        // channel went away, so no broker counts as beaten yet.
        $this->brokersBeaten = [];
        $this->heartbeatNextRunMillis = $now + $this->heartbeatIntervalMillis;

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
        $this->faultStrategy = null;
        $this->popAssignmentPollers = [];
        $this->pendingActions = [];
        $this->heartbeatNextRunMillis = null;
        $this->brokersBeaten = [];
        $this->heartbeatExecutor?->shutdown(true);
        $this->heartbeatExecutor = null;
        $this->consumerStatsManager->shutdown();
        $this->remotingClient->shutdown();
    }

    public function isStarted(): bool
    {
        return $this->started;
    }

    /**
     * Explicit event pump: advances all of this port's periodic tasks. Called periodically
     * by the caller (usually the main loop / tests).
     *
     * @param int|null $nowMillis injectable current time (milliseconds); uses the internal clock when empty.
     */
    public function tick(?int $nowMillis = null): void
    {
        $now = $nowMillis ?? $this->currentTimeMillis();

        // Delayed action queue (e.g. rebalance triggered by resetOffset)
        $this->runPendingActions();

        if (!$this->started) {
            return;
        }

        // (1) dynamic NameServer refresh
        if ($this->namesrvRefreshEnabled
            && $this->namesrvRefreshNextRunMillis !== null
            && $now >= $this->namesrvRefreshNextRunMillis) {
            $this->namesrvRefreshOnce();
            $this->namesrvRefreshNextRunMillis = $now + self::NAMESRV_REFRESH_INTERVAL_MILLIS;
        }

        // (2) thread elasticity patrol
        if ($this->adjustPoolNextRunMillis !== null && $now >= $this->adjustPoolNextRunMillis) {
            $this->adjustThreadPool();
            $this->adjustPoolNextRunMillis = $now + self::ADJUST_POOL_INTERVAL_MILLIS;
        }

        // (3) route refresh (period = pollNameServerInterval); a route that brings a new
        //   broker re-registers right away (heartbeatIfBrokerSetChanged inside the fetch).
        if ($this->routeRefreshNextRunMillis !== null && $now >= $this->routeRefreshNextRunMillis) {
            $this->routeRefreshOnce();
            $this->routeRefreshNextRunMillis = $now + $this->pollNameServerInterval;
        }

        // (3b) registration refresh. This port starts no threads, and the broker drops a
        //   group as soon as its channel is gone, so this stage is the only thing that keeps
        //   a producer-only client registered: the producer facade drives no beat itself.
        if ($this->heartbeatNextRunMillis !== null && $now >= $this->heartbeatNextRunMillis) {
            $this->heartbeatNextRunMillis = $now + $this->heartbeatIntervalMillis;
            try {
                $this->heartbeatOnce();
            } catch (\Throwable $e) {
                Logger::warning('heartbeat pump exception: ' . $e->getMessage());
            }
        }

        // (4) consume stats sampling
        if ($this->statsSampleNextRunMillis !== null && $now >= $this->statsSampleNextRunMillis) {
            $this->consumerStatsManager->sampleOnce();
            $this->statsSampleNextRunMillis = $now + self::STATS_SAMPLE_INTERVAL_MILLIS;
        }

        // (5) RequestFutureHolder TTL sweep (first 3s, period 1s). Under PHP's single thread there is no concurrency;
        //   only one of a request's timeout path and reply path needs to take effect -- putResponse uses
        //   the "only responsible if removed" remove semantics, which is naturally mutually exclusive.
        if ($this->requestScanNextRunMillis !== null && $now >= $this->requestScanNextRunMillis) {
            $this->requestScanNextRunMillis = $now + self::REQUEST_SCAN_INTERVAL_MILLIS;
            try {
                RequestFutureHolder::getInstance()->scanExpiredRequest();
            } catch (\Throwable $e) {
                Logger::warning('scan RequestFutureTable exception: ' . $e->getMessage());
            }
        }

        if ($this->faultStrategy !== null && $this->faultStrategy->isStartDetectorEnable()) {
            try {
                $this->faultStrategy->detectByOneRound(
                    $now,
                    fn (string $brokerName): ?string => $this->findBrokerAddressInPublish($brokerName),
                    $this->detectService(...)
                );
            } catch (\Throwable $e) {
                Logger::warning('fault detector round exception: ' . $e->getMessage());
            }
        }

        foreach ($this->popAssignmentPollers as $poller) {
            if ($now < $poller->nextRunMillis) {
                continue;
            }
            try {
                $this->runPopAssignmentRound($poller, $now);
            } catch (\Throwable $e) {
                // A single round blowing up does not affect other registrants or the next round (nextRunMillis is already rearranged at the round start).
                Logger::warning('pop assignment round exception: ' . $e->getMessage());
            }
        }
    }

    // ---------------- POP broker-side assignment (scheduling surface of the QUERY_ASSIGNMENT leg) ----------------

    public function registerPopAssignmentPoller(PopAssignmentPoller $poller): void
    {
        $poller->nextRunMillis = 0;
        $this->popAssignmentPollers[$poller->key] = $poller;
    }

    public function unregisterPopAssignmentPoller(string $key): void
    {
        unset($this->popAssignmentPollers[$key]);
    }

    public function popAssignmentImmediately(?string $key = null): void
    {
        foreach ($this->popAssignmentPollers as $k => $poller) {
            if ($key === null || $k === $key) {
                $poller->nextRunMillis = 0;
            }
        }
    }

    /** The set of registered poller keys (for tests/observation). */
    public function popAssignmentPollerKeys(): array
    {
        return array_keys($this->popAssignmentPollers);
    }

    public function runPopAssignmentRound(PopAssignmentPoller $poller, ?int $nowMillis = null): void
    {
        $now = $nowMillis ?? $this->currentTimeMillis();
        $poller->nextRunMillis = $now + $poller->intervalMillis;
        $topics = ($poller->topicsFn)();
        foreach ($topics as $topic) {
            try {
                $assignments = $this->queryAssignment(
                    $topic,
                    $poller->consumerGroup,
                    $poller->clientId,
                    $poller->strategyName,
                    $poller->messageModel,
                    self::QUERY_ASSIGNMENT_TIMEOUT_MILLIS,
                );
            } catch (\Throwable $e) {
                Logger::error(sprintf(
                    'allocate message queue exception. strategy name: %s, topic %s: %s (keep current assignment)',
                    $poller->strategyName,
                    $topic,
                    $e->getMessage(),
                ));
                continue;
            }
            if ($assignments === null) {
                Logger::warning(sprintf(
                    'queryAssignment: no assignment set for topic %s group %s, keep current assignment',
                    $topic,
                    $poller->consumerGroup,
                ));
                continue;
            }
            ($poller->applyFn)($topic, $assignments);
        }
    }

    public function registerFaultStrategy(?MQFaultStrategy $strategy): void
    {
        $this->faultStrategy = $strategy;
    }

    private function detectService(string $brokerAddr, int $timeoutMillis): bool
    {
        $topics = array_keys($this->topicsInUse);
        if ($topics === []) {
            return false;
        }
        $mq = new MessageQueue($topics[0], '', 0);
        try {
            $this->getMaxOffset($mq, $timeoutMillis, $brokerAddr);
            return true;
        } catch (\Throwable $e) {
            Logger::debug(sprintf(
                'detect GET_MAX_OFFSET to %s failed: %s',
                $brokerAddr,
                $e->getMessage()
            ));
            return false;
        }
    }

    /** An alias for tick() (the alternative naming requested by users). */
    public function pumpOnce(?int $nowMillis = null): void
    {
        $this->tick($nowMillis);
    }

    /** One round of dynamic NameServer refresh. */
    public function namesrvRefreshOnce(): void
    {
        try {
            $this->fetchNameServerAddr();
        } catch (\Throwable $e) {
            Logger::debug('fetchNameServerAddr exception: ' . $e->getMessage());
        }
    }

    /** One round of route refresh: back-fill each in-use topic. */
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
     * start()'s synchronous "first pump": first route refresh + one heartbeat + CHECK_CLIENT_CONFIG(46) (best-effort).
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

    /**
     * Send one heartbeat to all known brokers with the full registration of this clientId.
     *
     * A client that has registered nothing sends nothing: an empty HeartbeatData re-registers
     * neither a producer group nor a consumer group, so beating with one only spends the
     * round trip. The timeout falls back to this instance's mqClientApiTimeout; an explicit
     * argument still wins. Returns the number of brokers that accepted the beat.
     */
    public function heartbeatOnce(?int $timeoutMillis = null): int
    {
        return $this->heartbeatRound(false, $timeoutMillis);
    }

    /**
     * Round that follows a queue-allocation change: the remembered fingerprints are dropped
     * first, so every broker is handed the subscriptions again instead of the cheap body.
     */
    public function heartbeatOnceAfterRebalance(?int $timeoutMillis = null): int
    {
        return $this->heartbeatRound(true, $timeoutMillis);
    }

    private function heartbeatRound(bool $isRebalance, ?int $timeoutMillis): int
    {
        $timeoutMillis ??= $this->mqClientApiTimeout;
        $data = $this->buildHeartbeatData();
        if ($data->producerDataSet === [] && $data->consumerDataSet === []) {
            return 0;
        }
        // A slave holds no consumer registrations: when the body has no consumer half there is
        // nothing for it to learn, so the round only reaches masters.
        $targets = $this->heartbeatTargetAddrs($data->consumerDataSet === []);
        if ($targets === []) {
            return 0;
        }
        $fingerprint = 0;
        $withoutSub = null;
        if ($this->useHeartbeatV2) {
            // One value per round, stamped on both bodies: the cheap body is only valid because it
            // carries the **full** body's fingerprint, since recomputing over a subscription-
            // stripped body deliberately differs and would claim a change every round.
            $fingerprint = $data->computeFingerprint();
            $data->heartbeatFingerprint = $fingerprint;
            $withoutSub = $data->withoutSubCopy();
            if ($isRebalance) {
                $this->resetBrokerAddrHeartbeatFingerprint();
            }
        }
        // Dispatch order: the fingerprint path wins, then the concurrent pool, then the serial loop.
        if ($withoutSub !== null) {
            $ok = $this->sendHeartbeatRoundSeriallyV2($targets, $data, $withoutSub, $fingerprint, $timeoutMillis);
        } elseif ($this->enableConcurrentHeartbeat) {
            $ok = $this->sendHeartbeatRoundConcurrently($targets, $data, $timeoutMillis);
        } else {
            $ok = $this->sendHeartbeatRoundSerially($targets, $data, $timeoutMillis);
        }
        if ($ok > 0) {
            $this->heartbeatCount++;
        }
        // Only a round that reached every target counts as done: a broker that is briefly
        // unreachable keeps being retried by the next pump instead of being remembered as beaten.
        if ($ok > 0 && $ok === count($targets)) {
            $sorted = $targets;
            sort($sorted);
            $this->brokersBeaten = $sorted;
        }
        return $ok;
    }

    /**
     * Addresses one round must reach.
     *
     * The broker address table is the source of truth (it carries the broker ids); the topic
     * routes are only a fallback for an instance whose address table has not been filled yet.
     * A body without consumer data is addressed to masters only.
     *
     * @return list<string>
     */
    private function heartbeatTargetAddrs(bool $consumerEmpty): array
    {
        if ($this->brokerAddrTable === []) {
            return $this->getAllBrokerAddrs();
        }
        $addrs = [];
        foreach ($this->brokerAddrTable as $brokerAddrs) {
            foreach ($brokerAddrs as $id => $addr) {
                if ($addr === null || $addr === '') {
                    continue;
                }
                if ($consumerEmpty && (int) $id !== MixAll::MASTER_ID) {
                    continue;
                }
                if (!in_array($addr, $addrs, true)) {
                    $addrs[] = $addr;
                }
            }
        }
        return $addrs;
    }

    /**
     * @param list<string> $addrs
     */
    private function sendHeartbeatRoundSerially(array $addrs, HeartbeatData $data, int $timeoutMillis): int
    {
        $ok = 0;
        foreach ($addrs as $addr) {
            try {
                $this->sendHeartbeat($addr, $data, $timeoutMillis);
                $ok++;
            } catch (\Throwable $e) {
                Logger::debug(sprintf('heartbeatOnce failed, addr=%s: %s', $addr, $e->getMessage()));
            }
        }
        return $ok;
    }

    /**
     * @param list<string> $addrs
     */
    private function sendHeartbeatRoundSeriallyV2(
        array $addrs,
        HeartbeatData $withSub,
        HeartbeatData $withoutSub,
        int $fingerprint,
        int $timeoutMillis,
    ): int {
        $ok = 0;
        foreach ($addrs as $addr) {
            try {
                $this->sendHeartbeatV2($addr, $withSub, $withoutSub, $fingerprint, $timeoutMillis);
                $ok++;
            } catch (\Throwable $e) {
                Logger::debug(sprintf('heartbeatOnce failed, addr=%s: %s', $addr, $e->getMessage()));
            }
        }
        return $ok;
    }

    /**
     * One task per broker on the pool created by start(); the round returns only after every
     * submitted task has run. A rejected submission still counts its slot down, otherwise the
     * wait would never finish; a failed beat counts as a failed broker, not a failed round.
     *
     * @param list<string> $addrs
     */
    private function sendHeartbeatRoundConcurrently(array $addrs, HeartbeatData $data, int $timeoutMillis): int
    {
        $pool = $this->heartbeatExecutor;
        if ($pool === null) {
            // An instance driven before start() still gets a pool, so the switch behaves the same.
            $pool = new ConsumeExecutor(
                $this->concurrentHeartbeatThreadPoolSize,
                $this->concurrentHeartbeatThreadPoolSize,
                0.0,
                'client-heartbeat',
                0,
            );
            $this->heartbeatExecutor = $pool;
        }
        $ok = 0;
        $pending = 0;
        foreach ($addrs as $addr) {
            $pending++;
            try {
                $pool->submit(function () use ($addr, $data, $timeoutMillis, &$ok, &$pending): void {
                    try {
                        $this->sendHeartbeat($addr, $data, $timeoutMillis);
                        $ok++;
                    } catch (\Throwable $e) {
                        Logger::debug(sprintf('heartbeatOnce failed, addr=%s: %s', $addr, $e->getMessage()));
                    } finally {
                        $pending--;
                    }
                });
            } catch (RejectedExecutionError $e) {
                // Nothing was queued, so nothing will count this slot down later: do it here or
                // the wait below never returns.
                Logger::warning(sprintf('heartbeat submission rejected for %s: %s', $addr, $e->getMessage()));
                $pending--;
            }
        }
        // Cooperative "wait": PHP has one thread, so draining the pool is waiting for the tasks.
        while ($pending > 0 && $pool->pumpOnce()) {
        }
        return $ok;
    }

    /**
     * Beat as soon as the route cache shows brokers that the registration has not reached.
     *
     * A heartbeat only goes to brokers already in the cache, so the first beat of a client
     * that has not fetched any route lands nowhere; without this hook the group would wait
     * for the next 30s round to register with a broker that joined in between.
     */
    public function heartbeatIfBrokerSetChanged(?int $timeoutMillis = null): void
    {
        $addrs = $this->getAllBrokerAddrs();
        if ($addrs === []) {
            return;
        }
        sort($addrs);
        if ($addrs === $this->brokersBeaten) {
            return;
        }
        $this->heartbeatOnce($timeoutMillis);
    }

    /** Number of successful heartbeat rounds driven by this instance. */
    public function heartbeatCount(): int
    {
        return $this->heartbeatCount;
    }

    /**
     * The registration of this clientId: every producer group and every consumer group it
     * owns, so one beat refreshes both kinds on the broker.
     *
     * The broker's ProducerManager/ConsumerManager learn a group only from this payload; a
     * beat missing one kind leaves that kind unregistered whenever its facade drives no
     * heartbeat of its own, and the broker then holds no channel to route
     * CHECK_TRANSACTION_STATE or a reply back on.
     */
    public function buildHeartbeatData(): HeartbeatData
    {
        $hb = new HeartbeatData($this->clientId);
        $groups = array_keys($this->producerTable);
        sort($groups);
        foreach ($groups as $group) {
            $hb->producerDataSet[] = new ProducerData($group);
        }
        foreach ($this->consumerTable as $group => $consumer) {
            if (!$consumer instanceof DefaultMQPushConsumer) {
                continue;
            }
            try {
                $one = $consumer->buildHeartbeat();
            } catch (\Throwable $e) {
                Logger::debug(sprintf('heartbeat build failed for group %s: %s', $group, $e->getMessage()));
                continue;
            }
            foreach ($one->consumerDataSet as $cd) {
                $hb->consumerDataSet[] = $cd;
            }
        }
        return $hb;
    }

    /** Execute and clear the delayed action queue (all actions that should not slow the caller are queued here). */
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

    // ---------------- Dynamic NameServer ----------------

    /**
     * Fetch the name server list from the address server: apply only when the address changes.
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

    // ---------------- Consumer registration / thread patrol ----------------

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

    /**
     * A producer group owned by this clientId. The heartbeat payload carries only the group
     * name, so the registry is a set.
     */
    public function registerProducer(string $group): void
    {
        $this->producerTable[$group] = true;
    }

    public function unregisterProducer(string $group): void
    {
        unset($this->producerTable[$group]);
    }

    public function hasProducer(string $group): bool
    {
        return isset($this->producerTable[$group]);
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
     * CHECK_CLIENT_CONFIG(46): let the subscription expressions in consumer groups be validated by the broker side.
     *
     * Only **non-TAG** (SQL92 / CLASS_FILTER) expressions are sent to the broker for
     * validation. When a consumer has "no subscriptions" it is ``return`` rather than
     * ``continue`` (protocol-wise the whole round of validation ends here); when no route is
     * found, that subscription is skipped.
     */
    public function checkClientInBroker(?int $timeoutMillis = null): void
    {
        foreach ($this->consumerTable as $group => $consumer) {
            $subs = $consumer !== null ? $consumer->subscriptions() : null;
            if ($subs === null || $subs === [] || $subs === false) {
                // return, not continue: the subscriptions of later consumers are no longer validated.
                return;
            }
            $this->checkSubscriptionsInBroker($group, $subs, $timeoutMillis);
        }
    }

    /**
     * @param iterable<SubscriptionData> $subs
     */
    public function checkSubscriptionsInBroker(string $group, iterable $subs, ?int $timeoutMillis = null): void
    {
        foreach ($subs as $sub) {
            // null / "" / "TAG" all count as TAG expressions and are skipped uniformly (the broker side needs no validation).
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
                $this->checkClientConfig($addr, $group, $this->clientId, $sub, $timeoutMillis);
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
     * NOTIFY_CONSUMER_IDS_CHANGED(40): the broker sends it via invokeOneway; returning null means no reply.
     * group is only for logging; the whole group is woken together.
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

    // ---------------- Broker-initiated request handling ----------------

    /**
     * RESET_CONSUMER_CLIENT_OFFSET(220): the broker sends it via invokeOneway; no reply needed.
     *
     * The reset logic triggers rebalance (lock/unlock and other invokeSync calls), which
     * cannot run synchronously in this round -- it enters the pending queue here and is
     * executed on the next tick(), avoiding nested synchronous calls in the reverse
     * request's handling thread.
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
        // The PHP-side offset table uses ResetOffsetBody's native list<array{mq,offset}> shape
        // (MessageQueue is an object and cannot be an array key).
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

    /** GET_CONSUMER_STATUS_FROM_CLIENT(221): returns the consumed offset table. */
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

    /** GET_CONSUMER_RUNNING_INFO(307): returns this consumer's running info. */
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

    /** CONSUME_MESSAGE_DIRECTLY(309): the broker pushes one message down, requiring one real local consumption. */
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

    // ---------------- Low-level invoke ----------------

    /**
     * The instance's single synchronous exit point. Protected so a test subclass can intercept
     * every request the instance puts on the wire -- and answer it -- without a broker.
     */
    protected function invokeSync(string $addr, RemotingCommand $request, ?int $timeoutMillis = null): RemotingCommand
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

    // ---------------- Route management ----------------

    /**
     * Fetch and persist the topic route.
     *
     * ``isDefault`` is the switch for "producer creating a new topic": **only producers**
     * fall back to TBW102 to synthesize publish info for a new topic when the real route
     * cannot be fetched.
     */
    public function updateTopicRouteInfoFromNameServer(string $topic, ?int $timeoutMillis = null, bool $isDefault = false): bool
    {
        if ($this->nameServerAddrs === []) {
            // This port throws when the route cannot be fetched; here the 10004 (no name server) code is carried directly.
            throw new MQClientException(
                'name server address list is empty',
                ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
            );
        }
        // An explicit per-call timeout wins; otherwise the configured internal-API timeout applies
        // to both the per-topic fetch and the default-topic fallback below.
        $timeoutMillis ??= $this->mqClientApiTimeout;

        $fetch = function (string $t) use ($timeoutMillis): ?TopicRouteData {
            $request = RemotingCommand::createRequestCommand(RequestCode::GET_ROUTEINFO_BY_TOPIC, null);
            $request->extFields['topic'] = $t;
            $lastExc = null;
            foreach ($this->nameServerAddrs as $nsAddr) {
                try {
                    $response = $this->invokeSync($nsAddr, $request, $timeoutMillis);
                } catch (\Throwable $e) {
                    // Connect/send/timeout-level failures -> try the next name server.
                    $lastExc = $e;
                    continue;
                }
                // Response received: whether SUCCESS or TOPIC_NOT_EXIST, it is a final answer; no more polling.
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
                // A new topic has queues created by the broker using default_topic_queue_nums, trimmed to the broker's actual created count.
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

        // The route table and broker address table must be updated together: under a single thread this section is atomic,
        // so no intermediate state of "new route, old address table" can appear.
        $this->topicRouteTable[$topic] = $route;
        foreach ($route->getBrokerDatas() as $bd) {
            $this->brokerAddrTable[$bd->brokerName] = $bd->brokerAddrs;
        }
        $endpoints = StaticTopic::topicRouteData2EndpointsForStaticTopic($topic, $route);
        if ($endpoints !== null) {
            $this->topicEndPointsTable[$topic] = $endpoints;
        } else {
            unset($this->topicEndPointsTable[$topic]);
        }
        $publish = $this->topicPublishInfoTable[$topic] ??= new TopicPublishInfo();
        $publish->orderTopic = $route->orderTopicConf !== null;
        $publish->topicRouteData = $route;
        $publish->msgQueueList = $route->getAllMessageQueue($topic);
        $this->heartbeatIfBrokerSetChanged();
        return true;
    }

    public function getTopicPublishInfo(string $topic, bool $isDefault = false): TopicPublishInfo
    {
        $info = $this->topicPublishInfoTable[$topic] ?? null;
        if ($info !== null && $info->ok()) {
            return $info;
        }
        $this->updateTopicRouteInfoFromNameServer($topic, isDefault: $isDefault);
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
            // Back-fill failure is silent; return the cache (null at this point)
        }
        return $this->topicRouteTable[$topic] ?? null;
    }

    /**
     * All queues this instance should see when subscribing to this topic (including read-only queues, for rebalance to take from).
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
     * Randomly pick a broker from the route (master address preferred); returns null if not found (no throw).
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

    // ---------------- Message sending ----------------

    /**
     * VIP channel conversion: subtract 2 from the broker port when the switch is on.
     * Returns as-is when off or the port is unparseable; **applied only once before
     * sending a request** (the port gets -2; repeated application subtracts multiple times
     * -- the caller must not convert the same address twice).
     */
    public function vipAddrFor(string $addr): string
    {
        return MixAll::brokerVipChannel($this->vipChannelEnabled, $addr);
    }

    /**
     * "Master only" address resolution: look up the publish address (brokerId=0 only) -> if
     * not found, refresh the route once by topic -> re-query -> if still not found, throw
     * "The broker[X] not exist".
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

    public function getBrokerNameFromMessageQueue(MessageQueue $mq): string
    {
        $endpoints = $this->topicEndPointsTable[$mq->topic] ?? null;
        if ($endpoints !== null && $endpoints !== []) {
            $real = $endpoints[StaticTopic::queueKey($mq)] ?? null;
            if ($real !== null) {
                return $real;
            }
        }
        return $mq->brokerName;
    }

    public function popAckAddrFor(string $brokerName, string $topic, int $queueId): string
    {
        $resolveDes = function () use ($brokerName, $topic, $queueId): string {
            if (StaticTopic::isLogicalQueueMockBrokerName($brokerName)) {
                return $this->getBrokerNameFromMessageQueue(
                    new MessageQueue($topic, $brokerName, $queueId),
                );
            }
            return $brokerName;
        };

        $des = $resolveDes();
        $addr = $this->findBrokerAddressInPublish($des);
        if ($addr === null || $addr === '') {
            $this->updateTopicRouteInfoFromNameServer($topic);
            $des = $resolveDes();
            $addr = $this->findBrokerAddressInPublish($des);
        }
        if ($addr === null || $addr === '') {
            throw new MQClientException(sprintf('Broker[%s] master node does not exist', $des));
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
        // The send path goes through VIP channel conversion
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

    /** Only **builds** a SEND_MESSAGE request object without sending it (the async send chain needs to reuse the same request across retries). */
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
     * Send an already-built send request asynchronously.
     *
     * ``$onComplete`` looks like ``function (?SendResult $result, ?\Throwable $error): void``,
     * and is invoked exactly once. Response decoding also happens at this layer (parsed on the
     * success branch, and parse errors go through the callback too).
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
        // Non-batch messages get UNIQ_KEY added before sending; batch messages have each written it at MessageBatch construction time.
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
        // WARNING: maxReconsumeTimes is only sent when "sending to %RETRY% and the message carries the MAX_RECONSUME_TIMES property".
        $header->maxReconsumeTimes = null;
        // When sending to %RETRY%<group>, lift the retry properties into the request header (the broker reads requestHeader).
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
        // V2's brokerName key is the single letter `n`.
        $header->brokerName = $mq->brokerName !== '' ? $mq->brokerName : null;

        // reply takes precedence over batch: when the same message carries both the reply flag and is a batch, send it as SEND_REPLY_MESSAGE_V2.
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

    /** The message body is sent as-is (in the protocol the body is the message payload; properties go in the request header). */
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
        // msgId = client unique ID (batches use the batch's own UNIQ_KEY); offsetMsgId = response header msgId;
        // regionId = response header MSG_REGION (falls back to DefaultRegion when absent); traceOn = TRACE_ON != "false".
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

    // ---------------- Scheduled message recall ----------------

    /** RECALL_MESSAGE(370): recall a scheduled/delayed message, returning the recall msgId given by the broker. */
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

    // ---------------- Request-Reply: receiving the broker's pushed-back reply ----------------

    /**
     * Handle PUSH_REPLY_MESSAGE_TO_CLIENT(326): hand the reply to the waiting request().
     *
     * **A response must be returned** (the broker side is invokeSync; not returning one times out).
     */
    public function processReplyMessage(RemotingCommand $cmd, string $addr): ?RemotingCommand
    {
        $header = new ReplyMessageRequestHeader();
        $header->fromExtFields($cmd->extFields);
        try {
            $body = $cmd->body ?? '';
            // When sysFlag carries a compression flag, decompress first: 326 pushes a bare packet, not going through the message decoding path.
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
                // Not found is normal (the request timed out / the reply is duplicated); only a warn is logged.
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

    // ---------------- Message pulling ----------------

    /**
     * Resolve the broker address and build the pull frame for one queue.
     *
     * This is the whole request-shape logic, shared by the synchronous {@see pullMessage()} and
     * the asynchronous {@see pullMessageAsync()}: both put **the same** header cells on the wire
     * (including the commit-offset clearing that a slave answer requires).
     *
     * @return array{0: RemotingCommand, 1: string} [request frame, broker address to send it to]
     */
    private function preparePullRequest(
        MessageQueue $mq,
        int $queueOffset,
        int $maxMsgNums,
        int $sysFlag,
        int $commitOffset,
        ?string $subscription,
        int $subVersion,
        ?string $expressionType,
        int $maxMsgBytes,
        int $suspendTimeoutMillis,
        string $consumerGroup,
        int $requestSource = 0,
        ?string $addr = null,
        ?int $brokerId = null,
    ): array {
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
            // Committing offsets on a slave is meaningless; clear the COMMIT_OFFSET bit.
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
        // The lite pull bit decides the request code LITE_PULL_MESSAGE(361) vs PULL_MESSAGE(11).
        $code = PullSysFlag::hasLitePullFlag($sysFlag)
            ? RequestCode::LITE_PULL_MESSAGE
            : RequestCode::PULL_MESSAGE;
        return [RemotingCommand::createRequestCommand($code, $header), $addr];
    }

    /**
     * Turn one pull response into a pull result: status mapping, response header cells and the
     * decoded message list (each message re-stamped with the queue it came from).
     */
    private function decodePullResponse(RemotingCommand $response, MessageQueue $mq): PullResult
    {
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
            // The body switch is read at decode time: off still walks every frame (cursor advances,
            // queue offsets and properties survive) but leaves each message body null.
            $found = MessageDecoder::decodeMessages($response->body, $this->decodeReadBody);
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
        [$request, $target] = $this->preparePullRequest(
            $mq,
            $queueOffset,
            $maxMsgNums,
            $sysFlag,
            $commitOffset,
            $subscription,
            $subVersion,
            $expressionType,
            $maxMsgBytes,
            $suspendTimeoutMillis,
            $consumerGroup,
            $requestSource,
            $addr,
            $brokerId,
        );
        // The pull path likewise goes through VIP channel conversion
        $response = $this->invokeSync($target, $request, $timeoutMillis);
        return $this->decodePullResponse($response, $mq);
    }

    /**
     * Asynchronous sibling of {@see pullMessage()}: same frame, same address resolution, same
     * decoding — only the transport differs. The request is written out and this returns right
     * away; ``$onResult`` fires when the caller pumps the connection (see
     * {@see \RocketMQ\Remoting\RemotingClient::waitResponses()}).
     *
     * A request that cannot be built at all (no route, unknown broker) throws synchronously,
     * exactly like the pull entry points declare; everything that fails **after** the frame was
     * written — broker error code, decode failure, transport failure, timeout — goes to
     * ``$onResult(null, error)``.
     *
     * @param callable(?PullResult, ?\Throwable): void $onResult
     */
    public function pullMessageAsync(
        callable $onResult,
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
    ): void {
        [$request, $target] = $this->preparePullRequest(
            $mq,
            $queueOffset,
            $maxMsgNums,
            $sysFlag,
            $commitOffset,
            $subscription,
            $subVersion,
            $expressionType,
            $maxMsgBytes,
            $suspendTimeoutMillis,
            $consumerGroup,
            $requestSource,
            $addr,
            $brokerId,
        );
        $onSuccess = function (?RemotingCommand $response) use ($onResult, $mq): void {
            try {
                if ($response === null) {
                    throw new MQClientException('pull response is empty');
                }
                $onResult($this->decodePullResponse($response, $mq), null);
            } catch (\Throwable $failure) {
                $onResult(null, $failure);
            }
        };
        $onFailure = function (\Throwable $failure) use ($onResult): void {
            $onResult(null, $failure);
        };
        $this->remotingClient->invokeAsync(
            $target,
            $request,
            \Closure::fromCallable($onSuccess),
            \Closure::fromCallable($onFailure),
            $timeoutMillis,
        );
    }

    /**
     * Select an address by brokerId: exact hit on that id; try adjacent ids when a slave is missing; fall back to the first when neither exists and not strict.
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

    // ---------------- POP mode (5.x lightweight consumption) ----------------

    public function queryAssignment(
        string $topic,
        string $consumerGroup,
        string $clientId,
        string $strategyName,
        string $messageModel,
        int $timeoutMillis = self::QUERY_ASSIGNMENT_TIMEOUT_MILLIS,
        ?string $addr = null,
    ): ?array {
        if ($addr === null) {
            $addr = $this->findBrokerAddrByTopic($topic);
            if ($addr === null) {
                try {
                    $this->updateTopicRouteInfoFromNameServer($topic);
                } catch (\Throwable $e) {
                    Logger::debug(sprintf('queryAssignment: route refresh for %s failed: %s', $topic, $e->getMessage()));
                }
                $addr = $this->findBrokerAddrByTopic($topic);
            }
            if ($addr === null) {
                return null;
            }
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
            if ($response->code === ResponseCode::TOPIC_NOT_EXIST) {
                return [];
            }
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }
        $set = QueryAssignmentResponseBody::decode($response->body)->messageQueueAssignments;
        if ($set === null) {
            return null;
        }
        $out = [];
        foreach ($set as $a) {
            if ($a->messageQueue === null) {
                continue;
            }
            $out[] = ['mq' => $a->messageQueue, 'mode' => $a->mode];
        }
        return $out;
    }

    /**
     * POP messages (RequestCode.POP_MESSAGE = 200050).
     *
     * Semantic difference from pull: no offset commits needed; confirm with ackMessage
     * after consumption; unacked messages are revived and redelivered by the broker after
     * invisibleTime; queueId = -1 means pop all queues of that topic.
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
            // POP is master only.
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

        // Response code mapping: only these four kinds are normal results; everything else is thrown as a broker exception.
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
            // Same decode-read-body contract as the pull path: with the switch off the POP frame is
            // walked to the end, messages keep their offsets/properties, bodies stay null.
            $found = MessageDecoder::decodeMessages($response->body, $this->decodeReadBody);
            // Reverse-construct POP_CK before rewriting the topic (retryFlag is derived from the message's original topic).
            $this->stampPopCk($found, $brokerName, $respHeader);
        }
        // Uniformly stamp brokerName and restore the topic to the requested topic.
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

    /** Lookup key for startOffsetInfo / msgOffsetInfo / sortMap: retry topics are grouped by retry marker + queueId. */
    private static function popQueueMapKey(MessageExt $m): string
    {
        $ck = $m->getProperty(MessageConst::PROPERTY_POP_CK);
        if ($ck !== null && $ck !== '') {
            return ExtraInfoUtil::getRetry(ExtraInfoUtil::split($ck)) . '@' . $m->queueId;
        }
        return ExtraInfoUtil::getStartOffsetInfoMapKey($m->topic, $m->queueId);
    }

    /**
     * Reverse-construct POP_CK and 1ST_POP_TIME for popped messages.
     *
     * @param list<MessageExt> $found
     */
    private function stampPopCk(array $found, string $brokerName, PopMessageResponseHeader $respHeader): void
    {
        $popTime = $respHeader->popTime ?? 0;
        $invisibleTime = $respHeader->invisibleTime ?? 0;
        $reviveQid = $respHeader->reviveQid ?? 0;

        if ($respHeader->startOffsetInfo === null || $respHeader->startOffsetInfo === '') {
            // When the broker did not provide startOffsetInfo: use the message's own queueOffset as ckQueueOffset to assemble 7 segments,
            // then manually add one more segment to make 8.
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
                // For messages popped back from a retry topic, the broker has already written POP_CK; it must not be overwritten.
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

        // Only fill in when missing; do not overwrite an existing value (the first pop time brought along the retry chain).
        foreach ($found as $m) {
            if (!array_key_exists(MessageConst::PROPERTY_FIRST_POP_TIME, $m->properties)) {
                $m->putProperty(MessageConst::PROPERTY_FIRST_POP_TIME, (string) $popTime);
            }
        }
    }

    /** Resolve broker addresses by topic (optionally further by brokerName). */
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
     * Confirm one POP message (RequestCode.ACK_MESSAGE = 200051). Returns the broker response code.
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
            $addr = $this->popAckAddrFor($brokerName, $topic, $queueId);
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
     * Confirm a whole POP batch in one round trip (BATCH_ACK_MESSAGE = 200151).
     *
     * Every checkpoint that shares retry flag + queueId + checkpoint offset + pop time collapses into
     * one entry whose bit set marks ``queueOffset - ckQueueOffset``, so a batch costs a single request
     * instead of one ACK_MESSAGE per message. The request carries **no custom header**: the broker needs
     * nothing beyond the JSON body. The body's broker name -- and therefore the target address -- comes
     * from the first extra info, so a batch is single-broker by construction; checkpoints of another
     * broker must be acknowledged by a separate call. Returns the broker response code.
     */
    public function batchAckMessage(
        string $consumerGroup,
        string $topic,
        array $extraInfoList,
        int $timeoutMillis = 3000,
        ?string $addr = null,
    ): int {
        if ($extraInfoList === []) {
            throw new MQClientException('batch ack needs at least one extra info');
        }
        $body = BatchAckMessageRequestBody::fromExtraInfos($consumerGroup, $topic, $extraInfoList);
        if ($addr === null) {
            $first = ExtraInfoUtil::split($extraInfoList[0]);
            $addr = $this->popAckAddrFor(
                ExtraInfoUtil::getBrokerName($first),
                $topic,
                ExtraInfoUtil::getQueueId($first),
            );
        }
        $request = RemotingCommand::createRequestCommand(RequestCode::BATCH_ACK_MESSAGE, null);
        $request->body = $body->encode();
        $response = $this->invokeSync($addr, $request, $timeoutMillis);
        return $response->code;
    }

    /**
     * Extend a POP message's invisible time (CHANGE_MESSAGE_INVISIBLETIME = 200053).
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
            $addr = $this->popAckAddrFor($brokerName, $topic, $queueId);
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

    // ---------------- Offset query/update ----------------

    /**
     * setZeroIfNotFound defaults to false: the client never sets this field when querying
     * offsets, so a new consume group returns QUERY_NOT_FOUND (null) rather than 0.
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
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
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
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
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
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
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
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
        $this->checkResponse($response);
        $respHeader = new GetMinOffsetResponseHeader();
        $respHeader->fromExtFields($response->extFields);
        return $respHeader->offset ?? 0;
    }

    /**
     * boundaryType defaults to LOWER (take the lower bound when finding an offset by timestamp);
     * passing null omits the boundaryType field.
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
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
        $this->checkResponse($response);
        $respHeader = new SearchOffsetResponseHeader();
        $respHeader->fromExtFields($response->extFields);
        return $respHeader->offset ?? 0;
    }

    /**
     * Query messages by key (QUERY_MESSAGE, via the broker index).
     *
     * When uniqKey=true, additionally send extFields["_UNIQUE_KEY_QUERY"]="true".
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
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
        if ($response->code === ResponseCode::QUERY_NOT_FOUND) {
            return null;
        }
        $this->checkResponse($response);
        return $response->body;
    }

    /**
     * Query messages across all brokers of this topic, merged and deduplicated by key.
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

    // ---------------- Queue lock (orderly consumption) ----------------

    /**
     * Lock a batch of queues (RequestCode.LOCK_BATCH_MQ).
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
            // Master only, no route refresh; skip this broker entirely when unavailable.
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
                $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
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
     * Unlock a batch of queues (RequestCode.UNLOCK_BATCH_MQ).
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
                $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
                $this->checkResponse($response);
            } catch (\Throwable $e) {
                Logger::warning(sprintf('unlock_batch_mq failed for broker %s: %s', $brokerName, $e->getMessage()));
            }
        }
    }

    // ---------------- Heartbeat / unregister ----------------

    public function sendHeartbeat(string $addr, HeartbeatData $heartbeatData, ?int $timeoutMillis = null): void
    {
        $request = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT, null);
        $request->body = $heartbeatData->encode();
        $response = $this->invokeSync($addr, $request, $timeoutMillis ?? $this->mqClientApiTimeout);
        $this->checkResponse($response);
    }

    /**
     * One broker over the fingerprint path -- the same HEART_BEAT(34) frame, read through the two
     * response ext fields. There is no separate request code for it.
     *
     * A broker already known to speak the protocol **and** still holding our fingerprint gets the
     * subscription-free body; if it then answers IS_SUB_CHANGE (its per-group table moved under
     * our feet, e.g. another instance of the same group registered different subscriptions) the
     * fingerprint is forgotten, so the next round re-sends everything. Anything else sends the full
     * body, and only an IS_SUPPORT_HEART_BEAT_V2 answer earns the address a place in the tables --
     * a broker that never heard of the fields keeps receiving the subscriptions, untouched.
     */
    public function sendHeartbeatV2(
        string $addr,
        HeartbeatData $withSub,
        HeartbeatData $withoutSub,
        int $fingerprint,
        ?int $timeoutMillis = null,
    ): HeartbeatV2Result {
        $simple = isset($this->supportV2BrokerAddrs[$addr])
            && ($this->brokerAddrHeartbeatFingerprint[$addr] ?? null) === $fingerprint;
        $request = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT, null);
        $request->body = ($simple ? $withoutSub : $withSub)->encode();
        $response = $this->invokeSync($addr, $request, $timeoutMillis ?? $this->mqClientApiTimeout);
        $this->checkResponse($response);
        $result = HeartbeatV2Result::fromResponse($response->version, $response->extFields);
        if ($simple) {
            if ($result->subChange) {
                unset($this->brokerAddrHeartbeatFingerprint[$addr]);
            }
        } elseif ($result->supportV2) {
            $this->supportV2BrokerAddrs[$addr] = true;
            if ($result->subChange) {
                unset($this->brokerAddrHeartbeatFingerprint[$addr]);
            } else {
                $this->brokerAddrHeartbeatFingerprint[$addr] = $fingerprint;
            }
        }
        Logger::debug(sprintf(
            'heartbeatV2 %s addr=%s supportV2=%s subChange=%s',
            $simple ? 'simple' : 'normal',
            $addr,
            $result->supportV2 ? 'true' : 'false',
            $result->subChange ? 'true' : 'false',
        ));
        return $result;
    }

    /**
     * Forget every remembered fingerprint, so the next round to each broker re-delivers the
     * subscriptions. The "this broker speaks the protocol" knowledge survives: it is a property of
     * the broker, not of this round.
     */
    public function resetBrokerAddrHeartbeatFingerprint(): void
    {
        $this->brokerAddrHeartbeatFingerprint = [];
    }

    /** Addresses this instance is known to have registered with over the fingerprint path. */
    public function supportV2BrokerAddrsForTest(): array
    {
        return array_keys($this->supportV2BrokerAddrs);
    }

    /** Remembered broker address => fingerprint table, for tests and diagnostics. */
    public function heartbeatFingerprintTable(): array
    {
        return $this->brokerAddrHeartbeatFingerprint;
    }

    /** UNREGISTER_CLIENT(35): detach this clientId from this broker. */
    public function unregisterClient(
        string $addr,
        string $clientId,
        ?string $producerGroup,
        ?string $consumerGroup,
        ?int $timeoutMillis = null,
    ): void {
        $header = new UnregisterClientRequestHeader();
        $header->clientId = $clientId;
        // An empty string would be treated by the broker as "this group exists" and trigger a
        // no-op unregister round, so blank values are handled as "this group is absent".
        $pg = trim((string) $producerGroup);
        $header->producerGroup = $pg !== '' ? $pg : null;
        $cg = trim((string) $consumerGroup);
        $header->consumerGroup = $cg !== '' ? $cg : null;
        $request = RemotingCommand::createRequestCommand(RequestCode::UNREGISTER_CLIENT, $header);
        $response = $this->invokeSync($addr, $request, $timeoutMillis ?? $this->mqClientApiTimeout);
        $this->checkResponse($response);
    }

    /**
     * CHECK_CLIENT_CONFIG(46): have the broker validate a subscription expression.
     */
    public function checkClientConfig(
        string $brokerAddr,
        string $consumerGroup,
        string $clientId,
        SubscriptionData $subscriptionData,
        ?int $timeoutMillis = null,
    ): void {
        $request = RemotingCommand::createRequestCommand(RequestCode::CHECK_CLIENT_CONFIG, null);
        $body = new CheckClientRequestBody();
        $body->clientId = $clientId;
        $body->group = $consumerGroup;
        $body->subscriptionData = $subscriptionData;
        $request->body = $body->encode();
        $response = $this->invokeSync($this->vipAddrFor($brokerAddr), $request, $timeoutMillis ?? $this->mqClientApiTimeout);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQClientException($response->remark ?? '', $response->code);
        }
    }

    // ---------------- Admin APIs ----------------

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
        // On the wire, attributes is sent as an empty string "" when it has no content,
        // rather than defaulting the field away.
        $header->attributes = $attributes ?? '';
        $header->force = false;
        $request = RemotingCommand::createRequestCommand(RequestCode::UPDATE_AND_CREATE_TOPIC, $header);

        $attempts = max(1, $retryTimes);
        $lastExc = null;
        for ($attempt = 0; $attempt < $attempts; $attempt++) {
            try {
                $response = $this->invokeSync($this->vipAddrFor($brokerAddr), $request, $timeoutMillis);
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
        $response = $this->invokeSync($this->vipAddrFor($brokerAddr), $request, $timeoutMillis);
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
        $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis);
        $this->checkResponse($response);
        if ($response->body !== null && $response->body !== '') {
            return GetConsumerListByGroupResponseBody::decode($response->body);
        }
        return new GetConsumerListByGroupResponseBody();
    }

    // ---------------- Tools ----------------

    /**
     * Address policy for offset reads: master only, then refresh the route once, then relax the
     * lookup (may fall to a slave node; offsets are the same HA copy anyway).
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

    /** Master preferred; fall back to one slave node when there is no master. */
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

    /** Offset query policy: master only, error out when the master is gone. */
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
     * Public exit of ``publishAddrInAdmin``: reused by pull-mode consumers for
     * earliestMsgStoreTime (purely additive public wrapper, logic stays as-is).
     */
    public function adminAddrFor(MessageQueue $mq): string
    {
        return $this->publishAddrInAdmin($mq);
    }

    /** Find this brokerName's address in topic_route_table (master preferred, any node when no master). */
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

    /** Take **only** the brokerId=0 address from brokerAddrTable; return null when absent. */
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
     * Every broker address seen in the routes (master + slave), not deduplicated to
     * "one node per brokerName".
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
     * Query all clientIds within a consumer group (GET_CONSUMER_LIST_BY_GROUP).
     *
     * @return list<string>|null
     */
    public function getConsumerIdListByGroup(string $topic, string $consumerGroup, ?int $timeoutMillis = null): ?array
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
            $response = $this->invokeSync($this->vipAddrFor($addr), $request, $timeoutMillis ?? $this->mqClientApiTimeout);
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

    /** Unregister this clientId from all known brokers (master + slave), sending
     * UNREGISTER_CLIENT one by one and only logging failures. */
    public function unregisterClientAllBrokers(
        string $clientId,
        ?string $producerGroup,
        ?string $consumerGroup,
        ?int $timeoutMillis = null,
    ): void {
        foreach ($this->getAllBrokerAddrs() as $addr) {
            try {
                $this->unregisterClient($addr, $clientId, $producerGroup, $consumerGroup, $timeoutMillis);
            } catch (\Throwable $e) {
                Logger::debug(sprintf('unregister_client failed, addr=%s: %s', $addr, $e->getMessage()));
            }
        }
    }

    // ---------------- In-use topic registration ----------------

    /** Register a topic whose route must be refreshed periodically in the background
     * (both subscribed and published topics count). */
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
