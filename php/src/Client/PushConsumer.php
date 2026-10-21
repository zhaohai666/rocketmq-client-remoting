<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\FilterAPI;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageAccessor;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PullSysFlag;
use RocketMQ\Common\SubscriptionData;
use RocketMQ\Common\UtilAll;
use RocketMQ\Remoting\Protocol\CMResult;
use RocketMQ\Remoting\Protocol\ConsumerRunningInfo;
use RocketMQ\Remoting\Protocol\ConsumeMessageDirectlyResult;
use RocketMQ\Remoting\Protocol\ProcessQueueInfo;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\ConsumerSendMsgBackRequestHeader;
use RocketMQ\Remoting\SocksProxyConfig;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\ConsumeType;
use RocketMQ\Remoting\Protocol\MessageModel;
use RocketMQ\Remoting\Protocol\MessageRequestMode;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\NamespaceUtil;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\ConsumerData;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\RemotingTimeoutException;
// consumerRunningInfo (307) uses message_queue_key to build the mqTable/mqPopTable keys.
// Unprefixed PHP function calls look in the **current namespace** first; omitting this line
// is not "inconsistent style" but an Uncaught Error at runtime - the whole running info
// becomes unavailable, and the console/diagnostics chain goes blind.
use function RocketMQ\Remoting\Protocol\message_queue_key;

// The module-level helper functions used by this file are autoloaded together with the
// ConsumerSupport.php classes; to guard against the "function called without touching the
// class" ordering, the file is explicitly ensured loaded here.
if (!function_exists(__NAMESPACE__ . '\\client_side_tag_filter')) {
    require_once __DIR__ . '/ConsumerSupport.php';
}

/**
 * Push-mode consumer (the push consumption side of the classic remoting protocol).
 *
 * ## PHP single-thread adaptation (intentional deviation, read before changing)
 *
 * There are 6 kinds of background tasks (heartbeat / rebalance / per-queue pull / dispatch /
 * offset persistence / orderly lock) plus POP popping; PHP has no threads, so all converge
 * into the caller-driven ``tick()``:
 *
 * - **Heartbeat / rebalance / offset persistence / orderly lock / expiry sweep**: changed to
 *   due-time checks inside tick
 *   (periods 30s / 20s / 10s+persistConsumerOffsetInterval / 20s / consumeTimeout minutes);
 * - **Pull loop**: each queue had its own long-polling loop (suspend=true) running
 *   independently in the multi-threaded model; under PHP single-thread, long polling would
 *   starve the other queues, so it becomes **short polling** (suspend=false) plus idle
 *   backoff ``pullIntervalMillis`` on empty results (1000ms by default). Semantic cost:
 *   offset correction of empty queues (correctTagsOffset) goes from ~once per 15s to ~once
 *   per 1s, only more frequent never lazier; throughput cost: at most
 *   ``maxPullsPerQueuePerTick`` short-poll rounds per queue per single tick;
 * - **Consumption**: dispatch means synchronous consumption; PHP dispatches synchronously
 *   inside tick as well, and orderly consumption's "suspend retry" changes from sleep to the
 *   ``suspendedUntil`` table (the queue is skipped until tick reaches the due time);
 * - **POP**: same as above, pollTime=0 short polling plus idle backoff; consume batches run
 *   inline (no ConsumeExecutor is built, as "real concurrency == core" is meaningless under a
 *   single thread, while the declared value stays observable).
 */
final class DefaultMQPushConsumer
{
    // ---------------- POP mode (5.x lite consumption) configuration ----------------

    public string $consumerGroup;
    public ?bool $tlsEnable = null;
    /** TLS details (caCert/clientCert/clientKey/serverName), semantics in RemotingClient::$tlsOptions. */
    public ?array $tlsOptions = null;
    public ?SocksProxyConfig $socksProxy = null;
    public string $namespace = '';
    public string $instanceName = MixAll::DEFAULT_INSTANCE_NAME;
    public ?string $clientId = null;
    /** unitName/unitMode/enableStreamRequestType: unitization and streaming request switches; the former takes part in clientId construction, and unitMode enters heartbeat and subscription filtering. */
    public ?string $unitName = null;
    public bool $unitMode = false;
    public bool $enableStreamRequestType = false;
    /**
     * 5.x new namespace (namespaceV2): when non-empty, **every** request carries
     * the two extension headers `nsd=true` / `ns=<value>`, which the broker resolves to the
     * corresponding serverless instance. It is a separate mechanism from the `namespace`
     * above (client-side `namespace%` prefix for topic/group), and here **no** resource name
     * is modified. See NamespaceRpcHook.
     */
    public string $namespaceV2 = '';
    /**
     * Heartbeat fingerprint path (default false). On, this facade's HEART_BEAT(34) rounds carry a
     * non-zero heartbeatFingerprint and, once a broker answers IS_SUPPORT_HEART_BEAT_V2 and the
     * fingerprint is unchanged, the subscription set drops out of the body.
     */
    public bool $useHeartbeatV2 = false;
    /** Decode-read-body switch (default true): off leaves received message bodies unmaterialised. */
    public bool $decodeReadBody = true;
    /** Fallback timeout of internal client API calls; explicit call timeouts still win. */
    public int $mqClientApiTimeout = 3000;
    /** Concurrent heartbeat dispatch (default false); the fingerprint path wins when both are on. */
    public bool $enableConcurrentHeartbeat = false;
    /** Worker count of the instance's concurrent heartbeat pool, default = available processors. */
    public int $concurrentHeartbeatThreadPoolSize;
    /**
     * VIP channel (default false; the broker only opens the `listen port - 2` channel when started
     * with it). On, the offset / queue-lock / consumer-list / send-back requests this facade makes go
     * to that port, while pulls, heartbeats and unregistration stay on the published one.
     */
    public bool $vipChannelEnabled = false;
    /** @var 'CLUSTERING'|'BROADCASTING' */
    public string $messageModel = MessageModel::CLUSTERING;
    public string $consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
    /** Start point of CONSUME_FROM_TIMESTAMP (format yyyyMMddHHmmss), 30 minutes ago by default. */
    public string $consumeTimestamp;

    // ---- Consume concurrency (declared value, min=max=20 by default; under the single-thread model it is for observation and runtime adjustment only) ----
    public int $consumeThreadMin = 20;
    public int $consumeThreadMax = 20;
    public int $adjustThreadPoolNumsThreshold = 100000;
    private int $corePoolSize;
    /** msgAccCnt (by queue key): the "backlog count" computed by the last pull (queue max offset minus the queueOffset of the batch's last message). */
    /** @var array<string,int> */
    private array $msgAccCntTable = [];
    public int $consumeConcurrentlyMaxSpan = 2000;
    public int $pullThresholdForQueue = 1000;
    public int $pullThresholdSizeForQueue = 100;
    public int $pullThresholdForTopic = -1;
    public int $pullThresholdSizeForTopic = -1;
    public int $pullInterval = 0;
    /**
     * Pull RPC timeout. WARNING: under long-poll semantics it should be 30000 (suspend budget);
     * under PHP short polling the broker returns immediately, so the default is lowered to
     * 5000 - a stuck connection need not wait 30s.
     */
    public int $pullTimeoutMillis = 5000;
    public int $pullIntervalMillis = 1000;
    /** Max short-poll rounds per queue within a single tick (new in the PHP adaptation; throughput knob). */
    public int $maxPullsPerQueuePerTick = 8;
    /** Suspend duration of long polling (reserved field: passed through with the request header; the broker ignores it under PHP short polling). */
    public int $pullSuspendTimeoutMillis = 20000;
    public int $consumeMessageBatchMaxSize = 1;
    public int $pullBatchSize = 32;
    public int $pullBatchSizeInBytes = 256 * 1024;
    public bool $postSubscriptionWhenPull = false;
    public int $maxReconsumeTimes = -1;
    public int $suspendCurrentQueueTimeMillis = 1000;
    public int $consumeTimeout = 15;
    public bool $clientRebalance = true;
    /** @var list<string> */
    public array $nameServerAddrs = [];
    public mixed $rpcHook = null;
    public AllocateMessageQueueStrategy $allocateStrategy;
    /** @var array<string, SubscriptionData> topic => SubscriptionData */
    public array $subscriptionData = [];
    public mixed $messageListener = null;

    public ?MQClientInstance $mqClient = null;
    private bool $started = false;
    /** @var array<string,int> */
    private array $msgQueueInflight = [];
    /** @var array<string,list<MessageExt>> */
    private array $inflightMsgs = [];
    /** @var array<string,int> Pull cursor (nextBeginOffset) */
    private array $offsetTable = [];
    private array $pending = [];
    /** @var array<string,MessageQueue> Assigned queue registry */
    private array $mqMap = [];
    /** @var array<string,int> Consumed offset (the one persisted to the broker) */
    private array $consumeOffsets = [];
    /** @var array<string,bool> Offsets frozen by OFFSET_ILLEGAL correction */
    private array $frozenOffsets = [];
    /** @var array<string,int> Per-queue ProcessQueue epoch (+1 on revocation) */
    private array $queueEpoch = [];
    /** @var array<string,bool> Queues confirmed locked by broker LOCK_BATCH_MQ (orderly consumption) */
    private array $lockOk = [];
    private int $flowControlTriggered = 0;
    /** @var array<string,float> Last time a pull/pop was initiated per queue (seconds) */
    private array $lastPullTable = [];
    private bool $paused = false;
    /** @var array<string,int> MessageQueue key -> brokerId to pick for the next pull round */
    private array $pullFromWhichNode = [];
    /** @var array<string,MessageQueue> Queue set currently assigned to this instance (key => mq) */
    private array $assigned = [];
    private bool $rebalanceNow = false;
    /** @var array<string,float> Due time of orderly consumption suspend-retry (seconds, PHP adaptation) */
    private array $suspendedUntil = [];
    private float $startTime = 0.0;

    public bool $heartbeatEnabled = true;
    public int $heartbeatIntervalMillis = 30000;
    /** Route refresh period (passed through only once when start() builds the MQClientInstance). */
    public int $pollNameServerInterval = 30000;
    public int $persistConsumerOffsetInterval = 5000;
    /**
     * awaitTerminationMillisWhenShutdown: how many milliseconds shutdown() may spend handing the
     * batches still waiting in the per-queue buffers to the listener. ``<= 0`` (default) puts no
     * extra ceiling on that drain -- it runs until the buffers are empty or the round cap below
     * is reached; a positive value stops it once the budget is spent (what is left stays queued
     * for the broker's redelivery, exactly as a consume pool that refused to terminate in time).
     */
    public int $awaitTerminationMillisWhenShutdown = 0;
    /**
     * Batches a single tick hands to the listener (PHP adaptation of the consume pool's throughput
     * limit; also the chunk size the shutdown drain walks, so a time ceiling can bind mid-backlog).
     */
    public int $dispatchBatchesPerTick = 1000;
    /** Round cap of the shutdown drain: keeps a listener that never accepts its batch from hanging shutdown. */
    private const SHUTDOWN_DRAIN_MAX_ROUNDS = 64;
    private const SHUTDOWN_DRAIN_CHUNK_BATCHES = 16;
    private int $heartbeatCount = 0;

    // ---- POP mode ----
    public bool $popMode = false;
    public int $popInvisibleTime = 60000;
    public int $popBatchNums = 32;
    public int $popThresholdForQueue = 96;
    /** @var list<int> */
    public array $popDelayLevel = ConsumerDefaults::POP_DELAY_LEVEL;
    /** POP long-poll suspend duration (0 passed under PHP short polling). */
    public int $popPollTimeMillis = 15000;
    public int $popTimeoutMillis = 5000;
    public int $popIntervalMillis = 1000;
    public int $maxPopsPerQueuePerTick = 8;
    public int $popAssignmentIntervalMillis = MQClientInstance::POP_ASSIGNMENT_INTERVAL_MILLIS;
    /** The assignment poller registered with the instance (non-null only on the broker-side assignment path; the driving object of tick stage 7). */
    private ?PopAssignmentPoller $popAssignmentPoller = null;
    /** @var array<string,PopProcessQueue> */
    private array $popQueues = [];
    /** @var array<string,bool> POP orderly request dedup set (key = mqKey) */
    private array $popOrderlyRequests = [];

    // ---- Message trace ----
    public bool $enableTrace = false;
    public ?string $traceTopic = null;
    public int $traceMsgBatchNum = 10;
    /** @var list<object> */
    public array $consumeMessageHookList = [];
    /** @var list<object> */
    public array $filterMessageHookList = [];
    public ?AsyncTraceDispatcher $traceDispatcher = null;

    private ?ConsumerStatsManager $statsManager = null;

    // ---- tick timers (PHP adaptation: due times of the original background threads, seconds) ----
    private float $nextHeartbeatAt = 0.0;
    private float $nextRebalanceAt = 0.0;
    private float $nextPersistAt = 0.0;
    private float $nextLockAt = 0.0;
    private float $nextCleanAt = 0.0;
    /** PHP adaptation: next pullable time of empty-result queues (key => seconds) */
    private array $idleUntil = [];

    public function __construct(
        string $consumerGroup = MixAll::DEFAULT_CONSUMER_GROUP,
        mixed $rpcHook = null,
        string $namespace = '',
        string $messageModel = MessageModel::CLUSTERING,
    ) {
        if (trim($consumerGroup) === '') {
            throw new MQClientException('consumerGroup is empty');
        }
        $this->consumerGroup = $consumerGroup;
        $this->rpcHook = $rpcHook;
        $this->namespace = $namespace;
        $this->messageModel = $messageModel;
        $this->concurrentHeartbeatThreadPoolSize = MQClientInstance::availableProcessors();
        $this->corePoolSize = $this->consumeThreadMin;
        $this->consumeTimestamp = date('YmdHis', time() - 30 * 60);
        $this->allocateStrategy = new AllocateMessageQueueAveragely();
    }

    // ---------------- Configuration ----------------

    public function setNamesrvAddr(string $addr): void
    {
        $this->nameServerAddrs = array_values(array_filter(array_map('trim', explode(';', $addr))));
    }

    /** @param list<string> $addrs */
    public function setNameServerAddresses(array $addrs): void
    {
        $this->nameServerAddrs = array_values($addrs);
    }

    public function setInstanceName(string $name): void
    {
        $this->instanceName = $name;
    }

    public function setUnitName(?string $unitName): void
    {
        $this->unitName = $unitName;
    }

    /**
     * Turn the VIP channel on or off, and push the change into the live client instance so the next
     * request already carries the rewritten port.
     */
    public function setVipChannelEnabled(bool $enable): void
    {
        $this->vipChannelEnabled = $enable;
        if ($this->mqClient !== null) {
            $this->mqClient->vipChannelEnabled = $enable;
        }
    }

    public function isVipChannelEnabled(): bool
    {
        return $this->vipChannelEnabled;
    }

    /** Gate body materialisation of received pull/pop results; the live instance follows immediately. */
    public function setDecodeReadBody(bool $decodeReadBody): void
    {
        $this->decodeReadBody = $decodeReadBody;
        if ($this->mqClient !== null) {
            $this->mqClient->setDecodeReadBody($decodeReadBody);
        }
    }

    /** Fallback timeout of the internal client API calls; explicit per-call timeouts still win. */
    public function setMqClientApiTimeout(int $timeoutMillis): void
    {
        $this->mqClientApiTimeout = $timeoutMillis;
        if ($this->mqClient !== null) {
            $this->mqClient->setMqClientApiTimeout($timeoutMillis);
        }
    }

    public function setEnableConcurrentHeartbeat(bool $enable): void
    {
        $this->enableConcurrentHeartbeat = $enable;
        if ($this->mqClient !== null) {
            $this->mqClient->setEnableConcurrentHeartbeat($enable);
        }
    }

    public function setConcurrentHeartbeatThreadPoolSize(int $size): void
    {
        $this->concurrentHeartbeatThreadPoolSize = $size > 0
            ? $size
            : MQClientInstance::availableProcessors();
        if ($this->mqClient !== null) {
            $this->mqClient->setConcurrentHeartbeatThreadPoolSize($this->concurrentHeartbeatThreadPoolSize);
        }
    }

    public function setUnitMode(bool $unitMode): void
    {
        $this->unitMode = $unitMode;
    }

    public function setNamespaceV2(string $namespaceV2): void
    {
        $this->namespaceV2 = $namespaceV2;
        if ($this->mqClient !== null) {
            $this->mqClient->namespaceV2 = $namespaceV2;
        }
    }

    public function getNamespaceV2(): string
    {
        return $this->namespaceV2;
    }

    public function setSocksProxy(?SocksProxyConfig $proxy): void
    {
        $this->socksProxy = $proxy;
    }

    public function getSocksProxy(): ?SocksProxyConfig
    {
        return $this->socksProxy;
    }

    public function setMessageModel(string $model): void
    {
        $this->messageModel = $model;
    }

    public function setConsumeFromWhere(string $where): void
    {
        $this->consumeFromWhere = $where;
    }

    public function setConsumeTimestamp(string $timestamp): void
    {
        $this->consumeTimestamp = $timestamp;
    }

    public function setConsumeThreadNums(int $n): void
    {
        $n = max(1, $n);
        $this->consumeThreadMin = $n;
        $this->consumeThreadMax = $n;
        $this->corePoolSize = $n;
    }

    public function setConsumeThreadMin(int $n): void
    {
        $this->consumeThreadMin = max(1, $n);
        $this->corePoolSize = $this->consumeThreadMin;
    }

    public function setConsumeThreadMax(int $n): void
    {
        $this->consumeThreadMax = max(1, $n);
    }

    public function setMessageListener(mixed $listener): void
    {
        $this->messageListener = $listener;
    }

    /** Ceiling in milliseconds that shutdown() spends draining queued consumption; <= 0 = no extra ceiling. */
    public function setAwaitTerminationMillisWhenShutdown(int $millis): void
    {
        $this->awaitTerminationMillisWhenShutdown = $millis;
    }

    public function getAwaitTerminationMillisWhenShutdown(): int
    {
        return $this->awaitTerminationMillisWhenShutdown;
    }

    public function setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy $strategy): void
    {
        $this->allocateStrategy = $strategy;
    }

    public function setEnableMsgTrace(bool $enable): void
    {
        $this->enableTrace = $enable;
    }

    public function setTraceTopic(?string $topic): void
    {
        $this->traceTopic = $topic;
    }

    public function registerConsumeMessageHook(object $hook): void
    {
        $this->consumeMessageHookList[] = $hook;
    }

    public function registerFilterMessageHook(object $hook): void
    {
        $this->filterMessageHookList[] = $hook;
    }

    public function getConsumerGroup(): string
    {
        return $this->consumerGroup;
    }

    public function subscriptions(): array
    {
        return array_values($this->subscriptionData);
    }

    // ---------------- Subscription ----------------

    public function subscribe(string $topic, string $subExpression = '*'): void
    {
        $topic = $this->withNamespace($topic);
        $sub = FilterAPI::buildSubscriptionData($topic, $subExpression);
        $this->subscriptionData[$topic] = $sub;
        $this->notifySubscriptionChanged($topic);
    }

    public function subscribeWithSelector(string $topic, MessageSelector $selector): void
    {
        $topic = $this->withNamespace($topic);
        if ($selector->type === \RocketMQ\Common\ExpressionType::TAG) {
            $sub = FilterAPI::buildSubscriptionData($topic, $selector->expression);
        } else {
            if ($selector->expression === '') {
                throw new \ValueError("Expression can't be null! " . $selector->type);
            }
            $sub = new SubscriptionData($topic, $selector->expression);
            $sub->setExpressionType($selector->type);
        }
        $this->subscriptionData[$topic] = $sub;
        $this->notifySubscriptionChanged($topic);
    }

    public function unsubscribe(string $topic): void
    {
        unset($this->subscriptionData[$this->withNamespace($topic)]);
    }

    private function notifySubscriptionChanged(string $topic): void
    {
        if (!$this->started || $this->mqClient === null) {
            return;
        }
        $this->mqClient->registerTopicInUse($topic);
        try {
            $this->sendHeartbeatToAllBroker();
        } catch (\Throwable $e) {
            Logger::debug("immediate heartbeat after subscribe($topic) failed: " . $e->getMessage());
        }
    }

    private function withNamespace(string $topic): string
    {
        if ($this->namespace === '') {
            return $topic;
        }
        return NamespaceUtil::wrapNamespace($this->namespace, $topic);
    }

    // ---------------- Config value gates ----------------

    private function checkConfigRanges(): void
    {
        if ($this->consumeThreadMin < 1 || $this->consumeThreadMin > 1000) {
            throw new MQClientException('consumeThreadMin Out of range [1, 1000]');
        }
        if ($this->consumeThreadMax < 1 || $this->consumeThreadMax > 1000) {
            throw new MQClientException('consumeThreadMax Out of range [1, 1000]');
        }
        if ($this->consumeThreadMin > $this->consumeThreadMax) {
            throw new MQClientException(sprintf(
                'consumeThreadMin (%d) is larger than consumeThreadMax (%d)',
                $this->consumeThreadMin,
                $this->consumeThreadMax,
            ));
        }
        if ($this->consumeConcurrentlyMaxSpan < 1 || $this->consumeConcurrentlyMaxSpan > 65535) {
            throw new MQClientException('consumeConcurrentlyMaxSpan Out of range [1, 65535]');
        }
        if ($this->pullThresholdForQueue < 1 || $this->pullThresholdForQueue > 65535) {
            throw new MQClientException('pullThresholdForQueue Out of range [1, 65535]');
        }
        if ($this->pullThresholdForTopic !== -1
            && ($this->pullThresholdForTopic < 1 || $this->pullThresholdForTopic > 6553500)) {
            throw new MQClientException('pullThresholdForTopic Out of range [1, 6553500]');
        }
        if ($this->pullThresholdSizeForQueue < 1 || $this->pullThresholdSizeForQueue > 1024) {
            throw new MQClientException('pullThresholdSizeForQueue Out of range [1, 1024]');
        }
        if ($this->pullThresholdSizeForTopic !== -1
            && ($this->pullThresholdSizeForTopic < 1 || $this->pullThresholdSizeForTopic > 102400)) {
            throw new MQClientException('pullThresholdSizeForTopic Out of range [1, 102400]');
        }
        if ($this->pullInterval < 0 || $this->pullInterval > 65535) {
            throw new MQClientException('pullInterval Out of range [0, 65535]');
        }
        if ($this->consumeMessageBatchMaxSize < 1 || $this->consumeMessageBatchMaxSize > 1024) {
            throw new MQClientException('consumeMessageBatchMaxSize Out of range [1, 1024]');
        }
        if ($this->pullBatchSize < 1 || $this->pullBatchSize > 1024) {
            throw new MQClientException('pullBatchSize Out of range [1, 1024]');
        }
        if ($this->popInvisibleTime < ConsumerDefaults::MIN_POP_INVISIBLE_TIME
            || $this->popInvisibleTime > ConsumerDefaults::MAX_POP_INVISIBLE_TIME) {
            throw new MQClientException(sprintf(
                'popInvisibleTime Out of range [%d, %d]',
                ConsumerDefaults::MIN_POP_INVISIBLE_TIME,
                ConsumerDefaults::MAX_POP_INVISIBLE_TIME,
            ));
        }
        if ($this->popBatchNums <= 0 || $this->popBatchNums > 32) {
            throw new MQClientException('popBatchNums Out of range [1, 32]');
        }
    }

    /** Parse consumeTimestamp (unparseable input must fail hard). */
    public function consumeTimestampMillis(): int
    {
        $ts = $this->consumeTimestamp;
        $dt = \DateTime::createFromFormat('YmdHis', $ts);
        if ($dt === false || strlen($ts) !== 14) {
            throw new MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received $ts",
            );
        }
        $parsed = (int) $dt->format('YmdHis');
        if ($parsed !== (int) $ts) {
            throw new MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received $ts",
            );
        }
        return (int) $dt->format('U') * 1000 + (int) $dt->format('v');
    }

    // ---------------- Lifecycle ----------------

    public function start(): void
    {
        if ($this->started) {
            return;
        }
        if ($this->namespace !== '') {
            $this->consumerGroup = NamespaceUtil::wrapNamespace($this->namespace, $this->consumerGroup);
        }
        Validators::checkGroup($this->consumerGroup);
        if ($this->consumerGroup === MixAll::DEFAULT_CONSUMER_GROUP) {
            throw new MQClientException(sprintf(
                'consumerGroup can not equal %s, please specify another one.',
                MixAll::DEFAULT_CONSUMER_GROUP,
            ));
        }
        if ($this->nameServerAddrs === [] && !DefaultTopAddressing::isConfigured()) {
            throw new MQClientException('name server address is not set');
        }
        if ($this->subscriptionData === []) {
            throw new MQClientException('subscription is not set, call subscribe() first');
        }
        if ($this->messageListener === null) {
            throw new MQClientException('message listener is not set');
        }
        $this->consumeTimestampMillis();
        if (!isset($this->allocateStrategy)) {
            throw new MQClientException('allocateMessageQueueStrategy is null');
        }
        $this->checkConfigRanges();
        if ($this->messageModel === MessageModel::CLUSTERING) {
            $this->instanceName = MixAll::changeInstanceNameToPid($this->instanceName);
        }
        if ($this->clientId === null) {
            $this->clientId = MixAll::clientIdFor($this->instanceName, $this->unitName, $this->enableStreamRequestType);
        }
        $this->mqClient = new MQClientInstance(
            $this->clientId,
            $this->nameServerAddrs,
            tlsEnable: $this->tlsEnable,
            enableStreamRequestType: $this->enableStreamRequestType,
            namespaceV2: $this->namespaceV2,
            unitName: $this->unitName,
            pollNameServerInterval: $this->pollNameServerInterval,
            tlsOptions: $this->tlsOptions,
            socksProxy: $this->socksProxy,
            useHeartbeatV2: $this->useHeartbeatV2,
            vipChannelEnabled: $this->vipChannelEnabled,
            decodeReadBody: $this->decodeReadBody,
            mqClientApiTimeout: $this->mqClientApiTimeout,
            enableConcurrentHeartbeat: $this->enableConcurrentHeartbeat,
            concurrentHeartbeatThreadPoolSize: $this->concurrentHeartbeatThreadPoolSize,
        );
        if ($this->rpcHook !== null) {
            $this->mqClient->remotingClient->registerRpcHook($this->rpcHook);
        }
        $this->mqClient->start();
        // Dynamic name server backfill.
        if ($this->nameServerAddrs === [] && $this->mqClient->nameServerAddrs !== []) {
            $this->nameServerAddrs = array_values($this->mqClient->nameServerAddrs);
        }
        // Consume stats (shared at instance level).
        $this->statsManager = $this->mqClient->consumerStatsManager;
        $this->mqClient->registerConsumer($this->consumerGroup, $this);
        $this->started = true;
        $this->startTime = microtime(true);
        $this->nextHeartbeatAt = $this->startTime + $this->heartbeatIntervalMillis / 1000.0;
        $this->nextRebalanceAt = $this->startTime + 20.0;
        $this->nextPersistAt = $this->startTime + 10.0;
        $this->nextLockAt = $this->startTime;
        $this->nextCleanAt = $this->startTime + max(1, $this->consumeTimeout) * 60.0;
        if ($this->messageModel !== MessageModel::BROADCASTING) {
            $retryTopic = MixAll::getRetryTopic($this->consumerGroup);
            if (!isset($this->subscriptionData[$retryTopic])) {
                // Under SUB_ALL both tagsSet / codeSet are **empty** (not {"*"}).
                $this->subscriptionData[$retryTopic] = FilterAPI::buildSubscriptionData($retryTopic, '*');
            }
        }
        $this->refreshRoutes();
        try {
            $this->mqClient->checkClientInBroker();
        } catch (\Throwable $e) {
            $this->shutdown();
            throw $e;
        }
        try {
            $this->sendHeartbeatToAllBroker();
        } catch (\Throwable $e) {
            Logger::debug('initial heartbeat failed: ' . $e->getMessage());
        }
        // POP broker-side assignment: attach the poller first (the driving object of instance
        // tick stage 7), then run the first synchronous assignment round. Order matters -
        // doRebalance() on this path just "runs one QUERY_ASSIGNMENT round", and with no
        // poller registered nothing is asked at all. Other paths (default clientRebalance=true /
        // classic pull) have zero overhead.
        if ($this->usesBrokerPopAssignment()) {
            $this->registerPopAssignment();
        }
        // The first assignment round must complete synchronously: otherwise pulling spins
        // pointlessly on an empty assignment set until the first rebalance.
        try {
            $this->doRebalance();
        } catch (\Throwable $e) {
            Logger::warning('initial rebalance failed: ' . $e->getMessage());
        }
        // Trace dispatcher (the consume loop has no threads, so starting it last is fine).
        $this->startTraceDispatcher();
    }

    private function startTraceDispatcher(): void
    {
        if ($this->enableTrace) {
            try {
                $dispatcher = new AsyncTraceDispatcher(
                    $this->consumerGroup,
                    TraceDispatcherType::CONSUME,
                    $this->traceMsgBatchNum,
                    $this->traceTopic,
                    $this->rpcHook,
                );
                $dispatcher->setHostConsumer($this);
                $dispatcher->setNamespaceV2($this->namespaceV2);
                $this->traceDispatcher = $dispatcher;
                $this->registerConsumeMessageHook(new ConsumeMessageTraceHook($dispatcher));
            } catch (\Throwable $e) {
                Logger::error("system mqtrace hook init failed ,maybe can't send msg trace data: " . $e->getMessage());
            }
        }
        if ($this->traceDispatcher !== null) {
            try {
                $this->traceDispatcher->start(implode(';', $this->nameServerAddrs), AccessChannel::LOCAL);
            } catch (\Throwable $e) {
                Logger::warning('trace dispatcher start failed: ' . $e->getMessage());
            }
        }
    }

    public function shutdown(): void
    {
        if (!$this->started) {
            return;
        }
        // Consume-side drain comes first: it still needs the live client (offset advancement,
        // send-back) and must happen before anything is torn down.
        $this->drainQueuedMessagesOnShutdown();
        $this->started = false;
        if ($this->popAssignmentPoller !== null) {
            $this->mqClient?->unregisterPopAssignmentPoller($this->popAssignmentPoller->key);
            $this->popAssignmentPoller = null;
        }
        // POP: mark all queues as dropped; in-flight batches are no longer acked (left to broker revival redelivery).
        if ($this->popMode) {
            foreach ($this->popQueues as $pq) {
                $pq->setDropped(true);
            }
            $this->popQueues = [];
            $this->popOrderlyRequests = [];
        }
        try {
            $this->persistOffsetsOnce();
        } catch (\Throwable $e) {
            Logger::debug('persist offsets on shutdown failed: ' . $e->getMessage());
        }
        if ($this->isOrderly() && !$this->popMode
            && $this->messageModel !== MessageModel::BROADCASTING
            && $this->mqClient !== null) {
            try {
                $mqs = $this->assignedQueues();
                if ($mqs !== []) {
                    $this->mqClient->unlockBatchMq($this->consumerGroup, $this->clientId ?? '', $mqs);
                }
            } catch (\Throwable $e) {
                Logger::debug('unlock on shutdown failed: ' . $e->getMessage());
            }
        }
        // Graceful unregister: detach from each broker's ConsumerManager immediately, without waiting for heartbeat timeout.
        try {
            $this->requireClient()->unregisterClientAllBrokers(
                $this->clientId ?? '',
                '',
                $this->consumerGroup,
            );
        } catch (\Throwable $e) {
            Logger::debug('unregister on shutdown failed: ' . $e->getMessage());
        }
        foreach (array_keys($this->lastPullTable) as $k) {
            unset($this->lastPullTable[$k]);
        }
        if ($this->mqClient !== null) {
            try {
                $this->mqClient->shutdown();
            } catch (\Throwable) {
                // ignore
            }
        }
        // The trace dispatcher shuts down last (flush remaining traces).
        if ($this->traceDispatcher !== null) {
            try {
                $this->traceDispatcher->shutdown();
            } catch (\Throwable $e) {
                Logger::warning('trace dispatcher shutdown failed: ' . $e->getMessage());
            }
        }
    }

    private function requireClient(): MQClientInstance
    {
        if (!$this->started || $this->mqClient === null) {
            throw new MQClientException('consumer not started, call start() first');
        }
        return $this->mqClient;
    }

    /** @return bool */
    public function isStarted(): bool
    {
        return $this->started;
    }

    // ---------------- Heartbeat (consumer registration) ----------------

    /**
     * Pull the routes of subscribed topics once, filling the broker address table along the way
     * (the heartbeat relies on it).
     */
    private function refreshRoutes(): void
    {
        $client = $this->requireClient();
        foreach (array_keys($this->subscriptionData) as $topic) {
            $client->registerTopicInUse($topic);
            try {
                $client->getTopicPublishInfo($topic);
            } catch (MQClientException $e) {
                Logger::debug("refresh route for $topic failed: " . $e->getMessage());
            }
        }
    }

    /**
     * This consumer's slice of the registration payload. Public because the instance builds
     * its heartbeat from every group it owns; a consumer that drives its own beat sends the
     * very same payload, so the two paths cannot diverge.
     */
    public function buildHeartbeat(): HeartbeatData
    {
        $hb = new HeartbeatData($this->clientId ?? '');
        $cd = new ConsumerData(
            $this->consumerGroup,
            ConsumeType::CONSUME_PASSIVELY,
            $this->messageModel,
            $this->consumeFromWhere,
        );
        $cd->unitMode = $this->unitMode;
        foreach ($this->subscriptionData as $sub) {
            $cd->subscriptionDataSet[] = $sub;
        }
        $hb->consumerDataSet[] = $cd;
        return $hb;
    }

    /**
     * Send heartbeats to all known brokers (master + slave), returning the success count.
     *
     * Consumers **must** register with the broker: the broker's ConsumerManager learns which
     * clientIds are in a consumer group only upon receiving heartbeats, so rebalance's
     * GET_CONSUMER_LIST_BY_GROUP has anything to return. Skipping slaves is not "one fewer
     * redundant heartbeat": when a slave receives no heartbeat, it answers pulls directed at
     * itself with SUBSCRIPTION_NOT_EXIST (see the comment at the send-heartbeat-to-all-brokers site).
     */
    public function sendHeartbeatToAllBroker(): int
    {
        $client = $this->requireClient();
        $hb = $this->buildHeartbeat();
        $ok = 0;
        foreach ($client->getAllBrokerAddrs() as $addr) {
            try {
                $client->sendHeartbeat($addr, $hb);
                $ok++;
            } catch (\Throwable $e) {
                Logger::debug("heartbeat to $addr failed: " . $e->getMessage());
            }
        }
        if ($ok > 0) {
            $this->heartbeatCount++;
        }
        return $ok;
    }

    /** Number of successful heartbeat rounds (for real-machine verification). */
    public function heartbeatCount(): int
    {
        return $this->heartbeatCount;
    }

    /** Number of queues currently assigned to this instance (for real-machine multi-instance allocation verification). */
    public function assignedQueueCount(): int
    {
        return count($this->assigned);
    }

    /** @return list<string> */
    public function assignedQueueKeys(): array
    {
        $keys = [];
        foreach ($this->assigned as $key => $mq) {
            $keys[] = $key;
        }
        sort($keys);
        return $keys;
    }

    /** Number of queues assigned to this instance and belonging to the specified topic (counted by (topic,broker,queueId), for real-machine allocation verification). */
    public function assignedQueueCountForTopic(string $topic): int
    {
        $n = 0;
        foreach ($this->assigned as $mq) {
            if ($mq instanceof MessageQueue && $mq->topic === $topic) {
                $n++;
            }
        }
        return $n;
    }


    /** POP: number of in-flight PopProcessQueues. */
    public function popQueueCount(): int
    {
        return count($this->popQueues);
    }

    /** POP: total ACK debt (messages popped but not acked; should return to zero after normal consumption). */
    public function popWaitAckCount(): int
    {
        $n = 0;
        foreach ($this->popQueues as $pq) {
            $n += $pq->waitAckCount();
        }
        return $n;
    }

    /**
     * Number of local offset-table entries. POP's cursor lives in the broker's revive queue,
     * so the **client holds no offset table**, and this value should always be 0 in POP mode
     * (one entry per resolved queue in pull mode).
     */
    public function localOffsetCount(): int
    {
        return count($this->consumeOffsets);
    }

    // ---------------- tick driver (PHP adaptation core) ----------------

    /**
     * Drive one round: instance event pump + heartbeat / rebalance / pull + dispatch
     * consumption + offset persistence + orderly lock.
     * The caller calls this periodically in a loop (sleep 50ms is advised when idling).
     *
     * @param int|null $nowMillis Injectable current time (milliseconds); the internal clock is used when empty.
     */
    public function tick(?int $nowMillis = null): void
    {
        if (!$this->started) {
            return;
        }
        $nowMs = $nowMillis ?? UtilAll::currentTimeMillis();
        $now = $nowMs / 1000.0;
        $client = $this->mqClient;
        if ($client === null) {
            return;
        }
        // 1) Instance event pump (route refresh / stats sampling / pending actions).
        $client->tick($nowMs);

        // 2) Heartbeat.
        if ($this->heartbeatEnabled && $now >= $this->nextHeartbeatAt) {
            $this->nextHeartbeatAt = $now + $this->heartbeatIntervalMillis / 1000.0;
            try {
                $this->sendHeartbeatToAllBroker();
            } catch (\Throwable $e) {
                Logger::debug('heartbeat loop error: ' . $e->getMessage());
            }
        }

        // 3) Rebalance (fast retry during startup while there is no assignment).
        // The POP broker-side assignment path does not run here: not a single cell of the
        // local strategy should execute, and the rhythm is decided by stage 7 of the
        // instance tick (the QUERY_ASSIGNMENT round); see usesBrokerPopAssignment().
        if (!$this->usesBrokerPopAssignment()
            && ($this->rebalanceNow || $now >= $this->nextRebalanceAt)) {
            $startingUp = ($now - $this->startTime) < 60.0;
            $interval = ($startingUp && $this->assigned === []) ? 2.0 : 20.0;
            $this->nextRebalanceAt = $now + $interval;
            $this->rebalanceNow = false;
            try {
                $this->doRebalance();
            } catch (\Throwable $e) {
                Logger::debug('rebalance error: ' . $e->getMessage());
            }
        }

        // 4) Orderly lock (every 20s).
        if ($this->isOrderly() && !$this->popMode
            && $this->messageModel !== MessageModel::BROADCASTING
            && $now >= $this->nextLockAt) {
            $this->nextLockAt = $now + 20.0;
            $this->lockQueuesOnce();
        }

        // 5) Pull / POP round.
        if ($this->popMode) {
            $this->popRound($now);
        } else {
            $this->pullRound($now);
            // 6) Dispatch consumption (dispatch follows each pull round immediately).
            $this->dispatchRound();
            if (!$this->isOrderly() && $now >= $this->nextCleanAt) {
                $this->nextCleanAt = $now + max(1, $this->consumeTimeout) * 60.0;
                try {
                    $this->cleanExpiredMsgOnce();
                } catch (\Throwable $e) {
                    Logger::error('scheduleAtFixedRate cleanExpireMsg exception: ' . $e->getMessage());
                }
            }
        }

        // 8) Offset persistence.
        if ($now >= $this->nextPersistAt) {
            $this->nextPersistAt = $now + $this->persistConsumerOffsetInterval / 1000.0;
            try {
                $this->persistOffsetsOnce();
            } catch (\Throwable $e) {
                Logger::debug('persist offsets error: ' . $e->getMessage());
            }
        }
    }

    /** Alias of tick(). */
    public function pumpOnce(?int $nowMillis = null): void
    {
        $this->tick($nowMillis);
    }

    // ---------------- Rebalance ----------------

    private function rebalancePullState(): void
    {
        $current = [];
        foreach ($this->assigned as $key => $mq) {
            $current[$key] = $mq;
        }
        $retired = [];
        $pop = $this->popMode;
        foreach (array_keys($this->mqMap) as $key) {
            if (!isset($current[$key])) {
                $this->retireQueue($key, null, $retired);
                continue;
            }
            if ($this->started && $this->pullStalled($key)) {
                Logger::error("[BUG]doRebalance, $this->consumerGroup, try remove unnecessary mq, $key, "
                    . 'because pull is pause, so try to fixed it');
                $this->retireQueue($key, $current[$key], $retired);
            }
        }
        // Finishing must happen before 2)-building.
        if ($retired !== []) {
            $this->onQueuesRevoked($retired);
        }
        // 2)-build: prepare state for missing queues.
        foreach ($current as $key => $mq) {
            if (isset($this->mqMap[$key])) {
                continue;
            }
            if ($pop && !isset($this->popQueues[$key])) {
                $this->popQueues[$key] = new PopProcessQueue();
            }
            // A new ProcessQueue is in place, so unfreeze (rebuilt queues resume advancing from the corrected offset).
            unset($this->frozenOffsets[$key]);
            // The thread was just created and not stamped yet; occupy with the current time to
            // avoid a false stall verdict on the next round.
            $this->lastPullTable[$key] = microtime(true);
            // Once assigned, a queue enters _mq_map: offset persistence, 307 running info and 220 reset all rely on it.
            $this->mqMap[$key] = $mq;
        }
    }

    private function retireQueue(string $key, ?MessageQueue $fallbackMq, array &$retired): void
    {
        unset($this->pending[$key], $this->idleUntil[$key], $this->suspendedUntil[$key]);
        // In-flight registrations are voided together.
        unset($this->inflightMsgs[$key], $this->msgQueueInflight[$key]);
        unset($this->lockOk[$key]);
        // Epoch +1 invalidates all acks of in-flight batches; the freeze mark is kept until the queue is rebuilt.
        $this->queueEpoch[$key] = ($this->queueEpoch[$key] ?? 0) + 1;
        $off = $this->consumeOffsets[$key] ?? null;
        unset($this->consumeOffsets[$key], $this->offsetTable[$key], $this->lastPullTable[$key]);
        $mq = $this->mqMap[$key] ?? $fallbackMq;
        unset($this->mqMap[$key]);
        $pq = $this->popQueues[$key] ?? null;
        if ($pq !== null) {
            $pq->setDropped(true);
            unset($this->popQueues[$key]);
            unset($this->popOrderlyRequests[$key]);
        }
        if ($mq !== null) {
            $retired[] = [$mq, $off];
        }
    }

    private function pullStalled(string $key): bool
    {
        $began = $this->lastPullTable[$key] ?? null;
        if ($began === null) {
            return false;
        }
        return (microtime(true) - $began) > ConsumerDefaults::PULL_MAX_IDLE_TIME;
    }

    private function onQueuesRevoked(array $revoked): void
    {
        $broadcast = $this->messageModel === MessageModel::BROADCASTING;
        if ($broadcast) {
            // Broadcast-mode offsets are stored locally only. Revoked offsets must be written back to the file along with the queue info.
            $this->saveLocalOffsets($revoked);
            return;
        }
        $client = $this->mqClient;
        if ($client === null) {
            return;
        }
        foreach ($revoked as [$mq, $off]) {
            if ($off !== null) {
                try {
                    $client->updateConsumerOffset($this->consumerGroup, $mq, $off);
                } catch (\Throwable $e) {
                    Logger::debug("persist offset on revoke failed for $mq: " . $e->getMessage());
                }
            }
            // The single-queue unlock on queue revocation belongs to classic orderly only (it does not run in POP mode).
            if ($this->isOrderly() && !$this->popMode) {
                try {
                    $client->unlockBatchMq($this->consumerGroup, $this->clientId ?? '', [$mq]);
                } catch (\Throwable $e) {
                    Logger::debug("unlock on revoke failed for $mq: " . $e->getMessage());
                }
            }
        }
        Logger::info(sprintf('queues revoked, group=%s count=%d', $this->consumerGroup, count($revoked)));
    }

    private static function mqKey(MessageQueue $mq): string
    {
        return $mq->topic . $mq->brokerName . (string) $mq->queueId;
    }

    private function allQueuesOfTopic(string $topic): array
    {
        $client = $this->requireClient();
        $queues = $client->getTopicSubscribeInfo($topic);
        if ($queues === []) {
            Logger::debug("rebalance: no subscribe info for topic $topic");
        }
        return $queues;
    }

    public function doRebalance(): void
    {
        if ($this->usesBrokerPopAssignment()) {
            $this->runPopAssignmentRound();
            return;
        }
        $client = $this->requireClient();
        $was = [];
        foreach ($this->assigned as $key => $mq) {
            $was[$key] = true;
        }
        $assigned = [];
        $topics = array_keys($this->subscriptionData);
        if ($this->messageModel === MessageModel::BROADCASTING) {
            foreach ($topics as $topic) {
                foreach ($this->allQueuesOfTopic($topic) as $mq) {
                    $assigned[] = $mq;
                }
            }
        } else {
            foreach ($topics as $topic) {
                $mqAll = $this->allQueuesOfTopic($topic);
                usort($mqAll, static fn(MessageQueue $a, MessageQueue $b) =>
                    consumer_mq_sort_key($a) <=> consumer_mq_sort_key($b));
                if ($mqAll === []) {
                    continue;
                }
                $cidAll = $client->getConsumerIdListByGroup($topic, $this->consumerGroup);
                if ($cidAll === null || $cidAll === []) {
                    Logger::debug("rebalance: no consumer id list for $topic/$this->consumerGroup, keep current");
                    foreach ($this->assigned as $mq) {
                        if ($mq->topic === $topic) {
                            $assigned[] = $mq;
                        }
                    }
                    continue;
                }
                sort($cidAll);
                try {
                    $got = $this->allocateStrategy->allocate(
                        $this->consumerGroup,
                        $this->clientId ?? '',
                        $mqAll,
                        $cidAll,
                    );
                } catch (\Throwable $e) {
                    Logger::error('allocate message queue exception, strategy='
                        . $this->allocateStrategy->getName() . ': ' . $e->getMessage());
                    return;
                }
                foreach ($got as $mq) {
                    $assigned[] = $mq;
                }
            }
        }
        $newAssigned = [];
        foreach ($assigned as $mq) {
            $newAssigned[self::mqKey($mq)] = $mq;
        }
        $this->assigned = $newAssigned;
        $now = [];
        foreach ($newAssigned as $key => $mq) {
            $now[$key] = true;
        }
        if ($now != $was) {
            Logger::info(sprintf(
                'rebalance result changed, group=%s clientId=%s assigned=%d',
                $this->consumerGroup,
                $this->clientId,
                count($assigned),
            ));
        }
        foreach ($newAssigned as $key => $mq) {
            if (isset($was[$key])) {
                continue;
            }
            if (isset($this->offsetTable[$key])) {
                continue;
            }
            $sub = $this->subscriptionData[$mq->topic] ?? null;
            if ($sub === null) {
                continue;
            }
            try {
                $off = $this->resolveInitialOffset($client, $mq);
            } catch (\Throwable $e) {
                Logger::debug("resolve initial offset for $mq failed: " . $e->getMessage());
                continue;
            }
            if (!isset($this->offsetTable[$key])) {
                $this->offsetTable[$key] = $off;
            }
        }
        $this->rebalancePullState();
    }

    public function usesBrokerPopAssignment(): bool
    {
        return $this->popMode
            && !$this->clientRebalance
            && $this->messageModel !== MessageModel::BROADCASTING
            && !$this->isOrderly();
    }

    /**
     * Build and register the assignment poller ({@see MQClientInstance::registerPopAssignmentPoller()},
     * the driving object of tick stage 7). start() calls it once; repeated calls only overwrite
     * the old entry.
     */
    private function registerPopAssignment(): void
    {
        $client = $this->requireClient();
        $this->popAssignmentPoller = new PopAssignmentPoller(
            key: ($this->clientId ?? '') . '|' . $this->consumerGroup,
            consumerGroup: $this->consumerGroup,
            clientId: $this->clientId ?? '',
            strategyName: $this->allocateStrategy->getName(),
            messageModel: $this->messageModel,
            // The topic set is **queried fresh every round**: subscribe/unsubscribe during runtime changes it.
            topicsFn: fn (): array => $this->popAssignmentTopics(),
            applyFn: function (string $topic, array $assignments): void {
                $this->applyPopAssignment($topic, $assignments);
            },
            intervalMillis: $this->popAssignmentIntervalMillis,
        );
        $client->registerPopAssignmentPoller($this->popAssignmentPoller);
    }

    /** The topic set to ask the broker about this round (same source as the local rebalance's input: the subscription table). */
    public function popAssignmentTopics(): array
    {
        return array_keys($this->subscriptionData);
    }

    /**
     * Run one broker-side assignment round synchronously (start()'s first round and
     * rebalanceImmediately()'s immediate rebalance go through here; the periodic rhythm is
     * driven by instance tick stage 7).
     */
    public function runPopAssignmentRound(): void
    {
        $poller = $this->popAssignmentPoller;
        if ($poller === null) {
            return;
        }
        $this->requireClient()->runPopAssignmentRound($poller);
    }

    public function applyPopAssignment(string $topic, array $assignments): void
    {
        $was = [];
        foreach ($this->assigned as $key => $mq) {
            $was[$key] = true;
        }
        $newAssigned = [];
        foreach ($this->assigned as $key => $mq) {
            if ($mq->topic !== $topic) {
                $newAssigned[$key] = $mq;
            }
        }
        $popCnt = 0;
        $pullCnt = 0;
        foreach ($assignments as $one) {
            $mq = $one['mq'];
            if ($mq->topic !== $topic) {
                Logger::warning(sprintf(
                    'queryAssignment for %s/%s returned a queue of foreign topic %s, ignored',
                    $this->consumerGroup, $topic, (string) $mq,
                ));
                continue;
            }
            $newAssigned[self::mqKey($mq)] = $mq;
            if ($one['mode'] === MessageRequestMode::POP) {
                $popCnt++;
            } else {
                $pullCnt++;
            }
        }
        if ($popCnt > 0 && $pullCnt > 0) {
            Logger::warning(sprintf(
                'queryAssignment for %s/%s returned mixed modes (pop=%d pull=%d): the queue set is taken as-is, each queue is popped per this consumer\'s popMode',
                $this->consumerGroup, $topic, $popCnt, $pullCnt,
            ));
        }
        $this->assigned = $newAssigned;
        $now = [];
        foreach ($newAssigned as $key => $mq) {
            $now[$key] = true;
        }
        if ($now != $was) {
            Logger::info(sprintf(
                'broker rebalanced result changed. allocateMessageQueueStrategyName=%s, group=%s, topic=%s, clientId=%s, assignmentSet=%d',
                $this->allocateStrategy->getName(),
                $this->consumerGroup,
                $topic,
                (string) $this->clientId,
                count($assignments),
            ));
        }
        $this->rebalancePullState();
    }

    public function rebalanceImmediately(): void
    {
        if ($this->usesBrokerPopAssignment()) {
            $this->mqClient?->popAssignmentImmediately();
            return;
        }
        $this->rebalanceNow = true;
    }

    public function suspend(): void
    {
        $this->paused = true;
        Logger::info("suspend this consumer, {$this->consumerGroup}");
    }

    public function resume(): void
    {
        $this->paused = false;
        $this->rebalanceImmediately();
        Logger::info("resume this consumer, {$this->consumerGroup}");
    }

    public function isPaused(): bool
    {
        return $this->paused;
    }

    // ---------------- Pull (PHP: short-poll round inside tick) ----------------

    /** One pull round: at most maxPullsPerQueuePerTick short polls per assigned queue. */
    private function pullRound(float $now): void
    {
        $orderly = $this->isOrderly();
        $keys = array_keys($this->mqMap);
        foreach ($keys as $key) {
            $mq = $this->mqMap[$key] ?? null;
            if ($mq === null) {
                continue;
            }
            for ($i = 0; $i < max(1, $this->maxPullsPerQueuePerTick); $i++) {
                $verdict = $this->pullOnce($mq, $key, $orderly, $now);
                if ($verdict !== true) {
                    break;
                }
                $now = microtime(true);
            }
        }
    }

    /**
     * One pull of a single queue (a single round).
     * true = messages were pulled, another round can run immediately; false = this queue is done for this tick.
     */
    private function pullOnce(MessageQueue $mq, string $key, bool $orderly, float $now): bool
    {
        if (!$this->started) {
            return false;
        }
        $client = $this->requireClient();
        $this->lastPullTable[$key] = microtime(true);
        if ($this->paused) {
            return false;
        }
        $sub = $this->subscriptionData[$mq->topic] ?? null;
        if ($sub === null) {
            return false;
        }
        // Orderly consumption: queues not confirmed locked by the broker (LOCK_BATCH_MQ) are not pulled.
        if ($orderly && !isset($this->lockOk[$key])) {
            return false;
        }
        // Suspend retry not yet due (PHP adaptation: replaces orderly consumption's sleep).
        if (isset($this->suspendedUntil[$key]) && $now < $this->suspendedUntil[$key]) {
            return false;
        }
        if ($this->flowControlHit($mq, $key)) {
            return false;
        }
        // Idle backoff (PHP short-poll adaptation).
        if (isset($this->idleUntil[$key]) && $now < $this->idleUntil[$key]) {
            return false;
        }
        $offset = $this->offsetTable[$key] ?? null;
        if ($offset === null) {
            try {
                $offset = $this->resolveInitialOffset($client, $mq);
            } catch (\Throwable $e) {
                Logger::debug("resolve initial offset failed for $mq: " . $e->getMessage());
                $this->idleUntil[$key] = microtime(true) + 1.0;
                return false;
            }
            $this->offsetTable[$key] = $offset;
        }
        try {
            $subExpression = ($this->postSubscriptionWhenPull && !$sub->isClassFilterMode())
                ? ($sub->subString ?? '*') : null;
            $sysFlag = PullSysFlag::buildSysFlag(
                commitOffset: false,
                suspend: false,          // PHP short polling (intentional deviation, see the class header comment)
                subscription: $subExpression !== null,
                classFilter: false,
            );
            $brokerId = $this->pullFromWhichNode[$key] ?? MixAll::MASTER_ID;
            $pullBegan = microtime(true);
            $result = $client->pullMessage(
                $this->consumerGroup,
                $mq,
                $offset,
                $this->pullBatchSize,
                $sysFlag,
                0,
                $subExpression,
                $sub->subVersion,
                $sub->expressionType,
                timeoutMillis: $this->pullTimeoutMillis,
                maxMsgBytes: $this->pullBatchSizeInBytes,
                suspendTimeoutMillis: $this->pullSuspendTimeoutMillis,
                brokerId: $brokerId,
            );
            $this->pullFromWhichNode[$key] = $result->suggestWhichBrokerId ?? MixAll::MASTER_ID;
            // Consume stats: RT is recorded every time, TPS only when there are messages.
            if ($this->statsManager !== null) {
                $this->statsManager->incPullRt($this->consumerGroup, $mq->topic,
                    (int) ((microtime(true) - $pullBegan) * 1000));
                if ($result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
                    $this->statsManager->incPullTps($this->consumerGroup, $mq->topic,
                        count($result->msgFoundList));
                }
            }
        } catch (RemotingTimeoutException $e) {
            // Network timeout under short polling backs off and retries as an expected path.
            Logger::debug("pull timeout for $mq (will retry): " . $e->getMessage());
            $this->idleUntil[$key] = microtime(true) + 0.5;
            return false;
        } catch (\Throwable $e) {
            // Broker-side non-SUCCESS codes (TOPIC_NOT_EXIST etc.): expected path, debug + backoff.
            Logger::debug("pull error for $mq: " . get_class($e) . ': ' . $e->getMessage());
            $this->idleUntil[$key] = microtime(true) + 0.5;
            return false;
        }

        $found = $result->status === PullStatus::FOUND && $result->msgFoundList !== [];
        if ($found) {
            $result->msgFoundList = filter_messages_for_delivery(
                $this->consumerGroup,
                $this->filterMessageHookList,
                $mq,
                $sub,
                $result->msgFoundList,
                $this->unitMode,
            );
        }

        // Under the single-thread model, queue revocation cannot happen during a synchronous
        // pull; offset advancement runs after the "still holds the queue" guard.
        $illegal = false;
        if ($found && $result->msgFoundList !== []) {
            if (!isset($this->pending[$key])) {
                $this->pending[$key] = [];
            }
            foreach ($result->msgFoundList as $m) {
                $this->pending[$key][] = $m;
            }
            $this->updateMsgAccCnt($key, $result->msgFoundList);
        }
        if ($result->nextBeginOffset !== null) {
            $this->offsetTable[$key] = $result->nextBeginOffset;
            if ($result->status === PullStatus::OFFSET_ILLEGAL) {
                $this->frozenOffsets[$key] = true;
                $this->consumeOffsets[$key] = (int) $result->nextBeginOffset;
                $illegal = true;
            } else {
                $this->correctTagsOffset($key, $result->status, $result->nextBeginOffset);
            }
        }
        if ($illegal) {
            $this->offsetIllegalRecover($key);
            return false;
        }
        if ($result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
            return true; // Pulled something, run another round immediately
        }
        // Empty result: idle backoff (under short polling the broker returns immediately, with no suspend budget).
        $this->idleUntil[$key] = microtime(true) + max(0, $this->pullIntervalMillis) / 1000.0;
        return false;
    }

    private function updateMsgAccCnt(string $key, array $msgs): void
    {
        if ($msgs === []) {
            return;
        }
        $last = $msgs[count($msgs) - 1];
        $prop = $last->getProperty(MessageConst::PROPERTY_MAX_OFFSET);
        if ($prop === null) {
            return;
        }
        $accTotal = (int) $prop - (int) $last->queueOffset;
        if ($accTotal > 0) {
            $this->msgAccCntTable[$key] = $accTotal;
        }
    }

    /** Read ``msgAccCnt``: with a key read a single queue, without one sum (equivalent to computeAccumulationTotal). */
    public function msgAccCnt(?string $key = null): int
    {
        if ($key === null) {
            return array_sum($this->msgAccCntTable);
        }
        return $this->msgAccCntTable[$key] ?? 0;
    }

    public function computeAccumulationTotal(): int
    {
        return array_sum($this->msgAccCntTable);
    }

    public function adjustThreadPool(): void
    {
        $accTotal = $this->computeAccumulationTotal();
        $threshold = $this->adjustThreadPoolNumsThreshold;
        if ($accTotal >= $threshold) {
            Logger::debug("adjustThreadPool: acc=$accTotal >= incThreshold=$threshold (inc is a no-op upstream)");
        }
        if ($accTotal < $threshold * 0.8) {
            Logger::debug(sprintf('adjustThreadPool: acc=%d < decThreshold=%d (dec is a no-op upstream)',
                $accTotal, (int) ($threshold * 0.8)));
        }
    }

    public function updateCorePoolSize(int $corePoolSize): bool
    {
        if ($corePoolSize <= 0 || $corePoolSize > 32767) {
            return false;
        }
        if ($corePoolSize >= $this->consumeThreadMax) {
            return false;
        }
        $this->corePoolSize = $corePoolSize;
        return true;
    }

    public function getCorePoolSize(): int
    {
        return $this->corePoolSize;
    }

    private function flowControlHit(MessageQueue $mq, string $key): bool
    {
        $dq = $this->pending[$key] ?? [];
        $sizeMb = 0.0;
        $span = 0;
        foreach ($dq as $m) {
            $sizeMb += $m->storeSize;
        }
        $sizeMb /= (1024.0 * 1024.0);
        if ($dq !== []) {
            $offsets = array_map(static fn(MessageExt $m) => $m->queueOffset, $dq);
            $span = max($offsets) - min($offsets);
        }
        $reason = '';
        if (count($dq) >= max(1, $this->pullThresholdForQueue)) {
            $reason = 'count=' . count($dq);
        } elseif ($this->pullThresholdSizeForQueue > 0 && $sizeMb >= $this->pullThresholdSizeForQueue) {
            $reason = sprintf('size=%.1fMB', $sizeMb);
        } elseif ($this->consumeConcurrentlyMaxSpan > 0 && $span > $this->consumeConcurrentlyMaxSpan) {
            $reason = "span=$span";
        } elseif ($this->pullThresholdForTopic > 0 || $this->pullThresholdSizeForTopic > 0) {
            $topicPending = [];
            foreach ($this->pending as $k => $q) {
                $kmq = $this->mqMap[$k] ?? null;
                if ($kmq !== null && $kmq->topic === $mq->topic) {
                    foreach ($q as $m) {
                        $topicPending[] = $m;
                    }
                }
            }
            if ($this->pullThresholdForTopic > 0 && count($topicPending) >= $this->pullThresholdForTopic) {
                $reason = 'topicCount=' . count($topicPending);
            } elseif ($this->pullThresholdSizeForTopic > 0) {
                $topicMb = array_sum(array_map(static fn(MessageExt $m) => $m->storeSize, $topicPending))
                    / (1024.0 * 1024.0);
                if ($topicMb >= $this->pullThresholdSizeForTopic) {
                    $reason = sprintf('topicSize=%.1fMB', $topicMb);
                }
            }
        }
        if ($reason === '') {
            return false;
        }
        $this->flowControlTriggered++;
        Logger::debug("flow control: queue $mq $reason, pause pull");
        return true;
    }

    public function flowControlTriggeredCount(): int
    {
        return $this->flowControlTriggered;
    }

    // ---------------- Initial offset ----------------

    private function resolveInitialOffset(MQClientInstance $client, MessageQueue $mq): int
    {
        // Rule for the starting offset (this client's design): the initial
        // offset NEVER depends on the subscription type. An earlier draft short-circuited
        // SQL92 subscriptions to the max offset, which silently skipped every message
        // produced *before* the consumer started (send-first + CONSUME_FROM_FIRST_OFFSET
        // received nothing; consumer-first only passed because the queues were empty and
        // the max offset was 0 anyway). This client reads the stored offset and, when absent,
        // uses 0 for FIRST_OFFSET / max for LAST_OFFSET etc. -- the broker applies the
        // SQL92 filter to whatever range the pull covers.
        if ($this->messageModel === MessageModel::BROADCASTING) {
            $stored = $this->loadLocalOffsets();
            $key = self::mqKey($mq);
            if (isset($stored[$key])) {
                return $stored[$key];
            }
            if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
                return 0;
            }
            return $client->getMaxOffset($mq);
        }
        try {
            $stored = $client->queryConsumerOffset($this->consumerGroup, $mq, 5000, null, false);
            if ($stored !== null && $stored >= 0) {
                return $stored;
            }
        } catch (\Throwable $e) {
            Logger::debug("query consumer offset for $mq not found: " . $e->getMessage());
        }
        if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
            return 0;
        }
        if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_TIMESTAMP) {
            return $client->searchOffsetByTimestamp($mq, $this->consumeTimestampMillis());
        }
        if (NamespaceUtil::isRetryTopic($mq->topic)) {
            return 0;
        }
        // Default CONSUME_FROM_LAST_OFFSET
        return $client->getMaxOffset($mq);
    }

    // ---------------- Dispatch consumption (PHP: synchronous execution inside tick) ----------------

    /**
     * One dispatch round: hand the queued batches in each queue's pending to the listener
     * (synchronously). The loop body's batch count is bounded, keeping tick controllable.
     */
    private function dispatchRound(?int $budget = null): void
    {
        $budget = $budget ?? max(1, $this->dispatchBatchesPerTick);
        foreach (array_keys($this->pending) as $key) {
            if ($budget <= 0) {
                return;
            }
            $mq = $this->mqMap[$key] ?? null;
            if ($mq === null) {
                continue;
            }
            while ($budget > 0 && ($this->pending[$key] ?? []) !== []) {
                $now = microtime(true);
                if (isset($this->suspendedUntil[$key]) && $now < $this->suspendedUntil[$key]) {
                    break;
                }
                $n = min(count($this->pending[$key]), max(1, $this->consumeMessageBatchMaxSize));
                $batch = array_splice($this->pending[$key], 0, $n);
                if (isset($this->pending[$key]) && $this->pending[$key] === []) {
                    unset($this->pending[$key]);
                }
                // Register in-flight in the same critical section as "taking the batch away" (sequential under a single thread).
                $this->msgQueueInflight[$key] = ($this->msgQueueInflight[$key] ?? 0) + 1;
                // Take away the ProcessQueue epoch this batch's messages belong to as well.
                $epoch = $this->queueEpoch[$key] ?? 0;
                try {
                    $this->consumeBatch($key, $mq, $batch, $epoch);
                } catch (\Throwable $e) {
                    // Unexpected exception on the dispatch path: push the batch back to the head and retry later (do not let it kill tick).
                    Logger::error('dispatch batch error (will retry): ' . get_class($e) . ': ' . $e->getMessage());
                    if (isset($this->pending[$key])) {
                        foreach (array_reverse($batch) as $m) {
                            array_unshift($this->pending[$key], $m);
                        }
                    } else {
                        $this->pending[$key] = $batch;
                    }
                    break;
                } finally {
                    $left = ($this->msgQueueInflight[$key] ?? 0) - 1;
                    if ($left > 0) {
                        $this->msgQueueInflight[$key] = $left;
                    } else {
                        unset($this->msgQueueInflight[$key]);
                    }
                }
                $budget--;
            }
        }
    }

    /**
     * Drain the batches still queued for consumption while shutting down, bounded by
     * {@see $awaitTerminationMillisWhenShutdown}. The drain walks the buffers in chunks so a
     * positive ceiling can bind in the middle of a backlog instead of only after all of it;
     * ``<= 0`` adds no time ceiling and leaves only the round cap (a listener that keeps
     * rejecting its batch must not hang shutdown).
     */
    private function drainQueuedMessagesOnShutdown(): void
    {
        if ($this->pending === [] || $this->messageListener === null) {
            return;
        }
        $ceiling = $this->awaitTerminationMillisWhenShutdown;
        $beginMs = UtilAll::currentTimeMillis();
        for ($round = 0; $round < self::SHUTDOWN_DRAIN_MAX_ROUNDS; $round++) {
            if ($this->pending === []) {
                return;
            }
            $this->dispatchRound(self::SHUTDOWN_DRAIN_CHUNK_BATCHES);
            if ($ceiling > 0 && UtilAll::currentTimeMillis() - $beginMs >= $ceiling) {
                Logger::info(sprintf(
                    'shutdown drain stopped by awaitTerminationMillisWhenShutdown=%dms, %d message(s) left queued',
                    $ceiling,
                    $this->queuedMessageCount(),
                ));
                return;
            }
        }
    }

    /** Total messages waiting in the per-queue buffers. */
    private function queuedMessageCount(): int
    {
        $n = 0;
        foreach ($this->pending as $dq) {
            $n += is_array($dq) ? count($dq) : 0;
        }
        return $n;
    }

    private function resetRetryTopicAndNamespace(array $msgs): void
    {
        $groupTopic = MixAll::getRetryTopic($this->consumerGroup);
        foreach ($msgs as $msg) {
            $retryTopic = $msg->getProperty(MessageConst::PROPERTY_RETRY_TOPIC);
            if ($retryTopic !== null && $msg->topic === $groupTopic) {
                $msg->topic = $retryTopic;
            }
            if ($this->namespace !== '') {
                $msg->topic = NamespaceUtil::withoutNamespace($msg->topic, $this->namespace);
            }
        }
    }

    // ---------------- Broker-initiated requests: 220 / 221 / 307 / 309 ----------------

    public function resetOffset(string $topic, array $offsetTable): void
    {
        if ($topic === '' || $offsetTable === []) {
            return;
        }
        $retired = [];
        foreach ($this->mqMap as $key => $mq) {
            if ($mq->topic !== $topic) {
                continue;
            }
            $off = null;
            foreach ($offsetTable as $entry) {
                if ($entry['mq']->equals($mq)) {
                    $off = (int) $entry['offset'];
                    break;
                }
            }
            if ($off === null) {
                continue;
            }
            // The new offset goes into the table first: revocation hands **it** to onQueuesRevoked for persistence.
            $this->consumeOffsets[$key] = $off;
            $this->retireQueue($key, $mq, $retired);
        }
        if ($retired === []) {
            return;
        }
        usleep(200000); // 0.2s (intentional deviation: the epoch has already invalidated all in-flight acks)
        $this->onQueuesRevoked($retired);
        try {
            $this->doRebalance();
        } catch (\Throwable $e) {
            Logger::debug('rebalance after reset offset failed: ' . $e->getMessage());
        }
        Logger::info(sprintf(
            'reset offset applied, group=%s topic=%s queues=%d',
            $this->consumerGroup,
            $topic,
            count($retired),
        ));
    }

    public function getConsumerStatus(?string $topic): array
    {
        $out = [];
        foreach ($this->mqMap as $key => $mq) {
            if ($topic !== null && $topic !== '' && $mq->topic !== $topic) {
                continue;
            }
            $off = $this->consumeOffsets[$key] ?? null;
            if ($off !== null) {
                $out[] = ['mq' => $mq, 'offset' => (int) $off];
            }
        }
        return $out;
    }

    public function consumerRunningInfo(): ConsumerRunningInfo
    {
        $info = new ConsumerRunningInfo();
        $info->properties = [
            ConsumerRunningInfo::PROP_NAMESERVER_ADDR => implode(';', $this->nameServerAddrs) . ';',
            ConsumerRunningInfo::PROP_CONSUME_TYPE => 'CONSUME_PASSIVELY',
            ConsumerRunningInfo::PROP_CONSUME_ORDERLY => $this->isOrderly() ? 'true' : 'false',
            ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE => (string) $this->getCorePoolSize(),
            ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP => (string) (int) ($this->startTime * 1000),
            ConsumerRunningInfo::PROP_CLIENT_VERSION => 'V5_5_1',
        ];
        $subs = array_values($this->subscriptionData);
        // In POP mode the popped ones go to popQueues, while classic's processQueue is empty - the two tables are mutually exclusive.
        $popKeys = $this->popMode ? array_fill_keys(array_keys($this->popQueues), true) : [];
        foreach ($this->mqMap as $key => $mq) {
            if (isset($popKeys[$key])) {
                continue;
            }
            $pqi = new ProcessQueueInfo();
            $pqi->commitOffset = (int) ($this->consumeOffsets[$key] ?? 0);
            $pqi->cachedMsgCount = count($this->pending[$key] ?? []);
            $pqi->droped = false;
            $pqi->lastPullTimestamp = (int) (($this->lastPullTable[$key] ?? 0) * 1000);
            $info->mqTable[message_queue_key($mq)] = $pqi->toDict();
        }
        if ($this->popMode) {
            foreach ($this->popQueues as $key => $pq) {
                $mq = $this->mqMap[$key] ?? null;
                if ($mq === null) {
                    continue;
                }
                $pqi = new ProcessQueueInfo();
                $pqi->cachedMsgCount = $pq->waitAckCount();
                $pqi->droped = $pq->isDropped();
                $pqi->lastPullTimestamp = (int) ($pq->lastPopTimestamp * 1000);
                $info->mqPopTable[message_queue_key($mq)] = $pqi->toDict();
            }
        }
        $info->subscriptionSet = array_map(
            static fn(SubscriptionData $s): array => $s->toDict(),
            $subs,
        );
        foreach ($subs as $s) {
            if ($this->statsManager !== null && $s->topic !== null) {
                $info->statusTable[$s->topic] = $this->statsManager->consumeStatus($this->consumerGroup, $s->topic)->toDict();
            }
        }
        return $info;
    }

    public function consumeMessageDirectly(MessageExt $msg, ?string $brokerName): ConsumeMessageDirectlyResult
    {
        $result = new ConsumeMessageDirectlyResult();
        $orderly = $this->isOrderly();
        $result->order = $orderly;
        $msgs = [$msg];
        $mq = new MessageQueue($msg->topic, $brokerName ?? '', $msg->queueId);
        $this->resetRetryTopicAndNamespace($msgs);
        $context = $orderly ? new ConsumeOrderlyContext($mq) : new ConsumeConcurrentlyContext($mq);
        $begin = UtilAll::currentTimeMillis();
        try {
            $status = $this->messageListener !== null
                ? $this->messageListener->consumeMessage($msgs, $context)
                : null;
            if ($orderly) {
                $result->consumeResult = match ($status) {
                    ConsumeOrderlyStatus::SUCCESS => CMResult::CR_SUCCESS,
                    ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT => CMResult::CR_LATER,
                    ConsumeOrderlyStatus::COMMIT => CMResult::CR_COMMIT,
                    ConsumeOrderlyStatus::ROLLBACK => CMResult::CR_ROLLBACK,
                    null => CMResult::CR_RETURN_NULL,
                    default => null,
                };
            } else {
                $result->consumeResult = match ($status) {
                    ConsumeConcurrentlyStatus::CONSUME_SUCCESS => CMResult::CR_SUCCESS,
                    ConsumeConcurrentlyStatus::RECONSUME_LATER => CMResult::CR_LATER,
                    null => CMResult::CR_RETURN_NULL,
                    default => null,
                };
            }
        } catch (\Throwable $e) {
            $result->consumeResult = CMResult::CR_THROW_EXCEPTION;
            $result->remark = get_class($e) . ': ' . $e->getMessage();
        }
        $result->autoCommit = $orderly ? $context->autoCommit : true;
        $result->spentTimeMills = UtilAll::currentTimeMillis() - $begin;
        return $result;
    }

    // ---------------- Batch consumption ----------------

    private function isOrderly(): bool
    {
        return $this->messageListener instanceof MessageListenerOrderly;
    }

    private function consumeBatch(string $key, MessageQueue $mq, array $batch, ?int $epoch = null): bool
    {
        if ($epoch !== null && $epoch !== ($this->queueEpoch[$key] ?? 0)) {
            $dropped = $this->queueEpoch[$key] ?? 0;
            Logger::warning(sprintf(
                "the message queue not be able to consume, because it's dropped. group=%s mq=%s msgs=%d epoch=%d->%d",
                $this->consumerGroup,
                (string) $mq,
                count($batch),
                $epoch,
                $dropped,
            ));
            return false;
        }
        $listener = $this->messageListener;
        $broadcast = $this->messageModel === MessageModel::BROADCASTING;
        $this->resetRetryTopicAndNamespace($batch);
        if ($this->isOrderly()) {
            $ocontext = new ConsumeOrderlyContext($mq);
            $ohookCtx = null;
            if ($this->consumeMessageHookList !== []) {
                $ohookCtx = $this->buildConsumeHookContext($batch, $mq);
                $this->executeConsumeHookBefore($ohookCtx);
            }
            $obeginMs = UtilAll::currentTimeMillis();
            $ohasException = false;
            try {
                $status = $listener->consumeMessage($batch, $ocontext);
            } catch (\Throwable $e) {
                Logger::debug('orderly listener error (retry in place): ' . $e->getMessage());
                $status = null;
                $ohasException = true;
            }
            if ($status === null || $status === ConsumeOrderlyStatus::ROLLBACK
                || $status === ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
                Logger::warning(sprintf(
                    'consumeMessage Orderly return not OK, Group: %s Msgs: %d MQ: %s',
                    $this->consumerGroup,
                    count($batch),
                    (string) $mq,
                ));
            }
            $rawStatus = $status;
            // Normalization: null / unknown values are always treated as suspend (otherwise they would be silently acked as SUCCESS).
            if (!($status instanceof ConsumeOrderlyStatus)) {
                $status = ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
            $this->finishConsumeHook(
                $ohookCtx,
                $rawStatus,
                $ohasException,
                $obeginMs,
                failed: $status === ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT,
                succeeded: $status === ConsumeOrderlyStatus::SUCCESS || $status === ConsumeOrderlyStatus::COMMIT,
                hookStatus: $status,
            );
            if ($ocontext->autoCommit) {
                if ($status === ConsumeOrderlyStatus::COMMIT || $status === ConsumeOrderlyStatus::ROLLBACK) {
                    Logger::warning('the message queue consume result is illegal, '
                        . "we think you want to ack these message $mq");
                    $status = ConsumeOrderlyStatus::SUCCESS;
                }
                if ($status === ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
                    $this->recordConsumeStats($mq->topic, count($batch), $obeginMs, failed: true);
                    if ($this->checkOrderlyReconsumeTimes($batch)) {
                        $this->requeuePending($key, $batch);
                        $this->suspendedUntil[$key] = microtime(true) + $this->orderlySuspendMillis($ocontext) / 1000.0;
                        return false;
                    }
                } else {
                    $this->recordConsumeStats($mq->topic, count($batch), $obeginMs, failed: false);
                }
                $this->advanceConsumeOffset($key, $batch, null, $epoch);
                return true;
            }
            if ($status === ConsumeOrderlyStatus::COMMIT) {
                $this->recordConsumeRt($mq->topic, $obeginMs);
                $this->advanceConsumeOffset($key, $batch, null, $epoch);
                return true;
            }
            if ($status === ConsumeOrderlyStatus::ROLLBACK) {
                $this->recordConsumeRt($mq->topic, $obeginMs);
                $this->requeuePending($key, $batch);
                $this->suspendedUntil[$key] = microtime(true) + $this->orderlySuspendMillis($ocontext) / 1000.0;
                return false;
            }
            if ($status === ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
                $this->recordConsumeStats($mq->topic, count($batch), $obeginMs, failed: true);
                if ($this->checkOrderlyReconsumeTimes($batch)) {
                    $this->requeuePending($key, $batch);
                    $this->suspendedUntil[$key] = microtime(true) + $this->orderlySuspendMillis($ocontext) / 1000.0;
                }
                return false;
            }
            $this->recordConsumeStats($mq->topic, count($batch), $obeginMs, failed: false);
            $this->requeuePending($key, $batch);
            $this->suspendedUntil[$key] = microtime(true) + $this->orderlySuspendMillis($ocontext) / 1000.0;
            return false;
        }
        if (!isset($this->inflightMsgs[$key])) {
            $this->inflightMsgs[$key] = [];
        }
        foreach ($batch as $m) {
            $this->inflightMsgs[$key][] = $m;
        }
        try {
            return $this->consumeConcurrentBatch($key, $mq, $batch, $epoch, $broadcast);
        } finally {
            $this->deregisterInflight($key, $batch);
        }
    }

    private function consumeConcurrentBatch(
        string $key,
        MessageQueue $mq,
        array $batch,
        ?int $epoch,
        bool $broadcast,
    ): bool {
        $listener = $this->messageListener;
        $context = new ConsumeConcurrentlyContext($mq);
        $hookCtx = null;
        if ($this->consumeMessageHookList !== []) {
            $hookCtx = $this->buildConsumeHookContext($batch, $mq);
            $this->executeConsumeHookBefore($hookCtx);
        }
        $beginMs = UtilAll::currentTimeMillis();
        $hasException = false;
        try {
            foreach ($batch as $m) {
                MessageAccessor::setConsumeStartTimestamp($m, UtilAll::currentTimeMillis());
            }
            $status = $listener->consumeMessage($batch, $context);
        } catch (\Throwable $e) {
            Logger::debug('listener error, treat as RECONSUME_LATER: ' . $e->getMessage());
            $status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
            $hasException = true;
        }
        $rawStatus = $status;
        if ($status === null) {
            Logger::warning(sprintf(
                'consumeMessage return null, Group: %s Msgs: %d MQ: %s',
                $this->consumerGroup,
                count($batch),
                (string) $mq,
            ));
            $status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
        }
        $ackIndex = $context->ackIndex;
        if ($status === ConsumeConcurrentlyStatus::CONSUME_SUCCESS) {
            if ($ackIndex >= count($batch)) {
                $ackIndex = count($batch) - 1;
            }
        } else {
            $ackIndex = -1;
        }
        $this->recordConsumeStats(
            $mq->topic,
            count($batch),
            $beginMs,
            failed: $status === ConsumeConcurrentlyStatus::RECONSUME_LATER,
            ackCount: $ackIndex + 1,
        );
        $this->finishConsumeHook(
            $hookCtx,
            $rawStatus,
            $hasException,
            $beginMs,
            failed: $status === ConsumeConcurrentlyStatus::RECONSUME_LATER,
            succeeded: $status === ConsumeConcurrentlyStatus::CONSUME_SUCCESS,
            hookStatus: $status,
        );
        if ($broadcast) {
            $dropped = count($batch) - $ackIndex - 1;
            if ($dropped > 0) {
                Logger::warning(sprintf(
                    'BROADCASTING, the message consume failed, drop it: %d msgs in %s',
                    $dropped,
                    (string) $mq,
                ));
            }
            $this->advanceConsumeOffset($key, $batch, null, $epoch);
            return true;
        }
        if ($ackIndex + 1 >= count($batch)) {
            // Whole batch acknowledged (default path).
            $this->advanceConsumeOffset($key, $batch, null, $epoch);
            return true;
        }
        // Clustering mode: the unacknowledged [ack_index+1, size) are sent back one by one to %RETRY%topic.
        $msgBackFailed = $this->sendBackBatch($key, array_slice($batch, $ackIndex + 1), $context);
        if ($msgBackFailed !== []) {
            if (isset($this->pending[$key])) {
                foreach (array_reverse($msgBackFailed) as $m) {
                    array_unshift($this->pending[$key], $m);
                }
            } else {
                $this->pending[$key] = $msgBackFailed;
            }
            usleep(200000);
        }
        $failedIds = [];
        foreach ($msgBackFailed as $m) {
            $failedIds[spl_object_id($m)] = true;
        }
        $acked = array_values(array_filter(
            $batch,
            static fn(MessageExt $m): bool => !isset($failedIds[spl_object_id($m)]),
        ));
        $floor = null;
        if ($msgBackFailed !== []) {
            $floor = min(array_map(static fn(MessageExt $m) => $m->queueOffset, $msgBackFailed));
        }
        $this->advanceConsumeOffset($key, $acked, $floor, $epoch);
        return $msgBackFailed === [];
    }

    private function requeuePending(string $key, array $batch): void
    {
        if (isset($this->pending[$key])) {
            foreach (array_reverse($batch) as $m) {
                array_unshift($this->pending[$key], $m);
            }
        } else {
            $this->pending[$key] = array_values($batch);
        }
    }

    /** Detach this batch from the "in the listener's hands" registration (by object identity, idempotent). */
    private function deregisterInflight(string $key, array $batch): void
    {
        $bucket = $this->inflightMsgs[$key] ?? null;
        if ($bucket === null || $bucket === []) {
            return;
        }
        $gone = [];
        foreach ($batch as $m) {
            $gone[spl_object_id($m)] = true;
        }
        $bucket = array_values(array_filter($bucket, static fn(MessageExt $m): bool => !isset($gone[spl_object_id($m)])));
        if ($bucket === []) {
            unset($this->inflightMsgs[$key]);
        } else {
            $this->inflightMsgs[$key] = $bucket;
        }
    }

    private function processQueueEntries(string $key): array
    {
        $out = [];
        $seen = [];
        foreach (($this->inflightMsgs[$key] ?? []) as $m) {
            $id = spl_object_id($m);
            if (!isset($seen[$id])) {
                $seen[$id] = true;
                $out[] = $m;
            }
        }
        foreach (($this->pending[$key] ?? []) as $m) {
            $id = spl_object_id($m);
            if (!isset($seen[$id])) {
                $seen[$id] = true;
                $out[] = $m;
            }
        }
        return $out;
    }

    private function processQueueFirstEntry(string $key): ?MessageExt
    {
        $entries = $this->processQueueEntries($key);
        if ($entries === []) {
            return null;
        }
        $first = null;
        foreach ($entries as $m) {
            if ($first === null || ($m->queueOffset) < ($first->queueOffset)) {
                $first = $m;
            }
        }
        return $first;
    }

    private function processQueueContains(string $key, MessageExt $msg): bool
    {
        foreach (($this->inflightMsgs[$key] ?? []) as $m) {
            if ($m === $msg) {
                return true;
            }
        }
        foreach (($this->pending[$key] ?? []) as $m) {
            if ($m === $msg) {
                return true;
            }
        }
        return false;
    }

    /** Detach the entry just swept (the caller has confirmed it is the head of the queue). */
    private function removeProcessQueueEntry(string $key, MessageExt $msg): void
    {
        $this->deregisterInflight($key, [$msg]);
        $dq = $this->pending[$key] ?? null;
        if ($dq !== null && ($dq[0] ?? null) === $msg) {
            array_shift($this->pending[$key]);
            if (($this->pending[$key] ?? []) === []) {
                unset($this->pending[$key]);
            }
        }
    }

    // ---------------- cleanExpiredMsg: the escape hatch of a hung listener ----------------

    public function cleanExpiredMsgOnce(): void
    {
        foreach (array_keys($this->mqMap) as $key) {
            $this->cleanExpiredQueue($key);
        }
    }

    private function cleanExpiredQueue(string $key): void
    {
        if ($this->isOrderly()) {
            return;
        }
        $loop = min(count($this->processQueueEntries($key)), 16);
        for ($i = 0; $i < $loop; $i++) {
            $msg = null;
            $first = $this->processQueueFirstEntry($key);
            if ($first !== null) {
                $stamp = MessageAccessor::getConsumeStartTimestamp($first);
                if ($stamp !== null
                    && (UtilAll::currentTimeMillis() - (float) $stamp) > $this->consumeTimeout * 60 * 1000) {
                    $msg = $first;
                }
            }
            if ($msg === null) {
                break;
            }
            try {
                $this->sendMessageBack($msg, 3);
            } catch (\Throwable $e) {
                Logger::error('send expired msg exception: ' . $e->getMessage());
                continue;
            }
            Logger::info(sprintf(
                'send expire msg back. topic=%s, msgId=%s, storeHost=%s, queueId=%s, queueOffset=%s',
                $msg->topic,
                (string) $msg->msgId,
                (string) $msg->getStoreHostString(),
                (string) $msg->queueId,
                (string) $msg->queueOffset,
            ));
            if ($this->processQueueFirstEntry($key) === $msg) {
                $this->removeProcessQueueEntry($key, $msg);
            }
        }
    }

    // ---------------- Concurrent send-back ----------------

    private function sendBackBatch(string $key, array $batch, ConsumeConcurrentlyContext $context): array
    {
        $failed = [];
        foreach ($batch as $msg) {
            $present = $this->processQueueContains($key, $msg);
            if (!$present) {
                Logger::info(sprintf(
                    'Message is not found in its process queue; skip send-back-procedure, topic=%s, '
                    . 'brokerName=%s, queueId=%s, queueOffset=%s',
                    $msg->topic,
                    (string) $msg->brokerName,
                    (string) $msg->queueId,
                    (string) $msg->queueOffset,
                ));
                continue;
            }
            try {
                // The reconsume count is field 13 of MessageExt's wire format; the broker increments it on redelivery.
                $delayLevel = $context->delayLevelWhenNextConsume;
                if (!$delayLevel) {
                    $delayLevel = 3 + $msg->getReconsumeTimes();
                }
                $this->sendMessageBack($msg, $delayLevel);
            } catch (\Throwable $e) {
                Logger::debug('send message back failed for msg ' . $msg->msgId . ': ' . $e->getMessage());
                $msg->setReconsumeTimes($msg->getReconsumeTimes() + 1);
                $failed[] = $msg;
            }
        }
        return $failed;
    }

    public function sendMessageBack(MessageExt $msg, int $delayLevel, ?string $brokerName = null): void
    {
        $client = $this->requireClient();
        $brokerName = $brokerName ?? (string) $msg->brokerName;
        $addr = $client->findBrokerAddressInPublish($brokerName);
        if ($addr === null || $addr === '') {
            throw new MQClientException("Broker[$brokerName] master node does not exist");
        }
        $maxReconsume = $this->maxReconsumeTimes === -1 ? 16 : $this->maxReconsumeTimes;
        $header = new ConsumerSendMsgBackRequestHeader();
        $header->offset = $msg->commitLogOffset;
        $header->group = $this->consumerGroup;
        $header->delayLevel = $delayLevel;
        $header->originMsgId = (string) $msg->msgId;
        $header->originTopic = $msg->topic;
        $header->unitMode = $this->unitMode;
        $header->maxReconsumeTimes = $maxReconsume;
        $request = RemotingCommand::createRequestCommand(RequestCode::CONSUMER_SEND_MSG_BACK, $header);
        $response = $client->remotingClient->invokeSync($client->vipAddrFor($addr), $request, 5000);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }
    }

    // ---------------- Orderly consumption's retry counting and send-back ----------------

    private function orderlySuspendMillis(ConsumeOrderlyContext $context): int
    {
        $ms = $context->suspendCurrentQueueTimeMillis;
        if ($ms === -1) {
            $ms = $this->suspendCurrentQueueTimeMillis;
        }
        if ($ms < 10) {
            return 10;
        }
        if ($ms > 30000) {
            return 30000;
        }
        return $ms;
    }

    private function orderlyMaxReconsumeTimes(): int
    {
        if ($this->maxReconsumeTimes === -1) {
            return ConsumerDefaults::INT32_MAX;
        }
        return $this->maxReconsumeTimes;
    }

    private function checkOrderlyReconsumeTimes(?array $msgs): bool
    {
        $suspend = false;
        $maxTimes = $this->orderlyMaxReconsumeTimes();
        foreach ($msgs ?? [] as $msg) {
            if ($msg->getReconsumeTimes() >= $maxTimes) {
                MessageAccessor::setReconsumeTime($msg, $msg->getReconsumeTimes());
                if (!$this->orderlySendMessageBack($msg)) {
                    $suspend = true;
                    $msg->setReconsumeTimes($msg->getReconsumeTimes() + 1);
                }
            } else {
                $suspend = true;
                $msg->setReconsumeTimes($msg->getReconsumeTimes() + 1);
            }
        }
        return $suspend;
    }

    private function orderlySendMessageBack(MessageExt $msg): bool
    {
        try {
            $client = $this->requireClient();
            $newMsg = new Message(MixAll::getRetryTopic($this->consumerGroup), $msg->body);
            $newMsg->properties = $msg->properties;
            $newMsg->flag = $msg->flag;
            $originMsgId = MessageAccessor::getOriginMessageId($msg) ?? $msg->msgId;
            if ($originMsgId !== null && $originMsgId !== '') {
                MessageAccessor::setOriginMessageId($newMsg, $originMsgId);
            }
            MessageAccessor::putProperty($newMsg, MessageConst::PROPERTY_RETRY_TOPIC, $msg->topic);
            MessageAccessor::setReconsumeTime($newMsg, $msg->getReconsumeTimes() + 1);
            MessageAccessor::setMaxReconsumeTimes($newMsg, $this->orderlyMaxReconsumeTimes());
            // The half-message mark must be cleared, otherwise the broker will treat it as a transaction check-back message again
            MessageAccessor::clearProperty($newMsg, MessageConst::PROPERTY_TRANSACTION_PREPARED);
            $newMsg->setDelayTimeLevel(3 + $msg->getReconsumeTimes());
            $publish = $client->getTopicPublishInfo($newMsg->topic, true);
            $mq = $publish !== null ? $publish->selectOneMessageQueue() : null;
            if ($mq === null) {
                throw new MQClientException("no writable queue for retry topic $newMsg->topic");
            }
            // unitMode follows the consumer: without it, sent-back messages lose their unitization mark.
            $client->sendMessage(MixAll::CLIENT_INNER_PRODUCER_GROUP, $newMsg, $mq, 3000, 0, $this->unitMode);
            return true;
        } catch (\Throwable $e) {
            Logger::debug(sprintf(
                'orderly send message back failed, group=%s msg=%s: %s',
                $this->consumerGroup,
                (string) $msg->msgId,
                $e->getMessage(),
            ));
            return false;
        }
    }

    // ---------------- Offset advancement / correction / recovery ----------------

    private function advanceConsumeOffset(string $key, array $batch, ?int $floor = null, ?int $epoch = null): void
    {
        if ($batch === []) {
            // When the whole batch's send-back fails, no entry is acknowledged and the offset stays put
            return;
        }
        $nextOff = max(array_map(static fn(MessageExt $m) => $m->queueOffset, $batch)) + 1;
        if ($floor !== null) {
            $nextOff = min($nextOff, $floor);
        }
        if ($epoch !== null && $epoch !== ($this->queueEpoch[$key] ?? 0)) {
            Logger::debug(sprintf(
                'drop ack for %s: process queue was dropped (epoch %d -> %d)',
                $key,
                $epoch,
                $this->queueEpoch[$key] ?? 0,
            ));
            return;
        }
        if (isset($this->frozenOffsets[$key])) {
            return;
        }
        $cur = $this->consumeOffsets[$key] ?? 0;
        $this->consumeOffsets[$key] = max($cur, $nextOff);
    }

    private function correctTagsOffset(string $key, PullStatus $status, ?int $nextOff): void
    {
        if ($status !== PullStatus::NO_NEW_MSG && $status !== PullStatus::NO_MATCHED_MSG) {
            return;
        }
        if ($nextOff === null) {
            return;
        }
        if (isset($this->frozenOffsets[$key])) {
            // The offset has been frozen by OFFSET_ILLEGAL correction, so this path must not change it either
            return;
        }
        if (($this->pending[$key] ?? []) !== [] || ($this->msgQueueInflight[$key] ?? 0) !== 0) {
            return;
        }
        $cur = $this->consumeOffsets[$key] ?? null;
        if ($cur === null || $nextOff > $cur) {
            $this->consumeOffsets[$key] = (int) $nextOff;
        }
    }

    private function offsetIllegalRecover(string $key): void
    {
        $retired = [];
        $this->retireQueue($key, null, $retired);
        $this->onQueuesRevoked($retired);
        $this->rebalanceImmediately();
        Logger::warning("the pull request offset illegal, fix it, queue=$key");
    }

    // ---------------- Offset persistence ----------------

    public function persistOffsetsOnce(): void
    {
        if ($this->messageModel === MessageModel::BROADCASTING) {
            $this->saveLocalOffsets();
            return;
        }
        $client = $this->requireClient();
        foreach ($this->consumeOffsets as $key => $off) {
            $mq = $this->mqMap[$key] ?? null;
            if ($mq === null) {
                continue;
            }
            try {
                $client->updateConsumerOffset($this->consumerGroup, $mq, $off);
            } catch (\Throwable $e) {
                Logger::debug("update consumer offset failed for $mq: " . $e->getMessage());
            }
        }
    }

    private function localOffsetPath(): string
    {
        $home = getenv('HOME') ?: '';
        return implode(DIRECTORY_SEPARATOR, [
            $home,
            '.rocketmq_offsets',
            $this->clientId ?: 'DEFAULT',
            $this->consumerGroup,
            'offsets.json',
        ]);
    }

    private function saveLocalOffsets(?array $extra = null): void
    {
        $items = $this->consumeOffsets;
        $mqMap = $this->mqMap;
        foreach ($extra ?? [] as [$mq, $off]) {
            if ($off === null) {
                continue;
            }
            $key = self::mqKey($mq);
            $items[$key] = (int) $off;
            $mqMap[$key] = $mqMap[$key] ?? $mq;
        }
        $path = $this->localOffsetPath();
        $dir = dirname($path);
        if (!is_dir($dir)) {
            @mkdir($dir, 0777, true);
        }
        $text = build_local_offsets_json($items, $mqMap);
        if (is_file($path)) {
            $prev = (string) @file_get_contents($path);
            if ($prev !== '') {
                @file_put_contents($path . '.bak', $prev);
            }
        }
        $tmp = $path . '.tmp';
        file_put_contents($tmp, $text);
        @rename($tmp, $path);
    }

    private function loadLocalOffsets(): array
    {
        foreach ([$this->localOffsetPath(), $this->localOffsetPath() . '.bak'] as $path) {
            if (!is_file($path)) {
                continue;
            }
            $text = @file_get_contents($path);
            if ($text === false || $text === '') {
                continue;
            }
            $parsed = parse_local_offsets_json($text);
            if ($parsed !== null) {
                return $parsed;
            }
        }
        return [];
    }

    // ---------------- Orderly consumption queue lock ----------------

    private function lockQueuesOnce(): void
    {
        try {
            $client = $this->requireClient();
            $mqs = $this->assignedQueues();
            if ($mqs !== []) {
                $ok = $client->lockBatchMq($this->consumerGroup, $this->clientId ?? '', $mqs);
                $okKeys = [];
                foreach ($ok as $m) {
                    $okKeys[self::mqKey($m)] = true;
                }
                $this->lockOk = $okKeys;
                Logger::debug(sprintf('lock_batch_mq: %d/%d queues locked', count($okKeys), count($mqs)));
            }
        } catch (\Throwable $e) {
            Logger::debug('lock mq error: ' . $e->getMessage());
        }
    }

    /** @return list<MessageQueue> */
    private function assignedQueues(): array
    {
        return array_values($this->assigned);
    }

    // ---------------- Admin capabilities ----------------

    /** @return list<MessageQueue> */
    public function fetchSubscribeMessageQueues(string $topic): array
    {
        $client = $this->requireClient();
        return $client->getTopicSubscribeInfo($topic);
    }

    // ---------------- Consume hooks ----------------

    private function buildConsumeHookContext(array $msgs, MessageQueue $mq): ConsumeMessageContext
    {
        $context = new ConsumeMessageContext($this->consumerGroup, $msgs, $mq);
        $context->success = false;
        $context->props = [];
        $context->accessChannel = AccessChannel::LOCAL;
        return $context;
    }

    private function executeConsumeHookBefore(ConsumeMessageContext $context): void
    {
        foreach ($this->consumeMessageHookList as $hook) {
            try {
                $hook->consumeMessageBefore($context);
            } catch (\Throwable $e) {
                Logger::warning('consumeMessageHook executeHookBefore exception: ' . $e->getMessage());
            }
        }
    }

    private function executeConsumeHookAfter(ConsumeMessageContext $context): void
    {
        foreach ($this->consumeMessageHookList as $hook) {
            try {
                $hook->consumeMessageAfter($context);
            } catch (\Throwable $e) {
                Logger::warning('consumeMessageHook executeHookAfter exception: ' . $e->getMessage());
            }
        }
    }

    private function consumeReturnType(
        ?object $status,
        bool $hasException,
        int $consumeRtMs,
        bool $failed,
        bool $succeeded,
    ): ConsumeReturnType {
        if ($status === null) {
            return $hasException ? ConsumeReturnType::EXCEPTION : ConsumeReturnType::RETURNNULL;
        }
        if ($consumeRtMs >= $this->consumeTimeout * 60 * 1000) {
            return ConsumeReturnType::TIME_OUT;
        }
        if ($failed) {
            return ConsumeReturnType::FAILED;
        }
        return ConsumeReturnType::SUCCESS;
    }

    private function recordConsumeRt(string $topic, int $beginMs): void
    {
        if ($this->statsManager === null) {
            return;
        }
        $this->statsManager->incConsumeRt($this->consumerGroup, $topic, UtilAll::currentTimeMillis() - $beginMs);
    }

    private function recordConsumeStats(
        string $topic,
        int $msgCount,
        int $beginMs,
        bool $failed,
        ?int $ackCount = null,
    ): void {
        if ($this->statsManager === null) {
            return;
        }
        if ($failed) {
            $this->statsManager->incConsumeFailedTps($this->consumerGroup, $topic, $msgCount);
        } else {
            $ok = $ackCount ?? $msgCount;
            $this->statsManager->incConsumeOkTps($this->consumerGroup, $topic, $ok);
            if ($msgCount > $ok) {
                $this->statsManager->incConsumeFailedTps($this->consumerGroup, $topic, $msgCount - $ok);
            }
        }
        $this->recordConsumeRt($topic, $beginMs);
    }

    private function finishConsumeHook(
        ?ConsumeMessageContext $hookCtx,
        ?object $status,
        bool $hasException,
        int $beginMs,
        bool $failed,
        bool $succeeded,
        ?object $hookStatus = null,
    ): void {
        if ($hookCtx === null) {
            return;
        }
        $rt = UtilAll::currentTimeMillis() - $beginMs;
        $ret = $this->consumeReturnType($status, $hasException, $rt, $failed, $succeeded);
        if (!is_array($hookCtx->props)) {
            $hookCtx->props = [];
        }
        $hookCtx->props['ConsumeContextType'] = $ret->name;
        $hookCtx->status = consumeStatusName($hookStatus ?? $status);
        $hookCtx->success = $succeeded;
        $this->executeConsumeHookAfter($hookCtx);
    }

    // ---------------------------------------------------------------- POP consume loop (PHP: short polling inside a tick)

    /**
     * One POP round: pops at most maxPopsPerQueuePerTick times from every assigned queue (pollTime=0).
     */
    private function popRound(float $now): void
    {
        foreach (array_keys($this->mqMap) as $key) {
            $mq = $this->mqMap[$key] ?? null;
            if ($mq === null) {
                continue;
            }
            for ($i = 0; $i < max(1, $this->maxPopsPerQueuePerTick); $i++) {
                $verdict = $this->popOnce($mq, $key, $now);
                if ($verdict !== true) {
                    break;
                }
                $now = microtime(true);
            }
        }
    }

    /**
     * A single POP against one queue (one round).
     *
     * Key differences from pull: **no lookup, no consumption offset commit** (progress is tracked by the
     * broker-side checkpoint, confirmation relies on ack alone); a popped message is delivered for
     * consumption inline; ``POLLING_NOT_FOUND`` is a normal state.
     */
    private function popOnce(MessageQueue $mq, string $key, float $now): bool
    {
        if (!$this->started) {
            return false;
        }
        $client = $this->requireClient();
        $invisible = $this->popInvisibleTime;
        if ($invisible < ConsumerDefaults::MIN_POP_INVISIBLE_TIME
            || $invisible > ConsumerDefaults::MAX_POP_INVISIBLE_TIME) {
            $invisible = 60000;
        }
        $initMode = $this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET
            ? ConsumerDefaults::CONSUME_INIT_MODE_MIN
            : ConsumerDefaults::CONSUME_INIT_MODE_MAX;
        $pq = $this->popQueues[$key] ?? null;
        if ($pq === null || $pq->isDropped()) {
            return false;
        }
        // Stamp the time as soon as the pop starts: neither flow control nor idle backoff should let a
        // loop that is still running be judged stalled.
        $now = microtime(true);
        $this->lastPullTable[$key] = $now;
        $pq->lastPopTimestamp = $now;
        if ($this->paused) {
            return false;
        }
        $sub = $this->subscriptionData[$mq->topic] ?? null;
        if ($sub === null) {
            return false;
        }
        if ($pq->waitAckCount() > $this->popThresholdForQueue) {
            return false;
        }
        // Idle backoff (PHP short-poll adaptation).
        if (isset($this->idleUntil[$key]) && $now < $this->idleUntil[$key]) {
            return false;
        }
        $began = microtime(true);
        try {
            $result = $client->popMessage(
                $this->consumerGroup,
                $mq->topic,
                $mq->queueId,
                maxMsgNums: $this->popBatchNums,
                invisibleTime: $invisible,
                pollTime: 0,             // PHP short polling (intentional deviation, see the class header comment)
                initMode: $initMode,
                exp: ($sub->subString ?? '') !== '' ? $sub->subString : '*',
                expType: $sub->expressionType,
                order: false,
                brokerName: $mq->brokerName,
                timeoutMillis: $this->popTimeoutMillis,
            );
        } catch (RemotingTimeoutException $e) {
            Logger::debug("pop timeout for $mq (will retry): " . $e->getMessage());
            $this->idleUntil[$key] = microtime(true) + 0.5;
            return false;
        } catch (\Throwable $e) {
            Logger::debug("pop error for $mq: " . get_class($e) . ': ' . $e->getMessage());
            $this->idleUntil[$key] = microtime(true) + 0.5;
            return false;
        }
        // The queue was revoked by rebalance after the pop came back: this batch is **neither consumed nor
        // acked** (extremely rare in a single thread, the guard is copied as-is).
        if ($pq->isDropped()) {
            return false;
        }
        $found = $result->status === PopStatus::FOUND;
        if ($found) {
            if ($this->statsManager !== null) {
                $this->statsManager->incPullRt($this->consumerGroup, $mq->topic,
                    (int) ((microtime(true) - $began) * 1000));
                if ($result->msgFoundList !== []) {
                    $this->statsManager->incPullTps($this->consumerGroup, $mq->topic,
                        count($result->msgFoundList));
                }
            }
        }
        if ($found && $result->msgFoundList !== []) {
            $pq->incFoundMsg(count($result->msgFoundList));
            $kept = filter_messages_for_delivery(
                $this->consumerGroup,
                $this->filterMessageHookList,
                $mq,
                $sub,
                $result->msgFoundList,
                $this->unitMode,
            );
            if (count($kept) !== count($result->msgFoundList)) {
                $keptIds = [];
                foreach ($kept as $m) {
                    $keptIds[spl_object_id($m)] = true;
                }
                foreach ($result->msgFoundList as $msg) {
                    if (!isset($keptIds[spl_object_id($msg)])) {
                        $this->ackPopMsg($msg);
                        $pq->ack();
                    }
                }
                Logger::info(sprintf(
                    'pop filter dropped %d of %d messages (acked)',
                    count($result->msgFoundList) - count($kept),
                    count($result->msgFoundList),
                ));
            }
            if ($kept !== []) {
                $this->submitPopConsumeRequest($kept, $pq, $mq);
                return true;
            }
            return true;
        }
        // Empty result: an immediate return from short polling would turn into a hot loop, so fall back to
        // idle backoff.
        $this->idleUntil[$key] = microtime(true) + max(0, $this->popIntervalMillis) / 1000.0;
        return false;
    }

    private function submitPopConsumeRequest(array $msgs, PopProcessQueue $pq, MessageQueue $mq): void
    {
        if ($this->isOrderly()) {
            $this->submitPopOrderlyRequest($pq, $mq);
            return;
        }
        $size = max(1, $this->consumeMessageBatchMaxSize);
        $batches = array_chunk($msgs, $size) ?: [$msgs];
        foreach ($batches as $batch) {
            if ($batch === []) {
                continue;
            }
            $this->consumePopBatch($batch, $pq, $mq);
        }
    }

    private function submitPopOrderlyRequest(PopProcessQueue $pq, MessageQueue $mq, bool $force = false): void
    {
        $key = self::mqKey($mq);
        $isNew = !isset($this->popOrderlyRequests[$key]);
        if ($isNew) {
            $this->popOrderlyRequests[$key] = true;
        }
        if (!$force && !$isNew) {
            return;
        }
        $this->runPopOrderlyRequest($pq, $mq);
    }

    private function runPopOrderlyRequest(PopProcessQueue $pq, MessageQueue $mq): void
    {
        if ($pq->isDropped()) {
            Logger::warning("run, message queue not be able to consume, because it's dropped. $mq");
            unset($this->popOrderlyRequests[self::mqKey($mq)]);
            return;
        }
    }

    public function popOrderlyRequestCount(): int
    {
        return count($this->popOrderlyRequests);
    }

    private function consumePopBatch(array $msgs, PopProcessQueue $pq, MessageQueue $mq): void
    {
        if ($pq->isDropped() || $msgs === []) {
            return;
        }
        $popTime = 0;
        $invisible = 0;
        try {
            $seg = ExtraInfoUtil::split($msgs[0]->getProperty(MessageConst::PROPERTY_POP_CK));
            $popTime = ExtraInfoUtil::getPopTime($seg);
            $invisible = ExtraInfoUtil::getInvisibleTime($seg);
        } catch (\Throwable $e) {
            Logger::debug("parse pop ck failed for $mq, treat as not timed out: " . $e->getMessage());
        }

        if ($this->isPopTimeout($msgs, $popTime, $invisible)) {
            // Already past invisibleTime: the ack would not be accepted either, so abandon this batch
            // (wait for the broker to revive and redeliver it)
            Logger::debug(sprintf('pop timeout, abort consume for %s: popTime=%s invisible=%s',
                (string) $mq, $popTime, $invisible));
            $pq->decFoundMsg(-count($msgs));
            return;
        }

        $this->resetRetryTopicAndNamespace($msgs);
        $context = new ConsumeConcurrentlyContext($mq);
        $context->ackIndex = count($msgs) - 1;
        $hookCtx = null;
        if ($this->consumeMessageHookList !== []) {
            $hookCtx = $this->buildConsumeHookContext($msgs, $mq);
            $this->executeConsumeHookBefore($hookCtx);
        }
        $beginMs = UtilAll::currentTimeMillis();
        $hasException = false;
        try {
            foreach ($msgs as $m) {
                MessageAccessor::setConsumeStartTimestamp($m, UtilAll::currentTimeMillis());
            }
            $status = $this->messageListener->consumeMessage($msgs, $context);
        } catch (\Throwable $e) {
            Logger::debug('pop listener error, treat as RECONSUME_LATER: ' . $e->getMessage());
            $status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
            $hasException = true;
        }
        $rawStatus = $status;
        if ($status === null) {
            Logger::warning(sprintf(
                'consumeMessage return null, Group: %s Msgs: %d MQ: %s',
                $this->consumerGroup,
                count($msgs),
                (string) $mq,
            ));
            $status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
        }
        $this->recordConsumeStats(
            $mq->topic,
            count($msgs),
            $beginMs,
            failed: $status === ConsumeConcurrentlyStatus::RECONSUME_LATER,
        );
        $this->finishConsumeHook(
            $hookCtx,
            $rawStatus,
            $hasException,
            $beginMs,
            failed: $status === ConsumeConcurrentlyStatus::RECONSUME_LATER,
            succeeded: $status === ConsumeConcurrentlyStatus::CONSUME_SUCCESS,
            hookStatus: $status,
        );

        if ($pq->isDropped() || $this->isPopTimeout($msgs, $popTime, $invisible)) {
            // The queue was revoked or it already timed out while consuming: the result is no longer processed
            $pq->decFoundMsg(-count($msgs));
            return;
        }
        $this->processPopConsumeResult($status, $context, $msgs, $pq, $mq);
    }

    private static function isPopTimeout(array $msgs, int $popTime, int $invisible): bool
    {
        if ($msgs === [] || $popTime <= 0 || $invisible <= 0) {
            return true;
        }
        return UtilAll::currentTimeMillis() - $popTime >= $invisible;
    }

    private function processPopConsumeResult(
        ConsumeConcurrentlyStatus $status,
        ConsumeConcurrentlyContext $context,
        array $msgs,
        PopProcessQueue $pq,
        MessageQueue $mq,
    ): void {
        $ackIndex = $context->ackIndex;
        if ($status === ConsumeConcurrentlyStatus::CONSUME_SUCCESS) {
            if ($ackIndex >= count($msgs)) {
                $ackIndex = count($msgs) - 1;
            }
        } else {
            // RECONSUME_LATER: not a single message is acked
            $ackIndex = -1;
        }

        for ($i = 0; $i <= $ackIndex; $i++) {
            $this->ackPopMsg($msgs[$i]);
            $pq->ack();
        }

        for ($i = $ackIndex + 1; $i < count($msgs); $i++) {
            $pq->ack();
            $msg = $msgs[$i];
            if ($this->maxReconsumeTimes >= 0 && $msg->getReconsumeTimes() >= $this->maxReconsumeTimes) {
                $this->checkNeedAckOrDelay($msg);
                continue;
            }
            $this->changePopInvisibleTime($msg, $context->delayLevelWhenNextConsume);
        }
    }

    private function checkNeedAckOrDelay(MessageExt $msg): void
    {
        $table = $this->popDelayLevel;
        $msgDelayTime = UtilAll::currentTimeMillis() - $msg->bornTimestamp;
        if ($msgDelayTime > $table[count($table) - 1] * 1000 * 2) {
            Logger::warning('pop consume too many times, ack and drop: key=' . (string) ($msg->getKeys() ?? ''));
            $this->ackPopMsg($msg);
            return;
        }
        $level = count($table) - 1;
        while ($level >= 0) {
            if ($msgDelayTime >= $table[$level] * 1000) {
                $level += 1;
                break;
            }
            $level -= 1;
        }
        $this->changePopInvisibleTime($msg, $level);
    }

    /**
     * Extracts the (topic, brokerName, queueId, offset, ck) needed to ack / extend the invisible time
     * from POP_CK.
     *
     * ⚠ Neither of the two may be taken for granted:
     *  1. the topic must be restored with ``ExtraInfoUtil::getRealTopic`` according to the CK retryFlag;
     *  2. the address must be looked up by the brokerName inside the CK, not by querying the route for the
     *     topic (a retry topic usually has no dedicated route table entry).
     *
     * @return array{0:string,1:string,2:int,3:int,4:string}|null
     */
    private function popCkTarget(MessageExt $msg): ?array
    {
        $ck = $msg->getProperty(MessageConst::PROPERTY_POP_CK);
        if ($ck === null || $ck === '') {
            Logger::debug('pop message without POP_CK, cannot ack: ' . (string) $msg->msgId);
            return null;
        }
        try {
            $seg = ExtraInfoUtil::split($ck);
            $brokerName = ExtraInfoUtil::getBrokerName($seg);
            $queueId = ExtraInfoUtil::getQueueId($seg);
            $offset = ExtraInfoUtil::getQueueOffset($seg);
            $retry = ExtraInfoUtil::getRetry($seg);
        } catch (\Throwable $e) {
            Logger::debug("bad POP_CK $ck: " . $e->getMessage());
            return null;
        }
        $topic = ExtraInfoUtil::getRealTopic($msg->topic, $this->consumerGroup, $retry);
        return [$topic, $brokerName, $queueId, $offset, $ck];
    }

    private function ackPopMsg(MessageExt $msg): void
    {
        $target = $this->popCkTarget($msg);
        if ($target === null) {
            return;
        }
        [$topic, $brokerName, $queueId, $offset, $ck] = $target;
        try {
            $client = $this->requireClient();
            $client->ackMessage(
                $this->consumerGroup,
                $topic,
                $queueId,
                $ck,
                $offset,
                brokerName: $brokerName,
                addr: $client->popAckAddrFor($brokerName, $topic, $queueId),
            );
        } catch (\Throwable $e) {
            // A failed ack is not fatal: the broker will revive and redeliver the message once invisibleTime expires
            Logger::debug('ack failed for ' . $msg->msgId . ': ' . $e->getMessage());
        }
    }

    private function changePopInvisibleTime(MessageExt $msg, int $delayLevel): void
    {
        $target = $this->popCkTarget($msg);
        if ($target === null) {
            return;
        }
        [$topic, $brokerName, $queueId, $offset, $ck] = $target;
        if ($delayLevel === 0) {
            $delayLevel = $msg->getReconsumeTimes();
        }
        $table = $this->popDelayLevel;
        $delaySecond = $delayLevel >= count($table)
            ? $table[count($table) - 1]
            : $table[max(0, $delayLevel)];
        try {
            $client = $this->requireClient();
            $client->changeInvisibleTime(
                $this->consumerGroup,
                $topic,
                $queueId,
                $ck,
                $offset,
                $delaySecond * 1000,
                brokerName: $brokerName,
                addr: $client->popAckAddrFor($brokerName, $topic, $queueId),
            );
        } catch (\Throwable $e) {
            Logger::debug('change invisible time failed for ' . $msg->msgId . ': ' . $e->getMessage());
        }
    }
}
