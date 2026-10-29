<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\ExpressionType;
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
use RocketMQ\Remoting\Protocol\ConsumerSendMsgBackRequestHeader;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\GetEarliestMsgStoretimeRequestHeader;
use RocketMQ\Remoting\Protocol\GetEarliestMsgStoretimeResponseHeader;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\ConsumeType;
use RocketMQ\Remoting\Protocol\MessageModel;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\NamespaceUtil;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\ConsumerData;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Remoting\SocksProxyConfig;

// Module-level helper functions are autoloaded together with the ConsumerSupport.php classes
// (same guard as in PushConsumer).
if (!function_exists(__NAMESPACE__ . '\\client_side_tag_filter')) {
    require_once __DIR__ . '/ConsumerSupport.php';
}

/**
 * Pull-mode consumer, following the classic remoting protocol's PULL_MESSAGE path.
 *
 * Essential difference from the push consumer: the caller pulls queue by queue itself via
 * ``pull($mq, $offset)`` and manages offsets itself; there is no local buffer, no background
 * pull thread and no rebalance, so a threadless model fits it naturally with zero PHP
 * adaptation drift.
 */
final class DefaultMQPullConsumer
{
    public string $consumerGroup;
    public string $namespace = '';
    public string $instanceName = MixAll::DEFAULT_INSTANCE_NAME;
    public ?string $clientId = null;
    /** unitName/unitMode/enableStreamRequestType: the three switches taking part in clientId assembly, unitization marking and request-code selection. */
    public ?string $unitName = null;
    public bool $unitMode = false;
    /**
     * The pull-mode consumer fixes the initial value of ``enableStreamRequestType`` to true:
     * it takes part in clientId assembly, and its default differs from the push consumer's
     * false; the two initial values must not be mixed up.
     */
    public bool $enableStreamRequestType = true;
    /**
     * 5.x new namespace: when non-empty, **every** request carries
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
    public string $messageModel = MessageModel::CLUSTERING;
    public int $brokerSuspendMaxTimeMillis = 20000;
    public int $consumerPullTimeoutMillis = 10000;
    public int $consumerTimeoutMillisWhenSuspend = 30000;
    /**
     * Retry ceiling reported with message redelivery: pull mode's own default is 16, and
     * -1 ("unlimited") is reported as 16 as well.
     */
    public int $maxReconsumeTimes = 16;
    /** Route refresh period (passed through only once when start() builds the MQClientInstance). */
    public int $pollNameServerInterval = 30000;
    /** @var list<string> */
    public array $nameServerAddrs = [];
    public mixed $rpcHook = null;
    /** @var array<string,bool> */
    public array $registerTopics = [];
    /** TLS switch (null = defer to env ROCKETMQ_TLS_ENABLE). */
    public ?bool $tlsEnable = null;
    /** TLS details (caCert/clientCert/clientKey/serverName), semantics in RemotingClient::$tlsOptions. */
    public ?array $tlsOptions = null;
    public ?SocksProxyConfig $socksProxy = null;
    public ?MessageQueueListener $messageQueueListener = null;
    /**
     * Queue allocation strategy (AVG by default; setters/getters do not validate values).
     * In this pull-mode port the caller manages queues itself, so it exists purely as
     * configuration and is validated by start().
     */
    public ?AllocateMessageQueueStrategy $allocateMessageQueueStrategy;
    /** @var list<object> Pre-delivery filter hooks, registered into pull-result processing at start() */
    public array $filterMessageHookList = [];
    /** mqKey -> brokerId: suggested next-hop node table echoed back by the broker in pull responses */
    private array $pullFromWhichNode = [];
    /**
     * The **key set** of the same table (mqKey -> MessageQueue). The table key is a string and
     * offset/node decisions need no queue object, but fetchMessageQueuesInBalance has to hand
     * back "the queues this instance actually pulled" to the caller, so the object is kept
     * along (mqKey is a direct topic+brokerName+queueId concatenation; reverse parsing is
     * unreliable).
     */
    private array $pulledQueues = [];
    // ---- Consumer heartbeat (broker-side subscription registration period, 30s by default) ----
    public bool $heartbeatEnabled = true;
    public int $heartbeatIntervalMillis = 30000;
    private int $heartbeatCount = 0;
    private float $nextHeartbeatAt = 0.0;
    public ?MQClientInstance $mqClient = null;
    private bool $started = false;

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
        $this->allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
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

    /**
     * Server-side namespace (`nsd`/`ns` extension headers), topic/group names unchanged;
     * unrelated to the mechanism of that `$namespace` ("client prefixes `namespace%`").
     * Changing it after start() also takes effect, the hook reads
     * {@see MQClientInstance::$namespaceV2} live on every request.
     */
    public function setNamespaceV2(string $namespaceV2): void
    {
        $this->namespaceV2 = $namespaceV2;
        if ($this->mqClient !== null) {
            $this->mqClient->namespaceV2 = $namespaceV2;
        }
    }

    /** Read the server-side namespace (the one used by the `nsd`/`ns` extension headers). */
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

    public function setMaxReconsumeTimes(int $n): void
    {
        $this->maxReconsumeTimes = $n;
    }

    public function getMaxReconsumeTimes(): int
    {
        return $this->maxReconsumeTimes;
    }

    public function setMessageQueueListener(MessageQueueListener $listener): void
    {
        $this->messageQueueListener = $listener;
    }

    /** Set the queue allocation strategy: the setter does not validate; start() owns validity. */
    public function setAllocateMessageQueueStrategy(?AllocateMessageQueueStrategy $strategy): void
    {
        $this->allocateMessageQueueStrategy = $strategy;
    }

    public function registerTopic(string $topic): void
    {
        $this->registerTopics[$topic] = true;
    }

    public function registerFilterMessageHook(object $hook): void
    {
        $this->filterMessageHookList[] = $hook;
    }

    // ---------------- Pre-delivery filter hooks ----------------

    /**
     * Pull mode also runs filter hooks (pull results pass the hook chain once before delivery).
     * Tag filtering in pull mode is left to the caller (with no subscriptionData the tagsSet
     * is empty, so nothing is filtered); only the hooks run here.
     *
     * @param list<MessageExt> $msgs
     * @return list<MessageExt>
     */
    private function filterMessagesForDelivery(MessageQueue $mq, array $msgs): array
    {
        if ($msgs === [] || $this->filterMessageHookList === []) {
            return $msgs;
        }
        $context = new FilterMessageContext($this->consumerGroup, array_values($msgs), $mq);
        $context->unitMode = $this->unitMode;
        execute_filter_hooks($this->filterMessageHookList, $context);
        return array_values($context->msgList);
    }

    // ---------------- Heartbeat (register the consumer group with the broker) ----------------

    /**
     * Prepare broker addresses for heartbeat: pull routes of registerTopics once and register
     * them as "in use".
     */
    private function refreshRouteForHeartbeat(): void
    {
        $client = $this->requireClient();
        $topics = array_keys($this->registerTopics);
        sort($topics);
        foreach ($topics as $topic) {
            $client->registerTopicInUse($topic);
            try {
                $client->getTopicPublishInfo($topic);
            } catch (MQClientException $e) {
                Logger::debug("refresh route for $topic failed: " . $e->getMessage());
            }
        }
    }

    private function buildHeartbeat(): HeartbeatData
    {
        $hb = new HeartbeatData($this->clientId ?? '');
        $cd = new ConsumerData(
            $this->consumerGroup,
            ConsumeType::CONSUME_ACTIVELY,
            $this->messageModel,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
        );
        $cd->unitMode = $this->unitMode;
        $topics = array_keys($this->registerTopics);
        sort($topics);
        foreach ($topics as $topic) {
            $sub = FilterAPI::buildSubscriptionData($topic, FilterAPI::SUB_ALL);
            $sub->setSubVersion(0);
            $cd->subscriptionDataSet[] = $sub;
        }
        $hb->consumerDataSet[] = $cd;
        return $hb;
    }

    /** Send one heartbeat of this group to all known brokers (including slaves), returning the success count. */
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

    /** Heartbeat-driven on schedule (the loop body of the background heartbeat loop; skippable with no subscription). */
    public function tick(?int $nowMillis = null): void
    {
        if (!$this->started) {
            return;
        }
        $nowMs = $nowMillis ?? UtilAll::currentTimeMillis();
        $this->mqClient?->tick($nowMs);
        // Asynchronous pulls have no thread of their own: every pull callback the broker already
        // answered is dispatched here, inside the caller's loop.
        $this->pumpAsyncPulls();
        $now = $nowMs / 1000.0;
        if ($this->heartbeatEnabled && $now >= $this->nextHeartbeatAt) {
            $this->nextHeartbeatAt = $now + $this->heartbeatIntervalMillis / 1000.0;
            try {
                $this->sendHeartbeatToAllBroker();
            } catch (\Throwable $e) {
                Logger::debug('heartbeat loop error: ' . $e->getMessage());
            }
        }
    }

    // ---------------- Lifecycle ----------------

    public function start(): void
    {
        if ($this->started) {
            return;
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
        if ($this->allocateMessageQueueStrategy === null) {
            throw new MQClientException('allocateMessageQueueStrategy is null');
        }
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
        $this->started = true;
        // Heartbeat: refresh the routes of registerTopics first, then send one round synchronously
        // so the broker recognizes this group immediately.
        $this->refreshRouteForHeartbeat();
        try {
            $this->sendHeartbeatToAllBroker();
        } catch (\Throwable $e) {
            Logger::debug('initial heartbeat failed: ' . $e->getMessage());
        }
        $this->nextHeartbeatAt = microtime(true) + $this->heartbeatIntervalMillis / 1000.0;
    }

    public function shutdown(): void
    {
        if (!$this->started) {
            return;
        }
        $this->started = false;
        $client = $this->mqClient;
        if ($client !== null) {
            try {
                $client->unregisterClientAllBrokers($this->clientId ?? '', '', $this->consumerGroup);
            } catch (\Throwable $e) {
                Logger::debug('unregister on shutdown failed: ' . $e->getMessage());
            }
            $client->shutdown();
        }
    }

    private function requireClient(): MQClientInstance
    {
        if (!$this->started || $this->mqClient === null) {
            throw new MQClientException('consumer not started, call start() first');
        }
        return $this->mqClient;
    }

    // ---------------- Pulling ----------------

    /** @return list<MessageQueue> */
    public function fetchSubscribeMessageQueues(string $topic): array
    {
        return $this->requireClient()->getTopicSubscribeInfo($topic);
    }

    public function fetchMessageQueuesInBalance(string $topic): array
    {
        $client = $this->requireClient();
        $pulled = array_values(array_filter(
            $this->pulledQueues,
            static fn(MessageQueue $mq): bool => $mq->topic === $topic,
        ));
        if ($this->messageModel === MessageModel::BROADCASTING) {
            $subscribe = $client->getTopicSubscribeInfo($topic);
            $allocated = $subscribe !== [] ? $subscribe : $pulled;
            return $this->sortedQueues($allocated, $topic);
        }
        $allocated = null;
        try {
            $mqAll = $this->sortedQueues($client->getTopicSubscribeInfo($topic), $topic);
            $cidAll = $client->getConsumerIdListByGroup($topic, $this->consumerGroup);
            if ($mqAll !== [] && $cidAll !== null && $cidAll !== []) {
                sort($cidAll);
                $allocated = $this->allocateMessageQueueStrategy
                    ?->allocate($this->consumerGroup, $this->clientId ?? '', $mqAll, $cidAll);
            }
        } catch (\Throwable $e) {
            Logger::debug(sprintf('fetchMessageQueuesInBalance rebalance view unavailable: %s', $e->getMessage()));
        }
        if ($allocated === null) {
            Logger::debug(sprintf('fetchMessageQueuesInBalance: no route/consumer list for %s/%s, keep current assignment',
                $this->consumerGroup, $topic));
            $allocated = $pulled;
        }
        return $this->sortedQueues($allocated, $topic);
    }

    /** Scope by topic plus house sort order (topic -> brokerName -> queueId), keeping repeated calls stable. */
    private function sortedQueues(array $mqs, string $topic): array
    {
        $out = [];
        foreach ($mqs as $mq) {
            if ($mq instanceof MessageQueue && $mq->topic === $topic) {
                $out[implode("\0", AllocationHelper::mqSortKey($mq))] = $mq;
            }
        }
        ksort($out);
        return array_values($out);
    }

    public function pull(
        MessageQueue $mq,
        string $subExpression = '*',
        int $offset = 0,
        int $maxNums = 32,
        ?int $timeoutMillis = null,
    ): PullResult {
        return $this->doPull(
            $mq,
            build_pull_subscription_data($mq, $subExpression),
            $offset,
            $maxNums,
            false,
            $timeoutMillis ?? $this->consumerPullTimeoutMillis,
        );
    }

    /**
     * Selector flavour of {@see pull()}: the expression type of the selector is what goes on
     * the wire, so a SQL92 expression is reported as SQL92 instead of being mistaken for a tag
     * list; a TAG selector keeps the tag derivation of {@see pull()}.
     */
    public function pullBySelector(
        MessageQueue $mq,
        MessageSelector $selector,
        int $offset = 0,
        int $maxNums = 32,
        ?int $timeoutMillis = null,
    ): PullResult {
        return $this->doPull(
            $mq,
            build_pull_subscription_data($mq, null, $selector),
            $offset,
            $maxNums,
            false,
            $timeoutMillis ?? $this->consumerPullTimeoutMillis,
        );
    }

    public function pullBlockIfNotFound(
        MessageQueue $mq,
        string $subExpression,
        int $offset,
        int $maxNums,
    ): PullResult {
        return $this->doPull(
            $mq,
            build_pull_subscription_data($mq, $subExpression),
            $offset,
            $maxNums,
            true,
            $this->consumerTimeoutMillisWhenSuspend,
        );
    }

    /** Selector flavour of {@see pullBlockIfNotFound()} (same expression-type rule). */
    public function pullBlockIfNotFoundBySelector(
        MessageQueue $mq,
        MessageSelector $selector,
        int $offset,
        int $maxNums,
    ): PullResult {
        return $this->doPull(
            $mq,
            build_pull_subscription_data($mq, null, $selector),
            $offset,
            $maxNums,
            true,
            $this->consumerTimeoutMillisWhenSuspend,
        );
    }

    /**
     * The single pull body behind the four entry points above: ``$block`` picks long polling
     * (suspend bit on, the suspend budgets apply) versus short polling, and the subscription
     * decides both the reported expression and its expression type.
     */
    private function doPull(
        MessageQueue $mq,
        SubscriptionData $sub,
        int $offset,
        int $maxNums,
        bool $block,
        int $timeoutMillis,
    ): PullResult {
        $client = $this->requireClient();
        $isTag = $sub->expressionType === ExpressionType::TAG;
        // block=true: suspend=True makes the broker hang until messages arrive; timeout uses consumerTimeoutMillisWhenSuspend.
        // The class-filter bit stays clear on this path: it asks for the legacy filter-server route.
        // The subscription's own expression type is still what the header reports.
        $sysFlag = PullSysFlag::buildSysFlag(false, $block, true, false);
        $key = self::mqKey($mq);
        $result = $client->pullMessage(
            $this->consumerGroup,
            $mq,
            $offset,
            $maxNums,
            $sysFlag,
            0,
            $sub->subString ?? '*',
            // A tag subscription keeps the version-0 shape it has always sent; a non-tag one
            // reports the subscription's own version.
            $isTag ? 0 : $sub->subVersion,
            $sub->expressionType,
            timeoutMillis: $timeoutMillis,
            maxMsgBytes: -1,
            suspendTimeoutMillis: $block ? $this->brokerSuspendMaxTimeMillis : 15000,
            brokerId: $this->pullFromWhichNode[$key] ?? MixAll::MASTER_ID,
        );
        $this->pullFromWhichNode[$key] = $result->suggestWhichBrokerId ?? MixAll::MASTER_ID;
        if (!$block) {
            // Only the short-poll entry points book the queue as "pulled from" (the long-poll
            // ones never did, and fetchMessageQueuesInBalance reads exactly this key set).
            $this->pulledQueues[$key] = $mq;
        }
        if ($result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
            $result->msgFoundList = $this->filterMessagesForDelivery($mq, $result->msgFoundList);
        }
        return $result;
    }

    // ---------------- Offset management ----------------

    // ---------------- Asynchronous pulling ----------------

    /**
     * Asynchronous form of {@see pull()}: the request goes out and this returns immediately;
     * the outcome arrives on ``$callback`` once the caller pumps (``tick()`` drives the pump).
     *
     * The frame is built by the same body as the synchronous pull, so the wire shape —
     * subscription text, expression type, version stamp, sysFlag bits — is identical.
     *
     * @throws MQClientException when the arguments are unusable (not started, bad offset, no callback)
     */
    public function pullAsync(
        MessageQueue $mq,
        string $subExpression,
        int $offset,
        int $maxNums,
        ?PullCallback $callback = null,
        ?int $timeoutMillis = null,
    ): void {
        $this->doPullAsync(
            $mq,
            build_pull_subscription_data($mq, $subExpression),
            $offset,
            $maxNums,
            false,
            $timeoutMillis ?? $this->consumerPullTimeoutMillis,
            $callback,
        );
    }

    /** Selector flavour of {@see pullAsync()} (same expression-type rule as the sync selector pulls). */
    public function pullAsyncBySelector(
        MessageQueue $mq,
        MessageSelector $selector,
        int $offset,
        int $maxNums,
        ?PullCallback $callback = null,
        ?int $timeoutMillis = null,
    ): void {
        $this->doPullAsync(
            $mq,
            build_pull_subscription_data($mq, null, $selector),
            $offset,
            $maxNums,
            false,
            $timeoutMillis ?? $this->consumerPullTimeoutMillis,
            $callback,
        );
    }

    /** Asynchronous long-polling pull: the suspend bit goes on and the suspend budget applies. */
    public function pullBlockIfNotFoundAsync(
        MessageQueue $mq,
        string $subExpression,
        int $offset,
        int $maxNums,
        ?PullCallback $callback = null,
    ): void {
        $this->doPullAsync(
            $mq,
            build_pull_subscription_data($mq, $subExpression),
            $offset,
            $maxNums,
            true,
            $this->consumerTimeoutMillisWhenSuspend,
            $callback,
        );
    }

    /**
     * The single asynchronous pull body: same subscription/sysFlag rules as {@see doPull()},
     * same post-processing (suggested broker, pulled-queue bookkeeping, filter hooks), only the
     * transport differs.
     */
    private function doPullAsync(
        MessageQueue $mq,
        SubscriptionData $sub,
        int $offset,
        int $maxNums,
        bool $block,
        int $timeoutMillis,
        ?PullCallback $callback,
    ): void {
        $client = $this->requireClient();
        if ($offset < 0) {
            throw new MQClientException('offset < 0');
        }
        if ($maxNums <= 0) {
            throw new MQClientException('maxNums <= 0');
        }
        if ($callback === null) {
            throw new MQClientException('pullCallback is null');
        }
        // The topic of an in-flight pull joins the subscription set the heartbeat registers.
        $this->registerTopic($mq->topic);
        $isTag = $sub->expressionType === ExpressionType::TAG;
        // The class-filter bit stays clear here too, and a tag subscription keeps version 0.
        $sysFlag = PullSysFlag::buildSysFlag(false, $block, true, false);
        $key = self::mqKey($mq);
        $client->pullMessageAsync(
            function (?PullResult $result, ?\Throwable $failure) use ($callback, $mq, $key, $block): void {
                try {
                    if ($result === null) {
                        $callback->onException($failure ?? new MQClientException('pullAsync unknown exception'));
                        return;
                    }
                    $this->pullFromWhichNode[$key] = $result->suggestWhichBrokerId ?? MixAll::MASTER_ID;
                    if (!$block) {
                        $this->pulledQueues[$key] = $mq;
                    }
                    if ($result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
                        $result->msgFoundList = $this->filterMessagesForDelivery($mq, $result->msgFoundList);
                    }
                    $callback->onSuccess($result);
                } catch (\Throwable $inner) {
                    $callback->onException($inner);
                }
            },
            $this->consumerGroup,
            $mq,
            $offset,
            $maxNums,
            $sysFlag,
            0,
            $sub->subString ?? '*',
            $isTag ? 0 : $sub->subVersion,
            $sub->expressionType,
            timeoutMillis: $timeoutMillis,
            maxMsgBytes: -1,
            suspendTimeoutMillis: $block ? $this->brokerSuspendMaxTimeMillis : 15000,
            brokerId: $this->pullFromWhichNode[$key] ?? MixAll::MASTER_ID,
        );
    }

    /**
     * Read the responses the asynchronous pulls are waiting for, up to the given pump budget.
     * Callbacks fire from inside this call; it returns as soon as nothing is in flight.
     */
    public function pumpAsyncPulls(int $timeoutMillis = 20): void
    {
        $this->requireClient()->remotingClient->waitResponses($timeoutMillis);
    }

    public function fetchConsumeOffset(MessageQueue $mq): ?int
    {
        $this->requireClient();
        if ($this->messageModel === MessageModel::BROADCASTING) {
            // Broadcasting keeps its offsets client-side; the broker is never asked.
            $table = read_local_offset_table($this->clientId ?? '', $this->consumerGroup) ?? [];
            $key = self::mqKey($mq);
            return array_key_exists($key, $table) ? $table[$key] : null;
        }
        return $this->mqClient->queryConsumerOffset($this->consumerGroup, $mq);
    }

    public function updateConsumeOffset(MessageQueue $mq, int $offset): void
    {
        $this->requireClient();
        if ($this->messageModel === MessageModel::BROADCASTING) {
            $key = self::mqKey($mq);
            persist_local_offset_entries($this->clientId ?? '', $this->consumerGroup, [$key => $offset], [$key => $mq]);
            return;
        }
        $this->mqClient->updateConsumerOffset($this->consumerGroup, $mq, $offset);
    }

    public function searchOffset(MessageQueue $mq, int $timestamp): int
    {
        return $this->requireClient()->searchOffsetByTimestamp($mq, $timestamp);
    }

    public function maxOffset(MessageQueue $mq): int
    {
        return $this->requireClient()->getMaxOffset($mq);
    }

    public function minOffset(MessageQueue $mq): int
    {
        return $this->requireClient()->getMinOffset($mq);
    }

    public function earliestMsgStoreTime(MessageQueue $mq): int
    {
        $client = $this->requireClient();
        $addr = $client->adminAddrFor($mq);
        $header = new GetEarliestMsgStoretimeRequestHeader();
        $header->topic = $mq->topic;
        $header->queueId = $mq->queueId;
        $request = RemotingCommand::createRequestCommand(RequestCode::GET_EARLIEST_MSG_STORETIME, $header);
        $response = $client->remotingClient->invokeSync($client->vipAddrFor($addr), $request, 5000);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }
        $respHeader = new GetEarliestMsgStoretimeResponseHeader();
        $respHeader->fromExtFields($response->extFields);
        return $respHeader->timestamp ?? 0;
    }

    public function sendMessageBack(MessageExt $msg, int $delayLevel): void
    {
        $client = $this->requireClient();
        $maxReconsume = $this->maxReconsumeTimes === -1 ? 16 : $this->maxReconsumeTimes;
        try {
            // Slaves do not accept CONSUMER_SEND_MSG_BACK (master only), so only brokerId=0 counts here.
            $addr = $client->findBrokerAddressInPublish($msg->brokerName ?? '');
            if ($addr === null || $addr === '') {
                throw new MQClientException('Broker[' . ($msg->brokerName ?? '') . '] master node does not exist');
            }
            $header = new ConsumerSendMsgBackRequestHeader();
            $header->offset = $msg->commitLogOffset;
            $header->group = $this->consumerGroup;
            $header->delayLevel = $delayLevel;
            $header->originMsgId = (string) $msg->msgId;
            $header->originTopic = $msg->topic;
            $header->unitMode = $this->unitMode;
            $header->maxReconsumeTimes = $maxReconsume;
            $request = RemotingCommand::createRequestCommand(RequestCode::CONSUMER_SEND_MSG_BACK, $header);
            $response = $client->remotingClient->invokeSync($client->vipAddrFor($addr), $request, 3000);
            if ($response->code !== ResponseCode::SUCCESS) {
                throw new MQBrokerException($response->code, $response->remark ?? '');
            }
        } catch (\Throwable $e) {
            // A refused bounce (missing master, transport error, non-success answer) degrades to
            // a normal send to %RETRY%<group>, so the redelivery is not lost; only when that send
            // fails too does the ORIGINAL error reach the caller.
            Logger::error(sprintf(
                'Failed to send message back, consumerGroup=%s, brokerName=%s, msg=%s: %s; fallback to normal send',
                $this->consumerGroup,
                (string) $msg->brokerName,
                (string) $msg->msgId,
                $e->getMessage(),
            ));
            try {
                $this->sendRetryMessage($msg, $maxReconsume);
            } catch (\Throwable $fallbackError) {
                Logger::error(sprintf(
                    'Fallback normal send failed, consumerGroup=%s, msg=%s: %s',
                    $this->consumerGroup,
                    (string) $msg->msgId,
                    $fallbackError->getMessage(),
                ));
                throw $e;
            }
        }
    }

    // sendRetryMessage sends a failed delivery as a normal message to
    // %RETRY%<group> through the internal producer: body and properties copied,
    // the origin id falling back to the message id, the retry bookkeeping written,
    // and the half-message marker cleared so the broker does not treat the copy as
    // a transaction check-back message all over again.
    private function sendRetryMessage(MessageExt $msg, int $maxReconsumeTimes): void
    {
        $client = $this->requireClient();
        $newMsg = new Message(MixAll::getRetryTopic($this->consumerGroup), $msg->body);
        // PHP copies the array on assignment, so clearing a property below never
        // touches the source message.
        $newMsg->properties = $msg->properties;
        $newMsg->flag = $msg->flag;
        $originMsgId = MessageAccessor::getOriginMessageId($msg) ?? $msg->msgId;
        if ($originMsgId !== null && $originMsgId !== '') {
            MessageAccessor::setOriginMessageId($newMsg, $originMsgId);
        }
        MessageAccessor::putProperty($newMsg, MessageConst::PROPERTY_RETRY_TOPIC, $msg->topic);
        MessageAccessor::setReconsumeTime($newMsg, $msg->getReconsumeTimes() + 1);
        MessageAccessor::setMaxReconsumeTimes($newMsg, $maxReconsumeTimes);
        MessageAccessor::clearProperty($newMsg, MessageConst::PROPERTY_TRANSACTION_PREPARED);
        $newMsg->setDelayTimeLevel(3 + $msg->getReconsumeTimes());
        $publish = $client->getTopicPublishInfo($newMsg->topic, true);
        $mq = $publish->selectOneMessageQueue();
        if ($mq === null) {
            throw new MQClientException("no writable queue for retry topic $newMsg->topic");
        }
        // unitMode follows the consumer: without it, sent-back messages lose their unitization mark.
        $client->sendMessage(MixAll::CLIENT_INNER_PRODUCER_GROUP, $newMsg, $mq, 3000, 0, $this->unitMode);
    }

    public function createTopic(string $key, string $newTopic, int $queueNum = 4, int $topicSysFlag = 0): void
    {
        $this->requireClient()->createTopicInRoute($newTopic, $queueNum, $queueNum, 6);
    }

    private static function mqKey(MessageQueue $mq): string
    {
        return $mq->topic . $mq->brokerName . (string) $mq->queueId;
    }
}

final class DefaultLitePullConsumer
{
    public string $consumerGroup;
    public string $namespace = '';
    public string $instanceName = MixAll::DEFAULT_INSTANCE_NAME;
    public ?string $clientId = null;
    public ?string $unitName = null;
    public bool $unitMode = false;
    /** The lite-pull consumer fixes the initial value of ``enableStreamRequestType`` to true (it takes part in clientId assembly). */
    public bool $enableStreamRequestType = true;
    /**
     * 5.x new namespace: when non-empty, **every** request carries
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
    public string $messageModel = MessageModel::CLUSTERING;
    /** @var list<string> */
    public array $nameServerAddrs = [];
    public mixed $rpcHook = null;
    public ?bool $tlsEnable = null;
    /** TLS details (caCert/clientCert/clientKey/serverName), semantics in RemotingClient::$tlsOptions. */
    public ?array $tlsOptions = null;
    public ?SocksProxyConfig $socksProxy = null;

    /** Subscription table in subscribe mode (topic => sub_expression). */
    public array $subscription = [];
    /** @var array<string,SubscriptionData> */
    public array $subscriptionData = [];
    /** Tag filter expression for the specified topic in assign mode. */
    private array $assignSubExpr = [];
    /**
     * The lite consumer is in exactly one of these modes for its whole life. A call that would
     * switch it is refused: subscribe mode and assign mode build different subscription tables,
     * pull targets and heartbeats, so a mid-flight switch would leave the running halves
     * describing two different consumers.
     */
    private const MODE_NONE = 0;
    private const MODE_SUBSCRIBE = 1;
    private const MODE_ASSIGN = 2;
    private int $mode = self::MODE_NONE;

    /**
     * Service lifecycle: start is accepted only from CREATE_JUST. Once start has been
     * attempted the state never returns to CREATE_JUST, so a second start (after a failed
     * start or a shutdown) is refused instead of silently doing nothing.
     */
    private const SERVICE_CREATE_JUST = 0;
    private const SERVICE_START_FAILED = 1;
    private const SERVICE_RUNNING = 2;
    private const SERVICE_SHUTDOWN_ALREADY = 3;
    private int $serviceState = self::SERVICE_CREATE_JUST;
    /** @var array<string,MessageQueue> Assigned queues (key = mqKey) */
    private array $assigned = [];

    /** Rebalance algorithm (subscribe mode). */
    public AllocateMessageQueueStrategy $allocateMessageQueueStrategy;

    // Pull / poll configuration
    public string $consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
    /** Start point of CONSUME_FROM_TIMESTAMP (format yyyyMMddHHmmss), 30 minutes ago by default. */
    public string $consumeTimestamp;
    /**
     * Messages requested per pull round. The aggregate gate below budgets every pending round at a
     * whole batch of this size, so this is also that budget's multiplier, not only a pull size.
     */
    public int $pullBatchSize = 10;
    public int $pollTimeoutMillis = 5000;
    public int $pollNameServerInterval = 30000;
    public bool $autoCommit = true;
    /** Period of the auto-commit sweep; only a value of at least 1000 ms can be set, shorter ones are ignored. */
    public int $autoCommitIntervalMillis = 5000;
    public int $consumerTimeoutMillisWhenSuspend = 30000;
    public int $brokerSuspendMaxTimeMillis = 20000;
    public int $pullIntervalMillis = 50;  // Backoff on empty polls at queue tail, avoiding busy loops hammering the broker
    /**
     * Message-count budget for the **whole** local buffer: the aggregate cache gate. It budgets the
     * pull rounds whose messages the application has not taken yet -- one whole batch per round,
     * whether or not that round came back full -- so it asks "how much may still be on the way"
     * rather than "how much is cached now". A queue whose rounds are half empty therefore still
     * reserves its batch. When that projection is strictly greater than this number the application
     * is not draining at all, so no queue is worth pulling. A plain budget, never clamped: 0 means
     * "trip on the first pending round".
     */
    public int $pullThresholdForAll = 10000;
    /**
     * Message count still cached for **one** queue; strictly greater refuses that queue's pull.
     * A sibling queue over its own budget never stops this one. Never clamped: 0 trips on the
     * first cached message.
     */
    public int $pullThresholdForQueue = 1000;
    /**
     * MiB still cached for **one** queue, measured on the *stored* message size the decoder
     * records (never the body length) and compared as an integer number of MiB. Never clamped.
     */
    public int $pullThresholdSizeForQueue = 100;
    /**
     * Offset span of **one** queue's cached messages: its highest cached queue offset minus its
     * lowest. An empty or single-message cache has span 0 and can therefore never trip.
     */
    public int $consumeMaxSpan = 2000;

    // Runtime state
    public ?MQClientInstance $mqClient = null;
    private bool $started = false;
    private bool $running = false;

    /** @var list<MessageExt> Local buffer */
    private array $localBuffer = [];
    /** @var array<string,int> Pull cursor (key = mqKey) */
    private array $nextOffset = [];
    /** @var array<string,int> Consume cursor (advances only when poll() hands messages out) */
    private array $consumeOffset = [];
    private array $offsetTable = [];
    /** @var array<string,int> */
    private array $seekOffset = [];
    private float $nextAutoCommitDeadline = -1.0;
    /** @var array<string,bool> */
    private array $paused = [];
    /**
     * Per-queue cache accumulators (key = mqKey), the cache gates' O(1) view of the local buffer:
     * how many messages of this queue the buffer holds, how many **body** bytes, and the lowest
     * and highest cached queue offset -- with the offset -> refcount map behind the boundaries, so
     * a removal whose boundary offset lost its last message can re-pick the boundary without
     * walking the buffer, and the list of still-undrained pull rounds of this queue, which is what
     * the aggregate gate counts. Only the buffer's own primitives (``bufferAppend`` / ``bufferTake`` /
     * ``bufferRetain``) write these, in the same step as they move the buffer itself, so an
     * accumulator can never describe a buffer the buffer does not have; a queue that leaves the
     * assignment loses its record together with its cached messages.
     *
     * @var array<string,array{count:int,bytes:int,min:int,max:int,offsets:array<int,int>,rounds:list<int>}>
     */
    private array $queueCache = [];
    /** @var array<string,float> Due time of one queue's own-cache pause (seconds, microtime basis; the PHP form of a pull task re-scheduled 50 ms later) */
    private array $cacheFlowControlUntil = [];
    /** @var array<string,float> Due time of one queue's short pause after the broker answered FLOW_CONTROL(215) */
    private array $pullIdleUntil = [];
    /** @var int Trips of the aggregate (whole-buffer) cache gate */
    private int $flowControlForAllTimes = 0;
    /** @var int Trips of the per-queue count gate and the per-queue size gate, which share one budget */
    private int $flowControlForQueueTimes = 0;
    /** @var int Trips of the per-queue offset-span gate */
    private int $flowControlForSpanTimes = 0;
    private ?object $messageQueueListener = null;
    private float $lastRebalanceTs = 0.0;
    private float $nextHeartbeatAt = 0.0;
    /**
     * Comparison period of the queue-set change listener (milliseconds). 0 or negative is
     * clamped to a 1s floor: a period of 0 means querying full routes on every tick, which
     * runs the comparison as usual but turns tick into an RPC storm.
     */
    public int $topicMetadataCheckIntervalMillis = 30000;
    /** @var array<string,object|callable> Listeners, key = namespaced topic */
    private array $topicChangeListeners = [];
    /** @var array<string,array<string,bool>> Last snapshot: topic => {mqKey => true} */
    private array $messageQueuesForTopic = [];
    /**
     * Time of the next comparison round (seconds, microtime basis). 0 = not scheduled yet.
     * start() schedules it at now+10s (the first query does not steal RPC from the startup
     * path); it is public so unit tests can pull the first query forward without really
     * waiting 10s.
     */
    public float $nextMetadataCheckAt = 0.0;

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
        $this->consumeTimestamp = UtilAll::defaultConsumeTimestamp();
        $this->concurrentHeartbeatThreadPoolSize = MQClientInstance::availableProcessors();
        $this->allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    }

    // ---------------- Namespace / configuration ----------------

    private function withNamespace(string $topic): string
    {
        if ($this->namespace !== '' && !str_starts_with($topic, $this->namespace . '%')) {
            return $this->namespace . '%' . $topic;
        }
        return $topic;
    }

    public function setNamesrvAddr(string $addr): void
    {
        $this->nameServerAddrs = array_values(array_filter(array_map('trim', explode(';', $addr))));
    }

    public function setInstanceName(string $name): void
    {
        $this->instanceName = $name;
    }

    public function setConsumeFromWhere(string $where): void
    {
        $this->consumeFromWhere = $where;
    }

    public function setPullBatchSize(int $n): void
    {
        $this->pullBatchSize = max(1, $n);
    }

    public function setPollTimeoutMillis(int $ms): void
    {
        $this->pollTimeoutMillis = max(0, $ms);
    }

    public function setAutoCommit(bool $auto): void
    {
        $this->autoCommit = $auto;
    }

    /**
     * Auto-commit interval in milliseconds. A value below 1000 is ignored outright: whatever
     * interval was already running keeps running, rather than being pulled down to the floor.
     */
    public function setAutoCommitIntervalMillis(int $ms): void
    {
        if ($ms >= 1000) {
            $this->autoCommitIntervalMillis = $ms;
        }
    }

    public function getAutoCommitIntervalMillis(): int
    {
        return $this->autoCommitIntervalMillis;
    }

    /**
     * Message-count budget of the whole local buffer (aggregate cache gate). Stored as given:
     * these are plain budgets, so 0 is a legal value that trips on the first cached message.
     */
    public function setPullThresholdForAll(int $n): void
    {
        $this->pullThresholdForAll = $n;
    }

    public function getPullThresholdForAll(): int
    {
        return $this->pullThresholdForAll;
    }

    /** Message-count budget of one queue's cached messages (no clamping, same rule as above). */
    public function setPullThresholdForQueue(int $n): void
    {
        $this->pullThresholdForQueue = $n;
    }

    public function getPullThresholdForQueue(): int
    {
        return $this->pullThresholdForQueue;
    }

    /** MiB budget of one queue's cached messages, counted on the stored size (no clamping). */
    public function setPullThresholdSizeForQueue(int $n): void
    {
        $this->pullThresholdSizeForQueue = $n;
    }

    public function getPullThresholdSizeForQueue(): int
    {
        return $this->pullThresholdSizeForQueue;
    }

    /** Offset-span budget of one queue's cached messages (no clamping). */
    public function setConsumeMaxSpan(int $n): void
    {
        $this->consumeMaxSpan = $n;
    }

    public function getConsumeMaxSpan(): int
    {
        return $this->consumeMaxSpan;
    }

    /**
     * Messages of this queue the local buffer still holds (handed to the application or not, both
     * stay in the buffer until ``poll()`` takes them away). Read from the same accumulator the
     * cache gates read, so an observation and a verdict can never disagree.
     */
    public function cachedMessageCountOf(MessageQueue $mq): int
    {
        return $this->queueCache[self::mqKey($mq)]['count'] ?? 0;
    }

    /** Body bytes of this queue's cached messages (the size gate's numerator, before the MiB division). */
    public function cachedByteSizeOf(MessageQueue $mq): int
    {
        return $this->queueCache[self::mqKey($mq)]['bytes'] ?? 0;
    }

    /** Highest minus lowest cached queue offset of this queue; 0 while it holds fewer than two messages. */
    public function cachedOffsetSpanOf(MessageQueue $mq): int
    {
        $stat = $this->queueCache[self::mqKey($mq)] ?? null;
        return $stat === null ? 0 : $stat['max'] - $stat['min'];
    }

    /** Messages cached across every queue right now -- what the application can still be handed. */
    public function cachedMessageCount(): int
    {
        return count($this->localBuffer);
    }

    /**
     * Pull rounds whose messages are still in the buffer, across every queue: the aggregate gate's
     * measurement. A round counts as long as any of its messages remain, whether it came back full
     * or with one message.
     */
    public function pendingPullRounds(): int
    {
        $rounds = 0;
        foreach ($this->queueCache as $stat) {
            $rounds += count($stat['rounds']);
        }
        return $rounds;
    }

    /** Trips of the aggregate gate; the push path's ``flowControlTriggeredCount()`` counterpart for this path. */
    public function flowControlForAllTimes(): int
    {
        return $this->flowControlForAllTimes;
    }

    /** Trips of the per-queue count gate and size gate, which feed this one counter. */
    public function flowControlForQueueTimes(): int
    {
        return $this->flowControlForQueueTimes;
    }

    /** Trips of the per-queue offset-span gate. */
    public function flowControlForSpanTimes(): int
    {
        return $this->flowControlForSpanTimes;
    }

    /**
     * Server-side namespace (`nsd`/`ns` extension headers), topic/group names unchanged;
     * unrelated to the mechanism of that `$namespace` ("client prefixes `namespace%`").
     * Changing it after start() also takes effect, the hook reads
     * {@see MQClientInstance::$namespaceV2} live on every request.
     */
    public function setNamespaceV2(string $namespaceV2): void
    {
        $this->namespaceV2 = $namespaceV2;
        if ($this->mqClient !== null) {
            $this->mqClient->namespaceV2 = $namespaceV2;
        }
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

    /** Read the server-side namespace (the one used by the `nsd`/`ns` extension headers). */
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

    // ---------------- Subscription / allocation ----------------

    public function subscribe(string $topic, string $subExpression = '*'): void
    {
        if (trim($topic) === '') {
            throw new \InvalidArgumentException('Topic can not be null or empty.');
        }
        $this->claimMode(self::MODE_SUBSCRIBE);
        $ns = $this->withNamespace($topic);
        $this->subscription[$ns] = $subExpression;
        // Build SubscriptionData synchronously so the heartbeat registers the tag subscription with the broker.
        try {
            $this->subscriptionData[$ns] = FilterAPI::buildSubscriptionData($ns, $subExpression);
        } catch (\Throwable) {
            unset($this->subscriptionData[$ns]);
        }
    }

    /**
     * Drops a topic's subscription AND the queues this instance holds for it. A queue left behind
     * would keep being pulled for a topic the caller has just left -- until a rebalance hands it
     * away in subscribe mode, and forever in assign mode, where nothing ever would.
     */
    public function unsubscribe(string $topic): void
    {
        if (trim($topic) === '') {
            return;
        }
        $ns = $this->withNamespace($topic);
        unset($this->subscription[$ns]);
        if (!isset($this->assignSubExpr[$ns])) {
            unset($this->subscriptionData[$ns]);
        }
        foreach ($this->assigned as $key => $mq) {
            if ($mq->topic !== $ns) {
                continue;
            }
            unset($this->assigned[$key], $this->nextOffset[$key], $this->consumeOffset[$key],
                $this->seekOffset[$key]);
            $this->dropQueueCache($key);
        }
    }

    public function setSubExpressionForAssign(string $topic, string $subExpression): void
    {
        if (trim($subExpression) === '') {
            throw new \InvalidArgumentException('subExpression can not be null or empty.');
        }
        if ($this->started) {
            throw new MQClientException('setSubExpressionForAssign only can be called before start.');
        }
        if (trim($topic) === '') {
            throw new \InvalidArgumentException('Topic can not be null or empty.');
        }
        $this->claimMode(self::MODE_ASSIGN);
        $ns = $this->withNamespace($topic);
        $this->assignSubExpr[$ns] = $subExpression;
        try {
            $this->subscriptionData[$ns] = FilterAPI::buildSubscriptionData($ns, $subExpression);
        } catch (\Throwable) {
            unset($this->subscriptionData[$ns]);
        }
    }

    /**
     * Selector flavour of {@see subscribe()}: the expression type travels with the subscription,
     * so a SQL92 expression is both reported to the broker as SQL92 in the heartbeat and pulled
     * with expressionType=SQL92 instead of being parsed as a tag list.
     */
    public function subscribeWithSelector(string $topic, MessageSelector $selector): void
    {
        $this->claimMode(self::MODE_SUBSCRIBE);
        $ns = $this->withNamespace($topic);
        $sub = build_selector_subscription_data($ns, $selector);
        $this->subscription[$ns] = $sub->subString ?? '*';
        $this->subscriptionData[$ns] = $sub;
    }

    /** Selector flavour of {@see setSubExpressionForAssign()} (same expression-type rule). */
    public function setSubExpressionForAssignWithSelector(string $topic, MessageSelector $selector): void
    {
        if ($this->started) {
            throw new MQClientException('setSubExpressionForAssign only can be called before start.');
        }
        if (trim($topic) === '') {
            throw new \InvalidArgumentException('Topic can not be null or empty.');
        }
        $this->claimMode(self::MODE_ASSIGN);
        $ns = $this->withNamespace($topic);
        $sub = build_selector_subscription_data($ns, $selector);
        $this->assignSubExpr[$ns] = $sub->subString ?? '*';
        $this->subscriptionData[$ns] = $sub;
    }

    /**
     * assign mode: queues are specified explicitly, no rebalance.
     *
     * @param list<MessageQueue> $messageQueues
     */
    public function assign(array $messageQueues): void
    {
        if ($messageQueues === []) {
            throw new \InvalidArgumentException('Message queues can not be null or empty.');
        }
        $this->claimMode(self::MODE_ASSIGN);
        $queues = [];
        foreach ($messageQueues as $mq) {
            $queues[self::mqKey($mq)] = $mq;
        }
        foreach (array_keys($this->assigned) as $key) {
            if (!isset($queues[$key])) {
                unset($this->nextOffset[$key], $this->consumeOffset[$key], $this->seekOffset[$key]);
                // A queue taken out of the assignment loses its cache, its accumulators and its
                // armed pauses in one step, so nothing stale can refuse a later pull round.
                $this->dropQueueCache($key);
            }
        }
        $this->assigned = $queues;
        foreach (array_keys($this->assigned) as $key) {
            if (!isset($this->nextOffset[$key])) {
                try {
                    $this->nextOffset[$key] = $this->resolveInitialOffset($this->assigned[$key]);
                } catch (\Throwable $e) {
                    Logger::debug('assign: resolve initial offset failed: ' . $e->getMessage());
                }
            }
        }
    }

    // ---------------- Lifecycle ----------------

    public function start(): void
    {
        if ($this->serviceState !== self::SERVICE_CREATE_JUST) {
            throw new MQClientException(
                'The PullConsumer service state not OK, maybe started once, ' . $this->serviceStateName());
        }
        // Any failure from here on leaves the consumer in START_FAILED, so a retry is refused.
        $this->serviceState = self::SERVICE_START_FAILED;
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
        if (!isset($this->allocateMessageQueueStrategy)) {
            throw new MQClientException('allocateMessageQueueStrategy is null');
        }
        if ($this->consumerTimeoutMillisWhenSuspend < $this->brokerSuspendMaxTimeMillis) {
            throw new MQClientException(
                'Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis');
        }
        // The start instant is checked whatever the policy is: a malformed value must fail here
        // rather than surface mid-consumption as a failed offset resolution on one queue.
        $this->parseConsumeTimestamp($this->consumeTimestamp);
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
        // assign mode resolves initial offsets at start.
        if ($this->mode === self::MODE_ASSIGN) {
            foreach (array_keys($this->assigned) as $key) {
                if (!isset($this->nextOffset[$key])) {
                    try {
                        $this->nextOffset[$key] = $this->resolveInitialOffset($this->assigned[$key]);
                    } catch (\Throwable $e) {
                        Logger::debug('start: resolve initial offset failed: ' . $e->getMessage());
                    }
                }
            }
        }
        // Register the tag subscription with the broker (heartbeat) before starting pulling.
        // The heartbeat relies on the "known broker list", and that list comes only from the
        // topic route table: pull routes of subscribed/assigned topics once at start.
        $this->refreshRouteForHeartbeat();
        $this->running = true;
        $this->sendHeartbeatToAllBroker();
        $this->nextHeartbeatAt = microtime(true) + 5.0;
        // Queue-set change comparison: the first query is scheduled 10s after startup, not stealing RPC from the startup path.
        $this->nextMetadataCheckAt = microtime(true) + 10.0;
        $this->started = true;
        $this->serviceState = self::SERVICE_RUNNING;
    }

    private function serviceStateName(): string
    {
        return match ($this->serviceState) {
            self::SERVICE_START_FAILED => 'START_FAILED',
            self::SERVICE_RUNNING => 'RUNNING',
            self::SERVICE_SHUTDOWN_ALREADY => 'SHUTDOWN_ALREADY',
            default => 'CREATE_JUST',
        };
    }

    /** Prepare broker addresses for heartbeat: pull and register the topic routes this instance cares about. */
    private function refreshRouteForHeartbeat(): void
    {
        // WARNING: start() calls this method before $started=true, so requireClient() cannot be
        // used here (deadlock).
        $client = $this->mqClient;
        if ($client === null) {
            throw new MQClientException('The consumer not running, please start it first.');
        }
        $topics = array_keys($this->subscription);
        foreach ($this->assigned as $mq) {
            if (!in_array($mq->topic, $topics, true)) {
                $topics[] = $mq->topic;
            }
        }
        foreach ($topics as $topic) {
            $client->registerTopicInUse($topic);
            try {
                $client->getTopicPublishInfo($topic);
            } catch (\Throwable $e) {
                Logger::debug("lite start: refresh route for $topic failed: " . $e->getMessage());
            }
        }
    }

    public function shutdown(): void
    {
        if ($this->serviceState !== self::SERVICE_RUNNING) {
            return;
        }
        try {
            if ($this->autoCommit) {
                $this->commit();
            } else {
                $this->persistOffsetTable(array_keys($this->assigned));
            }
        } catch (\Throwable) {
            Logger::debug('shutdown commit failed');
        }
        $this->started = false;
        $this->running = false;
        if ($this->mqClient !== null) {
            $this->mqClient->shutdown();
        }
        $this->serviceState = self::SERVICE_SHUTDOWN_ALREADY;
    }

    public function isRunning(): bool
    {
        return $this->running;
    }

    private function requireClient(): MQClientInstance
    {
        if (!$this->started || $this->mqClient === null) {
            throw new MQClientException('The consumer not running, please start it first.');
        }
        return $this->mqClient;
    }

    // ---------------- Heartbeat (register the tag subscription with the broker) ----------------

    private function buildHeartbeat(): HeartbeatData
    {
        $hb = new HeartbeatData($this->clientId ?? '');
        $cd = new ConsumerData(
            $this->consumerGroup,
            ConsumeType::CONSUME_ACTIVELY,
            $this->messageModel,
            $this->consumeFromWhere,
        );
        $cd->unitMode = $this->unitMode;
        // Only rebalance subscriptions are announced. An assign-mode expression filters the queues
        // this instance was handed; advertising it would make the broker treat the group as a
        // subscriber of the whole topic.
        foreach (array_keys($this->subscription) as $topic) {
            if (isset($this->subscriptionData[$topic])) {
                $cd->subscriptionDataSet[] = $this->subscriptionData[$topic];
            }
        }
        $hb->consumerDataSet[] = $cd;
        return $hb;
    }

    private function sendHeartbeatToAllBroker(): int
    {
        if ($this->mqClient === null) {
            return 0;
        }
        $hb = $this->buildHeartbeat();
        $ok = 0;
        foreach ($this->mqClient->getAllBrokerAddrs() as $addr) {
            try {
                $this->mqClient->sendHeartbeat($addr, $hb);
                $ok++;
            } catch (\Throwable $e) {
                Logger::debug("lite heartbeat to $addr failed: " . $e->getMessage());
            }
        }
        return $ok;
    }

    /**
     * tick-driven (PHP adaptation): rebalance (1s throttle) + short-poll pulling of all queues + heartbeat.
     */
    public function tick(?int $nowMillis = null): void
    {
        if (!$this->started || !$this->running) {
            return;
        }
        $nowMs = $nowMillis ?? UtilAll::currentTimeMillis();
        $this->mqClient?->tick($nowMs);
        $now = $nowMs / 1000.0;
        if ($now >= $this->nextHeartbeatAt) {
            $this->nextHeartbeatAt = $now + 5.0;
            try {
                $this->sendHeartbeatToAllBroker();
            } catch (\Throwable) {
                Logger::debug('lite heartbeat loop error');
            }
        }
        if ($this->mode !== self::MODE_ASSIGN && ($this->lastRebalanceTs === 0.0 || ($now - $this->lastRebalanceTs) > 1.0)) {
            $this->rebalance();
            $this->lastRebalanceTs = $now;
        }
        if ($this->nextMetadataCheckAt > 0.0 && $now >= $this->nextMetadataCheckAt) {
            $this->nextMetadataCheckAt = $now + $this->topicMetadataCheckIntervalMillis / 1000.0;
            try {
                $this->fetchTopicMessageQueuesAndCompare();
            } catch (\Throwable $e) {
                Logger::debug('metadata compare error: ' . $e->getMessage());
            }
        }
        foreach (array_keys($this->assigned) as $key) {
            if (!$this->running) {
                return;
            }
            if (isset($this->paused[$key])) {
                continue;
            }
            $this->pullOne($this->assigned[$key]);
        }
    }

    private function subscriptionFor(string $topic): string
    {
        return $this->subscription[$topic] ?? $this->assignSubExpr[$topic] ?? '*';
    }

    /**
     * The SubscriptionData a pull of this topic reports: the stored one (built by subscribe /
     * subscribeWithSelector / setSubExpressionForAssign) when present, otherwise the raw
     * expression parsed as a tag subscription.
     */
    private function subscriptionDataFor(string $topic): SubscriptionData
    {
        $stored = $this->subscriptionData[$topic] ?? null;
        if ($stored instanceof SubscriptionData && $stored->topic === $topic) {
            return $stored;
        }
        return parse_tag_subscription($topic, $this->subscriptionFor($topic) ?: '*');
    }

    /**
     * One pull of a single queue: short polling; FOUND/NO_NEW_MSG/NO_MATCHED_MSG/
     * OFFSET_ILLEGAL all advance the pull cursor to nextBeginOffset; brake: **in-flight
     * results must not overwrite just-seeked / just-removed offsets** (is the cursor still
     * the value it was when the request was sent).
     *
     * The queue's round starts with the cache gates: a refused round sends nothing, commits
     * nothing and moves no cursor, it only re-schedules this queue 50 ms later.
     */
    private function pullOne(MessageQueue $mq): bool
    {
        $key = self::mqKey($mq);
        $now = microtime(true);
        // An armed deadline is this tick model's "pull task re-scheduled later": while it is unpaid
        // the queue's round does not even reach the gates. A queue rebalance has already taken away
        // is dropped here as well, so a leftover buffer state can never put a request on the wire.
        if (!isset($this->assigned[$key])
            || $now < (float) ($this->cacheFlowControlUntil[$key] ?? 0.0)
            || $now < (float) ($this->pullIdleUntil[$key] ?? 0.0)) {
            return false;
        }
        if ($this->cacheFlowControlHit($mq, $key)) {
            return false;
        }
        $offset = $this->nextOffset[$key] ?? null;
        if ($offset === null) {
            try {
                $offset = $this->resolveInitialOffset($mq);
            } catch (\Throwable $e) {
                // The committed offset could not be determined for a reason other than
                // "not found": skip this queue for the round instead of starting at the tail.
                Logger::debug("lite resolve initial offset failed for $mq->topic@$mq->queueId: " . $e->getMessage());
                $this->pullIdleUntil[$key] = microtime(true) + 1.0;
                return false;
            }
            $this->nextOffset[$key] = $offset;
        }
        try {
            $sub = $this->subscriptionDataFor($mq->topic);
            // Short polling (suspend=False); lite_pull=True sets FLAG_LITE_PULL_MESSAGE,
            // switching the request code to LITE_PULL_MESSAGE(361).
            $sysFlag = PullSysFlag::buildSysFlag(false, false, true, false, true);
            $result = $this->requireClient()->pullMessage(
                $this->consumerGroup,
                $mq,
                $offset,
                $this->pullBatchSize,
                $sysFlag,
                0,
                $sub->subString ?? '*',
                $sub->expressionType === ExpressionType::TAG ? 0 : $sub->subVersion,
                $sub->expressionType,
                timeoutMillis: 30000,
                maxMsgBytes: -1,
                suspendTimeoutMillis: 15000,
            );
        } catch (\Throwable $e) {
            Logger::debug("lite pull_one failed for $mq->topic@$mq->queueId: " . $e->getMessage());
            $this->armBrokerThrottlePause($key, $e);
            return false;
        }
        $intact = ($this->nextOffset[$key] ?? null) === $offset;
        if ($intact) {
            $this->nextOffset[$key] = $result->nextBeginOffset;
        }
        if ($intact && $result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
            // Client-side second filtering only exists for tag subscriptions: a SQL92 /
            // class-filter match is decided by the broker alone.
            $msgs = client_side_tag_filter($sub, $result->msgFoundList);
            if ($msgs !== []) {
                $this->enqueue($msgs);
                return true;
            }
        }
        return false;
    }

    /** @param list<MessageExt> $msgs */
    private function enqueue(array $msgs): void
    {
        $this->bufferAppend($msgs);
    }

    // ---------------- Local cache flow control ----------------

    /**
     * The buffer and its per-queue accumulators, moved in one step: every message that lands in
     * the buffer also lands in its queue's count / body bytes / offset boundaries, and one pull
     * round that left messages here is booked as one pending round of that queue. This is the
     * only writer of ``$localBuffer``'s tail, so a gate verdict always describes what is really
     * cached -- the messages already handed to the application included, since they stay here
     * until ``poll()`` takes them away.
     *
     * @param list<MessageExt> $msgs
     */
    private function bufferAppend(array $msgs): void
    {
        $perQueue = [];
        foreach ($msgs as $m) {
            $k = self::messageCacheKey($m);
            $perQueue[$k] = ($perQueue[$k] ?? 0) + 1;
        }
        $roundBooked = [];
        foreach ($msgs as $m) {
            $this->localBuffer[] = $m;
            $key = self::messageCacheKey($m);
            $stat = $this->queueCache[$key]
                ?? ['count' => 0, 'bytes' => 0, 'min' => 0, 'max' => 0, 'offsets' => [], 'rounds' => []];
            $stat['count']++;
            $stat['bytes'] += strlen((string) $m->body);
            if (!isset($roundBooked[$key]) && $perQueue[$key] > 0) {
                $roundBooked[$key] = true;
                $stat['rounds'][] = $perQueue[$key];
            }
            $stat['offsets'][$m->queueOffset] = ($stat['offsets'][$m->queueOffset] ?? 0) + 1;
            if ($stat['count'] === 1) {
                $stat['min'] = $m->queueOffset;
                $stat['max'] = $m->queueOffset;
            } else {
                $stat['min'] = min($stat['min'], $m->queueOffset);
                $stat['max'] = max($stat['max'], $m->queueOffset);
            }
            $this->queueCache[$key] = $stat;
        }
    }

    /**
     * Take at most $max messages off the head of the buffer and hand them out, subtracting exactly
     * those messages from their queues' accumulators.
     *
     * @return list<MessageExt>
     */
    private function bufferTake(int $max): array
    {
        $out = [];
        while ($this->localBuffer !== [] && count($out) < $max) {
            $out[] = array_shift($this->localBuffer);
        }
        $this->bufferForget($out);
        return $out;
    }

    /**
     * Keep-only pass: what the predicate rejects leaves the buffer and its queue's accumulators
     * together (``seek()`` pruning and queue revocation both come through here).
     *
     * @param callable(MessageExt):bool $keep
     */
    private function bufferRetain(callable $keep): void
    {
        $kept = [];
        $dropped = [];
        foreach ($this->localBuffer as $m) {
            if ($keep($m)) {
                $kept[] = $m;
            } else {
                $dropped[] = $m;
            }
        }
        $this->localBuffer = $kept;
        $this->bufferForget($dropped);
    }

    /**
     * Subtract handed-over / pruned messages from their queues' accumulators. The offset
     * boundaries only need re-picking when a removal took the **last** message of the boundary
     * offset away, and that re-pick walks this queue's own distinct cached offsets, not the buffer
     * and not once per message but once per removal batch -- so a round of the gates stays O(1) and
     * never scans the buffer.
     *
     * A queue's messages always leave in round order -- ``poll()`` drains the shared buffer from its
     * head and ``seek()`` prunes a prefix of this queue's cached offsets -- so taking one message
     * away eats the front pending round and a round that runs out is dropped. A half-drained round
     * still counts as pending, exactly as the aggregate gate budgets it.
     *
     * @param list<MessageExt> $msgs
     */
    private function bufferForget(array $msgs): void
    {
        $boundaryGone = [];
        foreach ($msgs as $m) {
            $key = self::messageCacheKey($m);
            $stat = $this->queueCache[$key] ?? null;
            if ($stat === null || $stat['count'] === 0) {
                // A message the cache does not own cannot drive a counter below zero.
                continue;
            }
            $stat['count']--;
            $stat['bytes'] = max(0, $stat['bytes'] - strlen((string) $m->body));
            $rounds = $stat['rounds'] ?? [];
            if ($rounds !== []) {
                $front = $rounds[0] - 1;
                if ($front > 0) {
                    $rounds[0] = $front;
                } else {
                    array_shift($rounds);
                }
                $stat['rounds'] = $rounds;
            }
            $left = ($stat['offsets'][$m->queueOffset] ?? 0) - 1;
            if ($left > 0) {
                $stat['offsets'][$m->queueOffset] = $left;
            } else {
                unset($stat['offsets'][$m->queueOffset]);
                if ($m->queueOffset === $stat['min'] || $m->queueOffset === $stat['max']) {
                    $boundaryGone[$key] = true;
                }
            }
            if ($stat['count'] === 0) {
                // An empty cache owns no record: dropping it is what keeps the aggregate sum and
                // the per-queue verdicts reading the same thing.
                unset($this->queueCache[$key]);
                continue;
            }
            $this->queueCache[$key] = $stat;
        }
        foreach (array_keys($boundaryGone) as $key) {
            if (isset($this->queueCache[$key])) {
                $this->refreshCacheBounds($key);
            }
        }
    }

    /** Re-pick one queue's lowest / highest cached offset from the offsets it still holds. */
    private function refreshCacheBounds(string $key): void
    {
        $offsets = array_keys($this->queueCache[$key]['offsets'] ?? []);
        if ($offsets === []) {
            unset($this->queueCache[$key]);
            return;
        }
        $this->queueCache[$key]['min'] = min($offsets);
        $this->queueCache[$key]['max'] = max($offsets);
    }

    /**
     * Drop one queue's cache with the queue: its messages leave the buffer and its accumulators,
     * armed pauses and paused flag leave with them, in the same step. Re-assigning that queue later
     * therefore starts from an empty cache instead of from a stale record, and a queue this consumer
     * no longer owns can neither refuse a pull nor be skipped by one.
     */
    private function dropQueueCache(string $key): void
    {
        if (isset($this->queueCache[$key])) {
            $this->bufferRetain(static fn(MessageExt $m): bool => self::messageCacheKey($m) !== $key);
            unset($this->queueCache[$key]);
        }
        unset(
            $this->cacheFlowControlUntil[$key],
            $this->pullIdleUntil[$key],
            $this->paused[$key],
        );
    }

    /**
     * The four cache gates, judged for **this one queue** before its round resolves a pull offset
     * and therefore before any request leaves. The order is fixed:
     *
     * 1. aggregate: the pull rounds still waiting to be polled across every queue, each budgets a
     *    whole ``pullBatchSize``, against ``pullThresholdForAll``;
     * 2. count: this queue's cached message count against ``pullThresholdForQueue``;
     * 3. size: this queue's cached **body** bytes, as whole MiB, against ``pullThresholdSizeForQueue``;
     * 4. span: this queue's cached offset span against ``consumeMaxSpan``.
     *
     * All four compare **strictly greater than**, so a queue that sits exactly on its threshold
     * still pulls and a threshold reads as "how many may stay cached"; nothing here is clamped, so
     * 0 trips on the first cached message. Gates 2 to 4 judge this queue alone out of the
     * accumulators -- a sibling queue over its own budget never stops this one, and no threshold is
     * ever summed over the queues.
     */
    private function cacheFlowControlHit(MessageQueue $mq, string $key): bool
    {
        $pendingRounds = $this->pendingPullRounds();
        $reserved = $pendingRounds * $this->pullBatchSize;
        if ($reserved > $this->pullThresholdForAll) {
            $times = ++$this->flowControlForAllTimes;
            // The budget is stated in messages while this gate counts rounds, so the warn translates
            // it back into the number of rounds that fit: ceil(threshold / batch).
            $fits = intdiv($this->pullThresholdForAll + $this->pullBatchSize - 1, max(1, $this->pullBatchSize));
            $this->armCacheFlowControlPause(
                $mq,
                $key,
                $times,
                sprintf(
                    'the consume request count exceeds threshold %d, so do flow control,'
                        . ' consume request count=%d, flowControlTimes=%d',
                    $fits,
                    $pendingRounds,
                    $times,
                ),
            );
            return true;
        }
        $stat = $this->queueCache[$key] ?? null;
        if ($stat === null) {
            // Nothing of this queue is cached: no per-queue gate can have anything to say.
            return false;
        }
        $sizeMb = intdiv($stat['bytes'], 1024 * 1024);
        $span = $stat['max'] - $stat['min'];
        $measured = sprintf('minOffset=%d, maxOffset=%d, count=%d, size=%d MiB',
            $stat['min'], $stat['max'], $stat['count'], $sizeMb);
        if ($stat['count'] > $this->pullThresholdForQueue) {
            $times = ++$this->flowControlForQueueTimes;
            $this->armCacheFlowControlPause(
                $mq,
                $key,
                $times,
                sprintf(
                    'the cached message count exceeds the threshold %d, so do flow control, %s,'
                        . ' flowControlTimes=%d',
                    $this->pullThresholdForQueue,
                    $measured,
                    $times,
                ),
            );
            return true;
        }
        if ($sizeMb > $this->pullThresholdSizeForQueue) {
            $times = ++$this->flowControlForQueueTimes;
            $this->armCacheFlowControlPause(
                $mq,
                $key,
                $times,
                sprintf(
                    'the cached message size exceeds the threshold %d MiB, so do flow control, %s,'
                        . ' flowControlTimes=%d',
                    $this->pullThresholdSizeForQueue,
                    $measured,
                    $times,
                ),
            );
            return true;
        }
        if ($span > $this->consumeMaxSpan) {
            $times = ++$this->flowControlForSpanTimes;
            $this->armCacheFlowControlPause(
                $mq,
                $key,
                $times,
                sprintf(
                    "the queue's messages, span too long, so do flow control,"
                        . ' minOffset=%d, maxOffset=%d, maxSpan=%d, flowControlTimes=%d',
                    $stat['min'],
                    $stat['max'],
                    $span,
                    $times,
                ),
            );
            return true;
        }
        return false;
    }

    /**
     * Record one cache-gate refusal of this queue's pull. Every gate writes the deadline through
     * here, so a refusal is exactly one 50 ms pause on exactly this queue -- the same own-cache
     * pause and the same once-per-1000 warn cadence the push path's gates use, shared through the
     * push class's public constants rather than restated with a second value. Nothing else moves:
     * no offset is committed, the cursor stays, the messages stay cached.
     */
    private function armCacheFlowControlPause(MessageQueue $mq, string $key, int $times, string $reason): void
    {
        $pauseMillis = DefaultMQPushConsumer::PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL;
        $this->cacheFlowControlUntil[$key] = microtime(true) + $pauseMillis / 1000.0;
        $line = sprintf('flow control: queue %s %s, pause pull for %d ms', $mq, $reason, $pauseMillis);
        Logger::debug($line);
        if (DefaultMQPushConsumer::isFlowControlWarnTrip($times)) {
            Logger::warning($line);
        }
    }

    /**
     * A broker throttle on this path is not an error: when the pull comes back answered with
     * FLOW_CONTROL(215) the broker held the request on its own window, which says nothing about
     * this client's cache, so this queue's next round waits the push path's 20 ms broker pause
     * (``PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL``, reached through the shared code-to-wait
     * mapping) instead of hammering away on the next tick. Every other failure keeps the lite
     * path's existing behaviour -- log it and retry on the next tick -- so no other response
     * code's handling changes here, and the mapping's other arm (the push consumer's configured
     * failed-round backoff, whose default the call below leaves untouched) is never read on this
     * path: the guard above only lets 215 through to it.
     */
    private function armBrokerThrottlePause(string $key, \Throwable $e): void
    {
        $code = $e instanceof MQBrokerException ? $e->responseCode : null;
        if ($code !== ResponseCode::FLOW_CONTROL) {
            return;
        }
        $this->pullIdleUntil[$key] = microtime(true)
            + DefaultMQPushConsumer::pullBackoffMillisForResponseCode($code) / 1000.0;
    }

    private function maybeAutoCommit(): void
    {
        $now = microtime(true);
        if ($now < $this->nextAutoCommitDeadline) {
            return;
        }
        $this->nextAutoCommitDeadline = $now + $this->autoCommitIntervalMillis / 1000.0;
        $this->commit();
    }

    private function resolveInitialOffset(MessageQueue $mq): int
    {
        $key = self::mqKey($mq);
        if (isset($this->seekOffset[$key])) {
            return $this->seekOffset[$key];
        }
        if ($this->mqClient === null) {
            // assign-before-start is allowed, but 0 must **not** be handed out without asking
            // the broker, as assign stores the result into nextOffset, pinning the start
            // point at 0 forever.
            throw new MQClientException('The lite pull consumer is not running');
        }
        // Reuse a committed offset when present (guaranteeing consumption resumes after restart).
        // Not found is a normal answer; any other query failure propagates so the caller skips
        // this queue for the round instead of starting it at the tail.
        if ($this->messageModel === MessageModel::BROADCASTING) {
            $table = read_local_offset_table($this->clientId ?? '', $this->consumerGroup) ?? [];
            if (isset($table[$key]) && $table[$key] >= 0) {
                return $table[$key];
            }
        } else {
            $off = $this->mqClient->queryConsumerOffset($this->consumerGroup, $mq);
            if ($off !== null) {
                return $off;
            }
        }
        if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
            return 0;
        }
        if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_TIMESTAMP) {
            if (NamespaceUtil::isRetryTopic($mq->topic)) {
                // Same rule as the push consumer: a %RETRY% queue cannot be searched
                // backwards from a timestamp, so it starts at the tail.
                return $this->mqClient->getMaxOffset($mq);
            }
            return $this->mqClient->searchOffsetByTimestamp($mq, $this->parseConsumeTimestamp($this->consumeTimestamp));
        }
        // Default CONSUME_FROM_LAST_OFFSET: a %RETRY% queue starts at 0 so every queued
        // retry is delivered.
        if (NamespaceUtil::isRetryTopic($mq->topic)) {
            return 0;
        }
        return $this->mqClient->getMaxOffset($mq);
    }

    private function parseConsumeTimestamp(string $ts): int
    {
        return UtilAll::parseConsumeTimestamp($ts);
    }

    // ---------------- rebalance (subscribe mode) ----------------

    private function rebalance(): void
    {
        $client = $this->requireClient();
        $wasByTopic = consumer_queues_by_topic($this->assigned);
        $newSet = [];
        foreach (array_keys($this->subscription) as $topic) {
            try {
                $mqAll = $client->getTopicSubscribeInfo($topic);
                usort($mqAll, static fn(MessageQueue $a, MessageQueue $b) =>
                    consumer_mq_sort_key($a) <=> consumer_mq_sort_key($b));
            } catch (\Throwable) {
                $mqAll = [];
            }
            $cidAll = $client->getConsumerIdListByGroup($topic, $this->consumerGroup) ?? [];
            sort($cidAll);
            if ($this->clientId !== null && !in_array($this->clientId, $cidAll, true)) {
                $cidAll[] = $this->clientId;
                sort($cidAll);
            }
            try {
                $allocated = $this->allocateMessageQueueStrategy->allocate(
                    $this->consumerGroup,
                    $this->clientId ?? '',
                    $mqAll,
                    $cidAll,
                );
            } catch (\Throwable $e) {
                Logger::error('allocate message queue exception, strategy='
                    . $this->allocateMessageQueueStrategy->getName() . ': ' . $e->getMessage());
                foreach ($this->assigned as $key => $mq) {
                    if ($mq->topic === $topic) {
                        $newSet[$key] = $mq;
                    }
                }
                continue;
            }
            foreach ($allocated as $mq) {
                $newSet[self::mqKey($mq)] = $mq;
            }
        }
        $moved = consumer_changed_topics($wasByTopic, consumer_queues_by_topic($newSet));
        if ($moved !== []) {
            $oldKeys = array_fill_keys(array_keys($this->assigned), true);
            $this->assigned = $newSet;
            foreach (array_keys($newSet) as $key) {
                if (isset($oldKeys[$key])) {
                    continue;
                }
                if (!isset($this->nextOffset[$key])) {
                    try {
                        $this->nextOffset[$key] = $this->resolveInitialOffset($newSet[$key]);
                    } catch (\Throwable $e) {
                        Logger::debug('rebalance: resolve offset failed: ' . $e->getMessage());
                    }
                }
            }
            foreach (array_keys($oldKeys) as $key) {
                if (isset($newSet[$key])) {
                    continue;
                }
                $this->persistOffsetByKey($key);
                // Drop the AssignedMessageQueue entries together: keeping seekOffset would let
                // this queue silently jump back to a position the user pinned manually long
                // ago when it someday returns to this instance. The cache goes with the queue, so
                // a queue this consumer no longer owns keeps no accumulator, no armed pause and no
                // buffered message behind its back.
                unset($this->nextOffset[$key], $this->consumeOffset[$key],
                    $this->offsetTable[$key], $this->seekOffset[$key]);
                $this->dropQueueCache($key);
            }
            if ($this->messageQueueListener !== null) {
                // The MessageQueueListener interface signature is (topic, mqAll, mqDivided): the
                // topic's FULL queue set from the route and this instance's share of it. Only the
                // topics whose share moved are told -- an unchanged topic has nothing to report, and
                // re-reporting every subscription each round would have the application rebuild
                // state for queues that never changed hands.
                foreach ($moved as $topic) {
                    try {
                        $this->messageQueueListener->messageQueueChanged(
                            $topic,
                            $client->getTopicSubscribeInfo($topic),
                            array_values(array_filter(
                                $newSet,
                                static fn(MessageQueue $mq): bool => $mq->topic === $topic,
                            )),
                        );
                    } catch (\Throwable $e) {
                        Logger::debug("queue listener failed for $topic: " . $e->getMessage());
                    }
                }
            }
        }
    }

    // ---------------- poll / offsets ----------------

    public function poll(?int $timeout = null): array
    {
        if (!$this->started) {
            throw new MQClientException('The consumer not running, please start it first.');
        }
        $timeout = $timeout ?? $this->pollTimeoutMillis;
        if ($timeout < 0) {
            throw new \InvalidArgumentException('Timeout must not be negative');
        }
        if ($this->autoCommit) {
            $this->maybeAutoCommit();
        }
        $deadline = microtime(true) + $timeout / 1000.0;
        while ($this->localBuffer === []) {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                return [];
            }
            // PHP adaptation: Condition.wait -> bounded spin at 10ms granularity.
            usleep(10000);
        }
        $out = $this->bufferTake(1024);
        $advanced = [];
        foreach ($out as $m) {
            $key = self::messageCacheKey($m);
            if (!isset($this->nextOffset[$key])) {
                continue;
            }
            $nxt = $m->queueOffset + 1;
            if ($nxt > ($advanced[$key] ?? -1)) {
                $advanced[$key] = $nxt;
            }
        }
        foreach ($advanced as $key => $off) {
            $this->consumeOffset[$key] = $off;
        }
        return $out;
    }

    /**
     * seek: dial all three cursors back together and drop buffered messages below the new offset.
     *
     * A queue this consumer does not hold is refused first -- writing a cursor for it would leave
     * an entry nothing pulls, so the caller would watch an offset change that no round ever
     * honours -- and the offset is then held to the queue's own [minOffset, maxOffset] range.
     */
    public function seek(MessageQueue $mq, int $offset): void
    {
        $key = self::mqKey($mq);
        if (!isset($this->assigned[$key])) {
            if ($this->mode !== self::MODE_ASSIGN) {
                throw new MQClientException(
                    'The message queue is not in assigned list, may be rebalancing, message queue: ' . $mq);
            }
            throw new MQClientException('The message queue is not in assigned list, message queue: ' . $mq);
        }
        $client = $this->requireClient();
        $minOffset = $client->getMinOffset($mq);
        $maxOffset = $client->getMaxOffset($mq);
        if ($offset < $minOffset || $offset > $maxOffset) {
            throw new MQClientException(sprintf(
                'Seek offset illegal, seek offset = %d, min offset = %d, max offset = %d',
                $offset, $minOffset, $maxOffset));
        }
        $this->seekOffset[$key] = $offset;
        $this->nextOffset[$key] = $offset;
        $this->consumeOffset[$key] = $offset;
        $this->bufferRetain(static function (MessageExt $m) use ($mq, $offset): bool {
            $sameQueue = $m->topic === $mq->topic
                && ($m->brokerName ?? '') === $mq->brokerName
                && $m->queueId === $mq->queueId;
            return !$sameQueue || $m->queueOffset >= $offset;
        });
    }

    public function seekToBegin(MessageQueue $mq): void
    {
        $this->seek($mq, $this->requireClient()->getMinOffset($mq));
    }

    public function seekToEnd(MessageQueue $mq): void
    {
        $this->seek($mq, $this->requireClient()->getMaxOffset($mq));
    }

    /** Pull cursor (observation point: what a real-machine comparison shows is exactly the difference between "pulled" and "handed out"). */
    public function pullCursorOf(MessageQueue $mq): int
    {
        return $this->nextOffset[self::mqKey($mq)] ?? -1;
    }

    /** Consume cursor (the cell ``poll()`` hands out). */
    public function consumeCursorOf(MessageQueue $mq): int
    {
        return $this->consumeOffset[self::mqKey($mq)] ?? -1;
    }

    /** The cell in the in-memory offset table (where ``persist=False`` committed-but-not-yet-flushed values live). */
    public function pendingCommitOf(MessageQueue $mq): int
    {
        return $this->offsetTable[self::mqKey($mq)] ?? -1;
    }

    public function committed(MessageQueue $mq): ?int
    {
        // The running check comes first: the in-memory table belongs to a session,
        // and a consumer that never started has no committed offset to report.
        $client = $this->requireClient();
        $key = self::mqKey($mq);
        $cached = $this->offsetTable[$key] ?? null;
        if ($cached !== null) {
            return $cached;
        }
        if ($this->messageModel === MessageModel::BROADCASTING) {
            // Broadcasting stores offsets in the local file; the broker is never asked.
            $table = read_local_offset_table($this->clientId ?? '', $this->consumerGroup) ?? [];
            $localOffset = $table[$key] ?? null;
            if ($localOffset !== null) {
                $this->offsetTable[$key] = $localOffset;
            }
            return $localOffset;
        }
        $brokerOffset = $client->queryConsumerOffset($this->consumerGroup, $mq);
        if ($brokerOffset !== null) {
            $this->offsetTable[$key] = $brokerOffset;
        }
        return $brokerOffset;
    }

    private function persistOffset(?MessageQueue $mq): void
    {
        if ($mq === null) {
            return;
        }
        $key = self::mqKey($mq);
        $offset = $this->offsetTable[$key] ?? null;
        if ($offset === null) {
            return;
        }
        if ($this->messageModel === MessageModel::BROADCASTING) {
            // Broadcasting commits to the local file only; the broker is never told.
            persist_local_offset_entries($this->clientId ?? '', $this->consumerGroup, [$key => $offset], [$key => $mq]);
            return;
        }
        try {
            $this->requireClient()->updateConsumerOffset($this->consumerGroup, $mq, $offset);
        } catch (\Throwable $e) {
            Logger::debug("lite persist failed for $mq: " . $e->getMessage());
        }
    }

    private function persistOffsetTable(?iterable $keys = null): void
    {
        if ($keys === null) {
            foreach (array_keys($this->offsetTable) as $key) {
                $this->persistOffsetByKey($key);
            }
            return;
        }
        $wanted = [];
        foreach ($keys as $key) {
            $wanted[$key] = true;
        }
        if ($wanted === []) {
            return;
        }
        foreach (array_keys($this->offsetTable) as $key) {
            if (!isset($wanted[$key])) {
                unset($this->offsetTable[$key]);
                continue;
            }
            $this->persistOffsetByKey($key);
        }
    }

    private function persistOffsetByKey(string $key): void
    {
        $mq = $this->assigned[$key] ?? null;
        if ($mq === null) {
            return;
        }
        $this->persistOffset($mq);
    }

    public function commit(?array $offsets = null, bool $persist = true): void
    {
        if ($offsets === null) {
            $targets = [];
            foreach (array_keys($this->assigned) as $key) {
                $targets[$key] = $this->consumeOffset[$key] ?? -1;
            }
            $scope = array_keys($this->assigned);
        } elseif ($offsets !== [] && array_is_list($offsets) && is_string($offsets[0] ?? null)) {
            $queues = $offsets;
            if ($queues === []) {
                return;
            }
            $targets = [];
            foreach ($queues as $key) {
                $targets[$key] = $this->consumeOffset[$key] ?? -1;
            }
            $scope = $queues;
        } else {
            if ($offsets === []) {
                Logger::warning('MessageQueues is empty, Ignore this commit ');
                return;
            }
            $targets = $offsets;
            $scope = array_keys($offsets);
        }

        foreach ($targets as $key => $offset) {
            if ($offset === -1) {
                Logger::error("consumerOffset is -1 in messageQueue [$key].");
                continue;
            }
            if (!isset($this->assigned[$key])) {
                continue;
            }
            $this->offsetTable[$key] = (int) $offset;
        }

        if ($persist) {
            $this->persistOffsetTable($scope);
        }
    }

    public function offsetForTimestamp(MessageQueue $mq, int $timestamp): int
    {
        return $this->requireClient()->searchOffsetByTimestamp($mq, $timestamp);
    }

    /** @return list<MessageQueue> */
    public function assignment(): array
    {
        return array_values($this->assigned);
    }

    public function isAssignMode(): bool
    {
        return $this->mode === self::MODE_ASSIGN;
    }

    /**
     * Pin this consumer's mode on first use and refuse any later call that would switch it:
     * the running halves (subscription table, pull targets, heartbeat) would otherwise
     * describe two different consumers at once.
     */
    private function claimMode(int $mode): void
    {
        if ($this->mode === self::MODE_NONE) {
            $this->mode = $mode;
            return;
        }
        if ($this->mode !== $mode) {
            throw new MQClientException('Subscribe and assign are mutually exclusive.');
        }
    }

    /** @return list<MessageQueue> */
    public function fetchMessageQueues(string $topic): array
    {
        return $this->fetchMessageQueuesFull($this->withNamespace($topic));
    }

    /**
     * List queues by **already namespaced** topic. The queue change listener goes through this
     * layer internally: the key it holds is namespaced already, and namespacing it again only
     * yields an ns%ns%topic name that cannot be found.
     *
     * Two things must be done right together, otherwise the queue change listener silently
     * distorts:
     * 1. **Ask the name server fresh every round**, never eating the periodically refreshed
     *    route cache; without this step, scaling out becomes visible only after one route
     *    polling round at the earliest (listener callbacks lag one period behind expectation).
     * 2. An empty queue set **throws an exception** rather than returning an empty list:
     *    "not found" is not the same as "this topic shrank to 0 queues", as the latter would
     *    give the listener a false scale-down callback and flush the snapshot to empty.
     *
     * @return list<MessageQueue>
     */
    private function fetchMessageQueuesFull(string $fullTopic): array
    {
        $client = $this->requireClient();
        try {
            $client->updateTopicRouteInfoFromNameServer($fullTopic);
        } catch (\Throwable $e) {
            // On jitter, fall back to the current cache and judge once more (the outer layer
            // reports "still no queues" as one warn).
            Logger::debug("fetch queues: route refresh for $fullTopic failed: " . $e->getMessage());
        }
        $queues = $client->getTopicSubscribeInfo($fullTopic);
        if ($queues === []) {
            throw new MQClientException(
                'Can not find Message Queue for this topic, ' . $fullTopic . ' Namesrv return empty'
            );
        }
        return $queues;
    }

    // ---------------- Topic queue-set change listener ----------------

    public function setTopicMetadataCheckIntervalMillis(int $millis): void
    {
        $this->topicMetadataCheckIntervalMillis = max(1000, $millis);
    }

    public function topicMetadataCheckIntervalMillis(): int
    {
        return $this->topicMetadataCheckIntervalMillis;
    }

    /**
     * Register a queue-set change listener for a topic: tick pulls the topic's subscribed queue
     * set on the topicMetadataCheckIntervalMillis period and does a **set** comparison against
     * the last snapshot, calling back only when it really changed.
     *
     * The listener can be an object with an `onChanged(string $topic, array $mqs)` method, or a
     * callable taking the same two arguments.
     *
     * Policy:
     * * empty topic or null listener -> throw MQClientException('Topic or listener is null');
     * * registering the same topic twice -> overwrite the old listener and warn once;
     * * the key is the namespaced topic, and the callback's first argument is it too;
     * * registering while running records a snapshot immediately, otherwise the next round
     *   would report the "current set" as a change once; when not started queues cannot be
     *   queried, so no snapshot is recorded, and the first round after startup fills in the
     *   initial state.
     */
    public function registerTopicMessageQueueChangeListener(string $topic, mixed $listener): void
    {
        if (trim($topic) === '' || $listener === null) {
            throw new MQClientException('Topic or listener is null');
        }
        if (!is_object($listener) && !is_callable($listener)) {
            throw new MQClientException('Topic or listener is null');
        }
        $key = $this->withNamespace($topic);
        if (isset($this->topicChangeListeners[$key])) {
            Logger::warning("Topic $key had been registered, new listener will overwrite the old one");
        }
        $this->topicChangeListeners[$key] = $listener;
        if (!$this->started) {
            return;
        }
        try {
            $this->messageQueuesForTopic[$key] = self::queueKeySet($this->fetchMessageQueues($topic));
        } catch (\Throwable $e) {
            Logger::debug("register listener: fetch queues for $key failed: " . $e->getMessage());
        }
    }

    /**
     * Run one comparison round (tick calls it; callers and unit tests can drive it directly),
     * returning how many callbacks fired this round.
     *
     * WARNING - intentional: a single topic failing only logs and skips itself, while other
     * topics in the same round compare as usual. A topic that long cannot find a route should
     * not starve the listeners queued behind it forever; the next round proceeds normally,
     * and neither writing interrupts scheduling.
     */
    public function fetchTopicMessageQueuesAndCompare(): int
    {
        $fired = 0;
        foreach ($this->topicChangeListeners as $key => $listener) {
            try {
                $queues = $this->fetchMessageQueuesFull($key);
            } catch (\Throwable $e) {
                Logger::warning('ScheduledTask fetchMessageQueuesAndCompare exception: ' . $e->getMessage());
                continue;
            }
            $current = self::queueKeySet($queues);
            $snapshot = $this->messageQueuesForTopic[$key] ?? null;
            // No snapshot => treat as change: querying queues always returns a non-empty set,
            // so the first round necessarily hands the current state to the caller, and only
            // increments are reported afterwards.
            if ($snapshot !== null && self::sameKeySet($snapshot, $current)) {
                continue;
            }
            $this->messageQueuesForTopic[$key] = $current;
            self::invokeQueueChangeListener($listener, $key, $queues);
            $fired++;
        }
        return $fired;
    }

    /** @param list<MessageQueue> $queues @return array<string,bool> */
    private static function queueKeySet(array $queues): array
    {
        $set = [];
        foreach ($queues as $mq) {
            $set[self::mqKey($mq)] = true;
        }
        return $set;
    }

    /**
     * Sets are equal: same count and every member is in the other (order irrelevant).
     *
     * @param array<string,bool> $old
     * @param array<string,bool> $new
     */
    private static function sameKeySet(array $old, array $new): bool
    {
        if (count($old) !== count($new)) {
            return false;
        }
        return array_diff_key($old, $new) === [] && array_diff_key($new, $old) === [];
    }

    /** @param object|callable $listener @param list<MessageQueue> $queues */
    private static function invokeQueueChangeListener(mixed $listener, string $topic, array $queues): void
    {
        try {
            if (is_object($listener) && method_exists($listener, 'onChanged')) {
                $listener->onChanged($topic, $queues);
                return;
            }
            if (is_callable($listener)) {
                $listener($topic, $queues);
                return;
            }
            Logger::warning("Topic queue change listener for $topic is not callable");
        } catch (\Throwable $e) {
            // The callback is the caller's code: its throw must not take the comparison
            // thread down, nor be swallowed silently.
            Logger::error("Topic queue change listener for $topic threw: " . $e->getMessage());
        }
    }

    /** @param list<MessageQueue> $messageQueues */
    public function pause(array $messageQueues): void
    {
        foreach ($messageQueues as $mq) {
            $this->paused[self::mqKey($mq)] = true;
        }
    }

    /** @param list<MessageQueue> $messageQueues */
    public function resume(array $messageQueues): void
    {
        foreach ($messageQueues as $mq) {
            unset($this->paused[self::mqKey($mq)]);
        }
    }

    public function setMessageQueueListener(object $listener): void
    {
        $this->messageQueueListener = $listener;
    }

    private static function mqKey(MessageQueue $mq): string
    {
        return $mq->topic . $mq->brokerName . (string) $mq->queueId;
    }

    /**
     * The same key the queue state maps use, rebuilt from a buffered message: the buffer is one
     * global container, so a message carries its own queue's identity and that is what the cache
     * accumulators are keyed by (``poll()`` advances the consume cursor the same way).
     */
    private static function messageCacheKey(MessageExt $m): string
    {
        return $m->topic . ($m->brokerName ?? '') . (string) $m->queueId;
    }
}

/** Convenient listener wrapper: turning consume logic into a pure function. */
final class SimpleMessageListener implements MessageListenerConcurrently
{
    /** @var callable(list<MessageExt>): ConsumeConcurrentlyStatus */
    private $fn;

    /** @param callable(list<MessageExt>): ConsumeConcurrentlyStatus $fn */
    public function __construct(callable $fn)
    {
        $this->fn = $fn;
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        return ($this->fn)($msgs);
    }
}
