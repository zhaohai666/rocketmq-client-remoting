<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\ExpressionType;
use RocketMQ\Common\FilterAPI;
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
    public string $messageModel = MessageModel::CLUSTERING;
    public int $brokerSuspendMaxTimeMillis = 20000;
    public int $consumerPullTimeoutMillis = 10000;
    public int $consumerTimeoutMillisWhenSuspend = 30000;
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
                $client->sendHeartbeat($addr, $hb, 5000);
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
        $client = $this->requireClient();
        $timeout = $timeoutMillis ?? $this->consumerPullTimeoutMillis;
        $sub = FilterAPI::buildSubscriptionData($mq->topic, $subExpression);
        $sysFlag = PullSysFlag::buildSysFlag(false, false, true, false);
        $key = self::mqKey($mq);
        $result = $client->pullMessage(
            $this->consumerGroup,
            $mq,
            $offset,
            $maxNums,
            $sysFlag,
            0,
            $sub->subString ?? '*',
            0,
            ExpressionType::TAG,
            timeoutMillis: $timeout,
            maxMsgBytes: -1,
            suspendTimeoutMillis: 15000,
            brokerId: $this->pullFromWhichNode[$key] ?? MixAll::MASTER_ID,
        );
        $this->pullFromWhichNode[$key] = $result->suggestWhichBrokerId ?? MixAll::MASTER_ID;
        $this->pulledQueues[$key] = $mq;
        if ($result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
            $result->msgFoundList = $this->filterMessagesForDelivery($mq, $result->msgFoundList);
        }
        return $result;
    }

    public function pullBlockIfNotFound(
        MessageQueue $mq,
        string $subExpression,
        int $offset,
        int $maxNums,
    ): PullResult {
        $client = $this->requireClient();
        $sub = FilterAPI::buildSubscriptionData($mq->topic, $subExpression);
        // block=true: suspend=True makes the broker hang until messages arrive; timeout uses consumerTimeoutMillisWhenSuspend.
        $sysFlag = PullSysFlag::buildSysFlag(false, true, true, false);
        $key = self::mqKey($mq);
        $result = $client->pullMessage(
            $this->consumerGroup,
            $mq,
            $offset,
            $maxNums,
            $sysFlag,
            0,
            $sub->subString ?? '*',
            0,
            ExpressionType::TAG,
            timeoutMillis: $this->consumerTimeoutMillisWhenSuspend,
            maxMsgBytes: -1,
            suspendTimeoutMillis: $this->brokerSuspendMaxTimeMillis,
            brokerId: $this->pullFromWhichNode[$key] ?? MixAll::MASTER_ID,
        );
        $this->pullFromWhichNode[$key] = $result->suggestWhichBrokerId ?? MixAll::MASTER_ID;
        if ($result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
            $result->msgFoundList = $this->filterMessagesForDelivery($mq, $result->msgFoundList);
        }
        return $result;
    }

    // ---------------- Offset management ----------------

    public function fetchConsumeOffset(MessageQueue $mq): ?int
    {
        return $this->requireClient()->queryConsumerOffset($this->consumerGroup, $mq);
    }

    public function updateConsumeOffset(MessageQueue $mq, int $offset): void
    {
        $this->requireClient()->updateConsumerOffset($this->consumerGroup, $mq, $offset);
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
        $response = $client->remotingClient->invokeSync($addr, $request, 5000);
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
        $header->maxReconsumeTimes = null;
        $request = RemotingCommand::createRequestCommand(RequestCode::CONSUMER_SEND_MSG_BACK, $header);
        $response = $client->remotingClient->invokeSync($addr, $request, 5000);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }
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
    private bool $assignMode = false;
    /** @var array<string,MessageQueue> Assigned queues (key = mqKey) */
    private array $assigned = [];

    /** Rebalance algorithm (subscribe mode). */
    public AllocateMessageQueueStrategy $allocateMessageQueueStrategy;

    // Pull / poll configuration
    public string $consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
    /** Start point of CONSUME_FROM_TIMESTAMP (format yyyyMMddHHmmss), 30 minutes ago by default. */
    public string $consumeTimestamp;
    public int $pullBatchSize = 32;
    public int $pollTimeoutMillis = 5000;
    public int $pollNameServerInterval = 30000;
    public bool $autoCommit = true;
    public int $autoCommitIntervalMillis = 5000;
    public int $consumerTimeoutMillisWhenSuspend = 30000;
    public int $brokerSuspendMaxTimeMillis = 20000;
    public int $pullIntervalMillis = 50;  // Backoff on empty polls at queue tail, avoiding busy loops hammering the broker

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
        $this->consumeTimestamp = date('YmdHis', time() - 30 * 60);
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

    // ---------------- Subscription / allocation ----------------

    public function subscribe(string $topic, string $subExpression = '*'): void
    {
        $this->assignMode = false;
        $ns = $this->withNamespace($topic);
        $this->subscription[$ns] = $subExpression;
        // Build SubscriptionData synchronously so the heartbeat registers the tag subscription with the broker.
        try {
            $this->subscriptionData[$ns] = FilterAPI::buildSubscriptionData($ns, $subExpression);
        } catch (\Throwable) {
            unset($this->subscriptionData[$ns]);
        }
    }

    public function unsubscribe(string $topic): void
    {
        $ns = $this->withNamespace($topic);
        unset($this->subscription[$ns], $this->subscriptionData[$ns]);
    }

    public function setSubExpressionForAssign(string $topic, string $subExpression): void
    {
        $ns = $this->withNamespace($topic);
        $this->assignSubExpr[$ns] = $subExpression;
        try {
            $this->subscriptionData[$ns] = FilterAPI::buildSubscriptionData($ns, $subExpression);
        } catch (\Throwable) {
            unset($this->subscriptionData[$ns]);
        }
    }

    /**
     * assign mode: queues are specified explicitly, no rebalance.
     *
     * @param list<MessageQueue> $messageQueues
     */
    public function assign(array $messageQueues): void
    {
        $this->assignMode = true;
        $queues = [];
        foreach ($messageQueues as $mq) {
            $queues[self::mqKey($mq)] = $mq;
        }
        foreach (array_keys($this->assigned) as $key) {
            if (!isset($queues[$key])) {
                unset($this->nextOffset[$key], $this->consumeOffset[$key]);
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
        if ($this->subscription === [] && !$this->assignMode) {
            throw new MQClientException('subscription is not set, call subscribe() or assign() first');
        }
        if (!isset($this->allocateMessageQueueStrategy)) {
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
            pollNameServerInterval: $this->pollNameServerInterval,
            tlsOptions: $this->tlsOptions,
            socksProxy: $this->socksProxy,
        );
        if ($this->rpcHook !== null) {
            $this->mqClient->remotingClient->registerRpcHook($this->rpcHook);
        }
        $this->mqClient->start();
        // assign mode resolves initial offsets at start.
        if ($this->assignMode) {
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
    }

    /** Prepare broker addresses for heartbeat: pull and register the topic routes this instance cares about. */
    private function refreshRouteForHeartbeat(): void
    {
        // WARNING: start() calls this method before $started=true, so requireClient() cannot be
        // used here (deadlock).
        $client = $this->mqClient;
        if ($client === null) {
            throw new MQClientException('consumer not started, call start() first');
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
        if (!$this->started) {
            return;
        }
        $this->started = false;
        $this->running = false;
        try {
            if ($this->autoCommit) {
                $this->commit();
            } else {
                $this->persistOffsetTable(array_keys($this->assigned));
            }
        } catch (\Throwable) {
            Logger::debug('shutdown commit failed');
        }
        if ($this->mqClient !== null) {
            $this->mqClient->shutdown();
        }
    }

    public function isRunning(): bool
    {
        return $this->running;
    }

    private function requireClient(): MQClientInstance
    {
        if (!$this->started || $this->mqClient === null) {
            throw new MQClientException('consumer not started, call start() first');
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
        foreach ($this->subscriptionData as $sub) {
            $cd->subscriptionDataSet[] = $sub;
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
                $this->mqClient->sendHeartbeat($addr, $hb, 5000);
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
        if (!$this->assignMode && ($this->lastRebalanceTs === 0.0 || ($now - $this->lastRebalanceTs) > 1.0)) {
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
     * One pull of a single queue: short polling; FOUND/NO_NEW_MSG/NO_MATCHED_MSG/
     * OFFSET_ILLEGAL all advance the pull cursor to nextBeginOffset; brake: **in-flight
     * results must not overwrite just-seeked / just-removed offsets** (is the cursor still
     * the value it was when the request was sent).
     */
    private function pullOne(MessageQueue $mq): bool
    {
        $key = self::mqKey($mq);
        $offset = $this->nextOffset[$key] ?? null;
        if ($offset === null) {
            $offset = $this->resolveInitialOffset($mq);
            $this->nextOffset[$key] = $offset;
        }
        $sub = $this->subscriptionFor($mq->topic) ?: '*';
        // Short polling (suspend=False); lite_pull=True sets FLAG_LITE_PULL_MESSAGE,
        // switching the request code to LITE_PULL_MESSAGE(361).
        $sysFlag = PullSysFlag::buildSysFlag(false, false, true, false, true);
        try {
            $result = $this->requireClient()->pullMessage(
                $this->consumerGroup,
                $mq,
                $offset,
                $this->pullBatchSize,
                $sysFlag,
                0,
                $sub,
                0,
                ExpressionType::TAG,
                timeoutMillis: 30000,
                maxMsgBytes: -1,
                suspendTimeoutMillis: 15000,
            );
        } catch (\Throwable $e) {
            Logger::debug("lite pull_one failed for $mq->topic@$mq->queueId: " . $e->getMessage());
            return false;
        }
        $intact = ($this->nextOffset[$key] ?? null) === $offset;
        if ($intact) {
            $this->nextOffset[$key] = $result->nextBeginOffset;
        }
        if ($intact && $result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
            $msgs = $this->filterTags($mq->topic, $result->msgFoundList, $sub);
            if ($msgs !== []) {
                $this->enqueue($msgs);
                return true;
            }
        }
        return false;
    }

    /** @param list<MessageExt> $msgs @return list<MessageExt> */
    private function filterTags(string $topic, array $msgs, string $sub): array
    {
        if ($sub === '' || $sub === '*') {
            return $msgs;
        }
        try {
            $subData = FilterAPI::buildSubscriptionData($topic, $sub);
        } catch (\Throwable) {
            return $msgs;
        }
        if ($subData->tagsSet === []) {
            return $msgs;
        }
        $kept = [];
        foreach ($msgs as $m) {
            $tag = $m->getTags();
            // WARNING: tagsSet is a list, not a map: isset($tagsSet[$tag]) always misses (same as ConsumerSupport).
            if ($tag !== null && in_array($tag, $subData->tagsSet, true)) {
                $kept[] = $m;
            }
        }
        return $kept;
    }

    /** @param list<MessageExt> $msgs */
    private function enqueue(array $msgs): void
    {
        foreach ($msgs as $m) {
            $this->localBuffer[] = $m;
        }
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
        // Reuse a committed offset when present (guaranteeing consumption resumes after restart)
        try {
            $off = $this->mqClient->queryConsumerOffset($this->consumerGroup, $mq);
            if ($off !== null) {
                return $off;
            }
        } catch (\Throwable) {
            // ignore
        }
        if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
            return 0;
        }
        if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_TIMESTAMP) {
            return $this->mqClient->searchOffsetByTimestamp($mq, $this->parseConsumeTimestamp($this->consumeTimestamp));
        }
        return $this->mqClient->getMaxOffset($mq);
    }

    private function parseConsumeTimestamp(string $ts): int
    {
        $dt = \DateTime::createFromFormat('YmdHis', $ts);
        if ($dt === false || strlen($ts) !== 14 || (int) $dt->format('YmdHis') !== (int) $ts) {
            throw new MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received $ts",
            );
        }
        return (int) $dt->format('U') * 1000 + (int) $dt->format('v');
    }

    // ---------------- rebalance (subscribe mode) ----------------

    private function rebalance(): void
    {
        $client = $this->requireClient();
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
        if (array_keys($newSet) !== array_keys($this->assigned) || $newSet != $this->assigned) {
            $old = $this->assigned;
            $oldKeys = array_fill_keys(array_keys($old), true);
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
                // ago when it someday returns to this instance.
                unset($this->nextOffset[$key], $this->consumeOffset[$key],
                    $this->offsetTable[$key], $this->seekOffset[$key]);
            }
            if ($this->messageQueueListener !== null) {
                try {
                    // The MessageQueueListener interface signature is (topic, mqAll, mqDivided);
                    // lite's rebalance computes all subscriptions together, so notify per topic here.
                    foreach (array_keys($this->subscription) as $topic) {
                        $this->messageQueueListener->messageQueueChanged(
                            $topic,
                            array_values($old),
                            array_values(array_filter(
                                $newSet,
                                static fn(MessageQueue $mq): bool => $mq->topic === $topic,
                            )),
                        );
                    }
                } catch (\Throwable) {
                    // ignore
                }
            }
        }
    }

    // ---------------- poll / offsets ----------------

    public function poll(?int $timeout = null): array
    {
        $timeout = $timeout ?? $this->pollTimeoutMillis;
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
        $out = [];
        while ($this->localBuffer !== [] && count($out) < 1024) {
            $out[] = array_shift($this->localBuffer);
        }
        $advanced = [];
        foreach ($out as $m) {
            $key = $m->topic . ($m->brokerName ?? '') . (string) $m->queueId;
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

    /** seek: dial all three cursors back together and drop buffered messages below the new offset. */
    public function seek(MessageQueue $mq, int $offset): void
    {
        $key = self::mqKey($mq);
        $this->seekOffset[$key] = $offset;
        $this->nextOffset[$key] = $offset;
        $this->consumeOffset[$key] = $offset;
        $kept = [];
        foreach ($this->localBuffer as $m) {
            $sameQueue = $m->topic === $mq->topic
                && ($m->brokerName ?? '') === $mq->brokerName
                && $m->queueId === $mq->queueId;
            if (!$sameQueue || $m->queueOffset >= $offset) {
                $kept[] = $m;
            }
        }
        $this->localBuffer = $kept;
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
        $key = self::mqKey($mq);
        $cached = $this->offsetTable[$key] ?? null;
        if ($cached !== null) {
            return $cached;
        }
        $brokerOffset = $this->requireClient()->queryConsumerOffset($this->consumerGroup, $mq);
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
