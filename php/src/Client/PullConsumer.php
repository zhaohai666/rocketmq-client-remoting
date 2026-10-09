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

// 模块级辅助函数随 ConsumerSupport.php 的类一起被 autoload（见 PushConsumer 同款保护）。
if (!function_exists(__NAMESPACE__ . '\\client_side_tag_filter')) {
    require_once __DIR__ . '/ConsumerSupport.php';
}

/**
 * 拉模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPullConsumer），
 * 移植自 python/client/consumer.py 的 DefaultMQPullConsumer。
 *
 * 与 DefaultMQPushConsumer 的本质区别：调用方自己 ``pull($mq, $offset)`` 逐队列拉、
 * 自己管位点；没有本地缓冲、没有后台拉取线程、不做 rebalance —— 无线程模型对它
 * 天然成立，PHP 适配零偏差。
 */
final class DefaultMQPullConsumer
{
    public string $consumerGroup;
    public string $namespace = '';
    public string $instanceName = MixAll::DEFAULT_INSTANCE_NAME;
    public ?string $clientId = null;
    /** unitName/unitMode/enableStreamRequestType：对应 Java ClientConfig 同名字段。 */
    public ?string $unitName = null;
    public bool $unitMode = false;
    /**
     * Java 的 DefaultMQPullConsumer 每个构造函数都写 ``enableStreamRequestType = true``
     * （:113/:126）—— 与推送消费者默认 false 是两套初值，照抄。
     */
    public bool $enableStreamRequestType = true;
    /**
     * 5.x 新命名空间（对应 Java ClientConfig.namespaceV2）：非空时**每笔**请求带
     * `nsd=true` / `ns=<该值>` 两个扩展头，由 broker 解析到对应 serverless 实例。
     * 与上面那个 `namespace`（客户端给 topic/group 拼 `namespace%` 前缀）是两套机制，
     * 这里**不**改任何资源名。见 NamespaceRpcHook。
     */
    public string $namespaceV2 = '';
    public string $messageModel = MessageModel::CLUSTERING;
    public int $brokerSuspendMaxTimeMillis = 20000;
    public int $consumerPullTimeoutMillis = 10000;
    public int $consumerTimeoutMillisWhenSuspend = 30000;
    /** 路由刷新周期（只在 start() 建 MQClientInstance 时透传一次）。 */
    public int $pollNameServerInterval = 30000;
    /** @var list<string> */
    public array $nameServerAddrs = [];
    public mixed $rpcHook = null;
    /** @var array<string,bool> */
    public array $registerTopics = [];
    /** TLS（Java tls.enable 等价物；null = 交给 env ROCKETMQ_TLS_ENABLE）。 */
    public ?bool $tlsEnable = null;
    /** TLS 细项（caCert/clientCert/clientKey/serverName），语义见 RemotingClient::$tlsOptions。 */
    public ?array $tlsOptions = null;
    public ?MessageQueueListener $messageQueueListener = null;
    /**
     * 队列分配策略（Java :89 字段初值 AVG / getter/setter:196-202）。
     * 本端口拉模式由调用方自己管队列，所以它只作为配置存在并被 start() 校验。
     */
    public ?AllocateMessageQueueStrategy $allocateMessageQueueStrategy;
    /** @var list<object> 投递前过滤钩子（Java :80，start() 时注册进 PullAPIWrapper） */
    public array $filterMessageHookList = [];
    /** 对应 Java pullAPIWrapper.pullFromWhichNodeTable：mqKey → brokerId */
    private array $pullFromWhichNode = [];
    /**
     * 同一张表的**键集**（mqKey → MessageQueue）。Java 的表键就是 MessageQueue，
     * 本端口的表键是字符串，位点/节点决策用不到队列对象，但
     * fetchMessageQueuesInBalance 要把「本实例实际拉过的队列」还给调用方，
     * 所以顺手把对象留着（mqKey 是 topic+brokerName+queueId 直拼，反解不可靠）。
     */
    private array $pulledQueues = [];
    // ---- 消费者心跳（对齐 Java heartbeatBrokerInterval 默认 30s）----
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

    // ---------------- 配置 ----------------

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
     * 对应 Java `ClientConfig#setNamespaceV2`：服务端命名空间（`nsd`/`ns` 扩展头），
     * 不改 topic/group 名；与 `$namespace` 那个「客户端拼 `namespace%` 前缀」的机制互不相干。
     * start() 之后改也生效——钩子每笔请求实时读 {@see MQClientInstance::$namespaceV2}。
     */
    public function setNamespaceV2(string $namespaceV2): void
    {
        $this->namespaceV2 = $namespaceV2;
        if ($this->mqClient !== null) {
            $this->mqClient->namespaceV2 = $namespaceV2;
        }
    }

    /** 对应 Java `ClientConfig#getNamespaceV2`。 */
    public function getNamespaceV2(): string
    {
        return $this->namespaceV2;
    }

    public function setMessageModel(string $model): void
    {
        $this->messageModel = $model;
    }

    public function setMessageQueueListener(MessageQueueListener $listener): void
    {
        $this->messageQueueListener = $listener;
    }

    /** 对应 Java DefaultMQPullConsumer.setAllocateMessageQueueStrategy(:200)：setter 不校验。 */
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

    // ---------------- 投递前过滤钩子 ----------------

    /**
     * 拉模式也要过过滤钩子（Java pullSyncImpl → pullAPIWrapper.processPullResult）。
     * 拉模式的 tag 过滤交给调用方（不传 subscriptionData 时 tagsSet 为空 → 不筛），
     * 这里只跑钩子。
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

    // ---------------- 心跳（把消费组注册给 broker） ----------------

    /**
     * 为心跳准备 broker 地址：把 registerTopics 的路由拉一遍并登记为「在用」。
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

    /**
     * Java `MQClientInstance#prepareHeartbeatData:1031-1045` 为拉模式消费者组出来的那一份。
     *
     * `consumeType()` 恒为 `CONSUME_ACTIVELY`、`consumeFromWhere()` 恒为
     * `CONSUME_FROM_LAST_OFFSET` —— 与推送消费者的 PASSIVELY 是两个口径（broker 侧按
     * consumeType 分流，见 Python 蓝本注释）。订阅集逐条 buildSubscriptionData(topic, "*")
     * 并显式把 **subVersion 置 0** —— 拉模式没有"订阅版本"语义。
     */
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

    /** 向所有已知 broker（含从节点）发一次本组心跳，返回成功台数。 */
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

    /** 心跳成功轮数（真机验证用）。 */
    public function heartbeatCount(): int
    {
        return $this->heartbeatCount;
    }

    /** 心跳到点驱动（对齐 Python 后台心跳循环的循环体；无订阅时可跳过）。 */
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

    // ---------------- 生命周期 ----------------

    public function start(): void
    {
        if ($this->started) {
            return;
        }
        // 对应 Java DefaultMQPullConsumerImpl.checkConfig（:772）——纯本地校验。
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
        // Java :803：策略为 None 直接拒绝启动。
        if ($this->allocateMessageQueueStrategy === null) {
            throw new MQClientException('allocateMessageQueueStrategy is null');
        }
        // Java :712-714：CLUSTERING 才改写 instanceName。
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
        );
        if ($this->rpcHook !== null) {
            $this->mqClient->remotingClient->registerRpcHook($this->rpcHook);
        }
        $this->mqClient->start();
        $this->started = true;
        // 心跳：先刷 registerTopics 的路由，再同步发一轮让 broker 立刻认识本组。
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
        // 优雅注销（对齐 Java DefaultMQPullConsumerImpl.shutdown:689-692）。
        // 本端口没有 Java 的本地位点表，persistConsumerOffset 无对应物
        // （位点由调用方 updateConsumeOffset 直接写给 broker）。
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

    // ---------------- 拉取 ----------------

    /** @return list<MessageQueue> */
    public function fetchSubscribeMessageQueues(string $topic): array
    {
        // Java DefaultMQPullConsumerImpl:142 读订阅信息（读位、不筛 master）。
        return $this->requireClient()->getTopicSubscribeInfo($topic);
    }

    /**
     * 本实例「平衡后」应负责的队列（Java MQPullConsumer:187，官方
     * example/simple/PullConsumer.java:62 就靠它决定去拉哪些队列）。
     *
     * Java（DefaultMQPullConsumerImpl:120-135）读的是后台 rebalance 填出来的
     * processQueueTable；本端口拉模式没有那条后台线程，所以按 RebalanceImpl.rebalanceByTopic
     * 的同一条公式当场算：mqAll=订阅信息（读位口径，与 fetchSubscribeMessageQueues 同源）、
     * cidAll=GET_CONSUMER_LIST_BY_GROUP、分配策略取本实例那一份。公式与推送消费者的
     * rebalance 共用一份口径，两处一旦分叉，同一队列会被两个实例同时认领。
     *
     * 拿不到路由/消费者列表时**保留现有分配**（退回本实例实际拉过的队列），
     * 绝不回退成"独占全部队列"——那会让同组多实例互相重复消费（同 python 端口）。
     *
     * @return list<MessageQueue>
     */
    public function fetchMessageQueuesInBalance(string $topic): array
    {
        $client = $this->requireClient(); // Java isRunning()：未启动直接抛 MQClientException
        $pulled = array_values(array_filter(
            $this->pulledQueues,
            static fn(MessageQueue $mq): bool => $mq->topic === $topic,
        ));
        if ($this->messageModel === MessageModel::BROADCASTING) {
            // Java rebalanceByTopic 对 BROADCASTING 不查消费者列表、全量分配。
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

    /** 按 topic 收口 + house 排序口径（topic → brokerName → queueId），保证多次调用稳定。 */
    private function sortedQueues(array $mqs, string $topic): array
    {
        $out = [];
        foreach ($mqs as $mq) {
            // Java :128-131 逐个比对表键的 topic；别让策略的意外返回值把别的 topic 混进拉取循环。
            if ($mq instanceof MessageQueue && $mq->topic === $topic) {
                $out[implode("\0", AllocationHelper::mqSortKey($mq))] = $mq;
            }
        }
        ksort($out);
        return array_values($out);
    }

    /**
     * 短轮询拉取（对应 Java pullSyncImpl，block=false）。
     *
     * ⚠ sysFlag 的 suspend 位是 **false** —— 这是**短轮询**不挂起。曾在这里写成
     * suspend=true：broker 在队尾会挂起到 brokerSuspendMaxTimeMillis（默认 20s），
     * 而客户端 5s 就超时 → RemotingTimeoutException（真机必现，见 INVARIANTS B3）。
     */
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
        // Java :248：sysFlag = buildSysFlag(false, block=false, true, false)。
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
            // Java：TAG 类型时 subVersion 传 0（isTagType ? 0L : subVersion）
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

    /** 长轮询拉取（对应 Java pullBlockIfNotFound，block=true → 挂起等消息）。 */
    public function pullBlockIfNotFound(
        MessageQueue $mq,
        string $subExpression,
        int $offset,
        int $maxNums,
    ): PullResult {
        $client = $this->requireClient();
        $sub = FilterAPI::buildSubscriptionData($mq->topic, $subExpression);
        // block=true：suspend=True 让 broker 挂起到有消息；超时用 consumerTimeoutMillisWhenSuspend。
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

    // ---------------- Offset 管理 ----------------

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
        // Java MQAdminImpl:250：与 max/min/search 同一个形状 —— 只认 master，刷一次路由重查。
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

    /**
     * 消息回投（对应 Java DefaultMQPullConsumer.sendMessageBack）。
     *
     * 注意两点（真机踩过）：
     * 1. 地址靠 findBrokerAddressInPublish 反查**发布地址表**（只认 master）；
     * 2. 与 Java 的**有意差异**：Java 在失败时会吞掉异常、改用内部默认生产者把消息直接发到
     *    %RETRY%group。本实现不做这个兜底 —— 回投失败就抛，让调用方看见。
     */
    public function sendMessageBack(MessageExt $msg, int $delayLevel): void
    {
        $client = $this->requireClient();
        // 从节点不接 CONSUMER_SEND_MSG_BACK（master 专属），所以这里只认 brokerId=0
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
        // ⚠ 这里**不能**照抄 Java 弃用的 sendMessageBack（它直接传 getMaxReconsumeTimes()，
        // 默认 -1）。客户端版本 ≥ V3_4_9 后 broker 会无条件采用该字段，-1 会让
        // reconsumeTimes(0) >= -1 成立、消息直接进 %DLQ%。留 null 交给订阅组的 retryMaxTimes。
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

/**
 * 轻量拉取消费者（对应 org.apache.rocketmq.client.consumer.DefaultLitePullConsumer），
 * 移植自 python/client/consumer.py 的 DefaultLitePullConsumer。
 *
 * 两种模式：**subscribe 模式**（自动 rebalance + 本地缓冲 + poll）与 **assign 模式**
 * （调用方显式指定队列）。位点默认 autoCommit（poll 交出去即向 broker 提交）。
 *
 * ## PHP 单线程适配（有意偏差，对应 PORTING.md「异步模型」）
 *
 * Python 的后台 pull 服务线程改为 ``tick()`` 驱动：每轮先按 1s 节奏 rebalance
 * （对齐 Python _pull_service_loop 的 rebalance 节流），再对每条已分配队列做一次
 * 短轮询拉取（Python 本来就是 suspend=False 的短轮询，语义零偏差）；poll() 的
 * Condition 等待改为 10ms 粒度的有界自旋（deadline 语义不变）。
 */
final class DefaultLitePullConsumer
{
    public string $consumerGroup;
    public string $namespace = '';
    public string $instanceName = MixAll::DEFAULT_INSTANCE_NAME;
    public ?string $clientId = null;
    public ?string $unitName = null;
    public bool $unitMode = false;
    /** Java 的 DefaultLitePullConsumer 每个构造函数都写 ``enableStreamRequestType = true``。 */
    public bool $enableStreamRequestType = true;
    /**
     * 5.x 新命名空间（对应 Java ClientConfig.namespaceV2）：非空时**每笔**请求带
     * `nsd=true` / `ns=<该值>` 两个扩展头，由 broker 解析到对应 serverless 实例。
     * 与上面那个 `namespace`（客户端给 topic/group 拼 `namespace%` 前缀）是两套机制，
     * 这里**不**改任何资源名。见 NamespaceRpcHook。
     */
    public string $namespaceV2 = '';
    public string $messageModel = MessageModel::CLUSTERING;
    /** @var list<string> */
    public array $nameServerAddrs = [];
    public mixed $rpcHook = null;
    /** TLS（Java tls.enable 等价物；null = 交给 env ROCKETMQ_TLS_ENABLE）。 */
    public ?bool $tlsEnable = null;
    /** TLS 细项（caCert/clientCert/clientKey/serverName），语义见 RemotingClient::$tlsOptions。 */
    public ?array $tlsOptions = null;

    /** subscribe 模式的订阅表（topic => sub_expression）。 */
    public array $subscription = [];
    /** @var array<string,SubscriptionData> */
    public array $subscriptionData = [];
    /** assign 模式下指定 topic 的 tag 过滤表达式。 */
    private array $assignSubExpr = [];
    private bool $assignMode = false;
    /** @var array<string,MessageQueue> 已分配队列（key = mqKey） */
    private array $assigned = [];

    /** rebalance 算法（subscribe 模式）。 */
    public AllocateMessageQueueStrategy $allocateMessageQueueStrategy;

    // 拉取 / poll 配置
    public string $consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
    /** CONSUME_FROM_TIMESTAMP 的起点（格式 yyyyMMddHHmmss），默认 30 分钟前。 */
    public string $consumeTimestamp;
    public int $pullBatchSize = 32;
    public int $pollTimeoutMillis = 5000;
    public int $pollNameServerInterval = 30000;
    public bool $autoCommit = true;
    public int $autoCommitIntervalMillis = 5000;
    public int $consumerTimeoutMillisWhenSuspend = 30000;
    public int $brokerSuspendMaxTimeMillis = 20000;
    public int $pullIntervalMillis = 50;  // 队尾空轮询时的退避，避免空转打爆 broker

    // 运行状态
    public ?MQClientInstance $mqClient = null;
    private bool $started = false;
    private bool $running = false;

    /** @var list<MessageExt> 本地缓冲 */
    private array $localBuffer = [];
    /** @var array<string,int> 拉取游标（key = mqKey） */
    private array $nextOffset = [];
    /** @var array<string,int> 已消费游标（poll 交出去才前进） */
    private array $consumeOffset = [];
    /** @var array<string,int> 提交落点（对位 Java OffsetStore 的内存位点表） */
    private array $offsetTable = [];
    /** @var array<string,int> */
    private array $seekOffset = [];
    /** Java nextAutoCommitDeadline 初值 -1 ⇒ 第一次 poll 就提交一次。 */
    private float $nextAutoCommitDeadline = -1.0;
    /** @var array<string,bool> */
    private array $paused = [];
    private ?object $messageQueueListener = null;
    private float $lastRebalanceTs = 0.0;
    private float $nextHeartbeatAt = 0.0;

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

    // ---------------- 命名空间 / 配置 ----------------

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
     * 对应 Java `ClientConfig#setNamespaceV2`：服务端命名空间（`nsd`/`ns` 扩展头），
     * 不改 topic/group 名；与 `$namespace` 那个「客户端拼 `namespace%` 前缀」的机制互不相干。
     * start() 之后改也生效——钩子每笔请求实时读 {@see MQClientInstance::$namespaceV2}。
     */
    public function setNamespaceV2(string $namespaceV2): void
    {
        $this->namespaceV2 = $namespaceV2;
        if ($this->mqClient !== null) {
            $this->mqClient->namespaceV2 = $namespaceV2;
        }
    }

    /** 对应 Java `ClientConfig#getNamespaceV2`。 */
    public function getNamespaceV2(): string
    {
        return $this->namespaceV2;
    }

    // ---------------- 订阅 / 分配 ----------------

    public function subscribe(string $topic, string $subExpression = '*'): void
    {
        $this->assignMode = false;
        $ns = $this->withNamespace($topic);
        $this->subscription[$ns] = $subExpression;
        // 同步构建 SubscriptionData，供心跳把 tag 订阅注册给 broker。
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

    /**
     * assign 模式下给某个 topic 的队列指定 tag 过滤表达式（对应 Java setSubExpressionForAssign）。
     */
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
     * assign 模式：显式指定队列，不走 rebalance。
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
        // Java assignedMessageQueue.updateAssignedMessageQueue：撤掉的队列连着整份
        // MessageQueueState 丢掉（拉取游标与已消费游标一起消失）。**不**动 offsetTable。
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

    // ---------------- 生命周期 ----------------

    public function start(): void
    {
        if ($this->started) {
            return;
        }
        // 对应 Java DefaultLitePullConsumerImpl.checkConfig（:415）：纯本地校验，最先做。
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
        // Java :435：策略为 null 直接拒绝启动。
        if (!isset($this->allocateMessageQueueStrategy)) {
            throw new MQClientException('allocateMessageQueueStrategy is null');
        }
        // Java :287-289：CLUSTERING 才改写 instanceName。
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
        );
        if ($this->rpcHook !== null) {
            $this->mqClient->remotingClient->registerRpcHook($this->rpcHook);
        }
        $this->mqClient->start();
        // assign 模式在 start 时解析初始位点。
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
        // 先把 tag 订阅注册给 broker（心跳），再启动拉取。心跳要靠「已知 broker 列表」，
        // 而该列表只来自 topic 路由表：start 时先把订阅/指派的 topic 拉一遍路由。
        $this->refreshRouteForHeartbeat();
        $this->running = true;
        $this->sendHeartbeatToAllBroker();
        $this->nextHeartbeatAt = microtime(true) + 5.0;
        $this->started = true;
    }

    /** 为心跳准备 broker 地址：拉取并登记本实例关注的 topic 路由。 */
    private function refreshRouteForHeartbeat(): void
    {
        // ⚠ start() 在 $started=true 之前调用本方法，不能用 requireClient()（死锁）。
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
        // Java 的 shutdown 走 persistConsumerOffset()：与 auto_commit 无关。
        // 自动提交模式再多走一步 commit()：本端口没有 Java 那份 5s 定时器，"poll 交出去
        // 但还没到截止时刻"的位点得在这里补上，否则重启后从上一格重投。
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

    // ---------------- 心跳（把 tag 订阅注册给 broker）----------------

    private function buildHeartbeat(): HeartbeatData
    {
        $hb = new HeartbeatData($this->clientId ?? '');
        $cd = new ConsumerData(
            $this->consumerGroup,
            ConsumeType::CONSUME_ACTIVELY,
            $this->messageModel,
            $this->consumeFromWhere,
        );
        // consumeType 是 CONSUME_ACTIVELY 而**不是**推送消费者的 PASSIVELY
        //（Java DefaultLitePullConsumerImpl.consumeType():1111-1112）。
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
        // 每台都发（主 + 从）：与推送消费者同一条 Java 依据。
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
     * tick 驱动（PHP 适配）：rebalance（1s 节流）+ 全队列短轮询拉取 + 心跳。
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
     * 单队列一次拉取（对齐 Python _pull_one）：短轮询；FOUND/NO_NEW_MSG/NO_MATCHED_MSG/
     * OFFSET_ILLEGAL 都推进拉取游标到 nextBeginOffset；刹车：**在途结果不许盖掉刚 seek /
     * 刚被撤走的位点**（游标还是不是我发请求时的那个值）。
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
        // 短轮询（suspend=False）；lite_pull=True 置 FLAG_LITE_PULL_MESSAGE，
        // 请求码切到 LITE_PULL_MESSAGE(361)。
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
        // Java :982-998：一轮拉取回来之后无论哪种状态都推进游标；刹车只有一只。
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
            // ⚠ tagsSet 是 list，不是 map：isset($tagsSet[$tag]) 永远 miss（同 ConsumerSupport）。
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
        // 对位 Java ``maybeAutoCommit``：到点提交**全部**已分配队列；提交的是"已消费游标"
        // 而不是拉取游标 —— 缓冲里还没交给调用方的消息不算已消费。
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
            // 允许先 assign 再 start，但**不能**在没问过 broker 的情况下给出 0
            // —— assign 会把结果存进 nextOffset，起点会被永久钉死在 0。
            throw new MQClientException('The lite pull consumer is not running');
        }
        // 有已提交位点则沿用（保证重启续消费）
        try {
            $off = $this->mqClient->queryConsumerOffset($this->consumerGroup, $mq);
            if ($off !== null) {
                return $off;
            }
        } catch (\Throwable) {
            // ignore
        }
        if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
            // Java RebalanceLitePullImpl:FIRST_OFFSET 分支与 push 同形（result = 0L）。
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

    // ---------------- rebalance（subscribe 模式）----------------

    private function rebalance(): void
    {
        // 对应 Java DefaultLitePullConsumerImpl.rebalance：与 push 完全相同的
        // rebalanceByTopic 路径，mqAll / cidAll 都要先排序。
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
                // 对应 Java rebalanceByTopic 的 catch (Throwable)：本轮跳过该 topic
                //（保留它当前的分配），绝不能因为一次策略异常就把队列撤走。
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
                // Java RebalanceLitePullImpl#removeUnnecessaryMessageQueue：先 persist(mq)
                // 再 removeOffset(mq)——撤手之前把最后一次提交的位点补发出去。
                $this->persistOffsetByKey($key);
                // AssignedMessageQueue 的条目一起丢掉：留着 seekOffset 就是让这条队列
                // 哪天回到本实例时静默跳回用户很久以前手动钉过的位置。
                unset($this->nextOffset[$key], $this->consumeOffset[$key],
                    $this->offsetTable[$key], $this->seekOffset[$key]);
            }
            if ($this->messageQueueListener !== null) {
                try {
                    // MessageQueueListener 接口签名是 (topic, mqAll, mqDivided)；
                    // lite 的 rebalance 是全体订阅一起算，这里逐 topic 通知。
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

    // ---------------- poll / 位点 ----------------

    /**
     * 从本地缓冲取批量消息（Java poll 语义：最多 1024 条；deadline 内等待）。
     *
     * @return list<MessageExt>
     */
    public function poll(?int $timeout = null): array
    {
        $timeout = $timeout ?? $this->pollTimeoutMillis;
        // Java poll() 进来先按截止时刻试一次自动提交（拿缓冲之前做）。
        if ($this->autoCommit) {
            $this->maybeAutoCommit();
        }
        $deadline = microtime(true) + $timeout / 1000.0;
        while ($this->localBuffer === []) {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                return [];
            }
            // PHP 适配：Condition.wait → 10ms 粒度有界自旋。
            usleep(10000);
        }
        $out = [];
        while ($this->localBuffer !== [] && count($out) < 1024) {
            $out[] = array_shift($this->localBuffer);
        }
        // 对位 Java poll()：消息交到调用方手上才推进"已消费游标"；拉取游标一律不动。
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

    /** seek：三张游标一起拨回去，并丢弃缓冲里低于新位点的旧消息。 */
    public function seek(MessageQueue $mq, int $offset): void
    {
        $key = self::mqKey($mq);
        $this->seekOffset[$key] = $offset;
        $this->nextOffset[$key] = $offset;
        // Java 的 seek 只置 seekOffset，下一次拉取时才写进 consumeOffset
        //（"跳回去"意味着"那里之前都还没消费"）。
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

    /** 拉取游标（观测点：真机对拍要看的正是"拉了多少"与"交了多少"这两格的差）。 */
    public function pullCursorOf(MessageQueue $mq): int
    {
        return $this->nextOffset[self::mqKey($mq)] ?? -1;
    }

    /** 已消费游标（``poll()`` 交出去的那一格）。 */
    public function consumeCursorOf(MessageQueue $mq): int
    {
        return $this->consumeOffset[self::mqKey($mq)] ?? -1;
    }

    /** 内存位点表里那一格（``persist=False`` 提交但还没落盘的值就在这）。 */
    public function pendingCommitOf(MessageQueue $mq): int
    {
        return $this->offsetTable[self::mqKey($mq)] ?? -1;
    }

    /**
     * Java ``committed()`` 走 ``offsetStore.readOffset(MEMORY_FIRST_THEN_STORE)``：
     * 先看内存位点表，再问 broker，并把 broker 的值回填进表里。
     */
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

    /** Java ``OffsetStore#persist(mq)``：只把这一条队列的内存位点发给 broker。 */
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

    /**
     * Java ``RemoteBrokerOffsetStore#persistAll(Set)``：落在 ``$keys`` 里的写给 broker，
     * **不在**其中的条目顺手从表里删掉。
     *
     * @param iterable<string>|null $keys mqKey 集合；null = 全部
     */
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
            // 撤走的队列没有 MessageQueue 无法发请求；Java 该场景下条目也已不在表里。
            return;
        }
        $this->persistOffset($mq);
    }

    /**
     * 提交消费位点。三个入口对位 Java 的三个重载：
     *   commit()                    → commitAll()：按"已消费游标"提交所有已分配队列
     *   commit([{mqKey => offset}]) → commit(Map, persist)：调用方指定位点
     *   commit([mqKey, ...])        → commit(Set, persist)：只提交这几条队列
     *
     * 指定位点**只改提交位置，不改拉取游标**。
     *
     * @param array<string,int>|list<string>|null $offsets
     */
    public function commit(?array $offsets = null, bool $persist = true): void
    {
        if ($offsets === null) {
            // Java commitAll()：只遍历当下持有的队列
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
                // Java 的 processQueue != null && !isDropped() 守卫：不是本实例持有的队列
                // 一律不替它提交，静默跳过。
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
        return $this->requireClient()->getTopicSubscribeInfo($this->withNamespace($topic));
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

/** 便捷监听器包装：把消费逻辑转成纯函数（对应 Python SimpleMessageListener）。 */
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
