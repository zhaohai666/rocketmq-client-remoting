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
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\ConsumeType;
use RocketMQ\Remoting\Protocol\MessageModel;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\NamespaceUtil;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\ConsumerData;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\RemotingTimeoutException;

// 本文件用到的模块级辅助函数随 ConsumerSupport.php 的类一起被 autoload；
// 为防"只调函数不碰类"的调用次序，这里显式保证文件已加载。
if (!function_exists(__NAMESPACE__ . '\\client_side_tag_filter')) {
    require_once __DIR__ . '/ConsumerSupport.php';
}

/**
 * 推模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPushConsumer），
 * 移植自 python/client/consumer.py 的 DefaultMQPushConsumer。
 *
 * ## PHP 单线程适配（PORTING.md「异步模型」，有意偏差，改动前先读）
 *
 * Python 用 6 类后台线程（心跳 / 重平衡 / 每队列拉取 / 分发 / 位点落盘 / 顺序锁）+
 * POP 线程池；PHP 无线程，全部收敛为调用方驱动的 ``tick()``：
 *
 * - **心跳 / 重平衡 / 位点落盘 / 顺序锁 / 过期清扫**：改为 tick 内的到点检查
 *   （周期与 Java 一致：30s / 20s / 10s+persistConsumerOffsetInterval / 20s / consumeTimeout 分钟）；
 * - **拉取循环**：Java 是每队列一个长轮询线程（suspend=true）；PHP 单线程下长轮询会
 *   饿死其它队列，改为**短轮询**（suspend=false）+ 空结果空闲退避
 *   ``pullIntervalMillis``（默认 1000ms）。语义代价：空队列的位点修正
 *   （correctTagsOffset）从 ~15s 一次变 ~1s 一次，只会更勤不会更懒；
 *   吞吐代价：单队列单次 tick 至多 ``maxPullsPerQueuePerTick`` 轮短轮询；
 * - **消费**：Python 的分发线程即同步消费；PHP 在 tick 内分发同样同步执行，
 *   顺序消费的"挂起重试"从 sleep 改为 ``suspendedUntil`` 表（tick 到点前跳过该队列）；
 * - **POP**：同上，pollTime=0 短轮询 + 空闲退避；消费批次内联执行（不再建
 *   ConsumeExecutor —— 单线程下"真实并发度==core"没有意义，保留声明值可观测）。
 */
final class DefaultMQPushConsumer
{
    // ---------------- POP 模式（5.x 轻量消费）配置 ----------------
    //
    // Java 的 push-consumer POP 走 **broker 侧分配**：QUERY_ASSIGNMENT(400) 拿
    // MessageQueueAssignment(mode=POP)，客户端不做 rebalance。本项目**刻意不实现这条路径**，
    // 而是复用已有的**客户端 rebalance**：队列由本地按分配策略算出，然后每队列独立 POP。
    // 语义等价（都是"每队列一个 POP 循环 + ack 确认"），差别只在于"谁决定分哪些队列"
    // —— 这是有意差异，改动前请先读 Python 蓝本同位置的注释。

    public string $consumerGroup;
    public ?bool $tlsEnable = null;
    /** TLS 细项（caCert/clientCert/clientKey/serverName），语义见 RemotingClient::$tlsOptions。 */
    public ?array $tlsOptions = null;
    public string $namespace = '';
    public string $instanceName = MixAll::DEFAULT_INSTANCE_NAME;
    public ?string $clientId = null;
    /** unitName/unitMode/enableStreamRequestType：对应 Java ClientConfig 同名字段。 */
    public ?string $unitName = null;
    public bool $unitMode = false;
    public bool $enableStreamRequestType = false;
    /** @var 'CLUSTERING'|'BROADCASTING' */
    public string $messageModel = MessageModel::CLUSTERING;
    public string $consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
    /** CONSUME_FROM_TIMESTAMP 的起点（格式 yyyyMMddHHmmss），默认 30 分钟前。 */
    public string $consumeTimestamp;

    // ---- 消费线程池（声明值；Java 5.x 默认 min=max=20）----
    public int $consumeThreadMin = 20;
    public int $consumeThreadMax = 20;
    public int $adjustThreadPoolNumsThreshold = 100000;
    private int $corePoolSize;
    /** ProcessQueue.msgAccCnt（按队列 key）：最近一次拉取算出的"积压条数"。 */
    /** @var array<string,int> */
    private array $msgAccCntTable = [];
    public int $consumeConcurrentlyMaxSpan = 2000;
    public int $pullThresholdForQueue = 1000;
    public int $pullThresholdSizeForQueue = 100;
    public int $pullThresholdForTopic = -1;
    public int $pullThresholdSizeForTopic = -1;
    public int $pullInterval = 0;
    /**
     * 拉取 RPC 超时。⚠ Java/Python 是 30000（长轮询挂起预算）；PHP 短轮询下
     * broker 立即返回，默认压到 5000 —— 卡死的连接不必等 30s。
     */
    public int $pullTimeoutMillis = 5000;
    /** 短轮询的空闲退避（PHP 适配新增；对应 Java pullInterval 的运行时角色）。 */
    public int $pullIntervalMillis = 1000;
    /** 单队列单次 tick 内最多短轮询轮数（PHP 适配新增，吞吐旋钮）。 */
    public int $maxPullsPerQueuePerTick = 8;
    /** 长轮询的 suspend 时长（保留字段：随请求头透传，PHP 短轮询下 broker 忽略）。 */
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
    /** @var array<string,int> 拉取游标（nextBeginOffset） */
    private array $offsetTable = [];
    /** @var array<string,list<MessageExt>> 已拉未消费缓冲（Java ProcessQueue） */
    private array $pending = [];
    /** @var array<string,MessageQueue> 已分配队列注册表 */
    private array $mqMap = [];
    /** @var array<string,int> 已消费位点（持久化到 broker 的是它） */
    private array $consumeOffsets = [];
    /** @var array<string,bool> 被 OFFSET_ILLEGAL 纠错冻结的位点 */
    private array $frozenOffsets = [];
    /** @var array<string,int> 每队列 ProcessQueue 代号（撤销时 +1） */
    private array $queueEpoch = [];
    /** @var array<string,bool> broker LOCK_BATCH_MQ 确认锁定成功的队列（顺序消费） */
    private array $lockOk = [];
    private int $flowControlTriggered = 0;
    /** @var array<string,float> 每队列最近一次发起拉取/弹出的时刻（秒） */
    private array $lastPullTable = [];
    /** @var array<string,int> MessageQueue key → 下一轮拉取应选的 brokerId */
    private array $pullFromWhichNode = [];
    /** @var array<string,MessageQueue> 当前分给本实例的队列集（key => mq） */
    private array $assigned = [];
    private bool $rebalanceNow = false;
    /** @var array<string,float> 顺序消费挂起重试的到点时刻（秒，PHP 适配） */
    private array $suspendedUntil = [];
    private float $startTime = 0.0;

    // ---- 心跳（对齐 Java heartbeatBrokerInterval 默认 30s）----
    public bool $heartbeatEnabled = true;
    public int $heartbeatIntervalMillis = 30000;
    /** 路由刷新周期（只在 start() 建 MQClientInstance 时透传一次）。 */
    public int $pollNameServerInterval = 30000;
    /** 已消费位点落盘周期（对齐 Java persistConsumerOffsetInterval 默认 5000ms）。 */
    public int $persistConsumerOffsetInterval = 5000;
    private int $heartbeatCount = 0;

    // ---- POP 模式 ----
    public bool $popMode = false;
    public int $popInvisibleTime = 60000;
    public int $popBatchNums = 32;
    public int $popThresholdForQueue = 96;
    /** @var list<int> */
    public array $popDelayLevel = ConsumerDefaults::POP_DELAY_LEVEL;
    /** POP 长轮询挂起时长（PHP 短轮询下传 0）。 */
    public int $popPollTimeMillis = 15000;
    public int $popTimeoutMillis = 5000;
    public int $popIntervalMillis = 1000;
    public int $maxPopsPerQueuePerTick = 8;
    /** @var array<string,PopProcessQueue> */
    private array $popQueues = [];
    /** @var array<string,bool> POP 顺序请求去重集（key = mqKey） */
    private array $popOrderlyRequests = [];

    // ---- 消息轨迹 ----
    public bool $enableTrace = false;
    public ?string $traceTopic = null;
    public int $traceMsgBatchNum = 10;
    /** @var list<object> */
    public array $consumeMessageHookList = [];
    /** @var list<object> */
    public array $filterMessageHookList = [];
    public ?AsyncTraceDispatcher $traceDispatcher = null;

    /** 消费统计（Java ConsumerStatsManager），start() 时绑定到实例的 manager。 */
    private ?ConsumerStatsManager $statsManager = null;

    // ---- tick 定时器（PHP 适配：原后台线程的到点时刻，秒）----
    private float $nextHeartbeatAt = 0.0;
    private float $nextRebalanceAt = 0.0;
    private float $nextPersistAt = 0.0;
    private float $nextLockAt = 0.0;
    private float $nextCleanAt = 0.0;
    /** PHP 适配：空结果队列的下次可拉时刻（key => 秒） */
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
        $this->corePoolSize = $this->consumeThreadMin;
        $this->consumeTimestamp = date('YmdHis', time() - 30 * 60);
        $this->allocateStrategy = new AllocateMessageQueueAveragely();
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

    /** 对应 Java ``MQConsumerInner#subscriptions()``：给 checkClientInBroker 用。 */
    public function subscriptions(): array
    {
        return array_values($this->subscriptionData);
    }

    // ---------------- 订阅 ----------------

    /**
     * 订阅 topic（对应 Java DefaultMQPushConsumerImpl#subscribe:1265-1275）。
     * start() 之后仍可调用：订阅表是活的，put 进去之后立即发一次心跳。
     */
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

    /**
     * 取消订阅。Java 这里**只删表项、不发心跳**（后一轮心跳自然带出新订阅集），
     * 所以刻意不调 notifySubscriptionChanged。
     */
    public function unsubscribe(string $topic): void
    {
        unset($this->subscriptionData[$this->withNamespace($topic)]);
    }

    /**
     * 订阅表新增/更新后的立即动作，对齐 Java subscribe 里的第二句：
     * ``sendHeartbeatToAllBrokerWithLock`` —— broker 的 ConsumerManager 只在收到
     * 心跳时才把 topic→group 记进它自己那张表，晚一轮就是默认 30s 的空窗。
     */
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

    // ---------------- 配置数值闸门 ----------------

    /**
     * 对应 Java ``DefaultMQPushConsumerImpl#checkConfig`` 的数值段（:1099-1209），
     * 逐条照抄 Java 的**顺序、区间和文案**。
     */
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

    /** 解析 consumeTimestamp（解析不了必须硬失败，见 Python _consume_timestamp_millis）。 */
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

    // ---------------- 生命周期 ----------------

    public function start(): void
    {
        if ($this->started) {
            return;
        }
        // 消费组也拼命名空间（对齐 Java DefaultMQPushConsumer.start:763）。必须在算
        // 重试主题之前：重试主题 = %RETRY% + 带前缀的组名。
        if ($this->namespace !== '') {
            $this->consumerGroup = NamespaceUtil::wrapNamespace($this->namespace, $this->consumerGroup);
        }
        // 对应 Java checkConfig：先 Validators.checkGroup，再挡 DEFAULT_CONSUMER。
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
        // Java :1058：启动即无条件校验起点时间。
        $this->consumeTimestampMillis();
        // Java :1067：策略为 None 直接拒绝启动。
        if (!isset($this->allocateStrategy)) {
            throw new MQClientException('allocateMessageQueueStrategy is null');
        }
        // Java 数值段：排在所有 null 检查之后、任何网络动作之前。
        $this->checkConfigRanges();
        // Java :934-936：只有 CLUSTERING 才 changeInstanceNameToPID。
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
            unitName: $this->unitName,
            pollNameServerInterval: $this->pollNameServerInterval,
            tlsOptions: $this->tlsOptions,
        );
        if ($this->rpcHook !== null) {
            $this->mqClient->remotingClient->registerRpcHook($this->rpcHook);
        }
        $this->mqClient->start();
        // 动态 name server 回填。
        if ($this->nameServerAddrs === [] && $this->mqClient->nameServerAddrs !== []) {
            $this->nameServerAddrs = array_values($this->mqClient->nameServerAddrs);
        }
        // 消费统计（实例级共享）。
        $this->statsManager = $this->mqClient->consumerStatsManager;
        $this->mqClient->registerConsumer($this->consumerGroup, $this);
        $this->started = true;
        $this->startTime = microtime(true);
        $this->nextHeartbeatAt = $this->startTime + $this->heartbeatIntervalMillis / 1000.0;
        $this->nextRebalanceAt = $this->startTime + 20.0;
        $this->nextPersistAt = $this->startTime + 10.0; // Java initialDelay = 1000 * 10
        $this->nextLockAt = $this->startTime;
        $this->nextCleanAt = $this->startTime + max(1, $this->consumeTimeout) * 60.0;
        // 集群模式自动订阅重试 topic（对齐 Java copySubscription → retryTopic）。
        if ($this->messageModel !== MessageModel::BROADCASTING) {
            $retryTopic = MixAll::getRetryTopic($this->consumerGroup);
            if (!isset($this->subscriptionData[$retryTopic])) {
                // SUB_ALL 下 tagsSet / codeSet 都为**空**（不是 {"*"}）。
                $this->subscriptionData[$retryTopic] = FilterAPI::buildSubscriptionData($retryTopic, '*');
            }
        }
        // 对齐 Java start 顺序：拉一次路由 → CHECK_CLIENT_CONFIG → 心跳 → 立即 rebalance。
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
        // 首轮分配必须同步完成：否则拉取会在空分配集上白转，直到第一轮 rebalance。
        try {
            $this->doRebalance();
        } catch (\Throwable $e) {
            Logger::warning('initial rebalance failed: ' . $e->getMessage());
        }
        // 轨迹分发器（消费循环无线程，故最后启动即可）。
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
        $this->started = false;
        // POP：把所有队列标成 dropped，在途批次不再 ack（交给 broker 复活重投）。
        if ($this->popMode) {
            foreach ($this->popQueues as $pq) {
                $pq->setDropped(true);
            }
            $this->popQueues = [];
            $this->popOrderlyRequests = [];
        }
        // 退出前把已消费位点持久化一次（对齐 Java MQClientInstance.shutdown）。
        try {
            $this->persistOffsetsOnce();
        } catch (\Throwable $e) {
            Logger::debug('persist offsets on shutdown failed: ' . $e->getMessage());
        }
        // 顺序消费清退时解锁队列（对齐 Java ConsumeMessageOrderlyService.shutdown → unlockAll）。
        // ⚠ 不能走 requireClient()：started 已置 false，它会抛"not started"把解锁变成死代码。
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
        // 优雅注销：立刻从各 broker 的 ConsumerManager 摘除，不必等心跳超时。
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
        // 轨迹分发器最后关（flush 剩余轨迹）。
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

    // ---------------- 心跳（消费者注册） ----------------

    /**
     * 订阅 topic 的路由拉一遍，顺带把 broker 地址表填上（心跳要靠它）。
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

    private function buildHeartbeat(): HeartbeatData
    {
        $hb = new HeartbeatData($this->clientId ?? '');
        $cd = new ConsumerData(
            $this->consumerGroup,
            ConsumeType::CONSUME_PASSIVELY,
            $this->messageModel,
            $this->consumeFromWhere,
        );
        // Java `MQClientInstance`:1039 把 `impl.isUnitMode()` 写进 ConsumerData。
        $cd->unitMode = $this->unitMode;
        foreach ($this->subscriptionData as $sub) {
            $cd->subscriptionDataSet[] = $sub;
        }
        $hb->consumerDataSet[] = $cd;
        return $hb;
    }

    /**
     * 向所有已知 broker（主 + 从）发心跳，返回成功台数。
     *
     * 消费者**必须**注册到 broker：broker 的 ConsumerManager 只有收到心跳才知道
     * 消费组里有哪些 clientId，rebalance 的 GET_CONSUMER_LIST_BY_GROUP 才有返回。
     * 从节点漏发不是"少一发冗余"：从节点收不到心跳时，它会给指向自己的拉取回
     * SUBSCRIPTION_NOT_EXIST（详见 Python 蓝本 _send_heartbeat_to_all_broker 注释）。
     */
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

    /** 当前分给本实例的队列数（真机验证多实例分配用）。 */
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

    /** 分给本实例、属于指定 topic 的队列数（按 (topic,broker,queueId) 计，真机验证分配用）。 */
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

    // ---------------- POP 观察点（真机验证 live_pop 用，对齐 Java 可观察口径） ----------------

    /** POP：在途 PopProcessQueue 数。 */
    public function popQueueCount(): int
    {
        return count($this->popQueues);
    }

    /** POP：ACK 债务总和（已 pop 未 ack 的消息数；正常消费后应归零）。 */
    public function popWaitAckCount(): int
    {
        $n = 0;
        foreach ($this->popQueues as $pq) {
            $n += $pq->waitAckCount();
        }
        return $n;
    }

    /**
     * 本地位点表条目数。POP 的游标在 broker 的 revive 队列里，**客户端不持有
     * 位点表**——POP 模式下此值应恒为 0（pull 模式下每个已解析队列一条）。
     */
    public function localOffsetCount(): int
    {
        return count($this->consumeOffsets);
    }

    // ---------------- tick 驱动（PHP 适配核心） ----------------

    /**
     * 驱动一轮：实例事件泵 + 心跳 / 重平衡 / 拉取 + 分发消费 + 位点落盘 + 顺序锁。
     * 调用方在循环里周期调用（空转时建议 sleep 50ms）。
     *
     * @param int|null $nowMillis 可注入的当前时间（毫秒），为空时用内部时钟。
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
        // ① 实例事件泵（路由刷新 / 统计采样 / pending 动作）。
        $client->tick($nowMs);

        // ② 心跳。
        if ($this->heartbeatEnabled && $now >= $this->nextHeartbeatAt) {
            $this->nextHeartbeatAt = $now + $this->heartbeatIntervalMillis / 1000.0;
            try {
                $this->sendHeartbeatToAllBroker();
            } catch (\Throwable $e) {
                Logger::debug('heartbeat loop error: ' . $e->getMessage());
            }
        }

        // ③ 重平衡（启动阶段且无分配时快速重试，对齐 Python _rebalance_loop）。
        if ($this->rebalanceNow || $now >= $this->nextRebalanceAt) {
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

        // ④ 顺序锁（每 20s；对齐 Python _lock_loop）。
        if ($this->isOrderly() && !$this->popMode
            && $this->messageModel !== MessageModel::BROADCASTING
            && $now >= $this->nextLockAt) {
            $this->nextLockAt = $now + 20.0;
            $this->lockQueuesOnce();
        }

        // ⑤ 拉取 / POP 轮。
        if ($this->popMode) {
            $this->popRound($now);
        } else {
            $this->pullRound($now);
            // ⑥ 分发消费（每轮拉取后紧接分发；对齐 Python 分发线程）。
            $this->dispatchRound();
            // ⑦ 过期消息清扫（只在经典并发路径；Java 只为它建调度）。
            if (!$this->isOrderly() && $now >= $this->nextCleanAt) {
                $this->nextCleanAt = $now + max(1, $this->consumeTimeout) * 60.0;
                try {
                    $this->cleanExpiredMsgOnce();
                } catch (\Throwable $e) {
                    Logger::error('scheduleAtFixedRate cleanExpireMsg exception: ' . $e->getMessage());
                }
            }
        }

        // ⑧ 位点落盘。
        if ($now >= $this->nextPersistAt) {
            $this->nextPersistAt = $now + $this->persistConsumerOffsetInterval / 1000.0;
            try {
                $this->persistOffsetsOnce();
            } catch (\Throwable $e) {
                Logger::debug('persist offsets error: ' . $e->getMessage());
            }
        }
    }

    /** tick() 的别名。 */
    public function pumpOnce(?int $nowMillis = null): void
    {
        $this->tick($nowMillis);
    }

    // ---------------- 重平衡 ----------------

    /**
     * 按当前分配的队列同步本地状态，并清理被撤销/已停摆队列的状态。
     *
     * 对齐 Java ``RebalanceImpl.updateProcessQueueTableInRebalance``：队列被撤走时必须
     * ①persist 该队列**已消费**位点 ②丢弃 ProcessQueue（在途消息不再消费）③顺序消费
     * 还要 UNLOCK_BATCH_MQ。刻意保持 Java 的**先撤后建**顺序，撤的收尾放在建之前。
     */
    private function rebalancePullState(): void
    {
        $current = [];
        foreach ($this->assigned as $key => $mq) {
            $current[$key] = $mq;
        }
        $retired = [];
        $pop = $this->popMode;
        // ①撤：被分走的 + 拉取停摆的（Java 的 !mqSet.contains / isPullExpired 两支）。
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
        // 收尾必须早于 ②建。
        if ($retired !== []) {
            $this->onQueuesRevoked($retired);
        }
        // ②建：为缺失的队列准备状态。
        foreach ($current as $key => $mq) {
            if (isset($this->mqMap[$key])) {
                continue;
            }
            if ($pop && !isset($this->popQueues[$key])) {
                $this->popQueues[$key] = new PopProcessQueue();
            }
            // 新 ProcessQueue 就位 ⇒ 解冻（重建后的队列按修正位点重新开始推进）。
            unset($this->frozenOffsets[$key]);
            // 线程刚建、还没盖章，先用当前时刻占位，避免下一趟误判停摆。
            $this->lastPullTable[$key] = microtime(true);
            // 队列一旦分配就进 _mq_map：位点持久化、307 运行信息、220 重置都靠它。
            $this->mqMap[$key] = $mq;
        }
    }

    /**
     * 丢弃一个队列的全部本地状态，位点交给调用方在撤收尾里持久化。
     *
     * 代号 +1 ⇒ 在途批次的 ack 全部失效（Java setDropped(true)）；冻结标记**保留**到
     * 队列重建为止，避免纠错后的位点被旧 ack 覆盖。
     *
     * @param list<array{0:MessageQueue,1:int|null}> $retired
     */
    private function retireQueue(string $key, ?MessageQueue $fallbackMq, array &$retired): void
    {
        unset($this->pending[$key], $this->idleUntil[$key], $this->suspendedUntil[$key]);
        // 在途登记一并作废。
        unset($this->inflightMsgs[$key], $this->msgQueueInflight[$key]);
        unset($this->lockOk[$key]);
        // 代号 +1 ⇒ 在途批次的 ack 全部失效；冻结标记保留到队列重建为止。
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

    /**
     * 该队列的拉取/弹出循环是否已停摆（Java ProcessQueue.isPullExpired）。
     * PHP 单线程下没有"线程死了"分支，只有超时判据。
     */
    private function pullStalled(string $key): bool
    {
        $began = $this->lastPullTable[$key] ?? null;
        if ($began === null) {
            return false;
        }
        return (microtime(true) - $began) > ConsumerDefaults::PULL_MAX_IDLE_TIME;
    }

    /**
     * 被撤销队列的收尾（对应 Java RebalanceImpl.removeUnnecessaryMessageQueue）。
     *
     * @param list<array{0:MessageQueue,1:int|null}> $revoked
     */
    private function onQueuesRevoked(array $revoked): void
    {
        $broadcast = $this->messageModel === MessageModel::BROADCASTING;
        if ($broadcast) {
            // 广播模式位点只存本地。撤下来的位点必须连同队列信息写回文件。
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
            // 撤队列时的单队列解锁只属于 classic 顺序（POP 模式下不会跑）。
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

    /**
     * topic 的全部队列（对应 Java RebalanceImpl.topicSubscribeInfoTable）。
     * 取自**订阅信息**（读位 + readQueueNums、不筛 master），不是发布信息。
     *
     * @return list<MessageQueue>
     */
    private function allQueuesOfTopic(string $topic): array
    {
        $client = $this->requireClient();
        $queues = $client->getTopicSubscribeInfo($topic);
        if ($queues === []) {
            Logger::debug("rebalance: no subscribe info for topic $topic");
        }
        return $queues;
    }

    /**
     * 按 Java RebalanceImpl.rebalanceByTopic 计算分配，再同步本地状态。
     * 查不到消费者列表时**保留现有分配**（Java 仅告警；绝不回退成"独占全部队列"）。
     */
    public function doRebalance(): void
    {
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
        // 新分配的队列**立刻**解析初始位点（对齐 Java updateProcessQueueTableInRebalance）。
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
                $off = $this->resolveInitialOffset($client, $mq, $sub);
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

    /** 实例收到 broker 的 40 通知后触发即时重平衡（对应 Java rebalanceImmediately）。 */
    public function rebalanceImmediately(): void
    {
        $this->rebalanceNow = true;
    }

    // ---------------- 拉取（PHP：tick 内的短轮询轮） ----------------

    /** 一轮拉取：对每条已分配队列执行至多 maxPullsPerQueuePerTick 次短轮询。 */
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
     * 单队列一次拉取（对齐 Python _queue_pull_loop 的单轮）。
     * 返回 true = 拉到了消息，可以立刻再来一轮；false = 本 tick 该队列到此为止。
     */
    private function pullOnce(MessageQueue $mq, string $key, bool $orderly, float $now): bool
    {
        if (!$this->started) {
            return false;
        }
        $client = $this->requireClient();
        // Java DefaultMQPushConsumerImpl.pullMessage:253 —— 每次**发起**拉取就盖时刻。
        $this->lastPullTable[$key] = microtime(true);
        $sub = $this->subscriptionData[$mq->topic] ?? null;
        if ($sub === null) {
            return false;
        }
        // 顺序消费：broker 未确认锁定（LOCK_BATCH_MQ）的队列不拉取。
        if ($orderly && !isset($this->lockOk[$key])) {
            return false;
        }
        // 挂起重试未到点（PHP 适配：替代顺序消费的 sleep）。
        if (isset($this->suspendedUntil[$key]) && $now < $this->suspendedUntil[$key]) {
            return false;
        }
        // 流控（对齐 Java ProcessQueue.putMessage / checkReconsumeTimes 五阈值）。
        if ($this->flowControlHit($mq, $key)) {
            return false;
        }
        // 空闲退避（PHP 短轮询适配）。
        if (isset($this->idleUntil[$key]) && $now < $this->idleUntil[$key]) {
            return false;
        }
        $offset = $this->offsetTable[$key] ?? null;
        if ($offset === null) {
            try {
                $offset = $this->resolveInitialOffset($client, $mq, $sub);
            } catch (\Throwable $e) {
                Logger::debug("resolve initial offset failed for $mq: " . $e->getMessage());
                $this->idleUntil[$key] = microtime(true) + 1.0;
                return false;
            }
            $this->offsetTable[$key] = $offset;
        }
        try {
            // Java :458-468：仅当 postSubscriptionWhenPull 打开且非类过滤模式才带订阅表达式。
            $subExpression = ($this->postSubscriptionWhenPull && !$sub->isClassFilterMode())
                ? ($sub->subString ?? '*') : null;
            $sysFlag = PullSysFlag::buildSysFlag(
                commitOffset: false,
                suspend: false,          // PHP 短轮询（有意偏差，见类头注释）
                subscription: $subExpression !== null,
                classFilter: false,
            );
            // Java PullAPIWrapper#pullKernelImpl：按上轮应答的 suggestWhichBrokerId 选主/从。
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
            // Java PullAPIWrapper#processPullResult:77 —— 每轮应答都更新 pullFromWhichNodeTable。
            $this->pullFromWhichNode[$key] = $result->suggestWhichBrokerId ?? MixAll::MASTER_ID;
            // 消费统计：RT 每次都记，TPS 只在有消息时记。
            if ($this->statsManager !== null) {
                $this->statsManager->incPullRt($this->consumerGroup, $mq->topic,
                    (int) ((microtime(true) - $pullBegan) * 1000));
                if ($result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
                    $this->statsManager->incPullTps($this->consumerGroup, $mq->topic,
                        count($result->msgFoundList));
                }
            }
        } catch (RemotingTimeoutException $e) {
            // 短轮询下的网络超时按预期路径退避重试。
            Logger::debug("pull timeout for $mq (will retry): " . $e->getMessage());
            $this->idleUntil[$key] = microtime(true) + 0.5;
            return false;
        } catch (\Throwable $e) {
            // broker 侧非 SUCCESS 码（TOPIC_NOT_EXIST 等）：预期路径，debug + 退避。
            Logger::debug("pull error for $mq: " . get_class($e) . ': ' . $e->getMessage());
            $this->idleUntil[$key] = microtime(true) + 0.5;
            return false;
        }

        // 投递前的客户端侧过滤（对齐 Java processPullResult:113-128）。拉取路径被摘掉的
        // 消息不 ack：位点照常推进 = 静默跳过。
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

        // 单线程模型下撤队列不可能发生在同步拉取期间；位点推进照 Python 口径在
        // "仍持有队列"守卫之后执行。
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
                // Java :402-427 —— 位点被 broker 纠正时必须连纠错带重建一起做。
                $this->frozenOffsets[$key] = true;
                $this->consumeOffsets[$key] = (int) $result->nextBeginOffset;
                $illegal = true;
            } else {
                // Java :394-401 —— 空应答（NO_NEW_MSG / NO_MATCHED_MSG）修正"已消费位点"。
                $this->correctTagsOffset($key, $result->status, $result->nextBeginOffset);
            }
        }
        if ($illegal) {
            $this->offsetIllegalRecover($key);
            return false;
        }
        if ($result->status === PullStatus::FOUND && $result->msgFoundList !== []) {
            return true; // 拉到了，立刻再来一轮
        }
        // 空结果：空闲退避（短轮询下 broker 立即返回，没有挂起预算）。
        $this->idleUntil[$key] = microtime(true) + max(0, $this->pullIntervalMillis) / 1000.0;
        return false;
    }

    /** 对应 Java ``ProcessQueue.putMessage`` 里的 ``msgAccCnt`` 计算（取本批最后一条）。 */
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

    /** 读 ``msgAccCnt``：给 key 读单队列，不给则求和（等价 computeAccumulationTotal）。 */
    public function msgAccCnt(?string $key = null): int
    {
        if ($key === null) {
            return array_sum($this->msgAccCntTable);
        }
        return $this->msgAccCntTable[$key] ?? 0;
    }

    /** Java ``DefaultMQPushConsumerImpl.computeAccumulationTotal``。 */
    public function computeAccumulationTotal(): int
    {
        return array_sum($this->msgAccCntTable);
    }

    /**
     * Java ``DefaultMQPushConsumerImpl.adjustThreadPool`` —— ⚠ 在 Java 5.5.1 这是 no-op，
     * 我们照抄 no-op（真正生效的是显式的 updateCorePoolSize；不要"修好"它）。
     */
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

    /** 运行时调整消费并发度（对应 Java updateCorePoolSize → setCorePoolSize）。 */
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

    /** 是否触发流控（对齐 Java ProcessQueue.putMessage 的五个阈值检查）。 */
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

    // ---------------- 初始位点 ----------------

    private function resolveInitialOffset(MQClientInstance $client, MessageQueue $mq, SubscriptionData $sub): int
    {
        if ($sub->expressionType === \RocketMQ\Common\ExpressionType::SQL92) {
            // SQL 过滤无 offset 语义，默认最新
            return $client->getMaxOffset($mq);
        }
        if ($this->messageModel === MessageModel::BROADCASTING) {
            // 广播模式：offset 只存本地（对齐 Java LocalFileOffsetStore）
            $stored = $this->loadLocalOffsets();
            $key = self::mqKey($mq);
            if (isset($stored[$key])) {
                return $stored[$key];
            }
            if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
                // Java RebalancePushImpl:197-208：FIRST_OFFSET 且本地没有位点时直接给 0。
                return 0;
            }
            return $client->getMaxOffset($mq);
        }
        // 集群模式：先查 broker 上已提交的位点（对齐 Java RemoteBrokerOffsetStore.readOffset）。
        try {
            $stored = $client->queryConsumerOffset($this->consumerGroup, $mq, 5000, null, false);
            if ($stored !== null && $stored >= 0) {
                return $stored;
            }
        } catch (\Throwable $e) {
            Logger::debug("query consumer offset for $mq not found: " . $e->getMessage());
        }
        if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
            // Java RebalancePushImpl:197-208：**不**发 minOffset 查询，越界由 broker 纠正。
            return 0;
        }
        if ($this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_TIMESTAMP) {
            return $client->searchOffsetByTimestamp($mq, $this->consumeTimestampMillis());
        }
        if (NamespaceUtil::isRetryTopic($mq->topic)) {
            // Java RebalancePushImpl:181-182：%RETRY% 主题首次消费且无位点时从 0 开始。
            return 0;
        }
        // 默认 CONSUME_FROM_LAST_OFFSET
        return $client->getMaxOffset($mq);
    }

    // ---------------- 分发消费（PHP：tick 内同步执行） ----------------

    /**
     * 一轮分发：把各队列 pending 里排好的批次交给 listener（同步执行）。
     * 对齐 Python _dispatch_loop 的循环体；批次数有界，保证 tick 可控。
     */
    private function dispatchRound(): void
    {
        $budget = 1000;
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
                // 与"取走批次"同一临界区登记在途（单线程下即顺序执行）。
                $this->msgQueueInflight[$key] = ($this->msgQueueInflight[$key] ?? 0) + 1;
                // 连同这批消息所属的 ProcessQueue 代号一起取走。
                $epoch = $this->queueEpoch[$key] ?? 0;
                try {
                    $this->consumeBatch($key, $mq, $batch, $epoch);
                } catch (\Throwable $e) {
                    // 分发路径意外异常：批次塞回队首，稍后重试（不要让它杀死 tick）。
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

    /** 对应 Java DefaultMQPushConsumerImpl.resetRetryAndNamespace（分发前调用）。 */
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

    // ---------------- broker 主动请求：220 / 221 / 307 / 309 ----------------

    /**
     * 对应 Java MQClientInstance.resetOffset（220 的处理逻辑）。
     * 新位点先落进表，再走统一撤销路径，撤销收尾把**新位点**持久化，最后 rebalance 重建。
     *
     * @param list<array{mq: MessageQueue, offset: int}> $offsetTable
     */
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
            // 新位点先写进表：撤销会把**它**交给 onQueuesRevoked 落盘。
            $this->consumeOffsets[$key] = $off;
            $this->retireQueue($key, $mq, $retired);
        }
        if ($retired === []) {
            return;
        }
        usleep(200000); // 0.2s（有意偏差，见 Python 注释：代号已让在途 ack 全部失效）
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

    /**
     * 对应 Java MQClientInstance.getConsumerStatus（221 的应答数据源）——
     * **已消费位点**表（不是拉取游标）。
     *
     * @return list<array{mq: MessageQueue, offset: int}>
     */
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

    /** 对应 Java DefaultMQPushConsumerImpl.consumerRunningInfo（307 的应答）。 */
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
        // POP 模式下弹出去 popQueues，classic 的 processQueue 是空的 —— 两把表互斥。
        $popKeys = $this->popMode ? array_fill_keys(array_keys($this->popQueues), true) : [];
        foreach ($this->mqMap as $key => $mq) {
            if (isset($popKeys[$key])) {
                continue;
            }
            $pqi = new ProcessQueueInfo();
            $pqi->commitOffset = (int) ($this->consumeOffsets[$key] ?? 0);
            $pqi->cachedMsgCount = count($this->pending[$key] ?? []);
            $pqi->droped = false;
            // Java ProcessQueue.fillOutRunningInfo:456 —— 运维看的就是这个时刻。
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

    /**
     * 对应 Java 的 consumeMessageDirectly（并发 :102-139 / 顺序 :103-161）。
     */
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
        // Java 顺序 :156 —— autoCommit 读的是 **listener 跑完之后**的上下文值。
        $result->autoCommit = $orderly ? $context->autoCommit : true;
        $result->spentTimeMills = UtilAll::currentTimeMillis() - $begin;
        return $result;
    }

    // ---------------- 批次消费 ----------------

    private function isOrderly(): bool
    {
        return $this->messageListener instanceof MessageListenerOrderly;
    }

    /**
     * 消费一个批次并处理回投/挂起。``epoch``：这批消息所属的 ProcessQueue 代号；
     * 不一致说明队列已被撤销/重建，**整批作废**（Java isDropped() 短路）。
     */
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
        // ---- 顺序消费（Java ConsumeMessageOrderlyService）----
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
                // Java:463-469 —— 异常只置 hasException，status 留 null ⇒ 挂起。
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
            // 归一化：null / 未知值一律按挂起（否则会被当成 SUCCESS 静默 ack）。
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
                // Java processConsumeResult:244-269
                if ($status === ConsumeOrderlyStatus::COMMIT || $status === ConsumeOrderlyStatus::ROLLBACK) {
                    // Java:246-250 —— autoCommit=true 时 COMMIT/ROLLBACK 是非法用法，警告后照 ack。
                    Logger::warning('the message queue consume result is illegal, '
                        . "we think you want to ack these message $mq");
                    $status = ConsumeOrderlyStatus::SUCCESS;
                }
                if ($status === ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
                    $this->recordConsumeStats($mq->topic, count($batch), $obeginMs, failed: true);
                    // Java:256-266 —— 挂起之前先过 checkReconsumeTimes。
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
            // ---- autoCommit=False（Java:270-300，binlog 消费场景）----
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
                // Java:288-296 —— 毒消息交给 broker 后**不 commit**。
                return false;
            }
            // SUCCESS + autoCommit=False：Java 把消息留在 ProcessQueue 里等显式 commit()；
            // 本端口没有暴露 commit 口子，等价地塞回队首并等一个挂起周期（见 Python 注释）。
            $this->recordConsumeStats($mq->topic, count($batch), $obeginMs, failed: false);
            $this->requeuePending($key, $batch);
            $this->suspendedUntil[$key] = microtime(true) + $this->orderlySuspendMillis($ocontext) / 1000.0;
            return false;
        }
        // ---- 并发消费（Java ConsumeMessageConcurrentlyService$ConsumeRequest.run）----
        // 登记在途消息：cleanExpiredMsg 的"队首"清扫与 containsMessage 判据都看它。
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
            // Java:366-370 —— 交给 listener **之前**逐条盖 CONSUME_START_TIME。
            foreach ($batch as $m) {
                MessageAccessor::setConsumeStartTimestamp($m, UtilAll::currentTimeMillis());
            }
            $status = $listener->consumeMessage($batch, $context);
        } catch (\Throwable $e) {
            // Java：消费抛异常按 RECONSUME_LATER 处理
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
        // Java processConsumeResult:207-229 —— ackIndex 划分「已认可前缀 / 待回投后缀」。
        $ackIndex = $context->ackIndex;
        if ($status === ConsumeConcurrentlyStatus::CONSUME_SUCCESS) {
            if ($ackIndex >= count($batch)) {
                $ackIndex = count($batch) - 1;
            }
        } else {
            $ackIndex = -1;
        }
        // 统计口径同 Java 的 ok/failed 计数：部分 ack 时尾巴算 failed。
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
            // Java:232-237 —— 广播模式不回投：未认可的尾巴打 warn 丢弃，整批位点照样前进。
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
            // 整批认可（默认路径）。
            $this->advanceConsumeOffset($key, $batch, null, $epoch);
            return true;
        }
        // 集群模式：未认可的 [ack_index+1, size) 逐条回投 %RETRY%topic。
        $msgBackFailed = $this->sendBackBatch($key, array_slice($batch, $ackIndex + 1), $context);
        // Java:256-260 —— 回投失败的那几条塞回队首稍后再消费。
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
        // Java:266-269 —— 提交「本批已处理条目里最大的 queueOffset + 1」，且不能越过
        // 仍留在 ProcessQueue 里的那几条（floor = 回投失败条目的最小 queueOffset）。
        $floor = null;
        if ($msgBackFailed !== []) {
            $floor = min(array_map(static fn(MessageExt $m) => $m->queueOffset, $msgBackFailed));
        }
        $this->advanceConsumeOffset($key, $acked, $floor, $epoch);
        return $msgBackFailed === [];
    }

    /** 把这一批塞回本地队列队首，等价 Java makeMessageToConsumeAgain/rollback。 */
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

    /** 把这批消息从"在 listener 手里"的登记里摘除（按对象身份，幂等）。 */
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

    /** Java ``ProcessQueue.msgTreeMap`` 的等价视图：在途 ∪ 已拉未分发（按身份去重）。 */
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

    /** 队首 = 最小 ``queueOffset`` 的那条（Java ``msgTreeMap.firstEntry()``）。 */
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

    /** Java ``ProcessQueue.containsMessage``：这条消息还挂在队列上吗。 */
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

    /** 摘除刚清扫过的那条（调用方已确认它就是队首）。 */
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

    // ---------------- cleanExpiredMsg：挂起 listener 的逃生口 ----------------

    /**
     * 对应 Java ``cleanExpireMsg()``（:192-200）：遍历当前持有的队列逐个清扫。
     */
    public function cleanExpiredMsgOnce(): void
    {
        foreach (array_keys($this->mqMap) as $key) {
            $this->cleanExpiredQueue($key);
        }
    }

    /**
     * 对应 Java ``ProcessQueue.cleanExpiredMsg``（:80-127），逐句转写。
     * 语义三件套：只看队首（最小位点）、单条判定严格大于 consumeTimeout、每轮最多 16 条。
     */
    private function cleanExpiredQueue(string $key): void
    {
        if ($this->isOrderly()) {
            // Java:76-78 —— 顺序消费没有这条路径（消息本来就要原地重试，回投会乱序）。
            return;
        }
        $loop = min(count($this->processQueueEntries($key)), 16);
        for ($i = 0; $i < $loop; $i++) {
            $msg = null;
            $first = $this->processQueueFirstEntry($key);
            if ($first !== null) {
                $stamp = MessageAccessor::getConsumeStartTimestamp($first);
                // Java:87-90 —— 没盖过章的直接当成未过期；过期判据是**严格**大于。
                if ($stamp !== null
                    && (UtilAll::currentTimeMillis() - (float) $stamp) > $this->consumeTimeout * 60 * 1000) {
                    $msg = $first;
                }
            }
            if ($msg === null) {
                break; // Java:97-99 —— 队首没过期，后面的更不可能过期
            }
            try {
                $this->sendMessageBack($msg, 3); // Java:103 —— delayLevel 固定 3
            } catch (\Throwable $e) {
                // Java:122-125 —— 回投失败只记日志：消息留在原地，绝不摘除（摘了就真丢了）。
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
            // Java:106-115 —— 只有它**仍是**队首时才摘除。
            if ($this->processQueueFirstEntry($key) === $msg) {
                $this->removeProcessQueueEntry($key, $msg);
            }
        }
    }

    // ---------------- 并发回投 ----------------

    /**
     * 把未认可的条目逐条回投 broker，返回回投失败的那些。
     * 对齐 Java ``ConsumeMessageConcurrentlyService#processConsumeResult:238-254``。
     */
    private function sendBackBatch(string $key, array $batch, ConsumeConcurrentlyContext $context): array
    {
        $failed = [];
        foreach ($batch as $msg) {
            $present = $this->processQueueContains($key, $msg);
            if (!$present) {
                // Java:243-248 —— 已被 cleanExpiredMsg 清扫（或队列已撤销）的条目跳过回投。
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
                // 重投次数在 MessageExt 线上格式第 13 字段，broker 重投时会 +1。
                $delayLevel = $context->delayLevelWhenNextConsume;
                if (!$delayLevel) {
                    // Java：delayLevelWhenNextConsume == 0 → 3 + reconsumeTimes
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

    /** 消息重投（对应 Java sendMessageBack，CONSUMER_SEND_MSG_BACK(36)）。 */
    public function sendMessageBack(MessageExt $msg, int $delayLevel, ?string $brokerName = null): void
    {
        $client = $this->requireClient();
        $brokerName = $brokerName ?? (string) $msg->brokerName;
        // Java :768 用的是 findBrokerAddressInPublish（只认 master）；
        // 从节点不接 CONSUMER_SEND_MSG_BACK。
        $addr = $client->findBrokerAddressInPublish($brokerName);
        if ($addr === null || $addr === '') {
            throw new MQClientException("Broker[$brokerName] master node does not exist");
        }
        // Java：maxReconsumeTimes == -1 时按 16 传给 broker（超限由 broker 转 %DLQ%）。
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
        $response = $client->remotingClient->invokeSync($addr, $request, 5000);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }
    }

    // ---------------- 顺序消费的重试计数与回投 ----------------

    /**
     * Java ``ConsumeMessageOrderlyService#submitConsumeRequestLater:211-234``：
     * 解析 -1 → 回落到消费者配置，再钳到 [10, 30000]。
     */
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

    /**
     * Java ConsumeMessageOrderlyService#getMaxReconsumeTimes:313-320。
     * 顺序侧 `-1` 是「不设限」（Integer.MAX_VALUE）；并发侧 `-1 → 16` 是另一套语义。
     */
    private function orderlyMaxReconsumeTimes(): int
    {
        if ($this->maxReconsumeTimes === -1) {
            return ConsumerDefaults::JAVA_INT_MAX;
        }
        return $this->maxReconsumeTimes;
    }

    /** Java ConsumeMessageOrderlyService#checkReconsumeTimes:322-336。返回是否还要原地挂起。 */
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

    /**
     * Java ConsumeMessageOrderlyService#sendMessageBack:338-360。
     * 拿实例自带的内部生产者，把这条消息**当普通消息**发到 `%RETRY%<group>`。
     * 失败只返回 false、绝不抛。
     */
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
            // 半消息标记必须清掉，否则 broker 会把它再当事务回查消息处理
            MessageAccessor::clearProperty($newMsg, MessageConst::PROPERTY_TRANSACTION_PREPARED);
            $newMsg->setDelayTimeLevel(3 + $msg->getReconsumeTimes());
            $publish = $client->getTopicPublishInfo($newMsg->topic, true);
            $mq = $publish !== null ? $publish->selectOneMessageQueue() : null;
            if ($mq === null) {
                throw new MQClientException("no writable queue for retry topic $newMsg->topic");
            }
            // unitMode 跟着消费者：不带的话重投出去的消息会丢单元标记。
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

    // ---------------- 位点推进 / 修正 / 纠错 ----------------

    /**
     * 推进"已消费位点"（Java ConsumeMessageConcurrentlyService:266 的 updateOffset）。
     *
     * ``epoch`` 与当前代号不一致说明队列已被撤销/重建，**整批 ack 作废**；
     * ``frozenOffsets`` 里的是被 OFFSET_ILLEGAL 纠错冻结的位点，同样不许改。
     * 判据与写入必须在同一"临界区"（单线程下即同一段顺序代码）。
     */
    private function advanceConsumeOffset(string $key, array $batch, ?int $floor = null, ?int $epoch = null): void
    {
        if ($batch === []) {
            // 整批回投都失败时没有任何条目被认可，位点原地不动
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

    /**
     * Java ``DefaultMQPushConsumerImpl#correctTagsOffset``（:713-717）。
     *
     * 拉取应答是 ``NO_NEW_MSG``（队列里真没消息）或 ``NO_MATCHED_MSG`` 时，"已消费位点"
     * 必须跟着拉取游标走，否则会**永久卡死**。闸门：``0L == processQueue.getMsgCount()``
     * —— ``pending`` 为空**且**在途批次为 0 才允许推进。
     */
    private function correctTagsOffset(string $key, PullStatus $status, ?int $nextOff): void
    {
        if ($status !== PullStatus::NO_NEW_MSG && $status !== PullStatus::NO_MATCHED_MSG) {
            return;
        }
        if ($nextOff === null) {
            return;
        }
        if (isset($this->frozenOffsets[$key])) {
            // 位点已被 OFFSET_ILLEGAL 纠错冻结，这条路径同样不许改
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

    /**
     * Java ``DefaultMQPushConsumerImpl`` 的 OFFSET_ILLEGAL 分支（:402-427）。
     * 置位点 + 冻结 → 撤队列（代号 +1，等价 setDropped）→ 锁外把修正位点推给 broker
     * （等价 persist）→ 触发 rebalance 重建。
     */
    private function offsetIllegalRecover(string $key): void
    {
        $retired = [];
        $this->retireQueue($key, null, $retired);
        // persist：把修正后的位点立刻写回 broker（Java 显式的一次 persist）。
        $this->onQueuesRevoked($retired);
        $this->rebalanceImmediately();
        Logger::warning("the pull request offset illegal, fix it, queue=$key");
    }

    // ---------------- 位点持久化 ----------------

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

    /** Java LocalFileOffsetStore：$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json */
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

    /**
     * 把本地位点表写盘（Java ``LocalFileOffsetStore.persist``）。
     *
     * @param list<array{0:MessageQueue,1:int|null}>|null $extra 刚被撤下来、已不在表里的队列位点
     */
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
        // Java MixAll.string2File：旧内容先滚到 .bak 再写新文件
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

    /** Java readLocalOffset：主文件缺失/为空/解析失败 → .bak。 */
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

    // ---------------- 顺序消费队列锁 ----------------

    /** 每 20s 批量锁分到的队列（Java ConsumeMessageOrderlyService.lockMQ；tick 内到点调用）。 */
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

    // ---------------- 管理能力 ----------------

    /** @return list<MessageQueue> */
    public function fetchSubscribeMessageQueues(string $topic): array
    {
        $client = $this->requireClient();
        // Java #fetchSubscribeMessageQueues:198 读订阅信息（读位、不筛 master），不是发布信息。
        return $client->getTopicSubscribeInfo($topic);
    }

    // ---------------- 消费钩子 ----------------

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

    /**
     * 对应 Java 的 returnType 判定（决定轨迹的 contextCode）。
     * 顺序消费把「挂起」等价于 FAILED、成功等价于 SUCCESS。
     */
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

    /** 消费耗时记数（Java ConsumeRequest.run 里那条**无条件**的 incConsumeRT）。 */
    private function recordConsumeRt(string $topic, int $beginMs): void
    {
        if ($this->statsManager === null) {
            return;
        }
        $this->statsManager->incConsumeRt($this->consumerGroup, $topic, UtilAll::currentTimeMillis() - $beginMs);
    }

    /**
     * 消费侧 TPS 记数（Java ``processConsumeResult`` 的 ok/failed 分支）。
     * ``ackCount`` 对应 Java :217-220 的 ``ok = ackIndex + 1``。
     */
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

    /**
     * 把 returnType/status/success 写回上下文并触发 after 钩子（对齐 Java）。
     * ``status`` 是决定 returnType 的那一个（归一化**前**）；``hookStatus`` 是写进上下文的
     * 那一个（归一化**后**）—— Java 里这两步用的**不是同一个值**。
     */
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

    // ---------------------------------------------------------------- POP 消费循环（PHP：tick 内短轮询）

    /**
     * 一轮 POP：对每条已分配队列执行至多 maxPopsPerQueuePerTick 次弹取（pollTime=0）。
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
     * 单队列一次 POP（对齐 Python _queue_pop_loop 的单轮）。
     *
     * 与 pull 的关键差别：**不查、不提交消费位点**（进度由 broker 侧 checkpoint 跟踪，
     * 确认只靠 ack）；弹出即投递消费（内联执行）；``POLLING_NOT_FOUND`` 是正常态。
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
            // Java 的钳制：超出 [5s, 300s] 一律回落到 60s
            $invisible = 60000;
        }
        // Java PopRequest 默认 ConsumeInitMode.MAX；按 consume_from_where 映射。
        $initMode = $this->consumeFromWhere === ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET
            ? ConsumerDefaults::CONSUME_INIT_MODE_MIN
            : ConsumerDefaults::CONSUME_INIT_MODE_MAX;
        $pq = $this->popQueues[$key] ?? null;
        if ($pq === null || $pq->isDropped()) {
            return false;
        }
        // 发起弹出就盖时刻：流控/空闲退避都不该让一条还在跑的循环被判成停摆。
        $now = microtime(true);
        $this->lastPullTable[$key] = $now;
        $pq->lastPopTimestamp = $now;
        $sub = $this->subscriptionData[$mq->topic] ?? null;
        if ($sub === null) {
            return false;
        }
        // 流控：已弹未 ack 太多就先缓一缓（Java popThresholdForQueue）。
        if ($pq->waitAckCount() > $this->popThresholdForQueue) {
            return false;
        }
        // 空闲退避（PHP 短轮询适配）。
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
                pollTime: 0,             // PHP 短轮询（有意偏差，见类头注释）
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
        // 弹出后队列被 rebalance 撤走：这一批**既不消费也不 ack**（单线程下极难发生，
        // 守卫照抄）。
        if ($pq->isDropped()) {
            return false;
        }
        $found = $result->status === PopStatus::FOUND;
        if ($found) {
            // Java PopCallback.onSuccess:556-563：RT 在 FOUND 分支入口就记，TPS 按弹到的条数记。
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
            // 投递前过滤（对齐 Java processPopResult:621-661）：POP 路径**必须 ack 被摘掉的**，
            // 否则 invisibleTime 到期后 broker 会复活重投 —— 表现为"过滤没生效"。
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
        // 空结果：短轮询立即返回会变成热循环，按空闲退避兜底。
        $this->idleUntil[$key] = microtime(true) + max(0, $this->popIntervalMillis) / 1000.0;
        return false;
    }

    /**
     * Java DefaultMQPushConsumerImpl:960-990 按 listener 类型选服务：顺序监听器 +
     * POP 走 ConsumeMessagePopOrderlyService —— 上游 5.5.0 那是个未完成骨架（POPTODO）：
     * 消息**不消费、不 ack**，invisibleTime 到期由 broker 复活重投。照抄这个行为，别"修好"。
     */
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
            // PHP 单线程：消费批次内联执行（Java 的 consumeExecutor 线程池无对应物）。
            $this->consumePopBatch($batch, $pq, $mq);
        }
    }

    /**
     * 对应 Java ConsumeMessagePopOrderlyService.submitConsumeRequest:178-191：锁下去重。
     */
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

    /**
     * 对应 Java ConsumeMessagePopOrderlyService$ConsumeRequest.run:315-324 —— 上游
     * 5.5.0 的骨架到此为止：pq 被撤销才摘请求；否则**什么都不做**。
     */
    private function runPopOrderlyRequest(PopProcessQueue $pq, MessageQueue $mq): void
    {
        if ($pq->isDropped()) {
            Logger::warning("run, message queue not be able to consume, because it's dropped. $mq");
            unset($this->popOrderlyRequests[self::mqKey($mq)]);
            return;
        }
    }

    /** 请求集条数（Java consumeRequestSet.size()；去重行为要能离线锁死）。 */
    public function popOrderlyRequestCount(): int
    {
        return count($this->popOrderlyRequests);
    }

    /** 消费一个 POP 批次并按结果 ack / 延长不可见时间（Java ConsumeMessagePopConcurrentlyService$ConsumeRequest.run）。 */
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
            // 已经超过 invisibleTime：ack 也不会被承认，直接放弃本批（等 broker 复活重投）
            Logger::debug(sprintf('pop timeout, abort consume for %s: popTime=%s invisible=%s',
                (string) $mq, $popTime, $invisible));
            $pq->decFoundMsg(-count($msgs));
            return;
        }

        $this->resetRetryTopicAndNamespace($msgs);
        $context = new ConsumeConcurrentlyContext($mq);
        // 对齐 Java ackIndex = Integer.MAX_VALUE 的默认值，显式钳成 size-1：CONSUME_SUCCESS
        // 默认全部 ack。若写成 -1，一条都不会 ack —— 短观测窗口下会伪装成通过。
        $context->ackIndex = count($msgs) - 1;
        $hookCtx = null;
        if ($this->consumeMessageHookList !== []) {
            $hookCtx = $this->buildConsumeHookContext($msgs, $mq);
            $this->executeConsumeHookBefore($hookCtx);
        }
        $beginMs = UtilAll::currentTimeMillis();
        $hasException = false;
        try {
            // Java :379-385 —— 交给 listener 前逐条盖 CONSUME_START_TIME。
            foreach ($msgs as $m) {
                MessageAccessor::setConsumeStartTimestamp($m, UtilAll::currentTimeMillis());
            }
            $status = $this->messageListener->consumeMessage($msgs, $context);
        } catch (\Throwable $e) {
            // Java：消费抛异常按 RECONSUME_LATER 处理
            Logger::debug('pop listener error, treat as RECONSUME_LATER: ' . $e->getMessage());
            $status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
            $hasException = true;
        }
        // Java POP :396-405 —— listener 返回 null 同样按 RECONSUME_LATER 处理。
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
            // 消费期间队列被撤走或已超时：结果不再处理
            $pq->decFoundMsg(-count($msgs));
            return;
        }
        $this->processPopConsumeResult($status, $context, $msgs, $pq, $mq);
    }

    /** Java ConsumeRequest.isPopTimeout：不能解析出 popTime/invisibleTime 时按超时处理。 */
    private static function isPopTimeout(array $msgs, int $popTime, int $invisible): bool
    {
        if ($msgs === [] || $popTime <= 0 || $invisible <= 0) {
            return true;
        }
        return UtilAll::currentTimeMillis() - $popTime >= $invisible;
    }

    /** 对应 Java ConsumeMessagePopConcurrentlyService.processConsumeResult。 */
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
            // RECONSUME_LATER：一条都不 ack
            $ackIndex = -1;
        }

        for ($i = 0; $i <= $ackIndex; $i++) {
            $this->ackPopMsg($msgs[$i]);
            $pq->ack();
        }

        for ($i = $ackIndex + 1; $i < count($msgs); $i++) {
            $pq->ack();
            $msg = $msgs[$i];
            // 超过最大重试次数：Java 走 checkNeedAckOrDelay。
            if ($this->maxReconsumeTimes >= 0 && $msg->getReconsumeTimes() >= $this->maxReconsumeTimes) {
                $this->checkNeedAckOrDelay($msg);
                continue;
            }
            $this->changePopInvisibleTime($msg, $context->delayLevelWhenNextConsume);
        }
    }

    /**
     * Java checkNeedAckOrDelay：重试次数用尽后的兜底。
     * 存活时间已超过最大延迟档位的 2 倍 → 直接 ack 丢弃；否则按存活时间选档位续命。
     */
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
     * 从 POP_CK 解出 ack/延长不可见时间需要的 (topic, brokerName, queueId, offset, ck)。
     *
     * ⚠ 两处都不能想当然：
     *  1. topic 要用 ``ExtraInfoUtil::getRealTopic`` 按 CK 的 retryFlag 还原；
     *  2. 地址要按 CK 里的 brokerName 反查，不能按 topic 查路由（retry topic 通常没有
     *     独立路由表项）。
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

    /** 单条 ack（对应 Java DefaultMQPushConsumerImpl.ackAsync）。 */
    private function ackPopMsg(MessageExt $msg): void
    {
        $target = $this->popCkTarget($msg);
        if ($target === null) {
            return;
        }
        [$topic, $brokerName, $queueId, $offset, $ck] = $target;
        try {
            $client = $this->requireClient();
            // Java ackAsync 走 findBrokerAddressInSubscribe(brokerName, MASTER_ID, true)：**只要主**。
            $client->ackMessage(
                $this->consumerGroup,
                $topic,
                $queueId,
                $ck,
                $offset,
                brokerName: $brokerName,
                addr: $client->publishAddrFor($brokerName, $topic),
            );
        } catch (\Throwable $e) {
            // ack 失败不致命：消息会在 invisibleTime 到期后被 broker 复活重投
            Logger::debug('ack failed for ' . $msg->msgId . ': ' . $e->getMessage());
        }
    }

    /**
     * 延长不可见时间（对应 Java changePopInvisibleTime）。
     * ``delayLevel == 0`` 时 Java 用消息已重试次数当档位；档位表是**秒**，接口要毫秒。
     */
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
            // Java changePopInvisibleTimeAsync:869-876 同样是「只要主」+ 刷一次路由
            $client->changeInvisibleTime(
                $this->consumerGroup,
                $topic,
                $queueId,
                $ck,
                $offset,
                $delaySecond * 1000,
                brokerName: $brokerName,
                addr: $client->publishAddrFor($brokerName, $topic),
            );
        } catch (\Throwable $e) {
            Logger::debug('change invisible time failed for ' . $msg->msgId . ': ' . $e->getMessage());
        }
    }
}
