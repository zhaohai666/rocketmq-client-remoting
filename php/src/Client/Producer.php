<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\ClientErrorCode;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\Exceptions\RemotingConnectException;
use RocketMQ\Client\Exceptions\RemotingException;
use RocketMQ\Client\Exceptions\RemotingSendRequestException;
use RocketMQ\Client\Exceptions\RemotingTimeoutException;
use RocketMQ\Client\Exceptions\RemotingTooMuchRequestException;
use RocketMQ\Client\Exceptions\RequestTimeoutException;
use RocketMQ\Common\CompressionCodec;
use RocketMQ\Common\HandleV1;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageBatch;
use RocketMQ\Common\MessageClientIdSetter;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MessageSysFlag;
use RocketMQ\Common\MessageType;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\RecallMessageHandle;
use RocketMQ\Remoting\Protocol\CheckTransactionStateRequestHeader;
use RocketMQ\Remoting\Protocol\EndTransactionRequestHeader;
use RocketMQ\Remoting\Protocol\GetEarliestMsgStoretimeRequestHeader;
use RocketMQ\Remoting\Protocol\GetEarliestMsgStoretimeResponseHeader;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\NamespaceUtil;
use RocketMQ\Remoting\Protocol\ProducerData;
use RocketMQ\Remoting\Protocol\RecallMessageRequestHeader;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\RPCHook;

/**
 * 生产者（对应 org.apache.rocketmq.client.producer.* 及 impl.producer，
 * 1:1 移植自 python/client/producer.py）。
 *
 * 提供：DefaultMQProducer（同步/异步/单向/批量/选择器发送、事务消息）、
 * TransactionMQProducer、MessageQueueSelector 系列选择器、SendCallback 回调、
 * LocalTransactionState / TransactionListener 等。
 *
 * ## PHP 单线程适配（见 php/PORTING.md「异步模型」）
 *
 * Python 的 send_async 注册回调后**立刻返回**、由后台线程收响应；PHP 改为注册回调后
 * **在内部泵到完成**（``pumpAsyncSend``：驱动 AsyncSenderExecutor 队列、remoting 层
 * ``waitResponses``、回调执行器，直到回调链走完），对调用方呈现与 Python 相同的回调时序。
 * 所有后台线程（心跳循环、事务回查线程）一律改为调用方驱动的显式泵：
 *
 *   * 心跳：``heartbeatOnce()``（替代 Python 的 _heartbeat_loop 线程）；
 *   * 事务回查 CHECK_TRANSACTION_STATE(39)：处理器内联执行（Python 起线程）。
 *
 * 允许注入时钟（``monoClock`` / ``wallClock``）便于单测。
 */

// 定点发送守卫的两处 Java 文案（同步 DefaultMQProducerImpl:1235、异步 :1278），
// 逐字保留：两处文案不同是 Java 的原样，跨语言对齐时按入口取。
// （以 DefaultMQProducer::PINNED_TOPIC_MISMATCH_SYNC / _ASYNC 常量落地。）

/**
 * request() 专用的空发送回调（对齐 Java 里给 sendDefaultImpl 传的那个匿名 SendCallback，
 * 移植自 producer.py 的 _NullSendCallback）。
 *
 * 它只负责把「发送成功/失败」写回 RequestResponseFuture，应答本身由 broker 的
 * 326 推送投递，与这个回调无关。
 */
final class NullSendCallback implements SendCallback
{
    public function __construct(private readonly ?RequestResponseFuture $future = null)
    {
    }

    public function onSuccess(?SendResult $sendResult): void
    {
        if ($this->future !== null) {
            $this->future->sendRequestOk = true;
        }
    }

    public function onException(\Throwable $e): void
    {
        if ($this->future !== null) {
            // 与 Java 的 onException 完全一致：三件事都要做。少了 putResponseMessage(null)
            // 的话，等待方会一直阻塞到超时才抛错（明明是发送失败，却要等满 timeout）。
            $this->future->sendRequestOk = false;
            $this->future->putResponseMessage(null);
            $this->future->cause = $e;
        }
    }
}

/**
 * 异步链内部**唯一**被调用的回调（对应 Java
 * ``DefaultMQProducerImpl.BackpressureSendCallBack:577-633``，移植自 _BackPressureSendCallback）。
 *
 * 它是异步链内部**唯一**被调用的回调：链的终点 ``complete()`` 先跑 ``SendMessageHook.after``，
 * 再进这里的 ``onSuccess``/``onException`` —— 与 Java 的顺序一致（after 钩子在
 * ``sendKernelImpl`` 的包装回调里、信号量在外层，所以 after 先于归还许可）。归还之后才把
 * 结果交给用户回调。
 *
 * 只归还**本次真正拿到**的许可（``numAcquired``/``sizeAcquired``）：字节信号量超时而
 * 条数信号量已到手时，必须把那条还回去，否则关一次背压就把容量永久吃掉一格。
 */
final class BackPressureSendCallback implements SendCallback
{
    /** PHP 单线程泵需要知道回调链是否已走完（Python 无此字段）。 */
    public bool $done = false;
    public bool $numAcquired = false;
    public bool $sizeAcquired = false;

    public function __construct(
        private readonly object $delegate, // SendCallback|InternalSendCallback（鸭子类型同形）
        private readonly FairSemaphore $numSemaphore,
        private readonly FairSemaphore $sizeSemaphore,
        public readonly int $msgLen,
    ) {
    }

    private bool $released = false;

    public function onSuccess(?SendResult $sendResult): void
    {
        $this->done = true;
        $this->releasePermits();
        $this->delegate->onSuccess($sendResult);
    }

    public function onException(\Throwable $e): void
    {
        $this->done = true;
        $this->releasePermits();
        $this->delegate->onException($e);
    }

    /**
     * Java ``semaphoreProcessor:599-610``（先还字节、再还条数）。
     *
     * 与 Java 的一处加固：用 ``$released`` 保证只还一次（对应 Python 的 _released）。
     * Java 直接还，链上任何一条走到终点之后的重复回调都会把容量虚增出去；
     * 本端口的重试链更长，还一次更稳妥。
     */
    private function releasePermits(): void
    {
        if ($this->released) {
            return;
        }
        $this->released = true;
        if ($this->sizeAcquired) {
            $this->sizeSemaphore->release($this->msgLen);
        }
        if ($this->numAcquired) {
            $this->numSemaphore->release(1);
        }
    }
}

/**
 * 异步发送重试计数（对应 Python 的 ``times: List[int]``——PHP 数组是值类型，
 * 用一个小对象代替「跨重试共享的自增下标」）。
 */
final class AsyncSendAttempt
{
    public int $times = 0;
}

/**
 * 消息队列选择器（对应 Java MessageQueueSelector）。
 */
interface MessageQueueSelector
{
    /**
     * @param list<MessageQueue> $mqs
     */
    public function select(array $mqs, Message $msg, mixed $arg): MessageQueue;
}

/**
 * 按 arg 的 hash 选择队列（Java SelectMessageQueueByHash）。
 */
final class SelectMessageQueueByHash implements MessageQueueSelector
{
    public function select(array $mqs, Message $msg, mixed $arg): MessageQueue
    {
        if ($mqs === []) {
            throw new MQClientException('no message queue');
        }
        $value = $arg ?? 0;
        // Python 用跨进程随机化的 hash()；这里取确定性的 Java 风格 hashCode：
        // 只保证「同值同队列」与均匀分布语义，跨语言不逐位对齐。
        $idx = abs(self::valueHash($value)) % count($mqs);
        return $mqs[$idx];
    }

    /** Java ``String.hashCode``（s[0]*31^(n-1)+...，32 位有环绕）+ 其它类型的兜底。 */
    public static function valueHash(mixed $value): int
    {
        if (is_int($value)) {
            return $value;
        }
        if (is_string($value)) {
            $h = 0;
            $len = strlen($value);
            for ($i = 0; $i < $len; $i++) {
                $h = (31 * $h + ord($value[$i])) & 0xFFFFFFFF;
            }
            if ($h >= 0x80000000) {
                $h -= 0x100000000;
            }
            return $h;
        }
        return crc32(serialize($value));
    }
}

/**
 * 随机选择一个可用队列（Java SelectMessageQueueByRandom）。
 */
final class SelectMessageQueueByRandom implements MessageQueueSelector
{
    public function select(array $mqs, Message $msg, mixed $arg): MessageQueue
    {
        if ($mqs === []) {
            throw new MQClientException('no message queue');
        }
        return $mqs[random_int(0, count($mqs) - 1)];
    }
}

/**
 * 按机房（brokerName 前缀）选择队列（Java SelectMessageQueueByMachineRoom）。
 */
final class SelectMessageQueueByMachineRoom implements MessageQueueSelector
{
    public function select(array $mqs, Message $msg, mixed $arg): MessageQueue
    {
        if ($mqs === []) {
            throw new MQClientException('no message queue');
        }
        $room = (string) $arg;
        foreach ($mqs as $mq) {
            if (str_starts_with($mq->brokerName, $room)) {
                return $mq;
            }
        }
        // 无匹配则回退首个
        return $mqs[0];
    }
}

/**
 * 异步发送回调（对应 Java SendCallback）。
 */
interface SendCallback
{
    public function onSuccess(?SendResult $sendResult): void;

    public function onException(\Throwable $e): void;
}

/**
 * 本地事务状态（对应 Java LocalTransactionState，ordinal 与 Java 一致）。
 */
enum LocalTransactionState: int
{
    case COMMIT_MESSAGE = 0;
    case ROLLBACK_MESSAGE = 1;
    case UNKNOW = 2;
}

/**
 * 事务监听器（对应 Java TransactionListener）。
 */
interface TransactionListener
{
    public function executeLocalTransaction(Message $msg, mixed $arg): LocalTransactionState;

    public function checkLocalTransaction(MessageExt $msg): LocalTransactionState;
}

/**
 * 事务消息发送结果（对应 Java TransactionSendResult）。
 *
 * ⚠ 与 Python/Java 的**结构性差异**：既有的 ``SendResult`` 已被声明为 ``final``，
 * PHP 无法 ``extends``。这里用组合替代继承——构造时拷贝父结果字段（与 Python 的
 * ``__init__(send_result)`` 逐字段拷贝一致），并对全部读取口径做同形委托。
 * 若未来 SendResult 放开 final，可无感切回继承（本文件不改对外 API）。
 */
final class TransactionSendResult
{
    public function __construct(
        public readonly SendResult $sendResult,
        private ?LocalTransactionState $localTransactionState = null,
    ) {
    }

    public function getLocalTransactionState(): ?LocalTransactionState
    {
        return $this->localTransactionState;
    }

    public function setLocalTransactionState(?LocalTransactionState $state): void
    {
        $this->localTransactionState = $state;
    }

    /** 底层那条 SendResult（组合替代继承的取回入口）。 */
    public function getSendResult(): SendResult
    {
        return $this->sendResult;
    }

    // ---- 与 SendResult 同形委托 ----

    public function getSendStatus(): SendStatus
    {
        return $this->sendResult->getSendStatus();
    }

    public function getMsgId(): ?string
    {
        return $this->sendResult->getMsgId();
    }

    public function getMessageQueue(): ?MessageQueue
    {
        return $this->sendResult->getMessageQueue();
    }

    public function getQueueOffset(): int
    {
        return $this->sendResult->getQueueOffset();
    }

    public function getTransactionId(): ?string
    {
        return $this->sendResult->getTransactionId();
    }

    public function setTransactionId(?string $transactionId): void
    {
        $this->sendResult->setTransactionId($transactionId);
    }

    public function getOffsetMsgId(): ?string
    {
        return $this->sendResult->getOffsetMsgId();
    }

    public function getRegionId(): ?string
    {
        return $this->sendResult->getRegionId();
    }

    public function isTraceOn(): bool
    {
        return $this->sendResult->isTraceOn();
    }

    public function __toString(): string
    {
        return (string) $this->sendResult;
    }
}

/**
 * 默认生产者（对应 org.apache.rocketmq.client.producer.DefaultMQProducer）。
 */
class DefaultMQProducer
{
    /** 定点发送守卫的两处 Java 文案（同步 ``DefaultMQProducerImpl:1235``、异步 ``:1278``）。 */
    public const PINNED_TOPIC_MISMATCH_SYNC = "message's topic not equal mq's topic";
    public const PINNED_TOPIC_MISMATCH_ASYNC = 'Topic of the message does not match its target message queue';

    /** Java ``canBatch`` 里四个延时属性（缺省 0；名字逐字对齐 MessageConst）。 */
    private const TIMER_DELAY_PROPERTIES = ['DELAY', 'TIMER_DELAY_MS', 'TIMER_DELAY_SEC', 'TIMER_DELIVER_MS'];

    public string $producerGroup;
    /** TLS（Java 全局系统属性 tls.enable 的等价物；null = 交给 env ROCKETMQ_TLS_ENABLE） */
    public ?bool $tlsEnable;
    /** TLS 细项（caCert/clientCert/clientKey/serverName），语义见 RemotingClient::$tlsOptions。 */
    public ?array $tlsOptions = null;
    /** W3C traceparent 透传（opt-in） */
    public bool $enableTraceContext;
    public string $namespace = '';
    public string $instanceName = MixAll::DEFAULT_INSTANCE_NAME;
    public ?string $clientId = null;

    // ---- unit / stream 配置（对应 Java ClientConfig 的同名字段）----
    // unitName 会拼进 clientId 与动态取址 URL；unitMode 会透传给 broker
    // （发消息头、心跳 ConsumerData、拦截钩子上下文）；enableStreamRequestType
    // 既给 clientId 加 @STREAM 后缀（Java 注释：防止 MQClientInstance 被意外复用），
    // 也给每个请求加 ReqT=0 扩展字段。Java 的 producer 默认三个都是「关/空」。
    public ?string $unitName = null;
    public bool $unitMode = false;
    public bool $enableStreamRequestType = false;

    public string $createTopicKey = MixAll::DEFAULT_TOPIC;
    public int $defaultTopicQueueNums = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    public int $sendMsgTimeout = 3000;
    // 压缩配置，默认值与 Java DefaultMQProducer 一致
    public int $compressMsgBodyOverHowmuch = 1024 * 4;
    public int $compressLevel = 5;
    public int $compressType = MessageSysFlag::ZLIB_TYPE;
    public int $retryTimesWhenSendFailed = 2;
    public int $retryTimesWhenSendAsyncFailed = 2;
    public bool $retryAnotherBrokerWhenNotStoreOk = false;
    // Java DefaultMQProducer 默认 -1 = 不限制单次请求超时；开了之后每次重试的单请求
    // 超时被压到该值，慢 broker 才会被换掉而不是把总预算吃光。
    public int $sendMsgMaxTimeoutPerRequest = -1;
    /**
     * Java retryResponseCodes：broker 明确回了这些码才值得换一台重试，
     * 其余码（如 MESSAGE_ILLEGAL）重试也是白试，必须原样抛出。
     * @var array<int, true>
     */
    public array $retryResponseCodes;
    public int $maxMessageSize = 1024 * 1024 * 4;
    public ?RPCHook $rpcHook;
    /** @var list<string> */
    public array $topics;
    /** @var list<string> */
    public array $nameServerAddrs = [];
    /** @var MQClientInstance|null */
    public $mqClient = null;
    public bool $started = false;
    /** 当前事务监听器（sendMessageInTransaction 时记录，供 broker 回查调用） */
    private ?TransactionListener $transactionListener = null;
    /**
     * PHP 无线程：Python 的心跳线程改由调用方显式泵（``heartbeatOnce()``）。
     * 字段保留只为与 Python 状态对齐（shutdown 后置 false）。
     */
    public bool $heartbeatRunning = false;
    public int $heartbeatIntervalMillis = 30000;
    // 路由刷新周期（对应 Java ClientConfig.pollNameServerInterval 默认 30000ms）。
    // 只在 start() 建 MQClientInstance 时透传一次，之后修改不生效（Java 的
    // scheduledExecutorService 也是按启动时的周期排定）。
    public int $pollNameServerInterval = 30000;
    // 发送延迟故障容错：默认关闭，与 Java sendLatencyFaultEnable 一致
    public bool $sendLatencyFaultEnable = false;
    public MQFaultStrategy $mqFaultStrategy;

    // ---- 自动攒批（对应 Java DefaultMQProducer 的 autoBatch / batchMaxDelayMs /
    // batchMaxBytes / totalBatchMaxBytes）----
    // 四个配置在 Java 里都是"先记在 producer 上、start() 建累加器时再同步下去"，
    // 因为累加器是按 clientId 复用的（同 clientId 的第二个 producer 会**复用**第一个
    // 的累加器，此时 setter 立即生效、不再重新初始化）。-1 表示"不动累加器的默认值"。
    public bool $autoBatch = false;
    public int $batchMaxDelayMs = -1;
    public int $batchMaxBytes = -1;
    public int $totalBatchMaxBytes = -1;
    public ?ProduceAccumulator $produceAccumulator = null;

    // ---- 消息轨迹（对应 Java ClientConfig.enableTrace / traceTopic / traceMsgBatchNum）----
    public bool $enableTrace = false;
    public ?string $traceTopic = null; // null → 用 RMQ_SYS_TRACE_TOPIC
    public int $traceMsgBatchNum = 10;
    /** @var list<SendMessageHook> */
    public array $sendMessageHookList = [];
    /** @var list<EndTransactionHook> */
    public array $endTransactionHookList = [];
    /** 发送前拦截钩子（Java DefaultMQProducerImpl.checkForbiddenHookList） */
    /** @var list<CheckForbiddenHook> */
    public array $checkForbiddenHookList = [];
    public ?AsyncTraceDispatcher $traceDispatcher = null;
    /** 基础客户端指标（send/consume RT 与计数） */
    public ClientMetrics $metrics;

    /** Request-Reply 的默认超时（不传 timeout 给 request() 时用它）。 */
    public int $requestTimeout = RequestResponseFuture::DEFAULT_REQUEST_TIMEOUT_MILLIS;

    // ---- 异步发送（对应 Java DefaultMQProducerImpl 的 asyncSenderThreadPoolQueue +
    // defaultAsyncSenderExecutor，以及 NettyRemotingClient 的 publicExecutor）----
    // Java：LinkedBlockingQueue(50000)，池大小 = CPU 核数（core==max，keepAlive 60s）
    public int $asyncSenderQueueCapacity = 50000;
    // Java NettyClientConfig.clientCallbackExecutorThreads 默认 availableProcessors，
    // <=0 时兜到 4；这里 0 表示"按 CPU 核数"
    public int $clientCallbackExecutorThreads = 0;
    private ?ConsumeExecutor $asyncSenderExecutor = null;
    private ?ConsumeExecutor $callbackExecutor = null;

    // ---- 异步发送背压（对应 Java DefaultMQProducerImpl:122-153 的两个公平信号量）----
    // 默认关闭，且开关本身**不是**启动期配置：Java 允许跑到一半再打开/关掉，
    // 三个字段都是普通属性，改容量走 setter。
    public bool $enableBackpressureForAsyncMode = false;
    public int $backPressureForAsyncSendNum = 1024;
    public int $backPressureForAsyncSendSize = 100 * 1024 * 1024;
    private FairSemaphore $semaphoreAsyncSendNum;
    private FairSemaphore $semaphoreAsyncSendSize;

    /**
     * 注入时钟（毫秒，单调口径；对应 Python 的 time.monotonic）。null = microtime(true)*1000。
     * 仅供单测推进超时/重试预算。
     *
     * @var \Closure(): float|null
     */
    public ?\Closure $monoClock = null;

    /**
     * 注入墙钟（毫秒；对应 Python 的 time.time()*1000，Request-Reply 用）。
     *
     * @var \Closure(): int|null
     */
    public ?\Closure $wallClock = null;

    public function __construct(
        ?string $producerGroup = MixAll::DEFAULT_PRODUCER_GROUP,
        ?RPCHook $rpcHook = null,
        string $namespace = '',
        ?array $topics = null,
        ?bool $tlsEnable = null,
        ?bool $enableTraceContext = null,
    ) {
        if ($producerGroup === null || trim($producerGroup) === '') {
            throw new MQClientException('producerGroup is empty');
        }
        $this->producerGroup = $producerGroup;
        $this->tlsEnable = $tlsEnable;
        // W3C traceparent 透传（opt-in；null = 交给 env ROCKETMQ_TRACE_CONTEXT_ENABLE）
        if ($enableTraceContext === null) {
            $enableTraceContext = TraceContextPropagator::traceContextEnabledFromEnv();
        }
        $this->enableTraceContext = (bool) $enableTraceContext;
        $this->namespace = $namespace;
        $this->rpcHook = $rpcHook;
        $this->topics = $topics !== null ? array_values($topics) : [];
        $this->retryResponseCodes = [
            ResponseCode::SYSTEM_ERROR => true,
            ResponseCode::SYSTEM_BUSY => true,
            ResponseCode::SERVICE_NOT_AVAILABLE => true,
            ResponseCode::NO_PERMISSION => true,
            ResponseCode::TOPIC_NOT_EXIST => true,
            ResponseCode::NO_BUYER_ID => true,
            ResponseCode::NOT_IN_CURRENT_UNIT => true,
            ResponseCode::GO_AWAY => true,
        ];
        $this->metrics = new ClientMetrics();
        $this->mqFaultStrategy = new MQFaultStrategy(false);
        // Java 在建 impl 时按配置建两个公平信号量，并对越界的配置兜地板值
        $this->semaphoreAsyncSendNum = new FairSemaphore(
            self::backPressurePermits(
                $this->backPressureForAsyncSendNum,
                FairSemaphore::MIN_ASYNC_SEND_NUM,
                'semaphoreAsyncSendNum'
            )
        );
        $this->semaphoreAsyncSendSize = new FairSemaphore(
            self::backPressurePermits(
                $this->backPressureForAsyncSendSize,
                FairSemaphore::MIN_ASYNC_SEND_SIZE,
                'semaphoreAsyncSendSize'
            )
        );
    }

    // ================================================================ 模块级 helper（Python producer.py 顶层函数）

    /**
     * 对应 Java ``DefaultMQProducerImpl:141-153``：配置不高于地板值时用地板值并记一条日志。
     *
     * Java 的分支写成 ``if (cfg > 10) new Semaphore(max(cfg, 10)) else new Semaphore(10)``，
     * 也就是**恰好等于**地板值时也会走 else 分支（照样打那条日志），这里保持一致。
     */
    public static function backPressurePermits(int $configured, int $floor, string $name): int
    {
        if ($configured > $floor) {
            return $configured;
        }
        Logger::info(sprintf('%s can not be smaller than %d.', $name, $floor));
        return $floor;
    }

    /**
     * 扣多少个「字节」许可（Java ``executeAsyncMessageSend:642`` 的
     * ``msg.getBody() == null ? 1 : msg.getBody().length``）。
     *
     * 批量异步（Java ``DefaultMQProducer.send(Collection, SendCallback, timeout)``，
     * :1121 起）没有 Java 那边的现成公式可依（Java 走 SEND_BATCH + invokeAsync，本实现批量
     * 只有同步内核），这里按每条累加、空 body 也算 1，整批为空时算 1 —— 不给它一个值就等于
     * 一批消息整体只占 1 份容量，字节闸对批量形同虚设。
     */
    public static function backPressureMsgLen(Message|array $msg): int
    {
        if (is_array($msg) || $msg instanceof MessageBatch) {
            $messages = is_array($msg) ? $msg : $msg->getMessages();
            $total = 0;
            foreach ($messages as $one) {
                $total += self::backPressureMsgLen($one);
            }
            return $total === 0 ? 1 : $total;
        }
        $body = $msg->getBody();
        return ($body !== null && $body !== '') ? strlen($body) : 1;
    }

    /**
     * Java ``canBatch`` 里四个延时属性的最大值（缺省 0）。
     *
     * 属性名逐字对齐 ``MessageConst``：``DELAY``（delayTimeLevel）、``TIMER_DELAY_MS``、
     * ``TIMER_DELAY_SEC``、``TIMER_DELIVER_MS``。值非法时与 Java 的
     * ``Integer/Long.parseLong`` 一样**直接抛**，不静默当 0。
     */
    public static function maxDelayValue(Message $msg): int
    {
        $result = 0;
        foreach (self::TIMER_DELAY_PROPERTIES as $name) {
            $raw = $msg->getProperty($name);
            if ($raw === null) {
                continue;
            }
            $trimmed = trim($raw);
            if ($trimmed === '' || preg_match('/^[+-]?\d+$/', $trimmed) !== 1) {
                throw new \InvalidArgumentException(
                    sprintf('invalid integer for property %s: %s', $name, $raw)
                );
            }
            $value = (int) $trimmed;
            if ($value > $result) {
                $result = $value;
            }
        }
        return $result;
    }

    /**
     * 对应 Java ``sendMessageAsync`` 里 ``operationFail`` 的三分支：包装 + 判定能否重试。
     *
     * ⚠ 只有 remoting 层抛回来的异常走这里。**已经收到响应**、但 ``parseSendResponse``
     * 判定为失败的错误（``MQBrokerException``）不走这里 —— Java 那条路径传的是
     * ``needRetry=false`` 且**原样**抛出。也就是说异步发送**不看** ``retryResponseCodes``：
     * broker 明确回了错就不会换 broker 重试。别和同步发送的语义混为一谈。
     *
     * @return array{0: \Throwable, 1: bool} [包装后的异常, 是否可重试]
     */
    public static function classifyAsyncFailure(\Throwable $error, int $cost): array
    {
        if ($error instanceof RemotingSendRequestException) {
            return [new MQClientException('send request failed', null, $error), true];
        }
        if ($error instanceof RemotingTimeoutException) {
            return [new MQClientException(sprintf('wait response timeout, cost=%d', $cost), null, $error), true];
        }
        if ($error instanceof RemotingException) {
            $retry = !($error instanceof RemotingTooMuchRequestException);
            return [new MQClientException('unknown reason', null, $error), $retry];
        }
        return [$error, false];
    }

    /**
     * 对应 Python 装饰器 ``_restores_caller_message`` 的还原体（PHP 无装饰器，做成
     * 显式 try/finally：发送结束后还原**调用方**那条 Message，Java
     * ``sendKernelImpl:1095-1096`` 的 finally）。
     *
     * body 换回压缩前那一份、topic 剥掉命名空间。不还原会坏在哪：压缩是就地
     * ``msg->setBody(...)``，调用方拿着同一个对象再发一次，第二次就把压缩流当原文又压
     * 一遍（zlib(zlib(x))），消费端只解一层、拿到的是压缩流；带 namespace 时调用方还会
     * 一直盯着被改写的 ``ns%topic``。Java 的两个入口（``DefaultMQProducerImpl:930`` 取
     * ``prevBody``）在成功、失败、拦截钩子抛异常三条路上都会还原，所以必须在 ``finally``
     * 而不是正常返回路径。
     *
     * topic 是 ``withoutNamespace`` 而不是"恢复进入时的字符串"——Java 的 finally 就是这么
     * 写的：调用方自己传进来、本来就带前缀的 topic 同样会被剥掉。
     *
     * 只对单条消息生效（批量路径由调用方在进入前分流，不做还原）。
     */
    private function restoreCallerMessage(Message $msg, string $prevBody): void
    {
        $msg->setBody($prevBody);
        $msg->setTopic(NamespaceUtil::withoutNamespace($msg->getTopic(), $this->namespace));
    }

    private function monoMillis(): float
    {
        $clock = $this->monoClock;
        return $clock !== null ? $clock() : microtime(true) * 1000.0;
    }

    private function wallMillis(): int
    {
        $clock = $this->wallClock;
        return $clock !== null ? $clock() : (int) (microtime(true) * 1000.0);
    }

    // ================================================================ 配置

    /** 按 ";" 拆分并去空白（对应 Python set_namesrv_addr）。 */
    public function setNamesrvAddr(string $addr): void
    {
        $addrs = [];
        foreach (explode(';', $addr) as $a) {
            $a = trim($a);
            if ($a !== '') {
                $addrs[] = $a;
            }
        }
        $this->nameServerAddrs = $addrs;
    }

    /** @param list<string> $addrs */
    public function setNameServerAddresses(array $addrs): void
    {
        $this->nameServerAddrs = array_values($addrs);
    }

    public function getNamesrvAddr(): string
    {
        return implode(';', $this->nameServerAddrs);
    }

    public function setInstanceName(string $name): void
    {
        $this->instanceName = $name;
    }

    /** 对应 Java `ClientConfig#setUnitName`：影响 clientId 后缀与动态取址 URL。 */
    public function setUnitName(?string $unitName): void
    {
        $this->unitName = $unitName;
    }

    public function getUnitName(): ?string
    {
        return $this->unitName;
    }

    /** 对应 Java `ClientConfig#setUnitMode`：随请求头/心跳上报给 broker。 */
    public function setUnitMode(bool $unitMode): void
    {
        $this->unitMode = $unitMode;
    }

    public function isUnitMode(): bool
    {
        return $this->unitMode;
    }

    /** 对应 Java `ClientConfig#setEnableStreamRequestType`。 */
    public function setEnableStreamRequestType(bool $enable): void
    {
        $this->enableStreamRequestType = $enable;
    }

    public function setMaxMessageSize(int $size): void
    {
        $this->maxMessageSize = $size;
    }

    public function setSendMsgTimeout(int $timeout): void
    {
        $this->sendMsgTimeout = $timeout;
    }

    public function setRetryTimesWhenSendFailed(int $n): void
    {
        $this->retryTimesWhenSendFailed = $n;
    }

    public function setSendMsgMaxTimeoutPerRequest(int $timeout): void
    {
        $this->sendMsgMaxTimeoutPerRequest = $timeout;
    }

    public function getSendMsgMaxTimeoutPerRequest(): int
    {
        return $this->sendMsgMaxTimeoutPerRequest;
    }

    public function setRetryAnotherBrokerWhenNotStoreOk(bool $retry): void
    {
        $this->retryAnotherBrokerWhenNotStoreOk = $retry;
    }

    public function isRetryAnotherBrokerWhenNotStoreOk(): bool
    {
        return $this->retryAnotherBrokerWhenNotStoreOk;
    }

    // ---------------- 异步发送背压（Java DefaultMQProducer:1368-1408）----------------

    public function setEnableBackpressureForAsyncMode(bool $enable): void
    {
        $this->enableBackpressureForAsyncMode = $enable;
    }

    public function isEnableBackpressureForAsyncMode(): bool
    {
        return $this->enableBackpressureForAsyncMode;
    }

    /**
     * 运行时改「在途条数」上限（Java ``setBackPressureForAsyncSendNum:1383-1391``）。
     *
     * 语义不是「设成 num」而是「总量变成 num、已经在途的那几份原样保留」：改完之后
     * ``空闲许可 = num - 在途份数``（Java 写成先算 ``acquired = 旧配置 - 空闲``、再
     * ``new Semaphore(num - acquired)``，同一个结果，它的测试断言的正是这个和，见
     * ``DefaultMQProducerTest:593-595``）。所以调小之后空闲许可可能是负数（在途超额），
     * 归还许可时才慢慢回正 —— Java 的 ``new Semaphore(负数)`` 同样接受。
     *
     * ⚠ 改容量要走这个方法，别直接赋 ``backPressureForAsyncSendNum``：
     * 直接改字段只动配置值，信号量不会跟着变（Java 的字段是 private，没这个坑）。
     */
    public function setBackPressureForAsyncSendNum(int $num): void
    {
        $num = max($num, FairSemaphore::MIN_ASYNC_SEND_NUM);
        $this->backPressureForAsyncSendNum = $num;
        $this->semaphoreAsyncSendNum->setTotalPermits($num);
    }

    public function getBackPressureForAsyncSendNum(): int
    {
        return $this->backPressureForAsyncSendNum;
    }

    /** 运行时改「在途字节数」上限，语义与 ``setBackPressureForAsyncSendNum`` 相同。 */
    public function setBackPressureForAsyncSendSize(int $size): void
    {
        $size = max($size, FairSemaphore::MIN_ASYNC_SEND_SIZE);
        $this->backPressureForAsyncSendSize = $size;
        $this->semaphoreAsyncSendSize->setTotalPermits($size);
    }

    public function getBackPressureForAsyncSendSize(): int
    {
        return $this->backPressureForAsyncSendSize;
    }

    /** 对应 Java ``DefaultMQProducerImpl:200-202``（观测用，也是改容量的输入）。 */
    public function getSemaphoreAsyncSendNumAvailablePermits(): int
    {
        return $this->semaphoreAsyncSendNum->availablePermits();
    }

    public function getSemaphoreAsyncSendSizeAvailablePermits(): int
    {
        return $this->semaphoreAsyncSendSize->availablePermits();
    }

    public function addRetryResponseCode(int $responseCode): void
    {
        $this->retryResponseCodes[$responseCode] = true;
    }

    public function isRetryResponseCode(?int $responseCode): bool
    {
        return $responseCode !== null && isset($this->retryResponseCodes[$responseCode]);
    }

    public function setCompressMsgBodyOverHowmuch(int $size): void
    {
        $this->compressMsgBodyOverHowmuch = $size;
    }

    public function setCompressLevel(int $level): void
    {
        $this->compressLevel = $level;
    }

    public function setCompressType(int $compressionType): void
    {
        $this->compressType = $compressionType;
    }

    public function setCreateTopicKey(string $key): void
    {
        $this->createTopicKey = $key;
    }

    public function setDefaultTopicQueueNums(int $n): void
    {
        $this->defaultTopicQueueNums = $n;
    }

    public function getProducerGroup(): string
    {
        return $this->producerGroup;
    }

    public function setProducerGroup(string $group): void
    {
        if ($this->started) {
            throw new MQClientException('producerGroup cannot be changed after startup');
        }
        $this->producerGroup = $group;
    }

    /** 对应 Java DefaultMQProducer.setSendLatencyFaultEnable。默认关闭。 */
    public function setSendLatencyFaultEnable(bool $enable): void
    {
        $this->sendLatencyFaultEnable = $enable;
        $this->mqFaultStrategy->setSendLatencyFaultEnable($enable);
    }

    /** 设置 Request-Reply 的默认超时（不传 timeout 给 request() 时用它）。 */
    public function setRequestTimeout(int $timeoutMillis): void
    {
        $this->requestTimeout = $timeoutMillis;
    }

    // ---------------- 自动攒批（对应 Java DefaultMQProducer:1189-1243）----------------
    // 这套 setter 是"两处记账"：先写 producer 自己的字段（保证 start() 之前设置也生效），
    // 累加器已存在时**立刻**同步下去（Java 同）。三个 getter 在累加器还没建时返回 0，
    // 与 Java 的 `if (produceAccumulator == null) return 0;` 一致（不是 -1）。

    /**
     * 开关自动攒批（Java ``setAutoBatch``，默认 false）。
     *
     * 打开后 ``send(Message)``（未显式给 timeout、且不是批量消息）会按
     * ``AggregateKey(topic, mq, waitStoreMsgOK, tag)`` 攒批，攒够 ``batchMaxDelayMs``
     * 或 ``batchMaxBytes`` 再发。**没 start 时 getAutoBatch() 恒为 false**（Java 的
     * ``getAutoBatch`` 有 ``produceAccumulator == null`` 短路）。
     */
    public function setAutoBatch(bool $autoBatch): void
    {
        $this->autoBatch = $autoBatch;
    }

    public function getAutoBatch(): bool
    {
        if ($this->produceAccumulator === null) {
            return false;
        }
        return $this->autoBatch;
    }

    public function isAutoBatch(): bool
    {
        return $this->getAutoBatch();
    }

    /** 单批最大等待毫秒（Java ``batchMaxDelayMs``，取值 (0, 30000]）。 */
    public function setBatchMaxDelayMs(int $holdMs): void
    {
        $this->batchMaxDelayMs = $holdMs;
        if ($this->produceAccumulator !== null) {
            $this->produceAccumulator->batchMaxDelayMs($holdMs);
        }
    }

    public function getBatchMaxDelayMs(): int
    {
        if ($this->produceAccumulator === null) {
            return 0;
        }
        return $this->produceAccumulator->getBatchMaxDelayMs();
    }

    /** 单批最大字节（Java ``batchMaxBytes``，取值 (0, 2MB]）。 */
    public function setBatchMaxBytes(int $holdSize): void
    {
        $this->batchMaxBytes = $holdSize;
        if ($this->produceAccumulator !== null) {
            $this->produceAccumulator->batchMaxBytes($holdSize);
        }
    }

    public function getBatchMaxBytes(): int
    {
        if ($this->produceAccumulator === null) {
            return 0;
        }
        return $this->produceAccumulator->getBatchMaxBytes();
    }

    /**
     * **全部**在途批次的总字节上限（Java ``totalBatchMaxBytes``，> 0）。
     *
     * 超过之后 ``tryAddMessage`` 会拒绝新消息，``send()`` 自动退回直发。
     */
    public function setTotalBatchMaxBytes(int $totalHoldSize): void
    {
        $this->totalBatchMaxBytes = $totalHoldSize;
        if ($this->produceAccumulator !== null) {
            $this->produceAccumulator->totalBatchMaxBytes($totalHoldSize);
        }
    }

    public function getTotalBatchMaxBytes(): int
    {
        if ($this->produceAccumulator === null) {
            return 0;
        }
        return $this->produceAccumulator->getTotalBatchMaxBytes();
    }

    /** 返回本生产者的基础指标计数器（send/consume RT 与计数）。 */
    public function getMetrics(): ClientMetrics
    {
        return $this->metrics;
    }

    // ---------------- 消息轨迹配置（对应 Java ClientConfig / DefaultMQProducer）----------------

    /**
     * 开关消息轨迹（Java ``ClientConfig.setEnableTrace``，默认 false）。
     *
     * 开启后 ``start()`` 会注册 SendMessageTraceHook，把每条消息的发送轨迹
     * 异步发到轨迹 topic。**内部轨迹生产者自身必须保持关闭**，否则无限递归。
     */
    public function setEnableTrace(bool $enable): void
    {
        $this->enableTrace = $enable;
    }

    public function isEnableTrace(): bool
    {
        return $this->enableTrace;
    }

    /** 自定义轨迹 topic（Java ``ClientConfig.setTraceTopic``）；空则用系统默认。 */
    public function setTraceTopic(?string $traceTopic): void
    {
        $this->traceTopic = $traceTopic;
    }

    public function setTraceMsgBatchNum(int $n): void
    {
        $this->traceMsgBatchNum = $n;
    }

    /** 注册发送钩子（对应 Java DefaultMQProducerImpl.registerSendMessageHook）。 */
    public function registerSendMessageHook(SendMessageHook $hook): void
    {
        $this->sendMessageHookList[] = $hook;
    }

    public function hasSendMessageHook(): bool
    {
        return $this->sendMessageHookList !== [];
    }

    // ---------------- 发送前拦截钩子（对应 Java CheckForbiddenHook 三个方法）----------------

    /** 注册发送前拦截钩子（Java DefaultMQProducerImpl.registerCheckForbiddenHook:186）。 */
    public function registerCheckForbiddenHook(CheckForbiddenHook $hook): void
    {
        $this->checkForbiddenHookList[] = $hook;
    }

    public function hasCheckForbiddenHook(): bool
    {
        return $this->checkForbiddenHookList !== [];
    }

    private function hasSendInterceptors(): bool
    {
        return $this->sendMessageHookList !== [] || $this->checkForbiddenHookList !== [];
    }

    /**
     * ⚠ 与 send/consume 钩子不同：这里**不吞异常**（Java 签名 ``throws MQClientException``）。
     *
     * 钩子抛出的异常会沿 sendDefaultImpl 的重试链向上传播，这正是"禁止发送"的实现方式。
     */
    public function executeCheckForbiddenHook(CheckForbiddenContext $context): void
    {
        if (!$this->hasCheckForbiddenHook()) {
            return;
        }
        foreach ($this->checkForbiddenHookList as $hook) {
            $hook->checkForbidden($context);
        }
    }

    /** 注册事务收尾钩子（对应 Java registerEndTransactionHook）。 */
    public function registerEndTransactionHook(EndTransactionHook $hook): void
    {
        $this->endTransactionHookList[] = $hook;
    }

    public function executeEndTransactionHook(EndTransactionContext $context): void
    {
        foreach ($this->endTransactionHookList as $hook) {
            try {
                $hook->endTransaction($context);
            } catch (\Throwable $e) {
                Logger::warning('failed to executeEndTransactionHook: ' . $e->getMessage());
            }
        }
    }

    // ---------------- 发送钩子调用（对应 Java executeSendMessageHookBefore/After）----------------

    /** 钩子异常一律吞掉并记 warn（Java DefaultMQProducerImpl:1159）。 */
    public function executeSendMessageHookBefore(SendMessageContext $context): void
    {
        foreach ($this->sendMessageHookList as $hook) {
            try {
                $hook->sendMessageBefore($context);
            } catch (\Throwable $e) {
                Logger::warning('failed to executeSendMessageHookBefore: ' . $e->getMessage());
            }
        }
    }

    public function executeSendMessageHookAfter(SendMessageContext $context): void
    {
        foreach ($this->sendMessageHookList as $hook) {
            try {
                $hook->sendMessageAfter($context);
            } catch (\Throwable $e) {
                Logger::warning('failed to executeSendMessageHookAfter: ' . $e->getMessage());
            }
        }
    }

    /**
     * 构造 SendMessageContext（对齐 Java DefaultMQProducerImpl:969-989）。
     *
     * msgType 的判定顺序也照抄：TRAN_MSG=true → Trans_Msg_Half；
     * 带任何延迟类属性 → Delay_Msg；否则 Normal_Msg。
     */
    private function buildSendContext(
        Message $msg,
        MessageQueue $mq,
        string $brokerAddr,
        string $communicationMode = CommunicationMode::SYNC,
    ): SendMessageContext {
        $context = new SendMessageContext();
        $context->producer = $this;
        $context->producerGroup = $this->producerGroup;
        $context->message = $msg;
        $context->mq = $mq;
        $context->brokerAddr = $brokerAddr;
        $context->namespace = $this->namespace;
        $context->communicationMode = $communicationMode;
        if ($msg->getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED) === 'true') {
            $context->msgType = MessageType::TRANS_MSG_HALF;
        }
        foreach (['__STARTDELIVERTIME', MessageConst::PROPERTY_DELAY_TIME_LEVEL,
            'TIMER_DELIVER_MS', 'TIMER_DELAY_SEC', 'TIMER_DELAY_MS'] as $key) {
            if ($msg->getProperty($key) !== null) {
                $context->msgType = MessageType::DELAY_MSG;
                break;
            }
        }
        return $context;
    }

    /**
     * 构造 CheckForbiddenContext 并执行（异常**不吞**，见 executeCheckForbiddenHook）。
     */
    private function executeCheckForbidden(
        Message $msg,
        MessageQueue $mq,
        string $brokerAddr,
        mixed $arg = null,
        string $communicationMode = CommunicationMode::SYNC,
    ): void {
        $context = new CheckForbiddenContext();
        $context->nameSrvAddr = $this->getNamesrvAddr();
        $context->group = $this->producerGroup;
        $context->communicationMode = $communicationMode;
        $context->brokerAddr = $brokerAddr;
        $context->message = $msg;
        $context->mq = $mq;
        $context->unitMode = $this->unitMode;
        $context->arg = $arg;
        $this->executeCheckForbiddenHook($context);
    }

    /**
     * 每次发请求都要带上、且只来源于 producer 配置的那几个头字段（键名即
     * ``MQClientInstance::sendMessage`` 的命名参数，可直接 ``...`` 展开）。
     *
     * 对位 Java `DefaultMQProducerImpl.sendKernelImpl`：
     *   - :996-997 `setDefaultTopic(producer.getCreateTopicKey())` /
     *     `setDefaultTopicQueueNums(producer.getDefaultTopicQueueNums())` ——
     *     broker 侧新建 topic 时用这两个值决定队列数，写死默认值会让
     *     `setCreateTopicKey` / `setDefaultTopicQueueNums` 变成假 setter。
     *   - :1007 `setBrokerName(brokerName)`，V2 编码成单字母键 `n`。
     *   - unit_mode 同 :991。
     *
     * @return array{unitMode: bool, defaultTopic: string, defaultTopicQueueNums: int}
     */
    private function sendHeaderArgs(): array
    {
        return [
            'unitMode' => $this->unitMode,
            'defaultTopic' => $this->createTopicKey,
            'defaultTopicQueueNums' => $this->defaultTopicQueueNums,
        ];
    }

    /**
     * 真正发起请求的那一步（对应 Java sendKernelImpl 内的钩子点）。
     *
     * 执行顺序严格照抄 Java `sendKernelImpl:956-990`：
     *   1. **CheckForbiddenHook**（每次尝试都跑；异常**不吞**，直接抛给重试链）
     *   2. SendMessageHook.before
     *   3. 发请求
     *   4. SendMessageHook.after（成功带 sendResult / 失败带 exception）
     * 重试时每轮都会重建 context，所以钩子会被调用多次 —— 与 Java 一致。
     */
    private function sendWithHooks(
        MQClientInstance $client,
        Message $msg,
        MessageQueue $mqSel,
        int $timeout,
        int $sysFlag,
        mixed $arg = null,
        string $communicationMode = CommunicationMode::SYNC,
    ): SendResult {
        $brokerAddr = $client->publishAddrFor($mqSel->brokerName, $mqSel->topic);
        if ($this->hasCheckForbiddenHook()) {
            $this->executeCheckForbidden($msg, $mqSel, $brokerAddr, $arg, $communicationMode);
        }
        // W3C traceparent 透传（opt-in）：没有就注入根上下文，已有值不覆盖
        if ($this->enableTraceContext) {
            TraceContextPropagator::injectTraceContext($msg);
        }
        if ($this->sendMessageHookList === []) {
            return $client->sendMessage(
                $this->producerGroup,
                $msg,
                $mqSel,
                $timeout,
                $sysFlag,
                ...$this->sendHeaderArgs()
            );
        }
        $context = $this->buildSendContext($msg, $mqSel, $brokerAddr, $communicationMode);
        $this->executeSendMessageHookBefore($context);
        try {
            $result = $client->sendMessage(
                $this->producerGroup,
                $msg,
                $mqSel,
                $timeout,
                $sysFlag,
                ...$this->sendHeaderArgs()
            );
        } catch (\Throwable $e) {
            $context->exception = $e;
            $this->executeSendMessageHookAfter($context);
            throw $e;
        }
        $context->sendResult = $result;
        $this->executeSendMessageHookAfter($context);
        return $result;
    }

    // ================================================================ 生命周期

    public function start(): void
    {
        if ($this->started) {
            return;
        }
        // 生产者组也拼命名空间（对齐 Java DefaultMQProducer.start:375
        // setProducerGroup(withNamespace(producerGroup))），broker 侧按带前缀的组名登记
        if ($this->namespace !== '') {
            $this->producerGroup = NamespaceUtil::wrapNamespace($this->namespace, $this->producerGroup);
        }
        // 对应 Java DefaultMQProducerImpl.checkConfig（:295）：组名校验排在拼完命名空间之后
        // （Java 也是 start() 先 withNamespace 再 impl.start()），并且要挡住
        // DEFAULT_PRODUCER —— 多进程共用默认组会互相踢下线。checkConfig 是 Java start() 的
        // 第一步，所以这里也领先于 name server 地址检查。
        Validators::checkGroup($this->producerGroup);
        if ($this->producerGroup === MixAll::DEFAULT_PRODUCER_GROUP) {
            throw new MQClientException(sprintf(
                'producerGroup can not equal %s, please specify another one.',
                MixAll::DEFAULT_PRODUCER_GROUP
            ));
        }
        if ($this->nameServerAddrs === [] && !DefaultTopAddressing::isConfigured()) {
            // 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用。
            // 码值用 Java 的 10004（``validateNameServerSetting`` 对同一个故障给的码）：
            // Java 不在 start() 里查，故障要等第一次发送才以 10004 冒出来，本端口
            // 提前到 start（更可预期），但不能让调用方看到两种不同的码。
            throw new MQClientException('name server address is not set', ClientErrorCode::NO_NAME_SERVER_EXCEPTION);
        }
        // 对应 Java `DefaultMQProducerImpl#start`:250-252 的两步：先
        // `changeInstanceNameToPID`（Java 只对非 CLIENT_INNER_PRODUCER 的生产者做，
        // 本客户端没有内部生产者，所以无条件执行），再由 `ClientConfig#buildMQClientId`
        // 拼 `<本机 IP>@<instanceName>[@<unitName>][@STREAM]`。instanceName 就地写回，
        // 和 Java 一样：第二次 start() 复用同一个 clientId，而不是每重启一次换一个名字。
        $this->instanceName = MixAll::changeInstanceNameToPid($this->instanceName);
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
        // 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本生产者
        if ($this->nameServerAddrs === [] && $this->mqClient->nameServerAddrs !== []) {
            $this->nameServerAddrs = array_values($this->mqClient->nameServerAddrs);
        }
        // 注册 broker 主动请求处理器：事务回查 CHECK_TRANSACTION_STATE(39)。
        // 按 message_ext 的 PGROUP 属性匹配本生产者，不匹配则丢弃。
        // PHP 单线程：回查处理内联执行（Python 起线程），见 _handleCheckTransactionState。
        $this->mqClient->remotingClient->registerProcessor(
            RequestCode::CHECK_TRANSACTION_STATE,
            $this->handleCheckTransactionState(...)
        );
        // 异步发送的两个线程池（Java 在构造器里 new，线程本身按需创建）
        $this->createAsyncExecutors();
        // 自动攒批：按 clientId 复用累加器（Java DefaultMQProducerImpl:256 →
        // MQClientManager.getOrCreateProduceAccumulator），并把 producer 上先设好的
        // 三个阈值同步过去。放在 registerProducer 之前，与 Java 同序。
        $this->initProduceAccumulator();
        $this->started = true;
        $this->heartbeatRunning = true;
        // 守卫线程要在生产者本体起来之后再拉（Java DefaultMQProducer.start:377-379：
        // impl.start() 返回后 accumulated.start()）。
        // ⚠ PHP 无线程：心跳线程（Python _heartbeat_loop）不启动，改由调用方显式
        // ``heartbeatOnce()`` 驱动；不注册心跳时 COMMIT/ROLLBACK 仍能成功（客户端主动
        // END_TRANSACTION），但 UNKNOW 的半消息不会被回查。
        if ($this->produceAccumulator !== null) {
            $this->produceAccumulator->start();
        }
        // 轨迹分发器在锁外启动（Java 同样在 defaultMQProducerImpl.start() 之后做）：
        // 它要新建内部生产者并拉路由，属网络操作。
        $this->startTraceDispatcher();
    }

    /**
     * 对应 Java DefaultMQProducer.start():380-405。
     *
     * enableTrace=true 时建 AsyncTraceDispatcher（Type=PRODUCE）并注册
     * SendMessageTraceHook；随后无论新建还是复用，都要 start 它。
     * 任何异常都只记日志 —— 轨迹挂了不能影响正常发送。
     */
    private function startTraceDispatcher(): void
    {
        if ($this->enableTrace) {
            try {
                $dispatcher = new AsyncTraceDispatcher(
                    $this->producerGroup,
                    TraceDispatcherType::PRODUCE,
                    $this->traceMsgBatchNum,
                    $this->traceTopic,
                    $this->rpcHook
                );
                $dispatcher->setHostProducer($this);
                $this->traceDispatcher = $dispatcher;
                $this->registerSendMessageHook(new SendMessageTraceHook($dispatcher));
                $this->registerEndTransactionHook(new EndTransactionTraceHook($dispatcher));
            } catch (\Throwable $e) {
                Logger::error('system mqtrace hook init failed ,maybe can\'t send msg trace data: ' . $e->getMessage());
            }
        }
        if ($this->traceDispatcher !== null) {
            try {
                $this->traceDispatcher->start($this->getNamesrvAddr(), AccessChannel::LOCAL);
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
        $this->heartbeatRunning = false;
        // 对齐 Java DefaultMQProducerImpl.shutdown:313-317：先关异步发送池，再关客户端实例。
        // ⚠ Java 这里用的是不等待的 shutdown()，所以「send_async 完立刻 shutdown」会丢掉
        // 还没跑完的任务（回调里拿到 RemotingConnectException）。本实现照抄：不等。
        // 调用方要确保发完，自己等回调。
        if ($this->asyncSenderExecutor !== null) {
            $this->asyncSenderExecutor->shutdown();
            $this->asyncSenderExecutor = null;
        }
        if ($this->callbackExecutor !== null) {
            $this->callbackExecutor->shutdown();
            $this->callbackExecutor = null;
        }
        // 优雅注销（对齐 Java DefaultMQProducerImpl.shutdown:313 →
        // MQClientInstance.unregisterProducer:1198-1201 → unregisterClient(group, null)）：
        // 逐台 broker 发 UNREGISTER_CLIENT(35)，让 broker 端 ProducerManager 立刻把本
        // clientId 从 groupChannelTable 摘掉。不发的话只能等心跳超时（默认 ~120s）清理，
        // 这段时间 broker 仍认为该组有连接（`examineProducerConnectionInfo` 看得见、
        // 事务回查也会挑到这个已经退出的 channel）。
        // ⚠ 必须在 mqClient->shutdown() **之前**：35 只能走还开着的长连接，
        //   broker 侧也只摘除「这条 frame 到达的那条 channel」（ClientManageProcessor:226-230）。
        // 与消费侧同一口径：任何异常只记 debug，shutdown 不因网络抖动抛异常。
        if ($this->mqClient !== null) {
            try {
                $this->mqClient->unregisterClientAllBrokers($this->clientId ?? '', $this->producerGroup, '');
            } catch (\Throwable $e) {
                Logger::debug('unregister on shutdown failed: ' . $e->getMessage());
            }
            $this->mqClient->shutdown();
        }
        $this->started = false;
        // Java DefaultMQProducer.shutdown():410-412：impl.shutdown() 之后关累加器的
        // 两个守卫线程。注意**只停线程**，不清表：在途批次没了守卫线程唤醒，同步
        // send 会一直等到自己的 holdMs 阈值再自己发出去（与 Java 同）。
        if ($this->produceAccumulator !== null) {
            $this->produceAccumulator->shutdown();
        }
        // 顺序对齐 Java DefaultMQProducer.shutdown()：先关本生产者，再 flush 并关轨迹分发器
        // （分发器用的是**自己的**内部生产者，与本客户端实例无关，所以关掉了照样能发完）
        if ($this->traceDispatcher !== null) {
            try {
                $this->traceDispatcher->shutdown();
            } catch (\Throwable $e) {
                Logger::warning('trace dispatcher shutdown failed: ' . $e->getMessage());
            }
        }
    }

    /**
     * 周期性向所有已知 broker 发心跳（含 ProducerData）—— Python ``_heartbeat_loop``
     * 线程体的**单轮**显式泵（PHP 无线程，由调用方按 ``heartbeatIntervalMillis``
     * 周期驱动）。对齐 Java 的生产者注册。
     */
    public function heartbeatOnce(int $timeoutMillis = 5000): int
    {
        return $this->sendHeartbeatToAllBroker($timeoutMillis);
    }

    private function sendHeartbeatToAllBroker(int $timeoutMillis = 5000): int
    {
        $client = $this->mqClient;
        if ($client === null) {
            return 0;
        }
        try {
            $addrs = $client->getRouteOfAllBrokers();
        } catch (\Throwable $e) {
            Logger::warning('producer heartbeat: gather brokers failed: ' . $e->getMessage());
            return 0;
        }
        if ($addrs === []) {
            return 0;
        }
        $hb = new HeartbeatData($this->clientId ?? '');
        $pd = new ProducerData($this->producerGroup);
        // ⚠ Python 的 HeartbeatData 没有 heartbeatFingerprint / withoutSub 字段
        // （已知缺陷，见项目记忆）。缺失时 broker 反序列化为 0，等价于走 V1 注册路径，
        // 与 C++ 侧显式置 0 的效果一致，这里保持现状不引入新差异。
        $hb->producerDataSet[] = $pd;
        $okCount = 0;
        foreach ($addrs as $addr) {
            try {
                $client->sendHeartbeat($addr, $hb, $timeoutMillis);
                $okCount++;
            } catch (\Throwable $e) {
                Logger::warning(sprintf('producer heartbeat to %s failed: %s', $addr, $e->getMessage()));
            }
        }
        return $okCount;
    }

    private function requireClient(): MQClientInstance
    {
        if (!$this->started || $this->mqClient === null) {
            throw new MQClientException('producer not started, call start() first');
        }
        return $this->mqClient;
    }

    // ================================================================ 压缩

    /**
     * 满足阈值且非批量时**就地压缩 msg.body**，返回应下发的 sys_flag。
     *
     * 与 Java 逐条对齐：
     *   * 批量消息（MessageBatch）**永不压缩**；
     *   * body 长度 >= compressMsgBodyOverHowmuch（默认 4096）才压缩；
     *   * 压缩失败按 Java 的做法**降级为不压缩**并记日志，而不是让发送失败；
     *   * 压缩后不比较体积（Java 也不比较）。
     * 不压缩时返回 0。
     */
    public function tryToCompressMessage(Message $msg): int
    {
        if ($msg instanceof MessageBatch) {
            return 0;
        }
        $body = $msg->getBody();
        if ($body === null || $body === '' || strlen($body) < $this->compressMsgBodyOverHowmuch) {
            return 0;
        }
        try {
            $compressed = $this->compressBody($body, $this->compressType, $this->compressLevel);
        } catch (\Throwable $e) {
            // 对齐 Java：压缩失败降级为不压缩
            Logger::warning('tryToCompressMessage failed, send uncompressed: ' . $e->getMessage());
            return 0;
        }
        if ($compressed === '') {
            return 0;
        }
        $msg->setBody($compressed);
        $sysFlag = MessageSysFlag::COMPRESSED_FLAG;
        return MessageSysFlag::setCompressionType($sysFlag, $this->compressType);
    }

    /**
     * 对应 Python message_decoder._compress：ZLIB 走 gzcompress，LZ4（Frame）/ZSTD
     * （CLI 优先、Raw/RLE 兜底）由 CompressionCodec 承担，与 MessageDecoder 的
     * 口径一致；未知类型抛出 → tryToCompressMessage 降级不压缩。
     */
    private function compressBody(string $data, int $compressionType, int $level): string
    {
        $ctype = MessageDecoder::normalizeCompressionType($compressionType);
        if ($ctype === MessageSysFlag::ZLIB_TYPE) {
            $out = @gzcompress($data, $level);
            if ($out === false) {
                throw new \RuntimeException('zlib compress failed');
            }
            return $out;
        }
        if ($ctype === MessageSysFlag::LZ4_TYPE) {
            return CompressionCodec::lz4CompressFrame($data);
        }
        if ($ctype === MessageSysFlag::ZSTD_TYPE) {
            return CompressionCodec::zstdCompress($data);
        }
        throw new \RuntimeException(sprintf('unsupported compression type: %d', $compressionType));
    }

    /** 给 topic 拼上命名空间前缀（对应 Java ClientConfig.withNamespace）。 */
    private function withNamespace(string $topic): string
    {
        if ($this->namespace === '') {
            return $topic;
        }
        return NamespaceUtil::wrapNamespace($this->namespace, $topic);
    }

    /**
     * 定点发送的 topic 一致性守卫（Java ``DefaultMQProducerImpl:1234-1236`` 同步 /
     * ``:1277-1278`` 异步，两处文案不同，由调用方给）。
     *
     * 比较的是**各自拼过命名空间之后**的名字 —— Java 的公开入口先
     * ``msg.setTopic(withNamespace(...))``、再把 mq 过 ``ClientConfig.queueWithNamespace``，
     * 所以比的是同一命名空间下的两个资源名（``wrapNamespace`` 幂等，已经带前缀的入参不会
     * 套两层）。少了这道守卫，topic 与目标队列不符的消息照样发得出去：broker 按请求里带的
     * 队列名写入，SendResult 一切正常，而消息落进了**另一个 topic** 的分区，谁也消费不到。
     */
    private function checkPinnedTopic(string $topic, MessageQueue $mq, string $message): void
    {
        if ($this->withNamespace($mq->topic) !== $topic) {
            throw new MQClientException($message);
        }
    }

    /**
     * 对应 Java DefaultMQProducerImpl.tryToFindTopicPublishInfo。
     *
     * 先拉**真实**路由；只有确实拉不到（新 topic 尚未在 NameServer 注册）时才按 Java 的做法
     * 回退到默认 topic（TBW102）为该 topic 合成发布信息，否则新 topic 的**首条**消息没队列可选。
     * 消费者路径不做这个兜底（理由见 MQClientInstance.updateTopicRouteInfoFromNameServer）。
     */
    private function topicPublishInfo(string $topic): TopicPublishInfo
    {
        $client = $this->requireClient();
        $client->registerTopicInUse($topic);
        try {
            return $client->getTopicPublishInfo($topic);
        } catch (MQClientException $e) {
            try {
                return $client->getTopicPublishInfo($topic, true);
            } catch (MQClientException $inner) {
                // Java 的三条「拿不到路由」分支都是先 validateNameServerSetting() 再抛
                // no-route：一个 name server 地址都没有（配了地址服务器却没返回地址也算）
                // 时报 10004，而不是把寻址故障说成"这个 topic 没路由"。
                $this->validateNameServerSetting();
                throw $inner;
            }
        }
    }

    /**
     * 对应 Java ``DefaultMQProducerImpl#validateNameServerSetting``。
     *
     * 只在「拿不到路由」这条分支上跑，把两种完全不同的故障分开：
     * * 压根一个 name server 地址都没有（配了地址服务器却没返回地址也算这种）
     *   → 10004 ``No name server address, please set it.``
     * * 地址有、只是这个 topic 没路由 → 让调用方原来那条异常照旧抛（10005 等）。
     * 少了这一步，用户把地址服务器配错拿到空列表，看到的却是"No route info of this
     * topic"——那是"topic 不存在"的意思，把人往建 topic 的方向查，而真正坏的是寻址。
     * Java 的文案后面拼了 ``FAQUrl.suggestTodo(...)``，本仓库按约定不带后缀。
     */
    private function validateNameServerSetting(): void
    {
        $client = $this->mqClient;
        if ($client === null || $client->nameServerAddrs === []) {
            throw new MQClientException(
                'No name server address, please set it.',
                ClientErrorCode::NO_NAME_SERVER_EXCEPTION
            );
        }
    }

    /**
     * 容错表记一次尝试的延迟。延迟必须用单调钟量：本地亚毫秒往返用毫秒墙钟差
     * 会记成 0，那样延迟阈值永远不会生效。
     */
    private function updateFaultItem(?MessageQueue $selected, float $began, bool $isolation, bool $reachable): void
    {
        if ($selected === null) {
            return;
        }
        $this->mqFaultStrategy->updateFaultItem($selected->brokerName, $this->monoMillis() - $began, $isolation, $reachable);
    }

    // ================================================================ 正常发送

    /**
     * 同步发送：msg 或 array；指定 mq 走定点发送，否则轮询选择。
     * （对应 Python 装饰器 ``@_restores_caller_message`` 包裹的 ``send``：还原语义
     * 见 ``restoreCallerMessage``，批量路径不还原。）
     */
    public function send(Message|array $msg, ?int $timeoutMillis = null, ?MessageQueue $mq = null): SendResult
    {
        // 批量直入 _sendBatch（Python：isinstance(list, tuple) 分支，不还原）
        if (is_array($msg)) {
            return $this->sendBatch($msg, $mq, $timeoutMillis ?? $this->sendMsgTimeout);
        }
        // 自动攒批分流（Java DefaultMQProducer.send(Message):472-478）。两个不分流的情况
        // 与 Java 一一对应：显式给了 timeout 的重载直接进 impl；
        // 批量消息（MessageBatch）被 `!(msg instanceof MessageBatch)` 挡住 —— 那正是
        // accumulator 自己发出去的东西，不挡就会无限递归。
        if ($timeoutMillis === null
            && $this->getAutoBatch()
            && !($msg instanceof MessageBatch)) {
            return $this->sendByAccumulator($msg, $mq, null);
        }
        $prevBody = (string) $msg->getBody();
        try {
            return $this->sendSingle($msg, $timeoutMillis, $mq);
        } finally {
            // Java ``sendKernelImpl:1095-1096`` 的 finally：成功/失败/钩子异常都还原
            $this->restoreCallerMessage($msg, $prevBody);
        }
    }

    /** ``send()`` 的单条主体（对应 Python send 装饰器内的函数体）。 */
    private function sendSingle(Message $msg, ?int $timeoutMillis, ?MessageQueue $mq): SendResult
    {
        $client = $this->requireClient();
        $timeout = $timeoutMillis ?? $this->sendMsgTimeout;
        $msg->setTopic($this->withNamespace($msg->getTopic()));
        $this->checkMessage($msg);
        if ($mq !== null) {
            // Java 的顺序：Validators → 定点守卫 → sendKernelImpl（压缩在核里）→ 超时复检。
            // 排在压缩之前，拒绝时调用方那条消息连次数都不用还原。
            $this->checkPinnedTopic($msg->getTopic(), $mq, self::PINNED_TOPIC_MISMATCH_SYNC);
        }
        // 在重试循环**之外**压缩一次：Java 是在循环内调用 tryToCompressMessage 的，
        // 而它会就地 setBody，重试时会把已压缩的 body 再压一遍（zlib(zlib(x))），
        // 消费端只解一层就拿到压缩流。这里避免该问题。
        $sysFlag = $this->tryToCompressMessage($msg);
        if ($mq !== null) {
            // 定点发送同样要过钩子（Java：目标是 mq 也走 sendKernelImpl）
            if ($this->hasSendInterceptors()) {
                return $this->sendWithHooks($client, $msg, $mq, $timeout, $sysFlag);
            }
            return $client->sendMessage(
                $this->producerGroup,
                $msg,
                $mq,
                $timeout,
                $sysFlag,
                ...$this->sendHeaderArgs()
            );
        }

        // 对应 Java sendDefaultImpl：重试分类逐异常类型走，不用"啥都重试"糊过去。
        try {
            $publish = $this->topicPublishInfo($msg->getTopic());
        } catch (MQClientException $e) {
            // Java：路由拿不到时立刻按 NOT_FOUND_TOPIC_EXCEPTION 定性，不把重试次数空转掉。
            // 已经带码的（10004「没有 name server」，在 topicPublishInfo 里判的）原样透传，
            // 别把它改写成 10005 —— 那是两种故障，覆盖掉就白判了。
            throw new MQClientException(
                $e->getMessage(),
                $e->responseCode ?: ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
                $e->getPrevious()
            );
        }
        $timesTotal = $this->retryTimesWhenSendFailed + 1;
        $beginFirst = $this->monoMillis();
        $brokersSent = [];
        $lastBrokerName = null;
        $result = null;
        $lastExc = null;
        $callTimeout = false;
        for ($attempt = 0; $attempt < $timesTotal; $attempt++) {
            $selected = null;
            $began = $this->monoMillis();
            try {
                // 故障规避：开启时按 broker 延迟/隔离状态选队列（Java MQFaultStrategy）；
                // 关闭时退化为普通轮询（策略内部判断）。重试时 resetIndex 让轮询从头开始，
                // 从而能避开 lastBrokerName 选到别的 broker。
                $selected = $this->mqFaultStrategy->selectOneMessageQueue($publish, $lastBrokerName, $attempt > 0);
                if ($selected === null) {
                    break;
                }
                $lastBrokerName = $selected->brokerName;
                $brokersSent[] = $selected->brokerName;
                $mqSel = new MessageQueue($msg->getTopic(), $selected->brokerName, $selected->queueId);
                $began = $this->monoMillis();
                $costTime = (int) ($began - $beginFirst);
                if ($timeout < $costTime) {
                    $callTimeout = true;
                    break;
                }
                $curTimeout = $timeout - $costTime;
                $canRetryAgain = $attempt + 1 < $timesTotal;
                if ($this->sendMsgMaxTimeoutPerRequest > -1 && $canRetryAgain
                    && $curTimeout > $this->sendMsgMaxTimeoutPerRequest) {
                    $curTimeout = $this->sendMsgMaxTimeoutPerRequest;
                }
                $sendStart = $this->metrics->recordSendStart();
                try {
                    $result = $this->sendWithHooks($client, $msg, $mqSel, $curTimeout, $sysFlag);
                } catch (\Throwable $e) {
                    // 指标记账后按原异常分类处理
                    $this->metrics->recordSendFailure($sendStart);
                    throw $e;
                }
                $this->metrics->recordSendSuccess($sendStart);
                // 记录发送延迟；超出阈值会把该 broker 隔离一段时间
                $this->updateFaultItem($selected, $began, false, true);
                // Java：非 SEND_OK 且开了 retryAnotherBrokerWhenNotStoreOK 才换 broker，
                // 否则把这个"存了但没存好"的结果原样返回
                if ($result !== null && $result->sendStatus !== SendStatus::SEND_OK
                    && $this->retryAnotherBrokerWhenNotStoreOk) {
                    continue;
                }
                return $result;
            } catch (MQBrokerException $e) {
                // broker 明确回了错误码：隔离该 broker（可达性不动），只有可重试码才换一台
                $this->updateFaultItem($selected, $began, true, false);
                $lastExc = $e;
                if ($this->isRetryResponseCode($e->responseCode)) {
                    continue;
                }
                if ($result !== null) {
                    return $result;
                }
                throw $e;
            } catch (RemotingException $e) {
                // 连不上/超时/发不出去：隔离该 broker。本项目无后台可达性探测任务，
                // 所以 Java 的 reachable = !isStartDetectorEnable() 恒为 True。
                $this->updateFaultItem($selected, $began, true, true);
                $lastExc = $e;
            } catch (MQClientException $e) {
                // 客户端自己的问题（选不到队列、路由没了…）：Java 同样只记延迟、不隔离
                $this->updateFaultItem($selected, $began, false, true);
                $lastExc = $e;
            }
        }

        if ($result !== null) {
            return $result;
        }
        if ($callTimeout) {
            throw new RemotingTooMuchRequestException('sendDefaultImpl call timeout');
        }
        $info = sprintf(
            'Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: [%s], last error: %s',
            count($brokersSent),
            (int) ($this->monoMillis() - $beginFirst),
            $msg->getTopic(),
            implode(', ', $brokersSent),
            $lastExc !== null ? $lastExc->getMessage() : ''
        );
        $code = null;
        if ($lastExc instanceof MQBrokerException) {
            $code = $lastExc->responseCode;
        } elseif ($lastExc instanceof RemotingConnectException) {
            $code = ClientErrorCode::CONNECT_BROKER_EXCEPTION;
        } elseif ($lastExc instanceof RemotingTimeoutException) {
            $code = ClientErrorCode::ACCESS_BROKER_TIMEOUT;
        } elseif ($lastExc instanceof MQClientException) {
            $code = ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION;
        }
        throw new MQClientException($info, $code, $lastExc);
    }

    /**
     * Request-Reply（5.x）：发一条请求消息并**同步等应答**，返回应答消息。
     *
     * 对应 Java ``DefaultMQProducerImpl#request(msg, mq, timeout)``（:1738-1767）。
     * 请求方做三件事：
     *   1. 给请求消息写上 CORRELATION_ID（随机 UUID）、REPLY_TO_CLIENT（**本客户端 clientId**）、
     *      TTL（= timeout）；后两个是 broker 找回本连接、应答方原样带回的依据。
     *   2. 把等待槽按 correlationId 登记到进程内的 REQUEST_FUTURE_HOLDER。
     *   3. 发送后阻塞等待；应答由 broker 经 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 推回，
     *      由 ``MQClientInstance.processReplyMessage`` 投递进等待槽。
     *
     * 超时抛 ``RequestTimeoutException``（消息已发出但没等到应答）；
     * 发送本身失败则抛 ``MQClientException``（带着底层 cause），与 Java 一致。
     *
     * ``REPLY_TO_CLIENT`` 是 clientId —— broker 要靠它反查 channel，
     * 所以本生产者必须先发过心跳（这里会补一次，对齐 Java
     * ``prepareSendRequest`` 的 ``sendHeartbeatToAllBrokerWithLock``）。
     */
    public function request(Message $msg, ?int $timeoutMillis = null, ?MessageQueue $mq = null): Message
    {
        $timeout = $timeoutMillis ?? $this->requestTimeout;
        $msg->setTopic($this->withNamespace($msg->getTopic()));
        $this->checkMessage($msg);

        $correlationId = CorrelationIdUtil::createCorrelationId();
        $client = $this->requireClient();
        $msg->putProperty(MessageConst::PROPERTY_CORRELATION_ID, $correlationId);
        $msg->putProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT, $client->clientId);
        $msg->putProperty(MessageConst::PROPERTY_MESSAGE_TTL, (string) $timeout);

        $begin = $this->wallMillis();
        // 对齐 Java prepareSendRequest：确保路由已知，然后补一次心跳 ——
        // 没在 broker 上登记为 producer，broker 就找不到 channel 把应答推回来。
        try {
            $this->topicPublishInfo($msg->getTopic());
            $this->sendHeartbeatToAllBroker();
        } catch (\Throwable) {
            // 拿不到路由就让下面的发送路径自己报错
            Logger::debug('request: prepare route/heartbeat failed');
        }

        $future = new RequestResponseFuture($correlationId, $timeout);
        RequestFutureHolder::getInstance()->putRequest($correlationId, $future);
        $cost = (int) ($this->wallMillis() - $begin);
        try {
            // Java 这里也是 ASYNC + 等 latch：send_async 把发送丢进 AsyncSenderExecutor
            // 立即返回，本线程只等 326 推回来的应答。
            // 协议上无差别 —— 应答是 broker 通过**另一条** 326 通道推回来的，
            // 与本次发送的 CommunicationMode 无关。
            // 发送失败时回调会把 future 标成 !send_request_ok 并主动唤醒等待方，
            // 所以发送失败不会等满 timeout。
            $this->sendAsync($msg, new NullSendCallback($future), $timeout > $cost ? $timeout - $cost : $timeout, $mq);
            return $this->waitRequestResponse($msg, $timeout, $future, $cost);
        } finally {
            RequestFutureHolder::getInstance()->removeRequest($correlationId);
        }
    }

    /** 对应 Java ``waitResponse``：超时/发送失败分别抛不同异常。 */
    private function waitRequestResponse(Message $msg, int $timeout, RequestResponseFuture $future, int $cost): Message
    {
        // PHP 单线程：future 的 waitResponseMessage 不阻塞（返回当前已到达的应答），
        // 真实的等待由这里泵 remoting 层（应答经 326 推回）直到到期。
        $deadline = $this->wallMillis() + max(0, $timeout - $cost);
        while (true) {
            $response = $future->waitResponseMessage(0);
            if ($response !== null) {
                return $response;
            }
            $now = $this->wallMillis();
            if ($now >= $deadline) {
                break;
            }
            $client = $this->mqClient;
            if ($client !== null) {
                // 必须用 pumpIncoming 而不是 waitResponses：等待期发送早已完成、
                // pending 已清空，waitResponses 只盯有在途请求的连接会直接返回，
                // 326 推回的应答压在内核缓冲区里永远读不到（真机实锤，见
                // RemotingClient::pumpIncoming 注释）。
                $client->remotingClient->pumpIncoming(min(20, $deadline - $now));
            }
            if ($this->wallMillis() < $deadline) {
                usleep(1000);
            }
        }
        $response = $future->waitResponseMessage(0);
        if ($response !== null) {
            return $response;
        }
        if ($future->sendRequestOk) {
            // Java ``RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, ...)``：
            // 光有类型和文案不够，10006 这个码也要带上 —— 调用方按码分流时
            // "没等到应答"（可能对方只是慢，消息其实已投出去）和别的客户端故障不是一类处置。
            throw new RequestTimeoutException(
                sprintf(
                    'send request message to <%s> OK, but wait reply message timeout, %d ms.',
                    $msg->getTopic(),
                    $timeout
                ),
                ClientErrorCode::REQUEST_TIMEOUT_EXCEPTION
            );
        }
        throw new MQClientException(
            sprintf('send request message to <%s> fail', $msg->getTopic()),
            null,
            $future->cause
        );
    }

    /**
     * Request-Reply 应答侧：由收到的请求消息派生应答并**同步发送**。
     *
     * 对应 nodeJs ``producer.reply``（Java 范式 = ``MessageUtil.createReplyMessage``
     * + ``producer.send``）。createReplyMessage 取请求里的 CLUSTER 派生
     * topic=<CLUSTER>_REPLY_TOPIC、回填 CORRELATION_ID / REPLY_TO_CLIENT / TTL，
     * 并打上 MSG_TYPE="reply" —— 发送路径据此换码 SEND_REPLY_MESSAGE_V2(325)，
     * broker 才会把应答按 REPLY_TO_CLIENT 推回请求方（326）。
     *
     * 注意：请求消息必须来自 broker 投递（带 CLUSTER 属性）；对手工 new 的
     * Message 直接调用会抛 MQClientException(CREATE_REPLY_MESSAGE_EXCEPTION)。
     */
    public function reply(Message $requestMessage, string $body, ?int $timeoutMillis = null): SendResult
    {
        return $this->send(MessageUtil::createReplyMessage($requestMessage, $body), $timeoutMillis);
    }

    /**
     * 建异步发送链的两个池（对应 Java DefaultMQProducerImpl:133-140 与
     * NettyRemotingClient:152-157）：
     *
     * * ``AsyncSenderExecutor_N`` —— 把 send_async 的准备工作（拉路由、选队列、建请求、
     *   换 broker 重试）从调用方线程挪走；队列**有界**，满了抛 "executor rejected"。
     * * ``NettyClientPublicExecutor_N`` —— 用户回调与 ``SendMessageHook.after`` 在这里跑。
     *
     * 两个池都是 core==max、keepAlive 60s（Java 同款）。PHP 无线程：池由 ``pumpAsyncSend``
     * 显式驱动。
     */
    private function createAsyncExecutors(): void
    {
        $cores = self::cpuCount();
        $this->asyncSenderExecutor = new ConsumeExecutor(
            $cores,
            $cores,
            60.0,
            'AsyncSenderExecutor',
            $this->asyncSenderQueueCapacity,
            '_',
            1
        );
        $callbackThreads = $this->clientCallbackExecutorThreads;
        if ($callbackThreads <= 0) {
            // Java：默认 availableProcessors；显式配成 <=0 时 NettyRemotingClient 兜到 4
            $callbackThreads = $cores;
        }
        $this->callbackExecutor = new ConsumeExecutor(
            $callbackThreads,
            $callbackThreads,
            60.0,
            'NettyClientPublicExecutor',
            0,
            '_',
            1
        );
    }

    /**
     * os.cpu_count() 的 PHP 替身：PHP 没有暴露核数，默认按 4 兜（可用环境变量
     * RMQ_CPU_CORES 覆盖）。只影响并发逻辑 worker 的容量观感，不影响语义。
     */
    private static function cpuCount(): int
    {
        $raw = getenv('RMQ_CPU_CORES');
        if ($raw !== false && (int) $raw > 0) {
            return (int) $raw;
        }
        return 4;
    }

    /**
     * 异步发送（对应 Java ``DefaultMQProducerImpl.send(msg, SendCallback, timeout)``）。
     *
     * **调用方立即返回**的语义在 PHP 里以「注册回调后内部泵到完成」呈现（见文件头注释），
     * 四段与 Java 逐段对齐：
     *
     * 1. 任务投到 ``AsyncSenderExecutor_N``（池大小 = CPU 核数、队列有界
     *    ``asyncSenderQueueCapacity``，默认 50000，同 Java 的 ``LinkedBlockingQueue(50000)``）。
     *    队列满了 Java 抛 ``MQClientException("executor rejected")``，这里同样**抛给调用方**
     *    而不是走回调 —— 只有开了背压时才改为就地跑（见 ``executeAsyncMessageSend``）。
     * 2. 出队之后才算真实耗时：预算被排队吃掉就直接回调
     *    ``RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout")``，不再发请求。
     * 3. ``sendKernelImpl`` 的 ASYNC 分支：拦截钩子 → 建请求 → ``invokeAsync``。
     * 4. 失败进 ``onSendException``（Java ``onExceptionImpl``）：换一台 broker 的队列、给
     *    **同一个请求**换新 opaque 再试，上限 ``retryTimesWhenSendAsyncFailed``；
     *    超时预算是所有尝试**共享**的剩余时间，不是每次尝试各给一份。
     *
     * 用户回调和 ``SendMessageHook.after`` 跑在 ``NettyClientPublicExecutor_N`` 上（Java 的
     * ``NettyRemotingAbstract.executeInvokeCallback`` 就是把回调 submit 到 publicExecutor，
     * 为的是不让业务代码占着连接读线程）。
     *
     * 开了 ``enableBackpressureForAsyncMode`` 之后，投队列**之前**还要过一道闸
     * （``executeAsyncMessageSend``，Java ``executeAsyncMessageSend``）：按条数和字节数
     * 两个维度各拿一份许可，拿不到就直接回调 ``RemotingTooMuchRequestException``。注意这道闸
     * **在调用方线程上等**，所以「异步」在背压打满时会退化成「等满 timeout 再报错」。
     *
     * 与 Java 的两处有意差别：未 start 时**同步抛**（Java 走回调，那样问题更难查）；
     * 批量消息复用同步批量内核（只是不阻塞调用方，见 ``sendAsyncInner``）。
     */
    public function sendAsync(
        Message|array $msg,
        SendCallback|InternalSendCallback $callback,
        ?int $timeoutMillis = null,
        ?MessageQueue $mq = null,
    ): void {
        // 自动攒批分流（Java DefaultMQProducer.send(Message, SendCallback):517-527）：
        // 整段包在 try 里，异常统一转给回调。
        if ($timeoutMillis === null && $this->getAutoBatch()
            && !is_array($msg) && !($msg instanceof MessageBatch)) {
            try {
                $this->sendByAccumulator($msg, $mq, $callback);
            } catch (\Throwable $e) {
                // Java：catch (Throwable) → onException
                $callback->onException($e);
            }
            return;
        }
        $this->requireClient();
        $executor = $this->asyncSenderExecutor;
        if ($executor === null) {
            throw new MQClientException('producer already shutdown');
        }
        $timeout = $timeoutMillis ?? $this->sendMsgTimeout;
        $begin = $this->monoMillis();
        // Java ``:555`` —— 进链的是包了一层的新回调：链的终点先归还许可，再转交用户
        $gated = new BackPressureSendCallback(
            $callback,
            $this->semaphoreAsyncSendNum,
            $this->semaphoreAsyncSendSize,
            self::backPressureMsgLen($msg)
        );

        $run = function () use ($msg, $mq, $gated, $timeout, $begin): void {
            $cost = (int) ($this->monoMillis() - $begin);
            if ($timeout <= $cost) {
                $this->complete(
                    $gated,
                    null,
                    new RemotingTooMuchRequestException('DEFAULT ASYNC send call timeout'),
                    null
                );
                return;
            }
            try {
                $this->sendAsyncInner($msg, $mq, $gated, $timeout - $cost);
            } catch (\Throwable $e) {
                // Java：runnable 的 catch → newCallBack.onException(e)
                $this->complete($gated, null, $e, null);
            }
        };

        $this->executeAsyncMessageSend($run, $gated, $timeout, $begin, $executor);
        // PHP 单线程适配（PORTING.md）：Python 注册回调后由后台线程收响应；
        // 这里在内部泵到回调链走完（对调用方呈现相同的回调时序）。
        $this->pumpAsyncSend($gated, $timeout);
    }

    /**
     * 对应 Java ``DefaultMQProducerImpl.executeAsyncMessageSend:635-682``。
     *
     * 两个许可**顺序**申请、都用「从 ``begin`` 算起的剩余预算」去等，所以第一个闸就能把
     * 预算花光；哪个没拿到就回调哪个，消息文案也与 Java 逐字一致。已经拿到手的由
     * ``gated`` 在回调里原样归还（Java 靠 ``isSemaphoreAsyncNumAcquired`` /
     * ``isSemaphoreAsyncSizeAcquired`` 两个标记做到这一点）。
     */
    private function executeAsyncMessageSend(
        \Closure $runnable,
        BackPressureSendCallback $gated,
        int $timeout,
        float $begin,
        ConsumeExecutor $executor,
    ): void {
        if ($this->enableBackpressureForAsyncMode) {
            $cost = (int) ($this->monoMillis() - $begin);
            $gated->numAcquired = $timeout - $cost > 0
                && $this->semaphoreAsyncSendNum->tryAcquire(1, $timeout - $cost);
            if (!$gated->numAcquired) {
                $this->complete($gated, null, new RemotingTooMuchRequestException(
                    'send message tryAcquire semaphoreAsyncNum timeout'
                ), null);
                return;
            }

            $cost = (int) ($this->monoMillis() - $begin);
            $gated->sizeAcquired = $timeout - $cost > 0
                && $this->semaphoreAsyncSendSize->tryAcquire($gated->msgLen, $timeout - $cost);
            if (!$gated->sizeAcquired) {
                $this->complete($gated, null, new RemotingTooMuchRequestException(
                    'send message tryAcquire semaphoreAsyncSize timeout'
                ), null);
                return;
            }
        }

        try {
            $executor->submit($runnable);
        } catch (RejectedExecutionError $e) {
            if ($this->enableBackpressureForAsyncMode) {
                // Java ``:675-681``：许可已经扣掉了，就地跑完这一笔（**阻塞调用方**），
                // 好让回调把许可还回来；否则队列一满就直接抛，白扣的容量还得等超时。
                $runnable();
            } else {
                throw new MQClientException('executor rejected', null, $e);
            }
        }
    }

    /**
     * 泵异步发送链到回调走完（PHP 单线程替代 Python 的后台线程收响应）：
     * 每轮依次执行 AsyncSenderExecutor 的排队任务、remoting 层 waitResponses、
     * 回调执行器的排队任务，直到 ``gated`` 标记完成或超出兜底期限
     * （timeout + 10s；正常情况下底层超时早就把回调打完）。
     */
    private function pumpAsyncSend(BackPressureSendCallback $gated, int $timeout): void
    {
        $deadline = $this->monoMillis() + $timeout + 10000;
        while (!$gated->done) {
            if ($this->monoMillis() >= $deadline) {
                break; // 兜底：底层超时理应已经触发回调
            }
            if ($this->asyncSenderExecutor !== null) {
                $this->asyncSenderExecutor->pumpAll();
            }
            if ($this->mqClient !== null) {
                $this->mqClient->remotingClient->waitResponses(20);
            }
            if ($this->callbackExecutor !== null) {
                $this->callbackExecutor->pumpAll();
            }
            if (!$gated->done) {
                usleep(1000);
            }
        }
    }

    /**
     * 出队后的准备工作（Java ``sendDefaultImpl(ASYNC)`` → ``sendKernelImpl``）。
     *
     * 还原放在这一层而不是 ``sendAsync``：Java 5.5.1 的异步链把**调用方那条消息**
     * 交给 ``AsyncSenderExecutor`` 的 runnable，`finally` 是在**工作线程**上跑的
     * （调用方从 ``send_async`` 返回时消息还是压缩后的样子，发完才还原），
     * 还原跟到工作层才等价。
     */
    private function sendAsyncInner(
        Message|array $msg,
        ?MessageQueue $mq,
        SendCallback|InternalSendCallback $callback,
        int $timeout,
    ): void {
        if (is_array($msg)) {
            // 批量异步：Java 走 SEND_BATCH_MESSAGE + invokeAsync，本实现的批量发送只有同步内核，
            // 所以这里是「在 AsyncSenderExecutor 里同步发一批」。对调用方语义没差别 ——
            // 不阻塞发送方、回调照样在 callbackExecutor 上跑。
            // 定点时带上异步那处文案（Java ``:1277-1278``）；没有队列就无从比较，
            // 沿用内核默认的三参调用（同步文案也用不上）。
            // （批量路径不做还原，与 Python 装饰器的 isinstance 分支一致。）
            if ($mq === null) {
                $result = $this->sendBatch($msg, $mq, $timeout);
            } else {
                $result = $this->sendBatch($msg, $mq, $timeout, self::PINNED_TOPIC_MISMATCH_ASYNC);
            }
            $this->executeOnCallbackThread(function () use ($callback, $result): void {
                $this->complete($callback, $result, null, null);
            });
            return;
        }
        $prevBody = (string) $msg->getBody();
        try {
            $this->sendAsyncInnerSingle($msg, $mq, $callback, $timeout);
        } finally {
            $this->restoreCallerMessage($msg, $prevBody);
        }
    }

    private function sendAsyncInnerSingle(
        Message $msg,
        ?MessageQueue $mq,
        SendCallback|InternalSendCallback $callback,
        int $timeout,
    ): void {
        $client = $this->requireClient();
        $msg->setTopic($this->withNamespace($msg->getTopic()));
        $this->checkMessage($msg);
        if ($mq !== null) {
            // Java ``:1277-1278``：异步分支在同一位置用另一处文案抛，异常由 runnable 的
            // catch 转给 ``newCallBack.onException``（这里由 ``sendAsync`` 的 `$run` 收口）。
            $this->checkPinnedTopic($msg->getTopic(), $mq, self::PINNED_TOPIC_MISMATCH_ASYNC);
        }
        // 与同步发送同理：压缩在重试链之外做一次，避免重试时把已压缩的 body 再压一遍
        $sysFlag = $this->tryToCompressMessage($msg);
        if ($mq !== null) {
            // Java send(msg, mq, cb, timeout) → sendKernelImpl 直接定点发，传下去的
            // topicPublishInfo 是 null，所以失败只会在**同一台 broker** 上换 opaque 重试。
            $this->sendKernelAsync($client, $msg, $mq, $callback, $timeout, $sysFlag, null);
            return;
        }
        try {
            $publish = $this->topicPublishInfo($msg->getTopic());
        } catch (MQClientException $e) {
            // 同同步发送：10004 原样透传，其余按 10005 定性
            throw new MQClientException(
                $e->getMessage(),
                $e->responseCode ?: ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
                $e->getPrevious()
            );
        }
        // Java sendDefaultImpl:756 —— ASYNC 的 timesTotal 固定为 1：外层循环只跑一次，
        // 换 broker 的重试全部发生在 onExceptionImpl 里。
        $selected = $this->mqFaultStrategy->selectOneMessageQueue($publish, null, false);
        if ($selected === null) {
            throw new MQClientException(sprintf(
                'Send [0] times, still failed, Topic: %s, BrokersSent: []',
                $msg->getTopic()
            ));
        }
        $this->sendKernelAsync(
            $client,
            $msg,
            new MessageQueue($msg->getTopic(), $selected->brokerName, $selected->queueId),
            $callback,
            $timeout,
            $sysFlag,
            $publish
        );
    }

    /** Java ``sendKernelImpl`` 的 ASYNC 分支：钩子 + 建请求 + 交给 sendMessageAsync。 */
    private function sendKernelAsync(
        MQClientInstance $client,
        Message $msg,
        MessageQueue $mq,
        SendCallback|InternalSendCallback $callback,
        int $timeout,
        int $sysFlag,
        ?TopicPublishInfo $publish,
    ): void {
        $began = $this->monoMillis();
        // 地址解析两步，与 Java ``sendKernelImpl:919-924`` 一致：先查已缓存的**发布**地址
        // （只认 master），查不到再按 topic 刷一次路由重查，仍查不到照 ``:1100`` 报
        // 「The broker[X] not exist」。定点发送（调用方给了 mq）不会在 sendDefaultImpl
        // 里取发布信息，这一步是它唯一的路由来源 —— 少了这一步，第一次定点发送必然拿到空地址。
        $brokerAddr = $client->publishAddrFor($mq->brokerName, $mq->topic);
        if ($this->hasCheckForbiddenHook()) {
            $this->executeCheckForbidden($msg, $mq, $brokerAddr, null, CommunicationMode::ASYNC);
        }
        if ($this->enableTraceContext) {
            TraceContextPropagator::injectTraceContext($msg);
        }
        // 请求只建一次：跨重试复用同一个对象（Java onExceptionImpl 只换 opaque），
        // 所以 header 里的 queueId 也跟着上一次 —— 这是 Java 的真实行为，别"修"它。
        $request = $client->buildSendRequest(
            $this->producerGroup,
            $msg,
            $mq,
            $timeout,
            $sysFlag,
            ...$this->sendHeaderArgs()
        );
        $context = null;
        if ($this->sendMessageHookList !== []) {
            $context = $this->buildSendContext($msg, $mq, $brokerAddr, CommunicationMode::ASYNC);
            $this->executeSendMessageHookBefore($context);
        }
        // Java ``sendKernelImpl:1043-1046``：ASYNC 分支自己的总闸 —— 钩子、压缩、路由
        // 都算耗时，预算被它们吃光就不再发起请求。RemotingTooMuchRequestException 是
        // RemotingException 的子类，所以 Java 在 :1088 先跑 hook.after 再抛给回调，
        // 这里用 complete 复刻同一顺序（且**不重试**）。
        $costAsync = (int) ($this->monoMillis() - $began);
        if ($timeout < $costAsync) {
            $this->complete($callback, null, new RemotingTooMuchRequestException('sendKernelImpl call timeout'), $context);
            return;
        }
        $this->sendMessageAsync(
            $client,
            $brokerAddr,
            $mq->brokerName,
            $mq,
            $msg,
            $request,
            $timeout - $costAsync,
            $callback,
            $publish,
            $context,
            new AsyncSendAttempt()
        );
    }

    /** 一笔在途尝试（对应 Java ``MQClientAPIImpl#sendMessageAsync``）。 */
    private function sendMessageAsync(
        MQClientInstance $client,
        string $addr,
        string $brokerName,
        MessageQueue $mq,
        Message $msg,
        RemotingCommand $request,
        int $timeout,
        SendCallback|InternalSendCallback $callback,
        ?TopicPublishInfo $publish,
        ?SendMessageContext $context,
        AsyncSendAttempt $times,
    ): void {
        $began = $this->monoMillis();

        $handle = function (?SendResult $result, ?\Throwable $error) use (
            $client,
            $brokerName,
            $mq,
            $msg,
            $request,
            $timeout,
            $callback,
            $publish,
            $context,
            $times,
            $began
        ): void {
            $cost = (int) ($this->monoMillis() - $began);
            if ($error === null) {
                $this->updateFaultItem($mq, $began, false, true);
                $this->complete($callback, $result, null, $context);
                return;
            }
            $this->updateFaultItem($mq, $began, true, true);
            [$wrapped, $needRetry] = self::classifyAsyncFailure($error, $cost);
            $this->onSendException(
                $client,
                $brokerName,
                $mq,
                $msg,
                $request,
                $timeout - $cost,
                $callback,
                $publish,
                $context,
                $times,
                $wrapped,
                $needRetry
            );
        };

        $onComplete = function (?SendResult $result, ?\Throwable $error) use ($handle): void {
            $this->executeOnCallbackThread(function () use ($handle, $result, $error): void {
                $handle($result, $error);
            });
        };

        try {
            $client->sendMessageAsync($addr, $request, $msg, $mq, $timeout, $onComplete);
        } catch (\Throwable $e) {
            // Java sendMessageAsync 的外层 catch：就地失败（连不上/通道没了），
            // 异常**原样**传递（不包装）、needRetry=true。
            $cost = (int) ($this->monoMillis() - $began);
            $this->updateFaultItem($mq, $began, true, false);
            $this->onSendException(
                $client,
                $brokerName,
                $mq,
                $msg,
                $request,
                $timeout - $cost,
                $callback,
                $publish,
                $context,
                $times,
                $e,
                true
            );
        }
    }

    /** 对应 Java ``MQClientAPIImpl#onExceptionImpl``：还能试就换队列重试，否则终止。 */
    private function onSendException(
        MQClientInstance $client,
        string $brokerName,
        MessageQueue $mq,
        Message $msg,
        RemotingCommand $request,
        int $timeout,
        SendCallback|InternalSendCallback $callback,
        ?TopicPublishInfo $publish,
        ?SendMessageContext $context,
        AsyncSendAttempt $times,
        \Throwable $error,
        bool $needRetry,
    ): void {
        $times->times++;
        $attempt = $times->times;
        if ($needRetry && $attempt <= $this->retryTimesWhenSendAsyncFailed && $timeout > 0) {
            $retryBroker = $brokerName;
            $retryMq = $mq;
            if ($publish !== null) {
                // Java: producer.selectOneMessageQueue(topicPublishInfo, brokerName, false)
                // —— 第三个参数是 false，所以按 lastBrokerName 避开刚失败的那台
                $selected = $this->mqFaultStrategy->selectOneMessageQueue($publish, $brokerName, false);
                if ($selected !== null) {
                    $retryMq = new MessageQueue($msg->getTopic(), $selected->brokerName, $selected->queueId);
                    $retryBroker = $selected->brokerName;
                }
            }
            $addr = $client->findBrokerAddressInPublish($retryBroker);
            if ($addr === null || $addr === '') {
                // Java onExceptionImpl:725 只查发布地址表（同样只认 master）、不刷路由；
                // 查不到就带着 null 撞进 invokeAsync。这里就地终止，别让 null 传进传输层。
                $this->complete(
                    $callback,
                    null,
                    new MQClientException(sprintf('The broker[%s] not exist', $retryBroker)),
                    $context
                );
                return;
            }
            Logger::warning(sprintf(
                'async send msg by retry %d times. topic=%s, brokerAddr=%s, brokerName=%s: %s',
                $attempt,
                $msg->getTopic(),
                $addr,
                $retryBroker,
                $error->getMessage()
            ));
            // 换新 opaque：旧请求还挂在 responseTable 里等超时，复用会把两次尝试的应答串台
            $request->opaque = RemotingCommand::createNewRequestId();
            $this->sendMessageAsync(
                $client,
                $addr,
                $retryBroker,
                $retryMq,
                $msg,
                $request,
                $timeout,
                $callback,
                $publish,
                $context,
                $times
            );
            return;
        }
        $this->complete($callback, null, $error, $context);
    }

    /**
     * 在 ``NettyClientPublicExecutor_N`` 上跑回调处理（Java executeInvokeCallback 的
     * publicExecutor 分支）；池子已关或投不进去时**就地跑**，与 Java 的 runInThisThread 一致。
     */
    private function executeOnCallbackThread(\Closure $fn): void
    {
        $pool = $this->callbackExecutor;
        if ($pool === null) {
            $fn();
            return;
        }
        try {
            $pool->submit($fn);
        } catch (RejectedExecutionError) {
            $fn();
        }
    }

    /**
     * 异步链的终点：先跑 SendMessageHook.after，再回调用户。
     *
     * 用户回调抛的异常必须吞掉（Java 两处都是 ``catch (Throwable)``），否则它会带走
     * 回调线程池的 worker。
     */
    private function complete(
        SendCallback|InternalSendCallback $callback,
        ?SendResult $result,
        ?\Throwable $error,
        ?SendMessageContext $context,
    ): void {
        if ($context !== null) {
            if ($error === null) {
                $context->sendResult = $result;
            } else {
                $context->exception = $error;
            }
            $this->executeSendMessageHookAfter($context);
        }
        try {
            if ($error === null) {
                $callback->onSuccess($result);
            } else {
                $callback->onException($error);
            }
        } catch (\Throwable $e) {
            Logger::warning('send callback raised: ' . $e->getMessage());
        }
    }

    /** 单向发送（对应 Java sendOneway）。 */
    public function sendOneway(Message $msg, ?MessageQueue $mq = null): void
    {
        $prevBody = (string) $msg->getBody();
        try {
            $this->sendOnewayInner($msg, $mq);
        } finally {
            $this->restoreCallerMessage($msg, $prevBody);
        }
    }

    private function sendOnewayInner(Message $msg, ?MessageQueue $mq): void
    {
        $client = $this->requireClient();
        $msg->setTopic($this->withNamespace($msg->getTopic()));
        $this->checkMessage($msg);
        $sysFlag = $this->tryToCompressMessage($msg);
        if ($mq !== null) {
            if ($this->hasCheckForbiddenHook()) {
                // Java sendOneway 同样走 sendKernelImpl → 拦截钩子照跑（communicationMode=ONEWAY）
                $this->executeCheckForbidden(
                    $msg,
                    $mq,
                    $this->needAddr($client, $mq),
                    null,
                    CommunicationMode::ONEWAY
                );
            }
            $client->sendMessageOneway(
                $this->producerGroup,
                $msg,
                $mq,
                $this->needAddr($client, $mq),
                $this->sendMsgTimeout,
                $sysFlag,
                ...$this->sendHeaderArgs()
            );
            return;
        }
        $publish = $this->topicPublishInfo($msg->getTopic());
        $selected = $this->mqFaultStrategy->selectOneMessageQueue($publish, null);
        if ($selected === null) {
            throw new MQClientException('no message queue selected for oneway send');
        }
        $mqSel = new MessageQueue($msg->getTopic(), $selected->brokerName, $selected->queueId);
        if ($this->hasCheckForbiddenHook()) {
            $this->executeCheckForbidden(
                $msg,
                $mqSel,
                $this->needAddr($client, $mqSel),
                null,
                CommunicationMode::ONEWAY
            );
        }
        $client->sendMessageOneway(
            $this->producerGroup,
            $msg,
            $mqSel,
            $this->needAddr($client, $mqSel),
            $this->sendMsgTimeout,
            $sysFlag,
            ...$this->sendHeaderArgs()
        );
    }

    /**
     * 使用 MessageQueueSelector 选择队列发送（对应 Java send(msg, selector, arg)）。
     */
    public function sendBySelector(
        Message $msg,
        MessageQueueSelector $selector,
        mixed $arg,
        ?int $timeoutMillis = null,
    ): SendResult {
        $prevBody = (string) $msg->getBody();
        try {
            return $this->sendBySelectorInner($msg, $selector, $arg, $timeoutMillis);
        } finally {
            $this->restoreCallerMessage($msg, $prevBody);
        }
    }

    private function sendBySelectorInner(
        Message $msg,
        MessageQueueSelector $selector,
        mixed $arg,
        ?int $timeoutMillis,
    ): SendResult {
        $client = $this->requireClient();
        $timeout = $timeoutMillis ?? $this->sendMsgTimeout;
        $msg->setTopic($this->withNamespace($msg->getTopic()));
        $publish = $this->topicPublishInfo($msg->getTopic());
        $selected = $selector->select($publish->msgQueueList, $msg, $arg);
        $mqSel = new MessageQueue($msg->getTopic(), $selected->brokerName, $selected->queueId);
        // 选择器用的是原始消息（topic/业务字段），压缩只影响 body
        $this->checkMessage($msg);
        $sysFlag = $this->tryToCompressMessage($msg);
        if ($this->hasSendInterceptors()) {
            // arg 要透传给 CheckForbiddenContext（Java sendKernelImpl 的 context.setArg）
            return $this->sendWithHooks($client, $msg, $mqSel, $timeout, $sysFlag, $arg);
        }
        return $client->sendMessage(
            $this->producerGroup,
            $msg,
            $mqSel,
            $timeout,
            $sysFlag,
            ...$this->sendHeaderArgs()
        );
    }

    // ---------------- 定时消息撤回（对应 Java recallMessage）----------------

    /**
     * 撤回一条定时/延迟消息，返回被撤回消息的 uniqKey。
     *
     * 校验顺序与 Java ``DefaultMQProducerImpl#recallMessage``(:1570-1601) 逐条对齐：
     * 状态 → checkTopic → 禁 retry/DLQ → 解句柄 → 预热路由 → 定位 broker → 发请求。
     * 句柄来自定时消息的 ``SendResult.recallHandle``，普通消息没有。
     *
     * 与 Java 的差异：Java 的 ``findBrokerAddrByTopic`` 返回该 topic 的**全部** broker
     * 地址再随机取一个，这里直接取路由里的第一个可用地址——单 broker 场景等价，
     * 多 broker 场景两者都只会命中句柄里那个 broker 之外的地址，最终由 broker 用
     * ``ILLEGAL_OPERATION``（brokerName 不匹配）拒绝，语义不变。
     */
    public function recallMessage(string $topic, string $recallHandle): string
    {
        $client = $this->requireClient();
        $topic = $this->withNamespace($topic);
        Validators::checkTopic($topic);
        if (MixAll::isRetryTopic($topic) || MixAll::isDlqTopic($topic)) {
            throw new MQClientException('topic is not supported');
        }
        /** @var HandleV1 $handle */
        $handle = RecallMessageHandle::decodeHandle($recallHandle);
        // Java 只是调用 tryToFindTopicPublishInfo 预热路由，返回值并不使用，但**异常照抛**
        // （DefaultMQProducerImpl:1586）—— 连路由都拿不到时，后面的 broker 定位也没有意义。
        $this->topicPublishInfo($topic);
        $addr = $client->findBrokerAddressInPublish($handle->brokerName ?? '');
        if ($addr === null || $addr === '') {
            // Java :1586-1594：发布地址表里没有（主掉了 / 是多 proxy 端点）时退到
            // findBrokerAddrByTopic —— 那条路走 select_broker_addr()，**允许**落到从节点上。
            $addr = $client->findBrokerAddrByTopic($topic);
        }
        if ($addr === null || $addr === '') {
            Logger::warning(sprintf("can't find broker service address. %s", $handle->brokerName));
            throw new MQClientException('The broker service address not found');
        }
        $header = new RecallMessageRequestHeader(
            producerGroup: $this->producerGroup,
            topic: $topic,
            recallHandle: $recallHandle,
            bname: $handle->brokerName,
        );
        return $client->recallMessage($addr, $header, $this->sendMsgTimeout);
    }

    // ---------------- 自动攒批的转发层（对应 Java DefaultMQProducer:434-452/759-793）----------------

    /** Java ``DefaultMQProducer.initProduceAccumulator()``：建/复用累加器并同步阈值。 */
    public function initProduceAccumulator(): void
    {
        $this->produceAccumulator = ProduceAccumulator::getOrCreateProduceAccumulator(
            $this->clientId ?? MixAll::DEFAULT_INSTANCE_NAME,
            fn (MessageBatch $batch, ?MessageQueue $mq, ?InternalSendCallback $callback): ?SendResult =>
                $this->sendDirect($batch, $mq, $callback)
        );
        if ($this->batchMaxDelayMs > -1) {
            $this->produceAccumulator->batchMaxDelayMs($this->batchMaxDelayMs);
        }
        if ($this->batchMaxBytes > -1) {
            $this->produceAccumulator->batchMaxBytes($this->batchMaxBytes);
        }
        if ($this->totalBatchMaxBytes > -1) {
            $this->produceAccumulator->totalBatchMaxBytes($this->totalBatchMaxBytes);
        }
    }

    /**
     * Java ``DefaultMQProducer.canBatch:434-452``。
     *
     * ⚠ 先过全局字节闸门 ``tryAddMessage``：**放行即记账**，而后面四条「不能攒批」
     * 的判断只是让调用方退回直发 —— 那条消息的字节数**不会被归还**（上游遗漏，
     * 照抄；改掉就与 Java 对不上了）。四条排除项：
     *   1. 延时/定时消息（DELAY / TIMER_DELAY_MS / TIMER_DELAY_SEC / TIMER_DELIVER_MS）
     *      任何一个 > 0 都不攒批 —— 一个 MessageBatch 只能有一个延时属性；
     *   2. 重试 topic（``%RETRY%`` 前缀）：重试消息的位点语义特殊；
     *   3. 已带 PGROUP 属性的消息（事务半消息那种）：broker 侧要按组找连接。
     */
    private function canBatch(Message $msg): bool
    {
        if ($this->produceAccumulator === null || !$this->produceAccumulator->tryAddMessage($msg)) {
            return false;
        }
        if (self::maxDelayValue($msg) > 0) {
            return false;
        }
        if (str_starts_with($msg->getTopic(), MixAll::RETRY_GROUP_TOPIC_PREFIX)) {
            return false;
        }
        if ($msg->getProperty(MessageConst::PROPERTY_PRODUCER_GROUP) !== null) {
            return false;
        }
        return true;
    }

    /** Java ``DefaultMQProducer.sendDirect``：绕过累加器直发（同步或异步）。 */
    public function sendDirect(Message $msg, ?MessageQueue $mq, SendCallback|InternalSendCallback|null $sendCallback): ?SendResult
    {
        if ($sendCallback === null) {
            if ($mq === null) {
                return $this->send($msg);
            }
            return $this->send($msg, null, $mq);
        }
        if ($mq === null) {
            $this->sendAsync($msg, $sendCallback);
        } else {
            $this->sendAsync($msg, $sendCallback, null, $mq);
        }
        return null;
    }

    /**
     * Java ``DefaultMQProducer.sendByAccumulator:778-793``。
     *
     * 不能攒批（见 ``canBatch``）→ 退回直发；否则先过 ``Validators.checkMessage``
     * 再给本条消息打 UNIQ_KEY，然后交给累加器（同步版返回本条自己的 SendResult，
     * 异步版立刻返回 null、结果走回调）。
     */
    public function sendByAccumulator(
        Message $msg,
        ?MessageQueue $mq,
        SendCallback|InternalSendCallback|null $sendCallback,
    ): ?SendResult {
        if (!$this->canBatch($msg)) {
            return $this->sendDirect($msg, $mq, $sendCallback);
        }
        Validators::checkMessage($msg, $this->maxMessageSize);
        MessageClientIdSetter::setUniqId($msg);
        if ($this->produceAccumulator === null) {
            throw new MQClientException('produce accumulator is not initialized');
        }
        if ($sendCallback === null) {
            if ($mq === null) {
                return $this->produceAccumulator->send($msg);
            }
            return $this->produceAccumulator->sendWithMq($msg, $mq);
        }
        $wrapped = function (?SendResult $result, ?\Throwable $error) use ($sendCallback): void {
            if ($error !== null) {
                $sendCallback->onException($error);
            } else {
                $sendCallback->onSuccess($result);
            }
        };
        if ($mq === null) {
            $this->produceAccumulator->sendAsync($msg, $wrapped);
        } else {
            $this->produceAccumulator->sendAsyncWithMq($msg, $mq, $wrapped);
        }
        return null;
    }

    // ---------------- 批量发送 ----------------

    private function sendBatch(
        array $msgs,
        ?MessageQueue $mq = null,
        ?int $timeoutMillis = null,
        string $pinnedGuardMessage = self::PINNED_TOPIC_MISMATCH_SYNC,
    ): SendResult {
        $client = $this->requireClient();
        $timeout = $timeoutMillis ?? $this->sendMsgTimeout;
        if ($msgs === []) {
            throw new MQClientException('message list is empty');
        }
        // 对应 Java DefaultMQProducer.batch():1172-1184：**每条子消息**先 Validators
        // .checkMessage（在拼命名空间之前、用原始 topic），再 MessageClientIDSetter
        // .setUniqID，然后才拼命名空间；批量消息本身也要一个 UNIQ_KEY（broker 判 inner-batch
        // 用得到，SendMessageProcessor:617），最后**编码** —— 顺序错了就会把没有 UNIQ_KEY 的
        // 子消息体写进 body，消费端每条子消息都没有客户端 ID。
        // 少 checkMessage 这一步等于批量路径绕过了所有本地校验——超长/空 body/非法 topic 都能发出去。
        foreach ($msgs as $m) {
            Validators::checkMessage($m, $this->maxMessageSize);
            MessageClientIdSetter::setUniqId($m);
            $m->setTopic($this->withNamespace($m->getTopic()));
        }
        $batch = MessageBatch::generateFromList($msgs);
        MessageClientIdSetter::setUniqId($batch);
        $batch->setBody($batch->encode());
        if ($mq !== null) {
            // Java：`impl.send(batch(msgs), queueWithNamespace(mq), timeout)` 与单条共用同一处
            // 同步守卫（MessageBatch extends Message），异步入口则是另一处文案。
            // ``pinnedGuardMessage`` 由调用方按入口选：同步默认，异步传 ASYNC。
            $this->checkPinnedTopic($batch->getTopic(), $mq, $pinnedGuardMessage);
        }
        // MessageBatch 会被 tryToCompressMessage 直接跳过（返回 0），批量消息永不压缩
        $sysFlag = $this->tryToCompressMessage($batch);
        if ($mq !== null) {
            if ($this->hasSendInterceptors()) {
                return $this->sendWithHooks($client, $batch, $mq, $timeout, $sysFlag);
            }
            return $client->sendMessage(
                $this->producerGroup,
                $batch,
                $mq,
                $timeout,
                $sysFlag,
                ...$this->sendHeaderArgs()
            );
        }
        $publish = $this->topicPublishInfo($batch->getTopic());
        $selected = $publish->selectOneMessageQueue();
        $mqSel = new MessageQueue($batch->getTopic(), $selected->brokerName, $selected->queueId);
        if ($this->hasSendInterceptors()) {
            return $this->sendWithHooks($client, $batch, $mqSel, $timeout, $sysFlag);
        }
        return $client->sendMessage(
            $this->producerGroup,
            $batch,
            $mqSel,
            $timeout,
            $sysFlag,
            ...$this->sendHeaderArgs()
        );
    }

    // ================================================================ 事务消息
    // 对齐 Java DefaultMQProducerImpl.sendMessageInTransaction（L1433-1509）的**两阶段**：
    //   1) 半消息：给 msg 打 TRAN_MSG / PGROUP 属性，发送时 sysFlag 置 TRANSACTION_PREPARED_TYPE；
    //   2) 本地事务：仅 SEND_OK 时执行，结果/异常汇总为 LocalTransactionState；
    //   3) endTransaction：以 END_TRANSACTION(37, oneway) 告知 broker 提交/回滚/未知；
    //   4) 若 UNKNOW（或本地事务没执行成功），broker 会回查 CHECK_TRANSACTION_STATE(39)，
    //      由 handleCheckTransactionState 调 listener.checkLocalTransaction 后再 END_TRANSACTION。

    /** LocalTransactionState -> Java MessageSysFlag 的 commitOrRollback 值。 */
    private static function transactionFlag(LocalTransactionState $state): int
    {
        if ($state === LocalTransactionState::COMMIT_MESSAGE) {
            return MessageSysFlag::TRANSACTION_COMMIT_TYPE;    // 0x2 << 2 = 8
        }
        if ($state === LocalTransactionState::ROLLBACK_MESSAGE) {
            return MessageSysFlag::TRANSACTION_ROLLBACK_TYPE;  // 0x3 << 2 = 12
        }
        return MessageSysFlag::TRANSACTION_NOT_TYPE;           // 0（UNKNOW）
    }

    /**
     * 发送事务消息（对应 Java sendMessageInTransaction）。
     *
     * 与 Java 一致的两阶段语义；返回 TransactionSendResult，其中
     * localTransactionState 是本地事务的最终状态。
     */
    public function sendMessageInTransaction(Message $msg, TransactionListener $listener, mixed $arg = null): TransactionSendResult
    {
        // Java `sendKernelImpl:930` 的 `prevBody`：半消息发完就还原调用方那份 body，
        // 所以随后的 `executeLocalTransaction(msg, arg)` 看到的是**原始 body**（不是压缩流）。
        // 这一层不能用 restoreCallerMessage：那样要等方法结束才还原，
        // listener 与 `endTransaction` 都会拿到压缩后的消息。
        $prevBody = (string) $msg->getBody();
        $msg->setTopic($this->withNamespace($msg->getTopic()));

        // Java ensureNotDelayedForTransactional：事务消息不支持任何形式的延迟投递
        foreach ([MessageConst::PROPERTY_DELAY_TIME_LEVEL, MessageConst::PROPERTY_DELAY_TIME,
            'TIMER_DELAY_MS', 'TIMER_DELAY_SEC', 'TIMER_DELIVER_MS'] as $key) {
            if ($msg->getProperty($key) !== null) {
                throw new MQClientException('Transactional messages do not support delayed delivery', null);
            }
        }

        $client = $this->requireClient();
        $this->checkMessage($msg);

        // 半消息标记（broker 侧据此把消息写入 RMQ_SYS_TRANS_HALF_TOPIC）
        $msg->putProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED, 'true');
        $msg->putProperty(MessageConst::PROPERTY_PRODUCER_GROUP, $this->producerGroup);
        // 回查时按此 listener 回调（broker 通过 PGROUP 属性定位到本生产者）
        $this->transactionListener = $listener;

        $publish = $this->topicPublishInfo($msg->getTopic());
        $selected = $publish->selectOneMessageQueue();
        $mqSel = new MessageQueue($msg->getTopic(), $selected->brokerName, $selected->queueId);

        // 压缩与普通发送一致（Java 的事务发送同样走 sendKernelImpl），
        // 再叠加事务类型位（对应 Java L951-953 检测 TRAN_MSG 后置 TRANSACTION_PREPARED）
        $sysFlag = $this->tryToCompressMessage($msg);
        $sysFlag = MessageSysFlag::resetTransactionValue($sysFlag, MessageSysFlag::TRANSACTION_PREPARED_TYPE);

        try {
            // 事务发送同样走 sendKernelImpl（→ 同样触发发送钩子），
            // 所以开启轨迹后事务消息会先落一条 Pub（Trans_Msg_Half）轨迹
            $sendResult = $this->sendWithHooks($client, $msg, $mqSel, $this->sendMsgTimeout, $sysFlag);
        } catch (\Throwable $e) {
            throw new MQClientException('send message Exception', null, $e);
        } finally {
            // 与 Java 的 finally 同点位（`sendKernelImpl:1095-1096`）：**半消息发完**就还原，
            // 所以下面的 `executeLocalTransaction` 和 `endTransaction` 看到的是原始
            // body + 已剥命名空间的 topic（Java 5.5.1 的 `endTransaction:1543` 用的正是
            // `msg.getTopic()`）。属性不动：UNIQ_KEY 那些 Java 也不还原。
            $msg->setBody($prevBody);
            $msg->setTopic(NamespaceUtil::withoutNamespace($msg->getTopic(), $this->namespace));
        }

        $state = LocalTransactionState::UNKNOW;
        $localException = null;
        if ($sendResult->sendStatus === SendStatus::SEND_OK) {
            if ($sendResult->transactionId !== null) {
                $msg->putProperty('__transactionId__', $sendResult->transactionId);
            }
            $uniq = $msg->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            if ($uniq !== null && $uniq !== '') {
                $msg->setTransactionId($uniq);
            }
            try {
                $ret = $listener->executeLocalTransaction($msg, $arg);
                // Java：返回 null 视为 UNKNOW（PHP 非空返回类型天然覆盖）
                $state = $ret;
            } catch (\Throwable $e) {
                Logger::error('executeLocalTransactionBranch exception, topic=' . $msg->getTopic());
                $localException = $e;
            }
        } elseif (in_array($sendResult->sendStatus, [
            SendStatus::FLUSH_DISK_TIMEOUT,
            SendStatus::FLUSH_SLAVE_TIMEOUT,
            SendStatus::SLAVE_NOT_AVAILABLE,
        ], true)) {
            $state = LocalTransactionState::ROLLBACK_MESSAGE;
        }

        try {
            $this->endTransaction($sendResult, $msg, $state, $localException, false);
        } catch (\Throwable $e) {
            // Java：end broker transaction 失败只 warn，不影响返回结果
            Logger::warning(sprintf(
                'local transaction execute %s, but end broker transaction failed: %s',
                $state->name,
                $e->getMessage()
            ));
        }

        return new TransactionSendResult($sendResult, $state);
    }

    /**
     * 向 broker 发送 END_TRANSACTION(37, oneway)，对齐 Java endTransaction + checkTransactionState。
     *
     * - 普通收尾（fromTransactionCheck=false）：偏移/事务号取自 send_result；
     * - 回查收尾（fromTransactionCheck=true）：偏移/事务号取自 broker 的回查 header
     *   （send_result/mq 此时不可用），msgId 取 message_ext 的 UNIQ_KEY。
     */
    private function endTransaction(
        ?SendResult $sendResult,
        ?Message $msg,
        LocalTransactionState $state,
        ?\Throwable $localException,
        bool $fromTransactionCheck,
        ?CheckTransactionStateRequestHeader $checkHeader = null,
        ?MessageExt $msgExt = null,
        ?string $brokerAddr = null,
    ): void {
        $client = $this->requireClient();
        $header = new EndTransactionRequestHeader();

        if ($fromTransactionCheck) {
            // 回收时 broker 会把 COMPRESSED/事务相关信息放在回查请求里
            $header->commitLogOffset = $checkHeader->commitLogOffset ?? null;
            $header->tranStateTableOffset = $checkHeader->tranStateTableOffset ?? null;
            $header->transactionId = $checkHeader->transactionId ?? null;
            $header->bname = $checkHeader->bname ?? null;
            $header->topic = $checkHeader->topic ?? null;
            $uniq = $msgExt !== null
                ? $msgExt->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                : null;
            $header->msgId = ($uniq !== null && $uniq !== '')
                ? $uniq
                : ($msgExt !== null ? $msgExt->getMsgId() : null);
        } else {
            if ($sendResult === null || $msg === null) {
                throw new MQClientException('send result and message are required for normal end transaction');
            }
            // Java：id = decodeMessageId(offsetMsgId != null ? offsetMsgId : msgId)
            [, , $offset] = MessageDecoder::decodeMessageId($sendResult->offsetMsgId ?? $sendResult->msgId ?? '');
            $brokerName = $sendResult->messageQueue !== null ? $sendResult->messageQueue->brokerName : '';
            $header->commitLogOffset = $offset;
            $header->tranStateTableOffset = $sendResult->queueOffset;
            $header->transactionId = $sendResult->transactionId;
            $header->bname = $brokerName;
            $header->topic = $msg->getTopic();
            $header->msgId = $sendResult->msgId;
            // Java endTransaction:1541 用的也是 findBrokerAddressInPublish（只认 master），
            // 且**不做 null 检查**就直接 oneway；这里保留本端提前报错的守卫（比 Java 的
            // NPE 干净），但地址来源必须同样是 master-only。
            $brokerAddr = $client->findBrokerAddressInPublish($brokerName);
        }

        $header->producerGroup = $this->producerGroup;
        $header->commitOrRollback = self::transactionFlag($state);
        $header->fromTransactionCheck = $fromTransactionCheck;

        $remark = null;
        if ($localException !== null) {
            $remark = 'executeLocalTransactionBranch exception: ' . $localException->getMessage();
        }

        $cmd = RemotingCommand::createRequestCommand(RequestCode::END_TRANSACTION, $header);
        $cmd->remark = $remark;
        if ($brokerAddr === null || $brokerAddr === '') {
            throw new MQClientException('no broker address for end transaction', null);
        }
        $client->remotingClient->invokeOneway($brokerAddr, $cmd);
        // 对应 Java endTransaction 末尾的 executeEndTransactionHook：无论主动提交还是
        // broker 回查后提交，都会落一条 EndTransaction 轨迹
        if ($this->endTransactionHookList !== []) {
            $ctx = new EndTransactionContext();
            $ctx->producerGroup = $this->producerGroup;
            $ctx->message = $msgExt ?? $msg;
            $ctx->brokerAddr = $brokerAddr ?? '';
            $ctx->msgId = $header->msgId;
            $ctx->transactionId = $header->transactionId;
            $ctx->transactionState = $state;
            $ctx->fromTransactionCheck = $fromTransactionCheck;
            $ctx->namespace = $this->namespace;
            $this->executeEndTransactionHook($ctx);
        }
    }

    /**
     * 处理 broker 主动发来的事务回查（CHECK_TRANSACTION_STATE=39）。
     *
     * 对齐 Java ClientRemotingProcessor.checkTransactionState + DefaultMQProducerImpl
     * .checkTransactionState：broker 是 **oneway** 发来的（body 为整条编码后的
     * MessageExt），因此**不回响应**，而是在新线程里调 listener.checkLocalTransaction，
     * 再以 END_TRANSACTION(fromTransactionCheck=true) 把最终状态告知 broker。
     *
     * ⚠ PHP 单线程：Python 在新线程里跑 ``_run``，这里改为**内联**执行（注册处理器在
     * remoting 层收帧分发时被调用，不会死锁——END_TRANSACTION 走 oneway 纯写）。
     */
    private function handleCheckTransactionState(RemotingCommand $cmd, string $addr): ?RemotingCommand
    {
        $header = new CheckTransactionStateRequestHeader();
        try {
            $header->fromExtFields($cmd->extFields ?? []);
        } catch (\Throwable) {
            Logger::warning(sprintf('checkTransactionState: decode header failed from %s', $addr));
            return null;
        }

        $msgExt = $cmd->body !== null && $cmd->body !== '' ? MessageDecoder::decodeMessage($cmd->body) : null;
        if ($msgExt === null) {
            Logger::warning('checkTransactionState: decode message failed');
            return null;
        }

        $group = $msgExt->getProperty(MessageConst::PROPERTY_PRODUCER_GROUP);
        if ($group !== null && $group !== $this->producerGroup) {
            Logger::debug(sprintf('checkTransactionState: group %s not mine (%s)', $group, $this->producerGroup));
            return null;
        }

        $listener = $this->transactionListener;
        if ($listener === null) {
            Logger::warning(sprintf(
                'checkTransactionState: no transaction listener for group %s',
                $this->producerGroup
            ));
            return null;
        }

        try {
            $ret = $listener->checkLocalTransaction($msgExt);
            $state = $ret ?? LocalTransactionState::UNKNOW;
            $exception = null;
        } catch (\Throwable $e) {
            Logger::error('Broker call checkTransactionState, but checkLocalTransaction exception');
            $state = LocalTransactionState::UNKNOW;
            $exception = $e;
        }
        try {
            $this->endTransaction(null, null, $state, $exception, true, $header, $msgExt, $addr);
        } catch (\Throwable $e) {
            Logger::warning('checkTransactionState: end transaction failed: ' . $e->getMessage());
        }
        return null; // broker 的回查是 oneway，不回响应
    }

    // ================================================================ 管理能力

    /** @return list<MessageQueue> */
    public function fetchPublishMessageQueues(string $topic): array
    {
        $client = $this->requireClient();
        $publish = $client->getTopicPublishInfo($topic);
        return $publish->msgQueueList;
    }

    public function createTopic(string $key, string $newTopic, int $queueNum = 4, int $topicSysFlag = 0): void
    {
        $client = $this->requireClient();
        // 对应 Java DefaultMQProducerImpl.createTopic：checkTopic + isSystemTopic
        Validators::checkTopic($newTopic);
        Validators::isSystemTopic($newTopic);
        $perm = 6; // PermName.PERM_READ | PERM_WRITE
        $client->createTopicInRoute($newTopic, $queueNum, $queueNum, $perm);
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

    /**
     * Java MQAdminImpl:250：earliestMsgStoreTime 与 max/min/search 同一个形状 —— 只认 master。
     *
     * ⚠ 移植差异：Python 调了 MQClientInstance 的私有 ``_publish_addr_in_admin`` /
     * ``_invoke_sync`` / ``_check_response``；PHP 端这三个是 private，这里用公开面等价复刻
     * （publishAddrFor 逻辑 + remotingClient->invokeSync + 本地状态码检查），语义不变。
     */
    public function earliestMsgStoreTime(MessageQueue $mq): int
    {
        $client = $this->requireClient();
        $addr = $client->findBrokerAddressInPublish($mq->brokerName);
        if ($addr === null || $addr === '') {
            $client->updateTopicRouteInfoFromNameServer($mq->topic);
            $addr = $client->findBrokerAddressInPublish($mq->brokerName);
        }
        if ($addr === null || $addr === '') {
            throw new MQClientException(sprintf('The broker[%s] not exist', $mq->brokerName));
        }
        $header = new GetEarliestMsgStoretimeRequestHeader(topic: $mq->topic, queueId: $mq->queueId);
        $request = RemotingCommand::createRequestCommand(RequestCode::GET_EARLIEST_MSG_STORETIME, $header);
        $response = $client->remotingClient->invokeSync($addr, $request, 5000);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQBrokerException($response->code, $response->remark ?? '');
        }
        $respHeader = new GetEarliestMsgStoretimeResponseHeader();
        $respHeader->fromExtFields($response->extFields);
        return $respHeader->timestamp ?? 0;
    }

    /** 查询消息（返回原始 body），对应 Java queryMessage。 */
    public function queryMessage(string $topic, string $key, int $maxNum, int $begin, int $end): array
    {
        $client = $this->requireClient();
        $body = $client->queryMessage($topic, $key, $maxNum, $begin, $end);
        if ($body === null) {
            return [];
        }
        return MessageDecoder::decodeMessages($body);
    }

    public function viewMessage(string $topic, string $msgId): never
    {
        throw new MQClientException('viewMessage by msgId is not supported in Python edition');
    }

    // ================================================================ 辅助

    /**
     * 对应 ``Validators.checkMessage(msg, this)``——纯本地、打网络之前就跑完。
     *
     * 校验项与顺序都跟着 Java 走：topic（blank/长度/字符表）→ 禁发 topic →
     * body（null/零长/超过 maxMessageSize）→ ``INNER_MULTI_DISPATCH`` 不能带路径分隔符。
     */
    private function checkMessage(Message $msg): void
    {
        Validators::checkMessage($msg, $this->maxMessageSize);
    }

    private static function needAddr(MQClientInstance $client, MessageQueue $mq): string
    {
        // 单向发送没有应答，地址只能提前解析；口径同 Java sendKernelImpl（只认 master）
        return $client->publishAddrFor($mq->brokerName, $mq->topic);
    }
}

/**
 * 事务消息生产者（对应 Java TransactionMQProducer）。
 */
final class TransactionMQProducer extends DefaultMQProducer
{
    public ?TransactionListener $transactionListener = null;
    public int $checkThreadPoolMinSize = 1;
    public int $checkThreadPoolMaxSize = 1;
    public int $checkRequestHoldMax = 2000;

    public function setTransactionListener(?TransactionListener $listener): void
    {
        $this->transactionListener = $listener;
    }

    public function getTransactionListener(): ?TransactionListener
    {
        return $this->transactionListener;
    }

    public function sendMessageInTransaction(Message $msg, ?TransactionListener $listener = null, mixed $arg = null): TransactionSendResult
    {
        $listener = $listener ?? $this->transactionListener;
        if ($listener === null) {
            throw new MQClientException('transaction listener is not set');
        }
        return parent::sendMessageInTransaction($msg, $listener, $arg);
    }
}

/**
 * 便捷回调包装：把 onSuccess / onException 转成普通函数（对应 Python SendCallbackImpl）。
 */
final class SendCallbackImpl implements SendCallback
{
    /** @var (callable(SendResult|null): void)|null */
    private $successFn;
    /** @var (callable(\Throwable): void)|null */
    private $exceptionFn;

    /** @param (callable(SendResult|null): void)|null $successFn @param callable(\Throwable): void|null $exceptionFn */
    public function __construct(?callable $successFn = null, ?callable $exceptionFn = null)
    {
        $this->successFn = $successFn;
        $this->exceptionFn = $exceptionFn;
    }

    public function onSuccess(?SendResult $sendResult): void
    {
        if ($this->successFn !== null) {
            ($this->successFn)($sendResult);
        }
    }

    public function onException(\Throwable $e): void
    {
        if ($this->exceptionFn !== null) {
            ($this->exceptionFn)($e);
        }
    }
}
