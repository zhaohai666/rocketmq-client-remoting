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
use RocketMQ\Remoting\SocksProxyConfig;

/**
 * 生产者：经典 remoting 协议下的生产者实现。
 *
 * 提供：DefaultMQProducer（同步/异步/单向/批量/选择器发送、事务消息）、
 * TransactionMQProducer、MessageQueueSelector 系列选择器、SendCallback 回调、
 * LocalTransactionState / TransactionListener 等。
 *
 * ## PHP 单线程适配（见 php/PORTING.md「异步模型」）
 *
 * sendAsync 注册回调后**在内部泵到完成**（``pumpAsyncSend``：驱动 AsyncSenderExecutor 队列、
 * remoting 层 ``waitResponses``、回调执行器，直到回调链走完），对调用方呈现「注册回调后
 * 由回调通知结果」的时序，没有后台线程。所有周期性/异步的后台工作一律改为调用方驱动的显式泵：
 *
 *   * 心跳：``heartbeatOnce()`` 由调用方按周期驱动；
 *   * 事务回查 CHECK_TRANSACTION_STATE(39)：处理器内联执行。
 *
 * 允许注入时钟（``monoClock`` / ``wallClock``）便于单测。
 */

// 定点发送守卫的两处错误文案（同步入口与异步入口各一处），
// 逐字保留：两处文案本就不同，按调用入口取对应的那条。
// （以 DefaultMQProducer::PINNED_TOPIC_MISMATCH_SYNC / _ASYNC 常量落地。）

/**
 * request() 专用的空发送回调（同步请求-应答走异步发送链时，需要一个占位回调，
 * 它不消费发送结果，只负责把状态写回 future）。
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
            // 失败时必须同时做满三件事。少了 putResponseMessage(null)
            // 的话，等待方会一直阻塞到超时才抛错（明明是发送失败，却要等满 timeout）。
            $this->future->sendRequestOk = false;
            $this->future->putResponseMessage(null);
            $this->future->cause = $e;
        }
    }
}

/**
 * 异步链内部**唯一**被调用的回调（背压版发送回调，包在用户回调外面）。
 *
 * 它是异步链内部**唯一**被调用的回调：链的终点 ``complete()`` 先跑 ``SendMessageHook.after``，
 * 再进这里的 ``onSuccess``/``onException`` —— after 钩子必须早于归还许可（它在
 * ``sendKernelImpl`` 的包装回调里、信号量在外层）。归还之后才把
 * 结果交给用户回调。
 *
 * 只归还**本次真正拿到**的许可（``numAcquired``/``sizeAcquired``）：字节信号量超时而
 * 条数信号量已到手时，必须把那条还回去，否则关一次背压就把容量永久吃掉一格。
 */
final class BackPressureSendCallback implements SendCallback
{
    /** PHP 单线程泵需要知道回调链是否已走完。 */
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
     * 归还顺序：先还字节、再还条数。
     *
     * 用 ``$released`` 保证只还一次。直接归还的话，链上任何一条走到终点之后的
     * 重复回调都会把容量虚增出去；本端的异步重试链较长，还一次更稳妥。
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
 * 异步发送重试计数（PHP 数组是值类型，用一个小对象代替「跨重试共享的自增下标」）。
 */
final class AsyncSendAttempt
{
    public int $times = 0;
}

/**
 * 消息队列选择器。
 */
interface MessageQueueSelector
{
    /**
     * @param list<MessageQueue> $mqs
     */
    public function select(array $mqs, Message $msg, mixed $arg): MessageQueue;
}

/**
 * 按 arg 的 hash 选择队列。
 */
final class SelectMessageQueueByHash implements MessageQueueSelector
{
    public function select(array $mqs, Message $msg, mixed $arg): MessageQueue
    {
        if ($mqs === []) {
            throw new MQClientException('no message queue');
        }
        $value = $arg ?? 0;
        // 这里取确定性的 32 位字符串哈希：
        // 只保证「同值同队列」与均匀分布语义，不追求和别的语言实现逐位相同。
        $idx = abs(self::valueHash($value)) % count($mqs);
        return $mqs[$idx];
    }

    /** 确定性字符串哈希（``h = s[0]*31^(n-1) + ...``，32 位有环绕）+ 其它类型的兜底。 */
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
 * 随机选择一个可用队列。
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
 * 按机房（brokerName 前缀）选择队列。
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
 * 异步发送回调。
 */
interface SendCallback
{
    public function onSuccess(?SendResult $sendResult): void;

    public function onException(\Throwable $e): void;
}

/**
 * 本地事务状态。整型取值就是回查应答里上报给 broker 的 localState。
 */
enum LocalTransactionState: int
{
    case COMMIT_MESSAGE = 0;
    case ROLLBACK_MESSAGE = 1;
    case UNKNOW = 2;
}

/**
 * 事务监听器：执行本地事务、以及响应 broker 的事务回查。
 */
interface TransactionListener
{
    public function executeLocalTransaction(Message $msg, mixed $arg): LocalTransactionState;

    public function checkLocalTransaction(MessageExt $msg): LocalTransactionState;
}

/**
 * 事务消息发送结果：半消息的发送结果加上本地事务状态。
 *
 * ⚠ 既有的 ``SendResult`` 已被声明为 ``final``，PHP 无法 ``extends``。这里用组合替代
 * 继承——构造时拷贝基础结果的字段，并对全部读取口径做同形委托。
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
 * 默认生产者：同步 / 异步 / 单向发送、事务消息、Request-Reply 与消息查询的入口。
 */
class DefaultMQProducer
{
    /** 定点发送守卫的两处文案：同步入口与异步入口各自一处。 */
    public const PINNED_TOPIC_MISMATCH_SYNC = "message's topic not equal mq's topic";
    public const PINNED_TOPIC_MISMATCH_ASYNC = 'Topic of the message does not match its target message queue';

    /** 攒批排除项检查的四个 broker 延时属性（缺省 0）。 */
    private const TIMER_DELAY_PROPERTIES = ['DELAY', 'TIMER_DELAY_MS', 'TIMER_DELAY_SEC', 'TIMER_DELIVER_MS'];

    public string $producerGroup;
    /** TLS（null = 交给 env ROCKETMQ_TLS_ENABLE 决定） */
    public ?bool $tlsEnable;
    /** TLS 细项（caCert/clientCert/clientKey/serverName），语义见 RemotingClient::$tlsOptions。 */
    public ?array $tlsOptions = null;
    public ?SocksProxyConfig $socksProxy = null;
    /** W3C traceparent 透传（opt-in） */
    public bool $enableTraceContext;
    public string $namespace = '';
    public string $instanceName = MixAll::DEFAULT_INSTANCE_NAME;
    public ?string $clientId = null;

    // ---- unit / stream 配置 ----
    // unitName 会拼进 clientId 与动态取址 URL；unitMode 会透传给 broker
    // （发消息头、心跳 ConsumerData、拦截钩子上下文）；enableStreamRequestType
    // 既给 clientId 加 @STREAM 后缀（防止客户端实例被意外复用），
    // 也给每个请求加 ReqT=0 扩展字段。三者默认都是「关/空」。
    public ?string $unitName = null;
    public bool $unitMode = false;
    public bool $enableStreamRequestType = false;
    /**
     * 5.x 新命名空间（namespaceV2）：非空时**每笔**请求都带
     * `nsd=true` / `ns=<该值>` 两个扩展头，broker 据此把请求解析到对应的 serverless 实例。
     * 与 {@see $namespace}（客户端把 `namespace%` 拼进 topic/group 名）是两套机制：
     * namespaceV2 **不改**任何资源名，只改请求头。见 NamespaceRpcHook。
     */
    public string $namespaceV2 = '';

    public string $createTopicKey = MixAll::DEFAULT_TOPIC;
    public int $defaultTopicQueueNums = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    public int $sendMsgTimeout = 3000;
    // 压缩配置：body 超过 compressMsgBodyOverHowmuch 才压缩，默认 ZLIB
    public int $compressMsgBodyOverHowmuch = 1024 * 4;
    public int $compressLevel = 5;
    public int $compressType = MessageSysFlag::ZLIB_TYPE;
    public int $retryTimesWhenSendFailed = 2;
    public int $retryTimesWhenSendAsyncFailed = 2;
    public bool $retryAnotherBrokerWhenNotStoreOk = false;
    // 默认 -1 = 不限制单次请求超时；设成正值后每次重试的单请求
    // 超时被压到该值，慢 broker 才会被换掉而不是把总预算吃光。
    public int $sendMsgMaxTimeoutPerRequest = -1;
    /**
     * 可重试的 broker 响应码：明确回了这些码才值得换一台重试，
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
     * PHP 无线程：心跳不启后台循环，改由调用方显式泵（``heartbeatOnce()``）。
     * 字段保留只是为了表达「心跳是否还在跑」这个状态（shutdown 后置 false）。
     */
    public bool $heartbeatRunning = false;
    public int $heartbeatIntervalMillis = 30000;
    // 路由刷新周期（默认 30000ms）。
    // 只在 start() 建 MQClientInstance 时透传一次，之后修改不生效：排期按启动时的周期定下。
    public int $pollNameServerInterval = 30000;
    public bool $sendLatencyFaultEnable = false;
    public MQFaultStrategy $mqFaultStrategy;

    public bool $startDetectorEnable = false;
    public int $detectTimeout = 200;
    public int $detectInterval = 2000;

    public bool $autoBatch = false;
    public int $batchMaxDelayMs = -1;
    public int $batchMaxBytes = -1;
    public int $totalBatchMaxBytes = -1;
    public ?ProduceAccumulator $produceAccumulator = null;

    // ---- 消息轨迹（enableTrace / traceTopic / traceMsgBatchNum）----
    public bool $enableTrace = false;
    public ?string $traceTopic = null; // null → 用 RMQ_SYS_TRACE_TOPIC
    public int $traceMsgBatchNum = 10;
    /** @var list<SendMessageHook> */
    public array $sendMessageHookList = [];
    /** @var list<EndTransactionHook> */
    public array $endTransactionHookList = [];
    /** 发送前拦截钩子 */
    /** @var list<CheckForbiddenHook> */
    public array $checkForbiddenHookList = [];
    public ?AsyncTraceDispatcher $traceDispatcher = null;
    /** 基础客户端指标（send/consume RT 与计数） */
    public ClientMetrics $metrics;

    /** Request-Reply 的默认超时（不传 timeout 给 request() 时用它）。 */
    public int $requestTimeout = RequestResponseFuture::DEFAULT_REQUEST_TIMEOUT_MILLIS;

    // ---- 异步发送（待发送队列 + 发送执行器 + 回调执行器）----
    // 待发送队列容量默认 50000，发送池大小 = CPU 核数。
    public int $asyncSenderQueueCapacity = 50000;
    // 回调执行器线程数默认取可用 CPU 核数，<=0 时兜到 4；这里 0 表示"按 CPU 核数"
    public int $clientCallbackExecutorThreads = 0;
    private ?ConsumeExecutor $asyncSenderExecutor = null;
    private ?ConsumeExecutor $callbackExecutor = null;

    // ---- 异步发送背压（在途条数与在途字节数两个公平信号量）----
    // 默认关闭，且开关本身**不是**启动期配置：允许跑到一半再打开/关掉，
    // 三个字段都是普通属性，改容量走 setter。
    public bool $enableBackpressureForAsyncMode = false;
    public int $backPressureForAsyncSendNum = 1024;
    public int $backPressureForAsyncSendSize = 100 * 1024 * 1024;
    private FairSemaphore $semaphoreAsyncSendNum;
    private FairSemaphore $semaphoreAsyncSendSize;

    /**
     * 注入时钟（毫秒，单调口径）。null = microtime(true)*1000。
     * 仅供单测推进超时/重试预算。
     *
     * @var \Closure(): float|null
     */
    public ?\Closure $monoClock = null;

    /**
     * 注入墙钟（毫秒，Request-Reply 用）。
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
        $this->mqFaultStrategy->setStartDetectorEnable($this->startDetectorEnable);
        $this->mqFaultStrategy->setDetectTimeout($this->detectTimeout);
        $this->mqFaultStrategy->setDetectInterval($this->detectInterval);
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

    // ================================================================ 模块级 helper

    /**
     * 背压许可容量：配置不高于地板值时用地板值并记一条日志。
     *
     * 分支条件是严格大于：也就是**恰好等于**地板值时照样打那条日志。
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
     * 扣多少个「字节」许可：按 body 长度，空 body 也算 1。
     *
     * 批量发送没有单条那样的现成公式，这里按每条累加、整批为空时算 1 —— 不给它一个值就等于
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
     * 攒批排除项检查用的四个 broker 延时属性的最大值（缺省 0）。
     *
     * 属性名为服务端定义的投递延时属性：``DELAY``（延时等级）、``TIMER_DELAY_MS``、
     * ``TIMER_DELAY_SEC``、``TIMER_DELIVER_MS``。值非法时**直接抛**，不静默当 0。
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
     * 异步发送失败的三分支包装：按 remoting 层异常类型判定能否重试。
     *
     * ⚠ 只有 remoting 层抛回来的异常走这里。**已经收到响应**、但 ``parseSendResponse``
     * 判定为失败的错误（``MQBrokerException``）不走这里 —— 那条路径的 ``needRetry`` 恒为
     * false 且**原样**抛出。也就是说异步发送**不看** ``retryResponseCodes``：
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
     * 发送结束后还原**调用方**那条 Message（用显式 try/finally 保证三条出口都走到）。
     *
     * body 换回压缩前那一份、topic 剥掉命名空间。不还原会坏在哪：压缩是就地
     * ``msg->setBody(...)``，调用方拿着同一个对象再发一次，第二次就把压缩流当原文又压
     * 一遍（zlib(zlib(x))），消费端只解一层、拿到的是压缩流；带 namespace 时调用方还会
     * 一直盯着被改写的 ``ns%topic``。成功、失败、拦截钩子抛异常三条路上都要还原，
     * 所以必须在 ``finally`` 而不是正常返回路径。
     *
     * topic 是 ``withoutNamespace`` 而不是"恢复进入时的字符串"：调用方自己传进来、
     * 本来就带前缀的 topic 同样会被剥掉。
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

    /** 按 ";" 拆分并去空白。 */
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

    public function setUnitName(?string $unitName): void
    {
        $this->unitName = $unitName;
    }

    public function getUnitName(): ?string
    {
        return $this->unitName;
    }

    public function setUnitMode(bool $unitMode): void
    {
        $this->unitMode = $unitMode;
    }

    public function isUnitMode(): bool
    {
        return $this->unitMode;
    }

    public function setEnableStreamRequestType(bool $enable): void
    {
        $this->enableStreamRequestType = $enable;
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


    public function setEnableBackpressureForAsyncMode(bool $enable): void
    {
        $this->enableBackpressureForAsyncMode = $enable;
    }

    public function isEnableBackpressureForAsyncMode(): bool
    {
        return $this->enableBackpressureForAsyncMode;
    }

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

    public function setSendLatencyFaultEnable(bool $enable): void
    {
        $this->sendLatencyFaultEnable = $enable;
        $this->mqFaultStrategy->setSendLatencyFaultEnable($enable);
    }


    public function setStartDetectorEnable(bool $enable): void
    {
        $this->startDetectorEnable = $enable;
        $this->mqFaultStrategy->setStartDetectorEnable($enable);
    }

    public function isStartDetectorEnable(): bool
    {
        return $this->startDetectorEnable;
    }

    public function setDetectTimeout(int $timeoutMillis): void
    {
        $this->detectTimeout = $timeoutMillis;
        $this->mqFaultStrategy->setDetectTimeout($timeoutMillis);
    }

    public function getDetectTimeout(): int
    {
        return $this->detectTimeout;
    }

    public function setDetectInterval(int $intervalMillis): void
    {
        $this->detectInterval = $intervalMillis;
        $this->mqFaultStrategy->setDetectInterval($intervalMillis);
    }

    public function getDetectInterval(): int
    {
        return $this->detectInterval;
    }

    /** 设置 Request-Reply 的默认超时（不传 timeout 给 request() 时用它）。 */
    public function setRequestTimeout(int $timeoutMillis): void
    {
        $this->requestTimeout = $timeoutMillis;
    }


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


    public function setEnableTrace(bool $enable): void
    {
        $this->enableTrace = $enable;
    }

    public function isEnableTrace(): bool
    {
        return $this->enableTrace;
    }

    public function setTraceTopic(?string $traceTopic): void
    {
        $this->traceTopic = $traceTopic;
    }

    public function setTraceMsgBatchNum(int $n): void
    {
        $this->traceMsgBatchNum = $n;
    }

    public function registerSendMessageHook(SendMessageHook $hook): void
    {
        $this->sendMessageHookList[] = $hook;
    }

    public function hasSendMessageHook(): bool
    {
        return $this->sendMessageHookList !== [];
    }


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

    public function executeCheckForbiddenHook(CheckForbiddenContext $context): void
    {
        if (!$this->hasCheckForbiddenHook()) {
            return;
        }
        foreach ($this->checkForbiddenHookList as $hook) {
            $hook->checkForbidden($context);
        }
    }

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

    private function sendHeaderArgs(): array
    {
        return [
            'unitMode' => $this->unitMode,
            'defaultTopic' => $this->createTopicKey,
            'defaultTopicQueueNums' => $this->defaultTopicQueueNums,
        ];
    }

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
        if ($this->namespace !== '') {
            $this->producerGroup = NamespaceUtil::wrapNamespace($this->namespace, $this->producerGroup);
        }
        Validators::checkGroup($this->producerGroup);
        if ($this->producerGroup === MixAll::DEFAULT_PRODUCER_GROUP) {
            throw new MQClientException(sprintf(
                'producerGroup can not equal %s, please specify another one.',
                MixAll::DEFAULT_PRODUCER_GROUP
            ));
        }
        if ($this->nameServerAddrs === [] && !DefaultTopAddressing::isConfigured()) {
            throw new MQClientException('name server address is not set', ClientErrorCode::NO_NAME_SERVER_EXCEPTION);
        }
        $this->instanceName = MixAll::changeInstanceNameToPid($this->instanceName);
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
        $this->createAsyncExecutors();
        $this->initProduceAccumulator();
        $this->started = true;
        $this->heartbeatRunning = true;
        foreach ($this->topics as $topic) {
            $newTopic = $this->withNamespace($topic);
            try {
                $info = $this->topicPublishInfo($newTopic);
                if ($info === null || !$info->ok()) {
                    Logger::warning('No route info of this topic: ' . $newTopic);
                }
            } catch (\Throwable $e) {
                Logger::warning(sprintf('initTopicRoute failed for %s: %s', $newTopic, $e->getMessage()));
            }
        }
        $this->mqClient->registerFaultStrategy($this->mqFaultStrategy);
        if ($this->produceAccumulator !== null) {
            $this->produceAccumulator->start();
        }
        $this->startTraceDispatcher();
    }

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
                $dispatcher->setNamespaceV2($this->namespaceV2);
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
        if ($this->asyncSenderExecutor !== null) {
            $this->asyncSenderExecutor->shutdown();
            $this->asyncSenderExecutor = null;
        }
        if ($this->callbackExecutor !== null) {
            $this->callbackExecutor->shutdown();
            $this->callbackExecutor = null;
        }
        if ($this->mqClient !== null) {
            try {
                $this->mqClient->unregisterClientAllBrokers($this->clientId ?? '', $this->producerGroup, '');
            } catch (\Throwable $e) {
                Logger::debug('unregister on shutdown failed: ' . $e->getMessage());
            }
            $this->mqClient->shutdown();
        }
        $this->started = false;
        if ($this->produceAccumulator !== null) {
            $this->produceAccumulator->shutdown();
        }
        if ($this->traceDispatcher !== null) {
            try {
                $this->traceDispatcher->shutdown();
            } catch (\Throwable $e) {
                Logger::warning('trace dispatcher shutdown failed: ' . $e->getMessage());
            }
        }
    }

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

    private function withNamespace(string $topic): string
    {
        if ($this->namespace === '') {
            return $topic;
        }
        return NamespaceUtil::wrapNamespace($this->namespace, $topic);
    }

    private function checkPinnedTopic(string $topic, MessageQueue $mq, string $message): void
    {
        if ($this->withNamespace($mq->topic) !== $topic) {
            throw new MQClientException($message);
        }
    }

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
                $this->validateNameServerSetting();
                throw $inner;
            }
        }
    }

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
        if ($timeoutMillis === null
            && $this->getAutoBatch()
            && !($msg instanceof MessageBatch)) {
            return $this->sendByAccumulator($msg, $mq, null);
        }
        $prevBody = (string) $msg->getBody();
        try {
            return $this->sendSingle($msg, $timeoutMillis, $mq);
        } finally {
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
            $this->checkPinnedTopic($msg->getTopic(), $mq, self::PINNED_TOPIC_MISMATCH_SYNC);
        }
        $sysFlag = $this->tryToCompressMessage($msg);
        if ($mq !== null) {
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

        try {
            $publish = $this->topicPublishInfo($msg->getTopic());
        } catch (MQClientException $e) {
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
                $this->updateFaultItem($selected, $began, true, !$this->mqFaultStrategy->isStartDetectorEnable());
                $lastExc = $e;
            } catch (MQClientException $e) {
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
            $this->sendAsync($msg, new NullSendCallback($future), $timeout > $cost ? $timeout - $cost : $timeout, $mq);
            return $this->waitRequestResponse($msg, $timeout, $future, $cost);
        } finally {
            RequestFutureHolder::getInstance()->removeRequest($correlationId);
        }
    }

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

    public function reply(Message $requestMessage, string $body, ?int $timeoutMillis = null): SendResult
    {
        return $this->send(MessageUtil::createReplyMessage($requestMessage, $body), $timeoutMillis);
    }

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

    public function sendAsync(
        Message|array $msg,
        SendCallback|InternalSendCallback $callback,
        ?int $timeoutMillis = null,
        ?MessageQueue $mq = null,
    ): void {
        if ($timeoutMillis === null && $this->getAutoBatch()
            && !is_array($msg) && !($msg instanceof MessageBatch)) {
            try {
                $this->sendByAccumulator($msg, $mq, $callback);
            } catch (\Throwable $e) {
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
                $this->complete($gated, null, $e, null);
            }
        };

        $this->executeAsyncMessageSend($run, $gated, $timeout, $begin, $executor);
        // PHP 单线程适配（PORTING.md）：Python 注册回调后由后台线程收响应；
        // 这里在内部泵到回调链走完（对调用方呈现相同的回调时序）。
        $this->pumpAsyncSend($gated, $timeout);
    }

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

    private function sendAsyncInner(
        Message|array $msg,
        ?MessageQueue $mq,
        SendCallback|InternalSendCallback $callback,
        int $timeout,
    ): void {
        if (is_array($msg)) {
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
            $this->checkPinnedTopic($msg->getTopic(), $mq, self::PINNED_TOPIC_MISMATCH_ASYNC);
        }
        // 与同步发送同理：压缩在重试链之外做一次，避免重试时把已压缩的 body 再压一遍
        $sysFlag = $this->tryToCompressMessage($msg);
        if ($mq !== null) {
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
        $brokerAddr = $client->publishAddrFor($mq->brokerName, $mq->topic);
        if ($this->hasCheckForbiddenHook()) {
            $this->executeCheckForbidden($msg, $mq, $brokerAddr, null, CommunicationMode::ASYNC);
        }
        if ($this->enableTraceContext) {
            TraceContextPropagator::injectTraceContext($msg);
        }
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
                $selected = $this->mqFaultStrategy->selectOneMessageQueue($publish, $brokerName, false);
                if ($selected !== null) {
                    $retryMq = new MessageQueue($msg->getTopic(), $selected->brokerName, $selected->queueId);
                    $retryBroker = $selected->brokerName;
                }
            }
            $addr = $client->findBrokerAddressInPublish($retryBroker);
            if ($addr === null || $addr === '') {
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
        $this->topicPublishInfo($topic);
        $addr = $client->findBrokerAddressInPublish($handle->brokerName ?? '');
        if ($addr === null || $addr === '') {
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
        foreach ($msgs as $m) {
            Validators::checkMessage($m, $this->maxMessageSize);
            MessageClientIdSetter::setUniqId($m);
            $m->setTopic($this->withNamespace($m->getTopic()));
        }
        $batch = MessageBatch::generateFromList($msgs);
        MessageClientIdSetter::setUniqId($batch);
        $batch->setBody($batch->encode());
        if ($mq !== null) {
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

    public function sendMessageInTransaction(Message $msg, TransactionListener $listener, mixed $arg = null): TransactionSendResult
    {
        $prevBody = (string) $msg->getBody();
        $msg->setTopic($this->withNamespace($msg->getTopic()));

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

        $sysFlag = $this->tryToCompressMessage($msg);
        $sysFlag = MessageSysFlag::resetTransactionValue($sysFlag, MessageSysFlag::TRANSACTION_PREPARED_TYPE);

        try {
            // 事务发送同样走 sendKernelImpl（→ 同样触发发送钩子），
            // 所以开启轨迹后事务消息会先落一条 Pub（Trans_Msg_Half）轨迹
            $sendResult = $this->sendWithHooks($client, $msg, $mqSel, $this->sendMsgTimeout, $sysFlag);
        } catch (\Throwable $e) {
            throw new MQClientException('send message Exception', null, $e);
        } finally {
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
            Logger::warning(sprintf(
                'local transaction execute %s, but end broker transaction failed: %s',
                $state->name,
                $e->getMessage()
            ));
        }

        return new TransactionSendResult($sendResult, $state);
    }

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
            [, , $offset] = MessageDecoder::decodeMessageId($sendResult->offsetMsgId ?? $sendResult->msgId ?? '');
            $brokerName = $sendResult->messageQueue !== null ? $sendResult->messageQueue->brokerName : '';
            $header->commitLogOffset = $offset;
            $header->tranStateTableOffset = $sendResult->queueOffset;
            $header->transactionId = $sendResult->transactionId;
            $header->bname = $brokerName;
            $header->topic = $msg->getTopic();
            $header->msgId = $sendResult->msgId;
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

    private function checkMessage(Message $msg): void
    {
        Validators::checkMessage($msg, $this->maxMessageSize);
    }

    private static function needAddr(MQClientInstance $client, MessageQueue $mq): string
    {
        return $client->publishAddrFor($mq->brokerName, $mq->topic);
    }
}

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
