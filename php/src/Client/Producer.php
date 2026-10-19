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
 * Producer: a producer implementation under the classic remoting protocol.
 *
 * Provides: DefaultMQProducer (sync/async/oneway/batch/selector sending, transactional
 * messages), TransactionMQProducer, the MessageQueueSelector family, the SendCallback
 * callback, LocalTransactionState / TransactionListener, etc.
 *
 * ## PHP single-threaded adaptation (see php/PORTING.md "async model")
 *
 * sendAsync **pumps to completion internally** after registering the callback
 * (``pumpAsyncSend``: drives the AsyncSenderExecutor queue, the remoting layer's
 * ``waitResponses``, and the callback executor until the callback chain finishes),
 * presenting callers the "result notified by the callback after registration" timing
 * with no background threads. All periodic/async background work becomes caller-driven
 * explicit pumping:
 *
 *   * Heartbeat: the instance registers this producer in its own table, so the group is
 *     (re-)registered when a route lands and on every ``MQClientInstance::tick()``; an
 *     application that drives no pump at all can still call ``heartbeatOnce()`` itself;
 *   * Transaction check-back CHECK_TRANSACTION_STATE(39): executed inline in the processor.
 *
 * Clock injection (``monoClock`` / ``wallClock``) is allowed for unit tests.
 */

// The two error texts of the pinned-send guard (one at the sync entry, one at the async
// entry), kept verbatim: the two texts are inherently different; take the one
// corresponding to the call entry.
// (Landed as the DefaultMQProducer::PINNED_TOPIC_MISMATCH_SYNC / _ASYNC constants.)

/**
 * The empty send callback dedicated to request() (a synchronous request-reply going
 * through the async send chain needs a placeholder callback; it does not consume the
 * send result, it only writes the status back to the future).
 *
 * It only writes "send success/failure" back to the RequestResponseFuture; the reply
 * itself is delivered by the broker's 326 push, unrelated to this callback.
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
            // On failure, all three things must be done at once. Without putResponseMessage(null),
            // the waiter would block until timeout before throwing (a send failure, yet waiting the full timeout).
            $this->future->sendRequestOk = false;
            $this->future->putResponseMessage(null);
            $this->future->cause = $e;
        }
    }
}

/**
 * The **only** callback invoked inside the async chain (the backpressure send callback,
 * wrapped around the user callback).
 *
 * It is the **only** callback invoked inside the async chain: the chain's terminus
 * ``complete()`` first runs ``SendMessageHook.after``, then enters this
 * ``onSuccess``/``onException`` here -- the after hook must precede permit return (it is
 * in ``sendKernelImpl``'s wrapper callback, with the semaphore outside). Only after the
 * return is the result handed to the user callback.
 *
 * Only return the permits **actually acquired this time** (``numAcquired``/``sizeAcquired``):
 * when the byte semaphore times out but the count semaphore was already acquired, that one
 * must be returned, otherwise closing backpressure once permanently eats one slot of capacity.
 */
final class BackPressureSendCallback implements SendCallback
{
    /** PHP's single-threaded pump needs to know whether the callback chain has finished. */
    public bool $done = false;
    public bool $numAcquired = false;
    public bool $sizeAcquired = false;

    public function __construct(
        private readonly object $delegate, // SendCallback|InternalSendCallback (duck-typed, same shape)
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
     * Return order: bytes first, then count.
     *
     * Use ``$released`` to guarantee only one return. Returning directly would let any
     * duplicate callback after the chain's terminus inflate the capacity; this port's
     * async retry chain is long, so returning once is safer.
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
 * Async send retry count (PHP arrays are value types; use a small object instead of an
 * "incrementing index shared across retries").
 */
final class AsyncSendAttempt
{
    public int $times = 0;
}

/**
 * Message queue selector.
 */
interface MessageQueueSelector
{
    /**
     * @param list<MessageQueue> $mqs
     */
    public function select(array $mqs, Message $msg, mixed $arg): MessageQueue;
}

/**
 * Select a queue by the hash of arg.
 */
final class SelectMessageQueueByHash implements MessageQueueSelector
{
    public function select(array $mqs, Message $msg, mixed $arg): MessageQueue
    {
        if ($mqs === []) {
            throw new MQClientException('no message queue');
        }
        $value = $arg ?? 0;
        // A deterministic 32-bit string hash is used here:
        // it only guarantees "same value -> same queue" and even-distribution semantics,
        // not bit-for-bit equality with other language implementations.
        $idx = abs(self::valueHash($value)) % count($mqs);
        return $mqs[$idx];
    }

    /** Deterministic string hash (``h = s[0]*31^(n-1) + ...``, 32-bit with wraparound) + a fallback for other types. */
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
 * Randomly select an available queue.
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
 * Select a queue by machine room (brokerName prefix).
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
        // Fall back to the first if no match
        return $mqs[0];
    }
}

/**
 * Async send callback.
 */
interface SendCallback
{
    public function onSuccess(?SendResult $sendResult): void;

    public function onException(\Throwable $e): void;
}

/**
 * Local transaction state. The integer values are the localState reported to the broker in the check-back reply.
 */
enum LocalTransactionState: int
{
    case COMMIT_MESSAGE = 0;
    case ROLLBACK_MESSAGE = 1;
    case UNKNOW = 2;
}

/**
 * Transaction listener: executes the local transaction and responds to the broker's transaction check-back.
 */
interface TransactionListener
{
    public function executeLocalTransaction(Message $msg, mixed $arg): LocalTransactionState;

    public function checkLocalTransaction(MessageExt $msg): LocalTransactionState;
}

/**
 * Transactional message send result: the half message's send result plus the local transaction state.
 *
 * WARNING: the existing ``SendResult`` is already declared ``final`` and PHP cannot
 * ``extends`` it. Composition is used here instead of inheritance -- the base result's
 * fields are copied at construction, with same-shape delegation for all read paths.
 * If SendResult ever drops final, switching back to inheritance is seamless (this file's
 * public API does not change).
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

    /** The underlying SendResult (the retrieval entry point for composition-over-inheritance). */
    public function getSendResult(): SendResult
    {
        return $this->sendResult;
    }

    // ---- Same-shape delegation with SendResult ----

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
 * Default producer: the entry point for sync / async / oneway sending, transactional
 * messages, Request-Reply and message queries.
 */
class DefaultMQProducer
{
    /** The two texts of the pinned-send guard: one each at the sync and async entries. */
    public const PINNED_TOPIC_MISMATCH_SYNC = "message's topic not equal mq's topic";
    public const PINNED_TOPIC_MISMATCH_ASYNC = 'Topic of the message does not match its target message queue';

    /** The four broker delay properties checked for batch exclusion (default 0). */
    private const TIMER_DELAY_PROPERTIES = ['DELAY', 'TIMER_DELAY_MS', 'TIMER_DELAY_SEC', 'TIMER_DELIVER_MS'];

    public string $producerGroup;
    /** TLS (null = let env ROCKETMQ_TLS_ENABLE decide) */
    public ?bool $tlsEnable;
    /** TLS details (caCert/clientCert/clientKey/serverName); semantics see RemotingClient::$tlsOptions. */
    public ?array $tlsOptions = null;
    public ?SocksProxyConfig $socksProxy = null;
    /** W3C traceparent pass-through (opt-in) */
    public bool $enableTraceContext;
    public string $namespace = '';
    public string $instanceName = MixAll::DEFAULT_INSTANCE_NAME;
    public ?string $clientId = null;

    // ---- unit / stream config ----
    // unitName is appended into the clientId and the dynamic address-lookup URL; unitMode
    // is passed through to the broker (send headers, heartbeat ConsumerData, interceptor
    // hook context); enableStreamRequestType both adds the @STREAM suffix to the clientId
    // (preventing accidental reuse of client instances) and adds a ReqT=0 extension field
    // to every request. All three default to "off/empty".
    public ?string $unitName = null;
    public bool $unitMode = false;
    public bool $enableStreamRequestType = false;
    /**
     * 5.x new namespace (namespaceV2): when non-empty, **every** request carries the two
     * extension headers `nsd=true` / `ns=<value>`, by which the broker resolves the
     * request to the corresponding serverless instance. This is a separate mechanism from
     * {@see $namespace} (the client prefixes `namespace%` into topic/group names):
     * namespaceV2 **does not change** any resource name, only request headers.
     * See NamespaceRpcHook.
     */
    public string $namespaceV2 = '';
    /**
     * Heartbeat fingerprint path (default false). On, this facade's HEART_BEAT(34) rounds carry a
     * non-zero heartbeatFingerprint and, once a broker answers IS_SUPPORT_HEART_BEAT_V2 and the
     * fingerprint is unchanged, the subscription set drops out of the body.
     */
    public bool $useHeartbeatV2 = false;
    /**
     * VIP channel (default false). The broker opens this second channel at `listen port - 2` only
     * when it was started with it enabled, so the default keeps every request on the published port.
     * On, the requests it answers there -- message sends in all flavours, and the earliest-message
     * lookup this facade exposes -- are addressed at that port; heartbeats, transaction back-checks
     * and the oneway END_TRANSACTION never shift.
     */
    public bool $vipChannelEnabled = false;

    public string $createTopicKey = MixAll::DEFAULT_TOPIC;
    public int $defaultTopicQueueNums = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    public int $sendMsgTimeout = 3000;
    // Compression config: compress only when the body exceeds compressMsgBodyOverHowmuch; ZLIB by default
    public int $compressMsgBodyOverHowmuch = 1024 * 4;
    public int $compressLevel = 5;
    public int $compressType = MessageSysFlag::ZLIB_TYPE;
    public int $retryTimesWhenSendFailed = 2;
    public int $retryTimesWhenSendAsyncFailed = 2;
    public bool $retryAnotherBrokerWhenNotStoreOk = false;
    // Default -1 = no per-request timeout limit; when set to a positive value, each retry's
    // single-request timeout is clamped to that value, so a slow broker gets swapped out
    // instead of eating the whole budget.
    public int $sendMsgMaxTimeoutPerRequest = -1;
    /**
     * Retryable broker response codes: only an explicit one of these codes is worth
     * retrying on another broker; other codes (e.g. MESSAGE_ILLEGAL) make retrying futile
     * and must be rethrown as-is.
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
    /** The current transaction listener (recorded during sendMessageInTransaction, for broker check-back calls) */
    private ?TransactionListener $transactionListener = null;
    /**
     * PHP has no threads: this flag starts no loop. ``heartbeatOnce()`` is the explicit
     * round for callers that want one; the instance's own pump registers and refreshes the
     * group without it. The field only expresses the "is the heartbeat still running" state
     * (set to false after shutdown).
     */
    public bool $heartbeatRunning = false;
    public int $heartbeatIntervalMillis = 30000;
    // Route refresh period (default 30000ms).
    // Passed through only once when start() builds the MQClientInstance; later changes take
    // no effect: scheduling is fixed by the period at startup.
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

    // ---- Message tracing (enableTrace / traceTopic / traceMsgBatchNum) ----
    public bool $enableTrace = false;
    public ?string $traceTopic = null; // null -> use RMQ_SYS_TRACE_TOPIC
    public int $traceMsgBatchNum = 10;
    /** @var list<SendMessageHook> */
    public array $sendMessageHookList = [];
    /** @var list<EndTransactionHook> */
    public array $endTransactionHookList = [];
    /** Pre-send interceptor hook */
    /** @var list<CheckForbiddenHook> */
    public array $checkForbiddenHookList = [];
    public ?AsyncTraceDispatcher $traceDispatcher = null;
    /** Basic client metrics (send/consume RT and counts) */
    public ClientMetrics $metrics;

    /** Request-Reply's default timeout (used when no timeout is passed to request()). */
    public int $requestTimeout = RequestResponseFuture::DEFAULT_REQUEST_TIMEOUT_MILLIS;

    // ---- Async send (pending-send queue + send executor + callback executor) ----
    // The pending-send queue capacity defaults to 50000, and the send pool size = CPU cores.
    public int $asyncSenderQueueCapacity = 50000;
    // The callback executor thread count defaults to available CPU cores, falling back to 4
    // when <=0; here 0 means "by CPU cores"
    public int $clientCallbackExecutorThreads = 0;
    private ?ConsumeExecutor $asyncSenderExecutor = null;
    private ?ConsumeExecutor $callbackExecutor = null;

    // ---- Async send backpressure (two fair semaphores: in-flight count and in-flight bytes) ----
    // Off by default, and the switch itself is **not** startup config: it may be turned
    // on/off mid-run; all three fields are plain properties, with capacity changed via setter.
    public bool $enableBackpressureForAsyncMode = false;
    public int $backPressureForAsyncSendNum = 1024;
    public int $backPressureForAsyncSendSize = 100 * 1024 * 1024;
    private FairSemaphore $semaphoreAsyncSendNum;
    private FairSemaphore $semaphoreAsyncSendSize;

    /**
     * Injected clock (milliseconds, monotonic). null = microtime(true)*1000.
     * For unit tests to advance timeout/retry budget only.
     *
     * @var \Closure(): float|null
     */
    public ?\Closure $monoClock = null;

    /**
     * Injected wall clock (milliseconds, for Request-Reply).
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
        // W3C traceparent pass-through (opt-in; null = let env ROCKETMQ_TRACE_CONTEXT_ENABLE decide)
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

    // ================================================================ Module-level helpers

    /**
     * Backpressure permit capacity: use the floor value and log a line when the config is
     * not above the floor.
     *
     * The branch condition is strictly greater: that is, when it is **exactly equal** to
     * the floor value, the log line is still printed.
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
     * How many "byte" permits to deduct: by body length; an empty body also counts as 1.
     *
     * Batch sending has no ready-made formula like the single-message case; here each
     * message is accumulated and an empty whole batch counts as 1 -- not giving it a value
     * would mean a whole batch of messages occupies only 1 unit of capacity, making the
     * byte gate a no-op for batches.
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
     * The maximum of the four broker delay properties used for the batch exclusion check (default 0).
     *
     * The property names are server-defined delivery delay properties: ``DELAY`` (delay
     * level), ``TIMER_DELAY_MS``, ``TIMER_DELAY_SEC``, ``TIMER_DELIVER_MS``. An illegal
     * value **throws** directly, not silently treated as 0.
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
     * The three-branch wrapping of async send failures: decide retryability by the remoting layer's exception type.
     *
     * WARNING: only exceptions thrown by the remoting layer go through here. Errors where a
     * **response was already received** but ``parseSendResponse`` judged as failed
     * (``MQBrokerException``) do not go through here -- that path's ``needRetry`` is always
     * false and is **rethrown** as-is. That is, async sending does **not** consult
     * ``retryResponseCodes``: once the broker explicitly returns an error, no broker swap
     * retry happens. Do not confuse this with sync sending's semantics.
     *
     * @return array{0: \Throwable, 1: bool} [the wrapped exception, whether retryable]
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
     * Restore the **caller's** Message after sending finishes (using an explicit try/finally to guarantee all three exits are covered).
     *
     * The body is swapped back to the pre-compression copy and the topic has the namespace
     * stripped. What breaks without restoring: compression is in-place
     * ``msg->setBody(...)``, so when the caller sends the same object again, the second
     * send compresses the compressed stream as raw input again (zlib(zlib(x))), and the
     * consumer only decompresses one layer, getting the compressed stream; with a
     * namespace, the caller would also keep staring at the rewritten ``ns%topic``.
     * All three paths -- success, failure, and interceptor hook throwing -- must restore,
     * so it must be in ``finally`` rather than the normal return path.
     *
     * The topic is ``withoutNamespace`` rather than "restore the string on entry": a topic
     * the caller passed in already carrying the prefix is likewise stripped.
     *
     * Only effective for single messages (the batch path is split off by the caller before entering, no restore).
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

    // ================================================================ Config

    /** Split by ";" and strip whitespace. */
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

    /** Change the "in-flight bytes" cap at runtime; same semantics as ``setBackPressureForAsyncSendNum``. */
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

    /** Set Request-Reply's default timeout (used when no timeout is passed to request()). */
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

    /** Return this producer's basic metrics counters (send/consume RT and counts). */
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
     * Build a CheckForbiddenContext and execute it (exceptions are **not swallowed**, see executeCheckForbiddenHook).
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
        // W3C traceparent pass-through (opt-in): inject the root context if absent; do not overwrite an existing value
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

    // ================================================================ Lifecycle

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
            useHeartbeatV2: $this->useHeartbeatV2,
            vipChannelEnabled: $this->vipChannelEnabled,
        );
        if ($this->rpcHook !== null) {
            $this->mqClient->remotingClient->registerRpcHook($this->rpcHook);
        }
        // The instance heartbeat is what registers this group with the broker (its payload is
        // built from the instance's registries), so the producer must be in the table before
        // the first beat -- including the one start()'s opening pump sends.
        $this->mqClient->registerProducer($this->producerGroup);
        $this->mqClient->start();
        // Dynamic name server: the instance may already have obtained an address from the address server at startup; backfill it to this producer
        if ($this->nameServerAddrs === [] && $this->mqClient->nameServerAddrs !== []) {
            $this->nameServerAddrs = array_values($this->mqClient->nameServerAddrs);
        }
        // Register the broker-initiated request processor: transaction check-back CHECK_TRANSACTION_STATE(39).
        // Match this producer by message_ext's PGROUP property; drop if not matched.
        // PHP single-threaded: check-back handling is executed inline.
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
            $this->mqClient->unregisterProducer($this->producerGroup);
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
        // heartbeatFingerprint / withoutSub keep their 0/false defaults on the legacy path, which
        // is what keeps the broker on its full registration branch (the group is reported
        // alongside the subscription set). With the fingerprint path on, both fields are filled in
        // below and the instance's tables decide which body each broker gets.
        $hb->producerDataSet[] = $pd;
        $okCount = 0;
        if ($client->useHeartbeatV2) {
            // One fingerprint for the round, carried by both bodies.
            $fingerprint = $hb->computeFingerprint();
            $hb->heartbeatFingerprint = $fingerprint;
            $withoutSub = $hb->withoutSubCopy();
            foreach ($addrs as $addr) {
                try {
                    $client->sendHeartbeatV2($addr, $hb, $withoutSub, $fingerprint, $timeoutMillis);
                    $okCount++;
                } catch (\Throwable $e) {
                    Logger::warning(sprintf('producer heartbeat to %s failed: %s', $addr, $e->getMessage()));
                }
            }
            return $okCount;
        }
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

    // ================================================================ Compression

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
     * Message compression: ZLIB goes through gzcompress; LZ4 (Frame) / ZSTD
     * (CLI preferred, Raw/RLE fallback) are handled by CompressionCodec, consistent with
     * MessageDecoder's convention; unknown types throw -> tryToCompressMessage degrades to no compression.
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
     * Record one attempt's latency in the fault-tolerance table. Latency must be measured
     * with the monotonic clock: a local sub-millisecond round trip measured with the
     * millisecond wall-clock difference would record 0, so the latency threshold would
     * never take effect.
     */
    private function updateFaultItem(?MessageQueue $selected, float $began, bool $isolation, bool $reachable): void
    {
        if ($selected === null) {
            return;
        }
        $this->mqFaultStrategy->updateFaultItem($selected->brokerName, $this->monoMillis() - $began, $isolation, $reachable);
    }

    // ================================================================ Normal send

    /**
     * Sync send: msg or array; a specified mq goes through pinned send, otherwise round-robin selection.
     * (Restore semantics see ``restoreCallerMessage``; the batch path does not restore.)
     */
    public function send(Message|array $msg, ?int $timeoutMillis = null, ?MessageQueue $mq = null): SendResult
    {
        // Batches go straight into _sendBatch (the batch branch does not restore)
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

    /** The single-message body of ``send()``. */
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
                    // Metrics accounting, then classify by the original exception
                    $this->metrics->recordSendFailure($sendStart);
                    throw $e;
                }
                $this->metrics->recordSendSuccess($sendStart);
                // Record send latency; exceeding the threshold isolates that broker for a while
                $this->updateFaultItem($selected, $began, false, true);
                if ($result !== null && $result->sendStatus !== SendStatus::SEND_OK
                    && $this->retryAnotherBrokerWhenNotStoreOk) {
                    continue;
                }
                return $result;
            } catch (MQBrokerException $e) {
                // The broker explicitly returned an error code: isolate that broker (reachability untouched); only retryable codes swap to another broker
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
            // If the route cannot be obtained, let the send path below report the error itself
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
        // PHP single-threaded: the future's waitResponseMessage does not block (returns the
        // currently arrived reply); the real waiting pumps the remoting layer here (the
        // reply is pushed back via 326) until expiry.
        $deadline = $this->wallMillis() + max(0, $timeout - $cost);
        while (true) {
            $response = $future->waitResponseMessage(0);
            if ($response !== null) {
                return $response;
            }
            if (!$future->sendRequestOk || $future->cause !== null) {
                // The send has been judged failed (onException would countDown the latch and wake the waiter immediately);
                // equivalently break out here at once, otherwise a send failure would still wait the full timeout.
                break;
            }
            $now = $this->wallMillis();
            if ($now >= $deadline) {
                break;
            }
            $client = $this->mqClient;
            if ($client !== null) {
                // pumpIncoming must be used instead of waitResponses: during the wait the send
                // has long finished and pending is cleared, so waitResponses -- which only
                // watches connections with in-flight requests -- would return directly, and
                // the reply pushed back via 326 would sit in the kernel buffer forever
                // (confirmed live, see the RemotingClient::pumpIncoming comment).
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
     * The PHP stand-in for os.cpu_count(): PHP does not expose the core count, defaulting to
     * 4 (overridable via the RMQ_CPU_CORES environment variable). Only affects the capacity
     * perception of concurrent logical workers, not semantics.
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
        // PHP single-threaded adaptation (PORTING.md): receiving responses via a background
        // thread after callback registration is the conventional deployment form; here we
        // pump internally until the callback chain finishes (presenting callers the same
        // callback timing).
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
     * Pump the async send chain until the callback finishes (actively collecting responses under PHP's single thread):
     * each round runs the AsyncSenderExecutor's queued tasks, the remoting layer's
     * waitResponses, and the callback executor's queued tasks in turn, until ``gated`` is
     * marked done or the fallback deadline is exceeded (timeout + 10s; normally the
     * underlying timeout would have fired the callback long before).
     */
    private function pumpAsyncSend(BackPressureSendCallback $gated, int $timeout): void
    {
        $deadline = $this->monoMillis() + $timeout + 10000;
        while (!$gated->done) {
            if ($this->monoMillis() >= $deadline) {
                break; // fallback: the underlying timeout should already have fired the callback
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
        // Same as sync send: compression is done once outside the retry chain, avoiding re-compressing the already-compressed body on retries
        $sysFlag = $this->tryToCompressMessage($msg);
        if ($mq !== null) {
            $this->sendKernelAsync($client, $msg, $mq, $callback, $timeout, $sysFlag, null);
            return;
        }
        try {
            $publish = $this->topicPublishInfo($msg->getTopic());
        } catch (MQClientException $e) {
            // Same as sync send: 10004 is passed through as-is, others are classified as 10005
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
            // A synchronous throw (e.g. connection refused) must also go through classification: a connect failure gets wrapped
            // as MQClientException('unknown reason') before entering the retry chain; PHP's blocking connect
            // throws synchronously here, and without classification the final callback would get a bare RemotingConnectException.
            [$wrapped, $needRetry] = self::classifyAsyncFailure($e, $cost);
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
            // New opaque swap: the old request is still hanging in responseTable awaiting timeout; reusing it would cross the two attempts' replies
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
        // The selector uses the original message (topic/business fields); compression only affects the body
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
        // Explicitly passing sendMsgTimeout: send()/sendAsync() only divert into sendByAccumulator
        // when timeout===null and autoBatch is on -- without passing it here, non-batched messages
        // (delay/group properties) would recurse sendDirect -> send() -> sendByAccumulator -> sendDirect until OOM.
        if ($sendCallback === null) {
            if ($mq === null) {
                return $this->send($msg, $this->sendMsgTimeout);
            }
            return $this->send($msg, $this->sendMsgTimeout, $mq);
        }
        if ($mq === null) {
            $this->sendAsync($msg, $sendCallback, $this->sendMsgTimeout);
        } else {
            $this->sendAsync($msg, $sendCallback, $this->sendMsgTimeout, $mq);
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

    // ---------------- Batch send ----------------

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
        // MessageBatch is skipped directly by tryToCompressMessage (returns 0); batch messages are never compressed
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
        return MessageSysFlag::TRANSACTION_NOT_TYPE;           // 0 (UNKNOW)
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

        // Half-message marker (the broker side writes the message into RMQ_SYS_TRANS_HALF_TOPIC accordingly)
        $msg->putProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED, 'true');
        $msg->putProperty(MessageConst::PROPERTY_PRODUCER_GROUP, $this->producerGroup);
        // Callback via this listener on check-back (the broker locates this producer via the PGROUP property)
        $this->transactionListener = $listener;

        $publish = $this->topicPublishInfo($msg->getTopic());
        $selected = $publish->selectOneMessageQueue();
        $mqSel = new MessageQueue($msg->getTopic(), $selected->brokerName, $selected->queueId);

        $sysFlag = $this->tryToCompressMessage($msg);
        $sysFlag = MessageSysFlag::resetTransactionValue($sysFlag, MessageSysFlag::TRANSACTION_PREPARED_TYPE);

        try {
            // Transactional sending also goes through sendKernelImpl (-> likewise triggers send hooks),
            // so with tracing enabled a transactional message first lands a Pub (Trans_Msg_Half) trace
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
            // On recycling, the broker puts COMPRESSED/transaction-related info in the check-back request
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
        return null; // the broker's check-back is oneway and returns no response
    }

    // ================================================================ Admin capabilities

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
        $response = $client->remotingClient->invokeSync($client->vipAddrFor($addr), $request, 5000);
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
        throw new MQClientException('viewMessage by msgId is not supported in this edition');
    }

    // ================================================================ Helpers

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
 * Convenience callback wrapper: turns onSuccess / onException into plain functions.
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
