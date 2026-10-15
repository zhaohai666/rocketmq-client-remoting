<?php

declare(strict_types=1);

/**
 * DefaultMQProducer / TransactionMQProducer pure-PHP assert-style self-test (no phpunit, no real cluster).
 *
 * Run: C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunClientProducer.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Network-related cases re-launch this script with a `--broker` argument as a **child-process**
 * fake broker (stream_socket_server, same technique as RunClientInstance.php). Every command the
 * broker receives is appended to a JSON-lines log file so the parent process can assert "what the
 * broker actually received" (send codes, sysFlag, commitOrRollback of END_TRANSACTION, etc.).
 * Clocks are injected via DefaultMQProducer::$monoClock / $wallClock.
 */

require_once __DIR__ . '/../bootstrap.php';

// Clear dynamic name-server lookup env vars so the start() 10004 branch stays testable
putenv('ROCKETMQ_NAMESRV_DOMAIN');
\RocketMQ\Client\Logger::setHandler(static function (string $line): void {
});

use RocketMQ\Client\BackPressureSendCallback;
use RocketMQ\Client\CheckForbiddenContext;
use RocketMQ\Client\CheckForbiddenHook;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\EndTransactionContext;
use RocketMQ\Client\EndTransactionHook;
use RocketMQ\Client\FairSemaphore;
use RocketMQ\Client\LocalTransactionState;
use RocketMQ\Client\CommunicationMode;
use RocketMQ\Client\MessageQueueSelector;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\NullSendCallback;
use RocketMQ\Client\RequestFutureHolder;
use RocketMQ\Client\RequestResponseFuture;
use RocketMQ\Client\SelectMessageQueueByHash;
use RocketMQ\Client\SelectMessageQueueByMachineRoom;
use RocketMQ\Client\SelectMessageQueueByRandom;
use RocketMQ\Client\SendCallback;
use RocketMQ\Client\SendCallbackImpl;
use RocketMQ\Client\SendResult;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\SendMessageContext;
use RocketMQ\Client\SendMessageHook;
use RocketMQ\Client\TransactionListener;
use RocketMQ\Client\TransactionMQProducer;
use RocketMQ\Client\TransactionSendResult;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\Exceptions\RemotingConnectException;
use RocketMQ\Client\Exceptions\RemotingSendRequestException;
use RocketMQ\Client\Exceptions\RemotingTimeoutException;
use RocketMQ\Client\Exceptions\RemotingTooMuchRequestException;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageBatch;
use RocketMQ\Common\MessageClientIdSetter;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MessageSysFlag;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\RecallMessageHandle;
use RocketMQ\Common\PermName;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\CheckTransactionStateRequestHeader;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RecallMessageResponseHeader;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\ReplyMessageRequestHeader;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\SendMessageResponseHeader;
use RocketMQ\Remoting\Protocol\TopicRouteData;

// ======================================================================
// Child-process fake broker: `php RunClientProducer.php --broker <logfile>`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    producerBrokerMain($argv[2] ?? '');
    exit(0);
}

function producerBrokerReadExact($conn, int $n): ?string
{
    $buf = '';
    while (strlen($buf) < $n) {
        $chunk = @fread($conn, $n - strlen($buf));
        if ($chunk === false || $chunk === '') {
            return null;
        }
        $buf .= $chunk;
    }
    return $buf;
}

function producerBrokerReadFrame($conn): ?RemotingCommand
{
    $head = producerBrokerReadExact($conn, 4);
    if ($head === null) {
        return null;
    }
    $total = RocketMQSerializable_unpackSignedInt($head);
    if ($total <= 0) {
        return null;
    }
    $rest = producerBrokerReadExact($conn, $total);
    if ($rest === null) {
        return null;
    }
    return RemotingCommand::decode($head . $rest);
}

function RocketMQSerializable_unpackSignedInt(string $raw): int
{
    $v = unpack('N', $raw)[1];
    return $v >= 0x80000000 ? $v - 0x100000000 : $v;
}

function producerBrokerRouteBody(string $brokerName, string $addr, int $queues = 2): string
{
    $qd = new QueueData($brokerName, $queues, $queues, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', $brokerName, [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

function producerBrokerLog(string $logFile, RemotingCommand $cmd): void
{
    $entry = [
        'code' => $cmd->code,
        'oneway' => $cmd->isOnewayRPC(),
        'ext' => $cmd->extFields,
    ];
    @file_put_contents($logFile, json_encode($entry, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
}

/**
 * Send-family responses: routed by topic name; returns [response, list of push frames]
 * (push frames are written right after the response).
 */
function producerBrokerSendResponse(RemotingCommand $cmd, string $selfAddr): array
{
    $ext = $cmd->extFields;
    $topic = (string) ($ext['b'] ?? $ext['topic'] ?? '');
    // Namespace-enabled producers (PHPNS) send topics shaped like "PHPNS%FailTopic" on the wire;
    // script branches match on the bare name, so strip the first prefix segment before comparing
    // (the log records the raw ext, assertions are unaffected).
    if (str_contains($topic, '%')) {
        $topic = substr($topic, strpos($topic, '%') + 1);
    }
    $oneway = $cmd->isOnewayRPC();

    if ($topic === 'FailTopic') {
        return [RemotingCommand::createResponseCommand(ResponseCode::MESSAGE_ILLEGAL, 'illegal body'), []];
    }
    if ($topic === 'RetryTopic' || $topic === 'AsyncFailTopic') {
        return [RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'system busy'), []];
    }
    if ($oneway) {
        return [null, []]; // oneway: log it but do not reply
    }

    // offsetMsgId uses valid 32 hex chars: ip=10.0.0.1, port=8000, offset=100
    $offsetMsgId = '0A0000011F4000000000000000000064';
    $header = new SendMessageResponseHeader(
        msgId: $offsetMsgId,
        queueId: (int) ($ext['e'] ?? 0),
        queueOffset: 100,
        transactionId: 'tx-9',
    );
    $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    $resp->customHeader = $header;

    $pushes = [];
    $props = MessageDecoder::string2MessageProperties((string) ($ext['i'] ?? ''));
    if ($topic === 'RequestTopic') {
        $cid = (string) ($props[MessageConst::PROPERTY_CORRELATION_ID] ?? 'no-cid');
        $pushHeader = new ReplyMessageRequestHeader(
            producerGroup: (string) ($ext['a'] ?? ''),
            topic: MixAll::getReplyTopic('DefaultCluster'),
            queueId: 0,
            properties: MessageDecoder::messageProperties2String([
                MessageConst::PROPERTY_CORRELATION_ID => $cid,
                MessageConst::PROPERTY_MESSAGE_TYPE => 'reply',
            ]),
            bornHost: '127.0.0.1:1',
            storeHost: '127.0.0.1:1',
            storeTimestamp: 1000,
        );
        $push = RemotingCommand::createRequestCommand(RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT, $pushHeader);
        $push->body = 'reply-body';
        $pushes[] = $push;
    }
    if ($topic === 'TransCheckTopic' && ($props[MessageConst::PROPERTY_TRANSACTION_PREPARED] ?? '') === 'true') {
        // Transaction check push: CHECK_TRANSACTION_STATE(39), body is the fully encoded MessageExt
        $m = new MessageExt(topic: $topic, body: 'trans-body');
        $m->putProperty(MessageConst::PROPERTY_PRODUCER_GROUP, (string) ($ext['a'] ?? ''));
        $m->putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, 'uniq-trans-1');
        $m->setMsgId('MSG-EXT-1');
        $push = RemotingCommand::createRequestCommand(RequestCode::CHECK_TRANSACTION_STATE, new CheckTransactionStateRequestHeader(
            topic: $topic,
            tranStateTableOffset: 7,
            commitLogOffset: 99,
            msgId: 'uniq-trans-1',
            transactionId: 'tx-9',
            offsetMsgId: $offsetMsgId,
            bname: 'broker-a',
        ));
        $push->body = MessageDecoder::encodeMessageExt($m);
        $pushes[] = $push;
    }
    return [$resp, $pushes];
}

function producerBrokerHandleFrame($conn, RemotingCommand $cmd, string $logFile, string $selfAddr): void
{
    producerBrokerLog($logFile, $cmd);
    $resp = null;
        $pushes = [];
        switch ($cmd->code) {
            case RequestCode::GET_ROUTEINFO_BY_TOPIC: {
                $topic = (string) ($cmd->extFields['topic'] ?? '');
                // NoRouteTopic and TBW102 (the fallback default topic) both answer NOT_FOUND:
                // per protocol semantics the client, after the direct lookup fails, also falls back
                // to the TBW102 route (the isDefault branch of tryToFindTopicPublishInfo). If the
                // mock also returned a route for TBW102, the 10005 for NoRouteTopic would never fire.
                if ($topic === 'NoRouteTopic' || $topic === MixAll::DEFAULT_TOPIC) {
                    $resp = RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST, 'no route');
                } elseif ($topic === 'GhostTopic') {
                    // a broker that can never be connected to (connection refused)
                    $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                    $resp->body = producerBrokerRouteBody('broker-ghost', '127.0.0.1:1', 2);
                } else {
                    $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                    $resp->body = producerBrokerRouteBody('broker-a', $selfAddr, 2);
                }
                break;
            }
            case RequestCode::SEND_MESSAGE:
            case RequestCode::SEND_MESSAGE_V2:
            case RequestCode::SEND_BATCH_MESSAGE:
            case RequestCode::SEND_REPLY_MESSAGE_V2: {
                [$resp, $pushes] = producerBrokerSendResponse($cmd, $selfAddr);
                // Decompress one layer of a compressed message and log the length
                // (lets the parent assert "there is no zlib(zlib(x))")
                if ($resp !== null && (int) ($cmd->extFields['f'] ?? 0) & MessageSysFlag::COMPRESSED_FLAG) {
                    $decoded = @gzuncompress((string) $cmd->body);
                    @file_put_contents(
                        $logFile,
                        json_encode(['code' => -1, 'decodedLen' => $decoded === false ? -1 : strlen($decoded)]) . "\n",
                        FILE_APPEND
                    );
                }
                break;
            }
            case RequestCode::HEART_BEAT:
            case RequestCode::UNREGISTER_CLIENT:
            case RequestCode::CHECK_CLIENT_CONFIG:
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                break;
            case RequestCode::RECALL_MESSAGE:
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->customHeader = new RecallMessageResponseHeader(msgId: 'recall-uniq-1');
                break;
            case RequestCode::GET_MAX_OFFSET:
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->extFields['offset'] = '777';
                break;
            case RequestCode::GET_MIN_OFFSET:
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->extFields['offset'] = '11';
                break;
            case RequestCode::GET_EARLIEST_MSG_STORETIME:
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->extFields['timestamp'] = '12345';
                break;
            default:
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                break;
        }
        if ($resp !== null) {
            $resp->opaque = $cmd->opaque;
            @fwrite($conn, $resp->encode());
        }
        foreach ($pushes as $push) {
            @fwrite($conn, $push->encode());
        }
}

/**
 * Take one complete frame from the buffer (ROCKETMQ frame = 4-byte total length + body).
 * Returns [raw frame bytes, decoded RemotingCommand]; null when incomplete.
 *
 * @return array{0: string, 1: RemotingCommand}|null
 */
function producerBrokerTakeFrame(string &$buf): ?array
{
    if (strlen($buf) < 4) {
        return null;
    }
    $total = RocketMQSerializable_unpackSignedInt(substr($buf, 0, 4));
    if ($total <= 0) {
        // invalid length: clear the buffer to avoid an infinite loop
        $buf = '';
        return null;
    }
    if (strlen($buf) < 4 + $total) {
        return null;
    }
    $raw = substr($buf, 0, 4 + $total);
    $buf = substr($buf, 4 + $total);
    return [$raw, RemotingCommand::decode($raw)];
}

function producerBrokerMain(string $logFile): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "broker: cannot listen: {$errstr}\n");
        return;
    }
    $addr = (string) stream_socket_get_name($server, false);
    fwrite(STDOUT, $addr . "\n");
    fflush(STDOUT);
    fwrite(STDERR, json_encode(['event' => 'bound', 'addr' => $addr]) . "\n");

    /**
     * Multi-connection event loop. The old implementation was "single-connection blocking": while
     * the parent kept its connection open, the child stayed stuck in fread and never returned to
     * accept(), so on Windows new connections were refused outright. Here stream_select watches
     * the server plus all open connections simultaneously.
     *
     * @var array<int, resource> $conns
     * @var array<int, string> $bufs
     */
    $conns = [];
    $bufs = [];

    while (true) {
        $read = array_values($conns);
        $read[] = $server;
        $w = null;
        $e = null;
        if (@stream_select($read, $w, $e, 5) === false) {
            break;
        }
        foreach ($read as $r) {
            if ($r === $server) {
                $conn = @stream_socket_accept($server, 0);
                if ($conn !== false) {
                    stream_set_blocking($conn, false);
                    $conns[(int) $conn] = $conn;
                    $bufs[(int) $conn] = '';
                    fwrite(STDERR, json_encode(['event' => 'accepted']) . "\n");
                }
                continue;
            }
            $key = (int) $r;
            $chunk = @fread($r, 65536);
            if ($chunk === false || ($chunk === '' && feof($r))) {
                unset($conns[$key], $bufs[$key]);
                @fclose($r);
                fwrite(STDERR, json_encode(['event' => 'eof']) . "\n");
                continue;
            }
            $bufs[$key] .= $chunk;
            while (true) {
                $frame = producerBrokerTakeFrame($bufs[$key]);
                if ($frame === null) {
                    break;
                }
                producerBrokerHandleFrame($r, $frame[1], $logFile, $addr);
            }
        }
    }
    @fclose($server);
}

// ======================================================================
// Child-process fake broker handle
// ======================================================================

final class ProducerFakeBroker
{
    /** @var resource */
    private $proc;
    /** @var array<int, resource> */
    private array $pipes;
    public string $addr;
    private string $logFile;

    private function __construct($proc, array $pipes, string $addr, string $logFile)
    {
        $this->proc = $proc;
        $this->pipes = $pipes;
        $this->addr = $addr;
        $this->logFile = $logFile;
    }

    public static function start(): self
    {
        $logFile = tempnam(sys_get_temp_dir(), 'rmq-producer-log-');
        $errFile = $logFile . '.err';
        $cmd = [PHP_BINARY, __FILE__, '--broker', $logFile];
        $desc = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['file', $errFile, 'a']];
        $proc = proc_open($cmd, $desc, $pipes);
        if (!is_resource($proc)) {
            throw new \RuntimeException('cannot spawn fake broker');
        }
        $line = fgets($pipes[1]);
        $addr = $line === false ? '' : trim($line);
        if ($addr === '') {
            $err = (string) file_get_contents($errFile);
            proc_terminate($proc);
            proc_close($proc);
            throw new \RuntimeException('fake broker did not report address: ' . $err);
        }
        return new self($proc, $pipes, $addr, $logFile);
    }

    /** @return list<array<string,mixed>> */
    public function logEntries(): array
    {
        $out = [];
        foreach (file($this->logFile) ?: [] as $line) {
            $line = trim($line);
            if ($line !== '') {
                $out[] = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            }
        }
        return $out;
    }

    /** Number of log entries for a request code (optionally filtered by topic). */
    public function isAlive(): bool
    {
        return is_resource($this->proc) && proc_get_status($this->proc)['running'];
    }
    public function logCount(int $code, ?string $topic = null, ?string $topicKey = 'b'): int
    {
        $n = 0;
        foreach ($this->logEntries() as $e) {
            if (($e['code'] ?? null) !== $code) {
                continue;
            }
            if ($topic !== null) {
                $t = $e['ext'][$topicKey] ?? ($e['ext']['topic'] ?? null);
                if ($t !== $topic) {
                    continue;
                }
            }
            $n++;
        }
        return $n;
    }

    public function stop(): void
    {
        foreach ($this->pipes as $p) {
            if (is_resource($p)) {
                @fclose($p);
            }
        }
        if (is_resource($this->proc)) {
            @proc_terminate($this->proc);
            @proc_close($this->proc);
        }
        @unlink($this->logFile);
    }
}

// ======================================================================
// Test hooks / listeners
// ======================================================================

final class RecordingSendHook implements SendMessageHook
{
    public int $before = 0;
    public int $after = 0;
    /** @var list<SendMessageContext> */
    public array $afterContexts = [];

    public function hookName(): string
    {
        return 'RecordingSendHook';
    }

    public function sendMessageBefore(SendMessageContext $context): void
    {
        $this->before++;
    }

    public function sendMessageAfter(SendMessageContext $context): void
    {
        $this->after++;
        $this->afterContexts[] = $context;
    }
}

final class ThrowingForbiddenHook implements CheckForbiddenHook
{
    public int $calls = 0;

    public function hookName(): string
    {
        return 'ThrowingForbiddenHook';
    }

    public function checkForbidden(CheckForbiddenContext $context): void
    {
        $this->calls++;
        throw new MQClientException('forbidden by test hook');
    }
}

final class RecordingEndTransactionHook implements EndTransactionHook
{
    public int $calls = 0;
    /** @var list<EndTransactionContext> */
    public array $contexts = [];

    public function hookName(): string
    {
        return 'RecordingEndTransactionHook';
    }

    public function endTransaction(EndTransactionContext $context): void
    {
        $this->calls++;
        $this->contexts[] = $context;
    }
}

final class ScriptedTransactionListener implements TransactionListener
{
    public function __construct(public LocalTransactionState $localState = LocalTransactionState::COMMIT_MESSAGE)
    {
    }

    public LocalTransactionState $checkState = LocalTransactionState::COMMIT_MESSAGE;
    public int $checkCalls = 0;
    public ?MessageExt $lastChecked = null;

    public function executeLocalTransaction(Message $msg, mixed $arg): LocalTransactionState
    {
        return $this->localState;
    }

    public function checkLocalTransaction(MessageExt $msg): LocalTransactionState
    {
        $this->checkCalls++;
        $this->lastChecked = $msg;
        return $this->checkState;
    }
}

/** Slot that collects async callback results. */
final class CallbackSlot implements SendCallback
{
    public ?SendResult $result = null;
    public ?\Throwable $error = null;
    public int $calls = 0;

    public function onSuccess(?SendResult $sendResult): void
    {
        $this->calls++;
        $this->result = $sendResult;
    }

    public function onException(\Throwable $e): void
    {
        $this->calls++;
        $this->error = $e;
    }
}

// ======================================================================
// Test runner
// ======================================================================

final class RunClientProducer
{
    private int $passed = 0;
    /** @var list<string> */
    private array $failures = [];

    public function check(bool $cond, string $label): void
    {
        if ($cond) {
            $this->passed++;
        } else {
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$label}\n");
        }
    }

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        if ($expected === $actual) {
            $this->passed++;
        } else {
            $detail = sprintf("%s\n  expected: %s\n  actual:   %s", $label, var_export($expected, true), var_export($actual, true));
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$detail}\n");
        }
    }

    /**
     * @param callable(): mixed $fn
     * @param class-string<\Throwable> $exceptionClass
     */
    public function checkThrows(callable $fn, string $exceptionClass, string $label): void
    {
        try {
            $fn();
            $this->check(false, "{$label} (no exception thrown)");
        } catch (\Throwable $e) {
            $this->check($e instanceof $exceptionClass, sprintf('%s (got %s: %s)', $label, get_class($e), $e->getMessage()));
        }
    }

    public function summary(): int
    {
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }

    // ================================================================ 1. Constants and defaults

    private function testConstantsAndDefaults(): void
    {
        $p = new DefaultMQProducer('GID-defs');
        $this->checkSame('message\'s topic not equal mq\'s topic', DefaultMQProducer::PINNED_TOPIC_MISMATCH_SYNC, 'pinned guard sync message');
        $this->checkSame('Topic of the message does not match its target message queue', DefaultMQProducer::PINNED_TOPIC_MISMATCH_ASYNC, 'pinned guard async message');
        $this->checkSame(3000, $p->sendMsgTimeout, 'default sendMsgTimeout=3000');
        $this->checkSame(1024 * 4, $p->compressMsgBodyOverHowmuch, 'default compressMsgBodyOverHowmuch=4096');
        $this->checkSame(5, $p->compressLevel, 'default compressLevel=5');
        $this->checkSame(MessageSysFlag::ZLIB_TYPE, $p->compressType, 'default compress type ZLIB');
        $this->checkSame(2, $p->retryTimesWhenSendFailed, 'default sync retries 2');
        $this->checkSame(2, $p->retryTimesWhenSendAsyncFailed, 'default async retries 2');
        $this->checkSame(false, $p->retryAnotherBrokerWhenNotStoreOk, 'default: do not retry on another broker for non-OK');
        $this->checkSame(-1, $p->sendMsgMaxTimeoutPerRequest, 'default per-request timeout -1 (unlimited)');
        $this->checkSame(1024 * 1024 * 4, $p->maxMessageSize, 'default maxMessageSize=4MB');
        $this->checkSame(MixAll::DEFAULT_TOPIC, $p->createTopicKey, 'default createTopicKey=TBW102');
        $this->checkSame(MixAll::DEFAULT_TOPIC_QUEUE_NUMS, $p->defaultTopicQueueNums, 'default defaultTopicQueueNums=4');
        $this->checkSame(30000, $p->heartbeatIntervalMillis, 'default heartbeat interval 30000');
        $this->checkSame(30000, $p->pollNameServerInterval, 'default route refresh interval 30000');
        $this->checkSame(false, $p->sendLatencyFaultEnable, 'default latency fault tolerance off');
        $this->checkSame(false, $p->enableBackpressureForAsyncMode, 'default backpressure off');
        $this->checkSame(1024, $p->backPressureForAsyncSendNum, 'default backpressure count 1024');
        $this->checkSame(100 * 1024 * 1024, $p->backPressureForAsyncSendSize, 'default backpressure bytes 100MB');
        $this->checkSame(1024, $p->getSemaphoreAsyncSendNumAvailablePermits(), 'count semaphore initial 1024');
        $this->checkSame(100 * 1024 * 1024, $p->getSemaphoreAsyncSendSizeAvailablePermits(), 'byte semaphore initial 100MB');
        $this->checkSame(50000, $p->asyncSenderQueueCapacity, 'default async queue 50000');
        $this->checkSame(0, $p->clientCallbackExecutorThreads, 'default callback threads 0 (per CPU count)');
        $this->checkSame(10, $p->traceMsgBatchNum, 'default trace batch size 10');
        $this->checkSame(false, $p->enableTrace, 'default trace off');
        $this->checkSame(RequestResponseFuture::DEFAULT_REQUEST_TIMEOUT_MILLIS, $p->requestTimeout, 'default request timeout 3000');
        $this->checkSame(3000, $p->requestTimeout, 'DEFAULT_REQUEST_TIMEOUT_MILLIS=3000');
        $this->checkSame(8, count($p->retryResponseCodes), 'default retryable response codes: 8');
        $this->checkSame(true, $p->isRetryResponseCode(1), 'SYSTEM_ERROR retryable');
        $this->checkSame(true, $p->isRetryResponseCode(17), 'TOPIC_NOT_EXIST retryable');
        $this->checkSame(false, $p->isRetryResponseCode(13), 'MESSAGE_ILLEGAL not retryable');
        $this->checkSame(false, $p->isRetryResponseCode(null), 'null code not retryable');
        $p->addRetryResponseCode(13);
        $this->checkSame(true, $p->isRetryResponseCode(13), 'addRetryResponseCode takes effect');

        $this->checkSame(0, LocalTransactionState::COMMIT_MESSAGE->value, 'COMMIT_MESSAGE ordinal 0');
        $this->checkSame(1, LocalTransactionState::ROLLBACK_MESSAGE->value, 'ROLLBACK_MESSAGE ordinal 1');
        $this->checkSame(2, LocalTransactionState::UNKNOW->value, 'UNKNOW ordinal 2');

        // LocalTransactionState flag constants
        $this->checkSame(8, MessageSysFlag::TRANSACTION_COMMIT_TYPE, 'TRANSACTION_COMMIT_TYPE=8');
        $this->checkSame(12, MessageSysFlag::TRANSACTION_ROLLBACK_TYPE, 'TRANSACTION_ROLLBACK_TYPE=12');
        $this->checkSame(0, MessageSysFlag::TRANSACTION_NOT_TYPE, 'TRANSACTION_NOT_TYPE=0');
        $this->checkSame(4, MessageSysFlag::TRANSACTION_PREPARED_TYPE, 'TRANSACTION_PREPARED_TYPE=4');
    }

    // ================================================================ 2. Constructor / config setters

    private function testConstructorAndSetters(): void
    {
        $this->checkThrows(fn() => new DefaultMQProducer('  '), MQClientException::class, 'blank group name constructor throws MQClientException');
        $this->checkThrows(fn() => new DefaultMQProducer(null), MQClientException::class, 'null group name constructor throws MQClientException');

        $p = new DefaultMQProducer('GID-cfg', null, 'NS-CFG');
        $p->setNamesrvAddr(' 127.0.0.1:1 ; 127.0.0.2:2 ;');
        $this->checkSame(['127.0.0.1:1', '127.0.0.2:2'], $p->nameServerAddrs, 'setNamesrvAddr splits on semicolons and trims whitespace');
        $this->checkSame('127.0.0.1:1;127.0.0.2:2', $p->getNamesrvAddr(), 'getNamesrvAddr joins with semicolons');
        $p->setNameServerAddresses(['a:1']);
        $this->checkSame(['a:1'], $p->nameServerAddrs, 'setNameServerAddresses direct assignment');
        $p->setInstanceName('inst-1');
        $this->checkSame('inst-1', $p->instanceName, 'setInstanceName');
        $p->setUnitName('unit-x');
        $this->checkSame('unit-x', $p->getUnitName(), 'setUnitName/getUnitName');
        $p->setUnitMode(true);
        $this->checkSame(true, $p->isUnitMode(), 'setUnitMode/isUnitMode');
        $p->setEnableStreamRequestType(true);
        $this->checkSame(true, $p->enableStreamRequestType, 'setEnableStreamRequestType');
        $p->setMaxMessageSize(2048);
        $this->checkSame(2048, $p->maxMessageSize, 'setMaxMessageSize');
        $p->setSendMsgTimeout(1234);
        $this->checkSame(1234, $p->sendMsgTimeout, 'setSendMsgTimeout');
        $p->setRetryTimesWhenSendFailed(5);
        $this->checkSame(5, $p->retryTimesWhenSendFailed, 'setRetryTimesWhenSendFailed');
        $p->setSendMsgMaxTimeoutPerRequest(800);
        $this->checkSame(800, $p->getSendMsgMaxTimeoutPerRequest(), 'set/getSendMsgMaxTimeoutPerRequest');
        $p->setRetryAnotherBrokerWhenNotStoreOk(true);
        $this->checkSame(true, $p->isRetryAnotherBrokerWhenNotStoreOk(), 'set/isRetryAnotherBrokerWhenNotStoreOk');
        $p->setCompressMsgBodyOverHowmuch(64);
        $this->checkSame(64, $p->compressMsgBodyOverHowmuch, 'setCompressMsgBodyOverHowmuch');
        $p->setCompressLevel(1);
        $this->checkSame(1, $p->compressLevel, 'setCompressLevel');
        $p->setCompressType(MessageSysFlag::ZLIB_TYPE);
        $this->checkSame(MessageSysFlag::ZLIB_TYPE, $p->compressType, 'setCompressType');
        $p->setCreateTopicKey('MyTBW');
        $this->checkSame('MyTBW', $p->createTopicKey, 'setCreateTopicKey');
        $p->setDefaultTopicQueueNums(9);
        $this->checkSame(9, $p->defaultTopicQueueNums, 'setDefaultTopicQueueNums');
        $p->setSendLatencyFaultEnable(true);
        $this->checkSame(true, $p->sendLatencyFaultEnable, 'setSendLatencyFaultEnable propagates to strategy');
        $this->checkSame(true, $p->mqFaultStrategy->isSendLatencyFaultEnable(), 'MQFaultStrategy switch kept in sync');
        $p->setRequestTimeout(9999);
        $this->checkSame(9999, $p->requestTimeout, 'setRequestTimeout');
        $p->setTraceTopic('MY_TRACE');
        $this->checkSame('MY_TRACE', $p->traceTopic, 'setTraceTopic');
        $p->setTraceMsgBatchNum(20);
        $this->checkSame(20, $p->traceMsgBatchNum, 'setTraceMsgBatchNum');
        $p->setEnableTrace(true);
        $this->checkSame(true, $p->isEnableTrace(), 'set/isEnableTrace');
        $p->setAutoBatch(true);
        $this->checkSame(false, $p->getAutoBatch(), 'getAutoBatch stays false before start (short-circuits when not started)');
        $this->checkSame(0, $p->getBatchMaxDelayMs(), 'getBatchMaxDelayMs=0 when accumulator not built');
        $this->checkSame(0, $p->getBatchMaxBytes(), 'getBatchMaxBytes=0 when accumulator not built');
        $this->checkSame(0, $p->getTotalBatchMaxBytes(), 'getTotalBatchMaxBytes=0 when accumulator not built');

        // Backpressure setters (including floor values)
        $p->setEnableBackpressureForAsyncMode(true);
        $this->checkSame(true, $p->isEnableBackpressureForAsyncMode(), 'set/isEnableBackpressureForAsyncMode');
        $p->setBackPressureForAsyncSendNum(5);
        $this->checkSame(10, $p->getBackPressureForAsyncSendNum(), 'count limit raised to floor 10');
        $this->checkSame(10, $p->getSemaphoreAsyncSendNumAvailablePermits(), 'count semaphore follows the setter');
        $p->setBackPressureForAsyncSendSize(1024);
        $this->checkSame(1024 * 1024, $p->getBackPressureForAsyncSendSize(), 'byte limit raised to floor 1MB');
        $p->setBackPressureForAsyncSendNum(100);
        $this->checkSame(100, $p->getBackPressureForAsyncSendNum(), 'normal count limit applies directly');

        // Runtime capacity change semantics: total shifts, in-flight retained
        $sem = $this->privateOf($p, 'semaphoreAsyncSendNum');
        $sem->tryAcquire(30, 0); // 30 permits in flight
        $p->setBackPressureForAsyncSendNum(80);
        $this->checkSame(50, $p->getSemaphoreAsyncSendNumAvailablePermits(), 'after resize available = new total - in-flight');
        $sem->release(30);

        $this->checkThrows(fn() => $this->startedProducerSetGroup(), MQClientException::class, 'setProducerGroup after start throws');
    }

    private function startedProducerSetGroup(): void
    {
        $broker = ProducerFakeBroker::start();
        try {
            $p = $this->makeProducer($broker, 'GID-setgrp', 'SetGrpTopic', 'setgrp-inst');
            $p->start();
            $p->setProducerGroup('GID-other');
        } finally {
            if (isset($p)) {
                $p->shutdown();
            }
            $broker->stop();
        }
    }

    // ================================================================ 3. Module-level helpers

    private function testStaticHelpers(): void
    {
        // backPressurePermits: >floor passes through; ==floor / <floor clamp to the floor (permit lower bound)
        $this->checkSame(20, DefaultMQProducer::backPressurePermits(20, 10, 'x'), 'permits: 20>10 → 20');
        $this->checkSame(10, DefaultMQProducer::backPressurePermits(10, 10, 'x'), 'permits: ==floor also takes the else branch → 10');
        $this->checkSame(10, DefaultMQProducer::backPressurePermits(3, 10, 'x'), 'permits: 3<10 → 10');

        // backPressureMsgLen
        $empty = new Message('T', null);
        $this->checkSame(1, DefaultMQProducer::backPressureMsgLen($empty), 'msgLen: body=null → 1');
        $empty2 = new Message('T', '');
        $this->checkSame(1, DefaultMQProducer::backPressureMsgLen($empty2), 'msgLen: body=empty string → 1');
        $m3 = new Message('T', 'abcde');
        $this->checkSame(5, DefaultMQProducer::backPressureMsgLen($m3), 'msgLen: body length');
        $b1 = new Message('T', 'abcd');
        $b2 = new Message('T', 'ef');
        $batch = MessageBatch::generateFromList([$b1, $b2]);
        $this->checkSame(6, DefaultMQProducer::backPressureMsgLen($batch), 'msgLen: batch accumulates per message');
        $this->checkSame(11, DefaultMQProducer::backPressureMsgLen([$m3, $batch]), 'msgLen: list accumulation (5 + batch 6)');
        $emptyBatch = MessageBatch::generateFromList([new Message('T', null)]);
        $this->checkSame(1, DefaultMQProducer::backPressureMsgLen($emptyBatch), 'msgLen: all-empty batch → 1');

        // maxDelayValue
        $plain = new Message('T', 'b');
        $this->checkSame(0, DefaultMQProducer::maxDelayValue($plain), 'maxDelay: no delay property → 0');
        $d1 = new Message('T', 'b');
        $d1->putProperty('DELAY', '3');
        $this->checkSame(3, DefaultMQProducer::maxDelayValue($d1), 'maxDelay: DELAY=3');
        $d2 = new Message('T', 'b');
        $d2->putProperty('TIMER_DELIVER_MS', '5000');
        $this->checkSame(5000, DefaultMQProducer::maxDelayValue($d2), 'maxDelay: TIMER_DELIVER_MS');
        $d3 = new Message('T', 'b');
        $d3->putProperty('DELAY', '2');
        $d3->putProperty('TIMER_DELAY_SEC', '9');
        $this->checkSame(9, DefaultMQProducer::maxDelayValue($d3), 'maxDelay: takes the maximum');
        $this->checkThrows(function () use ($d3) {
            $d3->putProperty('TIMER_DELAY_MS', 'abc');
            DefaultMQProducer::maxDelayValue($d3);
        }, \InvalidArgumentException::class, 'maxDelay: invalid value throws directly (number parse failure throws)');

        // classifyAsyncFailure: three branches + non-remoting passed through
        $cost = 33;
        [$e1, $r1] = DefaultMQProducer::classifyAsyncFailure(new RemotingSendRequestException('1.2.3.4:1'), $cost);
        $this->check($e1 instanceof MQClientException, 'send request failed → MQClientException');
        $this->checkSame(true, $r1, 'send request failed → retryable');
        $this->check($e1->getPrevious() instanceof RemotingSendRequestException, 'wrapping preserves cause');
        [$e2, $r2] = DefaultMQProducer::classifyAsyncFailure(new RemotingTimeoutException('a', 100), $cost);
        $this->check($e2 instanceof MQClientException, 'timeout → MQClientException');
        $this->check(str_contains($e2->getMessage(), 'wait response timeout, cost=33'), 'timeout message includes cost');
        $this->checkSame(true, $r2, 'timeout → retryable');
        [$e3, $r3] = DefaultMQProducer::classifyAsyncFailure(new RemotingConnectException('a'), $cost);
        $this->checkSame('unknown reason', $e3->getMessage(), 'other RemotingException → unknown reason');
        $this->checkSame(true, $r3, 'connect → retryable');
        [$e4, $r4] = DefaultMQProducer::classifyAsyncFailure(new RemotingTooMuchRequestException('too much'), $cost);
        $this->checkSame('unknown reason', $e4->getMessage(), 'TooMuchRequest → unknown reason wrapping');
        $this->checkSame(false, $r4, 'TooMuchRequest → not retryable');
        $brokerErr = new MQBrokerException(1, 'busy');
        [$e5, $r5] = DefaultMQProducer::classifyAsyncFailure($brokerErr, $cost);
        $this->checkSame($brokerErr, $e5, 'MQBrokerException returned as-is (not wrapped)');
        $this->checkSame(false, $r5, 'MQBrokerException → not retryable (retryResponseCodes ignored)');
    }

    // ================================================================ 4. Selectors

    private function testSelectors(): void
    {
        $mqs = [
            new MessageQueue('T', 'broker-a', 0),
            new MessageQueue('T', 'broker-b', 1),
            new MessageQueue('T', 'broker-c', 2),
        ];
        $msg = new Message('T', 'b');

        $byHash = new SelectMessageQueueByHash();
        $this->checkThrows(fn() => $byHash->select([], $msg, 'x'), MQClientException::class, 'ByHash: throws on empty queue list');
        $picked = $byHash->select($mqs, $msg, 'abc');
        $this->check(in_array($picked, $mqs, true), 'ByHash: hits some queue');
        $this->checkSame($byHash->select($mqs, $msg, 'abc'), $byHash->select($mqs, $msg, 'abc'), 'ByHash: same arg yields same queue (deterministic)');
        $this->checkSame($mqs[1], $byHash->select($mqs, $msg, 1), 'ByHash: int arg=1 → queue 1 (abs(1)%3)');
        $this->checkSame($mqs[0], $byHash->select($mqs, $msg, null), 'ByHash: null arg → 0 → queue 0');
        $this->checkSame($mqs[2], $byHash->select($mqs, $msg, -2), 'ByHash: negative uses abs');

        $byRandom = new SelectMessageQueueByRandom();
        $this->checkThrows(fn() => $byRandom->select([], $msg, null), MQClientException::class, 'ByRandom: throws on empty queue list');
        for ($i = 0; $i < 8; $i++) {
            $q = $byRandom->select($mqs, $msg, null);
            $this->check(in_array($q, $mqs, true), 'ByRandom: result within the queue set');
        }

        $byRoom = new SelectMessageQueueByMachineRoom();
        $roomMqs = [
            new MessageQueue('T', 'bj-1', 0),
            new MessageQueue('T', 'sh-1', 1),
        ];
        $this->checkThrows(fn() => $byRoom->select([], $msg, null), MQClientException::class, 'ByMachineRoom: throws on empty queue list');
        $this->checkSame('sh-1', $byRoom->select($roomMqs, $msg, 'sh')->brokerName, 'ByMachineRoom: prefix match sh');
        $this->checkSame('bj-1', $byRoom->select($roomMqs, $msg, 'bj')->brokerName, 'ByMachineRoom: prefix match bj');
        $this->checkSame('bj-1', $byRoom->select($roomMqs, $msg, 'gz')->brokerName, 'ByMachineRoom: no match falls back to first');
        $this->check($byRoom instanceof MessageQueueSelector, 'selectors implement the MessageQueueSelector interface');
    }

    // ================================================================ 5. Callback wrappers / transaction results

    private function testCallbacksAndResults(): void
    {
        // SendCallbackImpl
        $got = null;
        $err = null;
        $cb = new SendCallbackImpl(
            static function (?SendResult $r) use (&$got): void {
                $got = $r;
            },
            static function (\Throwable $e) use (&$err): void {
                $err = $e;
            }
        );
        $sr = new SendResult(SendStatus::SEND_OK, 'm-1');
        $cb->onSuccess($sr);
        $this->checkSame($sr, $got, 'SendCallbackImpl: onSuccess forwarded');
        $ex = new MQClientException('boom');
        $cb->onException($ex);
        $this->checkSame($ex, $err, 'SendCallbackImpl: onException forwarded');
        $cb->onSuccess(null);
        $this->checkSame(null, $got, 'SendCallbackImpl: null result passed through');
        (new SendCallbackImpl())->onSuccess($sr); // empty implementation does not throw
        $this->check(true, 'SendCallbackImpl: empty callbacks are safe');

        // NullSendCallback + future
        $future = new RequestResponseFuture('cid-1', 3000);
        (new NullSendCallback($future))->onSuccess($sr);
        $this->checkSame(true, $future->sendRequestOk, 'NullSendCallback: onSuccess → sendRequestOk=true');
        $future2 = new RequestResponseFuture('cid-2', 3000);
        $cause = new MQClientException('send fail');
        (new NullSendCallback($future2))->onException($cause);
        $this->checkSame(false, $future2->sendRequestOk, 'NullSendCallback: onException → sendRequestOk=false');
        $this->checkSame(null, $future2->responseMsg, 'NullSendCallback: putResponseMessage(null)');
        $this->checkSame($cause, $future2->cause, 'NullSendCallback: cause written back');
        (new NullSendCallback())->onSuccess(null);
        $this->check(true, 'NullSendCallback: safe without a future');

        // BackPressureSendCallback: release only what was actually acquired
        $semNum = new FairSemaphore(10);
        $semSize = new FairSemaphore(1024);
        $delegate = new CallbackSlot();
        $gated = new BackPressureSendCallback($delegate, $semNum, $semSize, 7);
        $this->check($semNum->tryAcquire(1, 0), 'backpressure: count permit acquired');
        $gated->numAcquired = true;
        $this->check($semSize->tryAcquire(7, 0), 'backpressure: byte permit acquired');
        $gated->sizeAcquired = true;
        $this->checkSame(9, $semNum->availablePermits(), 'backpressure: after deduction count is 9');
        $this->checkSame(1017, $semSize->availablePermits(), 'backpressure: after deduction bytes are 1017');
        $gated->onSuccess($sr);
        $this->checkSame(10, $semNum->availablePermits(), 'backpressure: count fully released on success');
        $this->checkSame(1024, $semSize->availablePermits(), 'backpressure: bytes fully released on success (body=7 included)');
        $this->checkSame(1, $delegate->calls, 'backpressure: delegate callback invoked exactly once');
        $this->checkSame($sr, $delegate->result, 'backpressure: delegate received the result');
        $this->checkSame(true, $gated->done, 'backpressure: done flag set');
        // Duplicate callbacks do not double-release (hardening: release only once)
        $gated->onException(new MQClientException('late'));
        $this->checkSame(10, $semNum->availablePermits(), 'backpressure: duplicate callback does not inflate count');
        $this->checkSame(1024, $semSize->availablePermits(), 'backpressure: duplicate callback does not inflate bytes');
        $this->checkSame(2, $delegate->calls, 'backpressure: delegate still called again (forwards directly when permits are short)');

        // Only count acquired, bytes not: release only the count
        $semNum2 = new FairSemaphore(10);
        $semSize2 = new FairSemaphore(1024);
        $gated2 = new BackPressureSendCallback(new CallbackSlot(), $semNum2, $semSize2, 7);
        $this->check($semNum2->tryAcquire(1, 0), 'backpressure2: count permit acquired');
        $gated2->numAcquired = true;
        $gated2->onException(new RemotingTooMuchRequestException('size timeout'));
        $this->checkSame(10, $semNum2->availablePermits(), 'backpressure2: count released');
        $this->checkSame(1024, $semSize2->availablePermits(), 'backpressure2: bytes never acquired are not released (total unchanged)');

        // TransactionSendResult
        $mq = new MessageQueue('T', 'b', 1);
        $base = new SendResult(SendStatus::SEND_OK, 'm-2', $mq, 42, 'tx-1', 'off-1', 'Region-A');
        $tsr = new TransactionSendResult($base, LocalTransactionState::COMMIT_MESSAGE);
        $this->checkSame(SendStatus::SEND_OK, $tsr->getSendStatus(), 'TransactionSendResult: status delegated');
        $this->checkSame('m-2', $tsr->getMsgId(), 'TransactionSendResult: msgId delegated');
        $this->checkSame($mq, $tsr->getMessageQueue(), 'TransactionSendResult: mq delegated');
        $this->checkSame(42, $tsr->getQueueOffset(), 'TransactionSendResult: queueOffset delegated');
        $this->checkSame('tx-1', $tsr->getTransactionId(), 'TransactionSendResult: transactionId delegated');
        $this->checkSame('off-1', $tsr->getOffsetMsgId(), 'TransactionSendResult: offsetMsgId delegated');
        $this->checkSame('Region-A', $tsr->getRegionId(), 'TransactionSendResult: regionId delegated');
        $this->checkSame(LocalTransactionState::COMMIT_MESSAGE, $tsr->getLocalTransactionState(), 'TransactionSendResult: local transaction state');
        $tsr->setLocalTransactionState(LocalTransactionState::ROLLBACK_MESSAGE);
        $this->checkSame(LocalTransactionState::ROLLBACK_MESSAGE, $tsr->getLocalTransactionState(), 'TransactionSendResult: set state');
        $tsr->setTransactionId('tx-2');
        $this->checkSame('tx-2', $tsr->getTransactionId(), 'TransactionSendResult: setTransactionId delegated');
        $this->checkSame($base, $tsr->getSendResult(), 'TransactionSendResult: getSendResult returns the underlying result');
    }

    // ================================================================ 6. Lifecycle / validation

    private function makeProducer(ProducerFakeBroker $broker, string $group, string $topic, string $instanceName): DefaultMQProducer
    {
        $p = new DefaultMQProducer($group, null, '', [$topic]);
        $p->setInstanceName($instanceName);
        $p->setNameServerAddresses([$broker->addr]);
        $p->setSendMsgTimeout(3000);
        return $p;
    }

    private function testLifecycleValidation(): void
    {
        // Not started
        $p = new DefaultMQProducer('GID-lc');
        $p->setNamesrvAddr('127.0.0.1:59999');
        $this->checkThrows(fn() => $p->send(new Message('T', 'b')), MQClientException::class, 'send without start throws not started');
        $this->checkThrows(fn() => $p->sendAsync(new Message('T', 'b'), new CallbackSlot()), MQClientException::class, 'async send without start throws not started');
        $this->checkThrows(fn() => $p->sendOneway(new Message('T', 'b')), MQClientException::class, 'oneway without start throws not started');
        $p->shutdown(); // shutdown without start returns silently (no throw)
        $this->check(true, 'shutdown without start is silent (no throw)');

        // Group name validation (start order: withNamespace → checkGroup → DEFAULT_PRODUCER guard)
        $p2 = new DefaultMQProducer(MixAll::DEFAULT_PRODUCER_GROUP);
        $p2->setNamesrvAddr('127.0.0.1:59999');
        $this->checkThrows(fn() => $p2->start(), MQClientException::class, 'start: DEFAULT_PRODUCER group is rejected');

        // No name server address (dynamic lookup was cleared in the file header)
        $p3 = new DefaultMQProducer('GID-nons');
        $this->checkThrows(function () use ($p3): void {
            $p3->start();
        }, MQClientException::class, 'start: no address and no address server → throws');
        try {
            $p3->start();
        } catch (MQClientException $e) {
            $this->checkSame(10004, $e->responseCode, 'start: no-address failure code 10004');
        }

        // Message validation (purely local, no start needed)
        $p4 = new DefaultMQProducer('GID-lc2');
        $p4->setSendMsgTimeout(100);
        $blank = new Message('   ', 'b');
        $this->checkThrows(fn() => $this->invokePrivateCheckMessage($p4, $blank), MQClientException::class, 'checkMessage: blank topic throws');
        $oversize = new Message('T', str_repeat('x', 5 * 1024 * 1024));
        $this->checkThrows(fn() => $this->invokePrivateCheckMessage($p4, $oversize), MQClientException::class, 'checkMessage: oversized body throws');
        $nullBody = new Message('T', null);
        $this->checkThrows(fn() => $this->invokePrivateCheckMessage($p4, $nullBody), MQClientException::class, 'checkMessage: null body throws');
    }

    private function invokePrivateCheckMessage(DefaultMQProducer $p, Message $msg): void
    {
        $m = new \ReflectionMethod(DefaultMQProducer::class, 'checkMessage');
        $m->setAccessible(true);
        $m->invoke($p, $msg);
    }

    // ================================================================ 7. Sync send (real network)

    private function testSyncSend(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-sync', 'SyncTopic', 'sync-inst-1');
        $hook = new RecordingSendHook();
        $p->registerSendMessageHook($hook);
        $p->start();

        $msg = new Message('SyncTopic', 'sync-body');
        $msg->setKeys('k1');
        $result = $p->send($msg);
        $this->check($result instanceof SendResult, 'sync send returns SendResult');
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, 'sync send SEND_OK');
        $this->checkSame('0A0000011F4000000000000000000064', $result->offsetMsgId, 'offsetMsgId from the response header');
        $this->checkSame(100, $result->queueOffset, 'queueOffset from the response header');
        $this->checkSame('tx-9', $result->transactionId, 'transactionId passed through');
        $this->checkSame('broker-a', $result->messageQueue->brokerName ?? '', 'result mq.brokerName');
        $this->check(is_string($result->msgId) && $result->msgId !== '', 'msgId = client UNIQ_KEY');
        $this->checkSame(MixAll::DEFAULT_TRACE_REGION_ID, $result->regionId, 'regionId defaults to DefaultRegion');
        $this->checkSame(1, $hook->before, 'SendMessageHook.before invoked once');
        $this->checkSame(1, $hook->after, 'SendMessageHook.after invoked once');
        $this->checkSame(SendStatus::SEND_OK, $hook->afterContexts[0]->sendResult->sendStatus ?? null, 'after context carries the result');
        $this->checkSame(CommunicationMode::SYNC, $hook->afterContexts[0]->communicationMode, 'after context communicationMode=SYNC');

        // The broker actually received SEND_MESSAGE_V2
        $this->checkSame(1, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'SyncTopic'), 'broker received one SEND_MESSAGE_V2');
        $entries = $broker->logEntries();
        $send = null;
        foreach ($entries as $e) {
            if (($e['code'] ?? 0) === RequestCode::SEND_MESSAGE_V2 && ($e['ext']['b'] ?? '') === 'SyncTopic') {
                $send = $e;
                break;
            }
        }
        $this->checkSame('GID-sync', $send['ext']['a'] ?? '', 'send header a=producerGroup');
        $this->checkSame('SyncTopic', $send['ext']['b'] ?? '', 'send header b=topic');
        $this->checkSame(MixAll::DEFAULT_TOPIC, $send['ext']['c'] ?? '', 'send header c=defaultTopic (configurable)');
        $this->checkSame('4', $send['ext']['d'] ?? '', 'send header d=defaultTopicQueueNums (configurable)');
        $this->checkSame('false', $send['ext']['k'] ?? '', 'send header k=unitMode=false');

        // Send the same message again (restore semantics: no double compression / no namespace residue)
        $again = $p->send($msg);
        $this->checkSame(SendStatus::SEND_OK, $again->sendStatus, 'second send of the same message OK');

        // Pinned send
        $mq = new MessageQueue('SyncTopic', 'broker-a', 1);
        $pinned = $p->send(new Message('SyncTopic', 'pinned-body'), 3000, $mq);
        $this->checkSame(1, $pinned->messageQueue->queueId ?? -1, 'pinned send queueId=1');

        // Pinned topic mismatch → sync message
        $badMq = new MessageQueue('OtherTopic', 'broker-a', 0);
        $this->checkThrows(
            fn() => $p->send(new Message('SyncTopic', 'x'), 3000, $badMq),
            MQClientException::class,
            'pinned topic mismatch throws MQClientException'
        );
        try {
            $p->send(new Message('SyncTopic', 'x'), 3000, $badMq);
        } catch (MQClientException $e) {
            $this->checkSame(DefaultMQProducer::PINNED_TOPIC_MISMATCH_SYNC, $e->getMessage(), 'pinned guard uses the sync message');
        }

        // Trace context msgType classification
        $hook2 = new RecordingSendHook();
        $p->registerSendMessageHook($hook2);
        $delayMsg = new Message('SyncTopic', 'delay-body');
        $delayMsg->putProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL, '2');
        $p->send($delayMsg);
        $ctx = $hook2->afterContexts[0] ?? null;
        $this->check($ctx !== null && 'DELAY_MSG' === $ctx->msgType->name, 'hook context msgType=Delay_Msg (with DELAY)');
        $hook3 = new RecordingSendHook();
        $p->registerSendMessageHook($hook3);
        $p->send(new Message('SyncTopic', 'plain'));
        $ctx3 = $hook3->afterContexts[0] ?? null;
        $this->check($ctx3 !== null && 'NORMAL_MSG' === $ctx3->msgType->name, 'hook context msgType=Normal_Msg');

        $p->shutdown();
        MQClientInstance::removeInstance('sync-inst-1-cid');
        $this->check(true, 'sync send suite shutdown complete');
    }

    // ================================================================ 8. Compression and restore semantics

    private function testCompressionAndRestore(ProducerFakeBroker $broker): void
    {
        $p = new DefaultMQProducer('GID-restore', null, 'PHPNS');
        $p->setInstanceName('restore-inst-1');
        $p->setNameServerAddresses([$broker->addr]);
        $p->start();

        // 1) Compression triggered + body/topic restored + no double compression
        $bigBody = str_repeat('A', 5000);
        $msg = new Message('SyncTopic', $bigBody);
        $result = $p->send($msg);
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, 'compressed message send OK');
        $this->checkSame('SyncTopic', $msg->getTopic(), 'restore: topic stripped of namespace (PHPNS%SyncTopic → SyncTopic)');
        $this->checkSame($bigBody, $msg->getBody(), 'restore: body swapped back to the pre-compression copy');

        // Broker side sysFlag carries the compression bit, and one decompression yields the original
        // (proving there is no zlib(zlib(x)))
        $sends = 0;
        foreach ($broker->logEntries() as $e) {
            $topic = $e['ext']['b'] ?? '';
            if (($e['code'] ?? 0) === RequestCode::SEND_MESSAGE_V2 && $topic === 'PHPNS%SyncTopic') {
                $sends++;
                $this->checkSame('769', (string) ($e['ext']['f'] ?? ''), 'compressed sysFlag=0x301(COMPRESSED|ZLIB)');
            }
            if (($e['code'] ?? -99) === -1) {
                $this->checkSame(5000, $e['decodedLen'] ?? -1, 'broker decodes one layer to original length 5000 (no double compression)');
            }
        }
        $this->checkSame(1, $sends, 'namespaced topic reached the broker');

        // 2) Failure path also restores (finally)
        $bodyBefore = 'fail-body';
        $failMsg = new Message('FailTopic', $bodyBefore);
        try {
            $p->send($failMsg);
            $this->check(false, 'FailTopic send should throw');
        } catch (MQBrokerException $e) {
            $this->checkSame(13, $e->responseCode, 'FailTopic: non-retryable code rethrown as MQBrokerException(13)');
        }
        $this->checkSame($bodyBefore, $failMsg->getBody(), 'restore: failure path still restores body');
        $this->checkSame('FailTopic', $failMsg->getTopic(), 'restore: failure path still restores topic');

        // 3) Sending the same message twice; the broker still decodes it the second time (core of restore semantics)
        $msg2 = new Message('SyncTopic', str_repeat('B', 5000));
        $p->send($msg2);
        $p->send($msg2);
        $decoded = [];
        foreach ($broker->logEntries() as $e) {
            if (($e['code'] ?? -99) === -1) {
                $decoded[] = $e['decodedLen'];
            }
        }
        $this->checkSame(5000, $decoded[count($decoded) - 2] ?? -1, 'second send decodes one layer to 5000');
        $this->checkSame(5000, $decoded[count($decoded) - 1] ?? -1, 'third send decodes one layer to 5000');

        // 4) Pinned send goes through hooks. Pinned guard semantics:
        //    the message topic and mq.topic are compared **after each gets its namespace applied**
        //    (withNamespace is idempotent) — applying PHPNS to the bare-name mq makes it equal to
        //    the message PHPNS%SyncTopic → no throw, sent normally.
        $pinnedMq = new MessageQueue('SyncTopic', 'broker-a', 0);
        try {
            $r = $p->send(new Message('SyncTopic', 'x'), 3000, $pinnedMq);
            $this->check($r->sendStatus === SendStatus::SEND_OK, 'pinned guard compares both sides with namespaces: bare-name mq hits, SEND_OK');
        } catch (\Throwable $e) {
            $this->check(false, 'pinned guard compares both sides with namespaces: should not throw (' . get_class($e) . ': ' . $e->getMessage() . ')');
        }
        // When mq and message topic genuinely differ the guard still intercepts (async variant in the async section)
        $this->checkThrows(
            fn() => $p->send(new Message('SyncTopic', 'x'), 3000, new MessageQueue('OtherTopic', 'broker-a', 0)),
            MQClientException::class,
            'pinned guard: mq vs message topic mismatch throws the sync message'
        );

        $p->shutdown();
        MQClientInstance::removeInstance('restore-inst-1-cid');
        $this->check(true, 'restore suite shutdown complete');
    }

    // ================================================================ 9. Retry / failure classification

    private function testSyncRetryAndFailures(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-retry', 'RetryTopic', 'retry-inst-1');
        $p->start();

        // RetryTopic: SYSTEM_ERROR is retryable → 3 attempts (1+2) then classified by the last error
        try {
            $p->send(new Message('RetryTopic', 'r-body'));
            $this->check(false, 'RetryTopic send should throw');
        } catch (MQClientException $e) {
            $this->check(str_contains($e->getMessage(), 'Send [3] times, still failed'), 'retry-exhausted message Send [3] times');
            $this->check(str_contains($e->getMessage(), 'BrokersSent: [broker-a, broker-a, broker-a]'), 'BrokersSent records 3 brokers');
            $this->checkSame(1, $e->responseCode, 'final code = last broker error code SYSTEM_ERROR(1)');
        }
        $this->checkSame(3, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'RetryTopic'), 'broker received 3 sends');

        // Route unavailable: NOT_FOUND_TOPIC_EXCEPTION(10005) classification
        $pNoRoute = $this->makeProducer($broker, 'GID-noroute', 'NoRouteTopic', 'noroute-inst-1');
        $pNoRoute->start();
        try {
            $pNoRoute->send(new Message('NoRouteTopic', 'x'));
            $this->check(false, 'NoRouteTopic send should throw');
        } catch (MQClientException $e) {
            $this->checkSame(10005, $e->responseCode, 'missing route classified as 10005');
        }
        $pNoRoute->shutdown();
        MQClientInstance::removeInstance('noroute-inst-1-cid');

        // Controllable clock: budget exhausted → call timeout, not a single request sent
        $pClock = $this->makeProducer($broker, 'GID-clock', 'ClockTopic', 'clock-inst-1');
        $pClock->start();
        $n = 0;
        $base = 1000000.0;
        $pClock->monoClock = static function () use (&$n, $base): float {
            return $base + (++$n) * 10000.0;
        };
        try {
            $pClock->send(new Message('ClockTopic', 'x'));
            $this->check(false, 'clock over budget should throw');
        } catch (RemotingTooMuchRequestException $e) {
            $this->checkSame('sendDefaultImpl call timeout', $e->getMessage(), 'call timeout message');
        }
        $this->checkSame(0, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'ClockTopic'), 'call timeout sends no request at all');
        $pClock->shutdown();
        MQClientInstance::removeInstance('clock-inst-1-cid');

        // Non-retryable code recorded while FaultStrategy is enabled (isolation not verified here; see Latency suite)
        $p2 = $this->makeProducer($broker, 'GID-metric', 'SyncTopic', 'metric-inst-1');
        $p2->start();
        $p2->send(new Message('SyncTopic', 'metric-body'));
        $snap = $p2->getMetrics()->snapshot();
        $this->checkSame(1, $snap['sendCount'], 'metrics sendCount=1');
        $this->checkSame(0, $snap['sendFailureCount'], 'metrics sendFailureCount=0');
        try {
            $p2->send(new Message('FailTopic', 'x'));
        } catch (MQBrokerException) {
            $this->check(true, 'non-retryable code thrown directly');
        }
        $snap2 = $p2->getMetrics()->snapshot();
        $this->checkSame(1, $snap2['sendFailureCount'], 'metrics sendFailureCount=1');
        $p2->shutdown();
        MQClientInstance::removeInstance('metric-inst-1-cid');

        $p->shutdown();
        MQClientInstance::removeInstance('retry-inst-1-cid');
        $this->check(true, 'retry suite shutdown complete');
    }

    // ================================================================ 10. Oneway

    private function testOneway(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-oneway', 'OnewayTopic', 'oneway-inst-1');
        $p->start();
        $msg = new Message('OnewayTopic', 'oneway-body');
        $p->sendOneway($msg);
        $this->check(true, 'sendOneway does not throw');
        $this->checkSame('OnewayTopic', $msg->getTopic(), 'oneway message topic restored');
        $this->checkSame(1, $this->waitLogCount($broker, RequestCode::SEND_MESSAGE_V2, 1, 'OnewayTopic'), 'broker received the oneway send');
        $onewayEntries = array_values(array_filter($broker->logEntries(), static fn ($e) =>
            ($e['code'] ?? 0) === RequestCode::SEND_MESSAGE_V2 && ($e['ext']['b'] ?? '') === 'OnewayTopic'));
        $this->checkSame(true, (bool) ($onewayEntries[0]['oneway'] ?? false), 'oneway flag set');
        // Pinned oneway
        $p->sendOneway(new Message('OnewayTopic', 'p2'), new MessageQueue('OnewayTopic', 'broker-a', 1));
        $this->checkSame(2, $this->waitLogCount($broker, RequestCode::SEND_MESSAGE_V2, 2, 'OnewayTopic'), 'pinned oneway arrived');
        $p->shutdown();
        MQClientInstance::removeInstance('oneway-inst-1-cid');
        $this->check(true, 'oneway suite shutdown complete');
    }

    // ================================================================ 11. Async send

    private function testAsyncSend(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-async', 'AsyncOkTopic', 'async-inst-1');
        $hook = new RecordingSendHook();
        $p->registerSendMessageHook($hook);
        $p->start();

        // Success: register callback → internal pump drives it to completion → onSuccess
        $slot = new CallbackSlot();
        $p->sendAsync(new Message('AsyncOkTopic', 'async-body'), $slot);
        $this->checkSame(1, $slot->calls, 'async success: callback invoked exactly once');
        $this->check($slot->error === null, 'async success: no exception');
        $this->check($slot->result instanceof SendResult, 'async success: result is SendResult');
        $this->checkSame(SendStatus::SEND_OK, $slot->result->sendStatus ?? null, 'async success: SEND_OK');
        $this->checkSame(1, $hook->before, 'async: before hook once');
        $this->checkSame(1, $hook->after, 'async: after hook once');
        $this->checkSame(1, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'AsyncOkTopic'), 'broker received the async send');

        // Batch async
        $slotB = new CallbackSlot();
        $p->sendAsync([new Message('AsyncOkTopic', 'b1'), new Message('AsyncOkTopic', 'b2')], $slotB);
        $this->checkSame(1, $slotB->calls, 'batch async: callback invoked exactly once');
        $this->check($slotB->result !== null && SendStatus::SEND_OK === $slotB->result->sendStatus, 'batch async: success');
        $this->checkSame(1, $broker->logCount(RequestCode::SEND_BATCH_MESSAGE, 'AsyncOkTopic'), 'batch async uses SEND_BATCH_MESSAGE');

        // Pinned async + pinned topic mismatch → async message
        $slotP = new CallbackSlot();
        $p->sendAsync(new Message('AsyncOkTopic', 'p'), $slotP, 3000, new MessageQueue('AsyncOkTopic', 'broker-a', 1));
        $this->check($slotP->result !== null, 'pinned async success');
        $slotBad = new CallbackSlot();
        $p->sendAsync(new Message('AsyncOkTopic', 'x'), $slotBad, 3000, new MessageQueue('OtherTopic', 'broker-a', 0));
        $this->check($slotBad->error instanceof MQClientException, 'pinned async topic mismatch → callback exception');
        $this->checkSame(
            DefaultMQProducer::PINNED_TOPIC_MISMATCH_ASYNC,
            $slotBad->error->getMessage() ?? '',
            'pinned async uses the async message'
        );

        // Broker explicitly errors: MQBrokerException passed to the callback as-is (async ignores retryResponseCodes)
        $slotF = new CallbackSlot();
        $p->sendAsync(new Message('AsyncFailTopic', 'x'), $slotF);
        $this->checkSame(1, $slotF->calls, 'async failure: callback invoked exactly once');
        $this->check($slotF->error instanceof MQBrokerException, 'async failure: broker code surfaced as MQBrokerException');

        // Connect failure: MQClientAPIImpl layer retries retryTimesWhenSendAsyncFailed=2 times (1+2=3 kernel attempts),
        // finally wrapped as MQClientException('unknown reason') (taking the operationFail else branch),
        // with cause preserved as RemotingConnectException. The forbidden hook must sit **above the retry
        // layer** (sendKernelImpl) and execute only once — you cannot count kernel attempts with a throwing hook.
        $pGhost = $this->makeProducer($broker, 'GID-ghost', 'GhostTopic', 'ghost-inst-1');
        $counting = new class() implements CheckForbiddenHook {
            public int $calls = 0;

            public function hookName(): string
            {
                return 'CountingForbiddenHook';
            }

            public function checkForbidden(CheckForbiddenContext $context): void
            {
                $this->calls++;
            }
        };
        $pGhost->registerCheckForbiddenHook($counting);
        $pGhost->start();
        $slotG = new CallbackSlot();
        $pGhost->sendAsync(new Message('GhostTopic', 'x'), $slotG);
        $this->checkSame(1, $slotG->calls, 'connect failure: callback invoked exactly once');
        $this->check($slotG->error instanceof MQClientException, 'connect failure: wrapped as MQClientException');
        $this->checkSame('unknown reason', $slotG->error->getMessage() ?? '', 'connect failure: unknown reason message');
        $this->check($slotG->error->getPrevious() instanceof RemotingConnectException, 'connect failure: cause preserved');
        $this->checkSame(1, $counting->calls, 'forbidden hook sits above the retry layer, executes once (protocol contract)');

        // Throwing hook: throws on the first round → runnable catch forwards it to the callback as-is (async semantics),
        // no retry, no wrapping
        $pThrowG = $this->makeProducer($broker, 'GID-ghost2', 'GhostTopic', 'ghost2-inst-1');
        $throwingG = new ThrowingForbiddenHook();
        $pThrowG->registerCheckForbiddenHook($throwingG);
        $pThrowG->start();
        $slotT = new CallbackSlot();
        $pThrowG->sendAsync(new Message('GhostTopic', 'x'), $slotT);
        $this->checkSame(1, $slotT->calls, 'throwing hook: callback invoked exactly once');
        $this->checkSame('forbidden by test hook', $slotT->error->getMessage() ?? '', 'throwing hook: first-round exception surfaced to the callback as-is');
        $this->checkSame(1, $throwingG->calls, 'throwing hook: executes once, no retry afterwards');
        $pThrowG->shutdown();
        MQClientInstance::removeInstance('ghost2-inst-1-cid');

        // Backpressure: semaphore drained → tryAcquire fails → RemotingTooMuchRequestException message
        $pBp = $this->makeProducer($broker, 'GID-bp', 'AsyncOkTopic', 'bp-inst-1');
        $pBp->setEnableBackpressureForAsyncMode(true);
        $pBp->setBackPressureForAsyncSendNum(10);
        // Byte gate explicitly set to 1MB: with the default 100MB the "release" assertion cannot numerically
        // distinguish a missing body; only 1MB verifies the release precisely after the callback.
        $pBp->setBackPressureForAsyncSendSize(1024 * 1024);
        $pBp->start();
        $semNum = $this->privateOf($pBp, 'semaphoreAsyncSendNum');
        $semNum->tryAcquire(10, 0);
        $slotBp = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 'x'), $slotBp, 500);
        $this->checkSame(1, $slotBp->calls, 'backpressure full: callback invoked exactly once');
        $this->check($slotBp->error instanceof RemotingTooMuchRequestException, 'backpressure full: RemotingTooMuchRequestException');
        $this->checkSame('send message tryAcquire semaphoreAsyncNum timeout', $slotBp->error->getMessage() ?? '', 'backpressure full: count gate message');
        $semNum->release(10);
        // Byte gate
        $semSize = $this->privateOf($pBp, 'semaphoreAsyncSendSize');
        $semSize->tryAcquire($semSize->totalPermits(), 0);
        $slotBp2 = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 'x'), $slotBp2, 500);
        $this->checkSame('send message tryAcquire semaphoreAsyncSize timeout', $slotBp2->error->getMessage() ?? '', 'backpressure full: byte gate message');
        $semSize->release($semSize->totalPermits());
        // Recovery after release: normal send + permits returned
        $slotBp3 = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 'ok'), $slotBp3);
        $this->check($slotBp3->result !== null, 'send succeeds after backpressure release');
        $this->checkSame(10, $pBp->getSemaphoreAsyncSendNumAvailablePermits(), 'count permits returned after callback');
        $this->checkSame(1024 * 1024, $pBp->getSemaphoreAsyncSendSizeAvailablePermits(), 'byte permits returned after callback');

        // Backpressure off + queue not full: two sends in a row both succeed
        $pBp->setEnableBackpressureForAsyncMode(false);
        $s1 = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 's1'), $s1);
        $s2 = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 's2'), $s2);
        $this->check($s1->result !== null && $s2->result !== null, 'two consecutive async sends both succeed');

        $pBp->shutdown();
        MQClientInstance::removeInstance('bp-inst-1-cid');
        $pGhost->shutdown();
        MQClientInstance::removeInstance('ghost-inst-1-cid');
        $p->shutdown();
        MQClientInstance::removeInstance('async-inst-1-cid');
        $this->check(true, 'async suite shutdown complete');
    }

    // ================================================================ 12. Send via selector

    private function testSendBySelector(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-sel', 'SelTopic', 'sel-inst-1');
        $forbidden = new class() implements CheckForbiddenHook {
            public mixed $lastArg = null;
            public int $calls = 0;

            public function hookName(): string
            {
                return 'ArgRecordingForbiddenHook';
            }

            public function checkForbidden(CheckForbiddenContext $context): void
            {
                $this->calls++;
                $this->lastArg = $context->arg;
            }
        };
        $p->registerCheckForbiddenHook($forbidden);
        $p->start();

        $byHash = new SelectMessageQueueByHash();
        $result = $p->sendBySelector(new Message('SelTopic', 'sel-body'), $byHash, 'order-42', 3000);
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, 'selector send OK');
        $qid = $result->messageQueue->queueId ?? -1;
        $this->check($qid >= 0 && $qid < 2, 'selector send hit a valid queue');
        // The queueId the broker received matches the selection
        $found = false;
        foreach ($broker->logEntries() as $e) {
            if (($e['code'] ?? 0) === RequestCode::SEND_MESSAGE_V2 && ($e['ext']['b'] ?? '') === 'SelTopic'
                && (string) ($e['ext']['e'] ?? '') === (string) $qid) {
                $found = true;
                break;
            }
        }
        $this->check($found, 'queueId received by broker matches the selector');
        $this->checkSame(1, $forbidden->calls, 'CheckForbidden executed with the send');
        $this->checkSame('order-42', $forbidden->lastArg, 'arg passed through to CheckForbiddenContext');

        $byRandom = new SelectMessageQueueByRandom();
        $r2 = $p->sendBySelector(new Message('SelTopic', 'r'), $byRandom, null, 3000);
        $this->checkSame(SendStatus::SEND_OK, $r2->sendStatus, 'ByRandom send OK');
        $byRoom = new SelectMessageQueueByMachineRoom();
        $r3 = $p->sendBySelector(new Message('SelTopic', 'r'), $byRoom, 'broker', 3000);
        $this->checkSame(SendStatus::SEND_OK, $r3->sendStatus, 'ByMachineRoom send OK (broker- prefix match)');

        // Intercepting hook throws: pinned/selector sendSelectImpl has **no retry loop** —
        // one queue selection, one send; the hook exception propagates as-is (no Send[N] times wrapper,
        // responseCode stays as the hook set it)
        $throwing = new ThrowingForbiddenHook();
        $p2 = $this->makeProducer($broker, 'GID-sel2', 'SelTopic', 'sel2-inst-1');
        $p2->registerCheckForbiddenHook($throwing);
        $p2->start();
        try {
            $p2->sendBySelector(new Message('SelTopic', 'x'), $byHash, 'a', 3000);
            $this->check(false, 'intercepting hook exception should propagate');
        } catch (MQClientException $e) {
            $this->check(str_contains($e->getMessage(), 'forbidden by test hook'), 'intercepting exception thrown as-is');
            $this->checkSame(null, $e->responseCode, 'hook exception not wrapped (responseCode stays as the hook set it)');
        }
        $this->checkSame(1, $throwing->calls, 'selector path has no retry loop, hook executes exactly once');

        $p2->shutdown();
        MQClientInstance::removeInstance('sel2-inst-1-cid');
        $p->shutdown();
        MQClientInstance::removeInstance('sel-inst-1-cid');
        $this->check(true, 'selector suite shutdown complete');
    }

    // ================================================================ 13. Batch send

    private function testBatchSend(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-batch', 'BatchTopic', 'batch-inst-1');
        $p->start();

        $m1 = new Message('BatchTopic', 'batch-1');
        $m2 = new Message('BatchTopic', 'batch-2');
        $result = $p->send([$m1, $m2]);
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, 'batch sync send OK');
        $this->checkSame(1, $broker->logCount(RequestCode::SEND_BATCH_MESSAGE, 'BatchTopic'), 'batch uses SEND_BATCH_MESSAGE');
        // Sub-message topics are already the original values (batch does not restore, but we sent the originals)
        $this->checkSame('BatchTopic', $m1->getTopic(), 'batch sub-message topic not rewritten by restore');

        $this->checkThrows(fn() => $p->send([]), MQClientException::class, 'empty batch throws message list is empty');

        // Batch pinned mismatch → sync message
        $badMq = new MessageQueue('OtherTopic', 'broker-a', 0);
        $this->checkThrows(
            fn() => $p->send([new Message('BatchTopic', 'x')], 3000, $badMq),
            MQClientException::class,
            'batch pinned mismatch throws the sync message'
        );

        $p->shutdown();
        MQClientInstance::removeInstance('batch-inst-1-cid');
        $this->check(true, 'batch suite shutdown complete');
    }

    // ================================================================ 14. Transactional messages

    private function testTransaction(ProducerFakeBroker $broker): void
    {
        $p = new TransactionMQProducer('GID-trans', null, '', ['TransTopic', 'TransCheckTopic']);
        $p->setInstanceName('trans-inst-1');
        $p->setNameServerAddresses([$broker->addr]);
        $endHook = new RecordingEndTransactionHook();
        $p->registerEndTransactionHook($endHook);
        $listener = new ScriptedTransactionListener(LocalTransactionState::COMMIT_MESSAGE);
        $p->setTransactionListener($listener);
        $p->start();

        // COMMIT
        $msg = new Message('TransTopic', 'trans-body');
        $txResult = $p->sendMessageInTransaction($msg, $listener);
        $this->check($txResult instanceof TransactionSendResult, 'transactional send returns TransactionSendResult');
        $this->checkSame(SendStatus::SEND_OK, $txResult->getSendStatus(), 'transaction: SEND_OK');
        $this->checkSame(LocalTransactionState::COMMIT_MESSAGE, $txResult->getLocalTransactionState(), 'transaction: local state COMMIT');
        $this->checkSame('TransTopic', $msg->getTopic(), 'transaction: caller topic restored');
        $this->checkSame('trans-body', $msg->getBody(), 'transaction: caller body restored (executeLocalTransaction sees the original)');
        // END_TRANSACTION(37) oneway reached the broker: commitOrRollback=8, fromTransactionCheck=false
        // (oneway returns as soon as it is written; the mock child logs asynchronously → poll and wait for frames first)
        $this->waitLogCount($broker, RequestCode::END_TRANSACTION, 1);
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $this->check(count($endEntries) >= 1, 'broker received END_TRANSACTION');
        $first = $endEntries[0] ?? [];
        $this->checkSame('8', (string) ($first['ext']['commitOrRollback'] ?? ''), 'COMMIT → commitOrRollback=8');
        $this->checkSame('false', (string) ($first['ext']['fromTransactionCheck'] ?? ''), 'active finalize has fromTransactionCheck=false');
        $this->checkSame('100', (string) ($first['ext']['tranStateTableOffset'] ?? ''), 'tranStateTableOffset takes queueOffset');
        $this->checkSame('100', (string) ($first['ext']['commitLogOffset'] ?? ''), 'commitLogOffset = offset of decodeMessageId(offsetMsgId)');
        $this->checkSame('GID-trans', $first['ext']['producerGroup'] ?? '', 'END_TRANSACTION carries producerGroup');
        $this->checkSame(true, (bool) ($first['oneway'] ?? false), 'END_TRANSACTION is oneway');
        $this->checkSame(1, $endHook->calls, 'EndTransactionHook executed once');
        $this->checkSame(LocalTransactionState::COMMIT_MESSAGE, $endHook->contexts[0]->transactionState ?? null, 'EndTransactionHook context carries the state');

        // ROLLBACK
        $listener2 = new ScriptedTransactionListener(LocalTransactionState::ROLLBACK_MESSAGE);
        $p->sendMessageInTransaction(new Message('TransTopic', 'r'), $listener2);
        $this->waitLogCount($broker, RequestCode::END_TRANSACTION, 2);
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $this->checkSame('12', (string) ($endEntries[1]['ext']['commitOrRollback'] ?? ''), 'ROLLBACK → commitOrRollback=12');

        // UNKNOW
        $listener3 = new ScriptedTransactionListener(LocalTransactionState::UNKNOW);
        $p->sendMessageInTransaction(new Message('TransTopic', 'u'), $listener3);
        $this->waitLogCount($broker, RequestCode::END_TRANSACTION, 3);
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $this->checkSame('0', (string) ($endEntries[2]['ext']['commitOrRollback'] ?? ''), 'UNKNOW → commitOrRollback=0');

        // Local transaction throws → UNKNOW + remark
        $boomListener = new class() implements TransactionListener {
            public function executeLocalTransaction(Message $msg, mixed $arg): LocalTransactionState
            {
                throw new \RuntimeException('local boom');
            }

            public function checkLocalTransaction(MessageExt $msg): LocalTransactionState
            {
                return LocalTransactionState::UNKNOW;
            }
        };
        $txBoom = $p->sendMessageInTransaction(new Message('TransTopic', 'e'), $boomListener);
        $this->checkSame(LocalTransactionState::UNKNOW, $txBoom->getLocalTransactionState(), 'local transaction exception → UNKNOW');
        $this->waitLogCount($broker, RequestCode::END_TRANSACTION, 4);
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $this->check(count($endEntries) >= 4, 'END_TRANSACTION requests total 4');

        // Broker check (TransCheckTopic): after the response the broker pushes 39, the inline callback
        // invokes listener.checkLocalTransaction → END_TRANSACTION(fromTransactionCheck=true)
        $checkListener = new ScriptedTransactionListener(LocalTransactionState::COMMIT_MESSAGE);
        $checkListener->checkState = LocalTransactionState::ROLLBACK_MESSAGE;
        $checkCountBefore = $checkListener->checkCalls;
        $p->sendMessageInTransaction(new Message('TransCheckTopic', 'trans-body'), $checkListener);
        // One extra plain send flushes the leftover 39 push frame from the socket (single-threaded pump model)
        $p->send(new Message('TransTopic', 'flusher'));
        $this->checkSame($checkCountBefore + 1, $checkListener->checkCalls, 'check: checkLocalTransaction invoked once');
        $this->check($checkListener->lastChecked !== null, 'check: received MessageExt');
        $this->checkSame('trans-body', $checkListener->lastChecked->getBody() ?? '', 'check: MessageExt body');
        $this->checkSame('GID-trans', $checkListener->lastChecked->getProperty(MessageConst::PROPERTY_PRODUCER_GROUP), 'check: PGROUP property matches');
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $checkEnd = null;
        foreach ($endEntries as $e) {
            if (($e['ext']['fromTransactionCheck'] ?? '') === 'true') {
                $checkEnd = $e;
                break;
            }
        }
        $this->check($checkEnd !== null, 'check finalize END_TRANSACTION(fromTransactionCheck=true) reached the broker');
        $this->checkSame('12', (string) ($checkEnd['ext']['commitOrRollback'] ?? ''), 'check rollback → commitOrRollback=12');
        $this->checkSame('7', (string) ($checkEnd['ext']['tranStateTableOffset'] ?? ''), 'check: offsets taken from the check header');
        $this->checkSame('99', (string) ($checkEnd['ext']['commitLogOffset'] ?? ''), 'check: commitLogOffset taken from the check header');

        // Delay property rejected
        $delayed = new Message('TransTopic', 'd');
        $delayed->putProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL, '1');
        $this->checkThrows(
            fn() => $p->sendMessageInTransaction($delayed, $checkListener),
            MQClientException::class,
            'transactional message rejects delayed delivery'
        );

        // TransactionMQProducer without listener → throws
        $noListener = new TransactionMQProducer('GID-trans-none');
        $noListener->setInstanceName('trans-none-inst');
        $noListener->setNameServerAddresses([$broker->addr]);
        $noListener->start();
        $this->checkThrows(
            fn() => $noListener->sendMessageInTransaction(new Message('TransTopic', 'x')),
            MQClientException::class,
            'no transaction listener throws transaction listener is not set'
        );
        $noListener->shutdown();
        MQClientInstance::removeInstance('trans-none-inst-cid');

        $p->shutdown();
        MQClientInstance::removeInstance('trans-inst-1-cid');
        $this->check(true, 'transaction suite shutdown complete');
    }

    // ================================================================ 15. Request-Reply

    private function testRequestReply(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-request', 'RequestTopic', 'request-inst-1');
        $p->start();

        $reply = $p->request(new Message('RequestTopic', 'request-body'), 5000);
        $this->check($reply instanceof MessageExt, 'request returns the reply message');
        $this->checkSame('reply-body', $reply->getBody(), 'reply body');
        $this->check(is_string($reply->getProperty(MessageConst::PROPERTY_CORRELATION_ID)), 'reply carries CORRELATION_ID');
        // Wait for the slot to be cleaned up
        $this->checkSame(0, count(RequestFutureHolder::getInstance()->requestFutureTable), 'future table empty after request');
        // Request carries TTL / REPLY_TO_CLIENT
        $saw = false;
        foreach ($broker->logEntries() as $e) {
            if (($e['code'] ?? 0) === RequestCode::SEND_MESSAGE_V2 && ($e['ext']['b'] ?? '') === 'RequestTopic') {
                $props = MessageDecoder::string2MessageProperties((string) ($e['ext']['i'] ?? ''));
                $saw = isset($props[MessageConst::PROPERTY_MESSAGE_TTL])
                    && isset($props[MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT])
                    && isset($props[MessageConst::PROPERTY_CORRELATION_ID]);
            }
        }
        $this->check($saw, 'request message carries CORRELATION_ID / REPLY_TO_CLIENT / TTL');
        // Heartbeat resend (REPLY_TO_CLIENT depends on the broker knowing this client)
        $this->check($broker->logCount(RequestCode::HEART_BEAT) >= 1, 'broker received a heartbeat before request');

        // Send itself fails → MQClientException (does not wait out the full timeout)
        $pFail = $this->makeProducer($broker, 'GID-request2', 'AsyncFailTopic', 'request2-inst-1');
        $pFail->start();
        $t0 = microtime(true);
        try {
            $pFail->request(new Message('AsyncFailTopic', 'x'), 5000);
            $this->check(false, 'request failure should throw');
        } catch (MQClientException $e) {
            $this->check(str_contains($e->getMessage(), 'send request message to <AsyncFailTopic> fail'), 'send-failure message');
            $this->check($e->getPrevious() instanceof MQBrokerException, 'failure cause is a broker exception');
        }
        $elapsed = microtime(true) - $t0;
        $this->check($elapsed < 4.5, 'send failure does not wait out the full timeout (took ' . round($elapsed, 2) . 's)');
        $pFail->shutdown();
        MQClientInstance::removeInstance('request2-inst-1-cid');

        // reply(): createReplyMessage + recode-325 send
        $req = new Message('RequestTopic', 'req-for-reply');
        $req->putProperty(MessageConst::PROPERTY_CLUSTER, 'DefaultCluster');
        $req->putProperty(MessageConst::PROPERTY_CORRELATION_ID, 'corr-1');
        $req->putProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT, 'client-1');
        $req->putProperty(MessageConst::PROPERTY_MESSAGE_TTL, '5000');
        $rr = $p->reply($req, 'offline-reply-body');
        $this->check($rr->sendStatus === SendStatus::SEND_OK, 'reply sync send SEND_OK');
        $saw325 = false;
        $props325 = [];
        foreach ($broker->logEntries() as $e) {
            if (($e['code'] ?? 0) === RequestCode::SEND_REPLY_MESSAGE_V2
                && ($e['ext']['b'] ?? '') === MixAll::getReplyTopic('DefaultCluster')) {
                $saw325 = true;
                $props325 = MessageDecoder::string2MessageProperties((string) ($e['ext']['i'] ?? ''));
            }
        }
        $this->check($saw325, 'reply recodes to SEND_REPLY_MESSAGE_V2(325) with topic=<CLUSTER>_REPLY_TOPIC');
        $this->checkSame('reply', $props325[MessageConst::PROPERTY_MESSAGE_TYPE] ?? '', 'reply carries MSG_TYPE=reply');
        $this->checkSame('corr-1', $props325[MessageConst::PROPERTY_CORRELATION_ID] ?? '', 'reply fills back CORRELATION_ID');
        $this->checkSame('client-1', $props325[MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT] ?? '', 'reply fills back REPLY_TO_CLIENT');
        try {
            $p->reply(new Message('RequestTopic', 'no-cluster'), 'x');
            $this->check(false, 'reply without CLUSTER should throw');
        } catch (MQClientException $e) {
            $this->check(str_contains($e->getMessage(), 'property[CLUSTER] is null'), 'no-CLUSTER message (CREATE_REPLY_MESSAGE_EXCEPTION semantics)');
        }

        $p->shutdown();
        MQClientInstance::removeInstance('request-inst-1-cid');
        $this->check(true, 'request-reply suite shutdown complete');
    }

    // ================================================================ 16. Recall / admin capabilities

    private function testAdminAndRecall(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-admin', 'RecallTopic', 'admin-inst-1');
        $p->start();

        $handle = RecallMessageHandle::buildHandle('RecallTopic', 'broker-a', '123', 'msg-1');
        $uniq = $p->recallMessage('RecallTopic', $handle);
        $this->checkSame('recall-uniq-1', $uniq, 'recallMessage returns the broker msgId');
        $this->checkSame(1, $broker->logCount(RequestCode::RECALL_MESSAGE), 'broker received RECALL_MESSAGE');

        $this->checkThrows(
            fn() => $p->recallMessage('RecallTopic', 'not-a-valid-handle!!'),
            MQClientException::class,
            'invalid handle throws recall handle is invalid'
        );
        $this->checkThrows(
            fn() => $p->recallMessage('%RETRY%GID-admin', $handle),
            MQClientException::class,
            'retry topic does not support recall'
        );

        // Admin queries (route already cached for broker-a)
        $mq = new MessageQueue('RecallTopic', 'broker-a', 0);
        $this->checkSame(777, $p->maxOffset($mq), 'maxOffset');
        $this->checkSame(11, $p->minOffset($mq), 'minOffset');
        $this->checkSame(12345, $p->earliestMsgStoreTime($mq), 'earliestMsgStoreTime');
        $this->checkSame([], $p->queryMessage('RecallTopic', 'k', 32, 0, 0), 'queryMessage empty result → []');

        $queues = $p->fetchPublishMessageQueues('RecallTopic');
        $this->checkSame(2, count($queues), 'fetchPublishMessageQueues returns 2 queues');
        $this->checkSame('broker-a', $queues[0]->brokerName, 'queue brokerName');

        $this->checkThrows(fn() => $p->viewMessage('T', 'id'), MQClientException::class, 'viewMessage unsupported');
        $this->checkThrows(fn() => $p->createTopic('k', 'SCHEDULE_TOPIC_XXXX'), MQClientException::class, 'createTopic rejects system topic');

        $p->shutdown();
        MQClientInstance::removeInstance('admin-inst-1-cid');
        $this->check(true, 'admin suite shutdown complete');
    }

    // ================================================================ 17. Auto batching

    private function testAutoBatch(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-acc', 'AccTopic', 'acc-inst-1');
        $p->setAutoBatch(true);
        $p->setBatchMaxDelayMs(200);
        $p->setBatchMaxBytes(32 * 1024);
        $p->start();
        $this->checkSame(true, $p->getAutoBatch(), 'getAutoBatch=true after start');
        $this->checkSame(200, $p->getBatchMaxDelayMs(), 'thresholds synced to the accumulator (delayMs)');
        $this->checkSame(32 * 1024, $p->getBatchMaxBytes(), 'thresholds synced to the accumulator (bytes)');
        $this->check($p->produceAccumulator !== null, 'accumulator built after start');

        $msg = new Message('AccTopic', 'acc-body-1');
        $result = $p->send($msg);
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, 'auto-batch pass-through send OK');
        $this->check(is_string($result->msgId) && $result->msgId !== '', 'batched send returns the per-message msgId');

        // Delayed messages cannot be batched → direct send (equivalent path)
        $delayed = new Message('AccTopic', 'd');
        $delayed->putProperty('DELAY', '1');
        $r2 = $p->send($delayed);
        $this->checkSame(SendStatus::SEND_OK, $r2->sendStatus, 'delayed message falls back to direct send OK');

        // Async goes through the accumulator: PHP single-threaded adaptation, no background thread,
        // explicit flushAll pumps it out (same driving style as the async section in RunClientAccumulator)
        $slot = new CallbackSlot();
        $p->sendAsync(new Message('AccTopic', 'async-acc'), $slot);
        $p->produceAccumulator->flushAll();
        $this->checkSame(1, $slot->calls, 'batched async callback invoked exactly once');
        $this->check($slot->result !== null, 'batched async success');

        $p->shutdown();
        MQClientInstance::removeInstance('acc-inst-1-cid');
        $this->check(true, 'auto-batch suite shutdown complete');
    }

    // ================================================================ 18. Trace dispatcher (enableTrace)

    private function testTraceDispatcherWiring(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-trace', 'TraceTopic', 'trace-inst-1');
        $p->setEnableTrace(true);
        $p->start();
        // Note the FQCN: this file is in the global namespace and does not import AsyncTraceDispatcher;
        // a bare class name in instanceof against an undefined class **silently returns false** (no error).
        $this->check($p->traceDispatcher instanceof \RocketMQ\Client\AsyncTraceDispatcher, 'enableTrace → AsyncTraceDispatcher built');
        $this->checkSame(1, count(array_filter($p->sendMessageHookList, static fn ($h) => $h instanceof \RocketMQ\Client\SendMessageTraceHook)), 'SendMessageTraceHook registered');
        $this->checkSame(1, count(array_filter($p->endTransactionHookList, static fn ($h) => $h instanceof \RocketMQ\Client\EndTransactionTraceHook)), 'EndTransactionTraceHook registered');
        $this->check($p->traceDispatcher->traceProducer !== null, 'internal trace producer built');
        $tp = $p->traceDispatcher->traceProducer;
        $this->check($tp instanceof DefaultMQProducer, 'internal trace producer is a DefaultMQProducer');
        if ($tp instanceof DefaultMQProducer) {
            $this->checkSame(false, $tp->enableTrace, 'internal trace producer enableTrace=false (recursion guard)');
        }
        $p->send(new Message('TraceTopic', 'trace-body'));
        $this->check($p->traceDispatcher->batchNum >= 1, 'trace batchNum >= 1');
        // The trace queue collected the Pub context (appended by the after hook)
        $p->traceDispatcher->pumpOnce(true); // forced flush (logs only when no route; does not throw)
        $this->check(true, 'trace pumpOnce does not throw');
        $p->shutdown();
        MQClientInstance::removeInstance('trace-inst-1-cid');
        $this->check(true, 'trace suite shutdown complete');
    }

    // ================================================================ helpers

    private function privateOf(object $obj, string $name): mixed
    {
        $r = new \ReflectionProperty(get_class($obj), $name);
        $r->setAccessible(true);
        return $r->getValue($obj);
    }

    /**
     * Poll until the mock broker log count reaches the target (oneway / END_TRANSACTION are
     * fire-and-forget: the client returns as soon as it writes, and the mock child logs
     * asynchronously — you must poll before asserting, otherwise it is a pure race-induced
     * false failure). Returns the count reached at that time (for checkSame assertions).
     */
    private function waitLogCount(ProducerFakeBroker $broker, int $code, int $want, ?string $topic = null, int $timeoutMs = 3000): int
    {
        $deadline = microtime(true) + $timeoutMs / 1000.0;
        $n = $broker->logCount($code, $topic);
        while ($n < $want && microtime(true) < $deadline) {
            usleep(20000);
            $n = $broker->logCount($code, $topic);
        }
        return $n;
    }

    // ================================================================ run

    public function run(): int
    {
        $this->testConstantsAndDefaults();
        $this->testConstructorAndSetters();
        $this->testStaticHelpers();
        $this->testSelectors();
        $this->testCallbacksAndResults();
        $this->testLifecycleValidation();

        $broker = ProducerFakeBroker::start();
        try {
            $this->testSyncSend($broker);
            $this->testCompressionAndRestore($broker);
            $this->testSyncRetryAndFailures($broker);
            $this->testOneway($broker);
            $this->testAsyncSend($broker);
            $this->testSendBySelector($broker);
            $this->testBatchSend($broker);
            $this->testTransaction($broker);
            $this->testRequestReply($broker);
            $this->testAdminAndRecall($broker);
            $this->testAutoBatch($broker);
            $this->testTraceDispatcherWiring($broker);
        } finally {
            $broker->stop();
        }
        return $this->summary();
    }
}

exit((new RunClientProducer())->run());
