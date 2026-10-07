<?php

declare(strict_types=1);

/**
 * DefaultMQProducer / TransactionMQProducer 纯 PHP assert 风格自测（不依赖 phpunit，不连真机）。
 *
 * 运行：C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunClientProducer.php
 * 全部通过输出 "ALL TESTS PASSED (N checks)"，任一失败列出明细并以非零码退出。
 *
 * 网络相关用例以 `--broker` 参数二次拉起自身作为**子进程**假 broker（stream_socket_server，
 * 手法照抄 RunClientInstance.php）。broker 收到的每条命令追加写入一个 JSON 行日志文件，
 * 供父进程断言「broker 实际收到了什么」（发送码、sysFlag、END_TRANSACTION 的
 * commitOrRollback 等）。时钟通过 DefaultMQProducer::$monoClock / $wallClock 注入。
 */

require_once __DIR__ . '/../bootstrap.php';

// 清掉动态取址环境变量，保证 start() 的 10004 分支可测
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
// 子进程假 broker：`php RunClientProducer.php --broker <logfile>`
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
 * 发送族响应：按 topic 名分流；返回 [响应, 推送帧列表]（推送帧在响应后立刻写出）。
 */
function producerBrokerSendResponse(RemotingCommand $cmd, string $selfAddr): array
{
    $ext = $cmd->extFields;
    $topic = (string) ($ext['b'] ?? $ext['topic'] ?? '');
    $oneway = $cmd->isOnewayRPC();

    if ($topic === 'FailTopic') {
        return [RemotingCommand::createResponseCommand(ResponseCode::MESSAGE_ILLEGAL, 'illegal body'), []];
    }
    if ($topic === 'RetryTopic' || $topic === 'AsyncFailTopic') {
        return [RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'system busy'), []];
    }
    if ($oneway) {
        return [null, []]; // oneway：记日志但不回
    }

    // offsetMsgId 用合法的 32 hex：ip=10.0.0.1, port=8000, offset=100
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
        // 事务回查推送：CHECK_TRANSACTION_STATE(39)，body 为整条编码后的 MessageExt
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
                if ($topic === 'NoRouteTopic') {
                    $resp = RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST, 'no route');
                } elseif ($topic === 'GhostTopic') {
                    // 一台永远连不上的 broker（连接拒绝）
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
                // 压缩消息解一层再记长度（供父进程断言"没有 zlib(zlib(x))"）
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
            case RequestCode::HEARTBEAT:
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
 * 从缓冲里取一条完整帧（ROCKETMQ 帧 = 4 字节总长 + body）。
 * 返回 [raw 帧字节, 解码后的 RemotingCommand]；不完整返回 null。
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
        // 非法长度：清空缓冲，防止死循环
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
     * 多连接事件循环。老实现是「单连接阻塞式」：父进程的连接不关闭时，
     * 子进程会永远卡在 fread 上回不到 accept()，Windows 上新连接直接被拒。
     * 这里用 stream_select 同时监听 server + 全部已打开连接。
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
// 子进程假 broker 句柄
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

    /** 某个请求码的日志条数（可选按 topic 过滤）。 */
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
// 测试用钩子 / listener
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

/** 收集异步回调结果的槽。 */
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
// 测试 runner
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

    // ================================================================ 1. 常量与默认值

    private function testConstantsAndDefaults(): void
    {
        $p = new DefaultMQProducer('GID-defs');
        $this->checkSame("message's topic not equal mq's topic", DefaultMQProducer::PINNED_TOPIC_MISMATCH_SYNC, '定点守卫同步文案（Java :1235）');
        $this->checkSame('Topic of the message does not match its target message queue', DefaultMQProducer::PINNED_TOPIC_MISMATCH_ASYNC, '定点守卫异步文案（Java :1278）');
        $this->checkSame(3000, $p->sendMsgTimeout, '默认 sendMsgTimeout=3000');
        $this->checkSame(1024 * 4, $p->compressMsgBodyOverHowmuch, '默认 compressMsgBodyOverHowmuch=4096');
        $this->checkSame(5, $p->compressLevel, '默认 compressLevel=5');
        $this->checkSame(MessageSysFlag::ZLIB_TYPE, $p->compressType, '默认压缩类型 ZLIB');
        $this->checkSame(2, $p->retryTimesWhenSendFailed, '默认同步重试 2');
        $this->checkSame(2, $p->retryTimesWhenSendAsyncFailed, '默认异步重试 2');
        $this->checkSame(false, $p->retryAnotherBrokerWhenNotStoreOk, '默认不换 broker 重试非 OK');
        $this->checkSame(-1, $p->sendMsgMaxTimeoutPerRequest, '默认单请求超时 -1（不限制）');
        $this->checkSame(1024 * 1024 * 4, $p->maxMessageSize, '默认 maxMessageSize=4MB');
        $this->checkSame(MixAll::DEFAULT_TOPIC, $p->createTopicKey, '默认 createTopicKey=TBW102');
        $this->checkSame(MixAll::DEFAULT_TOPIC_QUEUE_NUMS, $p->defaultTopicQueueNums, '默认 defaultTopicQueueNums=4');
        $this->checkSame(30000, $p->heartbeatIntervalMillis, '默认心跳周期 30000');
        $this->checkSame(30000, $p->pollNameServerInterval, '默认路由刷新周期 30000');
        $this->checkSame(false, $p->sendLatencyFaultEnable, '默认延迟容错关闭');
        $this->checkSame(false, $p->enableBackpressureForAsyncMode, '默认背压关闭');
        $this->checkSame(1024, $p->backPressureForAsyncSendNum, '默认背压条数 1024');
        $this->checkSame(100 * 1024 * 1024, $p->backPressureForAsyncSendSize, '默认背压字节 100MB');
        $this->checkSame(1024, $p->getSemaphoreAsyncSendNumAvailablePermits(), '条数信号量初始 1024');
        $this->checkSame(100 * 1024 * 1024, $p->getSemaphoreAsyncSendSizeAvailablePermits(), '字节信号量初始 100MB');
        $this->checkSame(50000, $p->asyncSenderQueueCapacity, '默认异步队列 50000');
        $this->checkSame(0, $p->clientCallbackExecutorThreads, '默认回调线程 0（按核数）');
        $this->checkSame(10, $p->traceMsgBatchNum, '默认轨迹批量 10');
        $this->checkSame(false, $p->enableTrace, '默认轨迹关闭');
        $this->checkSame(RequestResponseFuture::DEFAULT_REQUEST_TIMEOUT_MILLIS, $p->requestTimeout, '默认 request 超时 3000');
        $this->checkSame(3000, $p->requestTimeout, 'DEFAULT_REQUEST_TIMEOUT_MILLIS=3000');
        $this->checkSame(8, count($p->retryResponseCodes), '默认可重试响应码 8 个');
        $this->checkSame(true, $p->isRetryResponseCode(1), 'SYSTEM_ERROR 可重试');
        $this->checkSame(true, $p->isRetryResponseCode(17), 'TOPIC_NOT_EXIST 可重试');
        $this->checkSame(false, $p->isRetryResponseCode(13), 'MESSAGE_ILLEGAL 不可重试');
        $this->checkSame(false, $p->isRetryResponseCode(null), 'null 码不可重试');
        $p->addRetryResponseCode(13);
        $this->checkSame(true, $p->isRetryResponseCode(13), 'addRetryResponseCode 生效');

        $this->checkSame(0, LocalTransactionState::COMMIT_MESSAGE->value, 'COMMIT_MESSAGE ordinal 0');
        $this->checkSame(1, LocalTransactionState::ROLLBACK_MESSAGE->value, 'ROLLBACK_MESSAGE ordinal 1');
        $this->checkSame(2, LocalTransactionState::UNKNOW->value, 'UNKNOW ordinal 2');

        // LocalTransactionState 常量位
        $this->checkSame(8, MessageSysFlag::TRANSACTION_COMMIT_TYPE, 'TRANSACTION_COMMIT_TYPE=8');
        $this->checkSame(12, MessageSysFlag::TRANSACTION_ROLLBACK_TYPE, 'TRANSACTION_ROLLBACK_TYPE=12');
        $this->checkSame(0, MessageSysFlag::TRANSACTION_NOT_TYPE, 'TRANSACTION_NOT_TYPE=0');
        $this->checkSame(4, MessageSysFlag::TRANSACTION_PREPARED_TYPE, 'TRANSACTION_PREPARED_TYPE=4');
    }

    // ================================================================ 2. 构造 / 配置 setter

    private function testConstructorAndSetters(): void
    {
        $this->checkThrows(fn() => new DefaultMQProducer('  '), MQClientException::class, '空组名构造抛 MQClientException');
        $this->checkThrows(fn() => new DefaultMQProducer(null), MQClientException::class, 'null 组名构造抛 MQClientException');

        $p = new DefaultMQProducer('GID-cfg', null, 'NS-CFG');
        $p->setNamesrvAddr(' 127.0.0.1:1 ; 127.0.0.2:2 ;');
        $this->checkSame(['127.0.0.1:1', '127.0.0.2:2'], $p->nameServerAddrs, 'setNamesrvAddr 分号拆分+去空白');
        $this->checkSame('127.0.0.1:1;127.0.0.2:2', $p->getNamesrvAddr(), 'getNamesrvAddr 分号拼接');
        $p->setNameServerAddresses(['a:1']);
        $this->checkSame(['a:1'], $p->nameServerAddrs, 'setNameServerAddresses 直接赋值');
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
        $this->checkSame(true, $p->sendLatencyFaultEnable, 'setSendLatencyFaultEnable 透传策略');
        $this->checkSame(true, $p->mqFaultStrategy->isSendLatencyFaultEnable(), 'MQFaultStrategy 开关同步');
        $p->setRequestTimeout(9999);
        $this->checkSame(9999, $p->requestTimeout, 'setRequestTimeout');
        $p->setTraceTopic('MY_TRACE');
        $this->checkSame('MY_TRACE', $p->traceTopic, 'setTraceTopic');
        $p->setTraceMsgBatchNum(20);
        $this->checkSame(20, $p->traceMsgBatchNum, 'setTraceMsgBatchNum');
        $p->setEnableTrace(true);
        $this->checkSame(true, $p->isEnableTrace(), 'set/isEnableTrace');
        $p->setAutoBatch(true);
        $this->checkSame(false, $p->getAutoBatch(), '未 start 时 getAutoBatch 恒 false（Java 短路）');
        $this->checkSame(0, $p->getBatchMaxDelayMs(), '累加器未建时 getBatchMaxDelayMs=0');
        $this->checkSame(0, $p->getBatchMaxBytes(), '累加器未建时 getBatchMaxBytes=0');
        $this->checkSame(0, $p->getTotalBatchMaxBytes(), '累加器未建时 getTotalBatchMaxBytes=0');

        // 背压 setter（含地板值）
        $p->setEnableBackpressureForAsyncMode(true);
        $this->checkSame(true, $p->isEnableBackpressureForAsyncMode(), 'set/isEnableBackpressureForAsyncMode');
        $p->setBackPressureForAsyncSendNum(5);
        $this->checkSame(10, $p->getBackPressureForAsyncSendNum(), '条数上限被抬到地板 10');
        $this->checkSame(10, $p->getSemaphoreAsyncSendNumAvailablePermits(), '条数信号量跟随 setter');
        $p->setBackPressureForAsyncSendSize(1024);
        $this->checkSame(1024 * 1024, $p->getBackPressureForAsyncSendSize(), '字节上限被抬到地板 1MB');
        $p->setBackPressureForAsyncSendNum(100);
        $this->checkSame(100, $p->getBackPressureForAsyncSendNum(), '正常条数上限直接生效');

        // 运行时改容量的语义：总量平移、在途保留（Java DefaultMQProducerTest:593-595）
        $sem = $this->privateOf($p, 'semaphoreAsyncSendNum');
        $sem->tryAcquire(30, 0); // 30 份在途
        $p->setBackPressureForAsyncSendNum(80);
        $this->checkSame(50, $p->getSemaphoreAsyncSendNumAvailablePermits(), '改容量后 空闲=新总量-在途');
        $sem->release(30);

        $this->checkThrows(fn() => $this->startedProducerSetGroup(), MQClientException::class, 'start 后 setProducerGroup 抛');
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

    // ================================================================ 3. 模块级 helper

    private function testStaticHelpers(): void
    {
        // backPressurePermits：>floor 透传；==floor / <floor 兜地板（Java :141-153）
        $this->checkSame(20, DefaultMQProducer::backPressurePermits(20, 10, 'x'), 'permits: 20>10 → 20');
        $this->checkSame(10, DefaultMQProducer::backPressurePermits(10, 10, 'x'), 'permits: ==floor 也走 else → 10');
        $this->checkSame(10, DefaultMQProducer::backPressurePermits(3, 10, 'x'), 'permits: 3<10 → 10');

        // backPressureMsgLen
        $empty = new Message('T', null);
        $this->checkSame(1, DefaultMQProducer::backPressureMsgLen($empty), 'msgLen: body=null → 1');
        $empty2 = new Message('T', '');
        $this->checkSame(1, DefaultMQProducer::backPressureMsgLen($empty2), 'msgLen: body=空串 → 1');
        $m3 = new Message('T', 'abcde');
        $this->checkSame(5, DefaultMQProducer::backPressureMsgLen($m3), 'msgLen: body 长度');
        $b1 = new Message('T', 'abcd');
        $b2 = new Message('T', 'ef');
        $batch = MessageBatch::generateFromList([$b1, $b2]);
        $this->checkSame(6, DefaultMQProducer::backPressureMsgLen($batch), 'msgLen: 批量按条累加');
        $this->checkSame(11, DefaultMQProducer::backPressureMsgLen([$m3, $batch]), 'msgLen: 列表累加（5 + 批量 6）');
        $emptyBatch = MessageBatch::generateFromList([new Message('T', null)]);
        $this->checkSame(1, DefaultMQProducer::backPressureMsgLen($emptyBatch), 'msgLen: 全空批 → 1');

        // maxDelayValue
        $plain = new Message('T', 'b');
        $this->checkSame(0, DefaultMQProducer::maxDelayValue($plain), 'maxDelay: 无延时属性 → 0');
        $d1 = new Message('T', 'b');
        $d1->putProperty('DELAY', '3');
        $this->checkSame(3, DefaultMQProducer::maxDelayValue($d1), 'maxDelay: DELAY=3');
        $d2 = new Message('T', 'b');
        $d2->putProperty('TIMER_DELIVER_MS', '5000');
        $this->checkSame(5000, DefaultMQProducer::maxDelayValue($d2), 'maxDelay: TIMER_DELIVER_MS');
        $d3 = new Message('T', 'b');
        $d3->putProperty('DELAY', '2');
        $d3->putProperty('TIMER_DELAY_SEC', '9');
        $this->checkSame(9, DefaultMQProducer::maxDelayValue($d3), 'maxDelay: 取最大值');
        $this->checkThrows(function () use ($d3) {
            $d3->putProperty('TIMER_DELAY_MS', 'abc');
            DefaultMQProducer::maxDelayValue($d3);
        }, \InvalidArgumentException::class, 'maxDelay: 非法值直接抛（对齐 Long.parseLong）');

        // classifyAsyncFailure：三分支 + 非 remoting 原样
        $cost = 33;
        [$e1, $r1] = DefaultMQProducer::classifyAsyncFailure(new RemotingSendRequestException('1.2.3.4:1'), $cost);
        $this->check($e1 instanceof MQClientException, 'send request failed → MQClientException');
        $this->checkSame(true, $r1, 'send request failed → 可重试');
        $this->check($e1->getPrevious() instanceof RemotingSendRequestException, '包装保留 cause');
        [$e2, $r2] = DefaultMQProducer::classifyAsyncFailure(new RemotingTimeoutException('a', 100), $cost);
        $this->check($e2 instanceof MQClientException, 'timeout → MQClientException');
        $this->check(str_contains($e2->getMessage(), 'wait response timeout, cost=33'), 'timeout 文案带 cost');
        $this->checkSame(true, $r2, 'timeout → 可重试');
        [$e3, $r3] = DefaultMQProducer::classifyAsyncFailure(new RemotingConnectException('a'), $cost);
        $this->checkSame('unknown reason', $e3->getMessage(), '其余 RemotingException → unknown reason');
        $this->checkSame(true, $r3, 'connect → 可重试');
        [$e4, $r4] = DefaultMQProducer::classifyAsyncFailure(new RemotingTooMuchRequestException('too much'), $cost);
        $this->checkSame('unknown reason', $e4->getMessage(), 'TooMuchRequest → unknown reason 包装');
        $this->checkSame(false, $r4, 'TooMuchRequest → 不可重试');
        $brokerErr = new MQBrokerException(1, 'busy');
        [$e5, $r5] = DefaultMQProducer::classifyAsyncFailure($brokerErr, $cost);
        $this->checkSame($brokerErr, $e5, 'MQBrokerException 原样返回（不包装）');
        $this->checkSame(false, $r5, 'MQBrokerException → 不可重试（不看 retryResponseCodes）');
    }

    // ================================================================ 4. 选择器

    private function testSelectors(): void
    {
        $mqs = [
            new MessageQueue('T', 'broker-a', 0),
            new MessageQueue('T', 'broker-b', 1),
            new MessageQueue('T', 'broker-c', 2),
        ];
        $msg = new Message('T', 'b');

        $byHash = new SelectMessageQueueByHash();
        $this->checkThrows(fn() => $byHash->select([], $msg, 'x'), MQClientException::class, 'ByHash: 空队列抛');
        $picked = $byHash->select($mqs, $msg, 'abc');
        $this->check(in_array($picked, $mqs, true), 'ByHash: 命中某个队列');
        $this->checkSame($byHash->select($mqs, $msg, 'abc'), $byHash->select($mqs, $msg, 'abc'), 'ByHash: 同 arg 同队列（确定性）');
        $this->checkSame($mqs[1], $byHash->select($mqs, $msg, 1), 'ByHash: int arg=1 → 队列 1（abs(1)%3）');
        $this->checkSame($mqs[0], $byHash->select($mqs, $msg, null), 'ByHash: null arg → 0 → 队列 0');
        $this->checkSame($mqs[2], $byHash->select($mqs, $msg, -2), 'ByHash: 负数取 abs');

        $byRandom = new SelectMessageQueueByRandom();
        $this->checkThrows(fn() => $byRandom->select([], $msg, null), MQClientException::class, 'ByRandom: 空队列抛');
        for ($i = 0; $i < 8; $i++) {
            $q = $byRandom->select($mqs, $msg, null);
            $this->check(in_array($q, $mqs, true), 'ByRandom: 结果在队列集合内');
        }

        $byRoom = new SelectMessageQueueByMachineRoom();
        $roomMqs = [
            new MessageQueue('T', 'bj-1', 0),
            new MessageQueue('T', 'sh-1', 1),
        ];
        $this->checkThrows(fn() => $byRoom->select([], $msg, null), MQClientException::class, 'ByMachineRoom: 空队列抛');
        $this->checkSame('sh-1', $byRoom->select($roomMqs, $msg, 'sh')->brokerName, 'ByMachineRoom: 前缀命中 sh');
        $this->checkSame('bj-1', $byRoom->select($roomMqs, $msg, 'bj')->brokerName, 'ByMachineRoom: 前缀命中 bj');
        $this->checkSame('bj-1', $byRoom->select($roomMqs, $msg, 'gz')->brokerName, 'ByMachineRoom: 无匹配回退首个');
        $this->check($byRoom instanceof MessageQueueSelector, '选择器实现 MessageQueueSelector 接口');
    }

    // ================================================================ 5. 回调包装 / 事务结果

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
        $this->checkSame($sr, $got, 'SendCallbackImpl: onSuccess 转发');
        $ex = new MQClientException('boom');
        $cb->onException($ex);
        $this->checkSame($ex, $err, 'SendCallbackImpl: onException 转发');
        $cb->onSuccess(null);
        $this->checkSame(null, $got, 'SendCallbackImpl: null result 透传');
        (new SendCallbackImpl())->onSuccess($sr); // 空实现不抛
        $this->check(true, 'SendCallbackImpl: 空回调安全');

        // NullSendCallback + future
        $future = new RequestResponseFuture('cid-1', 3000);
        (new NullSendCallback($future))->onSuccess($sr);
        $this->checkSame(true, $future->sendRequestOk, 'NullSendCallback: onSuccess → sendRequestOk=true');
        $future2 = new RequestResponseFuture('cid-2', 3000);
        $cause = new MQClientException('send fail');
        (new NullSendCallback($future2))->onException($cause);
        $this->checkSame(false, $future2->sendRequestOk, 'NullSendCallback: onException → sendRequestOk=false');
        $this->checkSame(null, $future2->responseMsg, 'NullSendCallback: putResponseMessage(null)');
        $this->checkSame($cause, $future2->cause, 'NullSendCallback: cause 写回');
        (new NullSendCallback())->onSuccess(null);
        $this->check(true, 'NullSendCallback: 无 future 安全');

        // BackPressureSendCallback：归还只还真正拿到的
        $semNum = new FairSemaphore(10);
        $semSize = new FairSemaphore(1024);
        $delegate = new CallbackSlot();
        $gated = new BackPressureSendCallback($delegate, $semNum, $semSize, 7);
        $this->check($semNum->tryAcquire(1, 0), '背压: 条数许可可得');
        $gated->numAcquired = true;
        $this->check($semSize->tryAcquire(7, 0), '背压: 字节许可可得');
        $gated->sizeAcquired = true;
        $this->checkSame(9, $semNum->availablePermits(), '背压: 扣减后条数 9');
        $this->checkSame(1017, $semSize->availablePermits(), '背压: 扣减后字节 1017');
        $gated->onSuccess($sr);
        $this->checkSame(10, $semNum->availablePermits(), '背压: 成功后条数全还');
        $this->checkSame(1024, $semSize->availablePermits(), '背压: 成功后字节全还（含 body=7）');
        $this->checkSame(1, $delegate->calls, '背压: 委托回调恰一次');
        $this->checkSame($sr, $delegate->result, '背压: 委托拿到结果');
        $this->checkSame(true, $gated->done, '背压: done 标记');
        // 重复回调不重复归还（加固：只还一次）
        $gated->onException(new MQClientException('late'));
        $this->checkSame(10, $semNum->availablePermits(), '背压: 重复回调不虚增条数');
        $this->checkSame(1024, $semSize->availablePermits(), '背压: 重复回调不虚增字节');
        $this->checkSame(2, $delegate->calls, '背压: 委托仍被再次调用（与 Java 直接转发一致）');

        // 只拿到条数、字节没拿到：只还条数
        $semNum2 = new FairSemaphore(10);
        $semSize2 = new FairSemaphore(1024);
        $gated2 = new BackPressureSendCallback(new CallbackSlot(), $semNum2, $semSize2, 7);
        $this->check($semNum2->tryAcquire(1, 0), '背压2: 条数许可可得');
        $gated2->numAcquired = true;
        $gated2->onException(new RemotingTooMuchRequestException('size timeout'));
        $this->checkSame(10, $semNum2->availablePermits(), '背压2: 条数归还');
        $this->checkSame(1024, $semSize2->availablePermits(), '背压2: 未拿到的字节不还（总量不变）');

        // TransactionSendResult
        $mq = new MessageQueue('T', 'b', 1);
        $base = new SendResult(SendStatus::SEND_OK, 'm-2', $mq, 42, 'tx-1', 'off-1', 'Region-A');
        $tsr = new TransactionSendResult($base, LocalTransactionState::COMMIT_MESSAGE);
        $this->checkSame(SendStatus::SEND_OK, $tsr->getSendStatus(), 'TransactionSendResult: 状态委托');
        $this->checkSame('m-2', $tsr->getMsgId(), 'TransactionSendResult: msgId 委托');
        $this->checkSame($mq, $tsr->getMessageQueue(), 'TransactionSendResult: mq 委托');
        $this->checkSame(42, $tsr->getQueueOffset(), 'TransactionSendResult: queueOffset 委托');
        $this->checkSame('tx-1', $tsr->getTransactionId(), 'TransactionSendResult: transactionId 委托');
        $this->checkSame('off-1', $tsr->getOffsetMsgId(), 'TransactionSendResult: offsetMsgId 委托');
        $this->checkSame('Region-A', $tsr->getRegionId(), 'TransactionSendResult: regionId 委托');
        $this->checkSame(LocalTransactionState::COMMIT_MESSAGE, $tsr->getLocalTransactionState(), 'TransactionSendResult: 本地事务状态');
        $tsr->setLocalTransactionState(LocalTransactionState::ROLLBACK_MESSAGE);
        $this->checkSame(LocalTransactionState::ROLLBACK_MESSAGE, $tsr->getLocalTransactionState(), 'TransactionSendResult: set 状态');
        $tsr->setTransactionId('tx-2');
        $this->checkSame('tx-2', $tsr->getTransactionId(), 'TransactionSendResult: setTransactionId 委托');
        $this->checkSame($base, $tsr->getSendResult(), 'TransactionSendResult: getSendResult 取回底层');
    }

    // ================================================================ 6. 生命周期 / 校验

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
        // 未 start
        $p = new DefaultMQProducer('GID-lc');
        $p->setNamesrvAddr('127.0.0.1:59999');
        $this->checkThrows(fn() => $p->send(new Message('T', 'b')), MQClientException::class, '未 start 发送抛 not started');
        $this->checkThrows(fn() => $p->sendAsync(new Message('T', 'b'), new CallbackSlot()), MQClientException::class, '未 start 异步发送抛 not started');
        $this->checkThrows(fn() => $p->sendOneway(new Message('T', 'b')), MQClientException::class, '未 start oneway 抛 not started');
        $p->shutdown(); // 未 start 的 shutdown 静默返回（不抛）
        $this->check(true, '未 start shutdown 静默（不抛）');

        // 组名校验（start 顺序：withNamespace → checkGroup → DEFAULT_PRODUCER 挡板）
        $p2 = new DefaultMQProducer(MixAll::DEFAULT_PRODUCER_GROUP);
        $p2->setNamesrvAddr('127.0.0.1:59999');
        $this->checkThrows(fn() => $p2->start(), MQClientException::class, 'start: DEFAULT_PRODUCER 组被挡');

        // 无 name server 地址（动态取址已在文件头清掉）
        $p3 = new DefaultMQProducer('GID-nons');
        $this->checkThrows(function () use ($p3): void {
            $p3->start();
        }, MQClientException::class, 'start: 无地址且无地址服务器 → 抛');
        try {
            $p3->start();
        } catch (MQClientException $e) {
            $this->checkSame(10004, $e->responseCode, 'start: 无地址故障码 10004');
        }

        // 消息校验（纯本地，无需 start）
        $p4 = new DefaultMQProducer('GID-lc2');
        $p4->setSendMsgTimeout(100);
        $blank = new Message('   ', 'b');
        $this->checkThrows(fn() => $this->invokePrivateCheckMessage($p4, $blank), MQClientException::class, 'checkMessage: 空 topic 抛');
        $oversize = new Message('T', str_repeat('x', 5 * 1024 * 1024));
        $this->checkThrows(fn() => $this->invokePrivateCheckMessage($p4, $oversize), MQClientException::class, 'checkMessage: 超长 body 抛');
        $nullBody = new Message('T', null);
        $this->checkThrows(fn() => $this->invokePrivateCheckMessage($p4, $nullBody), MQClientException::class, 'checkMessage: null body 抛');
    }

    private function invokePrivateCheckMessage(DefaultMQProducer $p, Message $msg): void
    {
        $m = new \ReflectionMethod(DefaultMQProducer::class, 'checkMessage');
        $m->setAccessible(true);
        $m->invoke($p, $msg);
    }

    // ================================================================ 7. 同步发送（真网络）

    private function testSyncSend(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-sync', 'SyncTopic', 'sync-inst-1');
        $hook = new RecordingSendHook();
        $p->registerSendMessageHook($hook);
        $p->start();

        $msg = new Message('SyncTopic', 'sync-body');
        $msg->setKeys('k1');
        $result = $p->send($msg);
        $this->check($result instanceof SendResult, '同步发送返回 SendResult');
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, '同步发送 SEND_OK');
        $this->checkSame('0A0000011F4000000000000000000064', $result->offsetMsgId, 'offsetMsgId 来自响应头');
        $this->checkSame(100, $result->queueOffset, 'queueOffset 来自响应头');
        $this->checkSame('tx-9', $result->transactionId, 'transactionId 透传');
        $this->checkSame('broker-a', $result->messageQueue->brokerName ?? '', '结果 mq.brokerName');
        $this->check(is_string($result->msgId) && $result->msgId !== '', 'msgId = 客户端 UNIQ_KEY');
        $this->checkSame(MixAll::DEFAULT_TRACE_REGION_ID, $result->regionId, 'regionId 缺省 DefaultRegion');
        $this->checkSame(1, $hook->before, 'SendMessageHook.before 调用 1 次');
        $this->checkSame(1, $hook->after, 'SendMessageHook.after 调用 1 次');
        $this->checkSame(SendStatus::SEND_OK, $hook->afterContexts[0]->sendResult->sendStatus ?? null, 'after 上下文带结果');
        $this->checkSame(CommunicationMode::SYNC, $hook->afterContexts[0]->communicationMode, 'after 上下文 communicationMode=SYNC');

        // broker 实际收到 SEND_MESSAGE_V2
        $this->checkSame(1, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'SyncTopic'), 'broker 收到 1 条 SEND_MESSAGE_V2');
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
        $this->checkSame(MixAll::DEFAULT_TOPIC, $send['ext']['c'] ?? '', 'send header c=defaultTopic（可配）');
        $this->checkSame('4', $send['ext']['d'] ?? '', 'send header d=defaultTopicQueueNums（可配）');
        $this->checkSame('false', $send['ext']['k'] ?? '', 'send header k=unitMode=false');

        // 同一条消息再发一次（还原语义：无重复压缩/无命名空间残留）
        $again = $p->send($msg);
        $this->checkSame(SendStatus::SEND_OK, $again->sendStatus, '同消息第二次发送 OK');

        // 定点发送
        $mq = new MessageQueue('SyncTopic', 'broker-a', 1);
        $pinned = $p->send(new Message('SyncTopic', 'pinned-body'), 3000, $mq);
        $this->checkSame(1, $pinned->messageQueue->queueId ?? -1, '定点发送 queueId=1');

        // 定点 topic 不一致 → 同步文案
        $badMq = new MessageQueue('OtherTopic', 'broker-a', 0);
        $this->checkThrows(
            fn() => $p->send(new Message('SyncTopic', 'x'), 3000, $badMq),
            MQClientException::class,
            '定点 topic 不一致抛 MQClientException'
        );
        try {
            $p->send(new Message('SyncTopic', 'x'), 3000, $badMq);
        } catch (MQClientException $e) {
            $this->checkSame(DefaultMQProducer::PINNED_TOPIC_MISMATCH_SYNC, $e->getMessage(), '定点守卫用同步文案');
        }

        // 轨迹上下文 msgType 判定
        $hook2 = new RecordingSendHook();
        $p->registerSendMessageHook($hook2);
        $delayMsg = new Message('SyncTopic', 'delay-body');
        $delayMsg->putProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL, '2');
        $p->send($delayMsg);
        $ctx = $hook2->afterContexts[0] ?? null;
        $this->check($ctx !== null && 'DELAY_MSG' === $ctx->msgType->name, '钩子上下文 msgType=Delay_Msg（带 DELAY）');
        $hook3 = new RecordingSendHook();
        $p->registerSendMessageHook($hook3);
        $p->send(new Message('SyncTopic', 'plain'));
        $ctx3 = $hook3->afterContexts[0] ?? null;
        $this->check($ctx3 !== null && 'NORMAL_MSG' === $ctx3->msgType->name, '钩子上下文 msgType=Normal_Msg');

        $p->shutdown();
        MQClientInstance::removeInstance('sync-inst-1-cid');
        $this->check(true, '同步发送套件 shutdown 完成');
    }

    // ================================================================ 8. 压缩与还原语义

    private function testCompressionAndRestore(ProducerFakeBroker $broker): void
    {
        $p = new DefaultMQProducer('GID-restore', null, 'PHPNS');
        $p->setInstanceName('restore-inst-1');
        $p->setNameServerAddresses([$broker->addr]);
        $p->start();

        // 1) 压缩触发 + body/topic 还原 + 无二次压缩
        $bigBody = str_repeat('A', 5000);
        $msg = new Message('SyncTopic', $bigBody);
        $result = $p->send($msg);
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, '压缩消息发送 OK');
        $this->checkSame('SyncTopic', $msg->getTopic(), '还原: topic 剥掉命名空间（PHPNS%SyncTopic → SyncTopic）');
        $this->checkSame($bigBody, $msg->getBody(), '还原: body 换回压缩前那一份');

        // broker 侧 sysFlag 带压缩位，且解一层就是原文（证明没有 zlib(zlib(x))）
        $sends = 0;
        foreach ($broker->logEntries() as $e) {
            $topic = $e['ext']['b'] ?? '';
            if (($e['code'] ?? 0) === RequestCode::SEND_MESSAGE_V2 && $topic === 'PHPNS%SyncTopic') {
                $sends++;
                $this->checkSame('769', (string) ($e['ext']['f'] ?? ''), '压缩 sysFlag=0x301(COMPRESSED|ZLIB)');
            }
            if (($e['code'] ?? -99) === -1) {
                $this->checkSame(5000, $e['decodedLen'] ?? -1, 'broker 解一层即原文长度 5000（无双重压缩）');
            }
        }
        $this->checkSame(1, $sends, '带命名空间的 topic 到达 broker');

        // 2) 失败路径也还原（finally）
        $bodyBefore = 'fail-body';
        $failMsg = new Message('FailTopic', $bodyBefore);
        try {
            $p->send($failMsg);
            $this->check(false, 'FailTopic 发送应抛');
        } catch (MQBrokerException $e) {
            $this->checkSame(13, $e->responseCode, 'FailTopic: 非可重试码原样抛 MQBrokerException(13)');
        }
        $this->checkSame($bodyBefore, $failMsg->getBody(), '还原: 失败路径 body 仍还原');
        $this->checkSame('FailTopic', $failMsg->getTopic(), '还原: 失败路径 topic 仍还原');

        // 3) 同一消息发两次，第二次 broker 依然解得开（还原语义核心）
        $msg2 = new Message('SyncTopic', str_repeat('B', 5000));
        $p->send($msg2);
        $p->send($msg2);
        $decoded = [];
        foreach ($broker->logEntries() as $e) {
            if (($e['code'] ?? -99) === -1) {
                $decoded[] = $e['decodedLen'];
            }
        }
        $this->checkSame(5000, $decoded[count($decoded) - 2] ?? -1, '第二次发送解一层=5000');
        $this->checkSame(5000, $decoded[count($decoded) - 1] ?? -1, '第三次发送解一层=5000');

        // 4) 定点发送走钩子（命名空间下定点守卫用拼过的名字比较）
        $pinnedMq = new MessageQueue('SyncTopic', 'broker-a', 0);
        $this->checkThrows(
            fn() => $p->send(new Message('SyncTopic', 'x'), 3000, $pinnedMq),
            MQClientException::class,
            '定点守卫比较的是各自拼过命名空间后的名字（SyncTopic vs PHPNS%SyncTopic）'
        );

        $p->shutdown();
        MQClientInstance::removeInstance('restore-inst-1-cid');
        $this->check(true, '还原套件 shutdown 完成');
    }

    // ================================================================ 9. 重试 / 失败分类

    private function testSyncRetryAndFailures(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-retry', 'RetryTopic', 'retry-inst-1');
        $p->start();

        // RetryTopic：SYSTEM_ERROR 可重试 → 重试 3 次（1+2）后按最后错误定性
        try {
            $p->send(new Message('RetryTopic', 'r-body'));
            $this->check(false, 'RetryTopic 发送应抛');
        } catch (MQClientException $e) {
            $this->check(str_contains($e->getMessage(), 'Send [3] times, still failed'), '重试耗尽文案 Send [3] times');
            $this->check(str_contains($e->getMessage(), 'BrokersSent: [broker-a, broker-a, broker-a]'), 'BrokersSent 记录 3 台');
            $this->checkSame(1, $e->responseCode, '最终码 = 最后 broker 错误码 SYSTEM_ERROR(1)');
        }
        $this->checkSame(3, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'RetryTopic'), 'broker 收到 3 次发送');

        // 路由拿不到：NOT_FOUND_TOPIC_EXCEPTION(10005) 定性
        $pNoRoute = $this->makeProducer($broker, 'GID-noroute', 'NoRouteTopic', 'noroute-inst-1');
        $pNoRoute->start();
        try {
            $pNoRoute->send(new Message('NoRouteTopic', 'x'));
            $this->check(false, 'NoRouteTopic 发送应抛');
        } catch (MQClientException $e) {
            $this->checkSame(10005, $e->responseCode, '路由缺失定性 10005');
        }
        $pNoRoute->shutdown();
        MQClientInstance::removeInstance('noroute-inst-1-cid');

        // 可控时钟：预算被吃光 → call timeout，一次请求都不发
        $pClock = $this->makeProducer($broker, 'GID-clock', 'ClockTopic', 'clock-inst-1');
        $pClock->start();
        $n = 0;
        $base = 1000000.0;
        $pClock->monoClock = static function () use (&$n, $base): float {
            return $base + (++$n) * 10000.0;
        };
        try {
            $pClock->send(new Message('ClockTopic', 'x'));
            $this->check(false, '时钟超预算应抛');
        } catch (RemotingTooMuchRequestException $e) {
            $this->checkSame('sendDefaultImpl call timeout', $e->getMessage(), 'call timeout 文案');
        }
        $this->checkSame(0, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'ClockTopic'), 'call timeout 一次请求都没发');
        $pClock->shutdown();
        MQClientInstance::removeInstance('clock-inst-1-cid');

        // 非可重试码在 FaultStrategy 开启时的记录（不隔离验证留给 Latency 套件）
        $p2 = $this->makeProducer($broker, 'GID-metric', 'SyncTopic', 'metric-inst-1');
        $p2->start();
        $p2->send(new Message('SyncTopic', 'metric-body'));
        $snap = $p2->getMetrics()->snapshot();
        $this->checkSame(1, $snap['sendCount'], 'metrics sendCount=1');
        $this->checkSame(0, $snap['sendFailureCount'], 'metrics sendFailureCount=0');
        try {
            $p2->send(new Message('FailTopic', 'x'));
        } catch (MQBrokerException) {
            $this->check(true, '非可重试码直接抛');
        }
        $snap2 = $p2->getMetrics()->snapshot();
        $this->checkSame(1, $snap2['sendFailureCount'], 'metrics sendFailureCount=1');
        $p2->shutdown();
        MQClientInstance::removeInstance('metric-inst-1-cid');

        $p->shutdown();
        MQClientInstance::removeInstance('retry-inst-1-cid');
        $this->check(true, '重试套件 shutdown 完成');
    }

    // ================================================================ 10. oneway

    private function testOneway(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-oneway', 'OnewayTopic', 'oneway-inst-1');
        $p->start();
        $msg = new Message('OnewayTopic', 'oneway-body');
        $p->sendOneway($msg);
        $this->check(true, 'sendOneway 不抛');
        $this->checkSame('OnewayTopic', $msg->getTopic(), 'oneway 消息 topic 还原');
        $this->checkSame(1, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'OnewayTopic'), 'broker 收到 oneway 发送');
        $onewayEntries = array_values(array_filter($broker->logEntries(), static fn ($e) =>
            ($e['code'] ?? 0) === RequestCode::SEND_MESSAGE_V2 && ($e['ext']['b'] ?? '') === 'OnewayTopic'));
        $this->checkSame(true, (bool) ($onewayEntries[0]['oneway'] ?? false), 'oneway 标记置位');
        // 定点 oneway
        $p->sendOneway(new Message('OnewayTopic', 'p2'), new MessageQueue('OnewayTopic', 'broker-a', 1));
        $this->checkSame(2, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'OnewayTopic'), '定点 oneway 到达');
        $p->shutdown();
        MQClientInstance::removeInstance('oneway-inst-1-cid');
        $this->check(true, 'oneway 套件 shutdown 完成');
    }

    // ================================================================ 11. 异步发送

    private function testAsyncSend(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-async', 'AsyncOkTopic', 'async-inst-1');
        $hook = new RecordingSendHook();
        $p->registerSendMessageHook($hook);
        $p->start();

        // 成功：注册回调 → 内部泵到完成 → onSuccess
        $slot = new CallbackSlot();
        $p->sendAsync(new Message('AsyncOkTopic', 'async-body'), $slot);
        $this->checkSame(1, $slot->calls, '异步成功: 回调恰一次');
        $this->check($slot->error === null, '异步成功: 无异常');
        $this->check($slot->result instanceof SendResult, '异步成功: 结果为 SendResult');
        $this->checkSame(SendStatus::SEND_OK, $slot->result->sendStatus ?? null, '异步成功: SEND_OK');
        $this->checkSame(1, $hook->before, '异步: before 钩子 1 次');
        $this->checkSame(1, $hook->after, '异步: after 钩子 1 次');
        $this->checkSame(1, $broker->logCount(RequestCode::SEND_MESSAGE_V2, 'AsyncOkTopic'), 'broker 收到异步发送');

        // 批量异步
        $slotB = new CallbackSlot();
        $p->sendAsync([new Message('AsyncOkTopic', 'b1'), new Message('AsyncOkTopic', 'b2')], $slotB);
        $this->checkSame(1, $slotB->calls, '批量异步: 回调恰一次');
        $this->check($slotB->result !== null && SendStatus::SEND_OK === $slotB->result->sendStatus, '批量异步: 成功');
        $this->checkSame(1, $broker->logCount(RequestCode::SEND_BATCH_MESSAGE, 'AsyncOkTopic'), '批量异步走 SEND_BATCH_MESSAGE');

        // 定点异步 + 定点 topic 不一致 → 异步文案
        $slotP = new CallbackSlot();
        $p->sendAsync(new Message('AsyncOkTopic', 'p'), $slotP, 3000, new MessageQueue('AsyncOkTopic', 'broker-a', 1));
        $this->check($slotP->result !== null, '定点异步成功');
        $slotBad = new CallbackSlot();
        $p->sendAsync(new Message('AsyncOkTopic', 'x'), $slotBad, 3000, new MessageQueue('OtherTopic', 'broker-a', 0));
        $this->check($slotBad->error instanceof MQClientException, '定点异步 topic 不一致 → 回调异常');
        $this->checkSame(
            DefaultMQProducer::PINNED_TOPIC_MISMATCH_ASYNC,
            $slotBad->error->getMessage() ?? '',
            '定点异步用异步文案'
        );

        // broker 明确回错：原样回调 MQBrokerException（异步不看 retryResponseCodes）
        $slotF = new CallbackSlot();
        $p->sendAsync(new Message('AsyncFailTopic', 'x'), $slotF);
        $this->checkSame(1, $slotF->calls, '异步失败: 回调恰一次');
        $this->check($slotF->error instanceof MQBrokerException, '异步失败: broker 码原样 MQBrokerException');

        // 连接失败：换 opaque 重试 retryTimesWhenSendAsyncFailed=2 → 共 3 次内核尝试
        $pGhost = $this->makeProducer($broker, 'GID-ghost', 'GhostTopic', 'ghost-inst-1');
        $forbidden = new ThrowingForbiddenHook(); // 每次 sendKernelAsync 都跑 → 计内核尝试数
        $pGhost->registerCheckForbiddenHook($forbidden);
        $pGhost->start();
        $slotG = new CallbackSlot();
        $pGhost->sendAsync(new Message('GhostTopic', 'x'), $slotG);
        $this->checkSame(1, $slotG->calls, '连接失败: 回调恰一次');
        $this->check($slotG->error instanceof MQClientException, '连接失败: 包装为 MQClientException');
        $this->checkSame('unknown reason', $slotG->error->getMessage() ?? '', '连接失败: unknown reason 文案');
        $this->check($slotG->error->getPrevious() instanceof RemotingConnectException, '连接失败: cause 保留');
        $this->checkSame(3, $forbidden->calls, '连接失败: 内核尝试 1+2 次（forbidden 钩子计数）');

        // 背压：信号量掏空 → tryAcquire 失败 → RemotingTooMuchRequestException 文案
        $pBp = $this->makeProducer($broker, 'GID-bp', 'AsyncOkTopic', 'bp-inst-1');
        $pBp->setEnableBackpressureForAsyncMode(true);
        $pBp->setBackPressureForAsyncSendNum(10);
        $pBp->start();
        $semNum = $this->privateOf($pBp, 'semaphoreAsyncSendNum');
        $semNum->tryAcquire(10, 0);
        $slotBp = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 'x'), $slotBp, 500);
        $this->checkSame(1, $slotBp->calls, '背压打满: 回调恰一次');
        $this->check($slotBp->error instanceof RemotingTooMuchRequestException, '背压打满: RemotingTooMuchRequestException');
        $this->checkSame('send message tryAcquire semaphoreAsyncNum timeout', $slotBp->error->getMessage() ?? '', '背压打满: 条数闸文案');
        $semNum->release(10);
        // 字节闸
        $semSize = $this->privateOf($pBp, 'semaphoreAsyncSendSize');
        $semSize->tryAcquire($semSize->totalPermits(), 0);
        $slotBp2 = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 'x'), $slotBp2, 500);
        $this->checkSame('send message tryAcquire semaphoreAsyncSize timeout', $slotBp2->error->getMessage() ?? '', '背压打满: 字节闸文案');
        $semSize->release($semSize->totalPermits());
        // 释放后恢复：正常发送 + 许可归还
        $slotBp3 = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 'ok'), $slotBp3);
        $this->check($slotBp3->result !== null, '背压释放后发送成功');
        $this->checkSame(10, $pBp->getSemaphoreAsyncSendNumAvailablePermits(), '回调后条数许可归还');
        $this->checkSame(1024 * 1024, $pBp->getSemaphoreAsyncSendSizeAvailablePermits(), '回调后字节许可归还');

        // 关背压 + 队列未满：连发两次都成功
        $pBp->setEnableBackpressureForAsyncMode(false);
        $s1 = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 's1'), $s1);
        $s2 = new CallbackSlot();
        $pBp->sendAsync(new Message('AsyncOkTopic', 's2'), $s2);
        $this->check($s1->result !== null && $s2->result !== null, '连发两次异步均成功');

        $pBp->shutdown();
        MQClientInstance::removeInstance('bp-inst-1-cid');
        $pGhost->shutdown();
        MQClientInstance::removeInstance('ghost-inst-1-cid');
        $p->shutdown();
        MQClientInstance::removeInstance('async-inst-1-cid');
        $this->check(true, '异步套件 shutdown 完成');
    }

    // ================================================================ 12. 队列选择器发送

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
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, '选择器发送 OK');
        $qid = $result->messageQueue->queueId ?? -1;
        $this->check($qid >= 0 && $qid < 2, '选择器发送命中合法队列');
        // broker 收到的 queueId 与选择结果一致
        $found = false;
        foreach ($broker->logEntries() as $e) {
            if (($e['code'] ?? 0) === RequestCode::SEND_MESSAGE_V2 && ($e['ext']['b'] ?? '') === 'SelTopic'
                && (string) ($e['ext']['e'] ?? '') === (string) $qid) {
                $found = true;
                break;
            }
        }
        $this->check($found, 'broker 收到的 queueId 与选择器一致');
        $this->checkSame(1, $forbidden->calls, 'CheckForbidden 随发送执行');
        $this->checkSame('order-42', $forbidden->lastArg, 'arg 透传到 CheckForbiddenContext');

        $byRandom = new SelectMessageQueueByRandom();
        $r2 = $p->sendBySelector(new Message('SelTopic', 'r'), $byRandom, null, 3000);
        $this->checkSame(SendStatus::SEND_OK, $r2->sendStatus, 'ByRandom 发送 OK');
        $byRoom = new SelectMessageQueueByMachineRoom();
        $r3 = $p->sendBySelector(new Message('SelTopic', 'r'), $byRoom, 'broker', 3000);
        $this->checkSame(SendStatus::SEND_OK, $r3->sendStatus, 'ByMachineRoom 发送 OK（broker- 前缀命中）');

        // 拦截钩子抛异常：不吞、沿重试链向上（最终 Send [N] times）
        $throwing = new ThrowingForbiddenHook();
        $p2 = $this->makeProducer($broker, 'GID-sel2', 'SelTopic', 'sel2-inst-1');
        $p2->registerCheckForbiddenHook($throwing);
        $p2->start();
        try {
            $p2->sendBySelector(new Message('SelTopic', 'x'), $byHash, 'a', 3000);
            $this->check(false, '拦截钩子异常应传播');
        } catch (MQClientException $e) {
            $this->check(str_contains($e->getMessage(), 'forbidden by test hook'), '拦截异常出现在最终信息里');
            $this->checkSame(10003, $e->responseCode, '客户端自身故障定性 10003');
        }
        $this->checkSame(3, $throwing->calls, '拦截钩子每轮重试都执行（3 次）');

        $p2->shutdown();
        MQClientInstance::removeInstance('sel2-inst-1-cid');
        $p->shutdown();
        MQClientInstance::removeInstance('sel-inst-1-cid');
        $this->check(true, '选择器套件 shutdown 完成');
    }

    // ================================================================ 13. 批量发送

    private function testBatchSend(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-batch', 'BatchTopic', 'batch-inst-1');
        $p->start();

        $m1 = new Message('BatchTopic', 'batch-1');
        $m2 = new Message('BatchTopic', 'batch-2');
        $result = $p->send([$m1, $m2]);
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, '批量同步发送 OK');
        $this->checkSame(1, $broker->logCount(RequestCode::SEND_BATCH_MESSAGE, 'BatchTopic'), '批量走 SEND_BATCH_MESSAGE');
        // 子消息的 topic 已被还原到原始值（批量不还原，但我们发的本来就是原始值）
        $this->checkSame('BatchTopic', $m1->getTopic(), '批量子消息 topic 不被还原改写');

        $this->checkThrows(fn() => $p->send([]), MQClientException::class, '空批量抛 message list is empty');

        // 批量定点不一致 → 同步文案
        $badMq = new MessageQueue('OtherTopic', 'broker-a', 0);
        $this->checkThrows(
            fn() => $p->send([new Message('BatchTopic', 'x')], 3000, $badMq),
            MQClientException::class,
            '批量定点不一致抛同步文案'
        );

        $p->shutdown();
        MQClientInstance::removeInstance('batch-inst-1-cid');
        $this->check(true, '批量套件 shutdown 完成');
    }

    // ================================================================ 14. 事务消息

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
        $this->check($txResult instanceof TransactionSendResult, '事务发送返回 TransactionSendResult');
        $this->checkSame(SendStatus::SEND_OK, $txResult->getSendStatus(), '事务: SEND_OK');
        $this->checkSame(LocalTransactionState::COMMIT_MESSAGE, $txResult->getLocalTransactionState(), '事务: 本地状态 COMMIT');
        $this->checkSame('TransTopic', $msg->getTopic(), '事务: 调用方 topic 还原');
        $this->checkSame('trans-body', $msg->getBody(), '事务: 调用方 body 还原（executeLocalTransaction 看到原文）');
        // END_TRANSACTION(37) oneway 到达 broker：commitOrRollback=8, fromTransactionCheck=false
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $this->check(count($endEntries) >= 1, 'broker 收到 END_TRANSACTION');
        $first = $endEntries[0] ?? [];
        $this->checkSame('8', (string) ($first['ext']['commitOrRollback'] ?? ''), 'COMMIT → commitOrRollback=8');
        $this->checkSame('false', (string) ($first['ext']['fromTransactionCheck'] ?? ''), '主动收尾 fromTransactionCheck=false');
        $this->checkSame('100', (string) ($first['ext']['tranStateTableOffset'] ?? ''), 'tranStateTableOffset 取 queueOffset');
        $this->checkSame('100', (string) ($first['ext']['commitLogOffset'] ?? ''), 'commitLogOffset=decodeMessageId(offsetMsgId) 的偏移');
        $this->checkSame('GID-trans', $first['ext']['producerGroup'] ?? '', 'END_TRANSACTION 带 producerGroup');
        $this->checkSame(true, (bool) ($first['oneway'] ?? false), 'END_TRANSACTION 是 oneway');
        $this->checkSame(1, $endHook->calls, 'EndTransactionHook 执行 1 次');
        $this->checkSame(LocalTransactionState::COMMIT_MESSAGE, $endHook->contexts[0]->transactionState ?? null, 'EndTransactionHook 上下文带状态');

        // ROLLBACK
        $listener2 = new ScriptedTransactionListener(LocalTransactionState::ROLLBACK_MESSAGE);
        $p->sendMessageInTransaction(new Message('TransTopic', 'r'), $listener2);
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $this->checkSame('12', (string) ($endEntries[1]['ext']['commitOrRollback'] ?? ''), 'ROLLBACK → commitOrRollback=12');

        // UNKNOW
        $listener3 = new ScriptedTransactionListener(LocalTransactionState::UNKNOW);
        $p->sendMessageInTransaction(new Message('TransTopic', 'u'), $listener3);
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $this->checkSame('0', (string) ($endEntries[2]['ext']['commitOrRollback'] ?? ''), 'UNKNOW → commitOrRollback=0');

        // 本地事务抛异常 → UNKNOW + remark
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
        $this->checkSame(LocalTransactionState::UNKNOW, $txBoom->getLocalTransactionState(), '本地事务异常 → UNKNOW');
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $this->check(count($endEntries) >= 4, 'END_TRANSACTION 请求共 4 条');

        // broker 回查（TransCheckTopic）：响应后 broker 推 39，内联回调 listener.checkLocalTransaction
        // → END_TRANSACTION(fromTransactionCheck=true)
        $checkListener = new ScriptedTransactionListener(LocalTransactionState::COMMIT_MESSAGE);
        $checkListener->checkState = LocalTransactionState::ROLLBACK_MESSAGE;
        $checkCountBefore = $checkListener->checkCalls;
        $p->sendMessageInTransaction(new Message('TransCheckTopic', 'trans-body'), $checkListener);
        // 追加一次普通发送，把 socket 里遗留的 39 推送帧读出来（单线程泵模型）
        $p->send(new Message('TransTopic', 'flusher'));
        $this->checkSame($checkCountBefore + 1, $checkListener->checkCalls, '回查: checkLocalTransaction 被调用 1 次');
        $this->check($checkListener->lastChecked !== null, '回查: 收到 MessageExt');
        $this->checkSame('trans-body', $checkListener->lastChecked->getBody() ?? '', '回查: MessageExt body');
        $this->checkSame('GID-trans', $checkListener->lastChecked->getProperty(MessageConst::PROPERTY_PRODUCER_GROUP), '回查: PGROUP 属性匹配');
        $endEntries = array_values(array_filter($broker->logEntries(), static fn ($e) => ($e['code'] ?? 0) === RequestCode::END_TRANSACTION));
        $checkEnd = null;
        foreach ($endEntries as $e) {
            if (($e['ext']['fromTransactionCheck'] ?? '') === 'true') {
                $checkEnd = $e;
                break;
            }
        }
        $this->check($checkEnd !== null, '回查收尾 END_TRANSACTION(fromTransactionCheck=true) 到达 broker');
        $this->checkSame('12', (string) ($checkEnd['ext']['commitOrRollback'] ?? ''), '回查回滚 → commitOrRollback=12');
        $this->checkSame('7', (string) ($checkEnd['ext']['tranStateTableOffset'] ?? ''), '回查: 偏移取回查 header');
        $this->checkSame('99', (string) ($checkEnd['ext']['commitLogOffset'] ?? ''), '回查: commitLogOffset 取回查 header');

        // 延迟属性被拒
        $delayed = new Message('TransTopic', 'd');
        $delayed->putProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL, '1');
        $this->checkThrows(
            fn() => $p->sendMessageInTransaction($delayed, $checkListener),
            MQClientException::class,
            '事务消息拒绝延迟投递'
        );

        // TransactionMQProducer 无 listener → 抛
        $noListener = new TransactionMQProducer('GID-trans-none');
        $noListener->setInstanceName('trans-none-inst');
        $noListener->setNameServerAddresses([$broker->addr]);
        $noListener->start();
        $this->checkThrows(
            fn() => $noListener->sendMessageInTransaction(new Message('TransTopic', 'x')),
            MQClientException::class,
            '无事务监听器抛 transaction listener is not set'
        );
        $noListener->shutdown();
        MQClientInstance::removeInstance('trans-none-inst-cid');

        $p->shutdown();
        MQClientInstance::removeInstance('trans-inst-1-cid');
        $this->check(true, '事务套件 shutdown 完成');
    }

    // ================================================================ 15. Request-Reply

    private function testRequestReply(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-request', 'RequestTopic', 'request-inst-1');
        $p->start();

        $reply = $p->request(new Message('RequestTopic', 'request-body'), 5000);
        $this->check($reply instanceof MessageExt, 'request 返回应答消息');
        $this->checkSame('reply-body', $reply->getBody(), '应答 body');
        $this->check(is_string($reply->getProperty(MessageConst::PROPERTY_CORRELATION_ID)), '应答带 CORRELATION_ID');
        // 等待槽被清理
        $this->checkSame(0, count(RequestFutureHolder::getInstance()->requestFutureTable), 'request 后等待槽清空');
        // 请求带 TTL / REPLY_TO_CLIENT
        $saw = false;
        foreach ($broker->logEntries() as $e) {
            if (($e['code'] ?? 0) === RequestCode::SEND_MESSAGE_V2 && ($e['ext']['b'] ?? '') === 'RequestTopic') {
                $props = MessageDecoder::string2MessageProperties((string) ($e['ext']['i'] ?? ''));
                $saw = isset($props[MessageConst::PROPERTY_MESSAGE_TTL])
                    && isset($props[MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT])
                    && isset($props[MessageConst::PROPERTY_CORRELATION_ID]);
            }
        }
        $this->check($saw, '请求消息带 CORRELATION_ID / REPLY_TO_CLIENT / TTL');
        // 心跳补发（REPLY_TO_CLIENT 依赖 broker 认识本客户端）
        $this->check($broker->logCount(RequestCode::HEARTBEAT) >= 1, 'request 前 broker 收到心跳');

        // 发送本身失败 → MQClientException（不等满 timeout）
        $pFail = $this->makeProducer($broker, 'GID-request2', 'AsyncFailTopic', 'request2-inst-1');
        $pFail->start();
        $t0 = microtime(true);
        try {
            $pFail->request(new Message('AsyncFailTopic', 'x'), 5000);
            $this->check(false, 'request 失败应抛');
        } catch (MQClientException $e) {
            $this->check(str_contains($e->getMessage(), 'send request message to <AsyncFailTopic> fail'), '发送失败文案');
            $this->check($e->getPrevious() instanceof MQBrokerException, '失败 cause 是 broker 异常');
        }
        $elapsed = microtime(true) - $t0;
        $this->check($elapsed < 4.5, '发送失败不等满 timeout（耗时 ' . round($elapsed, 2) . 's）');
        $pFail->shutdown();
        MQClientInstance::removeInstance('request2-inst-1-cid');

        $p->shutdown();
        MQClientInstance::removeInstance('request-inst-1-cid');
        $this->check(true, 'request-reply 套件 shutdown 完成');
    }

    // ================================================================ 16. 撤回 / 管理能力

    private function testAdminAndRecall(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-admin', 'RecallTopic', 'admin-inst-1');
        $p->start();

        $handle = RecallMessageHandle::buildHandle('RecallTopic', 'broker-a', '123', 'msg-1');
        $uniq = $p->recallMessage('RecallTopic', $handle);
        $this->checkSame('recall-uniq-1', $uniq, 'recallMessage 返回 broker 的 msgId');
        $this->checkSame(1, $broker->logCount(RequestCode::RECALL_MESSAGE), 'broker 收到 RECALL_MESSAGE');

        $this->checkThrows(
            fn() => $p->recallMessage('RecallTopic', 'not-a-valid-handle!!'),
            MQClientException::class,
            '非法句柄抛 recall handle is invalid'
        );
        $this->checkThrows(
            fn() => $p->recallMessage('%RETRY%GID-admin', $handle),
            MQClientException::class,
            'retry topic 不支持撤回'
        );

        // 管理查询（route 已缓存 broker-a）
        $mq = new MessageQueue('RecallTopic', 'broker-a', 0);
        $this->checkSame(777, $p->maxOffset($mq), 'maxOffset');
        $this->checkSame(11, $p->minOffset($mq), 'minOffset');
        $this->checkSame(12345, $p->earliestMsgStoreTime($mq), 'earliestMsgStoreTime');
        $this->checkSame([], $p->queryMessage('RecallTopic', 'k', 32, 0, 0), 'queryMessage 空结果 → []');

        $queues = $p->fetchPublishMessageQueues('RecallTopic');
        $this->checkSame(2, count($queues), 'fetchPublishMessageQueues 2 个队列');
        $this->checkSame('broker-a', $queues[0]->brokerName, '队列 brokerName');

        $this->checkThrows(fn() => $p->viewMessage('T', 'id'), MQClientException::class, 'viewMessage 不支持');
        $this->checkThrows(fn() => $p->createTopic('k', 'SCHEDULE_TOPIC_XXX'), MQClientException::class, 'createTopic 拒绝系统 topic');

        $p->shutdown();
        MQClientInstance::removeInstance('admin-inst-1-cid');
        $this->check(true, '管理套件 shutdown 完成');
    }

    // ================================================================ 17. 自动攒批

    private function testAutoBatch(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-acc', 'AccTopic', 'acc-inst-1');
        $p->setAutoBatch(true);
        $p->setBatchMaxDelayMs(200);
        $p->setBatchMaxBytes(32 * 1024);
        $p->start();
        $this->checkSame(true, $p->getAutoBatch(), 'start 后 getAutoBatch=true');
        $this->checkSame(200, $p->getBatchMaxDelayMs(), '阈值同步到累加器（delayMs）');
        $this->checkSame(32 * 1024, $p->getBatchMaxBytes(), '阈值同步到累加器（bytes）');
        $this->check($p->produceAccumulator !== null, 'start 后累加器已建');

        $msg = new Message('AccTopic', 'acc-body-1');
        $result = $p->send($msg);
        $this->checkSame(SendStatus::SEND_OK, $result->sendStatus, '自动攒批直通发送 OK');
        $this->check(is_string($result->msgId) && $result->msgId !== '', '攒批返回本条 msgId');

        // 延时消息不能攒批 → 直发（等价路径）
        $delayed = new Message('AccTopic', 'd');
        $delayed->putProperty('DELAY', '1');
        $r2 = $p->send($delayed);
        $this->checkSame(SendStatus::SEND_OK, $r2->sendStatus, '延时消息退回直发 OK');

        // 异步走累加器：回调被驱动
        $slot = new CallbackSlot();
        $p->sendAsync(new Message('AccTopic', 'async-acc'), $slot);
        $this->checkSame(1, $slot->calls, '攒批异步回调恰一次');
        $this->check($slot->result !== null, '攒批异步成功');

        $p->shutdown();
        MQClientInstance::removeInstance('acc-inst-1-cid');
        $this->check(true, '攒批套件 shutdown 完成');
    }

    // ================================================================ 18. 轨迹分发器（enableTrace）

    private function testTraceDispatcherWiring(ProducerFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-trace', 'TraceTopic', 'trace-inst-1');
        $p->setEnableTrace(true);
        $p->start();
        $this->check($p->traceDispatcher instanceof AsyncTraceDispatcher, 'enableTrace → AsyncTraceDispatcher 建立');
        $this->checkSame(1, count(array_filter($p->sendMessageHookList, static fn ($h) => $h instanceof \RocketMQ\Client\SendMessageTraceHook)), '注册 SendMessageTraceHook');
        $this->checkSame(1, count(array_filter($p->endTransactionHookList, static fn ($h) => $h instanceof \RocketMQ\Client\EndTransactionTraceHook)), '注册 EndTransactionTraceHook');
        $this->check($p->traceDispatcher->traceProducer !== null, '内部轨迹生产者建立');
        $tp = $p->traceDispatcher->traceProducer;
        $this->check($tp instanceof DefaultMQProducer, '内部轨迹生产者是 DefaultMQProducer');
        if ($tp instanceof DefaultMQProducer) {
            $this->checkSame(false, $tp->enableTrace, '内部轨迹生产者 enableTrace=false（防递归）');
        }
        $p->send(new Message('TraceTopic', 'trace-body'));
        $this->check($p->traceDispatcher->batchNum >= 1, '轨迹 batchNum >= 1');
        // 轨迹队列收集了 Pub 上下文（after 钩子 append）
        $p->traceDispatcher->pumpOnce(true); // 强制刷写（无路由时只记日志，不抛）
        $this->check(true, '轨迹 pumpOnce 不抛');
        $p->shutdown();
        MQClientInstance::removeInstance('trace-inst-1-cid');
        $this->check(true, '轨迹套件 shutdown 完成');
    }

    // ================================================================ helpers

    private function privateOf(object $obj, string $name): mixed
    {
        $r = new \ReflectionProperty(get_class($obj), $name);
        $r->setAccessible(true);
        return $r->getValue($obj);
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
