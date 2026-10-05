<?php

declare(strict_types=1);

/**
 * MQClientInstance 纯 PHP assert 风格自测（不依赖 phpunit，不连真机）。
 *
 * 运行：C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunClientInstance.php
 * 全部通过输出 "ALL TESTS PASSED (N checks)"，任一失败列出明细并以非零码退出。
 *
 * 网络相关用例（路由拉取/默认 topic 回退、PULL、POP、offset 等）用一个本地
 * stream_socket_server 假 broker 承载；本文件以 `--broker` 参数二次拉起自身作为
 * **子进程** broker，从而让"客户端同步 call 阻塞"与"服务端读帧回包"能真正并发，
 * 避开单线程里两个连续同步 RPC 无法交错推进的问题。其余纯逻辑用例不启进程。
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PopStatus;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\RequestFutureHolder;
use RocketMQ\Client\RequestResponseFuture;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\TopicPublishInfo;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageBatch;
use RocketMQ\Common\MessageClientIdSetter;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Common\PullSysFlag;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\PopMessageResponseHeader;
use RocketMQ\Remoting\Protocol\PullMessageResponseHeader;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\ReplyMessageRequestHeader;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\SendMessageRequestHeader;
use RocketMQ\Remoting\Protocol\SendMessageRequestHeaderV2;
use RocketMQ\Remoting\Protocol\SendMessageResponseHeader;
use RocketMQ\Remoting\Protocol\TopicRouteData;

// ======================================================================
// 子进程假 broker：`php RunClientInstance.php --broker`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    brokerMain();
    exit(0);
}

/** 读满 n 字节；EOF 返回 null。 */
function brokerReadExact($conn, int $n): ?string
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

/** 读一整帧；EOF 返回 null。 */
function brokerReadFrame($conn): ?RemotingCommand
{
    $head = brokerReadExact($conn, 4);
    if ($head === null) {
        return null;
    }
    $total = RocketMQSerializable::unpackSignedInt($head);
    if ($total <= 0) {
        return null;
    }
    $rest = brokerReadExact($conn, $total);
    if ($rest === null) {
        return null;
    }
    return RemotingCommand::decode($head . $rest);
}

/** 构造一条 route body。 */
function brokerRouteBody(string $brokerName, string $addr, int $write, int $read): string
{
    $qd = new QueueData($brokerName, $read, $write, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', $brokerName, [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

/** 用 17 段格式编码一条消息，拼进 PULL/POP 响应体。 */
function brokerEncodeMsg(string $topic, string $body, int $queueId, int $queueOffset): string
{
    $m = new MessageExt(topic: $topic, body: $body);
    $m->setQueueId($queueId);
    $m->setQueueOffset($queueOffset);
    $m->setBornTimestamp(1_700_000_000_000);
    $m->setStoreTimestamp(1_700_000_000_000);
    return MessageDecoder::encodeMessageExt($m);
}

/**
 * 按请求码/字段回一个响应；返回 null 表示不回（oneway）。
 */
function brokerRespond(RemotingCommand $cmd): ?RemotingCommand
{
    $ext = $cmd->extFields;

    switch ($cmd->code) {
        case RequestCode::GET_ROUTEINFO_BY_TOPIC: {
            $topic = (string) ($ext['topic'] ?? '');
            if ($topic === 'RouteTopic') {
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->body = brokerRouteBody('broker-a', '127.0.0.1:10911', 2, 2);
                return $resp;
            }
            if ($topic === MixAll::DEFAULT_TOPIC) {
                // 默认 topic：队列数 8 > DEFAULT_TOPIC_QUEUE_NUMS(4)，用于验证裁剪。
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->body = brokerRouteBody('broker-def', '127.0.0.1:10912', 8, 8);
                return $resp;
            }
            return RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST);
        }

        case RequestCode::PULL_MESSAGE:
        case RequestCode::LITE_PULL_MESSAGE: {
            $qid = (int) ($ext['queueId'] ?? 0);
            // 把实际收到的请求码写进 remark，供调用方断言 V1/PULL vs LITE_PULL 选择。
            $remark = 'reqCode=' . $cmd->code;
            switch ($qid) {
                case 0: {
                    $h = new PullMessageResponseHeader(
                        nextBeginOffset: 10,
                        minOffset: 1,
                        maxOffset: 100,
                        suggestWhichBrokerId: 1,
                    );
                    $resp = RemotingCommand::createResponseCommandWithHeader(ResponseCode::SUCCESS, $h);
                    $resp->remark = $remark;
                    $resp->body = brokerEncodeMsg('PullTopic', 'pull-body-0', 0, 5);
                    return $resp;
                }
                case 1:
                    return RemotingCommand::createResponseCommand(ResponseCode::PULL_NOT_FOUND, $remark);
                case 2:
                    return RemotingCommand::createResponseCommand(ResponseCode::PULL_OFFSET_MOVED, $remark);
                case 3:
                    return RemotingCommand::createResponseCommand(ResponseCode::PULL_RETRY_IMMEDIATELY, $remark);
                default:
                    return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, $remark);
            }
        }

        case RequestCode::POP_MESSAGE: {
            $qid = (int) ($ext['queueId'] ?? 0);
            if ($qid === 0) {
                $h = new PopMessageResponseHeader(
                    popTime: 111,
                    invisibleTime: 222,
                    reviveQid: 333,
                    restNum: 7,
                    startOffsetInfo: '0 0 5',
                    msgOffsetInfo: '0 0 5,6',
                );
                $resp = RemotingCommand::createResponseCommandWithHeader(ResponseCode::SUCCESS, $h);
                $resp->body = brokerEncodeMsg('PopTopic', 'pop-body-5', 0, 5)
                    . brokerEncodeMsg('PopTopic', 'pop-body-6', 0, 6);
                return $resp;
            }
            if ($qid === 9) {
                return RemotingCommand::createResponseCommand(ResponseCode::POLLING_FULL);
            }
            return RemotingCommand::createResponseCommand(ResponseCode::POLLING_TIMEOUT);
        }

        case RequestCode::QUERY_CONSUMER_OFFSET: {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '42';
            return $resp;
        }
    }

    return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'unhandled code ' . $cmd->code);
}

function brokerServeConn($conn): void
{
    stream_set_blocking($conn, true);
    while (($cmd = brokerReadFrame($conn)) !== null) {
        try {
            $resp = brokerRespond($cmd);
        } catch (\Throwable $e) {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'broker error: ' . $e->getMessage());
        }
        if ($resp !== null) {
            $resp->opaque = $cmd->opaque;
            @fwrite($conn, $resp->encode());
        }
    }
}

function brokerMain(): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "broker: cannot listen: {$errstr}\n");
        return;
    }
    fwrite(STDOUT, stream_socket_get_name($server, false) . "\n");
    fflush(STDOUT);

    // 串行服务连接：每条连接处理到客户端主动关闭（shutdown）为止。
    while (true) {
        $conn = @stream_socket_accept($server, 30);
        if ($conn === false) {
            break;
        }
        brokerServeConn($conn);
        @fclose($conn);
    }
    @fclose($server);
}

/**
 * 子进程假 broker 句柄。
 */
final class FakeBroker
{
    /** @var resource */
    private $proc;

    /** @var array<int, resource> */
    private array $pipes;

    public string $addr;

    private function __construct($proc, array $pipes, string $addr)
    {
        $this->proc = $proc;
        $this->pipes = $pipes;
        $this->addr = $addr;
    }

    public static function start(): self
    {
        $cmd = [PHP_BINARY, __FILE__, '--broker'];
        $desc = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $proc = proc_open($cmd, $desc, $pipes);
        if (!is_resource($proc)) {
            throw new \RuntimeException('cannot spawn fake broker');
        }
        $line = fgets($pipes[1]);
        $addr = $line === false ? '' : trim($line);
        if ($addr === '') {
            $err = stream_get_contents($pipes[2]) ?: '';
            proc_terminate($proc);
            proc_close($proc);
            throw new \RuntimeException('fake broker did not report address: ' . $err);
        }
        return new self($proc, $pipes, $addr);
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
    }
}

// ======================================================================
// 测试 runner
// ======================================================================

final class RunClientInstance
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

    // ================================================================ 1. TopicPublishInfo

    private function testTopicPublishInfo(): void
    {
        $info = new TopicPublishInfo();
        $this->check(!$info->ok(), 'TopicPublishInfo 空队列 ok()=false');
        $this->checkSame(0, count($info->msgQueueList), 'msgQueueList 默认空');
        $this->checkSame(
            ['orderTopic' => false, 'messageQueueList' => []],
            $info->toDict(),
            'TopicPublishInfo 空态 toDict',
        );
        $this->checkThrows(
            static fn() => $info->selectOneMessageQueue(),
            MQClientException::class,
            '空队列 selectOneMessageQueue 抛 MQClientException',
        );

        $q0 = new MessageQueue('T', 'b', 0);
        $q1 = new MessageQueue('T', 'b', 1);
        $q2 = new MessageQueue('T', 'b', 2);
        $info->msgQueueList = [$q0, $q1, $q2];
        $info->orderTopic = true;
        $this->check($info->ok(), 'TopicPublishInfo 有队列 ok()=true');

        // 无过滤器 → 固定轮询，永不 null
        $info->resetIndex();
        $this->checkSame($q0, $info->selectOneMessageQueue(), '轮询 1/3');
        $this->checkSame($q1, $info->selectOneMessageQueue(), '轮询 2/3');
        $this->checkSame($q2, $info->selectOneMessageQueue(), '轮询 3/3');
        $this->checkSame($q0, $info->selectOneMessageQueue(), '轮询回卷 1/3');

        // 有过滤器 → 跳过不可用
        $info->resetIndex();
        $picked = $info->selectOneMessageQueue(static fn(MessageQueue $mq): bool => $mq->queueId !== 0);
        $this->checkSame($q1, $picked, '过滤器跳过 queueId=0 选中 q1');

        // 有过滤器 → 一轮内全不可用返回 null
        $info->resetIndex();
        $none = $info->selectOneMessageQueue(static fn(MessageQueue $mq): bool => false);
        $this->checkSame(null, $none, '全过滤器拒绝返回 null');

        // 无过滤器分支不受"全拒绝"影响（index 已推进）
        $info->resetIndex();
        $this->checkSame($q0, $info->selectOneMessageQueue(), 'resetIndex 后从 q0 开始');

        // toDict 元素结构
        $dict = $info->toDict();
        $this->checkSame(true, $dict['orderTopic'], 'toDict orderTopic 透传');
        $this->checkSame(
            ['topic' => 'T', 'brokerName' => 'b', 'queueId' => 1],
            $dict['messageQueueList'][1],
            'toDict 队列元素结构',
        );
    }

    // ================================================================ 2. buildSendRequest（V1/V2）

    private function testBuildSendRequest(): void
    {
        $inst = new MQClientInstance('build-cid', []);
        $inst->clock = static fn(): int => 1_718_000_000_000;

        // --- V1 header 用长字段名，V2 header 用单字母 ---
        $v1 = new SendMessageRequestHeader(producerGroup: 'G', topic: 'T');
        $this->checkSame('G', $v1->toExtFields()['producerGroup'], 'V1 header 长字段名 producerGroup');
        $v2 = new SendMessageRequestHeaderV2(producerGroup: 'G', topic: 'T', brokerName: 'bx');
        $v2e = $v2->toExtFields();
        $this->checkSame('G', $v2e['a'], 'V2 header producerGroup → a');
        $this->checkSame('T', $v2e['b'], 'V2 header topic → b');
        $this->checkSame('bx', $v2e['n'], 'V2 header brokerName → n');
        $this->check(!array_key_exists('producerGroup', $v2e), 'V2 header 不含长字段名');

        // --- 普通消息：SEND_MESSAGE_V2(310) ---
        $msg = new Message('BuildTopic', 'build-body');
        $msg->putProperty('k1', 'v1');
        $msg->setKeys('KEY-A');
        $mq = new MessageQueue('BuildTopic', 'broker-b', 3);
        $req = $inst->buildSendRequest('GID-build', $msg, $mq);
        // 头字段落到 extFields 是在 encode 时（headerEncode→makeCustomHeaderToNet）；
        // 这里显式执行同一步，拿到与“发出去”完全一致的 header 快照。
        $req->makeCustomHeaderToNet();
        $e = $req->extFields;
        $this->checkSame(RequestCode::SEND_MESSAGE_V2, $req->code, '普通消息走 SEND_MESSAGE_V2');
        $this->checkSame('GID-build', $e['a'], 'Send header a=producerGroup');
        $this->checkSame('BuildTopic', $e['b'], 'Send header b=topic');
        $this->checkSame(MixAll::DEFAULT_TOPIC, $e['c'], 'Send header c=defaultTopic(TBW102)');
        $this->checkSame('4', $e['d'], 'Send header d=defaultTopicQueueNums(4)');
        $this->checkSame('3', $e['e'], 'Send header e=queueId');
        $this->checkSame('0', $e['f'], 'Send header f=sysFlag');
        $this->checkSame('1718000000000', $e['g'], 'Send header g=bornTimestamp（注入时钟）');
        $this->checkSame('0', $e['h'], 'Send header h=flag');
        $this->check(str_contains($e['i'], 'k1') && str_contains($e['i'], 'UNIQ_KEY'), 'Send header i=properties（含属性与 UNIQ_KEY）');
        $this->checkSame('0', $e['j'], 'Send header j=reconsumeTimes');
        $this->checkSame('false', $e['k'], 'Send header k=unitMode=false');
        $this->checkSame('false', $e['m'], 'Send header m=batch=false');
        $this->checkSame('broker-b', $e['n'], 'Send header n=brokerName');
        $this->check(!array_key_exists('l', $e), 'maxReconsumeTimes 为 null 时不下发 l');
        $this->checkSame('build-body', $req->body, 'Send body = msg.getBody()');

        // --- 重试 topic：抬 reconsumeTimes / maxReconsumeTimes 并清属性 ---
        $retryMsg = new Message(MixAll::RETRY_GROUP_TOPIC_PREFIX . 'GID-x', 'retry-body');
        $retryMsg->putProperty(MessageConst::PROPERTY_RECONSUME_TIME, '3');
        $retryMsg->putProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES, '5');
        $retryReq = $inst->buildSendRequest('GID-x', $retryMsg, new MessageQueue(MixAll::RETRY_GROUP_TOPIC_PREFIX . 'GID-x', 'b', 0));
        $retryReq->makeCustomHeaderToNet();
        $this->checkSame('3', $retryReq->extFields['j'], '重试 topic 抬高 reconsumeTimes→j');
        $this->checkSame('5', $retryReq->extFields['l'], '重试 topic 抬高 maxReconsumeTimes→l');
        $this->checkSame(null, $retryMsg->getProperty(MessageConst::PROPERTY_RECONSUME_TIME), '重试消息 RECONSUME_TIME 属性被清除');
        $this->checkSame(null, $retryMsg->getProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES), '重试消息 MAX_RECONSUME_TIMES 属性被清除');

        // --- 应答消息：SEND_REPLY_MESSAGE_V2(325) 优先于 batch ---
        $reply = new Message();
        $reply->setTopic(MixAll::getReplyTopic('DefaultCluster'));
        $reply->putProperty(MessageConst::PROPERTY_MESSAGE_TYPE, MixAll::REPLY_MESSAGE_FLAG);
        $reply->setBody('reply-body');
        $replyReq = $inst->buildSendRequest('GID-r', $reply, new MessageQueue(MixAll::getReplyTopic('DefaultCluster'), '', 0));
        $this->checkSame(RequestCode::SEND_REPLY_MESSAGE_V2, $replyReq->code, '应答消息走 SEND_REPLY_MESSAGE_V2');
        $this->checkSame('reply-body', $replyReq->body, '应答 body 透传');

        // --- 批量消息：SEND_BATCH_MESSAGE(320)，body 为批量编码 ---
        $batch = MessageBatch::generateFromList([
            new Message('BatchTopic', 'b1'),
            new Message('BatchTopic', 'b2'),
        ]);
        $batchReq = $inst->buildSendRequest('GID-b', $batch, new MessageQueue('BatchTopic', 'broker-b', 0));
        $batchReq->makeCustomHeaderToNet();
        $this->checkSame(RequestCode::SEND_BATCH_MESSAGE, $batchReq->code, '批量消息走 SEND_BATCH_MESSAGE');
        $this->checkSame('true', $batchReq->extFields['m'], '批量 batch→m=true');
        $this->checkSame($batch->encode(), $batchReq->body, '批量 body = MessageBatch.encode()');

        // encodeBody 静态口径
        $this->checkSame('build-body', MQClientInstance::encodeBody($msg), 'encodeBody 返回 getBody()');

        $inst->shutdown();
        MQClientInstance::removeInstance('build-cid');
    }

    // ================================================================ 3. parseSendResponse

    private function testParseSendResponse(): void
    {
        $inst = new MQClientInstance('parse-cid', []);
        $inst->clock = static fn(): int => 1_718_000_000_000;

        $msg = new Message('ParseTopic', 'body-1');
        MessageClientIdSetter::setUniqId($msg);
        $uniq = MessageClientIdSetter::getUniqId($msg);
        $this->check(is_string($uniq) && $uniq !== '', 'setUniqId 生成 UNIQ_KEY');
        $mq = new MessageQueue('ParseTopic', 'broker-x', 0);

        $h = new SendMessageResponseHeader(
            msgId: 'OFFSET-MSG-ID',
            queueId: 2,
            queueOffset: 88,
            transactionId: 'tx-1',
            batchUniqId: null,
            recallHandle: 'RH-1',
        );
        $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $resp->customHeader = $h;
        $resp->makeCustomHeaderToNet();
        $resp->extFields[MessageConst::PROPERTY_MSG_REGION] = 'Region-A';
        $resp->extFields[MessageConst::PROPERTY_TRACE_SWITCH] = 'false';

        $r = $inst->parseSendResponse($resp, $msg, $mq);
        $this->checkSame(SendStatus::SEND_OK, $r->sendStatus, 'parseSendResponse SUCCESS→SEND_OK');
        $this->checkSame($uniq, $r->msgId, 'msgId = 客户端 UNIQ_KEY');
        $this->checkSame('OFFSET-MSG-ID', $r->offsetMsgId, 'offsetMsgId = 响应头 msgId');
        $this->checkSame(2, $r->messageQueue->queueId, 'messageQueue.queueId 用响应头覆盖');
        $this->checkSame('ParseTopic', $r->messageQueue->topic, 'messageQueue.topic 保留 mq.topic');
        $this->checkSame('broker-x', $r->messageQueue->brokerName, 'messageQueue.brokerName 保留 mq.brokerName');
        $this->checkSame(88, $r->queueOffset, 'queueOffset 用响应头');
        $this->checkSame('tx-1', $r->transactionId, 'transactionId 透传');
        $this->checkSame('RH-1', $r->recallHandle, 'recallHandle 透传');
        $this->checkSame('Region-A', $r->regionId, 'regionId = MSG_REGION');
        $this->checkSame(false, $r->traceOn, 'traceOn = TRACE_ON != false');

        // 缺省回落：无 MSG_REGION / TRACE_ON
        $resp2 = RemotingCommand::createResponseCommand(ResponseCode::FLUSH_DISK_TIMEOUT);
        $resp2->customHeader = new SendMessageResponseHeader(msgId: 'OM-2', queueId: 1, queueOffset: 5);
        $resp2->makeCustomHeaderToNet();
        $r2 = $inst->parseSendResponse($resp2, $msg, $mq);
        $this->checkSame(SendStatus::FLUSH_DISK_TIMEOUT, $r2->sendStatus, 'FLUSH_DISK_TIMEOUT 状态映射');
        $this->checkSame(MixAll::DEFAULT_TRACE_REGION_ID, $r2->regionId, 'regionId 缺省回落 DefaultRegion');
        $this->checkSame(true, $r2->traceOn, 'traceOn 缺省 true');
        $this->checkSame(null, $r2->recallHandle, 'recallHandle 缺省 null');

        $this->checkSame(
            SendStatus::SLAVE_NOT_AVAILABLE,
            $inst->parseSendResponse(
                RemotingCommand::createResponseCommand(ResponseCode::SLAVE_NOT_AVAILABLE),
                $msg,
                $mq,
            )->sendStatus,
            'SLAVE_NOT_AVAILABLE 状态映射',
        );

        // 非法响应码 → MQBrokerException
        $this->checkThrows(
            fn() => $inst->parseSendResponse(RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'boom'), $msg, $mq),
            MQBrokerException::class,
            'parseSendResponse 非法码抛 MQBrokerException',
        );

        $inst->shutdown();
        MQClientInstance::removeInstance('parse-cid');
    }

    // ================================================================ 4. processReplyMessage（请求-应答回写）

    private function testProcessReplyMessage(): void
    {
        $inst = new MQClientInstance('reply-cid', []);
        $inst->clock = static fn(): int => 1_718_000_000_000;

        $holder = RequestFutureHolder::getInstance();
        $cid = 'corr-0001';
        $future = new RequestResponseFuture($cid, 3000);
        $holder->putRequest($cid, $future);

        $h = new ReplyMessageRequestHeader(
            producerGroup: 'GID-reply',
            topic: 'ReplyTopic',
            queueId: 3,
            sysFlag: 0,
            bornTimestamp: 456,
            flag: 0,
            properties: MessageDecoder::messageProperties2String([
                MessageConst::PROPERTY_CORRELATION_ID => $cid,
            ]),
            reconsumeTimes: 2,
            bornHost: '1.2.3.4:10911',
            storeHost: '5.6.7.8:10911',
            storeTimestamp: 789,
        );
        $cmd = RemotingCommand::createRequestCommand(RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT, $h);
        $cmd->body = 'reply-body';
        $cmd->makeCustomHeaderToNet();

        $resp = $inst->processReplyMessage($cmd, 'broker');
        $this->check($resp instanceof RemotingCommand, 'processReplyMessage 返回响应命令');
        $this->checkSame(ResponseCode::SUCCESS, $resp->code, 'processReplyMessage 回 SUCCESS');

        $got = $future->responseMsg;
        $this->check($got instanceof MessageExt, '应答被投递成 MessageExt');
        $this->checkSame('reply-body', $got->getBody(), '应答 body');
        $this->checkSame('ReplyTopic', $got->getTopic(), '应答 topic');
        $this->checkSame(3, $got->queueId, '应答 queueId');
        $this->checkSame(2, $got->reconsumeTimes, '应答 reconsumeTimes');
        $this->checkSame('1.2.3.4:10911', $got->bornHost, '应答 bornHost');
        $this->checkSame('5.6.7.8:10911', $got->storeHost, '应答 storeHost');
        $this->checkSame(789, $got->storeTimestamp, '应答 storeTimestamp');
        $this->checkSame($cid, $got->getProperty(MessageConst::PROPERTY_CORRELATION_ID), 'CORRELATION_ID 保留');
        $this->checkSame(
            '1718000000000',
            $got->getProperty(MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME),
            '到达时间属性被写入',
        );
        $this->checkSame(null, $holder->getRequest($cid), 'putResponse 摘除等待槽');

        // 未匹配的 correlationId：仍回 SUCCESS，不抛
        $orphan = RemotingCommand::createRequestCommand(RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT, new ReplyMessageRequestHeader(
            topic: 'ReplyTopic',
            properties: MessageDecoder::messageProperties2String([MessageConst::PROPERTY_CORRELATION_ID => 'nope']),
        ));
        $orphan->body = 'x';
        $orphan->makeCustomHeaderToNet();
        $resp2 = $inst->processReplyMessage($orphan, 'broker');
        $this->checkSame(ResponseCode::SUCCESS, $resp2->code, '未匹配 correlationId 仍回 SUCCESS');

        $inst->shutdown();
        MQClientInstance::removeInstance('reply-cid');
    }

    // ================================================================ 5. tick() 时间阈值

    private function testTickThresholds(): void
    {
        $probe = new TickProbeInstance('tick-cid', []);
        $base = 1_000_000_000_000;
        $probe->clock = static fn(): int => $base;
        $probe->start();
        $this->check($probe->isStarted(), 'start() 后 isStarted()=true');
        $this->checkSame(1, $probe->routeCalls, 'start 同步首泵执行一次 routeRefreshOnce');
        $this->checkSame(1, $probe->heartbeatCalls, 'start 同步首泵执行一次心跳');

        // 打开 namesrv 刷新并排定下次到期（无真实网络，namesrvRefreshOnce 被探针覆写）
        $this->setPrivate($probe, 'namesrvRefreshEnabled', true);
        $this->setPrivate($probe, 'namesrvRefreshNextRunMillis', $base + 500);

        $probe->tick($base);
        $this->checkSame(1, $probe->routeCalls, 'tick(base)：路由未到期（+10）');
        $this->checkSame(0, $probe->adjustCalls, 'tick(base)：线程巡检未到期（+60000）');
        $this->checkSame(0, $probe->namesrvCalls, 'tick(base)：namesrv 未到期（+500）');

        $probe->tick($base + 9);
        $this->checkSame(1, $probe->routeCalls, 'tick(base+9)：路由仍未到期');

        $probe->tick($base + 10);
        $this->checkSame(2, $probe->routeCalls, 'tick(base+10)：路由到期执行');

        $probe->tick($base + 11);
        $this->checkSame(2, $probe->routeCalls, 'tick(base+11)：路由重新排程后不再执行');

        $probe->tick($base + 500);
        $this->checkSame(1, $probe->namesrvCalls, 'tick(base+500)：namesrv 到期执行');
        $this->checkSame(2, $probe->routeCalls, 'namesrv 触发不影响路由计数');

        $probe->tick($base + 60000);
        $this->checkSame(1, $probe->adjustCalls, 'tick(base+60000)：线程巡检到期执行');
        $this->checkSame(3, $probe->routeCalls, '同一 tick 内路由（下一次 +30010）也到期');

        // pumpOnce 别名等价
        $before = $probe->routeCalls + $probe->adjustCalls + $probe->namesrvCalls;
        $probe->pumpOnce($base + 60001);
        $after = $probe->routeCalls + $probe->adjustCalls + $probe->namesrvCalls;
        $this->checkSame($before, $after, 'pumpOnce 别名在未到期时刻不触发任何泵');

        // shutdown 后 tick 不再推进
        $probe->shutdown();
        $r = $probe->routeCalls;
        $probe->tick($base + 999999);
        $this->checkSame($r, $probe->routeCalls, 'shutdown 后 tick 不再执行泵');

        // pending 队列：入队即被 tick/runPendingActions 消化
        $ran = 0;
        $this->setPrivate($probe, 'pendingActions', [static function () use (&$ran): void {
            $ran++;
        }]);
        $probe->runPendingActions();
        $this->checkSame(1, $ran, 'runPendingActions 执行待办动作');
        $this->checkSame([], $this->getPrivate($probe, 'pendingActions'), 'runPendingActions 清空队列');

        MQClientInstance::removeInstance('tick-cid');
    }

    private function setPrivate(object $obj, string $name, mixed $value): void
    {
        $p = new \ReflectionProperty(MQClientInstance::class, $name);
        $p->setAccessible(true);
        $p->setValue($obj, $value);
    }

    private function getPrivate(object $obj, string $name): mixed
    {
        $p = new \ReflectionProperty(MQClientInstance::class, $name);
        $p->setAccessible(true);
        return $p->getValue($obj);
    }

    // ================================================================ 6. 路由缓存与 default topic 回退

    private function testRouteCacheAndFallback(): void
    {
        $broker = FakeBroker::start();
        $inst = new MQClientInstance('route-cid', [$broker->addr], 2000, 3000);

        // 正常路由拉取并落缓存
        $ok = $inst->updateTopicRouteInfoFromNameServer('RouteTopic');
        $this->checkSame(true, $ok, '拉取 RouteTopic 路由成功');
        $route = $inst->getTopicRouteData('RouteTopic');
        $this->check($route instanceof TopicRouteData, 'topicRouteTable 命中');
        $this->checkSame(
            true,
            $route === ($this->getPrivate($inst, 'topicRouteTable')['RouteTopic'] ?? null),
            'getTopicRouteData 返回同一缓存对象',
        );
        $this->checkSame(['127.0.0.1:10911'], $this->getPrivate($inst, 'brokerAddrTable')['broker-a'] ?? [], 'brokerAddrTable 落库');
        $this->checkSame('127.0.0.1:10911', $inst->findBrokerAddressInPublish('broker-a'), 'findBrokerAddressInPublish 取主地址');
        $this->checkSame('127.0.0.1:10911', MQClientInstance::findBrokerAddrInRoute($route, 'broker-a'), 'findBrokerAddrInRoute 取地址');
        $this->checkSame(null, MQClientInstance::findBrokerAddrInRoute($route, 'nope'), 'findBrokerAddrInRoute 未知 broker 返回 null');

        $info = $inst->getTopicPublishInfo('RouteTopic');
        $this->checkSame(2, count($info->msgQueueList), 'getTopicPublishInfo 产出 2 个写队列');
        $this->checkSame(false, $info->orderTopic, '非顺序 topic orderTopic=false');
        $this->checkSame(2, count($inst->getTopicSubscribeInfo('RouteTopic')), 'getTopicSubscribeInfo 产出 2 个读队列');

        // 真实路由拉不到且非默认 → 返回 false，不落缓存
        $this->checkSame(false, $inst->updateTopicRouteInfoFromNameServer('OtherTopic'), '未知 topic 非默认模式返回 false');
        $this->checkSame(null, $this->getPrivate($inst, 'topicRouteTable')['OtherTopic'] ?? null, '未知 topic 未落缓存');

        // default topic 回退：NewTopic 不存在 → 用 TBW102 合成，且写队列按 4 裁剪
        $fallback = $inst->updateTopicRouteInfoFromNameServer('NewTopic', 5000, true);
        $this->checkSame(true, $fallback, 'default topic 回退成功');
        $newInfo = $inst->getTopicPublishInfo('NewTopic');
        $this->checkSame(MixAll::DEFAULT_TOPIC_QUEUE_NUMS, count($newInfo->msgQueueList), '回退队列数被裁到 DEFAULT_TOPIC_QUEUE_NUMS');
        $this->checkSame('broker-def', $newInfo->msgQueueList[0]->brokerName, '回退用的是 TBW102 的 broker');

        // 非回退模式：NewTopic 仍然不存在
        $this->checkSame(false, $inst->updateTopicRouteInfoFromNameServer('NewTopic'), '非 isDefault 不触发回退（TOPIC_NOT_EXIST → false）');

        // publishAddrFor：已知 broker 直接命中；未知 broker 抛异常
        $this->checkSame('127.0.0.1:10911', $inst->publishAddrFor('broker-a', 'RouteTopic'), 'publishAddrFor 命中主地址');
        $this->checkThrows(
            fn() => $inst->publishAddrFor('ghost-broker', 'RouteTopic'),
            MQClientException::class,
            'publishAddrFor 未知 broker 抛 MQClientException',
        );

        $inst->shutdown();
        MQClientInstance::removeInstance('route-cid');
        $broker->stop();
    }

    // ================================================================ 7. pullMessage

    private function testPullMessage(): void
    {
        $broker = FakeBroker::start();
        $inst = new MQClientInstance('pull-cid', [$broker->addr], 2000, 3000);

        // FOUND：解析响应头 + body
        $mq = new MessageQueue('PullTopic', 'broker-pull', 0);
        $found = $inst->pullMessage('GID-pull', $mq, 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::FOUND, $found->status, 'PULL FOUND 状态');
        $this->checkSame(10, $found->nextBeginOffset, 'PULL nextBeginOffset');
        $this->checkSame(1, $found->minOffset, 'PULL minOffset');
        $this->checkSame(100, $found->maxOffset, 'PULL maxOffset');
        $this->checkSame(1, $found->suggestWhichBrokerId, 'PULL suggestWhichBrokerId');
        $this->checkSame(1, count($found->msgFoundList), 'PULL 拉回 1 条消息');
        $this->checkSame('pull-body-0', $found->msgFoundList[0]->getBody(), 'PULL 消息 body');
        $this->checkSame('broker-pull', $found->msgFoundList[0]->brokerName, 'PULL 回填 brokerName');
        $this->checkSame(0, $found->msgFoundList[0]->queueId, 'PULL 回填 queueId');

        // 各响应码 → PullStatus 映射
        $noNew = $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 1), 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::NO_NEW_MSG, $noNew->status, 'PULL_NOT_FOUND → NO_NEW_MSG');
        $illegal = $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 2), 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::OFFSET_ILLEGAL, $illegal->status, 'PULL_OFFSET_MOVED → OFFSET_ILLEGAL');
        $noMatch = $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 3), 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::NO_MATCHED_MSG, $noMatch->status, 'PULL_RETRY_IMMEDIATELY → NO_MATCHED_MSG');

        // 非法码 → MQBrokerException
        $this->checkThrows(
            fn() => $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 4), 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000),
            MQBrokerException::class,
            'PULL 非法响应码抛 MQBrokerException',
        );

        // lite pull 位 → 请求码切到 LITE_PULL_MESSAGE（broker 在 remark 回显请求码）
        $lite = $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 0), 0, 32, PullSysFlag::FLAG_LITE_PULL_MESSAGE, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::FOUND, $lite->status, 'lite pull 仍能解析响应');

        // findBrokerAddrInSubscribe 主/从挑选
        $this->checkSame(['127.0.0.1:1', false], MQClientInstance::findBrokerAddrInSubscribe([0 => '127.0.0.1:1'], 0), 'findBrokerAddrInSubscribe 命中 master');
        [$slaveAddr, $isSlave] = MQClientInstance::findBrokerAddrInSubscribe([0 => '127.0.0.1:1', 1 => '127.0.0.1:2'], 1);
        $this->checkSame('127.0.0.1:2', $slaveAddr, 'findBrokerAddrInSubscribe 指定 slave');
        $this->checkSame(true, $isSlave, 'findBrokerAddrInSubscribe slave 标志');
        [$fallbackAddr, $fallbackSlave] = MQClientInstance::findBrokerAddrInSubscribe([0 => '127.0.0.1:1'], 5);
        $this->checkSame('127.0.0.1:1', $fallbackAddr, 'findBrokerAddrInSubscribe 未知 id 回落首个');
        $this->checkSame(false, $fallbackSlave, 'findBrokerAddrInSubscribe 回落 master 非 slave');

        $inst->shutdown();
        MQClientInstance::removeInstance('pull-cid');
        $broker->stop();
    }

    // ================================================================ 8. popMessage

    private function testPopMessage(): void
    {
        $broker = FakeBroker::start();
        $inst = new MQClientInstance('pop-cid', [$broker->addr], 2000, 3000);

        $pop = $inst->popMessage('GID-pop', 'PopTopic', 0, 32, 60000, 0, 0, null, null, false, 'broker-pop', 3000, $broker->addr);
        $this->checkSame(PopStatus::FOUND, $pop->status, 'POP FOUND 状态');
        $this->checkSame(7, $pop->restNum, 'POP restNum');
        $this->checkSame(111, $pop->popTime, 'POP popTime');
        $this->checkSame(222, $pop->invisibleTime, 'POP invisibleTime');
        $this->checkSame(333, $pop->reviveQid, 'POP reviveQid');
        $this->checkSame('0 0 5', $pop->startOffsetInfo, 'POP startOffsetInfo 原样保留');
        $this->checkSame('0 0 5,6', $pop->msgOffsetInfo, 'POP msgOffsetInfo 原样保留');
        $this->checkSame(2, count($pop->msgFoundList), 'POP 拉回 2 条消息');

        $m0 = $pop->msgFoundList[0];
        $m1 = $pop->msgFoundList[1];
        $this->checkSame('PopTopic', $m0->topic, 'POP 消息 topic 还原为请求 topic');
        $this->checkSame('broker-pop', $m0->brokerName, 'POP 消息 brokerName 回填');
        $this->checkSame(5, $m0->queueOffset, 'POP 消息 0 queueOffset');
        $this->checkSame(6, $m1->queueOffset, 'POP 消息 1 queueOffset');
        $this->checkSame('pop-body-5', $m0->getBody(), 'POP 消息 0 body');
        $this->checkSame('pop-body-6', $m1->getBody(), 'POP 消息 1 body');

        // 反构 POP_CK：ckQueueOffset 用 startOffset(5)，末段用各消息自身 offset
        $expected0 = ExtraInfoUtil::buildExtraInfo(5, 111, 222, 333, 'PopTopic', 'broker-pop', 0, 5);
        $expected1 = ExtraInfoUtil::buildExtraInfo(5, 111, 222, 333, 'PopTopic', 'broker-pop', 0, 6);
        $this->checkSame($expected0, $m0->getProperty(MessageConst::PROPERTY_POP_CK), 'POP_CK 反构（消息 0）');
        $this->checkSame($expected1, $m1->getProperty(MessageConst::PROPERTY_POP_CK), 'POP_CK 反构（消息 1）');
        $this->checkSame('111', $m0->getProperty(MessageConst::PROPERTY_FIRST_POP_TIME), '1ST_POP_TIME 用 popTime');
        $this->checkSame('111', $m1->getProperty(MessageConst::PROPERTY_FIRST_POP_TIME), '1ST_POP_TIME（消息 1）');

        // 状态映射：POLLING_FULL / POLLING_TIMEOUT
        $full = $inst->popMessage('GID-pop', 'PopTopic', 9, 32, 60000, 0, 0, null, null, false, 'broker-pop', 3000, $broker->addr);
        $this->checkSame(PopStatus::POLLING_FULL, $full->status, 'POLLING_FULL 状态映射');
        $this->checkSame(0, count($full->msgFoundList), 'POLLING_FULL 无消息');
        $timeout = $inst->popMessage('GID-pop', 'PopTopic', 10, 32, 60000, 0, 0, null, null, false, 'broker-pop', 3000, $broker->addr);
        $this->checkSame(PopStatus::POLLING_NOT_FOUND, $timeout->status, 'POLLING_TIMEOUT → POLLING_NOT_FOUND');

        $inst->shutdown();
        MQClientInstance::removeInstance('pop-cid');
        $broker->stop();
    }

    // ================================================================ 9. offset 查询（附带）

    private function testQueryConsumerOffset(): void
    {
        $broker = FakeBroker::start();
        $inst = new MQClientInstance('offset-cid', [$broker->addr], 2000, 3000);
        $offset = $inst->queryConsumerOffset('GID-offset', new MessageQueue('OffsetTopic', 'broker-a', 0), 3000, $broker->addr);
        $this->checkSame(42, $offset, 'queryConsumerOffset 解析 offset');
        $inst->shutdown();
        MQClientInstance::removeInstance('offset-cid');
        $broker->stop();
    }

    // ================================================================ run

    public function run(): int
    {
        $this->testTopicPublishInfo();
        $this->testBuildSendRequest();
        $this->testParseSendResponse();
        $this->testProcessReplyMessage();
        $this->testTickThresholds();
        $this->testRouteCacheAndFallback();
        $this->testPullMessage();
        $this->testPopMessage();
        $this->testQueryConsumerOffset();
        return $this->summary();
    }
}

/**
 * tick() 探针：把各 pump 覆写成计数器，纯逻辑验证时间阈值判定。
 * MQClientInstance 未声明 final，可直接继承。
 */
class TickProbeInstance extends MQClientInstance
{
    public int $routeCalls = 0;
    public int $adjustCalls = 0;
    public int $namesrvCalls = 0;
    public int $heartbeatCalls = 0;

    public function routeRefreshOnce(): void
    {
        $this->routeCalls++;
    }

    public function adjustThreadPool(): void
    {
        $this->adjustCalls++;
    }

    public function namesrvRefreshOnce(): void
    {
        $this->namesrvCalls++;
    }

    public function heartbeatOnce(int $timeoutMillis = 5000): void
    {
        $this->heartbeatCalls++;
    }
}

exit((new RunClientInstance())->run());
