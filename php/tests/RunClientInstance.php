<?php

declare(strict_types=1);

/**
 * MQClientInstance pure-PHP assert-style self test (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientInstance.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Network cases (route fetch / default-topic fallback, PULL, POP, offsets, etc.) are served by a
 * local stream_socket_server fake broker; this file re-spawns itself with the `--broker` argument
 * as a **child-process** broker so that "client synchronous call blocking" and "server reading
 * frames and replying" can truly interleave, avoiding the single-thread problem where two
 * consecutive synchronous RPCs cannot make progress. Pure-logic cases do not spawn processes.
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQPushConsumer;
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
use RocketMQ\Common\SubscriptionData;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\ConsumerData;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\PopMessageResponseHeader;
use RocketMQ\Remoting\Protocol\ProducerData;
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
// child-process fake broker: `php RunClientInstance.php --broker`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    brokerMain();
    exit(0);
}

/** Reads exactly n bytes; returns null on EOF. */
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

/** Reads one full frame; returns null on EOF. */
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

/** Builds a route body. */
function brokerRouteBody(string $brokerName, string $addr, int $write, int $read): string
{
    $qd = new QueueData($brokerName, $read, $write, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', $brokerName, [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

/** Encodes one message in the 17-segment format and concatenates it into the PULL/POP response body. */
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
 * Replies per request code / fields; returning null means no reply (oneway).
 */
function brokerRespond(RemotingCommand $cmd): ?RemotingCommand
{
    $ext = $cmd->extFields;

    switch ($cmd->code) {
        case RequestCode::SEND_MESSAGE: {
            // namespaceV2 probe: echo the **received** ns/nsd back verbatim in remark. The client
            // saying so itself is not evidence; only the fields read by the peer prove the hook is
            // really attached at the transport layer and its value is the current configuration.
            if (($ext['probe'] ?? '') === 'ns') {
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->remark = 'ns=' . (string) ($ext['ns'] ?? '')
                    . ';nsd=' . (string) ($ext['nsd'] ?? '');
                return $resp;
            }
            return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'unhandled send');
        }

        case RequestCode::GET_ROUTEINFO_BY_TOPIC: {
            $topic = (string) ($ext['topic'] ?? '');
            if ($topic === 'RouteTopic') {
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->body = brokerRouteBody('broker-a', '127.0.0.1:10911', 2, 2);
                return $resp;
            }
            if ($topic === MixAll::DEFAULT_TOPIC) {
                // default topic: queue count 8 > DEFAULT_TOPIC_QUEUE_NUMS(4), used to verify trimming.
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->body = brokerRouteBody('broker-def', '127.0.0.1:10912', 8, 8);
                return $resp;
            }
            return RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST);
        }

        case RequestCode::PULL_MESSAGE:
        case RequestCode::LITE_PULL_MESSAGE: {
            $qid = (int) ($ext['queueId'] ?? 0);
            // write the actually received request code into remark so callers can assert V1/PULL vs LITE_PULL choice.
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

    // serially serve connections: each connection is served until the client closes it (shutdown).
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
 * Child-process fake broker handle.
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
// test runner
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
        $this->check(!$info->ok(), 'TopicPublishInfo ok()=false with empty queue list');
        $this->checkSame(0, count($info->msgQueueList), 'msgQueueList empty by default');
        $this->checkSame(
            ['orderTopic' => false, 'messageQueueList' => []],
            $info->toDict(),
            'TopicPublishInfo empty-state toDict',
        );
        $this->checkThrows(
            static fn() => $info->selectOneMessageQueue(),
            MQClientException::class,
            'selectOneMessageQueue on empty queue list throws MQClientException',
        );

        $q0 = new MessageQueue('T', 'b', 0);
        $q1 = new MessageQueue('T', 'b', 1);
        $q2 = new MessageQueue('T', 'b', 2);
        $info->msgQueueList = [$q0, $q1, $q2];
        $info->orderTopic = true;
        $this->check($info->ok(), 'TopicPublishInfo ok()=true with queues');

        // no filter -> fixed round-robin, never null
        $info->resetIndex();
        $this->checkSame($q0, $info->selectOneMessageQueue(), 'round-robin 1/3');
        $this->checkSame($q1, $info->selectOneMessageQueue(), 'round-robin 2/3');
        $this->checkSame($q2, $info->selectOneMessageQueue(), 'round-robin 3/3');
        $this->checkSame($q0, $info->selectOneMessageQueue(), 'round-robin wraps back to 1/3');

        // with filter -> skips unavailable
        $info->resetIndex();
        $picked = $info->selectOneMessageQueue(static fn(MessageQueue $mq): bool => $mq->queueId !== 0);
        $this->checkSame($q1, $picked, 'filter skips queueId=0 and picks q1');

        // with filter -> all unavailable within one round returns null
        $info->resetIndex();
        $none = $info->selectOneMessageQueue(static fn(MessageQueue $mq): bool => false);
        $this->checkSame(null, $none, 'filter rejecting everything returns null');

        // the no-filter branch is unaffected by the "reject all" case (index already advanced)
        $info->resetIndex();
        $this->checkSame($q0, $info->selectOneMessageQueue(), 'starts from q0 after resetIndex');

        // toDict element structure
        $dict = $info->toDict();
        $this->checkSame(true, $dict['orderTopic'], 'toDict orderTopic passthrough');
        $this->checkSame(
            ['topic' => 'T', 'brokerName' => 'b', 'queueId' => 1],
            $dict['messageQueueList'][1],
            'toDict queue element structure',
        );
    }

    // ================================================================ 2. buildSendRequest (V1/V2)

    private function testBuildSendRequest(): void
    {
        $inst = new MQClientInstance('build-cid', []);
        $inst->clock = static fn(): int => 1_718_000_000_000;

        // --- V1 header uses long field names, V2 header uses single letters ---
        $v1 = new SendMessageRequestHeader(producerGroup: 'G', topic: 'T');
        $this->checkSame('G', $v1->toExtFields()['producerGroup'], 'V1 header long field name producerGroup');
        $v2 = new SendMessageRequestHeaderV2(producerGroup: 'G', topic: 'T', brokerName: 'bx');
        $v2e = $v2->toExtFields();
        $this->checkSame('G', $v2e['a'], 'V2 header producerGroup -> a');
        $this->checkSame('T', $v2e['b'], 'V2 header topic -> b');
        $this->checkSame('bx', $v2e['n'], 'V2 header brokerName -> n');
        $this->check(!array_key_exists('producerGroup', $v2e), 'V2 header contains no long field names');

        // --- normal message: SEND_MESSAGE_V2(310) ---
        $msg = new Message('BuildTopic', 'build-body');
        $msg->putProperty('k1', 'v1');
        $msg->setKeys('KEY-A');
        $mq = new MessageQueue('BuildTopic', 'broker-b', 3);
        $req = $inst->buildSendRequest('GID-build', $msg, $mq);
        // header fields land in extFields at encode time (headerEncode -> makeCustomHeaderToNet);
        // run the same step explicitly here to get a header snapshot identical to "what goes on the wire".
        $req->makeCustomHeaderToNet();
        $e = $req->extFields;
        $this->checkSame(RequestCode::SEND_MESSAGE_V2, $req->code, 'normal message uses SEND_MESSAGE_V2');
        $this->checkSame('GID-build', $e['a'], 'Send header a=producerGroup');
        $this->checkSame('BuildTopic', $e['b'], 'Send header b=topic');
        $this->checkSame(MixAll::DEFAULT_TOPIC, $e['c'], 'Send header c=defaultTopic(TBW102)');
        $this->checkSame('4', $e['d'], 'Send header d=defaultTopicQueueNums(4)');
        $this->checkSame('3', $e['e'], 'Send header e=queueId');
        $this->checkSame('0', $e['f'], 'Send header f=sysFlag');
        $this->checkSame('1718000000000', $e['g'], 'Send header g=bornTimestamp (injected clock)');
        $this->checkSame('0', $e['h'], 'Send header h=flag');
        $this->check(str_contains($e['i'], 'k1') && str_contains($e['i'], 'UNIQ_KEY'), 'Send header i=properties (includes properties and UNIQ_KEY)');
        $this->checkSame('0', $e['j'], 'Send header j=reconsumeTimes');
        $this->checkSame('false', $e['k'], 'Send header k=unitMode=false');
        $this->checkSame('false', $e['m'], 'Send header m=batch=false');
        $this->checkSame('broker-b', $e['n'], 'Send header n=brokerName');
        $this->check(!array_key_exists('l', $e), 'l not sent when maxReconsumeTimes is null');
        $this->checkSame('build-body', $req->body, 'Send body = msg.getBody()');

        // --- retry topic: bumps reconsumeTimes / maxReconsumeTimes and clears properties ---
        $retryMsg = new Message(MixAll::RETRY_GROUP_TOPIC_PREFIX . 'GID-x', 'retry-body');
        $retryMsg->putProperty(MessageConst::PROPERTY_RECONSUME_TIME, '3');
        $retryMsg->putProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES, '5');
        $retryReq = $inst->buildSendRequest('GID-x', $retryMsg, new MessageQueue(MixAll::RETRY_GROUP_TOPIC_PREFIX . 'GID-x', 'b', 0));
        $retryReq->makeCustomHeaderToNet();
        $this->checkSame('3', $retryReq->extFields['j'], 'retry topic bumps reconsumeTimes -> j');
        $this->checkSame('5', $retryReq->extFields['l'], 'retry topic bumps maxReconsumeTimes -> l');
        $this->checkSame(null, $retryMsg->getProperty(MessageConst::PROPERTY_RECONSUME_TIME), 'retry message RECONSUME_TIME property cleared');
        $this->checkSame(null, $retryMsg->getProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES), 'retry message MAX_RECONSUME_TIMES property cleared');

        // --- reply message: SEND_REPLY_MESSAGE_V2(325) takes precedence over batch ---
        $reply = new Message();
        $reply->setTopic(MixAll::getReplyTopic('DefaultCluster'));
        $reply->putProperty(MessageConst::PROPERTY_MESSAGE_TYPE, MixAll::REPLY_MESSAGE_FLAG);
        $reply->setBody('reply-body');
        $replyReq = $inst->buildSendRequest('GID-r', $reply, new MessageQueue(MixAll::getReplyTopic('DefaultCluster'), '', 0));
        $this->checkSame(RequestCode::SEND_REPLY_MESSAGE_V2, $replyReq->code, 'reply message uses SEND_REPLY_MESSAGE_V2');
        $this->checkSame('reply-body', $replyReq->body, 'reply body passthrough');

        // --- batch message: SEND_BATCH_MESSAGE(320), body is the batch encoding ---
        $batch = MessageBatch::generateFromList([
            new Message('BatchTopic', 'b1'),
            new Message('BatchTopic', 'b2'),
        ]);
        $batchReq = $inst->buildSendRequest('GID-b', $batch, new MessageQueue('BatchTopic', 'broker-b', 0));
        $batchReq->makeCustomHeaderToNet();
        $this->checkSame(RequestCode::SEND_BATCH_MESSAGE, $batchReq->code, 'batch message uses SEND_BATCH_MESSAGE');
        $this->checkSame('true', $batchReq->extFields['m'], 'batch sets m=true');
        $this->checkSame($batch->encode(), $batchReq->body, 'batch body = MessageBatch.encode()');

        // encodeBody static behavior
        $this->checkSame('build-body', MQClientInstance::encodeBody($msg), 'encodeBody returns getBody()');

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
        $this->check(is_string($uniq) && $uniq !== '', 'setUniqId generates UNIQ_KEY');
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
        $this->checkSame(SendStatus::SEND_OK, $r->sendStatus, 'parseSendResponse SUCCESS -> SEND_OK');
        $this->checkSame($uniq, $r->msgId, 'msgId = client UNIQ_KEY');
        $this->checkSame('OFFSET-MSG-ID', $r->offsetMsgId, 'offsetMsgId = response header msgId');
        $this->checkSame(2, $r->messageQueue->queueId, 'messageQueue.queueId overridden by response header');
        $this->checkSame('ParseTopic', $r->messageQueue->topic, 'messageQueue.topic keeps mq.topic');
        $this->checkSame('broker-x', $r->messageQueue->brokerName, 'messageQueue.brokerName keeps mq.brokerName');
        $this->checkSame(88, $r->queueOffset, 'queueOffset taken from response header');
        $this->checkSame('tx-1', $r->transactionId, 'transactionId passthrough');
        $this->checkSame('RH-1', $r->recallHandle, 'recallHandle passthrough');
        $this->checkSame('Region-A', $r->regionId, 'regionId = MSG_REGION');
        $this->checkSame(false, $r->traceOn, 'traceOn = TRACE_ON != false');

        // default fallback: no MSG_REGION / TRACE_ON
        $resp2 = RemotingCommand::createResponseCommand(ResponseCode::FLUSH_DISK_TIMEOUT);
        $resp2->customHeader = new SendMessageResponseHeader(msgId: 'OM-2', queueId: 1, queueOffset: 5);
        $resp2->makeCustomHeaderToNet();
        $r2 = $inst->parseSendResponse($resp2, $msg, $mq);
        $this->checkSame(SendStatus::FLUSH_DISK_TIMEOUT, $r2->sendStatus, 'FLUSH_DISK_TIMEOUT status mapping');
        $this->checkSame(MixAll::DEFAULT_TRACE_REGION_ID, $r2->regionId, 'regionId falls back to DefaultRegion');
        $this->checkSame(true, $r2->traceOn, 'traceOn defaults to true');
        $this->checkSame(null, $r2->recallHandle, 'recallHandle defaults to null');

        $this->checkSame(
            SendStatus::SLAVE_NOT_AVAILABLE,
            $inst->parseSendResponse(
                RemotingCommand::createResponseCommand(ResponseCode::SLAVE_NOT_AVAILABLE),
                $msg,
                $mq,
            )->sendStatus,
            'SLAVE_NOT_AVAILABLE status mapping',
        );

        // invalid response code -> MQBrokerException
        $this->checkThrows(
            fn() => $inst->parseSendResponse(RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'boom'), $msg, $mq),
            MQBrokerException::class,
            'parseSendResponse invalid code throws MQBrokerException',
        );

        $inst->shutdown();
        MQClientInstance::removeInstance('parse-cid');
    }

    // ================================================================ 4. processReplyMessage (request-reply write-back)

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
        $this->check($resp instanceof RemotingCommand, 'processReplyMessage returns a response command');
        $this->checkSame(ResponseCode::SUCCESS, $resp->code, 'processReplyMessage replies SUCCESS');

        $got = $future->responseMsg;
        $this->check($got instanceof MessageExt, 'reply delivered as MessageExt');
        $this->checkSame('reply-body', $got->getBody(), 'reply body');
        $this->checkSame('ReplyTopic', $got->getTopic(), 'reply topic');
        $this->checkSame(3, $got->queueId, 'reply queueId');
        $this->checkSame(2, $got->reconsumeTimes, 'reply reconsumeTimes');
        $this->checkSame('1.2.3.4:10911', $got->bornHost, 'reply bornHost');
        $this->checkSame('5.6.7.8:10911', $got->storeHost, 'reply storeHost');
        $this->checkSame(789, $got->storeTimestamp, 'reply storeTimestamp');
        $this->checkSame($cid, $got->getProperty(MessageConst::PROPERTY_CORRELATION_ID), 'CORRELATION_ID preserved');
        $this->checkSame(
            '1718000000000',
            $got->getProperty(MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME),
            'arrive-time property written',
        );
        $this->checkSame(null, $holder->getRequest($cid), 'putResponse removes the waiting slot');

        // unmatched correlationId: still replies SUCCESS, does not throw
        $orphan = RemotingCommand::createRequestCommand(RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT, new ReplyMessageRequestHeader(
            topic: 'ReplyTopic',
            properties: MessageDecoder::messageProperties2String([MessageConst::PROPERTY_CORRELATION_ID => 'nope']),
        ));
        $orphan->body = 'x';
        $orphan->makeCustomHeaderToNet();
        $resp2 = $inst->processReplyMessage($orphan, 'broker');
        $this->checkSame(ResponseCode::SUCCESS, $resp2->code, 'unmatched correlationId still replies SUCCESS');

        $inst->shutdown();
        MQClientInstance::removeInstance('reply-cid');
    }

    // ================================================================ 5. tick() time thresholds

    private function testTickThresholds(): void
    {
        $probe = new TickProbeInstance('tick-cid', []);
        $base = 1_000_000_000_000;
        $probe->clock = static fn(): int => $base;
        $probe->start();
        $this->check($probe->isStarted(), 'isStarted()=true after start()');
        $this->checkSame(1, $probe->routeCalls, 'start synchronous first pump runs routeRefreshOnce');
        $this->checkSame(1, $probe->heartbeatCalls, 'start synchronous first pump runs heartbeat once');

        // enable namesrv refresh and schedule the next due time (no real network; namesrvRefreshOnce is overridden by the probe)
        $this->setPrivate($probe, 'namesrvRefreshEnabled', true);
        $this->setPrivate($probe, 'namesrvRefreshNextRunMillis', $base + 500);

        $probe->tick($base);
        $this->checkSame(1, $probe->routeCalls, 'tick(base): route not due (+10)');
        $this->checkSame(0, $probe->adjustCalls, 'tick(base): thread check not due (+60000)');
        $this->checkSame(0, $probe->namesrvCalls, 'tick(base): namesrv not due (+500)');

        $probe->tick($base + 9);
        $this->checkSame(1, $probe->routeCalls, 'tick(base+9): route still not due');

        $probe->tick($base + 10);
        $this->checkSame(2, $probe->routeCalls, 'tick(base+10): route due and runs');

        $probe->tick($base + 11);
        $this->checkSame(2, $probe->routeCalls, 'tick(base+11): route rescheduled, does not run again');

        $probe->tick($base + 500);
        $this->checkSame(1, $probe->namesrvCalls, 'tick(base+500): namesrv due and runs');
        $this->checkSame(2, $probe->routeCalls, 'namesrv trigger does not affect route count');
        $this->checkSame(1, $probe->heartbeatCalls, 'tick(base+500): the 30s registration pump is not yet due');

        $probe->tick($base + 60000);
        $this->checkSame(1, $probe->adjustCalls, 'tick(base+60000): thread check due and runs');
        $this->checkSame(3, $probe->routeCalls, 'route also due within the same tick (next +30010)');
        $this->checkSame(2, $probe->heartbeatCalls, 'tick(base+60000): the 30s registration pump is due and runs');

        // pumpOnce alias equivalence
        $before = $probe->routeCalls + $probe->adjustCalls + $probe->namesrvCalls;
        $probe->pumpOnce($base + 60001);
        $after = $probe->routeCalls + $probe->adjustCalls + $probe->namesrvCalls;
        $this->checkSame($before, $after, 'pumpOnce alias triggers no pump before anything is due');
        $this->checkSame(2, $probe->heartbeatCalls, 'the beat that just ran is rescheduled a full period out');

        // tick does not advance after shutdown
        $probe->shutdown();
        $r = $probe->routeCalls;
        $h = $probe->heartbeatCalls;
        $probe->tick($base + 999999);
        $this->checkSame($r, $probe->routeCalls, 'tick runs no pumps after shutdown');
        $this->checkSame($h, $probe->heartbeatCalls, 'the registration pump stops after shutdown too');

        // pending queue: enqueued items are consumed by tick/runPendingActions
        $ran = 0;
        $this->setPrivate($probe, 'pendingActions', [static function () use (&$ran): void {
            $ran++;
        }]);
        $probe->runPendingActions();
        $this->checkSame(1, $ran, 'runPendingActions executes the pending action');
        $this->checkSame([], $this->getPrivate($probe, 'pendingActions'), 'runPendingActions empties the queue');

        MQClientInstance::removeInstance('tick-cid');
    }

    // ================================================================ 5b. heartbeat interval knob

    /**
     * The registration pump reads one value, taken when the instance is built: a facade that
     * beats faster than the broker forgets it must not be put back on the 30s default, and a
     * nonsense value (0 or less) must not stop the pump either. Everything is asserted against
     * an injected clock, so the only thing under test is the schedule arithmetic.
     */
    private function testHeartbeatIntervalKnob(): void
    {
        $base = 1_000_000_000_000;

        // A non-default value is what drives the next beat.
        $knob = new TickProbeInstance('hb-knob-cid', [], heartbeatIntervalMillis: 5000);
        $this->checkSame(5000, $knob->heartbeatIntervalMillis, 'the instance keeps the interval it was given');
        $knob->clock = static fn(): int => $base;
        $this->checkSame(null, $this->getPrivate($knob, 'heartbeatNextRunMillis'),
            'nothing is scheduled before start(), so no tick can beat');
        $knob->tick($base + 10_000_000);
        $this->checkSame(0, $knob->heartbeatCalls, 'an unstarted instance runs no registration pump');
        $knob->start();
        $this->checkSame(1, $knob->heartbeatCalls, 'start() beats once and schedules the next one');
        $this->checkSame($base + 5000, $this->getPrivate($knob, 'heartbeatNextRunMillis'),
            'start() anchors the next beat to the given interval, not to the constant');

        $knob->tick($base + 4999);
        $this->checkSame(1, $knob->heartbeatCalls, 'tick(+4999): the 5s beat is not due yet');
        $knob->tick($base + 5000);
        $this->checkSame(2, $knob->heartbeatCalls, 'tick(+5000): the 5s beat is due and runs');
        $this->checkSame($base + 10000, $this->getPrivate($knob, 'heartbeatNextRunMillis'),
            'the beat that just ran is rescheduled one interval out, using the knob again');
        $knob->tick($base + 9999);
        $this->checkSame(2, $knob->heartbeatCalls, 'tick(+9999): still not due');
        $knob->tick($base + 10000);
        $this->checkSame(3, $knob->heartbeatCalls, 'tick(+10000): the second 5s beat runs');
        $this->checkSame($base + 15000, $this->getPrivate($knob, 'heartbeatNextRunMillis'),
            'the schedule keeps advancing by the knob, never by the 30s default');
        $knob->shutdown();
        $this->checkSame(null, $this->getPrivate($knob, 'heartbeatNextRunMillis'), 'shutdown unschedules the beat');
        MQClientInstance::removeInstance('hb-knob-cid');

        // Control: no argument means the documented constant.
        $def = new TickProbeInstance('hb-default-cid', []);
        $this->checkSame(MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, $def->heartbeatIntervalMillis,
            'the default interval is the class constant');
        $this->checkSame(30_000, $def->heartbeatIntervalMillis, 'the default interval is 30s');
        $def->clock = static fn(): int => $base;
        $def->start();
        $this->checkSame($base + 30_000, $this->getPrivate($def, 'heartbeatNextRunMillis'),
            'the default schedule is anchored 30s out');
        $def->tick($base + 4999);
        $this->checkSame(1, $def->heartbeatCalls, 'tick(+4999): another client interval does not leak in');
        $def->tick($base + 29999);
        $this->checkSame(1, $def->heartbeatCalls, 'tick(+29999): the default beat is not due yet');
        $def->tick($base + 30000);
        $this->checkSame(2, $def->heartbeatCalls, 'tick(+30000): the default beat runs');
        $def->shutdown();
        MQClientInstance::removeInstance('hb-default-cid');

        // A value of 0 or less falls back to the constant instead of disabling or spinning the pump.
        foreach ([0, -1] as $bad) {
            $cid = 'hb-invalid-' . $bad . '-cid';
            $fallback = new TickProbeInstance($cid, [], heartbeatIntervalMillis: $bad);
            $this->checkSame(MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, $fallback->heartbeatIntervalMillis,
                "interval $bad falls back to the constant");
            $fallback->clock = static fn(): int => $base;
            $fallback->start();
            $this->checkSame(1, $fallback->heartbeatCalls, "interval $bad still beats once at start");
            $this->checkSame($base + 30_000, $this->getPrivate($fallback, 'heartbeatNextRunMillis'),
                "interval $bad schedules the constant period, not an immediate re-beat");
            $fallback->tick($base + 1);
            $this->checkSame(1, $fallback->heartbeatCalls, "interval $bad does not beat every tick");
            $fallback->shutdown();
            MQClientInstance::removeInstance($cid);
        }
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

    // ================================================================ 6. route cache and default-topic fallback

    private function testRouteCacheAndFallback(): void
    {
        $broker = FakeBroker::start();
        $inst = new MQClientInstance('route-cid', [$broker->addr], 2000, 3000);

        // normal route fetch lands in cache
        $ok = $inst->updateTopicRouteInfoFromNameServer('RouteTopic');
        $this->checkSame(true, $ok, 'RouteTopic route fetch succeeds');
        $route = $inst->getTopicRouteData('RouteTopic');
        $this->check($route instanceof TopicRouteData, 'topicRouteTable hit');
        $this->checkSame(
            true,
            $route === ($this->getPrivate($inst, 'topicRouteTable')['RouteTopic'] ?? null),
            'getTopicRouteData returns the same cached object',
        );
        $this->checkSame(['127.0.0.1:10911'], $this->getPrivate($inst, 'brokerAddrTable')['broker-a'] ?? [], 'brokerAddrTable persisted');
        $this->checkSame('127.0.0.1:10911', $inst->findBrokerAddressInPublish('broker-a'), 'findBrokerAddressInPublish takes the master address');
        $this->checkSame('127.0.0.1:10911', MQClientInstance::findBrokerAddrInRoute($route, 'broker-a'), 'findBrokerAddrInRoute takes the address');
        $this->checkSame(null, MQClientInstance::findBrokerAddrInRoute($route, 'nope'), 'findBrokerAddrInRoute returns null for unknown broker');

        $info = $inst->getTopicPublishInfo('RouteTopic');
        $this->checkSame(2, count($info->msgQueueList), 'getTopicPublishInfo yields 2 write queues');
        $this->checkSame(false, $info->orderTopic, 'non-ordered topic orderTopic=false');
        $this->checkSame(2, count($inst->getTopicSubscribeInfo('RouteTopic')), 'getTopicSubscribeInfo yields 2 read queues');

        // real route unavailable and not default -> returns false, nothing cached
        $this->checkSame(false, $inst->updateTopicRouteInfoFromNameServer('OtherTopic'), 'unknown topic returns false in non-default mode');
        $this->checkSame(null, $this->getPrivate($inst, 'topicRouteTable')['OtherTopic'] ?? null, 'unknown topic not cached');

        // default topic fallback: NewTopic does not exist -> synthesized from TBW102, write queues trimmed to 4
        $fallback = $inst->updateTopicRouteInfoFromNameServer('NewTopic', 5000, true);
        $this->checkSame(true, $fallback, 'default topic fallback succeeds');
        $newInfo = $inst->getTopicPublishInfo('NewTopic');
        $this->checkSame(MixAll::DEFAULT_TOPIC_QUEUE_NUMS, count($newInfo->msgQueueList), 'fallback queue count trimmed to DEFAULT_TOPIC_QUEUE_NUMS');
        $this->checkSame('broker-def', $newInfo->msgQueueList[0]->brokerName, 'fallback uses the TBW102 broker');

        // non-fallback mode: NewTopic still does not exist
        $this->checkSame(false, $inst->updateTopicRouteInfoFromNameServer('NewTopic'), 'non-isDefault does not trigger fallback (TOPIC_NOT_EXIST -> false)');

        // publishAddrFor: known broker hits directly; unknown broker throws
        $this->checkSame('127.0.0.1:10911', $inst->publishAddrFor('broker-a', 'RouteTopic'), 'publishAddrFor hits the master address');
        $this->checkThrows(
            fn() => $inst->publishAddrFor('ghost-broker', 'RouteTopic'),
            MQClientException::class,
            'publishAddrFor unknown broker throws MQClientException',
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

        // FOUND: parses response header + body
        $mq = new MessageQueue('PullTopic', 'broker-pull', 0);
        $found = $inst->pullMessage('GID-pull', $mq, 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::FOUND, $found->status, 'PULL FOUND status');
        $this->checkSame(10, $found->nextBeginOffset, 'PULL nextBeginOffset');
        $this->checkSame(1, $found->minOffset, 'PULL minOffset');
        $this->checkSame(100, $found->maxOffset, 'PULL maxOffset');
        $this->checkSame(1, $found->suggestWhichBrokerId, 'PULL suggestWhichBrokerId');
        $this->checkSame(1, count($found->msgFoundList), 'PULL brings back 1 message');
        $this->checkSame('pull-body-0', $found->msgFoundList[0]->getBody(), 'PULL message body');
        $this->checkSame('broker-pull', $found->msgFoundList[0]->brokerName, 'PULL backfills brokerName');
        $this->checkSame(0, $found->msgFoundList[0]->queueId, 'PULL backfills queueId');

        // response codes -> PullStatus mapping
        $noNew = $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 1), 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::NO_NEW_MSG, $noNew->status, 'PULL_NOT_FOUND -> NO_NEW_MSG');
        $illegal = $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 2), 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::OFFSET_ILLEGAL, $illegal->status, 'PULL_OFFSET_MOVED -> OFFSET_ILLEGAL');
        $noMatch = $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 3), 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::NO_MATCHED_MSG, $noMatch->status, 'PULL_RETRY_IMMEDIATELY -> NO_MATCHED_MSG');

        // invalid code -> MQBrokerException
        $this->checkThrows(
            fn() => $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 4), 0, 32, 0, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000),
            MQBrokerException::class,
            'PULL invalid response code throws MQBrokerException',
        );

        // lite pull bit -> request code switches to LITE_PULL_MESSAGE (broker echoes the request code in remark)
        $lite = $inst->pullMessage('GID-pull', new MessageQueue('PullTopic', 'broker-pull', 0), 0, 32, PullSysFlag::FLAG_LITE_PULL_MESSAGE, 0, null, 0, null, addr: $broker->addr, timeoutMillis: 3000);
        $this->checkSame(PullStatus::FOUND, $lite->status, 'lite pull still parses the response');

        // findBrokerAddrInSubscribe master/slave selection
        $this->checkSame(['127.0.0.1:1', false], MQClientInstance::findBrokerAddrInSubscribe([0 => '127.0.0.1:1'], 0), 'findBrokerAddrInSubscribe hits master');
        [$slaveAddr, $isSlave] = MQClientInstance::findBrokerAddrInSubscribe([0 => '127.0.0.1:1', 1 => '127.0.0.1:2'], 1);
        $this->checkSame('127.0.0.1:2', $slaveAddr, 'findBrokerAddrInSubscribe picks the specified slave');
        $this->checkSame(true, $isSlave, 'findBrokerAddrInSubscribe slave flag');
        [$fallbackAddr, $fallbackSlave] = MQClientInstance::findBrokerAddrInSubscribe([0 => '127.0.0.1:1'], 5);
        $this->checkSame('127.0.0.1:1', $fallbackAddr, 'findBrokerAddrInSubscribe unknown id falls back to the first');
        $this->checkSame(false, $fallbackSlave, 'findBrokerAddrInSubscribe fallback is master, not slave');

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
        $this->checkSame(PopStatus::FOUND, $pop->status, 'POP FOUND status');
        $this->checkSame(7, $pop->restNum, 'POP restNum');
        $this->checkSame(111, $pop->popTime, 'POP popTime');
        $this->checkSame(222, $pop->invisibleTime, 'POP invisibleTime');
        $this->checkSame(333, $pop->reviveQid, 'POP reviveQid');
        $this->checkSame('0 0 5', $pop->startOffsetInfo, 'POP startOffsetInfo kept as-is');
        $this->checkSame('0 0 5,6', $pop->msgOffsetInfo, 'POP msgOffsetInfo kept as-is');
        $this->checkSame(2, count($pop->msgFoundList), 'POP brings back 2 messages');

        $m0 = $pop->msgFoundList[0];
        $m1 = $pop->msgFoundList[1];
        $this->checkSame('PopTopic', $m0->topic, 'POP message topic restored to the requested topic');
        $this->checkSame('broker-pop', $m0->brokerName, 'POP message brokerName backfilled');
        $this->checkSame(5, $m0->queueOffset, 'POP message 0 queueOffset');
        $this->checkSame(6, $m1->queueOffset, 'POP message 1 queueOffset');
        $this->checkSame('pop-body-5', $m0->getBody(), 'POP message 0 body');
        $this->checkSame('pop-body-6', $m1->getBody(), 'POP message 1 body');

        // POP_CK reconstruction: ckQueueOffset uses startOffset(5), tail segment uses each message's own offset
        $expected0 = ExtraInfoUtil::buildExtraInfo(5, 111, 222, 333, 'PopTopic', 'broker-pop', 0, 5);
        $expected1 = ExtraInfoUtil::buildExtraInfo(5, 111, 222, 333, 'PopTopic', 'broker-pop', 0, 6);
        $this->checkSame($expected0, $m0->getProperty(MessageConst::PROPERTY_POP_CK), 'POP_CK reconstruction (message 0)');
        $this->checkSame($expected1, $m1->getProperty(MessageConst::PROPERTY_POP_CK), 'POP_CK reconstruction (message 1)');
        $this->checkSame('111', $m0->getProperty(MessageConst::PROPERTY_FIRST_POP_TIME), '1ST_POP_TIME uses popTime');
        $this->checkSame('111', $m1->getProperty(MessageConst::PROPERTY_FIRST_POP_TIME), '1ST_POP_TIME (message 1)');

        // status mapping: POLLING_FULL / POLLING_TIMEOUT
        $full = $inst->popMessage('GID-pop', 'PopTopic', 9, 32, 60000, 0, 0, null, null, false, 'broker-pop', 3000, $broker->addr);
        $this->checkSame(PopStatus::POLLING_FULL, $full->status, 'POLLING_FULL status mapping');
        $this->checkSame(0, count($full->msgFoundList), 'POLLING_FULL has no messages');
        $timeout = $inst->popMessage('GID-pop', 'PopTopic', 10, 32, 60000, 0, 0, null, null, false, 'broker-pop', 3000, $broker->addr);
        $this->checkSame(PopStatus::POLLING_NOT_FOUND, $timeout->status, 'POLLING_TIMEOUT -> POLLING_NOT_FOUND');

        $inst->shutdown();
        MQClientInstance::removeInstance('pop-cid');
        $broker->stop();
    }

    // ================================================================ 9. offset query (incidental)

    private function testQueryConsumerOffset(): void
    {
        $broker = FakeBroker::start();
        $inst = new MQClientInstance('offset-cid', [$broker->addr], 2000, 3000);
        $offset = $inst->queryConsumerOffset('GID-offset', new MessageQueue('OffsetTopic', 'broker-a', 0), 3000, $broker->addr);
        $this->checkSame(42, $offset, 'queryConsumerOffset parses offset');
        $inst->shutdown();
        MQClientInstance::removeInstance('offset-cid');
        $broker->stop();
    }

    // ================================================================ 9. namespaceV2 on-wire evidence

    /**
     * Whether the NamespaceRpcHook installed in the MQClientInstance constructor is really attached
     * at the transport layer — proven from the **peer's** side.
     *
     * Criteria (5.x server-side namespace extension-header semantics):
     *   * the transport layer registers NamespaceRpcHook **unconditionally**, reading
     *     `clientConfig.getNamespaceV2()` fresh on every request; when the value is empty
     *     `doBeforeRequest` returns immediately, adding not a single extField.
     *   * therefore all three cases must be proven on the wire: ① unset -> goes out bare;
     *     ② configured at construction => the very first request carries `nsd=true`/`ns=<value>`;
     *     ③ changing the value after start => the next request follows immediately (a snapshot-holding
     *     implementation would silently fail this case, and silence is the hardest to debug:
     *     the broker would just treat the request as "no namespace").
     *
     * On seeing SEND_MESSAGE with `probe=ns`, the fake broker echoes the **received** ns/nsd
     * verbatim in remark (see brokerRespond in this file).
     */
    private function testNamespaceRpcHookOnWire(): void
    {
        $broker = FakeBroker::start();

        $unset = new MQClientInstance('ns-unset', [$broker->addr], 2000, 3000);
        $this->checkSame('ns=;nsd=', $this->probeNamespace($unset, $broker->addr),
            'namespaceV2 unset: ns/nsd absent on the wire (hook installed but a no-op)');
        $unset->shutdown();
        MQClientInstance::removeInstance('ns-unset');

        $named = new MQClientInstance('ns-named', [$broker->addr], 2000, 3000,
            namespaceV2: 'MQ_INST_ctor');
        $this->checkSame('ns=MQ_INST_ctor;nsd=true', $this->probeNamespace($named, $broker->addr),
            'configured at construction: the very first request carries ns/nsd');

        // read-fresh semantics: config change needs no restart, the next request follows
        $named->namespaceV2 = 'MQ_INST_late';
        $this->checkSame('ns=MQ_INST_late;nsd=true', $this->probeNamespace($named, $broker->addr),
            'changing namespaceV2 after start takes effect on the next request');

        $named->namespaceV2 = '';
        $this->checkSame('ns=;nsd=', $this->probeNamespace($named, $broker->addr),
            'after clearing, back to bare requests (hook does not retain the previous ns)');

        $named->shutdown();
        MQClientInstance::removeInstance('ns-named');
        $broker->stop();
    }

    // ================================================================ 11. registration heartbeat

    /**
     * The instance beat is what registers a group whose facade drives no loop of its own, so it
     * has to carry every group the clientId owns and fire the moment the route cache shows a
     * broker that registration has not reached yet. `sendHeartbeat()` is overridden here so the
     * payload can be inspected on the way out without any broker round-trip.
     */
    private function testRegistrationHeartbeat(): void
    {
        $broker = FakeBroker::start();
        $probe = new HeartbeatProbeInstance('reg-cid', [$broker->addr], 2000, 3000);
        $probe->start();

        // A route landing registers nothing while the client owns no group at all.
        $this->checkSame(true, $probe->updateTopicRouteInfoFromNameServer('RouteTopic'), 'RouteTopic route cached');
        $this->checkSame([], $probe->beats, 'a beat carries nothing until a group is registered');
        $this->checkSame(0, $probe->heartbeatCount(), 'an empty round is not a successful beat');

        // Producer-only: groups sorted, no consumer half, and the known broker beaten once.
        $probe->registerProducer('GID_reg_b');
        $probe->registerProducer('GID_reg_a');
        $probe->heartbeatOnce();
        $this->checkSame(
            [['addr' => '127.0.0.1:10911', 'producers' => ['GID_reg_a', 'GID_reg_b'], 'consumers' => []]],
            $probe->beats,
            'a beat carries both producer groups and no consumer data',
        );
        $this->checkSame(1, $probe->heartbeatCount(), 'a round that lands counts once');
        $this->checkSame(['127.0.0.1:10911'], $this->getPrivate($probe, 'brokersBeaten'),
            'the reached broker is remembered as beaten');

        // Unchanged broker set: refreshing the same route registers nothing again.
        $probe->updateTopicRouteInfoFromNameServer('RouteTopic');
        $this->checkSame(1, count($probe->beats), 'a route refresh on an unchanged broker set does not re-beat');

        // A broker joins the cache (TBW102 lives on another machine): that one refresh registers
        // the new set, and the round beats every known broker, not only the newcomer.
        $this->checkSame(true, $probe->updateTopicRouteInfoFromNameServer('NewTopic', 5000, true), 'default-topic fallback cached');
        $this->checkSame(
            [
                ['addr' => '127.0.0.1:10911', 'producers' => ['GID_reg_a', 'GID_reg_b'], 'consumers' => []],
                ['addr' => '127.0.0.1:10911', 'producers' => ['GID_reg_a', 'GID_reg_b'], 'consumers' => []],
                ['addr' => '127.0.0.1:10912', 'producers' => ['GID_reg_a', 'GID_reg_b'], 'consumers' => []],
            ],
            $probe->beats,
            'one beat registers the whole known set, not only the broker that joined',
        );
        $this->checkSame(2, $probe->heartbeatCount(), 'the second round counts once regardless of broker count');
        $this->checkSame(['127.0.0.1:10911', '127.0.0.1:10912'], $this->getPrivate($probe, 'brokersBeaten'),
            'both brokers are remembered as beaten');

        // Consumers merge into the same payload, so a producer-only app and a mixed app differ
        // only in which half is filled.
        $consumer = new DefaultMQPushConsumer('GID_reg_consumer');
        $consumer->subscribe('RegSubTopic', 'tagA');
        $probe->registerConsumer('GID_reg_consumer', $consumer);
        $data = $probe->buildHeartbeatData();
        $this->checkSame(['GID_reg_a', 'GID_reg_b'], array_map(static fn($p) => $p->groupName, $data->producerDataSet),
            'buildHeartbeatData keeps the producer half');
        $this->checkSame(['GID_reg_consumer'], array_map(static fn($c) => $c->groupName, $data->consumerDataSet),
            'the consumer group rides along in the same beat');
        $sub = $data->consumerDataSet[0]->subscriptionDataSet[0] ?? null;
        $this->checkSame(['RegSubTopic', 'tagA'], $sub === null ? [] : [$sub->topic, $sub->subString],
            'the consumer slice keeps its subscription');

        // Dropping every producer group leaves only the consumer half; the beat still goes out.
        $probe->unregisterProducer('GID_reg_a');
        $probe->unregisterProducer('GID_reg_b');
        $probe->heartbeatOnce();
        $last = $probe->beats[count($probe->beats) - 1] ?? null;
        $this->checkSame(
            ['addr' => '127.0.0.1:10912', 'producers' => [], 'consumers' => ['GID_reg_consumer']],
            $last,
            'unregistered producer groups leave the payload',
        );

        // With nothing left registered a round sends no request at all -- this is what keeps a
        // client that shut every group down from re-registering itself.
        $probe->unregisterConsumer('GID_reg_consumer');
        $beatsAfterLastRound = count($probe->beats);
        $probe->heartbeatOnce();
        $this->checkSame($beatsAfterLastRound, count($probe->beats), 'an unregistered client beats nothing');

        $probe->shutdown();
        $this->checkSame([], $this->getPrivate($probe, 'brokersBeaten'), 'shutdown forgets which brokers were beaten');

        MQClientInstance::removeInstance('reg-cid');
        $broker->stop();
    }

    // ==================================================== 11b. heartbeat fingerprint rounds

    /**
     * Seed the two lookup tables one route fetch would have filled: the heartbeat fan-out walks the
     * cached topic routes, while the lock family resolves a broker by name, so a probe needs both
     * before a request leaves.
     */
    private function seedProbeRoute(MQClientInstance $probe): void
    {
        $route = new TopicRouteData();
        $route->brokerDatas = [new BrokerData('DefaultCluster', 'broker-a', [MixAll::MASTER_ID => '127.0.0.1:10911'])];
        $probe->topicRouteTable['V2BeatTopic'] = $route;
        $probe->brokerAddrTable['broker-a'] = [MixAll::MASTER_ID => '127.0.0.1:10911'];
    }

    /**
     * The fingerprint path decides per broker, per round, from what that broker answered last time.
     * Every assertion here is about which body left the client, because that is all the broker sees:
     * the full one while it has not said it understands the fields (or the moment it says its own
     * table moved), the subscription-free one only once both are remembered.
     */
    private function testHeartbeatV2Rounds(): void
    {
        $brokerAddrs = ['127.0.0.1:10911'];

        // -- control: switch off, the round is the plain registration body and stays that way.
        $legacy = new HeartbeatV2ProbeInstance('v2-off', $brokerAddrs, 2000, 3000);
        $this->seedProbeRoute($legacy);
        $legacy->registerProducer('GID_v2_p');
        $legacy->heartbeatOnce();
        $legacy->script[] = ['supportV2' => true, 'subChange' => false];
        $legacy->heartbeatOnce();
        $this->checkSame(2, count($legacy->sent), 'two rounds went out');
        $this->checkSame(
            [0, false],
            [$legacy->sent[0]['fingerprint'], $legacy->sent[0]['withoutSub']],
            'with the switch off the body reports fingerprint 0, which is the broker legacy path',
        );
        $this->checkSame([], $legacy->supportV2BrokerAddrsForTest(),
            'a client with the switch off never learns that a broker speaks the protocol');
        $this->checkSame([], $legacy->heartbeatFingerprintTable(), 'and remembers no fingerprint');
        $legacy->shutdown();
        MQClientInstance::removeInstance('v2-off');

        // -- switch on, broker does not know the fields: the full body keeps going out.
        $naive = new HeartbeatV2ProbeInstance('v2-naive', $brokerAddrs, 2000, 3000);
        $this->seedProbeRoute($naive);
        $naive->useHeartbeatV2 = true;
        $naive->registerProducer('GID_v2_p');
        $naive->heartbeatOnce();
        $naive->heartbeatOnce();
        $this->check($naive->sent[0]['fingerprint'] !== 0, 'a stamped body carries a non-zero fingerprint');
        $this->checkSame(false, $naive->sent[0]['withoutSub'], 'the first round to a broker sends the subscriptions');
        $this->checkSame($naive->sent[0]['fingerprint'], $naive->sent[1]['fingerprint'],
            'unchanged subscriptions keep the same fingerprint');
        $this->checkSame(false, $naive->sent[1]['withoutSub'],
            'a broker that does not answer the fields keeps receiving the subscriptions');
        $this->checkSame([], $naive->supportV2BrokerAddrsForTest(),
            'only an IS_SUPPORT_HEART_BEAT_V2 answer earns the address a place in the tables');
        $naive->shutdown();
        MQClientInstance::removeInstance('v2-naive');

        // -- switch on, broker answers: full round, then cheap rounds, then whatever moves it back.
        $p = new HeartbeatV2ProbeInstance('v2-live', $brokerAddrs, 2000, 3000);
        $this->seedProbeRoute($p);
        $p->useHeartbeatV2 = true;
        $p->registerProducer('GID_v2_p');
        $consumer = new DefaultMQPushConsumer('GID_v2_c');
        $consumer->subscribe('V2SubTopic', 'tagA');
        $p->registerConsumer('GID_v2_c', $consumer);

        $p->script[] = ['supportV2' => true, 'subChange' => false];
        $p->heartbeatOnce();
        $full = $p->sent[0];
        $this->checkSame(['V2SubTopic:tagA'], $full['subs'], 'the first body carries the subscription');
        $this->checkSame(['127.0.0.1:10911'], $p->supportV2BrokerAddrsForTest(),
            'the address that answered IS_SUPPORT_HEART_BEAT_V2 is remembered');
        $this->checkSame(['127.0.0.1:10911' => $full['fingerprint']], $p->heartbeatFingerprintTable(),
            'and the fingerprint it accepted is stored under that address');

        $p->heartbeatOnce();
        $cheap = $p->sent[1];
        $this->checkSame(true, $cheap['withoutSub'], 'the remembered broker gets the subscription-free body');
        $this->checkSame([], $cheap['subs'], 'and that body really holds no subscription set');
        $this->checkSame($full['producers'], $cheap['producers'], 'the producer half still rides along');
        $this->checkSame($full['consumers'], $cheap['consumers'], 'so does the consumer group');
        $this->checkSame($full['fingerprint'], $cheap['fingerprint'],
            'the cheap body keeps the full body stamp rather than recomputing one');

        // The broker says its own table moved: forget the fingerprint, send everything again.
        $p->script[] = ['supportV2' => true, 'subChange' => true];
        $p->heartbeatOnce();
        $this->checkSame(true, $p->sent[2]['withoutSub'],
            'a broker that already knows us gets the cheap body first, then learns it disagrees');
        $this->checkSame([], $p->heartbeatFingerprintTable(),
            'IS_SUB_CHANGE drops the remembered fingerprint so the next round re-delivers');
        $p->script[] = ['supportV2' => true, 'subChange' => false];
        $p->heartbeatOnce();
        $this->checkSame(false, $p->sent[3]['withoutSub'], 'the next round carries the subscriptions again');
        $this->checkSame(['V2SubTopic:tagA'], $p->sent[3]['subs'], 'the whole subscription set is back');
        $this->checkSame(['127.0.0.1:10911' => $p->sent[3]['fingerprint']], $p->heartbeatFingerprintTable(),
            'and the re-delivered body re-arms the remembered stamp');

        // A new subscription is a new fingerprint, so the remembered broker gets the full body.
        $p->heartbeatOnce();
        $this->checkSame(true, $p->sent[4]['withoutSub'], 'once re-armed the broker is back on the cheap body');
        $consumer->subscribe('V2SecondTopic', 'tagB');
        $p->script[] = ['supportV2' => true, 'subChange' => false];
        $p->heartbeatOnce();
        $this->checkSame(false, $p->sent[5]['withoutSub'],
            'a subscription change moves the fingerprint, so the full body goes out');
        $this->check(['V2SubTopic:tagA', 'V2SecondTopic:tagB'] === array_values($p->sent[5]['subs']),
            'both subscriptions are reported in the round that re-delivers them');

        // A rebalance round must re-deliver even to a broker that is already current.
        $p->heartbeatOnce();
        $this->checkSame(true, $p->sent[6]['withoutSub'], 'the queue change is remembered as accepted again');
        $p->heartbeatOnceAfterRebalance();
        $this->checkSame(false, $p->sent[7]['withoutSub'],
            'the round after an allocation change re-delivers the subscriptions');
        $this->checkSame(['127.0.0.1:10911'], $p->supportV2BrokerAddrsForTest(),
            'but knowing that the broker speaks the protocol survives the reset');

        // The frame never changes: no separate request code for the fingerprint path.
        $this->check(array_reduce($p->sent, static fn(bool $c, array $b): bool => $c && $b['code'] === RequestCode::HEART_BEAT, true),
            'every round of either path is a HEART_BEAT(34) request');

        $p->shutdown();
        MQClientInstance::removeInstance('v2-live');
    }

    // ==================================================== 11c. VIP channel address rewrite

    /**
     * The fast channel is a per-request port change and never a change to the cached route: only
     * the RPCs the protocol allows are moved, each one at the moment it leaves the client. Both
     * directions are pinned here -- a call site left unwrapped silently loses the fast channel,
     * while a heartbeat, a pull or a registration that *was* wrapped would aim at a port a broker
     * only opens when the channel is enabled.
     */
    private function testVipChannelAddressRewrite(): void
    {
        $listen = '127.0.0.1:10911';
        $fast = '127.0.0.1:10909';
        $mq = new MessageQueue('V2BeatTopic', 'broker-a', 0);

        $mk = static fn(string $id, bool $on): VipChannelProbeInstance =>
            new VipChannelProbeInstance($id, ['127.0.0.1:9876'], 2000, 3000, vipChannelEnabled: $on);

        // -- control: switch off, so every request still goes to the port the route table holds.
        $off = $mk('vip-off', false);
        $this->seedProbeRoute($off);
        $this->checkSame($listen, $off->vipAddrFor($listen), 'switch off leaves the address alone');
        $this->runVipProbeCalls($off, $mq, $listen);
        $this->checkSame([], array_values(array_filter($off->wire, static fn(array $w): bool => $w['addr'] !== $listen)),
            'with the switch off not one broker RPC is moved');
        $off->shutdown();
        MQClientInstance::removeInstance('vip-off');

        // -- switch on: the offset / consumer-list / lock / administration family moves.
        $on = $mk('vip-on', true);
        $this->seedProbeRoute($on);
        $this->checkSame($fast, $on->vipAddrFor($listen), 'switch on moves the port by 2');
        $this->runVipProbeCalls($on, $mq, $listen);
        foreach ([
            RequestCode::QUERY_CONSUMER_OFFSET,
            RequestCode::UPDATE_CONSUMER_OFFSET,
            RequestCode::GET_MAX_OFFSET,
            RequestCode::GET_MIN_OFFSET,
            RequestCode::SEARCH_OFFSET_BY_TIMESTAMP,
            RequestCode::GET_CONSUMER_LIST_BY_GROUP,
            RequestCode::LOCK_BATCH_MQ,
            RequestCode::UNLOCK_BATCH_MQ,
            RequestCode::CHECK_CLIENT_CONFIG,
            RequestCode::UPDATE_AND_CREATE_TOPIC,
            RequestCode::DELETE_TOPIC_IN_BROKER,
            RequestCode::SEND_MESSAGE_V2,
            RequestCode::QUERY_MESSAGE,
        ] as $code) {
            $this->checkSame([$fast], $on->addrSetFor($code), sprintf('request %d targets the fast port', $code));
        }

        // -- switch on, and still never moved: registration traffic, the pull and pop family.
        foreach ([
            RequestCode::HEART_BEAT,
            RequestCode::UNREGISTER_CLIENT,
            RequestCode::PULL_MESSAGE,
            RequestCode::POP_MESSAGE,
            RequestCode::ACK_MESSAGE,
            RequestCode::CHANGE_MESSAGE_INVISIBLETIME,
        ] as $code) {
            $this->checkSame([$listen], $on->addrSetFor($code), sprintf('request %d keeps the listening port', $code));
        }
        $this->checkSame($on->topicRouteTable['V2BeatTopic']->brokerDatas[0]->brokerAddrs,
            [MixAll::MASTER_ID => $listen], 'the cached route is never rewritten, only each request');
        $on->shutdown();
        MQClientInstance::removeInstance('vip-on');
    }

    /**
     * Fires one call per VIP-allowed and one per never-rewritten broker RPC through the recording
     * probe. Only the address the request leaves on is under test, so a response this canned body
     * cannot decode is expected and ignored.
     */
    private function runVipProbeCalls(VipChannelProbeInstance $probe, MessageQueue $mq, string $listen): void
    {
        $subscription = new SubscriptionData('V2BeatTopic', 'tagVip');
        $beat = new HeartbeatData($probe->clientId);
        $beat->producerDataSet[] = new ProducerData('GID_vip_p');
        $extraInfo = ExtraInfoUtil::buildExtraInfo(0, 0, 0, 0, 'V2BeatTopic', 'broker-a', 0, 1);

        $calls = [
            // The offset family.
            fn() => $probe->queryConsumerOffset('GID_vip', $mq, 3000, $listen),
            fn() => $probe->updateConsumerOffset('GID_vip', $mq, 7, 3000, $listen),
            fn() => $probe->getMaxOffset($mq, 3000, $listen),
            fn() => $probe->getMinOffset($mq, 3000, $listen),
            fn() => $probe->searchOffsetByTimestamp($mq, 1_700_000_000_000, 3000, $listen),
            // The consumer-side family.
            fn() => $probe->getConsumerListByGroup('GID_vip', 3000, $listen),
            fn() => $probe->getConsumerIdListByGroup('V2BeatTopic', 'GID_vip', 3000),
            fn() => $probe->lockBatchMq('GID_vip', $probe->clientId, [$mq], 3000),
            fn() => $probe->unlockBatchMq('GID_vip', $probe->clientId, [$mq], 3000),
            fn() => $probe->checkClientConfig($listen, 'GID_vip', $probe->clientId, $subscription, 3000),
            // The administration family the client itself calls.
            fn() => $probe->createTopicInBroker($listen, 'VipTopic', 'VipTopic', 1, 1, 6, 0, timeoutMillis: 3000, retryTimes: 1),
            fn() => $probe->deleteTopicInBroker($listen, 'VipTopic', 3000),
            // A producer send follows the same switch.
            fn() => $probe->sendMessageToAddr('GID_vip', new Message('V2BeatTopic', 'vip-body'), $mq, $listen, 3000),
            // The never-rewritten half.
            fn() => $probe->sendHeartbeat($listen, $beat, 3000),
            fn() => $probe->sendHeartbeatV2($listen, $beat, $beat, 1234, 3000),
            fn() => $probe->unregisterClient($listen, $probe->clientId, 'GID_vip_p', 'GID_vip', 3000),
            fn() => $probe->pullMessage('GID_vip', $mq, 0, 1, PullSysFlag::FLAG_COMMIT_OFFSET, 0, 'tagVip', 1, 'TAG', 3000, -1, 100, $listen),
            fn() => $probe->popMessage('GID_vip', 'V2BeatTopic', 0, 1, 1000, 100, 0, 'tagVip', 'TAG', false, 'broker-a', 3000, $listen),
            fn() => $probe->ackMessage('GID_vip', 'V2BeatTopic', 0, $extraInfo, 0, 'broker-a', 3000, $listen),
            fn() => $probe->changeInvisibleTime('GID_vip', 'V2BeatTopic', 0, $extraInfo, 0, 1000, 'broker-a', 3000, $listen),
            fn() => $probe->queryMessage('V2BeatTopic', 'vip-key', 1, 1_700_000_000_000, 1_700_000_001_000, 3000, $listen),
        ];
        foreach ($calls as $call) {
            try {
                $call();
            } catch (\Throwable) {
                // The canned response body is not decodable for every op; the wire address is the observation.
            }
        }
    }


    /** Sends one namespaceV2 probe, returning the peer-echoed `ns=<value>;nsd=<value>`. */
    private function probeNamespace(MQClientInstance $inst, string $addr): string
    {
        $cmd = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, null);
        $cmd->addExtField('probe', 'ns');
        $resp = $inst->remotingClient->invokeSync($addr, $cmd, 3000);
        return (string) ($resp->remark ?? '');
    }

    // ================================================================ run

    public function run(): int
    {
        $this->testTopicPublishInfo();
        $this->testBuildSendRequest();
        $this->testParseSendResponse();
        $this->testProcessReplyMessage();
        $this->testTickThresholds();
        $this->testHeartbeatIntervalKnob();
        $this->testRouteCacheAndFallback();
        $this->testPullMessage();
        $this->testPopMessage();
        $this->testQueryConsumerOffset();
        $this->testNamespaceRpcHookOnWire();
        $this->testRegistrationHeartbeat();
        $this->testHeartbeatV2Rounds();
        $this->testVipChannelAddressRewrite();
        return $this->summary();
    }
}

/**
 * tick() probe: overrides each pump with counters; pure-logic verification of time-threshold decisions.
 * MQClientInstance is not declared final, so it can be subclassed directly.
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

    public function heartbeatOnce(?int $timeoutMillis = null): int
    {
        $this->heartbeatCalls++;
        return 1;
    }
}

/**
 * Registration probe: records every heartbeat request the instance puts on the wire together with
 * the payload built for it, so the producer/consumer halves and the broker set of a round can be
 * asserted without a broker round-trip.
 */
class HeartbeatProbeInstance extends MQClientInstance
{
    /** @var list<array{addr: string, producers: list<string>, consumers: list<string>}> */
    public array $beats = [];

    public function sendHeartbeat(string $addr, HeartbeatData $heartbeatData, ?int $timeoutMillis = null): void
    {
        $this->beats[] = [
            'addr' => $addr,
            'producers' => array_map(static fn(ProducerData $p): string => $p->groupName, $heartbeatData->producerDataSet),
            'consumers' => array_map(static fn(ConsumerData $c): string => $c->groupName, $heartbeatData->consumerDataSet),
        ];
    }
}

/**
 * Fingerprint-path probe: takes over the instance's single synchronous exit point, records which
 * heartbeat body went out, and answers with the two response fields the next round is decided from.
 * `$script` is consumed one entry per beat; anything left over reads as "this broker does not know
 * the fields".
 */
class HeartbeatV2ProbeInstance extends MQClientInstance
{
    /** @var list<array{code: int, addr: string, fingerprint: int, withoutSub: bool, producers: list<string>, consumers: list<string>, subs: list<string>}> */
    public array $sent = [];

    /** @var list<array{supportV2: bool, subChange: bool}> */
    public array $script = [];

    protected function invokeSync(string $addr, RemotingCommand $request, ?int $timeoutMillis = null): RemotingCommand
    {
        $data = $request->code === RequestCode::HEART_BEAT && ($request->body ?? '') !== ''
            ? HeartbeatData::decode($request->body)
            : null;

        $subs = [];
        foreach ($data?->consumerDataSet ?? [] as $cd) {
            foreach ($cd->subscriptionDataSet as $sd) {
                $subs[] = $sd->topic . ':' . $sd->subString;
            }
        }
        $this->sent[] = [
            'code' => $request->code,
            'addr' => $addr,
            'fingerprint' => $data?->heartbeatFingerprint ?? 0,
            'withoutSub' => $data?->withoutSub ?? false,
            'producers' => array_map(static fn(ProducerData $p): string => $p->groupName, $data?->producerDataSet ?? []),
            'consumers' => array_map(static fn(ConsumerData $c): string => $c->groupName, $data?->consumerDataSet ?? []),
            'subs' => $subs,
        ];

        $answer = array_shift($this->script) ?? ['supportV2' => false, 'subChange' => false];
        $response = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $response->addExtField(MixAll::IS_SUPPORT_HEART_BEAT_V2, $answer['supportV2'] ? 'true' : 'false');
        $response->addExtField(MixAll::IS_SUB_CHANGE, $answer['subChange'] ? 'true' : 'false');
        return $response;
    }
}

/**
 * VIP-channel probe: records the address every request really left on, then answers with a body
 * that decodes for most operations. The observation is the address, never the result.
 */
class VipChannelProbeInstance extends MQClientInstance
{
    /** @var list<array{code: int, addr: string}> */
    public array $wire = [];

    protected function invokeSync(string $addr, RemotingCommand $request, ?int $timeoutMillis = null): RemotingCommand
    {
        $this->wire[] = ['code' => $request->code, 'addr' => $addr];
        $response = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $response->body = '{"offset":7,"consumerIdList":["vip@client"],"maxOffset":7,"minOffset":0}';
        return $response;
    }

    /** @return list<string> The distinct addresses used for one request code. */
    public function addrSetFor(int $code): array
    {
        $addrs = [];
        foreach ($this->wire as $w) {
            if ($w['code'] === $code) {
                $addrs[$w['addr']] = $w['addr'];
            }
        }
        return array_values($addrs);
    }
}

exit((new RunClientInstance())->run());
