<?php

declare(strict_types=1);

/**
 * Send-back fallback on both consumer facades (offline: the bounce goes to a child-process
 * fake broker, the retry send is recorded instead of dialed).
 *
 * Run: php tests/RunClientSendBackFallback.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * A refused bounce (a non-success answer, a transport error, a missing master) must not lose the
 * redelivery: it degrades to a normal send to %RETRY%<group> through the internal producer. The
 * legs pinned here are:
 *
 *   1. the successful bounce is a CONTROL: it sends nothing to %RETRY% and the request itself
 *      carries the group/origin/delay fields plus the effective retry ceiling;
 *   2. push: a rejected answer falls back to exactly one retry send whose copy carries the whole
 *      retry contract (topic, RETRY_TOPIC, RECONSUME_TIME, MAX_RECONSUME_TIMES, ORIGIN_MESSAGE_ID,
 *      DELAY) and no half-message marker, while the SOURCE message keeps its marker;
 *   3. push: a transport error (a dead port) and a missing master fall back the same way, and
 *      maxReconsumeTimes moves what both the bounce and the copy report;
 *   4. push: when the fallback send fails too, the ORIGINAL error reaches the caller;
 *   5. the orderly path sends the same retry copy and reports success only for a confirmed send;
 *   6. pull: a rejected answer falls back with 16 as the default ceiling, and the knob moves what
 *      the request carries (5 stays 5, -1 reports 16).
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\SendResult;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\TopicPublishInfo;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;

// ======================================================================
// child-process fake broker: `php RunClientSendBackFallback.php --broker`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    sbBrokerMain();
    exit(0);
}

/**
 * The fake broker answers CONSUMER_SEND_MSG_BACK per origin topic: "Reject*" is refused with
 * SYSTEM_ERROR, everything else succeeds. Every bounce is appended to the JSONL file named by
 * RMQ_SENDBACK_LOG so the parent can assert the request exactly as it went on the wire.
 *
 * Connections are multiplexed with stream_select: each consumer leg keeps its own channel open
 * for the whole run, so a broker that served one connection to exhaustion would stall every leg
 * after the first.
 */
function sbBrokerMain(): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "send-back broker: cannot listen: {$errstr}\n");
        return;
    }
    stream_set_blocking($server, false);
    fwrite(STDOUT, (string) stream_socket_get_name($server, false) . "\n");
    fflush(STDOUT);

    /** @var array<int, resource> $clients */
    $clients = [];
    /** @var array<int, string> $buffers */
    $buffers = [];
    $idleDeadline = microtime(true) + 30.0;
    while (microtime(true) < $idleDeadline) {
        $read = array_merge([$server], array_values($clients));
        $write = null;
        $except = null;
        $ready = @stream_select($read, $write, $except, 1);
        if ($ready === false) {
            break;
        }
        if ($ready === 0) {
            continue;
        }
        foreach ($read as $stream) {
            if ($stream === $server) {
                $conn = @stream_socket_accept($server, 0);
                if ($conn === false) {
                    continue;
                }
                stream_set_blocking($conn, false);
                $clients[(int) $conn] = $conn;
                $buffers[(int) $conn] = '';
                continue;
            }
            $key = (int) $stream;
            $chunk = @fread($stream, 65536);
            if ($chunk === '' || $chunk === false) {
                if (feof($stream)) {
                    @fclose($stream);
                    unset($clients[$key], $buffers[$key]);
                }
                continue;
            }
            $idleDeadline = microtime(true) + 30.0;
            $buffers[$key] .= $chunk;
            while (($cmd = sbTakeFrame($buffers[$key])) !== null) {
                $resp = sbAnswer($cmd);
                $resp->opaque = $cmd->opaque;
                @fwrite($stream, $resp->encode());
            }
        }
    }
    foreach ($clients as $conn) {
        @fclose($conn);
    }
    @fclose($server);
}

/** Pops one complete frame out of the buffer, or returns null when more bytes are needed. */
function sbTakeFrame(string &$buffer): ?RemotingCommand
{
    if (strlen($buffer) < 4) {
        return null;
    }
    $total = RocketMQSerializable::unpackSignedInt(substr($buffer, 0, 4));
    if ($total <= 0) {
        $buffer = '';
        return null;
    }
    if (strlen($buffer) < 4 + $total) {
        return null;
    }
    $frame = substr($buffer, 0, 4 + $total);
    $buffer = substr($buffer, 4 + $total);
    return RemotingCommand::decode($frame);
}

function sbAnswer(RemotingCommand $cmd): RemotingCommand
{
    $ext = $cmd->extFields;
    if ($cmd->code !== RequestCode::CONSUMER_SEND_MSG_BACK) {
        return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
    $originTopic = (string) ($ext['originTopic'] ?? '');
    file_put_contents(
        (string) getenv('RMQ_SENDBACK_LOG'),
        json_encode([
            'code' => $cmd->code,
            'group' => (string) ($ext['group'] ?? ''),
            'originTopic' => $originTopic,
            'originMsgId' => (string) ($ext['originMsgId'] ?? ''),
            'offset' => (int) ($ext['offset'] ?? 0),
            'delayLevel' => (int) ($ext['delayLevel'] ?? 0),
            'maxReconsumeTimes' => isset($ext['maxReconsumeTimes']) ? (int) $ext['maxReconsumeTimes'] : null,
            'unitMode' => (string) ($ext['unitMode'] ?? ''),
        ], JSON_UNESCAPED_SLASHES) . "\n",
        FILE_APPEND,
    );
    $code = str_starts_with($originTopic, 'Reject')
        ? ResponseCode::SYSTEM_ERROR
        : ResponseCode::SUCCESS;
    return RemotingCommand::createResponseCommand($code, $code === ResponseCode::SUCCESS ? '' : 'rejected by script');
}

/** Child-process fake broker handle. */
final class SendBackFakeBroker
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
            throw new RuntimeException('cannot spawn send-back broker');
        }
        $line = fgets($pipes[1]);
        $addr = $line === false ? '' : trim($line);
        if ($addr === '') {
            $err = stream_get_contents($pipes[2]) ?: '';
            proc_terminate($proc);
            proc_close($proc);
            throw new RuntimeException('send-back broker did not report an address: ' . $err);
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
// instance double
// ======================================================================

/**
 * Answers the bounce through the real transport (the fake broker), resolves the retry topic to a
 * writable queue, and RECORDS the retry send instead of putting it on the wire.
 */
final class SendBackInstance extends MQClientInstance
{
    /** @var list<Message> */
    public array $retrySends = [];

    public ?string $publishAddr;

    public bool $failRetrySend = false;

    public string $retryTopic;

    public function __construct(string $clientId, ?string $publishAddr, string $retryTopic)
    {
        parent::__construct($clientId, []);
        $this->publishAddr = $publishAddr;
        $this->retryTopic = $retryTopic;
    }

    public function findBrokerAddressInPublish(string $brokerName): ?string
    {
        return $this->publishAddr;
    }

    public function vipAddrFor(string $addr): string
    {
        return $addr;
    }

    public function getTopicPublishInfo(string $topic, bool $isDefault = false): TopicPublishInfo
    {
        $info = new TopicPublishInfo();
        $info->msgQueueList = [new MessageQueue($this->retryTopic, 'broker-a', 0)];
        return $info;
    }

    public function sendMessage(
        string $producerGroup,
        Message $msg,
        MessageQueue $mq,
        int $timeoutMillis = 3000,
        int $sysFlag = 0,
        bool $unitMode = false,
        ?string $defaultTopic = null,
        ?int $defaultTopicQueueNums = null,
    ): SendResult {
        if ($this->failRetrySend) {
            throw new RuntimeException('inner producer send failed');
        }
        $this->retrySends[] = $msg;
        return new SendResult(SendStatus::SEND_OK, 'retry-msg-id', $mq);
    }
}

// ======================================================================
// helpers
// ======================================================================

$checks = 0;
$failures = [];
/** Records one labelled assertion. */
$ck = static function (bool $cond, string $label) use (&$checks, &$failures): void {
    $checks++;
    if (!$cond) {
        $failures[] = $label;
        fwrite(STDOUT, "FAIL: {$label}\n");
    }
};
$ckSame = static function (mixed $expected, mixed $actual, string $label) use (&$checks, &$failures): void {
    $checks++;
    if ($expected !== $actual) {
        $failures[] = $label . ' (expected ' . var_export($expected, true) . ', got ' . var_export($actual, true) . ')';
        fwrite(STDOUT, "FAIL: {$label} expected=" . var_export($expected, true)
            . ' actual=' . var_export($actual, true) . "\n");
    }
};

const SBF_MSG_ID = '0A0B0C0D0000000000000000000EA7';

/** Builds the consumed message every leg starts from, including a half-message marker. */
function sbfMessage(string $topic, int $reconsumeTimes = 0): MessageExt
{
    $m = new MessageExt(topic: $topic, body: 'payload');
    $m->setBrokerName('broker-a');
    $m->setQueueId(0);
    $m->setQueueOffset(7);
    $m->setCommitLogOffset(123456);
    $m->setMsgId(SBF_MSG_ID);
    $m->setFlag(7);
    $m->setReconsumeTimes($reconsumeTimes);
    $m->putProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED, 'true');
    return $m;
}

function sbfPush(string $group, SendBackInstance $inst): DefaultMQPushConsumer
{
    $c = new DefaultMQPushConsumer($group);
    $c->mqClient = $inst;
    (new ReflectionProperty(DefaultMQPushConsumer::class, 'started'))->setValue($c, true);
    return $c;
}

function sbfPull(string $group, SendBackInstance $inst): DefaultMQPullConsumer
{
    $c = new DefaultMQPullConsumer($group);
    $c->mqClient = $inst;
    (new ReflectionProperty(DefaultMQPullConsumer::class, 'started'))->setValue($c, true);
    return $c;
}

/** Reads the bounces recorded since the previous call and truncates the log. */
function sbfBounces(): array
{
    $path = (string) getenv('RMQ_SENDBACK_LOG');
    if (!is_file($path)) {
        return [];
    }
    $lines = file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [];
    @unlink($path);
    $out = [];
    foreach ($lines as $line) {
        $decoded = json_decode($line, true);
        if (is_array($decoded)) {
            $out[] = $decoded;
        }
    }
    return $out;
}

/** Asserts the complete retry-copy contract for one recorded send. */
function sbfAssertRetry(callable $ckSame, Message $copy, string $group, int $maxReconsumeTimes): void
{
    $ckSame(MixAll::getRetryTopic($group), $copy->getTopic(), 'retry.copy.topic');
    $ckSame($maxReconsumeTimes, (int) $copy->getProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES), 'retry.copy.maxReconsumeTimes');
    $ckSame('1', (string) $copy->getProperty(MessageConst::PROPERTY_RECONSUME_TIME), 'retry.copy.reconsumeTime');
    $ckSame('3', (string) $copy->getProperty('DELAY'), 'retry.copy.delay');
    $ckSame(SBF_MSG_ID, (string) $copy->getProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID), 'retry.copy.originMsgId');
    $ckSame(null, $copy->getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED), 'retry.copy.noHalfMessageMarker');
    $ckSame('payload', (string) $copy->getBody(), 'retry.copy.body');
    $ckSame(7, $copy->getFlag(), 'retry.copy.flag');
}

// ======================================================================
// setup
// ======================================================================

$logPath = sys_get_temp_dir() . '/rmq_php_sendback_' . getmypid() . '.jsonl';
@unlink($logPath);
putenv('RMQ_SENDBACK_LOG=' . $logPath);
$broker = SendBackFakeBroker::start();

try {
    $record = static function (string $group, ?string $addr, bool $failRetrySend = false): SendBackInstance {
        $inst = new SendBackInstance('sbf-' . $group, $addr, MixAll::getRetryTopic($group));
        $inst->failRetrySend = $failRetrySend;
        return $inst;
    };

    // ---------------------------------------------------------------- 1. control
    {
        $group = 'GID_SBF_Control';
        $inst = $record($group, $broker->addr);
        $c = sbfPush($group, $inst);
        $c->sendMessageBack(sbfMessage('AcceptTopic'), 3);
        $bounces = sbfBounces();
        $ckSame(1, count($bounces), 'push.accept.bounceCount');
        $ckSame(RequestCode::CONSUMER_SEND_MSG_BACK, $bounces[0]['code'] ?? null, 'push.accept.requestCode');
        $ckSame($group, $bounces[0]['group'] ?? null, 'push.accept.group');
        $ckSame('AcceptTopic', $bounces[0]['originTopic'] ?? null, 'push.accept.originTopic');
        $ckSame(SBF_MSG_ID, $bounces[0]['originMsgId'] ?? null, 'push.accept.originMsgId');
        $ckSame(123456, $bounces[0]['offset'] ?? null, 'push.accept.commitLogOffset');
        $ckSame(3, $bounces[0]['delayLevel'] ?? null, 'push.accept.delayLevel');
        $ckSame(16, $bounces[0]['maxReconsumeTimes'] ?? null, 'push.accept.maxReconsumeTimes(-1->16)');
        $ckSame(0, count($inst->retrySends), 'push.accept.noFallbackSend');
    }

    // ---------------------------------------------------------------- 2. push rejected
    {
        $group = 'GID_SBF_PushReject';
        $inst = $record($group, $broker->addr);
        $c = sbfPush($group, $inst);
        $src = sbfMessage('RejectTopic');
        $c->sendMessageBack($src, 3);
        $ckSame(1, count($inst->retrySends), 'push.reject.fallbackSendCount');
        if (count($inst->retrySends) === 1) {
            sbfAssertRetry($ckSame, $inst->retrySends[0], $group, 16);
            $ckSame('RejectTopic', (string) $inst->retrySends[0]->getProperty(MessageConst::PROPERTY_RETRY_TOPIC), 'push.reject.retryTopic');
        }
        $ckSame('true', (string) $src->getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED), 'push.reject.sourceMarkerKept');
        sbfBounces();
    }

    // ---------------------------------------------------------------- 3. push transport error
    {
        $group = 'GID_SBF_PushDeadPort';
        $inst = $record($group, '127.0.0.1:1');
        $c = sbfPush($group, $inst);
        $c->sendMessageBack(sbfMessage('DeadPortTopic'), 0);
        $ckSame(1, count($inst->retrySends), 'push.transport.fallbackSendCount');
        if (count($inst->retrySends) === 1) {
            sbfAssertRetry($ckSame, $inst->retrySends[0], $group, 16);
        }
        sbfBounces();
    }

    // ---------------------------------------------------------------- 4. push missing master
    {
        $group = 'GID_SBF_PushNoMaster';
        $inst = $record($group, null);
        $c = sbfPush($group, $inst);
        $c->sendMessageBack(sbfMessage('NoMasterTopic'), 0);
        $ckSame(0, count(sbfBounces()), 'push.missingMaster.noBounceAttempted');
        $ckSame(1, count($inst->retrySends), 'push.missingMaster.fallbackSendCount');
        if (count($inst->retrySends) === 1) {
            sbfAssertRetry($ckSame, $inst->retrySends[0], $group, 16);
        }
    }

    // ---------------------------------------------------------------- 5. push knob
    {
        $group = 'GID_SBF_PushKnob';
        $inst = $record($group, $broker->addr);
        $c = sbfPush($group, $inst);
        $c->setMaxReconsumeTimes(5);
        $c->sendMessageBack(sbfMessage('RejectTopic'), 0);
        $bounces = sbfBounces();
        $ckSame(5, $bounces[0]['maxReconsumeTimes'] ?? null, 'push.knob.bounceReportsKnob');
        $ckSame(1, count($inst->retrySends), 'push.knob.fallbackSendCount');
        if (count($inst->retrySends) === 1) {
            sbfAssertRetry($ckSame, $inst->retrySends[0], $group, 5);
        }
    }

    // ---------------------------------------------------------------- 6. push double failure
    {
        $group = 'GID_SBF_PushDoubleFail';
        $inst = $record($group, $broker->addr, true);
        $c = sbfPush($group, $inst);
        $caught = null;
        try {
            $c->sendMessageBack(sbfMessage('RejectTopic'), 0);
        } catch (Throwable $e) {
            $caught = $e;
        }
        $ck($caught instanceof MQBrokerException, 'push.doubleFailure.rejectsWithOriginalBrokerError');
        $ckSame(ResponseCode::SYSTEM_ERROR, $caught?->responseCode, 'push.doubleFailure.originalCodeSurvives');
        $ck(str_contains($caught?->getMessage() ?? '', 'inner producer send failed') === false, 'push.doubleFailure.notTheFallbackError');
        sbfBounces();
    }

    // ---------------------------------------------------------------- 7. orderly path
    {
        $group = 'GID_SBF_Orderly';
        $inst = $record($group, $broker->addr);
        $c = sbfPush($group, $inst);
        $sendBack = new ReflectionMethod(DefaultMQPushConsumer::class, 'orderlySendMessageBack');
        $sendBack->setAccessible(true);
        $src = sbfMessage('OrderlyTopic');
        $ckSame(true, $sendBack->invoke($c, $src), 'orderly.confirmedSendReportsTrue');
        $ckSame(1, count($inst->retrySends), 'orderly.fallbackSendCount');
        if (count($inst->retrySends) === 1) {
            sbfAssertRetry($ckSame, $inst->retrySends[0], $group, 2147483647);
        }
        $ckSame('true', (string) $src->getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED), 'orderly.sourceMarkerKept');

        $failing = $record($group . '_Fail', $broker->addr, true);
        $cf = sbfPush($group . '_Fail', $failing);
        $ckSame(false, $sendBack->invoke($cf, sbfMessage('OrderlyTopic')), 'orderly.failingSendReportsFalse');
    }

    // ---------------------------------------------------------------- 8. pull rejected
    {
        $group = 'GID_SBF_PullReject';
        $inst = $record($group, $broker->addr);
        $c = sbfPull($group, $inst);
        $ckSame(16, $c->getMaxReconsumeTimes(), 'pull.defaultCeiling');
        $c->sendMessageBack(sbfMessage('RejectTopic'), 3);
        $bounces = sbfBounces();
        $ckSame(1, count($bounces), 'pull.reject.bounceCount');
        $ckSame(16, $bounces[0]['maxReconsumeTimes'] ?? null, 'pull.reject.bounceReports16');
        $ckSame(3, $bounces[0]['delayLevel'] ?? null, 'pull.reject.delayLevel');
        $ckSame(1, count($inst->retrySends), 'pull.reject.fallbackSendCount');
        if (count($inst->retrySends) === 1) {
            sbfAssertRetry($ckSame, $inst->retrySends[0], $group, 16);
        }
    }

    // ---------------------------------------------------------------- 9. pull knob
    {
        $group = 'GID_SBF_PullKnob';
        $inst = $record($group, $broker->addr);
        $c = sbfPull($group, $inst);
        $c->setMaxReconsumeTimes(5);
        $c->sendMessageBack(sbfMessage('RejectTopic'), 0);
        $bounces = sbfBounces();
        $ckSame(5, $bounces[0]['maxReconsumeTimes'] ?? null, 'pull.knob.fiveReportedOnWire');
        $ckSame(5, $c->getMaxReconsumeTimes(), 'pull.knob.getterMirrorsSetter');
        if (count($inst->retrySends) === 1) {
            sbfAssertRetry($ckSame, $inst->retrySends[0], $group, 5);
        }

        $group2 = 'GID_SBF_PullUnlimited';
        $inst2 = $record($group2, $broker->addr);
        $c2 = sbfPull($group2, $inst2);
        $c2->setMaxReconsumeTimes(-1);
        $c2->sendMessageBack(sbfMessage('RejectTopic'), 0);
        $bounces2 = sbfBounces();
        $ckSame(16, $bounces2[0]['maxReconsumeTimes'] ?? null, 'pull.knob.unlimitedReports16');
        if (count($inst2->retrySends) === 1) {
            sbfAssertRetry($ckSame, $inst2->retrySends[0], $group2, 16);
        }
    }

    // ---------------------------------------------------------------- 10. pull missing master
    {
        $group = 'GID_SBF_PullNoMaster';
        $inst = $record($group, null);
        $c = sbfPull($group, $inst);
        $c->sendMessageBack(sbfMessage('NoMasterTopic'), 0);
        $ckSame(0, count(sbfBounces()), 'pull.missingMaster.noBounceAttempted');
        $ckSame(1, count($inst->retrySends), 'pull.missingMaster.fallbackSendCount');
    }
} finally {
    $broker->stop();
    @unlink($logPath);
}

// ====================================================================== summary

if ($failures === []) {
    fwrite(STDOUT, "ALL TESTS PASSED ({$checks} checks)\n");
    exit(0);
}
fwrite(STDOUT, 'TESTS FAILED: ' . count($failures) . " / {$checks}\n");
foreach ($failures as $f) {
    fwrite(STDOUT, "  - {$f}\n");
}
exit(1);
