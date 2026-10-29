<?php

declare(strict_types=1);

/**
 * Consumer suite self test (DefaultMQPushConsumer / DefaultMQPullConsumer / DefaultLitePullConsumer
 * + ConsumerSupport common primitives). No phpunit, no real cluster.
 *
 * Run: php tests/RunClientConsumer.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Network cases follow the RunClientInstance pattern: this file re-spawns itself with the
 * `--broker` argument as a **child-process** fake broker. Unlike the instance suite, this
 * broker is **stateful**:
 *   - per-queue pull counters (first pull returns FOUND / OFFSET_ILLEGAL, later ones NO_NEW_MSG);
 *   - OTopic's QUERY_CONSUMER_OFFSET returns 77 after "an OFFSET_ILLEGAL happened";
 *   - writes PULL / LITE_PULL / HEART_BEAT / UPDATE_CONSUMER_OFFSET / CONSUMER_SEND_MSG_BACK /
 *     LOCK_BATCH_MQ requests line-by-line into the JSONL file named by RMQ_LOG; the parent process
 *     asserts wire behavior from it (sysFlag suspend bit, the subscription expression cells a
 *     selector puts on the pull request, the subscription set a heartbeat registers, send-back
 *     delayLevel, lock mqSet, commit offsets).
 * The consumer list (for rebalance) is read from the JSON file named by RMQ_CONSUMER_MAP: {group: [cid,...]}.
 */

require_once __DIR__ . '/../bootstrap.php';

// autoloader loads classes but not functions: this suite calls ConsumerSupport global
// functions directly, so make sure the file is loaded first.
if (!function_exists('RocketMQ\\Client\\wire_split')) {
    require_once __DIR__ . '/../src/Client/ConsumerSupport.php';
}

use RocketMQ\Client\ConsumerDefaults;
use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageSelector;
use RocketMQ\Client\PopProcessQueue;
use RocketMQ\Client\SimpleMessageListener;
use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\ConsumeOrderlyContext;
use RocketMQ\Client\ConsumeOrderlyStatus;
use RocketMQ\Client\FilterMessageContext;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MessageListenerOrderly;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\PullStatus;
use RocketMQ\Common\ExpressionType;
use RocketMQ\Common\FilterAPI;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PullSysFlag;
use RocketMQ\Common\SubscriptionData;
use RocketMQ\Common\UtilAll;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\PullMessageResponseHeader;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\TopicRouteData;
use RocketMQ\Common\PermName;
use function RocketMQ\Client\build_local_offsets_json;
use function RocketMQ\Client\build_pull_subscription_data;
use function RocketMQ\Client\build_selector_subscription_data;
use function RocketMQ\Client\client_side_tag_filter;
use function RocketMQ\Client\filter_messages_for_delivery;
use function RocketMQ\Client\parse_tag_subscription;
use function RocketMQ\Client\wire_queue_key_string;
use function RocketMQ\Client\wire_split;
use function RocketMQ\Client\parse_local_offsets_json;
use function RocketMQ\Client\scan_offset_table_body;
use function RocketMQ\Remoting\Protocol\message_queue_key;

// ======================================================================
// child-process fake broker: `php RunClientConsumer.php --broker`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    consBrokerMain();
    exit(0);
}

/** FOUND script per topic: body prefix / count / whether to switch to NO_NEW_MSG after the first round. */
function consPullPlan(string $topic, int $seq, string $reqCode): array
{
    // returns [responseCode, nextBeginOffset, list<array{body, offset}>]
    switch ($topic) {
        case 'CTopic':
        case 'RTopic':
        case 'PTopic':
        case 'STopic':
            $prefix = ['CTopic' => 'push', 'RTopic' => 'retry', 'PTopic' => 'part', 'STopic' => 'order'][$topic];
            if ($seq === 1) {
                return [ResponseCode::SUCCESS, 3, [
                    ['body' => "{$prefix}-0", 'offset' => 0],
                    ['body' => "{$prefix}-1", 'offset' => 1],
                    ['body' => "{$prefix}-2", 'offset' => 2],
                ]];
            }
            return [ResponseCode::PULL_NOT_FOUND, 3, []];
        case 'PTopic2': // reserved
            return [ResponseCode::PULL_NOT_FOUND, 0, []];
        case 'FTopic':
            if ($seq === 1) {
                return [ResponseCode::SUCCESS, 3, [
                    ['body' => 'f-0', 'offset' => 0],
                    ['body' => 'f-1', 'offset' => 1],
                    ['body' => 'f-2', 'offset' => 2],
                ]];
            }
            return [ResponseCode::PULL_NOT_FOUND, 3, []];
        case 'OTopic':
            if ($seq === 1) {
                $GLOBALS['CONS_OTOPIC_ILLEGAL'] = true;
                return [ResponseCode::PULL_OFFSET_MOVED, 77, []];
            }
            return [ResponseCode::PULL_NOT_FOUND, 77, []];
        case 'DTopic':
            return [ResponseCode::SUCCESS, 5, [['body' => 'direct-4', 'offset' => 4]]];

        // ---- selector-driven subscription scenarios ----------------------------------
        // Every topic below hands back the same two tagged messages on the first round, so
        // the only variable left is the subscription shape the client put on the wire.
        // TagA matches a "TagA || TagB" tag subscription, TagC matches no tag at all.
        case 'QTopic':   // pull consumer + SQL92 selector
        case 'QTopic2':  // pull consumer + bare tag expression (control of QTopic)
        case 'QTopic3':  // pull consumer + TAG selector (control of QTopic2)
        case 'QTopic4':  // pull consumer + CLASS_FILTER selector
        case 'LSqlTopic':    // lite subscribe mode + SQL92 selector
        case 'LTagTopic':    // lite subscribe mode + tag expression (control)
        case 'LAssignTopic': // lite assign mode + SQL92 selector
        case 'LClassTopic':  // lite assign mode + CLASS_FILTER selector
        case 'LRawTopic':    // lite assign mode + no subscription at all (parsed on the spot)
            if ($seq === 1) {
                return [ResponseCode::SUCCESS, 2, [
                    ['body' => 'sel-tag-a', 'offset' => 0, 'tag' => 'TagA'],
                    ['body' => 'sel-tag-c', 'offset' => 1, 'tag' => 'TagC'],
                ]];
            }
            return [ResponseCode::PULL_NOT_FOUND, 2, []];

        // ---- shutdown drain scenarios ------------------------------------------------
        // KTopic/KTopic2/KTopic3: one backlog of 21 messages each (bigger than a drain chunk,
        // small enough for the no-ceiling walk), one topic per consumer so each scenario owns
        // its own pull sequence. JTopic: FOUND on every round, so the caller can stack a
        // backlog past the drain round cap.
        case 'KTopic':
        case 'KTopic2':
        case 'KTopic3':
            if ($seq === 1) {
                $msgs = [];
                for ($i = 0; $i < 21; $i++) {
                    $msgs[] = ['body' => "drain-$i", 'offset' => $i];
                }
                return [ResponseCode::SUCCESS, 21, $msgs];
            }
            return [ResponseCode::PULL_NOT_FOUND, 21, []];
        case 'JTopic':
            $msgs = [];
            for ($i = 0; $i < 32; $i++) {
                $msgs[] = ['body' => "cap-$i", 'offset' => $i];
            }
            return [ResponseCode::SUCCESS, 32, $msgs];

        case 'LTopic':
            if ($reqCode != RequestCode::LITE_PULL_MESSAGE) {
                return [ResponseCode::PULL_NOT_FOUND, 0, []];
            }
            if ($seq === 1) {
                return [ResponseCode::SUCCESS, 2, [
                    ['body' => 'lite-0', 'offset' => 0],
                    ['body' => 'lite-1', 'offset' => 1],
                ]];
            }
            return [ResponseCode::PULL_NOT_FOUND, 2, []];
        default:
            return [ResponseCode::PULL_NOT_FOUND, 0, []];
    }
}

function consLog(array $entry): void
{
    $path = getenv('RMQ_LOG');
    if ($path === false || $path === '') {
        return;
    }
    @file_put_contents($path, json_encode($entry, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
}

/** Reads exactly n bytes; returns null on EOF. */
function consReadExact($conn, int $n): ?string
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
function consReadFrame($conn): ?RemotingCommand
{
    $head = consReadExact($conn, 4);
    if ($head === null) {
        return null;
    }
    $total = RocketMQSerializable::unpackSignedInt($head);
    if ($total <= 0) {
        return null;
    }
    $rest = consReadExact($conn, $total);
    if ($rest === null) {
        return null;
    }
    return RemotingCommand::decode($head . $rest);
}

/** Builds a route body: single broker with one read/write queue, all pointing at the fake broker itself. */
function consRouteBody(string $addr): string
{
    $qd = new QueueData('b-cons', 1, 1, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', 'b-cons', [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

/** Encodes one message in the 17-segment format and concatenates it into the PULL/LITE_PULL response body. */
function consEncodeMsg(string $topic, string $body, int $queueOffset, ?string $tag = null): string
{
    $m = new MessageExt(topic: $topic, body: $body);
    $m->setQueueId(0);
    $m->setQueueOffset($queueOffset);
    $m->setBornTimestamp(1_700_000_000_000);
    $m->setStoreTimestamp(1_700_000_000_000);
    if ($tag !== null) {
        $m->setTags($tag);
    }
    return MessageDecoder::encodeMessageExt($m);
}

function consRespond(RemotingCommand $cmd): ?RemotingCommand
{
    $ext = $cmd->extFields;
    $topic = (string) ($ext['topic'] ?? '');

    switch ($cmd->code) {
        case RequestCode::GET_ROUTEINFO_BY_TOPIC:
            if (str_starts_with($topic, '%RETRY%')) {
                return RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST);
            }
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = consRouteBody($GLOBALS['CONS_BROKER_ADDR']);
            return $resp;

        case RequestCode::GET_CONSUMER_LIST_BY_GROUP: {
            $mapPath = getenv('RMQ_CONSUMER_MAP');
            $map = is_string($mapPath) && is_file($mapPath)
                ? (array) json_decode((string) file_get_contents($mapPath), true) : [];
            $cids = $map[(string) ($ext['consumerGroup'] ?? '')] ?? [];
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = json_encode(['consumerIdList' => array_values($cids)]);
            return $resp;
        }

        case RequestCode::QUERY_CONSUMER_OFFSET: {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            if ($topic === 'OTopic') {
                $off = !empty($GLOBALS['CONS_OTOPIC_ILLEGAL']) ? 77 : 0;
            } else {
                $off = match ($topic) {
                    'CTopic', 'RTopic', 'PTopic', 'STopic', 'LTopic', 'FTopic' => 0,
                    'DTopic' => 7,
                    // Selector scenarios and drain scenarios start every queue from 0, so the
                    // recorded pull always begins at the scripted batch.
                    'QTopic', 'QTopic2', 'QTopic3', 'QTopic4',
                    'LSqlTopic', 'LTagTopic', 'LAssignTopic', 'LClassTopic', 'LRawTopic',
                    'KTopic', 'KTopic2', 'KTopic3', 'JTopic' => 0,
                    default => null,
                };
            }
            if ($off === null) {
                return RemotingCommand::createResponseCommand(ResponseCode::QUERY_NOT_FOUND);
            }
            $resp->extFields['offset'] = (string) $off;
            return $resp;
        }

        case RequestCode::GET_MAX_OFFSET:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '100';
            return $resp;

        case RequestCode::GET_MIN_OFFSET:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '0';
            return $resp;

        case RequestCode::SEARCH_OFFSET_BY_TIMESTAMP:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '55';
            return $resp;

        case RequestCode::GET_EARLIEST_MSG_STORETIME:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['timestamp'] = '1234567890000';
            return $resp;

        case RequestCode::PULL_MESSAGE:
        case RequestCode::LITE_PULL_MESSAGE: {
            $seq = ($GLOBALS['CONS_PULL_SEEN'][$topic] = ($GLOBALS['CONS_PULL_SEEN'][$topic] ?? 0) + 1);
            consLog([
                'code' => $cmd->code,
                'topic' => $topic,
                'queueId' => (int) ($ext['queueId'] ?? 0),
                'queueOffset' => (int) ($ext['queueOffset'] ?? 0),
                'sysFlag' => (int) ($ext['sysFlag'] ?? 0),
                'group' => (string) ($ext['consumerGroup'] ?? ''),
                // The subscription the caller reported: expression text, expression type and the
                // version stamp are the three cells a selector-driven pull is judged on.
                'subscription' => $ext['subscription'] ?? null,
                'expressionType' => $ext['expressionType'] ?? null,
                'subVersion' => isset($ext['subVersion']) ? (int) $ext['subVersion'] : null,
                'seq' => $seq,
            ]);
            [$code, $next, $msgs] = consPullPlan($topic, $seq, (string) $cmd->code);
            $h = new PullMessageResponseHeader(
                nextBeginOffset: $next,
                minOffset: 0,
                maxOffset: 100,
                suggestWhichBrokerId: MixAll::MASTER_ID,
            );
            $resp = RemotingCommand::createResponseCommandWithHeader($code, $h);
            if ($msgs !== []) {
                $body = '';
                foreach ($msgs as $m) {
                    $body .= consEncodeMsg($topic, $m['body'], $m['offset'], $m['tag'] ?? null);
                }
                $resp->body = $body;
            }
            return $resp;
        }

        case RequestCode::HEART_BEAT: {
            // Registration body decoded off the wire: one log line per consumer group, carrying
            // exactly the subscription set the broker would install.
            $hb = ($cmd->body ?? '') !== '' ? HeartbeatData::decode($cmd->body) : null;
            $consumers = [];
            foreach ($hb?->consumerDataSet ?? [] as $cd) {
                $subs = [];
                foreach ($cd->subscriptionDataSet as $sd) {
                    $subs[] = [
                        'topic' => (string) $sd->topic,
                        'subString' => (string) ($sd->subString ?? ''),
                        'expressionType' => $sd->expressionType,
                        'classFilterMode' => $sd->classFilterMode,
                        'tagsSet' => $sd->tagsSet,
                        'codeSet' => $sd->codeSet,
                        'subVersion' => $sd->subVersion,
                    ];
                }
                $consumers[] = ['group' => $cd->groupName, 'subs' => $subs];
            }
            consLog([
                'code' => $cmd->code,
                'clientId' => $hb?->clientId ?? '',
                'consumers' => $consumers,
            ]);
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        }

        case RequestCode::UPDATE_CONSUMER_OFFSET:
            consLog([
                'code' => $cmd->code,
                'topic' => $topic,
                'queueId' => (int) ($ext['queueId'] ?? 0),
                'commitOffset' => (int) ($ext['commitOffset'] ?? -1),
                'group' => (string) ($ext['consumerGroup'] ?? ''),
            ]);
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);

        case RequestCode::CONSUMER_SEND_MSG_BACK:
            consLog([
                'code' => $cmd->code,
                'group' => (string) ($ext['group'] ?? ''),
                'originTopic' => (string) ($ext['originTopic'] ?? ''),
                'originMsgId' => (string) ($ext['originMsgId'] ?? ''),
                'delayLevel' => (int) ($ext['delayLevel'] ?? 0),
                'maxReconsumeTimes' => isset($ext['maxReconsumeTimes']) ? (int) $ext['maxReconsumeTimes'] : null,
            ]);
            // FTopic send-backs always fail: verifies the C4 floor (failed send-back entries stall offset commit).
            if (($ext['originTopic'] ?? '') === 'FTopic') {
                return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'send back rejected');
            }
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);

        case RequestCode::SEND_MESSAGE:
        case RequestCode::SEND_MESSAGE_V2: {
            // The send-back fallback goes out through the client-inner producer. Keep that leg
            // failing too, so the rejected FTopic send-backs never turn into a successful retry.
            // V2 carries the same fields under compact single-letter keys (a=producerGroup).
            $producerGroup = (string) ($ext['producerGroup'] ?? $ext['a'] ?? '');
            if ($producerGroup === MixAll::CLIENT_INNER_PRODUCER_GROUP) {
                return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'inner producer unavailable');
            }
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        }

        case RequestCode::LOCK_BATCH_MQ:
        case RequestCode::UNLOCK_BATCH_MQ: {
            $reqBody = (array) (json_decode($cmd->body ?? '{}', true) ?: []);
            $mqSet = array_values((array) ($reqBody['mqSet'] ?? []));
            consLog([
                'code' => $cmd->code,
                'group' => (string) ($reqBody['consumerGroup'] ?? ''),
                'clientId' => (string) ($reqBody['clientId'] ?? ''),
                'mqSet' => $mqSet,
            ]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            if ($cmd->code === RequestCode::LOCK_BATCH_MQ) {
                // All locks succeed: echo the request's mqSet back as lockOKMQSet.
                $resp->body = json_encode(['lockOKMQSet' => $mqSet]);
            }
            return $resp;
        }

        default:
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

function consServeConn($conn): void
{
    stream_set_blocking($conn, true);
    while (($cmd = consReadFrame($conn)) !== null) {
        try {
            $resp = consRespond($cmd);
        } catch (\Throwable $e) {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'broker error: ' . $e->getMessage());
        }
        if ($resp !== null) {
            $resp->opaque = $cmd->opaque;
            @fwrite($conn, $resp->encode());
        }
    }
}

function consBrokerMain(): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "broker: cannot listen: {$errstr}\n");
        return;
    }
    $GLOBALS['CONS_BROKER_ADDR'] = (string) stream_socket_get_name($server, false);
    $GLOBALS['CONS_PULL_SEEN'] = [];
    $GLOBALS['CONS_OTOPIC_ILLEGAL'] = false;
    fwrite(STDOUT, $GLOBALS['CONS_BROKER_ADDR'] . "\n");
    fflush(STDOUT);

    while (true) {
        $conn = @stream_socket_accept($server, 30);
        if ($conn === false) {
            break;
        }
        consServeConn($conn);
        @fclose($conn);
    }
    @fclose($server);
}

/**
 * Child-process fake broker handle (same shape as RunClientInstance; declared separately to avoid cross-file exit pollution).
 */
final class ConsFakeBroker
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
// test listeners
// ======================================================================

/**
 * Concurrent listener: routes by the first message's topic (CTopic accepts / RTopic re-consumes / PTopic acks only the 1st).
 */
final class ConsTopicListener implements MessageListenerConcurrently
{
    /** @var array<string,int> */
    private array $calls = [];

    /** @param array<string,mixed> $behavior topic => ['status'=>..., 'ackIndex'=>int|null, 'ackIndexOnlyFirst'=>bool] */
    public function __construct(private array $behavior)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        $topic = (string) ($msgs[0]->topic ?? '');
        foreach ($msgs as $m) {
            $GLOBALS['RECV'][$topic][] = (string) $m->getBody();
        }
        $this->calls[$topic] = ($this->calls[$topic] ?? 0) + 1;
        $b = $this->behavior[$topic] ?? ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => null];
        if ($b['ackIndex'] !== null) {
            $context->ackIndex = (int) $b['ackIndex'];
        }
        if (!empty($b['ackIndexOnlyFirst']) && $this->calls[$topic] === 1) {
            // First round acks only the 1st message (triggers partial-ack send-back); the
            // re-ordered re-consume acks the whole batch, ending the loop.
            $context->ackIndex = 0;
        }
        return $b['status'];
    }
}

/** Orderly listener: suspends the first time, succeeds the second (skeleton for the re-consume scenario). */
final class ConsOrderlyListener implements MessageListenerOrderly
{
    public int $calls = 0;

    public function consumeMessage(array $msgs, ConsumeOrderlyContext $context): ConsumeOrderlyStatus
    {
        $this->calls++;
        foreach ($msgs as $m) {
            $GLOBALS['RECV']['STopic'][] = (string) $m->getBody();
        }
        return $this->calls === 1
            ? ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT
            : ConsumeOrderlyStatus::SUCCESS;
    }
}

/**
 * Shutdown-drain listener: counts every batch (and message) it is handed. When built with a work
 * size it hashes that many bytes per batch — a fixed, deterministic amount of CPU per batch, so a
 * millisecond ceiling provably binds in the middle of the first drain chunk. Nothing here sleeps
 * or waits: the drain either hands a batch over or it does not, and that is what is asserted.
 */
final class ConsDrainListener implements MessageListenerConcurrently
{
    public int $batches = 0;

    /** @var list<string> */
    public array $bodies = [];

    private string $pad;

    public function __construct(private int $workBytes = 0)
    {
        $this->pad = $workBytes > 0 ? str_repeat('d', $workBytes) : '';
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        $this->batches++;
        foreach ($msgs as $m) {
            $this->bodies[] = (string) $m->getBody();
        }
        if ($this->workBytes > 0) {
            md5($this->pad);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    public function messageCount(): int
    {
        return count($this->bodies);
    }
}

// ======================================================================
// test runner
// ======================================================================

final class RunClientConsumer
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    /** @var list<array<string,mixed>> */
    private array $log = [];

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

    public function summary(): int
    {
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }

    // ---------------- log helpers ----------------

    private function loadLog(string $path): void
    {
        $this->log = [];
        foreach (file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
            $d = json_decode($line, true);
            if (is_array($d)) {
                $this->log[] = $d;
            }
        }
    }

    /** @return list<array<string,mixed>> */
    private function logWhere(int $code, ?string $topic = null, ?string $group = null): array
    {
        $out = [];
        foreach ($this->log as $e) {
            if (($e['code'] ?? 0) !== $code) {
                continue;
            }
            if ($topic !== null && ($e['topic'] ?? null) !== $topic) {
                continue;
            }
            if ($group !== null && ($e['group'] ?? null) !== $group) {
                continue;
            }
            $out[] = $e;
        }
        return $out;
    }

    private function cleanup(DefaultMQPushConsumer|DefaultMQPullConsumer|DefaultLitePullConsumer $c): void
    {
        $c->shutdown();
        if ($c->clientId !== null) {
            MQClientInstance::removeInstance($c->clientId);
        }
    }

    /** Last recorded pull request of one topic + group (the cell set a subscription is judged on). */
    private function lastPullRow(int $code, string $topic, string $group): array
    {
        $rows = $this->logWhere($code, $topic, $group);
        $this->check($rows !== [], "pull of $topic by $group reached the broker");
        return $rows[count($rows) - 1] ?? [];
    }

    /** @return array{subscription:mixed,expressionType:mixed,subVersion:mixed,sysFlag:mixed} */
    private function subscriptionCells(array $row): array
    {
        return [
            'subscription' => $row['subscription'] ?? null,
            'expressionType' => $row['expressionType'] ?? null,
            'subVersion' => $row['subVersion'] ?? null,
            'sysFlag' => $row['sysFlag'] ?? null,
        ];
    }

    /** Whether any heartbeat mentioning the group reached the broker at all. */
    private function heartbeatSeen(string $group): bool
    {
        foreach ($this->log as $e) {
            if (($e['code'] ?? 0) !== RequestCode::HEART_BEAT) {
                continue;
            }
            foreach (($e['consumers'] ?? []) as $cd) {
                if (($cd['group'] ?? '') === $group) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The subscription set a heartbeat registered for one group: topic => the cells the broker
     * installs from. Reads the last beat that mentions the group.
     *
     * @return array<string,array<string,mixed>>
     */
    private function heartbeatSubs(string $group, bool $requireRegistered = true): array
    {
        $found = [];
        foreach ($this->log as $e) {
            if (($e['code'] ?? 0) !== RequestCode::HEART_BEAT) {
                continue;
            }
            foreach (($e['consumers'] ?? []) as $cd) {
                if (($cd['group'] ?? '') !== $group) {
                    continue;
                }
                $found = [];
                foreach (($cd['subs'] ?? []) as $sd) {
                    $found[(string) $sd['topic']] = $sd;
                }
            }
        }
        if ($requireRegistered) {
            $this->check($found !== [], "heartbeat of $group registered a subscription set");
        }
        return $found;
    }

    /** Read a private member of the push facade (drain bookkeeping is not part of the public API). */
    private function pushPrivate(DefaultMQPushConsumer $c, string $name): mixed
    {
        $p = new \ReflectionProperty(DefaultMQPushConsumer::class, $name);
        $p->setAccessible(true);
        return $p->getValue($c);
    }

    private function pushPrivateMethod(DefaultMQPushConsumer $c, string $name, array $args = []): mixed
    {
        $m = new \ReflectionMethod(DefaultMQPushConsumer::class, $name);
        $m->setAccessible(true);
        return $m->invokeArgs($c, $args);
    }

    private function pushPrivateConstant(string $name): int
    {
        $value = (new \ReflectionClass(DefaultMQPushConsumer::class))->getConstant($name);
        $this->check(is_int($value), "drain constant $name is an int");
        return (int) $value;
    }

    // ================================================================ 1b. Selector-driven subscription building (pure)

    private function testSelectorSubscriptionUnit(): void
    {
        $mq = new MessageQueue('QTopic', 'b-cons', 0);

        // No selector: the expression is a tag list and FilterAPI compiles it.
        $tag = build_pull_subscription_data($mq, 'TagA || TagB');
        $this->checkSame('QTopic', $tag->topic, 'pull subscription takes the queue topic');
        $this->checkSame('TagA || TagB', $tag->subString, 'bare expression keeps its text');
        $this->checkSame(ExpressionType::TAG, $tag->expressionType, 'bare expression is a tag subscription');
        $this->checkSame(['TagA', 'TagB'], $tag->tagsSet, 'bare expression compiles the tag set');
        $this->checkSame([2598919, 2598920], $tag->codeSet, 'bare expression compiles the tag hash set');
        $this->checkSame(false, $tag->classFilterMode, 'tag subscription is not class-filter mode');
        // Nothing at all means SUB_ALL: an empty tag set, which is what disables client-side filtering.
        $all = build_pull_subscription_data($mq, null);
        $this->checkSame('*', $all->subString, 'no expression falls back to SUB_ALL');
        $this->checkSame([], $all->tagsSet, 'SUB_ALL keeps the tag set empty');
        $this->checkSame([], $all->codeSet, 'SUB_ALL keeps the hash set empty');

        // TAG selector (and the empty type, which reads as TAG): the same tag derivation.
        $byTag = build_selector_subscription_data('QTopic', MessageSelector::byTag('TagA || TagB'));
        $this->checkSame($tag->tagsSet, $byTag->tagsSet, 'tag selector compiles the same tag set as the bare expression');
        $this->checkSame($tag->codeSet, $byTag->codeSet, 'tag selector compiles the same hash set as the bare expression');
        $this->checkSame(ExpressionType::TAG, $byTag->expressionType, 'tag selector reports TAG');
        $blankType = build_selector_subscription_data('QTopic', new MessageSelector('', 'TagA || TagB'));
        $this->checkSame(ExpressionType::TAG, $blankType->expressionType, 'empty selector type reads as TAG');
        $this->checkSame(['TagA', 'TagB'], $blankType->tagsSet, 'empty selector type keeps the tag derivation');

        // Non-tag selector: the raw expression is kept as it stands, no tag compilation.
        $sql = build_selector_subscription_data('QTopic', MessageSelector::bySql('a > 10'));
        $this->checkSame('SQL92', $sql->expressionType, 'SQL92 selector reports SQL92');
        $this->checkSame('a > 10', $sql->subString, 'SQL92 expression is not parsed as a tag list');
        $this->checkSame([], $sql->tagsSet, 'SQL92 subscription leaves tagsSet empty');
        $this->checkSame([], $sql->codeSet, 'SQL92 subscription leaves codeSet empty');
        $this->checkSame(false, $sql->classFilterMode, 'SQL92 subscription is not class-filter mode');
        $this->checkSame('QTopic', $sql->topic, 'SQL92 subscription keeps the topic');

        $cls = build_selector_subscription_data('QTopic', new MessageSelector(ExpressionType::CLASS_FILTER, 'MyClassFilter'));
        $this->checkSame(ExpressionType::CLASS_FILTER, $cls->expressionType, 'class-filter selector keeps its own type');
        $this->checkSame('MyClassFilter', $cls->subString, 'class-filter expression is kept verbatim');
        $this->checkSame(true, $cls->classFilterMode, 'class-filter mode is set only for CLASS_FILTER');
        $this->checkSame([], $cls->tagsSet, 'class-filter subscription leaves tagsSet empty');
        $this->checkSame([], $cls->codeSet, 'class-filter subscription leaves codeSet empty');

        // A non-tag selector without an expression is refused outright.
        $threw = null;
        try {
            build_selector_subscription_data('QTopic', new MessageSelector(ExpressionType::SQL92, ''));
        } catch (\ValueError $e) {
            $threw = $e->getMessage();
        }
        $this->checkSame("Expression can't be null! SQL92", $threw, 'empty SQL92 expression throws ValueError');
        $threw = null;
        try {
            build_pull_subscription_data($mq, null, new MessageSelector(ExpressionType::CLASS_FILTER, ''));
        } catch (\ValueError $e) {
            $threw = $e->getMessage();
        }
        $this->checkSame("Expression can't be null! CLASS_FILTER", $threw, 'empty class-filter expression throws ValueError');

        // Unusable queue, and a tag expression FilterAPI rejects, both surface as client exceptions.
        foreach ([null, new MessageQueue('', 'b-cons', 0)] as $i => $bad) {
            $message = null;
            try {
                build_pull_subscription_data($bad, 'TagA');
            } catch (MQClientException $e) {
                $message = $e->getMessage();
            }
            $this->checkSame('mq is null', $message, "unusable queue #$i reports 'mq is null'");
        }
        $message = null;
        try {
            build_pull_subscription_data($mq, '||');
        } catch (MQClientException $e) {
            $message = $e->getMessage();
        }
        $this->checkSame('parse subscription error: subString split error', $message,
            'a tag expression FilterAPI rejects is rethrown as a subscription error');
        $message = null;
        try {
            build_pull_subscription_data($mq, null, MessageSelector::byTag('||'));
        } catch (MQClientException $e) {
            $message = $e->getMessage();
        }
        $this->checkSame('parse subscription error: subString split error', $message,
            'a TAG selector holding a bad expression reports the same subscription error');

        // parse_tag_subscription is the single place that rule lives.
        $this->checkSame(['TagA'], build_pull_subscription_data($mq, 'TagA')->tagsSet, 'single tag expression compiles');
        $this->checkSame(FilterAPI::buildSubscriptionData('QTopic', 'TagA')->tagsSet,
            parse_tag_subscription('QTopic', 'TagA')->tagsSet,
            'parse_tag_subscription matches the FilterAPI build');
        $this->checkSame('*', parse_tag_subscription('QTopic', null)->subString,
            'parse_tag_subscription reads a missing expression as SUB_ALL');

        // Client-side filtering is a tag-only affair: an empty tagsSet means the broker decided.
        $mTagA = new MessageExt(topic: 'QTopic', body: 'sel-tag-a');
        $mTagA->setTags('TagA');
        $mTagC = new MessageExt(topic: 'QTopic', body: 'sel-tag-c');
        $mTagC->setTags('TagC');
        $this->checkSame([$mTagA], client_side_tag_filter($tag, [$mTagA, $mTagC]),
            'tag subscription re-filters the batch by tag string');
        $this->checkSame([$mTagA, $mTagC], client_side_tag_filter($sql, [$mTagA, $mTagC]),
            'SQL92 subscription is never re-filtered client-side');
        $this->checkSame([$mTagA, $mTagC], client_side_tag_filter($cls, [$mTagA, $mTagC]),
            'class-filter subscription is never re-filtered client-side');
    }

    // ================================================================ 1. ConsumerSupport pure logic

    private function testConsumerSupportUnit(): void
    {
        // wire_split: splits by the protocol wire-field separator, dropping trailing empty segments.
        $this->checkSame(['room1'], wire_split('room1@', '@'), 'wire_split drops trailing empty segment');
        $this->checkSame(['room1', 'b'], wire_split('room1@b@', '@'), 'wire_split keeps interior segments, drops trailing');
        $this->checkSame([], wire_split('@', '@'), 'wireSplit all separators -> empty');
        $this->checkSame([''], wire_split('', '@'), 'wire_split no hit returns as-is (including empty string)');
        $this->checkSame(['abc'], wire_split('abc', '@'), 'wire_split no hit returns as-is');

        // wire_queue_key_string: input string for consistent hashing, format fixed as
        // "MessageQueue [topic=..., brokerName=..., queueId=...]", must not deform character-by-character.
        $mq = new MessageQueue('T', 'b', 3);
        $this->checkSame('MessageQueue [topic=T, brokerName=b, queueId=3]', wire_queue_key_string($mq),
            'wire_queue_key_string format');

        // local offsets JSON: strict flat-map roundtrip.
        $items = ['Tb1' => 42, 'Tb2' => 7];
        $mqMap = ['Tb1' => new MessageQueue('T', 'b', 1), 'Tb2' => new MessageQueue('T', 'b', 2)];
        $json = build_local_offsets_json($items, $mqMap);
        $this->checkSame($items, parse_local_offsets_json($json), 'local offsets JSON roundtrip');
        // non-standard offsetTable JSON with "object as key" (read back by protocol field names brokerName/queueId/topic).
        $javaStyle = '{"offsetTable":{{"brokerName":"b","queueId":1,"topic":"T"}:42,'
            . '{"brokerName":"b","queueId":2,"topic":"T"}:7}}';
        $this->checkSame($items, parse_local_offsets_json($javaStyle), 'fastjson2 object-key format parsing');
        $pretty = "{\n  \"offsetTable\" : {\n    {\"brokerName\":\"b\",\"queueId\":1,\"topic\":\"T\"} : 42\n  }\n}";
        $this->checkSame(['Tb1' => 42], parse_local_offsets_json($pretty), 'fastjson2 object-key pretty tolerance');
        $this->checkSame(null, parse_local_offsets_json('not json'), 'bad input returns null');
        $this->checkSame(null, parse_local_offsets_json(''), 'empty string returns null');
        $this->checkSame(null, scan_offset_table_body('{"offsetTable":garbage}'), 'corrupt offsetTable body returns null');

        // client-side second tag filtering (subscription table goes through the real FilterAPI build entry).
        $sub = \RocketMQ\Common\FilterAPI::buildSubscriptionData('T', 'TagA || TagB');
        $mTagA = new MessageExt(topic: 'T', body: 'a');
        $mTagA->setTags('TagA');
        $mTagC = new MessageExt(topic: 'T', body: 'c');
        $mTagC->setTags('TagC');
        $kept = client_side_tag_filter($sub, [$mTagA, $mTagC]);
        $this->checkSame([$mTagA], $kept, 'tag filter keeps hits');
        $this->checkSame([$mTagA, $mTagC], client_side_tag_filter(null, [$mTagA, $mTagC]), 'no subscription does not filter');
        // SUB_ALL: tagsSet must be empty -> no filtering.
        $subAll = new SubscriptionData('T', '*');
        $this->checkSame([$mTagA, $mTagC], client_side_tag_filter($subAll, [$mTagA, $mTagC]), 'SUB_ALL empty tagsSet passes through');

        // pre-delivery filter hooks: hooks may prune messages; exceptions are always swallowed.
        $mDrop = new MessageExt(topic: 'T', body: 'drop');
        $dropper = new class {
            public function filterMessage(FilterMessageContext $ctx): void
            {
                $ctx->msgList = array_values(array_filter(
                    $ctx->msgList,
                    static fn(MessageExt $m): bool => $m->getBody() !== 'drop',
                ));
            }
        };
        $bomber = new class {
            public function filterMessage(FilterMessageContext $ctx): void
            {
                throw new \RuntimeException('hook boom');
            }
        };
        $out = filter_messages_for_delivery('G', [$dropper, $bomber], $mq, null, [$mTagA, $mDrop]);
        $this->checkSame(['a'], array_map(static fn(MessageExt $m) => $m->getBody(), $out),
            'hook prunes the drop message and exceptions are swallowed');

        // PopProcessQueue: waitAck counting and the dropped flag.
        $pq = new PopProcessQueue();
        $pq->incFoundMsg(3);
        $this->checkSame(3, $pq->waitAckCount(), 'PopProcessQueue incFoundMsg');
        $this->checkSame(2, $pq->ack(), 'PopProcessQueue ack decrements by one');
        $pq->decFoundMsg(-1);
        $this->checkSame(1, $pq->waitAckCount(), 'PopProcessQueue decFoundMsg (protocol semantics pass negative to decrement)');
        $this->check(!$pq->isDropped(), 'PopProcessQueue not dropped by default');
        $pq->setDropped(true);
        $this->check($pq->isDropped(), 'PopProcessQueue setDropped');

        // consumer-side constant values: POP delay tiers and invisible-time bounds.
        $this->checkSame([10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200],
            ConsumerDefaults::POP_DELAY_LEVEL, 'POP_DELAY_LEVEL 16 tiers');
        $this->checkSame(5000, ConsumerDefaults::MIN_POP_INVISIBLE_TIME, 'MIN_POP_INVISIBLE_TIME');
        $this->checkSame(300000, ConsumerDefaults::MAX_POP_INVISIBLE_TIME, 'MAX_POP_INVISIBLE_TIME');

        // 221's topic argument: only a non-empty topic filters. The asking side
        // leaves the field empty to mean "everything this client has consumed",
        // so an empty string must not be read as a topic literally named empty.
        $statusC = new DefaultMQPushConsumer('GID_status_unit');
        $q1 = new MessageQueue('StatusTopic', 'b-cons', 0);
        $q2 = new MessageQueue('OtherTopic', 'b-cons', 1);
        $k1 = 'StatusTopicb-cons0';
        $k2 = 'OtherTopicb-cons1';
        $mqMapProp = new \ReflectionProperty(DefaultMQPushConsumer::class, 'mqMap');
        $mqMapProp->setAccessible(true);
        $mqMapProp->setValue($statusC, [$k1 => $q1, $k2 => $q2]);
        $offProp = new \ReflectionProperty(DefaultMQPushConsumer::class, 'consumeOffsets');
        $offProp->setAccessible(true);
        $offProp->setValue($statusC, [$k1 => 42, $k2 => 7]);
        $offsetsByTopic = static function (array $entries): array {
            $byTopic = [];
            foreach ($entries as $e) {
                $byTopic[$e['mq']->topic] = $e['offset'];
            }
            ksort($byTopic);
            return $byTopic;
        };
        $this->checkSame(
            ['OtherTopic' => 7, 'StatusTopic' => 42],
            $offsetsByTopic($statusC->getConsumerStatus(null)),
            'no topic reports the whole consumed-offset table',
        );
        $this->checkSame(
            ['OtherTopic' => 7, 'StatusTopic' => 42],
            $offsetsByTopic($statusC->getConsumerStatus('')),
            'a blank topic reports the whole consumed-offset table',
        );
        // Control: a named topic really does filter, so the blank case above is
        // the no-filter rule and not a handler that ignores its argument.
        $this->checkSame(
            ['StatusTopic' => 42],
            $offsetsByTopic($statusC->getConsumerStatus('StatusTopic')),
            'a named topic reports only that topic',
        );
        $this->checkSame([], $statusC->getConsumerStatus('Never'), 'a topic nobody holds reports nothing');
    }

    // ================================================================ 2. Push concurrent: full batches / re-consume / partial ack / offset correction

    private function testPushConcurrent(ConsFakeBroker $broker): void
    {
        $GLOBALS['RECV'] = [];
        $listener = new ConsTopicListener([
            'CTopic' => ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => null],
            'RTopic' => ['status' => ConsumeConcurrentlyStatus::RECONSUME_LATER, 'ackIndex' => null],
            'PTopic' => ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => 0],
            'FTopic' => ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => null,
                'ackIndexOnlyFirst' => true],
            'OTopic' => ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => null],
        ]);
        $c = new DefaultMQPushConsumer('GID-C-CONS');
        $c->setInstanceName('CONS-C');
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener($listener);
        $c->consumeMessageBatchMaxSize = 3; // whole-batch delivery; needed for partial ack / send-back scenarios
        $c->subscribe('CTopic');
        $c->subscribe('RTopic');
        $c->subscribe('PTopic');
        $c->subscribe('FTopic');
        $c->subscribe('OTopic');
        $c->start();
        $this->check($c->isStarted(), 'push isStarted after start');
        $this->checkSame(MixAll::clientIdFor('CONS-C', null, false), $c->clientId, 'push clientId deterministic');
        $this->checkSame(5, $c->assignedQueueCount(), 'start completes the first rebalance synchronously (5 queues)');

        // first tick: each of the 5 queues pulls one FOUND + one NO_NEW_MSG; dispatch and consumption all finish.
        $c->tick();
        $this->checkSame(['push-0', 'push-1', 'push-2'], $GLOBALS['RECV']['CTopic'] ?? [], 'CTopic receives all 3');
        $this->checkSame(['retry-0', 'retry-1', 'retry-2'], $GLOBALS['RECV']['RTopic'] ?? [], 'RTopic receives all 3');
        $this->checkSame(['part-0', 'part-1', 'part-2'], $GLOBALS['RECV']['PTopic'] ?? [], 'PTopic receives all 3');
        // C4 floor semantics: FTopic send-back of f-1/f-2 fails -> floor=min(1,2)=1 stalls the offset, failed entries are pushed back to the queue head.
        // Deliberate deviation in this port: dispatchRound retries failed re-orders **immediately in the same tick** (no delayed commit),
        // so the floor's intermediate state is not observable single-threaded; only the final state can be asserted.
        // The real verification of "offset never passes failed entries" belongs to a real cluster (retries naturally span ticks).
        $this->checkSame(['f-0', 'f-1', 'f-2', 'f-1', 'f-2'], $GLOBALS['RECV']['FTopic'] ?? [],
            'the 2 failed FTopic send-backs are pushed back to the queue head and replayed within the same tick');
        // PTopic: send-back of p-1/p-2 succeeds -> the broker will re-deliver to %RETRY%, the client advances past them as usual
        // (successfully sent-back messages are removed from the local buffer; offset = max(queueOffset)+1 of the remaining msgs).
        $this->checkSame(3, ($c->getConsumerStatus('PTopic')[0]['offset'] ?? -1),
            'offset advances past 3 after successful send-back');

        // second tick: OTopic rebuilds after OFFSET_ILLEGAL and pulls at the corrected offset.
        $c->tick();

        // final offsets: C=3 / R=3 / P=3 / F=3 (caught up after replay) / O=77 (OFFSET_ILLEGAL corrected).
        $status = $c->getConsumerStatus(null);
        $byTopic = [];
        foreach ($status as $e) {
            $byTopic[$e['mq']->topic] = $e['offset'];
        }
        $this->checkSame(3, $byTopic['CTopic'] ?? -1, 'CTopic consumed offset=3');
        $this->checkSame(3, $byTopic['PTopic'] ?? -1, 'PTopic offset=3');
        $this->checkSame(3, $byTopic['RTopic'] ?? -1, 'RTopic offset=3 after re-consume');
        $this->checkSame(3, $byTopic['FTopic'] ?? -1, 'FTopic offset=3 after replay');
        $this->checkSame(77, $byTopic['OTopic'] ?? -1, 'OTopic offset corrected to 77');

        // wire-behavior assertions.
        $this->loadLog(getenv('RMQ_LOG'));
        $pulls = $this->logWhere(RequestCode::PULL_MESSAGE);
        $this->check($pulls !== [], 'PULL requests logged');
        foreach ($pulls as $p) {
            $this->check(((int) $p['sysFlag'] & PullSysFlag::FLAG_SUSPEND) === 0,
                "push short-polling suspend bit off (seq={$p['seq']})");
        }
        $firstC = $this->logWhere(RequestCode::PULL_MESSAGE, 'CTopic')[0] ?? null;
        $this->check($firstC !== null && (int) $firstC['queueOffset'] === 0, 'CTopic first pull starts from offset 0');
        $oPulls = $this->logWhere(RequestCode::PULL_MESSAGE, 'OTopic');
        $this->check(count($oPulls) >= 2 && (int) $oPulls[1]['queueOffset'] === 77,
            're-pulls at corrected offset 77 after OFFSET_ILLEGAL');

        // send-back: RTopic 3 + PTopic 2 succeed; FTopic 2 rejected by the broker.
        // delayLevel = 3 + reconsumeTimes(0).
        $backs = $this->logWhere(RequestCode::CONSUMER_SEND_MSG_BACK, null, 'GID-C-CONS');
        $this->checkSame(7, count($backs), 'RECONSUME_LATER×3 + partial ack×2 + failed send-back×2 = 7 total');
        foreach ($backs as $b) {
            $this->checkSame(3, (int) $b['delayLevel'], 'send-back delayLevel=3+reconsumeTimes');
            $this->checkSame(16, (int) $b['maxReconsumeTimes'], 'maxReconsumeTimes -1 -> 16');
        }
        $this->checkSame(3, count(array_filter($backs, static fn($e) => ($e['originTopic'] ?? '') === 'RTopic')),
            'RTopic sends back 3');
        $this->checkSame(2, count(array_filter($backs, static fn($e) => ($e['originTopic'] ?? '') === 'PTopic')),
            'PTopic sends back 2');
        $this->checkSame(2, count(array_filter($backs, static fn($e) => ($e['originTopic'] ?? '') === 'FTopic')),
            'FTopic sends back 2 (failed)');

        // offset commit: persistOffsetsOnce writes consumed offsets to the broker.
        $c->persistOffsetsOnce();
        $this->loadLog(getenv('RMQ_LOG'));
        $updates = $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, null, 'GID-C-CONS');
        $commitByTopic = [];
        foreach ($updates as $u) {
            $commitByTopic[$u['topic']] = (int) $u['commitOffset'];
        }
        $this->checkSame(3, $commitByTopic['CTopic'] ?? -1, 'UPDATE_CONSUMER_OFFSET CTopic=3');
        $this->checkSame(3, $commitByTopic['PTopic'] ?? -1, 'UPDATE_CONSUMER_OFFSET PTopic=3');
        $this->checkSame(77, $commitByTopic['OTopic'] ?? -1, 'UPDATE_CONSUMER_OFFSET OTopic=77');
        $this->checkSame(3, $commitByTopic['FTopic'] ?? -1, 'UPDATE_CONSUMER_OFFSET FTopic=3');

        // resetOffset (220 contract): new offset 42 tears down and rebuilds queues; later empty pulls correct the offset back to 3.
        $c->resetOffset('CTopic', [['mq' => new MessageQueue('CTopic', 'b-cons', 0), 'offset' => 42]]);
        $this->loadLog(getenv('RMQ_LOG'));
        $resetUpdates = array_filter(
            $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, 'CTopic', 'GID-C-CONS'),
            static fn($u) => (int) $u['commitOffset'] === 42,
        );
        $this->check($resetUpdates !== [], 'resetOffset commits the new offset 42 to the broker');
        $c->tick();
        $status = $c->getConsumerStatus('CTopic');
        $this->checkSame(3, $status[0]['offset'] ?? -1, 'empty pulls correct the offset back to 3 after resetOffset');

        // instance duck-typing contract: adjustThreadPool is a no-op; declared corePoolSize is observable.
        $c->adjustThreadPool();
        $this->checkSame(20, $c->getCorePoolSize(), 'corePoolSize defaults to 20');

        $this->cleanup($c);
    }

    // ================================================================ 2b. Push suspend/resume (suspend/resume/isPause)

    /**
     * Contract: suspend/resume only flips the pause flag, isPause reads it; the pause gate
     * only applies at the PULL_MESSAGE / POP_MESSAGE send entry point (pause flag checked before sending).
     *
     * What is locked here is **ordering**, not "did it stop": the gate must sit after the timestamp
     * stamping. If placed before, a suspension longer than PULL_MAX_IDLE_TIME (120s) would make the
     * stall detector kill this loop, rebalance would tear down and rebuild the queue, and an ops
     * "pause two minutes then resume" would actually cost a full queue re-delivery. Single-threaded
     * PHP has no "thread died" branch, so there is only the timeout criterion — this race is easy to get wrong.
     */
    private function testPushSuspendResume(ConsFakeBroker $broker): void
    {
        // constants: 1000ms suspend backoff vs 120s stall threshold — two orders of magnitude apart for a reason.
        $this->checkSame(1.0, ConsumerDefaults::PULL_TIME_DELAY_WHEN_SUSPEND,
            'PULL_TIME_DELAY_WHEN_SUSPEND = default 1000ms');
        $this->checkSame(120.0, ConsumerDefaults::PULL_MAX_IDLE_TIME, 'PULL_MAX_IDLE_TIME = 120s');

        // must be safe without start (suspend/resume do not check service state), and flipping is idempotent.
        $idle = new DefaultMQPushConsumer('GID-SUS-IDLE');
        $idle->setInstanceName('CONS-SUS-IDLE');
        $this->checkSame(false, $idle->isPaused(), 'not paused by default (initial pause = false)');
        $idle->suspend();
        $idle->suspend();
        $this->checkSame(true, $idle->isPaused(), 'repeated suspend stays paused');
        $idle->resume();
        $idle->resume();
        $this->checkSame(false, $idle->isPaused(), 'repeated resume stays running');

        $GLOBALS['RECV'] = [];
        $c = new DefaultMQPushConsumer('GID-SUS-CONS');
        $c->setInstanceName('CONS-SUS');
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener(new ConsTopicListener([]));
        $c->subscribe('UTopic');
        $c->start();
        $this->checkSame(1, $c->assignedQueueCount(), 'suspend case gets 1 queue');

        try {
            // get pulling going first: without the "was pulling" premise, "it stopped" means nothing.
            // tick count is not fixed — the first tick may be spent on resolveInitialOffset instead of pulling.
            for ($i = 0; $i < 6; $i++) {
                $c->tick();
                usleep(200_000);
                $this->loadLog(getenv('RMQ_LOG'));
                if (count($this->logWhere(RequestCode::PULL_MESSAGE, 'UTopic', 'GID-SUS-CONS')) >= 2) {
                    break;
                }
            }
            $before = count($this->logWhere(RequestCode::PULL_MESSAGE, 'UTopic', 'GID-SUS-CONS'));
            $this->check($before >= 2, "PULL requests already on the wire before suspending ($before entries)");

            $key = message_queue_key(new MessageQueue('UTopic', 'b-cons', 0));
            $stampBefore = $c->consumerRunningInfo()->mqTable[$key]['lastPullTimestamp'] ?? 0;

            $c->suspend();
            $this->checkSame(true, $c->isPaused(), 'isPaused true after suspend');

            $baselinePulls = $before;
            // several ticks, crossing the 1s suspend backoff window: not one network write allowed while suspended.
            for ($i = 0; $i < 4; $i++) {
                $c->tick();
                usleep(300_000);
            }
            $this->loadLog(getenv('RMQ_LOG'));
            $this->checkSame($baselinePulls, count($this->logWhere(RequestCode::PULL_MESSAGE, 'UTopic', 'GID-SUS-CONS')),
                'no further PULL_MESSAGE on the wire while suspended (pause gate effective at the send entry)');

            // key invariant: suspended ≠ stalled. Timestamps must keep advancing, otherwise the 120s criterion tears the queue down.
            $stampAfter = $c->consumerRunningInfo()->mqTable[$key]['lastPullTimestamp'] ?? 0;
            $this->check($stampAfter > $stampBefore,
                sprintf('lastPullTimestamp must keep advancing while suspended (%d -> %d): gate sits after stamping',
                    $stampBefore, $stampAfter));
            $this->checkSame(1, $c->assignedQueueCount(), 'suspend does not tear down queues or change assignment');
            $this->checkSame($stampAfter,
                $c->consumerRunningInfo()->mqTable[$key]['lastPullTimestamp'] ?? 0,
                'the 307 running info still reads the advancing timestamp while suspended');

            $c->resume();
            $this->checkSame(false, $c->isPaused(), 'back to running after resume');
            $c->tick();
            $this->loadLog(getenv('RMQ_LOG'));
            $after = count($this->logWhere(RequestCode::PULL_MESSAGE, 'UTopic', 'GID-SUS-CONS'));
            $this->check($after > $baselinePulls,
                sprintf('pulls resume on their own after resume (%d -> %d)', $baselinePulls, $after));
        } finally {
            $this->cleanup($c);
        }
    }

    // ================================================================ 3. Push orderly: lock queues -> suspend re-order -> success

    private function testPushOrderly(ConsFakeBroker $broker): void
    {
        $GLOBALS['RECV'] = [];
        $listener = new ConsOrderlyListener();
        $c = new DefaultMQPushConsumer('GID-S-CONS');
        $c->setInstanceName('CONS-S');
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener($listener);
        $c->suspendCurrentQueueTimeMillis = 10; // clamp the suspend re-order period to the lower bound so the test does not wait 1s
        $c->consumeMessageBatchMaxSize = 3;     // whole-batch delivery: one suspend/success covers all 3
        $c->subscribe('STopic');
        $c->start();
        $this->checkSame(1, $c->assignedQueueCount(), 'orderly start completes rebalance');

        // first tick: LOCK_BATCH_MQ -> pull -> listener suspends -> batch pushed back to the queue head.
        // Whether one tick lands depends on when the fake-broker child process gets scheduled; on a busy
        // machine whole rounds may spin empty (no lock sent, no batch pulled, and every later assertion collapses).
        // So this is a gate: tick repeatedly until the first delivery, then stop — no extra round is run,
        // so the "runs exactly once" assertion still holds.
        for ($i = 0; $i < 8 && ($GLOBALS['RECV']['STopic'] ?? []) === []; $i++) {
            $c->tick();
        }
        $this->checkSame(['order-0', 'order-1', 'order-2'], $GLOBALS['RECV']['STopic'] ?? [], 'orderly first round receives 3');
        $this->checkSame(1, $listener->calls, 'listener runs exactly once on the suspend path');
        $sStatus = $c->getConsumerStatus('STopic');
        $this->check($sStatus === [] || ($sStatus[0]['offset'] ?? -1) <= 0, 'offset does not advance while suspended');

        $this->loadLog(getenv('RMQ_LOG'));
        $locks = $this->logWhere(RequestCode::LOCK_BATCH_MQ, null, 'GID-S-CONS');
        $this->check($locks !== [], 'orderly consumption issues LOCK_BATCH_MQ');
        $lockMqSet = $locks[0]['mqSet'] ?? [];
        $this->check($lockMqSet !== []
            && ($lockMqSet[0]['topic'] ?? '') === 'STopic'
            && ($lockMqSet[0]['brokerName'] ?? '') === 'b-cons',
            'lock request carries mqSet(topic/brokerName/queueId)');
        $this->checkSame(MixAll::clientIdFor('CONS-S', null, false), $locks[0]['clientId'] ?? '', 'lock request carries clientId');
        // orderly consumption never sends CONSUMER_SEND_MSG_BACK (retries are re-ordered locally in place).
        $this->checkSame([], $this->logWhere(RequestCode::CONSUMER_SEND_MSG_BACK, null, 'GID-S-CONS'),
            'orderly suspend does not send back');

        // when the suspension period elapses: re-order the batch -> SUCCESS -> offset advances to 3. The criterion is
        // "the listener ran a second time", so advance by gate rather than betting on a fixed time window
        // (the 10ms-lower-bound suspend period is just the earliest possible re-order moment).
        for ($i = 0; $i < 20 && $listener->calls < 2; $i++) {
            usleep(20_000);
            $c->tick();
        }
        $this->checkSame(2, $listener->calls, 'listener runs a second time after re-order');
        $this->checkSame(array_merge(['order-0', 'order-1', 'order-2'], ['order-0', 'order-1', 'order-2']),
            $GLOBALS['RECV']['STopic'] ?? [], 'the same batch is re-ordered and replayed in place');
        $this->checkSame(3, ($c->getConsumerStatus('STopic')[0]['offset'] ?? -1), 'offset=3 after SUCCESS');

        // orderly consumption must unlock on shutdown (UNLOCK_BATCH_MQ).
        $this->cleanup($c);
        $this->loadLog(getenv('RMQ_LOG'));
        $this->check($this->logWhere(RequestCode::UNLOCK_BATCH_MQ, null, 'GID-S-CONS') !== [],
            'shutdown issues UNLOCK_BATCH_MQ');
    }

    // ================================================================ 4. PullConsumer: manual offset management

    private function testPullConsumer(ConsFakeBroker $broker): void
    {
        $c = new DefaultMQPullConsumer('GID-P-CONS');
        $c->setInstanceName('CONS-P');
        $c->setNameServerAddresses([$broker->addr]);
        $c->registerTopic('DTopic');
        $c->start();
        $this->check($c->mqClient !== null, 'pull instance ready after start');

        $queues = $c->fetchSubscribeMessageQueues('DTopic');
        $this->checkSame(1, count($queues), 'fetchSubscribeMessageQueues returns 1 queue');
        $mq = $queues[0];
        $this->checkSame('DTopic', $mq->topic, 'queue topic');

        // committed offset (QUERY_CONSUMER_OFFSET -> DTopic=7).
        $this->checkSame(7, $c->fetchConsumeOffset($mq), 'fetchConsumeOffset=7');

        // short polling: suspend bit must be off (INVARIANTS B3: writing true always times out).
        $r = $c->pull($mq, '*', 0);
        $this->checkSame(PullStatus::FOUND, $r->status, 'pull FOUND');
        $this->checkSame(5, $r->nextBeginOffset, 'pull nextBeginOffset=5');
        $this->checkSame(1, count($r->msgFoundList), 'pull brings back 1 message');
        $this->checkSame('direct-4', $r->msgFoundList[0]->getBody(), 'pull message body');
        $this->checkSame(4, $r->msgFoundList[0]->queueOffset, 'pull message queueOffset');

        $this->loadLog(getenv('RMQ_LOG'));
        $dpull = $this->logWhere(RequestCode::PULL_MESSAGE, 'DTopic', 'GID-P-CONS');
        $this->check($dpull !== [] && (int) $dpull[0]['sysFlag'] === PullSysFlag::FLAG_SUBSCRIPTION,
            'pull sysFlag=4 (short polling + subscription bit)');

        // long polling: suspend bit must be on.
        $rb = $c->pullBlockIfNotFound($mq, '*', 0, 32);
        $this->checkSame(PullStatus::FOUND, $rb->status, 'pullBlockIfNotFound parses too');
        $this->loadLog(getenv('RMQ_LOG'));
        $dpull = $this->logWhere(RequestCode::PULL_MESSAGE, 'DTopic', 'GID-P-CONS');
        $last = $dpull[count($dpull) - 1] ?? null;
        $this->check($last !== null && (int) $last['sysFlag'] === (PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_SUSPEND),
            'pullBlockIfNotFound sysFlag=6 (suspend bit on)');

        // offset management and ops queries.
        $c->updateConsumeOffset($mq, 9);
        $this->loadLog(getenv('RMQ_LOG'));
        $upd = $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, 'DTopic', 'GID-P-CONS');
        $this->check($upd !== [] && (int) $upd[0]['commitOffset'] === 9, 'updateConsumeOffset commits 9');
        $this->checkSame(55, $c->searchOffset($mq, 1_700_000_000_000), 'searchOffset=55');
        $this->checkSame(100, $c->maxOffset($mq), 'maxOffset=100');
        $this->checkSame(0, $c->minOffset($mq), 'minOffset=0');
        $this->checkSame(1234567890000, $c->earliestMsgStoreTime($mq), 'earliestMsgStoreTime');

        // send-back: delayLevel specified by the caller; the default retry ceiling (16) rides along.
        $c->sendMessageBack($r->msgFoundList[0], 2);
        $this->loadLog(getenv('RMQ_LOG'));
        $backs = $this->logWhere(RequestCode::CONSUMER_SEND_MSG_BACK, null, 'GID-P-CONS');
        $this->checkSame(1, count($backs), 'pull sendMessageBack called once');
        $this->checkSame(2, (int) ($backs[0]['delayLevel'] ?? -1), 'send-back delayLevel=2');
        $this->checkSame(16, $backs[0]['maxReconsumeTimes'] ?? null, 'pull send-back carries the default retry ceiling 16');

        // fetchMessageQueuesInBalance: pull mode has no background rebalance, so compute this instance's
        // queues on the spot with the same formula as rebalanceByTopic.
        // The group contains only this instance (RMQ_CONSUMER_MAP registers only CONS-P) -> the whole share is mine.
        $balanced = $c->fetchMessageQueuesInBalance('DTopic');
        $this->checkSame(1, count($balanced), 'balanced view gives the 1 queue this instance owns');
        $this->checkSame('DTopic', $balanced[0]->topic ?? '', 'balanced view queue topic');
        $this->checkSame('b-cons', $balanced[0]->brokerName ?? '', 'balanced view queue brokerName');
        $this->checkSame(0, $balanced[0]->queueId ?? -1, 'balanced view queue queueId');
        // When the consumer list cannot be fetched, **keep the existing allocation**: switch to an unregistered group;
        // DTopic was actually pulled from so it stays (local bookkeeping), while never-pulled ETopic must be empty —
        // never fall back to "exclusive ownership of all queues", otherwise multiple instances in one group consume in duplicate.
        $group = $c->consumerGroup;
        $c->consumerGroup = 'GID-NO-MAP';
        $this->checkSame(['DTopic@b-cons@0'],
            array_map(static fn($mq) => "{$mq->topic}@{$mq->brokerName}@{$mq->queueId}",
                $c->fetchMessageQueuesInBalance('DTopic')),
            'falls back to the local pullFromWhichNodeTable key set when the list is unavailable');
        $this->checkSame([], $c->fetchMessageQueuesInBalance('ETopic'),
            'never pulled and no list -> empty set (same semantics as the empty table before the first rebalance)');
        $c->consumerGroup = $group;
        // not-started consumers are always rejected (isRunning() guard): calling on a fresh instance must throw.
        $idle = new DefaultMQPullConsumer('GID-P-CONS');
        $idle->setNameServerAddresses([$broker->addr]);
        $threw = false;
        try {
            $idle->fetchMessageQueuesInBalance('DTopic');
        } catch (MQClientException) {
            $threw = true;
        }
        $this->check($threw, 'balanced view without start -> MQClientException');

        $this->cleanup($c);
    }

    // ================================================================ 4b. Pull consumer: selector on the wire

    /**
     * A selector decides what the pull reports: the expression text, its type and the version
     * stamp travel together, so a SQL92 expression is never mistaken for a tag list. The tag
     * path is the paired control and must stay on the version-0 shape it has always used.
     */
    private function testPullSelectorWire(ConsFakeBroker $broker): void
    {
        $c = new DefaultMQPullConsumer('GID-Q-CONS');
        $c->setInstanceName('CONS-Q');
        $c->setNameServerAddresses([$broker->addr]);
        $c->registerTopic('QTopic');
        $c->registerTopic('QTopic2');
        $c->registerTopic('QTopic3');
        $c->registerTopic('QTopic4');
        $c->start();

        $sql = "(TAGS in ('TagA')) AND (a > 10)";
        $mq = $c->fetchSubscribeMessageQueues('QTopic')[0] ?? null;
        $this->check($mq !== null, 'pull consumer resolves the QTopic queue');
        if ($mq === null) {
            return;
        }

        $r = $c->pullBySelector($mq, MessageSelector::bySql($sql), 0, 32);
        $this->checkSame(PullStatus::FOUND, $r->status, 'SQL92 selector pull finds the batch');
        // Pull mode hands the batch over untouched: only a tag subscription filters client-side.
        $this->checkSame(['sel-tag-a', 'sel-tag-c'],
            array_map(static fn($m) => $m->getBody(), $r->msgFoundList),
            'pull mode does not second-guess a SQL92 match');

        $this->loadLog(getenv('RMQ_LOG'));
        $sqlRow = $this->lastPullRow(RequestCode::PULL_MESSAGE, 'QTopic', 'GID-Q-CONS');
        $this->checkSame($sql, $sqlRow['subscription'] ?? null, 'SQL92 expression reaches the wire verbatim');
        $this->checkSame(ExpressionType::SQL92, $sqlRow['expressionType'] ?? null, 'SQL92 selector reports SQL92');
        $this->check((int) ($sqlRow['subVersion'] ?? 0) > 0, 'non-tag pull reports the subscription version');
        $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION, (int) ($sqlRow['sysFlag'] ?? 0),
            'SQL92 short-poll sysFlag=4 (class-filter bit stays off)');

        // Paired control 1: the bare tag expression keeps the old shape (TAG + version 0 + compiled tags).
        $mq2 = $c->fetchSubscribeMessageQueues('QTopic2')[0] ?? null;
        $bare = $c->pull($mq2, 'TagA || TagB', 0);
        $this->checkSame(PullStatus::FOUND, $bare->status, 'bare tag pull finds the batch');
        $this->loadLog(getenv('RMQ_LOG'));
        $tagRow = $this->lastPullRow(RequestCode::PULL_MESSAGE, 'QTopic2', 'GID-Q-CONS');
        $this->checkSame('TagA || TagB', $tagRow['subscription'] ?? null, 'tag expression reaches the wire as written');
        $this->checkSame(ExpressionType::TAG, $tagRow['expressionType'] ?? null, 'bare expression reports TAG');
        $this->checkSame(0, (int) ($tagRow['subVersion'] ?? -1), 'tag pull keeps the version-0 stamp');
        $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION, (int) ($tagRow['sysFlag'] ?? 0),
            'tag short-poll sysFlag=4 (subscription bit, no suspend, no class filter)');

        // Paired control 2: a TAG selector is byte-identical to the bare expression.
        $mq3 = $c->fetchSubscribeMessageQueues('QTopic3')[0] ?? null;
        $c->pullBySelector($mq3, MessageSelector::byTag('TagA || TagB'), 0);
        $this->loadLog(getenv('RMQ_LOG'));
        $selTagRow = $this->lastPullRow(RequestCode::PULL_MESSAGE, 'QTopic3', 'GID-Q-CONS');
        $this->checkSame($this->subscriptionCells($tagRow), $this->subscriptionCells($selTagRow),
            'a TAG selector puts the same subscription cells on the wire as the bare expression');

        // CLASS_FILTER: its own type on the wire, and the pull sysFlag stays the plain subscription
        // one (the class-filter bit routes to a legacy filter server this client does not speak).
        $mq4 = $c->fetchSubscribeMessageQueues('QTopic4')[0] ?? null;
        $c->pullBySelector($mq4, new MessageSelector(ExpressionType::CLASS_FILTER, 'MyClassFilter'), 0);
        $this->loadLog(getenv('RMQ_LOG'));
        $clsRow = $this->lastPullRow(RequestCode::PULL_MESSAGE, 'QTopic4', 'GID-Q-CONS');
        $this->checkSame('MyClassFilter', $clsRow['subscription'] ?? null, 'class-filter expression is not parsed as a tag list');
        $this->checkSame(ExpressionType::CLASS_FILTER, $clsRow['expressionType'] ?? null, 'class-filter selector reports CLASS_FILTER');
        $this->check((int) ($clsRow['subVersion'] ?? 0) > 0, 'class-filter pull reports the subscription version');
        $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION, (int) ($clsRow['sysFlag'] ?? 0),
            'class-filter pull keeps the class-filter sysFlag bit off (4)');

        // Long polling keeps the selector shape too: only the suspend bit differs.
        $blocked = $c->pullBlockIfNotFoundBySelector($mq, MessageSelector::bySql($sql), 0, 32);
        $this->checkSame(PullStatus::NO_NEW_MSG, $blocked->status, 'blocked SQL92 pull reports the scripted empty round');
        $this->loadLog(getenv('RMQ_LOG'));
        $blockRow = $this->lastPullRow(RequestCode::PULL_MESSAGE, 'QTopic', 'GID-Q-CONS');
        $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_SUSPEND, (int) ($blockRow['sysFlag'] ?? 0),
            'pullBlockIfNotFoundBySelector sysFlag=6 (suspend bit on)');
        $this->checkSame($sql, $blockRow['subscription'] ?? null, 'long-poll keeps the SQL92 expression');
        $this->checkSame(ExpressionType::SQL92, $blockRow['expressionType'] ?? null, 'long-poll keeps SQL92 as the type');
        $this->check((int) ($blockRow['subVersion'] ?? 0) > 0,
            'a long-poll round of the same selector still reports a version, not the tag version 0');

        // An unusable queue is rejected before any request leaves.
        $message = null;
        try {
            $c->pullBySelector(new MessageQueue('', 'b-cons', 0), MessageSelector::bySql($sql));
        } catch (MQClientException $e) {
            $message = $e->getMessage();
        }
        $this->checkSame('mq is null', $message, 'pullBySelector rejects a queue without a topic');

        // What a pull consumer registers is its own contract: the topics it subscribes to, with the
        // SUB_ALL tag subscription. Selectors belong to the pull request, not to the registration.
        $subs = $this->heartbeatSubs('GID-Q-CONS');
        $this->checkSame(['QTopic', 'QTopic2', 'QTopic3', 'QTopic4'], array_keys($subs),
            'pull heartbeat registers every subscribed topic in sorted order');
        $this->checkSame('*', $subs['QTopic']['subString'] ?? null, 'pull heartbeat subscription is SUB_ALL');
        $this->checkSame(ExpressionType::TAG, $subs['QTopic']['expressionType'] ?? null, 'pull heartbeat subscription is a tag one');
        $this->checkSame([], $subs['QTopic']['tagsSet'] ?? null, 'SUB_ALL registers an empty tag set');
        $this->checkSame(0, (int) ($subs['QTopic']['subVersion'] ?? -1), 'pull heartbeat subscription is version 0');

        $this->cleanup($c);
    }

    // ================================================================ 5. LitePull: assign-mode dual cursors + commit + seek

    private function testLitePull(ConsFakeBroker $broker): void
    {
        $c = new DefaultLitePullConsumer('GID-L-CONS');
        $c->setInstanceName('CONS-L');
        $c->setNamesrvAddr($broker->addr);
        $mq = new MessageQueue('LTopic', 'b-cons', 0);
        $c->assign([$mq]); // assigned before start: initial offset resolution failure is swallowed and retried at tick time
        $c->start();
        $this->check($c->isRunning(), 'lite running after start');
        $this->checkSame([$mq], $c->assignment(), 'assignment returns the assigned queues');

        // tick: LITE_PULL_MESSAGE short polling -> FOUND 2 into the buffer.
        $c->tick();
        $this->checkSame(2, $c->pullCursorOf($mq), 'pull cursor advances to 2');
        $this->checkSame(-1, $c->consumeCursorOf($mq), 'consume cursor untouched before poll');

        // poll: hands out the buffer and advances the "consumed cursor" (poll only hands out the buffer; the pull cursor stays).
        $msgs = $c->poll(500);
        $this->checkSame(['lite-0', 'lite-1'], array_map(static fn($m) => $m->getBody(), $msgs), 'poll hands out 2 messages');
        $this->checkSame(2, $c->consumeCursorOf($mq), 'consume cursor=2 after poll');
        $this->checkSame(2, $c->pullCursorOf($mq), 'poll does not move the pull cursor');

        // commit: commits the "consumed cursor" to the broker.
        $c->commit();
        $this->loadLog(getenv('RMQ_LOG'));
        $upd = $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, 'LTopic', 'GID-L-CONS');
        $this->check($upd !== [] && (int) $upd[0]['commitOffset'] === 2, 'commit commits offset 2');
        $this->checkSame(2, $c->committed($mq), 'committed reads the in-memory offset table');
        $this->checkSame(2, $c->pendingCommitOf($mq), 'pendingCommit exposes the in-memory commit position');

        // explicit-offset commit (Map overload): only changes the commit position, not the cursors.
        $c->commit([$mq->topic . $mq->brokerName . '0' => 1], true);
        $this->loadLog(getenv('RMQ_LOG'));
        $upd = $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, 'LTopic', 'GID-L-CONS');
        $lastCommit = (int) ($upd[count($upd) - 1]['commitOffset'] ?? -1);
        $this->checkSame(1, $lastCommit, 'commit(Map) commits the explicit offset 1');
        $this->checkSame(2, $c->pullCursorOf($mq), 'commit(Map) does not move the pull cursor');
        $this->checkSame(2, $c->consumeCursorOf($mq), 'commit(Map) does not move the consume cursor');

        // seek: rewinds all three cursors; the next pull round starts from the new offset.
        $c->seek($mq, 1);
        $this->checkSame(1, $c->pullCursorOf($mq), 'seek rewinds the pull cursor');
        $this->checkSame(1, $c->consumeCursorOf($mq), 'seek rewinds the consume cursor');
        $c->tick();
        $this->checkSame(2, $c->pullCursorOf($mq), 'empty pull after seek pushes the pull cursor back to next=2');

        // pause/resume: no pulling while paused.
        $this->loadLog(getenv('RMQ_LOG'));
        $litePullsBefore = count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-L-CONS'));
        $c->pause([$mq]);
        $c->tick();
        $this->loadLog(getenv('RMQ_LOG'));
        $litePullsPaused = count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-L-CONS'));
        $this->checkSame($litePullsBefore, $litePullsPaused, 'no pulling while paused');
        $c->resume([$mq]);
        $c->tick();
        $this->loadLog(getenv('RMQ_LOG'));
        $litePullsResumed = count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-L-CONS'));
        $this->checkSame($litePullsBefore + 1, $litePullsResumed, 'pulling resumes after resume');

        // LITE_PULL wire behavior: the lite bit must be set and the suspend bit must be off.
        $liteReq = $this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-L-CONS');
        $this->check($liteReq !== [], 'lite uses the LITE_PULL_MESSAGE request code');
        foreach ($liteReq as $e) {
            $this->check(((int) $e['sysFlag'] & PullSysFlag::FLAG_LITE_PULL_MESSAGE) !== 0, 'lite bit set');
            $this->check(((int) $e['sysFlag'] & PullSysFlag::FLAG_SUSPEND) === 0, 'lite short-polling suspend bit off');
        }

        // SimpleMessageListener wraps a callable.
        $l = new SimpleMessageListener(static fn(array $ms) => ConsumeConcurrentlyStatus::CONSUME_SUCCESS);
        $ctx = new ConsumeConcurrentlyContext($mq);
        $this->checkSame(ConsumeConcurrentlyStatus::CONSUME_SUCCESS,
            $l->consumeMessage([], $ctx), 'SimpleMessageListener passes the callable through');

        $this->cleanup($c);
    }

    // ================================================================ 5b. LitePull: selector subscription end to end

    /**
     * The selector has to survive the whole path: the local subscription table, the registration
     * the broker is handed, and every pull request that follows. A tag subscription is the paired
     * control and keeps both the compiled tag set on the registration and the version-0 pull shape,
     * which is also what makes the client-side second filtering possible.
     */
    private function testLitePullSelectorWire(ConsFakeBroker $broker): void
    {
        $sql = "(TAGS in ('TagA')) AND (a > 10)";

        // ---- subscribe mode + SQL92 selector ----
        $sqlC = new DefaultLitePullConsumer('GID-M-CONS');
        $sqlC->setInstanceName('CONS-M');
        $sqlC->setNamesrvAddr($broker->addr);
        $sqlC->subscribeWithSelector('LSqlTopic', MessageSelector::bySql($sql));
        $sqlC->start();

        $this->loadLog(getenv('RMQ_LOG'));
        $sqlSubs = $this->heartbeatSubs('GID-M-CONS');
        $this->checkSame(['LSqlTopic'], array_keys($sqlSubs), 'SQL92 selector is the only registered subscription');
        $sqlSub = $sqlSubs['LSqlTopic'] ?? [];
        $this->checkSame($sql, $sqlSub['subString'] ?? null, 'heartbeat registers the SQL92 expression text');
        $this->checkSame(ExpressionType::SQL92, $sqlSub['expressionType'] ?? null,
            'ConsumerData.subscriptionDataSet says SQL92, not TAG');
        $this->checkSame([], $sqlSub['tagsSet'] ?? null, 'SQL92 registration carries no compiled tag set');
        $this->checkSame([], $sqlSub['codeSet'] ?? null, 'SQL92 registration carries no compiled hash set');
        $this->checkSame(false, $sqlSub['classFilterMode'] ?? null, 'SQL92 registration is not class-filter mode');
        $this->check((int) ($sqlSub['subVersion'] ?? 0) > 0, 'SQL92 registration carries its own version stamp');

        // One tick rebalances and pulls, so the request the subscription produced is on the log.
        $sqlC->tick();
        $this->checkSame(['sel-tag-a', 'sel-tag-c'],
            array_map(static fn($m) => $m->getBody(), $sqlC->poll(0)),
            'a SQL92 batch is handed over whole: the client never re-filters it');
        $this->loadLog(getenv('RMQ_LOG'));
        $sqlPull = $this->lastPullRow(RequestCode::LITE_PULL_MESSAGE, 'LSqlTopic', 'GID-M-CONS');
        $this->checkSame($sql, $sqlPull['subscription'] ?? null, 'the pull request repeats the SQL92 expression');
        $this->checkSame(ExpressionType::SQL92, $sqlPull['expressionType'] ?? null, 'the pull request is typed SQL92');
        $this->checkSame((int) $sqlSub['subVersion'], (int) ($sqlPull['subVersion'] ?? -1),
            'the pulled version is the registered version');
        $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_LITE_PULL_MESSAGE,
            (int) ($sqlPull['sysFlag'] ?? 0), 'SQL92 lite pull sysFlag=20 (subscription + lite, no class filter)');
        // One live connection at a time: the fake broker serves connections sequentially.
        $this->cleanup($sqlC);

        // ---- subscribe mode + tag expression (paired control, byte-identical to the old shape) ----
        $tagC = new DefaultLitePullConsumer('GID-N-CONS');
        $tagC->setInstanceName('CONS-N');
        $tagC->setNamesrvAddr($broker->addr);
        $tagC->subscribe('LTagTopic', 'TagA || TagB');
        $tagC->start();

        $this->loadLog(getenv('RMQ_LOG'));
        $tagSubs = $this->heartbeatSubs('GID-N-CONS');
        $tagSub = $tagSubs['LTagTopic'] ?? [];
        $this->checkSame('TagA || TagB', $tagSub['subString'] ?? null, 'tag registration keeps the expression text');
        $this->checkSame(ExpressionType::TAG, $tagSub['expressionType'] ?? null, 'tag registration is typed TAG');
        $this->checkSame(['TagA', 'TagB'], $tagSub['tagsSet'] ?? null, 'tag registration carries the compiled tag set');
        $this->checkSame([2598919, 2598920], $tagSub['codeSet'] ?? null, 'tag registration carries the compiled hash set');
        $this->checkSame(false, $tagSub['classFilterMode'] ?? null, 'tag registration is not class-filter mode');
        $this->check((int) ($tagSub['subVersion'] ?? 0) > 0, 'the tag registration stamps its own version');

        $tagC->tick();
        // tagsSet is what makes client-side filtering possible: TagC is not in the subscription.
        $this->checkSame(['sel-tag-a'],
            array_map(static fn($m) => $m->getBody(), $tagC->poll(0)),
            'a tag subscription re-filters the batch client-side');
        $this->loadLog(getenv('RMQ_LOG'));
        $tagPull = $this->lastPullRow(RequestCode::LITE_PULL_MESSAGE, 'LTagTopic', 'GID-N-CONS');
        $this->checkSame('TagA || TagB', $tagPull['subscription'] ?? null, 'tag pull reports the tag expression');
        $this->checkSame(ExpressionType::TAG, $tagPull['expressionType'] ?? null, 'tag pull is typed TAG');
        $this->checkSame(0, (int) ($tagPull['subVersion'] ?? -1),
            'a tag pull keeps the version-0 shape whatever the registration says');
        $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_LITE_PULL_MESSAGE,
            (int) ($tagPull['sysFlag'] ?? 0), 'tag lite pull sysFlag=20 matches the pre-selector wire');
        $this->cleanup($tagC);

        // ---- assign mode + selectors (and one queue with no subscription at all) ----
        $assignC = new DefaultLitePullConsumer('GID-O-CONS');
        $assignC->setInstanceName('CONS-O');
        $assignC->setNamesrvAddr($broker->addr);
        $assignC->setSubExpressionForAssignWithSelector('LAssignTopic', MessageSelector::bySql($sql));
        $assignC->setSubExpressionForAssignWithSelector('LClassTopic', new MessageSelector(ExpressionType::CLASS_FILTER, 'MyClassFilter'));
        $assignC->assign([
            new MessageQueue('LAssignTopic', 'b-cons', 0),
            new MessageQueue('LClassTopic', 'b-cons', 0),
            new MessageQueue('LRawTopic', 'b-cons', 0),
        ]);
        $assignC->start();
        $assignC->tick();

        $this->loadLog(getenv('RMQ_LOG'));
        // Assign mode advertises nothing: the heartbeat carries only rebalance subscriptions, and
        // these expressions belong to individually assigned queues rather than to a topic-wide one.
        $this->check($this->heartbeatSeen('GID-O-CONS'),
            'the assign-mode heartbeat itself reached the broker');
        $assignSubs = $this->heartbeatSubs('GID-O-CONS', false);
        $this->checkSame([], array_keys($assignSubs),
            'an assign-mode consumer announces no subscription in its heartbeat');

        $assignReg = $assignC->subscriptionData['LAssignTopic'] ?? null;
        $clsReg = $assignC->subscriptionData['LClassTopic'] ?? null;
        $this->checkSame(ExpressionType::SQL92, $assignReg->expressionType ?? null,
            'setSubExpressionForAssignWithSelector registers SQL92');
        $this->checkSame(ExpressionType::CLASS_FILTER, $clsReg->expressionType ?? null,
            'setSubExpressionForAssignWithSelector registers CLASS_FILTER');
        $this->checkSame(true, $clsReg->classFilterMode ?? null,
            'class-filter mode is the only thing CLASS_FILTER sets beyond the type');
        $this->checkSame([], $clsReg->tagsSet ?? null,
            'class-filter registration carries no compiled tag set');

        $assignSql = $this->lastPullRow(RequestCode::LITE_PULL_MESSAGE, 'LAssignTopic', 'GID-O-CONS');
        $this->checkSame($sql, $assignSql['subscription'] ?? null, 'assign-mode pull repeats the SQL92 expression');
        $this->checkSame(ExpressionType::SQL92, $assignSql['expressionType'] ?? null, 'assign-mode pull is typed SQL92');
        $this->checkSame((int) $assignReg->subVersion, (int) ($assignSql['subVersion'] ?? -1),
            'assign-mode pulled version matches the registered version');

        $assignCls = $this->lastPullRow(RequestCode::LITE_PULL_MESSAGE, 'LClassTopic', 'GID-O-CONS');
        $this->checkSame('MyClassFilter', $assignCls['subscription'] ?? null, 'class-filter pull reports the raw expression');
        $this->checkSame(ExpressionType::CLASS_FILTER, $assignCls['expressionType'] ?? null, 'class-filter pull is typed CLASS_FILTER');
        $this->checkSame(
            PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_LITE_PULL_MESSAGE,
            (int) ($assignCls['sysFlag'] ?? 0), 'class-filter lite pull sysFlag=20 (subscription + lite, class filter off)');

        // A queue assigned with no subscription at all: the raw expression is parsed on the spot,
        // which is SUB_ALL, so nothing is compiled and nothing is re-filtered client-side.
        $rawPull = $this->lastPullRow(RequestCode::LITE_PULL_MESSAGE, 'LRawTopic', 'GID-O-CONS');
        $this->checkSame('*', $rawPull['subscription'] ?? null, 'unsubscribed queue pulls with SUB_ALL');
        $this->checkSame(ExpressionType::TAG, $rawPull['expressionType'] ?? null, 'a parsed-on-the-spot expression is a tag one');
        $this->checkSame(0, (int) ($rawPull['subVersion'] ?? -1), 'SUB_ALL pull keeps the version-0 shape');
        $this->check($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LSqlTopic', 'GID-O-CONS') === []
            && $this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTagTopic', 'GID-O-CONS') === [],
            'assign mode pulls exactly the queues it was given');
        $this->cleanup($assignC);

        // subscriptionDataFor guards against a stored SubscriptionData belonging to another topic.
        $guard = new DefaultLitePullConsumer('GID-O-CONS');
        $guard->subscriptionData['LRawTopic'] = new SubscriptionData('OtherTopic', 'TagZ');
        $resolved = $this->litePrivateMethod($guard, 'subscriptionDataFor', ['LRawTopic']);
        $this->checkSame('LRawTopic', $resolved->topic, 'a stored subscription of another topic is not reused');
        $this->checkSame('*', $resolved->subString, 'the topic is parsed from its own raw expression instead');
        $this->checkSame(ExpressionType::TAG, $resolved->expressionType, 'the fallback parse is a tag subscription');
        $this->checkSame([], $resolved->tagsSet, 'the fallback parse of an absent expression has no tags');
        // The stored subscription wins as soon as its topic matches.
        $guard->subscriptionData['LRawTopic'] = new SubscriptionData('LRawTopic', 'TagA');
        $this->checkSame('TagA',
            $this->litePrivateMethod($guard, 'subscriptionDataFor', ['LRawTopic'])->subString,
            'a matching stored subscription is the one a pull reports');
        // An empty stored expression still reads as SUB_ALL, never as an empty string on the wire.
        $bare = new DefaultLitePullConsumer('GID-O-CONS');
        $bare->subscription['LRawTopic'] = '';
        $this->checkSame('*', $this->litePrivateMethod($bare, 'subscriptionDataFor', ['LRawTopic'])->subString,
            'an empty raw expression falls back to SUB_ALL');
    }

    private function litePrivateMethod(object $c, string $name, array $args = []): mixed
    {
        $m = new \ReflectionMethod(DefaultLitePullConsumer::class, $name);
        $m->setAccessible(true);
        return $m->invokeArgs($c, $args);
    }

    // ================================================================ 5c. LitePull: revocation, heartbeat scope, stopped commit

    private function testLitePullRevocationHeartbeatAndStoppedCommit(ConsFakeBroker $broker): void
    {
        // ---- unsubscribe revokes the topic's queues, cursors and cache right away ----
        $c = new DefaultLitePullConsumer('GID-P-CONS');
        $c->setInstanceName('CONS-P');
        $c->setNamesrvAddr($broker->addr);
        $keep = new MessageQueue('LTopic', 'b-cons', 0);
        $drop = new MessageQueue('LTagTopic', 'b-cons', 0);
        $c->assign([$keep, $drop]);
        $c->start();
        $c->tick();
        $this->checkSame(2, $c->pullCursorOf($keep), 'the kept queue pulled its first batch');
        $this->checkSame(2, $c->pullCursorOf($drop), 'the dropped queue pulled its first batch before unsubscribe');

        $c->unsubscribe('LTagTopic');
        $this->checkSame([$keep], $c->assignment(), "unsubscribe takes the topic's queues out of the assignment");
        $this->checkSame(-1, $c->pullCursorOf($drop), 'the revoked queue loses its pull cursor');
        $this->checkSame(-1, $c->consumeCursorOf($drop), 'the revoked queue loses its consume cursor');

        $this->loadLog(getenv('RMQ_LOG'));
        $droppedBefore = count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTagTopic', 'GID-P-CONS'));
        $keptBefore = count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-P-CONS'));
        // A blank topic is not a topic: it must not revoke the whole assignment.
        $c->unsubscribe('   ');
        $this->checkSame([$keep], $c->assignment(), 'a blank unsubscribe leaves the assignment untouched');
        $c->tick();
        $this->loadLog(getenv('RMQ_LOG'));
        $this->checkSame($droppedBefore,
            count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTagTopic', 'GID-P-CONS')),
            'a revoked topic is never pulled again');
        $this->checkSame($keptBefore + 1,
            count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-P-CONS')),
            'the queues the consumer still holds keep pulling');
        $this->cleanup($c);

        // ---- an assign-mode heartbeat advertises no subscription; subscribe mode is the control ----
        $assignC = new DefaultLitePullConsumer('GID-Q-CONS');
        $assignC->setInstanceName('CONS-Q');
        $assignC->setNamesrvAddr($broker->addr);
        $assignC->setSubExpressionForAssign('LAssignTopic', 'TagA');
        $assignC->assign([new MessageQueue('LAssignTopic', 'b-cons', 0)]);
        $assignC->start();
        $this->loadLog(getenv('RMQ_LOG'));
        $this->check($this->heartbeatSeen('GID-Q-CONS'), 'the assign-mode heartbeat reached the broker');
        $this->checkSame([], array_keys($this->heartbeatSubs('GID-Q-CONS', false)),
            'an assigned queue carries no topic-wide subscription into the heartbeat');
        $this->cleanup($assignC);

        $subC = new DefaultLitePullConsumer('GID-R-CONS');
        $subC->setInstanceName('CONS-R');
        $subC->setNamesrvAddr($broker->addr);
        $subC->subscribe('LTagTopic', 'TagA');
        $subC->start();
        $this->loadLog(getenv('RMQ_LOG'));
        $this->checkSame(['LTagTopic'], array_keys($this->heartbeatSubs('GID-R-CONS')),
            'a subscribe-mode heartbeat does announce its subscription (control)');
        $this->cleanup($subC);

        // ---- committed refuses to answer once the consumer stopped, even with a memory commit ----
        $stopped = new DefaultLitePullConsumer('GID-S-CONS');
        $stopped->setInstanceName('CONS-S');
        $stopped->setNamesrvAddr($broker->addr);
        $mq = new MessageQueue('LTopic', 'b-cons', 0);
        $stopped->assign([$mq]);
        $stopped->start();
        $stopped->commit(['LTopicb-cons0' => 1], false);
        $this->checkSame(1, $stopped->committed($mq), 'a live consumer reports its in-memory commit');
        $this->cleanup($stopped);
        $message = null;
        try {
            $stopped->committed($mq);
        } catch (MQClientException $e) {
            $message = $e->getMessage();
        }
        $this->checkSame('The consumer not running, please start it first.', $message,
            'a stopped consumer refuses to report a committed offset, even one it still holds in memory');
    }

    // ================================================================ 6. Shutdown drain (awaitTerminationMillisWhenShutdown)

    /**
     * shutdown() hands the batches still sitting in the per-queue buffers to the listener before
     * tearing anything down, bounded by awaitTerminationMillisWhenShutdown. Everything here is
     * judged on counters (what the listener got, what is still queued), never on elapsed time: the
     * ceiling is only given a backlog and a per-batch workload big enough that it provably binds
     * inside the first chunk, and the round-cap case needs no timing at all.
     */
    private function testShutdownDrain(ConsFakeBroker $broker): void
    {
        $GLOBALS['DRAIN_LISTENER'] = [];

        // Defaults: no extra ceiling, and the drain's own bookkeeping is fixed by the class.
        $idle = new DefaultMQPushConsumer('GID-DRAIN-IDLE');
        $this->checkSame(0, $idle->awaitTerminationMillisWhenShutdown, 'shutdown drain has no time ceiling by default');
        $this->checkSame(0, $idle->getAwaitTerminationMillisWhenShutdown(), 'the getter reads the public field back');
        $this->checkSame(1000, $idle->dispatchBatchesPerTick, 'a tick dispatches 1000 batches by default');
        $this->checkSame(16, $this->pushPrivateConstant('SHUTDOWN_DRAIN_CHUNK_BATCHES'),
            'the drain walks the backlog 16 batches at a time');
        $this->checkSame(64, $this->pushPrivateConstant('SHUTDOWN_DRAIN_MAX_ROUNDS'),
            'the drain stops after 64 rounds whatever the backlog');

        // Setter round trip, including the "no ceiling" reading of a non-positive value.
        $idle->setAwaitTerminationMillisWhenShutdown(250);
        $this->checkSame(250, $idle->getAwaitTerminationMillisWhenShutdown(), 'the setter drives the ceiling');
        $this->checkSame(250, $idle->awaitTerminationMillisWhenShutdown, 'the setter writes the public field');
        $idle->setAwaitTerminationMillisWhenShutdown(0);
        $this->checkSame(0, $idle->awaitTerminationMillisWhenShutdown, '0 puts no time ceiling on the drain');
        // Never started: nothing queued, nothing drained, nothing thrown.
        $this->checkSame(0, $this->pushPrivateMethod($idle, 'queuedMessageCount'), 'an idle consumer queues nothing');
        $idle->shutdown();
        $this->checkSame(0, $this->pushPrivateMethod($idle, 'queuedMessageCount'), 'shutdown of an idle consumer drains nothing');

        // ---- positive ceiling: shutdown returns while messages are still queued ----
        // A ceiling of 1ms against a ~10ms-per-batch listener cannot survive the first 16-batch
        // chunk, so exactly one chunk goes out and the rest stays queued.
        $limited = $this->drainConsumer($broker, 'GID-K-CONS', 'CONS-K', 'KTopic', 400_000, 2, 1);
        $this->checkSame(1, $limited->getAwaitTerminationMillisWhenShutdown(), 'drain case keeps its ceiling');
        $tick = $this->drainTick($limited);
        $this->checkSame(2, $tick['batches'], 'dispatchBatchesPerTick=2 hands exactly 2 batches per tick');
        $this->checkSame(['drain-0', 'drain-1'], $tick['bodies'], 'the first two batches go out in order');
        $this->checkSame(19, $tick['queued'], '21 pulled - 2 dispatched leaves 19 queued');
        $after = $this->drainShutdown($limited);
        $this->checkSame(18, $after['batches'], 'a bounded drain stops after one 16-batch chunk');
        $ordered = [];
        for ($i = 2; $i < 18; $i++) {
            $ordered[] = "drain-$i";
        }
        $this->checkSame($ordered, array_slice($this->drainListener($limited)->bodies, 2, 16),
            'the bounded drain walks the backlog in queue order');
        $this->checkSame(3, $after['queued'], 'shutdown returns with the rest of the backlog still queued');
        $this->checkSame(3, array_sum($after['pending']), 'what is left is still in the per-queue buffer');

        // ---- control: the default 0 walks the whole backlog ----
        $plain = $this->drainConsumer($broker, 'GID-K2-CONS', 'CONS-K2', 'KTopic2', 0, 2, 0);
        $this->checkSame(2, $this->drainTick($plain)['batches'], 'control tick dispatched the same 2 batches');
        $done = $this->drainShutdown($plain);
        $this->checkSame(21, $done['batches'], 'with no ceiling the drain delivers the whole backlog');
        $this->checkSame(0, $done['queued'], 'and leaves nothing queued');
        $this->checkSame([], $done['pending'], 'the per-queue buffers are gone, not merely empty');

        // ---- a ceiling that binds cannot strand a backlog fitting in one chunk ----
        $small = $this->drainConsumer($broker, 'GID-K3-CONS', 'CONS-K3', 'KTopic3', 400_000, 10, 1);
        $busy = $this->drainTick($small);
        $this->checkSame(10, $busy['batches'], 'dispatchBatchesPerTick=10 hands 10 batches to the listener');
        $this->checkSame(11, $busy['queued'], 'the other 11 are still queued at the ceiling');
        $few = $this->drainShutdown($small);
        $this->checkSame(21, $few['batches'], '11 queued batches fit in one chunk, so all of them go out');
        $this->checkSame(0, $few['queued'], 'a spending ceiling strands nothing once the buffer is empty');

        // ---- round cap: no timing involved, the backlog simply outruns 64 chunks ----
        $cap = $this->drainConsumer($broker, 'GID-J-CONS', 'CONS-J', 'JTopic', 0, 1, 0, 4096);
        $rounds = $this->pushPrivateConstant('SHUTDOWN_DRAIN_MAX_ROUNDS');
        $chunk = $this->pushPrivateConstant('SHUTDOWN_DRAIN_CHUNK_BATCHES');
        $queuedAfter = 0;
        for ($i = 0; $i < 5; $i++) {
            $cap->tick();
            $queuedAfter = $this->pushPrivateMethod($cap, 'queuedMessageCount');
        }
        $this->checkSame(1275, $queuedAfter, '5 ticks of 8 short polls less 1 batch each leave a 1275-batch backlog');
        $this->check($queuedAfter > $rounds * $chunk,
            'the backlog is deliberately bigger than one drain may deliver (64 x 16 batches)');
        $batchesBefore = $this->drainListener($cap)->batches;
        $cap->shutdown();
        $delivered = $this->drainListener($cap)->batches - $batchesBefore;
        $this->checkSame($rounds * $chunk, $delivered, 'the cap is what stopped this drain, not the clock');
        $this->checkSame($queuedAfter - $rounds * $chunk,
            $this->pushPrivateMethod($cap, 'queuedMessageCount'), 'everything past the cap stays queued');
    }

    /**
     * Build and start a drain-scenario push consumer: one message per batch, a fixed number of
     * batches per tick, a listener that burns a known amount of CPU per batch (0 = no work at all),
     * and optionally a pull threshold high enough to let a backlog grow past the drain cap.
     */
    private function drainConsumer(
        ConsFakeBroker $broker,
        string $group,
        string $instance,
        string $topic,
        int $workBytes,
        int $batchesPerTick,
        int $ceilingMillis,
        ?int $pullThreshold = null,
    ): DefaultMQPushConsumer {
        $listener = new ConsDrainListener($workBytes);
        $GLOBALS['DRAIN_LISTENER'][$instance] = $listener;
        $c = new DefaultMQPushConsumer($group);
        $c->setInstanceName($instance);
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener($listener);
        $c->consumeMessageBatchMaxSize = 1;
        $c->dispatchBatchesPerTick = $batchesPerTick;
        $c->setAwaitTerminationMillisWhenShutdown($ceilingMillis);
        if ($pullThreshold !== null) {
            $c->pullThresholdForQueue = $pullThreshold;
        }
        $c->subscribe($topic);
        $c->start();
        return $c;
    }

    private function drainListener(DefaultMQPushConsumer $c): ConsDrainListener
    {
        $listener = $GLOBALS['DRAIN_LISTENER'][$c->instanceName] ?? null;
        $this->check($listener instanceof ConsDrainListener, 'drain listener found for ' . $c->instanceName);
        return $listener;
    }

    /** @return array{batches:int,bodies:list<string>,queued:int} */
    private function drainTick(DefaultMQPushConsumer $c): array
    {
        $c->tick();
        $listener = $this->drainListener($c);
        return [
            'batches' => $listener->batches,
            'bodies' => $listener->bodies,
            'queued' => $this->pushPrivateMethod($c, 'queuedMessageCount'),
        ];
    }

    /** @return array{batches:int,queued:int,pending:array<string,int>} */
    private function drainShutdown(DefaultMQPushConsumer $c): array
    {
        $c->shutdown();
        $listener = $this->drainListener($c);
        $pending = [];
        foreach ((array) $this->pushPrivate($c, 'pending') as $key => $dq) {
            $pending[(string) $key] = is_array($dq) ? count($dq) : 0;
        }
        return [
            'batches' => $listener->batches,
            'queued' => $this->pushPrivateMethod($c, 'queuedMessageCount'),
            'pending' => $pending,
        ];
    }

    // ================================================================ run

    public function run(): int
    {
        $this->testConsumerSupportUnit();
        $this->testSelectorSubscriptionUnit();

        // ---- network cases: start the fake broker + log/consumer-list files ----
        $tmpDir = sys_get_temp_dir() . '/rmq-cons-test-' . getmypid();
        if (!is_dir($tmpDir)) {
            @mkdir($tmpDir, 0777, true);
        }
        $logPath = $tmpDir . '/broker.jsonl';
        $groupMapPath = $tmpDir . '/groups.json';
        @unlink($logPath);
        putenv("RMQ_LOG={$logPath}");
        // consumer list isolated per group: each group contains only its own clientId (order must match the client).
        $groupMap = [
            'GID-C-CONS' => [MixAll::clientIdFor('CONS-C', null, false)],
            'GID-S-CONS' => [MixAll::clientIdFor('CONS-S', null, false)],
            'GID-P-CONS' => [MixAll::clientIdFor('CONS-P', null, true)],
            'GID-L-CONS' => [MixAll::clientIdFor('CONS-L', null, true)],
            'GID-SUS-CONS' => [MixAll::clientIdFor('CONS-SUS', null, false)],
            // shutdown-drain push consumers (one queue each, single-member group).
            'GID-K-CONS' => [MixAll::clientIdFor('CONS-K', null, false)],
            'GID-K2-CONS' => [MixAll::clientIdFor('CONS-K2', null, false)],
            'GID-K3-CONS' => [MixAll::clientIdFor('CONS-K3', null, false)],
            'GID-J-CONS' => [MixAll::clientIdFor('CONS-J', null, false)],
        ];
        file_put_contents($groupMapPath, json_encode($groupMap));
        putenv("RMQ_CONSUMER_MAP={$groupMapPath}");

        $broker = ConsFakeBroker::start();
        try {
            $this->testPushConcurrent($broker);
            $this->testPushSuspendResume($broker);
            $this->testPushOrderly($broker);
            $this->testPullConsumer($broker);
            $this->testPullSelectorWire($broker);
            $this->testLitePull($broker);
            $this->testLitePullSelectorWire($broker);
            $this->testLitePullRevocationHeartbeatAndStoppedCommit($broker);
            $this->testShutdownDrain($broker);
        } finally {
            $broker->stop();
            putenv('RMQ_LOG');
            putenv('RMQ_CONSUMER_MAP');
            @unlink($logPath);
            @unlink($groupMapPath);
            @rmdir($tmpDir);
        }
        return $this->summary();
    }
}

exit((new RunClientConsumer())->run());
