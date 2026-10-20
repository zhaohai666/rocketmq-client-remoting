<?php

declare(strict_types=1);

/**
 * Consumer wire-shape and shutdown-drain contract suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientConsumerWire.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Two contracts are pinned here, both read from the **peer side**:
 *
 * 1. A pull request carries the subscription's own expression type, expression text and version
 *    stamp. That is only observable on the wire, so every PULL_MESSAGE / LITE_PULL_MESSAGE frame
 *    the fake broker receives is appended to a JSON-lines log with those three cells, and the
 *    assertions read the log -- never the client object.
 * 2. shutdown() hands the batches still queued for consumption to the listener, and
 *    awaitTerminationMillisWhenShutdown puts a ceiling on exactly that walk. The proof is a count
 *    of listener batches plus the queue depth left behind; nothing here asserts on measured time.
 *
 * Network cases follow the existing pattern: this file re-launches itself with the `--broker`
 * argument as a child-process fake broker (stream_socket_server). The consumer list that
 * rebalance reads is served from the JSON file named by RMQ_CONSUMER_MAP: {group: [cid, ...]}.
 */

require_once __DIR__ . '/../bootstrap.php';

// No dynamic name-server lookup may interfere with the unreachable-address producers below.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MessageSelector;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PullStatus;
use RocketMQ\Common\ExpressionType;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Common\PullSysFlag;
use RocketMQ\Common\UtilAll;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\TopicRouteData;
use function RocketMQ\Remoting\Protocol\message_queue_key;

// The drain ceiling is a wall-clock ceiling, so the walk has to spend real time inside one batch
// for it to bind. This is that slice: one fixed spend on the first drained batch, after which the
// stop point is decided by counts alone (3000us is comfortably past the 1ms ceiling).
const WIRE_DRAIN_SLICE_MICROS = 3000;
const WIRE_DRAIN_CEILING_MILLIS = 1;
const WIRE_DRAIN_CHUNK_BATCHES = 16;
const WIRE_DRAIN_MAX_ROUNDS = 64;

// ======================================================================
// child-process fake broker: `php RunClientConsumerWire.php --broker`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    wireBrokerMain();
    exit(0);
}

/**
 * Response script per topic.
 *
 * The selector-driven topics answer their **first** pull with the same two tagged messages
 * (TagA matches a "TagA || TagB" subscription, TagC matches none), so the delivery outcome is
 * decided by the subscription shape and nothing else. The drain topics answer every pull, so the
 * same scenario can be replayed for several consumer groups.
 *
 * @return array{0:int,1:int,2:list<array{body:string,offset:int,tag?:string}>}
 */
function wirePullPlan(string $topic, int $seq, int $reqCode): array
{
    $selectorMsgs = [
        ['body' => 'sel-tag-a', 'offset' => 0, 'tag' => 'TagA'],
        ['body' => 'sel-tag-c', 'offset' => 1, 'tag' => 'TagC'],
    ];
    switch ($topic) {
        // pull consumer: SQL92 / bare tag expression / TAG selector / CLASS_FILTER selector
        case 'QTopic':
        case 'QTopic2':
        case 'QTopic3':
        case 'QTopic4':
            if ($reqCode !== RequestCode::PULL_MESSAGE) {
                return [ResponseCode::PULL_NOT_FOUND, 0, []];
            }
            return $seq === 1
                ? [ResponseCode::SUCCESS, 2, $selectorMsgs]
                : [ResponseCode::PULL_NOT_FOUND, 2, []];

        // lite pull consumer: LITE_PULL_MESSAGE only (the request code itself is part of the claim)
        case 'LSqlTopic':
        case 'LTagTopic':
        case 'LAssignTopic':
        case 'LClassTopic':
        case 'LRawTopic':
            if ($reqCode !== RequestCode::LITE_PULL_MESSAGE) {
                return [ResponseCode::PULL_NOT_FOUND, 0, []];
            }
            return $seq === 1
                ? [ResponseCode::SUCCESS, 2, $selectorMsgs]
                : [ResponseCode::PULL_NOT_FOUND, 2, []];

        // shutdown drain: 21 queued messages -- past one drain chunk, inside the round cap.
        case 'KTopic':
            $msgs = [];
            for ($i = 0; $i < 21; $i++) {
                $msgs[] = ['body' => "drain-$i", 'offset' => $i];
            }
            return [ResponseCode::SUCCESS, 21, $msgs];

        // shutdown drain cap: 32 messages per pull, so the backlog can pass the round cap.
        case 'JTopic':
            $msgs = [];
            for ($i = 0; $i < 32; $i++) {
                $msgs[] = ['body' => "cap-$i", 'offset' => $i];
            }
            return [ResponseCode::SUCCESS, 32, $msgs];

        default:
            return [ResponseCode::PULL_NOT_FOUND, 0, []];
    }
}

function wireLog(array $entry): void
{
    $path = getenv('RMQ_LOG');
    if ($path === false || $path === '') {
        return;
    }
    @file_put_contents($path, json_encode($entry, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
}

/** Reads exactly n bytes; returns null on EOF. */
function wireReadExact($conn, int $n): ?string
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
function wireReadFrame($conn): ?RemotingCommand
{
    $head = wireReadExact($conn, 4);
    if ($head === null) {
        return null;
    }
    $total = RocketMQSerializable::unpackSignedInt($head);
    if ($total <= 0) {
        return null;
    }
    $rest = wireReadExact($conn, $total);
    if ($rest === null) {
        return null;
    }
    return RemotingCommand::decode($head . $rest);
}

/** Route body: one broker with one read/write queue, pointing at the fake broker itself. */
function wireRouteBody(string $addr): string
{
    $qd = new QueueData('b-cons', 1, 1, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', 'b-cons', [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

/** Encodes one message in the store format for a PULL / LITE_PULL response body. */
function wireEncodeMsg(string $topic, string $body, int $queueOffset, ?string $tag = null): string
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

function wireRespond(RemotingCommand $cmd): RemotingCommand
{
    $ext = $cmd->extFields;
    $topic = (string) ($ext['topic'] ?? '');

    switch ($cmd->code) {
        case RequestCode::GET_ROUTEINFO_BY_TOPIC:
            if (str_starts_with($topic, '%RETRY%')) {
                return RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST);
            }
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = wireRouteBody($GLOBALS['WIRE_BROKER_ADDR']);
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

        case RequestCode::QUERY_CONSUMER_OFFSET:
            // Nothing committed yet: the consumer must resolve its own start point.
            return RemotingCommand::createResponseCommand(ResponseCode::QUERY_NOT_FOUND);

        case RequestCode::GET_MAX_OFFSET:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '100';
            return $resp;

        case RequestCode::GET_MIN_OFFSET:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '0';
            return $resp;

        case RequestCode::PULL_MESSAGE:
        case RequestCode::LITE_PULL_MESSAGE: {
            $seq = ($GLOBALS['WIRE_PULL_SEEN'][$topic] = ($GLOBALS['WIRE_PULL_SEEN'][$topic] ?? 0) + 1);
            // What the broker actually received, subscription cells included.
            wireLog([
                'code' => $cmd->code,
                'topic' => $topic,
                'queueId' => (int) ($ext['queueId'] ?? 0),
                'queueOffset' => (int) ($ext['queueOffset'] ?? 0),
                'maxMsgNums' => (int) ($ext['maxMsgNums'] ?? 0),
                'sysFlag' => (int) ($ext['sysFlag'] ?? 0),
                'commitOffset' => (int) ($ext['commitOffset'] ?? 0),
                'suspendTimeoutMillis' => (int) ($ext['suspendTimeoutMillis'] ?? 0),
                'group' => (string) ($ext['consumerGroup'] ?? ''),
                'subscription' => $ext['subscription'] ?? null,
                'expressionType' => $ext['expressionType'] ?? null,
                'subVersion' => isset($ext['subVersion']) ? (int) $ext['subVersion'] : null,
                'seq' => $seq,
            ]);
            [$code, $next, $msgs] = wirePullPlan($topic, $seq, $cmd->code);
            $resp = RemotingCommand::createResponseCommand($code);
            $resp->extFields['nextBeginOffset'] = (string) $next;
            $resp->extFields['minOffset'] = '0';
            $resp->extFields['maxOffset'] = '100';
            $resp->extFields['suggestWhichBrokerId'] = (string) MixAll::MASTER_ID;
            if ($msgs !== []) {
                $body = '';
                foreach ($msgs as $m) {
                    $body .= wireEncodeMsg($topic, $m['body'], $m['offset'], $m['tag'] ?? null);
                }
                $resp->body = $body;
            }
            return $resp;
        }

        case RequestCode::UPDATE_CONSUMER_OFFSET:
            wireLog([
                'code' => $cmd->code,
                'topic' => $topic,
                'commitOffset' => (int) ($ext['commitOffset'] ?? -1),
                'group' => (string) ($ext['consumerGroup'] ?? ''),
            ]);
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);

        default:
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

function wireServeConn($conn): void
{
    stream_set_blocking($conn, true);
    while (($cmd = wireReadFrame($conn)) !== null) {
        try {
            $resp = wireRespond($cmd);
        } catch (\Throwable $e) {
            $resp = RemotingCommand::createResponseCommand(
                ResponseCode::SYSTEM_ERROR,
                'broker error: ' . $e->getMessage(),
            );
        }
        $resp->opaque = $cmd->opaque;
        @fwrite($conn, $resp->encode());
    }
}

function wireBrokerMain(): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "broker: cannot listen: {$errstr}\n");
        return;
    }
    $GLOBALS['WIRE_BROKER_ADDR'] = (string) stream_socket_get_name($server, false);
    $GLOBALS['WIRE_PULL_SEEN'] = [];
    fwrite(STDOUT, $GLOBALS['WIRE_BROKER_ADDR'] . "\n");
    fflush(STDOUT);

    while (true) {
        $conn = @stream_socket_accept($server, 30);
        if ($conn === false) {
            break;
        }
        wireServeConn($conn);
        @fclose($conn);
    }
    @fclose($server);
}

/**
 * Child-process fake broker handle: prints its listening address on the first stdout line, then
 * answers frames; every pull it receives lands in the JSON-lines log named by RMQ_LOG.
 */
final class WireFakeBroker
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
            $err = (string) (stream_get_contents($pipes[2]) ?: '');
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
// listeners
// ======================================================================

/**
 * Counts every batch handed to it, so "how much of the backlog shutdown walked" is a number.
 *
 * With {@see WireDrainListener::$firstBatchMicros} set, the first batch of the drain phase spends
 * a fixed slice of wall time: that is what makes a positive ceiling bind after exactly one round
 * instead of leaving the stop point up to machine speed. The assertions read batch counts.
 */
final class WireDrainListener implements MessageListenerConcurrently
{
    public int $batches = 0;

    /** @var list<string> */
    public array $bodies = [];

    public bool $drainPhase = false;

    public int $firstBatchMicros = 0;

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        $this->batches++;
        foreach ($msgs as $m) {
            $this->bodies[] = (string) $m->getBody();
        }
        if ($this->drainPhase && $this->firstBatchMicros > 0) {
            $micros = $this->firstBatchMicros;
            $this->firstBatchMicros = 0;
            usleep($micros);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

// ======================================================================
// suite
// ======================================================================

final class RunClientConsumerWire
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

    /** The pull frame a topic/group pair put on the wire (null when nothing was written). */
    private function frame(int $code, string $topic, string $group, int $index = 0): ?array
    {
        $rows = $this->logWhere($code, $topic, $group);
        return $rows[$index] ?? null;
    }

    private function pullConsumer(DefaultMQPullConsumer|DefaultLitePullConsumer $c): void
    {
        $c->shutdown();
        if ($c->clientId !== null) {
            MQClientInstance::removeInstance($c->clientId);
        }
    }

    // ================================================================ 1. pull wire carries the subscription's own type

    /**
     * The four pull entry points all report the SubscriptionData they were given: the expression
     * text, its expression type and its version stamp travel together, and the class-filter bit
     * follows the subscription type. The tag shape is the paired control: it keeps the compiled
     * tag text, TAG as the type and version 0 -- the shape it has always put on the wire.
     */
    private function testPullSubscriptionTypesOnWire(WireFakeBroker $broker): void
    {
        $c = new DefaultMQPullConsumer('GID-Q-WIRE');
        $c->setInstanceName('WIRE-Q');
        $c->setNameServerAddresses([$broker->addr]);
        $c->start();
        $this->check($c->mqClient !== null, 'pull consumer instance ready');

        // ---- SQL92: verbatim expression, SQL92 type, the subscription's own version stamp ----
        $mq = $c->fetchSubscribeMessageQueues('QTopic')[0] ?? null;
        $this->check($mq !== null, 'QTopic queue resolved');
        if ($mq !== null) {
            $t0 = UtilAll::currentTimeMillis();
            $r = $c->pullBySelector($mq, MessageSelector::bySql('a > 10'), 0, 32);
            $t1 = UtilAll::currentTimeMillis();
            $this->checkSame(PullStatus::FOUND, $r->status, 'SQL92 pull finds the scripted messages');
            $this->loadLog($GLOBALS['WIRE_LOG']);
            $f = $this->frame(RequestCode::PULL_MESSAGE, 'QTopic', 'GID-Q-WIRE');
            $this->check($f !== null, 'SQL92 pull reached the broker');
            if ($f !== null) {
                $this->checkSame('a > 10', $f['subscription'], 'SQL92 expression reaches the wire verbatim');
                $this->checkSame(ExpressionType::SQL92, $f['expressionType'], 'SQL92 subscription is reported as SQL92');
                $this->checkSame(true, (int) $f['subVersion'] > 0, 'SQL92 pull carries a non-zero version stamp');
                $this->checkSame(true, (int) $f['subVersion'] >= $t0, 'SQL92 version stamp is the subscription it just built');
                $this->checkSame(true, (int) $f['subVersion'] <= $t1, 'SQL92 version stamp is not a stale value');
                $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION, (int) $f['sysFlag'],
                    'SQL92 short poll: subscription bit on, class-filter and suspend bits off');
                $this->checkSame(0, (int) $f['sysFlag'] & PullSysFlag::FLAG_CLASS_FILTER,
                    'SQL92 does not set the class-filter bit');
                $this->checkSame(['sel-tag-a', 'sel-tag-c'],
                    array_map(static fn(MessageExt $m): string => (string) $m->getBody(), $r->msgFoundList),
                    'SQL92 result is not re-filtered by a tag set');
            }

            // The long-polling sibling shares one body: same three cells, suspend bit added.
            $t2 = UtilAll::currentTimeMillis();
            $rb = $c->pullBlockIfNotFoundBySelector($mq, MessageSelector::bySql('a > 10'), 0, 32);
            $t3 = UtilAll::currentTimeMillis();
            $this->checkSame(PullStatus::NO_NEW_MSG, $rb->status, 'long-poll SQL92 reads the empty second round');
            $this->loadLog($GLOBALS['WIRE_LOG']);
            $f2 = $this->frame(RequestCode::PULL_MESSAGE, 'QTopic', 'GID-Q-WIRE', 1);
            $this->check($f2 !== null, 'long-poll SQL92 pull reached the broker');
            if ($f2 !== null) {
                $this->checkSame('a > 10', $f2['subscription'] ?? null, 'long-poll keeps the SQL92 expression verbatim');
                $this->checkSame(ExpressionType::SQL92, $f2['expressionType'] ?? null, 'long-poll keeps the SQL92 type');
                $this->checkSame(true, (int) ($f2['subVersion'] ?? 0) >= $t2 && (int) ($f2['subVersion'] ?? 0) <= $t3,
                    'long-poll version stamp follows the same rule');
                $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_SUSPEND,
                    (int) ($f2['sysFlag'] ?? 0), 'long-poll SQL92 adds only the suspend bit');
            }
        }

        // ---- CLASS_FILTER: its own expression type on the wire, expression kept as it stands ----
        $mq4 = $c->fetchSubscribeMessageQueues('QTopic4')[0] ?? null;
        if ($mq4 !== null) {
            $t4 = UtilAll::currentTimeMillis();
            $r4 = $c->pullBySelector($mq4, new MessageSelector(ExpressionType::CLASS_FILTER, 'MyClassFilter'), 0, 32);
            $t5 = UtilAll::currentTimeMillis();
            $this->loadLog($GLOBALS['WIRE_LOG']);
            $f = $this->frame(RequestCode::PULL_MESSAGE, 'QTopic4', 'GID-Q-WIRE');
            $this->check($f !== null, 'class-filter pull reached the broker');
            if ($f !== null) {
                $this->checkSame('MyClassFilter', $f['subscription'], 'class-filter expression reaches the wire verbatim');
                $this->checkSame(ExpressionType::CLASS_FILTER, $f['expressionType'], 'class-filter subscription is reported as CLASS_FILTER');
                $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION, (int) $f['sysFlag'],
                    'class-filter subscription leaves the class-filter sysFlag bit off');
                $this->checkSame(true, (int) $f['subVersion'] >= $t4 && (int) $f['subVersion'] <= $t5,
                    'class-filter pull carries its own version stamp');
                $this->checkSame(['sel-tag-a', 'sel-tag-c'],
                    array_map(static fn(MessageExt $m): string => (string) $m->getBody(), $r4->msgFoundList),
                    'class-filter result is not re-filtered by a tag set');
            }
        }

        // ---- CONTROL: a TAG selector keeps the historic shape (compiled text, TAG, version 0) ----
        $mq3 = $c->fetchSubscribeMessageQueues('QTopic3')[0] ?? null;
        $mq2 = $c->fetchSubscribeMessageQueues('QTopic2')[0] ?? null;
        if ($mq3 !== null && $mq2 !== null) {
            $rt = $c->pullBySelector($mq3, MessageSelector::byTag('TagA || TagB'), 0, 32);
            $rb2 = $c->pull($mq2, 'TagA || TagB', 0, 32);
            $this->loadLog($GLOBALS['WIRE_LOG']);
            $fTag = $this->frame(RequestCode::PULL_MESSAGE, 'QTopic3', 'GID-Q-WIRE');
            $fBare = $this->frame(RequestCode::PULL_MESSAGE, 'QTopic2', 'GID-Q-WIRE');
            $this->check($fTag !== null && $fBare !== null, 'tag pulls reached the broker');
            if ($fTag !== null) {
                $this->checkSame('TagA || TagB', $fTag['subscription'],
                    'tag subscription reports the text the tag parser compiled');
                $this->checkSame(ExpressionType::TAG, $fTag['expressionType'], 'tag subscription is reported as TAG');
                $this->checkSame(0, (int) $fTag['subVersion'], 'tag subscription keeps the historic version 0');
                $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION, (int) $fTag['sysFlag'],
                    'tag subscription keeps the class-filter bit off');
                $this->checkSame(['sel-tag-a', 'sel-tag-c'],
                    array_map(static fn(MessageExt $m): string => (string) $m->getBody(), $rt->msgFoundList),
                    'a tag request on the pull entry point reports its shape, it does not re-filter the batch');
            }
            if ($fTag !== null && $fBare !== null) {
                $this->checkSame(
                    [$fTag['subscription'], $fTag['expressionType'], $fTag['subVersion'], $fTag['sysFlag']],
                    [$fBare['subscription'], $fBare['expressionType'], $fBare['subVersion'], $fBare['sysFlag']],
                    'the tag selector and the bare tag expression put the same subscription on the wire'
                );
                $this->checkSame(['sel-tag-a', 'sel-tag-c'],
                    array_map(static fn(MessageExt $m): string => (string) $m->getBody(), $rb2->msgFoundList),
                    'the bare tag expression hands the same batch over: filtering is the caller\'s job here');
            }
            // SUB_ALL keeps the TAG shape too: the wildcard is a tag subscription with no tags.
            $c->pull($mq2, '*', 0, 32);
            $this->loadLog($GLOBALS['WIRE_LOG']);
            $fAll = $this->frame(RequestCode::PULL_MESSAGE, 'QTopic2', 'GID-Q-WIRE', 1);
            $this->checkSame(['*', ExpressionType::TAG, 0],
                [$fAll['subscription'] ?? null, $fAll['expressionType'] ?? null, (int) ($fAll['subVersion'] ?? -1)],
                'SUB_ALL keeps the historic tag shape');

            // An unusable subscription never reaches the wire.
            $before = count($this->logWhere(RequestCode::PULL_MESSAGE, 'QTopic', 'GID-Q-WIRE'));
            $message = null;
            try {
                $c->pull($mq, '||', 0, 32);
            } catch (MQClientException $e) {
                $message = $e->getMessage();
            }
            $this->checkSame('parse subscription error: subString split error', $message,
                'a tag expression the parser rejects is reported as a subscription error');
            $this->loadLog($GLOBALS['WIRE_LOG']);
            $this->checkSame($before, count($this->logWhere(RequestCode::PULL_MESSAGE, 'QTopic', 'GID-Q-WIRE')),
                'a rejected tag expression writes no pull frame');

            $message = null;
            try {
                $c->pullBySelector($mq, new MessageSelector(ExpressionType::SQL92, ''), 0, 32);
            } catch (\ValueError $e) {
                $message = $e->getMessage();
            }
            $this->checkSame("Expression can't be null! SQL92", $message,
                'a non-tag selector without an expression is refused');
            $this->loadLog($GLOBALS['WIRE_LOG']);
            $this->checkSame($before, count($this->logWhere(RequestCode::PULL_MESSAGE, 'QTopic', 'GID-Q-WIRE')),
                'a refused selector writes no pull frame');
        }

        $this->pullConsumer($c);
    }

    // ================================================================ 2. the same rule on the lite pull path

    /**
     * pullOne() builds its frame from the topic's stored subscription table, so the subscription
     * a caller registered is what the broker reads back -- expression, type and version. Assign
     * mode is used for the deterministic first frame; subscribe mode proves the same shape once
     * rebalance has handed out the queue.
     */
    private function testLitePullSubscriptionTypesOnWire(WireFakeBroker $broker): void
    {
        $lite = new DefaultLitePullConsumer('GID-L-ASSIGN');
        $lite->setInstanceName('WIRE-LA');
        $lite->setNamesrvAddr($broker->addr);
        $lite->consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET;
        $lite->assign([
            new MessageQueue('LAssignTopic', 'b-cons', 0),
            new MessageQueue('LClassTopic', 'b-cons', 0),
            new MessageQueue('LRawTopic', 'b-cons', 0),
        ]);
        $lite->setSubExpressionForAssignWithSelector('LAssignTopic', MessageSelector::bySql('a > 10'));
        $lite->setSubExpressionForAssignWithSelector('LClassTopic', new MessageSelector(ExpressionType::CLASS_FILTER, 'MyClassFilter'));
        $lite->setSubExpressionForAssign('LRawTopic', 'TagA || TagB');
        $lite->start();
        $this->check($lite->isRunning(), 'assign-mode lite consumer running');

        $lite->tick();
        $this->loadLog($GLOBALS['WIRE_LOG']);

        // SQL92 on the lite path.
        $sqlStored = $lite->subscriptionData['LAssignTopic'] ?? null;
        $this->check($sqlStored !== null, 'SQL92 subscription stored for the assigned topic');
        $f = $this->frame(RequestCode::LITE_PULL_MESSAGE, 'LAssignTopic', 'GID-L-ASSIGN');
        $this->check($f !== null, 'lite SQL92 pull reached the broker');
        if ($f !== null && $sqlStored !== null) {
            $this->checkSame('a > 10', $f['subscription'], 'lite SQL92 expression reaches the wire verbatim');
            $this->checkSame(ExpressionType::SQL92, $f['expressionType'], 'lite SQL92 subscription is reported as SQL92');
            $this->checkSame($sqlStored->subVersion, (int) $f['subVersion'],
                'lite SQL92 pull reports the stored subscription version');
            $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_LITE_PULL_MESSAGE, (int) $f['sysFlag'],
                'lite SQL92 sysFlag: subscription + lite bits, class-filter off');
            $this->checkSame(0, (int) $f['sysFlag'] & PullSysFlag::FLAG_SUSPEND, 'lite pull never sets the suspend bit');
        }

        // CLASS_FILTER on the lite path.
        $clsStored = $lite->subscriptionData['LClassTopic'] ?? null;
        $f = $this->frame(RequestCode::LITE_PULL_MESSAGE, 'LClassTopic', 'GID-L-ASSIGN');
        if ($f !== null && $clsStored !== null) {
            $this->checkSame('MyClassFilter', $f['subscription'], 'lite class-filter expression reaches the wire verbatim');
            $this->checkSame(ExpressionType::CLASS_FILTER, $f['expressionType'], 'lite class-filter subscription is reported as CLASS_FILTER');
            $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_LITE_PULL_MESSAGE,
                (int) $f['sysFlag'], 'lite class-filter subscription leaves the class-filter sysFlag bit off');
            $this->checkSame($clsStored->subVersion, (int) $f['subVersion'],
                'lite class-filter pull reports the stored subscription version');
        } else {
            $this->check(false, 'lite class-filter pull and stored subscription both present');
        }

        // CONTROL: the tag subscription keeps the historic shape on the lite path as well.
        $tagStored = $lite->subscriptionData['LRawTopic'] ?? null;
        $f = $this->frame(RequestCode::LITE_PULL_MESSAGE, 'LRawTopic', 'GID-L-ASSIGN');
        if ($f !== null && $tagStored !== null) {
            $this->checkSame('TagA || TagB', $f['subscription'],
                'lite tag subscription reports the text the tag parser compiled');
            $this->checkSame(ExpressionType::TAG, $f['expressionType'], 'lite tag subscription is reported as TAG');
            $this->checkSame(0, (int) $f['subVersion'], 'lite tag subscription keeps the historic version 0');
            $this->checkSame(true, $tagStored->subVersion > 0,
                'the stored tag subscription does hold a version: only the wire keeps 0');
            $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_LITE_PULL_MESSAGE, (int) $f['sysFlag'],
                'lite tag sysFlag keeps the class-filter bit off');
        } else {
            $this->check(false, 'lite tag pull and stored subscription both present');
        }

        // Delivery follows the same split: only a tag subscription re-filters locally.
        $delivered = [];
        foreach ($lite->poll(500) as $m) {
            $delivered[$m->topic][] = (string) $m->getBody();
        }
        $this->checkSame(['sel-tag-a', 'sel-tag-c'], $delivered['LAssignTopic'] ?? [],
            'lite SQL92 batch is handed over whole');
        $this->checkSame(['sel-tag-a', 'sel-tag-c'], $delivered['LClassTopic'] ?? [],
            'lite class-filter batch is handed over whole');
        $this->checkSame(['sel-tag-a'], $delivered['LRawTopic'] ?? [],
            'lite tag batch is still re-filtered by tag string');
        $this->pullConsumer($lite);

        // ---- subscribe mode: the same table is what reaches the wire ----
        $sub = new DefaultLitePullConsumer('GID-L-SUB');
        $sub->setInstanceName('WIRE-LS');
        $sub->setNamesrvAddr($broker->addr);
        $sub->consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET;
        $sub->subscribeWithSelector('LSqlTopic', MessageSelector::bySql('a > 10'));
        $sub->subscribe('LTagTopic', 'TagA || TagB');
        $sub->start();
        for ($i = 0; $i < 6; $i++) {
            $sub->tick();
            $this->loadLog($GLOBALS['WIRE_LOG']);
            if ($this->frame(RequestCode::LITE_PULL_MESSAGE, 'LSqlTopic', 'GID-L-SUB') !== null
                && $this->frame(RequestCode::LITE_PULL_MESSAGE, 'LTagTopic', 'GID-L-SUB') !== null) {
                break;
            }
        }
        $fSql = $this->frame(RequestCode::LITE_PULL_MESSAGE, 'LSqlTopic', 'GID-L-SUB');
        $fTag = $this->frame(RequestCode::LITE_PULL_MESSAGE, 'LTagTopic', 'GID-L-SUB');
        $this->check($fSql !== null && $fTag !== null, 'subscribe-mode lite pulls reached the broker');
        if ($fSql !== null) {
            $this->checkSame('a > 10', $fSql['subscription'], 'subscribe-mode SQL92 expression reaches the wire verbatim');
            $this->checkSame(ExpressionType::SQL92, $fSql['expressionType'], 'subscribe-mode SQL92 subscription is reported as SQL92');
            $this->checkSame($sub->subscriptionData['LSqlTopic']->subVersion, (int) $fSql['subVersion'],
                'subscribe-mode SQL92 pull reports the stored subscription version');
        }
        if ($fTag !== null) {
            $this->checkSame('TagA || TagB', $fTag['subscription'], 'subscribe-mode tag subscription reports the compiled text');
            $this->checkSame(ExpressionType::TAG, $fTag['expressionType'], 'subscribe-mode tag subscription is reported as TAG');
            $this->checkSame(0, (int) $fTag['subVersion'], 'subscribe-mode tag subscription keeps the historic version 0');
            $this->checkSame(true, $sub->subscriptionData['LTagTopic']->subVersion > 0,
                'the version 0 on the wire is a rule of the pull path, not an empty field');
        }
        $this->pullConsumer($sub);
    }

    // ================================================================ 3. shutdown drain ceiling

    /**
     * Configuration surface, asserted without any network: the knob defaults to "no extra
     * ceiling", the setter stores what it is given (including a negative value, which reads as
     * the same "no ceiling" as 0), and the drain's own bounds are fixed constants.
     */
    private function testDrainConfigurationUnit(): void
    {
        $c = new DefaultMQPushConsumer('GID-DRAIN-UNIT');
        $this->checkSame(0, $c->awaitTerminationMillisWhenShutdown,
            'awaitTerminationMillisWhenShutdown defaults to 0 (no extra ceiling)');
        $this->checkSame(0, $c->getAwaitTerminationMillisWhenShutdown(), 'getter matches the default field');
        $this->checkSame(1000, $c->dispatchBatchesPerTick, 'dispatchBatchesPerTick defaults to 1000');

        $c->setAwaitTerminationMillisWhenShutdown(1500);
        $this->checkSame(1500, $c->getAwaitTerminationMillisWhenShutdown(), 'setter/getter roundtrip');
        $this->checkSame(1500, $c->awaitTerminationMillisWhenShutdown, 'setter writes the public field');
        $c->setAwaitTerminationMillisWhenShutdown(0);
        $this->checkSame(0, $c->getAwaitTerminationMillisWhenShutdown(), '0 restores the no-ceiling default');
        $c->setAwaitTerminationMillisWhenShutdown(-1);
        $this->checkSame(-1, $c->getAwaitTerminationMillisWhenShutdown(),
            'the setter stores a negative value as given (it reads as no ceiling)');

        $consts = (new ReflectionClass(DefaultMQPushConsumer::class))->getConstants();
        $this->checkSame(64, $consts['SHUTDOWN_DRAIN_MAX_ROUNDS'] ?? null, 'drain round cap is 64 rounds');
        $this->checkSame(16, $consts['SHUTDOWN_DRAIN_CHUNK_BATCHES'] ?? null, 'drain walks 16 batches per round');
    }

    /**
     * A queue of batches plus a started consumer: dispatch hands over one batch per tick so the
     * backlog survives the ticks, and the pull script answers every round so several groups can
     * replay the same scenario.
     *
     * @return array{0:DefaultMQPushConsumer,1:WireDrainListener,2:MessageQueue}
     */
    private function makeDrainConsumer(
        WireFakeBroker $broker,
        string $group,
        string $instance,
        string $topic,
        int $threshold,
    ): array {
        $rec = new WireDrainListener();
        $c = new DefaultMQPushConsumer($group);
        $c->setInstanceName($instance);
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener($rec);
        $c->subscribe($topic);
        $c->consumeMessageBatchMaxSize = 1;
        $c->dispatchBatchesPerTick = 1;
        $c->pullThresholdForQueue = $threshold;
        $c->start();
        $mq = new MessageQueue($topic, 'b-cons', 0);
        return [$c, $rec, $mq];
    }

    /** Queued-but-not-yet-dispatched messages of one queue, read from the running info. */
    private function queuedCount(DefaultMQPushConsumer $c, MessageQueue $mq): int
    {
        $row = $c->consumerRunningInfo()->mqTable[message_queue_key($mq)] ?? null;
        return $row === null ? 0 : (int) ($row['cachedMsgCount'] ?? 0);
    }

    /**
     * Tick until the backlog is at least `$want` batches deep. One tick pulls a full script and
     * dispatches exactly one batch, so the count only moves in steps the drain contract allows.
     */
    private function buildBacklog(DefaultMQPushConsumer $c, MessageQueue $mq, int $want, int $maxTicks): int
    {
        $queued = $this->queuedCount($c, $mq);
        for ($i = 0; $i < $maxTicks && $queued < $want; $i++) {
            $c->tick();
            $queued = $this->queuedCount($c, $mq);
        }
        return $queued;
    }

    /**
     * A positive awaitTerminationMillisWhenShutdown is a ceiling on the one drain shutdown does:
     * it must bind in the middle of a backlog, so a small positive value leaves batches queued.
     */
    private function testShutdownDrainCeilingBinds(WireFakeBroker $broker): void
    {
        [$c, $rec, $mq] = $this->makeDrainConsumer($broker, 'GID-D-CEIL', 'WIRE-D1', 'KTopic', 1000);
        $backlog = $this->buildBacklog($c, $mq, 20, 6);
        $this->check($backlog >= WIRE_DRAIN_CHUNK_BATCHES + 1,
            "backlog is past one drain chunk before shutdown ($backlog batches queued)");
        $dispatchedBefore = $rec->batches;

        $c->setAwaitTerminationMillisWhenShutdown(WIRE_DRAIN_CEILING_MILLIS);
        $rec->drainPhase = true;
        $rec->firstBatchMicros = WIRE_DRAIN_SLICE_MICROS;
        $c->shutdown();

        $dispatched = $rec->batches - $dispatchedBefore;
        $left = $this->queuedCount($c, $mq);
        $this->checkSame(WIRE_DRAIN_CHUNK_BATCHES, $dispatched,
            'a 1ms ceiling stops the drain after the first chunk of batches');
        $this->checkSame($backlog - WIRE_DRAIN_CHUNK_BATCHES, $left,
            'the batches the ceiling did not reach stay queued');
        $this->check($left > 0, 'the ceiling binds mid-backlog instead of after the whole backlog');
        $this->checkSame($rec->batches, count($rec->bodies),
            'every batch the drain walked carried exactly one message');
    }

    /**
     * Control: with the knob unset (and with a non-positive value) the drain keeps its existing
     * budget -- the whole backlog is walked, so the ceiling is the only thing that stopped the
     * run above.
     */
    private function testShutdownDrainWithoutCeiling(WireFakeBroker $broker): void
    {
        foreach ([['GID-D-DEFAULT', 'WIRE-D2', 0], ['GID-D-NEGATIVE', 'WIRE-D3', -1]] as [$group, $instance, $knob]) {
            [$c, $rec, $mq] = $this->makeDrainConsumer($broker, $group, $instance, 'KTopic', 1000);
            $backlog = $this->buildBacklog($c, $mq, 20, 6);
            $this->check($backlog >= WIRE_DRAIN_CHUNK_BATCHES + 1,
                "backlog is past one drain chunk before shutdown ($backlog batches queued, knob=$knob)");
            $dispatchedBefore = $rec->batches;

            if ($knob !== 0) {
                $c->setAwaitTerminationMillisWhenShutdown($knob);
            }
            $rec->drainPhase = true;
            $rec->firstBatchMicros = WIRE_DRAIN_SLICE_MICROS;
            $c->shutdown();

            $dispatched = $rec->batches - $dispatchedBefore;
            $this->checkSame($backlog, $dispatched,
                "knob=$knob reads as no ceiling: the drain walks the whole backlog");
            $this->checkSame(0, $this->queuedCount($c, $mq),
                "knob=$knob leaves nothing queued");
        }
    }

    /**
     * The drain budget without any ceiling is still bounded by the round cap: past 64 x 16
     * batches the walk stops by itself, so a listener that keeps accepting batches cannot make
     * shutdown run forever. This pins both bounds of that product.
     */
    private function testShutdownDrainRoundCap(WireFakeBroker $broker): void
    {
        [$c, $rec, $mq] = $this->makeDrainConsumer($broker, 'GID-D-CAP', 'WIRE-D4', 'JTopic', 4000);
        $backlog = $this->buildBacklog($c, $mq, 1100, 12);
        $cap = WIRE_DRAIN_MAX_ROUNDS * WIRE_DRAIN_CHUNK_BATCHES;
        $this->check($backlog > $cap, "backlog is past the drain round cap before shutdown ($backlog batches queued)");
        $dispatchedBefore = $rec->batches;

        $rec->drainPhase = true;
        $c->shutdown();

        $dispatched = $rec->batches - $dispatchedBefore;
        $this->checkSame($cap, $dispatched, 'with no ceiling the drain stops at the round cap');
        $this->checkSame($backlog - $cap, $this->queuedCount($c, $mq),
            'batches past the round cap stay queued for the broker to redeliver');
    }

    // ================================================================ run

    public function run(): int
    {
        $this->testDrainConfigurationUnit();

        $tmpDir = sys_get_temp_dir() . '/rmq-wire-test-' . getmypid();
        if (!is_dir($tmpDir)) {
            @mkdir($tmpDir, 0777, true);
        }
        $logPath = $tmpDir . '/broker.jsonl';
        $groupMapPath = $tmpDir . '/groups.json';
        @unlink($logPath);
        putenv("RMQ_LOG={$logPath}");
        $GLOBALS['WIRE_LOG'] = $logPath;
        // Each drain group holds exactly one member, so rebalance hands every queue to it.
        $groupMap = [
            'GID-D-CEIL' => [MixAll::clientIdFor('WIRE-D1', null, false)],
            'GID-D-DEFAULT' => [MixAll::clientIdFor('WIRE-D2', null, false)],
            'GID-D-NEGATIVE' => [MixAll::clientIdFor('WIRE-D3', null, false)],
            'GID-D-CAP' => [MixAll::clientIdFor('WIRE-D4', null, false)],
        ];
        file_put_contents($groupMapPath, json_encode($groupMap));
        putenv("RMQ_CONSUMER_MAP={$groupMapPath}");

        $broker = WireFakeBroker::start();
        try {
            $this->testPullSubscriptionTypesOnWire($broker);
            $this->testLitePullSubscriptionTypesOnWire($broker);
            $this->testShutdownDrainCeilingBinds($broker);
            $this->testShutdownDrainWithoutCeiling($broker);
            $this->testShutdownDrainRoundCap($broker);
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

exit((new RunClientConsumerWire())->run());
