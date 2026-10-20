<?php

declare(strict_types=1);

/**
 * Asynchronous pull and per-queue scheduled pull contract suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientPullSchedule.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Everything is asserted on **recorded state**:
 *  - the pull frames that actually reached the socket (read back from the JSON-lines log the fake
 *    broker writes), and
 *  - the callbacks that fired, with the decoded result or error each carried.
 * Nothing here waits on the wall clock: the scheduled service is advanced by handing later clock
 * values to tick(), and every pump loop is gated on the state it is waiting for.
 *
 * Network cases follow this port's existing pattern: this file re-launches itself with the
 * `--broker` argument as a child-process fake endpoint (stream_socket_server) that answers both
 * the name-server requests (routes) and the broker requests (pulls, heartbeats, offsets).
 */

require_once __DIR__ . '/../bootstrap.php';

// No dynamic name-server lookup may interfere with the fake endpoint below.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\MQPullConsumerScheduleService;
use RocketMQ\Client\PullCallback;
use RocketMQ\Client\PullRequest;
use RocketMQ\Client\PullResult;
use RocketMQ\Client\PullScheduleQueueListener;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\PullTaskContext;
use RocketMQ\Client\PullTaskCallback;
use RocketMQ\Client\MessageSelector;
use RocketMQ\Common\ExpressionType;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Common\PullSysFlag;
use RocketMQ\Common\UtilAll;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\MessageModel;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\TopicRouteData;

const SCH_BROKER_NAME = 'b-sch';

// The six cells that make a pull frame: any change to how the async request is built shows up here.
const SCH_SHAPE_CELLS = [
    'code', 'topic', 'queueId', 'queueOffset', 'maxMsgNums', 'sysFlag', 'commitOffset',
    'subscription', 'expressionType', 'subVersion', 'maxMsgBytes', 'requestSource', 'group',
];

// ======================================================================
// child-process fake endpoint: `php RunClientPullSchedule.php --broker`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    schBrokerMain();
    exit(0);
}

/**
 * Response script per topic, indexed by the pull number that topic has already seen.
 *
 * @return array{0:int,1:int,2:int,3:int,4:list<array{body:string,offset:int,tag?:string}>}
 *         [response code, nextBeginOffset, minOffset, maxOffset, messages]
 */
function schPlan(string $topic, int $seq): array
{
    switch ($topic) {
        // Async short poll: the first round answers with two messages, then stays empty.
        case 'AsyncFound':
            return $seq === 1
                ? [ResponseCode::SUCCESS, 3, 0, 50, [['body' => 'a-1', 'offset' => 0, 'tag' => 'TagA'], ['body' => 'a-2', 'offset' => 1, 'tag' => 'TagC']]]
                : [ResponseCode::PULL_NOT_FOUND, 3, 0, 50, []];

        // Async SQL92 and its synchronous twin: same script, so the two frames must match.
        case 'AsyncSql':
        case 'SyncSql':
            return [ResponseCode::SUCCESS, 1, 0, 9, [['body' => 'sql-1', 'offset' => 0]]];

        // Long-polling async pull: always empty, so only the request shape is under test.
        case 'AsyncBlock':
            return [ResponseCode::PULL_NOT_FOUND, 0, 0, 0, []];

        // A broker error code: the request must land on the exception side of the callback.
        case 'AsyncErr':
            return [ResponseCode::SYSTEM_ERROR, 0, 0, 0, []];

        // Offset cursor walk for the scheduled service (see the expected wire offsets below).
        case 'OffsetTopic':
            $plans = [
                1 => [ResponseCode::SUCCESS, 2, 0, 100, [['body' => 'o-0', 'offset' => 0], ['body' => 'o-1', 'offset' => 1]]],
                2 => [ResponseCode::PULL_NOT_FOUND, 2, 0, 100, []],
                3 => [ResponseCode::PULL_OFFSET_MOVED, 8, 8, 100, []],
                4 => [ResponseCode::PULL_NOT_FOUND, 99, 0, 100, []],
                5 => [ResponseCode::PULL_NOT_FOUND, 1, 9, 100, []],
                6 => [ResponseCode::SUCCESS, 2, 0, 100, [['body' => 'o-1', 'offset' => 1]]],
            ];
            // Any later round is consumable and names an offset no scripted round uses, so the wire
            // records show exactly which round answered.
            return $plans[$seq] ?? [ResponseCode::SUCCESS, 100 + $seq, 0, 100, []];

        // Nothing ever consumable: the cursor is inside the bounds, so it stays put and the queue
        // keeps being reported as not pulling well.
        case 'StaleTopic':
            return [ResponseCode::PULL_NOT_FOUND, 0, 0, 0, []];

        // The queue-view topics: a consumable round, so every task's cursor moves once.
        case 'TwoQueueTopic':
        case 'PairTopic':
            return [ResponseCode::SUCCESS, 1, 0, 100, []];

        // The paired control: a consumable queue stays healthy across the same clock jump.
        case 'HealthyTopic':
            return [ResponseCode::SUCCESS, $seq, 0, 100, []];

        // Queue-view topics: the body never pulls, so the answer only needs to be harmless.
        default:
            return [ResponseCode::PULL_NOT_FOUND, 0, 0, 0, []];
    }
}

function schLog(array $entry): void
{
    $path = getenv('RMQ_LOG');
    if ($path === false || $path === '') {
        return;
    }
    @file_put_contents($path, json_encode($entry, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
}

/** One broker; the two queue-view topics carry two queues, everything else one. */
function schRouteBody(string $addr, string $topic): string
{
    $nums = in_array($topic, ['TwoQueueTopic', 'PairTopic'], true) ? 2 : 1;
    $qd = new QueueData(SCH_BROKER_NAME, $nums, $nums, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', SCH_BROKER_NAME, [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

function schEncodeMsg(string $topic, string $body, int $queueOffset, ?string $tag = null): string
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

function schRespond(RemotingCommand $cmd): RemotingCommand
{
    $ext = $cmd->extFields;
    $topic = (string) ($ext['topic'] ?? '');

    switch ($cmd->code) {
        case RequestCode::GET_ROUTEINFO_BY_TOPIC:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = schRouteBody($GLOBALS['SCH_ADDR'], $topic);
            return $resp;

        case RequestCode::GET_CONSUMER_LIST_BY_GROUP:
            // No peer is reported, so the balance view of this port falls back to the queues it
            // actually pulled from -- which is exactly what the booking assertions read.
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = json_encode(['consumerIdList' => []]);
            return $resp;

        case RequestCode::PULL_MESSAGE:
        case RequestCode::LITE_PULL_MESSAGE: {
            $seq = ($GLOBALS['SCH_SEEN'][$topic] = ($GLOBALS['SCH_SEEN'][$topic] ?? 0) + 1);
            // Every cell the request really carried, keyed by the shape list above.
            schLog([
                'code' => $cmd->code,
                'topic' => $topic,
                'queueId' => (int) ($ext['queueId'] ?? 0),
                'queueOffset' => (int) ($ext['queueOffset'] ?? 0),
                'maxMsgNums' => (int) ($ext['maxMsgNums'] ?? 0),
                'sysFlag' => (int) ($ext['sysFlag'] ?? 0),
                'commitOffset' => (int) ($ext['commitOffset'] ?? 0),
                'suspendTimeoutMillis' => (int) ($ext['suspendTimeoutMillis'] ?? 0),
                'subscription' => $ext['subscription'] ?? null,
                'expressionType' => $ext['expressionType'] ?? null,
                'subVersion' => isset($ext['subVersion']) ? (int) $ext['subVersion'] : null,
                'maxMsgBytes' => isset($ext['maxMsgBytes']) ? (int) $ext['maxMsgBytes'] : null,
                'requestSource' => isset($ext['requestSource']) ? (int) $ext['requestSource'] : null,
                'group' => (string) ($ext['consumerGroup'] ?? ''),
                'seq' => $seq,
            ]);
            [$code, $next, $min, $max, $msgs] = schPlan($topic, $seq);
            $resp = RemotingCommand::createResponseCommand($code);
            if ($code === ResponseCode::SYSTEM_ERROR) {
                $resp->remark = 'broker says no';
            }
            $resp->extFields['nextBeginOffset'] = (string) $next;
            $resp->extFields['minOffset'] = (string) $min;
            $resp->extFields['maxOffset'] = (string) $max;
            $resp->extFields['suggestWhichBrokerId'] = (string) MixAll::MASTER_ID;
            if ($msgs !== []) {
                $body = '';
                foreach ($msgs as $m) {
                    $body .= schEncodeMsg($topic, $m['body'], $m['offset'], $m['tag'] ?? null);
                }
                $resp->body = $body;
            }
            return $resp;
        }

        default:
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

/**
 * Pull whole frames out of what this connection has sent so far and queue their answers.
 *
 * The record of a pull is written before its answer leaves the endpoint, so once the caller has
 * read the answer the record is on disk too.
 *
 * @param array<int,array{conn:resource,in:string,out:string}> $conns
 */
function schDrainFrames(array &$conns, int $id): void
{
    while (true) {
        $in = $conns[$id]['in'];
        if (strlen($in) < 4) {
            return;
        }
        $total = RocketMQSerializable::unpackSignedInt(substr($in, 0, 4));
        if ($total <= 0) {
            $conns[$id]['in'] = '';
            return;
        }
        if (strlen($in) < 4 + $total) {
            return; // the rest of this frame has not arrived yet
        }
        $cmd = RemotingCommand::decode(substr($in, 0, 4 + $total));
        $conns[$id]['in'] = substr($in, 4 + $total);
        try {
            $resp = schRespond($cmd);
        } catch (\Throwable $e) {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'endpoint error: ' . $e->getMessage());
        }
        $resp->opaque = $cmd->opaque;
        $conns[$id]['out'] .= $resp->encode();
    }
}

/**
 * Every accepted connection is served from one non-blocking loop, so a second client never waits
 * for the first one to finish. The select timeout only paces the idle guard; nothing here decides
 * what a test asserts.
 */
function schBrokerMain(): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "endpoint: cannot listen: {$errstr}\n");
        return;
    }
    stream_set_blocking($server, false);
    $GLOBALS['SCH_ADDR'] = (string) stream_socket_get_name($server, false);
    $GLOBALS['SCH_SEEN'] = [];
    fwrite(STDOUT, $GLOBALS['SCH_ADDR'] . "\n");
    fflush(STDOUT);

    /** @var array<int,array{conn:resource,in:string,out:string}> $conns */
    $conns = [];
    $lastActivity = microtime(true);

    while (true) {
        $read = [$server];
        $write = [];
        foreach ($conns as $entry) {
            $read[] = $entry['conn'];
            if ($entry['out'] !== '') {
                $write[] = $entry['conn'];
            }
        }
        $except = null;
        $ready = @stream_select($read, $write, $except, 1);
        if ($ready === false) {
            break;
        }
        foreach ($write as $conn) {
            $id = (int) $conn;
            $written = @fwrite($conn, $conns[$id]['out']);
            if ($written === false) {
                @fclose($conn);
                unset($conns[$id]);
                continue;
            }
            $conns[$id]['out'] = substr($conns[$id]['out'], $written);
            $lastActivity = microtime(true);
        }
        foreach ($read as $conn) {
            if ($conn === $server) {
                $new = @stream_socket_accept($server, 0);
                if (is_resource($new)) {
                    stream_set_blocking($new, false);
                    $conns[(int) $new] = ['conn' => $new, 'in' => '', 'out' => ''];
                    $lastActivity = microtime(true);
                }
                continue;
            }
            $id = (int) $conn;
            if (!isset($conns[$id])) {
                continue;
            }
            $chunk = @fread($conn, 65535);
            $eof = (bool) (stream_get_meta_data($conn)['eof'] ?? true);
            if ($chunk === false || ($chunk === '' && $eof)) {
                @fclose($conn);
                unset($conns[$id]);
                continue;
            }
            if ($chunk === '') {
                continue;
            }
            $conns[$id]['in'] .= $chunk;
            $lastActivity = microtime(true);
            schDrainFrames($conns, $id);
        }
        if ($conns === [] && microtime(true) - $lastActivity > 30) {
            break;
        }
    }
    foreach ($conns as $entry) {
        @fclose($entry['conn']);
    }
    @fclose($server);
}

/** Child-process handle: prints its listening address, then answers frames and logs every pull. */
final class SchFakeEndpoint
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
            throw new \RuntimeException('cannot spawn fake endpoint');
        }
        $line = fgets($pipes[1]);
        $addr = $line === false ? '' : trim($line);
        if ($addr === '') {
            $err = (string) (stream_get_contents($pipes[2]) ?: '');
            proc_terminate($proc);
            proc_close($proc);
            throw new \RuntimeException('fake endpoint did not report address: ' . $err);
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
// recorded callbacks and task bodies
// ======================================================================

/** Collects whatever arrives, in arrival order, per side of the callback contract. */
final class SchRecorder implements PullCallback
{
    /** @var list<PullResult> */
    public array $results = [];

    /** @var list<\Throwable> */
    public array $errors = [];

    public function onSuccess(PullResult $pullResult): void
    {
        $this->results[] = $pullResult;
    }

    public function onException(\Throwable $e): void
    {
        $this->errors[] = $e;
    }
}

/**
 * A scheduled task body: remembers which cursor it ran on and pulls that queue either
 * asynchronously (through the callback that moves the cursor) or synchronously.
 */
final class SchTaskBody implements PullTaskCallback
{
    /** @var list<int> cursor each run started from, in run order */
    public array $cursors = [];

    /** @var list<PullResult> results the synchronous runs returned */
    public array $results = [];

    /** @var list<SchRecorder> one per asynchronous run, in run order */
    public array $recorders = [];

    public int $runs = 0;

    public bool $async = true;

    public bool $throws = false;

    /** When set, the body asks for this delay before its next run. */
    public ?int $delay = null;

    public function doPullTask(MessageQueue $mq, PullTaskContext $context): void
    {
        $this->runs++;
        $request = $context->getPullRequest();
        $this->cursors[] = $request?->nextBeginOffset ?? -1;
        if ($this->throws) {
            throw new \RuntimeException('task body failed');
        }
        if ($this->delay !== null) {
            $context->setPullNextDelayTimeMillis($this->delay);
        }
        $recorder = $this->async ? new SchRecorder() : null;
        if ($recorder !== null) {
            $this->recorders[] = $recorder;
        }
        $result = $context->pullNext(16, '*', $recorder);
        if ($result !== null) {
            $this->results[] = $result;
        }
    }

    /** Did every round this body started already have its answer put away? */
    public function settled(): bool
    {
        foreach ($this->recorders as $recorder) {
            if ($recorder->results === [] && $recorder->errors === []) {
                return false;
            }
        }
        return true;
    }
}

// ======================================================================
// suite
// ======================================================================

final class RunClientPullSchedule
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
            $this->failures[] = $label;
            fwrite(STDERR, sprintf(
                "FAIL: %s\n  expected: %s\n  actual:   %s\n",
                $label,
                var_export($expected, true),
                var_export($actual, true),
            ));
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

    // ---------------- recorded-state helpers ----------------

    private function loadLog(): void
    {
        $this->log = [];
        foreach (file($GLOBALS['SCH_LOG'], FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
            $d = json_decode($line, true);
            if (is_array($d)) {
                $this->log[] = $d;
            }
        }
    }

    /** @return list<array<string,mixed>> */
    private function pullsOf(string $topic): array
    {
        $out = [];
        foreach ($this->log as $e) {
            if (($e['code'] ?? 0) === RequestCode::PULL_MESSAGE && ($e['topic'] ?? null) === $topic) {
                $out[] = $e;
            }
        }
        return $out;
    }

    /** @return list<int> */
    private function requestedOffsets(string $topic): array
    {
        return array_map(static fn(array $e): int => (int) $e['queueOffset'], $this->pullsOf($topic));
    }

    /** @return list<string> */
    private function shape(array $frame): array
    {
        $out = [];
        foreach (SCH_SHAPE_CELLS as $cell) {
            $out[] = var_export($frame[$cell] ?? null, true);
        }
        return $out;
    }

    /** Read the responses already on the wire, bounded and gated on the state being waited for. */
    private function awaitState(DefaultMQPullConsumer $c, callable $done, int $rounds = 40): void
    {
        for ($i = 0; $i < $rounds && !$done(); $i++) {
            $c->pumpAsyncPulls(100);
        }
    }

    /**
     * Advance the loop by one clock value and settle that round: an asynchronous body has its
     * answer read back before the next round starts, so every assertion below is on recorded state
     * and never on a request still in flight.
     */
    private function tickRound(MQPullConsumerScheduleService $svc, int $nowMillis, SchTaskBody $body): void
    {
        $svc->tick($nowMillis);
        $cons = $svc->getDefaultMQPullConsumer();
        for ($i = 0; $i < 40 && !$body->settled(); $i++) {
            $cons->pumpAsyncPulls(200);
        }
    }

    /**
     * The clock a task registered just now is due on. A fresh task is due as soon as it is
     * registered, so this is the first tick that must run it -- no wall-clock guessing involved.
     */
    private function dueClock(MQPullConsumerScheduleService $svc, MessageQueue ...$mqs): int
    {
        $due = 0;
        foreach ($mqs as $mq) {
            $due = max($due, $svc->nextRunAt($mq));
        }
        return $due;
    }

    private function stopConsumer(DefaultMQPullConsumer $c): void
    {
        $c->shutdown();
        if ($c->clientId !== null) {
            MQClientInstance::removeInstance($c->clientId);
        }
    }

    private function makeConsumer(string $group, string $instance, SchFakeEndpoint $ep): DefaultMQPullConsumer
    {
        $c = new DefaultMQPullConsumer($group);
        $c->setInstanceName($instance);
        $c->setNameServerAddresses([$ep->addr]);
        $c->start();
        return $c;
    }

    private function makeService(
        string $group,
        string $instance,
        SchFakeEndpoint $ep,
        string $topic,
        PullTaskCallback $body,
    ): MQPullConsumerScheduleService {
        $svc = new MQPullConsumerScheduleService($group);
        $svc->getDefaultMQPullConsumer()->setInstanceName($instance);
        $svc->getDefaultMQPullConsumer()->setNameServerAddresses([$ep->addr]);
        $svc->registerPullTaskCallback($topic, $body);
        $svc->start();
        return $svc;
    }

    // ================================================================ 1. async pull: wire shape and success callback

    /**
     * The asynchronous pull is the synchronous kernel with a different transport: the frame it
     * writes must match the synchronous one cell for cell, and the callback must receive the
     * decoded result the synchronous call would have returned.
     */
    private function testAsyncPullMatchesSyncWire(SchFakeEndpoint $ep): void
    {
        $c = $this->makeConsumer('GID-A-SHAPE', 'SCH-A1', $ep);
        $mq = new MessageQueue('AsyncFound', SCH_BROKER_NAME, 0);

        $rec = new SchRecorder();
        $c->pullAsync($mq, 'TagA || TagB', 0, 16, $rec);
        $this->check($rec->results === [] && $rec->errors === [],
            'an asynchronous pull reports nothing before the caller pumps');

        $this->awaitState($c, fn(): bool => $rec->results !== []);
        $this->checkSame(1, count($rec->results), 'the asynchronous pull reports success once');
        $this->checkSame([], $rec->errors, 'a successful asynchronous pull reports no exception');
        $result = $rec->results[0] ?? null;
        $this->check($result !== null, 'the success callback received a pull result');
        if ($result instanceof PullResult) {
            $this->checkSame(PullStatus::FOUND, $result->status, 'the asynchronous result carries the response status');
            $this->checkSame(3, $result->nextBeginOffset, 'the asynchronous result carries the next begin offset');
            $this->checkSame(['a-1', 'a-2'], array_map(static fn(MessageExt $m): string => (string) $m->getBody(), $result->msgFoundList),
                'the asynchronous batch is handed over whole, exactly as the synchronous pull hands it over');
            $this->checkSame(0, $result->msgFoundList[0]?->queueId, 'the asynchronous messages are stamped with the queue');
            $this->checkSame(SCH_BROKER_NAME, $result->msgFoundList[0]?->brokerName, 'the messages carry the queue broker');
        }

        // Bookkeeping of the round that just came back, read before anything else pulls this queue.
        $this->checkSame([$mq], $c->fetchMessageQueuesInBalance('AsyncFound'),
            'the asynchronous pull books its queue as pulled');

        // The same arguments through the synchronous kernel, for a cell-by-cell comparison.
        $sync = $c->pull($mq, 'TagA || TagB', 0, 16);
        $this->checkSame(PullStatus::NO_NEW_MSG, $sync->status, 'the synchronous twin reads the scripted empty round');
        $this->loadLog();
        $frames = $this->pullsOf('AsyncFound');
        $this->checkSame(2, count($frames), 'both pulls reached the endpoint');
        if (count($frames) === 2) {
            $this->checkSame($this->shape($frames[0]), $this->shape($frames[1]),
                'the asynchronous frame equals the synchronous frame cell for cell');
            $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION, (int) $frames[0]['sysFlag'],
                'the asynchronous short poll carries the subscription bit only');
            $this->checkSame(0, (int) $frames[0]['sysFlag'] & PullSysFlag::FLAG_CLASS_FILTER,
                'the asynchronous pull leaves the class-filter bit clear');
            $this->checkSame(0, (int) $frames[0]['subVersion'], 'a tag subscription keeps the historic version 0');
            $this->checkSame('TagA || TagB', $frames[0]['subscription'], 'the tag text reaches the wire as compiled');
            $this->checkSame(ExpressionType::TAG, $frames[0]['expressionType'], 'a tag subscription is reported as TAG');
            $this->checkSame(15000, (int) $frames[0]['suspendTimeoutMillis'], 'a short poll keeps the short suspend budget');
            $this->checkSame(-1, $frames[0]['maxMsgBytes'], 'the asynchronous pull keeps the byte ceiling unset');
        }
        $this->stopConsumer($c);
    }

    /** Selector and long-poll flavours keep the same subscription rules as their synchronous twins. */
    private function testAsyncPullSelectorAndBlock(SchFakeEndpoint $ep): void
    {
        $c = $this->makeConsumer('GID-A-SEL', 'SCH-A2', $ep);

        $sqlMq = new MessageQueue('AsyncSql', SCH_BROKER_NAME, 0);
        $rec = new SchRecorder();
        $c->pullAsyncBySelector($sqlMq, MessageSelector::bySql('a > 10'), 0, 16, $rec);
        $c->pullBySelector(new MessageQueue('SyncSql', SCH_BROKER_NAME, 0), MessageSelector::bySql('a > 10'), 0, 16);
        $this->awaitState($c, fn(): bool => $rec->results !== []);
        $this->loadLog();
        $async = $this->pullsOf('AsyncSql')[0] ?? null;
        $sync = $this->pullsOf('SyncSql')[0] ?? null;
        $this->check($async !== null && $sync !== null, 'both SQL92 pulls reached the endpoint');
        if ($async !== null && $sync !== null) {
            $this->checkSame('a > 10', $async['subscription'], 'the asynchronous SQL92 expression reaches the wire verbatim');
            $this->checkSame(ExpressionType::SQL92, $async['expressionType'], 'the asynchronous SQL92 subscription is reported as SQL92');
            $this->checkSame(true, (int) $async['subVersion'] > 0, 'the asynchronous SQL92 pull carries its own version stamp');
            $this->checkSame(true, (int) $sync['subVersion'] > 0, 'the synchronous SQL92 pull stamps its own version the same way');
            // Every shape cell but the version stamp itself, which each request takes from the clock.
            $this->checkSame(
                $this->shapeCells($sync, ['topic', 'subVersion']),
                $this->shapeCells($async, ['topic', 'subVersion']),
                'the asynchronous SQL92 frame equals its synchronous twin cell for cell'
            );
        }
        $this->checkSame(['sql-1'], array_map(
            static fn(MessageExt $m): string => (string) $m->getBody(),
            $rec->results[0]->msgFoundList ?? [],
        ), 'the asynchronous SQL92 result is not re-filtered by a tag set');

        // Long polling: only the suspend bit and the suspend budget change.
        $blockMq = new MessageQueue('AsyncBlock', SCH_BROKER_NAME, 0);
        $blockRec = new SchRecorder();
        $c->pullBlockIfNotFoundAsync($blockMq, '*', 0, 16, $blockRec);
        $this->awaitState($c, fn(): bool => $blockRec->results !== []);
        $this->loadLog();
        $block = $this->pullsOf('AsyncBlock')[0] ?? null;
        $this->check($block !== null, 'the asynchronous long poll reached the endpoint');
        if ($block !== null) {
            $this->checkSame(PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_SUSPEND, (int) $block['sysFlag'],
                'the asynchronous long poll adds only the suspend bit');
            $this->checkSame(20000, (int) $block['suspendTimeoutMillis'],
                'the asynchronous long poll uses the broker suspend budget');
        }
        $this->checkSame(PullStatus::NO_NEW_MSG, $blockRec->results[0]?->status, 'the long poll reports the empty round');
        // CONTROL, mirroring the synchronous kernel: only short polls book the queue as pulled.
        $this->checkSame([], $c->fetchMessageQueuesInBalance('AsyncBlock'),
            'the asynchronous long poll books no queue as pulled');
        $this->stopConsumer($c);
    }

    /** @return list<string> shape cells except the ones named (they differ by topic name, not by rules) */
    private function shapeCells(array $frame, array $skip): array
    {
        $out = [];
        foreach (array_diff(SCH_SHAPE_CELLS, $skip) as $cell) {
            $out[] = var_export($frame[$cell] ?? null, true);
        }
        return $out;
    }

    // ================================================================ 2. async pull: error path and argument guards

    /** A broker error code travels to the exception side, and an unusable request never reaches the wire. */
    private function testAsyncPullErrorAndGuards(SchFakeEndpoint $ep): void
    {
        $c = $this->makeConsumer('GID-A-ERR', 'SCH-A3', $ep);
        $mq = new MessageQueue('AsyncErr', SCH_BROKER_NAME, 0);
        $rec = new SchRecorder();
        $c->pullAsync($mq, '*', 0, 16, $rec);
        $this->awaitState($c, fn(): bool => $rec->errors !== []);
        $this->loadLog();
        $this->checkSame(1, count($this->pullsOf('AsyncErr')), 'the failing pull was written to the socket');
        $this->checkSame(1, count($rec->errors), 'the broker error code reaches the exception side of the callback');
        $this->checkSame([], $rec->results, 'a broker error code never reports success');
        $err = $rec->errors[0] ?? null;
        $this->check($err instanceof MQBrokerException, 'the failure is reported as a broker error');
        $this->checkSame(ResponseCode::SYSTEM_ERROR, $err?->responseCode, 'the broker code is carried through to the callback');
        $this->checkSame('broker says no', $err?->getMessage(), 'the broker remark is carried through to the callback');

        // Argument guards: each one is refused before anything is written.
        $before = count($this->pullsOf('AsyncErr'));
        $guards = [
            ['offset < 0', fn() => $c->pullAsync($mq, '*', -1, 16, $rec)],
            ['maxNums <= 0', fn() => $c->pullAsync($mq, '*', 0, 0, $rec)],
            ['pullCallback is null', fn() => $c->pullAsync($mq, '*', 0, 16, null)],
            ['parse subscription error: subString split error', fn() => $c->pullAsync($mq, '||', 0, 16, $rec)],
        ];
        foreach ($guards as [$expected, $call]) {
            $message = null;
            try {
                $call();
            } catch (MQClientException $e) {
                $message = $e->getMessage();
            }
            $this->checkSame($expected, $message, "a rejected asynchronous request reports: {$expected}");
        }
        $this->loadLog();
        $this->checkSame($before, count($this->pullsOf('AsyncErr')), 'no rejected request reaches the socket');

        // Before start(): the same guard the synchronous kernel has.
        $idle = new DefaultMQPullConsumer('GID-A-IDLE');
        $message = null;
        try {
            $idle->pullAsync($mq, '*', 0, 16, new SchRecorder());
        } catch (MQClientException $e) {
            $message = $e->getMessage();
        }
        $this->checkSame('consumer not started, call start() first', $message,
            'an asynchronous pull on a stopped consumer is refused');
        $this->stopConsumer($c);
    }

    // ================================================================ 3. scheduled service: due gating and cursor rules

    /**
     * One task per queue, and a queue pulls only when its task is due. The cursor moves by the
     * classic rules, which the recorded request offsets show directly: 0 (fresh) -> 2 (found) ->
     * 2 (nothing new, still inside the bounds) -> 8 (offset the broker rejected) ->
     * 8 (nothing new, still inside) -> 1 (the cursor fell outside the bounds, so it is reset).
     */
    private function testScheduledDueGatingAndCursor(SchFakeEndpoint $ep): void
    {
        $body = new SchTaskBody();
        $svc = $this->makeService('GID-S-CURSOR', 'SCH-S1', $ep, 'OffsetTopic', $body);
        $cons = $svc->getDefaultMQPullConsumer();
        $mq = new MessageQueue('OffsetTopic', SCH_BROKER_NAME, 0);
        $svc->putTask('OffsetTopic', [$mq]);
        $this->checkSame(1, $svc->taskCount(), 'the queue view holds one task');
        $request = $svc->pullRequestOf($mq);
        $this->check($request !== null, 'the queue has a cursor of its own');
        $this->checkSame(0, $request?->nextBeginOffset, 'a fresh cursor starts at zero');

        $t0 = $this->dueClock($svc, $mq);
        $this->tickRound($svc, $t0, $body);
        $this->loadLog();
        $this->checkSame([0], $this->requestedOffsets('OffsetTopic'), 'a due task pulls once, from its cursor');
        $this->checkSame($t0 + 200, $svc->nextRunAt($mq), 'the task is next due after its default delay');
        $this->checkSame(2, $request?->nextBeginOffset, 'a found round moves the cursor to the offset the broker names');
        $this->checkSame(true, $request?->pullOk ?? false, 'a found round marks the queue as pulling well');
        $this->checkSame($t0, $request?->lastPullTimestamp, 'the round stamps the cursor with the tick clock');
        $this->checkSame(0, $request?->minOffset, 'the cursor keeps the lower bound the broker reported');
        $this->checkSame(100, $request?->maxOffset, 'the cursor keeps the upper bound the broker reported');
        $this->checkSame(1, count($body->recorders), 'the round ran once');
        $this->checkSame(['o-0', 'o-1'], array_map(
            static fn(MessageExt $m): string => (string) $m->getBody(),
            $body->recorders[0]->results[0]->msgFoundList ?? [],
        ), 'the batch of an asynchronous round arrives through the callback the service wrapped');

        // Not due yet: two further rounds of the loop must not put anything new on the wire.
        $svc->tick($t0 + 50);
        $svc->tick($t0 + 199);
        $this->loadLog();
        $this->checkSame(1, count($this->pullsOf('OffsetTopic')), 'a task that is not due pulls nothing');
        $this->checkSame(1, $body->runs, 'a task that is not due never runs');

        $this->tickRound($svc, $t0 + 200, $body);
        $this->loadLog();
        $this->checkSame([0, 2], $this->requestedOffsets('OffsetTopic'),
            'the round after the default delay starts from the cursor the found round left');
        $this->checkSame(2, $request?->nextBeginOffset,
            'an empty round inside the reported bounds keeps the cursor where it is');
        $this->checkSame(false, $request?->pullOk ?? true, 'an empty round marks the queue as not pulling well');

        $this->tickRound($svc, $t0 + 400, $body);
        $this->loadLog();
        $this->checkSame([0, 2, 2], $this->requestedOffsets('OffsetTopic'), 'a not-due round writes nothing, a due round writes exactly one');
        $this->checkSame(8, $request?->nextBeginOffset,
            'an offset the broker rejected is replaced by the offset it names');
        $this->checkSame(true, $request?->pullOk ?? false, 'a rejected offset is a usable round again');
        $this->checkSame(8, $request?->minOffset, 'the cursor adopts the new lower bound of a rejected offset');

        // The cursor now sits inside the bounds the broker reports, so an empty round keeps it; one
        // tick later the bounds have moved past it, so it is reset.
        $this->tickRound($svc, $t0 + 600, $body);
        $this->loadLog();
        $this->checkSame([0, 2, 2, 8], $this->requestedOffsets('OffsetTopic'),
            'the round after a rejected offset starts from the offset the broker named');
        $this->checkSame(8, $request?->nextBeginOffset,
            'an empty round that leaves the cursor inside the bounds does not chase the broker offset');
        $this->checkSame(false, $request?->pullOk ?? true, 'the second empty round marks the queue as not pulling well');

        $this->tickRound($svc, $t0 + 800, $body);
        $this->loadLog();
        $this->checkSame([0, 2, 2, 8, 8], $this->requestedOffsets('OffsetTopic'),
            'an empty round pulls from the cursor it kept, not from the offset the broker named');
        $this->checkSame(1, $request?->nextBeginOffset,
            'a cursor that fell outside the bounds is reset to the offset the broker names');

        $this->tickRound($svc, $t0 + 1000, $body);
        $this->loadLog();
        $this->checkSame([0, 2, 2, 8, 8, 1], $this->requestedOffsets('OffsetTopic'),
            'the reset cursor is the one the next round pulls from');
        $this->checkSame(2, $request?->nextBeginOffset, 'the last scripted round moves the cursor once its response is read');
        $this->checkSame(6, $body->runs, 'the task ran once per due round');
        $this->checkSame([0, 2, 2, 8, 8, 1], $body->cursors, 'every run read the cursor the previous response left');

        // A round of the loop never delivers the answer of the request it just started: the answer
        // is read by the following round, which is where the callback goes off.
        $svc->tick($t0 + 1200);
        $this->checkSame(2, $request?->nextBeginOffset, 'a round of the loop does not deliver the answer of its own request');
        $this->checkSame(7, $body->runs, 'that round started exactly one pull');
        // The clock stays where it is, so the task is not due again and these rounds only read back
        // what is already on the wire.
        for ($i = 0; $i < 40 && $request->nextBeginOffset === 2; $i++) {
            $svc->tick($t0 + 1200);
        }
        $this->checkSame(107, $request?->nextBeginOffset, 'the following round of the loop delivers that answer');
        $this->loadLog();
        $this->checkSame([0, 2, 2, 8, 8, 1, 2], $this->requestedOffsets('OffsetTopic'),
            'the answer that moved the cursor was the round that pulled from the reset cursor');
        $this->stopConsumer($cons);
    }

    // ================================================================ 4. scheduled service: unpullable queues are dropped

    /**
     * A queue that has not produced anything for the whole window loses its task; a queue that is
     * consuming normally keeps its task through the same clock jump.
     */
    private function testPullImpossibleDropsTask(SchFakeEndpoint $ep): void
    {
        $staleBody = new SchTaskBody();
        $staleBody->async = false;
        $svc = $this->makeService('GID-S-STALE', 'SCH-S2', $ep, 'StaleTopic', $staleBody);
        $cons = $svc->getDefaultMQPullConsumer();
        $mq = new MessageQueue('StaleTopic', SCH_BROKER_NAME, 0);
        $svc->putTask('StaleTopic', [$mq]);

        $t0 = $this->dueClock($svc, $mq);
        $this->tickRound($svc, $t0, $staleBody);
        $this->loadLog();
        $this->checkSame(1, count($this->pullsOf('StaleTopic')), 'the stalled queue pulled once');
        $this->checkSame(false, $svc->pullRequestOf($mq)?->pullOk ?? true, 'the stalled queue is marked as not pulling well');

        // Just inside the window: the task stays and pulls again, which also refreshes its stamp.
        $inside = $t0 + 119_999;
        $this->tickRound($svc, $inside, $staleBody);
        $this->loadLog();
        $this->check($svc->hasTask($mq), 'a stalled queue just inside the window keeps its task');
        $this->checkSame(2, count($this->pullsOf('StaleTopic')), 'the task inside the window pulled again');
        $this->checkSame($inside, $svc->pullRequestOf($mq)?->lastPullTimestamp, 'the round inside the window restamped the cursor');

        $this->tickRound($svc, $inside + MQPullConsumerScheduleService::PULL_IMPOSSIBLE_MILLIS + 1, $staleBody);
        $this->loadLog();
        $this->checkSame(2, count($this->pullsOf('StaleTopic')), 'a queue unpullable past the window pulls no more');
        $this->checkSame(false, $svc->hasTask($mq), 'the stalled task is dropped once the window passed');
        $this->checkSame(0, $svc->taskCount(), 'the task table is empty after the drop');
        $this->checkSame(2, $staleBody->runs, 'a dropped task never runs again');

        // Re-registering the same queue starts it over: due at once, with a fresh cursor.
        $svc->putTask('StaleTopic', [$mq]);
        $this->check($svc->hasTask($mq), 'the queue view registers the task anew');
        $this->stopConsumer($cons);

        $healthyBody = new SchTaskBody();
        $healthyBody->async = false;
        $healthy = $this->makeService('GID-S-HEALTHY', 'SCH-S3', $ep, 'HealthyTopic', $healthyBody);
        $hmq = new MessageQueue('HealthyTopic', SCH_BROKER_NAME, 0);
        $healthy->putTask('HealthyTopic', [$hmq]);
        $h0 = $this->dueClock($healthy, $hmq);
        $healthy->tick($h0);
        $healthy->tick($h0 + MQPullConsumerScheduleService::PULL_IMPOSSIBLE_MILLIS + 1);
        $this->loadLog();
        $this->check($healthy->hasTask($hmq), 'CONTROL: a queue that consumes is never dropped by the window');
        $this->checkSame(2, count($this->pullsOf('HealthyTopic')), 'CONTROL: the healthy task keeps pulling on later rounds');
        $this->checkSame(2, $healthy->pullRequestOf($hmq)?->nextBeginOffset, 'the healthy cursor follows the found rounds');
        $this->stopConsumer($healthy->getDefaultMQPullConsumer());
    }

    // ================================================================ 5. scheduled service: failure and table rules

    /**
     * A body that throws is retried on the failure delay and leaves its queue marked as not
     * pulling well; a queue with no registered body is never pulled; and the queue view
     * reconciles the table without disturbing the tasks it keeps.
     */
    private function testTaskFailureAndTable(SchFakeEndpoint $ep): void
    {
        $body = new SchTaskBody();
        $body->throws = true;
        $svc = $this->makeService('GID-S-THROW', 'SCH-S4', $ep, 'ThrowTopic', $body);
        $cons = $svc->getDefaultMQPullConsumer();
        $mq = new MessageQueue('ThrowTopic', SCH_BROKER_NAME, 0);
        $svc->putTask('ThrowTopic', [$mq]);
        $t0 = $this->dueClock($svc, $mq);
        $svc->tick($t0);
        $this->checkSame(1, $body->runs, 'a failing body still ran once');
        $this->checkSame($t0 + 1000, $svc->nextRunAt($mq), 'a failing body is retried on the failure delay');
        $this->checkSame(false, $svc->pullRequestOf($mq)?->pullOk ?? true, 'a failing body marks its queue as not pulling well');
        $this->loadLog();
        $this->checkSame(0, count($this->pullsOf('ThrowTopic')), 'a body that throws before pulling writes nothing');

        $svc->tick($t0 + 200);
        $this->checkSame(1, $body->runs, 'the retried task is not due on the default delay');
        $svc->tick($t0 + 1000);
        $this->checkSame(2, $body->runs, 'the retried task runs again on the failure delay');

        // A body that asks for its own delay overrides both defaults.
        $fast = new SchTaskBody();
        $fast->delay = 50;
        $fastSvc = $this->makeService('GID-S-DELAY', 'SCH-S5', $ep, 'FastTopic', $fast);
        $fmq = new MessageQueue('FastTopic', SCH_BROKER_NAME, 0);
        $fastSvc->putTask('FastTopic', [$fmq]);
        $ft = $this->dueClock($fastSvc, $fmq);
        $this->tickRound($fastSvc, $ft, $fast);
        $this->checkSame($ft + 50, $fastSvc->nextRunAt($fmq), 'the delay a body sets is what schedules its next run');
        $this->loadLog();
        $this->checkSame([0], $this->requestedOffsets('FastTopic'), 'a body that sets its own delay still pulls once');
        $this->stopConsumer($cons);
        $this->stopConsumer($fastSvc->getDefaultMQPullConsumer());

        // ---- no body for the queue: warned, never pulled, slot not consumed ----
        $plain = new MQPullConsumerScheduleService('GID-S-NOBODY');
        $plain->getDefaultMQPullConsumer()->setInstanceName('SCH-S6');
        $plain->getDefaultMQPullConsumer()->setNameServerAddresses([$ep->addr]);
        $plain->start();
        $nmq = new MessageQueue('NoBodyTopic', SCH_BROKER_NAME, 0);
        $plain->putTask('NoBodyTopic', [$nmq]);
        $due = $plain->nextRunAt($nmq);
        $plain->tick($due);
        $plain->tick($due + 500);
        $this->loadLog();
        $this->checkSame(0, count($this->pullsOf('NoBodyTopic')), 'a queue with no registered body is never pulled');
        $this->checkSame($due, $plain->nextRunAt($nmq), 'a queue with no body keeps its due time for the next round');
        $this->check($plain->hasTask($nmq), 'a queue with no body is not dropped for missing a body');

        // ---- table reconciliation (its own two-queue topic, so the frame records stay readable) ----
        $q0 = new MessageQueue('PairTopic', SCH_BROKER_NAME, 0);
        $q1 = new MessageQueue('PairTopic', SCH_BROKER_NAME, 1);
        $keep = new SchTaskBody();
        $keep->delay = 5000;
        $keep->async = false;
        $reconcile = $this->makeService('GID-S-TABLE', 'SCH-S7', $ep, 'PairTopic', $keep);
        $reconcile->putTask('PairTopic', [$q0, $q1]);
        $this->checkSame(2, $reconcile->taskCount(), 'both queues of the view hold a task');
        $tr = $this->dueClock($reconcile, $q0, $q1);
        $reconcile->tick($tr);
        $keptDue = $reconcile->nextRunAt($q0);
        $this->checkSame($tr + 5000, $keptDue, 'the delay a body set is stored for every queue it ran for');
        $this->checkSame(2, $keep->runs, 'one round runs every task that is due');
        $this->loadLog();
        $this->checkSame([0, 1], array_map(
            static fn(array $f): int => (int) $f['queueId'],
            $this->pullsOf('PairTopic'),
        ), 'one request per queue of the view, in view order');
        $reconcile->putTask('PairTopic', [$q0]);
        $this->checkSame(1, $reconcile->taskCount(), 'a queue that left the view loses its task');
        $this->checkSame(false, $reconcile->hasTask($q1), 'the task of the queue that left is cancelled');
        $this->checkSame($keptDue, $reconcile->nextRunAt($q0), 'the task the view kept is not rescheduled');
        $this->checkSame(1, $reconcile->pullRequestOf($q0)?->nextBeginOffset, 'the cursor of the kept task survives');
        $this->checkSame(1, $reconcile->pullRequestOf($q1)?->nextBeginOffset, 'a dropped queue keeps the cursor it reached');
        $reconcile->putTask('PairTopic', [$q0, $q1]);
        $this->checkSame($tr, $reconcile->nextRunAt($q1), 'a queue that rejoins the view is due at once');
        $this->checkSame(2, $reconcile->taskCount(), 'the queue that rejoined the view holds a task again');
        $this->stopConsumer($reconcile->getDefaultMQPullConsumer());
        $this->stopConsumer($plain->getDefaultMQPullConsumer());
    }

    /** The queue view of a registered topic is picked up by the loop, without the caller naming queues. */
    private function testTaskDiscoveryFromQueueView(SchFakeEndpoint $ep): void
    {
        $body = new SchTaskBody();
        $svc = $this->makeService('GID-S-VIEW', 'SCH-S8', $ep, 'TwoQueueTopic', $body);
        $this->checkSame(0, $svc->taskCount(), 'a task table starts empty before the loop sees the queue view');
        $this->tickRound($svc, UtilAll::currentTimeMillis(), $body);
        $this->checkSame(2, $svc->taskCount(), 'the loop registers one task per queue of the topic view');
        $this->check($svc->hasTask(new MessageQueue('TwoQueueTopic', SCH_BROKER_NAME, 1)),
            'the second queue of the view got a task too');
        $this->loadLog();
        $this->checkSame(2, count($this->pullsOf('TwoQueueTopic')), 'both discovered tasks pulled on their first round');
        $this->checkSame(2, $body->runs, 'each discovered task ran once');
        $this->checkSame([0, 1], array_map(
            static fn(array $f): int => (int) $f['queueId'],
            $this->pullsOf('TwoQueueTopic'),
        ), 'one request per queue of the view, in view order');
        $this->stopConsumer($svc->getDefaultMQPullConsumer());
    }

    // ================================================================ 6. no-network contract checks

    /** Cursor rules, defaults and the queue-view dispatch, asserted directly on the state. */
    private function testCursorRulesAndDefaults(): void
    {
        $svc = new MQPullConsumerScheduleService('GID-S-UNIT');
        $this->checkSame(MessageModel::CLUSTERING, $svc->getMessageModel(), 'the service clusters by default');
        $this->checkSame(20, $svc->getPullThreadNums(), 'the advisory task-count knob defaults to 20');
        $svc->setPullThreadNums(3);
        $this->checkSame(3, $svc->getPullThreadNums(), 'the advisory task-count knob stores what it is given');
        $this->checkSame(200, MQPullConsumerScheduleService::DEFAULT_PULL_NEXT_DELAY_MILLIS, 'a task waits 200 ms by default');
        $this->checkSame(1000, MQPullConsumerScheduleService::PULL_TASK_ERROR_DELAY_MILLIS, 'a failed task waits 1000 ms');
        $this->checkSame(120000, MQPullConsumerScheduleService::PULL_IMPOSSIBLE_MILLIS, 'the unpullable window is two minutes');

        $context = new PullTaskContext($svc->getDefaultMQPullConsumer(), $svc);
        $this->checkSame(200, $context->getPullNextDelayTimeMillis(), 'a fresh context carries the default delay');
        $context->setPullNextDelayTimeMillis(75);
        $this->checkSame(75, $context->getPullNextDelayTimeMillis(), 'the delay a body sets is stored');
        $message = null;
        try {
            $context->pullNext();
        } catch (MQClientException $e) {
            $message = $e->getMessage();
        }
        $this->checkSame('this context carries no queue to pull', $message, 'an unbound context refuses to pull');

        $mq = new MessageQueue('UTopic', SCH_BROKER_NAME, 0);
        $pr = new PullRequest('GID-S-UNIT', $mq);
        $pr->processPullResult(new PullResult(PullStatus::FOUND, 11, 4, 40), 1000);
        $this->checkSame(11, $pr->nextBeginOffset, 'a found round moves the cursor');
        $this->checkSame(true, $pr->pullOk, 'a found round is a usable round');
        $this->checkSame(4, $pr->minOffset, 'the cursor records the lower bound');
        $this->checkSame(40, $pr->maxOffset, 'the cursor records the upper bound');

        $pr->processPullResult(new PullResult(PullStatus::NO_NEW_MSG, 30, 4, 40), 2000);
        $this->checkSame(11, $pr->nextBeginOffset, 'an empty round inside the bounds keeps the cursor');
        $pr->processPullResult(new PullResult(PullStatus::NO_MATCHED_MSG, 30, 12, 40), 3000);
        $this->checkSame(30, $pr->nextBeginOffset, 'an empty round that leaves the cursor below the bounds resets it');
        $pr->processPullResult(new PullResult(PullStatus::NO_NEW_MSG, 2, 1, 5), 4000);
        $this->checkSame(2, $pr->nextBeginOffset, 'an empty round that leaves the cursor above the bounds resets it');
        $pr->processPullResult(new PullResult(PullStatus::OFFSET_ILLEGAL, 7, 1, 5), 5000);
        $this->checkSame(7, $pr->nextBeginOffset, 'a rejected offset is replaced wholesale');
        $this->checkSame(true, $pr->pullOk, 'a rejected offset is a usable round');
        $pr->processPullResult(null, 6000);
        $this->checkSame(6000, $pr->lastPullTimestamp, 'a round without any result still stamps the cursor');

        $pr->markPullFailure(7000);
        $this->checkSame(false, $pr->pullOk, 'a failed round marks the queue as not pulling well');
        $this->checkSame(true, $pr->isPullImpossible(7000 + 120_001, MQPullConsumerScheduleService::PULL_IMPOSSIBLE_MILLIS),
            'the window is over once the stamp is older than it');
        $this->checkSame(false, $pr->isPullImpossible(7000 + 120_000, MQPullConsumerScheduleService::PULL_IMPOSSIBLE_MILLIS),
            'the window is not over at exactly its length');

        $fresh = new PullRequest('GID-S-UNIT', $mq);
        $this->checkSame(false, $fresh->isPullImpossible(9_999_999, 1),
            'a cursor that never answered anything is not treated as stalled');

        // ---- queue view dispatch ----
        $listener = new PullScheduleQueueListener($svc);
        $q0 = new MessageQueue('VTopic', SCH_BROKER_NAME, 0);
        $q1 = new MessageQueue('VTopic', SCH_BROKER_NAME, 1);
        $listener->messageQueueChanged('VTopic', [$q0, $q1], [$q0]);
        $this->checkSame(1, $svc->taskCount(), 'clustering takes only the queues allocated to this client');
        $this->checkSame(true, $svc->hasTask($q0), 'the allocated queue holds a task');
        $this->checkSame(false, $svc->hasTask($q1), 'the unallocated queue holds no task');

        $bc = new MQPullConsumerScheduleService('GID-S-BC');
        $bc->setMessageModel(MessageModel::BROADCASTING);
        (new PullScheduleQueueListener($bc))->messageQueueChanged('VTopic', [$q0, $q1], [$q0]);
        $this->checkSame(2, $bc->taskCount(), 'broadcasting takes every queue of the view');
        $this->checkSame(MessageModel::BROADCASTING, $bc->getMessageModel(), 'the model setter reaches the consumer');

        // ---- registration surface ----
        $named = new MQPullConsumerScheduleService('GID-S-NS', null, 'NS');
        $named->registerPullTaskCallback('NTopic', new SchTaskBody());
        $this->checkSame(['NS%NTopic'], array_keys($named->getCallbackTable()),
            'a body is registered under the namespace-wrapped topic');
        $this->checkSame(['NTopic' => true], $named->getDefaultMQPullConsumer()->registerTopics,
            'registering a body puts the topic on the subscription list');

        $message = null;
        try {
            new MQPullConsumerScheduleService('  ');
        } catch (MQClientException $e) {
            $message = $e->getMessage();
        }
        $this->checkSame('consumerGroup is empty', $message, 'an empty group is refused by the constructor');

        $idle = new MQPullConsumerScheduleService('GID-S-IDLE');
        $idle->tick(12345);
        $this->checkSame(0, $idle->taskCount(), 'a stopped service does not run any task');
        $this->checkSame(false, $idle->isRunning(), 'a service that never started is not running');
    }

    // ================================================================ run

    public function run(): int
    {
        $this->testCursorRulesAndDefaults();

        $tmpDir = sys_get_temp_dir() . '/rmq-schedule-test-' . getmypid();
        if (!is_dir($tmpDir)) {
            @mkdir($tmpDir, 0777, true);
        }
        $logPath = $tmpDir . '/endpoint.jsonl';
        @unlink($logPath);
        putenv("RMQ_LOG={$logPath}");
        $GLOBALS['SCH_LOG'] = $logPath;

        $ep = SchFakeEndpoint::start();
        try {
            $this->testAsyncPullMatchesSyncWire($ep);
            $this->testAsyncPullSelectorAndBlock($ep);
            $this->testAsyncPullErrorAndGuards($ep);
            $this->testScheduledDueGatingAndCursor($ep);
            $this->testPullImpossibleDropsTask($ep);
            $this->testTaskFailureAndTable($ep);
            $this->testTaskDiscoveryFromQueueView($ep);
        } finally {
            $ep->stop();
            putenv('RMQ_LOG');
            @unlink($logPath);
            @rmdir($tmpDir);
        }
        return $this->summary();
    }
}

exit((new RunClientPullSchedule())->run());
