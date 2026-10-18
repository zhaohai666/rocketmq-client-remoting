<?php

declare(strict_types=1);

/**
 * POP broker-side assignment suite (QUERY_ASSIGNMENT, request code 400).
 *
 * Run: php tests/RunClientPopAssignment.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Coverage (offline, no cluster):
 *   1) wire protocol: QueryAssignmentRequestBody / QueryAssignmentResponseBody exact JSON shape,
 *      including the null-vs-empty distinction of `messageQueueAssignments`;
 *   2) gate matrix: only popMode + clientRebalance=false + CLUSTERING + non-orderly takes the
 *      broker-side leg (usesBrokerPopAssignment);
 *   3) live wire via a child-process fake broker (same pattern as RunClientFaultDetector):
 *      - normal answer -> the broker's queue set (not the local strategy) becomes the assignment
 *        and the POP pull surface (PopProcessQueues + POP_MESSAGE legs) follows it;
 *      - the 400 request body carries exactly topic / consumerGroup / clientId / strategyName /
 *        messageModel (the broker load-balances by them), and the local-rebalance cell
 *        GET_CONSUMER_LIST_BY_GROUP(38) is never consulted on this path;
 *      - periodic rhythm: one round per interval inside MQClientInstance::tick() stage 7,
 *        rebalanceImmediately() wakes it;
 *      - assignment change is applied on the next tick (shrinking drops the revoked queue);
 *      - broker SYSTEM_ERROR / missing set -> keep the previous assignment (never "take all");
 *      - TOPIC_NOT_EXIST(17) -> empty set -> clear the assignment;
 *      - the default (clientRebalance=true, pull mode) path never sends a 400 and keeps the
 *        local AVG rebalance driven by GET_CONSUMER_LIST_BY_GROUP.
 */

require_once __DIR__ . '/../bootstrap.php';

// Clear dynamic address-discovery env vars; swallow client logs (assertions here do not depend on log content)
putenv('ROCKETMQ_NAMESRV_DOMAIN');
\RocketMQ\Client\Logger::setHandler(static function (string $line): void {
});

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\ConsumeOrderlyContext;
use RocketMQ\Client\ConsumeOrderlyStatus;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MessageListenerOrderly;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\MessageQueueAssignment;
use RocketMQ\Remoting\Protocol\MessageRequestMode;
use RocketMQ\Remoting\Protocol\QueryAssignmentRequestBody;
use RocketMQ\Remoting\Protocol\QueryAssignmentResponseBody;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\TopicRouteData;

// ======================================================================
// Child-process fake broker: `php RunClientPopAssignment.php --broker <logfile> <planfile>`
// ======================================================================

/** Topic the fake route knows; every other topic answers TOPIC_NOT_EXIST (like %RETRY% groups). */
const ASSIGN_TOPIC = 'ATopic';
const ASSIGN_BROKER = 'broker-a';

if (($argv[1] ?? '') === '--broker') {
    assignBrokerMain($argv[2] ?? '', $argv[3] ?? '');
    exit(0);
}

function assignBrokerReadExact($conn, int $n): ?string
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

/** @return array{0: string, 1: RemotingCommand}|null */
function assignBrokerTakeFrame(string &$buf): ?array
{
    if (strlen($buf) < 4) {
        return null;
    }
    $total = RocketMQSerializable::unpackSignedInt(substr($buf, 0, 4));
    if ($total <= 0) {
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

function assignBrokerLog(string $logFile, array $entry): void
{
    @file_put_contents($logFile, json_encode($entry, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
}

function assignBrokerPlan(string $planFile): array
{
    $raw = @file_get_contents($planFile);
    $plan = ($raw === false || $raw === '') ? null : json_decode($raw, true);
    return is_array($plan) ? $plan : [];
}

function assignBrokerRouteBody(string $addr): string
{
    $qd = new QueueData(ASSIGN_BROKER, 2, 2, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', ASSIGN_BROKER, [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

function assignBrokerBody(RemotingCommand $cmd): array
{
    return ($cmd->body === null || $cmd->body === '')
        ? []
        : (json_decode($cmd->body, true) ?: []);
}

/**
 * QUERY_ASSIGNMENT branch. The plan file drives the answer:
 *   {"topic":"ATopic","response":"success|empty|missing|error|topic_not_exist","queues":[0,1],"mode":"POP"}
 * A request for any other topic answers SUCCESS **without** a set (the "keep current" default),
 * so the auto-subscribed %RETRY% group never disturbs the asserted topic.
 */
function assignBrokerAnswerQueryAssignment(RemotingCommand $cmd, string $planFile): RemotingCommand
{
    $req = QueryAssignmentRequestBody::decode($cmd->body);
    $plan = assignBrokerPlan($planFile);
    if ($req->topic !== (string) ($plan['topic'] ?? '')) {
        return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
    switch ((string) ($plan['response'] ?? 'missing')) {
        case 'error':
            return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'fixture error');
        case 'topic_not_exist':
            return RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST, 'topic not exist');
        case 'empty':
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $body = new QueryAssignmentResponseBody();
            $body->messageQueueAssignments = [];
            $resp->body = $body->encode();
            return $resp;
        case 'success':
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $items = [];
            foreach ((array) ($plan['queues'] ?? []) as $qid) {
                $a = new MessageQueueAssignment();
                $a->messageQueue = new MessageQueue($req->topic, ASSIGN_BROKER, (int) $qid);
                $a->mode = MessageRequestMode::tryFrom((string) ($plan['mode'] ?? 'POP')) ?? MessageRequestMode::POP;
                $items[] = $a;
            }
            $body = new QueryAssignmentResponseBody();
            $body->messageQueueAssignments = $items;
            $resp->body = $body->encode();
            return $resp;
        default: // 'missing': SUCCESS with no set at all -> the client must keep its previous assignment
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

function assignBrokerHandleFrame($conn, RemotingCommand $cmd, string $logFile, string $planFile, string $selfAddr): void
{
    $ext = $cmd->extFields;
    $resp = null;
    switch ($cmd->code) {
        case RequestCode::GET_ROUTEINFO_BY_TOPIC:
            assignBrokerLog($logFile, ['code' => $cmd->code, 'topic' => $ext['topic'] ?? null]);
            if (($ext['topic'] ?? '') === ASSIGN_TOPIC) {
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->body = assignBrokerRouteBody($selfAddr);
            } else {
                $resp = RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST, 'topic not exist');
            }
            break;
        case RequestCode::HEART_BEAT:
            // Harvest group -> clientIds so GET_CONSUMER_LIST_BY_GROUP can answer the local-rebalance leg.
            $hb = assignBrokerBody($cmd);
            $clientId = (string) ($hb['clientID'] ?? '');
            foreach ((array) ($hb['consumerDataSet'] ?? []) as $cd) {
                $g = (string) ($cd['groupName'] ?? '');
                if ($g !== '' && $clientId !== '') {
                    $GLOBALS['ASSIGN_GROUPS'][$g][$clientId] = true;
                }
            }
            assignBrokerLog($logFile, ['code' => $cmd->code, 'clientId' => $clientId]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            break;
        case RequestCode::GET_CONSUMER_LIST_BY_GROUP:
            $g = (string) ($ext['consumerGroup'] ?? '');
            assignBrokerLog($logFile, ['code' => $cmd->code, 'group' => $g]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = json_encode(['consumerIdList' => array_keys($GLOBALS['ASSIGN_GROUPS'][$g] ?? [])]);
            break;
        case RequestCode::QUERY_ASSIGNMENT:
            $req = QueryAssignmentRequestBody::decode($cmd->body);
            assignBrokerLog($logFile, [
                'code' => $cmd->code,
                'topic' => $req->topic,
                'group' => $req->consumerGroup,
                'clientId' => $req->clientId,
                'strategyName' => $req->strategyName,
                'messageModel' => $req->messageModel,
            ]);
            $resp = assignBrokerAnswerQueryAssignment($cmd, $planFile);
            break;
        case RequestCode::POP_MESSAGE:
            assignBrokerLog($logFile, [
                'code' => $cmd->code,
                'topic' => $ext['topic'] ?? null,
                'group' => $ext['consumerGroup'] ?? null,
                'queueId' => isset($ext['queueId']) ? (int) $ext['queueId'] : null,
            ]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::POLLING_TIMEOUT);
            break;
        case RequestCode::PULL_MESSAGE:
            assignBrokerLog($logFile, [
                'code' => $cmd->code,
                'topic' => $ext['topic'] ?? null,
                'group' => $ext['consumerGroup'] ?? null,
                'queueId' => isset($ext['queueId']) ? (int) $ext['queueId'] : null,
            ]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::PULL_NOT_FOUND);
            $resp->extFields['nextBeginOffset'] = '0';
            $resp->extFields['minOffset'] = '0';
            $resp->extFields['maxOffset'] = '0';
            break;
        case RequestCode::GET_MAX_OFFSET:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '0';
            break;
        case RequestCode::QUERY_CONSUMER_OFFSET:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '0';
            break;
        case RequestCode::UPDATE_CONSUMER_OFFSET:
        case RequestCode::UNREGISTER_CLIENT:
        case RequestCode::CHECK_CLIENT_CONFIG:
            assignBrokerLog($logFile, ['code' => $cmd->code]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            break;
        default:
            assignBrokerLog($logFile, ['code' => $cmd->code, 'ext' => $ext]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            break;
    }
    if ($resp !== null) {
        $resp->opaque = $cmd->opaque;
        @fwrite($conn, $resp->encode());
    }
}

function assignBrokerMain(string $logFile, string $planFile): void
{
    $GLOBALS['ASSIGN_GROUPS'] = [];
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "broker: cannot listen: {$errstr}\n");
        return;
    }
    $addr = (string) stream_socket_get_name($server, false);
    fwrite(STDOUT, $addr . "\n");
    fflush(STDOUT);

    /** Multi-connection event loop (a single blocking connection rejects new connections on Windows). */
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
                }
                continue;
            }
            $key = (int) $r;
            $chunk = @fread($r, 65536);
            if ($chunk === false || ($chunk === '' && feof($r))) {
                unset($conns[$key], $bufs[$key]);
                @fclose($r);
                continue;
            }
            $bufs[$key] .= $chunk;
            while (true) {
                $frame = assignBrokerTakeFrame($bufs[$key]);
                if ($frame === null) {
                    break;
                }
                assignBrokerHandleFrame($r, $frame[1], $logFile, $planFile, $addr);
            }
        }
    }
    @fclose($server);
}

// ======================================================================
// Child-process fake broker handle
// ======================================================================

final class AssignFakeBroker
{
    /** @var resource */
    private $proc;
    /** @var array<int, resource> */
    private array $pipes;
    public string $addr;
    private string $logFile;
    private string $planFile;

    private function __construct($proc, array $pipes, string $addr, string $logFile, string $planFile)
    {
        $this->proc = $proc;
        $this->pipes = $pipes;
        $this->addr = $addr;
        $this->logFile = $logFile;
        $this->planFile = $planFile;
    }

    public static function start(array $plan): self
    {
        $logFile = tempnam(sys_get_temp_dir(), 'rmq-assign-log-');
        $planFile = tempnam(sys_get_temp_dir(), 'rmq-assign-plan-');
        file_put_contents($planFile, json_encode($plan));
        $errFile = $logFile . '.err';
        $cmd = [PHP_BINARY, __FILE__, '--broker', $logFile, $planFile];
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
        return new self($proc, $pipes, $addr, $logFile, $planFile);
    }

    public function setPlan(array $plan): void
    {
        file_put_contents($this->planFile, json_encode($plan));
    }

    /** @return list<array<string,mixed>> */
    public function logEntries(): array
    {
        $out = [];
        foreach (file($this->logFile) ?: [] as $line) {
            $line = trim($line);
            if ($line !== '') {
                $decoded = json_decode($line, true);
                if (is_array($decoded)) {
                    $out[] = $decoded;
                }
            }
        }
        return $out;
    }

    /** Count log entries by request code, optionally filtered by topic / group / queueId. */
    public function logCount(int $code, ?string $topic = null, ?string $group = null, ?int $queueId = null, int $fromIndex = 0): int
    {
        $n = 0;
        foreach (array_slice($this->logEntries(), $fromIndex) as $e) {
            if (($e['code'] ?? null) !== $code) {
                continue;
            }
            if ($topic !== null && ($e['topic'] ?? null) !== $topic) {
                continue;
            }
            if ($group !== null && ($e['group'] ?? null) !== $group) {
                continue;
            }
            if ($queueId !== null && ($e['queueId'] ?? null) !== $queueId) {
                continue;
            }
            $n++;
        }
        return $n;
    }

    public function logSize(): int
    {
        return count($this->logEntries());
    }

    /** @return list<array<string,mixed>> */
    public function logWhere(int $code, int $fromIndex = 0): array
    {
        return array_values(array_filter(
            array_slice($this->logEntries(), $fromIndex),
            static fn (array $e): bool => ($e['code'] ?? null) === $code
        ));
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
        @unlink($this->logFile . '.err');
        @unlink($this->planFile);
    }
}

// ======================================================================
// Listeners
// ======================================================================

final class AssignOkListener implements MessageListenerConcurrently
{
    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

final class AssignOrderlyListener implements MessageListenerOrderly
{
    public function consumeMessage(array $msgs, ConsumeOrderlyContext $context): ConsumeOrderlyStatus
    {
        return ConsumeOrderlyStatus::SUCCESS;
    }
}

// ======================================================================
// Test runner
// ======================================================================

final class RunClientPopAssignment
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

    private function summary(): int
    {
        if ($this->failures !== []) {
            printf("FAILED (%d/%d checks passed)\n", $this->passed, $this->passed + count($this->failures));
            return 1;
        }
        printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
        return 0;
    }

    private function cleanup(DefaultMQPushConsumer $c): void
    {
        $c->shutdown();
        if ($c->clientId !== null) {
            MQClientInstance::removeInstance($c->clientId);
        }
    }

    // ================================================================ 1. Wire protocol, pure

    private function testProtocolPure(): void
    {
        // Request body: the exact five fields the broker load-balances by.
        $req = new QueryAssignmentRequestBody();
        $req->topic = 'T';
        $req->consumerGroup = 'G';
        $req->clientId = 'cid@1';
        $req->messageModel = 'CLUSTERING';
        $req->strategyName = 'AVG';
        $decoded = json_decode($req->encode(), true);
        $this->checkSame(
            ['topic' => 'T', 'consumerGroup' => 'G', 'clientId' => 'cid@1', 'messageModel' => 'CLUSTERING', 'strategyName' => 'AVG'],
            $decoded,
            'request body carries exactly topic/consumerGroup/clientId/messageModel/strategyName'
        );
        $round = QueryAssignmentRequestBody::decode($req->encode());
        $this->checkSame('T', $round->topic, 'request body roundtrip: topic');
        $this->checkSame('G', $round->consumerGroup, 'request body roundtrip: consumerGroup');
        $this->checkSame('cid@1', $round->clientId, 'request body roundtrip: clientId');
        $this->checkSame('AVG', $round->strategyName, 'request body roundtrip: strategyName');
        $this->checkSame('CLUSTERING', QueryAssignmentRequestBody::decode(null)->messageModel, 'empty request defaults to CLUSTERING');

        // Response body: the set lives under `messageQueueAssignments` (the Java wire spelling).
        $json = '{"messageQueueAssignments":[{"messageQueue":{"topic":"T","brokerName":"b","queueId":1},'
            . '"mode":"POP","attachments":{"k":"v"}}]}';
        $resp = QueryAssignmentResponseBody::decode($json);
        $this->checkSame(1, count($resp->messageQueueAssignments ?? []), 'response decodes one assignment');
        $one = $resp->messageQueueAssignments[0];
        $mq = $one->messageQueue;
        $this->check(
            $mq instanceof MessageQueue && $mq->topic === 'T' && $mq->brokerName === 'b' && $mq->queueId === 1,
            'assignment queue fields'
        );
        $this->check(MessageRequestMode::POP === $one->mode, 'assignment mode POP');
        $this->checkSame(['k' => 'v'], $one->attachments, 'assignment attachments');

        // null (missing key) vs empty (explicit clear) must stay distinguishable.
        $this->checkSame(null, QueryAssignmentResponseBody::decode('{}')->messageQueueAssignments, 'missing key decodes to null (keep previous)');
        $this->checkSame([], QueryAssignmentResponseBody::decode('{"messageQueueAssignments":[]}')->messageQueueAssignments, 'explicit empty array decodes to [] (clear)');
        $this->checkSame(null, QueryAssignmentResponseBody::decode(null)->messageQueueAssignments, 'empty body decodes to null');
        $this->checkSame(null, QueryAssignmentResponseBody::decode('')->messageQueueAssignments, 'empty string body decodes to null');

        // Unknown mode falls back to PULL; missing queue stays null (skipped by the caller).
        $odd = MessageQueueAssignment::fromDict(['mode' => 'BOGUS']);
        $this->check(MessageRequestMode::PULL === $odd->mode, 'unknown mode falls back to PULL');
        $this->checkSame(null, $odd->messageQueue, 'missing messageQueue stays null');
        $this->check(MessageRequestMode::PULL === MessageQueueAssignment::fromDict([])->mode, 'missing mode defaults to PULL');

        // toDict wire shape (mock-broker side): empty attachments serialize as {} not [].
        $a = new MessageQueueAssignment();
        $a->messageQueue = new MessageQueue('T', 'b', 2);
        $a->mode = MessageRequestMode::POP;
        $d = $a->toDict();
        $this->checkSame(['topic' => 'T', 'brokerName' => 'b', 'queueId' => 2], $d['messageQueue'], 'toDict queue shape');
        $this->checkSame('POP', $d['mode'], 'toDict mode is the enum name');
        $this->check($d['attachments'] instanceof \stdClass, 'toDict empty attachments stay a JSON object');
    }

    // ================================================================ 2. Gate matrix, pure

    private function testGateMatrix(): void
    {
        $c = new DefaultMQPushConsumer('GID-PA-GATE');
        $c->setMessageListener(new AssignOkListener());
        $this->check(!$c->usesBrokerPopAssignment(), 'default consumer (pull + clientRebalance) stays local');

        $c->popMode = true;
        $this->check(!$c->usesBrokerPopAssignment(), 'popMode alone is not enough: clientRebalance defaults to true');

        $c->clientRebalance = false;
        $this->check($c->usesBrokerPopAssignment(), 'popMode + clientRebalance=false takes the broker leg');

        $c->setMessageModel(\RocketMQ\Remoting\Protocol\MessageModel::BROADCASTING);
        $this->check(!$c->usesBrokerPopAssignment(), 'BROADCASTING never uses broker-side assignment');
        $c->setMessageModel(\RocketMQ\Remoting\Protocol\MessageModel::CLUSTERING);

        $c->setMessageListener(new AssignOrderlyListener());
        $this->check(!$c->usesBrokerPopAssignment(), 'orderly consumption stays on the local leg');
    }

    // ================================================================ 3. Live wire: normal assignment + rhythm

    private function makePopConsumer(AssignFakeBroker $broker, string $group, string $instance): DefaultMQPushConsumer
    {
        $c = new DefaultMQPushConsumer($group);
        $c->setInstanceName($instance);
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener(new AssignOkListener());
        $c->popMode = true;
        $c->clientRebalance = false;
        // Shorten the empty-pop idle backoff (1000ms by default) so the "who is popped next"
        // assertions below do not have to sleep a full second per tick.
        $c->popIntervalMillis = 100;
        return $c;
    }

    private function testBrokerAssignmentLive(AssignFakeBroker $broker): void
    {
        $group = 'GID-PA-LIVE';
        $c = $this->makePopConsumer($broker, $group, 'PA-LIVE');
        $c->subscribe(ASSIGN_TOPIC);
        $c->start();
        $instance = $c->mqClient;
        try {
            // The first assignment round runs synchronously inside start().
            $this->checkSame(2, $c->assignedQueueCountForTopic(ASSIGN_TOPIC), 'broker answer becomes the assignment (2 queues)');
            $this->checkSame(2, $c->popQueueCount(), 'POP pull surface built: one PopProcessQueue per assigned queue');
            $this->checkSame(0, $c->localOffsetCount(), 'POP keeps no local offset table');

            // The 400 request body carries exactly what the broker load-balances by.
            $reqs = $broker->logWhere(RequestCode::QUERY_ASSIGNMENT);
            $forTopic = array_values(array_filter($reqs, static fn (array $e): bool => ($e['topic'] ?? null) === ASSIGN_TOPIC));
            $this->check(count($forTopic) >= 1, 'QUERY_ASSIGNMENT was sent for the subscribed topic');
            $this->checkSame(ASSIGN_TOPIC, $forTopic[0]['topic'] ?? null, '400 body topic');
            $this->checkSame($group, $forTopic[0]['group'] ?? null, '400 body consumerGroup');
            $this->checkSame($c->clientId, $forTopic[0]['clientId'] ?? null, '400 body clientId is this client');
            $this->checkSame('AVG', $forTopic[0]['strategyName'] ?? null, '400 body strategyName is the local strategy name');
            $this->checkSame('CLUSTERING', $forTopic[0]['messageModel'] ?? null, '400 body messageModel');

            // The local-rebalance cell must never run on this path.
            $this->checkSame(0, $broker->logCount(RequestCode::GET_CONSUMER_LIST_BY_GROUP, null, $group), 'no GET_CONSUMER_LIST_BY_GROUP on the broker-assignment path');

            // The POP loops pop exactly the broker-assigned queues.
            $c->tick();
            $this->checkSame(1, $broker->logCount(RequestCode::POP_MESSAGE, ASSIGN_TOPIC, $group, 0), 'POP_MESSAGE leg for assigned queue 0');
            $this->checkSame(1, $broker->logCount(RequestCode::POP_MESSAGE, ASSIGN_TOPIC, $group, 1), 'POP_MESSAGE leg for assigned queue 1');

            // Periodic rhythm: without a wake-up the next ticks inside the interval query nothing.
            $n400 = $broker->logCount(RequestCode::QUERY_ASSIGNMENT, ASSIGN_TOPIC);
            $c->tick();
            $c->tick();
            $this->checkSame($n400, $broker->logCount(RequestCode::QUERY_ASSIGNMENT, ASSIGN_TOPIC), 'interval not due => tick sends no new 400');

            // rebalanceImmediately() wakes the poller (nextRunMillis=0) => the next tick queries again.
            $c->rebalanceImmediately();
            $c->tick();
            $this->checkSame($n400 + 1, $broker->logCount(RequestCode::QUERY_ASSIGNMENT, ASSIGN_TOPIC), 'rebalanceImmediately wakes the round on the next tick');

            // ---------------- change on the next tick ----------------
            $broker->setPlan(['topic' => ASSIGN_TOPIC, 'response' => 'success', 'queues' => [1], 'mode' => 'POP']);
            $c->rebalanceImmediately();
            $c->tick();
            $this->checkSame(1, $c->assignedQueueCountForTopic(ASSIGN_TOPIC), 'shrunken answer applied on the next tick (1 queue)');
            $this->checkSame([ASSIGN_TOPIC . ASSIGN_BROKER . '1'], $c->assignedQueueKeys(), 'revoked queue 0 dropped, queue 1 kept');
            $this->checkSame(1, $c->popQueueCount(), 'POP surface follows the shrunken assignment');

            // The revoked queue is no longer popped; the kept one still is (wait out the idle backoff first).
            usleep(250_000);
            $mark = $broker->logSize();
            $c->tick();
            $this->checkSame(0, $broker->logCount(RequestCode::POP_MESSAGE, ASSIGN_TOPIC, $group, 0, $mark), 'revoked queue 0 no longer popped');
            $this->checkSame(1, $broker->logCount(RequestCode::POP_MESSAGE, ASSIGN_TOPIC, $group, 1, $mark), 'kept queue 1 still popped');

            // ---------------- RPC failure keeps the previous assignment ----------------
            $broker->setPlan(['topic' => ASSIGN_TOPIC, 'response' => 'error']);
            $c->rebalanceImmediately();
            $c->tick();
            $this->checkSame(1, $c->assignedQueueCountForTopic(ASSIGN_TOPIC), 'broker SYSTEM_ERROR => previous assignment kept');
            $this->checkSame([ASSIGN_TOPIC . ASSIGN_BROKER . '1'], $c->assignedQueueKeys(), 'broker SYSTEM_ERROR => queue set untouched (never degrades to take-all)');
            $this->checkSame(1, $c->popQueueCount(), 'broker SYSTEM_ERROR => POP surface untouched');

            // ---------------- missing set (SUCCESS without the key) also keeps ----------------
            $broker->setPlan(['topic' => ASSIGN_TOPIC, 'response' => 'success', 'queues' => [0, 1], 'mode' => 'POP']);
            $c->rebalanceImmediately();
            $c->tick();
            $this->checkSame(2, $c->assignedQueueCountForTopic(ASSIGN_TOPIC), 'recovered broker grows the assignment back to 2');
            $broker->setPlan(['topic' => ASSIGN_TOPIC, 'response' => 'missing']);
            $c->rebalanceImmediately();
            $c->tick();
            $this->checkSame(2, $c->assignedQueueCountForTopic(ASSIGN_TOPIC), 'SUCCESS without a set => keep previous assignment');

            // ---------------- explicit empty set clears ----------------
            $broker->setPlan(['topic' => ASSIGN_TOPIC, 'response' => 'empty']);
            $c->rebalanceImmediately();
            $c->tick();
            $this->checkSame(0, $c->assignedQueueCountForTopic(ASSIGN_TOPIC), 'explicit empty set clears the assignment');
            $this->checkSame(0, $c->popQueueCount(), 'explicit empty set tears the POP surface down');

            // ---------------- TOPIC_NOT_EXIST answers empty => clears too ----------------
            $broker->setPlan(['topic' => ASSIGN_TOPIC, 'response' => 'success', 'queues' => [0], 'mode' => 'POP']);
            $c->rebalanceImmediately();
            $c->tick();
            $this->checkSame(1, $c->assignedQueueCountForTopic(ASSIGN_TOPIC), 'assignment re-established after recovery');
            $broker->setPlan(['topic' => ASSIGN_TOPIC, 'response' => 'topic_not_exist']);
            $c->rebalanceImmediately();
            $c->tick();
            $this->checkSame(0, $c->assignedQueueCountForTopic(ASSIGN_TOPIC), 'TOPIC_NOT_EXIST(17) => empty set => assignment cleared');
            $this->checkSame(0, $c->popQueueCount(), 'TOPIC_NOT_EXIST(17) => POP surface cleared');
            $this->check(in_array($c->clientId . '|' . $group, $instance?->popAssignmentPollerKeys() ?? [], true), 'poller registered in the instance while started');
        } finally {
            $this->cleanup($c);
        }
        // Shutdown unregisters the poller: the instance no longer drives any round.
        $this->checkSame([], $instance === null ? [] : $instance->popAssignmentPollerKeys(), 'shutdown unregisters the assignment poller');
    }

    // ================================================================ 4. Non-POP path keeps the local rebalance

    private function testLocalRebalanceUntouched(AssignFakeBroker $broker): void
    {
        $group = 'GID-PA-LOCAL';
        $c = new DefaultMQPushConsumer($group);
        $c->setInstanceName('PA-LOCAL');
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener(new AssignOkListener());
        // popMode stays false, clientRebalance stays true: the classic pull consumer.
        $c->subscribe(ASSIGN_TOPIC);
        $c->start();
        try {
            // Local AVG over the 2-queue route with this client as the only group member.
            $this->checkSame(2, $c->assignedQueueCountForTopic(ASSIGN_TOPIC), 'local rebalance assigns both queues via AVG');
            $this->check($broker->logCount(RequestCode::GET_CONSUMER_LIST_BY_GROUP, null, $group) >= 1, 'local rebalance consults GET_CONSUMER_LIST_BY_GROUP');
            $this->checkSame(0, $broker->logCount(RequestCode::QUERY_ASSIGNMENT, null, $group), 'local path never sends QUERY_ASSIGNMENT');
            $c->tick();
            $this->checkSame(0, $broker->logCount(RequestCode::QUERY_ASSIGNMENT, null, $group), 'tick on the local path still sends no QUERY_ASSIGNMENT');
            $this->check($broker->logCount(RequestCode::PULL_MESSAGE, ASSIGN_TOPIC, $group) >= 1, 'local path pulls as usual');
        } finally {
            $this->cleanup($c);
        }

        // A POP consumer that keeps clientRebalance=true also stays on the local leg.
        $p = $this->makePopConsumer($broker, 'GID-PA-POPLOCAL', 'PA-POPLOCAL');
        $p->clientRebalance = true;
        $p->subscribe(ASSIGN_TOPIC);
        $p->start();
        try {
            $this->checkSame(0, $broker->logCount(RequestCode::QUERY_ASSIGNMENT, null, 'GID-PA-POPLOCAL'), 'popMode + clientRebalance=true sends no 400');
            $this->checkSame(2, $p->assignedQueueCountForTopic(ASSIGN_TOPIC), 'popMode + clientRebalance=true assigns locally (all queues)');
        } finally {
            $this->cleanup($p);
        }
    }

    // ================================================================ run

    public function run(): int
    {
        $this->testProtocolPure();
        $this->testGateMatrix();

        $broker = AssignFakeBroker::start(['topic' => ASSIGN_TOPIC, 'response' => 'success', 'queues' => [0, 1], 'mode' => 'POP']);
        try {
            $this->testBrokerAssignmentLive($broker);
        } finally {
            $broker->stop();
        }

        $broker = AssignFakeBroker::start(['topic' => ASSIGN_TOPIC, 'response' => 'success', 'queues' => [0, 1], 'mode' => 'POP']);
        try {
            $this->testLocalRebalanceUntouched($broker);
        } finally {
            $broker->stop();
        }

        return $this->summary();
    }
}

exit((new RunClientPopAssignment())->run());
