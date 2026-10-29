<?php

declare(strict_types=1);

/**
 * Where a consumer with no committed offset starts — offline wire suite (no cluster).
 *
 * Behaviour baseline, for both the push and the lite pull consumer:
 *   * A stored offset always wins; the policy only places a group that has never
 *     committed one.
 *   * CONSUME_FROM_FIRST_OFFSET starts at literal 0 and asks the broker for no
 *     boundary at all (an overrun is healed by the offset-moved pull path).
 *   * CONSUME_FROM_TIMESTAMP asks for the offset of the configured instant — the
 *     instant parsed from consumeTimestamp, not "now" — except on a %RETRY% queue,
 *     which is created by the first redelivery long after that instant and
 *     therefore starts at the queue tail.
 *   * CONSUME_FROM_LAST_OFFSET (and any deprecated variant) skips the backlog on a
 *     business queue but replays a %RETRY% queue from 0: retried messages must all
 *     be retried.
 *   * consumeTimestamp is parsed unconditionally at start-up. A malformed value is
 *     a hard failure — falling back to "now" would silently turn the policy into
 *     "skip the backlog".
 *   * The default instant is boot time minus 30 minutes.
 *
 * "Which RPC drove the start" is decided by what the fake broker actually saw, with
 * a distinct offset per boundary RPC (tail=100, searched=200, head=50, committed=7),
 * so a wrong branch cannot pass by accident.
 *
 * Run: php tests/RunClientInitialOffset.php
 * The fake broker re-spawns this file with the `--broker` argument; its routes,
 * committed offsets and group membership are read fresh from JSON files on every
 * request, so rewriting those files from the parent is all a scenario needs.
 */

require_once __DIR__ . '/../bootstrap.php';

putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Common\UtilAll;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\TopicRouteData;

const IO_BROKER = 'b-init';
const IO_TAIL = 100;
const IO_SEARCHED = 200;
const IO_HEAD = 50;
const IO_STORED = 7;
const IO_CONFIGURED = '20230101000000';

if (($argv[1] ?? '') === '--broker') {
    ioBrokerMain();
    exit(0);
}

// ======================================================================
// fake broker (child process)
// ======================================================================

function ioReadJson(string $path): array
{
    $raw = is_string($path) && is_file($path) ? (string) @file_get_contents($path) : '';
    $dec = json_decode($raw, true);
    return is_array($dec) ? $dec : [];
}

function ioEnvPath(string $var): string
{
    $p = getenv($var);
    return is_string($p) ? $p : '';
}

function ioLog(array $entry): void
{
    $path = ioEnvPath('IO_LOG');
    if ($path === '') {
        return;
    }
    @file_put_contents($path, json_encode($entry, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
}

function ioRouteBody(string $addr, int $queueNums): string
{
    $qd = new QueueData(IO_BROKER, $queueNums, $queueNums, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', IO_BROKER, [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

function ioRespond(RemotingCommand $cmd, string $addr): RemotingCommand
{
    $ext = $cmd->extFields;
    $topic = (string) ($ext['topic'] ?? '');
    $group = (string) ($ext['consumerGroup'] ?? '');
    $key = $topic . '@' . (int) ($ext['queueId'] ?? 0);

    switch ($cmd->code) {
        case RequestCode::GET_ROUTEINFO_BY_TOPIC: {
            $routes = ioReadJson(ioEnvPath('IO_ROUTES'));
            $n = $routes[$topic] ?? null;
            if ($n === null) {
                return RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST);
            }
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = ioRouteBody($addr, (int) $n);
            return $resp;
        }

        case RequestCode::GET_CONSUMER_LIST_BY_GROUP: {
            $groups = ioReadJson(ioEnvPath('IO_GROUPS'));
            $cids = $groups[$group] ?? [];
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = json_encode(['consumerIdList' => array_values((array) $cids)]);
            return $resp;
        }

        case RequestCode::QUERY_CONSUMER_OFFSET: {
            ioLog(['code' => $cmd->code, 'topic' => $topic, 'group' => $group]);
            $committed = ioReadJson(ioEnvPath('IO_COMMITTED'));
            if (!array_key_exists($key, $committed)) {
                // Nothing committed: the policy has to place the queue.
                return RemotingCommand::createResponseCommand(ResponseCode::QUERY_NOT_FOUND);
            }
            if ($committed[$key] === 'FAIL') {
                // A store read failure -- NOT "nothing committed": the queue must be left
                // unplaced rather than started from a guessed position.
                return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR,
                    'offset store read failed');
            }
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = (string) (int) $committed[$key];
            return $resp;
        }

        case RequestCode::GET_MAX_OFFSET:
            ioLog(['code' => $cmd->code, 'topic' => $topic, 'group' => $group]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = (string) IO_TAIL;
            return $resp;

        case RequestCode::GET_MIN_OFFSET:
            ioLog(['code' => $cmd->code, 'topic' => $topic, 'group' => $group]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = (string) IO_HEAD;
            return $resp;

        case RequestCode::SEARCH_OFFSET_BY_TIMESTAMP:
            ioLog([
                'code' => $cmd->code,
                'topic' => $topic,
                'group' => $group,
                'timestamp' => (int) ($ext['timestamp'] ?? -1),
            ]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = (string) IO_SEARCHED;
            return $resp;

        case RequestCode::PULL_MESSAGE:
        case RequestCode::LITE_PULL_MESSAGE: {
            $offset = (int) ($ext['queueOffset'] ?? -1);
            ioLog([
                'code' => $cmd->code,
                'topic' => $topic,
                'group' => $group,
                'queueId' => (int) ($ext['queueId'] ?? 0),
                'queueOffset' => $offset,
            ]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::PULL_NOT_FOUND);
            // Echo the request: the cursor must not drift, so the offset on the
            // first frame stays the one the policy produced.
            $resp->extFields['nextBeginOffset'] = (string) $offset;
            $resp->extFields['minOffset'] = (string) IO_HEAD;
            $resp->extFields['maxOffset'] = (string) IO_TAIL;
            $resp->extFields['suggestWhichBrokerId'] = (string) MixAll::MASTER_ID;
            return $resp;
        }

        default:
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

function ioReadExact($conn, int $n): ?string
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

function ioServeConn($conn, string $addr): void
{
    stream_set_blocking($conn, true);
    while (true) {
        $head = ioReadExact($conn, 4);
        if ($head === null) {
            return;
        }
        $total = RocketMQSerializable::unpackSignedInt($head);
        if ($total <= 0) {
            return;
        }
        $rest = ioReadExact($conn, $total);
        if ($rest === null) {
            return;
        }
        try {
            $cmd = RemotingCommand::decode($head . $rest);
            $resp = ioRespond($cmd, $addr);
        } catch (\Throwable $e) {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'broker: ' . $e->getMessage());
            if (!isset($cmd)) {
                continue;
            }
        }
        $resp->opaque = $cmd->opaque;
        @fwrite($conn, $resp->encode());
    }
}

function ioBrokerMain(): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "broker: cannot listen: {$errstr}\n");
        return;
    }
    $addr = (string) stream_socket_get_name($server, false);
    fwrite(STDOUT, $addr . "\n");
    fflush(STDOUT);
    while (true) {
        $conn = @stream_socket_accept($server, 60);
        if ($conn === false) {
            break;
        }
        ioServeConn($conn, $addr);
        @fclose($conn);
    }
    @fclose($server);
}

/** Child-process fake broker handle. */
final class IoFakeBroker
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
// listener (the push consumer needs one; it never sees a message here)
// ======================================================================

final class IoNoopListener implements MessageListenerConcurrently
{
    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

// ======================================================================
// suite
// ======================================================================

final class RunClientInitialOffset
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    /** @var list<array<string,mixed>> */
    private array $log = [];

    private string $logPath;

    private string $routesPath;

    private string $committedPath;

    private string $groupsPath;

    public function check(bool $cond, string $label, string $detail = ''): void
    {
        if ($cond) {
            $this->passed++;
            return;
        }
        $this->failures[] = $label;
        printf("  FAIL %s %s\n", $label, $detail);
    }

    // ---------------- log access ----------------

    private function resetLog(): void
    {
        @file_put_contents($this->logPath, '');
        $this->log = [];
    }

    private function loadLog(): void
    {
        $this->log = [];
        foreach (explode("\n", (string) @file_get_contents($this->logPath)) as $line) {
            $line = trim($line);
            if ($line === '') {
                continue;
            }
            $row = json_decode($line, true);
            if (is_array($row)) {
                $this->log[] = $row;
            }
        }
    }

    /** The offset on the first pull frame of one (group, topic). */
    private function firstPullOffset(string $group, string $topic): ?int
    {
        foreach ($this->log as $row) {
            $code = (int) ($row['code'] ?? 0);
            if (($code === RequestCode::PULL_MESSAGE || $code === RequestCode::LITE_PULL_MESSAGE)
                && (string) ($row['group'] ?? '') === $group
                && (string) ($row['topic'] ?? '') === $topic) {
                return (int) $row['queueOffset'];
            }
        }
        return null;
    }

    private function rpcCount(int $code, string $topic = ''): int
    {
        $n = 0;
        foreach ($this->log as $row) {
            if ((int) ($row['code'] ?? -1) !== $code) {
                continue;
            }
            if ($topic !== '' && (string) ($row['topic'] ?? '') !== $topic) {
                continue;
            }
            $n++;
        }
        return $n;
    }

    /** The timestamp carried by the first SEARCH_OFFSET_BY_TIMESTAMP frame. */
    private function searchedAt(): ?int
    {
        foreach ($this->log as $row) {
            if ((int) ($row['code'] ?? -1) === RequestCode::SEARCH_OFFSET_BY_TIMESTAMP) {
                return (int) ($row['timestamp'] ?? -1);
            }
        }
        return null;
    }

    // ---------------- scenario plumbing ----------------

    /** @param array<string,int> $routes topic => queue count */
    private function setRoutes(array $routes): void
    {
        file_put_contents($this->routesPath, json_encode($routes, JSON_UNESCAPED_SLASHES));
    }

    /** @param array<string,int> $committed `topic@queueId` => offset */
    private function setCommitted(array $committed): void
    {
        file_put_contents($this->committedPath, json_encode($committed, JSON_UNESCAPED_SLASHES));
    }

    /** @param array<string,list<string>> $groups group => client ids */
    private function setGroups(array $groups): void
    {
        file_put_contents($this->groupsPath, json_encode($groups, JSON_UNESCAPED_SLASHES));
    }

    private function lite(IoFakeBroker $broker, string $group, string $instance, string $policy,
        string $topic, ?string $consumeTimestamp, string $retryTopic, bool $withRetry = true
    ): void {
        $lite = new DefaultLitePullConsumer($group);
        $lite->setInstanceName($instance);
        $lite->setNamesrvAddr($broker->addr);
        $lite->consumeFromWhere = $policy;
        if ($consumeTimestamp !== null) {
            $lite->consumeTimestamp = $consumeTimestamp;
        }
        $queues = [new MessageQueue($topic, IO_BROKER, 0)];
        if ($withRetry) {
            $queues[] = new MessageQueue($retryTopic, IO_BROKER, 0);
        }
        $lite->assign($queues);
        $lite->setSubExpressionForAssign($topic, '*');
        if ($withRetry) {
            $lite->setSubExpressionForAssign($retryTopic, '*');
        }
        $lite->start();
        $lite->tick();
        $lite->shutdown();
        MQClientInstance::removeInstance($lite->clientId);
    }

    private function push(IoFakeBroker $broker, string $group, string $instance, string $policy,
        string $topic, ?string $consumeTimestamp, int $ticks = 3
    ): void {
        $c = new DefaultMQPushConsumer($group);
        $c->setInstanceName($instance);
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener(new IoNoopListener());
        $c->consumeFromWhere = $policy;
        if ($consumeTimestamp !== null) {
            $c->consumeTimestamp = $consumeTimestamp;
        }
        $c->subscribe($topic, '*');
        $c->start();
        for ($i = 0; $i < $ticks; $i++) {
            $c->tick();
        }
        $c->shutdown();
        MQClientInstance::removeInstance($c->clientId);
    }

    // ---------------- 1. the instant itself ----------------

    private function testInstant(): void
    {
        $parsed = UtilAll::parseConsumeTimestamp(IO_CONFIGURED);
        $this->check(UtilAll::timeMillisToHumanString3($parsed) === IO_CONFIGURED,
            'the configured instant round-trips through the formatter',
            UtilAll::timeMillisToHumanString3($parsed));
        $this->check(UtilAll::parseConsumeTimestamp('20230101000001') - $parsed === 1000,
            'one second of the instant is one millisecond');

        $def = UtilAll::defaultConsumeTimestamp();
        $this->check((bool) preg_match('/^\d{14}$/', $def), 'the default instant is a legal yyyyMMddHHmmss', $def);
        $ageMinutes = (UtilAll::currentTimeMillis() - UtilAll::parseConsumeTimestamp($def)) / 60000.0;
        $this->check($ageMinutes >= 29 && $ageMinutes <= 31,
            'and sits half an hour behind boot time', (string) $ageMinutes);

        foreach (['2023010100000', '', 'not-a-time', '2023-01-01 00:00:00', '202313010000001'] as $bad) {
            $msg = '';
            try {
                UtilAll::parseConsumeTimestamp($bad);
            } catch (MQClientException $e) {
                $msg = $e->getMessage();
            }
            $this->check($msg !== '' && str_contains($msg,
                    'consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received')
                && str_ends_with($msg, $bad),
                sprintf('a malformed instant is rejected: "%s"', $bad), $msg);
        }
        $ok = true;
        try {
            UtilAll::parseConsumeTimestamp(IO_CONFIGURED);
        } catch (\Throwable) {
            $ok = false;
        }
        $this->check($ok, 'the paired control: a legal instant is accepted');
    }

    // ---------------- 2. start-up rejects a bad instant ----------------

    private function testStartupRejectsBadInstant(): void
    {
        $push = new DefaultMQPushConsumer('GID-IO-PUSH-BADTS');
        $push->setNameServerAddresses(['127.0.0.1:1']);
        $push->setMessageListener(new IoNoopListener());
        $push->subscribe('IoAnyTopic', '*');
        $push->consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_TIMESTAMP;
        $push->consumeTimestamp = '202301010000';
        $msg = '';
        try {
            $push->start();
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
        }
        $this->check(str_contains($msg, 'consumeTimestamp is invalid'),
            'push: a malformed instant stops start-up', $msg);

        $lite = new DefaultLitePullConsumer('GID-IO-LITE-BADTS');
        $lite->setNamesrvAddr('127.0.0.1:1');
        $lite->assign([new MessageQueue('IoAnyTopic', IO_BROKER, 0)]);
        $lite->consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_TIMESTAMP;
        $lite->consumeTimestamp = '2023010100000';
        $msg = '';
        try {
            $lite->start();
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
        }
        $this->check(str_contains($msg, 'consumeTimestamp is invalid'),
            'lite: a malformed instant stops start-up', $msg);
        $this->check(!$lite->isRunning(), 'lite: and it is not left half started');

        // The same value is legal for a policy that never looks at it, so the
        // check really is unconditional rather than TIMESTAMP-only.
        $first = new DefaultLitePullConsumer('GID-IO-LITE-BADTS-FIRST');
        $first->setNamesrvAddr('127.0.0.1:1');
        $first->assign([new MessageQueue('IoAnyTopic', IO_BROKER, 0)]);
        $first->consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET;
        $first->consumeTimestamp = '2023010100000';
        $msg = '';
        try {
            $first->start();
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
        }
        $this->check(str_contains($msg, 'consumeTimestamp is invalid'),
            'and the check is unconditional: a non-TIMESTAMP policy rejects it too', $msg);
    }

    // ---------------- 3. the policy table on the lite path ----------------

    private function testLiteTable(IoFakeBroker $broker): void
    {
        $topic = 'IoLiteTopic';
        $retry = '%RETRY%GID-IO-LITE-LAST';
        $this->resetLog();
        $this->setCommitted([]);
        $this->setRoutes([$topic => 1, $retry => 1]);
        $this->lite($broker, 'GID-IO-LITE-LAST', 'IO-LA', ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
            $topic, null, $retry);
        $this->loadLog();
        $this->check($this->firstPullOffset('GID-IO-LITE-LAST', $topic) === IO_TAIL,
            'lite LAST: a business queue skips the backlog',
            $this->describe('GID-IO-LITE-LAST', $topic));
        $this->check($this->firstPullOffset('GID-IO-LITE-LAST', $retry) === 0,
            'lite LAST: a retry queue replays from 0',
            $this->describe('GID-IO-LITE-LAST', $retry));
        $this->check($this->rpcCount(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP) === 0,
            'lite LAST: no backlog search is issued');

        // A deprecated variant of LAST falls through to the same branch.
        $this->resetLog();
        $this->lite($broker, 'GID-IO-LITE-ALIAS', 'IO-LAL', 'CONSUME_FROM_LAST_OFFSET_OVERMITTED',
            $topic, null, $retry);
        $this->loadLog();
        $this->check($this->firstPullOffset('GID-IO-LITE-ALIAS', $topic) === IO_TAIL,
            'lite LAST_OVERMITTED: still skips the backlog',
            $this->describe('GID-IO-LITE-ALIAS', $topic));
        $this->check($this->firstPullOffset('GID-IO-LITE-ALIAS', $retry) === 0,
            'lite LAST_OVERMITTED: still replays the retry queue from 0',
            $this->describe('GID-IO-LITE-ALIAS', $retry));

        $retry = '%RETRY%GID-IO-LITE-FIRST';
        $this->resetLog();
        $this->setRoutes([$topic => 1, $retry => 1]);
        $this->lite($broker, 'GID-IO-LITE-FIRST', 'IO-LF', ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET,
            $topic, null, $retry);
        $this->loadLog();
        $this->check($this->firstPullOffset('GID-IO-LITE-FIRST', $topic) === 0,
            'lite FIRST: a business queue starts at 0 without asking for the head',
            $this->describe('GID-IO-LITE-FIRST', $topic));
        $this->check($this->firstPullOffset('GID-IO-LITE-FIRST', $retry) === 0,
            'lite FIRST: the retry queue starts at 0 too',
            $this->describe('GID-IO-LITE-FIRST', $retry));
        $this->check($this->rpcCount(RequestCode::GET_MIN_OFFSET) === 0
            && $this->rpcCount(RequestCode::GET_MAX_OFFSET) === 0
            && $this->rpcCount(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP) === 0,
            'lite FIRST: no boundary RPC at all');

        $retry = '%RETRY%GID-IO-LITE-TS';
        $this->resetLog();
        $this->setRoutes([$topic => 1, $retry => 1]);
        $this->lite($broker, 'GID-IO-LITE-TS', 'IO-LT', ConsumeFromWhere::CONSUME_FROM_TIMESTAMP,
            $topic, IO_CONFIGURED, $retry);
        $this->loadLog();
        $this->check($this->firstPullOffset('GID-IO-LITE-TS', $topic) === IO_SEARCHED,
            'lite TIMESTAMP: a business queue starts at the offset of the configured instant',
            $this->describe('GID-IO-LITE-TS', $topic));
        $this->check($this->searchedAt() === UtilAll::parseConsumeTimestamp(IO_CONFIGURED),
            'lite TIMESTAMP: the instant searched for is the configured one, not now',
            sprintf('%s vs %s', (string) $this->searchedAt(),
                (string) UtilAll::parseConsumeTimestamp(IO_CONFIGURED)));
        $this->check($this->firstPullOffset('GID-IO-LITE-TS', $retry) === IO_TAIL,
            'lite TIMESTAMP: the retry queue cannot be searched, so it takes the tail',
            $this->describe('GID-IO-LITE-TS', $retry));
        $this->check($this->rpcCount(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP) === 1,
            'lite TIMESTAMP: only the business queue is searched',
            (string) $this->rpcCount(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP));

        $retry = '%RETRY%GID-IO-LITE-STORED';
        $this->resetLog();
        $this->setCommitted([$topic . '@0' => IO_STORED, $retry . '@0' => IO_STORED]);
        $this->setRoutes([$topic => 1, $retry => 1]);
        $this->lite($broker, 'GID-IO-LITE-STORED', 'IO-LS', ConsumeFromWhere::CONSUME_FROM_TIMESTAMP,
            $topic, IO_CONFIGURED, $retry);
        $this->loadLog();
        $this->check($this->firstPullOffset('GID-IO-LITE-STORED', $topic) === IO_STORED,
            'lite stored: a committed offset beats the policy on the business queue',
            $this->describe('GID-IO-LITE-STORED', $topic));
        $this->check($this->firstPullOffset('GID-IO-LITE-STORED', $retry) === IO_STORED,
            'lite stored: and beats it on the retry queue',
            $this->describe('GID-IO-LITE-STORED', $retry));
        $this->check($this->rpcCount(RequestCode::GET_MAX_OFFSET) === 0
            && $this->rpcCount(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP) === 0,
            'lite stored: no boundary RPC is issued');
        $this->setCommitted([]);
    }

    private function describe(string $group, string $topic): string
    {
        $off = $this->firstPullOffset($group, $topic);
        return $off === null ? 'no pull frame' : (string) $off;
    }

    // ---------------- 4. the same table on the push path ----------------

    private function testPushTable(IoFakeBroker $broker): void
    {
        $topic = 'IoPushTopic';
        $cases = [
            ['GID-IO-PUSH-LAST', 'IO-PA', ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET, null,
                IO_TAIL, 0, 'LAST'],
            ['GID-IO-PUSH-FIRST', 'IO-PB', ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET, null,
                0, 0, 'FIRST'],
            ['GID-IO-PUSH-TS', 'IO-PC', ConsumeFromWhere::CONSUME_FROM_TIMESTAMP, IO_CONFIGURED,
                IO_SEARCHED, IO_TAIL, 'TIMESTAMP'],
        ];
        foreach ($cases as [$group, $instance, $policy, $ts, $wantBusiness, $wantRetry, $label]) {
            $retry = MixAll::getRetryTopic($group);
            $this->resetLog();
            $this->setCommitted([]);
            $this->setRoutes([$topic => 1, $retry => 1]);
            $this->setGroups([$group => [MixAll::clientIdFor($instance, null, false)]]);
            $this->push($broker, $group, $instance, $policy, $topic, $ts);
            $this->loadLog();
            $this->check($this->firstPullOffset($group, $topic) === $wantBusiness,
                sprintf('push %s: a business queue starts where the policy puts it', $label),
                $this->describe($group, $topic));
            $this->check($this->firstPullOffset($group, $retry) === $wantRetry,
                sprintf('push %s: a retry queue starts where the policy puts it', $label),
                $this->describe($group, $retry));
        }

        // LAST must not search the backlog, FIRST must not ask for a boundary, and
        // a committed offset must outrank the policy.
        $retry = MixAll::getRetryTopic('GID-IO-PUSH-STORED');
        $this->resetLog();
        $this->setCommitted([$topic . '@0' => IO_STORED]);
        $this->setRoutes([$topic => 1, $retry => 1]);
        $this->setGroups(['GID-IO-PUSH-STORED' => [MixAll::clientIdFor('IO-PD', null, false)]]);
        $this->push($broker, 'GID-IO-PUSH-STORED', 'IO-PD', ConsumeFromWhere::CONSUME_FROM_TIMESTAMP,
            $topic, IO_CONFIGURED);
        $this->loadLog();
        $this->check($this->firstPullOffset('GID-IO-PUSH-STORED', $topic) === IO_STORED,
            'push stored: a committed offset beats the policy',
            $this->describe('GID-IO-PUSH-STORED', $topic));
        $this->check($this->rpcCount(RequestCode::GET_MAX_OFFSET, $topic) === 0
            && $this->rpcCount(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP, $topic) === 0,
            'push stored: no boundary RPC for the queue that has one',
            sprintf('max=%d search=%d', $this->rpcCount(RequestCode::GET_MAX_OFFSET, $topic),
                $this->rpcCount(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP, $topic)));
        // The retry queue has nothing committed, so its policy branch still runs.
        $this->check($this->firstPullOffset('GID-IO-PUSH-STORED', $retry) === IO_TAIL,
            'push stored: a queue without one still follows the policy',
            $this->describe('GID-IO-PUSH-STORED', $retry));
        $this->setCommitted([]);
    }

    // ---------------- 5. a failed store read is not "nothing committed" ----------------

    private function testPushUnreadableOffset(IoFakeBroker $broker): void
    {
        $topic = 'IoReadFailTopic';
        $group = 'GID-IO-PUSH-READFAIL';
        $retry = MixAll::getRetryTopic($group);
        $this->resetLog();
        $this->setRoutes([$topic => 1, $retry => 1]);
        $this->setGroups([$group => [MixAll::clientIdFor('IO-PF', null, false)]]);
        $this->setCommitted([$topic . '@0' => 'FAIL']);

        $c = new DefaultMQPushConsumer($group);
        $c->setInstanceName('IO-PF');
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener(new IoNoopListener());
        $c->consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
        $c->subscribe($topic, '*');
        $c->start();
        // The next rebalance is 20s out once the round has produced an assignment, so the
        // clock is advanced explicitly instead of sleeping.
        $t0 = UtilAll::currentTimeMillis();
        $c->tick($t0);
        $c->tick($t0);
        $this->loadLog();
        $this->check($this->firstPullOffset($group, $topic) === null,
            'push read failure: the queue is skipped for the round, never pulled from a guess',
            $this->describe($group, $topic));
        $this->check($this->rpcCount(RequestCode::GET_MAX_OFFSET, $topic) === 0,
            'push read failure: and no tail query is issued for it',
            (string) $this->rpcCount(RequestCode::GET_MAX_OFFSET, $topic));

        // The store recovers: the next round places the queue and resumes at the committed offset.
        $this->setCommitted([$topic . '@0' => IO_STORED]);
        $c->tick($t0 + 21000);
        $c->tick($t0 + 21000);
        $this->loadLog();
        $this->check($this->firstPullOffset($group, $topic) === IO_STORED,
            'push read failure: once the store answers, the queue resumes at the committed offset',
            $this->describe($group, $topic));

        $c->shutdown();
        MQClientInstance::removeInstance($c->clientId);
        $this->setCommitted([]);
    }

    // ---------------- run ----------------

    public function run(): int
    {
        $tmpDir = sys_get_temp_dir() . '/rmq-initial-offset-' . getmypid();
        if (!is_dir($tmpDir)) {
            @mkdir($tmpDir, 0777, true);
        }
        $this->logPath = $tmpDir . '/broker.jsonl';
        $this->routesPath = $tmpDir . '/routes.json';
        $this->committedPath = $tmpDir . '/committed.json';
        $this->groupsPath = $tmpDir . '/groups.json';
        @unlink($this->logPath);
        file_put_contents($this->logPath, '');
        $this->setRoutes([]);
        $this->setCommitted([]);
        $this->setGroups([]);
        putenv("IO_LOG={$this->logPath}");
        putenv("IO_ROUTES={$this->routesPath}");
        putenv("IO_COMMITTED={$this->committedPath}");
        putenv("IO_GROUPS={$this->groupsPath}");

        $this->testInstant();
        $this->testStartupRejectsBadInstant();

        $broker = IoFakeBroker::start();
        try {
            $this->testLiteTable($broker);
            $this->testPushTable($broker);
            $this->testPushUnreadableOffset($broker);
        } finally {
            $broker->stop();
            putenv('IO_LOG');
            putenv('IO_ROUTES');
            putenv('IO_COMMITTED');
            putenv('IO_GROUPS');
            @unlink($this->logPath);
            @unlink($this->routesPath);
            @unlink($this->committedPath);
            @unlink($this->groupsPath);
            @rmdir($tmpDir);
        }

        printf("\ninitial offset policy: %d checks, %d failed\n", $this->passed, count($this->failures));
        return count($this->failures) === 0 ? 0 : 1;
    }
}

exit((new RunClientInitialOffset())->run());
