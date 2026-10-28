<?php

declare(strict_types=1);

/**
 * Lite pull consumer "topic message queue set change" listener
 * (registerTopicMessageQueueChangeListener) offline unit test -- no cluster needed.
 *
 * This locks in this port's own contract; all criteria come from live wire traffic:
 *   * The callback fires once only when the **queue set** actually changed relative
 *     to the previous snapshot; a reordering alone is not a change.
 *   * Registered before start = no snapshot, so the first pass always fires once
 *     (handing the current set to the caller);
 *     registered while running = a snapshot is recorded immediately, so the current
 *     state is not re-reported as a "change".
 *   * Every comparison pass **asks the name server fresh** (no route cache), otherwise
 *     a scale-up would only become visible after one polling cycle at best.
 *   * When queues cannot be found it **throws** instead of returning an empty list --
 *     "not found" != "shrunk to 0 queues"; the latter would deliver a fake shrink
 *     callback to the listener and flush the snapshot to an empty set.
 *   * One topic failing to resolve only skips that topic itself; the rest of this
 *     round's topics are still compared as usual.
 *   * Registering the same topic twice overwrites the old listener.
 *   * Namespacing: register with the bare name; comparison and callbacks both use
 *     the one namespaced key, and never double-wrap.
 *
 * PHP has no threads; the comparison pass is driven by tick() on a period. The unit
 * test calls fetchTopicMessageQueuesAndCompare() directly to control ordering; only
 * one case goes through tick().
 *
 * Run: php tests/RunClientLiteTopicChange.php
 * The fake name server is in the same process family as this file: it re-spawns
 * itself with the `--ns` argument, and route contents are freshly read each time
 * from a JSON file (topic => read queue count); the parent editing that file equals
 * "cluster scale up/down".
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\TopicRouteData;

if (($argv[1] ?? '') === '--ns') {
    liteNsMain();
    exit(0);
}

$GLOBALS['LITE_PASS'] = 0;
$GLOBALS['LITE_FAIL'] = 0;

function liteCheck(bool $ok, string $name, string $detail = ''): void
{
    if ($ok) {
        $GLOBALS['LITE_PASS']++;
        return;
    }
    $GLOBALS['LITE_FAIL']++;
    printf("  FAIL %s %s\n", $name, $detail);
}

// ======================================================================
// Fake name server (child process)
// ======================================================================

function liteNsReadExact($conn, int $n): ?string
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

function liteNsReadFrame($conn): ?RemotingCommand
{
    $head = liteNsReadExact($conn, 4);
    if ($head === null) {
        return null;
    }
    $total = RocketMQSerializable::unpackSignedInt($head);
    if ($total <= 0) {
        return null;
    }
    $rest = liteNsReadExact($conn, $total);
    if ($rest === null) {
        return null;
    }
    return RemotingCommand::decode($head . $rest);
}

/** Route contents are read fresh each time: the parent editing this file equals "cluster scale up/down". */
function liteRoutes(): array
{
    $path = (string) getenv('RMQ_LITE_ROUTES');
    $raw = is_file($path) ? (string) file_get_contents($path) : '';
    $map = json_decode($raw, true);
    return is_array($map) ? $map : [];
}

function liteNsLog(string $topic): void
{
    $path = (string) getenv('RMQ_LITE_LOG');
    @file_put_contents($path, $topic . "\n", FILE_APPEND);
}

function liteNsRespond(RemotingCommand $cmd): RemotingCommand
{
    $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    if ($cmd->code === RequestCode::GET_ROUTEINFO_BY_TOPIC) {
        $topic = (string) ($cmd->extFields['topic'] ?? '');
        liteNsLog($topic);
        $routes = liteRoutes();
        if (!array_key_exists($topic, $routes)) {
            return RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST);
        }
        $n = (int) $routes[$topic];
        $trd = new TopicRouteData();
        $trd->queueDatas = [new QueueData('b-lite', $n, $n, PermName::PERM_READ | PermName::PERM_WRITE, 0)];
        $trd->brokerDatas = [new BrokerData('DefaultCluster', 'b-lite', [MixAll::MASTER_ID => $GLOBALS['LITE_NS_ADDR']])];
        $resp->body = $trd->encode();
        return $resp;
    }
    return $resp;
}

function liteNsServe($conn): void
{
    stream_set_blocking($conn, true);
    while (($cmd = liteNsReadFrame($conn)) !== null) {
        try {
            $resp = liteNsRespond($cmd);
        } catch (\Throwable $e) {
            $resp = RemotingCommand::createResponseCommand(
                ResponseCode::SYSTEM_ERROR,
                'ns error: ' . $e->getMessage()
            );
        }
        $resp->opaque = $cmd->opaque;
        @fwrite($conn, (string) $resp->encode());
    }
}

function liteNsMain(): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "ns: cannot listen: {$errstr}\n");
        return;
    }
    $GLOBALS['LITE_NS_ADDR'] = (string) stream_socket_get_name($server, false);
    fwrite(STDOUT, $GLOBALS['LITE_NS_ADDR'] . "\n");
    fflush(STDOUT);
    while (true) {
        $conn = @stream_socket_accept($server, 60);
        if ($conn === false) {
            break;
        }
        liteNsServe($conn);
        @fclose($conn);
    }
    @fclose($server);
}

/** Child-process fake name server handle. */
final class LiteFakeNameserver
{
    /** @var resource */
    private $proc;
    /** @var array<int, resource> */
    private array $pipes;
    public string $addr;
    public string $routesFile;
    public string $logFile;
    private int $seq;

    private function __construct($proc, array $pipes, string $addr, int $seq)
    {
        $this->proc = $proc;
        $this->pipes = $pipes;
        $this->addr = $addr;
        $this->seq = $seq;
        $this->routesFile = sys_get_temp_dir() . "/rmq_lite_routes_$seq.json";
        $this->logFile = sys_get_temp_dir() . "/rmq_lite_log_$seq.txt";
        $this->writeRoutes([]);
        @file_put_contents($this->logFile, '');
    }

    public static function start(int $seq): self
    {
        $seqDir = sys_get_temp_dir() . "/rmq_lite_$seq";
        @mkdir($seqDir);
        $routes = $seqDir . '/routes.json';
        $log = $seqDir . '/log.txt';
        $cmd = [PHP_BINARY, __FILE__, '--ns'];
        $desc = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $env = getenv();
        $env['RMQ_LITE_ROUTES'] = $routes;
        $env['RMQ_LITE_LOG'] = $log;
        // Pin client logs to the temp dir: this port writes logs under cwd by default,
        // and the test must not grow logs/ inside the repo.
        $env['ROCKETMQ_CLIENT_LOG_DIR'] = $seqDir;
        $proc = proc_open($cmd, $desc, $pipes, null, $env);
        if (!is_resource($proc)) {
            throw new RuntimeException('cannot spawn fake nameserver');
        }
        $line = fgets($pipes[1]);
        $addr = $line === false ? '' : trim($line);
        if ($addr === '') {
            $err = is_resource($pipes[2]) ? (string) stream_get_contents($pipes[2]) : '';
            proc_terminate($proc);
            proc_close($proc);
            throw new RuntimeException('fake nameserver did not report address: ' . $err);
        }
        $self = new self($proc, $pipes, $addr, $seq);
        $self->routesFile = $routes;
        $self->logFile = $log;
        return $self;
    }

    /** @param array<string,int> $map topic => read queue count */
    public function writeRoutes(array $map): void
    {
        @file_put_contents($this->routesFile, json_encode($map, JSON_UNESCAPED_SLASHES));
    }

    public function setQueues(string $topic, int $n): void
    {
        $map = json_decode((string) @file_get_contents($this->routesFile), true);
        $map = is_array($map) ? $map : [];
        $map[$topic] = $n;
        $this->writeRoutes($map);
    }

    public function drop(string $topic): void
    {
        $map = json_decode((string) @file_get_contents($this->routesFile), true);
        $map = is_array($map) ? $map : [];
        unset($map[$topic]);
        $this->writeRoutes($map);
    }

    /** @return list<string> route-query topics received so far, in arrival order */
    public function queries(): array
    {
        $raw = (string) @file_get_contents($this->logFile);
        return array_values(array_filter(explode("\n", $raw), static fn($l) => $l !== ''));
    }

    public function clearQueries(): void
    {
        @file_put_contents($this->logFile, '');
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
        @unlink($this->routesFile);
        @unlink($this->logFile);
    }
}

// ======================================================================
// Recording queue-change listener
// ======================================================================

final class LiteRecorder
{
    /** @var list<array{0:string,1:list<int>}> */
    public array $events = [];

    public function onChanged(string $topic, array $messageQueues): void
    {
        $ids = array_map(static fn(MessageQueue $mq) => $mq->queueId, $messageQueues);
        sort($ids);
        $this->events[] = [$topic, $ids];
    }

    public function count(): int
    {
        return count($this->events);
    }
}

// ======================================================================
// Scaffolding
// ======================================================================

final class LiteHarness
{
    public static int $seq = 0;

    /** @return array{0:LiteFakeNameserver,1:string} fake endpoint + this case's own topic */
    public static function open(string $topicName, int $queues = 1): array
    {
        self::$seq++;
        $ns = LiteFakeNameserver::start(self::$seq);
        $ns->setQueues($topicName, $queues);
        return [$ns, $topicName];
    }

    /** @param array<string,mixed> $extra */
    public static function started(
        LiteFakeNameserver $ns,
        string $instance,
        string $group,
        string $topic,
        array $extra = []
    ): DefaultLitePullConsumer {
        $c = new DefaultLitePullConsumer($group, namespace: (string) ($extra['namespace'] ?? ''));
        $c->setInstanceName($instance);
        $c->setNamesrvAddr($ns->addr);
        $c->subscribe($topic, '*');
        $c->start();
        return $c;
    }

    public static function idsOf(LiteRecorder $rec, int $index): string
    {
        return implode(',', $rec->events[$index][1] ?? []);
    }

    public static function topicOf(LiteRecorder $rec, int $index): string
    {
        return (string) ($rec->events[$index][0] ?? '');
    }
}

$GROUP = 'GID_lite_qc_unit';

// ---------------------------------------------------------------- 1. Argument guards and period
{
    $c = new DefaultLitePullConsumer($GROUP);
    foreach ([
        ['empty topic', static function (DefaultLitePullConsumer $x): void {
            $x->registerTopicMessageQueueChangeListener('', new LiteRecorder());
        }],
        ['blank topic', static function (DefaultLitePullConsumer $x): void {
            $x->registerTopicMessageQueueChangeListener('   ', new LiteRecorder());
        }],
        ['null listener', static function (DefaultLitePullConsumer $x): void {
            $x->registerTopicMessageQueueChangeListener('AnyTopic', null);
        }],
    ] as [$label, $act]) {
        $threw = '';
        try {
            $act($c);
        } catch (MQClientException $e) {
            $threw = $e->getMessage();
        }
        liteCheck(str_contains($threw, 'Topic or listener is null'), "$label rejected", $threw);
    }

    liteCheck($c->topicMetadataCheckIntervalMillis() === 30000, 'default compare period is 30s');
    $c->setTopicMetadataCheckIntervalMillis(0);
    liteCheck(
        $c->topicMetadataCheckIntervalMillis() === 1000,
        'period floor clamps to 1s (0 would turn every tick into an RPC storm)'
    );
    $c->setTopicMetadataCheckIntervalMillis(5000);
    liteCheck($c->topicMetadataCheckIntervalMillis() === 5000, 'period can be raised');
}

// ---------------------------------------------------------------- 2. Fetching queues before start
{
    $c = new DefaultLitePullConsumer($GROUP);
    $threw = '';
    try {
        $c->fetchMessageQueues('AnyTopic');
    } catch (MQClientException $e) {
        $threw = $e->getMessage();
    }
    // An empty list would be read by callers as "no queues available" and stop;
    // before start the error must be explicit.
    liteCheck(str_contains($threw, 'not running'), 'fetching queues before start fails outright', $threw);
}

// ---------------------------------------------------------------- 3. Registered before start
{
    [$ns, $topic] = LiteHarness::open('LiteQcPre', 2);
    $rec = new LiteRecorder();
    $c = new DefaultLitePullConsumer($GROUP);
    $c->setInstanceName('lite_qc_pre');
    $c->setNamesrvAddr($ns->addr);
    $c->subscribe($topic, '*');
    $c->registerTopicMessageQueueChangeListener($topic, $rec);
    $c->start();
    // No queues could be fetched at registration time => no snapshot => the first
    // pass hands over the "current set" as a change.
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, 'first pass after start fires once');
    liteCheck(LiteHarness::idsOf($rec, 0) === '0,1', 'first callback carries the current queue set', LiteHarness::idsOf($rec, 0));
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 0, 'no repeat callback while the set is unchanged');
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 4. Registered while running + scale up/down
{
    [$ns, $topic] = LiteHarness::open('LiteQcScale', 1);
    $c = LiteHarness::started($ns, 'lite_qc_scale', $GROUP, $topic);
    $rec = new LiteRecorder();
    $c->registerTopicMessageQueueChangeListener($topic, $rec);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 0, 'first pass after mid-run registration does not false-fire');

    $ns->setQueues($topic, 3);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, 'scale-up is observed');
    liteCheck(LiteHarness::idsOf($rec, 0) === '0,1,2', 'callback carries the scaled-up set', LiteHarness::idsOf($rec, 0));
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 0, 'same set does not fire again');

    $ns->setQueues($topic, 2);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, 'scale-down is also observed');
    liteCheck(LiteHarness::idsOf($rec, 1) === '0,1', 'callback carries the scaled-down set', LiteHarness::idsOf($rec, 1));
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 5. Every pass asks the name server fresh
{
    [$ns, $topic] = LiteHarness::open('LiteQcFresh', 2);
    $c = LiteHarness::started($ns, 'lite_qc_fresh', $GROUP, $topic);
    $rec = new LiteRecorder();
    $c->registerTopicMessageQueueChangeListener($topic, $rec);
    $ns->clearQueries();
    $c->fetchTopicMessageQueuesAndCompare();
    $first = count($ns->queries());
    $ns->clearQueries();
    $c->fetchTopicMessageQueuesAndCompare();
    $second = count($ns->queries());
    liteCheck($first >= 1, 'the comparison pass queries routes', (string) $first);
    // With a cache, a scale-up would only surface after one polling cycle, delaying
    // the listener by a full period.
    liteCheck($second >= 1, 'the next pass queries again', (string) $second);
    liteCheck($rec->count() === 0, 'no callback when the set is unchanged, even after querying');
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 6. One failing topic does not starve the others
{
    [$ns, $topic] = LiteHarness::open('LiteQcIsolate', 2);
    $ghost = 'LiteQcGhost';
    $good = new LiteRecorder();
    $bad = new LiteRecorder();
    $c = LiteHarness::started($ns, 'lite_qc_isolate', $GROUP, $topic);
    // Register the unresolvable one first: it sorts first, and if the exception
    // escaped the loop the healthy one would never get its callback.
    $c->registerTopicMessageQueueChangeListener($ghost, $bad);
    $c->registerTopicMessageQueueChangeListener($topic, $good);
    $ns->setQueues($topic, 3);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, 'a bad topic does not affect the rest of this round');
    liteCheck($bad->count() === 0, 'unresolvable topic fires no callback (and never pretends to shrink with an empty list)');
    liteCheck(LiteHarness::idsOf($good, 0) === '0,1,2', 'healthy topic receives the new set', LiteHarness::idsOf($good, 0));
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 7. Error when queues cannot be found
{
    [$ns, $topic] = LiteHarness::open('LiteQcEmpty', 1);
    $c = LiteHarness::started($ns, 'lite_qc_empty', $GROUP, $topic);
    $ns->setQueues($topic, 0);   // route still there, read queue count gone
    $threw = '';
    try {
        $c->fetchMessageQueues($topic);
    } catch (MQClientException $e) {
        $threw = $e->getMessage();
    }
    liteCheck(str_contains($threw, 'Can not find Message Queue'), 'errors when no queues are available', $threw);
    $ns->drop($topic);           // no route for this topic at all
    $threw = '';
    try {
        $c->fetchMessageQueues($topic);
    } catch (MQClientException $e) {
        $threw = $e->getMessage();
    }
    liteCheck(str_contains($threw, 'Can not find Message Queue'), 'same error when the route is missing entirely', $threw);
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 8. Re-registering overwrites the old listener
{
    [$ns, $topic] = LiteHarness::open('LiteQcOverwrite', 1);
    $c = LiteHarness::started($ns, 'lite_qc_overwrite', $GROUP, $topic);
    $first = new LiteRecorder();
    $second = new LiteRecorder();
    $c->registerTopicMessageQueueChangeListener($topic, $first);
    $c->registerTopicMessageQueueChangeListener($topic, $second);
    $ns->setQueues($topic, 2);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, 'after overwrite the same topic fires only once');
    liteCheck($first->count() === 0, 'old listener receives no callback');
    liteCheck($second->count() === 1, 'only the new listener is notified');
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 9. Namespace semantics
{
    $nameSpace = 'MQ_INST_lite';
    $plain = 'LiteQcNs';
    $wrapped = $nameSpace . '%' . $plain;
    [$ns, $unused] = LiteHarness::open($wrapped, 1);
    $rec = new LiteRecorder();
    $c = LiteHarness::started($ns, 'lite_qc_ns', $GROUP, $plain, ['namespace' => $nameSpace]);
    $c->registerTopicMessageQueueChangeListener($plain, $rec);
    $ns->clearQueries();
    $ns->setQueues($wrapped, 2);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, 'comparison detects changes with a namespace too');
    liteCheck(LiteHarness::topicOf($rec, 0) === $wrapped, 'callback topic is the namespaced one', LiteHarness::topicOf($rec, 0));
    liteCheck(LiteHarness::idsOf($rec, 0) === '0,1', 'callback carries the scaled-up set', LiteHarness::idsOf($rec, 0));
    $asked = $ns->queries();
    liteCheck(in_array($wrapped, $asked, true), 'comparison queries routes with the namespaced key', implode('|', $asked));
    liteCheck(
        !in_array($nameSpace . '%' . $wrapped, $asked, true),
        'never a double-wrapped query',
        implode('|', $asked)
    );
    // Re-register with the already-namespaced key: it just overwrites the same
    // entry, no second prefix grows.
    $c->registerTopicMessageQueueChangeListener($wrapped, $rec);
    $ns->clearQueries();
    $ns->setQueues($wrapped, 3);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, 're-registering the namespaced key does not add a second listener');
    liteCheck($rec->count() === 2, 'callback still comes exactly once', (string) $rec->count());
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 10. tick really drives the comparison
{
    [$ns, $topic] = LiteHarness::open('LiteQcTick', 1);
    $c = LiteHarness::started($ns, 'lite_qc_tick', $GROUP, $topic);
    $rec = new LiteRecorder();
    $c->registerTopicMessageQueueChangeListener($topic, $rec);
    $ns->setQueues($topic, 2);
    // The first check is normally scheduled 10s after start; pull it earlier here
    // so tick() takes the real branch.
    $c->nextMetadataCheckAt = microtime(true) - 1.0;
    $c->tick();
    liteCheck($rec->count() === 1, 'tick runs a comparison round by itself', (string) $rec->count());
    liteCheck(LiteHarness::idsOf($rec, 0) === '0,1', 'the tick-driven pass receives the new set', LiteHarness::idsOf($rec, 0));
    liteCheck(
        $c->nextMetadataCheckAt > microtime(true),
        'after a round the next pass is pushed to the next period',
        (string) ($c->nextMetadataCheckAt - microtime(true))
    );
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 11. Listener throwing does not break the comparison
{
    [$ns, $topic] = LiteHarness::open('LiteQcBoom', 1);
    $c = LiteHarness::started($ns, 'lite_qc_boom', $GROUP, $topic);
    $blowUp = new LiteRecorder();
    $c->registerTopicMessageQueueChangeListener($topic, function (string $topic, array $mqs) use ($blowUp): void {
        $blowUp->onChanged($topic, $mqs);
        throw new RuntimeException('listener bug');
    });
    $ns->setQueues($topic, 2);
    $fired = 0;
    try {
        $fired = $c->fetchTopicMessageQueuesAndCompare();
    } catch (Throwable $e) {
        liteCheck(false, 'a caller listener throwing must not take down the comparison', $e->getMessage());
    }
    liteCheck($fired === 1, 'callback still counts as one fire');
    // The snapshot has already advanced, so the exception does not re-report the
    // same set on the next round.
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 0, 'snapshot still applies after the pass that threw');
    $c->shutdown();
    $ns->stop();
}

printf(
    "\n%s (%d checks)\n",
    $GLOBALS['LITE_FAIL'] === 0 ? 'ALL TESTS PASSED' : sprintf('FAILED (%d)', $GLOBALS['LITE_FAIL']),
    $GLOBALS['LITE_PASS'] + $GLOBALS['LITE_FAIL']
);
exit($GLOBALS['LITE_FAIL'] === 0 ? 0 : 1);
