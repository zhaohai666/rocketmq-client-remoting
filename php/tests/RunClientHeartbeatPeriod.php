<?php

declare(strict_types=1);

/**
 * Registration-refresh period contract suite (no phpunit, no real cluster, no sleeping).
 *
 * Run: php tests/RunClientHeartbeatPeriod.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * The knob under test is the heartbeat (registration refresh) period a facade hands to the client
 * instance it starts. Two things are pinned:
 *
 * 1. The instance stores the value it was given and uses it twice: once to arm the first beat when
 *    it starts, once to re-arm every later beat. Both are read as recorded state -- the schedule
 *    field the instance holds and the number of beats that actually reached the broker -- so no
 *    assertion here depends on how fast the machine is.
 * 2. A value that is unset, zero or negative is not a period: the instance falls back to the
 *    built-in default. That fallback is asserted next to every positive case, which is what makes
 *    the positive case a statement about the knob instead of a statement about any number.
 *
 * The wire case re-launches this file with `--broker` (stream_socket_server) so a beat is counted
 * by the peer that received it.
 */

require_once __DIR__ . '/../bootstrap.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\TopicRouteData;

// The period a probe instance is driven with, and the default it must not be confused with.
const HB_PERIOD_MILLIS = 5000;
const HB_LATER_PERIOD_MILLIS = 7000;
const HB_UNREACHABLE_ADDR = '127.0.0.1:59999';

// ======================================================================
// child-process fake broker: `php RunClientHeartbeatPeriod.php --broker`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    hbBrokerMain();
    exit(0);
}

function hbLog(array $entry): void
{
    $path = getenv('RMQ_LOG');
    if ($path === false || $path === '') {
        return;
    }
    @file_put_contents($path, json_encode($entry, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
}

/** Reads exactly n bytes; returns null on EOF. */
function hbReadExact($conn, int $n): ?string
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
function hbReadFrame($conn): ?RemotingCommand
{
    $head = hbReadExact($conn, 4);
    if ($head === null) {
        return null;
    }
    $total = RocketMQSerializable::unpackSignedInt($head);
    if ($total <= 0) {
        return null;
    }
    $rest = hbReadExact($conn, $total);
    if ($rest === null) {
        return null;
    }
    return RemotingCommand::decode($head . $rest);
}

/** Route body: one broker with one read/write queue, pointing at the fake broker itself. */
function hbRouteBody(string $addr): string
{
    $qd = new QueueData('b-hb', 1, 1, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', 'b-hb', [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

function hbRespond(RemotingCommand $cmd): RemotingCommand
{
    $topic = (string) (($cmd->extFields['topic'] ?? ''));

    switch ($cmd->code) {
        case RequestCode::GET_ROUTEINFO_BY_TOPIC:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = hbRouteBody($GLOBALS['HB_BROKER_ADDR']);
            return $resp;

        case RequestCode::HEART_BEAT:
            // Counted by the peer: one log line per registration refresh it actually received.
            hbLog([
                'code' => $cmd->code,
                'clientId' => (string) (json_decode((string) ($cmd->body ?? '{}'), true)['clientID'] ?? ''),
            ]);
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);

        default:
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

function hbServeConn($conn): void
{
    stream_set_blocking($conn, true);
    while (($cmd = hbReadFrame($conn)) !== null) {
        try {
            $resp = hbRespond($cmd);
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

function hbBrokerMain(): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "broker: cannot listen: {$errstr}\n");
        return;
    }
    $GLOBALS['HB_BROKER_ADDR'] = (string) stream_socket_get_name($server, false);
    fwrite(STDOUT, $GLOBALS['HB_BROKER_ADDR'] . "\n");
    fflush(STDOUT);

    while (true) {
        $conn = @stream_socket_accept($server, 30);
        if ($conn === false) {
            break;
        }
        hbServeConn($conn);
        @fclose($conn);
    }
    @fclose($server);
}

/**
 * Child-process fake broker handle: prints its listening address on the first stdout line, then
 * answers frames and logs every HEART_BEAT it receives into the JSON-lines file named by RMQ_LOG.
 */
final class HeartbeatFakeBroker
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

    /** @return list<array<string,mixed>> */
    public function entries(string $logPath): array
    {
        $out = [];
        foreach (file($logPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
            $d = json_decode($line, true);
            if (is_array($d)) {
                $out[] = $d;
            }
        }
        return $out;
    }

    public function beatCount(string $logPath): int
    {
        $n = 0;
        foreach ($this->entries($logPath) as $e) {
            if (($e['code'] ?? 0) === RequestCode::HEART_BEAT) {
                $n++;
            }
        }
        return $n;
    }
}

// ======================================================================
// schedule probe: an instance whose beats are counted instead of sent
// ======================================================================

/**
 * Counts the two stages the pump drives over a network, so the schedule arithmetic can be read as a
 * number and no request leaves the process.
 */
final class HeartbeatScheduleProbe extends MQClientInstance
{
    public int $beatCalls = 0;

    public int $routeCalls = 0;

    public function heartbeatOnce(?int $timeoutMillis = null): int
    {
        $this->beatCalls++;
        return 1;
    }

    public function routeRefreshOnce(): void
    {
        $this->routeCalls++;
    }

    public function adjustThreadPool(): void
    {
    }

    public function namesrvRefreshOnce(): void
    {
    }
}

// ======================================================================
// suite
// ======================================================================

final class RunClientHeartbeatPeriod
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

    public function summary(): int
    {
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }

    /** The schedule field is private by design: reading it is the only way to see what the instance armed itself with. */
    private function nextBeatAt(MQClientInstance $inst): ?int
    {
        $p = new \ReflectionProperty(MQClientInstance::class, 'heartbeatNextRunMillis');
        $p->setAccessible(true);
        return $p->getValue($inst);
    }

    /**
     * Builds a started instance with one clock, so every schedule value is arithmetic on a fixed
     * origin instead of on the wall clock.
     */
    private function probe(string $clientId, ?int $period, int $base): HeartbeatScheduleProbe
    {
        $inst = $period === null
            ? new HeartbeatScheduleProbe($clientId, [HB_UNREACHABLE_ADDR])
            : new HeartbeatScheduleProbe($clientId, [HB_UNREACHABLE_ADDR], heartbeatIntervalMillis: $period);
        $inst->clock = static fn(): int => $base;
        $inst->start();
        return $inst;
    }

    // ================================================================ 1. the knob is stored and used

    /**
     * A non-default period configured on the instance reaches the schedule at both places that read
     * it: the first beat arms `period` after start, and every following beat re-arms `period` after
     * the beat that just fired. A tick short of the deadline fires nothing, so the count is a proof
     * about the schedule and not about the machine.
     */
    private function testInstanceScheduleUsesTheKnob(): void
    {
        $base = 1_000_000_000_000;
        $inst = $this->probe('HB-KNOB-1', HB_PERIOD_MILLIS, $base);
        try {
            $this->checkSame(HB_PERIOD_MILLIS, $inst->heartbeatIntervalMillis,
                'the instance holds the period it was configured with');
            $this->checkSame($base + HB_PERIOD_MILLIS, $this->nextBeatAt($inst),
                'start() arms the first beat one configured period out');
            $this->checkSame(1, $inst->beatCalls, 'start() pumps the opening beat');

            $inst->tick($base + HB_PERIOD_MILLIS - 1);
            $this->checkSame(1, $inst->beatCalls,
                'one millisecond short of the configured period the pump beats nobody');
            $this->checkSame($base + HB_PERIOD_MILLIS, $this->nextBeatAt($inst),
                'a short tick leaves the deadline untouched');

            $inst->tick($base + HB_PERIOD_MILLIS);
            $this->checkSame(2, $inst->beatCalls,
                'the beat fires exactly on the configured period');
            $this->checkSame($base + 2 * HB_PERIOD_MILLIS, $this->nextBeatAt($inst),
                'the beat re-arms the schedule with the configured period, not the default');

            $inst->tick($base + 2 * HB_PERIOD_MILLIS - 1);
            $this->checkSame(2, $inst->beatCalls, 'the second beat honours the same boundary');

            $inst->tick($base + 2 * HB_PERIOD_MILLIS);
            $this->checkSame(3, $inst->beatCalls, 'the configured period repeats beat after beat');
            $this->checkSame($base + 3 * HB_PERIOD_MILLIS, $this->nextBeatAt($inst),
                'every re-arm reads the configured period');

            // A different value is a different schedule: the period is not a fixed constant.
            $other = $this->probe('HB-KNOB-2', HB_LATER_PERIOD_MILLIS, $base);
            $this->checkSame($base + HB_LATER_PERIOD_MILLIS, $this->nextBeatAt($other),
                'a second configured period arms its own schedule');
            $other->tick($base + HB_PERIOD_MILLIS);
            $this->checkSame(1, $other->beatCalls,
                'the first period is not the second period: an early tick beats nobody');
            $other->tick($base + HB_LATER_PERIOD_MILLIS);
            $this->checkSame(2, $other->beatCalls, 'the second period fires on its own deadline');
            $this->checkSame($base + 2 * HB_LATER_PERIOD_MILLIS, $this->nextBeatAt($other),
                'the second period re-arms by itself');
            $other->shutdown();
        } finally {
            $inst->shutdown();
            MQClientInstance::removeInstance('HB-KNOB-1');
            MQClientInstance::removeInstance('HB-KNOB-2');
        }
    }

    // ================================================================ 2. controls: the default and the fallbacks

    /**
     * The same probes with no period, an explicit zero and an explicit negative value: all three
     * read as the built-in default, both in the value held and in the schedule armed. This is what
     * turns the assertions above into a statement about the knob.
     */
    private function testDefaultPeriodIsTheControl(): void
    {
        $base = 1_000_000_000_000;
        $this->checkSame(30000, MQClientInstance::HEARTBEAT_INTERVAL_MILLIS,
            'the built-in registration refresh period is 30000ms');

        $cases = [
            'unset' => ['HB-DEF-UNSET', null],
            'zero' => ['HB-DEF-ZERO', 0],
            'negative' => ['HB-DEF-NEG', -1],
        ];
        foreach ($cases as $name => [$clientId, $period]) {
            $inst = $this->probe($clientId, $period, $base);
            try {
                $this->checkSame(MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, $inst->heartbeatIntervalMillis,
                    "{$name} period falls back to the default constant");
                $this->checkSame($base + MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, $this->nextBeatAt($inst),
                    "{$name} period arms the default schedule at start");

                // The configured-period boundary must NOT fire: the default is really in force.
                $inst->tick($base + HB_PERIOD_MILLIS);
                $this->checkSame(1, $inst->beatCalls, "{$name} period: no beat at 5000ms");
                $inst->tick($base + MQClientInstance::HEARTBEAT_INTERVAL_MILLIS - 1);
                $this->checkSame(1, $inst->beatCalls, "{$name} period: still no beat just before 30000ms");
                $inst->tick($base + MQClientInstance::HEARTBEAT_INTERVAL_MILLIS);
                $this->checkSame(2, $inst->beatCalls, "{$name} period: the beat lands on the default deadline");
                $this->checkSame($base + 2 * MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, $this->nextBeatAt($inst),
                    "{$name} period re-arms by the default");
                $inst->tick($base + 2 * MQClientInstance::HEARTBEAT_INTERVAL_MILLIS);
                $this->checkSame(3, $inst->beatCalls, "{$name} period repeats by the default");
            } finally {
                $inst->shutdown();
                MQClientInstance::removeInstance($clientId);
            }
        }
    }

    // ================================================================ 3. the producer facade passes its knob through

    /**
     * The instance a producer starts must carry the producer's own period: the facade field is read
     * exactly once, at the moment the instance is built, so a later change to the facade cannot
     * re-time a running instance either.
     */
    private function testProducerPassesItsKnobThrough(): void
    {
        $cases = [
            [HB_PERIOD_MILLIS, HB_PERIOD_MILLIS, 'producer period reaches the instance'],
            [0, MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, 'a zero producer period reads as the default'],
            [-7, MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, 'a negative producer period reads as the default'],
        ];
        foreach ($cases as [$configured, $expected, $label]) {
            $p = new DefaultMQProducer('GID-HB-P-' . $configured);
            $p->setInstanceName('HB-P-' . $configured);
            $p->setNamesrvAddr(HB_UNREACHABLE_ADDR);
            $p->heartbeatIntervalMillis = $configured;
            $p->start();
            $inst = $p->mqClient;
            $this->check($inst instanceof MQClientInstance, "{$label}: the producer owns an instance");
            if (!$inst instanceof MQClientInstance) {
                continue;
            }
            $this->checkSame($expected, $inst->heartbeatIntervalMillis, $label);
            // The value the instance holds is what its own schedule was armed with: same number,
            // read out of the instance rather than from the facade.
            $now = $inst->currentTimeMillis();
            $next = $this->nextBeatAt($inst);
            $this->check($next !== null, "{$label}: the beat schedule is armed");
            $this->checkSame(true, $next !== null && $next > $now && $next <= $now + $expected,
                "{$label}: the armed deadline is one configured period out, no further");
            $p->shutdown();
            MQClientInstance::removeInstance($p->clientId);
        }

        // Control for the untouched facade: the default field value is the default constant.
        $plain = new DefaultMQProducer('GID-HB-P-DEFAULT');
        $plain->setInstanceName('HB-P-DEFAULT');
        $plain->setNamesrvAddr(HB_UNREACHABLE_ADDR);
        $this->checkSame(MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, $plain->heartbeatIntervalMillis,
            'an unset producer period keeps the default field value');
        $plain->start();
        $this->checkSame(MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, $plain->mqClient->heartbeatIntervalMillis,
            'an unset producer period gives the instance the default period');
        $plain->shutdown();
        MQClientInstance::removeInstance($plain->clientId);

        // The knob is taken once: changing the facade after start does not re-time the instance.
        $late = new DefaultMQProducer('GID-HB-P-LATE');
        $late->setInstanceName('HB-P-LATE');
        $late->setNamesrvAddr(HB_UNREACHABLE_ADDR);
        $late->heartbeatIntervalMillis = HB_PERIOD_MILLIS;
        $late->start();
        $this->checkSame(HB_PERIOD_MILLIS, $late->mqClient->heartbeatIntervalMillis,
            'the instance is built with the producer period in force at start()');
        $late->heartbeatIntervalMillis = 9000;
        $this->checkSame(HB_PERIOD_MILLIS, $late->mqClient->heartbeatIntervalMillis,
            'a producer period changed after start() does not re-time the running instance');
        $late->shutdown();
        MQClientInstance::removeInstance($late->clientId);
    }

    // ================================================================ 4. the configured period on the wire

    /**
     * End to end on the wire: the beats a broker counts must follow the period the producer was
     * configured with. Deadlines are read from the instance's own armed schedule, so the ticks are
     * placed relative to recorded state and never relative to elapsed wall time.
     */
    private function testBeatsFollowTheConfiguredPeriodOnTheWire(HeartbeatFakeBroker $broker, string $logPath): void
    {
        $p = new DefaultMQProducer('GID-HB-WIRE');
        $p->setInstanceName('HB-WIRE');
        $p->setNamesrvAddr($broker->addr);
        $p->heartbeatIntervalMillis = HB_PERIOD_MILLIS;
        $p->start();
        $inst = $p->mqClient;
        // A beat only reaches a broker the client knows, so the route comes first.
        $queues = $p->fetchPublishMessageQueues('HeartbeatTopic');
        $this->check($queues !== [], 'the producer resolved a broker to beat');
        $first = $queues[0] ?? null;
        $this->checkSame(
            ['HeartbeatTopic', 'b-hb', 0],
            $first === null ? null : [$first->topic, $first->brokerName, $first->queueId],
            'the resolved publish queue',
        );

        $armed = $this->nextBeatAt($inst);
        $this->check($armed !== null, 'the running instance holds an armed beat deadline');
        $beats = $broker->beatCount($logPath);
        $this->check($beats >= 1, 'the opening beat reached the broker');

        if ($armed !== null) {
            $inst->tick($armed - 1);
            $this->checkSame($beats, $broker->beatCount($logPath),
                'a tick short of the configured period puts no beat on the wire');
            $inst->tick($armed);
            $this->checkSame($beats + 1, $broker->beatCount($logPath),
                'the beat the configured period promised arrives on the wire');
            $this->checkSame($armed + HB_PERIOD_MILLIS, $this->nextBeatAt($inst),
                'the wire beat re-armed the schedule with the configured period');
            $this->checkSame(1, $broker->beatCount($logPath) - $beats,
                'one tick of the configured period sends exactly one beat round');
            $this->check($inst->heartbeatCount() >= 1, 'the instance counts the beats it sent');
        }
        $p->shutdown();
        MQClientInstance::removeInstance($p->clientId);

        // Control: the same probe on a default-period producer must put no beat on the wire at the
        // configured-period boundary, and one beat at the default boundary.
        $d = new DefaultMQProducer('GID-HB-WIRE-DEF');
        $d->setInstanceName('HB-WIRE-DEF');
        $d->setNamesrvAddr($broker->addr);
        $d->start();
        $dinst = $d->mqClient;
        $d->fetchPublishMessageQueues('HeartbeatTopic');
        $armed2 = $this->nextBeatAt($dinst);
        $beats2 = $broker->beatCount($logPath);
        $this->check($armed2 !== null, 'the default-period instance holds an armed beat deadline');
        if ($armed2 !== null) {
            $this->checkSame(true, $armed2 - (int) (microtime(true) * 1000) > HB_PERIOD_MILLIS,
                'the default deadline is further out than the configured period would put it');
            $dinst->tick($armed2 - 1);
            $this->checkSame($beats2, $broker->beatCount($logPath),
                'the default period puts no beat on the wire before its own deadline');
            $dinst->tick($armed2);
            $this->checkSame($beats2 + 1, $broker->beatCount($logPath),
                'the default period beats once on the default deadline');
            $this->checkSame($armed2 + MQClientInstance::HEARTBEAT_INTERVAL_MILLIS, $this->nextBeatAt($dinst),
                'the default period re-arms by the default constant');
        }
        $d->shutdown();
        MQClientInstance::removeInstance($d->clientId);
    }

    // ================================================================ run

    public function run(): int
    {
        $tmpDir = sys_get_temp_dir() . '/rmq-hb-test-' . getmypid();
        if (!is_dir($tmpDir)) {
            @mkdir($tmpDir, 0777, true);
        }
        $logPath = $tmpDir . '/broker.jsonl';
        @unlink($logPath);
        putenv("RMQ_LOG={$logPath}");

        $this->testInstanceScheduleUsesTheKnob();
        $this->testDefaultPeriodIsTheControl();
        $this->testProducerPassesItsKnobThrough();

        $broker = HeartbeatFakeBroker::start();
        try {
            $this->testBeatsFollowTheConfiguredPeriodOnTheWire($broker, $logPath);
        } finally {
            $broker->stop();
            putenv('RMQ_LOG');
            @unlink($logPath);
            @rmdir($tmpDir);
        }
        return $this->summary();
    }
}

exit((new RunClientHeartbeatPeriod())->run());
