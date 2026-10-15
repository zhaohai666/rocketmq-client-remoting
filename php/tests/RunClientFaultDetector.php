<?php

declare(strict_types=1);


require_once __DIR__ . '/../bootstrap.php';

// Clear dynamic address-discovery env vars; swallow client logs (assertions in this suite do not depend on log content)
putenv('ROCKETMQ_NAMESRV_DOMAIN');
\RocketMQ\Client\Logger::setHandler(static function (string $line): void {
});

use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\FaultItem;
use RocketMQ\Client\LatencyFaultToleranceImpl;
use RocketMQ\Client\MQFaultStrategy;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\TopicRouteData;

// ======================================================================
// Child-process fake broker: `php RunClientFaultDetector.php --broker <logfile> <modefile>`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    detectorBrokerMain($argv[2] ?? '', $argv[3] ?? '');
    exit(0);
}

function detectorBrokerReadExact($conn, int $n): ?string
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

function detectorBrokerReadFrame($conn): ?RemotingCommand
{
    $head = detectorBrokerReadExact($conn, 4);
    if ($head === null) {
        return null;
    }
    $total = RocketMQSerializable::unpackSignedInt($head);
    if ($total <= 0) {
        return null;
    }
    $rest = detectorBrokerReadExact($conn, $total);
    if ($rest === null) {
        return null;
    }
    return RemotingCommand::decode($head . $rest);
}

function detectorBrokerRouteBody(string $brokerName, string $addr, int $queues = 2): string
{
    $qd = new QueueData($brokerName, $queues, $queues, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', $brokerName, [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

function detectorBrokerLog(string $logFile, RemotingCommand $cmd): void
{
    $entry = ['code' => $cmd->code, 'ext' => $cmd->extFields];
    @file_put_contents($logFile, json_encode($entry, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
}

/**
 * Mode file's current values: 'ok' = GET_MAX_OFFSET answered normally; 'silent' = accepted but
 * not answered (appears client-side as a single probe timeout).
 */
function detectorBrokerMode(string $modeFile): string
{
    $raw = @file_get_contents($modeFile);
    return $raw === false ? 'ok' : trim($raw);
}

function detectorBrokerHandleFrame($conn, RemotingCommand $cmd, string $logFile, string $modeFile, string $selfAddr): void
{
    detectorBrokerLog($logFile, $cmd);
    $resp = null;
    switch ($cmd->code) {
        case RequestCode::GET_ROUTEINFO_BY_TOPIC:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = detectorBrokerRouteBody('broker-a', $selfAddr, 2);
            break;
        case RequestCode::HEART_BEAT:
        case RequestCode::UNREGISTER_CLIENT:
        case RequestCode::CHECK_CLIENT_CONFIG:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            break;
        case RequestCode::GET_MAX_OFFSET:
            // ServiceDetector's probe: 'ok' mode answers SUCCESS, 'silent' mode swallows it (client times out)
            if (detectorBrokerMode($modeFile) !== 'silent') {
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->extFields['offset'] = '777';
            }
            break;
        default:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            break;
    }
    if ($resp !== null) {
        $resp->opaque = $cmd->opaque;
        @fwrite($conn, $resp->encode());
    }
}

/** @return array{0: string, 1: RemotingCommand}|null */
function detectorBrokerTakeFrame(string &$buf): ?array
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

function detectorBrokerMain(string $logFile, string $modeFile): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "broker: cannot listen: {$errstr}\n");
        return;
    }
    $addr = (string) stream_socket_get_name($server, false);
    fwrite(STDOUT, $addr . "\n");
    fflush(STDOUT);

    /**
     * Multi-connection event loop (same as RunClientProducer's fake broker: a single
     * blocking connection rejects new connections on Windows). @var array<int, resource> $conns
     */
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
                $frame = detectorBrokerTakeFrame($bufs[$key]);
                if ($frame === null) {
                    break;
                }
                detectorBrokerHandleFrame($r, $frame[1], $logFile, $modeFile, $addr);
            }
        }
    }
    @fclose($server);
}

// ======================================================================
// Child-process fake broker handle
// ======================================================================

final class DetectorFakeBroker
{
    /** @var resource */
    private $proc;
    /** @var array<int, resource> */
    private array $pipes;
    public string $addr;
    private string $logFile;
    private string $modeFile;

    private function __construct($proc, array $pipes, string $addr, string $logFile, string $modeFile)
    {
        $this->proc = $proc;
        $this->pipes = $pipes;
        $this->addr = $addr;
        $this->logFile = $logFile;
        $this->modeFile = $modeFile;
    }

    public static function start(string $initialMode = 'ok'): self
    {
        $logFile = tempnam(sys_get_temp_dir(), 'rmq-detector-log-');
        $modeFile = tempnam(sys_get_temp_dir(), 'rmq-detector-mode-');
        file_put_contents($modeFile, $initialMode);
        $errFile = $logFile . '.err';
        $cmd = [PHP_BINARY, __FILE__, '--broker', $logFile, $modeFile];
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
        return new self($proc, $pipes, $addr, $logFile, $modeFile);
    }

    public function setMode(string $mode): void
    {
        file_put_contents($this->modeFile, $mode);
    }

    /** @return list<array<string,mixed>> */
    public function logEntries(): array
    {
        $out = [];
        foreach (file($this->logFile) ?: [] as $line) {
            $line = trim($line);
            if ($line !== '') {
                $out[] = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            }
        }
        return $out;
    }

    /** Log entry count for a request code (optionally filtered by topic). */
    public function logCount(int $code, ?string $topic = null): int
    {
        $n = 0;
        foreach ($this->logEntries() as $e) {
            if (($e['code'] ?? null) !== $code) {
                continue;
            }
            if ($topic !== null && ($e['ext']['topic'] ?? null) !== $topic) {
                continue;
            }
            $n++;
        }
        return $n;
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
        @unlink($this->modeFile);
    }
}

// ======================================================================
// Pure-logic detector stand-in
// ======================================================================

/** ServiceDetector stand-in: answers per script entry by entry, and records the arguments it received. */
final class DetectorScript
{
    /** @var list<bool> */
    public array $script = [];
    public int $calls = 0;
    /** @var list<array{0: string, 1: int}> */
    public array $args = [];

    public function detect(string $addr, int $timeoutMillis): bool
    {
        $this->args[] = [$addr, $timeoutMillis];
        $answer = $this->script[$this->calls] ?? false;
        $this->calls++;
        return $answer;
    }
}

// ======================================================================
// Test runner
// ======================================================================

final class RunClientFaultDetector
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

    // ================================================================ 1. Configuration and defaults

    private function testConfigDefaults(): void
    {
        $p = new DefaultMQProducer('GID-det-cfg');
        $this->check(false === $p->isStartDetectorEnable(), 'producer defaults startDetectorEnable=false');
        $this->checkSame(200, $p->getDetectTimeout(), 'producer defaults detectTimeout=200');
        $this->checkSame(2000, $p->getDetectInterval(), 'producer defaults detectInterval=2000');

        $st = $p->mqFaultStrategy;
        $this->check(false === $st->isStartDetectorEnable(), 'strategy defaults startDetectorEnable=false');
        $this->checkSame(200, $st->getDetectTimeout(), 'strategy defaults detectTimeout=200');
        $this->checkSame(2000, $st->getDetectInterval(), 'strategy defaults detectInterval=2000');
        $tol = $st->getLatencyFaultTolerance();
        $this->checkSame(200, $tol->detectTimeout, 'tolerance defaults detectTimeout=200');
        $this->checkSame(2000, $tol->detectInterval, 'tolerance defaults detectInterval=2000');
        $this->check(false === $tol->isStartDetectorEnable(), 'tolerance defaults startDetectorEnable=false');

        $p->setStartDetectorEnable(true);
        $p->setDetectTimeout(500);
        $p->setDetectInterval(1500);
        $this->check(true === $p->isStartDetectorEnable(), 'setStartDetectorEnable(true) takes effect');
        $this->checkSame(500, $p->getDetectTimeout(), 'setDetectTimeout takes effect');
        $this->checkSame(1500, $p->getDetectInterval(), 'setDetectInterval takes effect');
        $this->check(true === $st->isStartDetectorEnable(), 'setStartDetectorEnable propagates to strategy');
        $this->checkSame(500, $st->getDetectTimeout(), 'setDetectTimeout propagates to strategy');
        $this->checkSame(1500, $st->getDetectInterval(), 'setDetectInterval propagates to strategy');
        $this->checkSame(500, $tol->detectTimeout, 'setDetectTimeout propagates to tolerance');
        $this->checkSame(1500, $tol->detectInterval, 'setDetectInterval propagates to tolerance');
        $this->check(true === $tol->isStartDetectorEnable(), 'setStartDetectorEnable propagates to tolerance');
        $p->setStartDetectorEnable(false);
        $this->check(false === $st->isStartDetectorEnable(), 'switch can be flipped back to false');
    }

    // ================================================================ 2. detectByOneRound pure logic

    private function testDetectRoundPure(): void
    {
        $tol = new LatencyFaultToleranceImpl();
        $tol->detectTimeout = 50;
        $tol->detectInterval = 100;
        $script = new DetectorScript();
        $script->script = [false, true]; // first probe round fails, second succeeds
        $resolver = static fn (string $name): ?string => $name === 'gone' ? null : "addr-{$name}";

        // Put an unreachable broker in the fault table (initial checkStamp 0 => due at any time)
        $tol->updateFaultItem('b1', 1000.0, 0.0, false);
        $this->check(false === $tol->isReachable('b1'), 'setup: b1 unreachable');

        // Round one: due => probe (fails) => flag untouched; checkStamp pushed to now+100
        $tol->detectByOneRound(1000, $resolver, $script->detect(...));
        $this->checkSame(1, $script->calls, 'round one due triggers a probe');
        $this->checkSame(['addr-b1', 50], $script->args[0] ?? null, 'probe args = resolved address + detectTimeout');
        $this->check(false === $tol->isReachable('b1'), 'failed probe leaves flag untouched (marking unreachable is the send path\'s job)');
        $this->checkSame(1100.0, $tol->getFaultItem('b1')->checkStamp, 'checkStamp pushed to now+detectInterval');

        // Not due yet: zero probe overhead
        $tol->detectByOneRound(1050, $resolver, $script->detect(...));
        $this->checkSame(1, $script->calls, 'no probe while not due');

        $tol->detectByOneRound(1100, $resolver, $script->detect(...));
        $this->checkSame(2, $script->calls, 'second round probes once due');
        $this->check(true === $tol->isReachable('b1'), 'successful probe => reachable flips back to true');

        // resolver cannot resolve (broker left the route) => fault entry removed
        $tol->updateFaultItem('gone', 0.0, 0.0, false);
        $tol->detectByOneRound(2000, $resolver, $script->detect(...));
        $this->checkSame(null, $tol->getFaultItem('gone'), 'resolver=null => fault entry removed');
        $this->check($tol->getFaultItem('b1') !== null, 'other entries unaffected');

        $tol->updateFaultItem('b3', 0.0, 0.0, false);
        $tol->detectByOneRound(3000, $resolver, null);
        $this->check(false === $tol->isReachable('b3'), 'no detector => flag untouched');
        $this->check($tol->getFaultItem('b3') !== null, 'no detector => entry kept');
    }

    // ================================================================ 3. tick driven: due triggers / not-due zero overhead

    private function testTickDueTrigger(DetectorFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-det-due', 'DetectTopic', 'det-inst-due');
        $p->setSendLatencyFaultEnable(true);
        $p->setStartDetectorEnable(true);
        $p->setDetectInterval(400);
        $p->setDetectTimeout(1500);
        $p->start();
        try {
            $strategy = $p->mqFaultStrategy;
            $tol = $strategy->getLatencyFaultTolerance();
            $strategy->updateFaultItem('broker-a', 1000.0, true, false);
            $item = $tol->getFaultItem('broker-a');
            $this->check($item !== null, 'fault table entry created');
            $this->check(false === $tol->isReachable('broker-a'), 'setup: broker-a unreachable');

            $now = (int) FaultItem::nowMillis();

            // Not due (checkStamp pushed into the future): tick has zero probe overhead
            $item->checkStamp = $now + 5000;
            $p->mqClient->tick($now);
            $this->checkSame(0, $broker->logCount(RequestCode::GET_MAX_OFFSET), 'not due => no probe sent');

            // Due: probe happens synchronously inside tick, complete by the time tick returns
            $item->checkStamp = FaultItem::nowMillis() - 1;
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(1, $broker->logCount(RequestCode::GET_MAX_OFFSET), 'due => probe sent within tick');
            $this->check(true === $tol->isReachable('broker-a'), 'successful probe => broker-a flips back to reachable');

            // Probe parameters: ServiceDetector = queue 0 of the probe topic
            $probes = array_values(array_filter(
                $broker->logEntries(),
                static fn (array $e): bool => ($e['code'] ?? null) === RequestCode::GET_MAX_OFFSET
            ));
            $this->checkSame('DetectTopic', $probes[0]['ext']['topic'] ?? null, 'probe topic uses the in-use topic');
            $this->checkSame('0', (string) ($probes[0]['ext']['queueId'] ?? ''), 'probe queueId=0');

            // Period rescheduling: tick again right after a probe does not probe again (detectInterval not yet due)
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(1, $broker->logCount(RequestCode::GET_MAX_OFFSET), 'checkStamp rescheduled after probe => no duplicate probe');
        } finally {
            $p->shutdown();
        }
    }

    // ================================================================ 4. tick driven: no probe when the switch is off

    private function testTickGateDisabled(DetectorFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-det-off', 'DetectTopic', 'det-inst-off');
        // startDetectorEnable stays default false (sendLatencyFaultEnable also off -- the
        // probe round and the send switch are independent: feed an unreachable entry
        // straight into the fault table to verify tick's switch gate)
        $p->start();
        try {
            $tol = $p->mqFaultStrategy->getLatencyFaultTolerance();
            $tol->updateFaultItem('broker-a', 1000.0, 0.0, false);
            $item = $tol->getFaultItem('broker-a');
            $this->check($item !== null, 'setup: fault table entry created (written at the tolerance layer)');
            $item->checkStamp = FaultItem::nowMillis() - 1; // already due
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(0, $broker->logCount(RequestCode::GET_MAX_OFFSET), 'switch off => no probe even when due');
            $this->check(false === $tol->isReachable('broker-a'), 'switch off => flag untouched');

            $p->setStartDetectorEnable(true);
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(1, $broker->logCount(RequestCode::GET_MAX_OFFSET), 'switch turned on mid-run => probe next round');
            $this->check(true === $tol->isReachable('broker-a'), 'switch turned on mid-run => reachable again');
        } finally {
            $p->shutdown();
        }
    }

    // ================================================================ 5. tick driven: probe timeout => recovery lets traffic through

    private function testTimeoutThenRecover(DetectorFakeBroker $broker): void
    {
        // Broker starts in silent mode: GET_MAX_OFFSET accepted but unanswered => a single probe can only time out
        $p = $this->makeProducer($broker, 'GID-det-to', 'DetectTopic', 'det-inst-to');
        $p->setSendLatencyFaultEnable(true);
        $p->setStartDetectorEnable(true);
        $p->setDetectInterval(400);
        $p->setDetectTimeout(1200);
        $p->start();
        try {
            $strategy = $p->mqFaultStrategy;
            $tol = $strategy->getLatencyFaultTolerance();
            $strategy->updateFaultItem('broker-a', 1000.0, true, false);
            $item = $tol->getFaultItem('broker-a');
            $this->check(false === $tol->isReachable('broker-a'), 'setup: broker-a unreachable');

            // Probe timeout (broker alive but silent => invokeSync times out per detectTimeout, no connection-refused quirk)
            $item->checkStamp = FaultItem::nowMillis() - 1;
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(1, $broker->logCount(RequestCode::GET_MAX_OFFSET), 'probe was sent in the timeout scenario');
            $this->check(false === $tol->isReachable('broker-a'), 'probe timeout => still unreachable (record kept for the next round)');

            // Broker recovers its answers => next round probes successfully => traffic let through
            $broker->setMode('ok');
            $item->checkStamp = FaultItem::nowMillis() - 1;
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(2, $broker->logCount(RequestCode::GET_MAX_OFFSET), 'second probe after recovery');
            $this->check(true === $tol->isReachable('broker-a'), 'successful probe => reachable again (admitted back from the unreachable window)');
        } finally {
            $p->shutdown();
        }
    }

    // ================================================================ run

    private function makeProducer(DetectorFakeBroker $broker, string $group, string $topic, string $instanceName): DefaultMQProducer
    {
        $p = new DefaultMQProducer($group, null, '', [$topic]);
        $p->setInstanceName($instanceName);
        $p->setNameServerAddresses([$broker->addr]);
        $p->setSendMsgTimeout(3000);
        return $p;
    }

    public function run(): int
    {
        $this->testConfigDefaults();
        $this->testDetectRoundPure();

        // Each network case starts its own fake broker (JSON line logs accumulate per process, cannot be shared across cases)
        $broker = DetectorFakeBroker::start();
        try {
            $this->testTickDueTrigger($broker);
        } finally {
            $broker->stop();
        }

        $broker = DetectorFakeBroker::start();
        try {
            $this->testTickGateDisabled($broker);
        } finally {
            $broker->stop();
        }

        $broker = DetectorFakeBroker::start('silent');
        try {
            $this->testTimeoutThenRecover($broker);
        } finally {
            $broker->stop();
        }

        return $this->summary();
    }
}

exit((new RunClientFaultDetector())->run());
