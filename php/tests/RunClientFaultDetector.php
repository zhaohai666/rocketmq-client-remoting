<?php

declare(strict_types=1);


require_once __DIR__ . '/../bootstrap.php';

// 清掉动态取址环境变量；客户端日志吞掉（本套件断言不依赖日志内容）
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
// 子进程假 broker：`php RunClientFaultDetector.php --broker <logfile> <modefile>`
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
 * 模式文件当前值：'ok' = GET_MAX_OFFSET 正常应答；'silent' = 收下但不应答
 * （客户端侧表现为单次探测超时）。
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
            // ServiceDetector 的探针：ok 模式回 SUCCESS，silent 模式吞掉（客户端超时）
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
     * 多连接事件循环（同 RunClientProducer 的假 broker：单连接阻塞式在 Windows 上
     * 会拒新连接）。@var array<int, resource> $conns
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
// 子进程假 broker 句柄
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

    /** 某个请求码的日志条数（可选按 topic 过滤）。 */
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
// 纯逻辑探测器替身
// ======================================================================

/** ServiceDetector 替身：按脚本逐次应答，并记下收到的参数。 */
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
// 测试 runner
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

    // ================================================================ 1. 配置与默认值

    private function testConfigDefaults(): void
    {
        $p = new DefaultMQProducer('GID-det-cfg');
        $this->check(false === $p->isStartDetectorEnable(), 'producer 默认 startDetectorEnable=false');
        $this->checkSame(200, $p->getDetectTimeout(), 'producer 默认 detectTimeout=200');
        $this->checkSame(2000, $p->getDetectInterval(), 'producer 默认 detectInterval=2000');

        $st = $p->mqFaultStrategy;
        $this->check(false === $st->isStartDetectorEnable(), 'strategy 默认 startDetectorEnable=false');
        $this->checkSame(200, $st->getDetectTimeout(), 'strategy 默认 detectTimeout=200');
        $this->checkSame(2000, $st->getDetectInterval(), 'strategy 默认 detectInterval=2000');
        $tol = $st->getLatencyFaultTolerance();
        $this->checkSame(200, $tol->detectTimeout, 'tolerance 默认 detectTimeout=200');
        $this->checkSame(2000, $tol->detectInterval, 'tolerance 默认 detectInterval=2000');
        $this->check(false === $tol->isStartDetectorEnable(), 'tolerance 默认 startDetectorEnable=false');

        $p->setStartDetectorEnable(true);
        $p->setDetectTimeout(500);
        $p->setDetectInterval(1500);
        $this->check(true === $p->isStartDetectorEnable(), 'setStartDetectorEnable(true) 生效');
        $this->checkSame(500, $p->getDetectTimeout(), 'setDetectTimeout 生效');
        $this->checkSame(1500, $p->getDetectInterval(), 'setDetectInterval 生效');
        $this->check(true === $st->isStartDetectorEnable(), 'setStartDetectorEnable 传导到 strategy');
        $this->checkSame(500, $st->getDetectTimeout(), 'setDetectTimeout 传导到 strategy');
        $this->checkSame(1500, $st->getDetectInterval(), 'setDetectInterval 传导到 strategy');
        $this->checkSame(500, $tol->detectTimeout, 'setDetectTimeout 传导到 tolerance');
        $this->checkSame(1500, $tol->detectInterval, 'setDetectInterval 传导到 tolerance');
        $this->check(true === $tol->isStartDetectorEnable(), 'setStartDetectorEnable 传导到 tolerance');
        $p->setStartDetectorEnable(false);
        $this->check(false === $st->isStartDetectorEnable(), '开关可翻回 false');
    }

    // ================================================================ 2. detectByOneRound 纯逻辑

    private function testDetectRoundPure(): void
    {
        $tol = new LatencyFaultToleranceImpl();
        $tol->detectTimeout = 50;
        $tol->detectInterval = 100;
        $script = new DetectorScript();
        $script->script = [false, true]; // 第一轮探测失败，第二轮成功
        $resolver = static fn (string $name): ?string => $name === 'gone' ? null : "addr-{$name}";

        // 故障表里放一个不可达 broker（checkStamp 初始 0 → 任意时刻都到期）
        $tol->updateFaultItem('b1', 1000.0, 0.0, false);
        $this->check(false === $tol->isReachable('b1'), '预备：b1 不可达');

        // 第一轮：到期 → 探测（失败）→ flag 不动；checkStamp 推到 now+100
        $tol->detectByOneRound(1000, $resolver, $script->detect(...));
        $this->checkSame(1, $script->calls, '第一轮到期触发探测');
        $this->checkSame(['addr-b1', 50], $script->args[0] ?? null, '探测参数 = 解析地址 + detectTimeout');
        $this->check(false === $tol->isReachable('b1'), '探测失败 flag 不动（置不可达是发送路径的职责）');
        $this->checkSame(1100.0, $tol->getFaultItem('b1')->checkStamp, 'checkStamp 推到 now+detectInterval');

        // 未到期：零探测开销
        $tol->detectByOneRound(1050, $resolver, $script->detect(...));
        $this->checkSame(1, $script->calls, '未到期不再探测');

        $tol->detectByOneRound(1100, $resolver, $script->detect(...));
        $this->checkSame(2, $script->calls, '到期后第二轮探测');
        $this->check(true === $tol->isReachable('b1'), '探测成功 → reachable 翻回 true');

        // resolver 解析不到（broker 已离开路由）→ 故障记录摘除
        $tol->updateFaultItem('gone', 0.0, 0.0, false);
        $tol->detectByOneRound(2000, $resolver, $script->detect(...));
        $this->checkSame(null, $tol->getFaultItem('gone'), 'resolver=null → 摘除故障记录');
        $this->check($tol->getFaultItem('b1') !== null, '其余记录不受影响');

        $tol->updateFaultItem('b3', 0.0, 0.0, false);
        $tol->detectByOneRound(3000, $resolver, null);
        $this->check(false === $tol->isReachable('b3'), '无探测器 → flag 不动');
        $this->check($tol->getFaultItem('b3') !== null, '无探测器 → 记录保留');
    }

    // ================================================================ 3. tick 驱动：到期触发 / 未到期零开销

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
            $this->check($item !== null, '故障表建项');
            $this->check(false === $tol->isReachable('broker-a'), '预备：broker-a 不可达');

            $now = (int) FaultItem::nowMillis();

            // 未到期（checkStamp 推到未来）：tick 零探测开销
            $item->checkStamp = $now + 5000;
            $p->mqClient->tick($now);
            $this->checkSame(0, $broker->logCount(RequestCode::GET_MAX_OFFSET), '未到期 → 不发探测');

            // 到期：tick 内同步探测，tick 返回时探测已完成
            $item->checkStamp = FaultItem::nowMillis() - 1;
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(1, $broker->logCount(RequestCode::GET_MAX_OFFSET), '到期 → tick 内发出探测');
            $this->check(true === $tol->isReachable('broker-a'), '探测成功 → broker-a 翻回可达');

            // 探测参数：ServiceDetector = 探测 topic 的 queue 0
            $probes = array_values(array_filter(
                $broker->logEntries(),
                static fn (array $e): bool => ($e['code'] ?? null) === RequestCode::GET_MAX_OFFSET
            ));
            $this->checkSame('DetectTopic', $probes[0]['ext']['topic'] ?? null, '探测 topic 取在用 topic');
            $this->checkSame('0', (string) ($probes[0]['ext']['queueId'] ?? ''), '探测 queueId=0');

            // 周期重排：刚探测完立刻再 tick 不重复探测（detectInterval 未到期）
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(1, $broker->logCount(RequestCode::GET_MAX_OFFSET), '探测后 checkStamp 重排 → 不重复探测');
        } finally {
            $p->shutdown();
        }
    }

    // ================================================================ 4. tick 驱动：开关关闭时不探测

    private function testTickGateDisabled(DetectorFakeBroker $broker): void
    {
        $p = $this->makeProducer($broker, 'GID-det-off', 'DetectTopic', 'det-inst-off');
        // startDetectorEnable 保持默认 false（sendLatencyFaultEnable 也关——探测轮与
        // 发送开关独立：容错表直接喂一条不可达记录，验证 tick 的开关判定）
        $p->start();
        try {
            $tol = $p->mqFaultStrategy->getLatencyFaultTolerance();
            $tol->updateFaultItem('broker-a', 1000.0, 0.0, false);
            $item = $tol->getFaultItem('broker-a');
            $this->check($item !== null, '预备：故障表建项（tolerance 层直写）');
            $item->checkStamp = FaultItem::nowMillis() - 1; // 已到期
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(0, $broker->logCount(RequestCode::GET_MAX_OFFSET), '开关关闭 → 到期也不探测');
            $this->check(false === $tol->isReachable('broker-a'), '开关关闭 → flag 不动');

            $p->setStartDetectorEnable(true);
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(1, $broker->logCount(RequestCode::GET_MAX_OFFSET), '运行中打开开关 → 下一轮探测');
            $this->check(true === $tol->isReachable('broker-a'), '运行中打开 → 恢复可达');
        } finally {
            $p->shutdown();
        }
    }

    // ================================================================ 5. tick 驱动：探测超时 → 恢复放行

    private function testTimeoutThenRecover(DetectorFakeBroker $broker): void
    {
        // broker 以 silent 模式起步：GET_MAX_OFFSET 收下不回 → 单次探测只能超时
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
            $this->check(false === $tol->isReachable('broker-a'), '预备：broker-a 不可达');

            // 探测超时（broker 活着但不回 → invokeSync 按 detectTimeout 超时，无连接拒绝怪癖）
            $item->checkStamp = FaultItem::nowMillis() - 1;
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(1, $broker->logCount(RequestCode::GET_MAX_OFFSET), '超时场景探测已发出');
            $this->check(false === $tol->isReachable('broker-a'), '探测超时 → 仍不可达（record 保留等下一轮）');

            // broker 恢复应答 → 下一轮探测成功 → 放行
            $broker->setMode('ok');
            $item->checkStamp = FaultItem::nowMillis() - 1;
            $p->mqClient->tick((int) FaultItem::nowMillis());
            $this->checkSame(2, $broker->logCount(RequestCode::GET_MAX_OFFSET), '恢复后第二轮探测');
            $this->check(true === $tol->isReachable('broker-a'), '探测成功 → 恢复可达（从不可达窗口放回）');
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

        // 每个网络用例各起一个假 broker（JSON 行日志按进程累计，不能跨用例共享）
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
