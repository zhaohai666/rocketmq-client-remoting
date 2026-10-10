<?php

declare(strict_types=1);

/**
 * 轻量拉取消费者的「topic 队列集合变更」监听（registerTopicMessageQueueChangeListener）
 * 离线单测 —— 不需要集群。
 *
 * 锁的是本端口自己的契约，判据全部取自线上报文：
 *   * 回调只在**队列集合**相对上一次快照真的变化时发一次；顺序变了不算变化。
 *   * 启动前注册 = 没有快照，第一趟必然回调一次（把当前集合交给调用方）；
 *     运行中注册 = 立刻记一版快照，当前状态不会被当成「变化」重复上报。
 *   * 每趟比对都**现问 name server**（不吃路由缓存），否则扩容最快要等一次周期轮询才看得见。
 *   * 查不到队列时**抛**而不是回空表 —— "查不到" ≠ "缩到 0 队列"，后者会让监听器
 *     收到一次假缩容回调、并把快照刷成空集。
 *   * 一个 topic 查不到只跳过它自己，本轮其余 topic 照常比对。
 *   * 重复注册同一 topic 覆盖旧监听器。
 *   * 命名空间：登记用裸名，比对与回调都用套好前缀的那一个键，且绝不二次套壳。
 *
 * PHP 没有线程，比对那一趟由 tick() 按周期驱动；单测直接调
 * fetchTopicMessageQueuesAndCompare() 自己定序，只有一条用例走 tick()。
 *
 * 运行：php tests/RunClientLiteTopicChange.php
 * 假 name server 与本文件同进程族：以 `--ns` 参数二次拉起自身，路由内容每次现读
 * 一份 JSON（topic => 读队列数），父进程改文件就等于「集群扩缩容」。
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
// 假 name server（子进程）
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

/** 路由内容每次现读：父进程改这个文件就是「集群扩缩容」。 */
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

/** 子进程假 name server 句柄。 */
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
        // 客户端日志钉在临时目录：本端口默认按 cwd 落盘，测试不该在仓库里长 logs/。
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

    /** @param array<string,int> $map topic => 读队列数 */
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

    /** @return list<string> 收到过的路由查询 topic，按到达顺序 */
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
// 记录用的队列变更监听器
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
// 脚手架
// ======================================================================

final class LiteHarness
{
    public static int $seq = 0;

    /** @return array{0:LiteFakeNameserver,1:string} 假端点 + 本用例专属 topic */
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

// ---------------------------------------------------------------- 1. 入参守卫与周期
{
    $c = new DefaultLitePullConsumer($GROUP);
    foreach ([
        ['空 topic', static function (DefaultLitePullConsumer $x): void {
            $x->registerTopicMessageQueueChangeListener('', new LiteRecorder());
        }],
        ['空白 topic', static function (DefaultLitePullConsumer $x): void {
            $x->registerTopicMessageQueueChangeListener('   ', new LiteRecorder());
        }],
        ['null 监听器', static function (DefaultLitePullConsumer $x): void {
            $x->registerTopicMessageQueueChangeListener('AnyTopic', null);
        }],
    ] as [$label, $act]) {
        $threw = '';
        try {
            $act($c);
        } catch (MQClientException $e) {
            $threw = $e->getMessage();
        }
        liteCheck(str_contains($threw, 'Topic or listener is null'), "$label 被拒", $threw);
    }

    liteCheck($c->topicMetadataCheckIntervalMillis() === 30000, '默认比对周期 30s');
    $c->setTopicMetadataCheckIntervalMillis(0);
    liteCheck(
        $c->topicMetadataCheckIntervalMillis() === 1000,
        '周期下限收到 1s（0 会把每次 tick 变成 RPC 风暴）'
    );
    $c->setTopicMetadataCheckIntervalMillis(5000);
    liteCheck($c->topicMetadataCheckIntervalMillis() === 5000, '周期可放大');
}

// ---------------------------------------------------------------- 2. 未启动查队列
{
    $c = new DefaultLitePullConsumer($GROUP);
    $threw = '';
    try {
        $c->fetchMessageQueues('AnyTopic');
    } catch (MQClientException $e) {
        $threw = $e->getMessage();
    }
    // 空表会被调用方读成「没有队列可用」而停手，未启动必须说清楚。
    liteCheck(str_contains($threw, 'not started'), '未启动查队列直接报错', $threw);
}

// ---------------------------------------------------------------- 3. 启动前注册
{
    [$ns, $topic] = LiteHarness::open('LiteQcPre', 2);
    $rec = new LiteRecorder();
    $c = new DefaultLitePullConsumer($GROUP);
    $c->setInstanceName('lite_qc_pre');
    $c->setNamesrvAddr($ns->addr);
    $c->subscribe($topic, '*');
    $c->registerTopicMessageQueueChangeListener($topic, $rec);
    $c->start();
    // 注册时还没法查队列 ⇒ 没有快照 ⇒ 第一趟把「当前集合」当变化交出来。
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, '启动后第一趟回调一次');
    liteCheck(LiteHarness::idsOf($rec, 0) === '0,1', '首回调带当前队列集合', LiteHarness::idsOf($rec, 0));
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 0, '集合没变就不再回调');
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 4. 运行中注册 + 扩缩容
{
    [$ns, $topic] = LiteHarness::open('LiteQcScale', 1);
    $c = LiteHarness::started($ns, 'lite_qc_scale', $GROUP, $topic);
    $rec = new LiteRecorder();
    $c->registerTopicMessageQueueChangeListener($topic, $rec);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 0, '运行中注册后第一趟不误报');

    $ns->setQueues($topic, 3);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, '扩容被观察到');
    liteCheck(LiteHarness::idsOf($rec, 0) === '0,1,2', '回调带扩容后的集合', LiteHarness::idsOf($rec, 0));
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 0, '同一集合不重复回调');

    $ns->setQueues($topic, 2);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, '缩容也被观察到');
    liteCheck(LiteHarness::idsOf($rec, 1) === '0,1', '回调带缩容后的集合', LiteHarness::idsOf($rec, 1));
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 5. 每趟现问 name server
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
    liteCheck($first >= 1, '比对这一趟会问路由', (string) $first);
    // 吃缓存的话扩容要等一次周期轮询才看得见，监听整整慢一个周期。
    liteCheck($second >= 1, '下一趟再问一次', (string) $second);
    liteCheck($rec->count() === 0, '集合没变时问了也不回调');
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 6. 一个 topic 失败不饿死别人
{
    [$ns, $topic] = LiteHarness::open('LiteQcIsolate', 2);
    $ghost = 'LiteQcGhost';
    $good = new LiteRecorder();
    $bad = new LiteRecorder();
    $c = LiteHarness::started($ns, 'lite_qc_isolate', $GROUP, $topic);
    // 先登记查不到的那个：它排在前面，如果异常逃出循环体，后面正常的就收不到回调。
    $c->registerTopicMessageQueueChangeListener($ghost, $bad);
    $c->registerTopicMessageQueueChangeListener($topic, $good);
    $ns->setQueues($topic, 3);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, '坏 topic 不影响本轮其余比对');
    liteCheck($bad->count() === 0, '查不到路由的 topic 不发回调（也绝不回空表假装缩容）');
    liteCheck(LiteHarness::idsOf($good, 0) === '0,1,2', '正常 topic 收到的是新集合', LiteHarness::idsOf($good, 0));
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 7. 查不到队列时报错
{
    [$ns, $topic] = LiteHarness::open('LiteQcEmpty', 1);
    $c = LiteHarness::started($ns, 'lite_qc_empty', $GROUP, $topic);
    $ns->setQueues($topic, 0);   // 路由还在，读队列数没了
    $threw = '';
    try {
        $c->fetchMessageQueues($topic);
    } catch (MQClientException $e) {
        $threw = $e->getMessage();
    }
    liteCheck(str_contains($threw, 'Can not find Message Queue'), '没有可用队列时报错', $threw);
    $ns->drop($topic);           // 干脆没有这个 topic 的路由
    $threw = '';
    try {
        $c->fetchMessageQueues($topic);
    } catch (MQClientException $e) {
        $threw = $e->getMessage();
    }
    liteCheck(str_contains($threw, 'Can not find Message Queue'), '查不到路由时同样报错', $threw);
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 8. 重复注册覆盖旧的
{
    [$ns, $topic] = LiteHarness::open('LiteQcOverwrite', 1);
    $c = LiteHarness::started($ns, 'lite_qc_overwrite', $GROUP, $topic);
    $first = new LiteRecorder();
    $second = new LiteRecorder();
    $c->registerTopicMessageQueueChangeListener($topic, $first);
    $c->registerTopicMessageQueueChangeListener($topic, $second);
    $ns->setQueues($topic, 2);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, '覆盖后同一 topic 只回一次');
    liteCheck($first->count() === 0, '旧监听器不再收到回调');
    liteCheck($second->count() === 1, '只有新监听器收到');
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 9. 命名空间口径
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
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, '带命名空间也能比对出变化');
    liteCheck(LiteHarness::topicOf($rec, 0) === $wrapped, '回调的 topic 是套好命名空间的那一个', LiteHarness::topicOf($rec, 0));
    liteCheck(LiteHarness::idsOf($rec, 0) === '0,1', '回调带扩容后的集合', LiteHarness::idsOf($rec, 0));
    $asked = $ns->queries();
    liteCheck(in_array($wrapped, $asked, true), '比对拿套好的键去问路由', implode('|', $asked));
    liteCheck(
        !in_array($nameSpace . '%' . $wrapped, $asked, true),
        '绝不出现二次套壳的查询',
        implode('|', $asked)
    );
    // 拿已经套好的键再登记一次：只是覆盖同一个条目，不会长出第二个前缀。
    $c->registerTopicMessageQueueChangeListener($wrapped, $rec);
    $ns->clearQueries();
    $ns->setQueues($wrapped, 3);
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 1, '重复登记套好的键不会多出一份监听器');
    liteCheck($rec->count() === 2, '回调仍然只来一次', (string) $rec->count());
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 10. tick 真的在驱动比对
{
    [$ns, $topic] = LiteHarness::open('LiteQcTick', 1);
    $c = LiteHarness::started($ns, 'lite_qc_tick', $GROUP, $topic);
    $rec = new LiteRecorder();
    $c->registerTopicMessageQueueChangeListener($topic, $rec);
    $ns->setQueues($topic, 2);
    // 首查本来排在 start 之后 10s；这里把它提前，让 tick 走真实分支。
    $c->nextMetadataCheckAt = microtime(true) - 1.0;
    $c->tick();
    liteCheck($rec->count() === 1, 'tick 自己跑出一轮比对', (string) $rec->count());
    liteCheck(LiteHarness::idsOf($rec, 0) === '0,1', 'tick 驱动的那一趟收到新集合', LiteHarness::idsOf($rec, 0));
    liteCheck(
        $c->nextMetadataCheckAt > microtime(true),
        '跑完一轮就把下一趟推到下个周期',
        (string) ($c->nextMetadataCheckAt - microtime(true))
    );
    $c->shutdown();
    $ns->stop();
}

// ---------------------------------------------------------------- 11. 回调抛异常不影响比对
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
        liteCheck(false, '调用方监听器抛异常不该把比对带下去', $e->getMessage());
    }
    liteCheck($fired === 1, '回调照常计一次');
    // 快照已经推进，所以异常不会让下一轮重复上报同一个集合。
    liteCheck($c->fetchTopicMessageQueuesAndCompare() === 0, '抛过异常的那一趟快照照样生效');
    $c->shutdown();
    $ns->stop();
}

printf(
    "\n%s (%d checks)\n",
    $GLOBALS['LITE_FAIL'] === 0 ? 'ALL TESTS PASSED' : sprintf('FAILED (%d)', $GLOBALS['LITE_FAIL']),
    $GLOBALS['LITE_PASS'] + $GLOBALS['LITE_FAIL']
);
exit($GLOBALS['LITE_FAIL'] === 0 ? 0 : 1);
