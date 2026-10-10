<?php

declare(strict_types=1);

// PHP 端 LitePull 消费者的 topic 队列集合变更监听真机验证。
//
//   php examples/live_lite_topic_queue_change.php [namesrv]
//
// 为什么必须真机：比对趟次每趟都现问 nameserver 取订阅队列集合，而普通路由缓存是 30s
// 才刷一次。假 nameserver 能证比对逻辑，只有真集群能证「现查」。这里把检查周期压到 1s
// 下限、路由轮询保持默认 30s，再把 topic 真的扩容：从 nameserver 报出新队列数到监听器
// 收到回调只该隔一两趟检查（≤5s）；读 30s 缓存的话这个窗口会拖到半分钟以上。
//
// PHP 适配：没有调度线程，比对由调用方驱动 —— 主循环每轮先 tick() 再 poll()，
// tick() 里到点的那一趟才会去比对。
//
//   L1  队列没动 ⇒ 监听器不被打扰
//   L2  扩容 2→4：nameserver 认了之后回调紧跟几趟检查
//   L3  回调后快照推进 ⇒ 同一套队列不重复回调
//   L4  缩容 4→2：同样靠现查看到
//   L5  没建过的 topic：取不到队列算「查不到」，不伪装成缩到 0 队列
//
// 收口行 `PASS=<n> FAIL=<n>`；任一 FAIL 非 0 退出。
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQProducer;

// 日志不落在用户 $HOME 下：默认写当前工作目录的 logs/，这里显式钉到临时目录。
if (getenv('ROCKETMQ_CLIENT_LOG_DIR') === false) {
    putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_qc_live_logs');
}

$pass = 0;
$fail = 0;
$check = function (bool $ok, string $name, string $detail = '') use (&$pass, &$fail): void {
    echo ($ok ? '  [PASS] ' : '  [FAIL] ') . $name . ($detail !== '' ? '  ' . $detail : '') . PHP_EOL;
    if ($ok) {
        $pass++;
    } else {
        $fail++;
    }
};

$ns = $argv[1] ?? '127.0.0.1:9876';
$stamp = (int)(microtime(true) * 1000);
$topic = 'PhpLiteQcLive_' . $stamp;
$group = 'GID_PhpLiteQcLive_' . $stamp;
$ghost = 'PhpLiteQcGhost_' . $stamp;

const BASE_QUEUES = 2;
const SCALED_QUEUES = 4;
// 检查周期压到下限，好把「每趟现查」和「吃 30s 缓存」在时间上分开。
const CHECK_INTERVAL_MS = 1000;
// nameserver 见到变化后允许的最大回调间隔。
const FRESH_WINDOW_MS = 5000;

echo "======================================================================" . PHP_EOL;
echo "LitePull queue-change live (PHP): ns=$ns topic=$topic group=$group" . PHP_EOL;
echo "======================================================================" . PHP_EOL;

/** 对同一 topic 再下发一次建 topic 请求，把读写队列数改成 $queueNum。 */
$scale = static function (int $queueNum) use ($ns, $topic): void {
    $prod = new DefaultMQProducer('PG_PhpQc_' . bin2hex(random_bytes(4)));
    $prod->setNamesrvAddr($ns);
    $prod->start();
    try {
        $prod->createTopic('TBW102', $topic, $queueNum);
    } catch (\Throwable $e) {
        echo "!! createTopic($queueNum) failed: " . $e->getMessage() . PHP_EOL;
    }
    $prod->shutdown();
};

/** 只记回调；比对在同一个线程里跑，所以不需要锁。 */
final class QcRecorder
{
    /** @var list<array{0:string,1:list<int>}> */
    public array $events = [];

    public function onChanged(string $topic, array $queues): void
    {
        $ids = array_map(static fn ($mq): int => $mq->queueId, $queues);
        sort($ids);
        $this->events[] = [$topic, $ids];
    }

    public function count(): int
    {
        return count($this->events);
    }

    public function idsAt(int $i): string
    {
        return isset($this->events[$i]) ? implode(',', $this->events[$i][1]) : '';
    }
}

/**
 * 驱动消费者跑一段时间：每轮先 tick()（到点的那趟才比对）再 poll()。
 * 返回实际跑的毫秒数；cond 提前成立就立刻返回。
 */
$drive = static function (DefaultLitePullConsumer $c, int $millis, ?callable $cond = null): int {
    $waited = 0;
    while ($waited < $millis) {
        $c->tick();
        $c->poll(200);
        $waited += 200;
        if ($cond !== null && $cond() === true) {
            return $waited;
        }
    }
    return $waited;
};

/** 现查队列集合，直到报出 $want 个为止；返回等待毫秒数，超时返回 -1。 */
$waitQueueNum = static function (DefaultLitePullConsumer $c, string $t, int $want, int $timeoutMs): int {
    $waited = 0;
    while ($waited < $timeoutMs) {
        try {
            if (count($c->fetchMessageQueues($t)) === $want) {
                return $waited;
            }
        } catch (\Throwable) {
            // 路由还没更新，继续等
        }
        usleep(250000);
        $waited += 250;
    }
    return -1;
};

$scale(BASE_QUEUES);

$c = new DefaultLitePullConsumer($group);
$c->setNamesrvAddr($ns);
$c->instanceName = 'php-lite-qc-live';
$c->setTopicMetadataCheckIntervalMillis(CHECK_INTERVAL_MS);
$check($c->topicMetadataCheckIntervalMillis === CHECK_INTERVAL_MS,
    'L0 检查周期压到 1s 下限', (string)$c->topicMetadataCheckIntervalMillis);
$c->subscribe($topic, '*');
$c->start();

$visible = $waitQueueNum($c, $topic, BASE_QUEUES, 30000);
$check($visible >= 0, 'L0 路由可见（2 个队列）', $visible . 'ms');

// 运行中注册 ⇒ 立刻记快照 ⇒ 之后队列不动就一律静默（tick() 驱动的那一趟也照样不打扰）
$rec = new QcRecorder();
$c->registerTopicMessageQueueChangeListener($topic, $rec);
$drive($c, 3000);
$check($rec->count() === 0, 'L1 队列没动 ⇒ 静默（快照吃掉每一趟）', 'events=' . $rec->count());

// 把首查排到当下：PHP 没有线程，首查延迟只是 nextMetadataCheckAt 的一个时间戳。
$c->nextMetadataCheckAt = microtime(true);
$drive($c, 1000);
$check($rec->count() === 0, 'L1b 到点那趟真的跑过 ⇒ 依旧静默', 'events=' . $rec->count());

// ---- L2 扩容 2→4
$scale(SCALED_QUEUES);
$nsMs = $waitQueueNum($c, $topic, SCALED_QUEUES, 45000);
$check($nsMs >= 0, 'L2a nameserver 报出 4 个队列', $nsMs . 'ms');
if ($nsMs >= 0) {
    $driven = $drive($c, FRESH_WINDOW_MS, static fn (): bool => $rec->count() >= 1);
    $check($rec->count() >= 1 && $rec->idsAt(0) === '0,1,2,3',
        'L2b 比对趟次现查路由（回调紧跟 nameserver，不等 30s 缓存）',
        $driven . 'ms, ids=' . $rec->idsAt(0));
}

// ---- L3 快照推进
$drive($c, 3000);
$check($rec->count() === 1, 'L3 回调后快照推进 ⇒ 不重复回调', 'events=' . $rec->count());

// ---- L4 缩容 4→2
$scale(BASE_QUEUES);
$nsMs = $waitQueueNum($c, $topic, BASE_QUEUES, 45000);
$check($nsMs >= 0, 'L4a nameserver 报回 2 个队列', $nsMs . 'ms');
if ($nsMs >= 0) {
    $driven = $drive($c, FRESH_WINDOW_MS, static fn (): bool => $rec->count() >= 2);
    $check($rec->count() >= 2 && $rec->idsAt(1) === '0,1',
        'L4b 缩容同样靠现查路由看到', $driven . 'ms, ids=' . $rec->idsAt(1));
}

// ---- L5 未知 topic：空队列集算「查不到」
$raised = '';
try {
    $raised = 'no exception, queues=' . count($c->fetchMessageQueues($ghost));
} catch (\Throwable $e) {
    $raised = $e->getMessage();
}
$check(str_contains($raised, 'Namesrv return empty') || str_contains($raised, 'Can not find'),
    'L5 未知 topic 取队列抛「查不到」而不是返回空', $raised);

$c->shutdown();

echo PHP_EOL . 'LitePull queue-change live (PHP): PASS=' . $pass . ' FAIL=' . $fail . PHP_EOL;
exit($fail === 0 ? 0 : 1);
