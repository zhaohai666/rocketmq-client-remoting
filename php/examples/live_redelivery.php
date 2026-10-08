<?php

declare(strict_types=1);

// PHP 端真机重投/死信矩阵：对标 go/examples/live_redelivery（S1-S4 全部口径）。
//
//   php examples/live_redelivery.php [namesrv] [legs]
//   legs = all | s1,s2,s3,s4
//
// S1 REDELIVERY：listener 只对一条消息的首投 RECONSUME_LATER。broker 经
//    %RETRY%<group> 以延迟级别 3（10s）回投、RECONSUME_TIMES=1，客户端在
//    listener 看到它之前把 topic 还原成业务 topic。另一条必须恰好投一次。
// S2 DEAD-LETTER TERMINAL：maxReconsumeTimes=2 ⇒ broker 拒绝第三次回投
//    （`>=`）转 %DLQ%<group>（reconsumeTimes=3），客户端恰好投递 3 次（0/1/2）。
//    **两半都要断言**：错一半要么"永远重试"要么"提前进死信"，单测都看不出来。
// S3 ORDERLY POISON：顺序侧恒 SUSPEND 得到恰好 3 次本地投递（客户端自抬
//    reconsumeTimes），然后作为普通 SEND 交给 %RETRY%；组还持锁 ⇒ broker 直接
//    进 %DLQ%。这条路径死信判定是 SendMessageProcessor 的严格 `>`，与 S2 的
//    CONSUMER_SEND_MSG_BACK(36) 用 `>=` 不是同一条代码路径，只测一个钉不死。
// S4 PARTIAL ACK (ackIndex)：3 条一批只 ack 到下标 0 ⇒ 尾巴 2 条经 %RETRY%
//    回投、已 ack 的那条绝不回投，业务队列位点仍整批提交到 3。对照组（不碰
//    ackIndex）一条都不回投——没有对照组，回投就赖不到 ack 头上。
//
// PHP 适配（单线程）：所有"后台线程"收敛为 tick() 内到点检查，所以每个等待窗
// 都在**主动 tick 消费者**地轮询，绝不能裸 sleep（客户端冻住 = 拉不到回投）。
//
// 每个场景先显式建 topic：消费者没有默认 topic 兜底（TBW102 只给生产者），
// 缺路由 = 无分配 = 无消费，那会伪装成客户端 bug（B5 不变量）。
//
// 收口行 `PASS=<n> FAIL=<n>`；任一 FAIL 进程非 0 退出。
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\ConsumeOrderlyContext;
use RocketMQ\Client\ConsumeOrderlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MessageListenerOrderly;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;

// ------------------------------------------------------------------ harness

$pass = 0;
$fail = 0;
$check = function (bool $ok, string $name, string $detail = '') use (&$pass, &$fail): void {
    echo ($ok ? 'PASS' : 'FAIL') . ' ' . $name . ($detail !== '' ? ' | ' . $detail : '') . PHP_EOL;
    if ($ok) {
        $pass++;
    } else {
        $fail++;
    }
};

$ns = $argv[1] ?? '127.0.0.1:9876';
$legsArg = trim($argv[2] ?? 'all');
$stamp = time();
$prefix = 'PhpRedelivery' . $stamp;

/** 一次投递 = listener 视角的 body / topic / wire 上的 reconsumeTimes / 时刻。 */
final class RecBox
{
    /** @var list<array{body:string,topic:string,recon:int,at:float}> */
    public array $recs = [];

    public function add(MessageExt $m): void
    {
        $this->recs[] = [
            'body' => (string) $m->getBody(),
            'topic' => (string) $m->topic,
            'recon' => $m->reconsumeTimes,
            'at' => microtime(true),
        ];
    }

    /** @return list<array{body:string,topic:string,recon:int,at:float}> */
    public function ofBody(string $body): array
    {
        return array_values(array_filter($this->recs, static fn(array $r): bool => $r['body'] === $body));
    }

    /** @return list<string> */
    public function distinctBodies(): array
    {
        $set = [];
        foreach ($this->recs as $r) {
            $set[$r['body']] = true;
        }
        $out = array_keys($set);
        sort($out, SORT_STRING);
        return $out;
    }

    public function topicSummary(): string
    {
        $counts = [];
        foreach ($this->recs as $r) {
            $counts[$r['topic']] = ($counts[$r['topic']] ?? 0) + 1;
        }
        ksort($counts, SORT_STRING);
        $parts = [];
        foreach ($counts as $k => $v) {
            $parts[] = "$k=$v";
        }
        return implode(' ', $parts);
    }
}

final class RetryOnceListener implements MessageListenerConcurrently
{
    public function __construct(private readonly RecBox $box, private readonly string $target)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        $fail = false;
        foreach ($msgs as $m) {
            if ($m->getBody() === $this->target && $m->reconsumeTimes === 0) {
                $fail = true; // 只翻首投这一次；之后的投递（recon>=1）全部 SUCCESS
            }
            $this->box->add($m);
        }
        return $fail ? ConsumeConcurrentlyStatus::RECONSUME_LATER : ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

final class AlwaysFailListener implements MessageListenerConcurrently
{
    public function __construct(private readonly RecBox $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        return ConsumeConcurrentlyStatus::RECONSUME_LATER; // reconsumeTimes 阶梯爬到顶 ⇒ broker 死信
    }
}

final class SuspendOrderlyListener implements MessageListenerOrderly
{
    public function __construct(private readonly RecBox $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeOrderlyContext $ctx): ConsumeOrderlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        return ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT; // 客户端自己数投递次数
    }
}

final class PartialAckTo0Listener implements MessageListenerConcurrently
{
    public function __construct(private readonly RecBox $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        $retry = false;
        foreach ($msgs as $m) {
            if ($m->reconsumeTimes > 0) {
                $retry = true; // 重投批次整批收下，阶梯才能终止
            }
            $this->box->add($m);
        }
        if (!$retry) {
            $ctx->ackIndex = 0; // 首投 3 条一批：只认可下标 0（含自身）
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

final class CollectListener implements MessageListenerConcurrently
{
    public function __construct(private readonly RecBox $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS; // 对照组：全收
    }
}

// ------------------------------------------------------------------ helpers

/** 在窗口内轮询条件；每轮先 tick 消费者（PHP 无后台线程），条件成立即返回。 */
function rd_wait_until(float $windowSec, callable $cond, object ...$tickables): bool
{
    $deadline = microtime(true) + $windowSec;
    for (;;) {
        foreach ($tickables as $t) {
            if (method_exists($t, 'tick')) {
                $t->tick();
            }
        }
        if ($cond()) {
            return true;
        }
        if (microtime(true) >= $deadline) {
            return $cond();
        }
        usleep(100_000);
    }
}

/** 观察窗：只 tick 不判断——给"不应再发生的事"留出暴露时间。 */
function rd_observe(float $sec, object ...$tickables): void
{
    $deadline = microtime(true) + $sec;
    while (microtime(true) < $deadline) {
        foreach ($tickables as $t) {
            if (method_exists($t, 'tick')) {
                $t->tick();
            }
        }
        usleep(100_000);
    }
}

/** 从集群信息里挑 master broker 名/地址（单 broker 集群，取排序第一个）。 */
function rd_cluster(DefaultMQAdminExt $admin): array
{
    $ci = $admin->fetchBrokerClusterInfo();
    $names = array_keys($ci->brokerAddrTable);
    sort($names, SORT_STRING);
    $name = $names[0] ?? 'broker-a';
    $ids = $ci->brokerAddrTable[$name] ?? [];
    ksort($ids);
    $addr = $ids[0] ?? null;
    return [$name, $addr, $ci];
}

function rd_new_admin(string $ns): DefaultMQAdminExt
{
    $a = new DefaultMQAdminExt();
    $a->setNamesrvAddr($ns);
    $a->start();
    return $a;
}

/** 显式建 topic（单队列，让"一批 3 条"这类前提变得确定）。 */
function rd_ensure_topic(string $ns, string $topic, int $queues): void
{
    $a = rd_new_admin($ns);
    try {
        [$brokerName] = rd_cluster($a);
        $a->createTopic($brokerName, $topic, $queues);
    } finally {
        $a->shutdown();
    }
}

function rd_new_producer(string $ns, string $group): DefaultMQProducer
{
    $p = new DefaultMQProducer($group);
    $p->setNamesrvAddr($ns);
    $p->sendMsgTimeout = 5000;
    $p->start();
    return $p;
}

/** 发送带重试：路由/建联抖动不算失败，30 次 × 2s 打满才算。 */
function rd_send(DefaultMQProducer $p, string $topic, string $body): void
{
    $last = '';
    for ($i = 0; $i < 30; $i++) {
        try {
            $r = $p->send(new Message($topic, $body));
            if ($r->sendStatus === SendStatus::SEND_OK) {
                return;
            }
            $last = 'status=' . $r->sendStatus->name;
        } catch (Throwable $e) {
            $last = get_class($e) . ': ' . $e->getMessage();
        }
        sleep(2);
    }
    throw new RuntimeException("send '$body' never succeeded: $last");
}

/**
 * 起一个 push 消费者并等分配到位（FIRST_OFFSET：新组从队首读，S4 的
 * 消息发在启动之前也必须可见——Go 参考实现同款注释）。
 */
function rd_start_consumer(string $ns, string $group, object $listener, string ...$topics): DefaultMQPushConsumer
{
    $c = new DefaultMQPushConsumer($group);
    $c->setNamesrvAddr($ns);
    $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    $c->setMessageListener($listener);
    foreach ($topics as $t) {
        $c->subscribe($t, '*');
    }
    $c->start();
    return $c;
}

/** 等 broker 把 topic 路由注册出来（%DLQ% 在第一条落进去之前不存在）。 */
function rd_wait_route(string $ns, string $topic, float $windowSec): bool
{
    $a = rd_new_admin($ns);
    try {
        return rd_wait_until($windowSec, function () use ($a, $topic): bool {
            try {
                $r = $a->examineTopicRoute($topic);
                return $r !== null && count($r->brokerDatas ?? []) > 0;
            } catch (Throwable) {
                return false;
            }
        });
    } finally {
        $a->shutdown();
    }
}

/** 用一次性 pull 组从队首读全 topic——证明消息真的落在 %DLQ%，不信位点指针。 */
function rd_read_from_head(string $ns, string $topic, string $group, float $windowSec): RecBox
{
    $box = new RecBox();
    $c = new DefaultMQPullConsumer($group);
    $c->setNamesrvAddr($ns);
    $c->registerTopic($topic);
    $c->start();
    try {
        $got = 0;
        rd_wait_until($windowSec, function () use ($c, $topic, $box, &$got): bool {
            try {
                $queues = $c->fetchSubscribeMessageQueues($topic);
            } catch (Throwable) {
                return false; // 路由还没出来
            }
            foreach ($queues as $mq) {
                $offset = 0;
                for ($round = 0; $round < 40; $round++) {
                    $r = $c->pull($mq, '*', $offset, 32);
                    if ($r->status !== PullStatus::FOUND) {
                        break;
                    }
                    foreach ($r->msgFoundList as $m) {
                        $box->add($m);
                        $got++;
                    }
                    $offset = $r->nextBeginOffset;
                }
            }
            return $got > 0;
        }, $c);
        rd_observe(2.0, $c); // 同一次 pull 里可能还有漏网的
    } finally {
        $c->shutdown();
    }
    return $box;
}

function rd_recon_list(array $rs): string
{
    $first = $rs[0]['at'] ?? 0.0;
    $parts = [];
    foreach ($rs as $r) {
        $parts[] = $r['recon'] . '@' . sprintf('%.0fs', $r['at'] - $first);
    }
    return '[' . implode(' ', $parts) . ']';
}

function rd_rec_summary(array $rs): string
{
    $parts = [];
    foreach ($rs as $r) {
        $parts[] = "({$r['body']},recon={$r['recon']})";
    }
    return '[' . implode(' ', $parts) . ']';
}

// ------------------------------------------------------------------ main

echo "=== PHP redelivery live: ns=$ns prefix=$prefix legs=" . ($legsArg === '' ? 'all' : $legsArg) . " ===" . PHP_EOL;

$want = function (string $leg) use ($legsArg): bool {
    if ($legsArg === '' || $legsArg === 'all') {
        return true;
    }
    foreach (explode(',', $legsArg) as $l) {
        if (trim($l) === $leg) {
            return true;
        }
    }
    return false;
};

$createdTopics = [];
$shutdowns = [];
$cleanup = function () use (&$shutdowns, &$createdTopics, $ns): void {
    foreach ($shutdowns as $fn) {
        $fn();
    }
    if ($createdTopics !== []) {
        try {
            $a = rd_new_admin($ns);
            foreach ($createdTopics as $t) {
                try {
                    $a->deleteTopic($t);
                } catch (Throwable $e) {
                    echo "  delete $t: " . $e->getMessage() . PHP_EOL;
                }
            }
            $a->shutdown();
        } catch (Throwable) {
            // 清理失败不影响结果
        }
    }
};

// ------------------------------------------------------------------ S1
if ($want('s1')) {
    echo PHP_EOL . '--- S1 回投：%RETRY% 二次投递 + 延迟梯度 + topic 还原 ---' . PHP_EOL;
    $topic = $prefix . '_Retry';
    $createdTopics[] = $topic;
    $group = 'GID_' . $prefix . '_s1';
    try {
        rd_ensure_topic($ns, $topic, 1);
        $p = rd_new_producer($ns, 'GID_' . $prefix . '_s1_prod');
        $shutdowns[] = static fn() => $p->shutdown();

        $b = new RecBox();
        $c = rd_start_consumer($ns, $group, new RetryOnceListener($b, 'retry-me'), $topic);
        $shutdowns[] = static fn() => $c->shutdown();
        rd_wait_until(20.0, fn() => $c->assignedQueueCount() >= 1, $c);

        foreach (['retry-me', 'normal-1'] as $body) {
            rd_send($p, $topic, $body);
        }

        // 回投走延迟级别 3 = 10s，且 %RETRY% 路由要等 broker 下一次注册（<=30s）
        // 加一轮 rebalance 才可见——窗口必须盖住两者。
        rd_wait_until(60.0, fn() => count($b->ofBody('retry-me')) >= 2, $c);

        $retry = $b->ofBody('retry-me');
        $normal = $b->ofBody('normal-1');
        $check(count($retry) >= 2, 'S1 retry-me 被投递多次（>=2）', 'arrivals=' . count($retry));
        if (count($retry) >= 2) {
            $gap = $retry[count($retry) - 1]['at'] - $retry[0]['at'];
            $check($gap >= 8.0, 'S1 回投有延迟梯度（level3≈10s，>=8s）', sprintf('gap=%.1fs', $gap));
        } else {
            $check(false, 'S1 回投有延迟梯度（level3≈10s，>=8s）', '不足两次投递');
        }
        $redelivered = array_values(array_filter($retry, static fn(array $r): bool => $r['recon'] >= 1));
        $check(count($redelivered) >= 1, 'S1 二次投递带 RECONSUME_TIMES>=1', 'times=' . rd_recon_list($retry));
        $topicOk = count($redelivered) >= 1;
        foreach ($redelivered as $r) {
            if ($r['topic'] !== $topic) {
                $topicOk = false; // listener 看到的必须是业务 topic（客户端还原过）
            }
        }
        $check($topicOk, 'S1 重投消息 topic 还原为业务 topic', 'listener 主题分布=' . $b->topicSummary());
        $check(count($normal) === 1, 'S1 正常消息只投一次', 'arrivals=' . count($normal));
    } catch (Throwable $e) {
        $check(false, 'S1 异常中止', get_class($e) . ': ' . $e->getMessage());
    }
}

// ------------------------------------------------------------------ S2
if ($want('s2')) {
    echo PHP_EOL . '--- S2 死信终态：maxReconsumeTimes=2 ⇒ 3 次投递后进 %DLQ% ---' . PHP_EOL;
    $topic = $prefix . '_Dlq';
    $createdTopics[] = $topic;
    $group = 'GID_' . $prefix . '_s2';
    $dlqTopic = MixAll::getDlqTopic($group);
    try {
        rd_ensure_topic($ns, $topic, 1);
        $p = rd_new_producer($ns, 'GID_' . $prefix . '_s2_prod');
        $shutdowns[] = static fn() => $p->shutdown();

        $b = new RecBox();
        // maxReconsumeTimes 必须在 start 之前设（订阅/配置随 start 上报 broker）。
        $c = new DefaultMQPushConsumer($group);
        $c->setNamesrvAddr($ns);
        $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $c->maxReconsumeTimes = 2;
        $c->setMessageListener(new AlwaysFailListener($b));
        $c->subscribe($topic, '*');
        $c->start();
        $shutdowns[] = static fn() => $c->shutdown();
        rd_wait_until(20.0, fn() => $c->assignedQueueCount() >= 1, $c);
        rd_send($p, $topic, 'dlq-me');

        // 延迟级别 3（10s）+ 4（30s）意味着第三次投递在首投后 ~40s；共享机器
        // 争抢会再推后——窗口放宽，时间无论如何都打印出来。
        rd_wait_until(150.0, fn() => count($b->ofBody('dlq-me')) >= 3, $c);
        rd_observe(15.0, $c); // 反证窗：第 4 次不该出现
        $all = $b->ofBody('dlq-me');
        $times = rd_recon_list($all);
        $seqOk = count($all) >= 3 && $all[0]['recon'] === 0 && $all[1]['recon'] === 1 && $all[2]['recon'] === 2;
        $check($seqOk, 'S2 恰好投递 3 次且 RECONSUME_TIMES 为 0/1/2', 'times=' . $times);
        $check(count($all) === 3, 'S2 用尽后不再投递（观察窗口内只有 3 次）',
            'arrivals=' . count($all) . ' times=' . $times);
        $topicOk = count($all) >= 3;
        foreach (array_slice($all, 0, 3) as $r) {
            if ($r['topic'] !== $topic) {
                $topicOk = false;
            }
        }
        $check($topicOk, 'S2 重投期间 topic 还原为业务 topic', 'listener 主题分布=' . $b->topicSummary());
        $c->shutdown();

        // broker 在第一条消息落进去时才建并注册 %DLQ%<group>。
        $check(rd_wait_route($ns, $dlqTopic, 30.0), 'S2 broker 自动创建并注册 %DLQ% 路由', 'dlq=' . $dlqTopic);

        $probe = rd_read_from_head($ns, $dlqTopic, 'GID_' . $prefix . '_s2probe', 25.0);
        $got = $probe->ofBody('dlq-me');
        $check(count($got) >= 1, 'S2 消息落在 %DLQ%<group>', 'got=' . rd_rec_summary($got));
        // broker 死信时存 reconsumeTimes+1，所以 2 -> 3。
        $check(count($got) === 1 && $got[0]['recon'] === 3,
            'S2 DLQ 消息 RECONSUME_TIMES=3（第 3 次回投转死信）', 'got=' . rd_rec_summary($got));
        $check(count($got) === 1 && $got[0]['topic'] === $dlqTopic,
            'S2 DLQ 消息 topic 就是 %DLQ%<group>', 'probe 主题分布=' . $probe->topicSummary());
    } catch (Throwable $e) {
        $check(false, 'S2 异常中止', get_class($e) . ': ' . $e->getMessage());
    }
}

// ------------------------------------------------------------------ S3
if ($want('s3')) {
    echo PHP_EOL . '--- S3 顺序毒消息：Suspend 到本地上限 → 顺序回投 → %DLQ% ---' . PHP_EOL;
    $topic = $prefix . '_OrderlyDlq';
    $createdTopics[] = $topic;
    $group = 'GID_' . $prefix . '_s3';
    $dlqTopic = MixAll::getDlqTopic($group);
    try {
        rd_ensure_topic($ns, $topic, 1);
        $p = rd_new_producer($ns, 'GID_' . $prefix . '_s3_prod');
        $shutdowns[] = static fn() => $p->shutdown();

        $b = new RecBox();
        $c = new DefaultMQPushConsumer($group);
        $c->setNamesrvAddr($ns);
        $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $c->consumeMessageBatchMaxSize = 1;
        $c->maxReconsumeTimes = 2;
        $c->suspendCurrentQueueTimeMillis = 50; // 挂起短一点，阶梯爬得快
        $c->setMessageListener(new SuspendOrderlyListener($b));
        $c->subscribe($topic, '*');
        $c->start();
        $shutdowns[] = static fn() => $c->shutdown();
        rd_wait_until(20.0, fn() => $c->assignedQueueCount() >= 1, $c);
        rd_send($p, $topic, 'orderly-poison');

        rd_wait_until(60.0, fn() => count($b->ofBody('orderly-poison')) >= 3, $c);
        rd_observe(15.0, $c);
        $all = $b->ofBody('orderly-poison');
        $times = rd_recon_list($all);
        $seqOk = count($all) >= 3 && $all[0]['recon'] === 0 && $all[1]['recon'] === 1 && $all[2]['recon'] === 2;
        $check($seqOk, 'S3 顺序侧本地恰好投递 3 次（0/1/2）', 'times=' . $times);
        // 第三次之后消息被交给 broker、队列位点前进——出现第 4 次说明上限没生效。
        $check(count($all) === 3, 'S3 交 broker 后不再投递（观察窗口内只有 3 次）',
            'arrivals=' . count($all) . ' times=' . $times);
        $c->shutdown();

        $check(rd_wait_route($ns, $dlqTopic, 30.0), 'S3 broker 自动创建并注册 %DLQ% 路由', 'dlq=' . $dlqTopic);
        $probe = rd_read_from_head($ns, $dlqTopic, 'GID_' . $prefix . '_s3probe', 25.0);
        $got = $probe->ofBody('orderly-poison');
        $check(count($got) >= 1, 'S3 顺序回投的消息落在 %DLQ%（组仍持队列锁）', 'got=' . rd_rec_summary($got));
        // 顺序回投把 msg.getReconsumeTimes()+1 写进 RECONSUME_TIME（客户端侧 +1，
        // 并发路径的 +1 来自 broker——见 S2）。这个 +1 掉了其余表现全都对。
        $check(count($got) === 1 && $got[0]['recon'] === 3,
            'S3 DLQ 消息 RECONSUME_TIMES=3（客户端侧 +1）', 'got=' . rd_rec_summary($got));
        $check(count($got) === 1 && $got[0]['topic'] === $dlqTopic,
            'S3 DLQ 消息 topic 就是 %DLQ%<group>', 'probe 主题分布=' . $probe->topicSummary());
    } catch (Throwable $e) {
        $check(false, 'S3 异常中止', get_class($e) . ': ' . $e->getMessage());
    }
}

// ------------------------------------------------------------------ S4
if ($want('s4')) {
    echo PHP_EOL . '--- S4 部分 ack（ackIndex=0）：尾巴 2 条回投、已 ack 的不回投 ---' . PHP_EOL;
    $topic = $prefix . '_AckIndex';
    $ctrlTopic = $prefix . '_AckControl';
    $createdTopics[] = $topic;
    $createdTopics[] = $ctrlTopic;
    $group = 'GID_' . $prefix . '_s4';
    $ctrlGroup = 'GID_' . $prefix . '_s4ctrl';
    try {
        foreach ([$topic, $ctrlTopic] as $t) {
            rd_ensure_topic($ns, $t, 1);
        }
        $p = rd_new_producer($ns, 'GID_' . $prefix . '_s4_prod');
        $shutdowns[] = static fn() => $p->shutdown();

        $bodies = ['ack-0', 'ack-1', 'ack-2'];
        // 发在启动消费者之前：单队列 topic 的第一次 pull 就会把 3 条全带回来并
        // 组成一批——这是整个场景的前提。
        foreach ([$topic, $ctrlTopic] as $t) {
            foreach ($bodies as $body) {
                rd_send($p, $t, $body);
            }
        }

        $b = new RecBox();
        $c = new DefaultMQPushConsumer($group);
        $c->setNamesrvAddr($ns);
        $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $c->consumeMessageBatchMaxSize = count($bodies); // 3 条必须一批
        $c->setMessageListener(new PartialAckTo0Listener($b));
        $c->subscribe($topic, '*');
        $c->start();
        $shutdowns[] = static fn() => $c->shutdown();

        // 未 ack 的尾巴按延迟级别 3 = 10s 回投。
        rd_wait_until(60.0, function () use ($b, $bodies): bool {
            foreach (array_slice($bodies, 1) as $body) {
                foreach ($b->ofBody($body) as $r) {
                    if ($r['recon'] >= 1) {
                        return true;
                    }
                }
            }
            return false;
        }, $c);
        rd_wait_until(30.0, fn() => count($b->distinctBodies()) === count($bodies), $c);
        rd_observe(3.0, $c);
        $c->shutdown();

        $first = $b->ofBody($bodies[0]);
        $check(count($first) === 1, 'S4 已 ack 的首条整个窗口只投一次', 'arrivals=' . count($first));
        $redelivered = 0;
        $tailDetail = [];
        foreach (array_slice($bodies, 1) as $body) {
            $n = 0;
            foreach ($b->ofBody($body) as $r) {
                if ($r['recon'] >= 1) {
                    $n++;
                }
            }
            if ($n > 0) {
                $redelivered++;
            }
            $tailDetail[] = $body . '=' . rd_recon_list($b->ofBody($body));
        }
        $check($redelivered === 2, 'S4 未 ack 的尾巴 2 条经 %RETRY% 回投', implode(' ', $tailDetail));
        $check(count($b->distinctBodies()) === count($bodies), 'S4 3 条最终全部消费完',
            'bodies=' . implode(',', $b->distinctBodies()));

        // 位点仍整批提交（ackIndex 也一样）：尾巴经 send-back 交给了 broker，位点
        // 可以推进到 max+1 = 3（C4 地板的"已 ack 批"口径）。
        $a = rd_new_admin($ns);
        try {
            $route = $a->examineTopicRoute($topic);
            $bn = $route->brokerDatas[0]->brokerName ?? 'broker-a';
            $mq0 = new MessageQueue($topic, (string) $bn, 0);
            $off = $a->examineConsumerOffset($group, $mq0);
            $check($off === 3, 'S4 业务队列位点仍整批提交到 3', 'offset=' . var_export($off, true));
        } finally {
            $a->shutdown();
        }

        // 对照组：同样 3 条、不碰 ackIndex ⇒ 一条都不回投。没有它，上面的回投
        // 就可能被赖给脚手架而不是 ack。
        $cb = new RecBox();
        $cc = new DefaultMQPushConsumer($ctrlGroup);
        $cc->setNamesrvAddr($ns);
        $cc->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $cc->consumeMessageBatchMaxSize = count($bodies);
        $cc->setMessageListener(new CollectListener($cb));
        $cc->subscribe($ctrlTopic, '*');
        $cc->start();
        rd_wait_until(30.0, fn() => count($cb->distinctBodies()) === count($bodies), $cc);
        rd_observe(12.0, $cc); // 回投要 level 3 = 10s 才会显形
        $cc->shutdown();
        $ctrlOk = count($cb->distinctBodies()) === count($bodies) && count($cb->recs) === count($bodies);
        $check($ctrlOk, 'S4 对照组（不碰 ackIndex）一条都不回投',
            'deliveries=' . count($cb->recs) . ' bodies=' . implode(',', $cb->distinctBodies()));
    } catch (Throwable $e) {
        $check(false, 'S4 异常中止', get_class($e) . ': ' . $e->getMessage());
    }
}

$cleanup();

echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);
