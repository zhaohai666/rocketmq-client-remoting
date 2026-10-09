<?php

declare(strict_types=1);

// PHP 端真机拉模式消费者验证：对标 cpp/examples/live_pull.cpp 与 rust/examples/live_pull_consumer.rs
// 的同场景（S1 队列、S1b 平衡视图、S2 发送+逐队列拉全、S3 位点提交回读）。
//
//   php examples/live_pull.php [namesrv] [legs]     # legs = all | s1,s2,s3
//
// 为什么值得单独一条腿：拉模式没有后台 rebalance 线程，`fetchMessageQueuesInBalance`
// 是**当场**用「真实路由 + GET_CONSUMER_LIST_BY_GROUP(38)」算出来的，两路输入只有真机
// 才给得起：路由来自真 name server，成员表来自 broker 的 consumerTable（心跳注册）。
// 判据是「**一笔都没拉过**的实例拿到非空份额」——算不动时本端口的设计是保留现有分配
// （空集），所以非空只可能来自真算，不是兜底。
//
// 两个踩过的坑（不要"顺手优化"掉）：
//   1. 心跳订阅集只从 registerTopics 来（Java subscriptions():357-385），所以必须在
//      start() **之前** registerTopic()，否则 broker 数不出本组、38 回空列表（这是正确
//      行为，但这条腿就没得测了）；
//   2. 发完不能立刻读 maxOffset：broker 的 ConsumeQueue 是异步分发的，必须轮询到各队列
//      max-min 之和到位（同 python 的 wait_offsets）。
//
// 收口行 `PASS=<n> FAIL=<n>`；任一 FAIL 非 0 退出。
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;

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
$legsArg = trim($argv[2] ?? 'all');
$want = static fn (string $leg): bool => $legsArg === 'all'
    || in_array($leg, array_map('trim', explode(',', $legsArg)), true);

$stamp = time();
$topic = 'PhpPullLive_' . $stamp;
$group = 'PG_PhpPullLive_' . $stamp;
$nMsg = 12;
$queueNum = 4;

echo "======================================================================" . PHP_EOL;
echo "PullConsumer live (PHP): ns=$ns topic=$topic group=$group" . PHP_EOL;
echo "======================================================================" . PHP_EOL;

/** 队列的稳定标识：brokerName:queueId（与其余端口的 qKey 同口径）。 */
$qKey = static fn (MessageQueue $q): string => $q->brokerName . ':' . $q->queueId;

/** 轮询到条件成立；返回是否成立（顺带把最后一次的观测值带回来）。 */
$until = static function (int $timeoutMs, callable $cond): bool {
    $deadline = microtime(true) + $timeoutMs / 1000.0;
    do {
        if ($cond() === true) {
            return true;
        }
        usleep(500_000);
    } while (microtime(true) < $deadline);
    return false;
};

// ---------------------------------------------------------------- 建 topic
$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $brokerName = array_key_first($ci->brokerAddrTable);
    $admin->createTopic($brokerName, $topic, $queueNum);
} catch (Throwable $e) {
    echo '!! createTopic failed: ' . $e->getMessage() . PHP_EOL;
    exit(1);
}

// ============================================================ S1 / S1b 队列
if ($want('s1') || $want('s2') || $want('s3')) {
    $consumer = new DefaultMQPullConsumer($group);
    $consumer->setNamesrvAddr($ns);
    $consumer->setInstanceName('php-pull-live-' . $stamp);
    // 心跳订阅集的唯一来源，必须在 start() 之前登记（见文件头坑 1）。
    $consumer->registerTopic($topic);
    $consumer->start();

    try {
        echo PHP_EOL . 'S1 fetchSubscribeMessageQueues' . PHP_EOL;
        $routes = [];
        $until(20_000, static function () use ($consumer, $topic, $queueNum, &$routes): bool {
            try {
                $routes = $consumer->fetchSubscribeMessageQueues($topic);
                return count($routes) >= $queueNum;
            } catch (Throwable) {
                return false; // 路由还没出来
            }
        });
        $check(count($routes) === $queueNum, "S1 拿到 $queueNum 个队列", 'got=' . count($routes));
        $ids = array_map(static fn (MessageQueue $q): int => $q->queueId, $routes);
        sort($ids);
        $check($ids === range(0, $queueNum - 1), 'S1 队列号 0..3 不重不漏',
            'ids=' . implode(',', $ids));

        // ---- S1b 平衡视图 ----
        // 本实例此刻一笔都没拉过：兜底路径给的是空集，所以非空只可能是路由 + 38 真算的。
        echo PHP_EOL . 'S1b fetchMessageQueuesInBalance' . PHP_EOL;
        $hbOk = $consumer->sendHeartbeatToAllBroker();
        $check($hbOk > 0, 'S1b 登记 topic 后心跳发出去了（38 可见的前提）', "heartbeat_ok=$hbOk");
        $mine = [];
        $until(20_000, static function () use ($consumer, $topic, $queueNum, &$mine): bool {
            try {
                $mine = $consumer->fetchMessageQueuesInBalance($topic);
                if (count($mine) === $queueNum) {
                    return true;
                }
            } catch (Throwable) {
                // 路由 / 38 一时数不出来 —— 补一发心跳再试，不算失败
            }
            $consumer->sendHeartbeatToAllBroker();
            return false;
        });
        $check(count($mine) === $queueNum,
            "S1b 平衡视图：独占分组拿到全部 $queueNum 个队列（未拉取过⇒只可能是路由+38 算出来的）",
            'got=' . count($mine) . " of $queueNum");
        $whole = array_map($qKey, $routes);
        $subset = true;
        foreach ($mine as $mq) {
            if (!in_array($qKey($mq), $whole, true)) {
                $subset = false;
            }
        }
        $check($subset, 'S1b 平衡视图是订阅视图的子集',
            'mine=' . implode(' ', array_map($qKey, $mine)));

        // ---- S2 发送 + 逐队列拉全 ----
        if ($want('s2')) {
            echo PHP_EOL . "S2 生产 $nMsg 条并逐队列拉取" . PHP_EOL;
            $sent = [];
            $producer = new DefaultMQProducer($group);
            $producer->setNamesrvAddr($ns);
            $producer->start();
            try {
                for ($i = 0; $i < $nMsg; $i++) {
                    $body = sprintf('php-pull-%02d', $i);
                    $msg = new Message($topic, $body, 'TagA', $body);
                    $res = $producer->send($msg);
                    if ($res->sendStatus === SendStatus::SEND_OK) {
                        $sent[$body] = true;
                    }
                }
            } finally {
                $producer->shutdown();
            }
            $check(count($sent) === $nMsg, "S2 生产 $nMsg 条成功", 'ok=' . count($sent));

            // ConsumeQueue 异步分发：轮询到各队列 max-min 之和到位（见文件头坑 2）。
            $spanOk = $until(15_000, static function () use ($consumer, $routes, $nMsg): bool {
                $total = 0;
                foreach ($routes as $mq) {
                    try {
                        $total += $consumer->maxOffset($mq) - $consumer->minOffset($mq);
                    } catch (Throwable) {
                        return false;
                    }
                }
                return $total >= $nMsg;
            });
            $check($spanOk, "S2 各队列存量合计 $nMsg 条", 'span_ok=' . ($spanOk ? '1' : '0'));

            $got = [];
            foreach ($routes as $mq) {
                $offset = $consumer->minOffset($mq);
                $maxOffset = $consumer->maxOffset($mq);
                for ($round = 0; $round < 40 && $offset < $maxOffset; $round++) {
                    $r = $consumer->pull($mq, '*', $offset, 32);
                    if ($r->status !== PullStatus::FOUND) {
                        break;
                    }
                    foreach ($r->msgFoundList as $m) {
                        $got[(string) $m->getBody()] = true;
                    }
                    $offset = $r->nextBeginOffset;
                }
            }
            $check(count(array_diff(array_keys($sent), array_keys($got))) === 0
                && count($got) === $nMsg, "S2 手动拉取收全 $nMsg 条且内容一致",
                'got=' . count($got) . ' sent=' . count($sent));

            // ---- S3 位点提交 / 回读（broker 往返，不是本地表）----
            if ($want('s3')) {
                echo PHP_EOL . 'S3 位点提交后回读' . PHP_EOL;
                $mq0 = $routes[0];
                $target = $consumer->maxOffset($mq0);
                $consumer->updateConsumeOffset($mq0, $target);
                $back = null;
                $ok = $until(10_000, static function () use ($consumer, $mq0, $target, &$back): bool {
                    $back = $consumer->fetchConsumeOffset($mq0);
                    return $back === $target;
                });
                $check($ok, 'S3 位点提交后回读一致', "want=$target got=" . var_export($back, true));
            }
        }
    } finally {
        $consumer->shutdown();
    }
}

// ---------------------------------------------------------------- 收尾
try {
    $admin->deleteTopic($topic);
    $check(true, '收尾：deleteTopic', "topic=$topic");
} catch (Throwable $e) {
    $check(false, '收尾：deleteTopic', $e->getMessage());
}
$admin->shutdown();

echo '==' . " summary: PASS=$pass FAIL=$fail ==" . PHP_EOL;
exit($fail > 0 ? 1 : 0);
