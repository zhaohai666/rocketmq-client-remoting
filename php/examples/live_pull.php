<?php

declare(strict_types=1);

// Live pull-consumer verification covering the same scenario set (S1 queues, S1b balanced view,
// S2 send + per-queue drain, S3 offset commit round-trip).
//
//   php examples/live_pull.php [namesrv] [legs]     # legs = all | s1,s2,s3
//
// Why this deserves its own leg: the pull mode has no background rebalance thread, and
// `fetchMessageQueuesInBalance` is computed **on the spot** from "real route +
// GET_CONSUMER_LIST_BY_GROUP(38)"; only a live cluster can supply both inputs: the route comes
// from a real name server, the membership table from the broker's consumerTable (heartbeat
// registration). The criterion is "an instance that has **never pulled anything** gets a
// non-empty share" — when computation is impossible, this port keeps the existing assignment
// (empty set), so a non-empty result can only come from a real computation, not a fallback.
//
//
// Closing line `PASS=<n> FAIL=<n>`; exits non-zero on any FAIL.
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

/** Stable queue identifier: brokerName:queueId; all comparisons of observations key on it. */
$qKey = static fn (MessageQueue $q): string => $q->brokerName . ':' . $q->queueId;

/** Polls until the condition holds; returns whether it holds (also carries back the last observation). */
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

// ---------------------------------------------------------------- create topic
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

// ============================================================ S1 / S1b queues
if ($want('s1') || $want('s2') || $want('s3')) {
    $consumer = new DefaultMQPullConsumer($group);
    $consumer->setNamesrvAddr($ns);
    $consumer->setInstanceName('php-pull-live-' . $stamp);
    // The single source of the heartbeat subscription set; must be registered before start()
    // (see pitfall 1 in the file header).
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
                return false; // route not ready yet
            }
        });
        $check(count($routes) === $queueNum, "S1 got $queueNum queues", 'got=' . count($routes));
        $ids = array_map(static fn (MessageQueue $q): int => $q->queueId, $routes);
        sort($ids);
        $check($ids === range(0, $queueNum - 1), 'S1 queue ids 0..3 with no duplicates or gaps',
            'ids=' . implode(',', $ids));

        // ---- S1b balanced view ----
        // This instance has not pulled anything yet: the fallback path yields an empty set, so a
        // non-empty result can only come from a real route + 38 computation.
        echo PHP_EOL . 'S1b fetchMessageQueuesInBalance' . PHP_EOL;
        $hbOk = $consumer->sendHeartbeatToAllBroker();
        $check($hbOk > 0, 'S1b heartbeat emitted after registering the topic (prerequisite for 38 visibility)', "heartbeat_ok=$hbOk");
        $mine = [];
        $until(20_000, static function () use ($consumer, $topic, $queueNum, &$mine): bool {
            try {
                $mine = $consumer->fetchMessageQueuesInBalance($topic);
                if (count($mine) === $queueNum) {
                    return true;
                }
            } catch (Throwable) {
                // route / 38 momentarily unreadable — re-send a heartbeat and retry; not a failure
            }
            $consumer->sendHeartbeatToAllBroker();
            return false;
        });
        $check(count($mine) === $queueNum,
            "S1b balanced view: an exclusively-assigned group gets all $queueNum queues (never pulled => only route+38 could compute it)",
            'got=' . count($mine) . " of $queueNum");
        $whole = array_map($qKey, $routes);
        $subset = true;
        foreach ($mine as $mq) {
            if (!in_array($qKey($mq), $whole, true)) {
                $subset = false;
            }
        }
        $check($subset, 'S1b the balanced view is a subset of the subscription view',
            'mine=' . implode(' ', array_map($qKey, $mine)));

        // ---- S2 send + per-queue drain ----
        if ($want('s2')) {
            echo PHP_EOL . "S2 producing $nMsg messages and pulling queue by queue" . PHP_EOL;
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
            $check(count($sent) === $nMsg, "S2 produced $nMsg messages successfully", 'ok=' . count($sent));

            // ConsumeQueue dispatches asynchronously: poll until the per-queue max-min sum lands (see pitfall 2 in the file header).
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
            $check($spanOk, "S2 total backlog across queues reaches $nMsg", 'span_ok=' . ($spanOk ? '1' : '0'));

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
                && count($got) === $nMsg, "S2 manual pull receives all $nMsg messages with matching content",
                'got=' . count($got) . ' sent=' . count($sent));

            // ---- S3 offset commit / round-trip (broker round-trip, not a local table)----
            if ($want('s3')) {
                echo PHP_EOL . 'S3 read back after offset commit' . PHP_EOL;
                $mq0 = $routes[0];
                $target = $consumer->maxOffset($mq0);
                $consumer->updateConsumeOffset($mq0, $target);
                $back = null;
                $ok = $until(10_000, static function () use ($consumer, $mq0, $target, &$back): bool {
                    $back = $consumer->fetchConsumeOffset($mq0);
                    return $back === $target;
                });
                $check($ok, 'S3 offset round-trips consistently after commit', "want=$target got=" . var_export($back, true));
            }
        }
    } finally {
        $consumer->shutdown();
    }
}

// ---------------------------------------------------------------- teardown
try {
    $admin->deleteTopic($topic);
    $check(true, 'teardown: deleteTopic', "topic=$topic");
} catch (Throwable $e) {
    $check(false, 'teardown: deleteTopic', $e->getMessage());
}
$admin->shutdown();

echo '==' . " summary: PASS=$pass FAIL=$fail ==" . PHP_EOL;
exit($fail > 0 ? 1 : 0);
