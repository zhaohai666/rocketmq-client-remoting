<?php

declare(strict_types=1);

// Live counterpart of tests/RunPullRebalance.php: the pull-side background rebalance
// (Java DefaultMQPullConsumer.enableRebalance, default true) asserted against a real
// RocketMQ 5.x broker.
//
//   php examples/live_pull_rebalance.php [namesrv]
//
// Java's pull consumer rebalances: DefaultMQPullConsumer.java:97 defaults enableRebalance
// to true, and the shared RebalanceService drives tryRebalance() every 20s, calling
// MessageQueueListener.messageQueueChanged(topic, mqAll, mqDivided) when the allocation
// changed. What a live cluster proves that a unit test cannot:
//
//   L1  two instances of one group split a multi-queue topic -- disjoint shares whose
//       union is the whole topic (only possible if both read the same broker-side
//       consumer list and run the same allocation formula);
//   L2  the listener really fired with mqAll = the topic's full queue set;
//   L3  enableRebalance=false stops the loop (no callbacks, empty table) while
//       fetchMessageQueuesInBalance keeps computing on the spot.
//
// PHP has no consumer threads: rebalance rounds are driven by tickRebalance() here, the
// same tick style the rest of this port uses. Closing line `PASS=<n> FAIL=<n>`; exits
// non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\MessageQueueListener;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQPullConsumer;

final class PrbLiveRecorder implements MessageQueueListener
{
    /** @var list<array{string, list<MessageQueue>, list<MessageQueue>}> */
    public array $calls = [];

    public function messageQueueChanged(string $topic, array $mqAll, array $mqDivided): void
    {
        $this->calls[] = [$topic, $mqAll, $mqDivided];
    }
}

$ns = $argv[1] ?? '127.0.0.1:9876';
$prefix = 'PullRebLive_' . number_format(microtime(true) * 1000, 0, '.', '');
$topic = $prefix . '_Topic';
const N_QUEUE = 8;
// Shorter than Java's 20s so the probe finishes quickly; the loop shape is the point.
const WAIT_MILLIS = 3000;
const MIN_MILLIS = 500;

$pass = 0;
$fail = 0;
$check = static function (string $name, bool $ok, string $detail = '') use (&$pass, &$fail): void {
    if ($ok) {
        $pass++;
        echo "  [PASS] $name" . ($detail !== '' ? "  $detail" : '') . PHP_EOL;
    } else {
        $fail++;
        echo "  [FAIL] $name" . ($detail !== '' ? "  $detail" : '') . PHP_EOL;
    }
};

echo "======================================================================" . PHP_EOL;
echo "Pull-side rebalance live (PHP): ns=$ns prefix=$prefix" . PHP_EOL;
echo "======================================================================" . PHP_EOL;

// ------------------------------------------------------------- bootstrap: topic
$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $names = array_keys($ci->brokerAddrTable);
    sort($names, SORT_STRING);
    $brokerName = $names[0] ?? 'broker-a';
    $admin->createTopic($brokerName, $topic, N_QUEUE);
} catch (Throwable $e) {
    echo '!! createTopic failed: ' . $e->getMessage() . PHP_EOL;
    exit(1);
}
// A createTopic only reaches the name server on the broker's own registration round; the
// probe creates nothing else, so the route must be fully visible (all 8 queues) first.
$routeReady = false;
$deadline = microtime(true) + 30.0;
while (microtime(true) < $deadline) {
    try {
        $r = $admin->examineTopicRoute($topic);
        $n = 0;
        foreach ($r->queueDatas as $q) {
            $n += $q->readQueueNums;
        }
        if ($n >= N_QUEUE) {
            $routeReady = true;
            break;
        }
    } catch (Throwable) {
    }
    usleep(300000);
}
$check('the topic route is visible with all queues before the consumers start', $routeReady,
    "want $N_QUEUE read queues");
if (!$routeReady) {
    echo "PASS=$pass FAIL=$fail" . PHP_EOL;
    exit(1);
}
echo "  created $topic with " . N_QUEUE . " queues" . PHP_EOL;

$make = static function (string $inst, string $group, ?PrbLiveRecorder $rec, bool $enable)
use ($ns, $topic, $check): DefaultMQPullConsumer {
    $c = new DefaultMQPullConsumer($group);
    // A distinct instanceName gives a distinct clientId; the clientId is what the broker's
    // GET_CONSUMER_LIST_BY_GROUP returns, so without this both facades would collapse
    // into one group member.
    $c->setInstanceName($inst);
    $c->setNamesrvAddr($ns);
    $c->setRebalanceWaitMillis(WAIT_MILLIS);
    $c->setRebalanceMinIntervalMillis(MIN_MILLIS);
    if ($rec !== null) {
        $c->registerMessageQueueListener($topic, $rec);
    } else {
        $c->registerMessageQueueListener($topic, null);
    }
    $c->setEnableRebalance($enable);
    $c->start();
    return $c;
};
// PHP has no consumer threads: rebalance rounds are driven here, alternating over the
// facades the way tick() is alternated with poll() elsewhere in this port.
$drive = static function (array $consumers, float $seconds): void {
    $deadline = microtime(true) + $seconds;
    while (microtime(true) < $deadline) {
        foreach ($consumers as $c) {
            $c->tickRebalance();
        }
        usleep(200000);
    }
};
$shareIds = static function (array $share): array {
    $ids = [];
    foreach ($share as $mq) {
        $ids[] = $mq->queueId;
    }
    sort($ids);
    return $ids;
};

$recA = new PrbLiveRecorder();
$recB = new PrbLiveRecorder();
$a = $make($prefix . 'InstA', $prefix . '_group', $recA, true);
$b = $make($prefix . 'InstB', $prefix . '_group', $recB, true);
$c = null;
try {
    echo '  started two instances, driving rebalance rounds...' . PHP_EOL;
    $drive([$a, $b], 15.0);
    $shareA = $a->getRebalanceTable()[$topic] ?? [];
    $shareB = $b->getRebalanceTable()[$topic] ?? [];
    echo '  INFO share A=' . count($shareA) . ' queues, share B=' . count($shareB) . ' queues' . PHP_EOL;

    $idsA = $shareIds($shareA);
    $idsB = $shareIds($shareB);
    $check('L1-both instances ended up with a share of the topic',
        $idsA !== [] && $idsB !== [], sprintf('A=%d B=%d', count($idsA), count($idsB)));
    $overlap = count(array_intersect($idsA, $idsB));
    $check('L1-the two shares are disjoint (one queue is never owned twice)', $overlap === 0,
        "overlap=$overlap");
    $union = count(array_unique(array_merge($idsA, $idsB)));
    $check('L1-the two shares cover every queue of the topic exactly once',
        $union === N_QUEUE, "union=$union of " . N_QUEUE);

    $check('L2-the allocation change was reported to the listener',
        count($recA->calls) >= 1 && count($recB->calls) >= 1,
        sprintf('A calls=%d B calls=%d', count($recA->calls), count($recB->calls)));
    [$lTopic, $lAll, $lDivided] = $recA->calls[0];
    $check('L2-messageQueueChanged named the topic', $lTopic === $topic, $lTopic);
    $check('L2-messageQueueChanged carried the topic\'s full queue set as mqAll',
        count($lAll) === N_QUEUE, 'mqAll=' . count($lAll));

    $inBalance = $a->fetchMessageQueuesInBalance($topic);
    $check('L1-fetch_message_queues_in_balance answers from the rebalance table',
        $shareIds($inBalance) === $idsA,
        sprintf('got=%d expect=%d', count($inBalance), count($idsA)));

    // L3: the switch really gates the loop.
    $recC = new PrbLiveRecorder();
    $c = $make($prefix . 'InstC', $prefix . '_group', $recC, false);
    echo '  started a third instance with enableRebalance=false, driving 2 rounds...' . PHP_EOL;
    $drive([$a, $b, $c], (WAIT_MILLIS * 2 + 4000) / 1000.0);
    $check('L3-enableRebalance=false stops the loop: no callback at all',
        count($recC->calls) === 0, 'calls=' . count($recC->calls));
    $check('L3-enableRebalance=false leaves the rebalance table empty',
        $c->getRebalanceTable() === [], 'table=' . json_encode($c->getRebalanceTable()));
    $onTheSpot = $c->fetchMessageQueuesInBalance($topic);
    $check('L3-with the loop off, fetch_message_queues_in_balance still computes on the spot',
        $onTheSpot !== [], 'got=' . count($onTheSpot) . ' queues');
} finally {
    foreach ([$c, $b, $a] as $consumer) {
        if ($consumer !== null) {
            try {
                $consumer->shutdown();
            } catch (Throwable $e) {
                echo '  (shutdown ignored: ' . $e->getMessage() . ')' . PHP_EOL;
            }
        }
    }
}

echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);
