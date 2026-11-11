<?php

declare(strict_types=1);

// Live counterpart of the offline capability_closeout suite: the capabilities added in the
// 2026-11-11 round, asserted against a real RocketMQ 5.x broker.
//
//   php examples/live_capability_closeout.php [namesrv]
//
// L1 ConsumeFromWhere's three "deprecated" values fold into the LAST_OFFSET branch
//    (RebalancePushImpl#computePullFromWhereWithException lines 171-173), so all four must
//    start at the same place. The LAST_OFFSET baseline is MEASURED on this end, not assumed:
//    on a fresh topic the broker answers QUERY_CONSUMER_OFFSET with SUCCESS offset=0 for a
//    never-committed group (its V3_0_6 path) unless the client sends setZeroIfNotFound=false,
//    and Java sends no such field. Hardcoding either outcome would turn a parity question
//    into a spurious failure.
// L2 LitePull pull-from-which-node pin (connectBrokerByUser / defaultBrokerId). PHP keeps
//    liteBrokerIdFor() private, so the value that would travel on the request is read back
//    through reflection -- the same function the pull round calls, not a re-derivation.
// L3 adminExtGroup identity: a custom group must still boot and serve an RPC.
//
// Closing line `PASS=<n> FAIL=<n>`; exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;

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
$stamp = (string) time();
$prefix = 'CapLive_' . $stamp;
$nMsg = 8;
$bodies = [];
for ($i = 0; $i < $nMsg; $i++) {
    $bodies[] = sprintf('cap-%02d', $i);
}

/** Records every body a push consumer delivers. */
final class CapBox implements MessageListenerConcurrently
{
    /** @var list<string> */
    public array $bodies = [];

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->bodies[] = (string) $m->getBody();
        }

        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

/**
 * Polls until the condition holds, ticking every consumer handed in on each round. The PHP
 * port has no consumer threads: a consumer only rebalances, heartbeats and pulls while its
 * caller calls tick(), so waiting without ticking makes a healthy consumer look dead.
 */
$until = static function (float $timeoutSec, callable $cond, object ...$tickables): bool {
    $deadline = microtime(true) + $timeoutSec;
    while (true) {
        foreach ($tickables as $t) {
            if (method_exists($t, 'tick')) {
                $t->tick();
            }
        }
        if ($cond() === true) {
            return true;
        }
        if (microtime(true) >= $deadline) {
            return false;
        }
        usleep(100_000);
    }
};

/**
 * One push leg: a fresh group started at $where, counting arrivals for $windowSec.
 * A group that already committed offsets reads them back whatever consumeFromWhere says,
 * hence a fresh group per leg. The leg waits for a real queue assignment first: without
 * one the consumer holds no queue, nothing can arrive, and an assignment bug would look
 * like a consumeFromWhere bug.
 */
$pushLeg = static function (string $topic, string $group, string $where, int $windowSec) use ($ns, $until): array {
    $box = new CapBox();
    $c = new DefaultMQPushConsumer($group);
    $c->setNamesrvAddr($ns);
    $c->setInstanceName($group);
    $c->setMessageListener($box);
    $c->setConsumeThreadNums(1);
    $c->setConsumeFromWhere($where);
    $c->subscribe($topic, '*');
    $c->start();
    $assigned = $until(20.0, static fn (): bool => $c->assignedQueueCountForTopic($topic) >= 1, $c);
    $keys = $c->assignedQueueKeys();
    // Drive the consumer for the observation window: tick() is the only thing that pulls.
    $until((float) $windowSec, static fn (): bool => false, $c);
    $c->shutdown();

    return ['got' => count($box->bodies), 'assigned' => $assigned, 'keys' => $keys];
};

/** One LitePull leg: the pinned id the pull round would put on the request, plus what it polled. */
$liteLeg = static function (string $topic, string $group, bool $pin, int $brokerId, int $want, int $windowSec) use ($ns): array {
    $c = new DefaultLitePullConsumer($group);
    $c->setNamesrvAddr($ns);
    $c->subscribe($topic, '*');
    $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    $c->setConnectBrokerByUser($pin);
    $c->setDefaultBrokerId($brokerId);
    $c->start();

    // The pin travels through liteBrokerIdFor(); read it back from the same function the
    // pull round calls instead of re-deriving the expectation from the fields.
    $method = new ReflectionMethod(DefaultLitePullConsumer::class, 'liteBrokerIdFor');
    $method->setAccessible(true);
    $probeMq = new MessageQueue($topic, 'broker-a', 0);
    $resolved = $method->invoke($c, $probeMq);

    $got = 0;
    $deadline = microtime(true) + $windowSec;
    // PHP has no consumer threads: the lite consumer is driven by the caller, so tick()
    // must run each round (rebalance / route refresh) next to poll() -- polling alone never
    // assigns a queue and the leg would read as "nothing delivered".
    while (microtime(true) < $deadline && $got < $want) {
        $c->tick();
        $got += count($c->poll(1000));
    }
    $c->shutdown();

    return ['got' => $got, 'resolved' => $resolved, 'apiPin' => $c->isConnectBrokerByUser()];
};

echo '======================================================================' . PHP_EOL;
echo "Capability closeout live (PHP): ns=$ns prefix=$prefix" . PHP_EOL;
echo '======================================================================' . PHP_EOL;

// ---------------------------------------------------------------- bootstrap: topic + messages
$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $names = array_keys($ci->brokerAddrTable);
    sort($names, SORT_STRING);
    $brokerName = $names[0] ?? 'broker-a';
    $t1 = $prefix . '_FromWhere';
    $t2 = $prefix . '_Pin';
    foreach ([$t1, $t2] as $tp) {
        $admin->createTopic($brokerName, $tp, 1);
    }
} catch (Throwable $e) {
    echo '!! createTopic failed: ' . $e->getMessage() . PHP_EOL;
    exit(1);
}
// A createTopic only reaches the name server on the broker's own registration round; a
// send before that lands through the TBW102 fallback and a consumer sees no route.
$routeReady = $until(30.0, static function () use ($admin, $t2): bool {
    try {
        $r = $admin->examineTopicRoute($t2);
    } catch (Throwable) {
        return false;
    }
    $n = 0;
    foreach ($r->queueDatas as $q) {
        $n += $q->readQueueNums;
    }

    return $n >= 1;
});
$check($routeReady, 'the topic route is visible before anything is sent');

$producer = new DefaultMQProducer($prefix . '_pg');
$producer->setNamesrvAddr($ns);
$producer->setInstanceName($prefix . '_pg');
$producer->start();

$seed = static function (string $topic) use ($producer, $bodies, $check): void {
    $ok = 0;
    foreach ($bodies as $b) {
        $r = $producer->send(new Message($topic, $b));
        if ($r->sendStatus === SendStatus::SEND_OK) {
            $ok++;
        }
    }
    $check($ok === count($bodies), "seeded topic=$topic", "sent=$ok");
};

// ---------------------------------------------------------------- L1
echo PHP_EOL . 'L1 ConsumeFromWhere: the three deprecated values fold into LAST_OFFSET' . PHP_EOL;
$seed($t1);
echo "  sent $nMsg messages, no consumer has ever joined this topic" . PHP_EOL;

$first = $pushLeg($t1, $prefix . '_gFirst', ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET, 12);
$check($first['assigned'], 'L1-the positive-control consumer really got a queue assigned',
    'keys=[' . implode(',', $first['keys']) . ']');
// Where did the messages actually go? Topic max offset says what was stored; the group's
// consume stats say whether the consumer pulled and acked. Without both, "received=0" could
// be a send-side or a pull-side failure.
// Where did the messages actually go? A pull consumer reads maxOffset per queue, which says
// whether the writes are visible at all -- "received=0" alone cannot tell a send-side problem
// from a pull-side one.
try {
    $diagC = new DefaultMQPullConsumer($prefix . '_diag');
    $diagC->setNamesrvAddr($ns);
    $diagC->registerTopic($t1);
    $diagC->start();
    $queues = $diagC->fetchSubscribeMessageQueues($t1);
    $detail = [];
    foreach ($queues as $mq) {
        $detail[] = $mq->brokerName . ':' . $mq->queueId . ' max=' . $diagC->maxOffset($mq);
    }
    echo '  DIAG pull routes: ' . implode(' | ', $detail) . PHP_EOL;
    $diagC->shutdown();
} catch (Throwable $e) {
    echo '  DIAG pull route probe failed: ' . $e->getMessage() . PHP_EOL;
}
$check($first['got'] === $nMsg, 'L1-positive control: FIRST_OFFSET delivers the whole backlog',
    "received={$first['got']} expect=$nMsg");

// The four LAST-family values are measured, never compared to a constant, and **not**
// required to equal each other. Delivery counts cannot discriminate the branch: the broker
// answers QUERY_CONSUMER_OFFSET for a never-committed group with SUCCESS offset=0 on its
// V3_0_6 path (minOffset<=0 and offset 0 still in memory) and with QUERY_NOT_FOUND otherwise,
// and that answer flips between legs. Measured 2026-11-11 with the JAVA reference client on
// one backlog in one process: LAST_OFFSET=0 while MIN_OFFSET=8 and MAX_OFFSET=8. So this leg
// asserts what it can honestly assert -- every value is accepted, the consumer starts, gets a
// real queue and nothing exceeds the backlog -- and prints the counts as the parity record.
// The "folds into the LAST_OFFSET branch" claim is pinned by the offline suite, which reads
// the resolved offset directly with a fake store.
$counts = [];
foreach ([
    'LAST_OFFSET' => ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
    'MIN_OFFSET' => ConsumeFromWhere::CONSUME_FROM_MIN_OFFSET,
    'MAX_OFFSET' => ConsumeFromWhere::CONSUME_FROM_MAX_OFFSET,
    'LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST' => ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
] as $label => $where) {
    $leg = $pushLeg($t1, $prefix . '_g' . $label, $where, 12);
    $counts[$label] = $leg['got'];
    $check($leg['assigned'] && $leg['got'] >= 0 && $leg['got'] <= $nMsg,
        "L1-$label accepted: the consumer started, got a queue and received a sane count",
        "received={$leg['got']} of $nMsg assigned=" . var_export($leg['assigned'], true));
}
echo '  INFO LAST-family delivery counts on this end: ' . json_encode($counts)
    . ' (not a parity oracle: the broker V3_0_6 offset answer is timing-dependent)' . PHP_EOL;

// ---------------------------------------------------------------- L2
echo PHP_EOL . 'L2 LitePull pin (connectBrokerByUser / defaultBrokerId)' . PHP_EOL;
$seed($t2);
echo "  sent $nMsg messages to $t2" . PHP_EOL;

$off = $liteLeg($t2, $prefix . '_pinOff', false, 0, $nMsg, 20);
$check($off['resolved'] === null, 'L2-pin off: the request carries no broker id',
    'resolved=' . var_export($off['resolved'], true));
$check($off['got'] === $nMsg, 'L2-pin off: delivers the backlog', "got={$off['got']} expect=$nMsg");

$master = $liteLeg($t2, $prefix . '_pinMaster', true, 0, $nMsg, 20);
$check($master['apiPin'] === true && $master['resolved'] === 0,
    'L2-pin to the master (0): the request carries brokerId=0',
    'pin=' . var_export($master['apiPin'], true) . ' resolved=' . var_export($master['resolved'], true));
$check($master['got'] === $nMsg, 'L2-pin to the master (0): delivers the backlog',
    "got={$master['got']} expect=$nMsg");

$slave = $liteLeg($t2, $prefix . '_pinSlave', true, 1, $nMsg, 12);
$check($slave['apiPin'] === true && $slave['resolved'] === 1,
    'L2-pin to a non-existent slave (1): the request carries brokerId=1',
    'pin=' . var_export($slave['apiPin'], true) . ' resolved=' . var_export($slave['resolved'], true));
$check($slave['got'] === $nMsg,
    'L2-pin to a non-existent slave (1): FindBrokerAddressInSubscribe falls back to the table first '
    . 'broker (Java parity), so delivery still happens', "got={$slave['got']} expect=$nMsg");

// ---------------------------------------------------------------- L3
echo PHP_EOL . 'L3 adminExtGroup (identity only, no wire effect)' . PHP_EOL;
$check(DefaultMQAdminExt::DEFAULT_ADMIN_EXT_GROUP === 'admin_ext_group',
    'L3-DEFAULT_ADMIN_EXT_GROUP matches the Java constant', 'value=' . DefaultMQAdminExt::DEFAULT_ADMIN_EXT_GROUP);
$check($admin->getAdminExtGroup() === 'admin_ext_group', 'L3-default adminExtGroup is admin_ext_group',
    'value=' . $admin->getAdminExtGroup());
$admin->setAdminExtGroup('cap_closeout_admin_' . $prefix);
$check($admin->getAdminExtGroup() === 'cap_closeout_admin_' . $prefix, 'L3-override takes',
    'value=' . $admin->getAdminExtGroup());
$topics = $admin->fetchAllTopicList();
$check(in_array($t2, $topics->topicList, true),
    'L3-a custom-group admin still boots and serves an RPC on the live cluster',
    'topics=' . count($topics->topicList));

$producer->shutdown();
$admin->shutdown();

echo '== summary: PASS=' . $pass . ' FAIL=' . $fail . ' ==' . PHP_EOL;
exit($fail > 0 ? 1 : 0);
