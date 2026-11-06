<?php

declare(strict_types=1);

// Live PullConsumerScheduleService verification (PHP): the facade expands one task
// per queue in its table, the task body pulls through the context, and the
// broker-side committed offset for every queue advances past the seed batch.
//
//   php examples/live_pull_schedule.php [namesrv] [seed]
//
// This port's facade is caller-driven ("driven by the caller's loop" in the
// PullSchedule.php header): no scheduler thread, so the live entry owns the tick
// loop and drives tick() every 200 ms exactly like a resident integration would.
//
// Closing line `PASS=<n> FAIL=<n>`; exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\MQPullConsumerScheduleService;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\PullTaskCallback;
use RocketMQ\Client\PullTaskContext;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\UtilAll;

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

// PHP is caller-driven: the schedule service has no background thread, so every
// wait window must actively tick() it (task discovery + pull rounds + offset
// persistence all happen inside tick()).
$waitUntil = function (callable $pred, int $timeoutMillis, int $intervalMillis = 200) use (&$svc): bool {
    $deadline = UtilAll::currentTimeMillis() + $timeoutMillis;
    while (UtilAll::currentTimeMillis() < $deadline) {
        if ($svc !== null) {
            $svc->tick();
        }
        if ($pred()) {
            return true;
        }
        usleep($intervalMillis * 1000);
    }
    return $pred();
};

$ns = $argv[1] ?? '127.0.0.1:9876';
// The shared run_php_live.sh runner passes its legs argument here ("all"); treat any
// non-numeric value as "use the default seed count".
$nMsg = (int) ($argv[2] ?? 6);
if ($nMsg <= 0) {
    $nMsg = 6;
}
$queueNum = 2;
$stamp = time();
$topic = 'PhpPullScheduleLive_' . $stamp;
$group = 'PG_PhpPullSchedule_' . $stamp;

echo "======================================================================" . PHP_EOL;
echo "PullConsumerScheduleService live (PHP): ns=$ns topic=$topic group=$group seed=$nMsg" . PHP_EOL;
echo "======================================================================" . PHP_EOL;

$brokerAddr = null;
$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();

$producer = null;
$svc = null;
$consumer = null;
try {
    // ------------------------------------------------ S1 cluster + topic + producer
    $ci = $admin->fetchBrokerClusterInfo();
    $brokerName = array_key_first($ci->brokerAddrTable);
    if ($brokerName === null) {
        $check(false, 'S1 cluster probe', 'no broker registered with the name server');
        throw new RuntimeException('no broker');
    }
    // brokerAddrTable[name] is already the flat {brokerId: addr} map
    // (sorted ascending: brokerId 0, the master, comes first).
    $addrs = $ci->brokerAddrTable[$brokerName] ?? [];
    $brokerAddr = ($addrs !== []) ? (string) reset($addrs) : null;
    if ($brokerAddr === null || $brokerAddr === '') {
        $check(false, 'S1 cluster probe', "broker=$brokerName has no address");
        throw new RuntimeException('no broker address');
    }
    $check(true, 'S1 cluster probe', "broker=$brokerName addr=$brokerAddr");

    $admin->createTopic($brokerName, $topic, $queueNum);
    $check(true, 'S1 topic created', "$topic x$queueNum");

    $producer = new DefaultMQProducer('PG_PhpPullScheduleSend_' . $stamp);
    $producer->setNamesrvAddr($ns);
    $producer->start();
    for ($i = 0; $i < $nMsg; ++$i) {
        $producer->send(new Message($topic, ('php-schedule-' . $i)));
    }
    $check(true, 'S1 producer delivered the seed batch', "$nMsg messages");

    // ------------------------------------------------ S2/S3 schedule + tick loop
    $received = [];
    $seenQueues = [];
    $bodyCallback = new class($received, $seenQueues) implements PullTaskCallback {
        /** @var list<string> */
        public array $received;

        /** @var array<string, true> */
        public array $seenQueues;

        public function __construct(array &$received, array &$seenQueues)
        {
            $this->received = &$received;
            $this->seenQueues = &$seenQueues;
        }

        public function doPullTask(MessageQueue $mq, PullTaskContext $context): void
        {
            $result = $context->pullNext(32);
            if ($result !== null && $result->status === PullStatus::FOUND) {
                $this->seenQueues[$mq->brokerName . ':' . $mq->queueId] = true;
                foreach ($result->msgFoundList as $m) {
                    $this->received[] = (string) $m->getBody();
                }
                // The pull model is caller-driven: the loop never commits, the callback
                // advances the broker-side cursor (this is what S4 asserts).
                $context->getPullConsumer()->updateConsumeOffset($mq, $result->nextBeginOffset);
            }
            // default resident interval (no explicit delay)
        }
    };

    $svc = new MQPullConsumerScheduleService($group);
    $svc->getDefaultMQPullConsumer()->setNamesrvAddr($ns);
    $svc->registerPullTaskCallback($topic, $bodyCallback);
    $svc->start();
    $check(true, 'S2 schedule service started', "group=$group");

    $tasksOk = $waitUntil(fn (): bool => $svc->taskCount() >= $queueNum, 20000);
    $check($tasksOk, 'S2 one PullTask per queue expanded in the task table',
        'tasks=' . $svc->taskCount() . " (want >=$queueNum)");

    $delivered = $waitUntil(fn (): bool => count($bodyCallback->received) >= $nMsg, 40000);
    $check($delivered, 'S3 tick loop delivered every seed body',
        'collected=' . count($bodyCallback->received) . " (want $nMsg)");
    $check(count($bodyCallback->seenQueues) >= 1, 'S3 pulls landed on the assigned queues',
        'queues=' . count($bodyCallback->seenQueues));

    // ------------------------------------------------ S4 broker-side offsets
    $offsetsOk = true;
    $offsetParts = [];
    foreach ($bodyCallback->seenQueues as $key => $_) {
        [$brokerKey, $qid] = explode(':', $key);
        $mq = new MessageQueue($topic, $brokerKey, (int) $qid);
        $off = $svc->getDefaultMQPullConsumer()->fetchConsumeOffset($mq);
        $offsetsOk = $offsetsOk && $off !== null && $off >= 1;
        $offsetParts[] = 'q' . $qid . '=' . ($off ?? 'none');
    }
    $check($offsetsOk, 'S4 broker-side committed offset exists on every pulled queue',
        implode(' ', $offsetParts));

    // ------------------------------------------------ S5 clean shutdown
    $svc->shutdown();
    $svc = null;
    $check(true, 'S5 shutdown clean', '');
} catch (Throwable $e) {
    $check(false, 'the verification threw an exception', get_class($e) . ': ' . $e->getMessage());
} finally {
    // ------------------------------------------------ cleanup
    if ($svc !== null) {
        try {
            $svc->shutdown();
        } catch (Throwable $e) {
            echo '    (schedule service shutdown failed: ' . $e->getMessage() . ')' . PHP_EOL;
        }
    }
    if ($producer !== null) {
        try {
            $producer->shutdown();
        } catch (Throwable $e) {
            echo '    (producer shutdown failed: ' . $e->getMessage() . ')' . PHP_EOL;
        }
    }
    try {
        $admin->deleteTopic($topic);
    } catch (Throwable $e) {
        echo '    (deleteTopic failed: ' . $e->getMessage() . ')' . PHP_EOL;
    }
    try {
        $admin->deleteSubscriptionGroup($brokerAddr ?? '', $group, true);
    } catch (Throwable $e) {
        echo '    (deleteSubscriptionGroup failed: ' . $e->getMessage() . ')' . PHP_EOL;
    }
    $admin->shutdown();
}

echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);
