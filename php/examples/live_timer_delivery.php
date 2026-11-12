<?php

declare(strict_types=1);

// Live timer-message delivery verification (php) — re-check of the user's
// sequential-run scenario with a WIDENED observation window.
//
//   php examples/live_timer_delivery.php [namesrv]
//
// Background: with an orderly consumer polling 8 queues, one pull round can
// take ~105s (long-poll suspend), and the earlier verification closed its
// window before a single round finished, so a timer message that matured
// mid-round looked like "the timer never delivers". This probe keeps the
// window open well past 105s and pins three things:
//
//   T1  a plain message on the same 8-queue orderly topic is delivered
//       (consumer liveness + round cadence measurement);
//   T2  a timer message (TIMER_DELIVER_MS = now + 5s) is delivered too —
//       maturation during an orderly pull round must not lose it;
//   T3  the delivery time is not earlier than the requested deliverAt
//       (timer semantics respected), with the observed latency echoed.
//
// The broker must run with timerEnable=true (run_trace_live.sh sets it).
// Closing line `PASS=<n> FAIL=<n>`; exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeOrderlyContext;
use RocketMQ\Client\ConsumeOrderlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerOrderly;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;

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
$stamp = time();
$topic = 'PhpTimerWide' . $stamp;
const QUEUE_COUNT = 8;
// One orderly pull round over 8 queues measured ~105s on the live cluster; the
// window must be comfortably wider than that to rule out "just not delivered yet".
const OBSERVE_SECONDS = 240;

final class OrderlyBox
{
    /** @var array<string, int> body -> first-delivered wall-clock seconds */
    public array $deliveredAt = [];

    public function add(MessageExt $m): void
    {
        $body = (string) $m->getBody();
        if (!isset($this->deliveredAt[$body])) {
            $this->deliveredAt[$body] = time();
        }
    }
}

final class WideOrderlyListener implements MessageListenerOrderly
{
    public function __construct(private readonly OrderlyBox $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeOrderlyContext $ctx): ConsumeOrderlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        return ConsumeOrderlyStatus::SUCCESS;
    }
}

/** Caller-driven wait loop: ticks the consumer while polling the predicate. */
function timer_wait_until(float $windowSec, callable $cond, object $consumer): bool
{
    $deadline = microtime(true) + $windowSec;
    for (;;) {
        if (method_exists($consumer, 'tick')) {
            $consumer->tick();
        }
        if ($cond()) {
            return true;
        }
        if (microtime(true) >= $deadline) {
            return $cond();
        }
        usleep(200_000);
    }
}

// ---------------------------------------------------------------- setup

$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $brokerNames = array_keys($ci->brokerAddrTable);
    sort($brokerNames, SORT_STRING);
    $brokerName = $brokerNames[0] ?? 'broker-a';
    $admin->createTopic($brokerName, $topic, QUEUE_COUNT);
    echo "topic $topic created with " . QUEUE_COUNT . " queues on $brokerName" . PHP_EOL;
} finally {
    $admin->shutdown();
}

$box = new OrderlyBox();
$consumer = new DefaultMQPushConsumer('GID_' . $topic . '_rd');
$consumer->setNamesrvAddr($ns);
$consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
$consumer->subscribe($topic, '*');
$consumer->setMessageListener(new WideOrderlyListener($box));
$consumer->start();
$assigned = timer_wait_until(30.0, fn (): bool => $consumer->assignedQueueCountForTopic($topic) >= QUEUE_COUNT, $consumer);
$check($assigned, 'T0 the orderly consumer was assigned all ' . QUEUE_COUNT . ' queues',
    'assigned=' . $consumer->assignedQueueCountForTopic($topic));

$producer = new DefaultMQProducer('GID_' . $topic . 'prod');
$producer->setNamesrvAddr($ns);
$producer->start();

// ---------------------------------------------------------------- T1: plain message (liveness + cadence)

$t1SentAt = time();
$plainBody = "plain-$stamp";
$plainOk = false;
try {
    $r = $producer->send(new Message($topic, $plainBody));
    $plainOk = $r->sendStatus === SendStatus::SEND_OK;
} catch (Throwable $e) {
    echo '  plain send error: ' . $e->getMessage() . PHP_EOL;
}
$check($plainOk, 'T1 the plain message was sent with SEND_OK', 'status=' . ($plainOk ? 'SEND_OK' : 'error'));
$plainSeen = timer_wait_until(150.0, fn (): bool => isset($box->deliveredAt[$plainBody]), $consumer);
$check($plainSeen, 'T1 the plain message was delivered (consumer round cadence proven)',
    $plainSeen ? sprintf('latency=%ds', $box->deliveredAt[$plainBody] - $t1SentAt) : 'not delivered in 150s');

// ---------------------------------------------------------------- T2/T3: timer message, window > 105s

$deliverAtMs = (time() + 5) * 1000;
$timerBody = "timer-$stamp";
$timerMsg = new Message($topic, $timerBody);
$timerMsg->putProperty(MessageConst::PROPERTY_TIMER_DELIVER_MS, (string) $deliverAtMs);
$t2SentAt = time();
$timerOk = false;
try {
    $r = $producer->send($timerMsg);
    $timerOk = $r->sendStatus === SendStatus::SEND_OK;
} catch (Throwable $e) {
    echo '  timer send error: ' . $e->getMessage() . PHP_EOL;
}
$check($timerOk, 'T2 the timer message (deliverAt=now+5s) was sent with SEND_OK', 'status=' . ($timerOk ? 'SEND_OK' : 'error'));

echo '  observing up to ' . OBSERVE_SECONDS . "s (one orderly round measured ~105s)..." . PHP_EOL;
$timerSeen = timer_wait_until((float) OBSERVE_SECONDS, fn (): bool => isset($box->deliveredAt[$timerBody]), $consumer);
$check($timerSeen, 'T2 the timer message was delivered inside the >105s window',
    $timerSeen ? sprintf('latency=%ds (window=%ds)', $box->deliveredAt[$timerBody] - $t2SentAt, OBSERVE_SECONDS) : 'not delivered');

if ($timerSeen) {
    $deliveredSec = $box->deliveredAt[$timerBody];
    // 2s slack for client/broker clock skew: the semantics we pin is "no early delivery",
    // not millisecond precision across two clocks.
    $check($deliveredSec >= (int) floor($deliverAtMs / 1000) - 2, 'T3 the delivery respected the requested deliver time (no early delivery)',
        sprintf('deliverAt=%d deliveredAt=%d', (int) floor($deliverAtMs / 1000), $deliveredSec));
} else {
    $check(false, 'T3 the delivery respected the requested deliver time (no early delivery)', 'timer never delivered');
}

$consumer->shutdown();
$producer->shutdown();

echo "############ PASS=$pass FAIL=$fail ############" . PHP_EOL;
exit($fail === 0 ? 0 : 1);
