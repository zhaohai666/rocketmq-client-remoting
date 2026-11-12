<?php

declare(strict_types=1);

// Live consume-side message trace probe (php).
//
//   php examples/live_trace_consume.php [namesrv]
//
// The Java client (ConsumeMessageTraceHookImpl + AsyncTraceDispatcher) emits SubBefore before
// every delivery and SubAfter after it; a RECONSUME_LATER delivery yields
// SubAfter(success=false, contextCode=4 == ConsumeReturnType.FAILED) and the %RETRY% redelivery
// yields SubBefore(retryTimes=1) + SubAfter(success=true, contextCode=0). This probe asserts the
// whole failure ladder on a real cluster (broker needs traceTopicEnable=true).
//
// PHP-specific: the dispatcher has no background thread; the flush is driven by the consumer's
// tick() (PushConsumer::tick step 9) and by shutdown(). So every wait window ticks the consumer —
// both to pull messages and to pump trace records out.
//
// Closing line `PASS=<n> FAIL=<n>`; the process exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\TraceContext;
use RocketMQ\Client\TraceDataEncoder;
use RocketMQ\Client\TraceType;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MixAll;
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
$topic = 'PhpTraceConsume' . $stamp;
$consumerGroup = 'GID_php_trace_' . $stamp;
$traceReaderGroup = 'GID_php_trace_reader_' . $stamp;

final class TraceDecodeListener implements MessageListenerConcurrently
{
    /** @var list<TraceContext> */
    public array $records = [];

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            try {
                foreach (TraceDataEncoder::decoderFromTraceDataString((string) $m->getBody()) as $c) {
                    $this->records[] = $c;
                }
            } catch (Throwable $e) {
                echo 'WARN decode trace failed: ' . $e->getMessage() . PHP_EOL;
            }
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

final class RetryOnceListener implements MessageListenerConcurrently
{
    /** @var list<array{body:string,recon:int}> */
    public array $deliveries = [];

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        $fail = false;
        foreach ($msgs as $m) {
            $this->deliveries[] = ['body' => (string) $m->getBody(), 'recon' => $m->reconsumeTimes];
            if ((string) $m->getBody() === $this->target && $m->reconsumeTimes === 0) {
                $fail = true;
            }
        }
        return $fail ? ConsumeConcurrentlyStatus::RECONSUME_LATER : ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    public function __construct(public readonly string $target)
    {
    }
}

function tc_wait_until(float $windowSec, callable $cond, object ...$tickables): bool
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

// ------------------------------------------------------------------ setup

$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $brokerNames = array_keys($ci->brokerAddrTable);
    sort($brokerNames, SORT_STRING);
    $admin->createTopic($brokerNames[0] ?? 'broker-a', $topic, 2);
    echo "topic $topic created on {$brokerNames[0]}" . PHP_EOL;
} finally {
    $admin->shutdown();
}

$traceCollector = new TraceDecodeListener();
$traceReader = new DefaultMQPushConsumer($traceReaderGroup);
$traceReader->setNamesrvAddr($ns);
$traceReader->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
$traceReader->subscribe(MixAll::TRACE_TOPIC, '*');
$traceReader->setMessageListener($traceCollector);
$traceReader->start();
echo "trace reader started group=$traceReaderGroup" . PHP_EOL;

$listener = new RetryOnceListener("fail-me-$stamp");
$consumer = new DefaultMQPushConsumer($consumerGroup);
$consumer->setNamesrvAddr($ns);
$consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
$consumer->subscribe($topic, '*');
$consumer->setMessageListener($listener);
$consumer->setEnableMsgTrace(true);
$consumer->start();

tc_wait_until(20.0, fn (): bool => $consumer->assignedQueueCount() >= 1, $consumer);
echo 'assigned=' . $consumer->assignedQueueCount() . PHP_EOL;

$producer = new DefaultMQProducer('GID_php_trace_prod_' . $stamp);
$producer->setNamesrvAddr($ns);
$producer->sendMsgTimeout = 5000;
$producer->start();
$result = $producer->send(new Message($topic, "fail-me-$stamp", 'TagA', 'KeyFail'));
echo 'sent msgId=' . $result->msgId . ' status=' . $result->sendStatus->name . PHP_EOL;
$uniq = $result->msgId;

// F1 first delivery + RECONSUME_LATER
$firstDelivered = tc_wait_until(
    25.0,
    fn (): bool => (bool) array_filter($listener->deliveries, static fn (array $d): bool => $d['body'] === "fail-me-$stamp"),
    $consumer
);
$check($firstDelivered, 'F1 the first delivery reached the listener',
    'deliveries=' . count($listener->deliveries));

$ofUniq = static fn (TraceContext $r): bool =>
    $r->traceBeans !== [] && $r->traceBeans[0]->msgId === $uniq;

// F2/F3: the failed SubAfter surfaces once the trace dispatcher's flush is pumped by tick()
$failedAfterSeen = tc_wait_until(25.0, static function () use ($traceCollector, $ofUniq): bool {
    foreach ($traceCollector->records as $r) {
        if ($r->traceType === TraceType::SUB_AFTER && $ofUniq($r) && !$r->isSuccess && $r->contextCode === 4) {
            return true;
        }
    }
    return false;
}, $consumer, $traceReader);

$befores = array_values(array_filter($traceCollector->records,
    static fn (TraceContext $r): bool => $r->traceType === TraceType::SUB_BEFORE && $ofUniq($r)));
$afters = array_values(array_filter($traceCollector->records,
    static fn (TraceContext $r): bool => $r->traceType === TraceType::SUB_AFTER && $ofUniq($r)));

$check((bool) array_filter($befores, static fn (TraceContext $r): bool => $r->traceBeans[0]->retryTimes === 0),
    'F2 SubBefore(retryTimes=0) for the failed message', 'before=' . count($befores));
$check($failedAfterSeen, 'F3 SubAfter(success=false, contextCode=4 FAILED)',
    'afters=' . json_encode(array_map(static fn (TraceContext $r): array =>
        [$r->isSuccess, $r->contextCode], $afters)));

// F4/F5/F6: the retry ladder (~10s broker delay) with success trace
$retried = tc_wait_until(
    60.0,
    fn (): bool => (bool) array_filter($listener->deliveries, static fn (array $d): bool => $d['recon'] >= 1),
    $consumer
);
// give the final SubAfter one flush window, then stop the consumer (shutdown also flushes)
$finalAfterSeen = tc_wait_until(25.0, static function () use ($traceCollector, $ofUniq): bool {
    foreach ($traceCollector->records as $r) {
        if ($r->traceType === TraceType::SUB_AFTER && $ofUniq($r) && $r->isSuccess && $r->contextCode === 0) {
            return true;
        }
    }
    return false;
}, $consumer, $traceReader);
$consumer->shutdown();
$producer->shutdown();
usleep(1_000_000);

$befores = array_values(array_filter($traceCollector->records,
    static fn (TraceContext $r): bool => $r->traceType === TraceType::SUB_BEFORE && $ofUniq($r)));
$afters = array_values(array_filter($traceCollector->records,
    static fn (TraceContext $r): bool => $r->traceType === TraceType::SUB_AFTER && $ofUniq($r)));

$check($retried, 'F4 the %RETRY% redelivery was consumed',
    'deliveries=' . json_encode($listener->deliveries));
$check((bool) array_filter($befores, static fn (TraceContext $r): bool => $r->traceBeans[0]->retryTimes === 1),
    'F5 SubBefore(retryTimes=1) for the same message id',
    'retryTimes=' . json_encode(array_map(static fn (TraceContext $r): int => $r->traceBeans[0]->retryTimes, $befores)));
$check($finalAfterSeen, 'F6 SubAfter(success=true, contextCode=0) for the retry',
    'afters=' . json_encode(array_map(static fn (TraceContext $r): array =>
        [$r->isSuccess, $r->contextCode], $afters)));

$reqIds = [];
foreach ($befores as $b) {
    $reqIds[$b->requestId] = true;
}
$allPaired = true;
foreach ($afters as $a) {
    if (!isset($reqIds[$a->requestId])) {
        $allPaired = false;
    }
}
$check($allPaired, 'F7 every SubAfter requestId was preceded by a SubBefore',
    'before=' . count($befores) . ' after=' . count($afters));

$traceReader->shutdown();

echo "############ PASS=$pass FAIL=$fail ############" . PHP_EOL;
exit($fail === 0 ? 0 : 1);
