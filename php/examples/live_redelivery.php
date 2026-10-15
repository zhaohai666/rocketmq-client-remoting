<?php

declare(strict_types=1);

// Live redelivery / dead-letter matrix (S1-S4, all angles).
//
//   php examples/live_redelivery.php [namesrv] [legs]
//   legs = all | s1,s2,s3,s4
//
// S1 REDELIVERY: the listener RECONSUME_LATERs only the first delivery of one message. The
//    broker redelivers via %RETRY%<group> at delay level 3 (10s) with RECONSUME_TIMES=1, and the
//    client restores the topic back to the business topic before the listener sees it. The other
//    message must be delivered exactly once.
// S2 DEAD-LETTER TERMINAL: maxReconsumeTimes=2 => the broker rejects the third redelivery
//    (`>=`) and moves it to %DLQ%<group> (reconsumeTimes=3); the client delivers exactly 3 times
//    (0/1/2). **Both halves must be asserted**: getting one wrong yields either "retry forever"
//    or "dead-letter too early", and neither is visible in unit tests.
// S3 ORDERLY POISON: the orderly side SUSPENDs forever, yielding exactly 3 local deliveries (the
//    client bumps reconsumeTimes itself), then hands the message to %RETRY% as a normal SEND;
//    the group still holds the lock => the broker goes straight to %DLQ%. The dead-letter
//    decision on this path is SendMessageProcessor's strict `>`, not the same code path as S2's
//    CONSUMER_SEND_MSG_BACK(36) using `>=` — testing only one cannot pin both.
// S4 PARTIAL ACK (ackIndex): of a batch of 3, only index 0 is acked => the trailing 2 redeliver
//    via %RETRY%, the acked one never redelivers, and the business queue offset is still
//    committed for the whole batch to 3. The control group (untouched ackIndex) redelivers
//    nothing — without the control, the redelivery cannot be blamed on the ack.
//
// PHP adaptation (single-threaded): every "background thread" converges into time-based checks
// inside tick(), so every wait window polls while **actively ticking the consumer**; a bare
// sleep is never acceptable (a frozen client = no redelivery pulled).
//
// Each scenario creates its topic explicitly first: the consumer has no default-topic fallback
// (TBW102 is only for producers); a missing route = no assignment = no consumption, which would
// masquerade as a client bug (the B5 invariant).
//
// Closing line `PASS=<n> FAIL=<n>`; the process exits non-zero on any FAIL.
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

/** One delivery = body / topic / wire reconsumeTimes / timestamp from the listener's point of view. */
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
                $fail = true; // flip only the first delivery; later deliveries (recon>=1) are all SUCCESS
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
        return ConsumeConcurrentlyStatus::RECONSUME_LATER; // the reconsumeTimes ladder climbs to the top => broker dead letter
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
        return ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT; // the client counts deliveries itself
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
                $retry = true; // accept the whole redelivered batch so the ladder can terminate
            }
            $this->box->add($m);
        }
        if (!$retry) {
            $ctx->ackIndex = 0; // first delivery, batch of 3: ack only up to index 0 (inclusive)
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
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS; // control group: accept everything
    }
}

// ------------------------------------------------------------------ helpers

/** Polls the condition within the window; ticks the consumers each round (PHP has no background threads), returning as soon as the condition holds. */
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

/** Observation window: tick only, no assertion — gives "things that must not happen again" time to surface. */
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

/** Picks the master broker name/addr from the cluster info (single-broker cluster; the sorted first). */
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

/** Creates the topic explicitly (single queue, making preconditions like "a batch of 3" deterministic). */
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

/** Send with retries: route/connection jitter is not a failure; only a full 30 x 2s budget is. */
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
 * Starts a push consumer and waits for assignment (FIRST_OFFSET: a new group reads from the
 * queue head, so S4's messages sent before startup must still be visible).
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

/** Waits for the broker to register the topic route (%DLQ% does not exist before the first message lands in it). */
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

/** Reads the whole topic from the head with a throwaway pull group — proves the message really landed in %DLQ%, not trusting offset pointers. */
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
                return false; // route not ready yet
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
        rd_observe(2.0, $c); // a single pull round may still have stragglers
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
            // Cleanup failures must not affect the result
        }
    }
};

// ------------------------------------------------------------------ S1
if ($want('s1')) {
    echo PHP_EOL . '--- S1 redelivery: %RETRY% second delivery + delay gradient + topic restoration ---' . PHP_EOL;
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

        // Redelivery uses delay level 3 = 10s, and the %RETRY% route only becomes visible after
        // the broker's next registration (<=30s) plus a round of rebalance — the window must
        // cover both.
        rd_wait_until(60.0, fn() => count($b->ofBody('retry-me')) >= 2, $c);

        $retry = $b->ofBody('retry-me');
        $normal = $b->ofBody('normal-1');
        $check(count($retry) >= 2, 'S1 retry-me delivered multiple times (>=2)', 'arrivals=' . count($retry));
        if (count($retry) >= 2) {
            $gap = $retry[count($retry) - 1]['at'] - $retry[0]['at'];
            $check($gap >= 8.0, 'S1 redelivery has a delay gradient (level3≈10s, >=8s)', sprintf('gap=%.1fs', $gap));
        } else {
            $check(false, 'S1 redelivery has a delay gradient (level3≈10s, >=8s)', 'fewer than two deliveries');
        }
        $redelivered = array_values(array_filter($retry, static fn(array $r): bool => $r['recon'] >= 1));
        $check(count($redelivered) >= 1, 'S1 second delivery carries RECONSUME_TIMES>=1', 'times=' . rd_recon_list($retry));
        $topicOk = count($redelivered) >= 1;
        foreach ($redelivered as $r) {
            if ($r['topic'] !== $topic) {
                $topicOk = false; // what the listener sees must be the business topic (the client restored it)
            }
        }
        $check($topicOk, 'S1 redelivered message topic restored to the business topic', 'listener topic distribution=' . $b->topicSummary());
        $check(count($normal) === 1, 'S1 the normal message is delivered exactly once', 'arrivals=' . count($normal));
    } catch (Throwable $e) {
        $check(false, 'S1 aborted by exception', get_class($e) . ': ' . $e->getMessage());
    }
}

// ------------------------------------------------------------------ S2
if ($want('s2')) {
    echo PHP_EOL . '--- S2 dead-letter terminal: maxReconsumeTimes=2 => enters %DLQ% after 3 deliveries ---' . PHP_EOL;
    $topic = $prefix . '_Dlq';
    $createdTopics[] = $topic;
    $group = 'GID_' . $prefix . '_s2';
    $dlqTopic = MixAll::getDlqTopic($group);
    try {
        rd_ensure_topic($ns, $topic, 1);
        $p = rd_new_producer($ns, 'GID_' . $prefix . '_s2_prod');
        $shutdowns[] = static fn() => $p->shutdown();

        $b = new RecBox();
        // maxReconsumeTimes must be set before start (subscription/config is reported to the broker at start).
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

        // Delay levels 3 (10s) + 4 (30s) mean the third delivery lands ~40s after the first;
        // contention on shared machines pushes it later still — the window is generous and the
        // timeline is printed regardless.
        rd_wait_until(150.0, fn() => count($b->ofBody('dlq-me')) >= 3, $c);
        rd_observe(15.0, $c); // counter-evidence window: a 4th delivery must not appear
        $all = $b->ofBody('dlq-me');
        $times = rd_recon_list($all);
        $seqOk = count($all) >= 3 && $all[0]['recon'] === 0 && $all[1]['recon'] === 1 && $all[2]['recon'] === 2;
        $check($seqOk, 'S2 delivered exactly 3 times with RECONSUME_TIMES 0/1/2', 'times=' . $times);
        $check(count($all) === 3, 'S2 no delivery after exhaustion (only 3 within the observation window)',
            'arrivals=' . count($all) . ' times=' . $times);
        $topicOk = count($all) >= 3;
        foreach (array_slice($all, 0, 3) as $r) {
            if ($r['topic'] !== $topic) {
                $topicOk = false;
            }
        }
        $check($topicOk, 'S2 topic restored to the business topic during redelivery', 'listener topic distribution=' . $b->topicSummary());
        $c->shutdown();

        // The broker creates and registers %DLQ%<group> only when the first message lands in it.
        $check(rd_wait_route($ns, $dlqTopic, 30.0), 'S2 the broker auto-creates and registers the %DLQ% route', 'dlq=' . $dlqTopic);

        $probe = rd_read_from_head($ns, $dlqTopic, 'GID_' . $prefix . '_s2probe', 25.0);
        $got = $probe->ofBody('dlq-me');
        $check(count($got) >= 1, 'S2 message landed in %DLQ%<group>', 'got=' . rd_rec_summary($got));
        // The broker stores reconsumeTimes+1 at dead-letter time, so 2 -> 3.
        $check(count($got) === 1 && $got[0]['recon'] === 3,
            'S2 DLQ message RECONSUME_TIMES=3 (the 3rd redelivery became the dead letter)', 'got=' . rd_rec_summary($got));
        $check(count($got) === 1 && $got[0]['topic'] === $dlqTopic,
            'S2 DLQ message topic is %DLQ%<group>', 'probe topic distribution=' . $probe->topicSummary());
    } catch (Throwable $e) {
        $check(false, 'S2 aborted by exception', get_class($e) . ': ' . $e->getMessage());
    }
}

// ------------------------------------------------------------------ S3
if ($want('s3')) {
    echo PHP_EOL . '--- S3 orderly poison: Suspend to the local cap -> orderly send-back -> %DLQ% ---' . PHP_EOL;
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
        $c->suspendCurrentQueueTimeMillis = 50; // keep the suspension short so the ladder climbs fast
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
        $check($seqOk, 'S3 the orderly side delivers locally exactly 3 times (0/1/2)', 'times=' . $times);
        // After the third, the message is handed to the broker and the queue offset advances — a
        // 4th delivery would mean the cap did not take effect.
        $check(count($all) === 3, 'S3 no delivery after handing to the broker (only 3 within the observation window)',
            'arrivals=' . count($all) . ' times=' . $times);
        $c->shutdown();

        $check(rd_wait_route($ns, $dlqTopic, 30.0), 'S3 the broker auto-creates and registers the %DLQ% route', 'dlq=' . $dlqTopic);
        $probe = rd_read_from_head($ns, $dlqTopic, 'GID_' . $prefix . '_s3probe', 25.0);
        $got = $probe->ofBody('orderly-poison');
        $check(count($got) >= 1, 'S3 the orderly send-back message landed in %DLQ% (the group still holds the queue lock)', 'got=' . rd_rec_summary($got));
        // The orderly send-back writes msg.getReconsumeTimes()+1 into RECONSUME_TIME (a client-side
        // +1; the concurrent path's +1 comes from the broker — see S2). Without this +1 everything
        // else would look right.
        $check(count($got) === 1 && $got[0]['recon'] === 3,
            'S3 DLQ message RECONSUME_TIMES=3 (client-side +1)', 'got=' . rd_rec_summary($got));
        $check(count($got) === 1 && $got[0]['topic'] === $dlqTopic,
            'S3 DLQ message topic is %DLQ%<group>', 'probe topic distribution=' . $probe->topicSummary());
    } catch (Throwable $e) {
        $check(false, 'S3 aborted by exception', get_class($e) . ': ' . $e->getMessage());
    }
}

// ------------------------------------------------------------------ S4
if ($want('s4')) {
    echo PHP_EOL . '--- S4 partial ack (ackIndex=0): the trailing 2 redeliver, the acked one does not ---' . PHP_EOL;
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
        // Sent before the consumer starts: the first pull on a single-queue topic brings back all
        // 3 and forms one batch — the precondition of the whole scenario.
        foreach ([$topic, $ctrlTopic] as $t) {
            foreach ($bodies as $body) {
                rd_send($p, $t, $body);
            }
        }

        $b = new RecBox();
        $c = new DefaultMQPushConsumer($group);
        $c->setNamesrvAddr($ns);
        $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $c->consumeMessageBatchMaxSize = count($bodies); // all 3 must arrive as one batch
        $c->setMessageListener(new PartialAckTo0Listener($b));
        $c->subscribe($topic, '*');
        $c->start();
        $shutdowns[] = static fn() => $c->shutdown();

        // The un-acked tail redelivers at delay level 3 = 10s.
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
        $check(count($first) === 1, 'S4 the acked first message is delivered exactly once across the whole window', 'arrivals=' . count($first));
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
        $check($redelivered === 2, 'S4 the un-acked trailing 2 redelivered via %RETRY%', implode(' ', $tailDetail));
        $check(count($b->distinctBodies()) === count($bodies), 'S4 all 3 eventually fully consumed',
            'bodies=' . implode(',', $b->distinctBodies()));

        // The offset is still committed for the whole batch (ackIndex behaves the same): the tail
        // was handed to the broker via send-back, so the offset may advance to max+1 = 3 (the
        // "acked batch" semantics of the C4 floor).
        $a = rd_new_admin($ns);
        try {
            $route = $a->examineTopicRoute($topic);
            $bn = $route->brokerDatas[0]->brokerName ?? 'broker-a';
            $mq0 = new MessageQueue($topic, (string) $bn, 0);
            $off = $a->examineConsumerOffset($group, $mq0);
            $check($off === 3, 'S4 the business queue offset is still committed for the whole batch to 3', 'offset=' . var_export($off, true));
        } finally {
            $a->shutdown();
        }

        // Control group: the same 3 messages with untouched ackIndex => nothing redelivers.
        // Without it, the redelivery above could be blamed on the scaffolding instead of the ack.
        $cb = new RecBox();
        $cc = new DefaultMQPushConsumer($ctrlGroup);
        $cc->setNamesrvAddr($ns);
        $cc->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $cc->consumeMessageBatchMaxSize = count($bodies);
        $cc->setMessageListener(new CollectListener($cb));
        $cc->subscribe($ctrlTopic, '*');
        $cc->start();
        rd_wait_until(30.0, fn() => count($cb->distinctBodies()) === count($bodies), $cc);
        rd_observe(12.0, $cc); // a redelivery takes level 3 = 10s to become visible
        $cc->shutdown();
        $ctrlOk = count($cb->distinctBodies()) === count($bodies) && count($cb->recs) === count($bodies);
        $check($ctrlOk, 'S4 control group (untouched ackIndex) redelivers nothing',
            'deliveries=' . count($cb->recs) . ' bodies=' . implode(',', $cb->distinctBodies()));
    } catch (Throwable $e) {
        $check(false, 'S4 aborted by exception', get_class($e) . ': ' . $e->getMessage());
    }
}

$cleanup();

echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);
