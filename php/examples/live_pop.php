<?php

declare(strict_types=1);

// Live POP consumption verification (S1/S2, all angles).
//
//   php examples/live_pop.php [namesrv] [legs]     # legs = all | s1,s2
//
// POP is the only consumption path where failure is fully silent: the client has no offset to
// read back, no send-back to observe, and no error code to assert — the broker hands out a batch
// of "invisible" messages and the client either ACKs or requests a longer invisible time. With
// one segment wrong in POP_CK, every ACK is a no-op on the broker while the client reports
// success all along; the only symptom is the message coming back after popInvisibleTime. So the
// only valid criterion is a **timed observation window + broker-side visible state**, never a
// return code:
//
//   S1 ACK really works: after consuming 6 messages, keep watching for 2.5x the invisible
//      window and assert that no body is delivered a second time. If the ACK uses the wrong
//      segment (taking the batch start 0 as the message's own offset) or the wrong topic, the
//      broker revives the whole batch and redelivers within the window. The same leg pins "POP
//      holds no client offset table".
//   S2 failure backoff: RECONSUME_LATER only for one message's first delivery ->
//      changePopInvisibleTime extends the window -> revive moves it into %RETRY%<group>_<topic>
//      -> when redelivered, POP_CK's retry marker must be "1" (both evidence of "came from the
//      retry topic" and the reason "the follow-up ACK must be sent back to the retry topic"),
//      and 1ST_POP_TIME keeps the broker's value unmodified.
//
// Hard cluster prerequisites (POP-specific broker.conf, see the pop branch of
// scripts/run_php_live.sh):
//   timerWheelEnable=true / defaultMessageRequestMode=PULL (the PHP-side 401 is sent explicitly
//   via Admin by this tool) / popResponseReturnActualRetryTopic=false / enablePopBatchAck=false.
//
// Closing line `PASS=<n> FAIL=<n>`; exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;

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
$prefix = 'PhpPop' . $stamp;

// ------------------------------------------------------------------ accounting

/** One delivery = body / topic / recon / POP_CK / 1ST_POP_TIME / timestamp from the listener's point of view. */
final class PopBox
{
    /** @var list<array{body:string,topic:string,recon:int,ck:string,firstPop:string,at:float}> */
    public array $recs = [];

    public function add(Message $m): void
    {
        $this->recs[] = [
            'body' => (string) $m->getBody(),
            'topic' => (string) $m->getTopic(),
            'recon' => $m instanceof MessageExt ? $m->reconsumeTimes : 0,
            'ck' => (string) ($m->getUserProperty(MessageConst::PROPERTY_POP_CK) ?? ''),
            'firstPop' => (string) ($m->getUserProperty(MessageConst::PROPERTY_FIRST_POP_TIME) ?? ''),
            'at' => microtime(true),
        ];
    }

    /** @return list<array{body:string,topic:string,recon:int,ck:string,firstPop:string,at:float}> */
    public function ofBody(string $body): array
    {
        return array_values(array_filter($this->recs, static fn(array $r): bool => $r['body'] === $body));
    }

    /** @return array<string,int> body -> delivery count (accounted per body, C3) */
    public function summary(): array
    {
        $out = [];
        foreach ($this->recs as $r) {
            $out[$r['body']] = ($out[$r['body']] ?? 0) + 1;
        }
        ksort($out, SORT_STRING);
        return $out;
    }

    public function noDupes(): bool
    {
        return max([0, ...array_values($this->summary())]) <= 1;
    }

    public function dupes(): string
    {
        $out = [];
        foreach ($this->summary() as $body => $n) {
            if ($n > 1) {
                $out[] = "$body=$n";
            }
        }
        return implode(' ', $out);
    }
}

final class PopListener implements MessageListenerConcurrently
{
    public function __construct(private readonly PopBox $box, private readonly string $poison = '')
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        if ($this->poison === '') {
            return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
        }
        foreach ($msgs as $m) {
            if ($m->getBody() === $this->poison) {
                // Flip only the first delivery; the failure path uses changePopInvisibleTime
                // (POP semantics), not PULL's CONSUMER_SEND_MSG_BACK.
                return ConsumeConcurrentlyStatus::RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

// ------------------------------------------------------------------ helpers

/** Polls the condition within the window; ticks the consumers each round (PHP has no background threads). */
function pop_wait_until(float $windowSec, callable $cond, object ...$tickables): bool
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

/** Observation window: keeps ticking but asserts nothing — gives "things that must not happen" time to surface. */
function pop_observe(float $sec, object ...$tickables): void
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

function pop_new_admin(string $ns): DefaultMQAdminExt
{
    $a = new DefaultMQAdminExt();
    $a->setNamesrvAddr($ns);
    $a->start();
    return $a;
}

function pop_ensure_topic(string $ns, string $topic, int $queues): void
{
    $a = pop_new_admin($ns);
    try {
        $ci = $a->fetchBrokerClusterInfo();
        $names = array_keys($ci->brokerAddrTable);
        sort($names, SORT_STRING);
        $a->createTopic($names[0] ?? 'broker-a', $topic, $queues);
    } finally {
        $a->shutdown();
    }
}

/** Logical broker name the topic route points to (must appear in checkpoint segment 5, otherwise the ACK cannot be addressed). */
function pop_broker_name(string $ns, string $topic): string
{
    $a = pop_new_admin($ns);
    try {
        $route = $a->examineTopicRoute($topic);
        $bn = $route->brokerDatas[0]->brokerName ?? null;
        if ($bn === null || $bn === '') {
            throw new RuntimeException("no broker route for $topic");
        }
        return (string) $bn;
    } finally {
        $a->shutdown();
    }
}

function pop_send(DefaultMQProducer $p, string $topic, string $body): void
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

/** Segments the POP_CK shape assertion depends on. */
function pop_parse_ck(string $ck): array
{
    $seg = ExtraInfoUtil::split($ck !== '' ? $ck : null);
    if (count($seg) !== 8) {
        throw new InvalidArgumentException("checkpoint has " . count($seg) . " segments, want 8: \"$ck\"");
    }
    return [
        'segments' => 8,
        'ckOffset' => ExtraInfoUtil::getCkQueueOffset($seg),
        'popTime' => ExtraInfoUtil::getPopTime($seg),
        'invisible' => ExtraInfoUtil::getInvisibleTime($seg),
        'retry' => ExtraInfoUtil::getRetry($seg),
        'broker' => ExtraInfoUtil::getBrokerName($seg),
        'queueId' => ExtraInfoUtil::getQueueId($seg),
        'queueOffset' => ExtraInfoUtil::getQueueOffset($seg),
    ];
}

function pop_ck_summary(string $ck): string
{
    try {
        $s = pop_parse_ck($ck);
        return sprintf('ckOffset=%d queueOffset=%d retry=%s broker=%s qid=%d popTime=%d invisible=%d',
            $s['ckOffset'], $s['queueOffset'], $s['retry'], $s['broker'], $s['queueId'], $s['popTime'], $s['invisible']);
    } catch (Throwable $e) {
        return $e->getMessage();
    }
}

function pop_wait_route(string $ns, string $topic, float $windowSec): bool
{
    $a = pop_new_admin($ns);
    try {
        return pop_wait_until($windowSec, function () use ($a, $topic): bool {
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

function pop_set_request_mode(string $ns, string $topic, string $group): void
{
    $a = pop_new_admin($ns);
    try {
        $route = $a->examineTopicRoute($topic);
        $bd = $route->brokerDatas[0] ?? null;
        if ($bd === null) {
            throw new RuntimeException("no route for $topic");
        }
        $addrs = $bd->brokerAddrs ?? [];
        ksort($addrs);
        $addr = $addrs[0] ?? null;
        if ($addr === null) {
            throw new RuntimeException("no broker addr for $topic");
        }
        $a->setMessageRequestMode((string) $addr, $topic, $group, 'POP', 8);
    } finally {
        $a->shutdown();
    }
}

// ------------------------------------------------------------------ main

echo "=== PHP POP live: ns=$ns prefix=$prefix legs=" . ($legsArg === '' ? 'all' : $legsArg) . ' ===' . PHP_EOL;

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
            $a = pop_new_admin($ns);
            foreach ($createdTopics as $t) {
                try {
                    $a->deleteTopic($t);
                } catch (Throwable $e) {
                    echo '  delete ' . $t . ': ' . $e->getMessage() . PHP_EOL;
                }
            }
            $a->shutdown();
        } catch (Throwable) {
        }
    }
};

// ------------------------------------------------------------------ S1
if ($want('s1')) {
    echo PHP_EOL . '--- S1 basics: POP consumption + ACK really works (no redelivery in the window) + no offset commit ---' . PHP_EOL;
    $topic = $prefix . '_Basic';
    $createdTopics[] = $topic;
    $group = 'GID_' . $prefix . '_s1';
    $invisible = 5000; // 2.5x observation window ≈ 12.5s
    try {
        pop_ensure_topic($ns, $topic, 1);
        $brokerName = pop_broker_name($ns, $topic);
        $p = new DefaultMQProducer('GID_' . $prefix . '_s1_prod');
        $p->setNamesrvAddr($ns);
        $p->sendMsgTimeout = 5000;
        $p->start();
        $shutdowns[] = static fn() => $p->shutdown();

        $b = new PopBox();
        $c = new DefaultMQPushConsumer($group);
        $c->setNamesrvAddr($ns);
        $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $c->popMode = true;                    // must be set before start
        $c->popInvisibleTime = $invisible;
        $c->popBatchNums = 32;
        $c->consumeMessageBatchMaxSize = 5;
        $c->setMessageListener(new PopListener($b));
        $c->subscribe($topic, '*');
        $c->start();
        $shutdowns[] = static fn() => $c->shutdown();

        pop_set_request_mode($ns, $topic, $group);
        // POP consumers rebalance like pull (the broker's group mode only decides the protocol
        // answer side); the assignment holds 2 entries: 1 business-topic queue + 1 implicit
        // %RETRY%<group> (C1: counted by (topic,broker,queueId); counting queueID alone would
        // produce phantom overlaps).
        pop_wait_until(20.0, fn() => $c->assignedQueueCountForTopic($topic) >= 1, $c);
        $check($c->assignedQueueCountForTopic($topic) >= 1 && $c->assignedQueueCount() >= 2,
            'S1 assigned 1 queue of the business topic (+ implicit %RETRY%<group>)',
            'biz=' . $c->assignedQueueCountForTopic($topic) . ' total=' . $c->assignedQueueCount()
            . ' popQueues=' . $c->popQueueCount());

        $bodies = ['pop-a', 'pop-b', 'pop-c', 'pop-d', 'pop-e', 'pop-f'];
        $sendErr = '';
        try {
            foreach ($bodies as $body) {
                pop_send($p, $topic, $body);
            }
        } catch (Throwable $e) {
            $sendErr = $e->getMessage();
        }
        $check($sendErr === '', 'S1 sent 6 messages', $sendErr);

        $gotAll = pop_wait_until(40.0, function () use ($b, $bodies): bool {
            return count($b->summary()) === count($bodies);
        }, $c);
        $check($gotAll, 'S1 all 6 messages POPped', json_encode($b->summary(), JSON_UNESCAPED_UNICODE));

        // Every message must carry a checkpoint usable for ACK: 8 segments, segment 0 is the
        // batch start, segment 7 is the message's own offset (equality => the ACK segment was
        // chosen correctly), the marker is the business topic, segment 5 is the logical broker name.
        $recs = $b->recs;
        $shapeOK = false;
        $shape = count($recs) === 0 ? 'no delivery' : '';
        if ($recs !== []) {
            $shape = pop_ck_summary($recs[0]['ck']);
            try {
                $s = pop_parse_ck($recs[0]['ck']);
                $shapeOK = $s['ckOffset'] === $s['queueOffset'] && $s['retry'] === '0'
                    && $s['broker'] === $brokerName && $s['queueId'] === 0
                    && $s['invisible'] === $invisible && $s['popTime'] > 0;
            } catch (Throwable) {
                $shapeOK = false;
            }
        }
        $check($shapeOK, 'S1 POP_CK shape: 8 segments / ackOffset==msgQueueOffset / marker=0 / logical broker name', $shape);

        $firstPopOK = false;
        if ($recs !== []) {
            try {
                $s = pop_parse_ck($recs[0]['ck']);
                $firstPopOK = $recs[0]['firstPop'] !== '' && $recs[0]['firstPop'] === (string) $s['popTime'];
            } catch (Throwable) {
            }
        }
        $check($firstPopOK, 'S1 1ST_POP_TIME == this popTime',
            $recs === [] ? 'no delivery' : 'firstPop=' . $recs[0]['firstPop']);

        $listenerTopicOK = $recs !== [] && $recs[0]['topic'] === $topic;
        $check($listenerTopicOK, 'S1 the listener sees the business topic',
            $recs === [] ? 'no delivery' : 'got ' . $recs[0]['topic']);

        // The ACK debt must reach zero: every popped message got answered.
        $check(pop_wait_until(5.0, fn() => $c->popWaitAckCount() === 0, $c),
            'S1 ACK debt reaches zero', 'waitAck=' . $c->popWaitAckCount());

        // The central assertion. Deliberately watching "no duplicates" rather than "an ACK 200
        // came back": the broker also answers SUCCESS to a POP_ACK whose checkpoint it cannot
        // find. If the ACK is a no-op, the broker revives the whole batch once the window
        // closes, and the redelivery must land within the 2.5x window.
        pop_observe($invisible * 2.5 / 1000.0, $c);
        $check($b->noDupes(), 'S1 no duplicate delivery within 2.5x the invisible window (the ACK reached the broker)',
            'dup=' . $b->dupes() . ' all=' . json_encode($b->summary(), JSON_UNESCAPED_UNICODE));

        // POP keeps no client cursor: the broker's revive queue is the cursor, so the client
        // offset table must be empty.
        $check($c->localOffsetCount() === 0, 'S1 in POP mode the client holds no offset table',
            'localOffsets=' . $c->localOffsetCount());
    } catch (Throwable $e) {
        $check(false, 'S1 aborted by exception', get_class($e) . ': ' . $e->getMessage());
    }
}

// ------------------------------------------------------------------ S2
if ($want('s2')) {
    echo PHP_EOL . '--- S2 failure backoff: change invisible time -> %RETRY%<group>_<topic> -> redelivery + stable 1ST_POP_TIME ---' . PHP_EOL;
    $topic = $prefix . '_Retry';
    $createdTopics[] = $topic;
    $group = 'GID_' . $prefix . '_s2';
    $retryTopic = '%RETRY%' . $group . '_' . $topic;
    $poison = 'pop-poison';
    $invisible = 5000;
    try {
        pop_ensure_topic($ns, $topic, 1);
        pop_broker_name($ns, $topic);
        $p = new DefaultMQProducer('GID_' . $prefix . '_s2_prod');
        $p->setNamesrvAddr($ns);
        $p->sendMsgTimeout = 5000;
        $p->start();
        $shutdowns[] = static fn() => $p->shutdown();

        $b = new PopBox();
        // batchMax=1 is a requirement, not decoration: the client splits pop batches to the
        // listener at this granularity, and a failure must not hit the neighbors (otherwise
        // "the other messages delivered exactly once" becomes meaningless).
        $c = new DefaultMQPushConsumer($group);
        $c->setNamesrvAddr($ns);
        $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $c->popMode = true;
        $c->popInvisibleTime = $invisible;
        $c->popBatchNums = 32;
        $c->consumeMessageBatchMaxSize = 1;
        $c->maxReconsumeTimes = 16;
        $c->setMessageListener(new PopListener($b, $poison));
        $c->subscribe($topic, '*');
        $c->start();
        $shutdowns[] = static fn() => $c->shutdown();

        pop_set_request_mode($ns, $topic, $group);
        pop_wait_until(20.0, fn() => $c->assignedQueueCountForTopic($topic) >= 1, $c);

        $sendErr = '';
        try {
            foreach (['keep-1', $poison, 'keep-2'] as $body) {
                pop_send($p, $topic, $body);
            }
        } catch (Throwable $e) {
            $sendErr = $e->getMessage();
        }
        $check($sendErr === '', 'S2 sent 3 messages', $sendErr);

        $firstArrived = pop_wait_until(40.0, fn() => count($b->ofBody($poison)) >= 1, $c);
        $check($firstArrived, 'S2 first delivery of all 3',
            json_encode($b->summary(), JSON_UNESCAPED_UNICODE) . ' popQueues=' . $c->popQueueCount());
        if (!$firstArrived) {
            $cleanup();
            echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
            exit($fail === 0 ? 0 : 1);
        }
        $first = $b->ofBody($poison)[0];
        $firstMarker = '';
        try {
            $firstMarker = pop_parse_ck($first['ck'])['retry'];
        } catch (Throwable) {
        }
        $check($first['recon'] === 0 && $firstMarker === '0', 'S2 first delivery recon=0 and marker=0 (came from the business topic)',
            "recon={$first['recon']} marker=$firstMarker");

        // The failed message is hidden again rather than sent back: changePopInvisibleTime asks
        // the broker for a longer window (popDelayLevel[recon=0] = 10s); after the window closes
        // the revive service moves it into %RETRY%<group>_<topic>, and the next pop brings it back.
        $redelivered = pop_wait_until(120.0, fn() => count($b->ofBody($poison)) >= 2, $c);
        $check($redelivered, 'S2 the failed message was redelivered (change-invisible-time + revive worked)',
            json_encode($b->summary(), JSON_UNESCAPED_UNICODE));
        if ($redelivered) {
            $second = $b->ofBody($poison)[1];
            $check($second['recon'] >= 1, 'S2 redelivery recon increments',
                "recon {$first['recon']} -> {$second['recon']}");
            // Marker assertion: '1' = the message was popped from %RETRY%<group>_<topic> — both
            // evidence that the revive move succeeded and the reason the ACK must be addressed
            // to the retry topic (the broker records the checkpoint under the retry topic). A
            // marker wrongly '0' is exactly the bug this leg hunts.
            $secondMarker = '';
            try {
                $secondMarker = pop_parse_ck($second['ck'])['retry'];
            } catch (Throwable) {
            }
            $check($secondMarker === '1', 'S2 redelivery marker=1 (came from the POP retry topic; the ACK must be sent back there)',
                "marker=$secondMarker");
            $check($second['topic'] === $topic && $first['topic'] === $topic,
                'S2 the listener still sees the business topic on redelivery',
                "first={$first['topic']} second={$second['topic']} want=$topic");
            $secondPopTime = 0;
            try {
                $secondPopTime = pop_parse_ck($second['ck'])['popTime'];
            } catch (Throwable) {
            }
            $kept = $second['firstPop'] !== '' && $second['firstPop'] !== (string) $secondPopTime;
            $check($kept, 'S2 1ST_POP_TIME is not rewritten by the client (keeps the broker value)',
                "first={$first['firstPop']} second={$second['firstPop']} secondPopTime=$secondPopTime");
        }

        $sum = $b->summary();
        $check(($sum['keep-1'] ?? 0) === 1 && ($sum['keep-2'] ?? 0) === 1, 'S2 the other messages are delivered exactly once',
            json_encode($sum, JSON_UNESCAPED_UNICODE));

        // The retry topic is created by the revive service; its route appearing is an independent
        // broker-side confirmation of the whole path.
        $check(pop_wait_route($ns, $retryTopic, 45.0), 'S2 %RETRY%<group>_<topic> route appears', $retryTopic);

        // The redelivery was ACKed too, so the debt reaches zero again.
        $check(pop_wait_until(10.0, fn() => $c->popWaitAckCount() === 0, $c),
            'S2 ACK debt reaches zero', 'waitAck=' . $c->popWaitAckCount());
    } catch (Throwable $e) {
        $check(false, 'S2 aborted by exception', get_class($e) . ': ' . $e->getMessage());
    }
}

$cleanup();

echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);
