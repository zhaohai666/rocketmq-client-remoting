<?php

declare(strict_types=1);

// Live BATCH_ACK_MESSAGE (200151) verification for the POP path (L1/L2/L3).
//
//   php examples/live_pop_batch_ack.php [namesrv] [legs]     # legs = all | l1,l2,l3
//
// Why this needs a live leg of its own: the POP push consumer answers message by message with
// ACK_MESSAGE (200051) and never builds a batch, so nothing in the normal runtime reaches the
// batch wire path. The whole capability — the merge key, the bit index, the byte-minimal base64
// bit set, the header-less request shape — is only reachable through the exported primitive, and
// a primitive nobody has pointed at a broker is a primitive that may not work.
//
// The batch body carries no per-message offsets, only a start offset plus a bit set, so the one
// question a return code cannot answer is "which messages did the broker really clear". The
// broker answers SUCCESS to a body whose group key, start offset or bits are wrong; what it does
// not answer is the message going away. As with every POP leg, the criterion is therefore a
// **timed observation window + broker-side observable state**, never a return code:
//
//   L1 ONE REQUEST, WHOLE BATCH: 8 checkpoints cleared by a single 200151, then keep popping the
//      business topic and the group's POP retry topic for 2.5x the invisibility window. A wrong
//      bit or a wrong start offset leaves checkpoints alive and the revive service puts those
//      bodies back inside the window.
//   L2 PAIRED CONTROL: the same request with exactly one checkpoint left out must bring that one
//      message back and leave the rest gone — the assertion that fails if a bit set is treated as
//      "the whole batch". It also closes the retry direction: the revived message's checkpoint
//      carries the retry marker and lives in %RETRY%<group>_<topic>, and the batch body that
//      clears it must still name the BUSINESS topic, because the broker re-derives the physical
//      topic from marker + group. This window is a probe that stops at the first sighting: a
//      checkpoint that is sighted, expires and is sighted again is already revived by the time the
//      ack lands, and the ack cannot take it back, so the silence would fail on timing rather than
//      on anything the client did.
//   L3 NO OVER-MERGING: two pop rounds of one queue acked by one request must produce one entry
//      per (retry, queueId, ck offset, popTime) and must not collapse the rounds — collapsing
//      them would acknowledge offsets nobody popped.
//
// The broker needs no special switch: 200151 is registered unconditionally and the revive side
// handles the batch ack tag the same way, so the standard cluster config already serves it
// (enablePopBatchAck only steers the broker's internal re-put of buffered acks, which is inert
// while enablePopBufferMerge=false). Every topic is created up front and each leg waits until the
// broker reports the expected queue depth before popping — a partially dispatched batch would
// make "one request cleared the batch" unprovable.
//
// PHP starts no threads: the client instance is advanced with tick() and every pop is an explicit
// call in the main loop.
//
// Closing line `PASS=<n> FAIL=<n>`; exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PopResult;
use RocketMQ\Client\PopStatus;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\Protocol\BatchAck;
use RocketMQ\Remoting\Protocol\BatchAckMessageRequestBody;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\ResponseCode;

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
$prefix = 'PhpBatchAck' . $stamp;

// Ten seconds of invisibility keeps the observation windows short while staying far above the
// revive latency; 2.5x of it is the window each leg proves silence in.
const BA_INVISIBLE_MILLIS = 10000;
const BA_OBSERVE_WINDOW_SEC = 25.0;
/**
 * L2's first window is a probe, not a fixed window: it stops shortly after the dropped message is
 * sighted. Every sighting leaves a fresh checkpoint that is invisible for BA_INVISIBLE_MILLIS, so a
 * long fixed window watches the same message revive, get popped and revive again, and the batch ack
 * then carries a checkpoint whose invisibility already ran out — the broker moved it on, the ack
 * cannot take it back, and the final silence check fails on timing instead of on the wire path.
 */
const BA_REVIVE_WAIT_SEC = 40.0;
const BA_REVIVE_TAIL_SEC = 3.0;
/** A long poll adds its own wait on top of the RPC timeout. */
const BA_POP_TIMEOUT_OVERHEAD = 5000;
const BA_RPC_TIMEOUT_MILLIS = 5000;

// ------------------------------------------------------------------ wire driver

/**
 * The batch ack is never sent by the push consumer, so the leg drives the primitives directly:
 * popMessage / batchAckMessage on a started client instance, borrowed from an admin instance
 * (which resolves the broker address from the topic route on demand).
 */
final class BaWire
{
    public function __construct(
        public readonly DefaultMQAdminExt $admin,
        public readonly MQClientInstance $inst,
        public readonly string $brokerName,
    ) {
    }

    public function tick(): void
    {
        $this->inst->tick();
    }

    public function shutdown(): void
    {
        $this->admin->shutdown();
    }
}

function ba_new_admin(string $ns): DefaultMQAdminExt
{
    $a = new DefaultMQAdminExt();
    $a->setNamesrvAddr($ns);
    $a->start();
    return $a;
}

/** One queue: a batch of N must land on a single queue, otherwise the merge keys say nothing. */
function ba_ensure_topic(string $ns, string $topic): void
{
    $a = ba_new_admin($ns);
    try {
        $ci = $a->fetchBrokerClusterInfo();
        $names = array_keys($ci->brokerAddrTable);
        sort($names, SORT_STRING);
        $a->createTopic($names[0] ?? 'broker-a', $topic, 1);
    } finally {
        $a->shutdown();
    }
}

function ba_broker_name(string $ns, string $topic): string
{
    $a = ba_new_admin($ns);
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

function ba_new_verifier(string $ns, string $brokerName): BaWire
{
    $a = ba_new_admin($ns);
    return new BaWire($a, $a->getMqClientInstance(), $brokerName);
}

/**
 * POP a batch. queueId -1 means every queue of the topic, which is what the observation loop
 * needs; a leg that must pin the merge keys asks for queue 0 explicitly.
 */
function ba_pop(
    MQClientInstance $inst,
    string $brokerName,
    string $topic,
    string $group,
    int $queueId,
    int $maxNums,
    int $pollMillis,
): PopResult {
    return $inst->popMessage(
        consumerGroup: $group,
        topic: $topic,
        queueId: $queueId,
        maxMsgNums: $maxNums,
        invisibleTime: BA_INVISIBLE_MILLIS,
        pollTime: $pollMillis,
        initMode: 0,
        order: false,
        brokerName: $brokerName,
        timeoutMillis: $pollMillis + BA_POP_TIMEOUT_OVERHEAD,
    );
}

/**
 * The body sent on the wire, rebuilt here from the same checkpoints so the structural assertions
 * describe the very construction batchAckMessage() performs before putting the request on the
 * socket. The builder is deterministic, so this is the same body, not a look-alike.
 */
function ba_body(string $topic, string $group, array $rawList): BatchAckMessageRequestBody
{
    return BatchAckMessageRequestBody::fromExtraInfos($group, $topic, $rawList);
}

function ba_batch_ack(MQClientInstance $inst, string $topic, string $group, array $rawList): int
{
    return $inst->batchAckMessage($group, $topic, $rawList, BA_RPC_TIMEOUT_MILLIS);
}

// ------------------------------------------------------------------ producer

function ba_start_producer(string $ns, string $group): DefaultMQProducer
{
    $p = new DefaultMQProducer($group);
    $p->setNamesrvAddr($ns);
    $p->sendMsgTimeout = 5000;
    $p->start();
    return $p;
}

function ba_send(DefaultMQProducer $p, string $topic, string $body): void
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

/** @param list<string> $bodies */
function ba_send_all(DefaultMQProducer $p, string $topic, array $bodies): void
{
    foreach ($bodies as $body) {
        ba_send($p, $topic, $body);
    }
}

/**
 * Messages reach the queue through the dispatch service, so "sent" is not "poppable": wait until
 * the broker reports the expected depth, otherwise a partial batch makes the silence of L1 and
 * L3 mean nothing. Returns [depth, reached].
 *
 * @return array{int, bool}
 */
function ba_wait_depth(string $ns, string $topic, int $want, float $windowSec): array
{
    $a = ba_new_admin($ns);
    try {
        $deadline = microtime(true) + $windowSec;
        $depth = 0;
        for (;;) {
            try {
                $depth = ba_queue_depth($a, $topic);
            } catch (Throwable) {
                $depth = 0;
            }
            if ($depth >= $want) {
                return [$depth, true];
            }
            if (microtime(true) >= $deadline) {
                return [$depth, false];
            }
            usleep(500_000);
        }
    } finally {
        $a->shutdown();
    }
}

function ba_queue_depth(DefaultMQAdminExt $a, string $topic): int
{
    $sum = 0;
    foreach ($a->examineTopicStats($topic)->offsetTable as ['value' => $v]) {
        $sum += max(0, $v->maxOffset - $v->minOffset);
    }
    return $sum;
}

// ------------------------------------------------------------------ checkpoints

/**
 * One checkpoint, read out of the POP_CK property independently of the builder: the batch body
 * addresses a message only through segments 0 and 7 and routes only through segments 3-6, so
 * every assertion about the body starts from these values.
 *
 * @return array{raw:string, body:string, segments:int, ckOff:int, popTime:int, invisible:int,
 *               reviveQID:int, retry:string, broker:string, queueID:int, queueOff:int}
 */
function ba_ck_from_raw(string $raw, string $body): array
{
    $segments = ExtraInfoUtil::split($raw !== '' ? $raw : null);
    $entry = [
        'raw' => $raw,
        'body' => $body,
        'segments' => count($segments),
        'ckOff' => 0,
        'popTime' => 0,
        'invisible' => 0,
        'reviveQID' => 0,
        'retry' => '',
        'broker' => '',
        'queueID' => 0,
        'queueOff' => 0,
    ];
    if (count($segments) !== 8) {
        throw new InvalidArgumentException("checkpoint has {$entry['segments']} segments, want 8: \"$raw\"");
    }
    return [
        ...$entry,
        'ckOff' => ExtraInfoUtil::getCkQueueOffset($segments),
        'popTime' => ExtraInfoUtil::getPopTime($segments),
        'invisible' => ExtraInfoUtil::getInvisibleTime($segments),
        'reviveQID' => ExtraInfoUtil::getReviveQid($segments),
        'retry' => ExtraInfoUtil::getRetry($segments),
        'broker' => ExtraInfoUtil::getBrokerName($segments),
        'queueID' => ExtraInfoUtil::getQueueId($segments),
        // Segment 7, not the message's own decoded field: this is the value the body turns into
        // a bit index.
        'queueOff' => ExtraInfoUtil::getQueueOffset($segments),
    ];
}

/**
 * @param list<MessageExt> $msgs
 * @return array{list<array{raw:string,body:string,segments:int,ckOff:int,popTime:int,invisible:int,reviveQID:int,retry:string,broker:string,queueID:int,queueOff:int}>, list<string>}
 */
function ba_parse_cks(array $msgs): array
{
    $out = [];
    $problems = [];
    foreach ($msgs as $m) {
        $raw = (string) ($m->getUserProperty(MessageConst::PROPERTY_POP_CK) ?? '');
        $body = (string) $m->getBody();
        try {
            $entry = ba_ck_from_raw($raw, $body);
        } catch (Throwable $e) {
            $problems[] = "$body: " . $e->getMessage();
            continue;
        }
        // The message's own offset must agree with segment 7: if it does not, the pop filled the
        // checkpoint from the wrong source and every bit derived from it addresses a different
        // message than the one in hand.
        if ($entry['queueOff'] !== $m->queueOffset) {
            $problems[] = "$body: ck segment 7 = {$entry['queueOff']} but the message offset is {$m->queueOffset}";
        }
        $out[] = $entry;
    }
    return [$out, $problems];
}

/**
 * @param list<string> $rawList
 * @return array{list<array{raw:string,body:string,segments:int,ckOff:int,popTime:int,invisible:int,reviveQID:int,retry:string,broker:string,queueID:int,queueOff:int}>, list<string>}
 */
function ba_parse_ck_strings(array $rawList, string $body): array
{
    $out = [];
    $problems = [];
    foreach ($rawList as $raw) {
        try {
            $out[] = ba_ck_from_raw((string) $raw, $body);
        } catch (Throwable $e) {
            $problems[] = "$body: " . $e->getMessage();
        }
    }
    return [$out, $problems];
}

/**
 * The grouping key the batch body is built on, recomputed here rather than read from the builder:
 * "the builder grouped it" must not be the same statement as "the key says it should group".
 *
 * @param array{retry:string, queueID:int, ckOff:int, popTime:int} $c
 */
function ba_merge_key(array $c): string
{
    return sprintf('%s@%d@%d@%d', $c['retry'], $c['queueID'], $c['ckOff'], $c['popTime']);
}

/** The same formula applied to an entry the builder produced — that is what makes the comparison a statement about the builder. */
function ba_merge_key_of(BatchAck $ack): string
{
    return sprintf('%s@%d@%d@%d', $ack->retry, $ack->queueId, $ack->startOffset, $ack->popTime);
}

/** @param list<array{retry:string, queueID:int, ckOff:int, popTime:int}> $cks */
function ba_distinct_keys(array $cks): array
{
    $keys = [];
    foreach ($cks as $c) {
        $keys[ba_merge_key($c)] = true;
    }
    $out = array_keys($keys);
    sort($out, SORT_STRING);
    return $out;
}

/**
 * The body expanded back into the queue offsets it claims to acknowledge: entry start offset plus
 * each bit index. Offsets that appear twice mean the same message acked from two entries.
 *
 * @return array<int, int> offset -> how many bits claim it
 */
function ba_acked_offsets(BatchAckMessageRequestBody $body): array
{
    $out = [];
    foreach ($body->acks as $ack) {
        foreach ($ack->bitSet->setIndexes() as $index) {
            $offset = $ack->startOffset + $index;
            $out[$offset] = ($out[$offset] ?? 0) + 1;
        }
    }
    ksort($out, SORT_NUMERIC);
    return $out;
}

/** @param list<array{queueOff:int}> $cks @return list<int> */
function ba_offsets_of(array $cks): array
{
    $out = array_map(static fn(array $c): int => $c['queueOff'], $cks);
    sort($out, SORT_NUMERIC);
    return $out;
}

/** The set equivalence plus the "once each" condition: every acked offset appears exactly one time. */
function ba_same_offset_set(array $acked, array $offsets): bool
{
    $seen = [];
    foreach ($offsets as $o) {
        $seen[$o] = true;
    }
    if (count($acked) !== count($seen)) {
        return false;
    }
    foreach ($acked as $offset => $count) {
        if (!isset($seen[$offset]) || $count !== 1) {
            return false;
        }
    }
    return true;
}

/** @param list<array{body:string}> $cks */
function ba_bodies_of(array $cks): string
{
    $names = array_map(static fn(array $c): string => $c['body'], $cks);
    sort($names, SORT_STRING);
    return implode(' ', $names);
}

/** @param list<array{body:string,ckOff:int,queueOff:int,popTime:int,retry:string,queueID:int,broker:string,invisible:int}> $cks */
function ba_detail_of(array $cks): string
{
    $parts = [];
    foreach ($cks as $c) {
        $parts[] = sprintf('%s{ck=%d off=%d pt=%d r=%s q=%d broker=%s it=%d}',
            $c['body'], $c['ckOff'], $c['queueOff'], $c['popTime'], $c['retry'],
            $c['queueID'], $c['broker'], $c['invisible']);
    }
    return implode(' ', $parts);
}

/** @param list<array{raw:string}> $cks @return list<string> */
function ba_raw_strings(array $cks): array
{
    return array_map(static fn(array $c): string => $c['raw'], $cks);
}

/** @param list<array{retry:string}> $cks */
function ba_all_markers(array $cks, string $want): bool
{
    if ($cks === []) {
        return false;
    }
    foreach ($cks as $c) {
        if ($c['retry'] !== $want) {
            return false;
        }
    }
    return true;
}

/** @return list<string> the retry marker of every entry, in body order */
function ba_markers_of_body(BatchAckMessageRequestBody $body): array
{
    return array_map(static fn(BatchAck $ack): string => $ack->retry, $body->acks);
}

function ba_clip(string $s, int $max = 220): string
{
    return strlen($s) > $max ? substr($s, 0, $max) . '...' : $s;
}

// ------------------------------------------------------------------ observation

/**
 * What the observation loop saw. A checkpoint is captured at sighting time: every POP that returns
 * a message hides it again for the whole invisibility window, so a later re-pop of the same body
 * answers POLLING_NOT_FOUND and the follow-up ack would have nothing to work from.
 */
final class BaEyes
{
    /** @var array<string, list<array{from:string, ck:string, marker:string, recon:int, firstPop:string}>> */
    public array $sighted = [];
    /** @var list<string> */
    public array $problems = [];

    /** @param list<MessageExt> $msgs */
    public function record(array $msgs, string $from): void
    {
        foreach ($msgs as $m) {
            $body = (string) $m->getBody();
            $raw = (string) ($m->getUserProperty(MessageConst::PROPERTY_POP_CK) ?? '');
            $marker = '';
            $segments = $raw === '' ? [] : ExtraInfoUtil::split($raw);
            if (count($segments) === 8) {
                $marker = ExtraInfoUtil::getRetry($segments);
            }
            $this->sighted[$body][] = [
                'from' => $from,
                'ck' => $raw,
                'marker' => $marker,
                'recon' => $m->reconsumeTimes,
                'firstPop' => (string) ($m->getUserProperty(MessageConst::PROPERTY_FIRST_POP_TIME) ?? ''),
            ];
        }
    }

    /** @return list<array{from:string, ck:string, marker:string, recon:int, firstPop:string}> */
    public function sightings(string $body): array
    {
        return $this->sighted[$body] ?? [];
    }

    /** @return list<string> the topics this body was answered from */
    public function topicsSeen(string $body): array
    {
        $out = [];
        foreach ($this->sightings($body) as $s) {
            if (!in_array($s['from'], $out, true)) {
                $out[] = $s['from'];
            }
        }
        return $out;
    }

    /**
     * Every checkpoint the window collected for a body. A message sighted more than once has one
     * live checkpoint per sighting, and an ack that clears only one of them leaves the others to
     * revive it again.
     *
     * @return list<string>
     */
    public function checkpointsFor(string $body): array
    {
        $out = [];
        foreach ($this->sightings($body) as $s) {
            if ($s['ck'] !== '') {
                $out[] = $s['ck'];
            }
        }
        return $out;
    }

    /** @return list<string> */
    public function markersFor(string $body): array
    {
        return array_map(static fn(array $s): string => $s['marker'], $this->sightings($body));
    }

    public function cameBack(string $body): bool
    {
        return ($this->sighted[$body] ?? []) !== [];
    }

    /** @return list<string> */
    public function allBodies(): array
    {
        $out = array_keys($this->sighted);
        sort($out, SORT_STRING);
        return $out;
    }

    public function summary(): string
    {
        $bodies = $this->allBodies();
        return $bodies === [] ? 'nothing revived' : 'revived: ' . implode(' ', $bodies);
    }
}

/**
 * One round: POP the business topic AND the group's POP retry topic. Both, because a checkpoint
 * nobody acked is revived into the retry topic, and a POP of the business topic only consults that
 * topic by probability — watching one of them would make a silence ambiguous.
 */
function ba_round(BaWire $v, string $group, string $topic, BaEyes $e, string &$firstError): void
{
    $retryTopic = ExtraInfoUtil::buildPopRetryTopicV1($topic, $group);
    $v->tick();
    foreach ([$topic, $retryTopic] as $t) {
        try {
            $res = ba_pop($v->inst, $v->brokerName, $t, $group, -1, 32, 500);
        } catch (Throwable $ex) {
            // A POP retry topic only exists once the revive service has put something in it,
            // so "topic not exist" is the expected answer while nothing has been revived;
            // anything else is worth seeing.
            if ($firstError === '' && !str_contains($ex->getMessage(), 'not exist')) {
                $firstError = $t . ': ' . get_class($ex) . ': ' . $ex->getMessage();
            }
            continue;
        }
        if ($res->status !== PopStatus::FOUND) {
            continue;
        }
        $e->record($res->msgFoundList, $t);
    }
}

/** Keep popping both topics until the window expires. */
function ba_watch(BaWire $v, string $group, string $topic, float $windowSec, BaEyes $e): void
{
    $deadline = microtime(true) + $windowSec;
    $firstError = '';
    while (microtime(true) < $deadline) {
        ba_round($v, $group, $topic, $e, $firstError);
    }
    if ($firstError !== '') {
        $e->problems[] = $firstError;
    }
}

/**
 * Pop both topics until `$waitFor` turns up, then keep going for `$tailSec` — long enough for the
 * same round set to show that the acked messages did NOT come back, short enough to stay inside the
 * checkpoint's invisibility — and return. Returns false if `$maxWindowSec` ran out first.
 */
function ba_probe(BaWire $v, string $group, string $topic, string $waitFor, float $maxWindowSec, float $tailSec, BaEyes $e): bool
{
    $deadline = microtime(true) + $maxWindowSec;
    $stoppedAt = null;
    $firstError = '';
    while (microtime(true) < $deadline) {
        ba_round($v, $group, $topic, $e, $firstError);
        if ($stoppedAt === null && $e->cameBack($waitFor)) {
            $stoppedAt = microtime(true) + $tailSec;
        }
        if ($stoppedAt !== null && microtime(true) >= $stoppedAt) {
            if ($firstError !== '') {
                $e->problems[] = $firstError;
            }
            return true;
        }
    }
    if ($firstError !== '') {
        $e->problems[] = $firstError;
    }
    return $e->cameBack($waitFor);
}

/**
 * The POP retry topic is created when the revive service moves a checkpoint into it, so its route
 * reaching the name server is a broker-side confirmation that the revival really happened — and,
 * unlike "which RPC answered the message", it is a fact about the broker.
 */
function ba_wait_route(string $ns, string $topic, float $windowSec): bool
{
    $a = ba_new_admin($ns);
    try {
        $deadline = microtime(true) + $windowSec;
        for (;;) {
            try {
                $route = $a->examineTopicRoute($topic);
                if ($route !== null && count($route->brokerDatas ?? []) > 0) {
                    return true;
                }
            } catch (Throwable) {
            }
            if (microtime(true) >= $deadline) {
                return false;
            }
            usleep(500_000);
        }
    } finally {
        $a->shutdown();
    }
}

/** @param list<array{body:string}> $cks @return array<string, true> */
function ba_body_set(array $cks): array
{
    $out = [];
    foreach ($cks as $c) {
        $out[$c['body']] = true;
    }
    return $out;
}

// ------------------------------------------------------------------ main

echo "=== PHP POP batch-ack live: ns=$ns prefix=$prefix legs=" . ($legsArg === '' ? 'all' : $legsArg) . ' ===' . PHP_EOL;

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
        try {
            $fn();
        } catch (Throwable) {
        }
    }
    if ($createdTopics !== []) {
        try {
            $a = ba_new_admin($ns);
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

// ------------------------------------------------------------------ L1
if ($want('l1')) {
    echo PHP_EOL . '--- L1 one BATCH_ACK_MESSAGE(200151) clears a whole POP batch ---' . PHP_EOL;
    $batch = 8;
    $topic = $prefix . '_BaAll';
    $group = 'GID_' . $prefix . '_l1';
    $createdTopics[] = $topic;
    $bodies = [];
    for ($i = 0; $i < $batch; $i++) {
        $bodies[] = "ba-all-$i";
    }
    $v = null;
    try {
        ba_ensure_topic($ns, $topic);
        $brokerName = ba_broker_name($ns, $topic);
        $p = ba_start_producer($ns, 'GID_' . $prefix . '_l1_prod');
        $shutdowns[] = static fn() => $p->shutdown();
        ba_send_all($p, $topic, $bodies);
        $check(true, "L1 send $batch messages", $topic);

        [$depth, $ready] = ba_wait_depth($ns, $topic, $batch, 30.0);
        $check($ready, "L1 all $batch messages are on the queue before popping", "depth=$depth want=$batch");
        if (!$ready) {
            throw new RuntimeException('queue never filled');
        }

        $v = ba_new_verifier($ns, $brokerName);
        $shutdowns[] = static fn() => $v->shutdown();

        $res = ba_pop($v->inst, $brokerName, $topic, $group, 0, $batch, 3000);
        $check($res->status === PopStatus::FOUND && count($res->msgFoundList) === $batch,
            'L1 POP_MESSAGE answered the whole batch',
            'status=' . $res->status->name . ' got=' . count($res->msgFoundList));

        [$cks, $problems] = ba_parse_cks($res->msgFoundList);
        $check($problems === [], 'L1 every message carries a parsable 8-segment checkpoint', implode('; ', $problems));
        if (count($cks) !== $batch) {
            throw new RuntimeException('checkpoint count ' . count($cks));
        }

        // The body can only address a message through segment 7 and only route through segments
        // 4-6, so pin them per message before the builder is trusted with them.
        $shapeOK = true;
        $shapeDetail = '';
        foreach ($cks as $c) {
            if ($c['retry'] !== ExtraInfoUtil::NORMAL_TOPIC || $c['broker'] !== $brokerName
                || $c['queueID'] !== 0 || $c['invisible'] !== BA_INVISIBLE_MILLIS
                || $c['popTime'] <= 0 || $c['ckOff'] > $c['queueOff']) {
                $shapeOK = false;
                $shapeDetail = sprintf('%s{ck=%d off=%d r=%s q=%d broker=%s it=%d}',
                    $c['body'], $c['ckOff'], $c['queueOff'], $c['retry'], $c['queueID'],
                    $c['broker'], $c['invisible']);
                break;
            }
        }
        $check($shapeOK, 'L1 checkpoint fields the batch body depends on (marker 0 / logical broker / queueId / invisible / ck<=msg offset)',
            $shapeDetail !== '' ? $shapeDetail : 'all 8 checkpoints');
        if (!$shapeOK) {
            echo '     ' . ba_detail_of($cks) . PHP_EOL;
            throw new RuntimeException('checkpoint shape unexpected');
        }

        $body = ba_body($topic, $group, ba_raw_strings($cks));

        // Three body invariants, each one a way the builder could be wrong while still producing a
        // request the broker answers SUCCESS to.
        $acked = ba_acked_offsets($body);
        $check(ba_same_offset_set($acked, ba_offsets_of($cks)),
            'L1 body acknowledges exactly the popped offsets, once each',
            'acked=[' . implode(' ', array_keys($acked)) . '] want=[' . implode(' ', ba_offsets_of($cks)) . ']');
        $check(count($body->acks) === count(ba_distinct_keys($cks)),
            'L1 body has exactly one entry per merge key',
            'entries=' . count($body->acks) . ' keys=[' . implode(' ', ba_distinct_keys($cks)) . ']');

        // Every field of an entry has to be the value the checkpoint carried: the broker looks the
        // checkpoint up by (group, real topic, queueId, popTime, invisibleTime, revive queue) and
        // then walks the bits from startOffset, so one invented field makes the whole entry a
        // no-op that still answers SUCCESS.
        $byKey = [];
        foreach ($cks as $c) {
            $byKey[ba_merge_key($c)] = $c;
        }
        $fieldsOK = true;
        $fieldDetail = '';
        foreach ($body->acks as $ack) {
            if ($body->brokerName !== $brokerName || $ack->consumerGroup !== $group || $ack->topic !== $topic) {
                $fieldsOK = false;
                $fieldDetail = sprintf('entry c=%s t=%s bodyBroker=%s want c=%s t=%s broker=%s',
                    var_export($ack->consumerGroup, true), var_export($ack->topic, true),
                    var_export($body->brokerName, true), $group, $topic, $brokerName);
                break;
            }
            $key = ba_merge_key_of($ack);
            $source = $byKey[$key] ?? null;
            if ($source === null) {
                $fieldsOK = false;
                $fieldDetail = "entry key $key matches no popped checkpoint";
                break;
            }
            if ($ack->startOffset !== $source['ckOff'] || $ack->popTime !== $source['popTime']
                || $ack->invisibleTime !== $source['invisible'] || $ack->reviveQueueId !== $source['reviveQID']
                || $ack->queueId !== $source['queueID'] || $ack->retry !== $source['retry']
                || $ack->bitSet->setIndexes() === []) {
                $fieldsOK = false;
                $fieldDetail = sprintf('entry %s diverges from checkpoint %s: so=%d/%d pt=%d/%d it=%d/%d rq=%d/%d q=%d/%d r=%s/%s bits=%d',
                    $key, ba_bodies_of([$source]),
                    $ack->startOffset, $source['ckOff'], $ack->popTime, $source['popTime'],
                    $ack->invisibleTime, $source['invisible'], $ack->reviveQueueId, $source['reviveQID'],
                    $ack->queueId, $source['queueID'], var_export($ack->retry, true), $source['retry'],
                    count($ack->bitSet->setIndexes()));
                break;
            }
        }
        $check($fieldsOK, 'L1 every entry echoes its checkpoint fields, names the group, the business topic and the checkpoint broker',
            $fieldDetail);

        // The wire form is where a base64 bit set goes wrong silently: the broker reads the byte
        // array back as a bit set of absolute positions, so a non-minimal or byte-reversed
        // encoding still decodes as *some* set of offsets.
        $encoded = $body->encode();
        $decoded = BatchAckMessageRequestBody::decode($encoded);
        $check(count($decoded->acks) === count($body->acks)
            && ba_same_offset_set(ba_acked_offsets($decoded), ba_offsets_of($cks)),
            'L1 body round-trips through the wire codec',
            sprintf('entries %d -> %d, bytes=%d, body=%s',
                count($body->acks), count($decoded->acks), strlen($encoded), ba_clip($encoded)));

        $code = ba_batch_ack($v->inst, $topic, $group, ba_raw_strings($cks));
        $check($code === ResponseCode::SUCCESS, 'L1 one BATCH_ACK_MESSAGE answered SUCCESS',
            sprintf('code=%d %d checkpoints -> %d entries', $code, count($cks), count($body->acks)));

        // The central assertion. Deliberately watching "nothing comes back" rather than "a 200
        // came back": the broker answers SUCCESS to a batch body whose checkpoints it cannot find.
        $e = new BaEyes();
        ba_watch($v, $group, $topic, BA_OBSERVE_WINDOW_SEC, $e);
        $check($e->allBodies() === [], 'L1 nothing revived inside 2.5x the invisibility window (the batch really landed)',
            $e->summary());
        if ($e->problems !== []) {
            echo '     watch notes: ' . implode('; ', $e->problems) . PHP_EOL;
        }
    } catch (Throwable $ex) {
        $check(false, 'L1 aborted', get_class($ex) . ': ' . $ex->getMessage());
    }
}

// ------------------------------------------------------------------ L2
if ($want('l2')) {
    echo PHP_EOL . '--- L2 paired control: an unacked bit comes back, the acked ones do not ---' . PHP_EOL;
    $batch = 6;
    $topic = $prefix . '_BaPart';
    $group = 'GID_' . $prefix . '_l2';
    $createdTopics[] = $topic;
    $bodies = [];
    for ($i = 0; $i < $batch; $i++) {
        $bodies[] = "ba-part-$i";
    }
    try {
        ba_ensure_topic($ns, $topic);
        $brokerName = ba_broker_name($ns, $topic);
        $p = ba_start_producer($ns, 'GID_' . $prefix . '_l2_prod');
        $shutdowns[] = static fn() => $p->shutdown();
        ba_send_all($p, $topic, $bodies);
        $check(true, "L2 send $batch messages", $topic);

        [$depth, $ready] = ba_wait_depth($ns, $topic, $batch, 30.0);
        $check($ready, "L2 all $batch messages are on the queue before popping", "depth=$depth");
        if (!$ready) {
            throw new RuntimeException('queue never filled');
        }

        $v = ba_new_verifier($ns, $brokerName);
        $shutdowns[] = static fn() => $v->shutdown();

        $res = ba_pop($v->inst, $brokerName, $topic, $group, 0, $batch, 3000);
        $check($res->status === PopStatus::FOUND && count($res->msgFoundList) === $batch,
            'L2 POP_MESSAGE answered the batch', 'status=' . $res->status->name . ' got=' . count($res->msgFoundList));
        [$cks, $problems] = ba_parse_cks($res->msgFoundList);
        if ($problems !== [] || count($cks) !== $batch) {
            $check(false, 'L2 checkpoints parsable', implode('; ', $problems) . ' count=' . count($cks));
            throw new RuntimeException('unparsable checkpoints');
        }

        // Drop exactly one: the message with the HIGHEST queue offset, whose bit is the last one
        // the bitmap can carry. Every other message keeps its ack.
        usort($cks, static fn(array $a, array $b): int => $a['queueOff'] <=> $b['queueOff']);
        $dropped = $cks[count($cks) - 1];
        $ackedCks = array_slice($cks, 0, count($cks) - 1);

        $body = ba_body($topic, $group, ba_raw_strings($ackedCks));
        $acked = ba_acked_offsets($body);
        $check(ba_same_offset_set($acked, ba_offsets_of($ackedCks)) && !isset($acked[$dropped['queueOff']]),
            sprintf('L2 body acknowledges %d of %d offsets and not the dropped one', count($ackedCks), $batch),
            'acked=[' . implode(' ', array_keys($acked)) . '] dropped=' . $dropped['queueOff']);

        $code = ba_batch_ack($v->inst, $topic, $group, ba_raw_strings($ackedCks));
        $check($code === ResponseCode::SUCCESS, 'L2 partial BATCH_ACK_MESSAGE answered SUCCESS',
            sprintf('code=%d %d of %d checkpoints', $code, count($ackedCks), $batch));

        $e = new BaEyes();
        // Stop at the first sighting plus a short tail: the tail is the fair comparison for the
        // acked messages (they would have revived at the same instant if the bitmap had been
        // decorative), and stopping there keeps the one checkpoint we are about to ack inside its
        // own invisibility.
        $broughtBack = ba_probe($v, $group, $topic, $dropped['body'], BA_REVIVE_WAIT_SEC, BA_REVIVE_TAIL_SEC, $e);

        // THE control. If the bitmap were decorative — on the client or the broker — this leg looks
        // exactly like L1.
        $check($broughtBack, 'L2 the unacked message really came back',
            sprintf('bodies=%s topics=[%s]', var_export($dropped['body'], true), implode(' ', $e->topicsSeen($dropped['body']))));
        $restGone = true;
        $cameBackToo = [];
        foreach ($ackedCks as $c) {
            if ($e->cameBack($c['body'])) {
                $restGone = false;
                $cameBackToo[] = $c['body'];
            }
        }
        $check($restGone, sprintf('L2 the %d acked messages stayed gone while the dropped one came back', count($ackedCks)),
            $cameBackToo === [] ? $e->summary() : 'also revived: ' . implode(' ', $cameBackToo) . ' | ' . $e->summary());
        if (!$broughtBack) {
            echo '     watch notes: ' . implode('; ', $e->problems) . PHP_EOL;
            throw new RuntimeException('the control message never came back');
        }

        // The revived message carries marker "1" in its checkpoint, and it is that marker — not the
        // topic the reply came from — that tells the broker where the checkpoint lives. So the
        // follow-up batch ack must carry the BUSINESS topic.
        $retryTopic = ExtraInfoUtil::buildPopRetryTopicV1($topic, $group);
        $seenFrom = $e->topicsSeen($dropped['body']);

        // Clear the checkpoint right away: it started its invisibility the moment the reply handed
        // it over, and every assertion in between has to be a local one.
        $raws = $e->checkpointsFor($dropped['body']);
        [$cks2, $problems2] = ba_parse_ck_strings($raws, $dropped['body']);
        if ($problems2 !== []) {
            $check(false, 'L2 the revived checkpoints are parsable', implode('; ', $problems2));
            throw new RuntimeException('unparsable revived checkpoints');
        }
        $check(ba_all_markers($cks2, ExtraInfoUtil::RETRY_TOPIC),
            'L2 the revived checkpoints carry the retry marker (they live in the retry queue)',
            'sightings=' . count($raws) . ' markers=[' . implode(' ', $e->markersFor($dropped['body'])) . ']');

        $second = ba_body($topic, $group, ba_raw_strings($cks2));
        // The body names the BUSINESS topic even though the checkpoints came from the retry topic:
        // the broker re-derives the physical topic from marker + group, so a client that
        // "helpfully" passed the retry topic here would double-wrap the name and ack nothing.
        $retryBodyOK = $second->acks !== [];
        foreach ($second->acks as $ack) {
            if ($ack->topic !== $topic || $ack->retry !== ExtraInfoUtil::RETRY_TOPIC || $ack->consumerGroup !== $group) {
                $retryBodyOK = false;
            }
        }
        $check($retryBodyOK, 'L2 the retry body keeps the business topic and carries the marker on every entry',
            'entries=' . count($second->acks) . ' t=' . var_export($topic, true)
            . ' r=[' . implode(' ', ba_markers_of_body($second)) . ']');

        $code2 = ba_batch_ack($v->inst, $topic, $group, ba_raw_strings($cks2));
        $check($code2 === ResponseCode::SUCCESS, 'L2 retry-topic BATCH_ACK_MESSAGE answered SUCCESS',
            sprintf('code=%d %d revived checkpoints -> %d entries', $code2, count($cks2), count($second->acks)));

        // Which RPC answered the message is deliberately NOT the criterion: a POP of the business
        // topic also consults the retry queue (and the client rewrites the reply topic to the
        // requested one), so "seen from" can name either queue's topic for the same physical
        // revival. What is broker state: the retry topic exists, and the checkpoint the reply
        // carried is addressed inside it. Probed after the ack because the route reaching the name
        // server can take a broker heartbeat, and that wait must not push the ack past the
        // checkpoint's invisibility.
        $check(ba_wait_route($ns, $retryTopic, 30.0),
            'L2 the POP retry topic exists on the broker (the revive service really moved it there)',
            $retryTopic . ' answered from [' . implode(' ', $seenFrom) . ']');

        $settled = new BaEyes();
        ba_watch($v, $group, $topic, BA_OBSERVE_WINDOW_SEC, $settled);
        $check(!$settled->cameBack($dropped['body']), 'L2 the marker-driven retry ack silenced it (no further revival)',
            $settled->summary());
    } catch (Throwable $ex) {
        $check(false, 'L2 aborted', get_class($ex) . ': ' . $ex->getMessage());
    }
}

// ------------------------------------------------------------------ L3
if ($want('l3')) {
    echo PHP_EOL . '--- L3 two pop rounds, one request: entries group by retry@queueId@ckOffset@popTime ---' . PHP_EOL;
    $total = 4;
    $perRound = 2;
    $topic = $prefix . '_BaRounds';
    $group = 'GID_' . $prefix . '_l3';
    $createdTopics[] = $topic;
    $bodies = [];
    for ($i = 0; $i < $total; $i++) {
        $bodies[] = "ba-round-$i";
    }
    try {
        ba_ensure_topic($ns, $topic);
        $brokerName = ba_broker_name($ns, $topic);
        $p = ba_start_producer($ns, 'GID_' . $prefix . '_l3_prod');
        $shutdowns[] = static fn() => $p->shutdown();
        ba_send_all($p, $topic, $bodies);
        $check(true, "L3 send $total messages", $topic);

        [$depth, $ready] = ba_wait_depth($ns, $topic, $total, 30.0);
        $check($ready, "L3 all $total messages are on the queue before popping", "depth=$depth");
        if (!$ready) {
            throw new RuntimeException('queue never filled');
        }

        $v = ba_new_verifier($ns, $brokerName);
        $shutdowns[] = static fn() => $v->shutdown();

        $first = ba_pop($v->inst, $brokerName, $topic, $group, 0, $perRound, 3000);
        $check($first->status === PopStatus::FOUND && count($first->msgFoundList) === $perRound,
            "L3 first POP answers $perRound messages",
            'status=' . $first->status->name . ' got=' . count($first->msgFoundList));
        // The second round asks for 2 more. The first round's messages are invisible for the whole
        // window, so the broker has no choice but to hand out the other two — which is what makes
        // the two rounds distinguishable by checkpoint.
        $second = ba_pop($v->inst, $brokerName, $topic, $group, 0, $perRound, 1000);
        $check($second->status === PopStatus::FOUND && count($second->msgFoundList) === $perRound,
            "L3 second POP answers the other $perRound messages",
            'status=' . $second->status->name . ' got=' . count($second->msgFoundList));

        [$cksA, $problemsA] = ba_parse_cks($first->msgFoundList);
        [$cksB, $problemsB] = ba_parse_cks($second->msgFoundList);
        if ($problemsA !== [] || $problemsB !== [] || count($cksA) !== $perRound || count($cksB) !== $perRound) {
            $check(false, 'L3 both rounds carry parsable checkpoints',
                implode('; ', array_merge($problemsA, $problemsB)) . ' A=' . count($cksA) . ' B=' . count($cksB));
            throw new RuntimeException('unparsable round checkpoints');
        }
        $seenA = ba_body_set($cksA);
        $disjoint = true;
        foreach ($cksB as $c) {
            if (isset($seenA[$c['body']])) {
                $disjoint = false;
            }
        }
        $check($disjoint, 'L3 the two rounds are disjoint batches of one queue',
            'A=' . ba_bodies_of($cksA) . ' B=' . ba_bodies_of($cksB));

        $all = array_merge($cksA, $cksB);
        $body = ba_body($topic, $group, ba_raw_strings($all));
        $keys = ba_distinct_keys($all);
        $keysA = ba_distinct_keys($cksA);
        $keysB = ba_distinct_keys($cksB);
        $shared = array_intersect($keysA, $keysB);
        // What the offline suite cannot produce and this leg can: the rounds really differ. If
        // they shared a merge key, the body would legitimately fold them together and the entry
        // count would prove nothing about the grouping.
        $check($shared === [] && count($keys) >= 2,
            'L3 the two rounds produce different merge keys, so the body cannot collapse them',
            'keys=' . count($keys) . ' shared=' . count($shared) . ' [' . implode(' ', $keys) . ']');
        $acked = ba_acked_offsets($body);
        $check(ba_same_offset_set($acked, ba_offsets_of($all)) && count($body->acks) === count($keys),
            "L3 one body covers all $total offsets exactly once",
            'entries=' . count($body->acks) . ' acked=[' . implode(' ', array_keys($acked))
            . '] want=[' . implode(' ', ba_offsets_of($all)) . ']');

        $code = ba_batch_ack($v->inst, $topic, $group, ba_raw_strings($all));
        $check($code === ResponseCode::SUCCESS, 'L3 one BATCH_ACK_MESSAGE for two rounds answered SUCCESS',
            sprintf('code=%d %d checkpoints -> %d entries', $code, count($all), count($body->acks)));

        $e = new BaEyes();
        ba_watch($v, $group, $topic, BA_OBSERVE_WINDOW_SEC, $e);
        $check($e->allBodies() === [], 'L3 no message of either round revived inside 2.5x the window',
            $e->summary());
        if ($e->problems !== []) {
            echo '     watch notes: ' . implode('; ', $e->problems) . PHP_EOL;
        }
    } catch (Throwable $ex) {
        $check(false, 'L3 aborted', get_class($ex) . ': ' . $ex->getMessage());
    }
}

$cleanup();

echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);
