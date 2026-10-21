<?php

declare(strict_types=1);

/**
 * Offline self-test for the BATCH_ACK_MESSAGE (200151) send path.
 *
 * Everything is asserted on RECORDED state: the primitive is driven through a client double that
 * captures the request it would have sent, and the body is checked as data, never as a timing window.
 * Each positive assertion is paired with a control that goes red when the batch path is broken or
 * falls back to per-message acknowledge, so a toothless test cannot pass.
 *
 * Run: php tests/RunClientBatchAck.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Remoting\Protocol\AckMessageRequestHeader;
use RocketMQ\Remoting\Protocol\BatchAck;
use RocketMQ\Remoting\Protocol\BatchAckBitSet;
use RocketMQ\Remoting\Protocol\BatchAckMessageRequestBody;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;

// ==================================================================== doubles and helpers

/**
 * Client double that records every sync request instead of putting it on the wire, and answers a
 * scripted response code. The offline constructor avoids any name server or route resolution.
 */
final class RecordingBatchClient extends MQClientInstance
{
    /** @var list<array{addr: string, cmd: RemotingCommand}> */
    public array $requests = [];
    public int $replyCode = ResponseCode::SUCCESS;

    public function __construct()
    {
        parent::__construct('batch-ack-test', []);
    }

    protected function invokeSync(string $addr, RemotingCommand $request, ?int $timeoutMillis = null): RemotingCommand
    {
        $this->requests[] = ['addr' => $addr, 'cmd' => $request];
        return RemotingCommand::createResponseCommand($this->replyCode);
    }

    /** @return list<RemotingCommand> */
    public function commandsForCode(int $code): array
    {
        $out = [];
        foreach ($this->requests as $r) {
            if ($r['cmd']->code === $code) {
                $out[] = $r['cmd'];
            }
        }
        return $out;
    }
}

$GROUP = 'GID_BATCH';
$TOPIC = 'TopicBatch';
$BROKER = 'broker-a';
$ADDR = '127.0.0.1:10911';

/**
 * Builds a checkpoint string. The retry flag is derived from the topic by the real encoder, so a
 * ``%RETRY%`` topic yields "1" exactly as on the wire; a normal topic yields "0".
 */
function ck(string $topic, int $queueId, int $ckQueueOffset, int $popTime, int $invisibleTime, int $reviveQid, int $queueOffset): string
{
    return ExtraInfoUtil::buildExtraInfo(
        $ckQueueOffset,
        $popTime,
        $invisibleTime,
        $reviveQid,
        $topic,
        'broker-a',
        $queueId,
        $queueOffset,
    );
}

$checks = 0;
$failures = [];
/** Records one labelled assertion. */
$ck = static function (bool $cond, string $label) use (&$checks, &$failures): void {
    $checks++;
    if (!$cond) {
        $failures[] = $label;
        fwrite(STDOUT, "FAIL: {$label}\n");
    }
};
$ckSame = static function (mixed $expected, mixed $actual, string $label) use (&$checks, &$failures): void {
    $checks++;
    if ($expected !== $actual) {
        $failures[] = $label . ' (expected ' . var_export($expected, true)
            . ', got ' . var_export($actual, true) . ')';
        fwrite(STDOUT, "FAIL: {$label} expected=" . var_export($expected, true)
            . " actual=" . var_export($actual, true) . "\n");
    }
};

// ==================================================================== 1. byte-minimal bit set

{
    // The contract is BYTE-MINIMAL little-endian: length is decided by the highest set bit, never
    // rounded up to a word. A word-padded encoder decodes back to the same bits, so only the exact
    // string can tell them apart -- assert the string, not just the decoded set.
    $ckSame('CQ==', (new BatchAckBitSet([0, 3]))->toBase64(), 'bitset.{0,3}=CQ==');
    $ckSame('AQ==', (new BatchAckBitSet([0]))->toBase64(), 'bitset.{0}=AQ==');
    $ckSame('AAE=', (new BatchAckBitSet([8]))->toBase64(), 'bitset.{8}=AAE=');
    $ckSame('', (new BatchAckBitSet([]))->toBase64(), 'bitset.empty=""');
    // A high isolated bit must not pad to its word: bit 71 -> the highest set byte is index 8, so 9 bytes.
    $b71 = (new BatchAckBitSet([71]))->toBase64();
    $ck(strlen((string)base64_decode($b71, true)) === 9, 'bitset.bit71.is9Bytes.notWordPadded');
    // Round trip: whatever the encoder writes, the reader recovers the identical bit set.
    $round = (new BatchAckBitSet([0, 3, 8, 71]))->toBase64();
    $ckSame([0, 3, 8, 71], BatchAckBitSet::fromBase64($round)->setIndexes(), 'bitset.roundTrip');
    // Control: an absent/empty field is the empty set, not an error.
    $ck(BatchAckBitSet::fromBase64('')->isEmpty(), 'bitset.emptyTextReadsEmpty');
    $ck(BatchAckBitSet::fromBase64(null)->isEmpty(), 'bitset.nullReadsEmpty');
    // A negative index is a client bug and must be rejected.
    $threw = false;
    try {
        (new BatchAckBitSet())->set(-1);
    } catch (\InvalidArgumentException) {
        $threw = true;
    }
    $ck($threw, 'bitset.negativeIndexThrows');
}

// ==================================================================== 2. merge: one key collapses

{
    // Three checkpoints sharing retry@queueId@ckQueueOffset@popTime, offsets 100 / 103 / 100.
    $body = BatchAckMessageRequestBody::fromExtraInfos($GROUP, $TOPIC, [
        ck($TOPIC, 0, 100, 5000, 60000, 0, 100),
        ck($TOPIC, 0, 100, 5000, 60000, 0, 103),
        ck($TOPIC, 0, 100, 5000, 60000, 0, 100),
    ]);
    $ckSame(1, count($body->acks), 'merge.oneEntryPerKey');
    $ckSame($BROKER, $body->brokerName, 'merge.brokerFromFirst');
    $ack = $body->acks[0];
    $ckSame('CQ==', $ack->bitSet->toBase64(), 'merge.bitSetIsByteMinimal');
    // The bits are RELATIVE to the checkpoint; the absolute offsets are recovered by adding startOffset.
    $ckSame([100, 103], $ack->ackedOffsets(), 'merge.bitsAreRelative');
    $ckSame(100, $ack->startOffset, 'merge.startOffsetIsCk');
    $ckSame($GROUP, $ack->consumerGroup, 'merge.group');
    $ckSame($TOPIC, $ack->topic, 'merge.topic');
    $ckSame('0', $ack->retry, 'merge.retryIsStringZero');
}

// ==================================================================== 3. distinct keys -> one entry each, in order

{
    // A different queue, then a different retry flag: three merge keys,
    // emitted in first-seen order. The first entry's fields must belong to the first checkpoint.
    $body = BatchAckMessageRequestBody::fromExtraInfos($GROUP, $TOPIC, [
        ck($TOPIC, 0, 10, 1000, 60000, 0, 12),   // key 0@0@10@1000
        ck($TOPIC, 1, 20, 1000, 60000, 0, 20),   // key 0@1@20@1000 (different queue)
        ck('%RETRY%' . $GROUP, 0, 10, 1000, 60000, 0, 11), // key 1@0@10@1000 (retry flag "1")
        ck($TOPIC, 0, 10, 1000, 60000, 0, 14),   // back to first key -> folds in as another bit
    ]);
    $ckSame(3, count($body->acks), 'merge.distinctKeysAreSeparate');
    $ckSame($TOPIC, $body->acks[0]->topic, 'order.firstSeenIsFirst');
    $ckSame(1, $body->acks[1]->queueId, 'order.secondIsOtherQueue');
    $ckSame('1', $body->acks[2]->retry, 'merge.retryFlagIsStringOne');
    // The fourth info joins the first key (still 3 entries) and adds bit (14-10)=4 alongside (12-10)=2.
    $ckSame('FA==', $body->acks[0]->bitSet->toBase64(), 'merge.laterSameKeyFoldsBit');
}

// ==================================================================== 3b. pop time separates rounds

{
    // Same retry flag, same queue, SAME checkpoint offset, only the pop time differs: two pops of
    // one queue that start at the same offset. Dropping popTime from the merge key would fold them
    // into one entry and acknowledge offsets the second pop never returned -- and because the
    // broker answers SUCCESS either way, only the entry count and the per-entry pop time can see it.
    $body = BatchAckMessageRequestBody::fromExtraInfos($GROUP, $TOPIC, [
        ck($TOPIC, 0, 10, 1000, 60000, 0, 10),   // key 0@0@10@1000
        ck($TOPIC, 0, 10, 2000, 60000, 0, 10),   // key 0@0@10@2000 (only popTime differs)
    ]);
    $ckSame(2, count($body->acks), 'merge.popTimeIsPartOfTheKey');
    // Each entry keeps its own single bit at index 0 -- neither round borrows the other's ack.
    // Read through ?-> so a lost entry reports as a failed check instead of a fatal.
    $roundA = $body->acks[0] ?? null;
    $roundB = $body->acks[1] ?? null;
    $ckSame(1000, $roundA?->popTime, 'merge.firstRoundPopTime');
    $ckSame(2000, $roundB?->popTime, 'merge.secondRoundPopTime');
    $ckSame([10], $roundA?->ackedOffsets(), 'merge.firstRoundOwnBits');
    $ckSame([10], $roundB?->ackedOffsets(), 'merge.secondRoundOwnBits');
    $ckSame('AQ==', $roundB?->bitSet->toBase64(), 'merge.secondRoundBitMinimal');
}

// ==================================================================== 4. compact keys + long-name read

{
    $body = BatchAckMessageRequestBody::fromExtraInfos($GROUP, $TOPIC, [
        ck($TOPIC, 0, 7, 900, 30000, 3, 7),
    ]);
    $entry = $body->acks[0]->toDict();
    $ckSame(
        ['c', 't', 'r', 'so', 'q', 'rq', 'pt', 'it', 'b'],
        array_keys($entry),
        'keys.compactFamily',
    );
    $ckSame('3', (string)$entry['rq'], 'keys.reviveQueueId');
    // Reading back accepts the long names a broker-side tool may emit.
    $long = [
        'consumerGroup' => $GROUP,
        'topic' => $TOPIC,
        'retry' => '2',
        'startOffset' => 5,
        'queueId' => 9,
        'reviveQueueId' => 1,
        'popTime' => 77,
        'invisibleTime' => 88,
        'bitSet' => (new BatchAckBitSet([0, 2]))->toBase64(),
    ];
    $back = BatchAck::fromDict($long);
    $ckSame($GROUP, $back->consumerGroup, 'read.longNameGroup');
    $ckSame('2', $back->retry, 'read.longNameRetryString');
    $ckSame([0, 2], $back->bitSet->setIndexes(), 'read.longNameBitSet');
    // The compact spelling wins when both are present.
    $both = $long;
    $both['c'] = 'COMPACT';
    $ckSame('COMPACT', BatchAck::fromDict($both)->consumerGroup, 'read.compactWinsOverLong');
}

// ==================================================================== 5. primitive: one headerless request

{
    $client = new RecordingBatchClient();
    $rc = $client->batchAckMessage($GROUP, $TOPIC, [
        ck($TOPIC, 0, 100, 5000, 60000, 0, 100),
        ck($TOPIC, 0, 100, 5000, 60000, 0, 103),
    ], 3000, $ADDR);

    // positive: exactly one 200151 request was sent.
    $ckSame(1, count($client->commandsForCode(RequestCode::BATCH_ACK_MESSAGE)), 'primitive.oneBatchRequest');
    // control: it did NOT fall back to per-message ACK_MESSAGE.
    $ckSame(0, count($client->commandsForCode(RequestCode::ACK_MESSAGE)), 'primitive.noSingleAcksSent');
    // control: the broker code is returned verbatim.
    $ckSame(ResponseCode::SUCCESS, $rc, 'primitive.returnsSuccessCode');

    $cmd = $client->commandsForCode(RequestCode::BATCH_ACK_MESSAGE)[0];
    // positive: the request carries NO custom header.
    $ck($cmd->customHeader === null, 'primitive.noCustomHeader');
    // control: a real single-ack on the same double DOES build a custom header, so the null above is
    // the batch contract, not the double failing to attach headers at all.
    $client->ackMessage($GROUP, $TOPIC, 0, ck($TOPIC, 0, 7, 900, 30000, 0, 7), 7, $BROKER, 3000, $ADDR);
    $acks = $client->commandsForCode(RequestCode::ACK_MESSAGE);
    $ck($acks[0]->customHeader instanceof AckMessageRequestHeader, 'control.singleAckHasHeader');
    $ckSame($ADDR, $client->requests[0]['addr'], 'primitive.usesGivenAddr');

    // The recorded body parses to the documented shape.
    $decoded = json_decode((string)$cmd->body, true);
    $ck(is_array($decoded), 'primitive.bodyIsJson');
    $ckSame($BROKER, $decoded['brokerName'] ?? null, 'primitive.bodyBrokerName');
    $ckSame(1, count($decoded['acks'] ?? []), 'primitive.bodyOneEntry');
    $ckSame('CQ==', $decoded['acks'][0]['b'] ?? null, 'primitive.bodyBitMinimal');
}

// ==================================================================== 6. non-success code passthrough + empty throw

{
    $client = new RecordingBatchClient();
    $client->replyCode = ResponseCode::SYSTEM_ERROR;
    $rc = $client->batchAckMessage($GROUP, $TOPIC, [
        ck($TOPIC, 0, 1, 2, 3, 0, 1),
    ], 3000, $ADDR);
    // control: the primitive surfaces whatever the broker answered, it does not swallow it.
    $ckSame(ResponseCode::SYSTEM_ERROR, $rc, 'primitive.passthroughErrorCode');

    $threw = false;
    try {
        (new RecordingBatchClient())->batchAckMessage($GROUP, $TOPIC, [], 3000, $ADDR);
    } catch (MQClientException) {
        $threw = true;
    }
    $ck($threw, 'primitive.emptyListThrows');
}

// ==================================================================== summary

if ($failures === []) {
    fwrite(STDOUT, "ALL TESTS PASSED ({$checks} checks)\n");
    exit(0);
}
fwrite(STDOUT, "TESTS FAILED: " . count($failures) . " / {$checks}\n");
foreach ($failures as $f) {
    fwrite(STDOUT, "  - {$f}\n");
}
exit(1);
