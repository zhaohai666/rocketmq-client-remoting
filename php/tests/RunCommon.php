<?php

declare(strict_types=1);

/**
 * Common layer pure PHP assert-style self-test (no phpunit dependency).
 *
 * Run: php tests/RunCommon.php
 * On full success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Common\BoundaryType;
use RocketMQ\Common\FilterAPI;
use RocketMQ\Common\HandleV1;
use RocketMQ\Common\InnerIdGenerator;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageAccessor;
use RocketMQ\Common\MessageBatch;
use RocketMQ\Common\MessageClientIdSetter;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\CompressionCodec;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MessageSysFlag;
use RocketMQ\Common\MessageType;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PullSysFlag;
use RocketMQ\Common\PermName;
use RocketMQ\Common\RecallMessageHandle;
use RocketMQ\Common\SubscriptionData;
use RocketMQ\Common\TopicConfig;
use RocketMQ\Common\TopicFilterType;
use RocketMQ\Common\TopicValidator;
use RocketMQ\Common\UtilAll;

final class RunCommon
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    public function check(bool $cond, string $label): void
    {
        if ($cond) {
            $this->passed++;
        } else {
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$label}\n");
        }
    }

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        if ($expected === $actual) {
            $this->passed++;
        } else {
            $detail = sprintf('%s\n  expected: %s\n  actual:   %s', $label, var_export($expected, true), var_export($actual, true));
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$detail}\n");
        }
    }

    /**
     * @param callable(): mixed $fn
     * @param class-string<\Throwable> $exceptionClass
     */
    public function checkThrows(callable $fn, string $exceptionClass, string $label): void
    {
        try {
            $fn();
            $this->check(false, "{$label} (no exception thrown)");
        } catch (\Throwable $e) {
            $this->check($e instanceof $exceptionClass, sprintf('%s (got %s: %s)', $label, get_class($e), $e->getMessage()));
        }
    }

    public function summary(): int
    {
        $total = $this->passed + count($this->failures);
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("FAILED: %d/%d checks\n", count($this->failures), $total);
        return 1;
    }
}

$T = new RunCommon();

// ============================================================ UtilAll
$T->checkSame(2598919, UtilAll::wireStringHash('TagA'), 'wireStringHash TagA=2598919');
$T->checkSame(2598920, UtilAll::wireStringHash('TagB'), 'wireStringHash TagB=2598920');
$T->checkSame(80, UtilAll::wireStringHash('P'), 'wireStringHash P=80');
$T->checkSame(2545, UtilAll::wireStringHash('PA'), 'wireStringHash PA=2545');
$T->checkSame(42, UtilAll::wireStringHash('*'), 'wireStringHash *=42');
$T->checkSame(0, UtilAll::wireStringHash(''), 'wireStringHash ""=0');

$hex = UtilAll::bytes2String("\xde\xad\xbe\xef");
$T->checkSame('DEADBEEF', $hex, 'bytes2String uppercase hex');
$T->checkSame("\xde\xad\xbe\xef", UtilAll::string2Bytes('deadBEEF'), 'string2Bytes case-insensitive');
$T->checkSame(null, UtilAll::string2Bytes(''), 'string2Bytes("")=null');
$T->checkSame(null, UtilAll::string2Bytes(null), 'string2Bytes(null)=null');
$T->checkSame(20, strlen(UtilAll::offset2Filename(123)), 'offset2Filename length 20');
$T->checkSame('00000000000000000123', UtilAll::offset2Filename(123), 'offset2Filename zero-padded');
$T->checkSame('-', UtilAll::timeToHumanString(0), 'timeToHumanString(0)="-"');
$T->check(UtilAll::isBlank(null) && UtilAll::isBlank('  ') && !UtilAll::isBlank('x') && !UtilAll::isNotBlank(' '), 'isBlank/isNotBlank');
$T->check(UtilAll::isIpv4('10.0.0.1') && !UtilAll::isIpv4('::1') && !UtilAll::isIpv4('999.1.1.1'), 'isIpv4');
// BODYCRC holds the CRC-32 of the body with the high (sign) bit dropped, so this helper masks with
// 0x7FFFFFFF rather than returning the whole 32-bit value.
$T->checkSame(907060870, UtilAll::crc32('hello'), 'crc32("hello")=907060870, sign bit already clear');
$T->checkSame(891568578, UtilAll::crc32('abc'), 'crc32("abc")=891568578, sign bit already clear');
$T->checkSame(222957957, UtilAll::crc32('hello world'), 'crc32("hello world")=222957957, sign bit already clear');
$T->checkSame(1756872259, UtilAll::crc32('a'), 'crc32("a")=1756872259, sign bit dropped from 3904355907');
$T->checkSame(297861834, UtilAll::crc32('the quick brown fox'), 'crc32("the quick brown fox")=297861834, sign bit dropped from 2445345482');
$T->checkSame(1937991511, UtilAll::crc32('ba-all-0'), 'crc32("ba-all-0")=1937991511, sign bit dropped from 4085475159');
$T->checkSame(
    1756872259,
    MessageDecoder::crc32('a'),
    'MessageDecoder::crc32 follows the same sign-bit-dropped contract'
);
// Control pair: the built-in and this helper differ on exactly one bit, and only on bodies whose CRC
// has the sign bit set — this pins the mask rather than letting the two agree by accident.
foreach (['', 'a', 'abc', 'hello', 'hello world', 'ba-all-0', 'the quick brown fox'] as $crcBody) {
    $whole = crc32($crcBody) & 0xFFFFFFFF;
    $dropped = UtilAll::crc32($crcBody);
    $T->checkSame(
        $whole >= 0x80000000 ? 0x80000000 : 0,
        $whole ^ $dropped,
        sprintf('crc32(%s) differs from the whole 32-bit CRC only in the sign bit', var_export($crcBody, true))
    );
}
$T->check(
    UtilAll::crc32('a') !== (crc32('a') & 0xFFFFFFFF),
    'crc32("a") is not the whole 32-bit CRC'
);
$T->checkSame(
    0x80000000,
    (crc32('a') & 0xFFFFFFFF) ^ UtilAll::crc32('a'),
    'crc32("a") and the whole 32-bit CRC differ in the sign bit alone'
);
$T->check(UtilAll::currentTimeMillis() > 1600000000000, 'currentTimeMillis is in milliseconds');
$T->checkSame('127.0.0.1', UtilAll::string2UnicodeShift('127.0.0.1', 3), 'string2UnicodeShift returns as-is');

// ============================================================ MixAll
$T->checkSame('%RETRY%G1', MixAll::getRetryTopic('G1'), 'getRetryTopic');
$T->check(MixAll::isRetryTopic('%RETRY%G1') && !MixAll::isRetryTopic('T'), 'isRetryTopic');
$T->checkSame('%DLQ%G1', MixAll::getDlqTopic('G1'), 'getDlqTopic');
$T->checkSame('T', MixAll::resetRetryAndDlqTopic('%RETRY%T'), 'resetRetryAndDlqTopic retry');
$T->checkSame('T', MixAll::resetRetryAndDlqTopic('%DLQ%T'), 'resetRetryAndDlqTopic dlq');
$T->checkSame('T', MixAll::resetRetryAndDlqTopic('T'), 'resetRetryAndDlqTopic unchanged');
$T->checkSame('b1:10909', MixAll::brokerVipChannel(true, 'b1:10911'), 'brokerVipChannel -2');
$T->checkSame('b1:10911', MixAll::brokerVipChannel(false, 'b1:10911'), 'brokerVipChannel off');
$T->checkSame('no-port', MixAll::brokerVipChannel(true, 'no-port'), 'brokerVipChannel no port unchanged');
$T->checkSame('cluster_REPLY_TOPIC', MixAll::getReplyTopic('cluster'), 'getReplyTopic');
$T->check(MixAll::isSysTopic('rmq_sys_x') && !MixAll::isSysTopic('x'), 'isSysTopic');
$T->check(MixAll::isLmq('%LMQ%t') && !MixAll::isLmq('t'), 'isLmq');
$T->check(MixAll::isSysConsumerGroup('CID_RMQ_SYS_x') && !MixAll::isSysConsumerGroup('G'), 'isSysConsumerGroup');
$T->check(MixAll::isPredefinedGroup('DEFAULT_CONSUMER') && !MixAll::isPredefinedGroup('G'), 'isPredefinedGroup');
$T->checkSame('ip@inst@unit', MixAll::buildMqClientId('ip', 'inst', 'unit'), 'buildMqClientId unit');
$T->checkSame('ip@inst@unit@STREAM', MixAll::buildMqClientId('ip', 'inst', 'unit', true), 'buildMqClientId STREAM');
$T->checkSame('ip@inst', MixAll::buildMqClientId('ip', 'inst'), 'buildMqClientId minimal');
$T->check(MixAll::changeInstanceNameToPid('DEFAULT') !== 'DEFAULT', 'changeInstanceNameToPid DEFAULT swaps in pid#nano');
$T->checkSame('myinst', MixAll::changeInstanceNameToPid('myinst'), 'changeInstanceNameToPid non-DEFAULT unchanged');
$T->checkSame('%ns%%inst', MixAll::compareAndIncreaseNamespace('inst', 'ns'), 'compareAndIncreaseNamespace');
$T->checkSame('%ns%%inst', MixAll::compareAndIncreaseNamespace('%ns%%inst', 'ns'), 'compareAndIncreaseNamespace idempotent');
$T->check(str_starts_with(MixAll::createUniqName('p'), 'p') && strlen(MixAll::createUniqName('p')) === 33, 'createUniqName 32hex');

// properties <-> string round-trip (null values skipped, ordering)
$props = ['b' => '2', 'a' => '1', 'c' => null];
$T->checkSame("b=2\na=1\n", MixAll::properties2String($props), 'properties2String skips null values and keeps order');
$T->checkSame("a=1\nb=2\n", MixAll::properties2String($props, true), 'properties2String sorted');
$roundtrip = MixAll::string2Properties(MixAll::properties2String(['k1' => 'v1', 'k2' => 'v 2', 'k3' => 'x=y']));
$T->checkSame(['k1' => 'v1', 'k2' => 'v 2', 'k3' => 'x=y'], $roundtrip, 'properties round-trip');
// properties text parsing semantics (comment lines / separators / continuations / whitespace)
$T->checkSame(['a' => '1', 'b' => '2'], MixAll::string2Properties("# comment\n!bang\na=1\n\nb=2"), 'comment lines and blank lines skipped');
$T->checkSame(['key' => 'va lue'], MixAll::string2Properties("key: va lue"), ': separator');
$T->checkSame(['key' => 'value'], MixAll::string2Properties("key value"), 'whitespace separator');
$T->checkSame(['a' => '1', 'b' => '2'], MixAll::string2Properties("a=\\\n  1\nb=2"), 'trailing-backslash line continuation');
$T->checkSame(['a' => 'tail '], MixAll::string2Properties("a= tail "), 'trailing whitespace in value kept');
$T->checkSame([], MixAll::string2Properties(null), 'string2Properties(null)');

// ============================================================ Message / properties / delay level
$msg = new Message('TopicTest', 'body-bytes', 'TagA', 'key-1', 7);
$T->checkSame('TopicTest', $msg->getTopic(), 'Message topic');
$T->checkSame('body-bytes', $msg->getBody(), 'Message body');
$T->checkSame('TagA', $msg->getTags(), 'Message constructor writes TAGS');
$T->checkSame('key-1', $msg->getKeys(), 'Message constructor writes KEYS');
$T->checkSame(7, $msg->getFlag(), 'Message flag');
$T->checkSame('', (new Message('t'))->getBody(), 'Message body defaults to empty string');
$T->checkSame(null, (new Message('t'))->getTags(), 'Message tags default to null');

$msg->setDelayTimeLevel(3);
$T->checkSame('3', $msg->getDelayTimeLevel(), 'delayTimeLevel stored as string "3"');
$T->checkSame('3', $msg->getProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL), 'DELAY property key');
$msg->setWaitStoreMsgOk(false);
$T->checkSame('false', $msg->getWaitStoreMsgOk(), 'WAIT=false');
$T->check(!Message::isWaitStoreMsgOk($msg), 'isWaitStoreMsgOk explicit false');
$T->check(Message::isWaitStoreMsgOk(new Message('t')), 'isWaitStoreMsgOk defaults to true');
$msg->setUserProperty('u1', 'v1');
$T->checkSame('v1', $msg->getUserProperty('u1'), 'userProperty');
$T->checkSame('v1', $msg->getProperty('u1'), 'getProperty');
$msg->removeProperty('u1');
$T->checkSame(null, $msg->getProperty('u1'), 'removeProperty');
$msg->clearProperty();
$T->checkSame([], $msg->getProperties(), 'clearProperty empties');
$msg->putProperty('p', '1');
$T->checkSame('1', $msg->getProperty('p'), 'putProperty');
$msg->setTransactionId('tid');
$T->checkSame('tid', $msg->getTransactionId(), 'transactionId');

// MessageQueue
$mq = new MessageQueue('TopicTest', 'broker-a', 3);
$T->checkSame('3', $mq->getQueueIdStr(), 'MessageQueue.getQueueIdStr');
$T->check($mq->equals(new MessageQueue('TopicTest', 'broker-a', 3)), 'MessageQueue.equals');
$T->check(!$mq->equals(new MessageQueue('TopicTest', 'broker-a', 4)), 'MessageQueue.equals not equal');
$T->check(MessageQueue::wrapInt32(0x7FFFFFFF) === 2147483647 && MessageQueue::wrapInt32(0x80000000) === -2147483648, 'wrapInt32 32-bit wraparound');

// ============================================================ MessageClientIdSetter
$u1 = new Message('t');
MessageClientIdSetter::setUniqId($u1);
$uniq1 = $u1->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
$T->check(is_string($uniq1) && strlen($uniq1) === 32, 'uniqId is 32 hex chars');
$T->checkSame($uniq1, MessageClientIdSetter::getUniqId($u1), 'set/get uniqId consistent');
$u2 = new Message('t');
MessageClientIdSetter::setUniqId($u2);
MessageClientIdSetter::setUniqId($u2); // idempotent: existing value is not overwritten
$uniq2 = $u2->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
$T->check($uniq1 !== $uniq2, 'two generated uniqIds differ (uniqueness)');
MessageClientIdSetter::setUniqId($u2);
$T->checkSame($uniq2, $u2->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX), 'setUniqId does not overwrite an existing value');
$T->checkSame(null, MessageClientIdSetter::getUniqId(null), 'getUniqId(null)');
MessageClientIdSetter::setUniqId(null); // must not throw
$T->checkSame(32, strlen(InnerIdGenerator::createUniqId()), 'InnerIdGenerator generates 32 chars directly');
$T->check(ctype_xdigit(MessageClientIdSetter::createUniqId()), 'uniqId is all hex characters');

// ============================================================ MessageDecoder: msgId encode/decode round-trip
$addrBytes = inet_pton('192.168.1.10') . pack('N', 10911);
$offset = 123456789;
$msgId = MessageDecoder::createMessageId($addrBytes, $offset);
$T->checkSame('C0A8010A00002A9F' . sprintf('%016X', $offset), $msgId, 'createMessageId 32 hex chars');
[$ip, $port, $off] = MessageDecoder::decodeMessageId($msgId);
$T->checkSame('192.168.1.10', $ip, 'decodeMessageId ip');
$T->checkSame(10911, $port, 'decodeMessageId port');
$T->checkSame(123456789, $off, 'decodeMessageId offset');

// UtilAll HEX round-trip
$T->checkSame('DEADBEEF0102', UtilAll::bytes2String(UtilAll::string2Bytes('deadbeef0102')), 'UtilAll hex round-trip');

// property string <-> map
$T->checkSame(['A' => '1', 'B' => '2'], MessageDecoder::string2MessageProperties("A\x011\x02B\x012\x02"), 'string2MessageProperties');
$T->checkSame("A\x011\x02B\x012\x02", MessageDecoder::messageProperties2String(['A' => '1', 'B' => '2']), 'messageProperties2String');
$T->checkSame([], MessageDecoder::string2MessageProperties(''), 'empty property string -> empty map');

// 17-segment format: encodeMessageExt -> decodeMessage full-field round-trip
$ext = new MessageExt('TopicTest', 'hello world', 'TagA', 'k1');
$ext->setQueueId(2);
$ext->setQueueOffset(1000);
$ext->setCommitLogOffset(77777);
$ext->setSysFlag(MessageSysFlag::TRANSACTION_PREPARED_TYPE);
$ext->setBornTimestamp(1700000001000);
$ext->setBornHost('10.0.0.2');
$ext->bornHostPort = 58012;
$ext->setStoreTimestamp(1700000002000);
$ext->setStoreHost('10.0.0.3');
$ext->storeHostPort = 10911;
$ext->setReconsumeTimes(1);
$ext->setPreparedTransactionOffset(-1);
$ext->setBodyCrc(MessageDecoder::crc32('hello world'));
$ext->setFlag(9);
$ext->setStoreSize(0);
MessageAccessor::putProperty($ext, 'user.key', 'user.value');

$encoded = MessageDecoder::encodeMessageExt($ext);
$decoded = MessageDecoder::decodeMessage($encoded);
$T->check($decoded !== null, 'decodeMessage returns non-null');
if ($decoded !== null) {
    $T->checkSame('TopicTest', $decoded->getTopic(), 'decode topic');
    $T->checkSame('hello world', $decoded->getBody(), 'decode body');
    $T->checkSame('TagA', $decoded->getTags(), 'decode TAGS');
    $T->checkSame(2, $decoded->getQueueId(), 'decode queueId');
    $T->checkSame(1000, $decoded->getQueueOffset(), 'decode queueOffset');
    $T->checkSame(77777, $decoded->getCommitLogOffset(), 'decode commitLogOffset');
    $T->checkSame(MessageSysFlag::TRANSACTION_PREPARED_TYPE, $decoded->getSysFlag(), 'decode sysFlag');
    $T->checkSame(1700000001000, $decoded->getBornTimestamp(), 'decode bornTimestamp');
    $T->checkSame('10.0.0.2', $decoded->getBornHost(), 'decode bornHost');
    $T->checkSame(58012, $decoded->bornHostPort, 'decode bornHostPort');
    $T->checkSame('10.0.0.2:58012', $decoded->getBornHostString(), 'bornHostString');
    $T->checkSame('10.0.0.3', $decoded->getStoreHost(), 'decode storeHost');
    $T->checkSame(1, $decoded->getReconsumeTimes(), 'decode reconsumeTimes');
    $T->checkSame(9, $decoded->getFlag(), 'decode flag');
    $T->checkSame('user.value', $decoded->getUserProperty('user.key'), 'decode user property');
    // msgId = storeHost(ip+port) + commitLogOffset, and with isClient the offsetMsgId equals it
    $expectedMsgId = MessageDecoder::createMessageId(inet_pton('10.0.0.3') . pack('N', 10911), 77777);
    $T->checkSame($expectedMsgId, $decoded->getMsgId(), 'decode msgId = storeHost+offset');
    $T->checkSame($expectedMsgId, $decoded->getOffsetMsgId(), 'offsetMsgId equals msgId (isClient)');
    [$dip, $dport, $doff] = MessageDecoder::decodeMessageId((string) $decoded->getMsgId());
    $T->checkSame(['10.0.0.3', 10911, 77777], [$dip, $dport, $doff], 'offsetMsgId parseAddr/parseOffset round-trip');
    $T->checkSame(222957957, $decoded->getBodyCrc(), 'bodyCrc round-trips the sign-bit-dropped CRC');
    $T->checkSame(
        MessageDecoder::crc32((string) $decoded->getBody()),
        $decoded->getBodyCrc(),
        'bodyCrc verifies consistent'
    );
}

// Unknown magic -> null (decode-returns-null semantics)
$bad = $encoded;
$bad[5] = "\x00";
$T->checkSame(null, MessageDecoder::decodeMessage($bad), 'unknown magic decodes to null');

// Truncated buffer -> null (exception swallowed, returns null)
$T->checkSame(null, MessageDecoder::decodeMessage(substr($encoded, 0, 30)), 'truncated input decodes to null');

// readBody=false -> body is null
$T->checkSame(null, MessageDecoder::decodeMessage($encoded, readBody: false)?->getBody(), 'readBody=false yields null body');

// checkCrc=true against a BODYCRC stamped with the sign-bit-dropped CRC: a frame whose body CRC has the
// high bit set must still decode, and a whole batch of such frames must yield every frame.
$crcFrames = [];
for ($i = 0; count($crcFrames) < 8; $i++) {
    $candidate = sprintf('msg-body-%d', $i);
    // Keep only bodies whose whole 32-bit CRC has the sign bit set: those are exactly the ones a
    // comparison against the unmasked value would wrongly reject.
    if ((crc32($candidate) & 0x80000000) === 0) {
        continue;
    }
    $crcExt = new MessageExt('TopicCrc', $candidate, 'TagA');
    $crcExt->setQueueId(0);
    $crcExt->setQueueOffset($i);
    $crcExt->setCommitLogOffset(900000 + $i);
    $crcExt->setBodyCrc(UtilAll::crc32($candidate));
    $crcFrames[$candidate] = MessageDecoder::encodeMessageExt($crcExt);
}
$T->checkSame(8, count($crcFrames), 'collected 8 bodies whose CRC has the sign bit set');

$crcStream = '';
foreach ($crcFrames as $candidateBody => $frame) {
    $checked = MessageDecoder::decodeMessage($frame, checkCrc: true);
    $T->check($checked !== null, sprintf('checkCrc=true keeps the frame of %s', var_export($candidateBody, true)));
    $T->checkSame(
        $candidateBody,
        $checked?->getBody(),
        sprintf('checkCrc=true returns the payload of %s', var_export($candidateBody, true))
    );
    $crcStream .= $frame;
}

$streamFrames = MessageDecoder::decodeMessages($crcStream);
$T->checkSame(8, count($streamFrames), 'the multi-frame stream decodes into all 8 frames');
$T->checkSame(
    array_keys($crcFrames),
    array_map(static fn (MessageExt $m): string => (string) $m->getBody(), $streamFrames),
    'the 8 frames come back in order with their payloads intact'
);

// The CRC check is really applied: stamping BODYCRC with the unmasked value must be rejected.
$uncrcExt = new MessageExt('TopicCrc', 'a', 'TagA');
$uncrcExt->setBodyCrc(crc32('a') & 0xFFFFFFFF);
$T->checkSame(
    null,
    MessageDecoder::decodeMessage(MessageDecoder::encodeMessageExt($uncrcExt), checkCrc: true),
    'an unmasked BODYCRC is rejected by checkCrc=true'
);
$T->checkSame(
    'a',
    MessageDecoder::decodeMessage(MessageDecoder::encodeMessageExt($uncrcExt), checkCrc: false)?->getBody(),
    'the same frame decodes when checkCrc is off'
);

// 6-segment lightweight format: batch
$bmsgs = [new Message('T', 'b1', 'TagA'), new Message('T', 'b2', null, 'k2')];
$bmsgs[0]->setFlag(3);
$batchBody = MessageDecoder::encodeMessages($bmsgs);
$roundtripMsgs = MessageDecoder::decodeBatchMessages($batchBody);
$T->checkSame(2, count($roundtripMsgs), 'decodeBatchMessages count');
$T->checkSame('b1', $roundtripMsgs[0]->getBody() ?? null, 'batch first message body');
$T->checkSame('TagA', $roundtripMsgs[0]->getTags(), 'batch first message TAGS');
$T->checkSame(3, $roundtripMsgs[0]->getFlag(), 'batch first message flag');
$T->checkSame('k2', $roundtripMsgs[1]->getKeys(), 'batch second message KEYS');
$T->checkSame(2, MessageDecoder::countInnerMsgNum($batchBody), 'countInnerMsgNum');

// MessageBatch.generateFromList
$batch = MessageBatch::generateFromList($bmsgs);
$T->checkSame('T', $batch->getTopic(), 'MessageBatch topic takes the first message');
$T->check(MessageBatch::isWaitStoreMsgOk($batch), 'MessageBatch WAIT defaults to true');
$T->checkSame($batchBody, $batch->getBody(), 'MessageBatch body is the encodeMessages result');
$T->checkThrows(
    static fn () => MessageBatch::generateFromList([new Message('%RETRY%G'), new Message('%RETRY%G')]),
    \InvalidArgumentException::class,
    'batch rejects retry topics'
);
$T->checkThrows(
    static fn () => MessageBatch::generateFromList([]),
    \InvalidArgumentException::class,
    'batch rejects an empty list'
);
$delayed = new Message('T');
$delayed->setDelayTimeLevel(1);
$T->checkThrows(
    static fn () => MessageBatch::generateFromList([$delayed]),
    \InvalidArgumentException::class,
    'batch rejects delayed messages'
);

// zlib compression path: type bits 0 normalized to ZLIB per the legacy semantics
$compressed = MessageDecoder::decompressBody(gzcompress('payload-data'), 0);
$T->checkSame('payload-data', $compressed, 'normalizeCompressionType 0 -> ZLIB');
$T->checkSame('xyz', MessageDecoder::decompressBody(gzcompress('xyz'), MessageSysFlag::ZLIB_TYPE), 'ZLIB decompression');
$T->checkThrows(
    static fn () => MessageDecoder::decompressBody('x', 5),
    \RuntimeException::class,
    'unknown compression type throws RuntimeException'
);
// Compressed-message decode: sysFlag carries COMPRESSED + ZLIB type bits
$cext = new MessageExt('TopicC', 'plain', 'TagC');
$cext->setSysFlag(MessageSysFlag::setCompressionType(
    MessageSysFlag::COMPRESSED_FLAG,
    MessageSysFlag::ZLIB_TYPE
));
$cenc = MessageDecoder::encodeMessageExt($cext, true);
$cdec = MessageDecoder::decodeMessage($cenc);
$T->check($cdec !== null && $cdec->getBody() === 'plain', 'compressed message decodes back to the original body');
$T->check($cdec !== null && MessageSysFlag::isCompressed($cdec->getSysFlag()) === false, 'COMPRESSED_FLAG cleared after decode');

// LZ4 Frame (standard LZ4 Frame spec; the block layer is its inner layer):
// xxhash32 official-vector anchoring + complete frames produced by an external standard
// LZ4 implementation + Frame round-trips + overlapping matches
$T->checkSame(0x02CC5D05, CompressionCodec::xxh32(''), 'xxh32("") official vector');
$T->checkSame(0x32D153FF, CompressionCodec::xxh32('abc'), 'xxh32("abc") official vector');
// Frames produced by an external standard LZ4 Frame implementation (decompression anchor;
// HC is byte two of xxh32(header)>>8):
//   'abc' => magic|FLG=0x68|BD=0x40|C.Size=3|HC=0x87|raw block(0x80000003)|'abc'|EndMark
$lz4framePy = hex2bin('04224d1868400300000000000000870300008061626300000000');
$T->checkSame('abc', CompressionCodec::lz4DecompressFrame($lz4framePy), 'LZ4 Frame from external implementation decompresses');
// 'hello world ' x40: compressed block (bare block shape, deterministic encoding of the same input)
$lz4framePy2 = hex2bin('04224d186840e0010000000000009b17000000cf68656c6c6f20776f726c64200c00ffbd506f726c642000000000');
$T->checkSame(str_repeat('hello world ', 40), CompressionCodec::lz4DecompressBlock(hex2bin('cf68656c6c6f20776f726c64200c00ffbd506f726c6420')), 'LZ4 bare block long-string decompression');
$T->checkSame(str_repeat('hello world ', 40), CompressionCodec::lz4DecompressFrame($lz4framePy2), 'LZ4 Frame long string from external implementation decompresses');
// Frame round-trips (covering >64KB multi-block, RLE, raw-block fallback)
$lz4FrameSamples = [
    '', 'a', 'abcabcabcabc',
    str_repeat('hello world ', 40),
    str_repeat('x', 300000),                       // RLE-shaped, block compression handles it
    str_repeat('0123456789', 8192) . 'tail',       // 80KB+ => multi-block
];
foreach ($lz4FrameSamples as $idx => $sample) {
    $enc = CompressionCodec::lz4CompressFrame($sample);
    $T->checkSame($sample, CompressionCodec::lz4DecompressFrame($enc), "LZ4 Frame round-trip #$idx (len=" . strlen($sample) . ')');
}
// Bad HC must throw (prevents misreading other byte streams as an LZ4 Frame)
$T->checkThrows(
    static fn () => CompressionCodec::lz4DecompressFrame(hex2bin('04224d1868400300000000000000880300008061626300000000')),
    \RuntimeException::class,
    'LZ4 Frame bad HC throws'
);
$lz4samples = [
    '',
    'a',
    'ab',
    str_repeat('hello world ', 40),
    str_repeat('x', 300000),            // long RLE (spanning multiple literal-run expansions)
    str_repeat('0123456789', 8192) . uniqid('', true), // high repetition + unique tail
    random_bytes_wellknown(),
];
/** Deterministic pseudo-random (avoids random_bytes changing each call) */
function random_bytes_wellknown(): string
{
    $out = '';
    $s = 12345;
    for ($i = 0; $i < 100000; $i++) {
        $s = ($s * 1103515245 + 12345) & 0x7FFFFFFF;
        $out .= chr(0x20 + ($s % 96)); // printable range, creating matchable repetition
    }
    return $out;
}
foreach ($lz4samples as $idx => $sample) {
    $enc = CompressionCodec::lz4CompressBlock($sample);
    $T->checkSame($sample, CompressionCodec::lz4DecompressBlock($enc), "LZ4 round-trip #$idx (len=" . strlen($sample) . ')');
}
// Overlapping match (off < matchLen, LZ4's classic RLE scenario): decompression must copy byte by byte
// token 0x1F: litLen=1 'A', off=1, ml bits=15+extension 0 => matchLen=19
$overlap = CompressionCodec::lz4DecompressBlock(hex2bin('1F4101000'. '0'));
$T->checkSame(str_repeat('A', 20), $overlap, 'LZ4 overlapping match decompression');
$T->checkThrows(
    static fn () => CompressionCodec::lz4DecompressBlock(hex2bin('306162630000')),
    \RuntimeException::class,
    'LZ4 zero offset throws'
);

// ZSTD Raw/RLE frames: one complete standard small frame (magic/header/RAW block) + round-trip + RLE
$zstdNode = hex2bin('28b52ffde01600000000000000b100007a7374642d7261772d6672616d652d7061796c6f6164');
$T->checkSame('zstd-raw-frame-payload', CompressionCodec::zstdDecompressFrame($zstdNode), 'ZSTD standard frame decompression');
$T->checkSame($zstdNode, CompressionCodec::zstdCompressRaw('zstd-raw-frame-payload'), 'ZSTD compression byte-for-byte = fixed vector');
$zstdRle = CompressionCodec::zstdCompressRaw(str_repeat('Z', 500000)); // 500KB of all Z => RLE block
$T->check(strlen($zstdRle) < 5000, 'ZSTD RLE compresses long runs effectively');
$T->checkSame(str_repeat('Z', 500000), CompressionCodec::zstdDecompressFrame($zstdRle), 'ZSTD RLE round-trip');
$zstdBig = CompressionCodec::zstdCompressRaw(str_repeat('chunk-', 60000)); // >128KB => multiple Raw blocks
$T->checkSame(str_repeat('chunk-', 60000), CompressionCodec::zstdDecompressFrame($zstdBig), 'ZSTD multi-block frame round-trip');
// Compressed block (type=2): throws explicitly, never passes a compressed stream through as body
// magic + header(FCS=8B) + block header 04 00 00 (last=0, type=2=Compressed, size=0)
$compFrame = pack('V', 0xFD2FB528) . chr(0xE0) . pack('P', 0) . chr(0x04) . chr(0x00) . chr(0x00) . "\x01\x02";
$T->checkThrows(
    static fn () => CompressionCodec::zstdDecompressFrame($compFrame),
    \RuntimeException::class,
    'ZSTD Compressed block throws'
);

// MessageDecoder wiring: LZ4/ZSTD decompress via the public decompressBody entry; Producer-side compressBody shares the source
$T->checkSame('via-decoder', MessageDecoder::decompressBody(
    CompressionCodec::lz4CompressFrame('via-decoder'),
    MessageSysFlag::LZ4_TYPE
), 'MessageDecoder LZ4 branch');
$T->checkSame('via-decoder', MessageDecoder::decompressBody(
    CompressionCodec::zstdCompressRaw('via-decoder'),
    MessageSysFlag::ZSTD_TYPE
), 'MessageDecoder ZSTD branch');

// ZSTD unified entry (CLI first / Raw fallback): with a zstd CLI present, verify the real
// compression path -- the ratio is genuinely effective + it can decode Compressed frames
// produced by the external zstd CLI.
if (trim((string) @shell_exec('command -v zstd 2>/dev/null')) !== '') {
    $payload = str_repeat('zstd-cli-roundtrip-payload-', 4000); // ~108KB compressible text
    $cliComp = CompressionCodec::zstdCompress($payload);
    $T->check(strlen($cliComp) < strlen($payload) / 10, 'ZSTD CLI achieves real compression (<10% of original)');
    $T->checkSame($payload, CompressionCodec::zstdDecompress($cliComp), 'ZSTD CLI round-trip');
    // A real CLI-compressed frame contains Compressed blocks -- zstdDecompressFrame (the pure
    // implementation) must reject it, proving the layering holds: "Compressed blocks are only
    // decoded via the CLI channel; the pure implementation does not silently pass them through."
    $T->checkThrows(
        static fn () => CompressionCodec::zstdDecompressFrame($cliComp),
        \RuntimeException::class,
        'ZSTD CLI frame (Compressed block) rejected by the pure implementation'
    );
    $T->checkSame('via-decoder', MessageDecoder::decompressBody(
        CompressionCodec::zstdCompress('via-decoder'),
        MessageSysFlag::ZSTD_TYPE
    ), 'MessageDecoder ZSTD CLI branch');
} else {
    // No-CLI fallback: zstdCompress must equal the pure Raw implementation
    $T->checkSame(
        CompressionCodec::zstdCompressRaw('no-cli'),
        CompressionCodec::zstdCompress('no-cli'),
        'ZSTD falls back to Raw frames without a CLI'
    );
}

// ============================================================ SysFlag bit operations
$T->checkSame(3, MessageSysFlag::getCompressionType(MessageSysFlag::setCompressionType(0, 3)), 'get/setCompressionType');
$T->checkSame(0, MessageSysFlag::getCompressionType(0x1234 & ~MessageSysFlag::COMPRESSION_TYPE_COMPARATOR), 'compression type mask cleared');
$T->check(MessageSysFlag::isCompressed(0x1) && !MessageSysFlag::isCompressed(0x2), 'isCompressed');
$T->checkSame(0x0, MessageSysFlag::clearCompressedFlag(0x1), 'clearCompressedFlag');
$T->checkSame(MessageSysFlag::TRANSACTION_COMMIT_TYPE, MessageSysFlag::getTransactionValue(MessageSysFlag::TRANSACTION_COMMIT_TYPE), 'getTransactionValue commit');
$T->checkSame(MessageSysFlag::TRANSACTION_ROLLBACK_TYPE, MessageSysFlag::getTransactionValue(MessageSysFlag::TRANSACTION_ROLLBACK_TYPE), 'getTransactionValue rollback');
$T->checkSame(MessageSysFlag::TRANSACTION_PREPARED_TYPE, MessageSysFlag::resetTransactionValue(0, MessageSysFlag::TRANSACTION_PREPARED_TYPE), 'resetTransactionValue');
$T->check(MessageSysFlag::check(MessageSysFlag::BORNHOST_V6_FLAG, MessageSysFlag::BORNHOST_V6_FLAG), 'MessageSysFlag.check');
$T->checkSame(0b1011, PullSysFlag::buildSysFlag(true, true, false, true), 'PullSysFlag.buildSysFlag');
$T->check(PullSysFlag::hasCommitOffsetFlag(1) && !PullSysFlag::hasSuspendFlag(1), 'PullSysFlag has flags');
$T->checkSame(2, PullSysFlag::clearCommitOffsetFlag(3), 'PullSysFlag.clearCommitOffsetFlag');
$T->checkSame(6, PullSysFlag::buildSysFlagWithSubscription(2), 'buildSysFlagWithSubscription');
$T->checkSame('RW-', PermName::permToString(PermName::PERM_READ | PermName::PERM_WRITE), 'permToString(6)="RW-"');
$T->checkSame('R-X', PermName::permToString(PermName::PERM_READ | PermName::PERM_INHERIT), 'permToString(5)="R-X"');
$T->check(PermName::isValid(6) && !PermName::isValid(8), 'PermName.isValid');
$T->check(PermName::checkPerm(6, 4), 'checkPerm');
$T->checkThrows(static fn () => PermName::isValid('abc'), \ValueError::class, 'isValid(non-numeric) throws ValueError');

// ============================================================ TopicValidator whitelist
$T->check(!TopicValidator::isTopicOrGroupIllegal('TestTopic_123'), 'legal topic name');
$T->check(TopicValidator::isTopicOrGroupIllegal('bad topic'), 'space makes it illegal');
$T->check(TopicValidator::isTopicOrGroupIllegal('topic!'), '! makes it illegal');
$T->check(TopicValidator::isTopicOrGroupIllegal('主题'), 'non-ASCII is illegal');
$T->check(!TopicValidator::isTopicOrGroupIllegal('%RETRY%x_-|'), 'whitelist includes % - _ |');
$T->check(!TopicValidator::isTopicOrGroupIllegal(''), 'empty string returns false (left to isBlank)');
$T->check(TopicValidator::isSystemTopic('rmq_sys_TRACE_DATA'), 'isSystemTopic prefix hit');
$T->check(TopicValidator::isSystemTopic('TBW102'), 'isSystemTopic set hit');
$T->check(TopicValidator::isSystemTopic('RMQ_SYS_TRANS_HALF_TOPIC'), 'isSystemTopic set hit 2');
$T->check(!TopicValidator::isSystemTopic('MyTopic'), 'isSystemTopic ordinary name no hit');
$T->check(TopicValidator::isNotAllowedSendTopic('SCHEDULE_TOPIC_XXXX'), 'isNotAllowedSendTopic hit');
$T->check(!TopicValidator::isNotAllowedSendTopic('TBW102'), 'TBW102 is allowed to send');
$T->checkSame(127, TopicValidator::TOPIC_MAX_LENGTH, 'TOPIC_MAX_LENGTH=127');
$T->checkSame(120, TopicValidator::GROUP_MAX_LENGTH, 'GROUP_MAX_LENGTH=120');

// ============================================================ TopicConfig JSON round-trip
$tc = new TopicConfig('DemoTopic');
$T->checkSame(16, $tc->readQueueNums, 'TopicConfig defaults readQueueNums=16');
$T->checkSame(16, $tc->writeQueueNums, 'TopicConfig defaults writeQueueNums=16');
$T->checkSame(6, $tc->perm, 'TopicConfig defaults perm=6');
$T->checkSame(TopicFilterType::SINGLE_TAG, $tc->topicFilterType, 'TopicConfig defaults SINGLE_TAG');
$T->checkSame(false, $tc->order, 'TopicConfig defaults order=false');
$T->checkSame([], $tc->attributes, 'TopicConfig defaults attributes={}');

$tc->attributes = ['key1' => 'value1'];
$json = $tc->encode();
$tc2 = TopicConfig::decode($json);
$T->checkSame('DemoTopic', $tc2->topicName, 'TopicConfig JSON round-trip topicName');
$T->checkSame(16, $tc2->readQueueNums, 'TopicConfig JSON round-trip readQueueNums');
$T->checkSame(6, $tc2->perm, 'TopicConfig JSON round-trip perm');
$T->checkSame(['key1' => 'value1'], $tc2->attributes, 'TopicConfig JSON round-trip attributes (serialized)');
$T->checkSame(false, $tc2->order, 'TopicConfig JSON round-trip order');
$decoded2 = TopicConfig::fromDict(json_decode($json, true));
$T->checkSame($tc->toDict(), $decoded2->toDict(), 'TopicConfig toDict/fromDict round-trip');
$T->checkThrows(static fn () => TopicConfig::decode('{bad json'), \JsonException::class, 'TopicConfig.decode invalid JSON throws');

// ============================================================ SubscriptionData / FilterAPI
$sub = FilterAPI::buildSubscriptionData('TopicTest', 'TagA');
$T->checkSame(['TagA'], $sub->getTagsSet(), 'TagA tagsSet');
$T->checkSame([2598919], $sub->getCodeSet(), 'TagA codeSet=2598919');
$sub2 = FilterAPI::buildSubscriptionData('TopicTest', 'TagA||TagB');
$T->checkSame(['TagA', 'TagB'], $sub2->getTagsSet(), 'TagA||TagB tagsSet');
$T->checkSame([2598919, 2598920], $sub2->getCodeSet(), 'TagA||TagB codeSet');
$subAll = FilterAPI::buildSubscriptionData('TopicTest', '*');
$T->checkSame('*', $subAll->getSubString(), '* normalized');
$T->checkSame([], $subAll->getTagsSet(), '* tagsSet empty');
$T->checkSame([], $subAll->getCodeSet(), '* codeSet empty');
$T->checkSame('*', FilterAPI::buildSubscriptionData('T', null)->getSubString(), 'null normalized to *');
$T->checkSame('   ', FilterAPI::buildSubscriptionData('T', '   ')->getSubString(), 'whitespace-only subString kept verbatim');
$T->checkSame(['|'], FilterAPI::buildSubscriptionData('T', '|||')->getTagsSet(), '||| -> tag="|"');
$T->checkSame(' TagA || TagB ', FilterAPI::buildSubscriptionData('T', ' TagA || TagB ')->getSubString(), 'whitespace kept verbatim');
$T->checkThrows(static fn () => FilterAPI::buildSubscriptionData('T', '||'), \InvalidArgumentException::class, '|| throws split error');
$sd = new SubscriptionData('T', 'TagA');
$T->checkSame(SubscriptionData::class, get_class($sd), 'SubscriptionData construction');
$T->check($sd->equals(new SubscriptionData('T', 'TagA')), 'SubscriptionData.equals');
$T->checkSame('TAG', $sd->getExpressionType(), 'expressionType defaults to TAG');
$T->check(!array_key_exists('filterClassSource', $sd->toDict()), 'toDict does not serialize filterClassSource');

// ============================================================ MessageType / BoundaryType
$T->checkSame(0, MessageType::NORMAL_MSG->value, 'MessageType value=protocol enum ordinal');
$T->checkSame('Normal', MessageType::NORMAL_MSG->shortName(), 'shortName Normal');
$T->checkSame('TransCommit', MessageType::TRANS_MSG_COMMIT->shortName(), 'shortName TransCommit');
$T->checkSame(MessageType::DELAY_MSG, MessageType::getByShortName('Delay'), 'getByShortName Delay');
$T->checkSame(MessageType::NORMAL_MSG, MessageType::getByShortName('unknown'), 'getByShortName unknown falls back to NORMAL');
$T->checkSame('LOWER', BoundaryType::LOWER->value, 'BoundaryType wire text LOWER');
$T->checkSame(BoundaryType::UPPER, BoundaryType::getType('upper'), 'getType("upper")=UPPER');
$T->checkSame(BoundaryType::UPPER, BoundaryType::getType('UPPER'), 'getType("UPPER")=UPPER');
$T->checkSame(BoundaryType::LOWER, BoundaryType::getType('lower'), 'getType("lower")=LOWER (lenient parsing)');
$T->checkSame(BoundaryType::LOWER, BoundaryType::getType(null), 'getType(null)=LOWER');
$T->checkSame('lower', BoundaryType::LOWER->lowercaseName(), 'lowercaseName');

// ============================================================ RecallMessageHandle
$handle = RecallMessageHandle::buildHandle('topic1', 'broker-a', '1700000000000', 'MSGID-001');
$T->check(!str_contains($handle, '+') && !str_contains($handle, '/'), 'buildHandle base64url alphabet');
$h1 = RecallMessageHandle::decodeHandle($handle);
$T->check($h1->equals(new HandleV1('topic1', 'broker-a', '1700000000000', 'MSGID-001')), 'build/decode round-trip');
$T->check($h1->timestampStr === '1700000000000', 'timestampStr kept as string');
// Unpadded handles also decode (base64 unpadded encoding variant)
$T->check($h1->equals(RecallMessageHandle::decodeHandle(rtrim($handle, '='))), 'unpadded handle decodes compatibly');
$T->checkThrows(static fn () => RecallMessageHandle::decodeHandle(''), \RocketMQ\Client\Exceptions\MQClientException::class, 'empty handle throws MQClientException');
$T->checkThrows(static fn () => RecallMessageHandle::decodeHandle('###'), \RocketMQ\Client\Exceptions\MQClientException::class, 'invalid base64 throws MQClientException');
$badV = base64_encode('v2 t b ts id');
$T->checkThrows(static fn () => RecallMessageHandle::decodeHandle($badV), \RocketMQ\Client\Exceptions\MQClientException::class, 'wrong version throws MQClientException');
$extra = base64_encode('v1 t b ts id extra');
$T->check((RecallMessageHandle::decodeHandle($extra))->messageId === 'id', 'extra segments ignored');

// ============================================================ Wrap-up
exit($T->summary());
