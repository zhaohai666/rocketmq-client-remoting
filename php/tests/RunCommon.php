<?php

declare(strict_types=1);

/**
 * Common 层纯 PHP assert 风格自测（不依赖 phpunit）。
 *
 * 运行：C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunCommon.php
 * 全部通过输出 "ALL TESTS PASSED (N checks)"，任一失败列出明细并以非零码退出。
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
$T->checkSame(2598919, UtilAll::javaStringHash('TagA'), 'javaStringHash TagA=2598919');
$T->checkSame(2598920, UtilAll::javaStringHash('TagB'), 'javaStringHash TagB=2598920');
$T->checkSame(80, UtilAll::javaStringHash('P'), 'javaStringHash P=80');
$T->checkSame(2545, UtilAll::javaStringHash('PA'), 'javaStringHash PA=2545');
$T->checkSame(42, UtilAll::javaStringHash('*'), 'javaStringHash *=42');
$T->checkSame(0, UtilAll::javaStringHash(''), 'javaStringHash ""=0');

$hex = UtilAll::bytes2String("\xde\xad\xbe\xef");
$T->checkSame('DEADBEEF', $hex, 'bytes2String 大写十六进制');
$T->checkSame("\xde\xad\xbe\xef", UtilAll::string2Bytes('deadBEEF'), 'string2Bytes 大小写不敏感');
$T->checkSame(null, UtilAll::string2Bytes(''), 'string2Bytes("")=null');
$T->checkSame(null, UtilAll::string2Bytes(null), 'string2Bytes(null)=null');
$T->checkSame(20, strlen(UtilAll::offset2Filename(123)), 'offset2Filename 长度 20');
$T->checkSame('00000000000000000123', UtilAll::offset2Filename(123), 'offset2Filename 补零');
$T->checkSame('-', UtilAll::timeToHumanString(0), 'timeToHumanString(0)="-"');
$T->check(UtilAll::isBlank(null) && UtilAll::isBlank('  ') && !UtilAll::isBlank('x') && !UtilAll::isNotBlank(' '), 'isBlank/isNotBlank');
$T->check(UtilAll::isIpv4('10.0.0.1') && !UtilAll::isIpv4('::1') && !UtilAll::isIpv4('999.1.1.1'), 'isIpv4');
$T->check(UtilAll::crc32('hello') === crc32('hello'), 'crc32 与内置一致');
$T->check(UtilAll::currentTimeMillis() > 1600000000000, 'currentTimeMillis 毫秒量级');
$T->checkSame('127.0.0.1', UtilAll::string2UnicodeShift('127.0.0.1', 3), 'string2UnicodeShift 原样返回');

// ============================================================ MixAll
$T->checkSame('%RETRY%G1', MixAll::getRetryTopic('G1'), 'getRetryTopic');
$T->check(MixAll::isRetryTopic('%RETRY%G1') && !MixAll::isRetryTopic('T'), 'isRetryTopic');
$T->checkSame('%DLQ%G1', MixAll::getDlqTopic('G1'), 'getDlqTopic');
$T->checkSame('T', MixAll::resetRetryAndDlqTopic('%RETRY%T'), 'resetRetryAndDlqTopic retry');
$T->checkSame('T', MixAll::resetRetryAndDlqTopic('%DLQ%T'), 'resetRetryAndDlqTopic dlq');
$T->checkSame('T', MixAll::resetRetryAndDlqTopic('T'), 'resetRetryAndDlqTopic 原样');
$T->checkSame('b1:10909', MixAll::brokerVipChannel(true, 'b1:10911'), 'brokerVipChannel -2');
$T->checkSame('b1:10911', MixAll::brokerVipChannel(false, 'b1:10911'), 'brokerVipChannel 关闭');
$T->checkSame('no-port', MixAll::brokerVipChannel(true, 'no-port'), 'brokerVipChannel 无端口原样');
$T->checkSame('cluster_REPLY_TOPIC', MixAll::getReplyTopic('cluster'), 'getReplyTopic');
$T->check(MixAll::isSysTopic('rmq_sys_x') && !MixAll::isSysTopic('x'), 'isSysTopic');
$T->check(MixAll::isLmq('%LMQ%t') && !MixAll::isLmq('t'), 'isLmq');
$T->check(MixAll::isSysConsumerGroup('CID_RMQ_SYS_x') && !MixAll::isSysConsumerGroup('G'), 'isSysConsumerGroup');
$T->check(MixAll::isPredefinedGroup('DEFAULT_CONSUMER') && !MixAll::isPredefinedGroup('G'), 'isPredefinedGroup');
$T->checkSame('ip@inst@unit', MixAll::buildMqClientId('ip', 'inst', 'unit'), 'buildMqClientId unit');
$T->checkSame('ip@inst@unit@STREAM', MixAll::buildMqClientId('ip', 'inst', 'unit', true), 'buildMqClientId STREAM');
$T->checkSame('ip@inst', MixAll::buildMqClientId('ip', 'inst'), 'buildMqClientId 最简');
$T->check(MixAll::changeInstanceNameToPid('DEFAULT') !== 'DEFAULT', 'changeInstanceNameToPid DEFAULT 会换 pid#nano');
$T->checkSame('myinst', MixAll::changeInstanceNameToPid('myinst'), 'changeInstanceNameToPid 非 DEFAULT 原样');
$T->checkSame('%ns%%inst', MixAll::compareAndIncreaseNamespace('inst', 'ns'), 'compareAndIncreaseNamespace');
$T->checkSame('%ns%%inst', MixAll::compareAndIncreaseNamespace('%ns%%inst', 'ns'), 'compareAndIncreaseNamespace 幂等');
$T->check(str_starts_with(MixAll::createUniqName('p'), 'p') && strlen(MixAll::createUniqName('p')) === 33, 'createUniqName 32hex');

// properties <-> string 往返（含 null 值跳过与排序）
$props = ['b' => '2', 'a' => '1', 'c' => null];
$T->checkSame("b=2\na=1\n", MixAll::properties2String($props), 'properties2String null 值跳过且保持顺序');
$T->checkSame("a=1\nb=2\n", MixAll::properties2String($props, true), 'properties2String 排序');
$roundtrip = MixAll::string2Properties(MixAll::properties2String(['k1' => 'v1', 'k2' => 'v 2', 'k3' => 'x=y']));
$T->checkSame(['k1' => 'v1', 'k2' => 'v 2', 'k3' => 'x=y'], $roundtrip, 'properties 往返');
// java.util.Properties.load 语义
$T->checkSame(['a' => '1', 'b' => '2'], MixAll::string2Properties("# comment\n!bang\na=1\n\nb=2"), '注释行与空行跳过');
$T->checkSame(['key' => 'va lue'], MixAll::string2Properties("key: va lue"), ': 分隔符');
$T->checkSame(['key' => 'value'], MixAll::string2Properties("key value"), '空白分隔符');
$T->checkSame(['a' => '1', 'b' => '2'], MixAll::string2Properties("a=\\\n  1\nb=2"), '行尾反斜杠续行');
$T->checkSame(['a' => 'tail '], MixAll::string2Properties("a= tail "), '值尾部空白保留');
$T->checkSame([], MixAll::string2Properties(null), 'string2Properties(null)');

// ============================================================ Message / 属性 / 延迟等级
$msg = new Message('TopicTest', 'body-bytes', 'TagA', 'key-1', 7);
$T->checkSame('TopicTest', $msg->getTopic(), 'Message topic');
$T->checkSame('body-bytes', $msg->getBody(), 'Message body');
$T->checkSame('TagA', $msg->getTags(), 'Message 构造写 TAGS');
$T->checkSame('key-1', $msg->getKeys(), 'Message 构造写 KEYS');
$T->checkSame(7, $msg->getFlag(), 'Message flag');
$T->checkSame('', (new Message('t'))->getBody(), 'Message body 缺省空串');
$T->checkSame(null, (new Message('t'))->getTags(), 'Message tags 缺省 null');

$msg->setDelayTimeLevel(3);
$T->checkSame('3', $msg->getDelayTimeLevel(), 'delayTimeLevel 存为字符串 "3"');
$T->checkSame('3', $msg->getProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL), 'DELAY 属性键');
$msg->setWaitStoreMsgOk(false);
$T->checkSame('false', $msg->getWaitStoreMsgOk(), 'WAIT=false');
$T->check(!Message::isWaitStoreMsgOk($msg), 'isWaitStoreMsgOk 显式 false');
$T->check(Message::isWaitStoreMsgOk(new Message('t')), 'isWaitStoreMsgOk 缺省即 true');
$msg->setUserProperty('u1', 'v1');
$T->checkSame('v1', $msg->getUserProperty('u1'), 'userProperty');
$T->checkSame('v1', $msg->getProperty('u1'), 'getProperty');
$msg->removeProperty('u1');
$T->checkSame(null, $msg->getProperty('u1'), 'removeProperty');
$msg->clearProperty();
$T->checkSame([], $msg->getProperties(), 'clearProperty 清空');
$msg->putProperty('p', '1');
$T->checkSame('1', $msg->getProperty('p'), 'putProperty');
$msg->setTransactionId('tid');
$T->checkSame('tid', $msg->getTransactionId(), 'transactionId');

// MessageQueue
$mq = new MessageQueue('TopicTest', 'broker-a', 3);
$T->checkSame('3', $mq->getQueueIdStr(), 'MessageQueue.getQueueIdStr');
$T->check($mq->equals(new MessageQueue('TopicTest', 'broker-a', 3)), 'MessageQueue.equals');
$T->check(!$mq->equals(new MessageQueue('TopicTest', 'broker-a', 4)), 'MessageQueue.equals 不等');
$T->check(MessageQueue::wrapInt32(0x7FFFFFFF) === 2147483647 && MessageQueue::wrapInt32(0x80000000) === -2147483648, 'wrapInt32 32位回绕');

// ============================================================ MessageClientIdSetter
$u1 = new Message('t');
MessageClientIdSetter::setUniqId($u1);
$uniq1 = $u1->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
$T->check(is_string($uniq1) && strlen($uniq1) === 32, 'uniqId 为 32 位十六进制');
$T->checkSame($uniq1, MessageClientIdSetter::getUniqId($u1), 'set/get uniqId 一致');
$u2 = new Message('t');
MessageClientIdSetter::setUniqId($u2);
MessageClientIdSetter::setUniqId($u2); // 幂等：已有则不覆盖
$uniq2 = $u2->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
$T->check($uniq1 !== $uniq2, '两次生成的 uniqId 互不相同（唯一性）');
MessageClientIdSetter::setUniqId($u2);
$T->checkSame($uniq2, $u2->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX), 'setUniqId 已存在不覆盖');
$T->checkSame(null, MessageClientIdSetter::getUniqId(null), 'getUniqId(null)');
MessageClientIdSetter::setUniqId(null); // 不应抛异常
$T->checkSame(32, strlen(InnerIdGenerator::createUniqId()), 'InnerIdGenerator 直接生成 32 位');
$T->check(ctype_xdigit(MessageClientIdSetter::createUniqId()), 'uniqId 全为十六进制字符');

// ============================================================ MessageDecoder：msgId 编码往返
$addrBytes = inet_pton('192.168.1.10') . pack('N', 10911);
$offset = 123456789;
$msgId = MessageDecoder::createMessageId($addrBytes, $offset);
$T->checkSame('C0A8010A00002A9F' . sprintf('%016X', $offset), $msgId, 'createMessageId 32 位十六进制');
[$ip, $port, $off] = MessageDecoder::decodeMessageId($msgId);
$T->checkSame('192.168.1.10', $ip, 'decodeMessageId ip');
$T->checkSame(10911, $port, 'decodeMessageId port');
$T->checkSame(123456789, $off, 'decodeMessageId offset');

// UtilAll HEX 往返
$T->checkSame('DEADBEEF0102', UtilAll::bytes2String(UtilAll::string2Bytes('deadbeef0102')), 'UtilAll hex 往返');

// 属性串 <-> map
$T->checkSame(['A' => '1', 'B' => '2'], MessageDecoder::string2MessageProperties("A\x011\x02B\x012\x02"), 'string2MessageProperties');
$T->checkSame("A\x011\x02B\x012\x02", MessageDecoder::messageProperties2String(['A' => '1', 'B' => '2']), 'messageProperties2String');
$T->checkSame([], MessageDecoder::string2MessageProperties(''), '空属性串 -> 空 map');

// 17 段格式：encodeMessageExt -> decodeMessage 全字段往返
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
$T->check($decoded !== null, 'decodeMessage 返回非 null');
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
    $T->checkSame('user.value', $decoded->getUserProperty('user.key'), 'decode 用户属性');
    // msgId = storeHost(ip+port) + commitLogOffset，且 isClient 时 offsetMsgId 与之相等
    $expectedMsgId = MessageDecoder::createMessageId(inet_pton('10.0.0.3') . pack('N', 10911), 77777);
    $T->checkSame($expectedMsgId, $decoded->getMsgId(), 'decode msgId = storeHost+offset');
    $T->checkSame($expectedMsgId, $decoded->getOffsetMsgId(), 'offsetMsgId 与 msgId 相同（isClient）');
    [$dip, $dport, $doff] = MessageDecoder::decodeMessageId((string) $decoded->getMsgId());
    $T->checkSame(['10.0.0.3', 10911, 77777], [$dip, $dport, $doff], 'offsetMsgId parseAddr/parseOffset 往返');
    $T->checkSame(3668123815, MessageDecoder::crc32($decoded->getBody()) === $decoded->getBodyCrc() ? 3668123815 : 0, 'bodyCrc 校验一致');
}

// 未知魔数 -> null（decode 返回 null 语义）
$bad = $encoded;
$bad[5] = "\x00";
$T->checkSame(null, MessageDecoder::decodeMessage($bad), '未知魔数 decode 返回 null');

// 截断缓冲 -> null（异常吞掉返回 null）
$T->checkSame(null, MessageDecoder::decodeMessage(substr($encoded, 0, 30)), '截断输入 decode 返回 null');

// readBody=false -> body 为 null
$T->checkSame(null, MessageDecoder::decodeMessage($encoded, readBody: false)?->getBody(), 'readBody=false body 为 null');

// 6 段轻量格式：批量
$bmsgs = [new Message('T', 'b1', 'TagA'), new Message('T', 'b2', null, 'k2')];
$bmsgs[0]->setFlag(3);
$batchBody = MessageDecoder::encodeMessages($bmsgs);
$roundtripMsgs = MessageDecoder::decodeBatchMessages($batchBody);
$T->checkSame(2, count($roundtripMsgs), 'decodeBatchMessages 条数');
$T->checkSame('b1', $roundtripMsgs[0]->getBody() ?? null, '批量第一条 body');
$T->checkSame('TagA', $roundtripMsgs[0]->getTags(), '批量第一条 TAGS');
$T->checkSame(3, $roundtripMsgs[0]->getFlag(), '批量第一条 flag');
$T->checkSame('k2', $roundtripMsgs[1]->getKeys(), '批量第二条 KEYS');
$T->checkSame(2, MessageDecoder::countInnerMsgNum($batchBody), 'countInnerMsgNum');

// MessageBatch.generateFromList
$batch = MessageBatch::generateFromList($bmsgs);
$T->checkSame('T', $batch->getTopic(), 'MessageBatch topic 取第一条');
$T->check(MessageBatch::isWaitStoreMsgOk($batch), 'MessageBatch WAIT 缺省即 true');
$T->checkSame($batchBody, $batch->getBody(), 'MessageBatch body 即 encodeMessages 结果');
$T->checkThrows(
    static fn () => MessageBatch::generateFromList([new Message('%RETRY%G'), new Message('%RETRY%G')]),
    \InvalidArgumentException::class,
    '批量不支持重试 topic'
);
$T->checkThrows(
    static fn () => MessageBatch::generateFromList([]),
    \InvalidArgumentException::class,
    '批量不允许为空'
);
$delayed = new Message('T');
$delayed->setDelayTimeLevel(1);
$T->checkThrows(
    static fn () => MessageBatch::generateFromList([$delayed]),
    \InvalidArgumentException::class,
    '批量不允许延时消息'
);

// zlib 压缩路径：类型位 0 按老版本语义归一化为 ZLIB
$compressed = MessageDecoder::decompressBody(gzcompress('payload-data'), 0);
$T->checkSame('payload-data', $compressed, 'normalizeCompressionType 0 -> ZLIB');
$T->checkSame('xyz', MessageDecoder::decompressBody(gzcompress('xyz'), MessageSysFlag::ZLIB_TYPE), 'ZLIB 解压');
$T->checkThrows(
    static fn () => MessageDecoder::decompressBody('x', 5),
    \RuntimeException::class,
    '未知压缩类型抛 RuntimeException'
);
// 压缩消息 decode：sysFlag 置 COMPRESSED + ZLIB 类型位
$cext = new MessageExt('TopicC', 'plain', 'TagC');
$cext->setSysFlag(MessageSysFlag::setCompressionType(
    MessageSysFlag::COMPRESSED_FLAG,
    MessageSysFlag::ZLIB_TYPE
));
$cenc = MessageDecoder::encodeMessageExt($cext, true);
$cdec = MessageDecoder::decodeMessage($cenc);
$T->check($cdec !== null && $cdec->getBody() === 'plain', '压缩消息解码还原正文');
$T->check($cdec !== null && MessageSysFlag::isCompressed($cdec->getSysFlag()) === false, '解码后清掉 COMPRESSED_FLAG');

// LZ4 Frame（Java LZ4FrameOutputStream / Python lz4.frame 同规范；block 层是它的内部）：
// xxhash32 官方向量锚定 + Python lz4.frame 产出的完整帧 + Frame 往返 + 重叠 match
$T->checkSame(0x02CC5D05, CompressionCodec::xxh32(''), 'xxh32("") 官方向量');
$T->checkSame(0x32D153FF, CompressionCodec::xxh32('abc'), 'xxh32("abc") 官方向量');
// Python lz4.frame.compress 产出的帧（跨端解压锚点；HC=xxh32(header)>>8 的第二字节）：
//   'abc' → magic|FLG=0x68|BD=0x40|C.Size=3|HC=0x87|raw块(0x80000003)|'abc'|EndMark
$lz4framePy = hex2bin('04224d1868400300000000000000870300008061626300000000');
$T->checkSame('abc', CompressionCodec::lz4DecompressFrame($lz4framePy), 'LZ4 Frame Python 产帧解压');
// 'hello world '×40：compressed 块（裸 block 形状，node 版同输入产出一致）
$lz4framePy2 = hex2bin('04224d186840e0010000000000009b17000000cf68656c6c6f20776f726c64200c00ffbd506f726c642000000000');
$T->checkSame(str_repeat('hello world ', 40), CompressionCodec::lz4DecompressBlock(hex2bin('cf68656c6c6f20776f726c64200c00ffbd506f726c6420')), 'LZ4 裸 block 长串（node 同款）解压');
$T->checkSame(str_repeat('hello world ', 40), CompressionCodec::lz4DecompressFrame($lz4framePy2), 'LZ4 Frame Python 长串帧解压');
// Frame 往返（含 >64KB 多块、RLE、raw 块兜底）
$lz4FrameSamples = [
    '', 'a', 'abcabcabcabc',
    str_repeat('hello world ', 40),
    str_repeat('x', 300000),                       // RLE 型，块压缩得动
    str_repeat('0123456789', 8192) . 'tail',       // 80KB+ → 多块
];
foreach ($lz4FrameSamples as $idx => $sample) {
    $enc = CompressionCodec::lz4CompressFrame($sample);
    $T->checkSame($sample, CompressionCodec::lz4DecompressFrame($enc), "LZ4 Frame 往返 #$idx (len=" . strlen($sample) . ')');
}
// 坏 HC 必须抛（防止把别的字节流误当 LZ4 Frame 解）
$T->checkThrows(
    static fn () => CompressionCodec::lz4DecompressFrame(hex2bin('04224d1868400300000000000000880300008061626300000000')),
    \RuntimeException::class,
    'LZ4 Frame 坏 HC 抛异常'
);
$lz4samples = [
    '',
    'a',
    'ab',
    str_repeat('hello world ', 40),
    str_repeat('x', 300000),            // 长 RLE（跨多段字面量扩展）
    str_repeat('0123456789', 8192) . uniqid('', true), // 高重复 + 唯一尾巴
    random_bytes_wellknown(),
];
/** 确定性伪随机（避免 random_bytes 逐次变化） */
function random_bytes_wellknown(): string
{
    $out = '';
    $s = 12345;
    for ($i = 0; $i < 100000; $i++) {
        $s = ($s * 1103515245 + 12345) & 0x7FFFFFFF;
        $out .= chr(0x20 + ($s % 96)); // 可打印域，制造可匹配的重复
    }
    return $out;
}
foreach ($lz4samples as $idx => $sample) {
    $enc = CompressionCodec::lz4CompressBlock($sample);
    $T->checkSame($sample, CompressionCodec::lz4DecompressBlock($enc), "LZ4 往返 #$idx (len=" . strlen($sample) . ')');
}
// 重叠 match（off < matchLen，LZ4 的经典 RLE 场景）：解压必须逐字节拷贝
// token 0x1F: litLen=1 'A'，off=1，ml 位=15+扩展 0 → matchLen=19
$overlap = CompressionCodec::lz4DecompressBlock(hex2bin('1F4101000'. '0'));
$T->checkSame(str_repeat('A', 20), $overlap, 'LZ4 重叠 match 解压');
$T->checkThrows(
    static fn () => CompressionCodec::lz4DecompressBlock(hex2bin('306162630000')),
    \RuntimeException::class,
    'LZ4 零偏移抛异常'
);

// ZSTD Raw/RLE 帧：node 产出的完整小帧（magic/帧头/RAW 块跨端锚点）+ 往返 + RLE
$zstdNode = hex2bin('28b52ffde01600000000000000b100007a7374642d7261772d6672616d652d7061796c6f6164');
$T->checkSame('zstd-raw-frame-payload', CompressionCodec::zstdDecompressFrame($zstdNode), 'ZSTD node 帧解压');
$T->checkSame($zstdNode, CompressionCodec::zstdCompressRaw('zstd-raw-frame-payload'), 'ZSTD 与 node 压缩逐字节一致');
$zstdRle = CompressionCodec::zstdCompressRaw(str_repeat('Z', 500000)); // 500KB 全 Z → RLE 块
$T->check(strlen($zstdRle) < 5000, 'ZSTD RLE 大跑长压缩有效');
$T->checkSame(str_repeat('Z', 500000), CompressionCodec::zstdDecompressFrame($zstdRle), 'ZSTD RLE 往返');
$zstdBig = CompressionCodec::zstdCompressRaw(str_repeat('chunk-', 60000)); // >128KB → 多 Raw 块
$T->checkSame(str_repeat('chunk-', 60000), CompressionCodec::zstdDecompressFrame($zstdBig), 'ZSTD 多块帧往返');
// Compressed 块（type=2）：显式抛，绝不把压缩流当正文透传
// magic + 帧头(FCS=8B) + 块头 04 00 00（last=0, type=2=Compressed, size=0）
$compFrame = pack('V', 0xFD2FB528) . chr(0xE0) . pack('P', 0) . chr(0x04) . chr(0x00) . chr(0x00) . "\x01\x02";
$T->checkThrows(
    static fn () => CompressionCodec::zstdDecompressFrame($compFrame),
    \RuntimeException::class,
    'ZSTD Compressed 块抛异常'
);

// MessageDecoder 接线：LZ4/ZSTD 经公开入口 decompressBody 可解；Producer 侧 compressBody 同源
$T->checkSame('via-decoder', MessageDecoder::decompressBody(
    CompressionCodec::lz4CompressFrame('via-decoder'),
    MessageSysFlag::LZ4_TYPE
), 'MessageDecoder LZ4 分支');
$T->checkSame('via-decoder', MessageDecoder::decompressBody(
    CompressionCodec::zstdCompressRaw('via-decoder'),
    MessageSysFlag::ZSTD_TYPE
), 'MessageDecoder ZSTD 分支');

// ZSTD 统一入口（CLI 优先 / Raw 兜底）：有 zstd CLI 时验证真压缩路径——
// 压缩率真实生效 + 能解回其他端（CLI）压出的 Compressed 帧。
if (trim((string) @shell_exec('command -v zstd 2>/dev/null')) !== '') {
    $payload = str_repeat('zstd-cli-roundtrip-payload-', 4000); // ~108KB 可压文本
    $cliComp = CompressionCodec::zstdCompress($payload);
    $T->check(strlen($cliComp) < strlen($payload) / 10, 'ZSTD CLI 真压缩率生效（<10% 原文）');
    $T->checkSame($payload, CompressionCodec::zstdDecompress($cliComp), 'ZSTD CLI 往返');
    // 真 CLI 压缩帧里是 Compressed 块 —— zstdDecompressFrame（纯实现）必须拒绝它，
    // 证明「Compressed 块只经由 CLI 通道解，纯实现不静默透传」的分层成立。
    $T->checkThrows(
        static fn () => CompressionCodec::zstdDecompressFrame($cliComp),
        \RuntimeException::class,
        'ZSTD CLI 帧（Compressed 块）纯实现拒绝'
    );
    $T->checkSame('via-decoder', MessageDecoder::decompressBody(
        CompressionCodec::zstdCompress('via-decoder'),
        MessageSysFlag::ZSTD_TYPE
    ), 'MessageDecoder ZSTD CLI 分支');
} else {
    // 无 CLI 兜底：zstdCompress 必须等于纯 Raw 实现
    $T->checkSame(
        CompressionCodec::zstdCompressRaw('no-cli'),
        CompressionCodec::zstdCompress('no-cli'),
        'ZSTD 无 CLI 时回退 Raw 帧'
    );
}

// ============================================================ SysFlag 位运算
$T->checkSame(3, MessageSysFlag::getCompressionType(MessageSysFlag::setCompressionType(0, 3)), 'get/setCompressionType');
$T->checkSame(0, MessageSysFlag::getCompressionType(0x1234 & ~MessageSysFlag::COMPRESSION_TYPE_COMPARATOR), '压缩类型掩码清零');
$T->check(MessageSysFlag::isCompressed(0x1) && !MessageSysFlag::isCompressed(0x2), 'isCompressed');
$T->checkSame(0x0, MessageSysFlag::clearCompressedFlag(0x1), 'clearCompressedFlag');
$T->checkSame(MessageSysFlag::TRANSACTION_COMMIT_TYPE, MessageSysFlag::getTransactionValue(MessageSysFlag::TRANSACTION_COMMIT_TYPE), 'getTransactionValue commit');
$T->checkSame(MessageSysFlag::TRANSACTION_ROLLBACK_TYPE, MessageSysFlag::getTransactionValue(MessageSysFlag::TRANSACTION_ROLLBACK_TYPE), 'getTransactionValue rollback');
$T->checkSame(MessageSysFlag::TRANSACTION_PREPARED_TYPE, MessageSysFlag::resetTransactionValue(0, MessageSysFlag::TRANSACTION_PREPARED_TYPE), 'resetTransactionValue');
$T->check(MessageSysFlag::check(MessageSysFlag::BORNHOST_V6_FLAG, MessageSysFlag::BORNHOST_V6_FLAG), 'MessageSysFlag.check');
$T->checkSame(0b1011, PullSysFlag::buildSysFlag(true, true, false, true), 'PullSysFlag.buildSysFlag');
$T->check(PullSysFlag::hasCommitOffsetFlag(1) && !PullSysFlag::hasSuspendFlag(1), 'PullSysFlag has 标志');
$T->checkSame(2, PullSysFlag::clearCommitOffsetFlag(3), 'PullSysFlag.clearCommitOffsetFlag');
$T->checkSame(6, PullSysFlag::buildSysFlagWithSubscription(2), 'buildSysFlagWithSubscription');
$T->checkSame('RW-', PermName::permToString(PermName::PERM_READ | PermName::PERM_WRITE), 'permToString(6)="RW-"');
$T->checkSame('R-X', PermName::permToString(PermName::PERM_READ | PermName::PERM_INHERIT), 'permToString(5)="R-X"');
$T->check(PermName::isValid(6) && !PermName::isValid(8), 'PermName.isValid');
$T->check(PermName::checkPerm(6, 4), 'checkPerm');
$T->checkThrows(static fn () => PermName::isValid('abc'), \ValueError::class, 'isValid(非数字) 抛 ValueError');

// ============================================================ TopicValidator 白名单
$T->check(!TopicValidator::isTopicOrGroupIllegal('TestTopic_123'), '合法 topic 名');
$T->check(TopicValidator::isTopicOrGroupIllegal('bad topic'), '含空格非法');
$T->check(TopicValidator::isTopicOrGroupIllegal('topic!'), '含 ! 非法');
$T->check(TopicValidator::isTopicOrGroupIllegal('主题'), '非 ASCII 非法');
$T->check(!TopicValidator::isTopicOrGroupIllegal('%RETRY%x_-|'), '白名单含 % - _ |');
$T->check(!TopicValidator::isTopicOrGroupIllegal(''), '空串返回 false（交给 isBlank）');
$T->check(TopicValidator::isSystemTopic('rmq_sys_TRACE_DATA'), 'isSystemTopic 前缀命中');
$T->check(TopicValidator::isSystemTopic('TBW102'), 'isSystemTopic 集合命中');
$T->check(TopicValidator::isSystemTopic('RMQ_SYS_TRANS_HALF_TOPIC'), 'isSystemTopic 集合命中 2');
$T->check(!TopicValidator::isSystemTopic('MyTopic'), 'isSystemTopic 普通名不命中');
$T->check(TopicValidator::isNotAllowedSendTopic('SCHEDULE_TOPIC_XXXX'), 'isNotAllowedSendTopic 命中');
$T->check(!TopicValidator::isNotAllowedSendTopic('TBW102'), 'TBW102 允许发送');
$T->checkSame(127, TopicValidator::TOPIC_MAX_LENGTH, 'TOPIC_MAX_LENGTH=127');
$T->checkSame(120, TopicValidator::GROUP_MAX_LENGTH, 'GROUP_MAX_LENGTH=120');

// ============================================================ TopicConfig JSON 往返
$tc = new TopicConfig('DemoTopic');
$T->checkSame(16, $tc->readQueueNums, 'TopicConfig 默认 readQueueNums=16');
$T->checkSame(16, $tc->writeQueueNums, 'TopicConfig 默认 writeQueueNums=16');
$T->checkSame(6, $tc->perm, 'TopicConfig 默认 perm=6');
$T->checkSame(TopicFilterType::SINGLE_TAG, $tc->topicFilterType, 'TopicConfig 默认 SINGLE_TAG');
$T->checkSame(false, $tc->order, 'TopicConfig 默认 order=false');
$T->checkSame([], $tc->attributes, 'TopicConfig 默认 attributes={}');

$tc->attributes = ['key1' => 'value1'];
$json = $tc->encode();
$tc2 = TopicConfig::decode($json);
$T->checkSame('DemoTopic', $tc2->topicName, 'TopicConfig JSON 往返 topicName');
$T->checkSame(16, $tc2->readQueueNums, 'TopicConfig JSON 往返 readQueueNums');
$T->checkSame(6, $tc2->perm, 'TopicConfig JSON 往返 perm');
$T->checkSame(['key1' => 'value1'], $tc2->attributes, 'TopicConfig JSON 往返 attributes（会序列化）');
$T->checkSame(false, $tc2->order, 'TopicConfig JSON 往返 order');
$decoded2 = TopicConfig::fromDict(json_decode($json, true));
$T->checkSame($tc->toDict(), $decoded2->toDict(), 'TopicConfig toDict/fromDict 往返');
$T->checkThrows(static fn () => TopicConfig::decode('{bad json'), \JsonException::class, 'TopicConfig.decode 非法 JSON 抛异常');

// ============================================================ SubscriptionData / FilterAPI
$sub = FilterAPI::buildSubscriptionData('TopicTest', 'TagA');
$T->checkSame(['TagA'], $sub->getTagsSet(), 'TagA tagsSet');
$T->checkSame([2598919], $sub->getCodeSet(), 'TagA codeSet=2598919');
$sub2 = FilterAPI::buildSubscriptionData('TopicTest', 'TagA||TagB');
$T->checkSame(['TagA', 'TagB'], $sub2->getTagsSet(), 'TagA||TagB tagsSet');
$T->checkSame([2598919, 2598920], $sub2->getCodeSet(), 'TagA||TagB codeSet');
$subAll = FilterAPI::buildSubscriptionData('TopicTest', '*');
$T->checkSame('*', $subAll->getSubString(), '* 归一');
$T->checkSame([], $subAll->getTagsSet(), '* tagsSet 为空');
$T->checkSame([], $subAll->getCodeSet(), '* codeSet 为空');
$T->checkSame('*', FilterAPI::buildSubscriptionData('T', null)->getSubString(), 'null 归一 *');
$T->checkSame('   ', FilterAPI::buildSubscriptionData('T', '   ')->getSubString(), '纯空白 subString 原样保留');
$T->checkSame(['|'], FilterAPI::buildSubscriptionData('T', '|||')->getTagsSet(), '||| -> tag="|"');
$T->checkSame(' TagA || TagB ', FilterAPI::buildSubscriptionData('T', ' TagA || TagB ')->getSubString(), '空白原样保留');
$T->checkThrows(static fn () => FilterAPI::buildSubscriptionData('T', '||'), \InvalidArgumentException::class, '|| 抛 split error');
$sd = new SubscriptionData('T', 'TagA');
$T->checkSame(SubscriptionData::class, get_class($sd), 'SubscriptionData 构造');
$T->check($sd->equals(new SubscriptionData('T', 'TagA')), 'SubscriptionData.equals');
$T->checkSame('TAG', $sd->getExpressionType(), 'expressionType 缺省 TAG');
$T->check(!array_key_exists('filterClassSource', $sd->toDict()), 'toDict 不序列化 filterClassSource');

// ============================================================ MessageType / BoundaryType
$T->checkSame(0, MessageType::NORMAL_MSG->value, 'MessageType value=Java ordinal');
$T->checkSame('Normal', MessageType::NORMAL_MSG->shortName(), 'shortName Normal');
$T->checkSame('TransCommit', MessageType::TRANS_MSG_COMMIT->shortName(), 'shortName TransCommit');
$T->checkSame(MessageType::DELAY_MSG, MessageType::getByShortName('Delay'), 'getByShortName Delay');
$T->checkSame(MessageType::NORMAL_MSG, MessageType::getByShortName('unknown'), 'getByShortName 未知回 NORMAL');
$T->checkSame('LOWER', BoundaryType::LOWER->value, 'BoundaryType 入网文本 LOWER');
$T->checkSame(BoundaryType::UPPER, BoundaryType::getType('upper'), 'getType("upper")=UPPER');
$T->checkSame(BoundaryType::UPPER, BoundaryType::getType('UPPER'), 'getType("UPPER")=UPPER');
$T->checkSame(BoundaryType::LOWER, BoundaryType::getType('lower'), 'getType("lower")=LOWER（宽松解析）');
$T->checkSame(BoundaryType::LOWER, BoundaryType::getType(null), 'getType(null)=LOWER');
$T->checkSame('lower', BoundaryType::LOWER->lowercaseName(), 'lowercaseName');

// ============================================================ RecallMessageHandle
$handle = RecallMessageHandle::buildHandle('topic1', 'broker-a', '1700000000000', 'MSGID-001');
$T->check(!str_contains($handle, '+') && !str_contains($handle, '/'), 'buildHandle base64url 字母表');
$h1 = RecallMessageHandle::decodeHandle($handle);
$T->check($h1->equals(new HandleV1('topic1', 'broker-a', '1700000000000', 'MSGID-001')), 'build/decode 往返');
$T->check($h1->timestampStr === '1700000000000', 'timestampStr 保留字符串');
// 无填充句柄也能解（其他客户端移植版的无填充编码）
$T->check($h1->equals(RecallMessageHandle::decodeHandle(rtrim($handle, '='))), '无填充句柄兼容解码');
$T->checkThrows(static fn () => RecallMessageHandle::decodeHandle(''), \RocketMQ\Client\Exceptions\MQClientException::class, '空句柄抛 MQClientException');
$T->checkThrows(static fn () => RecallMessageHandle::decodeHandle('###'), \RocketMQ\Client\Exceptions\MQClientException::class, '非法 base64 抛 MQClientException');
$badV = base64_encode('v2 t b ts id');
$T->checkThrows(static fn () => RecallMessageHandle::decodeHandle($badV), \RocketMQ\Client\Exceptions\MQClientException::class, '版本不对抛 MQClientException');
$extra = base64_encode('v1 t b ts id extra');
$T->check((RecallMessageHandle::decodeHandle($extra))->messageId === 'id', '多余分段忽略');

// ============================================================ 收尾
exit($T->summary());
