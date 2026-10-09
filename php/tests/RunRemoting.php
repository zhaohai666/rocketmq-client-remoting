<?php

declare(strict_types=1);

/**
 * Remoting 层纯 PHP assert 风格自测（不依赖 phpunit）。
 *
 * 运行：C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunRemoting.php
 * 全部通过输出 "ALL TESTS PASSED (N checks)"，任一失败列出明细并以非零码退出。
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\Exceptions\RemotingCommandException;
use RocketMQ\Client\Exceptions\RemotingSendRequestException;
use RocketMQ\Client\Exceptions\RemotingTimeoutException;
use RocketMQ\Client\Logger;
use RocketMQ\Common\BoundaryType;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\SubscriptionData;
use RocketMQ\Remoting\AclClientRPCHook;
use RocketMQ\Remoting\AclRpcHook;
use RocketMQ\Remoting\NamespaceRpcHook;
use RocketMQ\Remoting\PendingResponse;
use RocketMQ\Remoting\RemotingClient;
use RocketMQ\Remoting\SessionCredentials;
use RocketMQ\Remoting\StreamTypeRPCHook;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\CheckTransactionStateRequestHeader;
use RocketMQ\Remoting\Protocol\ConsumerData;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\FastJsonDecodeError;
use RocketMQ\Remoting\Protocol\GetRouteInfoRequestHeader;
use RocketMQ\Remoting\Protocol\HeartbeatData;
use RocketMQ\Remoting\Protocol\LanguageCode;
use RocketMQ\Remoting\Protocol\NamespaceUtil;
use RocketMQ\Remoting\Protocol\NotifyConsumerIdsChangedRequestHeader;
use RocketMQ\Remoting\Protocol\PopMessageRequestHeader;
use RocketMQ\Remoting\Protocol\ProducerData;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RemotingSerializable;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\SearchOffsetRequestHeader;
use RocketMQ\Remoting\Protocol\SendMessageRequestHeader;
use RocketMQ\Remoting\Protocol\SendMessageRequestHeaderV2;
use RocketMQ\Remoting\Protocol\SendMessageResponseHeader;
use RocketMQ\Remoting\Protocol\SerializeType;
use RocketMQ\Remoting\Protocol\TopicRouteData;

final class RunRemoting
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
            $detail = sprintf("%s\n  expected: %s\n  actual:   %s", $label, var_export($expected, true), var_export($actual, true));
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
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }

    // ==================================================================== JSON 帧

    private function testJsonFrameRoundtrip(): void
    {
        $header = new SendMessageRequestHeader();
        $header->producerGroup = 'GID-php';
        $header->topic = 'TopicTest';
        $header->queueId = 3;
        $header->sysFlag = 0;
        $header->bornTimestamp = 1718000000000;
        $header->flag = 0;
        $header->properties = 'KEY=VALUE';
        $header->unitMode = true;
        $header->batch = false;

        $cmd = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $cmd->remark = 'json-roundtrip';
        $cmd->addExtField('handWritten', 'yes');
        $cmd->body = "body-bytes-\x01\x02";

        $frame = $cmd->encode();
        // totalLength 前缀
        $total = RocketMQSerializable::unpackSignedInt(substr($frame, 0, 4));
        $this->checkSame(strlen($frame), $total + 4, 'JSON 帧 totalLength 覆盖除前缀外全部字节');
        // headerLength 高 8 位 = SerializeType(JSON=0)
        $oriHeaderLen = RocketMQSerializable::unpackSignedInt(substr($frame, 4, 4));
        $this->checkSame(SerializeType::JSON->value, RemotingCommand::getProtocolType($oriHeaderLen), 'JSON 帧序列化类型位');
        $headerLen = RemotingCommand::getHeaderLength($oriHeaderLen);
        $this->check($headerLen > 0 && $headerLen < strlen($frame), 'JSON 帧 header 长度合理');

        $decoded = RemotingCommand::decode($frame);
        $this->checkSame($cmd->code, $decoded->code, 'JSON 往返 code');
        $this->checkSame($cmd->opaque, $decoded->opaque, 'JSON 往返 opaque');
        $this->checkSame($cmd->version, $decoded->version, 'JSON 往返 version(CURRENT_VERSION)');
        $this->checkSame($cmd->flag, $decoded->flag, 'JSON 往返 flag');
        $this->checkSame($cmd->remark, $decoded->remark, 'JSON 往返 remark');
        $this->checkSame($cmd->body, $decoded->body, 'JSON 往返 body');
        $this->checkSame('yes', $decoded->getExtField('handWritten') ?? '', 'JSON 往返 extFields(手写)');
        $this->checkSame('GID-php', $decoded->getExtField('producerGroup') ?? '', 'JSON 往返 extFields(customHeader 落地)');
        $this->checkSame('3', $decoded->getExtField('queueId') ?? '', 'JSON 往返 extFields(int → str)');
        $this->checkSame('true', $decoded->getExtField('unitMode') ?? '', 'JSON 往返 extFields(bool → 小写 true)');

        // flag 位
        $req = new RemotingCommand(10);
        $this->check(!$req->isResponseType() && !$req->isOnewayRPC(), '新命令默认非响应非 oneway');
        $req->markResponseType();
        $req->markOnewayRPC();
        $this->check($req->isResponseType() && $req->isOnewayRPC(), 'markResponseType/markOnewayRPC 置位');
        $this->checkSame('RESPONSE_COMMAND', $req->getType(), 'getType 响应');
        $back = RemotingCommand::decode($req->encode());
        $this->check($back->isResponseType() && $back->isOnewayRPC(), 'flag 位往返');

        // 响应命令工厂
        $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'ok');
        $resp->opaque = 777;
        $back = RemotingCommand::decode($resp->encode());
        $this->checkSame(ResponseCode::SUCCESS, $back->code, '响应命令 code 往返');
        $this->checkSame(777, $back->opaque, '响应命令 opaque 往返');
        $this->checkSame('ok', $back->remark, '响应命令 remark 往返');

        // language 字符串形态（5.x NameServer 把 language 序列化为枚举名）
        $json = '{"code":105,"language":"JAVA","version":515,"opaque":9,"flag":0,"extFields":{"topic":"t"}}';
        $cmd2 = RemotingCommand::decode(pack('N', strlen($json) + 4) . pack('N', strlen($json)) . $json);
        $this->checkSame(LanguageCode::JAVA->value, $cmd2->language, 'language 枚举名字符串 → int 码');
        $this->checkSame('t', $cmd2->getExtField('topic') ?? '', 'language 字符串形态 extFields');

        // 坏帧
        $this->checkThrows(
            static fn(): RemotingCommand => RemotingCommand::decode(pack('N', 999) . pack('N', 1) . 'x'),
            RemotingCommandException::class,
            'decode 坏 totalLength 抛 RemotingCommandException'
        );

        // opaque 自增 + createNewRequestId
        $a = new RemotingCommand(1);
        $b = new RemotingCommand(1);
        $this->check($b->opaque === $a->opaque + 1, 'opaque 静态自增');
        $this->checkSame(RemotingCommand::createNewRequestId(), $b->opaque + 1, 'createNewRequestId 同一计数器');
    }

    // ==================================================================== ROCKETMQ 序列化

    private function testRocketmqSerialization(): void
    {
        $header = new SendMessageRequestHeader();
        $header->producerGroup = 'GID-bin';
        $header->topic = 'BinTopic';
        $header->queueId = 1;
        $header->properties = 'a=1';
        $header->unitMode = false;

        $cmd = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT, $header);
        $cmd->remark = 'bin-remark';
        $cmd->body = 'BINBODY';
        $cmd->serializeTypeCurrentRpc = SerializeType::ROCKETMQ->value;

        $frame = $cmd->encode();
        $oriHeaderLen = RocketMQSerializable::unpackSignedInt(substr($frame, 4, 4));
        $this->checkSame(SerializeType::ROCKETMQ->value, RemotingCommand::getProtocolType($oriHeaderLen), 'ROCKETMQ 帧序列化类型位');
        $headerData = substr($frame, 8, RemotingCommand::getHeaderLength($oriHeaderLen));
        $this->checkSame(strlen($headerData), RocketMQSerializable::calTotalLen('bin-remark', RocketMQSerializable::mapSerialize($cmd->extFields)), 'ROCKETMQ header 长度 = calTotalLen');

        // 字段序：code(2) language(1) version(2) opaque(4) flag(4) remark(int+utf8) ext(int+map)
        $fields = RocketMQSerializable::rocketMqProtocolDecode($headerData);
        $this->checkSame(RequestCode::HEART_BEAT, $fields['code'], 'ROCKETMQ 编码 code');
        $this->checkSame(LanguageCode::PHP->value, $fields['language'], 'ROCKETMQ 编码 language=PHP');
        $this->checkSame(RemotingCommand::CURRENT_VERSION, $fields['version'], 'ROCKETMQ 编码 version');
        $this->checkSame($cmd->opaque, $fields['opaque'], 'ROCKETMQ 编码 opaque');
        $this->checkSame(0, $fields['flag'], 'ROCKETMQ 编码 flag');
        $this->checkSame('bin-remark', $fields['remark'], 'ROCKETMQ 编码 remark');
        $this->checkSame('GID-bin', $fields['extFields']['producerGroup'] ?? '', 'ROCKETMQ 编码 extFields.producerGroup');
        $this->checkSame('false', $fields['extFields']['unitMode'] ?? '', 'ROCKETMQ 编码 extFields.unitMode(小写 false)');

        $decoded = RemotingCommand::decode($frame);
        $this->checkSame(SerializeType::ROCKETMQ->value, $decoded->serializeTypeCurrentRpc, 'ROCKETMQ 往返 serializeType');
        $this->checkSame($cmd->code, $decoded->code, 'ROCKETMQ 往返 code');
        $this->checkSame($cmd->opaque, $decoded->opaque, 'ROCKETMQ 往返 opaque');
        $this->checkSame($cmd->remark, $decoded->remark, 'ROCKETMQ 往返 remark');
        $this->checkSame($cmd->body, $decoded->body, 'ROCKETMQ 往返 body');
        $this->checkSame($cmd->extFields, $decoded->extFields, 'ROCKETMQ 往返 extFields 全等');

        // 与 Python serialize 的字段序一致性：手拼 Python 顺序的字节串应逐字节一致
        $extBytes = (string)RocketMQSerializable::mapSerialize($cmd->extFields);
        $expectHeader = pack('n', $cmd->code & 0xFFFF)
            . pack('C', $cmd->language & 0xFF)
            . pack('n', $cmd->version & 0xFFFF)
            . pack('N', $cmd->opaque)
            . pack('N', $cmd->flag)
            . pack('N', strlen('bin-remark')) . 'bin-remark'
            . pack('N', strlen($extBytes)) . $extBytes;
        $this->checkSame($expectHeader, $headerData, 'ROCKETMQ header 与 Python 字段序逐字节一致');

        // writeDecimalLong 边界
        $buf = '';
        RocketMQSerializable::writeDecimalLong($buf, 0);
        $this->checkSame(pack('N', 1) . '0', $buf, 'writeDecimalLong(0)');
        $buf = '';
        RocketMQSerializable::writeDecimalLong($buf, -123);
        $this->checkSame(pack('N', 4) . '-123', $buf, 'writeDecimalLong(-123)');
        $buf = '';
        RocketMQSerializable::writeDecimalLong($buf, PHP_INT_MIN);
        $this->checkSame(pack('N', 20) . '-9223372036854775808', $buf, 'writeDecimalLong(PHP_INT_MIN)');

        // map 往返（key short 长度 / value int 长度）
        $map = ['k1' => 'v1', '中文' => '值'];
        $bin = (string)RocketMQSerializable::mapSerialize($map);
        [$back, $off] = RocketMQSerializable::mapDeserialize($bin, 0, strlen($bin));
        $this->checkSame($map, $back, 'mapSerialize/mapDeserialize 往返');
        $this->checkSame(strlen($bin), $off, 'mapDeserialize 消费完全部字节');
        // 空串 value 编码成长度 0 → readStr 还原为 null → 该键被丢弃
        //（Java RocketMQSerializable / Python serialize.py 同样有损，非移植缺陷）
        $lossy = (string)RocketMQSerializable::mapSerialize(['empty' => '']);
        [$backLossy, $offLossy] = RocketMQSerializable::mapDeserialize($lossy, 0, strlen($lossy));
        $this->checkSame([], $backLossy, '空串 value 往返有损（长度0→null，与 Java/Python 一致）');
        $this->checkSame(strlen($lossy), $offLossy, '有损用例仍消费完全部字节');
        $this->checkSame(null, RocketMQSerializable::mapSerialize(null), 'mapSerialize(null) → null');
        $this->checkSame(null, RocketMQSerializable::mapSerialize([]), 'mapSerialize([]) → null');

        // readStr 长度 0 → null
        $this->checkSame([null, 4], RocketMQSerializable::readStr(pack('N', 0), 0, false), 'readStr 长度0 → null');
    }

    // ==================================================================== header 快照

    private function testHeaderSnapshot(): void
    {
        $h = new SendMessageRequestHeader();
        $h->producerGroup = 'GID';
        $h->topic = 'T';
        $h->defaultTopic = 'TBW102';
        $h->defaultTopicQueueNums = 8;
        $h->queueId = 2;
        $h->sysFlag = 4;
        $h->bornTimestamp = 1718000000000;
        $h->flag = 0;
        $h->properties = 'KEYS=1';
        $h->reconsumeTimes = 3;
        $h->unitMode = true;
        $h->maxReconsumeTimes = 16;
        $h->batch = false;
        $this->checkSame([
            'producerGroup' => 'GID',
            'topic' => 'T',
            'defaultTopic' => 'TBW102',
            'defaultTopicQueueNums' => 8,
            'queueId' => 2,
            'sysFlag' => 4,
            'bornTimestamp' => 1718000000000,
            'flag' => 0,
            'properties' => 'KEYS=1',
            'reconsumeTimes' => 3,
            'unitMode' => 'true',
            'maxReconsumeTimes' => 16,
            'batch' => 'false',
        ], $h->toExtFields(), 'SendMessageRequestHeader.toExtFields 字段名/顺序/bool 小写快照');

        // null 字段不落 ext
        $h2 = new SendMessageRequestHeader();
        $h2->topic = 'T';
        $this->checkSame(['topic' => 'T'], $h2->toExtFields(), 'null 字段不写键');

        // V2 短字段名
        $v2 = new SendMessageRequestHeaderV2();
        $v2->producerGroup = 'GID';
        $v2->queueId = 5;
        $v2->brokerName = 'broker-a';
        $this->checkSame(['a' => 'GID', 'e' => 5, 'n' => 'broker-a'], $v2->toExtFields(), 'SendMessageRequestHeaderV2 短字段名');

        // fromExtFields 回读（含类型转换）
        $h3 = new SendMessageRequestHeader();
        $h3->fromExtFields(['producerGroup' => 'G', 'queueId' => '7', 'bornTimestamp' => '123', 'unitMode' => 'True', 'batch' => '0']);
        $this->checkSame('G', $h3->producerGroup, 'fromExtFields string');
        $this->checkSame(7, $h3->queueId, 'fromExtFields int');
        $this->checkSame(123, $h3->bornTimestamp, 'fromExtFields long');
        $this->check(true === $h3->unitMode, 'fromExtFields bool(true)');
        $this->check(false === $h3->batch, 'fromExtFields bool(0=false)');

        // PopMessageRequestHeader.order 总在报文里（Java 非空 Boolean）
        $pop = new PopMessageRequestHeader();
        $pop->topic = 't';
        $ext = $pop->toExtFields();
        $this->check(array_key_exists('order', $ext) && $ext['order'] === 'false', 'PopMessageRequestHeader.order 恒在且小写');

        // SearchOffsetRequestHeader.boundaryType 入网为枚举名大写
        $so = new SearchOffsetRequestHeader();
        $so->topic = 't';
        $so->boundaryType = BoundaryType::UPPER;
        $this->checkSame(['topic' => 't', 'boundaryType' => 'UPPER'], $so->toExtFields(), 'SearchOffsetRequestHeader boundaryType=UPPER');
    }

    // ==================================================================== ACL 签名

    private function testAclSignature(): void
    {
        Logger::setHandler(static function (string $line): void {}); // 测试期间静音

        $sk = 'sk-fixed-123';
        $hook = new AclClientRPCHook(new SessionCredentials('ak-fixed', $sk));

        $header = new SendMessageRequestHeader();
        $header->producerGroup = 'GID-sig';
        $header->topic = 'SigTopic';
        $header->queueId = 0;
        $cmd = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $cmd->body = 'sig-payload';

        $hook->doBeforeRequest('127.0.0.1:10911', $cmd);
        $this->checkSame('ak-fixed', $cmd->getExtField('AccessKey') ?? '', 'ACL AccessKey 已注入');
        $sig = $cmd->getExtField('Signature') ?? '';
        $this->checkSame(28, strlen($sig), 'HmacSHA1 Base64 长度恒为 28');

        // 独立复算：字典序取 extFields 全部 value（排除 Signature）+ body
        $cmd->makeCustomHeaderToNet();
        $fields = $cmd->extFields;
        unset($fields['Signature']);
        ksort($fields, SORT_STRING);
        $content = implode('', array_map(strval(...), array_values($fields))) . 'sig-payload';
        $expect = base64_encode(hash_hmac('sha1', $content, $sk, true));
        $this->checkSame($expect, $sig, '签名 = Base64(HmacSHA1(secretKey, 排序 value + body)) 可复算');

        // 固定输入 → 确定性签名（同输入两次构建一致）
        $cmd2 = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $cmd2->body = 'sig-payload';
        $hook->doBeforeRequest('127.0.0.1:10911', $cmd2);
        $this->checkSame($sig, $cmd2->getExtField('Signature') ?? '', '同输入签名确定');

        // Signature 不参与签名内容
        $raw = AclClientRPCHook::buildRequestContent($cmd2);
        $this->check(!str_contains($raw, $sig), 'Signature 不参与签名内容');

        // securityToken 参与签名
        $hookTok = new AclClientRPCHook(new SessionCredentials('ak', 'sk', 'st-1'));
        $cmd4 = new RemotingCommand(RequestCode::HEART_BEAT);
        $hookTok->doBeforeRequest('addr', $cmd4);
        $this->checkSame('st-1', $cmd4->getExtField('SecurityToken') ?? '', 'SecurityToken 已注入');

        // AclRpcHook 是 AclClientRPCHook 的兼容别名（对应 Python 的 AclRPCHook = AclClientRPCHook）
        $alias = new AclRpcHook(new SessionCredentials('a', 'b'));
        $this->check($alias instanceof AclClientRPCHook, 'AclRpcHook 兼容别名');
    }

    // ==================================================================== namespaceV2 钩子

    /**
     * 5.x 服务端命名空间钩子（org.apache.rocketmq.client.rpchook.NamespaceRpcHook）。
     *
     * 对齐基准（逐行读过 Java 5.5.1）：
     *   * doBeforeRequest 只有一段：namespaceV2 非空时加 **两个** 扩展头
     *     nsd="true" / ns=<namespaceV2>（MixAll.java:122-123）；doAfterResponse 空实现。
     *   * 空命名空间必须 **完全不碰** extFields —— Java 的 NamespaceRpcHookTest 断言的
     *     就是「未配置时请求原样」，把 extFields 初始化成空 map 都算错（上线会多一个空对象）。
     *   * 值是**每笔请求现读**的（Java 读 clientConfig.getNamespaceV2()），所以 start() 之后
     *     改配置也从下一笔请求生效；持字符串快照的写法会把这条路堵死。
     *   * 注册顺序 Namespace → Stream → 用户钩子(ACL) 是语义不是风格（MQClientAPIImpl:329-335）：
     *     nsd/ns 必须先进 extFields 才算得进签名，装反了签名「看着合法」但开鉴权的 broker
     *     验签多出两个未签字段直接拒签。
     *
     * 与 python/tests/test_namespace_rpc_hook.py、csharp ClientParityTests、
     * nodeJs test/namespace_rpc_smoke.ts、cpp tests/test_namespace_hook.cpp 同题。
     */
    private function testNamespaceRpcHook(): void
    {
        Logger::setHandler(static function (string $line): void {}); // 测试期间静音

        // 字段名与 Java 常量逐字一致（拼错 = broker 认不出命名空间，静默失败）
        $this->checkSame('nsd', MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD, 'nsd 字段名');
        $this->checkSame('ns', MixAll::RPC_REQUEST_HEADER_NAMESPACE_FIELD, 'ns 字段名');

        $ns = 'MQ_INST_php_parity';
        $hook = new NamespaceRpcHook($ns);
        $cmd = new RemotingCommand(RequestCode::SEND_MESSAGE);
        $hook->doBeforeRequest('127.0.0.1:10911', $cmd);
        $this->checkSame('true', $cmd->getExtField('nsd') ?? '', 'nsd=true');
        $this->checkSame($ns, $cmd->getExtField('ns') ?? '', 'ns=<namespaceV2>');

        // doAfterResponse 是空实现（基类空方法，与 Java 的空方法体逐字一致）
        $after = $cmd->extFields;
        $hook->doAfterResponse('127.0.0.1:10911', $cmd, null);
        $this->checkSame($after, $cmd->extFields, 'doAfterResponse 不动请求');

        // 空命名空间 = no-op，而且连 extFields 都不碰（'' 与 null 两条腿）
        foreach (['', null] as $unset) {
            $bare = new RemotingCommand(RequestCode::SEND_MESSAGE);
            $before = $bare->extFields;
            (new NamespaceRpcHook($unset))->doBeforeRequest('a', $bare);
            $this->checkSame($before, $bare->extFields,
                '未配命名空间时 extFields 原样（' . var_export($unset, true) . '）');
            $this->check(!array_key_exists('ns', $bare->extFields), 'ns 没被写成空串');
            $this->check(!array_key_exists('nsd', $bare->extFields), 'nsd 没被写成空串');
        }

        // 取值函数每笔请求现读：先空后配，第二笔必须带上（Java 的 clientConfig 语义）
        $current = '';
        $live = new NamespaceRpcHook(function () use (&$current): string {
            return $current;
        });
        $first = new RemotingCommand(RequestCode::SEND_MESSAGE);
        $live->doBeforeRequest('a', $first);
        $this->check(!array_key_exists('ns', $first->extFields), '现读：第一笔还没配命名空间');
        $current = $ns;
        $second = new RemotingCommand(RequestCode::SEND_MESSAGE);
        $live->doBeforeRequest('a', $second);
        $this->checkSame($ns, $second->getExtField('ns') ?? '', '现读：改完配置下一笔就生效');
        $current = $ns . '_2';
        $third = new RemotingCommand(RequestCode::SEND_MESSAGE);
        $live->doBeforeRequest('a', $third);
        $this->checkSame($ns . '_2', $third->getExtField('ns') ?? '', '现读：再改再跟');

        // 顺序：Namespace 排在 ACL 之前 ⇒ ns/nsd 进了签名内容，broker 复算得同一份签名
        $sk = 'sk-ns-123';
        $acl = new AclClientRPCHook(new SessionCredentials('ak-ns', $sk));
        $header = new SendMessageRequestHeader();
        $header->producerGroup = 'GID-ns';
        $header->topic = 'NsTopic';
        $header->queueId = 0;

        $signed = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $signed->body = 'ns-payload';
        $hook->doBeforeRequest('a', $signed);          // 先写 nsd/ns
        $acl->doBeforeRequest('a', $signed);           // 再算签名（Java 的顺序）
        $sigWithNs = $signed->getExtField('Signature') ?? '';
        $this->check($sigWithNs !== '', '链式签名已生成');

        // 独立复算 broker 视角：字典序 value（排除 Signature）+ body
        $verify = $signed->extFields;
        unset($verify[SessionCredentials::SIGNATURE]);
        ksort($verify, SORT_STRING);
        $content = implode('', array_map(strval(...), array_values($verify))) . 'ns-payload';
        $this->checkSame(base64_encode(hash_hmac('sha1', $content, $sk, true)), $sigWithNs,
            'nsd/ns 在签名内容里（broker 按上线字段复算能通过）');

        // 反证：同一请求若不先装 Namespace 钩子，签名必然不同 —— 顺序写反就是验签失败
        $noNs = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $noNs->body = 'ns-payload';
        $acl->doBeforeRequest('a', $noNs);
        $this->check($sigWithNs !== ($noNs->getExtField('Signature') ?? ''),
            '不先装 Namespace 就签不出同一份内容');

        // Stream 也在 ACL 之前：ReqT 与 ns 同时进签名
        $chain = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $chain->body = 'ns-payload';
        (new NamespaceRpcHook($ns))->doBeforeRequest('a', $chain);
        (new StreamTypeRPCHook())->doBeforeRequest('a', $chain);
        $acl->doBeforeRequest('a', $chain);
        $this->checkSame('true', $chain->getExtField('nsd') ?? '', '链上 nsd 仍在');
        $this->check(($chain->getExtField('ReqT') ?? '') !== '', '链上 ReqT 已写入');
        $chainVerify = $chain->extFields;
        unset($chainVerify[SessionCredentials::SIGNATURE]);
        ksort($chainVerify, SORT_STRING);
        $chainContent = implode('', array_map(strval(...), array_values($chainVerify))) . 'ns-payload';
        $this->checkSame(base64_encode(hash_hmac('sha1', $chainContent, $sk, true)),
            $chain->getExtField('Signature') ?? '', 'ns + ReqT 一并进签名（Java MQClientAPIImpl:329-335）');
    }

    // ==================================================================== 路由解码

    private function testRouteDecode(): void
    {
        // fastjson2 非标准 JSON：数字键不带引号
        $raw = '{"orderTopicConf":null,'
            . '"queueDatas":[{"brokerName":"broker-a","readQueueNums":8,"writeQueueNums":8,"perm":6,"topicSysFlag":0},'
            . '{"brokerName":"broker-b","readQueueNums":4,"writeQueueNums":4,"perm":4,"topicSysFlag":0}],'
            . '"brokerDatas":[{"brokerAddrs":{0:"10.0.0.1:10911",1:"10.0.0.1:10921"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false},'
            . '{"brokerAddrs":{0:"10.0.0.2:10911"},"brokerName":"broker-b","cluster":"DefaultCluster","enableActingMaster":false}],'
            . '"filterServerTable":{}}';
        $route = TopicRouteData::decode($raw);
        $this->checkSame(2, count($route->queueDatas), 'fastjson 数字键解码 queueDatas');
        $this->checkSame(2, count($route->brokerDatas), 'fastjson 数字键解码 brokerDatas');
        $this->checkSame('broker-a', $route->queueDatas[0]->brokerName, 'QueueData.brokerName');
        $this->checkSame(8, $route->queueDatas[0]->readQueueNums, 'QueueData.readQueueNums');
        $this->checkSame([0 => '10.0.0.1:10911', 1 => '10.0.0.1:10921'], $route->brokerDatas[0]->brokerAddrs, 'BrokerData.brokerAddrs 数字键还原');

        // 发布侧：可写 + master 才收
        $mqs = $route->getAllMessageQueue('T');
        $this->checkSame(8, count($mqs), '发布侧 broker-a 8 个写队列');
        $this->check($mqs[0]->topic === 'T' && $mqs[0]->brokerName === 'broker-a' && $mqs[0]->queueId === 0, 'MessageQueue(topic 回填, broker, queueId)');
        // broker-b 只有读位（perm=4）→ 不进发布信息
        $names = array_map(static fn(MessageQueue $mq): string => $mq->brokerName, $mqs);
        $this->check(!in_array('broker-b', $names, true), 'perm=4 的 broker 不进发布侧');
        // 订阅侧：只看读位，不要求 master
        $subMqs = $route->getAllSubscribeMessageQueue('T');
        $this->checkSame(12, count($subMqs), '订阅侧 8+4 读队列');

        // master 缺失的 broker 不进发布信息
        $route2 = new TopicRouteData();
        $qd = new QueueData('b1', 2, 2, 6);
        $route2->queueDatas = [$qd];
        $bdNoMaster = new BrokerData('c', 'b1', [1 => '10.0.0.9:10921']);
        $route2->brokerDatas = [$bdNoMaster];
        $this->checkSame([], $route2->getAllMessageQueue('T'), '无 master 的 broker 跳过（发布侧）');
        $this->checkSame(2, count($route2->getAllSubscribeMessageQueue('T')), '无 master 照样可订阅（订阅侧）');

        $this->checkSame('10.0.0.1:10911', $route->brokerDatas[0]->selectBrokerAddr(), 'selectBrokerAddr 优先 master');
        $this->checkSame('10.0.0.9:10921', $route2->brokerDatas[0]->selectBrokerAddr(), '无 master 随机取（单元素）');
        $this->checkSame(null, (new BrokerData('c', 'b2', []))->selectBrokerAddr(), '空 brokerAddrs → null');

        // 严格 JSON 也能解
        $strict = json_encode($route->toDict(), JSON_UNESCAPED_SLASHES);
        $route3 = TopicRouteData::decode((string)$strict);
        $this->checkSame(2, count($route3->queueDatas), '严格 JSON 解码 queueDatas');
        $this->check($route3->topicRouteDataChanged($route) === false, '内容相同 → 路由未变化');
        $route3->queueDatas[0]->readQueueNums = 9;
        $this->check($route3->topicRouteDataChanged($route) === true, 'queueNums 变化 → 路由变化');

        // encode → decode 往返
        $route4 = TopicRouteData::decode($route->encode());
        $this->checkSame($route->queueDatas[0]->brokerName, $route4->queueDatas[0]->brokerName, 'TopicRouteData encode/decode 往返');
    }

    // ==================================================================== 心跳

    private function testHeartbeat(): void
    {
        $hb = new HeartbeatData('cid-php-1');
        $hb->producerDataSet[] = new ProducerData('GID-p');
        $cd = new ConsumerData('GID-c');
        $sub = new SubscriptionData('TopicTest', '*');
        $sub->subVersion = 42;
        $cd->subscriptionDataSet[] = $sub;
        $hb->consumerDataSet[] = $cd;

        $obj = $hb->toDict();
        $this->checkSame('cid-php-1', $obj['clientID'], 'HeartbeatData.clientID 键名');
        $this->checkSame(0, $obj['heartbeatFingerprint'], 'heartbeatFingerprint 恒 0（V1 注册路径）');
        $this->checkSame(false, $obj['withoutSub'], 'withoutSub 恒 false');
        $this->checkSame('GID-c', $obj['consumerDataSet'][0]['groupName'], 'ConsumerData.groupName');
        $this->checkSame('CLUSTERING', $obj['consumerDataSet'][0]['messageModel'], 'ConsumerData.messageModel');
        $this->checkSame('*', $obj['consumerDataSet'][0]['subscriptionDataSet'][0]['subString'], 'subscriptionDataSet 走 toDict');
        $this->checkSame('TAG', $obj['consumerDataSet'][0]['subscriptionDataSet'][0]['expressionType'], 'expressionType 默认 TAG');

        $back = HeartbeatData::decode($hb->encode());
        $this->checkSame($hb->clientId, $back->clientId, 'HeartbeatData 往返 clientId');
        $this->checkSame(1, count($back->consumerDataSet), 'HeartbeatData 往返 consumerDataSet');
        $this->checkSame('*', $back->consumerDataSet[0]->subscriptionDataSet[0]->subString ?? '', 'HeartbeatData 往返 subscription');
        $this->checkSame(42, $back->consumerDataSet[0]->subscriptionDataSet[0]->subVersion ?? 0, 'HeartbeatData 往返 subVersion');
        $this->checkSame('GID-p', $back->producerDataSet[0]->groupName ?? '', 'HeartbeatData 往返 producerDataSet');
    }

    // ==================================================================== ExtraInfo / Namespace

    private function testExtraInfoAndNamespace(): void
    {
        $ck = ExtraInfoUtil::buildExtraInfo(5, 1000, 30000, 2, 'TopicTest', 'broker-a', 3, 17);
        $this->checkSame('5 1000 30000 2 0 broker-a 3 17', $ck, 'buildExtraInfo 8 段（普通 topic，空格分隔）');
        $segs = ExtraInfoUtil::split($ck);
        $this->checkSame(8, count($segs), 'split 得 8 段');
        $this->checkSame(5, ExtraInfoUtil::getCkQueueOffset($segs), 'getCkQueueOffset');
        $this->checkSame(1000, ExtraInfoUtil::getPopTime($segs), 'getPopTime');
        $this->checkSame(30000, ExtraInfoUtil::getInvisibleTime($segs), 'getInvisibleTime');
        $this->checkSame(2, ExtraInfoUtil::getReviveQid($segs), 'getReviveQid');
        $this->checkSame('0', ExtraInfoUtil::getRetry($segs), 'getRetry(普通)');
        $this->checkSame('broker-a', ExtraInfoUtil::getBrokerName($segs), 'getBrokerName');
        $this->checkSame(3, ExtraInfoUtil::getQueueId($segs), 'getQueueId');
        $this->checkSame(17, ExtraInfoUtil::getQueueOffset($segs), 'getQueueOffset');
        $this->check(ExtraInfoUtil::isOrder($segs) === false, '非 999 不是顺序消费');

        $ck7 = ExtraInfoUtil::buildExtraInfo(1, 2, 3, 4, 'TopicTest', 'b', 0);
        $this->checkSame('1 2 3 4 0 b 0', $ck7, 'buildExtraInfo 7 段');
        $this->checkThrows(static fn(): int => ExtraInfoUtil::getQueueOffset(ExtraInfoUtil::split($ck7)), \InvalidArgumentException::class, '段数不足抛异常');

        // retry topic 形状
        $this->checkSame('1', ExtraInfoUtil::retryOfTopic('%RETRY%GID_topic'), 'V1 retry');
        $this->checkSame('2', ExtraInfoUtil::retryOfTopic('%RETRY%GID+topic'), 'V2 retry');
        $this->checkSame('0', ExtraInfoUtil::retryOfTopic('NormalTopic'), '普通');
        $this->checkSame('%RETRY%GID_topic', ExtraInfoUtil::getRealTopic('topic', 'GID', '1'), 'getRealTopic V1');
        $this->checkSame('%RETRY%GID+topic', ExtraInfoUtil::getRealTopic('topic', 'GID', '2'), 'getRealTopic V2');
        $this->checkSame('topic', ExtraInfoUtil::getRealTopic('topic', 'GID', '0'), 'getRealTopic 普通');
        $this->checkSame('%RETRY%G_topic', ExtraInfoUtil::buildPopRetryTopicV1('topic', 'G'), 'buildPopRetryTopicV1');
        $this->checkSame('%RETRY%G+topic', ExtraInfoUtil::buildPopRetryTopicV2('topic', 'G'), 'buildPopRetryTopicV2');
        $this->check(ExtraInfoUtil::isPopRetryTopicV2('%RETRY%G+topic'), 'isPopRetryTopicV2');
        $this->check(!ExtraInfoUtil::isPopRetryTopicV2('%RETRY%G_topic'), 'V1 不是 V2');
        $orderSegs = ExtraInfoUtil::split(ExtraInfoUtil::buildExtraInfo(0, 0, 0, 999, 't', 'b', 0));
        $this->check(ExtraInfoUtil::isOrder($orderSegs), 'reviveQid=999 → isOrder');

        // startOffsetInfo / msgOffsetInfo
        $start = ExtraInfoUtil::parseStartOffsetInfo('0 3 0;0 2 0');
        $this->checkSame(['0@3' => 0, '0@2' => 0], $start, 'parseStartOffsetInfo');
        $msg = ExtraInfoUtil::parseMsgOffsetInfo('0 3 0,1,2;0 2 5');
        $this->checkSame([0, 1, 2], $msg['0@3'] ?? [], 'parseMsgOffsetInfo 列表');
        $this->checkSame([5], $msg['0@2'] ?? [], 'parseMsgOffsetInfo 单条');
        $order = ExtraInfoUtil::parseOrderCountInfo('0 3 2');
        $this->checkSame(['0@3' => 2], $order, 'parseOrderCountInfo');
        $this->checkSame(null, ExtraInfoUtil::parseStartOffsetInfo(null), 'null → null');
        $this->checkThrows(static fn(): ?array => ExtraInfoUtil::parseStartOffsetInfo('0 3'), \InvalidArgumentException::class, 'startOffsetInfo 段数错');

        $this->checkSame('0@3', ExtraInfoUtil::getStartOffsetInfoMapKey('TopicTest', 3), 'getStartOffsetInfoMapKey');
        $this->checkSame('qo3%17', ExtraInfoUtil::getQueueOffsetKeyValueKey(3, 17), 'getQueueOffsetKeyValueKey');
        $this->checkSame('0@qo3%17', ExtraInfoUtil::getQueueOffsetMapKey('TopicTest', 3, 17), 'getQueueOffsetMapKey');
        $this->checkSame('2@qo0%9', ExtraInfoUtil::getQueueOffsetMapKey('%RETRY%G+t', 0, 9), 'V2 topic 的 map key');

        // NamespaceUtil
        $this->checkSame('Topic', NamespaceUtil::withoutNamespace('MQ_INST_XX%Topic'), 'withoutNamespace 普通');
        $this->checkSame('%RETRY%GID', NamespaceUtil::withoutNamespace('%RETRY%MQ_INST_XX%GID'), 'withoutNamespace retry');
        $this->checkSame('MQ_INST_YY%Topic', NamespaceUtil::withoutNamespace('MQ_INST_YY%Topic', 'MQ_INST_XX'), '命名空间不符原样返回');
        $this->checkSame('MQ_INST_XX%Topic', NamespaceUtil::wrapNamespace('MQ_INST_XX', 'Topic'), 'wrapNamespace');
        $this->checkSame('%RETRY%MQ_INST_XX%GID', NamespaceUtil::wrapNamespaceAndRetry('MQ_INST_XX', 'GID'), 'wrapNamespaceAndRetry');
        $this->checkSame('MQ_INST_XX', NamespaceUtil::getNamespaceFromResource('MQ_INST_XX%Topic'), 'getNamespaceFromResource');
        $this->checkSame('', NamespaceUtil::getNamespaceFromResource('rmq_sys_ROUTE'), '系统 topic 无命名空间');
        $this->check(!NamespaceUtil::isAlreadyWithNamespace('Topic', 'NS'), '未带命名空间判定');
        $this->check(NamespaceUtil::isAlreadyWithNamespace('NS%Topic', 'NS'), '已带命名空间判定');
    }

    // ==================================================================== fastjson 兼容解析

    private function testFastjsonParser(): void
    {
        $v = RemotingSerializable::fastjsonLoads('{"a":1,"b":[1,2,{"c":"x"}],"d":{0:"z",1:"y"},"e":true,"f":null}');
        $this->checkSame(1, $v['a'], 'fastjson 数字值');
        $this->checkSame('x', $v['b'][2]['c'], 'fastjson 嵌套');
        $this->checkSame('z', $v['d']['0'], 'fastjson 裸数字键');
        $this->checkSame(true, $v['e'], 'fastjson true');
        $this->checkSame(null, $v['f'], 'fastjson null');

        $v2 = RemotingSerializable::fastjsonLoads('{"g":1.5,"h":NaN,"i":Infinity,"j":-Infinity,}');
        $this->checkSame(1.5, $v2['g'], 'fastjson 浮点');
        $this->check(is_nan($v2['h']) && is_infinite($v2['i']) && $v2['j'] === -INF, 'fastjson NaN/Infinity 容忍');
        $this->check($v2 !== null, 'fastjson 尾随逗号容忍');

        // MessageQueue 内联对象键
        $v3 = RemotingSerializable::fastjsonLoads(
            '{"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{"minOffset":0,"maxOffset":9}}}'
        );
        $this->check(isset($v3['offsetTable']), '内联对象键可解析');
        $key = (string)array_key_first($v3['offsetTable']);
        $mq = RemotingSerializable::decodeMessageQueueKey($key);
        $this->check($mq !== null && $mq['topic'] === 't' && $mq['brokerName'] === 'b' && $mq['queueId'] === 3, 'decodeMessageQueueKey 还原');
        $this->checkSame(null, RemotingSerializable::decodeMessageQueueKey('plain-key'), '普通字符串键 → null');

        // 中文 / 转义
        $v4 = RemotingSerializable::fastjsonLoads('{"s":"中文\\n\"q\""}');
        $this->checkSame("中文\n\"q\"", $v4['s'], 'fastjson 中文与转义');
        $this->checkThrows(static fn() => RemotingSerializable::fastjsonLoads('{"a":}'), FastJsonDecodeError::class, '非法输入抛 FastJsonDecodeError');

        // encode
        $this->checkSame('', RemotingSerializable::encode(null), 'encode(null) → 空串');
        $this->checkSame('raw', RemotingSerializable::encode('raw'), 'encode(bytes) 原样');
        $this->checkSame('{"k":"v","u":"中/文"}', RemotingSerializable::encode(['k' => 'v', 'u' => '中/文']), 'encode 不转义中文与斜杠');
    }

    // ==================================================================== TCP 端到端

    /** @return array{0: resource, 1: string} [server, addr] */
    private static function startEchoServer(): array
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            throw new \RuntimeException("cannot start test server: {$errstr}");
        }
        $name = stream_socket_get_name($server, false);
        return [$server, (string)$name];
    }

    /** 精确读 n 字节（阻塞）。 */
    private static function readExact($stream, int $n): string
    {
        $buf = '';
        while (strlen($buf) < $n) {
            $chunk = fread($stream, $n - strlen($buf));
            if ($chunk === false || $chunk === '') {
                throw new \RuntimeException('test server: unexpected EOF');
            }
            $buf .= $chunk;
        }
        return $buf;
    }

    /** 从 server 侧读走一整帧并解码。 */
    private static function readFrame($conn): RemotingCommand
    {
        $total = RocketMQSerializable::unpackSignedInt(self::readExact($conn, 4));
        return RemotingCommand::decode(pack('N', $total) . self::readExact($conn, $total));
    }

    /**
     * 建连并 accept。
     *
     * 客户端建连是**惰性**的（与 Python 一致：首次 send 才 connect），而 PHP 单线程里
     * `invokeSync` 会一直阻塞在读响应上。所以测试先用一次 `invokeOneway` 把 TCP 连接
     * 建起来（oneway 只写不等响应），再从 server 侧 accept，并丢弃这帧握手请求。
     *
     * @param resource $server
     * @return resource 已 accept 的 server 侧连接（阻塞模式）
     */
    private static function connectAndAccept(RemotingClient $client, string $addr, $server)
    {
        $client->invokeOneway($addr, RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT));
        $conn = stream_socket_accept($server, 5);
        if ($conn === false) {
            throw new \RuntimeException('test server: accept failed');
        }
        stream_set_blocking($conn, true);
        self::readFrame($conn); // 丢弃握手 oneway 帧
        return $conn;
    }

    private function testTcpEndToEnd(): void
    {
        // ---- 1. 同步 RPC：server 校验请求并回响应；连接复用 ----
        [$server, $addr] = self::startEchoServer();
        $client = new RemotingClient(1000, 3000);

        $header = new SendMessageRequestHeader();
        $header->producerGroup = 'GID-e2e';
        $header->topic = 'E2E';
        $header->queueId = 1;
        $req = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $req->body = 'e2e-body';

        $conn = self::connectAndAccept($client, $addr, $server);
        $this->check(is_resource($conn), 'e2e: server accept');

        // 单线程里 invokeSync 会阻塞在读响应上，服务端代码跑不到 → 先把响应写进连接缓冲
        // （opaque 构造时已知），等 invokeSync 返回后再回读服务端收到的请求帧做断言。
        $respHeader = new SendMessageResponseHeader();
        $respHeader->msgId = 'MSG-1';
        $respHeader->queueId = 1;
        $respHeader->queueOffset = 99;
        $resp = RemotingCommand::createResponseCommandWithHeader(ResponseCode::SUCCESS, $respHeader);
        $resp->opaque = $req->opaque;
        $resp->body = 'resp-body';
        fwrite($conn, $resp->encode());

        $got = $client->invokeSync($addr, $req, 3000);
        $this->checkSame(ResponseCode::SUCCESS, $got->code, 'e2e: invokeSync 拿到响应 code');
        $this->checkSame($req->opaque, $got->opaque, 'e2e: opaque 对上');
        $this->checkSame('resp-body', $got->body, 'e2e: 响应 body');
        $hdr = $got->decodeCommandCustomHeader(SendMessageResponseHeader::class);
        $this->check($hdr instanceof SendMessageResponseHeader, 'decodeCommandCustomHeader 返回 header 对象');
        $this->checkSame('MSG-1', $hdr->msgId, 'decodeCommandCustomHeader msgId');
        $this->checkSame(1, $hdr->queueId, 'decodeCommandCustomHeader queueId(int 转换)');
        $this->checkSame(99, $hdr->queueOffset, 'decodeCommandCustomHeader queueOffset');

        // 回读服务端侧真正收到的请求帧
        $srvCmd = self::readFrame($conn);
        $this->checkSame(RequestCode::SEND_MESSAGE, $srvCmd->code, 'e2e: server 收到请求 code');
        $this->checkSame('e2e-body', $srvCmd->body, 'e2e: server 收到请求 body');
        $this->checkSame('GID-e2e', $srvCmd->getExtField('producerGroup') ?? '', 'e2e: server 收到 header 落地字段');
        $this->check(!$srvCmd->isResponseType(), 'e2e: 请求不带响应标志');

        // 连接复用：第二个请求走同一连接
        $req2 = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        $resp2 = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'hb-ok');
        $resp2->opaque = $req2->opaque;
        fwrite($conn, $resp2->encode());
        $got2 = $client->invokeSync($addr, $req2, 3000);
        $this->checkSame('hb-ok', $got2->remark ?? '', 'e2e: 复用连接 invokeSync 成功');
        $srvCmd2 = self::readFrame($conn);
        $this->checkSame(RequestCode::HEART_BEAT, $srvCmd2->code, 'e2e: 复用连接上的第二个请求');
        fclose($conn);
        $client->shutdown();
        fclose($server);

        // ---- 2. oneway + broker 主动请求 + 异步 ----
        [$server2, $addr2] = self::startEchoServer();
        $client2 = new RemotingClient(1000, 3000);
        $connA = self::connectAndAccept($client2, $addr2, $server2);

        // 2a. oneway：flag 位带 oneway 标记
        $ow = RemotingCommand::createRequestCommand(RequestCode::NOTIFY_CONSUMER_IDS_CHANGED, new NotifyConsumerIdsChangedRequestHeader());
        $client2->invokeOneway($addr2, $ow);
        $srvO = self::readFrame($connA);
        $this->check($srvO->isOnewayRPC(), 'oneway 请求 flag 位');

        // 2b. broker 主动请求（processor）：server 发 CHECK_TRANSACTION_STATE，客户端处理器回响应
        $seen = null;
        $client2->registerProcessor(RequestCode::CHECK_TRANSACTION_STATE, static function (RemotingCommand $c, string $from) use (&$seen): ?RemotingCommand {
            $seen = $c;
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'processed');
        });

        // PHP 单线程没有独立 IO 线程：broker 主动请求必须靠泵读进来。这里先发起 2c 要用的
        // 异步请求（写出去后 pending 非空），用它驱动 waitResponses 的读循环。
        $asyncResult = null;
        $asyncError = null;
        $reqA = RemotingCommand::createRequestCommand(RequestCode::GET_ROUTEINFO_BY_TOPIC, new GetRouteInfoRequestHeader());
        $client2->invokeAsync(
            $addr2,
            $reqA,
            static function (RemotingCommand $r) use (&$asyncResult): void {
                $asyncResult = $r;
            },
            static function (\Throwable $e) use (&$asyncError): void {
                $asyncError = $e;
            },
            3000
        );
        $srvA = self::readFrame($connA);
        $this->checkSame(RequestCode::GET_ROUTEINFO_BY_TOPIC, $srvA->code, 'e2e: 异步请求已上线');

        $pushReq = RemotingCommand::createRequestCommand(RequestCode::CHECK_TRANSACTION_STATE, new CheckTransactionStateRequestHeader());
        $pushReq->opaque = 4321;
        $pushReq->body = 'check-me';
        fwrite($connA, $pushReq->encode());
        // 泵一把：读到 pushReq → 交给 processor → 响应写回 connA（异步那条还没响应，pending 非空）
        $client2->waitResponses(300);
        $gotResp = self::readFrame($connA);
        $this->check($seen !== null && $seen->body === 'check-me', 'processor 收到 broker 主动请求');
        $this->check($gotResp->isResponseType(), 'processor 响应带响应标志');
        $this->checkSame(4321, $gotResp->opaque, 'processor 响应 opaque 回填');
        $this->checkSame('processed', $gotResp->remark ?? '', 'processor 响应 remark');
        $client2->unregisterProcessor(RequestCode::CHECK_TRANSACTION_STATE);

        // 2c. 异步：server 回响应后 waitResponses 泵到回调
        $respA = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'route-ok');
        $respA->opaque = $srvA->opaque;
        $respA->body = json_encode(['topicList' => ['T1', 'T2']]);
        fwrite($connA, $respA->encode());
        $client2->waitResponses(3000);
        $this->check($asyncError === null, 'e2e: 异步无失败回调');
        $this->check($asyncResult instanceof RemotingCommand && $asyncResult->opaque === $reqA->opaque, 'e2e: 异步回调拿到响应（opaque 对上）');
        $this->checkSame('route-ok', $asyncResult->remark ?? '', 'e2e: 异步响应 remark');
        $this->checkSame(['T1', 'T2'], json_decode((string)$asyncResult->body, true)['topicList'] ?? [], 'e2e: 异步响应 body');

        fclose($connA);
        $client2->shutdown();
        fclose($server2);

        // ---- 3. 超时 ----
        [$server3, $addr3] = self::startEchoServer();
        $client3 = new RemotingClient(1000, 3000);
        $reqT = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        $t0 = microtime(true);
        $this->checkThrows(static fn(): RemotingCommand => $client3->invokeSync($addr3, $reqT, 400), RemotingTimeoutException::class, '无响应 → RemotingTimeoutException');
        $elapsed = (microtime(true) - $t0) * 1000;
        $this->check($elapsed < 2000, sprintf('超时按时返回（%.0fms）', $elapsed));
        // 客户端连接已在 backlog 里，事后收走即可（server 全程不响应）
        $connT = stream_socket_accept($server3, 5);
        if (is_resource($connT)) {
            fclose($connT);
        }
        $client3->shutdown();
        fclose($server3);

        // ---- 4. 连接断开 → failFast（同步与异步）----
        [$server4, $addr4] = self::startEchoServer();
        $client4 = new RemotingClient(1000, 5000);
        $connF = self::connectAndAccept($client4, $addr4, $server4);

        // 4a. 同步：连接已被 server 关掉 → RemotingSendRequestException（不是超时）
        $reqF = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        fclose($connF); // server 侧断开；客户端下次 send 会重建连接
        $this->checkThrows(static fn(): RemotingCommand => $client4->invokeSync($addr4, $reqF, 5000), RemotingSendRequestException::class, '连接断开 → 同步报 RemotingSendRequestException');

        // 4b. 异步：请求已发出、响应未回时连接断开 → onFailure(RemotingSendRequestException)
        $asyncErr4 = null;
        $reqF2 = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        $client4->invokeAsync(
            $addr4,
            $reqF2,
            static function (RemotingCommand $r): void {},
            static function (\Throwable $e) use (&$asyncErr4): void {
                $asyncErr4 = $e;
            },
            5000
        );
        // 4a 已把旧连接关掉，这里的请求必定走新建连接
        $connF2 = stream_socket_accept($server4, 5);
        if (!is_resource($connF2)) {
            throw new \RuntimeException('test server: accept failed (4b)');
        }
        stream_set_blocking($connF2, true);
        self::readFrame($connF2); // 读走请求
        fclose($connF2);          // 不回响应直接断开
        $client4->waitResponses(2000);
        $this->check($asyncErr4 instanceof RemotingSendRequestException, '连接断开 → 异步 onFailure(RemotingSendRequestException)');
        $this->check(!$asyncErr4 instanceof RemotingTimeoutException, 'failFast 不是超时');

        fclose($server4);
        $client4->shutdown();

        // ---- 5. GO_AWAY 换连接重发一次 ----
        [$server5, $addr5] = self::startEchoServer();
        $client5 = new RemotingClient(1000, 3000);
        $connG = self::connectAndAccept($client5, $addr5, $server5);
        $reqG = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        $gotG = null;
        $errG = null;
        $client5->invokeAsync(
            $addr5,
            $reqG,
            static function (RemotingCommand $r) use (&$gotG): void {
                $gotG = $r;
            },
            static function (\Throwable $e) use (&$errG): void {
                $errG = $e;
            },
            3000
        );
        // 第一轮：server 回 GO_AWAY
        $srvG1 = self::readFrame($connG);
        $goAway = RemotingCommand::createResponseCommand(ResponseCode::GO_AWAY, 'busy');
        $goAway->opaque = $srvG1->opaque;
        fwrite($connG, $goAway->encode());
        // 泵一把：客户端在 dispatch 里关旧连接 + 换连接重发（新连接进 server backlog）
        $client5->waitResponses(300);
        // 第二轮：accept 新连接，读重发请求并回成功
        $connG2 = stream_socket_accept($server5, 5);
        if (!is_resource($connG2)) {
            throw new \RuntimeException('test server: accept failed (go-away retry)');
        }
        stream_set_blocking($connG2, true);
        $srvG2 = self::readFrame($connG2);
        $this->checkSame($reqG->code, $srvG2->code, 'GO_AWAY 后换连接重发同一请求');
        $this->check($srvG2->opaque !== $reqG->opaque, '重发请求换了新 opaque');
        $okResp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'after-goaway');
        $okResp->opaque = $srvG2->opaque;
        fwrite($connG2, $okResp->encode());
        $client5->waitResponses(3000);
        $this->check($errG === null, 'GO_AWAY 重发无失败回调');
        $this->checkSame('after-goaway', $gotG?->remark ?? '', 'GO_AWAY 重发拿到最终响应');
        fclose($connG);
        fclose($connG2);
        $client5->shutdown();
        fclose($server5);

        // ---- 6. namesrv 列表与轮询 ----
        $client6 = new RemotingClient(nameServers: ['ns-a:9876', 'ns-b:9876']);
        $this->checkSame(['ns-a:9876', 'ns-b:9876'], $client6->getNameServerAddressList(), 'getNameServerAddressList');
        $this->checkSame('ns-a:9876', $client6->chooseNameServer(), '轮询第 1 个');
        $this->checkSame('ns-b:9876', $client6->chooseNameServer(), '轮询第 2 个');
        $this->checkSame('ns-a:9876', $client6->chooseNameServer(), '轮询回绕');
        $client6->updateNameServerAddressList(['ns-c:9876']);
        $this->checkSame(['ns-c:9876'], $client6->getNameServerAddressList(), 'updateNameServerAddressList（故障切换入口）');
        $client6->shutdown();
    }

    public function run(): int
    {
        $this->testJsonFrameRoundtrip();
        $this->testRocketmqSerialization();
        $this->testHeaderSnapshot();
        $this->testAclSignature();
        $this->testNamespaceRpcHook();
        $this->testRouteDecode();
        $this->testHeartbeat();
        $this->testExtraInfoAndNamespace();
        $this->testFastjsonParser();
        $this->testTcpEndToEnd();
        return $this->summary();
    }
}

(new RunRemoting())->run();
exit(0);
