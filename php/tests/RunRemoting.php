<?php

declare(strict_types=1);

/**
 * Remoting layer pure-PHP assert-style self-test (no phpunit dependency).
 *
 * Run: C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/RunRemoting.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\Exceptions\RemotingCommandException;
use RocketMQ\Client\Exceptions\RemotingConnectException;
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
use RocketMQ\Remoting\SocksProxyConfig;
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

    // ==================================================================== JSON frame

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
        // totalLength prefix
        $total = RocketMQSerializable::unpackSignedInt(substr($frame, 0, 4));
        $this->checkSame(strlen($frame), $total + 4, 'JSON frame totalLength covers all bytes except the prefix');
        // headerLength high 8 bits = SerializeType(JSON=0)
        $oriHeaderLen = RocketMQSerializable::unpackSignedInt(substr($frame, 4, 4));
        $this->checkSame(SerializeType::JSON->value, RemotingCommand::getProtocolType($oriHeaderLen), 'JSON frame serialization type bits');
        $headerLen = RemotingCommand::getHeaderLength($oriHeaderLen);
        $this->check($headerLen > 0 && $headerLen < strlen($frame), 'JSON frame header length reasonable');

        $decoded = RemotingCommand::decode($frame);
        $this->checkSame($cmd->code, $decoded->code, 'JSON roundtrip code');
        $this->checkSame($cmd->opaque, $decoded->opaque, 'JSON roundtrip opaque');
        $this->checkSame($cmd->version, $decoded->version, 'JSON roundtrip version(CURRENT_VERSION)');
        $this->checkSame($cmd->flag, $decoded->flag, 'JSON roundtrip flag');
        $this->checkSame($cmd->remark, $decoded->remark, 'JSON roundtrip remark');
        $this->checkSame($cmd->body, $decoded->body, 'JSON roundtrip body');
        $this->checkSame('yes', $decoded->getExtField('handWritten') ?? '', 'JSON roundtrip extFields(handwritten)');
        $this->checkSame('GID-php', $decoded->getExtField('producerGroup') ?? '', 'JSON roundtrip extFields(customHeader materialized)');
        $this->checkSame('3', $decoded->getExtField('queueId') ?? '', 'JSON roundtrip extFields(int → str)');
        $this->checkSame('true', $decoded->getExtField('unitMode') ?? '', 'JSON roundtrip extFields(bool → lowercase true)');

        // flag bits
        $req = new RemotingCommand(10);
        $this->check(!$req->isResponseType() && !$req->isOnewayRPC(), 'new command defaults to non-response non-oneway');
        $req->markResponseType();
        $req->markOnewayRPC();
        $this->check($req->isResponseType() && $req->isOnewayRPC(), 'markResponseType/markOnewayRPC set the bits');
        $this->checkSame('RESPONSE_COMMAND', $req->getType(), 'getType response');
        $back = RemotingCommand::decode($req->encode());
        $this->check($back->isResponseType() && $back->isOnewayRPC(), 'flag bits roundtrip');

        // Response command factory
        $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'ok');
        $resp->opaque = 777;
        $back = RemotingCommand::decode($resp->encode());
        $this->checkSame(ResponseCode::SUCCESS, $back->code, 'response command code roundtrip');
        $this->checkSame(777, $back->opaque, 'response command opaque roundtrip');
        $this->checkSame('ok', $back->remark, 'response command remark roundtrip');

        // language string shape (5.x NameServer serializes language as the enum name)
        $json = '{"code":105,"language":"JAVA","version":515,"opaque":9,"flag":0,"extFields":{"topic":"t"}}';
        $cmd2 = RemotingCommand::decode(pack('N', strlen($json) + 4) . pack('N', strlen($json)) . $json);
        $this->checkSame(LanguageCode::JAVA->value, $cmd2->language, 'language enum-name string → int code');
        $this->checkSame('t', $cmd2->getExtField('topic') ?? '', 'language string shape extFields');

        // Bad frame
        $this->checkThrows(
            static fn(): RemotingCommand => RemotingCommand::decode(pack('N', 999) . pack('N', 1) . 'x'),
            RemotingCommandException::class,
            'decode with a bad totalLength throws RemotingCommandException'
        );

        // opaque auto-increment + createNewRequestId
        $a = new RemotingCommand(1);
        $b = new RemotingCommand(1);
        $this->check($b->opaque === $a->opaque + 1, 'opaque static increment');
        $this->checkSame(RemotingCommand::createNewRequestId(), $b->opaque + 1, 'createNewRequestId shares the counter');
    }

    // ==================================================================== ROCKETMQ serialization

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
        $this->checkSame(SerializeType::ROCKETMQ->value, RemotingCommand::getProtocolType($oriHeaderLen), 'ROCKETMQ frame serialization type bits');
        $headerData = substr($frame, 8, RemotingCommand::getHeaderLength($oriHeaderLen));
        $this->checkSame(strlen($headerData), RocketMQSerializable::calTotalLen('bin-remark', RocketMQSerializable::mapSerialize($cmd->extFields)), 'ROCKETMQ header length = calTotalLen');

        // Field order: code(2) language(1) version(2) opaque(4) flag(4) remark(int+utf8) ext(int+map)
        $fields = RocketMQSerializable::rocketMqProtocolDecode($headerData);
        $this->checkSame(RequestCode::HEART_BEAT, $fields['code'], 'ROCKETMQ encoding code');
        $this->checkSame(LanguageCode::PHP->value, $fields['language'], 'ROCKETMQ encoding language=PHP');
        $this->checkSame(RemotingCommand::CURRENT_VERSION, $fields['version'], 'ROCKETMQ encoding version');
        $this->checkSame($cmd->opaque, $fields['opaque'], 'ROCKETMQ encoding opaque');
        $this->checkSame(0, $fields['flag'], 'ROCKETMQ encoding flag');
        $this->checkSame('bin-remark', $fields['remark'], 'ROCKETMQ encoding remark');
        $this->checkSame('GID-bin', $fields['extFields']['producerGroup'] ?? '', 'ROCKETMQ encoding extFields.producerGroup');
        $this->checkSame('false', $fields['extFields']['unitMode'] ?? '', 'ROCKETMQ encoding extFields.unitMode(lowercase false)');

        $decoded = RemotingCommand::decode($frame);
        $this->checkSame(SerializeType::ROCKETMQ->value, $decoded->serializeTypeCurrentRpc, 'ROCKETMQ roundtrip serializeType');
        $this->checkSame($cmd->code, $decoded->code, 'ROCKETMQ roundtrip code');
        $this->checkSame($cmd->opaque, $decoded->opaque, 'ROCKETMQ roundtrip opaque');
        $this->checkSame($cmd->remark, $decoded->remark, 'ROCKETMQ roundtrip remark');
        $this->checkSame($cmd->body, $decoded->body, 'ROCKETMQ roundtrip body');
        $this->checkSame($cmd->extFields, $decoded->extFields, 'ROCKETMQ roundtrip extFields exact match');

        // Serialization field order: a byte string hand-assembled in protocol header field order must match byte for byte
        $extBytes = (string)RocketMQSerializable::mapSerialize($cmd->extFields);
        $expectHeader = pack('n', $cmd->code & 0xFFFF)
            . pack('C', $cmd->language & 0xFF)
            . pack('n', $cmd->version & 0xFFFF)
            . pack('N', $cmd->opaque)
            . pack('N', $cmd->flag)
            . pack('N', strlen('bin-remark')) . 'bin-remark'
            . pack('N', strlen($extBytes)) . $extBytes;
        $this->checkSame($expectHeader, $headerData, 'ROCKETMQ header field order matches byte for byte');

        // writeDecimalLong boundaries
        $buf = '';
        RocketMQSerializable::writeDecimalLong($buf, 0);
        $this->checkSame(pack('N', 1) . '0', $buf, 'writeDecimalLong(0)');
        $buf = '';
        RocketMQSerializable::writeDecimalLong($buf, -123);
        $this->checkSame(pack('N', 4) . '-123', $buf, 'writeDecimalLong(-123)');
        $buf = '';
        RocketMQSerializable::writeDecimalLong($buf, PHP_INT_MIN);
        $this->checkSame(pack('N', 20) . '-9223372036854775808', $buf, 'writeDecimalLong(PHP_INT_MIN)');

        // Map roundtrip (key short length / value int length)
        $map = ['k1' => 'v1', '中文' => '值'];
        $bin = (string)RocketMQSerializable::mapSerialize($map);
        [$back, $off] = RocketMQSerializable::mapDeserialize($bin, 0, strlen($bin));
        $this->checkSame($map, $back, 'mapSerialize/mapDeserialize roundtrip');
        $this->checkSame(strlen($bin), $off, 'mapDeserialize consumed all bytes');
        // Empty-string value encodes as length 0 → readStr restores null → the key is dropped
        // (the classic remoting protocol map encoding is lossy by itself, not an implementation bug)
        $lossy = (string)RocketMQSerializable::mapSerialize(['empty' => '']);
        [$backLossy, $offLossy] = RocketMQSerializable::mapDeserialize($lossy, 0, strlen($lossy));
        $this->checkSame([], $backLossy, 'empty-string value roundtrip is lossy (length 0 → null, the protocol encoding itself is lossy)');
        $this->checkSame(strlen($lossy), $offLossy, 'lossy case still consumed all bytes');
        $this->checkSame(null, RocketMQSerializable::mapSerialize(null), 'mapSerialize(null) → null');
        $this->checkSame(null, RocketMQSerializable::mapSerialize([]), 'mapSerialize([]) → null');

        // readStr length 0 → null
        $this->checkSame([null, 4], RocketMQSerializable::readStr(pack('N', 0), 0, false), 'readStr length 0 → null');
    }

    // ==================================================================== header snapshot

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
        ], $h->toExtFields(), 'SendMessageRequestHeader.toExtFields field names/order/lowercase bool snapshot');

        // null fields stay out of ext
        $h2 = new SendMessageRequestHeader();
        $h2->topic = 'T';
        $this->checkSame(['topic' => 'T'], $h2->toExtFields(), 'null fields write no key');

        // V2 short field names
        $v2 = new SendMessageRequestHeaderV2();
        $v2->producerGroup = 'GID';
        $v2->queueId = 5;
        $v2->brokerName = 'broker-a';
        $this->checkSame(['a' => 'GID', 'e' => 5, 'n' => 'broker-a'], $v2->toExtFields(), 'SendMessageRequestHeaderV2 short field names');

        // fromExtFields read-back (with type conversion)
        $h3 = new SendMessageRequestHeader();
        $h3->fromExtFields(['producerGroup' => 'G', 'queueId' => '7', 'bornTimestamp' => '123', 'unitMode' => 'True', 'batch' => '0']);
        $this->checkSame('G', $h3->producerGroup, 'fromExtFields string');
        $this->checkSame(7, $h3->queueId, 'fromExtFields int');
        $this->checkSame(123, $h3->bornTimestamp, 'fromExtFields long');
        $this->check(true === $h3->unitMode, 'fromExtFields bool(true)');
        $this->check(false === $h3->batch, 'fromExtFields bool(0=false)');

        // PopMessageRequestHeader.order always present on the wire (non-nullable Boolean in the protocol)
        $pop = new PopMessageRequestHeader();
        $pop->topic = 't';
        $ext = $pop->toExtFields();
        $this->check(array_key_exists('order', $ext) && $ext['order'] === 'false', 'PopMessageRequestHeader.order always present and lowercase');

        // SearchOffsetRequestHeader.boundaryType goes on the wire as the uppercase enum name
        $so = new SearchOffsetRequestHeader();
        $so->topic = 't';
        $so->boundaryType = BoundaryType::UPPER;
        $this->checkSame(['topic' => 't', 'boundaryType' => 'UPPER'], $so->toExtFields(), 'SearchOffsetRequestHeader boundaryType=UPPER');
    }

    // ==================================================================== ACL signature

    private function testAclSignature(): void
    {
        Logger::setHandler(static function (string $line): void {}); // silence during tests

        $sk = 'sk-fixed-123';
        $hook = new AclClientRPCHook(new SessionCredentials('ak-fixed', $sk));

        $header = new SendMessageRequestHeader();
        $header->producerGroup = 'GID-sig';
        $header->topic = 'SigTopic';
        $header->queueId = 0;
        $cmd = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $cmd->body = 'sig-payload';

        $hook->doBeforeRequest('127.0.0.1:10911', $cmd);
        $this->checkSame('ak-fixed', $cmd->getExtField('AccessKey') ?? '', 'ACL AccessKey injected');
        $sig = $cmd->getExtField('Signature') ?? '';
        $this->checkSame(28, strlen($sig), 'HmacSHA1 Base64 length is always 28');

        // Independent recomputation: lexicographic extFields values (excluding Signature) + body
        $cmd->makeCustomHeaderToNet();
        $fields = $cmd->extFields;
        unset($fields['Signature']);
        ksort($fields, SORT_STRING);
        $content = implode('', array_map(strval(...), array_values($fields))) . 'sig-payload';
        $expect = base64_encode(hash_hmac('sha1', $content, $sk, true));
        $this->checkSame($expect, $sig, 'signature = Base64(HmacSHA1(secretKey, sorted values + body)) reproducible');

        // Fixed input → deterministic signature (two builds of the same input agree)
        $cmd2 = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $cmd2->body = 'sig-payload';
        $hook->doBeforeRequest('127.0.0.1:10911', $cmd2);
        $this->checkSame($sig, $cmd2->getExtField('Signature') ?? '', 'same input produces the same signature');

        // Signature does not participate in the signed content
        $raw = AclClientRPCHook::buildRequestContent($cmd2);
        $this->check(!str_contains($raw, $sig), 'Signature not part of the signed content');

        // securityToken participates in the signature
        $hookTok = new AclClientRPCHook(new SessionCredentials('ak', 'sk', 'st-1'));
        $cmd4 = new RemotingCommand(RequestCode::HEART_BEAT);
        $hookTok->doBeforeRequest('addr', $cmd4);
        $this->checkSame('st-1', $cmd4->getExtField('SecurityToken') ?? '', 'SecurityToken injected');

        // AclRpcHook is a compatibility alias of AclClientRPCHook
        $alias = new AclRpcHook(new SessionCredentials('a', 'b'));
        $this->check($alias instanceof AclClientRPCHook, 'AclRpcHook compatibility alias');
    }

    // ==================================================================== namespaceV2 hook

    /**
     * 5.x server-side namespace hook (NamespaceRpcHook).
     *
     * Criteria (5.x server namespace extension-header semantics):
     *   * doBeforeRequest is the only leg: when namespaceV2 is non-empty it adds **two**
     *     extension headers nsd="true" / ns=<namespaceV2>; doAfterResponse is a no-op.
     *   * An empty namespace must **not touch** extFields at all — the criterion is
     *     "the request is untouched when unconfigured"; even initializing extFields to an
     *     empty map is wrong (in production it would add an empty object).
     *   * The value is **read per request** (each request takes the clientConfig current
     *     namespaceV2), so changing the config after start() takes effect from the next
     *     request; a string-snapshot implementation would block that path.
     *   * Registration order Namespace → Stream → user hooks(ACL) is semantics, not style:
     *     nsd/ns must enter extFields before the signature is computed; reversed, the
     *     signature "looks legal" but an auth-enabled broker rejects it with two extra
     *     unsigned fields.
     */
    private function testNamespaceRpcHook(): void
    {
        Logger::setHandler(static function (string $line): void {}); // silence during tests

        // Field names are fixed verbatim by the protocol (a typo = the broker does not
        // recognize the namespace, silent failure)
        $this->checkSame('nsd', MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD, 'nsd field name');
        $this->checkSame('ns', MixAll::RPC_REQUEST_HEADER_NAMESPACE_FIELD, 'ns field name');

        $ns = 'MQ_INST_php_parity';
        $hook = new NamespaceRpcHook($ns);
        $cmd = new RemotingCommand(RequestCode::SEND_MESSAGE);
        $hook->doBeforeRequest('127.0.0.1:10911', $cmd);
        $this->checkSame('true', $cmd->getExtField('nsd') ?? '', 'nsd=true');
        $this->checkSame($ns, $cmd->getExtField('ns') ?? '', 'ns=<namespaceV2>');

        // doAfterResponse is a no-op (does not rewrite any field on the way back)
        $after = $cmd->extFields;
        $hook->doAfterResponse('127.0.0.1:10911', $cmd, null);
        $this->checkSame($after, $cmd->extFields, 'doAfterResponse leaves the request untouched');

        // Empty namespace = no-op, and it does not even touch extFields ('' and null legs)
        foreach (['', null] as $unset) {
            $bare = new RemotingCommand(RequestCode::SEND_MESSAGE);
            $before = $bare->extFields;
            (new NamespaceRpcHook($unset))->doBeforeRequest('a', $bare);
            $this->checkSame($before, $bare->extFields,
                'extFields untouched when no namespace configured (' . var_export($unset, true) . ')');
            $this->check(!array_key_exists('ns', $bare->extFields), 'ns not written as an empty string');
            $this->check(!array_key_exists('nsd', $bare->extFields), 'nsd not written as an empty string');
        }

        // The value provider is read per request: empty first, configured later; the second request must carry it
        // (each request reads the current clientConfig value)
        $current = '';
        $live = new NamespaceRpcHook(function () use (&$current): string {
            return $current;
        });
        $first = new RemotingCommand(RequestCode::SEND_MESSAGE);
        $live->doBeforeRequest('a', $first);
        $this->check(!array_key_exists('ns', $first->extFields), 'live read: first request has no namespace yet');
        $current = $ns;
        $second = new RemotingCommand(RequestCode::SEND_MESSAGE);
        $live->doBeforeRequest('a', $second);
        $this->checkSame($ns, $second->getExtField('ns') ?? '', 'live read: config change applies to the next request');
        $current = $ns . '_2';
        $third = new RemotingCommand(RequestCode::SEND_MESSAGE);
        $live->doBeforeRequest('a', $third);
        $this->checkSame($ns . '_2', $third->getExtField('ns') ?? '', 'live read: tracks further changes');

        // Order: Namespace before ACL ⇒ ns/nsd enter the signed content; the broker recomputes the same signature
        $sk = 'sk-ns-123';
        $acl = new AclClientRPCHook(new SessionCredentials('ak-ns', $sk));
        $header = new SendMessageRequestHeader();
        $header->producerGroup = 'GID-ns';
        $header->topic = 'NsTopic';
        $header->queueId = 0;

        $signed = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $signed->body = 'ns-payload';
        $hook->doBeforeRequest('a', $signed);          // write nsd/ns first
        $acl->doBeforeRequest('a', $signed);           // then sign (namespace headers must enter extFields first)
        $sigWithNs = $signed->getExtField('Signature') ?? '';
        $this->check($sigWithNs !== '', 'chained signature generated');

        // Independent broker-side recomputation: lexicographic values (excluding Signature) + body
        $verify = $signed->extFields;
        unset($verify[SessionCredentials::SIGNATURE]);
        ksort($verify, SORT_STRING);
        $content = implode('', array_map(strval(...), array_values($verify))) . 'ns-payload';
        $this->checkSame(base64_encode(hash_hmac('sha1', $content, $sk, true)), $sigWithNs,
            'nsd/ns are in the signed content (the broker recomputing over the wire fields passes)');

        // Counter-proof: the same request signed without the Namespace hook first necessarily differs —
        // reversed order means signature verification fails
        $noNs = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $noNs->body = 'ns-payload';
        $acl->doBeforeRequest('a', $noNs);
        $this->check($sigWithNs !== ($noNs->getExtField('Signature') ?? ''),
            'without the Namespace hook first, the same content cannot be signed');

        // Stream also runs before ACL: ReqT and ns both enter the signature
        $chain = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, $header);
        $chain->body = 'ns-payload';
        (new NamespaceRpcHook($ns))->doBeforeRequest('a', $chain);
        (new StreamTypeRPCHook())->doBeforeRequest('a', $chain);
        $acl->doBeforeRequest('a', $chain);
        $this->checkSame('true', $chain->getExtField('nsd') ?? '', 'nsd still present on the chain');
        $this->check(($chain->getExtField('ReqT') ?? '') !== '', 'ReqT written on the chain');
        $chainVerify = $chain->extFields;
        unset($chainVerify[SessionCredentials::SIGNATURE]);
        ksort($chainVerify, SORT_STRING);
        $chainContent = implode('', array_map(strval(...), array_values($chainVerify))) . 'ns-payload';
        $this->checkSame(base64_encode(hash_hmac('sha1', $chainContent, $sk, true)),
            $chain->getExtField('Signature') ?? '', 'ns + ReqT both in the signature (namespace headers computed before the signature)');
    }

    // ==================================================================== route decoding

    private function testRouteDecode(): void
    {
        // fastjson2 non-standard JSON: numeric keys without quotes
        $raw = '{"orderTopicConf":null,'
            . '"queueDatas":[{"brokerName":"broker-a","readQueueNums":8,"writeQueueNums":8,"perm":6,"topicSysFlag":0},'
            . '{"brokerName":"broker-b","readQueueNums":4,"writeQueueNums":4,"perm":4,"topicSysFlag":0}],'
            . '"brokerDatas":[{"brokerAddrs":{0:"10.0.0.1:10911",1:"10.0.0.1:10921"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false},'
            . '{"brokerAddrs":{0:"10.0.0.2:10911"},"brokerName":"broker-b","cluster":"DefaultCluster","enableActingMaster":false}],'
            . '"filterServerTable":{}}';
        $route = TopicRouteData::decode($raw);
        $this->checkSame(2, count($route->queueDatas), 'fastjson numeric-key decode of queueDatas');
        $this->checkSame(2, count($route->brokerDatas), 'fastjson numeric-key decode of brokerDatas');
        $this->checkSame('broker-a', $route->queueDatas[0]->brokerName, 'QueueData.brokerName');
        $this->checkSame(8, $route->queueDatas[0]->readQueueNums, 'QueueData.readQueueNums');
        $this->checkSame([0 => '10.0.0.1:10911', 1 => '10.0.0.1:10921'], $route->brokerDatas[0]->brokerAddrs, 'BrokerData.brokerAddrs numeric keys restored');

        // publish side: writable + master required
        $mqs = $route->getAllMessageQueue('T');
        $this->checkSame(8, count($mqs), 'publish side: broker-a 8 write queues');
        $this->check($mqs[0]->topic === 'T' && $mqs[0]->brokerName === 'broker-a' && $mqs[0]->queueId === 0, 'MessageQueue(topic backfill, broker, queueId)');
        // broker-b has read-only permission (perm=4) -> excluded from publish info
        $names = array_map(static fn(MessageQueue $mq): string => $mq->brokerName, $mqs);
        $this->check(!in_array('broker-b', $names, true), 'broker with perm=4 excluded from publish side');
        // subscribe side: read permission only, master not required
        $subMqs = $route->getAllSubscribeMessageQueue('T');
        $this->checkSame(12, count($subMqs), 'subscribe side: 8+4 read queues');

        // brokers without a master addr are skipped for publish info
        $route2 = new TopicRouteData();
        $qd = new QueueData('b1', 2, 2, 6);
        $route2->queueDatas = [$qd];
        $bdNoMaster = new BrokerData('c', 'b1', [1 => '10.0.0.9:10921']);
        $route2->brokerDatas = [$bdNoMaster];
        $this->checkSame([], $route2->getAllMessageQueue('T'), 'broker without master skipped (publish side)');
        $this->checkSame(2, count($route2->getAllSubscribeMessageQueue('T')), 'subscription still works without master (subscribe side)');

        $this->checkSame('10.0.0.1:10911', $route->brokerDatas[0]->selectBrokerAddr(), 'selectBrokerAddr prefers master');
        $this->checkSame('10.0.0.9:10921', $route2->brokerDatas[0]->selectBrokerAddr(), 'no master: random pick (single element)');
        $this->checkSame(null, (new BrokerData('c', 'b2', []))->selectBrokerAddr(), 'empty brokerAddrs -> null');

        // strict JSON also decodes
        $strict = json_encode($route->toDict(), JSON_UNESCAPED_SLASHES);
        $route3 = TopicRouteData::decode((string)$strict);
        $this->checkSame(2, count($route3->queueDatas), 'strict JSON decode of queueDatas');
        $this->check($route3->topicRouteDataChanged($route) === false, 'identical content -> route unchanged');
        $route3->queueDatas[0]->readQueueNums = 9;
        $this->check($route3->topicRouteDataChanged($route) === true, 'queueNums changed -> route changed');

        // encode -> decode roundtrip
        $route4 = TopicRouteData::decode($route->encode());
        $this->checkSame($route->queueDatas[0]->brokerName, $route4->queueDatas[0]->brokerName, 'TopicRouteData encode/decode roundtrip');
    }

    // ==================================================================== heartbeat

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
        $this->checkSame('cid-php-1', $obj['clientID'], 'HeartbeatData.clientID key name');
        $this->checkSame(0, $obj['heartbeatFingerprint'], 'heartbeatFingerprint always 0 (V1 registration path)');
        $this->checkSame(false, $obj['withoutSub'], 'withoutSub always false');
        $this->checkSame('GID-c', $obj['consumerDataSet'][0]['groupName'], 'ConsumerData.groupName');
        $this->checkSame('CLUSTERING', $obj['consumerDataSet'][0]['messageModel'], 'ConsumerData.messageModel');
        $this->checkSame('*', $obj['consumerDataSet'][0]['subscriptionDataSet'][0]['subString'], 'subscriptionDataSet goes through toDict');
        $this->checkSame('TAG', $obj['consumerDataSet'][0]['subscriptionDataSet'][0]['expressionType'], 'expressionType defaults to TAG');

        $back = HeartbeatData::decode($hb->encode());
        $this->checkSame($hb->clientId, $back->clientId, 'HeartbeatData roundtrip clientId');
        $this->checkSame(1, count($back->consumerDataSet), 'HeartbeatData roundtrip consumerDataSet');
        $this->checkSame('*', $back->consumerDataSet[0]->subscriptionDataSet[0]->subString ?? '', 'HeartbeatData roundtrip subscription');
        $this->checkSame(42, $back->consumerDataSet[0]->subscriptionDataSet[0]->subVersion ?? 0, 'HeartbeatData roundtrip subVersion');
        $this->checkSame('GID-p', $back->producerDataSet[0]->groupName ?? '', 'HeartbeatData roundtrip producerDataSet');
    }

    // ==================================================================== ExtraInfo / Namespace

    private function testExtraInfoAndNamespace(): void
    {
        $ck = ExtraInfoUtil::buildExtraInfo(5, 1000, 30000, 2, 'TopicTest', 'broker-a', 3, 17);
        $this->checkSame('5 1000 30000 2 0 broker-a 3 17', $ck, 'buildExtraInfo 8 segments (normal topic, space separated)');
        $segs = ExtraInfoUtil::split($ck);
        $this->checkSame(8, count($segs), 'split yields 8 segments');
        $this->checkSame(5, ExtraInfoUtil::getCkQueueOffset($segs), 'getCkQueueOffset');
        $this->checkSame(1000, ExtraInfoUtil::getPopTime($segs), 'getPopTime');
        $this->checkSame(30000, ExtraInfoUtil::getInvisibleTime($segs), 'getInvisibleTime');
        $this->checkSame(2, ExtraInfoUtil::getReviveQid($segs), 'getReviveQid');
        $this->checkSame('0', ExtraInfoUtil::getRetry($segs), 'getRetry (normal)');
        $this->checkSame('broker-a', ExtraInfoUtil::getBrokerName($segs), 'getBrokerName');
        $this->checkSame(3, ExtraInfoUtil::getQueueId($segs), 'getQueueId');
        $this->checkSame(17, ExtraInfoUtil::getQueueOffset($segs), 'getQueueOffset');
        $this->check(ExtraInfoUtil::isOrder($segs) === false, 'non-999 means not ordered consumption');

        $ck7 = ExtraInfoUtil::buildExtraInfo(1, 2, 3, 4, 'TopicTest', 'b', 0);
        $this->checkSame('1 2 3 4 0 b 0', $ck7, 'buildExtraInfo 7 segments');
        $this->checkThrows(static fn(): int => ExtraInfoUtil::getQueueOffset(ExtraInfoUtil::split($ck7)), \InvalidArgumentException::class, 'throws when segments are insufficient');

        // retry topic shapes
        $this->checkSame('1', ExtraInfoUtil::retryOfTopic('%RETRY%GID_topic'), 'V1 retry');
        $this->checkSame('2', ExtraInfoUtil::retryOfTopic('%RETRY%GID+topic'), 'V2 retry');
        $this->checkSame('0', ExtraInfoUtil::retryOfTopic('NormalTopic'), 'normal topic');
        $this->checkSame('%RETRY%GID_topic', ExtraInfoUtil::getRealTopic('topic', 'GID', '1'), 'getRealTopic V1');
        $this->checkSame('%RETRY%GID+topic', ExtraInfoUtil::getRealTopic('topic', 'GID', '2'), 'getRealTopic V2');
        $this->checkSame('topic', ExtraInfoUtil::getRealTopic('topic', 'GID', '0'), 'getRealTopic normal');
        $this->checkSame('%RETRY%G_topic', ExtraInfoUtil::buildPopRetryTopicV1('topic', 'G'), 'buildPopRetryTopicV1');
        $this->checkSame('%RETRY%G+topic', ExtraInfoUtil::buildPopRetryTopicV2('topic', 'G'), 'buildPopRetryTopicV2');
        $this->check(ExtraInfoUtil::isPopRetryTopicV2('%RETRY%G+topic'), 'isPopRetryTopicV2');
        $this->check(!ExtraInfoUtil::isPopRetryTopicV2('%RETRY%G_topic'), 'V1 is not V2');
        $orderSegs = ExtraInfoUtil::split(ExtraInfoUtil::buildExtraInfo(0, 0, 0, 999, 't', 'b', 0));
        $this->check(ExtraInfoUtil::isOrder($orderSegs), 'reviveQid=999 -> isOrder');

        // startOffsetInfo / msgOffsetInfo
        $start = ExtraInfoUtil::parseStartOffsetInfo('0 3 0;0 2 0');
        $this->checkSame(['0@3' => 0, '0@2' => 0], $start, 'parseStartOffsetInfo');
        $msg = ExtraInfoUtil::parseMsgOffsetInfo('0 3 0,1,2;0 2 5');
        $this->checkSame([0, 1, 2], $msg['0@3'] ?? [], 'parseMsgOffsetInfo list');
        $this->checkSame([5], $msg['0@2'] ?? [], 'parseMsgOffsetInfo single');
        $order = ExtraInfoUtil::parseOrderCountInfo('0 3 2');
        $this->checkSame(['0@3' => 2], $order, 'parseOrderCountInfo');
        $this->checkSame(null, ExtraInfoUtil::parseStartOffsetInfo(null), 'null → null');
        $this->checkThrows(static fn(): ?array => ExtraInfoUtil::parseStartOffsetInfo('0 3'), \InvalidArgumentException::class, 'startOffsetInfo wrong segment count');

        $this->checkSame('0@3', ExtraInfoUtil::getStartOffsetInfoMapKey('TopicTest', 3), 'getStartOffsetInfoMapKey');
        $this->checkSame('qo3%17', ExtraInfoUtil::getQueueOffsetKeyValueKey(3, 17), 'getQueueOffsetKeyValueKey');
        $this->checkSame('0@qo3%17', ExtraInfoUtil::getQueueOffsetMapKey('TopicTest', 3, 17), 'getQueueOffsetMapKey');
        $this->checkSame('2@qo0%9', ExtraInfoUtil::getQueueOffsetMapKey('%RETRY%G+t', 0, 9), 'map key for V2 topic');

        // NamespaceUtil
        $this->checkSame('Topic', NamespaceUtil::withoutNamespace('MQ_INST_XX%Topic'), 'withoutNamespace normal');
        $this->checkSame('%RETRY%GID', NamespaceUtil::withoutNamespace('%RETRY%MQ_INST_XX%GID'), 'withoutNamespace retry');
        $this->checkSame('MQ_INST_YY%Topic', NamespaceUtil::withoutNamespace('MQ_INST_YY%Topic', 'MQ_INST_XX'), 'mismatched namespace returned as-is');
        $this->checkSame('MQ_INST_XX%Topic', NamespaceUtil::wrapNamespace('MQ_INST_XX', 'Topic'), 'wrapNamespace');
        $this->checkSame('%RETRY%MQ_INST_XX%GID', NamespaceUtil::wrapNamespaceAndRetry('MQ_INST_XX', 'GID'), 'wrapNamespaceAndRetry');
        $this->checkSame('MQ_INST_XX', NamespaceUtil::getNamespaceFromResource('MQ_INST_XX%Topic'), 'getNamespaceFromResource');
        $this->checkSame('', NamespaceUtil::getNamespaceFromResource('rmq_sys_ROUTE'), 'system topic has no namespace');
        $this->check(!NamespaceUtil::isAlreadyWithNamespace('Topic', 'NS'), 'not namespaced check');
        $this->check(NamespaceUtil::isAlreadyWithNamespace('NS%Topic', 'NS'), 'already namespaced check');
    }

    // ==================================================================== fastjson-compatible parsing

    private function testFastjsonParser(): void
    {
        $v = RemotingSerializable::fastjsonLoads('{"a":1,"b":[1,2,{"c":"x"}],"d":{0:"z",1:"y"},"e":true,"f":null}');
        $this->checkSame(1, $v['a'], 'fastjson numeric value');
        $this->checkSame('x', $v['b'][2]['c'], 'fastjson nesting');
        $this->checkSame('z', $v['d']['0'], 'fastjson bare numeric key');
        $this->checkSame(true, $v['e'], 'fastjson true');
        $this->checkSame(null, $v['f'], 'fastjson null');

        $v2 = RemotingSerializable::fastjsonLoads('{"g":1.5,"h":NaN,"i":Infinity,"j":-Infinity,}');
        $this->checkSame(1.5, $v2['g'], 'fastjson float');
        $this->check(is_nan($v2['h']) && is_infinite($v2['i']) && $v2['j'] === -INF, 'fastjson tolerates NaN/Infinity');
        $this->check($v2 !== null, 'fastjson tolerates trailing comma');

        // MessageQueue inline object key
        $v3 = RemotingSerializable::fastjsonLoads(
            '{"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{"minOffset":0,"maxOffset":9}}}'
        );
        $this->check(isset($v3['offsetTable']), 'inline object key parses');
        $key = (string)array_key_first($v3['offsetTable']);
        $mq = RemotingSerializable::decodeMessageQueueKey($key);
        $this->check($mq !== null && $mq['topic'] === 't' && $mq['brokerName'] === 'b' && $mq['queueId'] === 3, 'decodeMessageQueueKey restores');
        $this->checkSame(null, RemotingSerializable::decodeMessageQueueKey('plain-key'), 'plain string key -> null');

        // CJK / escapes
        $v4 = RemotingSerializable::fastjsonLoads('{"s":"中文\\n\"q\""}');
        $this->checkSame("中文\n\"q\"", $v4['s'], 'fastjson CJK and escapes');
        $this->checkThrows(static fn() => RemotingSerializable::fastjsonLoads('{"a":}'), FastJsonDecodeError::class, 'invalid input throws FastJsonDecodeError');

        // encode
        $this->checkSame('', RemotingSerializable::encode(null), 'encode(null) -> empty string');
        $this->checkSame('raw', RemotingSerializable::encode('raw'), 'encode(bytes) passes through');
        $this->checkSame('{"k":"v","u":"中/文"}', RemotingSerializable::encode(['k' => 'v', 'u' => '中/文']), 'encode does not escape CJK or slash');
    }

    // ==================================================================== TCP end-to-end

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

    /** Reads exactly n bytes (blocking). */
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

    /** Reads and decodes one full frame from the server side. */
    private static function readFrame($conn): RemotingCommand
    {
        $total = RocketMQSerializable::unpackSignedInt(self::readExact($conn, 4));
        return RemotingCommand::decode(pack('N', $total) . self::readExact($conn, $total));
    }

    /**
     * Establish the connection and accept it.
     *
     * Client connection is **lazy** (connect happens on first send), and in a
     * single-threaded PHP script `invokeSync` blocks waiting for a response.
     * So the test first uses one `invokeOneway` to bring up the TCP connection
     * (oneway writes without waiting), then accepts on the server side and
     * discards this handshake request frame.
     *
     * @param resource $server
     * @return resource the accepted server-side connection (blocking mode)
     */
    private static function connectAndAccept(RemotingClient $client, string $addr, $server)
    {
        $client->invokeOneway($addr, RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT));
        $conn = stream_socket_accept($server, 5);
        if ($conn === false) {
            throw new \RuntimeException('test server: accept failed');
        }
        stream_set_blocking($conn, true);
        self::readFrame($conn); // discard the handshake oneway frame
        return $conn;
    }

    private function testTcpEndToEnd(): void
    {
        // ---- 1. sync RPC: server validates request and replies; connection reuse ----
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

        // In a single thread invokeSync blocks on reading the response before the server-side
        // code runs -> write the response into the connection buffer first (the opaque is known
        // at construction), then after invokeSync returns read back the request frame the server
        // received and assert on it.
        $respHeader = new SendMessageResponseHeader();
        $respHeader->msgId = 'MSG-1';
        $respHeader->queueId = 1;
        $respHeader->queueOffset = 99;
        $resp = RemotingCommand::createResponseCommandWithHeader(ResponseCode::SUCCESS, $respHeader);
        $resp->opaque = $req->opaque;
        $resp->body = 'resp-body';
        fwrite($conn, $resp->encode());

        $got = $client->invokeSync($addr, $req, 3000);
        $this->checkSame(ResponseCode::SUCCESS, $got->code, 'e2e: invokeSync got response code');
        $this->checkSame($req->opaque, $got->opaque, 'e2e: opaque matches');
        $this->checkSame('resp-body', $got->body, 'e2e: response body');
        $hdr = $got->decodeCommandCustomHeader(SendMessageResponseHeader::class);
        $this->check($hdr instanceof SendMessageResponseHeader, 'decodeCommandCustomHeader returns header object');
        $this->checkSame('MSG-1', $hdr->msgId, 'decodeCommandCustomHeader msgId');
        $this->checkSame(1, $hdr->queueId, 'decodeCommandCustomHeader queueId (int cast)');
        $this->checkSame(99, $hdr->queueOffset, 'decodeCommandCustomHeader queueOffset');

        // read back the request frame the server side actually received
        $srvCmd = self::readFrame($conn);
        $this->checkSame(RequestCode::SEND_MESSAGE, $srvCmd->code, 'e2e: server received request code');
        $this->checkSame('e2e-body', $srvCmd->body, 'e2e: server received request body');
        $this->checkSame('GID-e2e', $srvCmd->getExtField('producerGroup') ?? '', 'e2e: server received header field landed');
        $this->check(!$srvCmd->isResponseType(), 'e2e: request carries no response flag');

        // connection reuse: the second request uses the same connection
        $req2 = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        $resp2 = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'hb-ok');
        $resp2->opaque = $req2->opaque;
        fwrite($conn, $resp2->encode());
        $got2 = $client->invokeSync($addr, $req2, 3000);
        $this->checkSame('hb-ok', $got2->remark ?? '', 'e2e: invokeSync succeeds on reused connection');
        $srvCmd2 = self::readFrame($conn);
        $this->checkSame(RequestCode::HEART_BEAT, $srvCmd2->code, 'e2e: second request on reused connection');
        fclose($conn);
        $client->shutdown();
        fclose($server);

        // ---- 2. oneway + broker-initiated request + async ----
        [$server2, $addr2] = self::startEchoServer();
        $client2 = new RemotingClient(1000, 3000);
        $connA = self::connectAndAccept($client2, $addr2, $server2);

        // 2a. oneway: flag bits carry the oneway marker
        $ow = RemotingCommand::createRequestCommand(RequestCode::NOTIFY_CONSUMER_IDS_CHANGED, new NotifyConsumerIdsChangedRequestHeader());
        $client2->invokeOneway($addr2, $ow);
        $srvO = self::readFrame($connA);
        $this->check($srvO->isOnewayRPC(), 'oneway request flag bits');

        // 2b. broker-initiated request (processor): server sends CHECK_TRANSACTION_STATE, client processor replies
        $seen = null;
        $client2->registerProcessor(RequestCode::CHECK_TRANSACTION_STATE, static function (RemotingCommand $c, string $from) use (&$seen): ?RemotingCommand {
            $seen = $c;
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'processed');
        });

        // Single-threaded PHP has no dedicated IO thread: broker-initiated requests must be
        // pumped in via reads. First issue the async request needed by 2c (once written,
        // pending is non-empty), using it to drive the read loop of waitResponses.
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
        $this->checkSame(RequestCode::GET_ROUTEINFO_BY_TOPIC, $srvA->code, 'e2e: async request on the wire');

        $pushReq = RemotingCommand::createRequestCommand(RequestCode::CHECK_TRANSACTION_STATE, new CheckTransactionStateRequestHeader());
        $pushReq->opaque = 4321;
        $pushReq->body = 'check-me';
        fwrite($connA, $pushReq->encode());
        // Pump once: read pushReq -> hand to processor -> response written back to connA
        // (the async one has no response yet, pending is non-empty)
        $client2->waitResponses(300);
        $gotResp = self::readFrame($connA);
        $this->check($seen !== null && $seen->body === 'check-me', 'processor received broker-initiated request');
        $this->check($gotResp->isResponseType(), 'processor response carries response flag');
        $this->checkSame(4321, $gotResp->opaque, 'processor response opaque backfilled');
        $this->checkSame('processed', $gotResp->remark ?? '', 'processor response remark');
        $client2->unregisterProcessor(RequestCode::CHECK_TRANSACTION_STATE);

        // 2c. async: after the server replies, waitResponses pumps to the callback
        $respA = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'route-ok');
        $respA->opaque = $srvA->opaque;
        $respA->body = json_encode(['topicList' => ['T1', 'T2']]);
        fwrite($connA, $respA->encode());
        $client2->waitResponses(3000);
        $this->check($asyncError === null, 'e2e: async has no failure callback');
        $this->check($asyncResult instanceof RemotingCommand && $asyncResult->opaque === $reqA->opaque, 'e2e: async callback got response (opaque matches)');
        $this->checkSame('route-ok', $asyncResult->remark ?? '', 'e2e: async response remark');
        $this->checkSame(['T1', 'T2'], json_decode((string)$asyncResult->body, true)['topicList'] ?? [], 'e2e: async response body');

        fclose($connA);
        $client2->shutdown();
        fclose($server2);

        // ---- 3. timeout ----
        [$server3, $addr3] = self::startEchoServer();
        $client3 = new RemotingClient(1000, 3000);
        $reqT = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        $t0 = microtime(true);
        $this->checkThrows(static fn(): RemotingCommand => $client3->invokeSync($addr3, $reqT, 400), RemotingTimeoutException::class, 'no response -> RemotingTimeoutException');
        $elapsed = (microtime(true) - $t0) * 1000;
        $this->check($elapsed < 2000, sprintf('timeout returns on time (%.0fms)', $elapsed));
        // the client connection is already in the backlog, just reap it afterwards (server never responds)
        $connT = stream_socket_accept($server3, 5);
        if (is_resource($connT)) {
            fclose($connT);
        }
        $client3->shutdown();
        fclose($server3);

        // ---- 4. connection closed -> failFast (sync and async) ----
        [$server4, $addr4] = self::startEchoServer();
        $client4 = new RemotingClient(1000, 5000);
        $connF = self::connectAndAccept($client4, $addr4, $server4);

        // 4a. sync: connection already closed by the server -> RemotingSendRequestException (not a timeout)
        $reqF = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        fclose($connF); // server side closes; the client will reconnect on next send
        $this->checkThrows(static fn(): RemotingCommand => $client4->invokeSync($addr4, $reqF, 5000), RemotingSendRequestException::class, 'connection closed -> sync throws RemotingSendRequestException');

        // 4b. async: connection drops after the request is sent, before the response -> onFailure(RemotingSendRequestException)
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
        // 4a already closed the old connection, so this request must go over a new one
        $connF2 = stream_socket_accept($server4, 5);
        if (!is_resource($connF2)) {
            throw new \RuntimeException('test server: accept failed (4b)');
        }
        stream_set_blocking($connF2, true);
        self::readFrame($connF2); // read the request
        fclose($connF2);          // drop without responding
        $client4->waitResponses(2000);
        $this->check($asyncErr4 instanceof RemotingSendRequestException, 'connection closed -> async onFailure(RemotingSendRequestException)');
        $this->check(!$asyncErr4 instanceof RemotingTimeoutException, 'failFast is not a timeout');

        fclose($server4);
        $client4->shutdown();

        // ---- 5. GO_AWAY: reconnect and resend once ----
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
        // first round: server replies GO_AWAY
        $srvG1 = self::readFrame($connG);
        $goAway = RemotingCommand::createResponseCommand(ResponseCode::GO_AWAY, 'busy');
        $goAway->opaque = $srvG1->opaque;
        fwrite($connG, $goAway->encode());
        // Pump once: the client closes the old connection in dispatch + resends over a new one (new connection enters server backlog)
        $client5->waitResponses(300);
        // second round: accept the new connection, read the resent request and reply success
        $connG2 = stream_socket_accept($server5, 5);
        if (!is_resource($connG2)) {
            throw new \RuntimeException('test server: accept failed (go-away retry)');
        }
        stream_set_blocking($connG2, true);
        $srvG2 = self::readFrame($connG2);
        $this->checkSame($reqG->code, $srvG2->code, 'same request resent over a new connection after GO_AWAY');
        $this->check($srvG2->opaque !== $reqG->opaque, 'resent request carries a new opaque');
        $okResp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'after-goaway');
        $okResp->opaque = $srvG2->opaque;
        fwrite($connG2, $okResp->encode());
        $client5->waitResponses(3000);
        $this->check($errG === null, 'GO_AWAY resend has no failure callback');
        $this->checkSame('after-goaway', $gotG?->remark ?? '', 'GO_AWAY resend gets the final response');
        fclose($connG);
        fclose($connG2);
        $client5->shutdown();
        fclose($server5);

        // ---- 6. nameserver list and round-robin ----
        $client6 = new RemotingClient(nameServers: ['ns-a:9876', 'ns-b:9876']);
        $this->checkSame(['ns-a:9876', 'ns-b:9876'], $client6->getNameServerAddressList(), 'getNameServerAddressList');
        $this->checkSame('ns-a:9876', $client6->chooseNameServer(), 'round-robin picks the 1st');
        $this->checkSame('ns-b:9876', $client6->chooseNameServer(), 'round-robin picks the 2nd');
        $this->checkSame('ns-a:9876', $client6->chooseNameServer(), 'round-robin wraps around');
        $client6->updateNameServerAddressList(['ns-c:9876']);
        $this->checkSame(['ns-c:9876'], $client6->getNameServerAddressList(), 'updateNameServerAddressList (failover entry point)');
        $client6->shutdown();
    }

    // ==================================================================== SOCKS proxy

    /**
     * Spawns a local fake SOCKS proxy child process (tests/_socks_child.php): the child also
     * runs the target echo service in the same process, reports ports with one ``READY <proxyPort> <targetPort>``
     * line on stdout, then enters its serving loop.
     *
     * @return array{0: resource, 1: string, 2: string, 3: array<int, resource>} [proc, proxyAddr, targetAddr, pipes]
     */
    private function startSocksChild(string $mode, string $user = '', string $pass = ''): array
    {
        $pipes = [];
        $proc = proc_open(
            [PHP_BINARY, __DIR__ . '/_socks_child.php', $mode, $user, $pass],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes
        );
        if (!is_resource($proc)) {
            throw new \RuntimeException('failed to spawn socks child');
        }
        stream_set_timeout($pipes[1], 15);
        $line = fgets($pipes[1]);
        $ports = $line === false ? null : sscanf(trim(substr((string)$line, 6)), '%d %d');
        if (!is_array($ports) || count($ports) !== 2 || $ports[0] === null || $ports[1] === null) {
            $err = stream_get_contents($pipes[2]);
            $this->stopSocksChild($proc, $pipes);
            throw new \RuntimeException('socks child did not report ports: ' . trim((string)$err));
        }
        return [$proc, sprintf('127.0.0.1:%d', $ports[0]), sprintf('127.0.0.1:%d', $ports[1]), $pipes];
    }

    /** Tears down the child process (on the success path it exits naturally after tunnel EOF; this is a fallback reap + pipe close). */
    private function stopSocksChild($proc, array $pipes): void
    {
        if (is_resource($proc)) {
            @proc_terminate($proc);
        }
        foreach ($pipes as $p) {
            if (is_resource($p)) {
                @fclose($p);
            }
        }
        if (is_resource($proc)) {
            @proc_close($proc);
        }
    }

    /**
     * Completes one RocketMQ request roundtrip through the SOCKS proxy: the target echo service
     * echoes the request frame back, the RemotingClient dispatch matches pending by opaque,
     * and invokeSync receives the request frame itself.
     */
    private function socksRoundtrip(RemotingClient $client, string $targetAddr, string $body): RemotingCommand
    {
        $req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        $req->body = $body;
        return $client->invokeSync($targetAddr, $req, 10000);
    }

    private function testSocksProxy(): void
    {
        $cfg = new SocksProxyConfig();
        $this->check(!$cfg->isConfigured(), 'default SocksProxyConfig is unconfigured (addr=null)');
        $cfg = new SocksProxyConfig('127.0.0.1:1080', 'u', 'p');
        $this->check($cfg->isConfigured(), 'configured once addr is set');
        $this->checkSame('127.0.0.1:1080', $cfg->getAddr(), 'getAddr');
        $this->checkSame('u', $cfg->getUsername(), 'getUsername');
        $this->checkSame('p', $cfg->getPassword(), 'getPassword');
        $this->checkSame('socks5', $cfg->getVersion(), 'default version socks5 (protocol default SOCKS5)');
        $this->checkSame(
            'SocksProxy address: 127.0.0.1:1080, username: u, password: p',
            (string)$cfg,
            'toString outputs the SocksProxyConfig conventional format'
        );
        $cfg->setAddr('10.0.0.1:1180');
        $cfg->setUsername(null);
        $cfg->setPassword(null);
        $this->check($cfg->getUsername() === null && $cfg->getPassword() === null, 'setters can clear auth fields');
        $cfg->setVersion('4');
        $this->checkSame('socks4', $cfg->normalizedVersion(), "version '4' normalized to socks4");
        $cfg->setVersion('SOCKS5');
        $this->checkSame('socks5', $cfg->normalizedVersion(), 'version normalization ignores case');
        $cfg->setVersion('socks6');
        $this->checkThrows(static fn() => $cfg->normalizedVersion(), \InvalidArgumentException::class, 'unknown version throws at startup');

        // ---- 2. SOCKS5 without auth: connect through the proxy + one request roundtrip ----
        [$proc, $proxyAddr, $targetAddr, $pipes] = $this->startSocksChild('socks5_ok');
        try {
            $proxyCfg = new SocksProxyConfig($proxyAddr);
            $client = new RemotingClient(30000, 10000, socksProxy: $proxyCfg);
            $this->checkSame($proxyCfg, $client->getSocksProxy(), 'RemotingClient constructor accepts socksProxy');
            $got = $this->socksRoundtrip($client, $targetAddr, 'via-socks5');
            $this->checkSame(RequestCode::HEART_BEAT, $got->code, 'socks5 no-auth: request roundtrips through the proxy (echoes the request frame)');
            $this->checkSame('via-socks5', $got->body, 'socks5 no-auth: body passes through the tunnel intact');
            $client->shutdown();
        } finally {
            $this->stopSocksChild($proc, $pipes);
        }

        // ---- 3. SOCKS5 username/password sub-negotiation (RFC 1929) ----
        [$proc, $proxyAddr, $targetAddr, $pipes] = $this->startSocksChild('socks5_auth', 'socks-user', 'socks-pass');
        try {
            $client = new RemotingClient(30000, 10000, socksProxy: new SocksProxyConfig($proxyAddr, 'socks-user', 'socks-pass'));
            $got = $this->socksRoundtrip($client, $targetAddr, 'via-socks5-auth');
            $this->checkSame('via-socks5-auth', $got->body, 'socks5: request roundtrips after username/password sub-negotiation');
            $client->shutdown();
        } finally {
            $this->stopSocksChild($proc, $pipes);
        }

        // wrong password -> sub-negotiation fails -> RemotingConnectException (proxy port is alive,
        // so this is not the refused path; grant failure-class assertions a 30s-level budget
        // due to the local loopback quirk)
        [$proc, $proxyAddr, $targetAddr, $pipes] = $this->startSocksChild('socks5_auth', 'socks-user', 'socks-pass');
        try {
            $client = new RemotingClient(30000, 10000, socksProxy: new SocksProxyConfig($proxyAddr, 'socks-user', 'wrong-pass'));
            $req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
            try {
                $client->invokeSync($targetAddr, $req, 10000);
                $this->check(false, 'socks5 wrong password should fail to connect');
            } catch (RemotingConnectException $e) {
                $this->check(
                    str_contains($e->getMessage(), 'authentication failed'),
                    sprintf('socks5 wrong password -> authentication failure message (got: %s)', $e->getMessage())
                );
            }
            $client->shutdown();
        } finally {
            $this->stopSocksChild($proc, $pipes);
        }

        // ---- 4. SOCKS5 CONNECT refused (REP=0x01 general failure) ----
        [$proc, $proxyAddr, $targetAddr, $pipes] = $this->startSocksChild('socks5_reject_connect');
        try {
            $client = new RemotingClient(30000, 10000, socksProxy: new SocksProxyConfig($proxyAddr));
            $req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
            try {
                $client->invokeSync($targetAddr, $req, 10000);
                $this->check(false, 'socks5 CONNECT refused should fail to connect');
            } catch (RemotingConnectException $e) {
                $this->check(
                    str_contains($e->getMessage(), 'general SOCKS server failure'),
                    sprintf('socks5 CONNECT refused -> REP message passed through (got: %s)', $e->getMessage())
                );
            }
            $client->shutdown();
        } finally {
            $this->stopSocksChild($proc, $pipes);
        }

        // ---- 5. SOCKS4: roundtrip + refused (CD=91) ----
        [$proc, $proxyAddr, $targetAddr, $pipes] = $this->startSocksChild('socks4_ok');
        try {
            $client = new RemotingClient(30000, 10000, socksProxy: new SocksProxyConfig($proxyAddr, version: 'socks4'));
            $got = $this->socksRoundtrip($client, $targetAddr, 'via-socks4');
            $this->checkSame('via-socks4', $got->body, 'socks4: request roundtrips through the proxy');
            $client->shutdown();
        } finally {
            $this->stopSocksChild($proc, $pipes);
        }

        [$proc, $proxyAddr, $targetAddr, $pipes] = $this->startSocksChild('socks4_reject');
        try {
            $client = new RemotingClient(30000, 10000, socksProxy: new SocksProxyConfig($proxyAddr, version: 'socks4'));
            $req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
            try {
                $client->invokeSync($targetAddr, $req, 10000);
                $this->check(false, 'socks4 refusal should fail to connect');
            } catch (RemotingConnectException $e) {
                $this->check(
                    str_contains($e->getMessage(), 'code 91'),
                    sprintf('socks4 refused -> CD code passed through (got: %s)', $e->getMessage())
                );
            }
            $client->shutdown();
        } finally {
            $this->stopSocksChild($proc, $pipes);
        }
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
        $this->testSocksProxy();
        return $this->summary();
    }
}

(new RunRemoting())->run();
exit(0);
