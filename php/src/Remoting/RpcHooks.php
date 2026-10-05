<?php

declare(strict_types=1);

namespace RocketMQ\Remoting;

use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestType;

/**
 * RPC 钩子（对应 org.apache.rocketmq.remoting.RPCHook，移植自 rpchook.py）。
 *
 * AclClientRPCHook：基于 accessKey/secretKey 计算签名并注入 extFields
 * （AccessKey / Signature / SecurityToken）。签名算法**逐字节对齐** Java：
 *
 *   - org.apache.rocketmq.acl.common.AclClientRPCHook#doBeforeRequest
 *   - org.apache.rocketmq.acl.common.AclUtils#combineRequestContent
 *   - org.apache.rocketmq.acl.common.AclSigner#calSignature
 *
 * 签名内容：按 key 字典序取 extFields 的**全部** value（排除 Signature 键自身；
 * 只有 value，不带 key、不带 `=`/`&` 之类分隔符）按 UTF-8 拼接，再拼上 body 原始字节。
 * 签名值：标准 Base64( HMAC-SHA1(key = secretKey, data = 上述内容) )。
 *
 * 注意两处顺序：AccessKey / SecurityToken 必须在算 content **之前**写入 extFields
 * （它们参与签名）；Signature 最后写入（它自己不参与签名）。
 */
class RPCHook
{
    public function doBeforeRequest(string $remoteAddr, RemotingCommand $request): void
    {
    }

    public function doAfterResponse(string $remoteAddr, RemotingCommand $request, ?RemotingCommand $response): void
    {
    }
}

/** 对应 org.apache.rocketmq.acl.common.SessionCredentials。 */
final class SessionCredentials
{
    public const CHARSET = 'utf-8';
    public const ACCESS_KEY = 'AccessKey';
    public const SECRET_KEY = 'SecretKey';
    public const SIGNATURE = 'Signature';
    public const SECURITY_TOKEN = 'SecurityToken';

    public function __construct(
        public string $accessKey,
        public string $secretKey,
        public string $securityToken = '',
    ) {
    }
}

class AclClientRPCHook extends RPCHook
{
    public const ACCESS_KEY = SessionCredentials::ACCESS_KEY;
    public const SIGNATURE = SessionCredentials::SIGNATURE;
    public const SECURITY_TOKEN = SessionCredentials::SECURITY_TOKEN;

    public function __construct(public SessionCredentials $credentials)
    {
    }

    public function doBeforeRequest(string $remoteAddr, RemotingCommand $request): void
    {
        // 顺序必须与 Java 一致：先写 AccessKey/SecurityToken（参与签名），
        // 再算签名，最后写 Signature（自身不参与签名）。
        $request->addExtField(self::ACCESS_KEY, $this->credentials->accessKey);
        if ($this->credentials->securityToken !== '') {
            $request->addExtField(self::SECURITY_TOKEN, $this->credentials->securityToken);
        }
        $signature = $this->calcSignature($this->credentials->secretKey, $request);
        $request->addExtField(self::SIGNATURE, $signature);
    }

    /**
     * 对应 Java AclUtils.combineRequestContent。
     *
     * Java 的 parseRequestContent 会先 request.makeCustomHeaderToNet() 把
     * customHeader 的字段落进 extFields，再按 TreeMap（key 字典序）取全部 value。
     * 这里等价：先 makeCustomHeaderToNet()，再对 ext_fields 排序。
     */
    public static function buildRequestContent(RemotingCommand $request): string
    {
        $request->makeCustomHeaderToNet();
        $fields = $request->extFields;
        $buf = '';
        $keys = array_keys($fields);
        // 与 Java TreeMap / Python sorted 一致：按字节序（codepoint）比较
        sort($keys, SORT_STRING);
        foreach ($keys as $key) {
            if ($key === SessionCredentials::SIGNATURE) {
                continue;
            }
            $value = $fields[$key] ?? null;
            if ($value === null) {
                continue;
            }
            $buf .= (string)$value;
        }
        $body = $request->body;
        if ($body !== null && $body !== '') {
            $buf .= $body;
        }
        return $buf;
    }

    /** 对应 Java AclSigner.calSignature：HmacSHA1 + 标准 Base64（带 `=` 填充）。 */
    public static function calcSignature(string $secretKey, RemotingCommand $request): string
    {
        $data = self::buildRequestContent($request);
        $digest = hash_hmac('sha1', $data, $secretKey, true);
        return base64_encode($digest);
    }
}

/** 兼容旧名（最初写作 AclRPCHook）。 */
class AclRpcHook extends AclClientRPCHook
{
}

/**
 * 给每个请求打上请求类型标记（对应 org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook）。
 *
 * Java 的 `doBeforeRequest` 只有一行：
 * `request.addExtField(MixAll.REQ_T, String.valueOf(RequestType.STREAM.getCode()))`，
 * 即 `ReqT = "0"`（`RequestType` 目前只有 STREAM 一个枚举项，code 为 `(byte) 0`）。
 *
 * ⚠ 注册顺序有意义：`MQClientAPIImpl:329-332` 的注释写着
 * "Inject stream rpc hook first to make reserve field signature"，即它必须注册在
 * 用户的 rpcHook（通常是 ACL 签名钩子）**之前**，这样 ReqT 才会被算进签名内容，
 * broker 侧才不会因为多出一个未签名字段而拒签。
 */
final class StreamTypeRPCHook extends RPCHook
{
    public function doBeforeRequest(string $remoteAddr, RemotingCommand $request): void
    {
        $request->addExtField(\RocketMQ\Common\MixAll::REQ_T, (string)RequestType::STREAM);
    }
}
