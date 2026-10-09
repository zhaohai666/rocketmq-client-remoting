<?php

declare(strict_types=1);

namespace RocketMQ\Remoting;

use RocketMQ\Common\MixAll;
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

/**
 * 5.x 新命名空间钩子（对应 org.apache.rocketmq.client.rpchook.NamespaceRpcHook）。
 *
 * Java 的 `doBeforeRequest` 只有一段：`namespaceV2` 非空时给请求加**两个**扩展头
 *
 *     nsd = "true"、ns = <namespaceV2>
 *
 * （常量见 `MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD` / `..._NAMESPACE_FIELD`，
 * common/MixAll.java:122-123）。`doAfterResponse` 是空实现——基类 `RPCHook` 已是空方法，
 * 这里不覆写即与 Java 逐字一致。
 *
 * 这是**服务端**命名空间机制：broker 按 `ns` 把请求解析到对应实例（阿里云系 serverless
 * 实例 ID），客户端**不**改 topic/group 名；它和 `namespace` 字段（客户端拼 `namespace%`
 * 前缀，见 `NamespaceUtil`）是两套互不相干的机制。
 *
 * ⚠ 注册顺序是语义而不是风格：Java `MQClientAPIImpl:329-335` 把它排在钩子链**最前**
 * （Namespace → Stream → 用户钩子(ACL 签名) → DynamicalExtField），所以 `nsd`/`ns`
 * 必然在算签名**之前**写进 extFields、被签名覆盖。装反了签名照样「看着合法」，但开鉴权的
 * broker 验签时会多出两个没签过的字段直接拒掉请求，本端口在 `MQClientInstance` 构造器里
 * 按同一顺序装链。
 *
 * 取值用「构造函数」而非「构造时快照」：Java 每笔请求实时读
 * `clientConfig.getNamespaceV2()`，配置改了要能跟着走，故这里接受
 * `string`（固定值，等价 Go 端）或 `callable`（每笔请求调一次，等价 C# 端的 Func）。
 * 另外**空值也要注册**本钩子（与 Java 一致，注册无门槛），空判在钩子内部：
 * `namespaceV2` 为空时一个字段都不写——尤其**不能**把 extFields 初始化成空 map，
 * Java 的 NamespaceRpcHookTest 断言的就是「没配命名空间时 extFields 保持没被碰过」。
 */
final class NamespaceRpcHook extends RPCHook
{
    /** @var \Closure(): string 实时取 namespaceV2（对应 Java 的 clientConfig.getNamespaceV2()） */
    private \Closure $namespaceV2;

    public function __construct(string|callable|null $namespaceV2 = '')
    {
        // ⚠ 判序：字符串先于 callable。PHP 里 `is_callable('trim')` 为真，若先判 callable，
        // 一个恰好与函数同名的命名空间值会被当成取值函数（每笔请求去 trim 一遍命名空间）。
        if (is_string($namespaceV2)) {
            $fixed = $namespaceV2;
            $this->namespaceV2 = static function () use ($fixed): string {
                return $fixed;
            };
        } elseif ($namespaceV2 === null) {
            // null = Java 的 clientConfig.getNamespaceV2() 返回 null：未配置。
            // 不能把 null 交给 Closure::fromCallable（'' 会被当函数名，直接 fatal）。
            $this->namespaceV2 = static function (): string {
                return '';
            };
        } else {
            $this->namespaceV2 = \Closure::fromCallable($namespaceV2);
        }
    }

    /** 本次请求实际生效的 namespaceV2（空串 = 未配命名空间）。 */
    public function namespaceV2Value(): string
    {
        return (string) ($this->namespaceV2)();
    }

    public function doBeforeRequest(string $remoteAddr, RemotingCommand $request): void
    {
        // Java: StringUtils.isNotEmpty(clientConfig.getNamespaceV2())——只判空串，不 trim。
        $namespaceV2 = (string) ($this->namespaceV2)();
        if ($namespaceV2 === '') {
            return;
        }
        $request->addExtField(MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD, 'true');
        $request->addExtField(MixAll::RPC_REQUEST_HEADER_NAMESPACE_FIELD, $namespaceV2);
    }
}
