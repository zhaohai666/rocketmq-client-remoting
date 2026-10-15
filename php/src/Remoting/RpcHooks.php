<?php

declare(strict_types=1);

namespace RocketMQ\Remoting;

use RocketMQ\Common\MixAll;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestType;

/**
 * Request/response hooks: doBeforeRequest mutates the request before it is sent, doAfterResponse
 * inspects the result after the reply arrives.
 *
 * AclClientRPCHook: computes the signature from accessKey/secretKey and injects extFields
 * (AccessKey / Signature / SecurityToken). The signature algorithm must be byte-for-byte
 * identical to the broker's verification:
 *
 * Signed content: take the **full** set of extFields values in key lexicographic order
 * (excluding the Signature key itself; values only — no key, no separators like `=`/`&`),
 * concatenate them as UTF-8, then append the raw body bytes.
 * Signature value: standard Base64( HMAC-SHA1(key = secretKey, data = the content above) ).
 *
 * Note two ordering points: AccessKey / SecurityToken must be written into extFields **before**
 * computing the content (they take part in the signature); Signature is written last (it does
 * not sign itself).
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

/** ACL session credentials; the three constants are the literal extFields key names the broker reads. */
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
        // The write order cannot be swapped: write AccessKey/SecurityToken first (they take part
        // in the signature), then compute the signature, and write Signature last (it does not
        // sign itself).
        $request->addExtField(self::ACCESS_KEY, $this->credentials->accessKey);
        if ($this->credentials->securityToken !== '') {
            $request->addExtField(self::SECURITY_TOKEN, $this->credentials->securityToken);
        }
        $signature = $this->calcSignature($this->credentials->secretKey, $request);
        $request->addExtField(self::SIGNATURE, $signature);
    }

    /**
     * Builds the content to be signed.
     *
     * When the broker verifies the signature it first spreads the custom header fields into
     * extFields, then takes all values in key lexicographic order, so here makeCustomHeaderToNet()
     * must run before sorting too; spreading fewer or more fields yields a different signature.
     */
    public static function buildRequestContent(RemotingCommand $request): string
    {
        $request->makeCustomHeaderToNet();
        $fields = $request->extFields;
        $buf = '';
        $keys = array_keys($fields);
        // Compare by byte order (codepoint), matching the broker-side key lexicographic order
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

    /** HmacSHA1 + standard Base64 (with `=` padding); the broker compares the shape of these two steps literally. */
    public static function calcSignature(string $secretKey, RemotingCommand $request): string
    {
        $data = self::buildRequestContent($request);
        $digest = hash_hmac('sha1', $data, $secretKey, true);
        return base64_encode($digest);
    }
}

/** Compatibility alias for the old name (originally written as AclRPCHook). */
class AclRpcHook extends AclClientRPCHook
{
}

/**
 * Stamps every request with a request-type marker (extFields key `ReqT`, see `MixAll::REQ_T`).
 *
 * The value is always `ReqT = "0"` (`RequestType` currently has STREAM as its only entry, with
 * code `(byte) 0`).
 *
 * WARNING: registration order matters. This hook must be registered **before** user hooks
 * (usually the ACL signing hook) so that ReqT is included in the signed content; installed the
 * other way around, the broker verification sees one extra unsigned field and rejects outright.
 */
final class StreamTypeRPCHook extends RPCHook
{
    public function doBeforeRequest(string $remoteAddr, RemotingCommand $request): void
    {
        $request->addExtField(\RocketMQ\Common\MixAll::REQ_T, (string)RequestType::STREAM);
    }
}

/**
 * 5.x server-side namespace hook: when `namespaceV2` is non-empty, adds **two** extension
 * headers to the request
 *
 *     nsd = "true"、ns = <namespaceV2>
 *
 * (key constants see `MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD` / `..._NAMESPACE_FIELD`).
 * `doAfterResponse` does not override — the base class is already an empty implementation.
 *
 * This is the **server-side** namespace mechanism: the broker resolves the request to the
 * corresponding instance (the serverless instance ID) by `ns`, and the client does **not**
 * rewrite topic/group names; it is unrelated to the `namespace` field (where the client itself
 * prepends the `namespace%` prefix, see `NamespaceUtil`) — two independent mechanisms.
 *
 * WARNING: registration order is semantics, not style. This port puts it at the **front** of the
 * hook chain (Namespace -> Stream -> user hook (ACL signing) -> DynamicalExtField; chain
 * assembly in `MQClientInstance`), so `nsd`/`ns` are necessarily written into extFields and
 * covered by the signature **before** signing.
 * Reversed, the signature still "looks valid", but a broker with authentication enabled sees
 * two extra unsigned fields and rejects the request outright.
 *
 * The value uses a "closure" instead of a constructor-time snapshot: namespaceV2 is read live
 * per request so config changes are followed; hence this accepts a `string` (fixed value) or a
 * `callable` (invoked once per request).
 * Also, this hook must be registered even for **empty values** (registration is unconditional);
 * the emptiness check lives inside the hook:
 * when `namespaceV2` is empty, no field is written at all — in particular extFields must
 * **not** be initialized to an empty map; "extFields stays untouched when no namespace is
 * configured" is behavior pinned by tests.
 */
final class NamespaceRpcHook extends RPCHook
{
    /** @var \Closure(): string reads namespaceV2 live per request, no snapshot kept */
    private \Closure $namespaceV2;

    public function __construct(string|callable|null $namespaceV2 = '')
    {
        // WARNING: check order — string before callable. In PHP `is_callable('trim')` is true;
        // checking callable first would misread a namespace value that happens to share a name
        // with a function as a getter (trimming the namespace on every request).
        if (is_string($namespaceV2)) {
            $fixed = $namespaceV2;
            $this->namespaceV2 = static function () use ($fixed): string {
                return $fixed;
            };
        } elseif ($namespaceV2 === null) {
            // null means no namespace is configured.
            // Do not hand null to Closure::fromCallable ('' would be treated as a function name
            // and fatal outright).
            $this->namespaceV2 = static function (): string {
                return '';
            };
        } else {
            $this->namespaceV2 = \Closure::fromCallable($namespaceV2);
        }
    }

    /** The namespaceV2 actually in effect for this request (empty string = no namespace configured). */
    public function namespaceV2Value(): string
    {
        return (string) ($this->namespaceV2)();
    }

    public function doBeforeRequest(string $remoteAddr, RemotingCommand $request): void
    {
        // The emptiness check only accepts an empty string, no trimming: a whitespace-only
        // namespace value is still reported as a valid value.
        $namespaceV2 = (string) ($this->namespaceV2)();
        if ($namespaceV2 === '') {
            return;
        }
        $request->addExtField(MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD, 'true');
        $request->addExtField(MixAll::RPC_REQUEST_HEADER_NAMESPACE_FIELD, $namespaceV2);
    }
}
