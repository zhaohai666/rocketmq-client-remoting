// RPC 钩子（对应 org.apache.rocketmq.remoting.RPCHook）。
//
// AclClientRPCHook：基于 accessKey/secretKey 计算签名并注入 extFields
// （AccessKey / Signature / SecurityToken）。签名算法**逐字节对齐** Java：
//
//   - org.apache.rocketmq.acl.common.AclClientRPCHook#doBeforeRequest
//   - org.apache.rocketmq.acl.common.AclUtils#combineRequestContent
//   - org.apache.rocketmq.acl.common.AclSigner#calSignature
//
// 签名内容：先 makeCustomHeaderToNet()，再按 key 字典序取 extFields 的**全部**
// value（排除 Signature 键自身；拼接时只有 value，不带 key、不带 `=`/`&` 等分隔符），
// 最后拼上 body 原始字节。
// 签名值：标准 Base64( HMAC-SHA1(key = secretKey, data = 上述内容) )。
//
// SHA1 / HMAC / Base64 在本模块内自带实现，**不引入 OpenSSL 依赖**
// （与仓库「除 zlib 外零外部依赖」的约定一致）。
#ifndef ROCKETMQ_REMOTING_RPCHOOK_H
#define ROCKETMQ_REMOTING_RPCHOOK_H

#include <string>
#include <utility>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

namespace rocketmq {

// 对应 org.apache.rocketmq.remoting.RPCHook
class RPCHook {
public:
    virtual ~RPCHook() = default;

    // 发送前调用。实现**必须**自行保证线程安全；传输层不持锁调用本方法。
    virtual void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) = 0;

    // 收到响应后调用（AclClientRPCHook 里是空实现，与 Java 一致）。
    virtual void doAfterResponse(const std::string& remoteAddr, const RemotingCommand& request,
                                 const RemotingCommand* response) {
        (void)remoteAddr;
        (void)request;
        (void)response;
    }
};

// 对应 org.apache.rocketmq.acl.common.SessionCredentials
class SessionCredentials {
public:
    static constexpr const char* ACCESS_KEY = "AccessKey";
    static constexpr const char* SECRET_KEY = "SecretKey";
    static constexpr const char* SIGNATURE = "Signature";
    static constexpr const char* SECURITY_TOKEN = "SecurityToken";

    SessionCredentials() = default;
    SessionCredentials(std::string accessKey, std::string secretKey, std::string securityToken = "")
        : accessKey_(std::move(accessKey)),
          secretKey_(std::move(secretKey)),
          securityToken_(std::move(securityToken)) {}

    const std::string& accessKey() const { return accessKey_; }
    const std::string& secretKey() const { return secretKey_; }
    const std::string& securityToken() const { return securityToken_; }

private:
    std::string accessKey_;
    std::string secretKey_;
    std::string securityToken_;
};

// 对应 org.apache.rocketmq.acl.common.AclClientRPCHook
class AclClientRPCHook : public RPCHook {
public:
    explicit AclClientRPCHook(SessionCredentials credentials)
        : credentials_(std::move(credentials)) {}

    // AccessKey / SecurityToken 先写入（它们参与签名），再算签名，最后写 Signature。
    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override;

    const SessionCredentials& credentials() const { return credentials_; }

    // 对应 Java AclUtils.combineRequestContent（内部会先 makeCustomHeaderToNet()，
    // 因此形参是**非 const** 引用）。返回可供签名/对拍使用的原始字节串。
    static Bytes buildRequestContent(RemotingCommand& request);

    // 对应 Java AclSigner.calSignature：HMAC-SHA1 + 标准 Base64。
    static std::string calcSignature(const std::string& secretKey, RemotingCommand& request);

private:
    SessionCredentials credentials_;
};

// ------------------------------------------------------------------ 原语
// SHA1 / HMAC-SHA1 / 标准 Base64（带 '=' 填充）。导出出来是为了让单测能直接
// 用 Java（AclProbe）与 Python 产出的固定向量逐字节对拍。
Bytes sha1(const Bytes& data);
Bytes hmacSha1(const std::string& key, const Bytes& data);
std::string base64Encode(const Bytes& data);
std::string hmacSha1Base64(const std::string& key, const Bytes& data);

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_RPCHOOK_H
