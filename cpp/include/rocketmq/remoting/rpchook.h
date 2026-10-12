// 签名内容：先 makeCustomHeaderToNet()，再按 key 字典序取 extFields 的**全部**
// value（排除 Signature 键自身；拼接时只有 value，不带 key、不带 `=`/`&` 等分隔符），
// 最后拼上 body 原始字节。
// 签名值：标准 Base64( HMAC-SHA1(key = secretKey, data = 上述内容) )。
//
// SHA1 / HMAC / Base64 在本模块内自带实现，**不引入 OpenSSL 依赖**
// （与仓库「除 zlib 外零外部依赖」的约定一致）。
#ifndef ROCKETMQ_REMOTING_RPCHOOK_H
#define ROCKETMQ_REMOTING_RPCHOOK_H

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

namespace rocketmq {

class RPCHook {
public:
    virtual ~RPCHook() = default;

    // 发送前调用。实现**必须**自行保证线程安全；传输层不持锁调用本方法。
    virtual void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) = 0;

    virtual void doAfterResponse(const std::string& remoteAddr, const RemotingCommand& request,
                                 const RemotingCommand* response) {
        (void)remoteAddr;
        (void)request;
        (void)response;
    }
};

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

class AclClientRPCHook : public RPCHook {
public:
    explicit AclClientRPCHook(SessionCredentials credentials)
        : credentials_(std::move(credentials)) {}

    // AccessKey / SecurityToken 先写入（它们参与签名），再算签名，最后写 Signature。
    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override;

    const SessionCredentials& credentials() const { return credentials_; }

    static Bytes buildRequestContent(RemotingCommand& request);

    static std::string calcSignature(const std::string& secretKey, RemotingCommand& request);

private:
    SessionCredentials credentials_;
};

class StreamTypeRPCHook : public RPCHook {
public:
    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override;
};

class NamespaceRpcHook : public RPCHook {
public:
    explicit NamespaceRpcHook(std::function<std::string()> namespaceV2)
        : namespaceV2_(std::move(namespaceV2)) {}

    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override;


private:
    std::function<std::string()> namespaceV2_;
};

// 按注册顺序依次执行的组合钩子。
class ChainedRPCHook : public RPCHook {
public:
    explicit ChainedRPCHook(std::vector<std::shared_ptr<RPCHook>> hooks)
        : hooks_(std::move(hooks)) {}

    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override;
    void doAfterResponse(const std::string& remoteAddr, const RemotingCommand& request,
                         const RemotingCommand* response) override;

    // 链上的钩子（注册顺序）。单测用它直接断言 composeRequestHooks 装出来的链序。
    const std::vector<std::shared_ptr<RPCHook>>& hooks() const { return hooks_; }

private:
    std::vector<std::shared_ptr<RPCHook>> hooks_;
};

// 各 facade 在 start() 里统一走这里，顺序就不会写反 —— 反了会让 ACL 签名的内容
// 里缺 `nsd`/`ns`/`ReqT`，开鉴权的 broker 直接验签失败。
std::shared_ptr<RPCHook> composeRequestHooks(bool enableStreamRequestType,
                                             const std::shared_ptr<RPCHook>& userHook,
                                             const std::function<std::string()>& namespaceV2Getter =
                                                 std::function<std::string()>());

Bytes sha1(const Bytes& data);
Bytes hmacSha1(const std::string& key, const Bytes& data);
std::string base64Encode(const Bytes& data);
std::string hmacSha1Base64(const std::string& key, const Bytes& data);

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_RPCHOOK_H
