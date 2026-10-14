// Signed content: first makeCustomHeaderToNet(), then take **all** values of extFields in
// lexicographic key order (excluding the Signature key itself; concatenation carries only values, no key and no
// `=`/`&` separators), and finally append the raw body bytes.
// Signature value: standard Base64( HMAC-SHA1(key = secretKey, data = the content above) ).
//
// SHA1 / HMAC / Base64 are implemented inside this module, **without adding an OpenSSL dependency**
// (consistent with the repository convention of zero external dependencies except zlib).
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

    // Called before sending. Implementations **must** provide their own thread safety; the transport calls this without holding a lock.
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

    // Write AccessKey / SecurityToken first (they take part in the signature), then compute the signature, and finally write Signature.
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

// Combined hook that runs the registered hooks in registration order.
class ChainedRPCHook : public RPCHook {
public:
    explicit ChainedRPCHook(std::vector<std::shared_ptr<RPCHook>> hooks)
        : hooks_(std::move(hooks)) {}

    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override;
    void doAfterResponse(const std::string& remoteAddr, const RemotingCommand& request,
                         const RemotingCommand* response) override;

    // The hooks on the chain, in registration order. Tests use it to assert the order composeRequestHooks builds.
    const std::vector<std::shared_ptr<RPCHook>>& hooks() const { return hooks_; }

private:
    std::vector<std::shared_ptr<RPCHook>> hooks_;
};

// Every facade goes through this in start(), so the order cannot be inverted — an inverted order would leave
// `nsd`/`ns`/`ReqT` out of the ACL signature content, and an auth-enabled broker would fail the signature check.
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
