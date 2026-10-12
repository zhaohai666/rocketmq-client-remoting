// 提供：连接管理（惰性建连 + 复用 + TCP_NODELAY）、invokeSync / invokeAsync /
// invokeOneway、opaque -> 响应回调分发、超时控制、连接状态探活。
//
// 线程模型（与 Python 参考实现一致，读线程每连接一个）：
//   - 调用线程：encode -> 加写锁 -> sendall；随后在 invokeSync 里等条件变量
//   - 读线程：select(300ms) -> recv -> 按 totalLength(4) 分帧 -> decode ->
//             按 opaque 从响应表取出 future -> 唤醒调用线程 / 触发回调
//
// 线格式：totalLength(4) | headerLength(4) | header | body
//   totalLength = 4 + headerLength + bodyLength（即首 4 字节之后的所有字节数）
#ifndef ROCKETMQ_REMOTING_REMOTING_CLIENT_H
#define ROCKETMQ_REMOTING_REMOTING_CLIENT_H

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/rpchook.h"
#include "rocketmq/remoting/socks_proxy.h"

namespace rocketmq {

struct TlsOptions {
    std::string caCert;
    std::string clientCert;
    std::string clientKey;
    std::string serverName;

    bool empty() const { return caCert.empty() && clientCert.empty() && clientKey.empty() && serverName.empty(); }
};

struct InvokeError {
    enum class Kind {
        NONE = 0,          // 没有错误（响应已送达，response 有效）
        TIMEOUT,           // 对应 RemotingTimeoutException
        SEND_REQUEST,      // 对应 RemotingSendRequestException
        CONNECT,           // 对应 RemotingConnectException
        TOO_MUCH_REQUEST,  // 对应 RemotingTooMuchRequestException（唯一不重试的传输错误）
        RESPONSE_FAILED,
        OTHER,
    };

    Kind kind = Kind::NONE;
    std::string message;
    int32_t responseCode = 0;

    InvokeError() = default;
    InvokeError(Kind k, std::string msg) : kind(k), message(std::move(msg)) {}
    InvokeError(Kind k, std::string msg, int32_t code)
        : kind(k), message(std::move(msg)), responseCode(code) {}

    bool empty() const { return kind == Kind::NONE && message.empty(); }
};

class RemotingClient {
public:
    using InvokeCallback = std::function<void(const RemotingCommand& response,
                                              const InvokeError& error)>;

    using RequestProcessor = std::function<std::optional<RemotingCommand>(
        const RemotingCommand&, const std::string&)>;

    static constexpr int32_t MAX_FRAME_LENGTH = 16 * 1024 * 1024;

    // 默认 3s 建连、15s 调用超时（对应 Python RemotingClient 默认值）
    explicit RemotingClient(int32_t connectTimeoutMillis = 3000,
                            int32_t invokeTimeoutMillis = 15000);
    ~RemotingClient();

    RemotingClient(const RemotingClient&) = delete;
    RemotingClient& operator=(const RemotingClient&) = delete;

    // opaque 由调用方负责唯一性——用 RemotingCommand::createRequestCommand() 创建即可
    // （其内部按全局计数器分配）。传输层**不会**把 opaque==0 当成"未设置"来改写，
    // 因为计数器从 0 起算，第一个请求的 opaque 合法值就是 0。仅当 opaque 与**在途**
    // 请求冲突时，传输层才会改分配一个新的并写回 request.opaque。
    RemotingCommand invokeSync(const std::string& addr, RemotingCommand& request,
                               int32_t timeoutMillis = -1);

    void invokeAsync(const std::string& addr, RemotingCommand& request,
                     InvokeCallback callback, int32_t timeoutMillis = -1);

    void invokeOneway(const std::string& addr, RemotingCommand& request);

    // ---- broker 主动请求处理 ----
    // 注册/注销按 requestCode 索引的请求处理器。
    //
    // 只有**请求类型且不在本地在途响应表里**的命令才会派发到这里（典型场景：
    // broker 发来的事务回查 CHECK_TRANSACTION_STATE=39），不会影响现有的
    // invokeSync / invokeAsync 响应分发。handler 在读线程里被调用，需自保证线程安全。
    void registerProcessor(int32_t requestCode, RequestProcessor handler);
    void unregisterProcessor(int32_t requestCode);

    bool registerRPCHook(std::shared_ptr<RPCHook> hook);
    void unregisterRPCHook();

    void setTlsEnable(bool enable);
    bool tlsEnable() const;
    // 设置 TLS 细化选项（caCert/clientCert/clientKey/serverName）。必须在首条连接建立前
    // 调用；caCert 非空时 setTlsEnable(true) 会创建严格校验的 SSL_CTX（证书链 + 主机名）。
    // 若 TLS 已开启则立即重建 ctx（options 变了 ctx 必须跟着变），失败抛 RemotingException。
    void setTlsOptions(const TlsOptions& options);

    void setSocksProxy(const SocksProxyConfig& proxy);
    // 是否已配置代理（测试/诊断用）
    bool socksProxyEnabled() const;

    void setEnableReconnectForGoAway(bool enable);
    bool enableReconnectForGoAway() const;

    // ---- 连接管理 ----
    bool isChannelWritable(const std::string& addr) const;
    void closeChannel(const std::string& addr);

    // 预留：NameServer 地址列表由上层（MQClientInstance）维护
    void updateNameServerAddressList(const std::vector<std::string>& addrs);

    // 关闭全部连接并回收读线程；可重复调用
    void shutdown();

    int32_t connectTimeoutMillis() const { return connectTimeoutMillis_; }
    int32_t invokeTimeoutMillis() const { return invokeTimeoutMillis_; }

    // 当前活跃连接数（测试/诊断用）
    size_t connectionCount() const;

    size_t pendingRequestCount() const;

    // "host:port" 拆分，支持 IPv6 的 [::1]:10911 形式
    static void parseAddress(const std::string& addr, std::string& host, std::string& port);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    int32_t connectTimeoutMillis_;
    int32_t invokeTimeoutMillis_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_REMOTING_CLIENT_H
