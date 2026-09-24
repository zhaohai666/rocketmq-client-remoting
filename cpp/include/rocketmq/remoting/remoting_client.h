// Socket 长连接客户端（对应 org.apache.rocketmq.remoting.netty.NettyRemotingClient 的核心能力）。
//
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
//
// 与 Java 的一处口径差异（刻意如此）：NettyRemotingClient#scanChannelTablesOfNameServer
// （channelNotActiveInterval=60s）在 Java 客户端里**从未被调度** —— client + remoting 全树
// grep 不到调用点，属于死代码，所以这里不做空闲连接回收；对端真断开时读线程立刻见到 EOF，
// 惰性清理已覆盖真实场景。异步请求的超时清理（scanResponseTable）则有实现，连接断开时立刻
// 失败该连接名下的在途请求（failFast / requestFail）也有实现：读线程退出时把这些请求判为
// RemotingSendRequestException（不是超时），并唤醒还在等响应的同步调用方。
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

namespace rocketmq {

// 异步回调的错误载体（对应 Java ``InvokeCallback`` 收到的那个 Throwable，
// 也是 Python 版 ``invoke_async`` 回调里的 error 参数）。
//
// 为什么不能只有一个字符串：Java 的异步发送重试判据**看的是异常类型**
// （``MQClientAPIImpl:680-693`` 三个 instanceof 分支的文案各不相同，且只有
// RemotingTooMuchRequestException 不重试）。按错误消息的措辞去猜类型，等于把
// 重试行为挂在一句日志文案上，改个措辞就会改变语义。
struct InvokeError {
    enum class Kind {
        NONE = 0,          // 没有错误（响应已送达，response 有效）
        TIMEOUT,           // 对应 RemotingTimeoutException
        SEND_REQUEST,      // 对应 RemotingSendRequestException
        CONNECT,           // 对应 RemotingConnectException
        TOO_MUCH_REQUEST,  // 对应 RemotingTooMuchRequestException（唯一不重试的传输错误）
        // 不是传输错误：**响应已经到达**，只是处理失败了 —— 两种来源
        //   * broker 回了非 SUCCESS 响应码（Java processSendResponse 抛 MQBrokerException）
        //   * 响应体解析不出来（RemotingCommandException 一类）
        // 传输层不会产出它，只有客户端层解析应答时才设。合并成一个类型是因为 Java 在
        // 这两种分支上走的是同一条路（``MQClientAPIImpl:669-673`` operationSucceed 的
        // catch(Exception) → onExceptionImpl(needRetry=false)）：**不换 broker 也不重试**，
        // 与同步发送按 retryResponseCodes 换 broker 的语义刻意不同，必须能被区分出来。
        RESPONSE_FAILED,
        OTHER,             // 其它（Java 的 "unknown reason" 分支）
    };

    Kind kind = Kind::NONE;
    std::string message;

    InvokeError() = default;
    InvokeError(Kind k, std::string msg) : kind(k), message(std::move(msg)) {}

    bool empty() const { return kind == Kind::NONE && message.empty(); }
};

class RemotingClient {
public:
    // 异步回调（对应 Java InvokeCallback#operationComplete(ResponseFuture)）。
    // error 非空表示**失败**（超时、连接断开等），此时 response 无效必须忽略；
    // error 为空表示收到响应。Java 用 (response, throwable) 两个参数表达同一件事。
    using InvokeCallback = std::function<void(const RemotingCommand& response,
                                              const InvokeError& error)>;

    // broker 主动发来的**请求**（而非响应）的处理器：handler(请求命令, 对端地址)。
    // 对应 Java NettyRemotingAbstract 的 processor 表。返回值语义与 Java
    // NettyRequestProcessor#processRequest 一致：
    //   * 返回 std::nullopt —— 不回响应（Java 返回 null）。典型：事务回查
    //     CHECK_TRANSACTION_STATE(39)，broker 侧用 invokeOneway 发的，本就不期待响应。
    //   * 返回 RemotingCommand —— 把它作为响应写回（opaque 由框架填成请求的 opaque）。
    //     典型：PUSH_REPLY_MESSAGE_TO_CLIENT(326)，broker 用 invokeSync **等**这个响应，
    //     不回它 broker 侧会等到超时并在日志里记一条 push reply fail。
    using RequestProcessor = std::function<std::optional<RemotingCommand>(
        const RemotingCommand&, const std::string&)>;

    // 单帧上限（与 Java RemotingSysResponseCode / NettyRemotingClient 的 16MB 限制一致）
    static constexpr int32_t MAX_FRAME_LENGTH = 16 * 1024 * 1024;

    // 默认 3s 建连、15s 调用超时（对应 Python RemotingClient 默认值）
    explicit RemotingClient(int32_t connectTimeoutMillis = 3000,
                            int32_t invokeTimeoutMillis = 15000);
    ~RemotingClient();

    RemotingClient(const RemotingClient&) = delete;
    RemotingClient& operator=(const RemotingClient&) = delete;

    // ---- 请求发送 ----
    // 同步调用：等到响应或超时。超时抛 RemotingTimeoutException；
    // 建连失败抛 RemotingConnectException；发送失败抛 RemotingSendRequestException。
    // 对端在响应之前断开连接时也立刻抛 RemotingSendRequestException（对应 Java failFast
    // 唤醒等待方），不会让调用方等满 timeout 才拿到一个语义错误的超时。
    // timeoutMillis < 0 表示使用 invokeTimeoutMillis。
    //
    // opaque 由调用方负责唯一性——用 RemotingCommand::createRequestCommand() 创建即可
    // （其内部按全局计数器分配）。传输层**不会**把 opaque==0 当成"未设置"来改写，
    // 因为计数器从 0 起算，第一个请求的 opaque 合法值就是 0。仅当 opaque 与**在途**
    // 请求冲突时，传输层才会改分配一个新的并写回 request.opaque。
    RemotingCommand invokeSync(const std::string& addr, RemotingCommand& request,
                               int32_t timeoutMillis = -1);

    // 异步调用：发送后立即返回，响应到达时在读线程里触发 callback。
    // 注意 callback 在**读线程**（或超时清理线程）中执行，实现需自行保证线程安全。
    // timeoutMillis 会登记到在途表项，超时后由清理线程以 error(kind=TIMEOUT) 回调一次
    // （对应 Java NettyRemotingAbstract 的 scanResponseTable），不会因对端不回包而永久悬挂。
    // 对端在响应之前断开连接时，改由该连接的读线程以 error(kind=SEND_REQUEST) 立刻回调一次
    // （对应 Java failFast -> requestFail，不等超时清理线程，也不报成超时）。
    // ⚠ 建连/写失败在**本函数上就地抛出**类型化异常（Java 的 invokeAsync 也这样），
    //    调用方要 try/catch；异步回来的错误只可能是超时、断连或 GO_AWAY 重发失败。
    void invokeAsync(const std::string& addr, RemotingCommand& request,
                     InvokeCallback callback, int32_t timeoutMillis = -1);

    // 单向调用：置 oneway 标志后发送，不等响应（对应 Java invokeOneway）。
    void invokeOneway(const std::string& addr, RemotingCommand& request);

    // ---- broker 主动请求处理 ----
    // 注册/注销按 requestCode 索引的请求处理器。
    //
    // 只有**请求类型且不在本地在途响应表里**的命令才会派发到这里（典型场景：
    // broker 发来的事务回查 CHECK_TRANSACTION_STATE=39），不会影响现有的
    // invokeSync / invokeAsync 响应分发。handler 在读线程里被调用，需自保证线程安全。
    void registerProcessor(int32_t requestCode, RequestProcessor handler);
    void unregisterProcessor(int32_t requestCode);

    // ---- RPC 钩子（ACL 鉴权等）----
    // 对应 Java NettyRemotingClient#registerRPCHook。钩子在每次请求**编码之前**
    // 于发送路径上被调用（invokeSync / invokeAsync / invokeOneway 三条路径共用同一
    // 注入点），从而能把 AccessKey/Signature 写进 extFields。
    //
    // 注册发生在 start 阶段、读路径只读，故不需要在调用钩子时持锁。
    // **首次注册生效（first-wins）**：已有钩子时返回 false 且不覆盖，与 Java 的
    // 行为一致（Java 在构造 MQClientInstance 时绑定钩子，同一 clientId 复用实例）。
    // 因此钩子/凭据必须在 start() 之前设置。
    //
    // 注意（与 Java 的有意差异）：Java 还在响应完成回调里调用 doAfterResponse，
    // 本实现**没有**该调用——响应在读线程里分发，此处不持有请求对象，
    // 为了不把每个请求的 body 都拷一份挂在在途表上，故省略。
    // AclClientRPCHook#doAfterResponse 本身是空实现，因此无功能影响。
    // 返回 true 表示安装成功，false 表示已有钩子被保留。
    bool registerRPCHook(std::shared_ptr<RPCHook> hook);
    void unregisterRPCHook();

    // ---- TLS（对应 Java NettyRemotingClient 的 isUseTLS / tls.enable）----
    // 必须在**首条连接建立前**调用（建连时才握手）。true 时为每条新连接做 TLS 握手；
    // test mode 信任 broker 自签证书（Java tls.test.mode.enable 默认 true 的等价语义）。
    // 未编入 OpenSSL（RMQ_ENABLE_TLS=OFF 或找不到 OpenSSL）时传 true 会抛
    // MQClientException，明文路径零影响。
    void setTlsEnable(bool enable);
    bool tlsEnable() const;

    // ---- GO_AWAY（对应 Java NettyClientConfig.enableReconnectForGoAway，默认 true）----
    // broker / proxy 优雅下线时给在途请求回 ResponseCode.GO_AWAY(1500)，语义是
    // 「这条连接别再用了」。开启时换一条连接重发一次（只一次），第二次仍是 GO_AWAY
    // 就按发送失败抛出；关掉则直接失败、不重连。必须在首条连接建立前设置。
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

    // 当前在途请求数（测试/诊断用）。对应 Java NettyRemotingAbstract.responseTable
    // 的大小：真机验证用它证明「对端断开后在途表被 failFast 排空」，而不是等超时
    // 清理线程慢慢扫。
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
