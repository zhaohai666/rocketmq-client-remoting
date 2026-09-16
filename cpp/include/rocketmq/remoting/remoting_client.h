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

class RemotingClient {
public:
    using InvokeCallback = std::function<void(const RemotingCommand&)>;

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
    // timeoutMillis < 0 表示使用 invokeTimeoutMillis。
    //
    // opaque 由调用方负责唯一性——用 RemotingCommand::createRequestCommand() 创建即可
    // （其内部按全局计数器分配）。传输层**不会**把 opaque==0 当成"未设置"来改写，
    // 因为计数器从 0 起算，第一个请求的 opaque 合法值就是 0。仅当 opaque 与**在途**
    // 请求冲突时，传输层才会改分配一个新的并写回 request.opaque。
    RemotingCommand invokeSync(const std::string& addr, RemotingCommand& request,
                               int32_t timeoutMillis = -1);

    // 异步调用：发送后立即返回，响应到达时在读线程里触发 callback。
    // 注意 callback 在**读线程**中执行，实现需自行保证线程安全。
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
