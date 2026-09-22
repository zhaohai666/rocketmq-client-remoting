// 客户端 TLS 会话（对应 Java NettyRemotingClient 的 pipeline.addFirst(SslHandler)）。
//
// 语义（Java 5.5.1 TlsSystemConfig/TlsHelper 核对）：
//   - TLS 包住**整个流**：任何 RocketMQ 帧之前完成握手；
//   - test mode（tls.test.mode.enable 默认 true）：信任 broker 自签证书、不校验主机名、
//     不带客户端证书 —— broker 用 PERMISSIVE + 自签证书即可即配即通；
//   - broker 侧 PERMISSIVE（tls.server.mode）在同一端口按首字节嗅探，明文客户端不受影响。
//
// 编译期开关：CMake `RMQ_ENABLE_TLS`（找到 OpenSSL 时默认 ON）。找不到 OpenSSL 时
// 本头文件提供**报错型占位**：setTlsEnable(true) 会抛出带说明的异常，其余路径零影响。
#ifndef ROCKETMQ_REMOTING_TLS_SESSION_H
#define ROCKETMQ_REMOTING_TLS_SESSION_H

#include <memory>
#include <mutex>
#include <string>

#include "rocketmq/common/net_compat.h"

#ifdef RMQ_HAS_TLS
#include <openssl/ssl.h>
#endif

namespace rocketmq {

#ifdef RMQ_HAS_TLS

// test-mode 客户端 SSL_CTX（verify=NONE、无客户端证书、默认 TLS1.2+）。
// 返回的 shared_ptr 持有 SSL_CTX*（deleter 内 SSL_CTX_free）。
std::shared_ptr<void> createClientSslContext(std::string& err);

// 一条连接的 TLS 会话。生命周期：connect 后 handshake()，之后 read/writeAll
// 替代裸 recv/send，close 时析构（内部 best-effort SSL_shutdown）。
//
// ⚠ OpenSSL 不支持两个线程同时用**同一个 SSL 对象**（维护者在 openssl/openssl#20622
// 的原话："You cannot share (most) OpenSSL objects between threads"；openssl-threads(7)
// 也只承诺"most objects are not safe for simultaneous use"）。而本传输层的形状是
// "一连接一个读线程 + 调用方线程写"：读线程的 SSL_pending/SSL_read 与调用方的
// SSL_write 必然在同一条会话上交叠，光靠连接层那把 writeMutex（只锁写侧）挡不住。
// 所以锁放在**会话内部**，四个入口每次 SSL_* 调用取放一次：
//   * 粒度是"一次 SSL_* 调用"而不是"一次业务读写"——writeAll 的多次 SSL_write 逐次取放，
//     重试前的 10ms 睡眠在锁外，绝不抱着锁睡觉；
//   * 读线程只在 select 报可读（或 SSL_pending>0）时才进 read，正常情况下锁内只有一次
//     syscall 的量级；只有"记录只到了一半"这种对端停摆的极端情况会抱锁到 SO_RCVTIMEO
//     （1s）到点，那是这条链路本就要付的代价。
class TlsSession {
public:
    explicit TlsSession(std::shared_ptr<void> ctx);
    ~TlsSession();
    TlsSession(const TlsSession&) = delete;
    TlsSession& operator=(const TlsSession&) = delete;

    // 阻塞握手；失败返回 false 并填 err（含超时）。host 用作 SNI。
    bool handshake(netcompat::socket_t sock, const std::string& host, int timeoutMillis,
                   std::string& err);

    // 读：>0 = 字节数；0 = 对端正常关闭；-2 = 暂无数据（重试）；<0 = 错误。
    // SSL 内部缓冲未空时立即返回。
    int read(netcompat::socket_t sock, char* buf, int len);

    // 写全部字节；false = 对端关闭或错误。
    bool writeAll(netcompat::socket_t sock, const char* data, size_t len);

    // SSL 内部缓冲中尚未交付的字节数（>0 时 select 会漏报可读）。
    int pending();

    void shutdown();

private:
    std::shared_ptr<void> ctx_;
    SSL* ssl_ = nullptr;
    // 会话级 io 锁：见类注释。只包 SSL_* 调用，不包任何睡眠/等待。
    std::mutex ioMutex_;
};

#else  // !RMQ_HAS_TLS

// 未编入 OpenSSL 的占位：握手永远失败并给出明确原因，其余 API 不可达。
class TlsSession {
public:
    explicit TlsSession(std::shared_ptr<void> ctx) { (void)ctx; }
    ~TlsSession() = default;
    bool handshake(netcompat::socket_t sock, const std::string& host, int timeoutMillis,
                   std::string& err) {
        (void)sock; (void)host; (void)timeoutMillis;
        err = "client built without TLS support (install OpenSSL or set RMQ_ENABLE_TLS=ON)";
        return false;
    }
    int read(netcompat::socket_t sock, char* buf, int len) {
        (void)sock; (void)buf; (void)len; return -1;
    }
    bool writeAll(netcompat::socket_t sock, const char* data, size_t len) {
        (void)sock; (void)data; (void)len; return false;
    }
    int pending() { return 0; }
    void shutdown() {}
};

inline std::shared_ptr<void> createClientSslContext(std::string& err) {
    err = "client built without TLS support (install OpenSSL or set RMQ_ENABLE_TLS=ON)";
    return nullptr;
}

#endif  // RMQ_HAS_TLS

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_TLS_SESSION_H
