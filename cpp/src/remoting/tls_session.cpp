#include "tls_session.h"

#ifdef RMQ_HAS_TLS

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <cerrno>
#include <chrono>
#include <thread>

#include "rocketmq/common/logging.h"

namespace rocketmq {

namespace {

// 握手/读写期间给 socket 挂的 SO_RCVTIMEO（秒 + 微秒 helper）
void setRcvTimeout(netcompat::socket_t sock, int millis) {
    struct timeval tv;
    tv.tv_sec = millis / 1000;
    tv.tv_usec = (millis % 1000) * 1000;
    ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,
                 reinterpret_cast<const char*>(&tv), sizeof(tv));
}

void setSndTimeout(netcompat::socket_t sock, int millis) {
    struct timeval tv;
    tv.tv_sec = millis / 1000;
    tv.tv_usec = (millis % 1000) * 1000;
    ::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO,
                 reinterpret_cast<const char*>(&tv), sizeof(tv));
}

std::string lastOpenSslError() {
    unsigned long code = ERR_get_error();
    if (code == 0) return "no openssl error";
    char buf[256];
    ERR_error_string_n(code, buf, sizeof(buf));
    return std::string(buf);
}

}  // namespace

std::shared_ptr<void> createClientSslContext(std::string& err,
                                             const std::string& certPath,
                                             const std::string& keyPath) {
    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    if (ctx == nullptr) {
        err = "SSL_CTX_new failed: " + lastOpenSslError();
        return nullptr;
    }
    // test mode（对齐 Java tls.test.mode.enable 默认 true）：信任 broker 自签证书。
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    // 客户端证书可选：给了一对就加载（Java tls.client.certPath/keyPath 独立于 test.mode）。
    if (!certPath.empty()) {
        if (SSL_CTX_use_certificate_chain_file(ctx, certPath.c_str()) != 1 ||
            SSL_CTX_use_PrivateKey_file(ctx, keyPath.c_str(), SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_check_private_key(ctx) != 1) {
            err = "load client cert/key failed: " + lastOpenSslError();
            SSL_CTX_free(ctx);
            return nullptr;
        }
    }
    // 老 broker/新 broker 兼容：默认协议集即可（TLS 1.2/1.3），不锁死版本
    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv3);
    return std::shared_ptr<void>(ctx, [](void* p) {
        SSL_CTX_free(static_cast<SSL_CTX*>(p));
    });
}

std::shared_ptr<void> createStrictClientSslContext(const std::string& caPath,
                                                   const std::string& certPath,
                                                   const std::string& keyPath,
                                                   std::string& err) {
    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    if (ctx == nullptr) {
        err = "SSL_CTX_new failed: " + lastOpenSslError();
        return nullptr;
    }
    // 严格校验（对齐 Java tls.test.mode.enable=false）：verify=PEER，主机名/SAN 的逐连接
    // 校验在 handshake() 里做（OpenSSL 不做 SSL_set1_host/X509 param 就不校验名字）。
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    if (SSL_CTX_load_verify_locations(ctx, caPath.c_str(), nullptr) != 1) {
        err = "load CA file failed (" + caPath + "): " + lastOpenSslError();
        SSL_CTX_free(ctx);
        return nullptr;
    }
    if (!certPath.empty()) {
        if (SSL_CTX_use_certificate_chain_file(ctx, certPath.c_str()) != 1 ||
            SSL_CTX_use_PrivateKey_file(ctx, keyPath.c_str(), SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_check_private_key(ctx) != 1) {
            err = "load client cert/key failed: " + lastOpenSslError();
            SSL_CTX_free(ctx);
            return nullptr;
        }
    }
    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv3);
    return std::shared_ptr<void>(ctx, [](void* p) {
        SSL_CTX_free(static_cast<SSL_CTX*>(p));
    });
}

TlsSession::TlsSession(std::shared_ptr<void> ctx, const std::string& expectedName)
    : ctx_(std::move(ctx)), expectedName_(expectedName) {}

TlsSession::~TlsSession() { shutdown(); }

bool TlsSession::handshake(netcompat::socket_t sock, const std::string& host, int timeoutMillis,
                           std::string& err) {
    SSL_CTX* ctx = static_cast<SSL_CTX*>(ctx_.get());
    ssl_ = SSL_new(ctx);
    if (ssl_ == nullptr) {
        err = "SSL_new failed: " + lastOpenSslError();
        return false;
    }
    if (SSL_set_fd(ssl_, static_cast<int>(sock)) != 1) {
        err = "SSL_set_fd failed: " + lastOpenSslError();
        return false;
    }
    if (!host.empty()) {
        // SNI：broker 侧 PERMISSIVE 不校验，但带上是标准客户端行为
        SSL_set_tlsext_host_name(ssl_, host.c_str());
    }
    // 严格模式的主机名/SAN 校验（test-mode 的 ctx verify=NONE 时跳过）。
    // OpenSSL 与 Java 不同：不显式设置校验目标就**只验链不验名字**，所以两条都得自己做——
    // 目标是 IP 字面量时用 X509_VERIFY_PARAM_set1_ip_asc（对 X509 IP SAN），
    // 否则 SSL_set1_host（对 DNS SAN，支持单层 *. 通配，Java HostnameVerifier 同口径）。
    if (SSL_CTX_get_verify_mode(ctx) & SSL_VERIFY_PEER) {
        const std::string& name = expectedName_.empty() ? host : expectedName_;
        if (name.empty()) {
            err = "strict TLS: no hostname to verify against";
            SSL_free(ssl_);
            ssl_ = nullptr;
            return false;
        }
        unsigned char v6[16];
        unsigned char v4[4];
        if (inet_pton(AF_INET, name.c_str(), v4) == 1 ||
            inet_pton(AF_INET6, name.c_str(), v6) == 1) {
            if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl_), name.c_str()) != 1) {
                err = "strict TLS: set IP verify target failed: " + lastOpenSslError();
                SSL_free(ssl_);
                ssl_ = nullptr;
                return false;
            }
        } else if (SSL_set1_host(ssl_, name.c_str()) != 1) {
            err = "strict TLS: set hostname verify target failed: " + lastOpenSslError();
            SSL_free(ssl_);
            ssl_ = nullptr;
            return false;
        }
    }
    // 用 SO_RCVTIMEO/SO_SNDTIMEO 给阻塞握手兜底（超时表现为 WANT_READ/WANT_WRITE）
    setRcvTimeout(sock, timeoutMillis);
    setSndTimeout(sock, timeoutMillis);
    for (;;) {
        int rc = SSL_connect(ssl_);
        if (rc == 1) {
            // 会话级兜底：读 1s / 写 5s。读线程用 select 控节奏，这个超时只防
            // "select 报可读但记录只到了一半"的极端对端停摆把读线程永久挂住。
            setRcvTimeout(sock, 1000);
            setSndTimeout(sock, 5000);
            logger_debug("tls handshake ok with " + host + " (" + SSL_get_version(ssl_) + " "
                         + SSL_get_cipher_name(ssl_) + ")");
            return true;
        }
        int e = SSL_get_error(ssl_, rc);
        if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
            // SO_*TIMEO 到点
            err = "tls handshake timeout with " + host + " (" + std::to_string(timeoutMillis) + "ms)";
            return false;
        }
        err = "tls handshake failed with " + host + ": " + lastOpenSslError();
        return false;
    }
}

int TlsSession::read(netcompat::socket_t sock, char* buf, int len) {
    (void)sock;
    // 整个调用在会话锁内：里面只有 SSL_read 与 EINTR 重试，没有任何睡眠。
    std::lock_guard<std::mutex> lk(ioMutex_);
    for (;;) {
        int n = SSL_read(ssl_, buf, len);
        if (n > 0) return n;
        int e = SSL_get_error(ssl_, n);
        switch (e) {
            case SSL_ERROR_ZERO_RETURN:
                return 0;  // 对端正常关闭（close_notify）
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                // SO_RCVTIMEO 到点或 want-write：让上层 select 循环重试
                return -2;
            case SSL_ERROR_SYSCALL:
                // EAGAIN/EWOULDBLOCK = SO_RCVTIMEO 到点（某些 OpenSSL 版本归入 SYSCALL）；
                // EINTR 重试；其余为真错误
                if (errno == EAGAIN || errno == EWOULDBLOCK) return -2;
                if (errno == EINTR) continue;
                return -1;
            default:
                return -1;
        }
    }
}

bool TlsSession::writeAll(netcompat::socket_t sock, const char* data, size_t len) {
    (void)sock;  // SSL_write 绑定在握手时的 fd 上；形参仅为签名一致
    size_t sent = 0;
    while (sent < len) {
        int n;
        int err;
        {
            // 锁只圈住这一次 SSL_write（连同读它错误码的 SSL_get_error）：下面那 10ms
            // 重试睡眠必须在锁外，否则读线程（以及同一会话上的其它写者）会被对端满窗口堵住。
            std::lock_guard<std::mutex> lk(ioMutex_);
            n = SSL_write(ssl_, data + sent, static_cast<int>(len - sent));
            err = SSL_get_error(ssl_, n);
        }
        if (n > 0) {
            sent += static_cast<size_t>(n);
            continue;
        }
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
            if (errno == EINTR) continue;
            // SO_SNDTIMEO 到点：短暂等待后重试（对端 TCP 窗口满）
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        return false;
    }
    return true;
}

int TlsSession::pending() {
    std::lock_guard<std::mutex> lk(ioMutex_);
    return SSL_pending(ssl_);
}

void TlsSession::shutdown() {
    std::lock_guard<std::mutex> lk(ioMutex_);
    if (ssl_ != nullptr) {
        // best-effort：对端可能已关，失败不处理
        SSL_shutdown(ssl_);
        SSL_free(ssl_);
        ssl_ = nullptr;
    }
}

}  // namespace rocketmq

#endif  // RMQ_HAS_TLS
