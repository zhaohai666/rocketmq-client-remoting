#include "tls_session.h"

#ifdef RMQ_HAS_TLS

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <cerrno>
#include <chrono>
#include <thread>
#include <utility>

#include "rocketmq/common/logging.h"

namespace rocketmq {

namespace {

// SO_RCVTIMEO helper applied to the socket during handshake/read-write (seconds + microseconds)
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
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    if (!certPath.empty()) {
        if (SSL_CTX_use_certificate_chain_file(ctx, certPath.c_str()) != 1 ||
            SSL_CTX_use_PrivateKey_file(ctx, keyPath.c_str(), SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_check_private_key(ctx) != 1) {
            err = "load client cert/key failed: " + lastOpenSslError();
            SSL_CTX_free(ctx);
            return nullptr;
        }
    }
    // Old/new broker compatibility: the default protocol set is enough (TLS 1.2/1.3); do not pin a version
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
        // SNI: the broker side does not verify under PERMISSIVE, but sending it is standard client behaviour
        SSL_set_tlsext_host_name(ssl_, host.c_str());
    }
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
    // SO_RCVTIMEO/SO_SNDTIMEO bound the blocking handshake (a timeout surfaces as WANT_READ/WANT_WRITE)
    setRcvTimeout(sock, timeoutMillis);
    setSndTimeout(sock, timeoutMillis);
    for (;;) {
        int rc = SSL_connect(ssl_);
        if (rc == 1) {
            // Session-level fallback: 1s read / 5s write. The reader paces itself with select, so this timeout only guards
            // against a peer stall where select reports readability but only half a record has arrived, which would hang the reader forever.
            setRcvTimeout(sock, 1000);
            setSndTimeout(sock, 5000);
            logger_debug("tls handshake ok with " + host + " (" + SSL_get_version(ssl_) + " "
                         + SSL_get_cipher_name(ssl_) + ")");
            return true;
        }
        int e = SSL_get_error(ssl_, rc);
        if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
            // SO_*TIMEO expired
            err = "tls handshake timeout with " + host + " (" + std::to_string(timeoutMillis) + "ms)";
            return false;
        }
        err = "tls handshake failed with " + host + ": " + lastOpenSslError();
        return false;
    }
}

int TlsSession::read(netcompat::socket_t sock, char* buf, int len) {
    (void)sock;
    // The whole call runs under the session lock: it only contains SSL_read and EINTR retries, no sleeping.
    std::lock_guard<std::mutex> lk(ioMutex_);
    for (;;) {
        int n = SSL_read(ssl_, buf, len);
        if (n > 0) return n;
        int e = SSL_get_error(ssl_, n);
        switch (e) {
            case SSL_ERROR_ZERO_RETURN:
                return 0;  // peer closed cleanly (close_notify)
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                // SO_RCVTIMEO expired or want-write: let the upper select loop retry
                return -2;
            case SSL_ERROR_SYSCALL:
                // EAGAIN/EWOULDBLOCK = SO_RCVTIMEO expired (some OpenSSL versions report it as SYSCALL);
                // EINTR retries; anything else is a real error
                if (errno == EAGAIN || errno == EWOULDBLOCK) return -2;
                if (errno == EINTR) continue;
                return -1;
            default:
                return -1;
        }
    }
}

bool TlsSession::writeAll(netcompat::socket_t sock, const char* data, size_t len) {
    (void)sock;  // SSL_write is bound to the fd used at handshake; the parameter only keeps the signature uniform
    size_t sent = 0;
    while (sent < len) {
        int n;
        int err;
        {
            // The lock covers only this SSL_write (plus the SSL_get_error that reads its result): the 10ms
            // retry sleep below must stay outside it, otherwise the reader (and other writers on the same session) would be blocked by a full peer window.
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
            // SO_SNDTIMEO expired: retry after a short wait (the peer's TCP window is full)
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
        // best-effort: the peer may already have closed; failures are ignored
        SSL_shutdown(ssl_);
        SSL_free(ssl_);
        ssl_ = nullptr;
    }
}

}  // namespace rocketmq

#endif  // RMQ_HAS_TLS
