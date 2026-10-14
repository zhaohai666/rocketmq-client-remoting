// Compile-time switch: CMake `RMQ_ENABLE_TLS` (ON by default when OpenSSL is found). Without OpenSSL
// this header provides an **error-raising stub**: setTlsEnable(true) throws an explanatory exception and all other paths are unaffected.
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

std::shared_ptr<void> createClientSslContext(std::string& err,
                                             const std::string& certPath = std::string(),
                                             const std::string& keyPath = std::string());

std::shared_ptr<void> createStrictClientSslContext(const std::string& caPath,
                                                   const std::string& certPath,
                                                   const std::string& keyPath,
                                                   std::string& err);

// TLS session for one connection. Lifetime: handshake() after connect, then read/writeAll
// replace raw recv/send, and it is destroyed on close (best-effort SSL_shutdown internally).
//
// ⚠ OpenSSL does not support two threads using the **same SSL object** concurrently (the maintainer's
// words in openssl/openssl#20622: "You cannot share (most) OpenSSL objects between threads"; openssl-threads(7)
// only promises that "most objects are not safe for simultaneous use"). This transport's shape is
// "one reader thread per connection plus writes from caller threads", so the reader's SSL_pending/SSL_read and the
// caller's SSL_write necessarily overlap on one session, and the connection-level writeMutex (write side only) cannot prevent that.
// The lock therefore lives **inside the session**, taken and released around every SSL_* call in the four entry points:
//   * the granularity is "one SSL_* call" rather than "one business read/write" -- writeAll's repeated SSL_write calls take and release each time,
//     and the 10ms sleep before a retry is outside the lock, so the lock is never held while sleeping;
//   * the reader only enters read when select reports readability (or SSL_pending>0), so under normal conditions the lock is held for about one
//     syscall; only a peer stall with "a record only half received" holds the lock until SO_RCVTIMEO
//     (1s) expires, which is the price this link has to pay anyway.
class TlsSession {
public:
    // expectedName: the hostname/SAN to verify in strict mode (overridden by serverName); empty = use the connection host.
    // Ignored in test mode (ctx verify=NONE).
    explicit TlsSession(std::shared_ptr<void> ctx, const std::string& expectedName = std::string());
    ~TlsSession();
    TlsSession(const TlsSession&) = delete;
    TlsSession& operator=(const TlsSession&) = delete;

    // Blocking handshake; on failure returns false and fills err (including timeouts). host is used as SNI.
    bool handshake(netcompat::socket_t sock, const std::string& host, int timeoutMillis,
                   std::string& err);

    // Read: >0 = byte count; 0 = peer closed cleanly; -2 = no data yet (retry); <0 = error.
    // Returns immediately when the SSL internal buffer is not empty.
    int read(netcompat::socket_t sock, char* buf, int len);

    // Write all bytes; false = peer closed or an error.
    bool writeAll(netcompat::socket_t sock, const char* data, size_t len);

    // Bytes decrypted inside the SSL buffer but not yet delivered (when >0, select can miss readability).
    int pending();

    void shutdown();

private:
    std::shared_ptr<void> ctx_;
    std::string expectedName_;
    SSL* ssl_ = nullptr;
    // Session-level io lock: see the class comment. It wraps only SSL_* calls, never sleeps or waits.
    std::mutex ioMutex_;
};

#else  // !RMQ_HAS_TLS

// Stub when OpenSSL is not compiled in: the handshake always fails with an explicit reason; the other APIs are unreachable.
class TlsSession {
public:
    explicit TlsSession(std::shared_ptr<void> ctx, const std::string& expectedName = std::string()) {
        (void)ctx; (void)expectedName;
    }
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

inline std::shared_ptr<void> createClientSslContext(std::string& err,
                                                    const std::string& certPath = std::string(),
                                                    const std::string& keyPath = std::string()) {
    (void)certPath; (void)keyPath;
    err = "client built without TLS support (install OpenSSL or set RMQ_ENABLE_TLS=ON)";
    return nullptr;
}

inline std::shared_ptr<void> createStrictClientSslContext(const std::string& caPath,
                                                          const std::string& certPath,
                                                          const std::string& keyPath,
                                                          std::string& err) {
    (void)caPath; (void)certPath; (void)keyPath;
    err = "client built without TLS support (install OpenSSL or set RMQ_ENABLE_TLS=ON)";
    return nullptr;
}

#endif  // RMQ_HAS_TLS

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_TLS_SESSION_H
