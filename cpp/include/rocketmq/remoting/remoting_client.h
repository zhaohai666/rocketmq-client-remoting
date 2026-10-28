// Provides: connection management (lazy connect + reuse + TCP_NODELAY), invokeSync / invokeAsync /
// invokeOneway, opaque -> response callback dispatch, timeout control, connection liveness probing.
//
// Threading model (one read thread per connection):
//   - calling thread: encode -> take the write lock -> sendall; then wait on a condition variable inside invokeSync
//   - read thread: select(300ms) -> recv -> frame by totalLength(4) -> decode ->
//             pull the future out of the response table by opaque -> wake the calling thread / fire the callback
//
// Wire format: totalLength(4) | headerLength(4) | header | body
//   totalLength = 4 + headerLength + bodyLength (i.e. every byte after the first 4)
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
        NONE = 0,          // No error (the response arrived, response is valid)
        TIMEOUT,           // Corresponds to RemotingTimeoutException
        SEND_REQUEST,      // Corresponds to RemotingSendRequestException
        CONNECT,           // Corresponds to RemotingConnectException
        TOO_MUCH_REQUEST,  // Corresponds to RemotingTooMuchRequestException (the only transport error that is not retried)
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

    // A connection that has carried neither a read nor a write for this long is closed by the
    // transport's own sweep. A configured value that is not positive falls back to this default:
    // the sweep cannot be switched off.
    static constexpr int64_t DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS = 120 * 1000;

    // Defaults: 3s connect timeout, 15s invoke timeout
    explicit RemotingClient(int32_t connectTimeoutMillis = 3000,
                            int32_t invokeTimeoutMillis = 15000);
    ~RemotingClient();

    RemotingClient(const RemotingClient&) = delete;
    RemotingClient& operator=(const RemotingClient&) = delete;

    // opaque uniqueness is the caller's responsibility — creating the request with RemotingCommand::createRequestCommand()
    // is enough (it allocates from a global counter). The transport does **not** rewrite an opaque==0 as "unset",
    // because the counter starts at 0, so 0 is a legal opaque for the very first request. The transport only
    // reallocates a fresh value and writes it back to request.opaque when opaque collides with an **in-flight** request.
    RemotingCommand invokeSync(const std::string& addr, RemotingCommand& request,
                               int32_t timeoutMillis = -1);

    void invokeAsync(const std::string& addr, RemotingCommand& request,
                     InvokeCallback callback, int32_t timeoutMillis = -1);

    void invokeOneway(const std::string& addr, RemotingCommand& request);

    // ---- handling of broker-initiated requests ----
    // Register/unregister request processors indexed by requestCode.
    //
    // Only commands that are **of request type and not present in the local in-flight response table** are
    // dispatched here (typical case: the transaction check-back CHECK_TRANSACTION_STATE=39 sent by the broker);
    // existing invokeSync / invokeAsync response dispatch is unaffected. The handler runs on the read thread and must be thread safe.
    void registerProcessor(int32_t requestCode, RequestProcessor handler);
    void unregisterProcessor(int32_t requestCode);

    bool registerRPCHook(std::shared_ptr<RPCHook> hook);
    void unregisterRPCHook();

    void setTlsEnable(bool enable);
    bool tlsEnable() const;
    // Set fine-grained TLS options (caCert/clientCert/clientKey/serverName). Must be called before the first
    // connection is established; when caCert is non-empty, setTlsEnable(true) creates a strictly validating SSL_CTX (certificate chain + hostname).
    // If TLS is already enabled, ctx is rebuilt immediately (a change of options must change ctx), and a failure throws RemotingException.
    void setTlsOptions(const TlsOptions& options);

    void setSocksProxy(const SocksProxyConfig& proxy);
    // Whether a proxy is configured (for tests/diagnostics)
    bool socksProxyEnabled() const;

    void setEnableReconnectForGoAway(bool enable);
    bool enableReconnectForGoAway() const;

    // ---- connection management ----
    bool isChannelWritable(const std::string& addr) const;
    void closeChannel(const std::string& addr);

    // ---- idle connection close ----
    // The limit for one connection; a value <= 0 restores DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS
    // because the sweep is always installed once a connection exists.
    void setChannelIdleTimeoutMillis(int64_t millis);
    int64_t channelIdleTimeoutMillis() const;
    // Close every connection idle strictly past the limit and return how many were closed. The
    // transport's own sweep calls it on each tick; it is public so tests and diagnostics can
    // drive one pass deterministically.
    int sweepIdleConnections();

    // ---- sync timeout and the socket it timed out on ----
    // Whether a synchronous invoke that runs out of time also closes the connection it went out
    // on (true by default). Send failures always close the connection regardless of this switch.
    void setCloseSocketOnSyncTimeout(bool close);
    bool closeSocketOnSyncTimeout() const;

    // Reserved: the name server address list is maintained by the upper layer (MQClientInstance)
    void updateNameServerAddressList(const std::vector<std::string>& addrs);

    // Close all connections and reclaim the read threads; may be called repeatedly
    void shutdown();

    int32_t connectTimeoutMillis() const { return connectTimeoutMillis_; }
    int32_t invokeTimeoutMillis() const { return invokeTimeoutMillis_; }

    // Number of currently active connections (for tests/diagnostics)
    size_t connectionCount() const;

    size_t pendingRequestCount() const;

    // Split "host:port", supporting the IPv6 form [::1]:10911
    static void parseAddress(const std::string& addr, std::string& host, std::string& port);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    int32_t connectTimeoutMillis_;
    int32_t invokeTimeoutMillis_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_REMOTING_CLIENT_H
