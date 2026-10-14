// Real clusters cannot exercise these branches: a real broker does not reliably return FLUSH_DISK_TIMEOUT / SYSTEM_ERROR,
// and a broker address in the route does not just happen to be unreachable. So the results here are driven by scripted
// response codes, while the code path taken is the real DefaultMQProducer::send over a real socket.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/rpchook.h"

using namespace rocketmq;

namespace {

using netcompat::kInvalidSocket;
using netcompat::socket_t;
using netcompat::socklen_type;

#ifdef _WIN32
constexpr int kShutdownHow = SD_BOTH;
#else
constexpr int kShutdownHow = SHUT_RDWR;
#endif

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

void expectInt(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
}

// ---------------------------------------------------------------- socket helpers

bool readN(socket_t s, char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
}

bool writeAll(socket_t s, const Bytes& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        int n = static_cast<int>(::send(s, reinterpret_cast<const char*>(data.data() + sent),
                                        static_cast<int>(data.size() - sent),
                                        netcompat::sendFlags()));
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// select until readable (one round every 100ms so the thread exits quickly once alive turns false), then read one whole frame
bool readFrame(socket_t s, const std::atomic<bool>& alive, Bytes& out) {
    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(s, &rfds);
        timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        int ready = ::select(netcompat::selectNfds(s), &rfds, nullptr, nullptr, &tv);
        if (!alive.load()) return false;
        if (ready == 0) continue;
        if (ready < 0) return false;
        break;
    }
    char lenBuf[4];
    if (!readN(s, lenBuf, 4)) return false;
    Bytes lenStr(lenBuf, 4);
    const int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
    if (totalLen <= 0 || totalLen > 8 * 1024 * 1024) return false;
    Bytes body(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, reinterpret_cast<char*>(body.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + body;
    return true;
}

// a loopback port that was bound and then released immediately: connecting to it is always refused (more reliable than a fixed port)
std::string deadAddress() {
    netcompat::ensureInitialized();
    socket_t s = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(s, 1);
    socklen_type len = sizeof(addr);
    ::getsockname(s, reinterpret_cast<sockaddr*>(&addr), &len);
    const int port = ntohs(addr.sin_port);
    netcompat::closeSocket(s);
    return "127.0.0.1:" + std::to_string(port);
}

// an accepted connection: held by a reference count, and whoever releases it last closes it (otherwise a slow reply would write
// onto an already closed handle, and Windows would also recycle the handle number for another connection).
struct Conn {
    explicit Conn(socket_t s) : sock(s) {}
    socket_t sock;
    std::mutex writeM;
    std::atomic<int> refs{1};
    std::atomic<bool> closing{false};
};

using ConnRef = std::shared_ptr<Conn>;

void releaseConn(const ConnRef& c) {
    if (c->refs.fetch_sub(1) == 1 && c->closing.load()) {
        ::shutdown(c->sock, kShutdownHow);
        netcompat::closeSocket(c->sock);
    }
}

// ---------------------------------------------------------------- mock endpoint

// the evidence captured for one outbound request: request code (V1/V2, route, heartbeat...), extFields and the raw body
struct WireRecord {
    int32_t code = 0;
    PropertyMap ext;
    Bytes body;
    bool hasBody = false;
};

// cap on captured outbound requests: the lite consumer's pull loop keeps sending them, a few hundred records are enough to tell whether a tag was set
constexpr size_t kRequestLogCap = 500;

// the scripted response for one SEND request
struct SendReply {
    int32_t code = ResponseCode::SUCCESS;
    int delayMillis = 0;
};

// plays both the name server (answers with a route) and the broker (answers SEND with a result). Every broker in the route points at
// this endpoint, so switching to another broker on retry only needs the SEND count to be judged.
class MockEndpoint {
public:
    MockEndpoint() {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listen_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_, 16);
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~MockEndpoint() {
        running_.store(false);
        if (acceptor_.joinable()) acceptor_.join();
        // the accept thread and the reply thread both stop within 100ms / the scripted delay, so the sockets must not be closed before join
        for (;;) {
            std::vector<std::thread> pending;
            {
                std::lock_guard<std::mutex> lk(workersM_);
                pending.swap(workers_);
            }
            if (pending.empty()) break;
            for (auto& t : pending) {
                if (t.joinable()) t.join();
            }
        }
        netcompat::closeSocket(listen_);
    }

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    // topic -> route; an unregistered topic always answers TOPIC_NOT_EXIST (same as a real name server)
    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    void scriptSend(std::vector<SendReply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        sends_ = std::move(replies);
        sendLog_.clear();
        sendCount_.store(0);
    }
    int sendCount() const { return sendCount_.load(); }

    // evidence for the i-th SEND; out of range returns an empty record (the caller treats it as "there is no such request").
    WireRecord sendRecord(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= sendLog_.size()) return WireRecord{};
        return sendLog_[i];
    }

    // clear the evidence of all outbound requests; the hook cases only care about their own slice of traffic.
    void clearRequests() {
        std::lock_guard<std::mutex> lk(state_);
        requestLog_.clear();
    }

    // snapshot of the recorded outbound requests (GET_ROUTEINFO / HEARTBEAT / PULL besides SEND are all in it).
    std::vector<WireRecord> requests() {
        std::lock_guard<std::mutex> lk(state_);
        return requestLog_;
    }

    // among the records with request code `code`, how many carry an extension field key==value.
    int countRequestsWith(int32_t code, const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const WireRecord& r : requestLog_) {
            if (r.code != code) continue;
            auto it = r.ext.find(key);
            if (it != r.ext.end() && it->second == value) ++n;
        }
        return n;
    }

    // total number of records with request code `code`.
    int countRequests(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const WireRecord& r : requestLog_) {
            if (r.code == code) ++n;
        }
        return n;
    }

private:
    void acceptLoop() {
        while (running_.load()) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listen_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            int ready = ::select(netcompat::selectNfds(listen_), &rfds, nullptr, nullptr, &tv);
            if (ready == 0) continue;
            if (ready < 0) break;
            socket_t sock = ::accept(listen_, nullptr, nullptr);
            if (sock == kInvalidSocket) break;
            ConnRef conn = std::make_shared<Conn>(sock);
            std::lock_guard<std::mutex> lk(workersM_);
            workers_.emplace_back([this, conn]() { serveConn(conn); });
        }
    }

    void serveConn(const ConnRef& conn) {
        while (running_.load()) {
            Bytes frame;
            if (!readFrame(conn->sock, running_, frame)) break;
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            // every request is answered on its own thread: a real broker handles requests concurrently, a slow one must not block the ones behind it
            conn->refs.fetch_add(1);
            {
                std::lock_guard<std::mutex> lk(workersM_);
                workers_.emplace_back([this, conn, req]() {
                    respond(conn, req);
                    releaseConn(conn);
                });
            }
        }
        conn->closing.store(true);
        releaseConn(conn);
    }

    void respond(const ConnRef& conn, RemotingCommand req) {
        // keep a piece of evidence for every outbound request: the extension fields injected by hooks (ReqT / AccessKey / Signature)
        // are only written into extFields right before the client encodes, so local assertions cannot see them; only here can they be captured.
        {
            std::lock_guard<std::mutex> lk(state_);
            if (requestLog_.size() < kRequestLogCap) {
                requestLog_.push_back({req.code, req.extFields, req.body, req.hasBody});
            }
        }
        RemotingCommand resp;
        resp.opaque = req.opaque;
        resp.markResponseType();

        if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
            const std::string topic = req.extFields.count("topic") ? req.extFields["topic"]
                                                                   : std::string();
            Bytes body;
            bool found = false;
            {
                std::lock_guard<std::mutex> lk(state_);
                auto it = routes_.find(topic);
                if (it != routes_.end()) {
                    body = it->second;
                    found = true;
                }
            }
            resp.code = found ? ResponseCode::SUCCESS : ResponseCode::TOPIC_NOT_EXIST;
            resp.remark = found ? "route ok" : "topic not exist";
            resp.hasRemark = true;
            if (found) {
                resp.body = body;
                resp.hasBody = true;
            }
        } else if (req.code == RequestCode::SEND_MESSAGE_V2
                   || req.code == RequestCode::SEND_MESSAGE
                   || req.code == RequestCode::SEND_BATCH_MESSAGE
                   || req.code == RequestCode::SEND_REPLY_MESSAGE_V2) {
            const int idx = sendCount_.fetch_add(1);
            {
                // record the outbound request code and extFields: the field semantics outside retry classification (unitMode/batch)
                // can only be captured here.
                std::lock_guard<std::mutex> lk(state_);
                sendLog_.push_back({req.code, req.extFields, req.body, req.hasBody});
            }

            SendReply reply;
            {
                std::lock_guard<std::mutex> lk(state_);
                if (idx < static_cast<int>(sends_.size())) reply = sends_[idx];
            }
            // sleep in slices: so the thread can be stopped quickly when destructing
            for (int i = 0; i < reply.delayMillis / 50 && running_.load(); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            resp.code = reply.code;
            resp.remark = "mock send";
            resp.hasRemark = true;
            if (reply.code == ResponseCode::SUCCESS) {
                resp.extFields["msgId"] = "AC10000100001234567890ABCDEF0001";
                resp.extFields["queueId"] = "0";
                resp.extFields["queueOffset"] = std::to_string(1000 + idx);
            }
        } else {
            // HEARTBEAT and the like: always succeed; oneway requests get no response (a broker does the same)
            if (req.isOnewayRpc()) return;
            resp.code = ResponseCode::SUCCESS;
        }
        const Bytes out = resp.encode();
        std::lock_guard<std::mutex> lk(conn->writeM);
        writeAll(conn->sock, out);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::atomic<int> sendCount_{0};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<SendReply> sends_;
    std::vector<WireRecord> sendLog_;
    std::vector<WireRecord> requestLog_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// build a route of `brokers` brokers with `queuesPerBroker` queues each (every address points at the same endpoint)
TopicRouteData makeRoute(const std::string& brokerAddr, int brokers, int queuesPerBroker) {
    TopicRouteData route;
    for (int b = 0; b < brokers; ++b) {
        const std::string name = "broker-" + std::string(1, static_cast<char>('a' + b));
        route.queueDatas.emplace_back(name, queuesPerBroker, queuesPerBroker, 6, 0);
        std::map<int64_t, std::string> addrs;
        addrs[0] = brokerAddr;
        route.brokerDatas.emplace_back("DefaultCluster", name, addrs);
    }
    return route;
}

Message plainMessage(const std::string& topic) {
    Message m(topic, Bytes{'h', 'i'});
    m.setTags("T1");
    return m;
}

// ---------------------------------------------------------------- cases

// 1. a retryable code switches broker; a non-retryable code throws immediately and sends only once
void testRetryResponseCodes(MockEndpoint& mock) {
    const std::string topic = "SendRetryTopicExist";
    mock.addRoute(topic, makeRoute(mock.address(), 2, 1));

    {
        mock.scriptSend({{ResponseCode::SYSTEM_ERROR, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_retry_ok");
        p.setNamesrvAddr(mock.address());
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "retryable code then success -> SEND_OK");
        expectInt(mock.sendCount(), 2, "retryable broker code retried once");
        expectInt(static_cast<long long>(r.queueOffset), 1001,
                  "the retried send is the one whose result comes back");
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::MESSAGE_ILLEGAL, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_retry_no");
        p.setNamesrvAddr(mock.address());
        p.start();
        int threw = 0;
        try {
            p.send(plainMessage(topic), 3000);
        } catch (const MQBrokerException& e) {
            threw = 1;
            expectInt(e.getResponseCode(), ResponseCode::MESSAGE_ILLEGAL,
                      "non-retryable code keeps the original MQBrokerException");
        } catch (const std::exception& e) {
            std::printf("  wrong exception: %s\n", e.what());
        }
        expectInt(threw, 1, "MESSAGE_ILLEGAL is not in retryResponseCodes -> throws");
        expectInt(mock.sendCount(), 1, "non-retryable code must not retry");
        p.shutdown();
    }
    {
        // addRetryResponseCode can turn a non-retryable code into a retryable one
        mock.scriptSend({{ResponseCode::MESSAGE_ILLEGAL, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_retry_added");
        p.setNamesrvAddr(mock.address());
        p.addRetryResponseCode(ResponseCode::MESSAGE_ILLEGAL);
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "addRetryResponseCode makes it retryable");
        expectInt(mock.sendCount(), 2, "addRetryResponseCode retried");
        p.shutdown();
    }
}

// 2/3. the storeOK retry switch
void testRetryAnotherBrokerWhenNotStoreOK(MockEndpoint& mock) {
    const std::string topic = "SendRetryStoreOk";
    mock.addRoute(topic, makeRoute(mock.address(), 2, 1));

    {
        mock.scriptSend({{ResponseCode::FLUSH_DISK_TIMEOUT, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_storeok_off");
        p.setNamesrvAddr(mock.address());
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::FLUSH_DISK_TIMEOUT),
                  "default: FLUSH_DISK_TIMEOUT returned as-is");
        expectInt(mock.sendCount(), 1, "default: no broker switch on not-store-OK");
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::FLUSH_DISK_TIMEOUT, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_storeok_on");
        p.setNamesrvAddr(mock.address());
        p.setRetryAnotherBrokerWhenNotStoreOK(true);
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "enabled: switch broker and get SEND_OK");
        expectInt(mock.sendCount(), 2, "enabled: retried on another broker");
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::FLUSH_SLAVE_TIMEOUT, 0},
                         {ResponseCode::SLAVE_NOT_AVAILABLE, 0},
                         {ResponseCode::FLUSH_DISK_TIMEOUT, 0}});
        DefaultMQProducer p("PG_storeok_last");
        p.setNamesrvAddr(mock.address());
        p.setRetryAnotherBrokerWhenNotStoreOK(true);
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::FLUSH_DISK_TIMEOUT),
                  "exhausted retries still return the last non-OK result");
        expectInt(mock.sendCount(), 3, "storeOK retry honours retryTimesWhenSendFailed");
        p.shutdown();
    }
}

// 4. sendMsgMaxTimeoutPerRequest: a slow broker must give up at the shortened per-request timeout
void testSendMsgMaxTimeoutPerRequest(MockEndpoint& mock) {
    const std::string topic = "SendRetryCap";
    mock.addRoute(topic, makeRoute(mock.address(), 2, 1));

    {
        // without the cap: the first request does get to wait for the 400ms response, so only one send
        mock.scriptSend({{ResponseCode::SUCCESS, 400}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_cap_off");
        p.setNamesrvAddr(mock.address());
        p.start();
        p.send(plainMessage(topic), 3000);
        expectInt(mock.sendCount(), 1, "no cap: the slow first broker still answers");
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::SUCCESS, 400}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_cap_on");
        p.setNamesrvAddr(mock.address());
        p.setSendMsgMaxTimeoutPerRequest(150);
        p.start();
        const auto begin = std::chrono::steady_clock::now();
        SendResult r = p.send(plainMessage(topic), 3000);
        const long long elapsedMs = static_cast<long long>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - begin)
                .count());
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "cap: retry still ends up SEND_OK");
        expectInt(mock.sendCount(), 2, "cap: first attempt abandoned at 150ms, then retried");
        expect(elapsedMs < 1000, "cap: total cost stays well below the 3000ms budget",
               "elapsed=" + std::to_string(elapsedMs));
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::SUCCESS, 300}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_cap_big");
        p.setNamesrvAddr(mock.address());
        p.setRetryTimesWhenSendFailed(1);
        p.setSendMsgMaxTimeoutPerRequest(10000);
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "cap larger than the budget is a no-op");
        expectInt(mock.sendCount(), 1, "no abandonment when the cap exceeds the timeout");
        p.shutdown();
    }
}

class OneShotSlowHook : public SendMessageHook {
public:
    explicit OneShotSlowHook(int32_t sleepMillis) : sleepMillis_(sleepMillis) {}
    std::string hookName() const override { return "OneShotSlowHook"; }
    void sendMessageBefore(SendMessageContext&) override {
        if (fired_.exchange(true)) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMillis_));
    }
    void sendMessageAfter(SendMessageContext&) override {}

private:
    std::atomic<bool> fired_{false};
    int32_t sleepMillis_;
};

// 5. the total timeout runs out -> RemotingTooMuchRequestException
void testCallTimeout(MockEndpoint& mock) {
    const std::string topic = "SendRetryCallTimeout";
    mock.addRoute(topic, makeRoute(mock.address(), 2, 1));
    // First attempt: the hook sleeps 300ms (> the whole 200ms budget), the broker returns a retryable code -> keep looping;
    // second attempt: costTime(300) > timeout(200) -> give up right away (callTimeout).
    mock.scriptSend({{ResponseCode::SYSTEM_ERROR, 0}, {ResponseCode::SUCCESS, 0}});
    DefaultMQProducer p("PG_call_timeout");
    p.setNamesrvAddr(mock.address());
    p.registerSendMessageHook(std::make_shared<OneShotSlowHook>(300));
    p.start();
    const int before = mock.sendCount();
    bool tooMuch = false;
    try {
        p.send(plainMessage(topic), 200);
    } catch (const RemotingTooMuchRequestException&) {
        tooMuch = true;
    } catch (const std::exception& e) {
        std::printf("  wrong exception: %s\n", e.what());
    }
    expect(tooMuch, "budget exhausted -> RemotingTooMuchRequestException");
    expectInt(mock.sendCount() - before, 1, "no further broker is sent to after callTimeout");
    p.shutdown();
}

// 6. failure cause -> ClientErrorCode
void testErrorCodeMapping(MockEndpoint& mock) {
    const std::string deadTopic = "SendRetryDeadBroker";
    mock.addRoute(deadTopic, makeRoute(deadAddress(), 1, 1));
    {
        // on Windows, connecting to a just-closed loopback port takes ~1s and three attempts would burn the whole budget,
        // so only one attempt is run with a generous budget, to make sure what is seen is "cannot connect", not "total timeout".
        DefaultMQProducer p("PG_connect");
        p.setNamesrvAddr(mock.address());
        p.setRetryTimesWhenSendFailed(0);
        p.start();
        int code = 0;
        try {
            p.send(plainMessage(deadTopic), 8000);
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
        } catch (const std::exception& e) {
            std::printf("  wrong exception: %s\n", e.what());
        }
        expectInt(code, ClientErrorCode::CONNECT_BROKER_EXCEPTION,
                  "connect failure -> CONNECT_BROKER_EXCEPTION(10001)");
        p.shutdown();
    }
    {
        // no route: immediate NOT_FOUND_TOPIC, without spinning away the retry count
        DefaultMQProducer p("PG_noroute");
        p.setNamesrvAddr(mock.address());
        p.start();
        const int before = mock.sendCount();
        int code = 0;
        try {
            p.send(plainMessage("SendRetryNeverRegistered"), 1000);
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
        }
        expectInt(code, ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
                  "no route at all -> NOT_FOUND_TOPIC_EXCEPTION(10005)");
        expectInt(mock.sendCount() - before, 0, "no route -> never reaches the broker");
        p.shutdown();
    }
    {
        // the broker explicitly answers a non-retryable code: the original code is passed through
        const std::string topic = "SendRetryBrokerCode";
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        mock.scriptSend({{ResponseCode::NOT_LEADER_FOR_QUEUE, 0}});
        DefaultMQProducer p("PG_broker_code");
        p.setNamesrvAddr(mock.address());
        p.start();
        int code = 0;
        try {
            p.send(plainMessage(topic), 1000);
        } catch (const MQBrokerException& e) {
            code = e.getResponseCode();
        }
        expectInt(code, ResponseCode::NOT_LEADER_FOR_QUEUE,
                  "non-retryable MQBrokerException propagates its own code");
        p.shutdown();
    }
}

// 6b. an addressing failure (10004) must not be reported as "this topic has no route" (10005)
namespace {
struct RouteFunnelProbe : public DefaultMQProducer {
    RouteFunnelProbe() : DefaultMQProducer("PG_funnel_probe") {}
    using DefaultMQProducer::topicPublishInfo;
    using DefaultMQProducer::validateNameServerSetting;
};
}  // namespace

void testNoNameServerAddressQualifies10004(MockEndpoint& mock) {
    RouteFunnelProbe probe;
    {
        MQClientInstance empty("PG_funnel_probe_client", std::vector<std::string>{});
        int code = 0;
        std::string msg;
        try {
            probe.topicPublishInfo(empty, "SendRetryNoNamesrv");
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
            msg = e.what();
        } catch (const std::exception& e) {
            msg = e.what();
        }
        expectInt(code, ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
                  "empty name server list -> NO_NAME_SERVER_EXCEPTION(10004)");
        expect(msg == "No name server address, please set it.",
               "10004 keeps the reference wording", "msg=" + msg);
    }
    {
        // the address is there and only this topic has no route -> the 10004 gate must stay silent and the original exception is thrown
        // (the caller then classifies it as 10005, see the producer leg below)
        MQClientInstance hasAddr("PG_funnel_probe_dead",
                                 std::vector<std::string>{deadAddress()});
        int code = 0;
        std::string msg;
        try {
            probe.topicPublishInfo(hasAddr, "SendRetryNoRoute");
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
            msg = e.what();
        } catch (const std::exception& e) {
            msg = e.what();
        }
        expect(code != ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
               "configured (even unreachable) name server never reports 10004",
               "code=" + std::to_string(code) + " msg=" + msg);
        expect(msg.find("Can not find Message Queue for topic") == 0,
               "no route keeps its own message", "msg=" + msg);
    }
    {
        DefaultMQProducer p("PG_dead_namesrv");
        p.setNamesrvAddr(deadAddress());
        p.setRetryTimesWhenSendFailed(0);
        p.start();
        int code = 0;
        try {
            p.send(plainMessage("SendRetryDeadNamesrv"), 3000);
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
        } catch (const std::exception& e) {
            std::printf("  wrong exception: %s\n", e.what());
        }
        expectInt(code, ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
                  "dead-but-configured name server -> 10005, not 10004");
        p.shutdown();
    }
    {
        DefaultMQProducer p("PG_no_namesrv");
        p.setNamesrvAddr("");
        int code = 0;
        try {
            p.start();
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
        }
        expectInt(code, ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
                  "start() without any name server address -> 10004");
        expect(!p.isStarted(), "failed start() leaves the producer unstarted");
    }
    (void)mock;
}

void testFaultItemFlags(MockEndpoint& mock) {
    const std::string deadTopic = "SendFaultConnect";
    mock.addRoute(deadTopic, makeRoute(deadAddress(), 1, 1));
    {
        DefaultMQProducer p("PG_fault_connect");
        p.setNamesrvAddr(mock.address());
        p.setSendLatencyFaultEnable(true);
        p.setRetryTimesWhenSendFailed(0);
        p.start();
        try {
            p.send(plainMessage(deadTopic), 8000);
        } catch (const std::exception&) {
        }
        const FaultItem* item =
            p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr, "transport failure records a fault item");
        if (item != nullptr) {
            expect(item->isReachable(),
                   "RemotingException isolates but keeps reachable (no detector thread here)");
            expect(!item->isAvailable(), "RemotingException isolates the broker");
        }
        p.shutdown();
    }
    {
        const std::string topic = "SendFaultBrokerCode";
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        mock.scriptSend({{ResponseCode::SERVICE_NOT_AVAILABLE, 0},
                         {ResponseCode::SERVICE_NOT_AVAILABLE, 0},
                         {ResponseCode::SERVICE_NOT_AVAILABLE, 0}});
        DefaultMQProducer p("PG_fault_broker");
        p.setNamesrvAddr(mock.address());
        p.setSendLatencyFaultEnable(true);
        p.start();
        int code = 0;
        try {
            p.send(plainMessage(topic), 3000);
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
        }
        expectInt(code, ResponseCode::SERVICE_NOT_AVAILABLE,
                  "exhausted retryable broker codes surface the last broker code");
        const FaultItem* item =
            p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr, "broker failure records a fault item");
        if (item != nullptr) {
            expect(!item->isReachable(), "MQBrokerException marks the broker unreachable");
            expect(!item->isAvailable(), "MQBrokerException isolates the broker");
        }
        p.shutdown();
    }
    {
        // a successful send records the measured latency and is not isolated
        const std::string topic = "SendFaultSuccess";
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_fault_ok");
        p.setNamesrvAddr(mock.address());
        p.setSendLatencyFaultEnable(true);
        p.start();
        p.send(plainMessage(topic), 3000);
        const FaultItem* item =
            p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr && item->isAvailable() && item->isReachable(),
               "successful send records latency without isolating");
        // regression guard: a local round trip is often under 1ms, an integer millisecond difference records the latency as 0,
        // which would make the fault-tolerance table's latency thresholds never take effect.
        if (item != nullptr) {
            expect(item->currentLatency() > 0.0,
                   "success records a strictly positive latency",
                   "latency=" + std::to_string(item->currentLatency()));
        }
        p.shutdown();
    }
}

void testUnitModeReachesWire(MockEndpoint& mock) {
    const std::string topic = "SendRetryUnitMode";
    mock.addRoute(topic, makeRoute(mock.address(), 1, 1));

    {
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_unit_mode_on");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("unit-mode-on");
        p.setUnitMode(true);
        p.start();
        p.send(plainMessage(topic), 3000);
        const WireRecord rec = mock.sendRecord(0);
        expectInt(rec.code, RequestCode::SEND_MESSAGE_V2, "send goes out as SEND_MESSAGE_V2");
        expect(rec.ext.count("k") == 1, "unitMode is on the wire as V2 key k");
        expect(rec.ext.count("k") > 0 && rec.ext.at("k") == "true", "unitMode=true -> k=true");
        expect(rec.ext.count("a") > 0 && rec.ext.at("a") == "PG_unit_mode_on",
               "producerGroup is V2 key a in the same header");
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_unit_mode_off");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("unit-mode-off");
        p.start();
        p.send(plainMessage(topic), 3000);
        const WireRecord rec = mock.sendRecord(0);
        expect(rec.ext.count("k") > 0 && rec.ext.at("k") == "false", "unitMode=false -> k=false");
        p.shutdown();
    }
}

// 8a2. the three fields of the send header that follow the route/config: n (brokerName), c (defaultTopic),
// d (defaultTopicQueueNums).
void testSendHeaderFieldsReachWire(MockEndpoint& mock) {
    const std::string topic = "SendHeaderFields";
    mock.addRoute(topic, makeRoute(mock.address(), 1, 1));

    {
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_hdr_default");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("hdr-default");
        p.start();
        p.send(plainMessage(topic), 3000);
        const WireRecord rec = mock.sendRecord(0);
        expectInt(rec.code, RequestCode::SEND_MESSAGE_V2, "single send goes out as 310");
        expect(rec.ext.count("n") == 1 && rec.ext.at("n") == "broker-a",
               "brokerName is on the wire as V2 key n");
        expect(rec.ext.count("c") == 1 && rec.ext.at("c") == MixAll::DEFAULT_TOPIC,
               "defaultTopic defaults to TBW102");
        expect(rec.ext.count("d") == 1 && rec.ext.at("d") == "4",
               "defaultTopicQueueNums defaults to 4");
        p.shutdown();
    }
    {
        // the producer configuration must really go on the wire (the old code hardcoded it, making the setter a no-op)
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_hdr_configured");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("hdr-configured");
        p.setCreateTopicKey("CreatedTopicKey");
        p.setDefaultTopicQueueNums(9);
        p.start();
        p.send(plainMessage(topic), 3000);
        const WireRecord rec = mock.sendRecord(0);
        expect(rec.ext.count("c") == 1 && rec.ext.at("c") == "CreatedTopicKey",
               "setCreateTopicKey reaches the wire as c");
        expect(rec.ext.count("d") == 1 && rec.ext.at("d") == "9",
               "setDefaultTopicQueueNums reaches the wire as d");
        p.shutdown();
    }
    {
        // targeted send and batch share the same header-building code: n/c/d must not be dropped
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_hdr_pinned");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("hdr-pinned");
        p.setCreateTopicKey("CreatedTopicKey");
        p.start();
        p.send(plainMessage(topic), MessageQueue(topic, "broker-a", 0), 3000);
        const WireRecord pinned = mock.sendRecord(0);
        expect(pinned.ext.count("n") == 1 && pinned.ext.at("n") == "broker-a",
               "a pinned send still names its broker");
        expect(pinned.ext.count("c") == 1 && pinned.ext.at("c") == "CreatedTopicKey",
               "a pinned send carries the producer's createTopicKey");

        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        p.sendBatch({plainMessage(topic), plainMessage(topic)}, 3000);
        const WireRecord batch = mock.sendRecord(0);
        expectInt(batch.code, RequestCode::SEND_BATCH_MESSAGE, "batch goes out as 320");
        expect(batch.ext.count("n") == 1 && batch.ext.at("n") == "broker-a",
               "the batch request names its broker too");
        expect(batch.ext.count("c") == 1 && batch.ext.at("c") == "CreatedTopicKey",
               "and the batch request keeps c/d from the producer");
        p.shutdown();
    }
}

// The request code and the V2 header's single-letter key `m` (batch) are two different things: the broker uses `m` to pick
// batch writing versus a single-message write (it reads requestHeader.isBatch()), while the code only affects how the
// server classifies the request by code (the proxy request handling and the authorization context builder both
// list 310/320 in the same case, since they carry the same send semantics).
// So both are captured: aligning only the code but not `m` would have a batch body parsed as a single message.
void testSendRequestCodeMatchesProtocol(MockEndpoint& mock) {
    const std::string topic = "SendRetryCodeBranch";
    mock.addRoute(topic, makeRoute(mock.address(), 1, 1));

    {
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_code_single");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("code-single");
        p.start();
        p.send(plainMessage(topic), 3000);
        const WireRecord rec = mock.sendRecord(0);
        expectInt(rec.code, RequestCode::SEND_MESSAGE_V2, "single send goes out as 310");
        expect(rec.ext.count("m") > 0 && rec.ext.at("m") == "false", "single -> batch key m=false");
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_code_batch");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("code-batch");
        p.start();
        p.sendBatch({plainMessage(topic), plainMessage(topic)}, 3000);
        const WireRecord rec = mock.sendRecord(0);
        expectInt(rec.code, RequestCode::SEND_BATCH_MESSAGE, "batch send goes out as 320");
        expect(rec.ext.count("m") > 0 && rec.ext.at("m") == "true", "batch -> batch key m=true");
        p.shutdown();
    }
    {
        // reply is decided before batch: a batch carrying MSG_TYPE=reply still goes out as 325.
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_code_reply");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("code-reply");
        p.start();
        MessageBatch batch = MessageBatch::generateFromList({plainMessage(topic)});
        batch.properties[MessageConst::PROPERTY_MESSAGE_TYPE] = MixAll::REPLY_MESSAGE_FLAG;
        p.send(batch, 3000);
        const WireRecord rec = mock.sendRecord(0);
        expectInt(rec.code, RequestCode::SEND_REPLY_MESSAGE_V2, "reply wins over batch: 325");
        expect(rec.ext.count("m") > 0 && rec.ext.at("m") == "true",
               "reply batch still carries batch key m=true");
        p.shutdown();
    }
}

// 9. evidence of what the request hooks put ON THE WIRE. A hook rewrites extFields at the last moment before encoding, so
// local assertions cannot see the result; and the lite consumer once registered user hooks around composeRequestHooks, letting `ReqT`
// go out silently (test_acl pins the hook's own composition and order, not whether the facade actually uses it).
// So the extension fields are counted here, from the socket side.
void testRequestHooksReachWire(MockEndpoint& mock) {
    const std::string topic = "SendRetryReqT";
    mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
    const std::string ak = "AK-wire-probe";
    const std::string sk = "SK-wire-probe";

    {
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        mock.clearRequests();
        DefaultMQProducer p("PG_reqt_off");
        p.setNamesrvAddr(mock.address());
        p.setCredentials(ak, sk);
        p.setInstanceName("reqt-off");
        p.start();
        p.send(plainMessage(topic), 3000);
        p.shutdown();
        const WireRecord rec = mock.sendRecord(0);
        expect(rec.ext.count(SessionCredentials::ACCESS_KEY) == 1, "acl: AccessKey on the wire");
        expect(rec.ext.count(SessionCredentials::SIGNATURE) == 1, "acl: Signature on the wire");
        expect(rec.ext.count(std::string(MixAll::REQ_T)) == 0, "stream off -> no ReqT");
    }
    {
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        mock.clearRequests();
        DefaultMQProducer p("PG_reqt_on");
        p.setNamesrvAddr(mock.address());
        p.setCredentials(ak, sk);
        p.setEnableStreamRequestType(true);
        p.setInstanceName("reqt-on");
        p.start();
        p.send(plainMessage(topic), 3000);
        p.shutdown();
        const WireRecord rec = mock.sendRecord(0);
        // the value is RequestType.STREAM's code ("0"), not the enumerator name
        const auto reqt = rec.ext.find(std::string(MixAll::REQ_T));
        expect(reqt != rec.ext.end() && reqt->second == "0", "stream on -> ReqT=0 on the wire");
        const auto sig = rec.ext.find(std::string(SessionCredentials::SIGNATURE));
        expect(sig != rec.ext.end(), "stream on keeps the ACL signature");
        RemotingCommand replay;
        replay.code = rec.code;
        replay.extFields = rec.ext;
        replay.body = rec.body;
        replay.hasBody = rec.hasBody;
        const bool matches =
            sig != rec.ext.end() && AclClientRPCHook::calcSignature(sk, replay) == sig->second;
        expect(matches, "ReqT sits inside the signed content (broker-side replay matches)");
    }
    {
        mock.clearRequests();
        DefaultLitePullConsumer c("PG_lite_reqt");
        c.setNamesrvAddr(mock.address());
        c.setInstanceName("lite-reqt");
        c.subscribe(topic, "*");
        c.start();
        c.shutdown();
        const int routeTotal = mock.countRequests(RequestCode::GET_ROUTEINFO_BY_TOPIC);
        const int routeTagged =
            mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, MixAll::REQ_T, "0");
        expect(routeTotal > 0, "lite consumer asked the namesrv for its route",
               "routeRequests=" + std::to_string(routeTotal));
        expectInt(routeTotal, routeTagged, "every lite-consumer route request carries ReqT");
        expect(mock.countRequestsWith(RequestCode::HEART_BEAT, MixAll::REQ_T, "0") > 0,
               "lite consumer heartbeat carries ReqT");
    }
    {
        // the same path that explicitly turns stream off must carry not a single one (the default is not "hardcoded on")
        mock.clearRequests();
        DefaultLitePullConsumer c("PG_lite_no_reqt");
        c.setNamesrvAddr(mock.address());
        c.setInstanceName("lite-no-reqt");
        c.setEnableStreamRequestType(false);
        c.subscribe(topic, "*");
        c.start();
        c.shutdown();
        expectInt(mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, MixAll::REQ_T, "0"), 0,
                  "setEnableStreamRequestType(false) really turns ReqT off");
        expect(mock.countRequests(RequestCode::GET_ROUTEINFO_BY_TOPIC) > 0,
               "the consumer did talk to the namesrv, so that 0 means something");
    }
}

void testPinnedTopicGuard(MockEndpoint& mock) {
    const std::string topic = "PinnedGuardOk";
    mock.addRoute(topic, makeRoute(mock.address(), 1, 1));

    DefaultMQProducer p("PG_pinned_guard");
    p.setNamesrvAddr(mock.address());
    p.start();

    {
        // (1) synchronous single message with a mismatched topic: the synchronous wording, zero SEND
        const int before = mock.sendCount();
        int threw = 0;
        try {
            p.send(plainMessage("PinnedGuardOther"), MessageQueue(topic, "broker-a", 0), 3000);
        } catch (const MQClientException& e) {
            threw = 1;
            expect(std::string(e.what()) == "message's topic not equal mq's topic",
                   "the sync guard keeps the reference wording verbatim", e.what());
        } catch (const std::exception& e) {
            std::printf("  wrong exception: %s\n", e.what());
        }
        expectInt(threw, 1, "a mismatched pinned send is refused");
        expectInt(mock.sendCount() - before, 0, "the guard refuses before any SEND reaches the wire");
    }
    {
        // (2) same topic: goes on the wire as usual (the guard must not be so loose that it misfires)
        const int before = mock.sendCount();
        SendResult r = p.send(plainMessage(topic), MessageQueue(topic, "broker-a", 0), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "a matching pinned send still goes out");
        expectInt(mock.sendCount() - before, 1, "exactly one SEND on the wire");
        WireRecord rec = mock.sendRecord(static_cast<size_t>(mock.sendCount() - 1));
        const auto topicField = rec.ext.find("b");  // the V2 single-letter key: topic
        expect(topicField != rec.ext.end() && topicField->second == topic,
               "the wire carries the message topic");
    }
    {
        // (3) batch (over the synchronous kernel) hits the same guard: mismatch is rejected, zero SEND
        std::vector<Message> msgs{plainMessage("PinnedGuardOther"), plainMessage("PinnedGuardOther")};
        const int before = mock.sendCount();
        int threw = 0;
        try {
            p.sendBatch(msgs, MessageQueue(topic, "broker-a", 0), 3000);
        } catch (const MQClientException& e) {
            threw = 1;
            expect(std::string(e.what()) == "message's topic not equal mq's topic",
                   "the batch kernel shares the sync guard", e.what());
        } catch (const std::exception& e) {
            std::printf("  wrong exception: %s\n", e.what());
        }
        expectInt(threw, 1, "a mismatched pinned batch is refused");
        expectInt(mock.sendCount() - before, 0, "no SEND leaves for a refused batch");
    }
    {
        // (4) a batch for the same topic is still sent, with the batch request code
        std::vector<Message> msgs{plainMessage(topic), plainMessage(topic)};
        const int before = mock.sendCount();
        SendResult r = p.sendBatch(msgs, MessageQueue(topic, "broker-a", 0), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "a matching pinned batch still goes out");
        expectInt(mock.sendCount() - before, 1, "one batch SEND on the wire");
        WireRecord rec = mock.sendRecord(static_cast<size_t>(mock.sendCount() - 1));
        expectInt(rec.code, RequestCode::SEND_BATCH_MESSAGE, "the batch request code is used");
    }
    {
        const std::string owTarget = "PinnedGuardOnewayTarget";
        mock.addRoute(owTarget, makeRoute(mock.address(), 4, 4));
        const int before = mock.sendCount();
        int threw = 0;
        try {
            p.sendOneway(plainMessage("PinnedGuardOther"), MessageQueue(owTarget, "broker-a", 3));
        } catch (const std::exception& e) {
            threw = 1;
            std::printf("  wrong exception: %s\n", e.what());
        }
        expectInt(threw, 0, "the pinned oneway has no guard: a mismatched topic still leaves");
        // oneway gets no reply and the client leaves as soon as it has written: the mock's receive thread may not have booked it yet, so poll until it lands
        bool arrived = false;
        for (int i = 0; i < 60 && !arrived; ++i) {
            arrived = mock.sendCount() - before == 1;
            if (!arrived) std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        expectInt(mock.sendCount() - before, 1, "the mismatched oneway reaches the wire");
        if (mock.sendCount() > before) {
            const WireRecord rec = mock.sendRecord(static_cast<size_t>(mock.sendCount() - 1));
            const auto b = rec.ext.find("b");
            const auto e = rec.ext.find("e");
            expect(b != rec.ext.end() && b->second == "PinnedGuardOther",
                   "the wire topic is the message's own, not the queue's");
            expect(e != rec.ext.end() && e->second == "3",
                   "the queueId still comes from the pinned queue");
        }
    }
    p.shutdown();

    // (5) namespace: what is compared are the respective wrapped resource names
    const std::string nsTopic = "PinnedGuardNs";
    const std::string nsWrapped = "ns1%PinnedGuardNs";
    mock.addRoute(nsWrapped, makeRoute(mock.address(), 1, 1));
    DefaultMQProducer ns("PG_pinned_guard_ns");
    ns.setNamesrvAddr(mock.address());
    ns.setNamespace("ns1");
    ns.start();
    {
        // the queue topic already carries the prefix (that is the shape the route returns) -- must not be rejected by mistake
        const int before = mock.sendCount();
        SendResult r = ns.send(plainMessage(nsTopic), MessageQueue(nsWrapped, "broker-a", 0), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "an already-prefixed queue topic is not falsely rejected");
        expectInt(mock.sendCount() - before, 1, "the namespaced send goes out once");
        WireRecord rec = mock.sendRecord(static_cast<size_t>(mock.sendCount() - 1));
        const auto topicField = rec.ext.find("b");
        expect(topicField != rec.ext.end() && topicField->second == nsWrapped,
               "the wire carries the namespaced resource name");
    }
    {
        // a bare queue topic passes as well (both sides are equal once wrapped)
        const int before = mock.sendCount();
        int threw = 0;
        try {
            // the route is registered under the name the client actually queries (mq.topic): add a bare-name route here
            mock.addRoute(nsTopic, makeRoute(mock.address(), 1, 1));
            SendResult r = ns.send(plainMessage(nsTopic), MessageQueue(nsTopic, "broker-a", 0), 3000);
            expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                      "a raw queue topic is accepted under a namespace");
        } catch (const std::exception& e) {
            threw = 1;
            std::printf("  unexpected exception: %s\n", e.what());
        }
        expectInt(threw, 0, "no exception for a raw queue topic");
        expectInt(mock.sendCount() - before, 1, "it was really sent");
    }
    {
        // the same topic name under another namespace: that is another resource, it must be rejected
        const int before = mock.sendCount();
        int threw = 0;
        try {
            ns.send(plainMessage(nsTopic), MessageQueue("ns2%PinnedGuardNs", "broker-a", 0), 3000);
        } catch (const MQClientException& e) {
            threw = 1;
            expect(std::string(e.what()) == "message's topic not equal mq's topic",
                   "a cross-namespace topic is a mismatch", e.what());
        } catch (const std::exception& e) {
            std::printf("  wrong exception: %s\n", e.what());
        }
        expectInt(threw, 1, "cross-namespace pinned send is refused");
        expectInt(mock.sendCount() - before, 0, "no SEND leaves for the cross-namespace mismatch");
    }
    ns.shutdown();
}

}  // namespace

namespace {

// An unexpected exception inside a case must become a readable failure instead of terminating the whole process
void runCase(const char* name, MockEndpoint& mock, void (*fn)(MockEndpoint&)) {
    const auto begin = std::chrono::steady_clock::now();
    try {
        fn(mock);
    } catch (const std::exception& e) {
        ++checks;
        ++fails;
        std::printf("FAIL %s threw: %s\n", name, e.what());
    } catch (...) {
        ++checks;
        ++fails;
        std::printf("FAIL %s threw: unknown error\n", name);
    }
    const long long ms = static_cast<long long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin)
            .count());
    // Every case uses a local mock and normally finishes within a few hundred ms; over 2s means some request is waiting for a timeout, and it must be obvious who
    if (ms > 2000) std::printf("SLOW %s: %lldms\n", name, ms);
}

}  // namespace

int main() {
    MockEndpoint mock;

    runCase("retryResponseCodes", mock, testRetryResponseCodes);
    runCase("retryAnotherBrokerWhenNotStoreOK", mock, testRetryAnotherBrokerWhenNotStoreOK);
    runCase("sendMsgMaxTimeoutPerRequest", mock, testSendMsgMaxTimeoutPerRequest);
    runCase("callTimeout", mock, testCallTimeout);
    runCase("errorCodeMapping", mock, testErrorCodeMapping);
    runCase("noNameServerAddressQualifies10004", mock,
            testNoNameServerAddressQualifies10004);
    runCase("faultItemFlags", mock, testFaultItemFlags);
    runCase("unitModeReachesWire", mock, testUnitModeReachesWire);
    runCase("sendHeaderFieldsReachWire", mock, testSendHeaderFieldsReachWire);
    runCase("sendRequestCodeMatchesProtocol", mock, testSendRequestCodeMatchesProtocol);
    runCase("requestHooksReachWire", mock, testRequestHooksReachWire);
    runCase("pinnedTopicGuard", mock, testPinnedTopicGuard);

    std::printf("%s: %d checks, %d failures\n", fails == 0 ? "PASS" : "FAIL", checks, fails);
    return fails == 0 ? 0 : 1;
}
