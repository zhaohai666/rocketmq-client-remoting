// Offline tests for the client configuration switches: decodeReadBody, mqClientApiTimeout,
// concurrent heartbeat and maxPageSizeInGetMetadata (including the paged admin metadata walks).
// A loopback recording broker answers per script and keeps every request it read off the socket,
// so assertions are made on recorded wire state, never on timing sleeps.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/remoting/protocol/admin_body.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/subscription.h"

using namespace rocketmq;

namespace rocketmq {
// Seeds the broker address table for offline heartbeat rounds (the same friend the other offline
// tests use).
struct MQClientInstanceTestAccess {
    static void seedBrokerAddr(MQClientInstance& client, const std::string& brokerName,
                               int64_t brokerId, const std::string& addr) {
        std::lock_guard<std::recursive_mutex> lk(client.routeLock_);
        client.brokerAddrTable_[brokerName][brokerId] = addr;
    }
};
}  // namespace rocketmq

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

// ---------------------------------------------------------------- socket utilities

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

// 100ms select rounds: every server-side wait is bounded and exits promptly when alive turns false.
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

// ---------------------------------------------------------------- recording broker

struct RecordedRequest {
    int32_t code = 0;
    PropertyMap ext;
    Bytes body;
};

struct BrokerReply {
    int32_t code = ResponseCode::SUCCESS;
    PropertyMap ext;
    Bytes body;
    bool reply = true;  // false = keep the connection open but answer nothing (timeout probes)
};

class RecordingBroker {
public:
    using Handler = std::function<BrokerReply(const RecordedRequest&)>;

    RecordingBroker() {
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
        ::listen(listen_, 32);
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~RecordingBroker() {
        running_.store(false);
        if (acceptor_.joinable()) acceptor_.join();
        for (;;) {
            std::vector<std::thread> pending;
            {
                std::lock_guard<std::mutex> lk(workersM_);
                pending.swap(workers_);
            }
            if (pending.empty()) break;
            for (std::thread& t : pending) {
                if (t.joinable()) t.join();
            }
        }
        netcompat::closeSocket(listen_);
    }

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void setHandler(Handler handler) {
        std::lock_guard<std::mutex> lk(state_);
        handler_ = std::move(handler);
    }

    void clearRequests() {
        std::lock_guard<std::mutex> lk(state_);
        requests_.clear();
    }

    std::vector<RecordedRequest> requests() const {
        std::lock_guard<std::mutex> lk(state_);
        return requests_;
    }

    std::vector<RecordedRequest> requestsOf(int32_t code) const {
        std::lock_guard<std::mutex> lk(state_);
        std::vector<RecordedRequest> out;
        for (const RecordedRequest& r : requests_) {
            if (r.code == code) out.push_back(r);
        }
        return out;
    }

    int32_t countOf(int32_t code) const {
        return static_cast<int32_t>(requestsOf(code).size());
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
            std::lock_guard<std::mutex> lk(workersM_);
            workers_.emplace_back([this, sock]() { serveConn(sock); });
        }
    }

    void serveConn(socket_t sock) {
        Bytes frame;
        while (running_.load() && readFrame(sock, running_, frame)) {
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            RecordedRequest record{req.code, req.extFields, req.body};
            Handler handler;
            {
                std::lock_guard<std::mutex> lk(state_);
                requests_.push_back(record);
                handler = handler_;
            }
            BrokerReply reply;
            if (handler) reply = handler(record);
            if (!reply.reply) continue;
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            resp.code = reply.code;
            resp.extFields = reply.ext;
            if (!reply.body.empty()) {
                resp.body = reply.body;
                resp.hasBody = true;
            }
            if (!writeAll(sock, resp.encode())) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    mutable std::mutex state_;
    Handler handler_;
    std::vector<RecordedRequest> requests_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scripted bodies

JsonValue parseJson(const std::string& text) {
    JsonValue v;
    jsonParse(text, v);
    return v;
}

std::string versionText(const char* versionJson) {
    return parseJson(versionJson).dump();
}

MessageExt storedMessage(const std::string& topic, int32_t queueId, int64_t queueOffset,
                         const std::string& body) {
    MessageExt m;
    m.topic = topic;
    m.queueId = queueId;
    m.queueOffset = queueOffset;
    m.body = body;
    m.flag = 7;
    m.bornTimestamp = 1700000000000LL;
    m.storeTimestamp = 1700000000001LL;
    m.bornHost = "127.0.0.1";
    m.bornHostPort = 10911;
    m.storeHost = "127.0.0.1";
    m.storeHostPort = 10911;
    m.commitLogOffset = 4096 + queueOffset;
    m.setTags("TagA");
    m.setUserProperty("city", "Hangzhou");
    return m;
}

Bytes twoMessageBody(const std::string& topic, int64_t firstOffset) {
    return encodeMessageExt(storedMessage(topic, 0, firstOffset, "body-one"), false) +
           encodeMessageExt(storedMessage(topic, 0, firstOffset + 1, "body-two"), false);
}

BrokerReply pullReply(const Bytes& body) {
    BrokerReply r;
    r.code = ResponseCode::SUCCESS;
    r.ext["nextBeginOffset"] = "2";
    r.ext["minOffset"] = "0";
    r.ext["maxOffset"] = "9";
    r.ext["suggestWhichBrokerId"] = "0";
    r.body = body;
    return r;
}

BrokerReply topicPageReply(const std::vector<std::string>& topics, const char* versionJson,
                           long long total, bool withTotal) {
    TopicConfigSerializeWrapper wrapper;
    for (const std::string& name : topics) {
        wrapper.topicConfigTable[name] = TopicConfig(name);
    }
    wrapper.dataVersion = parseJson(versionJson);
    BrokerReply r;
    r.code = ResponseCode::SUCCESS;
    r.body = wrapper.encode();
    if (withTotal) r.ext["totalTopicNum"] = std::to_string(total);
    return r;
}

BrokerReply groupPageReply(const std::vector<std::string>& groups, const char* versionJson,
                           long long total, bool withTotal) {
    SubscriptionGroupWrapper wrapper;
    for (const std::string& name : groups) {
        wrapper.subscriptionGroupTable[name] = SubscriptionGroupConfig(name);
    }
    wrapper.dataVersion = parseJson(versionJson);
    BrokerReply r;
    r.code = ResponseCode::SUCCESS;
    r.body = wrapper.encode();
    if (withTotal) r.ext["totalGroupNum"] = std::to_string(total);
    return r;
}

// ---------------------------------------------------------------- test cases

void testDecodeSwitch() {
    RecordingBroker broker;
    const char* topic = "T_Decode";
    broker.setHandler([topic](const RecordedRequest& req) -> BrokerReply {
        if (req.code == RequestCode::PULL_MESSAGE || req.code == RequestCode::LITE_PULL_MESSAGE ||
            req.code == RequestCode::POP_MESSAGE) {
            return pullReply(twoMessageBody(topic, 10));
        }
        return BrokerReply{};
    });

    MQClientInstance instance("cid@decode", std::vector<std::string>{broker.address()});
    MessageQueue mq(topic, "broker-a", 0);

    // Default: bodies materialized.
    PullResult on = instance.pullMessage("GID_Decode", mq, 0, 32, 0, 0, "*", 0, "", 3000, -1,
                                         15000, broker.address());
    expectInt(static_cast<long long>(on.msgFoundList.size()), 2, "decode on: two messages");
    expect(on.msgFoundList[0].body == "body-one" && on.msgFoundList[1].body == "body-two",
           "decode on: bodies materialized");
    expect(on.msgFoundList[0].hasBody && on.msgFoundList[1].hasBody,
           "decode on: hasBody stays true");

    // Switch off: both frames are still returned, in order, with no body bytes.
    instance.setDecodeReadBody(false);
    PullResult off = instance.pullMessage("GID_Decode", mq, 0, 32, 0, 0, "*", 0, "", 3000, -1,
                                          15000, broker.address());
    expectInt(static_cast<long long>(off.msgFoundList.size()), 2,
              "decode off: the frame walk still returns both messages");
    expect(off.msgFoundList[0].body.empty() && off.msgFoundList[1].body.empty(),
           "decode off: bodies stay empty");
    expect(!off.msgFoundList[0].hasBody && !off.msgFoundList[1].hasBody,
           "decode off: hasBody is false");
    expectInt(off.msgFoundList[0].queueOffset, 10, "decode off: first queue offset intact");
    expectInt(off.msgFoundList[1].queueOffset, 11,
              "decode off: second frame reached (cursor advanced)");
    expect(off.msgFoundList[1].getTags() == "TagA"
               && off.msgFoundList[1].getUserProperty("city") == "Hangzhou",
           "decode off: properties intact");

    // Lite-pull request flag takes the same decode path (and really uses code 361).
    const int32_t liteFlag = PullSysFlag::buildSysFlag(false, false, true, false, true);
    PullResult lite = instance.pullMessage("GID_Decode", mq, 0, 32, liteFlag, 0, "*", 0, "",
                                           3000, -1, 15000, broker.address());
    expectInt(static_cast<long long>(lite.msgFoundList.size()), 2,
              "decode off (lite pull): two messages");
    expect(lite.msgFoundList[0].body.empty() && lite.msgFoundList[1].body.empty(),
           "decode off (lite pull): bodies stay empty");
    expectInt(broker.countOf(RequestCode::LITE_PULL_MESSAGE), 1,
              "lite pull flag sends LITE_PULL_MESSAGE");

    // POP default: bodies materialized.
    instance.setDecodeReadBody(true);
    PopResult popOn = instance.popMessage("GID_Decode", topic, 0, 32, 60000, 0, 0, "", "", false,
                                          3000, "broker-a", broker.address());
    expectInt(static_cast<long long>(popOn.msgFoundList.size()), 2, "pop decode on: two messages");
    expect(popOn.msgFoundList[0].body == "body-one", "pop decode on: body materialized");

    // POP off: header-only messages, offsets and properties still usable.
    instance.setDecodeReadBody(false);
    PopResult popOff = instance.popMessage("GID_Decode", topic, 0, 32, 60000, 0, 0, "", "", false,
                                           3000, "broker-a", broker.address());
    expectInt(static_cast<long long>(popOff.msgFoundList.size()), 2, "pop decode off: two messages");
    expect(popOff.msgFoundList[0].body.empty() && popOff.msgFoundList[1].body.empty(),
           "pop decode off: bodies stay empty");
    expectInt(popOff.msgFoundList[1].queueOffset, 11,
              "pop decode off: second frame reached (cursor advanced)");
    expect(popOff.msgFoundList[0].getTags() == "TagA",
           "pop decode off: properties intact");
}

void testHeartbeatTimeout() {
    RecordingBroker broker;
    broker.setHandler([](const RecordedRequest& req) -> BrokerReply {
        BrokerReply r;
        r.reply = req.code != RequestCode::HEART_BEAT;  // heartbeats are read but never answered
        return r;
    });
    MQClientInstance instance("cid@timeout", std::vector<std::string>{broker.address()});

    HeartbeatData hb("cid@timeout");
    ProducerData pd;
    pd.groupName = "GID_Timeout";
    hb.addProducerData(pd);

    auto elapsedMs = [](std::chrono::steady_clock::time_point begin) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - begin)
            .count();
    };

    bool threw = false;
    auto begin = std::chrono::steady_clock::now();
    try {
        instance.sendHeartbeat(broker.address(), hb);
    } catch (const std::exception&) {
        threw = true;
    }
    const long long defaultMs = elapsedMs(begin);
    expect(threw, "heartbeat default timeout: the call times out");
    expect(defaultMs >= 2500 && defaultMs < 4800,
           "heartbeat default timeout is 3000ms",
           "elapsed=" + std::to_string(defaultMs));

    // An explicit per-call timeout wins over the configured default.
    threw = false;
    begin = std::chrono::steady_clock::now();
    try {
        instance.sendHeartbeat(broker.address(), hb, 250);
    } catch (const std::exception&) {
        threw = true;
    }
    const long long overrideMs = elapsedMs(begin);
    expect(threw, "heartbeat explicit timeout: the call times out");
    expect(overrideMs >= 100 && overrideMs < 1500,
           "explicit 250ms heartbeat timeout wins",
           "elapsed=" + std::to_string(overrideMs));

    // The shared instance knob is actually read when no per-call timeout is given.
    instance.setMqClientApiTimeout(400);
    begin = std::chrono::steady_clock::now();
    try {
        instance.sendHeartbeat(broker.address(), hb);
    } catch (const std::exception&) {
    }
    const long long configuredMs = elapsedMs(begin);
    expect(configuredMs >= 200 && configuredMs < 1500,
           "configured mqClientApiTimeout is used by the heartbeat",
           "elapsed=" + std::to_string(configuredMs));
    expectInt(broker.countOf(RequestCode::HEART_BEAT), 3,
              "every heartbeat reached the broker before timing out");
}

void testConcurrentHeartbeat() {
    RecordingBroker masterA;
    RecordingBroker masterB;
    RecordingBroker slaveC;
    auto success = [](const RecordedRequest&) {
        BrokerReply r;
        r.code = ResponseCode::SUCCESS;
        return r;
    };
    masterA.setHandler(success);
    masterB.setHandler(success);
    slaveC.setHandler(success);

    MQClientInstance instance("cid@concurrent", std::vector<std::string>{masterA.address()});
    instance.setEnableConcurrentHeartbeat(true);
    instance.setConcurrentHeartbeatThreadPoolSize(2);
    MQClientInstanceTestAccess::seedBrokerAddr(instance, "broker-a", MixAll::MASTER_ID,
                                               masterA.address());
    MQClientInstanceTestAccess::seedBrokerAddr(instance, "broker-a", 1, slaveC.address());
    MQClientInstanceTestAccess::seedBrokerAddr(instance, "broker-b", MixAll::MASTER_ID,
                                               masterB.address());
    instance.start();

    HeartbeatData producerOnly("cid@concurrent");
    ProducerData pd;
    pd.groupName = "GID_Concurrent";
    producerOnly.addProducerData(pd);

    const int32_t producerOk = instance.sendHeartbeatConcurrently(producerOnly);
    expectInt(producerOk, 2, "concurrent round: every master succeeded");
    expectInt(masterA.countOf(RequestCode::HEART_BEAT), 1, "concurrent round: master A hit once");
    expectInt(masterB.countOf(RequestCode::HEART_BEAT), 1, "concurrent round: master B hit once");
    expectInt(slaveC.countOf(RequestCode::HEART_BEAT), 0,
              "producer-only heartbeat skips the slave");

    // Empty body: nothing is sent at all.
    HeartbeatData empty("cid@concurrent");
    expectInt(instance.sendHeartbeatConcurrently(empty), 0, "empty heartbeat body sends nothing");
    expectInt(masterA.countOf(RequestCode::HEART_BEAT), 1,
              "empty heartbeat body leaves master A untouched");
    expectInt(slaveC.countOf(RequestCode::HEART_BEAT), 0,
              "empty heartbeat body leaves the slave untouched");

    // With consumer data the slave is a legal target (control for the skip rule).
    HeartbeatData withConsumer("cid@concurrent");
    ConsumerData cd;
    cd.groupName = "GID_Concurrent";
    withConsumer.addConsumerData(cd);
    expectInt(instance.sendHeartbeatConcurrently(withConsumer), 3,
              "consumer heartbeat round reaches masters and slave");
    expectInt(masterA.countOf(RequestCode::HEART_BEAT), 2,
              "consumer heartbeat: master A hit again");
    expectInt(masterB.countOf(RequestCode::HEART_BEAT), 2,
              "consumer heartbeat: master B hit again");
    expectInt(slaveC.countOf(RequestCode::HEART_BEAT), 1,
              "consumer heartbeat: slave hit when consumer data is present");

    instance.shutdown();

    // The facade forwards every new knob to the shared instance at start().
    DefaultMQProducer producer("GID_Facade");
    producer.setNamesrvAddr(masterA.address());
    producer.setDecodeReadBody(false);
    producer.setMqClientApiTimeout(1234);
    producer.setEnableConcurrentHeartbeat(true);
    producer.setConcurrentHeartbeatThreadPoolSize(3);
    producer.start();
    expect(!producer.client().isDecodeReadBody(), "producer forwards decodeReadBody");
    expectInt(producer.client().mqClientApiTimeout(), 1234,
              "producer forwards mqClientApiTimeout");
    expect(producer.client().isEnableConcurrentHeartbeat(),
           "producer forwards enableConcurrentHeartbeat");
    expectInt(producer.client().concurrentHeartbeatThreadPoolSize(), 3,
              "producer forwards concurrentHeartbeatThreadPoolSize");
    producer.shutdown();
}

void testAdminPaging() {
    RecordingBroker broker;
    DefaultMQAdminExt admin("ADMIN_Paging");
    admin.setNamesrvAddr(broker.address());
    admin.start();

    // --- default page size reaches maxTopicNum; no totalTopicNum => one request.
    broker.setHandler([](const RecordedRequest& req) -> BrokerReply {
        if (req.code == RequestCode::GET_ALL_TOPIC_CONFIG) {
            return topicPageReply({"topicA"}, "{\"counter\":1,\"timestamp\":100}", 0, false);
        }
        return BrokerReply{};
    });
    TopicConfigSerializeWrapper single = admin.getAllTopicConfig(broker.address());
    std::vector<RecordedRequest> records = broker.requestsOf(RequestCode::GET_ALL_TOPIC_CONFIG);
    expectInt(static_cast<long long>(records.size()), 1,
              "topic paging: no totalTopicNum stops after one page");
    expectInt(records[0].ext.count("maxTopicNum"), 1, "topic paging: maxTopicNum is present");
    expect(records[0].ext.at("maxTopicNum") == "2000",
           "topic paging: default maxTopicNum is 2000");
    expectInt(records[0].ext.count("dataVersion"), 1,
              "topic paging: first page carries the dataVersion key");
    expect(records[0].ext.at("dataVersion").empty(),
           "topic paging: first page dataVersion is the empty string");
    expectInt(static_cast<long long>(single.topicConfigTable.size()), 1,
              "topic paging: single page merged");

    // --- configured page size, stop at totalTopicNum - 1.
    admin.setMaxPageSizeInGetMetadata(2);
    broker.clearRequests();
    int topicStep = 0;
    broker.setHandler([&topicStep](const RecordedRequest& req) -> BrokerReply {
        if (req.code != RequestCode::GET_ALL_TOPIC_CONFIG) return BrokerReply{};
        switch (topicStep++) {
            case 0:
                return topicPageReply({"topicA"}, "{\"counter\":1,\"timestamp\":100}", 5, true);
            case 1:
                return topicPageReply({"topicB"}, "{\"counter\":1,\"timestamp\":100}", 5, true);
            default:
                return topicPageReply({}, "{\"counter\":1,\"timestamp\":100}", 0, false);
        }
    });
    TopicConfigSerializeWrapper twoPages = admin.getAllTopicConfig(broker.address());
    records = broker.requestsOf(RequestCode::GET_ALL_TOPIC_CONFIG);
    expectInt(static_cast<long long>(records.size()), 2,
              "topic paging: stops once topicSeq reaches totalTopicNum - 1");
    expect(records[0].ext.at("topicSeq") == "0" && records[1].ext.at("topicSeq") == "2",
           "topic paging: topicSeq advances by the page size");
    expect(records[0].ext.at("maxTopicNum") == "2" && records[1].ext.at("maxTopicNum") == "2",
           "topic paging: configured maxTopicNum reaches the broker");
    expect(records[0].ext.at("dataVersion").empty(),
           "topic paging: empty-string dataVersion on the first page");
    expect(records[1].ext.at("dataVersion") ==
               versionText("{\"counter\":1,\"timestamp\":100}"),
           "topic paging: later pages send the last seen dataVersion");
    expectInt(static_cast<long long>(twoPages.topicConfigTable.size()), 2,
              "topic paging: both pages merged");
    expect(twoPages.topicConfigTable.count("topicA") == 1 &&
               twoPages.topicConfigTable.count("topicB") == 1,
           "topic paging: merged names from both pages");

    // --- a changed dataVersion restarts sequencing and clears the merged table.
    broker.clearRequests();
    int restartStep = 0;
    broker.setHandler([&restartStep](const RecordedRequest& req) -> BrokerReply {
        if (req.code != RequestCode::GET_ALL_TOPIC_CONFIG) return BrokerReply{};
        switch (restartStep++) {
            case 0:
                return topicPageReply({"oldA"}, "{\"counter\":1,\"timestamp\":100}", 5, true);
            case 1:
                return topicPageReply({"oldB"}, "{\"counter\":2,\"timestamp\":200}", 5, true);
            case 2:
                return topicPageReply({"newC"}, "{\"counter\":2,\"timestamp\":200}", 5, true);
            case 3:
                return topicPageReply({"newD"}, "{\"counter\":2,\"timestamp\":200}", 5, true);
            default:
                return topicPageReply({}, "{\"counter\":2,\"timestamp\":200}", 0, false);
        }
    });
    TopicConfigSerializeWrapper restarted = admin.getAllTopicConfig(broker.address());
    records = broker.requestsOf(RequestCode::GET_ALL_TOPIC_CONFIG);
    expectInt(static_cast<long long>(records.size()), 4,
              "topic paging: version change restarts and then finishes");
    expect(records[0].ext.at("topicSeq") == "0" && records[1].ext.at("topicSeq") == "2" &&
               records[2].ext.at("topicSeq") == "0" && records[3].ext.at("topicSeq") == "2",
           "topic paging: sequence restarts at 0 after a dataVersion change");
    expect(records[2].ext.at("dataVersion") ==
               versionText("{\"counter\":2,\"timestamp\":200}"),
           "topic paging: restarted page sends the new dataVersion");
    expectInt(static_cast<long long>(restarted.topicConfigTable.size()), 2,
              "topic paging: only post-restart pages survive");
    expect(restarted.topicConfigTable.count("oldA") == 0 &&
               restarted.topicConfigTable.count("oldB") == 0 &&
               restarted.topicConfigTable.count("newC") == 1 &&
               restarted.topicConfigTable.count("newD") == 1,
           "topic paging: table cleared on dataVersion change");

    // --- subscription groups: default page size and paged walk use the same rules.
    admin.setMaxPageSizeInGetMetadata(2000);
    broker.clearRequests();
    broker.setHandler([](const RecordedRequest& req) -> BrokerReply {
        if (req.code == RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG) {
            return groupPageReply({"groupZ"}, "{\"counter\":3,\"timestamp\":300}", 0, false);
        }
        return BrokerReply{};
    });
    SubscriptionGroupWrapper groupSingle = admin.getAllSubscriptionGroup(broker.address());
    records = broker.requestsOf(RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG);
    expectInt(static_cast<long long>(records.size()), 1,
              "group paging: no totalGroupNum stops after one page");
    expect(records[0].ext.at("maxGroupNum") == "2000",
           "group paging: default maxGroupNum is 2000");
    expectInt(records[0].ext.count("dataVersion"), 1,
              "group paging: first page carries the dataVersion key");
    expect(records[0].ext.at("dataVersion").empty(),
           "group paging: first page dataVersion is the empty string");
    expectInt(static_cast<long long>(groupSingle.subscriptionGroupTable.size()), 1,
              "group paging: single page merged");

    admin.setMaxPageSizeInGetMetadata(2);
    broker.clearRequests();
    int groupStep = 0;
    broker.setHandler([&groupStep](const RecordedRequest& req) -> BrokerReply {
        if (req.code != RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG) return BrokerReply{};
        switch (groupStep++) {
            case 0:
                return groupPageReply({"groupA"}, "{\"counter\":4,\"timestamp\":400}", 4, true);
            case 1:
                return groupPageReply({"groupB"}, "{\"counter\":4,\"timestamp\":400}", 4, true);
            default:
                return groupPageReply({}, "{\"counter\":4,\"timestamp\":400}", 0, false);
        }
    });
    SubscriptionGroupWrapper groupPages = admin.getAllSubscriptionGroup(broker.address());
    records = broker.requestsOf(RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG);
    expectInt(static_cast<long long>(records.size()), 2,
              "group paging: stops once groupSeq reaches totalGroupNum - 1");
    expect(records[0].ext.at("groupSeq") == "0" && records[1].ext.at("groupSeq") == "2",
           "group paging: groupSeq advances by the configured page size");
    expect(records[0].ext.at("maxGroupNum") == "2",
           "group paging: configured maxGroupNum reaches the broker");
    expect(records[0].ext.at("dataVersion").empty(),
           "group paging: empty-string dataVersion on the first page");
    expect(records[1].ext.at("dataVersion") ==
               versionText("{\"counter\":4,\"timestamp\":400}"),
           "group paging: later pages send the last seen dataVersion");
    expectInt(static_cast<long long>(groupPages.subscriptionGroupTable.size()), 2,
              "group paging: both pages merged");

    // --- group dataVersion change restarts like the topic walk.
    broker.clearRequests();
    int groupRestartStep = 0;
    broker.setHandler([&groupRestartStep](const RecordedRequest& req) -> BrokerReply {
        if (req.code != RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG) return BrokerReply{};
        switch (groupRestartStep++) {
            case 0:
                return groupPageReply({"oldG"}, "{\"counter\":5,\"timestamp\":500}", 4, true);
            case 1:
                return groupPageReply({"newG"}, "{\"counter\":6,\"timestamp\":600}", 4, true);
            case 2:
                return groupPageReply({"newH"}, "{\"counter\":6,\"timestamp\":600}", 4, true);
            case 3:
                return groupPageReply({"newI"}, "{\"counter\":6,\"timestamp\":600}", 4, true);
            default:
                return groupPageReply({}, "{\"counter\":6,\"timestamp\":600}", 0, false);
        }
    });
    SubscriptionGroupWrapper groupRestarted = admin.getAllSubscriptionGroup(broker.address());
    records = broker.requestsOf(RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG);
    expectInt(static_cast<long long>(records.size()), 4,
              "group paging: version change restarts and then finishes");
    expect(records[2].ext.at("groupSeq") == "0",
           "group paging: sequence restarts at 0 after a dataVersion change");
    expect(groupRestarted.subscriptionGroupTable.count("oldG") == 0 &&
               groupRestarted.subscriptionGroupTable.count("newG") == 0 &&
               groupRestarted.subscriptionGroupTable.count("newH") == 1 &&
               groupRestarted.subscriptionGroupTable.count("newI") == 1,
           "group paging: table cleared on dataVersion change");

    admin.shutdown();
}

}  // namespace

int main() {
    testDecodeSwitch();
    testHeartbeatTimeout();
    testConcurrentHeartbeat();
    testAdminPaging();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
