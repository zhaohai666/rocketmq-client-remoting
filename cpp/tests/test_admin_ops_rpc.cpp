// Wire-contract tests for the #47 ops RPC admin group (Java MQClientAPIImpl:2773-3816):
// queryCorrectionOffset(308), the three unit-topic reads (311/312/313), getAllProducerInfo(328),
// getBrokerHAStatus(907), resetMasterFlushOffset(908), electMaster(1002),
// getBrokerEpochCache(1007) and switchTimerEngine(5001). No cluster is required.
//
// The harness binds a listener pair: the primary plays the broker / controller (the address the
// caller passes in), the decoy sits exactly two ports below it and doubles as the name server.
// That split proves two routings at once with nothing but the recorded landing port: the
// unit-topic family has to arrive on the decoy (name-server addressing) and, with the VIP switch
// on, 308/328 have to move from the primary to the decoy (port-2 rewrite) instead of silently
// "working" against the plain port.
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/protocol/admin_body.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

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

int checks = 0;
int fails = 0;

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

void expectStr(const std::string& actual, const std::string& expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s\n  actual=[%s]\n  expect=[%s]\n", name.c_str(), actual.c_str(),
                    expected.c_str());
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
    Bytes payload(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, reinterpret_cast<char*>(payload.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + payload;
    return true;
}

// ---------------------------------------------------------------- recording listener

struct ScriptedReply {
    int32_t code = ResponseCode::SUCCESS;
    std::string remark;
    std::string body;
    PropertyMap ext;
};

struct WireRecord {
    int32_t code = 0;
    PropertyMap ext;
    std::string body;
};

class RecordingListener {
public:
    explicit RecordingListener(uint16_t port = 0) {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);
        if (::bind(listen_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            netcompat::closeSocket(listen_);
            listen_ = kInvalidSocket;
            return;
        }
        if (::listen(listen_, 16) != 0) {
            netcompat::closeSocket(listen_);
            listen_ = kInvalidSocket;
            return;
        }
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~RecordingListener() {
        running_.store(false);
        if (acceptor_.joinable()) acceptor_.join();
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
        if (listen_ != kInvalidSocket) netcompat::closeSocket(listen_);
    }

    bool ok() const { return listen_ != kInvalidSocket; }
    uint16_t port() const { return port_; }
    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void script(int32_t code, std::vector<ScriptedReply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        scripts_[code] = std::move(replies);
        records_.erase(code);
        arrivals_.erase(code);
    }

    int requestCount(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        auto it = arrivals_.find(code);
        return it == arrivals_.end() ? 0 : it->second;
    }

    WireRecord record(int32_t code, size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        auto it = records_.find(code);
        if (it == records_.end() || i >= it->second.size()) return WireRecord{};
        return it->second[i];
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
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            {
                std::lock_guard<std::mutex> lk(state_);
                int idx = arrivals_[req.code]++;
                records_[req.code].push_back(
                    {req.code, req.extFields, std::string(req.body.begin(), req.body.end())});
                auto it = scripts_.find(req.code);
                if (it != scripts_.end() && idx < static_cast<int>(it->second.size())) {
                    const ScriptedReply& reply = it->second[static_cast<size_t>(idx)];
                    resp.code = reply.code;
                    resp.remark = reply.remark;
                    resp.hasRemark = !reply.remark.empty();
                    if (!reply.body.empty()) {
                        resp.body.assign(reply.body.begin(), reply.body.end());
                        resp.hasBody = true;
                    }
                    resp.extFields = reply.ext;
                } else {
                    // Unscripted codes answer SUCCESS with nothing: enough for the route
                    // refresh and the heartbeat noise the client instance generates.
                    resp.code = ResponseCode::SUCCESS;
                }
            }
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<int32_t, std::vector<ScriptedReply>> scripts_;
    std::map<int32_t, std::vector<WireRecord>> records_;
    std::map<int32_t, int> arrivals_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- assertions on recorded state

void expectExt(const PropertyMap& ext,
               const std::vector<std::pair<std::string, std::string>>& want,
               const std::string& name) {
    expectInt(static_cast<long long>(ext.size()), static_cast<long long>(want.size()),
              name + ".extFieldCount");
    for (const auto& kv : want) {
        auto it = ext.find(kv.first);
        expect(it != ext.end() && it->second == kv.second, name + ".ext." + kv.first,
               it == ext.end() ? std::string("(absent)") : it->second);
    }
}

const std::string kTopic = "OpsTopic";
const std::string kGroup = "GID_OpsTopic";

// ---------------------------------------------------------------- 308

void testQueryCorrectionOffsetHeaderAndBody(DefaultMQAdminExt& admin, RecordingListener& broker) {
    broker.script(RequestCode::QUERY_CORRECTION_OFFSET,
                  {{ResponseCode::SUCCESS, "",
                    "{\"correctionOffsets\":{\"0\":110,\"3\":407}}"}});
    std::set<std::string> filters = {"GID_x", "GID_other"};
    std::map<int32_t, int64_t> out =
        admin.queryCorrectionOffset(broker.address(), kTopic, kGroup, filters);

    expectInt(broker.requestCount(RequestCode::QUERY_CORRECTION_OFFSET), 1, "308.sent");
    const WireRecord rec = broker.record(RequestCode::QUERY_CORRECTION_OFFSET, 0);
    expectInt(rec.code, RequestCode::QUERY_CORRECTION_OFFSET, "308.requestCode");
    // Java joins the Set itself, comma-joined; a std::set iterates in sorted order
    expectExt(rec.ext, {{"topic", kTopic},
                        {"compareGroup", kGroup},
                        {"filterGroups", "GID_other,GID_x"}},
              "308.threeFields");
    expect(rec.body.empty(), "308.carriesNoBody", rec.body);
    expectInt(static_cast<long long>(out.size()), 2, "308.decoded.size");
    expectInt(out.count(0) ? out.at(0) : -1, 110, "308.decoded.queue0");
    expectInt(out.count(3) ? out.at(3) : -1, 407, "308.decoded.queue3");
}

void testQueryCorrectionOffsetOmitsEmptyFilterGroups(DefaultMQAdminExt& admin,
                                                     RecordingListener& broker) {
    broker.script(RequestCode::QUERY_CORRECTION_OFFSET,
                  {{ResponseCode::SUCCESS, "", "{\"correctionOffsets\":{}}"}});
    std::map<int32_t, int64_t> out = admin.queryCorrectionOffset(broker.address(), kTopic, kGroup);

    const WireRecord rec = broker.record(RequestCode::QUERY_CORRECTION_OFFSET, 0);
    expectExt(rec.ext, {{"topic", kTopic}, {"compareGroup", kGroup}}, "308.noFilterGroups");
    expect(rec.ext.find("filterGroups") == rec.ext.end(), "308.emptySetOmitsField");
    expect(out.empty(), "308.emptyTableDecodesEmpty");
}

void testQueryCorrectionOffsetVipRewrite(DefaultMQAdminExt& admin, RecordingListener& broker,
                                         RecordingListener& decoy) {
    // With the switch on, the request must land two ports below the plain address: the decoy
    // is listening exactly there, so a shifted arrival is recorded instead of a silent miss.
    broker.script(RequestCode::QUERY_CORRECTION_OFFSET,
                  {{ResponseCode::SUCCESS, "", "{\"correctionOffsets\":{}}"}});
    decoy.script(RequestCode::QUERY_CORRECTION_OFFSET,
                 {{ResponseCode::SUCCESS, "", "{\"correctionOffsets\":{}}"}});

    admin.setVipChannelEnabled(true);
    int before = broker.requestCount(RequestCode::QUERY_CORRECTION_OFFSET);
    admin.queryCorrectionOffset(broker.address(), kTopic, kGroup);
    admin.setVipChannelEnabled(false);

    expectInt(decoy.requestCount(RequestCode::QUERY_CORRECTION_OFFSET), 1, "308.vip.landsOnPortMinus2");
    expectInt(broker.requestCount(RequestCode::QUERY_CORRECTION_OFFSET), before,
              "308.vip.plainPortUntouchedByVipCall");
    const WireRecord rec = decoy.record(RequestCode::QUERY_CORRECTION_OFFSET, 0);
    expectExt(rec.ext, {{"topic", kTopic}, {"compareGroup", kGroup}}, "308.vip.sameHeader");
}

// ---------------------------------------------------------------- 311/312/313

void testUnitTopicListFamily(DefaultMQAdminExt& admin, RecordingListener& namesrv) {
    const std::string body = "{\"topicList\":[\"TopicA\",\"%RETRY%G\"]}";
    const std::pair<int32_t, TopicList (DefaultMQAdminExt::*)(bool, int32_t)> family[3] = {
        {RequestCode::GET_UNIT_TOPIC_LIST, &DefaultMQAdminExt::getUnitTopicList},
        {RequestCode::GET_HAS_UNIT_SUB_TOPIC_LIST, &DefaultMQAdminExt::getHasUnitSubTopicList},
        {RequestCode::GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST,
         &DefaultMQAdminExt::getHasUnitSubUnUnitTopicList},
    };
    for (const auto& entry : family) {
        namesrv.script(entry.first, {{ResponseCode::SUCCESS, "", body}});
        TopicList list = (admin.*entry.second)(true, 5000);

        expectInt(namesrv.requestCount(entry.first), 1, "unitFamily.sent");
        const WireRecord rec = namesrv.record(entry.first, 0);
        expectInt(rec.code, entry.first, "unitFamily.requestCode");
        // Java invokeSync(null, ...): the request goes to a name server with NO header at all
        expectExt(rec.ext, {}, "unitFamily.nullHeader");
        expect(rec.body.empty(), "unitFamily.noRequestBody", rec.body);
        expectInt(static_cast<long long>(list.topicList.size()), 2, "unitFamily.decoded.count");
        expectStr(list.topicList[0], "TopicA", "unitFamily.decoded[0]");
        expectStr(list.topicList[1], "%RETRY%G", "unitFamily.decoded[1]");
    }
}

void testUnitTopicListFiltersRetryLocally(DefaultMQAdminExt& admin, RecordingListener& namesrv) {
    namesrv.script(RequestCode::GET_UNIT_TOPIC_LIST,
                   {{ResponseCode::SUCCESS, "",
                     "{\"topicList\":[\"TopicA\",\"%RETRY%G\",\"%RETRY%G_lite\"]}"}});
    TopicList list = admin.getUnitTopicList(false, 5000);

    const WireRecord rec = namesrv.record(RequestCode::GET_UNIT_TOPIC_LIST, 0);
    // The raw answer is untouched on the wire: the filter happens after decoding
    expectExt(rec.ext, {}, "unitFilter.nullHeader");
    expectInt(static_cast<long long>(list.topicList.size()), 1, "unitFilter.retryStripped");
    expectStr(list.topicList[0], "TopicA", "unitFilter.remainingTopic");
}

void testUnitTopicListFailureThrows(DefaultMQAdminExt& admin, RecordingListener& namesrv) {
    namesrv.script(RequestCode::GET_HAS_UNIT_SUB_TOPIC_LIST,
                   {{ResponseCode::SYSTEM_ERROR, "boom", ""}});
    int threw = 0;
    try {
        admin.getHasUnitSubTopicList(true, 5000);
    } catch (const MQClientException& e) {
        threw = 1;
        expectInt(e.getResponseCode(), ResponseCode::SYSTEM_ERROR, "unitFailure.code");
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "unitFailure.throwsClientException");
}

// ---------------------------------------------------------------- 328

void testGetAllProducerInfo(DefaultMQAdminExt& admin, RecordingListener& broker,
                            RecordingListener& decoy) {
    const std::string body =
        "{\"data\":{\"G\":[{\"clientId\":\"cid-1\",\"remoteIP\":\"10.0.0.1\","
        "\"language\":\"JAVA\",\"version\":500,\"lastUpdateTimestamp\":1700000000000}]}}";
    broker.script(RequestCode::GET_ALL_PRODUCER_INFO, {{ResponseCode::SUCCESS, "", body}});
    ProducerTableInfo table = admin.getAllProducerInfo(broker.address());

    expectInt(broker.requestCount(RequestCode::GET_ALL_PRODUCER_INFO), 1, "328.sent");
    const WireRecord rec = broker.record(RequestCode::GET_ALL_PRODUCER_INFO, 0);
    expectInt(rec.code, RequestCode::GET_ALL_PRODUCER_INFO, "328.requestCode");
    expectExt(rec.ext, {}, "328.emptyHeader");
    expect(rec.body.empty(), "328.noRequestBody", rec.body);
    expectInt(static_cast<long long>(table.data.size()), 1, "328.decoded.groups");
    if (table.data.count("G")) {
        expectInt(static_cast<long long>(table.data.at("G").size()), 1, "328.decoded.producers");
        if (!table.data.at("G").empty()) {
            const ProducerInfo& p = table.data.at("G")[0];
            expectStr(p.clientId, "cid-1", "328.decoded.clientId");
            expectStr(p.remoteIP, "10.0.0.1", "328.decoded.remoteIP");
            expectStr(p.language, "JAVA", "328.decoded.language");
            expectInt(p.version, 500, "328.decoded.version");
            expectInt(p.lastUpdateTimestamp, 1700000000000LL, "328.decoded.lastUpdate");
        }
    } else {
        expect(false, "328.decoded.groups", "missing group G");
    }

    // VIP rewrite: the producer table is in the same rewritten family as 308
    decoy.script(RequestCode::GET_ALL_PRODUCER_INFO,
                 {{ResponseCode::SUCCESS, "", "{\"data\":{}}"}});
    admin.setVipChannelEnabled(true);
    admin.getAllProducerInfo(broker.address());
    admin.setVipChannelEnabled(false);
    expectInt(decoy.requestCount(RequestCode::GET_ALL_PRODUCER_INFO), 1, "328.vip.landsOnPortMinus2");
}

void testGetAllProducerInfoEmptyBodyFails(DefaultMQAdminExt& admin, RecordingListener& broker) {
    broker.script(RequestCode::GET_ALL_PRODUCER_INFO, {{ResponseCode::SUCCESS, "", ""}});
    int threw = 0;
    try {
        admin.getAllProducerInfo(broker.address());
    } catch (const MQBrokerException&) {
        threw = 1;
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "328.emptyBody.throwsBrokerException");
}

// ---------------------------------------------------------------- 907

void testGetBrokerHAStatus(DefaultMQAdminExt& admin, RecordingListener& broker) {
    const std::string body =
        "{\"master\":true,\"masterCommitLogMaxOffset\":9001,\"inSyncSlaveNums\":2,"
        "\"haConnectionInfo\":[{\"addr\":\"10.0.0.2:10912\",\"slaveAckOffset\":8900,"
        "\"diff\":101,\"inSync\":true,\"transferredByteInSecond\":2048,"
        "\"transferFromWhere\":8700}],"
        "\"haClientRuntimeInfo\":{\"masterAddr\":\"10.0.0.1:10912\","
        "\"transferredByteInSecond\":12,\"maxOffset\":9001,\"lastReadTimestamp\":111,"
        "\"lastWriteTimestamp\":222,\"masterFlushOffset\":8800,\"activated\":true}}";
    broker.script(RequestCode::GET_BROKER_HA_STATUS, {{ResponseCode::SUCCESS, "", body}});
    HARuntimeInfo info = admin.getBrokerHAStatus(broker.address());

    expectInt(broker.requestCount(RequestCode::GET_BROKER_HA_STATUS), 1, "907.sent");
    const WireRecord rec = broker.record(RequestCode::GET_BROKER_HA_STATUS, 0);
    expectInt(rec.code, RequestCode::GET_BROKER_HA_STATUS, "907.requestCode");
    expectExt(rec.ext, {}, "907.nullHeader");
    expect(rec.body.empty(), "907.noRequestBody", rec.body);
    expect(info.master, "907.decoded.master");
    expectInt(info.masterCommitLogMaxOffset, 9001, "907.decoded.masterCommitLogMaxOffset");
    expectInt(info.inSyncSlaveNums, 2, "907.decoded.inSyncSlaveNums");
    expectInt(static_cast<long long>(info.haConnectionInfo.size()), 1, "907.decoded.connections");
    if (!info.haConnectionInfo.empty()) {
        const HAConnectionRuntimeInfo& c = info.haConnectionInfo[0];
        expectStr(c.addr, "10.0.0.2:10912", "907.decoded.conn.addr");
        expectInt(c.slaveAckOffset, 8900, "907.decoded.conn.slaveAckOffset");
        expectInt(c.diff, 101, "907.decoded.conn.diff");
        expect(c.inSync, "907.decoded.conn.inSync");
        expectInt(c.transferredByteInSecond, 2048, "907.decoded.conn.transferred");
        expectInt(c.transferFromWhere, 8700, "907.decoded.conn.transferFromWhere");
    }
    expectStr(info.haClientRuntimeInfo.masterAddr, "10.0.0.1:10912", "907.decoded.client.masterAddr");
    expectInt(info.haClientRuntimeInfo.maxOffset, 9001, "907.decoded.client.maxOffset");
    expectInt(info.haClientRuntimeInfo.masterFlushOffset, 8800, "907.decoded.client.masterFlushOffset");
    expect(info.haClientRuntimeInfo.activated, "907.decoded.client.activated");
}

void testGetBrokerHAStatusPlainAddressWithVipOn(DefaultMQAdminExt& admin, RecordingListener& broker,
                                                RecordingListener& decoy) {
    // 907 is outside the rewritten family: even with the VIP switch on it must stay on the
    // plain address (a misrouted call would land on the decoy instead of the broker).
    // Note script() resets the recorded count for 907, so after this call it is exactly 1.
    broker.script(RequestCode::GET_BROKER_HA_STATUS,
                  {{ResponseCode::SUCCESS, "", "{\"master\":false}"}});
    int before = decoy.requestCount(RequestCode::GET_BROKER_HA_STATUS);
    admin.setVipChannelEnabled(true);
    HARuntimeInfo info = admin.getBrokerHAStatus(broker.address());
    admin.setVipChannelEnabled(false);
    expect(!info.master, "907.vipOn.staysOnPlainAddress.decoded");
    expectInt(decoy.requestCount(RequestCode::GET_BROKER_HA_STATUS) - before, 0,
              "907.vipOn.noArrivalOnVipPort");
    expectInt(broker.requestCount(RequestCode::GET_BROKER_HA_STATUS), 1, "907.vipOn.brokerCount");
}

// ---------------------------------------------------------------- 908

void testResetMasterFlushOffset(DefaultMQAdminExt& admin, RecordingListener& broker) {
    broker.script(RequestCode::RESET_MASTER_FLUSH_OFFSET, {{ResponseCode::SUCCESS, "", ""}});
    admin.resetMasterFlushOffset(broker.address(), 12345);

    expectInt(broker.requestCount(RequestCode::RESET_MASTER_FLUSH_OFFSET), 1, "908.sent");
    const WireRecord rec = broker.record(RequestCode::RESET_MASTER_FLUSH_OFFSET, 0);
    expectInt(rec.code, RequestCode::RESET_MASTER_FLUSH_OFFSET, "908.requestCode");
    expectExt(rec.ext, {{"masterFlushOffset", "12345"}}, "908.primitiveLong");
    expect(rec.body.empty(), "908.noRequestBody", rec.body);

    // 0 is a real value and must be sent, never dropped as "unset"
    admin.resetMasterFlushOffset(broker.address(), 0);
    const WireRecord rec2 = broker.record(RequestCode::RESET_MASTER_FLUSH_OFFSET, 1);
    expectExt(rec2.ext, {{"masterFlushOffset", "0"}}, "908.zeroStillSent");
}

void testResetMasterFlushOffsetFailureThrows(DefaultMQAdminExt& admin, RecordingListener& broker) {
    broker.script(RequestCode::RESET_MASTER_FLUSH_OFFSET,
                  {{ResponseCode::SYSTEM_ERROR, "still syncing", ""}});
    int threw = 0;
    try {
        admin.resetMasterFlushOffset(broker.address(), 99);
    } catch (const MQBrokerException& e) {
        threw = 1;
        expectInt(e.getResponseCode(), ResponseCode::SYSTEM_ERROR, "908.failure.code");
        expectStr(e.getResponseMessage(), "still syncing", "908.failure.remark");
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "908.failure.throwsBrokerException");
}

// ---------------------------------------------------------------- 1002

void testElectMasterTwoPhase(DefaultMQAdminExt& admin, RecordingListener& controller) {
    controller.script(RequestCode::CONTROLLER_GET_METADATA_INFO,
                      {{ResponseCode::SUCCESS, "", "",
                        {{"group", "default"},
                         {"controllerLeaderId", "1"},
                         {"controllerLeaderAddress", controller.address()},
                         {"isLeader", "true"},
                         {"peers", "1: " + controller.address()}}}});
    controller.script(RequestCode::CONTROLLER_ELECT_MASTER,
                      {{ResponseCode::SUCCESS, "",
                        "{\"cluster\":\"DefaultCluster\",\"brokerName\":\"broker-a\","
                        "\"brokerAddrs\":{\"0\":\"10.0.0.1:10911\",\"1\":\"10.0.0.2:10911\"}}",
                        {{"masterBrokerId", "1"},
                         {"masterAddress", "10.0.0.2:10911"},
                         {"masterEpoch", "3"},
                         {"syncStateSetEpoch", "7"}}}});

    ElectMasterResult result = admin.electMaster(controller.address(), "DefaultCluster", "broker-a", 1);

    // step 1: the metadata read names the leader; step 2 lands on the leader
    expectInt(controller.requestCount(RequestCode::CONTROLLER_GET_METADATA_INFO), 1, "1002.meta");
    expectInt(controller.requestCount(RequestCode::CONTROLLER_ELECT_MASTER), 1, "1002.elect");
    const WireRecord meta = controller.record(RequestCode::CONTROLLER_GET_METADATA_INFO, 0);
    expectExt(meta.ext, {}, "1002.meta.nullHeader");
    const WireRecord rec = controller.record(RequestCode::CONTROLLER_ELECT_MASTER, 0);
    expectInt(rec.code, RequestCode::CONTROLLER_ELECT_MASTER, "1002.elect.requestCode");
    // ofAdminTrigger shape: five fields, designateElect forced true, invokeTime = now
    expectInt(static_cast<long long>(rec.ext.size()), 5, "1002.elect.extFieldCount");
    auto expectField = [&](const char* key, const std::string& want, const std::string& name) {
        auto it = rec.ext.find(key);
        expect(it != rec.ext.end() && it->second == want, name,
               it == rec.ext.end() ? std::string("(absent)") : it->second);
    };
    expectField("clusterName", "DefaultCluster", "1002.elect.ext.clusterName");
    expectField("brokerName", "broker-a", "1002.elect.ext.brokerName");
    expectField("brokerId", "1", "1002.elect.ext.brokerId");
    expectField("designateElect", "true", "1002.elect.ext.designateElect");
    {
        auto it = rec.ext.find("invokeTime");
        expect(it != rec.ext.end(), "1002.elect.ext.invokeTime");
        long long invokeTime = 0;
        if (it != rec.ext.end()) invokeTime = std::atoll(it->second.c_str());
        expect(invokeTime > 0, "1002.elect.invokeTimePositive",
               it == rec.ext.end() ? std::string() : it->second);
    }
    expect(rec.body.empty(), "1002.elect.noRequestBody", rec.body);
    // response header comes back from the extFields
    expectInt(result.header.masterBrokerId, 1, "1002.response.masterBrokerId");
    expectStr(result.header.masterAddress, "10.0.0.2:10911", "1002.response.masterAddress");
    expectInt(result.header.masterEpoch, 3, "1002.response.masterEpoch");
    expectInt(result.header.syncStateSetEpoch, 7, "1002.response.syncStateSetEpoch");
    // response body BrokerMemberGroup with numeric-keyed brokerAddrs
    expectStr(result.memberGroup.cluster, "DefaultCluster", "1002.member.cluster");
    expectStr(result.memberGroup.brokerName, "broker-a", "1002.member.brokerName");
    expectInt(static_cast<long long>(result.memberGroup.brokerAddrs.size()), 2,
              "1002.member.addrCount");
    expectStr(result.memberGroup.brokerAddrs.count(0) ? result.memberGroup.brokerAddrs.at(0) : "",
              "10.0.0.1:10911", "1002.member.addr0");
    expectStr(result.memberGroup.brokerAddrs.count(1) ? result.memberGroup.brokerAddrs.at(1) : "",
              "10.0.0.2:10911", "1002.member.addr1");
}

void testElectMasterWithoutDesignatedIdSendsMinusOne(DefaultMQAdminExt& admin,
                                                     RecordingListener& controller) {
    // Re-script the metadata leg: script() consumes one reply per arrival, and this test
    // makes its own second 1005 round before the elect request goes out.
    controller.script(RequestCode::CONTROLLER_GET_METADATA_INFO,
                      {{ResponseCode::SUCCESS, "", "",
                        {{"group", "default"},
                         {"controllerLeaderId", "1"},
                         {"controllerLeaderAddress", controller.address()},
                         {"isLeader", "true"},
                         {"peers", "1: " + controller.address()}}}});
    controller.script(RequestCode::CONTROLLER_ELECT_MASTER,
                      {{ResponseCode::SUCCESS, "", "{}", {}}});
    admin.electMaster(controller.address(), "DefaultCluster", "broker-a");

    const WireRecord rec = controller.record(RequestCode::CONTROLLER_ELECT_MASTER, 0);
    expectInt(static_cast<long long>(rec.ext.size()), 5, "1002.noId.extFieldCount");
    auto expectField = [&](const char* key, const std::string& want, const std::string& name) {
        auto it = rec.ext.find(key);
        expect(it != rec.ext.end() && it->second == want, name,
               it == rec.ext.end() ? std::string("(absent)") : it->second);
    };
    expectField("clusterName", "DefaultCluster", "1002.noId.ext.clusterName");
    expectField("brokerName", "broker-a", "1002.noId.ext.brokerName");
    expectField("brokerId", "-1", "1002.noId.ext.brokerId");
    expectField("designateElect", "true", "1002.noId.ext.designateElect");
    expect(rec.body.empty(), "1002.noId.emptyBodyStillSent", rec.body);
}

void testElectMasterFailureThrows(DefaultMQAdminExt& admin, RecordingListener& controller) {
    // The metadata leg is re-scripted for the same reason: every 1005 arrival consumes one reply
    controller.script(RequestCode::CONTROLLER_GET_METADATA_INFO,
                      {{ResponseCode::SUCCESS, "", "",
                        {{"group", "default"},
                         {"controllerLeaderId", "1"},
                         {"controllerLeaderAddress", controller.address()},
                         {"isLeader", "true"},
                         {"peers", "1: " + controller.address()}}}});
    controller.script(RequestCode::CONTROLLER_ELECT_MASTER,
                      {{ResponseCode::SYSTEM_ERROR, "elect failed", ""}});
    int threw = 0;
    try {
        admin.electMaster(controller.address(), "DefaultCluster", "broker-a");
    } catch (const MQBrokerException& e) {
        threw = 1;
        expectInt(e.getResponseCode(), ResponseCode::SYSTEM_ERROR, "1002.failure.code");
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "1002.failure.throwsBrokerException");
}

// ---------------------------------------------------------------- 1007

void testGetBrokerEpochCache(DefaultMQAdminExt& admin, RecordingListener& broker) {
    // The second epoch entry has no endOffset: the decoder must fill Java's
    // LAST_EPOCH_END_OFFSET default (Long.MAX_VALUE), not a padded zero.
    const std::string body =
        "{\"clusterName\":\"DefaultCluster\",\"brokerName\":\"broker-a\",\"brokerId\":1,"
        "\"epochList\":[{\"epoch\":1,\"startOffset\":0,\"endOffset\":500},"
        "{\"epoch\":2,\"startOffset\":500}],\"maxOffset\":900}";
    broker.script(RequestCode::GET_BROKER_EPOCH_CACHE, {{ResponseCode::SUCCESS, "", body}});
    EpochEntryCache cache = admin.getBrokerEpochCache(broker.address());

    expectInt(broker.requestCount(RequestCode::GET_BROKER_EPOCH_CACHE), 1, "1007.sent");
    const WireRecord rec = broker.record(RequestCode::GET_BROKER_EPOCH_CACHE, 0);
    expectInt(rec.code, RequestCode::GET_BROKER_EPOCH_CACHE, "1007.requestCode");
    expectExt(rec.ext, {}, "1007.nullHeader");
    expect(rec.body.empty(), "1007.noRequestBody", rec.body);
    expectStr(cache.clusterName, "DefaultCluster", "1007.decoded.clusterName");
    expectStr(cache.brokerName, "broker-a", "1007.decoded.brokerName");
    expectInt(cache.brokerId, 1, "1007.decoded.brokerId");
    expectInt(cache.maxOffset, 900, "1007.decoded.maxOffset");
    expectInt(static_cast<long long>(cache.epochList.size()), 2, "1007.decoded.epochCount");
    if (cache.epochList.size() == 2) {
        expectInt(cache.epochList[0].epoch, 1, "1007.decoded[0].epoch");
        expectInt(cache.epochList[0].startOffset, 0, "1007.decoded[0].startOffset");
        expectInt(cache.epochList[0].endOffset, 500, "1007.decoded[0].endOffset");
        expectInt(cache.epochList[1].epoch, 2, "1007.decoded[1].epoch");
        expectInt(cache.epochList[1].startOffset, 500, "1007.decoded[1].startOffset");
        expectInt(cache.epochList[1].endOffset, 9223372036854775807LL,
                  "1007.decoded[1].missingEndOffsetDefaultsToLongMax");
    }
}

// ---------------------------------------------------------------- 5001

void testSwitchTimerEngine(DefaultMQAdminExt& admin, RecordingListener& broker) {
    broker.script(RequestCode::SWITCH_TIMER_ENGINE, {{ResponseCode::SUCCESS, "", ""}});
    admin.switchTimerEngine(broker.address(), "rocksdb", 4500);

    expectInt(broker.requestCount(RequestCode::SWITCH_TIMER_ENGINE), 1, "5001.sent");
    const WireRecord rec = broker.record(RequestCode::SWITCH_TIMER_ENGINE, 0);
    expectInt(rec.code, RequestCode::SWITCH_TIMER_ENGINE, "5001.requestCode");
    // the engine type is the single ext field E_T (MessageConst.TIMER_ENGINE_TYPE)
    expectExt(rec.ext, {{"E_T", "rocksdb"}}, "5001.singleField");
    expect(rec.body.empty(), "5001.noRequestBody", rec.body);
}

void testSwitchTimerEngineFailureThrows(DefaultMQAdminExt& admin, RecordingListener& broker) {
    broker.script(RequestCode::SWITCH_TIMER_ENGINE,
                  {{ResponseCode::SYSTEM_ERROR, "engine busy", ""}});
    int threw = 0;
    try {
        admin.switchTimerEngine(broker.address(), "rocksdb");
    } catch (const MQBrokerException&) {
        threw = 1;
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "5001.failure.throwsBrokerException");
}

}  // namespace

int main() {
    // Bind the pair first: the decoy sits exactly two ports below the primary. The decoy is
    // both the name server (311/312/313 target) and the VIP fast channel (308/328 rewrite
    // target); the primary is the broker / controller address the caller passes in.
    std::unique_ptr<RecordingListener> primary;
    std::unique_ptr<RecordingListener> decoy;
    for (int attempt = 0; attempt < 16; ++attempt) {
        primary.reset(new RecordingListener());
        if (!primary->ok() || primary->port() <= 1024) {
            primary.reset();
            continue;
        }
        decoy.reset(new RecordingListener(static_cast<uint16_t>(primary->port() - 2)));
        if (decoy->ok()) break;
        primary.reset();
        decoy.reset();
    }
    if (!primary || !decoy || !decoy->ok()) {
        std::printf("FAIL unable to bind the loopback listener pair\n");
        return 1;
    }

    DefaultMQAdminExt admin("OpsRpcTest");
    // The name server is the decoy: unit-topic requests that reach it prove name-server
    // addressing, and VIP-rewritten 308/328 requests that reach it prove the port-2 rewrite.
    admin.setNamesrvAddr(decoy->address());
    admin.setTimeoutMillis(5000);
    try {
        admin.start();
    } catch (const std::exception& e) {
        std::printf("FAIL admin.start: %s\n", e.what());
        return 1;
    }

    // The VIP switch is off for the whole run except the two rewrite cases
    testQueryCorrectionOffsetHeaderAndBody(admin, *primary);
    testQueryCorrectionOffsetOmitsEmptyFilterGroups(admin, *primary);
    testUnitTopicListFamily(admin, *decoy);
    testUnitTopicListFiltersRetryLocally(admin, *decoy);
    testUnitTopicListFailureThrows(admin, *decoy);
    testGetAllProducerInfo(admin, *primary, *decoy);
    testGetAllProducerInfoEmptyBodyFails(admin, *primary);
    testGetBrokerHAStatus(admin, *primary);
    testGetBrokerHAStatusPlainAddressWithVipOn(admin, *primary, *decoy);
    testResetMasterFlushOffset(admin, *primary);
    testResetMasterFlushOffsetFailureThrows(admin, *primary);
    testElectMasterTwoPhase(admin, *primary);
    testElectMasterWithoutDesignatedIdSendsMinusOne(admin, *primary);
    testElectMasterFailureThrows(admin, *primary);
    testGetBrokerEpochCache(admin, *primary);
    testSwitchTimerEngine(admin, *primary);
    testSwitchTimerEngineFailureThrows(admin, *primary);
    testQueryCorrectionOffsetVipRewrite(admin, *primary, *decoy);

    admin.shutdown();

    std::printf("admin_ops_rpc: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
