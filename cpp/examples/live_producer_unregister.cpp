// 生产者退出时的 UNREGISTER_CLIENT(35) 真机验证。
// 用法：rmq_live_producer_unregister 127.0.0.1:9876
//
// 与 Python 的 verify_producer_unregister_live.py（U1~U6）、Rust 的
// live_producer.rs P11、.NET 的 ProducerUnregisterLive.cs 对齐。
//
// Java 的 `DefaultMQProducerImpl#shutdown`:313 调 `mQClientFactory.unregisterProducer(group)`
// （`MQClientInstance`:1198-1201），后者进私有的 `unregisterClient(group, null)`:1158-1182：
// 给 `brokerAddrTable` 里**每台 broker（含 slave）**同步发一发 code 35，超时
// `getMqClientApiTimeout()`（3000ms），任何异常只 `log.warn`。
// `MQClientAPIImpl#unregisterClient`:1615-1639 组的头是
// `UnregisterClientRequestHeader{clientID, producerGroup, consumerGroup}` —— 键名是大写 ID 的
// `clientID`，生产者退出时 `consumerGroup` 传 null（**整字段不上线**）。
// broker 端 `ClientManageProcessor#unregisterClient`:213-249 判的是 `group != null`：
// 空串会被当成「真有个空组名」去查 `""` 的订阅组配置，所以这里必须盯住字段有没有上线，
// 而不是只盯值。
//
// 本脚本证四件事：
//   U1  生产者起来并真的发了消息（组注册的前置条件）。
//   U2  204 `GET_PRODUCER_CONNECTION_LIST` 能看到本 clientId —— 注册确实发生过，
//       「消失」才有意义。注册靠心跳上线（30s 一轮），所以要轮询等。
//   U3  `shutdown()` 期间钩子抓到 code 35：每台已知 broker 各一发，头是
//       clientID + producerGroup，`consumerGroup` 不上线，且排在业务发送之后。
//   U5  紧接着查 204：这个组已经不在了（broker 回 SYSTEM_ERROR
//       `the producer group[...] not exist`，Java 的 mqadmin 也这么判）。
//   U6  对照组（另一个没退出的生产者组）仍在 —— 排掉「broker 把所有连接都清了」这种假阳性。
//
// ⚠ 判据强度：C++（和 Python/.NET）里每个生产者各自持有一份 `MQClientInstance`、各自一条
// TCP 连接，退出时连接也会关掉，broker 的通道扫描同样会把组摘掉 —— 单看 U5 分不出是 35
// 还是断连的功劳，所以这里必须由钩子抓帧（U3）直接证明「线上走了这一发」。行为级的判别式
// 证明在 `rust/examples/live_producer.rs` 的 P11：Rust 按 clientId 复用实例，两个同
// instanceName、不同组的生产者共用一条连接，先退的那个连接还活着，组能消失只可能是因为 35。
// 另：U4（每一发 35 都回 SUCCESS）在本移植**不可观测** —— 传输层有意不调
// `RPCHook#doAfterResponse`（见 remoting/remoting_client.h 的说明），U5 的 broker 侧效果
// 就是它的替代判据。
//
// 前置：NameServer + Broker 已起，``autoCreateTopicEnable=true``。
#include <algorithm>
#include <chrono>
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
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/rpchook.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::printf("  [%s] %s%s\n", ok ? "PASS" : "FAIL", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

std::string num(int32_t v) { return std::to_string(v); }

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return pred();
}

Bytes bytesOf(const std::string& s) { return Bytes(s.begin(), s.end()); }

std::string join(const std::vector<std::string>& v) {
    std::string out;
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) out += ",";
        out += v[i];
    }
    return out;
}

std::string extText(const PropertyMap& ext) {
    std::string out = "{";
    bool first = true;
    for (const auto& kv : ext) {
        if (!first) out += ", ";
        first = false;
        out += kv.first + "=" + kv.second;
    }
    return out + "}";
}

// 逐帧记录上线请求的探针。钩子跑在 encode() **之前**，此时头还挂在 customHeader 上
// （`makeCustomHeaderToNet` 是编码阶段的事，与 Java 同一时点），所以取它的 toExtFields()。
class UnregisterProbe : public RPCHook {
public:
    struct Frame {
        int32_t seq = 0;
        int32_t code = 0;
        std::string addr;
        PropertyMap ext;
    };

    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override {
        std::lock_guard<std::mutex> lk(mutex_);
        Frame f;
        f.seq = ++count_;
        f.code = request.code;
        f.addr = remoteAddr;
        f.ext = request.customHeader ? request.customHeader->toExtFields() : request.extFields;
        frames_.push_back(f);
    }

    std::vector<Frame> frames() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return frames_;
    }

    std::vector<Frame> of(int32_t code) const {
        std::vector<Frame> out;
        for (const Frame& f : frames()) {
            if (f.code == code) out.push_back(f);
        }
        return out;
    }

    // 最后一条业务发送 / 第一条 35 的序号，用来判「注销排在发送之后」。
    int32_t lastOf(const std::vector<int32_t>& codes) const {
        int32_t seq = -1;
        for (const Frame& f : frames()) {
            if (std::find(codes.begin(), codes.end(), f.code) != codes.end()) seq = f.seq;
        }
        return seq;
    }

private:
    mutable std::mutex mutex_;
    int32_t count_ = 0;
    std::vector<Frame> frames_;
};

// 204 看到的 clientId 列表；broker 说「组不存在」时返回空表（Java 的 mqadmin 同样把
// SYSTEM_ERROR 当「不在线」）。
std::vector<std::string> connectionClientIds(DefaultMQAdminExt& admin, const std::string& addr,
                                            const std::string& group) {
    try {
        const ProducerConnection pc = admin.examineProducerConnectionInfo(group, addr);
        std::vector<std::string> out;
        out.reserve(pc.connectionSet.size());
        for (const Connection& c : pc.connectionSet) out.push_back(c.clientId);
        return out;
    } catch (const std::exception& e) {
        const std::string msg = e.what();
        if (msg.find("not exist") != std::string::npos ||
            msg.find("not online") != std::string::npos) {
            return {};
        }
        std::printf("  [diag] examineProducerConnectionInfo(%s) 异常: %s\n", group.c_str(),
                    msg.c_str());
        return {};
    }
}

bool contains(const std::vector<std::string>& v, const std::string& want) {
    return std::find(v.begin(), v.end(), want) != v.end();
}

std::string extField(const UnregisterProbe::Frame& f, const std::string& key) {
    auto it = f.ext.find(key);
    return it == f.ext.end() ? std::string() : it->second;
}

// 按场景配好并启动一个生产者；退出作用域时 shutdown。
class ScopedProducer {
public:
    ScopedProducer(const std::string& group, const std::string& nsAddr, const std::string& kind,
                   const std::string& stamp, std::shared_ptr<RPCHook> hook = nullptr)
        : producer_(group) {
        producer_.setNamesrvAddr(nsAddr);
        producer_.setInstanceName(kind + "-" + stamp);
        producer_.setSendMsgTimeout(5000);
        if (hook) producer_.setRPCHook(std::move(hook));
        producer_.start();
    }
    ~ScopedProducer() {
        try {
            producer_.shutdown();
        } catch (const std::exception& e) {
            std::printf("  [diag] producer shutdown failed: %s\n", e.what());
        }
    }

    DefaultMQProducer& operator*() { return producer_; }

private:
    DefaultMQProducer producer_;
};

void runChecks(const std::string& nsAddr, const std::vector<std::string>& brokers,
               DefaultMQAdminExt& admin) {
    const std::string stamp = std::to_string(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    const std::string brokerAddr = brokers.empty() ? std::string() : brokers.front();
    const std::string topic = "Unreg_" + stamp;
    const std::string group = "PID_unreg_" + stamp;
    const std::string peerGroup = "PID_unreg_peer_" + stamp;

    auto probe = std::make_shared<UnregisterProbe>();
    ScopedProducer p(group, nsAddr, "unreg", stamp, probe);
    ScopedProducer peer(peerGroup, nsAddr, "unreg-peer", stamp);

    const SendResult r = (*p).send(Message(topic, bytesOf("unreg-probe")), 5000);
    check("U1 生产者发送成功", r.sendStatus == SendStatus::SEND_OK, "msgId=" + r.msgId);
    (*peer).send(Message(topic, bytesOf("peer")), 5000);

    const std::string clientId = (*p).clientId();
    std::vector<std::string> seen;
    const bool registered = waitUntil(
        [&] {
            seen = connectionClientIds(admin, brokerAddr, group);
            return contains(seen, clientId);
        },
        70000);
    check("U2 心跳后 204 能看到本 clientId", registered,
          "clientId=" + clientId + " 当前=" + join(seen));
    const std::vector<std::string> peerSeen =
        connectionClientIds(admin, brokerAddr, peerGroup);
    check("U2b 对照组注册可见（204 这条判据本身有效）",
          contains(peerSeen, (*peer).clientId()), "当前=" + join(peerSeen));

    check("U3 前置：还没退出时不应有 35", probe->of(RequestCode::UNREGISTER_CLIENT).empty(),
          "count=" + num(static_cast<int32_t>(probe->of(RequestCode::UNREGISTER_CLIENT).size())));

    // 注销走的是**还开着**的那条长连接：C++ 的 shutdown 顺序是先 35 再关客户端，
    // 这条顺序只有 broker 侧能验证（U5）。
    (*p).shutdown();

    const std::vector<UnregisterProbe::Frame> unregs =
        probe->of(RequestCode::UNREGISTER_CLIENT);
    check("U3 shutdown 给每台已知 broker 各发了一发 35",
          unregs.size() == brokers.size(),
          "count=" + num(static_cast<int32_t>(unregs.size())) +
              " brokers=" + num(static_cast<int32_t>(brokers.size())));
    bool shapeOk = !unregs.empty();
    for (const UnregisterProbe::Frame& f : unregs) {
        if (extField(f, "clientID") != clientId) shapeOk = false;
        if (extField(f, "producerGroup") != group) shapeOk = false;
        // Java 的 unregisterClient(group, null)：消费者槽位整个不上线
        if (f.ext.count("consumerGroup") != 0) shapeOk = false;
        if (!contains(brokers, f.addr)) shapeOk = false;
    }
    check("U3b 35 的头是 clientID + producerGroup，consumerGroup 不上线（Java 传 null）",
          shapeOk, unregs.empty() ? "没抓到帧" : ("addr=" + unregs.front().addr +
                                                 " ext=" + extText(unregs.front().ext)));
    const int32_t lastSend = probe->lastOf({RequestCode::SEND_MESSAGE, RequestCode::SEND_MESSAGE_V2});
    const int32_t firstUnreg = unregs.empty() ? -1 : unregs.front().seq;
    check("U3c 35 排在业务发送之后", lastSend >= 0 && firstUnreg > lastSend,
          "last_send=" + num(lastSend) + " first_unreg=" + num(firstUnreg));

    const bool gone = waitUntil(
        [&] { return connectionClientIds(admin, brokerAddr, group).empty(); }, 10000);
    check("U5 退出后 204 查不到这个组（broker 回 not exist）", gone,
          "当前=" + join(connectionClientIds(admin, brokerAddr, group)));

    const std::vector<std::string> peerAfter =
        connectionClientIds(admin, brokerAddr, peerGroup);
    check("U6 对照组仍在（排掉「broker 全清」这种假阳性）",
          contains(peerAfter, (*peer).clientId()), "当前=" + join(peerAfter));
}

}  // namespace

int main(int argc, char** argv) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    DefaultMQAdminExt admin{"UNREGADMIN"};
    admin.setNamesrvAddr(nsAddr);
    admin.setTimeoutMillis(10000);
    try {
        admin.start();
    } catch (const std::exception& e) {
        std::printf("admin start failed: %s\n", e.what());
        return 1;
    }

    // 已知 broker：主从都算，35 必须每台各一发（Java 遍历的是 brokerAddrTable）
    ClusterInfo cluster;
    for (int32_t i = 0; i < 40; ++i) {
        try {
            cluster = admin.fetchBrokerClusterInfo();
            if (!cluster.brokerAddrTable.empty()) break;
        } catch (const std::exception& e) {
            if (i == 39) std::printf("  [diag] cluster probe failed: %s\n", e.what());
        }
        if (!cluster.brokerAddrTable.empty()) break;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (cluster.brokerAddrTable.empty()) {
        check("集群探活", false, "nameServer 无 broker 注册");
        admin.shutdown();
        return 1;
    }
    std::vector<std::string> brokers;
    for (const auto& entry : cluster.brokerAddrTable) {
        for (const auto& addr : entry.second.brokerAddrs) {
            brokers.push_back(addr.second);
        }
    }
    check("集群探活", true, "brokers=" + join(brokers));

    try {
        runChecks(nsAddr, brokers, admin);
    } catch (const std::exception& e) {
        std::printf("  [FAIL] 用例抛出未捕获异常: %s\n", e.what());
        ++gFail;
    }

    try {
        admin.shutdown();
    } catch (const std::exception& e) {
        std::printf("  [diag] admin shutdown failed: %s\n", e.what());
    }
    std::printf("\n== 结果: %d/%d 通过 ==\n", gPass, gPass + gFail);
    return gFail == 0 ? 0 : 1;
}
