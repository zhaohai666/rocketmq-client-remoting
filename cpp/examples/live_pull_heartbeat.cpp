// 拉模式消费者心跳真机联调（#98）：对应 python/verify_pull_consumer_heartbeat_live.py 的 A0–A6。
//
// 为什么必须真机：离线单测能证明「报文形状对」，但证明不了 broker 的 ConsumerManager 真的
// 把本组登记进 consumerTable（broker 是按台建表的），也证明不了 35 注销之后立刻摘除。
// 这三个观测点只有真 broker 有：
//   * 203 examineConsumerConnectionInfo：组在不在、consumeType / consumeFromWhere /
//     messageModel / subscriptionTable 是什么（AdminBrokerProcessor:1971 读的就是这些）；
//   * 38 GET_CONSUMER_LIST_BY_GROUP：consumerTable 里的 clientId 列表（**裸 RPC**，
//     组不在时 broker 直接回 "no consumer for this group"，不是空列表）；
//   * shutdown → 35 之后 203 是否立刻查不到（不必等 ~120s 通道扫描）。
// 反面对照：从未心跳过的幽灵组必须查不到 —— 否则说明判据本身是空转的。
//
// 用法：./rmq_live_pull_heartbeat [namesrv] [masterAddr] [slaveAddr]
//   默认 127.0.0.1:9876 / 127.0.0.1:10911 /（无从节点）
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/body.h"

using namespace rocketmq;

namespace {

int gPass = 0;
int gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::printf("  %s %s%s\n", ok ? "[PASS]" : "[FAIL]", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

class NoopListener : public MessageQueueListener {
public:
    void messageQueueChanged(const std::string&, const std::vector<MessageQueue>&,
                             const std::vector<MessageQueue>&) override {}
};

// 203 的原始答案：在线返回 true；组不在时 broker 抛 MQBrokerException → 当不在线。
bool groupIsOnline(DefaultMQAdminExt& admin, const std::string& group, const std::string& addr,
                   ConsumerConnection& out, std::string& err) {
    try {
        out = admin.examineConsumerConnectionInfo(group, addr);
        return true;
    } catch (const std::exception& e) {
        err = e.what();
        return false;
    }
}

// 38 的原始答案：组不在时 broker 直接抛 "no consumer for this group"，当空列表。
std::vector<std::string> consumerIds(DefaultMQAdminExt& admin, const std::string& group,
                                     const std::string& addr, bool& ok) {
    ok = true;
    try {
        return admin.getConsumerListByGroup(group, addr).consumerIdList;
    } catch (const std::exception&) {
        ok = false;
        return {};
    }
}

std::string subStringOf(const ConsumerConnection& conn, const std::string& topic, bool& found) {
    found = false;
    const JsonValue* entry = conn.subscriptionTable.find(topic);
    if (entry == nullptr) return std::string();
    std::string s;
    if (entry->tryGetString("subString", s) || entry->tryGetString("sub_string", s)) {
        found = true;
        return s;
    }
    return std::string();
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    const std::string master = argc > 2 ? argv[2] : "127.0.0.1:10911";
    const std::string slave = argc > 3 ? argv[3] : std::string();
    const std::string stamp = std::to_string(nowMs() % 1000000);
    const std::string topic = "PullHbCpp_" + stamp;
    const std::string group = "GID_pullhb_cpp_" + stamp;
    const std::string ghost = "GID_pullhb_cpp_ghost_" + stamp;

    DefaultMQAdminExt admin("PullHbCppAdmin");
    admin.setNamesrvAddr(nsAddr);
    admin.start();
    DefaultMQPullConsumer* consumer = nullptr;
    std::printf("namesrv=%s master=%s topic=%s group=%s\n", nsAddr.c_str(), master.c_str(),
                topic.c_str(), group.c_str());

    try {
        // ---- A0 建 topic ----
        admin.createTopicInBroker(master, topic, 4, 4, 6);

        // ---- A1 起拉模式消费者并拉一轮 ----
        consumer = new DefaultMQPullConsumer(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setHeartbeatBrokerIntervalMillis(30000);
        auto listener = std::make_shared<NoopListener>();
        consumer->registerMessageQueueListener(topic, listener);
        consumer->start();
        const std::vector<MessageQueue> mqs = consumer->fetchSubscribeMessageQueues(topic);
        bool pulled = false;
        std::string pullStatus = "-";
        if (!mqs.empty()) {
            const PullResult r = consumer->pull(mqs[0], "*", 0, 32);
            pulled = true;
            pullStatus = pullStatusName(r.status);
        }
        check("A1 拉模式消费者启动并成功拉取一轮",
              consumer->heartbeatCount() >= 1 && pulled,
              "queues=" + std::to_string(mqs.size()) + " status=" + pullStatus +
                  " heartbeats=" + std::to_string(consumer->heartbeatCount()));

        // ---- A2 主节点 203 ----
        ConsumerConnection conn;
        std::string err;
        const bool online = groupIsOnline(admin, group, master, conn, err);
        check("A2 主节点 203 查到本组（心跳已注册）", online,
              "connections=" + std::to_string(conn.connectionSet.size()) +
                  (err.empty() ? "" : " err=" + err));
        check("A2 消费类型是 CONSUME_ACTIVELY（Java DefaultMQPullConsumerImpl:348）",
              online && conn.consumeType == ConsumeType::CONSUME_ACTIVELY,
              "consumeType=" + conn.consumeType);
        check("A2 消费位点是 CONSUME_FROM_LAST_OFFSET（:353）",
              online && conn.consumeFromWhere == ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
              "consumeFromWhere=" + conn.consumeFromWhere);
        check("A2 广播/集群口径是 CLUSTERING",
              online && conn.messageModel == MessageModel::CLUSTERING,
              "messageModel=" + conn.messageModel);

        // ---- A2b 订阅集来自 registerTopics（subscriptions():357-385）----
        bool found = false;
        const std::string subString = subStringOf(conn, topic, found);
        check("A2b 203 的订阅表带 registerTopics 的 topic 且 subString=*",
              found && subString == "*", "subscriptionTable=" + conn.subscriptionTable.dump());

        // ---- A3 38 主节点 ----
        bool idsOk = false;
        const std::vector<std::string> ids = consumerIds(admin, group, master, idsOk);
        bool hasSelf = false;
        for (const std::string& id : ids) {
            if (id == consumer->clientId()) hasSelf = true;
        }
        check("A3 主节点 38 查到本 clientId", idsOk && hasSelf,
              "ids=" + std::to_string(ids.size()) + " clientId=" + consumer->clientId());

        // ---- A4 从节点 ----
        if (!slave.empty()) {
            ConsumerConnection slaveConn;
            std::string slaveErr;
            const bool slaveOnline = groupIsOnline(admin, group, slave, slaveConn, slaveErr);
            check("A4 从节点 203 也查到本组（心跳扇出到从节点）", slaveOnline,
                  "slave=" + slave + " connections=" +
                      std::to_string(slaveConn.connectionSet.size()));
            bool slaveIdsOk = false;
            const std::vector<std::string> slaveIds = consumerIds(admin, group, slave, slaveIdsOk);
            bool slaveHasSelf = false;
            for (const std::string& id : slaveIds) {
                if (id == consumer->clientId()) slaveHasSelf = true;
            }
            check("A4 从节点 38 也查到本 clientId", slaveIdsOk && slaveHasSelf,
                  "ids=" + std::to_string(slaveIds.size()));
        }

        // ---- A5 对照：幽灵组（从未心跳）必须查不到 ----
        ConsumerConnection ghostConn;
        std::string ghostErr;
        const bool ghostOnline = groupIsOnline(admin, ghost, master, ghostConn, ghostErr);
        bool ghostIdsOk = false;
        const std::vector<std::string> ghostIds = consumerIds(admin, ghost, master, ghostIdsOk);
        check("A5 对照：未心跳的幽灵组 203 查不到", !ghostOnline, "err=" + ghostErr);
        check("A5 对照：未心跳的幽灵组 38 空列表", !ghostIdsOk || ghostIds.empty(),
              "ids=" + std::to_string(ghostIds.size()));

        // ---- A6 shutdown 立刻注销（35）----
        const std::string clientId = consumer->clientId();
        consumer->shutdown();
        delete consumer;
        consumer = nullptr;
        bool gone = false;
        const int64_t deadline = nowMs() + 10000;
        while (nowMs() < deadline) {
            ConsumerConnection c2;
            std::string e2;
            if (!groupIsOnline(admin, group, master, c2, e2)) {
                gone = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        check("A6 shutdown 后 203 立刻查不到本组（发过 35 注销）", gone, "clientId=" + clientId);
    } catch (const std::exception& e) {
        check("联调异常", false, e.what());
    }

    if (consumer != nullptr) {
        try {
            consumer->shutdown();
        } catch (const std::exception&) {
        }
        delete consumer;
    }
    try {
        admin.deleteTopicInBroker(master, topic);
    } catch (const std::exception&) {
    }
    admin.shutdown();
    std::printf("\n%d PASS / %d FAIL\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
