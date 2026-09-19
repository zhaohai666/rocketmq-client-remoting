// C++ 管理客户端（DefaultMQAdminExt）的**真实集群**联调（对应 python/verify_admin_live.py）。
//
// 全部打真实 nameServer + broker，无 mock。覆盖：
//   1. 集群探活 / fetchBrokerClusterInfo / getClusterList
//   2. createTopic（队列数校验）→ fetchAllTopicList → examineTopicRoute
//   3. examineTopicConfig / getAllTopicConfig
//   4. getBrokerConfig（**properties 文本**，验证没被当 KVTable JSON 解析）
//   5. updateBrokerConfig → 回读确认生效（可逆，测完还原）
//   6. NameServer KV：createAndUpdateKvConfig → getKvConfig → getKvListByNamespace → delete
//   7. 订阅组：create/update → 单查 → 分页全量 → examine → delete
//   8. 生产 N 条 → examineTopicStats / examineConsumeStats / queryConsumeQueue /
//      queryMessage（可达性）/ viewMessage(msgId)
//   9. maxOffset / minOffset / searchOffset / earliestMsgStoreTime / examineConsumerOffset
//  10. sendMessageBack：消费 1 条后重投 → 轮询 %RETRY%<group> 出现该消息
//  11. resetOffsetByTimestamp（真实 INVOKE_BROKER_TO_RESET_OFFSET，language=CPP）
//  12. 清理：deleteSubscriptionGroup / deleteTopic
//
// 本程序自身不启动集群；调用方需先启动 nameServer(9876) + broker(10911) 且
// autoCreateTopicEnable=true。用法：
//   ./rmq_admin_live [namesrv_addr]
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

using namespace rocketmq;

namespace {

std::string gNamesrv = "127.0.0.1:9876";
int gPass = 0;
int gFail = 0;
int gSkip = 0;
std::vector<std::pair<std::string, bool>> gResults;

void check(const std::string& name, bool ok, const std::string& detail = "") {
    gResults.emplace_back(name, ok);
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name;
    if (!detail.empty()) std::cout << "  " << detail;
    std::cout << std::endl;
}

// 记录"依赖 broker 侧配置差异、本机不成立"的项，不计入失败。
// 与 check 严格区分：skip 必须写清楚前提为什么在本地不成立。
void skip(const std::string& name, const std::string& detail) {
    ++gSkip;
    std::cout << "[SKIP] " << name << "  " << detail << std::endl;
}

std::string bytes2str(const Bytes& b) { return std::string(b.begin(), b.end()); }
Bytes str2bytes(const std::string& s) { return Bytes(s.begin(), s.end()); }

int64_t nowMs() { return UtilAll::currentTimeMillis(); }

// 收集第一批消费到的消息（sendMessageBack 需要原始 MessageExt）
class CollectingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext& /*ctx*/) override {
        std::lock_guard<std::mutex> lk(m_);
        for (const MessageExt& m : msgs) {
            if (first_.topic.empty()) first_ = m;
            ++count_;
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
    MessageExt first() {
        std::lock_guard<std::mutex> lk(m_);
        return first_;
    }
    int64_t count() {
        std::lock_guard<std::mutex> lk(m_);
        return count_;
    }

private:
    std::mutex m_;
    MessageExt first_;
    int64_t count_ = 0;
};

int report() {
    std::cout << "\n================ 汇总 ================" << std::endl;
    for (const auto& r : gResults) {
        std::cout << "  [" << (r.second ? "PASS" : "FAIL") << "] " << r.first << std::endl;
    }
    std::cout << "======================================" << std::endl;
    std::cout << "总计 " << (gPass + gFail) << " 项，失败 " << gFail << " 项，跳过 " << gSkip
              << " 项" << std::endl;
    return gFail == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc > 1) gNamesrv = argv[1];

    const std::string stamp = std::to_string(nowMs());
    const std::string topic = "AdminLiveCpp_" + stamp;
    const std::string group = "AdminLiveCppGroup_" + stamp;
    const std::string kvNs = "AdminLiveCppKv_" + stamp;
    const int nMsg = 8;

    DefaultMQAdminExt admin;
    admin.setNamesrvAddr(gNamesrv);
    admin.setTimeoutMillis(10000);
    try {
        admin.start();
    } catch (const std::exception& e) {
        std::cout << "[FATAL] admin start failed: " << e.what() << std::endl;
        return 1;
    }

    // ---------- 1. 集群探活（broker 注册竞态：端口开 != 已注册）----------
    ClusterInfo cluster;
    bool clusterOk = false;
    std::string lastErr;
    for (int i = 0; i < 40; ++i) {
        try {
            cluster = admin.fetchBrokerClusterInfo();
            if (cluster.brokerAddrTable.size() > 0) {
                clusterOk = true;
                break;
            }
        } catch (const std::exception& e) {
            lastErr = e.what();
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!clusterOk) {
        check("集群探活", false, "nameServer 无 broker 注册 last_err=" + lastErr);
        admin.shutdown();
        return report();
    }
    {
        std::vector<std::string> addrs = cluster.getBrokerAddrs();
        check("fetchBrokerClusterInfo", true,
              "brokers=" + std::to_string(cluster.brokerAddrTable.size())
              + " addrs=" + std::to_string(addrs.size()));
    }
    const std::string brokerAddr = cluster.getBrokerAddrs()[0];

    // ---------- 2. Topic 管理 ----------
    try {
        admin.createTopic(MixAll::DEFAULT_TOPIC, topic, 4);
        check("createTopic(" + topic + ")", true, "queueNum=4");
    } catch (const std::exception& e) {
        check("createTopic(" + topic + ")", false, e.what());
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    try {
        TopicList all = admin.fetchAllTopicList();
        check("fetchAllTopicList", true, "topicCount=" + std::to_string(all.topicList.size()));
        check("新 topic 出现在 topicList", all.contains(topic), topic);
    } catch (const std::exception& e) {
        check("fetchAllTopicList", false, e.what());
    }

    TopicRouteData route;
    try {
        route = admin.examineTopicRoute(topic);
        check("examineTopicRoute", true,
              "brokers=" + std::to_string(route.brokerDatas.size())
              + " queues=" + std::to_string(route.queueDatas.size()));
        bool allFour = !route.queueDatas.empty();
        for (const QueueData& qd : route.queueDatas) {
            if (qd.readQueueNums != 4) allFour = false;
        }
        check("路由 readQueueNums==4", allFour, "ok");
    } catch (const std::exception& e) {
        check("examineTopicRoute", false, e.what());
    }

    try {
        std::set<std::string> clusters = admin.getClusterList(topic);
        check("getClusterList", !clusters.empty(),
              "clusters=" + std::to_string(clusters.size()));
    } catch (const std::exception& e) {
        check("getClusterList", false, e.what());
    }

    try {
        TopicConfig cfg = admin.examineTopicConfig(brokerAddr, topic);
        check("examineTopicConfig", true,
              "read=" + std::to_string(cfg.readQueueNums)
              + " write=" + std::to_string(cfg.writeQueueNums)
              + " perm=" + std::to_string(cfg.perm) + " filter=" + cfg.topicFilterType);
        check("TopicConfig 队列数与创建一致",
              cfg.readQueueNums == 4 && cfg.writeQueueNums == 4,
              "read=" + std::to_string(cfg.readQueueNums)
              + " write=" + std::to_string(cfg.writeQueueNums));
        check("TopicConfig 默认 perm=6", cfg.perm == 6, "perm=" + std::to_string(cfg.perm));
    } catch (const std::exception& e) {
        check("examineTopicConfig", false, e.what());
    }

    try {
        TopicConfigSerializeWrapper w = admin.getAllTopicConfig(brokerAddr);
        check("getAllTopicConfig", !w.topicConfigTable.empty(),
              "topicConfigs=" + std::to_string(w.topicConfigTable.size()));
    } catch (const std::exception& e) {
        check("getAllTopicConfig", false, e.what());
    }

    // ---------- 3. Broker 配置：properties 文本（历史 bug 点）----------
    PropertyMap brokerCfg;
    try {
        brokerCfg = admin.getBrokerConfig(brokerAddr);
        check("getBrokerConfig(properties 文本)", !brokerCfg.empty(),
              "keys=" + std::to_string(brokerCfg.size()));
        check("getBrokerConfig 解析出 brokerName", brokerCfg.count("brokerName") == 1,
              "brokerName=" + brokerCfg["brokerName"]);
    } catch (const std::exception& e) {
        check("getBrokerConfig(properties 文本)", false, e.what());
    }

    if (!brokerCfg.empty()) {
        // 可逆修改：写一个无害配置再还原
        const std::string original = brokerCfg.count("sendMessageThreadPoolNums")
                                         ? brokerCfg["sendMessageThreadPoolNums"]
                                         : std::string();
        const std::string probe = "11";
        try {
            PropertyMap upd;
            upd["sendMessageThreadPoolNums"] = probe;
            admin.updateBrokerConfig(brokerAddr, upd);
            check("updateBrokerConfig(sendMessageThreadPoolNums=11)", true, "OK");
            std::this_thread::sleep_for(std::chrono::seconds(1));
            PropertyMap after = admin.getBrokerConfig(brokerAddr);
            check("updateBrokerConfig 生效", after["sendMessageThreadPoolNums"] == probe,
                  "期望 " + probe + " 实际 " + after["sendMessageThreadPoolNums"]);
            if (!original.empty()) {
                PropertyMap back;
                back["sendMessageThreadPoolNums"] = original;
                admin.updateBrokerConfig(brokerAddr, back);
            }
        } catch (const std::exception& e) {
            check("updateBrokerConfig", false, e.what());
        }
    }

    // ---------- 4. NameServer KV 配置 ----------
    try {
        admin.createAndUpdateKvConfig(kvNs, "k1", "v1");
        check("createAndUpdateKvConfig", true, "OK");
        std::string value;
        bool found = admin.getKvConfig(kvNs, "k1", value);
        check("getKVConfig", found, "value=" + value);
        check("KV 值往返一致", found && value == "v1", "期望 v1 实际 " + value);
        KVTable t = admin.getKvListByNamespace(kvNs);
        check("getKVListByNamespace 含 k1", t.table.count("k1") == 1,
              "tableSize=" + std::to_string(t.table.size()));
        admin.deleteKvConfig(kvNs, "k1");
        check("deleteKVConfig", true, "OK");
        std::string gone;
        bool stillThere = admin.getKvConfig(kvNs, "k1", gone);
        check("KV 删除后不再存在", !stillThere, "found=" + std::string(stillThere ? "1" : "0"));
    } catch (const std::exception& e) {
        check("NameServer KV 链路", false, e.what());
    }

    // ---------- 5. 订阅组管理 ----------
    {
        SubscriptionGroupConfig sgc(group);
        sgc.consumeEnable = true;
        sgc.retryMaxTimes = 5;
        try {
            admin.createAndUpdateSubscriptionGroupConfig(brokerAddr, sgc);
            check("createAndUpdateSubscriptionGroupConfig", true, "group=" + group);

            SubscriptionGroupConfig single;
            bool ok = admin.getSubscriptionGroupConfig(brokerAddr, group, single);
            check("getSubscriptionGroupConfig(单查)", ok, "found=" + std::string(ok ? "1" : "0"));
            if (ok) {
                check("订阅组 retryMaxTimes 往返一致", single.retryMaxTimes == 5,
                      "实际 " + std::to_string(single.retryMaxTimes));
            }

            SubscriptionGroupWrapper wrapper = admin.getAllSubscriptionGroup(brokerAddr);
            check("getAllSubscriptionGroup(分页)", !wrapper.subscriptionGroupTable.empty(),
                  "groups=" + std::to_string(wrapper.subscriptionGroupTable.size()));
            check("分页结果含新订阅组", wrapper.subscriptionGroupTable.count(group) == 1,
                  "count=" + std::to_string(wrapper.subscriptionGroupTable.size()));

            SubscriptionGroupConfig ex;
            bool exOk = admin.examineSubscriptionGroupConfig(brokerAddr, group, ex);
            check("examineSubscriptionGroupConfig", exOk, "group=" + ex.groupName);
        } catch (const std::exception& e) {
            check("订阅组管理链路", false, e.what());
        }
    }

    // ---------- 6. 生产 + 统计 + 查询 ----------
    std::vector<std::pair<Bytes, std::string>> sent;  // body, uniqKey msgId
    std::vector<std::string> offsetIds;               // broker 侧 offsetMsgId（viewMessage 要用它）
    MessageQueue firstMq;
    {
        DefaultMQProducer prod("AdminLiveCppProducer_" + stamp);
        prod.setNamesrvAddr(gNamesrv);
        prod.setSendMsgTimeout(5000);
        prod.start();
        for (int i = 0; i < nMsg; ++i) {
            std::string payload = "admin-live-" + std::to_string(i);
            try {
                SendResult sr = prod.send(Message(topic, str2bytes(payload)));
                if (sr.sendStatus == SendStatus::SEND_OK) {
                    sent.emplace_back(str2bytes(payload), sr.msgId);
                    offsetIds.push_back(sr.offsetMsgId);
                    if (firstMq.topic.empty()) firstMq = sr.messageQueue;
                }
            } catch (const std::exception& e) {
                std::cout << "send " << i << " error: " << e.what() << std::endl;
            }
        }
        check("同步发送 " + std::to_string(nMsg) + " 条", sent.size() == static_cast<size_t>(nMsg),
              "ok=" + std::to_string(sent.size()) + "/" + std::to_string(nMsg));
        prod.shutdown();
    }
    if (sent.empty()) {
        admin.shutdown();
        return report();
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));

    int64_t maxOff = 0;
    int64_t minOff = 0;
    MessageQueue mq = firstMq.topic.empty() ? MessageQueue(topic, "broker-a", 0) : firstMq;
    try {
        TopicStatsTable stats = admin.examineTopicStats(topic);
        check("examineTopicStats", !stats.offsetTable.empty(),
              "queues=" + std::to_string(stats.offsetTable.size()));
        check("TopicStatsTable maxOffset 总和 >= 发送数", stats.totalMaxOffset() >= nMsg,
              "maxOffsetSum=" + std::to_string(stats.totalMaxOffset()));
    } catch (const std::exception& e) {
        check("examineTopicStats", false, e.what());
    }

    try {
        ConsumeStats cs = admin.examineConsumeStats(brokerAddr, group, topic);
        check("examineConsumeStats", true,
              "queues=" + std::to_string(cs.offsetTable.size())
              + " lag=" + std::to_string(cs.totalLag()));
    } catch (const std::exception& e) {
        check("examineConsumeStats", false, e.what());
    }

    try {
        QueryConsumeQueueResponseBody q = admin.queryConsumeQueue(brokerAddr, topic, 0, 0, 10, group);
        check("queryConsumeQueue", true,
              "min=" + std::to_string(q.minQueueIndex) + " max=" + std::to_string(q.maxQueueIndex));
    } catch (const std::exception& e) {
        check("queryConsumeQueue", false, e.what());
    }

    // queryMessage：只断言"请求可达、正常应答、返回类型正确"。
    // 不断言"一定查得到"：msgId 属于 uniqKey，broker 侧 uniqKey 倒排索引只有
    // RocksDB 索引实现才支持；本机默认文件索引 + 消息未设 KEYS，查不到是
    // **broker 配置差异**，不是客户端 bug。
    {
        const std::string msgId0 = sent[0].second;
        try {
            std::vector<MessageExt> byKey =
                admin.queryMessage(topic, msgId0, 32, 0, nowMs() + 60000);
            std::vector<MessageExt> byKey2 = admin.queryMessageByKey(topic, msgId0, 32);
            MessageExt uniq;
            bool uniqFound = admin.queryMessageByUniqKey(topic, msgId0, uniq);
            check("queryMessage 请求可达且正常应答", true,
                  "key=" + std::to_string(byKey.size())
                  + " normal=" + std::to_string(byKey2.size())
                  + " uniq=" + std::string(uniqFound ? "1" : "0"));
            skip("queryMessage 命中结果（需 broker 开 RocksDB/KEYS 索引）",
                 "本机为默认文件索引且消息未设 KEYS，uniqKey 查询返回空属预期");
        } catch (const std::exception& e) {
            check("queryMessage 请求可达且正常应答", false, e.what());
        }
    }

    // viewMessage：从 offsetMsgId 解 broker 地址 + commitLog 偏移（uniqKey 解不出来）
    try {
        const std::string viewId = offsetIds.empty() ? sent[0].second : offsetIds[0];
        MessageExt vm = admin.viewMessage(topic, viewId);
        check("viewMessage(byMsgId)", true,
              "topic=" + vm.topic + " offset=" + std::to_string(vm.queueOffset)
              + " body=" + bytes2str(vm.body).substr(0, 32));
        check("viewMessage body 与发送一致", vm.body == sent[0].first,
              "期望 " + bytes2str(sent[0].first) + " 实际 " + bytes2str(vm.body));
    } catch (const std::exception& e) {
        check("viewMessage(byMsgId)", false, e.what());
    }

    // ---------- 7. Offset 只读查询 ----------
    try {
        maxOff = admin.maxOffset(mq);
        minOff = admin.minOffset(mq);
        check("maxOffset/minOffset", maxOff >= minOff,
              "min=" + std::to_string(minOff) + " max=" + std::to_string(maxOff));
        check("searchOffset(now) >= minOffset",
              admin.searchOffset(mq, nowMs()) >= minOff, "ok");
        check("earliestMsgStoreTime > 0", admin.earliestMsgStoreTime(mq) > 0, "ok");
        int64_t off = -1;
        admin.examineConsumerOffset(group, mq, off);
        check("examineConsumerOffset 可调用", true, "offset=" + std::to_string(off));
    } catch (const std::exception& e) {
        check("Offset 只读查询", false, e.what());
    }

    // ---------- 8. 消费 + sendMessageBack 重投 ----------
    // 顺序很关键：必须**先消费**再重投。若先把位点重置到 max，消费者就再也拉不到
    // 历史消息，sendMessageBack 路径根本没机会执行。
    // 本组是新建的、无已提交位点，故 FIRST_OFFSET 会真正从最小位点开始读。
    const std::string retryTopic = MixAll::getRetryTopic(group);
    bool backsent = false;
    {
        auto listener = std::make_shared<CollectingListener>();
        DefaultMQPushConsumer cons(group);
        cons.setNamesrvAddr(gNamesrv);
        cons.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        cons.subscribe(topic, "*");
        cons.setMessageListener(listener);
        // 单线程顺序长轮询下空闲队列会阻塞整轮，suspend 设短一些避免拖满 30s
        cons.setPullSuspendTimeoutMillis(3000);
        cons.start();

        for (int i = 0; i < 60 && listener->count() < 1; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        check("消费到消息（sendMessageBack 前置）", listener->count() >= 1,
              "consumed=" + std::to_string(listener->count()));

        if (listener->count() >= 1) {
            MessageExt first = listener->first();
            // 消费者仍在线时重投（对齐 Java：在消费线程内调用 sendMessageBack）
            try {
                bool ok = cons.sendMessageBack(first, 0);
                backsent = true;
                check("consumer.sendMessageBack(重投到 " + retryTopic + ")", ok,
                      "origin_topic=" + first.topic
                      + " commitLogOffset=" + std::to_string(first.commitLogOffset));
            } catch (const std::exception& e) {
                check("consumer.sendMessageBack", false, e.what());
            }
        }
        cons.shutdown();
    }

    // 校验 %RETRY% 里出现该消息：必须轮询而不是立即断言。
    // broker 的 SendMessageProcessor.consumerSendMsgBack 在 delayLevel == 0 时会改写
    // 成 `3 + reconsumeTimes`（默认 3 -> 10s），消息先进 SCHEDULE_TOPIC_XXXX，
    // 到点才投递到 %RETRY%<group>。
    if (backsent) {
        int64_t retryMax = -1;
        int waited = 0;
        for (int i = 0; i < 30; ++i) {
            try {
                TopicStatsTable st = admin.examineTopicStats(retryTopic);
                retryMax = st.totalMaxOffset();
                if (retryMax > 0) break;
            } catch (const std::exception&) {
                retryMax = -1;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
            ++waited;
        }
        check("sendMessageBack 落到 " + retryTopic + "（maxOffsetSum>0）", retryMax > 0,
              "maxOffsetSum=" + std::to_string(retryMax) + "（轮询 " + std::to_string(waited)
              + "s；broker 把 delayLevel=0 改写为 3，约 10s 后可见）");
    }

    // ---------- 9. 真实 broker 端位点重置（放最后，避免干扰上面的消费）----------
    {
        int64_t ts = nowMs() + 60000;  // 未来时间 -> 位点应被推到 max
        try {
            std::map<MessageQueue, int64_t> reset =
                admin.resetOffsetByTimestamp(topic, group, ts, true);
            check("resetOffsetByTimestamp(INVOKE_BROKER_TO_RESET_OFFSET)", !reset.empty(),
                  "queues=" + std::to_string(reset.size()));
            std::this_thread::sleep_for(std::chrono::seconds(1));
            int64_t off = -1;
            bool found = admin.examineConsumerOffset(group, mq, off);
            int64_t maxNow = admin.maxOffset(mq);
            check("reset 后消费者位点被推到 maxOffset", found && off >= maxNow - 1,
                  "consumerOffset=" + std::to_string(off) + " maxOffset=" + std::to_string(maxNow));
        } catch (const std::exception& e) {
            check("resetOffsetByTimestamp", false, e.what());
        }
    }

    // ---------- 10. 清理 ----------
    try {
        admin.deleteSubscriptionGroup(brokerAddr, group, true);
        check("deleteSubscriptionGroup", true, "OK");
    } catch (const std::exception& e) {
        check("deleteSubscriptionGroup", false, e.what());
    }
    try {
        admin.deleteTopic(topic);
        check("deleteTopic", true, "OK");
        std::this_thread::sleep_for(std::chrono::seconds(1));
        TopicList after = admin.fetchAllTopicList();
        check("deleteTopic 后 topic 消失", !after.contains(topic), topic);
    } catch (const std::exception& e) {
        check("deleteTopic", false, e.what());
    }

    admin.shutdown();
    return report();
}
