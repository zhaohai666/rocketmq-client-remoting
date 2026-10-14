// Integration of the C++ admin client (DefaultMQAdminExt) against a **real cluster**.
//
//
// This program does not start a cluster itself; the caller must first run nameServer(9876) + broker(10911) with
// autoCreateTopicEnable=true. Usage:
//   ./rmq_admin_live [namesrv_addr]
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/pull_consumer.h"
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

// Record items that "depend on broker-side configuration differences and do not hold on this machine"; they are not counted as failures.
// Strictly separated from check: skip must state clearly why the precondition does not hold locally.
void skip(const std::string& name, const std::string& detail) {
    ++gSkip;
    std::cout << "[SKIP] " << name << "  " << detail << std::endl;
}

std::string bytes2str(const Bytes& b) { return std::string(b.begin(), b.end()); }
Bytes str2bytes(const std::string& s) { return Bytes(s.begin(), s.end()); }

int64_t nowMs() { return UtilAll::currentTimeMillis(); }

// Collect the first consumed messages (sendMessageBack needs the original MessageExt)
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
    std::cout << "\n================ Summary ================" << std::endl;
    for (const auto& r : gResults) {
        std::cout << "  [" << (r.second ? "PASS" : "FAIL") << "] " << r.first << std::endl;
    }
    std::cout << "======================================" << std::endl;
    std::cout << "total " << (gPass + gFail) << " items, failed " << gFail << " items, skipped " << gSkip
              << " items" << std::endl;
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

    // ---------- 1. Cluster liveness (broker registration race: port open != registered) ----------
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
        check("cluster liveness", false, "no broker registered in the name server last_err=" + lastErr);
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

    // ---------- 2. Topic management ----------
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
        check("the new topic appears in topicList", all.contains(topic), topic);
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
        check("route readQueueNums==4", allFour, "ok");
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
        check("TopicConfig queue nums match the creation",
              cfg.readQueueNums == 4 && cfg.writeQueueNums == 4,
              "read=" + std::to_string(cfg.readQueueNums)
              + " write=" + std::to_string(cfg.writeQueueNums));
        check("TopicConfig default perm=6", cfg.perm == 6, "perm=" + std::to_string(cfg.perm));
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

    // ---------- 3. Broker configuration: properties text (historical bug spot) ----------
    PropertyMap brokerCfg;
    try {
        brokerCfg = admin.getBrokerConfig(brokerAddr);
        check("getBrokerConfig(properties text)", !brokerCfg.empty(),
              "keys=" + std::to_string(brokerCfg.size()));
        check("getBrokerConfig parses out brokerName", brokerCfg.count("brokerName") == 1,
              "brokerName=" + brokerCfg["brokerName"]);
    } catch (const std::exception& e) {
        check("getBrokerConfig(properties text)", false, e.what());
    }

    if (!brokerCfg.empty()) {
        // Reversible change: write a harmless config value then restore it
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
            check("updateBrokerConfig took effect", after["sendMessageThreadPoolNums"] == probe,
                  "expected " + probe + " actual " + after["sendMessageThreadPoolNums"]);
            if (!original.empty()) {
                PropertyMap back;
                back["sendMessageThreadPoolNums"] = original;
                admin.updateBrokerConfig(brokerAddr, back);
            }
        } catch (const std::exception& e) {
            check("updateBrokerConfig", false, e.what());
        }
    }

    // ---------- 4. NameServer KV configuration ----------
    try {
        admin.createAndUpdateKvConfig(kvNs, "k1", "v1");
        check("createAndUpdateKvConfig", true, "OK");
        std::string value;
        bool found = admin.getKvConfig(kvNs, "k1", value);
        check("getKVConfig", found, "value=" + value);
        check("KV value round-trips unchanged", found && value == "v1", "expected v1 actual " + value);
        KVTable t = admin.getKvListByNamespace(kvNs);
        check("getKVListByNamespace contains k1", t.table.count("k1") == 1,
              "tableSize=" + std::to_string(t.table.size()));
        admin.deleteKvConfig(kvNs, "k1");
        check("deleteKVConfig", true, "OK");
        std::string gone;
        bool stillThere = admin.getKvConfig(kvNs, "k1", gone);
        check("KV no longer exists after deletion", !stillThere, "found=" + std::string(stillThere ? "1" : "0"));
    } catch (const std::exception& e) {
        check("NameServer KV chain", false, e.what());
    }

    // ---------- 5. Subscription group management ----------
    {
        SubscriptionGroupConfig sgc(group);
        sgc.consumeEnable = true;
        sgc.retryMaxTimes = 5;
        try {
            admin.createAndUpdateSubscriptionGroupConfig(brokerAddr, sgc);
            check("createAndUpdateSubscriptionGroupConfig", true, "group=" + group);

            SubscriptionGroupConfig single;
            bool ok = admin.getSubscriptionGroupConfig(brokerAddr, group, single);
            check("getSubscriptionGroupConfig(single query)", ok, "found=" + std::string(ok ? "1" : "0"));
            if (ok) {
                check("subscription group retryMaxTimes round-trips", single.retryMaxTimes == 5,
                      "actual " + std::to_string(single.retryMaxTimes));
            }

            SubscriptionGroupWrapper wrapper = admin.getAllSubscriptionGroup(brokerAddr);
            check("getAllSubscriptionGroup(paged)", !wrapper.subscriptionGroupTable.empty(),
                  "groups=" + std::to_string(wrapper.subscriptionGroupTable.size()));
            check("the paged result contains the new subscription group", wrapper.subscriptionGroupTable.count(group) == 1,
                  "count=" + std::to_string(wrapper.subscriptionGroupTable.size()));

            SubscriptionGroupConfig ex;
            bool exOk = admin.examineSubscriptionGroupConfig(brokerAddr, group, ex);
            check("examineSubscriptionGroupConfig", exOk, "group=" + ex.groupName);
        } catch (const std::exception& e) {
            check("subscription group management chain", false, e.what());
        }
    }

    // ---------- 6. Produce + statistics + query ----------
    std::vector<std::pair<Bytes, std::string>> sent;  // body, uniqKey msgId
    std::vector<std::string> offsetIds;               // broker-side offsetMsgId (viewMessage needs it)
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
        check("sync-sent " + std::to_string(nMsg) + " messages", sent.size() == static_cast<size_t>(nMsg),
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
        check("TopicStatsTable maxOffset sum >= sent count", stats.totalMaxOffset() >= nMsg,
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

    // queryMessage: assert only "the request is reachable, answers normally and returns the right type".
    // Do not assert "the query must hit": msgId here is the uniqKey, and the broker-side uniqKey inverted
    // index is only supported by the RocksDB index implementation; with the default file index and
    // no KEYS on the message, a miss is a **broker configuration difference**, not a client bug.
    {
        const std::string msgId0 = sent[0].second;
        try {
            std::vector<MessageExt> byKey =
                admin.queryMessage(topic, msgId0, 32, 0, nowMs() + 60000);
            std::vector<MessageExt> byKey2 = admin.queryMessageByKey(topic, msgId0, 32);
            MessageExt uniq;
            bool uniqFound = admin.queryMessageByUniqKey(topic, msgId0, uniq);
            check("queryMessage request reachable with a normal answer", true,
                  "key=" + std::to_string(byKey.size())
                  + " normal=" + std::to_string(byKey2.size())
                  + " uniq=" + std::string(uniqFound ? "1" : "0"));
            skip("queryMessage hit result (requires broker RocksDB/KEYS index)",
                 "this machine uses the default file index and the messages carry no KEYS; an empty uniqKey query is expected");
        } catch (const std::exception& e) {
            check("queryMessage request reachable with a normal answer", false, e.what());
        }
    }

    // viewMessage(byOffsetMsgId): only the broker-assigned offsetMsgId decodes to a commitLog offset
    try {
        const std::string viewId = offsetIds.empty() ? sent[0].second : offsetIds[0];
        MessageExt vm = admin.viewMessage(topic, viewId);
        check("viewMessage(byOffsetMsgId)", true,
              "topic=" + vm.topic + " offset=" + std::to_string(vm.queueOffset)
              + " body=" + bytes2str(vm.body).substr(0, 32));
        check("viewMessage body matches what was sent", vm.body == sent[0].first,
              "expected " + bytes2str(sent[0].first) + " actual " + bytes2str(vm.body));
    } catch (const std::exception& e) {
        check("viewMessage(byOffsetMsgId)", false, e.what());
    }

    // viewMessage(uniqKey): a 5.x msgId decodes to no offset, so it must take the uniqKey fallback path and raise a clean exception,
    // instead of going connect with a garbage port
    try {
        admin.viewMessage(topic, sent[0].second);
        check("viewMessage(uniqKey) takes the fallback and raises a clean exception", false, "no exception thrown");
    } catch (const MQClientException& e) {
        check("viewMessage(uniqKey) takes the fallback and raises a clean exception", true,
              "code=" + std::to_string(e.getResponseCode()));
    } catch (const std::exception& e) {
        check("viewMessage(uniqKey) takes the fallback and raises a clean exception", false,
              std::string(typeid(e).name()) + ": " + e.what());
    }

    // ---------- 7. Offset read-only queries ----------
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
        check("examineConsumerOffset is callable", true, "offset=" + std::to_string(off));
    } catch (const std::exception& e) {
        check("Offset read-only queries", false, e.what());
    }

    const std::string bndTopic = "AdminBoundaryTopicCpp_" + stamp;
    try {
        admin.createTopic(MixAll::DEFAULT_TOPIC, bndTopic, 1);
        check("createTopic(" + bndTopic + ", 1 queue)", true, "queueNum=1");
    } catch (const std::exception& e) {
        check("createTopic(" + bndTopic + ", 1 queue)", false, e.what());
    }
    try {
        DefaultMQProducer bprod("AdminLiveCppBoundaryProducer_" + stamp);
        bprod.setNamesrvAddr(gNamesrv);
        bprod.setSendMsgTimeout(5000);
        bprod.start();
        for (int i = 0; i < 3; ++i) {
            bprod.send(Message(bndTopic, str2bytes("boundary-" + std::to_string(i))));
        }
        check("boundary topic sent 3 messages", true, "ok");
        bprod.shutdown();
    } catch (const std::exception& e) {
        check("boundary topic sent 3 messages", false, e.what());
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
    try {
        MessageQueue bmq(bndTopic, firstMq.brokerName, 0);
        int64_t bMax = admin.maxOffset(bmq);
        int64_t lo = admin.searchLowerBoundaryOffset(bmq, nowMs() + 600000);
        int64_t up = admin.searchUpperBoundaryOffset(bmq, nowMs() + 600000);
        int64_t loPast = admin.searchLowerBoundaryOffset(bmq, 1);
        int64_t upPast = admin.searchUpperBoundaryOffset(bmq, 1);
        check("boundary topic maxOffset == 3", bMax == 3, "max=" + std::to_string(bMax));
        check("LOWER boundary = maxOffset (the next offset after the queue tail)", lo == bMax,
              "lower=" + std::to_string(lo) + " maxOffset=" + std::to_string(bMax));
        check("UPPER boundary = maxOffset-1 (the last message's own offset)", up == bMax - 1,
              "upper=" + std::to_string(up) + " maxOffset-1=" + std::to_string(bMax - 1));
        check("the two boundaries really differ (proves boundaryType takes effect)", lo != up,
              "lower=" + std::to_string(lo) + " upper=" + std::to_string(up));
        check("when the timestamp predates all messages LOWER/UPPER both collapse to minOffset",
              loPast == 0 && upPast == 0,
              "lower=" + std::to_string(loPast) + " upper=" + std::to_string(upPast));
        check("searchOffset default boundary equals an explicit LOWER",
              admin.searchOffset(bmq, nowMs() + 600000) == lo,
              "lower=" + std::to_string(lo));
    } catch (const std::exception& e) {
        check("searchOffset boundaryType", false, e.what());
    }

    // ---------- 8. Consume + sendMessageBack redelivery ----------
    // The order is critical: consume first, then send back. If the offset is reset to max first, the consumer can never pull
    // historical messages again and the sendMessageBack path never gets a chance to run.
    // This group is newly created with no committed offset, so FIRST_OFFSET truly starts reading from the minimum offset.
    const std::string retryTopic = MixAll::getRetryTopic(group);
    bool backsent = false;
    {
        auto listener = std::make_shared<CollectingListener>();
        DefaultMQPushConsumer cons(group);
        cons.setNamesrvAddr(gNamesrv);
        cons.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        cons.subscribe(topic, "*");
        cons.setMessageListener(listener);
        // Under single-threaded sequential long polling an idle queue blocks the whole round, so set suspend shorter to avoid filling up 30s
        cons.setPullSuspendTimeoutMillis(3000);
        cons.start();

        for (int i = 0; i < 60 && listener->count() < 1; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        check("messages consumed (sendMessageBack precondition)", listener->count() >= 1,
              "consumed=" + std::to_string(listener->count()));

        if (listener->count() >= 1) {
            MessageExt first = listener->first();
            try {
                bool ok = cons.sendMessageBack(first, 0);
                backsent = true;
                check("consumer.sendMessageBack(redeliver to " + retryTopic + ")", ok,
                      "origin_topic=" + first.topic
                      + " commitLogOffset=" + std::to_string(first.commitLogOffset));
            } catch (const std::exception& e) {
                check("consumer.sendMessageBack", false, e.what());
            }
        }
        cons.shutdown();
    }

    // Verify the message appears in %RETRY%: must poll rather than assert immediately.
    // The broker's SendMessageProcessor.consumerSendMsgBack rewrites delayLevel == 0 into
    // `3 + reconsumeTimes` (default 3 -> 10s); the message first enters SCHEDULE_TOPIC_XXXX,
    // and only when due is it delivered to %RETRY%<group>.
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
        check("sendMessageBack lands in " + retryTopic + " (maxOffsetSum>0)", retryMax > 0,
              "maxOffsetSum=" + std::to_string(retryMax) + " (polled " + std::to_string(waited)
              + "s; the broker rewrites delayLevel=0 as 3, visible after ~10s)");
    }

    {
        size_t rows = 0;
        bool groupWithOffsets = false;
        int waited = 0;
        for (int i = 0; i < 20 && !groupWithOffsets; ++i) {
            try {
                ConsumeStatsList sl = admin.fetchConsumeStatsInBroker(brokerAddr, false);
                rows = sl.statsList.size();
                for (size_t r = 0; r < rows && !groupWithOffsets; ++r) {
                    const JsonValue& stats = sl.statsList.at(r).get(group);
                    for (size_t s = 0; s < stats.size(); ++s) {
                        if (!stats.at(s).get("offsetTable").objectItems().empty()) {
                            groupWithOffsets = true;
                            break;
                        }
                    }
                }
            } catch (const std::exception&) {
                rows = 0;
            }
            if (!groupWithOffsets) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                ++waited;
            }
        }
        check("fetchConsumeStatsInBroker produces rows per subscription group", rows > 0,
              "groups=" + std::to_string(rows));
        check("fetchConsumeStatsInBroker: this group's stats carry an offsetTable", groupWithOffsets,
              "polled " + std::to_string(waited) + "s");
    }

    // ---------- 9. Real broker-side offset reset (last, to avoid disturbing the consumption above) ----------
    {
        int64_t ts = nowMs() + 60000;  // future time -> the offset should be pushed to max
        try {
            std::map<MessageQueue, int64_t> reset =
                admin.resetOffsetByTimestamp(topic, group, ts, true);
            check("resetOffsetByTimestamp(INVOKE_BROKER_TO_RESET_OFFSET)", !reset.empty(),
                  "queues=" + std::to_string(reset.size()));
            std::this_thread::sleep_for(std::chrono::seconds(1));
            int64_t off = -1;
            bool found = admin.examineConsumerOffset(group, mq, off);
            int64_t maxNow = admin.maxOffset(mq);
            check("the consumer offset is pushed to maxOffset after reset", found && off >= maxNow - 1,
                  "consumerOffset=" + std::to_string(off) + " maxOffset=" + std::to_string(maxNow));
        } catch (const std::exception& e) {
            check("resetOffsetByTimestamp", false, e.what());
        }
    }

    {
        const int32_t queueId = mq.getQueueId();
        int64_t minReset = -1;
        try {
            minReset = admin.minOffset(mq);
        } catch (const std::exception& e) {
            check("minOffset (reset precondition)", false, e.what());
        }
        if (minReset >= 0) {
            try {
                std::map<MessageQueue, int64_t> table =
                    admin.resetOffsetByQueueId(brokerAddr, group, topic, queueId, minReset);
                bool consistent = table.empty() ||
                    table.begin()->first.getQueueId() == queueId;
                check("resetOffsetByQueueId(back to minOffset)", consistent,
                      "returnedTable=" + std::to_string(table.size()) +
                          " (the response is void and may be empty)");
                int64_t off = -1;
                bool found = admin.examineConsumerOffset(group, mq, off);
                check("offset == target offset after resetOffsetByQueueId", found && off == minReset,
                      "consumerOffset=" + std::to_string(off) +
                          " target=" + std::to_string(minReset));
            } catch (const std::exception& e) {
                check("resetOffsetByQueueId(back to minOffset)", false, e.what());
            }

            try {
                DefaultMQPullConsumer pull(group);
                pull.setNamesrvAddr(gNamesrv);
                pull.start();
                PullResult first = pull.pull(mq, "*", minReset, 16, 5000);
                check("the first pull after reset is short-circuited by the broker into PULL_OFFSET_MOVED",
                      first.status == PullStatus::OFFSET_ILLEGAL &&
                          first.nextBeginOffset == minReset,
                      "status=" + std::string(pullStatusName(first.status)) +
                          " nextBeginOffset=" + std::to_string(first.nextBeginOffset));
                PullResult second = pull.pull(mq, "*", first.nextBeginOffset, 16, 5000);
                check("historical messages can be pulled again after the reset offset", !second.msgFoundList.empty(),
                      "status=" + std::string(pullStatusName(second.status)) +
                          " found=" + std::to_string(second.msgFoundList.size()));
                pull.shutdown();
            } catch (const std::exception& e) {
                check("re-pull after reset", false, e.what());
            }

            int64_t badTarget = -1;
            try {
                badTarget = admin.maxOffset(mq) + 100;
                bool rejected = false;
                std::string remark;
                try {
                    admin.resetOffsetByQueueId(brokerAddr, group, topic, queueId, badTarget);
                } catch (const std::exception& e) {
                    rejected = true;
                    remark = e.what();
                }
                check("resetOffsetByQueueId out-of-range target rejected by the broker", rejected,
                      "badTarget=" + std::to_string(badTarget) + " " + remark.substr(0, 120));
                int64_t off = -1;
                bool found = admin.examineConsumerOffset(group, mq, off);
                check("the out-of-range reset stays at the illegal offset written by the first call (the two RPCs are not atomic)",
                      found && off == badTarget,
                      "consumerOffset=" + std::to_string(off));
            } catch (const std::exception& e) {
                check("resetOffsetByQueueId out-of-range target rejected by the broker", false, e.what());
            }
        }

        // 343 reads the broker's offsetTable (whichTopicByConsumer); it only has content once the group committed an offset.
        try {
            TopicList byBroker = admin.queryTopicsByConsumerToBroker(brokerAddr, group);
            check("queryTopicsByConsumerToBroker reads out the topics this group consumed",
                  byBroker.contains(topic), "topics=" + std::to_string(byBroker.topicList.size()));
            TopicList byGroup = admin.queryTopicsByConsumer(group);
            check("queryTopicsByConsumer(group) fans out via %RETRY% routing and merges",
                  byGroup.contains(topic), "topics=" + std::to_string(byGroup.topicList.size()));
        } catch (const std::exception& e) {
            check("queryTopicsByConsumer", false, e.what());
        }
    }

    // ---------- 10. Cleanup ----------
    try {
        admin.deleteSubscriptionGroup(brokerAddr, group, true);
        check("deleteSubscriptionGroup", true, "OK");
    } catch (const std::exception& e) {
        check("deleteSubscriptionGroup", false, e.what());
    }
    try {
        admin.deleteTopic(topic);
        admin.deleteTopic(bndTopic);
        check("deleteTopic", true, "OK");
        std::this_thread::sleep_for(std::chrono::seconds(1));
        TopicList after = admin.fetchAllTopicList();
        check("the topic is gone after deleteTopic", !after.contains(topic), topic);
        check("the boundary topic is gone after deleteTopic", !after.contains(bndTopic), bndTopic);
    } catch (const std::exception& e) {
        check("deleteTopic", false, e.what());
    }

    admin.shutdown();
    return report();
}
