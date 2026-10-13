// 真机静态主题腿（七端口径的 cpp 版，与 `python/verify_static_topic_live.py`
// 同场景、逐条同断言）：
//   S1 createStaticTopic(513)：单 broker，totalQueues=8，物理 4 队列；
//   S2 路由回读 TopicQueueMappingByBroker；
//   S3 topicRouteData2EndpointsForStaticTopic：8 条端点（0-3 → broker，
//      4-7 → __syslo__none__）；
//   S4 消费者实例把 mock 队列名重写到真实 broker（表外 id 透传）；
//   S5 端到端：生产者发 4 条、PUSH 消费者收齐 4 条。
// 用法：rmq_live_static_topic <namesrv>
#include <chrono>
#include <cstdint>
#include <cstdio>
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
#include "rocketmq/client/static_topic.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int gPass = 0;
int gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = "") {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name;
    if (!detail.empty()) std::cout << "  " << detail;
    std::cout << std::endl;
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Message body 构造是 std::string（按字节透传）。
std::string str2bytes(const std::string& s) { return s; }

JsonValue mappingDetail(const std::string& topic, const std::string& brokerName) {
    JsonValue hosted = JsonValue::makeObject();
    for (int32_t gid = 0; gid < 4; ++gid) {
        JsonValue item = JsonValue::makeObject();
        item.set("gen", JsonValue::makeInt(0));
        item.set("queueId", JsonValue::makeInt(gid));
        item.set("bname", JsonValue::makeString(brokerName));
        item.set("logicOffset", JsonValue::makeInt(0));
        item.set("startOffset", JsonValue::makeInt(0));
        item.set("endOffset", JsonValue::makeInt(-1));
        item.set("timeOfStart", JsonValue::makeInt(-1));
        item.set("timeOfEnd", JsonValue::makeInt(-1));
        JsonValue items = JsonValue::makeArray();
        items.pushArray(item);
        hosted.set(std::to_string(gid), items);
    }
    JsonValue curr = JsonValue::makeObject();
    for (int32_t gid = 0; gid < 4; ++gid) {
        curr.set(std::to_string(gid), JsonValue::makeInt(gid));
    }
    JsonValue detail = JsonValue::makeObject();
    detail.set("topic", JsonValue::makeString(topic));
    detail.set("scope", JsonValue::makeString("__global__"));
    detail.set("totalQueues", JsonValue::makeInt(8));
    detail.set("bname", JsonValue::makeString(brokerName));
    detail.set("epoch", JsonValue::makeInt(0));
    detail.set("dirty", JsonValue::makeBool(false));
    detail.set("currIdMap", curr);
    detail.set("hostedQueues", hosted);
    return detail;
}

class Inbox : public MessageListenerConcurrently {
public:
    void consume(const MessageExt& m) {
        std::lock_guard<std::mutex> lk(mtx_);
        bodies_.push_back(
            std::string(reinterpret_cast<const char*>(m.getBody().data()), m.getBody().size()));
    }
    int count() {
        std::lock_guard<std::mutex> lk(mtx_);
        return static_cast<int>(bodies_.size());
    }
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        for (const MessageExt& m : msgs) consume(m);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    std::mutex mtx_;
    std::vector<std::string> bodies_;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::printf("usage: rmq_live_static_topic <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const std::string stamp = std::to_string(nowMs() % 100000000);
    const std::string topic = "CppStaticLive" + stamp;
    const std::string group = "cpp_static_pg_" + stamp;
    const int32_t physicalQueues = 4;
    const int32_t totalQueues = 8;

    std::cout << "\nS1 起管理端，createStaticTopic(513)" << std::endl;
    DefaultMQAdminExt admin;
    admin.setNamesrvAddr(nsAddr);
    admin.setTimeoutMillis(10000);
    admin.start();

    std::string brokerName;
    std::string brokerAddr;
    for (int i = 0; i < 60; ++i) {
        try {
            ClusterInfo info = admin.fetchBrokerClusterInfo();
            if (!info.brokerAddrTable.empty()) {
                const BrokerData& bd = info.brokerAddrTable.begin()->second;
                brokerName = bd.brokerName;
                auto master = bd.brokerAddrs.find(0);
                if (master != bd.brokerAddrs.end()) brokerAddr = master->second;
                break;
            }
        } catch (const std::exception&) {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    check("S1 broker 地址解析", !brokerAddr.empty(),
          brokerName + "(" + brokerAddr + ")");
    if (brokerAddr.empty()) return 1;

    try {
        TopicConfig config(topic);
        config.readQueueNums = physicalQueues;
        config.writeQueueNums = physicalQueues;
        admin.createStaticTopic(brokerAddr, "TBW102", config,
                                mappingDetail(topic, brokerName), true);
        check("S1 createStaticTopic(513) 被接受", true,
              "totalQueues=" + std::to_string(totalQueues));
    } catch (const std::exception& e) {
        check("S1 createStaticTopic(513) 被接受", false, e.what());
        return 1;
    }

    std::cout << "\nS2/S3 路由回读 + 端点集推导" << std::endl;
    TopicRouteData route;
    bool routeOk = false;
    for (int i = 0; i < 30; ++i) {
        try {
            DefaultMQAdminExt probe;
            probe.setNamesrvAddr(nsAddr);
            probe.setTimeoutMillis(10000);
            probe.start();
            TopicRouteData one = probe.examineTopicRoute(topic);
            probe.shutdown();
            if (!one.topicQueueMappingByBroker.isNull()) {
                route = one;
                routeOk = true;
                break;
            }
        } catch (const std::exception&) {
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    check("S2 路由携带 TopicQueueMappingByBroker", routeOk);
    if (!routeOk) return 1;

    std::map<MessageQueue, std::string> endpoints;
    bool epOk = static_topic::topicRouteData2EndpointsForStaticTopic(topic, route, endpoints);
    check("S3 端点集非空", epOk);
    if (!epOk) return 1;

    const std::string mockName = static_topic::getMockBrokerName("__global__");
    check("S3 端点总数 = totalQueues",
          static_cast<int32_t>(endpoints.size()) == totalQueues,
          "got " + std::to_string(endpoints.size()));
    bool hostedOk = true;
    for (int32_t gid = 0; gid < physicalQueues; ++gid) {
        auto it = endpoints.find(MessageQueue(topic, mockName, gid));
        if (it == endpoints.end() || it->second != brokerName) hostedOk = false;
    }
    check("S3 承载 id 0-3 → broker", hostedOk);
    bool unhostedOk = true;
    for (int32_t gid = physicalQueues; gid < totalQueues; ++gid) {
        auto it = endpoints.find(MessageQueue(topic, mockName, gid));
        if (it == endpoints.end() || it->second != static_topic::kLogicalQueueMockNotExist) {
            unhostedOk = false;
        }
    }
    check("S3 无承载 id 4-7 → __syslo__none__", unhostedOk);

    std::cout << "\nS4/S5 实例重写 + 端到端收发" << std::endl;
    auto inbox = std::make_shared<Inbox>();
    auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
    consumer->setNamesrvAddr(nsAddr);
    consumer->subscribe(topic, "*");
    consumer->setMessageListener(inbox);
    consumer->start();
    std::this_thread::sleep_for(std::chrono::seconds(4));

    bool rewriteOk = true;
    for (int32_t gid = 0; gid < physicalQueues; ++gid) {
        if (consumer->client().getBrokerNameFromMessageQueue(
                MessageQueue(topic, mockName, gid)) != brokerName) {
            rewriteOk = false;
        }
    }
    check("S4 实例把 mock 队列重写到真实 broker", rewriteOk);
    const std::string sentinel = consumer->client().getBrokerNameFromMessageQueue(
        MessageQueue(topic, mockName, totalQueues - 1));
    check("S4 无承载 id 返回哨兵值",
          sentinel == static_topic::kLogicalQueueMockNotExist, sentinel);
    const std::string outside = consumer->client().getBrokerNameFromMessageQueue(
        MessageQueue(topic, mockName, 99));
    check("S4 表外 id 透传 mock 名", outside == mockName, outside);
    const std::string plain = consumer->client().getBrokerNameFromMessageQueue(
        MessageQueue("Plain" + stamp, brokerName, 0));
    check("S4 非静态主题原样透传", plain == brokerName, plain);

    DefaultMQProducer producer("cpp_static_prod_" + stamp);
    producer.setNamesrvAddr(nsAddr);
    producer.start();
    int sent = 0;
    for (int32_t i = 0; i < physicalQueues; ++i) {
        try {
            producer.send(Message(topic, str2bytes("static-" + std::to_string(i))));
            ++sent;
        } catch (const std::exception& e) {
            std::cout << "  [WARN] send #" << i << ": " << e.what() << std::endl;
        }
    }
    producer.shutdown();
    check("S5 发送 4 条", sent == physicalQueues, "sent=" + std::to_string(sent));

    bool got = false;
    for (int i = 0; i < 40; ++i) {
        if (inbox->count() >= physicalQueues) {
            got = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    check("S5 消费收齐 4 条", got, "got=" + std::to_string(inbox->count()));
    consumer->shutdown();
    admin.shutdown();

    std::cout << "\n=== RESULT pass=" << gPass << " fail=" << gFail << " ===" << std::endl;
    return gFail == 0 ? 0 : 1;
}
