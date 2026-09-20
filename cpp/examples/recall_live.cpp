// C++ 客户端「定时消息撤回（RECALL_MESSAGE 370）」真机联调
// （对应 python/verify_recall_live.py、rust/examples/live_producer.rs 的 P9 段）。
//
// 为什么必须真机：句柄不是客户端能自己拼出来的。只有 broker 在
// SendMessageProcessor#attachRecallHandle 里（且仅当消息带 TIMER_* 延迟属性）才会把句柄
// 挂回 SEND 响应头；撤回是否真的生效也只能靠「到点没投递」来证明。离线单测
// （tests/test_recall_message.cpp）锁的是编解码、报文键名和本地校验顺序，证不了语义。
//
// 场景：
//   R0 读到 / 临时打开 broker 的 recallMessageEnable（默认 false，Java BrokerConfig:546）
//   R1 定时消息带回 recallHandle，普通消息不带
//   R2 broker 给的句柄能解出 topic / brokerName / 被撤回消息的 uniqKey
//   R3 recallMessage 返回被撤回消息的 uniqKey（Java 取响应头 msgId）
//   R4 %RETRY% topic 在本地就被拒（"topic is not supported"）
//   R5 非法句柄在本地就被拒（"recall handle is invalid"），且秒回（不打网络）
//   R6 语义：两条同样延迟的消息，撤回其中一条 → 到点后对照消息投递、被撤回的永不到
//   R7 退出前把 recallMessageEnable 还原成原值（跑之前是 false 就跑完还是 false）
//
// 用法：./rmq_recall_live [namesrv]（需本地 5.5.1 集群，且 timerWheelEnable=true）
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/recall_message_handle.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

using namespace rocketmq;

namespace {

int gPass = 0;
int gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

Bytes bytesOf(const std::string& s) { return Bytes(s.begin(), s.end()); }

constexpr const char* kBrokerName = "broker-a";
constexpr const char* kConfigKey = "recallMessageEnable";
constexpr int kDelaySec = 12;
constexpr int kConsumeWindowSec = kDelaySec + 20;

class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(std::vector<std::string>& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) sink_.push_back(bodyOf(m));
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    std::vector<std::string>& sink_;
    std::mutex mtx_;
};

std::string readFlag(DefaultMQAdminExt& admin, const std::string& addr) {
    try {
        PropertyMap cfg = admin.getBrokerConfig(addr);
        auto it = cfg.find(kConfigKey);
        return it == cfg.end() ? std::string() : it->second;
    } catch (const std::exception& e) {
        std::printf("  [diag] getBrokerConfig failed: %s\n", e.what());
        return std::string();
    }
}

bool writeFlag(DefaultMQAdminExt& admin, const std::string& addr, const std::string& value) {
    try {
        PropertyMap props;
        props[kConfigKey] = value;
        admin.updateBrokerConfig(addr, props);
        return true;
    } catch (const std::exception& e) {
        std::printf("  [diag] updateBrokerConfig(%s=%s) failed: %s\n", kConfigKey, value.c_str(),
                    e.what());
        return false;
    }
}

bool hasBody(const std::vector<std::string>& bodies, const std::string& want) {
    for (const std::string& b : bodies) {
        if (b == want) return true;
    }
    return false;
}

std::string joinBodies(const std::vector<std::string>& bodies) {
    std::string out = "[";
    for (size_t i = 0; i < bodies.size(); ++i) {
        out += (i ? "," : "") + bodies[i];
    }
    return out + "]";
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    const std::string prefix = "RecallCpp_" + std::to_string(nowMs() % 1000000);
    const std::string topic = prefix + "_Topic";
    const std::string group = prefix + "_Group";

    DefaultMQProducer producer(group + "_pg");
    producer.setNamesrvAddr(nsAddr);
    producer.setSendMsgTimeout(5000);
    producer.start();

    DefaultMQAdminExt admin;
    admin.setNamesrvAddr(nsAddr);
    admin.start();

    std::string original;
    bool opened = false;
    try {
        std::string addr = producer.client().brokerAddrOf(kBrokerName);
        if (addr.empty()) addr = "127.0.0.1:10911";
        original = readFlag(admin, addr);
        check("R0 能读到 broker 的 recallMessageEnable", !original.empty(), "value=" + original);
        if (original != "true") {
            opened = writeFlag(admin, addr, "true");
            check("R0 已临时打开 recallMessageEnable", opened);
        }
        if (readFlag(admin, addr) != "true") {
            check("R0 recall 未在 broker 上开启，后续检查无意义", false);
            admin.shutdown();
            producer.shutdown();
            return 1;
        }

        // ---- R1/R2：定时消息才带句柄，且句柄内容与发送结果一致 ----
        Message toRecall(topic, bytesOf("to-recall"));
        toRecall.putProperty("TIMER_DELAY_SEC", std::to_string(kDelaySec));
        Message control(topic, bytesOf("control"));
        control.putProperty("TIMER_DELAY_SEC", std::to_string(kDelaySec));
        Message plain(topic, bytesOf("plain"));

        SendResult rRecall = producer.send(toRecall);
        SendResult rControl = producer.send(control);
        SendResult rPlain = producer.send(plain);
        const bool hasHandle = rRecall.recallHandle.has_value() && !rRecall.recallHandle->empty();
        check("R1 定时消息带回 recallHandle", hasHandle,
              hasHandle ? rRecall.recallHandle.value() : std::string());
        check("R1 普通消息不带 recallHandle", !rPlain.recallHandle.has_value());

        HandleV1 parsed;
        try {
            parsed = decodeRecallHandle(rRecall.recallHandle.value_or(""));
            check("R2 broker 的句柄能被我们的编解码器解开", true);
        } catch (const std::exception& e) {
            check("R2 broker 的句柄能被我们的编解码器解开", false, e.what());
        }
        check("R2 句柄里的 topic/brokerName 与发送目标一致",
              parsed.topic == topic && parsed.brokerName == kBrokerName,
              parsed.topic + "/" + parsed.brokerName);
        check("R2 句柄里的 uniqKey 就是这条消息的 UNIQ_KEY", parsed.messageId == rRecall.msgId,
              "handle=" + parsed.messageId + " send=" + rRecall.msgId);

        // ---- R4/R5：本地校验必须在打网络之前跑完 ----
        std::string msg;
        try {
            (void)producer.recallMessage(MixAll::getRetryTopic(group), rRecall.recallHandle.value_or(""));
            msg = "<no exception>";
        } catch (const std::exception& e) {
            msg = e.what();
        }
        check("R4 %RETRY% topic 被拒", msg == "topic is not supported", msg);

        const int64_t began = nowMs();
        try {
            (void)producer.recallMessage(topic, "not-a-handle");
            msg = "<no exception>";
        } catch (const std::exception& e) {
            msg = e.what();
        }
        const int64_t cost = nowMs() - began;
        check("R5 非法句柄本地即拒", msg == "recall handle is invalid" && cost < 200,
              msg + " (" + std::to_string(cost) + "ms)");

        // ---- R3/R6：真的撤掉了，且没牵连对照消息 ----
        try {
            const std::string recalled = producer.recallMessage(topic, rRecall.recallHandle.value());
            check("R3 recallMessage 返回被撤回消息的 uniqKey", recalled == rRecall.msgId,
                  "resp=" + recalled + " send=" + rRecall.msgId);
        } catch (const std::exception& e) {
            check("R3 recallMessage 返回被撤回消息的 uniqKey", false, e.what());
        }

        std::vector<std::string> bodies;
        {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group + "_c");
            consumer->setNamesrvAddr(nsAddr);
            consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
            consumer->setMessageListener(std::make_shared<CollectListener>(bodies));
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(kConsumeWindowSec));
            consumer->shutdown();
        }
        check("R6 对照定时消息按时投递", hasBody(bodies, "control"), joinBodies(bodies));
        check("R6 普通消息已投递", hasBody(bodies, "plain"), joinBodies(bodies));
        check("R6 被撤回的定时消息永远没投递", !hasBody(bodies, "to-recall"), joinBodies(bodies));
    } catch (const std::exception& e) {
        check("联调异常", false, e.what());
    }

    // 无论成功失败都要还原：这是共享的开发 broker，不能把开关留在打开状态。
    bool restored = false;
    if (!original.empty()) {
        try {
            std::string addr = producer.client().brokerAddrOf(kBrokerName);
            if (addr.empty()) addr = "127.0.0.1:10911";
            writeFlag(admin, addr, original);
            restored = readFlag(admin, addr) == original;
        } catch (const std::exception& e) {
            std::printf("  [diag] restore failed: %s\n", e.what());
        }
    } else {
        restored = !opened;  // 没读到也没改过 —— 视为无需还原
    }
    check(std::string("R7 broker 的 recallMessageEnable 已还原为 ") + original, restored);

    admin.shutdown();
    producer.shutdown();
    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
