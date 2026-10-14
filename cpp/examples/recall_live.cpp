// Live-cluster check of timed-message recall (RECALL_MESSAGE 370) for the C++ client
//
// Why a real cluster: the handle is not client-assemblable. Only the broker, in
// SendMessageProcessor#attachRecallHandle (and only for messages with TIMER_* delay properties),
// attaches it back to the SEND response header; only "no delivery at the due time" proves the recall.
// The offline unit test (tests/test_recall_message.cpp) pins codec, wire keys and check order only.
//
//
// Usage: ./rmq_recall_live [namesrv] (needs a local 5.5.1 cluster with timerWheelEnable=true)
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
        check("R0 broker's recallMessageEnable is readable", !original.empty(), "value=" + original);
        if (original != "true") {
            opened = writeFlag(admin, addr, "true");
            check("R0 recallMessageEnable temporarily switched on", opened);
        }
        if (readFlag(admin, addr) != "true") {
            check("R0 recall is not enabled on the broker, later checks are meaningless", false);
            admin.shutdown();
            producer.shutdown();
            return 1;
        }

        // ---- R1/R2: only timed messages carry a handle, and the handle matches the send result ----
        Message toRecall(topic, bytesOf("to-recall"));
        toRecall.putProperty("TIMER_DELAY_SEC", std::to_string(kDelaySec));
        Message control(topic, bytesOf("control"));
        control.putProperty("TIMER_DELAY_SEC", std::to_string(kDelaySec));
        Message plain(topic, bytesOf("plain"));

        SendResult rRecall = producer.send(toRecall);
        SendResult rControl = producer.send(control);
        SendResult rPlain = producer.send(plain);
        const bool hasHandle = rRecall.recallHandle.has_value() && !rRecall.recallHandle->empty();
        check("R1 timed message returns a recallHandle", hasHandle,
              hasHandle ? rRecall.recallHandle.value() : std::string());
        check("R1 plain message carries no recallHandle", !rPlain.recallHandle.has_value());

        HandleV1 parsed;
        try {
            parsed = decodeRecallHandle(rRecall.recallHandle.value_or(""));
            check("R2 the broker handle decodes with our codec", true);
        } catch (const std::exception& e) {
            check("R2 the broker handle decodes with our codec", false, e.what());
        }
        check("R2 topic/brokerName in the handle match the send target",
              parsed.topic == topic && parsed.brokerName == kBrokerName,
              parsed.topic + "/" + parsed.brokerName);
        check("R2 the uniqKey in the handle is this message's UNIQ_KEY", parsed.messageId == rRecall.msgId,
              "handle=" + parsed.messageId + " send=" + rRecall.msgId);

        // ---- R4/R5: local validation must finish before any network traffic ----
        std::string msg;
        try {
            (void)producer.recallMessage(MixAll::getRetryTopic(group), rRecall.recallHandle.value_or(""));
            msg = "<no exception>";
        } catch (const std::exception& e) {
            msg = e.what();
        }
        check("R4 %RETRY% topic is rejected", msg == "topic is not supported", msg);

        const int64_t began = nowMs();
        try {
            (void)producer.recallMessage(topic, "not-a-handle");
            msg = "<no exception>";
        } catch (const std::exception& e) {
            msg = e.what();
        }
        const int64_t cost = nowMs() - began;
        check("R5 invalid handle is rejected locally", msg == "recall handle is invalid" && cost < 200,
              msg + " (" + std::to_string(cost) + "ms)");

        // ---- R3/R6: the recall really took effect, and the control message was unharmed ----
        try {
            const std::string recalled = producer.recallMessage(topic, rRecall.recallHandle.value());
            check("R3 recallMessage returns the uniqKey of the recalled message", recalled == rRecall.msgId,
                  "resp=" + recalled + " send=" + rRecall.msgId);
        } catch (const std::exception& e) {
            check("R3 recallMessage returns the uniqKey of the recalled message", false, e.what());
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
        check("R6 control timed message delivered on time", hasBody(bodies, "control"), joinBodies(bodies));
        check("R6 plain message delivered", hasBody(bodies, "plain"), joinBodies(bodies));
        check("R6 the recalled timed message is never delivered", !hasBody(bodies, "to-recall"), joinBodies(bodies));
    } catch (const std::exception& e) {
        check("live run threw", false, e.what());
    }

    // Restore either way, pass or fail: the development broker is shared, the flag must not stay on.
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
        restored = !opened;  // neither read nor changed -- taken as nothing to restore
    }
    check(std::string("R7 broker's recallMessageEnable restored to ") + original, restored);

    admin.shutdown();
    producer.shutdown();
    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
