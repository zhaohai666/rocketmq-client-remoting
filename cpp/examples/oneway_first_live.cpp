// C++ client oneway send-first + CONSUME_FROM_FIRST_OFFSET live check.
//
// Pins the send-then-subscribe ordering the other oneway probes never cover:
// all messages exist BEFORE the consumer starts. A new consumer group with
// CONSUME_FROM_FIRST_OFFSET resolves its initial offset to 0 regardless of the
// subscription type (no stored offset -> 0), and the broker delivers whatever
// the pull covers.
// The sync control message proves the consumer is healthy, so a missing oneway
// body cannot be blamed on the consumer itself.
//
// Usage (start a local 5.5.1 cluster first):
//   ./rmq_oneway_first_live [namesrv]
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/route.h"

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

Bytes bytesOf(const std::string& s) { return Bytes(s.begin(), s.end()); }
std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

std::string join(const std::vector<std::string>& items) {
    std::string out = "[";
    for (size_t i = 0; i < items.size(); ++i) out += (i ? "," : "") + items[i];
    return out + "]";
}

template <typename F>
bool waitFor(F&& pred, int timeoutMs, int stepMs = 300) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    for (;;) {
        if (pred()) return true;
        if (std::chrono::steady_clock::now() >= deadline) return pred();
        std::this_thread::sleep_for(std::chrono::milliseconds(stepMs));
    }
}

struct Sink {
    std::mutex mtx;
    std::vector<std::string> bodies;

    size_t count() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies.size();
    }
    std::vector<std::string> sorted() {
        std::lock_guard<std::mutex> lk(mtx);
        std::vector<std::string> v = bodies;
        std::sort(v.begin(), v.end());
        return v;
    }
    bool has(const std::string& b) {
        std::lock_guard<std::mutex> lk(mtx);
        return std::find(bodies.begin(), bodies.end(), b) != bodies.end();
    }
};

class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(Sink& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.bodies.push_back(bodyOf(m));
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    Sink& sink_;
};

std::string gNsAddr;
std::string gStamp;

int routeQueueCount(DefaultMQAdminExt& admin, const std::string& topic) {
    try {
        const TopicRouteData route = admin.examineTopicRoute(topic);
        int n = 0;
        for (const QueueData& q : route.queueDatas) n += q.readQueueNums;
        return n;
    } catch (const std::exception&) {
        return 0;
    }
}

}  // namespace

int main(int argc, char** argv) {
    gNsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    gStamp = std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    std::printf("oneway send-first + FIRST_OFFSET live check on %s (stamp=%s)\n",
                gNsAddr.c_str(), gStamp.c_str());

    const std::string topic = "OnewayFirstCpp_" + gStamp;
    const std::string group = "GID_OnewayFirstCpp_" + gStamp;

    DefaultMQAdminExt admin("ONEWAYFIRSTCPPADMIN");
    admin.setNamesrvAddr(gNsAddr);
    admin.start();
    DefaultMQProducer prod(group + "_P");
    prod.setNamesrvAddr(gNsAddr);
    prod.setSendMsgTimeout(5000);
    prod.start();
    try {
        prod.createTopic(MixAll::DEFAULT_TOPIC, topic, 4);
        const bool ready = waitFor([&] { return routeQueueCount(admin, topic) >= 4; }, 20000);
        check("T0 topic route ready (4 queues)", ready,
              "queues=" + std::to_string(routeQueueCount(admin, topic)));
        if (!ready) {
            prod.shutdown();
            admin.shutdown();
            std::printf("\n%d PASS / %d FAIL\n", gPass, gFail);
            return 1;
        }

        // ---- send FIRST: 3 oneway + 1 sync control, no consumer running yet ----
        std::vector<std::string> onewayBodies;
        for (int i = 0; i < 3; ++i) {
            const std::string text = "oneway-first-" + std::to_string(i);
            Message m(topic, bytesOf(text));
            m.setKeys(gStamp + "-oneway-first-" + std::to_string(i));
            prod.sendOneway(m);
            onewayBodies.push_back(text);
        }
        check("O3 sendOneway x3 returned without throwing (fire-and-forget)", true, "");
        const std::string controlBody = "sync-control-first";
        Message control(topic, bytesOf(controlBody));
        control.setKeys(gStamp + "-sync-first");
        const SendResult r = prod.send(control, 5000);
        check("O3 the sync control message was sent (SEND_OK)",
              r.sendStatus == SendStatus::SEND_OK, r.msgId);
        std::this_thread::sleep_for(std::chrono::seconds(1));  // give the broker a beat to store the frames

        // ---- then start the FIRST_OFFSET consumer ----
        Sink sink;
        std::shared_ptr<CollectListener> listener(new CollectListener(sink));
        DefaultMQPushConsumer c(group);
        c.setNamesrvAddr(gNsAddr);
        c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c.subscribe(topic, "*");
        c.setMessageListener(listener);
        c.start();
        waitFor([&] { return sink.count() >= 4; }, 30000);
        std::this_thread::sleep_for(std::chrono::seconds(2));  // rule out "arriving late"
        const std::vector<std::string> got = sink.sorted();
        std::vector<std::string> want = onewayBodies;
        want.push_back(controlBody);
        std::sort(want.begin(), want.end());
        check("O3 FIRST_OFFSET receives all 4 messages sent BEFORE start (3 oneway + 1 sync control)",
              got == want, join(got));
        bool controlThere = sink.has(controlBody);
        check("O3 the sync control was delivered (the consumer itself is healthy)", controlThere,
              join(got));
        for (const std::string& b : onewayBodies) {
            check("O3 the oneway body '" + b + "' was delivered to the FIRST_OFFSET consumer",
                  sink.has(b), join(got));
        }
        c.shutdown();

        // cleanup
        try {
            admin.deleteTopic(topic);
        } catch (const std::exception&) {
        }
        try {
            admin.deleteTopic(MixAll::getRetryTopic(group));
        } catch (const std::exception&) {
        }
    } catch (const std::exception& e) {
        std::printf("  [FAIL] aborted: %s\n", e.what());
        ++gFail;
    }
    prod.shutdown();
    admin.shutdown();

    std::printf("\n%d PASS / %d FAIL\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
