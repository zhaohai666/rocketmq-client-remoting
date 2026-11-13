// Regression guards for the AccessChannel=CLOUD facade entry and the ACL-signed internal trace producer:
//   * producer.setAccessChannel(CLOUD) is read back and passed to the trace dispatcher at start();
//   * a TraceContext carrying no accessChannel falls back to the dispatcher's start()-time channel
//     (CLOUD -> rmq_sys_TRACE_DATA_<regionId>); the context's own channel wins when present;
//   * the internal trace producer inherits the host's RPC hook (ACL signing). Java parity:
//     getAndCreateTraceProducer(rpcHook) == new DefaultMQProducer(rpcHook); without it an
//     ACL-enabled broker rejects every unsigned trace request and no trace is ever delivered.
//
// No real network: a fake dispatcher overrides the internal producer's start() (virtual seam) and
// captures sends. shutdown() is therefore safe (the internal producer never really started).
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/client/producer.h"
#include "rocketmq/client/trace_dispatcher.h"
#include "rocketmq/remoting/rpchook.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const char* name) {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("  [FAIL] %s\n", name);
    }
}

void expectEq(const std::string& actual, const std::string& expected, const char* name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("  [FAIL] %s (actual=%s expected=%s)\n", name, actual.c_str(),
                    expected.c_str());
    }
}

// A PUB context with a region and one bean, but deliberately NO accessChannel: the dispatcher's
// start()-time channel is what the value_or fallback must use.
std::shared_ptr<TraceContext> pubContextWithoutChannel(const std::string& region) {
    auto ctx = std::make_shared<TraceContext>();
    ctx->traceType = TraceType::PUB;
    ctx->regionId = region;
    ctx->groupName = "GID_access";
    TraceBean bean;
    bean.topic = "TopicTest";
    bean.msgId = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6";
    ctx->traceBeans = {bean};
    return ctx;
}

// Fake dispatcher: captures the trace topic of every send and skips the internal producer's real
// network start(). startInternalProducer() is a protected virtual seam on AsyncTraceDispatcher.
class ProbeDispatcher : public AsyncTraceDispatcher {
public:
    std::vector<std::string> sentTopics;

    ProbeDispatcher(const std::string& group, TraceDispatcherType type, int32_t batchNum,
                    const std::string& topic, std::shared_ptr<RPCHook> rpcHook)
        : AsyncTraceDispatcher(group, type, batchNum, topic,
                               std::static_pointer_cast<void>(rpcHook)) {}

protected:
    void sendTraceMessage(const std::string& traceTopic, const std::string&,
                          const std::string&) override {
        sentTopics.push_back(traceTopic);
    }
    void startInternalProducer() override {}  // no network; producer stays unstarted
};

}  // namespace

int main() {
    // ---- 1. producer facade: default LOCAL, setter round-trip ----
    {
        DefaultMQProducer p("GID_access_channel_test");
        expect(p.accessChannel() == AccessChannel::LOCAL,
               "producer: accessChannel defaults to LOCAL");
        p.setAccessChannel(AccessChannel::CLOUD);
        expect(p.accessChannel() == AccessChannel::CLOUD,
               "producer: setAccessChannel(CLOUD) read back");
        p.setAccessChannel(AccessChannel::LOCAL);
        expect(p.accessChannel() == AccessChannel::LOCAL, "producer: reset to LOCAL");
    }

    // ---- 2. dispatcher start(CLOUD) stored + internal producer inherits the host ACL hook ----
    {
        std::shared_ptr<RPCHook> hook =
            std::make_shared<AclClientRPCHook>(SessionCredentials("ak", "sk"));
        ProbeDispatcher d("GID_trace", TraceDispatcherType::PRODUCE, 20, "", hook);

        // Installed at construction (before start()), matching Java's new DefaultMQProducer(rpcHook).
        expect(d.traceProducerForTest() != nullptr, "dispatcher: internal trace producer exists");
        expect(d.traceProducerForTest()->rpcHook() == hook,
               "dispatcher: internal trace producer inherits host rpcHook (ACL signature)");

        d.start("127.0.0.1:9876", AccessChannel::CLOUD);
        expect(d.accessChannel() == AccessChannel::CLOUD,
               "dispatcher: start(CLOUD) stored -> accessChannel()==CLOUD");
        d.shutdown();  // stop the flush worker; accessChannel_ stays CLOUD

        // ---- 3. CLOUD fallback: context carries NO accessChannel, dispatcher's CLOUD wins ----
        d.append(pubContextWithoutChannel("RegionCloud"));
        d.flush();
        expect(d.sentTopics.size() == 1, "dispatcher CLOUD fallback: one trace message");
        if (!d.sentTopics.empty()) {
            expectEq(d.sentTopics[0], "rmq_sys_TRACE_DATA_RegionCloud",
                     "dispatcher CLOUD fallback: no context channel -> prefix + regionId");
        }
    }

    // ---- 4. default LOCAL: start() without a channel, no host hook, default trace topic ----
    {
        ProbeDispatcher d("GID_trace_local", TraceDispatcherType::CONSUME, 10, "", nullptr);
        expect(d.traceProducerForTest()->rpcHook() == nullptr,
               "dispatcher: no host hook -> internal producer has none");
        d.start("127.0.0.1:9876");  // default LOCAL
        expect(d.accessChannel() == AccessChannel::LOCAL, "dispatcher: start() default LOCAL");
        d.shutdown();

        d.append(pubContextWithoutChannel("DefaultRegion"));
        d.flush();
        expect(d.sentTopics.size() == 1, "dispatcher LOCAL fallback: one trace message");
        if (!d.sentTopics.empty()) {
            expectEq(d.sentTopics[0], "RMQ_SYS_TRACE_TOPIC",
                     "dispatcher LOCAL fallback: no context channel -> default trace topic");
        }
    }

    // ---- 5. context channel overrides the dispatcher channel (value_or precedence) ----
    {
        ProbeDispatcher d("GID_trace_override", TraceDispatcherType::PRODUCE, 10, "", nullptr);
        d.start("127.0.0.1:9876", AccessChannel::CLOUD);
        d.shutdown();
        auto ctx = pubContextWithoutChannel("RegionB");
        ctx->accessChannel = AccessChannel::LOCAL;  // context LOCAL beats dispatcher CLOUD
        d.append(ctx);
        d.flush();
        expect(d.sentTopics.size() == 1, "dispatcher override: one trace message");
        if (!d.sentTopics.empty()) {
            expectEq(d.sentTopics[0], "RMQ_SYS_TRACE_TOPIC",
                     "dispatcher override: context LOCAL wins over dispatcher CLOUD");
        }
    }

    std::printf("access_channel: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
