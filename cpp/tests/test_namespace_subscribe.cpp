// Namespace subscribe surface (the Java facade semantics): DefaultMQPushConsumer
// wraps the TOPIC at the public Subscribe entry (Java DefaultMQPushConsumer
// .subscribe:838 -> withNamespace(topic)), and Unsubscribe resolves a raw caller
// name to the wrapped subscription key (see the note on the quirk in
// consumer.cpp::unsubscribe). Covers: wrap, raw-name unsubscribe, idempotent
// wrapped-name unsubscribe, system / retry shapes, and the no-namespace pass-through.
#include <cstdio>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/common/mix_all.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

std::string join(const std::vector<std::string>& v) {
    std::string out;
    for (const auto& s : v) {
        if (!out.empty()) out += ",";
        out += s;
    }
    return out;
}

bool contains(const std::vector<std::string>& v, const std::string& want) {
    for (const auto& s : v) {
        if (s == want) return true;
    }
    return false;
}

}  // namespace

int main() {
    DefaultMQPushConsumer c("GID_NsSubCpp");
    c.setNamespace("MQ_INST_ns");

    // Subscribe stores the namespaced topic as the subscription key: route
    // fetch / pull request / heartbeat / rebalance all read this key, and the
    // delivery path strips the prefix again.
    c.subscribe("TopicA", "TagA");
    auto topics = c.subscribedTopics();
    expect(topics.size() == 1 && topics[0] == "MQ_INST_ns%TopicA",
           "subscribe.wrapsTopic", join(topics));

    // Unsubscribe with the raw caller-facing name resolves to the wrapped key.
    c.unsubscribe("TopicA");
    expect(c.subscribedTopics().empty(), "unsubscribe.rawNameDropsWrappedKey", join(c.subscribedTopics()));

    // Idempotent wrap: an already-namespaced name unsubscribes the same key.
    c.subscribe("TopicB", "*");
    c.unsubscribe("MQ_INST_ns%TopicB");
    expect(c.subscribedTopics().empty(), "unsubscribe.wrappedNameDropsSameKey", join(c.subscribedTopics()));

    // System resources come back unchanged; retry/DLQ names keep their prefix
    // with only the inner resource wrapped (NamespaceUtil.wrapNamespace rules).
    c.subscribe("rmq_sys_TRACE_DATA_test", "*");
    c.subscribe(std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + "GID_NsSubCpp", "*");
    topics = c.subscribedTopics();
    expect(contains(topics, "rmq_sys_TRACE_DATA_test"),
           "subscribe.systemTopicNotWrapped", join(topics));
    expect(contains(topics, std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + "MQ_INST_ns%GID_NsSubCpp"),
           "subscribe.retryTopicKeepsPrefixWrapsInner", join(topics));

    // Without a namespace the key stays raw (the wrap helper is a no-op).
    DefaultMQPushConsumer plain("GID_NsFreeCpp");
    plain.subscribe("TopicA", "TagA");
    topics = plain.subscribedTopics();
    expect(topics.size() == 1 && topics[0] == "TopicA",
           "subscribe.noNamespaceStaysRaw", join(topics));

    std::printf("namespace_subscribe: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
