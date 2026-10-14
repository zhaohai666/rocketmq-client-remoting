#include <cstdint>
#include <iostream>
#include <string>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/mix_all.h"

using namespace rocketmq;

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg)                              \
    do {                                              \
        if (cond) {                                   \
            ++g_pass;                                 \
        } else {                                      \
            ++g_fail;                                 \
            std::cout << "[FAIL] " << (msg) << "\n";  \
        }                                             \
    } while (0)

static const std::string kPidPrefix = std::to_string(MixAll::pid()) + "#";
static const std::string kStreamSuffix = std::string("@") + MixAll::STREAM_REQUEST_TYPE;

static void expectEq(const std::string& actual, const std::string& expected, const char* what) {
    CHECK(actual == expected, (std::string(what) + " (actual=" + actual + ", expected=" + expected + ")").c_str());
}

/** `<IP>@<pid>#<nanoseconds>`: this shape appears only after the default instanceName has been rewritten. */
static bool isRewrittenClientId(const std::string& id) {
    const std::string bare =
        id.size() > kStreamSuffix.size() && id.compare(id.size() - kStreamSuffix.size(),
                                                       kStreamSuffix.size(), kStreamSuffix) == 0
            ? id.substr(0, id.size() - kStreamSuffix.size())
            : id;
    const std::string suffix = "@" + kPidPrefix;
    size_t at = bare.find('@');
    return at != std::string::npos && bare.compare(0, at, MixAll::cachedIpStr()) == 0 &&
           bare.find(suffix) == at && bare.size() > suffix.size() + at &&
           bare.find('@', at + 1) == std::string::npos;
}

// ---------------------------------------------------------------- pure string part
static void testBuildMqClientId() {
    expectEq(buildMqClientId("10.0.0.1", "inst"), "10.0.0.1@inst", "clientId starts with the IP");
    expectEq(buildMqClientId("10.0.0.1", "inst", "unit-a"), "10.0.0.1@inst@unit-a",
             "unitName is the suffix of the third segment");
    expectEq(buildMqClientId("10.0.0.1", "inst", "   "), "10.0.0.1@inst", "a blank unitName is not appended");
    expectEq(buildMqClientId("10.0.0.1", "inst", ""), "10.0.0.1@inst", "an empty unitName is not appended");
    expectEq(buildMqClientId("10.0.0.1", "inst", "", false), "10.0.0.1@inst",
             "no suffix is added when stream is off");
    expectEq(buildMqClientId("10.0.0.1", "inst", "", true), "10.0.0.1@inst@STREAM",
             "@STREAM ends the id when stream is on");
    expectEq(buildMqClientId("10.0.0.1", "inst", "unit-a", true), "10.0.0.1@inst@unit-a@STREAM",
             "ordering when unitName and STREAM are both present");
    expectEq(buildMqClientId("10.0.0.1", "inst", "  ", true), "10.0.0.1@inst@STREAM",
             "a blank unitName takes no slot, STREAM still follows instanceName");
}

static void testChangeInstanceNameToPID() {
    expectEq(changeInstanceNameToPID("inst"), "inst", "an explicitly set instanceName is kept verbatim");
    expectEq(changeInstanceNameToPID("ADMIN"), "ADMIN", "the admin default name is not the DEFAULT one");
    const std::string rewritten = changeInstanceNameToPID(MixAll::DEFAULT_INSTANCE_NAME);
    CHECK(rewritten.rfind(kPidPrefix, 0) == 0, "the default name becomes <pid>#<nanoTime>");
    expectEq(changeInstanceNameToPID(rewritten), rewritten, "the rewrite is idempotent");
    CHECK(changeInstanceNameToPID(MixAll::DEFAULT_INSTANCE_NAME) != rewritten,
          "two rewrites in the same process give different names");
}

static void testBuildClientId() {
    expectEq(buildClientId("inst"), MixAll::cachedIpStr() + "@inst",
             "default clientId = local IP@instanceName");
    expectEq(buildClientId("inst", "unit-a", true),
             MixAll::cachedIpStr() + "@inst@unit-a@STREAM",
             "buildClientId matches buildMQClientId (unitName first, STREAM last)");
    CHECK(MixAll::cachedIpStr() == MixAll::cachedIpStr(), "the local IP is probed only once");
}

// ---------------------------------------------------------------- facade stamping
// The name server address points at 127.0.0.1:1 (no broker listens there): the clientId is stamped
// **before** the instance is built, so the stamped value can be read back whether or not start() throws on network failure.
template <typename T>
static std::string stampedClientId(T& client) {
    try {
        client.start();
    } catch (...) {
    }
    std::string id = client.clientId();
    try {
        client.shutdown();
    } catch (...) {
    }
    return id;
}

static void testProducerStampsClientId() {
    DefaultMQProducer p("PG_clientid_shape");
    p.setNamesrvAddr("127.0.0.1:1");
    const std::string id = stampedClientId(p);
    CHECK(isRewrittenClientId(id), ("producer clientId is IP@<pid>#<nanoTime>: " + id).c_str());
    CHECK(id.find(MixAll::DEFAULT_INSTANCE_NAME) == std::string::npos,
          ("a producer must not keep the DEFAULT instanceName: " + id).c_str());
}

static void testTwoProducersDoNotCollide() {
    // The old second-granularity timestamp always collides inside the same second; a collision means two producers share one clientId
    DefaultMQProducer a("PG_clientid_a");
    DefaultMQProducer b("PG_clientid_b");
    a.setNamesrvAddr("127.0.0.1:1");
    b.setNamesrvAddr("127.0.0.1:1");
    const std::string idA = stampedClientId(a);
    const std::string idB = stampedClientId(b);
    CHECK(!idA.empty() && !idB.empty() && idA != idB,
          ("two producers in the same process get different clientIds: " + idA + " / " + idB).c_str());
}

static void testExplicitInstanceNameIsKept() {
    DefaultMQProducer p("PG_clientid_named");
    p.setInstanceName("clientid-parity-fixed");
    p.setNamesrvAddr("127.0.0.1:1");
    expectEq(stampedClientId(p), MixAll::cachedIpStr() + "@clientid-parity-fixed",
             "an explicit instanceName enters the clientId verbatim");
}

static void testBroadcastConsumerKeepsDefaultInstanceName() {
    DefaultLitePullConsumer c("CID_clientid_broadcast");
    c.setNamesrvAddr("127.0.0.1:1");
    c.setMessageModel(MessageModel::BROADCASTING);
    c.subscribe("clientid-parity-topic", "*");
    expectEq(stampedClientId(c), MixAll::cachedIpStr() + "@DEFAULT" + kStreamSuffix,
             "a broadcasting consumer keeps DEFAULT");

    DefaultLitePullConsumer clustered("CID_clientid_clustering");
    clustered.setNamesrvAddr("127.0.0.1:1");
    clustered.subscribe("clientid-parity-topic", "*");
    const std::string id = stampedClientId(clustered);
    CHECK(isRewrittenClientId(id), ("a CLUSTERING consumer rewrites the instanceName: " + id).c_str());
}

static void testUnitAndStreamDefaultsPerFacade() {
    DefaultMQProducer producer("PG_clientid_defaults");
    DefaultMQPullConsumer pull("CID_clientid_pull_defaults");
    DefaultLitePullConsumer lite("CID_clientid_lite_defaults");
    DefaultMQPushConsumer push("CID_clientid_push_defaults");
    DefaultMQAdminExt admin;
    CHECK(!producer.isEnableStreamRequestType(), "a producer does not enable stream by default");
    CHECK(!push.isEnableStreamRequestType(), "a push consumer does not enable stream by default");
    CHECK(!admin.isEnableStreamRequestType(), "admin does not enable stream by default");
    CHECK(pull.isEnableStreamRequestType(), "a pull consumer enables stream by default");
    CHECK(lite.isEnableStreamRequestType(), "a lite consumer enables stream by default");
    CHECK(!producer.isUnitMode() && !pull.isUnitMode() && !lite.isUnitMode() &&
              !push.isUnitMode(),
          "unitMode defaults to false");
    // unitName defaults to empty: the clientId must not gain an extra @segment out of nowhere
    CHECK(producer.unitName().empty() && pull.unitName().empty() && lite.unitName().empty() &&
              push.unitName().empty() && admin.unitName().empty(),
          "unitName defaults to empty");
}

static void testUnitNameAndStreamReachClientId() {
    // An explicit instanceName makes the clientId predictable, so the assertions cover concatenation order rather than the rewrite logic.
    DefaultMQProducer p("PG_clientid_unit_stream");
    p.setNamesrvAddr("127.0.0.1:1");
    p.setInstanceName("clientid-unit-stream");
    p.setUnitName("unitA");
    expectEq(stampedClientId(p), MixAll::cachedIpStr() + "@clientid-unit-stream@unitA",
             "producer clientId carries unitName (no @STREAM while stream is off)");

    DefaultMQProducer ps("PG_clientid_stream_on");
    ps.setNamesrvAddr("127.0.0.1:1");
    ps.setInstanceName("clientid-stream-on");
    ps.setUnitName("unitA");
    ps.setEnableStreamRequestType(true);
    expectEq(stampedClientId(ps),
             MixAll::cachedIpStr() + "@clientid-stream-on@unitA" + kStreamSuffix,
             "with stream enabled the clientId ends in @STREAM");

    // A pull consumer enables stream by default: unitName + @STREAM appear together
    DefaultMQPullConsumer pc("CID_clientid_pull_stream");
    pc.setNamesrvAddr("127.0.0.1:1");
    pc.setInstanceName("clientid-pull-stream");
    pc.setUnitName("unitB");
    expectEq(stampedClientId(pc),
             MixAll::cachedIpStr() + "@clientid-pull-stream@unitB" + kStreamSuffix,
             "pull consumer clientId = IP@instance@unit@STREAM");
}

int main() {
    testBuildMqClientId();
    testChangeInstanceNameToPID();
    testBuildClientId();
    testProducerStampsClientId();
    testTwoProducersDoNotCollide();
    testExplicitInstanceNameIsKept();
    testBroadcastConsumerKeepsDefaultInstanceName();
    testUnitAndStreamDefaultsPerFacade();
    testUnitNameAndStreamReachClientId();

    std::cout << "[PASS] " << g_pass << " checks, [FAIL] " << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
