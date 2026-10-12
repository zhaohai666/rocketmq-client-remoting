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

/** `<IP>@<pid>#<纳秒>`：只有默认 instanceName 被改写后才长成这样。 */
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

// ---------------------------------------------------------------- 纯字符串部分
static void testBuildMqClientId() {
    expectEq(buildMqClientId("10.0.0.1", "inst"), "10.0.0.1@inst", "clientId 以 IP 开头");
    expectEq(buildMqClientId("10.0.0.1", "inst", "unit-a"), "10.0.0.1@inst@unit-a",
             "unitName 是第三段后缀");
    expectEq(buildMqClientId("10.0.0.1", "inst", "   "), "10.0.0.1@inst", "空白 unitName 不拼");
    expectEq(buildMqClientId("10.0.0.1", "inst", ""), "10.0.0.1@inst", "空 unitName 不拼");
    expectEq(buildMqClientId("10.0.0.1", "inst", "", false), "10.0.0.1@inst",
             "stream 关闭时不加后缀");
    expectEq(buildMqClientId("10.0.0.1", "inst", "", true), "10.0.0.1@inst@STREAM",
             "stream 开启时 @STREAM 收尾");
    expectEq(buildMqClientId("10.0.0.1", "inst", "unit-a", true), "10.0.0.1@inst@unit-a@STREAM",
             "unitName 与 STREAM 同时存在时的顺序");
    expectEq(buildMqClientId("10.0.0.1", "inst", "  ", true), "10.0.0.1@inst@STREAM",
             "空白 unitName 不占位，STREAM 仍紧跟 instanceName");
}

static void testChangeInstanceNameToPID() {
    expectEq(changeInstanceNameToPID("inst"), "inst", "显式设置的 instanceName 原样保留");
    expectEq(changeInstanceNameToPID("ADMIN"), "ADMIN", "admin 的默认名不是 Java 的 DEFAULT");
    const std::string rewritten = changeInstanceNameToPID(MixAll::DEFAULT_INSTANCE_NAME);
    CHECK(rewritten.rfind(kPidPrefix, 0) == 0, "默认名换成 <pid>#<nanoTime>");
    expectEq(changeInstanceNameToPID(rewritten), rewritten, "改写是幂等的");
    CHECK(changeInstanceNameToPID(MixAll::DEFAULT_INSTANCE_NAME) != rewritten,
          "同进程两次改写给出不同名字");
}

static void testBuildClientId() {
    expectEq(buildClientId("inst"), MixAll::cachedIpStr() + "@inst",
             "默认 clientId = 本机 IP@instanceName");
    expectEq(buildClientId("inst", "unit-a", true),
             MixAll::cachedIpStr() + "@inst@unit-a@STREAM",
             "buildClientId 与 buildMQClientId 同口径（unitName 在前、STREAM 收尾）");
    CHECK(MixAll::cachedIpStr() == MixAll::cachedIpStr(), "本机 IP 只探测一次");
}

// ---------------------------------------------------------------- facade 盖章
// 名字服务地址指向 127.0.0.1:1（不会有 broker 监听）：clientId 是在建实例**之前**
// 盖好的，所以无论 start() 是否因为网络失败抛错，都能读回盖好的值。
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
    CHECK(isRewrittenClientId(id), ("生产者 clientId 是 IP@<pid>#<nanoTime>: " + id).c_str());
    CHECK(id.find(MixAll::DEFAULT_INSTANCE_NAME) == std::string::npos,
          ("生产者不该留着 DEFAULT instanceName: " + id).c_str());
}

static void testTwoProducersDoNotCollide() {
    // 旧口径的秒级时间戳在同一秒内必然撞车，撞了就是两个生产者共用一个 clientId
    DefaultMQProducer a("PG_clientid_a");
    DefaultMQProducer b("PG_clientid_b");
    a.setNamesrvAddr("127.0.0.1:1");
    b.setNamesrvAddr("127.0.0.1:1");
    const std::string idA = stampedClientId(a);
    const std::string idB = stampedClientId(b);
    CHECK(!idA.empty() && !idB.empty() && idA != idB,
          ("同进程两个生产者 clientId 不同: " + idA + " / " + idB).c_str());
}

static void testExplicitInstanceNameIsKept() {
    DefaultMQProducer p("PG_clientid_named");
    p.setInstanceName("clientid-parity-fixed");
    p.setNamesrvAddr("127.0.0.1:1");
    expectEq(stampedClientId(p), MixAll::cachedIpStr() + "@clientid-parity-fixed",
             "显式 instanceName 原样进 clientId");
}

static void testBroadcastConsumerKeepsDefaultInstanceName() {
    DefaultLitePullConsumer c("CID_clientid_broadcast");
    c.setNamesrvAddr("127.0.0.1:1");
    c.setMessageModel(MessageModel::BROADCASTING);
    c.subscribe("clientid-parity-topic", "*");
    expectEq(stampedClientId(c), MixAll::cachedIpStr() + "@DEFAULT" + kStreamSuffix,
             "广播消费者保持 DEFAULT");

    DefaultLitePullConsumer clustered("CID_clientid_clustering");
    clustered.setNamesrvAddr("127.0.0.1:1");
    clustered.subscribe("clientid-parity-topic", "*");
    const std::string id = stampedClientId(clustered);
    CHECK(isRewrittenClientId(id), ("CLUSTERING 消费者改写 instanceName: " + id).c_str());
}

static void testUnitAndStreamDefaultsPerFacade() {
    DefaultMQProducer producer("PG_clientid_defaults");
    DefaultMQPullConsumer pull("CID_clientid_pull_defaults");
    DefaultLitePullConsumer lite("CID_clientid_lite_defaults");
    DefaultMQPushConsumer push("CID_clientid_push_defaults");
    DefaultMQAdminExt admin;
    CHECK(!producer.isEnableStreamRequestType(), "生产者默认不开 stream");
    CHECK(!push.isEnableStreamRequestType(), "推送消费者默认不开 stream");
    CHECK(!admin.isEnableStreamRequestType(), "admin 默认不开 stream");
    CHECK(pull.isEnableStreamRequestType(), "pull 消费者默认开 stream");
    CHECK(lite.isEnableStreamRequestType(), "lite 消费者默认开 stream");
    CHECK(!producer.isUnitMode() && !pull.isUnitMode() && !lite.isUnitMode() &&
              !push.isUnitMode(),
          "unitMode 默认为 false");
    // unitName 默认为空：clientId 不该凭空多出 @段
    CHECK(producer.unitName().empty() && pull.unitName().empty() && lite.unitName().empty() &&
              push.unitName().empty() && admin.unitName().empty(),
          "unitName 默认为空");
}

static void testUnitNameAndStreamReachClientId() {
    // 显式 instanceName 让 clientId 可预测，这样断言的是拼接顺序而不是改写逻辑。
    DefaultMQProducer p("PG_clientid_unit_stream");
    p.setNamesrvAddr("127.0.0.1:1");
    p.setInstanceName("clientid-unit-stream");
    p.setUnitName("unitA");
    expectEq(stampedClientId(p), MixAll::cachedIpStr() + "@clientid-unit-stream@unitA",
             "生产者 clientId 带上 unitName（stream 关时不带 @STREAM）");

    DefaultMQProducer ps("PG_clientid_stream_on");
    ps.setNamesrvAddr("127.0.0.1:1");
    ps.setInstanceName("clientid-stream-on");
    ps.setUnitName("unitA");
    ps.setEnableStreamRequestType(true);
    expectEq(stampedClientId(ps),
             MixAll::cachedIpStr() + "@clientid-stream-on@unitA" + kStreamSuffix,
             "打开 stream 后 clientId 以 @STREAM 收尾");

    // pull 消费者默认就开 stream：unitName + @STREAM 一起出现
    DefaultMQPullConsumer pc("CID_clientid_pull_stream");
    pc.setNamesrvAddr("127.0.0.1:1");
    pc.setInstanceName("clientid-pull-stream");
    pc.setUnitName("unitB");
    expectEq(stampedClientId(pc),
             MixAll::cachedIpStr() + "@clientid-pull-stream@unitB" + kStreamSuffix,
             "pull 消费者 clientId = IP@instance@unit@STREAM");
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
