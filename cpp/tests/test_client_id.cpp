// clientId 口径与 Java `ClientConfig#buildMQClientId` 对齐的回归守卫。
//
// Java 的默认 clientId 是 `<本机 IP>@<instanceName>`，且 instanceName 还是默认值
// "DEFAULT" 时会在 start() 里被就地改写成 `<pid>#<nanoTime>`。旧的
// `instanceName@时间戳@pid@seq` 把唯一性做在后缀里，既不像 Java 也让 clientId 变长；
// 换成 Java 口径后唯一性来自 instanceName —— 而唯一性本身是必须的：broker 的消费组
// channel 表以 clientId 为键，撞了就等于两个客户端在 broker 侧互相顶掉。
#include <cstdint>
#include <iostream>
#include <string>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
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

static void expectEq(const std::string& actual, const std::string& expected, const char* what) {
    CHECK(actual == expected, (std::string(what) + " (actual=" + actual + ", expected=" + expected + ")").c_str());
}

/** `<IP>@<pid>#<纳秒>`：只有默认 instanceName 被改写后才长成这样。 */
static bool isRewrittenClientId(const std::string& id) {
    const std::string suffix = "@" + kPidPrefix;
    size_t at = id.find('@');
    return at != std::string::npos && id.compare(0, at, MixAll::cachedIpStr()) == 0 &&
           id.find(suffix) == at && id.size() > suffix.size() + at &&
           id.find('@', at + 1) == std::string::npos;
}

// ---------------------------------------------------------------- 纯字符串部分
static void testBuildMqClientId() {
    expectEq(buildMqClientId("10.0.0.1", "inst"), "10.0.0.1@inst", "clientId 以 IP 开头");
    expectEq(buildMqClientId("10.0.0.1", "inst", "unit-a"), "10.0.0.1@inst@unit-a",
             "unitName 是第三段后缀");
    // Java `UtilAll.isBlank(unitName)`：空白等同于没有
    expectEq(buildMqClientId("10.0.0.1", "inst", "   "), "10.0.0.1@inst", "空白 unitName 不拼");
    expectEq(buildMqClientId("10.0.0.1", "inst", ""), "10.0.0.1@inst", "空 unitName 不拼");
}

static void testChangeInstanceNameToPID() {
    expectEq(changeInstanceNameToPID("inst"), "inst", "显式设置的 instanceName 原样保留");
    expectEq(changeInstanceNameToPID("ADMIN"), "ADMIN", "admin 的默认名不是 Java 的 DEFAULT");
    const std::string rewritten = changeInstanceNameToPID(MixAll::DEFAULT_INSTANCE_NAME);
    CHECK(rewritten.rfind(kPidPrefix, 0) == 0, "默认名换成 <pid>#<nanoTime>");
    // Java 是就地覆盖字段：第二次调用不许再换一个名字，否则重启就换 clientId
    expectEq(changeInstanceNameToPID(rewritten), rewritten, "改写是幂等的");
    CHECK(changeInstanceNameToPID(MixAll::DEFAULT_INSTANCE_NAME) != rewritten,
          "同进程两次改写给出不同名字");
}

static void testBuildClientId() {
    expectEq(buildClientId("inst"), MixAll::cachedIpStr() + "@inst",
             "默认 clientId = 本机 IP@instanceName");
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
        // 校验顺序、网络失败都不是本用例关心的（Java 也是先定 clientId 再碰网络）
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
    // 无条件改写：默认名 DEFAULT 不会出现在 clientId 里
    //（Java 只放过 CLIENT_INNER_PRODUCER，本端口没有内部生产者）
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
    // Java 的三个消费者 impl 都只在 CLUSTERING 时改写 instanceName：广播模式保持
    // "DEFAULT"（Java 的 MQClientManager 因此让同进程的广播消费者复用同一份实例）。
    DefaultLitePullConsumer c("CID_clientid_broadcast");
    c.setNamesrvAddr("127.0.0.1:1");
    c.setMessageModel(MessageModel::BROADCASTING);
    c.subscribe("clientid-parity-topic", "*");
    expectEq(stampedClientId(c), MixAll::cachedIpStr() + "@DEFAULT",
             "广播消费者保持 DEFAULT");

    DefaultLitePullConsumer clustered("CID_clientid_clustering");
    clustered.setNamesrvAddr("127.0.0.1:1");
    clustered.subscribe("clientid-parity-topic", "*");
    const std::string id = stampedClientId(clustered);
    CHECK(isRewrittenClientId(id), ("CLUSTERING 消费者改写 instanceName: " + id).c_str());
}

int main() {
    testBuildMqClientId();
    testChangeInstanceNameToPID();
    testBuildClientId();
    testProducerStampsClientId();
    testTwoProducersDoNotCollide();
    testExplicitInstanceNameIsKept();
    testBroadcastConsumerKeepsDefaultInstanceName();

    std::cout << "[PASS] " << g_pass << " checks, [FAIL] " << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
