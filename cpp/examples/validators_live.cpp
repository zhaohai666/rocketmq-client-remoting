// Validators / TopicValidator 真机验证（任务 #32）。
// 用法：rmq_validators_live 127.0.0.1:9876
//
// 与 python/verify_validators_live.py、rust/examples/live_validators.rs 同场景对拍：
// 四语言要拿同一份断言证明「非法名字在本地第一行就被拦掉，合法名字照常在集群收发」。
//
// 前置：NameServer + Broker 已起（普通配置，不需要 ACL/TLS/trace）。
//
// 场景：
//   S1 发送路径：空白/超长/非法字符 topic、禁发的 broker 内部流水、body 三档、
//      INNER_MULTI_DISPATCH 分隔符 —— 全部在 <50ms 内本地拒（namesrv 就在场）
//   S2 批量路径：逐条 checkMessage（成员非法也要拦）+ 同质性检查
//   S3 生产者 start()：保留组 / 非法字符 / 超长三道门 + 等长(120)边界放行，失败不进 started 态
//   S4 正腿：合法名字照常在集群收发（push + lite 两路消费者各收到 3 条）
//   S5 对照腿：合法但**不存在**的 topic 要走真往返（broker 自动建出来），
//      比本地拒慢一个数量级以上 —— 这条量化了「本地校验省掉的是什么」
//   S6 pull / lite 的组名门 + 合法 pull 组能查到队列与位点
//   S7 createTopic 的本地拒（空白 / 非法字符 / 系统 topic）
//
// 码值口径（四语言一致，见 validators.h 注释）：Java 的 MQClientException(String, Throwable)
// 用 responseCode=-1 表示「纯客户端错误」；本项目（Python 先定、其余照抄）用
// SYSTEM_ERROR/UNKNOWN=1，只有 checkMessage 的 body 三档 + LMQ 那一档带 MESSAGE_ILLEGAL(13)。
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/validators.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/codes.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;
std::string gNamesrv = "127.0.0.1:9876";

/// 本地校验的超时预算（毫秒）：真往返那一腿是 S5 实测的几十毫秒起步。
constexpr double kLocalBudgetMs = 50.0;
constexpr int32_t kQueueNums = 4;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

std::string ms(double v) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.2fms", v);
    return buf;
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

int64_t stamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

bool startsWith(const std::string& s, const std::string& p) { return s.rfind(p, 0) == 0; }

/// 跑一个 thunk，把抛出的异常按类型摊平成 (是否抛, 文案, 响应码, 耗时)。
/// 非 MQClientException（如批量的 std::invalid_argument）码记 0，表示"不在客户端错误码体系里"。
struct Captured {
    bool threw = false;
    std::string what;
    int32_t code = 0;
    double elapsedMs = 0.0;
};

Captured capture(const std::function<void()>& fn) {
    Captured c;
    const int64_t began = nowMs();
    try {
        fn();
    } catch (const MQClientException& e) {
        c.threw = true;
        c.what = e.what();
        c.code = e.getResponseCode();
    } catch (const std::exception& e) {
        c.threw = true;
        c.what = e.what();
    }
    c.elapsedMs = static_cast<double>(nowMs() - began);
    return c;
}

/// 断言「本地就拒」：抛了、文案命中、码值对、且没慢到像跑了网络。返回实测耗时。
double expectLocalReject(const std::string& name, const std::function<void()>& fn,
                         const std::string& needle, int32_t wantCode) {
    Captured c = capture(fn);
    check(name,
          c.threw && c.what.find(needle) != std::string::npos && c.code == wantCode &&
              c.elapsedMs < kLocalBudgetMs,
          (c.threw ? c.what : std::string("没有抛异常")) + "  code=" + std::to_string(c.code) +
              "  " + ms(c.elapsedMs));
    return c.elapsedMs;
}

/// 断言「start() 本地就拒，且失败后没有把自己标成 started」。
double expectStartReject(const std::string& name, const std::function<bool()>& startedFlag,
                         const std::function<void()>& startFn, const std::string& needle) {
    Captured c = capture(startFn);
    check(name, c.threw && c.what.find(needle) != std::string::npos && !startedFlag(),
          (c.threw ? c.what : std::string("没有抛异常")) + "  " + ms(c.elapsedMs));
    return c.elapsedMs;
}

Message msg(const std::string& topic, const std::string& body) {
    return Message(topic, Bytes(body.begin(), body.end()));
}

std::string repeatedChar(char ch, size_t n) { return std::string(n, ch); }

const char* kIllegalTail = " contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$";

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return pred();
}

/// 路由查询在没有路由时会抛（TOPIC_NOT_EXIST / no route），这里摊平成"队列数 0"。
size_t routeQueueCount(const std::function<std::vector<MessageQueue>()>& fetch) {
    try {
        return fetch().size();
    } catch (const std::exception&) {
        return 0;
    }
}

// 收集消费到的消息（并发安全），按 body 前缀筛
class Collector : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(m_);
        for (const MessageExt& m : msgs) msgs_.push_back(m);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    std::vector<MessageExt> byPrefix(const std::string& prefix) {
        std::lock_guard<std::mutex> lk(m_);
        std::vector<MessageExt> out;
        for (const MessageExt& m : msgs_) {
            if (startsWith(bodyOf(m), prefix)) out.push_back(m);
        }
        return out;
    }

private:
    std::mutex m_;
    std::vector<MessageExt> msgs_;
};

// ---------------------------------------------------------------- S1 发送路径

double s1_sendPathRejects(DefaultMQProducer& p) {
    std::printf("== S1 发送路径的本地校验（namesrv 在场，仍然不碰网络）==\n");
    const std::string longTopic = repeatedChar('a', 128);
    struct Neg {
        const char* label;
        std::string topic;
        std::string needle;
        int32_t code;
    };
    const Neg negs[] = {
        {"S1a 空 topic", "", "The specified topic is blank", ResponseCode::SYSTEM_ERROR},
        {"S1b 全空白 topic", "   ", "The specified topic is blank", ResponseCode::SYSTEM_ERROR},
        {"S1c 超长 topic(128)", longTopic, "is longer than topic max length 127",
         ResponseCode::SYSTEM_ERROR},
        {"S1d 带点 topic", "Validators.Live", kIllegalTail, ResponseCode::SYSTEM_ERROR},
        {"S1e 非 ASCII topic", "Validators中文", kIllegalTail, ResponseCode::SYSTEM_ERROR},
        {"S1f 禁发的 broker 内部流水", "SCHEDULE_TOPIC_XXXX", "is forbidden",
         ResponseCode::SYSTEM_ERROR},
    };
    double local = 0.0;
    for (const Neg& n : negs) {
        local += expectLocalReject(
            n.label, [&] { p.send(msg(n.topic, "body")); }, n.needle, n.code);
    }
    local /= static_cast<double>(sizeof(negs) / sizeof(negs[0]));

    // body 三档 + LMQ 分隔符：只有这几档带 MESSAGE_ILLEGAL(13)
    expectLocalReject("S1g 空 body", [&] { p.send(msg("ValidatorsPositive", "")); },
                      "the message body length is zero", ResponseCode::MESSAGE_ILLEGAL);
    {
        Message m = msg("ValidatorsPositive", "x");
        m.hasBody = false;
        expectLocalReject("S1h null body", [&] { p.send(m); }, "the message body is null",
                          ResponseCode::MESSAGE_ILLEGAL);
    }
    // 超 maxMessageSize 走 Validators（producer 的 checkMessage 是 protected，
    // 且语义就是转发到 Validators::checkMessage(msg, maxMessageSize_)）
    expectLocalReject("S1i 超 maxMessageSize",
                      [&] { Validators::checkMessage(msg("ValidatorsPositive", "12345"), 4); },
                      "the message body size over max value, MAX: 4",
                      ResponseCode::MESSAGE_ILLEGAL);
    check("S1j 恰好等于 maxMessageSize 放行",
          !capture([&] { Validators::checkMessage(msg("ValidatorsPositive", "1234"), 4); }).threw);
    {
        Message m = msg("ValidatorsPositive", "hello");
        m.setUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                          std::string("a") + kFileSeparator + "b");
        expectLocalReject("S1k INNER_MULTI_DISPATCH 带路径分隔符", [&] { p.send(m); },
                          "INNER_MULTI_DISPATCH", ResponseCode::MESSAGE_ILLEGAL);
    }
    {
        Message legal = msg("ValidatorsPositive", "hello");
        legal.setUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%queue:testCID");
        check("S1l 常规 LMQ 取值不误杀",
              !capture([&] { Validators::checkMessage(legal, 4 << 20); }).threw);
    }
    // 顺序：先 topic → 禁发名单 → body。空 body + 非法 topic 必须报 topic
    {
        Captured c = capture([&] { p.send(msg("bad topic", "")); });
        check("S1m 校验顺序：topic 先于 body",
              c.threw && c.what.find("contains illegal characters") != std::string::npos,
              c.threw ? c.what : std::string("没有抛异常"));
    }
    return local;
}

// ---------------------------------------------------------------- S2 批量

void s2_batchRejects(DefaultMQProducer& p, const std::string& topic) {
    std::printf("== S2 批量路径的逐条校验 ==\n");
    expectLocalReject(
        "S2a 批量里有一条非法 topic",
        [&] { p.sendBatch({msg(topic, "ok-1"), msg("bad.topic", "ok-2")}); },
        std::string("The specified topic[bad.topic]") + kIllegalTail,
        ResponseCode::SYSTEM_ERROR);

    expectLocalReject(
        "S2b 批量里有一条空 body",
        [&] { p.sendBatch({msg(topic, "ok-1"), msg(topic, "")}); },
        "the message body length is zero", ResponseCode::MESSAGE_ILLEGAL);

    // 同质性检查在 Java 抛 IllegalArgumentException（不是 MQClientException）—— 码记 0
    Captured mixed = capture([&] {
        p.sendBatch({msg(topic, "ok-1"), msg(topic + "Other", "ok-2")});
    });
    check("S2c 批量 topic 不同质被拒（非 MQClientException 口径）",
          mixed.threw && mixed.what.find("should be the same") != std::string::npos &&
              mixed.code == 0 && mixed.elapsedMs < kLocalBudgetMs,
          mixed.threw ? mixed.what : std::string("没有抛异常"));
}

// ---------------------------------------------------------------- S3 生产者组名门

void s3_producerGroupGates() {
    std::printf("== S3 生产者 start() 的组名校验门 ==\n");
    const std::string longGroup = repeatedChar('g', 121);
    struct Neg {
        const char* label;
        std::string group;
        std::string needle;
    };
    const Neg negs[] = {
        {"S3a 保留组 DEFAULT_PRODUCER", MixAll::DEFAULT_PRODUCER_GROUP,
         "producerGroup can not equal DEFAULT_PRODUCER, please specify another one."},
        {"S3b 带空格的组", "bad group",
         std::string("the specified group[bad group]") + kIllegalTail},
        {"S3c 超长组(121)", longGroup, "is longer than group max length: 120"},
    };
    for (const Neg& n : negs) {
        // namesrv 指向**真实**集群也照样拒 —— 挡的是本地第一行，与网络可达性无关
        DefaultMQProducer p(n.group);
        p.setNamesrvAddr(gNamesrv);
        expectStartReject(n.label, [&] { return p.isStarted(); }, [&] { p.start(); }, n.needle);
    }
    // 等长边界：120 字符必须放行（Java 用 >，不是 >=）；这里真起起来再关掉
    DefaultMQProducer edge(repeatedChar('g', 120));
    edge.setNamesrvAddr(gNamesrv);
    Captured c = capture([&] { edge.start(); });
    check("S3d 120 字符组名放行（长度判定是 > 而非 >=）",
          !c.threw || c.what.find("group max length") == std::string::npos,
          c.threw ? c.what : std::string("start 未因组名长度失败"));
    if (edge.isStarted()) edge.shutdown();
}

// ---------------------------------------------------------------- S4 正腿

void s4_positiveLeg(DefaultMQProducer& p, const std::string& topic, const std::string& group,
                    const std::string& prefix) {
    std::printf("== S4 正腿：合法名字照常在集群收发 ==\n");
    auto collector = std::make_shared<Collector>();
    DefaultMQPushConsumer cons(group);
    cons.setNamesrvAddr(gNamesrv);
    cons.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    cons.subscribe(topic, "*");
    cons.setMessageListener(collector);
    Captured started = capture([&] { cons.start(); });
    check("S4a 合法组名的 push 消费者可启动", !started.threw,
          started.threw ? started.what : std::string("ok"));
    if (started.threw) return;

    DefaultLitePullConsumer lite(group + "_lite");
    lite.setNamesrvAddr(gNamesrv);
    lite.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    lite.subscribe(topic, "*");
    Captured liteStarted = capture([&] { lite.start(); });
    check("S4b 合法组名的 lite 消费者可启动", !liteStarted.threw,
          liteStarted.threw ? liteStarted.what : std::string("ok"));

    // 消费者先起来再发消息：新组 + CONSUME_FROM_LAST_OFFSET 取的是**启动那一刻**的队尾位点
    bool assigned = waitUntil([&] { return !lite.assignment().empty(); }, 20000);
    check("S4c lite 拿到队列分配", assigned,
          std::to_string(lite.assignment().size()) + " 个队列");

    for (int32_t i = 0; i < 3; ++i) {
        Captured s = capture([&] { p.send(msg(topic, prefix + "-body-" + std::to_string(i)), 5000); });
        check("S4d 合法 topic 发送成功 #" + std::to_string(i), !s.threw,
              s.threw ? s.what : std::string("SEND_OK"));
    }

    bool gotAll = waitUntil([&] { return collector->byPrefix(prefix).size() >= 3; }, 30000);
    check("S4e push 消费者收到全部 3 条", gotAll,
          "收到 " + std::to_string(collector->byPrefix(prefix).size()) + " 条");

    size_t polled = 0;
    const int64_t deadline = nowMs() + 20000;
    while (polled < 3 && nowMs() < deadline) {
        polled += lite.poll(1000).size();
    }
    check("S4f lite 消费者 poll 到全部 3 条", polled >= 3, "poll 到 " + std::to_string(polled) + " 条");
    lite.shutdown();
    cons.shutdown();
}

// ---------------------------------------------------------------- S5 对照腿

void s5_controlLeg(DefaultMQProducer& p, double localMs) {
    std::printf("== S5 对照腿：本地校验省掉的是什么 ==\n");
    // 合法但**不存在**的 topic：本地放行 → 真往返（broker 按 TBW102 自动建出来）
    const std::string missing = "ValidatorsMissing" + std::to_string(stamp());
    Captured c = capture([&] { p.send(msg(missing, "x"), 5000); });
    check("S5a 合法但不存在的 topic 不被本地误伤（broker 自动建出来）",
          !c.threw || c.what.find(kIllegalTail) == std::string::npos,
          (c.threw ? c.what : std::string("SEND_OK")) + "  " + ms(c.elapsedMs));
    const double baseline = localMs < 0.05 ? 0.05 : localMs;
    check("S5b 集群腿比本地反腿慢一个数量级以上", c.elapsedMs > 10.0 * baseline,
          ms(c.elapsedMs) + " vs 本地 " + ms(baseline));
}

// ---------------------------------------------------------------- S6 消费者侧

void s6_consumerGates(const std::string& topic) {
    std::printf("== S6 pull/lite 的组名门 + 合法 pull 组查位点 ==\n");
    {
        DefaultMQPullConsumer pc(MixAll::DEFAULT_CONSUMER_GROUP);
        pc.setNamesrvAddr(gNamesrv);
        expectStartReject("S6a pull 消费者挡 DEFAULT_CONSUMER", [&] { return pc.isStarted(); },
                          [&] { pc.start(); },
                          "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
    }
    {
        DefaultLitePullConsumer lite(MixAll::DEFAULT_CONSUMER_GROUP);
        lite.setNamesrvAddr(gNamesrv);
        lite.subscribe(topic, "*");
        expectStartReject("S6b lite 消费者挡 DEFAULT_CONSUMER", [&] { return lite.isStarted(); },
                          [&] { lite.start(); },
                          "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
    }
    {
        DefaultLitePullConsumer lite("bad group");
        lite.setNamesrvAddr(gNamesrv);
        lite.subscribe(topic, "*");
        expectStartReject("S6c lite 消费者挡非法字符组", [&] { return lite.isStarted(); },
                          [&] { lite.start(); },
                          std::string("the specified group[bad group]") + kIllegalTail);
    }
    {
        DefaultMQPullConsumer pc("GID_validators_pull_" + std::to_string(stamp()));
        pc.setNamesrvAddr(gNamesrv);
        Captured started = capture([&] { pc.start(); });
        bool ok = !started.threw;
        size_t queues = 0;
        int64_t maxOffset = -1;
        if (ok) {
            queues = routeQueueCount([&] { return pc.fetchSubscribeMessageQueues(topic); });
            if (queues > 0) {
                std::vector<MessageQueue> mqs = pc.fetchSubscribeMessageQueues(topic);
                try {
                    maxOffset = pc.maxOffset(mqs[0]);
                } catch (const std::exception& e) {
                    std::printf("  ... maxOffset 失败: %s\n", e.what());
                }
            }
        }
        check("S6d 合法组名的 pull 消费者可启动并查到位点",
              ok && queues >= static_cast<size_t>(kQueueNums) && maxOffset >= 0,
              started.threw ? started.what
                            : ("queues=" + std::to_string(queues) +
                               " maxOffset=" + std::to_string(maxOffset)));
        if (ok) pc.shutdown();
    }
}

// ---------------------------------------------------------------- S7 createTopic

void s7_createTopicRejects(DefaultMQProducer& p) {
    std::printf("== S7 createTopic 的本地拒 ==\n");
    expectLocalReject("S7a createTopic 挡非法字符", [&] { p.createTopic(MixAll::DEFAULT_TOPIC, "bad topic", 4); },
                      std::string("The specified topic[bad topic]") + kIllegalTail,
                      ResponseCode::SYSTEM_ERROR);
    expectLocalReject("S7b createTopic 挡系统 topic",
                      [&] { p.createTopic(MixAll::DEFAULT_TOPIC, "RMQ_SYS_TRACE_TOPIC", 4); },
                      "is conflict with system topic", ResponseCode::SYSTEM_ERROR);
    expectLocalReject("S7c createTopic 挡空 topic",
                      [&] { p.createTopic(MixAll::DEFAULT_TOPIC, "", 4); },
                      "The specified topic is blank", ResponseCode::SYSTEM_ERROR);
}

void report() {
    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc > 1) gNamesrv = argv[1];
    const int64_t st = stamp();
    const std::string sfx = std::to_string(st);
    const std::string topic = "ValidatorsCpp_" + sfx;
    const std::string group = "GID_validators_cpp_" + sfx;
    const std::string prefix = "validators-cpp-" + sfx;

    std::printf("=== RocketMQ validators live verify (cpp) ===\n");
    std::printf("namesrv=%s stamp=%lld\n", gNamesrv.c_str(), static_cast<long long>(st));

    DefaultMQProducer p("GID_validators_cpp_producer_" + sfx);
    p.setNamesrvAddr(gNamesrv);
    Captured started = capture([&] { p.start(); });
    if (started.threw) {
        check("S0 生产者启动", false, started.what);
        report();
        return 1;
    }
    std::printf("  [PASS] S0 生产者启动  group=%s\n", p.producerGroup().c_str());
    ++gPass;

    const double localMs = s1_sendPathRejects(p);
    s2_batchRejects(p, topic);
    s3_producerGroupGates();

    Captured created = capture([&] { p.createTopic(MixAll::DEFAULT_TOPIC, topic, kQueueNums); });
    if (created.threw) {
        check("S4 前置：建 topic", false, created.what);
        report();
        p.shutdown();
        return 1;
    }
    auto queues = [&] {
        return routeQueueCount([&] { return p.fetchPublishMessageQueues(topic); });
    };
    bool routed = waitUntil([&] { return queues() >= static_cast<size_t>(kQueueNums); }, 20000);
    check("S4 前置：路由可发现", routed, "queues=" + std::to_string(queues()));
    if (routed) {
        s4_positiveLeg(p, topic, group, prefix);
        s6_consumerGates(topic);
    }
    s5_controlLeg(p, localMs);
    s7_createTopicRejects(p);

    p.shutdown();
    report();
    return gFail == 0 ? 0 : 1;
}
