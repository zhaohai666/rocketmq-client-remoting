// C++ 客户端 SQL92 过滤 + CHECK_CLIENT_CONFIG(46) 真机联调
// （对应 python/verify_sql92_live.py、cpp/tests/test_check_client_config.cpp、
//   rust/examples/live_sql92.rs、dotnet/examples/.../LiveSql92.cs 的 S1–S4：四语言对拍）。
//
// 为什么必须在真集群上跑：SQL92 这条链路最容易「静默失效」。broker 的
// ExpressionMessageFilter 在 ConsumeQueue 阶段拿不到编译好的过滤数据时**直接放行全部
// 消息**（return true），于是两种错都表现为「消费者正常启动、消息也都收到了」：
//   1. broker 没开 enablePropertyFilter ⇒ 表达式根本没被编译；
//   2. 表达式语法错 ⇒ 同上；只有 Java 的 checkClientConfig 会把它变成启动错误。
// 离线单测（tests/test_check_client_config.cpp）锁得住协议形状，锁不住 broker 真的按属性
// 过滤了。所以这里四段都验：
//   S1 线上取证：SQL92 订阅 ⇒ 启动时正好一笔 46（body 是 CheckClientRequestBody）；
//      纯 TAG 订阅 ⇒ 一笔都不发（Java ExpressionType.isTagType 短路）
//   S2 真过滤：消费者**先起来再发消息**（新消费组 + CONSUME_FROM_LAST_OFFSET 会跳过启动前
//      的消息，先发消息这一段就是假绿）：SQL92 只订阅 red ⇒ 恰好那 3 条 red；
//      TAG '*' 对照组 ⇒ 6 条全收
//   S3 空结果腿：订阅永不匹配的 color = 'green' ⇒ 一条都不收（排除「其实全放行了」）
//   S4 反证：语法错的表达式让 start() 抛 SUBSCRIPTION_PARSE_FAILED(23)，且启动就地回滚
//      （同一个对象换成合法表达式能重新 start）
//
// 用法（先起本地 5.5.1 集群，broker 需 enablePropertyFilter=true）：
//   ./rmq_sql92_live [namesrv]
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/rpchook.h"

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

// ---------------------------------------------------------------- 取证钩子

// 钩在 transport 上，抓启动期真正发出去的 46 号请求（含 body）。
// doBeforeRequest 跑在发送线程上、且一个钩子对象可能被多线程共用，必须自带锁。
class CheckConfigProbe : public RPCHook {
public:
    void doBeforeRequest(const std::string& /*remoteAddr*/, RemotingCommand& request) override {
        std::lock_guard<std::mutex> lk(mtx_);
        codes.push_back(request.code);
        if (request.code != RequestCode::CHECK_CLIENT_CONFIG) return;
        CheckClientRequestBody body;
        if (!request.body.empty() && CheckClientRequestBody::decode(request.body, body)) {
            bodies.push_back(body);
        }
    }

    int checkCount() {
        std::lock_guard<std::mutex> lk(mtx_);
        int n = 0;
        for (int32_t c : codes) {
            if (c == RequestCode::CHECK_CLIENT_CONFIG) ++n;
        }
        return n;
    }

    std::string codesStr() {
        std::lock_guard<std::mutex> lk(mtx_);
        std::string out = "[";
        for (size_t i = 0; i < codes.size(); ++i) {
            out += (i ? "," : "") + std::to_string(codes[i]);
        }
        return out + "]";
    }

    bool firstBody(CheckClientRequestBody& out) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (bodies.empty()) return false;
        out = bodies.front();
        return true;
    }

private:
    std::mutex mtx_;
    std::vector<int32_t> codes;
    std::vector<CheckClientRequestBody> bodies;
};

// ---------------------------------------------------------------- 收集监听器

// 攒下消费到的消息（监听器跑在消费线程上，sink 跨线程共享）。
struct Sink {
    std::mutex mtx;
    std::vector<std::string> bodies;
    std::vector<std::string> colors;

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
    std::set<std::string> colorSet() {
        std::lock_guard<std::mutex> lk(mtx);
        return std::set<std::string>(colors.begin(), colors.end());
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
            auto it = m.properties.find("color");
            sink_.colors.push_back(it == m.properties.end() ? std::string("<missing>")
                                                            : it->second);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    Sink& sink_;
};

// ---------------------------------------------------------------- 环境

std::string gStamp;
std::string gNsAddr;

// 一套按 stamp 隔离的 topic/group，跑完自己扫干净。
struct Env {
    DefaultMQAdminExt admin{"SQL92CPPADMIN"};
    std::vector<std::string> topics;
    std::vector<std::string> groups;

    std::string topic() {
        std::string t = "Sql92Cpp_" + gStamp;
        topics.push_back(t);
        return t;
    }
    std::string group(const std::string& kind) {
        std::string g = "GID_Sql92Cpp_" + gStamp + "_" + kind;
        groups.push_back(g);
        return g;
    }

    std::string brokerAddr() {
        try {
            const TopicRouteData route = admin.examineTopicRoute(MixAll::DEFAULT_TOPIC);
            if (!route.brokerDatas.empty()) {
                const std::string addr = route.brokerDatas.front().selectBrokerAddr();
                if (!addr.empty()) return addr;
            }
        } catch (const std::exception& e) {
            std::printf("  [diag] broker route failed: %s\n", e.what());
        }
        return "127.0.0.1:10911";
    }

    void start() {
        admin.setNamesrvAddr(gNsAddr);
        admin.start();
    }

    // 消费者通用装配：地址 + 起点 + 订阅（selector 为空时用 TAG 表达式）+ 可选取证钩子。
    // 组名在构造 DefaultMQPushConsumer 时给（本端口没有 setConsumerGroup）。
    void configure(DefaultMQPushConsumer& c, const std::string& topic,
                   const MessageSelector* selector, const std::string& expression,
                   const std::shared_ptr<RPCHook>& probe) {
        c.setNamesrvAddr(gNsAddr);
        c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
        if (probe) c.setRPCHook(probe);
        if (selector != nullptr) {
            c.subscribe(topic, *selector);
        } else {
            c.subscribe(topic, expression);
        }
    }

    int routeQueueCount(const std::string& topic) {
        try {
            const TopicRouteData route = admin.examineTopicRoute(topic);
            int n = 0;
            for (const QueueData& q : route.queueDatas) n += q.readQueueNums;
            return n;
        } catch (const std::exception&) {
            return 0;
        }
    }

    void cleanup() {
        const std::vector<std::string> topicList = topics;
        const std::vector<std::string> groupList = groups;
        const std::string addr = brokerAddr();
        for (const std::string& t : topicList) {
            try {
                admin.deleteTopic(t);
            } catch (const std::exception& e) {
                std::printf("  [diag] deleteTopic(%s) failed: %s\n", t.c_str(), e.what());
            }
        }
        for (const std::string& g : groupList) {
            // %RETRY%/%DLQ% 随订阅组一起删（只有走删组接口 broker 才会真删 retry topic）
            try {
                admin.deleteSubscriptionGroup(addr, g, true);
            } catch (const std::exception& e) {
                std::printf("  [diag] deleteSubscriptionGroup(%s) failed: %s\n", g.c_str(),
                            e.what());
            }
            try {
                admin.deleteTopic(MixAll::getRetryTopic(g));
            } catch (const std::exception&) {
            }
        }
        admin.shutdown();
    }
};

// ---------------------------------------------------------------- S1 线上取证
void s1WireEvidence(Env& env, const std::string& topic) {
    std::printf("\n---------- S1 启动期的 46 号请求 ----------\n");
    const std::string sqlGroup = env.group("SQL");
    auto probe = std::make_shared<CheckConfigProbe>();
    Sink sqlSink;
    std::shared_ptr<CollectListener> sqlListener(new CollectListener(sqlSink));
    DefaultMQPushConsumer c(sqlGroup);
    MessageSelector sel = MessageSelector::bySql("color = 'red'");
    env.configure(c, topic, &sel, std::string(), probe);
    c.setMessageListener(sqlListener);
    try {
        c.start();
        c.shutdown();
    } catch (const std::exception& e) {
        check("S1 SQL92 消费者启动成功", false, e.what());
    }
    check("S1 SQL92 订阅触发恰好一笔 CHECK_CLIENT_CONFIG(46)", probe->checkCount() == 1,
          "codes=" + probe->codesStr());
    CheckClientRequestBody body;
    const bool hasBody = probe->firstBody(body);
    const bool shapeOk = hasBody && body.group == sqlGroup && body.clientId == c.clientId()
        && body.subscriptionData.expressionType == std::string(ExpressionType::SQL92)
        && body.subscriptionData.subString == "color = 'red'"
        && body.subscriptionData.topic == topic;
    check("S1 body 是 CheckClientRequestBody（clientId / group / subscriptionData）", shapeOk,
          "hasBody=" + std::to_string(hasBody) + " group=" + body.group
              + " clientId=" + body.clientId + " want=" + c.clientId()
              + " type=" + body.subscriptionData.expressionType
              + " sub=" + body.subscriptionData.subString
              + " topic=" + body.subscriptionData.topic + " want=" + topic);

    const std::string tagGroup = env.group("TAG");
    auto tagProbe = std::make_shared<CheckConfigProbe>();
    Sink tagSink;
    std::shared_ptr<CollectListener> tagListener(new CollectListener(tagSink));
    DefaultMQPushConsumer t(tagGroup);
    env.configure(t, topic, nullptr, "tagA || tagB", tagProbe);
    t.setMessageListener(tagListener);
    try {
        t.start();
    } catch (const std::exception& e) {
        check("S1 TAG 消费者启动成功", false, e.what());
    }
    check("S1 纯 TAG 订阅一笔 46 都不发（Java isTagType 短路）", tagProbe->checkCount() == 0,
          "codes=" + tagProbe->codesStr());
    t.shutdown();
}

// ---------------------------------------------------------------- S2 / S3
void s2S3RealFiltering(Env& env, const std::string& topic, DefaultMQProducer& prod) {
    std::printf("\n---------- S2 broker 按属性过滤 / S3 永不匹配的表达式 ----------\n");
    MessageSelector redSel = MessageSelector::bySql("color = 'red'");
    MessageSelector greenSel = MessageSelector::bySql("color = 'green'");

    Sink red, green, all;
    std::shared_ptr<CollectListener> redL(new CollectListener(red));
    std::shared_ptr<CollectListener> greenL(new CollectListener(green));
    std::shared_ptr<CollectListener> allL(new CollectListener(all));
    DefaultMQPushConsumer redC(env.group("FILTER"));
    DefaultMQPushConsumer greenC(env.group("NONE"));
    DefaultMQPushConsumer allC(env.group("ALL"));
    env.configure(redC, topic, &redSel, std::string(), nullptr);
    env.configure(greenC, topic, &greenSel, std::string(), nullptr);
    env.configure(allC, topic, nullptr, "*", nullptr);
    redC.setMessageListener(redL);
    greenC.setMessageListener(greenL);
    allC.setMessageListener(allL);
    // 必须先起来再发：新消费组的 CONSUME_FROM_LAST_OFFSET 会跳过启动前的消息。
    redC.start();
    greenC.start();
    allC.start();

    std::vector<std::string> redBodiesSent, blueBodiesSent;
    for (const char* color : {"red", "blue"}) {
        for (int i = 0; i < 3; ++i) {
            const std::string text = std::string("body-") + color + "-" + std::to_string(i);
            Message m(topic, bytesOf(text));
            m.setKeys(gStamp + "-" + color + "-" + std::to_string(i));
            m.putProperty("color", color);
            const SendResult r = prod.send(m, 5000);
            check("S2 发送 " + text + " 成功", r.sendStatus == SendStatus::SEND_OK, r.msgId);
            if (std::string(color) == "red") {
                redBodiesSent.push_back(text);
            } else {
                blueBodiesSent.push_back(text);
            }
        }
    }
    std::sort(redBodiesSent.begin(), redBodiesSent.end());
    std::sort(blueBodiesSent.begin(), blueBodiesSent.end());

    waitFor([&] { return all.count() >= 6; }, 25000);
    std::this_thread::sleep_for(std::chrono::seconds(4));  // 再等一会，确认 green 不是"来得晚"
    const std::vector<std::string> gotRed = red.sorted();
    const std::vector<std::string> gotAll = all.sorted();
    check("S2 SQL92(color=red) 收到 3 条，且正好是发出去的那 3 条", gotRed == redBodiesSent,
          join(gotRed));
    std::vector<std::string> everything = redBodiesSent;
    everything.insert(everything.end(), blueBodiesSent.begin(), blueBodiesSent.end());
    std::sort(everything.begin(), everything.end());
    check("S2 TAG '*' 对照组收到 6 条（红+蓝全在）", gotAll == everything, join(gotAll));
    bool leaked = false;
    for (const std::string& b : blueBodiesSent) {
        if (std::find(gotRed.begin(), gotRed.end(), b) != gotRed.end()) leaked = true;
    }
    check("S2 blue 的 3 条没漏进 SQL92 消费者（证明 broker 真在过滤）", !leaked, join(gotRed));
    const std::set<std::string> colors = red.colorSet();
    check("S2 收到的消息属性 color 可读且都是 red",
        colors == std::set<std::string>{"red"},
        join(std::vector<std::string>(colors.begin(), colors.end())));
    check("S3 订阅永不匹配的 color='green' ⇒ 一条都没收到", green.count() == 0,
          join(green.sorted()));
    redC.shutdown();
    greenC.shutdown();
    allC.shutdown();
}

// ---------------------------------------------------------------- S4 反证
void s4BadExpressionFails(Env& env, const std::string& topic) {
    std::printf("\n---------- S4 非法表达式在启动期失败 ----------\n");
    const std::string badGroup = env.group("BAD");
    MessageSelector bad = MessageSelector::bySql("color ==");
    Sink sink;
    std::shared_ptr<CollectListener> listener(new CollectListener(sink));
    DefaultMQPushConsumer c(badGroup);
    env.configure(c, topic, &bad, std::string(), nullptr);
    c.setMessageListener(listener);

    int threw = 0;
    int32_t code = 0;
    std::string msg;
    const auto began = std::chrono::steady_clock::now();
    try {
        c.start();
    } catch (const MQClientException& e) {
        threw = 1;
        code = e.getResponseCode();
        msg = e.what();
    } catch (const std::exception& e) {
        threw = 1;
        msg = std::string("wrong exception type: ") + e.what();
    }
    const long long costMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - began)
                                 .count();
    check("S4 start() 抛出 MQClientException", threw == 1, msg);
    check("S4 错误码是 broker 的 SUBSCRIPTION_PARSE_FAILED(23)",
        code == ResponseCode::SUBSCRIPTION_PARSE_FAILED,
        "code=" + std::to_string(code) + " remark=" + msg);
    check("S4 失败后消费者没留在已启动状态", !c.isStarted());
    check("S4 非法表达式立刻失败（<5s，不是等超时兜底）", costMs < 5000,
          std::to_string(costMs) + "ms");

    // 回滚干净了？同一个对象换个合法表达式应当能重新 start
    std::string retryMsg;
    try {
        MessageSelector ok = MessageSelector::bySql("color = 'blue'");
        env.configure(c, topic, &ok, std::string(), nullptr);
        c.start();
    } catch (const std::exception& e) {
        retryMsg = e.what();
    }
    check("S4 失败后修正表达式可重新 start（启动已就地回滚）", retryMsg.empty(), retryMsg);
    if (retryMsg.empty()) c.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
    gNsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    gStamp = std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    std::printf("SQL92 / CHECK_CLIENT_CONFIG live check on %s (stamp=%s)\n", gNsAddr.c_str(),
                gStamp.c_str());

    Env env;
    const std::string topic = env.topic();
    env.start();

    DefaultMQProducer prod("GID_Sql92Cpp_" + gStamp + "_P");
    prod.setNamesrvAddr(gNsAddr);
    prod.setSendMsgTimeout(5000);
    prod.start();
    try {
        // 队列数 4：与 python/verify_sql92_live.py 的 create_topic("TBW102", topic, 4) 对齐
        prod.createTopic(MixAll::DEFAULT_TOPIC, topic, 4);
        const bool ready = waitFor([&] { return env.routeQueueCount(topic) >= 4; }, 20000);
        check("T0 topic 路由就绪（4 队列）", ready,
              "queues=" + std::to_string(env.routeQueueCount(topic)));

        s1WireEvidence(env, topic);
        s2S3RealFiltering(env, topic, prod);
        s4BadExpressionFails(env, topic);
    } catch (const std::exception& e) {
        std::printf("  [FAIL] aborted: %s\n", e.what());
        ++gFail;
    }
    prod.shutdown();
    env.cleanup();

    std::printf("\n%d PASS / %d FAIL\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
