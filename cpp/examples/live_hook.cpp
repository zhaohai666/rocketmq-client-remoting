// CheckForbiddenHook / FilterMessageHook 真机验证（对齐 python/verify_hook_live.py 的 S1–S11）。
// 用法：rmq_live_hook 127.0.0.1:9876
//
// 前置：NameServer + Broker 已起（普通配置即可，不需要 traceTopicEnable）。
//
// 场景：
//   S0  订阅语义自检（本地纯逻辑）：SUB_ALL 两集合为空、显式 tag 填 codeSet
//   S1  预热建 topic + 消费者已分配队列
//   S2  放行钩子：发送成功、钩子被调用 1 次、上下文带 group/mq/unitMode=false/sendResult=null
//   S3  拦截钩子：send 抛 MQClientException，且按 retryTimes+1 次调用
//   S4  被拦截的消息没有落到 broker（只有放行的那 1 条）
//   S5  单向发送同样被拦截，且上下文 mode=ONEWAY
//   S6  过滤钩子在**拉取路径**生效：3 收 2 丢
//   S7  被摘掉的消息不会重投（位点已推进）
//   S8  客户端二次 tag 过滤：订阅 TagA 只收 TagA
//   S9  钩子抛异常被吞掉、后续钩子照常生效
//   S10 POP 路径过滤钩子生效（摘掉即 ack）
//   S11 POP 路径被摘掉的消息已 ack（观测窗 > invisibleTime，未复活重投）
//
// ⚠ 脚本自身的两条纪律（上一轮 trace 联调踩出来的）：
//   ① topic / 消费组一律带 stamp —— `run_hook_live.sh all` 会让三语言**依次**跑在同一个
//      broker 上，固定名会继承上一轮的提交位点；
//   ② 断言**按 body 前缀过滤** —— 预热消息、broker consumequeue 异步分发都可能让消费者多收几条。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

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

bool startsWith(const std::string& s, const std::string& p) { return s.rfind(p, 0) == 0; }

int64_t stamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    const int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return pred();
}

// 收集消费到的消息（并发安全），并按 body 前缀筛
class Collector : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(m_);
        for (const MessageExt& m : msgs) msgs_.push_back(m);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    std::vector<MessageExt> snapshot() {
        std::lock_guard<std::mutex> lk(m_);
        return msgs_;
    }

    // 按 body 前缀筛（预热消息 / 上一轮的残留都靠它排除）
    std::vector<MessageExt> byPrefix(const std::vector<std::string>& prefixes) {
        std::lock_guard<std::mutex> lk(m_);
        std::vector<MessageExt> out;
        for (const MessageExt& m : msgs_) {
            const std::string b = bodyOf(m);
            for (const std::string& p : prefixes) {
                if (startsWith(b, p)) { out.push_back(m); break; }
            }
        }
        return out;
    }

    size_t count() {
        std::lock_guard<std::mutex> lk(m_);
        return msgs_.size();
    }

private:
    std::mutex m_;
    std::vector<MessageExt> msgs_;
};

class ForbidHook : public CheckForbiddenHook {
public:
    ForbidHook(bool forbid, const std::string& onlyTopic)
        : forbid_(forbid), onlyTopic_(onlyTopic) {}

    std::string hookName() const override { return "live-forbid"; }

    void checkForbidden(CheckForbiddenContext& ctx) override {
        std::lock_guard<std::mutex> lk(m_);
        ++calls;
        lastGroup = ctx.group;
        lastTopic = ctx.mq.topic;
        lastUnitMode = ctx.unitMode;
        lastSendResultNull = (ctx.sendResult == nullptr);
        modes.push_back(ctx.communicationMode);
        if (forbid_ && (onlyTopic_.empty() || startsWith(ctx.mq.topic, onlyTopic_))) {
            throw MQClientException("forbidden by live hook");
        }
    }

    int32_t calls = 0;
    std::string lastGroup;
    std::string lastTopic;
    bool lastUnitMode = true;
    bool lastSendResultNull = false;
    std::vector<CommunicationMode> modes;

private:
    std::mutex m_;
    bool forbid_;
    std::string onlyTopic_;
};

// 摘掉 body 以 prefix 开头的消息（msgList 可变，见 Java FilterMessageContext）
class DropHook : public FilterMessageHook {
public:
    explicit DropHook(const std::string& prefix) : prefix_(prefix) {}

    std::string hookName() const override { return "live-drop"; }

    void filterMessage(FilterMessageContext& ctx) override {
        ++calls;
        seen.push_back(static_cast<int32_t>(ctx.msgList.size()));
        std::vector<MessageExt> kept;
        for (const MessageExt& m : ctx.msgList) {
            if (!startsWith(bodyOf(m), prefix_)) kept.push_back(m);
        }
        ctx.msgList = kept;
    }

    int32_t calls = 0;
    std::vector<int32_t> seen;

private:
    std::string prefix_;
};

class BoomHook : public FilterMessageHook {
public:
    std::string hookName() const override { return "live-boom"; }
    void filterMessage(FilterMessageContext&) override {
        ++calls;
        throw std::runtime_error("boom from live hook");
    }
    int32_t calls = 0;
};

// 预热：触发 broker 自动建 topic（必须先建 topic 再起消费者，否则拿不到路由）
bool warm(const std::string& nsAddr, const std::string& topic, const std::string& groupSuffix) {
    DefaultMQProducer p("GID_hook_live_warm_" + groupSuffix);
    p.setNamesrvAddr(nsAddr);
    try {
        p.start();
        p.send(Message(topic, std::string("warm-up")), 5000);
        p.shutdown();
        return true;
    } catch (const std::exception& e) {
        std::printf("  [warn] warm-up failed for %s: %s\n", topic.c_str(), e.what());
        p.shutdown();
        return false;
    }
}

struct ConsumerOpts {
    bool pop = false;
    int64_t invisibleMs = 0;
    std::vector<std::shared_ptr<FilterMessageHook>> hooks;
    std::string tagExpression = "*";
};

std::unique_ptr<DefaultMQPushConsumer> startConsumer(
    const std::string& nsAddr, const std::string& topic, const std::string& group,
    std::shared_ptr<Collector> collector, const ConsumerOpts& opts) {
    auto c = std::make_unique<DefaultMQPushConsumer>(group);
    c->setNamesrvAddr(nsAddr);
    c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    c->subscribe(topic, opts.tagExpression);
    c->setMessageListener(collector);
    for (const auto& h : opts.hooks) c->registerFilterMessageHook(h);
    if (opts.pop) {
        c->setPopMode(true);
        if (opts.invisibleMs > 0) c->setPopInvisibleTime(opts.invisibleMs);
        c->setPopBatchNums(8);
    }
    c->start();
    return c;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_hook <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const int64_t st = stamp();
    const std::string sfx = std::to_string(st);
    const std::string topicCk = "HookCheckForbidden_" + sfx;
    const std::string topicFilter = "HookFilterPull_" + sfx;
    const std::string topicTag = "HookFilterTag_" + sfx;
    const std::string topicBoom = "HookFilterBoom_" + sfx;
    const std::string topicPop = "HookFilterPop_" + sfx;
    const std::string group = "GID_hook_live_" + sfx;
    const std::string pg = "GID_hook_producer_" + sfx;

    std::printf("=== RocketMQ hook live verify (cpp) ===\n");
    std::printf("namesrv=%s stamp=%lld\n", nsAddr.c_str(), static_cast<long long>(st));

    // ---------- S0 订阅语义自检 ----------
    {
        SubscriptionData subAll = FilterAPI::buildSubscriptionData("T", "*");
        SubscriptionData subTag = FilterAPI::buildSubscriptionData("T", "TagA||TagB");
        check("S0 SUB_ALL 的 tagsSet/codeSet 为空、显式 tag 填 codeSet（Java 语义）",
              subAll.tagsSet.empty() && subAll.codeSet.empty() && subTag.tagsSet.size() == 2
                  && subTag.codeSet.count(2598919) == 1 && subTag.codeSet.count(2598920) == 1,
              "sub_all_tags=" + std::to_string(subAll.tagsSet.size()) + " sub_tag_codes="
                  + std::to_string(subTag.codeSet.size()));
    }

    // =============== 第一部分：CheckForbiddenHook ===============
    if (!warm(nsAddr, topicCk, "ck")) {
        check("S1 预热建 topic（CheckForbidden 用）", false);
        return 1;
    }
    auto ckCollector = std::make_shared<Collector>();
    ConsumerOpts ckOpts;
    auto ckConsumer = startConsumer(nsAddr, topicCk, group + "_ck", ckCollector, ckOpts);
    bool assigned = waitUntil([&]() { return !ckConsumer->assignedQueueKeys().empty(); }, 30000);
    check("S1 预热建 topic + 消费者已分配队列", assigned,
          "assigned=" + std::to_string(ckConsumer->assignedQueueKeys().size()));
    if (!assigned) {
        ckConsumer->shutdown();
        return 1;
    }

    // S2 放行
    {
        auto allow = std::make_shared<ForbidHook>(/*forbid=*/false, "");
        DefaultMQProducer p(pg + "_allow");
        p.setNamesrvAddr(nsAddr);
        p.setRetryTimesWhenSendFailed(2);
        p.registerCheckForbiddenHook(allow);
        p.start();
        bool ok = false;
        try {
            p.send(Message(topicCk, std::string("allowed-1")), 5000);
            ok = true;
        } catch (const std::exception& e) {
            std::printf("  [warn] allowed send failed: %s\n", e.what());
        }
        check("S2 放行钩子：发送成功且钩子被调用 1 次", ok && allow->calls == 1,
              "ok=" + std::string(ok ? "true" : "false") + " calls="
                  + std::to_string(allow->calls) + " mode="
                  + std::to_string(static_cast<int>(allow->modes.empty()
                                                        ? CommunicationMode::SYNC
                                                        : allow->modes[0])));
        check("S2b 拦截上下文带上 group / mq / unitMode=false / sendResult=null",
              allow->calls == 1 && allow->lastGroup == pg + "_allow" && allow->lastTopic == topicCk
                  && allow->lastUnitMode == false && allow->lastSendResultNull,
              "group=" + allow->lastGroup + " topic=" + allow->lastTopic);
        p.shutdown();
    }

    // S3 拦截（每次尝试都跑钩子）
    {
        auto forbid = std::make_shared<ForbidHook>(/*forbid=*/true, topicCk);
        DefaultMQProducer p(pg + "_forbid");
        p.setNamesrvAddr(nsAddr);
        p.setRetryTimesWhenSendFailed(2);
        p.registerCheckForbiddenHook(forbid);
        p.start();
        bool threw = false;
        try {
            p.send(Message(topicCk, std::string("blocked-1")), 5000);
        } catch (const MQClientException&) {
            threw = true;
        } catch (const std::exception&) {
            threw = true;
        }
        check("S3 拦截钩子：send 抛 MQClientException，且钩子按 retryTimes+1=3 次调用",
              threw && forbid->calls == 3,
              "threw=" + std::string(threw ? "true" : "false") + " calls="
                  + std::to_string(forbid->calls));
        p.shutdown();
    }

    // S5 单向发送同样被拦截
    {
        auto forbidOw = std::make_shared<ForbidHook>(/*forbid=*/true, topicCk);
        DefaultMQProducer p(pg + "_oneway");
        p.setNamesrvAddr(nsAddr);
        p.registerCheckForbiddenHook(forbidOw);
        p.start();
        bool threw = false;
        try {
            p.sendOneway(Message(topicCk, std::string("blocked-oneway")));
        } catch (const std::exception&) {
            threw = true;
        }
        check("S5 单向发送也被拦截，且上下文 mode=ONEWAY",
              threw && forbidOw->modes.size() == 1
                  && forbidOw->modes[0] == CommunicationMode::ONEWAY,
              "threw=" + std::string(threw ? "true" : "false") + " modes="
                  + std::to_string(forbidOw->modes.size()));
        p.shutdown();
    }

    // S4 被拦截的消息没有落到 broker：只有 allowed-1 这一条
    {
        bool got = waitUntil(
            [&]() { return !ckCollector->byPrefix({"allowed-"}).empty(); }, 20000);
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));  // 留出"万一真发出去了"的到达窗口
        std::vector<MessageExt> landed = ckCollector->byPrefix({"allowed-", "blocked-"});
        check("S4 被拦截的消息没有落到 broker（只有放行的那 1 条）",
              got && landed.size() == 1 && bodyOf(landed[0]) == "allowed-1",
              "count=" + std::to_string(landed.size()));
    }
    ckConsumer->shutdown();

    // =============== 第二部分：FilterMessageHook（拉取路径）===============
    if (!warm(nsAddr, topicFilter, "flt")) {
        check("S6 预热建 topic（过滤钩子用）", false);
        return 1;
    }
    {
        auto drop = std::make_shared<DropHook>("drop-");
        auto fltCollector = std::make_shared<Collector>();
        ConsumerOpts opts;
        opts.hooks.push_back(drop);
        auto fltConsumer = startConsumer(nsAddr, topicFilter, group + "_filter", fltCollector, opts);
        if (!waitUntil([&]() { return !fltConsumer->assignedQueueKeys().empty(); }, 30000)) {
            check("S6 消费者已分配到队列", false);
            fltConsumer->shutdown();
            return 1;
        }

        DefaultMQProducer p(pg + "_f");
        p.setNamesrvAddr(nsAddr);
        p.start();
        for (int i = 0; i < 3; ++i) {
            p.send(Message(topicFilter, std::string("keep-") + std::to_string(i)), 5000);
            p.send(Message(topicFilter, std::string("drop-") + std::to_string(i)), 5000);
        }

        bool got = waitUntil([&]() { return fltCollector->byPrefix({"keep-"}).size() >= 3; }, 25000);
        size_t kept = fltCollector->byPrefix({"keep-"}).size();
        size_t dropped = fltCollector->byPrefix({"drop-"}).size();
        check("S6 过滤钩子在拉取路径生效：3 收 2 丢",
              got && kept == 3 && dropped == 0,
              "keep=" + std::to_string(kept) + " drop=" + std::to_string(dropped)
                  + " hook_calls=" + std::to_string(drop->calls));

        // S7 被摘掉的消息不重投（拉取路径静默跳过、位点照常推进）
        std::this_thread::sleep_for(std::chrono::milliseconds(8000));
        check("S7 被摘掉的消息不会重投（位点已推进，等 8s 计数不变）",
              fltCollector->byPrefix({"drop-"}).empty() && fltCollector->byPrefix({"keep-"}).size() == 3,
              "keep=" + std::to_string(fltCollector->byPrefix({"keep-"}).size()) + " drop="
                  + std::to_string(fltCollector->byPrefix({"drop-"}).size()));
        p.shutdown();
        fltConsumer->shutdown();
    }

    // =============== 第三部分：客户端二次 tag 过滤 ===============
    if (!warm(nsAddr, topicTag, "tag")) {
        check("S8 预热建 topic（tag 过滤用）", false);
        return 1;
    }
    {
        auto tagCollector = std::make_shared<Collector>();
        ConsumerOpts opts;
        opts.tagExpression = "TagA";  // tagsSet={TagA} → 客户端会二次过滤
        auto tagConsumer = startConsumer(nsAddr, topicTag, group + "_tag", tagCollector, opts);
        if (!waitUntil([&]() { return !tagConsumer->assignedQueueKeys().empty(); }, 30000)) {
            check("S8 消费者已分配到队列（tag）", false);
            tagConsumer->shutdown();
            return 1;
        }

        DefaultMQProducer p(pg + "_tag");
        p.setNamesrvAddr(nsAddr);
        p.start();
        for (int i = 0; i < 2; ++i) {
            Message a(topicTag, std::string("tagA-") + std::to_string(i));
            a.setTags("TagA");
            p.send(a, 5000);
            Message b(topicTag, std::string("tagB-") + std::to_string(i));
            b.setTags("TagB");
            p.send(b, 5000);
        }

        bool got = waitUntil([&]() { return tagCollector->byPrefix({"tagA-"}).size() >= 2; }, 25000);
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        size_t ta = tagCollector->byPrefix({"tagA-"}).size();
        size_t tb = tagCollector->byPrefix({"tagB-"}).size();
        check("S8 订阅 TagA：只收到 TagA 的 2 条（broker 哈希过滤 + 客户端二次过滤）",
              got && ta == 2 && tb == 0,
              "tagA=" + std::to_string(ta) + " tagB=" + std::to_string(tb));

        p.shutdown();
        tagConsumer->shutdown();
    }

    // =============== 第四部分：钩子异常不影响消费 ===============
    if (!warm(nsAddr, topicBoom, "boom")) {
        check("S9 预热建 topic（异常钩子用）", false);
        return 1;
    }
    {
        auto boom = std::make_shared<BoomHook>();
        auto drop2 = std::make_shared<DropHook>("drop-");
        auto boomCollector = std::make_shared<Collector>();
        ConsumerOpts opts;
        opts.hooks.push_back(boom);
        opts.hooks.push_back(drop2);
        auto boomConsumer = startConsumer(nsAddr, topicBoom, group + "_boom", boomCollector, opts);
        if (!waitUntil([&]() { return !boomConsumer->assignedQueueKeys().empty(); }, 30000)) {
            check("S9 消费者已分配到队列（boom）", false);
            boomConsumer->shutdown();
            return 1;
        }

        DefaultMQProducer p(pg + "_boom");
        p.setNamesrvAddr(nsAddr);
        p.start();
        p.send(Message(topicBoom, std::string("keep-boom")), 5000);
        p.send(Message(topicBoom, std::string("drop-boom")), 5000);

        bool got = waitUntil([&]() { return !boomCollector->byPrefix({"keep-"}).empty(); }, 25000);
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        check("S9 前一个钩子抛异常被吞掉、后续钩子照常生效（异常不影响消费）",
              got && boom->calls >= 1 && drop2->calls >= 1
                  && boomCollector->byPrefix({"keep-"}).size() == 1
                  && boomCollector->byPrefix({"drop-"}).empty(),
              "boom_calls=" + std::to_string(boom->calls) + " drop_calls="
                  + std::to_string(drop2->calls) + " keep="
                  + std::to_string(boomCollector->byPrefix({"keep-"}).size()) + " drop="
                  + std::to_string(boomCollector->byPrefix({"drop-"}).size()));
        p.shutdown();
        boomConsumer->shutdown();
    }

    // =============== 第五部分：FilterMessageHook（POP 路径，摘掉即 ack）===============
    if (!warm(nsAddr, topicPop, "pop")) {
        check("S10 预热建 topic（POP 用）", false);
        return 1;
    }
    {
        auto popDrop = std::make_shared<DropHook>("drop-");
        auto popCollector = std::make_shared<Collector>();
        const int64_t invisibleMs = 10000;
        ConsumerOpts opts;
        opts.pop = true;
        opts.invisibleMs = invisibleMs;
        opts.hooks.push_back(popDrop);
        auto popConsumer = startConsumer(nsAddr, topicPop, group + "_pop", popCollector, opts);
        if (!waitUntil([&]() { return !popConsumer->assignedQueueKeys().empty(); }, 30000)) {
            check("S10 POP 消费者已分配到队列", false);
            popConsumer->shutdown();
            return 1;
        }

        DefaultMQProducer p(pg + "_pop");
        p.setNamesrvAddr(nsAddr);
        p.start();
        for (int i = 0; i < 2; ++i) {
            p.send(Message(topicPop, std::string("keep-pop-") + std::to_string(i)), 5000);
        }
        p.send(Message(topicPop, std::string("drop-pop-0")), 5000);

        bool got = waitUntil([&]() { return popCollector->byPrefix({"keep-"}).size() >= 2; }, 30000);
        check("S10 POP 路径过滤钩子生效：2 收 1 丢",
              got && popCollector->byPrefix({"keep-"}).size() == 2
                  && popCollector->byPrefix({"drop-"}).empty(),
              "keep=" + std::to_string(popCollector->byPrefix({"keep-"}).size()) + " drop="
                  + std::to_string(popCollector->byPrefix({"drop-"}).size()) + " hook_calls="
                  + std::to_string(popDrop->calls));

        // S11 观测窗口必须 > invisibleTime，否则"ack 没发出去"会伪装成通过
        std::printf("  ... 等待 %llds（> invisibleTime=%llds）确认被摘掉的消息不复活\n",
                    static_cast<long long>(invisibleMs / 1000 + 6),
                    static_cast<long long>(invisibleMs / 1000));
        std::this_thread::sleep_for(std::chrono::milliseconds(invisibleMs + 6000));
        check("S11 POP 路径被摘掉的消息已 ack（观测窗 > invisibleTime，未复活重投）",
              popCollector->byPrefix({"keep-"}).size() == 2
                  && popCollector->byPrefix({"drop-"}).empty(),
              "keep=" + std::to_string(popCollector->byPrefix({"keep-"}).size()) + " drop="
                  + std::to_string(popCollector->byPrefix({"drop-"}).size()));
        p.shutdown();
        popConsumer->shutdown();
    }

    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
