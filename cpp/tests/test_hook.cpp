// 钩子单测：CheckForbiddenHook（发送前拦截）与 FilterMessageHook（投递前过滤）——不需要集群。
//
// 这两个钩子是 send/consume 钩子体系之外**语义最反直觉**的两个，所以离线必须锁死：
//   * CheckForbiddenHook 的异常**不被吞掉**（与 send/consume/endTransaction 钩子相反），
//     它是靠"抛异常沿发送重试链传播"来实现"禁止发送"的；
//   * FilterMessageHook 的 msgList 是**可变**的，被摘掉的消息在拉取路径是"静默跳过"
//     （位点照常推进），在 POP 路径则必须**补 ack**（否则 invisibleTime 后复活重投）；
//   * 过滤顺序：先客户端二次 tag 过滤，再跑钩子（对齐 Java processPullResult 113-128）。
//
// 同时把"订阅语义"的 Java 对拍向量固化在这里（* / 空 / 纯空白 / 全分隔符 / 单竖线），
// 探针见 /tmp/subprobe/{SubProbe,BlankProbe,EdgeProbe}.java。
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/subscription_data.h"

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

void expectInt(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
}

MessageExt makeMsg(const std::string& msgId, const std::string& body, const std::string& tags) {
    MessageExt m;
    m.topic = "TopicHook";
    m.msgId = msgId;
    m.body = body;
    if (!tags.empty()) m.properties["TAGS"] = tags;
    return m;
}

// ---- CheckForbiddenHook：记录调用次数/上下文，可选抛异常 ----
class ForbidHook : public CheckForbiddenHook {
public:
    explicit ForbidHook(bool forbid, int32_t code = 1)
        : forbid_(forbid), code_(code) {}

    std::string hookName() const override { return "unit-forbid"; }

    void checkForbidden(CheckForbiddenContext& context) override {
        ++calls;
        lastMode = context.communicationMode;
        lastTopic = context.mq.topic;
        lastGroup = context.group;
        lastUnitMode = context.unitMode;
        lastSendResultIsNull = (context.sendResult == nullptr);
        lastArgIsNull = (context.arg == nullptr);
        lastBody = context.message != nullptr ? context.message->body : std::string();
        if (forbid_) throw MQClientException("forbidden by unit hook", code_);
    }

    int32_t calls = 0;
    CommunicationMode lastMode = CommunicationMode::SYNC;
    std::string lastTopic;
    std::string lastGroup;
    bool lastUnitMode = true;
    bool lastSendResultIsNull = false;
    bool lastArgIsNull = true;
    std::string lastBody;

private:
    bool forbid_;
    int32_t code_;
};

// ---- FilterMessageHook：按 body 前缀丢消息 / 抛异常 ----
class DropHook : public FilterMessageHook {
public:
    explicit DropHook(std::string prefix) : prefix_(std::move(prefix)) {}

    std::string hookName() const override { return "unit-drop"; }

    void filterMessage(FilterMessageContext& context) override {
        ++calls;
        seenCounts.push_back(static_cast<int32_t>(context.msgList.size()));
        seenGroup = context.consumerGroup;
        seenUnitMode = context.unitMode;
        std::vector<MessageExt> kept;
        for (const MessageExt& m : context.msgList) {
            if (m.body.rfind(prefix_, 0) != 0) kept.push_back(m);
        }
        context.msgList = kept;
    }

    int32_t calls = 0;
    std::vector<int32_t> seenCounts;
    std::string seenGroup;
    bool seenUnitMode = true;

private:
    std::string prefix_;
};

class BoomHook : public FilterMessageHook {
public:
    std::string hookName() const override { return "unit-boom"; }
    void filterMessage(FilterMessageContext&) override {
        ++calls;
        throw std::runtime_error("boom from unit hook");
    }
    int32_t calls = 0;
};

// ---------------------------------------------------------------- 订阅语义（Java 对拍向量）
void testSubscriptionJavaParity() {
    // "*" / "" → SUB_ALL 且两集合都空（Java setSubString("*") 后直接 return）
    SubscriptionData all1 = FilterAPI::buildSubscriptionData("T", "*");
    expect(all1.subString == "*" && all1.tagsSet.empty() && all1.codeSet.empty(),
           "sub '*': SUB_ALL + empty sets");
    SubscriptionData all2 = FilterAPI::buildSubscriptionData("T", "");
    expect(all2.subString == "*" && all2.tagsSet.empty() && all2.codeSet.empty(),
           "sub '': SUB_ALL + empty sets");
    // 纯空白：isEmpty 只认 null/"" → 走 split → 标签 trim 成空 → 集合空、subString 原样
    SubscriptionData blank = FilterAPI::buildSubscriptionData("T", "   ");
    expect(blank.subString == "   " && blank.tagsSet.empty() && blank.codeSet.empty(),
           "sub blank: split branch, subString preserved, empty sets");

    // "TagA" → tagsSet={TagA}, codeSet={2598919}
    SubscriptionData one = FilterAPI::buildSubscriptionData("T", "TagA");
    expect(one.tagsSet.size() == 1 && one.tagsSet.count("TagA") == 1, "sub 'TagA': tagsSet");
    expect(one.codeSet.size() == 1 && one.codeSet.count(2598919) == 1, "sub 'TagA': codeSet");

    // "TagA||TagB" → {2598919, 2598920}
    SubscriptionData two = FilterAPI::buildSubscriptionData("T", "TagA||TagB");
    expect(two.tagsSet.size() == 2 && two.codeSet.size() == 2, "sub 'TagA||TagB': sizes");
    expect(two.codeSet.count(2598919) == 1 && two.codeSet.count(2598920) == 1,
           "sub 'TagA||TagB': codeSet values");

    // " TagA || TagB " → subString 原样保留空格，标签各自 trim
    SubscriptionData spaced = FilterAPI::buildSubscriptionData("T", " TagA || TagB ");
    expect(spaced.subString == " TagA || TagB ", "sub spaced: subString preserved verbatim");
    expect(spaced.tagsSet.count("TagA") == 1 && spaced.tagsSet.count("TagB") == 1,
           "sub spaced: tags trimmed");

    // "|||" → Java split("\\|\\|") == ["", "|"] → 丢末尾空串后剩 ["|"] → 字面量标签 "|"（hash 124）
    SubscriptionData pipes = FilterAPI::buildSubscriptionData("T", "|||");
    expect(pipes.tagsSet.size() == 1 && pipes.tagsSet.count("|") == 1,
           "sub '|||': literal '|' tag (only TRAILING empties dropped)");
    expect(pipes.codeSet.count(124) == 1, "sub '|||': codeSet = 124");

    // "*||TagA" → {*, TagA}（表达式里的 "*" 是字面量 tag，不是 SUB_ALL）
    SubscriptionData star = FilterAPI::buildSubscriptionData("T", "*||TagA");
    expect(star.tagsSet.size() == 2 && star.codeSet.count(42) == 1,
           "sub '*||TagA': '*' is a literal tag with hash 42");

    // "||" / "||||" → Java-split 结果数组长度为 0 → 抛 "subString split error"
    for (const char* bad : {"||", "||||"}) {
        bool threw = false;
        try {
            FilterAPI::buildSubscriptionData("T", bad);
        } catch (const std::exception& e) {
            threw = std::string(e.what()) == "subString split error";
        }
        expect(threw, std::string("sub '") + bad + "': throws subString split error");
    }
}

// ---------------------------------------------------------------- CheckForbiddenHook
void testCheckForbiddenHook() {
    DefaultMQProducer producer("GID_HookUnitProducer");

    expect(!producer.hasCheckForbiddenHook() && producer.checkForbiddenHookCount() == 0,
           "forbidden: empty by default");
    expect(!producer.hasSendInterceptors(), "forbidden: no interceptors by default");

    auto hook = std::make_shared<ForbidHook>(/*forbid=*/false);
    producer.registerCheckForbiddenHook(hook);
    expect(producer.hasCheckForbiddenHook() && producer.checkForbiddenHookCount() == 1,
           "forbidden: register makes has/count true");
    expect(producer.hasSendInterceptors(), "forbidden: registering enables the hook path");
    // 注册 nullptr 不得进入列表（Java registerCheckForbiddenHook 同样判空）
    producer.registerCheckForbiddenHook(nullptr);
    expectInt(producer.checkForbiddenHookCount(), 1, "forbidden: nullptr not registered");

    Message msg("TopicHook", std::string("hello"));
    MessageQueue mq("TopicHook", "broker-a", 3);
    std::string selectorArg("order-key");
    CheckForbiddenContext ctx;
    ctx.nameSrvAddr = "127.0.0.1:9876";
    ctx.group = "GID_HookUnitProducer";
    ctx.message = &msg;
    ctx.mq = mq;
    ctx.brokerAddr = "127.0.0.1:10911";
    ctx.communicationMode = CommunicationMode::ASYNC;
    ctx.arg = &selectorArg;
    ctx.unitMode = false;

    producer.executeCheckForbiddenHook(ctx);
    expectInt(hook->calls, 1, "forbidden: hook invoked once per execute");
    expect(hook->lastTopic == "TopicHook", "forbidden: context carries mq.topic");
    expect(hook->lastMode == CommunicationMode::ASYNC, "forbidden: context carries mode");
    expect(hook->lastUnitMode == false, "forbidden: unitMode is false (no unit mode support)");
    expect(hook->lastSendResultIsNull, "forbidden: sendResult is null before send");
    expect(!hook->lastArgIsNull, "forbidden: selector arg is passed through");
    expect(hook->lastBody == "hello", "forbidden: context carries the message");

    // 每个钩子都被执行（Java 是 for 循环，不像 send hook 那样需要成对）
    auto hook2 = std::make_shared<ForbidHook>(false);
    producer.registerCheckForbiddenHook(hook2);
    producer.executeCheckForbiddenHook(ctx);
    expectInt(hook->calls, 2, "forbidden: first hook ran again");
    expectInt(hook2->calls, 1, "forbidden: second hook also runs");

    // ★ 关键语义：拦截钩子的异常**不被吞掉**，原样传出去
    DefaultMQProducer blocker("GID_HookUnitBlocker");
    auto strict = std::make_shared<ForbidHook>(/*forbid=*/true, /*code=*/2);
    blocker.registerCheckForbiddenHook(strict);
    auto after = std::make_shared<ForbidHook>(/*forbid=*/false);
    blocker.registerCheckForbiddenHook(after);
    bool threw = false;
    std::string what;
    int32_t code = -1;
    try {
        blocker.executeCheckForbiddenHook(ctx);
    } catch (const MQClientException& e) {
        threw = true;
        what = e.what();
        code = e.getResponseCode();
    }
    expect(threw, "forbidden: exception NOT swallowed (unlike send/consume hooks)");
    expect(what == "forbidden by unit hook", "forbidden: exception message preserved");
    expectInt(code, 2, "forbidden: exception responseCode preserved");
    expectInt(strict->calls, 1, "forbidden: throwing hook counted");
    // 异常抛出后，**后面的钩子不再执行** —— 这正是"拦截"的语义（发送已被中止）
    expectInt(after->calls, 0, "forbidden: hooks after the throwing one are skipped");

    // 没有钩子时 execute 是空操作
    DefaultMQProducer plain("GID_HookUnitPlain");
    plain.executeCheckForbiddenHook(ctx);
    expect(true, "forbidden: execute with no hooks is a no-op");
}

// ---------------------------------------------------------------- FilterMessageHook
void testFilterMessageHook() {
    DefaultMQPushConsumer consumer("GID_HookUnitConsumer");
    expect(!consumer.hasFilterMessageHook() && consumer.filterMessageHookCount() == 0,
           "filter: empty by default");
    expectInt(consumer.filteredMessageCount(), 0, "filter: counter starts at 0");

    auto drop = std::make_shared<DropHook>("drop-");
    consumer.registerFilterMessageHook(drop);
    expect(consumer.hasFilterMessageHook() && consumer.filterMessageHookCount() == 1,
           "filter: register makes has/count true");

    MessageQueue mq("TopicHook", "broker-a", 0);

    // ① 无订阅（sub == nullptr）时只跑钩子
    std::vector<MessageExt> msgs{makeMsg("M1", "keep-1", "TagA"),
                                 makeMsg("M2", "drop-2", "TagA"),
                                 makeMsg("M3", "keep-3", "TagA")};
    std::vector<MessageExt> kept = consumer.filterMessagesForDelivery(mq, nullptr, msgs);
    expectInt(static_cast<long long>(kept.size()), 2, "filter: hook dropped 1 of 3");
    expect(kept[0].msgId == "M1" && kept[1].msgId == "M3", "filter: kept the right ones");
    expect(drop->seenCounts.size() == 1 && drop->seenCounts[0] == 3,
           "filter: hook saw the full batch");
    expect(drop->seenGroup == "GID_HookUnitConsumer", "filter: context carries consumerGroup");
    expect(drop->seenUnitMode == false, "filter: unitMode is false");

    // ② 订阅 TagA：tag 过滤在前、钩子在后（先按 tag 摘掉 TagB，再给钩子看剩下的）
    SubscriptionData sub = FilterAPI::buildSubscriptionData("TopicHook", "TagA");
    drop->seenCounts.clear();
    std::vector<MessageExt> mixed{makeMsg("M1", "keep-1", "TagA"),
                                 makeMsg("M2", "keep-2", "TagB"),
                                 makeMsg("M3", "drop-3", "TagA")};
    std::vector<MessageExt> kept2 = consumer.filterMessagesForDelivery(mq, &sub, mixed);
    expectInt(static_cast<long long>(kept2.size()), 1, "filter: tag+hook composed -> 1 left");
    expect(kept2[0].msgId == "M1", "filter: TagB removed before hook saw the batch");
    expect(drop->seenCounts.size() == 1 && drop->seenCounts[0] == 2,
           "filter: hook saw only the tag-matching subset (tag filter runs FIRST)");

    // ③ 订阅 "*"（SUB_ALL）：tagsSet 为空 ⇒ **不做**客户端 tag 过滤
    SubscriptionData subAll = FilterAPI::buildSubscriptionData("TopicHook", "*");
    auto noop = std::make_shared<DropHook>("nothing-matches-this-");
    DefaultMQPushConsumer plain("GID_HookUnitConsumer2");
    plain.registerFilterMessageHook(noop);
    std::vector<MessageExt> kept3 = plain.filterMessagesForDelivery(mq, &subAll, mixed);
    expectInt(static_cast<long long>(kept3.size()), 3,
              "filter: SUB_ALL must NOT client-filter tags (tagsSet empty is the switch)");

    // ④ 钩子异常被吞掉，且**后续钩子照常执行**
    auto boom = std::make_shared<BoomHook>();
    auto drop2 = std::make_shared<DropHook>("drop-");
    DefaultMQPushConsumer robust("GID_HookUnitConsumer3");
    robust.registerFilterMessageHook(boom);
    robust.registerFilterMessageHook(drop2);
    std::vector<MessageExt> kept4 = robust.filterMessagesForDelivery(mq, nullptr, msgs);
    expectInt(boom->calls, 1, "filter: boom hook was called");
    expectInt(drop2->calls, 1, "filter: hook AFTER the throwing one still runs (swallowed)");
    expectInt(static_cast<long long>(kept4.size()), 2, "filter: boom did not break filtering");

    // ⑤ 空输入：不调用钩子（避免无意义回调）
    drop2->calls = 0;
    std::vector<MessageExt> empty;
    std::vector<MessageExt> kept5 = robust.filterMessagesForDelivery(mq, nullptr, empty);
    expect(kept5.empty() && drop2->calls == 0, "filter: empty input short-circuits");

    // ⑥ 差集：POP 路径要给被摘掉的消息补 ack，因此差集必须精确
    std::vector<MessageExt> orig{makeMsg("A", "keep", ""), makeMsg("B", "drop", ""),
                                 makeMsg("C", "keep2", "")};
    std::vector<MessageExt> keptSet{orig[0], orig[2]};
    std::vector<MessageExt> dropped = DefaultMQPushConsumer::droppedMessages(orig, keptSet);
    expectInt(static_cast<long long>(dropped.size()), 1, "dropped: size");
    expect(dropped[0].msgId == "B", "dropped: the missing one (by msgId)");
    expect(DefaultMQPushConsumer::droppedMessages(orig, orig).empty(),
           "dropped: nothing missing -> empty");
    std::vector<MessageExt> over{makeMsg("X", "", ""), makeMsg("Y", "", ""), makeMsg("Z", "", ""),
                                 makeMsg("W", "", "")};
    expect(DefaultMQPushConsumer::droppedMessages(orig, over).empty(),
           "dropped: kept >= original -> empty (guards negative reserve)");
}

// ---------------------------------------------------------------- 上下文默认值
void testContextDefaults() {
    CheckForbiddenContext ck;
    expect(ck.sendResult == nullptr, "ctx: CheckForbiddenContext has no sendResult");
    expect(ck.unitMode == false, "ctx: CheckForbiddenContext unitMode defaults false");
    expect(ck.arg == nullptr, "ctx: CheckForbiddenContext arg defaults null");
    expect(ck.communicationMode == CommunicationMode::SYNC, "ctx: CheckForbiddenContext mode SYNC");
    expect(ck.exception.empty(), "ctx: CheckForbiddenContext exception empty");

    FilterMessageContext fm;
    expect(fm.msgList.empty(), "ctx: FilterMessageContext msgList empty");
    expect(fm.unitMode == false, "ctx: FilterMessageContext unitMode defaults false");
    expect(fm.arg == nullptr, "ctx: FilterMessageContext arg defaults null");

    // 三种发送模式（对齐 Java CommunicationMode 枚举）
    expectInt(static_cast<int>(CommunicationMode::SYNC), 0, "ctx: SYNC ordinal");
    expectInt(static_cast<int>(CommunicationMode::ASYNC), 1, "ctx: ASYNC ordinal");
    expectInt(static_cast<int>(CommunicationMode::ONEWAY), 2, "ctx: ONEWAY ordinal");
}

}  // namespace

int main() {
    testSubscriptionJavaParity();
    testCheckForbiddenHook();
    testFilterMessageHook();
    testContextDefaults();
    std::printf("hook: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
