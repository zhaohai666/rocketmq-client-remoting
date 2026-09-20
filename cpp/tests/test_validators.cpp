// Validators / TopicValidator 单测（任务 #32 "Validators fast-fail"）。
//
// 锁定三件事：
//   1. **文案与判定顺序**以 python/rocketmq/client/validators.py 为准（逐字对拍）；
//   2. body/大小/INNER_MULTI_DISPATCH 三档带 MESSAGE_ILLEGAL(13)，topic/group 三档
//      用默认码（Python 的 UNKNOWN=1）；
//   3. 生产者/消费者 start() 的组名守卫在**建客户端实例之前**跑完——非法/保留组
//      失败时不碰网络（namesrv 指向 127.0.0.1:1 也不会发起连接）。
#include <cstdint>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
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
#include "rocketmq/common/topic_validator.h"
#include "rocketmq/remoting/protocol/codes.h"

using namespace rocketmq;

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << "\n";                           \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b, msg)                                                    \
    do {                                                                       \
        if ((a) == (b)) {                                                      \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << " (actual=" << (a)             \
                      << ", expect=" << (b) << ")\n";                          \
        }                                                                      \
    } while (0)

namespace {

struct Captured {
    bool threw = false;
    std::string what;
    int32_t responseCode = 0;  // 没抛时保持 0，抛了才有意义
};

// 只捕 MQClientException：其他异常类型会逃出去让进程崩——那正是要暴露的 bug
Captured capture(const std::function<void()>& fn) {
    Captured c;
    try {
        fn();
    } catch (const MQClientException& e) {
        c.threw = true;
        c.what = e.what();
        c.responseCode = e.getResponseCode();
    }
    return c;
}

bool throwsMessage(const std::function<void()>& fn, const std::string& expectWhat,
                   int32_t expectCode) {
    Captured c = capture(fn);
    if (!c.threw || c.what != expectWhat || c.responseCode != expectCode) {
        std::cout << "  threw=" << c.threw << " what=[" << c.what << "] code="
                  << c.responseCode << " expect=[" << expectWhat << "] code=" << expectCode
                  << "\n";
        return false;
    }
    return true;
}

std::string repeatedChar(char ch, size_t n) { return std::string(n, ch); }

// 与 Python 逐字一致的期望文案（改这里 = 改golden，先对齐 validators.py）
const char* kIllegalTail = " contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$";

// ---------------------------------------------------------------- TopicValidator 字符表
void testCharTable() {
    // 空串合法：blankness 由 isBlank 那步单独管（Python 同款口径）
    CHECK(!TopicValidator::isTopicOrGroupIllegal(""), "空串不是非法字符");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("%RETRY%group"), "%RETRY%group 合法");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("%DLQ%group"), "%DLQ%group 合法");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("a-b_C|1"), "a-b_C|1 合法");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("Topic123"), "字母数字合法");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a.b"), "点号非法");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a/b"), "正斜杠非法");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a\\b"), "反斜杠非法");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a b"), "空格非法");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a:b"), "冒号非法");
    // UTF-8 中文字符的每一字节都 >= 0x80（Java 按 char >= 128 拒绝，等价）
    CHECK(TopicValidator::isTopicOrGroupIllegal("主题"), "非 ASCII 非法");
    // 边界：0x7F (DEL) 不在表内
    CHECK(TopicValidator::isTopicOrGroupIllegal(std::string("a\x7f" "b")), "DEL 控制符非法");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("a|b_c-d%E"), "五个符号位全通过");
}

// ---------------------------------------------------------------- 系统 / 禁发 topic 名单
void testTopicClassification() {
    // 系统 topic：名单命中 + rmq_sys_ 前缀
    CHECK(TopicValidator::isSystemTopic("SCHEDULE_TOPIC_XXXX"), "SCHEDULE_TOPIC_XXXX 是系统 topic");
    CHECK(TopicValidator::isSystemTopic("TBW102"), "TBW102 是系统 topic");
    CHECK(TopicValidator::isSystemTopic("BenchmarkTest"), "BenchmarkTest 是系统 topic");
    CHECK(TopicValidator::isSystemTopic("RMQ_SYS_TRACE_TOPIC"), "RMQ_SYS_TRACE_TOPIC 是系统 topic");
    CHECK(TopicValidator::isSystemTopic("rmq_sys_ANYTHING"), "rmq_sys_ 前缀即系统 topic");
    CHECK(!TopicValidator::isSystemTopic("NormalTopic"), "普通 topic 不是系统 topic");
    CHECK(!TopicValidator::isSystemTopic("%RETRY%grp"), "%RETRY% 不是系统 topic 名单成员");

    // 禁发名单：Java/Python 的 8 个成员一个不多一个不少
    const char* forbidden[] = {"SCHEDULE_TOPIC_XXXX", "RMQ_SYS_TRANS_HALF_TOPIC",
                               "RMQ_SYS_TRANS_OP_HALF_TOPIC", "TRANS_CHECK_MAX_TIME_TOPIC",
                               "SELF_TEST_TOPIC", "OFFSET_MOVED_EVENT",
                               "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC",
                               "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC"};
    for (const char* t : forbidden) {
        CHECK(TopicValidator::isNotAllowedSendTopic(t), std::string("禁发: ") + t);
    }
    CHECK(TopicValidator::notAllowedSendTopicSet().size() == 8, "禁发名单恰好 8 个成员");
    // TBW102 是系统 topic 但**允许**发送（默认 topic / 建 topic 的 key 就靠它）
    CHECK(!TopicValidator::isNotAllowedSendTopic("TBW102"), "TBW102 不在禁发名单");
    CHECK(!TopicValidator::isNotAllowedSendTopic("BenchmarkTest"), "BenchmarkTest 不在禁发名单");
    // ⚠ %RETRY% 必须在允许侧：broker 的 sendMessageBack 就是往 %RETRY%group 写的，
    // 禁掉会打断重投链路（Python validators.py 的模块注释专门点了这条）。
    CHECK(!TopicValidator::isNotAllowedSendTopic("%RETRY%grp"), "%RETRY% 允许发送（重投链路）");
}

// ---------------------------------------------------------------- checkTopic
void testCheckTopic() {
    CHECK(throwsMessage([] { Validators::checkTopic(""); }, "The specified topic is blank", 1),
          "空 topic 文案");
    CHECK(throwsMessage([] { Validators::checkTopic("   "); }, "The specified topic is blank", 1),
          "纯空白 topic 文案");
    CHECK(throwsMessage([] { Validators::checkTopic(repeatedChar('a', 128)); },
                        "The specified topic is longer than topic max length 127.", 1),
          "128 字符 topic 超长");
    CHECK(!capture([] { Validators::checkTopic(repeatedChar('a', 127)); }).threw,
          "127 字符 topic 恰好通过");
    CHECK(throwsMessage([] { Validators::checkTopic("topic.with.dot"); },
                        "The specified topic[topic.with.dot]" + std::string(kIllegalTail), 1),
          "点号 topic 非法字符文案");
    CHECK(throwsMessage([] { Validators::checkTopic("a b"); },
                        "The specified topic[a b]" + std::string(kIllegalTail), 1),
          "空格 topic 非法字符文案");
    CHECK(throwsMessage([] { Validators::checkTopic("topic中文"); },
                        "The specified topic[topic中文]" + std::string(kIllegalTail), 1),
          "非 ASCII topic 非法字符文案（原文回显）");
    CHECK(!capture([] { Validators::checkTopic("%RETRY%group"); }).threw,
          "%RETRY%group 通过 checkTopic");
    CHECK(!capture([] { Validators::checkTopic("a-b_C|1"); }).threw, "a-b_C|1 通过 checkTopic");
}

// ---------------------------------------------------------------- checkGroup
void testCheckGroup() {
    CHECK(throwsMessage([] { Validators::checkGroup(""); }, "the specified group is blank", 1),
          "空组文案（注意与 topic 的大小写差异，照抄 Python）");
    CHECK(throwsMessage([] { Validators::checkGroup("\t "); }, "the specified group is blank", 1),
          "制表符组也算 blank");
    CHECK(throwsMessage([] { Validators::checkGroup(repeatedChar('g', 121)); },
                        "the specified group[" + repeatedChar('g', 121) +
                            "] is longer than group max length: 120.",
                        1),
          "121 字符组超长（上限 120，为 %RETRY%group_topic 留余量）");
    CHECK(!capture([] { Validators::checkGroup(repeatedChar('g', 120)); }).threw,
          "120 字符组恰好通过");
    CHECK(throwsMessage([] { Validators::checkGroup("bad.group"); },
                        "the specified group[bad.group]" + std::string(kIllegalTail), 1),
          "非法字符组文案");
    CHECK(!capture([] { Validators::checkGroup("CID_ok-1|2%3"); }).threw, "合法组通过");
}

// ---------------------------------------------------------------- isSystemTopic / 禁发（抛异常版）
void testSystemAndForbiddenThrowers() {
    CHECK(throwsMessage([] { Validators::isSystemTopic("SCHEDULE_TOPIC_XXXX"); },
                        "The topic[SCHEDULE_TOPIC_XXXX] is conflict with system topic.", 1),
          "系统 topic 冲突文案");
    CHECK(!capture([] { Validators::isSystemTopic("NormalTopic"); }).threw,
          "普通 topic 不触发系统冲突");
    CHECK(throwsMessage([] { Validators::isNotAllowedSendTopic("SELF_TEST_TOPIC"); },
                        "Sending message to topic[SELF_TEST_TOPIC] is forbidden.", 1),
          "禁发 topic 文案");
    CHECK(!capture([] { Validators::isNotAllowedSendTopic("%RETRY%grp"); }).threw,
          "%RETRY% 不被禁发拦截");
}

// ---------------------------------------------------------------- checkMessage
void testCheckMessage() {
    auto okMsg = [](const std::string& topic, const std::string& body) {
        Message m;
        m.topic = topic;
        m.body = body;
        m.hasBody = true;
        return m;
    };
    const int32_t kMax = 1024;

    CHECK(!capture([&] { Validators::checkMessage(okMsg("TopicOk", "hello"), kMax); }).threw,
          "合法消息通过");

    // 顺序：topic 三档 → 禁发 topic → body 三档 → INNER_MULTI_DISPATCH
    CHECK(throwsMessage([&] { Validators::checkMessage(okMsg("", "b"), kMax); },
                        "The specified topic is blank", 1),
          "空 topic 先于 body 检查（消息是 topic 文案、码是默认 1）");
    CHECK(throwsMessage([&] { Validators::checkMessage(okMsg("a.b", ""), kMax); },
                        "The specified topic[a.b]" + std::string(kIllegalTail), 1),
          "非法 topic 先于空 body");
    CHECK(throwsMessage([&] { Validators::checkMessage(okMsg("SCHEDULE_TOPIC_XXXX", "b"), kMax); },
                        "Sending message to topic[SCHEDULE_TOPIC_XXXX] is forbidden.", 1),
          "禁发 topic 先于 body 检查");

    // body 三档都带 MESSAGE_ILLEGAL(13)
    {
        Message m = okMsg("TopicOk", "");
        CHECK(throwsMessage([&] { Validators::checkMessage(m, kMax); },
                            "the message body length is zero", ResponseCode::MESSAGE_ILLEGAL),
              "空 body 文案 + 码 13");
        m.hasBody = false;
        CHECK(throwsMessage([&] { Validators::checkMessage(m, kMax); }, "the message body is null",
                            ResponseCode::MESSAGE_ILLEGAL),
              "null body（hasBody=false）文案 + 码 13");
    }
    {
        Message m = okMsg("TopicOk", repeatedChar('x', 5));
        CHECK(throwsMessage([&] { Validators::checkMessage(m, 4); },
                            "the message body size over max value, MAX: 4",
                            ResponseCode::MESSAGE_ILLEGAL),
              "超 maxMessageSize 文案 + 码 13");
        CHECK(!capture([&] { Validators::checkMessage(m, 5); }).threw, "恰好等于上限通过");
    }

    // INNER_MULTI_DISPATCH：带平台文件分隔符必须被拒（LMQ 路径越界防护）
    {
        Message m = okMsg("TopicOk", "hello");
        m.setUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                          std::string("a") + kFileSeparator + "b");
        Captured c = capture([&] { Validators::checkMessage(m, kMax); });
        CHECK(c.threw && c.responseCode == ResponseCode::MESSAGE_ILLEGAL &&
                  c.what == "INNER_MULTI_DISPATCH a" + std::string(kFileSeparator) +
                                "b can not contains " + kFileSeparator + " character",
              "INNER_MULTI_DISPATCH 含分隔符被拒（文案含实际路径与分隔符）");
    }
    {
        // LMQ 的常规取值（':' 分隔、无路径分隔符）不能误杀
        Message m = okMsg("TopicOk", "hello");
        m.setUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%queue:testCID");
        CHECK(!capture([&] { Validators::checkMessage(m, kMax); }).threw,
              "无分隔符的 INNER_MULTI_DISPATCH 放行");
    }
}

// ---------------------------------------------------------------- start() 组名守卫（不碰网络）
class NoopListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

void testProducerGroupGuard() {
    // 构造器守卫：纯空白组名直接被 ctor 拒绝（既有行为，isBlank 口径）
    CHECK(throwsMessage([] { DefaultMQProducer p("   "); (void)p; }, "producerGroup is empty", 1),
          "构造器挡空白组");

    // 非法字符组：start() 在建 MQClientInstance 之前抛 => namesrv 指向 127.0.0.1:1 也不会有连接
    {
        DefaultMQProducer p("bad.group");
        p.setNamesrvAddr("127.0.0.1:1");
        Captured c = capture([&p] { p.start(); });
        CHECK(c.threw && c.what == "the specified group[bad.group]" + std::string(kIllegalTail),
              "生产者 start 挡非法字符组");
        CHECK(!p.isStarted(), "校验失败的 producer 不进入 started 态");
    }
    // 保留组 DEFAULT_PRODUCER：构造放行（默认参数就用它），start 才拒
    {
        DefaultMQProducer p;
        p.setNamesrvAddr("127.0.0.1:1");
        Captured c = capture([&p] { p.start(); });
        CHECK(c.threw &&
                  c.what ==
                      "producerGroup can not equal DEFAULT_PRODUCER, please specify another one.",
              "生产者 start 挡 DEFAULT_PRODUCER");
        CHECK(!p.isStarted(), "保留组 producer 不进入 started 态");
    }
}

void testConsumerGroupGuard() {
    // push：前置的订阅/监听器都配好，唯一失败点就是组名校验
    {
        DefaultMQPushConsumer c("DEFAULT_CONSUMER");
        c.setNamesrvAddr("127.0.0.1:1");
        c.subscribe("TopicOk", "*");
        c.setMessageListener(std::make_shared<NoopListener>());
        Captured r = capture([&c] { c.start(); });
        CHECK(r.threw &&
                  r.what ==
                      "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.",
              "push 消费者 start 挡 DEFAULT_CONSUMER");
        CHECK(!c.isStarted(), "被拒的 push consumer 不进入 started 态");
    }
    {
        DefaultMQPushConsumer c("bad group");
        c.setNamesrvAddr("127.0.0.1:1");
        c.subscribe("TopicOk", "*");
        c.setMessageListener(std::make_shared<NoopListener>());
        Captured r = capture([&c] { c.start(); });
        CHECK(r.threw && r.what == "the specified group[bad group]" + std::string(kIllegalTail),
              "push 消费者 start 挡非法字符组（空格）");
    }
    // pull：构造器已用 isBlank 挡空白组；start 挡保留组与非法字符
    CHECK(throwsMessage([] { DefaultMQPullConsumer c("  "); (void)c; }, "consumerGroup is empty",
                        1),
          "pull 构造器挡纯空白组（旧实现 empty() 会漏）");
    {
        DefaultMQPullConsumer c(MixAll::DEFAULT_CONSUMER_GROUP);
        c.setNamesrvAddr("127.0.0.1:1");
        Captured r = capture([&c] { c.start(); });
        CHECK(r.threw &&
                  r.what ==
                      "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.",
              "pull 消费者 start 挡 DEFAULT_CONSUMER");
        CHECK(!c.isStarted(), "被拒的 pull consumer 不进入 started 态");
    }
    // lite 构造器只挡空白，非法字符留给 start（构造必须放行，否则默认参数语义都变了）
    CHECK(!capture([] { DefaultLitePullConsumer c("bad.group"); (void)c; }).threw,
          "lite 构造器放行非法字符组");
    {
        DefaultLitePullConsumer c("bad.group");
        c.setNamesrvAddr("127.0.0.1:1");
        c.subscribe("TopicOk", "*");
        Captured r = capture([&c] { c.start(); });
        CHECK(r.threw && r.what == "the specified group[bad.group]" + std::string(kIllegalTail),
              "lite 消费者 start 挡非法字符组");
        CHECK(!c.isStarted(), "被拒的 lite consumer 不进入 started 态");
    }
}

}  // namespace

int main() {
    testCharTable();
    testTopicClassification();
    testCheckTopic();
    testCheckGroup();
    testSystemAndForbiddenThrowers();
    testCheckMessage();
    testProducerGroupGuard();
    testConsumerGroupGuard();

    std::cout << "validators: PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
