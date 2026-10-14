// Validators / TopicValidator unit tests: illegal arguments fail fast.
//
// Three things are pinned down:
//   1. the wording and the check ORDER, verbatim as production records them;
//   2. the body / size / INNER_MULTI_DISPATCH tiers carry MESSAGE_ILLEGAL(13), while the
//      topic / group tiers use the default code UNKNOWN=1;
//   3. the producer / consumer start() group-name guards run before the client instance is
//      created, so an illegal or reserved group fails without touching the network (a name server pointing at 127.0.0.1:1 still starts no connection).
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
    int32_t responseCode = 0;  // stays 0 when nothing was thrown; only meaningful after a throw
};

// captures only MQClientException: other exception types escape and crash the process, which is the bug to expose
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

// expected wording identical, character by character, to the production baseline (editing this edits the golden)
const char* kIllegalTail = " contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$";

// ---------------------------------------------------------------- TopicValidator character table
void testCharTable() {
    // an empty string is legal: blankness is handled by the separate isBlank step
    CHECK(!TopicValidator::isTopicOrGroupIllegal(""), "an empty string is not an illegal character");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("%RETRY%group"), "%RETRY%group is legal");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("%DLQ%group"), "%DLQ%group is legal");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("a-b_C|1"), "a-b_C|1 is legal");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("Topic123"), "letters and digits are legal");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a.b"), "a dot is illegal");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a/b"), "a forward slash is illegal");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a\\b"), "a backslash is illegal");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a b"), "a space is illegal");
    CHECK(TopicValidator::isTopicOrGroupIllegal("a:b"), "a colon is illegal");
    CHECK(TopicValidator::isTopicOrGroupIllegal("主题"), "non-ASCII is illegal");
    // boundary: 0x7F (DEL) is not in the table
    CHECK(TopicValidator::isTopicOrGroupIllegal(std::string("a\x7f" "b")), "the DEL control character is illegal");
    CHECK(!TopicValidator::isTopicOrGroupIllegal("a|b_c-d%E"), "all five punctuation slots pass");
}

// ---------------------------------------------------------------- system / forbidden-send topic lists
void testTopicClassification() {
    // system topics: list membership plus the rmq_sys_ prefix
    CHECK(TopicValidator::isSystemTopic("SCHEDULE_TOPIC_XXXX"), "SCHEDULE_TOPIC_XXXX is a system topic");
    CHECK(TopicValidator::isSystemTopic("TBW102"), "TBW102 is a system topic");
    CHECK(TopicValidator::isSystemTopic("BenchmarkTest"), "BenchmarkTest is a system topic");
    CHECK(TopicValidator::isSystemTopic("RMQ_SYS_TRACE_TOPIC"), "RMQ_SYS_TRACE_TOPIC is a system topic");
    CHECK(TopicValidator::isSystemTopic("rmq_sys_ANYTHING"), "the rmq_sys_ prefix alone makes it a system topic");
    CHECK(!TopicValidator::isSystemTopic("NormalTopic"), "an ordinary topic is not a system topic");
    CHECK(!TopicValidator::isSystemTopic("%RETRY%grp"), "%RETRY% is not a member of the system topic list");

    const char* forbidden[] = {"SCHEDULE_TOPIC_XXXX", "RMQ_SYS_TRANS_HALF_TOPIC",
                               "RMQ_SYS_TRANS_OP_HALF_TOPIC", "TRANS_CHECK_MAX_TIME_TOPIC",
                               "SELF_TEST_TOPIC", "OFFSET_MOVED_EVENT",
                               "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC",
                               "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC"};
    for (const char* t : forbidden) {
        CHECK(TopicValidator::isNotAllowedSendTopic(t), std::string("forbidden: ") + t);
    }
    CHECK(TopicValidator::notAllowedSendTopicSet().size() == 8, "the forbidden list holds exactly 8 members");
    // TBW102 is a system topic but sending IS allowed (it is the default topic and the create-topic key)
    CHECK(!TopicValidator::isNotAllowedSendTopic("TBW102"), "TBW102 is not on the forbidden list");
    CHECK(!TopicValidator::isNotAllowedSendTopic("BenchmarkTest"), "BenchmarkTest is not on the forbidden list");
    // %RETRY% must stay on the allowed side: the broker's sendMessageBack writes to %RETRY%group,
    // forbidding it would break the redelivery path.
    CHECK(!TopicValidator::isNotAllowedSendTopic("%RETRY%grp"), "%RETRY% may be sent to (redelivery path)");
}

// ---------------------------------------------------------------- checkTopic
void testCheckTopic() {
    CHECK(throwsMessage([] { Validators::checkTopic(""); }, "The specified topic is blank", 1),
          "the empty topic message");
    CHECK(throwsMessage([] { Validators::checkTopic("   "); }, "The specified topic is blank", 1),
          "the whitespace-only topic message");
    CHECK(throwsMessage([] { Validators::checkTopic(repeatedChar('a', 128)); },
                        "The specified topic is longer than topic max length 127.", 1),
          "a 128 character topic is too long");
    CHECK(!capture([] { Validators::checkTopic(repeatedChar('a', 127)); }).threw,
          "a 127 character topic passes exactly");
    CHECK(throwsMessage([] { Validators::checkTopic("topic.with.dot"); },
                        "The specified topic[topic.with.dot]" + std::string(kIllegalTail), 1),
          "the dotted topic illegal-character message");
    CHECK(throwsMessage([] { Validators::checkTopic("a b"); },
                        "The specified topic[a b]" + std::string(kIllegalTail), 1),
          "the spaced topic illegal-character message");
    CHECK(throwsMessage([] { Validators::checkTopic("topic中文"); },
                        "The specified topic[topic中文]" + std::string(kIllegalTail), 1),
          "the non-ASCII topic illegal-character message (input echoed back)");
    CHECK(!capture([] { Validators::checkTopic("%RETRY%group"); }).threw,
          "%RETRY%group passes checkTopic");
    CHECK(!capture([] { Validators::checkTopic("a-b_C|1"); }).threw, "a-b_C|1 passes checkTopic");
}

// ---------------------------------------------------------------- checkGroup
void testCheckGroup() {
    CHECK(throwsMessage([] { Validators::checkGroup(""); }, "the specified group is blank", 1),
          "the empty group message (note the case difference from topic)");
    CHECK(throwsMessage([] { Validators::checkGroup("\t "); }, "the specified group is blank", 1),
          "a tab-only group counts as blank");
    CHECK(throwsMessage([] { Validators::checkGroup(repeatedChar('g', 121)); },
                        "the specified group[" + repeatedChar('g', 121) +
                            "] is longer than group max length: 120.",
                        1),
          "a 121 character group is too long (limit 120, leaving room for %RETRY%group_topic)");
    CHECK(!capture([] { Validators::checkGroup(repeatedChar('g', 120)); }).threw,
          "a 120 character group passes exactly");
    CHECK(throwsMessage([] { Validators::checkGroup("bad.group"); },
                        "the specified group[bad.group]" + std::string(kIllegalTail), 1),
          "the illegal-character group message");
    CHECK(!capture([] { Validators::checkGroup("CID_ok-1|2%3"); }).threw, "a legal group passes");
}

// ---------------------------------------------------------------- isSystemTopic / forbidden (throwing variants)
void testSystemAndForbiddenThrowers() {
    CHECK(throwsMessage([] { Validators::isSystemTopic("SCHEDULE_TOPIC_XXXX"); },
                        "The topic[SCHEDULE_TOPIC_XXXX] is conflict with system topic.", 1),
          "the system topic conflict message");
    CHECK(!capture([] { Validators::isSystemTopic("NormalTopic"); }).threw,
          "an ordinary topic does not trigger the system conflict");
    CHECK(throwsMessage([] { Validators::isNotAllowedSendTopic("SELF_TEST_TOPIC"); },
                        "Sending message to topic[SELF_TEST_TOPIC] is forbidden.", 1),
          "the forbidden topic message");
    CHECK(!capture([] { Validators::isNotAllowedSendTopic("%RETRY%grp"); }).threw,
          "%RETRY% is not blocked as forbidden");
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
          "a legal message passes");

    // order: the three topic tiers -> forbidden topic -> the three body tiers -> INNER_MULTI_DISPATCH
    CHECK(throwsMessage([&] { Validators::checkMessage(okMsg("", "b"), kMax); },
                        "The specified topic is blank", 1),
          "an empty topic is checked before the body (topic wording, default code 1)");
    CHECK(throwsMessage([&] { Validators::checkMessage(okMsg("a.b", ""), kMax); },
                        "The specified topic[a.b]" + std::string(kIllegalTail), 1),
          "an illegal topic is checked before an empty body");
    CHECK(throwsMessage([&] { Validators::checkMessage(okMsg("SCHEDULE_TOPIC_XXXX", "b"), kMax); },
                        "Sending message to topic[SCHEDULE_TOPIC_XXXX] is forbidden.", 1),
          "a forbidden topic is checked before the body");

    // all three body tiers carry MESSAGE_ILLEGAL(13)
    {
        Message m = okMsg("TopicOk", "");
        CHECK(throwsMessage([&] { Validators::checkMessage(m, kMax); },
                            "the message body length is zero", ResponseCode::MESSAGE_ILLEGAL),
              "the empty body message plus code 13");
        m.hasBody = false;
        CHECK(throwsMessage([&] { Validators::checkMessage(m, kMax); }, "the message body is null",
                            ResponseCode::MESSAGE_ILLEGAL),
              "the null body (hasBody=false) message plus code 13");
    }
    {
        Message m = okMsg("TopicOk", repeatedChar('x', 5));
        CHECK(throwsMessage([&] { Validators::checkMessage(m, 4); },
                            "the message body size over max value, MAX: 4",
                            ResponseCode::MESSAGE_ILLEGAL),
              "the over-maxMessageSize message plus code 13");
        CHECK(!capture([&] { Validators::checkMessage(m, 5); }).threw, "exactly at the limit passes");
    }

    // INNER_MULTI_DISPATCH: a platform file separator must be rejected (LMQ path traversal guard)
    {
        Message m = okMsg("TopicOk", "hello");
        m.setUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                          std::string("a") + kFileSeparator + "b");
        Captured c = capture([&] { Validators::checkMessage(m, kMax); });
        CHECK(c.threw && c.responseCode == ResponseCode::MESSAGE_ILLEGAL &&
                  c.what == "INNER_MULTI_DISPATCH a" + std::string(kFileSeparator) +
                                "b can not contains " + kFileSeparator + " character",
              "INNER_MULTI_DISPATCH with a separator is rejected (the message carries the path and the separator)");
    }
    {
        // the usual LMQ value (':' separated, no path separator) must not be rejected by mistake
        Message m = okMsg("TopicOk", "hello");
        m.setUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%queue:testCID");
        CHECK(!capture([&] { Validators::checkMessage(m, kMax); }).threw,
              "INNER_MULTI_DISPATCH without a separator passes");
    }
}

// ---------------------------------------------------------------- start() group-name guards (no network involved)
class NoopListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

void testProducerGroupGuard() {
    // constructor guard: a whitespace-only group name is rejected by the ctor (existing behaviour, isBlank)
    CHECK(throwsMessage([] { DefaultMQProducer p("   "); (void)p; }, "producerGroup is empty", 1),
          "the constructor blocks a blank group");

    // illegal-character group: start() throws before MQClientInstance is built, so even a name server of 127.0.0.1:1 would open no connection
    {
        DefaultMQProducer p("bad.group");
        p.setNamesrvAddr("127.0.0.1:1");
        Captured c = capture([&p] { p.start(); });
        CHECK(c.threw && c.what == "the specified group[bad.group]" + std::string(kIllegalTail),
              "the producer start blocks an illegal-character group");
        CHECK(!p.isStarted(), "a producer that fails validation does not enter the started state");
    }
    // the reserved group DEFAULT_PRODUCER: construction passes (the default argument uses it), start() rejects
    {
        DefaultMQProducer p;
        p.setNamesrvAddr("127.0.0.1:1");
        Captured c = capture([&p] { p.start(); });
        CHECK(c.threw &&
                  c.what ==
                      "producerGroup can not equal DEFAULT_PRODUCER, please specify another one.",
              "the producer start blocks DEFAULT_PRODUCER");
        CHECK(!p.isStarted(), "a reserved-group producer does not enter the started state");
    }
}

void testConsumerGroupGuard() {
    // push: the subscription and listener ahead of it are set up, so the only failure point is the group name
    {
        DefaultMQPushConsumer c("DEFAULT_CONSUMER");
        c.setNamesrvAddr("127.0.0.1:1");
        c.subscribe("TopicOk", "*");
        c.setMessageListener(std::make_shared<NoopListener>());
        Captured r = capture([&c] { c.start(); });
        CHECK(r.threw &&
                  r.what ==
                      "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.",
              "the push consumer start blocks DEFAULT_CONSUMER");
        CHECK(!c.isStarted(), "the rejected push consumer does not enter the started state");
    }
    {
        DefaultMQPushConsumer c("bad group");
        c.setNamesrvAddr("127.0.0.1:1");
        c.subscribe("TopicOk", "*");
        c.setMessageListener(std::make_shared<NoopListener>());
        Captured r = capture([&c] { c.start(); });
        CHECK(r.threw && r.what == "the specified group[bad group]" + std::string(kIllegalTail),
              "the push consumer start blocks an illegal-character group (space)");
    }
    // pull: the constructor already blocks blank groups with isBlank; start() blocks the reserved group and illegal characters
    CHECK(throwsMessage([] { DefaultMQPullConsumer c("  "); (void)c; }, "consumerGroup is empty",
                        1),
          "the pull constructor blocks a whitespace-only group (empty() alone would miss it)");
    {
        DefaultMQPullConsumer c(MixAll::DEFAULT_CONSUMER_GROUP);
        c.setNamesrvAddr("127.0.0.1:1");
        Captured r = capture([&c] { c.start(); });
        CHECK(r.threw &&
                  r.what ==
                      "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.",
              "the pull consumer start blocks DEFAULT_CONSUMER");
        CHECK(!c.isStarted(), "the rejected pull consumer does not enter the started state");
    }
    // lite ctor only blocks blanks; illegal characters are left to start() (ctor must pass them, else the default-arg semantics change)
    CHECK(!capture([] { DefaultLitePullConsumer c("bad.group"); (void)c; }).threw,
          "the lite constructor allows an illegal-character group");
    {
        DefaultLitePullConsumer c("bad.group");
        c.setNamesrvAddr("127.0.0.1:1");
        c.subscribe("TopicOk", "*");
        Captured r = capture([&c] { c.start(); });
        CHECK(r.threw && r.what == "the specified group[bad.group]" + std::string(kIllegalTail),
              "the lite consumer start blocks an illegal-character group");
        CHECK(!c.isStarted(), "the rejected lite consumer does not enter the started state");
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
