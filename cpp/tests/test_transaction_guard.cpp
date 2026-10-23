// Offline unit test for the schedule guard on the transactional send path -- no cluster required.
#include <exception>
#include <iostream>
#include <string>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"

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

namespace {

class CommitListener : public TransactionListener {
public:
    LocalTransactionState executeLocalTransaction(const Message&, const std::string&) override {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
    LocalTransactionState checkLocalTransaction(const MessageExt&) override {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
};

constexpr const char* kRefusal = "Transactional messages do not support delayed delivery";

const char* const kScheduleKeys[] = {MessageConst::PROPERTY_DELAY_TIME_LEVEL,
                                     MessageConst::PROPERTY_TIMER_DELAY_SEC,
                                     MessageConst::PROPERTY_TIMER_DELAY_MS,
                                     MessageConst::PROPERTY_TIMER_DELIVER_MS};

// What the send path answered with; an empty string means it did not throw at all. The producer
// is never started, so a message the guard lets through stops at "producer not started".
std::string refuse(const Message& msg) {
    DefaultMQProducer producer("PG_TxGuard");
    CommitListener listener;
    try {
        producer.sendMessageInTransaction(msg, listener, "");
    } catch (const std::exception& e) {
        return e.what();
    }
    return std::string();
}

void testEveryScheduleSpellingRefuses() {
    // A half message is committed exactly as it stands, so no schedule asked for here can be
    // honoured. The key alone decides -- unlike the batch gate, which reads the value: a "0" or
    // an empty value asks for nothing and still refuses.
    for (const char* key : kScheduleKeys) {
        for (const char* value : {"3", "0", ""}) {
            Message msg("T_Tx", "a");
            msg.properties[key] = value;
            CHECK(refuse(msg) == kRefusal,
                  std::string(key) + "=" + value + " refuses the transactional send");
        }
    }

    // The setters the public API exposes reach the same four properties.
    Message viaLevel("T_Tx", "a");
    viaLevel.setDelayTimeLevel(2);
    CHECK(refuse(viaLevel) == kRefusal, "setDelayTimeLevel refuses");
    Message viaSec("T_Tx", "a");
    viaSec.setDelayTimeSec(30);
    CHECK(refuse(viaSec) == kRefusal, "setDelayTimeSec refuses");
    Message viaMs("T_Tx", "a");
    viaMs.setDelayTimeMs(30000);
    CHECK(refuse(viaMs) == kRefusal, "setDelayTimeMs refuses");
    Message viaDeliver("T_Tx", "a");
    viaDeliver.setDeliverTimeMs(1700000000000LL);
    CHECK(refuse(viaDeliver) == kRefusal, "setDeliverTimeMs refuses");
}

void testRefusalLeavesTheMessageAlone() {
    Message msg("T_Tx", "a");
    msg.properties[MessageConst::PROPERTY_TIMER_DELIVER_MS] = "1700000000000";
    refuse(msg);
    CHECK(msg.getProperties().count(MessageConst::PROPERTY_TRANSACTION_PREPARED) == 0,
          "a refused message carries no half-message marker");
    CHECK(msg.getProperties().count(MessageConst::PROPERTY_PRODUCER_GROUP) == 0,
          "a refused message carries no producer group");
}

void testOnlyTheFourNamesRefuse() {
    // DELAY_TIME only looks like a schedule spelling and is not one, so the message reaches the
    // send path -- pinning that the refusal list holds exactly the four keys.
    Message lookalike("T_Tx", "a");
    lookalike.properties["DELAY_TIME"] = "3";
    CHECK(refuse(lookalike) != kRefusal, "DELAY_TIME is not a schedule spelling");
    CHECK(refuse(lookalike).find("not started") != std::string::npos,
          "the lookalike message stops at the unstarted producer");

    Message plain("T_Tx", "a");
    CHECK(refuse(plain).find("not started") != std::string::npos,
          "a plain message reaches the send path");
}

}  // namespace

int main() {
    testEveryScheduleSpellingRefuses();
    testRefusalLeavesTheMessageAlone();
    testOnlyTheFourNamesRefuse();

    std::cout << "\n===== transaction guard summary =====\n";
    std::cout << "  PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
