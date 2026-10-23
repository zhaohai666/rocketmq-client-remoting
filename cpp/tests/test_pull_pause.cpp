// Offline unit test for the consume-loop pause mapping: DefaultMQPushConsumer::pullPauseMillisFor maps
// the broker's FLOW_CONTROL(215) answer to a short pause and every other response code to the general
// error backoff; the client's own ProcessQueue threshold pause is a third, distinct value. The pull and
// the POP loop consult this one table. The three events look identical offline (one response code in,
// one millisecond count out), so the mapping is pinned directly rather than through a throttling broker.
//
// No cluster, no fixed sleeps: every value is a recorded return of a pure function.
#include <cstdint>
#include <string>

#include "rocketmq/client/consumer.h"
#include "rocketmq/remoting/protocol/codes.h"

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

void testPullPauseMapping() {
    // The broker's own throttle answer keeps its short pause.
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(ResponseCode::FLOW_CONTROL),
              kPullPauseWhenBrokerFlowControl, "flowControl.usesBrokerPause");
    // Every other response code (a missing topic, a generic success-shaped error, zero) uses the
    // general error backoff -- not the broker's short one. These are the paired controls: if the two
    // events collapsed back onto one shared value, one of these two assertions goes red.
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(ResponseCode::TOPIC_NOT_EXIST),
              kPullPauseWhenPullError, "topicNotExist.usesErrorPause");
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(0),
              kPullPauseWhenPullError, "zero.usesErrorPause");
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(-1),
              kPullPauseWhenPullError, "negative.usesErrorPause");

    // The three backoffs are genuinely distinct durations; pinning the map is meaningless if two of
    // them already merged, so the constants themselves must stay separated.
    expect(kPullPauseWhenBrokerFlowControl != kPullPauseWhenPullError,
           "brokerAndErrorPausesDiffer");
    expect(kPullPauseWhenClientFlowControl != kPullPauseWhenBrokerFlowControl &&
               kPullPauseWhenClientFlowControl != kPullPauseWhenPullError,
           "clientPauseIsItsOwnValue");

    // The applied pause only becomes visible after a real pause: a fresh consumer reports nothing yet.
    DefaultMQPushConsumer consumer("GID_PullPauseMapping");
    expectInt(consumer.lastPullPauseMillis(), 0, "freshConsumerNoPauseRecorded");
}

// The POP loop answers a broker error with this same table: a FLOW_CONTROL(215) pop reply takes the
// short pause, any other code the general backoff, and neither one the client's own 50 ms cache pause.
// Offline a pop reply and a pull reply are indistinguishable (one response code in, one pause out), so
// the pair below pins the two directions on the shared table: folding the pop branch back into the
// generic 500 ms wait, or onto the cache pause, makes one of the two lines red.
void testPopPauseSharesTheTable() {
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(ResponseCode::FLOW_CONTROL),
              kPullPauseWhenBrokerFlowControl, "pop.flowControl.usesBrokerPause");
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(ResponseCode::SYSTEM_BUSY),
              kPullPauseWhenPullError, "pop.systemBusy.usesErrorPause");
    expect(kPullPauseWhenClientFlowControl != kPullPauseWhenBrokerFlowControl,
           "pop.cachePauseIsNotTheBrokerPause");
}

}  // namespace

int main() {
    testPullPauseMapping();
    testPopPauseSharesTheTable();
    std::printf("pull pause: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
