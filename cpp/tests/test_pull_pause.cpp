// Offline unit test for the consume-loop pause mapping: the pause a pull or POP round takes after it
// failed is a table of three events -- the broker's FLOW_CONTROL(215) answer gets its own short pause,
// this client's own cache-threshold gate gets 50 ms, and every other failure (a non-215 response code,
// a round this client refused) waits out the configurable general error backoff, whose default is
// 1000 ms. The pull and the POP loop consult this one table. The events look identical offline (one
// response code in, one millisecond count out), so the mapping is pinned directly rather than through
// a throttling broker.
//
// No cluster, no fixed sleeps: every value is a recorded return of a pure function or of the same
// table read through a consumer instance's own configured value -- which is the value each round loop
// copies when it starts.
#include <cstdint>
#include <cstdio>
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
    // The general error backoff is a per-consumer knob, and this is the value a consumer starts with.
    expectInt(kDefaultPullPauseWhenPullErrorMillis, 1000, "defaultIs1000ms");
    DefaultMQPushConsumer consumer("GID_PullPauseMapping");
    expectInt(consumer.pullPauseWhenPullErrorMillis(), kDefaultPullPauseWhenPullErrorMillis,
              "freshConsumerCarriesTheDefault");

    // The broker's own throttle answer keeps its short pause -- at the default backoff, and at any
    // other value, because the 215 row never reads the knob.
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(ResponseCode::FLOW_CONTROL,
                                                        kDefaultPullPauseWhenPullErrorMillis),
              kPullPauseWhenBrokerFlowControl, "flowControl.usesBrokerPause");
    expectInt(consumer.pullPauseMillisFor(ResponseCode::FLOW_CONTROL),
              kPullPauseWhenBrokerFlowControl, "flowControl.usesBrokerPauseThroughTheConsumer");

    // Every other response code (a missing topic, a generic success-shaped error, zero) uses the
    // general error backoff -- not the broker's short one. These are the paired controls: if the two
    // events collapsed back onto one shared value, one of these assertions goes red.
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(ResponseCode::TOPIC_NOT_EXIST, 1000), 1000,
              "topicNotExist.usesErrorPause");
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(0, 1000), 1000, "zero.usesErrorPause");
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(-1, 1000), 1000, "negative.usesErrorPause");
    expectInt(consumer.pullPauseMillisFor(ResponseCode::TOPIC_NOT_EXIST),
              kDefaultPullPauseWhenPullErrorMillis, "topicNotExist.usesThisConsumersPause");

    // Setting the knob changes the wait the loops take: both loops read this consumer's value when
    // they start, so a non-215 round waits exactly what the knob says -- while 215 stays at 20 ms.
    consumer.setPullPauseWhenPullErrorMillis(321);
    expectInt(consumer.pullPauseWhenPullErrorMillis(), 321, "knob.roundTrips");
    expectInt(consumer.pullPauseMillisFor(ResponseCode::SYSTEM_BUSY), 321,
              "knob.drivesTheErrorArm");
    expectInt(consumer.pullPauseMillisFor(ResponseCode::FLOW_CONTROL),
              kPullPauseWhenBrokerFlowControl, "knob.leavesTheBrokerPauseAlone");
    // Control: another consumer keeps its own value -- the wait is per client, not one global number.
    DefaultMQPushConsumer other("GID_PullPauseMappingOther");
    expectInt(other.pullPauseMillisFor(ResponseCode::SYSTEM_BUSY),
              kDefaultPullPauseWhenPullErrorMillis, "knobIsPerConsumer");

    // The three backoffs are genuinely distinct durations; pinning the map is meaningless if two of
    // them already merged, so the values themselves must stay separated.
    expect(kPullPauseWhenBrokerFlowControl != kDefaultPullPauseWhenPullErrorMillis,
           "brokerAndErrorPausesDiffer");
    expect(kPullPauseWhenClientFlowControl != kPullPauseWhenBrokerFlowControl &&
               kPullPauseWhenClientFlowControl != kDefaultPullPauseWhenPullErrorMillis,
           "clientPauseIsItsOwnValue");

    // The applied pause only becomes visible after a real pause: a fresh consumer reports nothing yet.
    DefaultMQPushConsumer fresh("GID_PullPauseMappingFresh");
    expectInt(fresh.lastPullPauseMillis(), 0, "freshConsumerNoPauseRecorded");
}

// The POP loop answers a broker error with this same table: a FLOW_CONTROL(215) pop reply takes the
// short pause, any other code this consumer's configured backoff, and neither one the client's own
// cache pause. Offline a pop reply and a pull reply are indistinguishable (one response code in, one
// pause out), so the pair below pins the two directions on the shared table: folding the pop branch
// onto the cache pause, or onto the 215 row, makes one of the two lines red.
void testPopPauseSharesTheTable() {
    DefaultMQPushConsumer consumer("GID_PopPauseMapping");
    expectInt(DefaultMQPushConsumer::pullPauseMillisFor(ResponseCode::FLOW_CONTROL, 1000),
              kPullPauseWhenBrokerFlowControl, "pop.flowControl.usesBrokerPause");
    expectInt(consumer.pullPauseMillisFor(ResponseCode::SYSTEM_BUSY),
              kDefaultPullPauseWhenPullErrorMillis, "pop.systemBusy.usesErrorPause");
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
