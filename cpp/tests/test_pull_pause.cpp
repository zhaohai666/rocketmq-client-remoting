// Offline unit test for the pull-loop pause mapping: DefaultMQPushConsumer::pullPauseMillisFor maps
// the broker's FLOW_CONTROL(215) answer to a short pause and every other response code to the general
// error backoff; the client's own ProcessQueue threshold pause is a third, distinct value. The three
// events look identical offline (one response code in, one millisecond count out), so the mapping is
// pinned directly rather than through a throttling broker.
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

}  // namespace

int main() {
    testPullPauseMapping();
    std::printf("pull pause: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
