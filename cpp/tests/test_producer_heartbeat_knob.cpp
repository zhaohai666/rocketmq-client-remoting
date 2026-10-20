// Offline unit test for the producer heartbeat period knob.
//
// The heartbeat thread anchors each tick to heartbeatIntervalMillis_ and reads it live, but a field with
// no setter can never be changed -- the knob would be dead. This pins that the knob is reachable and
// that a non-positive period is rejected on the spot (it would anchor the next tick in the past and spin
// the loop with no pause), the same contract the other producer numeric setters enforce.
//
// Everything is a recorded return value of a setter/getter pair; no thread is started, no clock read.
#include <cstdint>
#include <stdexcept>
#include <string>

#include "rocketmq/client/producer.h"

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

bool rejects(int32_t value) {
    DefaultMQProducer producer("GID_HeartbeatKnob");
    try {
        producer.setHeartbeatIntervalMillis(value);
        return false;  // accepted: not rejected
    } catch (const std::invalid_argument&) {
        return true;
    }
}

void testKnobIsReachable() {
    DefaultMQProducer producer("GID_HeartbeatKnob");
    expectInt(producer.heartbeatIntervalMillis(), 30000, "defaultIs30s");

    // A positive value round-trips through the setter/getter: without a setter the field is unreachable
    // and this is the assertion that would not compile in the first place.
    producer.setHeartbeatIntervalMillis(5000);
    expectInt(producer.heartbeatIntervalMillis(), 5000, "setterRoundTrips");

    // The smallest legal period is still accepted -- the rejection is "non-positive", not "anything".
    producer.setHeartbeatIntervalMillis(1);
    expectInt(producer.heartbeatIntervalMillis(), 1, "oneMillisAccepted");
}

void testRejectsNonPositive() {
    // Paired against the accepted positive cases above: zero and negatives are refused.
    expect(rejects(0), "zeroRejected");
    expect(rejects(-1), "negativeRejected");
    expect(rejects(-30000), "largeNegativeRejected");

    // And a rejected assignment leaves the previously configured period untouched.
    DefaultMQProducer producer("GID_HeartbeatKnobKeep");
    producer.setHeartbeatIntervalMillis(7000);
    try {
        producer.setHeartbeatIntervalMillis(-5);
    } catch (const std::invalid_argument&) {
        // expected
    }
    expectInt(producer.heartbeatIntervalMillis(), 7000, "rejectedValueDoesNotOverwrite");
}

}  // namespace

int main() {
    testKnobIsReachable();
    testRejectsNonPositive();
    std::printf("producer heartbeat knob: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
