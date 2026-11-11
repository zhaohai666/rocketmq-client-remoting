// The capability gaps closed against the Java 5.x reference client.
//
// Each block pins one Java facade to this port and pairs the new accessor with the behaviour it
// steers, so a refactor that keeps the setter but drops the wiring goes red here:
//
//  - ConsumeFromWhere: all six values of the Java enum. The three "deprecated" ones are not dead:
//    Java folds them into the CONSUME_FROM_LAST_OFFSET branch of the offset-init switch
//    (RebalancePushImpl#computePullFromWhereWithException lines 171-173), so accepting them is the
//    whole of their behaviour.
//  - LitePull connectBrokerByUser / defaultBrokerId: the only way to read from a slave
//    (PullAPIWrapper#recalculatePullFromWhichNode). Off must keep the historic "no broker id on
//    the request" shape, or a route-missing broker starts reporting a different error.
//  - Admin adminExtGroup: default "admin_ext_group" plus the Java accessor pair.
//
// Nothing here dials: the pin is read through liteBrokerIdForTest, the broker id a round would
// carry, and the group is a value on the facade.
#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void check(bool cond, const std::string& label) {
    checks++;
    if (!cond) {
        fails++;
        std::fprintf(stderr, "FAIL: %s\n", label.c_str());
    }
}

void checkStr(const std::string& expected, const std::string& actual, const std::string& label) {
    checks++;
    if (expected != actual) {
        fails++;
        std::fprintf(stderr, "FAIL: %s\n  expected: %s\n  actual:   %s\n", label.c_str(),
                     expected.c_str(), actual.c_str());
    }
}

void checkInt(int64_t expected, int64_t actual, const std::string& label) {
    checks++;
    if (expected != actual) {
        fails++;
        std::fprintf(stderr, "FAIL: %s\n  expected: %lld\n  actual:   %lld\n", label.c_str(),
                     static_cast<long long>(expected), static_cast<long long>(actual));
    }
}

// The authoritative list is Java common/.../consumer/ConsumeFromWhere.java.
const char* kJavaConsumeFromWhere[] = {
    "CONSUME_FROM_LAST_OFFSET",
    "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
    "CONSUME_FROM_MIN_OFFSET",
    "CONSUME_FROM_MAX_OFFSET",
    "CONSUME_FROM_FIRST_OFFSET",
    "CONSUME_FROM_TIMESTAMP",
};

void testConsumeFromWhereHasAllSixJavaValues() {
    const char* ours[] = {
        ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
        ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
        ConsumeFromWhere::CONSUME_FROM_MIN_OFFSET,
        ConsumeFromWhere::CONSUME_FROM_MAX_OFFSET,
        ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET,
        ConsumeFromWhere::CONSUME_FROM_TIMESTAMP,
    };
    for (int i = 0; i < 6; i++) {
        checkStr(kJavaConsumeFromWhere[i], ours[i],
                 std::string("ConsumeFromWhere constant #") + std::to_string(i) +
                     " equals its wire string");
    }
}

void testLitePullBrokerPin() {
    DefaultLitePullConsumer c("GID_CloseoutPin");
    const MessageQueue mq("CloseoutTopic", "broker-a", 0);

    check(!c.isConnectBrokerByUser(), "the pin defaults to off");
    checkInt(MixAll::MASTER_ID, c.defaultBrokerId(), "defaultBrokerId defaults to MASTER_ID");

    // Off means "no broker id on the request": MQClientInstance::pullMessage then resolves the
    // address from the route, which is what every lite round did before this switch existed.
    check(!c.liteBrokerIdForTest(mq).has_value(), "pin off puts no broker id on the request");

    c.setConnectBrokerByUser(true);
    c.setDefaultBrokerId(1);
    check(c.isConnectBrokerByUser(), "the pin reports on after being set");
    checkInt(1, c.defaultBrokerId(), "defaultBrokerId round-trips");
    std::optional<int64_t> pinned = c.liteBrokerIdForTest(mq);
    check(pinned.has_value() && *pinned == 1, "pin on asks the pinned broker id");

    c.setConnectBrokerByUser(false);
    check(!c.liteBrokerIdForTest(mq).has_value(),
          "unpinning restores the historic request shape");
    checkInt(1, c.defaultBrokerId(), "unpinning keeps the id; only the switch decides its use");
}

void testAdminExtGroup() {
    DefaultMQAdminExt admin;
    checkStr("admin_ext_group", admin.getAdminExtGroup(), "facade default matches Java");
    admin.setAdminExtGroup("custom-admin-group");
    checkStr("custom-admin-group", admin.getAdminExtGroup(), "set/get round-trip");
    // setIdempotent: setting the same value twice must not disturb anything else.
    admin.setAdminExtGroup("custom-admin-group");
    checkStr("custom-admin-group", admin.getAdminExtGroup(), "re-setting the same value is stable");
}

}  // namespace

int main() {
    testConsumeFromWhereHasAllSixJavaValues();
    testLitePullBrokerPin();
    testAdminExtGroup();

    if (fails == 0) {
        std::printf("ALL TESTS PASSED (%d checks)\n", checks);
        return 0;
    }
    std::printf("TESTS FAILED: %d / %d\n", fails, checks);
    return 1;
}
