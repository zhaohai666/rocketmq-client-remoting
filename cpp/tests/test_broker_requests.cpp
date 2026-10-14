// Offline unit test for the broker-pushed NOTIFY_CONSUMER_IDS_CHANGED(40) -- no cluster needed.
//
// Why its own file: these requests come back at the client over the **already established**
// long connection, so a live test cannot inject them (the live tools only send requests outward).
// What can be pinned offline must be pinned first, or a "handler never registered" kind of bug
// would only show in production as "members changed, wait 20s for the periodic rebalance", silent.
//
//
// Covered:
//   - counting plus waking the whole group (two consumers registered, both are named)
//   - after unregisterRebalanceWakeup no more wakeups, but the notification is still handled
//   - a missing consumerGroup does not throw and is still counted
//   - instance shutdown clears the wakeup table (a callback capturing this must not outlive it dangling)
#include <cstdio>
#include <string>

#include "rocketmq/client/mq_client.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

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

/// The broker sends a oneway notification whose extFields carry only consumerGroup.
RemotingCommand notifyCmd(const std::string& group) {
    RemotingCommand cmd = RemotingCommand::createRequestCommand(
        RequestCode::NOTIFY_CONSUMER_IDS_CHANGED);
    if (!group.empty()) cmd.extFields["consumerGroup"] = group;
    return cmd;
}

void testFanOut() {
    MQClientInstance instance("cpp-unit-40@fanout", {"127.0.0.1:9876"});
    int wakeA = 0;
    int wakeB = 0;
    instance.registerRebalanceWakeup("GID_A", [&wakeA] { ++wakeA; });
    instance.registerRebalanceWakeup("GID_B", [&wakeB] { ++wakeB; });

    instance.processNotifyConsumerIdsChanged(notifyCmd("GID_A"), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 1, "notify.counted");
    expect(wakeA == 1, "notify.wakesFirst", "wakeA=" + std::to_string(wakeA));
    expect(wakeB == 1, "notify.wakesEveryConsumer", "wakeB=" + std::to_string(wakeB));

    instance.processNotifyConsumerIdsChanged(notifyCmd(""), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 2, "notify.blankGroupStillCounts");
    expect(wakeA == 2 && wakeB == 2, "notify.blankGroupWakesAll",
           "wakeA=" + std::to_string(wakeA) + " wakeB=" + std::to_string(wakeB));

    // The deregistered group is no longer woken, but the notification itself is still handled.
    instance.unregisterRebalanceWakeup("GID_B");
    instance.processNotifyConsumerIdsChanged(notifyCmd("GID_A"), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 3, "notify.countsAfterUnregister");
    expect(wakeA == 3, "notify.stillWakesRegistered", "wakeA=" + std::to_string(wakeA));
    expect(wakeB == 2, "notify.unregisteredNotWoken", "wakeB=" + std::to_string(wakeB));

    // An empty table must not blow up: with no consumer registered the notice is still consumed.
    instance.unregisterRebalanceWakeup("GID_A");
    instance.rebalanceImmediately();
    expect(instance.consumerIdsChangedCount() == 3, "notify.emptyTableIsNoop");
}

void testShutdownClearsWakeups() {
    MQClientInstance instance("cpp-unit-40@shutdown", {"127.0.0.1:9876"});
    int woken = 0;
    instance.registerRebalanceWakeup("GID_A", [&woken] { ++woken; });
    instance.shutdown();
    instance.rebalanceImmediately();
    expect(woken == 0, "shutdown.clearsWakeups", "woken=" + std::to_string(woken));
    // A notification arriving after shutdown (the read thread may still be draining) must not throw.
    instance.processNotifyConsumerIdsChanged(notifyCmd("GID_A"), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 1, "notify.afterShutdownStillCounts");
}

}  // namespace

int main() {
    testFanOut();
    testShutdownClearsWakeups();
    std::printf("broker requests: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
