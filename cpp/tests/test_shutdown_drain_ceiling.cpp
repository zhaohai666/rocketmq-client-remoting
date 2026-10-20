// Offline unit tests for the shutdown drain ceiling (awaitTerminationMillisWhenShutdown).
//
// The drain ceiling is a real, existing wait in this port: DefaultMQPushConsumer::shutdown retires the
// consume executor with shutdown(true, awaitTerminationMillisWhenShutdown_), and the executor walks its
// backlog applying that ceiling between batches. Two things are pinned here:
//   - the knob itself (default 0, round-trips through setter/getter, and 0-or-negative keeps the historic
//     unlimited drain -- the same way the ceiling "means nothing" is documented);
//   - the executor honouring the ceiling: a bounded backlog drains fully with no ceiling, but a positive
//     ceiling stops mid-backlog so batches are dropped, and every batch is accounted for exactly once.
//
// No fixed test sleep gates the assertions: they read the executor's own recorded counts, and the tasks
// carry enough real work that a one-millisecond ceiling is exceeded with a wide margin (hundreds of
// queued batches are left over), so "some batches were dropped" is a property of the construction, not a
// race. The control (no ceiling) drains the identical backlog to the end.
#include <atomic>
#include <cstdint>
#include <string>

#include "rocketmq/client/consume_executor.h"
#include "rocketmq/client/consumer.h"

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

// A batch with enough deterministic work (a volatile accumulator) that a large queue of them cannot
// clear inside a millisecond-scale ceiling -- the drain genuinely runs past the deadline.
void busy(std::atomic<int64_t>& ran) {
    volatile int64_t sink = 0;
    for (int i = 0; i < 200000; i++) {
        sink += i;
    }
    ran.fetch_add(1);
}

// ---------------------------------------------------------------- consumer-side knob plumbing

void testConsumerKnob() {
    DefaultMQPushConsumer consumer("GID_ShutdownDrainKnob");
    expectInt(consumer.awaitTerminationMillisWhenShutdown(), 0, "defaultKeepsHistoricBudget");
    expectInt(consumer.shutdownDroppedBatches(), 0, "nothingDroppedBeforeShutdown");

    consumer.setAwaitTerminationMillisWhenShutdown(2500);
    expectInt(consumer.awaitTerminationMillisWhenShutdown(), 2500, "positiveRoundTrips");

    // A non-positive value is a valid setting meaning "no ceiling" -- it is stored, not rejected.
    consumer.setAwaitTerminationMillisWhenShutdown(-1);
    expectInt(consumer.awaitTerminationMillisWhenShutdown(), -1, "negativeStoredAsNoCeiling");
}

// ---------------------------------------------------------------- executor drain: no ceiling drains all

void testDrainWithoutCeilingRunsEverything() {
    std::atomic<int64_t> ran{0};
    ConsumeExecutor ex(1, 1);
    const int kBatches = 400;
    for (int i = 0; i < kBatches; i++) {
        ex.submit([&ran]() { busy(ran); });
    }
    // awaitTerminationMillis <= 0 keeps the historic budget: the whole backlog is handed to the listener.
    ex.shutdown(true, 0);
    expectInt(ran.load(), kBatches, "noCeiling.allBatchesRan");
    expectInt(ex.droppedTaskCount(), 0, "noCeiling.nothingDropped");
}

void testGenerousCeilingAlsoRunsEverything() {
    // Paired against the tight-ceiling case: a ceiling far larger than the backlog behaves like none --
    // so the drop is caused by the ceiling expiring, not by the drain merely being asked to bound.
    std::atomic<int64_t> ran{0};
    ConsumeExecutor ex(1, 1);
    const int kBatches = 200;
    for (int i = 0; i < kBatches; i++) {
        ex.submit([&ran]() { busy(ran); });
    }
    ex.shutdown(true, 600000);
    expectInt(ran.load(), kBatches, "generousCeiling.allBatchesRan");
    expectInt(ex.droppedTaskCount(), 0, "generousCeiling.nothingDropped");
}

void testTightCeilingDropsBacklog() {
    std::atomic<int64_t> ran{0};
    ConsumeExecutor ex(1, 1);
    const int kBatches = 2000;
    for (int i = 0; i < kBatches; i++) {
        ex.submit([&ran]() { busy(ran); });
    }
    // A one-millisecond ceiling over ~2000 busy batches: the drain is stopped with the backlog intact.
    ex.shutdown(true, 1);
    const int64_t executed = ran.load();
    const int64_t dropped = ex.droppedTaskCount();
    expect(executed > 0, "tightCeiling.startedDraining", std::to_string(executed));
    expect(dropped > 0, "tightCeiling.droppedRemainder", std::to_string(dropped));
    expect(executed < kBatches, "tightCeiling.didNotFinishAll", std::to_string(executed));
    // Every batch is accounted for exactly once: it either ran or was counted as dropped.
    expectInt(executed + dropped, kBatches, "tightCeiling.executionIsConserve");
}

}  // namespace

int main() {
    testConsumerKnob();
    testDrainWithoutCeilingRunsEverything();
    testGenerousCeilingAlsoRunsEverything();
    testTightCeilingDropsBacklog();
    std::printf("shutdown drain ceiling: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
