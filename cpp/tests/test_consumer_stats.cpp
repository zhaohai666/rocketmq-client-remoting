// Unit tests for ConsumerStatsManager + ConsumeStatus/ConsumerRunningInfo(307) encoding.
// The sampling chain does not rely on real time windows (sum/tps use window-endpoint diffs; tests feed sample points directly).
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "rocketmq/client/consumer_stats.h"
#include "rocketmq/common/util_all.h"

using namespace rocketmq;

namespace {
int g_checks = 0;
void expectEq(const std::string& got, const std::string& want, const char* what) {
    ++g_checks;
    if (got != want) {
        std::printf("FAIL %s: got [%s], want [%s]\n", what, got.c_str(), want.c_str());
        std::exit(1);
    }
}
void expectTrue(bool cond, const char* what) {
    ++g_checks;
    if (!cond) {
        std::printf("FAIL %s\n", what);
        std::exit(1);
    }
}
void expectNear(double got, double want, double eps, const char* what) {
    ++g_checks;
    if (got < want - eps || got > want + eps) {
        std::printf("FAIL %s: got %.6f, want %.6f\n", what, got, want);
        std::exit(1);
    }
}

std::string bodyStr(const Bytes& b) { return std::string(b.begin(), b.end()); }
}  // namespace

void testComputeStatsData() {
    // Empty chain yields all zeros
    std::deque<std::tuple<int64_t, int64_t, int64_t>> empty;
    StatsSnapshot ss = computeStatsData(empty);
    expectEq(std::to_string(ss.sum), "0", "empty sum");
    expectEq(std::to_string(ss.times), "0", "empty times");

    // sum = last point - first point; tps = sum*1000/span; avgpt = sum/timesDiff
    std::deque<std::tuple<int64_t, int64_t, int64_t>> chain = {
        {1000, 0, 0}, {2000, 300, 10}, {3000, 900, 20}};
    ss = computeStatsData(chain);
    expectEq(std::to_string(ss.sum), "900", "sum diff");
    expectNear(ss.tps, 450.0, 1e-9, "tps per second");
    expectEq(std::to_string(ss.times), "20", "times diff");
    expectNear(ss.avgpt, 45.0, 1e-9, "avgpt");
}

void testStatsItemSampling() {
    StatsItem item("PULL_RT", "T@G");
    item.addValue(100, 2);
    item.sample();
    item.addValue(200, 2);
    item.sample();
    StatsSnapshot m = item.getStatsDataInMinute();
    expectEq(std::to_string(m.sum), "200", "minute window sum (differential)");
    expectEq(std::to_string(m.times), "2", "minute window times");
    // The hour chain samples independently, so the hour-window sum covers only hour points
    item.sampleHour();
    StatsSnapshot h = item.getStatsDataInHour();
    expectEq(std::to_string(h.sum), "0", "hour chain has single point -> 0");
    expectEq(std::to_string(h.times), "0", "hour chain times");
}

void testManagerConsumeStatus() {
    ConsumerStatsManager m;
    // Inject two sample points 1s apart directly (no dependency on the real clock)
    const int64_t now = UtilAll::currentTimeMillis();
    auto feed = [&](StatsItemSet& set, int64_t v, int64_t t) {
        set.getAndCreate("StatsTopic@GID_StatsUnit")
            ->appendSampleForTest(now - 1000, 0, 0);
        set.getAndCreate("StatsTopic@GID_StatsUnit")
            ->appendSampleForTest(now, v, t);
    };
    feed(m.topicAndGroupPullRT(), 50, 1);
    feed(m.topicAndGroupPullTPS(), 5, 1);
    feed(m.topicAndGroupConsumeRT(), 10, 1);
    feed(m.topicAndGroupConsumeOKTPS(), 8, 1);
    feed(m.topicAndGroupConsumeFailedTPS(), 3, 1);

    ConsumeStatus cs = m.consumeStatus("GID_StatsUnit", "StatsTopic");
    expectNear(cs.pullRT, 50.0, 1e-6, "pullRT = avgpt");
    expectNear(cs.pullTPS, 5.0, 1e-6, "pullTPS = calls/sec");
    expectNear(cs.consumeRT, 10.0, 1e-6, "consumeRT = avgpt");
    expectNear(cs.consumeOKTPS, 8.0, 1e-6, "consumeOKTPS");
    expectNear(cs.consumeFailedTPS, 3.0, 1e-6, "consumeFailedTPS");
    // An unrecorded key returns all zeros
    ConsumeStatus zero = m.consumeStatus("GID_StatsUnit", "NoTopic");
    expectNear(zero.pullRT, 0.0, 1e-9, "unknown topic pullRT 0");
}

void testStartShutdown() {
    // Lifecycle: after start the sampling thread runs; shutdown exits cleanly (no hang).
    // Regression guard: shutdown once failed to wake a sampling thread parked in a 10s
    // wait_for, so every producer/consumer shutdown stalled for a full sampling period.
    ConsumerStatsManager m;
    m.start();
    m.incPullRT("G", "T", 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const auto begin = std::chrono::steady_clock::now();
    m.shutdown();
    const long long ms = static_cast<long long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin)
            .count());
    if (ms >= 1000) std::printf("  shutdown blocked %lldms (sampling thread not woken)\n", ms);
    expectTrue(ms < 1000, "shutdown wakes the sampling thread immediately");
    m.shutdown();  // idempotent
}

void testConsumeStatusJson() {
    ConsumeStatus cs;
    cs.pullRT = 1.5;
    cs.pullTPS = 2.0;
    cs.consumeRT = 3.0;
    cs.consumeOKTPS = 4.0;
    cs.consumeFailedTPS = 5.0;
    cs.consumeFailedMsgs = 6;
    JsonValue v = cs.toJson();
    ConsumeStatus back = ConsumeStatus::fromJson(v);
    expectNear(back.pullRT, 1.5, 1e-9, "consumeStatus json roundtrip pullRT");
    expectEq(std::to_string(back.consumeFailedMsgs), "6", "consumeFailedMsgs roundtrip");

    // ConsumerRunningInfo: statusTable/mqPopTable/userConsumerInfo default to empty objects
    ConsumerRunningInfo ri;
    ri.properties[ConsumerRunningInfo::PROP_CONSUME_TYPE] = "CONSUME_PASSIVELY";
    ri.subscriptionSet = JsonValue::makeArray();
    const std::string s = bodyStr(ri.encode());
    expectTrue(s.find("\"statusTable\":{}") != std::string::npos, "statusTable empty object");
    expectTrue(s.find("\"mqPopTable\":{}") != std::string::npos, "mqPopTable empty object");
    expectTrue(s.find("\"userConsumerInfo\":{}") != std::string::npos, "userConsumerInfo empty object");
    // roundtrip
    ConsumerRunningInfo riBack;
    expectTrue(ConsumerRunningInfo::decode(ri.encode(), riBack), "decode ok");
    expectTrue(riBack.statusTable.isObject(), "statusTable roundtrip");
}

int main() {
    testComputeStatsData();
    testStatsItemSampling();
    testManagerConsumeStatus();
    testStartShutdown();
    testConsumeStatusJson();
    std::printf("consumer_stats: %d checks passed\n", g_checks);
    return 0;
}
