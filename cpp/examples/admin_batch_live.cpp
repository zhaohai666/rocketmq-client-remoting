// Integration of the C++ **batch** admin methods against a **real cluster**.
//
//
// This program does not start a cluster itself; run it wrapped by scripts/with_cluster.sh. Usage:
//   ./rmq_admin_batch_live [namesrv_addr]
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/subscription.h"

using namespace rocketmq;

namespace {

std::string gNamesrv = "127.0.0.1:9876";
int gPass = 0;
int gFail = 0;
int gSkip = 0;

void check(const std::string& name, bool ok, const std::string& detail = "") {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name;
    if (!detail.empty()) std::cout << "  " << detail;
    std::cout << std::endl;
}

// Records the items that are implementation-capability differences and do not hold locally; they are not counted as failures, and a skip must state the reason clearly.
void skip(const std::string& name, const std::string& detail) {
    ++gSkip;
    std::cout << "[SKIP] " << name << "  " << detail << std::endl;
}

int report() {
    std::cout << "\nPASS=" << gPass << " FAIL=" << gFail << " SKIP=" << gSkip << std::endl;
    return gFail == 0 ? 0 : 1;
}

std::string i2s(int64_t v) { return std::to_string(v); }

}  // namespace

int main(int argc, char** argv) {
    if (argc > 1) gNamesrv = argv[1];

    const int64_t stampMs = UtilAll::currentTimeMillis();
    const std::string stamp = i2s(stampMs);
    const std::string topicA = "CppBatchA_" + stamp;
    const std::string topicB = "CppBatchB_" + stamp;
    const std::string groupA = "GID_CppBatchA_" + stamp;
    const std::string groupB = "GID_CppBatchB_" + stamp;
    const std::string orderKey = "CppOrder_" + stamp;

    DefaultMQAdminExt admin;
    admin.setNamesrvAddr(gNamesrv);
    admin.setTimeoutMillis(10000);
    try {
        admin.start();
    } catch (const std::exception& e) {
        std::cout << "[FATAL] admin start failed: " << e.what() << std::endl;
        return 1;
    }

    // ---------- 0. Cluster liveness (the broker registration race: an open port != registered) ----------
    ClusterInfo cluster;
    bool clusterOk = false;
    std::string lastErr;
    for (int i = 0; i < 40; ++i) {
        try {
            cluster = admin.fetchBrokerClusterInfo();
            if (!cluster.getBrokerAddrs().empty()) {
                clusterOk = true;
                break;
            }
        } catch (const std::exception& e) {
            lastErr = e.what();
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!clusterOk) {
        check("cluster liveness", false, "no broker registered in the name server last_err=" + lastErr);
        admin.shutdown();
        return report();
    }
    const std::string brokerAddr = cluster.getBrokerAddrs()[0];
    std::cout << "cluster: master=" << brokerAddr << "\n" << std::endl;

    // ---- 1. Batch topic creation (18): body={"topicConfigList":[...]}, an empty header ----
    TopicConfig cfgA(topicA);
    cfgA.readQueueNums = 4;
    cfgA.writeQueueNums = 4;
    TopicConfig cfgB(topicB);
    cfgB.readQueueNums = 6;
    cfgB.writeQueueNums = 6;
    try {
        admin.createAndUpdateTopicConfigList(brokerAddr, {cfgA, cfgB});
        check("batch topic creation (18) request", true);
    } catch (const std::exception& e) {
        check("batch topic creation (18) request", false, e.what());
    }

    // Effect check: both topics exist and each keeps the queue counts of the batch request -- the proof that the broker really
    // accepted the body (with a wrong key name the broker receives an empty list, so the request "succeeds" while nothing is created).
    {
        bool ok = true;
        std::string detail;
        for (const TopicConfig* cfg : {&cfgA, &cfgB}) {
            try {
                TopicConfig got = admin.examineTopicConfig(brokerAddr, cfg->topicName);
                if (got.readQueueNums != cfg->readQueueNums) {
                    ok = false;
                    detail = cfg->topicName + " readQueueNums=" + i2s(got.readQueueNums)
                             + " want=" + i2s(cfg->readQueueNums);
                    break;
                }
            } catch (const std::exception& e) {
                ok = false;
                detail = cfg->topicName + ": " + e.what();
                break;
            }
        }
        check("batch topic creation (18) took effect with the right queue counts", ok, detail);
    }

    // ---- 2. Batch subscription-group creation (225): the same shape, but the body key is groupConfigList ----
    SubscriptionGroupConfig grpA(groupA);
    grpA.retryQueueNums = 3;
    SubscriptionGroupConfig grpB(groupB);
    grpB.retryMaxTimes = 5;
    try {
        admin.createAndUpdateSubscriptionGroupConfigList(brokerAddr, {grpA, grpB});
        check("batch subscription-group creation (225) request", true);
    } catch (const std::exception& e) {
        check("batch subscription-group creation (225) request", false, e.what());
    }

    {
        bool ok = true;
        std::string detail;
        const std::pair<const SubscriptionGroupConfig*, const char*> cases[2] = {
            {&grpA, "retryQueueNums"}, {&grpB, "retryMaxTimes"}};
        for (const auto& c : cases) {
            SubscriptionGroupConfig got;
            if (!admin.getSubscriptionGroupConfig(brokerAddr, c.first->groupName, got)) {
                ok = false;
                detail = c.first->groupName + ": not found";
                break;
            }
            int32_t actual = (c.second == std::string("retryQueueNums")) ? got.retryQueueNums
                                                                         : got.retryMaxTimes;
            int32_t want = (c.second == std::string("retryQueueNums")) ? 3 : 5;
            if (actual != want) {
                ok = false;
                detail = c.first->groupName + " " + c.second + "=" + i2s(actual) + " want="
                         + i2s(want);
                break;
            }
        }
        check("batch subscription-group creation (225) took effect with the right fields", ok, detail);
    }

    // ---- 3. The read-forbidden config (353) full chain: forbid reading -> query only without changes -> restore readability -> response fields ----
    try {
        GroupForbidden fb = admin.updateAndGetGroupReadForbidden(brokerAddr, groupA, topicA, false);
        check("consumer group read-forbidden config (353) forbids reading", !fb.readable,
              "readable=" + std::string(fb.readable ? "true" : "false"));
    } catch (const std::exception& e) {
        check("consumer group read-forbidden config (353) forbids reading", false, e.what());
    }

    try {
        // Query only (no readable supplied): it must not flip the forbidden state back
        GroupForbidden fb = admin.updateAndGetGroupReadForbidden(brokerAddr, groupA, topicA);
        check("consumer group read-forbidden config (353) queries without changing anything", !fb.readable,
              "the query call changed the forbidden state: readable=" + std::string(fb.readable ? "true" : "false"));
    } catch (const std::exception& e) {
        check("consumer group read-forbidden config (353) queries without changing anything", false, e.what());
    }

    try {
        GroupForbidden fb = admin.updateAndGetGroupReadForbidden(brokerAddr, groupA, topicA, true);
        check("consumer group read-forbidden config (353) restores readability", fb.readable,
              "readable=" + std::string(fb.readable ? "true" : "false"));
    } catch (const std::exception& e) {
        check("consumer group read-forbidden config (353) restores readability", false, e.what());
    }

    try {
        GroupForbidden fb = admin.updateAndGetGroupReadForbidden(brokerAddr, groupA, topicA);
        check("consumer group read-forbidden config (353) carries group/topic in the response",
              fb.group == groupA && fb.topic == topicA,
              "group=" + fb.group + " topic=" + fb.topic);
    } catch (const std::exception& e) {
        check("consumer group read-forbidden config (353) carries group/topic in the response", false, e.what());
    }

    try {
        bool resumed = admin.resumeCheckHalfMessage(
            brokerAddr, topicA, "0A0F0000000000000000000000000000000000");
        check("half-message resume (323) returns false for a non-half message instead of raising", !resumed,
              std::string("resumed=") + (resumed ? "true, and it even returns true for a non-half message" : "false"));
    } catch (const std::exception& e) {
        check("half-message resume (323) returns false for a non-half message instead of raising", false, e.what());
    }

    try {
        admin.resumeCheckHalfMessage(brokerAddr, "", "x");
        check("half-message resume (323) rejects a missing topic locally", false, "an empty topic was let through and sent");
    } catch (const MQClientException&) {
        check("half-message resume (323) rejects a missing topic locally", true);
    } catch (const std::exception& e) {
        check("half-message resume (323) rejects a missing topic locally", false,
              std::string("it should throw MQClientException, actually ") + typeid(e).name() + ": " + e.what());
    }

    // ---- 5. Ordering topic config (name server KV, read-modify-write outside cluster mode) ------------
    // Outside cluster mode the entries merge into a ";"-joined table: writing two topics under one key must keep both.
    try {
        admin.createOrUpdateOrderConf(orderKey, topicA + ":5", false);
        admin.createOrUpdateOrderConf(orderKey, topicB + ":8", false);
        std::string stored;
        bool found = admin.getKvConfig(MixAll::NAMESPACE_ORDER_TOPIC_CONFIG, orderKey, stored);
        check("ordering topic config merges the two entries instead of overwriting",
              found && stored.find(topicA + ":5") != std::string::npos
                  && stored.find(topicB + ":8") != std::string::npos,
              "stored=" + stored);
    } catch (const std::exception& e) {
        check("ordering topic config merges the two entries instead of overwriting", false, e.what());
    }

    try {
        admin.createOrUpdateOrderConf(orderKey, topicA + ":6", false);
        std::string stored;
        bool found = admin.getKvConfig(MixAll::NAMESPACE_ORDER_TOPIC_CONFIG, orderKey, stored);
        bool replaced = found && stored.find(topicA + ":6") != std::string::npos
                        && stored.find(topicA + ":5") == std::string::npos;
        check("ordering topic config overwrites the old value under the same key", replaced, "stored=" + stored);
    } catch (const std::exception& e) {
        check("ordering topic config overwrites the old value under the same key", false, e.what());
    }

    // ---- 6. The cleanup family ------------------------------------------------------
    // A safe probe with a huge window (1 year): the request must be accepted and nothing may be deleted.
    try {
        admin.cleanExpiredConsumerQueue(brokerAddr, 24 * 365);
        check("clean expired consume queues (306)", true);
    } catch (const std::exception& e) {
        check("clean expired consume queues (306)", false, e.what());
    }

    try {
        std::vector<std::string> failed =
            admin.cleanExpiredConsumerQueueByAddr({brokerAddr}, 24 * 365);
        check("clean expired consume queues (306) ByAddr", failed.empty(),
              "failed=" + (failed.empty() ? std::string("[]") : failed[0]));
    } catch (const std::exception& e) {
        check("clean expired consume queues (306) ByAddr", false, e.what());
    }

    try {
        admin.deleteExpiredCommitLog(brokerAddr, 24 * 365);
        check("delete expired commitlog (329)", true);
    } catch (const std::exception& e) {
        check("delete expired commitlog (329)", false, e.what());
    }

    try {
        std::vector<std::string> failed = admin.deleteExpiredCommitLogByAddr({brokerAddr}, 24 * 365);
        check("delete expired commitlog (329) ByAddr", failed.empty(),
              "failed=" + (failed.empty() ? std::string("[]") : failed[0]));
    } catch (const std::exception& e) {
        check("delete expired commitlog (329) ByAddr", false, e.what());
    }

    // A dead address must be reported rather than silently swallowed
    try {
        std::vector<std::string> failed = admin.deleteExpiredCommitLogByAddr({"127.0.0.1:1"}, 1);
        check("the cleanup ByAddr methods report the failed address",
              failed.size() == 1 && failed[0] == "127.0.0.1:1",
              "dead addr should be reported");
    } catch (const std::exception& e) {
        check("the cleanup ByAddr methods report the failed address", false, e.what());
    }

    // ---- 7. Clean unused topics (316): a single request, PASS as soon as the broker accepts it ----
    try {
        admin.cleanUnusedTopicByAddr(brokerAddr);
        check("clean unused topics (316)", true);
    } catch (const std::exception& e) {
        check("clean unused topics (316)", false, e.what());
    }

    // ---- 8. Consume time span (303): fanned out over the route and aggregated, returning a consumeTimeSpanSet array ----
    try {
        JsonValue spans = admin.queryConsumeTimeSpan(topicA, groupA);
        // An empty group (never consumed) legitimately returns an empty array: only assert that the shape is right and the request reaches the whole chain
        check("consume time span (303) route fan-out aggregation", spans.isArray(),
              "spans=" + (spans.isArray() ? i2s(static_cast<int64_t>(spans.size()))
                                          : std::string("not an array")));
    } catch (const std::exception& e) {
        check("consume time span (303) route fan-out aggregation", false, e.what());
    }

    // ---- 9. Name server config (318/319) write-read round trip ---------------------------
    // The name server's Configuration.update only accepts real fields (an unknown key is silently dropped),
    // so the round trip uses the real field orderMessageEnable and restores it once done.
    try {
        std::string oldVal = "false";
        for (const auto& kv : admin.getNameServerConfig()) {
            auto it = kv.second.find("orderMessageEnable");
            if (it != kv.second.end()) oldVal = it->second;
        }
        PropertyMap upd;
        upd["orderMessageEnable"] = "true";
        admin.updateNameServerConfig(upd);
        std::string val;
        for (const auto& kv : admin.getNameServerConfig()) {
            auto it = kv.second.find("orderMessageEnable");
            if (it != kv.second.end()) val = it->second;
        }
        // Restore first no matter what the assertion says, so the cluster state is not polluted
        PropertyMap back;
        back["orderMessageEnable"] = oldVal;
        admin.updateNameServerConfig(back);
        check("name server config (318/319) write-read round trip", val == "true", "got=" + val + " want='true'");
    } catch (const std::exception& e) {
        check("name server config (318/319) write-read round trip", false, e.what());
    }

    // ---- 10. Static topic (513): an implementation capability difference, SKIP ----
    skip("static topic (513)",
         "building the mapping needs a full TopicQueueMappingDetail document (hostedBrokerIds/scope/"
         "per-queue mappingInfo); this tool has no real-cluster coverage, and the header wiring is locked by unit tests");

    // ---- Cleanup -----------------------------------------------------------
    for (const std::string& t : {topicA, topicB}) {
        try {
            admin.deleteTopicInBroker(brokerAddr, t);
        } catch (const std::exception&) {
            // a cleanup failure does not affect the conclusion
        }
    }
    try {
        admin.deleteKvConfig(MixAll::NAMESPACE_ORDER_TOPIC_CONFIG, orderKey);
    } catch (const std::exception&) {
    }

    admin.shutdown();
    return report();
}
