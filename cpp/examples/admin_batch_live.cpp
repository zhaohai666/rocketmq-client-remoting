// C++ 批量 admin 方法的**真实集群**联调（对标 python/verify_admin_batch_live.py，
// 与 go/examples/live_admin_batch 同口径）。
//
// 这些方法此前只存在于 RequestCode 常量层：body 键名写错、header 字段缺失这类
// 编码错误 mock 层看不见，必须打真实 broker。覆盖（全部真实 nameServer + broker）：
//   1. UPDATE_AND_CREATE_TOPIC_LIST(18)：body={"topicConfigList":[...]}，header 为空
//   2. UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST(225)：body={"groupConfigList":[...]}
//   3. UPDATE_AND_GET_GROUP_FORBIDDEN(353)：禁读→仅查询不改动→恢复可读→回包字段
//   4. RESUME_CHECK_HALF_MESSAGE(323)：非半消息返回 false 而非报错（Java 语义）
//   5. createOrUpdateOrderConf：nameserver KV 的合并/覆盖语义
//   6. CLEAN_EXPIRED_CONSUMEQUEUE(306) / DELETE_EXPIRED_COMMITLOG(329) + ByAddr + 死地址
//   7. CLEAN_UNUSED_TOPIC(316)：broker 接受即可（清理策略是 broker 侧的）
//   8. QUERY_CONSUME_TIME_SPAN(303)：路由扇出聚合
//   9. UPDATE/GET_NAMESRV_CONFIG(318/319)：properties 文本 + 写读回环（可逆）
//
// 本程序自身不启动集群；用 scripts/with_cluster.sh 包一层跑。用法：
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

// 记录"端能力差异、本地不成立"的项，不计入失败；skip 必须写清楚原因。
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

    // ---------- 0. 集群探活（broker 注册竞态：端口开 != 已注册）----------
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
        check("集群探活", false, "nameServer 无 broker 注册 last_err=" + lastErr);
        admin.shutdown();
        return report();
    }
    const std::string brokerAddr = cluster.getBrokerAddrs()[0];
    std::cout << "cluster: master=" << brokerAddr << "\n" << std::endl;

    // ---- 1. 批量建 topic(18)：body={"topicConfigList":[...]}，header 为空 ----
    TopicConfig cfgA(topicA);
    cfgA.readQueueNums = 4;
    cfgA.writeQueueNums = 4;
    TopicConfig cfgB(topicB);
    cfgB.readQueueNums = 6;
    cfgB.writeQueueNums = 6;
    try {
        admin.createAndUpdateTopicConfigList(brokerAddr, {cfgA, cfgB});
        check("批量建 topic(18) 请求", true);
    } catch (const std::exception& e) {
        check("批量建 topic(18) 请求", false, e.what());
    }

    // 生效校验：两个 topic 都存在，且各自保住批量请求的队列数 —— 这是 body 真被
    // broker 认下的证据（键名写错时 broker 收到空列表，请求"成功"但什么都不建）。
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
        check("批量建 topic(18) 生效且队列数正确", ok, detail);
    }

    // ---- 2. 批量建订阅组(225)：同形，但 body key 是 groupConfigList ----
    SubscriptionGroupConfig grpA(groupA);
    grpA.retryQueueNums = 3;
    SubscriptionGroupConfig grpB(groupB);
    grpB.retryMaxTimes = 5;
    try {
        admin.createAndUpdateSubscriptionGroupConfigList(brokerAddr, {grpA, grpB});
        check("批量建订阅组(225) 请求", true);
    } catch (const std::exception& e) {
        check("批量建订阅组(225) 请求", false, e.what());
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
        check("批量建订阅组(225) 生效且字段正确", ok, detail);
    }

    // ---- 3. 读禁配(353) 全链：禁读 → 仅查询不改动 → 恢复可读 → 回包字段 ----
    try {
        GroupForbidden fb = admin.updateAndGetGroupReadForbidden(brokerAddr, groupA, topicA, false);
        check("消费组读禁配(353) 设置禁读", !fb.readable,
              "readable=" + std::string(fb.readable ? "true" : "false"));
    } catch (const std::exception& e) {
        check("消费组读禁配(353) 设置禁读", false, e.what());
    }

    try {
        // 仅查询（readable 不带）：不能把禁读状态翻回去
        GroupForbidden fb = admin.updateAndGetGroupReadForbidden(brokerAddr, groupA, topicA);
        check("消费组读禁配(353) 仅查询不改动", !fb.readable,
              "禁读状态被查询调用改掉: readable=" + std::string(fb.readable ? "true" : "false"));
    } catch (const std::exception& e) {
        check("消费组读禁配(353) 仅查询不改动", false, e.what());
    }

    bool readableRestored = false;
    try {
        GroupForbidden fb = admin.updateAndGetGroupReadForbidden(brokerAddr, groupA, topicA, true);
        readableRestored = fb.readable;
        check("消费组读禁配(353) 恢复可读", fb.readable,
              "readable=" + std::string(fb.readable ? "true" : "false"));
    } catch (const std::exception& e) {
        check("消费组读禁配(353) 恢复可读", false, e.what());
    }

    try {
        GroupForbidden fb = admin.updateAndGetGroupReadForbidden(brokerAddr, groupA, topicA);
        check("消费组读禁配(353) 回包含 group/topic",
              fb.group == groupA && fb.topic == topicA,
              "group=" + fb.group + " topic=" + fb.topic);
    } catch (const std::exception& e) {
        check("消费组读禁配(353) 回包含 group/topic", false, e.what());
    }

    // ---- 4. 恢复半消息(323) ---------------------------------------------
    // Java（MQClientAPIImpl:3279）对非 SUCCESS 返回 false 而非抛错：对非半消息
    // （broker 表现为 SYSTEM_ERROR）的良构调用必须得到 false，而不是异常。
    try {
        bool resumed = admin.resumeCheckHalfMessage(
            brokerAddr, topicA, "0A0F0000000000000000000000000000000000");
        check("恢复半消息(323) 非半消息返回 false 而非报错", !resumed,
              std::string("resumed=") + (resumed ? "true，对非半消息竟返回 true" : "false"));
    } catch (const std::exception& e) {
        check("恢复半消息(323) 非半消息返回 false 而非报错", false, e.what());
    }

    try {
        admin.resumeCheckHalfMessage(brokerAddr, "", "x");
        check("恢复半消息(323) 缺 topic 被本地拒绝", false, "空 topic 竟被放行发出");
    } catch (const MQClientException&) {
        check("恢复半消息(323) 缺 topic 被本地拒绝", true);
    } catch (const std::exception& e) {
        check("恢复半消息(323) 缺 topic 被本地拒绝", false,
              std::string("应抛 MQClientException，实际 ") + typeid(e).name() + ": " + e.what());
    }

    // ---- 5. 顺序 topic 配置（nameserver KV，非集群模式读改写）------------
    // 非集群模式合并进 ";" 连接的条目表：同 key 写两个 topic 必须两条都在。
    try {
        admin.createOrUpdateOrderConf(orderKey, topicA + ":5", false);
        admin.createOrUpdateOrderConf(orderKey, topicB + ":8", false);
        std::string stored;
        bool found = admin.getKvConfig(MixAll::NAMESPACE_ORDER_TOPIC_CONFIG, orderKey, stored);
        check("顺序 topic 配置 合并两条而非覆盖",
              found && stored.find(topicA + ":5") != std::string::npos
                  && stored.find(topicB + ":8") != std::string::npos,
              "stored=" + stored);
    } catch (const std::exception& e) {
        check("顺序 topic 配置 合并两条而非覆盖", false, e.what());
    }

    try {
        admin.createOrUpdateOrderConf(orderKey, topicA + ":6", false);
        std::string stored;
        bool found = admin.getKvConfig(MixAll::NAMESPACE_ORDER_TOPIC_CONFIG, orderKey, stored);
        bool replaced = found && stored.find(topicA + ":6") != std::string::npos
                        && stored.find(topicA + ":5") == std::string::npos;
        check("顺序 topic 配置 同 key 覆盖旧值", replaced, "stored=" + stored);
    } catch (const std::exception& e) {
        check("顺序 topic 配置 同 key 覆盖旧值", false, e.what());
    }

    // ---- 6. 清理类 ------------------------------------------------------
    // 大窗口（1 年）的安全探针：请求必须被接受且什么都不删。
    try {
        admin.cleanExpiredConsumerQueue(brokerAddr, 24 * 365);
        check("清理过期消费队列(306)", true);
    } catch (const std::exception& e) {
        check("清理过期消费队列(306)", false, e.what());
    }

    try {
        std::vector<std::string> failed =
            admin.cleanExpiredConsumerQueueByAddr({brokerAddr}, 24 * 365);
        check("清理过期消费队列(306) ByAddr", failed.empty(),
              "failed=" + (failed.empty() ? std::string("[]") : failed[0]));
    } catch (const std::exception& e) {
        check("清理过期消费队列(306) ByAddr", false, e.what());
    }

    try {
        admin.deleteExpiredCommitLog(brokerAddr, 24 * 365);
        check("删除过期 commitlog(329)", true);
    } catch (const std::exception& e) {
        check("删除过期 commitlog(329)", false, e.what());
    }

    try {
        std::vector<std::string> failed = admin.deleteExpiredCommitLogByAddr({brokerAddr}, 24 * 365);
        check("删除过期 commitlog(329) ByAddr", failed.empty(),
              "failed=" + (failed.empty() ? std::string("[]") : failed[0]));
    } catch (const std::exception& e) {
        check("删除过期 commitlog(329) ByAddr", false, e.what());
    }

    // 死地址必须被报告而不是被静默吞掉
    try {
        std::vector<std::string> failed = admin.deleteExpiredCommitLogByAddr({"127.0.0.1:1"}, 1);
        check("清理类 ByAddr 报告失败地址",
              failed.size() == 1 && failed[0] == "127.0.0.1:1",
              "dead addr should be reported");
    } catch (const std::exception& e) {
        check("清理类 ByAddr 报告失败地址", false, e.what());
    }

    // ---- 7. 清理未使用 topic(316)：单请求，broker 接受即 PASS ----
    try {
        admin.cleanUnusedTopicByAddr(brokerAddr);
        check("清理未使用 topic(316)", true);
    } catch (const std::exception& e) {
        check("清理未使用 topic(316)", false, e.what());
    }

    // ---- 8. 消费时间跨度(303)：路由扇出聚合，返回 consumeTimeSpanSet 数组 ----
    try {
        JsonValue spans = admin.queryConsumeTimeSpan(topicA, groupA);
        // 空组（从未消费）返回空数组属正常：只断言形态正确、请求全链可达
        check("消费时间跨度(303) 路由扇出聚合", spans.isArray(),
              "spans=" + (spans.isArray() ? i2s(static_cast<int64_t>(spans.size()))
                                          : std::string("非数组")));
    } catch (const std::exception& e) {
        check("消费时间跨度(303) 路由扇出聚合", false, e.what());
    }

    // ---- 9. nameserver 配置(318/319) 写读回环 ---------------------------
    // namesrv 的 Configuration.update 只认真实字段（未知 key 被静默丢弃），
    // 所以用 orderMessageEnable 这个真实字段做回环，测完还原。
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
        // 无论断言结果如何先还原，避免污染集群状态
        PropertyMap back;
        back["orderMessageEnable"] = oldVal;
        admin.updateNameServerConfig(back);
        check("nameserver 配置(318/319) 写读回环", val == "true", "got=" + val + " want='true'");
    } catch (const std::exception& e) {
        check("nameserver 配置(318/319) 写读回环", false, e.what());
    }

    // ---- 10. 静态 topic(513)：端能力差异，SKIP ----
    skip("静态 topic(513)",
         "mapping 构造需完整 TopicQueueMappingDetail 文档（hostedBrokerIds/scope/"
         "每队列 mappingInfo），Python/Go 参考断言集均未覆盖；header 布线由单测锁定");

    // ---- 清理 -----------------------------------------------------------
    for (const std::string& t : {topicA, topicB}) {
        try {
            admin.deleteTopicInBroker(brokerAddr, t);
        } catch (const std::exception&) {
            // 清理失败不影响结论
        }
    }
    try {
        admin.deleteKvConfig(MixAll::NAMESPACE_ORDER_TOPIC_CONFIG, orderKey);
    } catch (const std::exception&) {
    }

    admin.shutdown();
    return report();
}
