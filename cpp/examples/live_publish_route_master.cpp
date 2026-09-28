// 发布路由必须跳过「没有 master 的 broker」真机验证（Java MQClientInstance:294-303），
// 以及同一条分界线的**地址侧**（#100：findBrokerAddressInPublish:1295-1305，发送只认主）。
//
// 与 `python/verify_publish_route_master_live.py`、`rust/examples/live_publish_route_master.rs`、
// `dotnet/examples/RocketMQ.Examples/LivePublishRouteMaster.cs` 同场景、逐条同断言。
//
// 前置：namesrv + master + slave 都在跑（按本地集群 runbook）；脚本自己**只停一次 master**
// （scripts/rmq_test_broker.sh 只认 master 的 java 进程），收尾块保证 master 一定回来。
//
// Java 依据：MQClientInstance.topicRouteData2TopicPublishInfo:294-303 组装发布信息时，brokerDatas
// 里没有同名 broker、或它的 brokerAddrs 没有 MASTER_ID，整条 QueueData 跳过。从节点自己也注册
// 进 namesrv，且默认配置下照样带写位（RouteInfoManager 只在「prime slave 且 enableActingMaster」
// 时才抹掉 WRITE，本机 broker.conf 是 false），所以 master 一掉线，路由里同一个 brokerName 只剩
// brokerId=1 —— 漏判这条，生产者就会把消息发到从节点上，而从节点对发送请求一律 reject
// （SendMessageProcessor ⇒ SYSTEM_BUSY(2)，**还是可重试码**），白烧重试。消费侧是另一份口径
// （topicRouteData2TopicSubscribeInfo:318-332：读位 + readQueueNums、**不要求有 master**），
// 停窗口内消费者仍要看得见队列、还得能从从节点拉。
//
// **地址侧**也是同一条分界线：Java sendKernelImpl:919-924 从 brokerAddrTable 里取
// brokerId=0（findBrokerAddressInPublish:1295-1305），拿不到按 topic 刷一次路由再查，
// 仍拿不到就本端抛 MQClientException("The broker[X] not exist") —— 定点发送（调用方给了 mq）
// 走的正是这条，**不会**退到从节点地址上让 broker 回 SYSTEM_BUSY(2) 白烧一轮。
//
// 离线单测：tests/test_route_heartbeat.cpp 的发布/订阅信息组锁「队列集判据」，
// tests/test_publish_route_master.cpp 锁「地址判据」；真机锁的是判据作用在真实路由形状上的
// 结果 —— 名字服务里 broker-a 真的只剩 {1: slave}。
//
// 场景（同一停窗口里做完）：
//   S0 控制腿（master 在）：路由 {0: master, 1: slave}；发布队列 4、订阅队列 4。
//   S1 预埋：每队列定点一条共 4 条，等从节点 store 追上（不然 S6 无从消费）。
//   S2 停 master → 刷新路由直到 broker-a 只剩 {1: slave}。
//   S3 (A) 发布队列 == 0；访问器本端抛「选不到队列」。
//   S4 (C) 订阅队列仍是 4（消费侧不看 master）。
//   S5 发送快速失败、报错里没有从节点地址（旧缓存腿打的是死掉的 master；周期刷新恰好已跑过
//      则是本端 10005）；再显式把发送实例刷成停后形状：(A) 生效 —— 发送本端 10005 且
//      **一条 wire 都不发**。
//   S5d (B) 对照：定点发到该队列 → 地址侧只认 master，本端同样立刻报
//       「The broker[broker-a] not exist」，也无 wire（漏掉 (B) 这条对照时的旧行为：
//       请求打到从节点上，broker 回 SYSTEM_BUSY(2)，一个可重试码 —— 白烧一整轮重试，
//       错误类型也和 Java 不一样）。两条腿合起来是「无 wire」，落库与否由 S7b 钉死。
//   S6 (C 端到端) 停窗口内新起的 push 消费者仍看到 4 条队列，并从**从节点**把 S1 的 4 条收齐。
//   S7 负控：master 拉回 → 发布队列恢复 4、两条失败发送都没在 broker 上留下消息、发送 SEND_OK。
//
// 用法：./rmq_live_publish_route_master 127.0.0.1:9876 [127.0.0.1:10911] [127.0.0.1:10931]
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::printf("  [%s] %s%s\n", ok ? "PASS" : "FAIL", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

double monotonicSeconds() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// 真机窗口里路由/位点的收敛是异步的：超时不算断言失败，由调用方对返回值做断言。
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 500) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

std::string trimRight(const std::string& s) {
    size_t end = s.find_last_not_of(" \t\r\n");
    return end == std::string::npos ? std::string() : s.substr(0, end + 1);
}

std::string readFile(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return trimRight(ss.str());
}

std::string gBrokerCtl;

/// 跑 scripts/rmq_test_broker.sh。输出走**文件**而不是管道：start 会把 broker 拉成
/// 常驻进程，谁继承它的写端谁就等不到 EOF。
bool brokerCtl(const std::string& action, std::string& out) {
    const std::string logPath = "/tmp/rmq_pr_master_cpp_broker_ctl." + action + ".log";
    const std::string cmd = "sh '" + gBrokerCtl + "' " + action + " > '" + logPath + "' 2>&1";
    int rc = std::system(cmd.c_str());
    out = readFile(logPath);
    return rc != -1 && WEXITSTATUS(rc) == 0;
}

const int32_t kQueues = 4;
const char* const kBrokerName = "broker-a";
// 本端失败上界：发布信息为空时压根没有 wire 调用，真机给 500ms 已留量级余量。
constexpr double kLocalBudgetMs = 500.0;

// 只收 body：这一趟关心的是"从节点上的 4 条能不能收齐"，不关心位点与顺序。
struct BodySink {
    std::mutex mtx;
    std::vector<std::string> bodies;

    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies;
    }
};

class BodyListener : public MessageListenerConcurrently {
public:
    explicit BodyListener(BodySink& sink) : sink_(sink) {}

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.bodies.push_back(m.getBody());
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    BodySink& sink_;
};

std::string fmtList(const std::vector<std::string>& items) {
    std::string s;
    for (const std::string& it : items) {
        if (!s.empty()) s += ",";
        s += it;
    }
    return s;
}

std::string fmtAddrs(const std::map<int64_t, std::string>& addrs) {
    std::string s;
    for (const auto& kv : addrs) {
        if (!s.empty()) s += " ";
        s += std::to_string(kv.first) + ":" + kv.second;
    }
    return "{" + s + "}";
}

std::string fmtQueues(const std::vector<MessageQueue>& qs) {
    std::string s;
    for (const MessageQueue& q : qs) {
        if (!s.empty()) s += " ";
        s += "(" + q.brokerName + "," + std::to_string(q.queueId) + ")";
    }
    return s;
}

std::string fmtMqOffsets(const std::vector<std::pair<MessageQueue, int64_t>>& rows) {
    std::string s;
    for (const auto& r : rows) {
        if (!s.empty()) s += " ";
        s += "q" + std::to_string(r.first.queueId) + ":" + std::to_string(r.second);
    }
    return s;
}

struct Fixture {
    std::string namesrv;
    std::string master;
    std::string slave;
    std::string stamp;
    DefaultMQAdminExt admin;
    std::shared_ptr<DefaultMQProducer> producer;

    void start(const std::string& ns, const std::string& masterAddr, const std::string& slaveAddr) {
        namesrv = ns;
        master = masterAddr;
        slave = slaveAddr;
        stamp = std::to_string(nowMs() % 1000000);
        admin.setNamesrvAddr(ns);
        admin.start();
        // 管理实例与生产者实例的 clientId 不同 ⇒ 进程内是两个 MQClientInstance：
        // S5 的「旧缓存」看的是发送实例自己那张表，这里要能分别驱动。
        producer = std::make_shared<DefaultMQProducer>("PID_PrMasterCpp_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("pr_master_cpp_" + stamp);
        producer->start();
    }

    std::string topic() const { return "PrMasterCpp" + stamp; }

    // 强制刷新后取 broker-a 的 brokerAddrs（刷不到返回空 map）。
    std::map<int64_t, std::string> routeAddrs(const std::string& t) {
        try {
            admin.client().updateTopicRouteInfoFromNameServer(t, 5000, false);
        } catch (const std::exception& e) {
            std::printf("    (路由刷新失败: %s)\n", e.what());
        }
        std::shared_ptr<TopicRouteData> route = admin.client().getTopicRouteData(t);
        if (route == nullptr) return std::map<int64_t, std::string>();
        for (const BrokerData& bd : route->getBrokerDatas()) {
            if (bd.getBrokerName() == kBrokerName) return bd.getBrokerAddrs();
        }
        return std::map<int64_t, std::string>();
    }

    std::vector<MessageQueue> seedQueues() {
        return admin.client().getTopicPublishInfo(topic(), false)->msgQueueList;
    }

    int64_t maxOffsetOf(const MessageQueue& mq, const std::string& addr) {
        return admin.client().getMaxOffset(mq, 5000, addr);
    }

    void shutdown() {
        if (producer) {
            try {
                producer->shutdown();
            } catch (const std::exception&) {
            }
        }
        try {
            admin.shutdown();
        } catch (const std::exception&) {
        }
    }
};

bool hasId(const std::map<int64_t, std::string>& addrs, int64_t id) {
    return addrs.find(id) != addrs.end();
}

/// 「发布信息组不出队列」的判据：访问器只在发布信息**有队列**时返回，
/// 否则本端抛 MQClientException("Can not find Message Queue for topic: ...")。
/// 返回 true 表示确实抛了这条（detail 里带上原文）；意外拿到非空列表时返回 false。
bool publishInfoEmpty(MQClientInstance& client, const std::string& topic, std::string& detail) {
    try {
        std::shared_ptr<TopicPublishInfo> info = client.getTopicPublishInfo(topic, false);
        detail = "意外拿到 " + std::to_string(info->msgQueueList.size()) + " 条队列";
        return false;
    } catch (const MQClientException& e) {
        detail = e.what();
        return std::string(e.what()).find("Can not find Message Queue for topic") !=
               std::string::npos;
    }
}

void scenario(Fixture& fx, std::shared_ptr<DefaultMQPushConsumer>& consumer) {
    const std::string topic = fx.topic();

    // ---------------- S0 控制腿 ----------------
    std::printf("S0 控制腿（master 在）：路由 {0: master, 1: slave}、发布/订阅各 %d 条\n", kQueues);
    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic, kQueues);
    } catch (const std::exception& e) {
        check("S0 建 topic（master）", false, e.what());
        return;
    }
    // 从节点也直建一份，不赌 SlaveSynchronize 的 5s 周期
    try {
        fx.admin.createTopicInBroker(fx.slave, topic, kQueues, kQueues);
    } catch (const std::exception& e) {
        std::printf("    (从节点建 topic 失败: %s)\n", e.what());
    }

    std::map<int64_t, std::string> addrs;
    const bool routeOk = waitUntil(
        [&] {
            addrs = fx.routeAddrs(topic);
            return hasId(addrs, 0) && hasId(addrs, 1);
        },
        30000);
    check("S0 路由含 {0: master, 1: slave}", routeOk, "brokerAddrs=" + fmtAddrs(addrs));
    if (!routeOk) return;
    check("S0 从节点地址与参数一致", addrs[1] == fx.slave,
          "route=" + addrs[1] + " argv=" + fx.slave);

    std::vector<MessageQueue> queues;
    try {
        queues = fx.seedQueues();
    } catch (const std::exception& e) {
        check("S0 发布队列 4（控制）", false, e.what());
        return;
    }
    check("S0 发布队列 4（控制）", queues.size() == static_cast<size_t>(kQueues),
          "queues=" + fmtQueues(queues));
    const std::vector<MessageQueue> subs = fx.admin.client().getTopicSubscribeInfo(topic);
    check("S0 订阅队列 4（控制）", subs.size() == static_cast<size_t>(kQueues),
          "queues=" + fmtQueues(subs));
    if (queues.size() != static_cast<size_t>(kQueues)) return;

    // ---------------- S1 预埋 ----------------
    std::printf("\nS1 预埋 %d 条（每队列定点一条）并等从节点 store 追上\n", kQueues);
    std::vector<std::string> seeded;
    for (size_t i = 0; i < queues.size(); ++i) {
        const std::string body = "pr-master-" + std::to_string(i);
        try {
            Message msg(topic, body);
            SendResult r = fx.producer->send(msg, queues[i], 20000);
            if (r.sendStatus == SendStatus::SEND_OK) {
                seeded.push_back(body);
            } else {
                check("S1 第 " + std::to_string(i) + " 条预埋 SEND_OK", false,
                      "status=" + std::to_string(static_cast<int>(r.sendStatus)));
            }
        } catch (const std::exception& e) {
            check("S1 第 " + std::to_string(i) + " 条预埋 SEND_OK", false, e.what());
        }
    }
    check("S1 4 条预埋全部 SEND_OK", seeded.size() == static_cast<size_t>(kQueues),
          "seeded=" + std::to_string(seeded.size()));
    if (seeded.size() != static_cast<size_t>(kQueues)) return;

    std::vector<std::pair<MessageQueue, int64_t>> masterMax;
    for (const MessageQueue& mq : queues) {
        try {
            masterMax.emplace_back(mq, fx.maxOffsetOf(mq, fx.master));
        } catch (const std::exception& e) {
            std::printf("    (master maxOffset 查询失败: %s)\n", e.what());
        }
    }
    std::vector<std::pair<MessageQueue, int64_t>> slaveMax;
    const bool caughtUp = waitUntil(
        [&] {
            if (masterMax.empty()) return false;
            std::vector<std::pair<MessageQueue, int64_t>> now;
            for (const MessageQueue& mq : queues) {
                try {
                    now.emplace_back(mq, fx.maxOffsetOf(mq, fx.slave));
                } catch (const std::exception& e) {
                    std::printf("    (从节点取 maxOffset 失败: %s)\n", e.what());
                    return false;
                }
            }
            for (const auto& m : masterMax) {
                bool covered = false;
                for (const auto& s : now) {
                    if (s.first.queueId == m.first.queueId && s.second >= m.second) covered = true;
                }
                if (!covered) return false;
            }
            slaveMax = now;
            return true;
        },
        30000);
    check("S1 4 条已复制到从节点 store", caughtUp,
          "master=" + fmtMqOffsets(masterMax) + " slave=" + fmtMqOffsets(slaveMax));

    // ---------------- S2 停 master ----------------
    std::printf("\nS2 停 master（scripts/rmq_test_broker.sh stop），等路由只剩从节点\n");
    std::string out;
    check("S2 master 已优雅停机", brokerCtl("stop", out), out);

    std::map<int64_t, std::string> downAddrs;
    const bool masterless = waitUntil(
        [&] {
            downAddrs = fx.routeAddrs(topic);
            return !hasId(downAddrs, 0) && hasId(downAddrs, 1);
        },
        60000);
    check("S2 路由里 broker-a 只剩 {1: slave}", masterless, "brokerAddrs=" + fmtAddrs(downAddrs));
    if (!masterless) return;

    // ---------------- S3 (A) 发布队列 ----------------
    std::printf("\nS3 (A) 发布信息跳过没有 master 的 broker\n");
    std::string pubDetail;
    check("S3 停 master 后发布队列 == 0（访问器本端抛「选不到队列」）",
          publishInfoEmpty(fx.admin.client(), topic, pubDetail), pubDetail);

    // ---------------- S4 (C) 订阅队列 ----------------
    const std::vector<MessageQueue> subsDown = fx.admin.client().getTopicSubscribeInfo(topic);
    bool allBrokerA = subsDown.size() == static_cast<size_t>(kQueues);
    for (const MessageQueue& q : subsDown) {
        if (q.brokerName != kBrokerName) allBrokerA = false;
    }
    check("S4 (C) 订阅队列仍是 4、且都在 broker-a（消费侧不看 master）", allBrokerA,
          "queues=" + fmtQueues(subsDown));

    // ---------------- S5 (A 定型) 发送快速失败 ----------------
    // 分两段看：**缓存还没刷**时（现实中 30s 周期任务未到）发送实例手里还是停前的旧路由，
    // 地址解析落在死掉的 master 上，快速失败、绝不静默改发从节点；**路由刷成停后形状**后
    // （周期任务 / 显式刷新），(A) 生效：发布队列为空，发送连一条 wire 都不发。
    std::printf("\nS5 不指定队列的同步发送：旧缓存快速失败 → 刷新后本端快速失败\n");
    const double staleBegin = monotonicSeconds();
    std::string staleText;
    bool staleFailed = false;
    try {
        Message msg(topic, "must-not-send");
        SendResult r = fx.producer->send(msg, 20000);
        staleText = "Ok(status=" + std::to_string(static_cast<int>(r.sendStatus)) + ")";
    } catch (const std::exception& e) {
        staleFailed = true;
        staleText = e.what();
    }
    const double staleMs = (monotonicSeconds() - staleBegin) * 1000.0;
    // 这一腿是机会腿：周期刷新是否已经跑过不由本脚本定。两条腿的共同判据是
    // "快速失败 + 绝不落到从节点地址上"（旧缓存腿打的是死掉的 master）。
    check("S5 发送快速失败，且报错里没有从节点地址（绝不改发从节点）",
          staleFailed && staleMs < kLocalBudgetMs &&
              staleText.find(fx.slave) == std::string::npos,
          std::to_string(static_cast<int>(staleMs)) + "ms " + staleText);

    // 让发送实例自己的路由缓存刷成停后形状（与 30s 周期任务同一条代码路径）
    try {
        fx.producer->client().updateTopicRouteInfoFromNameServer(topic, 5000, false);
    } catch (const std::exception& e) {
        std::printf("    (发送实例路由刷新失败: %s)\n", e.what());
    }
    std::string sendPubDetail;
    check("S5b 发送实例的发布队列也 == 0（(A) 就作用在这里）",
          publishInfoEmpty(fx.producer->client(), topic, sendPubDetail), sendPubDetail);

    const double freshBegin = monotonicSeconds();
    std::string freshText;
    int32_t freshCode = 0;
    bool freshFailed = false;
    try {
        Message msg2(topic, "must-not-send-2");
        SendResult r = fx.producer->send(msg2, 20000);
        freshText = "Ok(status=" + std::to_string(static_cast<int>(r.sendStatus)) + ")";
    } catch (const MQClientException& e) {
        freshFailed = true;
        freshCode = e.getResponseCode();
        freshText = e.what();
    } catch (const std::exception& e) {
        freshFailed = true;
        freshText = e.what();
    }
    const double freshMs = (monotonicSeconds() - freshBegin) * 1000.0;
    check("S5c 刷新后：本端 10005 抛「选不到队列」，无 wire 调用（无 BrokersSent）",
          freshFailed && freshCode == ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION &&
              freshText.find("Can not find Message Queue for topic") != std::string::npos &&
              freshText.find("BrokersSent") == std::string::npos && freshMs < kLocalBudgetMs,
          std::to_string(static_cast<int>(freshMs)) + "ms code=" + std::to_string(freshCode) +
              ": " + freshText);

    // ---------------- S5d (B) 对照：定点发送的地址解析 ----------------
    // 定点发送不经过发布信息（调用方直接给了 mq），地址侧若还按「主优先、没主退一台」去解析，
    // 请求就会落到从节点上换来一个 SYSTEM_BUSY(2)。Java 的 findBrokerAddressInPublish
    // 只认 brokerId=0，本端应当直接报「broker 不存在」，一条 wire 都不发。
    std::printf("\nS5d (B) 对照：定点发到该队列 → 本端报 broker 不存在（不发 wire）\n");
    const double pinnedBegin = monotonicSeconds();
    std::string pinnedText;
    bool pinnedFailed = false;
    bool pinnedIsClientError = false;
    try {
        Message msg3(topic, "pinned-to-slave");
        SendResult r = fx.producer->send(msg3, MessageQueue(topic, kBrokerName, 0), 20000);
        pinnedText = "Ok(status=" + std::to_string(static_cast<int>(r.sendStatus)) + ")";
    } catch (const MQClientException& e) {
        // MQBrokerException 不继承 MQClientException：这里能进来说明不是 broker 回的码
        pinnedFailed = true;
        pinnedIsClientError = true;
        pinnedText = e.what();
    } catch (const std::exception& e) {
        pinnedFailed = true;
        pinnedText = e.what();
    }
    const double pinnedMs = (monotonicSeconds() - pinnedBegin) * 1000.0;
    check("S5d 定点发送本端报「The broker[broker-a] not exist」，无 wire"
          "（不是从节点回的 SYSTEM_BUSY(2)）",
          pinnedFailed && pinnedIsClientError &&
              pinnedText == "The broker[" + std::string(kBrokerName) + "] not exist" &&
              pinnedMs < kLocalBudgetMs,
          std::to_string(static_cast<int>(pinnedMs)) + "ms " + pinnedText);

    // ---------------- S6 (C 端到端) 停窗口内消费 ----------------
    std::printf("\nS6 停窗口内新起的 push 消费者：4 条队列 + 从从节点收齐预埋的 4 条\n");
    const std::string group = "GID_PrMasterCpp_" + fx.stamp;
    BodySink sink;
    consumer = std::make_shared<DefaultMQPushConsumer>(group);
    consumer->setNamesrvAddr(fx.namesrv);
    consumer->setInstanceName("pr_master_cpp_c_" + fx.stamp);
    consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    consumer->setMessageListener(std::make_shared<BodyListener>(sink));
    consumer->subscribe(topic, "*");
    try {
        consumer->start();
    } catch (const std::exception& e) {
        check("S6 consumer start", false, e.what());
        consumer.reset();
        return;
    }
    // client() 要求已 start（C++ 侧未启动会抛），所以只能在 start 之后取。
    try {
        const std::vector<MessageQueue> subsInWindow =
            consumer->client().getTopicSubscribeInfo(topic);
        check("S6a 窗口内消费者自己的订阅信息也是 4 条",
              subsInWindow.size() == static_cast<size_t>(kQueues), "queues=" + fmtQueues(subsInWindow));
    } catch (const std::exception& e) {
        check("S6a 窗口内消费者自己的订阅信息也是 4 条", false, e.what());
    }
    std::vector<std::string> expected = seeded;
    std::sort(expected.begin(), expected.end());
    const bool gotOk = waitUntil(
        [&] {
            std::vector<std::string> got = sink.snapshot();
            std::sort(got.begin(), got.end());
            return got == expected;
        },
        60000);
    check("S6 停 master 期间从从节点收齐 4 条", gotOk, "got=[" + fmtList(sink.snapshot()) + "]");

    // ---------------- S7 负控（先把 master 拉回来） ----------------
    std::printf("\nS7 负控：master 拉回后发布队列恢复、发送恢复\n");
    std::string outStart;
    if (!brokerCtl("start", outStart)) {
        check("S7 master 复位", false, outStart);
    }

    std::vector<MessageQueue> queuesBack;
    const bool publishBack = waitUntil(
        [&] {
            try {
                queuesBack = fx.seedQueues();
                return queuesBack.size() == static_cast<size_t>(kQueues);
            } catch (const std::exception&) {
                return false;
            }
        },
        60000);
    check("S7 发布队列恢复 4", publishBack, "queues=" + fmtQueues(queuesBack));
    // 两条失败发送都没在 broker 上留下消息：每条预埋队列的 maxOffset 仍是 1
    std::vector<std::pair<MessageQueue, int64_t>> after;
    for (const MessageQueue& mq : (queuesBack.empty() ? queues : queuesBack)) {
        try {
            after.emplace_back(mq, fx.maxOffsetOf(mq, fx.master));
        } catch (const std::exception& e) {
            std::printf("    (maxOffset 查询失败: %s)\n", e.what());
        }
    }
    bool allOne = !after.empty();
    for (const auto& r : after) {
        if (r.second != 1) allOne = false;
    }
    check("S7b 两条失败发送没留下消息（maxOffset 仍是 1）", allOne, "maxOffset=" + fmtMqOffsets(after));
    try {
        Message back(topic, "pr-master-back");
        SendResult r = fx.producer->send(back, 20000);
        check("S7c 发送恢复 SEND_OK", r.sendStatus == SendStatus::SEND_OK,
              "status=" + std::to_string(static_cast<int>(r.sendStatus)));
    } catch (const std::exception& e) {
        check("S7c 发送恢复 SEND_OK", false, e.what());
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    const std::string masterAddr = argc > 2 ? argv[2] : "127.0.0.1:10911";
    const std::string slaveAddr = argc > 3 ? argv[3] : "127.0.0.1:10931";
    const char* root = std::getenv("RMQ_REPO_ROOT");
    gBrokerCtl = (root != nullptr ? std::string(root) : std::string("..")) +
                 "/scripts/rmq_test_broker.sh";

    std::printf("broker_ctl=%s namesrv=%s master=%s slave=%s\n", gBrokerCtl.c_str(),
                nsAddr.c_str(), masterAddr.c_str(), slaveAddr.c_str());
    std::string out;
    if (!brokerCtl("status", out)) {
        std::printf("master 没在跑：先按本地集群 runbook 起 namesrv + master + slave（%s）\n",
                    out.c_str());
        return 2;
    }

    Fixture fx;
    std::shared_ptr<DefaultMQPushConsumer> consumer;
    try {
        fx.start(nsAddr, masterAddr, slaveAddr);
    } catch (const std::exception& e) {
        check("集群探活 / 客户端启动", false, e.what());
        return 1;
    }

    try {
        scenario(fx, consumer);
    } catch (const std::exception& e) {
        check("验证过程抛出异常", false, e.what());
    }

    // ---------------- 收尾 ----------------
    // master 必须回来（start 幂等，异常路径也走到这里）；再关客户端、删 topic。
    std::string outBack;
    if (!brokerCtl("start", outBack)) {
        std::printf("  [FAIL] master 复位失败: %s\n", outBack.c_str());
        ++gFail;
    }
    if (consumer) {
        try {
            consumer->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (consumer shutdown 失败: %s)\n", e.what());
        }
    }
    try {
        fx.admin.deleteTopic(fx.topic());
    } catch (const std::exception& e) {
        std::printf("    (deleteTopic(%s) 失败: %s)\n", fx.topic().c_str(), e.what());
    }
    fx.shutdown();

    std::printf("\n== 结果: %d/%d 通过 ==\n", gPass, gPass + gFail);
    return gFail == 0 ? 0 : 1;
}
