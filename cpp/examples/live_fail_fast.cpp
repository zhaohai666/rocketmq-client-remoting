// broker 真的死了：在途请求必须**立刻**有终态（Java ``failFast`` → ``requestFail``）真机验证。
// 用法：rmq_live_fail_fast 127.0.0.1:9876
//
// 与 Python verify_fail_fast_live.py 同场景、同断言（三语言对拍共用一套口径）。
//
// 离线用例（tests/test_fail_fast.cpp）锁的是传输层契约：本机假对端读完就关，断言
// "毫秒级判死 + 报 RemotingSendRequestException 而不是超时 + 回调只投一次"。但这条路径
// 存在的意义正是**真机上的 broker 重启 / 主备切换 / 网络抖动**，只有真集群能回答：
//
//   L1 基线：真 broker 上发送与长轮询都正常（先确认后面的失败不是环境造成的）。
//   L2 挂起：把三条**真的挂在 broker 上**的长轮询（suspend 20s、客户端超时 30s）钉在在途表里。
//   L3 收口：杀掉 broker（读线程见到 EOF）→ 长轮询必须立刻拿到 RemotingSendRequestException，
//       而不是等满 30s 报一个 RemotingTimeoutException。类型不能错：异步发送的重试分类按
//       异常**类型**分流（InvokeError::Kind），报成超时等于换了一整套重试决策。
//   L4 范围：判死只牵连死掉那条连接；同一个传输实例上的 namesrv 连接照常服务
//       （GET_ALL_TOPIC_LIST_FROM_NAMESERVER 仍返回 SUCCESS）。
//   L5 恢复：broker 拉起后同一个 producer 实例重新建连照常发送；已经拿到 SEND_OK 的消息
//       一条都不能少。
//
// L3 的阈值（8s）远小于客户端超时（30s），也小于 broker 的 suspend 上限（20s）：缺了
// failFast 这条断言必然失败，不是碰运气。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/remoting_client.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

double monotonicSeconds() {
    return std::chrono::duration<double>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
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

// 客户端侧超时故意放到 30s：判死若走的是超时路径，至少要等这么久。
constexpr int32_t kCLIENT_TIMEOUT_MILLIS = 30000;
constexpr int32_t kBROKER_SUSPEND_MILLIS = 20000;
// failFast 应当是毫秒级；留 8s 给真机调度（EOF 到达 + 回调投递）。
constexpr double kFailFastLimitSeconds = 8.0;
constexpr int32_t kBASELINE_MSGS = 5;
constexpr int32_t kPARKED = 3;

/// 跑 scripts/rmq_test_broker.sh。输出走**文件**而不是管道：start 会把 broker 拉成
/// 常驻进程，谁继承它的写端谁就等不到 EOF。
bool brokerCtl(const std::string& action, std::string& out) {
    const std::string logPath = "/tmp/rmq_fail_fast_cpp_broker_ctl." + action + ".log";
    const std::string cmd = "sh '" + gBrokerCtl + "' " + action + " > '" + logPath + "' 2>&1";
    int rc = std::system(cmd.c_str());
    out = readFile(logPath);
    return rc != -1 && WEXITSTATUS(rc) == 0;
}

/// 一条挂起的长轮询的结局。
struct Parked {
    std::string tag;
    std::string kind;  // response / send_request / timeout / connect / other
    std::string message;
    double costSeconds = 0.0;
};

/// 手工构造一条 suspend=true 的长轮询：不经 pull consumer 的钳制，30s 客户端超时与
/// 20s broker suspend 都由本用例说了算。
RemotingCommand parkedPullRequest(const std::string& group, const MessageQueue& mq,
                                  int64_t queueOffset) {
    auto header = std::make_shared<PullMessageRequestHeader>();
    header->consumerGroup = group;
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    header->queueOffset = queueOffset;
    header->maxMsgNums = 32;
    header->sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/false, /*suspend=*/true,
                                                /*subscription=*/true, /*classFilter=*/false);
    header->commitOffset = 0;
    header->suspendTimeoutMillis = kBROKER_SUSPEND_MILLIS;
    header->subscription = "*";
    header->subVersion = 0;
    header->expressionType = "TAG";
    header->maxMsgBytes = -1;
    header->requestSource = 0;
    return RemotingCommand::createRequestCommand(RequestCode::PULL_MESSAGE, header);
}

void parkOne(RemotingClient& remoting, const std::string& addr, const std::string& group,
             const MessageQueue& mq, int64_t offset, const std::string& tag,
             std::vector<Parked>& out, std::mutex& mu, std::atomic<int>& returned) {
    Parked p;
    p.tag = tag;
    RemotingCommand req = parkedPullRequest(group, mq, offset);
    const double started = monotonicSeconds();
    try {
        RemotingCommand resp = remoting.invokeSync(addr, req, kCLIENT_TIMEOUT_MILLIS);
        p.kind = "response";
        p.message = "code=" + std::to_string(resp.code);
    } catch (const RemotingSendRequestException& e) {
        p.kind = "send_request";
        p.message = e.what();
    } catch (const RemotingTimeoutException& e) {
        p.kind = "timeout";
        p.message = e.what();
    } catch (const RemotingConnectException& e) {
        p.kind = "connect";
        p.message = e.what();
    } catch (const std::exception& e) {
        p.kind = "other";
        p.message = e.what();
    }
    p.costSeconds = monotonicSeconds() - started;
    {
        std::lock_guard<std::mutex> lk(mu);
        out.push_back(p);
    }
    ++returned;
}

/// 已经拿到 SEND_OK 的那几条消息在 broker 上还在不在（各队列队尾位点合计）。
/// 重启后路由与 store 加载需要一点时间，所以带重试。
int64_t totalMaxOffset(DefaultMQPullConsumer& consumer, const std::vector<MessageQueue>& queues,
                       int32_t attempts) {
    int64_t total = 0;
    for (int32_t i = 0; i < attempts; ++i) {
        total = 0;
        bool ok = true;
        for (const MessageQueue& q : queues) {
            try {
                total += consumer.maxOffset(q);
            } catch (const std::exception& e) {
                ok = false;
                std::printf("  [retry] maxOffset(queueId=%d) 失败: %s\n", q.queueId, e.what());
                break;
            }
        }
        if (ok && total >= kBASELINE_MSGS) {
            return total;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return total;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string namesrv = argc > 1 ? argv[1] : "127.0.0.1:9876";
    const char* root = std::getenv("RMQ_REPO_ROOT");
    gBrokerCtl = (root != nullptr ? std::string(root) : std::string("..")) +
                 "/scripts/rmq_test_broker.sh";
    const int64_t stamp = nowMs();
    const std::string TOPIC = "FailFastCpp_" + std::to_string(stamp);
    const std::string GROUP = "fail_fast_cpp_" + std::to_string(stamp);

    std::printf("broker_ctl=%s namesrv=%s topic=%s\n", gBrokerCtl.c_str(), namesrv.c_str(),
                TOPIC.c_str());

    std::string out;
    if (!brokerCtl("status", out)) {
        std::printf("broker 没在跑：先按本地集群 runbook 起 namesrv + broker（%s）\n", out.c_str());
        return 2;
    }

    DefaultMQProducer producer(GROUP + "_p");
    producer.setNamesrvAddr(namesrv);
    producer.setInstanceName("ff_cpp_" + std::to_string(stamp));
    DefaultMQPullConsumer consumer(GROUP);
    consumer.setNamesrvAddr(namesrv);
    consumer.setInstanceName("ff_cpp_" + std::to_string(stamp));

    std::vector<std::thread> threads;
    std::mutex parkedMu;
    std::vector<Parked> parked;
    std::atomic<int> returned{0};
    int exitCode = 1;
    bool brokerStopped = false;

    // 无论走到哪一步，都不能把测试集群留在停机状态交给下一个用例。
    auto ensureBrokerUp = [&]() {
        std::string so;
        if (!brokerCtl("status", so)) {
            std::printf("  [cleanup] broker 仍在 DOWN，补一次 start\n");
            brokerCtl("start", so);
        }
    };

    try {
        // ------------------------------------------------------------- L1
        producer.start();
        int32_t landed = 0;
        for (int32_t i = 0; i < kBASELINE_MSGS; ++i) {
            Message msg(TOPIC, "fail-fast-cpp-base-" + std::to_string(i));
            msg.setKeys("ff-base-" + std::to_string(i));
            SendResult r = producer.send(msg, 5000);
            if (r.sendStatus == SendStatus::SEND_OK) {
                ++landed;
            }
        }
        check("L1 基线：" + std::to_string(kBASELINE_MSGS) + " 条同步发送 SEND_OK",
              landed == kBASELINE_MSGS, "landed=" + std::to_string(landed));

        consumer.start();
        std::vector<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(TOPIC);
        check("L1 取到队列", !queues.empty(), "queues=" + std::to_string(queues.size()));
        if (queues.empty()) {
            throw std::runtime_error("no queues");
        }
        MQClientInstance& client = producer.client();
        auto route = client.getTopicRouteData(TOPIC);
        const std::string brokerName = queues.front().brokerName;
        const std::string addr = route == nullptr
                                     ? std::string()
                                     : MQClientInstance::findBrokerAddrInRoute(*route, brokerName);
        check("L1 拿到 broker 地址", !addr.empty(), "addr=" + addr);
        if (addr.empty()) {
            throw std::runtime_error("no broker addr");
        }

        int64_t maxOffset = consumer.maxOffset(queues.front());
        int64_t totalMax = maxOffset;
        for (size_t i = 1; i < queues.size(); ++i) {
            totalMax += consumer.maxOffset(queues[i]);
        }
        // 发送是跨队列轮转的，单条队列的队尾只覆盖落在它上面的那部分，
        // 所以"真的落盘"要看全部队列的合计。
        check("L1 各队列队尾位点合计覆盖刚发的 " + std::to_string(kBASELINE_MSGS) + " 条（真的落盘）",
              totalMax >= kBASELINE_MSGS, "max_offset 合计=" + std::to_string(totalMax));

        // ------------------------------------------------------------- L2
        RemotingClient& remoting = client.remotingClient();
        const size_t baselineInFlight = remoting.pendingRequestCount();
        for (int32_t i = 0; i < kPARKED; ++i) {
            threads.emplace_back(parkOne, std::ref(remoting), std::cref(addr), std::cref(GROUP),
                                 std::cref(queues.front()), maxOffset,
                                 "park-" + std::to_string(i), std::ref(parked),
                                 std::ref(parkedMu), std::ref(returned));
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
        check("L2 三条长轮询真的挂在 broker 上（2s 后仍未返回）",
              returned.load() == 0, "returned=" + std::to_string(returned.load()));
        check("L2 在途表里有它们",
              remoting.pendingRequestCount() >= baselineInFlight + kPARKED,
              "in_flight=" + std::to_string(remoting.pendingRequestCount())
                  + " baseline=" + std::to_string(baselineInFlight));

        // ------------------------------------------------------------- L3
        brokerStopped = true;
        check("L3 停掉 broker", brokerCtl("stop", out), out);
        for (std::thread& t : threads) {
            t.join();
        }
        threads.clear();
        check("L3 挂起的长轮询全部返回（没有卡死）",
              static_cast<int32_t>(parked.size()) == kPARKED,
              "got=" + std::to_string(parked.size()));

        int32_t sendRequest = 0;
        int32_t timeouts = 0;
        double worst = 0.0;
        std::string kinds;
        std::string firstMsg;
        for (const Parked& p : parked) {
            kinds += (kinds.empty() ? "" : ",") + p.kind;
            if (p.kind == "send_request") {
                ++sendRequest;
                if (firstMsg.empty()) {
                    firstMsg = p.message;
                }
            } else if (p.kind == "timeout") {
                ++timeouts;
            }
            worst = std::max(worst, p.costSeconds);
        }
        // 报的是 RemotingSendRequestException（Java failFast 的口径）
        check("L3 报的是 RemotingSendRequestException（Java failFast 的口径）",
              sendRequest == kPARKED, "kinds=" + kinds);
        check("L3 一条都没被报成超时（类型错 = 重试决策错）", timeouts == 0,
              "timeout=" + std::to_string(timeouts));
        char worstBuf[32];
        std::snprintf(worstBuf, sizeof(worstBuf), "%.2fs", worst);
        check("L3 判死耗时远小于 30s 客户端超时", worst < kFailFastLimitSeconds,
              std::string("worst=") + worstBuf
                  + " limit=" + std::to_string(kFailFastLimitSeconds) + "s");
        check("L3 异常文案带着断连原因", firstMsg.find("connection closed") != std::string::npos,
              firstMsg);
        // 在途表被 failFast 排空（不是等清理线程扫）
        bool drained = false;
        for (int32_t i = 0; i < 40 && !drained; ++i) {
            drained = remoting.pendingRequestCount() == 0;
            if (!drained) {
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
            }
        }
        check("L3 判死之后在途表排空", drained,
              "in_flight=" + std::to_string(remoting.pendingRequestCount()));

        // ------------------------------------------------------------- L4
        // 判死必须只牵连死掉那条连接：namesrv 走的是同一个传输实例的另一条连接。
        RemotingCommand probe =
            RemotingCommand::createRequestCommand(RequestCode::GET_ALL_TOPIC_LIST_FROM_NAMESERVER);
        std::string nsKind;
        int32_t nsCode = -1;
        try {
            RemotingCommand nsResp = remoting.invokeSync(namesrv, probe, 5000);
            nsCode = nsResp.code;
            nsKind = "response";
        } catch (const std::exception& e) {
            nsKind = e.what();
        }
        check("L4 namesrv 连接没被牵连（broker 死了它还在服务）",
              nsKind == "response" && nsCode == ResponseCode::SUCCESS,
              nsKind + " code=" + std::to_string(nsCode));

        // ------------------------------------------------------------- L5
        check("L5 broker 重新拉起", brokerCtl("start", out), out);
        brokerStopped = false;
        bool recovered = false;
        std::string recErr;
        for (int32_t i = 0; i < 20 && !recovered; ++i) {
            try {
                Message msg(TOPIC, "fail-fast-cpp-recover-" + std::to_string(i));
                msg.setKeys("ff-recover");
                SendResult r = producer.send(msg, 5000);
                recovered = r.sendStatus == SendStatus::SEND_OK;
                if (!recovered) {
                    recErr = "status not OK";
                }
            } catch (const std::exception& e) {
                recErr = e.what();
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        check("L5 同一个 producer 实例重新建连后照常发送", recovered, recErr);

        int64_t afterRestart = totalMaxOffset(consumer, queues, 20);
        check("L5 重启后 broker 上仍有那 " + std::to_string(kBASELINE_MSGS) + " 条 SEND_OK 的消息",
              afterRestart >= kBASELINE_MSGS, "max_offset 合计=" + std::to_string(afterRestart));

        exitCode = gFail == 0 ? 0 : 1;
    } catch (const std::exception& e) {
        std::printf("  [ABORT] %s\n", e.what());
        for (std::thread& t : threads) {
            t.join();
        }
        exitCode = 1;
    }

    try {
        consumer.shutdown();
    } catch (...) {
    }
    try {
        producer.shutdown();
    } catch (...) {
    }
    if (brokerStopped) {
        ensureBrokerUp();
    }
    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    return exitCode;
}
