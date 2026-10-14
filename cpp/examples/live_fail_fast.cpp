// Real-cluster verification of fail-fast on send failures (scenarios and assertions below).
//
// The offline case (tests/test_fail_fast.cpp) locks the transport contract: a local fake peer reads then
// closes, asserting "death detected in milliseconds + reports RemotingSendRequestException not a timeout + callback fired exactly once". But this path
// exists precisely for **broker restarts / master-standby failover / network jitter on real machines**, and only a real cluster can answer:
//
//   L1 baseline: sends and long polling both work against a real broker (first confirm that later failures are not caused by the environment).
//   L2 park: pin three long polls that are **really parked on the broker** (suspend 20s, client timeout 30s) in the in-flight table.
//   L3 close: kill the broker (the read thread sees EOF) -> long polls must immediately get a RemotingSendRequestException,
//       instead of waiting out 30s and reporting a RemotingTimeoutException. The type cannot be wrong: the async-send retry classification
//       branches on exception **type** (InvokeError::Kind), so reporting it as a timeout swaps out the whole retry decision.
//   L4 scope: detection only drags in the connection that died; the namesrv connection on the same transport instance keeps
//       serving (GET_ALL_TOPIC_LIST_FROM_NAMESERVER still returns SUCCESS).
//   L5 recovery: after the broker comes back up, the same producer instance reconnects and sends as usual; not one of the messages
//       that already got SEND_OK may be missing.
//
// The L3 threshold (8s) is far below the client timeout (30s) and below the broker suspend cap (20s): without failFast
// this assertion necessarily fails, not by luck.
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

// The client-side timeout is deliberately raised to 30s: if detection took the timeout path, it would wait at least this long.
constexpr int32_t kCLIENT_TIMEOUT_MILLIS = 30000;
constexpr int32_t kBROKER_SUSPEND_MILLIS = 20000;
// failFast should fire at millisecond scale; leave 8s for real-machine scheduling (EOF arrival + callback delivery).
constexpr double kFailFastLimitSeconds = 8.0;
constexpr int32_t kBASELINE_MSGS = 5;
constexpr int32_t kPARKED = 3;

/// Runs scripts/rmq_test_broker.sh. Output goes to a **file** instead of a pipe: start keeps the broker as
/// a resident process, and whoever inherits its write end never sees EOF.
bool brokerCtl(const std::string& action, std::string& out) {
    const std::string logPath = "/tmp/rmq_fail_fast_cpp_broker_ctl." + action + ".log";
    const std::string cmd = "sh '" + gBrokerCtl + "' " + action + " > '" + logPath + "' 2>&1";
    int rc = std::system(cmd.c_str());
    out = readFile(logPath);
    return rc != -1 && WEXITSTATUS(rc) == 0;
}

/// The outcome of one parked long poll.
struct Parked {
    std::string tag;
    std::string kind;  // response / send_request / timeout / connect / other
    std::string message;
    double costSeconds = 0.0;
};

/// Build a suspend=true long poll by hand: bypassing the pull consumer's clamping, the 30s client timeout and
/// the 20s broker suspend are both decided by this case.
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

/// Whether the messages that already got SEND_OK are still on the broker (max offsets summed over all queues).
/// After a restart, route and store loading need some time, so this retries.
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
                std::printf("  [retry] maxOffset(queueId=%d) failed: %s\n", q.queueId, e.what());
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
        std::printf("broker not running: first start namesrv + broker per the local cluster runbook (%s)\n", out.c_str());
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

    // No matter which step fails, never hand the test cluster over to the next case in a stopped state.
    auto ensureBrokerUp = [&]() {
        std::string so;
        if (!brokerCtl("status", so)) {
            std::printf("  [cleanup] broker still DOWN, issuing one more start\n");
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
        check("L1 baseline: " + std::to_string(kBASELINE_MSGS) + " sync sends got SEND_OK",
              landed == kBASELINE_MSGS, "landed=" + std::to_string(landed));

        consumer.start();
        std::vector<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(TOPIC);
        check("L1 fetched queues", !queues.empty(), "queues=" + std::to_string(queues.size()));
        if (queues.empty()) {
            throw std::runtime_error("no queues");
        }
        MQClientInstance& client = producer.client();
        auto route = client.getTopicRouteData(TOPIC);
        const std::string brokerName = queues.front().brokerName;
        const std::string addr = route == nullptr
                                     ? std::string()
                                     : MQClientInstance::findBrokerAddrInRoute(*route, brokerName);
        check("L1 got broker address", !addr.empty(), "addr=" + addr);
        if (addr.empty()) {
            throw std::runtime_error("no broker addr");
        }

        int64_t maxOffset = consumer.maxOffset(queues.front());
        int64_t totalMax = maxOffset;
        for (size_t i = 1; i < queues.size(); ++i) {
            totalMax += consumer.maxOffset(queues[i]);
        }
        // Sends rotate across queues, so one queue's tail covers only the messages that landed on it;
        // "really stored" therefore means the sum over all queues.
        check("L1 max offsets summed over queues cover the " + std::to_string(kBASELINE_MSGS) + " just sent (really stored)",
              totalMax >= kBASELINE_MSGS, "max_offset total=" + std::to_string(totalMax));

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
        check("L2 the three long polls really sit parked on the broker (still not returned after 2s)",
              returned.load() == 0, "returned=" + std::to_string(returned.load()));
        check("L2 they appear in the in-flight table",
              remoting.pendingRequestCount() >= baselineInFlight + kPARKED,
              "in_flight=" + std::to_string(remoting.pendingRequestCount())
                  + " baseline=" + std::to_string(baselineInFlight));

        // ------------------------------------------------------------- L3
        brokerStopped = true;
        check("L3 broker stopped", brokerCtl("stop", out), out);
        for (std::thread& t : threads) {
            t.join();
        }
        threads.clear();
        check("L3 all parked long polls returned (nothing stuck)",
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
        check("L3 the reported error is RemotingSendRequestException (the failFast contract)",
              sendRequest == kPARKED, "kinds=" + kinds);
        check("L3 none were reported as timeouts (wrong type = wrong retry decision)", timeouts == 0,
              "timeout=" + std::to_string(timeouts));
        char worstBuf[32];
        std::snprintf(worstBuf, sizeof(worstBuf), "%.2fs", worst);
        check("L3 detection took far less than the 30s client timeout", worst < kFailFastLimitSeconds,
              std::string("worst=") + worstBuf
                  + " limit=" + std::to_string(kFailFastLimitSeconds) + "s");
        check("L3 the exception text carries the disconnect reason", firstMsg.find("connection closed") != std::string::npos,
              firstMsg);
        // The in-flight table is drained by failFast itself (not waiting for the sweep thread)
        bool drained = false;
        for (int32_t i = 0; i < 40 && !drained; ++i) {
            drained = remoting.pendingRequestCount() == 0;
            if (!drained) {
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
            }
        }
        check("L3 the in-flight table is drained after detection", drained,
              "in_flight=" + std::to_string(remoting.pendingRequestCount()));

        // ------------------------------------------------------------- L4
        // Detection must only drag in the connection that died: namesrv runs on another connection of the same transport instance.
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
        check("L4 the namesrv connection was not dragged in (still serving while the broker was dead)",
              nsKind == "response" && nsCode == ResponseCode::SUCCESS,
              nsKind + " code=" + std::to_string(nsCode));

        // ------------------------------------------------------------- L5
        check("L5 broker brought back up", brokerCtl("start", out), out);
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
        check("L5 the same producer instance sends as usual after reconnecting", recovered, recErr);

        int64_t afterRestart = totalMaxOffset(consumer, queues, 20);
        check("L5 after restart the broker still holds those " + std::to_string(kBASELINE_MSGS) + " SEND_OK messages",
              afterRestart >= kBASELINE_MSGS, "max_offset total=" + std::to_string(afterRestart));

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
