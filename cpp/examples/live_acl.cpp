// Prerequisite: the broker has `authenticationEnabled=true` and, via `initAuthenticationUser`,
//       a SUPER user matching the accessKey/secretKey (see broker_acl.conf of /tmp/run_acl_live.sh).
//       Then **every** broker RPC requires a valid signature:
//       AuthConfig.isAuthenticationRequired() = authenticationEnabled && !whitelist.contains(rpc),
//       and the whitelist is empty by default.
//
// Scenarios:
//   S1 positive: admin with credentials creates the topic -> succeeds (admin-path signature accepted)
//   S2 negative: admin without credentials creates a topic -> rejected (broker NO_PERMISSION=16)
//   S3 negative: producer with a wrong secretKey sends -> rejected (NO_PERMISSION=16)
//   S4 positive: producer with credentials sends 3 messages -> SEND_OK (msgId assigned by the broker)
//   S5 positive: consumer with credentials receives all 3 (heartbeat / long-poll pull / offset commit all signed)
//            ⚠ S4/S5 must **start the consumer before sending**, otherwise the CONSUME_FROM_LAST_OFFSET initial-offset
//              semantics + async broker delivery make this same timing sequence yield inconsistent results (see the comments at S4/S5)
//   S6 negative: bare RPC straight to the broker without credentials -> rejected (NO_PERMISSION=16)
//   S7 positive: credential-less NameServer route query still succeeds (the hook only acts on auth-required targets)
//
// Key fact (do not change this from memory): every broker-side auth failure throws
// AbortProcessException(NO_PERMISSION=16);
// the failure reason cannot be told apart by ResponseCode, only by the remark:
//   - wrong signature → "check signature failed."
//   - missing credentials → "User:null is not found."
#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"

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
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool contains(const std::string& s, const std::string& sub) {
    return s.find(sub) != std::string::npos;
}

// The unified response code for broker-side auth failures.
constexpr int32_t NO_PERMISSION = 16;

// createTopicInRoute appends the underlying MQBrokerException's what() into the message, shaped like
// "create new topic failed: CODE: 16 DESC: ...", so this marker identifies NO_PERMISSION.
std::string noPermissionMarker() { return "CODE: " + std::to_string(NO_PERMISSION); }

std::vector<std::string> splitAddrs(const std::string& addr) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : addr) {
        if (c == ';' || c == ',') {
            if (!cur.empty()) out.push_back(cur);
            cur.clear();
        } else if (c != ' ') {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

class CountingListener : public MessageListenerConcurrently {
public:
    std::atomic<int32_t> count{0};
    std::mutex mu;
    std::vector<std::string> bodies;

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext& /*context*/) override {
        std::lock_guard<std::mutex> lk(mu);
        for (const MessageExt& m : msgs) {
            bodies.push_back(bodyOf(m));
        }
        count.fetch_add(static_cast<int32_t>(msgs.size()));
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

}  // namespace

int main(int argc, char** argv) {
    const std::string namesrv = argc > 1 ? argv[1] : std::string("127.0.0.1:9876");
    const std::string ak = argc > 2 ? argv[2] : std::string("AK_TEST");
    const std::string sk = argc > 3 ? argv[3] : std::string("SK_TEST_SECRET_12345678");
    const std::vector<std::string> nsAddrs = splitAddrs(namesrv);

    const std::string stamp = std::to_string(nowMs());
    const std::string topic = "AclLiveCpp_" + stamp;
    const std::string group = "GID_AclLiveCpp_" + stamp;

    std::printf("======================================================================\n");
    std::printf("ACL live (C++): namesrv=%s topic=%s group=%s ak=%s\n", namesrv.c_str(),
                topic.c_str(), group.c_str(), ak.c_str());
    std::printf("======================================================================\n");

    // ---------------- S1 positive: admin with credentials creates the topic ----------------
    std::printf("\nS1 admin with credentials creates the topic\n");
    {
        DefaultMQAdminExt admin("ACL_ADMIN_OK");
        admin.setNamesrvAddr(namesrv);
        admin.setCredentials(ak, sk);
        admin.start();
        try {
            admin.createTopic("TBW102", topic, 4);
            check("S1 credentialed admin creates the topic successfully", true);
        } catch (const std::exception& e) {
            check("S1 credentialed admin creates the topic successfully", false, e.what());
        }
        admin.shutdown();
    }

    // ---------------- S2 negative: credential-less admin must be rejected ----------------
    std::printf("\nS2 credential-less admin creating a topic must be rejected\n");
    {
        DefaultMQAdminExt admin("ACL_ADMIN_NO");
        admin.setNamesrvAddr(namesrv);
        admin.start();
        bool rejected = false;
        std::string detail;
        try {
            admin.createTopic("TBW102", topic + "_DENIED", 4);
            detail = "unexpectedly succeeded";
        } catch (const std::exception& e) {
            detail = e.what();
            rejected = contains(detail, noPermissionMarker());
        }
        check("S2 credential-less admin rejected by the broker (NO_PERMISSION=16)", rejected, detail);
        admin.shutdown();
    }

    // ---------------- S3 negative: a wrong secretKey must be rejected ----------------
    std::printf("\nS3 producer with a wrong secretKey must be rejected\n");
    {
        DefaultMQProducer bad("PG_AclCppBad_" + stamp);
        bad.setNamesrvAddr(namesrv);
        bad.setCredentials(ak, "WRONG_SECRET_KEY");
        bad.start();
        bool rejected = false;
        std::string detail;
        try {
            Message msg(topic, std::string("should-not-send"));
            msg.setKeys("acl-wrong");
            bad.send(msg);
            detail = "unexpectedly succeeded";
        } catch (const MQBrokerException& e) {
            detail = "code=" + std::to_string(e.getResponseCode()) + " " + e.getResponseMessage();
            rejected = e.getResponseCode() == NO_PERMISSION;
        } catch (const std::exception& e) {
            detail = e.what();
            rejected = contains(detail, noPermissionMarker());
        }
        check("S3 wrong secretKey rejected by the broker (NO_PERMISSION=16)", rejected, detail);
        bad.shutdown();
    }

    int32_t sent = 0;
    std::printf("\nS4/S5 producer/consumer with correct credentials (consumer starts before sending)\n");
    {
        auto listener = std::make_shared<CountingListener>();
        DefaultMQPushConsumer consumer(group);
        consumer.setNamesrvAddr(namesrv);
        consumer.setCredentials(ak, sk);
        consumer.subscribe(topic, "*");
        consumer.setMessageListener(listener);
        consumer.start();
        try {
            // Wait for rebalance to assign queues: the initial offset must resolve while the topic is still empty
            std::this_thread::sleep_for(std::chrono::seconds(5));

            std::string evidence;
            DefaultMQProducer prod("PG_AclCppOk_" + stamp);
            prod.setNamesrvAddr(namesrv);
            prod.setCredentials(ak, sk);
            prod.start();
            for (int i = 0; i < 3; ++i) {
                try {
                    Message msg(topic, std::string("acl-ok-") + std::to_string(i));
                    msg.setKeys("acl-ok");
                    SendResult r = prod.send(msg);
                    if (r.getSendStatus() == SendStatus::SEND_OK) {
                        ++sent;
                    }
                    evidence += "[" + std::string(sendStatusName(r.getSendStatus())) + "/"
                              + r.getMsgId() + "]";
                } catch (const std::exception& e) {
                    evidence += "[EXC:" + std::string(e.what()) + "]";
                }
            }
            prod.shutdown();
            // msgId is generated by the broker: non-empty proves the broker really accepted this signed request
            check("S4 credentialed producer sends 3 (SEND_OK)", sent == 3,
                  "sent=" + std::to_string(sent) + " " + evidence);

            // S5: heartbeat / long-poll pull / offset commit signed throughout; only a full receive proves the consume path passes auth
            const int64_t deadline = nowMs() + 30000;
            while (nowMs() < deadline && listener->count.load() < sent) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            const int32_t got = listener->count.load();
            std::string bodies;
            {
                std::lock_guard<std::mutex> lk(listener->mu);
                for (const std::string& b : listener->bodies) {
                    bodies += b + " ";
                }
            }
            check("S5 credentialed consumer receives all " + std::to_string(sent) + " messages", got == sent,
                  "got=" + std::to_string(got) + " bodies=" + bodies);
        } catch (const std::exception& e) {
            check("S4/S5 scenario execution", false, e.what());
        }
        consumer.shutdown();
    }

    // ---------------- S6 negative: bare broker RPC without credentials must be rejected ----------------
    std::printf("\nS6 credential-less direct broker RPC must be rejected\n");
    {
        MQClientInstance client("ACL_PROBE_" + stamp, nsAddrs);
        client.start();
        std::string addr;
        auto route = client.getTopicRouteData(topic);
        if (route != nullptr && !route->brokerDatas.empty()) {
            addr = route->brokerDatas[0].selectBrokerAddr();
        }
        bool rejected = false;
        std::string detail;
        if (addr.empty()) {
            detail = "no broker addr in route for topic " + topic;
        } else {
            try {
                // getConsumerListByGroup is a **bare** RPC: anything but SUCCESS throws MQBrokerException.
                // Do not use getConsumerIdListByGroup -- it swallows the exception and returns an empty vector,
                // so "rejected" and "nothing found" look identical to the caller (this once read a FAIL as a PASS).
                client.getConsumerListByGroup(group, addr, 5000);
                detail = "unexpectedly succeeded, addr=" + addr;
            } catch (const MQBrokerException& e) {
                detail = "addr=" + addr + " code=" + std::to_string(e.getResponseCode())
                       + " " + e.getResponseMessage();
                rejected = e.getResponseCode() == NO_PERMISSION;
            } catch (const std::exception& e) {
                detail = "addr=" + addr + " " + e.what();
            }
        }
        check("S6 credential-less broker RPC rejected (NO_PERMISSION=16)", rejected, detail);
        client.shutdown();
    }

    // ---------------- S7 positive: credential-less namesrv access should still succeed ----------------
    std::printf("\nS7 credential-less NameServer route query should still succeed\n");
    {
        MQClientInstance client("ACL_NS_" + stamp, nsAddrs);
        client.start();
        bool ok = false;
        std::string detail;
        try {
            ok = client.updateTopicRouteInfoFromNameServer(topic, 5000, false);
            if (!ok) detail = "returned false";
        } catch (const std::exception& e) {
            detail = e.what();
        }
        check("S7 credential-less namesrv route query succeeds", ok, detail);
        client.shutdown();
    }

    std::printf("\n===== ACL live (C++) summary =====\n");
    std::printf("  PASS=%d FAIL=%d\n", gPass, gFail);
    if (gFail == 0) {
        std::printf("  result: ACL auth (signature / rejection / namesrv compatibility) passed on the real cluster\n");
        return 0;
    }
    std::printf("  result: %d checks failed\n", gFail);
    return 1;
}
