// ACL 鉴权真机验证（对齐 Java AclClientRPCHook；与 Python verify_acl_live.py 同场景）。
// 用法：rmq_live_acl 127.0.0.1:9876 [accessKey] [secretKey]
//
// 前置：broker 开了 `authenticationEnabled=true`，并用 `initAuthenticationUser`
//       建了 accessKey/secretKey 对应的 SUPER 用户（见 /tmp/run_acl_live.sh 的
//       broker_acl.conf）。此时**所有** broker RPC 都要求合法签名：
//       AuthConfig.isAuthenticationRequired() = authenticationEnabled && !whitelist.contains(rpc),
//       而白名单默认为空。
//
// 场景：
//   S1 正向：带凭据的 admin 建 topic → 成功（管理路径签名被接受）
//   S2 反向：不带凭据的 admin 建 topic → 被拒（broker NO_PERMISSION=16）
//   S3 反向：secretKey 错误的生产者发送 → 被拒（NO_PERMISSION=16）
//   S4 正向：带凭据的生产者发 3 条 → SEND_OK（msgId 由 broker 赋值）
//   S5 正向：带凭据的消费者收满 3 条（心跳 / 长轮询拉取 / 位点提交都带签名）
//            ⚠ S4/S5 必须**先起消费者再发送**，否则 CONSUME_FROM_LAST_OFFSET 的初始位点
//              语义 + broker 异步分发会让同一时序在三语言间结果不一致（见 S4/S5 处注释）
//   S6 反向：不带凭据直连 broker 的裸 RPC → 被拒（NO_PERMISSION=16）
//   S7 正向：不带凭据走 NameServer 路由查询仍成功（钩子只作用于需要鉴权的目标）
//
// 关键事实（不要凭记忆改）：broker 侧所有鉴权失败都抛
// AbortProcessException(NO_PERMISSION=16)（broker/auth/pipeline/AuthenticationPipeline.java:53）；
// 失败原因用 ResponseCode 区分不了，只能看 remark：
//   - 签名不对 → "check signature failed."（DefaultAuthenticationHandler:68）
//   - 凭据缺失 → "User:null is not found."（DefaultAuthenticationHandler:60）
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

// broker 侧鉴权失败的统一响应码。
constexpr int32_t NO_PERMISSION = 16;

// createTopicInRoute 会把底层 MQBrokerException 的 what() 拼进消息，形如
// "create new topic failed: CODE: 16 DESC: ..."，故用这个标记识别 NO_PERMISSION。
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

    // ---------------- S1 正向：带凭据 admin 建 topic ----------------
    std::printf("\nS1 带凭据 admin 建 topic\n");
    {
        DefaultMQAdminExt admin("ACL_ADMIN_OK");
        admin.setNamesrvAddr(namesrv);
        admin.setCredentials(ak, sk);
        admin.start();
        try {
            admin.createTopic("TBW102", topic, 4);
            check("S1 带凭据 admin 建 topic 成功", true);
        } catch (const std::exception& e) {
            check("S1 带凭据 admin 建 topic 成功", false, e.what());
        }
        admin.shutdown();
    }

    // ---------------- S2 反向：无凭据 admin 必须被拒 ----------------
    std::printf("\nS2 不带凭据的 admin 建 topic 必须被拒\n");
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
        check("S2 无凭据 admin 被 broker 拒绝(NO_PERMISSION=16)", rejected, detail);
        admin.shutdown();
    }

    // ---------------- S3 反向：错误 secretKey 必须被拒 ----------------
    std::printf("\nS3 错误 secretKey 的生产者必须被拒\n");
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
        check("S3 错误 secretKey 被 broker 拒绝(NO_PERMISSION=16)", rejected, detail);
        bad.shutdown();
    }

    // ---------------- S4/S5 正向：**先起消费者再发送** ----------------
    // 顺序不能反：Java 默认 CONSUME_FROM_LAST_OFFSET 把新消费组的初始位点解析成该队列
    // **当时的** maxOffset（RebalancePushImpl.java:174-190），所以"先发、后起消费者"
    // 会（正确地）一条都收不到；而 broker 的 consumequeue 是异步分发/刷盘的，
    // "刚发完立刻查 maxOffset"还可能读到 0 —— 同一时序在三种语言间结果不一致
    // （实测 Python 收 0 条、C++/.NET 收 3 条）。先起消费者才是确定性的、只测 ACL 的顺序。
    int32_t sent = 0;
    std::printf("\nS4/S5 带正确凭据的生产者/消费者（先起消费者再发送）\n");
    {
        auto listener = std::make_shared<CountingListener>();
        DefaultMQPushConsumer consumer(group);
        consumer.setNamesrvAddr(namesrv);
        consumer.setCredentials(ak, sk);
        consumer.subscribe(topic, "*");
        consumer.setMessageListener(listener);
        consumer.start();
        try {
            // 等 rebalance 把队列分下来：初始位点必须在 topic 还空着的时候解析
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
            // msgId 由 broker 生成：非空即证明 broker 真的接受了这条签名请求
            check("S4 带凭据生产者发送 3 条(SEND_OK)", sent == 3,
                  "sent=" + std::to_string(sent) + " " + evidence);

            // S5：心跳 / 长轮询拉取 / 位点提交全程带签名，收满才算消费链路鉴权通过
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
            check("S5 带凭据消费者收满 " + std::to_string(sent) + " 条", got == sent,
                  "got=" + std::to_string(got) + " bodies=" + bodies);
        } catch (const std::exception& e) {
            check("S4/S5 场景执行", false, e.what());
        }
        consumer.shutdown();
    }

    // ---------------- S6 反向：无凭据裸 broker RPC 必须被拒 ----------------
    std::printf("\nS6 不带凭据的直接 broker RPC 必须被拒\n");
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
                // getConsumerListByGroup 是**裸** RPC：非 SUCCESS 直接抛 MQBrokerException。
                // 不能用 getConsumerIdListByGroup —— 它内部吞掉异常返回空 vector，
                // 「被拒绝」与「查不到」在调用方看来一模一样（曾因此把 FAIL 看成 PASS）。
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
        check("S6 无凭据 broker RPC 被拒绝(NO_PERMISSION=16)", rejected, detail);
        client.shutdown();
    }

    // ---------------- S7 正向：无凭据走 namesrv 仍应成功 ----------------
    std::printf("\nS7 不带凭据走 NameServer 路由查询仍应成功\n");
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
        check("S7 无凭据 namesrv 路由查询成功", ok, detail);
        client.shutdown();
    }

    std::printf("\n===== ACL live (C++) summary =====\n");
    std::printf("  PASS=%d FAIL=%d\n", gPass, gFail);
    if (gFail == 0) {
        std::printf("  result: ACL 鉴权（签名 / 拒绝 / namesrv 兼容）真机通过\n");
        return 0;
    }
    std::printf("  result: %d 项失败\n", gFail);
    return 1;
}
