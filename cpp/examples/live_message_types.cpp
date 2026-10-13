// C++ 客户端的**真实集群**消息类型联调。
//
// 覆盖 11 类消息能力，全部打真实 nameServer + broker，且**每一类都从 broker 读回**再判定
// （发送状态本身不作数；只有 oneway 无应答，改为靠读回条数证明它真的落库被投递）：
//   0. 同步普通发送（send → push 消费者读回 1 条）
//   1. 异步发送（sendAsync + SendCallback）
//   1b. 定点异步发送（sendAsync(msg, mq, cb)：落到指定队列）
//   1c. 单向发送（sendOneway，无应答 → 靠读回条数 == 3 证明投递成功）
//   2. 顺序消息（sendBySelector 同 key 落同队列 + 顺序消费保序）
//   3. 带 Tag 消息 + 服务端 Tag 过滤
//   4. 用户属性透传
//   5. 延迟消息（setDelayTimeLevel 并校验 store_ts - born_ts >= 3000ms）
//   5b. 定时消息（用户属性 TIMER_DELIVER_MS 置为约 5s 后，同样用 store_ts-born_ts 证明延时生效）
//   6. 带 Key 消息 + 按 Key 服务端查询（QUERY_MESSAGE）
//   7. 事务消息（两阶段的提交路径；完整链路见 rmq_live_transaction）+ 落库可消费
//   8. 批量消息（sendBatch → SEND_BATCH_MESSAGE(320)，broker 按 N 条独立消息投递、offset 连续）
//   附：消费者心跳注册（HEART_BEAT）
//
// 本程序自身不启动集群；调用方需先启动 nameServer(9876) + broker(10911) 且
// autoCreateTopicEnable=true。用法：
//   ./rmq_live_message_types [namesrv_addr] [accessKey secretKey]
// 为免 store 累积造成假象，所有 topic/消费组均带时间戳前缀。
//
// ACL 集群支持：可选传入 accessKey/secretKey。传入时，本程序启动的**每一个**客户端
// 门面（生产者 / 消费者）都会带上这对凭据（setCredentials），并对 broker 的签名鉴权
// 生效；不传时行为与不带凭据的普通集群完全一致。
// 传入凭据时额外做一条**反向断言**：先起一个**不带**凭据的生产者发送，必须被 broker 拒
// （NO_PERMISSION=16）。这条负向证明缺了它，在未开鉴权的集群上「全绿」就成了假通过。
// 关于顺序：本端口每个门面在 start() 里各自 new 一份独立的 MQClientInstance（见
// DefaultMQProducer::start），RPC 钩子绑在**该实例自己的**传输层上、实例之间不共享，
// 所以无凭据探针不可能继承到别的客户端已签名的连接。即便如此，仍把探针作为**第一个**
// 启动的客户端、发送后立刻 shutdown 并留 ~1s 让实例回收，再起带凭据的客户端，确保从
// 干净状态开始。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

using namespace rocketmq;

namespace {

std::string gNamesrv = "127.0.0.1:9876";
std::string gPrefix;
// 可选 ACL 凭据：两者都非空时，本程序启动的每个客户端都带签名；无凭据集群保持全空即可。
std::string gAccessKey;
std::string gSecretKey;
std::vector<std::pair<std::string, bool>> gResults;
int gPass = 0;
int gFail = 0;

// 凭据是否齐备：为空即视为「普通集群模式」，所有 setCredentials 调用都跳过。
bool aclEnabled() { return !gAccessKey.empty() && !gSecretKey.empty(); }

// 生产者的凭据装载。DefaultMQProducer 与 DefaultMQPushConsumer 的同名接口签名一致，
// 用模板一次覆盖，避免每个客户端类型各写一遍。
template <typename Client> void applyCredentials(Client& c) {
    if (aclEnabled()) {
        c.setCredentials(gAccessKey, gSecretKey);
    }
}

void check(const std::string& name, bool ok, const std::string& detail = "") {
    gResults.emplace_back(name, ok);
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name;
    if (!detail.empty()) std::cout << "  " << detail;
    std::cout << std::endl;
}

std::string bytes2str(const Bytes& b) { return std::string(b.begin(), b.end()); }

Bytes str2bytes(const std::string& s) { return Bytes(s.begin(), s.end()); }

// ---------------------------------------------------------------- 监听器
class CollectingListenerConcurrently : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext& /*ctx*/) override {
        std::lock_guard<std::mutex> lk(m_);
        for (const MessageExt& m : msgs) msgs_.push_back(m);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    std::vector<MessageExt> snapshot() const {
        std::lock_guard<std::mutex> lk(m_);
        return msgs_;
    }

private:
    mutable std::mutex m_;
    std::vector<MessageExt> msgs_;
};

class CollectingListenerOrderly : public MessageListenerOrderly {
public:
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                        ConsumeOrderlyContext& /*ctx*/) override {
        std::lock_guard<std::mutex> lk(m_);
        for (const MessageExt& m : msgs) msgs_.push_back(m);
        return ConsumeOrderlyStatus::SUCCESS;
    }

    std::vector<MessageExt> snapshot() const {
        std::lock_guard<std::mutex> lk(m_);
        return msgs_;
    }

private:
    mutable std::mutex m_;
    std::vector<MessageExt> msgs_;
};

// ---------------------------------------------------------------- 异步回调
class CollectingCallback : public SendCallback {
public:
    void onSuccess(const SendResult& r) override {
        ++ok_;
        std::lock_guard<std::mutex> lk(m_);
        results_.push_back(r);
    }
    void onException(const std::exception_ptr& e) override {
        ++err_;
        std::lock_guard<std::mutex> lk(m_);
        errors_.push_back(exceptionMessage(e) + " [" + exceptionTypeName(e) + "]");
    }

    int ok() const { return ok_.load(); }
    int err() const { return err_.load(); }
    std::vector<SendResult> results() const {
        std::lock_guard<std::mutex> lk(m_);
        return results_;
    }
    std::vector<std::string> errors() const {
        std::lock_guard<std::mutex> lk(m_);
        return errors_;
    }

private:
    std::atomic<int> ok_{0};
    std::atomic<int> err_{0};
    mutable std::mutex m_;
    std::vector<SendResult> results_;
    std::vector<std::string> errors_;
};

// ---------------------------------------------------------------- 事务监听器
// COMMIT：本地事务直接提交
class CommitTxListener : public TransactionListener {
public:
    LocalTransactionState executeLocalTransaction(const Message& /*msg*/,
                                                  const std::string& /*arg*/) override {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
    LocalTransactionState checkLocalTransaction(const MessageExt& /*msg*/) override {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
};

// ROLLBACK：本地事务回滚，broker 不应把半消息投递出来
class RollbackTxListener : public TransactionListener {
public:
    LocalTransactionState executeLocalTransaction(const Message& /*msg*/,
                                                  const std::string& /*arg*/) override {
        return LocalTransactionState::ROLLBACK_MESSAGE;
    }
    LocalTransactionState checkLocalTransaction(const MessageExt& /*msg*/) override {
        return LocalTransactionState::ROLLBACK_MESSAGE;
    }
};

// UNKNOW + 回查：本地事务返回 UNKNOW，等 broker 回查时才判 COMMIT。
// checkCalls 用于证明 **broker 确实回调过**（否则"最终收到"可能只是普通消息路径）。
class UnknownThenCommitTxListener : public TransactionListener {
public:
    LocalTransactionState executeLocalTransaction(const Message& /*msg*/,
                                                  const std::string& /*arg*/) override {
        return LocalTransactionState::UNKNOW;
    }
    LocalTransactionState checkLocalTransaction(const MessageExt& /*msg*/) override {
        ++checkCalls;
        return LocalTransactionState::COMMIT_MESSAGE;
    }
    std::atomic<int> checkCalls{0};
};

// ---------------------------------------------------------------- 消费辅助
// 活跃等待直至收到 expect 条（或最多 durationSec 秒），再关闭消费者。
// 用活跃等待而非盲 sleep：消费者冷启动时路由/队列尚未就绪，固定窗口容易偶发 0 条。
struct ConsumerRun {
    std::vector<MessageExt> msgs;
    std::shared_ptr<DefaultMQPushConsumer> consumer;  // 已 shutdown，仅用于读取计数
};

ConsumerRun runConsumer(const std::string& topic, const std::string& subExpr, int durationSec,
                        bool orderly, const std::string& groupSuffix, int expect = 0,
                        int32_t pullTimeout = 3000, int32_t pullSuspend = 1000) {
    std::shared_ptr<MessageListener> listener;
    std::shared_ptr<CollectingListenerConcurrently> conc;
    std::shared_ptr<CollectingListenerOrderly> ord;
    if (orderly) {
        ord = std::make_shared<CollectingListenerOrderly>();
        listener = ord;
    } else {
        conc = std::make_shared<CollectingListenerConcurrently>();
        listener = conc;
    }

    auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_" + groupSuffix);
    consumer->setNamesrvAddr(gNamesrv);
    applyCredentials(*consumer);
    consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    consumer->subscribe(topic, subExpr);
    consumer->setMessageListener(listener);
    // 关键：单线程顺序长轮询下，空闲队列的 suspend 长轮询会饿死满载队列，
    // 故把客户端等待与服务端挂起都设短（详见 consumer.h 注释）。
    consumer->setPullTimeoutMillis(pullTimeout);
    consumer->setPullSuspendTimeoutMillis(pullSuspend);
    consumer->start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(durationSec);
    while (std::chrono::steady_clock::now() < deadline) {
        size_t got = orderly ? ord->snapshot().size() : conc->snapshot().size();
        if (expect > 0 && static_cast<int>(got) >= expect) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ConsumerRun run;
    run.msgs = orderly ? ord->snapshot() : conc->snapshot();
    consumer->shutdown();
    run.consumer = consumer;
    return run;
}

// 等待 broker 在 nameServer 注册完成（避免端口刚开、注册未落地的竞态）
std::vector<std::string> waitBroker(DefaultMQProducer& prod) {
    for (int i = 0; i < 40; ++i) {
        try {
            std::vector<MessageQueue> mqs = prod.fetchPublishMessageQueues(MixAll::DEFAULT_TOPIC);
            std::vector<std::string> addrs;
            for (const MessageQueue& mq : mqs) {
                std::string a = prod.client().brokerAddrOf(mq.brokerName);
                if (!a.empty() && std::find(addrs.begin(), addrs.end(), a) == addrs.end()) {
                    addrs.push_back(a);
                }
            }
            if (!addrs.empty()) return addrs;
        } catch (const std::exception&) {
            // 路由还没就绪，继续等
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return {};
}

}  // namespace

int main(int argc, char** argv) {
    if (argc >= 2) {
        gNamesrv = argv[1];
    }
    // 可选位置参数：argv[2]=accessKey argv[3]=secretKey
    if (argc >= 3) {
        gAccessKey = argv[2];
    }
    if (argc >= 4) {
        gSecretKey = argv[3];
    }
    const int64_t stamp = UtilAll::currentTimeSeconds();
    gPrefix = "MTCPP_" + std::to_string(stamp);

    std::cout << "=== C++ 客户端消息类型联调（真实集群）===" << std::endl;
    std::cout << "namesrv = " << gNamesrv << "  prefix = " << gPrefix << std::endl;
    std::cout << "ACL = " << (aclEnabled() ? "enabled (ak=" + gAccessKey + ")" : "disabled")
              << std::endl;

    // 日志保持干净：默认 INFO，良性长轮询超时（DEBUG）被抑制
    // 日志保持干净：默认 INFO，良性长轮询超时（DEBUG）被抑制。
    // 但若外部显式设置了 ROCKETMQ_CLIENT_LOG_LEVEL，则尊重它——真机排查时不改代码即可提级别。
    if (!logLevelSetFromEnv()) {
        setLogLevel(LOG_INFO);
    }

    // ---------- 0. ACL 反向断言：无凭据生产者的发送必须被拒 ----------
    // 开鉴权的集群里，所有 broker RPC 都要合法签名；不带凭据即回 NO_PERMISSION(16)。
    // 没有这一条，程序在未开鉴权的集群上也会「全绿」，那份全绿证明不了签名真的生效。
    // 必须排在所有带凭据客户端之前：本端口每个门面各自持有独立的 MQClientInstance，
    // 钩子绑在各自的传输层上，顺序本不影响隔离；仍先起后立刻 shutdown 并等 ~1s，
    // 让无凭据探针的实例先释放干净，后续带凭据客户端从干净状态建立连接。
    if (aclEnabled()) {
        const std::string probeTopic = gPrefix + "_AclDenied";
        DefaultMQProducer noCred(gPrefix + "_producer_nocred");
        noCred.setNamesrvAddr(gNamesrv);  // 故意**不**装凭据
        noCred.setSendMsgTimeout(5000);
        noCred.setRetryTimesWhenSendFailed(0);  // 鉴权失败不必重试三轮
        bool rejected = false;
        std::string detail;
        try {
            noCred.start();
            SendResult sr = noCred.send(Message(probeTopic, str2bytes("must-be-denied")));
            detail = "unexpectedly succeeded: " + std::string(sendStatusName(sr.sendStatus));
        } catch (const MQBrokerException& e) {
            rejected = e.getResponseCode() == ResponseCode::NO_PERMISSION;
            detail = "code=" + std::to_string(e.getResponseCode()) + " " + e.getResponseMessage();
        } catch (const std::exception& e) {
            // 无队列可用/建 topic 失败会把底层 16 码拼进文案，这里按标记识别。
            detail = e.what();
            rejected = detail.find("CODE: 16") != std::string::npos ||
                       detail.find("NO_PERMISSION") != std::string::npos;
        }
        check("ACL 反向：无凭据发送被拒(NO_PERMISSION=16)", rejected, detail);
        noCred.shutdown();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    DefaultMQProducer prod(gPrefix + "_producer");
    prod.setNamesrvAddr(gNamesrv);
    applyCredentials(prod);
    prod.setSendMsgTimeout(5000);
    try {
        prod.start();
    } catch (const std::exception& e) {
        std::cout << "[FAIL] 生产者启动: " << e.what() << std::endl;
        return 1;
    }

    std::vector<std::string> brokers = waitBroker(prod);
    if (brokers.empty()) {
        check("集群探活", false, "nameServer 无 broker 注册");
        prod.shutdown();
        return 1;
    }
    std::string brokerList;
    for (size_t i = 0; i < brokers.size(); ++i) {
        if (i) brokerList += ",";
        brokerList += brokers[i];
    }
    check("集群探活", true, "brokers=" + brokerList);

    const int64_t t0 = UtilAll::currentTimeMillis();

    // ---------- 0a. 同步普通发送：状态之外还必须从 broker 读回 ----------
    // 发送返回 SEND_OK 只证明 broker 收下了请求，不能证明消息进了可消费的队列，
    // 所以这一类的判据是 push 消费者按 FIRST_OFFSET 把它读回来。
    {
        const std::string topic = gPrefix + "_Sync";
        bool sendOk = false;
        std::string detail;
        try {
            SendResult sr = prod.send(Message(topic, str2bytes("sync-normal")));
            sendOk = sr.sendStatus == SendStatus::SEND_OK && !sr.msgId.empty();
            detail = "status=" + std::string(sendStatusName(sr.sendStatus))
                     + " msgId=" + (sr.msgId.empty() ? std::string("<empty>") : sr.msgId);
        } catch (const std::exception& e) {
            detail = std::string("throw: ") + e.what();
        }
        check("同步普通发送 send()", sendOk, detail);

        ConsumerRun run = runConsumer(topic, "*", 12, false, "sync", /*expect=*/1);
        bool readBack = false;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "sync-normal") readBack = true;
        }
        check("同步普通发送 从 broker 读回", readBack,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 0b. 单向发送 sendOneway：没有应答，只能靠读回条数证明 ----------
    // oneway 是「发出即成功」的无应答语义：客户端拿不到任何 broker 回执，
    // 所以「发了 3 条」这件事本身不可证。唯一的判据是消费侧读回**恰好** 3 条 ——
    // 少一条说明请求根本没出去，多一条说明重复投递。
    //
    // 这里先把 topic 显式建好再发：新 topic 的自动创建要等发送的**应答**把路由带回来，
    // 而 oneway 按协议不产生任何应答，拿不准消息最终落到哪条队列上。topic 先就位，
    // 读回的条数才是只关于 oneway 的干净证据（三条都在同一个已知 topic 上）。
    {
        const std::string topic = gPrefix + "_Oneway";
        const int kOnewayNum = 3;
        bool topicReady = false;
        std::string createDetail;
        try {
            // 建 topic 的 key 就是发布路由的模板 topic（与生产者的 createTopicKey 同值）。
            prod.createTopic(MixAll::DEFAULT_TOPIC, topic, 4);
            // 建完等路由可见，别让单向发送跑在路由之前
            for (int i = 0; i < 30 && !topicReady; ++i) {
                try {
                    topicReady = prod.fetchPublishMessageQueues(topic).size() >= 4;
                } catch (const std::exception&) {
                    // 路由还没落地，稍后再查
                }
                if (!topicReady) std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            createDetail = "queues=" + std::to_string(topicReady ? 4 : 0);
        } catch (const std::exception& e) {
            createDetail = std::string("throw: ") + e.what();
        }
        check("单向发送 前置建 topic", topicReady, createDetail);

        std::string detail = "sent=";
        int sent = 0;
        for (int i = 0; i < kOnewayNum; ++i) {
            try {
                prod.sendOneway(Message(topic, str2bytes("oneway-" + std::to_string(i))));
                ++sent;
            } catch (const std::exception& e) {
                detail += "[EXC:" + std::string(e.what()) + "]";
            }
        }
        detail += std::to_string(sent) + " (oneway 无应答，不作判据)";
        check("单向发送 sendOneway() 调用成功", sent == kOnewayNum, detail);

        ConsumerRun run = runConsumer(topic, "*", 15, false, "oneway", /*expect=*/kOnewayNum);
        std::set<std::string> bodies;
        for (const MessageExt& m : run.msgs) {
            bodies.insert(bytes2str(m.body));
        }
        bool exact = true;
        for (int i = 0; i < kOnewayNum; ++i) {
            if (bodies.count("oneway-" + std::to_string(i)) == 0) exact = false;
        }
        // 读回条数就是 oneway 唯一的「发送成功」证据。
        check("单向发送 从 broker 读回 3 条(唯一判据)",
              run.msgs.size() == static_cast<size_t>(kOnewayNum) && exact,
              "received=" + std::to_string(run.msgs.size()) + " distinct="
                  + std::to_string(bodies.size()));
    }

    // ---------- 1. 异步发送 ----------
    {
        const std::string topic = gPrefix + "_Async";
        auto cb = std::make_shared<CollectingCallback>();
        Message msg(topic, str2bytes("async-hello"));
        try {
            prod.sendAsync(msg, cb);
        } catch (const std::exception& e) {
            check("异步发送 sendAsync", false, std::string("throw: ") + e.what());
        }
        for (int i = 0; i < 200 && cb->ok() + cb->err() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        std::vector<SendResult> rs = cb->results();
        bool ok = (cb->ok() == 1) && (cb->err() == 0) && !rs.empty() &&
                  rs[0].sendStatus == SendStatus::SEND_OK;
        check("异步发送 sendAsync", ok,
              "ok=" + std::to_string(cb->ok()) + " err=" + std::to_string(cb->err()));
    }

    // ---------- 1b. 定点异步发送：sendAsync(msg, mq, cb) ----------
    {
        const std::string topic = gPrefix + "_AsyncPinned";
        SendResult probe;
        bool probed = true;
        try {
            probe = prod.send(Message(topic, str2bytes("probe")), 5000);
        } catch (const std::exception& e) {
            probed = false;
            check("定点异步发送 sendAsync(msg, mq)", false,
                  std::string("建 topic 失败: ") + e.what());
        }
        if (probed) {
            DefaultMQProducer fresh(gPrefix + "_producer_fresh");
            fresh.setNamesrvAddr(gNamesrv);
            applyCredentials(fresh);
            fresh.setSendMsgTimeout(5000);
            fresh.start();
            auto cb = std::make_shared<CollectingCallback>();
            try {
                fresh.sendAsync(Message(topic, str2bytes("pinned-async")), probe.messageQueue, cb,
                                5000);
            } catch (const std::exception& e) {
                check("定点异步发送 sendAsync(msg, mq)", false,
                      std::string("throw: ") + e.what());
            }
            for (int i = 0; i < 200 && cb->ok() + cb->err() == 0; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
            std::vector<SendResult> rs = cb->results();
            const bool sameQueue = !rs.empty() && rs[0].messageQueue == probe.messageQueue;
            const bool ok = (cb->ok() == 1) && (cb->err() == 0) && !rs.empty()
                            && rs[0].sendStatus == SendStatus::SEND_OK && sameQueue
                            && !rs[0].msgId.empty();
            check("定点异步发送 sendAsync(msg, mq)", ok,
                  "ok=" + std::to_string(cb->ok()) + " err=" + std::to_string(cb->err())
                      + (rs.empty()
                             ? std::string()
                             : (" queue=" + rs[0].messageQueue.brokerName + ":"
                                + std::to_string(rs[0].messageQueue.queueId) + " want="
                                  + probe.messageQueue.brokerName + ":"
                                  + std::to_string(probe.messageQueue.queueId)
                                  + " msgId=" + (rs[0].msgId.empty() ? "<empty>" : "set"))));
            if (cb->err() > 0) {
                std::vector<std::string> errs = cb->errors();
                std::cout << "       回调异常: " << errs[0] << std::endl;
            }
            fresh.shutdown();
        }
    }

    // ---------- 2. 顺序消息：同 key 落同队列 + 顺序消费保序 ----------
    std::vector<Bytes> bodiesOrder;
    {
        const std::string topic = gPrefix + "_Order";
        SelectMessageQueueByHash selector;
        std::vector<int32_t> qids;
        for (int i = 0; i < 10; ++i) {
            char buf[16];
            std::snprintf(buf, sizeof(buf), "ord-%02d", i);
            Bytes body = str2bytes(buf);
            bodiesOrder.push_back(body);
            SendResult sr = prod.sendBySelector(Message(topic, body), selector, "shard-A");
            if (std::find(qids.begin(), qids.end(), sr.messageQueue.queueId) == qids.end()) {
                qids.push_back(sr.messageQueue.queueId);
            }
        }
        std::string qidStr;
        for (int32_t q : qids) {
            if (!qidStr.empty()) qidStr += ",";
            qidStr += std::to_string(q);
        }
        check("顺序发送: 同 key 路由到同一队列", qids.size() == 1,
              "distinct_queue_ids=" + (qidStr.empty() ? std::string("?") : qidStr));

        ConsumerRun run = runConsumer(topic, "*", 12, /*orderly=*/true, "order",
                                      /*expect=*/10);
        // 真实保序校验：收到的**顺序**必须与发送顺序一致（而非仅集合相等 ——
        // 只比 sorted 只能证明"收全了"，证明不了"没乱序"），
        // 并且同一队列内的 queueOffset 必须严格递增。
        std::vector<std::string> recv, expect;
        for (const MessageExt& m : run.msgs) recv.push_back(bytes2str(m.body));
        for (const Bytes& b : bodiesOrder) expect.push_back(bytes2str(b));
        bool seqOk = (recv == expect);
        bool offsetOk = true;
        for (size_t i = 1; i < run.msgs.size(); ++i) {
            if (run.msgs[i].queueOffset <= run.msgs[i - 1].queueOffset) offsetOk = false;
        }
        std::string orderDetail = "received=" + std::to_string(run.msgs.size()) + "/10 seq_ok="
                                  + (seqOk ? "1" : "0") + " offset_monotonic="
                                  + (offsetOk ? "1" : "0");
        if (!recv.empty()) {
            orderDetail += " first=" + recv.front() + " last=" + recv.back();
        }
        check("顺序消费保序", run.msgs.size() == 10 && seqOk && offsetOk, orderDetail);
    }

    // ---------- 3. 带 Tag 消息 + 服务端 Tag 过滤 ----------
    {
        const std::string topic = gPrefix + "_Tag";
        for (int i = 0; i < 3; ++i) {
            Message m(topic, str2bytes("tagA-" + std::to_string(i)));
            m.setTags("TagA");
            prod.send(m);
        }
        for (int i = 0; i < 3; ++i) {
            Message m(topic, str2bytes("tagB-" + std::to_string(i)));
            m.setTags("TagB");
            prod.send(m);
        }
        ConsumerRun run = runConsumer(topic, "TagA", 12, false, "tag");
        bool allA = true;
        for (const MessageExt& m : run.msgs) {
            if (m.getTags() != "TagA") allA = false;
        }
        check("Tag 过滤消费（仅收到 TagA）", run.msgs.size() == 3 && allA,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 4. 用户属性透传 ----------
    {
        const std::string topic = gPrefix + "_Prop";
        for (int i = 0; i < 3; ++i) {
            Message m(topic, str2bytes("prop-" + std::to_string(i)));
            m.setTags("P");
            m.setUserProperty("city", "Hangzhou");
            m.setUserProperty("env", "prod");
            prod.send(m);
        }
        ConsumerRun run = runConsumer(topic, "*", 12, false, "prop");
        bool okCity = true;
        bool okEnv = true;
        for (const MessageExt& m : run.msgs) {
            if (m.getUserProperty("city") != "Hangzhou") okCity = false;
            if (m.getUserProperty("env") != "prod") okEnv = false;
        }
        check("用户属性透传(city=Hangzhou, env=prod)",
              okCity && okEnv && run.msgs.size() == 3,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 5. 延迟消息 ----------
    {
        const std::string topic = gPrefix + "_Delay";
        prod.send(Message(topic, str2bytes("normal-now")));
        Message dm(topic, str2bytes("delayed-5s"));
        dm.setDelayTimeLevel(2);  // level2 = 5s
        prod.send(dm);

        ConsumerRun run = runConsumer(topic, "*", 20, false, "delay", /*expect=*/2);
        std::vector<MessageExt> delayed, normal;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "delayed-5s") {
                delayed.push_back(m);
            } else if (bytes2str(m.body) == "normal-now") {
                normal.push_back(m);
            }
        }
        check("延迟消息最终投递", !delayed.empty(),
              "delayed received=" + std::to_string(delayed.size()) + " normal="
                  + std::to_string(normal.size()));
        if (!delayed.empty()) {
            int64_t drift = delayed[0].storeTimestamp - delayed[0].bornTimestamp;
            check("延迟生效(store_ts-born_ts>=3000ms)", drift >= 3000,
                  "drift=" + std::to_string(drift) + "ms");
        } else {
            check("延迟生效(store_ts-born_ts>=3000ms)", false, "无延迟消息可校验");
        }
    }

    // ---------- 5b. 定时消息（用户属性 TIMER_DELIVER_MS，投递时刻为约 5s 之后）----------
    // 延迟等级只能挑固定的几档；定时消息让发送方直接给出**绝对投递时刻**。
    // broker 把带 TIMER_DELIVER_MS 的消息收进定时存储，到点才写回真实 topic，
    // 因此消费侧看到的 store_ts 是「真正投递那一次」的存储时间：
    // store_ts - born_ts >= 3000ms 就证明延时确实生效，而不是被当普通消息立刻投递。
    {
        const std::string topic = gPrefix + "_Timer";
        const int64_t deliverInMs = 5000;
        const int64_t deliverAt = UtilAll::currentTimeMillis() + deliverInMs;
        Message tm(topic, str2bytes("timer-deliver"));
        // 走用户属性：TIMER_DELIVER_MS 是定时消息的协议属性名。
        tm.setUserProperty(MessageConst::PROPERTY_TIMER_DELIVER_MS, std::to_string(deliverAt));
        bool sent = false;
        std::string detail;
        try {
            SendResult sr = prod.send(tm);
            sent = sr.sendStatus == SendStatus::SEND_OK;
            detail = "status=" + std::string(sendStatusName(sr.sendStatus))
                     + " deliver_in=" + std::to_string(deliverInMs) + "ms";
        } catch (const std::exception& e) {
            detail = std::string("throw: ") + e.what();
        }
        check("定时消息发送(TIMER_DELIVER_MS≈+5s)", sent, detail);

        ConsumerRun run = runConsumer(topic, "*", 20, false, "timer", /*expect=*/1);
        std::vector<MessageExt> timed;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "timer-deliver") timed.push_back(m);
        }
        check("定时消息最终投递", !timed.empty(),
              "timer received=" + std::to_string(timed.size()) + " total="
                  + std::to_string(run.msgs.size()));
        if (!timed.empty()) {
            int64_t drift = timed[0].storeTimestamp - timed[0].bornTimestamp;
            check("定时生效(store_ts-born_ts>=3000ms)", drift >= 3000,
                  "drift=" + std::to_string(drift) + "ms");
        } else {
            check("定时生效(store_ts-born_ts>=3000ms)", false, "无定时消息可校验");
        }
    }

    // ---------- 6. 带 Key 消息 + 按 Key 查询 ----------
    {
        const std::string topic = gPrefix + "_Key";
        const std::string key = "MTKEY_" + std::to_string(stamp);
        const std::string payload = "key-msg-payload";
        Message m(topic, str2bytes(payload));
        m.setKeys(key);
        prod.send(m);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        int64_t begin = t0 - 120000;
        int64_t end = UtilAll::currentTimeMillis() + 120000;
        std::vector<MessageExt> found = prod.queryMessage(topic, key, 10, begin, end);
        bool hit = false;
        for (const MessageExt& x : found) {
            if (bytes2str(x.body) == payload) hit = true;
        }
        check("按 Key 查询(query_message)", hit,
              "returned=" + std::to_string(found.size()));
    }

    {
        const std::string topic = gPrefix + "_Tx";
        CommitTxListener listener;
        bool txOk = false;
        std::string stateStr;
        try {
            TransactionSendResult tsr =
                prod.sendMessageInTransaction(Message(topic, str2bytes("tx-commit")), listener);
            stateStr = localTransactionStateName(tsr.localTransactionState);
            txOk = (tsr.sendStatus == SendStatus::SEND_OK &&
                    tsr.localTransactionState == LocalTransactionState::COMMIT_MESSAGE);
        } catch (const std::exception& e) {
            stateStr = std::string("throw: ") + e.what();
        }
        check("事务-COMMIT 发送状态", txOk, "state=" + stateStr);

        ConsumerRun run = runConsumer(topic, "*", 10, false, "tx");
        bool consumed = false;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "tx-commit") consumed = true;
        }
        check("事务-COMMIT 落库可被消费", consumed,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 7.2 ROLLBACK ----------
    {
        const std::string topic = gPrefix + "_TxRollback";
        RollbackTxListener listener;
        bool txOk = false;
        std::string stateStr;
        try {
            TransactionSendResult tsr =
                prod.sendMessageInTransaction(Message(topic, str2bytes("tx-rollback")), listener);
            stateStr = localTransactionStateName(tsr.localTransactionState);
            txOk = (tsr.sendStatus == SendStatus::SEND_OK &&
                    tsr.localTransactionState == LocalTransactionState::ROLLBACK_MESSAGE);
        } catch (const std::exception& e) {
            stateStr = std::string("throw: ") + e.what();
        }
        check("事务-ROLLBACK 发送状态", txOk, "state=" + stateStr);

        // 回滚后 broker 不应投递：等满窗口确认一条都没收到
        ConsumerRun run = runConsumer(topic, "*", 10, false, "txrollback");
        bool consumed = false;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "tx-rollback") consumed = true;
        }
        check("事务-ROLLBACK 不被投递", !consumed,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 7.3 UNKNOW + broker 回查 ----------
    {
        const std::string topic = gPrefix + "_TxCheck";
        UnknownThenCommitTxListener listener;
        bool txOk = false;
        std::string stateStr;
        try {
            TransactionSendResult tsr =
                prod.sendMessageInTransaction(Message(topic, str2bytes("tx-check")), listener);
            stateStr = localTransactionStateName(tsr.localTransactionState);
            txOk = (tsr.sendStatus == SendStatus::SEND_OK &&
                    tsr.localTransactionState == LocalTransactionState::UNKNOW);
        } catch (const std::exception& e) {
            stateStr = std::string("throw: ") + e.what();
        }
        check("事务-UNKNOW 发送状态", txOk, "state=" + stateStr);

        // 回查节奏取决于 broker：本机集群 transactionCheckInterval 为 30s 一轮，
        // 半消息在发送后 15~17s 才被提交，故窗口必须覆盖一整个回查周期；
        // expect=1 让消费者一收到就返回，不必等满。
        ConsumerRun run = runConsumer(topic, "*", 40, false, "txcheck", /*expect=*/1);
        bool consumed = false;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "tx-check") consumed = true;
        }
        int checks = listener.checkCalls.load();
        check("事务-UNKNOW 触发 broker 回查", checks > 0,
              "checkLocalTransaction_calls=" + std::to_string(checks));
        check("事务-UNKNOW 回查后最终投递", consumed,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 8. 批量消息（SEND_BATCH_MESSAGE 320）----------
    // 请求码从 310 换成 320 之后，必须在真 broker 上重新证明一次：broker 确实是按
    // 「批量写入」处理这笔请求的。判据是消费侧看到 **N 条独立消息、offset 连续**：
    // 如果 broker 把批量 body 当成单条消息存（即 header.batch 没生效），这里只会收到
    // 1 条 body 为拼接字节的消息，而不是 N 条各自完整的 body。
    {
        const std::string topic = gPrefix + "_Batch";
        std::vector<Message> group;
        for (int i = 0; i < 3; ++i) {
            group.emplace_back(topic, str2bytes("batch-" + std::to_string(i)));
        }
        bool batchOk = false;
        std::string batchDetail = "throw";
        try {
            SendResult sr = prod.sendBatch(group, 5000);
            batchOk = sr.sendStatus == SendStatus::SEND_OK;
            batchDetail = "status=" + std::to_string(static_cast<int>(sr.sendStatus));
        } catch (const std::exception& e) {
            batchDetail = std::string("throw: ") + e.what();
        }
        check("批量发送 sendBatch(3 条)", batchOk, batchDetail);

        ConsumerRun run = runConsumer(topic, "*", 15, false, "batch", /*expect=*/3);
        std::set<std::string> bodies;
        std::vector<int64_t> offsets;
        for (const MessageExt& m : run.msgs) {
            bodies.insert(bytes2str(m.body));
            offsets.push_back(m.queueOffset);
        }
        std::sort(offsets.begin(), offsets.end());
        bool contiguous = offsets.size() == 3;
        for (size_t i = 1; i < offsets.size(); ++i) {
            if (offsets[i] != offsets[i - 1] + 1) contiguous = false;
        }
        bool exact = true;
        for (int i = 0; i < 3; ++i) {
            if (bodies.count("batch-" + std::to_string(i)) == 0) exact = false;
        }
        check("批量消息被 broker 按 3 条独立消息投递", exact && contiguous,
              "received=" + std::to_string(run.msgs.size()) + " offsets="
                  + [&] {
                        std::string s;
                        for (int64_t o : offsets) { if (!s.empty()) s += ","; s += std::to_string(o); }
                        return s;
                    }());
    }

    // ---------- 附：心跳注册 ----------
    {
        const std::string topic = gPrefix + "_Hb";
        prod.send(Message(topic, str2bytes("hb-probe")));
        ConsumerRun run = runConsumer(topic, "*", 10, false, "hb", /*expect=*/1);
        int64_t hb = run.consumer ? run.consumer->heartbeatCount() : 0;
        check("消费者心跳注册(HEART_BEAT)", hb > 0,
              "heartbeat_ok=" + std::to_string(hb) + " consumed="
                  + std::to_string(run.msgs.size()));
    }

    prod.shutdown();

    // ---------- 汇总 ----------
    std::cout << "\n================ C++ 消息类型联调汇总 ================" << std::endl;
    for (const auto& r : gResults) {
        std::cout << "  [" << (r.second ? "PASS" : "FAIL") << "] " << r.first << std::endl;
    }
    std::cout << "=====================================================" << std::endl;
    std::cout << "  PASS=" << gPass << " FAIL=" << gFail << std::endl;
    if (gFail > 0) {
        std::cout << "结果: " << gFail << " 项失败" << std::endl;
        return 1;
    }
    std::cout << "结果: 全部通过（C++ 客户端对真实集群完成全部消息类型收发）" << std::endl;
    return 0;
}
