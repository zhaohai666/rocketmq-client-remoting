// C++ 客户端的**真实集群**消息类型联调（对应 python/verify_message_types.py）。
//
// 覆盖 7 类消息能力，全部打真实 nameServer + broker：
//   1. 异步发送（sendAsync + SendCallback）
//   2. 顺序消息（sendBySelector 同 key 落同队列 + 顺序消费保序）
//   3. 带 Tag 消息 + 服务端 Tag 过滤
//   4. 用户属性透传
//   5. 延迟消息（setDelayTimeLevel 并校验 store_ts - born_ts >= 3000ms）
//   6. 带 Key 消息 + 按 Key 服务端查询（QUERY_MESSAGE）
//   7. 事务消息（两阶段的提交路径；完整链路见 rmq_live_transaction）+ 落库可消费
//   附：消费者心跳注册（HEART_BEAT，Python 参考实现缺此能力）
//
// 本程序自身不启动集群；调用方需先启动 nameServer(9876) + broker(10911) 且
// autoCreateTopicEnable=true。用法：
//   ./rmq_live_message_types [namesrv_addr]
// 为免 store 累积造成假象，所有 topic/消费组均带时间戳前缀。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/consumer.h"
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
std::vector<std::pair<std::string, bool>> gResults;
int gPass = 0;
int gFail = 0;

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
    void onException(const std::string& e) override {
        ++err_;
        std::lock_guard<std::mutex> lk(m_);
        errors_.push_back(e);
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
    const int64_t stamp = UtilAll::currentTimeSeconds();
    gPrefix = "MTCPP_" + std::to_string(stamp);

    std::cout << "=== C++ 客户端消息类型联调（真实集群）===" << std::endl;
    std::cout << "namesrv = " << gNamesrv << "  prefix = " << gPrefix << std::endl;

    // 日志保持干净：默认 INFO，良性长轮询超时（DEBUG）被抑制
    // 日志保持干净：默认 INFO，良性长轮询超时（DEBUG）被抑制。
    // 但若外部显式设置了 ROCKETMQ_CPP_LOG_LEVEL，则尊重它——真机排查时不改代码即可提级别。
    if (!logLevelSetFromEnv()) {
        setLogLevel(LOG_INFO);
    }

    DefaultMQProducer prod(gPrefix + "_producer");
    prod.setNamesrvAddr(gNamesrv);
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

    // ---------- 7. 事务消息（对齐 Java 的两阶段：半消息 + END_TRANSACTION + 回查）----------
    // ---------- 7.1 COMMIT ----------
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

        // 回查默认 60s 一轮；联调 broker 配了 transactionCheckInterval=3000，
        // 这里给足窗口等 broker 回查 + 提交后再投递
        ConsumerRun run = runConsumer(topic, "*", 25, false, "txcheck");
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
