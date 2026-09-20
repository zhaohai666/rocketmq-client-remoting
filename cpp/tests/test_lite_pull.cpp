// 轻量拉取消费者（DefaultLitePullConsumer）+ 220/221/309 body 单测。
//
// 不起网络：只测「start 之前」的纯状态行为（subscribe/assign/seek/poll/committed）、
// 以及 221/309 应答体的 wire 形状（fastjson2 内联对象键 / null 字段）——
// 这些形状 broker/admin 端会按 Java 语义解释，错一个键名就静默丢字段。
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/remoting/protocol/body.h"

using namespace rocketmq;

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << "\n";                           \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b, msg)                                                    \
    do {                                                                       \
        if ((a) == (b)) {                                                      \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << " (actual=" << (a)             \
                      << ", expect=" << (b) << ")\n";                          \
        }                                                                      \
    } while (0)

// ---------------------------------------------------------------- start 之前的纯状态行为
static void testLitePullPreStartBehavior() {
    // Java 默认消费组
    DefaultLitePullConsumer c;
    CHECK_EQ(c.consumerGroup(), std::string(MixAll::DEFAULT_CONSUMER_GROUP),
             "LitePull 默认消费组 = DEFAULT_CONSUMER");
    CHECK(!c.isStarted(), "构造后未启动");

    // start 前没配 namesrv / 没订阅：start() 必须显式报错（不能静默起空转线程）
    bool threw = false;
    try {
        c.start();
    } catch (const MQClientException&) {
        threw = true;
    } catch (...) {
        threw = true;
    }
    CHECK(threw, "无订阅无 namesrv 时 start() 抛异常");

    // subscribe（未启动也要能登记，Java 同语义）
    c.subscribe("MyTopic", "TagA || TagB");

    // assign（未启动时 resolveInitialOffset 会失败但必须被吞掉，不能崩）
    MessageQueue mq("MyTopic", "broker-a", 3);
    c.assign({mq});
    CHECK_EQ(c.assignment().size(), static_cast<size_t>(1), "assign 后 assignment 有 1 个队列");
    CHECK_EQ(c.assignment()[0].queueId, 3, "assignment 队列 id 一致");

    // 未启动时 committed 必须回 -1（没有 client 可查）
    CHECK_EQ(c.committed(mq), static_cast<int64_t>(-1), "未启动 committed = -1");

    // poll 空缓冲：短超时返回空列表（不阻塞、不崩）
    std::vector<MessageExt> got = c.poll(20);
    CHECK(got.empty(), "空缓冲 poll 返回空");

    // seek 未启动也要能登记位点（不查 broker）
    c.seek(mq, 42);
    CHECK_EQ(c.assignment().size(), static_cast<size_t>(1), "seek 后 assignment 不变");

    // pause / resume 未启动不崩
    c.pause({mq});
    c.resume({mq});

    // 重复 subscribe 覆盖旧表达式（Java 同语义：subscriptionTable.put）
    c.subscribe("MyTopic", "TagC");
    // 不崩即通过（表达式存私有表，真机验证覆盖）
    ++g_pass;
}

// ---------------------------------------------------------------- consumeTimestamp（Java 格式）
static void testLitePullConsumeTimestamp() {
    // 组名要合法且不能是保留的 DEFAULT_CONSUMER：checkConfig 排在起点校验之前，
    // 用默认组只会测到组名那条错误（见 Validators::checkGroup 的接线）。
    DefaultLitePullConsumer c("LitePullTsGroup");
    // Java DefaultLitePullConsumer.java:168：默认是 now-30min 的 14 位 yyyyMMddHHmmss，不是空串
    CHECK_EQ(c.consumeTimestamp().size(), static_cast<size_t>(14), "默认 consumeTimestamp 14 位");
    bool allDigit = !c.consumeTimestamp().empty();
    for (char ch : c.consumeTimestamp()) {
        if (ch < '0' || ch > '9') allDigit = false;
    }
    CHECK(allDigit, "默认 consumeTimestamp 全是数字");

    c.setNamesrvAddr("127.0.0.1:1");
    c.subscribe("MyTopic", "*");
    // 纯数字的 epoch 毫秒必须被拒（旧实现按 epoch 解释，静默算出错位起点）
    c.setConsumeTimestamp("1700000000000");
    std::string what;
    bool threw = false;
    try {
        c.start();
    } catch (const MQClientException& e) {
        threw = true;
        what = e.what();
    } catch (...) {
        threw = true;
        what = "<not MQClientException>";
    }
    CHECK(threw, "非法 consumeTimestamp 时 start() 抛异常");
    CHECK(what.find("consumeTimestamp is invalid") != std::string::npos,
          "异常文案对齐 Java checkConfig");

    // 合法值不能被这条守卫误杀（namesrv 不可达是另一回事）
    DefaultLitePullConsumer ok;
    ok.setNamesrvAddr("127.0.0.1:1");
    ok.subscribe("MyTopic", "*");
    ok.setConsumeTimestamp("20230101000000");
    std::string okWhat;
    try {
        ok.start();
    } catch (const MQClientException& e) {
        okWhat = e.what();
    } catch (...) {
    }
    CHECK(okWhat.find("consumeTimestamp is invalid") == std::string::npos,
          "合法 consumeTimestamp 不被误杀");
    ok.shutdown();
}

// ---------------------------------------------------------------- 221 GetConsumerStatusBody
static void testGetConsumerStatusBodyWireShape() {
    GetConsumerStatusBody b;
    b.messageQueueTable[MessageQueue("MyTopic", "broker-a", 3)] = 42;
    b.messageQueueTable[MessageQueue("MyTopic", "broker-a", 1)] = 7;

    std::string json = b.encode();
    // 键是 fastjson2 风格的 MessageQueue 内联 JSON（字母序：brokerName, queueId, topic），
    // 作为**转义字符串键**序列化（与已真机验证过的 307 mqTable 同款编码）
    CHECK(json.find("\"{\\\"brokerName\\\":\\\"broker-a\\\",\\\"queueId\\\":3,"
                    "\\\"topic\\\":\\\"MyTopic\\\"}\":42") != std::string::npos,
          "221 messageQueueTable 内联对象键形状");
    // Java 保留的废弃字段 consumerTable 必须带（空对象），缺失会破坏 admin 端解析
    CHECK(json.find("\"consumerTable\":{}") != std::string::npos,
          "221 consumerTable 空对象必须序列化");
    CHECK(json.find("\"messageQueueTable\":") != std::string::npos,
          "221 顶层键名 messageQueueTable");
}

// ---------------------------------------------------------------- 309 ConsumeMessageDirectlyResult
static void testConsumeMessageDirectlyResult() {
    // 默认值对齐 Java：order=false / autoCommit=true
    ConsumeMessageDirectlyResult def;
    CHECK(!def.order, "309 默认 order=false");
    CHECK(def.autoCommit, "309 默认 autoCommit=true");

    // 空字段序列化成 null（对齐 Python/Java 形状：admin 端按 null 展示）
    std::string json = def.encode();
    CHECK(json.find("\"order\":false") != std::string::npos, "309 order 字段名");
    CHECK(json.find("\"autoCommit\":true") != std::string::npos, "309 autoCommit 字段名");
    CHECK(json.find("\"consumeResult\":null") != std::string::npos, "309 空 consumeResult 序列化为 null");
    CHECK(json.find("\"remark\":null") != std::string::npos, "309 空 remark 序列化为 null");
    CHECK(json.find("\"spentTimeMills\":0") != std::string::npos, "309 spentTimeMills 字段名");

    // 成功消费的回执
    ConsumeMessageDirectlyResult ok;
    ok.consumeResult = "CR_SUCCESS";
    ok.spentTimeMills = 12;
    std::string okJson = ok.encode();
    CHECK(okJson.find("\"consumeResult\":\"CR_SUCCESS\"") != std::string::npos,
          "309 CR_SUCCESS");

    // 异常回执 + 往返
    ConsumeMessageDirectlyResult bad;
    bad.order = true;
    bad.consumeResult = "CR_THROW_EXCEPTION";
    bad.remark = "std::exception: boom";
    bad.spentTimeMills = 33;
    ConsumeMessageDirectlyResult back;
    CHECK(ConsumeMessageDirectlyResult::decode(bad.encode(), back), "309 往返解码");
    CHECK(back.order, "往返 order=true");
    CHECK(back.autoCommit, "往返 autoCommit 保持默认 true");
    CHECK_EQ(back.consumeResult, std::string("CR_THROW_EXCEPTION"), "往返 consumeResult");
    CHECK_EQ(back.remark, std::string("std::exception: boom"), "往返 remark");
    CHECK_EQ(back.spentTimeMills, static_cast<int64_t>(33), "往返 spentTimeMills");
}

int main() {
    testLitePullPreStartBehavior();
    testLitePullConsumeTimestamp();
    testGetConsumerStatusBodyWireShape();
    testConsumeMessageDirectlyResult();
    std::cout << "lite_pull: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
