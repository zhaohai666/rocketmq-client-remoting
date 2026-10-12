// 轻量拉取消费者（DefaultLitePullConsumer）+ 220/221/309 body 单测。
#include <cstdint>
#include <iostream>
#include <map>
#include <set>
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

    c.subscribe("MyTopic", "TagC");
    // 不崩即通过（表达式存私有表，真机验证覆盖）
    ++g_pass;
}

static void testLitePullOffsetTable() {
    DefaultLitePullConsumer c("LitePGOffsetTable");
    const MessageQueue q0("MyTopic", "broker-a", 0);
    const MessageQueue q1("MyTopic", "broker-a", 1);
    const MessageQueue stranger("MyTopic", "broker-a", 7);
    c.assign({q0, q1});

    CHECK_EQ(c.pullCursorOf(q0), -1, "未解析起点前拉取游标 = -1");
    CHECK_EQ(c.consumeCursorOf(q0), -1, "没交付过 ⇒ 已消费游标 = -1");

    c.commit();
    CHECK_EQ(c.committed(q0), -1, "commitAll 不提交没消费过的队列");

    // commit(map)：调用方指定位点只写提交落点，两条游标一律不动
    std::map<MessageQueue, int64_t> specified;
    specified[q0] = 5;
    specified[q1] = 8;
    c.commit(specified, false);
    CHECK_EQ(c.committed(q0), 5, "指定位点进内存位点表");
    CHECK_EQ(c.committed(q1), 8, "指定位点进内存位点表(q1)");
    CHECK_EQ(c.pullCursorOf(q0), -1, "提交位点不改拉取游标");
    CHECK_EQ(c.consumeCursorOf(q0), -1, "提交位点不改已消费游标");

    std::map<MessageQueue, int64_t> guarded;
    guarded[q0] = -1;
    guarded[stranger] = 3;
    c.commit(guarded, false);
    CHECK_EQ(c.committed(q0), 5, "offset == -1 只记日志，不覆盖已有位点");
    CHECK_EQ(c.committed(stranger), -1, "没分配到的队列不替它提交");

    c.commit(std::map<MessageQueue, int64_t>(), false);
    CHECK_EQ(c.committed(q0), 5, "空 map 忽略这次提交");
    c.commit(std::set<MessageQueue>(), false);
    CHECK_EQ(c.committed(q0), 5, "空集合忽略这次提交");

    // commit(set) 走的是已消费游标，不是任意指定值：未交付 ⇒ 守卫拦住
    std::set<MessageQueue> only0;
    only0.insert(q0);
    c.commit(only0, false);
    CHECK_EQ(c.committed(q0), 5, "commit(Set) 在没有交付记录时不写 -1");

    c.seek(q0, 2);
    CHECK_EQ(c.pullCursorOf(q0), 2, "seek 改拉取游标");
    CHECK_EQ(c.consumeCursorOf(q0), 2, "seek 也要改已消费游标，否则重放的段会被旧位点跳过");

    c.assign({q0});
    CHECK_EQ(c.pullCursorOf(q1), -1, "assign 撤队列后拉取游标消失");
    CHECK_EQ(c.consumeCursorOf(q1), -1, "assign 撤队列后已消费游标消失");
    CHECK_EQ(c.committed(q1), 8, "assign 模式不碰 offsetStore");
    // 撤掉的队列再被指定提交要守卫拦住（不再是本实例持有的队列）
    std::map<MessageQueue, int64_t> late;
    late[q1] = 99;
    c.commit(late, false);
    CHECK_EQ(c.committed(q1), 8, "撤掉的队列不替它改位点");

    c.commit(only0, true);
    // q0 这次提交用的是当下已消费游标（上面 seek 把它和拉取游标一起改成了 2）
    CHECK_EQ(c.committed(q0), 2, "commit(Set) 提交的是已消费游标");
    CHECK_EQ(c.committed(q1), -1, "persistAll 会把没点名的队列从内存表里丢掉");
}

static void testLitePullConsumeTimestamp() {
    // 组名要合法且不能是保留的 DEFAULT_CONSUMER：checkConfig 排在起点校验之前，
    // 用默认组只会测到组名那条错误（见 Validators::checkGroup 的接线）。
    DefaultLitePullConsumer c("LitePullTsGroup");
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
    CHECK(json.find("\"consumerTable\":{}") != std::string::npos,
          "221 consumerTable 空对象必须序列化");
    CHECK(json.find("\"messageQueueTable\":") != std::string::npos,
          "221 顶层键名 messageQueueTable");
}

// ---------------------------------------------------------------- 309 ConsumeMessageDirectlyResult
static void testConsumeMessageDirectlyResult() {
    ConsumeMessageDirectlyResult def;
    CHECK(!def.order, "309 默认 order=false");
    CHECK(def.autoCommit, "309 默认 autoCommit=true");

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
    testLitePullOffsetTable();
    testLitePullConsumeTimestamp();
    testGetConsumerStatusBodyWireShape();
    testConsumeMessageDirectlyResult();
    std::cout << "lite_pull: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
