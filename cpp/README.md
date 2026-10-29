# RocketMQ C++ 客户端

> 中文 ｜ [English](README.en.md)

## 概述

RocketMQ 经典 remoting 协议的 C++17 客户端，直连 NameServer(9876) + Broker(10911)，无需 proxy。

网络层手写（POSIX socket + 标准库线程），**运行期零第三方依赖**：zlib 为唯一必需的系统库，
liblz4 / libzstd / OpenSSL 是可选项，缺一个只关一个后端。

覆盖普通 / 顺序 / 延迟 / 事务 / 批量 / 单向 / 真异步 / Request-Reply / 定时撤回的发送能力，
Push、Pull、LitePull 三种消费模型（含 POP 消费）、管理端、ACL、命名空间、消息压缩、TLS、
消息轨迹与钩子。全部功能在 RocketMQ 5.5.1 本地集群上做过真机联调。

## 先决条件

| 依赖 | 要求 | 说明 |
| --- | --- | --- |
| CMake | >= 3.15 | `cmake_minimum_required(VERSION 3.15)` |
| 编译器 | 完整支持 C++17 | Apple clang 14+ / GCC / MSVC（MSVC 自动加 `/utf-8`） |
| 线程库 | 必需 | `find_package(Threads REQUIRED)`，Linux 上需显式链接 |
| zlib | 必需 | `find_package(ZLIB REQUIRED)`，缺失则 configure 失败；`-DRMQ_WITH_ZLIB=OFF` 可出无 zlib 精简版 |
| liblz4 / libzstd | 可选 | 在 `/usr/local`、`/opt/homebrew` 与 CMake config target 中查找；找不到只关该后端 |
| OpenSSL | 可选 | 找到即默认编入 TLS；找不到时 `setTlsEnable(true)` 在运行期抛错，其余路径不受影响 |
| 集群 | NameServer 9876 + Broker 10911 | 仅真机联调需要，`autoCreateTopicEnable=true` |

构建开关：`RMQ_BUILD_TESTS`（默认 ON）、`RMQ_BUILD_EXAMPLES`（默认 ON）、
`RMQ_WITH_ZLIB` / `RMQ_WITH_LZ4` / `RMQ_WITH_ZSTD` / `RMQ_ENABLE_TLS`（默认均 ON）。
configure 日志会写明每个可选后端的状态：

```
-- RocketMQ client zstd: enabled (/usr/local/lib/libzstd.dylib)
-- RocketMQ client lz4: enabled (/usr/local/lib/liblz4.dylib)
-- RocketMQ client TLS: enabled (OpenSSL 3.6.3)
```

编译开了 `-Wall -Wextra`，目标零 warning。

## 构建与测试

```bash
cd cpp
cmake -S . -B build
cmake --build build -j6
cd build && ctest --output-on-failure
```

产出静态库 `build/librocketmq_remoting.a`（别名 `rocketmq::remoting`）、73 个测试二进制与
44 个 `build/examples/` 工具。本机实测：

```
100% tests passed out of 74
Total Test time (real) = 197.80 sec   # ctest --output-on-failure（串行）
```

74 个用例 = 73 个测试二进制 + 1 个编解码互调用例。断言数按每个二进制自己的汇总行汇总（措辞按二进制不同：`N checks, M failures`、
`N passed, M failed`、`PASS=n FAIL=m`、`[PASS] n checks, [FAIL] m`、`checks=n fails=m`、`ALL PASS (n checks)` 等）：逐个串行跑 `build/tests/` 下 73 个测试二进制，各取自己那行汇总，checks 合计
**8,549 项、failures 0**；`tests/interop_check.py` 另有 **73 项 PASS**，合计 **8,622 项**。

不依赖集群的协议层自检：

```bash
./build/examples/rmq_selfcheck      # 3 项 [PASS]，末行 selfcheck: ALL PASS，退出码 0
```

安装到头文件与库的默认目录：

```bash
cmake --install build --prefix /usr/local
```

安装会同时落一份 **CMake 包配置**（`lib/cmake/rocketmq_client_remoting/`：config /
config-version / targets），消费者端不再需要手工声明 Threads/zlib/zstd/lz4/OpenSSL：

```cmake
find_package(rocketmq_client_remoting CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE rocketmq::remoting)
```

config 文件会在消费者作用域内重放各依赖的 find（缺失的可选后端自动跳过）；纯路径后端
（如 brew 的 dylib）以绝对路径随导出固化，同机使用无需额外配置。

## 快速上手

引用头文件在 `include/rocketmq/` 下，按 `add_subdirectory(cpp)` 链接 `rocketmq::remoting`，
或链接安装后的 `librocketmq_remoting.a`（走 `find_package` 时同样用 `rocketmq::remoting`）。

### 生产者

普通消息：

```cpp
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
using namespace rocketmq;

DefaultMQProducer producer("PG_DEMO");
producer.setNamesrvAddr("127.0.0.1:9876");
producer.start();

Message msg("TopicDemo", "hello rocketmq");
msg.setTags("TagA");
msg.setKeys("ORDER_2026");
SendResult r = producer.send(msg, 3000);
// r.msgId / r.offsetMsgId / r.queueOffset
producer.shutdown();
```

顺序消息（同一业务键落同一队列）：

```cpp
producer.sendBySelector(msg, SelectMessageQueueByHash(), "ORDER_2026", 3000);
```

定点发送：`producer.send(msg, mq, 3000)`，队列来自 `producer.fetchPublishMessageQueues(topic)`。

延迟 / 定时消息：

```cpp
Message delay("TopicDemo", "later");
delay.setDelayTimeLevel(3);             // broker messageDelayLevel 定义的档位
delay.setDelayTimeMs(10000);            // 或按毫秒
delay.setDeliverTimeMs(nowMs + 10000);  // 定时投递，返回的 SendResult 带 recallHandle
```

批量消息（整批一个请求、broker 按 N 条独立消息落库）：

```cpp
std::vector<Message> batch;
batch.emplace_back("TopicDemo", "a");
batch.emplace_back("TopicDemo", "b");
SendResult rb = producer.sendBatch(batch, 3000);
```

自动攒批：`producer.setAutoBatch(true)` 后普通 `send()` 会按批聚合发送，
可调 `setBatchMaxDelayMs`（攒批停留毫秒）/ `setBatchMaxBytes`（单批字节上限）/
`setTotalBatchMaxBytes`（全局字节闸门）。

单向发送（不等响应）：`producer.sendOneway(msg);`

真异步发送（调用方不阻塞，回调在独立线程池）：

```cpp
class DemoCallback : public SendCallback {
public:
    void onSuccess(const SendResult& result) override { /* ... */ }
    void onException(const std::exception_ptr& e) override { /* ... */ }
};

producer.sendAsync(msg, std::make_shared<DemoCallback>(), 3000);
producer.sendBatchAsync(batch, std::make_shared<DemoCallback>(), 3000);
```

异步发送背压：`producer.setEnableBackpressureForAsyncMode(true)`（默认关），
在途条数 / 字节各一道公平信号量，`setBackPressureForAsyncSendNum` /
`setBackPressureForAsyncSendSize` 可配；异步失败重试次数是 `setRetryTimesWhenSendAsyncFailed`。

事务消息（半消息 → 本地事务 → END_TRANSACTION，broker 回查走 `checkLocalTransaction`）：

```cpp
class DemoListener : public TransactionListener {
public:
    LocalTransactionState executeLocalTransaction(const Message&, const std::string&) override {
        return LocalTransactionState::COMMIT_MESSAGE;   // 或 ROLLBACK_MESSAGE / UNKNOW
    }
    LocalTransactionState checkLocalTransaction(const MessageExt& msg) override {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
};

TransactionMQProducer txProducer("PG_TX");
txProducer.setNamesrvAddr("127.0.0.1:9876");
txProducer.setTransactionListener(std::make_shared<DemoListener>());
txProducer.start();
TransactionSendResult tr = txProducer.sendMessageInTransaction(
    Message("TopicDemo", "half"), "arg");
// tr.getLocalTransactionState() == LocalTransactionState::COMMIT_MESSAGE
```

`DefaultMQProducer::sendMessageInTransaction(msg, listener, arg)` 也接受一个
`TransactionListener&` 形参，不必用 `TransactionMQProducer`。

Request-Reply（请求方阻塞等应答，应答由 broker 推回）：

```cpp
// 请求方
Message reply = producer.request(Message("TopicDemo", "ping"), 3000);

// 应答方（在消费回调里派生并发出应答）
Message response = createReplyMessage(requestMsg, "pong");
producer.send(response);
```

定时消息撤回（需 broker `timerWheelEnable=true`）：

```cpp
SendResult rs = producer.send(delayMsg);
if (rs.recallHandle) {
    std::string uniqKey = producer.recallMessage("TopicDemo", *rs.recallHandle);
}
```

其他查询：`queryMessage(topic, key, maxNum, begin, end)`、`searchOffset(mq, ts)`、
`maxOffset(mq)`、`minOffset(mq)`、`createTopic(key, newTopic, queueNum)`。

### 推消费者

并发消费：

```cpp
#include "rocketmq/client/consumer.h"

class DemoListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext& ctx) override {
        for (const MessageExt& m : msgs) {
            // m.body / m.getTags() / m.queueOffset / m.getReconsumeTimes()
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;   // 或 RECONSUME_LATER 触发重投
    }
};

DefaultMQPushConsumer consumer("GID_DEMO");
consumer.setNamesrvAddr("127.0.0.1:9876");
consumer.setConsumeFromWhere("CONSUME_FROM_LAST_OFFSET");
consumer.subscribe("TopicDemo", "TagA || TagB");
consumer.setMessageListener(std::make_shared<DemoListener>());
consumer.setConsumeThreadMin(4);
consumer.setConsumeThreadMax(16);
consumer.start();
```

顺序消费（换 `MessageListenerOrderly`，返回 `ConsumeOrderlyStatus`）：

```cpp
class OrderlyListener : public MessageListenerOrderly {
public:
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                        ConsumeOrderlyContext& ctx) override {
        ctx.suspendCurrentQueueTimeMillis = 1000;   // 挂起时长，-1 表示用消费者配置
        return ConsumeOrderlyStatus::SUCCESS;       // SUSPEND_CURRENT_QUEUE_A_MOMENT / ROLLBACK / COMMIT
    }
};
consumer.setMessageListener(std::make_shared<OrderlyListener>());
consumer.setMaxReconsumeTimes(3);
```

广播消费（位点落本地文件，组内不分队列）：

```cpp
consumer.setMessageModel("BROADCASTING");
```

POP 消费（不提交位点，靠 ack + invisibleTime 复活重投）：

```cpp
consumer.setPopMode(true);
consumer.setPopInvisibleTime(30000);
consumer.setPopBatchNums(16);
consumer.setPopThresholdForQueue(96);   // 本队列待 ack 上限，严格大于才停；命中记进拉取路径同一个流控计数
```

拉取前的缓存闸门（三条都严格大于才停，0 不是合法值，`start()` 的数值闸门直接拒绝；闸门只看这一个
队列自己的持有量，从不高估到 topic 级）：

```cpp
consumer.setPullThresholdForQueue(1000);              // 持有条数 = 还没分发的缓冲 ∪ 已交给监听器、还没结算的批次（按位点去重）
consumer.setPullThresholdSizeForQueue(100);           // 持有消息体的整 MiB（累加 body 长度，不是 storeSize；1.9MiB 记作 1）
consumer.setConsumeConcurrentlyMaxSpan(2000);         // 持有位点跨度 max-min；顺序消费不判这一条
consumer.setPullThresholdForTopic(-1);                // topic 预算：-1 不启用，正数在重平衡时整数拆分进第一条
consumer.setPullThresholdSizeForTopic(-1);            // 同上，拆进第二条
```

拆分按本实例当前持有 live 拉取状态的队列数（并发/普通走 `mqMap_`，POP 走 `popQueues_`，跨所有订阅
topic），商至少 1；预算启用时它会覆盖手设的每队列值，之后的 getter 读到的是商。
命中只把这一个队列延后 `kPullPauseWhenClientFlowControl` = 50ms 重排，broker 侧的
`FLOW_CONTROL`(215) 只等 `kPullPauseWhenBrokerFlowControl` = 20ms；客户端流控次数与告警节奏
（第 1、1001、2001… 次告警，其余 debug）与 POP 路径共用一个计数器。
除了这两种，一轮拉取/POP 因为别的原因失败（不是 215 的 broker 错误、POP 一轮本客户端没取出消息——
POLLING_FULL 应答或本客户端认不出的应答、本客户端自己拒绝的一轮）等
`setPullPauseWhenPullErrorMillis`（默认 1000ms）再发下一次请求；每条循环在启动时取一次这个值，
之后再改只影响新起的循环，不影响已经在跑的循环。POLLING_FULL 应答和一个本客户端认不出的 POP
应答，如今和别的失败轮一样等这一个配置好的 backoff。`pullInterval` 只给真正把消息交给消费端的一轮
排节奏：拉取循环在一轮什么都没交付（被过滤清空的 FOUND、或非 FOUND 应答）之后立即再拉——节奏管的
是交付出去的那一轮，间隔不会拖慢空轮的下一次取数；上面那几种失败/暂停/215 的等待完全不受影响。

重平衡轮次里某个 topic 分到的队列**集合**变了（增长、缩减、同数量换 broker、topic 新出现或消失；
同一集合换顺序不算）时，这一轮依次做四件事：只给变动的 topic 重打订阅版本（墙钟毫秒，日志
`<topic> rebalance changed, also update version: <旧>, <新>`）→ 按本轮队列数把 topic 预算整数拆分进
每队列阈值 → 立即向所有 broker 推一次心跳（broker 按版本缓存这条订阅的过滤结果，不等下一个周期）
→ 只把变动的 topic 告诉应用的 `MessageQueueListener`。集合没变的轮次与停摆自愈轮次四件事都不做。

```cpp
#include "rocketmq/client/message_queue_listener.h"

class QueueHandover : public MessageQueueListener {
public:
    void messageQueueChanged(const std::string& topic, const std::vector<MessageQueue>& mqAll,
                             const std::vector<MessageQueue>& mqDivided) override {
        // mqAll 是路由里这条 topic 的全部队列，mqDivided 是本实例应得的那一份
    }
};
consumer.setMessageQueueListener(std::make_shared<QueueHandover>());
```

`DefaultMQPullConsumer` 没有后台重平衡，队列集合由调用方给出，它只欠监听器这一半：按集合相等判断
在自己的均衡视图变化时通知同一个回调，不重打订阅版本、不额外推心跳。

其他：`suspend()` / `resume()`、`setPullBatchSize` / `setPullThresholdForQueue`（拉取前流控）、
`setAllocateMessageQueueStrategy`（六种队列分配策略）、
`registerConsumeMessageHook` / `registerCheckForbiddenHook` / `registerFilterMessageHook`、
`updateCorePoolSize(n)` 运行时调并发度。

### 拉消费者

无后台 rebalance，队列集合与位点都由调用方掌握：

```cpp
#include "rocketmq/client/pull_consumer.h"

DefaultMQPullConsumer pull("GID_PULL");
pull.setNamesrvAddr("127.0.0.1:9876");
pull.start();

for (const MessageQueue& mq : pull.fetchSubscribeMessageQueues("TopicDemo")) {
    int64_t offset = 0;
    int64_t stored = 0;
    if (pull.fetchConsumeOffset(mq, stored)) offset = stored;

    PullResult res = pull.pull(mq, "*", offset, 32, 5000);       // 一次短轮询
    // PullResult res = pull.pullBlockIfNotFound(mq, "*", offset, 32);   长轮询
    for (const MessageExt& m : res.msgFoundList) { /* ... */ }
    pull.updateConsumeOffset(mq, res.nextBeginOffset);
}
pull.startHeartbeatLoop();   // 需要 broker 侧可见本组注册时打开
```

`fetchMessageQueuesInBalance(topic)` 只返回本实例按分配策略应得的那份队列。

异步拉取（`DefaultMQPullConsumer::pull(..., PullCallback, timeout)`）：
立即返回，工作线程执行同一套同步拉取内核，回调线程上 `onSuccess` / `onException`
**恰好一次**触发；`shutdown()` 先排空在途请求：

```cpp
class MyPullCallback : public PullCallback {
public:
    void onSuccess(const PullResult& res) override { /* res.msgFoundList / res.nextBeginOffset */ }
    void onException(const std::exception_ptr& e) override { /* std::rethrow_exception(e) 查看 */ }
};

pull.pullAsync(mq, "*", offset, 32, std::make_shared<MyPullCallback>(), 5000);
```

调度服务（`MQPullConsumerScheduleService`）：按 topic 注册 `PullTaskCallback`，
常驻调度线程周期性重算本实例应得份额（CLUSTERING 走 `fetchMessageQueuesInBalance`、
BROADCASTING 走 `fetchSubscribeMessageQueues`），展开成 queue 维度拉取任务——同队列严格串行、
不同队列在 `setPullThreadNums`（默认 20）宽的线程池上并行；服务代拉并管理位点
（首轮 `fetchConsumeOffset`，查不到则 `maxOffset`；其后跟随 `nextBeginOffset`）：

```cpp
#include "rocketmq/client/pull_schedule_service.h"

class MyPullTask : public PullTaskCallback {
public:
    int32_t doPullTask(const MessageQueue& mq, PullResult& res, PullTaskContext& ctx) override {
        for (const MessageExt& m : res.msgFoundList) { /* ... */ }
        ctx.pullConsumer()->updateConsumeOffset(mq, res.nextBeginOffset);
        return res.isFound() ? 0 : PullTaskCallback::kUseContextDelay;
    }
};

MQPullConsumerScheduleService svc("GID_PULL_SCHED");
svc.defaultMQPullConsumer().setNamesrvAddr("127.0.0.1:9876");
svc.registerPullTaskCallback("TopicDemo", std::make_shared<MyPullTask>(), "*");
svc.start();
// ...
svc.shutdown();   // 先停调度，再排空在途 doPullTask，最后关消费者
```

下一轮延时按三通道优先级决议：`doPullTask` **返回值**（>= 0 毫秒，最高）＞
`ctx.pullMessageLater(延时)`（一次性顺延）＞ `ctx.setPullInterval(间隔)` ＞ 状态默认
（FOUND=0 立即；空应答 / OFFSET_ILLEGAL=`setPullTimeDelayMillisWhenException`，默认 3s；
拉取失败或回调抛异常=`setPullTaskExceptionDelayMillis`，默认 1s）。

### 轻量拉消费者

这一表面的误用一律在本地先报错：空白 topic 的 `subscribe`、空队列列表的 `assign`（那等于撤销全部持有队列）、已经在跑之后再调 `setSubExpressionForAssign`、没启动就 `poll` / `committed`。`poll` 的负超时同样被拒绝，但运行状态检查排在它前面——空的一轮和「还没有位点」都和队列停在尾位点长得一模一样，缺了 `start()` 只有这样才不会一直被藏下去。`subscribe` / `assign` 互斥，先调用者定形；`seek` 依次判「队列是否持有」→「是否已启动」→ 位点落在 `[minOffset, maxOffset]` 内；`assign` 收窄与 `unsubscribe` 立刻撤销不再持有队列的光标、seek 定位、暂停位、park 与本地缓冲。

```cpp
#include "rocketmq/client/lite_pull_consumer.h"

DefaultLitePullConsumer lite("GID_LITE");
lite.setNamesrvAddr("127.0.0.1:9876");
lite.subscribe("TopicDemo", "TagA");
lite.setPollTimeoutMillis(3000);
lite.setAutoCommit(true);
lite.setAutoCommitIntervalMillis(5000);
lite.start();

while (running) {
    for (const MessageExt& m : lite.poll()) { /* ... */ }
}
lite.commit();
lite.shutdown();
```

解析拉取位点前先过四道缓存闸门（`pullBatchSize` 默认 10，四道都严格大于才停，命中只重排这一个队列；
这四个数值在轻量拉消费者上不做启动期校验，0 合法且第一条缓存消息就会命中）：

```cpp
lite.setPullThresholdForAll(10000);       // 整个缓冲：待拉轮次 × pullBatchSize 的容量投影，跨所有队列的累加器求和
lite.setPullThresholdForQueue(1000);      // 本队列已缓存条数
lite.setPullThresholdSizeForQueue(100);   // 本队列已缓存消息体的整 MiB（累加 body 长度，不是 storeSize）
lite.setConsumeMaxSpan(2000);             // 本队列缓存位点跨度 max-min
```

计数器和告警节奏：本队列条数与大小共用一个计数器，容量投影与跨度各一个，告警仍是第 1、1001、2001…
次 warn、其余 debug。队列被收回时（`assign()` 收窄、后台重平衡、拉取轮次的兜底清理三条路径同一条
实现），它的缓存行与待拉轮次一起离开缓冲——既不继续 `poll()` 得到，也不再替整个缓冲占容量投影。

队列变更监听（后台按 `topicMetadataCheckIntervalMillis` 比对，集合真的变了才回调）：

```cpp
class QueueChange : public TopicMessageQueueChangeListener {
public:
    void onChanged(const std::string& topic,
                   const std::vector<MessageQueue>& mqs) override {
        std::printf("%s 现在有 %zu 个队列\n", topic.c_str(), mqs.size());
    }
};
lite.setTopicMetadataCheckIntervalMillis(1000);
lite.registerTopicMessageQueueChangeListener("TopicDemo", std::make_shared<QueueChange>());
```

assign 模式与位点控制：

```cpp
lite.assign(lite.fetchMessageQueues("TopicDemo"));
lite.setSubExpressionForAssign("TopicDemo", "TagA");
lite.seek(mq, 100);                 // seekToBegin(mq) / seekToEnd(mq)
lite.pause({mq}); lite.resume({mq});
int64_t committed = lite.committed(mq);
lite.commit({{mq, 200}}, true);
```

订阅表达式也可以用选择器写法（`subscribe` / `setSubExpressionForAssign` 与 pull 侧的 `pull` /
`pullBlockIfNotFound` / `pullAsync` 都有同名重载）：`MessageSelector::byTag("TagA||TagB")` 与字符串写法
在线上完全同形；`MessageSelector::bySql("color = 'red'")` 把表达式原样带上请求头（`expressionType=SQL92`、
`subVersion` 为订阅自身版本），并且不再走客户端的 tag 二次过滤——本地按 tag 列表去解析一段 SQL 会把 broker
已经选出的消息全部丢弃。

```cpp
lite.subscribe("TopicDemo", MessageSelector::bySql("color = 'red' AND weight > 10"));
```

### 管理端

```cpp
#include "rocketmq/client/admin.h"

DefaultMQAdminExt admin("ADMIN");
admin.setNamesrvAddr("127.0.0.1:9876");
admin.start();

admin.createTopic(MixAll::DEFAULT_TOPIC, "TopicDemo", 4);      // 按模板 broker 建 topic
TopicStatsTable stats = admin.examineTopicStats("TopicDemo");  // 逐队列 min/max/lastUpdate
ConsumeStatsList progress = admin.fetchConsumeStatsInBroker("127.0.0.1:10911");

// 按时间戳重置（返回逐队列的新位点）与按队列重置（两笔 RPC：25 + 222）
auto offsets = admin.resetOffsetByTimestamp("TopicDemo", "GID_DEMO", admin.minOffset(mq), true);
admin.resetOffsetByQueueId("127.0.0.1:10911", "GID_DEMO", "TopicDemo", 0, 100);

admin.deleteTopic("TopicDemo");
admin.shutdown();
```

其余：`examineTopicConfig` / `createAndUpdateTopicConfig` / `createAndUpdateTopicConfigList`、
`createAndUpdateSubscriptionGroupConfig` / `deleteSubscriptionGroup`、
`examineConsumerConnectionInfo`、`examineConsumeStats` / `examineConsumeStatsGroup` / `consumed` /
`messageTrackDetail`、`queryTopicsByConsumer`、`getAllTopicConfig` / `getUserTopicConfig`、
`fetchAllTopicRoute` / `getClusterList` / `examineBrokerClusterInfo` / `fetchBrokerRuntimeStats`、
`wipeWritePermOfBroker`、`searchOffset` / `searchLowerBoundaryOffset` / `searchUpperBoundaryOffset` /
`maxOffset` / `minOffset` / `earliestMsgStoreTime`、`examineConsumerOffset` /
`updateConsumerOffset` / `resetOffsetNew`、`queryMessage` / `queryMessageByKey` /
`queryMessageByUniqKey`、`cloneGroupOffset`。

### ACL 鉴权

```cpp
#include "rocketmq/remoting/rpchook.h"

producer.setCredentials("AccessKey", "SecretKey");                 // 可选第三参 securityToken
// 等价写法：
producer.setRPCHook(std::make_shared<AclClientRPCHook>(
    SessionCredentials("AccessKey", "SecretKey")));
```

三种消费者与 `DefaultMQAdminExt` 同样有 `setCredentials` / `setRPCHook`。
钩子链顺序固定为 Namespace → Stream → ACL，因此 `ns` / `ReqT` 都落在签名内容之内；
自定义钩子用 `composeRequestHooks()` 合成后再注册。

### 命名空间

两套机制，彼此独立：

```cpp
// 1) 客户端本地资源名前缀：topic/group 被改写成 <ns>%<资源>，收发、心跳、位点全线包装与还原
producer.setNamespace("MQ_INST_XX");

// 2) 服务端命名空间：每笔请求额外盖 nsd=true / ns=<值>，start() 之后改仍然生效
producer.setNamespaceV2("MQ_INST_XX");
```

`setNamespace` 影响的是资源名本身（`%RETRY%` / `%DLQ%` 前缀在命名空间之外），
`setNamespaceV2` 影响的是请求头字段。四种客户端门面与管理端都支持两套。

### 消息压缩

```cpp
producer.setCompressMsgBodyOverHowmuch(4096);   // 超过该字节数自动压缩，0 关闭
producer.setCompressType(CompressionType::ZSTD);  // ZLIB(3) / LZ4(1) / ZSTD(2)
producer.setCompressLevel(5);                     // 仅 ZLIB 生效
```

消费端按报文 `COMPRESSED_FLAG` 自动解压；构建时关掉的后端遇到对应消息会抛异常，
不会把压缩字节当正文交出。类型位 `0` 与 `3` 都按 ZLIB 解。

### TLS

```cpp
producer.setTlsEnable(true);                     // 也可用环境变量 ROCKETMQ_TLS_ENABLE=1
TlsOptions opts;
opts.caCert = "/path/ca.pem";                    // 非空 = 证书链 + 主机名严格校验
opts.clientCert = "/path/client.pem";            // mTLS
opts.clientKey = "/path/client.key";
opts.serverName = "broker-a";
producer.setTlsOptions(opts);
```

每条 TLS 连接一个 `TlsSession`，会话内部对 `SSL_*` 调用加锁（读线程与调用方写线程
在同一条 SSL 会话上交叠）。`DefaultMQPushConsumer` / `DefaultMQPullConsumer` /
`DefaultLitePullConsumer` 同样有 `setTlsEnable` / `setTlsOptions`。

## 特性与进度

- ✅ remoting 协议：JSON 与 ROCKETMQ 二进制两路序列化、`RemotingCommand` 帧编解码、
  CommandCustomHeader 家族（含 V2 单字母短字段名）、17 段消息存储格式与 6 段批量格式
- ✅ 传输层：同步 / 异步 / oneway、半包重组、opaque 匹配、断线重连、
  GO_AWAY(1500) 换连接重发、连接判死、SIGPIPE 处理
- ✅ 连接生命周期，每条规则一个离线用例：解不开帧的连接只打一条点名对端的 ERROR，并且只拆掉那一条连接（长度前缀越界是同一件事更早一层的表现，只在抓到它的那一层报一次，不会再补一条解码失败），对端下一个合法请求在新连接上被服务；没人等的响应被丢弃并打一条点名 opaque 与地址的 WARNING，连接留着并继续服务下一个请求；`isChannelWritable()` 回答的是 socket 发送缓冲，不是有没有连接——没有连接的地址即可写（下一次发送自己建连，而且问一次不会凭空建出连接），有缓冲区的活连接可写，只有不读的把缓冲塞满才是不可写；传输层自己驱动空闲清扫——停止承载流量、静默严格超过 `setChannelIdleTimeoutMillis()`（默认 120000；配 ≤0 恢复默认，因为只要有连接清扫就一直在装）的连接被关掉并打一条点名地址的 WARNING，每 100ms 还被写的连接永远不会被当成空闲；每个被回收的在途请求一条超时 WARNING 点名地址，而且回收请求不是连接事件；同步调用等满超时后默认关掉它发出去的那条连接，`setCloseSocketOnSyncTimeout(false)` 才关不掉，而发送本身失败则无论开关一律关闭
- ✅ TLS：test-mode 与严格 CA 校验、mTLS
- ✅ 生产者：普通 / 顺序（selector 与定点）/ 延迟与定时 / 事务 / 批量 / 单向 / 真异步 /
  异步背压 / 自动攒批 / Request-Reply / 定时消息撤回 / 消息查询
- ✅ 消费者：`DefaultMQPushConsumer`（并发、顺序、广播、POP）、
  `DefaultMQPullConsumer`（短轮询 / 长轮询 / 位点表 / 心跳 / 异步拉取 `pullAsync`）、
  `DefaultLitePullConsumer`（subscribe 与 assign 双模式、seek、三张位点表、队列变更监听、
  解析拉取位点前先过四道缓存闸门：整个缓冲的容量投影 `setPullThresholdForAll` 10000（待拉轮次 ×
  `pullBatchSize`，默认 10）/ 本队列条数 `setPullThresholdForQueue` 1000 / 本队列消息体整 MiB
  `setPullThresholdSizeForQueue` 100 / 本队列位点跨度 `setConsumeMaxSpan` 2000，都严格大于才停；
  队列被收回时它的缓存与待拉轮次一起删除；命中只重排这一个队列
  `kPullPauseWhenClientFlowControl` = 50ms，broker 的 `FLOW_CONTROL`(215) 只等
  `kPullPauseWhenBrokerFlowControl` = 20ms）、
  push 侧的三道缓存闸门（持有条数含已交给监听器未结算的批次、大小按消息体整 MiB、跨度顺序消费不判，
  topic 预算在重平衡时按本轮队列数整数拆分进它们）、
  队列集合变化的一轮（只给变动的 topic 重打订阅版本 → 拆分预算 → 立即心跳 → 只通知变动的 topic 给
  `MessageQueueListener`；不变轮次与自愈轮次四件事都不做）、
  `MQPullConsumerScheduleService`（按 topic 注册 `PullTaskCallback`，按本实例应得份额展开
  queue 维度拉取任务：同队列串行、三通道延时优先级、关停排空在途）
- ✅ 队列分配策略：AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH /
  MACHINE_ROOM / MACHINE_ROOM_NEARBY，三类消费者均可替换
- ✅ 管理端 `DefaultMQAdminExt`：topic 与订阅组增删查、消费进度、连接信息、
  位点重置（220 / 222）、路由与集群信息、写权限摘除
- ✅ ACL V1 / V2 / V3 签名（SHA1 / HMAC / Base64 标准向量），STS securityToken
- ✅ 命名空间两套：本地资源前缀与服务端 `nsd`/`ns`
- ✅ 消息压缩：zlib / LZ4 Frame / ZSTD 三后端，生产自动压、消费自动解
- ✅ 消息轨迹（Pub / SubBefore / SubAfter / EndTransaction / Recall）与 W3C `traceparent` 上下文
- ✅ 钩子：SendMessageHook / ConsumeMessageHook / EndTransactionHook /
  CheckForbiddenHook / FilterMessageHook
- ✅ 故障容错：发送重试换 broker、延迟故障隔离与恢复、拉取前流控、停摆队列自愈、
  `cleanExpiredMsg` 挂起逃生口、OFFSET_ILLEGAL 与空应答位点纠正
- ✅ 名字校验 `Validators` / `TopicValidator`，启动期配置数值闸门
- ✅ 动态 NameServer（`DefaultTopAddressing` 的 URL 规则）
- ✅ 本地位点文件（广播模式）与 `.bak` 滚动
- ✅ 客户端日志：级别过滤、按大小轮转、线程名 / 毫秒 / 文件行号
- ⬜ Windows 分支代码在内，未在真机验证

## 客户端日志

`include/rocketmq/common/logging.h` 是 header-only 日志，默认级别 INFO，
同时写 stderr 与文件。

行格式 `日期.毫秒 级别 [pid] [线程名] [文件:行号] - 消息`，实测两行：

```
2026-10-10 16:44:16.450 INFO  [21908] [main] [producer.cpp:270] - DefaultMQProducer[PG_DEMO] started, clientId=30.234.192.255@21908#198086958751588
2026-10-10 16:44:16.510 INFO  [21908] [tid-7e48] [produce_accumulator.cpp:746] - ..._GuardForAsyncSend service end
```

主线程落 `main`，工作线程名由内部给出；攒批守卫线程名是 `<clientId>_GuardForSyncSend` /
`<clientId>_GuardForAsyncSend`。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` / `OFF` |
| `ROCKETMQ_CLIENT_LOG_FILE` | `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log` | 设为空串 / `OFF` / `NONE` 则只留 stderr |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864`（64MB） | 单文件上限，`0` 表示不轮转 |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | 备份份数，`0` 表示不保留备份 |

轮转是 Fixed Window：达到 `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` 时先删 `<file>.N`，
其余依次后移（`.N-1 → .N` … `.1 → .2`），最后当前文件 → `.1`，因此备份名是
`rocketmq_cpp_client.log.1 … rocketmq_cpp_client.log.10`，`.10` 最旧。备份不压缩。
写入是同步的（每行 `fflush`），`tail -f` 实时可见。

连接关闭记 DEBUG；协议异常（帧长非法、解码失败）记 WARN。
长轮询超时属正常路径，记 DEBUG，所以默认级别下 `ERROR=0` 是预期状态。

## 真实集群联调

需要跑着 NameServer(9876) + Broker(10911) 且 `autoCreateTopicEnable=true` 的集群。
这些工具**不打进 ctest**（依赖外部集群）。先起消费者再发消息——多数场景靠消费者先注册，
broker 侧的订阅表、位点与轨迹才落在预期的组上。

```bash
./build/examples/rmq_live_message_types 127.0.0.1:9876
./build/examples/rmq_live_message_types 127.0.0.1:9876 <accessKey> <secretKey>   # 集群开了认证
```

失败以非 0 退出码结束，输出里逐条打 `[PASS]` / `[FAIL]` 与末行 `PASS=<n> FAIL=<m>`；
需要额外 broker 配置的分支会打 SKIP 并写明原因。

绝大多数工具只接一个可选的 namesrv 参数（默认 `127.0.0.1:9876`）。参数更多的几只：

```bash
./build/examples/rmq_live_tls 127.0.0.1:9876 <topic> <group> [leg] [caCert] [serverName] [clientCert] [clientKey]
./build/examples/rmq_live_tls_negative 127.0.0.1:9876 <topic> <wrongCaCert> [serverName]
./build/examples/rmq_live_pull_heartbeat 127.0.0.1:9876 127.0.0.1:10911 [slaveAddr]
./build/examples/rmq_live_publish_route_master 127.0.0.1:9876 127.0.0.1:10911 [slaveAddr]
./build/examples/rmq_compression_live selftest 127.0.0.1:9876
./build/examples/rmq_compression_live send 127.0.0.1:9876 <topic> <group> <size> [codec]
./build/examples/rmq_live_pop_batch_ack 127.0.0.1:9876 [legs]   # legs 为 l1/l2/l3 的任意组合（逗号分隔），默认三条全跑
./build/examples/rmq_live_pop_mode 127.0.0.1:9876 [legs]        # 401/402 请求模式切换，legs 同上
```

| 二进制 | 验的东西 | 额外 broker 配置 |
| --- | --- | --- |
| `rmq_selfcheck` | 协议层往返自检，不需要集群 | — |
| `rmq_interop` | 编解码互操作工具，由 `tests/interop_check.py` 驱动 | — |
| `rmq_live_message_types` | 同步 / 单向 / 异步 / 定点异步 / 顺序 / Tag / 属性 / 延迟 / 定时 / Key 查询 / 事务三态 / 批量等消息能力；带 `[ak] [sk]` 时每类都在鉴权集群上收发，并含无凭据被拒的反向断言 | `authenticationEnabled=true`（跑鉴权腿时） |
| `rmq_live_redelivery` | 回投、位点持久化、顺序锁、广播、流控、多实例 rebalance、显式 ack 等十三段 | — |
| `rmq_live_pop` | POP 拉取 / ack / 延长不可见时间 / 不 ack 复活重投 | — |
| `rmq_live_pop_consumer` | POP 消费循环 + 307 状态表里的 pullRT / pullTPS | — |
| `rmq_live_pop_batch_ack` | 批量 ack（200151）三条腿：整批一次请求且 body 里的位集逐条对得上被弹出的位点、只丢一条时只有那一条带重试标记回来、同一队列两轮弹取合进一个报文；发出的原始帧由 RPC 钩子抓取，再用独立实现的 base64 解码校验，不靠编码自己证明自己 | — |
| `rmq_live_pop_mode` | SET_MESSAGE_REQUEST_MODE(401) 的真机三条腿（实测 `PASS=13 FAIL=0`）：生产 `setMessageRequestMode` 把四个值全写进 JSON body → GET_ALL_MESSAGE_REQUEST_MODE(402) 回读必须按这对 topic+group 出现 `mode=POP`、`popShareQueueNum` 是数字 4；同组改写 PULL/0 必须覆盖前一次；对照腿把同样四个值塞进 header、body 留空，broker 必须拒绝（实测 code=1，remark 说请求体解不出来）且该控制组在 402 里始终缺席——同一次回读里 body 形态的锚点条目仍然可见，缺席才真是缺席。topic 每次新建、名字带 stamp，主 broker 地址取自建 topic 的路由，回读轮询到有答案为止 | — |
| `rmq_live_pull` | 拉消费者的队列、短轮询 / 长轮询、手动位点 | — |
| `rmq_live_pull_heartbeat` | 拉消费者的 203 / 38 / 35 报文与 consumeType 口径 | — |
| `rmq_live_lite_pull` | 轻量拉消费者的 subscribe / assign 双模式与 poll、本队列缓存条数闸门（超标只 park 这一路，排空后续读到尾部） | — |
| `rmq_live_lite_pull_cursor` | 空应答跟随 `nextBeginOffset` 与 OFFSET_ILLEGAL 越界自愈 | — |
| `rmq_live_lite_pull_code` | 请求码 361 与 lite 位、`litePullMessageEnable` 开关还原 | `litePullMessageEnable`（工具自开自关） |
| `rmq_live_lite_topic_queue_change` | 队列扩缩容触发的监听回调，比对趟次现查路由 | — |
| `rmq_live_subscribe` | `start()` 之后 `subscribe` 立即推心跳、新 topic 真被消费 | — |
| `rmq_live_async_send` | 异步发送线程口径 / 并发 / 定点 / 拦截 / 批量 / 关池排空 | — |
| `rmq_live_backpressure` | 异步发送的两个公平信号量限流与运行时扩容 | — |
| `rmq_live_send_header` | 发送头 `c` / `d` / `n` 三字段与自动建 topic 的队列数 | — |
| `rmq_live_pinned_guard` | 定点发送的 topic 守卫：拒在本端、broker 无痕 | — |
| `rmq_live_producer_unregister` | 退出时向每台 broker 注销 clientId(35) | — |
| `rmq_live_flow_control` | 拉取前的三个单队列阈值命中（topic 预算在重平衡时按队列数拆分进它们）且一条不丢 | — |
| `rmq_live_correct_tags_offset` | 空应答也把已提交位点推到 maxOffset | — |
| `rmq_live_offset_illegal` | 位点被纠正时整批作废并按修正值重建 | — |
| `rmq_live_reset_offset` | 220 重置位点：立刻落盘 + 在途批次作废 + 队列重建 | — |
| `rmq_live_scheduled_intervals` | 路由刷新与位点落盘周期的首跳与固定速率 | — |
| `rmq_live_clean_expired_msg` | 挂起 listener 的逃生口：清扫回投 `%RETRY%` 再投第二次（约 4 分钟） | — |
| `rmq_live_fail_fast` | broker 真停时在途请求秒级判死（会停一次 broker 再拉起） | — |
| `rmq_live_publish_route_master` | master 掉线时发布队列归零、订阅不变、仍从从节点消费 | 需从节点 |
| `rmq_live_latency` | 发送延迟故障容错与隔离恢复 | — |
| `rmq_live_hook` | CheckForbiddenHook 拦截 + FilterMessageHook 两条拉取路径 | — |
| `rmq_live_trace` | 消息轨迹端到端上报与查询 | `traceTopicEnable=true` |
| `rmq_live_request_reply` | 请求 → 应答 → 326 推回，含 10006 / 10007 错误码 | — |
| `rmq_live_acl` | ACL 签名真机验签 | `authenticationEnabled=true` |
| `rmq_live_tls` | TLS 通路 + traceparent 注入 / 子 span | `tls.test.mode.enable` |
| `rmq_live_tls_negative` | 严格校验下信任错误 CA 必须报出 CA 标记 | broker 开 TLS，另给一份错误 CA 路径 |
| `rmq_live_unit_config` | unitName / unitMode / stream 在 broker 侧的落点 | — |
| `rmq_admin_live` | 管理端 topic / 订阅组 / 进度 / 连接信息 / 重置全链路 | — |
| `rmq_admin_batch_live` | 管理端批量接口（`createAndUpdateTopicConfigList` 等） | — |
| `rmq_validators_live` | 名字校验本地快拒 + 合法名字照常收发 | — |
| `rmq_sql92_live` | SQL92 属性过滤与 CHECK_CLIENT_CONFIG(46) | `enablePropertyFilter=true` |
| `rmq_recall_live` | 定时消息撤回句柄往返 + 到点不投递 | `timerWheelEnable=true`，`recallMessageEnable`（工具自开自关） |
| `rmq_compression_live` | 自动压缩自产自销：`selftest` 或 `send|recv <namesrv> <topic> <group> <size> [codec]` | — |

## 目录结构

```
cpp/
├── CMakeLists.txt              静态库 + 可选后端探测（zlib / lz4 / zstd / OpenSSL）
├── include/rocketmq/
│   ├── common/                 消息模型、编解码、压缩、哈希环、命名空间、日志
│   ├── remoting/               RemotingClient、TLS 会话、RPC 钩子、protocol/（codes /
│   │                           headers / route / heartbeat / body / admin_body / ...）
│   └── client/                 producer / consumer / pull_consumer / pull_schedule_service /
│                               lite_pull_consumer / admin / mq_client / allocate_strategy /
│                               hook / trace / request_reply / backpressure / validators /
│                               result / exception
├── src/                        与 include 同构的实现（common / remoting / client）
├── tests/                      73 个测试源文件 + interop_check.py = 74 个 ctest 用例
├── examples/                   rmq_selfcheck / rmq_interop + 41 个真机联调工具
└── tools/                      Windows 头文件准备与语法检查脚本
```

## License

Apache-2.0，与 Apache RocketMQ 保持一致。
