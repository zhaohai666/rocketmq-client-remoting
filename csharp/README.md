# RocketMQ C# 客户端

> 中文 ｜ [English](README.en.md)

## 概述

`csharp/src/RocketMQ.Client` 是 Apache RocketMQ 经典 remoting 协议的 .NET 客户端：直接对接
NameServer（取路由、动态取址）与 Broker（收发消息、心跳、位点、管理请求），不需要代理或 sidecar。

- 目标框架 `net10.0`，**零外部 NuGet 依赖**，只用 BCL：JSON 容错解析、帧编解码、TCP 传输全部自实现，
  zlib 走内置 `ZLibStream`，LZ4 / ZSTD 通过 P/Invoke 调系统库。
- 一个库覆盖生产者、Push / Pull / LitePull 三种消费者、管理端、消息轨迹、ACL、TLS。
- 能力在真实 RocketMQ 5.5.1 集群（namesrv 9876 + broker 10911）上逐条联调，
  离线行为由 808 项单元测试锁死。

## 先决条件

- .NET 10 SDK（本机 `dotnet --version` → `10.0.203`）。
- 一个可达的集群：NameServer 监听 `9876`，Broker 监听 `10911`，且 `autoCreateTopicEnable=true`。
- 集群起停可用仓库脚本：`sh ../scripts/rmq_test_broker.sh status | stop | start`
  （需要注入 broker 停启的用例也走它，不会删 store）。

## 构建与测试

```bash
cd csharp
dotnet build                        # src + tests + examples 三个工程
dotnet test tests/RocketMQ.Client.Tests
```

本机实测：

| 命令 | 结果 |
| --- | --- |
| `dotnet build --no-incremental` | `0 个警告` / `0 个错误`，用时 `00:00:17.94` |
| `dotnet test tests/RocketMQ.Client.Tests` | `已通过! - 失败: 0，通过: 808，已跳过: 0，总计: 808，持续时间: 2 m 52 s` |

`Directory.Build.props` 里 `TreatWarningsAsErrors=true` 全局生效，所以「构建通过」等价于「零 warning」。
测试输出跟随系统区域设置，中文 locale 下汇总行是中文，检索时按 `总计:` 匹配。

示例工程（单一可执行文件 `rmq`，子命令式真机联调工具）：

```bash
dotnet build examples/RocketMQ.Examples                     # 产物 bin/Debug/net10.0/rmq.dll
dotnet run --no-build --project examples/RocketMQ.Examples -- selfcheck
# selfcheck: ALL PASS (PASS=3 FAIL=0)，不依赖集群
```

装进你自己的工程，或打成包：

```bash
dotnet add reference ../csharp/src/RocketMQ.Client/RocketMQ.Client.csproj
dotnet pack src/RocketMQ.Client/RocketMQ.Client.csproj -o ./artifacts   # → RocketMQ.Client.Remoting.1.0.0.nupkg
```

代码里需要的命名空间：`RocketMQ.Client`（三种客户端 / admin / 轨迹 / 结果类型）、
`RocketMQ.Common`（`Message` / `MessageExt` / `MessageQueue` / 压缩 / 日志）、
`RocketMQ.Remoting`（`TlsOptions` / RPC 钩子）、`RocketMQ.Remoting.Protocol`
（`RequestCode` / `MessageModel` / `ConsumeFromWhere` / 路由与管理端 DTO）。

## 快速上手

### 生产者

生产者上的 NameServer 是**属性赋值**（`NamesrvAddr`），消费者与管理端是**方法**（`SetNamesrvAddr(...)`）。

```csharp
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;

var producer = new DefaultMQProducer("PID_DEMO") { NamesrvAddr = "127.0.0.1:9876" };
producer.Start();

Message msg = new("TopicTest", Encoding.UTF8.GetBytes("hello rocketmq"));
SendResult result = producer.Send(msg);                       // 同步发送，失败自动重试
Console.WriteLine($"{result.SendStatus} {result.MsgId} {result.MessageQueue} offset={result.QueueOffset}");

producer.Shutdown();
```

多地址与超时：`NameServerAddresses = new List<string> { "10.0.0.1:9876", "10.0.0.2:9876" }`、
`SendMsgTimeout`、`RetryTimesWhenSendFailed`、`RetryAnotherBrokerWhenNotStoreOk`、
`SendLatencyFaultEnable`（故障规避选队列）、`MaxMessageSize`。

#### 顺序消息

```csharp
// 1) 定点投递
producer.Send(msg, new MessageQueue("TopicTest", "broker-a", 0));

// 2) 按业务键选队列：同一个 key 恒定落同一队列
producer.SendBySelector(msg, new SelectMessageQueueByHash(), orderId);

// 自定义选择器：MessageQueue Select(IReadOnlyList<MessageQueue> mqs, Message msg, string arg)
```

#### 延迟 / 定时消息

```csharp
msg.DelayTimeLevel = 3;                  // 档位由 broker 的 messageDelayLevel 定义
msg.SetDelayTimeSec(60);                 // 5.x 定时消息：60 秒后投递
msg.SetDelayTimeMs(60_000);              // 毫秒延迟
msg.SetDeliverTimeMs(deadlineUnixMs);    // 绝对投递时刻
```

#### 批量消息

```csharp
var batch = new List<Message>
{
    new("TopicTest", Encoding.UTF8.GetBytes("a")),
    new("TopicTest", Encoding.UTF8.GetBytes("b")),
};
producer.SendBatch(batch);                            // 请求码 320，broker 落成 N 条独立消息、offset 连续
producer.SendBatch(batch, new MessageQueue("TopicTest", "broker-a", 1));
producer.SendBatchAsync(batch, callback);             // 异步批量

// 自动攒批：满足条件的消息先进累加器，按时间/体积成批发出
producer.AutoBatch = true;
producer.BatchMaxDelayMs = 100;
producer.BatchMaxBytes = 1024 * 1024;
bool accepted = producer.CanBatch(msg);               // 延迟消息、%RETRY% topic 不进攒批
```

#### 单向消息

```csharp
producer.SendOneway(msg);                  // 不等响应，只保证请求写出去
producer.SendOneway(msg, new MessageQueue("TopicTest", "broker-a", 0));
```

#### 异步消息

```csharp
producer.SendAsync(msg, new DemoCallback(), timeoutMillis: 3000);
producer.SendAsync(msg, new DemoCallback(), mq: new MessageQueue("TopicTest", "broker-a", 0));

sealed class DemoCallback : ISendCallback
{
    public void OnSuccess(SendResult sendResult) { /* 恰好回调一次 */ }
    public void OnException(Exception error) { /* broker 明确回错是 MQBrokerException，其余是 MQClientException */ }
}
```

可调项：`RetryTimesWhenSendAsyncFailed`、`AsyncSenderQueueCapacity`、`ClientCallbackExecutorThreads`、
`AsyncSenderExecutor`（自带发送池），以及公平信号量背压：

```csharp
producer.EnableBackpressureForAsyncMode = true;
producer.BackPressureForAsyncSendNum = 4096;          // 在途条数闸
producer.BackPressureForAsyncSendSize = 64 * 1024 * 1024;  // 在途字节闸（按压缩前 body 计）
long left = producer.SemaphoreAsyncSendNumAvailablePermits;
```

`Shutdown()` 先拒新请求、再等发送池排空，保证每笔都跑完准备段并把报文交给传输层。

#### 事务消息

```csharp
var tx = new TransactionMQProducer("PID_TX") { NamesrvAddr = "127.0.0.1:9876" };
tx.TransactionListener = new DemoTransactionListener();
tx.Start();

TransactionSendResult r = tx.SendMessageInTransaction(msg, arg: "order-42");

sealed class DemoTransactionListener : ITransactionListener
{
    // 半消息发送成功后回调；返回什么就发什么 END_TRANSACTION
    public LocalTransactionState ExecuteLocalTransaction(Message msg, string arg) =>
        DoBusiness(arg) ? LocalTransactionState.CommitMessage : LocalTransactionState.RollbackMessage;

    // broker 回查 CHECK_TRANSACTION_STATE(39) 时调用，未知状态可留到下次回查
    public LocalTransactionState CheckLocalTransaction(MessageExt msg) =>
        LocalTransactionState.Unknow;
}
```

半消息 → 本地事务 → `END_TRANSACTION(37, oneway)` → broker 回查，全程走真实协议。
生产者会周期性向 broker 发含 `ProducerData` 的心跳，事务回查依赖它，别关掉。

#### Request-Reply

```csharp
// 请求方
Message reply = producer.Request(requestMsg, timeoutMillis: 8000);
producer.Request(requestMsg, new DemoRequestCallback(), timeoutMillis: 8000);   // 异步版

sealed class DemoRequestCallback : RequestCallback
{
    public void OnSuccess(Message? responseMessage) { }
    public void OnException(Exception? e) { }         // 超时抛 RequestTimeoutException
}

// 应答方：普通 push 消费者收到请求后，用原请求消息造应答再发出去
Message reply = RequestReply.CreateReplyMessage(requestMsg, Encoding.UTF8.GetBytes("pong"));
producer.Send(reply);
```

请求消息带 `CORRELATION_ID` / `REPLY_TO_CLIENT` / TTL，应答走 `<cluster>_REPLY_TOPIC`；
`RequestReply.IsReplyMessage(msg)` 用来区分应答流量。

#### 撤回定时消息

```csharp
Message timer = new("TopicTest", body);
timer.SetDelayTimeSec(120);
SendResult sent = producer.Send(timer);

string? handle = sent.RecallHandle;                                 // 普通消息恒为 null
HandleV1 parsed = RecallMessageHandle.DecodeHandle(handle!);        // Topic / BrokerName / TimestampStr / MessageId
string uniqKey = producer.RecallMessage("TopicTest", handle!);      // 返回被撤回消息的 uniqKey
```

撤回需要 broker 开 `recallMessageEnable=true`。

### PushConsumer（并发 / 顺序 / 广播 / POP）

```csharp
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

var consumer = new DefaultMQPushConsumer("GID_DEMO");
consumer.SetNamesrvAddr("127.0.0.1:9876");
consumer.Subscribe("TopicTest", "tagA || tagB");            // 或 MessageSelector.BySql("price > 10")
consumer.SetMessageListener(new DemoListener());
consumer.Start();
// ... 退出前
consumer.Shutdown();

sealed class DemoListener : IMessageListenerConcurrently
{
    public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context)
    {
        foreach (MessageExt m in msgs)
            Console.WriteLine($"{m.MsgId} {Encoding.UTF8.GetString(m.Body)} reconsume={m.ReconsumeTimes}");
        return ConsumeConcurrentlyStatus.ConsumeSuccess;     // ReconsumeLater ⇒ 回投 %RETRY%
    }
}
```

顺序消费：

```csharp
consumer.SetMessageListener(new OrderlyListener());

sealed class OrderlyListener : IMessageListenerOrderly
{
    public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context)
    {
        context.SuspendCurrentQueueTimeMillis = 3000;        // 失败时本队列挂起时长
        return ConsumeOrderlyStatus.Success;                 // 或 SuspendCurrentQueueAMoment
    }
}
```

广播消费（位点本地、不走 broker 重置）：

```csharp
consumer.MessageModel = MessageModel.Broadcasting;           // 默认 MessageModel.Clustering
```

POP 模式（同一实例多队列共享消费线程、逐条 ack、可延长不可见时间）：

```csharp
consumer.PopMode = true;
consumer.PopInvisibleTime = 60_000;      // 合法区间 5000 ~ 300000
consumer.PopBatchNums = 32;
consumer.PopPollTimeMillis = 15_000;
consumer.PopThresholdForQueue = 96;
```

常用调优：`ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset`（另有
`ConsumeFromLastOffset` / `ConsumeFromTimestamp`）、`SetConsumeThreadNums(n)`、
`PullBatchSize`、`PullThresholdForQueue` / `PullThresholdSizeForQueue` / `PullThresholdForTopic`
（拉取前流控）、`MaxReconsumeTimes`、`SuspendCurrentQueueTimeMillis`、
`SetAllocateMessageQueueStrategy(new AllocateMessageQueueConsistentHash())`、
`Suspend()` / `Resume()`（暂停拉取但不掉线）、`SetEnableMsgTrace(true)`、
`consumer.SubscribedTopics()` / `ConsumedCount` / `ClientId`。
运行中改并发度：`consumer.UpdateCorePoolSize(32)`、`GetCorePoolSize()`。

### PullConsumer（主动拉取，位点自己管）

```csharp
var pull = new DefaultMQPullConsumer("GID_PULL");
pull.SetNamesrvAddr("127.0.0.1:9876");
pull.Start();

foreach (MessageQueue mq in pull.FetchSubscribeMessageQueues("TopicTest"))   // 整个 topic 的队列
{
    pull.FetchConsumeOffset(mq, out long offset);
    PullResult pr = pull.Pull(mq, "tagA", offset, maxNums: 32);              // 短轮询
    if (pr.IsFound) Deliver(pr.MsgFoundList);
    pull.UpdateConsumeOffset(mq, pr.NextBeginOffset);                        // 位点由调用方推进
}

PullResult blocked = pull.PullBlockIfNotFound(mq, "*", offset, 32);          // 长轮询，挂到有消息或超时
List<MessageQueue> mine = pull.FetchMessageQueuesInBalance("TopicTest");     // 只给本实例应得的那份
long off = pull.SearchOffset(mq, timestampMs);
Console.WriteLine($"{pull.MinOffset(mq)} {pull.MaxOffset(mq)} {pull.EarliestMsgStoreTime(mq)}");
pull.SendMessageBack(rejectedMsg, delayLevel: 3);                            // 主动回投重投
pull.CreateTopic("TBW102", "NewTopic", queueNum: 4);
pull.Shutdown();
```

队列变更通知：`pull.SetMessageQueueListener(listener)` 或
`pull.RegisterMessageQueueListener(topic, listener)`（`IMessageQueueListener.MessageQueueChanged(mqAll, mqDivided)`）。
拉模式消费者默认会发消费者心跳：`HeartbeatEnabled`、`HeartbeatBrokerIntervalMillis`。

### LitePullConsumer（自己拉、自己提交，含队列集合变更监听）

```csharp
var lite = new DefaultLitePullConsumer("GID_LITE");
lite.SetNamesrvAddr("127.0.0.1:9876");
lite.SetInstanceName("lite-demo");
lite.SetPollTimeoutMillis(1000);
lite.SetPullBatchSize(32);
lite.SetAutoCommit(false);
lite.SetAutoCommitIntervalMillis(5000);
lite.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
lite.Subscribe("TopicTest", "tagA");                       // 订阅模式：由分配策略分队列
// lite.Assign(pull.FetchSubscribeMessageQueues("TopicTest"));  // 手动指派模式
// lite.SetSubExpressionForAssign("TopicTest", "tagB");         // 指派模式下另设过滤表达式

lite.SetTopicMetadataCheckIntervalMillis(1000);            // 队列集合比对周期（下限 1s）
lite.RegisterTopicMessageQueueChangeListener("TopicTest", new QueueChangeListener());
lite.Start();

while (running)
{
    List<MessageExt> msgs = lite.Poll(3000);
    Handle(msgs);
    lite.Commit();                                         // 提交本地已消费位点
}

lite.Seek(mq, 100); lite.SeekToBegin(mq); lite.SeekToEnd(mq);
lite.Commit(new Dictionary<MessageQueue, long> { [mq] = 200 });   // 显式指定位点提交
long committed = lite.Committed(mq);
Console.WriteLine($"{lite.PullCursorOf(mq)} {lite.ConsumeCursorOf(mq)} {lite.PendingCommitOf(mq)}");
lite.Pause(new[] { mq }); lite.Resume(new[] { mq });
lite.Assignment(); lite.FetchMessageQueues("TopicTest"); lite.OffsetForTimestamp(mq, timestampMs);
lite.Shutdown();

sealed class QueueChangeListener : ITopicMessageQueueChangeListener
{
    // topic 的订阅队列集合真的变了（扩容 / 缩容）才回调；没变不回调
    public void OnChanged(string topic, IReadOnlyList<MessageQueue> messageQueues) =>
        Console.WriteLine($"{topic} -> {string.Join(",", messageQueues.Select(mq => mq.QueueId))}");
}
```

本实例分到哪些队列的通知走另一个接口：`lite.SetMessageQueueListener(...)`
（`ILiteMessageQueueListener.MessageQueueChanged(mqAll, mqDivided)`）。
监听器若想在启动后注册，就不会吃到「首轮回调」：只有运行中注册才立刻记一版快照，
未启动时注册的那位首趟一定回调一次。
队列集合比对每趟都现问 NameServer，不吃 30s 路由缓存 —— 真机扩缩容的到达延迟在秒级
（见 `lite-queue-change` 用例）。

### Admin

```csharp
using RocketMQ.Client;
using RocketMQ.Remoting.Protocol;

var admin = new DefaultMQAdminExt();                       // 默认 instanceName 为 "ADMIN"
admin.SetNamesrvAddr("127.0.0.1:9876");
admin.SetTimeoutMillis(15000);
admin.Start();

TopicList topics = admin.FetchAllTopicList();
TopicRouteData route = admin.ExamineTopicRoute("TopicTest");
ClusterInfo cluster = admin.FetchBrokerClusterInfo();
TopicConfig config = admin.ExamineTopicConfig(brokerAddr, "TopicTest");
TopicStatsTable stats = admin.ExamineTopicStats("TopicTest");
ConsumeStats cstats = admin.ExamineConsumeStats(brokerAddr, "GID_DEMO", "TopicTest");
ConsumeStats byGroup = admin.ExamineConsumeStatsGroup("GID_DEMO");

admin.CreateTopic("TBW102", "NewTopic", queueNum: 4);
admin.CreateAndUpdateTopicConfig(brokerAddr, new TopicConfig("NewTopic2"));
admin.CreateAndUpdateSubscriptionGroupConfig(brokerAddr, new SubscriptionGroupConfig("GID_NEW"));
admin.DeleteTopic("NewTopic");
admin.DeleteSubscriptionGroup(brokerAddr, "GID_NEW", removeOffset: true);

SortedDictionary<MessageQueue, long> reset = admin.ResetOffsetByTimestamp("TopicTest", "GID_DEMO", timestampMs);
admin.ResetOffsetNew("GID_DEMO", "TopicTest", timestampMs);
admin.ResetOffsetByQueueId(brokerAddr, "GID_DEMO", "TopicTest", queueId: 0, resetOffset: 123);
admin.CloneGroupOffset(brokerAddr, srcGroup, destGroup, timestampMs, isClone: false);

List<MessageExt> byKey = admin.QueryMessage("TopicTest", key: "order-42", maxNum: 32, begin, end);
MessageExt one = admin.ViewMessage("TopicTest", msgId);
Console.WriteLine($"{admin.MinOffset(mq)} {admin.MaxOffset(mq)} {admin.EarliestMsgStoreTime(mq)}");
admin.SearchOffset(mq, timestampMs);
admin.SearchLowerBoundaryOffset(mq, timestampMs);         // 边界语义：LOWER
admin.SearchUpperBoundaryOffset(mq, timestampMs);         // 边界语义：UPPER

PropertyMap conf = admin.GetBrokerConfig(brokerAddr);
conf["flushDiskType"] = "ASYNC_FLUSH";
admin.UpdateBrokerConfig(brokerAddr, conf);
KvTable runtime = admin.FetchBrokerRuntimeStats(brokerAddr);
admin.WipeWritePermOfBroker("127.0.0.1:9876", "broker-a");
admin.Shutdown();
```

### ACL

```csharp
// 最省事：三类客户端与 admin 都有同名方法
producer.SetCredentials(accessKey, secretKey);
consumer.SetCredentials(accessKey, secretKey, securityToken);   // STS 三元组

// 或者直接注入钩子（必须在 Start() 之前）
producer.SetRpcHook(new AclClientRPCHook(new SessionCredentials(accessKey, secretKey)));
```

签名内容 = 请求扩展字段的值按字段名 Ordinal 序拼接、再接请求 body，HMAC-SHA1 + 标准 Base64
（`AclClientRPCHook.BuildRequestContent` / `CalcSignature` / `HmacSha1Base64` 可单独取证，
`Signature` 自身不入签名内容）。钩子链顺序由 `RequestHooks.Compose(enableStreamRequestType, userHook, nsV2Getter)`
还原（Namespace → stream → 用户钩子），所以 `SetRpcHook` 传进来的 ACL 钩子会排在最后、
签名内容包含已就位的 `ns` / `ReqT` 字段。联调用例：`rmq acl`（需 broker 开 `authenticationEnabled=true`，
配套 `../scripts/run_acl_live.sh`）。

### 命名空间

两套彼此独立，可以同时用：

```csharp
// 1) 本地资源名前缀：上线前把资源名写成 <ns>%<resource>，回退/重试 topic 一并套上
producer.Namespace = "myNs";
consumer.Namespace = "myNs";
lite.Namespace = "myNs";
pull.Namespace = "myNs";
string wrapped = NamespaceUtil.WrapNamespace("myNs", "TopicTest");   // "myNs%TopicTest"
string bare = NamespaceUtil.WithoutNamespace(wrapped, "myNs");       // "TopicTest"

// 2) 服务端命名空间：给每笔请求盖 nsd=true / ns=<值> 扩展头，资源名本身不动
producer.NamespaceV2 = "my-ns-v2";
consumer.NamespaceV2 = "my-ns-v2";
pull.NamespaceV2 = "my-ns-v2";
lite.NamespaceV2 = "my-ns-v2";
admin.NamespaceV2 = "my-ns-v2";
```

生产者、三种消费者、管理端、轨迹分发器都暴露 `NamespaceV2`；钩子每笔请求现读该属性，
`Start()` 之后改也从下一笔请求生效。

### 压缩

```csharp
producer.CompressMsgBodyOverHowmuch = 4 * 1024;    // body 超过这个字节数才压
producer.CompressLevel = 5;
producer.CompressType = CompressionType.ZSTD;      // CompressionType.ZLIB / LZ4 / ZSTD

// 手动压/解（消费者侧自动解压，无需干预）
byte[] packed = CompressorFactory.Compress(body, CompressionType.ZLIB, level: 5);
byte[] original = CompressorFactory.Decompress(packed, CompressionType.ZLIB);
Console.WriteLine($"{CompressorFactory.HasZlibSupport()} {CompressorFactory.HasLz4Support()} {CompressorFactory.HasZstdSupport()}");
```

zlib 由内置 `ZLibStream` 提供，恒可用；LZ4（LZ4 Frame）与 ZSTD 走 P/Invoke 调系统库
（macOS `/usr/local/lib/liblz4.dylib`、`libzstd.dylib`；Linux `liblz4.so.1`、`libzstd.so.1`），
装不到时该后端抛异常，不会把「没压的字节」当压缩字节透传。
队列分配策略共六种：`AllocateMessageQueueAveragely`、`AllocateMessageQueueAveragelyByCircle`、
`AllocateMessageQueueByConfig`、`AllocateMessageQueueConsistentHash`、
`AllocateMessageQueueByMachineRoom`、`AllocateMachineRoomNearby`。

### TLS

```csharp
producer.TlsEnable = true;                    // 或用环境变量 ROCKETMQ_TLS_ENABLE=1
consumer.TlsEnable = true;
producer.TlsOptions = new TlsOptions
{
    CaCert = "/path/ca.pem",                  // 严格校验证书链
    ServerName = "127.0.0.1",                 // 校验 SAN / CN
    ClientCert = "/path/client.pem",          // 双向认证（broker 需开 authClient）
    ClientKey = "/path/client.key",
};
```

不给 `CaCert` 时是 test-mode：信任 broker 的自签证书、不校验主机名。给了 `CaCert` 就是严格校验：
证书必须链到该 CA 且主机名 / SAN 匹配（默认用连接 host，`ServerName` 可覆盖），
此时 broker 侧要真的以 TLS 提供服务 —— 一条命令跑完三腿（plain / ca_verify / mtls）：
`bash ../scripts/run_csharp_tls_live.sh`（它会自起带证书的集群并自行收工）。
每条出连接恰好一个读线程 + 一把写锁串行化写侧，这是 `SslStream` 的并发契约，勿给同一条连接加第二个读者。

## 特性与进度

- ✅ 协议层：`RemotingCommand` 帧编解码；JSON / RocketMQ 二进制双序列化；V2 单字母短键 header；
  非法 JSON 输出容错解析；17 段 + 6 段消息编解码
- ✅ 传输层：Socket TCP 长连接惰性建连、每连接读线程、分帧、opaque→future 分发；
  同步 / 异步 / oneway；半包重组；超时与重连；连接断开时在途请求立即判死
- ✅ 发送：同步 / 定点 / 队列选择器 / 批量（320）/ 自动攒批 / 单向 / 异步（发送池 + 公平背压信号量）/
  事务（两阶段 + broker 回查）/ 定时与延迟 / 撤回（recallMessage）/ Request-Reply
- ✅ 消费：Push（长轮询 + POP + 顺序 + 广播 + 位点持久化 + 启动期数值校验 + 拉取前流控 +
  挂起 listener 的清扫逃生口）、Pull（带消费者心跳）、LitePull（拉取 / 已消费 / 待提交三张位点表 +
  队列集合变更监听）
- ✅ 队列分配：六种可插拔策略
- ✅ 命名空间：本地前缀与服务端 `nsd`/`ns` 两套
- ✅ 管理端：topic / 订阅组 / 集群与路由 / 各类统计 / 消息查询 / 位点重置 / `searchOffset` 边界语义 /
  broker 配置读写
- ✅ 观测与安全：消息轨迹（编码 + 异步分发 + 三类钩子）、消费统计、`ConsumerRunningInfo`(307)、
  发送 / 消费 / 事务 / Forbidden / FilterMessage 钩子、ACL 签名、动态 NameServer 取址、故障规避选队列
- ✅ 压缩：zlib / LZ4 / ZSTD；TLS：`SslStream`，test-mode 与严格校验 / mTLS
- ✅ 客户端日志：按大小轮转、双写 stderr 与文件
- ✅ 单元测试 808 项；`rmq` 真机联调工具 38 条子命令（含主从、故障注入、压缩互通矩阵）

## 客户端日志

行格式：

```
2026-10-10 16:29:45.032 INFO  [3869] [main] [MqClient.cs:438] - MQClientInstance[...] started, namesrv=127.0.0.1:9876
```

即 `时间(毫秒) 级别 [pid] [线程名] [文件:行号] - 消息`，同时写 stderr 与文件，每行之后 Flush。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` / `OFF` |
| `ROCKETMQ_CLIENT_LOG_FILE` | `$HOME/logs/rocketmqlogs/rocketmq_csharp_client.log` | 设为 `OFF` / `NONE` / 空串 ⇒ 只留 stderr |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864`（64MB） | 单文件上限，按大小轮转；备份名 `<file>.1` … `<file>.N`，不压缩 |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | 保留的备份份数 |

级别与文件路径在**首次写日志时**求值并缓存，所以环境变量必须在第一条日志之前设好。
程序内可随时改：

```csharp
using RocketMQ.Common;

ClientLog.SetLogLevel(LogLevel.Debug);
ClientLog.SetLogFile("/tmp/rmq-client.log");
ClientLog.SetLogFileLimits(maxSize: 8 * 1024 * 1024, maxIndex: 3);
ClientLog.SetThreadName("worker-1");        // 内部线程已命名：ConsumeMessageThread_N / AsyncSenderThread_N / RemotingClientReader-<addr>
```

工作线程由客户端命名，未命名线程回落 `main` 或 `tid-xxxx`。

## 真实集群联调

约定：需要 namesrv `9876` + broker `10911`、`autoCreateTopicEnable=true`；每条用例**自断言**，
全部通过退出 `0`，有失败退出非 `0`（未知子命令 / 参数不足是 `2`，未捕获异常是 `1`）。
用法统一为：

```bash
dotnet run --no-build --project examples/RocketMQ.Examples -- <case> 127.0.0.1:9876
```

**先起消费者、再发消息**：先建好 topic，再起消费者并等它把队列分到 / 心跳发出去，然后发送。
反过来做的话早到的消息要么落到还没建好的默认路由上，要么被 `ConsumeFromLastOffset` 的起点跳过，
用例会偶发「一条也没收到」的假失败。部分用例自带预建 topic 与等待逻辑，改写时保持这个次序。

本机实测的一条腿（队列集合变更监听，真实扩缩容 + 1s 检查周期）：

```bash
dotnet run --no-build --project examples/RocketMQ.Examples -- lite-queue-change 127.0.0.1:9876
# LitePull queue-change live (C#): PASS=10 FAIL=0     退出码 0
```

子命令清单：

| 子命令 | 验什么 |
| --- | --- |
| `selfcheck` | 本地协议编解码自检，不依赖集群 |
| `interop --emit` / `interop --decode <hex>` | 打印 / 反向解出规范帧 hex（JSON 与 RocketMQ 二进制双序列化），供字节级互通核对 |
| `message-types` | 8 类消息能力：异步 / 顺序 / Tag / 用户属性 / 延迟 / Key / 事务 / 批量（320 落成 N 条独立消息） |
| `admin-live` | 管理端全链路 |
| `admin-batch-live` | 批量类管理请求：批量建 topic(18) / 订阅组(225)、读禁配(353)、恢复半消息(323)、顺序 topic 配置、清理类(306/329/316)、消费时间跨度(303)、nameserver 配置(318/319) |
| `compression-live selftest\|send\|recv <namesrv> <topic> <group> <size> [codec]` | 压缩真实性：真机发送 → 消费 → 解压 → CRC；`send` / `recv` 作为 `../scripts/compression_matrix.sh` 的一端 |
| `redelivery` | 重投 / 死信 / 重启续投 / 顺序 / 广播 / 流控 / rebalance / 部分 ack / 停摆自愈 / 显式 ack 回滚 / 空应答位点修正 |
| `acl <namesrv> [ak] [sk]` | ACL 鉴权链路（需 broker `authenticationEnabled=true`，配 `../scripts/run_acl_live.sh`） |
| `pull` | 主动拉取：手动拉取 / 手动位点 / 回投 |
| `pull-heartbeat <namesrv> <master> [slave]` | 拉模式消费者心跳（主从集群：203/38 主从可见、35 注销即摘、幽灵组对照） |
| `lite-pull` | lite-pull 全链路：subscribe / assign 双模式 + poll + 六种分配策略 + 三张位点表 |
| `lite-pull-cursor` | 拉取游标跟随 `nextBeginOffset`、OFFSET_ILLEGAL 越界自愈 |
| `lite-pull-code` | 请求码 361 与 `litePullMessageEnable` 开关（运行时翻开关再还原） |
| `lite-queue-change` | topic 队列集合变更监听：1s 周期 + 真实扩容 2→4 / 缩容 4→2，证明比对趟次现查路由 |
| `rr`（别名 `reqreply`） | Request-Reply：请求 / 应答 / 并发不串台 / 无应答时超时 |
| `latency` | 发送延迟故障规避：默认关 / 开 / 隔离退化链 / 到期恢复 |
| `pop` | POP 协议：POP / ACK / 延长不可见 / 复活重投 |
| `popc` | POP 消费侧：消费循环 / ack / 延迟重投 |
| `trace` | 消息轨迹全链路（需 broker `traceTopicEnable=true`） |
| `hook` | CheckForbidden / FilterMessage 钩子 |
| `backpressure` | 异步发送背压公平信号量：条数与字节闸、拒绝对账、运行时扩容 |
| `flow-control` | 拉取前流控五个阈值 + 启动期数值闸门 |
| `async-send` | 异步发送内核：不阻塞调用方、线程口径、并发不串台、`Shutdown` 排空 |
| `validators-live` | 名字校验：本地快拒 + 合法名字收发 + 寻址故障定性 |
| `recall` | 定时消息撤回（自动打开并在退出时还原 `recallMessageEnable`） |
| `unit-config` | `unitName` / `unitMode` / stream：clientId 后缀、topic UNIT 位、`%RETRY%` UNIT_SUB 位 |
| `send-header` | 发送头 c/d/n 三字段（模板 topic 决定自动建出的队列数） |
| `sql92` | SQL92 过滤 + CHECK_CLIENT_CONFIG(46)（需 broker `enablePropertyFilter=true`） |
| `unreg-live` | 退出注销 UNREGISTER_CLIENT(35) |
| `fail-fast` | broker 真死时在途请求立刻判死（⚠ 会停一次 broker，跑完拉起，store 不删） |
| `scheduled-intervals` | 周期任务 initialDelay / 固定速率（一轮要数分钟） |
| `subscribe` | `Start()` 之后订阅立即推心跳、新 topic 真被消费 |
| `pinned-guard` | 定点发送 topic 守卫：真路由不误拒、拒在本端且 broker 无痕 |
| `offset-illegal` | OFFSET_ILLEGAL：在途 / 缓冲整批作废 + 修正位点立刻落盘 |
| `reset-offset` | 220 重置位点：当场落盘 + 在途批次作废 + 队列按新位点重建（收尾还原 `useServerSideResetOffset`） |
| `publish-route-master <namesrv> <master> [slave]` | 发布地址只认 master（⚠ 会停一次 master，主从集群） |
| `clean-expired-msg` | 挂起 listener 的清扫逃生口（约 4 分钟，等两个清扫周期） |
| `tls <namesrv> <topic> <group> [plain\|ca_verify\|mtls]` | TLS 传输层压测 + TLS 全链路收发（见「TLS」） |

需要额外 broker 配置的腿：`trace`（`traceTopicEnable=true`）、`sql92`（`enablePropertyFilter=true`）、
`acl`（`authenticationEnabled=true`）、`recall` / `lite-pull-code` / `reset-offset` /
`admin-batch-live`（用例会自己翻开关并还原）、`tls` 的 `ca_verify` / `mtls`（broker 侧真起 TLS）、
`fail-fast` / `publish-route-master` / `pull-heartbeat`（主从与停启，走 `../scripts/rmq_test_broker.sh`）。
POP 与广播不需要额外开关，`popc` / `pop` 直接跑。

## 目录结构

```
csharp/
├── Directory.Build.props           # Nullable + TreatWarningsAsErrors 全局生效
├── RocketMQ.slnx                   # src / tests / examples 三个工程
├── src/RocketMQ.Client/
│   ├── Common/                     # 公共层
│   │   ├── ByteBuffer.cs           #   ByteWriter/ByteReader + LegacyHash/LegacyNumber（线上格式用的字符串 hashCode 与溢出语义）
│   │   ├── UtilAll.cs              #   时间/IP/CRC32/hex（全部显式 InvariantCulture）
│   │   ├── MixAll.cs  MessageConst.cs  SysFlag.cs（MessageSysFlag/PullSysFlag/PermName）
│   │   ├── Message.cs              #   Message/MessageExt/MessageBatch（引用语义，发送前由 Producer 克隆）
│   │   ├── MessageDecoder.cs       #   17 段存储格式 + 6 段批量格式编解码
│   │   ├── Compression.cs          #   zlib / LZ4 / ZSTD 三后端；类型位 0/3 = ZLIB；不支持的类型抛异常
│   │   ├── NativeCompression.cs    #   P/Invoke 到系统 liblz4（LZ4 Frame）/ libzstd，缺库时该后端抛异常
│   │   ├── SubscriptionData.cs     #   FilterAPI.BuildSubscriptionData / Equals 语义
│   │   ├── NamespaceUtil.cs        #   本地资源名前缀 <ns>%<resource> 的套/拆
│   │   ├── BoundaryType.cs         #   时间戳查位点的边界语义（LOWER/UPPER，含宽松解析）
│   │   ├── ConsistentHash.cs  TopicConfig.cs  TopicValidator.cs
│   │   └── ClientLog.cs            #   客户端日志：按大小轮转 + 线程名 + 毫秒 + 文件:行号
│   ├── Remoting/
│   │   ├── RemotingClient.cs       # Socket TCP：惰性建连、每连接读线程、分帧、opaque→future 分发、TLS
│   │   ├── TlsOptions.cs  RpcHook.cs（AclClientRPCHook / StreamTypeRPCHook / NamespaceRpcHook / RequestHooks）
│   │   ├── Exception.cs
│   │   └── Protocol/
│   │       ├── Json.cs             # 容错 JSON：容忍上游的非法输出（对象键内联/裸数字键/NaN/尾逗号）
│   │       ├── Serialize.cs        # RemotingSerializable(JSON) + RocketMQSerializable(二进制)
│   │       ├── Codes.cs            # RequestCode/ResponseCode/LanguageCode
│   │       ├── Headers.cs          # CommandCustomHeader + V1<->V2
│   │       ├── RemotingCommand.cs  # JSON/ROCKETMQ 双序列化、header V1/V2
│   │       ├── Route.cs  Heartbeat.cs  Subscription.cs
│   │       └── Body.cs  AdminBody.cs   # 管理端响应 DTO
│   └── Client/
│       ├── MqClient.cs             # MQClientInstance：路由发现 + TBW102 回退裁剪、心跳、位点请求、Schedules（定时任务按绝对时刻锚定）
│       ├── Producer.cs             # DefaultMQProducer / TransactionMQProducer：同步/定点/选择器/批量/单向/异步/事务
│       ├── Consumer.cs             # DefaultMQPushConsumer：拉取循环、并发/顺序/POP、sendMessageBack
│       ├── PullConsumer.cs  LitePullConsumer.cs
│       ├── Admin.cs                # DefaultMQAdminExt 全套
│       ├── ProduceAccumulator.cs   # 自动攒批
│       ├── Result.cs               # SendResult/PullResult/监听器接口/队列选择器
│       ├── RequestReply.cs  RecallMessageHandle.cs
│       ├── AllocateStrategy.cs     # 六种队列分配策略
│       ├── Hook.cs                 # Send/Consume/EndTransaction + CheckForbidden/FilterMessage 钩子与上下文
│       ├── Trace.cs  TraceHook.cs  TraceDispatcher.cs  TraceParentContext.cs
│       ├── BackPressure.cs  Latency.cs  ConsumeExecutor.cs  Schedules（定时任务锚定）
│       ├── TopAddressing.cs        # 动态 NameServer 取址
│       ├── ConsumerStats.cs  Validators.cs  ClientException.cs
├── tests/RocketMQ.Client.Tests/    # xunit，808 项（编解码/路由/日志/传输/轨迹/异步/流控/位点…）
└── examples/RocketMQ.Examples/     # rmq 真机联调工具（见上）
```

## License

Apache-2.0。
