# RocketMQ C# 客户端（remoting 协议）

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 C# 实现，覆盖生产者 / 推送与拉取消费者 /
管理端 / 消息轨迹 / 事务与定时消息全链路。适配 RocketMQ 4.x / 5.x 集群，全部能力在真实
5.5.1 集群上联调验证过；与本仓库的 Python / C++ / Rust 实现逐项对齐。

**零外部 NuGet 依赖**（仅 BCL）；zlib 走 `System.IO.Compression.ZLibStream`，
LZ4 / ZSTD 走 P/Invoke 调**系统库**（`/usr/local/lib/liblz4.dylib`、`libzstd.dylib`，
Linux 上是 `liblz4.so.1` / `libzstd.so.1`）——装不到时该后端抛异常而不是静默透传压缩字节。

## 构建 / 测试

```bash
cd csharp
dotnet build                              # 全解决方案，0 warning（TreatWarningsAsErrors 全局开启）
dotnet test tests/RocketMQ.Client.Tests   # xunit，740 项测试
```

要求 .NET 10 SDK。

## 快速上手

```csharp
using System.Text;
using RocketMQ.Client;

var producer = new DefaultMQProducer("PID_DEMO");
producer.NamesrvAddr = "127.0.0.1:9876";
producer.Start();
producer.Send(new Message("TopicTest", Encoding.UTF8.GetBytes("hello rocketmq")));
producer.Shutdown();

var consumer = new DefaultMQPushConsumer("GID_DEMO");
consumer.SetNamesrvAddr("127.0.0.1:9876");
consumer.Subscribe("TopicTest");
consumer.SetMessageListener(new DemoListener());
consumer.Start();
// ... 收到退出信号后
consumer.Shutdown();


class DemoListener : IMessageListenerConcurrently
{
    public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context)
    {
        foreach (var msg in msgs)
            Console.WriteLine(Encoding.UTF8.GetString(msg.Body));
        return ConsumeConcurrentlyStatus.ConsumeSuccess;
    }
}
```

注意两处 API 形状不同：生产者的 name server 是**属性赋值**（`producer.NamesrvAddr = ...`），
消费者与管理端是方法（`SetNamesrvAddr(...)`）。

## 功能一览

| 领域 | 内容 |
| --- | --- |
| 协议层 | `RemotingCommand` 帧编解码；JSON / RocketMQ 二进制双序列化；V2 单字母短键 header；fastjson2 非法输出容错解析；17 段 + 6 段消息编解码 |
| 传输层 | Socket TCP 长连接惰性建连、每连接读线程、分帧、opaque→future 分发；同步 / 异步 / oneway；半包重组；超时与重连；连接断开时在途请求立即判死 |
| 发送 | 同步 / 定点 / 队列选择器 / 批量 / 单向 / 异步（真异步发送池 + 两个公平背压信号量）/ 事务消息（两阶段 + broker 回查）/ 定时消息撤回（recallMessage） |
| 消费 | Push Consumer（长轮询 + POP + 顺序 + 广播 + 位点持久化 + 启动期数值校验 + 拉取前流控五阈值 + 挂起 listener 的清扫逃生口）、Pull Consumer（带消费者心跳）、Lite Pull Consumer（拉取/已消费/内存提交三张位点表） |
| 队列分配 | 六个可插拔策略：`AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>` |
| 管理端 | `DefaultMQAdminExt`：topic / 订阅组 / 集群信息 / 各类统计 / 消息查询 / 位点重置 / `searchOffset` 边界语义 |
| 观测与安全 | 消息轨迹（编码 + 异步分发 + 钩子）、消费统计、`ConsumerRunningInfo`(307)、发送/消费/事务/Forbidden/FilterMessage 钩子、ACL 签名、动态 NameServer 取址、故障规避选队列 |

## 真机联调工具

需要本地 RocketMQ 集群（namesrv 9876 + broker 10911，`autoCreateTopicEnable=true`）。
集群启停参考 `../scripts/` 下的脚本。工具全部自断言、失败以非 0 退出码收口：

```bash
PROG=examples/RocketMQ.Examples/bin/Debug/net10.0/rmq.dll

dotnet $PROG selfcheck                    # 本地自检（无需集群）
dotnet $PROG interop --emit               # 打印规范帧 hex（JSON/ROCKETMQ 双序列化）；--decode <hex> 反向解码，供跨语言字节级互通验证
dotnet $PROG message-types 127.0.0.1:9876   # 8 类消息能力（含批量 320 在 broker 上落成 3 条独立消息）
dotnet $PROG admin-live 127.0.0.1:9876    # 管理端全链路
dotnet $PROG compression-live selftest 127.0.0.1:9876    # 压缩真实性：真机发送→消费→解压→CRC
dotnet $PROG compression-live send|recv 127.0.0.1:9876 <topic> <group> <size> [codec]   # 作为 ../scripts/compression_matrix.sh 的一端参与四语言矩阵
dotnet $PROG trace 127.0.0.1:9876         # 消息轨迹全链路（需 broker traceTopicEnable=true）
dotnet $PROG hook 127.0.0.1:9876          # CheckForbidden / FilterMessage 钩子
dotnet $PROG backpressure 127.0.0.1:9876  # 异步发送背压公平信号量
dotnet $PROG async-send 127.0.0.1:9876    # 异步发送内核：线程口径、offsetMsgId 读回原文、批量异步、Shutdown 排空
dotnet $PROG fail-fast 127.0.0.1:9876     # broker 真死时在途请求立刻判死（⚠ 会停一次 broker，跑完拉起，store 不删）
dotnet $PROG validators-live 127.0.0.1:9876   # 名字校验（含寻址故障定性：没配地址 ⇒ 10004 快拒）
dotnet $PROG recall 127.0.0.1:9876        # 定时消息撤回（自动开关并在退出时还原 recallMessageEnable）
dotnet $PROG unit-config 127.0.0.1:9876   # unitName / unitMode / stream
dotnet $PROG send-header 127.0.0.1:9876   # 发送头 c/d/n 三字段（自动建 topic 的队列数算术）
dotnet $PROG flow-control 127.0.0.1:9876  # 拉取前流控五个阈值 + 启动期数值闸门
dotnet $PROG sql92 127.0.0.1:9876         # SQL92 过滤 + CHECK_CLIENT_CONFIG(46)（需 broker enablePropertyFilter=true）
dotnet $PROG scheduled-intervals 127.0.0.1:9876   # 周期任务的 initialDelay / 固定速率（跑一轮要数分钟）
dotnet $PROG subscribe 127.0.0.1:9876     # 后置订阅立即心跳
dotnet $PROG pinned-guard 127.0.0.1:9876  # 定点发送 topic 守卫：真路由不误拒、拒在本端且 broker 无痕
dotnet $PROG offset-illegal 127.0.0.1:9876   # OFFSET_ILLEGAL：在途/缓冲整批作废 + 修正位点立刻落盘
dotnet $PROG reset-offset 127.0.0.1:9876  # 220 重置消费位点：当场落盘 + 在途批次作废 + 队列按新位点重建（收尾还原 useServerSideResetOffset）
dotnet $PROG pull-heartbeat 127.0.0.1:9876 127.0.0.1:10911 [从节点地址]   # 拉模式消费者的心跳（主从集群）
dotnet $PROG lite-pull 127.0.0.1:9876     # lite-pull 全链路（含六种分配策略与三张位点表）
dotnet $PROG lite-pull-cursor 127.0.0.1:9876   # 拉取游标跟随 nextBeginOffset / OFFSET_ILLEGAL 越界自愈
dotnet $PROG lite-pull-code 127.0.0.1:9876   # 请求码 361 / litePullMessageEnable 开关（运行时翻开关再还原）
dotnet $PROG publish-route-master 127.0.0.1:9876 127.0.0.1:10911 [从节点地址]   # 发布地址只认 master（⚠ 会停一次 master，主从集群）
dotnet $PROG clean-expired-msg 127.0.0.1:9876   # 挂起 listener 的清扫逃生口（约 4 分钟，等两个清扫周期）
dotnet $PROG tls 127.0.0.1:9876 <topic> <group>   # TLS 传输层压测 + TLS 全链路收发（见「TLS」）
dotnet $PROG redelivery 127.0.0.1:9876    # 重投/死信/重启/顺序/广播/流控/rebalance/部分 ack/停摆自愈/显式 ack 回滚/空应答位点修正
dotnet $PROG unreg-live 127.0.0.1:9876    # 生产者退出注销 UNREGISTER_CLIENT(35)
```

其余子命令：`acl`（ACL 鉴权链路，需开启鉴权的 broker，参考 `../scripts/run_acl_live.sh`）、
`pull`、`rr`（request-reply 全链路）、`latency`（故障规避）、`pop` / `popc`（POP 协议与消费循环）、
`reqreply`。部分用例会删掉自己建的 topic（脚本头注释里写明）。

## 目录结构

```
csharp/
├── Directory.Build.props           # 全局 Nullable + TreatWarningsAsErrors + InvariantGlobalization 提示
├── src/RocketMQ.Client/
│   ├── Common/                     # 公共层
│   │   ├── ByteBuffer.cs           #   ByteWriter/ByteReader/JavaHash（broker 侧使用的字符串 hashCode 语义）
│   │   ├── UtilAll.cs              #   时间/IP/CRC32/hex 等（全部显式 InvariantCulture）
│   │   ├── MixAll.cs  MessageConst.cs  SysFlag.cs（MessageSysFlag/PullSysFlag/PermName）
│   │   ├── Message.cs              #   Message/MessageExt/MessageBatch（引用语义，发送前由 Producer 克隆）
│   │   ├── MessageDecoder.cs       #   17 段存储格式 + 6 段批量格式编解码
│   │   ├── Compression.cs          #   zlib / LZ4 / ZSTD 三后端；类型位 0/3 = ZLIB；未支持类型必须抛异常（不透传）
│   │   ├── NativeCompression.cs    #   P/Invoke 到系统 liblz4（LZ4 Frame）/ libzstd，缺库时该后端抛异常
│   │   ├── SubscriptionData.cs     #   FilterAPI.BuildSubscriptionData / Equals 语义
│   │   ├── BoundaryType.cs         #   时间戳查位点的边界语义（LOWER/UPPER，含宽松解析）
│   │   ├── TopicConfig.cs
│   │   └── ClientLog.cs            #   客户端日志：按大小轮转 + 线程名 + 毫秒 + 文件:行号
│   ├── Remoting/
│   │   ├── RemotingClient.cs       # Socket TCP：惰性建连、每连接读线程、分帧、opaque→future 分发
│   │   ├── Exception.cs
│   │   └── Protocol/
│   │       ├── Json.cs             # 容错 JSON：容忍 fastjson2 非法输出（对象键内联/裸数字键/NaN/尾逗号）
│   │       ├── Serialize.cs        # RemotingSerializable(JSON) + RocketMQSerializable(二进制)
│   │       ├── Codes.cs            # RequestCode/ResponseCode/LanguageCode
│   │       ├── Headers.cs          # 26 个 CommandCustomHeader + V1<->V2
│   │       ├── RemotingCommand.cs  # JSON/ROCKETMQ 双序列化、header V1/V2
│   │       ├── Route.cs  Heartbeat.cs  Subscription.cs
│   │       └── Body.cs  AdminBody.cs   # 管理端响应 DTO
│   ├── Client/
│   │   ├── MqClient.cs             # MQClientInstance：路由发现 + TBW102 回退裁剪、心跳、offset 请求
│   │   ├── Producer.cs             # DefaultMQProducer：同步/定点/选择器/异步/单向/批量/事务
│   │   ├── Consumer.cs             # DefaultMQPushConsumer：拉取循环、并发/顺序监听、sendMessageBack
│   │   ├── LitePullConsumer.cs  PullConsumer.cs
│   │   ├── Admin.cs                # DefaultMQAdminExt 全套
│   │   ├── Result.cs               # SendResult/PullResult/监听器接口/队列选择器
│   │   ├── Hook.cs                 # Send/Consume/EndTransaction + CheckForbidden/FilterMessage 钩子与上下文
│   │   ├── Trace.cs                # 消息轨迹模型 + 文本编解码
│   │   ├── TraceHook.cs            # 三类轨迹钩子（发送 / 消费 / 结束事务）
│   │   └── TraceDispatcher.cs      # AsyncTraceDispatcher：异步队列 + 分组 + 128K 切块 + 定时 flush
├── tests/RocketMQ.Client.Tests/    # xunit（编解码/路由/日志/传输/轨迹/异步/流控/位点…… 30+ 个套件）
└── examples/RocketMQ.Examples/     # 真机联调工具（见上）
```

## 客户端日志

行格式 `2026-09-14 19:40:06.300 INFO  [pid] [线程名] [文件:行号] - msg`，
按大小 FixedWindow 轮转（默认 64MB × maxIndex 10），同时写 stderr 与
`$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log`（与 C++ 端同名——各端口若共用
这台机器的 `~/logs`，注意落到同一文件会互相插行）。

环境变量与 C++ 端一致：`ROCKETMQ_CPP_LOG_LEVEL` / `ROCKETMQ_CPP_LOG_FILE` /
`ROCKETMQ_CPP_LOG_FILE_MAX_SIZE` / `ROCKETMQ_CPP_LOG_FILE_MAX_INDEX`；
程序内可用 `ClientLog.SetLogLevel/SetLogFile/SetLogFileLimits/SetThreadName`。

## TLS

`TlsEnable = true`（或 `ROCKETMQ_TLS_ENABLE=1`）后每条出连接换成 `SslStream`，握手在
`AuthenticateAsClient` 里完成、随后整个流都走 TLS。test-mode（默认开）信任 broker 自签
证书、不校验主机名，所以本机 5.5.1 集群**不用改 `useTLS`**：nameServer 9876 与 broker
10911 按首字节嗅探，明文与 TLS 同一端口都收。

线程契约是关键约束：`SslStream` 只支持**一个并发读 + 一个并发写**（运行时源码
`SslStream.IO.cs` 里 `_nestedRead` 与 `_nestedWrite` 是两把独立的 `Interlocked` 闸门，
同类重入才抛 `net_io_invalidnestedcall`，读写互不干涉）。这里每条连接恰好一个读线程 +
一把 `Connection.WriteLock` 串行化写侧，正落在这个契约内 —— 注释写在
`src/RocketMQ.Client/Remoting/RemotingClient.cs` 的 `WriteLock` 上，别把它当成可以随手删的锁，
也别给同一条连接再加第二个读者。

`dotnet $PROG tls <namesrv> <topic> <group>` 的传输层压测（本机实测）：S0a 每轮新建一条
TLS 连接只打一个请求、30 轮 **0 丢**（最慢一轮 110ms，含握手）；S0b 单条 TLS 连接上
16 线程并发 320 笔 **0 失败**、响应 opaque 逐笔对上、总耗时 38ms。之后 TLS 生产者 +
TLS push 消费者 3 发 3 收，生产侧注入的 `traceparent` 在消费侧提取到且合法。

## 单元测试覆盖

`dotnet test tests/RocketMQ.Client.Tests` → **740 passed / 0 failed**，零 warning。
主要套件（每个都是行为断言，不是快照测试）：

- **协议与编解码**：`CodecTests` / `RouteHeartbeatTests` / `AdminBodyTests` / `LoggingTests` /
  `TraceTests`（轨迹文本编解码 + 坏记录隔离）/ `InteropTests`
- **传输与故障**：`TransportTests` / `FailFastTests`（真 socket：对端断开毫秒级判死、回调恰好一次、
  按连接对象身份认领在途请求、Shutdown 排空）
- **发送**：`SendRetryTests`（进程内 mock 集群抓真报文：重试分类、发送头 c/d/n、定点 topic 守卫）、
  `ProducerAsyncTests`（异步链路：预算共享、换 broker 重试、异步自建头、回调线程分岔）、
  `BackPressureTests`（公平信号量：队首语义、扩容叫醒、两条丢唤醒守卫）、`RecallMessageTests`、
  `ValidatorsTests`（名字校验文案与码值）、`AclTests`（签名 + 钩子组合顺序 + 反证）
- **消费**：`FlowControlTests`（五阈值判据与文案）/ `ConsumerCheckConfigTests`（启动期 12 条区间
  两端各测一次）/ `ConsumeThreadPoolTests`（并发度 == core、默认 20/20、运行时调小有效）/
  `PullExpiredTests`（120s 停摆判据与收尾）/ `OrderlyReconsumeTests`（顺序重投三支 + 显式
  COMMIT/ROLLBACK）/ `OffsetIllegalRecoverTests` / `ResetOffsetTests` / `PopConsumerTests` /
  `CleanExpiredMsgTests` / `SubscribeAfterStartTests` / `PullConsumerHeartbeatTests`
- **lite-pull 与位点**：`InitialOffsetTests`（真 socket 假集群：FIRST_OFFSET 起点是字面量 0，
  不发 `GET_MIN_OFFSET`）/ `LitePullCursorTests`（游标跟随 `nextBeginOffset`、在途 seek 刹车、
  请求码 361 抓真报文）
- **寻址与身份**：`ConsistentHashTests` / `AllocateStrategyTests`（六策略口径）/
  `ClientIdTests`（clientId 口径与实例名）/ `SearchOffsetBoundaryTests`（boundaryType 大写枚举名
  入网 + 抓真报文）/ `ScheduledIntervalsTests`（initialDelay 首跳 + 固定速率锚定）
- **主从路由**：`PublishRouteMasterTests`（发布地址只认 master 的地址侧与订阅侧三支）

## 几个必须知道的实现约定

- **事务消息是完整两阶段**：半消息（TRAN_MSG/PGROUP + sysFlag `TRANSACTION_PREPARED`）→
  本地事务 → `END_TRANSACTION(37, oneway)` → broker 回查 `CHECK_TRANSACTION_STATE(39)` 时回调
  `CheckLocalTransaction` 并回发 END_TRANSACTION。生产者会周期性向 broker 发心跳（含
  ProducerData）——**broker 的事务回查依赖它**，别把生产者心跳当可有可无的装饰砍掉。
- **异步发送有一根真正的发送池**：调用方线程过背压闸 → `AsyncSenderExecutor_N`（core==max==CPU
  核数、队列有界 50000）跑准备段（校验、压缩、选队列、建请求、Forbidden 钩子、Send 钩子 before，
  请求只建一次）→ 传输层 `InvokeAsync` → 失败走重试链（上限 `retryTimesWhenSendAsyncFailed`、
  每轮换 broker 并换新 opaque、broker 明确回了错就不换机器）→ `NettyClientPublicExecutor_N` 上跑
  Send 钩子 after + 归还许可 + 用户回调（恰好一次）。请求交给传输层**之前**的失败（闸门、排队超
  预算、校验、路由缺失）在当下这根线程就地回调。
- **`Shutdown()` 会排空发送池**：先拒新请求，再等两根池排空，保证交进来的每一笔都跑完准备段
  并把报文交给传输层。⚠ 保证**止于**「交给传输层」：紧接着就关客户端，响应没回来的那几笔
  没有终态回调 —— 要每笔都有回调，调用方得自己等完再关。
- **背压闸是手写公平信号量**：`SendAsync` 在**调用方线程**上先按条数拿 1 格、再按**压缩前**
  body 字节数拿 N 格，共享同一份 `timeout` 预算，拿不到就回调
  `send message tryAcquire semaphoreAsyncNum/Size timeout`，许可在把结果交给用户**之前**
  按「先 size 后 num」归还。改容量是**原地**平移绝对值并顺带叫醒等待者（等待者不会被搁死）；
  内建 `AsyncSenderExecutor` 队满时，开背压就地跑完这一笔（好让已扣的许可还回来）、
  关背压抛 `MQClientException("executor rejected")`。
- **批量发送的请求码是 320**：先 `IsReplyMessage` ⇒ 325，再 `msg.IsBatch` ⇒ 320，
  否则 `SendMessageV2(310)`。请求码与 V2 头的 `m`(batch) 是两件事，必须成对取证。
- **发送头 c/d/n 三字段跟着路由/配置走**：`c`=CreateTopicKey（默认 `TBW102`）、
  `d`=DefaultTopicQueueNums（默认 4，broker 按 `min(d, 模板队列数)` 自动建 topic）、
  `n`=该请求落到的 broker 名；手工指定空 brokerName 时 `n` 整条键不上线。
- **unitMode / stream 是上线字段，不是本地摆设**：`unitMode=true` 的发送让自动建出的 topic 带
  UNIT 位；消费者心跳的 `ConsumerData.unitMode` 让 `%RETRY%group` 带 UNIT_SUB 位；`unitName`
  参与 clientId 与动态取址。ExtFields 里的 `ReqT` 值是 `"0"`，clientId 尾巴上才是枚举名
  `@STREAM`。传输层只有**一槽**请求钩子，顺序靠 `RequestHooks.Compose(enableStream, userHook)`
  还原——stream 必须注在 ACL **之前**（`ReqT` 要落在签名内容里），且钩子必须在实例
  `Start()` **之前**注册。
- **clientId 口径**：`<本机 IP>@<instanceName>[@<unitName>][@STREAM]`；instanceName 还是默认值
  `DEFAULT` 时由 `Start()` 就地换成 `<pid>#<nanoTime>` —— 生产者与 admin 无条件，三个消费者
  只在 `CLUSTERING` 下（广播消费者保持 `DEFAULT`）。
- **定时任务是 initialDelay 首跳 + 固定速率锚定**：逐跳对着同一时间轴算、误差不累积，
  落后于计划时立刻补跑。位点落盘首跳在 10s（不是立刻、也不是 initialDelay+一个周期）。
  ⚠ 不要用「按 100ms 切片睡满 N 秒」的实现：macOS 上 `ManualResetEventSlim.Wait(100ms)` 实测
  会多给一个 tick，全部周期被拉长 ~30%；统一走 `Schedules.WaitUntil(...)` 一次睡到绝对计划时刻，
  整段又能被 `Shutdown` 立刻唤醒。
- **容错 JSON**：管理端应答解析容忍 fastjson2 非法输出（对象键内联 / 裸数字键 / NaN / 尾逗号），
  键名错一个字符会静默解析成空容器 —— 管理端 DTO 的字段名要与线上报文逐字一致。
- **消息轨迹解码比上游更健壮**：无 keys 消息的 SubBefore 缺段按空串取（上游会数组越界），
  且**单条坏记录**只跳过自己、不毁掉整条轨迹消息的解码。
- **ClientLog**：备份文件不压缩、同步写。
- **心跳指纹固定 0**：走 V1 完整注册路径，未实现依赖字段序的 V2 指纹。
- **TBW102 兜底只在发送路径启用**：`GetTopicPublishInfo(topic, isDefault: true)` 的第二跳
  与发送链路的 `tryToFindTopicPublishInfo` 语义一致，管理端查询不会拿到兜底路由。

## License

Apache-2.0。
