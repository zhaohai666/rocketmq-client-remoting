# RocketMQ .NET 客户端（remoting 协议）

以 Java 客户端（5.x）为语义基准、`cpp/` 与 `python/` 为实现参照的 **C# (.NET) 移植**，
与 Java broker 在真实 5.5.1 集群上联调验证通过。

## 构建 / 测试

```bash
cd dotnet
dotnet build                        # 全解决方案，0 warning（TreatWarningsAsErrors 已全局开启）
dotnet test tests/RocketMQ.Client.Tests   # xunit，46 项断言测试
```

要求 .NET 10 SDK。**零外部 NuGet 依赖**（仅 BCL）；zlib 走 `System.IO.Compression.ZLibStream`。

## 目录结构

```
dotnet/
├── Directory.Build.props           # 全局 Nullable + TreatWarningsAsErrors + InvariantGlobalization 提示
├── src/RocketMQ.Client/
│   ├── Common/                     # 公共层
│   │   ├── ByteBuffer.cs           #   ByteWriter/ByteReader/JavaHash（Java String.hashCode 语义）
│   │   ├── UtilAll.cs              #   时间/IP/CRC32/hex 等（全部显式 InvariantCulture）
│   │   ├── MixAll.cs  MessageConst.cs  SysFlag.cs（MessageSysFlag/PullSysFlag/PermName）
│   │   ├── Message.cs              #   Message/MessageExt/MessageBatch（引用语义，发送前由 Producer 克隆）
│   │   ├── MessageDecoder.cs       #   17 段存储格式 + 6 段批量格式编解码
│   │   ├── Compression.cs          #   zlib；类型位 0/3 = ZLIB；未支持类型必须抛异常（不透传）
│   │   ├── SubscriptionData.cs     #   FilterAPI.BuildSubscriptionData / Equals 语义对齐 Java
│   │   ├── TopicConfig.cs
│   │   └── ClientLog.cs            #   对齐 Java logback：按大小轮转 + 线程名 + 毫秒 + 文件:行号
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
│   │       ├── Body.cs  AdminBody.cs   # 管理端响应 DTO
│   ├── Client/
│   │   ├── MqClient.cs             # MQClientInstance：路由发现 + TBW102 回退裁剪、心跳、offset 请求
│   │   ├── Producer.cs             # DefaultMQProducer：同步/定点/选择器/异步/单向/批量/事务简化
│   │   ├── Consumer.cs             # DefaultMQPushConsumer：拉取循环、并发/顺序监听、sendMessageBack
│   │   ├── Admin.cs                # DefaultMQAdminExt 全套（47 项真机检查全通过）
│   │   ├── Result.cs               # SendResult/PullResult/监听器接口/队列选择器
│   │   ├── Hook.cs                 # SendMessage/ConsumeMessage/EndTransaction 钩子接口与上下文
│   │   ├── Trace.cs                # 消息轨迹模型 + 文本编解码（与 Java 官方实现逐字节对拍）
│   │   ├── TraceHook.cs            # 三类轨迹钩子（发送 / 消费 / 结束事务）
│   │   ├── TraceDispatcher.cs      # AsyncTraceDispatcher：异步队列 + 分组 + 128K 切块 + 定时 flush
│   │   └── ClientException.cs
├── tests/RocketMQ.Client.Tests/    # xunit（Codec/RouteHeartbeat/Logging/AdminBody/Transport/Trace…）
└── examples/RocketMQ.Examples/     # 真机联调工具（见下）
```

## 日志

`Common/ClientLog.cs` 对齐 Java `rmq.client.logback.xml`：
行格式 `2026-09-14 19:40:06.300 INFO  [pid] [线程名] [文件:行号] - msg`，
按大小 FixedWindow 轮转（默认 64MB × maxIndex 10），同时写 stderr 与
`$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log`（沿用 C++ 文件名，避免与 Java 撞车）。

环境变量与 C++ 完全一致：`ROCKETMQ_CPP_LOG_LEVEL` / `ROCKETMQ_CPP_LOG_FILE` /
`ROCKETMQ_CPP_LOG_FILE_MAX_SIZE` / `ROCKETMQ_CPP_LOG_FILE_MAX_INDEX`；
程序内可用 `ClientLog.SetLogLevel/SetLogFile/SetLogFileLimits/SetThreadName`。

## 真机联调工具

需要本地 RocketMQ 集群（namesrv 9876 + broker 10911，`autoCreateTopicEnable=true`）。
参考启动方式见 `cpp/README.md` 的集群脚本章节；.NET 工具直接对接：

```bash
PROG=examples/RocketMQ.Examples/bin/Debug/net10.0/rmq.dll
dotnet $PROG selfcheck                    # 本地自检（无需集群）
dotnet $PROG message-types 127.0.0.1:9876  # 7 类消息能力（12 项检查）
dotnet $PROG admin-live 127.0.0.1:9876     # Admin 全链路（47 PASS / 0 FAIL / 1 SKIP）
dotnet $PROG compression-live selftest 127.0.0.1:9876   # 压缩真实性（真机发送→消费→解压→CRC）
dotnet $PROG compression-live send 127.0.0.1:9876 <topic> <group> <size>
dotnet $PROG compression-live recv 127.0.0.1:9876 <topic> <group> <size>
dotnet $PROG interop --emit               # 打印规范帧 hex（JSON/ROCKETMQ 双序列化）
dotnet $PROG interop --decode <hex>       # 解码外部帧（供 Python/C++ -> .NET 字节级互通验证）
dotnet $PROG trace 127.0.0.1:9876         # 消息轨迹全链路（17 PASS / 0 FAIL，需 broker traceTopicEnable=true）
```

其它子命令：`redelivery` / `acl` / `pull` / `rr` / `latency` / `pop` / `popc`（POP 消费循环）。

## 实测结果（真实 5.5.1 集群）

| 工具 | 结果 |
|---|---|
| selfcheck | PASS=3 FAIL=0 |
| message-types | 12 PASS / 0 FAIL |
| admin-live | 47 PASS / 0 FAIL / 1 SKIP |
| compression-live selftest | ALL PASS（storeSize 8192→384，21:1；CRC 一致；flag 清除） |
| interop | Python ↔ .NET 双向解码逐字段一致（JSON 与 ROCKETMQ 双序列化） |
| trace | 17 PASS / 0 FAIL（消息轨迹全链路：SendResult 字段 → Pub → SubBefore/SubAfter 配对 → 防递归 → 无 keys 容错） |

单测：`dotnet test tests/RocketMQ.Client.Tests` → **178 passed / 0 failed**，零 warning
（`Directory.Build.props` 开了 `TreatWarningsAsErrors`）。

## 与 Java 的已知差异

- 事务消息已对齐 Java 的**两阶段**：半消息（TRAN_MSG/PGROUP + sysFlag TRANSACTION_PREPARED）→
  本地事务 → END_TRANSACTION(37, oneway) → broker 回查 CHECK_TRANSACTION_STATE(39) 时回调
  `CheckLocalTransaction` 并回发 END_TRANSACTION(FromTransactionCheck=true)。
  真机验证 COMMIT / ROLLBACK / UNKNOW+回查 三场景全通过（2026-09-15）。
  生产者会周期性向 broker 发心跳（含 ProducerData）——**broker 的事务回查依赖它**。
- `ClientLog` 备份文件不压缩（Java 会 gzip）；同步写（Java 走 AsyncAppender）。
- 心跳指纹固定 0（走 broker V1 完整注册路径），未实现依赖 fastjson2 字段序的 V2 指纹。
- 消息轨迹的解码器比 Java **更健壮**：Java `TraceDataEncoder` 对无 keys 消息的
  `SubBefore` 会 `line[7]` 越界抛 `ArrayIndexOutOfBoundsException`（上游真实缺陷，5.5.1 复现），
  我们缺段按空串取；同时把**单条记录**的解码包在 try/catch 里 —— Java 是一条坏记录
  毁掉整条轨迹消息的解码，我们只跳过坏记录。
- `GetTopicPublishInfo(topic, isDefault: true)` 的第二跳（TBW102 兜底）只在发送路径启用，
  与 Java `tryToFindTopicPublishInfo` 一致。
