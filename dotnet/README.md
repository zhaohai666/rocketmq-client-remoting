# RocketMQ .NET 客户端（remoting 协议）

以 Java 客户端（5.x）为语义基准、`cpp/` 与 `python/` 为实现参照的 **C# (.NET) 移植**，
与 Java broker 在真实 5.5.1 集群上联调验证通过。

## 构建 / 测试

```bash
cd dotnet
dotnet build                        # 全解决方案，0 warning（TreatWarningsAsErrors 已全局开启）
dotnet test tests/RocketMQ.Client.Tests   # xunit，450 项测试
```

要求 .NET 10 SDK。**零外部 NuGet 依赖**（仅 BCL）；zlib 走 `System.IO.Compression.ZLibStream`，
LZ4 / ZSTD 走 P/Invoke 调**系统库**（`/usr/local/lib/liblz4.dylib`、`libzstd.dylib`，
Linux 上是 `liblz4.so.1` / `libzstd.so.1`）—— 不引第三方包，装不到时该后端抛异常而不是
静默透传压缩字节。

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
│   │   ├── Compression.cs          #   zlib / LZ4 / ZSTD 三后端；类型位 0/3 = ZLIB；未支持类型必须抛异常（不透传）
│   │   ├── NativeCompression.cs    #   P/Invoke 到系统 liblz4（LZ4 Frame）/ libzstd，缺库时该后端抛异常
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
│   │   ├── Hook.cs                 # Send/Consume/EndTransaction + CheckForbidden/FilterMessage 钩子与上下文
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
dotnet $PROG message-types 127.0.0.1:9876  # 8 类消息能力（19 项检查）
dotnet $PROG admin-live 127.0.0.1:9876     # Admin 全链路（47 PASS / 0 FAIL / 1 SKIP）
dotnet $PROG compression-live selftest 127.0.0.1:9876   # 压缩真实性（真机发送→消费→解压→CRC）
dotnet $PROG compression-live send 127.0.0.1:9876 <topic> <group> <size> [codec]
dotnet $PROG compression-live recv 127.0.0.1:9876 <topic> <group> <size>
dotnet $PROG interop --emit               # 打印规范帧 hex（JSON/ROCKETMQ 双序列化）
dotnet $PROG interop --decode <hex>       # 解码外部帧（供 Python/C++ -> .NET 字节级互通验证）
dotnet $PROG trace 127.0.0.1:9876         # 消息轨迹全链路（17 PASS / 0 FAIL，需 broker traceTopicEnable=true）
dotnet $PROG hook 127.0.0.1:9876          # CheckForbidden/FilterMessage 钩子（13 PASS / 0 FAIL）
dotnet $PROG validators-live 127.0.0.1:9876  # 名字校验（39 PASS / 0 FAIL）
dotnet $PROG recall 127.0.0.1:9876        # 定时消息撤回（16 PASS / 0 FAIL，脚本负责开关并还原 recallMessageEnable）
dotnet $PROG unit-config 127.0.0.1:9876   # unitName/unitMode/stream（20 PASS / 0 FAIL）
```

其它子命令：`redelivery` / `acl` / `pull` / `rr` / `latency` / `pop` / `popc`（POP 消费循环）。

单测里的 `ValidatorsTests`（45 项）锁死名字校验的文案、判定顺序与码值口径：topic/group 的
blank→长度(127/120)→字符表三步都走纯客户端错误码（Java 是 -1，本工程沿用默认 1），
只有 `CheckMessage` 的 body 档位与 `INNER_MULTI_DISPATCH` 分隔符带 `MessageIllegal`(13)。

## 实测结果（真实 5.5.1 集群）

| 工具 | 结果 |
|---|---|
| selfcheck | PASS=3 FAIL=0 |
| message-types | 19 PASS / 0 FAIL（含批量：`SendBatch` 走 `SEND_BATCH_MESSAGE(320)`，真 broker 投成 3 条独立消息、offset 连续 0,1,2） |
| admin-live | 47 PASS / 0 FAIL / 1 SKIP |
| compression-live selftest | 10 PASS / 0 FAIL（zlib 真机往返 storeSize 8192→~360、CRC 一致、flag 清除、阈值与编解码闭环 + **后端 lz4 / zstd 真机往返**） |
| compression-live send/recv | 作为 `../scripts/compression_matrix.sh` 的一端参与四语言矩阵（zlib 13/13、lz4 13/13、zstd 7/7，全 PASS） |
| interop | Python ↔ .NET 双向解码逐字段一致（JSON 与 ROCKETMQ 双序列化） |
| latency | 18 PASS / 0 FAIL（S1-S5 故障规避链 + S6 发送重试内核：默认可重试 8 码、上限/换 broker 开关不误伤正常发送、无路由快速失败定性 10005） |
| trace | 17 PASS / 0 FAIL（消息轨迹全链路：SendResult 字段 → Pub → SubBefore/SubAfter 配对 → 防递归 → 无 keys 容错） |
| hook | 13 PASS / 0 FAIL（CheckForbiddenHook 放行/拦截/单向/不落 broker + FilterMessageHook 拉取与 POP 两条路径 + 二次 tag 过滤 + 钩子异常吞掉） |
| lite-pull | 33 PASS / 0 FAIL（`DefaultLitePullConsumer` 真机全链路：S1 rebalance 拿 4 队列 → S2 subscribe+poll 收全 12 条 → S3 commit 位点 >0 → S4 assign+seek 重收 → S5 订阅级 tag 只收 6 条 → S6a `ConsumeFromTimestamp` 收全、S6b `OffsetForTimestamp` 双向（30 分钟前→Σ=0，10 分钟后→Σ=12）→ S7a 默认策略 `AVG` + 策略为 null 时 `Start()` 报 Java 同款文案、S7b 换 `AVG_BY_CIRCLE` 同组两实例无交集/并集覆盖 4 队列/步长 2 交叉、S7c 两半 `CONFIG` 各自只收配置队列且合起来恰好 12 条互不重叠、S7d `CONSISTENT_HASH` 用**真实 clientId** 建环且线上分配收敛到「真实 mqAll/cidAll 离线跑同一策略」的预测（合起来收全 12 条）、S7e `MACHINE_ROOM_NEARBY-CONSISTENT_HASH` 单机房下原样透传内层策略 + resolver 被逐个队列和两个真实 clientId 问过、S7f `MACHINE_ROOM` 白名单不匹配真实 `broker-a` → 安静饿死（分不到队列、poll 不到消息，同组 AVG 对照组仍只拿自己半边）） |
| validators-live | 39 PASS / 0 FAIL（名字校验四语言对拍：S1 发送路径 13 项本地快拒（<50ms、不碰网络）、S2 批量逐条校验 + 同质性、S3 生产者 `Start()` 三道组名门 + 120 等长边界、S4 正腿 push/lite 各收 3 条、S5 对照腿（合法但不存在的 topic 真往返 45ms vs 本地 0.66ms）、S6 pull/lite 组名门 + 查队列与位点、S7 `CreateTopic` 挡空白/非法/系统 topic） |
| recall | 16 PASS / 0 FAIL（定时消息撤回 `recallMessage`(370)，与 Python/C++/Rust 同场景：R0 读到并临时打开 broker 的 `recallMessageEnable` → R1 只有带 `TIMER_DELAY_SEC` 的消息回 `recallHandle` → R2 broker 给的句柄能被解码、`topic`/`brokerName`/`uniqKey` 与发送结果逐字段一致 → R4/R5 `%RETRY%`/`%DLQ%`/非法句柄都在打网络之前秒回 → R3 撤回返回被撤回消息的 uniqKey → **R6 语义**：对照定时消息按时投递、被撤回那条整个窗口都不出现 → R7 无条件还原开关） |
| unit-config | 20 PASS / 0 FAIL（unitName/unitMode/stream 四语言同场景：U1 `unitName` 拼进 clientId 且照常发送 → U2 stream 消费者 `@unitA@STREAM` 收尾，**broker 的 `examineConsumerConnectionInfo` 记录的 clientId 也带同一后缀**（唯一能证明「上线的就是拼好的那个」的观测点）→ U3 `unitMode=true` 自动建出的 topic `topicSysFlag` 带 UNIT 位、对照组不带 → U4 心跳 `ConsumerData.unitMode` 让 `%RETRY%` 带 UNIT_SUB 位 → U5 lite 消费者默认带 `@STREAM`、显式开 stream 的生产者同样，3 发 3 收） |

单测：`dotnet test tests/RocketMQ.Client.Tests` → **461 passed / 0 failed**，零 warning
（`Directory.Build.props` 开了 `TreatWarningsAsErrors`）。其中 `SendRetryTests`（13 项）用
**进程内 mock 集群**（真 socket + 脚本化响应码）锁死 `sendDefaultImpl` 的重试分类语义 ——
这些分支真集群给不了： broker 不会稳定回 SYSTEM_BUSY，也不会刚好"路由里的地址连不上"。
同一种"抓 socket"的能力也被用来验请求钩子（`RequestHooksReachTheWire`）：只注册 ACL 时线上
有 `AccessKey`/`Signature` 而没有 `ReqT`；ACL + stream 时 `ReqT="0"` 在场，且把抓下来的报文按
broker 的口径复算 HMAC-SHA1 **能对上签名**（等价于「`ReqT` 落在被签的那段内容里」，顺序写反
就复算不上）；lite 消费者的路由与心跳默认全部带标，`EnableStreamRequestType=false` 后一笔都不
带、但请求照发（排除"根本没打出去"的假绿）。同一套抓取也被用来验发送请求码
（`SendRequestCodeFollowsJava`）：普通发 310、批量发 320、`MSG_TYPE=reply` 发 325，并成对
断言 V2 头的 `m`(batch) —— 抓的是 V2 系列而不是 V1(10)，本端口与 Java 一样默认
`sendSmartMsg=true`。
`ConsistentHashTests` + `AllocateStrategyTests` 则把六个队列分配策略与 Java 单测逐条对拍
（哈希环表、`String.Split('@')` 与 Java `split("@")` 的裁尾空段差异、NEARBY 的同机房优先与
resolver 空机房抛错语义），口径与 C++ `allocate_strategy` / `consistent_hash` 用例一致。
`RecallMessageTests`（17 项）锁定时消息撤回：句柄编解码对 Java `buildHandle` 的真值向量、
`RecallMessageRequestHeader` 的 **`bname`** 键名守卫（写成 `brokerName` 会被 broker 静默丢掉）、
`SendMessageResponseHeader.recallHandle` 往返，以及 producer 的本地校验顺序——把名字服务器指向
必然拒绝的端口，非法 topic / 非法句柄必须**亚毫秒**返回 Java 文案，路由拿不到时预热带异常照抛
（`DefaultMQProducerImpl:1586`）。
`ClientIdTests`（18 项）锁 clientId 口径：`BuildMqClientId` 的
`ip@instanceName[@unitName][@STREAM]`（含 unitName 与 stream 的先后、空白 unitName 视同没有）、
`ChangeInstanceNameToPID` 只改默认名且幂等、四类 `Start()` 盖出的 `<ip>@<pid>#<nanoTime>`、
同进程两个生产者不撞号、广播消费者保持 `DEFAULT`、五个门面的 stream 默认值
（producer/push/admin 关，pull/lite 开 —— Java 在 `DefaultMQPullConsumer:113/126` 和
`DefaultLitePullConsumer:213/228` 的构造函数里置真）。
`AclTests` 里新增的 4 项锁住请求钩子的**组合顺序**：`RequestHooks.Compose` 的形状
（stream 在前、用户钩子在后）、`ReqT="0"` 落在 ACL 签名**之内**（用签名后的 ExtFields 复算
能对上）、反证（顺序写反时同一断言必须红）、`ChainedRpcHook` 逐个转发 `doAfterResponse`。

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
- clientId 口径按 Java：`ClientIds.Build(instanceName[, unitName][, enableStreamRequestType])`
  = `<本机 IP>@<instanceName>[@<unitName>][@STREAM]`（`ClientConfig#buildMQClientId`），
  instanceName 还是默认值 `DEFAULT` 时由 `Start()` 调 `ClientIds.ChangeInstanceNameToPID`
  **就地**换成 `<pid>#<nanoTime>` —— 生产者与 admin 无条件，三个消费者只在 `CLUSTERING`
  下（广播消费者保持 `DEFAULT`，与 Java 一致；本端口每个门面各建私有 `MQClientInstance`，
  没有 Java 的 `MQClientManager` 工厂表）。旧口径 `instanceName@时间戳@pid@seq` 已废弃。
  本机 IP 用 UDP sockname 探测（Java 枚举网卡）。回归：
  `tests/RocketMQ.Client.Tests/ClientIdTests.cs`。
- **unitMode / stream 是上线字段，不是本地摆设。** 三个开关（`UnitName` / `UnitMode` /
  `EnableStreamRequestType`）五个门面都有，默认值与 Java 一致（producer/push/admin 不开
  stream，pull/lite 开）。落到线上的三处：`unitMode=true` 的发送让自动建出的 topic 带
  `TopicSysFlag.UNIT`（`AbstractSendMessageProcessor:487-497`）；消费者心跳的
  `ConsumerData.unitMode` 让 `%RETRY%group` 带 `UNIT_SUB`（`MQClientInstance:1039` →
  `ClientManageProcessor:113-118`）；`unitName` 参与 clientId 与 `TopAddressing` 的 ns 后缀。
  ⚠ **两处 `ReqT`/`@STREAM` 口径不同别混**：ExtFields 里的 `MixAll.ReqT` 写的是
  `RequestType.STREAM.getCode()` 的字符串形式 `"0"`（`StreamTypeRPCHook:28`），clientId 尾巴
  上才是枚举 name `@STREAM`。`RemotingClient` 只有**一槽**钩子（Java 是 `List<RPCHook>`），
  所以顺序靠 `RequestHooks.Compose(enableStream, userHook)` 还原——Java
  `MQClientAPIImpl:329-332` 把 stream 注在 ACL **之前**（"Inject stream rpc hook first to
  make reserve field signature"），`ReqT` 必须落在签名内容里，否则开鉴权的 broker 验签必挂。
  钩子还必须在 `MQClientInstance.Start()` **之前**注册（Java 在构造 `MQClientAPIImpl` 时就
  传进去了，晚一步首包就是裸的）。回归：`AclTests`（顺序 + 反证）、`SendRetryTests`
  （钩子真的写到 socket 上）、`rmq unit-config` 的 U1–U5（broker 侧 `topicSysFlag` 与
  `examineConsumerConnectionInfo` 的 clientId）。
- **批量发送的请求码是 320，不是 310。** `MqClient.BuildSendRequest` 按 Java
  `MQClientAPIImpl:550-563` 的三级判据走：先 `IsReplyMessage` ⇒ 325，再 `msg.IsBatch`
  ⇒ `SendBatchMessage(320)`，否则 `SendMessageV2(310)`。请求码与 V2 头的 `m`（batch）是
  两件事：broker 按 `m` 选 `sendBatchMessage` 还是单条写入（`SendMessageProcessor:117` 读
  `requestHeader.isBatch()`），码只影响服务端按码归类（proxy `AbstractRemotingActivity:69`
  与 auth `DefaultAuthorizationContextBuilder:230-240` 把 310/320 列在同一个 case 里），
  所以对齐 320 不是修 bug、而是请求码这一层也与 Java 一致 —— 两个字段必须成对取证。
  回归：`SendRetryTests.SendRequestCodeFollowsJava`（真 socket 上取 `Code` + `m`）、
  `rmq message-types` 第 8 项（真 broker 把批量投成 3 条独立消息、offset 连续）。
