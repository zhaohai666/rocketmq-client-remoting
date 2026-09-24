# RocketMQ .NET 客户端（remoting 协议）

以 Java 客户端（5.x）为语义基准、`cpp/` 与 `python/` 为实现参照的 **C# (.NET) 移植**，
与 Java broker 在真实 5.5.1 集群上联调验证通过。

## 构建 / 测试

```bash
cd dotnet
dotnet build                        # 全解决方案，0 warning（TreatWarningsAsErrors 已全局开启）
dotnet test tests/RocketMQ.Client.Tests   # xunit，587 项测试
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
│   │   ├── BoundaryType.cs         #   时间戳查位点的边界语义（LOWER/UPPER，含 getType 宽松解析）
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
│   │   ├── Admin.cs                # DefaultMQAdminExt 全套（admin-live 66 项真机检查全通过）
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
dotnet $PROG admin-live 127.0.0.1:9876     # Admin 全链路（66 PASS / 0 FAIL / 1 SKIP）
dotnet $PROG compression-live selftest 127.0.0.1:9876   # 压缩真实性（真机发送→消费→解压→CRC）
dotnet $PROG compression-live send 127.0.0.1:9876 <topic> <group> <size> [codec]
dotnet $PROG compression-live recv 127.0.0.1:9876 <topic> <group> <size>
dotnet $PROG interop --emit               # 打印规范帧 hex（JSON/ROCKETMQ 双序列化）
dotnet $PROG interop --decode <hex>       # 解码外部帧（供 Python/C++ -> .NET 字节级互通验证）
dotnet $PROG trace 127.0.0.1:9876         # 消息轨迹全链路（17 PASS / 0 FAIL，需 broker traceTopicEnable=true）
dotnet $PROG hook 127.0.0.1:9876          # CheckForbidden/FilterMessage 钩子（13 PASS / 0 FAIL）
dotnet $PROG backpressure 127.0.0.1:9876  # 异步发送背压公平信号量（25 PASS / 0 FAIL）
dotnet $PROG async-send 127.0.0.1:9876    # 异步发送内核（43 PASS / 0 FAIL / 1 SKIP）
dotnet $PROG fail-fast 127.0.0.1:9876     # broker 真死时在途请求立刻判死（18 PASS / 0 FAIL，会停一次 broker 再拉起）
dotnet $PROG validators-live 127.0.0.1:9876  # 名字校验（43 PASS / 0 FAIL）
dotnet $PROG recall 127.0.0.1:9876        # 定时消息撤回（16 PASS / 0 FAIL，脚本负责开关并还原 recallMessageEnable）
dotnet $PROG unit-config 127.0.0.1:9876   # unitName/unitMode/stream（20 PASS / 0 FAIL）
dotnet $PROG send-header 127.0.0.1:9876   # 发送头 c/d/n 三字段（14 PASS / 0 FAIL）
dotnet $PROG flow-control 127.0.0.1:9876  # 拉取前流控五个阈值 + 启动期数值闸门（29 PASS / 0 FAIL）
dotnet $PROG sql92 127.0.0.1:9876         # SQL92 过滤 + CHECK_CLIENT_CONFIG(46)（20 PASS / 0 FAIL，需 broker enablePropertyFilter=true）
dotnet $PROG scheduled-intervals 127.0.0.1:9876   # 周期任务的 initialDelay/固定速率（20 PASS / 0 FAIL，含位点落盘 10s 首跳）
dotnet $PROG tls 127.0.0.1:9876 <topic> <group>   # TLS 传输层压测 + TLS 全链路收发（见「TLS」）
```

其它子命令：`redelivery`（60 PASS / 0 FAIL，2026-09-24 实测；重投/死信/重启/顺序/广播/流控/rebalance/namespace/部分 ack/停摆自愈/顺序死信 十二段）/ `acl` / `pull` / `rr`（request-reply 全链路，22 PASS / 0 FAIL：325 应答链路 + 10006 超时码 + 10007 造应答失败码）/ `latency` / `pop` / `popc`（POP 消费循环，11 PASS / 0 FAIL，见下）。

`redelivery` 的 S9 是**死信终态**，也是「用尽」这条判据唯一能验的地方——客户端只把
`maxReconsumeTimes` 通过 `sendMessageBack` 的 header 递上去，真正决定第几次转死信的是 broker
（`AbstractSendMessageProcessor:183` 判 `reconsumeTimes >= maxReconsumeTimes || delayLevel < 0`，
`:193` 改写 topic，`:228` 存储时 `reconsumeTimes + 1`）。`maxReconsumeTimes=2` 实测只投 3 次、
延迟梯度 `0s/10s/40s`（Java 的 `delayLevel = 3 + reconsumeTimes` 档位算术），死信落在
`%DLQ%<group>`、`reconsumeTimes=3`、`RETRY_TOPIC` 保留业务 topic，且观察窗内没有第 4 次投递。
窗口给 150s 而不是 100s：整机并发时 broker 定时服务会拖档，100s 是假失败。

`redelivery` 的 S10 是**部分 ack（`AckIndex`）**：Java
`ConsumeMessageConcurrentlyService#processConsumeResult:207-254` 用 listener 写的 `ackIndex`
（默认 `Integer.MAX_VALUE` = 整批认可）把本批切成「已认可前缀提交位点 / 尾巴逐条
`sendMessageBack`」。一批 3 条只认可第 1 条后实测：尾巴 2 条经 `%RETRY%` 二次到达
（`reconsumeTimes>=1`、listener 看到业务 topic）、被认可那条整个窗口只投一次、3 条最终全部消费、
业务队列位点仍整批提交到 3；对照组完全不碰 `AckIndex` 一条都不回投。topic 只建 1 个队列，
并且**先把 3 条发上去再起消费者**（新组显式 `ConsumeFromFirstOffset`）——批次怎么切由拉取时机
决定，后起消费者时首批可能只有 1~2 条，前缀/后缀就不确定了。

`redelivery` 的 S11 是**拉取循环停摆自愈**（Java `ProcessQueue.PULL_MAX_IDLE_TIME` = **120000ms**，
判据 `pq.isPullExpired()` 打在 `RebalanceImpl.updateProcessQueueTableInRebalance:438-461`：队列仍归
本实例却停摆 ⇒ `setDropped(true)` + `removeUnnecessaryMessageQueue`（持久化位点）+ 同一趟的 add 分支
换一具新的 ProcessQueue 重建）。这条路径坏掉是**静默的**：客户端不报错、心跳照发、别的队列照常推进，
真机上只能从「某个组的某条队列位点永远不动」反推，所以停摆→恢复的闭环必须真机取证。三步：
H1 基线（3 条消费掉、位点到 3、`LastPullAt` 是循环自己盖的真时刻、进程内已消费位点 3）；
H2 把这一路登记成**一条已退出的线程**（对位 Java 的「循环被异常打穿」）⇒ `PullStalled` 即刻为真
（不等满 120s），叫醒 rebalance 后必须换成一条**新的活线程**才判恢复——注入的是死线程，旧循环不可能
自己把判据翻回 false，所以这一步无歧义；H3 线程活着但把盖章时刻**倒拨 125s** ⇒ 同样被撤并重建，
且 307 应答里能读到被倒拨的那个 `"lastPullTimestamp":<injected>`（证明运维看得见判据的现场证据）。
H2/H3 之后各发 3 条，位点走到 6/9，9 条各只投一次、`redelivered=0`、时钟恢复新鲜。
阈值 120s 与**严格大于**的边界（-120000ms 不算、-120001ms 才算）由离线单测
`PullExpiredTests`（12 项）锁死；那一项用注入时钟调 `PullStalledForTest`，因为等真 120s 的判据
分不清走的是哪一支。本端口的存活代理是**登记线程的 `Thread.IsAlive`**（对位 Java 每队列一具
ProcessQueue），POP 分支改读 `PopProcessQueue.LastPopTimestamp` 且撤走时 `SetDropped`。

`redelivery` 的 S12 是**顺序消费的死信终态**（Java `ConsumeMessageOrderlyService:236-362`，与并发侧
的 S9 是两条不同的代码路径）：SUSPEND 先过 `checkReconsumeTimes:322-339` —— 没用尽就本地
`ReconsumeTimes + 1` 并原地挂起，用尽则回投，**只有回投失败**才继续挂起（回投成功必须提交位点，
否则一条毒消息永久占住那条队列，真机上跟"消费者挂了"长得一模一样）。1 队列 topic +
`ConsumeMessageBatchMaxSize=1` + `SuspendCurrentQueueTimeMillis=500` + `MaxReconsumeTimes=2` 实测：
毒消息恰好投 3 次、`reconsumeTimes` 走 0/1/2 的阶梯（每一格都是客户端自己 +1），第 3 次交回 broker
后业务队列**立刻前进**（后一条被消费）、再等 15s 也没有第 4 次，挂起期间 listener 始终看到业务
topic。`%DLQ%<group>` 的路由到这一刻才建出来，死信那条 `reconsumeTimes=3`（broker 存储时 +1）、
`RETRY_TOPIC` 仍是业务 topic —— **顺序回投能落进死信而不是走延迟档位，本身就是 broker 此刻看到该组
的重平衡锁没过期**（`SendMessageProcessor#handleRetryAndDLQ:202-207`）的证据，也就是"拿着
`LOCK_BATCH_MQ` 把消息交给 broker"这条链真的接上了。S12b 反过来验 `-1` 那一支：顺序侧
`getMaxReconsumeTimes:313-320` 把 `-1` 读成**不设上限**（**不是**并发侧 `DefaultMQPushConsumerImpl:890`
的 16），实测投过 19 次、`reconsumeTimes` 已到 18 而 broker 连 `%DLQ%` 的 topic 都没建；写成 16 的话
第 17 次投递就该有死信。三条分支（+1、用尽才回投、回投失败才继续挂起）与回投那条消息的字段由离线
单测 `OrderlyReconsumeTests`（10 项）锁死 —— 离线未 `Start()` 的消费者拿不到内部生产者，回投必定
失败，所以离线锁的只有失败分支，"回投成功 ⇒ 位点前进、队列不堵"只能靠这一段。

`popc` 的 S5 是**POP 循环把拉取统计写进 307 状态表**（Java `DefaultMQPushConsumerImpl.popMessage`
的 `PopCallback.onSuccess:556-563`：`case FOUND:` 先 `IncPullRT`，这一格打在**空列表判定之前**，
`MsgFoundList` 非空才 `IncPullTPS`；`POLLING_NOT_FOUND` 两格都不动 —— 空手而归是长轮询的常态，
把挂起时间折进 RT 会毁掉它）。漏记是**静默**的：弹、ack、消费完全正常，只有运维看板上
`pullRT`/`pullTPS` 一片 0，而看板上"这个消费者没在拉取"和"这个消费者压根没起来"是两种完全不同的
处置。判据从 `DefaultMQAdminExt.ExamineConsumerRunningInfo`(307) 应答的 `StatusTable[topic]` 读，
拉取侧两格与消费侧 `consumeOKTPS` **各自断言**（只有后者有值正是漏记的形状），实测
`pullRT=2048.90`、`pullTPS=1.9998`、52 发 52 收（两格取值随真机节奏浮动，判据只要求非 0）。⚠ 快照每 10s 采样一次、窗口取 minute 差分，夹具
必须**持续有流量**并跨过两个采样点（这里 26s 内每 2s 发 4 条），否则 `pullTPS` 仍是 0 —— 那是夹具
不够长，不是判据错。三种 status 各自记哪几格由离线单测 `PopConsumerTests`（16 项）里的
`RecordPopPullStats_*` 三项锁死：`RecordPopPullStats` 之所以是 `public`（本解决方案没有
`InternalsVisibleTo`）就是为了让它能在进程内被单独喂 `PopResult` 断言，注释里写明了**勿用于业务代码**。

单测里的 `ValidatorsTests`（45 项）锁死名字校验的文案、判定顺序与码值口径：topic/group 的
blank→长度(127/120)→字符表三步都走纯客户端错误码（Java 是 -1，本工程沿用默认 1），
只有 `CheckMessage` 的 body 档位与 `INNER_MULTI_DISPATCH` 分隔符带 `MessageIllegal`(13)。

## 实测结果（真实 5.5.1 集群）

| 工具 | 结果 |
|---|---|
| selfcheck | PASS=3 FAIL=0 |
| message-types | 19 PASS / 0 FAIL（含批量：`SendBatch` 走 `SEND_BATCH_MESSAGE(320)`，真 broker 投成 3 条独立消息、offset 连续 0,1,2） |
| admin-live | 66 PASS / 0 FAIL / 1 SKIP（含 `SearchOffset` 的 `boundaryType`：1 队列 topic 发 3 条 ⇒ 远未来时间戳下 LOWER = maxOffset(3)、UPPER = maxOffset-1(2)，两数不同才证明字段真到了 broker；时间戳早于全部消息时两个边界都塌到 0。含 `ResetOffsetByQueueId`：25 + 带 queueId/offset 的 222，重置后首笔 pull 被 broker 短路成 `PULL_OFFSET_MOVED`、第二笔才取到历史消息；越界目标被拒时位点停在第 1 笔写入的非法值 ⇒ 两笔 RPC 非原子，与 Java 同构；`QueryTopicsByConsumer(group)` 按 `%RETRY%` 路由扇出合并 + `QueryTopicsByConsumerToBroker`） |
| compression-live selftest | 10 PASS / 0 FAIL（zlib 真机往返 storeSize 8192→~360、CRC 一致、flag 清除、阈值与编解码闭环 + **后端 lz4 / zstd 真机往返**） |
| compression-live send/recv | 作为 `../scripts/compression_matrix.sh` 的一端参与四语言矩阵（zlib 13/13、lz4 13/13、zstd 7/7，全 PASS） |
| interop | Python ↔ .NET 双向解码逐字段一致（JSON 与 ROCKETMQ 双序列化） |
| latency | 18 PASS / 0 FAIL（S1-S5 故障规避链 + S6 发送重试内核：默认可重试 8 码、上限/换 broker 开关不误伤正常发送、无路由快速失败定性 10005） |
| trace | 17 PASS / 0 FAIL（消息轨迹全链路：SendResult 字段 → Pub → SubBefore/SubAfter 配对 → 防递归 → 无 keys 容错） |
| hook | 13 PASS / 0 FAIL（CheckForbiddenHook 放行/拦截/单向/不落 broker + FilterMessageHook 拉取与 POP 两条路径 + 二次 tag 过滤 + 钩子异常吞掉） |
| backpressure | 25 PASS / 0 FAIL（异步发送背压 B1-B5，与 Python/C++/Rust 同场景：B1 默认容量 40 笔异步全 SEND_OK、broker 侧正好落 40 条、两个信号量满额归还 1024 / 104857600 → B2 条数闸夹到地板值 10 时在途占满后空闲为 0，超额的 2 笔在调用方线程上等满预算才回调（实测等了 151ms）、文案与 Java 逐字一致，且 broker 上一条没留（`landed=10`）→ B3 运行时扩容到 12 叫醒卡在闸上的发送方、全部归还后空闲 = 新容量 12、broker 总数 21 → B4 字节闸 1M 地板 + 600KB body：在途空闲字节 434176、第二笔回调 `semaphoreAsyncSize timeout`、被拒时条数许可已归还、broker 只落 1 条 → B5 关背压后 30 笔并发（含 300KB 大 body）全落地） |
| async-send | 43 PASS / 0 FAIL / 1 SKIP（异步发送内核 A1-A6：A1 before 钩子睡 400ms 时调用方 17ms 就返回，这一笔 SEND_OK 后**用 broker 回的 `offsetMsgId` 能 `ViewMessage` 读回原 body**、`queueOffset` 正好等于该队列 `maxOffset-1`、`MsgId` 是 32 位客户端 UNIQ_KEY 且与 `offsetMsgId` 不同；线程口径实测 `AsyncSenderExecutor_1` 跑准备段、`NettyClientPublicExecutor_1` 跑用户回调 → A2 并发 30 笔：一笔恰好一个终态、全 SEND_OK、broker 落 30 条、30 个 `(broker,queueId,queueOffset)` 槽位与 30 个 UNIQ_KEY 两两不重复 → A3 定点异步发送只让指定的那条队列多 1 条、其它队列一条没多 → A4 `CheckForbiddenHook` 看到 `CommunicationMode.Async`，拒绝时异常原样到回调且**连 topic 路由都没建出来**（`landed=-1`），换个标签照常落地、钩子被调 2 次 → A5 `SendBatchAsync`（对位 Java `send(Collection, SendCallback, timeout)`）一批 3 条：回调恰好一次且 SEND_OK、broker 侧 `landed=3`、应答的 `OffsetMsgId` 是**逐条回的 3 个 commitLog 偏移**，用它 `ViewMessage` 读回的那条**子消息**带着客户端生成的 32 位 `UNIQ_KEY`（逐条 ID 真编进了 body 的落地证据；缺了它发送侧照样 SEND_OK，只有真 broker 看得出来）、`MsgId` 是批量自身的 32 位客户端 ID；定点批量只让那条队列多 3 条；混 topic / 空批的本地校验在异步路径上照样跑且错误**进回调**；字节闸按**整批**扣（1 MiB 地板下 2×600 KiB 被拒、2×100 KiB 照常 SEND_OK、两份许可满额归还）→ A6 `Shutdown()` 排空：36 笔全部上线（`landed=36`），至多 35 笔拿到终态回调（响应没回来就关了客户端，与 Java 同一条）） |
| fail-fast | 18 PASS / 0 FAIL（2026-09-24 实测，最差一笔 2430ms；broker 真死掉时在途请求立刻判死，Java `NettyRemotingHandler#close` → `failFast(channel)` → `requestFail(opaque)`，与 Python `verify_fail_fast_live.py`、C++ `rmq_live_fail_fast`、Rust `live_fail_fast` 的 L1~L5 同场景。**这个子命令会停一次测试 broker、跑完再拉起来**，store 不删）：L1 真 broker 上 5 条同步发送 SEND_OK、各队列队尾位点**合计**覆盖这 5 条（发送跨队列轮转，看单条队列会假红）→ L2 手工构三条 `suspend=true` 的长轮询（客户端超时 30s、broker suspend 20s，绕开 pull consumer 的钳制），2s 后一条都没返回、`PendingRequestCount` 从 0 涨到 3，请求**确实在途** → L3 `mqshutdown broker`：三条全部返回、异常类型是 `RemotingSendRequestException`（文案 `connection closed before response`）、**一条都没被报成 `RemotingTimeoutException`**（异步发送的重试分类按异常类型分流，报成超时等于换一整套决策），最差一笔 2430ms ≪ 8s 阈值 ≪ 30s 超时，判死后在途表排空 → L4 判死只覆盖死掉那条连接所在的地址：同一个 `RemotingClient` 上的 namesrv 连接照常服务，`GET_ALL_TOPIC_LIST_FROM_NAMESERVER` 仍回 `code=0`。⚠ 真机只能证**按地址隔离**（一台 broker 一个地址一条连接），"同地址换连接时旧连接的收尾不误伤新连接"没有确定性的时间窗，由离线 `FailFastTests` 的 `AfterFailFast_SameAddress_KeepsWorking` 负责 → L5 broker 拉起后**同一个 producer 实例**重新建连照常发送（`attempts=1`，一次没重），已拿到 SEND_OK 的那 5 条一条都没少。收尾无论走到哪一步都会把 broker 拉回来（`Finish()`），脚本的 `start` 本身幂等 |
| lite-pull | 61 PASS / 0 FAIL（`DefaultLitePullConsumer` 真机全链路：S1 rebalance 拿 4 队列 → S2 subscribe+poll 收全 12 条 → S3 **没人调 `Commit`**、只继续 `Poll()` 过一整个自动提交周期（闸门只在 `Poll()` 开头查）后 `Committed()` 自己 >0 → S4 assign+seek 重收 → S5 订阅级 tag 只收 6 条 → S6a `ConsumeFromTimestamp` 收全、S6b `OffsetForTimestamp` 双向（30 分钟前→Σ=0，10 分钟后→Σ=12）→ S7a 默认策略 `AVG` + 策略为 null 时 `Start()` 报 Java 同款文案、S7b 换 `AVG_BY_CIRCLE` 同组两实例无交集/并集覆盖 4 队列/步长 2 交叉、S7c 两半 `CONFIG` 各自只收配置队列且合起来恰好 12 条互不重叠、S7d `CONSISTENT_HASH` 用**真实 clientId** 建环且线上分配收敛到「真实 mqAll/cidAll 离线跑同一策略」的预测（合起来收全 12 条）、S7e `MACHINE_ROOM_NEARBY-CONSISTENT_HASH` 单机房下原样透传内层策略 + resolver 被逐个队列和两个真实 clientId 问过、S7f `MACHINE_ROOM` 白名单不匹配真实 `broker-a` → 安静饿死（分不到队列、poll 不到消息，同组 AVG 对照组仍只拿自己半边）→ **S8 三张位点表在真机各自数出来**：1 条队列的 topic 灌 1200 条，拉取游标 1200 / 已消费游标 -1 / broker 位点 0 三个数互不相等（一次都没交付时提交拉取游标就是静默丢消息）、单次 `Poll` 交 1024 ⇒ 落到 broker 的正是 1024 而不是 1200、`Commit(map)` 改点位到 5 而两格游标都不动且退回的 176 条照旧交付（全程 1200 条不重不漏）、`persist:false` 时 `Committed()` 读到内存那格 777 而 broker 仍是 5、新实例起点取 broker 上的 5（这里顺带抓出并修掉一处顺序缺陷：`Start()` 原本在实例启动**之前**解析 assign 模式的起点，`RequireClient()` 抛异常被吞 ⇒ `Start()` 返回时拉取游标还停在 -1，现已挪到 Java `operateAfterRunning` 对应的位置）、`seek` 同时改两格游标、点名提交抹掉没点名的内存行（Java `persistAll` 的 remove unused mq）且清理不写 broker） |
| validators-live | 43 PASS / 0 FAIL（名字校验四语言对拍：S1 发送路径 13 项本地快拒（<50ms、不碰网络）、S2 批量逐条校验 + 同质性、S3 生产者 `Start()` 三道组名门 + 120 等长边界、S4 正腿 push/lite 各收 3 条、S5 对照腿（合法但不存在的 topic 真往返 45ms vs 本地 0.66ms）、S6 pull/lite 组名门 + 查队列与位点、S7 `CreateTopic` 挡空白/非法/系统 topic、**S8 寻址故障定性**（一个地址都没配 ⇒ 10004 NO_NAME_SERVER_EXCEPTION + Java 原文案「No name server address, please set it.」，0.54ms 本地判定不空转重试预算；对照组地址恢复后同一条 topic 立刻 `SendOk`，说明判的是寻址不是 topic）） |
| recall | 16 PASS / 0 FAIL（定时消息撤回 `recallMessage`(370)，与 Python/C++/Rust 同场景：R0 读到并临时打开 broker 的 `recallMessageEnable` → R1 只有带 `TIMER_DELAY_SEC` 的消息回 `recallHandle` → R2 broker 给的句柄能被解码、`topic`/`brokerName`/`uniqKey` 与发送结果逐字段一致 → R4/R5 `%RETRY%`/`%DLQ%`/非法句柄都在打网络之前秒回 → R3 撤回返回被撤回消息的 uniqKey → **R6 语义**：对照定时消息按时投递、被撤回那条整个窗口都不出现 → R7 无条件还原开关） |
| unit-config | 20 PASS / 0 FAIL（unitName/unitMode/stream 四语言同场景：U1 `unitName` 拼进 clientId 且照常发送 → U2 stream 消费者 `@unitA@STREAM` 收尾，**broker 的 `examineConsumerConnectionInfo` 记录的 clientId 也带同一后缀**（唯一能证明「上线的就是拼好的那个」的观测点）→ U3 `unitMode=true` 自动建出的 topic `topicSysFlag` 带 UNIT 位、对照组不带 → U4 心跳 `ConsumerData.unitMode` 让 `%RETRY%` 带 UNIT_SUB 位 → U5 lite 消费者默认带 `@STREAM`、显式开 stream 的生产者同样，3 发 3 收） |

| send-header | 14 PASS / 0 FAIL（发送头 `c`/`d`/`n` 三字段真机，与 Python `verify_send_header_live.py`、C++ `rmq_live_send_header`、Rust `live_send_header` 的 H0~H5 一一对应：H0 先量出 `TBW102` 的 read/write 队列数（本机 8/8）当算术基准 → H1 什么都不配、发到全新 topic，broker 按 `min(d=4, TBW102.writeQueueNums)` 建出 **4** 条队列（`TopicConfigManager.java:289`）→ H2 `DefaultTopicQueueNums=2` 真的让 broker 只建 **2** 条（修之前写死 4，这一条必然红）→ H3 `CreateTopicKey` 指向带 `PERM_INHERIT` 的 3 队列模板 topic 时，新 topic 继承**模板**的 **3** 条而不是 TBW102 的 8 条（`isInherited` + `min` 两道门）→ H4 补上三字段后五种入口（`Send` / 定点 `Send(msg, mq)` / `SendOneway` / `SendBatch` 320 / `SendAsync`）逐条落地、7 条一条不差 → H5 落点 broker 名与路由选中那台一致。⚠ `n` 在经典 broker 的发送链路里**没有读者**（5.5.1 源码 grep 过），它上线的存在由离线抓帧单测取证，这里不假装能观测到） |
| flow-control | 29 PASS / 0 FAIL（拉取前流控五个阈值真机闭环 S0~S5，与 Python `verify_flow_control_live.py`、C++ `rmq_live_flow_control`、Rust `live_flow_control` 逐条同构。离线 `FlowControlTests` 锁判据本身；真机锁离线锁不住的两件事：**闸门确实会命中**（单位错一位、阈值读错一个字段，离线拿预置缓冲照样绿）与**命中后一条不丢**（写成"命中就丢批/退出循环"在十几秒窗口里看不出来）。实测：S0 默认闸门 + 快消费 ⇒ `triggered=0`、12 条全到；S1 只留队列级字节闸门 ⇒ 命中 11 次、8 条 400KB 不丢不重；S2 只留跨度闸门（`MaxSpan=2`）⇒ 命中 2 次、14 条仍全部消费；S3 只留 topic 级条数闸门 ⇒ 单队列到不了阈值、必须跨队列累计（命中 112 次）且每条队列都消费到底；S4 复用 S1 的组与 topic ⇒ 位点从 broker 末尾续上、闸门不是命中一次就失效（仍命中 7 次）、6 条不重不丢；S5 启动期数值闸门（Java `checkConfig` 数值段 `:1099-1209`）⇒ 贴着区间端点的配置真能启动并收全 10 条（闸门写坏最常见的方式是"比 Java 还严"，把合法配置也拒了），5 条越界配置逐字按 Java 文案在本地被拒且 `IsStarted` 仍为 false，broker 侧用**裸** `GetConsumerListByGroup` 反查：被拒的组查不到（本机 broker 回 `code=1 no consumer for this group`，空列表与该异常都算查无此组，其它异常 FAIL），边界值那个组恰好查得到 1 个 clientId——写成"先注册再校验"就会留下一堆永不心跳的僵尸 clientId 把 rebalance 用的 `cidAll` 撑歪。⚠ 有了 S5，"关掉某道闸门"的写法必须是 Java 的**上界**（`Huge=65535` / `HugeSizeMiB=1024`）而不是 `0`：`0` 现在正是启动期会拒的配置。⚠ 命中**次数**随真机投递/消费节奏浮动（S3 两轮分别报 100 与 112），判据只要求 `triggered > 0`。⚠ 两条夹具坑：大消息必须**不可压缩**（否则 broker 落盘 `StoreSize` 只有几百字节，size 闸门"永不命中"其实是夹具问题）；S1/S4 的 topic 必须**只有 1 条队列**（8 条 400KB 摊到 4 条队列每条才 800KB，够不到队列级那道 1MiB）） |
| sql92 | 20 PASS / 0 FAIL（SQL92 过滤 + `CHECK_CLIENT_CONFIG`(46) 四语言同场景：S1 SQL92 订阅启动时正好一笔 46、body 的 `clientId`/`group`/`subscriptionData` 逐字段对得上，纯 TAG 订阅一笔都不发（Java `ExpressionType.isTagType` 短路）→ S2 消费者**先起来再发** 6 条，`color='red'` 只收那 3 条 red、blue 一条没漏进来（broker 真在按属性过滤，而不是拿不到编译过滤数据就放行全部），`'*'` 对照组收全 6 条 → S3 永不匹配的 `color='green'` 收 0 条 → S4 语法错的表达式让 `Start()` 秒回 broker 的 `SUBSCRIPTION_PARSE_FAILED(23)` 并就地回滚（换个合法表达式能重新 `Start()`）。协议形状与四条分支语义另有离线单测 10 项（`CheckClientConfigTests`，进程内 mock broker） |

| unreg-live | 10 PASS / 0 FAIL（生产者退出注销 `UNREGISTER_CLIENT`(35) 真机，与 Python `verify_producer_unregister_live.py`、C++ `rmq_live_producer_unregister`、Rust `live_producer` 的 P11 同一套场景）：U1 发送成功 → U2 心跳后 204 `GET_PRODUCER_CONNECTION_LIST` 能看到本 clientId（注册确实发生过，"消失"才有意义，组靠心跳上线所以要轮询等）→ U2b 对照组注册可见（204 这条判据本身有效）→ U3 `Shutdown()` 给每台已知 broker 各发一发 35、头是 `clientID`+`producerGroup` 且 **`consumerGroup` 整个字段不上线**（Java 传 null；broker `ClientManageProcessor:228/237` 判的是 `group != null`）、addr 确实是路由里那台、且排在业务发送之后（`last_send=4 first_unreg=6`）→ U5 紧接着查 204 这个组已经不在了 → U6 对照组仍在（排掉"broker 把所有连接都清了"这种假阳性）。⚠ 判据强度：.NET 里每个生产者各持一份 `MQClientInstance`、各一条连接，退出时连接也关掉，单看 U5 分不出是 35 还是断连的功劳，所以这里必须由 `IRpcHook` 抓帧（钩子跑在 `Encode()` **之前**，头此时还挂在 `CustomHeader` 上）直接证明线上走了这一发；行为级的判别式证明在 Rust 的 P11（Rust 按 clientId 复用实例，先退的那个连接还活着）。⚠ 「每一发 35 都回 SUCCESS」在本移植**不可观测**——传输层有意不调 `IRpcHook#DoAfterResponse`（见 `Remoting/RemotingClient.cs`），U5 的 broker 侧效果是它的替代判据。头形状（含**纯空白组名**同空串处理，整个字段不上线）、扇出**含 slave**（`GetAllBrokerAddrs` vs 心跳用的 master 优先 `GetRouteOfAllBrokers`）、单台失败被吞且剩下的照旧注销、超时与 Java 的 `getMqClientApiTimeout()`=**3000ms** 同口径，共 9 项由 `ProducerUnregisterTests` 离线锁死（自带一个同一 brokerName 下挂 master(0)+slave(1) 的假集群——本机真集群只有一台 master，这条判据在真机上不可达） |
| scheduled-intervals | 20 PASS / 0 FAIL（2026-09-24 实测） | I1~I3（与 Python `verify_interval_live.py`、C++ `rmq_live_scheduled_intervals`、Rust `live_scheduled_intervals` 同场景）：I3 门面配的周期真的落到实例（`PollNameServerIntervalMillis` → 路由刷新周期，1s 组与不配的 30s 对照组各断言一次）→ I1 两个生产者各把一个**还没建出来**的 topic 登记进在用集合，先等 1.5s 让两边首跳（`scheduleAtFixedRate` 的 initialDelay=10ms）都落空一次、再建 topic ⇒ 缓存里何时出现它只由周期决定：1s 组 0.53s 拿到，那一刻 30s 组**还没有**，最终 28.76s 拿到（固定速率锚定，逐跳对着同一时间轴算、误差不累积）→ I2 两个消费者（落盘周期 1s / 60s）各消费 3 条后 broker 位点仍是 0，首个落盘落在 **10.49s**（≈ Java `:417-423` 的 initialDelay 10s，60s 组同样是 10.81s —— 这一步由 initialDelay 驱动、不是周期），第二批后 1s 组 0.68s 内把 6 推上去、60s 组**仍是 3**（下一跳在 60s 后），`Shutdown()` 收尾补一笔把 6 落盘。⚠ 修之前这里必然红：macOS 上 `ManualResetEventSlim.Wait(100ms)` 实测 131ms（系统定时器多给一个 tick），「按 100ms 切片睡满 30s」实际要 39.2s，全部周期被拉长 ~31% —— 现在统一走 `Schedules.WaitUntil(...)`（`ManualResetEventSlim.Wait` 一次睡到绝对计划时刻，整段又能被 `Shutdown` 的 `Set` 立刻唤醒；落后于计划时不等待、立刻补跑，与 Java 的 catch-up 一致），另有一组离线用例证明首跳落在 initialDelay 而不是 initialDelay+period |
| tls | PASS（TLS 全链路 + 传输层压测，见下节「TLS」） |

单测：`dotnet test tests/RocketMQ.Client.Tests` → **604 passed / 0 failed**，零 warning
（`Directory.Build.props` 开了 `TreatWarningsAsErrors`）。`FlowControlTests`（7 项）锁住拉取前
流控的**五个阈值**（Java `ProcessQueue`）：条数 `>= PullThresholdForQueue`（含 Java
`Math.max(1,n)` 的守卫——配 0 不是全放行而是 1 条就停）、字节 `>= PullThresholdSizeForQueue`
且单位是 **MiB**（`<=0` 关闭，正好 1MiB 即算命中）、位点跨度**严格大于**
`ConsumeConcurrentlyMaxSpan`（乱序缓冲量真实 min/max 而不是首尾差）、topic 级累计条数/字节
跨本实例该 topic **所有**队列聚合（别的 topic 不许掺进来，且 topic 级字节闸门**不复用**队列级
那道开关——Rust 曾这么错过：离线全绿，真机上那道闸门静默失效）、判定顺序条数→字节→跨度→
topic 条数→topic 字节、**命中一次只记一格** `FlowControlTriggered`。命中原因串按 Java 的口径
用 `F1` + `MB` 后缀（`size=1.2MB`），单测把文案也锁死，因为运维只看得到这一行。
`FailFastTests`（6 项，真 socket 而不是 mock 集群——被测的正是"读线程看见 EOF 之后做了什么"）锁住
连接判死（Java `NettyRemotingHandler#close` → `failFast` → `requestFail`）：同步调用在对端断开后**毫秒级**
抛 `RemotingSendRequestException` 而不是等满 30s 超时（超时故意设长一个量级，任何"等到超时才算失败"的实现
都会被耗时上限抓住）、异步回调同理、**failFast 与超时清理线程抢同一条 opaque 时回调只投递一次**（这里把超时
压到 200ms 让两条路径真的撞上）、判死一条连接不牵连另一条连接上尚未应答的请求（它最终拿到自己的成功响应）、
判死之后**同一地址**建新连接跑完新请求（按 `Connection` **对象引用**认领在途请求，旧读线程的收尾不许误伤新连接
——真机给不出这个时间窗，只有这里能锁）、`Shutdown()` 把在途排空且可重复调用不补投。
`ConsumerCheckConfigTests`（11 项）锁住启动期数值闸门（Java
`DefaultMQPushConsumerImpl#checkConfig` 数值段 `:1099-1209`）：一张与 Python/C++/Rust 同构的
`Gates` 表把 12 条区间的**两端各测一次**（越界各拒一次、放行各一次，文案逐字对 Java 只去
`FAQUrl` 尾巴）、`pullThresholdForTopic`/`pullThresholdSizeForTopic` 的 `-1` 关闭哨兵（其余闸门
没有这层豁免，`-1` 照拒）、`pullInterval` 的**下界是 0**、`consumeThreadMin > consumeThreadMax`
**严格大于**（相等合法、消息带两个数值）、`popBatchNums` 跟随 Java 字面 `<= 0`、多条同时越界时
**按 Java 顺序**报第一条，以及 `Start()` 在建连之前就把坏配置拒掉且 `IsStarted` 仍为 false。
⚠ 三条闸门（`consumeThreadMin/Max`、`ConsumeMessageBatchMaxSize`）的下界从公开 API **走不到**：
setter 的 `Math.Max(1, n)` 会把 0 抬成 1，这是本移植与 Java 的一处已知差异，单测锁的是这个兜底
本身（而不是假装下界可测）。Java `:1058` 的 `consumeTimestamp` 格式校验在这里是 no-op——本移植
没有可配的 `ConsumeTimestamp` 属性，属"少一个可配项"而非漏校验。
`PullExpiredTests`（12 项）锁住
拉取循环停摆自愈的判据与收尾：阈值 120000ms 与**严格大于**边界（用注入时钟调
`PullStalledForTest(key, now)`，-120000 不算、-120001 才算——等真 120s 分不清走的哪一支）、
从没盖过章的新循环不算停摆、线程已退出即刻算（盖章再新鲜也照撤）、健康队列一律不动
（换线程等于丢在途重投）、撤走时持久化已消费位点并清掉盖章/缓冲/游标（没有已消费位点就不臆造
一个 0）、判据**逐队列独立**（一路停摆不许连带换掉兄弟队列的循环）、没分配给本实例的队列不扫、`Start()` 之前与停机途中都不判停摆（否则刷一堆假 `[BUG]`
日志）、POP 分支扫掉 `PopProcessQueue` 并 `SetDropped`。
`ScheduledIntervalsTests`（4 项）把 Java `MQClientInstance#startScheduledTask:389-432` 的
`scheduleAtFixedRate(work, initialDelay, period)` 语义钉在 `WireRecord.ArrivalMs` 上：五个门面的
默认值 = Java `ClientConfig:58/:66` 的 30000 / 5000（setter 也真的生效，不是只读装饰）、`Start()`
时门面上的周期交给实例且**非正数回落到 30000**（不是退化成忙转）、路由刷新的**首跳落在 10ms 的
initialDelay**（阈值取 period/2，只有顺序写错才会翻倍到一个周期）之后按配置周期重复（离线周期
1200ms，实测 11 / 1212 / 2412ms）。⚠ 这两个偏差在真机上只表现为「慢」——没有异常、没有缺字段，
只有「新 topic 的路由要等半分钟才刷新」这种没人会去计时的现象，所以判据必须钉在到达时刻上；
位点落盘的节奏只能在真集群上验（`scheduled-intervals` 子命令），离线这一层锁的是「周期字段被读到、
门面透传正确」。
`SendRetryTests`（15 项）用
**进程内 mock 集群**（真 socket + 脚本化响应码）锁死 `sendDefaultImpl` 的重试分类语义 ——
这些分支真集群给不了： broker 不会稳定回 SYSTEM_BUSY，也不会刚好"路由里的地址连不上"。
其中两项锁**发送头那三个跟着路由/配置走的字段**（`SendHeaderCarriesBrokerNameAndTopicKeys`、
`EmptyBrokerNameStaysOutOfTheSendHeader`）：不配置时 `c`=`TBW102`/`d`=4（Java 那两个常量），配了
`CreateTopicKey`/`DefaultTopicQueueNums` 后同步 / 批量 320 / 单向三种入口带同一份值，轮询换到
broker-1 时 `n` 跟着换成**这一笔选中的**那台（不是路由里的第一台），`d=0` 原样上线（0 是调用方
明说的 0，不能被默认值顶掉），手工指定空 brokerName 的队列时 `n` **整条键消失**而不是写 `n=`
（Java `@CFNullable` + `writeIfNotNull`）；异步链自建头，单独一项抓真报文
（`ProducerAsyncTests.SendAsyncHeaderCarriesBrokerNameAndTopicKeys`）。
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
`SearchOffsetBoundaryTests`（8 项）锁时间戳查位点的 `boundaryType`：入网文本是
`Enum.toString()` 的**大写枚举名** `LOWER`/`UPPER`（不是 `getName()` 的小写名）、
`@CFNullable` 缺键不写且回读为 null、`BoundaryTypeNames.GetType` 的宽松解析
（只有 `equalsIgnoreCase("upper")` 才是 UPPER）、以及 **MockCluster 抓真报文**验三个 admin 入口
（`SearchOffset`/`SearchLowerBoundaryOffset` 发 LOWER、`SearchUpperBoundaryOffset` 发 UPPER、
`SearchOffsetByBoundary(..., null)` 整键不上线）。
`AclTests` 里新增的 4 项锁住请求钩子的**组合顺序**：`RequestHooks.Compose` 的形状
（stream 在前、用户钩子在后）、`ReqT="0"` 落在 ACL 签名**之内**（用签名后的 ExtFields 复算
能对上）、反证（顺序写反时同一断言必须红）、`ChainedRpcHook` 逐个转发 `doAfterResponse`。

## TLS

`TlsEnable = true`（或 `ROCKETMQ_TLS_ENABLE=1`）后每条出连接换成 `SslStream`，握手在
`AuthenticateAsClient` 里完成、随后整个流都走 TLS。test-mode（对齐 Java
`tls.test.mode.enable`，默认开）信任 broker 自签证书、不校验主机名，所以本机 5.5.1 集群
**不用改 `useTLS`**：nameServer 9876 与 broker 10911 按首字节嗅探，明文与 TLS 同一端口都收。

线程契约是本端口的关键约束：`SslStream` 只支持**一个并发读 + 一个并发写**（运行时源码
`SslStream.IO.cs` 里 `_nestedRead` 与 `_nestedWrite` 是两把独立的 `Interlocked` 闸门，同类
重入才抛 `net_io_invalidnestedcall`，读写互不干涉）。这里每条连接恰好一个读线程 + 一把
`Connection.WriteLock` 串行化写侧，正落在这个契约内 —— 注释写在
`src/RocketMQ.Client/Remoting/RemotingClient.cs` 的 `WriteLock` 上，别把它当成可以随手删的锁，
也别给同一条连接再加第二个读者。

`dotnet $PROG tls <namesrv> <topic> <group>` 除了真发真收，还先跑两段传输层压测
（2026-09-22 本机实测）：S0a 每轮新建一条 TLS 连接只打一个请求、30 轮 **0 丢**（最慢一轮
110ms，含握手）；S0b 单条 TLS 连接上 16 线程并发 320 笔 **0 失败**、响应 opaque 逐笔对上、
总耗时 38ms。之后 TLS 生产者 + TLS push 消费者 3 发 3 收，生产侧注入的 `traceparent`
在消费侧提取到且合法。

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
- **异步发送背压的闸是手写公平信号量，语义与 Java 有三处不同。** `SendAsync` 在**调用方线程**
  上先过 `FairSemaphore`（对应 Java `DefaultMQProducerImpl.executeAsyncMessageSend:635-682`）：
  按条数拿 1 格、再按**压缩前** body 字节数拿 N 格，共享同一份 `timeout` 预算，拿不到就回调
  `send message tryAcquire semaphoreAsyncNum/Size timeout`（Java :654-658 / :667-671 原文案），
  许可在把结果交给用户**之前**归还是先 size 后 num（`BackpressureSendCallBack:599-610`）。
  差别：① Java 的 `Semaphore(permits, true)` 运行时改容量靠 `ReadWriteCASLock` 换**新对象**
  （`DefaultMQProducer:1383-1391`），换的瞬间旧对象上等待的线程全部搁死；我们原地改绝对容量
  （`free = total - 在途`，收缩可负）、`SetTotalPermits` 顺带叫醒等待者。② Java 的拒绝分支
  （`executor.submit` 抛 `RejectedExecutionException` 时：开背压 ⇒ 在**调用方线程**就地跑完
  这一笔，好让回调把已扣的许可还回来；关背压 ⇒ 抛 `MQClientException("executor rejected")`，
  :675-681）本端口同构 —— 内建 `AsyncSenderExecutor` 的队列有界（50000），`Submit` 投不进就
  抛同一异常、走同样两条分支。③ Java 的三个异步入口里本端口有两个：默认入口与**定点**入口
  （`SendAsync(msg, cb, timeout, mq)`），selector 异步入口（`send(msg, selector, cb, timeout)`）
  没有对应物。
  回归：`tests/RocketMQ.Client.Tests/BackPressureTests.cs`（12 项，含两条丢唤醒守卫）与
  `rmq backpressure` 的 B1–B5。
- **异步发送有一根真正的发送池，`Shutdown()` 还会把它排空（Java/Python 都不等）。**
  `SendAsync` 的链路：调用方线程过背压闸（上一条）→ `AsyncSenderExecutor_N`（core==max==CPU
  核数、队列有界 50000，对应 Java `getAsyncSenderExecutor:1608-1613`）跑**准备段**（出队先核对
  共享预算，超了回调 `DEFAULT ASYNC send call timeout`；校验、压缩、选队列、建请求、
  `CheckForbiddenHook`、`SendMessageHook.before` 都在这一段，请求只建一次）→ 传输层
  `InvokeAsync` → 失败走 `onExceptionImpl` 形状的重试链（上限 `retryTimesWhenSendAsyncFailed`、
  每轮换 broker 并换新 opaque、**不看** `RetryResponseCodes`：broker 明确回了错就不换机器）→
  `NettyClientPublicExecutor_N` 上跑 `SendMessageHook.after` + 归还许可 + 用户回调（恰好一次）。
  三处要说清楚的：
  ① **回调线程分岔**（与 Python `_complete` 同一条）：请求交给传输层**之前**的失败（闸门、排队超
  预算、校验、`The broker[x] not exist`）在当下这根线程就地回调，只有传输层带回来的结果才转投回调
  池。实测 A1：准备段在 `AsyncSenderExecutor_1`、成功回调在 `NettyClientPublicExecutor_1`。
  ② **`Shutdown()` 排空**：Java/Python 用不等待的 `defaultAsyncSenderExecutor.shutdown()`，
  「发完立刻关」会把队列里没跑到的任务连回调一起丢掉；本端口先拒新请求（`producer already
  shutdown`，等价 Java `SHUTDOWN_ALREADY`）、再等两根池排空，保证交进来的每一笔都跑完准备段
  并把报文交给传输层。⚠ 保证**止于**「交给传输层」：紧接着就关客户端，broker 还没读走的尾部
  几帧会随连接丢掉、响应没回来的那几笔也就没有终态（离线跑整套用例实测 32/36 上线、单跑稳定
  36/36；真机 A6 实测 36 笔全上线、35 笔拿到终态）—— 后半句与 Java 同一条（关客户端会停掉
  超时清理并清空在途表）。要每笔都有回调，调用方得自己等完再关。
  ③ **自带池**：`AsyncSenderExecutor` 属性在 `Start()` 之前挂上后，池的生命周期归调用方（Java
  `setAsyncSenderExecutor`），生产者既不排空也不关它。
  回归：`tests/RocketMQ.Client.Tests/ProducerAsyncTests.cs`（33 项，进程内假 broker 取证：队满两条
  分支、预算共享、重试链每轮换新 opaque、外层 catch 原样抛出并记 `UpdateFaultItem`、异步链自建头的
  `n`/`c`/`d` 三键）与
  `rmq async-send` 的 A1–A6（真 broker 上读回原文、线程口径、落地对账）。
