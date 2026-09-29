# rocketmq-client-remoting (C++)

RocketMQ 经典 remoting 协议（对齐 5.x）的 C++17 实现，与本仓库的
Python 参考实现（`../python/`）及 .NET / Rust 实现（`../dotnet/`、`../rust/`）逐项对齐。

**无第三方运行时依赖**（只用 POSIX socket + 标准库 + 系统压缩库：zlib 必需，liblz4 /
libzstd 可选，找不到就只关那一个后端），网络层手写，目的是把"字节到底长什么样"
暴露出来、便于各语言实现之间做逐字节互操作验证。

已实现范围：

| 层 | 内容 |
| --- | --- |
| 协议层 | JSON / RocketMQ 二进制两路序列化；`RemotingCommand` 帧编解码；CommandCustomHeader 家族（含 V2 短字段名）；17 段消息存储格式与 6 段批量格式 |
| 传输层 | `RemotingClient`：同步 / 异步 / oneway、半包重组、opaque 匹配、重连、**GO_AWAY(1500) 换连接重发一次**、连接判死、SIGPIPE 处理 |
| 路由 / 心跳 | `TopicRouteData` / `QueueData` / `BrokerData`、`SubscriptionData`、`HeartbeatData` |
| 客户端 | `MQClientInstance`、`DefaultMQProducer`、`DefaultMQPushConsumer`、`DefaultMQPullConsumer`、`DefaultLitePullConsumer`、**`DefaultMQAdminExt`** |
| 异步发送 | `sendAsync`（含定点 `sendAsync(msg, mq, cb)`）跑在真实的 `AsyncSenderExecutor_1..N`（core==max==CPU 核数、有界队列 50000）上，调用方不阻塞；`retryTimesWhenSendAsyncFailed` 的换 broker 重试链只在 remoting 层失败时继续（已收到响应的错误码原样回调、不重试），重试**复用同一请求**只换 opaque，超时预算是整条链共享的剩余时间；用户回调与 `SendMessageHook.after` 在 `NettyClientPublicExecutor_N` 上跑，回调抛异常吞掉不带走 worker；`enableBackpressureForAsyncMode`（默认关）打开后，异步发送在**调用方线程上、投入 `AsyncSenderExecutor` 之前**过两个**公平**信号量的闸（在途 1024 条 / 100M 字节，地板 10 条 / 1M 字节），等不到许可就回调 `send message tryAcquire semaphoreAsyncNum|Size timeout`、一次请求都不发出，许可在链终点按「先 size 后 num」归还，队满时开着背压改为就地跑 |
| 校验门 | `Validators` / `TopicValidator`：`checkTopic` / `checkGroup` / `isSystemTopic` / `isNotAllowedSendTopic` / `checkMessage`，四类 facade 的 `start()` 在建客户端实例**之前**跑完组名校验（纯本地判定，失败不碰网络） |
| 压缩 | zlib / LZ4 Frame / ZSTD 三后端：生产端自动压缩 + 消费端自动解压（线上帧格式与各语言实现互通；`-DRMQ_WITH_ZLIB=OFF` 等可逐个关） |

**未实测**：Windows 分支（代码在，未在真机跑过）。

## 构建

```bash
cd cpp
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build          # 产出 librocketmq_remoting.a + tests/* + examples/*
```

工具链：CMake ≥ 3.15、C++17 编译器（Apple clang 14+ / GCC / MSVC）、`Threads`、`ZLIB`；
liblz4 / libzstd 用系统包（`find_path` + `find_library`，也接受 `lz4::lz4` / `zstd::zstd`
这类 CMake config target），**不下载、不 vendor 任何第三方源码**。
开 `-Wall -Wextra`（未开 `-Werror`），**目标是零 warning**。
MSVC 下自动加 `/utf-8`（源码含中文注释，否则 C4819）；Windows 走 `winsock`（`ws2_32`）。

选项：
- `-DRMQ_BUILD_TESTS=OFF` / `-DRMQ_BUILD_EXAMPLES=OFF`
- `-DRMQ_WITH_ZLIB=OFF` / `-DRMQ_WITH_LZ4=OFF` / `-DRMQ_WITH_ZSTD=OFF`：逐个关掉压缩后端。
  注意关掉之后**遇到该压缩消息会抛异常**，而不是静默返回压缩字节
  —— 避免把数据损坏伪装成成功。CMake 找不到库时自动只关那一个后端，configure 照样成功。

configure 日志里会写明每个可选后端的状态（zlib 走 `find_package(ZLIB REQUIRED)`，
缺了直接 configure 失败）：

```
RocketMQ client zstd: enabled (/usr/local/lib/libzstd.dylib)
RocketMQ client lz4:  enabled (/usr/local/lib/liblz4.dylib)
RocketMQ client TLS:  enabled (OpenSSL 3.6.3)
```

## TLS

`RMQ_ENABLE_TLS`（找到 OpenSSL 时默认 ON）时每条 TLS 连接一个 `TlsSession`。
**会话内部有一把 io 锁**（`src/remoting/tls_session.h`）：本传输层是"一连接一个读线程
（`SSL_pending`/`SSL_read`）+ 调用方线程写（`SSL_write`）"的形状，两个线程必然在同一条
SSL 会话上交叠，而 OpenSSL 不支持两个线程同时用一个 `SSL` 对象。连接层原来那把
`writeMutex` 只锁写侧，挡不住读/写重叠，所以锁下沉到会话里，`read` / `writeAll` /
`pending` / `shutdown` 每次 `SSL_*` 调用取放一次（`writeAll` 的重试睡眠在锁外）。

本机 loopback + 真 nameServer 实测（`examples/live_tls.cpp` 的 S0）：一条 TLS 连接上
16 线程并发 320 笔，加锁那几趟 20~26ms、同一台机器不加锁那趟 17~19ms，两种都是 0 失败、
响应 opaque 逐笔对上；另加 30 轮"每轮新建 TLS 连接打首包"，0 丢。也就是说这把锁是
**用法正确性**的修复：几毫秒摊到 320 笔（每笔 10~20µs 量级），在本机量不出有意义的代价。

## 测试

```bash
cd build && ctest --output-on-failure     # 50 个用例，3986 项断言（49 个测试二进制 3913 + interop 73），~64s
```

| 用例 | 断言 | 覆盖 |
| --- | --- | --- |
| `codec` | 77 | JSON / ROCKETMQ / `RemotingCommand` 两路 / 消息 17 段与 6 段 / header V1↔V2 / hashCode / CRC32 / msgId |
| `route_heartbeat` | 106 | 路由类往返 + 按 perm 过滤队列（**发布信息**额外跳过 `brokerAddrs` 里没有 master 的 broker；**订阅信息**只看读位 + `readQueueNums`、不筛 master 也不查 brokerDatas，两条口径各锁一条负控腿）；`SubscriptionData` / `HeartbeatData` 往返 + 字段名守卫 |
| `publish_route_master` | 44 | 发布地址「只要 master」的**地址侧**半边：只剩从节点（brokerId=1）时发布地址查不到，而同一份路由上退让口径 `brokerAddrOf` 必须拿得到从节点（两条口径不能合成一条）；缓存为空时 `publishAddrFor` 先按 topic 刷一次路由再查（假 name server 逐笔数 `GET_ROUTEINFO_BY_TOPIC`，恰好一次）；master 掉线与未知 brokerName 一律**本端**抛 `The broker[X] not exist`、responseCode=-1，且报 not exist 之前必须先刷路由；管理侧 `getMaxOffset` 同样只打主。**订阅侧**三支同题：`lockBatchMq`/`unlockBatchMq` 只认主、**不刷路由**，只剩从节点时整台跳过（返回空集、零 LOCK/UNLOCK wire）；`popMessage` 只认主 → 刷一次路由 → 仍没有则本端抛 not exist；`queryConsumerOffset` 只认主 → 刷一次路由 → 重查**放宽**到从节点（位点是 HA 复制的同一份数据）。mock broker 的 LOCK_BATCH_MQ 应答按请求体里的 `mqSet` 回 `lockOKMQSet`，POP 回 `POLLING_TIMEOUT`。平表由**真的** `updateTopicRouteInfoFromNameServer` 写入，换路由一律改假 name server 应答再让客户端自己刷。`CONSUME_FROM_FIRST_OFFSET` 的起点是**字面量 0**（push 与 lite pull 两支同形），**不**发 `GET_MIN_OFFSET(31)` 查询。⚠ `seekToBegin()` 是有意的例外：真会调 minOffset |
| `transport` | 55 | 真实本机 TCP：同步/异步/oneway、**半包重组**、opaque 匹配、建连失败、超时、重连、**GO_AWAY 重发（同步+异步、只重发一次、开关关掉不重连）**、地址解析 |
| `fail_fast` | 19 | 对端断开时在途请求立刻判死：真 socket 对端读完一帧就关 ⇒ 同步调用毫秒级抛 `RemotingSendRequestException`（不是等满超时才报 `RemotingTimeoutException`——异步发送的重试分类按异常**类型**分流）、异步回调**恰好一次**、按**连接对象身份**认领在途请求（别人家的请求不受牵连，同地址换新连接后的新请求也不被旧读线程误伤）、`shutdown` 把在途排空 |
| `compression` | 152 | 三后端：类型解析（含 `0→ZLIB` 兼容映射）、zlib / **LZ4 Frame** / **ZSTD** 往返、**外部硬编码真值夹具**（zlib、lz4、zstd 各产的标准帧）、帧头 magic 与 LZ4 block-independence 位、17 段报文 × 三种类型、解压后清 flag、后端缺失或未知类型必须抛错而非交出压缩流 |
| `admin` | 188 | fastjson2 非法 JSON 容错、`ConsumeStatsList` 字段名守卫（键名错一个字符就静默解析成空列表）、`TopicConfig` / `SubscriptionGroupConfig` 默认值与字段名、`TopicStatsTable` / `ConsumeStats` / `ResetOffsetBody`、properties 文本往返、`PermName::isValid`、`SearchOffsetRequestHeader.boundaryType`（入网是大写枚举名、`@CFNullable` 缺键不写、回解只认 `equalsIgnoreCase("upper")`） |
| `logging` | 36 | 行格式（毫秒 / pid / 线程名 / `文件:行号`）、主线程落 `main`、线程名 thread-local、按大小轮转与 `maxIndex` 上限、级别过滤、关闭文件输出后不写盘 |
| `acl` | 56 | ACL 签名算法（extFields 按 key 字典序、只拼 value、跳过 Signature，再拼 body）与官方签名向量对拍 |
| `request_reply` | 40 | 请求-响应模式的消息编解码、`reply_to` 属性、correlationId 匹配与超时；**错误码口径**：等应答超时抛 `RemotingTimeoutException` 并带 10006 `REQUEST_TIMEOUT_EXCEPTION`、`createReplyMessage` 造不出应答时带 10007 `CREATE_REPLY_MESSAGE_EXCEPTION`（文案点到缺失的 `CLUSTER` 属性） |
| `latency` | 31 | 故障规避：延迟窗口滑窗统计、可用性判定、broker 隔离与恢复、`sendLatencyFaultEnable` 开关 |
| `send_retry` | 100 | `sendDefaultImpl` 重试分类语义（进程内 mock 集群 + 真 socket）：可重试码换 broker、不可重试码立即抛、重试耗尽报 `BrokersSent`、单次超时钳位、预算耗尽报 callTimeout、无路由快速失败、连接失败隔离；**寻址缺失报 10004 而不是 10005**（一个地址都没配 ⇒ `NO_NAME_SERVER_EXCEPTION` + 官方原文案，配了但连不上 ⇒ 码值不是 10004）；同一套抓包取证明线上报文（`k`=unitMode、`ReqT`、发送请求码 310/320/325 与 `m`=batch 的三级判据）；**发送头那三个跟着路由/配置走的字段**同样抓真报文取证：默认 `c`=`TBW102`/`d`=4、`setCreateTopicKey`/`setDefaultTopicQueueNums` 之后二键换成配置值，`n` 取**路由选中的** broker 名，定点发送与批量 320 走同一份建头代码、三键都不能漏；**定点发送的 topic 守卫**：不符的同步单条与批量都在**任何 SEND 上线之前**拒（mock 端点逐笔数 `sendCount`）、文案 `message's topic not equal mq's topic`，同 topic 照常上线且线上 `b` 取消息自己的 topic；命名空间比的是 `queueWithNamespace` 包装**后**的资源名（`ns1%X` 不误拒、`ns2%X` 才拒）；**定点单向故意没有守卫** —— 报文 `b` 仍取 msg 自己的 topic、只有 `e` 来自指定队列；异步侧的同一守卫（**异步那句**文案 + 走回调）在 `producer_async` |
| `producer_async` | 229 | 真异步发送链（进程内 mock broker + 真 socket）：`sendAsync` 立刻返回、准备工作和请求都在 `AsyncSenderExecutor_N` 上、回调与 `SendMessageHook.after` 在 `NettyClientPublicExecutor_N` 上；出队后才算耗时（预算被排队吃光一次请求都不发）；只有 remoting 层失败才重试且重试**复用同一请求**只换 opaque；超时预算是整条链共享的剩余时间；已收到响应但 broker 报错码**不重试不包装**；定点发送只在同一台 broker 上重试、且必须自己刷出路由；队满把 `executor rejected` 抛给调用方；回调抛异常被吞掉。**异步发送背压**：开关默认关且关掉时**一格许可都不动**、两个配置的地板值、条数/字节闸在**调用方线程**上等到预算耗尽才回调（官方文案逐字）、被拒的发送一次请求都没发出、许可在链终点按「先 size 后 num」归还（失败与重试路径同样归还）、运行时扩容叫醒卡在闸上的人、队满时开着背压改为就地跑；**批量异步**：一批只发**一个**请求、只交付**一个**回调，字节闸按**整批** body 长度扣，未 `start()` 时与单条异步同一口径就地抛且**一个回调都不交付**，ID 顺序锁死（先给每条子消息 `setUniqID` → 再给整批那条补一个 → **最后**才 `setBody(encode())`）；**异步链的发送头**：异步自己建头，抓真报文验 `n`=该请求落到的 broker 名、`c`/`d` 取生产者的配置；**异步侧的定点 topic 守卫**：topic 不符时**走回调**（不抛给调用方）、文案是 `Topic of the message does not match its target message queue`、拒后一笔请求都没发 |
| `produce_accumulator` | 249 | 生产者自动攒批 `ProduceAccumulator`：三个场景（同步 / 异步 / 指定 `MessageQueue`）逐条对拍，默认三档参数逐值（10ms / 32KB / 32MB，含 `getTotalBatchMaxBytes` 返回 holdSize 这个**上游笔误**照抄）与三档参数校验的边界值 + 官方文案、`tryAddMessage` 全局字节闸门（放行即记账 / 归还 / 拒绝后调用方退回直发）、批量应答**拆条**（逗号分隔 msgId/offsetMsgId → 每条各自的结果、queueOffset 递增）、`AggregateKey` 四维分区（topic / mq / waitStoreMsgOK / tag）、批级属性（KEYS 空格并集、TAGS、WAIT）与「同步收集 keys、异步不收集」的不对称、守卫线程对空批次的清理与「已发完但 size 仍 > 0」批次的**保留**、同步/异步失败路径（异常上抛 + 额度归还、每个回调各拿一份）、`start → shutdown → start` 守卫线程可重建、守卫线程名 `<clientId>_GuardForSyncSend` / `_GuardForAsyncSend`、累加器按 clientId 复用；sender 生命周期：`detach` 后同步抛出且 `sendSync` 的 finally **仍归还额度**、异步回调拿到同一句文案而额度**不归还**、`attach` 只在 detached 时重绑 |
| `backpressure` | 50 | `FairSemaphore`：只有**队首**能拿许可（后来者不许插队）、超时返回 false 而不抛、超时/获准后都要**把队首换人这件事广播出去**（漏了这一步，后到的等待者会睡到自己的超时）、`release` 超过总量不校验、原地平移总量并保留在途份数（算出负的空闲许可也接受）、改容量能叫醒正堵在旧容量上的人 |
| `pop` | 93 | POP 协议管道：CK 反构（8 段 + `startOffsetInfo`/`msgOffsetInfo` 下标选择）、`bornTime`、ACK offset 语义 |
| `pop_consumer` | 54 | POP 消费循环：`ackIndex` 默认值、不可见时间内的 ack 与复活重投、`checkNeedAckOrDelay` 边界钳制；**POP 循环把拉取统计记进状态表**（`FOUND` 的 `incPullRT` 打在**空列表判定之前**、`msgFoundList` 非空才计 TPS、`POLLING_NOT_FOUND` 两格都不动）：走 `recordPopPullStats` 这道离线接缝，key 是 `topic@group`，漏记是**静默**的（弹、ack、消费全正常，只有 307 看板一片 0），真机半边由 `rmq_live_pop_consumer` 的 S5 取证 |
| `pop_orderly` | 16 | 顺序监听器 + POP 的**刻意不消费**（上游 5.5.x 的顺序 POP 服务是未完成骨架）：请求分流进骨架后 listener **一次都不被调**、不 ack（`waitAckCount` 只涨不落 ⇒ 交给流控把 pop 循环压停，broker 等 invisibleTime 到期复活重投）、请求留在集合里；去重集按 **(PopProcessQueue 引用, mq)** 判等（同队列重复提交只入队一次、rebalance 换新 `PopProcessQueue` 后照常入队、`force` 不产生第二份）；只有 `pq` 被撤销才摘请求；并发监听器那条分支**不受分流影响**（回归护栏）。离线锁不住的部分（POP 模式下 lockLoop/shutdown 不发 LOCK/UNLOCK_BATCH_MQ）留给真机侧 |
| `consume_ack_index` | 28 | classic 并发消费的 `ackIndex` 切分：默认整批认可不回投、listener 收窄到 0 时尾巴逐条 `sendMessageBack`、`RECONSUME_LATER` 强制整批回投、广播模式不回投、**回投失败时塞回队首且位点不越过它**（未 `start()` 的消费者回投必定失败，所以离线锁的是失败分支；「回投成功 → 位点整批前进」由 `rmq_live_redelivery` 的 S10 在真机上取证） |
| `trace` | 92 | 消息轨迹：与官方轨迹实现**逐字节对拍**（Pub / SubBefore / SubAfter / EndTransaction / Recall）+ 编解码双向 + 无 keys 空段容错 + 坏记录隔离 + 分发器分组/切块 |
| `orderly_reconsume` | 74 | 顺序消费的重投闸门：顺序侧把 `-1` 读成**不设上限**（不是并发侧的 16，两套口径是刻意的）；三条分支——没用尽就地 `reconsumeTimes + 1` 并挂起、用尽则回投、**只有回投失败**才继续挂起（回投成功必须提交位点，否则一条毒消息永久占住队列）；本地重投不换 topic；回投那条走内部生产者当**普通消息**发 `%RETRY%<group>`，且 `reconsumeTimes`/`maxReconsumeTimes` 要**抬进请求头**（broker 的 `handleRetryAndDLQ` 读的是 header 不是报文属性，抬错字段 broker 就退回订阅组默认的 16；负控腿：**普通 topic** 的发送不许抬）。另有**手工 `COMMIT`/`ROLLBACK`**：`autoCommit=true` 时两者都是**非法**状态、告警后当成功 ack，`autoCommit=false` 时 `COMMIT` 不重投直接推进位点、`ROLLBACK` 把整批退回 `ProcessQueue` 且**位点不动**、`SUSPEND` 永不提交；挂起时长的三档口径（context 给值优先 → 回落消费者配置 → 仍非法则钳到下限）；`consumeMessageDirectly` 比并发侧多 `CR_COMMIT`/`CR_ROLLBACK` 两档映射 |
| `correct_tags_offset` | 28 | 空应答也要把已消费位点推走：拉取应答是 `NO_NEW_MSG`/`NO_MATCHED_MSG` 时，这条队列的已消费位点必须跟着拉取游标 `nextBeginOffset` 走（只升不降），否则没人 ack 的消息会让位点永久卡死。闸门三条：ProcessQueue 上**没有待消费消息**、**没有在途消息**（`inflightCount_`）、以及只认这两种状态；负控腿覆盖「有在途就不修正」「`FOUND` 状态不许动位点」。真机半边由 `rmq_live_correct_tags_offset` 取证（零投递前提下 broker 位点前移） |
| `offset_illegal_recover` | 26 | OFFSET_ILLEGAL 纠错分支：broker 回 `PULL_OFFSET_MOVED`（对 OFFSET_OVERFLOW_BADLY / OFFSET_TOO_SMALL / OFFSET_RESET 一律如此，修正值在应答头 `nextBeginOffset`）时，处理是改位点（覆盖写）→ 丢队列（队列代号 +1）→ 冻结并**立刻落盘** → `wakeRebalanceLoop` 重建。这条路径错了是**静默**的两种极端：只拨游标不丢队列 —— 旧批次一 ack 又把位点推回非法值，与 broker 来回弹跳；位点没立刻落盘 —— 进程在下一轮周期落盘前崩掉，broker 上还是非法位点。冻结覆盖 `advanceConsumeOffset` 与 `correctTagsOffset` 两处，且**持续到队列被重建**。真机半边由 `rmq_live_offset_illegal` 取证 |
| `reset_offset` | 43 | 220 `RESET_CONSUMER_CLIENT_OFFSET` 的**客户端半边**：**请求体两种形状** —— map 形状 `ResetOffsetBody` 与数组形状 `ResetOffsetBodyForC`（`offsetTable` 是**数组**、字段名是驼峰 `brokerName/offset/queueId/topic`）；本端两种都解，并钉住「map 形状的解析器对数组只得到**空表**」这个机关（少了数组那一支 ⇒ 整笔 220 静默丢弃）；坏 body（空串 / 非 JSON / 别的形状）一律空表；**重置 = 丢队列 + 代号 +1 + 新位点经撤销尾巴落盘**：缓冲与 `lastPullAt` 作废、内存表清空、重置前取回而重置后才 ack 的旧批次**整批作废**、重建后的新批次照常推进；**范围**只动表里点名的队列、未分配的队列给了条目也是 no-op、空 topic / 空表直接返回；**第二次重置代号只增不减**；**广播模式**新位点必须当场落进本地文件且 merge 不是 replace、位点是 null 的条目不许凭空造；**222 报文的键名**逐键对齐 —— `isForce` 少一个字母都是**静默**的（broker 侧 `isForce` 恒为 false）、`offset`/`queueId` 缺一不可且 `false` 也要显式下发。真机半边由 `rmq_live_reset_offset` 取证 |
| `pull_expired` | 12 | 拉取循环停摆自愈（`PULL_MAX_IDLE_TIME` = **120000ms**，读 `rocketmq.client.pull.pullMaxIdleTime`）：阈值逐字锁死、判据是**严格大于**、没盖过章的新循环不算、循环线程已退出即刻算、健康队列一律不动（换线程等于丢在途重投）、撤走时持久化已消费位点并丢掉拉取游标与缓冲、分配即登记 `mqMap`、POP 分支读 `lastPopTimestamp` 且换一具干净的 `PopProcessQueue`、停机期间不判停摆、307 运行信息把 `lastPullTimestamp` 报成盖章节的真时刻、且 **`mqTable` 与 `mqPopTable` 互斥** |
| `pull_post_subscription` | 35 | 两项拉取侧行为：**`postSubscriptionWhenPull`**（默认 false；只在开关开且非类过滤时把 `subString` 拼进请求，`sysFlag` 的 SUBSCRIPTION 位 = `subExpression != null`；关掉是安全的 —— tag 过滤由客户端二次过滤兜底）；**`pullFromWhichNode`**（命中从节点时 `clearCommitOffsetFlag` —— 从节点不维护消费位点 —— 并且不打订阅；从节点缺席回落主节点且 COMMIT_OFFSET 位保留；应答头 `suggestWhichBrokerId` 回写 `pullFromWhichNodeTable`）。报文形状用**进程内假端点**从 socket 上取证（SUBSCRIPTION 位、`subscription` 键上没上线、打给 master 还是 slave、COMMIT_OFFSET 是否被清）；pull 消费者的表读写往返 |
| `flow_control` | 24 | 拉取前流控的**五个阈值**：条数 `>= pullThresholdForQueue`（含 `Math.max(1,n)` 的守卫——配 0 不是全放行而是 1 条就停）、字节 `>= pullThresholdSizeForQueue` 且单位是 **MiB**（`<=0` 关闭）、位点跨度**严格大于** `consumeConcurrentlyMaxSpan`（乱序缓冲量真实 min/max 而非首尾差）、topic 级累计条数/字节（跨本实例该 topic **所有**队列聚合，别的 topic 不许掺进来，且 topic 字节闸门**不复用**队列级那道开关）、判定顺序条数→字节→跨度→topic 条数→topic 字节，**命中一次只记一格** `flowControlTriggered()` |
| `hook` | 65 | `CheckForbiddenHook`（异常不吞、沿重试链传播）+ `FilterMessageHook`（可变 msgList、摘掉即静默跳过）+ 钩子异常隔离 |
| `consume_thread_pool` | 67 | 消费端 core/max 两档执行器（队列无界）：真实并发度 == corePoolSize、`setConsumeThreadNums` 生效、`updateCorePoolSize` 运行时调并发。**默认值两侧同为 20**（4.x 才是 min=20/max=64），于是默认配置下 `updateCorePoolSize` 只能往**下**调（守卫 `core < max`），回归用例 `testDefaultMaxIsTwenty` 钉住 |
| `top_addressing` | 37 | 动态 name server：WS 地址 / unitName / para 拼装、`clearNewLine`、非 200 与连接失败回退为空 |
| `consumer_stats` | 24 | `ConsumerStatsManager` 采样（sum/tps 窗口端点差分，不依赖真实时钟）+ `ConsumeStatus` / `ConsumerRunningInfo`(307) 编码 |
| `trace_context` | 24 | W3C `traceparent` 生成/校验/子 span/注入不覆盖/属性提取 |
| `interop` | 73 | C++ ↔ Python 双向编解码 + 路由/心跳结构体双向语义等价 |
| `lite_pull` | 51 | `DefaultLitePullConsumer` 无网络状态机（subscribe/assign/seek/poll/committed）+ 221/309 应答体 wire 形状 + **三张位点表**：`pullOffset`（拉了多少）/`consumeOffset`（`poll()` 交出去多少）/`offsetTable`（`commit(Map,persist=false)` 攒在内存里的那格）各自独立，提交只走已消费游标（把拉取游标交给 broker 等于静默丢消息）；`maybeAutoCommit` 只在 `poll()` 里查、全局一个 `nextAutoCommitDeadline`（初值 -1 ⇒ 首轮交付之前不提交），不再轮询的调用方位点就不动；`commit(Map)` 空表回文案 `MessageQueues is empty, Ignore this commit`、`commit(Set)` 空集静默返回、值为 -1 打 `consumerOffset is -1 in messageQueue [...]` 并跳过、没分到的队列静默跳过；`persistAll(scope)` 抹掉 scope 之外的内存行；`committed()` 走 `MEMORY_FIRST_THEN_STORE`（命中内存就不问 broker，未命中回读并回填）；subscribe 模式撤队列先 persist 再整份丢掉状态，assign 模式收缩只丢游标、不 persist 也不碰 offsetStore |
| `lite_pull_cursor` | 18 | **拉取游标跟随 `nextBeginOffset`**（一轮拉取**成功返回**之后，无论 `FOUND`/`NO_NEW_MSG`/`NO_MATCHED_MSG`/`OFFSET_ILLEGAL`，拉取游标都要推进到 broker 给的 `nextBeginOffset`）：旧实现只在 `FOUND` 时推游标 —— `NO_MATCHED_MSG` 时原地不动、每轮重扫同一段；`OFFSET_ILLEGAL` 的纠正值吃不到、越界不自愈，两者在真机都表现为「消费者活着但永远收不到消息」。假集群按脚本回应答，取证从**线上报文**查「下一笔 `PULL_MESSAGE` 是不是从新位点起的」；负控腿覆盖唯一那只刹车「在途应答撞上本轮 seek」。同一条测试还锁死 `CONSUME_FROM_FIRST_OFFSET` 的起点是**字面量 0**、整份请求日志里 `GET_MIN_OFFSET(31)` 出现 0 次。**请求码 / lite 位**（7 项）：抓每一笔拉取请求的**请求码**与 `sysFlag` —— lite 的每一笔必须是 `LITE_PULL_MESSAGE(361)` 且带 `FLAG_LITE_PULL_MESSAGE(0x10)` 位，并加一条经典拉取对照腿（请求码必须是 `PULL_MESSAGE(11)` 且无 lite 位 —— 多置一位就会让普通消费者也撞上 lite 开关）。真机半边由 `rmq_live_lite_pull_cursor` 与 `rmq_live_lite_pull_code` 取证 |
| `local_offsets` | 18 | 广播模式本地位点文件的格式：`{"offsetTable":{<MessageQueue 直接当 JSON key>:<offset>}}` —— broker 侧序列化器把对象当 key 写出，**严格 JSON 非法**但自产自销能读回，四端共用同一份 `~/.rocketmq_offsets/<clientId>/<group>/offsets.json` 时互认（夹具是真实现跑出的实测文本，含 pretty 版）；字段序 `brokerName/queueId/topic`；紧凑与 pretty 两种文本本端都能读、旧的扁平格式仍认、坏文本（截断 / 双开 / 空串）一律拒；落盘走 `MixAll.string2File` 语义 —— 首写不产生 `.bak`、改写把**上一代**滚进 `.bak`，读取主文件优先、主文件缺失回落 `.bak`、两者都缺按首次启动处理 |
| `allocate_strategy` | 1167 | 六个策略：`AVG` / `AVG_BY_CIRCLE` 的官方用例（10/4、7/3、边界队列续接）、四道 `check` 守卫返回空结果而非抛异常、`CONFIG` 不查守卫且返回副本、`getName()` 与常量一致、N 消费者恰好不重不漏覆盖每个队列、多 broker 真实队列、三类消费者（push / pull / lite）默认 AVG 且可替换，策略为 null 时 `start()` 拒绝；**`CONSISTENT_HASH`** 用实测的哈希环表逐格对拍（6×2/6×3/10×4/20×10 与 vc=10 的 4×2/8×3、覆盖矩阵、注入自定义 `HashFunction` 后的 TreeMap 撞坑退化）、**`MACHINE_ROOM`** 的 `[0,1,4]/[2,3]` 分片与 `split("@")` 裁尾空段真值表、**`MACHINE_ROOM_NEARBY-<内层>`** 的同机房优先 + 无消费者机房由全员共享与 resolver 空机房**抛错**（保住上一轮分配） |
| `consistent_hash` | 42 | 一致性哈希环 + 自带 MD5：RFC 1321 附录 A 全向量（含 55/56/57/64/65/200 字节的填充边界）与 `hash()` 取前 4 字节大端的真值、环的路由稳定性与越过末尾回绕、空环返回 null、负虚拟节点数只在 `addNode` 抛、`i + existingReplicas` 的副本下标不重叠、`removeNode` 不误伤别的节点、注入自定义 hash 生效、`ringHashes()` 严格升序 |
| `validators` | 75 | 名字校验：字符表（码点 >=128 一律非法）与正则口径、12 个系统 topic / 8 个禁发 topic 名单（`TBW102` 可发、`%RETRY%` 可发）、`checkTopic`/`checkGroup` 的 blank→长度(127/120)→字符表顺序与文案逐字、`checkMessage` 只有 body 档位带 `MESSAGE_ILLEGAL(13)`、LMQ 分隔符、四类 facade 的 `start()` 组名门在建实例之前 |
| `broker_requests` | 11 | broker 反向请求 `NOTIFY_CONSUMER_IDS_CHANGED`(40)：注册在**实例级**的 clientRemotingProcessor（各消费者重复注册会互相覆盖）、计数 + 整组唤醒、`unregisterRebalanceWakeup` 后不再被叫醒但通知仍被处理、缺 `consumerGroup` 不抛、shutdown 清掉唤醒表 |
| `check_client_config` | 32 | broker 侧客户端配置校验 `CHECK_CLIENT_CONFIG`(46)：请求头为 **null**（线上没有 extFields）、body 是 `CheckClientRequestBody` 的 JSON、非 SUCCESS 抛 `MQClientException(响应码, remark)`；只发非 TAG 订阅、地址走**只读缓存**的 `findBrokerAddrByTopic` 且取不到就跳过、网络类异常换成固定文案、超时用 `mqClientApiTimeout`(3000ms) |
| `client_id` | 32 | clientId 口径：`buildMqClientId` 的 `ip@instanceName[@unitName]`（空白 unitName 不拼）、`changeInstanceNameToPID` 只改默认名且幂等、四类 facade `start()` 盖出的 `<ip>@<pid>#<nanoTime>`、同进程两个生产者不撞号、广播消费者保持 `DEFAULT`、显式 instanceName 原样透传 |
| `recall_message` | 28 | 定时消息撤回 `recallMessage`(370)：句柄编解码与 **`buildHandle` 真值向量**对拍（含无填充句柄、6 段新版本、v2/段数不足/非法 utf-8 全部按文案 `"recall handle is invalid"` 拒）、`RecallMessageRequestHeader` 逐键守卫（继承字段反射名是 **`bname`** 而不是 `brokerName`）、`SendMessageResponseHeader.recallHandle` 往返、producer 本地校验顺序（未 start / `%RETRY%` / `%DLQ%` / 非法句柄都在打网络**之前**秒回） |
| `consumer_check_config` | 85 | 启动期数值闸门：13 条区间的**两端各测一次**（`consumeThreadMin/Max [1,1000]`、`consumeConcurrentlyMaxSpan`/`pullThresholdForQueue [1,65535]`、`pullThresholdForTopic [1,6553500]`、`pullThresholdSizeForQueue [1,1024]` **MiB**、`pullThresholdSizeForTopic [1,102400]`、`pullInterval [0,65535]`（**下界是 0**，别照抄邻居的 1）、`consumeMessageBatchMaxSize`/`pullBatchSize [1,1024]`、`popInvisibleTime [5000,300000]`、`popBatchNums [1,32]`），文案逐字（只去掉 FAQ 尾巴）；`pullThresholdForTopic`/`pullThresholdSizeForTopic` 的 `-1` 是"关闭"哨兵、其余闸门没有这层豁免；`consumeThreadMin > consumeThreadMax` **严格大于**（相等合法）且消息带两个数值；`popBatchNums` 跟随字面 `<= 0`；比较一律 `< lo \|\| > hi`（两端闭）；多条同时越界时**按官方顺序**报第一条 |
| `producer_unregister` | 21 | 退出注销 `UNREGISTER_CLIENT`(35) 的线上形状：生产者侧头只有 `clientID`+`producerGroup`、消费者侧只有 `clientID`+`consumerGroup`、两侧都有时三个键齐全；**空白组名整个字段不上线**（传的是 null 而不是 `""`，broker 按 `group != null` 分派）；扇出**含从节点**且每台各一发（改成只打 master 的用例必然红）；单台回 `SYSTEM_ERROR` 时 `unregisterClient` 抛 `MQBrokerException`、`unregisterClientAllBrokers` 吞掉且**下一台照样发**；`getRouteOfAllBrokers`（生产者心跳用的主优先那一台）与 `getAllBrokerAddrs`（35 与**消费者**心跳用的主+从）分工守住——消费者心跳必须到从节点（从节点漏发不是少一发冗余：它会给指向自己的拉取回 `SUBSCRIPTION_NOT_EXIST`）|
| `subscribe_after_start` | 12 | 后置订阅与立即心跳：`start()` 之后 `subscribe` **不再**报 `already started`、新订阅立刻进活订阅表（`subscribedTopics()`，心跳与 rebalance 读的同一张表）且立刻推一次心跳；`unsubscribe` 只删表项、**不**发心跳；启动前订阅照旧、那时一笔心跳都不发。对着**连不上的** name server 启动（零 broker ⇒ 心跳一台都发不出去），所以离线只能锁到"表进对了、`already started` 不再抛"这一步——报文层面"broker 真收到带新订阅的心跳"由 `rmq_live_subscribe` 真机取证 |
| `scheduled_intervals` | 21 | 周期任务的推进口径（`scheduleAtFixedRate`）：**首跳落在 initialDelay 这一刻**而不是 initialDelay+period（离线把路由刷新周期设成 1200ms，实测到达时刻 11 / 1212 / 2412ms —— 旧写法是 1211 / 2411 / 3611）；**固定速率**而非「干完再睡一个周期」（逐跳对着同一时间轴算，慢一拍的轮次不累积漂移）；落后于计划时不等待、立刻补跑（catch-up）；`pollNameServerIntervalMillis` 门面→构造→实例一路透传、非正数回落默认 30000；位点落盘循环的周期只在 `start()` 读一次，运行期改字段不重排已定型的节奏 |

`interop` 会输出 `WARN` 记录 **Python 参考客户端侧的已知缺陷**（不影响退出码）。
出现新的 `WARN` 要读一下——它是跨语言偏差的显式台账。

## 真实集群联调

需要在跑 nameServer(9876) + broker(10911)、`autoCreateTopicEnable=true` 的集群。
这些工具**不进 ctest**（依赖外部集群）。

```bash
./build/examples/rmq_selfcheck                       # 不依赖集群的协议层自检
./build/examples/rmq_live_message_types 127.0.0.1:9876
./build/examples/rmq_admin_live         127.0.0.1:9876
./build/examples/rmq_compression_live   selftest 127.0.0.1:9876
./build/examples/rmq_compression_live   send|recv 127.0.0.1:9876 <topic> <group> <size> [codec]
./build/examples/rmq_live_acl           127.0.0.1:9876   # 需开 ACL 的集群
./build/examples/rmq_live_pull          127.0.0.1:9876
./build/examples/rmq_live_lite_pull     127.0.0.1:9876
./build/examples/rmq_live_lite_pull_cursor 127.0.0.1:9876  # 拉取游标：NO_MATCHED_MSG 跟过整段 + OFFSET_ILLEGAL 越界自愈
./build/examples/rmq_live_lite_pull_code 127.0.0.1:9876   # lite 请求码 361 + lite 位：运行时翻 litePullMessageEnable，退出前还原
./build/examples/rmq_live_request_reply 127.0.0.1:9876   # 325 应答链路 + 10006/10007 码
./build/examples/rmq_live_latency       127.0.0.1:9876
./build/examples/rmq_live_pop           127.0.0.1:9876
./build/examples/rmq_live_pop_consumer  127.0.0.1:9876   # POP 消费：全收/不重投/不可见时间重投 + 307 状态表里的 pullRT/pullTPS
./build/examples/rmq_live_redelivery    127.0.0.1:9876   # 重投/死信/重启/广播/顺序/流控/namespace/部分 ack/停摆自愈/顺序死信/显式 ack 回滚 十三段
./build/examples/rmq_live_trace         127.0.0.1:9876   # 需 broker traceTopicEnable=true
./build/examples/rmq_live_hook          127.0.0.1:9876
./build/examples/rmq_validators_live    127.0.0.1:9876
./build/examples/rmq_recall_live        127.0.0.1:9876   # 需 broker 开 recallMessageEnable（工具自己打开并还原）
./build/examples/rmq_live_unit_config   127.0.0.1:9876   # unitName/unitMode/stream
./build/examples/rmq_sql92_live         127.0.0.1:9876   # 需 broker 开 enablePropertyFilter=true
./build/examples/rmq_live_backpressure  127.0.0.1:9876   # 异步发送背压（两个公平信号量）
./build/examples/rmq_live_async_send    127.0.0.1:9876   # 异步发送内核（线程口径/并发/定点/拦截/批量/关池）
./build/examples/rmq_live_send_header   127.0.0.1:9876   # 发送头 c/d/n：自动建 topic 的队列数由模板与 d 决定
./build/examples/rmq_live_flow_control  127.0.0.1:9876   # 拉取前流控五个阈值（条数/字节/跨度/topic 级）+ 命中后不丢消息
./build/examples/rmq_live_producer_unregister 127.0.0.1:9876   # 退出时向每台 broker 注销 clientId(35)
./build/examples/rmq_live_fail_fast   127.0.0.1:9876   # broker 真死掉：挂起的长轮询秒级判死（会停一次 broker 再拉起，不删 store）
./build/examples/rmq_live_scheduled_intervals 127.0.0.1:9876   # 周期任务的 initialDelay/固定速率（含位点落盘 10s 首跳）
./build/examples/rmq_live_subscribe     127.0.0.1:9876   # 后置订阅：start() 之后 subscribe 立即推心跳、新 topic 真被消费
./build/examples/rmq_live_pinned_guard  127.0.0.1:9876   # 定点发送 topic 守卫：真路由不误拒、拒在本端且 broker 无痕、单向无守卫
./build/examples/rmq_live_correct_tags_offset 127.0.0.1:9876   # correctTagsOffset：NO_NEW_MSG/NO_MATCHED_MSG 空应答也把已提交位点推到 maxOffset
./build/examples/rmq_live_offset_illegal 127.0.0.1:9876   # OFFSET_ILLEGAL：整批作废在途/缓冲消息并按修正位点重建；修正位点立刻落盘
./build/examples/rmq_live_reset_offset 127.0.0.1:9876   # 220 重置消费位点：broker 推 220 后立刻落盘 + 在途批次作废 + 队列按新位点重建
./build/examples/rmq_live_pull_heartbeat 127.0.0.1:9876 127.0.0.1:10911 [从节点地址]   # 拉模式消费者的 203/38/35（给了从节点就一并断言）
./build/examples/rmq_live_publish_route_master 127.0.0.1:9876 127.0.0.1:10911 [从节点地址]   # 会停一次 master：发布队列归零/订阅不变/仍能从从节点消费 + 顺序锁/POP/位点读取三支订阅口径
./build/examples/rmq_live_clean_expired_msg 127.0.0.1:9876   # 挂起 listener 的逃生口：清扫回投 %RETRY% 再投第二次（约 4 分钟，等两个清扫周期）
```

任何工具失败以非 0 退出码结束。SKIP 项与原因会在输出里写清楚
（例如 uniqKey 查询需要 broker 开 RocksDB 索引，本机默认文件索引查不到属
**broker 配置差异，不是客户端 bug**）。

## 目录结构

```
cpp/
├── include/rocketmq/
│   ├── common/                 消息模型与常量
│   │   ├── message.h               Message / MessageExt / MessageBatch / MessageQueue
│   │   ├── message_decoder.h       17 段 + 6 段编解码（含压缩）
│   │   ├── compression.h           CompressorFactory（zlib / lz4 / zstd 类型解析）
│   │   ├── sysflag.h / mix_all.h / topic_config.h / subscription_data.h / util_all.h
│   │   ├── byte_buffer.h           大端读写游标
│   │   ├── consistent_hash.h       一致性哈希环（`ConsistentHashRouter` + 自带 MD5）
│   │   ├── recall_message_handle.h 定时消息撤回句柄 v1 编解码
│   │   ├── boundary_type.h         时间戳查位点的边界语义（LOWER/UPPER，含 getType 宽松解析）
│   │   ├── logging.h               header-only 日志（默认 INFO，按大小轮转，线程名/毫秒/文件:行）
│   │   └── net_compat.h            socket 跨平台兼容（含 SIGPIPE 处理）
│   ├── remoting/
│   │   ├── remoting_client.h       同步 / 异步 / oneway + 拆包重组
│   │   └── protocol/               json / serialize / remoting_command / codes /
│   │                               headers / route / heartbeat / body / admin_body /
│   │                               subscription
│   └── client/
│       ├── mq_client.h             MQClientInstance：路由发现 + 全部 RPC
│       ├── producer.h / consumer.h / admin.h / result.h / exception.h
│       ├── allocate_strategy.h     六种队列分配策略（AVG / AVG_BY_CIRCLE / CONFIG /
│       │                            CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY）
│       ├── hook.h / trace.h / trace_hook.h / trace_dispatcher.h
│       │                            钩子接口（Send/Consume/EndTransaction/CheckForbidden/
│       │                            FilterMessage）+ 消息轨迹文本编解码 + 异步分发
├── src/                        与 include 同构的 44 个 .cpp
├── examples/                   selfcheck / interop_tool + 33 个真机联调工具
└── tests/                      48 个测试源文件、49 个 ctest 用例（含 interop_check.py）
```

## 几个必须知道的实现约定

**字段名错一个就静默丢字段。** broker 按官方协议的属性名做 JSON 反序列化，
字段名对不上时不报错、字段直接消失（默认值顶上），所以入网键名是逐字段对拍出来的。

**枚举字段入网是大写枚举名。** `SearchOffsetRequestHeader.boundaryType` 就是实例：
线上是 `"LOWER"`/`"UPPER"`，`BoundaryType.getName()` 的 `"lower"`/`"upper"`
只喂给 `BoundaryType.getType` 做比对。broker 侧 `getType` 是**宽松**解析
（只有 `equalsIgnoreCase("upper")` 才是 UPPER，未知值一律 LOWER），
字段本身是 `@CFNullable`（不 set 就整键不写、缺键回落 LOWER）。
`DefaultMQAdminExt::searchLowerBoundaryOffset` / `searchUpperBoundaryOffset`
分别固定发这两个值，`searchOffset` 等价于 LOWER。

**fastjson2 会产出非法 JSON。** broker 序列化时 map 的对象 key 会被内联
（`{"offsetTable":{{"brokerName":"b",...}:{...}}}`）、数字 key 不加引号、允许
NaN/Infinity 与尾逗号。所以 `json.cpp` 里是**容错解析器**而不是严格 JSON parser。
改它的时候务必保留这套宽容逻辑，否则所有管理端响应体全崩。

**`RemotingCommand.body` 的存在判定是 `hasBody || !body.empty()`**，
只赋 `body` 不设 `hasBody` 也要能编码出 body。

**opaque 不能用 0 当"未设置"哨兵。** `opaqueCounter` 从 0 起算，第一个请求的 opaque 合法值就是 0；
传输层只在**与在途请求真的冲突**时才重分配，否则响应永远匹配不上。

**`TopicPublishInfo` 不可拷贝**，必须用 `std::shared_ptr` 取用 —— 它的队列轮询游标是跨调用
共享状态（官方用 ThreadLocal），按值返回会让每次发送都从 0 号队列重来。

**push 消费者的每条循环线程都要自己兜住异常**（`DefaultMQPushConsumer::runLoop`）。
`shutdown()` 先翻 `started_` 再 join，而循环里任何一处拿内部客户端都会抛
`MQClientException("consumer not started, call start() first")` —— 线程函数体外没有 catch 就是
`std::terminate`，**整个进程**跟着消费者一起没（真机上是 S6 直接中断，看起来像集群挂了）。
所以四条常驻线程（dispatch / 位点持久化 / 顺序锁 / rebalance）统一走 `runLoop`：异常记 WARN 后
睡 1s 重试，`started_`/`stop_` 一旦落下就地退出；派生拉取线程前和 `queuePullLoop` 入口都再查一次
这两个标志（join 之前那趟 rebalance 仍可能在建线程的当口被 shutdown 插队）。

**压缩失败 / 未知算法必须响亮。** `CompressorFactory::decompress` 对未支持类型抛异常，
`decodeMessage` 捕获后返回 `false`（消息被丢弃）。**绝不能原样透传压缩字节**：
外层会清掉 `COMPRESSED_FLAG`，透传等于把压缩流当正文交出去且事后无法识别，属于静默数据损坏。

**clientId 口径。** `buildClientId(instanceName, unitName, enableStream)` =
`<本机 IP>@<instanceName>[@<unitName>][@STREAM]`（空白 unitName 不拼段，
`@STREAM` 取枚举名而非 code）；instanceName 还是默认值 `DEFAULT` 时由各 facade 的
`start()` 调 `changeInstanceNameToPID` **就地**换成 `<pid>#<nanoTime>` —— 生产者与 admin
无条件，三个消费者只在 `CLUSTERING` 下（广播消费者保持 `DEFAULT`）。本机 IP 用 UDP
sockname 探测。回归：`tests/test_client_id.cpp`（ctest `client_id`）。

**unitMode / stream 是上线字段，不是本地摆设。** 三类开关各有落点：
`unitName` 进 clientId 与动态取址 URL（`-<unitName>?nofix=1`）；
`unitMode` 进 `SEND_MESSAGE_V2` 的单字母键 `k`、心跳 `ConsumerData.unitMode`、
回投请求头与过滤/禁行钩子上下文；`enableStreamRequestType` 让每笔请求带 `ReqT=0`
（值是 `RequestType.STREAM` 的 **code**，与 clientId 的枚举名后缀不同）。
生产者默认关 stream（只有 pull / lite 消费者在构造里置 `true`）。
**顺序是语义**：先装 Stream 钩子再装用户（ACL）钩子，`ReqT` 必须落在签名内容**之内**，
否则开鉴权的 broker 验签必失败。本端口的传输层只有一槽 RPCHook，所以 facade 一律经
`composeRequestHooks()` 合成后再 `registerRPCHook()`，且绑在 `MQClientInstance::start()`
**之前** —— 实例第一笔报文就该带着它。
回归：`tests/test_acl.cpp`（钩子组合与签名内容）+ `tests/test_send_retry.cpp`
（真 socket 上取证的 `k` / `ReqT`，并用 broker 侧算法重放验签）+
`examples/live_unit_config.cpp`（真 broker 的 topic `sysFlag` UNIT=0x1 / UNIT_SUB=0x2、
broker 记录的 clientId）。

**批量发送的请求码是 320，不是 310。** `sendRequestCode()`（`src/client/mq_client.cpp`）
按三级判据走：先 `isReplyMessage` ⇒ 325，再 `msg.isBatch` ⇒ `SEND_BATCH_MESSAGE(320)`，
否则 `SEND_MESSAGE_V2(310)`。注意请求码与 V2 头的单字母键 `m`（batch）是两件事：broker 按
`m` 选批量还是单条写入，码只影响服务端按码归类 —— 两个字段必须成对取证。
回归：`tests/test_send_retry.cpp`（真 socket 上取 `code` + `m`）+
`examples/live_message_types.cpp` 第 8 项（真 broker 把批量投成 3 条独立消息、
`queueOffset` 连续 0,1,2）。

**消息轨迹的解码器比官方实现更健壮。** 官方实现对无 keys 消息的 `SubBefore` 会数组越界抛异常
（5.5.1 上游真实缺陷，已复现），本项目缺段按空串取；并且**单条记录**解码失败只跳过自己 ——
官方是一条坏记录直接毁掉整条轨迹消息的解码（表现为控制台整批轨迹消失）。轨迹文本按
`\x01` 分段、`\x02` 结尾，切分必须用 **`String.split` 语义（丢弃末尾空串）**，
原生 split 会多出一段。故障排查提示：轨迹 topic 默认 `RMQ_SYS_TRACE_TOPIC`，
broker 需 `traceTopicEnable=true` 才预建。

## 日志

`include/rocketmq/common/logging.h` 是 header-only 日志，默认级别 **INFO**，
同时输出到 stderr 与 `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log`。

行格式 `日期.毫秒 级别 [pid] [线程名] [文件:行号] - 消息`：

```
2026-09-14 17:06:14.566 INFO  [57308] [main] [producer.cpp:110] - DefaultMQProducer[...] started, clientId=...
2026-09-14 17:07:09.086 INFO  [57316] [ConsumeMessageThread_0] [consumer.cpp:161] - DefaultMQPushConsumer[...] started
```

线程名：主线程落 `main`，工作线程由内部命名 ——
`ConsumeMessageThread_N` / `AsyncSenderThread_N` / `RemotingClientReader-<ip:port>`。
后两者只在异常路径留痕（连接关闭、非法帧长、解码失败），所以正常日志里通常只看到前两者。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CPP_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` / `OFF` |
| `ROCKETMQ_CPP_LOG_FILE` | `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log` | 设为空串/`OFF`/`NONE` 则只留 stderr |
| `ROCKETMQ_CPP_LOG_FILE_MAX_SIZE` | `67108864`（64MB） | 单文件上限；`0` = 不轮转 |
| `ROCKETMQ_CPP_LOG_FILE_MAX_INDEX` | `10` | 备份份数；`0` = 不保留 |

轮转语义是 **FixedWindow**：`<file>.N` 最旧先删，其余依次后移，最后 base → `.1`。

三点已知行为（都已显式记录在头文件里，不是 bug）：

1. 备份**不压缩**；
2. **同步写**（每行 `fflush`，`tail -f` 实时可见）；
3. 连接关闭记 **DEBUG**（正常 shutdown 也命中同一路径，在默认 INFO 下记 INFO/WARN
   会变成"退出时的假异常"噪声）。真正的协议异常（帧长非法、解码失败）仍按 **WARN** 记录。

**良性长轮询超时走 DEBUG**（默认被抑制），所以正常运行日志里 `ERROR=0` 是预期状态 ——
出现 ERROR 就是真问题。

> 📌 文件名刻意与其它语言端口区分开。各端口轮转策略不同，写同一文件会互相插行。

## License

Apache-2.0，与上游 RocketMQ 保持一致。
