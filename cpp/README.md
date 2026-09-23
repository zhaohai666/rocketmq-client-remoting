# rocketmq-client-remoting (C++)

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 C++17 实现，迁移自 Java 的
`org.apache.rocketmq.client` + `org.apache.rocketmq.remoting` + `org.apache.rocketmq.tools`，
并与本仓库的 Python 参考实现（`../python/`）逐项对齐。

**无第三方运行时依赖**（只用 POSIX socket + 标准库 + 系统压缩库：zlib 必需，liblz4 /
libzstd 可选，找不到就只关那一个后端），网络层手写而不是引 netty 类似物，
目的是把"字节到底长什么样"暴露出来、便于与 Java / Python 做逐字节互操作验证。

已实现范围：

| 层 | 内容 |
| --- | --- |
| 协议层 | JSON / RocketMQ 二进制两路序列化；`RemotingCommand` 帧编解码；CommandCustomHeader 家族（含 V2 短字段名）；17 段消息存储格式与 6 段批量格式 |
| 传输层 | `RemotingClient`：同步 / 异步 / oneway、半包重组、opaque 匹配、重连、**GO_AWAY(1500) 换连接重发一次**、SIGPIPE 处理 |
| 路由 / 心跳 | `TopicRouteData` / `QueueData` / `BrokerData`、`SubscriptionData`、`HeartbeatData` |
| 客户端 | `MQClientInstance`、`DefaultMQProducer`、`DefaultMQPushConsumer`、`DefaultMQPullConsumer`、`DefaultLitePullConsumer`、**`DefaultMQAdminExt`** |
| 异步发送 | `sendAsync`（含定点 `sendAsync(msg, mq, cb)`）跑在真实的 `AsyncSenderExecutor_1..N`（core==max==CPU 核数、有界队列 50000）上，调用方不阻塞；`retryTimesWhenSendAsyncFailed` 的换 broker 重试链只在 remoting 层失败时继续（已收到响应的错误码原样回调、不重试），重试**复用同一请求**只换 opaque，超时预算是整条链共享的剩余时间；用户回调与 `SendMessageHook.after` 在 `NettyClientPublicExecutor_N` 上跑，回调抛异常吞掉不带走 worker；`enableBackpressureForAsyncMode`（默认关，与 Java 同）打开后，异步发送在**调用方线程上、投入 `AsyncSenderExecutor` 之前**过两个**公平**信号量的闸（在途 1024 条 / 100M 字节，地板 10 条 / 1M 字节），等不到许可就回调 `send message tryAcquire semaphoreAsyncNum|Size timeout`（Java 原文案）、一次请求都不发出，许可在链终点按「先 size 后 num」归还，队满时开着背压改为就地跑 |
| 校验门 | `Validators` / `TopicValidator`：`checkTopic` / `checkGroup` / `isSystemTopic` / `isNotAllowedSendTopic` / `checkMessage`，四类 facade 的 `start()` 在建客户端实例**之前**跑完组名校验（纯本地判定，失败不碰网络） |
| 压缩 | zlib / LZ4 Frame / ZSTD 三后端：生产端自动压缩 + 消费端自动解压（与 Java lz4-java、zstd-jni 的线上帧格式互通；`-DRMQ_WITH_ZLIB=OFF` 等可逐个关） |

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
SSL 会话上交叠，而 OpenSSL 明确不支持两个线程同时用一个 `SSL` 对象。连接层原来那把
`writeMutex` 只锁写侧，挡不住读/写重叠，所以锁下沉到会话里，`read` / `writeAll` /
`pending` / `shutdown` 每次 `SSL_*` 调用取放一次（`writeAll` 的重试睡眠在锁外）。

本机 loopback + 真 nameServer 实测（`examples/live_tls.cpp` 的 S0）：一条 TLS 连接上
16 线程并发 320 笔，加锁那几趟 20~26ms、同一台机器不加锁那趟 17~19ms，两种都是 0 失败、
响应 opaque 逐笔对上；另加 30 轮"每轮新建 TLS 连接打首包"，0 丢。也就是说这把锁是
**用法正确性**的修复：几毫秒摊到 320 笔（每笔 10~20µs 量级），在本机量不出有意义的代价。

## 测试

```bash
cd build && ctest --output-on-failure     # 32 个用例，3023 项断言（31 个测试二进制 2950 + interop 73），~42s
```

| 用例 | 断言 | 覆盖 |
| --- | --- | --- |
| `codec` | 65 | JSON / ROCKETMQ / `RemotingCommand` 两路 / 消息 17 段与 6 段 / header V1↔V2 / hashCode / CRC32 / msgId |
| `java_alignment` | 32（带 `ROCKETMQ_JAVA_SRC` 为 38） | `codes.h` 常量守卫，设了环境变量后**读真实 Java 源码**逐条比对 |
| `route_heartbeat` | 99 | 路由类往返 + 按 perm 过滤队列；`SubscriptionData` / `HeartbeatData` 往返 + **Java 字段名守卫** |
| `transport` | 55 | 真实本机 TCP：同步/异步/oneway、**半包重组**、opaque 匹配、建连失败、超时、重连、**GO_AWAY 重发（同步+异步、只重发一次、开关关掉不重连）**、地址解析 |
| `compression` | 152 | 三后端：类型解析（含 Java 的 `0→ZLIB` 兼容映射）、zlib / **LZ4 Frame** / **ZSTD** 往返、**外部硬编码真值夹具**（Python zlib.compress、Java lz4-java、zstd-jni、`zstd -3` CLI 各产的帧）、帧头 magic 与 LZ4 block-independence 位、17 段报文 × 三种类型、解压后清 flag、后端缺失或未知类型必须抛错而非交出压缩流 |
| `admin` | 159 | fastjson2 非法 JSON 容错、`ConsumeStatsList` 的 **Java 字段名 `consumeStatsList`**（键名错一个字符就静默解析成空列表）、`TopicConfig` / `SubscriptionGroupConfig` 默认值与字段名、`TopicStatsTable` / `ConsumeStats` / `ResetOffsetBody`、properties 文本往返、`PermName::isValid` |
| `logging` | 36 | 行格式（毫秒 / pid / 线程名 / `文件:行号`）、主线程落 `main`、线程名 thread-local、按大小轮转与 `maxIndex` 上限、级别过滤、关闭文件输出后不写盘 |
| `acl` | 56 | ACL 签名算法（extFields 按 key 字典序、只拼 value、跳过 Signature，再拼 body）与 Java 官方实现对拍 |
| `request_reply` | 37 | 请求-响应模式的消息编解码、`reply_to` 属性、correlationId 匹配与超时 |
| `latency` | 31 | 故障规避：延迟窗口滑窗统计、可用性判定、broker 隔离与恢复、`sendLatencyFaultEnable` 开关 |
| `send_retry` | 68 | `sendDefaultImpl` 重试分类语义（进程内 mock 集群 + 真 socket）：可重试码换 broker、不可重试码立即抛、重试耗尽报 `BrokersSent`、单次超时钳位、预算耗尽报 callTimeout、无路由快速失败、连接失败隔离；同一套抓包还取证明线上报文（`k`=unitMode、`ReqT`、发送请求码 310/320/325 与 `m`=batch 的三级判据）；**发送头那三个跟着路由/配置走的字段**同样抓真报文取证（用例 8a2）：默认 `c`=`TBW102`/`d`=4、`setCreateTopicKey`/`setDefaultTopicQueueNums` 之后二键换成配置值（旧实现写死，setter 是假的），`n` 取**路由选中的** broker 名，定点发送与批量 320 走同一份建头代码、三键都不能漏 |
| `producer_async` | 209 | 真异步发送链（进程内 mock broker + 真 socket）：`sendAsync` 立刻返回、准备工作和请求都在 `AsyncSenderExecutor_N` 上、回调与 `SendMessageHook.after` 在 `NettyClientPublicExecutor_N` 上；出队后才算耗时（预算被排队吃光一次请求都不发）；只有 remoting 层失败才重试且重试**复用同一请求**只换 opaque；超时预算是整条链共享的剩余时间；已收到响应但 broker 报错码**不重试不包装**；定点发送只在同一台 broker 上重试、且必须自己刷出路由；队满把 `executor rejected` 抛给调用方；回调抛异常被吞掉。**异步发送背压**（用例 10~17）：开关默认关且关掉时**一格许可都不动**、两个配置的地板值、条数/字节闸在**调用方线程**上等到预算耗尽才回调（Java 文案逐字）、被拒的发送一次请求都没发出、许可在链终点按「先 size 后 num」归还（失败与重试路径同样归还）、运行时扩容叫醒卡在闸上的人、队满时开着背压改为就地跑；**批量异步**（用例 18~21，对位 Java `send(Collection, SendCallback, timeout):1121`）：一批只发**一个**请求、只交付**一个**回调，字节闸按**整批** body 长度扣（Python `_back_pressure_msg_len` 的 list 分支），未 `start()` 时与单条异步同一口径就地抛且**一个回调都不交付**，ID 顺序锁死 Java `batch():1172-1184`（先给每条子消息 `setUniqID` → 再给整批那条补一个 → **最后**才 `setBody(encode())`，所以抓到的报文里每条子消息各带一个不重复的 32 位 UNIQ_KEY、批量自身那个 ID 也在）；**异步链的发送头**（用例 22）：异步自己建头（`buildSendRequest`），所以同步侧对齐不代表它也对齐——抓真报文验 `n`=该请求落到的 broker 名、`c`/`d` 取生产者的 `setCreateTopicKey`/`setDefaultTopicQueueNums`（`AsyncTopicKey`/11） |
| `backpressure` | 50 | `FairSemaphore`（对应 Java `new Semaphore(permits, true)`）：只有**队首**能拿许可（后来者不许插队）、超时返回 false 而不抛、超时/获准后都要**把队首换人这件事广播出去**（漏了这一步，后到的等待者会睡到自己的超时 —— 两个用例专门盯这两条）、`release` 超过总量不校验、原地平移总量并保留在途份数（算出负的空闲许可也照 Java 的 `new Semaphore(负数)` 接受）、改容量能叫醒正堵在旧容量上的人（Java 换对象做不到这一步） |
| `pop` | 93 | POP 协议管道：CK 反构（8 段 + `startOffsetInfo`/`msgOffsetInfo` 下标选择）、`bornTime`、ACK offset 语义 |
| `pop_consumer` | 47 | POP 消费循环：`ackIndex` 默认值、不可见时间内的 ack 与复活重投、`checkNeedAckOrDelay` 边界钳制 |
| `consume_ack_index` | 28 | classic 并发消费的 `ackIndex` 切分（Java `processConsumeResult:207-269`）：默认 `Integer.MAX_VALUE` 整批认可不回投、listener 收窄到 0 时尾巴逐条 `sendMessageBack`、`RECONSUME_LATER` 强制 `ackIndex=-1` 整批回投、广播模式不回投、**回投失败时塞回队首且位点不越过它**（未 `start()` 的消费者回投必定失败，所以离线锁的是失败分支；「回投成功 → 位点整批前进」由 `rmq_live_redelivery` 的 S10 在真机上取证） |
| `trace` | 92 | 消息轨迹：与 Java 官方实现的**逐字节对拍**（Pub / SubBefore / SubAfter / EndTransaction / Recall）+ 编解码双向 + 无 keys 空段容错 + 坏记录隔离 + 分发器分组/切块 |
| `pull_expired` | 12 | 拉取循环停摆自愈（Java `ProcessQueue.PULL_MAX_IDLE_TIME` = **120000ms**，读 `rocketmq.client.pull.pullMaxIdleTime`；判据在 `RebalanceImpl.updateProcessQueueTableInRebalance:438-461`）：阈值逐字锁死、判据是**严格大于**（正好 120s 不算停摆）、没盖过章的新循环不算、循环线程已退出即刻算（不等满阈值）、健康队列一律不动（换线程等于丢在途重投）、撤走时持久化已消费位点并丢掉拉取游标与缓冲、分配即登记 `mqMap`（否则撤时无 mq 可持久化）、POP 分支读 `lastPopTimestamp` 且 `setDropped` + 换一具干净的 `PopProcessQueue`、停机期间不判停摆（否则刷一堆假 `[BUG]` 日志）、307 运行信息把 `lastPullTimestamp` 报成盖章节的真时刻、且 **`mqTable` 与 `mqPopTable` 互斥**（Java `DefaultMQPushConsumerImpl#consumerRunningInfo` 分别取 `processQueueTable` / `popProcessQueueTable`：弹出去的队列只出现在 popTable） |
| `hook` | 65 | `CheckForbiddenHook`（异常不吞、沿重试链传播）+ `FilterMessageHook`（可变 msgList、摘掉即静默跳过）+ 钩子异常隔离 |
| `consume_thread_pool` | 61 | 消费端有界 core/max 执行器：真实并发度 == corePoolSize、`setConsumeThreadNums` 生效、`updateCorePoolSize` 运行时调并发 |
| `top_addressing` | 37 | 动态 name server：WS 地址 / unitName / para 拼装、`clearNewLine`、非 200 与连接失败回退为空 |
| `consumer_stats` | 24 | `ConsumerStatsManager` 采样（sum/tps 窗口端点差分，不依赖真实时钟）+ `ConsumeStatus` / `ConsumerRunningInfo`(307) 编码 |
| `trace_context` | 24 | W3C `traceparent` 生成/校验/子 span/注入不覆盖/属性提取 |
| `interop` | 73 | C++ ↔ Python 双向编解码 + 路由/心跳结构体双向语义等价 |
| `lite_pull` | 31 | `DefaultLitePullConsumer` 无网络状态机（subscribe/assign/seek/poll/committed）+ 221/309 应答体 wire 形状 |
| `allocate_strategy` | 1167 | 六个策略与 Java 单测逐条对拍：`AVG` / `AVG_BY_CIRCLE` 的 Java 用例（10/4、7/3、边界队列续接）、四道 `check` 守卫的返回口径（本端口返回空结果而非 Java 的 `IllegalArgumentException`）、`CONFIG` 不查守卫且返回副本、`getName()` 与 Java 常量一致（六个名字齐全）、N 消费者恰好不重不漏覆盖每个队列、与 Python/Rust 参考实现的公式对拍、多 broker 真实队列、三类消费者（push / pull / lite）默认 AVG 且可替换，策略为 null 时 `start()` 用 Java `checkConfig` 的文案拒绝；**`CONSISTENT_HASH`** 用 Java 实测的哈希环表逐格对拍（6×2/6×3/10×4/20×10 与 vc=10 的 4×2/8×3、覆盖矩阵、注入自定义 `HashFunction` 后的 TreeMap 撞坑退化）、**`MACHINE_ROOM`** 的 `[0,1,4]/[2,3]` 分片与 `String#split("@")` 的 8 行裁尾空段真值表、**`MACHINE_ROOM_NEARBY-<内层>`** 的同机房优先 + 无消费者机房由全员共享（Java 单测的精确顺序）与 resolver 空机房**抛错**（保住上一轮分配） |
| `consistent_hash` | 42 | 一致性哈希环 + 自带 MD5：RFC 1321 附录 A 全向量（含 55/56/57/64/65/200 字节的填充边界）与 Java `hash()` 取前 4 字节大端的真值、环的路由稳定性与越过末尾回绕、空环返回 null、负虚拟节点数只在 `addNode` 抛、`i + existingReplicas` 的副本下标不重叠、`removeNode` 不误伤别的节点、注入自定义 hash 生效、`ringHashes()` 严格升序 |
| `validators` | 75 | 名字校验：字符表（码点 >=128 一律非法）与正则口径、12 个系统 topic / 8 个禁发 topic 名单（`TBW102` 可发、`%RETRY%` 可发）、`checkTopic`/`checkGroup` 的 blank→长度(127/120)→字符表顺序与文案逐字、`checkMessage` 只有 body 档位带 `MESSAGE_ILLEGAL(13)`、LMQ 分隔符、四类 facade 的 `start()` 组名门在建实例之前 |
| `broker_requests` | 11 | broker 反向请求 `NOTIFY_CONSUMER_IDS_CHANGED`(40)：注册在**实例级**的 clientRemotingProcessor（各消费者重复注册会互相覆盖）、计数 + 整组唤醒、`unregisterRebalanceWakeup` 后不再被叫醒但通知仍被处理、缺 `consumerGroup` 不抛、shutdown 清掉唤醒表 |
| `check_client_config` | 32 | broker 侧客户端配置校验 `CHECK_CLIENT_CONFIG`(46)：请求头为 **null**（线上没有 extFields）、body 是 `CheckClientRequestBody` 的 JSON、非 SUCCESS 抛 `MQClientException(响应码, remark)`；只发非 TAG 订阅（`null`/`""`/`TAG` 都算 TAG）、地址走**只读缓存**的 `findBrokerAddrByTopic` 且取不到就跳过、网络类异常换成固定文案、超时用 `mqClientApiTimeout`(3000ms) |
| `client_id` | 32 | clientId 口径与 Java `ClientConfig` 对拍：`buildMqClientId` 的 `ip@instanceName[@unitName]`（空白 unitName 不拼）、`changeInstanceNameToPID` 只改默认名且幂等、四类 facade `start()` 盖出的 `<ip>@<pid>#<nanoTime>`、同进程两个生产者不撞号、广播消费者保持 `DEFAULT`、显式 instanceName 原样透传 |
| `recall_message` | 28 | 定时消息撤回 `recallMessage`(370)：句柄编解码与 **Java `buildHandle` 真值向量**对拍（含无填充句柄、6 段新版本、v2/段数不足/非法 utf-8 全部按 Java 文案 `"recall handle is invalid"` 拒）、`RecallMessageRequestHeader` 逐键守卫（继承字段反射名是 **`bname`** 而不是 `brokerName`）、`SendMessageResponseHeader.recallHandle` 往返、producer 本地校验顺序（未 start / `%RETRY%` / `%DLQ%` / 非法句柄都在打网络**之前**秒回，路由拿不到时预热带异常照抛） |

```bash
# Java 对齐（断言数 32 -> 38）
ROCKETMQ_JAVA_SRC=/path/to/zhaohai666-rocketmq ./tests/rmq_test_java_alignment
```

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
./build/examples/rmq_live_request_reply 127.0.0.1:9876
./build/examples/rmq_live_latency       127.0.0.1:9876
./build/examples/rmq_live_pop           127.0.0.1:9876
./build/examples/rmq_live_pop_consumer  127.0.0.1:9876
./build/examples/rmq_live_redelivery    127.0.0.1:9876   # 重投/死信/重启/广播/顺序/流控/namespace/部分 ack/停摆自愈 十一段
./build/examples/rmq_live_trace         127.0.0.1:9876   # 需 broker traceTopicEnable=true
./build/examples/rmq_live_hook          127.0.0.1:9876
./build/examples/rmq_validators_live    127.0.0.1:9876
./build/examples/rmq_recall_live        127.0.0.1:9876   # 需 broker 开 recallMessageEnable（工具自己打开并还原）
./build/examples/rmq_live_unit_config   127.0.0.1:9876   # unitName/unitMode/stream
./build/examples/rmq_sql92_live         127.0.0.1:9876   # 需 broker 开 enablePropertyFilter=true
./build/examples/rmq_live_backpressure  127.0.0.1:9876   # 异步发送背压（两个公平信号量）
./build/examples/rmq_live_async_send    127.0.0.1:9876   # 异步发送内核（线程口径/并发/定点/拦截/批量/关池）
./build/examples/rmq_live_send_header   127.0.0.1:9876   # 发送头 c/d/n：自动建 topic 的队列数由模板与 d 决定
```

| 工具 | 结果 | 覆盖 |
| --- | --- | --- |
| `rmq_live_message_types` | 20/20 | 异步发送（`sendAsync` 不阻塞调用方 + 定点 `sendAsync(msg, mq)` 在全新实例上自己刷出路由）/ 顺序消息（同 key 同队列 + 保序）/ Tag 过滤 / 用户属性 / 延迟消息 / 按 Key 查询 / 事务消息 / 批量发送（320，投成 3 条独立消息且 offset 连续）/ 心跳注册 |
| `rmq_admin_live` | 57 PASS / 1 SKIP | 集群探活 → 建 topic → 路由/配置查询 → **broker 配置（properties 文本）读改写回** → NameServer KV → 订阅组（建/单查/分页/examine/删）→ 生产 → 各类统计与查询 → `viewMessage` → **`sendMessageBack` 重投到 `%RETRY%`** → `resetOffsetByTimestamp` → `resetOffsetByQueueId`（25 + 带 queueId/offset 的 222：重置后首笔 pull 被 broker 短路成 `PULL_OFFSET_MOVED`、第二笔才取到历史消息；越界目标被拒时位点停在第 1 笔写入的非法值 ⇒ 两笔 RPC 非原子，与 Java 同构）→ `queryTopicsByConsumer(group)`（按 `%RETRY%` 路由扇出合并）与 `queryTopicsByConsumerToBroker` → 清理 |
| `rmq_compression_live` | 10 PASS | 三后端（zlib / LZ4 Frame / ZSTD）自动压缩自产自销 + **与真实 Java/Python/.NET/Rust 客户端双向互通**（互通矩阵见 `../scripts/compression_matrix.sh`）；构建时未编入的后端打 SKIP |
| `rmq_live_trace` | 17 PASS / 0 FAIL | 消息轨迹全链路：`SendResult`（UNIQ_KEY / offsetMsgId / regionId / traceOn）→ Pub 轨迹 → 业务消费 → SubBefore/SubAfter 配对与 contextCode → 轨迹消息 keys 反查 → 防递归（轨迹 topic 自身不上报）→ `enable_trace=false` 不产生轨迹 → 编码段数 == 解码记录数 → 无 keys 消息的空段容错 |
| `rmq_validators_live` | 39 PASS / 0 FAIL | 名字校验真机对拍（与 Python/Rust/.NET 同场景）：S1 发送路径 13 项本地快拒（空白/超长/非法字符 topic、禁发的 broker 内部流水、body 三档 + `INNER_MULTI_DISPATCH` 分隔符，全部 <50ms 且不碰网络）、S2 批量逐条校验 + 同质性、S3 生产者 `start()` 三道组名门 + 120 等长边界放行、S4 正腿（合法名字建 topic → push/lite 两路各收 3 条）、S5 对照腿（合法但不存在的 topic 不被误伤，真往返 46ms vs 本地 0.17ms）、S6 pull/lite 组名门 + 合法 pull 组查队列与位点、S7 `createTopic` 挡空白/非法/系统 topic |
| `rmq_live_hook` | 13 PASS / 0 FAIL | `CheckForbiddenHook`（放行 / 每次发送尝试都回调 / 单向也拦截 / 被拦截的消息确实没落 broker）+ `FilterMessageHook`（拉取路径 3 收 2 丢且不重投、POP 路径 2 收 1 丢且**摘掉即 ack**）+ 客户端二次 tag 过滤（订阅 `TagA` 只收 `TagA`）+ 钩子异常被吞掉不影响后续钩子 |
| `rmq_live_lite_pull` | 33 PASS / 0 FAIL | `DefaultLitePullConsumer` 真机全链路：S1 后台 rebalance 拿到 4 个队列 → S2 subscribe+poll 收全 12 条且内容一致 → S3 `commit` 后各队列位点 >0 → S4 assign+seek 从头重收 → S5 订阅级 tag 只收 6 条 → S6a `CONSUME_FROM_TIMESTAMP`（墙钟起点早于全部消息 → 收全）、S6b `offset_for_timestamp` 双向（30 分钟前 → 队首 Σ=0，10 分钟后 → Σ=12）→ S7a 默认策略名 `AVG` 且策略为 null 时 `start()` 报 Java 同款文案、S7b 换 `AVG_BY_CIRCLE` 后**同组两实例**分配无交集、并集覆盖 4 队列、下标步长 2（交叉而非连续段）、S7c 两半 `CONFIG` 各自只收到配置队列里的消息且合起来恰好 12 条互不重叠、S7d `CONSISTENT_HASH` 用**真实 clientId** 建环且线上分配收敛到「真实 mqAll/cidAll 离线跑同一策略」的预测（合起来收全 12 条）、S7e `MACHINE_ROOM_NEARBY-CONSISTENT_HASH` 在单机房下**原样透传**内层策略 + resolver 被逐个队列/两个真实 clientId 问过、S7f `MACHINE_ROOM` 白名单不匹配真实 `broker-a` → 安静饿死（分不到队列、poll 不到消息、同组 AVG 对照组仍只拿自己那半边）|

| `rmq_recall_live` | 14 PASS / 0 FAIL | 定时消息撤回 `recallMessage`(370) 真机（与 Python/Rust/.NET 同场景）：R0 读得到 broker 的 `recallMessageEnable` 并临时打开 → R1 只有带 `TIMER_DELAY_SEC` 的消息回 `recallHandle`，普通消息没有 → R2 broker 给的句柄能被本端口解码器解开，`topic`/`brokerName`/`uniqKey` 与发送结果逐字段一致 → R4 `%RETRY%` topic 本地用 Java 文案拒掉、R5 非法句柄 <200ms 秒回（没打网络）→ R3 撤回返回被撤回消息的 uniqKey → **R6 语义**：同样延迟的对照消息按时投递、被撤回的那条整个窗口都不出现 → R7 无条件把 `recallMessageEnable` 还原成跑之前的值 |
| `rmq_live_unit_config` | 20 PASS / 0 FAIL | `unitName`/`unitMode`/`stream` 真机（与 Python/Rust/.NET 同场景）：U1 `unitName` 拼进 clientId 且照常发送 → U2 `@unitA@STREAM` 的消费者收到消息，**broker 的 `examineConsumerConnectionInfo` 回读到同一串 clientId** → U3 `unitMode=true` 自动建出的 topic 带 `UNIT` 位、对照组不带 → U4 心跳的 `ConsumerData.unitMode` 让 `%RETRY%` 带 `UNIT_SUB` 位 → U5 lite 消费者与显式开 stream 的生产者都带 `@STREAM`，3 发 3 收。钩子顺序（`ReqT` 必须在 ACL 签名之内）与"钩子真的写到 socket 上"由离线用例 `testRequestHooksReachWire`（`tests/test_send_retry.cpp`，抓真报文 + broker 侧复算 HMAC）和 `tests/test_acl.cpp` 锁死 |

| `rmq_sql92_live` | 20 PASS / 0 FAIL | SQL92 过滤 + `CHECK_CLIENT_CONFIG`(46) 真机（与 Python/Rust/.NET 同场景）：S1 SQL92 订阅启动时正好一笔 46、body 的 `clientId`/`group`/`subscriptionData` 逐字段对得上，纯 TAG 订阅一笔都不发（Java `ExpressionType.isTagType` 短路）→ S2 消费者**先起来再发** 6 条，`color='red'` 只收那 3 条 red、blue 一条没漏进来（broker 真在按属性过滤，而不是拿不到编译过滤数据就放行全部），`'*'` 对照组收全 6 条 → S3 永不匹配的 `color='green'` 收 0 条 → S4 语法错的表达式让 `start()` 秒回 broker 的 `SUBSCRIPTION_PARSE_FAILED(23)` 并就地回滚（换个合法表达式能重新 `start()`）。协议形状与四条分支语义另有离线用例 `tests/test_check_client_config.cpp`（ctest `check_client_config`，9 项 / 32 断言，进程内 mock broker 抓真报文） |

| `rmq_live_backpressure` | 25 PASS / 0 FAIL | 异步发送背压真机（与 Python `verify_backpressure_live.py` 的 B1–B5 同场景）：B1 默认容量（1024 条 / 100M 字节）开着背压发 40 笔异步全部 `SEND_OK`、broker 上正好落 40 条、两个信号量**满额归还**（真机不泄配额）→ B2 条数闸夹到地板值 10、`sendMessageBefore` 钩子睡 600ms 占住在途，第 11、12 笔在**调用方线程**上等满 150ms 预算才回调 `send message tryAcquire semaphoreAsyncNum timeout`（Java 原文案），且**闸等到预算耗尽才报错**（实测等了 160ms）、被拒的两笔在 broker 上一条没留（连路由都没查）→ B3 运行时把容量从 10 调到 12：正卡在闸上的调用方被叫醒并发了出去（等的是回调而不是线程退出）、全部归还后空闲许可 == 新容量 12、broker 总数 == 两轮在途 + 被叫醒的那一笔 = 21 → B4 字节闸（1M 地板 + 600KB body）：在途时空闲字节正好是 `1M - 600K`（434176），第二笔回调 `...semaphoreAsyncSize timeout`，且**字节闸没过时条数许可已归还**（先 size 后 num），broker 只落 1 条 → B5 关掉背压后同样的容量配置完全不限流：30 笔并发（含每三笔一笔 300KB）全部落地。「被拒的发送连请求都没发出去」只看 broker 侧各队列 `maxOffset-minOffset` 之和，光看客户端回调会被「回调报错但请求照样发出去」的实现蒙过去；新 topic 注册到 namesrv 是秒级的，所以 broker 侧对账一律轮询到超时。 |

| `rmq_live_async_send` | 43 PASS / 0 FAIL | 异步发送内核真机（与 Python `verify_async_send_live.py`、.NET `async-send`、Rust `live_async_send` 的 A1–A6 同场景）：A1 `sendAsync` 在准备段（`sendMessageBefore` 睡 400ms）之前就返回、回调恰好一次且 `SEND_OK`，**线程口径**实测 `AsyncSenderExecutor_1` 跑 before 钩子、`NettyClientPublicExecutor_1` 跑用户回调（Java `executeInvokeCallback`），用 broker 回的 `offsetMsgId` 能 `viewMessage` 读回原 body、`queueOffset == 该队列 maxOffset-1`、`msgId` 是 32 位客户端 UNIQ_KEY 且与 `offsetMsgId` 不同 → A2 并发 30 笔各**恰好一个**终态、全 `SEND_OK`、broker 落 30 条、30 个 `(broker,queueId,queueOffset)` 槽位与 30 个 UNIQ_KEY 两两不重复 → A3 定点异步发送只让指定的那条队列多 1 条、其它队列一条没多 → A4 `CheckForbiddenHook` 看到 `ASYNC`，拒绝时异常原样到回调且**连 topic 路由都没建出来**（`landed=-1`），换个标签照常落地、钩子被调 2 次 → A5 `sendBatchAsync`（对位 Java `send(Collection, SendCallback, timeout)`）一批 5 条：回调恰好一次且 `SEND_OK`、broker 侧 `landed=5`、应答的 `offsetMsgId` 是**逐条回的 5 个 commitLog 偏移**，用它 `viewMessage` 读回的那条**子消息**带着客户端生成的 32 位 `UNIQ_KEY`（逐条 ID 编在 body 里的落地证据，缺失时发送侧照样 `SEND_OK`，只有真 broker 看得出来）、`msgId` 是批量自身的 32 位客户端 ID 而非 `offsetMsgId`；定点批量只让那条队列多 3 条、其它队列一条没多；混 topic / 空批的本地校验在异步路径上照样跑且错误**进回调**；字节闸按**整批**扣（1 MiB 地板下 2×600 KiB 被拒、2×100 KiB 照常 `SEND_OK`、两份许可满额归还）。请求码 320 由离线单测取证→ A6 `shutdown()` 用 `shutdown(true)` **join 完池子才关客户端**，36 笔全部上线（`landed=36`）——这是本端口相对 Java 的**刻意偏离**（Java `:314` 只 `shutdown()` 不 `awaitTermination`，Python/Rust 照抄那一派，同一用例 36 笔全报错、一条都没落）。 |

| `rmq_live_redelivery` | 42 PASS / 0 FAIL | 消费侧十一段真机（与 Python/Rust/.NET 同场景）：S1 `RECONSUME_LATER` 走 `sendMessageBack`(code 3) 重投，实测延迟梯度 ≥8s、重投来自 `%RETRY%` 且 `reconsumeTimes` 递增、正常消息只投一次 → S2 重启后接着消费且不重复 → S3 顺序消费 → S4 广播两组各收全 → S5 慢消费下 10 条全到 + 流控触发计数 >0 → S6 同组两实例队列不重不漏 + 40 条无重复 + 收到 broker 的 `NOTIFY_CONSUMER_IDS_CHANGED`(40) → S7 `shutdown()` 真的注销了 clientId（`queryConsumerIdList` 前后对照）→ S8 `namespace` 正腿/反腿（带 ns 收全、裸 topic 消费者收不到，证明真实 topic 是 `NS%topic`）→ **S9 死信终态**：`maxReconsumeTimes=2` 只投 3 次（实测 `0s/10s/40s`，即 Java `delayLevel = 3 + reconsumeTimes`，`AbstractSendMessageProcessor:209`），第 3 次回投被 broker 改写进 `%DLQ%<group>`（`:193`，路由此刻才建出来），lite pull 从队首读回的那条 `reconsumeTimes=3`（存储时 +1，`:228`）、`RETRY_TOPIC` 仍是业务 topic、之后不再投递。S9 的窗口给 150s：整机并发时定时服务会拖档，100s 会假失败。→ **S10 部分 ack（`ackIndex`）**：`CONSUME_SUCCESS` + 一批 3 条里 listener 只认可第 1 条 ⇒ 尾巴 2 条从 `%RETRY%` 回来（`reconsumeTimes>=1`、listener 看到的是业务 topic）、已认可那条整个窗口只投一次（没把前缀也回投）、3 条最终全部消费、业务队列位点仍整批前进到 3；对照组（完全不碰 `ackIndex`，Java 默认 `Integer.MAX_VALUE`）一条都不回投、位点同样到 3。topic 只建 1 个队列、并且**先发消息再起消费者**（新组显式 `CONSUME_FROM_FIRST_OFFSET`），批次切分才由不得拉取时机决定。所有断言的观察都用有界轮询（`waitUntil`）而不是固定 `sleep`，listener 的缓冲区由**跟着缓冲区走的互斥量**（`BodySink`）保护——原来「每个 listener 一把锁 + 主线程不持锁读」是数据竞争，会在高负载下漏读/误报重复。→ **S11 拉取循环停摆自愈**（Java `isPullExpired` / `PULL_MAX_IDLE_TIME`=120s，`RebalanceImpl:438-461` 的 `[BUG]` 分支）：1 队列 topic 先发 3 条并确认位点到 3，然后把这一路的拉取时刻**倒拨 125s** 并就地确认 `pullStalled()` 为真、307 应答里能读到被倒拨的那个值（`"lastPullTimestamp":<injected>`），再走一次真 rebalance（`syncPullThreads()`）⇒ 判据必须把它撤掉重建（日志里就是 Java 那句 `[BUG]doRebalance ... because pull is pause, so try to fixed it`），时刻回到"现在"、判据不再报警，之后同一队列继续消费到 6 条、位点到 6、6 条各只投一次且 `redelivered=0`（撤走前把已消费位点持久化回了 broker）。这条路径坏掉是**静默的**：不报错、心跳照发、别的队列照常推进，只有"这一路位点永远不动"，所以停摆→恢复的闭环必须真机取证，阈值与判据边界由离线用例 `pull_expired` 锁死。 |

| `rmq_live_send_header` | 14 PASS / 0 FAIL | 发送头 `c`/`d`/`n` 三字段真机（与 Python `verify_send_header_live.py`、Rust `live_send_header`、.NET `send-header` 的 H0~H5 一一对应）：H0 先量出 `TBW102` 的 read/write 队列数（本机 8/8）当算术基准 → H1 什么都不配、发到全新 topic，broker 按 `min(d=4, TBW102.writeQueueNums)` 建出 **4** 条队列（`TopicConfigManager.java:289`）→ H2 `setDefaultTopicQueueNums(2)` 真的让 broker 只建 **2** 条（修之前写死 4，这条必然红）→ H3 `setCreateTopicKey` 指向带 `PERM_INHERIT` 的 3 队列模板 topic 时，新 topic 继承**模板**的 **3** 条而不是 TBW102 的 8 条 → H4 补上三字段后五种入口（同步 / 定点 / 单向 / 批量 320 / 异步）逐条落地、7 条一条不差 → H5 落点 broker 名与路由选中那台一致。⚠ `n` 在经典 broker 的发送链路里**没有读者**（5.5.1 源码 grep 过），它上线的存在由离线抓帧用例（`test_send_retry` 的 8a2 + `producer_async` 的用例 22）取证，这里不假装能观测到 |

SKIP 项与原因会在输出里写清楚（例如 uniqKey 查询需要 broker 开 RocksDB 索引，
本机默认文件索引查不到属 **broker 配置差异，不是客户端 bug**）。

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
│   │   ├── recall_message_handle.h 定时消息撤回句柄 v1 编解码（Java `RecallMessageHandle`）
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
├── src/                        与 include 同构的 42 个 .cpp
├── examples/                   selfcheck / interop_tool + 20 个真机联调工具
└── tests/                      31 个测试源文件、32 个 ctest 用例（含 Java 对拍与 interop_check.py）
```

## 几个必须知道的实现约定

**字段名一律以 Java 为准。** broker 用 fastjson2 按 Java 属性名反序列化，
字段名错一个就**静默丢字段**（不报错）。差异清单见技能文档
`~/.workbuddy/skills/rocketmq-cpp-build-verify/SKILL.md`。

**fastjson2 会产出非法 JSON。** map 的对象 key 会被内联
（`{"offsetTable":{{"brokerName":"b",...}:{...}}}`）、数字 key 不加引号、允许
NaN/Infinity 与尾逗号。所以 `json.cpp` 里是**容错解析器**而不是严格 JSON parser。
改它的时候务必保留这套宽容逻辑，否则所有管理端响应体全崩。

**`RemotingCommand.body` 的存在判定是 `hasBody || !body.empty()`**（对齐 Java `body != null`），
只赋 `body` 不设 `hasBody` 也要能编码出 body。

**opaque 不能用 0 当"未设置"哨兵。** `opaqueCounter` 从 0 起算，第一个请求的 opaque 合法值就是 0；
传输层只在**与在途请求真的冲突**时才重分配，否则响应永远匹配不上。

**`TopicPublishInfo` 不可拷贝**，必须用 `std::shared_ptr` 取用 —— 它的队列轮询游标是跨调用
共享状态（Java 用 ThreadLocal），按值返回会让每次发送都从 0 号队列重来。

**压缩失败 / 未知算法必须响亮。** `CompressorFactory::decompress` 对未支持类型抛异常，
`decodeMessage` 捕获后返回 `false`（消息被丢弃）。**绝不能原样透传压缩字节**：
外层会清掉 `COMPRESSED_FLAG`，透传等于把压缩流当正文交出去且事后无法识别，属于静默数据损坏。

**clientId 口径按 Java。** `buildClientId(instanceName, unitName, enableStream)` =
`<本机 IP>@<instanceName>[@<unitName>][@STREAM]`（`buildMqClientId`，对应
`ClientConfig#buildMQClientId`；空白 unitName 不拼段，`@STREAM` 取枚举名而非 code）；
instanceName 还是默认值 `DEFAULT` 时由各 facade 的 `start()` 调 `changeInstanceNameToPID`
**就地**换成 `<pid>#<nanoTime>` —— 生产者与 admin 无条件，三个消费者只在 `CLUSTERING` 下
（广播消费者保持 `DEFAULT`，与 Java 一致 —— Java 的 `MQClientManager` 会让同进程的广播消费者
复用同一份实例，本端口是每门面各建一份私有实例）。与 Java 一点不同：本机 IP 用 UDP sockname
探测（Java 枚举网卡）。回归：`tests/test_client_id.cpp`（ctest `client_id`）。

**unitMode / stream 是上线字段，不是本地摆设。** 三类开关各有落点：
`unitName` 进 clientId 与动态取址 URL（`-<unitName>?nofix=1`）；
`unitMode` 进 `SEND_MESSAGE_V2` 的单字母键 `k`、心跳 `ConsumerData.unitMode`、
回投请求头与过滤/禁行钩子上下文；`enableStreamRequestType` 让每笔请求带 `ReqT=0`
（值是 `RequestType.STREAM` 的 **code**，与 clientId 的枚举名后缀不同）。
生产者默认关 stream（Java 只有 pull / lite 消费者在构造里置 `true`）。
**顺序是语义**：Java `MQClientAPIImpl:329-332` 先装 Stream 钩子再装用户（ACL）钩子，
`ReqT` 必须落在签名内容**之内**，否则开鉴权的 broker 验签必失败。本端口的传输层只有
一槽 RPCHook（Java 是列表，first-wins），所以 facade 一律经 `composeRequestHooks()`
合成后再 `registerRPCHook()`，且绑在 `MQClientInstance::start()` **之前** ——
Java 的 rpcHook 是随 `MQClientAPIImpl` 构造进去的，实例第一笔报文就该带着它。
回归：`tests/test_acl.cpp`（钩子组合与签名内容）+ `tests/test_send_retry.cpp`
（真 socket 上取证的 `k` / `ReqT`，并用 broker 侧算法重放验签）+
`examples/live_unit_config.cpp`（真 broker 的 topic `sysFlag` UNIT=0x1 / UNIT_SUB=0x2、
broker 记录的 clientId）。

**批量发送的请求码是 320，不是 310。** `sendRequestCode()`（`src/client/mq_client.cpp`）
按 Java `MQClientAPIImpl:550-563` 的三级判据走：先 `isReplyMessage` ⇒ 325，再 `msg.isBatch`
⇒ `SEND_BATCH_MESSAGE(320)`，否则 `SEND_MESSAGE_V2(310)`。注意请求码与 V2 头的单字母键 `m`
（batch）是两件事：broker 按 `m` 选 `sendBatchMessage` 还是单条写入
（`SendMessageProcessor:117` 读 `requestHeader.isBatch()`），码只影响服务端按码归类
（proxy `AbstractRemotingActivity:69` 与 auth `DefaultAuthorizationContextBuilder:230-240`
把 310/320 列在同一个 case 里）—— 所以对齐 320 不是修 bug，是请求码这一层也与 Java 一致，
两个字段必须成对取证。回归：`tests/test_send_retry.cpp`（`sendRequestCodeFollowsJava`，
真 socket 上取 `code` + `m`）+ `examples/live_message_types.cpp` 第 8 项（真 broker 把批量
投成 3 条独立消息、`queueOffset` 连续 0,1,2）。

**消息轨迹的解码器比 Java 更健壮。** Java `TraceDataEncoder` 对无 keys 消息的 `SubBefore`
会 `line[7]` 越界抛 `ArrayIndexOutOfBoundsException`（**5.5.1 上游真实缺陷**，已复现），
本项目缺段按空串取；并且**单条记录**解码失败只跳过自己 —— Java 是一条坏记录直接毁掉
整条轨迹消息的解码（表现为控制台整批轨迹消失）。轨迹文本按 `\x01` 分段、`\x02` 结尾，
切分必须用 **Java `String.split` 语义（丢弃末尾空串）**，原生 split 会多出一段。
故障排查提示：轨迹 topic 默认 `RMQ_SYS_TRACE_TOPIC`，broker 需 `traceTopicEnable=true` 才预建。

## 日志

`include/rocketmq/common/logging.h` 是 header-only 日志，默认级别 **INFO**，
同时输出到 stderr 与 `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log`。

行格式对齐 Java logback 的 `%d{...SSS} %-5p [%pid] [%t] [%logger#%M:%L] - %m`：

```
2026-09-14 17:06:14.566 INFO  [57308] [main] [producer.cpp:110] - DefaultMQProducer[...] started, clientId=...
2026-09-14 17:07:09.086 INFO  [57316] [ConsumeMessageThread_0] [consumer.cpp:161] - DefaultMQPushConsumer[...] started
```

线程名：主线程落 `main`（对齐 Java），工作线程由内部命名 ——
`ConsumeMessageThread_N` / `AsyncSenderThread_N` / `RemotingClientReader-<ip:port>`。
后两者只在异常路径留痕（连接关闭、非法帧长、解码失败），所以正常日志里通常只看到前两者。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CPP_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` / `OFF` |
| `ROCKETMQ_CPP_LOG_FILE` | `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log` | 设为空串/`OFF`/`NONE` 则只留 stderr |
| `ROCKETMQ_CPP_LOG_FILE_MAX_SIZE` | `67108864`（64MB） | 单文件上限，对齐 Java logback 的 `<maxFileSize>64MB</maxFileSize>`；`0` = 不轮转 |
| `ROCKETMQ_CPP_LOG_FILE_MAX_INDEX` | `10` | 备份份数，对齐 Java `rocketmq.log.file.maxIndex`；`0` = 不保留 |

轮转语义是 **FixedWindow**：`<file>.N` 最旧先删，其余依次后移，最后 base → `.1`。

与 Java 的三点已知差异（都已显式记录在头文件里，不是 bug）：

1. 备份**不压缩**（Java 会 gzip 到 `other_days/rocketmq_client-%i.log.gz`）；
2. **同步写**（Java 走 AsyncAppender），但每行 `fflush`，`tail -f` 实时可见；
3. 连接关闭记 **DEBUG**（Java 走 Netty `channelInactive` 记 INFO/WARN，但正常 shutdown 也命中同一路径，
   在默认 INFO 下会变成"退出时的假异常"噪声）。真正的协议异常（帧长非法、解码失败）仍按 **WARN** 记录。

**良性长轮询超时走 DEBUG**（默认被抑制），所以正常运行日志里 `ERROR=0` 是预期状态 ——
出现 ERROR 就是真问题。

> 📌 文件名刻意与 Java 客户端的 `rocketmq_client.log` 区分（Python 侧同理叫 `rocketmq_py_client.log`）。
> 三者轮转策略不同，写同一文件会互相插行；更糟的是按天滚动的实现会在午夜把文件**改名**，
> 而 JVM 仍持有旧 fd，后续日志会写进已 unlink 的 inode 而静默消失。

## License

Apache-2.0，与上游 RocketMQ 保持一致。
