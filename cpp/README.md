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

## 测试

```bash
cd build && ctest --output-on-failure     # 27 个用例，2602 项断言，~16s
```

| 用例 | 断言 | 覆盖 |
| --- | --- | --- |
| `codec` | 65 | JSON / ROCKETMQ / `RemotingCommand` 两路 / 消息 17 段与 6 段 / header V1↔V2 / hashCode / CRC32 / msgId |
| `java_alignment` | 32（带 `ROCKETMQ_JAVA_SRC` 为 38） | `codes.h` 常量守卫，设了环境变量后**读真实 Java 源码**逐条比对 |
| `route_heartbeat` | 95 | 路由类往返 + 按 perm 过滤队列；`SubscriptionData` / `HeartbeatData` 往返 + **Java 字段名守卫** |
| `transport` | 53 | 真实本机 TCP：同步/异步/oneway、**半包重组**、opaque 匹配、建连失败、超时、重连、**GO_AWAY 重发（同步+异步、只重发一次、开关关掉不重连）**、地址解析 |
| `compression` | 152 | 三后端：类型解析（含 Java 的 `0→ZLIB` 兼容映射）、zlib / **LZ4 Frame** / **ZSTD** 往返、**外部硬编码真值夹具**（Python zlib.compress、Java lz4-java、zstd-jni、`zstd -3` CLI 各产的帧）、帧头 magic 与 LZ4 block-independence 位、17 段报文 × 三种类型、解压后清 flag、后端缺失或未知类型必须抛错而非交出压缩流 |
| `admin` | 159 | fastjson2 非法 JSON 容错、`ConsumeStatsList` 的 **Java 字段名 `consumeStatsList`**（键名错一个字符就静默解析成空列表）、`TopicConfig` / `SubscriptionGroupConfig` 默认值与字段名、`TopicStatsTable` / `ConsumeStats` / `ResetOffsetBody`、properties 文本往返、`PermName::isValid` |
| `logging` | 36 | 行格式（毫秒 / pid / 线程名 / `文件:行号`）、主线程落 `main`、线程名 thread-local、按大小轮转与 `maxIndex` 上限、级别过滤、关闭文件输出后不写盘 |
| `acl` | 37 | ACL 签名算法（extFields 按 key 字典序、只拼 value、跳过 Signature，再拼 body）与 Java 官方实现对拍 |
| `request_reply` | 37 | 请求-响应模式的消息编解码、`reply_to` 属性、correlationId 匹配与超时 |
| `latency` | 31 | 故障规避：延迟窗口滑窗统计、可用性判定、broker 隔离与恢复、`sendLatencyFaultEnable` 开关 |
| `send_retry` | 35 | `sendDefaultImpl` 重试分类语义（进程内 mock 集群 + 真 socket）：可重试码换 broker、不可重试码立即抛、重试耗尽报 `BrokersSent`、单次超时钳位、预算耗尽报 callTimeout、无路由快速失败、连接失败隔离 |
| `pop` | 93 | POP 协议管道：CK 反构（8 段 + `startOffsetInfo`/`msgOffsetInfo` 下标选择）、`bornTime`、ACK offset 语义 |
| `pop_consumer` | 47 | POP 消费循环：`ackIndex` 默认值、不可见时间内的 ack 与复活重投、`checkNeedAckOrDelay` 边界钳制 |
| `trace` | 92 | 消息轨迹：与 Java 官方实现的**逐字节对拍**（Pub / SubBefore / SubAfter / EndTransaction / Recall）+ 编解码双向 + 无 keys 空段容错 + 坏记录隔离 + 分发器分组/切块 |
| `hook` | 65 | `CheckForbiddenHook`（异常不吞、沿重试链传播）+ `FilterMessageHook`（可变 msgList、摘掉即静默跳过）+ 钩子异常隔离 |
| `consume_thread_pool` | 61 | 消费端有界 core/max 执行器：真实并发度 == corePoolSize、`setConsumeThreadNums` 生效、`updateCorePoolSize` 运行时调并发 |
| `top_addressing` | 23 | 动态 name server：WS 地址 / unitName / para 拼装、`clearNewLine`、非 200 与连接失败回退为空 |
| `consumer_stats` | 24 | `ConsumerStatsManager` 采样（sum/tps 窗口端点差分，不依赖真实时钟）+ `ConsumeStatus` / `ConsumerRunningInfo`(307) 编码 |
| `trace_context` | 24 | W3C `traceparent` 生成/校验/子 span/注入不覆盖/属性提取 |
| `interop` | 70 | C++ ↔ Python 双向编解码 + 路由/心跳结构体双向语义等价 |
| `lite_pull` | 31 | `DefaultLitePullConsumer` 无网络状态机（subscribe/assign/seek/poll/committed）+ 221/309 应答体 wire 形状 |
| `allocate_strategy` | 1167 | 六个策略与 Java 单测逐条对拍：`AVG` / `AVG_BY_CIRCLE` 的 Java 用例（10/4、7/3、边界队列续接）、四道 `check` 守卫的返回口径（本端口返回空结果而非 Java 的 `IllegalArgumentException`）、`CONFIG` 不查守卫且返回副本、`getName()` 与 Java 常量一致（六个名字齐全）、N 消费者恰好不重不漏覆盖每个队列、与 Python/Rust 参考实现的公式对拍、多 broker 真实队列、三类消费者（push / pull / lite）默认 AVG 且可替换，策略为 null 时 `start()` 用 Java `checkConfig` 的文案拒绝；**`CONSISTENT_HASH`** 用 Java 实测的哈希环表逐格对拍（6×2/6×3/10×4/20×10 与 vc=10 的 4×2/8×3、覆盖矩阵、注入自定义 `HashFunction` 后的 TreeMap 撞坑退化）、**`MACHINE_ROOM`** 的 `[0,1,4]/[2,3]` 分片与 `String#split("@")` 的 8 行裁尾空段真值表、**`MACHINE_ROOM_NEARBY-<内层>`** 的同机房优先 + 无消费者机房由全员共享（Java 单测的精确顺序）与 resolver 空机房**抛错**（保住上一轮分配） |
| `consistent_hash` | 42 | 一致性哈希环 + 自带 MD5：RFC 1321 附录 A 全向量（含 55/56/57/64/65/200 字节的填充边界）与 Java `hash()` 取前 4 字节大端的真值、环的路由稳定性与越过末尾回绕、空环返回 null、负虚拟节点数只在 `addNode` 抛、`i + existingReplicas` 的副本下标不重叠、`removeNode` 不误伤别的节点、注入自定义 hash 生效、`ringHashes()` 严格升序 |
| `validators` | 75 | 名字校验：字符表（码点 >=128 一律非法）与正则口径、12 个系统 topic / 8 个禁发 topic 名单（`TBW102` 可发、`%RETRY%` 可发）、`checkTopic`/`checkGroup` 的 blank→长度(127/120)→字符表顺序与文案逐字、`checkMessage` 只有 body 档位带 `MESSAGE_ILLEGAL(13)`、LMQ 分隔符、四类 facade 的 `start()` 组名门在建实例之前 |
| `broker_requests` | 11 | broker 反向请求 `NOTIFY_CONSUMER_IDS_CHANGED`(40)：注册在**实例级**的 clientRemotingProcessor（各消费者重复注册会互相覆盖）、计数 + 整组唤醒、`unregisterRebalanceWakeup` 后不再被叫醒但通知仍被处理、缺 `consumerGroup` 不抛、shutdown 清掉唤醒表 |
| `client_id` | 17 | clientId 口径与 Java `ClientConfig` 对拍：`buildMqClientId` 的 `ip@instanceName[@unitName]`（空白 unitName 不拼）、`changeInstanceNameToPID` 只改默认名且幂等、四类 facade `start()` 盖出的 `<ip>@<pid>#<nanoTime>`、同进程两个生产者不撞号、广播消费者保持 `DEFAULT`、显式 instanceName 原样透传 |
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
./build/examples/rmq_live_trace         127.0.0.1:9876   # 需 broker traceTopicEnable=true
./build/examples/rmq_live_hook          127.0.0.1:9876
./build/examples/rmq_validators_live    127.0.0.1:9876
./build/examples/rmq_recall_live        127.0.0.1:9876   # 需 broker 开 recallMessageEnable（工具自己打开并还原）
```

| 工具 | 结果 | 覆盖 |
| --- | --- | --- |
| `rmq_live_message_types` | 12/12 | 异步发送 / 顺序消息（同 key 同队列 + 保序）/ Tag 过滤 / 用户属性 / 延迟消息 / 按 Key 查询 / 事务消息 / 心跳注册 |
| `rmq_admin_live` | 47 PASS / 1 SKIP | 集群探活 → 建 topic → 路由/配置查询 → **broker 配置（properties 文本）读改写回** → NameServer KV → 订阅组（建/单查/分页/examine/删）→ 生产 → 各类统计与查询 → `viewMessage` → **`sendMessageBack` 重投到 `%RETRY%`** → `resetOffsetByTimestamp` → 清理 |
| `rmq_compression_live` | 10 PASS | 三后端（zlib / LZ4 Frame / ZSTD）自动压缩自产自销 + **与真实 Java/Python/.NET/Rust 客户端双向互通**（互通矩阵见 `../scripts/compression_matrix.sh`）；构建时未编入的后端打 SKIP |
| `rmq_live_trace` | 17 PASS / 0 FAIL | 消息轨迹全链路：`SendResult`（UNIQ_KEY / offsetMsgId / regionId / traceOn）→ Pub 轨迹 → 业务消费 → SubBefore/SubAfter 配对与 contextCode → 轨迹消息 keys 反查 → 防递归（轨迹 topic 自身不上报）→ `enable_trace=false` 不产生轨迹 → 编码段数 == 解码记录数 → 无 keys 消息的空段容错 |
| `rmq_validators_live` | 39 PASS / 0 FAIL | 名字校验真机对拍（与 Python/Rust/.NET 同场景）：S1 发送路径 13 项本地快拒（空白/超长/非法字符 topic、禁发的 broker 内部流水、body 三档 + `INNER_MULTI_DISPATCH` 分隔符，全部 <50ms 且不碰网络）、S2 批量逐条校验 + 同质性、S3 生产者 `start()` 三道组名门 + 120 等长边界放行、S4 正腿（合法名字建 topic → push/lite 两路各收 3 条）、S5 对照腿（合法但不存在的 topic 不被误伤，真往返 46ms vs 本地 0.17ms）、S6 pull/lite 组名门 + 合法 pull 组查队列与位点、S7 `createTopic` 挡空白/非法/系统 topic |
| `rmq_live_hook` | 13 PASS / 0 FAIL | `CheckForbiddenHook`（放行 / 每次发送尝试都回调 / 单向也拦截 / 被拦截的消息确实没落 broker）+ `FilterMessageHook`（拉取路径 3 收 2 丢且不重投、POP 路径 2 收 1 丢且**摘掉即 ack**）+ 客户端二次 tag 过滤（订阅 `TagA` 只收 `TagA`）+ 钩子异常被吞掉不影响后续钩子 |
| `rmq_live_lite_pull` | 33 PASS / 0 FAIL | `DefaultLitePullConsumer` 真机全链路：S1 后台 rebalance 拿到 4 个队列 → S2 subscribe+poll 收全 12 条且内容一致 → S3 `commit` 后各队列位点 >0 → S4 assign+seek 从头重收 → S5 订阅级 tag 只收 6 条 → S6a `CONSUME_FROM_TIMESTAMP`（墙钟起点早于全部消息 → 收全）、S6b `offset_for_timestamp` 双向（30 分钟前 → 队首 Σ=0，10 分钟后 → Σ=12）→ S7a 默认策略名 `AVG` 且策略为 null 时 `start()` 报 Java 同款文案、S7b 换 `AVG_BY_CIRCLE` 后**同组两实例**分配无交集、并集覆盖 4 队列、下标步长 2（交叉而非连续段）、S7c 两半 `CONFIG` 各自只收到配置队列里的消息且合起来恰好 12 条互不重叠、S7d `CONSISTENT_HASH` 用**真实 clientId** 建环且线上分配收敛到「真实 mqAll/cidAll 离线跑同一策略」的预测（合起来收全 12 条）、S7e `MACHINE_ROOM_NEARBY-CONSISTENT_HASH` 在单机房下**原样透传**内层策略 + resolver 被逐个队列/两个真实 clientId 问过、S7f `MACHINE_ROOM` 白名单不匹配真实 `broker-a` → 安静饿死（分不到队列、poll 不到消息、同组 AVG 对照组仍只拿自己那半边）|

| `rmq_recall_live` | 14 PASS / 0 FAIL | 定时消息撤回 `recallMessage`(370) 真机（与 Python/Rust/.NET 同场景）：R0 读得到 broker 的 `recallMessageEnable` 并临时打开 → R1 只有带 `TIMER_DELAY_SEC` 的消息回 `recallHandle`，普通消息没有 → R2 broker 给的句柄能被本端口解码器解开，`topic`/`brokerName`/`uniqKey` 与发送结果逐字段一致 → R4 `%RETRY%` topic 本地用 Java 文案拒掉、R5 非法句柄 <200ms 秒回（没打网络）→ R3 撤回返回被撤回消息的 uniqKey → **R6 语义**：同样延迟的对照消息按时投递、被撤回的那条整个窗口都不出现 → R7 无条件把 `recallMessageEnable` 还原成跑之前的值 |

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
├── examples/                   selfcheck / interop_tool + 16 个真机联调工具
└── tests/                      27 个 ctest 用例（含 Java 对拍）+ interop_check.py
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

**clientId 口径按 Java。** `buildClientId(instanceName)` = `<本机 IP>@<instanceName>`
（`buildMqClientId`，对应 `ClientConfig#buildMQClientId`）；instanceName 还是默认值 `DEFAULT`
时由各 facade 的 `start()` 调 `changeInstanceNameToPID` **就地**换成 `<pid>#<nanoTime>` ——
生产者与 admin 无条件，三个消费者只在 `CLUSTERING` 下（广播消费者保持 `DEFAULT`，与 Java
一致 —— Java 的 `MQClientManager` 会让同进程的广播消费者复用同一份实例，本端口是每门面各建
一份私有实例）。与 Java 两点不同：本机 IP 用
UDP sockname 探测（Java 枚举网卡），且没有 `unitName` / `enableStreamRequestType` 配置项，
拼不出 `@<unitName>` / `@STREAM` 后缀。回归：`tests/test_client_id.cpp`（ctest `client_id`）。

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
