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
| 传输层 | `RemotingClient`：同步 / 异步 / oneway、半包重组、opaque 匹配、重连、SIGPIPE 处理 |
| 路由 / 心跳 | `TopicRouteData` / `QueueData` / `BrokerData`、`SubscriptionData`、`HeartbeatData` |
| 客户端 | `MQClientInstance`、`DefaultMQProducer`、`DefaultMQPushConsumer`、**`DefaultMQAdminExt`** |
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
cd build && ctest --output-on-failure     # 21 个用例，约 1240 项断言，~9s
```

| 用例 | 断言 | 覆盖 |
| --- | --- | --- |
| `codec` | 65 | JSON / ROCKETMQ / `RemotingCommand` 两路 / 消息 17 段与 6 段 / header V1↔V2 / hashCode / CRC32 / msgId |
| `java_alignment` | 32（带 `ROCKETMQ_JAVA_SRC` 为 38） | `codes.h` 常量守卫，设了环境变量后**读真实 Java 源码**逐条比对 |
| `route_heartbeat` | 95 | 路由类往返 + 按 perm 过滤队列；`SubscriptionData` / `HeartbeatData` 往返 + **Java 字段名守卫** |
| `transport` | 35 | 真实本机 TCP：同步/异步/oneway、**半包重组**、opaque 匹配、建连失败、超时、重连、地址解析 |
| `compression` | 152 | 三后端：类型解析（含 Java 的 `0→ZLIB` 兼容映射）、zlib / **LZ4 Frame** / **ZSTD** 往返、**外部硬编码真值夹具**（Python zlib.compress、Java lz4-java、zstd-jni、`zstd -3` CLI 各产的帧）、帧头 magic 与 LZ4 block-independence 位、17 段报文 × 三种类型、解压后清 flag、后端缺失或未知类型必须抛错而非交出压缩流 |
| `admin` | 152 | fastjson2 非法 JSON 容错、`TopicConfig` / `SubscriptionGroupConfig` 默认值与字段名、`TopicStatsTable` / `ConsumeStats` / `ResetOffsetBody`、properties 文本往返、`PermName::isValid` |
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
./build/examples/rmq_live_request_reply 127.0.0.1:9876
./build/examples/rmq_live_latency       127.0.0.1:9876
./build/examples/rmq_live_pop           127.0.0.1:9876
./build/examples/rmq_live_pop_consumer  127.0.0.1:9876
./build/examples/rmq_live_trace         127.0.0.1:9876   # 需 broker traceTopicEnable=true
./build/examples/rmq_live_hook          127.0.0.1:9876
```

| 工具 | 结果 | 覆盖 |
| --- | --- | --- |
| `rmq_live_message_types` | 12/12 | 异步发送 / 顺序消息（同 key 同队列 + 保序）/ Tag 过滤 / 用户属性 / 延迟消息 / 按 Key 查询 / 事务消息 / 心跳注册 |
| `rmq_admin_live` | 47 PASS / 1 SKIP | 集群探活 → 建 topic → 路由/配置查询 → **broker 配置（properties 文本）读改写回** → NameServer KV → 订阅组（建/单查/分页/examine/删）→ 生产 → 各类统计与查询 → `viewMessage` → **`sendMessageBack` 重投到 `%RETRY%`** → `resetOffsetByTimestamp` → 清理 |
| `rmq_compression_live` | 10 PASS | 三后端（zlib / LZ4 Frame / ZSTD）自动压缩自产自销 + **与真实 Java/Python/.NET/Rust 客户端双向互通**（互通矩阵见 `../scripts/compression_matrix.sh`）；构建时未编入的后端打 SKIP |
| `rmq_live_trace` | 17 PASS / 0 FAIL | 消息轨迹全链路：`SendResult`（UNIQ_KEY / offsetMsgId / regionId / traceOn）→ Pub 轨迹 → 业务消费 → SubBefore/SubAfter 配对与 contextCode → 轨迹消息 keys 反查 → 防递归（轨迹 topic 自身不上报）→ `enable_trace=false` 不产生轨迹 → 编码段数 == 解码记录数 → 无 keys 消息的空段容错 |
| `rmq_live_hook` | 13 PASS / 0 FAIL | `CheckForbiddenHook`（放行 / 每次发送尝试都回调 / 单向也拦截 / 被拦截的消息确实没落 broker）+ `FilterMessageHook`（拉取路径 3 收 2 丢且不重投、POP 路径 2 收 1 丢且**摘掉即 ack**）+ 客户端二次 tag 过滤（订阅 `TagA` 只收 `TagA`）+ 钩子异常被吞掉不影响后续钩子 |

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
│       ├── hook.h / trace.h / trace_hook.h / trace_dispatcher.h
│       │                            钩子接口（Send/Consume/EndTransaction/CheckForbidden/
│       │                            FilterMessage）+ 消息轨迹文本编解码 + 异步分发
├── src/                        与 include 同构的 31 个 .cpp
├── examples/                   selfcheck / interop_tool + 12 个真机联调工具
└── tests/                      15 个 ctest 用例（含 Java 对拍）+ interop_check.py
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
