# rocketmq-client-remoting (Rust)

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 Rust 实现，迁移自 Java 的
`org.apache.rocketmq.client` + `org.apache.rocketmq.remoting` + `org.apache.rocketmq.tools`，
逐条跟随本仓库的 Python 参考实现（`../python/`），并与 C++（`../cpp/`）/ .NET（`../dotnet/`）
两端同口径对拍。

分层与 Java / Python 一致：

- `remoting`：线协议（帧编解码、header、JSON 与 RocketMQ 二进制两路序列化）与长连接传输
- `common`：消息模型、17 段存储格式编解码、压缩、命名空间、常量、一致性哈希环
- `client`：`MQClientInstance`、Producer、Push/Pull/LitePull Consumer、Admin、钩子与轨迹

**全部对外 RPC 是 `async`**（tokio）。Python 侧的 `None` 默认参数在这里换成
`Option<...>` + 显式实参，默认值由同名常量给出。

已实现范围：

| 层 | 内容 |
| --- | --- |
| 协议层 | `RemotingCommand` 帧编解码；`CommandCustomHeader` 家族（含 V2 单字母短键 a..n）；JSON 与 RocketMQ 二进制双序列化；17 段存储格式 + 6 段批量格式 |
| 传输层 | 惰性建连 + 复用、同步 / 异步 / oneway、半包重组、opaque 匹配、超时、重连、**GO_AWAY(1500) 换连接重发一次**、broker 主动请求派发、TLS（`native-tls`）、真实 `MQVersion`（5.5.1）请求头 |
| 路由 / 心跳 | `TopicRouteData` / `QueueData` / `BrokerData`、`SubscriptionData`、`HeartbeatData`、`TopicPublishInfo` 队列轮询游标 |
| 客户端 | `MQClientInstance`（进程级实例表复用）、`DefaultMQProducer`（同步/定点/批量/oneway/选择器/异步/事务/回查）、`DefaultMQPushConsumer`（长轮询 + **POP** + 顺序 + 广播 + 位点持久化）、`DefaultMQPullConsumer`、`DefaultLitePullConsumer`、`DefaultMQAdminExt` |
| 队列分配 | 六个策略：`AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>`，可插拔、由真实重平衡驱动 |
| 5.x 能力 | POP（`POP_CK` 8 段反构 / `ACK` / `CHANGE_MESSAGE_INVISIBLE`）、Request-Reply、消息轨迹（编码 + 异步分发 + 钩子）、消费侧统计、`ConsumerRunningInfo`(307)、指标、ACL 签名、动态 name server 取址、故障规避选队列 |
| 校验门 | `Validators` / `TopicValidator`：`check_topic` / `check_group` / `is_system_topic` / `is_not_allowed_send_topic` / `check_message`，四类 facade 的 `start()` 在建客户端实例**之前**跑完组名校验（纯本地判定，失败不碰网络） |
| 压缩 | zlib / LZ4 Frame / ZSTD 三后端：生产端自动压缩 + 消费端自动解压，线上帧格式与 Java lz4-java / zstd-jni 互通 |

**未实测**：TLS 真机链路（代码在，`ROCKETMQ_TLS_ENABLE=1` 生效，本机集群没开 TLS 端口）、
Windows / MSVC 分支。

## 依赖

`Cargo.toml` 里的全部依赖，**没有 workspace 外的私有源，也不需要下载额外东西**：

| crate | 用途 |
| --- | --- |
| `tokio`（rt-multi-thread / net / time / io-util / sync / macros） | 运行时与异步 socket；dev 多开 `test-util`（进程内假集群的定时断言用得到） |
| `serde` + `serde_json`（`preserve_order`） | 协议 body 编解码；`preserve_order` 让 `Map` 按插入顺序序列化，`offsetTable` 这类以对象为键的 body 才能稳定复现 |
| `flate2` / `lz4_flex`（`frame`）/ `zstd` | 三个压缩后端 |
| `hmac` + `sha1` + `base64` | ACL 签名 `Base64(HmacSHA1(secretKey, content))` |
| `native-tls` | TLS 分支 |
| `chrono` | 14 位本地墙钟（`consumeTimestamp`、日志时间戳） |

## 构建与检查

```bash
cd rust
cargo build
cargo clippy --all-targets     # 零 warning 是硬门槛（examples 一起查）
cargo test --lib               # 653 条，~0.3s
```

## 单元测试

653 条按模块分布（`cargo test --lib -- --list` 可复算）：

| 模块 | 条数 | 覆盖 |
| --- | --- | --- |
| `remoting::protocol` | 105 | header 字段名表逐个与 Java 对拍（错一个字母就静默丢字段）、`codes` 常量守卫、`TopicStatsTable` / `ConsumeStats` / `ResetOffsetBody` 等 body、POP `extraInfo` 8 段反构、JSON 对 fastjson2 非标准输出的容忍（裸数字键、对象 key、NaN/Infinity、尾逗号）、RocketMQ 二进制往返、`RecallMessageRequestHeader` 的 **`bname`** 键名守卫 |
| `remoting::client` | 16 | 真 socket 回环：同步/异步/oneway、半包重组、并发请求各自匹配 opaque、静默超时、建连失败与坏端口、`close_channel` 强制重连、**GO_AWAY 重连后只重发一次**（第三次不陷入死循环、关掉开关则直接抛）、broker 推送抵达 processor、RPC 钩子在编码前执行、地址切分与帧长守卫 |
| `remoting::rpchook` | 4 | ACL 签名：extFields 按 key 字典序、只拼 value、跳过 `Signature`、再拼 body，与 Java 官方向量对拍 |
| `common::message_decoder` | 38 | 17 段 / 6 段两条编码路径（切勿混用）、压缩段的 crc32（Java `& 0x7FFFFFFF`）、批量消息、坏数据必须拒收 |
| `common::consistent_hash` | 8 | 环：MD5 摘要**只取前 4 字节大端**、虚拟节点 key 从 `existingReplicas` 起算、`tailMap` **含端点**、越过环末尾回绕、空环返回 `None`、负虚拟节点数只在构造处报错 |
| `common::compression` | 7 | 三后端往返 + 类型解析（含 Java 的 `0→ZLIB` 兼容映射）；未支持类型必须抛错而不是透传压缩字节 |
| `common::recall_message_handle` | 6 | 定时消息撤回句柄 v1：与 Java `buildHandle` 的**真值向量**对拍（带 `=` 填充）、无填充句柄也能解（跨客户端撤回）、6 段新版本忽略尾段、空串/坏 base64/非法 utf-8/`v2`/段数不足一律 Java 文案 `recall handle is invalid` |
| `common` 其余 | 74 | `message` / `message_const` / `message_type` / `message_client_id_setter`、`mix_all`（含 `%NS%` 前缀与 `build_mq_client_id` / `change_instance_name_to_pid` 的 clientId 口径）、`sysflag`、`util_all`（14 位墙钟、`nano_time`、`is_blank`）、`topic_config`、`topic_validator`、`buffer`、`logging` |
| `client::producer` | 49 | 配置与生命周期（含 Java `buildMQClientId` 的 clientId 口径：`<ip>@<pid>#<nanoTime>`、重启不换、同名 instanceName 共用一份实例）、选队列、压缩时机、事务两阶段；其中 `send_retry_tests` 用**进程内 mock 集群**（真 socket + 脚本化响应码）锁死 `sendDefaultImpl` 的重试分类：可重试码换 broker、不可重试码立即抛、重试耗尽报 `BrokersSent`、单次超时钳位、预算耗尽报 callTimeout、无路由快速失败 10005、连接失败隔离 |
| `client::allocate_strategy` | 30 | 六个策略与 Java 单测逐条对拍：`AVG` / `AVG_BY_CIRCLE` 的 Java 用例（10/4、7/3、边界队列续接）、四道 `check` 守卫返回**空结果**而非 Java 的 `IllegalArgumentException`、`CONFIG` 不查守卫且返回副本、六个 `get_name()` 与 Java 常量一致、N 消费者不重不漏、`CONSISTENT_HASH` 的哈希环表逐格、`MACHINE_ROOM` 的 `[0,1,4]/[2,3]` 分片与 `String#split("@")` 裁尾空段真值表、`MACHINE_ROOM_NEARBY` 同机房优先 + 无消费者机房由全员共享 + resolver 空机房**抛错**（保住上一轮分配） |
| `client::consumer` / `pull_consumer` / `consume_executor` / `consumer_stats` | 98 | 订阅与 `MessageSelector`、clientId 的 CLUSTERING/BROADCASTING 分岔（广播保持 `DEFAULT` 并复用同一份实例）、`PopProcessQueue`、过滤与投递接缝、pull/lite 状态机（subscribe/assign/seek/poll/committed）、`consume_executor` 的 core/max 两档弹性语义（空闲 worker 按 keepAlive 退休）、`StatsItem` 窗口端点差分（不依赖真实时钟） |
| 轨迹四件套 `trace` / `trace_hook` / `trace_dispatcher` / `trace_context` | 101 | 与 Java 官方实现的逐字节对拍（Pub / SubBefore / SubAfter / EndTransaction / Recall）、SOH/STX 文本编解码双向、无 keys 空段容错、坏记录只跳过自己、分发器攒批/切块/防递归、W3C `traceparent` 生成与校验 |
| `client` 其余 | 115 | `mq_client`（实例表复用、心跳装配、路由缓存、**共用实例的关闭守卫**：还有 producer / 拉模式消费者登记时 `shutdown()` 是 no-op，最后一个租户退掉才真拆并摘掉 `INSTANCE_MAP` 登记；启动失败同样就地清理）、`admin`（properties 文本、分页合并、地址挑选）、`latency`、`hook`、`request_reply`、`metrics`、`top_addressing`、`result`、`validators` |
| `error` | 2 | 错误码口径（10001..10005）与 `Display` |

## 真实集群联调

需要跑着 nameServer(9876) + broker(10911)、且 `autoCreateTopicEnable=true` 的集群
（5.5.1 上游构建即可）。这些工具**不进 `cargo test`**，依赖外部集群：

```bash
cargo run --example live_protocol           -- 127.0.0.1:9876
cargo run --example live_mq_client          -- 127.0.0.1:9876
cargo run --example live_producer           -- 127.0.0.1:9876
cargo run --example live_consumer           -- 127.0.0.1:9876
cargo run --example live_pull_consumer      -- 127.0.0.1:9876
cargo run --example live_lite_pull_consumer -- 127.0.0.1:9876
cargo run --example live_alloc_strategy     -- 127.0.0.1:9876
cargo run --example live_rebalance_and_trace -- 127.0.0.1:9876
cargo run --example live_client_modules     -- 127.0.0.1:9876
cargo run --example live_validators         -- 127.0.0.1:9876
cargo run --example live_admin              -- 127.0.0.1:9876
cargo run --example live_acl                -- 127.0.0.1:9876 <AK> <SK>   # 需开 ACL 的集群
cargo run --example live_compression_matrix -- send|recv ...             # 由 ../scripts/compression_matrix.sh 调度
```

任何一项失败进程以非 0 退出码结束；`== summary: N passed, M failed ==` 是收口行。
下表是 **2026-09-21 在本地 5.5.1 集群上的实测结果**（上表前 11 个工具合计 680 项断言；
`live_acl` 本机集群没开鉴权、`live_compression_matrix` 由脚本调度，都不计进去）：

| 工具 | 结果 | 覆盖 |
| --- | --- | --- |
| `live_protocol` | 43 PASS | S0~S8：路由/集群信息 → `SEND_MESSAGE_V2`(310) 短键 header → `PULL_MESSAGE`(11) + 17 段解码 → 四个 offset RPC → 心跳/消费组列表/注销 → **RocketMQ 二进制 header 在真 broker 上的往返** → 清理 |
| `live_mq_client` | 84 PASS | M1~M8：实例身份与复用、路由与 `TopicPublishInfo` 游标（未知 topic 只在生产者路径回退 `TBW102`）、收发逐字段、位点五 RPC、心跳真被 broker 采纳（用 `GET_CONSUMER_LIST_BY_GROUP` 反查证明）、**POP 弹回→ACK→改不可见时间→再弹拿不到**、队列批量锁真互斥、**共用实例的关闭守卫**（同 clientId 的两个生产者 + 一个 lite 消费者：先退的门面之后兄弟仍能真发消息，最后一个退掉才拆循环并摘掉 `INSTANCE_MAP`，同 clientId 重建才拿到可用新实例） |
| `live_producer` | 55 PASS | P1~P10：生命周期与心跳注册、六条发送路径、压缩消息 broker 端透明解压且清 flag、三类钩子、轨迹接缝、Request-Reply 三属性与超时、**事务两阶段 + broker 回查**、管理便捷方法、发送重试内核定性、**定时消息撤回 `recallMessage`(370)**（broker 给的句柄能解开、撤回返回 uniqKey、对照定时消息按时到 / 被撤回那条永不到、`recallMessageEnable` 还原）、topic 清理 |
| `live_consumer` | 88 PASS | C1~C10：`start()` 三道校验与首轮同步心跳、长轮询 24 条不重不丢且位点刷到 broker、tag 过滤（broker 存 20 只投 10）、`RECONSUME_LATER` 走 `%RETRY%` 重投、POP + ack、广播位点、顺序消费 broker 锁、多实例分摊与撤位、broker 推来的 `NOTIFY_CONSUMER_IDS_CHANGED`(40) 确实叫醒了两端、`ConsumerRunningInfo` |
| `live_pull_consumer` | 43 PASS | P1~P11：生命周期、`fetch_subscribe_message_queues`、定向 12 条、手动 pull 不重不漏 + `broker_name` 回填、位点由调用方掌控（回退再拉 FOUND、换 tag `NO_MATCHED_MSG`）、未提交组读位点得 `None` 而非 0、**长轮询真的挂起** |
| `live_lite_pull_consumer` | 48 PASS | L1~L10：`start()` 校验（含 14 位墙钟硬失败）、subscribe 后台重平衡收全 12 条、`auto_commit` 两态、assign + `seek_to_begin` 重放、`seek()` 丢掉缓冲里早于目标位点的消息、pause/resume、手工心跳 |
| `live_alloc_strategy` | 26 PASS | A1~A6：**策略真的驱动重平衡**。A1/A2 默认 `AVG` 与 null 策略被 `start()` 按 Java 文案拒绝、A3 `AVG_BY_CIRCLE` / `CONFIG` 两实例交叉与分半、A4 `CONSISTENT_HASH` 用真实 clientId 建环、A5 `MACHINE_ROOM_NEARBY-CONSISTENT_HASH` 单机房下原样透传内层策略且 resolver 被逐个真实 brokerName 与两个真实 clientId 问过、A6 `MACHINE_ROOM` 白名单不匹配 `broker-a` 时**安静饿死**（同组 AVG 对照组仍只拿自己半边）。收敛判据统一是「线上 `assignment()` == 用真实 mqAll/cidAll 离线跑同一策略的预测」——"两边都非空且并集覆盖全队列"是**假收敛**：环算法下一实例本就合法地拿到全部，且对端心跳落地前每台都会先拿全部 |
| `live_rebalance_and_trace` | 85 PASS | R1~R3：真实路由队列上跑三种策略的端到端切分（A 只看得到自己队列的消息）、真实消息过 core/max 两档执行器且每条恰好一次、轨迹钩子产出的记录真落到 broker 并被解码回来 |
| `live_client_modules` | 92 PASS | T1~T8：动态 namesrv 取址（本地起地址服务器桩并**用取到的地址真查一次路由**）、实测延迟喂故障规避选队列、真实 RT/TPS 过统计、五类钩子的相反异常语义、轨迹文本穿过 broker、指标记账、request-reply |
| `live_validators` | 31 PASS | V1~V7（与 Python/C++/.NET 同场景对拍）：非法名字**亚毫秒本地失败且不碰网络**、`maxMessageSize` 等长放行、批量逐条 + 同质性、四类 facade 的 `start()` 组名门、正反两条腿的往返耗时对照 |
| `live_admin` | 85 PASS / 1 SKIP | A1~A13：admin **私有实例**隔离（shutdown 后同 clientId 的 producer 照常收发）、集群与运行时信息、topic 建/查/配置、broker 配置（properties 文本）读改写回、NameServer KV、订阅组分页、`viewMessage`、`sendMessageBack` 重投、`resetOffsetByTimestamp`、消费统计与 ConsumeQueue、清理。SKIP：uniqKey 查询要 broker 开 RocksDB 索引，本机默认文件索引查不到属**配置差异，不是客户端 bug** |
| `live_acl` | 需开鉴权的集群 | S1~S8：签名被 broker 接受（建 topic / 发送 / 心跳+长轮询+位点三条 RPC 全程带签名）、不带凭据与 secretKey 写错都回 `NO_PERMISSION(16)`、拉模式签名链路。前置是 broker.conf 开 `authenticationEnabled=true` + `LocalAuthenticationMetadataProvider` + `initAuthenticationUser`（本机默认集群关着，跑不了这一项） |
| `live_compression_matrix` | 矩阵一端 | 与 Java/Python/C++/.NET 探针双向收发压缩消息，由 `../scripts/compression_matrix.sh` 调度 |

## 目录结构

```
rust/
├── Cargo.toml                  tokio + serde_json（+ 三压缩后端 / hmac+sha1 / native-tls）
├── src/
│   ├── lib.rs                  三层出口
│   ├── error.rs                Error / Result / client_error_code(10001..10005)
│   ├── common/
│   │   ├── message.rs              Message / MessageExt / MessageQueue
│   │   ├── message_decoder.rs      17 段存储格式 + 6 段批量格式
│   │   ├── compression.rs          zlib / LZ4 Frame / ZSTD
│   │   ├── consistent_hash.rs      一致性哈希环（自带 MD5，只取前 4 字节大端）
│   │   ├── recall_message_handle.rs 定时消息撤回句柄 v1（base64url + 5 段）
│   │   ├── buffer.rs / sysflag.rs / mix_all.rs / util_all.rs
│   │   ├── topic_config.rs / topic_validator.rs
│   │   ├── message_const.rs / message_type.rs / message_client_id_setter.rs
│   │   └── logging.rs              按天改名轮转 + stderr
│   ├── remoting/
│   │   ├── client.rs               同步 / 异步 / oneway + 拆包重组 + TLS + GO_AWAY
│   │   ├── rpchook.rs              AclClientRPCHook（签名逐字节对齐 Java）
│   │   └── protocol/               remoting_command / headers（含 V2 短键）/ codes /
│   │                               serialize / route / heartbeat / body / admin_body /
│   │                               subscription / extra_info(POP 8 段) / namespace_util /
│   │                               ext_fields
│   └── client/
│       ├── mq_client.rs            MQClientInstance：路由发现 + 全部 RPC + 心跳
│       ├── producer.rs             DefaultMQProducer（含 send_retry_tests）
│       ├── consumer.rs             DefaultMQPushConsumer（长轮询 + POP + 顺序）
│       ├── pull_consumer.rs        DefaultMQPullConsumer + DefaultLitePullConsumer
│       ├── allocate_strategy.rs    六个队列分配策略
│       ├── admin.rs                DefaultMQAdminExt
│       ├── consume_executor.rs     core/max 两档执行器（Java ThreadPoolExecutor 等价物）
│       ├── hook.rs / latency.rs / consumer_stats.rs / metrics.rs
│       ├── request_reply.rs / top_addressing.rs / validators.rs / result.rs
│       └── trace.rs / trace_context.rs / trace_hook.rs / trace_dispatcher.rs
└── examples/                   13 个真机联调工具（见上表，不依赖集群的没有）
```

单元测试全部内联在 `src/**/mod tests`（没有独立 `tests/` 目录）—— 需要访问
`pub(crate)` 与假时钟；只有真机才能证明的性质全部下沉到 `examples/`。

## 几个必须知道的实现约定

**字段名一律以 Java 为准。** broker 用 fastjson2 按 Java 属性名反序列化，字段名错一个
就**静默丢字段**（不报错、不报错的码）。`remoting/protocol/headers.rs` 里那张
`(类名, [字段名])` 表 + `serialize.rs` 的 JSON 容错就是为守住这件事而存在，别改成"看着更自然"的命名。

**守卫不抛异常。** Java `AbstractAllocateMessageQueueStrategy#check` 对 `currentCID` 空串 /
`mqAll` 空 / `cidAll` 空抛 `IllegalArgumentException`，本仓库跟随 Python：**返回空结果**。
两个例外都构造期/后台期分得很清：`MACHINE_ROOM_NEARBY` 的 resolver 给出空机房会**抛错**
（静默返回空等于把整个 topic 的队列撤走），`AllocateMachineRoomNearby::new` 缺参数由
`Arc` 在类型上排除（Java 的 `NullPointerException` 在这里不可能构造出来）。

**`get_name() -> &str` 逼着名字在构造期算好。** Java 的 `getName()` 每次拼接
（`MACHINE_ROOM_NEARBY-<内层>`），Rust 返回借用，所以 `AllocateMachineRoomNearby` 在
`new()` 里把 `name: String` 存下来。新增装饰类时记得同样在构造期落盘，别返回临时值的引用。

**`String#split("@")` 的语义不是"按 @ 切开"。** Java 丢弃**尾部空串**，
各语言原生 split 不丢。机房名从 clientId 里切出来时这一点直接决定 `MACHINE_ROOM` 分到
哪一组，四个端口这一轮一起修成 Java 口径（真值表在单测里，8 行）。

**压缩在重试循环之外只做一次。** Java `sendKernelImpl` 在重试循环**内**调
`tryToCompressMessage`，而它会就地 `setBody` —— 重试时把已压缩的 body 再压一遍
（`zlib(zlib(x))`），消费端只解一层就把压缩流当正文交出去。这里刻意把压缩提到循环外，
是真 bug 的规避，不是风格差异。

**客户端本地校验的错误没有 `response_code`。** Java `MQClientException(String, null)` ⇒
`responseCode = -1`；只有 `check_message` 的 body 档位带 `MESSAGE_ILLEGAL(13)`。所以
"往 `SCHEDULE_TOPIC_XXXX` 发消息"报的是**无码**错误而不是 13 —— 看着别扭，但上层按
`response_code` 分支时必须知道，四语言保持一致。

**非测试路径不用 `unwrap` / `expect`。** 全仓只剩一处：`rpchook.rs` 的
`Hmac::new_from_slice(...).expect("HMAC accepts any key length")`（`hmac` 的返回类型
是历史包袱，任何长度都不会失败）。锁中毒用 `unwrap_or_else(|e| e.into_inner())` 兜住，
不给后台任务留 panic 入口。

**日志刻意不叫 Java 的 `rocketmq_client.log`。** 落
`$HOME/logs/rocketmqlogs/rocketmq_rs_client.log`，按天改名轮转。同机同文件会互相插行；
更糟的是改名后其它进程仍持旧 fd，日志写进已 unlink 的 inode 而静默消失。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` | 取不到用户目录时退化为 `logs/rocketmqlogs` |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_rs_client.log` | 轮转后的名字带日期后缀 |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `WARN`(`WARNING`) / `ERROR` / `OFF`(`NONE`)，其余一律按 INFO |
| `ROCKETMQ_CLIENT_LOG_MAX_INDEX` | `10` | 保留份数，对齐 Java `rocketmq.log.file.maxIndex` |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | 开 | 设为 `false` 只写文件 |
| `ROCKETMQ_TLS_ENABLE` | `false` | 打开后所有出连接走 `native-tls` |
| `ROCKETMQ_TLS_TEST_MODE` | `true` | 对应 Java `tls.test.mode.enable`：信任自签证书、不校验主机名 |

## 与 Java 的已知差异 / 待办

- **clientId 后缀缺 `unitName` 与 `@STREAM`**：默认口径已按 Java 走
  `buildMQClientId` ⇒ `<本机 IP>@<instanceName>`，`instanceName` 为 `DEFAULT` 时在
  `start()` 里就地改写成 `<pid>#<nanoTime>`（生产者和 CLUSTERING 消费者；广播消费者
  保持 `DEFAULT`，因此同进程的广播消费者共用一份实例，与 Java 一致）。
  `build_mq_client_id(ip, instanceName, unitName)` 支持 unitName 后缀，但客户端配置里
  还没有 unitName / `enableStreamRequestType` 这两项，所以拼不出带后缀的 clientId。
- **本机 IP 探测方式不同**：Java 枚举网卡并优先非内网 IPv4，这里用 UDP「连」公网地址后
  读 sockname（不发包），取不到退化成 `127.0.0.1`。结果通常是同一块出口网卡的地址。
- **`unitName` / `unitMode` 没有客户端配置项**（协议字段在，能编解码）。
- **broker 主动请求（220/221/307/309/326）无法从外部注入**：它们走 broker 已建立的那条
  连接。协议与分派由离线单测覆盖，`live_mq_client` 只验实例侧的 seam。
- **SQL92 过滤 / `%DLQ%` 死信 / 批量发送真机 / Rust TLS / 动态地址服务器真机**这几条
  链路还没跑过带它们的环境（待办 #37）；ACL 需要开鉴权的 broker 才能跑。

## License

Apache-2.0，与上游 RocketMQ 保持一致。
