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

**未实测**：Windows / MSVC 分支。TLS 分支已按真机跑过：本机 5.5.1 集群在 test-mode 下**同一组端口**
（nameServer 9876、broker 10911）嗅探 TLS，`ROCKETMQ_TLS_ENABLE=1` 直接生效，六个 `live_*`
例子合计 **424 项断言全绿**（producer 62 / push consumer 95 / pull 43 / lite-pull 48 /
mq_client 84 / admin 92+1 skip），实测过程见「与 Java 的差异」的 TLS 条。

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
cargo test --lib               # 719 条，~1.3s
```

## 单元测试

719 条按模块分布（`cargo test --lib -- --list` 可复算）：

| 模块 | 条数 | 覆盖 |
| --- | --- | --- |
| `remoting::protocol` | 105 | header 字段名表逐个与 Java 对拍（错一个字母就静默丢字段）、`codes` 常量守卫、`TopicStatsTable` / `ConsumeStats` / `ResetOffsetBody` 等 body、POP `extraInfo` 8 段反构、JSON 对 fastjson2 非标准输出的容忍（裸数字键、对象 key、NaN/Infinity、尾逗号）、RocketMQ 二进制往返、`RecallMessageRequestHeader` 的 **`bname`** 键名守卫 |
| `remoting::client` | 21 | 真 socket 回环：同步/异步/oneway、半包重组、并发请求各自匹配 opaque、静默超时、建连失败与坏端口、`close_channel` 强制重连、**GO_AWAY 重连后只重发一次**（第三次不陷入死循环、关掉开关则直接抛）、broker 推送抵达 processor、RPC 钩子在编码前执行、地址切分与帧长守卫；另有 **5 条 TLS 离线回归**（openssl 现造自签证书 + 进程内 `TlsAcceptor`）：3 字节分片的半包续读、1MiB body 完整写出、8 路并发共用一条连接不被读写抢锁饿死、**broker 推来的请求在读线程上拿得到运行时上下文并回得出包**、TLS 客户端打明文端口必须映射成连接错误 |
| `remoting::rpchook` | 6 | ACL 签名：extFields 按 key 字典序、只拼 value、跳过 `Signature`、再拼 body，与 Java 官方向量对拍 |
| `common::message_decoder` | 38 | 17 段 / 6 段两条编码路径（切勿混用）、压缩段的 crc32（Java `& 0x7FFFFFFF`）、批量消息、坏数据必须拒收 |
| `common::consistent_hash` | 8 | 环：MD5 摘要**只取前 4 字节大端**、虚拟节点 key 从 `existingReplicas` 起算、`tailMap` **含端点**、越过环末尾回绕、空环返回 `None`、负虚拟节点数只在构造处报错 |
| `common::compression` | 7 | 三后端往返 + 类型解析（含 Java 的 `0→ZLIB` 兼容映射）；未支持类型必须抛错而不是透传压缩字节 |
| `common::recall_message_handle` | 6 | 定时消息撤回句柄 v1：与 Java `buildHandle` 的**真值向量**对拍（带 `=` 填充）、无填充句柄也能解（跨客户端撤回）、6 段新版本忽略尾段、空串/坏 base64/非法 utf-8/`v2`/段数不足一律 Java 文案 `recall handle is invalid` |
| `common` 其余 | 73 | `message` / `message_const` / `message_type` / `message_client_id_setter`、`mix_all`（含 `%NS%` 前缀与 `build_mq_client_id` / `change_instance_name_to_pid` 的 clientId 口径）、`sysflag`、`util_all`（14 位墙钟、`nano_time`、`is_blank`）、`topic_config`、`topic_validator`、`buffer`、`logging` |
| `client::producer` | 83 | 配置与生命周期（含 Java `buildMQClientId` 的 clientId 口径：`<ip>@<pid>#<nanoTime>`、重启不换、同名 instanceName 共用一份实例）、选队列、压缩时机、事务两阶段；其中 `send_retry_tests` 用**进程内 mock 集群**（真 socket + 脚本化响应码）锁死 `sendDefaultImpl` 的重试分类：可重试码换 broker、不可重试码立即抛、重试耗尽报 `BrokersSent`、单次超时钳位、预算耗尽报 callTimeout、无路由快速失败 10005、连接失败隔离；同一套抓取也取证明线上的字段口径（`k`=unitMode、`ReqT`、发送请求码 310/320/325 与 `m`=batch 的三级判据），并取证**批量发送的 ID 顺序**（Java `batch():1172-1184`：先给每条子消息 `setUniqID`、再给整批那条补一个、**最后**才 `setBody(encode())`，所以抓到的报文里 3 条子消息各带一个不重复的 32 位 UNIQ_KEY，`msgId` 是批量自身的、不是 broker 的 offsetMsgId）。另有 **27 项异步发送内核 + 背压闸门**（`send_retry_tests` 的「异步发送背压」一节，行号 866 到文件末尾）：闸门侧 —— 开关关掉完全不碰两个闸、条数闸超容按 Java 原文案 `send message tryAcquire semaphoreAsyncNum timeout` 拒绝且**一次请求都没发出去**、字节闸拒了要把**已拿到的条数许可**还回去、在途份数逐笔扣还、失败路径照样归还、整条重试链只占**一份**许可（不是每次尝试一份）、闸会等到预算耗尽而不是看一眼就拒、运行时改容量保留在途份额、扩容叫醒卡住的发送、空 body 也按 1 个字节许可算、排队排掉预算报 `DEFAULT ASYNC send call timeout`；有界队列侧 —— 队满时把 `executor rejected` **同步**抛回调用方且一笔都没发、开了背压则派发到队列之外照样跑完、`shutdown()` 之后再投直接被拒；异步内核侧 —— 预建请求阶段越过预算报 send kernel timeout、连接失败换 broker 重试且**同一个请求换新 opaque**、重试次数受 `retryTimesWhenSendAsyncFailed` 管（填 0 就是只试一次）、**broker 返回的业务码不进重试链**（一次就回调）、定点队列的重试仍留在同一台 broker、before/after 钩子每笔异步发送各跑一次、after 钩子看得见真实成败与异常、未 `start()` 时同步抛且**不回调**；再加一条 32 工作线程运行时下的回归：钩子同步睡住时池子仍逐笔派发（`resize_wakes_the_parked_sender_while_the_pool_is_saturated`，去掉「派发任务 + 信号量」的池设计会红） |
| `client::backpressure` | 12 | Java `Semaphore(permits, true)` 的公平计数信号量：按 Java 的单位逐个拿/还、`permits<=0` 请求像 Java 一样被忽略、**只有队首能拿许可**、阻塞的队首把后面的人按 FIFO 排住、改总量保留在途份额、改容量叫醒等待者、队首成交后仍要喂后面那个等待者（否则真机上是 5 秒死等）、队首超时离开也要叫醒后面的人、丢弃的等待方不留幽灵票据、预算恰好用尽时只在队首清空才放行、空闲许可可以为负再拉回正、两个地板值（10 条 / 1MiB 字节）与 Java 一致 |
| `client::allocate_strategy` | 30 | 六个策略与 Java 单测逐条对拍：`AVG` / `AVG_BY_CIRCLE` 的 Java 用例（10/4、7/3、边界队列续接）、四道 `check` 守卫返回**空结果**而非 Java 的 `IllegalArgumentException`、`CONFIG` 不查守卫且返回副本、六个 `get_name()` 与 Java 常量一致、N 消费者不重不漏、`CONSISTENT_HASH` 的哈希环表逐格、`MACHINE_ROOM` 的 `[0,1,4]/[2,3]` 分片与 `String#split("@")` 裁尾空段真值表、`MACHINE_ROOM_NEARBY` 同机房优先 + 无消费者机房由全员共享 + resolver 空机房**抛错**（保住上一轮分配） |
| `client::consumer` / `pull_consumer` / `consume_executor` / `consumer_stats` | 103 | 订阅与 `MessageSelector`、clientId 的 CLUSTERING/BROADCASTING 分岔（广播保持 `DEFAULT` 并复用同一份实例）、`PopProcessQueue`、过滤与投递接缝、pull/lite 状态机（subscribe/assign/seek/poll/committed）、`consume_executor` 的 core/max 两档弹性语义（空闲 worker 按 keepAlive 退休）、`StatsItem` 窗口端点差分（不依赖真实时钟） |
| 轨迹四件套 `trace` / `trace_hook` / `trace_dispatcher` / `trace_context` | 101 | 与 Java 官方实现的逐字节对拍（Pub / SubBefore / SubAfter / EndTransaction / Recall）、SOH/STX 文本编解码双向、无 keys 空段容错、坏记录只跳过自己、分发器攒批/切块/防递归、W3C `traceparent` 生成与校验 |
| `client` 其余 | 124 | `mq_client`（实例表复用、心跳装配、路由缓存、**共用实例的关闭守卫**：还有 producer / 拉模式消费者登记时 `shutdown()` 是 no-op，最后一个租户退掉才真拆并摘掉 `INSTANCE_MAP` 登记；启动失败同样就地清理）、`admin`（properties 文本、分页合并、地址挑选）、`latency`、`hook`、`request_reply`、`metrics`、`top_addressing`、`result`、`validators` |
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
cargo run --example live_unit_config        -- 127.0.0.1:9876
cargo run --example live_sql92              -- 127.0.0.1:9876   # 需 broker enablePropertyFilter=true
cargo run --example live_backpressure       -- 127.0.0.1:9876
cargo run --example live_async_send         -- 127.0.0.1:9876
cargo run --example live_acl                -- 127.0.0.1:9876 <AK> <SK>   # 需开 ACL 的集群
cargo run --example live_compression_matrix -- send|recv ...             # 由 ../scripts/compression_matrix.sh 调度
```

任何一项失败进程以非 0 退出码结束；`== summary: N passed, M failed ==` 是收口行。
下表是 **2026-09-22 在本地 5.5.1 集群上的实测结果**（全表当轮重测；`live_async_send` 是
2026-09-23 补的，同日重测。上表 15 个工具合计 797 项断言，`live_acl` 本机集群没开鉴权、
`live_compression_matrix` 由脚本调度，都不计进去）：

| 工具 | 结果 | 覆盖 |
| --- | --- | --- |
| `live_protocol` | 43 PASS | S0~S8：路由/集群信息 → `SEND_MESSAGE_V2`(310) 短键 header → `PULL_MESSAGE`(11) + 17 段解码 → 四个 offset RPC → 心跳/消费组列表/注销 → **RocketMQ 二进制 header 在真 broker 上的往返** → 清理 |
| `live_mq_client` | 84 PASS | M1~M8：实例身份与复用、路由与 `TopicPublishInfo` 游标（未知 topic 只在生产者路径回退 `TBW102`）、收发逐字段、位点五 RPC、心跳真被 broker 采纳（用 `GET_CONSUMER_LIST_BY_GROUP` 反查证明）、**POP 弹回→ACK→改不可见时间→再弹拿不到**、队列批量锁真互斥、**共用实例的关闭守卫**（同 clientId 的两个生产者 + 一个 lite 消费者：先退的门面之后兄弟仍能真发消息，最后一个退掉才拆循环并摘掉 `INSTANCE_MAP`，同 clientId 重建才拿到可用新实例） |
| `live_producer` | 62 PASS | P1~P10：生命周期与心跳注册、六条发送路径、压缩消息 broker 端透明解压且清 flag、三类钩子、轨迹接缝、Request-Reply 三属性与超时、**事务两阶段 + broker 回查**、管理便捷方法、发送重试内核定性、**定时消息撤回 `recallMessage`(370)**（broker 给的句柄能解开、撤回返回 uniqKey、对照定时消息按时到 / 被撤回那条永不到、`recallMessageEnable` 还原）、topic 清理。异步内核这一轮补了真机取证：P1 `shutdown()` **排空**有界发送队列（排队中的 10 笔每笔恰好拿到一次终态回调、一笔都没被吞、`shutdown()` 之后再投 `send_async` 直接拒且不新增回调）；P2 异步成功回调给出的 `SendResult` 形状与 Java 一致（状态、可解的 msgId、队列属于本 topic+broker、位点 ≥0）、**按回调自己的 queueId+offset 从 broker 把那条消息读回来**、30 笔并发异步各恰好一次终态回调且 30 个位点互不重复、每笔都在自己回调报的那个位点读得到、定点队列的 `send_async` 真落在指定队列、池子里被 `Validators.check_message` 拒掉的超大 body 恰好回调一次且文案逐字对 Java（`the message body size over max value, MAX: `）、之后生产者照常可用 |
| `live_consumer` | 102 PASS / 0 FAIL | C1~C10：`start()` 三道校验与首轮同步心跳、长轮询 24 条不重不丢且位点刷到 broker、tag 过滤（broker 存 20 只投 10）、`RECONSUME_LATER` 走 `%RETRY%` 重投、`maxReconsumeTimes=2` 用尽后落 `%DLQ%<group>`（C4b：只投 3 次、实测 0s/10s/40s、死信 `reconsumeTimes=3` 且带 `RETRY_TOPIC`）、**C4c 部分 ack**：`CONSUME_SUCCESS` + 一批 3 条里 listener 只认可第 1 条 ⇒ 尾巴 2 条从 `%RETRY%` 回来（`reconsumeTimes>=1`、listener 看到业务 topic）、已认可那条整个窗口只投一次、3 条最终全部消费、业务队列位点仍整批前进到 3，对照组（不碰 `ackIndex`，Java 默认 `Integer.MAX_VALUE`）一条都不回投（topic 只建 1 个队列且**先发消息再起消费者**，新组显式 `CONSUME_FROM_FIRST_OFFSET`，批次切分才由不得拉取时机决定）、POP + ack、广播位点、顺序消费 broker 锁、多实例分摊与撤位、broker 推来的 `NOTIFY_CONSUMER_IDS_CHANGED`(40) 确实叫醒了两端、`ConsumerRunningInfo` |
| `live_pull_consumer` | 43 PASS | P1~P11：生命周期、`fetch_subscribe_message_queues`、定向 12 条、手动 pull 不重不漏 + `broker_name` 回填、位点由调用方掌控（回退再拉 FOUND、换 tag `NO_MATCHED_MSG`）、未提交组读位点得 `None` 而非 0、**长轮询真的挂起** |
| `live_lite_pull_consumer` | 48 PASS | L1~L10：`start()` 校验（含 14 位墙钟硬失败）、subscribe 后台重平衡收全 12 条、`auto_commit` 两态、assign + `seek_to_begin` 重放、`seek()` 丢掉缓冲里早于目标位点的消息、pause/resume、手工心跳 |
| `live_alloc_strategy` | 26 PASS | A1~A6：**策略真的驱动重平衡**。A1/A2 默认 `AVG` 与 null 策略被 `start()` 按 Java 文案拒绝、A3 `AVG_BY_CIRCLE` / `CONFIG` 两实例交叉与分半、A4 `CONSISTENT_HASH` 用真实 clientId 建环、A5 `MACHINE_ROOM_NEARBY-CONSISTENT_HASH` 单机房下原样透传内层策略且 resolver 被逐个真实 brokerName 与两个真实 clientId 问过、A6 `MACHINE_ROOM` 白名单不匹配 `broker-a` 时**安静饿死**（同组 AVG 对照组仍只拿自己半边）。收敛判据统一是「线上 `assignment()` == 用真实 mqAll/cidAll 离线跑同一策略的预测」——"两边都非空且并集覆盖全队列"是**假收敛**：环算法下一实例本就合法地拿到全部，且对端心跳落地前每台都会先拿全部 |
| `live_rebalance_and_trace` | 85 PASS | R1~R3：真实路由队列上跑三种策略的端到端切分（A 只看得到自己队列的消息）、真实消息过 core/max 两档执行器且每条恰好一次、轨迹钩子产出的记录真落到 broker 并被解码回来 |
| `live_client_modules` | 92 PASS | T1~T8：动态 namesrv 取址（本地起地址服务器桩并**用取到的地址真查一次路由**）、实测延迟喂故障规避选队列、真实 RT/TPS 过统计、五类钩子的相反异常语义、轨迹文本穿过 broker、指标记账、request-reply |
| `live_validators` | 31 PASS | V1~V7（与 Python/C++/.NET 同场景对拍）：非法名字**亚毫秒本地失败且不碰网络**、`maxMessageSize` 等长放行、批量逐条 + 同质性、四类 facade 的 `start()` 组名门、正反两条腿的往返耗时对照 |
| `live_admin` | 92 PASS / 1 SKIP | A1~A13：admin **私有实例**隔离（shutdown 后同 clientId 的 producer 照常收发）、集群与运行时信息、topic 建/查/配置、broker 配置（properties 文本）读改写回、NameServer KV、订阅组分页、`viewMessage`、`sendMessageBack` 重投、`resetOffsetByTimestamp`/`resetOffsetByQueueId`（后者真机量到：重置后首笔 pull 被 broker 短路成 `PULL_OFFSET_MOVED`、第二笔才取到历史消息；越界目标被拒时位点停在第 1 笔写入的非法值 ⇒ 两笔 RPC 非原子，与 Java 同构）、`queryTopicsByConsumer(group)`（按 `%RETRY%` 路由扇出合并）与 `queryTopicsByConsumerToBroker`、消费统计与 ConsumeQueue、清理。SKIP：uniqKey 查询要 broker 开 RocksDB 索引，本机默认文件索引查不到属**配置差异，不是客户端 bug** |
| `live_unit_config` | 15 PASS | U1~U5（与 Python/C++/.NET 同场景）：`unit_name` 拼进 clientId（`<ip>@<instance>@<unitName>`）且照常发送、`@STREAM` 后缀的消费者在 **broker 的 `GET_CONSUMER_LIST_BY_GROUP` 里也是同一串**（唯一能证明「上线的就是拼好的 clientId」的观测点）、`unit_mode=true` 的发送让自动建出的 topic 带 `UNIT` 位而对照组不带、心跳里的 `ConsumerData.unit_mode` 让 `%RETRY%group` 带 `UNIT_SUB` 位、stream 生产者与 lite 消费者（默认开）每个请求带 `ReqT=0` 时收发照常 |
| `live_sql92` | 15 PASS | S1~S4（与 Python/C++/.NET 同场景）：SQL92 订阅启动时正好一笔 `CHECK_CLIENT_CONFIG`(46)、body 的 `clientId`/`group`/`subscriptionData` 逐字段对得上，纯 TAG 订阅一笔都不发 → 消费者**先起来再发** 6 条，`color='red'` 只收那 3 条 red、blue 一条没漏进来（broker 真在按属性过滤，不是放行全部），`'*'` 对照组收全 6 条，永不匹配的 `color='green'` 收 0 条 → 语法错的表达式让 `start()` 秒回 broker 的 `SUBSCRIPTION_PARSE_FAILED(23)` 并就地回滚（换个合法表达式能重新 `start()`）。协议形状与四条分支语义另有离线单测 9 项（`client::mq_client`：真 socket mock broker，含 Java 那个「订阅集合里有空 subscriptions 就整轮 `return` 而非 `continue`」的短路怪癖） |
| `live_backpressure` | 30 PASS | B1~B5（Java `executeAsyncMessageSend` 的两个公平闸 + 有界发送队列，真机版）：B1 默认容量 1024 条 / 100MiB 字节、40 笔并发异步发送全落 broker 且两个闸满额归还；B2 `minAsyncResendNum=10` 时**恰好**第 11、12 笔回调 `send message tryAcquire semaphoreAsyncNum timeout`（文案逐字对 Java）、闸等到 150ms 预算耗尽才报（不是看一眼就拒）、broker 上只落那 10 条、被拒的两笔**一笔都没进发送内核**（钩子计数 0）；B3 第 11 笔卡在闸上（**由看门 OS 线程在条数闸读到 0 的瞬间才补交**，否则池子里的任务和它会争公平闸的先后、断言就是假的），由**另一个 OS 线程**在 1000ms 时刻把容量抬到 12 才放它过去（钩子时间戳证明放行时刻 ∈ [1000, 2500)ms，早于在途那 10 笔归还），一共落 21 条、空闲许可 = 新容量 12；B4 一笔 600KB 在途精确扣掉 614400 个字节许可（剩 434176），超限两笔回调 `...semaphoreAsyncSize timeout`、只落 1 条，且**字节闸没过时已拿到的条数许可照样归还**；B5 配置越界被夹到地板值 10 条 / 1MiB，开关关掉时 30 笔并发（含 3 笔 300KB）全部落地、两个闸一分未动 |
| `live_async_send` | 47 PASS / 0 FAIL（2026-09-23 实测） | A1~A6（异步发送内核，与 `python/verify_async_send_live.py`、`cpp/examples/live_async_send.cpp`、.NET `async-send` 同场景）：A1 `send_async` 在准备段（`send_message_before` 睡 400ms）**之前**就返回，回调恰好一次且 SEND_OK，**用 broker 回的 `offsetMsgId` 能 `view_message` 读回原 body**、`queueOffset == 该队列 maxOffset-1`、`msgId` 是 32 位客户端 UNIQ_KEY 且与 `offsetMsgId` 不同；线程口径本端口没有 `AsyncSenderExecutor_N` / `NettyClientPublicExecutor_N` 这种池线程名，故把调用方放到 `spawn_blocking`（与 tokio 工作线程集合不相交）后用 `ThreadId` 证「before 钩子与用户回调都不在调用方线程上」→ A2 并发 30 笔：一笔恰好一个终态、全 SEND_OK、broker 落 30 条、30 个 `(broker,queueId,queueOffset)` 槽位与 30 个 UNIQ_KEY 两两不重复 → A3 定点异步发送只让指定的那条队列多 1 条、其它队列一条没多 → A4 `CheckForbiddenHook` 看到 `ASYNC`，拒绝时异常原样到回调且**连 topic 路由都没建出来**（`landed=-1`），换个标签照常落地、钩子被调 2 次 → A5 `send_batch_async`（对位 Java `send(Collection, SendCallback, timeout)`）一批 5 条：回调恰好一次且 SEND_OK、broker 侧 `landed=5`、应答的 `offsetMsgId` 是**逐条回的 5 个 commitLog 偏移**，用它 `view_message` 读回的那条**子消息**带着客户端生成的 32 位 `UNIQ_KEY`（逐条 ID 真编进了 body 的落地证据；缺了它发送侧照样 SEND_OK，只有真 broker 看得出来）、`msg_id` 是批量自身的 32 位客户端 ID；定点批量只让那条队列多 3 条；混 topic / 空批的本地校验在异步路径上照样跑且错误**进回调**；字节闸按**整批**扣（1 MiB 地板下 2×600 KiB 被拒、2×100 KiB 照常 SEND_OK、两份许可满额归还）→ A6 实测本端口与 **Java/Python 同派**：`shutdown()` 返回耗时 0ms、不等在途，交进来的 36 笔仍各拿到一个终态回调，但整轮以 `client already shutdown` 收场、broker 上一条都没落（连 topic 都没建出来，`landed=-1`）⇒「不等待真的会丢消息」，调用方要保消息得自己等回调再关；C++/.NET 那两版 join 完池子才关客户端、同一用例能落满 36 条，是它们相对 Java 的偏离。关停之后 `send_async` 同步被拒且不追加回调、新生产者照常能发（关掉的池子不会被复用） |
| `live_acl` | 需开鉴权的集群 | S1~S8：签名被 broker 接受（建 topic / 发送 / 心跳+长轮询+位点三条 RPC 全程带签名）、不带凭据与 secretKey 写错都回 `NO_PERMISSION(16)`、拉模式签名链路。前置是 broker.conf 开 `authenticationEnabled=true` + `LocalAuthenticationMetadataProvider` + `initAuthenticationUser`（本机默认集群关着，跑不了这一项） |
| `live_compression_matrix` | 矩阵一端 | 与 Java/Python/C++/.NET 探针双向收发压缩消息，由 `../scripts/compression_matrix.sh` 调度 |

上表是明文；**同一批 `live_*` 在 `ROCKETMQ_TLS_ENABLE=1` 下也整套跑过**（2026-09-22 本机 5.5.1
集群：producer 62、push consumer 95、pull 43、lite-pull 48、mq_client 84、admin 92+1 skip，
合计 **424 项断言 0 失败**），修掉的那个只有 TLS 才有的静默故障见「与 Java 的差异」。

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
└── examples/                   16 个真机联调工具（见上表，不依赖集群的没有）
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

- **clientId 口径按 Java 齐平**：`buildMQClientId` ⇒
  `<本机 IP>@<instanceName>[@<unitName>][@STREAM]`，`instanceName` 为 `DEFAULT` 时在
  `start()` 里就地改写成 `<pid>#<nanoTime>`（生产者和 CLUSTERING 消费者；广播消费者
  保持 `DEFAULT`，因此同进程的广播消费者共用一份实例，与 Java 一致）。
  `unit_name` / `unit_mode` / `enable_stream_request_type` 三项配置 producer、push、pull、
  lite、admin 五个门面都有，默认值与 Java 相同（producer/push/admin 关 stream，pull/lite 开）。
- **本机 IP 探测方式不同**：Java 枚举网卡并优先非内网 IPv4，这里用 UDP「连」公网地址后
  读 sockname（不发包），取不到退化成 `127.0.0.1`。结果通常是同一块出口网卡的地址。
- **unitMode / stream 是上线字段，不是本地摆设**（`live_unit_config` U1–U5 在真集群上验）：
  `unit_mode=true` 的发送让自动建出的 topic 带 `UNIT` 位（`AbstractSendMessageProcessor:487-497`），
  消费者心跳的 `ConsumerData.unit_mode` 让 `%RETRY%group` 带 `UNIT_SUB` 位
  （`MQClientInstance:1039` → `ClientManageProcessor:113-118`），`unit_name` 还参与动态取址
  URL 的 `-<unitName>?nofix=1` 后缀。⚠ ExtFields 的 `ReqT` 是 `RequestType.STREAM.getCode()`
  的字符串形式 `"0"`，clientId 尾巴上才是枚举 name `@STREAM`；stream 钩子必须排在 ACL 钩子
  **之前**（`MQClientAPIImpl:329-332`），否则 `ReqT` 落在签名之外，开鉴权的 broker 验签必失败。
- **broker 主动请求（220/221/307/309/326）无法从外部注入**：它们走 broker 已建立的那条
  连接。协议与分派由离线单测覆盖，`live_mq_client` 只验实例侧的 seam。
- **批量发送走 `SEND_BATCH_MESSAGE(320)`，与 Java 同判据**（`MQClientAPIImpl:562` 先判
  `isReply` 再判 `msg instanceof MessageBatch`）。服务端对 310/320 其实同路：broker 解码都走
  V2 头、由 `header.batch` 选 `sendBatchMessage`（`SendMessageProcessor:117`），proxy
  `AbstractRemotingActivity:69` 与 auth `DefaultAuthorizationContextBuilder:230-240` 把两个码
  列在同一个 case 里。所以这一项不是修 bug，是让请求码这一层也与 Java 一致。
  离线取证 `send_retry_tests::send_request_code_follows_java_three_way_branch`
  （310+`m=false` / 320+`m=true` / reply 批量仍是 325），真机取证 `live_mq_client` M3
  （批量发出后 broker 按 3 条独立消息投回、逻辑位点连续）。
- **`%DLQ%` 死信终态已在真机跑过**（`live_consumer` C4b，7 项）：`maxReconsumeTimes=2` 的
  组对同一条消息只投 3 次，实测档位 `0s / 10s / 40s`，正好是 Java 的 `delayLevel = 3 +
  reconsumeTimes`（`AbstractSendMessageProcessor:209`）；第 3 次回投被 broker 改写进
  `%DLQ%<group>`（`:193`），且存的是 `reconsumeTimes + 1 = 3`（`:228`）、`RETRY_TOPIC` 保留业务
  topic。客户端侧「用尽」的判据来自 `DefaultMQPushConsumerImpl#getMaxReconsumeTimes:890`
  的 `-1 → 16`，回投请求本身不带次数上限。观察窗口给到 150s：整机并发时定时服务会拖档。
- **异步发送的背压闸门按 Java 移植完了**（`send_async` → `execute_async_send`，
  `DefaultMQProducerImpl:122-153` + `executeAsyncMessageSend:635-682`）：开关默认关
  （`enableBackpressureForAsyncMode=false`），开了之后先拿 **1 个条数许可**
  （`semaphoreAsyncSendNum` 默认 1024、地板 10）、再按**压缩之前**的 body 长度拿**字节许可**
  （`semaphoreAsyncSendSize` 默认 100MiB、地板 1MiB，`body == null` 按 1 算），两个闸共用一条
  从调用时刻起算的预算；过不了闸就回调 `Error::TooMuchRequest`、文案与 Java 逐字一致
  （`send message tryAcquire semaphoreAsyncNum timeout` / `...semaphoreAsyncSize timeout`），
  一次请求都不会发出。归还固定在链终点、**先还字节再还条数**（Java `BackpressureSendCallBack`
  → `semaphoreProcessor:599-610`），失败路径靠 `SendPermits` 的 `Drop` 兜住，整条重试链只占
  **一份**（不是每次尝试一份）。公平性是这套闸的全部意义：`FairSemaphore` 只放行队首，
  非公平的话后到的持续流量能让先到的请求无限插队。离线 39 项（`client::backpressure` 12 +
  `send_retry_tests` 27），真机 `live_backpressure` 30 项。
- **异步发送是真内核，不再是「同步发送包一层回调」**（`send_async` → `AsyncSenderExecutor` →
  `run_async_send` → `async_send_inner` → `send_kernel_async` → `AsyncSendChain` →
  `complete_async`，对齐 Java `DefaultMQProducerImpl:541-576` + `sendKernelImpl` ASYNC 分支 +
  `onExceptionImpl`）：调用方**立即返回**，任务投进**有界**队列（`async_sender_queue_capacity`
  默认 50000，同 Java 写死的 `LinkedBlockingQueue(50000)`；并发额度 =
  `available_parallelism()`，同 Java `ThreadPoolExecutor(cores, cores)`），队满 ≡ Java
  `submit` 抛 `RejectedExecutionException` → 同步把 `executor rejected` 抛回调用方、**不走回调**；
  出队之后才算真实耗时，预算被排队吃掉直接回调 `DEFAULT ASYNC send call timeout`；请求**只建一次**，
  重试时复用同一个请求、按 Java 的做法给它换一个新 opaque；重试上限是
  `retry_times_when_send_async_failed`（默认 2，Java `DefaultMQProducer:140`），超时预算是**所有
  尝试共享**的剩余时间；broker 真返回的业务码**不进**重试链（与 Java/Python 一致，异步忽略
  `retry_response_codes`），只有传输层失败/超时才换 broker；终点固定是 `complete_async`：
  after 钩子 → 归还许可 → 用户回调**恰好一次**。`shutdown()` 按 Java 的顺序先 unregister 再
  **排空**发送队列（`shutdown()` 后不再接收新任务，但已入队的每笔仍会跑完并回调）——⚠「跑完」指的
  是跑完那条链，**不是**消息上线：实例是同一时刻拆掉的，所以关停时队列里那一笔一笔都会回调
  `client already shutdown`、broker 上一条都不落（真机 `live_async_send` A6 实测 36 笔全报错、
  `landed=-1`）。这与 Java/Python 同派，C++/.NET 那两版 join 完池子才关客户端、同一用例落满 36 条。
  要保消息就得自己等回调再关。真机口径另有 `live_async_send` A1~A6 六节（线程口径、并发不串台、
  定点、拦截钩子、批量、关停派系）。
- **异步批量入口与另外三语言同一口径**：`send_batch_async(msgs, cb, timeout, mq)` 对位 Java
  `DefaultMQProducer.send(Collection<Message>, SendCallback, long)`、Python `send_async(list)`、
  C++ `sendBatchAsync`、.NET `SendBatchAsync`（批量没有异步内核，四端口都是在发送池线程里跑完
  同步批量内核再把结果转交回调）。校验、组批、逐条 `UNIQ_KEY` 都在 `send_batch` 里做，与
  Java `batch():1172-1184` 同序。
- **上面那条「池子」为什么不是 `tokio::sync::Mutex<Receiver>` 那种一眼省事的写法**（实测过才写的）：
  互斥锁轮询队列看着等价，实际是**一次派发一份并发额度** —— 拿到锁的任务在被释放锁的那个工作线程
  本地队列上醒来，只要有一个发送钩子同步阻塞（`std::thread::sleep`）， baton 就落在那个睡着的
  线程上，`cores` 个消费者会塌成「每个钩子睡眠周期只派发一笔」。真机实测（`live_backpressure`
  B3，钩子睡 2.5s、队列里躺着 10 笔待派发）：池子**每 2505ms 才派发一笔**，后面的笔只能看着自己
  的预算被排队吃光 —— B3 早期正是这么红的。现在的形状是**一个永不阻塞的派发任务 +
  `Arc<Semaphore>`（`available_parallelism()` 份）+ 每笔任务 `spawn` 一次并持有一份许可**，
  派发方不持锁、也不等任何 worker 醒，因此阻塞钩子只占住自己那份额度。离线守卫
  `send_retry_tests::resize_wakes_the_parked_sender_while_the_pool_is_saturated`
  （32 工作线程运行时，去掉这个设计会红）。
- **上面那道闸有两处与 Java 结构性不同，都是刻意的**：① **等许可发生在 `tokio::spawn` 出去的
  发送任务里，不在调用方线程上**。Java/Python/C++/.NET 都在调用方线程 park 住 `tryAcquire`，
  所以背压打满时它们的「异步」会退化成「等满 timeout 再报错」；Rust 照做会**死锁**而不是变慢
  —— 生产者常常跑在唯一的 tokio 工作线程上（`#[tokio::test]` 的单线程运行时、
  `RuntimeFlavor::CurrentThread`），park 住那个线程就等于把**正要归还许可**的完成回调永远挡在
  队列外。预算仍从调用时刻起算，所以「多久过不了闸就报错」与 Java 一致；代价是调用方不再被闸门
  堵住，于是 Java 那句「异步队列满时有背压就地跑完」（`:675-681`）在这里变成**派发到队列之外**
  —— 那边扣许可发生在入队**之前**，不跑完就要白等超时归还；这边扣许可在出队之后，队满时一份许可
  都没扣，所以既不必阻塞调用方也不会漏容量（离线守卫
  `queue_full_with_backpressure_dispatches_off_the_queue`，队满且没开背压时对照
  `queue_full_rejects_the_caller_without_sending`）。② **改容量是在同一个信号量对象上平移
  总量**，不像 Java `DefaultMQProducer:1383-1391` 那样 `new Semaphore(num - acquired)` 换掉整个
  对象，因此既不需要那层 `ReadWriteCASLock`，也不会把正阻塞在旧对象上的等待者丢下（Java 那些
  等待者只能等到自己超时）。可观察结果一致：在途份额原样保留、空闲许可 = 新总量 − 在途。
- **`live_backpressure` 为什么把观测都放到普通线程上做**（实测环境性质，不是客户端 bug）：这台
  macOS（12 核）上 32 工作线程的 tokio 运行时里，**只要有任何任务在同步阻塞**（这里就是发送钩子
  里的 `std::thread::sleep`），**整个运行时的时间驱动都不推进** —— 实测一个任务睡 1500ms 期间，
  另外三个任务里 `sleep(100ms)` 的心跳一次都没醒，全在它睡醒那一刻集中补发。于是「在途占满」的
  采样、超限那几笔的补发、运行时扩容三件事必须由**普通 OS 线程**干，钩子进出的时刻也只能由钩子
  自己记（观察方的 `Instant` 全被冻住）。换到没有这种阻塞钩子的生产代码上不受影响。
- **TLS 已按真机跑通，并且修掉过一个只有 TLS 才有的静默故障**：`ROCKETMQ_TLS_ENABLE=1` 打本机
  5.5.1 集群（test-mode 下 nameServer 9876 与 broker 10911 按首字节嗅探 TLS，不需要改
  `useTLS`），producer / push consumer / pull / lite-pull / mq_client / admin 六个例子
  417 项断言全绿。修之前的实测是 producer **51 passed / 4 failed**，同时明文 55/0：broker 每
  30s（`transactionCheckInterval`）推来的 `CHECK_TRANSACTION_STATE(39)` 三次全被丢掉，日志里只有
  `checkTransactionState: no tokio runtime to run the transaction check`。根因在传输层：
  `connect_tls` 的读写线程是普通 std 线程，而运行时句柄是**惰性**从当前 tokio 上下文取的
  （`Inner::runtime_handle()` → `Handle::try_current()`）—— 明文路径的读循环本来就是 tokio 任务，
  顺手把句柄缓存了下来，纯 TLS 进程却一次都没进过运行时，于是 `ResponseSink::respond` 与回查
  处理器派不出任何后台任务。现在 `connect_tls`（async，必在运行时里）先把句柄取出来，读线程再
  `Handle::enter()` 包住整个读循环。离线守卫：`tls_inbound_request_is_processed_with_a_runtime_context`
  （进程内 TLS 服务端主动推一帧事务回查，断言处理器拿得到运行时**且**响应真写回对端；去掉修复会
  原样打出那句 warn 并失败）。同一条断言在真机侧由 `live_producer` P6 守着。
  另外 TLS 的 `live_producer` 里 P7 offset 那项当时也一起红了 —— 那是回查把整轮拖过 90s、
  「now-60s」基准越过了 topic 首条消息，属于该断言自己耦合运行时长，已改成以本进程开跑时刻为基准。
  其余 TLS 读写循环的回归（半包续读、1MiB body、8 路并发共线不饿死、明文端口映射成连接错误）见上表。
  ACL 仍需开鉴权的 broker 才能跑（本机集群 `aclEnable=false`）。
  SQL92 过滤不在此列：`CHECK_CLIENT_CONFIG`(46) 已接进 push consumer 的 `start()`，
  离线 9 项 + 真机 `live_sql92` 15 项都在本机 5.5.1 集群（`enablePropertyFilter=true`）跑过。

## License

Apache-2.0，与上游 RocketMQ 保持一致。
