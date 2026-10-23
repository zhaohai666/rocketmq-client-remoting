# RocketMQ Rust 客户端

> 中文 ｜ [English](README.en.md)

## 概述

`rocketmq-client-remoting` 是 Apache RocketMQ 经典 remoting 协议的 Rust 客户端 SDK：直连
NameServer + Broker，无 proxy、无本地扩展依赖、无代码生成步骤。

- 语言与工具链：Rust 2021 edition，本仓库实测 `rustc 1.95.0`。唯一在架构上能感知到的第三方
  依赖是 tokio——全部对外 RPC 都是 `async fn`；其余依赖（serde、flate2/lz4_flex/zstd、
  hmac/sha1/base64、chrono、native-tls）都是常规编解码/工具 crate。
- 协议覆盖：remoting 帧编解码、JSON 与 RocketMQ 二进制双序列化、17 段存储格式、POP 模式、
  消息轨迹、ACL 签名、TLS。
- 实测对端：RocketMQ 5.5.1 单主与主从集群；兼容 4.x / 5.x 集群（`autoCreateTopicEnable`
  等集群开关的影响见「真实集群联调」）。

## 先决条件

- Rust 工具链（`rustc` / `cargo`），edition 2021 编译即可，无额外要求。
- 联调时需要一个跑着的 NameServer(9876) + Broker(10911)，且 `autoCreateTopicEnable=true`。
- 单元测试全部离线可跑（进程内假集群/假 socket），不需要任何外部服务。

## 构建与测试

```bash
cd rust
cargo build --all-targets                       # lib + 42 个 examples
cargo test --quiet                              # 1127 passed; 0 failed; 0 ignored（~10s）
cargo clippy --all-targets --all-features -- -D warnings  # 零 warning 是硬门槛
```

单测全部内联在 `src/**/mod tests`（没有独立 `tests/` 目录）；真机联调工具在
`examples/live_*.rs`，**不进 `cargo test`**。跑一个例子：

```bash
cargo run --quiet --example live_protocol -- 127.0.0.1:9876
```

## 快速上手

所有门面共享同一套生命周期：`new / with_config → set_namesrv_addr → start().await → … →
shutdown()`。下面每个 API 名都能在 `src/` 里 grep 到。

### Producer（普通 / 顺序 / 延迟 / 事务 / 批量 / 单向 / 异步 / Request-Reply / 撤回）

```rust
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::common::message::Message;

let producer = DefaultMQProducer::new("GID_test")?;
producer.set_namesrv_addr("127.0.0.1:9876");
producer.start().await?;

let mut msg = Message::new("TopicTest", Some(b"hello"));
let result = producer.send(&mut msg, None, None).await?; // (msg, 超时, 指定队列)
producer.shutdown();
```

| 场景 | API（`DefaultMQProducer`） |
| --- | --- |
| 顺序（定点）发送 | `send_by_selector(&mut msg, &dyn MessageQueueSelector, arg, timeout)`，内置 `SelectMessageQueueByHash`；`fetch_publish_message_queues(topic)` 先看队列 |
| 延迟 / 定时 | `Message::set_delay_time_level(3)`；定时消息 `Message::set_delay_time_ms(ms)`，发送后从 `SendResult.recall_handle` 取撤回句柄 |
| 事务 | `set_transaction_listener(Arc<dyn TransactionListener>)` + `send_message_in_transaction(&mut msg, listener, arg)`；本地状态 `LocalTransactionState::{CommitMessage, RollbackMessage, Unknow}`，broker 回查走 `check_local_transaction` |
| 批量 | `send_batch(Vec<Message>, mq, timeout)`（同 topic 合并一帧）；异步版 `send_batch_async` |
| 单向 | `send_oneway(&mut msg, mq)`，不等应答 |
| 异步 | `send_async(msg, Arc<dyn SendCallback>, timeout, mq)`，回调 `on_success` / `on_exception` 恰好一次 |
| Request-Reply | `request(&mut msg, timeout, mq).await? -> MessageExt`；消费侧用 `client::request_reply::create_reply_message(&request, body)` 构造应答（应答 topic 由 `MixAll::get_reply_topic(cluster)` 给出，`is_reply_message` 判定）再经 producer 发出 |
| 撤回定时消息 | `recall_message(topic, recall_handle).await` |

使用注意：

- `shutdown()` 是同步收尾函数。在 tokio 运行时的 worker 线程里调用时无法阻塞等待收尾完成，
  收尾会被 detach 并打一条 WARN（进程若立刻退出则收尾丢失）。请在运行时之外的线程调用它，
  或让运行时先停下来。
- 异步发送的在途任务**不会**等 `shutdown()`：关停后队列里的任务只会回调
  `client already shutdown`。**要保证送达就自己等回调收齐再关。**
- 异步发送是真异步：调用立即返回，任务进入有界队列
  （`set_async_sender_queue_capacity`，默认 50000），队满时同步把 `executor rejected`
  抛回调用方，不走回调。
- 背压模式（配置字段 `enable_backpressure_for_async_mode`）下等发送许可发生在后台发送任务
  里而不是调用方线程上——调用方永不阻塞，超时预算从调用时刻起算。

### PushConsumer（并发 / 顺序 / 广播）

```rust
use rocketmq_client_remoting::client::consumer::DefaultMQPushConsumer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::MessageExt;
use std::sync::Arc;

struct MyListener;
impl MessageListenerConcurrently for MyListener {
    fn consume_message(&self, _msgs: &[MessageExt], _ctx: &mut ConsumeConcurrentlyContext)
        -> ConsumeConcurrentlyStatus
    {
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

let consumer = DefaultMQPushConsumer::new("GID_test")?;
consumer.set_namesrv_addr("127.0.0.1:9876");
consumer.subscribe("TopicTest", "*")?;                       // tag 表达式
consumer.set_message_listener_concurrently(Arc::new(MyListener));
consumer.start().await?;
```

- 顺序消费：`set_message_listener_orderly(Arc<dyn MessageListenerOrderly>)`（返回
  `ConsumeOrderlyStatus`，队列内加锁串行）。
- 广播：`consumer.update_config(|c| c.message_model = MessageModel::BROADCASTING.into())`
  （默认 `CLUSTERING`）；广播模式下位点只落本地
  `$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json`。
- SQL92 过滤：`subscribe_with_selector(topic, &MessageSelector::by_sql("a > 1"))`（需 broker
  `enablePropertyFilter=true`）；tag 用 `MessageSelector::by_tag`，`subscribe_all(topic)` 订全量。
- 起点：`set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET)`，
  或按时间戳 `set_consume_timestamp("20251001000000")`（14 位本地墙钟）。
- 运维/测试钩子：`do_rebalance`、`sync_pull_threads`、`suspend` / `resume`、
  `fetch_subscribe_message_queues(topic)`、`compute_accumulation_total()`（积压量）、
  `update_core_pool_size(n)`。

### PullConsumer（手动拉取）

```rust
use rocketmq_client_remoting::client::pull_consumer::DefaultMQPullConsumer;

let consumer = DefaultMQPullConsumer::new("GID_test")?;
consumer.set_namesrv_addr("127.0.0.1:9876");
consumer.register_topic("TopicTest");
consumer.start().await?;
let mqs = consumer.fetch_subscribe_message_queues("TopicTest").await?; // 整个 topic
for mq in &mqs {
    let offset = consumer.fetch_consume_offset(mq).await?.unwrap_or(0);
    let pulled = consumer.pull(mq, "*", offset, 32, None).await?;
    consumer.update_consume_offset(mq, pulled.next_begin_offset).await?; // 位点由调用方掌控
}
```

- 长轮询：`pull_block_if_not_found(mq, expr, offset, max, timeout)`。
- 均衡视图：`fetch_message_queues_in_balance(topic)` 只返回本实例按分配策略应得的那份。
- 按时间戳定位：`search_offset(mq, timestamp)`；失败退回投递：`send_message_back(msg, level)`。

### LitePullConsumer（poll 模型 + 队列变更监听 + 位点三张表）

```rust
use rocketmq_client_remoting::client::pull_consumer::DefaultLitePullConsumer;
use std::sync::Arc;

let consumer = DefaultLitePullConsumer::new("GID_test")?;
consumer.set_namesrv_addr("127.0.0.1:9876");
consumer.subscribe("TopicTest", "*");        // 或 assign(&[mq]) 手动指定
consumer.start().await?;
let msgs = consumer.poll(Some(3000)).await;  // 空 Vec == 超时，不报错
consumer.commit().await?;                    // auto_commit 关闭时手动提交
```

位点三张表彼此独立，各看各的：

| 表 | 语义 | 相关接口 |
| --- | --- | --- |
| 拉取游标 | 下一次从 broker 哪里拉（含被过滤跳过的消息） | `pull_cursor_of(mq)` |
| 已消费游标 | `poll()` 真正交给调用方的位置 | `seek(mq, offset)` / `seek_to_begin` / `seek_to_end` 可重钉 |
| 提交表 | `commit()` / 自动提交写出去、最终落 broker 的位点 | `committed(mq)`、`commit_offsets`、`commit_queues` |

辅助：`pause / resume`、`assignment()`、`fetch_message_queues(topic)`、
`send_heartbeat_to_all_broker()`。

队列变更监听（扩容/缩容回调）：

```rust
consumer
    .register_topic_message_queue_change_listener("TopicTest", Arc::new(MyQueueChangeListener))
    .await?; // MyQueueChangeListener: TopicMessageQueueChangeListener::on_changed(topic, mqs)
consumer.fetch_topic_message_queues_and_compare().await; // 主动触发一轮比对（联调用）
```

回调键与注册键同口径（含命名空间包装）；已启动时注册会先记一版快照，避免把当前集合误报为
变化。比对周期由 `topic_metadata_check_interval_millis` 配置。

### Admin

```rust
use rocketmq_client_remoting::client::admin::DefaultMQAdminExt;

let admin = DefaultMQAdminExt::new();
admin.set_namesrv_addr("127.0.0.1:9876");
admin.start().await?;
let route = admin.examine_topic_route("TopicTest").await?;
let stats = admin.examine_consume_stats_group("GID_test", Some("TopicTest")).await?;
admin.delete_topic("TopicTest", None).await?;
```

其余：`create_and_update_topic_config(addr, &TopicConfig)`、`create_topic`、
`fetch_all_topic_list`、`get_cluster_list(topic)`、`examine_topic_config`、
`fetch_broker_runtime_stats`、`wipe_write_perm_of_broker`、`clean_unused_topic`、
`search_offset(mq, timestamp)`、`reset_offset_by_timestamp`、`reset_offset_by_queue_id`。

### ACL

```rust
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::remoting::rpchook::{AclClientRPCHook, SessionCredentials};
use std::sync::Arc;

let hook = Arc::new(AclClientRPCHook::new(SessionCredentials {
    access_key: "AK".into(),
    secret_key: "SK".into(),
    security_token: String::new(),
}));
let producer = DefaultMQProducer::with_rpc_hook("GID_test", Some(hook))?;
```

签名内容 = extFields 按 key 字典序只拼 value（跳过 `Signature`）再拼 body，
`Base64(HmacSHA1(secretKey, content))`。四种门面的 `with_rpc_hook` / `set_rpc_hook` 同款。

### 命名空间（两套，彼此独立）

- `set_namespace("myNs")`：客户端本地资源名前缀，线上资源名变成
  `%%myNs%%TopicTest`（`remoting/protocol/namespace_util.rs`，收发 / 心跳 / 位点全线包装与
  还原）。Producer / Pull / LitePull 提供 `set_namespace`，其余门面配置字段同名 `namespace`。
- `set_namespace_v2(Some("ns"))`：服务端命名空间，每笔请求盖 `nsd=true` / `ns=<值>` 头，
  且排在 ACL 签名内容之内。生产者 / 三种消费者 / 管理端都有，`get_namespace_v2()` 读回。

### 压缩

```rust
use rocketmq_client_remoting::common::sysflag::MessageSysFlag;
producer.set_compress_type(MessageSysFlag::ZSTD_TYPE)?;      // 或 LZ4_TYPE / ZLIB_TYPE
producer.set_compress_msg_body_over_howmuch(4 * 1024);       // 正文超过即自动压缩
producer.set_compress_level(3);
```

zlib / LZ4 Frame / ZSTD 三后端；消费端按 sysFlag 压缩位自动解压，压缩发生在重试循环之外
（不会被二次压缩）。手动编解码走 `common::compression::{compress, decompress}`。

### TLS

```bash
ROCKETMQ_TLS_ENABLE=1 cargo run --quiet --example live_producer -- 127.0.0.1:9876
```

或代码侧 `producer.set_tls_enable(Some(true))`（其余门面用环境变量）。默认
`ROCKETMQ_TLS_TEST_MODE=true`（信任自签、不校验主机名）；生产部署请设为 `false` 并配好系统
信任链。

## 特性与状态

- ✅ remoting 帧 / JSON 与 RocketMQ 二进制双序列化 / 17 段存储格式 + 6 段批量格式
- ✅ 同步 / 异步 / oneway 传输：半包重组、opaque 匹配、超时、重连、GO_AWAY 换连接重发、
  连接判死快速失败
- ✅ Producer：上表九种发送形态全覆盖
- ✅ PushConsumer：长轮询 + POP（`POP_CK` / ACK / CHANGE_INVISIBLE）、顺序、广播、
  重试 / 死信、位点持久化、拉取停摆自愈、POP 的待 ack 欠账闸门（严格大于 `pop_threshold_for_queue`，默认 96，命中记进拉取路径同一个流控计数与告警节奏）
- ✅ PullConsumer / LitePullConsumer（含队列变更监听、三张位点表、pause / resume；LitePull
  解析拉取位点前先过四道缓存闸门——整个缓冲条数 `pull_threshold_for_all` 10000 / 本队列条数
  `pull_threshold_for_queue` 1000 / 本队列存储字节 `pull_threshold_size_for_queue` 100 MiB
  整除 / 本队列位点跨度 `consume_max_span` 2000，都严格大于才停；命中只重排这一个队列 50ms，
  broker 的 `FLOW_CONTROL`(215) 只等 20ms）
- ✅ 六个队列分配策略（AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM /
  MACHINE_ROOM_NEARBY），可插拔并由真实重平衡驱动
- ✅ Admin 全链路（topic / cluster / 消费统计 / 位点重置 / 锁队列 / 清理）
- ✅ ACL 签名、TLS（native-tls）、动态 NameServer 取址、故障规避选队列
- ✅ 消息轨迹（Pub / SubBefore / SubAfter / EndTransaction / Recall 编解码 + 异步分发 + 钩子）
- ✅ Request-Reply、消费侧统计、metrics、unitName / unitMode / stream
- ✅ zlib / LZ4 / ZSTD 压缩往返（真机互通见 `live_compression_matrix`）
- ⬜ Windows 目标未实测

## 客户端日志

日志默认写 `$HOME/logs/rocketmqlogs/rocketmq_rs_client.log`（取不到用户目录时退化为
`logs/rocketmqlogs/`），同时输出 stderr；按天改名轮转，历史文件带日期后缀。全部用环境变量控制：

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` | 日志目录 |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_rs_client.log` | 日志文件名 |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `WARN` / `ERROR` / `OFF`，其余按 INFO |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | 轮转保留份数 |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | 开 | 设为 `false` 只写文件 |

## 真实集群联调

前提：NameServer(9876) + Broker(10911) 在跑，`autoCreateTopicEnable=true`。统一运行方式：

```bash
cargo run --quiet --example live_xxx -- 127.0.0.1:9876
```

约定：`main()` 返回 `ExitCode`，任何一项失败进程即以非 0 退出；收口行固定为
`== summary: N passed, M failed ==`。**相当一部分场景必须"先起消费者、等订阅心跳登记到
broker 后再发消息"**（否则首批消息会因 broker 尚不认识本组而错过/多投），示例内部已按该
时序编排，手工复现时也要遵守。表中带 ⚠ 的行说明额外前置条件。

| 示例 | 验证内容 |
| --- | --- |
| `live_protocol` | 帧编解码 + 核心请求码打真实 broker 的冒烟往返 |
| `live_mq_client` | 实例复用、收发逐字段、位点五 RPC、POP 弹回-ACK-改不可见、批量锁、共用实例关闭守卫 |
| `live_pop_batch_ack` | BATCH_ACK_MESSAGE(200151) 三条腿（实测 `42 passed, 0 failed`）：**L1 一次请求清整批**——单队列 topic 发 8 条、轮询到队列真有 8 条再 POP，逐字段校验自建的 `BatchAckMessageRequestBody`（合并键 `retry@queueId@ckQueueOffset@popTime` 一项一键、位集下标 `queueOffset-ckQueueOffset` 解回来正好是本次弹出的 8 个位点、字节最小小端 base64、九项紧凑键名全在 body 上、brokerName 取首个 CK），再验 broker 回 SUCCESS 且 25s 静默窗口一条不弹回；**L2 缺一笔的对照**——发 6 pop 6 只 ack 5，只有被丢的那条弹回，弹回的 CK 重试标记为 1、其 batch body 仍写业务 topic 且每项标记 1，200151 回 SUCCESS，**ack 之后**才探 `%RETRY%<group>_<topic>` 路由（路由登记要等 broker 心跳），之后不再弹回；**L3 两轮一请求**——同队列两次各 pop 2 条，两轮合并键不同不可被折叠，一个 body 两项覆盖 4 个位点各一次、一次 SUCCESS、两轮都不弹回。200151 无需 broker 开关（请求码无条件注册）；弹回观察窗口按"首见即停 + 3s 尾巴、ack 后立即收口，之后才跑长静默"编排，任何断言都不判"哪条 RPC 应答的" |
| `live_producer` | 六条发送路径、事务两阶段+回查、撤回 recallMessage、异步内核、退出注销 35；⚠ TLS 腿需 `ROCKETMQ_TLS_ENABLE=1` |
| `live_consumer` | 长轮询 / tag 过滤 / `%RETRY%` 重投 / `%DLQ%` 死信 / 部分 ack / POP / 广播 / 顺序死信 / 显式 COMMIT-ROLLBACK / 停摆自愈 / 307 |
| `live_pull_consumer` | 手动 pull 不重不漏、位点调用方掌控、长轮询真的挂起 |
| `live_lite_pull_consumer` | subscribe / assign / seek / poll / committed、auto_commit 两态、三张位点表在真机各自数出来、本队列缓存条数闸门（超标只 park 这一路，排空后续读到尾部）|
| `live_lite_pull_cursor` | 拉取游标：NO_MATCHED_MSG 跟过整段 + OFFSET_ILLEGAL 越界自愈 |
| `live_lite_pull_code` | lite 请求码 361 + lite 位；⚠ 运行时翻 broker 开关 `litePullMessageEnable`，退出前还原 |
| `live_lite_topic_queue_change` | 队列变更监听：扩容/缩容即时回调、快照推进不重复回调 |
| `live_alloc_strategy` | 六种队列分配策略真的驱动重平衡 |
| `live_rebalance_and_trace` | 真实路由上三策略端到端切分、core/max 执行器、轨迹落 broker 再解码回来 |
| `live_client_modules` | 动态取址、故障规避选队列、统计、五类钩子、轨迹过 broker、指标、request-reply |
| `live_validators` | 名字校验亚毫秒本地失败 + 寻址故障定性（10004） |
| `live_admin` | admin 全链路（含 boundaryType 双边界、`reset_offset_by_queue_id`、按组查 topic） |
| `live_admin_batch` | 批量建 topic、静态 topic、写禁配、半消息、顺序配置等 admin 报文打真实 broker |
| `live_message_request_mode` | SET_MESSAGE_REQUEST_MODE(401) 的全部载荷只在 JSON body 里、GET_ALL_MESSAGE_REQUEST_MODE(402) 回读的真机证明（实测 `PASS=10 FAIL=0`）：**L1** 生产 `set_message_request_mode` 写 POP/4 后，402 必须按这对 topic+group 建条目，且 `popShareQueueNum` 是数字 4；**L2** 同组再写 PULL/0，回读必须变成 PULL 与数字 0（第二次写覆盖第一次，而不是留下第一次的值）；**L3 对照**——四个值全塞 header、body 为空的原始 401 必须被 broker 拒绝（非 SUCCESS；实测 code=1，remark 说明请求体解不出来），且该控制组在 402 里始终缺席：header 形态改不动模式，L1/L2 的切换才是真切换。topic 显式先建（401 拒绝 broker 没有配置的 topic，那种拒绝与报文错误无法区分），订阅组交给 broker autoCreate；名字每跑一次换新 stamp，回读轮询到有答案为止，不用固定 sleep |
| `live_unit_config` | unitName / unitMode / stream：broker 侧可见的 clientId 后缀与 UNIT / UNIT_SUB 位 |
| `live_sql92` | SQL92 过滤 + CHECK_CLIENT_CONFIG(46)；⚠ 需 broker `enablePropertyFilter=true`（broker 拒 SQL92 时打印修复指引并干净退出） |
| `live_oneway_push` | oneway 实时推送：消费者先起（LAST_OFFSET），`send_oneway` 自动选队/固定队列 + 同步对照组全部实时到达 |
| `live_acl` | ACL 签名鉴权全链路；⚠ 需 `authenticationEnabled=true`，实参 `<AK> <SK>` |
| `live_acl_message_types` | 鉴权集群上的全部消息类型收发：先断言无凭据发送被拒(16)，再带签名把同步 / 异步 / 单向 / 批量 / 延迟 / 定时 / 事务（含 transactionId）/ 顺序 / Tag / Key / 用户属性逐条发出并从 broker 读回，位点提交也走签名 RPC；⚠ 需 `authenticationEnabled=true`，实参 `<AK> <SK>`（实测 `PASS=33 FAIL=0`） |
| `live_latency` | 发送延迟故障容错 S1–S5：默认关闭不记表、健康 broker 不触发隔离、成功发送后可容错、手工注入隔离走 available→reachable→普通轮询退化链、到期恢复 |
| `live_static_topic` | 静态主题 S1–S5：513 建 8 队列映射 4 物理队列、路由回读 `TopicQueueMappingByBroker`、端点表 0-3 真 broker / 4-7 `__syslo__none__`、实例把 mock 队列名重写到真实 broker（表外 id 透传）、端到端发 4 收 4 |
| `live_backpressure` | 两个公平闸 + 有界发送队列的真机闭环（含运行时扩容放行） |
| `live_async_send` | 异步内核：offsetMsgId 读回原文、并发不串台、批量异步、shutdown 不等在途 |
| `live_fail_fast` | broker 真死时在途请求毫秒级终态；⚠ 会停一次 broker 再拉起（不删 store） |
| `live_send_header` | 发送头 c/d/n 与自动建 topic 的队列数算术 |
| `live_flow_control` | 拉取前每队列三条缓存闸门（topic 预算摊到每队列）+ 启动期数值闸门 |
| `live_scheduled_intervals` | 周期任务 initialDelay / 固定速率（含位点落盘 10s 首跳） |
| `live_subscribe` | 后置订阅立即心跳（判据：300 查询在 30s 周期前就看到本组） |
| `live_pinned_guard` | 定点 topic 守卫：真路由不误拒、拒在本端且 broker 无痕、单向无守卫 |
| `live_correct_tags_offset` | NO_NEW_MSG / NO_MATCHED_MSG 空应答也把已提交位点推到 maxOffset |
| `live_offset_illegal` | OFFSET_ILLEGAL：作废在途/缓冲并按修正位点重建，修正位点立刻落盘 |
| `live_reset_offset` | 220 重置消费位点：立刻落盘 + 在途批次作废 + 队列按新位点重建 |
| `live_pull_heartbeat` | 拉模式消费者的 203/38/35；⚠ 需主从集群实参 `-- 127.0.0.1:9876 127.0.0.1:10911 [127.0.0.1:10931]` |
| `live_publish_route_master` | 发布/顺序锁/POP/位点读取四条"只认主"口径；⚠ 会停一次 master，同上主从实参 |
| `live_shutdown_race` | 立即关停 / 立即退进程的丢数据契约 |
| `live_clean_expired_msg` | 挂起 listener 的逃生口：清扫回投 `%RETRY%` 再投第二次（约 4 分钟） |
| `live_trace_bridge` | 轨迹 dispatcher 自动桥接（未显式配置通道时自挂） |
| `live_trace_default` | 轨迹默认自建 producer 真的把轨迹发到 RMQ_SYS_TRACE_TOPIC |
| `live_compression_matrix` | 压缩互通端：`send` / `recv` / `reuse` 三条腿，由仓库级 `scripts/compression_matrix.sh` 调度 |

## 目录结构

```
rust/
├── Cargo.toml
├── src/
│   ├── lib.rs                  三层出口
│   ├── error.rs                Error / Result / client_error_code
│   ├── common/                 message / message_decoder(17 段+批量) / compression /
│   │                           consistent_hash / recall_message_handle / boundary_type /
│   │                           message_const / message_type / mix_all / sysflag /
│   │                           topic_config / topic_validator / util_all / buffer / logging
│   ├── remoting/
│   │   ├── client.rs           同步 / 异步 / oneway + 半包重组 + TLS + GO_AWAY
│   │   ├── rpchook.rs          AclClientRPCHook（ACL 签名）
│   │   └── protocol/           remoting_command / headers / codes / serialize / route /
│   │                           heartbeat / body / admin_body / subscription / extra_info /
│   │                           namespace_util / ext_fields
│   └── client/
│       ├── mq_client.rs        MQClientInstance：路由发现 + 全部 RPC + 心跳
│       ├── producer.rs         DefaultMQProducer（同步/定点/批量/单向/异步/事务/请求应答）
│       ├── consumer.rs         DefaultMQPushConsumer（长轮询 + POP + 顺序 + 广播）
│       ├── pull_consumer.rs    DefaultMQPullConsumer + DefaultLitePullConsumer
│       ├── allocate_strategy.rs  六个队列分配策略
│       ├── admin.rs            DefaultMQAdminExt
│       ├── consume_executor.rs / backpressure.rs / produce_accumulator/
│       ├── hook.rs / latency.rs / consumer_stats.rs / metrics.rs / shutdown.rs
│       ├── request_reply.rs / top_addressing.rs / validators.rs / result.rs
│       └── trace.rs / trace_context.rs / trace_hook.rs / trace_dispatcher.rs
└── examples/                   42 个真机联调工具（见上表）
```

单元测试内联在 `src/**/mod tests`（需要访问 `pub(crate)` 与假时钟）；只有真机才能证明的性质
全部下沉到 `examples/`。

## License

Apache-2.0。
