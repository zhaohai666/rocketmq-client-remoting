# rocketmq-client-remoting (Rust)

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 Rust 实现（tokio 全异步），适配
RocketMQ 4.x / 5.x 集群，全部能力在真实 5.5.1 集群上联调验证过；与本仓库的
Python / C++ / .NET 实现逐项对齐。

分层：

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
| 压缩 | zlib / LZ4 Frame / ZSTD 三后端：生产端自动压缩 + 消费端自动解压，线上帧格式与各语言实现互通 |

**未实测**：Windows / MSVC 分支。TLS 分支已按真机跑过：本机 5.5.1 集群在 test-mode 下
**同一组端口**（nameServer 9876、broker 10911）嗅探 TLS，`ROCKETMQ_TLS_ENABLE=1` 直接生效。

## 依赖

`Cargo.toml` 里的全部依赖，**没有私有源，也不需要下载额外东西**：

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
cargo test --lib               # 891 条，~4s
```

## 单元测试

891 条按模块分布（`cargo test --lib -- --list` 可复算）：

| 模块 | 条数 | 覆盖 |
| --- | --- | --- |
| `remoting::protocol` | 109 | header 字段名守卫（错一个字母就**静默丢字段**）、`codes` 常量、管理端 body（含 **`ResetOffsetBodyForC`**：offsetTable 是 JSON **数组**，只解 map 形状的解析器对它只会得到空表，整笔重置静默丢弃）、POP `extraInfo` 8 段反构、JSON 对 fastjson2 非标准输出的容忍（裸数字键、对象 key、NaN/Infinity、尾逗号）、RocketMQ 二进制往返、`RecallMessageRequestHeader` 的 **`bname`** 键名守卫、`boundaryType`（入网是大写枚举名 `LOWER`/`UPPER`、缺键不写、宽松解析只认 "upper"） |
| `remoting::client` | 26 | 真 socket 回环：同步/异步/oneway、半包重组、并发请求各自匹配 opaque、静默超时、建连失败与坏端口、强制重连、**GO_AWAY 重连后只重发一次**、broker 推送抵达 processor、RPC 钩子在编码前执行；**5 条 TLS 离线回归**（现造自签证书 + 进程内 `TlsAcceptor`）：半包续读、1MiB body、8 路并发共用一条连接不被读写抢锁饿死、broker 推来的请求在读线程上拿得到运行时上下文并回得出包、TLS 客户端打明文端口必须映射成连接错误；**5 条连接判死**：对端读完就关（EOF）时同步请求**毫秒级**拿到 `Error::SendRequest` 而**不是**等满 30s 报 `Error::Timeout`（异步发送的重试分类按错误**类型**分流）、回调恰好一次、在途表排空、按 `conn_id` 连接身份认领（另一条连接上的在途请求原样留着）、死连接摘除后**同一地址**立刻能建新连接、`shutdown()` 给在途请求终态 |
| `remoting::rpchook` | 6 | ACL 签名：extFields 按 key 字典序、只拼 value、跳过 `Signature`、再拼 body |
| `common::message_decoder` | 38 | 17 段 / 6 段两条编码路径（切勿混用）、压缩段的 crc32（`& 0x7FFFFFFF`）、批量消息、坏数据必须拒收 |
| `common::consistent_hash` | 8 | 环：MD5 摘要**只取前 4 字节大端**、虚拟节点 key 从 `existingReplicas` 起算、`tailMap` **含端点**、越过环末尾回绕、空环返回 `None`、负虚拟节点数只在构造处报错 |
| `common::compression` | 7 | 三后端往返 + 类型解析（含 `0→ZLIB` 兼容映射）；未支持类型必须抛错而不是透传压缩字节 |
| `common::recall_message_handle` | 6 | 定时消息撤回句柄 v1 真值向量（带 `=` 填充）、无填充句柄也能解（跨客户端撤回）、6 段新版本忽略尾段、空串/坏 base64/非法 utf-8/`v2`/段数不足一律 `recall handle is invalid` |
| `common::boundary_type` | 2 | 边界枚举的大写入网名与 `get_type` 宽松解析 |
| `common` 其余 | 75 | `message` / `message_const` / `message_type` / `message_client_id_setter`、`mix_all`（含 `%NS%` 前缀与 clientId 口径）、`sysflag`、`util_all`（14 位墙钟、`nano_time`、`is_blank`）、`topic_config`、`topic_validator`、`buffer`、`logging` |
| `client::producer`（含 `produce_accumulator` 16 条） | 117 | 配置与生命周期（clientId 口径 `<ip>@<pid>#<nanoTime>`、重启不换、同名 instanceName 共用一份实例；**`shutdown()` 返回的那一刻就腾空 `INSTANCE_MAP`** —— 注销(35) 与实例拆解都排在 `spawn` 的任务里，登记表晚一步腾空会让同 clientId 的重启复用回正被拆的实例）；`send_retry_tests` 用**进程内 mock 集群**锁死重试分类（可重试码换 broker、不可重试码立即抛、耗尽报 `BrokersSent`、单次超时钳位、预算耗尽报 callTimeout、无路由快速失败、连接失败隔离；**寻址缺失报 10004 而不是 10005** 三条腿）；线上字段口径抓包（`k`=unitMode、`ReqT`、发送请求码 310/320/325 与 `m`=batch 三级判据、批量发送的 ID 顺序：先给每条子消息 `setUniqID` → 再给整批补一个 → 最后才 `setBody(encode())`）；**27 项异步发送内核 + 背压闸门**：闸门侧（开关关掉不碰闸、条数/字节闸文案、拒了要还已拿到的条数许可、整条重试链只占**一份**许可、闸等到预算耗尽、扩容叫醒、空 body 按 1 字节算、排队吃光预算报 send kernel timeout）、有界队列侧（队满同步抛 `executor rejected` 一笔不发、开背压派发到队列之外照常跑完）、异步内核侧（换 broker 重试且同一请求换新 opaque、重试上限、**broker 业务码不进重试链**、定点重试留同一台、钩子各跑一次、未 `start()` 同步抛且不回调）、32 工作线程饱和回归；**发送头 c/d/n** 抓包（`d=0` 原样上线、`n` 取这一笔选中的 broker、空 brokerName 整键消失）；**7 项定点 topic 守卫**（不符就拒且一笔不上线、调用方 `Message` 一个字段没动、命名空间比包装后资源名、批量走同一处守卫、异步文案从回调交一次、未 `start()` 先报状态错、**定点单向故意没有守卫**） |
| `client::backpressure` | 12 | `FairSemaphore`：只有**队首**能拿许可、阻塞的队首把后面的人按 FIFO 排住、改总量保留在途份额、改容量叫醒等待者、队首成交/超时离开都要喂后面那个等待者（漏了就是真机 5 秒死等）、丢弃的等待方不留幽灵票据、空闲许可可为负再拉回正、两个地板值（10 条 / 1MiB） |
| `client::allocate_strategy` | 30 | 六个策略：`AVG` / `AVG_BY_CIRCLE` 边界用例、四道 `check` 守卫返回**空结果**而非异常、`CONFIG` 返回副本、`CONSISTENT_HASH` 哈希环表逐格、`MACHINE_ROOM` 的机房名切分**裁尾空段**真值表、`MACHINE_ROOM_NEARBY` 同机房优先 + 无消费者机房全员共享 + resolver 空机房**抛错** |
| `client::consumer` / `pull_consumer` / `consume_executor` / `consumer_stats` | 207 | 订阅与 `MessageSelector`、**后置 `subscribe`**（`start()` 之后照收，`unsubscribe` 只删表项）、clientId 的 CLUSTERING/BROADCASTING 分岔（广播保持 `DEFAULT` 共用实例）、pull/lite 状态机、**lite 三张位点表**（拉取游标 / 已消费游标 / 内存提交表各自独立，`commit()` 只走 `poll()` 交出去的那一格；`maybe_auto_commit` 只在 `poll()` 里查、全局一个 deadline；`persist_all(scope)` 抹掉 scope 外的内存行）；core/max 两档弹性与 **5.x 默认值两侧同为 20**；**顺序消费重投闸门**（`-1` 顺序侧读成不设上限 ≠ 并发侧 16、没用尽就地 +1 挂起、用尽才回投、**只有回投失败**才继续挂起）；**120s 拉取停摆自愈**（严格大于、新循环不算、线程已退出即刻算、健康队列不动、撤走时持久化位点、POP 分支读 `lastPopTimestamp`、停机不判停摆）；**POP 循环拉取统计**（`Found` 记 RT 且打在空列表判定之前、弹到消息才记 TPS、`PollingNotFound` 两格不动）；**启动期数值闸门**（12 条区间两端各测一次、`-1` 哨兵只给两个 topic 级闸门、`pullInterval` 下界 0、min>max 严格大于、多条越界按序报第一条、`consume_timestamp` 格式真会拒）；**空应答位点修正**（无待消费无在途 + `NoNewMsg`/`NoMatchedMsg` 才推到 `next_begin_offset`，只升不降）；**OFFSET_ILLEGAL 纠错**（换修正值 → 丢队列 + 冻结 → 立刻落盘 → 唤醒 rebalance，冻结覆盖两处且持续到重建）；**220 重置位点**（只动点名的队列、队列代号 +1 让旧 ack 作废）；**拉模式消费者心跳**（假主从集群：报文形状 `CONSUME_ACTIVELY`/`subVersion=0`、首轮主从各一发、循环按周期重发、`shutdown()` 各收一发 35 且 `producerGroup` 缺席）；**FIRST_OFFSET 起点是字面量 0**（整份请求日志 `GET_MIN_OFFSET(31)` 出现 0 次，负控腿 LAST_OFFSET 必须发 `GET_MAX_OFFSET`）；**拉取游标跟随 `nextBeginOffset`**（`NO_MATCHED_MSG` 跟过整段、`OFFSET_ILLEGAL` 采纳纠正值、在途 seek 刹车——负控用闸门拉出确定性窗口）；**lite 请求码 361 + lite 位**（从收到的请求取 `code` 与 `sysFlag`，经典对照腿 11 且无 lite 位）；**清扫逃生口 14 项**（只扫本实例持有的队列、单轮 `min(size,16)`、严格大于过期、只回投失败才放回、两道队首闸门、顺序队列整支跳过、回投时刻按 containsMessage 复核） |
| 轨迹四件套 `trace` / `trace_hook` / `trace_dispatcher` / `trace_context` | 101 | Pub / SubBefore / SubAfter / EndTransaction / Recall 编解码双向、SOH/STX 文本格式、无 keys 空段容错、坏记录只跳过自己、分发器攒批/切块/防递归、W3C `traceparent` 生成与校验 |
| `client` 其余 | 144 | `mq_client`（实例表复用、心跳装配、路由缓存、**共用实例的关闭守卫**：最后一个租户退场才真拆并摘 `INSTANCE_MAP`；**退出注销 35 遍历每个 brokerId——从节点也各收一发**，与心跳只打 master 的分工一起断言；**消费者心跳同样覆盖从节点**；空白组名整个字段不上线；**220 的收包口径**：数组与 map 两种 body 都能解、处理不在读线程上做、组不匹配安静丢弃）；**5 项发布地址只认 master**（只剩从节点时解析不到而退让口径拿得到、缓存为空先刷一次路由恰好一次、本端报 `The broker[X] not exist` 而不是打到从节点换可重试码、`get_max_offset` 同口径）；`admin`（properties 文本、分页合并、**222 请求体的 ext key 名 `isForce`**——写错时 broker 侧恒为 false 静默走错分支）；`latency`、`hook`、`request_reply`、`metrics`、`top_addressing`、`result`、`validators` |
| `error` | 3 | 错误码 10001..10007（`REQUEST_TIMEOUT_EXCEPTION` 由 `Error::RequestTimeout` 带出、`CREATE_REPLY_MESSAGE_EXCEPTION` 由 `create_reply_message` 带出）与 `Display` |

## 真实集群联调

需要跑着 nameServer(9876) + broker(10911)、且 `autoCreateTopicEnable=true` 的集群。
这些工具**不进 `cargo test`**，依赖外部集群；任何一项失败进程以非 0 退出码结束，
`== summary: N passed, M failed ==` 是收口行。部分用例有额外要求（停 broker、主从集群、
broker 开关自动还原、会删自己建的 topic），脚本头注释里写明：

```bash
cargo run --example live_protocol           -- 127.0.0.1:9876
cargo run --example live_mq_client          -- 127.0.0.1:9876   # 实例复用/收发逐字段/位点五 RPC/POP 弹回-ACK-改不可见/批量锁/共用实例关闭守卫
cargo run --example live_producer           -- 127.0.0.1:9876   # 六条发送路径/事务两阶段+回查/撤回 recallMessage/异步内核（排空、并发槽位、定点、拦截钩子）/退出注销 35
cargo run --example live_consumer           -- 127.0.0.1:9876   # 长轮询/tag 过滤/%RETRY% 重投/%DLQ% 死信/部分 ack/POP/广播/顺序死信/显式 COMMIT-ROLLBACK/停摆自愈/NOTIFY 叫醒/307
cargo run --example live_pull_consumer      -- 127.0.0.1:9876   # 手动 pull 不重不漏、位点调用方掌控、长轮询真的挂起
cargo run --example live_lite_pull_consumer -- 127.0.0.1:9876   # subscribe/assign/seek/poll/committed + auto_commit 两态 + 三张位点表在真机各自数出来
cargo run --example live_alloc_strategy     -- 127.0.0.1:9876   # 六种策略真的驱动重平衡（判据：assignment == 离线用真实 mqAll/cidAll 跑同一策略的预测）
cargo run --example live_rebalance_and_trace -- 127.0.0.1:9876  # 真实路由上三策略端到端切分、core/max 执行器、轨迹记录落 broker 再解码回来
cargo run --example live_client_modules     -- 127.0.0.1:9876   # 动态取址/故障规避/统计/五类钩子/轨迹过 broker/指标/request-reply
cargo run --example live_validators         -- 127.0.0.1:9876   # 名字校验亚毫秒本地失败 + 寻址故障定性（10004）
cargo run --example live_admin              -- 127.0.0.1:9876   # admin 全链路（含 boundaryType 双边界、resetOffsetByQueueId、queryTopicsByConsumer）
cargo run --example live_unit_config        -- 127.0.0.1:9876   # unitName/unitMode/stream（broker 侧可见的 clientId 后缀与 UNIT/UNIT_SUB 位）
cargo run --example live_sql92              -- 127.0.0.1:9876   # 需 broker enablePropertyFilter=true
cargo run --example live_backpressure       -- 127.0.0.1:9876   # 两个公平闸 + 有界发送队列的真机闭环（含运行时扩容放行）
cargo run --example live_async_send         -- 127.0.0.1:9876   # 异步内核：offsetMsgId 读回原文、并发不串台、批量异步、shutdown 不等在途
cargo run --example live_fail_fast          -- 127.0.0.1:9876   # 会停一次 broker 再拉起（store 不删）
cargo run --example live_send_header        -- 127.0.0.1:9876   # 发送头 c/d/n 与自动建 topic 的队列数算术
cargo run --example live_flow_control       -- 127.0.0.1:9876   # 拉取前流控五个阈值 + 启动期数值闸门（大消息不可压缩、S1/S4 topic 必须 1 条队列）
cargo run --example live_scheduled_intervals -- 127.0.0.1:9876  # 周期任务的 initialDelay/固定速率（含位点落盘 10s 首跳）
cargo run --example live_subscribe          -- 127.0.0.1:9876   # 后置订阅立即心跳（判据：300 查询在 30s 周期之前就看到本组）
cargo run --example live_pinned_guard       -- 127.0.0.1:9876   # 定点 topic 守卫：真路由不误拒、拒在本端且 broker 无痕、单向无守卫
cargo run --example live_correct_tags_offset -- 127.0.0.1:9876  # NO_NEW_MSG/NO_MATCHED_MSG 空应答也把已提交位点推到 maxOffset
cargo run --example live_offset_illegal     -- 127.0.0.1:9876   # OFFSET_ILLEGAL：整批作废在途/缓冲消息并按修正位点重建；修正位点立刻落盘
cargo run --example live_reset_offset       -- 127.0.0.1:9876   # 220 重置消费位点：broker 推 220 后立刻落盘 + 在途批次作废 + 队列按新位点重建
cargo run --example live_pull_heartbeat     -- 127.0.0.1:9876 127.0.0.1:10911 [127.0.0.1:10931]  # 拉模式消费者的 203/38/35（主从集群）
cargo run --example live_lite_pull_cursor   -- 127.0.0.1:9876   # 拉取游标：NO_MATCHED_MSG 跟过整段 + OFFSET_ILLEGAL 越界自愈
cargo run --example live_lite_pull_code     -- 127.0.0.1:9876   # lite 请求码 361 + lite 位：运行时翻 litePullMessageEnable，退出前还原
cargo run --example live_publish_route_master -- 127.0.0.1:9876 127.0.0.1:10911 [127.0.0.1:10931]  # 会停一次 master：发布/顺序锁/POP/位点读取四条"只认主"口径
cargo run --example live_clean_expired_msg  -- 127.0.0.1:9876   # 挂起 listener 的逃生口：清扫回投 %RETRY% 再投第二次（约 4 分钟）
cargo run --example live_acl                -- 127.0.0.1:9876 <AK> <SK>   # 需开 ACL 的集群（authenticationEnabled=true）
cargo run --example live_compression_matrix -- send|recv|reuse ...       # 由 ../scripts/compression_matrix.sh 调度；reuse 腿验证 send 后调用方 body 仍是原文
```

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
│   │   ├── boundary_type.rs        时间戳查位点的边界语义（LOWER/UPPER + 宽松解析）
│   │   ├── buffer.rs / sysflag.rs / mix_all.rs / util_all.rs
│   │   ├── topic_config.rs / topic_validator.rs
│   │   ├── message_const.rs / message_type.rs / message_client_id_setter.rs
│   │   └── logging.rs              按天改名轮转 + stderr
│   ├── remoting/
│   │   ├── client.rs               同步 / 异步 / oneway + 拆包重组 + TLS + GO_AWAY
│   │   ├── rpchook.rs              AclClientRPCHook（ACL 签名）
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
│       ├── consume_executor.rs     core/max 两档执行器
│       ├── hook.rs / latency.rs / consumer_stats.rs / metrics.rs
│       ├── request_reply.rs / top_addressing.rs / validators.rs / result.rs
│       └── trace.rs / trace_context.rs / trace_hook.rs / trace_dispatcher.rs
└── examples/                   31 个真机联调工具（见上，不依赖集群的没有）
```

单元测试全部内联在 `src/**/mod tests`（没有独立 `tests/` 目录）—— 需要访问
`pub(crate)` 与假时钟；只有真机才能证明的性质全部下沉到 `examples/`。

## 几个必须知道的实现约定

**字段名与线上报文逐字一致。** broker 用 fastjson2 按属性名反序列化，字段名错一个就
**静默丢字段**（不报错、没有错误码）。`remoting/protocol/headers.rs` 里那张
`(类名, [字段名])` 表 + `serialize.rs` 的 JSON 容错就是为守住这件事而存在，
别改成"看着更自然"的命名。

**分配策略的守卫不抛异常。** `currentCID` 空串 / `mqAll` 空 / `cidAll` 空
**返回空结果**。两个例外分得很清：`MACHINE_ROOM_NEARBY` 的 resolver 给出空机房会**抛错**
（静默返回空等于把整个 topic 的队列撤走），缺参数在构造期由类型排除。

**`get_name() -> &str` 逼着名字在构造期算好。** `MACHINE_ROOM_NEARBY-<内层>` 的组合名
在 `new()` 里存成 `String`；新增装饰类时同样在构造期落盘，别返回临时值的引用。

**机房名切分的语义不是"按 @ 切开"。** 参照语义丢弃**尾部空串**，Rust 原生 split 不丢。
机房名从 clientId 里切出来时这一点直接决定 `MACHINE_ROOM` 分到哪一组（真值表在单测里，8 行）。

**退出注销 `UNREGISTER_CLIENT`(35) 打主 + 从，心跳只打 master。** 注销遍历的是
`brokerAddrTable` 的**每个 brokerId**，心跳用 master 优先选择 —— 两道分工对应
`get_all_broker_addrs()` 与 `get_route_of_all_brokers()`，别合并成一个读法。空着的那个组
槽位**整个字段不上线**（不是写空串）：broker 按 `group != null` 分派，空串会拿 `""` 去查
订阅组。超时 3000ms，单台失败只记 warn —— shutdown 路径不因网络抖动抛异常。

**压缩在重试循环之外只做一次。** 压缩若发生在重试循环**内**并就地 `setBody`，重试时会把
已压缩的 body 再压一遍（`zlib(zlib(x))`），消费端只解一层就把压缩流当正文交出去。
刻意把压缩提到循环外，是真 bug 的规避，不是风格差异。

**客户端本地校验的错误没有 `response_code`。** 只有 `check_message` 的 body 档位带
`MESSAGE_ILLEGAL(13)`；"往 `SCHEDULE_TOPIC_XXXX` 发消息"报的是**无码**错误 —— 上层按
`response_code` 分支时必须知道，四语言保持一致。

**非测试路径不用 `unwrap` / `expect`。** 全仓只剩一处：`rpchook.rs` 的
`Hmac::new_from_slice(...).expect("HMAC accepts any key length")`（任何长度都不会失败）。
锁中毒用 `unwrap_or_else(|e| e.into_inner())` 兜住，不给后台任务留 panic 入口。

**周期任务按固定速率推进。** 实例上那五条任务（动态 namesrv 10s/2min、路由刷新
10ms/`pollNameServerInterval`、心跳 1s/`heartbeatBrokerInterval`、位点落盘
10s/`persistConsumerOffsetInterval`、线程池巡检 1min/1min）与消费者的重平衡等待都锚在一个
`next` 截止时刻上：首跳落在 `initialDelay` 这一刻，之后每轮只睡"还差多少"到
`next + n×period`。写成"先睡 initial 再睡 period"首跳就晚一整个周期；写成"每轮睡满一个
周期"则系统定时器误差按周期叠加（macOS 上 100ms 粒度实测多给几 ms，30s 周期能量到 31s；
切片式"按 100ms 睡满 30s"误差按片累加）。真机取证 `live_scheduled_intervals`，
离线守卫 `spawn_periodic_first_tick_lands_at_initial_delay`。

**日志刻意不与其它端口同文件。** 落 `$HOME/logs/rocketmqlogs/rocketmq_rs_client.log`，
按天改名轮转。同机同文件会互相插行；更糟的是改名后其它进程仍持旧 fd，日志写进已 unlink
的 inode 而静默消失。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` | 取不到用户目录时退化为 `logs/rocketmqlogs` |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_rs_client.log` | 轮转后的名字带日期后缀 |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `WARN`(`WARNING`) / `ERROR` / `OFF`(`NONE`)，其余一律按 INFO |
| `ROCKETMQ_CLIENT_LOG_MAX_INDEX` | `10` | 保留份数 |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | 开 | 设为 `false` 只写文件 |
| `ROCKETMQ_TLS_ENABLE` | `false` | 打开后所有出连接走 `native-tls` |
| `ROCKETMQ_TLS_TEST_MODE` | `true` | 信任自签证书、不校验主机名 |

**clientId 口径**：`<本机 IP>@<instanceName>[@<unitName>][@STREAM]`，`instanceName` 为
`DEFAULT` 时在 `start()` 里就地改写成 `<pid>#<nanoTime>`（生产者和 CLUSTERING 消费者；
广播消费者保持 `DEFAULT`，因此同进程的广播消费者共用一份实例）。`unit_name` / `unit_mode`
/ `enable_stream_request_type` 三项配置五个门面都有（producer/push/admin 关 stream，
pull/lite 开）。本机 IP 用 UDP「连」公网地址后读 sockname 探测（不发包），取不到退化成
`127.0.0.1`。

**unitMode / stream 是上线字段**：`unit_mode=true` 的发送让自动建出的 topic 带 `UNIT` 位，
消费者心跳的 `ConsumerData.unit_mode` 让 `%RETRY%group` 带 `UNIT_SUB` 位，`unit_name`
还参与动态取址 URL 的 `-<unitName>?nofix=1` 后缀。⚠ ExtFields 的 `ReqT` 是 stream 请求码的
字符串形式 `"0"`，clientId 尾巴上才是枚举名 `@STREAM`；stream 钩子必须排在 ACL 钩子
**之前**，否则 `ReqT` 落在签名之外，开鉴权的 broker 验签必失败。

**批量发送走 `SEND_BATCH_MESSAGE(320)`**：先判 reply（325）再判批量。服务端对 310/320 其实
同路：broker 解码都走 V2 头、由 header 的 `batch` 位选 `sendBatchMessage` —— 码与 `m` 位
是两件事，必须成对取证（离线 `send_request_code_follows_java_three_way_branch`，
真机 `live_mq_client` M3）。

**异步发送是真内核，不是「同步发送包一层回调」**：调用方立即返回，任务投进**有界**队列
（`async_sender_queue_capacity` 默认 50000；并发额度 = `available_parallelism()`），队满
同步把 `executor rejected` 抛回调用方、不走回调；出队之后才算真实耗时，预算被排队吃掉直接
回调 `DEFAULT ASYNC send call timeout`；请求**只建一次**，重试复用同一请求、换新 opaque；
上限 `retry_times_when_send_async_failed`（默认 2），超时预算所有尝试**共享**；broker 真返回
的业务码**不进**重试链（异步忽略 `retry_response_codes`），只有传输层失败/超时才换 broker；
终点固定是：after 钩子 → 归还许可 → 用户回调**恰好一次**。`shutdown()` 先 unregister 再
**排空**发送队列，⚠ 但实例同一时刻拆掉，关停时队列里的任务跑完链只会回调
`client already shutdown`、broker 上一条都不落（真机 `live_async_send` A6 实测 36 笔全报错、
`landed=-1`）—— **要保消息就得自己等回调再关**（C++/.NET 那两版会 join 完池子才关客户端，
是它们与这里的结构性差异）。

**发送池的形状是「永不阻塞的派发任务 + `Arc<Semaphore>`（核数份）+ 每笔任务 spawn 一次持
一份许可」。** 别改成 `tokio::sync::Mutex<Receiver>` 轮询：一次派发一份并发额度，只要有
发送钩子同步阻塞，baton 就落在睡着的线程上，核数个消费者会塌成「每个钩子睡眠周期只派发一
笔」（真机实测钩子睡 2.5s、队列躺 10 笔时每 2.5s 才派发一笔）。离线守卫
`resize_wakes_the_parked_sender_while_the_pool_is_saturated`（32 工作线程运行时）。

**背压闸门两处结构性差异都是刻意的**：① **等许可发生在 `tokio::spawn` 出去的发送任务里，
不在调用方线程上**。另三个端口都在调用方线程 park 住 `tryAcquire`，背压打满时它们的
「异步」退化成「等满 timeout 再报错」；Rust 照做会**死锁**而不是变慢 —— 生产者常跑在唯一的
tokio 工作线程上，park 住它就等于把正要归还许可的完成回调永远挡在队列外。预算仍从调用时刻
起算，「多久过不了闸就报错」一致；代价是队满时不是「就地跑完」而是**派发到队列之外**（这边
扣许可在出队之后，队满时一份许可都没扣，既不必阻塞调用方也不会漏容量）。② **改容量是在
同一个信号量对象上平移总量**（在途份额原样保留、空闲许可 = 新总量 − 在途），不换对象，
因此不会把正阻塞的等待者丢下。

**TLS 读线程在 `Handle::enter()` 里跑整个读循环。** 纯 TLS 进程可能从头到尾没进过 tokio
运行时，而运行时句柄若惰性从当前上下文取就会拿到空 —— broker 推来的
`CHECK_TRANSACTION_STATE(39)` 事务回查全部派不出任务、静默丢弃（日志只有一句 warn，真机实测
51 passed / 4 failed 而明文全绿）。修复后 `connect_tls`（必在运行时里）先取句柄、读线程
`Handle::enter()` 包住整个循环；离线守卫
`tls_inbound_request_is_processed_with_a_runtime_context`，真机 `live_producer` P6。

**心跳有一处刻意遗留**：实例级周期心跳之外，push 消费者仍保留一条自持心跳循环（首跳 30s、
周期 `heartbeat_interval_millis`）⇒ 真机上每 30s 会发两份内容一致的心跳，只是多一份流量
（删它要动 `heartbeat_enabled` 门面语义，收益不抵风险）。⚠ 拉模式消费者的自持循环**不属于**
这份遗留：它不进实例的 `consumer_table`，而实例级心跳只汇总那张表 ⇒ 没有自己的循环时
broker 上根本没有本组（`live_pull_heartbeat` 验的就是这件事）。位点落盘那份重复的**已删**
（真机抓到它抢在 initialDelay 之前落盘且无视配置周期）。

**broker 主动请求（220/221/307/309/326）无法从外部注入**：它们走 broker 已建立的那条连接。
协议与分派由离线单测覆盖，`live_mq_client` 验实例侧的接缝。

## License

Apache-2.0。
