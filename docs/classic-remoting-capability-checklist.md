# 经典 remoting 协议客户端能力全列表（七端真机验证依据）

参考实现树：本地构建的服务端发布版自带的经典 remoting 客户端源码
（`.../zhaohai666-rocketmq/client/src/main/**/org/apache/rocketmq/client/**`）。
本清单是「真实启动集群逐端验证」的依据：每一行能力都给出参考实现出处与各端 live 验证入口；
缺入口的端在真机矩阵里记 `N/A(无入口)` 并列为补测试的待办。

状态图例：✅ 有实现且有 live 入口 · 🟡 有实现、live 入口待补 · ❌ 未实现（真缺口）。

## 1. 协议 / 传输基座

| 能力 | 参考实现出处 | live 入口（七端） |
|---|---|---|
| RemotingCommand 编解码（JSON/ROCKETMQ 两路、17 段消息、header V1↔V2） | `remoting.protocol.*` | 各端 selfcheck / tests；cpp interop_tool ↔ python 对拍 |
| 半包重组 / opaque 配对 / sync-async-oneway | `remoting.net.RemotingClient` | 各端传输层离线测试（cpp transport、py test_transport_async 等） |
| TLS（ca_verify / mTLS） | `remoting.net.TlsHelper` | `scripts/run_{python,cpp,csharp,node}_tls_live.sh`、`php live_tls.php`、`rust live_tls`、go TLS 选项 |
| SOCKS5/4 代理 | `NettyRemotingClient` Socks5ProxyHandler | 各端 socks 单测（真机无代理则单测为准） |
| fail-fast（断连立刻判死在途请求） | `NettyRemotingClient#requestFail` | `live_fail_fast`（py/cpp/cs/rust） |
| ACL 签名（SHA1+HMAC-SHA1 RPC 钩子） | `AclClientRPCHook` | `scripts/run_acl_live.sh`、`live_acl`（py/cpp/cs/rust/go/node）、`verify_acl_live.py` |
| ACL 集群上的全部消息类型收发（七端） | `AclClientRPCHook` + `Message` 各族 | `scripts/run_acl_message_types_live.sh`：py `verify_message_types.py`、cpp `rmq_live_message_types`、cs `message-types`、rust / go / php `live_acl_message_types`、node `live_acl_message_types.ts`。每一端成对断言：无凭据必须先被拒(16)，再带签名把同步 / 异步 / 单向 / 批量 / 延迟等级 / 定时 / 事务 / 顺序 / Tag / 用户属性 / Key 逐条发出并从 broker 读回 |

## 2. 名字服务 / 路由 / 客户端实例

| 能力 | 参考实现出处 | live 入口 |
|---|---|---|
| TopAddressing（HTTP 动态取 NS 地址） | `TopAddressing` | 各端 top_addressing 单测 + `live_admin_ns`(node) |
| 路由拉取 / 缓存 / 周期刷新 / cleanOfflineBroker | `MQClientInstance#updateTopicRouteInfoFromNameServer` | 各端 producer/consumer live 的前置路径 |
| NS 容灾（粘性 + 全列表轮换） | `getAndCreateNameserverChannel` | `ns_failover_repro.py`、`live_slave_only`(node) |
| 心跳 / 注销 / consumerIdsChanged | `MQClientInstance#sendHeartbeatToAllBroker` | `live_pull_heartbeat`、`verify_consumer_heartbeat_slave_live.py` |
| 默认 topic 兜底（TBW102，仅生产者） | `MQClientInstance#tryToFindTopicPublishInfo` | 各端首次发送路径 |
| AccessChannel（LOCAL/CLOUD） | `AccessChannel` | `live_unit_config` |

## 3. 生产者

| 能力 | 参考实现出处 | live 入口 |
|---|---|---|
| 同步 / 异步 / 单向发送（含重试 2+1） | `DefaultMQProducerImpl#send*` | `live_producer` / `live_async_send`（py/cpp/cs/rust/node） |
| 消息类型（正常 / 延迟等级 / 顺序） | `MessageSysFlag` / `setDelayTimeSec` | `live_message_types`（py 用 `verify_message_types.py`）；鉴权集群上的同一套见上一行 |
| 压缩（>4K，ZLIB/LZ4/ZSTD） | `CompressionCodec` | `live_compression` / `scripts/compression_matrix.sh` |
| 批量发送（显式 batch，msgId=join UNIQ_KEY） | `DefaultMQProducerImpl#send(messages)` | `run_go_autobatch_live.sh`、`live_producer` |
| 自动攒批 ProduceAccumulator（默认 OFF） | `ProduceAccumulator` | `run_go_autobatch_live.sh`（Go 10/10 + Py 对拍） |
| 事务消息（半消息 / 提交回滚 / 回查 219） | `TransactionMQProducer` | `verify_transaction_live.py`、`live_producer`(node 事务腿)、go `live_producer` |
| MessageQueueSelector（hash/random/machineRoom） | `producer.selector.*` | `live_send_header` |
| 生产者注销（35 全 broker） | `MQClientInstance#unregisterClient` | `live_producer_unregister` |

## 4. 消费者

| 能力 | 参考实现出处 | live 入口 |
|---|---|---|
| PUSH 并发消费（重投 / 流控 / 背压 / ackIndex 退避） | `ConsumeMessageConcurrentlyService` | `live_consumer` / `verify_flow_control_live.py` / `live_backpressure` / `live_redelivery` |
| PUSH 顺序消费（锁 MQ + 分队线程） | `ConsumeMessageOrderlyService` | `verify_orderly_reconsume_live.py`、`live_pop_orderly` |
| 消息过滤（TAG / SQL92） | `FilterAPI` / broker codeSet | `live_subscribe`、`live_sql92`、`verify_correct_tags_offset_live.py` |
| POP 消费（ack / changeInvisibleTime / popDelayLevel / 8 段 CK） | `DefaultMQPushConsumerImpl` POP 族 | `live_pop`、`live_pop_consumer`、`run_go_pop_live.sh`、`php live_pop.php` |
| 主动拉取 pull（短轮询） | `DefaultMQPullConsumerImpl` | `live_pull`（七端） |
| LitePullConsumer（assign/subscribe/seek/commit） | `DefaultLitePullConsumer` | `live_lite_pull`、`live_lite_pull_code`、`live_lite_pull_cursor` |
| **PullConsumerScheduleService（定时 PullTask）** | `MQPullConsumerScheduleService` | 实现 7/7（py `client/pull_schedule_service.py`、cpp `pull_schedule_service.cpp/.h`、rust `pull_schedule_service.rs`、node `pull_schedule_service.ts`、php `PullSchedule.php`、go `pull_schedule_service.go`、cs `PullConsumerScheduleService.cs`）；live 入口 ✅cpp/cs/rust（`live_scheduled_intervals`），🟡py/go/node/php 待补（当前仅离线覆盖：py `tests/test_pull_schedule_service.py`、go `pull_schedule_service_test.go`、php 离线套件 `tests/RunClientPullSchedule.php`） |
| 广播 / 集群消费位点 | `LocalFileOffsetStore` / `RemoteBrokerOffsetStore` | `tests/test_local_offsets`、各端位点单测 |
| TopicMessageQueueChangeListener | `DefaultLitePullConsumer#registerTopicMessageQueueChangeListener` | `live_lite_topic_queue_change`（七端） |
| 位点重置（220/222） / offset illegal 恢复 | `RemoteBrokerOffsetStore` / broker 回推 | `live_reset_offset`、`live_offset_illegal` |

## 5. Request-Reply

| 能力 | 参考实现出处 | live 入口 |
|---|---|---|
| 同步 / 异步 request + 326 应答处理器 | `MQClientAPIImpl#request` 族 | `live_rr`(cpp/cs)、`verify_request_reply_live.py`、`nodeJs live_request_reply`、`php live_request_reply.php`、go `live_consumer` RR 腿 |

## 6. 管理 / 运维（MqClientAdminImpl 门面）

| 能力 | 参考实现出处 | live 入口 |
|---|---|---|
| createTopic（普通 + 静态 513 + TBW102 归属） | `MqClientAdminImpl#updateOrCreateTopic` | `live_admin`、`live_admin_batch`（七端） |
| topicStats / consumeTimeSpan / consumerConnection / topicsByConsumer | `MqClientAdminImpl` | `live_admin`（七端） |
| queryMessage（UNIQ_KEY/OFFSET）/ viewMessage | `MqClientAdminImpl#queryMessage` | `live_admin` |
| viewMessage 三门面（producer/push/pull 单腿 33 + admin 两段 uniq 兜底） | `MQAdminImpl#viewMessage:267`、`DefaultMQAdminExtImpl#viewMessage:578` | `live_admin`（七端，uniq 兜底腿全命中） |
| resetOffset / 消费位点管理 | `invokeBrokerToResetOffset` | `live_reset_offset`、`live_admin` |
| consumeMessageDirectly(309) / consumerRunningInfo(307) | `ClientRemotingProcessor` | 各端 307/309 单测 + `live_admin`（msgId 不中时各端明确报 client error，Java 此处潜在 NPE） |
| 集群信息 / deleteTopic / deleteKvConfig / deleteSubGroup | `MqClientAdminImpl` | `live_admin` |

## 7. 故障规避 / 延迟

| 能力 | 参考实现出处 | live 入口 |
|---|---|---|
| MQFaultStrategy（隔离表 / 三级退化选队列） | `latency.MQFaultStrategy` | `live_latency`、`verify_latency_live.py`、cpp/rust `live_latency` |

## 8. 钩子 / 轨迹

| 能力 | 参考实现出处 | live 入口 |
|---|---|---|
| SendMessage / ConsumeMessage / EndTransaction / CheckForbidden / FilterMessage 五类钩子 | `client.hook.*` | `live_hook`、`verify_hook_live.py` |
| 消息轨迹（AsyncTraceDispatcher + RMQ_SYS_TRACE_TOPIC） | `client.trace.*` | `live_trace`、`verify_trace_live.py`（broker 需 traceTopicEnable=true） |
| 消费侧 msgId 语义（`MessageClientExt.getMsgId()`=UNIQ_KEY parity） | `MessageClientExt.java:29-37`、`ConsumeMessageTraceHookImpl:70`、`MQClientAPIImpl:1691`、`DefaultMQPushConsumerImpl:792` | 七端统一 `clientMsgId(msg)` helper（非空 UNIQ_KEY 属性 ?: offset id）：SubBefore/SubAfter bean、send-back originMsgId、retry 兜底 ORIGIN_MESSAGE_ID 全走它；离线回归 `test_client_msg_id_parity`（py/cpp/cs/go/rs/node/php）+ 真机 trace 五腿（py/cpp/cs/rust/node）2026-11-10 全绿 |
| **OpenTracing 钩子三件套** | `trace.hook.*OpenTracingHookImpl` | ✅七端全齐（2026-11-10 复核证伪旧结论）：可插拔 Tracer SPI 三 hook php `OpenTracingHook.php`、py `client/open_tracing_hook.py:90,138`、cpp `open_tracing_hook.h:92,135,193`、cs `OpenTracingHook.cs:79,109,190`、go `client/open_tracing_hook.go:76,118,173`、rs `open_tracing_hook.rs:91`、node `open_tracing_hook.ts:66,108`；另有 W3C traceparent 消息级透传接线到发送路径（py `client/trace_context.py`、cpp `client/trace_context.h`、cs `TraceParentContext.cs`、go `client/trace_context.go`、node `client/traceparent.ts`、rust `client/trace_context.rs`） |
| recall 撤回 + 轨迹钩子 | `DefaultRecallMessageTraceHook` + RECALL 请求族 | 实现 7/7（py producer/trace、cpp `common/recall_message_handle.cpp`、go `common/recall_handle.go`、cs `Client/RecallMessageHandle.cs`、rust `common/recall_message_handle.rs`、node `common/recall_message_handle.ts`、php）；live 入口 ✅py（`verify_recall_live.py`）✅cpp（`recall_live.cpp`）✅cs（`LiveRecall.cs`）✅rust（`live_producer` P9 + `live_rebalance_and_trace`），🟡go/node/php 待补 |

## 9. 静态主题（单元化主题）

| 能力 | 参考实现出处 | live 入口 |
|---|---|---|
| createStaticTopic(513) | `ClientMetadata` / admin | `live_admin`（七端） |
| 消费者侧端点重写（endPointsTable + mock broker 名 + ack 路径） | `TopicQueueMappingUtils` | 离线单测七端全绿（2026-10-12）；真机静态主题腿七端全绿（2026-10-12，`verify_static_topic_live.py` / `live_static_topic.{rs,cpp,php}` / `LiveStaticTopic.cs` / `go examples/live_static_topic` / `nodeJs examples/live_static_topic.ts`，统一 S1-S5 场景） |

真机腿抓出的三个隐藏缺陷（均已修复）：
- go `parseTopicQueueMappingInfo` 只认 `float64`，而 wire 上 `ParseJSON` 产出
  `remoting.JSONNumber` → 真机路由的 mapping 全部解析失败、端点集为空
  （离线单测喂的是手工 float64 fixture，没暴露；已补 wire 形状回归单测）。
- node admin `createAndUpdateTopicConfig`/`createStaticTopic` 读 `config.topicName`，
  而 node `TopicConfig` 字段名是 `topic` → header topic=undefined，broker 回
  "specified topic is blank"。
- node 消费者拉取地址解析未走 `getBrokerNameFromMessageQueue` 端点重写
  （协议契约：拉取地址用端点重写名、wire header 保留
  逻辑名），静态主题逻辑队列永远拉不到。

## 10. 2026-11-11 与 Java 参考实现的能力对齐（本轮结论）

逐条比对 Java 5.x `client`/`common`/`tools` 三个模块后，**真缺口 4 项**、**证伪 2 项**
（两条是 2026-11-11 二次复核纠正的：①「有序 POP」首轮按行数误判成 P2 缺口，读方法体后确认
Java 自身即骨架 → 证伪；②「Pull `setEnableRebalance`」首轮误判成证伪，实际 Java 默认 true、
七端确实缺整套 pull 侧重平衡 → 转为真缺口并补齐。见下方判定纪律）。
每项的七端离线回归入口统一为 `capability_closeout`（py `tests/test_capability_closeout.py`、
cpp `tests/test_capability_closeout.cpp`、cs `tests/.../CapabilityCloseoutTests.cs`、
go `client/capability_closeout_test.go`、rust `pull_consumer.rs`/`admin.rs` 的 `#[cfg(test)]`、
node `test/capability_closeout_smoke.ts`、php `tests/RunCapabilityCloseout.php`）。

| 项 | Java 出处 | 结论 | 语义要点 |
|---|---|---|---|
| `ConsumeFromWhere` 六值 | `common/.../consumer/ConsumeFromWhere.java:20-29` | ✅ 七端补齐（2026-11-11） | 三个"废弃"值**不是死值**：`RebalancePushImpl#computePullFromWhereWithException:171-173` 把它们与 `CONSUME_FROM_LAST_OFFSET` 并入同一 case 分支，所以"暴露常量 + 心跳可原样序列化"即是全部行为，offset 初始化逻辑无需新分支。补齐前 py/rs/node/php 各缺 3 个、go 缺 `MAX_OFFSET`；go `client/listener.go` 的 `ConsumeFromWhereLastOffsetNow` 命名对应错了值（且无引用者），已更名为 `ConsumeFromWhereLastOffsetAndFromMinWhenBootFirst` |
| LitePull `connectBrokerByUser` / `defaultBrokerId` | `DefaultLitePullConsumer.java:411-424` → `PullAPIWrapper#recalculatePullFromWhichNode:285-293` | ✅ 七端补齐（2026-11-11） | 开关打开后每轮拉取一律问 `defaultBrokerId` 且**忽略** broker 回的 `suggestWhichBrokerId`——这是从 slave 消费的唯一入口。分两类落地：node/go 有共享 pull-api 建议表，改在 `recalculatePullFromWhichNode` 首行；py/cpp/cs/rust/php 的 lite 路径本就没有建议表（拉取直接进 `MQClientInstance`），故"关"保持历史形状=**请求不带 brokerId**（地址仍走路由解析），"开"才带 id |
| Push `setAwaitTerminationMillisWhenShutdown` | `DefaultMQPushConsumer.java:996-1002` | ✅ 七端齐（node 本轮补） | 其余六端早已有（默认 0=不等）；node 原本把 drain 预算写死 30000，现改为可配 + 保留 30000 默认（**有意 deviation**：进程立刻退出会截断 `CONSUMER_SEND_MSG_BACK(36)`，见 `shutdown()` 注释） |
| Pull `setEnableRebalance` + pull 侧重平衡 | `DefaultMQPullConsumer.java:97,503-509` → `DefaultMQPullConsumerImpl#doRebalance/tryRebalance:388-406` → `RebalanceImpl#rebalanceByTopic` → `RebalancePullImpl#messageQueueChanged:44-53` | ✅ **七端补齐（2026-11-11 二次复核，此前误判为「证伪」）** | **旧结论错在哪**：首轮看到七端都写着 "no rebalance"，就把这当成「Java 也不需要」，判为证伪。实际 `DefaultMQPullConsumer.java:97` 是 `enableRebalance = true`——Java 的 pull consumer 由共享 `RebalanceService`（`waitInterval=20000ms` / `minInterval=1000ms`）周期驱动 `tryRebalance()`，计算分配并在**变化时**回调 `MessageQueueListener.messageQueueChanged(topic, mqAll, mqDivided)`；七端缺的是**驱动**，不是 listener 接口（七端本就都有 `MessageQueueListener`，cpp 甚至有独立头文件）。落地要点：`enableRebalance=false` 时 `tryRebalance` 必须回 **true**（回 false 会让循环每 1s 狂刷）；listener 抛异常必须吞掉（Java `:49-51`，否则一条坏回调冻死全部分配）；`cid_all` 拿不到时只 warn 不回调且保持 balanced；`fetchMessageQueuesInBalance` 优先读重平衡产出的表。deviation：每 facade 自起一条循环（Java 每 client instance 共享一条），可观测行为一致 |
| Admin `setAdminExtGroup` / `getAdminExtGroup` | `DefaultMQAdminExt.java:85,105-111,706-712` | ✅ 七端补齐（七端均为新增，node 原只有构造参数） | 默认 `"admin_ext_group"`。Java 只用它做 admin 在 client 实例里的注册键 + 日志；七端 admin 不发合成 consumer 心跳，故**没有 wire 影响**，纯身份位。已按此写入各端注释，避免后来者误以为能改心跳组 |
| `MessageConst` 六个属性（`CORRECTION_FLAG`/`MQ2_FLAG`/`SHARDING_KEY`/`DLQ_ORIGIN_MESSAGE_ID`/`DLQ_ORIGIN_TOPIC`/`TRANSACTION_ID`） | `common/.../message/MessageConst.java` | ❌ **证伪，不是缺口** | 在 Java client 内**无调用者**（仅常量定义或 broker/proxy 侧使用）；reply 消息的 `PROPERTY_MESSAGE_TTL`（wire `"TTL"`）七端概念上已有 TTL 支持 |
| 有序 POP 消费（`MessageListenerOrderly` + POP） | `client/.../consumer/ConsumeMessagePopOrderlyService.java:315-324` | ❌ **证伪，不是缺口**（2026-11-11 复核，**此前误判为 P2 缺口，已更正**） | Java 5.5.1 这个类**自己就是未完成的骨架**：`ConsumeRequest#run()` 取到锁对象后**方法即结束**——没有 `synchronized (objLock)`、不调 `messageListener.consumeMessage`、不 ack；本类的 `processConsumeResult` 是 `return true` 且**无任何调用者**；4 参 `submitConsumeRequest` 直接抛 `UnsupportedOperationException`。对照同目录的真实现即可分辨：`ConsumeMessageOrderlyService:397/462/517`（`synchronized` + `consumeMessage` + `processConsumeResult(msgs,…)`）、`ConsumeMessagePopConcurrentlyService:97`（`consumeMessage`）。故七端「有序监听 + POP 收不到消息、积压不下降」是与 Java 完全一致的 parity；python/cpp 的空转骨架（`python/tests/test_pop_orderly.py` 用断言锁死"不消费不 ack"）与 go 的「不请求有序 POP」（`client/pop_consumer.go:238`）是同一行为的两种表达 |
| 7 个推送请求码（40/39/220/221/307/309/326） | `ClientRemotingProcessor` | ✅ 七端全齐（既有） | 复核确认，非缺口 |
| Admin 门面规模 | `tools/.../DefaultMQAdminExt.java`（125 个 public 方法） | ✅ 七端已覆盖（147–194 个，含内部辅助） | 未移植者仅 trivial 访问器 |

> **判定纪律**：判「Java 有、我们缺」时，**行数不是证据，必须读方法体**。上一条就是反例——
> `ConsumeMessagePopOrderlyService` 有 368 行，其中大半是 `equals`/`hashCode`/accessor/
> `consumeMessageDirectly` 样板，核心 `run()` 只有「取锁 + 返回」两行，一眼看行数会得出相反结论。

上表标记「七端补齐」的三项，除离线回归外**已对真实 5.5.1 集群跑通**（七端一次串行
`RESULT: all green`，119 断言 0 失败），矩阵与两条"别当 bug 再查一遍"的实测结论见第 12.2 节。

## 11. 已知未移植（P3 边缘，按需再补）

| Java 侧类 | 说明 | 各端现状 |
|---|---|---|
| `MQClientAPIFactory` + `common.NameserverAccessConfig` | proxy/sdk 场景的多实例 `MQClientAPIImpl` 工厂（instanceName 自动编号）+ NS 域名配置持有器；经典 remoting 直连用不到 | 七端均无，按需补 |
| `trace.TraceView` | 轨迹查看工具类，非运行时能力 | 七端均无，可忽略 |

已知**有意差异**（非缺口）：go 心跳 V2 指纹留 0 走 V1。

## 12. 真机能力矩阵验证流程

统一用「起集群 + 等端口 + 跑测试 + kill 在同一条 Bash 命令」的既有约定
（每条 live 入口脚本自身就是完整流程）：

1. 起单 broker 5.5.1 集群（namesrv 9876 + broker 10911，`autoCreateTopicEnable=true`、
   `traceTopicEnable=true`）；
2. 等集群注册完成（mqadmin clusterList 非空 + TBW102 路由可见）；
3. 按上表逐能力跑各端 live 入口（topic/组名带本次时间戳，断言按 body 过滤）；
4. 输出七端 × 能力矩阵（✅/❌/N/A），kill 集群。

### 12.1 第 10 节三项能力的真机入口

```bash
# 单端（自起集群、自等 TBW102 路由、自收尾）
bash scripts/run_capability_closeout_live.sh python
# 七端串行（共用一套集群，约 10 分钟）
bash scripts/run_capability_closeout_live.sh all
# 换端口并行跑（每端独立端口 + 独立 store，互不干扰）
ROCKETMQ_NS_PORT=9941 ROCKETMQ_BROKER_PORT=10981 RMQ_CAP_WORK=/tmp/rmq_cap_a \
  bash scripts/run_capability_closeout_live.sh all
```

探针文件：`python/verify_capability_closeout_live.py`、`go/examples/live_capability_closeout/`、
`nodeJs/examples/live_capability_closeout.ts`、`php/examples/live_capability_closeout.php`、
`rust/examples/live_capability_closeout.rs`、`cpp/examples/live_capability_closeout.cpp`、
`csharp/examples/RocketMQ.Examples/LiveCapabilityCloseout.cs`（`capability-closeout` 子命令）。
脚本按**端口**回收自己起的进程（`lsof -ti :PORT`），并发的多次运行不会互相误杀。

### 12.2 2026-11-11 实测矩阵（七端一次串行，`RESULT: all green`）

| 端 | L1 三个废弃值折叠 | L2 LitePull pin | L3 adminExtGroup | node L4 停机预算 | 断言数 |
|---|---|---|---|---|---|
| python | ✅ | ✅ | ✅ | — | 15 / 0 |
| go | ✅ | ✅ | ✅ | — | 15 / 0 |
| node | ✅ | ✅ | ✅ | ✅ | 18 / 0 |
| php | ✅ | ✅ | ✅ | — | 19 / 0 |
| rust | ✅ | ✅ | ✅ | — | 15 / 0 |
| cpp | ✅ | ✅ | ✅ | — | 17 / 0 |
| csharp | ✅ | ✅ | ✅ | — | 20 / 0 |

（"断言数" = PASS / FAIL。）

两条**必须记住的实测结论**，否则下轮会把 parity 当 bug 再查一遍：

1. **`CONSUME_FROM_LAST_OFFSET` 家族的投递条数不是折叠判据。** 同端基线为 0 或 8，
   由"该端是否显式发 `setZeroIfNotFound=false`"决定：py/node/php/rust 显式发 → broker 回
   `QUERY_NOT_FOUND` → 客户端从 maxOffset 起 → 收 **0**；java/cpp/cs/go 不带该字段 → 可能命中
   broker `ConsumerManageProcessor#queryConsumerOffset` 的 V3_0_6 兼容分支（`minOffset<=0`
   且 `checkInMemByConsumeOffset(topic,q,0,1)` 命中）→ 回 `SUCCESS offset=0` → 收 **8**。
   该分支依赖 offset 0 的页是否还在内存，**同端不同腿会翻转**：用 Java 参考客户端做 oracle，
   同一进程五腿实测为 `FIRST=8, LAST=0, MIN=8, MAX=8, BOOT=0`。所以探针只断言
   **"四个 LAST 家族值 == 同端 LAST_OFFSET 基线"**（折叠语义）+ 条数落在 `0..N`
   （不崩、不空转），绝对值只打 INFO 供记录。
2. **pin 到不存在的 slave 仍然投递**，不是洞：Java 用
   `findBrokerAddressInSubscribe(..., onlyThisBroker=false)`，目标 brokerId 不在表里就回退到
   表首 broker。断言必须落在**请求里真实携带的 brokerId**（`None`/`0`/`1`），不能落在"收不到"。

单 broker 集群没有 slave，所以 L2 只能证明"id 真的进了请求"，"从 slave 读到数据"这条腿
需要多 broker 拓扑，属已知未覆盖项。
