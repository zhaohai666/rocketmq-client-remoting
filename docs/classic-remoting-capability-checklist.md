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
| 消息类型（正常 / 延迟等级 / 顺序） | `MessageSysFlag` / `setDelayTimeSec` | `live_message_types`（py 用 `integration_live_test.py`） |
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
| **PullConsumerScheduleService（定时 PullTask）** | `MQPullConsumerScheduleService` | ✅py ✅go ✅cs；🟡cpp/node/rust/php 待补 |
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
| resetOffset / 消费位点管理 | `invokeBrokerToResetOffset` | `live_reset_offset`、`live_admin` |
| consumeMessageDirectly(309) / consumerRunningInfo(307) | `ClientRemotingProcessor` | 各端 307/309 单测 + `live_admin` |
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
| **OpenTracing 钩子三件套** | `trace.hook.*OpenTracingHookImpl` | ✅php；🟡其余六端待补 |
| recall 轨迹钩子 | `DefaultRecallMessageTraceHook` | 仅 php/node 有入口（其余端未实现 recall） |

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

## 10. 真机能力矩阵验证流程

统一用「起集群 + 等端口 + 跑测试 + kill 在同一条 Bash 命令」的既有约定
（每条 live 入口脚本自身就是完整流程）：

1. 起单 broker 5.5.1 集群（namesrv 9876 + broker 10911，`autoCreateTopicEnable=true`、
   `traceTopicEnable=true`）；
2. 等集群注册完成（mqadmin clusterList 非空 + TBW102 路由可见）；
3. 按上表逐能力跑各端 live 入口（topic/组名带本次时间戳，断言按 body 过滤）；
4. 输出七端 × 能力矩阵（✅/❌/N/A），kill 集群。

每次真机运行的矩阵结果只做本地归档，不随代码入库。
