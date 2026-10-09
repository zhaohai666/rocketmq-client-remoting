# PHP 客户端 vs Java 客户端 —— 差异对比（修订版）

> 2026-10-09 修订（第三轮：压缩三后端 + 客户端日志 + request-reply 应答方）。
> 本仓库 `php/` 已落地（参考 `python/` 蓝本 + Java `zhaohai666-rocketmq` 4.x remoting 客户端）。
> 全部数字为实测：`php/src` **69 文件 / 30,043 行**，`php/tests` **14 文件 / 11,938 行**；
> Java `client/` 161 文件 / 33,112 行。

## 0. 对比对象

| 客户端 | 路径 | 协议 | 体量 | 验证状态 |
|---|---|---|---|---|
| **PHP** | `rocketmq-client-remoting/php` | 4.x remoting（自研 TCP + JSON/ROCKETMQ 双序列化），零 composer 依赖 | src 69 文件 / 30,043 行 | 离线 **1590** 项全绿；真机 生产 9/9 + 消费 11/11 + redelivery 26/26 + admin 23/23 + POP 20/20 + TLS 9/9 + request-reply + 压缩三 codec 全 `match=1` |
| **Java 4.x remoting** | `jingsai/roocketmq/zhaohai666-rocketmq/client` | 4.x remoting | 161 文件 / 33,112 行 | 上游原生，本机 5.5.1 常用参照 |

> 背景：`~/project/zhaohai666-rocketmq-client/php`（5.x gRPC 仓库）里那份 PHP 只是「2 个 demo 脚本 + protoc 生成 stub」，
> 与本仓库无关，末尾附录保留其结论。

## 1. PHP 现状（实测）

- 分层与 python 蓝本 1:1：`src/Common` 28 文件 / 3,885 行、`src/Remoting` 15 / 7,368、
  `src/Client` 26 / 18,790。
- `php tests/run_all.php`（本机 PHP 8.1.34）：Common 238 + Remoting 196 + 客户端日志 16 +
  Client 叶子 478 + 轨迹 209 + OpenTracing 18 + 聚合器 149 + 实例 153 + 消费者 133 =
  **1590 全绿**，另跑一道**重复类名静态守卫**（PHP 没有模块隔离，同一 FQCN 在两处定义会在
  运行时随机炸，必须在离线阶段扫出来）。
- **真机冒烟（5.5.1 真实集群）**：生产侧 9/9（TBW102 自动建 topic → 同步 4 条 → oneway →
  admin 查路由 → 按 key 回读 body 一致）；**消费侧 11/11**：admin 建 topic →
  先起消费者再发消息（B5）→ Push 并发收满 8 条 → RECONSUME_LATER 全组回投 → broker 按
  delayLevel=3（≈10s）重投回来可见（按 body 记账 C3 / 不断言顺序 C2）→ LitePull subscribe 模式
  rebalance + poll → PullConsumer 短轮询拉全（suspend 位关闭 B3）+ 位点提交。
- **客户端日志（2026-10-09 对齐）**：默认写 `<cwd>/logs/rocketmqlogs/rocketmq_php_client.log`，
  六个 `ROCKETMQ_CLIENT_LOG_*` 变量与 Go / Node 端口同名同语义，按大小轮转 + 固定备份窗口，
  由 `php/tests/RunClientLogger.php` 的 16 项离线断言锁死。
  刻意**不写 `$HOME`**（Python 同口径）：PHP 端口多用于一次性脚本与 CI。
- ⚠ 环境观察：本机每个 `php` 进程启动都会打两行 `[CQ_POLLER] Background thread created/started`（连 `php -r` 都有），疑似 php.ini auto_prepend 或全局 bootstrap，排查日志噪声时先排除它。

## 2. 能力覆盖矩阵（vs Java `client/`）

| 能力 | Java | PHP | 说明 |
|---|---|---|---|
| RemotingCommand（JSON + ROCKETMQ 二进制两路） | ✅ | ✅ | 含 fastjson2 容错解析（数字裸键/内联对象键/NaN） |
| 传输层 sync/async/oneway、opaque 匹配、半包 | ✅ | ✅ | async = 非阻塞写 + pending 表 + `stream_select` 泵（PHP 单线程模型）；在途请求超时扫描 `ScanExpiredRequest` |
| RPC 钩子 / ACL 签名 | ✅ | ✅ | `AclClientRPCHook`，content 拼 key 字典序只拼 value 跳 Signature |
| 命名空间钩子（`nsd` / `ns`） | ✅ | ✅ | 组合顺序 Namespace → Stream → ACL，签名必须覆盖前两者 |
| 生产者 sync/async/oneway/selector/批量/accumulator | ✅ | ✅ | 含 `ProduceAccumulator`、背压（`Backpressure.php`） |
| 压缩（阈值 4096、防二次压缩、解压清 flag） | ✅ | ✅ | **三后端**：zlib = `gzcompress/gzuncompress`（RFC1950）；LZ4 = 纯 PHP **LZ4 Frame**（frame 内块即 block-format）；ZSTD = 优先 `zstd` CLI（proc_open，真压缩率 + 能解 Compressed 块），CLI 缺失退回纯 PHP Raw/RLE 帧。未支持类型抛异常、绝不透传压缩字节 |
| 事务（`TransactionMQProducer` + 回查 39） | ✅ | ✅ | 回查处理器内联执行（Python 起线程） |
| Recall 撤回（370） | ✅ | ✅ | `RecallMessageHandle` + 请求/响应头 + 撤回轨迹钩子 |
| Request-Reply（326） | ✅ | ✅ | `RequestFutureHolder` / `RequestResponseFuture`；**应答方入口已落地**（收到 326 推来的请求 → 回调 → 回 327），`live_request_reply.php` 真机往返 |
| 延迟容错 `MQFaultStrategy` | ✅ | ✅ | `Latency.php` + `FaultItem`；可达性探测按 `tick()` 推进（见下方线程行） |
| Admin `DefaultMQAdminExt` | ✅ | ✅ | **110** 个方法（route/topic/订阅组/KV/stats/消息查询…） |
| 消息轨迹（Pub/SubBefore/SubAfter/EndTransaction） | ✅ | ✅ | dispatcher + 三类钩子 + W3C `traceparent` 传播 + OpenTracing duck-type hook |
| POP 原语（POP/ACK/ChangeInvisibleTime，实例层） | ✅ | ✅ | `MQClientInstance.popMessage/ackMessage/changeInvisibleTime` |
| Lite pull 原语（361） | ✅ | ✅ | `LITE_PULL_MESSAGE` 已在实例层 |
| 客户端处理器 39/40/220/221/307/309/326 | ✅ | ✅ | 实例层注册 6 个 + 生产者注册 39 |
| 分配策略（Averagely/Circle/ByConfig/MachineRoom/Nearby/ConsistentHash） | ✅ | ✅ | 6 种 + `AllocationHelper` |
| 消费执行语义（并发/顺序 context、消费统计） | ✅ | ✅ | `ConsumeExecutor`（Java ThreadPoolExecutor 等价物）+ `ConsumerStatsManager` |
| POP 消费循环 / LitePull 双游标引擎 | ✅ | ✅ | Push 内嵌 POP 段（popRound/ack/changeInvisibleTime，照抄上游 POPTODO 骨架）；LitePull 三游标（拉取/已消费/提交）+ seek/pause/resume |
| **`DefaultMQPushConsumer` 用户门面** | ✅ | ✅ | 并发+顺序+POP、五阈值流控、C4 位点地板、OFFSET_ILLEGAL 纠错、307/309/220/221 契约、广播本地位点文件 |
| **`DefaultMQPullConsumer` 用户门面** | ✅ | ✅ | 短/长轮询、位点 query/update/search/max/min/earliest、sendMessageBack（maxReconsumeTimes 留 null 有意差异） |
| **`DefaultLitePullConsumer` 用户门面** | ✅ | ✅ | subscribe/assign 双模式、Java 三重载 commit 合一、poll 语义（交出才推进已消费游标） |
| **`RebalanceImpl` + 定时 rebalance 服务** | ✅ | ✅ | doRebalance（先撤后建 + 初始位点解析）+ tick 20s 周期（启动无分配 2s 快重试） |
| **`ProcessQueue` / `OffsetStore`（Remote/Local）** | ✅ | ✅ | pending/inflight 双表 + `_queue_epoch` 代号作废在途 ack；集群位点走 broker、广播走 `$HOME/.rocketmq_offsets`（fastjson2 对象键格式兼容） |
| `MQPullConsumerScheduleService` | ✅（已 `@Deprecated`） | ❌（有意） | Java 侧已废弃，**七端一致不提供**，用 `DefaultLitePullConsumer` 替代 |
| `PullMessageService` / `RebalanceService` 线程模型 | ✅ | ❌（有意） | PHP 无线程，由 `tick()/pumpOnce()` 驱动替代（短轮询+空闲退避，见 PORTING.md） |
| Trace On 轨迹 topic 关停冲刷等细粒度 | ✅ | ✅/🚧 | 模型在，真机未验 |

## 3. 结论

1. **全能力对齐**：协议/传输/生产侧/Admin/轨迹/消费侧（三个消费者门面 + rebalance + ProcessQueue/
   OffsetStore 等价物 + POP/LitePull 循环）PHP 均已与 Java 客户端对齐（同协议同字段名，
   离线 1590 + 真机 redelivery 26/26、admin 23/23、POP 20/20、TLS 三腿 9/9、
   request-reply 往返、压缩 zlib/lz4/zstd 全 `match=1`）。
   P3 残留项已清零：TLS 支持 CA 校验 + mTLS（六文件透传链），OpenTracingHook 轨迹钩子
   （零依赖 duck-type tracer）已落地并离线 18/18。
2. **压缩三后端（2026-10-08/09）**：LZ4 走**帧格式**而不是裸 block——Java `Lz4Compressor` 用
   lz4-java 的 `LZ4FrameOutputStream/LZ4FrameInputStream`，Python `lz4.frame` 同规范，本端补齐
   magic/FLG/BD/HC + 块长前缀后与其他端互解；ZSTD 优先外部 `zstd` CLI（拿到真压缩率并且能解
   其他端的 Compressed 块），无 CLI 时压缩退化为 Raw/RLE 帧（wire 合法、压缩比≈1:1）、解压只认
   Raw/RLE 并在遇到 Compressed 块时**显式报错**而不是把压缩字节当正文交回。
3. **本轮消费侧移植顺带挖出 5 个真 bug**（异常类缺 use 的静默 catch 不匹配 / tagsSet 用 isset 查 list
   整批误滤 / LitePull start 死锁 / Push shutdown 顺序解锁死代码 / 订阅构造器不解析 tag）——
   已全部修复并沉淀为 PORTING.md 坑位清单。
4. **架构差异是有意的**：PHP 单线程 `tick()/pumpOnce()/waitResponses()` 代替 Java 的后台线程组
   （心跳/路由刷新/rebalance 定时任务在 `tick()` 里推进；拉取改短轮询+空闲退避；顺序挂起改
   `suspendedUntil` 表），移植约定写在 `php/PORTING.md`「异步模型」。
5. **真机矩阵已补齐（redelivery / admin / compression）**：`php/examples/` 新增
   `live_redelivery.php`（S1 %RETRY% 回投梯度 + topic 还原 / S2 死信终态两半断言 /
   S3 顺序毒消息严格 `>` 路径 / S4 部分 ack + 对照组，26/26）、`live_admin.php`
   （集群/路由/topicConfig/KV/订阅组/topicStats/位点四件套/KEYS 索引/viewMessage/
   deleteTopic，23/23）、`live_compression.php`（send/recv 双模式腿，与 Go 参数口径
   一致，**zlib / lz4 / zstd 三种 codec 全跑**），入口 `scripts/run_php_live.sh`
   （jps 自管集群，不依赖被沙箱禁用的 rmq_test_broker.sh），并在
   `scripts/compression_matrix.sh` 注册 php_* 腿——**七端互测**，非 zlib codec 下
   PHP 腿不再 SKIP。真机 5.5.1 实测：redelivery 26/26、admin 23/23、压缩
   php↔php / py↔php / php↔go 等全 `match=1`。移植过程顺带挖出 **空 map 的
   json_encode wire bug**（SubscriptionGroupConfig 与 TopicConfig 的 `attributes` 空表编成 `[]`，
   broker fastjson2 按 Map 反序列化直接抛 "expect '{', but '['"——已修，空表输出 `{}`）。
   **2026-10-09 补 `live_pull.php`**（S1 队列 / S1b `fetchMessageQueuesInBalance` 平衡视图 /
   S2 发送 + 逐队列拉全 / S3 位点提交回读，PASS=10 FAIL=0）：拉模式心跳订阅集只从
   `registerTopics` 来，脚本在 `start()` 前 `registerTopic()` 再补发心跳，才能证明 38 数得出本组。
6. **P3 收口（POP 真机腿 / TLS+CA+mTLS / OpenTracing）**：`live_pop.php`（S1 ACK 生效
   窗口 + 不提交位点 / S2 改不可见时间 → %RETRY%<group>_<topic> → retry marker=1 +
   1ST_POP_TIME 保留，20/20）、`live_tls.php`（plain_tls / ca_verify / mtls 三腿，
   9/9）、`OpenTracingHook`（duck-type tracer，carrier 以 `OT_CARRIER` 属性带内传播，
   SplObjectStorage 防重，离线 18/18）。真机排障挖出 **两个 wire/配置级坑**：
   - **401（SET_MESSAGE_REQUEST_MODE）没有 header**：字段必须走 body JSON
     （`topic/consumerGroup/mode/popShareQueueNum`，mode 缺省 PULL），放 ext_fields 时
     broker 侧 `SetMessageRequestModeRequestBody` 反序列化为 null → NPE。PHP Admin 已修；
     Go `admin_batch.go` 的同名方法同病（真机路径走 `Instance.SetMessageRequestMode`
     body 版，故 Go live 未暴露）。
   - **服务端 TLS 配置三件套**：`tls.test.mode.enable` 默认 true 时服务端**无视
     certPath** 现场生成临时自签证书（CA 校验腿必挂）；mTLS 要求客户端证书的正确开关
     是 **`tls.server.authClient=true`**——官方名字更迷惑的 `tls.client.authServer` 是
     「client 认证 server」，写它会毒到 broker→namesrv 的注册通道（client 侧开始验证
     自签证书 → 注册静默失败）。

## 4. 真机可复现命令

```bash
# 统一入口（自带集群生命周期：探测 → 自起 5.5.1 集群 + 等 broker 路由注册 → 跑 → jps 收尾）：
bash scripts/run_php_live.sh redelivery [namesrv] [legs]   # legs = all | s1,s2,s3,s4
bash scripts/run_php_live.sh admin      [namesrv]
bash scripts/run_php_live.sh pull [namesrv] [legs]          # 拉模式：队列 / 平衡视图 / 拉全 / 位点（legs = all | s1,s2,s3）
bash scripts/run_php_live.sh compression [namesrv]          # php→php 自环冒烟
bash scripts/run_php_live.sh pop [namesrv] [legs]           # legs = all | s1,s2；POP 专用集群（独占）
bash scripts/run_php_live.sh tls  [namesrv]                 # 三腿 plain_tls/ca_verify/mtls；TLS 专用集群（独占）
bash scripts/run_php_live.sh request_reply [namesrv]        # 326 应答方 + 发起方往返
# 跨语言压缩矩阵（七端注册，php_* 腿在 zlib/lz4/zstd 下都跑）：
bash scripts/compression_matrix.sh zlib        # 或 lz4 | zstd | all
# ⚠ 集群 start + 等端口 + 等路由注册（wait_broker_route.php）+ 跑验证 + kill 必须同一条命令
# （沙箱前台返回回收后台 JVM）；/bin/ps 被禁，找 pid 一律 jps。
```

覆盖（生产 9/9）：namesrv 注册等待（TBW102 路由探针）→ 同步发送 ×4（SEND_OK + 轮转队列）→ oneway →
路由检查（1 broker × 4 队列）→ Admin 按 key 回读（轮询等索引）→ body 逐条比对。

覆盖（消费 11/11）：admin 建 topic → **先起消费者等分配、再发消息**（B5）→ Push 并发收满 →
RECONSUME_LATER 回投 + broker 延迟重投可见（C2/C3）→ LitePull rebalance+poll →
Pull 短轮询拉全 + 位点提交（B3）。

---

## 附录：`zhaohai666-rocketmq-client/php`（5.x gRPC 仓库）

该 PHP 与本仓库无关：手写 120 行（`Consumer.php` 47 / `Producer.php` 73）+ protoc 生成 stub 69 文件，
12 个 RPC 齐全但客户端逻辑层为 0，0 测试，CI 只 `composer validate/install`；
且 `posix_getpid()` 在 Windows 不存在、clientId 伪造 host、端点硬编码阿里云、
`hanson/foundation-sdk` 声明未用、namespace 与 psr-4 不对齐需手动 require。
