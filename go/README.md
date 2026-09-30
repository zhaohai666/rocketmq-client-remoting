# rocketmq-client-remoting (Go)

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 Go 实现（标准库 + `net`，**零第三方依赖**），
适配 RocketMQ 4.x / 5.x 集群；与本仓库的 Python / C++ / .NET / Rust 实现逐项对齐。
**真机联调工具目前 7 个**（见「真实集群联调」），覆盖发送 / 消费 / 拉取 / 轻量拉取 /
重投与死信 / 停机竞态 / **管理端**；其余场景在另外四端有真机工具而 Go 侧尚未补，所以这里
**不宣称「全部能力都已联调」**。另有一个**不依赖集群**的离线自检
（`go run ./examples/selfcheck`，见「离线自检」）—— 它与 Python / C++ / .NET 三端的同名工具对齐，
**不是** Java 的机制（`rocketmq-client` 里没有任何自检入口，只有 `mqadmin` 侧的检查命令）。

分层：

- `remoting`：线协议（帧编解码、header、JSON 与 RocketMQ 二进制两路序列化）与长连接传输
- `common`：消息模型、17 段存储格式编解码、压缩、命名空间、常量、校验器
- `client`：`MQClientInstance`、Producer、Push/Pull/LitePull Consumer、Admin、事务、Request-Reply、消息轨迹

Go 版是同步 API（阻塞调用 + 内部 goroutine），与本仓库 Python 实现同形态。

已实现范围：

| 层 | 内容 |
| --- | --- |
| 协议层 | `RemotingCommand` 帧编解码；`CommandCustomHeader` 家族（含 V2 单字母短键 a..n）；JSON 与 RocketMQ 二进制双序列化；17 段存储格式 + 6 段批量格式 |
| 传输层 | 惰性建连 + 复用、同步 / 异步 / oneway、半包重组、opaque 匹配、超时、重连、GO_AWAY(1500)、连接断开时在途请求立即判死、broker 主动请求派发、TLS（`crypto/tls`） |
| 发送 | 同步 / 定点 / 批量 / 单向 / 队列选择器 / 异步（真内核 + 背压信号量 + 有界队列）/ 事务消息（两阶段 + broker 回查 + 单工 check 线程）/ 定时消息撤回（recallMessage 370）/ Request-Reply |
| 消费 | Push Consumer（长轮询 + 顺序 + 广播 + 位点持久化 + 启动期数值校验 + 拉取前流控 + OFFSET_ILLEGAL 冻结重建 + 220 重置位点 + **POP 模式**）、Pull Consumer（调用方持有游标 + 长轮询）、Lite Pull Consumer（**双游标引擎**：拉取游标 / 消费游标 / 内存提交表，361 + LITE 位） |
| 消费侧应答 | `GET_CONSUMER_RUNNING_INFO(307)`（三层属性 + subscriptionSet + mqTable/mqPopTable 互斥 + statusTable）、`CONSUME_MESSAGE_DIRECTLY(309)`（并发/顺序两套判定 + panic → CR_THROW_EXCEPTION）、`CONSUMER_SEND_MSG_BACK(36)`、`GET_CONSUMER_STATUS_FROM_CLIENT(221)`、`RESET_CONSUMER_CLIENT_OFFSET(220)` |
| 消费侧统计 | `ConsumerStatsManager`：五组 StatsItemSet（CONSUME_OK/FAILED_TPS、CONSUME_RT、PULL_TPS/RT），累计 + 两级采样链，快照是**差分窗口**；307 的 statusTable 就是它 |
| 队列分配 | 六个策略：`AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>`，可插拔、由真实重平衡驱动 |
| 管理端 | `DefaultMQAdminExt`：topic / 订阅组 CRUD、集群信息、消费统计、消息查询（key / uniqKey / msgId）、位点读取与 broker 侧重置、消息轨迹查询；**真机工具 `examples/live_admin` 29 项**（管理员端此前只有单测，没有真机工具 —— 第一次真机就抓到 `groupRetryPolicy` 为 nil 时序列化 panic） |
| 消息轨迹 | 客户端轨迹生产：Pub / SubBefore / SubAfter / EndTransaction / Recall 五类记录，Java `TraceDataEncoder` **逐字节**对齐（真值向量见单测）；`AsyncTraceDispatcher`（2048 有界丢弃 + 批 20 + 128K 分片 + 5s 冲刷 + 关停冲尾批）、内部生产者与 topic 前缀两道防自噬、W3C `traceparent` 注入与透传（`ROCKETMQ_TRACE_CONTEXT_ENABLE`） |
| 5.x 能力 | Request-Reply（326 holder）、撤回句柄 v1 编解码、POP 消费（200050/200051/200052 + 检查点反构）、消费侧状态与运行信息应答（221 位点表 / 307 / 309）、五类钩子、ACL 签名（`HmacSHA1`，标准库）、动态 name server 取址、故障规避选队列 |
| 离线自检 | `examples/selfcheck`：**不依赖集群**的协议层自检 10 项（JSON / 二进制双帧回环、`clientID` 键拼写、V2 单字母短键集、17 段消息回环含派生 msgId、magic-v2 超长 topic 手工夹具、6 段批量、Crc32 向量、ACL 签名注入） |
| 校验门 | `Validators` / `TopicValidator`：组名 / topic 校验在 `start()` 建客户端实例**之前**本地跑完，失败不碰网络 |

**与其它四个端口的刻意差异 —— 压缩只有 ZLIB**：标准库没有 LZ4/ZSTD，而本模块承诺零第三方
依赖，所以这两种类型**大声报 `unsupported` 错误**，绝不把压缩流当正文透传（消费端解错就是
静默垃圾）。zlib 段的线上格式与其它语言互通；注意 `scripts/compression_matrix.sh`（跨语言
压缩矩阵）目前只覆盖 python / cpp / dotnet / rust，**Go 腿还没接**。

## 构建与检查

```bash
cd go
go build ./...
go vet ./...
gofmt -l .        # 空输出才是过
go test ./...     # 535 条，~9s
go test -race ./client/   # 并发回归（lite 消费者、异步发送、位点提交地板）
```

## 单元测试

535 条测试函数全部与源码同目录（`*_test.go`），其中一部分跑在**进程内假集群**上
（`client/consumer_test.go` 里的 `clusterFixture`：真 socket 监听的假 broker + 假 name server，
脚本化应答，能锁死请求码、ext 字段名与重试分类）：

| 包 | 条数 | 覆盖 |
| --- | --- | --- |
| `client`（287） | producer 19 | 六条发送路径、重试分类（可重试码换 broker / 不可重试码立即抛 / 预算耗尽）、批量 ID 顺序、发送头 c/d/n、发送头守卫 |
| | hooks 14 | `CheckForbiddenHook` 每次尝试都调且不吞异常、`FilterMessageHook` 吞异常且后续照跑、ACL 签名拼串（key 排序、只拼 value、跳过 Signature） |
| | async 27 | 真异步内核：换 broker 换 opaque、背压信号量、有界队满同步抛、回调恰好一次、预算共享 |
| | transaction 6 | 两阶段 + 回查响应、单工 check 线程 |
| | request_reply 13 | 326 holder、Request/AsyncRequest 超时 |
| | consumer 29 | 长轮询、顺序重投闸门、流控、位点五 RPC、OFFSET_ILLEGAL、220、广播、关停在途消费 join + send-back 守卫、位点提交地板三条（部分 ack 整批提交 / 回投失败钉住位点 / 乱序批次不回跳） |
| | consumer_stats 19 | 差分窗口口径、10s/10min 两级采样、`consumeRT` 独有的 hour 回退、`consumeFailedMsgs` 取 hour sum、key 是 topic@group、拉取与消费两条记录路径 |
| | consumer_running_info 19 | **307** 空体六键与 `jstack` 未设即不出现、`mqTable`/`mqPopTable` **内联对象键按原始字节**断言、经典 vs POP 两表互斥、statusTable 含 `%RETRY%` 且取自统计管理器、processQueueInfo 14 键 / popProcessQueueInfo 3 键、**309** 并发与顺序两套判定 + `autoCommit` 在 listener 之后读 + panic→CR_THROW_EXCEPTION + 重投 topic 还原 + 两条错误臂端到端 |
| | pop 15 | 检查点 8 段反构（含 `1ST_POP_TIME`）、ACK 用 checkpoint offset、失败改不可见时间、`checkNeedAckOrDelay` 两分支、401 请求模式、`order`/`suspend` 恒在报文里 |
| | lite_pull 9 | **361 + LITE 位上线**、双游标（NO_NEW_MSG 也跟 nextBeginOffset）、Seek 丢缓冲、提交表是**清扫**不是过滤、暂停恢复闸门、订阅模式重平衡 + 关停落盘 |
| | pull_consumer 18 | 短轮询不带 SUSPEND 位、长轮询真挂起、调用方游标、sendMessageBack |
| | admin 35 | topic/组 CRUD、分页合并、222 的 `isForce` 键名、26 号 body 是 Properties 文本 |
| | instance 15 | 实例表复用、路由刷新、发布地址只认 master、注销 35 遍历主从、220/221/40 的 broker 主动请求 |
| | route 10 | 路由表发布槽位（`brokerAddrs` 裸数字键）、TBW102 兜底、unknown topic 重试窗口 |
| | offset_store 9 | 本地/broker 双表、persistAll 清扫语义 |
| | trace 30 | Java 编码器**逐字节**真值向量、解码容错（无 keys 空段、坏记录只跳过自己）、SubBefore/SubAfter 共用 requestId + contextCode 五档、traceparent 注入与校验、分发器防自噬，以及两条**进程内真集群**端到端（Pub 落轨迹 topic 且不递归 / 消费对落盘并配对） |
| `common`（111） | 111 | 17 段编解码（坏数据拒收、压缩段 crc32）、消息模型、clientId 口径、recall 句柄真值向量、namespace、sysflag 位表、ExtraInfo 8 段、校验器 |
| `remoting`（137） | 137 | 真 socket 回环（同步/异步/oneway、半包、并发 opaque、静默超时）、TLS、ACL 签名、V2 短键名守卫（错一个字母就**静默丢字段**）、JSON 容错（裸数字键、对象 key、NaN）、**fastjson2 出站写入器**（MessageQueue 内联对象键 + Java double 格式）、`CurrentVersion`/`CurrentVersionDesc` 成对守卫、心跳装配、POP 与 ClientInfo body 形状、订阅组配置（**nil `groupRetryPolicy` 必须丢键而不是 panic**） |

## 离线自检

不需要集群、也不需要跑整套单测的协议层冒烟：

```bash
cd go
go run ./examples/selfcheck
```

10 项，逐条打印 `[PASS]` / `[FAIL]`，任一项失败进程以非 0 退出码结束，收口行
`selfcheck: ALL PASS (PASS=10 FAIL=0)`。与 `python -m rocketmq selfcheck`（7 项）、
`cpp/examples/selfcheck.cpp`（3 项）、`dotnet` 的 `selfcheck` 子命令（3 项）同名同用途，
是**动真机之前**最便宜的一道门。**Rust 侧没有这个工具**（`rust/examples/` 全是需要集群的
`live_*`），它的协议层离线覆盖由 `cargo test` 单测承担 —— 所以这里是 4/5 端对齐。

覆盖：JSON 帧回环（含**不转义** `<>&`、中文 remark、extFields 全等）、ROCKETMQ 私有二进制帧回环
（并断言协议类型确实走在打包头长度的高位）、`HEART_BEAT` 的 `clientID` **键拼写**、`SendMessageRequestHeaderV2`
**恰好**是单字母键 `a..n`（多写一个长名会被 broker 静默丢弃，且只验值的话查不出来）、17 段消息回环
（含派生的 `msgId`/`offsetMsgId`）、magic-v2 超长 topic（>255B）解码、6 段批量回环、`msgId` 反解、
Crc32 标准向量、ACL 签名注入（AccessKey/SecurityToken 必须在算签名**之前**进 extFields，SecretKey 不上线）。

两处刻意做成**字节级**而非"回环一下"：magic-v2 那条是**手工拼**的 broker 帧且 properties 硬编码
（编解码都是自己的话，对称 bug 会让回环照过）；Crc32 直接钉向量 —— 因为 Java 的 `UtilAll.crc32`
返回 `(int)(value & 0x7FFFFFFF)`（砍最高位），与标准 CRC-32 差 2^31，跨语言**绝不能**直接比 crc
（只比各自 `match` 字段；解码侧 `CheckCRC` 默认关，互通才成立）。

## 真实集群联调

需要跑着 nameServer(9876) + broker(10911)、且 `autoCreateTopicEnable=true` 的集群
（停机竞态那条另外要求 `traceTopicEnable=true`，否则 `RMQ_SYS_TRACE_TOPIC` 不预建）。
这些工具**不进 `go test`**，依赖外部集群；全部自断言，任何一项失败进程以非 0 退出码结束，
收口行 `PASS=<n> FAIL=<n>`：

```bash
cd go
go run ./examples/live_producer       -ns 127.0.0.1:9876   # 六条发送路径、SendResult 形状（msgId==UNIQ_KEY / offsetMsgId / queueOffset / regionId）、事务两阶段 + broker 回查、异步内核（回调恰好一次 / 定点 / 选择器 / 批 / 背压许可归还）
go run ./examples/live_consumer       -ns 127.0.0.1:9876 -topic T -group G -expect 12 -orderly-topic T2 -orderly-expect 6
                                                          # S1 收全且不重不漏（数据由 Python 侧灌）、S2 同组第二实例收不到、S3 客户端二次 tag 过滤、S4 顺序消费、S5 双实例切分、S6 优雅注销
go run ./examples/live_pull           -ns 127.0.0.1:9876   # 手动 pull：Min/Max/SearchOffset、队头短轮询不挂起、空队尾 NO_NEW_MSG 不挂起、长轮询唤醒、调用方游标、KEYS 保留
go run ./examples/live_lite_pull      -ns 127.0.0.1:9876   # assign 模式（双游标推进 / Commit / 重启续读不重放 / Seek 重放）、subscribe 模式重平衡 + 自动提交在 broker 读回
go run ./examples/live_redelivery     -ns 127.0.0.1:9876 [-legs s1,s2,s3,s4]
                                                          # S1 %RETRY% 二次投递 + delayLevel 3 延迟梯度 + topic 还原、S2 maxReconsumeTimes=2 ⇒ 3 次投递后 %DLQ% 且 recon=3、S3 顺序毒消息走「等 broker 回投」那条 DLQ 路径、S4 ackIndex 部分 ack（已 ack 的不回投 / 位点仍整批提交 / 对照组一条不回投）
go run ./examples/live_shutdown_race  -ns 127.0.0.1:9876   # 立即关停 / 立即退进程的丢数据契约：并发回投两轮（%RETRY% → %DLQ%）、顺序挂起回投、短生命周期 trace 生产者冲尾批
go run ./examples/live_admin          -ns 127.0.0.1:9876   # 管理端 29 项：集群探活与 master 选主、topic CRUD（路由/配置/列表一致）、broker 配置与运行时 KV、KV config 写读删、订阅组 CRUD、发 8 条验 topicStats、**四个位点查询**（Max/Min/LOWER/UPPER 边界/最早存储时间）、位点写 broker 再用另一个 RPC 读回、KEYS 索引查询 + viewMessage 取正文、删 topic 后确认消失
```

脚本（**起集群 + 等端口 + 跑验证 + 收工都在同一条命令内**，别拆开跑）：

```bash
bash scripts/run_go_producer_live.sh        # Go 生产 → Python 回读
bash scripts/run_go_consumer_live.sh        # Python 生产 → Go 消费 → Python 从 broker 回读位点/注销
bash scripts/run_go_pull_live.sh            # Go 拉取
bash scripts/run_go_redelivery_live.sh      # 重投 / 死信终态 / 顺序死信 / ackIndex 部分 ack（全跑约 5~7 分钟）
bash scripts/run_go_shutdown_race_live.sh   # 停机竞态（对标 Rust 的 live_shutdown_race）
bash scripts/run_go_admin_live.sh           # 管理端（29 项，自断言；工具自己造/删 topic、订阅组、KV namespace）
```

**尚未覆盖的真机场景**（另外四端已有对应工具，Go 侧待补）：`OFFSET_ILLEGAL` 冻结重建与 220
重置位点（目前只有单测）、拉取流控五档、心跳全景（203/38、300、从节点扇出）、六个分配策略真机、
`cleanExpiredMsg` 清扫、定时/延时消息与 key 查询、Request-Reply(326)、撤回 recallMessage(370)、
ACL、TLS、SQL92、压缩跨语言矩阵的 Go 腿，
以及 **307/309 的真实 broker 往返**（`mqadmin consumerStatus -s` 走的就是这两条；目前只在
进程内假集群上验证过线形与 Oracle 一致性，没有让真 broker 主动来问过）。

## 目录结构

```
go/
├── go.mod                      module github.com/zhaohai666/rocketmq-client-remoting/go（go 1.24，零依赖）
├── common/
│   ├── message.go                  Message / MessageExt / MessageQueue
│   ├── message_decoder.go          17 段存储格式 + 6 段批量格式
│   ├── compression.go              zlib（LZ4/ZSTD 刻意 unsupported）
│   ├── recall_handle.go            定时消息撤回句柄 v1（base64url + 5 段）
│   ├── buffer.go / sysflag.go / mixall.go / util.go
│   ├── namespace.go / topic_validator.go / validators.go
│   └── logging.go                  环境变量配置的文件/标准输出日志
├── remoting/
│   ├── remoting_command.go         帧编解码
│   ├── headers.go / codes.go       请求头家族（含 V2 短键）与常量
│   ├── serialize.go                JSON 与 RocketMQ 二进制双序列化
│   ├── client.go                   长连接传输：同步/异步/oneway + 半包 + TLS + 判死
│   ├── heartbeat.go / subscription.go / bodies.go / admin_bodies.go
│   ├── acl.go / rpchooks.go        ACL 签名与 RPC 钩子
│   └── json_value.go               fastjson2 容错解析
├── client/
│   ├── instance.go                 MQClientInstance：路由发现 + 全部 RPC + 心跳 + 周期任务
│   ├── producer.go / async.go / transaction_producer.go / request_reply.go
│   ├── consumer.go / consume_service.go / process_queue.go / pool.go   push 消费者与执行器
│   ├── pull_consumer.go            DefaultMQPullConsumer
│   ├── lite_pull_consumer.go       DefaultLitePullConsumer（双游标）
│   ├── pull_api.go                 pullKernel（经典 310/361 两路）
│   ├── admin.go / admin_api.go / admin_offset.go / admin_track.go / admin_util.go
│   ├── allocate.go                 六个队列分配策略
│   ├── trace.go / trace_hook.go / trace_dispatcher.go / trace_context.go   轨迹编解码 / 钩子 / 异步分发 / traceparent
│   ├── offset_store.go / route.go / broker_api.go / hooks.go
│   ├── fault_strategy.go / semaphore.go / listener.go / send_result.go
│   └── validators 走 common
└── examples/                   selfcheck（不依赖集群）+ 7 个真机联调工具（见上）
```

## 几个必须知道的实现约定

**字段名与线上报文逐字一致。** broker 用 fastjson2 按属性名反序列化，字段名错一个就
**静默丢字段**（不报错、没有错误码）。`remoting/headers.go` 与单测的 ext 键名守卫就是为
守住这件事而存在，别改成"看着更自然"的命名。

**短轮询绝不能带 SUSPEND 位。** Go 拉消费的 `Pull()`（短轮询）不置 `FLAG_SUSPEND`：
挂起位泄漏到短轮询上，broker 会把请求扣住整个 suspend 预算，而客户端早就超时了 ——
空队列上这是**必然超时**，是这条路上最贵的坑。长轮询（`PullBlockIfNotFound`）才置位。

**lite 消费者跑双游标。** 拉取游标（PULL cursor）在**每一次**应答后都跟
`nextBeginOffset`（FOUND / NO_NEW_MSG / NO_MATCHED_MSG / OFFSET_ILLEGAL 一视同仁，
"intact" 守卫兜住被撤走的队列）；消费游标（CONSUME cursor）只在 `Poll()` 真交付时前进，
是提交的唯一来源。两游标混用就是重放或漏消费。

**提交表是清扫，不是过滤。** `persistAll` / `commitAll` 把表里**范围之外**的单元整个删掉
（Java RemoteBrokerOffsetStore："offset is not in mqs, remove it"）—— 游标持有者之外的
陈旧位点会随这次提交一起蒸发，这是刻意的 Java 语义。空表整个跳过不碰网络，`-1` 游标
不上线（"consumerOffset is -1"）。

**位点提交地板按 Java `ProcessQueue#removeMessage` 算。** 并发 ack 的目标是「缓冲里**还剩下**的
最小 offset」，缓冲被清空时退回 `queueOffsetMax + 1`；被清掉的是**已 ack 的那批**
（`consumeRequest.getMsgs()` 减掉回投失败的），所以 `ackIndex` 部分 ack 时钉住位点的是
「没被 ack、仍留在缓冲里」的那些，**不含**已经交回 broker 的尾巴。两个反例都实测过：
把回投**失败**的那些从地板里排除，位点会跨过它们（崩溃即丢）；把目标当成「本批末位 + 1」，
更高位那批先完成时位点会停在本批（真实 broker 上读回 0，而 Java 是 3）。顺序侧**不同**：
Java 走 `commit()` = 本批 `lastKey + 1`（顺序是内联消费，同队列同时只有一批在跑）。
两者混用要么漏消息要么把队列钉死。

**两条死信上限用的不是同一个比较符。** `CONSUMER_SEND_MSG_BACK(36)` 走
`AbstractSendMessageProcessor.consumerSendMsgBack`，判据是 `reconsumeTimes >= maxReconsumeTimes`；
顺序侧的「普通发送到 `%RETRY%`」走 `SendMessageProcessor.handleRetryAndDLQ`，判据是
`reconsumeTimes > maxReconsumeTimes`（**严格大于**），并且先看
`RebalanceLockManager.isLockAllExpired` —— 组还持着队列锁就直接进 `%DLQ%`，根本不排队。
另外 `RECONSUME_TIME` 的 `+1` 只有顺序侧在**客户端**加（`ConsumeMessageOrderlyService:350`），
并发侧那个 `+1` 是 **broker** 加的，客户端写的是原值。

**lite 是独立上线身份。** 请求码 361 + `FLAG_LITE_PULL_MESSAGE` sysFlag 位 + 心跳
`ConsumerData` 的 LITE 位 + `CONSUME_ACTIVELY`；心跳是消费者自持循环（不进实例的
consumer_table，实例级心跳只汇总那张表 ⇒ 没有自己的循环时 broker 上根本没有本组），
扇出到主 + 从全部地址。

**压缩在重试循环之外只做一次。** 重试循环内就地 `setBody` 的话，重试会把已压缩的 body
再压一遍（`zlib(zlib(x))`），消费端只解一层就把压缩流当正文交出去。

**分配策略的守卫返回空结果而非异常。** `currentCID` 空串 / `mqAll` 空 / `cidAll` 空
**返回空分配**；`MACHINE_ROOM_NEARBY` 的 resolver 给出空机房会**报错**（静默返回空等于
把整个 topic 的队列撤走）。

**客户端本地校验的错误没有 response_code。** 只有 `check_message` 的 body 档位带
`MESSAGE_ILLEGAL(13)`；"往 `SCHEDULE_TOPIC_XXXX` 发消息"报的是**无码**错误 —— 上层按
`response_code` 分支时必须知道，五语言保持一致。

**轨迹的防自噬有两道闸门，缺一不可。** 内部分发生产者自己的 `enableTrace=false`，
且轨迹钩子跳过 topic 前缀是轨迹 topic 的消息 —— 只留一道，轨迹消息会给自己的轨迹再
产生轨迹，流量指数放大。轨迹分发器在 `Shutdown()` 里**最后**关并且会冲掉尾批（队列里
刚攒下的记录要等真的落 broker 才返回），否则短命客户端 / 立刻退出的进程丢最后一条。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `TRACE` / `DEBUG` / `INFO` / `WARN` / `ERROR` |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` | 日志目录 |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_client.log` | 日志文件名 |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | 空 | 任意非空值 = 写标准输出而不是文件 |
| `ROCKETMQ_SERIALIZE_TYPE` | `JSON` | 线协议序列化选择（JSON / ROCKETMQ） |
| `ROCKETMQ_TLS_ENABLE` | `false` | 打开后所有出连接走 TLS |
| `ROCKETMQ_TLS_TEST_MODE` | `true` | 信任自签证书、不校验主机名 |
| `ROCKETMQ_TRACE_CONTEXT_ENABLE` | 空 | `1` / `true` / `yes` = 发送时注入 W3C `traceparent`（已有则透传给消费侧） |

**clientId 口径**：`<本机 IP>@<instanceName>[@<unitName>][@STREAM]`，`instanceName` 为
`DEFAULT` 时在 `start()` 里就地改写成 `<pid>#<nanoTime>`（生产者和 CLUSTERING 消费者；
广播消费者保持 `DEFAULT`，同进程的广播消费者共用一份实例）。本机 IP 用 UDP「连」公网
地址后读 sockname 探测（不发包），取不到退化成 `127.0.0.1`。

## License

Apache-2.0。
