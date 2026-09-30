# rocketmq-client-remoting (Go)

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 Go 实现（标准库 + `net`，**零第三方依赖**），
适配 RocketMQ 4.x / 5.x 集群，全部能力在真实 5.5.1 集群上联调验证过；与本仓库的
Python / C++ / .NET / Rust 实现逐项对齐。

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
| 消费 | Push Consumer（长轮询 + 顺序 + 广播 + 位点持久化 + 启动期数值校验 + 拉取前流控 + OFFSET_ILLEGAL 冻结重建 + 220 重置位点）、Pull Consumer（调用方持有游标 + 长轮询）、Lite Pull Consumer（**双游标引擎**：拉取游标 / 消费游标 / 内存提交表，361 + LITE 位） |
| 队列分配 | 六个策略：`AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>`，可插拔、由真实重平衡驱动 |
| 管理端 | `DefaultMQAdminExt`：topic / 订阅组 CRUD、集群信息、消费统计、消息查询（key / uniqKey / msgId）、位点读取与 broker 侧重置、消息轨迹查询 |
| 消息轨迹 | 客户端轨迹生产：Pub / SubBefore / SubAfter / EndTransaction / Recall 五类记录，Java `TraceDataEncoder` **逐字节**对齐（真值向量见单测）；`AsyncTraceDispatcher`（2048 有界丢弃 + 批 20 + 128K 分片 + 5s 冲刷 + 关停冲尾批）、内部生产者与 topic 前缀两道防自噬、W3C `traceparent` 注入与透传（`ROCKETMQ_TRACE_CONTEXT_ENABLE`） |
| 5.x 能力 | Request-Reply（326 holder）、撤回句柄 v1 编解码、消费侧统计、五类钩子、ACL 签名（`HmacSHA1`，标准库）、动态 name server 取址、故障规避选队列 |
| 校验门 | `Validators` / `TopicValidator`：组名 / topic 校验在 `start()` 建客户端实例**之前**本地跑完，失败不碰网络 |

**与其它四个端口的刻意差异 —— 压缩只有 ZLIB**：标准库没有 LZ4/ZSTD，而本模块承诺零第三方
依赖，所以这两种类型**大声报 `unsupported` 错误**，绝不把压缩流当正文透传（消费端解错就是
静默垃圾）。zlib 段的线上格式与其它语言互通（`scripts/compression_matrix.sh` 的 Go 腿）。

## 构建与检查

```bash
cd go
go build ./...
go vet ./...
gofmt -l .        # 空输出才是过
go test ./...     # 424 条，~9s
go test -race ./client/   # 并发回归（lite 消费者、异步发送）
```

## 单元测试

424 条测试函数全部与源码同目录（`*_test.go`），其中一部分跑在**进程内假集群**上
（`client/consumer_test.go` 里的 `clusterFixture`：真 socket 监听的假 broker + 假 name server，
脚本化应答，能锁死请求码、ext 字段名与重试分类）：

| 包 | 条数 | 覆盖 |
| --- | --- | --- |
| `client`（231） | producer 33 | 六条发送路径、重试分类（可重试码换 broker / 不可重试码立即抛 / 预算耗尽）、批量 ID 顺序、发送头 c/d/n、钩子各跑一次、发送头守卫 |
| | async 27 | 真异步内核：换 broker 换 opaque、背压信号量、有界队满同步抛、回调恰好一次、预算共享 |
| | transaction 6 | 两阶段 + 回查响应、单工 check 线程 |
| | request_reply 13 | 326 holder、Request/AsyncRequest 超时 |
| | consumer 26 | 长轮询、顺序重投闸门、流控、位点五 RPC、OFFSET_ILLEGAL、220、广播、关停在途消费 join + send-back 守卫 |
| | lite_pull 9 | **361 + LITE 位上线**、双游标（NO_NEW_MSG 也跟 nextBeginOffset）、Seek 丢缓冲、提交表是**清扫**不是过滤、暂停恢复闸门、订阅模式重平衡 + 关停落盘 |
| | pull_consumer 18 | 短轮询不带 SUSPEND 位、长轮询真挂起、调用方游标、sendMessageBack |
| | admin 35 | topic/组 CRUD、分页合并、222 的 `isForce` 键名、26 号 body 是 Properties 文本 |
| | instance/route 25 | 实例表复用、路由刷新、发布地址只认 master、注销 35 遍历主从 |
| | offset_store 9 | 本地/broker 双表、persistAll 清扫语义 |
| | hooks 14 | 五类钩子时序、ACL 签名拼串 |
| | trace 30 | Java 编码器**逐字节**真值向量、解码容错（无 keys 空段、坏记录只跳过自己）、SubBefore/SubAfter 共用 requestId + contextCode 五档、traceparent 注入与校验、分发器防自噬，以及两条**进程内真集群**端到端（Pub 落轨迹 topic 且不递归 / 消费对落盘并配对） |
| `common`（101） | 101 | 17 段编解码（坏数据拒收、压缩段 crc32）、消息模型、clientId 口径、recall 句柄真值向量、namespace、sysflag 位表、校验器 |
| `remoting`（92） | 92 | 真 socket 回环（同步/异步/oneway、半包、并发 opaque、静默超时）、TLS、ACL 签名、V2 短键名守卫（错一个字母就**静默丢字段**）、JSON 容错（裸数字键、对象 key、NaN）、心跳装配 |

## 真实集群联调

需要跑着 nameServer(9876) + broker(10911)、且 `autoCreateTopicEnable=true` 的集群。
这些工具**不进 `go test`**，依赖外部集群；全部自断言，任何一项失败进程以非 0 退出码结束，
收口行 `PASS=<n> FAIL=<n>`：

```bash
cd go
go run ./examples/live_producer    -ns 127.0.0.1:9876   # 六条发送路径/事务两阶段+回查/批量/撤回 recallMessage/异步内核/Request-Reply
go run ./examples/live_consumer    -ns 127.0.0.1:9876   # 长轮询/tag 过滤/%RETRY% 重投/%DLQ% 死信/顺序死信/位点五 RPC/OFFSET_ILLEGAL
go run ./examples/live_pull        -ns 127.0.0.1:9876   # 手动 pull 不重不漏、空队尾短轮询不挂起、长轮询唤醒/到期、调用方游标、send-back
go run ./examples/live_lite_pull   -ns 127.0.0.1:9876   # assign 模式端到端、重启续读不重放、Seek 重放、订阅模式自动提交在 broker 读回
```

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
└── examples/                   4 个真机联调工具（见上，不依赖集群的没有）
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
