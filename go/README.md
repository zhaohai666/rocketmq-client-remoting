# RocketMQ Go 客户端

> 中文 ｜ [English](README.en.md)

## 概述

RocketMQ 经典 remoting 协议的 Go 客户端 SDK。直连 NameServer 取路由、直连 Broker 收发，
链路上没有任何代理、Sidecar 或网关。对外是同步风格的调用（`Send` / `Poll` / `Pull`），
路由刷新、心跳、重平衡、消费拉取、位点提交都由客户端内部的 goroutine 周期任务承担。

- 模块路径：`github.com/zhaohai666/rocketmq-client-remoting/go`
- 依赖：**零第三方依赖**，`go.mod` 里没有 `require` 段；LZ4 / ZSTD / 日志轮转全部手写
- 分层：`remoting`（线协议与长连接传输）→ `common`（消息模型、编解码、压缩、常量）→
  `client`（Producer / Push / Pull / LitePull / Admin / 事务 / 轨迹）
- 已在 **RocketMQ 5.5.1** 集群上跑通 13 个真机联调工具与 584 个单测

## 先决条件

- Go **1.24** 或更高（`go.mod` 声明 `go 1.24`；本仓库在 go1.24.13 上构建与测试）
- 一台可用的 NameServer，默认端口 **9876**
- 一台可用的 Broker，默认端口 **10911**
- 想让工具自己建 topic / 订阅组，Broker 需 `autoCreateTopicEnable=true` 与
  `autoCreateSubscriptionGroup=true`；跑定时消息撤回与消息轨迹另需
  `recallMessageEnable=true`、`traceTopicEnable=true`

## 安装与开发

```sh
go get github.com/zhaohai666/rocketmq-client-remoting/go
```

```go
import (
    "github.com/zhaohai666/rocketmq-client-remoting/go/client"
    "github.com/zhaohai666/rocketmq-client-remoting/go/common"
    "github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)
```

构建、静态检查、格式化、单测：

```sh
cd go
go build ./...                 # 通过，无输出
go vet ./...                   # 通过，无输出
gofmt -l .                     # 输出为空才是过
go test ./... -count=1
```

`go test ./... -count=1` 的实测结果（3 个测试包全部 `ok`）：

| 包 | 测试函数数 | 耗时 |
| --- | --- | --- |
| `client` | 313 | 12.9s |
| `common` | 134 | 6.0s |
| `remoting` | 137 | 1.6s |
| **合计** | **584**（50 个 `*_test.go`） | ~20s |

计数用 `go test ./client/ -list '.*' | grep -c '^Test'`。并发回归单独跑：

```sh
go test -race ./client/
```

不连集群的协议冒烟（逐条 `[PASS]`，收口 `selfcheck: ALL PASS (PASS=10 FAIL=0)`）：

```sh
go run ./examples/selfcheck
```

## 快速上手

### Producer

普通消息：

```go
producer, err := client.NewDefaultMQProducer("GID_demo")
if err != nil {
    panic(err)
}
producer.SetNameServerAddr("127.0.0.1:9876")
if err := producer.Start(); err != nil {
    panic(err)
}
defer producer.Shutdown()

result, err := producer.Send(common.NewMessage("TopicTest", []byte("hello")))
if err != nil {
    panic(err)
}
fmt.Println(result.SendStatus, result.MsgID, result.OffsetMsgID, result.QueueOffset)
```

带 Tag / Key / 自定义属性，并指定超时：

```go
msg := common.NewMessageWithTags("TopicTest", body, "TagA", "OrderID001", 0)
msg.SetUserProperty("bizType", "trade")
result, err = producer.SendWithTimeout(msg, 3000)
```

顺序消息（同一业务键固定落同一队列），以及定点队列：

```go
result, err = producer.SendBySelector(msg, client.SelectMessageQueueByHash{}, "shard-42")

mq := common.NewMessageQueue("TopicTest", "broker-a", 0)
result, err = producer.SendToQueue(msg, mq)
```

延迟与定时消息（经典档位与 5.x 定时属性都支持）：

```go
msg.SetDelayTimeLevel(3)        // 经典档位 3 = 10s
msg.SetDelayTimeSec(30)         // 定时器：30 秒后投递
msg.SetDelayTimeMs(5000)
msg.SetDeliverTimeMs(deliverAt) // 绝对时间戳

scheduled, err := producer.Send(msg)
handle := scheduled.RecallHandle // 只有定时消息带撤回句柄
```

撤回定时消息：

```go
newHandle, err := producer.RecallMessage("TopicTest", handle)
```

批量发送与单向发送：

```go
result, err = producer.SendBatch([]*common.Message{msgA, msgB, msgC})

err = producer.SendOneway(msg, &mq) // 不等 broker 应答，没有返回值
```

真异步发送（回调恰好触发一次，可开背压限流）：

```go
type callback struct{}

func (callback) OnSuccess(r *client.SendResult) { fmt.Println("ok", r.MsgID) }
func (callback) OnException(err error)          { fmt.Println("fail", err) }

producer.SetEnableBackpressureForAsyncMode(true)
producer.SetBackPressureForAsyncSendNum(1024)
producer.SetBackPressureForAsyncSendSize(64 * 1024 * 1024)
err = producer.SendAsync(msg, callback{})
```

Request-Reply：

```go
producer.SetRequestTimeout(3000)
reply, err := producer.Request(msg) // 回包正文在 reply.Body
```

事务消息（两阶段 + broker 回查）：

```go
type txListener struct{}

func (txListener) ExecuteLocalTransaction(msg *common.Message, arg any) client.LocalTransactionState {
    return client.CommitMessage // 或 RollbackMessage；Unknow 表示挂起等回查
}
func (txListener) CheckLocalTransaction(msg *common.MessageExt) client.LocalTransactionState {
    return client.CommitMessage
}

txProducer, _ := client.NewTransactionMQProducer("GID_demo_tx")
txProducer.SetNameServerAddr("127.0.0.1:9876")
txProducer.SetTransactionListener(txListener{})
txProducer.Start()
defer txProducer.Shutdown()

res, err := txProducer.SendMessageInTransaction(msg, nil)
fmt.Println(res.SendStatus, res.LocalTransactionState)
```

### PushConsumer

并发消费：

```go
type listener struct{}

func (listener) ConsumeMessage(msgs []*common.MessageExt,
    ctx *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
    for _, m := range msgs {
        fmt.Println(m.MsgID, string(m.Body))
    }
    return client.ConsumeSuccess // 要重投就返回 client.ReconsumeLater
}

consumer, err := client.NewDefaultMQPushConsumer("GID_demo")
if err != nil {
    panic(err)
}
consumer.SetNameServerAddr("127.0.0.1:9876")
consumer.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
consumer.SetConsumeThreadNums(20)
if err := consumer.Subscribe("TopicTest", "TagA || TagB"); err != nil {
    panic(err)
}
if err := consumer.SetMessageListener(listener{}); err != nil {
    panic(err) // 也可以用类型明确的 SetConcurrentlyListener / SetOrderlyListener
}
if err := consumer.Start(); err != nil {
    panic(err)
}
select {} // 常驻
```

顺序消费（换一个监听器接口，返回值决定本队列的挂起与提交）：

```go
type orderlyListener struct{}

func (orderlyListener) ConsumeMessage(msgs []*common.MessageExt,
    ctx *client.ConsumeOrderlyContext) client.ConsumeOrderlyStatus {
    return client.OrderlySuccess // 或 OrderlySuspendCurrentQueueAMoment
}

consumer.SetMessageListener(orderlyListener{})
```

广播消费（每个实例收全量，位点只落本地文件）：

```go
consumer.SetMessageModel(client.MessageModelBroadcasting) // 默认 client.MessageModelClustering
```

POP 模式（消息对所有实例可见，靠 ack 与不可见时间推进）：

```go
consumer.SetPopMode(true)
```

### PullConsumer

调用方自己持有游标，短轮询与长轮询是两条不同的路：

```go
pullConsumer, _ := client.NewDefaultMQPullConsumer("GID_demo_pull")
pullConsumer.SetNameServerAddr("127.0.0.1:9876")
pullConsumer.Start()
defer pullConsumer.Shutdown()

mqs, _ := pullConsumer.FetchSubscribeMessageQueues("TopicTest")
for _, mq := range mqs {
    result, err := pullConsumer.Pull(mq, "*", 0, 32) // 短轮询：绝不挂起
    if err != nil {
        panic(err)
    }
    switch result.PullStatus {
    case client.PullFound:
        for _, m := range result.MsgFoundList {
            fmt.Println(m.MsgID)
        }
    case client.PullNoNewMsg, client.PullNoMatchedMsg, client.PullOffsetIllegal:
        // 四种状态都跟随 result.NextBeginOffset 推游标
    }
    _ = pullConsumer.UpdateConsumeOffset(mq, result.NextBeginOffset)
}

// 长轮询（挂起位只在这条路上置）：
result, _ := pullConsumer.PullBlockIfNotFound(mqs[0], "*", result.NextBeginOffset, 32)

// 位点与队列边界：
lo, _ := pullConsumer.MinOffset(mq)
hi, _ := pullConsumer.MaxOffset(mq)
at, _ := pullConsumer.SearchOffset(mq, time.Now().Add(-time.Hour).UnixMilli())
stored, _ := pullConsumer.FetchConsumeOffset(mq, true)
```

### LitePullConsumer

内部跑后台短轮询填缓冲，由调用方 `Poll` 取；拉取游标与消费游标分开。

```go
lite, _ := client.NewDefaultLitePullConsumer("GID_demo_lite")
lite.SetNameServerAddr("127.0.0.1:9876")
lite.Subscribe("TopicTest", "*")
lite.Start()
defer lite.Shutdown()

for {
    for _, m := range lite.Poll() { // 只有真交付才推进消费游标
        fmt.Println(m.MsgID)
    }
    if err := lite.Commit(); err != nil { // 提交的是消费游标
        fmt.Println("commit:", err)
    }
    time.Sleep(time.Second)
}
```

指定队列（`Assign`）+ 重放 + 按时间定位：

```go
mqs, _ := lite.FetchMessageQueues("TopicTest")
lite.Assign(mqs)
lite.SeekToBegin() // 或者 lite.Seek(mq, offset) 精确重放某个队列
ts, _ := lite.OffsetForTimestamp(mqs[0], deliverTimestamp)
```

topic 队列变更监听（扩容 / 缩容都会回调）：

```go
type queueChange struct{}

func (queueChange) OnChanged(topic string, messageQueues []common.MessageQueue) {
    fmt.Println(topic, "现在", len(messageQueues), "个队列")
}

if err := lite.RegisterTopicMessageQueueChangeListener("TopicTest", queueChange{}); err != nil {
    panic(err)
}
```

### Admin

```go
admin := client.NewDefaultMQAdminExt(nil) // 要 ACL 就传一个 remoting.RPCHook
admin.SetNameServerAddresses([]string{"127.0.0.1:9876"})
if err := admin.Start(); err != nil {
    panic(err)
}
defer admin.Shutdown()

cluster, _ := admin.ExamineBrokerClusterInfo()
list, _ := admin.FetchAllTopicList()
route, _ := admin.ExamineTopicRoute("TopicTest")
stats, _ := admin.ExamineTopicStats("TopicTest")

_ = admin.CreateTopic(common.DefaultTopic, "TopicTest", 8, 0)
_ = admin.DeleteTopic("TopicTest", "DefaultCluster")
```

### ACL

```go
credentials := remoting.NewSessionCredentials("YourAccessKey", "YourSecretKey")
hook, err := remoting.NewAclClientRPCHook(credentials)
if err != nil {
    panic(err)
}
producer.SetRpcHook(hook)

// 带 STS token：
scoped := remoting.NewSessionCredentialsWithToken(accessKey, secretKey, securityToken)
```

### 命名空间

两套彼此独立的机制，按部署形态选一种：

```go
// 一：客户端侧改写资源名，topic / group 上线时就带上命名空间前缀
producer.SetNamespace("MyNamespace")

// 二：服务端侧命名空间，资源名原样上线，命名空间随 `ns` / `nsd` 扩展字段下发
producer.SetNamespaceV2("MyNamespace")
```

两个 setter 在 `DefaultMQProducer`、`TransactionMQProducer` 和三种消费者上都有。

### 压缩

```go
producer.SetCompressType(common.ZstdType)        // common.ZlibType / common.Lz4Type / common.ZstdType
producer.SetCompressLevel(5)
producer.SetCompressMsgBodyOverHowmuch(4 * 1024) // 正文超过这个字节数才压
```

### TLS

```go
producer.SetTLSEnable(true)
```

也可以纯环境变量打开（`ROCKETMQ_TLS_ENABLE=1`）。握手固定 `MinVersion = TLS 1.2`；
`ROCKETMQ_TLS_TEST_MODE` 默认 `true`，信任自签证书且不校验 CA，生产环境请显式设为
`false` 走完整校验。

## 特性与进度

- ✅ 生产者：普通消息、Tag / Key / 自定义属性、顺序（选择器与定点队列）、延迟档位与 5.x
  定时属性、批量、单向、真异步（回调 + 背压信号量 + 失败重试）、事务（两阶段 + broker 回查）、
  Request-Reply、定时消息撤回（`RecallMessage`）
- ✅ 消费者：Push（并发 / 顺序 / 广播 / POP、Tag 与 SQL92 过滤、拉取前流控、线程池在线调整、
  `OFFSET_ILLEGAL` 冻结重建、优雅关停时 join 在途消费）、Pull（调用方游标、短轮询、长轮询、
  位点读写、`sendMessageBack`）、LitePull（后台短轮询 + 双游标、`Seek`、自动提交、
  队列变更监听、关停落盘）
- ✅ 队列分配策略 6 个：平均、按圈平均、一致性哈希、机房、就近机房、按配置
- ✅ 管理端：集群与路由探活、topic / 订阅组 / KV 配置增删改查、位点重置与四路位点查询、
  消费统计与消费进度、运行信息与直投调试、消息查看与按 key 检索、批量配置、静态 topic、
  顺序 topic 配置、半消息恢复、过期清理
- ✅ 线协议：JSON 与私有二进制双序列化（`ROCKETMQ_SERIALIZE_TYPE`）、17 段存储格式与
  6 段批量格式、V2 单字母短键 header、心跳与订阅关系、请求码 / 响应码 / 语言码常量
- ✅ 传输：长连接复用、半包粘包重组、opaque 匹配、超时、连接断开即在途请求判死、
  `GO_AWAY`、同步 / 异步 / 单向、TLS
- ✅ 安全与多租：ACL 签名（`HmacSHA1`，标准库）、两套命名空间、单元化
  （`SetUnitName` / `SetUnitMode`）、`CheckForbiddenHook`
- ✅ 压缩：ZLIB、LZ4、ZSTD 三型可发可收；ZSTD 解码是完整格式（含 FSE + Huffman），
  编码侧的范围见「真实集群联调」末尾
- ✅ 可观测：消息轨迹（Pub / SubBefore / SubAfter / EndTransaction / Recall 五类记录 +
  异步分发器 + 两道防自噬闸门）、消费统计（TPS / RT 差分窗口）、故障延迟规避选队列
- ✅ 位点存储：本地文件与远端 Broker 两种，重启续读
- ✅ 日志：环境变量配置的文件 / 标准输出日志，按大小轮转

## 客户端日志

日志默认写文件，落盘位置与轮转策略都由环境变量控制：

| 环境变量 | 作用 | 默认值 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `TRACE` / `DEBUG` / `INFO` / `WARN` / `ERROR` | `INFO` |
| `ROCKETMQ_CLIENT_LOG_DIR` | 日志目录 | `$HOME/logs/rocketmqlogs` |
| `ROCKETMQ_CLIENT_LOG_FILE` | 文件名；含路径分隔符时按整条路径处理；空串 / `OFF` / `NONE` = 不写文件只留 stderr | `rocketmq_go_client.log` |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | 任意非空值 = 改写标准输出（优先于上面两项） | 未设置 |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | 单文件上限（字节），`0` = 不轮转 | `67108864`（64MB） |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | 轮转保留的备份份数，`0` = 不留备份、原地截断 | `10` |

默认落盘位置是 **`$HOME/logs/rocketmqlogs/rocketmq_go_client.log`**。写到上限就滚进固定
窗口的备份序列（`rocketmq_go_client.log.1` … `.10`），窗口里最老的一份被丢弃；文件被外部
删掉时日志会重开一个新文件，不会静默停写。调试期想直接看标准输出就设
`ROCKETMQ_CLIENT_LOG_USE_STDOUT=1`。换目录用 `ROCKETMQ_CLIENT_LOG_DIR`：

```sh
ROCKETMQ_CLIENT_LOG_DIR=/var/log/rmq-client \
ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE=16777216 \
go run ./examples/live_producer -ns 127.0.0.1:9876
```

其余可用开关：`ROCKETMQ_SERIALIZE_TYPE`（线协议序列化，默认 `JSON`）、
`ROCKETMQ_TLS_ENABLE` / `ROCKETMQ_TLS_TEST_MODE`（见「TLS」）、
`ROCKETMQ_TRACE_CONTEXT_ENABLE`（`1` / `true` / `yes` = 发送时注入 W3C `traceparent`）。

## 真实集群联调

`examples/` 下 13 个可执行工具，**不进 `go test`**，全部自断言；除 `selfcheck` 外都要
一个活着的集群。统一跑法：

```sh
cd go
go run ./examples/<名字> -ns 127.0.0.1:9876
```

退出码约定：**`0` 全部通过；`1` 至少一项 `FAIL`；`2` 环境不对**（参数缺失、连不上、
集群没就绪）—— 环境问题直接中止，不计入失败。每项打印 `PASS` / `FAIL`，最后一行是收口
计数（`PASS=<n> FAIL=<n>`）。

| 工具 | 验的是什么 |
| --- | --- |
| `selfcheck` | 不连集群的协议自检：双帧回环、header 键拼写、V2 短键集、17 段与 6 段消息格式、msgId 反解、Crc32 向量、ACL 签名注入（实测 `PASS=10 FAIL=0`） |
| `wait_cluster` | 阻塞到 NameServer 报出 master broker 才退出。Broker 的「boot success」只代表本地 store 打开，注册是另一条线程、晚几秒，跑批前先过这道门 |
| `live_producer` | 六条发送路径、`SendResult` 各字段形状、异步内核（回调恰好一次 / 定点 / 选择器 / 批量 / 背压许可归还）、事务两阶段与 broker 回查；收口行带 `SENT=<n>` |
| `live_consumer` | Push 消费：`CONSUME_FROM_FIRST_OFFSET` 收全且不重不漏、同组第二实例收不到、客户端二次 tag 过滤、顺序消费、双实例切分、优雅注销 |
| `live_pull` | Pull：min / max / 按时间搜位点、空队头短轮询不挂起、空队尾 `NO_NEW_MSG` 不挂起、长轮询提前唤醒与超时、调用方游标、`sendMessageBack` 进 `%RETRY%`；收口行带 `COMMITTED=<offset>` |
| `live_lite_pull` | LitePull：`Assign` 双游标推进、`Commit` 与重启续读不重放、`Seek` 重放、`Subscribe` 重平衡 + 自动提交在 broker 读回、关停持久化 |
| `live_lite_topic_queue_change` | 队列变更监听：真的把 topic 扩容与缩容，回调必须在一两个检查轮内落地，证明比对的是现查路由而不是 30s 路由缓存（实测 `PASS=11 FAIL=0`） |
| `live_pop` | POP 消费：不可见时间、ack 与批量 ack、改超时、重投窗口。断言的是定时窗口与 broker 侧可见状态，不是返回码 —— POP 出错全是静默的 |
| `live_redelivery` | `%RETRY%` 二次投递与延迟梯度、`maxReconsumeTimes` 到顶后进 `%DLQ%`、顺序毒消息的 DLQ 路径、`ackIndex` 部分 ack |
| `live_shutdown_race` | 「关掉客户端就立刻退进程」的丢数据契约：并发回投两轮（`%RETRY%` → `%DLQ%`）、顺序挂起回投、短生命周期轨迹生产者的尾批 |
| `live_admin` | 管理端 29 项：集群探活与 master 选主、topic CRUD、broker 配置与运行时 KV、订阅组 CRUD、发 8 条验 topic 统计、四路位点查询、KEYS 检索 + `viewMessage`、删 topic 后确认消失 |
| `live_admin_batch` | 批量与运维类 RPC：批量 topic / 订阅组配置、静态 topic、禁写、半消息恢复、顺序 topic 配置、清理类调用 |
| `live_compression_matrix` | 压缩矩阵的一腿，用位置参数而不是 flag：`send` 灌一个由固定配方本地重建的载荷，`recv` 读回并比对解压正文的 CRC |

需要额外参数的两条：

```sh
go run ./examples/live_consumer -ns 127.0.0.1:9876 \
    -topic T -group G -expect 12 -orderly-topic T_ord -orderly-expect 6

go run ./examples/live_compression_matrix send <topic> <group> <size> 127.0.0.1:9876 zlib
go run ./examples/live_compression_matrix recv <topic> <group> <size> 127.0.0.1:9876
```

`live_pop` 与 `live_redelivery` 支持 `-legs s1,s2` 只跑其中几个场景。仓库根目录 `scripts/`
下还有一层封装好的脚本（起集群、等就绪、跑验证、收工在同一条命令里），与上表按名字对应，
例如 `bash scripts/run_go_producer_live.sh`。

**先起消费者，再发消息。** 全新消费组第一次上线时 `ConsumeFromWhereLastOffset` 会把游标定
在它启动那一刻的队列尾部，启动之前落盘的消息不在这个窗口里，看起来就像「一条都没收到」。
所以跑 `live_consumer` / `live_pop` / `live_redelivery` 这类工具时，先确认消费者已经开始收，
再让生产端灌消息；确实要读历史消息就显式
`consumer.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)`。

**本 SDK 当前的能力边界：**

- ZSTD 编码只产出合法帧（RAW / RLE 块），**不做熵编码，压缩比≈1:1**：实测 9600 字节的高度
  可压缩载荷编码成 9616 字节（帧头 + 块头），回环一致、对端照读不误；只是本客户端发出去的
  ZSTD 正文不会变小。ZSTD 解码是完整实现，FSE + Huffman 都能解。
- ZLIB 与 LZ4 编码是真压缩：同一份 9600 字节载荷，ZLIB 压到 80 字节、LZ4 压到 103 字节，
  三个 codec 回环都一致。
- POP 的队列集合由本地重平衡算出（每队列一个 POP 循环 + ack），不向 broker 申请分配。
- `OFFSET_ILLEGAL` 冻结重建、拉取流控五档、心跳全景、六个分配策略目前只有单测与进程内假集群
  覆盖，`examples/` 里还没有对应的真机工具。

## 目录结构

```
go/
├── README.md · README.en.md
├── go.mod                      module github.com/zhaohai666/rocketmq-client-remoting/go（go 1.24，零依赖）
├── common/                     与协议无关的基础层
│   ├── message.go                  Message / MessageExt / MessageQueue（含队列的线上形态与哈希）
│   ├── message_decoder.go          17 段存储格式 + 6 段批量格式
│   ├── message_const.go · message_type.go   属性键名与消息类型
│   ├── message_client_id_setter.go clientId 口径
│   ├── compression.go              ZLIB / LZ4 / ZSTD 分派
│   ├── lz4.go · zstd.go · zstd_entropy.go   手写 LZ4 Frame 与 ZSTD 帧（编码 raw 块，解码全格式）
│   ├── recall_handle.go            定时消息撤回句柄
│   ├── namespace.go                两套命名空间的资源名包装与还原
│   ├── logging.go                  环境变量驱动的日志 + 大小轮转
│   ├── buffer.go · sysflag.go · mixall.go · util.go · errors.go
│   ├── topic_validator.go · validators.go · stringmap.go
│   └── pop_ack.go · extra_info.go
├── remoting/                   线协议层
│   ├── remoting_command.go         帧编解码
│   ├── headers.go · codes.go       请求头家族（含 V2 短键）、请求码 / 响应码 / 语言码
│   ├── serialize.go                JSON 与私有二进制双序列化
│   ├── json_value.go               容错 JSON 解析
│   ├── json_double.go              线协议要求的 double 字面量写法（0.0 / 1.0E20 / NaN→null）
│   ├── inline_key_json_encode.go   允许内联对象做 map key 的出站 JSON 写器
│   ├── bodies.go · common_bodies.go · consumer_bodies.go · admin_bodies.go · pop_bodies.go
│   ├── client_info_bodies.go · subscription.go · heartbeat.go
│   ├── client.go                   长连接传输：同步 / 异步 / 单向 + 半包 + TLS + 判死
│   └── acl.go · rpchooks.go · errors.go
├── client/                     SDK 层
│   ├── instance.go                 客户端实例：路由发现、全部 RPC、心跳、周期任务
│   ├── producer.go · async.go · transaction_producer.go · request_reply.go · send_result.go
│   ├── consumer.go · consume_service.go · process_queue.go · pool.go · semaphore.go
│   ├── pull_consumer.go · pull_api.go · lite_pull_consumer.go
│   ├── pop_api.go · pop_consumer.go · pop_process_queue.go
│   ├── allocate.go                 六个队列分配策略
│   ├── offset_store.go · route.go · broker_api.go · hooks.go · listener.go
│   ├── admin.go · admin_api.go · admin_batch.go · admin_offset.go · admin_track.go · admin_util.go
│   ├── trace.go · trace_hook.go · trace_dispatcher.go · trace_context.go
│   ├── consumer_stats.go · stats_item.go · consumer_running_info.go · fault_strategy.go
│   ├── consume_directly.go · jsonutil.go
│   └── *_test.go                   50 个测试文件，584 个测试函数
└── examples/                   13 个可执行工具（见「真实集群联调」）
```

## License

Apache-2.0，详见仓库根目录 `LICENSE`。
