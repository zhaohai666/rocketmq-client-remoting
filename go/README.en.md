# The Go Implementation of the RocketMQ Remoting Client

English | [中文](README.md)

## Overview

A Go client SDK for RocketMQ's classic remoting protocol. It talks to the NameServer for
route discovery and to the Broker for sending and receiving — no proxy, sidecar or gateway in
the path. The public API is synchronous (`Send` / `Poll` / `Pull`); route refresh, heartbeats,
rebalance, pull loops and offset commits are handled by periodic goroutines inside the client.

- Module path: `github.com/zhaohai666/rocketmq-client-remoting/go`
- Dependencies: **zero third-party dependencies** — `go.mod` has no `require` section; LZ4,
  ZSTD and log rotation are all hand-written here
- Layers: `remoting` (wire protocol + long-connection transport) → `common` (message model,
  codecs, compression, constants) → `client` (Producer / Push / Pull / LitePull / Admin /
  transaction / trace)
- Verified against a **RocketMQ 5.5.1** cluster: 13 live tools plus 584 unit tests

## Prerequisites

- Go **1.24** or newer (`go.mod` declares `go 1.24`; this repository builds and tests on go1.24.13)
- A reachable NameServer, default port **9876**
- A reachable Broker, default port **10911**
- For the tools to create topics / subscription groups themselves, the Broker needs
  `autoCreateTopicEnable=true` and `autoCreateSubscriptionGroup=true`; the scheduled-message
  recall and message-trace legs additionally need `recallMessageEnable=true` and
  `traceTopicEnable=true`

## Getting Started

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

Build, vet, format, test:

```sh
cd go
go build ./...                 # passes, no output
go vet ./...                   # passes, no output
gofmt -l .                     # empty output is the pass
go test ./... -count=1
```

Measured result of `go test ./... -count=1` (all three test packages `ok`):

| Package | Test functions | Time |
| --- | --- | --- |
| `client` | 313 | 12.9s |
| `common` | 134 | 6.0s |
| `remoting` | 137 | 1.6s |
| **Total** | **584** (50 `*_test.go` files) | ~20s |

To count them yourself: `go test ./client/ -list '.*' | grep -c '^Test'`. Concurrency
regression on its own:

```sh
go test -race ./client/
```

Protocol smoke test that needs no cluster (one `[PASS]` per check, closing line
`selfcheck: ALL PASS (PASS=10 FAIL=0)`):

```sh
go run ./examples/selfcheck
```

## Examples

### Producer

Plain message:

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

With Tag / Key / custom properties and an explicit timeout:

```go
msg := common.NewMessageWithTags("TopicTest", body, "TagA", "OrderID001", 0)
msg.SetUserProperty("bizType", "trade")
result, err = producer.SendWithTimeout(msg, 3000)
```

Ordered message (the same business key always lands on the same queue), and an explicit queue:

```go
result, err = producer.SendBySelector(msg, client.SelectMessageQueueByHash{}, "shard-42")

mq := common.NewMessageQueue("TopicTest", "broker-a", 0)
result, err = producer.SendToQueue(msg, mq)
```

Delayed and scheduled messages (both the classic level and the 5.x timer properties work):

```go
msg.SetDelayTimeLevel(3)        // classic level 3 = 10s
msg.SetDelayTimeSec(30)         // timer: deliver after 30 seconds
msg.SetDelayTimeMs(5000)
msg.SetDeliverTimeMs(deliverAt) // absolute timestamp

scheduled, err := producer.Send(msg)
handle := scheduled.RecallHandle // only scheduled messages carry a recall handle
```

Recall a scheduled message:

```go
newHandle, err := producer.RecallMessage("TopicTest", handle)
```

Batch and one-way sends:

```go
result, err = producer.SendBatch([]*common.Message{msgA, msgB, msgC})

err = producer.SendOneway(msg, &mq) // does not wait for the broker, returns nothing but an error
```

True asynchronous send (the callback fires exactly once; backpressure can be switched on):

```go
type callback struct{}

func (callback) OnSuccess(r *client.SendResult) { fmt.Println("ok", r.MsgID) }
func (callback) OnException(err error)          { fmt.Println("fail", err) }

producer.SetEnableBackpressureForAsyncMode(true)
producer.SetBackPressureForAsyncSendNum(1024)
producer.SetBackPressureForAsyncSendSize(64 * 1024 * 1024)
err = producer.SendAsync(msg, callback{})
```

Request-Reply:

```go
producer.SetRequestTimeout(3000)
reply, err := producer.Request(msg) // the reply body is in reply.Body
```

Transactional message (two-phase + broker check-back):

```go
type txListener struct{}

func (txListener) ExecuteLocalTransaction(msg *common.Message, arg any) client.LocalTransactionState {
    return client.CommitMessage // or RollbackMessage; Unknow keeps the half message pending
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

Concurrent consumption:

```go
type listener struct{}

func (listener) ConsumeMessage(msgs []*common.MessageExt,
    ctx *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
    for _, m := range msgs {
        fmt.Println(m.MsgID, string(m.Body))
    }
    return client.ConsumeSuccess // return client.ReconsumeLater to have it redelivered
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
    panic(err) // or the explicitly typed SetConcurrentlyListener / SetOrderlyListener
}
if err := consumer.Start(); err != nil {
    panic(err)
}
select {} // run forever
```

Ordered consumption (a different listener interface; the return value controls suspend and
commit for that queue):

```go
type orderlyListener struct{}

func (orderlyListener) ConsumeMessage(msgs []*common.MessageExt,
    ctx *client.ConsumeOrderlyContext) client.ConsumeOrderlyStatus {
    return client.OrderlySuccess // or OrderlySuspendCurrentQueueAMoment
}

consumer.SetMessageListener(orderlyListener{})
```

Broadcast consumption (every instance gets everything; offsets stay in the local file):

```go
consumer.SetMessageModel(client.MessageModelBroadcasting) // default is client.MessageModelClustering
```

POP mode (messages stay visible to all instances; progress comes from ack and the invisible
window):

```go
consumer.SetPopMode(true)
```

### PullConsumer

The caller owns the cursor, and short and long polling are two separate paths:

```go
pullConsumer, _ := client.NewDefaultMQPullConsumer("GID_demo_pull")
pullConsumer.SetNameServerAddr("127.0.0.1:9876")
pullConsumer.Start()
defer pullConsumer.Shutdown()

mqs, _ := pullConsumer.FetchSubscribeMessageQueues("TopicTest")
for _, mq := range mqs {
    result, err := pullConsumer.Pull(mq, "*", 0, 32) // short polling: never suspends
    if err != nil {
        panic(err)
    }
    switch result.PullStatus {
    case client.PullFound:
        for _, m := range result.MsgFoundList {
            fmt.Println(m.MsgID)
        }
    case client.PullNoNewMsg, client.PullNoMatchedMsg, client.PullOffsetIllegal:
        // all four statuses advance the cursor to result.NextBeginOffset
    }
    _ = pullConsumer.UpdateConsumeOffset(mq, result.NextBeginOffset)
}

// Long polling (the suspend bit is set only on this path):
result, _ := pullConsumer.PullBlockIfNotFound(mqs[0], "*", result.NextBeginOffset, 32)

// Offsets and queue boundaries:
lo, _ := pullConsumer.MinOffset(mq)
hi, _ := pullConsumer.MaxOffset(mq)
at, _ := pullConsumer.SearchOffset(mq, time.Now().Add(-time.Hour).UnixMilli())
stored, _ := pullConsumer.FetchConsumeOffset(mq, true)
```

### LitePullConsumer

A background short-pull loop fills a buffer and the caller drains it with `Poll`; the pull
cursor and the consume cursor are separate.

```go
lite, _ := client.NewDefaultLitePullConsumer("GID_demo_lite")
lite.SetNameServerAddr("127.0.0.1:9876")
lite.Subscribe("TopicTest", "*")
lite.Start()
defer lite.Shutdown()

for {
    for _, m := range lite.Poll() { // only actual delivery advances the consume cursor
        fmt.Println(m.MsgID)
    }
    if err := lite.Commit(); err != nil { // commits the consume cursor
        fmt.Println("commit:", err)
    }
    time.Sleep(time.Second)
}
```

Explicit queues (`Assign`) + replay + offset by timestamp:

```go
mqs, _ := lite.FetchMessageQueues("TopicTest")
lite.Assign(mqs)
lite.SeekToBegin() // or lite.Seek(mq, offset) to replay one exact queue
ts, _ := lite.OffsetForTimestamp(mqs[0], deliverTimestamp)
```

Topic queue-change listener (fires on both scale-up and scale-down):

```go
type queueChange struct{}

func (queueChange) OnChanged(topic string, messageQueues []common.MessageQueue) {
    fmt.Println(topic, "now has", len(messageQueues), "queues")
}

if err := lite.RegisterTopicMessageQueueChangeListener("TopicTest", queueChange{}); err != nil {
    panic(err)
}
```

### Admin

```go
admin := client.NewDefaultMQAdminExt(nil) // pass a remoting.RPCHook when ACL is required
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

// With an STS token:
scoped := remoting.NewSessionCredentialsWithToken(accessKey, secretKey, securityToken)
```

### Namespaces

Two independent mechanisms; pick one depending on how the cluster is set up:

```go
// 1: rewritten client-side — topic / group go on the wire with the namespace prefix
producer.SetNamespace("MyNamespace")

// 2: server-side namespace — resource names go on the wire unchanged, the namespace travels
//    as the `ns` / `nsd` extension fields
producer.SetNamespaceV2("MyNamespace")
```

Both setters exist on `DefaultMQProducer`, `TransactionMQProducer` and all three consumers.

### Compression

```go
producer.SetCompressType(common.ZstdType)        // common.ZlibType / common.Lz4Type / common.ZstdType
producer.SetCompressLevel(5)
producer.SetCompressMsgBodyOverHowmuch(4 * 1024) // only bodies above this size get compressed
```

### TLS

```go
producer.SetTLSEnable(true)
```

It can also be switched on purely by environment (`ROCKETMQ_TLS_ENABLE=1`). The handshake is
pinned to `MinVersion = TLS 1.2`; `ROCKETMQ_TLS_TEST_MODE` defaults to `true`, which trusts
self-signed certificates and skips CA verification — set it to `false` explicitly in production
to get full verification.

## Features and Status

- ✅ Producer: plain messages, Tag / Key / custom properties, ordered (selector and explicit
  queue), delay levels plus 5.x timer properties, batch, one-way, true async (callback +
  backpressure semaphore + retry on failure), transactional (two-phase + broker check-back),
  Request-Reply, scheduled-message recall (`RecallMessage`)
- ✅ Consumers: Push (concurrent / ordered / broadcast / POP, Tag and SQL92 filtering, flow
  control before pulling, online thread-pool resize, `OFFSET_ILLEGAL` freeze and rebuild,
  graceful shutdown joins in-flight consumption), Pull (caller-held cursor, short polling, long
  polling, offset read/write, `sendMessageBack`), LitePull (background short-pull + dual
  cursor, `Seek`, auto-commit, queue-change listener, shutdown persistence)
- ✅ Six queue allocation strategies: average, by-circle, consistent hash, machine room,
  nearby machine room, by configuration
- ✅ Admin: cluster and route discovery, topic / subscription group / KV config CRUD, offset
  reset plus four offset queries, consume stats and progress, running info and direct delivery
  for debugging, message viewing and lookup by key, batch configuration, static topics,
  order-topic configuration, half-message resume, expiry cleanup
- ✅ Wire protocol: dual JSON and private binary serialization (`ROCKETMQ_SERIALIZE_TYPE`),
  the 17-segment store format and 6-segment batch format, V2 single-letter short-key headers,
  heartbeat and subscription data, request / response / language code constants
- ✅ Transport: connection reuse, half-packet and sticky-packet reassembly, opaque matching,
  timeouts, in-flight requests marked dead the moment the connection dies, `GO_AWAY`, sync /
  async / one-way, TLS
- ✅ Security and multi-tenancy: ACL signing (`HmacSHA1`, standard library), both namespace
  schemes, unit mode (`SetUnitName` / `SetUnitMode`), `CheckForbiddenHook`
- ✅ Compression: ZLIB, LZ4 and ZSTD all send and receive; ZSTD decoding covers the full
  format (FSE + Huffman) — see the end of "Live Cluster Verification" for what the encoder
  side does
- ✅ Observability: message trace (Pub / SubBefore / SubAfter / EndTransaction / Recall plus
  the asynchronous dispatcher and two anti-self-swallowing gates), consume statistics
  (TPS / RT differential windows), latency-fault queue avoidance
- ✅ Offset storage: local file and remote broker, reading continues after a restart
- ✅ Logging: environment-variable driven file / stdout logging with size-based rotation

## Client Logging

Logs go to a file by default; the location and the rotation policy are environment driven:

| Environment variable | Purpose | Default |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `TRACE` / `DEBUG` / `INFO` / `WARN` / `ERROR` | `INFO` |
| `ROCKETMQ_CLIENT_LOG_DIR` | log directory | `$HOME/logs/rocketmqlogs` |
| `ROCKETMQ_CLIENT_LOG_FILE` | file name; if it contains a path separator the whole value is the path; empty / `OFF` / `NONE` = no file, stderr only | `rocketmq_go_client.log` |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | any non-empty value = log to stdout instead of a file (wins over the two above) | unset |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | per-file cap in bytes, `0` = no rotation | `67108864` (64MB) |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | backups kept after rotation, `0` = keep none, truncate in place | `10` |

The default location is **`$HOME/logs/rocketmqlogs/rocketmq_go_client.log`**. Once a file
reaches the cap it rolls into a fixed backup window (`rocketmq_go_client.log.1` … `.10`) and
the oldest file in the window is dropped; if the file is deleted externally the logger opens a
new one instead of silently stopping. To watch logs during development set
`ROCKETMQ_CLIENT_LOG_USE_STDOUT=1`. To relocate them use `ROCKETMQ_CLIENT_LOG_DIR`:

```sh
ROCKETMQ_CLIENT_LOG_DIR=/var/log/rmq-client \
ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE=16777216 \
go run ./examples/live_producer -ns 127.0.0.1:9876
```

Other switches: `ROCKETMQ_SERIALIZE_TYPE` (wire serialization, default `JSON`),
`ROCKETMQ_TLS_ENABLE` / `ROCKETMQ_TLS_TEST_MODE` (see "TLS"), and
`ROCKETMQ_TRACE_CONTEXT_ENABLE` (`1` / `true` / `yes` = inject a W3C `traceparent` on send).

## Live Cluster Verification

`examples/` holds 13 executables. They are **not part of `go test`**, every one is
self-asserting, and all except `selfcheck` need a live cluster. The uniform way to run them:

```sh
cd go
go run ./examples/<name> -ns 127.0.0.1:9876
```

Exit codes: **`0` everything passed; `1` at least one `FAIL`; `2` the environment is wrong**
(missing flag, cannot connect, cluster not ready) — those abort rather than count as failures.
Each check prints `PASS` / `FAIL` and the last line is the tally (`PASS=<n> FAIL=<n>`).

| Tool | What it verifies |
| --- | --- |
| `selfcheck` | Protocol self-test with no cluster: dual frame round-trips, header key spellings, the V2 short-key set, the 17-segment and 6-segment message formats, msgId reverse parsing, Crc32 vectors, ACL signature injection (measured `PASS=10 FAIL=0`) |
| `wait_cluster` | Blocks until the NameServer reports a master broker. The broker's "boot success" only means its local store opened — registration is a separate thread and lags several seconds, so gate on this first |
| `live_producer` | The six send paths, every `SendResult` field, the async kernel (callback exactly once / explicit queue / selector / batch / backpressure permit return), the two-phase transaction plus broker check-back; closing line carries `SENT=<n>` |
| `live_consumer` | Push consumption: `CONSUME_FROM_FIRST_OFFSET` receives everything with no duplication and no loss, a second instance of the same group gets nothing, client-side secondary Tag filtering, ordered consumption, two-instance split, graceful unregister |
| `live_pull` | Pull: min / max / offset-by-timestamp, empty queue head short polling does not suspend, empty queue tail `NO_NEW_MSG` does not suspend, long polling waking early and expiring, caller-owned cursor, `sendMessageBack` into `%RETRY%`; closing line carries `COMMITTED=<offset>` |
| `live_lite_pull` | LitePull: `Assign` dual-cursor advance, `Commit` and restart continuity without replay, `Seek` replay, `Subscribe` rebalance + auto-commit read back from the broker, shutdown persistence |
| `live_lite_topic_queue_change` | The queue-change listener: really scales the topic up and down, and the callback must land within one or two check rounds — which proves the comparison reads a fresh route rather than the 30s route cache (measured `PASS=11 FAIL=0`) |
| `live_pop` | POP consumption: the invisible window, ack and batch ack, changing the timeout on failure, the redelivery window. The assertions are timed windows and broker-side observable state, not return codes — POP goes wrong silently |
| `live_redelivery` | `%RETRY%` redelivery with its delay ladder, `%DLQ%` once `maxReconsumeTimes` is exhausted, the ordered poison-message DLQ path, `ackIndex` partial ack |
| `live_shutdown_race` | The data contract of "close the client and exit the process immediately": two rounds of concurrent send-back (`%RETRY%` → `%DLQ%`), the ordered suspended send-back, the tail batch of a short-lived trace producer |
| `live_admin` | 29 admin checks: cluster liveness and master election, topic CRUD, broker config and runtime KV, subscription group CRUD, eight messages to verify topic stats, the four offset queries, KEYS lookup + `viewMessage`, and confirmation that the topic is gone after deletion |
| `live_admin_batch` | The batch and housekeeping RPCs: batch topic / subscription group configuration, static topics, read-forbidden, half-message resume, order-topic config, expiry cleanup |
| `live_compression_matrix` | One leg of the compression matrix, using positional arguments instead of flags: `send` publishes a payload rebuilt locally from a fixed recipe, `recv` reads it back and compares the CRC of the decompressed body |

The two that need extra arguments:

```sh
go run ./examples/live_consumer -ns 127.0.0.1:9876 \
    -topic T -group G -expect 12 -orderly-topic T_ord -orderly-expect 6

go run ./examples/live_compression_matrix send <topic> <group> <size> 127.0.0.1:9876 zlib
go run ./examples/live_compression_matrix recv <topic> <group> <size> 127.0.0.1:9876
```

`live_pop` and `live_redelivery` accept `-legs s1,s2` to run only some scenarios. The
repository root's `scripts/` directory wraps the full sequence (bring up the cluster, wait for
readiness, verify, tear down — all in one command), named after the tools above, e.g.
`bash scripts/run_go_producer_live.sh`.

**Start the consumer before producing messages.** When a brand-new consumer group comes online
for the first time, `ConsumeFromWhereLastOffset` pins its cursor to the queue tail at that
moment, so messages that landed earlier are outside the window and it looks like "nothing was
received". For `live_consumer`, `live_pop` and `live_redelivery`, confirm the consumer is
already receiving before producing; to read history explicitly, call
`consumer.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)`.

**Current capability limits of this SDK:**

- ZSTD encoding emits a valid frame built from RAW / RLE blocks only — **no entropy coding, so
  the compression ratio is ≈1:1**. Measured: a highly compressible 9600-byte payload encodes to
  9616 bytes (frame header + block headers), round-trips identically, and the peer reads it
  normally; the ZSTD bodies this client sends simply do not shrink. ZSTD decoding is a complete
  implementation and handles FSE + Huffman.
- ZLIB and LZ4 encoding do compress: on the same 9600-byte payload ZLIB reaches 80 bytes and
  LZ4 103 bytes, and all three codecs round-trip consistently.
- The POP queue set is computed by local rebalance (one POP loop + ack per queue); the client
  does not ask the broker to assign queues.
- `OFFSET_ILLEGAL` freeze-and-rebuild, the five pull flow-control levels, the full heartbeat
  surface and the six allocation strategies are currently covered by unit tests and the
  in-process fake cluster only — there is no matching tool in `examples/` yet.

## Repository Layout

```
go/
├── README.md · README.en.md
├── go.mod                      module github.com/zhaohai666/rocketmq-client-remoting/go (go 1.24, zero deps)
├── common/                     protocol-agnostic foundation
│   ├── message.go                  Message / MessageExt / MessageQueue (incl. the queue's wire form and hash)
│   ├── message_decoder.go          17-segment store format + 6-segment batch format
│   ├── message_const.go · message_type.go   property keys and message types
│   ├── message_client_id_setter.go clientId construction
│   ├── compression.go              ZLIB / LZ4 / ZSTD dispatch
│   ├── lz4.go · zstd.go · zstd_entropy.go   hand-written LZ4 Frame and ZSTD frames (raw-block encode, full-format decode)
│   ├── recall_handle.go            scheduled-message recall handle
│   ├── namespace.go                resource-name wrapping for both namespace schemes
│   ├── logging.go                  environment-driven logging with size rotation
│   ├── buffer.go · sysflag.go · mixall.go · util.go · errors.go
│   ├── topic_validator.go · validators.go · stringmap.go
│   └── pop_ack.go · extra_info.go
├── remoting/                   wire-protocol layer
│   ├── remoting_command.go         frame encode/decode
│   ├── headers.go · codes.go       header family (incl. V2 short keys), request / response / language codes
│   ├── serialize.go                dual JSON and private binary serialization
│   ├── json_value.go               lenient JSON parsing
│   ├── json_double.go              the double literal form the protocol requires (0.0 / 1.0E20 / NaN→null)
│   ├── inline_key_json_encode.go   outbound JSON writer that allows inline objects as map keys
│   ├── bodies.go · common_bodies.go · consumer_bodies.go · admin_bodies.go · pop_bodies.go
│   ├── client_info_bodies.go · subscription.go · heartbeat.go
│   ├── client.go                   long-connection transport: sync / async / one-way + half-packet + TLS + mark-dead
│   └── acl.go · rpchooks.go · errors.go
├── client/                     SDK layer
│   ├── instance.go                 client instance: route discovery, all RPCs, heartbeat, periodic tasks
│   ├── producer.go · async.go · transaction_producer.go · request_reply.go · send_result.go
│   ├── consumer.go · consume_service.go · process_queue.go · pool.go · semaphore.go
│   ├── pull_consumer.go · pull_api.go · lite_pull_consumer.go
│   ├── pop_api.go · pop_consumer.go · pop_process_queue.go
│   ├── allocate.go                 the six queue allocation strategies
│   ├── offset_store.go · route.go · broker_api.go · hooks.go · listener.go
│   ├── admin.go · admin_api.go · admin_batch.go · admin_offset.go · admin_track.go · admin_util.go
│   ├── trace.go · trace_hook.go · trace_dispatcher.go · trace_context.go
│   ├── consumer_stats.go · stats_item.go · consumer_running_info.go · fault_strategy.go
│   ├── consume_directly.go · jsonutil.go
│   └── *_test.go                   50 test files, 584 test functions
└── examples/                   13 executables (see "Live Cluster Verification")
```

## License

Apache-2.0. See the `LICENSE` file at the repository root.
