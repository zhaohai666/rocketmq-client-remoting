# rocketmq-client-remoting (Go)
> [中文](README.md) | English

A Go implementation of Apache RocketMQ's classic remoting protocol (aligned with 5.x) (standard library + `net`, **zero third-party dependencies**),
adapted to RocketMQ 4.x / 5.x clusters; aligned item-by-item with this repository's Python / C++ / C# / Rust implementations.
**There are currently 9 real-cluster tools** (see "Real-cluster joint debugging"), covering send / consume / pull / lite pull /
POP / redelivery and dead letter / shutdown race / **admin** / **compression matrix** (the latter driven by `compression_matrix.sh`,
both the send and receive ends are itself, so it is not counted as an independent entry); the remaining scenarios have real-cluster tools
on the other four ports but the Go side has not yet filled them, so this **does not claim that "all capabilities have been jointly debugged"**.
There is also a **cluster-independent** offline self-check
(`go run ./examples/selfcheck`, see "Offline self-check") — it aligns with the same-named tools on the Python / C++ / C# three ports,
and is **not** a Java mechanism (`rocketmq-client` has no self-check entry at all, only the inspection commands on the `mqadmin` side).

Layering:

- `remoting`: the wire protocol (frame encode/decode, header, both the JSON and RocketMQ binary serialization paths) and long-connection transport
- `common`: message model, 17-segment store-format encode/decode, compression, namespace, constants, validators
- `client`: `MQClientInstance`, Producer, Push/Pull/LitePull Consumer, Admin, transaction, Request-Reply, message trace

The Go version is a synchronous API (blocking calls + internal goroutine), the same shape as this repository's Python implementation.

Implemented scope:

| Layer | Content |
| --- | --- |
| Protocol layer | `RemotingCommand` frame encode/decode; the `CommandCustomHeader` family (including V2 single-letter short keys a..n); JSON and RocketMQ binary dual serialization; 17-segment store format + 6-segment batch format |
| Transport layer | Lazy connection establishment + reuse, sync / async / oneway, half-packet reassembly, opaque matching, timeout, reconnect, GO_AWAY(1500), in-flight requests immediately marked dead on connection break, broker-initiated request dispatch, TLS (`crypto/tls`) |
| Send | Sync / designated / batch / oneway / queue selector / async (real kernel + backpressure semaphore + bounded queue) / transactional message (two-phase + broker check-back + unidirectional check thread) / scheduled message recall (recallMessage 370) / Request-Reply |
| Consume | Push Consumer (long polling + orderly + broadcast + offset persistence + startup numeric validation + flow control before pull + OFFSET_ILLEGAL freeze and rebuild + 220 offset reset + **POP mode**), Pull Consumer (caller-held cursor + long polling; `FetchSubscribeMessageQueues` returns the whole topic while `FetchMessageQueuesInBalance` returns only this instance's share), Lite Pull Consumer (**dual-cursor engine**: pull cursor / consume cursor / in-memory commit table, 361 + LITE bit) |
| Namespace | Two independent mechanisms: `Namespace` (a client-side resource prefix `%%ns%%res`, `common/namespace.go`, wrapped and unwrapped across send/consume/heartbeat/offset) and `NamespaceV2` (the server-side namespace: `NamespaceRpcHook` stamps `nsd=true` / `ns=<value>` on every request; hook order Namespace → Stream → ACL, so both `ns` and `ReqT` sit inside the ACL signature). The producer, all three consumers and the admin client read `NamespaceV2` live |
| Consume-side response | `GET_CONSUMER_RUNNING_INFO(307)` (three-level attributes + subscriptionSet + mqTable/mqPopTable mutual exclusion + statusTable), `CONSUME_MESSAGE_DIRECTLY(309)` (concurrent/orderly two sets of judgments + panic → CR_THROW_EXCEPTION), `CONSUMER_SEND_MSG_BACK(36)`, `GET_CONSUMER_STATUS_FROM_CLIENT(221)`, `RESET_CONSUMER_CLIENT_OFFSET(220)` |
| Consume-side stats | `ConsumerStatsManager`: five groups of StatsItemSet (CONSUME_OK/FAILED_TPS, CONSUME_RT, PULL_TPS/RT), cumulative + two-level sampling chain, the snapshot is a **differential window**; the statusTable of 307 is exactly this |
| Queue allocation | Six strategies: `AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<inner>`, pluggable, driven by real rebalance; when NEARBY's resolver yields an empty machine room it reports via `AllocateErrReporter`, and rebalance keeps the existing allocation (aligned with Java's exception-abort semantics) |
| Admin | `DefaultMQAdminExt`: topic / subscription group CRUD, cluster info, consume stats, message query (key / uniqKey / msgId), offset read and broker-side reset, message trace query; **real-cluster tool `examples/live_admin` 29 items** (the admin side previously only had unit tests, no real-cluster tool — the first real-cluster run caught a serialization panic when `groupRetryPolicy` was nil) |
| Message trace | Client trace production: Pub / SubBefore / SubAfter / EndTransaction / Recall five record types, **byte-by-byte** aligned with Java `TraceDataEncoder` (truth vectors in the unit tests); `AsyncTraceDispatcher` (2048 bounded drop + batch 20 + 128K sharding + 5s flush + shutdown flushes the tail batch), the internal producer and the topic prefix as two anti-self-swallowing gates, W3C `traceparent` injection and pass-through (`ROCKETMQ_TRACE_CONTEXT_ENABLE`) |
| 5.x capabilities | Request-Reply (326 holder), recall handle v1 encode/decode, POP consume (200050/200051/200052 + checkpoint deconstruction), consume-side status and running-info response (221 offset table / 307 / 309), five types of hooks, ACL signature (`HmacSHA1`, standard library), dynamic name server addressing, fault-avoidance queue selection, scheduled message three setters (`SetDelayTimeSec/SetDelayTimeMs/SetDeliverTimeMs`), admin-side `UpdateNameServerConfig(318)`/`GetNameServerConfig(319)`/`ConsumeMessageDirectly(309)`, producer-side `SendMessageWithVIPChannel` (the send RPC goes through the VIP port port-2) |
| Offline self-check | `examples/selfcheck`: **cluster-independent** protocol-layer self-check 10 items (JSON / binary dual-frame round-trip, `clientID` key spelling, V2 single-letter short-key set, 17-segment message round-trip including derived msgId, magic-v2 super-long topic handcrafted fixture, 6-segment batch, Crc32 vectors, ACL signature injection) |
| Validation gate | `Validators` / `TopicValidator`: group name / topic validation runs locally **before** `start()` creates the client instance; failure does not touch the network |

**All three compression types are present (ZLIB / LZ4 / ZSTD), all hand-written**: the standard library only has zlib, and this module
promises zero third-party dependencies, so both LZ4 and ZSTD are self-implemented per spec — `common/lz4.go` is the LZ4 **Frame** format (block format + frame header +
xxh32 HC/content checksum, same wire as Java `LZ4FrameOutputStream`, Python `lz4.frame`, lz4 CLI),
complete in both directions (the encoder has a real matcher, blocks that cannot compress smaller are stored raw via bit31).
The **decoder of `common/zstd.go` reads the complete zstd frame** (Raw/RLE/Compressed three block types, Huffman single/four streams,
FSE sequences, repeat offsets, all frame header fields, concatenated frames/skippable frames, xxh64 content checksum), and can directly decode the bytes
produced by zstd-jni and the zstd CLI; **the encoder only produces legal store-only frames (Raw/RLE blocks)** — the peer reads them correctly,
only the ZSTD body sent by Go does not shrink (in the matrix, storeSize≈payload size is the evidence for this). Completing the full encoder
would require also writing an FSE/Huffman encoder, a deliberate trade-off rather than an omission (2026-10-09). Anything that cannot be decoded **loudly reports `unsupported` /
decode error**, and never passes a compressed stream through as the body (a wrong decode on the consume side is silent garbage).
The Go leg of `scripts/compression_matrix.sh` (cross-language compression matrix) participates under all three of zlib / lz4 / zstd
(`examples/live_compression_matrix`, 9 directions: go↔go / go↔python / go↔cpp / go↔net / go↔rust).

## Build and checks

```bash
cd go
go build ./...
go vet ./...
gofmt -l .        # empty output is the pass
go test ./...     # 502 cases (466 test cases + 36 subtests), ~10s
go test -race ./client/   # concurrency regression (lite consumer, async send, offset-commit floor)
```

## Unit tests

All 535 test functions live in the same directory as the source (`*_test.go`); some of them run on an **in-process fake cluster**
(`clusterFixture` in `client/consumer_test.go`: a fake broker with real socket listeners + a fake name server,
scripted responses, able to pin down request codes, ext field names and retry classification):

| Package | Count | Coverage |
| --- | --- | --- |
| `client` (287) | producer 19 | six send paths, retry classification (retryable code switches broker / non-retryable code throws immediately / budget exhausted), batch ID order, send header c/d/n, send header guards |
| | hooks 14 | `CheckForbiddenHook` called on every attempt and does not swallow exceptions, `FilterMessageHook` swallows exceptions and the rest still runs, ACL signature string concatenation (key ordering, only values concatenated, skip Signature) |
| | async 27 | real async kernel: switch broker switch opaque, backpressure semaphore, bounded queue full throws synchronously, callback exactly once, budget sharing |
| | transaction 6 | two-phase + check-back response, unidirectional check thread |
| | request_reply 13 | 326 holder, Request/AsyncRequest timeout |
| | consumer 29 | long polling, orderly redelivery gate, flow control, offset five RPCs, OFFSET_ILLEGAL, 220, broadcast, shutdown in-flight consume join + send-back guard, three offset-commit floor rules (partial ack commits the whole batch / failed send-back pins the offset / out-of-order batches do not jump back) |
| | consumer_stats 19 | differential window metric, 10s/10min two-level sampling, `consumeRT`'s exclusive hour fallback, `consumeFailedMsgs` takes the hour sum, key is topic@group, two record paths for pull and consume |
| | consumer_running_info 19 | **307** empty-body six keys and `jstack` absent when not set, `mqTable`/`mqPopTable` **inline object keys asserted by raw bytes**, classic vs POP two tables mutually exclusive, statusTable contains `%RETRY%` and is taken from the stats manager, processQueueInfo 14 keys / popProcessQueueInfo 3 keys, **309** concurrent and orderly two sets of judgments + `autoCommit` read after the listener + panic→CR_THROW_EXCEPTION + redelivery topic restoration + two error arms end-to-end |
| | pop 15 | checkpoint 8-segment deconstruction (including `1ST_POP_TIME`), ACK uses the checkpoint offset, failure changes the invisible time, `checkNeedAckOrDelay` two branches, 401 request mode, `order`/`suspend` always present in the message |
| | lite_pull 9 | **361 + LITE bit online**, dual cursor (NO_NEW_MSG also follows nextBeginOffset), Seek drops the buffer, the commit table is a **sweep** not a filter, pause-resume gate, subscribe-mode rebalance + shutdown persistence |
| | pull_consumer 18 | short polling without the SUSPEND bit, long polling truly suspends, caller cursor, sendMessageBack |
| | admin 35 | topic/group CRUD, paging merge, 222's `isForce` key name, the body of code 26 is Properties text |
| | instance 15 | instance table reuse, route refresh, published address only recognizes master, unregister 35 iterates master and slave, 220/221/40 broker-initiated requests |
| | route 10 | route table published slot (`brokerAddrs` bare numeric keys), TBW102 fallback, unknown topic retry window |
| | offset_store 9 | local/broker dual tables, persistAll sweep semantics |
| | trace 30 | Java encoder **byte-by-byte** truth vectors, decode fault tolerance (empty segment without keys, a bad record only skips itself), SubBefore/SubAfter share requestId + contextCode five levels, traceparent injection and validation, dispatcher anti-self-swallowing, and two **in-process real-cluster** end-to-end paths (Pub lands on the trace topic without recursion / consume pair lands to disk and pairs up) |
| `common` (111) | 111 | 17-segment encode/decode (bad data rejected, compressed-segment crc32), message model, clientId metric, recall handle truth vectors, namespace, sysflag bit table, ExtraInfo 8 segments, validators |
| `remoting` (137) | 137 | real socket round-trip (sync/async/oneway, half-packet, concurrent opaque, silent timeout), TLS, ACL signature, V2 short-key name guard (one wrong letter **silently drops the field**), JSON fault tolerance (bare numeric keys, object key, NaN), **fastjson2 outbound writer** (MessageQueue inline object key + Java double format), `CurrentVersion`/`CurrentVersionDesc` paired guard, heartbeat assembly, POP and ClientInfo body shape, subscription group config (**nil `groupRetryPolicy` must drop the key rather than panic**) |

## Offline self-check

A protocol-layer smoke test that needs neither a cluster nor running the whole unit test suite:

```bash
cd go
go run ./examples/selfcheck
```

10 items, each printing `[PASS]` / `[FAIL]`; any failing item ends the process with a non-0 exit code, closing line
`selfcheck: ALL PASS (PASS=10 FAIL=0)`. Same name and purpose as `python -m selfcheck` (7 items),
`cpp/examples/selfcheck.cpp` (3 items), and `csharp`'s `selfcheck` subcommand (3 items);
it is the cheapest gate **before touching a real cluster**. **The Rust side has no such tool** (`rust/examples/` is entirely cluster-requiring
`live_*`); its protocol-layer offline coverage is carried by `cargo test` unit tests — so this is 4/5-port alignment here.

Coverage: JSON frame round-trip (including **unescaped** `<>&`, Chinese remark, extFields exact equality), ROCKETMQ private binary frame round-trip
(and asserting the protocol type really rides in the high bits of the packed header length), `HEART_BEAT`'s `clientID` **key spelling**, `SendMessageRequestHeaderV2`
being **exactly** single-letter keys `a..n` (writing one extra long name would be silently dropped by the broker, and verifying only the value wouldn't catch it), 17-segment message round-trip
(including the derived `msgId`/`offsetMsgId`), magic-v2 super-long topic (>255B) decode, 6-segment batch round-trip, `msgId` reverse parsing,
Crc32 standard vectors, ACL signature injection (AccessKey/SecurityToken must enter extFields **before** computing the signature, SecretKey never goes on the wire).

Two spots are deliberately made **byte-level** rather than just "round-trip once": the magic-v2 one is a **handcrafted** broker frame with hardcoded
properties (if both encode and decode are your own, a symmetric bug would let the round-trip pass anyway); Crc32 pins vectors directly — because Java's `UtilAll.crc32`
returns `(int)(value & 0x7FFFFFFF)` (drops the highest bit), differing from standard CRC-32 by 2^31, so across languages you **must never**
compare crc directly (only compare each side's `match` field; on the decode side `CheckCRC` is off by default, which is what makes interop hold).

## Real-cluster joint debugging

Requires a cluster running nameServer(9876) + broker(10911) with `autoCreateTopicEnable=true`
(the shutdown-race one additionally requires `traceTopicEnable=true`, otherwise `RMQ_SYS_TRACE_TOPIC` is not pre-created).
These tools are **not part of `go test`** and depend on an external cluster; all are self-asserting, any failing item ends the process with a non-0 exit code,
closing line `PASS=<n> FAIL=<n>`:

```bash
cd go
go run ./examples/live_producer       -ns 127.0.0.1:9876   # six send paths, SendResult shape (msgId==UNIQ_KEY / offsetMsgId / queueOffset / regionId), transaction two-phase + broker check-back, async kernel (callback exactly once / designated / selector / batch / backpressure permit return)
go run ./examples/live_consumer       -ns 127.0.0.1:9876 -topic T -group G -expect 12 -orderly-topic T2 -orderly-expect 6
                                                          # S1 receives all with no duplication or loss (data fed in by the Python side), S2 a second instance of the same group receives nothing, S3 client-side secondary tag filtering, S4 orderly consume, S5 two-instance split, S6 graceful unregister
go run ./examples/live_pull           -ns 127.0.0.1:9876   # manual pull: Min/Max/SearchOffset, queue-head short polling does not suspend, empty queue-tail NO_NEW_MSG does not suspend, long polling wakes up, caller cursor, KEYS preserved
go run ./examples/live_lite_pull      -ns 127.0.0.1:9876   # assign mode (dual-cursor advance / Commit / restart continues reading without replay / Seek replays), subscribe-mode rebalance + auto-commit read back from the broker
go run ./examples/live_redelivery     -ns 127.0.0.1:9876 [-legs s1,s2,s3,s4]
                                                          # S1 %RETRY% second delivery + delayLevel 3 delay gradient + topic restoration, S2 maxReconsumeTimes=2 ⇒ 3 deliveries then %DLQ% with recon=3, S3 orderly poison message takes the "wait for broker send-back" DLQ path, S4 ackIndex partial ack (acked ones not sent back / offset still commits the whole batch / control group sends none back)
go run ./examples/live_shutdown_race  -ns 127.0.0.1:9876   # data-loss contract of immediate shutdown / immediate process exit: two rounds of concurrent send-back (%RETRY% → %DLQ%), orderly suspended send-back, short-lived trace producer flushes the tail batch
go run ./examples/live_admin          -ns 127.0.0.1:9876   # admin 29 items: cluster liveness and master election, topic CRUD (route/config/list consistent), broker config and runtime KV, KV config write/read/delete, subscription group CRUD, send 8 to verify topicStats, **four offset queries** (Max/Min/LOWER/UPPER boundary/earliest store time), write the offset to the broker then read it back with another RPC, KEYS index query + viewMessage to fetch the body, confirm it is gone after deleting the topic
go run ./examples/live_pop            -ns 127.0.0.1:9876   # POP consume
```

Scripts (**bring up the cluster + wait for ports + run verification + wrap up, all within the same command**, do not split them):

```bash
bash scripts/run_go_producer_live.sh        # Go produce → Python read back
bash scripts/run_go_consumer_live.sh        # Python produce → Go consume → Python reads back offset/unregister from the broker
bash scripts/run_go_pull_live.sh            # Go pull
bash scripts/run_go_redelivery_live.sh      # redelivery / dead-letter terminal state / orderly dead letter / ackIndex partial ack (running all takes about 5~7 minutes)
bash scripts/run_go_shutdown_race_live.sh   # shutdown race (counterpart to Rust's live_shutdown_race)
bash scripts/run_go_pop_live.sh             # POP consume
bash scripts/run_go_admin_live.sh           # admin (29 items, self-asserting; the tool creates/deletes its own topic, subscription group, KV namespace)
bash scripts/compression_matrix.sh zlib     # cross-language compression matrix (seven ports tested against each other, including Go's 9 directions; lz4 / zstd use the same script with a different codec)
```

**Real-cluster scenarios not yet covered** (the other four ports already have corresponding tools, the Go side pending): `OFFSET_ILLEGAL` freeze-and-rebuild and 220
offset reset (currently only unit tests), pull flow control five levels, heartbeat panorama (203/38, 300, slave fan-out), six allocation strategies on a real cluster
(`MACHINE_ROOM_NEARBY` implementation added on 2026-10-01, see `client/allocate_nearby_test.go`),
`cleanExpiredMsg` sweep, scheduled/delayed messages and key query, Request-Reply(326), recall recallMessage(370),
ACL, TLS, SQL92, `MACHINE_ROOM_NEARBY` on a real cluster,
and the **real broker round-trip of 307/309** (`mqadmin consumerStatus -s` goes through these two; currently only
the line shape and Oracle consistency have been verified on the in-process fake cluster, never having a real broker actively ask).

Intentional differences remaining versus the Java client (zhaohai666-rocketmq 5.x): a batch message's msgId takes the batch's own
UNIQ_KEY (aligned with Rust/C++).

## Directory structure

```
go/
├── go.mod                      module github.com/zhaohai666/rocketmq-client-remoting/go (go 1.24, zero dependencies)
├── common/
│   ├── message.go                  Message / MessageExt / MessageQueue
│   ├── message_decoder.go          17-segment store format + 6-segment batch format
│   ├── compression.go              ZLIB/LZ4/ZSTD dispatch (same metric as Java CompressionFactory)
│   ├── lz4.go / zstd.go          hand-written LZ4 Frame and ZSTD frame (zero third-party dependencies; zstd decodes the full format, encodes store-only)
│   ├── recall_handle.go            scheduled message recall handle v1 (base64url + 5 segments)
│   ├── buffer.go / sysflag.go / mixall.go / util.go
│   ├── namespace.go / topic_validator.go / validators.go
│   └── logging.go                  env-var-configured file/stdout logging
├── remoting/
│   ├── remoting_command.go         frame encode/decode
│   ├── headers.go / codes.go       request header family (including V2 short keys) and constants
│   ├── serialize.go                JSON and RocketMQ binary dual serialization
│   ├── client.go                   long-connection transport: sync/async/oneway + half-packet + TLS + mark dead
│   ├── heartbeat.go / subscription.go / bodies.go / admin_bodies.go
│   ├── acl.go / rpchooks.go        ACL signature and RPC hooks
│   └── json_value.go               fastjson2 fault-tolerant parsing
├── client/
│   ├── instance.go                 MQClientInstance: route discovery + all RPCs + heartbeat + periodic tasks
│   ├── producer.go / async.go / transaction_producer.go / request_reply.go
│   ├── consumer.go / consume_service.go / process_queue.go / pool.go   push consumer and executor
│   ├── pull_consumer.go            DefaultMQPullConsumer
│   ├── lite_pull_consumer.go       DefaultLitePullConsumer (dual cursor)
│   ├── pull_api.go                 pullKernel (classic 310/361 two paths)
│   ├── admin.go / admin_api.go / admin_offset.go / admin_track.go / admin_util.go
│   ├── allocate.go                 six queue allocation strategies
│   ├── trace.go / trace_hook.go / trace_dispatcher.go / trace_context.go   trace encode/decode / hooks / async dispatch / traceparent
│   ├── offset_store.go / route.go / broker_api.go / hooks.go
│   ├── fault_strategy.go / semaphore.go / listener.go / send_result.go
│   └── validators go through common
└── examples/                   selfcheck (cluster-independent) + 9 real-cluster tools (see above)
```

## Several implementation conventions you must know

**Field names match the on-wire message verbatim.** The broker uses fastjson2 to deserialize by property name; one wrong field name
**silently drops the field** (no error, no error code). The ext key-name guards in `remoting/headers.go` and in the unit tests exist
precisely to hold this — do not change them to names that "look more natural".

**Short polling must never carry the SUSPEND bit.** Go's pull-side `Pull()` (short polling) does not set `FLAG_SUSPEND`:
if the suspend bit leaks into short polling, the broker holds the request for the entire suspend budget while the client has long since timed out —
on an empty queue this is a **guaranteed timeout**, the most expensive pitfall on this path. Only long polling (`PullBlockIfNotFound`) sets the bit.

**POP queues come from the client-side rebalance, not from broker assignment.** When `clientRebalance=false`, Java asks the broker
via `RebalanceImpl#getRebalanceResultFromBroker:345` → `MQClientAPIImpl#queryAssignment:405` (QUERY_ASSIGNMENT=400, returning
`MessageQueueAssignment` with mode=POP), so the broker decides which queues this instance owns. That path is **deliberately not
implemented here** (the same decision as the python / C++ / C# / Rust / Node.js / PHP ports): queues are still computed locally by
the allocation strategy, then one POP loop + ack per queue. The semantics are equivalent; only WHO picks the queue set differs.
Read the `doRebalance` comment in `client/consumer.go` before changing this.

**The lite consumer runs a dual cursor.** The pull cursor (PULL cursor) follows `nextBeginOffset` after **every** response
(FOUND / NO_NEW_MSG / NO_MATCHED_MSG / OFFSET_ILLEGAL treated alike, the "intact" guard catches queues that were revoked); the consume cursor (CONSUME cursor) only advances when `Poll()` actually delivers,
and is the sole source of commits. Mixing the two cursors means replay or missed consumption.

**The commit table is a sweep, not a filter.** `persistAll` / `commitAll` delete entire entries **out of range** from the table
(Java RemoteBrokerOffsetStore: "offset is not in mqs, remove it") — stale offsets outside the cursor holders evaporate with this commit, and this is deliberate Java semantics. An empty table skips the whole thing without touching the network, and a `-1` cursor
does not go on the wire ("consumerOffset is -1").

**The offset-commit floor is computed per Java `ProcessQueue#removeMessage`.** The target of concurrent ack is "the smallest offset **still remaining** in the buffer", falling back to `queueOffsetMax + 1` when the buffer is emptied; what gets removed is **the batch that has been acked**
(`consumeRequest.getMsgs()` minus those whose send-back failed), so under `ackIndex` partial ack what pins the offset is those "not acked and still in the buffer", **not including** the tail already returned to the broker. Both counterexamples were actually tested:
excluding the send-back-**failed** ones from the floor makes the offset jump over them (a crash loses them); treating the target as "the last of this batch + 1" makes the offset stay at this batch when the higher batch finishes first (reads back 0 on a real broker, whereas Java gives 3). The orderly side **differs**:
Java goes through `commit()` = this batch's `lastKey + 1` (orderly is inline consumption, only one batch runs at a time per queue).
Mixing the two either loses messages or pins the queue dead.

**The two dead-letter upper limits do not use the same comparison operator.** `CONSUMER_SEND_MSG_BACK(36)` goes through
`AbstractSendMessageProcessor.consumerSendMsgBack`, whose criterion is `reconsumeTimes >= maxReconsumeTimes`;
the orderly side's "normal send to `%RETRY%`" goes through `SendMessageProcessor.handleRetryAndDLQ`, whose criterion is
`reconsumeTimes > maxReconsumeTimes` (**strictly greater than**), and it first checks
`RebalanceLockManager.isLockAllExpired` — if the group still holds the queue lock it goes straight into `%DLQ%`, never queuing at all.
Additionally, the `+1` of `RECONSUME_TIME` is added on the **client** only on the orderly side (`ConsumeMessageOrderlyService:350`);
the `+1` on the concurrent side is added by the **broker**, and the client writes the original value.

**lite is an independent on-wire identity.** Request code 361 + the `FLAG_LITE_PULL_MESSAGE` sysFlag bit + the LITE bit of the heartbeat
`ConsumerData` + `CONSUME_ACTIVELY`; the heartbeat is a consumer-self-held loop (not entering the instance's
consumer_table, instance-level heartbeats only summarize that table ⇒ without its own loop the group does not exist on the broker at all),
fanning out to all master + slave addresses.

**Compression is done only once, outside the retry loop.** If `setBody` is done in-place inside the retry loop, a retry compresses the already-compressed body again
(`zlib(zlib(x))`), and the consume side decodes only one layer and hands out the compressed stream as the body.

**The allocation-strategy guards return empty results rather than exceptions.** Empty-string `currentCID` / empty `mqAll` / empty `cidAll`
**return an empty allocation**; `MACHINE_ROOM_NEARBY`'s resolver yielding an empty machine room **reports an error** (silently returning empty is equivalent to removing all queues of the whole topic).

**Client local-validation errors have no response_code.** Only the body level of `check_message` carries
`MESSAGE_ILLEGAL(13)`; "sending a message to `SCHEDULE_TOPIC_XXXX`" reports a **codeless** error — the upper layer must know this when branching on
`response_code`, consistent across the five languages.

**Trace anti-self-swallowing has two gates, both indispensable.** The internal dispatcher producer has its own `enableTrace=false`,
and the trace hook skips messages whose topic prefix is a trace topic — keeping only one, trace messages would generate traces for their own traces, amplifying traffic exponentially. The trace dispatcher shuts down **last** in `Shutdown()` and flushes the tail batch (records
just accumulated in the queue wait until they truly land on the broker before returning), otherwise short-lived clients / processes that exit immediately lose the last message.

| Environment variable | Default | Description |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `TRACE` / `DEBUG` / `INFO` / `WARN` / `ERROR` |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` | log directory |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_go_client.log` | log file name; empty string / `OFF` / `NONE` = disable file persistence and keep only stderr; when it contains a path separator the whole thing is treated as a path |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | empty | any non-empty value = write to stdout instead of a file (takes precedence over the two above) |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864` (64MB) | per-file limit, rotate by size (same value as Java logback `<maxFileSize>64MB`); `0` = no rotation |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | number of backups (same value as Java `rocketmq.log.file.maxIndex`), backup names `<file>.1` … `<file>.N`; `0` = keep no backups, truncate in place |

**The log file name deliberately does not use Java's `rocketmq_client.log`** (the same trade-off as cpp / csharp / rust / nodeJs / php):
when Java clients run on the same machine at the same time, two processes interleave lines into the same file, and whoever rotates first renames the other's file —
the JVM still holds the old fd, after which its logs are silently written into an already-unlinked inode. When forced alignment is needed, just set
`ROCKETMQ_CLIENT_LOG_FILE=rocketmq_client.log`. The unit tests for persistence and rotation are in `common/logging_test.go`
(the rotation window can only be produced with a small limit; the real-cluster scripts can only verify "whether a file exists").
| `ROCKETMQ_SERIALIZE_TYPE` | `JSON` | wire-protocol serialization choice (JSON / ROCKETMQ) |
| `ROCKETMQ_TLS_ENABLE` | `false` | when on, all outbound connections use TLS |
| `ROCKETMQ_TLS_TEST_MODE` | `true` | trust self-signed certificates, do not verify hostname |
| `ROCKETMQ_TRACE_CONTEXT_ENABLE` | empty | `1` / `true` / `yes` = inject W3C `traceparent` on send (if present, pass it through to the consume side) |

**clientId metric**: `<local IP>@<instanceName>[@<unitName>][@STREAM]`; when `instanceName` is
`DEFAULT` it is rewritten in place inside `start()` to `<pid>#<nanoTime>` (producer and CLUSTERING consumers;
broadcast consumers keep `DEFAULT`, and broadcast consumers in the same process share one instance). The local IP is probed by "connecting" a public
address over UDP and reading the sockname (no packet sent); if it cannot be obtained it falls back to `127.0.0.1`.

## License

Apache-2.0.
