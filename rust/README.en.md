# rocketmq-client-remoting (Rust)

> [中文](README.md) | English

A Rust implementation (fully async on tokio) of Apache RocketMQ's classic remoting
protocol (aligned with 5.x), compatible with RocketMQ 4.x / 5.x clusters. All
capabilities were integration-verified against a real 5.5.1 cluster, feature by feature
against the Python / C++ / C# implementations in this repository.

Layers:

- `remoting`: the wire protocol (frame encoding/decoding, headers, both JSON and RocketMQ binary serialization paths) and long-connection transport
- `common`: message model, the 17-segment store-format codec, compression, namespace, constants, consistent-hash ring
- `client`: `MQClientInstance`, Producer, Push/Pull/LitePull Consumer, Admin, hooks and tracing

**All outward-facing RPCs are `async`** (tokio). Python's `None` default parameters
become `Option<...>` + explicit arguments here, with the default values given by
same-named constants.

Implemented scope:

| Layer | Contents |
| --- | --- |
| Protocol layer | `RemotingCommand` frame encoding/decoding; the `CommandCustomHeader` family (including the V2 single-letter short keys a..n); dual JSON and RocketMQ binary serialization; the 17-segment store format + the 6-segment batch format |
| Transport layer | Lazy connection establishment + reuse, synchronous / asynchronous / oneway, half-packet reassembly, opaque matching, timeouts, reconnection, **GO_AWAY(1500): re-establish the connection and resend exactly once**, dispatching broker-initiated requests, TLS (`native-tls`), real `MQVersion` (5.5.1) request headers |
| Routing / heartbeat | `TopicRouteData` / `QueueData` / `BrokerData`, `SubscriptionData`, `HeartbeatData`, `TopicPublishInfo` queue-rotation cursor |
| Client | `MQClientInstance` (process-level instance-table reuse), `DefaultMQProducer` (sync / select-one / batch / oneway / selector / async / transaction / transaction check-back), `DefaultMQPushConsumer` (long polling + **POP** + ordering + broadcasting + offset persistence), `DefaultMQPullConsumer` (`fetch_subscribe_message_queues` returns the whole topic while `fetch_message_queues_in_balance` returns only this instance's share), `DefaultLitePullConsumer`, `DefaultMQAdminExt` |
| Namespace | Two independent mechanisms: `namespace` (a client-side resource prefix `%%ns%%res`, `common/namespace_util.rs`, wrapped and unwrapped across send/consume/heartbeat/offset) and `namespace_v2` (the server-side namespace: `NamespaceRpcHook` stamps `nsd=true` / `ns=<value>` on every request; hook order Namespace → Stream → ACL, so both `ns` and `ReqT` sit inside the ACL signature). The producer, all three consumers and the admin client read `namespace_v2` live |
| Queue allocation | Six strategies: `AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<inner>`, pluggable and driven by real rebalancing |
| 5.x capabilities | POP (`POP_CK` 8-segment reconstruction / `ACK` / `CHANGE_MESSAGE_INVISIBLE`), Request-Reply, message tracing (encoding + async dispatch + hooks), consumer-side statistics, `ConsumerRunningInfo`(307), metrics, ACL signing, dynamic name server addressing, fault-avoiding queue selection |
| Validation gates | `Validators` / `TopicValidator`: `check_topic` / `check_group` / `is_system_topic` / `is_not_allowed_send_topic` / `check_message`; the `start()` of all four facades finishes group-name validation **before** creating the client instance (pure local judgment; a failure never touches the network) |
| Compression | Three backends zlib / LZ4 Frame / ZSTD: automatic compression on the producer side + automatic decompression on the consumer side; the wire frame format is interoperable with the other language implementations |

**Not tested**: the Windows / MSVC branch. The TLS branch has been run on real hardware:
the local 5.5.1 cluster, in test mode, sniffs TLS on **the same ports** (nameServer 9876,
broker 10911), and `ROCKETMQ_TLS_ENABLE=1` takes effect directly.

## Dependencies

All dependencies are declared in `Cargo.toml`; **there are no private registries and
nothing extra needs to be downloaded**:

| crate | Purpose |
| --- | --- |
| `tokio` (rt-multi-thread / net / time / io-util / sync / macros) | Runtime and async sockets; dev additionally enables `test-util` (needed for the fake in-process cluster's timing assertions) |
| `serde` + `serde_json` (`preserve_order`) | Protocol body encoding/decoding; `preserve_order` makes `Map` serialize in insertion order so bodies keyed by objects, such as `offsetTable`, reproduce stably |
| `flate2` / `lz4_flex` (`frame`) / `zstd` | The three compression backends |
| `hmac` + `sha1` + `base64` | ACL signature `Base64(HmacSHA1(secretKey, content))` |
| `native-tls` | TLS branch |
| `chrono` | 14-digit local wall clock (`consumeTimestamp`, log timestamps) |

## Build and checks

```bash
cd rust
cargo build
cargo clippy --all-targets     # zero warnings is a hard gate (examples are checked too)
cargo test --lib               # 916 tests, ~4s
```

## Unit tests

The 916 tests distributed by module (recomputable with `cargo test --lib -- --list`):

| Module | Count | Coverage |
| --- | --- | --- |
| `remoting::protocol` | 109 | Header field-name guards (one wrong letter **silently drops the field**), the `codes` constants, admin-side bodies (including **`ResetOffsetBodyForC`**: offsetTable is a JSON **array**, and a parser that only understands the map shape yields an empty table for it, so the whole reset is silently discarded), POP `extraInfo` 8-segment reconstruction, JSON tolerance of fastjson2's non-standard output (bare numeric keys, object keys, NaN/Infinity, trailing commas), RocketMQ binary roundtrip, the **`bname`** key-name guard of `RecallMessageRequestHeader`, `boundaryType` (the wire name is the uppercase enum name `LOWER`/`UPPER`, a missing key writes nothing, lenient parsing recognizes only "upper") |
| `remoting::client` | 26 | Real-socket loopback: sync/async/oneway, half-packet reassembly, concurrent requests each matching their own opaque, silent timeout, connection-establishment failure and a bad port, forced reconnection, **resend exactly once after a GO_AWAY reconnect**, broker pushes reaching the processor, RPC hooks executing before encoding; **5 TLS offline regressions** (freshly generated self-signed certificate + an in-process `TlsAcceptor`): resuming reads across half packets, a 1MiB body, 8-way concurrency sharing one connection not starved by read/write lock contention, requests pushed by the broker getting a runtime context on the read thread and being answered outbound, a TLS client hitting a plaintext port must map to a connection error; **5 connection-fatal tests**: when the peer closes right after being read (EOF), a synchronous request gets `Error::SendRequest` at **millisecond** latency **instead** of waiting out 30s and reporting `Error::Timeout` (the retry classification of async sends branches on error **type**), the callback fires exactly once, the in-flight table drains, claims are made by `conn_id` connection identity (in-flight requests on another connection are left untouched), after a dead connection is removed a new connection to **the same address** can be built immediately, `shutdown()` gives in-flight requests a terminal state |
| `remoting::rpchook` | 6 | ACL signing: extFields ordered by key lexicographically, only values concatenated, `Signature` skipped, then the body appended |
| `common::message_decoder` | 38 | Both encoding paths, 17-segment / 6-segment (never mix them), the crc32 of compressed segments (`& 0x7FFFFFFF`), batch messages, bad data must be rejected |
| `common::consistent_hash` | 8 | The ring: the MD5 digest **takes only the first 4 bytes big-endian**, virtual-node keys counted starting from `existingReplicas`, `tailMap` **includes the endpoint**, wrapping past the ring's end, an empty ring returns `None`, a negative virtual-node count errors only at construction |
| `common::compression` | 8 | Roundtrip over the three backends + type parsing (including the `0→ZLIB` compatibility mapping); an unsupported type must throw rather than pass compressed bytes through |
| `common::recall_message_handle` | 6 | True-value vectors for timed-message recall handle v1 (with `=` padding), unpadded handles also decode (cross-client recall), a 6-segment newer version ignores trailing segments, empty string / bad base64 / invalid utf-8 / `v2` / too few segments all yield `recall handle is invalid` |
| `common::boundary_type` | 2 | The boundary enum's uppercase wire names and `get_type`'s lenient parsing |
| Other `common` | 76 | `message` / `message_const` / `message_type` / `message_client_id_setter`, `mix_all` (including the `%NS%` prefix and the clientId conventions), `sysflag`, `util_all` (14-digit wall clock, `nano_time`, `is_blank`), `topic_config`, `topic_validator`, `buffer`, `logging` |
| `client::producer` (including 16 `produce_accumulator` tests) | 120 | Configuration and lifecycle (the clientId convention `<ip>@<pid>#<nanoTime>`, unchanged across restarts, the same instanceName shares one instance; **`shutdown()` empties `INSTANCE_MAP` the moment it returns** — unregistration(35) and instance teardown both run inside `spawn`ed tasks, and a registry that empties one step late would let a restart with the same clientId reuse the very instance being torn down); `send_retry_tests` pins retry classification with an **in-process mock cluster** (retryable codes switch brokers, non-retryable codes throw immediately, exhaustion reports `BrokersSent`, single-attempt timeout clamping, budget exhaustion reports callTimeout, missing route fails fast, connection failures are isolated; **missing addressing reports 10004 rather than 10005** — three legs); wire-field conventions captured from the requests actually sent (`k`=unitMode, `ReqT`, send request codes 310/320/325 with the three-level `m`=batch criteria, the ID order for batch sends: first `setUniqID` on every sub-message → then append one for the whole batch → only then `setBody(encode())`); **27 items for the async send kernel + the backpressure gate**: gate side (the gate is untouched when the switch is off, the message-count/byte gate messages, a rejection must return the message-count permits already acquired, the whole retry chain holds only **one** permit, the gate waits until the budget is exhausted, a resize wakes waiters, an empty body counts as 1 byte, queueing that eats the whole budget reports send kernel timeout), bounded-queue side (queue full throws `executor rejected` synchronously and sends nothing, with backpressure enabled dispatch happens outside the queue and the task still runs to completion), async-kernel side (switching brokers on retry with a fresh opaque for the same request, retry cap, **broker business codes never enter the retry chain**, select-one retries stay on the same broker, each hook runs exactly once, not `start()`ed throws synchronously without invoking the callback), a 32-worker-thread saturation regression; **send headers c/d/n** captured from requests (`d=0` goes on the wire unchanged, `n` takes the broker selected for this send, an empty brokerName makes the whole key disappear); **7 select-one-topic guard items** (a mismatch rejects and sends not one message on the wire, the caller's `Message` has not had a single field touched, the namespace is compared against the wrapped resource name, batches go through the same guard, the async message is handed over once via the callback, not `start()`ed reports the state error first, **select-one oneway deliberately has no guard**) |
| `client::backpressure` | 12 | `FairSemaphore`: only the **head of the queue** can take a permit, a blocked head holds the people behind it in FIFO order, changing the total preserves the in-flight share, changing the capacity wakes waiters, both a head commit and a head timeout must feed the next waiter (miss it and you get the real-machine 5-second dead wait), a discarded waiter leaves no ghost ticket, free permits may go negative and be pulled back positive, the two floor values (10 messages / 1MiB) |
| `client::allocate_strategy` | 30 | Six strategies: boundary cases of `AVG` / `AVG_BY_CIRCLE`, the four `check` guards return an **empty result** rather than throwing, `CONFIG` returns a copy, `CONSISTENT_HASH` walks the hash-ring table slot by slot, the `MACHINE_ROOM` machine-room-name split **trims trailing empty segments** with a truth table, `MACHINE_ROOM_NEARBY` same-machine-room priority + machine rooms with no consumers shared by everyone + a resolver yielding an empty machine room **throws** |
| `client::consumer` / `pull_consumer` / `consume_executor` / `consumer_stats` | 215 | Subscription and `MessageSelector`, **post-start `subscribe`** (still accepted after `start()`, `unsubscribe` only deletes the table entry), the CLUSTERING/BROADCASTING fork of clientId (broadcasting keeps `DEFAULT` and shares the instance), pull/lite state machines, **lite's three offset tables** (the pull cursor / the consumed cursor / the in-memory commit table are each independent, `commit()` only advances the slot handed out by `poll()`; `maybe_auto_commit` is checked only inside `poll()` with one global deadline; `persist_all(scope)` wipes in-memory rows outside the scope); the two-tier core/max elastic executor with **the 5.x default of 20 on both sides**; the **ordered-consumption re-delivery gate** (`-1` reads as unlimited on the ordered side ≠ 16 on the concurrent side, not exhausted means +1 in place and suspend, only exhausted re-delivers, and **only a failed re-delivery** continues to suspend); **120s pull-stall self-healing** (strictly greater, a new loop does not count, a thread that already exited counts immediately, healthy queues are untouched, offsets are persisted on removal, the POP branch reads `lastPopTimestamp`, no stall judgment while stopping); **POP loop-pull statistics** (`Found` records RT and is incremented before the empty-list check, TPS is recorded only when messages are popped, `PollingNotFound` leaves both counters alone); **startup numeric gates** (both ends of 12 ranges each tested once, the `-1` sentinel only for the two topic-level gates, the `pullInterval` lower bound 0, min>max strictly greater, multiple out-of-range values report the first in order, the `consume_timestamp` format genuinely rejects); **empty-response offset correction** (only with nothing pending and nothing in flight + `NoNewMsg`/`NoMatchedMsg` is the offset pushed to `next_begin_offset`, only ever moving up); **OFFSET_ILLEGAL correction** (switch to the corrected value → drop the queue + freeze → persist immediately → wake the rebalance, the freeze covers both places and lasts until the rebuild); **220 reset offset** (only the named queues are touched, the queue generation +1 invalidates old acks); **pull-mode consumer heartbeat** (fake master-slave cluster: message shape `CONSUME_ACTIVELY`/`subVersion=0`, first round one send per master and per slave, the loop re-sends on period, `shutdown()` receives one 35 each with `producerGroup` absent); **FIRST_OFFSET's starting point is the literal 0** (the full request log shows `GET_MIN_OFFSET(31)` appearing 0 times, while the negative-control leg LAST_OFFSET must send `GET_MAX_OFFSET`); **the pull cursor follows `nextBeginOffset`** (`NO_MATCHED_MSG` follows past the whole run, `OFFSET_ILLEGAL` adopts the corrected value, an in-flight seek brakes it — the negative control uses the gate to pull out a deterministic window); **lite request code 361 + lite flag** (`code` and `sysFlag` are taken from the request actually received, the classic control leg is 11 with no lite flag); **14 escape-hatch sweep items** (only scan queues held by this instance, `min(size,16)` per round, strictly greater than the expiry, put back only on a failed re-delivery, two head-of-queue gates, ordered queues are skipped entirely, the re-delivery moment re-checks with containsMessage) |
| The four tracing modules `trace` / `trace_hook` / `trace_dispatcher` / `trace_context` | 104 | Pub / SubBefore / SubAfter / EndTransaction / Recall encode/decode in both directions, the SOH/STX text format, tolerance of the empty segment when there are no keys, a bad record only skips itself, the dispatcher's batching / chunking / recursion prevention, W3C `traceparent` generation and validation |
| Other `client` | 153 | `mq_client` (instance-table reuse, heartbeat assembly, route caching, **the shared-instance close guard**: only when the last tenant leaves is the instance really torn down and removed from `INSTANCE_MAP`; **the shutdown unregistration 35 walks every brokerId — slaves each receive one send too**, asserted together with the heartbeat's master-only division of labor; **consumer heartbeats likewise cover slaves**; a blank group name keeps the whole field off the wire; **220's receive-side parsing**: both the array and the map body shapes parse, the processing is not done on the read thread, a mismatched group is silently dropped); **5 publish-route master-only items** (with only a slave left it cannot resolve while the fallback convention can, an empty cache first refreshes the route exactly once, the client reports `The broker[X] not exist` instead of hitting a slave and trading in for a retryable code, `get_max_offset` uses the same convention); `admin` (properties text, pagination merging, **the ext key name of the 222 request body is `isForce`** — get it wrong and the broker-side value is always false, silently taking the wrong branch); `latency`, `hook`, `request_reply`, `metrics`, `top_addressing`, `result`, `validators` |
| `error` | 3 | Error codes 10001..10007 (`REQUEST_TIMEOUT_EXCEPTION` carried by `Error::RequestTimeout`, `CREATE_REPLY_MESSAGE_EXCEPTION` carried by `create_reply_message`) and `Display` |

## Real-cluster integration

Requires a running cluster with nameServer(9876) + broker(10911) and
`autoCreateTopicEnable=true`. These tools are **not part of `cargo test`** and depend on
an external cluster; if any item fails the process exits with a nonzero exit code, and
`== summary: N passed, M failed ==` is the closing line. Some cases have extra
requirements (stop the broker, a master-slave cluster, broker switches auto-restored,
topics created by the case itself are deleted) documented in each script's header
comments:

```bash
cargo run --example live_protocol           -- 127.0.0.1:9876
cargo run --example live_mq_client          -- 127.0.0.1:9876   # instance reuse / send-and-receive field by field / the five offset RPCs / POP pop-ACK-change-invisibility / batch lock / the shared-instance close guard
cargo run --example live_producer           -- 127.0.0.1:9876   # six send paths / transaction two-phase + check-back / recallMessage / async kernel (drain, concurrency slots, select-one, intercepting hook) / shutdown unregistration 35
cargo run --example live_consumer           -- 127.0.0.1:9876   # long polling / tag filter / %RETRY% re-delivery / %DLQ% dead letters / partial ack / POP / broadcasting / ordered dead letters / explicit COMMIT-ROLLBACK / stall self-healing / NOTIFY wakeup / 307
cargo run --example live_pull_consumer      -- 127.0.0.1:9876   # manual pull with no loss and no duplication, offsets controlled by the caller, long polling really suspends
cargo run --example live_lite_pull_consumer -- 127.0.0.1:9876   # subscribe/assign/seek/poll/committed + both auto_commit states + the three offset tables each counted out on real hardware
cargo run --example live_alloc_strategy     -- 127.0.0.1:9876   # the six strategies really drive rebalancing (criterion: assignment == the offline prediction of running the same strategy on the real mqAll/cidAll)
cargo run --example live_rebalance_and_trace -- 127.0.0.1:9876  # three-strategy end-to-end partitioning on real routes, core/max executors, traces recorded into the broker then decoded back
cargo run --example live_client_modules     -- 127.0.0.1:9876   # dynamic addressing / fault avoidance / statistics / five hook categories / traces through the broker / metrics / request-reply
cargo run --example live_validators         -- 127.0.0.1:9876   # name validation fails locally in sub-millisecond time + addressing-fault characterization (10004)
cargo run --example live_admin              -- 127.0.0.1:9876   # the full admin chain (including boundaryType double boundaries, resetOffsetByQueueId, queryTopicsByConsumer)
cargo run --example live_unit_config        -- 127.0.0.1:9876   # unitName/unitMode/stream (the clientId suffix visible broker-side and the UNIT/UNIT_SUB flags)
cargo run --example live_sql92              -- 127.0.0.1:9876   # requires broker enablePropertyFilter=true
cargo run --example live_backpressure       -- 127.0.0.1:9876   # the real-machine closed loop of the two fair gates + the bounded send queue (including admission after a runtime resize)
cargo run --example live_async_send         -- 127.0.0.1:9876   # async kernel: offsetMsgId read back verbatim, concurrency without cross-talk, batch async, shutdown does not wait for in-flight sends
cargo run --example live_fail_fast          -- 127.0.0.1:9876   # stops the broker once then brings it back (the store is not deleted)
cargo run --example live_send_header        -- 127.0.0.1:9876   # send headers c/d/n and the queue-count arithmetic of auto-created topics
cargo run --example live_flow_control       -- 127.0.0.1:9876   # the five flow-control thresholds before pulling + startup numeric gates (large messages are not compressible, the S1/S4 topics must have exactly 1 queue)
cargo run --example live_scheduled_intervals -- 127.0.0.1:9876  # periodic tasks' initialDelay / fixed rate (including the offset-persistence 10s first jump)
cargo run --example live_subscribe          -- 127.0.0.1:9876   # post-start subscribe heartbeats immediately (criterion: the 300 query sees this group before the 30s cycle does)
cargo run --example live_pinned_guard       -- 127.0.0.1:9876   # select-one topic guard: real routes are not mis-rejected, the rejection is client-side with no broker trace, oneway has no guard
cargo run --example live_correct_tags_offset -- 127.0.0.1:9876  # NO_NEW_MSG/NO_MATCHED_MSG empty responses still push the committed offset up to maxOffset
cargo run --example live_offset_illegal     -- 127.0.0.1:9876   # OFFSET_ILLEGAL: void the whole in-flight/buffered batch and rebuild at the corrected offset; the corrected offset persists immediately
cargo run --example live_reset_offset       -- 127.0.0.1:9876   # 220 reset consumer offset: persist immediately after the broker pushes 220 + void in-flight batches + rebuild queues at the new offset
cargo run --example live_pull_heartbeat     -- 127.0.0.1:9876 127.0.0.1:10911 [127.0.0.1:10931]  # the pull-mode consumer's 203/38/35 (master-slave cluster)
cargo run --example live_lite_pull_cursor   -- 127.0.0.1:9876   # pull cursor: NO_MATCHED_MSG follows past the whole run + OFFSET_ILLEGAL out-of-range self-healing
cargo run --example live_lite_pull_code     -- 127.0.0.1:9876   # lite request code 361 + lite flag: flips litePullMessageEnable at runtime, restores it before exit
cargo run --example live_publish_route_master -- 127.0.0.1:9876 127.0.0.1:10911 [127.0.0.1:10931]  # stops the master once: the four "master-only" conventions across publish / ordered lock / POP / offset reads
cargo run --example live_clean_expired_msg  -- 127.0.0.1:9876   # escape hatch with a hanging listener: sweep re-delivers %RETRY% a second time (about 4 minutes)
cargo run --example live_acl                -- 127.0.0.1:9876 <AK> <SK>   # requires a cluster with ACL enabled (authenticationEnabled=true)
cargo run --example live_compression_matrix -- send|recv|reuse ...       # orchestrated by ../scripts/compression_matrix.sh; the reuse leg verifies the caller's body is still the original plaintext after send
```

## Directory structure

```
rust/
├── Cargo.toml                  tokio + serde_json (+ the three compression backends / hmac+sha1 / native-tls)
├── src/
│   ├── lib.rs                  Exports of the three layers
│   ├── error.rs                Error / Result / client_error_code(10001..10005)
│   ├── common/
│   │   ├── message.rs              Message / MessageExt / MessageQueue
│   │   ├── message_decoder.rs      The 17-segment store format + the 6-segment batch format
│   │   ├── compression.rs          zlib / LZ4 Frame / ZSTD
│   │   ├── consistent_hash.rs      Consistent-hash ring (built-in MD5, takes only the first 4 bytes big-endian)
│   │   ├── recall_message_handle.rs Timed-message recall handle v1 (base64url + 5 segments)
│   │   ├── boundary_type.rs        Boundary semantics for offset lookup by timestamp (LOWER/UPPER + lenient parsing)
│   │   ├── buffer.rs / sysflag.rs / mix_all.rs / util_all.rs
│   │   ├── topic_config.rs / topic_validator.rs
│   │   ├── message_const.rs / message_type.rs / message_client_id_setter.rs
│   │   └── logging.rs              Daily rename rotation + stderr
│   ├── remoting/
│   │   ├── client.rs               Sync / async / oneway + fragment reassembly + TLS + GO_AWAY
│   │   ├── rpchook.rs              AclClientRPCHook (ACL signing)
│   │   └── protocol/               remoting_command / headers (incl. V2 short keys) / codes /
│   │                               serialize / route / heartbeat / body / admin_body /
│   │                               subscription / extra_info(POP 8 segments) / namespace_util /
│   │                               ext_fields
│   └── client/
│       ├── mq_client.rs            MQClientInstance: route discovery + all RPCs + heartbeat
│       ├── producer.rs             DefaultMQProducer (incl. send_retry_tests)
│       ├── consumer.rs             DefaultMQPushConsumer (long polling + POP + ordering)
│       ├── pull_consumer.rs        DefaultMQPullConsumer + DefaultLitePullConsumer
│       ├── allocate_strategy.rs    The six queue allocation strategies
│       ├── admin.rs                DefaultMQAdminExt
│       ├── consume_executor.rs     The two-tier core/max executor
│       ├── hook.rs / latency.rs / consumer_stats.rs / metrics.rs
│       ├── request_reply.rs / top_addressing.rs / validators.rs / result.rs
│       └── trace.rs / trace_context.rs / trace_hook.rs / trace_dispatcher.rs
└── examples/                   31 real-cluster integration tools (see above; none of them is cluster-independent)
```

All unit tests are inlined in `src/**/mod tests` (there is no standalone `tests/`
directory) — they need access to `pub(crate)` internals and the fake clock; properties
that only real hardware can prove all move down into `examples/`.

## A few implementation conventions you must know

**Field names match the wire messages character for character.** The broker deserializes
with fastjson2 by property name, and one wrong character in a field name
**silently drops the field** (no exception, no error code). The `(class name, [field
names])` table in `remoting/protocol/headers.rs` + the JSON tolerance in `serialize.rs`
exist precisely to guard this — do not rename things to something "that looks more natural".

**POP queues come from the client-side rebalance, not from broker assignment.** With
`clientRebalance=false`, Java asks the broker through
`RebalanceImpl#getRebalanceResultFromBroker:345` → `MQClientAPIImpl#queryAssignment:405`
(QUERY_ASSIGNMENT=400, returning `MessageQueueAssignment` with mode=POP), so the broker decides
which queues this instance owns. That path is **deliberately not implemented here** (the same
decision across all seven ports); the `client_rebalance` field stays `true` and only mirrors the
Java shape. Queues are computed locally by the allocation strategy, then one POP loop + ack per
queue — semantically equivalent, differing only in WHO picks the queue set.

**Allocation-strategy guards do not throw.** An empty-string `currentCID` / an empty
`mqAll` / an empty `cidAll` **return an empty result**. The two exceptions are cleanly
separated: a `MACHINE_ROOM_NEARBY` resolver yielding an empty machine room **throws**
(silently returning empty equals withdrawing every queue of the whole topic), and missing
parameters are ruled out at construction time by the types.

**`get_name() -> &str` forces names to be computed at construction time.** The composite
name `MACHINE_ROOM_NEARBY-<inner>` is stored as a `String` in `new()`; new decorator
classes must likewise fix their names at construction — never return a reference to a
temporary value.

**The semantics of the machine-room-name split are not "split on @".** The reference
semantics discard **trailing empty strings**, which Rust's native split does not. When the
machine-room name is cut out of the clientId, this single point directly determines which
group `MACHINE_ROOM` assigns to (the truth table is in the unit tests, 8 rows).

**Shutdown unregistration `UNREGISTER_CLIENT`(35) hits master + slave, while heartbeats
only hit the master.** The unregister sweep visits **every brokerId** in
`brokerAddrTable`, while heartbeats use master-prefixed selection — the two divisions of
labor correspond to `get_all_broker_addrs()` and `get_route_of_all_brokers()`; do not
merge them into one read. For a blank group slot the **whole field stays off the wire**
(not an empty string): the broker dispatches on `group != null`, and an empty string would
look up the subscription group with `""`. The timeout is 3000ms, and a single-broker
failure only logs a warn — the shutdown path must not throw because of network jitter.

**Compression runs once, outside the retry loop.** If compression happened **inside** the
retry loop and mutated the body in place with `setBody`, a retry would compress the
already-compressed body again (`zlib(zlib(x))`), and the consumer, which decompresses only
one layer, would hand out the compressed stream as the payload. Lifting compression out of
the loop is deliberate — a workaround for a real bug, not a stylistic difference.

**Client-side local validation errors carry no `response_code`.** Only `check_message`'s
body-size gate carries `MESSAGE_ILLEGAL(13)`; "sending to `SCHEDULE_TOPIC_XXXX`" reports a
**codeless** error — upper layers branching on `response_code` must know this, and the
four language ports stay consistent.

**No `unwrap` / `expect` outside test paths.** Only one occurrence remains in the whole
repository: `Hmac::new_from_slice(...).expect("HMAC accepts any key length")` in
`rpchook.rs` (it can never fail for any key length). Lock poisoning is absorbed with
`unwrap_or_else(|e| e.into_inner())`, leaving no panic entry point for background tasks.

**Periodic tasks advance at a fixed rate.** The instance's five tasks (dynamic namesrv
10s/2min, route refresh 10ms/`pollNameServerInterval`, heartbeat 1s/`heartbeatBrokerInterval`,
offset persistence 10s/`persistConsumerOffsetInterval`, thread-pool inspection
1min/1min) and the consumer's rebalance wait are all anchored on a single `next` deadline:
the first tick lands exactly at `initialDelay`, and after that each round sleeps only "how
much is left" until `next + n×period`. Written as "sleep initial first, then sleep period"
the first tick would be a whole period late; written as "sleep a full period each round"
the system timer's error accumulates period by period (on macOS, with a measured 100ms
granularity that adds a few ms per tick, a 30s period can stretch to 31s; a sliced
"sleep 100ms until 30s is full" scheme accumulates error per slice). Real-machine evidence
comes from `live_scheduled_intervals`, and the offline guard is
`spawn_periodic_first_tick_lands_at_initial_delay`.

**Logs deliberately do not share a file with the other ports.** They go to
`$HOME/logs/rocketmqlogs/rocketmq_rs_client.log`, rotated daily by rename. On the same
machine, sharing a file means interleaving lines; worse, after the rename other processes
still hold the old fd, so logs are written into an already-unlinked inode and silently
disappear.

| Environment variable | Default | Description |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` | Falls back to `logs/rocketmqlogs` when the home directory cannot be resolved |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_rs_client.log` | The rotated name carries a date suffix |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `WARN`(`WARNING`) / `ERROR` / `OFF`(`NONE`); anything else is treated as INFO |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | Number of retained rotations |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | On | Set to `false` to write only to the file |
| `ROCKETMQ_TLS_ENABLE` | `false` | When enabled, all outbound connections use `native-tls` |
| `ROCKETMQ_TLS_TEST_MODE` | `true` | Trust self-signed certificates and skip hostname verification |

**The clientId convention**: `<local IP>@<instanceName>[@<unitName>][@STREAM]`; when
`instanceName` is `DEFAULT` it is rewritten in place during `start()` to
`<pid>#<nanoTime>` (producers and CLUSTERING consumers; broadcasting consumers keep
`DEFAULT`, so broadcasting consumers in the same process share one instance). All five
facades carry the three settings `unit_name` / `unit_mode` /
`enable_stream_request_type` (producer/push/admin keep stream off, pull/lite keep it on).
The local IP is probed by "connecting" a UDP socket to a public address and reading the
sockname (no packet is sent), degrading to `127.0.0.1` when it cannot be obtained.

**unitMode / stream are wire fields**: sending with `unit_mode=true` makes auto-created
topics carry the `UNIT` flag, the `ConsumerData.unit_mode` field of consumer heartbeats
makes `%RETRY%group` carry the `UNIT_SUB` flag, and `unit_name` also participates in the
`-<unitName>?nofix=1` suffix of the dynamic addressing URL. Warning: the `ReqT` in
ExtFields is the string form of the stream request type, `"0"`, while the clientId tail
uses the enum name `@STREAM`; the stream hook must run **before** the ACL hook, otherwise
`ReqT` falls outside the signature and a broker with authentication enabled will always
fail verification.

**Batch sends use `SEND_BATCH_MESSAGE(320)`**: the reply case (325) is checked first, then
the batch case. Server-side, 310/320 actually take the same path: the broker decodes both
with the V2 header and the `batch` bit in the header selects `sendBatchMessage` — the code
and the `m` bit are two different things, and evidence for them must be captured in pairs
(offline: `send_request_code_follows_java_three_way_branch`; real machine:
`live_mq_client` M3).

**Async send is a real kernel, not "a sync send wrapped in a callback"**: the call returns
immediately, the task is submitted to a **bounded** queue (`async_sender_queue_capacity`
defaults to 50000; concurrency quota = `available_parallelism()`), and when the queue is
full `executor rejected` is thrown back to the caller synchronously without going through
the callback; real timing only starts after dequeue — if queueing ate the whole budget the
callback directly reports `DEFAULT ASYNC send call timeout`; the request is **built only
once**, retries reuse the same request with a fresh opaque; the cap is
`retry_times_when_send_async_failed` (default 2) and the timeout budget is **shared**
across all attempts; a business code genuinely returned by the broker **never enters** the
retry chain (async ignores `retry_response_codes`) — only transport failures/timeouts
switch brokers; the endpoint is fixed: after hook → return permits → user callback
**exactly once**. `shutdown()` unregisters first and then **drains** the send queue, but
Warning: the instance is torn down at the same moment, so on shutdown the tasks still in
the queue only ever complete their chain with a `client already shutdown` callback and not
one message lands at the broker (real machine `live_async_send` A6 measured all 36 sends
erroring with `landed=-1`) — **if you must not lose messages, wait for the callbacks
yourself before shutting down** (the C++/C# versions join the pool before closing the
client; that is their structural difference from this one).

**The send pool's shape is "a never-blocking dispatch task + `Arc<Semaphore>` (one permit
per core) + each task spawned once holding one permit".** Do not convert it to
`tokio::sync::Mutex<Receiver>` polling: one dispatch consumes one unit of concurrency, so
as soon as any send hook blocks synchronously the baton falls on a sleeping thread and the
per-core consumers collapse into "one dispatch per hook sleep period" (real-machine
measurement: with the hook sleeping 2.5s and 10 tasks sitting in the queue, only one task
dispatched every 2.5s). Offline guard:
`resize_wakes_the_parked_sender_while_the_pool_is_saturated` (32 worker threads at runtime).

**Both structural differences in the backpressure gates are deliberate**: (1) **Waiting
for a permit happens inside the send task spawned with `tokio::spawn`, not on the caller's
thread.** The other three ports all park the caller's thread in `tryAcquire`, and when
backpressure saturates their "async" degrades into "wait out the timeout then report an
error"; doing the same in Rust would **deadlock** rather than merely slow down — producers
typically run on the only tokio worker thread, and parking it forever blocks the completion
callbacks that are the ones returning permits out of the queue. The budget still starts at
the moment of the call, so "how long before the gate cannot be passed and an error is
reported" stays consistent; the price is that when the queue is full it is not "run to
completion in place" but **dispatched outside the queue** (permits here are taken after
dequeue, so when the queue is full not one permit has been taken — neither blocking the
caller nor leaking capacity). (2) **Changing capacity shifts the total on the same
semaphore object** (the in-flight share is kept as is, and free permits = new total −
in-flight); the object is not swapped, so waiters currently blocked are never abandoned.

**The TLS read thread runs the entire read loop inside `Handle::enter()`.** A pure-TLS
process may never have entered the tokio runtime, and a runtime handle taken lazily from
the current context would then come back empty — every `CHECK_TRANSACTION_STATE(39)`
transaction check-back pushed by the broker would fail to spawn a task and be silently
dropped (the log shows only one warn line; real machine measured 51 passed / 4 failed while
plaintext was all green). After the fix, `connect_tls` (always inside the runtime) takes
the handle first and the read thread wraps the whole loop in `Handle::enter()`; offline
guard: `tls_inbound_request_is_processed_with_a_runtime_context`; real machine:
`live_producer` P6.

**There is one deliberate legacy in heartbeating**: besides the instance-level periodic
heartbeat, push consumers still keep their own heartbeat loop (first tick 30s, period
`heartbeat_interval_millis`) ⇒ on a real machine two identical heartbeats go out every
30s, which is only extra traffic (removing it would change the `heartbeat_enabled` facade
semantics; the benefit does not outweigh the risk). Warning: the pull-mode consumer's
self-owned loop is **not** part of this legacy: it does not enter the instance's
`consumer_table`, while the instance-level heartbeat only summarizes that table ⇒ without
its own loop the group simply does not exist on the broker (which is exactly what
`live_pull_heartbeat` verifies). The duplicate offset-persistence task **has been deleted**
(captured on a real machine persisting ahead of the initialDelay and ignoring the
configured period).

**Broker-initiated requests (220/221/307/309/326) cannot be injected from outside**: they
travel over the connection the broker already established. The protocol and dispatch are
covered by offline unit tests, and `live_mq_client` verifies the instance-side seams.

## License

Apache-2.0.
