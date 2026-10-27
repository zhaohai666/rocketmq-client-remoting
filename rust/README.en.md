# The Rust Implementation of the RocketMQ Remoting Client

English | [中文](README.md)

## Overview

`rocketmq-client-remoting` is a Rust client SDK for Apache RocketMQ's classic remoting
protocol: it connects directly to NameServer + Broker — no proxy, no native extensions, no
code-generation step.

- Toolchain: Rust 2021 edition; measured with `rustc 1.95.0` in this repository. The only
  third-party dependency that is architecturally noticeable is tokio — every outward-facing
  RPC is an `async fn`; all other deps (serde, flate2/lz4_flex/zstd, hmac/sha1/base64,
  chrono, native-tls) are ordinary codec/utility crates.
- Protocol coverage: remoting frame encoding/decoding, dual JSON and RocketMQ binary
  serialization, the 17-segment store format, POP mode, message tracing, ACL signing, TLS.
- Measured peer: RocketMQ 5.5.1 single-master and master-slave clusters; compatible with
  4.x / 5.x clusters (cluster switches such as `autoCreateTopicEnable` affect the live
  examples — see "Live Cluster Verification").

## Prerequisites

- A Rust toolchain (`rustc` / `cargo`); anything that compiles edition 2021 is enough.
- For live verification: a running NameServer(9876) + Broker(10911) with
  `autoCreateTopicEnable=true`.
- All unit tests run offline (in-process fake cluster / fake sockets); no external service
  is required.

## Getting Started

```bash
cd rust
cargo build --all-targets                       # lib + 42 examples
cargo test --quiet                              # 1143 passed; 0 failed; 0 ignored (~10s)
cargo clippy --all-targets --all-features -- -D warnings  # zero warnings is a hard gate
```

Unit tests are inlined in `src/**/mod tests` (there is no standalone `tests/` directory);
the live-cluster tools live in `examples/live_*.rs` and are **not part of `cargo test`**.
Run one example:

```bash
cargo run --quiet --example live_protocol -- 127.0.0.1:9876
```

## Examples

Every facade shares the same lifecycle: `new / with_config → set_namesrv_addr →
start().await → … → shutdown()`. Every API name below is grep-able in `src/`.

### Producer (normal / ordered / delayed / transaction / batch / oneway / async / Request-Reply / recall)

```rust
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::common::message::Message;

let producer = DefaultMQProducer::new("GID_test")?;
producer.set_namesrv_addr("127.0.0.1:9876");
producer.start().await?;

let mut msg = Message::new("TopicTest", Some(b"hello"));
let result = producer.send(&mut msg, None, None).await?; // (msg, timeout, chosen queue)
producer.shutdown();
```

| Scenario | API (`DefaultMQProducer`) |
| --- | --- |
| Ordered (pinned) send | `send_by_selector(&mut msg, &dyn MessageQueueSelector, arg, timeout)`, with built-in `SelectMessageQueueByHash`; inspect queues first via `fetch_publish_message_queues(topic)` |
| Delayed / scheduled | `Message::set_delay_time_level(3)`; scheduled messages `Message::set_delay_time_ms(ms)` — after sending, take the recall handle from `SendResult.recall_handle` |
| Transaction | `set_transaction_listener(Arc<dyn TransactionListener>)` + `send_message_in_transaction(&mut msg, listener, arg)`; local states `LocalTransactionState::{CommitMessage, RollbackMessage, Unknow}`, broker check-back via `check_local_transaction` |
| Batch | `send_batch(Vec<Message>, mq, timeout)` (same topic merged into one frame); async variant `send_batch_async` |
| Oneway | `send_oneway(&mut msg, mq)` — no reply awaited |
| Async | `send_async(msg, Arc<dyn SendCallback>, timeout, mq)`; `on_success` / `on_exception` fires exactly once |
| Request-Reply | `request(&mut msg, timeout, mq).await? -> MessageExt`; on the consumer side build the reply with `client::request_reply::create_reply_message(&request, body)` (the reply topic comes from `MixAll::get_reply_topic(cluster)`, detected by `is_reply_message`) and send it through a producer |
| Recall a scheduled message | `recall_message(topic, recall_handle).await` |

Usage notes:

- `shutdown()` is a synchronous finalizer. Called from inside a tokio runtime worker
  thread it cannot block until finalization completes — the work is detached and a WARN is
  logged (finalization is lost if the process exits right away). Call it from a thread
  outside the runtime, or stop the runtime first.
- In-flight async sends are **not** awaited by `shutdown()`: after shutdown, tasks still in
  the queue only ever complete with a `client already shutdown` callback. **If delivery
  must be guaranteed, wait for your callbacks to land before shutting down.**
- Async send is genuinely async: the call returns immediately, the task enters a bounded
  queue (`set_async_sender_queue_capacity`, default 50000); when the queue is full,
  `executor rejected` is thrown back to the caller synchronously, never via the callback.
- In backpressure mode (config field `enable_backpressure_for_async_mode`) waiting for a
  send permit happens inside the background send task, not on the caller's thread — the
  caller never blocks, and the timeout budget starts at the moment of the call.

### PushConsumer (concurrent / orderly / broadcasting)

```rust
use rocketmq_client_remoting::client::consumer::DefaultMQPushConsumer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::MessageExt;
use std::sync::Arc;

struct MyListener;
impl MessageListenerConcurrently for MyListener {
    fn consume_message(&self, _msgs: &[MessageExt], _ctx: &mut ConsumeConcurrentlyContext)
        -> ConsumeConcurrentlyStatus
    {
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

let consumer = DefaultMQPushConsumer::new("GID_test")?;
consumer.set_namesrv_addr("127.0.0.1:9876");
consumer.subscribe("TopicTest", "*")?;                       // tag expression
consumer.set_message_listener_concurrently(Arc::new(MyListener));
consumer.start().await?;
```

- Ordered consumption: `set_message_listener_orderly(Arc<dyn MessageListenerOrderly>)`
  (returns `ConsumeOrderlyStatus`; locked and serialized per queue).
- Broadcasting:
  `consumer.update_config(|c| c.message_model = MessageModel::BROADCASTING.into())`
  (default `CLUSTERING`); in broadcasting mode offsets persist locally only, at
  `$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json`.
- SQL92 filtering: `subscribe_with_selector(topic, &MessageSelector::by_sql("a > 1"))`
  (requires broker `enablePropertyFilter=true`); tags via `MessageSelector::by_tag`,
  full subscription via `subscribe_all(topic)`.
- Start position: `set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET)`,
  or by timestamp `set_consume_timestamp("20251001000000")` (14-digit local wall clock).
- Ops/test hooks: `do_rebalance`, `sync_pull_threads`, `suspend` / `resume`,
  `fetch_subscribe_message_queues(topic)`, `compute_accumulation_total()` (backlog),
  `update_core_pool_size(n)`.

### PullConsumer (manual pull)

```rust
use rocketmq_client_remoting::client::pull_consumer::DefaultMQPullConsumer;

let consumer = DefaultMQPullConsumer::new("GID_test")?;
consumer.set_namesrv_addr("127.0.0.1:9876");
consumer.register_topic("TopicTest");
consumer.start().await?;
let mqs = consumer.fetch_subscribe_message_queues("TopicTest").await?; // whole topic
for mq in &mqs {
    let offset = consumer.fetch_consume_offset(mq).await?.unwrap_or(0);
    let pulled = consumer.pull(mq, "*", offset, 32, None).await?;
    consumer.update_consume_offset(mq, pulled.next_begin_offset).await?; // caller owns offsets
}
```

- Long polling: `pull_block_if_not_found(mq, expr, offset, max, timeout)`.
- Balanced view: `fetch_message_queues_in_balance(topic)` returns only this instance's
  share under the allocation strategy.
- Offset by timestamp: `search_offset(mq, timestamp)`; send a failed message back:
  `send_message_back(msg, level)`.

### LitePullConsumer (poll model + queue-change listener + three offset tables)

```rust
use rocketmq_client_remoting::client::pull_consumer::DefaultLitePullConsumer;
use std::sync::Arc;

let consumer = DefaultLitePullConsumer::new("GID_test")?;
consumer.set_namesrv_addr("127.0.0.1:9876");
consumer.subscribe("TopicTest", "*");        // or assign(&[mq]) manually
consumer.start().await?;
let msgs = consumer.poll(Some(3000)).await;  // empty Vec == timeout, not an error
consumer.commit().await?;                    // manual commit when auto_commit is off
```

The three offset tables are independent; each answers a different question:

| Table | Semantics | Related APIs |
| --- | --- | --- |
| Pull cursor | where the next fetch from the broker starts (includes filtered-out messages) | `pull_cursor_of(mq)` |
| Consumed cursor | the position `poll()` actually handed to the caller | re-pin with `seek(mq, offset)` / `seek_to_begin` / `seek_to_end` |
| Commit table | the offsets `commit()` / auto-commit write out and eventually land on the broker | `committed(mq)`, `commit_offsets`, `commit_queues` |

Helpers: `pause / resume`, `assignment()`, `fetch_message_queues(topic)`,
`send_heartbeat_to_all_broker()`.

Queue-change listener (scale-out / scale-in callbacks):

```rust
consumer
    .register_topic_message_queue_change_listener("TopicTest", Arc::new(MyQueueChangeListener))
    .await?; // MyQueueChangeListener: TopicMessageQueueChangeListener::on_changed(topic, mqs)
consumer.fetch_topic_message_queues_and_compare().await; // trigger one comparison round (for live tests)
```

The callback key uses the same form as the registration key (namespace wrapping included);
registering while started records a first snapshot so the current set is never
mis-reported as a change. The comparison period is configured by
`topic_metadata_check_interval_millis`.

### Admin

```rust
use rocketmq_client_remoting::client::admin::DefaultMQAdminExt;

let admin = DefaultMQAdminExt::new();
admin.set_namesrv_addr("127.0.0.1:9876");
admin.start().await?;
let route = admin.examine_topic_route("TopicTest").await?;
let stats = admin.examine_consume_stats_group("GID_test", Some("TopicTest")).await?;
admin.delete_topic("TopicTest", None).await?;
```

Others: `create_and_update_topic_config(addr, &TopicConfig)`, `create_topic`,
`fetch_all_topic_list`, `get_cluster_list(topic)`, `examine_topic_config`,
`fetch_broker_runtime_stats`, `wipe_write_perm_of_broker`, `clean_unused_topic`,
`search_offset(mq, timestamp)`, `reset_offset_by_timestamp`, `reset_offset_by_queue_id`.

### ACL

```rust
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::remoting::rpchook::{AclClientRPCHook, SessionCredentials};
use std::sync::Arc;

let hook = Arc::new(AclClientRPCHook::new(SessionCredentials {
    access_key: "AK".into(),
    secret_key: "SK".into(),
    security_token: String::new(),
}));
let producer = DefaultMQProducer::with_rpc_hook("GID_test", Some(hook))?;
```

Signed content = extFields values concatenated in lexicographic key order (`Signature`
skipped), then the body appended; the result is `Base64(HmacSHA1(secretKey, content))`.
The same `with_rpc_hook` / `set_rpc_hook` exists on all four facades.

### Namespaces (two mechanisms, fully independent)

- `set_namespace("myNs")`: a client-side resource-name prefix — on the wire the resource
  becomes `%%myNs%%TopicTest` (`remoting/protocol/namespace_util.rs`; wrapping/unwrapping
  across send / consume / heartbeat / offsets). Producer / Pull / LitePull expose
  `set_namespace`; the other facades use the identically named config field `namespace`.
- `set_namespace_v2(Some("ns"))`: the server-side namespace — every request stamps
  `nsd=true` / `ns=<value>` headers, and they sit inside the ACL signature content.
  Available on the producer, all three consumers and the admin client; read back with
  `get_namespace_v2()`.

### Compression

```rust
use rocketmq_client_remoting::common::sysflag::MessageSysFlag;
producer.set_compress_type(MessageSysFlag::ZSTD_TYPE)?;      // or LZ4_TYPE / ZLIB_TYPE
producer.set_compress_msg_body_over_howmuch(4 * 1024);       // auto-compress above this size
producer.set_compress_level(3);
```

Three backends: zlib / LZ4 Frame / ZSTD; the consumer decompresses automatically from the
compression bits in sysFlag, and compression runs outside the retry loop (never a second
time). Manual codecs: `common::compression::{compress, decompress}`.

### TLS

```bash
ROCKETMQ_TLS_ENABLE=1 cargo run --quiet --example live_producer -- 127.0.0.1:9876
```

Or in code: `producer.set_tls_enable(Some(true))` (other facades use the env var). Default
`ROCKETMQ_TLS_TEST_MODE=true` (trust self-signed certificates, skip hostname
verification); for production set it to `false` and configure the system trust chain.

## Features and Status

- ✅ remoting frames / dual JSON + RocketMQ binary serialization / 17-segment store format
  + 6-segment batch format
- ✅ sync / async / oneway transport: half-packet reassembly, opaque matching, timeouts,
  reconnection, GO_AWAY re-establish-and-resend, connection-fatal fast failure
- ✅ Producer: all nine send shapes in the table above
- ✅ PushConsumer: long polling + POP (`POP_CK` / ACK / CHANGE_INVISIBLE), ordering,
  broadcasting, retry / dead-letter, offset persistence, pull-stall self-healing, POP's
  outstanding-ACK gate (strictly above `pop_threshold_for_queue`, default 96, a trip counted on the
  pull path's one client-wide flow-control total and warn cadence)
- ✅ PushConsumer: long polling + POP (`POP_CK` / ACK / CHANGE_INVISIBLE), ordering,
  broadcasting, retry / dead-letter, offset persistence, pull-stall self-healing, POP's
  outstanding-ACK gate (strictly above `pop_threshold_for_queue`, default 96, a trip counted on the
  pull path's one client-wide flow-control total and warn cadence)
- ✅ PushConsumer's three per-queue cache gates, judged in the fixed order count → size → span: they
  read the queue's whole **pulled-but-unsettled** held set (the buffered rows plus the batch already
  handed to the listener, deduplicated by `queueOffset`), the size gate divides message **body**
  bytes by whole MiB, and the span gate is skipped for an orderly listener. All three compare
  strictly above their threshold and the configured number is used as it stands — no floor
  clamping, no value that switches a gate off; the legal range is owned by start-up `checkConfig`
- ✅ PushConsumer's assignment-change settlement: a round pays only when a topic's assigned queue
  SET actually moved (growth / shrink / a same-count hand-over to another broker / a topic appearing
  or vanishing), and then in one order — re-stamp the subscription version of the moved topics only
  (one wall-clock millisecond per round) with one INFO line, divide each topic cache budget by the
  queue count alive this round, send one immediate heartbeat to every broker, and report exactly the
  moved topics to the queue-change listener registered by `set_message_queue_listener`; a round whose
  sets did not move pays none of the four
- ✅ PullConsumer / LitePullConsumer (incl. queue-change listener, three offset tables,
  pause / resume; LitePull judges four cache gates before the pull offset is resolved —
  the buffer's pending-round projection `pending rounds × pull_batch_size` (default 10) against
  `pull_threshold_for_all` 10000 / this queue's count
  `pull_threshold_for_queue` 1000 / this queue's body bytes
  `pull_threshold_size_for_queue` 100 MiB with integer division / this queue's offset span
  `consume_max_span` 2000, each refusing only when strictly above its threshold; a refusal
  re-schedules just this queue 50ms later while the broker's `FLOW_CONTROL`(215) waits 20ms;
  a revoked queue loses its cache and its reserved rounds together)
- ✅ Six queue allocation strategies (AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH /
  MACHINE_ROOM / MACHINE_ROOM_NEARBY), pluggable and driven by real rebalancing
- ✅ The full Admin chain (topic / cluster / consume stats / offset reset / queue locks /
  cleanup)
- ✅ ACL signing, TLS (native-tls), dynamic name server addressing, fault-avoiding queue
  selection
- ✅ Message tracing (Pub / SubBefore / SubAfter / EndTransaction / Recall encode+decode +
  async dispatch + hooks)
- ✅ Request-Reply, consumer-side statistics, metrics, unitName / unitMode / stream
- ✅ zlib / LZ4 / ZSTD compression roundtrips (cross-client interop: `live_compression_matrix`)
- ⬜ Windows targets untested

## Client Logging

Logs go to `$HOME/logs/rocketmqlogs/rocketmq_rs_client.log` by default (degrading to
`logs/rocketmqlogs/` when the home directory cannot be resolved) and to stderr at the same
time; rotation is daily by rename, with date-suffixed history files. Everything is
controlled by environment variables:

| Environment variable | Default | Description |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` | log directory |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_rs_client.log` | log file name |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `WARN` / `ERROR` / `OFF`; anything else is treated as INFO |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | number of retained rotations |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | on | set to `false` to write only the file |

## Live Cluster Verification

Precondition: NameServer(9876) + Broker(10911) running with `autoCreateTopicEnable=true`.
Uniform run command:

```bash
cargo run --quiet --example live_xxx -- 127.0.0.1:9876
```

Conventions: `main()` returns an `ExitCode`; any failed check exits nonzero, and the
closing line is always `== summary: N passed, M failed ==`. **A fair number of scenarios
require "start the consumer first and wait until its subscription heartbeat is registered
with the broker before publishing"** — otherwise the first batch of messages is missed or
duplicated because the broker does not know the group yet. The examples are already
ordered that way internally; manual reproductions must keep the same order. Rows marked ⚠
list extra preconditions.

| Example | What it verifies |
| --- | --- |
| `live_protocol` | frame codec + smoke roundtrips of the core request codes against a real broker |
| `live_mq_client` | instance reuse, field-by-field send/receive, the five offset RPCs, POP pop-back-ACK-change-invisibility, batch lock, the shared-instance close guard |
| `live_producer` | six send paths, transaction two-phase + check-back, recallMessage, the async kernel, shutdown unregistration 35; ⚠ the TLS leg needs `ROCKETMQ_TLS_ENABLE=1` |
| `live_consumer` | long polling / tag filter / `%RETRY%` re-delivery / `%DLQ%` dead letters / partial ack / POP / broadcasting / ordered dead letters / explicit COMMIT-ROLLBACK / stall self-healing / 307 |
| `live_pull_consumer` | manual pull with no loss and no duplication, offsets owned by the caller, long polling really suspends |
| `live_lite_pull_consumer` | subscribe / assign / seek / poll / committed, both auto_commit states, the three offset tables each counted out on real hardware, and this queue's cached-count gate (over budget parks only that queue, and the cursor runs to the tail once the buffer drains) |
| `live_lite_pull_cursor` | pull cursor: NO_MATCHED_MSG follows past the whole run + OFFSET_ILLEGAL out-of-range self-healing |
| `live_lite_pull_code` | lite request code 361 + lite flag; ⚠ flips the broker switch `litePullMessageEnable` at runtime, restores it before exit |
| `live_lite_topic_queue_change` | queue-change listener: instant scale-out/scale-in callbacks, snapshot advances without duplicate callbacks |
| `live_alloc_strategy` | the six allocation strategies really drive rebalancing |
| `live_rebalance_and_trace` | three-strategy end-to-end partitioning on real routes, core/max executors, traces landed at the broker then decoded back |
| `live_client_modules` | dynamic addressing, fault-avoiding queue selection, statistics, five hook categories, traces through the broker, metrics, request-reply |
| `live_validators` | name validation fails locally in sub-millisecond time + addressing-fault characterization (10004) |
| `live_admin` | the full admin chain (incl. boundaryType double boundaries, `reset_offset_by_queue_id`, query topics by consumer group) |
| `live_admin_batch` | admin payloads against a real broker: batch topic creation, static topics, write-perm config, half messages, ordering config, etc. |
| `live_message_request_mode` | real-broker proof that SET_MESSAGE_REQUEST_MODE(401) carries its whole payload in the JSON body and that GET_ALL_MESSAGE_REQUEST_MODE(402) reads it back (measured `PASS=10 FAIL=0`): **L1** after the production `set_message_request_mode` writes POP/4, 402 must hold an entry keyed by exactly this topic+group whose `popShareQueueNum` is the number 4; **L2** writing PULL/0 for the same group must come back as PULL with the number 0 — the second write replaces the first rather than leaving the first value standing; **L3 control** — a raw 401 with all four values pushed into the header and an empty body must be rejected by the broker (not SUCCESS; measured code=1 with a remark saying the request body could not be decoded) and its control group must stay absent from 402, so the header shape switches nothing and the switches in L1/L2 are real. The topic is created explicitly first (401 refuses a topic the broker has no config for, and that refusal is indistinguishable from a broken request shape), the subscription group is left to the broker's autoCreate, names are stamped per run, and every read-back polls until it is answered instead of sleeping a fixed duration. |
| `live_unit_config` | unitName / unitMode / stream: the clientId suffix visible broker-side and the UNIT / UNIT_SUB flags |
| `live_sql92` | SQL92 filtering + CHECK_CLIENT_CONFIG(46); ⚠ requires broker `enablePropertyFilter=true` (when the broker refuses SQL92 the probe prints the fix and exits cleanly) |
| `live_oneway_push` | oneway real-time push: consumer starts first (LAST_OFFSET), `send_oneway` auto-selected/pinned queues + a sync control all arrive in real time |
| `live_acl` | the ACL signing/authentication chain end to end; ⚠ requires `authenticationEnabled=true`, real args `<AK> <SK>` |
| `live_acl_message_types` | every message type on an authenticating cluster: asserts first that an unsigned send is rejected with code 16, then sends and reads back sync / async / oneway / batch / delayed / timed / transaction (incl. transactionId) / orderly / tag / key / user property from the broker, with offset commit also going through signed RPC; ⚠ requires `authenticationEnabled=true`, real args `<AK> <SK>` (measured `PASS=33 FAIL=0`) |
| `live_latency` | send-latency fault tolerance S1–S5: off by default so nothing is recorded, a healthy broker triggers no isolation, successful sends populate the table, an injected outage degrades available→reachable→plain round-robin, and the entry recovers on expiry |
| `live_static_topic` | static topic S1–S5: 513 mapping 8 queues over 4 physical ones, route read-back of `TopicQueueMappingByBroker`, the endpoints table (0-3 → real broker, 4-7 → `__syslo__none__`), the instance rewriting mock queue names back to the real broker (ids outside the table pass through), end-to-end send 4 / consume 4 |
| `live_backpressure` | the real-machine closed loop of the two fair gates + the bounded send queue (incl. admission after a runtime resize) |
| `live_async_send` | async kernel: offsetMsgId read back verbatim, concurrency without cross-talk, batch async, shutdown does not wait for in-flight sends |
| `live_fail_fast` | in-flight requests reach a terminal state in milliseconds when the broker truly dies; ⚠ stops the broker once then brings it back (store not deleted) |
| `live_send_header` | send headers c/d/n and the queue-count arithmetic of auto-created topics |
| `live_flow_control` | the three per-queue cache gates before pulling (topic budgets divided into them at rebalance) + startup numeric gates |
| `live_scheduled_intervals` | periodic tasks' initialDelay / fixed rate (incl. the offset-persistence 10s first jump) |
| `live_subscribe` | post-start subscribe heartbeats immediately (criterion: the 300 query sees this group before the 30s cycle does) |
| `live_pinned_guard` | pinned-topic guard: real routes are not mis-rejected, the rejection is client-side with no broker trace, oneway has no guard |
| `live_correct_tags_offset` | NO_NEW_MSG / NO_MATCHED_MSG empty responses still push the committed offset up to maxOffset |
| `live_offset_illegal` | OFFSET_ILLEGAL: void the whole in-flight/buffered batch and rebuild at the corrected offset; the corrected offset persists immediately |
| `live_reset_offset` | 220 reset consumer offset: persist immediately after the broker pushes 220 + void in-flight batches + rebuild queues at the new offset |
| `live_pull_heartbeat` | the pull-mode consumer's 203/38/35; ⚠ requires a master-slave cluster, args `-- 127.0.0.1:9876 127.0.0.1:10911 [127.0.0.1:10931]` |
| `live_publish_route_master` | the four "master-only" conventions across publish / ordered lock / POP / offset reads; ⚠ stops the master once, same master-slave args |
| `live_shutdown_race` | the data-loss contract of immediate shutdown / immediate process exit |
| `live_clean_expired_msg` | escape hatch with a hanging listener: the sweep re-delivers `%RETRY%` a second time (about 4 minutes) |
| `live_trace_bridge` | trace dispatcher auto-bridge (self-attaches when no channel is configured) |
| `live_trace_default` | the default self-built trace producer really sends traces to RMQ_SYS_TRACE_TOPIC |
| `live_compression_matrix` | compression interop legs: `send` / `recv` / `reuse`, orchestrated by the repo-level `scripts/compression_matrix.sh` |

## Repository Layout

```
rust/
├── Cargo.toml
├── src/
│   ├── lib.rs                  exports of the three layers
│   ├── error.rs                Error / Result / client_error_code
│   ├── common/                 message / message_decoder (17-segment + batch) /
│   │                           compression / consistent_hash / recall_message_handle /
│   │                           boundary_type / message_const / message_type / mix_all /
│   │                           sysflag / topic_config / topic_validator / util_all /
│   │                           buffer / logging
│   ├── remoting/
│   │   ├── client.rs           sync / async / oneway + half-packet reassembly + TLS + GO_AWAY
│   │   ├── rpchook.rs          AclClientRPCHook (ACL signing)
│   │   └── protocol/           remoting_command / headers / codes / serialize / route /
│   │                           heartbeat / body / admin_body / subscription / extra_info /
│   │                           namespace_util / ext_fields
│   └── client/
│       ├── mq_client.rs        MQClientInstance: route discovery + all RPCs + heartbeat
│       ├── producer.rs         DefaultMQProducer (sync / pinned / batch / oneway / async /
│       │                       transaction / request-reply)
│       ├── consumer.rs         DefaultMQPushConsumer (long polling + POP + ordering + broadcasting)
│       ├── pull_consumer.rs    DefaultMQPullConsumer + DefaultLitePullConsumer
│       ├── allocate_strategy.rs  the six queue allocation strategies
│       ├── admin.rs            DefaultMQAdminExt
│       ├── consume_executor.rs / backpressure.rs / produce_accumulator/
│       ├── hook.rs / latency.rs / consumer_stats.rs / metrics.rs / shutdown.rs
│       ├── request_reply.rs / top_addressing.rs / validators.rs / result.rs
│       └── trace.rs / trace_context.rs / trace_hook.rs / trace_dispatcher.rs
└── examples/                   41 live-cluster tools (table above)
```

Unit tests are inlined in `src/**/mod tests` (they need access to `pub(crate)` internals
and the fake clock); properties that only real hardware can prove all live in
`examples/`.

## License

Apache-2.0.
