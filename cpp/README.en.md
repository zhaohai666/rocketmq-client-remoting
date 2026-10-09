# rocketmq-client-remoting (C++)

> [中文](README.md) | English

A C++17 implementation of RocketMQ's classic remoting protocol (aligned with 5.x), cross-checked item by item against this repository's Python reference implementation (`../python/`) and the C# / Rust implementations (`../csharp/`, `../rust/`).

**No third-party runtime dependencies** (only POSIX sockets + the standard library + system compression libraries: zlib is required, liblz4 / libzstd are optional — if one is not found, only that single backend is turned off). The network layer is hand-written, with the goal of exposing "what the bytes actually look like" and enabling byte-for-byte interoperability verification between the language implementations.

Implemented scope:

| Layer | Content |
| --- | --- |
| Protocol layer | JSON / RocketMQ binary dual serialization; `RemotingCommand` frame encode/decode; the CommandCustomHeader family (incl. the V2 single-letter field names); the 17-segment message storage format and the 6-segment batch format |
| Transport layer | `RemotingClient`: sync / async / oneway, partial-frame reassembly, opaque matching, reconnection, **GO_AWAY(1500) switches the connection and re-sends once**, connection death detection, SIGPIPE handling |
| Route / heartbeat | `TopicRouteData` / `QueueData` / `BrokerData`, `SubscriptionData`, `HeartbeatData` |
| Namespace | Two independent mechanisms: `Namespace` (a client-side resource prefix `%%ns%%res`, `common/namespace_util.h`, wrapped and unwrapped across send/consume/heartbeat/offset — note that `fetchSubscribeMessageQueues` here returns namespace-QUALIFIED queue names while Java's `fetchMessageQueuesInBalance` goes through `parseSubscribeMessageQueues` and returns BARE topics; both shapes are copied from Java on purpose) and `namespaceV2` (the server-side namespace: `NamespaceRpcHook` stamps `nsd=true` / `ns=<value>` on every request; hook order Namespace → Stream → ACL, so both `ns` and `ReqT` sit inside the ACL signature). The producer, all three consumers and the admin client read `namespaceV2` live per request |
| Client | `MQClientInstance`, `DefaultMQProducer`, `DefaultMQPushConsumer`, `DefaultMQPullConsumer` (`fetchSubscribeMessageQueues` returns the whole topic, `fetchMessageQueuesInBalance` returns only this instance's share), `DefaultLitePullConsumer`, **`DefaultMQAdminExt`** |
| Async send | `sendAsync` (incl. the pinned `sendAsync(msg, mq, cb)`) runs on real `AsyncSenderExecutor_1..N` threads (core==max==number of CPU cores, bounded queue of 50000) and the caller does not block; the broker-switching retry chain of `retryTimesWhenSendAsyncFailed` continues only on remoting-layer failures (error codes from responses already received are passed to the callback as-is, with no retry), retries **reuse the same request** swapping only the opaque, and the timeout budget is the remaining time shared by the whole chain; user callbacks and `SendMessageHook.after` run on `NettyClientPublicExecutor_N`, and an exception thrown by a callback is swallowed so it does not take down the worker; when `enableBackpressureForAsyncMode` (off by default) is on, an async send passes the gates of two **fair** semaphores **on the caller's thread, before being handed to the `AsyncSenderExecutor`** (in-flight 1024 messages / 100M bytes, floors 10 messages / 1M bytes); if a permit cannot be acquired the callback gets `send message tryAcquire semaphoreAsyncNum|Size timeout` and not a single request is sent, permits are released at the chain's end in "size first, then num" order, and when the queue is full with backpressure on, the send runs inline instead |
| Validation gates | `Validators` / `TopicValidator`: `checkTopic` / `checkGroup` / `isSystemTopic` / `isNotAllowedSendTopic` / `checkMessage`; the `start()` of all four facades runs the group-name validation **before** the client instance is created (purely local judgment; a failure never touches the network) |
| Compression | Three backends — zlib / LZ4 Frame / ZSTD: automatic compression on the producer side + automatic decompression on the consumer side (the on-wire frame format is interoperable with the other language implementations; individual backends can be turned off with `-DRMQ_WITH_ZLIB=OFF` etc.) |

**Not field-tested**: the Windows branch (the code is there, but it has never run on real Windows hardware).

## Build

```bash
cd cpp
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build          # produces librocketmq_remoting.a + tests/* + examples/*
```

Toolchain: CMake >= 3.15, a C++17 compiler (Apple clang 14+ / GCC / MSVC), `Threads`, `ZLIB`; liblz4 / libzstd come from system packages (`find_path` + `find_library`, and CMake config targets like `lz4::lz4` / `zstd::zstd` are also accepted), and **no third-party source is downloaded or vendored**. `-Wall -Wextra` are enabled (`-Werror` is not), **the goal is zero warnings**. Under MSVC, `/utf-8` is added automatically (the sources contain Chinese comments, otherwise C4819); Windows goes through `winsock` (`ws2_32`).

Options:
- `-DRMQ_BUILD_TESTS=OFF` / `-DRMQ_BUILD_EXAMPLES=OFF`
- `-DRMQ_WITH_ZLIB=OFF` / `-DRMQ_WITH_LZ4=OFF` / `-DRMQ_WITH_ZSTD=OFF`: turn off individual compression backends. Note that after a backend is turned off, **encountering a message with that compression type throws an exception** instead of silently returning the compressed bytes
  — so that data corruption cannot be disguised as success. When CMake cannot find a library it turns off only that single backend automatically and configure still succeeds.

The configure log spells out the status of each optional backend (zlib goes through `find_package(ZLIB REQUIRED)`; if it is missing, configure fails outright):

```
RocketMQ client zstd: enabled (/usr/local/lib/libzstd.dylib)
RocketMQ client lz4:  enabled (/usr/local/lib/liblz4.dylib)
RocketMQ client TLS:  enabled (OpenSSL 3.6.3)
```

## TLS

With `RMQ_ENABLE_TLS` (ON by default when OpenSSL is found), each TLS connection gets one `TlsSession`.
**There is an io lock inside the session** (`src/remoting/tls_session.h`): this transport layer has the shape of "one reader thread per connection (`SSL_pending`/`SSL_read`) + writes from caller threads (`SSL_write`)", so two threads inevitably overlap on the same SSL session, and OpenSSL does not allow two threads to use one `SSL` object at the same time. The connection layer's original `writeMutex` only locked the write side and cannot prevent read/write overlap, so the lock was pushed down into the session: `read` / `writeAll` / `pending` / `shutdown` take and release it around every `SSL_*` call (the retry sleep inside `writeAll` stays outside the lock).

Measured on local loopback against a real nameServer (S0 of `examples/live_tls.cpp`): 16 threads doing 320 concurrent calls on one TLS connection — the runs with the lock took 20~26ms, the lock-free run on the same machine 17~19ms; both had 0 failures and every response's opaque matched call by call; plus 30 rounds of "open a fresh TLS connection and send the first packet each round", 0 lost. In other words this lock is a fix of **usage correctness**: a few milliseconds spread over 320 calls (each 10~20µs) cannot be measured as a meaningful cost on a local machine.

## Tests

```bash
cd build && ctest --output-on-failure     # 50 cases, 3993 assertions (49 test binaries 3920 + interop 73), ~64s
```

| Case | Assertions | Coverage |
| --- | --- | --- |
| `codec` | 77 | JSON / ROCKETMQ / `RemotingCommand` dual paths / the 17-segment and 6-segment message formats / header V1<->V2 / hashCode / CRC32 / msgId |
| `route_heartbeat` | 106 | Route-class round trips + queue filtering by perm (**publish info** additionally skips brokers with no master in `brokerAddrs`; **subscription info** looks only at the read bit + `readQueueNums`, neither filtering master nor consulting brokerDatas — the two semantics each get one negative-control leg); `SubscriptionData` / `HeartbeatData` round trips + field-name guards |
| `publish_route_master` | 44 | The **address side** half of the publish-address "master only" rule: when only a slave remains (brokerId=1) the publish address lookup fails, while on the same route the yielding lookup `brokerAddrOf` must reach the slave (the two semantics cannot be merged into one); when the cache is empty `publishAddrFor` first refreshes the route once by topic and then looks it up (the fake name server counts each `GET_ROUTEINFO_BY_TOPIC` — exactly one); a lost master and an unknown brokerName both throw `The broker[X] not exist` **on the local end** with responseCode=-1, and a route refresh must happen before reporting not exist; the admin-side `getMaxOffset` likewise hits the master only. Three **subscription-side** legs on the same theme: `lockBatchMq`/`unlockBatchMq` recognize the master only and do **not refresh the route**, so with only a slave left the whole broker is skipped (empty set returned, zero LOCK/UNLOCK wire); `popMessage` master-only -> refresh the route once -> if still absent throw not exist locally; `queryConsumerOffset` master-only -> refresh the route once -> the re-lookup **relaxes** to the slave (offsets are the same HA-replicated data). The mock broker's LOCK_BATCH_MQ reply returns `lockOKMQSet` per the `mqSet` in the request body, and POP answers `POLLING_TIMEOUT`. The flat table is written by the **real** `updateTopicRouteInfoFromNameServer`; changing the route always means editing the fake name server's answers and letting the client refresh itself. The start point of `CONSUME_FROM_FIRST_OFFSET` is the **literal 0** (same shape in both the push and lite-pull legs) and it does **not** send a `GET_MIN_OFFSET(31)` query. ⚠ `seekToBegin()` is a deliberate exception: it really calls minOffset |
| `transport` | 55 | Real local TCP: sync/async/oneway, **partial-frame reassembly**, opaque matching, connection failure, timeout, reconnection, **GO_AWAY re-send (sync + async, re-sent only once, no reconnect when the switch is off)**, address resolution |
| `fail_fast` | 19 | In-flight requests are declared dead immediately when the peer disconnects: a real socket whose peer closes after reading one frame => the sync call throws `RemotingSendRequestException` at millisecond level (instead of waiting out the full timeout and reporting `RemotingTimeoutException` — the async-send retry classification branches on exception **type**), the async callback fires **exactly once**, in-flight requests are claimed by **connection object identity** (other connections' requests are untouched, and a new request on a new connection to the same address is not wrongly killed by the old reader thread), and `shutdown` drains all in-flight requests |
| `compression` | 152 | Three backends: type resolution (incl. the `0->ZLIB` compatibility mapping), zlib / **LZ4 Frame** / **ZSTD** round trips, **externally hardcoded ground-truth fixtures** (standard frames produced by zlib, lz4 and zstd respectively), frame-header magic and the LZ4 block-independence bit, the 17-segment wire format x three types, clearing the flag after decompression, and a missing backend or unknown type must throw rather than hand out compressed bytes |
| `admin` | 188 | fastjson2 tolerance of invalid JSON, the `ConsumeStatsList` field-name guard (one wrong character in a key silently parses into an empty list), `TopicConfig` / `SubscriptionGroupConfig` defaults and field names, `TopicStatsTable` / `ConsumeStats` / `ResetOffsetBody`, properties-text round trip, `PermName::isValid`, `SearchOffsetRequestHeader.boundaryType` (uppercase enum name on the wire, `@CFNullable` writes no key when unset, and re-parsing only accepts `equalsIgnoreCase("upper")`) |
| `logging` | 36 | Line format (milliseconds / pid / thread name / `file:line`), the main thread landing as `main`, thread names being thread-local, size-based rotation and the `maxIndex` cap, level filtering, and no disk writes after file output is closed |
| `acl` | 56 | The ACL signature algorithm (`extFields` sorted by key lexicographically, values only concatenated, Signature skipped, then the body appended) checked against the official signature vectors |
| `request_reply` | 40 | Message codec for the request-reply mode, the `reply_to` property, correlationId matching and timeout; **error-code semantics**: a wait-for-reply timeout throws `RemotingTimeoutException` carrying 10006 `REQUEST_TIMEOUT_EXCEPTION`, and when `createReplyMessage` cannot build a reply it carries 10007 `CREATE_REPLY_MESSAGE_EXCEPTION` (the message text points at the missing `CLUSTER` property) |
| `latency` | 31 | Fault lagging: latency-window sliding statistics, availability judgment, broker isolation and recovery, the `sendLatencyFaultEnable` switch |
| `send_retry` | 100 | The retry-classification semantics of `sendDefaultImpl` (in-process mock cluster + real sockets): retryable codes switch brokers, non-retryable codes throw immediately, retry exhaustion reports `BrokersSent`, the single-call timeout clamp, budget exhaustion reporting callTimeout, fast failure without a route, and isolation on connection failure; **missing addressing reports 10004 not 10005** (no address configured at all => `NO_NAME_SERVER_EXCEPTION` + the official message text, addresses configured but unreachable => the code is not 10004); wire packets captured from the same fixture to prove the on-wire messages (`k`=unitMode, `ReqT`, the send request codes 310/320/325 and the three-level decision for `m`=batch); **those three send-header fields that follow route/config** are likewise proven from real packets: by default `c`=`TBW102`/`d`=4, and after `setCreateTopicKey`/`setDefaultTopicQueueNums` the two keys become the configured values, `n` takes the **route-selected** broker name, and pinned send and batch 320 go through the same header-building code with none of the three keys allowed to be missing; **the topic guard of pinned sends**: mismatched sync single and batch sends are rejected **before any SEND hits the wire** (the mock endpoint counts each `sendCount`), with the text `message's topic not equal mq's topic`, while same-topic sends go to the wire as usual and the wire's `b` takes the message's own topic; the namespace is compared against the resource name **after** the `queueWithNamespace` wrapping (`ns1%X` is not wrongly rejected, only `ns2%X` is rejected); **pinned oneway deliberately has no guard** — the wire's `b` still takes the msg's own topic and only `e` comes from the specified queue; the same guard on the async side (with **the async wording** + going through the callback) is in `producer_async` |
| `producer_async` | 229 | The real async send chain (in-process mock broker + real sockets): `sendAsync` returns immediately, preparation and requests run on `AsyncSenderExecutor_N`, callbacks and `SendMessageHook.after` run on `NettyClientPublicExecutor_N`; elapsed time is only counted after dequeuing (if the budget is eaten up by queueing, not a single request is sent); only remoting-layer failures retry and a retry **reuses the same request** swapping only the opaque; the timeout budget is the remaining time shared by the whole chain; a response received but with a broker error code is **not retried and not wrapped**; a pinned send retries only on the same broker and must refresh its own route; a full queue throws `executor rejected` to the caller; exceptions thrown by callbacks are swallowed. **Async-send backpressure**: the switch defaults to off and when off **not a single permit is touched**, the two configured floor values, the count/byte gates on the **caller thread** wait until the budget is exhausted before the callback fires (the official message text verbatim), a rejected send emits no request at all, permits are released at the chain's end in "size first, then num" order (released on failure and retry paths as well), growing the capacity at runtime wakes people stuck at the gate, and when the queue is full with backpressure on the send runs inline instead; **batch async**: a batch sends exactly **one** request and delivers exactly **one** callback, the byte gate is charged by the **whole batch's** body length, before `start()` it throws inline on the same terms as single-message async and delivers **not a single callback**, and the ID order is locked down (first `setUniqID` on every sub-message -> then top up one for the whole batch -> **only then** `setBody(encode())`); **the send header on the async chain**: the async path builds its own header, proven from real packets that `n` = the broker name the request lands on and `c`/`d` take the producer's configuration; **the pinned topic guard on the async side**: on a topic mismatch it **goes through the callback** (not thrown to the caller) with the text `Topic of the message does not match its target message queue`, and after rejection not a single request is sent |
| `produce_accumulator` | 249 | The producer's automatic batching `ProduceAccumulator`: the three scenarios (sync / async / specified `MessageQueue`) checked item by item, the three default parameter tiers value by value (10ms / 32KB / 32MB, including faithfully copying the **upstream typo** of `getTotalBatchMaxBytes` returning holdSize) plus the boundary values and official message texts of the three-tier parameter validation, the global byte gate of `tryAddMessage` (admit = charge / release / on rejection the caller falls back to direct send), **splitting** batch replies (comma-separated msgId/offsetMsgId -> one result per message, queueOffset incrementing), the four-dimension partitioning of `AggregateKey` (topic / mq / waitStoreMsgOK / tag) and the asymmetry of "sync collects keys, async does not"; batch-level properties (KEYS space-joined union, TAGS, WAIT), the guard thread's cleanup of empty batches and the **retention** of batches that are "fully sent but size still > 0", the sync/async failure paths (exception propagated up + quota released, each callback getting its own share), the guard thread being rebuildable across `start -> shutdown -> start`, guard thread names `<clientId>_GuardForSyncSend` / `_GuardForAsyncSend`, and the accumulator being reused per clientId; sender lifecycle: after `detach` the sync path throws and `sendSync`'s finally **still releases the quota**, the async callback gets the same message text while the quota is **not released**, and `attach` rebinds only when detached |
| `backpressure` | 50 | `FairSemaphore`: only the **head of the queue** can take a permit (latecomers may not cut in line), a timeout returns false instead of throwing, and both after a timeout and after being granted the "the queue head has changed" fact must be **broadcast** (miss this step and later waiters sleep to their own timeouts), `release` beyond the total is unchecked, capping the total in place while preserving in-flight permits (a computed negative number of free permits is also accepted), and changing the capacity wakes people currently blocked on the old capacity |
| `pop` | 93 | The POP protocol pipeline: CK deconstruction (8 segments + index selection via `startOffsetInfo`/`msgOffsetInfo`), `bornTime`, ACK offset semantics |
| `pop_consumer` | 54 | The POP consume loop: the default value of `ackIndex`, ack within the invisible time and revival re-delivery, the boundary clamp of `checkNeedAckOrDelay`; **the POP loop records pull stats into the state table** (`incPullRT` for `FOUND` is placed **before the empty-list check**, msgFoundList counts TPS only when non-empty, and `POLLING_NOT_FOUND` leaves both cells untouched): going through the offline seam `recordPopPullStats`, keyed by `topic@group`, a missed record is **silent** (pop, ack and consumption all look normal, only the 307 dashboard shows zeros), and the real-cluster half is evidenced by S5 of `rmq_live_pop_consumer` |
| `pop_orderly` | 16 | The **deliberate non-consumption** of the orderly listener + POP (upstream 5.5.x's orderly POP service is an unfinished skeleton): after requests are diverted into the skeleton the listener is **never called once**, no ack happens (`waitAckCount` only grows and never falls => flow control is left to press the pop loop to a stop, and the broker revives and re-delivers the messages after invisibleTime expires), and the requests stay in the set; the dedup set judges equality by **(PopProcessQueue reference, mq)** (duplicate submission of the same queue enqueues only once, after a rebalance swaps in a new `PopProcessQueue` enqueueing works as before, and `force` does not produce a second copy); a request is removed only when its `pq` is revoked; the concurrent-listener branch is **unaffected by the diversion** (a regression guardrail). What cannot be locked offline (POP mode making lockLoop/shutdown not send LOCK/UNLOCK_BATCH_MQ) is left to the real-cluster side |
| `consume_ack_index` | 28 | The `ackIndex` split of classic concurrent consumption: by default the whole batch is accepted with no re-delivery, when the listener narrows to 0 the tail goes through `sendMessageBack` one by one, `RECONSUME_LATER` forces whole-batch send-back, broadcast mode does not send back, and **on send-back failure the message is pushed back to the front of the queue and the offset does not pass it** (a consumer that has not `start()`ed must fail its send-back, so what is locked offline is the failure branch; "send-back succeeds -> the offset advances for the whole batch" is evidenced on a real cluster by S10 of `rmq_live_redelivery`) |
| `trace` | 92 | Message tracing: **byte-for-byte cross-checks against the official trace implementation** (Pub / SubBefore / SubAfter / EndTransaction / Recall) + codec in both directions + tolerance of the empty segment when there are no keys + isolation of bad records + dispatcher grouping/chunking |
| `orderly_reconsume` | 74 | The re-delivery gate of orderly consumption: the orderly side reads `-1` as **no cap** (not the concurrent side's 16 — the two semantics are deliberate); three branches — quota not used up: `reconsumeTimes + 1` in place and suspend; used up: send back; and suspension continues **only when the send-back fails** (a successful send-back must commit the offset, otherwise one poison message permanently blocks the queue); local re-delivery does not switch topics; the send-back goes through the internal producer as a **regular message** to `%RETRY%<group>`, and `reconsumeTimes`/`maxReconsumeTimes` must be **lifted into the request header** (the broker's `handleRetryAndDLQ` reads the header, not the message properties — lifting the wrong field and the broker falls back to the subscription group's default of 16; negative-control leg: sends to a **normal topic** must not lift it). There is also **manual `COMMIT`/`ROLLBACK`**: with `autoCommit=true` both are **illegal** states and, after a warning, are treated as a successful ack; with `autoCommit=false` `COMMIT` advances the offset directly without re-delivery, `ROLLBACK` returns the whole batch to the `ProcessQueue` with the **offset unchanged**, and `SUSPEND` never commits; the three-tier semantics of the suspend duration (a value from the context wins -> fall back to the consumer's configuration -> if still illegal clamp to the lower bound); `consumeMessageDirectly` has two more mappings than the concurrent side: `CR_COMMIT`/`CR_ROLLBACK` |
| `correct_tags_offset` | 28 | Even empty replies must push the consumed offset forward: when the pull result is `NO_NEW_MSG`/`NO_MATCHED_MSG`, this queue's consumed offset must follow the pull cursor `nextBeginOffset` (monotonic, never moving back), otherwise messages that nobody acks would leave the offset permanently stuck. Three gate conditions: **no messages pending consumption** on the ProcessQueue, **no in-flight messages** (`inflightCount_`), and only these two statuses qualify; negative-control legs cover "with in-flight messages no correction happens" and "the `FOUND` status must not move the offset". The real-cluster half is evidenced by `rmq_live_correct_tags_offset` (the broker offset advances under a zero-delivery premise) |
| `offset_illegal_recover` | 26 | The OFFSET_ILLEGAL correction branch: when the broker answers `PULL_OFFSET_MOVED` (it does so for OFFSET_OVERFLOW_BADLY / OFFSET_TOO_SMALL / OFFSET_RESET alike, with the corrected value in the reply header's `nextBeginOffset`), the handling is change the offset (overwrite) -> discard the queue (queue generation +1) -> freeze and **persist immediately** -> `wakeRebalanceLoop` rebuilds. Getting this path wrong fails in two silent extremes: nudging only the cursor without discarding the queue — an ack of the old batch pushes the offset back to the illegal value and the client bounces with the broker; the offset not being persisted immediately — the process crashes before the next periodic flush and the broker still holds the illegal offset. The freeze covers both `advanceConsumeOffset` and `correctTagsOffset` and **lasts until the queue is rebuilt**. The real-cluster half is evidenced by `rmq_live_offset_illegal` |
| `reset_offset` | 43 | The **client half** of 220 `RESET_CONSUMER_CLIENT_OFFSET`: **two request-body shapes** — the map-shaped `ResetOffsetBody` and the array-shaped `ResetOffsetBodyForC` (`offsetTable` is an **array** and the field names are camelCase `brokerName/offset/queueId/topic`); this end decodes both, and pins the mechanism that "the map-shaped parser gets only an **empty table** from an array" (without the array branch => the entire 220 is silently dropped); bad bodies (empty string / not JSON / another shape) all yield an empty table; **a reset = discard the queue + generation +1 + the new offset landing on disk via the voided tail**: the buffer and `lastPullAt` are voided, the in-memory table is cleared, old batches fetched before the reset and acked after it are **voided wholesale**, and new batches after the rebuild advance as usual; **scope**: only the queues named in the table are touched, entries for unassigned queues are no-ops, and an empty topic / empty table returns directly; **a second reset's generation only increases and never decreases**; in **broadcast mode** the new offset must land in the local file on the spot and the merge must not be a replace, and entries with a null offset must not conjure one out of nothing; **the key names of the 222 wire format** are aligned key by key — even one missing letter in `isForce` is **silent** (on the broker side `isForce` is then always false), `offset`/`queueId` are both indispensable and `false` must also be explicitly sent. The real-cluster half is evidenced by `rmq_live_reset_offset` |
| `pull_expired` | 12 | Self-healing of stalled pull loops (`PULL_MAX_IDLE_TIME` = **120000ms**, read from `rocketmq.client.pull.pullMaxIdleTime`): the threshold locked verbatim, the predicate is **strictly greater**, a fresh loop without a timestamp stamp does not count, an already-exited loop thread counts immediately, healthy queues are never touched (switching threads would mean losing in-flight re-deliveries), on removal the consumed offset is persisted and the pull cursor and buffer are dropped, assignment registers in `mqMap`, the POP branch reads `lastPopTimestamp` and swaps in a clean `PopProcessQueue`, no stall is judged while stopped, the 307 running info reports `lastPullTimestamp` as the real stamping moment, and **`mqTable` and `mqPopTable` are mutually exclusive** |
| `pull_post_subscription` | 35 | Two pull-side behaviors: **`postSubscriptionWhenPull`** (default false; only when the switch is on and it is not a class filter is `subString` spliced into the request, and the SUBSCRIPTION bit of `sysFlag` = `subExpression != null`; turning it off is safe — tag filtering is backstopped by the client's second-pass filter); **`pullFromWhichNode`** (on hitting a slave, `clearCommitOffsetFlag` — slaves do not maintain consume offsets — and no subscription is posted; when the slave is absent it falls back to the master and the COMMIT_OFFSET bit is kept; the response header's `suggestWhichBrokerId` is written back to `pullFromWhichNodeTable`). The wire-format shape is evidenced from the socket via an **in-process fake endpoint** (the SUBSCRIPTION bit, whether the `subscription` key went on the wire, whether it was sent to the master or the slave, whether COMMIT_OFFSET was cleared); the pull consumer's table read/write round trip |
| `flow_control` | 24 | The **five thresholds** of pre-pull flow control: count `>= pullThresholdForQueue` (incl. the `Math.max(1,n)` guard — configuring 0 is not pass-all but stop-at-1), bytes `>= pullThresholdSizeForQueue` with the unit being **MiB** (`<=0` disables), the offset span **strictly greater than** `consumeConcurrentlyMaxSpan` (the real min/max of the out-of-order buffer rather than a head-tail difference), topic-level cumulative count/bytes (aggregated across **all** queues of this topic in this instance, other topics must not be mixed in, and the topic byte gate does **not reuse** the queue-level switch), the decision order count -> bytes -> span -> topic count -> topic bytes, and **one hit records exactly one cell** in `flowControlTriggered()` |
| `hook` | 65 | `CheckForbiddenHook` (exceptions not swallowed, propagating along the retry chain) + `FilterMessageHook` (mutable msgList, removing an entry silently skips it) + hook exception isolation |
| `consume_thread_pool` | 67 | The consume side's two-tier core/max executor (unbounded queue): real concurrency == corePoolSize, `setConsumeThreadNums` taking effect, `updateCorePoolSize` adjusting concurrency at runtime. **The defaults are 20 on both sides** (only 4.x had min=20/max=64), so under the default configuration `updateCorePoolSize` can only adjust **downward** (guarded by `core < max`), pinned by the regression case `testDefaultMaxIsTwenty` |
| `top_addressing` | 37 | Dynamic name server: WS address / unitName / para assembly, `clearNewLine`, and non-200 and connection failures falling back to empty |
| `consumer_stats` | 24 | `ConsumerStatsManager` sampling (sum/tps via endpoint differencing over the window, not depending on the real clock) + `ConsumeStatus` / `ConsumerRunningInfo`(307) encoding |
| `trace_context` | 24 | W3C `traceparent` generation/validation/child span/injection without overwriting/property extraction |
| `interop` | 73 | C++ <-> Python bidirectional codec + bidirectional semantic equivalence of the route/heartbeat structures |
| `lite_pull` | 51 | The `DefaultLitePullConsumer` network-free state machine (subscribe/assign/seek/poll/committed) + the wire shape of the 221/309 reply bodies + **three offset tables**: `pullOffset` (how much was pulled) / `consumeOffset` (how much `poll()` handed out) / `offsetTable` (the cell accumulated in memory by `commit(Map,persist=false)`) are each independent, and committing only follows the consumed cursor (handing the pulled cursor to the broker equals silently losing messages); `maybeAutoCommit` is only checked inside `poll()`, with one global `nextAutoCommitDeadline` (initial value -1 => no commit before the first hand-off), and callers that stop polling keep their offsets untouched; `commit(Map)` on an empty map returns the text `MessageQueues is empty, Ignore this commit`, `commit(Set)` on an empty set returns silently, a value of -1 prints `consumerOffset is -1 in messageQueue [...]` and is skipped, and unassigned queues are silently skipped; `persistAll(scope)` erases memory rows outside the scope; `committed()` goes through `MEMORY_FIRST_THEN_STORE` (a memory hit does not ask the broker, a miss reads back and refills); in subscribe mode a revoked queue is first persisted and then its state dropped wholesale, while in assign mode a shrinking only drops the cursor, neither persisting nor touching offsetStore |
| `lite_pull_cursor` | 18 | **The pull cursor follows `nextBeginOffset`** (after a pull round **returns successfully**, regardless of `FOUND`/`NO_NEW_MSG`/`NO_MATCHED_MSG`/`OFFSET_ILLEGAL`, the pull cursor must advance to the broker's `nextBeginOffset`): the old implementation only moved the cursor on `FOUND` — on `NO_MATCHED_MSG` it stayed put and rescanned the same span every round, and the correction value of `OFFSET_ILLEGAL` was never consumed so out-of-range never self-heals; both show up on a real cluster as "the consumer is alive but never receives messages". A fake cluster answers per script, and the evidence is taken from **the wire packets** checking "does the next `PULL_MESSAGE` start from the new offset"; the negative-control leg covers the one brake, "an in-flight reply colliding with this round's seek". The same test also pins that the start point of `CONSUME_FROM_FIRST_OFFSET` is the **literal 0** and that `GET_MIN_OFFSET(31)` appears 0 times in the entire request log. **Request code / lite bit** (7 items): capture **the request code** and the `sysFlag` of every pull request — every lite pull must be `LITE_PULL_MESSAGE(361)` carrying the `FLAG_LITE_PULL_MESSAGE(0x10)` bit, plus one classic-pull control leg (the request code must be `PULL_MESSAGE(11)` with no lite bit — one extra bit set would make even ordinary consumers hit the lite switch). The real-cluster half is evidenced by `rmq_live_lite_pull_cursor` and `rmq_live_lite_pull_code` |
| `local_offsets` | 18 | The format of the broadcast-mode local offset file: `{"offsetTable":{<MessageQueue used directly as the JSON key>:<offset>}}` — the broker-side serializer writes objects as keys, which is **invalid strict JSON** but self-produced content can be read back, and the four ends all sharing the same `~/.rocketmq_offsets/<clientId>/<group>/offsets.json` recognize each other (the fixture is measured text produced by a real implementation, incl. the pretty version); field order `brokerName/queueId/topic`; both compact and pretty texts are readable by this end, the old flat format is still accepted, and bad texts (truncated / double open / empty string) are all rejected; persistence follows `MixAll.string2File` semantics — the first write produces no `.bak`, a rewrite rolls the **previous generation** into `.bak`, reading prefers the main file, falls back to `.bak` when the main file is missing, and treats both missing as a first launch |
| `allocate_strategy` | 1167 | Six strategies: the official `AVG` / `AVG_BY_CIRCLE` cases (10/4, 7/3, boundary-queue continuation), the four `check` guards returning empty results instead of throwing, `CONFIG` not consulting the guards and returning a copy, `getName()` matching the constants, N consumers covering every queue exactly once with no overlap and no omission, real queues across multiple brokers, the three consumer types (push / pull / lite) defaulting to AVG and being replaceable, and `start()` rejecting a null strategy; **`CONSISTENT_HASH`** checked cell by cell against the measured hash-ring table (6x2/6x3/10x4/20x10 and, at vc=10, 4x2/8x3, the coverage matrix, and the degraded TreeMap hash-collision behavior after injecting a custom `HashFunction`), the `[0,1,4]/[2,3]` partitioning of **`MACHINE_ROOM`** and the ground-truth table of `split("@")` discarding trailing empty segments, and **`MACHINE_ROOM_NEARBY-<inner>`**'s same-machine-room priority + machine rooms with no consumers being shared by everyone and a resolver's unknown room **throwing** (keeping the previous allocation) |
| `consistent_hash` | 42 | The consistent-hash ring + the built-in MD5: the full RFC 1321 Appendix A vectors (incl. the 55/56/57/64/65/200-byte padding boundaries) and ground truths of `hash()` taking the first 4 bytes big-endian, the ring's route stability and wrap-around past the last entry, an empty ring returning null, a negative virtual-node count throwing only in `addNode`, the `i + existingReplicas` replica indices not overlapping, `removeNode` not harming other nodes, an injected custom hash taking effect, and `ringHashes()` being strictly ascending |
| `validators` | 75 | Name validation: the character table (code points >= 128 are uniformly illegal) and the regex semantics, the lists of 12 system topics / 8 not-allowed-to-send topics (`TBW102` is sendable, `%RETRY%` is sendable), the blank -> length (127/120) -> character-table order of `checkTopic`/`checkGroup` with the texts verbatim, `checkMessage` carrying `MESSAGE_ILLEGAL(13)` only in the body tier, the LMQ separator, and the group-name gate in the `start()` of all four facades running before the instance is created |
| `broker_requests` | 11 | The broker reverse request `NOTIFY_CONSUMER_IDS_CHANGED`(40): registered on the **instance-level** clientRemotingProcessor (repeated registration by multiple consumers overwrites each other), counting + waking the whole group, after `unregisterRebalanceWakeup` no more wakes but notifications are still processed, a missing `consumerGroup` not throwing, and shutdown clearing the wakeup table |
| `check_client_config` | 32 | The broker-side client-config check `CHECK_CLIENT_CONFIG`(46): the request header is **null** (no extFields on the wire), the body is the JSON of `CheckClientRequestBody`, non-SUCCESS throws `MQClientException(responseCode, remark)`; only non-TAG subscriptions are sent, addressing goes through the **read-only cache** `findBrokerAddrByTopic` and skips when it finds nothing, network-class exceptions are replaced with a fixed message text, and the timeout uses `mqClientApiTimeout` (3000ms) |
| `client_id` | 32 | The clientId semantics: `buildMqClientId`'s `ip@instanceName[@unitName]` (a blank unitName is not appended), `changeInstanceNameToPID` changing only the default name and being idempotent, the `<ip>@<pid>#<nanoTime>` stamped by the `start()` of the four facades, two producers in the same process not colliding, the broadcast consumer keeping `DEFAULT`, and an explicit instanceName passing through unchanged |
| `recall_message` | 28 | Recall of timed messages `recallMessage`(370): handle codec and **ground-truth vector cross-checks of `buildHandle`** (incl. an unpadded handle, the 6-segment newer version, and v2 / too-few-segments / invalid utf-8 all rejected with the text `"recall handle is invalid"`), a key-by-key guard of `RecallMessageRequestHeader` (the reflection name of the inherited field is **`bname`**, not `brokerName`), the round trip of `SendMessageResponseHeader.recallHandle`, and the producer's local validation order (not started / `%RETRY%` / `%DLQ%` / an invalid handle all return instantly **before** hitting the network) |
| `consumer_check_config` | 85 | The startup-time numeric gates: **both ends of each of the 13 ranges tested once apiece** (`consumeThreadMin/Max [1,1000]`, `consumeConcurrentlyMaxSpan`/`pullThresholdForQueue [1,65535]`, `pullThresholdForTopic [1,6553500]`, `pullThresholdSizeForQueue [1,1024]` **MiB**, `pullThresholdSizeForTopic [1,102400]`, `pullInterval [0,65535]` (**the lower bound is 0** — do not copy the neighbor's 1), `consumeMessageBatchMaxSize`/`pullBatchSize [1,1024]`, `popInvisibleTime [5000,300000]`, `popBatchNums [1,32]`), the texts verbatim (only the FAQ tail stripped); `-1` for `pullThresholdForTopic`/`pullThresholdSizeForTopic` is an "off" sentinel and the other gates have no such exemption; `consumeThreadMin > consumeThreadMax` is **strictly greater** (equal is legal) and the message carries both numbers; `popBatchNums` follows the literal `<= 0`; all comparisons are `< lo \|\| > hi` (both ends closed); when multiple values are out of range at once the **first one in official order** is reported |
| `producer_unregister` | 21 | The wire shape of `UNREGISTER_CLIENT`(35) on exit: the producer-side header carries only `clientID`+`producerGroup`, the consumer-side only `clientID`+`consumerGroup`, and when both are present all three keys are there; **a blank group name means the whole field does not go on the wire** (null is passed, not `""`, because the broker dispatches on `group != null`); the fan-out **includes slaves** with one send to each (a case changed to hit only the master must turn red); when one broker returns `SYSTEM_ERROR`, `unregisterClient` throws `MQBrokerException`, `unregisterClientAllBrokers` swallows it and **the next broker is still sent to**; and the division of labor between `getRouteOfAllBrokers` (the master-first broker used by producer heartbeats) and `getAllBrokerAddrs` (the master+slaves used by 35 and **consumer** heartbeats) is held — consumer heartbeats must reach slaves (a missed slave heartbeat is not merely one redundant send saved: the slave would answer pulls pointed at it with `SUBSCRIPTION_NOT_EXIST`) |
| `subscribe_after_start` | 12 | Late subscription and the immediate heartbeat: `subscribe` after `start()` no longer reports `already started`, a new subscription enters the live subscription table (`subscribedTopics()`, the same table read by heartbeats and rebalance) immediately and a heartbeat is pushed right away; `unsubscribe` only deletes the table entry and does **not** send a heartbeat; subscribing before startup works as before, and no heartbeat at all is sent at that point. The run starts against an **unreachable** name server (zero brokers => heartbeats cannot be sent to any broker), so offline one can only lock down to "the table entry is right and `already started` no longer throws" — the wire-level "a broker really received a heartbeat carrying the new subscription" is evidenced on a real cluster by `rmq_live_subscribe` |
| `scheduled_intervals` | 21 | The advancement semantics of periodic tasks (`scheduleAtFixedRate`): **the first tick lands at initialDelay itself**, not initialDelay+period (offline the route-refresh period is set to 1200ms and the measured arrival times are 11 / 1212 / 2412ms — the old formulation gave 1211 / 2411 / 3611); **fixed rate** rather than "sleep one period after finishing" (each tick is computed against the same time axis, so a slow round does not accumulate drift); when behind schedule there is no wait and the run catches up immediately (catch-up); `pollNameServerIntervalMillis` passing through facade -> constructor -> instance all the way, with a non-positive value falling back to the default 30000; and the period of the offset-persistence loop is read only once in `start()`, so changing the field at runtime does not reschedule the already-fixed cadence |
| `java_alignment` | 40 | The **two-level guard** over the request codes in `codes.h`: a per-item comparison against a built-in authoritative table (protecting against accidental edits on the C++ side), and, when `ROCKETMQ_JAVA_SRC` is set, additionally reading Java's `RequestCode.java` to regress the same-named constants (skipped when unset, not counted as failure) |
| `clean_expired_msg` | 64 | The suspension escape hatch of `cleanExpireMsg` (the only reclamation path when the listener hangs): looks only at **the head** with the smallest offset, the expiry predicate is **strictly greater than** `consumeTimeout`, one round takes `min(size,16)` with the cap computed once before entering the loop, send-back uses the fixed `delayLevel=3`, a message is removed only if it is **still the head** after a successful send-back, exceptions are only logged and messages stay in place, and `containsMessage` re-checks to prevent a message already swept from being re-delivered; only currently held queues are scanned |
| `pull_consumer_heartbeat` | 29 | The **heartbeat shape** of pull-mode / lite-pull consumers: `consumeType=CONSUME_ACTIVELY`, an empty `subVersion`, the `consumeFromWhere` semantics, one send each to master and slave in the first round, periodic re-sends in the loop, and `shutdown()` getting one code-35 send each with `producerGroup` absent (fake master-slave cluster) |

`interop` prints `WARN` records for **known defects on the Python reference client side** (which do not affect the exit code).
When a new `WARN` shows up, read it — it is the explicit ledger of cross-language deviations.
## Real-cluster integration

Requires a cluster running a nameServer(9876) + broker(10911) with `autoCreateTopicEnable=true`.
These tools are **not part of ctest** (they depend on an external cluster).

```bash
./build/examples/rmq_selfcheck                       # protocol-layer self-check, no cluster needed
./build/examples/rmq_live_message_types 127.0.0.1:9876
./build/examples/rmq_admin_live         127.0.0.1:9876
./build/examples/rmq_compression_live   selftest 127.0.0.1:9876
./build/examples/rmq_compression_live   send|recv 127.0.0.1:9876 <topic> <group> <size> [codec]
./build/examples/rmq_live_acl           127.0.0.1:9876   # needs a cluster with ACL enabled
./build/examples/rmq_live_pull          127.0.0.1:9876
./build/examples/rmq_live_lite_pull     127.0.0.1:9876
./build/examples/rmq_live_lite_pull_cursor 127.0.0.1:9876  # pull cursor: NO_MATCHED_MSG skips the whole span + OFFSET_ILLEGAL out-of-range self-heals
./build/examples/rmq_live_lite_pull_code 127.0.0.1:9876   # lite request code 361 + lite bit: flips litePullMessageEnable at runtime, restores it before exit
./build/examples/rmq_live_request_reply 127.0.0.1:9876   # 325 reply chain + the 10006/10007 codes
./build/examples/rmq_live_latency       127.0.0.1:9876
./build/examples/rmq_live_pop           127.0.0.1:9876
./build/examples/rmq_live_pop_consumer  127.0.0.1:9876   # POP consumption: consume-all / no re-delivery / re-delivery via invisible time + pullRT/pullTPS in the 307 state table
./build/examples/rmq_live_redelivery    127.0.0.1:9876   # thirteen sections: re-delivery/dead-letter/restart/broadcast/orderly/flow-control/namespace/partial ack/stall self-healing/orderly dead-letter/explicit ack rollback
./build/examples/rmq_live_trace         127.0.0.1:9876   # needs broker traceTopicEnable=true
./build/examples/rmq_live_hook          127.0.0.1:9876
./build/examples/rmq_validators_live    127.0.0.1:9876
./build/examples/rmq_recall_live        127.0.0.1:9876   # needs broker recallMessageEnable on (the tool enables it itself and restores it)
./build/examples/rmq_live_unit_config   127.0.0.1:9876   # unitName/unitMode/stream
./build/examples/rmq_sql92_live         127.0.0.1:9876   # needs broker enablePropertyFilter=true
./build/examples/rmq_live_backpressure  127.0.0.1:9876   # async-send backpressure (two fair semaphores)
./build/examples/rmq_live_async_send    127.0.0.1:9876   # async-send kernel (thread semantics/concurrency/pinned/interception/batch/pool shutdown)
./build/examples/rmq_live_send_header   127.0.0.1:9876   # send header c/d/n: the auto-created topic's queue count is decided by the template and d
./build/examples/rmq_live_flow_control  127.0.0.1:9876   # the five pre-pull flow-control thresholds (count/bytes/span/topic level) + no message lost after a hit
./build/examples/rmq_live_producer_unregister 127.0.0.1:9876   # unregister clientId(35) from every broker on exit
./build/examples/rmq_live_fail_fast   127.0.0.1:9876   # broker really dies: suspended long-polling declared dead within seconds (stops the broker once and restarts it, does not delete the store)
./build/examples/rmq_live_scheduled_intervals 127.0.0.1:9876   # periodic tasks' initialDelay/fixed rate (incl. the 10s first tick of offset persistence)
./build/examples/rmq_live_subscribe     127.0.0.1:9876   # late subscription: subscribe after start() pushes a heartbeat immediately and the new topic really gets consumed
./build/examples/rmq_live_pinned_guard  127.0.0.1:9876   # pinned-send topic guard: real routes are not wrongly rejected, rejection happens on the local end with no broker trace, oneway has no guard
./build/examples/rmq_live_correct_tags_offset 127.0.0.1:9876   # correctTagsOffset: NO_NEW_MSG/NO_MATCHED_MSG empty replies also push the committed offset up to maxOffset
./build/examples/rmq_live_offset_illegal 127.0.0.1:9876   # OFFSET_ILLEGAL: void in-flight/buffered messages wholesale and rebuild the queue at the corrected offset; the corrected offset persists immediately
./build/examples/rmq_live_reset_offset 127.0.0.1:9876   # 220 reset consume offset: after the broker pushes 220, persist immediately + void in-flight batches + rebuild the queue at the new offset
./build/examples/rmq_live_pull_heartbeat 127.0.0.1:9876 127.0.0.1:10911 [slave address]   # the pull consumer's 203/38/35 (assertions on the slave are included when its address is given)
./build/examples/rmq_live_publish_route_master 127.0.0.1:9876 127.0.0.1:10911 [slave address]   # stops the master once: publish queues drop to zero/subscription unchanged/consumption from the slave still works + the three subscription legs of orderly lock/POP/offset read
./build/examples/rmq_live_clean_expired_msg 127.0.0.1:9876   # the suspended-listener escape hatch: sweep sends back to %RETRY% and re-delivers a second time (~4 minutes, waits for two sweep cycles)
```

Any tool failure exits with a non-zero code. Skipped items and their reasons are spelled out in the output
(for example, the uniqKey query needs the broker to have RocksDB index enabled, and not finding it with the default
file index on a local machine is a **broker configuration difference, not a client bug**).

## Directory structure

```
cpp/
├── include/rocketmq/
│   ├── common/                 message model and constants
│   │   ├── message.h               Message / MessageExt / MessageBatch / MessageQueue
│   │   ├── message_decoder.h       17-segment + 6-segment codec (incl. compression)
│   │   ├── compression.h           CompressorFactory (zlib / lz4 / zstd type resolution)
│   │   ├── sysflag.h / mix_all.h / topic_config.h / subscription_data.h / util_all.h
│   │   ├── byte_buffer.h           big-endian read/write cursor
│   │   ├── consistent_hash.h       consistent-hash ring (`ConsistentHashRouter` + built-in MD5)
│   │   ├── recall_message_handle.h timed-message recall handle v1 codec
│   │   ├── boundary_type.h         boundary semantics of offset lookup by timestamp (LOWER/UPPER, incl. lenient getType parsing)
│   │   ├── logging.h               header-only logging (default INFO, size-based rotation, thread name/milliseconds/file:line)
│   │   └── net_compat.h            cross-platform socket compatibility (incl. SIGPIPE handling)
│   ├── remoting/
│   │   ├── remoting_client.h       sync / async / oneway + frame reassembly
│   │   └── protocol/               json / serialize / remoting_command / codes /
│   │                               headers / route / heartbeat / body / admin_body /
│   │                               subscription
│   └── client/
│       ├── mq_client.h             MQClientInstance: route discovery + all RPCs
│       ├── producer.h / consumer.h / admin.h / result.h / exception.h
│       ├── allocate_strategy.h     six queue-allocation strategies (AVG / AVG_BY_CIRCLE / CONFIG /
│       │                            CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY)
│       ├── hook.h / trace.h / trace_hook.h / trace_dispatcher.h
│       │                            hook interfaces (Send/Consume/EndTransaction/CheckForbidden/
│       │                            FilterMessage) + trace-text codec + async dispatch
├── src/                        44 .cpp files mirroring the include tree
├── examples/                   selfcheck / interop_tool + 33 real-cluster integration tools
└── tests/                      49 test source files + interop_check.py = 50 ctest cases
```

## A few implementation conventions you must know

**One wrong field name and the field is silently dropped.** The broker deserializes JSON by the official protocol's property names —
when a field name does not match there is no error; the field simply disappears (its default value takes over), so the on-wire key names were cross-checked field by field.

**POP queues come from the client-side rebalance, not from broker assignment.** With
`clientRebalance=false`, Java asks the broker through
`RebalanceImpl#getRebalanceResultFromBroker:345` → `MQClientAPIImpl#queryAssignment:405`
(QUERY_ASSIGNMENT=400, returning `MessageQueueAssignment` with mode=POP), so the broker decides
which queues this instance owns. That path is **deliberately not implemented here** (the same
decision as the other six ports, see the matching comment in `python/client/consumer.py`): queues
are still computed locally by the allocation strategy, then one POP loop + ack per queue. The
semantics are equivalent — only WHO picks the queue set differs. Read the `doRebalance` comment in
`src/client/consumer.cpp` before changing this.

**Enum fields go on the wire as uppercase enum names.** `SearchOffsetRequestHeader.boundaryType` is one example:
on the wire it is `"LOWER"`/`"UPPER"`, while `BoundaryType.getName()`'s `"lower"`/`"upper"`
only feed `BoundaryType.getType` for comparison. The broker-side `getType` parses **leniently**
(only `equalsIgnoreCase("upper")` is UPPER; unknown values are uniformly LOWER),
and the field itself is `@CFNullable` (if it is not set the whole key is not written; a missing key falls back to LOWER).
`DefaultMQAdminExt::searchLowerBoundaryOffset` / `searchUpperBoundaryOffset`
send those two values fixed respectively, and `searchOffset` is equivalent to LOWER.

**fastjson2 can produce invalid JSON.** When the broker serializes, object keys of maps get inlined
(`{"offsetTable":{{"brokerName":"b",...}:{...}}}`), numeric keys go unquoted, and NaN/Infinity and trailing
commas are allowed. So `json.cpp` contains a **lenient parser** rather than a strict JSON parser.
When modifying it, be sure to keep this tolerant logic, or every admin response body will break.

**The presence check for `RemotingCommand.body` is `hasBody || !body.empty()`** —
assigning only `body` without setting `hasBody` must still encode the body.

**opaque must not use 0 as an "unset" sentinel.** `opaqueCounter` starts counting at 0, so the very first request's legitimate opaque value is 0;
the transport layer reassigns only when it **truly conflicts with an in-flight request**, otherwise responses can never be matched.

**`TopicPublishInfo` is not copyable** and must be accessed via `std::shared_ptr` — its queue round-robin cursor is state shared
across calls (the official implementation uses a ThreadLocal), and returning it by value would make every send start over from queue 0.

**Each per-queue loop thread of the push consumer must catch its own exceptions** (`DefaultMQPushConsumer::runLoop`).
`shutdown()` flips `started_` first and then joins, while any point in the loop that grabs the internal client will throw
`MQClientException("consumer not started, call start() first")` — without a catch outside the thread function it is
`std::terminate` and the **entire process** goes down with the consumer (on a real cluster that looked like S6 simply aborting, as if the cluster had died).
So all four resident threads (dispatch / offset persistence / orderly lock / rebalance) go uniformly through `runLoop`: an exception is logged as WARN, then the thread sleeps 1s and retries, and it exits in place once `started_`/`stop_` go down; both before spawning a derived pull thread and at the entry of `queuePullLoop` these two flags are re-checked (the rebalance pass before the join may still be interrupted by shutdown right at the moment a thread is being created).

**Compression failures / unknown algorithms must be loud.** `CompressorFactory::decompress` throws for unsupported types,
and `decodeMessage` catches it and returns `false` (the message is discarded). **Compressed bytes must never be passed through as-is**:
the outer layer clears the `COMPRESSED_FLAG`, so passing them through equals handing out a compressed stream as the body with no way to recognize it afterwards — that is silent data corruption.

**The clientId semantics.** `buildClientId(instanceName, unitName, enableStream)` =
`<local IP>@<instanceName>[@<unitName>][@STREAM]` (a blank unitName segment is not appended,
and `@STREAM` uses the enum name rather than the code); when instanceName is still the default `DEFAULT`, each facade's
`start()` calls `changeInstanceNameToPID` to replace it **in place** with `<pid>#<nanoTime>` — unconditionally for the producer and admin,
and for the three consumers only under `CLUSTERING` (broadcast consumers keep `DEFAULT`). The local IP is probed via a UDP
sockname. Regression: `tests/test_client_id.cpp` (ctest `client_id`).

**unitMode / stream are on-wire fields, not local decorations.** The three kinds of switches each have their wire destinations:
`unitName` goes into the clientId and the dynamic addressing URL (`-<unitName>?nofix=1`);
`unitMode` goes into the single-letter key `k` of `SEND_MESSAGE_V2`, `ConsumerData.unitMode` in heartbeats,
the send-back request header, and the filter/forbidden hook contexts; `enableStreamRequestType` makes every request carry `ReqT=0`
(the value is the **code** of `RequestType.STREAM`, unlike the enum-name suffix in the clientId).
The producer defaults stream to off (only the pull / lite consumers set `true` in their constructors).
**The order is semantics**: install the Stream hook before user (ACL) hooks, and `ReqT` must fall **inside** the signed content,
otherwise a broker with authentication enabled will always fail signature verification. This port's transport layer has only one RPCHook slot, so facades uniformly compose via
`composeRequestHooks()` and then `registerRPCHook()`, and bind it **before** `MQClientInstance::start()` — the instance's very first packet should already carry it.
Regressions: `tests/test_acl.cpp` (hook composition and signature content) + `tests/test_send_retry.cpp`
(the `k` / `ReqT` captured over a real socket, with signature verification replayed using the broker-side algorithm) +
`examples/live_unit_config.cpp` (the real broker's topic `sysFlag` UNIT=0x1 / UNIT_SUB=0x2,
and the clientId as recorded by the broker).

**The batch-send request code is 320, not 310.** `sendRequestCode()` (`src/client/mq_client.cpp`)
follows a three-level decision: first `isReplyMessage` => 325, then `msg.isBatch` => `SEND_BATCH_MESSAGE(320)`,
otherwise `SEND_MESSAGE_V2(310)`. Note that the request code and the V2 header's single-letter batch key `m` are two different things: the broker
uses `m` to choose batch vs single-write, while the code only affects the server's by-code classification — the two fields must be evidenced as a pair.
Regressions: `tests/test_send_retry.cpp` (capturing `code` + `m` over a real socket) +
item 8 of `examples/live_message_types.cpp` (the real broker delivering a batch as 3 independent messages with
contiguous `queueOffset` 0,1,2).

**The trace decoder is more robust than the official implementation.** The official implementation throws an ArrayIndexOutOfBounds on `SubBefore` for messages without keys
(a real defect in the 5.5.1 upstream, reproduced), while this project takes a missing segment as the empty string; and a decoding failure of a **single record** only skips that record —
in the official implementation one bad record destroys the decoding of the whole trace message (manifesting as the entire batch of traces disappearing from the console). Trace text is segmented by
`\x01` and each record ends with `\x02`, and the split must use **`String.split` semantics (discarding trailing empty strings)** —
a native split yields one extra segment. Troubleshooting hint: the trace topic defaults to `RMQ_SYS_TRACE_TOPIC`,
and the broker needs `traceTopicEnable=true` to pre-create it.

## Logging

`include/rocketmq/common/logging.h` is a header-only logger, default level **INFO**,
writing to both stderr and `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log`.

Line format `date.milliseconds level [pid] [thread name] [file:line] - message`:

```
2026-09-14 17:06:14.566 INFO  [57308] [main] [producer.cpp:110] - DefaultMQProducer[...] started, clientId=...
2026-09-14 17:07:09.086 INFO  [57316] [ConsumeMessageThread_0] [consumer.cpp:161] - DefaultMQPushConsumer[...] started
```

Thread names: the main thread lands as `main`, and worker threads are named internally —
`ConsumeMessageThread_N` / `AsyncSenderThread_N` / `RemotingClientReader-<ip:port>`.
The latter two only leave traces on exception paths (connection closed, invalid frame length, decode failure), so normal logs usually show only the former.

| Environment variable | Default | Description |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` / `OFF` |
| `ROCKETMQ_CLIENT_LOG_FILE` | `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log` | Set to the empty string / `OFF` / `NONE` to keep only stderr |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864` (64MB) | Per-file cap; `0` = no rotation |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | Number of backups; `0` = keep none |

The rotation semantics are **FixedWindow**: the oldest `<file>.N` is deleted first, the rest shift one by one, and finally base -> `.1`.

Three known behaviors (all explicitly documented in the header; they are not bugs):

1. Backups are **not compressed**;
2. Writes are **synchronous** (each line `fflush`ed, so `tail -f` shows lines in real time);
3. Connection closure is logged at **DEBUG** (a normal shutdown also hits the same path, and logging it at INFO/WARN
   under the default INFO level would turn it into "fake exception" noise at exit). Real protocol anomalies (invalid frame length, decode failure) are still logged at **WARN**.

**Benign long-polling timeouts go to DEBUG** (suppressed by default), so `ERROR=0` in a normal running log is the expected state —
an ERROR means a real problem.

> 📌 The file name is deliberately differentiated from the other language ports. Each port has a different rotation policy, and writing to the same file would interleave lines.

## License

Apache-2.0, consistent with upstream RocketMQ.
