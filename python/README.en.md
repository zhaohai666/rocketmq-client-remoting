# rocketmq-client-remoting (Python)

> [中文](README.md) | English

A Python implementation of RocketMQ's classic remoting protocol: a Python process can talk to the
NameServer and the Broker directly using either of the two serializations, **JSON / RocketMQ binary**.
Compatible with 4.x / 5.x servers; every capability has been integration-tested against a real 5.5.1 cluster;
aligned item by item with this repository's C++ / C# / Rust implementations (`../cpp`, `../csharp`, `../rust`).

## Installation and testing

```bash
pip install -e .
pytest -q                     # 1163 passed + 4 skipped (the skips relate to the optional compression dependencies)
python selfcheck.py           # protocol codec round-trip self-check (7 items, no cluster needed; run inside the python/ directory)
```

> ⚠ Package name vs import name: the PyPI package name is still `rocketmq-client-remoting`, but in the
> source tree `client` / `common` / `remoting` are three **top-level packages** (the old nested `rocketmq.*`
> package has been dropped), so imports are written `from client.producer import DefaultMQProducer`.

## Quick start

Sending:

```python
from client.producer import DefaultMQProducer
from common.message import Message

producer = DefaultMQProducer("PID_DEMO")
producer.set_namesrv_addr("127.0.0.1:9876")
producer.start()
result = producer.send(Message("TopicTest", "hello".encode("utf-8")))
print(result.status, result.msg_id, result.queue_offset)
producer.shutdown()
```

Consuming (for a complete runnable snippet see `README.md` in the repository root):

```python
from client.consumer import DefaultMQPushConsumer, MessageListenerConcurrently
from client.consumer_result import ConsumeConcurrentlyStatus

consumer = DefaultMQPushConsumer("GID_DEMO")
consumer.set_namesrv_addr("127.0.0.1:9876")
consumer.subscribe("TopicTest")
consumer.set_message_listener(MessageListenerConcurrently())
consumer.start()
# When messages arrive the callback DemoListener.consume_message(msgs, context) runs, returning
# ConsumeConcurrentlyStatus.CONSUME_SUCCESS or RECONSUME_LATER
...
consumer.shutdown()
```

## Feature overview

| Layer | Contents |
| --- | --- |
| Protocol layer | Dual JSON / RocketMQ-binary serialization; `RemotingCommand` frame encoding/decoding; the CommandCustomHeader family (incl. the V2 short field names a..n); the 17-segment message storage format and the 6-segment batch format |
| Transport layer | `RemotingClient`: sync / async / oneway, half-packet reassembly, opaque matching, reconnection, connection death detection, TLS |
| Route / heartbeat | `TopicRouteData` / `QueueData` / `BrokerData`, `SubscriptionData`, `HeartbeatData`, dynamic name server |
| Producer | sync / pinned (specific-queue) / selector / async (incl. batch async and the backpressure semaphores) / oneway / batch / two-phase transactions / `recallMessage`(370) |
| Consumer | `DefaultMQPushConsumer` (concurrent / orderly listening, the five flow-control thresholds, offset correction and reset, stalled-loop self-healing, suspended-message sweeping), `DefaultMQPullConsumer` (`fetch_subscribe_message_queues` returns the whole topic, `fetch_message_queues_in_balance` returns only this instance's share), `DefaultLitePullConsumer` (three offset tables), the POP consumption loop |
| Queue allocation | AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY |
| Namespace | Two independent mechanisms: `namespace` (a client-side resource prefix `%%ns%%res`, Java's `NamespaceUtil`, wrapped and unwrapped across send/consume/heartbeat/offset) and `namespace_v2` (the server-side namespace: `NamespaceRpcHook` stamps `nsd=true` / `ns=<value>` on every request; hook order Namespace → Stream → ACL, so both `ns` and `ReqT` sit inside the ACL signature). The producer, all three consumers, the admin client and the trace dispatcher read `namespace_v2` live per request, not as a start-time snapshot |
| Admin | The full `DefaultMQAdminExt` set: topic/subscription-group configuration, offset query and reset, consumer-group connection query, message query |
| Observability and security | message tracing (Pub/SubBefore/SubAfter/EndTransaction/Recall encoding/decoding + asynchronous dispatch), Send/Consume/EndTransaction hooks, ACL signing |
| Compression | zlib (standard library) / LZ4 / ZSTD; type bits 0/3 = ZLIB; unsupported types must raise (compressed bytes are never passed through) |

## Real-cluster integration testing

You first need a running nameServer(9876) + broker(10911) with `autoCreateTopicEnable=true`
(cluster scripts live in this repository's `scripts/`; for the cross-language compression matrix see
`scripts/compression_matrix.sh`).

```bash
python verify_message_types.py            # the 7 message capability classes (sync/async/oneway/batch/transaction/pinned/request-reply)
python verify_live_clean.py               # send+receive + batch-send reconciliation + clean-store end to end
python verify_request_reply_live.py       # request-reply full chain (325 reply + timeout/fabricated-reply-failure error codes 10006/10007)
python verify_async_send_live.py          # the async send kernel (thread accounting/concurrency/pinned/hooks/batch/shutdown)
python verify_backpressure_live.py        # async send backpressure (two fair semaphores)
python verify_admin_live.py               # admin full chain + sendMessageBack redelivery + offset reset
python verify_validators_live.py          # name validation + addressing-fault qualification (10004)
python verify_unit_config_live.py         # unitName/unitMode/stream (clientId suffix, ReqT)
python verify_recall_live.py              # timer-message recall recallMessage(370) (flips the recallMessageEnable switch automatically and restores it)
python verify_trace_live.py               # message tracing full chain (requires broker traceTopicEnable=true)
python verify_hook_live.py                # CheckForbidden / FilterMessage hooks
python verify_compression_live.py selftest  # automatic compress produce-and-consume + broker-side compressed-body check + Message reuse
python verify_compression_live.py send|recv <topic> <group> <size>  # cross-client compression interop
python verify_send_header_live.py         # the three send-header fields c/d/n
python verify_pinned_guard_live.py        # the topic guard of pinned sends (real route not wrongly rejected/rejected on this end/oneway has no guard)
python verify_tls_live.py                 # the whole client chain running over TLS
python verify_acl_live.py                 # needs a cluster with ACL enabled
python verify_acl_java_parity.py          # ACL signature parity against the official signature vectors (offline, no cluster needed)
python verify_transaction_live.py         # two-phase transactions
python verify_pull_live.py                # DefaultMQPullConsumer pulling
python verify_pull_consumer_heartbeat_live.py  # 203/38/35 for the pull-mode consumer (deletes the topic it created)
python verify_consumer_heartbeat_slave_live.py # consumer heartbeat fan-out to slave nodes (needs one slave in the cluster)
python verify_producer_unregister_live.py # producer exit unregistration UNREGISTER_CLIENT(35)
python verify_subscribe_live.py           # post-start subscription + immediate heartbeat
python verify_interval_live.py            # scheduled-task periods (initialDelay/fixed rate)
python verify_fail_fast_live.py           # in-flight requests are declared dead immediately when the broker really dies (stops a broker once and brings it back)
python verify_lite_pull_live.py           # lite pull full chain (rebalance/assign+seek/policies/three offset tables)
python verify_lite_pull_cursor_live.py    # the lite pull cursor follows nextBeginOffset (NO_MATCHED_MSG/out-of-range self-healing)
python verify_lite_pull_code_live.py      # lite request code 361 + the lite bit (flips litePullMessageEnable at runtime, restores it before exit)
python verify_flow_control_live.py        # the five flow-control thresholds before pulling + the numeric gate at startup
python verify_pull_expired_live.py        # stalled pull loop self-healing (120s threshold)
python verify_ack_index_live.py           # ackIndex partial ack for concurrent consumption
python verify_orderly_reconsume_live.py   # orderly-consumption redelivery gate + explicit COMMIT/ROLLBACK
python verify_redelivery_live.py          # redelivery/dead-letter terminal state/partial ack/stall self-healing/orderly dead letter
python verify_correct_tags_offset_live.py # an empty response still advances the committed offset
python verify_offset_illegal_live.py      # the OFFSET_ILLEGAL correction branch (dropped queue + corrected offset persisted immediately)
python verify_reset_offset_live.py        # 220 consumer offset reset (two body shapes + in-flight invalidation)
python verify_publish_route_master_live.py # publish routing skips brokers without a master (stops a master once)
python verify_clean_expired_msg_live.py   # the escape hatch that sweeps suspended listeners (~4 minutes, deletes its own topic)
python verify_sql92_live.py               # SQL92 filtering + CHECK_CLIENT_CONFIG(46), needs broker enablePropertyFilter=true
python verify_pop_live.py / verify_pop_consumer_live.py  # POP control plane / POP consumption loop (incl. the 307 pull statistics)
python verify_latency_live.py             # fault avoidance (latency window/isolation/recovery)
```

Any script exits with a non-zero exit code on failure. Some scripts have extra requirements on the
cluster, noted item by item above (stopping/starting a broker, needing a slave node, needing to flip a
broker configuration and restore it on exit, and so on).

**POP queues come from the client-side rebalance, not from broker assignment**: with
`clientRebalance=false`, Java asks the broker through
`RebalanceImpl#getRebalanceResultFromBroker:345` → `MQClientAPIImpl#queryAssignment:405`
(QUERY_ASSIGNMENT=400, returning `MessageQueueAssignment` with mode=POP). That path is
**deliberately not implemented here** (the same decision across all seven ports; see the class-head
comment in `client/consumer.py`): queues are computed locally by the allocation strategy, then one
POP loop + ack per queue. Semantics are equivalent; only WHO picks the queue set differs.

## Client logging

`rocketmq_logging.py` bridges the internal logging to the standard `logging`:

- Files land in **`<current working directory>/logs/rocketmqlogs/rocketmq_py_client.log`** (deliberately not
  under the user HOME: the Python client is often embedded as a script inside somebody else's process, so
  quietly creating directories and writing files under `$HOME` is an out-of-bounds side effect; for the Java
  convention set `ROCKETMQ_CLIENT_LOG_DIR=$HOME/logs/rocketmqlogs` explicitly), daily rolling, with backup
  names `rocketmq_py_client.log.YYYY-MM-DD`, keeping `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` (default 10) files;
- Output also goes to stderr (can be turned off with `ROCKETMQ_CLIENT_LOG_USE_STDOUT=false`);
- If the host program has already configured Python logging (the root logger already has handlers), it does
  **not interfere at all** and leaves the logging to the host's configuration.

Environment variables: `ROCKETMQ_CLIENT_LOG_DIR` / `ROCKETMQ_CLIENT_LOG_FILE` /
`ROCKETMQ_CLIENT_LOG_LEVEL` / `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` /
`ROCKETMQ_CLIENT_LOG_USE_STDOUT` (the level accepts both spellings `WARN`/`WARNING` and `TRACE`/`DEBUG`).

> 📌 The default file name `rocketmq_py_client.log` is deliberately different from the file names of the other
> language ports: each port uses a different rotation policy (Python renames per day), and writing into the
> same file would interleave lines between them. The regression guard is `tests/test_logging_config.py`.

## Directory structure

`client/`, `common/` and `remoting/` sit directly in this directory (`python/`) and are three **top-level
packages** — put `python/` on `sys.path` and `from client.producer import DefaultMQProducer` works
(`tests/conftest.py` already does this).

```
python/
├── common/                message model and constants shared by the client
│   ├── message.py             Message / MessageExt / MessageBatch / MessageQueue
│   ├── message_decoder.py     encoding/decoding of the 17-segment storage format + 6-segment batch format (incl. zlib decompression)
│   ├── message_const.py       MessageConst property keys (incl. INDEX_KEY/UNIQUE/TAG_TYPE)
│   ├── sysflag.py             MessageSysFlag / PullSysFlag / PermName
│   ├── boundary_type.py       boundary semantics of timestamp offset search (LOWER/UPPER, incl. the lenient getType parsing)
│   ├── mix_all.py / util_all.py
│   ├── recall_message_handle.py timer-message recall handle v1 (base64url + 5 segments)
│   └── subscription_data.py / topic_config.py / message_accessor.py
├── remoting/              transport layer (plain sockets + threads)
│   ├── client.py              RemotingClient: sync / async / oneway
│   ├── exception.py
│   └── protocol/
│       ├── remoting_command.py  frame encoding/decoding (totalLen|headerLen+type|header|body)
│       ├── serialize.py         RemotingSerializable(JSON) / RocketMQSerializable(binary)
│       │                        + the fault-tolerant fastjson2 parser (see "Admin")
│       ├── codes.py             RequestCode / ResponseCode / LanguageCode / SerializeType
│       ├── headers.py           the CommandCustomHeader family (incl. the V2 short field names a..n)
│       ├── body.py / admin_body.py / route.py / heartbeat.py / subscription.py
├── client/                the user-facing API
│   ├── producer.py / consumer.py / admin.py / mq_client.py
│   ├── hook.py / trace.py / trace_hook.py / trace_dispatcher.py
│   │                          hook interfaces (Send/Consume/EndTransaction/CheckForbidden/FilterMessage)
│   │                          + message-trace text encoding/decoding + asynchronous dispatch
│   └── send_result.py / consumer_result.py / exception.py
├── rocketmq_logging.py    logging bridge (originally logging.py, moved up and renamed to avoid shadowing the standard-library logging)
├── __main__.py            command-line entry point (selfcheck)
└── selfcheck.py           protocol self-check without a cluster
```

## clientId conventions

The default clientId is `<local IP>@<instanceName>[@<unitName>][@STREAM]`, and while `instance_name` is
still the default value `DEFAULT` it is rewritten **in place** inside `start()` to `<pid>#<monotonic_ns>`:
the producer and the admin do this unconditionally, the three consumers only under `CLUSTERING` — broadcast
consumers keep `DEFAULT`, so broadcast consumers of the same process compute the same clientId (occupying the
same key in `INSTANCE_MAP`). Writing back in place means a restart does not change the identity.

The three settings `unit_name` / `unit_mode` / `enable_stream_request_type` exist on all five facades,
default values: producer/push/admin keep stream off, pull/lite turn it on already at construction.
⚠ Do not mix up two conventions: in ExtFields `ReqT` is the string form of `RequestType.STREAM.getCode()`,
i.e. `"0"`, and only the clientId tail carries the enum name `@STREAM`. The transport layer has only a
single hook slot, and the order is restored by `compose_request_hooks()` — stream must come **before** ACL,
otherwise `ReqT` falls outside the signature; hooks must be registered before `MQClientInstance.start()`.
The local IP is obtained by "connecting" a UDP socket to a public address and then reading the sockname.

Regression tests: `tests/test_client_id.py`, `tests/test_unit_config.py`, `verify_unit_config_live.py`.

## Push consumer startup validation (`consumer.py:_check_config_ranges()`)

`start()` checks 13 numeric ranges one by one, always compared as `< lo or > hi` (both ends inclusive), and
the messages are the original wording of the official Java client (only the FAQ short-link tail removed).
Key points:

- For `pullThresholdForTopic` / `pullThresholdSizeForTopic` the value `-1` is an "off" sentinel;
  **the other gates have no such exemption**, `-1` is rejected there as well;
- The lower bound of `pullInterval` is **0** (0 = no interval); do not copy the 1 from neighbouring gates;
- The unit of `pullThresholdSizeForQueue` / `pullThresholdSizeForTopic` is **MiB**;
- `consumeThreadMin > consumeThreadMax` is a **strict greater-than** (equality is legal, single-threaded
  consumers are allowed), and the message carries both values;
- `popBatchNums` follows the literal `<= 0`, while the message still says `[1, 32]`;
- The validation runs after all null checks and before `MQClientInstance` connects — a bad configuration
  must fail before the clientId is registered, otherwise the broker's `ConsumerManager` keeps a pile of
  zombie clientIds that never heartbeat, skewing the `cidAll` used for rebalance (on a real cluster this
  shows up as uneven queue allocation, while the client log contains only the startup failure entry).

`consume_timestamp` is a configurable `%Y%m%d%H%M%S` string, and the format check **really rejects** bad
values before connecting. Regression guard: `tests/test_consumer_check_config.py` (57 items, locking each
range's two ends, the `-1` sentinel, the check order and the wording); the real-cluster guard is S5 of
`verify_flow_control_live.py`.

## Scheduled task periods (pollNameServerInterval / persistConsumerOffsetInterval)

The background periodic tasks of `MQClientInstance` are as follows. **The first hop of every loop lands
exactly at `initialDelay`**, not at `initialDelay + period` (the `scheduleAtFixedRate` semantics; real-cluster
runs have caught the version scheduled the latter way):

| Loop | initialDelay | Period | Configurable field (default) |
|------|--------------|------|------------------|
| Dynamic name server refresh | 10s | 2min | — (scheduled only when no static address is configured and an address server exists) |
| In-use topic route refresh | 10ms | `pollNameServerInterval` | `poll_name_server_interval` (30000ms) |
| Heartbeat | 1s | 30s | `heartbeat_interval_millis` |
| Consumer offset persistence | 10s | `persistConsumerOffsetInterval` | `persist_consumer_offset_interval` (5000ms) |
| Thread-pool elasticity sweep | 1min | 1min | — |

Key points:

- `poll_name_server_interval` **exists on all five facades** (producer / push / pull / lite / admin),
  and each `start()` passes it through to `MQClientInstance`; the period is read once at the loop entry, so
  changing the field afterwards does not affect already-scheduled tasks (`scheduleAtFixedRate` schedules once).
- `persist_consumer_offset_interval` governs only the **background periodic persistence**; the final flush in
  `shutdown()` is unconditional, so enlarging the period only delays when offsets are persisted, it does not
  lose them. In clustering mode offset persistence uses the **synchronous** `UPDATE_CONSUMER_OFFSET`;
  broadcast mode writes the local `~/.rocketmq_offsets/<clientId>/<group>/offsets.json`.
- The heartbeat has one **known and intentional deviation**: Python sends one synchronous heartbeat round
  first inside `start()` and then runs on the 30s period. That first synchronous heartbeat covers the
  "visible right after the clientId is registered" role; the period is unchanged.
- The periods and first-hop ordering above are locked item by item by `tests/test_scheduled_intervals.py`;
  the real-cluster guard `verify_interval_live.py` contrasts two instances with different periods to show that
  the difference comes from the period itself.

## Admin (`client/admin.py`)

The full `DefaultMQAdminExt` set of admin interfaces. Three **counter-intuitive server behaviours**, all
found by hitting a real cluster:

| Pitfall | Actual behaviour |
| --- | --- |
| `GET_BROKER_CONFIG` | The body is **properties text** (`"k=v\n"`), not JSON/KVTable. It goes through `MixAll.string2_properties`, with semantics aligned to `java.util.Properties.load` (`#`/`!` comments, trailing `\` line continuation, **whitespace is also a separator**) |
| `CreateTopicRequestHeader` | Must carry `topicFilterType`, otherwise the broker throws `topicFilterType = [null] value invalid`; `attributes` must be `""` rather than null |
| `ResetOffsetBody.offsetTable` | It is a `Map<MessageQueue, Long>`; when a MessageQueue is used as a key the broker side **inlines it into a JSON object**, producing **invalid JSON** |

The last one is the key one: the broker's serializer (fastjson2) inlines object keys of maps
(`{{"brokerName":"b",...}:{...}}`), leaves numeric keys unquoted, and allows NaN/Infinity and trailing
commas. So `serialize.py` uses a **hand-written fault-tolerant parser**, `fastjson_loads`, not the standard
`json.loads`. **When changing this parser, keep the lenient logic**, otherwise every Admin response body breaks.

`GET_ALL_SUBSCRIPTIONGROUP_CONFIG` is a **paginated** interface (`groupSeq`/`maxGroupNum`/`dataVersion`);
old brokers have no `totalGroupNum` in the response, in which case one round is enough. KV-configuration
requests go to the **NameServer**, and PUT/DELETE must be broadcast to every NameServer.

Among the `GET_MESSAGE`-style queries, **the uniqKey (msgId) query requires the broker to have the RocksDB
index enabled**; with the default file index and a message that never set KEYS, not finding it is a
**broker configuration difference, not a client bug**.

Offset reset has **two different paths**, do not mix them up again: `reset_offset_by_timestamp` (222 without
`queueId`, `offset=-1` meaning null) resets the whole topic by timestamp; `reset_offset_by_queue_id` is
**two** RPCs — first `update_consumer_offset`(25) writing the offsetTable, then a 222 carrying
`queueId`+`offset` so the broker runs `resetOffsetInner` → `assignResetOffset` (which also writes the
**one-shot** `resetOffsetTable`). On a real cluster (5.5.1) two things were measured:

1. **The first pull after a reset gets no messages** — the broker answers `OFFSET_RESET` directly ⇒
   `PULL_OFFSET_MOVED`, which the client maps to `OFFSET_ILLEGAL` + `next_begin_offset=reset offset`, and
   only the second pull actually retrieves the historical messages.
2. These two RPCs are **not atomic**: for an out-of-range target the first call already persists the illegal
   offset and only the second is rejected by `Target offset N not in consume queue range [min-max]`. There is
   no protective rollback, and section 9.5 of `verify_admin_live.py` pins this behaviour as an assertion.

`query_topics_by_consumer` is the **group-level** overload (it takes only `group`: it looks up routes via
`%RETRY%<group>`, fans out 343 per broker, and merges results with Set de-duplication); the original raw
single-broker call was renamed to `query_topics_by_consumer_to_broker`. 343 reads the offsetTable
(`whichTopicByConsumer`), so **returning an empty table when the group never committed offsets is expected**.

Timestamp offset search carries a **`boundaryType`**: the admin's two boundary entry points
`search_lower_boundary_offset` / `search_upper_boundary_offset` send `LOWER` / `UPPER` fixed respectively,
`search_offset` is equivalent to LOWER; the MQ-level `search_offset_by_timestamp` also defaults to an
explicit LOWER, and passing `boundary_type=None` omits the key entirely. The wire text is the **uppercase
enum name** (`LOWER`/`UPPER`), the field is `@CFNullable`, a missing key makes the broker fall back to LOWER,
and a present key with an unrecognized value (it only checks `equalsIgnoreCase("upper")`) also falls back.
The real-cluster discriminator (section 7.5 of `verify_admin_live.py`): after sending 3 messages to a
1-queue topic, search the offset for a far-future timestamp — LOWER = maxOffset(3), UPPER = maxOffset-1(2);
two different numbers prove the field really reached the broker and was parsed. The wire shape is locked by
`tests/test_search_offset_boundary.py`.

## The two message encoding paths

This is the easiest place to get confused, and they **must not be mixed**:

| Scenario | Python function | Segments |
| --- | --- | --- |
| broker write / pull response | `encode_message_ext` | 17 segments |
| batch message body | `encode_message` / `encode_messages` | 6 segments |
| plain message send request body | take `msg.get_body()` directly | raw bytes |

The 17-segment format (`MessageExt`):

```
TOTALSIZE(4) | MAGICCODE(4) | BODYCRC(4) | QUEUEID(4) | FLAG(4) | QUEUEOFFSET(8)
| PHYSICALOFFSET(8) | SYSFLAG(4) | BORNTIMESTAMP(8) | BORNHOST(8|20) | STORETIMESTAMP(8)
| STOREHOST(8|20) | RECONSUMETIMES(4) | PREPAREDTRANSACTIONOFFSET(8) | BODY(4+n)
| TOPIC(1|2+n) | PROPERTIES(2+n)
```

MAGICCODE is `-626843481` (v1, topic length 1 byte) or `-626843477` (v2, used on the broker side when the
topic is longer than 127 characters, topic length 2 bytes). In the 6-segment format MAGICCODE and BODYCRC are
fixed at 0 and there is no topic.

## RemotingCommand frame format

```
totalLength(4) | headerLength(4) | headerData | bodyData
                ^^ the high 8 bits are the serialization type, the low 24 bits are the header length
```

`totalLength = 4 + len(headerData) + len(bodyData)`. A JSON header is a plain JSON object; the ROCKETMQ
binary header layout is
`code(2) | language(1) | version(2) | opaque(4) | flag(4) | remark(4+n) | extFields(4+map)`,
where map keys use a short length and values an int length.

## flag semantics

```
bit0 = response type (RPC_TYPE)      bit1 = oneway (RPC_ONEWAY)
```

`create_response_command` sets bit0; `mark_oneway_rpc` sets bit1.

## TLS connections (`tls_enable=True`)

The 5.5.1 nameServer/broker sniff the protocol by the first byte under `tls.test.mode.enable` (default true),
so a single port accepts both plaintext and TLS, which means the whole chain can be verified directly against
a real cluster: `verify_tls_live.py` (30 rounds of brand-new TLS connections sending the first packet,
producer + push consumer exchanging messages entirely over TLS, confirming no connection silently fell back
to plaintext, and no read thread left behind after shutdown).

Two constraints measured on macOS loopback are written in the corresponding docstrings of
`remoting/client.py`; read them before making changes:

- **The read thread must only start after the first record has been written out.** If the read thread enters
  OpenSSL (`pending()` / `recv()`) right after the handshake, the first request record has roughly a 3~5%
  chance of never reaching the peer: `sendall()` returns success, the peer's TLS read waits until timeout,
  and the caller can only burn the whole invoke timeout. This reproduces reliably only when the peer is CPython
  `ssl` (against the real cluster's nameServer/broker, both orderings lose 0 over 60 rounds each), so the
  regression guard lives on the local TLS mock server in `tests/test_tls_trace.py`. Plaintext connections lose
  0%; only TLS connections take the deferred-thread path in `_write`.
- **Closing must go through `close_notify`.** With a bare `closesocket()`, the kernel receive buffer still holds
  the peer's TLS 1.3 NewSessionTicket that the SSL layer never read, so an RST is sent instead of a FIN; that
  RST lands on the next new connection reusing the same 4-tuple, and it was measured to silently swallow that
  connection's first packet in about 25~30% of the cases.

Therefore a TLS connection's socket is always closed by that connection's read thread (`close_channel` /
`shutdown` first attach the connections whose read thread has not started yet, to avoid leaking fds). Two
threads entering one OpenSSL object at the same time corrupts its internal state (segfaults observed in
practice), so do not replace the two rules above with "add a lock".

## Message compression

The producer side compresses automatically when `body >= compress_msg_body_over_howmuch` (default **4096**),
setting `COMPRESSED_FLAG | type bits`; `MessageBatch` is **not compressed**. The consumer side decompresses in
`decode_message` as soon as it detects `COMPRESSED_FLAG`, and **clears that flag bit**.

A few counter-intuitive points:

- **Compression happens only once, outside the retry loop.** If you compress in place with `set_body` inside
  the loop, a retry compresses the already-compressed body **a second time** (`zlib(zlib(x))`), while the
  consumer only decompresses one layer → it gets the compressed stream.
- Compression type bits `0` and `3` are **both decoded as ZLIB** (backward compatibility); messages produced by
  old clients carry type bit 0.
- **Unsupported algorithms (e.g. SNAPPY=4) must raise, never pass the bytes through unchanged** — silent
  pass-through is silent data corruption. There are two layers here and both must be kept:
  1. `_decompress` raises `RuntimeError` for unsupported types;
  2. `decode_message` catches it and returns `None` (i.e. "the message is dropped").
- `zlib` is part of the Python standard library, so the decompression path needs no extra dependency; when
  `lz4`/`zstd` are not installed the code **raises** instead of silently degrading.

## Send request codes: 310 / 320 / 325

All three branches of `_build_send_request` must be evidenced on the live wire (`tests/test_request_reply.py`):

| Message | Request code | V2 header `m`(batch) |
| --- | --- | --- |
| plain | `SEND_MESSAGE_V2(310)` | `false` |
| `MessageBatch` | `SEND_BATCH_MESSAGE(320)` | `true` |
| `MSG_TYPE == "reply"` | `SEND_REPLY_MESSAGE_V2(325)` | depends on whether it is a batch |

Two things are easy to confuse: **the order of the checks** puts reply first and batch second, so a batch that
carries the reply attribute still goes to 325; and **the request code and `m` are two different things** — the
broker uses `m` to decide between batch and single-message writing, while the request code only affects how the
server classifies by code. So both must be asserted as a pair; changing only the code and not `m` makes the
batch body be parsed as a single message. On the real-cluster side this is covered by "batch send 3 messages"
plus the consumption-count reconciliation in `verify_live_clean.py`.

## Async send `send_async`

Async send chains three components (regression guard `tests/test_producer_async.py`, real-cluster guard the
first 9 items of `verify_message_types.py`):

| Component | Implementation |
| --- | --- |
| Send thread pool | `ConsumeExecutor(core,max=cpu_count, max_queue_size=async_sender_queue_capacity)`, threads named from `AsyncSenderExecutor_1` |
| Callback thread pool | `_callback_executor`, threads named from `NettyClientPublicExecutor_1`; can be overridden by `client_callback_executor_threads` |
| Switch broker on failure | `_on_send_exception` |

User callbacks **never run on a read thread or a timeout-sweep thread**; that is the only reason those two
pools exist. A caller of `send_async` only enqueues and returns (measured `callerBlockedMs=0`).

Three entry-level criteria:

- Queue full → `MQClientException("executor rejected")`;
- Queueing already consumed the whole budget → `RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout")`,
  and no request is even created;
- For ASYNC `timesTotal` is fixed at 1, and the retry count is read from `retry_times_when_send_async_failed`.

Four details of the retry are the easiest to miss: **the same request object is reused across retries**
(`SendMessageRequestHeaderV2` carries no brokerName, so switching brokers needs no rebuild), but each attempt
**uses a different `opaque`** (reusing it would cross the responses of the two attempts);
`select_one_message_queue(publish, last_broker, False)` avoids the broker that just failed; the timeout uses the
**shared remaining budget** instead of handing out a fresh one; and `timeout <= 0` stops immediately.

Failure classification (`_classify_async_failure`) is not uniform: **when the broker explicitly answered with an
error code it is handed to the callback as is, without switching brokers and retrying**, which differs from the
`retry_response_codes` semantics of synchronous sending; `RemotingSendRequestException`/`RemotingTimeoutException`/other
`RemotingException` are each wrapped in an `MQClientException` (`send request failed` / `wait response timeout, cost=N` /
`unknown reason`) and retried, and only `RemotingTooMuchRequestException` is not retried; while the outer catch
(raised synchronously) takes the raw-exception branch and **does not wrap**. `request()` internally is exactly
this ASYNC path + waiting on the latch, so it is subject to all of the semantics above as well.

Address resolution takes two steps: first look in the publish address table, and if it is not found **refresh the
route once for that topic** and look again. This step is the only routing source for pinned sends (where the
caller passes the `mq` directly) — they do not fetch publish information inside `sendDefaultImpl`, so without this
step the first pinned send would certainly try to connect with an empty address; if it is still missing after the
refresh the callback gets `MQClientException("The broker[x] not exist")`. The ASYNC branch has its own additional
overall gate: hook time, compression and request building are all charged to the budget, and when the budget is
used up no request is issued, the callback gets `RemotingTooMuchRequestException("sendKernelImpl call timeout")`
and there is no retry.

Three behaviours kept deliberately: `send_async` raises synchronously when not `start()`ed; batch messages reuse
the synchronous batch kernel inside the sender pool; `shutdown()` does not wait for in-flight async tasks —
measured (`verify_async_send_live.py` A6) all 36 got a terminal callback while the whole round errored with
`client already shutdown` and nothing landed on the broker, so "shutdown right after sending" loses messages. The
disabled-by-default **semaphore backpressure** has been fully ported (`enable_backpressure_for_async_mode` +
`FairSemaphore` across two dimensions, guarded by `verify_backpressure_live.py`).

## License

Apache-2.0, consistent with upstream RocketMQ.
