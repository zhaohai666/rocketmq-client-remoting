# The Python Implementation of the RocketMQ Remoting Client

English | [中文](README.md)

## Overview

A Python client for the RocketMQ classic remoting protocol: it talks straight to the NameServer
(port 9876) and the Broker (port 10911) — no proxy, no gRPC.
The public API is synchronous; heartbeating, rebalancing, pulling and offset persistence run on
background threads inside the client.
Built on the standard library only (socket / threading / zlib / json) — **no mandatory third-party
runtime dependency**. It targets RocketMQ 4.x and 5.x servers and every capability listed below has
been driven against a real 5.5.1 cluster (see *Live Cluster Verification*).

## Prerequisites

- Python 3.8 or newer (`requires-python = ">=3.8"` in `pyproject.toml`).
- A running server side: NameServer listening on `9876`, Broker listening on `10911`.
- For local trial-and-error, keep `autoCreateTopicEnable=true` on the broker so the first message
  creates the topic. Turn it off in real deployments and create the topic and subscription group
  up front with `DefaultMQAdminExt`.

## Getting Started

```bash
cd python
python3 -m venv .venv
.venv/bin/pip install -e .                     # top-level packages: client / common / remoting

.venv/bin/pip install pytest                   # needed only to run the unit tests
.venv/bin/python -m pytest tests/ -q           # 1210 passed, 4 skipped
.venv/bin/python selfcheck.py                  # selfcheck: 7/7 passed
```

- `pytest` collects 1214 cases and all of them run offline — no cluster needed. The 4 skips all come
  from `tests/test_protocol_alignment.py` (an optional protocol-constant cross-check, off by default).
- `selfcheck.py` runs 7 encode/decode round-trips of the protocol itself (JSON header / binary header /
  V2 short-field header / 17-segment storage format / long-topic V2 magic / 6-segment batch format /
  ACL signature injection) and needs no cluster. `python -m selfcheck` is equivalent to
  `python selfcheck.py`.

## Examples

### Producer

Normal message:

```python
from client.producer import DefaultMQProducer
from common.message import Message

producer = DefaultMQProducer("PID_DEMO")
producer.set_namesrv_addr("127.0.0.1:9876")
producer.start()

result = producer.send(Message("TopicTest", b"hello", tags="TagA", keys="order-1"))
print(result.send_status, result.msg_id, result.queue_offset)
producer.shutdown()
```

Ordered message (the same `arg` always lands on the same queue):

```python
from client.producer import SelectMessageQueueByHash

result = producer.send_by_selector(
    Message("TopicTest", b"part-1"), SelectMessageQueueByHash(), "order-1", 3000)
```

Send to one specific queue: `producer.send(msg, mq=MessageQueue("TopicTest", "broker-a", 0))`.

Delayed message and recall:

```python
msg = Message("TopicTest", b"deliver-later")
msg.set_delay_time_level(3)                      # delay level 3
result = producer.send(msg)
handle = result.recall_handle                    # only delayed messages carry a recall handle

uniq_key = producer.recall_message("TopicTest", handle)   # request code 370, returns the recalled uniqKey
```

Transactional message (two-phase plus broker check-back):

```python
from client.producer import TransactionMQProducer, TransactionListener, LocalTransactionState

class TxListener(TransactionListener):
    def execute_local_transaction(self, msg, arg):
        return LocalTransactionState.COMMIT_MESSAGE
    def check_local_transaction(self, msg):      # broker asks about the half message
        return LocalTransactionState.COMMIT_MESSAGE

tx = TransactionMQProducer("PID_TX")
tx.set_namesrv_addr("127.0.0.1:9876")
tx.set_transaction_listener(TxListener())
tx.start()
result = tx.send_message_in_transaction(Message("TopicTest", b"half"))
print(result.send_status, result.get_local_transaction_state())
tx.shutdown()
```

Batch message (one topic, no delay, 4MB per batch):

```python
from common.message import MessageBatch

batch = MessageBatch.generate_from_list(
    [Message("TopicTest", ("b-%d" % i).encode("utf-8")) for i in range(3)])
result = producer.send(batch)                    # a plain list works too: producer.send([...])
```

Oneway and async:

```python
from client.producer import SendCallback

producer.send_oneway(Message("TopicTest", b"no-result"))

class CB(SendCallback):
    def on_success(self, send_result):
        print(send_result.msg_id)
    def on_exception(self, e):
        print("send failed:", e)

producer.send_async(Message("TopicTest", b"async"), CB(), 5000)   # returns immediately
```

Request-Reply (the caller blocks for the answer; request codes 325 / 326):

```python
reply = producer.request(Message("TopicTest", b"ping"), 3000)
print(bytes(reply.body))

# async form: producer.request_async(msg, RequestCallback(), 3000)
```

The responder sends its answer back from inside its own push listener:

```python
from client.request_reply import create_reply_message

reply_msg = create_reply_message(request_msg, b"pong")
producer.send(reply_msg)                         # goes out as SEND_REPLY_MESSAGE_V2(325)
```

### Push Consumer

Concurrently:

```python
from client.consumer import DefaultMQPushConsumer, MessageListenerConcurrently
from client.consumer_result import ConsumeConcurrentlyStatus

class Listener(MessageListenerConcurrently):
    def consume_message(self, msgs, context):
        for msg in msgs:
            print(msg.msg_id, msg.topic, bytes(msg.body))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

consumer = DefaultMQPushConsumer("GID_DEMO")
consumer.set_namesrv_addr("127.0.0.1:9876")
consumer.subscribe("TopicTest", "TagA || TagB")
consumer.set_message_listener(Listener())
consumer.start()
# ... consumption happens on the client's background threads
consumer.shutdown()
```

Orderly (one queue is consumed strictly in order; returning
`SUSPEND_CURRENT_QUEUE_A_MOMENT` pauses and redelivers):

```python
from client.consumer import MessageListenerOrderly
from client.consumer_result import ConsumeOrderlyStatus

class OrderlyListener(MessageListenerOrderly):
    def consume_message(self, msgs, context):
        return ConsumeOrderlyStatus.SUCCESS
```

Broadcasting (offsets stay local, no rebalance involved):

```python
from remoting.protocol.heartbeat import MessageModel, ConsumeFromWhere

consumer.set_message_model(MessageModel.BROADCASTING)
consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
```

Other entry points:

```python
from client.consumer import (AllocateMessageQueueConsistentHash, MessageSelector)
from common.subscription_data import ExpressionType

consumer.subscribe_with_selector(
    "TopicTest", MessageSelector(ExpressionType.SQL92, "price > 100"))
consumer.set_allocate_message_queue_strategy(AllocateMessageQueueConsistentHash())
consumer.set_enable_trace(True)          # message tracing
consumer.pop_mode = True                 # switch the push consumer onto the POP loop
```

### Pull Consumer

You drive the cursor and commit the offsets yourself:

```python
import time
from client.consumer import DefaultMQPullConsumer
from client.consumer_result import PullStatus

consumer = DefaultMQPullConsumer("GID_PULL")
consumer.set_namesrv_addr("127.0.0.1:9876")
consumer.start()

for mq in consumer.fetch_subscribe_message_queues("TopicTest"):
    offset = consumer.fetch_consume_offset(mq) or 0
    result = consumer.pull(mq, "TagA", offset, 32, 5000)   # last argument is the timeout in ms
    if result.status == PullStatus.FOUND:
        for msg in result.msg_found_list:
            print(msg.msg_id, bytes(msg.body))
        consumer.update_consume_offset(mq, result.next_begin_offset)

# long polling: the broker holds the request until a message arrives
result = consumer.pull_block_if_not_found(mq, "TagA", offset, 32)

print(consumer.search_offset(mq, int(time.time() * 1000)))
consumer.shutdown()
```

`fetch_subscribe_message_queues(topic)` returns every queue of the topic,
`fetch_message_queues_in_balance(topic)` returns only the share this instance should own.

### Lite Pull Consumer

```python
from client.consumer import DefaultLitePullConsumer, TopicMessageQueueChangeListener

lite = DefaultLitePullConsumer("GID_LITE")
lite.set_namesrv_addr("127.0.0.1:9876")
lite.subscribe("TopicTest", "TagA")
lite.set_auto_commit(False)
lite.start()

msgs = lite.poll(timeout=1000)
lite.commit()

# pin specific queues and cursors
queues = lite.fetch_message_queues("TopicTest")
lite.assign([queues[0]])
lite.seek(queues[0], 0)                          # seek_to_begin / seek_to_end also available
lite.poll(timeout=1000)
lite.pause([queues[0]])
lite.resume([queues[0]])
lite.shutdown()
```

Watching a topic's queue set change (fires after a scale-out/scale-in, driven by a periodic compare):

```python
class QueueChangeListener(TopicMessageQueueChangeListener):
    def on_changed(self, topic, message_queues):
        print("queues changed:", topic, len(message_queues))

lite.register_topic_message_queue_change_listener("TopicTest", QueueChangeListener())
```

### Admin

```python
import time
from client.admin import DefaultMQAdminExt

admin = DefaultMQAdminExt()
admin.set_namesrv_addr("127.0.0.1:9876")
admin.start()

admin.create_topic("DefaultCluster", "TopicTest", 4)
print(admin.fetch_broker_cluster_info())
print(admin.examine_topic_route("TopicTest"))
print(admin.examine_topic_stats("TopicTest"))
print(admin.examine_consume_stats_group("GID_DEMO"))
print(admin.query_consume_time_span("TopicTest", "GID_DEMO"))
admin.reset_offset_by_timestamp("TopicTest", "GID_DEMO", int(time.time() * 1000) - 3600_000)
print(admin.view_message("TopicTest", "<msgId>"))
admin.shutdown()
```

Consumer-side operations live on the same object: `examine_consumer_connection_info(group)`,
`examine_consumer_running_info(group, client_id)` (request code 307) and its alias
`get_consumer_running_info`.

### ACL

```python
from remoting.rpchook import AclClientRPCHook, SessionCredentials
from client.producer import DefaultMQProducer

producer = DefaultMQProducer(
    "PID_DEMO",
    rpc_hook=AclClientRPCHook(SessionCredentials("AccessKey", "SecretKey")))
```

For an STS token, pass it as the third argument: `SessionCredentials(ak, sk, security_token)`.
The signed content is the concatenation of every extFields **value** in key order (including the
`AccessKey` / `SecurityToken` just written, excluding `Signature` itself) followed by the request body;
the algorithm is HmacSHA1 with standard Base64.

### Namespace

Two independent mechanisms that can be combined:

```python
producer = DefaultMQProducer("PID_DEMO", namespace="ns1")   # local resource prefix %%ns%%res
producer.set_namespace_v2("ns2")     # server-side namespace: every request carries nsd=true / ns=ns2
```

`namespace` is a constructor argument on all five facades (producer / the three consumers / admin) and
is wrapped and unwrapped across send, receive, heartbeat and offset calls; `set_namespace_v2` exists on
all five and is read per request rather than snapshotted at start-up. `DefaultLitePullConsumer` also
has `set_namespace` for a late change. Both `ns` and `ReqT` are part of the ACL signed content, and the
hook order is fixed: Namespace → Stream → ACL.

### Compression

```python
from common.sysflag import MessageSysFlag

producer.set_compress_msg_body_over_howmuch(1024)          # compress bodies >= 1KB (default 4096)
producer.set_compress_level(5)
producer.set_compress_type(MessageSysFlag.ZSTD_TYPE)       # ZLIB_TYPE / LZ4_TYPE / ZSTD_TYPE
```

zlib comes from the standard library; LZ4 needs `pip install lz4` and ZSTD needs
`pip install zstandard`. When the package is missing the client **raises instead of passing compressed
bytes through as if they were the payload**. On the consumer side a `COMPRESSED_FLAG` is detected, the
body decompressed and the flag cleared. Batch messages are never compressed.

### TLS

```python
producer = DefaultMQProducer("PID_DEMO", tls_enable=True,
                             tls_options={"caCert": "/path/ca.pem"})
```

`tls_options` additionally accepts `clientCert` / `clientKey` (mTLS) and `serverName`
(SNI / hostname override). When `tls_enable` is not passed explicitly the client reads the
`ROCKETMQ_TLS_ENABLE=1` environment variable. Producer, all three consumers and the admin run on the
same transport.

## Features and Status

| Capability | Status |
| --- | --- |
| Normal / ordered (selector) / pinned-queue / delayed / two-phase transactional / batch / oneway / async (with fair-semaphore backpressure) | ✅ |
| Request-Reply (325 send + 326 push-back, synchronous and asynchronous) | ✅ |
| Recalling a delayed message: `recall_message` (370, handle from `SendResult.recall_handle`) | ✅ |
| Push consumer: concurrent / orderly listeners, broadcasting, five flow-control thresholds, offset correction and reset, stalled-loop recovery, expired-message sweep | ✅ |
| Pull consumer: manual pull + long polling + manual commit + offset lookup by timestamp | ✅ |
| Lite pull consumer: `poll` / `assign` / `seek` / `pause` / `commit`, three offset tables (pull / consumed / committed cursors), topic queue-change listener | ✅ |
| POP consumption loop (`pop_mode`, with ack / renew / orderly POP) | ✅ |
| Queue allocation strategies: AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY | ✅ |
| Two namespace schemes: local `namespace` prefix and server-side `namespace_v2` | ✅ |
| ACL signing (incl. SecurityToken): `AclClientRPCHook` / `NamespaceRpcHook` / `StreamTypeRPCHook` | ✅ |
| Message tracing (Pub / SubBefore / SubAfter / EndTransaction / Recall encoding + async dispatch) | ✅ |
| Hooks: Send / Consume / EndTransaction / CheckForbidden / FilterMessage | ✅ |
| Client statistics (pull TPS and RT, consume RT, consume success/failure TPS) and `ConsumerRunningInfo` (307) | ✅ |
| Three compression backends: zlib (standard library) / LZ4 / ZSTD | ✅ |
| TLS (mTLS and strict CA verification) | ✅ |
| Dynamic NameServer discovery (`DefaultTopAddressing` + `ROCKETMQ_NAMESRV_DOMAIN`) | ✅ |
| Send-latency fault avoidance (`set_send_latency_fault_enable`) and master/slave switching | ✅ |
| Admin (`DefaultMQAdminExt`): topic and subscription-group config, offset query and reset, connection and runtime info, message lookup, broker config and cleanup | ✅ |
| Both serializations (JSON and RocketMQ binary), 17-segment storage format and 6-segment batch format | ✅ |

## Client Logging

`rocketmq_logging.py` bridges the internal logger onto standard `logging`. Log files default to
**`<current working directory>/logs/rocketmqlogs/rocketmq_py_client.log`** and rotate daily, with
backups named `rocketmq_py_client.log.YYYY-MM-DD`. If the host application has already configured
Python logging (the root logger has a handler), this module adds no handlers at all.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_DIR` | `<cwd>/logs/rocketmqlogs` | Log directory; point it anywhere else (for example `$HOME/logs/rocketmqlogs`) |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_py_client.log` | Log file name |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | Level; both `WARN`/`WARNING` and `TRACE`/`DEBUG` spellings are accepted, anything unknown falls back to `INFO` |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | Number of daily backups kept |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | `true` | Set to `false` to silence stderr output |

An unwritable directory produces a single warning and degrades to stderr; it never blocks start-up.
Regression guard: `tests/test_logging_config.py`.

## Live Cluster Verification

The unit tests are all offline mocks. The scripts below hit a real cluster and prove that the frames
are actually accepted by the broker and NameServer.

Prerequisites: NameServer on `9876` plus Broker on `10911` with `autoCreateTopicEnable=true`.

```bash
.venv/bin/python verify_pull_live.py 127.0.0.1:9876
```

The address is the optional first argument (default `127.0.0.1:9876`). Each script prints
`[PASS]/[FAIL]` per check and **exits 0 only when everything passed, 1 on the first failure**, so it
drops straight into CI.

> ⚠ Ordering rule: **start the consumer first, then send.** A brand-new subscription group begins at
> the tail of the queue and the first rebalance waits for the heartbeat registration, so sending
> before start-up reliably loses the first batch.

There are 41 `verify_*_live.py` scripts; the first three rows are the companion scripts that are not
named `_live`. One line each on what they prove:

| Script | What it proves |
| --- | --- |
| `verify_message_types.py` | End-to-end over the 7 message capabilities: sync / async / oneway / batch / transactional / pinned / request-reply (hits the cluster) |
| `verify_live_clean.py` | Send-and-receive reconciliation on a brand-new topic, group and store directory, including batch counts |
| `verify_acl_signature_vectors.py` | Fixed-vector comparison of the ACL signed content and signature (offline, no cluster) |
| `verify_ack_index_live.py` | Partial ack via `ackIndex` and whole-batch failure redelivery in concurrent consumption |
| `verify_acl_live.py` | Against an authenticated broker: valid credentials accepted, missing or wrong ones rejected |
| `verify_admin_live.py` | The whole admin path plus `sendMessageBack` redelivery and offset reset |
| `verify_admin_batch_live.py` | Real round-trips for batch / static-topic / read-forbidden / half-message check / order-config admin methods |
| `verify_async_send_live.py` | The async send kernel: thread accounting, concurrency, pinned queues, hooks, batches, shutdown |
| `verify_backpressure_live.py` | The two fair semaphores behind async-send backpressure |
| `verify_clean_expired_msg_live.py` | The expired-message escape hatch while a listener is hung (~4 minutes; deletes its own topic) |
| `verify_compression_live.py` | Auto-compression round trip, the broker really stores the compressed body, Message reuse (sub-commands `selftest|send|recv`) |
| `verify_consumer_heartbeat_slave_live.py` | Consumer heartbeats fan out to slave brokers (needs a slave in the cluster) |
| `verify_correct_tags_offset_live.py` | An empty response still advances the committed offset (correctTagsOffset) |
| `verify_fail_fast_live.py` | In-flight requests get a terminal state immediately once the broker really dies (stops and restarts a broker) |
| `verify_flow_control_live.py` | The five pre-pull flow-control thresholds plus the start-up numeric gates |
| `verify_hook_live.py` | CheckForbidden / FilterMessage hooks actually intercepting |
| `verify_interval_live.py` | Route-refresh and offset-persist periods take effect as configured (two instances, two periods) |
| `verify_latency_live.py` | Send-latency fault avoidance: isolation window, broker switch, recovery |
| `verify_lite_pull_code_live.py` | Lite request code 361 and the broker-side lite switch (flipped at runtime and restored on exit) |
| `verify_lite_pull_cursor_live.py` | The lite pull cursor follows `nextBeginOffset`, incl. NO_MATCHED_MSG and out-of-range self-healing |
| `verify_lite_pull_live.py` | The whole lite-pull path: rebalance, assign+seek, allocation strategies, three offset tables |
| `verify_lite_topic_queue_change_live.py` | `on_changed` fires after a topic queue scale-out/scale-in |
| `verify_offset_illegal_live.py` | The `OFFSET_ILLEGAL` branch: drop the queue and persist the corrected offset immediately |
| `verify_orderly_reconsume_live.py` | The orderly redelivery gate and explicit COMMIT/ROLLBACK |
| `verify_pinned_guard_live.py` | The pinned-send topic guard: real routes not falsely rejected, wrong topic rejected, oneway unguarded |
| `verify_pop_live.py` | The POP protocol surface: POP / ACK / invisible-time renew |
| `verify_pop_consumer_live.py` | The POP consumption loop (a push consumer driven by POP), incl. 307 statistics |
| `verify_producer_unregister_live.py` | Producer shutdown really sends `UNREGISTER_CLIENT`(35) |
| `verify_publish_route_master_live.py` | Publish routing skips brokers without a master (stops a master once) |
| `verify_pull_consumer_heartbeat_live.py` | The pull consumer's 203/38/35 heartbeats (deletes the topic it created) |
| `verify_pull_expired_live.py` | Recovery from a stalled pull loop (120s threshold) |
| `verify_pull_live.py` | `DefaultMQPullConsumer` pulling, offset commit and offset lookup by timestamp |
| `verify_recall_live.py` | Recalling a delayed message via `recallMessage`(370): the handle comes from the broker and the message really stops |
| `verify_redelivery_live.py` | Redelivery / dead-letter terminal state / partial ack / stalled-loop recovery / orderly dead letter |
| `verify_request_reply_live.py` | Full request-reply: the 325 answer plus timeout and answer-creation error codes |
| `verify_reset_offset_live.py` | 220 consumer offset reset: both body shapes plus in-flight invalidation |
| `verify_send_header_live.py` | The on-the-wire shape of the `c` / `d` / `n` send-header fields |
| `verify_sql92_live.py` | SQL92 filtering + `CHECK_CLIENT_CONFIG`(46) (needs broker `enablePropertyFilter=true`) |
| `verify_subscribe_live.py` | Late subscription + the immediate heartbeat round |
| `verify_tls_live.py` | The entire client path over TLS (`--leg plain|ca_verify|mtls`) |
| `verify_trace_live.py` | Message tracing end to end (needs broker `traceTopicEnable=true`) |
| `verify_transaction_live.py` | Two-phase transactions plus the broker check-back |
| `verify_unit_config_live.py` | The observable broker-side effects of `unitName` / `unitMode` / stream |
| `verify_validators_live.py` | Illegal names fail fast locally while legal names still send and receive normally |

## Repository Layout

`client/`, `common/` and `remoting/` sit directly under `python/` as three **top-level packages**: put
`python/` on `sys.path` and import `from client.producer import DefaultMQProducer`
(`tests/conftest.py` already does this).

```
python/
├── common/                 message model and constants
│   ├── message.py              Message / MessageExt / MessageBatch / MessageQueue
│   ├── message_decoder.py      17-segment storage + 6-segment batch codecs, three compression backends
│   ├── message_const.py        MessageConst property keys
│   ├── sysflag.py              MessageSysFlag / PullSysFlag / PermName / compression type bits
│   ├── boundary_type.py        boundary semantics of offset-by-timestamp lookups (LOWER / UPPER)
│   ├── recall_message_handle.py recall handle v1 for delayed messages (base64url + 5 segments)
│   ├── subscription_data.py    SubscriptionData / FilterAPI / ExpressionType
│   ├── topic_config.py / mix_all.py / util_all.py / message_accessor.py
│   ├── message_type.py / message_client_id_setter.py / topic_validator.py
├── remoting/               transport layer (plain sockets + threads)
│   ├── client.py               RemotingClient: sync / async / oneway, half-packet reassembly, TLS
│   ├── rpchook.py              RPCHook / AclClientRPCHook / NamespaceRpcHook / StreamTypeRPCHook
│   ├── exception.py
│   └── protocol/
│       ├── remoting_command.py   frame codec (totalLen | headerLen+type | header | body)
│       ├── serialize.py          JSON / RocketMQ binary + tolerant fastjson parser
│       ├── codes.py              RequestCode / ResponseCode / LanguageCode / SerializeType
│       ├── headers.py            CommandCustomHeader family (incl. V2 short names a..n)
│       ├── body.py / admin_body.py / route.py / heartbeat.py / subscription.py
│       ├── extra_info.py / namespace_util.py
├── client/                 the user-facing API
│   ├── producer.py             DefaultMQProducer / TransactionMQProducer / selectors / transaction listener
│   ├── consumer.py             Push, Pull and LitePull consumers + six allocation strategies
│   ├── admin.py                DefaultMQAdminExt
│   ├── mq_client.py            MQClientInstance: routing, heartbeat, rebalance, scheduled tasks
│   ├── request_reply.py        request-reply future pool and create_reply_message
│   ├── produce_accumulator.py  automatic batching
│   ├── backpressure.py / consume_executor.py / latency.py / consumer_stats.py / metrics.py
│   ├── hook.py / trace.py / trace_hook.py / trace_dispatcher.py / trace_context.py
│   ├── top_addressing.py       dynamic NameServer discovery
│   ├── validators.py / send_result.py / consumer_result.py / exception.py
├── rocketmq_logging.py     logging bridge
├── selfcheck.py            cluster-free protocol round trips (7 checks)
├── __main__.py             command entry (python -m selfcheck)
├── tests/                  1214 offline cases (conftest.py puts python/ on sys.path)
├── verify_*_live.py        live-cluster scripts (see Live Cluster Verification)
├── integration_live_test.py / ns_failover_repro.py / go_*_check.py  helper scripts
├── pyproject.toml          no runtime dependencies; requires-python >= 3.8
└── README.md / README.en.md
```

## License

Apache-2.0.
