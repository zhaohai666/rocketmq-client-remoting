# The Node.js/TypeScript Implementation of the RocketMQ Remoting Client

> English | [中文](README.md)

## Overview

A Node.js/TypeScript client for the Apache RocketMQ **classic remoting protocol**: it talks to the NameServer and the Broker directly to send and receive messages and manage offsets — no proxy layer required.

- TypeScript sources run directly via `node --experimental-strip-types` — **no build step**; ESM imports carry explicit `.ts` extensions.
- **Zero third-party dependencies** — only `node:` built-ins.
- Requires Node ≥ 22 (the `engines` field in `package.json`; everything here was measured on v24.14.0).
- All live cases were verified against a real RocketMQ **5.5.1** cluster (NameServer 9876 + Broker 10911).

## Prerequisites

- Node.js ≥ 22 (`.ts` runs directly through `--experimental-strip-types`).
- A reachable RocketMQ cluster: NameServer `9876`, Broker `10911` (the offline smoke suites need no cluster).
- No `npm install` — there are no third-party dependencies.

## Getting Started

```bash
git clone <this repo> && cd rocketmq-client-remoting/nodeJs
node --experimental-strip-types selfcheck.ts
```

`selfcheck.ts` loads all **59** modules under `src/` and then runs the **11** offline smoke suites; measured **ALL GREEN** on 2026-10-10. Run a single suite with `node --experimental-strip-types test/<suite>.ts`; measured results:

| Suite | Measured |
| --- | --- |
| `test/smoke.ts` (frame / transport / ACL signing) | 6 check groups passed |
| `test/producer_smoke.ts` | 4 check groups passed |
| `test/consumer_smoke.ts` | 30 passed / 0 failed |
| `test/stats_smoke.ts` | 14 passed / 0 failed |
| `test/compat_contract_smoke.ts` (pinned wire-format / behavior surface) | 10 check groups passed |
| `test/pop_smoke.ts` | 11 check groups passed |
| `test/fixes2_smoke.ts` | 38 checks passed |
| `test/namespace_rpc_smoke.ts` | 7 check groups passed |
| `test/ns_failover_smoke.ts` | 12 checks passed |
| `test/lite_topic_queue_change_smoke.ts` | 25 passed / 0 failed |
| `test/pull_balance_view_smoke.ts` | 14 passed / 0 failed |

Run an example against a live cluster:

```bash
node --experimental-strip-types examples/live_producer.ts --ns 127.0.0.1:9876
```

Examples print `PASS/FAIL` lines and exit 0 when every check passed (see "Live Cluster Verification").

## Examples

Every client shares one lifecycle: construct → `setNamesrvAddr` → `start()` → send/receive → `shutdown()`.

### Normal Message / Producer

```ts
import { DefaultMQProducer, SelectMessageQueueByHash } from './src/client/producer.ts';
import { Message, MessageBatch } from './src/common/message.ts';

const producer = new DefaultMQProducer('GID_TEST');
producer.setNamesrvAddr('127.0.0.1:9876');
await producer.start();

// synchronous send
const r = await producer.send(new Message('TopicTest', Buffer.from('hello'), 'TagA', 'key1'));
console.log(r.msgId, r.messageQueue.getQueueId());

// batch (folded into one RPC)
await producer.send(MessageBatch.generateFromList([
  new Message('TopicTest', Buffer.from('b1')), new Message('TopicTest', Buffer.from('b2')),
]));

// oneway / async
await producer.sendOneway(new Message('TopicTest', Buffer.from('fire')));
await producer.sendAsync(new Message('TopicTest', Buffer.from('hi')),
  (result, err) => console.log(result?.msgId, err?.message));

// ordered: pin a queue by hashing the business key
await producer.sendBySelector(new Message('TopicTest', Buffer.from('o1')),
  new SelectMessageQueueByHash(), 'orderNo-1');

// delayed / timed
const d = new Message('TopicTest', Buffer.from('later'));
d.setDelayTimeLevel(3);            // 4.x delay level
d.setDelayTimeSec(30);             // 5.x timer: TIMER_DELAY_SEC
d.setDelayTimeMs(1500);            // TIMER_DELAY_MS
d.setDeliverTimeMs(1699999999000); // TIMER_DELIVER_MS

producer.shutdown();
```

### Transactional Message / Producer

```ts
import { TransactionMQProducer, TransactionListener, LocalTransactionState } from './src/client/producer.ts';
import { Message } from './src/common/message.ts';

class Listener extends TransactionListener {
  executeLocalTransaction(msg: any, arg: any) { return LocalTransactionState.COMMIT_MESSAGE; }
  checkLocalTransaction(msg: any) { return LocalTransactionState.COMMIT_MESSAGE; } // broker check-back
}
const tx = new TransactionMQProducer('GID_TX', new Listener());
tx.setNamesrvAddr('127.0.0.1:9876');
await tx.start();
const half = new Message('TopicTest', Buffer.from('tx'));
await tx.sendMessageInTransaction(half, null); // half message → local transaction → COMMIT/ROLLBACK
tx.shutdown();
```

### Request-Reply / Recall / Producer

```ts
const answer = await producer.request(new Message('TopicTest', Buffer.from('ping'))); // the reply Message
// responder side, inside the consume callback: await producer.reply(requestMsg, Buffer.from('pong'));
await producer.recallMessage(recallHandle); // recall a delayed/timed message; handle comes from the send result
```

### Concurrently / PushConsumer

```ts
import { DefaultMQPushConsumer } from './src/client/consumer.ts';
import { ConsumeConcurrentlyStatus } from './src/client/consumer_result.ts';

const consumer = new DefaultMQPushConsumer('GID_TEST');
consumer.setNamesrvAddr('127.0.0.1:9876');
consumer.subscribe('TopicTest', '*');
consumer.registerMessageListenerConcurrently((msgs: any[]) => {
  for (const m of msgs) console.log(m.getBody()?.toString());
  return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // or RECONSUME_LATER for redelivery
});
await consumer.start();
```

### Orderly / PushConsumer

```ts
import { ConsumeOrderlyStatus } from './src/client/consumer_result.ts';
consumer.registerMessageListenerOrderly((msgs: any[]) =>
  ConsumeOrderlyStatus.SUCCESS); // or SUSPEND_CURRENT_QUEUE_A_MOMENT
```

### Broadcasting / PushConsumer

```ts
import { MessageModel } from './src/remoting/heartbeat.ts';
consumer.setMessageModel(MessageModel.BROADCASTING); // default is CLUSTERING; each instance stores its own offset
```

### PullConsumer

```ts
import { DefaultMQPullConsumer } from './src/client/pull_consumer.ts';
import { PullStatus } from './src/client/consumer_result.ts';

const puller = new DefaultMQPullConsumer('GID_PULL');
puller.setNamesrvAddr('127.0.0.1:9876');
await puller.start();
for (const mq of await puller.fetchMessageQueuesInBalance('TopicTest')) {
  const offset = await puller.fetchConsumeOffset(mq, false);
  const res = await puller.pull(mq, '*', offset, 32);        // short poll
  // await puller.pullBlockIfNotFound(mq, '*', offset, 32)   // long poll
  if (res.pullStatus === PullStatus.FOUND) puller.updateConsumeOffset(mq, res.nextBeginOffset);
}
await puller.persistConsumeOffset();
puller.shutdown();
```

### LitePullConsumer (with the topic queue-change listener)

```ts
import { DefaultLitePullConsumer } from './src/client/lite_pull_consumer.ts';

const lite = new DefaultLitePullConsumer('GID_LITE');
lite.setNamesrvAddr('127.0.0.1:9876');
lite.subscribe('TopicTest', '*');   // auto-rebalance; for a fixed set use lite.assign([mq])
await lite.start();

const msgs = await lite.poll(3000);
lite.seek(mq, 0);
await lite.seekToBegin(mq);         // reads the broker minOffset first, then seeks
await lite.seekToEnd(mq);           // reads the maxOffset first, then seeks
await lite.commitSync();

// Queue-change listener: the route is re-queried every 30s by default;
// setTopicMetadataCheckIntervalMillis(ms) tunes it (floor 1s). Listeners are keyed by
// the BARE topic name; namespaceV2 never prefixes the local topic name — it rides on the
// wire as the nsd/ns request fields. That is this port's design; the two never interfere.
lite.setTopicMetadataCheckIntervalMillis(1000);
await lite.registerTopicMessageQueueChangeListener('TopicTest', {
  onChanged(topic, messageQueues) { console.log(topic, messageQueues.map(q => q.getQueueId())); },
});
lite.shutdown();
```

### Admin

```ts
import { DefaultMQAdminExt } from './src/client/admin.ts';

const admin = new DefaultMQAdminExt();
admin.setNamesrvAddr('127.0.0.1:9876');
admin.start();
await admin.fetchAllTopicList();
await admin.createTopic('default', 'NewTopic', 8);
await admin.examineBrokerClusterInfo();
await admin.examineConsumeStats('GID_TEST');
await admin.resetOffsetByTimestamp('TopicTest', 'GID_TEST', Date.now(), true);
await admin.queryMessage('127.0.0.1:10911', 'TopicTest', 'key1');     // query by key (12)
await admin.viewMessage('TopicTest', msgId);                           // view directly at the address embedded in the msgId (33)
await admin.examineConsumerOffset('GID_TEST', mq);
admin.shutdown();
```

### ACL

```ts
import { AclRPCHook } from './src/remoting/acl.ts';

producer.rpcHook = new AclRPCHook(accessKey, secretKey); // per-request HMAC-SHA1 + Base64 signature
```

### Namespace (two mechanisms)

```ts
producer.setNamespace('MyNS');        // client-side prefix: resources become %%MyNS%%<resource>
producer.setNamespaceV2('RMQ_INST');  // server-side namespace: resource names stay as-is; every
                                      // request is stamped live with nsd=true / ns=RMQ_INST (hook
                                      // order Namespace → Stream → ACL, so ns lands inside the ACL signature)
```

`setNamespaceV2` is available on every client (Producer / Push / Pull / LitePull / Admin) and the value is read live per request.

### Compression (zlib / LZ4 / ZSTD)

```ts
producer.setCompressType('ZSTD');             // 'ZLIB' | 'LZ4' | 'ZSTD'
producer.setCompressMsgBodyOverHowmuch(8192); // compress only above the threshold, default 4096
await producer.send(bigMsg);                  // the receiving side decompresses automatically
```

- LZ4 uses the standard **Frame** format on the wire (frame header + xxh32 checksum), complete in both directions.
- ZSTD prefers the zstd binding shipped inside `node:zlib` (available on Node ≥ 23.8; this port feature-detects it). On runtimes without the binding it falls back to the built-in codec: the sender emits legal zstd frames made of Raw/RLE blocks only, the reader accepts Raw/RLE frames only, and input it cannot decode **throws explicitly** — a compressed stream is never passed through as the plain body.
- `MessageBatch` is never compressed (the batch body is the aggregate envelope the broker splits apart).

### TLS

```ts
producer.setTlsEnable(true);  // or the environment variable ROCKETMQ_TLS_ENABLE=1
producer.setTlsOptions({ caCert, clientCert, clientKey, serverName }); // null = test-mode (trust self-signed)
```

Supported by Producer / PushConsumer / LitePullConsumer; `tlsEnable` is process-wide — the NameServer connection goes through TLS as well.

## Features and Status

- ✅ Protocol layer: `RemotingCommand` frame, dual JSON + RocketMQ binary serialization, V2 short-key headers, fault-tolerant parsing, 17-segment/6-segment message encode/decode
- ✅ Transport layer: lazy long-connection reuse, sync/async/oneway, half-packet reassembly, opaque matching, resend-once on GO_AWAY, in-flight requests marked dead on disconnect
- ✅ Producer: sync / batch / oneway / async (fair-semaphore back-pressure) / queue selector / ordered / delayed and 5.x timed / transaction two-phase + check-back / Request-Reply (326 receive-side closed loop) / recallMessage / VIP channel (port-2)
- ✅ PushConsumer: concurrently / orderly / broadcasting, long polling, offset persistence, flow-control thresholds, 307/220/221 responses, RETRY restoration, suspend/resume, online core-thread resizing
- ✅ PullConsumer / LitePullConsumer: short/long polling, assign + seek + commitSync, auto-commit, topic queue-change listener
- ✅ POP consumption: POP(200050)/ACK(200051)/CHANGE_INVISIBLE(200053)/BATCH_ACK(200151)/SET_MESSAGE_REQUEST_MODE(401), POP_CK dual-path reconstruction, ack-debt flow control, back-off table
- ✅ Admin: topic CRUD, cluster/runtime info, subscription groups, consume statistics, connection queries, offset reset and read, KV config, NameServer config (318/319), message query/view, consumeMessageDirectly
- ✅ ACL (HMAC-SHA1 + Base64) and the RPC hook chain (Namespace → Stream → user hook)
- ✅ Two namespace mechanisms: `namespace` prefix + `namespaceV2` wire stamping; clientId `<ip>@<instanceName>[@<unitName>][@STREAM]`
- ✅ Six queue allocation strategies: AVG / AVG_BY_CIRCLE / CONFIG / MACHINE_ROOM / MACHINE_ROOM_NEARBY / CONSISTENT_HASH
- ✅ Message tracing (encode/decode + async dispatcher + send/receive hooks), consume statistics (differential window + 307 statusTable), W3C traceparent
- ✅ Three compression types: zlib / LZ4 (Frame) / ZSTD; TLS opt-in
- ✅ NameServer failover (client-side address switching and route re-fetch)

## Client Logging

Configured purely by environment variables — no build step:

| Variable | Default | Notes |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR` |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` | log directory |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_node_client.log` | file name; a value containing a separator is treated as a full path; `''`/`OFF`/`NONE` disables the file |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | (empty) | any non-empty value → stdout only, no file |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | 64MB | rotation threshold |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | 10 | backup window, `.1`..`.10` shifted up |

This port's rule: every line always goes to stdout/stderr; by default it is **also** written to the file, and `ROCKETMQ_CLIENT_LOG_USE_STDOUT` or `ROCKETMQ_CLIENT_LOG_FILE=OFF` turns the file off. Rotation is size-triggered; the file name carries the port's own tag (`rocketmq_node_client.log`) so concurrent clients on one machine never interleave into each other's log.

## Live Cluster Verification

Against an already-running cluster, execute a single example (the address comes from `--ns`, falling back to `NAMESRV_ADDR` or `127.0.0.1:9876`):

```bash
node --experimental-strip-types examples/live_lite_queue_change.ts --ns 127.0.0.1:9876
# measured 2026-10-10: PASS=10 FAIL=0, exit code 0
```

Or use the umbrella script (it brings up a local 5.5.1 cluster only when one is not ready, and stops only what it started):

```bash
bash scripts/run_node_live.sh <target> [namesrv]
# targets: producer|consumer|pull|lite_pull|admin|pop|request_reply|admin_ns|acl|tls|check_config|fixes2|slave
```

Exit-code convention: all example checks pass → 0; any FAIL → non-zero; the wrapper forwards it unchanged, and argument/environment errors → 2.

The examples:

| File | One-liner |
| --- | --- |
| `examples/live_producer.ts` | full send chain: sync/batch/oneway/async/selector/transaction COMMIT + broker check-back |
| `examples/live_consumer.ts` | push consume read-back: start the consumer first, then send; every message exactly-once |
| `examples/live_pull.ts` | PullConsumer short polling + manual offset bookkeeping + sendMessageBack + persistence |
| `examples/live_lite_pull.ts` | LitePull subscribe+poll, seek backwards, commitSync |
| `examples/live_lite_queue_change.ts` | queue-change listener: scale-out 2→4 / scale-in 4→2 / unknown topic errors / polling floor |
| `examples/live_pop.ts` | POP consumption end-to-end: pop → ack → invisible-time renewal |
| `examples/live_request_reply.ts` | 326 Request-Reply: requester asks, responder replies, closed loop verified |
| `examples/live_admin.ts` | admin: topic CRUD/cluster/runtime/subscription groups/connections/offsets/Properties config |
| `examples/live_admin_ns.ts` | admin-only wire round-trips: 318/319 NameServer config, 309 consumeMessageDirectly, etc. |
| `examples/live_acl.ts` | ACL: passes on a broker with authentication enabled using the right AK/SK; a wrong SK is rejected |
| `examples/live_tls.ts` | three TLS legs: plain_tls / ca_verify / mtls (needs a TLS-dedicated cluster) |
| `examples/live_tls_negative.ts` | negative probe: strict-CA client against the wrong CA — the route error must surface, not be swallowed |
| `examples/live_slave_only.ts` | with only the slave broker alive, a push consumer still consumes |
| `examples/live_check_client_config.ts` | CHECK_CLIENT_CONFIG(46) subscription pre-flight |
| `examples/live_fixes2.ts` | end-to-end re-verification of five reported fixes (e.g. a transaction half message visible right after COMMIT) |
| `examples/live_compression.ts` | compression round-trip matrix leg: zlib/LZ4/ZSTD sender + receiver with the match verdict (driven by `scripts/compression_matrix.sh`) |

Two ordering rules to respect: **start the consumer and wait until queue allocation lands before sending** (`CONSUME_FROM_LAST_OFFSET` semantics — otherwise the first messages fall before the assignment and are skipped); transaction check-back cases need a window ≥ 90s (the broker inspects every 30s). Topic/group names carry a timestamp by default, so leftover state cannot make an assertion falsely green.

## Repository Layout

```
nodeJs/
├── src/
│   ├── remoting/        # protocol layer: frame/serialization/header/body/route/subscription/namespace/ACL/TLS client
│   ├── common/          # message model, constants, compression, validation, utilities
│   └── client/          # MQClient, producer, push/pull/lite-pull/pop consumers, admin,
│                        # allocation/offset-store/consume-stats/tracing/back-pressure/traceparent
├── examples/            # live real-cluster tools (see "Live Cluster Verification")
├── test/                # offline smoke suites (protocol round-trip, allocation, offsets, tracing, stats, namespace, POP)
├── selfcheck.ts         # one-shot offline check (module load + 11 smoke suites)
└── package.json         # engines: node >= 22
```

## License

Apache-2.0
