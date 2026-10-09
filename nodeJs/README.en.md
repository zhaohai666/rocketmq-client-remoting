# nodeJs — Node.js / TypeScript client (sixth language, seven clients in total)

> [中文](README.md) | English

Node.js implementation of the Apache RocketMQ **classic remoting protocol** client. Isomorphic with the `python/` `go/` and other clients: the same protocol semantics, the same message model, zero third-party dependencies (uses only `node:` built-in modules).

- **File form**: TypeScript (`.ts`), run directly via `node --experimental-strip-types` (Node ≥ 22.x), **no build step required**; ESM imports with explicit `.ts` extensions.
- **Reference source**: Java 5.x (`zhaohai666-rocketmq`), cross-checked against the two verified Python / Go implementations.
- **Target clusters**: RocketMQ 4.x / 5.x, integration testing based on **5.5.1** (NameServer 9876 + Broker 10911).

## Self-check run (offline, no cluster needed)

```bash
node --experimental-strip-types selfcheck.ts
```

Loads all modules under `src/` and runs 5 smoke suites (protocol layer / producer / consumer / statistics / Java-gap fill surface).
(Note: if the local sandbox blocks `spawnSync` from spawning child processes, you can run the `test/*.ts` files one by one directly to verify.)

## Quick start

All seven languages share the same sequence of actions: build the facade → set the NameServer address → `start()` → send/receive → `shutdown()`.

```ts
import { DefaultMQProducer } from './src/client/producer.ts';
import { Message } from './src/common/message.ts';

const producer = new DefaultMQProducer('GID_TEST');
producer.setNamesrvAddr('127.0.0.1:9876');
producer.start();

const result = await producer.send(new Message('TopicTest', Buffer.from('hello'), 'TagA', 'k1'));
console.log(result.msgId, result.messageQueue.getQueueId());

producer.shutdown();
```

Push Consumer (cross-port hard rule: **start the consumer first, then send messages**):

```ts
import { DefaultMQPushConsumer } from './src/client/consumer.ts';
import { ConsumeConcurrentlyStatus } from './src/client/consumer_result.ts';

const consumer = new DefaultMQPushConsumer('GID_TEST_C');
consumer.setNamesrvAddr('127.0.0.1:9876');
consumer.subscribe('TopicTest', '*');
consumer.registerMessageListenerConcurrently((msgs) => {
  for (const m of msgs) console.log(m.getBody()?.toString());
  return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});
await consumer.start();
```

## Capability surface (aligned with the other six clients)

| Area | Status | Description |
| --- | --- | --- |
| Protocol layer | ✅ | `RemotingCommand` frame, dual JSON + RocketMQ binary serialization, V2 short-key header, fastjson2 fault-tolerant parsing, 17-segment/6-segment message encode/decode |
| Transport layer | ✅ | Long-connection lazy connect and reuse, sync/async/oneway, half-packet reassembly, opaque matching, resend once on GO_AWAY, mark in-flight requests dead on disconnect, TLS opt-in |
| Sending | ✅ | Sync/batch/oneway/queue selector/async/transaction two-phase + check-back/Request-Reply/recallMessage |
| Push Consumer | ✅ | Long polling, ordering, broadcast, offset persistence, flow-control thresholds, 307/220/221 responses, RETRY restoration |
| Pull / Lite Pull | ✅ | Short polling (pull) and blocking polling (pullBlockIfNotFound), Lite Pull subscribe + assign + seek + auto-commit; `fetchPublishMessageQueues` / `fetchSubscribeMessageQueues` are Java's two DIFFERENT views (:137 write queues vs :142 read queues), and `fetchMessageQueuesInBalance` returns only this instance's share |
| Namespace / clientId | ✅ | `namespaceV2` is read live per request by `NamespaceRpcHook` (`nsd=true` / `ns=<value>`; hook order Namespace → Stream → ACL, so `ns`/`ReqT` land inside the ACL signature). Every clientId comes from `MixAll.clientIdFor` as `<ip>@<instanceName>[@<unitName>][@STREAM]`, and under CLUSTERING the `DEFAULT` instance name becomes `<pid>#<nanotime>` exactly like Java's `changeInstanceNameToPID` — two instances in one process therefore do NOT share a clientId (sharing one makes allocate() hand both the same slice, i.e. duplicate consumption). Pull and Lite Pull consumers default `enableStreamRequestType` on, as their Java constructors do (clientId gets `@STREAM`, every request gets `ReqT=0`) |
| Queue allocation | ✅ | AVG / AVG_BY_CIRCLE / CONFIG / MACHINE_ROOM / CONSISTENT_HASH (MD5 ring + virtual nodes) / **MACHINE_ROOM_NEARBY** (Java AllocateMachineRoomNearBy: exclusive within a machine room + globally balanced across machine rooms with no live consumers; throws when the resolver yields an empty machine room) |
| Admin | ✅ | `DefaultMQAdminExt`: topic CRUD, cluster/runtime info, subscription groups (**paged 201**), consume statistics, connection query, offset reset, KV config (**broadcast**), GET_BROKER_CONFIG (**Properties text body**) |
| Message tracing | ✅ | Encode/decode (separated by `\x01`/`\x02`, 7-segment fault tolerance for SubBefore without keys), async dispatcher, send/receive hooks |
| Consume statistics | ✅ | Differential-window StatsItem (10s/10min sampling chain), 307 statusTable (consumeFailedMsgs takes the hour window) |
| Hooks/ACL | ✅ | CheckForbidden (does not swallow exceptions) / FilterMessage (must swallow), ACL signature (HMAC-SHA1 + Base64) |
| Request-Reply receive side | ✅ | `PUSH_REPLY_MESSAGE_TO_CLIENT(326)` handler: rebuilds MessageExt from ReplyMessageRequestHeader per Java `processReplyMsg`, decompresses the body, atomically removes the future by CORRELATION_ID and wakes it up (send side `request()` aligns with Java `prepareSendRequest`: REPLY_TO_CLIENT=clientId + TTL) |
| Async back-pressure | ✅ | Two fair semaphores (num/size) wired into `sendAsync`: throws synchronously when permits are insufficient, the callback returns them exactly once; `setBackPressureForAsyncSendNum/Size` adjustable at runtime |
| W3C traceparent | ✅ | `src/client/traceparent.ts`: injection (context propagated by the caller takes priority) / extraction / validation / child span, `ROCKETMQ_TRACE_CONTEXT_ENABLE` switch, same as the Go client |
| VIP channel | ✅ | Producer side `setSendMessageWithVIPChannel`: send RPCs go through the broker VIP port (port-2) |
| Name server config | ✅ | `updateNameServerConfig` (318 broadcast, Properties text body) / `getNameServerConfig` (319) |
| Message query | ✅ | `queryMessage`(12) / `queryMessageByUniqKey` / `viewMessage`(33, connect directly to the address embedded in msgId) / `consumeMessageDirectly`(309 admin-initiated) / boundary offsets LOWER/UPPER / `examineConsumerOffset` |
| 5.x timed messages | ✅ | `setDelayTimeSec/Ms` / `setDeliverTimeMs` (TIMER_DELAY_SEC / TIMER_DELAY_MS / TIMER_DELIVER_MS, same-named setters as Java Message) |
| Compression | ✅ | All three types complete, zero third-party dependencies (`src/common/compress.ts`): LZ4 is a hand-written **Frame** format (frame header + xxh32 HC, same wire format as Java `LZ4FrameOutputStream` and Python `lz4.frame`, with encode/decode complete in both directions); ZSTD goes through the zstd binding of `node:zlib` (shipped in Node's stdlib, Node ≥ 23.8), so on lower-version runtimes it falls back to a self-written Raw/RLE frame encoder plus a decoder that only recognizes Raw/RLE; input that cannot be decoded **throws explicitly**, and a compressed stream is never passed through as the plain body |

## Real-cluster integration testing

```bash
# Single item (internally auto-starts/reuses the local 5.5.1 cluster, and only stops the one it started when done)
bash scripts/run_node_live.sh producer   # full send chain: sync/batch/oneway/async/selector/transaction check-back
bash scripts/run_node_live.sh consumer   # push consume read-back: start the consumer first, then send messages
bash scripts/run_node_live.sh pull       # Pull Consumer: short polling + offset + sendMessageBack
bash scripts/run_node_live.sh lite_pull  # Lite Pull: poll + seek + commitSync
bash scripts/run_node_live.sh admin      # admin: topic/cluster/subscription group/connection/statistics/Properties config
```

Topic/group names carry a timestamp by default, so leftover state will not make an assertion falsely green.

## Known differences from the other six clients

- **POP consumption mode implemented (2026-10-01)**: POP_MESSAGE(200050) / ACK(200051) / CHANGE_MESSAGE_INVISIBLETIME(200053) / BATCH_ACK(200151) / SET_MESSAGE_REQUEST_MODE(401), POP_CK checkpoint dual-path reconstruction (offset table / message self-offset), flow control by response ack-debt, secondary validation of out-of-window batches, retry back-off in sixteen levels (checkNeedAckOrDelay), the 307 mqPopTable, auto-issuing 401 on setPopMode. Orderly POP is rejected in checkConfig per the Java stub ("POPTODO"). The classic client, staying faithful to Java, does not route batch ack (it only exposes the online capability). **POP queues come from this port's client-side rebalance**, not from Java's `clientRebalance=false` broker-side assignment (`RebalanceImpl#getRebalanceResultFromBroker:345` → `MQClientAPIImpl#queryAssignment:405`, QUERY_ASSIGNMENT=400) — the same deliberate decision as the other six ports: semantically equivalent, differing only in WHO picks the queue set (see the `doRebalance` comment in `src/client/consumer.ts`). **Live cases not run** — for the queues/assertions see `test/pop_smoke.ts`.
- **2026-10-01 gap filled against the Java client (zhaohai666-rocketmq 5.x)**: MACHINE_ROOM_NEARBY, Request-Reply 326 receive-side closed loop, async back-pressure wiring, traceparent, VIP channel, admin message query/boundary offset/307 offset read/name server config/309 admin-initiated, 5.x timed setters — see `test/java_gap_fill_smoke.ts`.
- **2026-09-30 live verification baseline**: producer 8/8 (sync×10 / batch / oneway / async / selector / transaction COMMIT / broker check-back), consumer 2/2 (20 messages exactly-once), pull 3/3, lite_pull 3/3 (poll / seek / commitSync), admin 11/11 — all against a real 5.5.1 cluster (`scripts/run_node_live.sh`). Caveats: consumption cases must **wait until queue allocation is in place before sending messages** (`CONSUME_FROM_LAST_OFFSET` semantics); the transaction check-back window ≥90s (broker inspection cycle 30s).
- **Message-tracing consumer-side decode** is implemented, but it is not advisable to rely on it before the tracing live cases pass.
- SUSPEND/RESUME_CONSUMER(209/210) and ADJUST_CONSUMER_THREAD_POOL(213), already removed in Java 5.x, have only request-code constants and no callers at all (the tools module does not use them either); the nodeJs client stays consistent with the Java baseline and does not implement them.

## Directory structure

```
nodeJs/
├── src/
│   ├── remoting/        # Protocol layer: frame/serialization/header/body/route/subscription/namespace/ACL/TLS client
│   ├── common/          # Message model, constants, validation, utilities
│   └── client/          # MQClient, producer, consumer, pull, admin, allocation/offset/statistics/tracing
├── examples/            # Live real-cluster tools (producer/consumer/pull/lite_pull/admin)
├── test/                # Offline smoke (protocol round-trip, allocation, offset, tracing, statistics diff window)
└── selfcheck.ts         # One-shot offline self-check
```
