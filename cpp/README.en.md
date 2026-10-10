# The C++ Implementation of the RocketMQ Remoting Client

English | [中文](README.md)

## Overview

A C++17 client for the classic RocketMQ remoting protocol. It talks straight to
NameServer(9876) + Broker(10911); no proxy is involved.

The network layer is hand-written (POSIX sockets + standard-library threads) and the runtime has
**zero third-party dependencies**: zlib is the only required system library, while liblz4 / libzstd /
OpenSSL are optional — a missing one only disables that single backend.

Supported: normal / FIFO / delayed / transactional / batch / oneway / true-async / request-reply /
timer-message recall producing; Push, Pull and LitePull consumption (POP consumption included); the
admin client; ACL; namespaces; message compression; TLS; message tracing; hooks. Everything above was
verified against a local RocketMQ 5.5.1 cluster.

## Prerequisites

| Dependency | Requirement | Notes |
| --- | --- | --- |
| CMake | >= 3.15 | `cmake_minimum_required(VERSION 3.15)` |
| Compiler | Full C++17 | Apple clang 14+ / GCC / MSVC (`/utf-8` is added automatically on MSVC) |
| Threads | Required | `find_package(Threads REQUIRED)`; explicit linking is needed on Linux |
| zlib | Required | `find_package(ZLIB REQUIRED)`; configure fails without it. `-DRMQ_WITH_ZLIB=OFF` builds a zlib-free variant |
| liblz4 / libzstd | Optional | Looked up under `/usr/local`, `/opt/homebrew` and as CMake config targets; absent means only that backend is off |
| OpenSSL | Optional | Compiled in whenever found; without it `setTlsEnable(true)` throws at runtime and every other path is unaffected |
| Cluster | NameServer 9876 + Broker 10911 | Only for live-cluster runs, with `autoCreateTopicEnable=true` |

Build options: `RMQ_BUILD_TESTS` (ON by default), `RMQ_BUILD_EXAMPLES` (ON by default),
`RMQ_WITH_ZLIB` / `RMQ_WITH_LZ4` / `RMQ_WITH_ZSTD` / `RMQ_ENABLE_TLS` (all ON by default).
The configure log states the status of every optional backend:

```
-- RocketMQ client zstd: enabled (/usr/local/lib/libzstd.dylib)
-- RocketMQ client lz4: enabled (/usr/local/lib/liblz4.dylib)
-- RocketMQ client TLS: enabled (OpenSSL 3.6.3)
```

`-Wall -Wextra` are enabled; the goal is zero warnings.

## Getting Started

```bash
cd cpp
cmake -S . -B build
cmake --build build -j6
cd build && ctest --output-on-failure
```

This produces the static library `build/librocketmq_remoting.a` (alias `rocketmq::remoting`),
52 test binaries and 40 tools under `build/examples/`. Measured on this machine:

```
100% tests passed out of 53
Total Test time (real) =  89.70 sec
```

The 53 cases are the 52 test binaries (4055 assertions) plus 1 codec-interop case
(73 assertions): **4128 assertions in total**.

The protocol-layer self-check needs no cluster:

```bash
./build/examples/rmq_selfcheck      # 3 [PASS] lines, last line "selfcheck: ALL PASS", exit code 0
```

Install headers and library:

```bash
cmake --install build --prefix /usr/local
```

## Examples

Headers live under `include/rocketmq/`. Link `rocketmq::remoting` via `add_subdirectory(cpp)`,
or link an installed `librocketmq_remoting.a`.

### Producer

Normal message:

```cpp
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
using namespace rocketmq;

DefaultMQProducer producer("PG_DEMO");
producer.setNamesrvAddr("127.0.0.1:9876");
producer.start();

Message msg("TopicDemo", "hello rocketmq");
msg.setTags("TagA");
msg.setKeys("ORDER_2026");
SendResult r = producer.send(msg, 3000);
// r.msgId / r.offsetMsgId / r.queueOffset
producer.shutdown();
```

FIFO message (the same business key always lands on the same queue):

```cpp
producer.sendBySelector(msg, SelectMessageQueueByHash(), "ORDER_2026", 3000);
```

Send to a chosen queue: `producer.send(msg, mq, 3000)`, queues from
`producer.fetchPublishMessageQueues(topic)`.

Delayed / timer message:

```cpp
Message delay("TopicDemo", "later");
delay.setDelayTimeLevel(3);             // a level of the broker's messageDelayLevel table
delay.setDelayTimeMs(10000);            // or milliseconds
delay.setDeliverTimeMs(nowMs + 10000);  // timer delivery; the SendResult carries recallHandle
```

Batch message (one request per batch; the broker stores N independent messages):

```cpp
std::vector<Message> batch;
batch.emplace_back("TopicDemo", "a");
batch.emplace_back("TopicDemo", "b");
SendResult rb = producer.sendBatch(batch, 3000);
```

Automatic batching: with `producer.setAutoBatch(true)` a plain `send()` is aggregated into batches;
tune `setBatchMaxDelayMs` (how long a batch may hold), `setBatchMaxBytes` (per-batch byte limit) and
`setTotalBatchMaxBytes` (global byte gate).

Oneway (no response waited for): `producer.sendOneway(msg);`

True async send (the caller never blocks; callbacks run on their own thread pools):

```cpp
class DemoCallback : public SendCallback {
public:
    void onSuccess(const SendResult& result) override { /* ... */ }
    void onException(const std::exception_ptr& e) override { /* ... */ }
};

producer.sendAsync(msg, std::make_shared<DemoCallback>(), 3000);
producer.sendBatchAsync(batch, std::make_shared<DemoCallback>(), 3000);
```

Async back pressure: `producer.setEnableBackpressureForAsyncMode(true)` (off by default) puts one
fair semaphore on in-flight count and one on bytes, configured by
`setBackPressureForAsyncSendNum` / `setBackPressureForAsyncSendSize`; async retry count is
`setRetryTimesWhenSendAsyncFailed`.

Transaction message (half message → local transaction → END_TRANSACTION; the broker's back-check
calls `checkLocalTransaction`):

```cpp
class DemoListener : public TransactionListener {
public:
    LocalTransactionState executeLocalTransaction(const Message&, const std::string&) override {
        return LocalTransactionState::COMMIT_MESSAGE;   // or ROLLBACK_MESSAGE / UNKNOW
    }
    LocalTransactionState checkLocalTransaction(const MessageExt& msg) override {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
};

TransactionMQProducer txProducer("PG_TX");
txProducer.setNamesrvAddr("127.0.0.1:9876");
txProducer.setTransactionListener(std::make_shared<DemoListener>());
txProducer.start();
TransactionSendResult tr = txProducer.sendMessageInTransaction(
    Message("TopicDemo", "half"), "arg");
// tr.getLocalTransactionState() == LocalTransactionState::COMMIT_MESSAGE
```

`DefaultMQProducer::sendMessageInTransaction(msg, listener, arg)` also takes a
`TransactionListener&`, so `TransactionMQProducer` is not mandatory.

Request-Reply (the requester blocks for the answer, which the broker pushes back):

```cpp
// requester
Message reply = producer.request(Message("TopicDemo", "ping"), 3000);

// replier (derived and sent from inside the consume callback)
Message response = createReplyMessage(requestMsg, "pong");
producer.send(response);
```

Timer-message recall (needs `timerWheelEnable=true` on the broker):

```cpp
SendResult rs = producer.send(delayMsg);
if (rs.recallHandle) {
    std::string uniqKey = producer.recallMessage("TopicDemo", *rs.recallHandle);
}
```

Other lookups: `queryMessage(topic, key, maxNum, begin, end)`, `searchOffset(mq, ts)`,
`maxOffset(mq)`, `minOffset(mq)`, `createTopic(key, newTopic, queueNum)`.

### Push Consumer

Concurrent consumption:

```cpp
#include "rocketmq/client/consumer.h"

class DemoListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext& ctx) override {
        for (const MessageExt& m : msgs) {
            // m.body / m.getTags() / m.queueOffset / m.getReconsumeTimes()
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;   // RECONSUME_LATER triggers redelivery
    }
};

DefaultMQPushConsumer consumer("GID_DEMO");
consumer.setNamesrvAddr("127.0.0.1:9876");
consumer.setConsumeFromWhere("CONSUME_FROM_LAST_OFFSET");
consumer.subscribe("TopicDemo", "TagA || TagB");
consumer.setMessageListener(std::make_shared<DemoListener>());
consumer.setConsumeThreadMin(4);
consumer.setConsumeThreadMax(16);
consumer.start();
```

Ordered consumption (use `MessageListenerOrderly`, return a `ConsumeOrderlyStatus`):

```cpp
class OrderlyListener : public MessageListenerOrderly {
public:
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                        ConsumeOrderlyContext& ctx) override {
        ctx.suspendCurrentQueueTimeMillis = 1000;   // suspension; -1 falls back to consumer config
        return ConsumeOrderlyStatus::SUCCESS;       // SUSPEND_CURRENT_QUEUE_A_MOMENT / ROLLBACK / COMMIT
    }
};
consumer.setMessageListener(std::make_shared<OrderlyListener>());
consumer.setMaxReconsumeTimes(3);
```

Broadcasting (offsets go to a local file; the group does not split queues):

```cpp
consumer.setMessageModel("BROADCASTING");
```

POP consumption (no offset commit; ack plus invisibleTime decide redelivery):

```cpp
consumer.setPopMode(true);
consumer.setPopInvisibleTime(30000);
consumer.setPopBatchNums(16);
```

Also available: `suspend()` / `resume()`, `setPullBatchSize` / `setPullThresholdForQueue`
(pre-pull flow control), `setAllocateMessageQueueStrategy` (six queue-allocation strategies),
`registerConsumeMessageHook` / `registerCheckForbiddenHook` / `registerFilterMessageHook`,
and `updateCorePoolSize(n)` to change concurrency at runtime.

### Pull Consumer

No background rebalance — the caller owns both the queue set and the offsets:

```cpp
#include "rocketmq/client/pull_consumer.h"

DefaultMQPullConsumer pull("GID_PULL");
pull.setNamesrvAddr("127.0.0.1:9876");
pull.start();

for (const MessageQueue& mq : pull.fetchSubscribeMessageQueues("TopicDemo")) {
    int64_t offset = 0;
    int64_t stored = 0;
    if (pull.fetchConsumeOffset(mq, stored)) offset = stored;

    PullResult res = pull.pull(mq, "*", offset, 32, 5000);       // one short poll
    // PullResult res = pull.pullBlockIfNotFound(mq, "*", offset, 32);   long polling
    for (const MessageExt& m : res.msgFoundList) { /* ... */ }
    pull.updateConsumeOffset(mq, res.nextBeginOffset);
}
pull.startHeartbeatLoop();   // turn on when the group must be visible on the broker
```

`fetchMessageQueuesInBalance(topic)` returns only the share this instance gets from the allocation
strategy.

### LitePull Consumer

```cpp
#include "rocketmq/client/lite_pull_consumer.h"

DefaultLitePullConsumer lite("GID_LITE");
lite.setNamesrvAddr("127.0.0.1:9876");
lite.subscribe("TopicDemo", "TagA");
lite.setPollTimeoutMillis(3000);
lite.setAutoCommit(true);
lite.setAutoCommitIntervalMillis(5000);
lite.start();

while (running) {
    for (const MessageExt& m : lite.poll()) { /* ... */ }
}
lite.commit();
lite.shutdown();
```

Queue-change listener (compared in the background every `topicMetadataCheckIntervalMillis`;
the callback fires only when the set actually changed):

```cpp
class QueueChange : public TopicMessageQueueChangeListener {
public:
    void onChanged(const std::string& topic,
                   const std::vector<MessageQueue>& mqs) override {
        std::printf("%s now has %zu queues\n", topic.c_str(), mqs.size());
    }
};
lite.setTopicMetadataCheckIntervalMillis(1000);
lite.registerTopicMessageQueueChangeListener("TopicDemo", std::make_shared<QueueChange>());
```

Assign mode and offset control:

```cpp
lite.assign(lite.fetchMessageQueues("TopicDemo"));
lite.setSubExpressionForAssign("TopicDemo", "TagA");
lite.seek(mq, 100);                 // seekToBegin(mq) / seekToEnd(mq)
lite.pause({mq}); lite.resume({mq});
int64_t committed = lite.committed(mq);
lite.commit({{mq, 200}}, true);
```

### Admin

```cpp
#include "rocketmq/client/admin.h"

DefaultMQAdminExt admin("ADMIN");
admin.setNamesrvAddr("127.0.0.1:9876");
admin.start();

admin.createTopic(MixAll::DEFAULT_TOPIC, "TopicDemo", 4);      // create from the template broker
TopicStatsTable stats = admin.examineTopicStats("TopicDemo");  // per-queue min/max/lastUpdate
ConsumeStatsList progress = admin.fetchConsumeStatsInBroker("127.0.0.1:10911");

// reset by timestamp (returns the new per-queue offsets) and reset by queue id (two RPCs: 25 + 222)
auto offsets = admin.resetOffsetByTimestamp("TopicDemo", "GID_DEMO", admin.minOffset(mq), true);
admin.resetOffsetByQueueId("127.0.0.1:10911", "GID_DEMO", "TopicDemo", 0, 100);

admin.deleteTopic("TopicDemo");
admin.shutdown();
```

Also: `examineTopicConfig` / `createAndUpdateTopicConfig` / `createAndUpdateTopicConfigList`,
`createAndUpdateSubscriptionGroupConfig` / `deleteSubscriptionGroup`,
`examineConsumerConnectionInfo`, `examineConsumeStats` / `examineConsumeStatsGroup` / `consumed` /
`messageTrackDetail`, `queryTopicsByConsumer`, `getAllTopicConfig` / `getUserTopicConfig`,
`fetchAllTopicRoute` / `getClusterList` / `examineBrokerClusterInfo` / `fetchBrokerRuntimeStats`,
`wipeWritePermOfBroker`, `searchOffset` / `searchLowerBoundaryOffset` / `searchUpperBoundaryOffset` /
`maxOffset` / `minOffset` / `earliestMsgStoreTime`, `examineConsumerOffset` /
`updateConsumerOffset` / `resetOffsetNew`, `queryMessage` / `queryMessageByKey` /
`queryMessageByUniqKey`, `cloneGroupOffset`.

### ACL

```cpp
#include "rocketmq/remoting/rpchook.h"

producer.setCredentials("AccessKey", "SecretKey");                 // optional third argument: securityToken
// equivalent form:
producer.setRPCHook(std::make_shared<AclClientRPCHook>(
    SessionCredentials("AccessKey", "SecretKey")));
```

All three consumers and `DefaultMQAdminExt` have `setCredentials` / `setRPCHook` as well.
The hook chain order is fixed as Namespace → Stream → ACL, so `ns` and `ReqT` both end up inside the
signed content; compose custom hooks with `composeRequestHooks()` before registering them.

### Namespaces

Two independent mechanisms:

```cpp
// 1) client-side resource prefix: topic/group become <ns>%<resource>, wrapped and unwrapped
//    consistently across sending, heartbeat and offsets
producer.setNamespace("MQ_INST_XX");

// 2) server-side namespace: every request additionally carries nsd=true / ns=<value>,
//    and changing it after start() still takes effect
producer.setNamespaceV2("MQ_INST_XX");
```

`setNamespace` changes the resource name itself (the `%RETRY%` / `%DLQ%` prefixes stay outside the
namespace), `setNamespaceV2` changes request-header fields. Both are supported by all four client
facades and by the admin client.

### Message Compression

```cpp
producer.setCompressMsgBodyOverHowmuch(4096);     // auto-compress above this many bytes; 0 disables
producer.setCompressType(CompressionType::ZSTD);  // ZLIB(3) / LZ4(1) / ZSTD(2)
producer.setCompressLevel(5);                     // only meaningful for ZLIB
```

The consumer decompresses automatically based on `COMPRESSED_FLAG`. A backend disabled at build
time throws when it meets such a message instead of handing out compressed bytes as the body.
Type bits `0` and `3` both decode as ZLIB.

### TLS

```cpp
producer.setTlsEnable(true);                     // or the ROCKETMQ_TLS_ENABLE=1 environment variable
TlsOptions opts;
opts.caCert = "/path/ca.pem";                    // non-empty = strict chain + hostname verification
opts.clientCert = "/path/client.pem";            // mTLS
opts.clientKey = "/path/client.key";
opts.serverName = "broker-a";
producer.setTlsOptions(opts);
```

Each TLS connection owns one `TlsSession`, and `SSL_*` calls inside the session are serialized (the
reader thread and the caller's writing thread overlap on the same SSL session).
`DefaultMQPushConsumer`, `DefaultMQPullConsumer` and `DefaultLitePullConsumer` expose
`setTlsEnable` / `setTlsOptions` too.

## Features and Status

- ✅ Remoting protocol: JSON and ROCKETMQ binary serialization, `RemotingCommand` frame
  encode/decode, the CommandCustomHeader family (V2 single-letter field names included), the 17-segment
  message storage format and the 6-segment batch format
- ✅ Transport: sync / async / oneway, partial-packet reassembly, opaque matching, reconnect,
  GO_AWAY(1500) connection switch with one resend, dead-connection detection, SIGPIPE handling
- ✅ TLS: test mode, strict CA verification, mTLS
- ✅ Producer: normal / FIFO (selector and pinned) / delayed and timer / transactional / batch /
  oneway / true async / async back pressure / automatic batching / request-reply / timer recall /
  message query
- ✅ Consumers: `DefaultMQPushConsumer` (concurrent, ordered, broadcasting, POP),
  `DefaultMQPullConsumer` (short and long polling, offset tables, heartbeat),
  `DefaultLitePullConsumer` (subscribe and assign modes, seek, three offset tables, queue-change listener)
- ✅ Queue allocation: AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM /
  MACHINE_ROOM_NEARBY, replaceable on all three consumers
- ✅ `DefaultMQAdminExt`: topic and subscription-group CRUD, consume progress, connection info,
  offset reset (220 / 222), route and cluster info, write-permission wiping
- ✅ ACL V1 / V2 / V3 signatures (SHA1 / HMAC / Base64 standard vectors) and STS securityToken
- ✅ Both namespace mechanisms: local resource prefix and server-side `nsd`/`ns`
- ✅ Compression: zlib / LZ4 Frame / ZSTD, auto-compress on produce, auto-decompress on consume
- ✅ Message tracing (Pub / SubBefore / SubAfter / EndTransaction / Recall) and W3C `traceparent` context
- ✅ Hooks: SendMessageHook / ConsumeMessageHook / EndTransactionHook / CheckForbiddenHook /
  FilterMessageHook
- ✅ Fault tolerance: broker-switching send retry, latency-based isolation and recovery, pre-pull flow
  control, stalled-queue self-healing, the `cleanExpiredMsg` escape hatch for hung listeners,
  OFFSET_ILLEGAL and empty-response offset correction
- ✅ Name validation (`Validators` / `TopicValidator`) plus startup numeric configuration gates
- ✅ Dynamic NameServer (`DefaultTopAddressing` URL rules)
- ✅ Local offset file (broadcasting) with `.bak` rolling
- ✅ Client logging: level filtering, size-based rotation, thread name / milliseconds / file:line
- ⬜ The Windows code path is included but has not been exercised on a real machine

## Client Logging

`include/rocketmq/common/logging.h` is a header-only logger. Default level is INFO, and output goes
to both stderr and a file.

Line format `date.milliseconds LEVEL [pid] [thread] [file:line] - message`, two measured lines:

```
2026-10-10 16:44:16.450 INFO  [21908] [main] [producer.cpp:270] - DefaultMQProducer[PG_DEMO] started, clientId=30.234.192.255@21908#198086958751588
2026-10-10 16:44:16.510 INFO  [21908] [tid-7e48] [produce_accumulator.cpp:746] - ..._GuardForAsyncSend service end
```

The main thread logs as `main`; worker threads get internal names, and the batching guard threads are
`<clientId>_GuardForSyncSend` / `<clientId>_GuardForAsyncSend`.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` / `OFF` |
| `ROCKETMQ_CLIENT_LOG_FILE` | `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log` | Empty string / `OFF` / `NONE` keeps stderr only |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864` (64MB) | Per-file cap; `0` disables rotation |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | Number of backups kept; `0` keeps none |

Rotation is a fixed window: when the file reaches `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE`, `<file>.N` is
deleted first, the rest shift by one (`.N-1 → .N` … `.1 → .2`) and the current file becomes `.1`.
Backups are therefore `rocketmq_cpp_client.log.1 … rocketmq_cpp_client.log.10`, with `.10` being the
oldest. Backups are not compressed. Writes are synchronous (`fflush` per line), so `tail -f` is live.

Connection close is logged at DEBUG; protocol anomalies (illegal frame length, decode failure) at
WARN. Long-polling timeouts are a normal path and log at DEBUG, so `ERROR=0` is the expected state at
the default level.

## Live Cluster Verification

These tools need a running NameServer(9876) + Broker(10911) cluster with
`autoCreateTopicEnable=true`, and they are **not part of ctest** (external dependency). Start the
consumer before sending messages: most scenarios rely on the consumer registering first, so that the
broker's subscription table, offsets and trace records land on the intended group.

```bash
./build/examples/rmq_live_message_types 127.0.0.1:9876   # the default namesrv address may be omitted
```

Any failure exits non-zero. Output prints `[PASS]` / `[FAIL]` per check and a final
`PASS=<n> FAIL=<m>` line; legs needing extra broker configuration print SKIP with the reason.

Most tools accept a single optional namesrv argument (default `127.0.0.1:9876`). The ones with more
arguments:

```bash
./build/examples/rmq_live_tls 127.0.0.1:9876 <topic> <group> [leg] [caCert] [serverName] [clientCert] [clientKey]
./build/examples/rmq_live_tls_negative 127.0.0.1:9876 <topic> <wrongCaCert> [serverName]
./build/examples/rmq_live_pull_heartbeat 127.0.0.1:9876 127.0.0.1:10911 [slaveAddr]
./build/examples/rmq_live_publish_route_master 127.0.0.1:9876 127.0.0.1:10911 [slaveAddr]
./build/examples/rmq_compression_live selftest 127.0.0.1:9876
./build/examples/rmq_compression_live send 127.0.0.1:9876 <topic> <group> <size> [codec]
```

| Binary | What it verifies | Extra broker config |
| --- | --- | --- |
| `rmq_selfcheck` | Protocol-layer round-trip self-check, no cluster needed | — |
| `rmq_interop` | Codec interop tool, driven by `tests/interop_check.py` | — |
| `rmq_live_message_types` | Eight message capabilities: async / FIFO / tag / property / delayed / key query / transactional / batch | — |
| `rmq_live_redelivery` | Thirteen legs: redelivery, offset persistence, ordered locks, broadcasting, flow control, multi-instance rebalance, explicit ack | — |
| `rmq_live_pop` | POP polling / ack / invisible-time extension / redelivery after no ack | — |
| `rmq_live_pop_consumer` | POP consume loop plus pullRT / pullTPS in the 307 status table | — |
| `rmq_live_pull` | Pull consumer queues, short and long polling, manual offsets | — |
| `rmq_live_pull_heartbeat` | The 203 / 38 / 35 payloads and consumeType shape of the pull consumer | — |
| `rmq_live_lite_pull` | LitePull subscribe and assign modes plus poll | — |
| `rmq_live_lite_pull_cursor` | Cursor following `nextBeginOffset` on empty responses and OFFSET_ILLEGAL self-healing | — |
| `rmq_live_lite_pull_code` | Request code 361 with the lite flag bit, and `litePullMessageEnable` restore | `litePullMessageEnable` (tool toggles it) |
| `rmq_live_lite_topic_queue_change` | Queue scale-out / scale-in firing the listener, route queried per comparison pass | — |
| `rmq_live_subscribe` | `subscribe` after `start()` pushes a heartbeat immediately and the new topic is really consumed | — |
| `rmq_live_async_send` | Async send thread identity / concurrency / pinned send / interception / batch / drain on shutdown | — |
| `rmq_live_backpressure` | The two fair semaphores throttling async sends, plus runtime capacity growth | — |
| `rmq_live_send_header` | Send-header fields `c` / `d` / `n` and the queue count of auto-created topics | — |
| `rmq_live_pinned_guard` | Pinned-send topic guard: rejected locally, no trace on the broker | — |
| `rmq_live_producer_unregister` | clientId(35) unregistration on every broker at shutdown | — |
| `rmq_live_flow_control` | All five pre-pull thresholds trigger and nothing is lost afterwards | — |
| `rmq_live_correct_tags_offset` | Empty responses still advance the committed offset to maxOffset | — |
| `rmq_live_offset_illegal` | Batch discarded and queue rebuilt at the corrected offset | — |
| `rmq_live_reset_offset` | 220 offset reset: persisted at once, in-flight batch discarded, queue rebuilt | — |
| `rmq_live_scheduled_intervals` | First tick and fixed-rate behaviour of route refresh and offset persistence | — |
| `rmq_live_clean_expired_msg` | Escape hatch for a hung listener: sweep, redeliver to `%RETRY%`, second delivery (~4 minutes) | — |
| `rmq_live_fail_fast` | In-flight requests die within milliseconds when the broker really stops (stops and restarts the broker once) | — |
| `rmq_live_publish_route_master` | With the master down: publish queues empty out, subscriptions unchanged, messages still consumed from the slave | Slave broker |
| `rmq_live_latency` | Send-latency fault tolerance, isolation and recovery | — |
| `rmq_live_hook` | CheckForbiddenHook interception plus both FilterMessageHook pull paths | — |
| `rmq_live_trace` | End-to-end message trace reporting and query | `traceTopicEnable=true` |
| `rmq_live_request_reply` | Request → reply → 326 push-back, including error codes 10006 / 10007 | — |
| `rmq_live_acl` | ACL signature verified by a real broker | `authenticationEnabled=true` |
| `rmq_live_tls` | TLS path plus traceparent injection / child span | `tls.test.mode.enable` |
| `rmq_live_tls_negative` | Trusting a wrong CA under strict verification must surface a CA marker | TLS enabled on the broker, plus a wrong CA file |
| `rmq_live_unit_config` | Where unitName / unitMode / stream land on the broker side | — |
| `rmq_admin_live` | Full admin path: topic / subscription group / progress / connection info / reset | — |
| `rmq_admin_batch_live` | Batch admin calls (`createAndUpdateTopicConfigList` and friends) | — |
| `rmq_validators_live` | Local fast rejection of illegal names; legal names still send and receive | — |
| `rmq_sql92_live` | SQL92 property filtering and CHECK_CLIENT_CONFIG(46) | `enablePropertyFilter=true` |
| `rmq_recall_live` | Timer-message recall handle round-trip and no delivery after recall | `timerWheelEnable=true`, `recallMessageEnable` (tool toggles it) |
| `rmq_compression_live` | Auto-compression send-to-self: `selftest` or `send|recv <namesrv> <topic> <group> <size> [codec]` | — |

## Repository Layout

```
cpp/
├── CMakeLists.txt              Static library + optional backend detection (zlib / lz4 / zstd / OpenSSL)
├── include/rocketmq/
│   ├── common/                 Message model, codecs, compression, hash ring, namespace, logging
│   ├── remoting/               RemotingClient, TLS session, RPC hooks, protocol/ (codes /
│   │                           headers / route / heartbeat / body / admin_body / ...)
│   └── client/                 producer / consumer / pull_consumer / lite_pull_consumer /
│                               admin / mq_client / allocate_strategy / hook / trace /
│                               request_reply / backpressure / validators / result / exception
├── src/                        Implementations mirroring include/ (common / remoting / client)
├── tests/                      52 test sources + interop_check.py = 53 ctest cases
├── examples/                   rmq_selfcheck / rmq_interop + 38 live-cluster tools
└── tools/                      Windows header preparation and syntax-check scripts
```

## License

Apache-2.0, the same as Apache RocketMQ.
