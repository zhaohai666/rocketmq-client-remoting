# The C# Implementation of the RocketMQ Remoting Client

English | [中文](README.md)

## Overview

`csharp/src/RocketMQ.Client` is a .NET client for the Apache RocketMQ classic remoting protocol.
It talks straight to the NameServer (route discovery, dynamic addressing) and to the Broker
(message I/O, heartbeat, offsets, admin requests) — no proxy, no sidecar.

- Target framework `net10.0`, **zero external NuGet dependencies**: BCL only. JSON-tolerant parsing,
  frame codecs and the TCP transport are hand-rolled; zlib uses the built-in `ZLibStream`,
  LZ4 / ZSTD are reached through P/Invoke against the system libraries.
- One library covers the producer, the Push / Pull / LitePull consumers, the admin client,
  message tracing, ACL and TLS.
- Every capability is verified against a real RocketMQ 5.5.1 cluster (namesrv 9876 + broker 10911);
  offline behaviour is pinned by 808 unit tests.

## Prerequisites

- .NET 10 SDK (`dotnet --version` on this machine → `10.0.203`).
- A reachable cluster: NameServer on `9876`, Broker on `10911`, with `autoCreateTopicEnable=true`.
- Cluster lifecycle via the repo scripts: `sh ../scripts/rmq_test_broker.sh status | stop | start`
  (the fault-injection cases use the same script; it never wipes the store).

## Getting Started

```bash
cd csharp
dotnet build                        # src + tests + examples
dotnet test tests/RocketMQ.Client.Tests
```

Measured on this machine:

| Command | Result |
| --- | --- |
| `dotnet build --no-incremental` | `0 个警告` / `0 个错误` (0 warnings / 0 errors), elapsed `00:00:17.94` |
| `dotnet test tests/RocketMQ.Client.Tests` | `已通过! - 失败: 0，通过: 808，已跳过: 0，总计: 808，持续时间: 2 m 52 s` (0 failed / 808 passed / 2 m 52 s) |

`Directory.Build.props` turns `TreatWarningsAsErrors=true` on globally, so "builds" means "builds with
zero warnings". The test summary line follows the machine locale — this box prints it in Chinese, so
grep for `总计:` when parsing.

The example project builds a single executable named `rmq` holding all live-cluster cases as
subcommands:

```bash
dotnet build examples/RocketMQ.Examples                     # output: bin/Debug/net10.0/rmq.dll
dotnet run --no-build --project examples/RocketMQ.Examples -- selfcheck
# selfcheck: ALL PASS (PASS=3 FAIL=0) — no cluster needed
```

Consume it from your own project, or pack it:

```bash
dotnet add reference ../csharp/src/RocketMQ.Client/RocketMQ.Client.csproj
dotnet pack src/RocketMQ.Client/RocketMQ.Client.csproj -o ./artifacts   # → RocketMQ.Client.Remoting.1.0.0.nupkg
```

Namespaces you need: `RocketMQ.Client` (clients, admin, tracing, result types),
`RocketMQ.Common` (`Message` / `MessageExt` / `MessageQueue` / compression / logging),
`RocketMQ.Remoting` (`TlsOptions` / RPC hooks), `RocketMQ.Remoting.Protocol`
(`RequestCode` / `MessageModel` / `ConsumeFromWhere` / route and admin DTOs).

## Examples

### Producer

On the producer the NameServer is a **property** (`NamesrvAddr`); on the consumers and the admin
client it is a **method** (`SetNamesrvAddr(...)`).

```csharp
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;

var producer = new DefaultMQProducer("PID_DEMO") { NamesrvAddr = "127.0.0.1:9876" };
producer.Start();

Message msg = new("TopicTest", Encoding.UTF8.GetBytes("hello rocketmq"));
SendResult result = producer.Send(msg);                       // sync send, retries on failure
Console.WriteLine($"{result.SendStatus} {result.MsgId} {result.MessageQueue} offset={result.QueueOffset}");

producer.Shutdown();
```

Multiple addresses and tuning: `NameServerAddresses = new List<string> { "10.0.0.1:9876", "10.0.0.2:9876" }`,
`SendMsgTimeout`, `RetryTimesWhenSendFailed`, `RetryAnotherBrokerWhenNotStoreOk`,
`SendLatencyFaultEnable` (fault-avoiding queue selection), `MaxMessageSize`.

#### Ordered messages

```csharp
// 1) pinned to one queue
producer.Send(msg, new MessageQueue("TopicTest", "broker-a", 0));

// 2) queue picked by a business key: the same key always lands on the same queue
producer.SendBySelector(msg, new SelectMessageQueueByHash(), orderId);

// custom selector: MessageQueue Select(IReadOnlyList<MessageQueue> mqs, Message msg, string arg)
```

#### Delayed / timed messages

```csharp
msg.DelayTimeLevel = 3;                  // levels come from the broker's messageDelayLevel
msg.SetDelayTimeSec(60);                 // deliver 60 seconds from now
msg.SetDelayTimeMs(60_000);              // millisecond delay
msg.SetDeliverTimeMs(deadlineUnixMs);    // absolute delivery timestamp
```

#### Batch messages

```csharp
var batch = new List<Message>
{
    new("TopicTest", Encoding.UTF8.GetBytes("a")),
    new("TopicTest", Encoding.UTF8.GetBytes("b")),
};
producer.SendBatch(batch);                            // request code 320; the broker stores N separate messages with consecutive offsets
producer.SendBatch(batch, new MessageQueue("TopicTest", "broker-a", 1));
producer.SendBatchAsync(batch, callback);             // async batch

// auto-batching: eligible messages go into an accumulator and are flushed by time / volume
producer.AutoBatch = true;
producer.BatchMaxDelayMs = 100;
producer.BatchMaxBytes = 1024 * 1024;
bool accepted = producer.CanBatch(msg);               // delayed messages and %RETRY% topics are not accumulated
```

#### One-way messages

```csharp
producer.SendOneway(msg);                  // no response awaited; only guarantees the request left
producer.SendOneway(msg, new MessageQueue("TopicTest", "broker-a", 0));
```

#### Async messages

```csharp
producer.SendAsync(msg, new DemoCallback(), timeoutMillis: 3000);
producer.SendAsync(msg, new DemoCallback(), mq: new MessageQueue("TopicTest", "broker-a", 0));

sealed class DemoCallback : ISendCallback
{
    public void OnSuccess(SendResult sendResult) { /* invoked exactly once */ }
    public void OnException(Exception error) { /* explicit broker error = MQBrokerException, everything else = MQClientException */ }
}
```

Tuning: `RetryTimesWhenSendAsyncFailed`, `AsyncSenderQueueCapacity`, `ClientCallbackExecutorThreads`,
`AsyncSenderExecutor` (bring your own pool), plus the fair-semaphore backpressure:

```csharp
producer.EnableBackpressureForAsyncMode = true;
producer.BackPressureForAsyncSendNum = 4096;          // in-flight message gate
producer.BackPressureForAsyncSendSize = 64 * 1024 * 1024;  // in-flight byte gate (pre-compression body size)
long left = producer.SemaphoreAsyncSendNumAvailablePermits;
```

`Shutdown()` refuses new submissions first, then drains the sender pool, so every accepted call
finishes its preparation stage and hands its request to the transport layer.

#### Transactional messages

```csharp
var tx = new TransactionMQProducer("PID_TX") { NamesrvAddr = "127.0.0.1:9876" };
tx.TransactionListener = new DemoTransactionListener();
tx.Start();

TransactionSendResult r = tx.SendMessageInTransaction(msg, arg: "order-42");

sealed class DemoTransactionListener : ITransactionListener
{
    // called after the half message is stored; the returned state is what END_TRANSACTION carries
    public LocalTransactionState ExecuteLocalTransaction(Message msg, string arg) =>
        DoBusiness(arg) ? LocalTransactionState.CommitMessage : LocalTransactionState.RollbackMessage;

    // called when the broker checks back CHECK_TRANSACTION_STATE(39); return unknown to be asked again
    public LocalTransactionState CheckLocalTransaction(MessageExt msg) =>
        LocalTransactionState.Unknow;
}
```

Half message → local transaction → `END_TRANSACTION(37, oneway)` → broker check-back, all on the real
protocol. The producer keeps sending heartbeats carrying `ProducerData`, and the check-back depends
on them — do not switch them off.

#### Request-Reply

```csharp
// requester
Message reply = producer.Request(requestMsg, timeoutMillis: 8000);
producer.Request(requestMsg, new DemoRequestCallback(), timeoutMillis: 8000);   // async flavour

sealed class DemoRequestCallback : RequestCallback
{
    public void OnSuccess(Message? responseMessage) { }
    public void OnException(Exception? e) { }         // a timeout surfaces as RequestTimeoutException
}

// responder: an ordinary push consumer builds the reply from the request message and sends it
Message reply = RequestReply.CreateReplyMessage(requestMsg, Encoding.UTF8.GetBytes("pong"));
producer.Send(reply);
```

Requests carry `CORRELATION_ID` / `REPLY_TO_CLIENT` / TTL, replies travel on `<cluster>_REPLY_TOPIC`,
and `RequestReply.IsReplyMessage(msg)` tells the two flows apart.

#### Recalling a timed message

```csharp
Message timer = new("TopicTest", body);
timer.SetDelayTimeSec(120);
SendResult sent = producer.Send(timer);

string? handle = sent.RecallHandle;                                 // null for ordinary messages
HandleV1 parsed = RecallMessageHandle.DecodeHandle(handle!);        // Topic / BrokerName / TimestampStr / MessageId
string uniqKey = producer.RecallMessage("TopicTest", handle!);      // returns the uniqKey of the recalled message
```

Recall requires `recallMessageEnable=true` on the broker.

### PushConsumer (concurrent / orderly / broadcasting / POP)

```csharp
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

var consumer = new DefaultMQPushConsumer("GID_DEMO");
consumer.SetNamesrvAddr("127.0.0.1:9876");
consumer.Subscribe("TopicTest", "tagA || tagB");            // or MessageSelector.BySql("price > 10")
consumer.SetMessageListener(new DemoListener());
consumer.Start();
// ... before exit
consumer.Shutdown();

sealed class DemoListener : IMessageListenerConcurrently
{
    public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context)
    {
        foreach (MessageExt m in msgs)
            Console.WriteLine($"{m.MsgId} {Encoding.UTF8.GetString(m.Body)} reconsume={m.ReconsumeTimes}");
        return ConsumeConcurrentlyStatus.ConsumeSuccess;     // ReconsumeLater ⇒ sent back to %RETRY%
    }
}
```

Orderly consumption:

```csharp
consumer.SetMessageListener(new OrderlyListener());

sealed class OrderlyListener : IMessageListenerOrderly
{
    public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context)
    {
        context.SuspendCurrentQueueTimeMillis = 3000;        // how long this queue stays suspended on failure
        return ConsumeOrderlyStatus.Success;                 // or SuspendCurrentQueueAMoment
    }
}
```

Broadcasting (offsets kept locally, not reset through the broker):

```csharp
consumer.MessageModel = MessageModel.Broadcasting;           // default is MessageModel.Clustering
```

POP mode (one instance shares consumer threads across queues, per-message ack, invisible time
extendable):

```csharp
consumer.PopMode = true;
consumer.PopInvisibleTime = 60_000;      // valid range 5000 ~ 300000
consumer.PopBatchNums = 32;
consumer.PopPollTimeMillis = 15_000;
consumer.PopThresholdForQueue = 96;
```

Common knobs: `ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset` (also
`ConsumeFromLastOffset` / `ConsumeFromTimestamp`), `SetConsumeThreadNums(n)`, `PullBatchSize`,
`PullThresholdForQueue` / `PullThresholdSizeForQueue` / `PullThresholdForTopic` (pre-pull flow
control), `MaxReconsumeTimes`, `SuspendCurrentQueueTimeMillis`,
`SetAllocateMessageQueueStrategy(new AllocateMessageQueueConsistentHash())`,
`Suspend()` / `Resume()` (stop pulling without going offline), `SetEnableMsgTrace(true)`,
`consumer.SubscribedTopics()` / `ConsumedCount` / `ClientId`.
Concurrency can be changed while running: `consumer.UpdateCorePoolSize(32)`, `GetCorePoolSize()`.

### PullConsumer (explicit pulls, you own the offsets)

```csharp
var pull = new DefaultMQPullConsumer("GID_PULL");
pull.SetNamesrvAddr("127.0.0.1:9876");
pull.Start();

foreach (MessageQueue mq in pull.FetchSubscribeMessageQueues("TopicTest"))   // every queue of the topic
{
    pull.FetchConsumeOffset(mq, out long offset);
    PullResult pr = pull.Pull(mq, "tagA", offset, maxNums: 32);              // short poll
    if (pr.IsFound) Deliver(pr.MsgFoundList);
    pull.UpdateConsumeOffset(mq, pr.NextBeginOffset);                        // the caller advances the offset
}

PullResult blocked = pull.PullBlockIfNotFound(mq, "*", offset, 32);          // long poll, blocks until a message or timeout
List<MessageQueue> mine = pull.FetchMessageQueuesInBalance("TopicTest");     // only the queues this instance owns
long off = pull.SearchOffset(mq, timestampMs);
Console.WriteLine($"{pull.MinOffset(mq)} {pull.MaxOffset(mq)} {pull.EarliestMsgStoreTime(mq)}");
pull.SendMessageBack(rejectedMsg, delayLevel: 3);                            // explicit send-back for redelivery
pull.CreateTopic("TBW102", "NewTopic", queueNum: 4);
pull.Shutdown();
```

Queue-change notifications: `pull.SetMessageQueueListener(listener)` or
`pull.RegisterMessageQueueListener(topic, listener)`
(`IMessageQueueListener.MessageQueueChanged(mqAll, mqDivided)`).
Pull-mode consumers send consumer heartbeats by default: `HeartbeatEnabled`, `HeartbeatBrokerIntervalMillis`.

### LitePullConsumer (poll and commit yourself, incl. topic queue-set change listener)

```csharp
var lite = new DefaultLitePullConsumer("GID_LITE");
lite.SetNamesrvAddr("127.0.0.1:9876");
lite.SetInstanceName("lite-demo");
lite.SetPollTimeoutMillis(1000);
lite.SetPullBatchSize(32);
lite.SetAutoCommit(false);
lite.SetAutoCommitIntervalMillis(5000);
lite.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
lite.Subscribe("TopicTest", "tagA");                       // subscribed mode: queues come from the allocation strategy
// lite.Assign(pull.FetchSubscribeMessageQueues("TopicTest"));  // assigned mode: you pin the queues
// lite.SetSubExpressionForAssign("TopicTest", "tagB");         // filter expression for assigned mode

lite.SetTopicMetadataCheckIntervalMillis(1000);            // queue-set comparison period (1s floor)
lite.RegisterTopicMessageQueueChangeListener("TopicTest", new QueueChangeListener());
lite.Start();

while (running)
{
    List<MessageExt> msgs = lite.Poll(3000);
    Handle(msgs);
    lite.Commit();                                         // persist what was actually consumed
}

lite.Seek(mq, 100); lite.SeekToBegin(mq); lite.SeekToEnd(mq);
lite.Commit(new Dictionary<MessageQueue, long> { [mq] = 200 });   // explicit offsets
long committed = lite.Committed(mq);
Console.WriteLine($"{lite.PullCursorOf(mq)} {lite.ConsumeCursorOf(mq)} {lite.PendingCommitOf(mq)}");
lite.Pause(new[] { mq }); lite.Resume(new[] { mq });
lite.Assignment(); lite.FetchMessageQueues("TopicTest"); lite.OffsetForTimestamp(mq, timestampMs);
lite.Shutdown();

sealed class QueueChangeListener : ITopicMessageQueueChangeListener
{
    // fires only when the topic's subscription queue set really changed (scale-out / scale-in)
    public void OnChanged(string topic, IReadOnlyList<MessageQueue> messageQueues) =>
        Console.WriteLine($"{topic} -> {string.Join(",", messageQueues.Select(mq => mq.QueueId))}");
}
```

Register the listener **after** `Start()` if you do not want an immediate first callback: only a
listener registered on a running consumer gets a baseline snapshot, so one registered while stopped
fires once on the first pass. Which queues *this instance* got is a separate interface:
`lite.SetMessageQueueListener(...)` (`ILiteMessageQueueListener.MessageQueueChanged(mqAll, mqDivided)`).
Each comparison pass asks the NameServer for the current route instead of reading the 30s route
cache, so scale in/out reaches the listener within seconds (see the `lite-queue-change` case).

### Admin

```csharp
using RocketMQ.Client;
using RocketMQ.Remoting.Protocol;

var admin = new DefaultMQAdminExt();                       // default instanceName is "ADMIN"
admin.SetNamesrvAddr("127.0.0.1:9876");
admin.SetTimeoutMillis(15000);
admin.Start();

TopicList topics = admin.FetchAllTopicList();
TopicRouteData route = admin.ExamineTopicRoute("TopicTest");
ClusterInfo cluster = admin.FetchBrokerClusterInfo();
TopicConfig config = admin.ExamineTopicConfig(brokerAddr, "TopicTest");
TopicStatsTable stats = admin.ExamineTopicStats("TopicTest");
ConsumeStats cstats = admin.ExamineConsumeStats(brokerAddr, "GID_DEMO", "TopicTest");
ConsumeStats byGroup = admin.ExamineConsumeStatsGroup("GID_DEMO");

admin.CreateTopic("TBW102", "NewTopic", queueNum: 4);
admin.CreateAndUpdateTopicConfig(brokerAddr, new TopicConfig("NewTopic2"));
admin.CreateAndUpdateSubscriptionGroupConfig(brokerAddr, new SubscriptionGroupConfig("GID_NEW"));
admin.DeleteTopic("NewTopic");
admin.DeleteSubscriptionGroup(brokerAddr, "GID_NEW", removeOffset: true);

SortedDictionary<MessageQueue, long> reset = admin.ResetOffsetByTimestamp("TopicTest", "GID_DEMO", timestampMs);
admin.ResetOffsetNew("GID_DEMO", "TopicTest", timestampMs);
admin.ResetOffsetByQueueId(brokerAddr, "GID_DEMO", "TopicTest", queueId: 0, resetOffset: 123);
admin.CloneGroupOffset(brokerAddr, srcGroup, destGroup, timestampMs, isClone: false);

List<MessageExt> byKey = admin.QueryMessage("TopicTest", key: "order-42", maxNum: 32, begin, end);
MessageExt one = admin.ViewMessage("TopicTest", msgId);
Console.WriteLine($"{admin.MinOffset(mq)} {admin.MaxOffset(mq)} {admin.EarliestMsgStoreTime(mq)}");
admin.SearchOffset(mq, timestampMs);
admin.SearchLowerBoundaryOffset(mq, timestampMs);         // boundary semantics: LOWER
admin.SearchUpperBoundaryOffset(mq, timestampMs);         // boundary semantics: UPPER

PropertyMap conf = admin.GetBrokerConfig(brokerAddr);
conf["flushDiskType"] = "ASYNC_FLUSH";
admin.UpdateBrokerConfig(brokerAddr, conf);
KvTable runtime = admin.FetchBrokerRuntimeStats(brokerAddr);
admin.WipeWritePermOfBroker("127.0.0.1:9876", "broker-a");
admin.Shutdown();
```

### ACL

```csharp
// simplest: every client and the admin expose the same method
producer.SetCredentials(accessKey, secretKey);
consumer.SetCredentials(accessKey, secretKey, securityToken);   // STS triplet

// or inject the hook yourself (must happen before Start())
producer.SetRpcHook(new AclClientRPCHook(new SessionCredentials(accessKey, secretKey)));
```

The signed content is the request's extension-field values concatenated in ordinal key order,
followed by the request body, then HMAC-SHA1 + standard Base64 (`AclClientRPCHook.BuildRequestContent`
/ `CalcSignature` / `HmacSha1Base64`; the `Signature` field itself is excluded). The hook chain order —
Namespace → stream → user hook — is rebuilt by
`RequestHooks.Compose(enableStreamRequestType, userHook, nsV2Getter)`, so the ACL hook you pass to
`SetRpcHook` runs last and its signature already covers the `ns` / `ReqT` fields.
Live case: `rmq acl` (needs `authenticationEnabled=true` on the broker, see `../scripts/run_acl_live.sh`).

### Namespaces

Two independent mechanisms, usable together:

```csharp
// 1) local resource-name prefix: resources go on the wire as <ns>%<resource>,
//    retry / DLQ topics get wrapped too
producer.Namespace = "myNs";
consumer.Namespace = "myNs";
lite.Namespace = "myNs";
pull.Namespace = "myNs";
string wrapped = NamespaceUtil.WrapNamespace("myNs", "TopicTest");   // "myNs%TopicTest"
string bare = NamespaceUtil.WithoutNamespace(wrapped, "myNs");       // "TopicTest"

// 2) server-side namespace: every request carries nsd=true / ns=<value>, resource names untouched
producer.NamespaceV2 = "my-ns-v2";
consumer.NamespaceV2 = "my-ns-v2";
pull.NamespaceV2 = "my-ns-v2";
lite.NamespaceV2 = "my-ns-v2";
admin.NamespaceV2 = "my-ns-v2";
```

The producer, all three consumers, the admin client and the trace dispatcher expose `NamespaceV2`.
The hook reads the property per request, so setting it after `Start()` takes effect from the next
request on.

### Compression

```csharp
producer.CompressMsgBodyOverHowmuch = 4 * 1024;    // only bodies above this size get compressed
producer.CompressLevel = 5;
producer.CompressType = CompressionType.ZSTD;      // CompressionType.ZLIB / LZ4 / ZSTD

// manual compress / decompress (the consumer decompresses automatically)
byte[] packed = CompressorFactory.Compress(body, CompressionType.ZLIB, level: 5);
byte[] original = CompressorFactory.Decompress(packed, CompressionType.ZLIB);
Console.WriteLine($"{CompressorFactory.HasZlibSupport()} {CompressorFactory.HasLz4Support()} {CompressorFactory.HasZstdSupport()}");
```

zlib comes from the built-in `ZLibStream` and is always available. LZ4 (LZ4 Frame) and ZSTD are
reached through P/Invoke against the system libraries (macOS `/usr/local/lib/liblz4.dylib`,
`libzstd.dylib`; Linux `liblz4.so.1`, `libzstd.so.1`); when the library is missing that backend
raises instead of silently passing uncompressed bytes through as if they were compressed.
Six queue allocation strategies are available: `AllocateMessageQueueAveragely`,
`AllocateMessageQueueAveragelyByCircle`, `AllocateMessageQueueByConfig`,
`AllocateMessageQueueConsistentHash`, `AllocateMessageQueueByMachineRoom`, `AllocateMachineRoomNearby`.

### TLS

```csharp
producer.TlsEnable = true;                    // or set ROCKETMQ_TLS_ENABLE=1
consumer.TlsEnable = true;
producer.TlsOptions = new TlsOptions
{
    CaCert = "/path/ca.pem",                  // trust anchor for strict verification
    ServerName = "127.0.0.1",                 // hostname / SAN to match
    ClientCert = "/path/client.pem",          // mTLS client certificate
    ClientKey = "/path/client.key",
};
```

Without `CaCert` the client runs in test-mode: it trusts the broker's self-signed certificate and
skips hostname checks. With `CaCert` it verifies strictly — the chain must reach that CA and the
hostname / SAN must match (the connection host by default, `ServerName` overrides it) — which
requires the broker to actually serve TLS. All three legs (plain / ca_verify / mtls) run with one
command: `bash ../scripts/run_csharp_tls_live.sh` (it starts its own certificate-backed cluster and
shuts it down again). Each connection has exactly one reader thread and one write lock serialising
the write side; that is the `SslStream` concurrency contract — do not add a second reader to a
connection.

## Features and Status

- ✅ Protocol layer: `RemotingCommand` frame codec; JSON and RocketMQ binary serialization; V2
  single-letter short-key headers; tolerant parsing of malformed JSON; 17-field and 6-field message codecs
- ✅ Transport: lazy TCP long connections over sockets, one reader thread per connection, framing,
  opaque→future dispatch; sync / async / oneway; partial-frame reassembly; timeouts and reconnects;
  in-flight requests failed immediately when the connection dies
- ✅ Sending: sync / pinned / selector / batch (320) / auto-batching / one-way / async (sender pool +
  fair semaphores) / transactions (two-phase + broker check-back) / delayed and timed /
  recall (`recallMessage`) / request-reply
- ✅ Consuming: Push (long polling + POP + orderly + broadcasting + offset persistence +
  startup range validation + pre-pull flow control + escape hatch for suspended listeners),
  Pull (with consumer heartbeats), LitePull (pull / consumed / pending-commit offset tables +
  queue-set change listener)
- ✅ Queue allocation: six pluggable strategies
- ✅ Namespaces: local prefix and server-side `nsd` / `ns`
- ✅ Admin: topics / subscription groups / cluster and routes / statistics / message queries /
  offset resets / `searchOffset` boundary semantics / broker config read-write
- ✅ Observability and security: message tracing (encoding + async dispatch + three hook kinds),
  consumer statistics, `ConsumerRunningInfo`(307), send / consume / transaction / Forbidden /
  FilterMessage hooks, ACL signing, dynamic NameServer addressing, fault-avoiding queue selection
- ✅ Compression: zlib / LZ4 / ZSTD; TLS: `SslStream`, test-mode plus strict verification / mTLS
- ✅ Client logging: size-based rotation, dual output to stderr and file
- ✅ 808 unit tests; 38 live-cluster subcommands in `rmq` (master/slave, fault injection,
  compression interoperability matrix)

## Client Logging

Line format:

```
2026-10-10 16:29:45.032 INFO  [3869] [main] [MqClient.cs:438] - MQClientInstance[...] started, namesrv=127.0.0.1:9876
```

That is `timestamp(ms) level [pid] [thread] [file:line] - message`, written to stderr and to the
file, flushed after every line.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` / `OFF` |
| `ROCKETMQ_CLIENT_LOG_FILE` | `$HOME/logs/rocketmqlogs/rocketmq_csharp_client.log` | `OFF` / `NONE` / empty ⇒ stderr only |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864` (64MB) | per-file cap, rotation by size; backups are `<file>.1` … `<file>.N`, uncompressed |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | number of backups kept |

Level and file path are resolved and cached on the **first log line written**, so the environment
variables must be set before anything logs. At runtime:

```csharp
using RocketMQ.Common;

ClientLog.SetLogLevel(LogLevel.Debug);
ClientLog.SetLogFile("/tmp/rmq-client.log");
ClientLog.SetLogFileLimits(maxSize: 8 * 1024 * 1024, maxIndex: 3);
ClientLog.SetThreadName("worker-1");        // internal threads are named: ConsumeMessageThread_N / AsyncSenderThread_N / RemotingClientReader-<addr>
```

Worker threads carry client-assigned names; unnamed threads fall back to `main` or `tid-xxxx`.

## Live Cluster Verification

Conventions: a NameServer on `9876` and a broker on `10911` with `autoCreateTopicEnable=true`; each
case asserts on itself and reports `[PASS]` / `[FAIL]` lines, exiting `0` when everything passes and
non-zero otherwise (`2` for unknown subcommand or missing arguments, `1` for an uncaught exception).
Uniform invocation:

```bash
dotnet run --no-build --project examples/RocketMQ.Examples -- <case> 127.0.0.1:9876
```

**Start the consumer before sending.** Create the topic first, then start the consumer and let it
finish its rebalance / heartbeat, and only then send. Reversing the order makes early messages land
on a not-yet-created default route or get skipped by `ConsumeFromLastOffset`, which shows up as an
intermittent "received nothing" false failure. Several cases already build the topic and wait; keep
that order when editing them.

One leg measured on this machine (queue-set change listener, real scale in/out with a 1s check period):

```bash
dotnet run --no-build --project examples/RocketMQ.Examples -- lite-queue-change 127.0.0.1:9876
# LitePull queue-change live (C#): PASS=10 FAIL=0     exit code 0
```

Subcommands:

| Subcommand | What it proves |
| --- | --- |
| `selfcheck` | Local protocol codec self-check, no cluster needed |
| `interop --emit` / `interop --decode <hex>` | Print / re-decode canonical frame hex (JSON and RocketMQ binary serialization) for byte-level comparison |
| `message-types` | 8 message capabilities: async / orderly / tag / user properties / delayed / key / transaction / batch (320 lands as N independent messages) |
| `admin-live` | Full admin path |
| `admin-batch-live` | Batch-style admin requests: batch create topics(18) / groups(225), read-permission ban(353), half-message resume(323), ordered topic config, cleanup calls(306/329/316), consume time span(303), nameserver config(318/319) |
| `compression-live selftest\|send\|recv <namesrv> <topic> <group> <size> [codec]` | Compression honesty: real send → consume → decompress → CRC; `send` / `recv` act as one end of `../scripts/compression_matrix.sh` |
| `redelivery` | Redelivery / DLQ / restart resume / orderly / broadcasting / flow control / rebalance / partial ack / stall self-heal / explicit ack rollback / offset fix on empty response |
| `acl <namesrv> [ak] [sk]` | ACL path (needs `authenticationEnabled=true`, see `../scripts/run_acl_live.sh`) |
| `pull` | Explicit pulls: manual pull / manual offsets / send-back |
| `pull-heartbeat <namesrv> <master> [slave]` | Pull-mode consumer heartbeats on a master/slave cluster (203/38 visible on both, 35 removes on unregister, ghost-group control) |
| `lite-pull` | Full LitePull path: subscribed and assigned modes + poll + six allocation strategies + three offset tables |
| `lite-pull-cursor` | Cursor follows `nextBeginOffset`, self-heals past OFFSET_ILLEGAL bounds |
| `lite-pull-code` | Request code 361 and the `litePullMessageEnable` switch (flipped at runtime and restored) |
| `lite-queue-change` | Topic queue-set change listener: 1s period plus real scale-out 2→4 / scale-in 4→2, proving the comparison pass queries the route |
| `rr` (alias `reqreply`) | Request-Reply: request / reply / concurrent correlation / timeout with no responder |
| `latency` | Send-latency fault avoidance: off / on / degraded isolation chain / recovery on expiry |
| `pop` | POP protocol: POP / ACK / invisible-time extension / resurfacing redelivery |
| `popc` | POP consumer side: consume loop / ack / delayed redelivery |
| `trace` | End-to-end message tracing (needs `traceTopicEnable=true`) |
| `hook` | CheckForbidden / FilterMessage hooks |
| `backpressure` | Async-send fair semaphores: message and byte gates, rejection accounting, runtime expansion |
| `flow-control` | The five pre-pull flow-control thresholds + startup numeric gates |
| `async-send` | Async send core: caller never blocks, thread accounting, no cross-talk, drain on `Shutdown` |
| `validators-live` | Name validation: local fast rejection + valid names round trip + addressing failure classification |
| `recall` | Timed message recall (enables and restores `recallMessageEnable` itself) |
| `unit-config` | `unitName` / `unitMode` / stream: clientId suffix, topic UNIT bit, `%RETRY%` UNIT_SUB bit |
| `send-header` | Send header c/d/n (template topic decides the auto-created queue count) |
| `sql92` | SQL92 filtering + CHECK_CLIENT_CONFIG(46) (needs `enablePropertyFilter=true`) |
| `unreg-live` | Unregistration on exit, UNREGISTER_CLIENT(35) |
| `fail-fast` | In-flight requests die immediately when the broker really dies (⚠ stops the broker once, restarts it, store kept) |
| `scheduled-intervals` | Scheduled task initialDelay / fixed rate (one round takes minutes) |
| `subscribe` | Subscribing after `Start()` pushes a heartbeat immediately and the new topic really gets consumed |
| `pinned-guard` | Pinned-send topic guard: real routes not falsely rejected, rejection happens locally and leaves no broker trace |
| `offset-illegal` | OFFSET_ILLEGAL: in-flight and buffered batch voided + corrected offset persisted immediately |
| `reset-offset` | 220 offset reset: persisted on the spot + in-flight batch voided + queues rebuilt from the new offset (restores `useServerSideResetOffset`) |
| `publish-route-master <namesrv> <master> [slave]` | Publish addresses honour the master only (⚠ stops the master once, master/slave cluster) |
| `clean-expired-msg` | The sweep escape hatch for a suspended listener (~4 minutes, waits for two sweep periods) |
| `tls <namesrv> <topic> <group> [plain\|ca_verify\|mtls]` | TLS transport stress + full TLS send/receive (see "TLS") |

Legs that need extra broker configuration: `trace` (`traceTopicEnable=true`), `sql92`
(`enablePropertyFilter=true`), `acl` (`authenticationEnabled=true`), `recall` / `lite-pull-code` /
`reset-offset` / `admin-batch-live` (these flip the switches themselves and restore them), the
`ca_verify` / `mtls` TLS legs (the broker must really serve TLS), and `fail-fast` /
`publish-route-master` / `pull-heartbeat` (master/slave plus stop-start, driven through
`../scripts/rmq_test_broker.sh`). POP and broadcasting need no extra switch — `pop`, `popc` and
`redelivery` run as-is.

## Repository Layout

```
csharp/
├── Directory.Build.props           # Nullable + TreatWarningsAsErrors, globally
├── RocketMQ.slnx                   # src / tests / examples
├── src/RocketMQ.Client/
│   ├── Common/                     # shared layer
│   │   ├── ByteBuffer.cs           #   ByteWriter/ByteReader + LegacyHash/LegacyNumber (string hashCode and overflow semantics used on the wire)
│   │   ├── UtilAll.cs              #   time / IP / CRC32 / hex (always explicit InvariantCulture)
│   │   ├── MixAll.cs  MessageConst.cs  SysFlag.cs (MessageSysFlag/PullSysFlag/PermName)
│   │   ├── Message.cs              #   Message/MessageExt/MessageBatch (reference semantics, cloned by the producer before sending)
│   │   ├── MessageDecoder.cs       #   17-field store format + 6-field batch codec
│   │   ├── Compression.cs          #   zlib / LZ4 / ZSTD backends; type bits 0/3 = ZLIB; unsupported types raise
│   │   ├── NativeCompression.cs    #   P/Invoke to system liblz4 (LZ4 Frame) / libzstd; backend raises when absent
│   │   ├── SubscriptionData.cs     #   FilterAPI.BuildSubscriptionData / Equals semantics
│   │   ├── NamespaceUtil.cs        #   wrap / unwrap the local <ns>%<resource> prefix
│   │   ├── BoundaryType.cs         #   timestamp→offset boundary semantics (LOWER/UPPER, lenient parsing)
│   │   ├── ConsistentHash.cs  TopicConfig.cs  TopicValidator.cs
│   │   └── ClientLog.cs            #   client log: size rotation + thread name + ms + file:line
│   ├── Remoting/
│   │   ├── RemotingClient.cs       # Socket TCP: lazy connect, reader per connection, framing, opaque→future dispatch, TLS
│   │   ├── TlsOptions.cs  RpcHook.cs (AclClientRPCHook / StreamTypeRPCHook / NamespaceRpcHook / RequestHooks)
│   │   ├── Exception.cs
│   │   └── Protocol/
│   │       ├── Json.cs             #   tolerant JSON: object keys inlined / bare numeric keys / NaN / trailing commas
│   │       ├── Serialize.cs        #   RemotingSerializable(JSON) + RocketMQSerializable(binary)
│   │       ├── Codes.cs            #   RequestCode/ResponseCode/LanguageCode
│   │       ├── Headers.cs          #   CommandCustomHeader + V1<->V2
│   │       ├── RemotingCommand.cs  #   JSON/ROCKETMQ serialization, header V1/V2
│   │       ├── Route.cs  Heartbeat.cs  Subscription.cs
│   │       └── Body.cs  AdminBody.cs   # admin response DTOs
│   └── Client/
│       ├── MqClient.cs             # MQClientInstance: route discovery + TBW102 fallback trimming, heartbeats, offset RPCs, Schedules (absolute-time task anchoring)
│       ├── Producer.cs             # DefaultMQProducer / TransactionMQProducer: sync/pinned/selector/batch/oneway/async/transaction
│       ├── Consumer.cs             # DefaultMQPushConsumer: pull loop, concurrent/orderly/POP, sendMessageBack
│       ├── PullConsumer.cs  LitePullConsumer.cs
│       ├── Admin.cs                # DefaultMQAdminExt
│       ├── ProduceAccumulator.cs   # auto-batching
│       ├── Result.cs               # SendResult/PullResult/listener interfaces/queue selectors
│       ├── RequestReply.cs  RecallMessageHandle.cs
│       ├── AllocateStrategy.cs     # six queue allocation strategies
│       ├── Hook.cs                 # Send/Consume/EndTransaction + CheckForbidden/FilterMessage hooks and contexts
│       ├── Trace.cs  TraceHook.cs  TraceDispatcher.cs  TraceParentContext.cs
│       ├── BackPressure.cs  Latency.cs  ConsumeExecutor.cs
│       ├── TopAddressing.cs        # dynamic NameServer addressing
│       ├── ConsumerStats.cs  Validators.cs  ClientException.cs
├── tests/RocketMQ.Client.Tests/    # xunit, 808 tests (codec/route/logging/transport/tracing/async/flow control/offsets…)
└── examples/RocketMQ.Examples/     # the rmq live-cluster tool (see above)
```

## License

Apache-2.0.
