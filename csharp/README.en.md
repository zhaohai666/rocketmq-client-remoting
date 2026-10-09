# RocketMQ C# Client (remoting protocol)
> [中文](README.md) | English

A C# implementation of Apache RocketMQ's classic remoting protocol (aligned with 5.x), covering
the full chain of producer / push and pull consumers / administration / message tracing /
transactions and scheduled messages. Works against RocketMQ 4.x / 5.x clusters; every capability
has been integration-validated on a real 5.5.1 cluster, and aligned item by item with the
Python / C++ / Rust implementations in this repository.

**Zero external NuGet dependencies** (BCL only); zlib uses `System.IO.Compression.ZLibStream`,
LZ4 / ZSTD use P/Invoke against **system libraries** (`/usr/local/lib/liblz4.dylib`,
`libzstd.dylib`; on Linux `liblz4.so.1` / `libzstd.so.1`) — when the library cannot be loaded,
that backend throws instead of silently passing compressed bytes through.

## Build / Test

```bash
cd csharp
dotnet build                              # the whole solution, 0 warnings (TreatWarningsAsErrors enabled globally)
dotnet test tests/RocketMQ.Client.Tests   # xunit, 789 tests
```

Requires the .NET 10 SDK.

## Quick Start

```csharp
using System.Text;
using RocketMQ.Client;

var producer = new DefaultMQProducer("PID_DEMO");
producer.NamesrvAddr = "127.0.0.1:9876";
producer.Start();
producer.Send(new Message("TopicTest", Encoding.UTF8.GetBytes("hello rocketmq")));
producer.Shutdown();

var consumer = new DefaultMQPushConsumer("GID_DEMO");
consumer.SetNamesrvAddr("127.0.0.1:9876");
consumer.Subscribe("TopicTest");
consumer.SetMessageListener(new DemoListener());
consumer.Start();
// ... after receiving the shutdown signal
consumer.Shutdown();


class DemoListener : IMessageListenerConcurrently
{
    public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context)
    {
        foreach (var msg in msgs)
            Console.WriteLine(Encoding.UTF8.GetString(msg.Body));
        return ConsumeConcurrentlyStatus.ConsumeSuccess;
    }
}
```

Note that two API shapes differ: the producer's name server is set by **property assignment**
(`producer.NamesrvAddr = ...`), while the consumers and the admin client use a method
(`SetNamesrvAddr(...)`).

## Feature Overview

| Area | Content |
| --- | --- |
| Protocol layer | `RemotingCommand` frame encode/decode; dual JSON / RocketMQ binary serialization; V2 single-letter short-key headers; tolerant parsing of fastjson2's invalid output; 17-segment + 6-segment message encoding/decoding |
| Transport layer | Lazy connection setup over Socket TCP long connections, one reader thread per connection, framing, opaque→future dispatch; sync / async / oneway; half-packet reassembly; timeouts and reconnection; in-flight requests fail immediately on connection loss |
| Sending | Sync / to-queue / queue selector / batch / one-way / async (a real async send pool + two fair back-pressure semaphores) / transactional messages (two-phase + broker check-back) / scheduled message recall (recallMessage) |
| Consuming | Push Consumer (long polling + POP + ordering + broadcasting + offset persistence + start-up numeric validation + five flow-control thresholds before pulling + an escape hatch for the suspended listener), Pull Consumer (with consumer heartbeat; `FetchSubscribeMessageQueues` returns the whole topic while `FetchMessageQueuesInBalance` returns only this instance's share), Lite Pull Consumer (three offset tables: pulled / consumed / in-memory) |
| Queue allocation | Six pluggable strategies: `AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<inner>` |
| Namespace | Two independent mechanisms: `Namespace` (a client-side resource prefix `%%ns%%res`, Java's `NamespaceUtil`) and `NamespaceV2` (the server-side namespace: `NamespaceRpcHook` stamps `nsd=true` / `ns=<value>` on every request; hook order Namespace → Stream → ACL, so both `ns` and `ReqT` land inside the ACL signature). The producer, all three consumers, the admin client and the trace dispatcher all expose `NamespaceV2` |
| Administration | `DefaultMQAdminExt`: topics / subscription groups / cluster info / various statistics / message queries / offset reset / `searchOffset` boundary semantics |
| Observability and security | Message tracing (encoding + asynchronous dispatch + hooks), consume statistics, `ConsumerRunningInfo`(307), send/consume/transaction/Forbidden/FilterMessage hooks, ACL signing, dynamic NameServer address retrieval, fault-avoiding queue selection |

## Real-Cluster Tooling

A local RocketMQ cluster is required (namesrv 9876 + broker 10911, `autoCreateTopicEnable=true`).
See the scripts under `../scripts/` for starting and stopping the cluster. Every tool self-asserts
and exits with a non-zero exit code on failure:

```bash
PROG=examples/RocketMQ.Examples/bin/Debug/net10.0/rmq.dll

dotnet $PROG selfcheck                    # local self-check (no cluster needed)
dotnet $PROG interop --emit               # print canonical frame hex (dual JSON/ROCKETMQ serialization); --decode <hex> decodes in reverse, for cross-language byte-level interop verification
dotnet $PROG message-types 127.0.0.1:9876   # 8 message capability classes (including batch 320 landing as 3 separate messages on the broker)
dotnet $PROG admin-live 127.0.0.1:9876    # full admin chain
dotnet $PROG compression-live selftest 127.0.0.1:9876    # compression authenticity: real send→consume→decompress→CRC
dotnet $PROG compression-live send|recv 127.0.0.1:9876 <topic> <group> <size> [codec]   # takes one side of the four-language matrix in ../scripts/compression_matrix.sh
dotnet $PROG trace 127.0.0.1:9876         # full message tracing chain (requires broker traceTopicEnable=true)
dotnet $PROG hook 127.0.0.1:9876          # CheckForbidden / FilterMessage hooks
dotnet $PROG backpressure 127.0.0.1:9876  # fair semaphores for async send back-pressure
dotnet $PROG async-send 127.0.0.1:9876    # async send kernel: thread accounting, offsetMsgId read back verbatim, batch async, Shutdown draining
dotnet $PROG fail-fast 127.0.0.1:9876     # in-flight requests fail immediately when the broker is really dead (⚠ stops the broker once, brings it back up afterwards, store is not deleted)
dotnet $PROG validators-live 127.0.0.1:9876   # name validation (including addressing fault triage: no address configured ⇒ 10004 fast rejection)
dotnet $PROG recall 127.0.0.1:9876        # scheduled message recall (toggles the switch automatically and restores recallMessageEnable on exit)
dotnet $PROG unit-config 127.0.0.1:9876   # unitName / unitMode / stream
dotnet $PROG send-header 127.0.0.1:9876   # the c/d/n fields of the send header (queue-count arithmetic for auto-created topics)
dotnet $PROG flow-control 127.0.0.1:9876  # the five flow-control thresholds before pulling + start-up numeric gates
dotnet $PROG sql92 127.0.0.1:9876         # SQL92 filtering + CHECK_CLIENT_CONFIG(46) (requires broker enablePropertyFilter=true)
dotnet $PROG scheduled-intervals 127.0.0.1:9876   # periodic task initialDelay / fixed rate (one round takes several minutes)
dotnet $PROG subscribe 127.0.0.1:9876     # immediate heartbeat after a late subscription
dotnet $PROG pinned-guard 127.0.0.1:9876  # pinned-send topic guard: real routing is not falsely rejected, rejection happens locally with no trace on the broker
dotnet $PROG offset-illegal 127.0.0.1:9876   # OFFSET_ILLEGAL: in-flight/buffered batches entirely invalidated + the corrected offset persisted immediately
dotnet $PROG reset-offset 127.0.0.1:9876  # 220 consumer offset reset: persisted on the spot + in-flight batches invalidated + queues rebuilt from the new offset (restores useServerSideResetOffset at the end)
dotnet $PROG pull-heartbeat 127.0.0.1:9876 127.0.0.1:10911 [slave address]   # heartbeat of the pull-mode consumer (master-slave cluster)
dotnet $PROG lite-pull 127.0.0.1:9876     # full lite-pull chain (including the six allocation strategies and three offset tables)
dotnet $PROG lite-pull-cursor 127.0.0.1:9876   # pull cursor following nextBeginOffset / out-of-range self-healing on OFFSET_ILLEGAL
dotnet $PROG lite-pull-code 127.0.0.1:9876   # request code 361 / the litePullMessageEnable switch (flips the switch at runtime and restores it)
dotnet $PROG publish-route-master 127.0.0.1:9876 127.0.0.1:10911 [slave address]   # publish addresses only accept the master (⚠ stops the master once, master-slave cluster)
dotnet $PROG clean-expired-msg 127.0.0.1:9876   # escape hatch for the suspended listener's cleanup (about 4 minutes, waiting for two cleanup cycles)
dotnet $PROG tls 127.0.0.1:9876 <topic> <group>   # TLS transport stress test + full TLS send/receive (see "TLS")
dotnet $PROG redelivery 127.0.0.1:9876    # redelivery/dead-letter/restart/ordering/broadcasting/flow control/rebalance/partial ack/stall self-healing/explicit ack rollback/offset correction on empty responses
dotnet $PROG unreg-live 127.0.0.1:9876    # producer exit unregisters via UNREGISTER_CLIENT(35)
```

Remaining sub-commands: `acl` (the ACL authentication chain, requires an authorization-enabled
broker; see `../scripts/run_acl_live.sh`), `pull`, `rr` (full request-reply chain), `latency`
(fault avoidance), `pop` / `popc` (the POP protocol and the consume loop), `reqreply`. Some cases
delete the topics they create (stated in the header comment of the script).

## Directory Structure

```
csharp/
├── Directory.Build.props           # global Nullable + TreatWarningsAsErrors + InvariantGlobalization hint
├── src/RocketMQ.Client/
│   ├── Common/                     # common layer
│   │   ├── ByteBuffer.cs           #   ByteWriter/ByteReader/JavaHash (the string hashCode semantics used on the broker side)
│   │   ├── UtilAll.cs              #   time/IP/CRC32/hex helpers (all explicitly InvariantCulture)
│   │   ├── MixAll.cs  MessageConst.cs  SysFlag.cs (MessageSysFlag/PullSysFlag/PermName)
│   │   ├── Message.cs              #   Message/MessageExt/MessageBatch (reference semantics, cloned by the Producer before sending)
│   │   ├── MessageDecoder.cs       #   17-segment storage format + 6-segment batch format encoding/decoding
│   │   ├── Compression.cs          #   zlib / LZ4 / ZSTD backends; type bits 0/3 = ZLIB; unsupported types must throw (no pass-through)
│   │   ├── NativeCompression.cs    #   P/Invoke into the system liblz4 (LZ4 Frame) / libzstd; that backend throws when the library is missing
│   │   ├── SubscriptionData.cs     #   FilterAPI.BuildSubscriptionData / Equals semantics
│   │   ├── BoundaryType.cs         #   boundary semantics for offset lookup by timestamp (LOWER/UPPER, with lenient parsing)
│   │   ├── TopicConfig.cs
│   │   └── ClientLog.cs            #   client log: size-based rollover + thread name + milliseconds + file:line
│   ├── Remoting/
│   │   ├── RemotingClient.cs       # Socket TCP: lazy connection setup, one reader thread per connection, framing, opaque→future dispatch
│   │   ├── Exception.cs
│   │   └── Protocol/
│   │       ├── Json.cs             # tolerant JSON: tolerates fastjson2's invalid output (inlined object keys / bare numeric keys / NaN / trailing commas)
│   │       ├── Serialize.cs        # RemotingSerializable(JSON) + RocketMQSerializable(binary)
│   │       ├── Codes.cs            # RequestCode/ResponseCode/LanguageCode
│   │       ├── Headers.cs          # 26 CommandCustomHeader types + V1<->V2
│   │       ├── RemotingCommand.cs  # dual JSON/ROCKETMQ serialization, header V1/V2
│   │       ├── Route.cs  Heartbeat.cs  Subscription.cs
│   │       └── Body.cs  AdminBody.cs   # admin response DTOs
│   ├── Client/
│   │   ├── MqClient.cs             # MQClientInstance: route discovery + TBW102 fallback trimming, heartbeats, offset requests
│   │   ├── Producer.cs             # DefaultMQProducer: sync/pinned/selector/async/one-way/batch/transaction
│   │   ├── Consumer.cs             # DefaultMQPushConsumer: pull loop, concurrent/orderly listeners, sendMessageBack
│   │   ├── LitePullConsumer.cs  PullConsumer.cs
│   │   ├── Admin.cs                # the full DefaultMQAdminExt suite
│   │   ├── Result.cs               # SendResult/PullResult/listener interfaces/queue selectors
│   │   ├── Hook.cs                 # Send/Consume/EndTransaction + CheckForbidden/FilterMessage hooks and contexts
│   │   ├── Trace.cs                # message trace model + text encoding/decoding
│   │   ├── TraceHook.cs            # three kinds of trace hooks (send / consume / end transaction)
│   │   └── TraceDispatcher.cs      # AsyncTraceDispatcher: async queue + grouping + 128K chunking + periodic flush
├── tests/RocketMQ.Client.Tests/    # xunit (codec/routing/logging/transport/trace/async/flow control/offsets... 30+ suites)
└── examples/RocketMQ.Examples/     # real-cluster tooling (see above)
```

## Client Log

Line format `2026-09-14 19:40:06.300 INFO  [pid] [thread name] [file:line] - msg`,
size-based FixedWindow rollover (by default 64MB × maxIndex 10), writing to both stderr and
`$HOME/logs/rocketmqlogs/rocketmq_csharp_client.log`.

**The file name is deliberately distinct from the other ports'** (Java `rocketmq_client.log`,
C++ `rocketmq_cpp_client.log`): the two ports use different rollover policies, so sharing one file
on the same machine would interleave lines between them, and after one port renames the file the
other port still holds the old fd, so subsequent log lines are silently written into the unlinked
inode.

The environment variables follow the same convention as the other ports:
`ROCKETMQ_CLIENT_LOG_LEVEL` / `ROCKETMQ_CLIENT_LOG_FILE` /
`ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` / `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX`;
in-process use `ClientLog.SetLogLevel/SetLogFile/SetLogFileLimits/SetThreadName`.

## TLS

With `TlsEnable = true` (or `ROCKETMQ_TLS_ENABLE=1`) every outgoing connection is replaced by an
`SslStream`; the handshake completes inside `AuthenticateAsClient` and the whole stream then runs
over TLS. Test mode (enabled by default) trusts the broker's self-signed certificate and does not
verify the hostname, so the local 5.5.1 cluster **needs no change to `useTLS`**: nameServer 9876
and broker 10911 sniff the first byte, and both plaintext and TLS are accepted on the same port.

The threading contract is the key constraint: `SslStream` supports only **one concurrent read + one
concurrent write** (in the runtime source `SslStream.IO.cs`, `_nestedRead` and `_nestedWrite` are
two separate `Interlocked` gates; only re-entry of the same kind throws `net_io_invalidnestedcall`,
reads and writes do not interfere with each other). Here each connection has exactly one reader
thread + one `Connection.WriteLock` serializing the write side, which lands squarely inside this
contract — the comment is written on the `WriteLock` in
`src/RocketMQ.Client/Remoting/RemotingClient.cs`, do not treat it as a lock you can casually
remove, and do not add a second reader to the same connection.

The transport-layer stress test of `dotnet $PROG tls <namesrv> <topic> <group>` (measured locally):
S0a creates a new TLS connection per round and sends a single request, 30 rounds with **0 losses**
(slowest round 110ms, including the handshake); S0b runs 16 threads concurrently for 320 requests
on a single TLS connection with **0 failures**, every response's opaque matched its request, total
elapsed 38ms. Afterwards a TLS producer + a TLS push consumer do 3 sends and 3 receives, and the
`traceparent` injected on the producing side is extracted on the consuming side and is valid.

## Unit Test Coverage

`dotnet test tests/RocketMQ.Client.Tests` → **789 passed / 0 failed**, zero warnings.
Main suites (each one is a behavioural assertion, not a snapshot test):

- **Protocol and codec**: `CodecTests` / `RouteHeartbeatTests` / `AdminBodyTests` / `LoggingTests` /
  `TraceTests` (trace text encoding/decoding + isolation of bad records) / `InteropTests`
- **Transport and failures**: `TransportTests` / `FailFastTests` (real sockets: peer disconnection
  judged dead within milliseconds, callback exactly once, in-flight requests claimed by connection
  object identity, Shutdown draining)
- **Sending**: `SendRetryTests` (an in-process mock cluster capturing real payloads: retry
  classification, send header c/d/n, pinned-topic guard),
  `ProducerAsyncTests` (the async chain: shared budget, retry on another broker, async-built
  headers, callback thread divergence),
  `BackPressureTests` (fair semaphore: head-of-queue semantics, capacity growth waking waiters,
  two lost-wakeup guards), `RecallMessageTests`,
  `ValidatorsTests` (name validation wording and code values), `AclTests` (signing + hook
  composition order + counter-proof)
- **Consuming**: `FlowControlTests` (the five thresholds' criteria and wording) /
  `ConsumerCheckConfigTests` (the 12 start-up ranges, each end tested once) /
  `ConsumeThreadPoolTests` (concurrency == core, default 20/20, shrinking at runtime takes effect) /
  `PullExpiredTests` (the 120s stall criterion and its wrap-up) /
  `OrderlyReconsumeTests` (the three orderly redelivery branches + explicit COMMIT/ROLLBACK) /
  `OffsetIllegalRecoverTests` / `ResetOffsetTests` / `PopConsumerTests` /
  `CleanExpiredMsgTests` / `SubscribeAfterStartTests` / `PullConsumerHeartbeatTests`
- **lite-pull and offsets**: `InitialOffsetTests` (real sockets against a fake cluster: the
  FIRST_OFFSET starting point is the literal 0, no `GET_MIN_OFFSET` is sent) /
  `LitePullCursorTests` (cursor following `nextBeginOffset`, braking of in-flight seeks,
  request code 361 with real payloads captured)
- **Addressing and identity**: `ConsistentHashTests` / `AllocateStrategyTests` (the criteria of the
  six strategies) / `ClientIdTests` (the clientId convention and the instance name) /
  `SearchOffsetBoundaryTests` (boundaryType as an upper-case enum name on the wire + real payloads
  captured) / `ScheduledIntervalsTests` (initialDelay first hop + fixed-rate anchoring)
- **Master-slave routing**: `PublishRouteMasterTests` (publish addresses accepting only the master:
  the address side and the subscription side, three branches)

## Implementation Conventions You Must Know

- **POP queues come from the client-side rebalance, not from broker assignment**: with
  `clientRebalance=false`, Java asks the broker through
  `RebalanceImpl#getRebalanceResultFromBroker:345` → `MQClientAPIImpl#queryAssignment:405`
  (QUERY_ASSIGNMENT=400, returning `MessageQueueAssignment` with mode=POP), so the broker decides
  which queues this instance owns. That path is **deliberately not implemented here** (the same
  decision as the other six ports): queues are still computed locally by the allocation strategy,
  then one POP loop + ack per queue. The semantics are equivalent — only WHO picks the queue set
  differs. Read the `DoRebalance` comment in `Client/Consumer.cs` before changing this.
- **Transactional messages are a complete two-phase flow**: half message (TRAN_MSG/PGROUP + sysFlag
  `TRANSACTION_PREPARED`) → local transaction → `END_TRANSACTION(37, oneway)` → when the broker
  checks back via `CHECK_TRANSACTION_STATE(39)`, `CheckLocalTransaction` is called back and an
  END_TRANSACTION is sent back. The producer periodically sends heartbeats to the broker (including
  ProducerData) — **the broker's transaction check-back depends on them**; do not cut producer
  heartbeats down as if they were a dispensable decoration.
- **Async sending has a real send pool**: the caller thread passes the back-pressure gate →
  `AsyncSenderExecutor_N` (core==max==number of CPU cores, bounded queue of 50000) runs the
  preparation section (validation, compression, queue selection, request building, the Forbidden
  hook, the Send hook before, the request built only once) → the transport layer's `InvokeAsync` →
  failures follow the retry chain (bounded by `retryTimesWhenSendAsyncFailed`, each round switching
  broker and using a new opaque; if the broker explicitly replied with an error, do not switch
  machines) → on `NettyClientPublicExecutor_N` run the Send hook after + return the permits + the
  user callback (exactly once). Failures **before** the request is handed to the transport layer
  (the gate, queueing beyond the budget, validation, missing route) call back in place on the
  thread they are currently on.
- **`Shutdown()` drains the send pool**: it first rejects new requests, then waits for both pools to
  drain, ensuring every submission it accepted runs its preparation section and hands its payload to
  the transport layer. ⚠ The guarantee **stops at** "handed to the transport layer": if the client is
  closed immediately afterwards, the submissions whose responses have not come back get no terminal
  callback — if you need a callback for every submission, the caller must wait for completion before
  closing.
- **The back-pressure gate is a hand-written fair semaphore**: `SendAsync` first takes 1 slot by
  message count on the **caller thread**, then N slots by **pre-compression** body bytes, sharing the
  same `timeout` budget; if it cannot acquire, it calls back
  `send message tryAcquire semaphoreAsyncNum/Size timeout`, and permits are returned in the order
  "size first, then num" **before** the result is given to the user. Changing capacity shifts the
  absolute values **in place** and wakes waiters along the way (waiters are never left stranded);
  when the built-in `AsyncSenderExecutor` queue is full, with back-pressure on the submission runs to
  completion in place (so the deducted permits are returned), with back-pressure off it throws
  `MQClientException("executor rejected")`.
- **The request code for batch sending is 320**: first `IsReplyMessage` ⇒ 325, then `msg.IsBatch` ⇒
  320, otherwise `SendMessageV2(310)`. The request code and the `m`(batch) field of the V2 header are
  two different things and must be evidenced as a pair.
- **The c/d/n fields of the send header follow routing/configuration**: `c`=CreateTopicKey (default
  `TBW102`), `d`=DefaultTopicQueueNums (default 4; the broker auto-creates the topic with
  `min(d, template queue count)`), `n`=the broker name this request lands on; when brokerName is
  explicitly given as empty, the whole `n` key does not go on the wire.
- **unitMode / stream are on-the-wire fields, not local decorations**: sending with `unitMode=true`
  makes the auto-created topic carry the UNIT bit; the `ConsumerData.unitMode` in the consumer
  heartbeat makes `%RETRY%group` carry the UNIT_SUB bit; `unitName` participates in the clientId and
  in dynamic address retrieval. The `ReqT` value in ExtFields is `"0"`; the enum name `@STREAM`
  appears only at the tail of the clientId. The transport layer has only **one slot** for a request
  hook, and the order is restored by `RequestHooks.Compose(enableStream, userHook)` — stream must be
  injected **before** ACL (`ReqT` has to be part of the signed content), and hooks must be registered
  **before** the instance's `Start()`.
- **The clientId convention**: `<local IP>@<instanceName>[@<unitName>][@STREAM]`; when instanceName is
  still the default `DEFAULT`, `Start()` replaces it in place with `<pid>#<nanoTime>` — unconditionally
  for the producer and the admin client, and for the three consumers only under `CLUSTERING` (the
  broadcasting consumer keeps `DEFAULT`).
- **Periodic tasks use an initialDelay first hop + fixed-rate anchoring**: each hop is computed against
  the same absolute timeline, errors do not accumulate, and when behind schedule it runs immediately to
  catch up. The first offset-persistence hop happens at 10s (not immediately, and not
  initialDelay + one period). ⚠ Do not use an implementation of "sleeping N seconds in 100ms slices":
  on macOS `ManualResetEventSlim.Wait(100ms)` was measured to give one extra tick, stretching every
  period by ~30%; use `Schedules.WaitUntil(...)` uniformly to sleep once until the absolute planned
  time, and the whole wait can still be woken immediately by `Shutdown`.
- **Tolerant JSON**: admin response parsing tolerates fastjson2's invalid output (inlined object keys
  / bare numeric keys / NaN / trailing commas); one wrong character in a key name silently parses into
  an empty container — admin DTO field names must match the on-the-wire payload character for character.
- **Message trace decoding is more robust than upstream**: for messages without keys, a missing
  SubBefore segment is taken as an empty string (upstream would hit an array index out of bounds), and
  a **single bad record** skips only itself rather than destroying the decoding of the whole trace
  message.
- **ClientLog**: backup files are not compressed, and writes are synchronous.
- **The heartbeat fingerprint is fixed at 0**: the V1 full-registration path is used; the V2
  fingerprint, which depends on field order, is not implemented.
- **The TBW102 fallback is enabled only on the send path**: the second hop of
  `GetTopicPublishInfo(topic, isDefault: true)` matches the semantics of `tryToFindTopicPublishInfo`
  in the send chain; admin queries never receive a fallback route.

## License

Apache-2.0.
