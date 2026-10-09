# PHP Client (RocketMQ 4.x/5.x classic remoting protocol)

> [中文](README.md) | English

`php/` is the seventh client in this repository: the same layering as `python/` and the same protocol
semantics, **without depending on any composer package**,
using only `ext-json` / `ext-sockets` / `ext-openssl` / `ext-mbstring`. The conventions that must be
obeyed during porting (naming, wire format, single-thread adaptation, duplicate-class-name guards) are
written in [`PORTING.md`](PORTING.md), and the item-by-item comparison of the capability gaps against
Java is written in [`../php-vs-java-client-diff.md`](../php-vs-java-client-diff.md).

- Runtime requirement: PHP **8.1+** (measured as 8.1.34 on the development machine; the 8.3 in `PORTING.md` is the target baseline, the code uses no 8.2+ syntax)
- Size: `src/` 69 files / about 30,000 lines; `tests/` 14 files / about 12,000 lines
- Wire protocol: **dual JSON and ROCKETMQ binary** serialization of `RemotingCommand`; messages use the **17-segment storage format + 6-segment batch format**
- Real-cluster baseline: RocketMQ **5.5.1** (NameServer 9876 + Broker 10911)

## Single-thread model (the mental model to build before reading the code)

PHP has no threads, so all the work that background threads carry in the other ports — heartbeats,
rebalance, offset flushing and the rest — collapses into a **caller-driven `tick()`**:

| Thread in the other ports | Form in this port |
| --- | --- |
| Heartbeat 30s / rebalance 20s / offset flush 10s(+5s) / ordering lock 20s | Executed by due time inside `PushConsumer::tick()`; 2s fast retry while nothing is allocated during startup |
| Per-queue long-polling thread | **Short polling** (`suspend=false`) + backoff on empty results by `pullIntervalMillis` + at most `maxPullsPerQueuePerTick` rounds per queue per tick |
| Consumption suspension (Java's `submitConsumeRequestLater` sleep) | A `suspendedUntil` table; redelivery happens only once the deadline is reached |
| POP's pollTime long polling | pollTime=0 short polling + inline execution of the batch |
| Who decides the POP queue set | Java asks the broker when `clientRebalance=false` (`QUERY_ASSIGNMENT(400)` returning `MessageQueueAssignment` with mode=POP, `RebalanceImpl#getRebalanceResultFromBroker:345`); this port **always uses the client-side rebalance**: queues come from the local allocation strategy, then one POP loop + ack per queue. Semantically equivalent, differing only in who picks the queue set — the same deliberate decision across all seven ports |
| The callback thread of `invokeAsync` | Non-blocking write + pending table (opaque→callback), `waitResponses()` pumps with `stream_select` |

> **Why consumers must drive `tick()` themselves**: a PHP process has no resident thread that can run
> in the background, so any "wait for the next heartbeat" semantics left in a thread would never happen.
> The caller looping `tick()` is this port's only entry point for rebalance/offset/heartbeat
> — the real-cluster scripts (`examples/live_*.php`) are all written in this pattern, and integrators
> only have to copy it.

## Quick start

```php
<?php
declare(strict_types=1);
require __DIR__ . '/bootstrap.php';   // PSR-4 + one-off classmap fallback, no composer needed

use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Common\Message;

$producer = new DefaultMQProducer('PID_DEMO');
$producer->setNamesrvAddr('127.0.0.1:9876');
$producer->start();
$producer->send(new Message('TopicTest', 'hello php'));
$producer->shutdown();

class DemoListener extends MessageListenerConcurrently
{
    public function consumeMessage(array $msgs, $context): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            fwrite(STDOUT, $m->getBody() . PHP_EOL);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

$consumer = new DefaultMQPushConsumer('GID_DEMO');
$consumer->setNamesrvAddr('127.0.0.1:9876');
$consumer->subscribe('TopicTest', '*');
$consumer->setMessageListener(new DemoListener());
$consumer->start();
while (true) {          // caller-driven: in this port the "background thread" is this loop
    $consumer->tick();
    usleep(100_000);
}
```

## Capability coverage

The same capability surface as `python/` / `go/` / `nodeJs/`, and the item-by-item comparison is in
[`../php-vs-java-client-diff.md`](../php-vs-java-client-diff.md):

| Area | Contents |
| --- | --- |
| Protocol / transport | `RemotingCommand` dual serialization (JSON + ROCKETMQ binary), 17-segment and 6-segment message encoding/decoding, V2 single-letter short keys, sync/async/oneway, opaque matching, half-packet reassembly, GO_AWAY connection-switch resend, TLS |
| Sending | Sync / pinned (specific-queue) / queue selector / batch (incl. `ProduceAccumulator` accumulation + two-level fair backpressure) / oneway / async / transactions (two-phase + broker check-back 39) / timed recall (370, `RecallMessageHandle`) / Request-Reply (325 send + 326 push-back + waiting-slot TTL scan) |
| Consuming | Push (concurrent / orderly / broadcasting / a short-polling implementation of the long-polling semantics / offset persistence / numeric validation at startup / the five flow-control thresholds before pulling), Pull (`fetchSubscribeMessageQueues` returns the whole topic, `fetchMessageQueuesInBalance` returns only this instance's share), LitePull, POP (200050/200051/200052 + checkpoints), `ConsumeMessageDirectly`(309) |
| Queue allocation | `AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY`, driven by real rebalance |
| Namespace | Two independent mechanisms: `namespace` (a client-side resource prefix `%%ns%%res`, `Common/Namespace.php`, wrapped and unwrapped across send/consume/heartbeat/offset) and `namespaceV2` (the server-side namespace: `NamespaceRpcHook` in `Remoting/RpcHooks.php` stamps `nsd=true` / `ns=<value>` on every request; hook order Namespace → Stream → ACL, so both `ns` and `ReqT` sit inside the ACL signature; when the value is empty not a single field is written). The producer, all three consumers and the admin client expose `namespaceV2`, read live per request rather than snapshotted at start-up |
| Admin | topic / subscription-group CRUD, cluster and broker runtime information, consume stats and progress, query messages by key / uniqKey / msgId, offset read and reset, `searchOffset` boundaries, name server config 318/319 |
| Observability | Message tracing (the three kinds production/consumption/check-back + an asynchronous dispatcher + byte-exact encoding of Java's `TraceDataEncoder`), consume stats, `ConsumerRunningInfo`(307), the five kinds of hooks, **`OpenTracingHook`** (Java's OpenTracing trio is implemented in this port under the same interface names, while the other ports substitute W3C `traceparent`), W3C `traceparent` injection and pass-through |
| Security and addressing | ACL signing (`HmacSHA1`, the signed content is byte-identical to Java's), dynamic name server addressing (HTTP domain + `NsAddr` file), fault-avoiding queue selection (`Latency.php`), `Validators` local-validation pre-check |
| Compression | All three `MessageSysFlag` type bits are present: ZLIB (`gzcompress`/RFC1950), LZ4 (**a pure-PHP LZ4 Frame-format implementation**, the same wire as Java `lz4-java` / Python `lz4.frame` / the lz4 CLI), ZSTD (the external `zstd` CLI is preferred for real compression, and when the CLI is missing it falls back to a pure implementation of Raw/RLE frames); threshold 4096, protection against double compression, the type bit is cleared after decompression, and **anything that cannot be decompressed always throws an exception — a compressed stream is never passed through as the body** |

## Directory structure

```
php/
├── bootstrap.php              PSR-4 autoloader + the entry point of the duplicate-class-name static guard (no composer needed)
├── composer.json              declares only ext-* and the autoload mapping, pulls in no dependency
├── PORTING.md                 porting conventions (naming / wire format / single-thread adaptation / class-name uniqueness)
├── src/
│   ├── Common/                message model, encoding/decoding, compression (CompressionCodec), namespace, constants, validation
│   ├── Remoting/              RemotingClient, RpcHooks, Protocol/ (frames, the two serializations, request/response headers and bodies)
│   └── Client/                Producer / PushConsumer / PullConsumer / Admin / allocation strategies /
│                              tracing / Latency / Backpressure / Logger / Exceptions
├── examples/                  real-cluster verification tools (live_*, failures always exit with a non-zero code)
└── tests/                     offline self-tests (run_all.php chains the suites together)
```

## Offline self-test

```bash
cd php
php tests/run_all.php          # all suites, measured 1590 checks all green
php tests/RunCommon.php        # a single suite (RunRemoting / RunClient* / RunCommon)
```

Besides asserting suite by suite, `run_all.php` also runs a **duplicate-class-name guard**: PHP has no
module isolation, so when the same FQCN is declared in two files, the one autoloaded first wins and the
other silently stops taking effect, hence this step cannot be skipped (the renaming rules are in
`PORTING.md`; for example `TraceContextPropagator` exists precisely to avoid the class name occupied by
Java's `trace.TraceContext`).

## Real-cluster verification

```bash
bash scripts/run_php_live.sh admin            # assert the admin operations item by item
bash scripts/run_php_live.sh redelivery       # redelivery / dead letter / orderly dead letter (legs=all|s1,s2,s3,s4)
bash scripts/run_php_live.sh pull             # pull consumer: queues / fetchMessageQueuesInBalance /
                                              # manual pull / offset commit and read-back (legs=all|s1,s2,s3)
bash scripts/run_php_live.sh pop              # POP consumption (needs POP-specific broker configuration, included in the script)
bash scripts/run_php_live.sh tls              # the plain_tls / ca_verify / mtls three legs
bash scripts/run_php_live.sh request_reply    # the 326 push-back chain
bash scripts/run_php_live.sh compression      # this end's zlib/lz4/zstd self-test smoke

# cross-language compression matrix (this end's php_* legs, compressing and decompressing against the other six ends)
bash scripts/compression_matrix.sh zlib       # lz4 / zstd use the same script with the codec swapped
```

`run_php_live.sh` brings up / reuses the 5.5.1 cluster by itself, and **only stops the instance it
started**; `pop` and `tls` need an exclusive cluster (process-level global switches), so do not run them
in parallel with the real-cluster scripts of the other ends.

## Logging

`RocketMQ\Client\Logger` **writes to a file by default** (matching the Java client's behaviour of
generating `rocketmq_client.log`), with the same set of environment-variable conventions as the other
ports:

| Environment variable | Default | Description |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` |
| `ROCKETMQ_CLIENT_LOG_DIR` | **`<current working directory>/logs/rocketmqlogs`** | log directory |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_php_client.log` | file name; an empty string / `OFF` / `NONE` = disable writing to a file; if it contains a path separator the whole value is treated as a path |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | empty | any non-empty value = write to stderr only |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864` (64MB) | rotate by size, same value as Java logback; `0` = no rotation |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | number of backups, backup names `<file>.1` … `<file>.N` |

**Why we do not write to `$HOME/logs/rocketmqlogs` as Java does**: PHP clients usually run inside
somebody else's host process / CLI script, so quietly creating a directory and writing files under the
user's HOME is an out-of-bounds side effect; landing in the current working directory follows the
deployment and is naturally predictable inside a container (the same trade-off as
`python/rocketmq_logging.py`). For the Java convention, set
`ROCKETMQ_CLIENT_LOG_DIR=$HOME/logs/rocketmqlogs` explicitly.

The file name is also deliberately not `rocketmq_client.log`: when the Java client runs on the same
machine at the same time, both sides interleave lines into one file, and whichever side rotates first
renames the other side's file, while the JVM still holds the old fd, so subsequent logs are silently
written into an already unlinked inode.

It can be changed directly in the program: `Logger::setLevel()` / `Logger::setHandler()` (inject a
callable, for unit tests) / `Logger::logFilePath()` (the assertion point for diagnostics and for the
real-cluster scripts' "was a file actually written").

> Local environment noise: every `php` process start prints two lines of
> `[CQ_POLLER] Background thread created/started`
> (an auto_prepend of the host php.ini, unrelated to this client), so exclude it first when inspecting logs.

## TLS

Once `setTlsEnable(true)` is turned on, every outbound connection goes over TLS, and as in Java this is
a **process-level** switch (NameServer connections also go over TLS). Certificate conventions: CA
verification (`caCertPath` / `serverName`), optional mutual authentication (`clientCertPath` /
`clientKeyPath`); hostname and certificate-chain validation is strict by default, and revocation
checking is consistent with the other ports. `scripts/run_php_live.sh tls`
uses `openssl` to create the CA / server(SAN:127.0.0.1,localhost) / client certificates on the spot and
runs the three legs, where "a plaintext connection to a TLS port must be rejected" is a mandatory
control leg.
