# The PHP Implementation of the RocketMQ Remoting Client

> [中文](README.md) | English

## Overview

This directory is the **PHP client SDK for Apache RocketMQ's classic remoting protocol
(4.x/5.x)**: it talks directly to NameServer + Broker, with no proxy involved.

- Requires **PHP >= 8.1** (measured on 8.1.34). **Zero composer and zero third-party
  dependencies** — only `ext-json` / `ext-sockets` / `ext-openssl` / `ext-mbstring`.
  PSR-4 autoloading plus a full classmap fallback in `bootstrap.php`: one `require` and
  the whole SDK is usable.
- PHP has no resident threads, so heartbeats, rebalance, offset persistence and trace
  dispatch all collapse into a **caller-driven `tick()`** invoked from your own loop
  (see the execution-model note in "Examples").
- Verified against a real RocketMQ **5.5.1** cluster (NameServer 9876 + Broker 10911).
- Size: `src/` 69 files / 30,642 lines; `tests/` 15 files / 12,831 lines.

## Prerequisites

- PHP **8.1+** with the `json`, `sockets`, `openssl` and `mbstring` extensions enabled.
- A reachable NameServer + Broker pair (or reuse the local test cluster managed by
  `scripts/run_php_live.sh`).
- Optional: the `zstd` command-line tool. ZSTD compression prefers the `zstd` CLI for
  real compression and automatically falls back to a built-in pure-PHP Raw/RLE frame
  implementation when the CLI is missing, so nothing breaks. LZ4 needs no CLI at all.

## Getting Started

The SDK is integrated from source: drop `php/` into your project and `require` its
`bootstrap.php`; `composer.json` only declares the extensions and the autoload mapping,
so `composer install` is not a required step.

```php
<?php
require __DIR__ . '/php/bootstrap.php'; // no composer needed; one include and you are set
```

Everyday development commands (run inside `php/`):

```bash
cd php

# syntax check of a file after modifying it
php -l src/Client/Producer.php

# offline self-test entry: static guard + 10 suites run sequentially, no cluster needed
php tests/run_all.php

# a single suite
php tests/RunCommon.php
php tests/RunRemoting.php

# compression smoke (needs a cluster; one php-to-php leg over zlib/lz4/zstd)
bash scripts/run_php_live.sh compression 127.0.0.1:9876
```

`tests/run_all.php` is the **offline** suite: before running anything it performs a
repository-wide **duplicate-class-name static guard** (PHP has no module isolation, so
when the same FQCN is declared in two files the one autoloaded first silently wins and
the other dies — this guard cannot be skipped), then chains 10 suites. Measured today:
**guard CLEAN, all 1,674 assertions across the 10 suites pass**; any failure exits with a
non-zero code.

## Examples

### The execution model: caller-driven `tick()`

Since PHP has no threads, the periodic work that background threads carry in other
deployment shapes converges into **`tick()` on the consumer object**: heartbeats,
rebalance, offset persistence, pull rounds and trace dispatch all run inside `tick()`,
each at its own due time. So a consumer process must run its own main loop:

```php
while (running()) {
    $consumer->tick();   // the only entry point for rebalance / offsets / heartbeats
    usleep(100_000);
}
```

**Note: `poll()` does not call `tick()`.** `poll()` only drains the local buffer and
advances the consumed cursor; the pulls that fill the buffer, queue-set changes,
heartbeats and offset commits all happen inside `tick()`. For LitePull / Push consumers
you must therefore **`tick()` and then `poll()` every round** (or alternate `tick()`
with your business handling) — polling without ticking starves the consumer on a stale
buffer. Every live script in `examples/live_*.php` follows this pattern; copy it.

### Producer

```php
<?php
declare(strict_types=1);
require __DIR__ . '/bootstrap.php';

use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\SendResult;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageQueue;

$producer = new DefaultMQProducer('PID_DEMO');
$producer->setNamesrvAddr('127.0.0.1:9876');
$producer->start();

// normal message (tags / keys supported: new Message(topic, body, tags, keys))
$result = $producer->send(new Message('TopicTest', 'hello php'));
echo $result->getMsgId(), PHP_EOL;
```

**Ordered messages** — pin to one queue, or pick a stable queue per key with a selector:

```php
$producer->send($msg, timeoutMillis: 3000, mq: $messageQueue);          // pinned
$producer->sendBySelector($msg, new SelectMessageQueueByHash(), $orderId); // hash by arg
```

**Delayed / timed messages** — `DELAY` uses delay levels, `TIMER_*` uses timer messages:

```php
$msg->setDelayTimeLevel(3);                       // delivered after 10s (delay level 3)
$msg->putProperty('TIMER_DELAY_SEC', '60');       // delivered after 60 seconds
$msg->putProperty('TIMER_DELAY_MS', '60000');
$msg->putProperty('TIMER_DELIVER_MS', (string) (time() + 60) * 1000);
```

**Batch / auto-accumulation / oneway / async**:

```php
$producer->send([new Message('TopicTest', 'a'), new Message('TopicTest', 'b')]); // batch

$producer->sendOneway(new Message('TopicTest', 'fire-and-forget'));

$producer->sendAsync(new Message('TopicTest', 'hi'), new class implements SendCallback {
    public function onSuccess(?SendResult $r): void { /* ... */ }
    public function onException(\Throwable $e): void { /* ... */ }
});
```

With `setAutoBatch(true)`, `send()` / `sendAsync()` go through the `ProduceAccumulator`
for automatic batching, governed by the two-level fair backpressure knobs
(`setEnableBackpressureForAsyncMode` / `setBackPressureForAsyncSendNum` /
`setBackPressureForAsyncSendSize`).

**Transactional messages**:

```php
use RocketMQ\Client\LocalTransactionState;
use RocketMQ\Client\TransactionListener;
use RocketMQ\Client\TransactionMQProducer;
use RocketMQ\Common\MessageExt;

$tx = new TransactionMQProducer('PID_TX');
$tx->setTransactionListener(new class implements TransactionListener {
    public function executeLocalTransaction(Message $msg, mixed $arg): LocalTransactionState
    {
        return LocalTransactionState::COMMIT_MESSAGE;   // or ROLLBACK_MESSAGE / UNKNOW
    }
    public function checkLocalTransaction(MessageExt $msg): LocalTransactionState
    {
        return LocalTransactionState::COMMIT_MESSAGE;   // decides when the broker checks back
    }
});
$tx->start();
$tx->sendMessageInTransaction($msg);
```

**Request-Reply** — the requester calls `request()`, the responder calls `reply()` inside
its consumption callback. Since PHP is single-threaded, run the responder and the
requester as two processes (exactly what `examples/live_request_reply.php` orchestrates):

```php
// responder: a PushConsumer receives the request, then its own producer writes the
// reply back onto the requester's connection
$response = $requester->request($msg, 10_000);   // requester: blocks until the reply Message

// inside the responder's listener:
$replyProducer->reply($requestMsg, 'reply-body');
```

**Recalling a timed message (recallMessage)** — only timed messages carry a handle:

```php
$res = $producer->send($timerMsg);
if ($handle = $res->getRecallHandle()) {
    $producer->recallMessage('TopicTest', $handle);
}
```

### PushConsumer

```php
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerConcurrently;

class DemoListener implements MessageListenerConcurrently
{
    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            fwrite(STDOUT, $m->getBody() . PHP_EOL);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

$consumer = new DefaultMQPushConsumer('GID_DEMO');
$consumer->setNamesrvAddr('127.0.0.1:9876');
$consumer->subscribe('TopicTest', '*');            // second argument: tag / SQL92 expression
$consumer->setMessageListener(new DemoListener());
$consumer->start();
while (true) {
    $consumer->tick();                              // heartbeats / rebalance / pulls / offsets
    usleep(100_000);                                // all happen right here
}
// $consumer->shutdown(); before exiting
```

For orderly consumption use `MessageListenerOrderly` / `ConsumeOrderlyStatus`; broadcasting,
start position and the numeric knobs are configured through `setMessageModel` /
`setConsumeFromWhere` / `setConsumeThreadMin` and friends. `suspend()` / `resume()` /
`isPaused()` pause and resume delivery; while suspended, `tick()` delivers nothing.

### PullConsumer (DefaultMQPullConsumer)

Pull mode: you control the queues and the offsets yourself. `registerTopic()` must be
called **before** `start()` (the heartbeat subscription set only carries the topic once
registered); read committed offsets back with `fetchConsumeOffset()`.

```php
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\PullStatus;

$pulled = new DefaultMQPullConsumer('GID_PULL');
$pulled->setNamesrvAddr('127.0.0.1:9876');
$pulled->registerTopic('TopicTest');
$pulled->start();

$mqs = $pulled->fetchSubscribeMessageQueues('TopicTest');      // all queues of the topic
$mine = $pulled->fetchMessageQueuesInBalance('TopicTest');     // only this instance's share
foreach ($mine as $mq) {
    $pulled->tick();
    $result = $pulled->pull($mq, '*', offset: 0, maxNums: 32);
    if ($result->status === PullStatus::FOUND) {
        $pulled->updateConsumeOffset($mq, $result->nextBeginOffset);
    }
}
```

### LitePullConsumer (DefaultLitePullConsumer)

`subscribe()` (automatic rebalance) or `assign()` (explicit queues); `poll()` drains the
local buffer and `commit()` submits offsets; `seek()` rewinds the cursor. **The main loop
must `tick()` + `poll()` every round**: `poll()` will not drive `tick()` for you.

```php
use RocketMQ\Client\DefaultLitePullConsumer;

$lite = new DefaultLitePullConsumer('GID_LITE');
$lite->setNamesrvAddr('127.0.0.1:9876');
$lite->subscribe('TopicTest');

// topic queue-set change listener: the comparison round queries routes live and only
// fires the callback when the set really changed
$lite->setTopicMetadataCheckIntervalMillis(1000);
$lite->registerTopicMessageQueueChangeListener('TopicTest',
    function (string $topic, array $mqs): void {
        fwrite(STDOUT, "$topic -> " . count($mqs) . " queues\n");
    });

$lite->start();
while (true) {
    $lite->tick();                        // the due round does comparison / heartbeat / offsets
    foreach ($lite->poll(200) as $m) {    // only reads the buffer; never calls tick()
        // ... handle ...
    }
    usleep(100_000);
}
```

Offsets commit automatically by default (`autoCommit`); you can also call `commit()` or
`commit($offsets, persist: false)` manually, and `pause()` / `resume()` per queue.

### Admin

```php
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Common\TopicConfig;

$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr('127.0.0.1:9876');
$admin->start();

$admin->examineBrokerClusterInfo();
$admin->fetchAllTopicList();
$admin->createAndUpdateTopicConfig('127.0.0.1:10911', new TopicConfig('TopicTest'));
$admin->examineConsumeStatsGroup('GID_DEMO');
$admin->resetOffsetByTimestamp('TopicTest', 'GID_DEMO', time() * 1000);
$admin->shutdown();
```

Topic / subscription-group CRUD, cluster and broker runtime information, consume stats and
progress, message queries by key / uniqKey / msgId, offset read and reset, and name server
configuration are all in `src/Client/Admin.php`.

### ACL

```php
use RocketMQ\Remoting\AclClientRPCHook;
use RocketMQ\Remoting\SessionCredentials;

$producer = new DefaultMQProducer('PID_DEMO', new AclClientRPCHook(
    new SessionCredentials('yourAccessKey', 'yourSecretKey')
));
```

The three consumers and the admin client take the same `RPCHook` as their first
constructor argument; pass a third argument for an STS token. Signing uses `HmacSHA1`
and the signed content includes the namespace and request-timestamp fields.

### Namespaces (two mechanisms, independent of each other)

- **`namespace` (local resource prefix)**: the third constructor argument, e.g.
  `new DefaultMQProducer('PID_DEMO', null, 'MyNamespace')`. Topics and groups are wrapped
  as `MyNamespace%resource` across send / consume / heartbeat / offset paths and unwrapped
  again on use — fully transparent to the broker.
- **`namespaceV2` (server-side namespace)**: `$client->setNamespaceV2('my-ns')`. It leaves
  resource names untouched and instead stamps `nsd=true` / `ns=<value>` extension fields
  on every request (**read live per request**, not snapshotted at start-up); the broker
  routes by that namespace.

Both can be used at the same time without interfering.

### Message Compression

Bodies over the threshold (default 4,096 bytes) are compressed automatically, and the
receiving side decompresses by the message type bit; anything that cannot be decompressed
always throws — a compressed stream is never passed through as the body. All three codecs
are built into this SDK:

- **ZLIB**: `gzcompress` / RFC1950 (the default codec).
- **LZ4**: a pure-PHP LZ4 Frame-format implementation — no extension or CLI needed.
- **ZSTD**: prefers the external `zstd` CLI for real compression; without the CLI it falls
  back to the built-in pure-PHP Raw/RLE frame implementation.

```php
$producer->setCompressMsgBodyOverHowmuch(4096);
$producer->setCompressType(MessageSysFlag::ZLIB_TYPE);   // switchable: LZ4_TYPE / ZSTD_TYPE
$producer->setCompressLevel(5);
```

### TLS

`tlsEnable` is a process-level switch: once on, every outbound connection (including the
NameServer ones) goes over TLS. Set it via the constructor, the public property, or the
`ROCKETMQ_TLS_ENABLE=1` environment variable:

```php
$producer = new DefaultMQProducer('PID_DEMO', tlsEnable: true);
$producer->tlsOptions = [
    'caCert'     => '/path/ca.crt',      // present => really validates the chain + hostname
    'clientCert' => '/path/client.crt',  // mTLS: client certificate
    'clientKey'  => '/path/client.key',
    'serverName' => '127.0.0.1',         // SNI / hostname-check override
];
```

Without `caCert` a self-signed server certificate is trusted; hostname and chain
validation are strict by default.

## Features and Status

- ✅ Protocol / transport: `RemotingCommand` dual serialization (JSON + ROCKETMQ binary),
  the 17-segment storage format and the 6-segment batch message codec, sync / async /
  oneway, opaque matching, half-packet reassembly, GO_AWAY connection-switch resend, TLS
- ✅ Sending: normal / pinned / queue selector / batch (with `ProduceAccumulator`
  auto-accumulation and two-level fair backpressure) / oneway / async / transactional
  (two-phase + broker check-back) / timed messages and recall / Request-Reply
- ✅ Consuming: Push (concurrent / orderly / broadcasting / offset persistence / start-up
  numeric validation / pre-pull flow-control thresholds), Pull (whole-topic view and
  this-instance share view), LitePull (incl. topic queue-set change listener),
  POP (200050 pop / 200051 ack / 200053 change-invisible-time + checkpoints),
  `ConsumeMessageDirectly`
- ✅ Queue allocation: `AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` /
  `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY`, driven by rebalance
- ✅ Namespaces: the local-prefix `namespace` and the server-side `namespaceV2` mechanisms
- ✅ Admin: topic / subscription-group CRUD, cluster and broker runtime information,
  consume stats and progress, message queries by key / uniqKey / msgId, offset read and
  reset, name server configuration
- ✅ Observability: message tracing (production / consumption / check-back + an
  asynchronous dispatcher), consume stats, `ConsumerRunningInfo`, the five kinds of hooks,
  `OpenTracingHook`, W3C `traceparent` injection and pass-through
- ✅ Security and addressing: ACL signing (HmacSHA1), dynamic NameServer addressing (HTTP
  domain + `NsAddr` file), fault-avoiding queue selection, `Validators` local pre-checks
- ✅ Compression: ZLIB (`gzcompress`), pure-PHP LZ4 Frame, ZSTD (CLI preferred, pure-PHP
  fallback)
- ✅ TLS: CA verification / mTLS / SNI
- ✅ Offline self-test: 10 suites with 1,674 assertions + the duplicate-class-name static
  guard (measured all green)
- ✅ Live-cluster verification: the 8 `examples/live_*.php` cases cover the admin surface,
  redelivery / dead-letter, pull mode, queue-change listening, POP, TLS, Request-Reply and
  the compression smoke

## Client Logging

`RocketMQ\Client\Logger` **writes to a file by default** at
`<current working directory>/logs/rocketmqlogs/rocketmq_php_client.log`, rotating by size
(default 64MB × 10 backups, named `<file>.1` … `<file>.N`). Landing in the current working
directory is deliberate: in script / CI scenarios it never pollutes the user's home
directory, and the path follows the deployment — naturally predictable inside a container.
Point `ROCKETMQ_CLIENT_LOG_DIR` elsewhere when needed (e.g. `$HOME/logs/rocketmqlogs` or a
temp directory); that is the official escape hatch.

| Environment variable | Default | Description |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` (`TRACE` folds into `DEBUG`) |
| `ROCKETMQ_CLIENT_LOG_DIR` | `<current working directory>/logs/rocketmqlogs` | log directory |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_php_client.log` | file name; empty / `OFF` / `NONE` disables writing to a file; a value containing a path separator is treated as a full path |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | empty | any non-empty value = write to stderr only, no file |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864` (64MB) | size-rotation threshold; `0` = no rotation |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | number of backups |

In-process knobs: `Logger::setLevel()` / `Logger::setHandler()` (inject a callable, used
by unit tests) / `Logger::logFilePath()` (the assertion point for diagnostics and for the
live scripts' "was a file actually written"). A failed file write degrades only once and
never breaks the client's main flow.

> Local environment noise: the host `php.ini` auto_prepend makes every `php` process
> print two `[CQ_POLLER] ...` lines; it is unrelated to this client — exclude it first
> when inspecting logs.

## Live Cluster Verification

`examples/live_*.php` are the live verification tools. Failures always end with a non-zero
exit code, and every run prints a `PASS=<n> FAIL=<n>` summary line. The recommended entry
is the root-level orchestration script, which brings up / reuses the local 5.5.1 test
cluster and **only stops the instance it started** (an already-running cluster is reused
as-is):

```bash
bash scripts/run_php_live.sh <case> 127.0.0.1:9876 [legs]
```

| case | script | contents (one line) |
| --- | --- | --- |
| `admin` | `examples/live_admin.php` | admin smoke: cluster probing, topic CRUD + routes, broker config/runtime info, KV config, subscription-group CRUD, send + topic stats, offset/timestamp queries, KEYS-index queries; the resources it creates are deleted at the end |
| `redelivery` | `examples/live_redelivery.php` | redelivery / dead-letter matrix S1–S4: RETRY re-delivery, maxReconsumeTimes turning into DLQ, orderly poison messages, partial ack (optional `legs=all\|s1,s2,s3,s4`) |
| `pull` | `examples/live_pull.php` | pull-mode consumer S1–S3: queue views / `fetchMessageQueuesInBalance` computed on the spot / manual full pull / offset commit and read-back (optional `legs=all\|s1,s2,s3`) |
| `lite_qc` | `examples/live_lite_topic_queue_change.php` | LitePull topic queue-set change listening: the check interval pinned to 1s plus real scale-in/out proves the comparison round queries routes live; the main loop drives `tick()`+`poll()` every round |
| `request_reply` | `examples/live_request_reply.php` | Request-Reply roundtrip: the responder process (consumer + `reply()`) and the requester process (`request()`) orchestrated in two stages, summarized by `ROUNDTRIP ok=<n> total=<n>` |
| `pop` | `examples/live_pop.php` | POP consumption S1–S2: timed-window observation that ACKs really take effect + failure backoff (change-invisible-time → RETRY → retry marker) (optional `legs=all\|s1,s2`) |
| `tls` | `examples/live_tls.php` | the three TLS legs `plain_tls` / `ca_verify` / `mtls`; the script mints CA/server(SAN:127.0.0.1)/client certificates on the spot |
| `compression` | `examples/live_compression.php` | this-end compression smoke: one php→php leg compressing and decompressing over zlib / lz4 / zstd; exit codes 0 ok / 1 ordinary failure / 2 bad codec or usage / 3 recv timeout |

Three hard rules for running the live cases:

1. **Start the consumer first, then send messages.** Consumers have no default-topic
   fallback: missing route = no assignment = no consumption, and sending before the
   consumer is up disguises an environment problem as a client bug. Every live script
   orchestrates in this order.
2. **Some cases need dedicated broker configuration** (the script handles it, but they
   demand an **exclusive cluster**): `pop` requires the four POP `broker.conf` settings
   (`timerWheelEnable=true`, `defaultMessageRequestMode=PULL`,
   `popResponseReturnActualRetryTopic=false`, `enablePopBatchAck=false`); `tls` requires
   both NameServer and Broker started with `tls.enable`; the recall and trace paths also
   need the broker to have `recallMessageEnable=true` / `traceTopicEnable=true`. If a
   plaintext cluster is already listening, the `pop` / `tls` cases refuse to run with exit
   code 2 instead of killing the shared cluster.
3. **Logs are pinned to a temp directory**: the `lite_qc` case points
   `ROCKETMQ_CLIENT_LOG_DIR` at the system temp directory by default (an existing value is
   never overwritten), and the offline suites do the same — live and test artifacts of the
   PHP client never land under the user's home directory.

The cross-codec compression matrix runs through the root-level
`scripts/compression_matrix.sh zlib` (`lz4` / `zstd` use the same script with the codec
swapped).

## Repository Layout

```
php/
├── bootstrap.php              PSR-4 autoloader + the entry of the duplicate-class-name static guard (no composer)
├── composer.json              declares only ext-* and the autoload mapping; pulls in no dependency
├── PORTING.md                 engineering conventions (naming / wire format / the tick execution model / class-name uniqueness)
├── src/
│   ├── Common/                message model, codecs, compression (CompressionCodec), constants, validation
│   ├── Remoting/              RemotingClient, RpcHooks, Protocol/ (frames, the two serializations, request/response headers and bodies)
│   └── Client/                Producer / PushConsumer / PullConsumer / Admin /
│                              allocation strategies / tracing / Latency / Backpressure / Logger / Exceptions
├── examples/                  live verification tools (live_*, failures always exit non-zero)
└── tests/                     offline self-tests (run_all.php chains the suites + the class-name conflict scan)
```

## License

Apache-2.0
