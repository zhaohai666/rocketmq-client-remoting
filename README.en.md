# Multi-language RocketMQ Clients for the Classic Remoting Protocol

[![License][license-image]][license-url]

[中文](README.md) | English | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Client SDKs for the **classic remoting protocol** of Apache RocketMQ, in **Python / C++ / C# /
Rust / Go / Node.js (TypeScript) / PHP**. Every implementation talks straight to **NameServer +
Broker** with no proxy in between, and does its own frame codec, long-lived connection management,
rebalancing, offset management and message encoding/decoding.

- Wire protocol: `RemotingCommand` frames, dual JSON / RocketMQ-binary serialization (V1/V2
  headers; V2 uses single-letter short keys);
- Message codec: the 17-field store format plus the 6-field batch format, identical to what the
  broker persists;
- Works against RocketMQ 4.x / 5.x clusters; all live verification runs against **5.5.1**
  (NameServer 9876 + Broker 10911);
- **Zero third-party runtime dependencies** everywhere except Rust, which uses `tokio` to expose
  an async API.

## Goal

Give every mainstream language a self-contained implementation of the same protocol semantics that
can be used on its own and interoperate with the rest. A message produced by any language —
including zlib / LZ4 / ZSTD compressed bodies — must be readable by the others.
`scripts/compression_matrix.sh` runs the whole cross-language compression matrix against a real
cluster (currently `fail=0` for all of zlib / lz4 / zstd). Each port implements LZ4 / ZSTD by hand
or uses capabilities built into its own runtime, so the compression surface is not identical across
ports; see the table below and each language README for the details.

## Features and Status

* Ready - ✅
* Partial - 🚧

| Feature | Python | C++ | C# | Rust | Go | Node.js | PHP |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Standard / FIFO / timed messages | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Transactional messages (two-phase + check-back) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Recall of timed messages | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Push / Pull / Lite Pull consumers | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Six allocation strategies + background rebalance | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ACL signing / TLS / namespaces | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Message trace (LOCAL / CLOUD) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Admin / message query / offset reset | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Compression: zlib / LZ4 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Compression: ZSTD | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Client logging (rotation + env vars) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

> Note: Go's ZSTD encoder emits valid store-only frames (ratio ~1:1, no entropy coding) while
> decoding is complete; every port can read them. Use the other six ports when you need a real
> compression ratio.

### Capability details

#### Protocol & transport

- Frame codec: `RemotingCommand`; dual JSON / RocketMQ-binary serialization (V1/V2 headers; V2 uses single-letter short keys); tolerant parsing of the non-strict JSON brokers return.
- Message codec: the 17-field store format plus the 6-field batch format, identical to what the broker persists.
- Connection management: lazy connect and reuse, sync / async / oneway, partial-packet reassembly, opaque matching, timeout and reconnect, in-flight request sweeper, TLS.
- Failure paths: one resend after GO_AWAY on a fresh connection; in-flight requests fail fast when the connection drops.
- Connection lifecycle rules (each covered by an offline case against a local-only fake broker):
  - a connection that sends an undecodable frame logs one ERROR naming the peer and is torn down (an out-of-range length prefix is the same event one level earlier, reported once at the level that caught it and never also as a decode failure);
  - a response no request waits on logs one WARN naming its opaque and address, and the connection stays up;
  - the writable answer describes the socket's send buffer — an address with no live connection is writable (the next send connects), and only a live socket with no room is not;
  - a connection that carried neither a read nor a write for strictly longer than the idle limit (120s by default; a non-positive configured value restores the default rather than switching the sweep off) is closed with one WARN per address, every successful read and write re-stamps its clock, and each expired in-flight entry logs its own WARN;
  - a synchronous wait that runs out closes the socket it went out on unless the port's close-on-timeout switch is turned off — the socket is kept only when the budget left at the deadline is neither above 100ms nor above a quarter of that timeout; a write that fails outright always closes;
  - knob names (idle limit / close-on-timeout): Go `ChannelIdleTimeoutSeconds` / `CloseChannelOnSyncTimeout`, Rust `channel_idle_timeout_millis` / `close_socket_on_sync_timeout`, Python `channel_not_active_interval_millis` / `enable_close_channel_on_sync_timeout`, Node.js `maxIdleTimeMillis` / `closeSocketOnSyncTimeout`, C++ `setChannelIdleTimeoutMillis()` / `setCloseSocketOnSyncTimeout()`, C# `ChannelIdleTimeoutMillis` / `CloseSocketOnSyncTimeout`, PHP `setChannelIdleTimeoutSeconds()` / `setCloseChannelOnSyncTimeout()` (PHP's sweep is driven by its own pump; Node.js's sweep timer is `unref()`ed).
- One registration follow-up per new connection: as soon as a new connection to a broker already in the route table is up, the instance immediately sends one follow-up heartbeat over it, re-submits the subscription of every consumer group under this clientId, and wakes one rebalance round; the nameserver connection, addresses missing from the route table, and instances without consumer groups get none, and neither does the switch-off case (the follow-up heartbeat triggers the listener once more, so the first contact shows one extra heartbeat beat — a side effect of the contract itself, not a defect). Knobs (all on by default): Go `EnableHeartbeatChannelEventListener`, Rust `enable_heartbeat_channel_event_listener`, Python `set_enable_heartbeat_channel_event_listener()`, Node.js `setEnableHeartbeatChannelEventListener()`, C++ `setEnableHeartbeatChannelEventListener()`, C# `EnableHeartbeatChannelEventListener`, PHP `setEnableHeartbeatChannelEventListener()`.

#### Producing & message types

- Sync / queue-selected / batch / oneway / queue selector; async is a real async send pool with two fair back-pressure semaphores.
- Transactional messages: two-phase + broker check-back; recall of timed messages (`recallMessage`).
- Message types: normal / FIFO / delayed / transactional / batch / request-reply / timed recall.

#### Consuming

- Push Consumer: long polling + POP + ordering + broadcast + offset persistence + startup value validation; start-offset resolution (the `CONSUME_FROM_FIRST` / `LAST` / `TIMESTAMP` dispatch, with broadcasting, `%RETRY%` groups and SQL92-filtered subscriptions all going through the same decision chain).
- The three per-queue cache gates before pulling (each judges everything the queue still "holds": the undispatched buffer plus the batches handed to the listener but not settled yet, deduplicated by queue offset): unconsumed count / message **body** bytes in whole MiB by integer division / queue-offset span; every gate trips strictly above its threshold and the threshold is taken literally — never clamped to 1, and 0 is not an off switch, the legal ranges belong to the startup config check; the byte gate accumulates body length rather than the decoder's `storeSize`, and the span gate never applies at all to orderly consumption.
- The two topic-level cache budgets are not summed across sibling queues at pull time; after a rebalance round changes the assignment they are divided by the number of owned queues and the quotient written back into the matching per-queue gate.
- Flow-control rhythm: a tripped local cache gate re-schedules in 50ms, the broker's FLOW_CONTROL(215) waits only 20ms, and any other failed pull waits that consumer's configurable generic backoff (default 1000ms); warnings on the 1st trip and every 1000th thereafter.
- A rebalance round whose per-topic assignment set moved pays the whole package in one order: re-share the subscription version only for the moved topics → divide the budgets → one immediate heartbeat → callback the queue listener for the moved topics only; unchanged rounds and stalled-loop self-heal rounds owe nothing.
- POP: the loop has no "pulled but unconsumed" buffer, so its cache gate is this queue's own debt of messages waiting to be acked (default 96, likewise strictly greater, reading only its own queue and never summing sibling queues), tripping into the same 50ms re-schedule, the same global counter and the same warning rhythm, while the broker's 215 still waits only 20ms; supports single ack and `BATCH_ACK_MESSAGE`(200151) batch ack — multiple CKs sharing the same retry flag + queue + pop time window merge into one request, with the bit set encoded as `queueOffset-ckQueueOffset`; plus a sweep escape hatch for suspended listeners.
- Pull Consumer: `fetchSubscribeMessageQueues` returns the whole topic; `fetchMessageQueuesInBalance` returns only this instance's fair share, keeps the existing assignment when it cannot compute one and never degrades into "ask for everything"; when the assignment set changes, only the queue-listener half is owed.
- Lite Pull Consumer: three offset tables — pull / consumed / in-memory commit; the background pull passes four cache gates before resolving this queue's pull offset — in-flight pull rounds × `PullBatchSize` (default 10) against `PullThresholdForAll` 10000, per-queue count 1000, per-queue body bytes in whole MiB 100, per-queue offset span 2000, each tripping strictly above its threshold; a trip re-schedules only that queue 50ms while sibling queues continue, and the broker's 215 still waits only 20ms; the count and byte gates share one trip counter while aggregate and span each have their own; a revoked queue loses its buffer, accumulators and in-flight rounds together.
- Lite differences and misuse gates: Node.js's lite `poll()` pulls in place with no prefetch buffer, so the four gates have no object on that port and only the per-queue 20ms stall on 215 remains; the auto-commit period's 1000ms is a floor, not a clamp — a lower new value is ignored whole and the already-running period keeps running; subscribing with a blank topic, assigning an empty queue list (which revokes all held queues), calling `setSubExpressionForAssign` after it is already running, and calling `poll` / `committed` before start all fail locally right away, and `poll` rejects negative timeouts with the state check ahead of the timeout check — an empty round and "no offsets yet" look exactly like a queue parked at its tail offset, and only this makes a missing `start()` discoverable.
- Queue-change listener: LitePull's `registerTopicMessageQueueChangeListener` registers a per-topic callback, compares the queue set each round, and scale-out/scale-in is seen on the very next check (the comparison round re-queries the route instead of reading the periodically refreshed route cache; an empty queue set is treated as "topic not found").
- Queue allocation: six pluggable strategies `AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<inner>`; all implementations share one allocation formula.

#### Namespaces & admin

- Two independent mechanisms: `namespace` (local resource-name prefix `%%ns%%res`, wrapped and unwrapped across send/receive, heartbeat and offsets) and `namespace_v2` (server-side namespace: `NamespaceRpcHook` stamps `nsd=true` / `ns=<value>` on every request; hook order is Namespace → Stream → ACL, so both `ns` and `ReqT` enter the ACL signature; an empty value writes no field at all). Producers, all three consumers and the admin client read it **per request**, not as a startup snapshot.
- Admin `DefaultMQAdminExt`: topics / subscription groups / cluster info / various statistics / message queries / offset reset / `searchOffset` boundary semantics.

#### Observability & security

- Message trace: encoding + async dispatch + hooks, including the recall trace hook; `AccessChannel` supports LOCAL / CLOUD (CLOUD routes to `rmq_sys_TRACE_DATA_<regionId>`).
- Consumption statistics, `ConsumerRunningInfo`(307), consumed-offset look-back `GET_CONSUMER_STATUS`(221, a blank topic reports the whole table).
- Five hook families, ACL signing, namespace RPC hooks (`nsd` / `ns`, ordered before ACL signing), dynamic NameServer addressing, fault-avoiding queue selection + broker reachability probe thread.

#### Compression & logging

- Three backends — zlib / LZ4 / ZSTD: automatic compression on the producer side and automatic decompression on the consumer side; an unsupported type must raise an error rather than pass the compressed stream through; per-port LZ4 / ZSTD are hand-written or runtime built-in, see the "SDKs" table.
- Every port writes a **client log file** with one shared set of `ROCKETMQ_CLIENT_LOG_*` environment variables, size- or day-based rotation and a fixed backup window (see "Client Logging").

## SDKs

| Language | Directory | Runtime model | Compression | Unit tests |
| --- | --- | --- | --- | --- |
| Python | [`python/`](python/README.en.md) | sync API with internal threads | zlib / LZ4 / ZSTD | `pytest -q`: 1867 passed + 4 skipped |
| C++ | [`cpp/`](cpp/README.en.md) | C++17, hand-written network layer, no third-party runtime deps | zlib / LZ4 / ZSTD | `ctest`: 80 cases green (80/80) |
| C# | [`csharp/`](csharp/README.en.md) | .NET 10, zero NuGet dependencies | zlib / LZ4 / ZSTD | `dotnet test`: 1255 passed |
| Rust | [`rust/`](rust/README.en.md) | tokio-based async API | zlib / LZ4 / ZSTD | `cargo test`: 1251 passed; `cargo clippy --all-targets` warning-free |
| Go | [`go/`](go/README.en.md) | sync API with internal goroutines, zero third-party deps | zlib / LZ4 both ways; ZSTD decodes every format, encodes store-only | `go test ./... -race`: green (84 `*_test.go` files); `go vet` / `gofmt` clean |
| Node.js | [`nodeJs/`](nodeJs/README.en.md) | TypeScript run without a build step (`node --experimental-strip-types`), zero third-party deps | zlib / LZ4 both ways with a hand-written frame; ZSTD via `node:zlib` (≥ 23.8), falling back to hand-written raw/RLE blocks on older runtimes | `node selfcheck.ts`: 67 modules loaded + 64 smoke suites green |
| PHP | [`php/`](php/README.en.md) | PHP 8.1+, single-threaded `tick()`-driven, zero composer deps | zlib via `gzcompress`; LZ4 as a pure-PHP frame; ZSTD prefers the `zstd` CLI and falls back to pure-PHP raw/RLE | `php tests/run_all.php`: 5,492 assertions green + duplicate class-name guard |

> Go's ZSTD encoder emits **valid frames without entropy coding** (store-only raw/RLE blocks): the
> other ports decode them fine, but the ratio is ~1:1 — `storeSize ≈ body size` in the live matrix
> is exactly that trade-off. Use the other six ports when you need real compression.

Each language README documents that port's build steps, quick start, configuration (logging / TLS /
compression), directory layout and live-verification tools.

## Prerequisites and Build

- A working cluster: NameServer `9876` + Broker `10911`. With `autoCreateTopicEnable=true` the
  first send creates the topic for you.
- The runtime for your language: CPython 3.8+, a C++17 compiler + CMake, the .NET 10 SDK, stable
  Rust + tokio, Go 1.24, Node.js 22+ (23.8+ if you want ZSTD through `node:zlib`), PHP 8.1+.
- Some cases need extra broker configuration (stated in the header comment of each live tool):
  master/slave cluster, `traceTopicEnable=true`, `enablePropertyFilter=true`,
  `recallMessageEnable`, POP-dedicated configuration, TLS certificates, ACL enabled.

This project is organized as a monorepo, with each language's binding containing its own build
instructions in the corresponding subdirectory:

```sh
git clone git@github.com:zhaohai666/rocketmq-client-remoting.git
cd rocketmq-client-remoting
```

## Quick Start

All seven languages follow the same shape: build the facade → set the NameServer address →
`start()` → send/receive → `shutdown()`.

> One hard rule for cross-language checks: **start the consumer first, then send**. A Push Consumer
> only consumes messages that entered the queue after the subscription, so the reversed order looks
> exactly like message loss.

### Python

```python
from rocketmq_client_remoting.client.producer import DefaultMQProducer
from rocketmq_client_remoting.client.consumer import DefaultMQPushConsumer, MessageListenerConcurrently
from rocketmq_client_remoting.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq_client_remoting.common.message import Message

producer = DefaultMQProducer("PID_DEMO")
producer.set_namesrv_addr("127.0.0.1:9876")
producer.start()
producer.send(Message("TopicTest", b"hello rocketmq"))
producer.shutdown()


class DemoListener(MessageListenerConcurrently):
    def consume_message(self, msgs, context):
        for msg in msgs:
            print(msg.body)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


consumer = DefaultMQPushConsumer("GID_DEMO")
consumer.set_namesrv_addr("127.0.0.1:9876")
consumer.subscribe("TopicTest")
consumer.set_message_listener(DemoListener())
consumer.start()
# ... after SIGINT
consumer.shutdown()
```

### C++

```cpp
#include <rocketmq/client/producer.h>
#include <rocketmq/client/consumer.h>
#include <rocketmq/common/message.h>
#include <rocketmq/client/result.h>

using namespace rocketmq;

class DemoListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        for (const auto& msg : msgs) std::printf("%s\n", msg.getBody().c_str());
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

int main() {
    DefaultMQProducer producer("PID_DEMO");
    producer.setNamesrvAddr("127.0.0.1:9876");
    producer.start();
    producer.send(Message("TopicTest", "hello rocketmq"));
    producer.shutdown();

    DefaultMQPushConsumer consumer("GID_DEMO");
    consumer.setNamesrvAddr("127.0.0.1:9876");
    consumer.subscribe("TopicTest");
    consumer.setMessageListener(std::make_shared<DemoListener>());
    consumer.start();
    // ... on the exit signal
    consumer.shutdown();
}
```

### C#

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
consumer.SetMessageListener(new DemoListener());   // IMessageListenerConcurrently
consumer.Start();
// ... on the exit signal
consumer.Shutdown();
```

### Rust

```rust
use rocketmq_client_remoting::client::consumer::DefaultMQPushConsumer;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{ConsumeConcurrentlyStatus, MessageListenerConcurrently};
use rocketmq_client_remoting::common::message::Message;

#[tokio::main]
async fn main() -> rocketmq_client_remoting::error::Result<()> {
    let producer = DefaultMQProducer::new("PID_DEMO")?;
    producer.set_namesrv_addr("127.0.0.1:9876");
    producer.start().await?;
    let mut msg = Message::new("TopicTest", Some(b"hello rocketmq".as_slice()));
    producer.send(&mut msg, None, None).await?;
    producer.shutdown();

    struct DemoListener;
    impl MessageListenerConcurrently for DemoListener {
        fn consume_message(&self, msgs: &[MessageExt], _ctx: &mut ConsumeConcurrentlyContext)
            -> ConsumeConcurrentlyStatus {
            for m in msgs {
                if let Some(body) = &m.body {
                    println!("{}", String::from_utf8_lossy(body));
                }
            }
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    let consumer = DefaultMQPushConsumer::new("GID_DEMO")?;
    consumer.set_namesrv_addr("127.0.0.1:9876");
    consumer.subscribe("TopicTest", "*")?;
    consumer.set_message_listener_concurrently(std::sync::Arc::new(DemoListener));
    consumer.start().await?;
    // ... on the exit signal
    consumer.shutdown();
    Ok(())
}
```

### Go

```go
package main

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

type demoListener struct{}

func (demoListener) ConsumeMessage(msgs []*common.MessageExt,
	ctx *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	for _, msg := range msgs {
		fmt.Println(string(msg.Body))
	}
	return client.ConsumeSuccess
}

func main() {
	producer, _ := client.NewDefaultMQProducer("PID_DEMO")
	producer.SetNameServerAddresses([]string{"127.0.0.1:9876"})
	producer.Start()
	producer.Send(common.NewMessage("TopicTest", []byte("hello rocketmq")))
	producer.Shutdown()

	consumer, _ := client.NewDefaultMQPushConsumer("GID_DEMO")
	consumer.SetNameServerAddresses([]string{"127.0.0.1:9876"})
	consumer.Subscribe("TopicTest", "*")
	consumer.SetMessageListener(demoListener{})
	consumer.Start()
	// ... on the exit signal
	consumer.Shutdown()
}
```

### Node.js / TypeScript

```ts
import { DefaultMQProducer } from './src/client/producer.ts';
import { DefaultMQPushConsumer } from './src/client/consumer.ts';
import { Message } from './src/common/message.ts';
import { ConsumeConcurrentlyStatus } from './src/client/consumer_result.ts';

const producer = new DefaultMQProducer('PID_DEMO');
producer.setNamesrvAddr('127.0.0.1:9876');
producer.start();
const result = await producer.send(new Message('TopicTest', Buffer.from('hello rocketmq')));
console.log(result.msgId);
producer.shutdown();

const consumer = new DefaultMQPushConsumer('GID_DEMO');
consumer.setNamesrvAddr('127.0.0.1:9876');
consumer.subscribe('TopicTest', '*');
consumer.registerMessageListenerConcurrently((msgs) => {
  for (const m of msgs) console.log(m.getBody()?.toString());
  return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});
await consumer.start();
// ... on the exit signal
consumer.shutdown();
```

Run it without a build step: `node --experimental-strip-types your_file.ts`.

### PHP

PHP has no resident threads, so heartbeat, rebalance and offset persistence are folded into a
caller-driven `tick()`:

```php
<?php
require __DIR__ . '/bootstrap.php';   // PSR-4 + classmap fallback, no composer needed

use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Common\Message;

$producer = new DefaultMQProducer('PID_DEMO');
$producer->setNamesrvAddr('127.0.0.1:9876');
$producer->start();
$producer->send(new Message('TopicTest', 'hello rocketmq'));
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
while (true) {          // this loop is this port's "background thread"
    $consumer->tick();
    usleep(100_000);
}
```

## Client Logging

All seven implementations write client logs to a file by default and rotate them, so they cannot
grow without bound.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `TRACE` / `DEBUG` / `INFO` / `WARN` / `ERROR` |
| `ROCKETMQ_CLIENT_LOG_DIR` | see the table below | log directory |
| `ROCKETMQ_CLIENT_LOG_FILE` | a per-port file name | treated as a full path when the value contains a path separator; empty / `OFF` / `NONE` disables file logging |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | see the three groups below | console output switch |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | 64MB | used by the size-rotating ports (C++ / C# / Go / Node.js / PHP) |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | 10 | number of backups; among the size-rotating ports `0` = truncate in place |

| Language | Default log file | Rotation |
| --- | --- | --- |
| Python | `<current working directory>/logs/rocketmqlogs/rocketmq_py_client.log` | daily, backup suffix `.YYYY-MM-DD` |
| C++ | `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log` | by size, `.1` … `.N` |
| C# | `$HOME/logs/rocketmqlogs/rocketmq_csharp_client.log` | by size, `.1` … `.N` |
| Rust | `$HOME/logs/rocketmqlogs/rocketmq_rs_client.log` | daily, backup suffix `.YYYY-MM-DD` |
| Go | `$HOME/logs/rocketmqlogs/rocketmq_go_client.log` | by size, `.1` … `.N` |
| Node.js | `$HOME/logs/rocketmqlogs/rocketmq_node_client.log` | by size, `.1` … `.N` |
| PHP | `<current working directory>/logs/rocketmqlogs/rocketmq_php_client.log` | by size, `.1` … `.N` |

File names differ per port on purpose: several client processes often run on the same machine, and
two processes interleaving into one file corrupts both. Rotation is worse — while this port renames
the file, the other process still holds the old descriptor and keeps writing into a renamed file.
Set `ROCKETMQ_CLIENT_LOG_FILE` explicitly if you do want a port to share a file.

Python and PHP default to the **current working directory** rather than the user's home: those two
ports mostly run as throwaway scripts and in CI, where a log under `$HOME` is hard to clean and
easy to pollute, while a cwd-relative one travels with the project. The other five default to
`$HOME`. `ROCKETMQ_CLIENT_LOG_DIR` overrides the directory on all seven.

`ROCKETMQ_CLIENT_LOG_USE_STDOUT` behaves differently in three groups:

| Group | Ports | Console behaviour |
| --- | --- | --- |
| File + stderr together | C++ / C# | no such variable: stderr is always written; to keep only stderr set `ROCKETMQ_CLIENT_LOG_FILE=OFF` |
| Either/or | Go / Node.js / PHP | any non-empty value = stdout only, **no** log file is created; unset = file only |
| File + console, console can be muted | Python / Rust | both by default; `ROCKETMQ_CLIENT_LOG_USE_STDOUT=false` mutes the console |

The difference between the last two groups is who reads the process's stdout: Python and Rust
clients are usually embedded in someone else's process or library, and a silent file would take
away the caller's only immediately visible signal.

A log write failure never affects sending or receiving: the port falls back to stderr and keeps
working.

## Live Cluster Verification

Every language directory ships a set of live tools that are **not part of the unit tests**,
covering the full send/receive path, redelivery and dead-letter, offset management, flow control,
POP, TLS, request-reply, compression and queue-change listening:

| Language | Form | Entry point |
| --- | --- | --- |
| Python | `python/verify_*_live.py` | `.venv/bin/python verify_xxx_live.py 127.0.0.1:9876` |
| C++ | `rmq_live_*` executables from `cpp/examples/` | `cpp/build/examples/rmq_live_xxx 127.0.0.1:9876` |
| C# | `rmq` example subcommands | `dotnet run --no-build --project examples/RocketMQ.Examples -- <case> 127.0.0.1:9876` |
| Rust | `rust/examples/live_*.rs` | `cargo run --quiet --example live_xxx -- 127.0.0.1:9876` |
| Go | `go/examples/live_*/` | `go run ./examples/live_xxx -ns 127.0.0.1:9876` |
| Node.js | `nodeJs/examples/live_*.ts` | `bash scripts/run_node_live.sh <case> 127.0.0.1:9876` |
| PHP | `php/examples/live_*.php` | `bash scripts/run_php_live.sh <case> 127.0.0.1:9876` |

Every tool asserts its own expectations and exits non-zero on failure; the per-port inventory and
coverage gaps are listed in the "Live Cluster Verification" section of each language README.
`scripts/with_cluster.sh` checks that NameServer / Broker are ready before running a command.

### Every message type on an ACL-enforcing cluster

Once the broker sets `authenticationEnabled=true`, each port must assert **both halves**: an
unsigned send first, which has to come back `NO_PERMISSION(16)`, and then every message type sent
signed and read back from the broker. Only the first half proves enforcement — without it the
second half is green on a cluster that never turned ACL on.

```bash
RMQ_ACL_AK=<accessKey> RMQ_ACL_SK=<secretKey> bash scripts/run_acl_message_types_live.sh         # all 7
RMQ_ACL_AK=<accessKey> RMQ_ACL_SK=<secretKey> bash scripts/run_acl_message_types_live.sh go php # a few
```

| Language | Entry point | Message types covered |
| --- | --- | --- |
| Python | `python/verify_message_types.py <namesrv> <ak> <sk>` | async / oneway / batch / delay-level / timer / transaction / orderly / tag / key query / user property |
| C++ | `cpp/build/examples/rmq_live_message_types <namesrv> <ak> <sk>` | sync / async / oneway / async-to-queue / FIFO / tag / property / delayed / timed / key query / transaction (three states) / batch |
| C# | `... -- message-types <namesrv> <ak> <sk>` | sync / async / oneway / FIFO / tag / property / delayed / timed / key / transaction (three states) / batch |
| Rust | `cargo run --example live_acl_message_types -- <namesrv> <ak> <sk>` | sync / async / oneway / batch / delayed / timed / transaction (incl. transactionId) / FIFO / tag / key / property + signed offset commit |
| Go | `go run ./examples/live_acl_message_types -ak .. -sk ..` | sync / async / oneway / batch / delayed / timed / transaction / FIFO / tag / property / key query |
| Node.js | `nodeJs/examples/live_acl_message_types.ts --ak .. --sk ..` | sync / async / oneway / batch / delayed / timed / transaction / FIFO / tag / property / key query |
| PHP | `php examples/live_acl_message_types.php <namesrv> <ak> <sk>` | sync / async / oneway / batch / delayed / timed / transaction / FIFO / tag / key; two reverse legs (unsigned and wrong secretKey both rejected with 16) |

`scripts/run_acl_live.sh` is the narrower sibling: it verifies the signature alone (correct
credentials pass, wrong or missing credentials are rejected) and does not spread message types.

There is also a cluster-free protocol-layer self-check available on six ports (`python -m
selfcheck`, `cpp`'s `rmq_selfcheck`, `csharp`'s `selfcheck` subcommand, `go run
./examples/selfcheck`, `node --experimental-strip-types selfcheck.ts`, `php tests/run_all.php`):
codec round-trips plus the key constants and field names — the cheapest gate before touching a
real cluster. On Rust this offline coverage is provided by `cargo test`.

Cross-language compression is one command away:

```bash
scripts/compression_matrix.sh [zlib|lz4|zstd|all]
```

Each leg is "port A sends → port B only receives → compare crc32", and only `match=1` on the
receiving side counts as a pass; exit codes are 0=pass, 1=fail, 2=codec or usage error, 3=receive
timeout.

## Repository Layout

```
├── python/    Python implementation (sync API, pytest suite + verify_*_live.py scripts)
├── cpp/       C++17 implementation (CMake, ctest suite + examples as live tools)
├── csharp/    C# / .NET 10 implementation (xunit suite + rmq subcommand live tools)
├── rust/      Rust / tokio implementation (inline tests + live_* examples)
├── go/        Go implementation (sync API, zero deps, go test suite + examples as live tools)
├── nodeJs/    Node.js / TypeScript implementation (zero deps, no build step, selfcheck + live_* scripts)
├── php/       PHP 8.1+ implementation (no composer deps, single-threaded tick() model + live_* scripts)
├── scripts/   cluster start/stop and cross-language interop scripts (compression_matrix.sh, …)
└── logs/      client logs dropped into the current directory by the Python / PHP live scripts
```

## Contributing

Any attempt to make this project better is welcome: file an issue, fix a bug, add a case, improve
the docs. Please ship tests with the change, and for anything touching protocol semantics run the
live tools plus `scripts/compression_matrix.sh`.

## Related

* [rocketmq](https://github.com/apache/rocketmq): the server-side implementation.
* [RocketMQ Website](https://rocketmq.apache.org/): product, documentation and ecosystem.
* [Capability checklist](docs/classic-remoting-capability-checklist.md): item-by-item comparison with
  Java 5.x, live matrices and the judgment discipline behind them.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation

[license-image]: https://img.shields.io/badge/license-Apache%202-4EB1BA.svg
[license-url]: https://www.apache.org/licenses/LICENSE-2.0.html
