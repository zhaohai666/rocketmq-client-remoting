# Multi-language RocketMQ Clients for the Classic Remoting Protocol

> [中文](README.md) ｜ English ｜ [RocketMQ Website](https://rocketmq.apache.org/)

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

| Area | Content |
| --- | --- |
| Protocol | `RemotingCommand` frame codec; JSON / binary dual serialization; 17-field + 6-field message codec; V2 short-key headers; tolerant parsing of the non-strict JSON brokers return |
| Transport | Lazy connect and connection reuse, sync / async / oneway, partial-packet reassembly, opaque matching, timeout and reconnect, in-flight request sweeper, one resend after GO_AWAY on a fresh connection, in-flight requests failed fast on disconnect, TLS |
| Producing | Sync / select-result / batch / oneway / queue selector / async (a real async send pool with two fair back-pressure semaphores) / transactional messages (two-phase + broker check-back) / recall of timed messages (`recallMessage`) |
| Consuming | Push Consumer (long polling + POP + ordering + broadcast + offset persistence + startup range validation + five flow-control thresholds before pulling + an escape hatch that sweeps suspended listeners), Pull Consumer (`fetchSubscribeMessageQueues` returns the whole topic, `fetchMessageQueuesInBalance` only the queues this instance owns; when allocation cannot be computed the current assignment is kept instead of degrading to "take everything"), Lite Pull Consumer (three offset tables: pulled / consumed / in-memory commit) |
| Queue-change listener | `registerTopicMessageQueueChangeListener` on LitePull: register a per-topic callback, compare the queue set each round, and see scale-out/scale-in on the very next check (the comparison round re-queries the route instead of reading the periodically refreshed cache; an empty queue set is treated as "topic not found") |
| Queue allocation | Six pluggable strategies: `AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<inner>`; all implementations share one allocation formula |
| Namespace | Two independent mechanisms: `namespace` (local resource-name prefix `%%ns%%res`, wrapped and unwrapped across send/receive, heartbeat and offsets) and `namespace_v2` (server-side namespace: `NamespaceRpcHook` stamps `nsd=true` / `ns=<value>` on every request; hook order is Namespace → Stream → ACL, so `ns` and `ReqT` both enter the ACL signature; an empty value writes no field at all). Producers, all three consumers and the admin client read it **per request**, not as a startup snapshot |
| Admin | `DefaultMQAdminExt`: topics / subscription groups / cluster info / various statistics / message queries / offset reset / `searchOffset` boundary semantics |
| Message types | Normal / FIFO / delayed / transactional / batch / request-reply / timed recall |
| Observability & security | Message trace (encoding + async dispatch + hooks, including the recall trace hook), consumption statistics, `ConsumerRunningInfo` (307), five hook families, ACL signing, namespace RPC hooks (`nsd` / `ns`, ordered before ACL signing), dynamic NameServer addressing, fault-avoiding queue selection + broker reachability probe thread |
| Compression | Three backends — zlib / LZ4 / ZSTD: automatic compression on the producer side, automatic decompression on the consumer side, and an error (never a passthrough of the compressed stream) for unknown types |
| Logging | Every port writes a **client log file**, sharing one set of `ROCKETMQ_CLIENT_LOG_*` environment variables, with size- or day-based rotation and a fixed backup window |

## SDKs

| Language | Directory | Runtime model | Compression | Unit tests |
| --- | --- | --- | --- | --- |
| Python | [`python/`](python/README.en.md) | sync API with internal threads | zlib / LZ4 / ZSTD | `pytest -q`: 1210 passed + 4 skipped |
| C++ | [`cpp/`](cpp/README.en.md) | C++17, hand-written network layer, no third-party runtime deps | zlib / LZ4 / ZSTD | `ctest`: 53 cases green |
| C# | [`csharp/`](csharp/README.en.md) | .NET 10, zero NuGet dependencies | zlib / LZ4 / ZSTD | `dotnet test`: 808 passed |
| Rust | [`rust/`](rust/README.en.md) | tokio-based async API | zlib / LZ4 / ZSTD | `cargo test`: 942 passed; `cargo clippy --all-targets` warning-free |
| Go | [`go/`](go/README.en.md) | sync API with internal goroutines, zero third-party deps | zlib / LZ4 both ways; ZSTD decodes every format, encodes store-only | `go test ./...`: 479 top-level cases; `go vet` / `gofmt` clean |
| Node.js | [`nodeJs/`](nodeJs/README.en.md) | TypeScript run without a build step (`node --experimental-strip-types`), zero third-party deps | zlib / LZ4 both ways with a hand-written frame; ZSTD via `node:zlib` (≥ 23.8), falling back to hand-written raw/RLE blocks on older runtimes | `node selfcheck.ts` + the `test/*.ts` smokes green; live legs via `scripts/run_node_live.sh` green |
| PHP | [`php/`](php/README.en.md) | PHP 8.1+, single-threaded `tick()`-driven, zero composer deps | zlib via `gzcompress`; LZ4 as a pure-PHP frame; ZSTD prefers the `zstd` CLI and falls back to pure-PHP raw/RLE | `php tests/run_all.php`: 1674 assertions + duplicate class-name guard |

> Go's ZSTD encoder emits **valid frames without entropy coding** (store-only raw/RLE blocks): the
> other ports decode them fine, but the ratio is ~1:1 — `storeSize ≈ body size` in the live matrix
> is exactly that trade-off. Use the other six ports when you need real compression.

Each language README documents that port's build steps, quick start, configuration (logging / TLS /
compression), directory layout and live-verification tools.

## Prerequisites

- A working cluster: NameServer `9876` + Broker `10911`. With `autoCreateTopicEnable=true` the
  first send creates the topic for you.
- The runtime for your language: CPython 3.9+, a C++17 compiler + CMake, the .NET 10 SDK, stable
  Rust + tokio, Go 1.21+, Node.js 20+ (23.8+ if you want ZSTD through `node:zlib`), PHP 8.1+.
- Some cases need extra broker configuration (stated in the header comment of each live tool):
  master/slave cluster, `traceTopicEnable=true`, `enablePropertyFilter=true`,
  `recallMessageEnable`, POP-dedicated configuration, TLS certificates, ACL enabled.

## Quick Start

All seven languages follow the same shape: build the facade → set the NameServer address →
`start()` → send/receive → `shutdown()`.

> One hard rule for cross-language checks: **start the consumer first, then send**. A Push Consumer
> only consumes messages that entered the queue after the subscription, so the reversed order looks
> exactly like message loss.

### Python

```python
from client.producer import DefaultMQProducer
from client.consumer import DefaultMQPushConsumer, MessageListenerConcurrently
from client.consumer_result import ConsumeConcurrentlyStatus
from common.message import Message

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

## License

Apache-2.0.
