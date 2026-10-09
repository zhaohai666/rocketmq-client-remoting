# rocketmq-client-remoting

> [中文](README.md) | English

A multi-language client SDK for Apache RocketMQ's **classic remoting protocol**: one protocol
semantics, one message model, with a complete implementation in each of
**Python / C++ / C# / Rust / Go / Node.js (TypeScript) / PHP** — seven ports in total.

Clients talk straight to the **NameServer + Broker**; there is no proxy in between:

- The wire protocol carries both serializations of the remoting protocol — **JSON and RocketMQ
  binary** (`RemotingCommand` frames, V1/V2 headers, V2 using single-letter short keys).
- Message codecs implement the **17-field store format plus the 6-field batch format**, byte for
  byte identical to what the broker writes to disk.
- Works against RocketMQ 4.x / 5.x clusters; all live verification runs on **5.5.1**
  (NameServer 9876 + Broker 10911).
- All seven ports have **zero third-party runtime dependencies** (Rust excepted: it uses `tokio`
  to expose an async API).

Cross-port interoperability is a **hard requirement**: a message sent by any language — including
zlib / LZ4 / ZSTD compressed bodies — must be readable by all the others.
`scripts/compression_matrix.sh` runs the whole seven-port compression matrix against a real
cluster (currently `fail=0` for zlib / lz4 / zstd). Every port's LZ4 / ZSTD is either a
**hand-written implementation in that language** or an **OS/built-in facility** — never a
third-party package — so the codec capability is not perfectly flat across ports; see the table
below and each port's README for the exact state.

## Capability overview

All seven ports cover the same surface:

| Area | Contents |
| --- | --- |
| Protocol | `RemotingCommand` frame codec; JSON / binary dual serialization; 17-field + 6-field message codecs; V2 short-key headers; fastjson2-style lenient parsing |
| Transport | Lazy long-connection setup and reuse, sync / async / oneway, half-packet reassembly, opaque matching, timeout and reconnect, in-flight request expiry sweep, GO_AWAY reconnect-and-resend-once, immediate failure of in-flight requests on disconnect, TLS |
| Sending | Sync / select / batch / one-way / queue selector / async (real async send pool + two fair back-pressure semaphores) / transactional messages (two-phase + broker check-back) / scheduled message recall (`recallMessage`) |
| Consuming | Push Consumer (long polling + POP + ordered + broadcasting + offset persistence + start-time numeric validation + five pre-pull flow-control thresholds + escape hatch for a hung listener), Pull Consumer (`fetchSubscribeMessageQueues` returns the whole topic, `fetchMessageQueuesInBalance` returns only this instance's share — all seven ports use the same allocation formula, and when it cannot be computed they keep the current assignment instead of falling back to "take everything"), Lite Pull Consumer (three offset tables: pulled / consumed / in-memory committed) |
| Queue allocation | Six pluggable strategies: `AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<inner>` |
| Namespace | Two independent mechanisms, present on all seven ports: `namespace` (a client-side resource prefix `%%ns%%res`, Java's `NamespaceUtil` semantics, wrapped and unwrapped across send/consume/heartbeat/offset) and `namespace_v2` (the server-side namespace: `NamespaceRpcHook` stamps `nsd=true` / `ns=<value>` on every request; hook order Namespace → Stream → ACL, so both `ns` and `ReqT` sit inside the ACL signature; when the value is empty not a single field is written). The producer, all three consumers and the admin client read it live per request, not as a start-time snapshot |
| Administration | `DefaultMQAdminExt`: topics / subscription groups / cluster info / various statistics / message query / offset reset / `searchOffset` boundary semantics |
| Message types | Normal / ordered / delayed / transactional / batch / request-reply / scheduled recall |
| Observability & security | Message trace (encoding + async dispatch + hooks, including the recall trace hook), consumer statistics, `ConsumerRunningInfo` (307), five hook families, ACL signing, namespace RPC hook (`nsd` / `ns`, ordered before ACL so the signature covers it), dynamic NameServer addressing, fault-avoiding queue selection + broker reachability detector thread |
| Compression | zlib / LZ4 / ZSTD: automatic compression on the producer side + automatic decompression on the consumer side; an unsupported type must throw rather than pass the compressed stream through |
| Logging | Every port writes a **client log file** (matching Java's `rocketmq_client.log`), driven by the same `ROCKETMQ_CLIENT_LOG_*` variables, size-based rotation with a fixed backup window |

## The seven SDKs

| Language | Directory | Runtime shape | Compression | Unit tests |
| --- | --- | --- | --- | --- |
| Python | [`python/`](python/README.md) | Sync API (internal threads) | zlib / LZ4 / ZSTD | `pytest -q`: 1163 passed + 4 skipped |
| C++ | [`cpp/`](cpp/README.md) | C++17, hand-written network layer, no third-party runtime deps | zlib / LZ4 / ZSTD | `ctest`: 50 cases / 3993 assertions |
| C# | [`csharp/`](csharp/README.md) | C# / .NET 10, zero NuGet dependencies | zlib / LZ4 / ZSTD | `dotnet test`: 789 passed |
| Rust | [`rust/`](rust/README.md) | tokio async API | zlib / LZ4 / ZSTD | `cargo test --lib`: 916; `cargo clippy --all-targets` warning-free |
| Go | [`go/`](go/README.md) | Sync API (internal goroutines), zero third-party deps | zlib / LZ4 both directions; **ZSTD decodes the full format, encodes store-only** | `go test ./...`: 502; `go vet` / `gofmt` clean |
| Node.js | [`nodeJs/`](nodeJs/README.md) | TypeScript run directly (`node --experimental-strip-types`), zero third-party deps | zlib; hand-written LZ4 frame both directions; ZSTD via `node:zlib` (≥ 23.8), falling back to hand-written Raw/RLE below that | `node selfcheck.ts`: 53 modules + 4 smoke suites green; live `scripts/run_node_live.sh producer\|consumer\|pull\|lite_pull\|admin` green |
| PHP | [`php/`](php/README.md) | PHP 8.1+, single-threaded `tick()` driver, zero composer deps | zlib via `gzcompress`; pure-PHP LZ4 frame; ZSTD prefers the `zstd` CLI, falls back to pure-PHP Raw/RLE | `php tests/run_all.php`: 1590 checks + duplicate-class guard |

> Go's ZSTD **encoder emits store-only Raw/RLE blocks**: the frame is valid zstd and every other
> port decompresses it, but the ratio is close to 1:1 — `storeSize ≈ payload size` in the live
> matrix is the evidence. This is a trade-off made to stay dependency-free, not a defect; use one
> of the other six ports when you need real compression ratios.

Each port's README documents its build steps, quick start, configuration (logging / TLS /
compression), directory layout, and the list of live-cluster tools.

## Quick start

All seven languages follow the same moves: build the facade → set the NameServer address →
`start()` → send and receive → `shutdown()`.
Cluster requirement: NameServer `9876` + Broker `10911`; with `autoCreateTopicEnable=true` the
first send creates the topic.

> One hard rule for cross-port testing: **start the consumer before sending**. A Push Consumer
> only consumes messages that land in its queues after subscribing; reversing the order looks
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
    // ... once the exit signal arrives
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
// ... once the exit signal arrives
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
    // ... once the exit signal arrives
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
	// ... once the exit signal arrives
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
// ... once the exit signal arrives
consumer.shutdown();
```

Run it without a build step: `node --experimental-strip-types your_file.ts`.

### PHP

PHP has no resident threads, so heartbeats, rebalancing and offset flushing all collapse into a
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
while (true) {          // this loop is the port's "background thread"
    $consumer->tick();
    usleep(100_000);
}
```

## Client logging

The Java client writes `$HOME/logs/rocketmqlogs/rocketmq_client.log`; all seven ports match that
behaviour: **a log file by default**, rotated, never growing without bound.

| Variable | Default | Meaning |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `TRACE` / `DEBUG` / `INFO` / `WARN` / `ERROR` |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` (`<cwd>/logs/rocketmqlogs` on Python / PHP) | Log directory |
| `ROCKETMQ_CLIENT_LOG_FILE` | per-port file name | A value containing a path separator is treated as a full path; empty / `OFF` / `NONE` disables the file sink |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | see the three groups below | Console output switch |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | 64MB | Used by the size-rotating ports (C++ / C# / Go / Node.js / PHP) |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | 10 | Number of backups; on the size-rotating ports `0` truncates in place |

| Language | Default log file | Rotation |
| --- | --- | --- |
| Python | `<current working directory>/logs/rocketmqlogs/rocketmq_py_client.log` | Daily, backups named `.YYYY-MM-DD` |
| C++ | `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log` | Size based, `.1` … `.N` |
| C# | `$HOME/logs/rocketmqlogs/rocketmq_csharp_client.log` | Size based, `.1` … `.N` |
| Rust | `$HOME/logs/rocketmqlogs/rocketmq_rs_client.log` | Daily, backups named `.YYYY-MM-DD` |
| Go | `$HOME/logs/rocketmqlogs/rocketmq_go_client.log` | Size based, `.1` … `.N` |
| Node.js | `$HOME/logs/rocketmqlogs/rocketmq_node_client.log` | Size based, `.1` … `.N` |
| PHP | `<current working directory>/logs/rocketmqlogs/rocketmq_php_client.log` | Size based, `.1` … `.N` |

`ROCKETMQ_CLIENT_LOG_USE_STDOUT` behaves differently across three groups, **by design**:

| Group | Ports | Console behaviour |
| --- | --- | --- |
| File + stderr always | C++ / C# | No such variable: stderr always gets the line; to keep only stderr set `ROCKETMQ_CLIENT_LOG_FILE=OFF` |
| Either/or | Go / Node.js / PHP | Any non-empty value writes stdout and creates **no** log file; unset writes only the file |
| File + console, console switchable | Python / Rust | Both by default; `ROCKETMQ_CLIENT_LOG_USE_STDOUT=false` silences the console |

The difference between the last two groups comes from who watches this process's stdout: the
Python / Rust clients are frequently embedded into someone else's process or library, so writing
only to a file would hide the one signal a caller can see immediately. Go / Node / PHP follow
Java's convention (Java also writes only a file).

Two further deliberate design decisions:

1. **No port reuses Java's `rocketmq_client.log`.** When a JVM client runs on the same machine, two
   processes would interleave lines into one file; worse, rotation makes it unsafe — this port would
   rename `rocketmq_client.log` while the JVM still holds the old file descriptor, so the JVM keeps
   writing into a renamed file. Set `ROCKETMQ_CLIENT_LOG_FILE=rocketmq_client.log` if you explicitly
   want a port to share Java's file.
2. **Python and PHP do not write under `$HOME`.** Those two ports mostly run one-shot scripts and CI
   jobs; putting logs into the user's home directory is hard to clean up and easy to pollute, so
   they default to `logs/rocketmqlogs/` **under the current working directory**. The other five
   ports keep Java's `$HOME` convention.

Log write failures never affect send/receive: the port falls back to stderr and keeps working.

## Live cluster verification

Every language directory ships a set of live tools that are **not part of the unit suites**
(`python/verify_*_live.py`, `cpp/examples/rmq_*_live`, the `rmq` subcommands in `csharp`,
`rust/examples/live_*`, `go/examples/live_*`, `nodeJs/examples/live_*.ts`,
`php/examples/live_*.php`) covering the full send/receive chain, redelivery and DLQ, offset
management, flow control, POP, TLS and more. Every tool self-asserts and exits non-zero on failure;
each port's README lists them. `scripts/with_cluster.sh` checks NameServer / Broker readiness before
running a command.

There is also an **offline protocol self-check** available on six ports
(`python -m selfcheck`, `rmq_selfcheck` in `cpp`, the `selfcheck` subcommand in `csharp`,
`go run ./examples/selfcheck`, `node --experimental-strip-types selfcheck.ts`,
`php tests/run_all.php`): codec round-trips plus key constants and field names — the cheapest gate
before touching a real cluster. Rust has no such tool (`rust/examples/` is entirely cluster-dependent
`live_*`); its offline protocol coverage lives in `cargo test`.

**Coverage is not equal across the seven ports**: Python / C++ / C# / Rust each have 30+ live tools,
Go currently has 9 (send / consume / pull / lite pull / POP / redelivery & DLQ / shutdown race /
**admin** / compression matrix), Node.js has 5 and PHP has 7 (admin / redelivery & DLQ / POP / TLS /
request-reply / compression smoke / **pull**) — the gaps are enumerated in the
"live cluster" section of each port's README.

Some cases need extra cluster configuration (documented in each script's header comment): a
master/slave cluster (broker slave), `traceTopicEnable=true`, `enablePropertyFilter=true`,
`recallMessageEnable`, ACL, etc. Cases that stop the broker start it back up and restore the
configuration themselves.

Cross-language compression interoperability is one command:

```bash
scripts/compression_matrix.sh [zlib|lz4|zstd|all]
```

Every leg is "port A sends → port B only receives → compare crc32"; the leg passes only when the
receiver prints `match=1`. Exit codes: 0 = pass, 1 = fail, 2 = bad codec or usage, 3 = receive
timeout.

## Known differences from Java

Per-item comparisons against the Java 5.5.1 classic client live in
[`php-vs-java-client-diff.md`](php-vs-java-client-diff.md) (PHP) and the "differences from Java"
section of each port's README. Two things are deliberately not implemented, consistently across all
seven ports:

- `MQPullConsumerScheduleService`: `@Deprecated` in Java and provided by no port here — use
  `LitePullConsumer` (each port's lite pull) instead;
- Trace `enableSendMsgTrace` and a few read-only admin calls remain simplified implementations on a
  small number of ports; the specific port READMEs say exactly which.

## Repository layout

```
├── python/    Python implementation (sync API, pytest unit suite + verify_*_live.py scripts)
├── cpp/       C++17 implementation (CMake, ctest unit suite + examples live tools)
├── csharp/    C# / .NET 10 implementation (xunit tests + rmq subcommand live tools)
├── rust/      Rust/tokio implementation (inline tests + live_* example tools)
├── go/        Go implementation (sync API, zero deps, go test suite + examples live tools)
├── nodeJs/    Node.js/TypeScript implementation (zero deps, no build step, selfcheck + live_* scripts)
├── php/       PHP 8.1+ implementation (zero composer deps, single-threaded tick() model + live_* scripts)
├── scripts/   Cluster start/stop and cross-language interoperability scripts (compression_matrix.sh, …)
└── logs/      Client logs the Python / PHP live scripts write into the current directory
```

## License

Apache-2.0.
