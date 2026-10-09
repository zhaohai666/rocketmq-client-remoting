# rocketmq-client-remoting

> 中文 ｜ [English](README.en.md)

Apache RocketMQ **经典 remoting 协议**的多语言客户端 SDK：同一套协议语义、同一套消息模型，
在 **Python / C++ / C# / Rust / Go / Node.js(TypeScript) / PHP** 七种语言里各有一份完整实现。

客户端直接与 **NameServer + Broker** 通信，不经过代理层：

- 线协议是 remoting 协议的 **JSON 与 RocketMQ 二进制**双序列化（`RemotingCommand` 帧、
  V1/V2 header，V2 使用单字母短键）；
- 消息编解码是 **17 段存储格式 + 6 段批量格式**，与 RocketMQ 服务端落盘格式一致；
- 适配 RocketMQ 4.x / 5.x 集群，全量联调基于 **5.5.1**（NameServer 9876 + Broker 10911）；
- 七端**全部零第三方运行时依赖**（Rust 除外：它用 `tokio` 提供 async API）。

七个实现之间的收发互通是**硬要求**：任一语言发出的消息（含 zlib / LZ4 / ZSTD 压缩体）
其余语言都能解开。`scripts/compression_matrix.sh` 会在真实集群上把七端的压缩矩阵整体跑一遍
（当前 zlib / lz4 / zstd 三轮 `fail=0`）。各端的 LZ4 / ZSTD **都是本端手写的实现或系统内置能力**，
不引第三方包，因此能力面并不完全齐平，逐项见「各语言 SDK」一表与对应语言 README。

## 能力总览

七个语言实现覆盖同一份能力面：

| 领域 | 内容 |
| --- | --- |
| 协议层 | `RemotingCommand` 帧编解码；JSON / 二进制双序列化；17 段 + 6 段消息编解码；V2 短键 header；fastjson2 风格容错解析 |
| 传输层 | 长连接惰性建连与复用、同步 / 异步 / oneway、半包重组、opaque 匹配、超时与重连、在途请求超时扫描、GO_AWAY 换连接重发一次、连接断开时在途请求立即判死、TLS |
| 发送 | 同步 / 定点 / 批量 / 单向 / 队列选择器 / 异步（真异步发送池 + 两个公平背压信号量）/ 事务消息（两阶段 + broker 回查）/ 定时消息撤回（recallMessage） |
| 消费 | Push Consumer（长轮询 + POP + 顺序 + 广播 + 位点持久化 + 启动期数值校验 + 拉取前流控五阈值 + 挂起 listener 的清扫逃生口）、Pull Consumer（`fetchSubscribeMessageQueues` 给整个 topic，`fetchMessageQueuesInBalance` 只给本实例应得的那份——七端同一条分配公式，算不动时保留现有分配、绝不退化成「全要」）、Lite Pull Consumer（拉取/已消费/内存提交三张位点表） |
| 队列分配 | 六个可插拔策略：`AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>` |
| 命名空间 | 两套彼此独立、七端都有：`namespace`（客户端本地资源名前缀 `%%ns%%res`，Java `NamespaceUtil` 口径，收发 / 心跳 / 位点全线包装与还原）与 `namespace_v2`（服务端命名空间，`NamespaceRpcHook` 给每笔请求盖 `nsd=true` / `ns=<值>`，钩子顺序 Namespace → Stream → ACL，所以 `ns` 与 `ReqT` 都进 ACL 签名内容；值为空时一个字段都不写）。生产端 / 三种消费者 / 管理端都**每笔请求现读**，不是启动期快照 |
| 管理端 | `DefaultMQAdminExt`：topic / 订阅组 / 集群信息 / 各类统计 / 消息查询 / 位点重置 / `searchOffset` 边界语义 |
| 消息类型 | 普通 / 顺序 / 延迟 / 事务 / 批量 / Request-Reply / 定时撤回 |
| 观测与安全 | 消息轨迹（编码 + 异步分发 + 钩子，含 recall 轨迹钩子）、消费统计、`ConsumerRunningInfo`(307)、五类钩子、ACL 签名、命名空间 RPC 钩子（`nsd` / `ns`，排在 ACL 签名之前）、动态 NameServer 取址、故障规避选队列 + broker 可达性探测线程 |
| 压缩 | zlib / LZ4 / ZSTD 三后端：生产端自动压缩 + 消费端自动解压，未支持类型必须报错而非透传压缩流 |
| 日志 | 每端都会落**客户端日志文件**（对齐 Java 的 `rocketmq_client.log`），同一套 `ROCKETMQ_CLIENT_LOG_*` 环境变量，按大小轮转 + 固定备份窗口 |

## 各语言 SDK

| 语言 | 目录 | 运行形态 | 压缩 | 单元测试 |
| --- | --- | --- | --- | --- |
| Python | [`python/`](python/README.md) | 同步 API（内部线程） | zlib / LZ4 / ZSTD | `pytest -q`：1163 passed + 4 skipped |
| C++ | [`cpp/`](cpp/README.md) | C++17，手写网络层，无第三方运行时依赖 | zlib / LZ4 / ZSTD | `ctest`：50 个用例 / 3993 项断言 |
| C# | [`csharp/`](csharp/README.md) | C# / .NET 10，零 NuGet 依赖 | zlib / LZ4 / ZSTD | `dotnet test`：789 passed |
| Rust | [`rust/`](rust/README.md) | tokio 异步 API | zlib / LZ4 / ZSTD | `cargo test --lib`：916 条；`cargo clippy --all-targets` 零 warning |
| Go | [`go/`](go/README.md) | 同步 API（内部 goroutine），零第三方依赖 | zlib / LZ4 双向；**ZSTD 解码全格式、编码 store-only** | `go test ./...`：502 条；`go vet` / `gofmt` 零告警 |
| Node.js | [`nodeJs/`](nodeJs/README.md) | TypeScript 直接运行（`node --experimental-strip-types`），零第三方依赖 | zlib / LZ4 双向手写 frame；ZSTD 走 `node:zlib`（≥ 23.8），低版本退回 Raw/RLE 手写 | `node selfcheck.ts`：53 模块加载 + 4 套冒烟全绿；真机 `scripts/run_node_live.sh producer\|consumer\|pull\|lite_pull\|admin` 全绿 |
| PHP | [`php/`](php/README.md) | PHP 8.1+，单线程 `tick()` 驱动，零 composer 依赖 | zlib 走 `gzcompress`；LZ4 纯 PHP frame；ZSTD 优先 `zstd` CLI，退回纯 PHP Raw/RLE | `php tests/run_all.php`：1590 项断言 + 重复类名守卫 |

> Go 的 ZSTD 编码是 **store-only Raw/RLE 块**：产出合法 zstd 帧、其他端能解开，但压缩比接近 1:1，
> 真机矩阵里 `storeSize≈消息体大小` 就是它的证据。这是「零依赖」与「压缩比」之间的取舍，
> 不是缺陷；需要真实压缩比时用另外六端。

各语言 README 包含该实现的构建方式、快速上手、配置项（日志 / TLS / 压缩）、目录结构，
以及真机联调工具清单。

## 快速上手

七种语言都是同一套动作：建门面 → 设 NameServer 地址 → `start()` → 收发 → `shutdown()`。
集群要求：NameServer `9876` + Broker `10911`，`autoCreateTopicEnable=true` 时首次发送会
自动建 topic。

> 跨端联调的一条硬规则：**先起消费者、再发消息**。Push Consumer 只消费订阅后落入队列的消息，
> 顺序颠倒会看起来"丢消息"。

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
# ... 收到 SIGINT 后
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
    // ... 收到退出信号后
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
// ... 收到退出信号后
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
    // ... 收到退出信号后
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
	// ... 收到退出信号后
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
// ... 收到退出信号后
consumer.shutdown();
```

运行方式（无需构建）：`node --experimental-strip-types your_file.ts`。

### PHP

PHP 没有常驻线程，心跳 / 重平衡 / 位点落盘全部收敛为调用方驱动的 `tick()`：

```php
<?php
require __DIR__ . '/bootstrap.php';   // PSR-4 + classmap 兜底，不需要 composer

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
while (true) {          // 这个循环就是本端口的"后台线程"
    $consumer->tick();
    usleep(100_000);
}
```

## 客户端日志

Java 客户端会在 `$HOME/logs/rocketmqlogs/rocketmq_client.log` 落一份客户端日志，
七个实现全部对齐这个行为：**默认写文件**、会轮转、不会撑爆磁盘。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `TRACE` / `DEBUG` / `INFO` / `WARN` / `ERROR` |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs`（Python / PHP 为 `<cwd>/logs/rocketmqlogs`） | 日志目录 |
| `ROCKETMQ_CLIENT_LOG_FILE` | 每端专属文件名 | 名字含路径分隔符时按整条路径处理；空值 / `OFF` / `NONE` = 关闭文件日志 |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | 见下方三组口径 | 控制台输出的开关 |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | 64MB | 按大小轮转的端口用（C++ / C# / Go / Node.js / PHP） |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | 10 | 备份份数；按大小轮转的端口里 `0` = 原地截断 |

| 语言 | 默认日志文件 | 轮转 |
| --- | --- | --- |
| Python | `<当前工作目录>/logs/rocketmqlogs/rocketmq_py_client.log` | 按天，备份名 `.YYYY-MM-DD` |
| C++ | `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log` | 按大小，`.1` … `.N` |
| C# | `$HOME/logs/rocketmqlogs/rocketmq_csharp_client.log` | 按大小，`.1` … `.N` |
| Rust | `$HOME/logs/rocketmqlogs/rocketmq_rs_client.log` | 按天，备份名 `.YYYY-MM-DD` |
| Go | `$HOME/logs/rocketmqlogs/rocketmq_go_client.log` | 按大小，`.1` … `.N` |
| Node.js | `$HOME/logs/rocketmqlogs/rocketmq_node_client.log` | 按大小，`.1` … `.N` |
| PHP | `<当前工作目录>/logs/rocketmqlogs/rocketmq_php_client.log` | 按大小，`.1` … `.N` |

`ROCKETMQ_CLIENT_LOG_USE_STDOUT` 在三组端口上的口径不同，这是**有意**的，不是没对齐：

| 组 | 端口 | 控制台行为 |
| --- | --- | --- |
| 文件 + stderr 同时写 | C++ / C# | 没有这个变量：stderr 永远写，只想留 stderr 就把 `ROCKETMQ_CLIENT_LOG_FILE=OFF` |
| 二选一 | Go / Node.js / PHP | 变量置任意非空值 = 只写标准输出、**不建**日志文件；不设 = 只写文件 |
| 文件 + 控制台同时写，可关控制台 | Python / Rust | 默认两边都写；`ROCKETMQ_CLIENT_LOG_USE_STDOUT=false` 关掉控制台 |

后两组的差别来自"谁会看这个进程的标准输出"：Python / Rust 客户端常被嵌进别人的进程或库，
静默写文件会让调用方失去唯一能立刻看见的信号，所以默认同时输出；Go / Node / PHP 的口径直接对齐
Java（Java 也只写文件）。

另外两点刻意的设计：

1. **文件名不带 Java 的 `rocketmq_client.log`**。同机同时跑 JVM 客户端时，两个进程会往同一个
   文件里插行；更糟的是轮转：本端把 `rocketmq_client.log` 改名时 JVM 仍持有旧 fd，
   日志会写进已经被改名的文件里。想让某个端和 Java 同名，设
   `ROCKETMQ_CLIENT_LOG_FILE=rocketmq_client.log` 即可。
2. **Python 与 PHP 不写 `$HOME`**。这两个端口常被用来跑一次性脚本和 CI，把日志写进用户主目录
   既难清理也容易污染；它们默认落在**当前工作目录**下的 `logs/rocketmqlogs/`，随项目走。
   其余五端保持 Java 的 `$HOME` 口径。

日志写入失败永远不会影响收发：本端会退回 stderr 并继续工作。

## 真实集群联调

每个语言目录下都带一组**不进单元测试**的真机联调工具（`python/verify_*_live.py`、
`cpp/examples/rmq_*_live`、`csharp` 的 `rmq` 子命令、`rust/examples/live_*`、
`go/examples/live_*`、`nodeJs/examples/live_*.ts`、`php/examples/live_*.php`），
覆盖收发全链路、重投与死信、位点管理、流控、POP、TLS 等。
全部工具自断言、失败以非 0 退出码收口，具体清单见各语言 README。
`scripts/with_cluster.sh` 会检查 NameServer / Broker 是否就绪再执行命令。

另有一个**不依赖集群**的协议层离线自检，Python / C++ / C# / Go / Node.js / PHP 六端各有一份
（`python -m selfcheck`、`cpp` 的 `rmq_selfcheck`、`csharp` 的 `selfcheck` 子命令、
`go run ./examples/selfcheck`、`node --experimental-strip-types selfcheck.ts`、
`php tests/run_all.php`）：跑编解码回环与关键常量 / 字段名，是动真机之前最便宜的一道门。
Rust 侧没有这个工具（`rust/examples/` 全是需要集群的 `live_*`），其协议层离线覆盖由 `cargo test` 承担。

**覆盖面不是七端齐平的**：Python / C++ / C# / Rust 四端各有 30 余个真机工具，
Go 目前只有 9 个（发送 / 消费 / 拉取 / 轻量拉取 / POP / 重投与死信 / 停机竞态 / **管理端** /
压缩矩阵），Node.js 5 个、PHP 7 个（管理端 / 重投与死信 / POP / TLS / 请求-应答 / 压缩冒烟 /
**拉取**）—— 未覆盖清单在各自 README 的「真实集群联调」一节逐项列出。

部分用例有额外要求（在脚本头注释里写明）：主从集群（Broker 从节点）、
`traceTopicEnable=true`、`enablePropertyFilter=true`、`recallMessageEnable`、
ACL 鉴权等；停 broker 类用例会自行拉起并把配置还原。

跨语言压缩互通用一条命令验证：

```bash
scripts/compression_matrix.sh [zlib|lz4|zstd|all]
```

每条腿都是「A 端发 → B 端只收 → 比对 crc32」，接收端 `match=1` 才算通过；
退出码 0=通过、1=失败、2=codec 或用法错误、3=接收超时。

## 与 Java 的已知差异

各端与 Java 5.5.1 经典客户端的逐项对照见 [`php-vs-java-client-diff.md`](php-vs-java-client-diff.md)
（PHP 端）与各语言 README 的「与 Java 的差异」小节。刻意不实现的两处，七端一致：

- `MQPullConsumerScheduleService`：Java 侧已 `@Deprecated`，本仓库七端都不提供，
  用 `LitePullConsumer`（各端的 lite pull）替代；
- 轨迹的 `enableSendMsgTrace` / 部分 admin 只读接口在少数端仍是简化实现，具体写在对应 README。

## 仓库布局

```
├── python/    Python 实现（同步 API，pytest 单测 + verify_*_live.py 真机脚本）
├── cpp/       C++17 实现（CMake，ctest 单测 + examples 真机工具）
├── csharp/    C# / .NET 10 实现（xunit 单测 + rmq 子命令真机工具）
├── rust/      Rust/tokio 实现（内联单测 + live_* 示例真机工具）
├── go/        Go 实现（同步 API 零依赖，go test 单测 + examples 真机工具）
├── nodeJs/    Node.js/TypeScript 实现（零依赖、免构建直接运行，selfcheck + live_* 真机脚本）
├── php/       PHP 8.1+ 实现（零 composer 依赖，单线程 tick() 模型 + live_* 真机脚本）
├── scripts/   集群启停与跨语言互测脚本（compression_matrix.sh 等）
└── logs/      Python / PHP 真机脚本在当前目录下落出的客户端日志
```

## License

Apache-2.0。
