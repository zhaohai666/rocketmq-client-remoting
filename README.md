# rocketmq-client-remoting

Apache RocketMQ **经典 remoting 协议**的多语言客户端 SDK：同一套协议语义、同一套消息模型，
在 **Python / C++ / C# / Rust / Go** 五种语言里各有一份完整实现
（Node.js / TypeScript 第六端已实现并真机验证，见 [`nodeJs/`](nodeJs/README.md)）。

客户端直接与 **NameServer + Broker** 通信，不经过代理层：

- 线协议是 remoting 协议的 **JSON 与 RocketMQ 二进制**双序列化（`RemotingCommand` 帧、
  V1/V2 header，V2 使用单字母短键）；
- 消息编解码是 **17 段存储格式 + 6 段批量格式**，与 RocketMQ 服务端落盘格式一致；
- 适配 RocketMQ 4.x / 5.x 集群，全量联调基于 **5.5.1**（NameServer 9876 + Broker 10911）。

五个实现之间的收发互通是**硬要求**：任一语言发出的消息（含 zlib / LZ4 / ZSTD 压缩体）
其余语言都能解开，`scripts/compression_matrix.sh` 会在真实集群上把 Python/C++/C#/Rust/Go
**五端**的压缩矩阵整体跑一遍（Go 只实现 ZLIB —— LZ4/ZSTD 刻意大声报错而不是透传，
所以非 zlib 的 codec 里含 Go 的腿会被跳过，那两种格式仍由另外四端互测覆盖，
见 [`go/README.md`](go/README.md)）。

## 能力总览

五个语言实现覆盖同一份能力面：

| 领域 | 内容 |
| --- | --- |
| 协议层 | `RemotingCommand` 帧编解码；JSON / 二进制双序列化；17 段 + 6 段消息编解码；V2 短键 header；fastjson2 风格容错解析 |
| 传输层 | 长连接惰性建连与复用、同步 / 异步 / oneway、半包重组、opaque 匹配、超时与重连、GO_AWAY 换连接重发一次、连接断开时在途请求立即判死、TLS |
| 发送 | 同步 / 定点 / 批量 / 单向 / 队列选择器 / 异步（真异步发送池 + 两个公平背压信号量）/ 事务消息（两阶段 + broker 回查）/ 定时消息撤回（recallMessage） |
| 消费 | Push Consumer（长轮询 + POP + 顺序 + 广播 + 位点持久化 + 启动期数值校验 + 拉取前流控五阈值 + 挂起 listener 的清扫逃生口）、Pull Consumer、Lite Pull Consumer（拉取/已消费/内存提交三张位点表） |
| 队列分配 | 六个可插拔策略：`AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>` |
| 管理端 | `DefaultMQAdminExt`：topic / 订阅组 / 集群信息 / 各类统计 / 消息查询 / 位点重置 / `searchOffset` 边界语义 |
| 消息类型 | 普通 / 顺序 / 延迟 / 事务 / 批量 / Request-Reply / 定时撤回 |
| 观测与安全 | 消息轨迹（编码 + 异步分发 + 钩子）、消费统计、`ConsumerRunningInfo`(307)、五类钩子、ACL 签名、动态 NameServer 取址、故障规避选队列 |
| 压缩 | zlib / LZ4 / ZSTD 三后端：生产端自动压缩 + 消费端自动解压，未支持类型必须报错而非透传压缩流 |

## 各语言 SDK

| 语言 | 目录 | 运行形态 | 单元测试 |
| --- | --- | --- | --- |
| Python | [`python/`](python/README.md) | 同步 API（内部线程） | `pytest -q`：1160 passed + 4 skipped |
| C++ | [`cpp/`](cpp/README.md) | C++17，手写网络层，无第三方运行时依赖 | `ctest`：50 个用例 / 3986 项断言 |
| C# | [`csharp/`](csharp/README.md) | C# / .NET 10，零 NuGet 依赖 | `dotnet test`：740 passed |
| Rust | [`rust/`](rust/README.md) | tokio 异步 API | `cargo test --lib`：891 条；`cargo clippy --all-targets` 零 warning |
| Go | [`go/`](go/README.md) | 同步 API（内部 goroutine），零第三方依赖，压缩 ZLIB-only | `go test ./...`：529 条；`go vet` / `gofmt` 零告警 |
| Node.js | [`nodeJs/`](nodeJs/README.md) | TypeScript 直接运行（`node --experimental-strip-types`），零第三方依赖，压缩 ZLIB-only | `node selfcheck.ts`：53 模块加载 + 4 套冒烟全绿；真机 5/5 live 全绿（`scripts/run_node_live.sh producer\|consumer\|pull\|lite_pull\|admin`） |

各语言 README 包含该实现的构建方式、快速上手、配置项（日志 / TLS / 压缩）、目录结构，
以及真机联调工具清单。

## 快速上手

五个语言都是同一套动作：建门面 → 设 NameServer 地址 → `start()` → 收发 → `shutdown()`。
集群要求：NameServer `9876` + Broker `10911`，`autoCreateTopicEnable=true` 时首次发送会
自动建 topic。

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

## 真实集群联调

每个语言目录下都带一组**不进单元测试**的真机联调工具（`python/verify_*_live.py`、
`cpp/examples/rmq_*_live`、`csharp` 的 `rmq` 子命令、`rust/examples/live_*`、
`go/examples/live_*`），覆盖收发全链路、重投与死信、位点管理、流控、POP、TLS 等。
全部工具自断言、失败以非 0 退出码收口，具体清单见各语言 README。

另有一个**不依赖集群**的协议层离线自检，Python / C++ / C# / Go 四端各有一份
（`python -m selfcheck`、`cpp` 的 `rmq_selfcheck`、`csharp` 的 `selfcheck` 子命令、
`go run ./examples/selfcheck`）：跑编解码回环与关键常量 / 字段名，是动真机之前最便宜的一道门。
Rust 侧没有这个工具（`rust/examples/` 全是需要集群的 `live_*`），其协议层离线覆盖由 `cargo test` 承担。

**覆盖面不是五端齐平的**：Python / C++ / C# / Rust 四端各有 30 余个真机工具，
Go 目前只有 9 个（发送 / 消费 / 拉取 / 轻量拉取 / POP / 重投与死信 / 停机竞态 / **管理端** /
压缩矩阵），其余场景 Go 侧待补 ——
`go/README.md` 的「真实集群联调」一节逐项列出未覆盖清单。

部分用例有额外要求（在脚本头注释里写明）：主从集群（Broker 从节点）、
`traceTopicEnable=true`、`enablePropertyFilter=true`、`recallMessageEnable`、
ACL 鉴权等；停 broker 类用例会自行拉起并把配置还原。

## 仓库布局

```
├── python/    Python 实现（同步 API，pytest 单测 + verify_*_live.py 真机脚本）
├── cpp/       C++17 实现（CMake，ctest 单测 + examples 真机工具）
├── csharp/    C# / .NET 10 实现（xunit 单测 + rmq 子命令真机工具）
├── rust/      Rust/tokio 实现（内联单测 + live_* 示例真机工具）
├── go/        Go 实现（同步 API 零依赖，go test 单测 + examples 真机工具）
└── scripts/   集群启停与跨语言互测脚本（compression_matrix.sh 等）
```

## License

Apache-2.0。
