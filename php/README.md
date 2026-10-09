# PHP 客户端（RocketMQ 4.x/5.x 经典 remoting 协议）

> 中文 ｜ [English](README.en.md)

`php/` 是本仓库的第七端：与 `python/` 同一分层、同一协议语义，**不依赖任何 composer 包**，
只用 `ext-json` / `ext-sockets` / `ext-openssl` / `ext-mbstring`。移植过程中必须遵守的约定
（命名、线格式、单线程适配、类名冲突守卫）写在 [`PORTING.md`](PORTING.md)，与 Java 的能力差
逐项对照写在 [`../php-vs-java-client-diff.md`](../php-vs-java-client-diff.md)。

- 运行要求：PHP **8.1+**（开发机实测 8.1.34；`PORTING.md` 的 8.3 是目标口径，代码没有用到 8.2+ 语法）
- 体量：`src/` 69 文件 / 约 3.0 万行；`tests/` 14 文件 / 约 1.2 万行
- 线协议：`RemotingCommand` 的 **JSON 与 ROCKETMQ 二进制**双序列化；消息是 **17 段存储格式 + 6 段批量格式**
- 真机基线：RocketMQ **5.5.1**（NameServer 9876 + Broker 10911）

## 单线程模型（读代码前先建立的心智模型）

PHP 没有线程，所以其他端口里由后台线程承担的工作全部收敛成**调用方驱动的 `tick()`**：

| 其他端口的线程 | 本端口的形态 |
| --- | --- |
| 心跳 30s / 重平衡 20s / 位点落盘 10s(+5s) / 顺序锁 20s | `PushConsumer::tick()` 内按到期时间执行；启动期无分配时 2s 快重试 |
| 每队列长轮询线程 | **短轮询**（`suspend=false`）+ 空结果按 `pullIntervalMillis` 退避 + 每队列每 tick 至多 `maxPullsPerQueuePerTick` 轮 |
| 消费挂起（Java 的 `submitConsumeRequestLater` sleep） | `suspendedUntil` 表，到点才再投递 |
| POP 的 pollTime 长轮询 | pollTime=0 短轮询 + 批次内联执行 |
| POP 的队列由谁决定 | Java 在 `clientRebalance=false` 时问 broker（`QUERY_ASSIGNMENT(400)` 回 `MessageQueueAssignment` mode=POP，`RebalanceImpl#getRebalanceResultFromBroker:345`）；本端口**恒走客户端 rebalance**：本地分配策略算出队列，再每队列一个 POP 循环 + ack。语义等价，只差在"谁决定队列集合"——七端同一刻意决定 |
| `invokeAsync` 的回调线程 | 非阻塞写 + pending 表（opaque→回调），`waitResponses()` 用 `stream_select` 泵 |

> **为什么消费者必须自己驱动 `tick()`**：PHP 进程没有可后台跑的常驻线程，任何"等下次心跳"
> 的语义如果留在线程里就永远不会发生。调用方循环 `tick()` 是本端口唯一的重平衡/位点/心跳入口
> ——真机脚本（`examples/live_*.php`）全部按这个模式写，接入方照抄即可。

## 快速上手

```php
<?php
declare(strict_types=1);
require __DIR__ . '/bootstrap.php';   // PSR-4 + 一次性 classmap 兜底，无需 composer

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
while (true) {          // 调用方驱动：本端口的"后台线程"就是这个循环
    $consumer->tick();
    usleep(100_000);
}
```

## 能力覆盖

与 `python/` / `go/` / `nodeJs/` 同一张能力面，逐项对照见
[`../php-vs-java-client-diff.md`](../php-vs-java-client-diff.md)：

| 领域 | 内容 |
| --- | --- |
| 协议 / 传输 | `RemotingCommand` 双序列化（JSON + ROCKETMQ 二进制）、17 段与 6 段消息编解码、V2 单字母短键、sync/async/oneway、opaque 匹配、半包重组、GO_AWAY 换连接重发、TLS |
| 发送 | 同步 / 定点 / 队列选择器 / 批量（含 `ProduceAccumulator` 攒批 + 两级公平背压）/ oneway / 异步 / 事务（两阶段 + broker 回查 39）/ 定时撤回（370，`RecallMessageHandle`）/ Request-Reply（325 发送 + 326 推回 + 等待槽 TTL 扫描） |
| 消费 | Push（并发 / 顺序 / 广播 / 长轮询语义的短轮询实现 / 位点持久化 / 启动期数值校验 / 拉取前流控五阈值）、Pull（`fetchSubscribeMessageQueues` 给整个 topic，`fetchMessageQueuesInBalance` 只给本实例应得的那份）、LitePull、POP（200050/200051/200052 + 检查点）、`ConsumeMessageDirectly`(309) |
| 队列分配 | `AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY`，由真实重平衡驱动 |
| 命名空间 | 两套彼此独立：`namespace`（客户端本地资源名前缀 `%%ns%%res`，`Common/Namespace.php`，收发 / 心跳 / 位点全线包装与还原）与 `namespaceV2`（服务端命名空间，`Remoting/RpcHooks.php` 的 `NamespaceRpcHook` 给每笔请求盖 `nsd=true` / `ns=<值>`；顺序 Namespace → Stream → ACL，故 `ns`/`ReqT` 都在 ACL 签名内容里；值为空时一个字段都不写）。生产者 / 三种消费者 / 管理端都暴露 `namespaceV2`，且是**每笔请求现读**而非启动期快照 |
| 管理端 | topic / 订阅组 CRUD、集群与 broker 运行时信息、消费统计与进度、按 key / uniqKey / msgId 查消息、位点读取与重置、`searchOffset` 边界、name server 配置 318/319 |
| 观测 | 消息轨迹（生产/消费/回查三类 + 异步分发器 + Java `TraceDataEncoder` 逐字节编码）、消费统计、`ConsumerRunningInfo`(307)、五类钩子、**`OpenTracingHook`**（Java 的 OpenTracing 三件套在本端口按同名接口实现，其余端口以 W3C `traceparent` 替代）、W3C `traceparent` 注入与透传 |
| 安全与寻址 | ACL 签名（`HmacSHA1`，签名内容与 Java 逐字节一致）、动态 name server 取址（HTTP 域 + `NsAddr` 文件）、故障规避选队列（`Latency.php`）、`Validators` 本地校验前置 |
| 压缩 | `MessageSysFlag` 类型位三型齐全：ZLIB（`gzcompress`/RFC1950）、LZ4（**纯 PHP 的 LZ4 Frame 格式**，与 Java `lz4-java` / Python `lz4.frame` / lz4 CLI 同 wire）、ZSTD（优先 `zstd` CLI 真压缩，缺 CLI 时退回纯实现的 Raw/RLE 帧）；阈值 4096、防二次压缩、解压后清类型位，**解不出的一律抛异常，绝不把压缩流当正文透传** |

## 目录结构

```
php/
├── bootstrap.php              PSR-4 autoloader + 重复类名静态守卫的入口（无需 composer）
├── composer.json              仅声明 ext-* 与 autoload 映射，不引入任何依赖
├── PORTING.md                 移植约定（命名 / 线格式 / 单线程适配 / 类名唯一性）
├── src/
│   ├── Common/                消息模型、编解码、压缩（CompressionCodec）、命名空间、常量、校验
│   ├── Remoting/              RemotingClient、RpcHooks、Protocol/（帧、两路序列化、请求响应头与体）
│   └── Client/                Producer / PushConsumer / PullConsumer / Admin / 分配策略 /
│                              轨迹 / Latency / Backpressure / Logger / Exceptions
├── examples/                  真机验证工具（live_*，失败一律非 0 退出码收口）
└── tests/                     离线自测（run_all.php 串起各套件）
```

## 离线自测

```bash
cd php
php tests/run_all.php          # 全部套件，实测 1590 项检查全绿
php tests/RunCommon.php        # 单套件（RunRemoting / RunClient* / RunCommon）
```

`run_all.php` 除了逐套件断言，还跑一道**重复类名守卫**：PHP 没有模块隔离，同一 FQCN 在两个
文件里声明时，先被 autoload 命中的胜出、另一个静默失效，所以这一步不能省（改名规则见
`PORTING.md`，例如 `TraceContextPropagator` 就是为了避开 Java `trace.TraceContext` 占用的类名）。

## 真机验证

```bash
bash scripts/run_php_live.sh admin            # 管理端逐项断言
bash scripts/run_php_live.sh redelivery       # 重投 / 死信 / 顺序死信（legs=all|s1,s2,s3,s4）
bash scripts/run_php_live.sh pull             # 拉模式消费者：队列 / fetchMessageQueuesInBalance /
                                              # 手动拉取 / 位点提交回读（legs=all|s1,s2,s3）
bash scripts/run_php_live.sh pop              # POP 消费（需要 POP 专用 broker 配置，脚本自带）
bash scripts/run_php_live.sh tls              # plain_tls / ca_verify / mtls 三腿
bash scripts/run_php_live.sh request_reply    # 326 推回链路
bash scripts/run_php_live.sh compression      # 本端 zlib/lz4/zstd 自测冒烟

# 跨语言压缩矩阵（本端 php_* 腿，与其余六端互压互解）
bash scripts/compression_matrix.sh zlib       # lz4 / zstd 同脚本换 codec
```

`run_php_live.sh` 会自己拉起 / 复用 5.5.1 集群，并且**只停自己起的那一次**；`pop` 与 `tls`
需要独占集群（进程级全局开关），别与其他端的真机脚本并行跑。

## 日志

`RocketMQ\Client\Logger` 默认**落盘**（对齐 Java 客户端会生成 `rocketmq_client.log` 的行为），
与其余端口同一套环境变量口径：

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` |
| `ROCKETMQ_CLIENT_LOG_DIR` | **`<当前工作目录>/logs/rocketmqlogs`** | 日志目录 |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_php_client.log` | 文件名；空串 / `OFF` / `NONE` = 关闭文件落盘；含路径分隔符按整路径处理 |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | 空 | 任意非空值 = 只写 stderr |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864`（64MB） | 按大小轮转，Java logback 同值；`0` = 不轮转 |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | 备份份数，备份名 `<file>.1` … `<file>.N` |

**为什么不跟 Java 一样写 `$HOME/logs/rocketmqlogs`**：PHP 客户端常在别人的宿主进程 / CLI 脚本里
跑，在用户 HOME 下悄悄建目录、写文件是越界副作用；落在当前工作目录则跟着部署走，容器里天然可
预期（与 `python/rocketmq_logging.py` 同一取舍）。需要 Java 口径时显式设
`ROCKETMQ_CLIENT_LOG_DIR=$HOME/logs/rocketmqlogs`。

文件名也刻意不叫 `rocketmq_client.log`：同机同时跑 Java 客户端时两边会往一个文件里插行，且谁
先轮转就把对方的文件改名了，JVM 仍持有旧 fd，之后日志静默写进已 unlink 的 inode。

程序内可直接改：`Logger::setLevel()` / `Logger::setHandler()`（注入 callable，单测用）/
`Logger::logFilePath()`（诊断与真机脚本"是否真的写出文件"的断言点）。

> 本机环境噪声：每个 `php` 进程启动都会打两行 `[CQ_POLLER] Background thread created/started`
> （宿主 php.ini 的 auto_prepend，与本客户端无关），排查日志时先排除它。

## TLS

`setTlsEnable(true)` 打开后所有出连接走 TLS，与 Java 一样是**进程级**开关（NameServer 连接也走
TLS）。证书口径：CA 校验（`caCertPath` / `serverName`）、可选双向认证（`clientCertPath` /
`clientKeyPath`），默认严格校验主机名与证书链，撤销检查与其余端口一致。`scripts/run_php_live.sh tls`
会用 `openssl` 现造 CA / server(SAN:127.0.0.1,localhost) / client 证书并跑三腿，其中
「明文连 TLS 端口必须被拒」是必测对照腿。
