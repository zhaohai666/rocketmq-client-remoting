# RocketMQ PHP 客户端

> 中文 ｜ [English](README.en.md)

## 概述

本目录是 **Apache RocketMQ 经典 remoting 协议（4.x/5.x）的 PHP 客户端 SDK**：直连
NameServer + Broker，不依赖 Proxy。

- 运行要求 **PHP >= 8.1**（实测 8.1.34）；**零 composer、零第三方依赖**，只用
  `ext-json` / `ext-sockets` / `ext-openssl` / `ext-mbstring`，PSR-4 自动加载加
  `bootstrap.php` 的全量 classmap 兜底，`require` 一个文件即可用全部能力。
- PHP 没有常驻线程，心跳、重平衡、位点持久化、轨迹分发等全部收敛为**由调用方循环驱动
  的 `tick()`**（见「快速上手」的执行模型说明）。
- 真机基线：RocketMQ **5.5.1**（NameServer 9876 + Broker 10911）。
- 体量：`src/` 69 个文件 / 30,642 行，`tests/` 15 个文件 / 12,831 行。

## 先决条件

- PHP **8.1+**，启用 `json`、`sockets`、`openssl`、`mbstring` 扩展。
- 一个可用的 NameServer + Broker（或复用 `scripts/run_php_live.sh` 自管的本地测试集群）。
- 可选：`zstd` 命令行工具。ZSTD 压缩优先调用 `zstd` CLI 做真压缩，缺 CLI 时自动退回
  内置的纯 PHP Raw/RLE 帧实现，功能不中断。LZ4 不需要任何 CLI。

## 安装与开发

SDK 以源码方式集成：把 `php/` 放进项目，`require` 它的 `bootstrap.php` 即可；
`composer.json` 只声明扩展与 autoload 映射，`composer install` 不是必需步骤。

```php
<?php
require __DIR__ . '/php/bootstrap.php'; // 无需 composer，任何入口 include 即可用
```

开发期常用命令（均在 `php/` 目录下执行）：

```bash
cd php

# 改动后对相关文件做语法检查
php -l src/Client/Producer.php

# 离线自测入口：静态守卫 + 10 个套件顺序执行，不连真机
php tests/run_all.php

# 单个套件
php tests/RunCommon.php
php tests/RunRemoting.php

# 压缩冒烟（需要集群，本端 zlib/lz4/zstd 自压自解一条腿）
bash scripts/run_php_live.sh compression 127.0.0.1:9876
```

`tests/run_all.php` 是**离线**套件：跑之前先做一道全仓**重复类名静态守卫**（PHP 没有
模块隔离，同一 FQCN 在两个文件声明时先被 autoload 命中的胜出、另一个静默失效，所以这道
守卫不能省），然后串起 10 个套件。当前实测：**守卫 CLEAN，10 个套件 1,674 项断言全部
通过**，任一失败即以非零码退出。

## 快速上手

### 执行模型：调用方驱动的 `tick()`

PHP 没有线程，所以其他部署形态里由后台线程承担的周期性工作，在本 SDK 里全部收敛到
**消费者对象的 `tick()`**：心跳、重平衡、位点持久化、拉取轮次、轨迹分发都在 `tick()` 内
按各自到期时间执行。因此消费者进程必须自己跑一个主循环：

```php
while (running()) {
    $consumer->tick();   // 本 SDK 唯一的重平衡 / 位点 / 心跳入口
    usleep(100_000);
}
```

**注意：`poll()` 不会调用 `tick()`。** `poll()` 只从本地缓冲取消息并推进已消费游标；
填充缓冲的拉取、队列集合的变更、向 broker 的心跳与位点提交都只发生在 `tick()` 里。
对 LitePull / Push 消费者，**每一轮都要先 `tick()` 再 `poll()`**（或 `tick()` 与业务处理
交替），只 `poll()` 不 `tick()` 会让消费者饿死在旧缓冲上。真机脚本 `examples/live_*.php`
全部按这个模式写，接入方照抄即可。

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

// 普通消息（可带 tag / keys：new Message(topic, body, tags, keys)）
$result = $producer->send(new Message('TopicTest', 'hello php'));
echo $result->getMsgId(), PHP_EOL;
```

**顺序消息**——定点发到某个队列，或按 key 用选择器稳定选队：

```php
$producer->send($msg, timeoutMillis: 3000, mq: $messageQueue);          // 定点
$producer->sendBySelector($msg, new SelectMessageQueueByHash(), $orderId); // 按 arg 哈希
```

**延迟 / 定时消息**——`DELAY` 走延迟级别，`TIMER_*` 走定时消息：

```php
$msg->setDelayTimeLevel(3);                       // 10s 后投递（延迟级别 3）
$msg->putProperty('TIMER_DELAY_SEC', '60');       // 60 秒后投递
$msg->putProperty('TIMER_DELAY_MS', '60000');
$msg->putProperty('TIMER_DELIVER_MS', (string) (time() + 60) * 1000);
```

**批量 / 攒批 / 单向 / 异步**：

```php
$producer->send([new Message('TopicTest', 'a'), new Message('TopicTest', 'b')]); // 批量

$producer->sendOneway(new Message('TopicTest', 'fire-and-forget'));

$producer->sendAsync(new Message('TopicTest', 'hi'), new class implements SendCallback {
    public function onSuccess(?SendResult $r): void { /* ... */ }
    public function onException(\Throwable $e): void { /* ... */ }
});
```

`setAutoBatch(true)` 后 `send()` / `sendAsync()` 会经过 `ProduceAccumulator` 自动攒批，
并受两级公平背压（`setEnableBackpressureForAsyncMode` / `setBackPressureForAsyncSendNum` /
`setBackPressureForAsyncSendSize`）控制。

**事务消息**：

```php
use RocketMQ\Client\LocalTransactionState;
use RocketMQ\Client\TransactionListener;
use RocketMQ\Client\TransactionMQProducer;
use RocketMQ\Common\MessageExt;

$tx = new TransactionMQProducer('PID_TX');
$tx->setTransactionListener(new class implements TransactionListener {
    public function executeLocalTransaction(Message $msg, mixed $arg): LocalTransactionState
    {
        return LocalTransactionState::COMMIT_MESSAGE;   // 或 ROLLBACK_MESSAGE / UNKNOW
    }
    public function checkLocalTransaction(MessageExt $msg): LocalTransactionState
    {
        return LocalTransactionState::COMMIT_MESSAGE;   // broker 回查时补判
    }
});
$tx->start();
$tx->sendMessageInTransaction($msg);
```

**Request-Reply**——发起方 `request()`，应答方在消费回调里 `reply()`。
PHP 单线程，应答方与发起方建议分两个进程（`examples/live_request_reply.php` 即两进程编排）：

```php
// 应答方：PushConsumer 收到请求后，用自己的 producer 把应答写回请求方连接
$response = $requester->request($msg, 10_000);   // 发起方：阻塞拿回应答 Message

// 应答方 listener 内：
$replyProducer->reply($requestMsg, 'reply-body');
```

**定时消息撤回（recallMessage）**——句柄只有定时消息才有：

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
$consumer->subscribe('TopicTest', '*');            // 第二个参数可为 tag / SQL92 表达式
$consumer->setMessageListener(new DemoListener());
$consumer->start();
while (true) {
    $consumer->tick();                              // 心跳 / 重平衡 / 拉取 / 位点都在这里
    usleep(100_000);
}
// 退出前 $consumer->shutdown();
```

顺序消费改用 `MessageListenerOrderly` / `ConsumeOrderlyStatus`；广播、起始位点、线程数值等
通过 `setMessageModel` / `setConsumeFromWhere` / `setConsumeThreadMin` 等配置。挂起与恢复用
`suspend()` / `resume()` / `isPaused()`，挂起期间 `tick()` 不再投递。

### PullConsumer（DefaultMQPullConsumer）

拉模式：自己控队列、自己控位点。`registerTopic()` 必须在 `start()` **之前**调用（心跳订阅集
只在注册时才带上该 topic）；位点回读用 `fetchConsumeOffset()`。

```php
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\PullStatus;

$pulled = new DefaultMQPullConsumer('GID_PULL');
$pulled->setNamesrvAddr('127.0.0.1:9876');
$pulled->registerTopic('TopicTest');
$pulled->start();

$mqs = $pulled->fetchSubscribeMessageQueues('TopicTest');      // 整个 topic 的队列
$mine = $pulled->fetchMessageQueuesInBalance('TopicTest');     // 只取本实例应得的份额
foreach ($mine as $mq) {
    $pulled->tick();
    $result = $pulled->pull($mq, '*', offset: 0, maxNums: 32);
    if ($result->status === PullStatus::FOUND) {
        $pulled->updateConsumeOffset($mq, $result->nextBeginOffset);
    }
}
```

### LitePullConsumer（DefaultLitePullConsumer）

`subscribe()`（自动重平衡）或 `assign()`（显式指定队列），`poll()` 从本地缓冲取消息，
`commit()` 提交位点，`seek()` 拨回游标。**每一轮主循环必须 `tick()` + `poll()`**：
`poll()` 不会替你驱动 `tick()`。

```php
use RocketMQ\Client\DefaultLitePullConsumer;

$lite = new DefaultLitePullConsumer('GID_LITE');
$lite->setNamesrvAddr('127.0.0.1:9876');
$lite->subscribe('TopicTest');

// topic 队列集合变更监听：比对趟次现查路由，队列真的增减才回调
$lite->setTopicMetadataCheckIntervalMillis(1000);
$lite->registerTopicMessageQueueChangeListener('TopicTest',
    function (string $topic, array $mqs): void {
        fwrite(STDOUT, "$topic -> " . count($mqs) . " queues\n");
    });

$lite->start();
while (true) {
    $lite->tick();                        // 到点的趟次才做队列比对 / 心跳 / 位点
    foreach ($lite->poll(200) as $m) {    // 只读缓冲，不驱动 tick()
        // ... 处理 ...
    }
    usleep(100_000);
}
```

位点默认自动提交（`autoCommit`），也可 `commit()` / `commit($offsets, persist: false)` 手动控制；
`pause()` / `resume()` 按队列暂停与恢复拉取。

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

topic / 订阅组 CRUD、消费统计与进度、按 key / uniqKey / msgId 查消息、位点读取与重置、
name server 配置等能力见 `src/Client/Admin.php`。

### ACL

```php
use RocketMQ\Remoting\AclClientRPCHook;
use RocketMQ\Remoting\SessionCredentials;

$producer = new DefaultMQProducer('PID_DEMO', new AclClientRPCHook(
    new SessionCredentials('yourAccessKey', 'yourSecretKey')
));
```

三种消费者与 Admin 的构造器第一个参数同样接受 `RPCHook`；带 STS token 时传第三个参数。
签名走 `HmacSHA1`，签名内容包含命名空间与请求时间戳字段。

### 命名空间（两套，彼此独立）

- **`namespace`（本地资源前缀）**：构造器第三个参数，如
  `new DefaultMQProducer('PID_DEMO', null, 'MyNamespace')`。topic、group 在收发、心跳、
  位点全链路自动包装为 `MyNamespace%资源名` 并在使用后还原，对 broker 完全透明。
- **`namespaceV2`（服务端命名空间）**：`$client->setNamespaceV2('my-ns')`。它不改资源名，
  而是给每笔请求盖 `nsd=true` / `ns=<值>` 扩展字段（且**每笔请求现读**，不是启动期快照），
  由 broker 侧按命名空间路由。

两者可同时使用、互不干扰。

### 压缩

超过阈值（默认 4,096 字节）的消息体自动压缩，接收端按消息类型位自动解压；解不出来一律
抛异常，绝不把压缩流当正文透传。三种编码都是本 SDK 自带能力：

- **ZLIB**：`gzcompress` / RFC1950（默认编码）。
- **LZ4**：纯 PHP 的 LZ4 Frame 格式实现，无需任何扩展或 CLI。
- **ZSTD**：优先调用 `zstd` CLI 真压缩；缺 CLI 时自动退回内置的纯 PHP Raw/RLE 帧实现。

```php
$producer->setCompressMsgBodyOverHowmuch(4096);
$producer->setCompressType(MessageSysFlag::ZLIB_TYPE);   // LZ4_TYPE / ZSTD_TYPE 可切换
$producer->setCompressLevel(5);
```

### TLS

`tlsEnable` 是进程级开关，打开后所有出连接（包括 NameServer）都走 TLS；可以构造器传，
也可以设公开属性或用环境变量 `ROCKETMQ_TLS_ENABLE=1`：

```php
$producer = new DefaultMQProducer('PID_DEMO', tlsEnable: true);
$producer->tlsOptions = [
    'caCert'     => '/path/ca.crt',      // 给了就真校验服务端证书链 + 主机名
    'clientCert' => '/path/client.crt',  // mTLS：客户端证书
    'clientKey'  => '/path/client.key',
    'serverName' => '127.0.0.1',         // SNI / 主机名校验覆盖
];
```

缺省不带 `caCert` 时信任自签证书；缺省严格校验主机名与证书链。

## 特性与进度

- ✅ 协议 / 传输：`RemotingCommand` JSON + ROCKETMQ 二进制双序列化、17 段存储格式与
  6 段批量消息编解码、sync / async / oneway、opaque 匹配、半包重组、GO_AWAY 换连接重发、TLS
- ✅ 发送：普通 / 定点 / 队列选择器 / 批量（含 `ProduceAccumulator` 自动攒批与两级公平
  背压）/ 单向 / 异步 / 事务（两阶段 + broker 回查）/ 定时消息与撤回 / Request-Reply
- ✅ 消费：Push（并发 / 顺序 / 广播 / 位点持久化 / 启动期数值校验 / 拉取前流控阈值）、
  Pull（全量队列与本实例份额两种视图）、LitePull（含 topic 队列集合变更监听）、
  POP（200050 弹出 / 200051 ACK / 200053 延长不可见时间 + 检查点）、`ConsumeMessageDirectly`
- ✅ 队列分配：`AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` /
  `MACHINE_ROOM_NEARBY`，由重平衡驱动
- ✅ 命名空间：本地前缀 `namespace` 与服务端 `namespaceV2` 两套机制
- ✅ 管理端：topic / 订阅组 CRUD、集群与 broker 运行时信息、消费统计与进度、按
  key / uniqKey / msgId 查消息、位点读取与重置、name server 配置
- ✅ 观测：消息轨迹（生产 / 消费 / 回查 + 异步分发器）、消费统计、`ConsumerRunningInfo`、
  五类钩子、`OpenTracingHook`、W3C `traceparent` 注入与透传
- ✅ 安全与寻址：ACL 签名（HmacSHA1）、动态 NameServer 取址（HTTP 域 + `NsAddr` 文件）、
  故障规避选队列、`Validators` 本地校验前置
- ✅ 压缩：ZLIB（`gzcompress`）、纯 PHP LZ4 Frame、ZSTD（CLI 优先，纯 PHP 兜底）
- ✅ TLS：CA 校验 / mTLS / SNI
- ✅ 离线自测：10 套件 1,674 项断言 + 重复类名静态守卫（实测全绿）
- ✅ 真机联调：`examples/live_*.php` 8 条用例覆盖管理端 / 重投死信 / 拉模式 / 队列变更监听 /
  POP / TLS / Request-Reply / 压缩冒烟

## 客户端日志

`RocketMQ\Client\Logger` 默认**落盘**，文件路径为
`<当前工作目录>/logs/rocketmqlogs/rocketmq_php_client.log`，按大小轮转（默认 64MB × 10 份，
备份名 `<file>.1` … `<file>.N`）。默认随项目工作目录走：脚本 / CI 场景下不污染用户主目录，
部署与容器里路径天然可预期；需要别的目录时显式设 `ROCKETMQ_CLIENT_LOG_DIR`
（例如 `$HOME/logs/rocketmqlogs` 或某个临时目录）即可，这是官方逃生门。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR`（`TRACE` 归入 `DEBUG`） |
| `ROCKETMQ_CLIENT_LOG_DIR` | `<当前工作目录>/logs/rocketmqlogs` | 日志目录 |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_php_client.log` | 文件名；空串 / `OFF` / `NONE` = 关闭文件落盘；含路径分隔符时按整路径处理 |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | 空 | 任意非空值 = 只写 stderr，不写文件 |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | `67108864`（64MB） | 按大小轮转的阈值，`0` = 不轮转 |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | 备份份数 |

程序内可直接改：`Logger::setLevel()` / `Logger::setHandler()`（注入 callable，单测用）/
`Logger::logFilePath()`（诊断与真机脚本「是否真的写出文件」的断言点）。写文件失败只降级
一次、绝不影响客户端主流程。

> 本机环境噪声：宿主机 `php.ini` 的 auto_prepend 会让每个 `php` 进程多打两行
> `[CQ_POLLER] ...`，与客户端无关，排查日志时先排除。

## 真实集群联调

`examples/live_*.php` 是真机验证工具，失败一律非 0 退出码收口，统一打印
`PASS=<n> FAIL=<n>`。推荐通过根目录的编排脚本跑（自动拉起/复用 5.5.1 测试集群，
且**只停自己起的那一次**；已有集群在跑时直接复用）：

```bash
bash scripts/run_php_live.sh <case> 127.0.0.1:9876 [legs]
```

| case | 脚本 | 内容（一行） |
| --- | --- | --- |
| `admin` | `examples/live_admin.php` | 管理端冒烟：集群探活、topic CRUD + 路由、broker 配置/运行时、KV 配置、订阅组 CRUD、发消息 + topic 统计、位点/时间戳查询、KEYS 索引查询，自建资源跑完自删 |
| `redelivery` | `examples/live_redelivery.php` | 重投/死信矩阵 S1–S4：RETRY 回投、maxReconsumeTimes 转 DLQ、顺序毒消息、部分 ack（可选 `legs=all\|s1,s2,s3,s4`） |
| `pull` | `examples/live_pull.php` | 拉模式消费者 S1–S3：队列视图 / `fetchMessageQueuesInBalance` 现算份额 / 手动拉全 / 位点提交回读（可选 `legs=all\|s1,s2,s3`） |
| `lite_qc` | `examples/live_lite_topic_queue_change.php` | LitePull topic 队列集合变更监听：检查周期压到 1s + 真实扩缩容，证明比对趟次现查路由；主循环每轮 `tick()`+`poll()` 驱动 |
| `request_reply` | `examples/live_request_reply.php` | Request-Reply 往返：应答方进程（consumer + `reply()`）与发起方进程（`request()`）两段编排，`ROUNDTRIP ok=<n> total=<n>` 收口 |
| `pop` | `examples/live_pop.php` | POP 消费 S1–S2：ACK 生效计时观察 + 失败退避（changeInvisibleTime→RETRY→重试标记）（可选 `legs=all\|s1,s2`） |
| `tls` | `examples/live_tls.php` | TLS 三腿 `plain_tls` / `ca_verify` / `mtls`，脚本现造 CA/server(SAN:127.0.0.1)/client 证书 |
| `compression` | `examples/live_compression.php` | 本端压缩冒烟：zlib / lz4 / zstd 自压自解一条腿；退出码 0 ok / 1 普通失败 / 2 坏 codec / 3 recv 超时 |

跑真机用例的三条硬规则：

1. **先起消费者、再发消息**：消费者没有默认 topic 兜底，缺路由 = 无分配 = 无消费；
   先发消息后起消费者会把环境问题伪装成客户端 bug。所有 live 脚本都按这个顺序编排。
2. **部分用例需要专用 broker 配置**（脚本已自动处理，但要求**独占集群**）：
   `pop` 需要 POP 专用 `broker.conf` 四件配置（`timerWheelEnable=true`、
   `defaultMessageRequestMode=PULL`、`popResponseReturnActualRetryTopic=false`、
   `enablePopBatchAck=false`）；`tls` 需要 NameServer + Broker 都以 `tls.enable` 启动；
   撤回与轨迹链路还要求 broker 开 `recallMessageEnable=true` / `traceTopicEnable=true`。
   已在跑的明文集群遇到 `pop` / `tls` 时脚本直接以 2 号退出码拒绝，避免误停共享集群。
3. **日志钉在临时目录**：`lite_qc` 用例默认把 `ROCKETMQ_CLIENT_LOG_DIR` 指到系统临时目录
   （已有值则不覆盖），离线套件同样如此——PHP 客户端的真机/自测产物不落进用户主目录。

压缩互通矩阵（与集群脚本解耦）走根目录 `scripts/compression_matrix.sh zlib`
（`lz4` / `zstd` 同脚本换 codec）。

## 目录结构

```
php/
├── bootstrap.php              PSR-4 autoloader + 重复类名静态守卫入口（无需 composer）
├── composer.json              仅声明 ext-* 与 autoload 映射，不引入任何依赖
├── PORTING.md                 工程约定（命名 / 线格式 / tick 执行模型 / 类名唯一性）
├── src/
│   ├── Common/                消息模型、编解码、压缩（CompressionCodec）、常量、校验
│   ├── Remoting/              RemotingClient、RpcHooks、Protocol/（帧、两路序列化、请求响应头与体）
│   └── Client/                Producer / PushConsumer / PullConsumer / Admin /
│                              队列分配策略 / 轨迹 / Latency / Backpressure / Logger / Exceptions
├── examples/                  真机验证工具（live_*，失败一律非 0 退出码收口）
└── tests/                     离线自测（run_all.php 串起各套件 + 类名冲突扫描）
```

## License

Apache-2.0
