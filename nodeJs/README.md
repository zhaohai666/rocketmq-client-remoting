# RocketMQ Node.js 客户端

> 中文 ｜ [English](README.en.md)

## 概述

Apache RocketMQ **经典 remoting 协议**的 Node.js/TypeScript 客户端：直连 NameServer 与 Broker 收发消息、管理位点，无需任何代理层。

- TypeScript 源码直接运行：`node --experimental-strip-types`，**无构建步骤**；ESM 导入带显式 `.ts` 扩展名。
- **零第三方依赖**，只用 `node:` 内置模块。
- 要求 Node ≥ 22（`package.json` 的 `engines`；本仓库实测环境为 v24.14.0）。
- 全部真机用例基于 RocketMQ **5.5.1** 集群联调（NameServer 9876 + Broker 10911）。

## 先决条件

- Node.js ≥ 22（`--experimental-strip-types` 直跑 `.ts`）。
- 一个可达的 RocketMQ 集群：NameServer `9876`、Broker `10911`（离线冒烟不需要集群）。
- 无需 `npm install`：没有第三方依赖。

## 安装与开发

```bash
git clone <本仓库> && cd rocketmq-client-remoting/nodeJs
node --experimental-strip-types selfcheck.ts
```

`selfcheck.ts` 加载 `src/` 全部 **59** 个模块并依次跑 **11** 套离线冒烟，2026-10-10 实测 **ALL GREEN**。单套运行：`node --experimental-strip-types test/<套件>.ts`，实测结果：

| 套件 | 实测 |
| --- | --- |
| `test/smoke.ts`（协议帧/传输/ACL 签名） | 6 组检查通过 |
| `test/producer_smoke.ts` | 4 组检查通过 |
| `test/consumer_smoke.ts` | 30 通过 / 0 失败 |
| `test/stats_smoke.ts` | 14 通过 / 0 失败 |
| `test/compat_contract_smoke.ts`（线上格式/行为钉死面） | 10 组检查通过 |
| `test/pop_smoke.ts` | 11 组检查通过 |
| `test/fixes2_smoke.ts` | 38 项检查通过 |
| `test/namespace_rpc_smoke.ts` | 7 组检查通过 |
| `test/ns_failover_smoke.ts` | 12 项检查通过 |
| `test/lite_topic_queue_change_smoke.ts` | 25 通过 / 0 失败 |
| `test/pull_balance_view_smoke.ts` | 14 通过 / 0 失败 |

真机示例直接对集群运行：

```bash
node --experimental-strip-types examples/live_producer.ts --ns 127.0.0.1:9876
```

示例逐行打印 `PASS/FAIL`，全部通过时退出码为 0（详见「真实集群联调」）。

## 快速上手

所有客户端生命周期一致：构造 → `setNamesrvAddr` → `start()` → 收发 → `shutdown()`。

### 普通消息 / Producer

```ts
import { DefaultMQProducer, SelectMessageQueueByHash } from './src/client/producer.ts';
import { Message, MessageBatch } from './src/common/message.ts';

const producer = new DefaultMQProducer('GID_TEST');
producer.setNamesrvAddr('127.0.0.1:9876');
await producer.start();

// 同步发送
const r = await producer.send(new Message('TopicTest', Buffer.from('hello'), 'TagA', 'key1'));
console.log(r.msgId, r.messageQueue.getQueueId());

// 批量（合并为一笔 RPC）
await producer.send(MessageBatch.generateFromList([
  new Message('TopicTest', Buffer.from('b1')), new Message('TopicTest', Buffer.from('b2')),
]));

// 单向 / 异步
await producer.sendOneway(new Message('TopicTest', Buffer.from('fire')));
await producer.sendAsync(new Message('TopicTest', Buffer.from('hi')),
  (result, err) => console.log(result?.msgId, err?.message));

// 顺序：按业务键哈希固定队列
await producer.sendBySelector(new Message('TopicTest', Buffer.from('o1')),
  new SelectMessageQueueByHash(), 'orderNo-1');

// 延迟 / 定时
const d = new Message('TopicTest', Buffer.from('later'));
d.setDelayTimeLevel(3);            // 4.x 档位
d.setDelayTimeSec(30);             // 5.x timer：TIMER_DELAY_SEC
d.setDelayTimeMs(1500);            // TIMER_DELAY_MS
d.setDeliverTimeMs(1699999999000); // TIMER_DELIVER_MS

producer.shutdown();
```

### 事务消息 / Producer

```ts
import { TransactionMQProducer, TransactionListener, LocalTransactionState } from './src/client/producer.ts';
import { Message } from './src/common/message.ts';

class Listener extends TransactionListener {
  executeLocalTransaction(msg: any, arg: any) { return LocalTransactionState.COMMIT_MESSAGE; }
  checkLocalTransaction(msg: any) { return LocalTransactionState.COMMIT_MESSAGE; } // broker 回查
}
const tx = new TransactionMQProducer('GID_TX', new Listener());
tx.setNamesrvAddr('127.0.0.1:9876');
await tx.start();
const half = new Message('TopicTest', Buffer.from('tx'));
await tx.sendMessageInTransaction(half, null); // 半消息 → 本地事务 → COMMIT/ROLLBACK
tx.shutdown();
```

### Request-Reply / 消息回溯 / Producer

```ts
const answer = await producer.request(new Message('TopicTest', Buffer.from('ping'))); // 应答 Message
// 应答侧在消费回调里：await producer.reply(requestMsg, Buffer.from('pong'));
await producer.recallMessage(recallHandle); // 撤回延迟/定时消息，handle 来自发送结果
```

### 并发消费 / PushConsumer

```ts
import { DefaultMQPushConsumer } from './src/client/consumer.ts';
import { ConsumeConcurrentlyStatus } from './src/client/consumer_result.ts';

const consumer = new DefaultMQPushConsumer('GID_TEST');
consumer.setNamesrvAddr('127.0.0.1:9876');
consumer.subscribe('TopicTest', '*');
consumer.registerMessageListenerConcurrently((msgs: any[]) => {
  for (const m of msgs) console.log(m.getBody()?.toString());
  return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 或 RECONSUME_LATER 重投
});
await consumer.start();
```

### 顺序消费 / PushConsumer

```ts
import { ConsumeOrderlyStatus } from './src/client/consumer_result.ts';
consumer.registerMessageListenerOrderly((msgs: any[]) =>
  ConsumeOrderlyStatus.SUCCESS); // 或 SUSPEND_CURRENT_QUEUE_A_MOMENT
```

### 广播消费 / PushConsumer

```ts
import { MessageModel } from './src/remoting/heartbeat.ts';
consumer.setMessageModel(MessageModel.BROADCASTING); // 默认 CLUSTERING，位点各机器自管
```

### PullConsumer

```ts
import { DefaultMQPullConsumer } from './src/client/pull_consumer.ts';
import { PullStatus } from './src/client/consumer_result.ts';

const puller = new DefaultMQPullConsumer('GID_PULL');
puller.setNamesrvAddr('127.0.0.1:9876');
await puller.start();
for (const mq of await puller.fetchMessageQueuesInBalance('TopicTest')) {
  const offset = await puller.fetchConsumeOffset(mq, false);
  const res = await puller.pull(mq, '*', offset, 32);        // 短轮询
  // await puller.pullBlockIfNotFound(mq, '*', offset, 32)   // 长轮询
  if (res.pullStatus === PullStatus.FOUND) puller.updateConsumeOffset(mq, res.nextBeginOffset);
}
await puller.persistConsumeOffset();
puller.shutdown();
```

### LitePullConsumer（含 topic 队列变更监听）

```ts
import { DefaultLitePullConsumer } from './src/client/lite_pull_consumer.ts';

const lite = new DefaultLitePullConsumer('GID_LITE');
lite.setNamesrvAddr('127.0.0.1:9876');
lite.subscribe('TopicTest', '*');   // 自动均衡；手动指定用 lite.assign([mq])
await lite.start();

const msgs = await lite.poll(3000);
lite.seek(mq, 0);
await lite.seekToBegin(mq);         // 先取 broker minOffset 再定位
await lite.seekToEnd(mq);           // 先取 maxOffset 再定位
await lite.commitSync();

// 队列变更监听：默认每 30s 重查一次路由，setTopicMetadataCheckIntervalMillis(ms)
// 可调（下限 1s）。监听按**裸 topic 名**登记；namespaceV2 不改本地 topic 名，
// 由请求头 nsd/ns 上线携带——这是本端设计，二者互不干扰。
lite.setTopicMetadataCheckIntervalMillis(1000);
await lite.registerTopicMessageQueueChangeListener('TopicTest', {
  onChanged(topic, messageQueues) { console.log(topic, messageQueues.map(q => q.getQueueId())); },
});
lite.shutdown();
```

### Admin

```ts
import { DefaultMQAdminExt } from './src/client/admin.ts';

const admin = new DefaultMQAdminExt();
admin.setNamesrvAddr('127.0.0.1:9876');
admin.start();
await admin.fetchAllTopicList();
await admin.createTopic('default', 'NewTopic', 8);
await admin.examineBrokerClusterInfo();
await admin.examineConsumeStats('GID_TEST');
await admin.resetOffsetByTimestamp('TopicTest', 'GID_TEST', Date.now(), true);
await admin.queryMessage('127.0.0.1:10911', 'TopicTest', 'key1');     // 按 key 查（12）
await admin.viewMessage('TopicTest', msgId);                         // 按 msgId 直连查看（33）
await admin.examineConsumerOffset('GID_TEST', mq);
admin.shutdown();
```

### ACL

```ts
import { AclRPCHook } from './src/remoting/acl.ts';

producer.rpcHook = new AclRPCHook(accessKey, secretKey); // HMAC-SHA1 + Base64 逐请求签名
```

### 命名空间（两套机制）

```ts
producer.setNamespace('MyNS');        // 客户端前缀：资源名变为 %%MyNS%%<资源>
producer.setNamespaceV2('RMQ_INST');  // 服务端命名空间：资源名不变，每笔请求现读现
                                      // 盖 nsd=true / ns=RMQ_INST（钩子顺序 Namespace→Stream→ACL，
                                      // ns 字段进 ACL 签名）
```

`setNamespaceV2` 对所有客户端（Producer / Push / Pull / LitePull / Admin）生效，逐请求读取当前值。

### 压缩（zlib / LZ4 / ZSTD）

```ts
producer.setCompressType('ZSTD');            // 'ZLIB' | 'LZ4' | 'ZSTD'
producer.setCompressMsgBodyOverHowmuch(8192); // 超过阈值才压，默认 4096
await producer.send(bigMsg);                  // 接收侧自动解压
```

- LZ4 走线上标准的 **Frame** 格式（帧头 + xxh32 校验），编解码双向完整。
- ZSTD 优先使用 `node:zlib` 自带的 zstd 绑定（Node ≥ 23.8 提供，本端特性检测）；低版本运行时自动退回内置编码器：发送侧产出仅含 Raw/RLE 块的合法 zstd 帧，接收侧只认 Raw/RLE 帧，解不出的输入**明确抛错**，绝不把压缩流当正文透传。
- `MessageBatch` 不参与压缩（批体是 broker 要拆分的聚合信封）。

### TLS

```ts
producer.setTlsEnable(true);  // 也可用环境变量 ROCKETMQ_TLS_ENABLE=1
producer.setTlsOptions({ caCert, clientCert, clientKey, serverName }); // null = 信任自签的 test-mode
```

Producer / PushConsumer / LitePullConsumer 均支持；`tlsEnable` 是进程级开关，NameServer 连接同样走 TLS。

## 特性与进度

- ✅ 协议层：`RemotingCommand` 帧、JSON + RocketMQ 二进制双序列化、V2 短键 header、容错解析、消息 17/6 段编解码
- ✅ 传输层：长连接惰性建连复用、同步/异步/单向、半包重组、opaque 匹配、GO_AWAY 重发一次、断连在途请求判死
- ✅ Producer：同步 / 批量 / 单向 / 异步（公平信号量背压）/ 队列选择器 / 顺序 / 延迟与 5.x 定时 / 事务两阶段 + 回查 / Request-Reply（326 接收闭环）/ recallMessage / VIP channel（port-2）
- ✅ PushConsumer：并发 / 顺序 / 广播、长轮询、位点持久化、流控阈值、307/220/221 应答、RETRY 还原、suspend/resume、核心线程数在线调整
- ✅ PullConsumer / LitePullConsumer：短/长轮询、assign+seek+commitSync、自动提交、topic 队列变更监听
- ✅ POP 消费模式：POP(200050)/ACK(200051)/CHANGE_INVISIBLE(200053)/BATCH_ACK(200151)/SET_MESSAGE_REQUEST_MODE(401)、POP_CK 双路径、欠账流控、退避表
- ✅ Admin：topic CRUD、集群/运行时信息、订阅组、消费统计、连接查询、位点重置与读取、KV 配置、NameServer 配置（318/319）、消息查询/查看、consumeMessageDirectly
- ✅ ACL（HMAC-SHA1 + Base64）与 RPC 钩子链（Namespace → Stream → 用户钩子）
- ✅ 命名空间两套：`namespace` 前缀 + `namespaceV2` 线上盖章；clientId `<ip>@<instanceName>[@<unitName>][@STREAM]`
- ✅ 队列分配 6 策略：AVG / AVG_BY_CIRCLE / CONFIG / MACHINE_ROOM / MACHINE_ROOM_NEARBY / CONSISTENT_HASH
- ✅ 消息轨迹（编解码 + 异步分发 + 收发钩子）、消费统计（差分窗口 + 307 statusTable）、W3C traceparent
- ✅ 压缩三型：zlib / LZ4（Frame）/ ZSTD；TLS opt-in
- ✅ NameServer 故障转移（客户端侧地址切换与路由重取）

## 客户端日志

只用环境变量配置，无构建步骤：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | `TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR` |
| `ROCKETMQ_CLIENT_LOG_DIR` | `$HOME/logs/rocketmqlogs` | 日志目录 |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_node_client.log` | 文件名；含分隔符按全路径处理；置 `''`/`OFF`/`NONE` 关闭文件 |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | （空） | 任意非空值 → 只走 stdout，不写文件 |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE` | 64MB | 滚动阈值 |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | 10 | 备份个数，`.1`..`.10` 依次顶替 |

本端口径：每行日志始终打 stdout/stderr；默认**同时**落文件，`ROCKETMQ_CLIENT_LOG_USE_STDOUT` 或 `ROCKETMQ_CLIENT_LOG_FILE=OFF` 关闭文件落盘。滚动按大小触发，文件名 `rocketmq_node_client.log` 带端口标识，多端并跑互不串写。

## 真实集群联调

对已在跑的集群直接跑单个示例（地址取 `--ns`，缺省 `NAMESRV_ADDR` 或 `127.0.0.1:9876`）：

```bash
node --experimental-strip-types examples/live_lite_queue_change.ts --ns 127.0.0.1:9876
# 2026-10-10 实测：PASS=10 FAIL=0，退出码 0
```

也可用总入口（未就绪时自动拉起本地 5.5.1 集群，收工只停自己起的）：

```bash
bash scripts/run_node_live.sh <目标> [namesrv]
# 目标：producer|consumer|pull|lite_pull|admin|pop|request_reply|admin_ns|acl|tls|check_config|fixes2|slave
```

退出码约定：示例全部检查通过 → 0；任一 FAIL → 非 0；包装脚本原样转发，参数/环境错误 → 2。

示例清单：

| 文件 | 一句话 |
| --- | --- |
| `examples/live_producer.ts` | 发送全链路：sync/batch/oneway/async/selector/事务 COMMIT + broker 回查 |
| `examples/live_consumer.ts` | push 消费回读：先起消费者再发消息，逐条 exactly-once 校验 |
| `examples/live_pull.ts` | PullConsumer 短轮询 + 位点簿记 + sendMessageBack + 持久化 |
| `examples/live_lite_pull.ts` | LitePull subscribe+poll、seek 回退、commitSync |
| `examples/live_lite_queue_change.ts` | 队列变更监听：扩容 2→4 / 缩容 4→2 / 未知 topic 报错 / 轮询间隔下限 |
| `examples/live_pop.ts` | POP 消费端到端：pop → ack → 不可见时间续期 |
| `examples/live_request_reply.ts` | 326 Request-Reply：requester 发问、responder 回执、应答闭环 |
| `examples/live_admin.ts` | 管理端：topic CRUD/集群/运行时/订阅组/连接/位点/Properties 配置 |
| `examples/live_admin_ns.ts` | 管理端专属回环：318/319 NameServer 配置、309 consumeMessageDirectly 等 |
| `examples/live_acl.ts` | ACL：开启鉴权的 broker 上，正确 AK/SK 通过、错 SK 被拒 |
| `examples/live_tls.ts` | TLS 三腿：plain_tls / ca_verify / mtls（需 TLS 专用集群） |
| `examples/live_tls_negative.ts` | 负向探针：严格 CA 连错 CA，路由错误必须透传而非吞掉 |
| `examples/live_slave_only.ts` | 只剩 slave broker 时 push 消费者仍能消费 |
| `examples/live_check_client_config.ts` | CHECK_CLIENT_CONFIG(46) 订阅预检 |
| `examples/live_fixes2.ts` | 五项实报修复的端到端复验（事务半消息提交后立即可见等） |
| `examples/live_compression.ts` | 压缩往返矩阵腿：zlib/LZ4/ZSTD 发送侧 + 接收侧 match 判定（`scripts/compression_matrix.sh` 调起） |

两条顺序铁律：**先起消费者、等队列分配到位再发消息**（`CONSUME_FROM_LAST_OFFSET` 语义，否则首段消息落在分配完成前被跳过）；事务回查用例窗口 ≥90s（broker 巡检周期 30s）。topic/组名默认带时间戳，残留状态不会让断言假绿。

## 目录结构

```
nodeJs/
├── src/
│   ├── remoting/        # 协议层：帧/序列化/头/体/路由/订阅/命名空间/ACL/TLS 客户端
│   ├── common/          # 消息模型、常量、压缩、校验、工具
│   └── client/          # MQClient、producer、push/pull/lite-pull/pop 消费者、admin、
│                        # 队列分配/位点存储/消费统计/轨迹/背压/traceparent
├── examples/            # 真机 live 工具（见「真实集群联调」）
├── test/                # 离线冒烟（协议往返、分配、位点、轨迹、统计、命名空间、POP）
├── selfcheck.ts         # 一键离线自检（模块加载 + 11 套冒烟）
└── package.json         # engines: node >= 22
```

## License

Apache-2.0
