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

`selfcheck.ts` 加载 `src/` 全部 **66** 个模块并依次跑 **52** 套离线冒烟（两份清单都由目录树推导，新增文件不会漏出自检），2026-10-30 实测 **ALL GREEN**，其中 47 套逐条计出 **1717** 项检查，另外 5 套按检查组报告。单套运行：`node --experimental-strip-types test/<套件>.ts`，实测结果：

| 套件 | 实测 |
| --- | --- |
| `test/admin_concurrent_smoke.ts`（并发 admin 调用） | 27 通过 / 0 失败 |
| `test/admin_lite_list_smoke.ts`（lite / concurrent 列表 admin） | 22 通过 / 0 失败 |
| `test/admin_longtail_smoke.ts`（控制器 / 容器 / 冷数据 / rocksdb 长尾） | 46 通过 / 0 失败 |
| `test/admin_query_smoke.ts` | 7 通过 / 0 失败 |
| `test/admin_track_smoke.ts`（消息轨迹回查） | 18 通过 / 0 失败 |
| `test/auth_admin_smoke.ts` | 71 通过 / 0 失败 |
| `test/auto_batch_and_retry_smoke.ts`（自动攒批与同步重试） | 112 通过 / 0 失败 |
| `test/backpressure_smoke.ts`（异步公平信号量闸门：等待、拒绝、回滚、文案） | 119 通过 / 0 失败 |
| `test/batch_schedule_guard_smoke.ts`（批量发送拒绝携带延迟/定时属性的成员） | 23 通过 / 0 失败 |
| `test/body_crc_smoke.ts`（存储体 BODYCRC 掩码形式） | 18 通过 / 0 失败 |
| `test/client_config_gaps_smoke.ts`（客户端配置旋钮） | 44 通过 / 0 失败 |
| `test/compat_contract_smoke.ts`（线上格式/行为钉死面） | 10 组检查通过 |
| `test/consumer_heartbeat_registration_smoke.ts`（消费端心跳注册） | 14 通过 / 0 失败 |
| `test/consumer_smoke.ts` | 34 通过 / 0 失败 |
| `test/consumer_status_reply_smoke.ts`（221 状态应答 + 307/309 未托管组 + 220 单向） | 22 通过 / 0 失败 |
| `test/fixes2_smoke.ts` | 40 项检查通过 |
| `test/flow_control_smoke.ts`（每队列三条闸门：只判本队列「持有」的一切——未派发缓冲 + 已交给监听但尚未结算的批次；严格大于才停，阈值按字面取值（不 clamp 到 1、0 不是「关闭」，合法区间由启动配置检查把守），字节闸门按消息体整 MiB 向下取整而不是 storeSize，跨度闸门不作用于顺序消费；命中原因按 条数 → 字节 → 跨度 的固定顺序、一次判定只记一个计数，告警第 1 次及其后每 1000 次；缓存闸门 50ms、broker 215 的 20ms、其余拉取失败走唯一的可配旋钮 `pullRetryDelayMillis`（默认 1000ms，`get/setPullRetryDelayMillis`，拉取与 POP 两条路径共用、每条队列循环建立时取值）；POP 队列的待 ack 欠账闸门默认 96、严格大于才停，命中记进拉取路径同一个流控计数与告警节奏，POP 被 broker 回 215 同样只等 20ms；lite 路径 `poll()` 就地拉取、无预取缓冲，所以只按队列停在 215 的 20ms、其余错误码不 park、兄弟队列不受影响，且 broker 拒绝码以带 `responseCode` 的 `MQBrokerException` 交给调用方） | 148 项检查通过 |
| `test/heartbeat_v2_beat_smoke.ts` | 28 通过 / 0 失败 |
| `test/heartbeat_v2_smoke.ts` | 30 通过 / 0 失败 |
| `test/initial_offset_policy_smoke.ts`（初始位点：广播 / TIMESTAMP / %RETRY% / SQL92） | 44 通过 / 0 失败 |
| `test/lite_surface_guards_smoke.ts`（lite 表面的误用闸门：空白 topic、空队列列表的 assign、已经在跑之后再设指派表达式、没启动就 poll / committed / 取队列，负超时排在状态检查之后；subscribe 与 assign 互斥且先到者定形，assign 模式本身不把指派队列写进心跳（要发必须走显式 built set）；assign 收窄与 unsubscribe 都把被撤销队列的光标、park、暂停位与缓冲一起丢掉；seek 依次判「是否持有」（按模式区分文案）→ 是否运行 → [minOffset, maxOffset] 区间，committed 的 broker 读失败报错而不是回 -1） | 48 通过 / 0 失败 |
| `test/lite_topic_queue_change_smoke.ts` | 25 通过 / 0 失败 |
| `test/lite_wire_contract_smoke.ts`（lite 线上契约：短轮询的 LITE_PULL 把 `suspendTimeoutMillis` 填成配置的 broker 挂起预算而 sysFlag 不置挂起位；`buildSubscriptionsForHeartbeat` 整组替换、空表清空、拒绝项让旧集合原样保留、rebalance 订阅优先只补空缺，topic 键只包一层命名空间，默认 assign 心跳不带订阅项） | 13 通过 / 0 失败 |
| `test/mq_client_manager_smoke.ts` | 52 通过 / 0 失败 |
| `test/namespace_rpc_smoke.ts` | 7 组检查通过 |
| `test/ns_failover_smoke.ts` | 12 项检查通过 |
| `test/offset_persist_timer_smoke.ts` | 27 通过 / 0 失败 |
| `test/parity_gaps_smoke.ts`（能力缺口的显式回归） | 45 通过 / 0 失败 |
| `test/pop_batch_ack_smoke.ts`（200151 请求形状） | 9 通过 / 0 失败 |
| `test/pop_init_mode_smoke.ts`（起始策略到 POP initMode 的翻译） | 14 项检查通过 |
| `test/pop_pre_deliver_filter_smoke.ts`（POP 轮次的交付前过滤；第 8 节把同一问题问给 pull 路径） | 27 项检查通过 |
| `test/pop_retry_topic_reset_smoke.ts`（POP 重试主题名还原） | 17 项检查通过 |
| `test/pop_round_reschedule_smoke.ts`（POP 轮次的重排、退避与记账） | 33 项检查通过 |
| `test/pop_smoke.ts` | 11 组检查通过 |
| `test/producer_smoke.ts` | 4 组检查通过 |
| `test/pull_balance_view_smoke.ts` | 14 通过 / 0 失败 |
| `test/pull_offset_sanity_smoke.ts`（FOUND 轮位点自检：nextBeginOffset 或交付首行低于本轮请求位点时告警；自洽轮、空 FOUND 轮与空闲状态都不告警；告警轮与自洽轮的存储位点、交付行数、节奏判定完全一致） | 23 项检查通过 |
| `test/pull_round_pacing_smoke.ts`（轮次节奏：只有交出消息的轮才等 pullInterval；空 FOUND、被过滤空的轮、NO_NEW_MSG、NO_MATCHED_MSG 立刻重拉，OFFSET_ILLEGAL 不取节奏等待） | 25 项检查通过 |
| `test/pull_retry_delay_knob_smoke.ts`（拉取/POP 通用错误退避作为一个旋钮：默认 1000ms、broker 215 仍是 20ms、改旋钮同时移动两条路径的等待、非 215 报错轮与 `POLLING_FULL` 轮都取旋钮值；每条队列的循环建立时取值，启动后再改不动已在跑的循环） | 18 项检查通过 |
| `test/pull_schedule_smoke.ts` | 43 通过 / 0 失败 |
| `test/pull_stats_contract_smoke.ts`（pull 路径 PULL_RT/PULL_TPS 记账：只有 FOUND 轮计入延迟——NO_NEW_MSG / NO_MATCHED_MSG / OFFSET_ILLEGAL / 报错 / 超时轮什么都不记；吞吐只按交付前过滤（二次 tag + FilterMessageHook）改写后的行数计，全部被滤掉的 FOUND 轮记 RT 而 TPS 为零） | 28 项检查通过 |
| `test/rebalance_change_smoke.ts`（分配集合改变的轮次按序补齐：只给变动的 topic 重打订阅版本 → 再把 topic 预算除以本轮自有队列数 → 立刻心跳 → 只向变动的 topic 触发队列监听；集合未变与自愈重建轮什么都不欠） | 32 项检查通过 |
| `test/send_back_fallback_smoke.ts`（送回兜底：被拒的 CONSUMER_SEND_MSG_BACK 降级为经内部生产者发往 `%RETRY%<group>` 的普通发送，retry 副本带 RETRY_TOPIC / RECONSUME_TIME / MAX_RECONSUME_TIMES / ORIGIN_MESSAGE_ID / DELAY，丢掉半消息标记且不动源消息；push 的被拒应答 / 传输错误 / 缺 master 各降级为一次重试发送，-1 在线上报 16、RPC 仍是 5000ms；兜底也失败时报最初错误；顺序消费走同一个副本；pull 的拒绝应答按 3000ms RPC、`maxReconsumeTimes` 旋钮改请求携带值；pull 停机拆掉惰性兜底生产者） | 103 通过 / 0 失败 |
| `test/send_timeout_contract_smoke.ts`（发送超时：每次调用一条预算，逐次尝试扣掉已花时间；预算耗尽抛 `sendDefaultImpl call timeout`、选择器耗时超预算抛 `sendSelectImpl call timeout`，两者都不再发出请求；`sendMsgMaxTimeoutPerRequest` 只收紧还剩重试的那次尝试、末次不受限，默认 -1 不设上限；显式超时一路传到传输层） | 12 通过 / 0 失败 |
| `test/smoke.ts`（协议帧/传输/ACL 签名） | 6 组检查通过 |
| `test/socks_smoke.ts`（SOCKS 代理握手 + 经代理建连） | 51 通过 / 0 失败 |
| `test/static_topic_smoke.ts` | 29 通过 / 0 失败 |
| `test/stats_smoke.ts` | 14 通过 / 0 失败 |
| `test/topic_threshold_smoke.ts`（topic 预算在重平衡改变分配时除以自有队列数、把商写成每队列闸门；闸门不累计兄弟队列） | 33 项检查通过 |
| `test/transaction_schedule_guard_smoke.ts`（事务半消息拒绝携带延迟/定时属性的消息） | 17 通过 / 0 失败 |
| `test/transport_contract_smoke.ts`（连接生命周期：解不开帧的连接打一条点名对端的 ERROR 并按对端 EOF 收掉，对端下一个合法帧在新连接上服务；没人等的响应被丢弃并打一条点名 opaque 与地址的 WARNING，连接留着；可写问答的是发送缓冲而不是有没有连接；空闲清扫只收超过上限的那条，配非正数恢复默认；同步等待超时默认关掉它发出去的连接，`closeSocketOnSyncTimeout: false` 才留，且每个过期在途请求各一条日志） | 50 项检查通过 |
| `test/vip_channel_smoke.ts`（VIP channel port-2 改写） | 36 通过 / 0 失败 |

### 打包 / npm 消费（tarball 与 registry）

仓库内开发**零依赖零构建**（`--experimental-strip-types` 直跑）；对外消费走编译产物：

```bash
npm install          # 安装 devDependency（typescript），并触发 prepare -> npm run build 生成 dist/
npm pack             # 产出 rocketmq-client-remoting-nodejs-5.5.1.tgz（只含 dist/ + README）
npm publish          # 或发布到 registry（main/types/exports 均指向 dist/index.js|d.ts）
```

- 消费端 `npm install <tgz>` 后**普通 import 即可用**（无需任何 node flag）：
  `import { DefaultMQProducer, Message } from 'rocketmq-client-remoting-nodejs'`。
- 构建用 `tsc -p tsconfig.build.json --noCheck`（转译 + `rewriteRelativeImportExtensions`
  把 `.ts` 导入改写为 `.js`），`fix-dts-imports.mjs` 补齐 `.d.ts` 里 tsc 不重写的同类引用。
- 运行时**零第三方依赖**不变：`typescript` 仅是构建期 devDependency。
- 构建为转译模式（`--noCheck`）：源码树尚有存量类型债（`tsc --noEmit` 全量报 205 处），
  打包门禁不因其阻塞；类型体检是独立课题，不随本包交付。

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
  const stored = await puller.fetchConsumeOffset(mq, true);    // 回读已提交位点，从未消费过时为 -1
  const offset = stored >= 0 ? stored : 0;
  const res = await puller.pull(mq, '*', offset, 32);        // 短轮询
  // await puller.pullBlockIfNotFound(mq, '*', offset, 32)   // 长轮询
  if (res.status === PullStatus.FOUND) puller.updateConsumeOffset(mq, res.nextBeginOffset);
}
await puller.persistConsumeOffset();
await puller.shutdown();
```

### LitePullConsumer（含 topic 队列变更监听）

这一表面的误用一律在本地先报错：空白 topic 的 `subscribe`、空队列列表的 `assign`（那等于撤销全部持有队列）、没启动就 `poll` / `committed`。`poll` 的负超时同样被拒绝，但运行状态检查排在它前面——空的一轮和「还没有位点」都和队列停在尾位点长得一模一样，缺了 `start()` 只有这样才不会一直被藏下去。`subscribe` / `assign` 互斥，先调用者定形；`seek` 依次判「队列是否持有」→「是否已启动」→ 位点落在 `[minOffset, maxOffset]` 内；`assign` 收窄与 `unsubscribe` 立刻撤销不再持有队列的光标、seek 定位、暂停位、park 与本地缓冲。

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

### SOCKS 代理

```ts
// SOCKS 代理配置：SocksProxyConfig（addr/username/password）
producer.setSocksProxyConfig({ addr: '127.0.0.1:1080' });                          // SOCKS5 无认证
producer.setSocksProxyConfig({ addr: '127.0.0.1:1080', username: 'u', password: 'p' }); // SOCKS5 RFC 1929 子协商
producer.setSocksProxyConfig({ addr: '127.0.0.1:1080', version: 4 });              // SOCKS4/4a
```

- TCP 建连后、TLS 握手前完成 CONNECT（SOCKS handler 在 SSL handler 之前的管线序）；`version` 缺省 5，可 4 / `'SOCKS4'`（域名目标自动走 4a 的 0.0.0.1 哨兵）。
- 作用于全部 remoting 连接（NameServer + Broker，即 `0.0.0.0/0` 全量口径）；**TopAddressing 的 HTTP 寻址不走代理**。
- Producer / PushConsumer 均支持；也可用环境变量 `ROCKETMQ_SOCKS_PROXY_CONFIG` 传 JSON（`{"0.0.0.0/0": {...}}` 路由表形状或裸对象）。

## 特性与进度

- ✅ 协议层：`RemotingCommand` 帧、JSON + RocketMQ 二进制双序列化、V2 短键 header、容错解析、消息 17/6 段编解码
- ✅ 传输层：长连接惰性建连复用、同步/异步/单向、半包重组、opaque 匹配、GO_AWAY 重发一次、断连在途请求判死
- ✅ 连接生命周期：解不开帧的连接打一条点名对端的 ERROR 并被拆掉（长度前缀越界是同一件事更早一层的表现，只在抓到它的那一层报一次，不会再补一条解码失败）；没人等的响应打一条点名 opaque 和地址的 WARN，连接照旧留着；每个过期的在途请求各有一条自己的 WARN；`isChannelWritable()` 回答的是 socket 发送缓冲，不是有没有连接——没有活连接的地址照样可写（下一次发送自己建连），只有发送缓冲塞满的活连接才报 false；一个 unref() 过的清扫定时器（不会把测试进程吊活）关掉既没读也没写、静默严格超过 `maxIdleTimeMillis`（默认 120000；配非正数恢复默认，而不是把清扫关掉）的连接，每个地址一条 WARN；同步等待超时后，`closeSocketOnSyncTimeout` 开着（默认 true）就销毁它发出去的那条 socket——只有截止时剩下的预算既不超过 100ms 又不超过该超时的四分之一才留；写本身失败则无论旋钮开合连接一律拆掉
- ✅ Producer：同步 / 批量 / 单向 / 异步（公平信号量背压）/ 队列选择器 / 顺序 / 延迟与 5.x 定时 / 事务两阶段 + 回查 / Request-Reply（326 接收闭环）/ recallMessage / VIP channel（port-2）
- ✅ PushConsumer：并发 / 顺序 / 广播、长轮询、位点持久化、流控阈值（每队列只判它「持有」的一切：未派发缓冲 + 已交给监听但尚未结算的批次；严格大于才停，阈值不 clamp、0 不是「关闭」，字节按消息体整 MiB 取整而不是 storeSize，跨度闸门不作用于顺序消费；topic 预算在重平衡改变分配时除以自有队列数，不在拉取时累计兄弟队列）、分配集合变化的重平衡轮按序补齐（只给变动的 topic 重打订阅版本 → 除预算 → 立刻心跳 → 只向变动的 topic 触发队列监听）、307/220/221 应答、RETRY 还原、suspend/resume、核心线程数在线调整、失败轮重问的通用退避是一个可配旋钮 `pullRetryDelayMillis`（默认 1000ms，`get/setPullRetryDelayMillis`，队列循环建立时取值，启动后再改不影响已在跑的循环；broker 215 的 20ms、本队列缓存闸门 50ms、暂停 1000ms 三条另有定值，不随这个旋钮动）；轮间节奏由 `pullInterval`（默认 0）管，且只管「真正交出消息」的那一轮——FOUND 且经客户端 tag 复查与 FilterMessageHook 改写之后仍然非空的批次等 `pullInterval`（为 0 就立刻重拉），空手而归的一轮（FOUND 回空、被过滤清空、NO_NEW_MSG、NO_MATCHED_MSG）什么都不等：这条节奏管的是交付出的那一次，本地再压一段只会推迟下一轮取消息
- ✅ PullConsumer / LitePullConsumer：短/长轮询、assign+seek+commitSync、自动提交、topic 队列变更监听；LitePull 的 `poll()` 就地拉取、没有本地预取缓冲也没有后台拉取任务，因此每队列缓存闸门（条数 / 消息体整 MiB / 位点跨度 / 整个缓冲条数）无对象可判——该路径的退避按队列停在 broker 的 `FLOW_CONTROL`(215)：`litePullParkMillisFor` 把 215 映射成 20ms（`PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL`）、其余错误码不 park，`setPullParkUntil` / `getPullParkUntil` 记录并读回每个队列的 park 截止时刻，兄弟队列不受影响；broker 的拒绝码以 `MQBrokerException.responseCode` 交给调用方
- ✅ POP 消费模式：POP(200050)/ACK(200051)/CHANGE_INVISIBLE(200053)/BATCH_ACK(200151)/SET_MESSAGE_REQUEST_MODE(401)、POP_CK 双路径、欠账流控（本队列待 ack 严格大于 `popThresholdForQueue`，默认 96，命中记进拉取路径同一个流控计数）、退避：broker 215 只等 20ms、本队列闸门 50ms、暂停 1000ms，其余失败轮（含 `POLLING_FULL` 与不认识的状态）等 `pullRetryDelayMillis`（与拉取路径同一个旋钮，默认 1000ms）
- ✅ Admin：topic CRUD、集群/运行时信息、订阅组、消费统计、连接查询、位点重置与读取、KV 配置、NameServer 配置（318/319）、消息查询/查看、consumeMessageDirectly
- ✅ ACL（HMAC-SHA1 + Base64）与 RPC 钩子链（Namespace → Stream → 用户钩子）
- ✅ 命名空间两套：`namespace` 前缀 + `namespaceV2` 线上盖章；clientId `<ip>@<instanceName>[@<unitName>][@STREAM]`
- ✅ 队列分配 6 策略：AVG / AVG_BY_CIRCLE / CONFIG / MACHINE_ROOM / MACHINE_ROOM_NEARBY / CONSISTENT_HASH
- ✅ 消息轨迹（编解码 + 异步分发 + 收发钩子）、消费统计（差分窗口 + 307 statusTable）、W3C traceparent
- ✅ 压缩三型：zlib / LZ4（Frame）/ ZSTD；TLS opt-in；SOCKS4/5 代理（对齐 ClientConfig#socksProxyConfig）
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

本端口径：每行日志始终打 stdout/stderr；默认**同时**落文件，`ROCKETMQ_CLIENT_LOG_USE_STDOUT` 或 `ROCKETMQ_CLIENT_LOG_FILE=OFF` 关闭文件落盘。滚动按大小触发，文件名 `rocketmq_node_client.log` 带端口标识，避免并发运行时串写。

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
| `examples/live_producer.ts` | 发送全链路：sync/batch/oneway/async/selector/事务 COMMIT + broker 回查；`--ak/--sk` 时全程签名发送 |
| `examples/live_consumer.ts` | push 消费回读：先起消费者再发消息，逐条 exactly-once 校验；`--ak/--sk` 时消费侧同样挂签名钩子 |
| `examples/live_pull.ts` | PullConsumer 短轮询 + 位点簿记 + sendMessageBack + 持久化 |
| `examples/live_lite_pull.ts` | LitePull subscribe+poll、seek 回退、commitSync |
| `examples/live_lite_queue_change.ts` | 队列变更监听：扩容 2→4 / 缩容 4→2 / 未知 topic 报错 / 轮询间隔下限 |
| `examples/live_pop.ts` | POP 消费端到端：pop → ack → 不可见时间续期 |
| `examples/live_pop_batch_ack.ts` | `BATCH_ACK_MESSAGE`(200151) 真机腿：整批 ack、漏掉一条后它从 retry 队列复活、两轮 pop 的合并键不塌缩 |
| `examples/live_request_reply.ts` | 326 Request-Reply：requester 发问、responder 回执、应答闭环 |
| `examples/live_admin.ts` | 管理端：topic CRUD/集群/运行时/订阅组/连接/位点/Properties 配置 |
| `examples/live_admin_ns.ts` | 管理端专属回环：318/319 NameServer 配置、309 consumeMessageDirectly 等 |
| `examples/live_acl.ts` | ACL：开启鉴权的 broker 上，正确 AK/SK 通过、错 SK 被拒 |
| `examples/live_acl_message_types.ts` | 鉴权集群上的全消息类型收发：无凭据被拒(16) 反向断言 + 同步/异步/单向/批量/延迟等级/定时/事务/顺序/Tag 过滤/用户属性/KEYS 服务端查询逐条读回；`--ak/--sk` 必填 |
| `examples/live_tls.ts` | TLS 三腿：plain_tls / ca_verify / mtls（需 TLS 专用集群） |
| `examples/live_tls_negative.ts` | 负向探针：严格 CA 连错 CA，路由错误必须透传而非吞掉 |
| `examples/live_slave_only.ts` | 只剩 slave broker 时 push 消费者仍能消费 |
| `examples/live_check_client_config.ts` | CHECK_CLIENT_CONFIG(46) 订阅预检 |
| `examples/live_fixes2.ts` | 五项实报修复的端到端复验（事务半消息提交后立即可见等） |
| `examples/live_auto_batch.ts` | autoBatch 分组发送回读：逐成员核对上报 queueOffset/offsetMsgId 与队列实际值、延迟与超限成员各走各路、单成员可按 UNIQ_KEY 查回；`autoBatch=false` 作对照组 |
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
├── test/                # 离线冒烟（协议往返、攒批重试、背压闸门、分配、位点、轨迹、统计、命名空间、POP、admin）
├── selfcheck.ts         # 一键离线自检（模块加载 + 52 套冒烟）
└── package.json         # engines: node >= 22
```

## License

Apache-2.0
