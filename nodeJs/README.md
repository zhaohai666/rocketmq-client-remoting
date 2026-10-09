# nodeJs — Node.js / TypeScript 端（第六语言，共七端）

> 中文 ｜ [English](README.en.md)

Apache RocketMQ **经典 remoting 协议**客户端的 Node.js 实现。与 `python/` `go/` 等各端同构：
同一套协议语义、同一套消息模型、零第三方依赖（只用 `node:` 内置模块）。

- **文件形态**：TypeScript（`.ts`），经 `node --experimental-strip-types` 直接运行
  （Node ≥ 22.x），**无需构建步骤**；ESM 显式 `.ts` 扩展名导入。
- **参考源码**：Java 5.x（`zhaohai666-rocketmq`），Python / Go 两份已验证实现做交叉核对。
- **适配集群**：RocketMQ 4.x / 5.x，联调基于 **5.5.1**（NameServer 9876 + Broker 10911）。

## 运行自检（离线，无需集群）

```bash
node --experimental-strip-types selfcheck.ts
```

加载 `src/` 全部模块并跑 5 套冒烟（协议层 / producer / consumer / 统计 / Java 差距补齐面）。
（注：本机沙箱若拦截 `spawnSync` 起子进程，可直接逐个运行 `test/*.ts` 验证。）

## 快速上手

七种语言都是同一套动作：建门面 → 设 NameServer 地址 → `start()` → 收发 → `shutdown()`。

```ts
import { DefaultMQProducer } from './src/client/producer.ts';
import { Message } from './src/common/message.ts';

const producer = new DefaultMQProducer('GID_TEST');
producer.setNamesrvAddr('127.0.0.1:9876');
producer.start();

const result = await producer.send(new Message('TopicTest', Buffer.from('hello'), 'TagA', 'k1'));
console.log(result.msgId, result.messageQueue.getQueueId());

producer.shutdown();
```

Push Consumer（跨端口硬规则：**先起消费者、再发消息**）：

```ts
import { DefaultMQPushConsumer } from './src/client/consumer.ts';
import { ConsumeConcurrentlyStatus } from './src/client/consumer_result.ts';

const consumer = new DefaultMQPushConsumer('GID_TEST_C');
consumer.setNamesrvAddr('127.0.0.1:9876');
consumer.subscribe('TopicTest', '*');
consumer.registerMessageListenerConcurrently((msgs) => {
  for (const m of msgs) console.log(m.getBody()?.toString());
  return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});
await consumer.start();
```

## 能力面（与其余六端对齐）

| 领域 | 状态 | 说明 |
| --- | --- | --- |
| 协议层 | ✅ | `RemotingCommand` 帧、JSON + RocketMQ 二进制双序列化、V2 短键 header、fastjson2 容错解析、17 段/6 段消息编解码 |
| 传输层 | ✅ | 长连接惰性建连复用、同步/异步/oneway、半包重组、opaque 匹配、GO_AWAY 重发一次、断连在途请求判死、TLS opt-in |
| 发送 | ✅ | 同步/批量/单向/队列选择器/异步/事务两阶段+回查/Request-Reply/recallMessage |
| Push Consumer | ✅ | 长轮询、顺序、广播、位点持久化、流控阈值、307/220/221 应答、RETRY 还原 |
| Pull / Lite Pull | ✅ | 短轮询（pull）与阻塞轮询（pullBlockIfNotFound）、Lite Pull 订阅+assign+seek+自动提交；`fetchPublishMessageQueues` / `fetchSubscribeMessageQueues` 是 Java :137 / :142 的两个**不同**视图（写队列 vs 读队列），`fetchMessageQueuesInBalance` 只给本实例应得的那份 |
| 命名空间 / clientId | ✅ | `namespaceV2` 由 `NamespaceRpcHook` 现读现盖章（`nsd=true` / `ns=<值>`，钩子顺序 Namespace → Stream → ACL，故 `ns`/`ReqT` 进 ACL 签名）；clientId 一律 `MixAll.clientIdFor` 的 `<ip>@<instanceName>[@<unitName>][@STREAM]`，CLUSTERING 下 `instanceName` 按 Java `changeInstanceNameToPID` 换成 `<pid>#<nanotime>`，**同进程两个实例不会撞成同一个 clientId**（撞了就会各自算出同一份队列、重复消费）；Pull / Lite Pull 消费者与 Java 构造函数一样默认 `enableStreamRequestType`（clientId 带 `@STREAM`、每笔请求带 `ReqT=0`） |
| 队列分配 | ✅ | AVG / AVG_BY_CIRCLE / CONFIG / MACHINE_ROOM / CONSISTENT_HASH（MD5 环 + 虚拟节点）/ **MACHINE_ROOM_NEARBY**（Java AllocateMachineRoomNearBy：机房内独占 + 无活消费者机房全局均摊；resolver 给出空机房抛错） |
| 管理端 | ✅ | `DefaultMQAdminExt`：topic CRUD、集群/运行时信息、订阅组（**分页 201**）、消费统计、连接查询、位点重置、KV 配置（**广播**）、GET_BROKER_CONFIG（**Properties 文本体**） |
| 消息轨迹 | ✅ | 编解码（`\x01`/`\x02` 分隔、无 keys SubBefore 7 段容错）、异步分发器、收发钩子 |
| 消费统计 | ✅ | 差分窗口 StatsItem（10s/10min 采样链）、307 statusTable（consumeFailedMsgs 取 hour 窗口） |
| 钩子/ACL | ✅ | CheckForbidden（不吞异常）/ FilterMessage（必须吞）、ACL 签名（HMAC-SHA1 + Base64） |
| Request-Reply 接收侧 | ✅ | `PUSH_REPLY_MESSAGE_TO_CLIENT(326)` 处理器：按 Java `processReplyMsg` 从 ReplyMessageRequestHeader 重建 MessageExt、解压 body、按 CORRELATION_ID 原子移除 future 并唤醒（发送侧 `request()` 对齐 Java `prepareSendRequest`：REPLY_TO_CLIENT=clientId + TTL） |
| 异步背压 | ✅ | 两个公平信号量（num/size）接入 `sendAsync`：许可不足同步抛、回调恰好一次归还；`setBackPressureForAsyncSendNum/Size` 运行时可调 |
| W3C traceparent | ✅ | `src/client/traceparent.ts`：注入（调用方传播的上下文优先）/ 提取 / 校验 / 子 span，`ROCKETMQ_TRACE_CONTEXT_ENABLE` 开关，与 Go 端同款 |
| VIP channel | ✅ | Producer 侧 `setSendMessageWithVIPChannel`：发送 RPC 走 broker VIP 端口（port-2） |
| Name server 配置 | ✅ | `updateNameServerConfig`（318 广播，Properties 文本体）/ `getNameServerConfig`（319） |
| 消息查询 | ✅ | `queryMessage`(12) / `queryMessageByUniqKey` / `viewMessage`(33，msgId 内嵌地址直连) / `consumeMessageDirectly`(309 admin 发起) / 边界位点 LOWER/UPPER / `examineConsumerOffset` |
| 5.x 定时消息 | ✅ | `setDelayTimeSec/Ms` / `setDeliverTimeMs`（TIMER_DELAY_SEC / TIMER_DELAY_MS / TIMER_DELIVER_MS，Java Message 同名 setter） |
| 压缩 | ✅ | 三型齐全、零第三方依赖（`src/common/compress.ts`）：LZ4 是手写的 **Frame** 格式（帧头 + xxh32 HC，与 Java `LZ4FrameOutputStream`、Python `lz4.frame` 同 wire，编解码双向完整）；ZSTD 走 `node:zlib` 的 zstd 绑定（Node 自带 stdlib，Node ≥ 23.8），因此在低版本运行时退回自写的 Raw/RLE 帧编码器 + 只认 Raw/RLE 的解码器；解不出的输入**明确抛错**，绝不把压缩流当正文透传 |

## 真实集群联调

```bash
# 单项（内部自动拉起/复用本地 5.5.1 集群，收工只停自己起的）
bash scripts/run_node_live.sh producer   # 发送全链路：sync/batch/oneway/async/selector/事务回查
bash scripts/run_node_live.sh consumer   # push 消费回读：先起消费者再发消息
bash scripts/run_node_live.sh pull       # Pull Consumer：短轮询 + 位点 + sendMessageBack
bash scripts/run_node_live.sh lite_pull  # Lite Pull：poll + seek + commitSync
bash scripts/run_node_live.sh admin      # 管理端：topic/集群/订阅组/连接/统计/Properties 配置
```

topic/组名默认带时间戳，残留状态不会让断言假绿。

## 已知与其他六端的差异

- **POP 消费模式已实现（2026-10-01）**：POP_MESSAGE(200050) / ACK(200051) /
  CHANGE_MESSAGE_INVISIBLETIME(200053) / BATCH_ACK(200151) / SET_MESSAGE_REQUEST_MODE(401)、
  POP_CK 检查点双路径重建（offset 表 / 消息自offset）、按应答欠账流控、超窗批次二次校验、
  重试退避十六档（checkNeedAckOrDelay）、307 的 mqPopTable、setPopMode 时自动下发 401。
  orderly POP 按 Java stub（"POPTODO"）在 checkConfig 拒绝。经典客户端按 Java 保真不路由
  batch ack（仅暴露线上能力）。**POP 的队列来自本端客户端 rebalance**，不走 Java 的
  `clientRebalance=false` broker 侧分配（`RebalanceImpl#getRebalanceResultFromBroker:345` →
  `MQClientAPIImpl#queryAssignment:405`，QUERY_ASSIGNMENT=400）——与其余六端同一刻意决定，
  语义等价、只差在「谁决定队列集合」，见 `src/client/consumer.ts` 的 `doRebalance` 注释。
  **真机用例未跑**——队列/断言见 `test/pop_smoke.ts`。
- **2026-10-01 对 Java 客户端（zhaohai666-rocketmq 5.x）补齐**：MACHINE_ROOM_NEARBY、
  Request-Reply 326 接收侧闭环、异步背压接线、traceparent、VIP channel、
  admin 消息查询/边界位点/307 位点读/name server 配置/309 admin 发起、5.x 定时 setter
  —— 见 `test/java_gap_fill_smoke.ts`。
- **2026-09-30 真机验证基线**：producer 8/8（sync×10 / batch / oneway / async / selector /
  事务 COMMIT / broker 回查）、consumer 2/2（20 条 exactly-once）、pull 3/3、lite_pull 3/3
  （poll / seek / commitSync）、admin 11/11 —— 全部对真实 5.5.1 集群（`scripts/run_node_live.sh`）。
  注意事项：消费类用例必须**等队列分配到位再发消息**（`CONSUME_FROM_LAST_OFFSET` 语义）；事务
  回查窗口 ≥90s（broker 巡检周期 30s）。
- **消息轨迹消费侧解码**已实现，但轨迹真机用例未跑通前不建议依赖。
- Java 5.x 已移除的 SUSPEND/RESUME_CONSUMER(209/210)、ADJUST_CONSUMER_THREAD_POOL(213)
  只有请求码常量、无任何调用方（tools 模块同样未用），nodeJs 端与 Java 基线保持一致、不实现。

## 目录结构

```
nodeJs/
├── src/
│   ├── remoting/        # 协议层：帧/序列化/头/体/路由/订阅/命名空间/ACL/TLS 客户端
│   ├── common/          # 消息模型、常量、校验、工具
│   └── client/          # MQClient、producer、consumer、pull、admin、分配/位点/统计/轨迹
├── examples/            # 真机 live 工具（producer/consumer/pull/lite_pull/admin）
├── test/                # 离线冒烟（协议往返、分配、位点、轨迹、统计差分窗）
└── selfcheck.ts         # 一键离线自检
```
