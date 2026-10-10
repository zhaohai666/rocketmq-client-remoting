# RocketMQ Python 客户端

> 中文 ｜ [English](README.en.md)

## 概述

RocketMQ 经典 remoting 协议的 Python 客户端：直连 NameServer(9876) 与 Broker(10911)，不经 proxy、不用 gRPC。
对外是同步 API，心跳、重平衡、拉取循环、位点落盘都由内部后台线程完成。
只用标准库（socket / threading / zlib / json），**没有必选的第三方运行时依赖**。
面向 4.x / 5.x 服务端，全部能力在真实 5.5.1 集群上做过真机联调（见「真实集群联调」）。

## 先决条件

- Python 3.8 及以上（`pyproject.toml` 中 `requires-python = ">=3.8"`）。
- 一套已经启动的服务端：NameServer 监听 `9876`，Broker 监听 `10911`。
- 本地快速验证建议 broker 开 `autoCreateTopicEnable=true`（首条消息自动建 topic）；
  正式环境请关掉，先用 `DefaultMQAdminExt` 建好 topic 与订阅组。

## 安装与构建

```bash
cd python
python3 -m venv .venv
.venv/bin/pip install -e .                     # 顶层包：client / common / remoting

.venv/bin/pip install pytest                   # 只有跑单元测试才需要它
.venv/bin/python -m pytest tests/ -q           # 1210 passed, 4 skipped
.venv/bin/python selfcheck.py                  # selfcheck: 7/7 passed
```

- `pytest` 收集 1214 个用例，全部离线跑，不需要集群。其中 4 个 skip 全部来自
  `tests/test_protocol_alignment.py`（可选的协议常量比对守卫，默认不启用）。
- `selfcheck.py` 是 7 项协议编解码回环自检（JSON 头 / 二进制头 / V2 短字段头 /
  17 段存储格式 / 长 topic V2 magic / 6 段批量格式 / ACL 签名注入），无需集群；
  `python -m selfcheck` 与 `python selfcheck.py` 等价。

## 快速上手

### 生产者

普通消息：

```python
from client.producer import DefaultMQProducer
from common.message import Message

producer = DefaultMQProducer("PID_DEMO")
producer.set_namesrv_addr("127.0.0.1:9876")
producer.start()

result = producer.send(Message("TopicTest", b"hello", tags="TagA", keys="order-1"))
print(result.send_status, result.msg_id, result.queue_offset)
producer.shutdown()
```

顺序消息（同一 `arg` 落同一队列）：

```python
from client.producer import SelectMessageQueueByHash

result = producer.send_by_selector(
    Message("TopicTest", b"part-1"), SelectMessageQueueByHash(), "order-1", 3000)
```

定点发送（自己指定 `MessageQueue`）：`producer.send(msg, mq=MessageQueue("TopicTest", "broker-a", 0))`。

延迟 / 定时消息与撤回：

```python
msg = Message("TopicTest", b"deliver-later")
msg.set_delay_time_level(3)                      # 延迟等级 3
result = producer.send(msg)
handle = result.recall_handle                    # 只有延迟/定时消息才带撤回句柄

uniq_key = producer.recall_message("TopicTest", handle)   # 请求码 370，返回被撤回消息的 uniqKey
```

事务消息（两阶段 + broker 回查）：

```python
from client.producer import TransactionMQProducer, TransactionListener, LocalTransactionState

class TxListener(TransactionListener):
    def execute_local_transaction(self, msg, arg):
        return LocalTransactionState.COMMIT_MESSAGE
    def check_local_transaction(self, msg):      # broker 回查半消息
        return LocalTransactionState.COMMIT_MESSAGE

tx = TransactionMQProducer("PID_TX")
tx.set_namesrv_addr("127.0.0.1:9876")
tx.set_transaction_listener(TxListener())
tx.start()
result = tx.send_message_in_transaction(Message("TopicTest", b"half"))
print(result.send_status, result.get_local_transaction_state())
tx.shutdown()
```

批量消息（同 topic、非延迟，单条上限 4MB）：

```python
from common.message import MessageBatch

batch = MessageBatch.generate_from_list(
    [Message("TopicTest", ("b-%d" % i).encode("utf-8")) for i in range(3)])
result = producer.send(batch)                    # 也可直接传列表：producer.send([...])
```

单向与异步：

```python
from client.producer import SendCallback

producer.send_oneway(Message("TopicTest", b"no-result"))

class CB(SendCallback):
    def on_success(self, send_result):
        print(send_result.msg_id)
    def on_exception(self, e):
        print("send failed:", e)

producer.send_async(Message("TopicTest", b"async"), CB(), 5000)   # 调用方立即返回
```

Request-Reply（请求方同步等应答，325/326 链路）：

```python
reply = producer.request(Message("TopicTest", b"ping"), 3000)
print(bytes(reply.body))

# 异步形态：producer.request_async(msg, RequestCallback(), 3000)
```

应答方在自己的 push 监听器里把结果发回去：

```python
from client.request_reply import create_reply_message

reply_msg = create_reply_message(request_msg, b"pong")
producer.send(reply_msg)                         # 走 SEND_REPLY_MESSAGE_V2(325)
```

### Push Consumer

并发监听：

```python
from client.consumer import DefaultMQPushConsumer, MessageListenerConcurrently
from client.consumer_result import ConsumeConcurrentlyStatus

class Listener(MessageListenerConcurrently):
    def consume_message(self, msgs, context):
        for msg in msgs:
            print(msg.msg_id, msg.topic, bytes(msg.body))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

consumer = DefaultMQPushConsumer("GID_DEMO")
consumer.set_namesrv_addr("127.0.0.1:9876")
consumer.subscribe("TopicTest", "TagA || TagB")
consumer.set_message_listener(Listener())
consumer.start()
# ... 消费在后台线程进行
consumer.shutdown()
```

顺序监听（同一队列串行，返回 `SUSPEND_CURRENT_QUEUE_A_MOMENT` 即挂起重投）：

```python
from client.consumer import MessageListenerOrderly
from client.consumer_result import ConsumeOrderlyStatus

class OrderlyListener(MessageListenerOrderly):
    def consume_message(self, msgs, context):
        return ConsumeOrderlyStatus.SUCCESS
```

广播消费（位点只存本地，不吃 rebalance）：

```python
from remoting.protocol.heartbeat import MessageModel, ConsumeFromWhere

consumer.set_message_model(MessageModel.BROADCASTING)
consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
```

其他常用入口：

```python
from client.consumer import (AllocateMessageQueueConsistentHash, MessageSelector)
from common.subscription_data import ExpressionType

consumer.subscribe_with_selector(
    "TopicTest", MessageSelector(ExpressionType.SQL92, "price > 100"))
consumer.set_allocate_message_queue_strategy(AllocateMessageQueueConsistentHash())
consumer.set_enable_trace(True)          # 消息轨迹
consumer.pop_mode = True                 # 推模式消费者改走 POP 循环
```

### Pull Consumer

自己控游标、自己提交位点：

```python
import time
from client.consumer import DefaultMQPullConsumer
from client.consumer_result import PullStatus

consumer = DefaultMQPullConsumer("GID_PULL")
consumer.set_namesrv_addr("127.0.0.1:9876")
consumer.start()

for mq in consumer.fetch_subscribe_message_queues("TopicTest"):
    offset = consumer.fetch_consume_offset(mq) or 0
    result = consumer.pull(mq, "TagA", offset, 32, 5000)   # 最后一个是超时毫秒
    if result.status == PullStatus.FOUND:
        for msg in result.msg_found_list:
            print(msg.msg_id, bytes(msg.body))
        consumer.update_consume_offset(mq, result.next_begin_offset)

# 长轮询（挂起等到有消息为止）
result = consumer.pull_block_if_not_found(mq, "TagA", offset, 32)

print(consumer.search_offset(mq, int(time.time() * 1000)))
consumer.shutdown()
```

`fetch_subscribe_message_queues(topic)` 给整个 topic 的队列，`fetch_message_queues_in_balance(topic)`
只给本实例按 rebalance 应得的那一份。

### Lite Pull Consumer

```python
from client.consumer import DefaultLitePullConsumer, TopicMessageQueueChangeListener

lite = DefaultLitePullConsumer("GID_LITE")
lite.set_namesrv_addr("127.0.0.1:9876")
lite.subscribe("TopicTest", "TagA")
lite.set_auto_commit(False)
lite.start()

msgs = lite.poll(timeout=1000)
lite.commit()

# 手动指定队列与游标
queues = lite.fetch_message_queues("TopicTest")
lite.assign([queues[0]])
lite.seek(queues[0], 0)                          # seek_to_begin / seek_to_end 亦可
lite.poll(timeout=1000)
lite.pause([queues[0]])
lite.resume([queues[0]])
lite.shutdown()
```

topic 队列集合变更监听（扩/缩容后回调，由后台周期比对触发）：

```python
class QueueChangeListener(TopicMessageQueueChangeListener):
    def on_changed(self, topic, message_queues):
        print("queues changed:", topic, len(message_queues))

lite.register_topic_message_queue_change_listener("TopicTest", QueueChangeListener())
```

### Admin

```python
import time
from client.admin import DefaultMQAdminExt

admin = DefaultMQAdminExt()
admin.set_namesrv_addr("127.0.0.1:9876")
admin.start()

admin.create_topic("DefaultCluster", "TopicTest", 4)
print(admin.fetch_broker_cluster_info())
print(admin.examine_topic_route("TopicTest"))
print(admin.examine_topic_stats("TopicTest"))
print(admin.examine_consume_stats_group("GID_DEMO"))
print(admin.query_consume_time_span("TopicTest", "GID_DEMO"))
admin.reset_offset_by_timestamp("TopicTest", "GID_DEMO", int(time.time() * 1000) - 3600_000)
print(admin.view_message("TopicTest", "<msgId>"))
admin.shutdown()
```

消费者侧运维也在 admin 上：`examine_consumer_connection_info(group)`、
`examine_consumer_running_info(group, client_id)`（请求码 307）、`get_consumer_running_info` 同义入口。

### ACL

```python
from remoting.rpchook import AclClientRPCHook, SessionCredentials
from client.producer import DefaultMQProducer

producer = DefaultMQProducer(
    "PID_DEMO",
    rpc_hook=AclClientRPCHook(SessionCredentials("AccessKey", "SecretKey")))
```

带 STS token 时给第三个参数 `SessionCredentials(ak, sk, security_token)`。
签名内容 = 按 key 字典序拼接的全部 extFields **值**（含刚写入的 `AccessKey` / `SecurityToken`，
`Signature` 自身除外）+ 请求 body；算法 HmacSHA1 + 标准 Base64。

### 命名空间

两套彼此独立，可同时使用：

```python
producer = DefaultMQProducer("PID_DEMO", namespace="ns1")   # 本地资源名前缀 %%ns%%res
producer.set_namespace_v2("ns2")                            # 服务端命名空间：每笔请求带 nsd=true / ns=ns2
```

`namespace` 是五个门面（生产者 / 三种消费者 / admin）构造函数里的同名参数，收发、心跳、位点全线
包装与还原；`set_namespace_v2` 五个门面都有，且每笔请求现读，不是启动期快照。
`DefaultLitePullConsumer` 另有 `set_namespace` 可后置修改。`ns` 与 `ReqT` 都进 ACL 签名内容，
钩子顺序固定为 Namespace → Stream → ACL。

### 消息压缩

```python
from common.sysflag import MessageSysFlag

producer.set_compress_msg_body_over_howmuch(1024)          # body ≥ 1KB 自动压缩（默认 4096）
producer.set_compress_level(5)
producer.set_compress_type(MessageSysFlag.ZSTD_TYPE)       # ZLIB_TYPE / LZ4_TYPE / ZSTD_TYPE
```

zlib 走标准库，无需安装；LZ4 需要 `pip install lz4`，ZSTD 需要 `pip install zstandard`，
缺包时**抛错而不是把压缩字节当正文透传**。消费端检测到 `COMPRESSED_FLAG` 自动解压并清掉该标志位。
批量消息永不压缩。

### TLS

```python
producer = DefaultMQProducer("PID_DEMO", tls_enable=True,
                             tls_options={"caCert": "/path/ca.pem"})
```

`tls_options` 另接受 `clientCert` / `clientKey`（mTLS）与 `serverName`（SNI/主机名覆盖）。
不显式传 `tls_enable` 时读环境变量 `ROCKETMQ_TLS_ENABLE=1`。生产者、三种消费者与 admin 同一条链路都支持。

## 特性与进度

| 能力 | 状态 |
| --- | --- |
| 普通 / 顺序（selector）/ 定点 / 延迟定时 / 事务两阶段 / 批量 / 单向 / 异步（含公平信号量背压） | ✅ |
| Request-Reply（325 发送 + 326 推回，同步与异步两种形态） | ✅ |
| 定时消息撤回 `recall_message`（370，句柄来自 `SendResult.recall_handle`） | ✅ |
| Push Consumer：并发 / 顺序监听、广播、流控五阈值、位点修正与重置、停摆自愈、挂起消息清扫 | ✅ |
| Pull Consumer：手动拉取 + 长轮询 + 手动提交位点 + 按时间戳查位点 | ✅ |
| Lite Pull Consumer：`poll` / `assign` / `seek` / `pause` / `commit`、三张位点表、topic 队列变更监听 | ✅ |
| POP 消费循环（`pop_mode`，含 ack / 续期 / POP 顺序） | ✅ |
| 队列分配策略：AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY | ✅ |
| 命名空间两套：本地前缀 `namespace` 与服务端 `namespace_v2` | ✅ |
| ACL 签名（含 SecurityToken）、`AclClientRPCHook` / `NamespaceRpcHook` / `StreamTypeRPCHook` | ✅ |
| 消息轨迹（Pub / SubBefore / SubAfter / EndTransaction / Recall 编解码 + 异步分发） | ✅ |
| Send / Consume / EndTransaction / CheckForbidden / FilterMessage 钩子 | ✅ |
| 客户端统计（拉取 TPS/RT、消费 RT、消费成功/失败 TPS）与 `ConsumerRunningInfo`（307） | ✅ |
| 压缩三后端：zlib（标准库）/ LZ4 / ZSTD | ✅ |
| TLS（含 mTLS、严格 CA 校验） | ✅ |
| 动态 NameServer 取址（`DefaultTopAddressing` + `ROCKETMQ_NAMESRV_DOMAIN`） | ✅ |
| 发送延迟故障规避（`set_send_latency_fault_enable`）与主从切换 | ✅ |
| 管理端 `DefaultMQAdminExt`：topic / 订阅组配置、位点查询与重置、连接与运行信息、消息查询、broker 配置与清理 | ✅ |
| JSON 与 RocketMQ 二进制两种序列化，17 段存储格式 / 6 段批量格式 | ✅ |

## 客户端日志

`rocketmq_logging.py` 把内部日志桥接到标准 `logging`，文件默认落在
**`<当前工作目录>/logs/rocketmqlogs/rocketmq_py_client.log`**，按天滚动，
备份名 `rocketmq_py_client.log.YYYY-MM-DD`。宿主程序若已经配置过 Python logging
（root logger 已有 handler），则本模块不添加任何 handler。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ROCKETMQ_CLIENT_LOG_DIR` | `<cwd>/logs/rocketmqlogs` | 日志目录，改这里即可指向别处（如 `$HOME/logs/rocketmqlogs`） |
| `ROCKETMQ_CLIENT_LOG_FILE` | `rocketmq_py_client.log` | 日志文件名 |
| `ROCKETMQ_CLIENT_LOG_LEVEL` | `INFO` | 级别，`WARN`/`WARNING`、`TRACE`/`DEBUG` 两种写法都认，不认识时退回 `INFO` |
| `ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX` | `10` | 保留的按天备份份数 |
| `ROCKETMQ_CLIENT_LOG_USE_STDOUT` | `true` | 置 `false` 关掉 stderr 输出 |

目录不可写时只打一条告警并降级到 stderr，不会阻断客户端启动。
回归守卫见 `tests/test_logging_config.py`。

## 真实集群联调

单元测试全是离线 mock；下列脚本打真实集群，用来证明报文真的被 broker/nameServer 接受。

前置：NameServer `9876` + Broker `10911`，`autoCreateTopicEnable=true`。

```bash
.venv/bin/python verify_pull_live.py 127.0.0.1:9876
```

地址是可选的第一个参数（缺省 `127.0.0.1:9876`）。每个脚本逐条打印 `[PASS]/[FAIL]`，
**全部通过退 0，任一失败退 1**，可直接进 CI。

> ⚠ 顺序规则：**先 `start()` 消费者，再发消息。** 新订阅组默认从队列尾部开始，
> 且首轮 rebalance 要等心跳注册完成；先发后起会稳定少掉第一批。

`verify_*_live.py` 共 41 个，前 3 行是不带 `_live` 后缀的补充脚本；每个脚本各证明一件事：

| 脚本 | 证明 |
| --- | --- |
| `verify_message_types.py` | 7 类消息能力端到端：同步 / 异步 / 单向 / 批量 / 事务 / 定点 / request-reply（打集群，非 `_live` 命名） |
| `verify_live_clean.py` | 全新 topic + 全新 group + 全新 store 的干净收发对账，含批量发送计数 |
| `verify_acl_signature_vectors.py` | ACL 签名内容与签名值的固定向量比对（离线，无需集群） |
| `verify_ack_index_live.py` | 并发消费的 `ackIndex` 部分 ack 与整批失败重投 |
| `verify_acl_live.py` | 开了认证的 broker 上：正确凭据通过、缺失/错误凭据被拒 |
| `verify_admin_live.py` | 管理端全链路 + `sendMessageBack` 重投 + 位点重置 |
| `verify_admin_batch_live.py` | 批量 / 静态 topic / 读禁配 / 半消息检查 / 顺序配置等 admin 方法的真实往返 |
| `verify_async_send_live.py` | 异步发送内核：线程口径、并发、定点、钩子、批量、关停 |
| `verify_backpressure_live.py` | 异步发送背压的两个公平信号量 |
| `verify_clean_expired_msg_live.py` | listener 挂起时的过期消息清扫逃生口（约 4 分钟，会删自建 topic） |
| `verify_compression_live.py` | 自动压缩的自产自销 + broker 侧确实是压缩体 + Message 复用（子命令 `selftest|send|recv`） |
| `verify_consumer_heartbeat_slave_live.py` | 消费者心跳扇出到从节点（需集群里有一台从节点） |
| `verify_correct_tags_offset_live.py` | 空应答也把已提交位点推走（correctTagsOffset） |
| `verify_fail_fast_live.py` | broker 真死时在途请求立刻有终态（会停一次 broker 再拉起） |
| `verify_flow_control_live.py` | 拉取前流控的五个阈值 + 启动期数值闸门 |
| `verify_hook_live.py` | CheckForbidden / FilterMessage 钩子的真实拦截 |
| `verify_interval_live.py` | 路由刷新周期与位点落盘周期按配置生效（两条不同周期对照） |
| `verify_latency_live.py` | 发送延迟故障规避：隔离窗口、换 broker、恢复 |
| `verify_lite_pull_code_live.py` | lite 请求码 361 与 broker 的 lite 开关（运行时翻转并在退出前还原） |
| `verify_lite_pull_cursor_live.py` | lite 拉取游标跟随 `nextBeginOffset`，含 NO_MATCHED_MSG 与越界自愈 |
| `verify_lite_pull_live.py` | lite pull 全链路：rebalance、assign+seek、分配策略、三张位点表 |
| `verify_lite_topic_queue_change_live.py` | topic 队列扩/缩容后 `on_changed` 被触发 |
| `verify_offset_illegal_live.py` | `OFFSET_ILLEGAL` 纠错分支：丢队列 + 修正位点立刻落盘 |
| `verify_orderly_reconsume_live.py` | 顺序消费的重投闸门与显式 COMMIT/ROLLBACK |
| `verify_pinned_guard_live.py` | 定点发送的 topic 一致性守卫：真路由不误拒、拒在本端、单向无守卫 |
| `verify_pop_live.py` | POP 协议面：POP / ACK / 续期不可见 |
| `verify_pop_consumer_live.py` | POP 消费循环（推模式消费者改走 POP），含 307 拉取统计 |
| `verify_producer_unregister_live.py` | 生产者退出真的发出 `UNREGISTER_CLIENT`(35) |
| `verify_publish_route_master_live.py` | 发布路由跳过没有 master 的 broker（会停一次 master） |
| `verify_pull_consumer_heartbeat_live.py` | 拉模式消费者的 203/38/35（会删掉自建 topic） |
| `verify_pull_expired_live.py` | 拉取循环停摆自愈（120s 阈值） |
| `verify_pull_live.py` | `DefaultMQPullConsumer` 的拉取、位点提交与按时间戳查位点 |
| `verify_recall_live.py` | 定时消息撤回 `recallMessage`(370)：句柄来自 broker，撤回后不再投递 |
| `verify_redelivery_live.py` | 重投 / 死信终态 / 部分 ack / 停摆自愈 / 顺序死信 |
| `verify_request_reply_live.py` | request-reply 全链路：325 应答 + 超时与造应答失败的错误码 |
| `verify_reset_offset_live.py` | 220 重置消费位点：两种 body 形状 + 在途作废 |
| `verify_send_header_live.py` | 发送头 `c` / `d` / `n` 三字段上线形状 |
| `verify_sql92_live.py` | SQL92 过滤 + `CHECK_CLIENT_CONFIG`(46)（需 broker `enablePropertyFilter=true`） |
| `verify_subscribe_live.py` | 后置订阅 + 立即推一轮心跳 |
| `verify_tls_live.py` | 整条客户端链路跑 TLS（`--leg plain|ca_verify|mtls`） |
| `verify_trace_live.py` | 消息轨迹全链路（需 broker `traceTopicEnable=true`） |
| `verify_transaction_live.py` | 事务两阶段 + broker 回查 |
| `verify_unit_config_live.py` | `unitName` / `unitMode` / stream 在 broker 侧的可观测后果 |
| `verify_validators_live.py` | 非法名字本地快速失败，合法名字照常在集群收发 |

## 目录结构

`client/`、`common/`、`remoting/` 直接位于 `python/` 下，是三个**顶层包**：
把 `python/` 放进 `sys.path` 即可 `from client.producer import DefaultMQProducer`
（`tests/conftest.py` 已经做了这件事）。

```
python/
├── common/                 消息模型与常量
│   ├── message.py              Message / MessageExt / MessageBatch / MessageQueue
│   ├── message_decoder.py      17 段存储格式 + 6 段批量格式编解码，压缩三后端
│   ├── message_const.py        MessageConst 属性键
│   ├── sysflag.py              MessageSysFlag / PullSysFlag / PermName / 压缩类型位
│   ├── boundary_type.py        时间戳查位点的边界语义（LOWER / UPPER）
│   ├── recall_message_handle.py 定时消息撤回句柄 v1（base64url + 5 段）
│   ├── subscription_data.py    SubscriptionData / FilterAPI / ExpressionType
│   ├── topic_config.py / mix_all.py / util_all.py / message_accessor.py
│   ├── message_type.py / message_client_id_setter.py / topic_validator.py
├── remoting/               传输层（纯 socket + 线程）
│   ├── client.py               RemotingClient：同步 / 异步 / oneway、半包重组、TLS
│   ├── rpchook.py              RPCHook / AclClientRPCHook / NamespaceRpcHook / StreamTypeRPCHook
│   ├── exception.py
│   └── protocol/
│       ├── remoting_command.py   帧编解码（totalLen | headerLen+type | header | body）
│       ├── serialize.py          JSON / RocketMQ 二进制 + fastjson 容错解析器
│       ├── codes.py              RequestCode / ResponseCode / LanguageCode / SerializeType
│       ├── headers.py            CommandCustomHeader 家族（含 V2 短字段名 a..n）
│       ├── body.py / admin_body.py / route.py / heartbeat.py / subscription.py
│       ├── extra_info.py / namespace_util.py
├── client/                 面向用户的 API
│   ├── producer.py             DefaultMQProducer / TransactionMQProducer / 选择器 / 事务监听
│   ├── consumer.py             Push / Pull / LitePull 三类消费者 + 六种分配策略
│   ├── admin.py                DefaultMQAdminExt
│   ├── mq_client.py            MQClientInstance：路由、心跳、rebalance、周期任务
│   ├── request_reply.py        Request-Reply 的 future 池与 create_reply_message
│   ├── produce_accumulator.py  自动攒批
│   ├── backpressure.py / consume_executor.py / latency.py / consumer_stats.py / metrics.py
│   ├── hook.py / trace.py / trace_hook.py / trace_dispatcher.py / trace_context.py
│   ├── top_addressing.py       动态 NameServer 取址
│   ├── validators.py / send_result.py / consumer_result.py / exception.py
├── rocketmq_logging.py     日志桥接
├── selfcheck.py            无集群的协议编解码回环自检（7 项）
├── __main__.py             命令行入口（python -m selfcheck）
├── tests/                  1214 个离线用例（conftest.py 负责把 python/ 加进 sys.path）
├── verify_*_live.py        真机联调脚本（见「真实集群联调」）
├── integration_live_test.py / ns_failover_repro.py / go_*_check.py  联调辅助脚本
├── pyproject.toml          零运行时依赖；requires-python >= 3.8
└── README.md / README.en.md
```

## License

Apache-2.0。
