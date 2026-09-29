# rocketmq-client-remoting (Python)

RocketMQ 经典 remoting 协议的 Python 实现，Python 进程可以直接用
**JSON / RocketMQ 二进制** 两种序列化方式与 NameServer、Broker 通信。
兼容 4.x / 5.x 服务端，全部能力在真实 5.5.1 集群上联调验证过；
与本仓库的 C++ / .NET / Rust 实现（`../cpp`、`../dotnet`、`../rust`）逐项对齐。

## 安装与测试

```bash
pip install -e .
pytest -q                     # 1156 passed + 4 skipped（skip 为可选压缩依赖相关）
python -m rocketmq selfcheck  # 协议编解码回环自检（7 项，无需集群）
```

## 快速上手

发送：

```python
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message

producer = DefaultMQProducer("PID_DEMO")
producer.set_namesrv_addr("127.0.0.1:9876")
producer.start()
result = producer.send(Message("TopicTest", "hello".encode("utf-8")))
print(result.status, result.msg_id, result.queue_offset)
producer.shutdown()
```

消费（完整可运行片段见仓库根目录 `README.md`）：

```python
from rocketmq.client.consumer import DefaultMQPushConsumer, MessageListenerConcurrently
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus

consumer = DefaultMQPushConsumer("GID_DEMO")
consumer.set_namesrv_addr("127.0.0.1:9876")
consumer.subscribe("TopicTest")
consumer.set_message_listener(MessageListenerConcurrently())
consumer.start()
# 收到消息时回调 DemoListener.consume_message(msgs, context)，返回
# ConsumeConcurrentlyStatus.CONSUME_SUCCESS 或 RECONSUME_LATER
...
consumer.shutdown()
```

## 功能一览

| 层 | 内容 |
| --- | --- |
| 协议层 | JSON / RocketMQ 二进制两路序列化；`RemotingCommand` 帧编解码；CommandCustomHeader 家族（含 V2 短字段名 a..n）；17 段消息存储格式与 6 段批量格式 |
| 传输层 | `RemotingClient`：同步 / 异步 / oneway、半包重组、opaque 匹配、重连、连接判死、TLS |
| 路由 / 心跳 | `TopicRouteData` / `QueueData` / `BrokerData`、`SubscriptionData`、`HeartbeatData`、动态 name server |
| 生产者 | 同步 / 定点 / 选择器 / 异步（含批量异步与背压信号量）/ 单向 / 批量 / 事务两阶段 / `recallMessage`(370) |
| 消费者 | `DefaultMQPushConsumer`（并发 / 顺序监听、流控五阈值、位点修正与重置、停摆自愈、挂起消息清扫）、`DefaultMQPullConsumer`、`DefaultLitePullConsumer`（三张位点表）、POP 消费循环 |
| 队列分配 | AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY |
| 管理端 | `DefaultMQAdminExt` 全套：topic/订阅组配置、位点查询与重置、消费组连接查询、消息查询 |
| 观测与安全 | 消息轨迹（Pub/SubBefore/SubAfter/EndTransaction/Recall 编解码 + 异步分发）、Send/Consume/EndTransaction 钩子、ACL 签名 |
| 压缩 | zlib（标准库）/ LZ4 / ZSTD；类型位 0/3 = ZLIB；未支持类型必须抛错（不透传压缩字节） |

## 真实集群联调

需要先起 nameServer(9876) + broker(10911)，且 `autoCreateTopicEnable=true`
（本仓库 `scripts/` 下有集群脚本；跨语言压缩矩阵见 `scripts/compression_matrix.sh`）。

```bash
python verify_message_types.py            # 7 类消息能力（同步/异步/单向/批量/事务/定点/request-reply）
python verify_live_clean.py               # 收发 + 批量发送对账 + 清 store 端到端
python verify_request_reply_live.py       # request-reply 全链路（325 应答 + 超时/造应答失败错误码 10006/10007）
python verify_async_send_live.py          # 异步发送内核（线程口径/并发/定点/钩子/批量/关停）
python verify_backpressure_live.py        # 异步发送背压（两个公平信号量）
python verify_admin_live.py               # 管理端全链路 + sendMessageBack 重投 + 位点重置
python verify_validators_live.py          # 名字校验 + 寻址故障定性（10004）
python verify_unit_config_live.py         # unitName/unitMode/stream（clientId 后缀、ReqT）
python verify_recall_live.py              # 定时消息撤回 recallMessage(370)（自动开关 recallMessageEnable 并还原）
python verify_trace_live.py               # 消息轨迹全链路（需 broker traceTopicEnable=true）
python verify_hook_live.py                # CheckForbidden / FilterMessage 钩子
python verify_compression_live.py selftest  # 自动压缩自产自销 + broker 侧压缩体校验 + Message 复用
python verify_compression_live.py send|recv <topic> <group> <size>  # 跨客户端压缩互通
python verify_send_header_live.py         # 发送头 c/d/n 三字段
python verify_pinned_guard_live.py        # 定点发送 topic 守卫（真路由不误拒/拒在本端/单向无守卫）
python verify_tls_live.py                 # 整条客户端链路跑 TLS
python verify_acl_live.py                 # 需开 ACL 的集群
python verify_acl_java_parity.py          # ACL 签名与官方签名向量对拍（离线，无需集群）
python verify_transaction_live.py         # 事务两阶段
python verify_pull_live.py                # DefaultMQPullConsumer 拉取
python verify_pull_consumer_heartbeat_live.py  # 拉模式消费者的 203/38/35（会删掉自建 topic）
python verify_consumer_heartbeat_slave_live.py # 消费者心跳扇出到从节点（需集群里有一台从节点）
python verify_producer_unregister_live.py # 生产者退出注销 UNREGISTER_CLIENT(35)
python verify_subscribe_live.py           # 后置订阅 + 立即心跳
python verify_interval_live.py            # 定时任务周期（initialDelay/固定速率）
python verify_fail_fast_live.py           # broker 真死时在途请求立刻判死（会停一次 broker 再拉起）
python verify_lite_pull_live.py           # lite pull 全链路（rebalance/assign+seek/策略/三张位点表）
python verify_lite_pull_cursor_live.py    # lite 拉取游标跟随 nextBeginOffset（NO_MATCHED_MSG/越界自愈）
python verify_lite_pull_code_live.py      # lite 请求码 361 + lite 位（运行时翻 litePullMessageEnable，退出前还原）
python verify_flow_control_live.py        # 拉取前流控五个阈值 + 启动期数值闸门
python verify_pull_expired_live.py        # 拉取循环停摆自愈（120s 阈值）
python verify_ack_index_live.py           # classic 并发消费的 ackIndex 部分 ack
python verify_orderly_reconsume_live.py   # 顺序消费重投闸门 + 显式 COMMIT/ROLLBACK
python verify_redelivery_live.py          # 重投/死信终态/部分 ack/停摆自愈/顺序死信
python verify_correct_tags_offset_live.py # 空应答也把已提交位点推走
python verify_offset_illegal_live.py      # OFFSET_ILLEGAL 纠错分支（丢队列 + 修正位点立刻落盘）
python verify_reset_offset_live.py        # 220 重置消费位点（两种 body 形状 + 在途作废）
python verify_publish_route_master_live.py # 发布路由跳过没有 master 的 broker（会停一次 master）
python verify_clean_expired_msg_live.py   # 挂起 listener 的清扫逃生口（约 4 分钟，会删自建 topic）
python verify_sql92_live.py               # SQL92 过滤 + CHECK_CLIENT_CONFIG(46)，需 broker enablePropertyFilter=true
python verify_pop_live.py / verify_pop_consumer_live.py  # POP 控制面 / POP 消费循环（含 307 拉取统计）
python verify_latency_live.py             # 故障规避（延迟窗口/隔离/恢复）
```

任何脚本失败以非 0 退出码结束。部分脚本对集群有额外要求，已在上面逐条注明
（停/启 broker、需要从节点、需要开关 broker 配置并在退出时还原等）。

## 客户端日志

`rocketmq/logging.py` 把内部日志桥接到标准 `logging`：

- 文件落在 `$HOME/logs/rocketmqlogs/rocketmq_py_client.log`，按天滚动，
  备份名 `rocketmq_py_client.log.YYYY-MM-DD`，保留 `ROCKETMQ_CLIENT_LOG_MAX_INDEX`（默认 10）份；
- 同时输出到 stderr（可用 `ROCKETMQ_CLIENT_LOG_USE_STDOUT=false` 关掉）；
- 若宿主程序已配置过 Python logging（root 已有 handler），则**完全不插手**，
  把日志交给宿主的配置。

环境变量：`ROCKETMQ_CLIENT_LOG_DIR` / `ROCKETMQ_CLIENT_LOG_FILE` /
`ROCKETMQ_CLIENT_LOG_LEVEL` / `ROCKETMQ_CLIENT_LOG_MAX_INDEX` /
`ROCKETMQ_CLIENT_LOG_USE_STDOUT`。

> 📌 默认文件名 `rocketmq_py_client.log` 与其它语言端口的文件名刻意不同：
> 各端口轮转策略不同（Python 按天重命名），落到同一文件会互相插行。
> 回归守卫见 `tests/test_logging_config.py`。

## 目录结构

```
rocketmq/
├── common/                客户端共用的消息模型与常量
│   ├── message.py             Message / MessageExt / MessageBatch / MessageQueue
│   ├── message_decoder.py     17 段存储格式 + 6 段批量格式的编解码（含 zlib 解压）
│   ├── message_const.py       MessageConst 属性键（含 INDEX_KEY/UNIQUE/TAG_TYPE）
│   ├── sysflag.py             MessageSysFlag / PullSysFlag / PermName
│   ├── boundary_type.py       时间戳查位点的边界语义（LOWER/UPPER，含 getType 宽松解析）
│   ├── mix_all.py / util_all.py
│   ├── recall_message_handle.py 定时消息撤回句柄 v1（base64url + 5 段）
│   └── subscription_data.py / topic_config.py / message_accessor.py
├── remoting/              传输层（纯 socket + 线程）
│   ├── client.py              RemotingClient：同步 / 异步 / oneway
│   ├── exception.py
│   └── protocol/
│       ├── remoting_command.py  帧编解码（totalLen|headerLen+type|header|body）
│       ├── serialize.py         RemotingSerializable(JSON) / RocketMQSerializable(二进制)
│       │                        + fastjson2 容错解析器（见「管理端」）
│       ├── codes.py             RequestCode / ResponseCode / LanguageCode / SerializeType
│       ├── headers.py           CommandCustomHeader 家族（含 V2 短字段名 a..n）
│       ├── body.py / admin_body.py / route.py / heartbeat.py / subscription.py
├── client/                面向用户的 API
│   ├── producer.py / consumer.py / admin.py / mq_client.py
│   ├── hook.py / trace.py / trace_hook.py / trace_dispatcher.py
│   │                          钩子接口（Send/Consume/EndTransaction/CheckForbidden/FilterMessage）
│   │                          + 消息轨迹文本编解码 + 异步分发
│   └── send_result.py / consumer_result.py / exception.py
├── __main__.py            命令行入口（selfcheck）
└── selfcheck.py           无集群环境下的协议自检
```

## clientId 口径

默认 clientId 是 `<本机 IP>@<instanceName>[@<unitName>][@STREAM]`，而 `instance_name`
还是默认值 `DEFAULT` 时会在 `start()` 里被**就地**改写成 `<pid>#<monotonic_ns>`：
生产者与 admin 无条件执行，三个消费者只在 `CLUSTERING` 下执行 —— 广播消费者保持
`DEFAULT`，于是同进程的广播消费者算出同一个 clientId（在 `INSTANCE_MAP` 里占同一个键）。
就地写回意味着 restart 不换身份。

`unit_name` / `unit_mode` / `enable_stream_request_type` 三项配置五个门面都有，
默认值：producer/push/admin 关 stream，pull/lite 在构造时就开。⚠ 两处口径别混：
ExtFields 里的 `ReqT` 是 `RequestType.STREAM.getCode()` 的字符串形式 `"0"`，clientId 尾巴上
才是枚举 name `@STREAM`。传输层只有一槽钩子，顺序靠 `compose_request_hooks()` 还原 ——
stream 必须排在 ACL **之前**，否则 `ReqT` 落在签名之外；钩子要在 `MQClientInstance.start()`
之前注册。本机 IP 用 UDP「连」公网地址后读 sockname 取得。

回归测试：`tests/test_client_id.py`、`tests/test_unit_config.py`、`verify_unit_config_live.py`。

## 推送消费者启动期校验（`consumer.py:_check_config_ranges()`）

`start()` 里逐条检查 13 条数值区间，比较一律 `< lo or > hi`（两端闭区），文案是
Java 官方客户端的原文（只去掉 FAQ 短链尾巴）。要点：

- `pullThresholdForTopic` / `pullThresholdSizeForTopic` 的 `-1` 是"关闭"哨兵，
  **其余闸门没有这层豁免**，`-1` 照拒；
- `pullInterval` 的下界是 **0**（0 = 不间隔），别照抄邻居闸门的 1；
- `pullThresholdSizeForQueue` / `pullThresholdSizeForTopic` 的单位是 **MiB**；
- `consumeThreadMin > consumeThreadMax` 是**严格大于**（相等合法，允许单线程消费者），
  消息里带上两个数值；
- `popBatchNums` 跟随字面的 `<= 0`，文案仍写 `[1, 32]`；
- 校验排在所有 null 检查之后、`MQClientInstance` 建连之前 —— 坏配置必须在注册 clientId 之前
  失败，否则 broker 的 `ConsumerManager` 会留下一堆永不心跳的僵尸 clientId，把 rebalance 用的
  `cidAll` 撑歪（真机表现为队列分配不均，而客户端日志里只有启动失败那一条）。

`consume_timestamp` 是可配的 `%Y%m%d%H%M%S` 字符串，格式校验在建连之前**真会拒**。
回归守卫：`tests/test_consumer_check_config.py`（57 项，逐条锁区间两端、`-1` 哨兵、
检查顺序与文案）；真机守卫见 `verify_flow_control_live.py` 的 S5。

## 定时任务周期（pollNameServerInterval / persistConsumerOffsetInterval）

`MQClientInstance` 的后台周期任务如下。**每个循环的首跳都落在 `initialDelay` 这一刻**，
不是 `initialDelay + period`（`scheduleAtFixedRate` 语义；真机实测抓到过按后者排的错误）：

| 循环 | initialDelay | 周期 | 可配字段（默认） |
|------|--------------|------|------------------|
| 动态 name server 刷新 | 10s | 2min | —（仅未配置静态地址且有地址服务器时调度） |
| 在用 topic 路由刷新 | 10ms | `pollNameServerInterval` | `poll_name_server_interval`（30000ms） |
| 心跳 | 1s | 30s | `heartbeat_interval_millis` |
| 消费者位点落盘 | 10s | `persistConsumerOffsetInterval` | `persist_consumer_offset_interval`（5000ms） |
| 线程池弹性巡检 | 1min | 1min | — |

要点：

- `poll_name_server_interval` **五个门面都有**（producer / push / pull / lite / admin），
  并在各自 `start()` 里透传给 `MQClientInstance`；周期只在循环入口读一次，之后再改字段
  不影响已排定的任务（`scheduleAtFixedRate` 一次性排定）。
- `persist_consumer_offset_interval` 只管**后台周期落盘**；`shutdown()` 里的收尾落盘是
  无条件的，所以调大周期只推迟落盘时机、不丢位点。集群模式下位点落盘走**同步**
  `UPDATE_CONSUMER_OFFSET`，广播模式写本地 `~/.rocketmq_offsets/<clientId>/<group>/offsets.json`。
- 心跳有一处**已知且有意的偏差**：Python 在 `start()` 里先同步打一轮心跳，之后按 30s
  周期跑。首轮同步心跳覆盖了"注册 clientId 后立刻可见"的作用，周期不变。
- 上述周期与首跳次序由 `tests/test_scheduled_intervals.py` 逐条锁死；真机守卫
  `verify_interval_live.py` 用两条不同周期的实例对照，证明差异来自周期本身。

## 管理端（`client/admin.py`）

`DefaultMQAdminExt` 全套管理接口。三处**与直觉不符的服务端行为**，都是真机踩出来的：

| 陷阱 | 真实行为 |
| --- | --- |
| `GET_BROKER_CONFIG` | body 是 **properties 文本**（`"k=v\n"`），不是 JSON/KVTable。走 `MixAll.string2_properties`，语义对齐 `java.util.Properties.load`（`#`/`!` 注释、行尾 `\` 续行、**空白也是分隔符**） |
| `CreateTopicRequestHeader` | 必须带 `topicFilterType`，否则 broker 抛 `topicFilterType = [null] value invalid`；`attributes` 要传 `""` 而非 null |
| `ResetOffsetBody.offsetTable` | 是 `Map<MessageQueue, Long>`；MessageQueue 作 key 时 broker 侧会**内联成 JSON 对象**，产出**非法 JSON** |

最后一条是关键：broker 的序列化器（fastjson2）会把 map 的对象 key 内联
（`{{"brokerName":"b",...}:{...}}`）、数字 key 不加引号、允许 NaN/Infinity 与尾逗号。
所以 `serialize.py` 里是**自写的容错解析器** `fastjson_loads`，不是标准 `json.loads`。
**改这个解析器时务必保留宽容逻辑**，否则所有 Admin 响应体全崩。

`GET_ALL_SUBSCRIPTIONGROUP_CONFIG` 是**分页**接口（`groupSeq`/`maxGroupNum`/`dataVersion`）；
老 broker 响应里没有 `totalGroupNum`，此时一轮即结束。KV 配置类请求打到 **NameServer**，
且 PUT/DELETE 要广播到每一个 NameServer。

`GET_MESSAGE` 类查询中，**uniqKey（msgId）查询需要 broker 开 RocksDB 索引**；
默认文件索引 + 消息未设 KEYS 时查不到是 **broker 配置差异，不是客户端 bug**。

位点重置有**两条不同的路径**，别再混：`reset_offset_by_timestamp`（222 不带
`queueId`、`offset=-1` 表示 null）按时间戳整 topic 重置；`reset_offset_by_queue_id`
是**两笔** RPC —— 先 `update_consumer_offset`(25) 写 offsetTable，再一笔带
`queueId`+`offset` 的 222 让 broker 走 `resetOffsetInner` → `assignResetOffset`
（同时写**一次性**的 `resetOffsetTable`）。真机（5.5.1）量到两件事：

1. 重置后**首笔 pull 拿不到消息** —— broker 直接回 `OFFSET_RESET` ⇒ `PULL_OFFSET_MOVED`，
   客户端映射成 `OFFSET_ILLEGAL` + `next_begin_offset=重置位点`，第二笔才真正取到历史消息。
2. 这两笔**不是原子的**：越界目标下第 1 笔已把非法位点落库、第 2 笔才被
   `Target offset N not in consume queue range [min-max]` 拒掉。不做保护性回滚，
   `verify_admin_live.py` 第 9.5 节把该行为钉成断言。

`query_topics_by_consumer` 是**组级**重载（只收 `group`：按 `%RETRY%<group>` 查路由、
逐 broker 扇出 343、按 Set 去重合并），原先那笔单 broker 的原始调用改名为
`query_topics_by_consumer_to_broker`。343 读的是 offsetTable（`whichTopicByConsumer`），
所以**组没提交过位点时回空表是预期**。

时间戳查位点带 **`boundaryType`**：admin 的两个边界入口
`search_lower_boundary_offset` / `search_upper_boundary_offset` 分别固定发
`LOWER` / `UPPER`，`search_offset` 等价于 LOWER；MQ 级 `search_offset_by_timestamp`
默认也是显式 LOWER，传 `boundary_type=None` 则整键不写。入网文本是**大写枚举名**
（`LOWER`/`UPPER`），字段 `@CFNullable`，broker 缺键回落 LOWER，有键但值不认识
（只认 `equalsIgnoreCase("upper")`）同样回落。真机判别式（`verify_admin_live.py`
第 7.5 节）：1 队列 topic 发 3 条后对远未来时间戳查位点，LOWER = maxOffset(3)、
UPPER = maxOffset-1(2) —— 两个数不同即证明字段真的到了 broker 并被解析。
报文形状由 `tests/test_search_offset_boundary.py` 锁死。

## 两条消息编码路径

这是最容易混淆的地方，**不能混用**：

| 场景 | Python 函数 | 段数 |
| --- | --- | --- |
| broker 写入 / pull 返回 | `encode_message_ext` | 17 段 |
| 批量消息 body | `encode_message` / `encode_messages` | 6 段 |
| 普通消息发送请求体 | 直接取 `msg.get_body()` | 原始字节 |

17 段格式（`MessageExt`）：

```
TOTALSIZE(4) | MAGICCODE(4) | BODYCRC(4) | QUEUEID(4) | FLAG(4) | QUEUEOFFSET(8)
| PHYSICALOFFSET(8) | SYSFLAG(4) | BORNTIMESTAMP(8) | BORNHOST(8|20) | STORETIMESTAMP(8)
| STOREHOST(8|20) | RECONSUMETIMES(4) | PREPAREDTRANSACTIONOFFSET(8) | BODY(4+n)
| TOPIC(1|2+n) | PROPERTIES(2+n)
```

MAGICCODE 为 `-626843481`（v1，topic 长度 1 字节）或 `-626843477`（v2，topic > 127 字符时
broker 侧使用，topic 长度 2 字节）。6 段格式里 MAGICCODE 与 BODYCRC 固定为 0，且不含 topic。

## RemotingCommand 帧格式

```
totalLength(4) | headerLength(4) | headerData | bodyData
                ^^ 高 8 位是序列化类型，低 24 位是 header 长度
```

`totalLength = 4 + len(headerData) + len(bodyData)`。JSON 头为普通 JSON 对象；
ROCKETMQ 二进制头的布局为
`code(2) | language(1) | version(2) | opaque(4) | flag(4) | remark(4+n) | extFields(4+map)`，
map 里 key 用 short 长度、value 用 int 长度。

## flag 语义

```
bit0 = 响应类型（RPC_TYPE）      bit1 = oneway（RPC_ONEWAY）
```

`create_response_command` 会置位 bit0；`mark_oneway_rpc` 置位 bit1。

## TLS 连接（`tls_enable=True`）

5.5.1 的 nameServer/broker 在 `tls.test.mode.enable`（默认 true）下按首字节嗅探协议，
同一个端口明文与 TLS 都收，所以整条链路可以直接对真集群验：`verify_tls_live.py`
（30 轮新建 TLS 连接打首包、producer+push consumer 全程 TLS 收发、确认没有连接悄悄退回
明文、shutdown 后不留读线程）。

两条在 macOS loopback 上实测出来的约束，都写在 `rocketmq/remoting/client.py` 的对应
docstring 里，改动前先读它们：

- **读线程要等第一个记录写出去再起。** 握手刚完成就让读线程进 OpenSSL（`pending()` /
  `recv()`）时，紧接着的第一个请求记录有约 3~5% 根本到不了对端：`sendall()` 返回成功，
  对端的 TLS 读一直等到超时，调用方只能等满 invoke 超时。这条只在对端是 CPython `ssl`
  时稳定复现（对真集群的 nameServer/broker 各 60 轮两种时序都是 0 丢），所以回归守卫放在
  `tests/test_tls_trace.py` 的本地 TLS mock server 上。明文连接 0%，只有 TLS 连接走
  `_write` 里的延迟起线程。
- **关闭必须走 `close_notify`。** 直接 `closesocket()` 时，内核接收缓冲里还留着对端
  TLS 1.3 的 NewSessionTicket 没被 SSL 层读走，于是发 RST 而不是 FIN；这条 RST 会打到
  复用同一 4 元组的下一条新连接上，实测约 25~30% 静默吞掉它的首包。

因此 TLS 连接的套接字一律由该连接的读线程关闭（`close_channel` / `shutdown` 会先把还没
起读线程的连接补上，避免 fd 泄漏）。两个线程同时进一个 OpenSSL 对象会踩坏其内部状态
（实测段错误），不要用"加锁"替代上面两条。

## 消息压缩

生产端在 `body >= compress_msg_body_over_howmuch`（默认 **4096**）时自动压缩，
置 `COMPRESSED_FLAG | 类型位`；`MessageBatch` **不压缩**。消费端在 `decode_message`
里检测到 `COMPRESSED_FLAG` 就解压，并**清掉该标志位**。

几个反直觉点：

- **压缩只在重试循环外做一次。** 若在循环内就地 `set_body` 压缩，重试时会把已压缩的
  body **再压一遍**（`zlib(zlib(x))`），而消费端只解一层 → 拿到压缩流。
- 压缩类型位 `0` 与 `3` **都按 ZLIB 解**（向后兼容），老客户端产的消息类型位是 0。
- **未支持的算法（如 SNAPPY=4）必须抛错，不要原样透传** —— 静默透传等于静默数据损坏。
  这里有两层，都要保住：
  1. `_decompress` 对未支持类型抛 `RuntimeError`；
  2. `decode_message` 捕获后返回 `None`（即"消息被丢弃"）。
- `zlib` 是 Python 标准库，解压路径无需额外依赖；`lz4`/`zstd` 未安装时**抛错**而非静默降级。

## 发送请求码：310 / 320 / 325

`_build_send_request` 的三条分支都要在线上报文取证（`tests/test_request_reply.py`）：

| 消息 | 请求码 | V2 头 `m`(batch) |
| --- | --- | --- |
| 普通 | `SEND_MESSAGE_V2(310)` | `false` |
| `MessageBatch` | `SEND_BATCH_MESSAGE(320)` | `true` |
| `MSG_TYPE == "reply"` | `SEND_REPLY_MESSAGE_V2(325)` | 视是否批量 |

两点容易搞混：**判据顺序**先 reply 再批量，带 reply 属性的批量仍走 325；
**请求码与 `m` 是两件事** —— broker 靠 `m` 决定走批量还是单条写入，请求码只影响服务端
按码归类。所以两个必须成对断言，只改码不改 `m` 会让批量 body 被按单条解析。
真机侧由 `verify_live_clean.py` 的「批量发送 3 条」+ 消费计数对账覆盖。

## 异步发送 `send_async`

异步发送串了三个部件（回归守卫 `tests/test_producer_async.py`，真机守卫
`verify_message_types.py` 前 9 项）：

| 部件 | 实现 |
| --- | --- |
| 发送线程池 | `ConsumeExecutor(core,max=cpu_count, max_queue_size=async_sender_queue_capacity)`，线程名 `AsyncSenderExecutor_1` 起 |
| 回调线程池 | `_callback_executor`，线程名 `NettyClientPublicExecutor_1` 起；`client_callback_executor_threads` 可覆盖 |
| 失败换 broker | `_on_send_exception` |

用户回调**永远不跑在读线程/超时扫描线程上**，这是那两个池存在的唯一理由。调用方
`send_async` 只入队就返回（实测 `callerBlockedMs=0`）。

三个入口级判据：

- 队列满 → `MQClientException("executor rejected")`；
- 排队已经把预算吃光 → `RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout")`，
  连请求都不会建；
- ASYNC 的 `timesTotal` 固定为 1，重试次数读 `retry_times_when_send_async_failed`。

重试的四个细节最容易漏：跨重试**复用同一个请求对象**（`SendMessageRequestHeaderV2` 不带
brokerName，所以换 broker 不必重建），但每次尝试**换一个 `opaque`**（复用会让两次尝试的
应答串台）；`select_one_message_queue(publish, last_broker, False)` 避开刚失败的那台；
超时用的是**共享的剩余预算**而不是重新给一份；`timeout <= 0` 立即停。

失败分类（`_classify_async_failure`）不是均匀的：**broker 明确回了错误码时原样交给回调、
不换 broker 重试**，这和同步发送的 `retry_response_codes` 语义**不同**；
`RemotingSendRequestException`/`RemotingTimeoutException`/其它 `RemotingException` 各包一层
`MQClientException`（`send request failed` / `wait response timeout, cost=N` /
`unknown reason`）并重试，只有 `RemotingTooMuchRequestException` 不重试；而外层 catch
（同步抛出）走 raw-exception 分支，**不包装**。`request()` 内部就是这条 ASYNC 路径 +
等 latch，所以它同样受上述全部语义约束。

地址解析走两步：先查发布地址表，查不到**按 topic 刷一次路由**再查。这一步是定点发送
（调用方直接给 `mq`）唯一的路由来源 —— 它不会在 `sendDefaultImpl` 里取发布信息，少了
这一步第一次定点发送必然带着空地址去连；刷完还没有就回调
`MQClientException("The broker[x] not exist")`。ASYNC 分支另有自己的一道总闸：钩子、
压缩、建请求的耗时都算进预算，被吃光时不再发起请求，回调拿
`RemotingTooMuchRequestException("sendKernelImpl call timeout")` 且不重试。

刻意保留的三处行为：未 `start()` 时 `send_async` 同步抛；批量消息在 sender 池里复用同步
批量内核；`shutdown()` 不等在途异步任务 —— 实测（`verify_async_send_live.py` A6）36 笔
全部拿到终态回调但整轮以 `client already shutdown` 报错、broker 上一条都没落，所以
"发完立刻 shutdown"会丢消息。默认关闭的**信号量背压**已完整移植
（`enable_backpressure_for_async_mode` + `FairSemaphore` 两个维度，守卫见
`verify_backpressure_live.py`）。

## License

Apache-2.0，与上游 RocketMQ 保持一致。
