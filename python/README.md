# rocketmq-client-remoting (Python)

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 Python 实现，迁移自 Java 的
`org.apache.rocketmq.client` + `org.apache.rocketmq.remoting` + `org.apache.rocketmq.tools`，
目标是让 Python 进程可以直接用 **JSON / RocketMQ 二进制** 两种序列化方式与
NameServer、Broker 通信。

对齐的 Java 源码位于 `rocketmq-client/java`（模块 `client` / `remoting` / `common` / `tools`）。

## 安装与测试

```bash
pip install -e .
pytest -q                     # 833 条单元/协议测试（829 passed + 4 skip，skip 为可选依赖相关）
python -m rocketmq selfcheck  # 协议编解码回环自检（7 项）
```

需要更强的回归守卫时，把环境变量指向 Java 协议的源码目录，测试会逐条比对常量取值：

```bash
ROCKETMQ_JAVA_SRC=<...>/remoting/src/main/java/org/apache/rocketmq/remoting/protocol pytest -q
```

### 真实集群联调（无 mock，需先起 nameServer(9876) + broker(10911)）

```bash
python verify_message_types.py    # 7 类消息能力，18 PASS/0 FAIL（异步 9 项：不阻塞返回、线程口径、并发、定点、失败只走回调）
python verify_request_reply_live.py # request-reply 全链路（16 PASS/0 FAIL）：325 落地、REPLY_TO_CLIENT=真实 clientId、超时/并发/普通消费不受影响
python verify_admin_live.py       # 管理端全链路 + sendMessageBack 重投（63 PASS/0 FAIL/1 SKIP）
python verify_compression_live.py selftest   # 自动压缩自产自销 + broker 侧压缩体校验
python verify_compression_live.py send|recv <topic> <group> <size>   # 与 Java 探针跨客户端互通
python verify_trace_live.py       # 消息轨迹全链路（17 PASS/0 FAIL，需 broker traceTopicEnable=true）
python verify_hook_live.py        # CheckForbidden/FilterMessage 钩子（13 PASS/0 FAIL）
python verify_validators_live.py  # 名字校验（24 PASS/0 FAIL）：非法 topic/group 本地快拒、合法名字照常收发、往返对照腿
python verify_recall_live.py      # 定时消息撤回 recallMessage(370)（15 PASS/0 FAIL，脚本会打开并在退出时还原 broker 的 recallMessageEnable）
python verify_unit_config_live.py # unitName/unitMode/stream（13 PASS/0 FAIL）：clientId 后缀、broker 侧 topic 的 UNIT/UNIT_SUB 位、每笔请求的 ReqT
python verify_lite_pull_live.py   # lite pull 全链路（32 PASS/0 FAIL）：rebalance/收 12 条/commit/assign+seek/tag/时间戳起点/pause+resume + 队列分配策略（默认 AVG、null 被 start() 拒、AVG_BY_CIRCLE 两实例交叉、CONFIG 两半不重叠、CONSISTENT_HASH 用真实 clientId 建环并收敛到离线预测、MACHINE_ROOM_NEARBY 单机房透传内层策略且 resolver 被真实 brokerName/clientId 问过、MACHINE_ROOM 白名单不匹配 broker-a 时安静饿死）
python verify_sql92_live.py       # SQL92 过滤 + CHECK_CLIENT_CONFIG(46)（20 PASS/0 FAIL）：SQL92 订阅启动时正好一笔 46、纯 TAG 订阅一笔不发；broker 真按属性过滤（red 只收 3 条、blue 不漏、'*' 对照组收 6 条、永不匹配收 0 条）；语法错的表达式让 start() 秒回 SUBSCRIPTION_PARSE_FAILED(23) 并就地回滚。需 broker 开 enablePropertyFilter=true
python verify_tls_live.py         # 整条客户端链路跑 TLS（8 PASS/0 FAIL）：30 轮新建 TLS 连接打首包、producer+push consumer 全程 TLS 收发、确认没退回明文、shutdown 不留读线程
python verify_async_send_live.py  # 异步发送内核 A1~A6（39 PASS/0 FAIL）：不阻塞返回 + 线程口径（AsyncSenderExecutor_1 跑准备段、NettyClientPublicExecutor_1 跑回调）+ 用 offsetMsgId 读回原文、30 笔并发各恰好一个终态且槽位/UNIQ_KEY 不重复、定点发送、CheckForbiddenHook 拒绝不留痕、批量走同步批量内核（一次回调、broker 逐条回 3 个 commitLog 偏移、读回的子消息带客户端 32 位 UNIQ_KEY）、shutdown 不等在途（36 笔全报错、一条都没落）
python verify_backpressure_live.py # 异步发送背压 B1~B5（Java 两个公平信号量，真机版）
python verify_flow_control_live.py # 拉取前流控五个阈值 S0~S4（13 PASS/0 FAIL）：S0 默认闸门+快消费**不命中**（12 条全到，闸门误伤正常流量表现为吞吐莫名腰斩，最难查）→ S1 只留队列级字节闸门（`pull_threshold_size_for_queue=1`，单位 **MiB**）⇒ 命中 15 次、8 条 400KB 一条不丢不重 → S2 只留跨度闸门（`consume_concurrently_max_span=2`）⇒ 命中 5 次、14 条仍全部消费 → S3 只留 topic 级条数闸门（`pull_threshold_for_topic=4`，4 队列）⇒ 单队列到不了 4 条、必须跨队列累计（命中 135 次）且每条队列都消费到底 → S4 复用 S1 的组与 topic ⇒ 位点从 broker 末尾续上、闸门**不是命中一次就失效**（仍命中 9 次）、6 条不重不丢（锁"暂停"被写成"退出拉取循环"，S1 看不出差别）。五个阈值的判定顺序、`Math.max(1,n)` 的守卫、**严格大于**的跨度边界、topic 级字节闸门**不复用**队列级那道开关、命中一次只记一格，都由 `tests/test_flow_control.py`（7 项）离线锁死；真机这一半锁的是离线锁不住的"确实会命中"和"命中后不丢"；命中的**次数**随真机投递/消费节奏浮动（S3 两轮分别报 105 与 135），判据只要求 `triggered > 0`。⚠ 两条夹具坑（都是实测踩出来的）：大消息必须**不可压缩**（`os.urandom`，全同字节会被生产者压到几百字节、broker 落盘 `store_size` 跟着变几百字节，size 闸门于是"永不命中"）；S1/S4 的 topic 必须**只有 1 条队列**（8 条 400KB 摊到 4 条队列每条才 800KB，永远够不到队列级那道 1MiB）
python verify_pull_expired_live.py # 拉取循环停摆**自愈**（Java isPullExpired / PULL_MAX_IDLE_TIME=120s，`RebalanceImpl.updateProcessQueueTableInRebalance:438-461`，11 PASS/0 FAIL）：A1 基线（3 条被消费、位点到 3、307 运行信息里 `lastPullTimestamp` 是循环自己盖的真时刻且新鲜）→ A2 把这一路的线程表条目换成一条**已退出的线程**（等价于循环被异常打穿）⇒ 下一趟 rebalance 必须换上另一条活线程，新发的 3 条照样被消费（位点到 6）→ A3 线程还活着但把盖章时刻**倒拨 121s**（> 120s）⇒ 同样被撤并重建、再发 3 条照样消费（位点到 9）→ A4 前 9 条各只投一次、`reconsumeTimes` 全 0、时钟恢复新鲜（撤走前持久化了位点，重建从 broker 位点续拉）。恢复判据用「既不是原线程、也不是注入的那条、而且活着」，否则注入还没被撤走也会被误判成通过。这条路径坏掉是**静默的**：不报错、心跳照发、别的队列照常推进，真机上只能从"某条队列位点永远不动"反推，所以停摆→恢复的闭环必须真机取证；阈值 120s 与**严格大于**的边界、盖章在流控/锁判定**之前**（`pullMessage:253`，卡住的循环也要留心跳）、POP 分支读 `lastPopTimestamp`（`PopProcessQueue:74`）、停机途中不判停摆，都由 `tests/test_pull_expired.py`（13 项）离线锁死
python verify_ack_index_live.py   # classic 并发消费的 ackIndex 部分 ack（11 PASS/0 FAIL）：A1 对照组整批认可（3 条各投一次、位点到 3、零回投）→ A2 ackIndex=0 只认可首批第一条 ⇒ 尾巴 2 条经 %RETRY% 二次到达（reconsumeTimes>=1、topic 还原成业务 topic）、被认可那条整个窗口只投一次、3 条最终全部消费、业务队列位点仍整批提交到 3 → A3 ackIndex=2 压不住 RECONSUME_LATER（Java :222-226 强制 ackIndex=-1，3 条全重投）→ A4 广播模式下尾巴不回投。批次切分由拉取时机决定，所以三个用例都**先把 3 条放上去再起消费者**（新组显式 CONSUME_FROM_FIRST_OFFSET），否则首批可能是 1~2 条、前缀/后缀根本不确定
python verify_send_header_live.py # 发送头 c/d/n 三个字段（14 PASS/0 FAIL）：H1 默认 `d=4` 时自动建出的 topic 队列数 = `min(4, TBW102.writeQueueNums)`；H2 `set_default_topic_queue_nums(2)` 真的让 broker 只建 2 条队列（写死 4 的旧行为必然是 4）；H3 `set_create_topic_key(模板)` 时继承**模板**的 3 条队列而不是 TBW102 的 8 条（`TopicConfigManager.java:286-289` 的 `isInherited` + `min`）；H4 同步/定点/单向/批量 320/异步五种入口逐条落地（7 条一条不差）；H5 落点 broker 名与路由一致。`n`（brokerName）在经典 broker 的发送链路里**没有读者**（5.5.1 源码 grep 过），它的线上存在由 `tests/test_send_header_fields.py`（7 项，抓真报文）取证
```

其它真机脚本：`verify_acl_live.py`（需开 ACL 的集群）/ `verify_pull_live.py` /
`verify_rr_live.py` / `verify_latency_live.py` / `verify_pop_live.py` /
`verify_pop_consumer_live.py` / `verify_redelivery_live.py`（30 PASS / 0 FAIL）。

`verify_redelivery_live.py` 的 S9 是**死信终态**：`maxReconsumeTimes=2` 的组对同一条消息只投
3 次（listener 一直回 `RECONSUME_LATER`），实测档位 `0s / 10s / 40s` —— Java broker 用
`delayLevel = 3 + reconsumeTimes` 决定重投延迟（`AbstractSendMessageProcessor:209`，本机
`messageDelayLevel` 的 3/4 档是 10s/30s）；第 3 次回投时 `reconsumeTimes >= maxReconsumeTimes`
成立，broker 把 topic 改写成 `%DLQ%<group>` 并现场建出这条 topic（`:193`），存进去的
`reconsumeTimes` 还要再 +1（`:228`），`RETRY_TOPIC` 保留原业务 topic。判据的客户端半边是
`DefaultMQPushConsumerImpl#getMaxReconsumeTimes:890` 的 `-1 → 16`，所以 `-1` 与 `16` 等价。
观察窗口给 150s：整机并发时 broker 的定时服务会拖档，卡 100s 会假失败。

`verify_compression_live.py` 的载荷是确定性的，与 Java 探针
`/tmp/probe_admin/CompressProbe.java` 同算法，因此可直接验证"Java 产的压缩消息我们能否解开"
以及反向。注意 Java `UtilAll.crc32` 会 `& 0x7FFFFFFF`，与标准 CRC-32 **差正好 2^31**，
对结果时看各自的 `match` 字段而不是 CRC 数字。

## 客户端日志

`rocketmq/logging.py` 把内部日志桥接到标准 `logging`：

- 文件落在 `$HOME/logs/rocketmqlogs/rocketmq_py_client.log`，按天滚动，
  备份名 `rocketmq_py_client.log.YYYY-MM-DD`，保留 `ROCKETMQ_CLIENT_LOG_MAX_INDEX`（默认 10）份；
- 同时输出到 stderr（可用 `ROCKETMQ_CLIENT_LOG_USE_STDOUT=false` 关掉）；
- 若宿主程序已配置过 Python logging（root 已有 handler），则**完全不插手**，
  等价于 Java 客户端的 `logUseSlf4j` 模式。

环境变量：`ROCKETMQ_CLIENT_LOG_DIR` / `ROCKETMQ_CLIENT_LOG_FILE` / `ROCKETMQ_CLIENT_LOG_LEVEL`
/ `ROCKETMQ_CLIENT_LOG_MAX_INDEX` / `ROCKETMQ_CLIENT_LOG_USE_STDOUT`。

> 📌 默认文件名**刻意不叫** Java 的 `rocketmq_client.log`。两者轮转策略不同（Java logback 按大小
> 64MB 滚动并 gzip，Python 按天重命名），落到同一文件会互相插行；更糟的是 Python 在午夜会把文件
> **改名**，而 JVM 仍持有旧 fd，后续 Java 日志会写进已 unlink 的 inode 而静默消失。
> 需要与 Java 一致时显式设 `ROCKETMQ_CLIENT_LOG_FILE=rocketmq_client.log`。
> 对应回归守卫见 `tests/test_logging_config.py`。

## 目录结构

```
rocketmq/
├── common/                客户端共用的消息模型与常量
│   ├── message.py             Message / MessageExt / MessageBatch / MessageQueue
│   ├── message_decoder.py     17 段存储格式 + 6 段批量格式的编解码（含 zlib 解压）
│   ├── message_const.py       MessageConst 属性键（含 INDEX_KEY/UNIQUE/TAG_TYPE）
│   ├── sysflag.py             MessageSysFlag / PullSysFlag / PermName
│   ├── mix_all.py / util_all.py
│   ├── recall_message_handle.py 定时消息撤回句柄 v1（base64url + 5 段，Java 同格式）
│   └── subscription_data.py / topic_config.py / message_accessor.py
├── remoting/              传输层（不依赖 netty，纯 socket + 线程）
│   ├── client.py              RemotingClient：同步 / 异步 / oneway
│   ├── exception.py
│   └── protocol/
│       ├── remoting_command.py  帧编解码（totalLen|headerLen+type|header|body）
│       ├── serialize.py         RemotingSerializable(JSON) / RocketMQSerializable(二进制)
│       │                        + **fastjson2 容错解析器**（见下）
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

`MixAll.client_id_for(instance_name, unit_name, enable_stream_request_type)` 对应 Java
`ClientConfig#buildMQClientId`：默认 clientId 是
`<本机 IP>@<instanceName>[@<unitName>][@STREAM]`，而 `instance_name` 还是默认值 `DEFAULT`
时会在 `start()` 里被**就地**改写成 `<pid>#<monotonic_ns>`（对应 `changeInstanceNameToPID`）——
生产者与 admin 无条件执行，三个消费者只在 `CLUSTERING` 下执行 —— 广播消费者保持
`DEFAULT`，于是同进程的广播消费者算出同一个 clientId（Java 的 `MQClientManager` 会让它们
复用同一份实例；本实现每个门面各建一份，只在 `INSTANCE_MAP` 里占同一个键）。就地写回
意味着 restart 不换身份。

`unit_name` / `unit_mode` / `enable_stream_request_type` 三项配置五个门面都有（setter 与
Java 同名），默认值也对齐：producer/push/admin 关 stream，pull/lite 在构造时就开
（`DefaultMQPullConsumer:113/126`、`DefaultLitePullConsumer:213/228`）。⚠ 两处口径别混：
ExtFields 里的 `ReqT` 是 `RequestType.STREAM.getCode()` 的字符串形式 `"0"`，clientId 尾巴上
才是枚举 name `@STREAM`。传输层只有一槽钩子（Java 是 `List<RPCHook>`），顺序靠
`compose_request_hooks()` 还原——stream 必须排在 ACL **之前**（`MQClientAPIImpl:329-332`），
否则 `ReqT` 落在签名之外；钩子还要在 `MQClientInstance.start()` 之前注册（Java 随
`MQClientAPIImpl` 构造传入）。与 Java 的另一处不同是本机 IP 用 UDP「连」公网地址后读
sockname（Java 枚举网卡）。回归测试：`tests/test_client_id.py`、`tests/test_unit_config.py`、
`verify_unit_config_live.py`。

## 管理端（`client/admin.py`）

`DefaultMQAdminExt` 对照 Java 的 `DefaultMQAdminExt` / `DefaultMQAdminExtImpl` / `MQAdminImpl`
逐项移植。有三处 **Java 5.x 的真实行为与直觉不符**，都是真机踩出来的：

| 陷阱 | 真实行为 |
| --- | --- |
| `GET_BROKER_CONFIG` | body 是 **properties 文本**（`"k=v\n"`），不是 JSON/KVTable。走 `MixAll.string2_properties`，语义对齐 `java.util.Properties.load`（`#`/`!` 注释、行尾 `\` 续行、**空白也是分隔符**） |
| `CreateTopicRequestHeader` | 必须带 `topicFilterType`，否则 broker 抛 `topicFilterType = [null] value invalid`；`attributes` 要传 `""` 而非 null |
| `ResetOffsetBody.offsetTable` | 是 `Map<MessageQueue, Long>`；MessageQueue 作 key 时 fastjson2 会**内联成 JSON 对象**，产出**非法 JSON** |

最后一条是关键：fastjson2 会把 map 的对象 key 内联（`{{"brokerName":"b",...}:{...}}`）、
数字 key 不加引号、允许 NaN/Infinity 与尾逗号。所以 `serialize.py` 里是**自写的容错解析器**
`fastjson_loads`，不是标准 `json.loads`。**改这个解析器时务必保留宽容逻辑**，否则所有
Admin 响应体全崩。

`GET_ALL_SUBSCRIPTIONGROUP_CONFIG` 是**分页**接口（`groupSeq`/`maxGroupNum`/`dataVersion`）；
老 broker 响应里没有 `totalGroupNum`，此时一轮即结束。KV 配置类请求打到 **NameServer**，
且 PUT/DELETE 要广播到每一个 NameServer。

`GET_MESSAGE` 类查询中，**uniqKey（msgId）查询需要 broker 开 RocksDB 索引**；
默认文件索引 + 消息未设 KEYS 时查不到是 **broker 配置差异，不是客户端 bug**。

位点重置有**两条不同的 Java 路径**，别再混：`reset_offset_by_timestamp`（222 不带
`queueId`、`offset=-1` 表示 Java 的 null）按时间戳整 topic 重置；`reset_offset_by_queue_id`
是**两笔** RPC——先 `update_consumer_offset`(25) 写 offsetTable，再一笔带 `queueId`+`offset`
的 222 让 broker 走 `resetOffsetInner` → `assignResetOffset`（同时写**一次性**的
`resetOffsetTable`）。真机（5.5.1）量到两件事：

1. 重置后**首笔 pull 拿不到消息**——broker 直接回 `OFFSET_RESET` ⇒ `PULL_OFFSET_MOVED`，
   客户端映射成 `OFFSET_ILLEGAL` + `next_begin_offset=重置位点`（`PullMessageProcessor:539-548/672`
   + `MQClientAPIImpl:1098`），第二笔才真正取到历史消息。
2. 这两笔**不是原子的**：`ConsumerOffsetManager#commitOffset` 无区间校验（连位点变小都只打
   `[NOTIFYME]` warn），所以越界目标下第 1 笔已把非法位点落库、第 2 笔才被
   `Target offset N not in consume queue range [min-max]` 拒掉。Java 同样如此，
   这里不做保护性回滚，`verify_admin_live.py` 第 9.5 节把该行为钉成断言。

`query_topics_by_consumer` 对齐 Java 的**组级**重载（只收 `group`：按 `%RETRY%<group>` 查路由、
逐 broker 扇出 343、按 Set 去重合并），原先那笔单 broker 的原始调用改名为
`query_topics_by_consumer_to_broker`。343 读的是 offsetTable（`whichTopicByConsumer`），
所以**组没提交过位点时回空表是预期**。

## 两条消息编码路径

这是最容易混淆的地方，**不能混用**：

| 场景 | Java 方法 | Python 函数 | 段数 |
| --- | --- | --- | --- |
| broker 写入 / pull 返回 | `MessageDecoder.encode(MessageExt, boolean)` | `encode_message_ext` | 17 段 |
| 批量消息 body | `MessageDecoder.encodeMessage(Message)` | `encode_message` / `encode_messages` | 6 段 |
| 普通消息发送请求体 | `request.setBody(msg.getBody())` | 直接取 `msg.get_body()` | 原始字节 |

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

`totalLength = 4 + len(headerData) + len(bodyData)`。JSON 头为普通 fastjson 对象，
其中 `body`、`customHeader` 等字段带 `@JSONField(serialize = false)` 不参与序列化；
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
同一个端口明文与 TLS 都收，所以整条链路可以直接对真集群验：`verify_tls_live.py`（8 PASS /
0 FAIL：30 轮新建 TLS 连接打首包、producer+push consumer 全程 TLS 收发 8 条、确认没有连接
悄悄退回明文、shutdown 后不留读线程）。

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
置 `COMPRESSED_FLAG | 类型位`；`MessageBatch` **不压缩**（对齐 Java
`DefaultMQProducerImpl.sendKernelImpl`）。消费端在 `decode_message` 里检测到
`COMPRESSED_FLAG` 就解压，并**清掉该标志位**（对齐 Java `MessageDecoder` 520-523 行）。

几个反直觉点：

- **压缩只在重试循环外做一次**。Java 在循环内调 `tryToCompressMessage`，它就地 `setBody`，
  重试时会把已压缩的 body **再压一遍**（`zlib(zlib(x))`），而消费端只解一层 → 拿到压缩流。
  本实现按"循环外压一次"处理。
- 压缩类型位 `0` 与 `3` **都按 ZLIB 解**（Java `CompressionType.findByValue` 的向后兼容），
  老客户端产的消息类型位是 0。
- **未支持的算法（如 SNAPPY=4）必须抛错，不要原样透传** —— 静默透传等于静默数据损坏。
  这里有两层，都要保住（Java 5.x 就是这个形状）：
  1. `_decompress` 对未支持类型抛 `RuntimeError`（对齐 Java `CompressorFactory.getCompressor`）；
  2. `decode_message` 捕获后返回 `None`（对齐 Java `MessageDecoder.decode` 的
     `catch → null`，即"消息被丢弃"）。

  早期实现两层都错：`_decompress` 直接 `return data` 透传，于是 `decode_message` 会返回一条
  body 是压缩字节、而 `COMPRESSED_FLAG` 已被清掉的消息 —— 事后完全无法识别。已修并有回归测试。
- `zlib` 是 Python 标准库，解压路径无需额外依赖；`lz4`/`zstd` 未安装时**抛错**而非静默降级。

## 发送请求码：310 / 320 / 325

`_build_send_request` 的判据与 Java `MQClientAPIImpl.sendMessage:550-563` 一致，三条分支
都要在线上报文取证（`tests/test_request_reply.py`）：

| 消息 | 请求码 | V2 头 `m`(batch) |
| --- | --- | --- |
| 普通 | `SEND_MESSAGE_V2(310)` | `false` |
| `MessageBatch` | `SEND_BATCH_MESSAGE(320)` | `true` |
| `MSG_TYPE == "reply"` | `SEND_REPLY_MESSAGE_V2(325)` | 视是否批量 |

两点容易搞混：**判据顺序**先 reply 再批量（Java 就是先 `isReply`），带 reply 属性的批量
仍走 325；**请求码与 `m` 是两件事** —— broker 靠 `m` 决定走 `sendBatchMessage` 还是单条
写入（`SendMessageProcessor:117` 读 `requestHeader.isBatch()`），请求码只影响服务端按码
归类（proxy `AbstractRemotingActivity:69`、auth
`DefaultAuthorizationContextBuilder:230-240` 把 310/320 列在同一个 case 里）。所以两个
必须成对断言，只改码不改 `m` 会让批量 body 被按单条解析。真机侧由
`verify_live_clean.py` 的「批量发送 3 条」+ 消费计数对账覆盖。

## 异步发送 `send_async`

Java 的异步发送不是一条直路，它串了三个部件，本实现逐个对齐（回归守卫
`tests/test_producer_async.py`，真机守卫 `verify_message_types.py` 前 9 项）：

| 部件 | Java 出处 | 本实现 |
| --- | --- | --- |
| 发送线程池 | `DefaultMQProducerImpl:133-140`：`core=max=availableProcessors`、队列 50000、线程名 `AsyncSenderExecutor_` | `ConsumeExecutor(core,max=cpu_count, max_queue_size=async_sender_queue_capacity)`，线程名 `AsyncSenderExecutor_1` 起 |
| 回调线程池 | `NettyRemotingAbstract.executeInvokeCallback:488-517` 把 `onSuccess/onException` 交给 `callbackExecutor`；`NettyClientConfig:28` 默认 **`availableProcessors()`**（`<=0` 才退成 4） | `_callback_executor`，线程名 `NettyClientPublicExecutor_1` 起；`client_callback_executor_threads` 可覆盖 |
| 失败换 broker | `MQClientAPIImpl#onExceptionImpl:704-743` | `_on_send_exception` |

用户回调**永远不跑在读线程/超时扫描线程上**，这是那两个池存在的唯一理由。调用方
`send_async` 只入队就返回（实测 `callerBlockedMs=0`）。

三个入口级判据：

- 队列满 → `MQClientException("executor rejected")`（Java `RejectedExecutionException` 分支）；
- 排队已经把预算吃光 → `RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout")`，
  连请求都不会建（Java `:553-575` 的 `timeout > costTime` 闸门）；
- ASYNC 的 `timesTotal` 固定为 1（Java `:756`），重试次数读
  `retry_times_when_send_async_failed`（Java `:1057`）——早期实现里那个配置是个死字段。

重试的四个细节最容易漏：跨重试**复用同一个请求对象**（`SendMessageRequestHeaderV2` 不带
brokerName，所以换 broker 不必重建），但每次尝试**换一个 `opaque`**（复用会让两次尝试的应答
串台）；`select_one_message_queue(publish, last_broker, False)` 避开刚失败的那台；超时用的是
**共享的剩余预算**而不是重新给一份；`timeout <= 0` 立即停。

失败分类（`_classify_async_failure`）不是均匀的：**broker 明确回了错误码时原样交给回调、
不换 broker 重试**（`needRetry=false`），这和同步发送的 `retry_response_codes` 语义**不同**；
`RemotingSendRequestException`/`RemotingTimeoutException`/其它 `RemotingException` 各按 Java
`operationFail` 的措辞包一层 `MQClientException`（`send request failed` /
`wait response timeout, cost=N` / `unknown reason`）并重试，只有
`RemotingTooMuchRequestException` 不重试；而外层 catch（同步抛出）走的是 Java 的
raw-exception + `needRetry=true` 分支，**不包装**。`request()` 内部就是这条 ASYNC 路径
+ 等 latch，所以它同样受上述全部语义约束。

地址解析也照 `sendKernelImpl:919-924` 走两步：先查发布地址表，查不到**按 topic 刷一次路由**
再查。这一步是定点发送（调用方直接给 `mq`）唯一的路由来源 —— 它不会在 `sendDefaultImpl`
里取发布信息，少了这一步第一次定点发送必然带着空地址去连；刷完还没有就回调
`MQClientException("The broker[x] not exist")`（Java `:1100`）。ASYNC 分支另有自己的一道总闸
（`:1043-1046`）：钩子、压缩、建请求的耗时都算进预算，被吃光时不再发起请求，回调拿
`RemotingTooMuchRequestException("sendKernelImpl call timeout")` 且不重试。

刻意保留的三处偏差：未 `start()` 时 `send_async` 同步抛（Java 从回调给）；批量消息在 sender
池里复用同步批量内核；`shutdown()` 不等在途异步任务
（Java `:314` 也只调 `shutdown()` 不 `awaitTermination`）——实测（`verify_async_send_live.py`
A6）36 笔全部拿到终态回调但整轮以 `client already shutdown` 报错、broker 上一条都没落，
所以"发完立刻 shutdown"会丢消息，与 Java 一致。Java 默认关闭的**信号量背压**已经按
`executeAsyncMessageSend:635-682` 移植完（`enable_backpressure_for_async_mode` +
`FairSemaphore` 两个维度，守卫见 `verify_backpressure_live.py`）。

## License

Apache-2.0，与上游 RocketMQ 保持一致。
