# rocketmq-client-remoting (Python)

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 Python 实现，迁移自 Java 的
`org.apache.rocketmq.client` + `org.apache.rocketmq.remoting` + `org.apache.rocketmq.tools`，
目标是让 Python 进程可以直接用 **JSON / RocketMQ 二进制** 两种序列化方式与
NameServer、Broker 通信。

对齐的 Java 源码位于 `rocketmq-client/java`（模块 `client` / `remoting` / `common` / `tools`）。

## 安装与测试

```bash
pip install -e .
pytest -q                     # 701 条单元/协议测试（697 passed + 4 skip，skip 为可选依赖相关）
python -m rocketmq selfcheck  # 协议编解码回环自检（7 项）
```

需要更强的回归守卫时，把环境变量指向 Java 协议的源码目录，测试会逐条比对常量取值：

```bash
ROCKETMQ_JAVA_SRC=<...>/remoting/src/main/java/org/apache/rocketmq/remoting/protocol pytest -q
```

### 真实集群联调（无 mock，需先起 nameServer(9876) + broker(10911)）

```bash
python verify_message_types.py    # 7 类消息能力（异步/顺序/Tag/属性/延迟/Key/事务）
python verify_admin_live.py       # 管理端全链路 + sendMessageBack 重投（52 PASS/0 FAIL/1 SKIP）
python verify_compression_live.py selftest   # 自动压缩自产自销 + broker 侧压缩体校验
python verify_compression_live.py send|recv <topic> <group> <size>   # 与 Java 探针跨客户端互通
python verify_trace_live.py       # 消息轨迹全链路（17 PASS/0 FAIL，需 broker traceTopicEnable=true）
python verify_hook_live.py        # CheckForbidden/FilterMessage 钩子（13 PASS/0 FAIL）
python verify_validators_live.py  # 名字校验（24 PASS/0 FAIL）：非法 topic/group 本地快拒、合法名字照常收发、往返对照腿
python verify_recall_live.py      # 定时消息撤回 recallMessage(370)（15 PASS/0 FAIL，脚本会打开并在退出时还原 broker 的 recallMessageEnable）
python verify_unit_config_live.py # unitName/unitMode/stream（13 PASS/0 FAIL）：clientId 后缀、broker 侧 topic 的 UNIT/UNIT_SUB 位、每笔请求的 ReqT
python verify_lite_pull_live.py   # lite pull 全链路（32 PASS/0 FAIL）：rebalance/收 12 条/commit/assign+seek/tag/时间戳起点/pause+resume + 队列分配策略（默认 AVG、null 被 start() 拒、AVG_BY_CIRCLE 两实例交叉、CONFIG 两半不重叠、CONSISTENT_HASH 用真实 clientId 建环并收敛到离线预测、MACHINE_ROOM_NEARBY 单机房透传内层策略且 resolver 被真实 brokerName/clientId 问过、MACHINE_ROOM 白名单不匹配 broker-a 时安静饿死）
```

其它真机脚本：`verify_acl_live.py`（需开 ACL 的集群）/ `verify_pull_live.py` /
`verify_rr_live.py` / `verify_latency_live.py` / `verify_pop_live.py` /
`verify_pop_consumer_live.py` / `verify_redelivery_live.py`。

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

## License

Apache-2.0，与上游 RocketMQ 保持一致。
