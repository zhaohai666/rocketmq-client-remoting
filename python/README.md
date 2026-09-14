# rocketmq-client-remoting (Python)

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 Python 实现，迁移自 Java 的
`org.apache.rocketmq.client` + `org.apache.rocketmq.remoting` + `org.apache.rocketmq.tools`，
目标是让 Python 进程可以直接用 **JSON / RocketMQ 二进制** 两种序列化方式与
NameServer、Broker 通信。

对齐的 Java 源码位于 `rocketmq-client/java`（模块 `client` / `remoting` / `common` / `tools`）。

## 安装与测试

```bash
pip install -e .
pytest -q                     # 142 条单元/协议测试（4 skip 为可选依赖相关）
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
```

`verify_compression_live.py` 的载荷是确定性的，与 Java 探针
`/tmp/probe_admin/CompressProbe.java` 同算法，因此可直接验证"Java 产的压缩消息我们能否解开"
以及反向。注意 Java `UtilAll.crc32` 会 `& 0x7FFFFFFF`，与标准 CRC-32 **差正好 2^31**，
对结果时看各自的 `match` 字段而不是 CRC 数字。

## 目录结构

```
rocketmq/
├── common/                客户端共用的消息模型与常量
│   ├── message.py             Message / MessageExt / MessageBatch / MessageQueue
│   ├── message_decoder.py     17 段存储格式 + 6 段批量格式的编解码（含 zlib 解压）
│   ├── message_const.py       MessageConst 属性键（含 INDEX_KEY/UNIQUE/TAG_TYPE）
│   ├── sysflag.py             MessageSysFlag / PullSysFlag / PermName
│   ├── mix_all.py / util_all.py
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
│   └── send_result.py / consumer_result.py / exception.py
├── __main__.py            命令行入口（selfcheck）
└── selfcheck.py           无集群环境下的协议自检
```

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
