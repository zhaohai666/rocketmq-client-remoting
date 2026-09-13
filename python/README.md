# rocketmq-client-remoting (Python)

Apache RocketMQ 4.x 经典 remoting 协议的 Python 实现，迁移自 Java 的
`org.apache.rocketmq.client` + `org.apache.rocketmq.remoting`，
目标是让 Python 进程可以直接用 **JSON / RocketMQ 二进制** 两种序列化方式与
NameServer、Broker 通信。

对齐的 Java 源码位于 `rocketmq-client/java`（模块 `client` / `remoting` / `common`）。

## 安装与测试

```bash
pip install -e .
pytest -q                     # 122 条单元/协议测试
python -m rocketmq selfcheck  # 协议编解码回环自检（7 项）
```

需要更强的回归守卫时，把环境变量指向 Java 协议的源码目录，测试会逐条比对常量取值：

```bash
ROCKETMQ_JAVA_SRC=<...>/remoting/src/main/java/org/apache/rocketmq/remoting/protocol pytest -q
```

## 目录结构

```
rocketmq/
├── common/                客户端共用的消息模型与常量
│   ├── message.py             Message / MessageExt / MessageBatch / MessageQueue
│   ├── message_decoder.py     17 段存储格式 + 6 段批量格式的编解码
│   ├── message_const.py       MessageConst 属性键
│   ├── sysflag.py             MessageSysFlag / PullSysFlag / PermName
│   ├── mix_all.py / util_all.py
│   └── subscription_data.py / topic_config.py / message_accessor.py
├── remoting/              传输层（不依赖 netty，纯 socket + 线程）
│   ├── client.py              RemotingClient：同步 / 异步 / oneway
│   ├── exception.py
│   └── protocol/
│       ├── remoting_command.py  帧编解码（totalLen|headerLen+type|header|body）
│       ├── serialize.py         RemotingSerializable(JSON) / RocketMQSerializable(二进制)
│       ├── codes.py             RequestCode / ResponseCode / LanguageCode / SerializeType
│       ├── headers.py           CommandCustomHeader 家族（含 V2 短字段名 a..n）
│       ├── body.py / route.py / heartbeat.py
├── client/                面向用户的 API
│   ├── producer.py / consumer.py / admin.py / mq_client.py
│   └── send_result.py / consumer_result.py / exception.py
├── __main__.py            命令行入口（selfcheck）
└── selfcheck.py           无集群环境下的协议自检
```

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

## License

Apache-2.0，与上游 RocketMQ 保持一致。
