# rocketmq-client-remoting · 长期项目笔记

本仓库把 RocketMQ remoting 协议层用 **Python** 与 **C++** 各实现一遍，参照 Java
（`/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq`，5.x）。Python 实现已对真实
5.5.1 集群验证；C++ 侧以 Python + Java 双方为参照移植。

## 目录
- `python/rocketmq/`：参考实现（common / remoting / client），已真实集群验证。
- `cpp/`：C++ 实现，共 **22** 个 .cpp。已覆盖**协议层 + 传输层 + 路由/心跳 + 客户端层**
  （`MQClientInstance` / `DefaultMQProducer` / `DefaultMQPushConsumer` / **`DefaultMQAdminExt`**）
  + **压缩（zlib，走 `find_package(ZLIB)`）**，并已对真实 5.5.1 集群跑通 7 类消息收发（12/12）、
  管理端全链路（C++ 47 PASS/0 FAIL/1 SKIP）、压缩跨客户端矩阵（6 组全 PASS）。
  **仅 Windows 分支未实测。**

## 压缩（两侧均已实现，2026-09-14 真机跨客户端验过）
- **阈值**：`compressMsgBodyOverHowmuch` 默认 **4096**；`MessageBatch` **不压缩**
  （对齐 Java `DefaultMQProducerImpl.sendKernelImpl` → `tryToCompressMessage`）。
  压缩后置 `COMPRESSED_FLAG | 类型位`。生产端**循环外只压一次**（Java 在循环内压会 `zlib(zlib(x))`）。
- **消费端**：`decode_message` / `decodeMessage` 检测到 `COMPRESSED_FLAG` 即解压并**清掉该标志位**
  （保留 bit8~10 类型位），对齐 Java `MessageDecoder` L520-523。
- **类型位向后兼容**：`0` 与 `3` **都映射到 ZLIB**（Java `CompressionType.findByValue`）。
  老客户端产的压缩消息类型位就是 0。
- **两层语义，缺一不可**（Java 5.x 的形状，别"顺手优化"）：
  1. `CompressorFactory::decompress` / Python `_decompress` 对**未支持类型**（如 SNAPPY=4）
     **抛异常**（对齐 Java `CompressorFactory.getCompressor` 抛 `IllegalArgumentException`）；
  2. `decodeMessage` 捕获后**返回 false** / Python `decode_message` 返回 `None`
     （Java `MessageDecoder.decode` 的 `catch { } return null;`，**Java 侧同样不打日志**）。
     即"消息被丢弃"，而不是"把压缩流当正文交出去"。
  曾两次在 Python 侧踩同源坑：类型位 0 未映射（返回压缩流+标志被清）、
  未支持类型原样透传（同样返回压缩流+标志被清）。**静默数据损坏，事后无法识别。**
- **跨客户端矩阵**（`/tmp/run_compression_live.sh`）：C++/Python 自产自销 + 与**真实 Java 客户端**
  双向互通。载荷确定性（重复固定行再截断），与 Java 探针 `CompressProbe` 同算法。
- ⚠ **Java `UtilAll.crc32` 返回 `(int)(value & 0x7FFFFFFF)`，砍掉最高位**，与标准 CRC-32
  差正好 2^31（Java 1785582993 ↔ 标准 3933066641）。跨语言对 CRC 看 `match` 字段，别比数字。

## 管理端（Admin）关键行为（真机踩出来的，勿凭直觉）
- **`GET_BROKER_CONFIG` 的 body 是 properties 文本，不是 JSON/KVTable**。用
  `MixAll::string2Properties` / `string2_properties`，语义对齐 `java.util.Properties.load`
  （`#`/`!` 注释、行尾 `\` 续行、**空白也是分隔符**、值只去前导空白）。
- **`CreateTopicRequestHeader` 必须带 `topicFilterType`**（+ `attributes` 传 `""` 而非 null），
  否则 broker `checkFields()` 抛 `topicFilterType = [null] value invalid`。
- **`ResetOffsetBody.offsetTable` 是 `Map<MessageQueue, Long>`**（不是嵌套 map）。
- **fastjson2 会产出非法 JSON**（map 的对象 key 内联、数字 key 不加引号、NaN/Infinity、尾逗号），
  两侧都需**自写容错解析器**（C++ `json.cpp` 的 `parseKey`/`parseObject`/`parseArray`，
  Python `serialize.fastjson_loads`）。改解析器务必保留宽容逻辑。
- `GET_ALL_SUBSCRIPTIONGROUP_CONFIG` 是**分页**接口；老 broker 无 `totalGroupNum` → 一轮结束。
- KV 配置类请求打到 **NameServer**，PUT/DELETE 要广播到每一个 NameServer。
- **uniqKey 查询需 broker 开 RocksDB 索引**；默认文件索引查不到是 **broker 配置差异，不是 bug**。
- `QueryMessageRequestHeader` 必须带 `indexType`（`K`/`U`/`T`）；uniqKey 模式在 request extFields
  加 `_UNIQUE_KEY_QUERY="true"`（**值是字符串，不是整数 1**）。
- **测 `sendMessageBack` 必须先消费再重投**；且 `delayLevel=0` 会被 broker 改写成 `3+reconsumeTimes`
  （约 10s 延迟后才进 `%RETRY%`），校验要**轮询**。

## 硬性约定
1. **字段名以 Java 为准**。broker 用 fastjson2 按 Java 属性名反序列化，字段名错一个就静默丢字段。
   已确认要点（5.x，勿再凭记忆写）：
   - `SubscriptionData`：`filterClassSource` 带 `@JSONField(serialize=false)`，**不序列化**。
   - `ConsumerData`：**没有** `consumeTimestamp` / `maxReconsumeTimes`（那是 4.x）。
   - `HeartbeatData`：有 `heartbeatFingerprint`(int) 与 `withoutSub`(bool)；
     Java 字段 `isWithoutSub` 的 fastjson2 名是 **`withoutSub`**。
   - fastjson2 **键按字母序**；`BrokerData.brokerAddrs` 输出**裸数字键** `{0:"..."}`（解析时要容忍）。
2. **确认 Java 行为要靠探针，不要猜**：用编译好的 class + fastjson2 写个 main
   直接 `JSON.toJSONString(obj)` 打印。classpath 见 `/tmp/rmq_cp.txt`，
   `JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.0.20+8/Contents/Home`。
3. **心跳指纹留 0**：`heartbeatFingerprint == 0` 让 broker 走 V1 注册路径（用完整
   subscriptionDataSet 注册），最稳妥；非 0 才进 heartBeatV2 优化。故无需实现
   `computeHeartbeatFingerprint()`（那依赖 fastjson2 字段序，很脆）。
4. **opaque 不是"0 表示未设置"**：`opaqueCounter` 从 0 起算，`createRequestCommand()` 第一个请求
   opaque 合法值就是 0。传输层只在**与在途请求真的冲突**时才重分配 opaque。
5. C++ 零 warning 是目标（`-Wall -Wextra`，未开 `-Werror`）；改完应保持干净重建零 warning。
6. **C++ 日志默认 INFO**（`include/rocketmq/common/logging.h`，落
   `$HOME/logs/rocketmqlogs/rocketmq_cpp_client.log`）。良性长轮询超时走 DEBUG，默认被抑制 ——
   运行日志必须保持 `ERROR=0`。新增"预期内"的失败日志时别用 ERROR/WARN。
7. **`TopicPublishInfo` 不可拷贝、必须用 `shared_ptr` 取用**：它的轮询游标是**跨调用共享状态**，
   按值返回会让每次发送都从 0 号队列重来（轮询失效）。

## 已知 Python 参考实现的缺陷（C++ 侧无此问题）
- `SubscriptionData` 有 `__eq__` 无 `__hash__` → **不可哈希**，`subscription_data_set.add()` 抛 TypeError。
- `SubscriptionData.to_dict()` 用 `__dict__` → **snake_case** 键，broker 认不出；
  `tagsSet/codeSet` 是 `set`，`json.dumps` 不可直接序列化。
- `ConsumerData` 仍按 4.x 带 `consumeTimestamp` / `maxReconsumeTimes`。
- 根因：`send_heartbeat()` 在 Python 客户端里**从未被调用** → 心跳/订阅路径是未经测试的死代码
  （Python 的消费者注册实际只靠 pull 请求里的 `subscription` 属性）。
- 若将来给 Python 补心跳能力，需先修上述 1~3 点。

## 验证入口
- 技能：`~/.workbuddy/skills/rocketmq-cpp-build-verify/SKILL.md`（编译命令、**7** 个 ctest 用例与
  断言数、三个真机联调工具、手动互操作工具、全部坑位清单）。改 C++/Python 后照它跑即可。
- Python 单测 **142 passed / 4 skipped**；C++ ctest **7/7 全绿**，合计 **476** 项断言
  （codec 65 / java_alignment 32[带 Java 源码 38] / route_heartbeat 92 / transport 32 /
  compression 33 / admin 152 / interop 70）。
- **真实集群联调（不进 ctest，一条命令起停集群）**：
  - `bash /tmp/run_admin_live.sh` → Python Admin（52 PASS/0 FAIL/1 SKIP）
  - `bash /tmp/run_admin_live_cpp.sh` → C++ Admin（47 PASS/0 FAIL/1 SKIP）
  - `bash /tmp/run_compression_live.sh` → 压缩跨客户端矩阵（C++/Python 自测 + 与 Java 双向互通）
  - `examples/rmq_live_message_types 127.0.0.1:9876` → 7 类消息能力（12/12）
- `interop` 的 WARN 不是失败，但**出现新 WARN 要读**——它是 Python 侧偏差的显式记录
  （当前 WARN=2：`SubscriptionData` 不可哈希、`to_dict()` 出 snake_case 键）。
  Python 参考实现仍缺心跳能力（`send_heartbeat()` 是死代码），C++ 侧已补上并实测通过。
- ⚠ **沙箱必须把"起集群 + 等端口 + 跑测试 + kill"放在同一条 Bash 命令里**，
  前台命令返回后后台 JVM 会被回收。
