# rocketmq-client-remoting · 长期项目笔记

RocketMQ remoting 协议层用 **Python / C++ / .NET(C#)** 各实现一遍，参照 Java 5.x
（`/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq`）。三侧均已对真实 5.5.1 集群验证。
**详细坑位清单在技能 `~/.workbuddy/skills/rocketmq-cpp-build-verify/SKILL.md`** —— 本文件只留
跨语言的关键约定与验证入口，避免重复。

## 目录
- `python/rocketmq/` 参考实现 · `cpp/` C++（24 个 .cpp，含 Admin / 压缩 / PullConsumer）·
  `dotnet/` .NET 10（零 NuGet）。
- 三侧已对齐 Java：**两阶段事务**（半消息 → END_TRANSACTION → broker 回查）、消费侧回投 / 位点持久化 /
  顺序锁 / 广播 / 流控、**真实 rebalance + 队列撤销收尾 + 重投 topic 还原 + 优雅注销 + 命名空间 +
  ACL 鉴权 + 主动拉取 PullConsumer**、压缩（zlib 跨客户端互通）。

## 验证入口（改完必跑）
- 技能 `rocketmq-cpp-build-verify/SKILL.md`：编译命令、9 个 ctest 用例与断言数、真机联调工具、全部坑位。
- Python `pytest` 158 passed / 4 skipped；C++ ctest **9/9**（约 552 项断言）；.NET xunit **55/55**。
- 真机（**起集群 + 等端口 + 跑测试 + kill 必须在同一条 Bash 命令里**，前台返回会回收后台 JVM）：
  `run_redelivery_live.sh cpp|python|dotnet|all`、`run_admin_live{,_cpp}.sh`、`run_compression_live.sh`、
  `run_logging_live.sh`、`run_transaction_live.sh`、`run_dotnet_live.sh`、**`run_acl_live.sh`**（开认证的集群）、
  **`run_pull_live.sh`**（主动拉取消费者 S1–S7）。

## 跨语言硬性约定
1. **字段名以 Java 为准**：broker 用 fastjson2 按 **Java 属性名**反序列化，错一个就**静默丢字段**。
   fastjson2 键按字母序；`BrokerData.brokerAddrs` 出裸数字键 `{0:"..."}`；`HeartbeatData.isWithoutSub`
   的 fastjson2 名是 **`withoutSub`**；`SubscriptionData.filterClassSource` 不序列化；`ConsumerData`
   **没有** `consumeTimestamp` / `maxReconsumeTimes`（4.x 遗留）。确认 Java 行为要**写探针**
   （`JSON.toJSONString(obj)` 打印），不要凭记忆（classpath `/tmp/rmq_cp.txt`）。
2. **心跳指纹留 0** → broker 走 V1 注册路径（用完整 subscriptionDataSet），最稳妥，不必实现
   `computeHeartbeatFingerprint()`（依赖 fastjson2 字段序，很脆）。
3. **opaque 的 0 是合法值**（`opaqueCounter` 从 0 起算），不能当"未设置"哨兵。
4. **`TopicPublishInfo` 必须 `shared_ptr` / 引用共享**：轮询游标是**跨调用**状态，按值返回会让每次发送
   都从 0 号队列重来（轮询失效）。
5. **消费者不做默认 topic 兜底**（TBW102 只给**生产者**用）。联调脚本必须**先建 topic 再起消费者**，
   否则拿不到路由 → 不分配队列 → 不消费（真机 12 项全红踩过，见技能「消费」小节）。
6. **remoting 回调（NOTIFY_CONSUMER_IDS_CHANGED 等）在读线程上跑：只能置标志，绝不能同步发请求**。
   读线程阻塞在自己发起的 `invokeSync` 上 = 自死锁（实测 5s 超时，且连带卡住该连接**所有**响应）。
7. 零 warning 是目标（C++ `-Wall -Wextra`；.NET `TreatWarningsAsErrors`）。日志：三侧文件名必须
   **不同名**（轮转策略不同，同文件会互相插行），运行日志保持 `ERROR=0`，良性超时走 DEBUG。
8. **测消费必须"先起消费者、再发消息"**。Java 默认 `CONSUME_FROM_LAST_OFFSET`：新消费组首次消费且
   无已提交位点时，初始位点 = 该队列**当时的** maxOffset（`RebalancePushImpl.java:174-190`），
   所以"先发后起消费者"本来就一条都收不到。更坑的是 broker 的 consumequeue **异步分发/刷盘**，
   刚发完立刻查 maxOffset 可能读到 0 —— 于是同一时序三语言结果不一致（实测 Python 收 0 条、
   C++/.NET 收 3 条），极易被误判成"某语言有 bug"。与约定 5（先建 topic 再起消费者）是一对。
9. **ACL 签名**（三侧已实现，改 remoting 层时勿破坏）：content = extFields 按 key 字典序、
   **只拼 value**、跳过 key == `"Signature"`，再拼 body；`Base64(HMAC-SHA1(secretKey, content))`；
   `AccessKey`/`SecurityToken` 必须在算签名**之前**写进 extFields（它们参与签名）。钩子必须在
   **encode 之前**调用。Python 曾有钩子但**从未被调用**、算法也是自造的（=三侧都连不开 ACL 集群）。
   对拍向量固化在 `python/verify_acl_java_parity.py`（Java 官方实现输出）。
10. **PullConsumer 的 sysFlag：`pull()` 是短轮询，不是长轮询**。Java
   `DefaultMQPullConsumerImpl.pullSyncImpl(:248)` 是 `buildSysFlag(false /*commitOffset*/,
   block /*suspend*/, true, false)` → `pull()` 的 block=false 所以 **suspend=false**，只有
   `pullBlockIfNotFound` 才 suspend=true；**两者都不带 commitOffset 位**（位点由调用方自己
   `update_consume_offset` 提交）。三侧（含 Python 参考实现）原先 pull() 也写了 suspend=true，
   真机必现 5s `RemotingTimeoutException`（broker 挂起到 brokerSuspendMaxTimeMillis=20s）。
   回归守卫：`python/tests/test_pull_consumer.py` 用 inspect 断言两个函数的 suspend 取值。
11. **拉模式回投要复用"访问过该 topic"的那个 consumer**：`send_message_back` 靠
   `broker_addr_of(msg.broker_name)` 反查路由表，新起的 consumer 路由表为空 →
   "broker xxx not found"（Java 同理，走 `findBrokerAddressInPublish` 读 brokerAddrTable）。
12. **POP 模式（5.x）的三条硬性事实**（真机实测 + Java 源码核对，2026-09-17）：
   - 单 broker 5.5.1 **原生支持 POP，无需 proxy、无需任何开关**；`BrokerController` 无条件注册
     POP_MESSAGE(200050) / ACK_MESSAGE(200051) / CHANGE_MESSAGE_INVISIBLETIME(**200053**，
     200052 是 PEEK)。唯一硬依赖 `timerWheelEnable`（默认 true）。三侧直连 broker:10911。
   - **`POP_CK` 必须由客户端反构**：普通 topic 直连 POP 时 broker **不在消息上写** `POP_CK`
     （只在 retry-topic 重编码路径写）。客户端要用响应头 `startOffsetInfo`/`msgOffsetInfo`
     按 `topic@queueId` 取 startOffset、按 queueOffset 求 index 取 msgQueueOffset，拼**8 段**
     （**空格**分隔）CK 串，再补 `1ST_POP_TIME`。**没有它就无法 ACK**。已带 POP_CK 的
     （retry 消息）**不能覆盖**。
   - **`bornTime` 必须填当前毫秒时间戳**：broker `now - bornTime - pollTime > 500` 即回
     `POLLING_TIMEOUT(210)`。**ACK 的 `offset` 是 consumeQueue offset（CK 第 8 段）**，
     不是 commitlog offset。不 ack 的消息在 invisibleTime 后被复活重投到
     `%RETRY%<group>_<topic>`（V1）。`queueId=-1` = 弹所有队列。
   - 移植坑：Java `String.split(" ")` **丢弃末尾空串**，Python/（C# 的 `Split(' ')`）会保留 ——
     段数校验依赖这个差异。`order`/`suspend` 在 Java 是非空 Boolean，**总是**出现在报文里。
   - 移植坑（**只有 .NET 踩到**）：反构 CK 求 index 要去**本批该队列的 queueOffset 排序表**
     （Java `sortMap`）里找下标，再拿这个下标去 `msgOffsetInfo` 取值；**不能**直接在
     `msgOffsetInfo` 列表里 `IndexOf(消息自身 queueOffset)` —— 两者不等时会一条都盖不上 CK
     （Java `MQClientAPIImpl:1202`）。回归守卫：`PopTests.StampPopCkIndexSelectsRightOffsetWithinQueue`
     故意让 msgOffsetInfo 值（100/101/102）≠ 消息 queueOffset（10/11/12）。
   - 三侧回归守卫：Python `tests/test_pop.py`（50 例，含 extFields 逐键断言）+ 真机
     `python/verify_pop_live.py` S1–S8；C++ `tests/test_pop.cpp`（ctest 12/12）+
     `examples/live_pop.cpp`；.NET `tests/.../PopTests.cs`（xunit 125/125）+
     `examples/.../LivePop.cs`（子命令 `rmq pop`）。harness `/tmp/run_pop_live.sh all` → 42/42。

## 两条"静默数据损坏"级的坑（最贵，别顺手优化）
- **压缩两层语义缺一不可**：未支持的压缩类型（如 SNAPPY）`decompress` **抛异常**，且
  `decode_message` 捕获后**返回 None / false**（丢弃消息）——而不是把压缩流当正文交出去。
- **Java `UtilAll.crc32` 返回 `(int)(value & 0x7FFFFFFF)`，砍掉最高位**，与标准 CRC-32 差正好 2^31。
  跨语言对 CRC 只看各自的 `match` 字段，别比数字。

## 与 Java 客户端仍存在的差距（POP 模式收官后，2026-09-17 盘点）
已对齐 Java：两阶段事务、真实 rebalance + 队列撤销收尾 + 分配时解析初始位点、回投 / 位点持久化 /
顺序锁 / 广播 / 流控、`resetRetryAndNamespace`、优雅注销、`UNREGISTER_CLIENT`、路由 30s 刷新、
压缩、**命名空间包裹（三侧）**、**ACL 鉴权（三侧）**、**主动拉取 PullConsumer（三侧）**、
**Request-Reply（三侧）**、**故障规避 sendLatencyFaultEnable（三侧）**、**POP 模式协议管道（三侧）**。
仍缺（下表全部为 P3）：

| 能力 | 严重度 | Py | C++ | .NET | 说明 |
| --- | --- | --- | --- | --- | --- |
| POP 消费侧循环（push consumer 走 POP） | P2(余) | ❌ | ❌ | ❌ | 只有协议管道；见下方说明 |
| 消息轨迹 Trace/Hook | P3 | ❌ | ❌ | ❌ | 三侧均无 |
| TLS | P3 | ❌ | ❌ | ❌ | 仅明文 TCP |
| 动态 name server (address server) | P3 | ❌ | ❌ | ❌ | 仅静态 namesrv 列表 |
| 客户端统计 / metrics | P3 | 部分 | ❌ | ❌ | Python 有 ClientMetrics 并接入 send |
| 其它 broker 主动请求 | P3 | 部分 | 部分 | 部分 | 仅接 CHECK_TRANSACTION_STATE(39)；GET_CONSUMER_RUNNING_INFO(307) 等未接（admin 有 VIEW_MESSAGE 部分） |
| 细粒度流控 / 线程弹性 | P3 | ❌ | ❌ | ❌ | 仅 pullThresholdForQueue；消费线程 min=max 固定 |

注：心跳 V2 指纹刻意留 0 走 V1（有意设计，非缺口）。命名空间 / ACL / PullConsumer /
Request-Reply / 故障规避 / **POP 协议管道** 均已三侧补齐并真机验证
（POP：`bash /tmp/run_pop_live.sh all` → 三语言各 14/14，单测 Py 50 例 / C++ ctest 12/12 /
.NET xunit 125/125，三侧零 warning）。**唯一残留的 P2 是"消费侧 POP 轻量消费循环"** ——
Java 的 push-consumer POP 走 **broker 侧分配** `QUERY_ASSIGNMENT(400)` +
`MessageQueueAssignment(mode=POP)`，不做客户端 rebalance；本项目若要补，改用客户端 rebalance
等价路径并**在文档写明这个有意差异**。再往后按 P3 推进：Trace / TLS / 动态 name server /
307 运行信息 / 消费线程弹性。
