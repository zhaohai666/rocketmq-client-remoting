# rocketmq-client-remoting · 长期项目笔记

RocketMQ remoting 协议层用 **Python / C++ / .NET(C#)** 各实现一遍，参照 Java 5.x
（`/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq`）。三侧均已对真实 5.5.1 集群验证。
**详细坑位清单在技能 `~/.workbuddy/skills/rocketmq-cpp-build-verify/SKILL.md`** —— 本文件只留
跨语言的关键约定与验证入口，避免重复。

## 目录
- `python/rocketmq/` 参考实现 · `cpp/` C++（22 个 .cpp，含 Admin / 压缩）· `dotnet/` .NET 10（零 NuGet）。
- 三侧已对齐 Java：**两阶段事务**（半消息 → END_TRANSACTION → broker 回查）、消费侧回投 / 位点持久化 /
  顺序锁 / 广播 / 流控、**真实 rebalance + 队列撤销收尾 + 重投 topic 还原 + 优雅注销 + 命名空间**、
  压缩（zlib 跨客户端互通）。

## 验证入口（改完必跑）
- 技能 `rocketmq-cpp-build-verify/SKILL.md`：编译命令、8 个 ctest 用例与断言数、真机联调工具、全部坑位。
- Python `pytest` 147 passed / 4 skipped；C++ ctest **8/8**（约 515 项断言）；.NET xunit 46/46。
- 真机（**起集群 + 等端口 + 跑测试 + kill 必须在同一条 Bash 命令里**，前台返回会回收后台 JVM）：
  `run_redelivery_live.sh cpp|python|dotnet|all`、`run_admin_live{,_cpp}.sh`、`run_compression_live.sh`、
  `run_logging_live.sh`、`run_transaction_live.sh`、`run_dotnet_live.sh`。

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

## 两条"静默数据损坏"级的坑（最贵，别顺手优化）
- **压缩两层语义缺一不可**：未支持的压缩类型（如 SNAPPY）`decompress` **抛异常**，且
  `decode_message` 捕获后**返回 None / false**（丢弃消息）——而不是把压缩流当正文交出去。
- **Java `UtilAll.crc32` 返回 `(int)(value & 0x7FFFFFFF)`，砍掉最高位**，与标准 CRC-32 差正好 2^31。
  跨语言对 CRC 只看各自的 `match` 字段，别比数字。

## 与 Java 客户端仍存在的差距（P1 收官后，2026-09-16 盘点）
已对齐 Java：两阶段事务、真实 rebalance + 队列撤销收尾 + 分配时解析初始位点、回投 / 位点持久化 /
顺序锁 / 广播 / 流控、`resetRetryAndNamespace`、优雅注销、`UNREGISTER_CLIENT`、路由 30s 刷新、压缩。
仍缺（grep 验证：`namespace_util`/`AclRPCHook` 仅 Python 有，其余三侧 grep 命中只在 admin 代码）：

| 能力 | 严重度 | Py | C++ | .NET | 说明 |
| --- | --- | --- | --- | --- | --- |
| 命名空间包裹 (withNamespace) | P1 正确性 | ✅ | ❌ | ❌ | C++/.NET 生产/消费不做 NS 包裹 → NS 集群 topic 解析错 |
| ACL 鉴权 | P1 连通性 | ✅ | ❌ | ❌ | 仅 Python 有 AclRPCHook+SessionCredentials；C++/.NET 连不开 ACL 集群会鉴权失败 |
| 主动拉取 PullConsumer | P2 | ✅ | ❌ | ❌ | 仅 Python 有完整实现 |
| 故障规避 sendLatencyFaultEnable | P2 | ❌ | ❌ | ❌ | 三侧均无 broker 故障避让 |
| POP 模式 (5.x 轻量消费) | P2 | ❌ | ❌ | ❌ | 仅常量，无管道 |
| Request-Reply (5.x) | P2 | ❌ | ❌ | ❌ | 仅常量，无管道 |
| 消息轨迹 Trace/Hook | P3 | ❌ | ❌ | ❌ | 三侧均无 |
| TLS | P3 | ❌ | ❌ | ❌ | 仅明文 TCP |
| 动态 name server (address server) | P3 | ❌ | ❌ | ❌ | 仅静态 namesrv 列表 |
| 客户端统计 / metrics | P3 | ❌ | ❌ | ❌ | 无 |
| 其它 broker 主动请求 | P3 | 部分 | 部分 | 部分 | 仅接 CHECK_TRANSACTION_STATE(39)；GET_CONSUMER_RUNNING_INFO(307) 等未接（admin 有 VIEW_MESSAGE 部分） |
| 细粒度流控 / 线程弹性 | P3 | ❌ | ❌ | ❌ | 仅 pullThresholdForQueue；消费线程 min=max 固定 |

注：心跳 V2 指纹刻意留 0 走 V1（有意设计，非缺口）。建议下一步优先补 **C++/.NET 的 namespace + ACL**
（直接照搬 Python 的 `namespace_util.py` 与 `rpchook.py`），这是生产集群真会踩的两个硬伤。
