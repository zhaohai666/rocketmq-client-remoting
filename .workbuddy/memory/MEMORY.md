# rocketmq-client-remoting · 长期项目笔记

RocketMQ remoting 协议层用 **Python / C++ / .NET(C#)** 各实现一遍，参照 Java 5.x
（`/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq`）。三侧均已对真实 5.5.1 集群验证。
**详细坑位清单 / 编译命令 / 断言数 / 真机 harness 全在技能
`~/.workbuddy/skills/rocketmq-cpp-build-verify/SKILL.md`** —— 本文件只留跨语言硬约定与验证入口。

## 目录 / 状态
- `python/rocketmq/` 参考实现 · `cpp/` C++ · `dotnet/` .NET 10（零 NuGet）。
- 三侧已对齐 Java：两阶段事务、消费侧回投 / 位点持久化 / 顺序锁 / 广播 / 流控、
  真实 rebalance + 队列撤销收尾 + 重投 topic 还原 + 优雅注销 + 命名空间 + ACL 鉴权 +
  主动拉取 PullConsumer + Request-Reply + 故障规避 + 压缩(zlib 跨语言) +
  **POP 协议管道 + POP 消费循环** + **消息轨迹 Trace/Hook**。
- 残留缺口全部 P3：TLS、动态 name server(TopAddressing)、307 运行信息、客户端 metrics、
  消费线程弹性、OpenTracing 版轨迹钩子。心跳 V2 指纹刻意留 0 走 V1（有意设计，非缺口）。

## 验证入口（改完必跑）
- Python `pytest`（当前 319 passed / 4 skipped，2 error 是沙箱 tmpdir 权限噪音）；C++ ctest
  **14/14**（约 849 项断言）、零 warning；.NET xunit（当前 178/178）、零 warning。
- 真机（**起集群 + 等端口 + 跑测试 + kill 必须在同一条 Bash 命令里**，前台返回会回收后台 JVM）：
  `run_redelivery_live.sh cpp|python|dotnet|all`、`run_admin_live{,_cpp}.sh`、`run_compression_live.sh`、
  `run_logging_live.sh`、`run_transaction_live.sh`、`run_dotnet_live.sh`、`run_acl_live.sh`（认证集群）、
  `run_pull_live.sh`、`run_rr_live.sh`、`run_latency_live.sh`、`run_pop_live.sh`、
  `run_pop_consumer_live.sh`、**`run_trace_live.sh`**（需 broker `traceTopicEnable=true`，
  否则 RMQ_SYS_TRACE_TOPIC 不预建）。

## 跨语言硬性约定
1. **字段名以 Java 为准**：broker 用 fastjson2 按 **Java 属性名**反序列化，错一个就**静默丢字段**。
   例：`BrokerData.brokerAddrs` 出裸数字键 `{0:"..."}`；`HeartbeatData` 的 fastjson2 名是
   **`withoutSub`**；`SubscriptionData.filterClassSource` 不序列化；`ConsumerData` **没有**
   `consumeTimestamp`/`maxReconsumeTimes`。确认 Java 行为要**写探针**（`JSON.toJSONString`），
   别凭记忆（classpath `/tmp/rmq_cp.txt`）。
2. **心跳指纹留 0** → broker 走 V1 注册路径（用完整 subscriptionDataSet），最稳。
3. **opaque 的 0 是合法值**（`opaqueCounter` 从 0 起算），不能当"未设置"哨兵。
4. **`TopicPublishInfo` 必须引用共享**：轮询游标是跨调用状态，按值返回会让每次发送都从 0 号队列重来。
5. **消费者不做默认 topic 兜底**（TBW102 只给生产者）。联调**先建 topic 再起消费者**，否则不分配队列。
6. **remoting 回调（NOTIFY_CONSUMER_IDS_CHANGED 等）在读线程上跑：只能置标志，绝不能同步发请求**
   （自死锁，且连带卡住该连接所有响应）。
7. 零 warning 是目标。三侧日志文件名必须**不同名**，运行日志保持 `ERROR=0`，良性超时走 DEBUG。
8. **测消费必须"先起消费者、再发消息"**：`CONSUME_FROM_LAST_OFFSET` 首次消费无位点时初始位点 =
   当时的 maxOffset；且 consumequeue 异步分发，刚发完查 maxOffset 可能读到 0 → 三语言结果不一致，
   极易误判成某语言有 bug。
9. **ACL 签名**：content = extFields 按 key 字典序、**只拼 value**、跳过 `Signature`，再拼 body；
   `Base64(HMAC-SHA1(secretKey, content))`；`AccessKey`/`SecurityToken` 必须在算签名**之前**写入
   extFields；钩子在 **encode 之前**调用。对拍向量见 `python/verify_acl_java_parity.py`。
10. **`pull()` 是短轮询**（`buildSysFlag(false, block, true, false)` → `pull()` 的 suspend=false，
    只有 `pullBlockIfNotFound` 才 suspend=true；两者都不带 commitOffset 位）。写 suspend=true 会必现
    5s 超时。回归守卫 `python/tests/test_pull_consumer.py` 用 inspect 断言。
11. **拉模式回投要复用"访问过该 topic"的那个 consumer**（`send_message_back` 靠路由表反查 broker）。
12. **POP 三条硬性事实**：单 broker 5.5.1 原生支持 POP（无需 proxy/开关，靠 `timerWheelEnable`）；
    **`POP_CK` 必须客户端反构**（broker 只在 retry-topic 路径写），8 段空格分隔 + `1ST_POP_TIME`，
    没有它无法 ACK；**`bornTime` 必须是当前毫秒**，否则 `POLLING_TIMEOUT(210)`；**ACK 的 offset 是
    consumeQueue offset**。求 index 要去**本批该队列的 queueOffset 排序表**找下标再查 msgOffsetInfo
    （不能直接 `IndexOf(自身 queueOffset)`）。`order`/`suspend` 总是出现在报文里。
13. **POP 消费侧**：`ackIndex` 默认必须是 `size-1`（本项目 push 回投默认 -1，照搬会一条都不 ack）；
    **"验证不重复投递"的观察窗口必须 > `popInvisibleTime`**，否则假绿；invisible 档位表单位是**秒**，
    `CHANGE_MESSAGE_INVISIBLETIME` 要**毫秒**；`checkNeedAckOrDelay` 的 `delayLevel=-1` 三侧钳到首档。
    调试：`SimpleMessageListener` 回调是 `fn(msgs)` 单参，两参会抛 TypeError 且被 POP 循环按
    RECONSUME_LATER **吞掉**（表现为一条都收不到）→ 开 `ROCKETMQ_CLIENT_LOG_LEVEL=DEBUG`。
14. **轨迹（Trace）三条**：① **消费侧 `msg_id` 是 offset 基 ID**（Java `MessageDecoder:557-561`
    先 setMsgId 再 setOffsetMsgId，同值）→ 它对齐 `SendResult.offsetMsgId`，**不是**
    `SendResult.msgId`(UNIQ_KEY)；Pub 轨迹里才是 msgId=UNIQ_KEY + offsetMsgId=broker ID。
    ② **无 keys 消息的 SubBefore 只有 7 段**（Java `String.split` 丢末尾空串 → `line[7]` AIOOBE，
    上游真实缺陷）：三侧缺段当空串，且**单条记录解码失败只跳过该条**（Java 是一条坏记录毁掉整条
    轨迹消息）。轨迹文本每条以 `\x02` 结尾 → 记录数 == `count("\x02")`，是"丢记录"探测器。
    ③ 真机脚本：topic/组名带时间戳、预热消息按 body 过滤、broker 需 `traceTopicEnable=true`。
    细节与 S1–S17 场景见技能；对拍向量 `python/tests/test_trace.py`（探针 `/tmp/TraceParity.java`）。

## 两条"静默数据损坏"级的坑（最贵，别顺手优化）
- **压缩两层语义缺一不可**：未支持的压缩类型 `decompress` **抛异常**，且 `decode_message` 捕获后
  **返回 None / false**（丢弃消息），而不是把压缩流当正文交出去。
- **Java `UtilAll.crc32` 返回 `(int)(value & 0x7FFFFFFF)`，砍掉最高位**，与标准 CRC-32 差 2^31。
  跨语言对 CRC 只看各自的 `match` 字段，别比数字。
