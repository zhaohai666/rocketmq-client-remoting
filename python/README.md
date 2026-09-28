# rocketmq-client-remoting (Python)

Apache RocketMQ 经典 remoting 协议（对齐 5.x）的 Python 实现，迁移自 Java 的
`org.apache.rocketmq.client` + `org.apache.rocketmq.remoting` + `org.apache.rocketmq.tools`，
目标是让 Python 进程可以直接用 **JSON / RocketMQ 二进制** 两种序列化方式与
NameServer、Broker 通信。

对齐的 Java 源码位于 `rocketmq-client/java`（模块 `client` / `remoting` / `common` / `tools`）。

## 安装与测试

```bash
pip install -e .
pytest -q                     # 1145 条单元/协议测试（1141 passed + 4 skip，skip 为可选依赖相关）
python -m rocketmq selfcheck  # 协议编解码回环自检（7 项）
```

需要更强的回归守卫时，把环境变量指向 Java 协议的源码目录，测试会逐条比对常量取值：

```bash
ROCKETMQ_JAVA_SRC=<...>/remoting/src/main/java/org/apache/rocketmq/remoting/protocol pytest -q
```

### 真实集群联调（无 mock，需先起 nameServer(9876) + broker(10911)）

```bash
python verify_message_types.py    # 7 类消息能力，18 PASS/0 FAIL（异步 9 项：不阻塞返回、线程口径、并发、定点、失败只走回调）
python verify_request_reply_live.py # request-reply 全链路（22 PASS/0 FAIL）：325 落地、REPLY_TO_CLIENT=真实 clientId、超时/并发/普通消费不受影响；**错误码口径**：等应答超时带 Java 的 10006 `REQUEST_TIMEOUT_EXCEPTION`，`create_reply_message` 拿不到 broker 写的 `CLUSTER`/请求为 None 时带 10007 `CREATE_REPLY_MESSAGE_EXCEPTION`（文案逐字对 Java）
python verify_admin_live.py       # 管理端全链路 + sendMessageBack 重投（72 PASS/0 FAIL/1 SKIP）
python verify_compression_live.py selftest   # 自动压缩自产自销 + broker 侧压缩体校验 + 同一条 Message 复用两次（11 PASS/0 FAIL/1 SKIP，2026-09-28 实测；SKIP 是本 venv 没装 zstandard）：zlib/lz4 真机往返 storeSize 317/339 ≪ 8192，复用的两条都落成压缩体（storeSize=[322,322]）且读回逐字节等于原文 —— 不还原 prevBody 时第二条送出的会是压缩流且无 COMPRESSED_FLAG、消费端不解压，静默乱码
python verify_compression_live.py send|recv <topic> <group> <size>   # 与 Java 探针跨客户端互通
python verify_trace_live.py       # 消息轨迹全链路（17 PASS/0 FAIL，需 broker traceTopicEnable=true）
python verify_hook_live.py        # CheckForbidden/FilterMessage 钩子（13 PASS/0 FAIL）
python verify_validators_live.py  # 名字校验（28 PASS/0 FAIL）：非法 topic/group 本地快拒、合法名字照常收发、往返对照腿；S7 寻址故障定性（一个地址都没配 ⇒ 10004 NO_NAME_SERVER_EXCEPTION + Java 原文案「No name server address, please set it.」，<50ms 本地判定不空转重试预算；对照组地址恢复后同一条 topic 立刻 SEND_OK，说明判的是寻址不是 topic）
python verify_recall_live.py      # 定时消息撤回 recallMessage(370)（15 PASS/0 FAIL，脚本会打开并在退出时还原 broker 的 recallMessageEnable）
python verify_unit_config_live.py # unitName/unitMode/stream（13 PASS/0 FAIL）：clientId 后缀、broker 侧 topic 的 UNIT/UNIT_SUB 位、每笔请求的 ReqT
python verify_lite_pull_live.py   # lite pull 全链路（61 PASS/0 FAIL）：rebalance/收 12 条/assign+seek/tag/时间戳起点/pause+resume + 队列分配策略（默认 AVG、null 被 start() 拒、AVG_BY_CIRCLE 两实例交叉、CONFIG 两半不重叠、CONSISTENT_HASH 用真实 clientId 建环并收敛到离线预测、MACHINE_ROOM_NEARBY 单机房透传内层策略且 resolver 被真实 brokerName/clientId 问过、MACHINE_ROOM 白名单不匹配 broker-a 时安静饿死）+ S3 **没人调 commit**、只继续 poll 过一整个自动提交周期（闸门只在 poll() 开头查）后 committed() 自己 >0 + **S8 三张位点表在真机各自数出来**（1 条队列的 topic 灌 1200 条：拉取游标 1200 / 已消费游标 -1 / broker 侧还查无提交（committed 回 -1）三个数互不相等 ⇒ 一次都没交付时把拉取游标提交上去就是静默丢消息；单次 poll 交 1024 ⇒ 落到 broker 的正是 1024 而不是 1200；commit(map) 改点位到 5 而两格游标都不动、退回的 176 条照旧交付、全程 1200 条不重不漏；persist=False 时 committed() 读到内存那格 777 而 broker 仍是 5；新实例的起点取 broker 上的 5 而不是另一个实例内存里的 777；seek 同时改两格游标；点名提交抹掉没点名的内存行（Java persistAll 的 remove unused mq）且清理不写 broker）
python verify_lite_pull_cursor_live.py # lite-pull **拉取游标**（8 PASS/0 FAIL，2026-09-29 实测；Java `DefaultLitePullConsumerImpl#PullTaskImpl.run:982-998`，与 C++ `rmq_live_lite_pull_cursor`、Rust `live_lite_pull_cursor`、.NET `lite-pull-cursor` 同场景）：一轮拉取**成功返回**之后，无论 `FOUND`/`NO_NEW_MSG`/`NO_MATCHED_MSG`/`OFFSET_ILLEGAL`，拉取游标都要推进到 broker 给的 `nextBeginOffset`（唯一的刹车是「在途请求的结果不许盖掉这轮里刚 seek 的位点」）。旧实现只在 `FOUND` 时用「最后一条.queueOffset + 1」推游标，坏法是**静默**的：S2 把 assign 表达式换成永不匹配的 Tag 再 seek(0) —— broker 按表达式把整段滤掉后回的 `nextBeginOffset` 已越过整段（== maxOffset），旧实现的游标永远停在 0、每轮重扫同一段（断言每条队列游标 == maxOffset=1 且零投递）→ S3 对每条队列 seek(maxOffset + 1000)：broker 回 `OFFSET_ILLEGAL` 的纠正值，游标必须跟回去（越界自愈），随后每条队列再钉 1 条必须**全部收到** —— 旧实现的游标永远卡在 1001 上，每轮收到同一个「越界纠正」，新消息一条也看不到。S1 对照组先证明链路本身通、`maxOffset == 1` 这个标尺成立。离线三个用例（`tests/test_lite_pull_consumer.py` 的 `test_no_matched_msg_jumps_the_cursor_past_the_scanned_window` / `test_offset_illegal_adopts_the_brokers_correction` / `test_in_flight_seek_wins_over_the_pull_result`）锁的是判据与刹车
python verify_lite_pull_code_live.py # lite-pull **请求码 / broker 开关**（13 PASS/0 FAIL，2026-09-29 实测；#107，Java 把两件事拆在两处：消费者侧 `DefaultLitePullConsumerImpl#pullSyncImpl:1058` 置 `FLAG_LITE_PULL_MESSAGE(0x10)`、客户端 API 侧 `MQClientAPIImpl#pullMessage:816-820` 按位把请求码切成 `LITE_PULL_MESSAGE(361)`；与 C++ `rmq_live_lite_pull_code`、Rust `live_lite_pull_code`、.NET `lite-pull-code` 同场景）：这条链在离线假 broker 上永远是绿的 —— 少了位、码还是 11 时，报文依然是一个完全合法的 pull，broker 照常回消息。能把它区分出来的只有真 broker 的 `litePullMessageEnable` 开关（`PullMessageProcessor:325-331` **只拦 361**）：S1 开关 true 基线（lite 消费者收到消息、拉取游标推进）→ S2 运行时把开关翻成 false（UPDATE_BROKER_CONFIG，不重启）：S2a **裸 361** 请求 → `NO_PERMISSION(16)` + `the broker[...] for lite pull consumer is forbidden`、S2b **对照组**同队列同一位点的裸 11 照常 SUCCESS 拿到消息（没有这条腿，S2a 的失败可能只是 broker 坏了）、S2c 新起的 lite 消费者**安静饿死**（消息明明在，poll 一条不来、拉取游标纹丝不动 —— 旧实现位不置/码为 11 时这条腿会收到消息，判别器当场变红）、S2d **对照组** push 消费者照常消费（整条消费链路没坏）→ S3 开关还原 true，lite 立即恢复。退出前**无条件**把 `litePullMessageEnable` 写回原值。离线判别式（`tests/test_lite_pull_consumer.py` 的 `TestLitePullWireContract` 5 项：请求码 361、`sysFlag` 逐位等于 Java :1058、经典拉取对照腿是 11 且无 lite 位、NO_PERMISSION 的 remark 不被吞）在负控下把 S2a+S2c×2 三条腿变红（真机实测：强制码 11 → 9 PASS/3 FAIL）
python verify_sql92_live.py       # SQL92 过滤 + CHECK_CLIENT_CONFIG(46)（20 PASS/0 FAIL）：SQL92 订阅启动时正好一笔 46、纯 TAG 订阅一笔不发；broker 真按属性过滤（red 只收 3 条、blue 不漏、'*' 对照组收 6 条、永不匹配收 0 条）；语法错的表达式让 start() 秒回 SUBSCRIPTION_PARSE_FAILED(23) 并就地回滚。需 broker 开 enablePropertyFilter=true
python verify_tls_live.py         # 整条客户端链路跑 TLS（8 PASS/0 FAIL）：30 轮新建 TLS 连接打首包、producer+push consumer 全程 TLS 收发、确认没退回明文、shutdown 不留读线程
python verify_async_send_live.py  # 异步发送内核 A1~A6（39 PASS/0 FAIL）：不阻塞返回 + 线程口径（AsyncSenderExecutor_1 跑准备段、NettyClientPublicExecutor_1 跑回调）+ 用 offsetMsgId 读回原文、30 笔并发各恰好一个终态且槽位/UNIQ_KEY 不重复、定点发送、CheckForbiddenHook 拒绝不留痕、批量走同步批量内核（一次回调、broker 逐条回 3 个 commitLog 偏移、读回的子消息带客户端 32 位 UNIQ_KEY）、shutdown 不等在途（36 笔全报错、一条都没落）
python verify_backpressure_live.py # 异步发送背压 B1~B5（Java 两个公平信号量，真机版）
python verify_flow_control_live.py # 拉取前流控五个阈值 + 启动期数值闸门 S0~S5（28 PASS/0 FAIL）：S0 默认闸门+快消费**不命中**（12 条全到，闸门误伤正常流量表现为吞吐莫名腰斩，最难查）→ S1 只留队列级字节闸门（`pull_threshold_size_for_queue=1`，单位 **MiB**）⇒ 命中 15 次、8 条 400KB 一条不丢不重 → S2 只留跨度闸门（`consume_concurrently_max_span=2`）⇒ 命中 5 次、14 条仍全部消费 → S3 只留 topic 级条数闸门（`pull_threshold_for_topic=4`，4 队列）⇒ 单队列到不了 4 条、必须跨队列累计（命中 135 次）且每条队列都消费到底 → S4 复用 S1 的组与 topic ⇒ 位点从 broker 末尾续上、闸门**不是命中一次就失效**（仍命中 9 次）、6 条不重不丢（锁"暂停"被写成"退出拉取循环"，S1 看不出差别）→ S5 启动期数值闸门（Java `checkConfig` 数值段 `:1099-1209`）⇒ 贴着 Java 区间**端点**的配置真能把消费者启动起来并收全 10 条（闸门写坏最常见的方式是"比 Java 还严"，把合法配置也拒了，用户直接起不来；这一半离线只证明了"越界会拒"，证不到"边界能用"），5 条越界配置逐字按 Java 文案在本地被拒、`_started`/`_mq_client` 都还是空（没留下半启动实例），broker 侧再用**裸** `get_consumer_list_by_group` 反查：被拒的组查不到（本机 5.5.1 broker 对从未注册过的组回 `code=1 no consumer for this group` 而不是空列表，两种形态都算"查无此组"，任何别的异常判 FAIL），边界值那个组恰好查得到 1 个 clientId —— 写成"先注册再校验"的话 `ConsumerManager` 会留下一堆永不心跳的僵尸 clientId，把 rebalance 用的 `cidAll` 撑歪，真机表现为队列分配不均，而客户端日志里只有启动失败那一条。⚠ 有了 S5，夹具里"把某道闸门关掉"的写法必须是 Java 的**上界**（`OFF_COUNT=65535` / `OFF_SIZE_MB=1024`）而不是 `0`：`0` 现在正是启动期会拒的配置。五个阈值的判定顺序、`Math.max(1,n)` 的守卫、**严格大于**的跨度边界、topic 级字节闸门**不复用**队列级那道开关、命中一次只记一格，都由 `tests/test_flow_control.py`（7 项）离线锁死；真机这一半锁的是离线锁不住的"确实会命中"和"命中后不丢"；命中的**次数**随真机投递/消费节奏浮动（S3 两轮分别报 105 与 135），判据只要求 `triggered > 0`。⚠ 两条夹具坑（都是实测踩出来的）：大消息必须**不可压缩**（`os.urandom`，全同字节会被生产者压到几百字节、broker 落盘 `store_size` 跟着变几百字节，size 闸门于是"永不命中"）；S1/S4 的 topic 必须**只有 1 条队列**（8 条 400KB 摊到 4 条队列每条才 800KB，永远够不到队列级那道 1MiB）
python verify_pull_expired_live.py # 拉取循环停摆**自愈**（Java isPullExpired / PULL_MAX_IDLE_TIME=120s，`RebalanceImpl.updateProcessQueueTableInRebalance:438-461`，11 PASS/0 FAIL）：A1 基线（3 条被消费、位点到 3、307 运行信息里 `lastPullTimestamp` 是循环自己盖的真时刻且新鲜）→ A2 把这一路的线程表条目换成一条**已退出的线程**（等价于循环被异常打穿）⇒ 下一趟 rebalance 必须换上另一条活线程，新发的 3 条照样被消费（位点到 6）→ A3 线程还活着但把盖章时刻**倒拨 121s**（> 120s）⇒ 同样被撤并重建、再发 3 条照样消费（位点到 9）→ A4 前 9 条各只投一次、`reconsumeTimes` 全 0、时钟恢复新鲜（撤走前持久化了位点，重建从 broker 位点续拉）。恢复判据用「既不是原线程、也不是注入的那条、而且活着」，否则注入还没被撤走也会被误判成通过。这条路径坏掉是**静默的**：不报错、心跳照发、别的队列照常推进，真机上只能从"某条队列位点永远不动"反推，所以停摆→恢复的闭环必须真机取证；阈值 120s 与**严格大于**的边界、盖章在流控/锁判定**之前**（`pullMessage:253`，卡住的循环也要留心跳）、POP 分支读 `lastPopTimestamp`（`PopProcessQueue:74`）、停机途中不判停摆，都由 `tests/test_pull_expired.py`（13 项）离线锁死
python verify_ack_index_live.py   # classic 并发消费的 ackIndex 部分 ack（11 PASS/0 FAIL）：A1 对照组整批认可（3 条各投一次、位点到 3、零回投）→ A2 ackIndex=0 只认可首批第一条 ⇒ 尾巴 2 条经 %RETRY% 二次到达（reconsumeTimes>=1、topic 还原成业务 topic）、被认可那条整个窗口只投一次、3 条最终全部消费、业务队列位点仍整批提交到 3 → A3 ackIndex=2 压不住 RECONSUME_LATER（Java :222-226 强制 ackIndex=-1，3 条全重投）→ A4 广播模式下尾巴不回投。批次切分由拉取时机决定，所以三个用例都**先把 3 条放上去再起消费者**（新组显式 CONSUME_FROM_FIRST_OFFSET），否则首批可能是 1~2 条、前缀/后缀根本不确定
python verify_orderly_reconsume_live.py # 顺序消费的重投闸门与显式 ack/回滚 O1~O7（31 PASS/0 FAIL，2026-09-28 实测，Java ConsumeMessageOrderlyService:236-362，与 verify_redelivery_live.py 的并发侧 S9 是两条不同代码路径）：O1 `max_reconsume_times=2` + `consume_message_batch_max_size=1` + `suspend_current_queue_time_millis=500` + 1 队列 topic ⇒ 毒消息恰好投 3 次、`reconsumeTimes` 走 0/1/2 的阶梯（每一格都是**客户端自己 +1**，broker 收到时已经加过）、第 3 次交回 broker 后业务队列**立刻前进**（后一条被消费，不是被毒消息永久堵住）、再等 15s 没有第 4 次、挂起期间 listener 始终看到业务 topic（本地重投不换 topic）；同一段里 broker 把毒消息改写进 `%DLQ%<g>`（路由此刻才建出来），死信那条 `reconsumeTimes=3`（存储时 +1）、`RETRY_TOPIC` 仍是业务 topic。**顺序回投能落进死信而不是走延迟档位，本身就是 broker 此刻看到该组的重平衡锁没过期**（`SendMessageProcessor#handleRetryAndDLQ:202-207` 只在 `!isLockAllExpired` 时立刻判死信）——也就是「拿着 `LOCK_BATCH_MQ` 把消息交给 broker」这条链真的接上了（O1 另外直接数了该组 `LOCK_BATCH_MQ` 续锁成功的次数，实测 2 次，>0 才算拿到判据的现场证据） → O2 反过来验 `-1` 那一支：顺序侧 `getMaxReconsumeTimes:313-320` 把 `-1` 读成**不设上限**（**不是**并发侧 `DefaultMQPushConsumerImpl:890` 的 16），实测同一条毒消息被持续重试 28 次、`reconsumeTimes` 阶梯一路爬到 19，而 `%DLQ%` 连 topic 都没建出来 → O3 挂起档位三档（`submitConsumeRequestLater:211-234`：context 值优先、`-1` 回落配置、结果钳到 [10, 30000]）：配置 900ms / context 70ms ⇒ 端到端中位间隔 0.129s（n=11）**且**分发线程上客户端请求的就是 0.07s；context 1ms / 配置 0 两个非法值都请求 0.01s 且实测间隔中位 0.0673s ≥ 10ms（漏钳就是忙等）；context 40s 只请求 30.0s。判据取「客户端请求的睡眠时长」而不是只看间隔：分发循环在挂起之后还有 50ms 固定轮询节拍，间隔 = 挂起 + ~50ms，10ms 与 1ms 的差别落不进这个噪声里（`SleepRecorder` 用线程名 `rmq-dispatch-` 只拦分发线程；「请求 30s」那档把真实等待压到 20ms，断言的是请求了多久） → O4 `autoCommit=false` 时 SUCCESS 只记 TPS **不提交**（`processConsumeResult:272-274`，binlog 消费场景）：listener 连持 3 次后放行，那条消息一共投递 4 次、后一条在第 5 个位置才出现且一出现就被提交（`afterIdx=4 auto=True`）；忽略 `autoCommit` 直接当成功 ack 的话毒消息第一轮就被吞、后一条立刻被消费，两条判据同时不成立。三条分支（+1、用尽才回投、回投失败才继续挂起）与回投那条消息的字段由 `tests/test_orderly_reconsume.py`（35 项）离线锁死——离线未 `start()` 的消费者拿不到内部生产者，回投必定失败，所以离线只锁得住失败分支，「回投成功 ⇒ 位点前进、队列不堵」只能靠真机这一段。**O5~O7 是顺序侧的显式批量 ack / 显式回滚**（Java `processConsumeResult:246-296`，与 C++ `rmq_live_redelivery` 的 S13、Rust `live_consumer` 的 C13、.NET `redelivery` 的 S13 同场景；两者都只在 `autoCommit=false` 时合法）：O5 `COMMIT` 把整批一次认可（1 队列 topic、批量上限 3、**先发 3 条再起消费者** ⇒ 首批就是完整三元素批次 `[ack-1,ack-2,ack-3]`、整个窗口只投这一批、broker 位点从 0 直接到 **3**，不是卡在 0）；O6 `ROLLBACK` 把这一批退回队首后在**本地**立即重投、不过 broker：head 恰好投 7 次 = 6 次回滚 + 第 7 次提交，相邻间隔中位数实测 `0.266s` / 最大 `0.268s`（挂起配 200ms），而真走 `%RETRY%` 最快也只能等 broker 的第一个延迟档（`delayLevel=3` 即 10s），相差 40 倍；重投期间 `reconsumeTimes` 全 0 且 listener 看到的仍是业务 topic（「没过 broker」的直接证据），后面的消息不越位（`idx7=6 idxNext=7`），显式提交后 head/next 各一次、broker 位点到 2；O7 `autoCommit=true`（默认）时两者都是**非法用法**，Java `:246-250` 只 warn 然后顺势落进 SUCCESS 分支按 ack 处理 —— head 只投一次、next 立刻被消费、位点到 2（真按回滚办的话反证窗口里 head 会被重投 ~10 次）
python verify_send_header_live.py # 发送头 c/d/n 三个字段（14 PASS/0 FAIL）：H1 默认 `d=4` 时自动建出的 topic 队列数 = `min(4, TBW102.writeQueueNums)`；H2 `set_default_topic_queue_nums(2)` 真的让 broker 只建 2 条队列（写死 4 的旧行为必然是 4）；H3 `set_create_topic_key(模板)` 时继承**模板**的 3 条队列而不是 TBW102 的 8 条（`TopicConfigManager.java:286-289` 的 `isInherited` + `min`）；H4 同步/定点/单向/批量 320/异步五种入口逐条落地（7 条一条不差）；H5 落点 broker 名与路由一致。`n`（brokerName）在经典 broker 的发送链路里**没有读者**（5.5.1 源码 grep 过），它的线上存在由 `tests/test_send_header_fields.py`（7 项，抓真报文）取证
python verify_producer_unregister_live.py # 生产者退出注销 UNREGISTER_CLIENT(35)（11 PASS/0 FAIL）：U1 发送成功 → U2 心跳后 204 `GET_PRODUCER_CONNECTION_LIST` 能看到本 clientId（注册确实发生过，"消失"才有意义；组靠心跳上线，所以要轮询等）→ U2b 对照组注册可见（204 这条判据本身有效）→ U3 `shutdown()` 期间钩子抓到 35：每台已知 broker 各一发、头是 `clientID`+`producerGroup` 且 **`consumerGroup` 整个字段不上线**（Java 传 null；broker `ClientManageProcessor:228/237` 判的是 `group != null`，空串会被拿去查 `""` 的订阅组配置）→ U4 每一发 35 都回 SUCCESS（走的是**还没关**的那条长连接）→ U4b 35 排在业务发送之后 → U5 紧接着查 204 这个组已经不在（broker 回 `the producer group[...] not exist`）→ U6 对照组仍在（排掉"broker 把所有连接都清了"这种假阳性）。⚠ 判据强度：Python 里每个生产者各持一份 `MQClientInstance`、各一条连接，退出时连接也关掉，单看 U5 分不出是 35 还是断连的功劳，所以这里必须由钩子抓帧直接证明线上走了这一发；行为级的判别式证明在 `rust/examples/live_producer.rs` 的 P11（Rust 按 clientId 复用实例，先退的那个连接还活着）。超时预算跟 Java 同一口径：`MQClientInstance#unregisterClient:1170` 传 `getMqClientApiTimeout()`=**3000ms**，异常一律吞成 debug（shutdown 不因单台抖动中断）。扇出**含 slave**（`get_all_broker_addrs`），心跳一侧仍只打 master 优先那台（`get_route_of_all_brokers`）——注意这个分工是**按带不带 ConsumerData 分**的：消费者心跳带 `ConsumerData`（`consumerEmpty=false`）必须每台都打（Java `sendHeartbeatToAllBroker`:732-750 仅在 `consumerEmpty && id != MASTER_ID` 跳从节点），生产者心跳只带 `ProducerData` 仍只打 master；从节点收不到消费者心跳时会给指向自己的拉取回 `SUBSCRIPTION_NOT_EXIST`（`PullMessageProcessor`:420-427，默认 `postSubscriptionWhenPull=false` 的拉取走的正是那条）。这个分工与四种"空白组名不上线"的分支由 `tests/test_producer_unregister.py`（10 项，含「默认预算就是 3000ms」与「消费者心跳扇出含从节点、生产者只打 master」两条——前者写歪只会让退出慢一档，不会有任何用例变红）离线锁死
python verify_interval_live.py   # 定时任务周期（22 PASS/0 FAIL）：I1 两条生产者（刷新周期 1s / 默认 30s）都把同一个**还没建**的 topic 登记进在用集合，建完后 1s 组 1.06s 拉到路由、30s 组此刻还没有、30.20s 才拉到（周期决定时机，不是缓存坏了）；I2 两条消费者（落盘周期 1s / 60s）首笔落盘都在 initialDelay 实测 10.18s / 10.48s（**不是立刻、也不是 initialDelay+一个周期**），第二批消费后 1s 组 0.83s 把 broker 位点推到 6 而 60s 组仍是 3，60s 组 `shutdown()` 收尾落盘到 6（只是周期没到，不是坏了）；I3 实例上拿到的就是调用方设的值。周期是配置项、时间是唯一可观测量，跑一遍数值会有 ±0.1s 抖动，量级与判据（"30s 组至少晚 20s""首笔落盘 ≥10s"）才是断言对象（2026-09-24 复跑 22 项仍全过：1.12s / 30.33s、首笔 10.14s / 10.44s、1s 组 0.81s 推到位，各时间量相对上面记录的值都在 ±0.2s 内）
python verify_fail_fast_live.py # broker 真死掉时在途请求立刻判死（Java failFast → requestFail，18 PASS/0 FAIL）：L1 基线（5 条同步发送 SEND_OK、各队列队尾位点合计覆盖 = 真的落盘）→ L2 三条 suspend=20s 的长轮询确实挂在 broker 上（2s 后仍未返回）且占了在途表 → L3 用 `scripts/rmq_test_broker.sh stop` 杀掉 broker，三条都在 **2.2s** 内拿到 `RemotingSendRequestException`（不是等满 30s 才报 `RemotingTimeoutException`：异步发送的重试分类按异常**类型**分流，报成超时等于换一整套重试决策）、在途表随后排空 → L4 同一个传输实例上的 namesrv 连接没被牵连（206 仍回 SUCCESS）→ L5 broker 拉起后**同一个 producer 实例**重新建连照常发送，那 5 条 SEND_OK 的消息一条不少。脚本只启停 broker、不删 store（四个语言的同一用例共用它）。⚠ 真机只能证"按地址隔离"；"同地址换连接时旧读线程收尾不误伤新连接"那一层由 `tests/test_fail_fast.py`（6 项，含回调恰好一次、`shutdown` 排空在途）离线锁死
python verify_subscribe_live.py  # 后置订阅 + 立即心跳（7 PASS/0 FAIL，Java DefaultMQPushConsumerImpl#subscribe:1265-1275 就是 put 完直接 sendHeartbeatToAllBrokerWithLock()，**没有** started 闸门）：S0 正腿对照 —— 起消费者时订阅的 topic B 在 `QUERY_TOPIC_CONSUME_BY_WHO(300)` 里查得到本组（先证明"心跳路径 + 300 号查询"这条观测链本身有效）→ S1 反腿对照 —— **没订阅**的 topic L 查不到本组（排掉"300 恒回本组"的假阳性）→ **S2 本条**：`start()` 之后才 `subscribe(L)`，紧接着查 300 立刻就有本组，实测耗时 **1.2ms** ≪ 30s 周期（ClientConfig.heartbeatBrokerInterval）—— 这个时间差就是"心跳是订阅路径同步推的、不是下一次定期心跳顺带发的"唯一证据；同时活订阅表里立刻能看到 L → S3 订阅真生效：L 的队列进 `assigned_queue_keys()`、发进去的消息被消费到（不是只把名字记进表）→ S4 `unsubscribe(L)` 后本地订阅集合里 L 消失（broker 侧不退组：`ConsumerManager#clearTopicGroupTable` 只在整组消失时才摘，Java 同样，所以这条只能在本地断言）。离线半边由 `tests/test_subscribe_after_start.py` 锁死（消费者对着连不上的 name server 启动 ⇒ 心跳一台都发不出去，只能验"表进对了、不再抛 already started"）
python verify_pinned_guard_live.py # 定点发送的 topic 守卫（20 PASS/0 FAIL，2026-09-28 实测；Java 全树只有两处守卫：同步 `DefaultMQProducerImpl:1234-1236` 抛 `message's topic not equal mq's topic`、异步 `:1277-1278` 抛 `Topic of the message does not match its target message queue`）：离线假集群证明得了「拒了、且报文没上线」，证明不了**另一面** —— 从真实路由取来的队列（broker 名与队列号都是集群给的）不能被误伤，守卫写宽一点或把 `mq.topic` 与 `msg.topic` 比错一边，离线预置的队列照样是绿的而线上第一条消息就发不出去。S1 四条放行腿在真路由队列上 SEND_OK 且落在指定队列、三笔子消息**真落库**（maxOffset = 单条 1 + 批量子消息 2）→ S2 反腿：同步单条/批量都拒、文案逐字对 Java、**亚毫秒**返回且无 broker 码（不是超时、不是 broker 的 remark），wire 反证 A 的 maxOffset 一动没动、B 上一条都没有（"守卫只是抛错、消息其实已经发出去了"这种坏法只在 broker 侧看得出来）→ S3 命名空间腿看 `queueWithNamespace` 的幂等：队列 topic 已带 `ns1%` 前缀（真路由返回的就是这个形状）不误拒、裸 topic 同样放行且两条都真落进 `ns1%topic`，换成 `ns2%` 才拒（对照腿：拒的是名字，不是"有前缀"）→ S4 异步单条/批量：拒的时候走回调、用的是**异步那句**文案、拒后 maxOffset 仍不动；放行的两条腿 SEND_OK 且真落库 → S5 **单向定点没有守卫**（Java `:1303-1310` 有意留的口子）：报文按 **msg 自己的** topic 落进 A、目标队列所在的 B 一条都没有 —— 这不是漏发，是 Java 的口子，写在这里是为了让守卫的位置若被"顺手补齐"当场红 → S6 push 消费者把七条正腿消息一条不少地收齐（放行腿真的可消费，不只是 SEND_OK）。离线半边由 `tests/test_pinned_send_topic_guard.py`（11 项，假集群抓帧：拒的**一笔请求都不上线**、同 topic 照发、命名空间下比各自包装后的名字、批量共用同步那处守卫、异步那句文案从回调交）锁死，另有两项钉住**没有守卫的两条腿**——`sendOneway(msg, mq)` 与选择器发送都照 Java 留着口子，不因为"顺手补齐"被误加守卫
python verify_correct_tags_offset_live.py # 空应答也把已提交位点推走（9 PASS/0 FAIL，2026-09-28 实测；Java `DefaultMQPushConsumerImpl#correctTagsOffset:713-717`，调用点 `:394-401`，与 C++ `rmq_live_correct_tags_offset`、Rust `live_correct_tags_offset`、.NET `redelivery` 的 S14 同场景）：离线单测锁得住「哪些状态要修正 + 闸门何时放行」（`tests/test_correct_tags_offset.py`），锁不住「这条修正真的走到了 broker」—— 位点最终由 `UPDATE_CONSUMER_OFFSET` 落盘，只有真集群能证明 broker 上的已提交位点前移了、而且是在**一条消息都没投递**的前提下前移的。S1 对照组：4 队列 topic 用 `TagA` 正常消费 5 条（消息确实在队列里，且「已提交位点 == 各队列 maxOffset」这个数值口径本身就是常规消费的落点）→ S2 换 `TagB`（永不匹配）另起一组：broker 侧过滤后应答是 `PULL_RETRY_IMMEDIATELY`（`MQClientAPIImpl:1095-1097` ⇒ `NO_MATCHED_MSG`），listener 一条都没收到而每条队列的已提交位点**仍等于该队列 maxOffset**（没有这条修正时 broker 上查无此组的位点记录，位点永远停在未提交状态）→ S3 同一消费者自动补上的 `%RETRY%<group>` 空队列（`PULL_NOT_FOUND` ⇒ `NO_NEW_MSG`）也留下值 == `maxOffset`(0) 的位点记录（没有修正时这个 key 根本不会进位点表，也就没有报文）→ S4 整轮下来 listener 依旧是 0 条 —— 修正只抬位点、不会凭空投递
python verify_offset_illegal_live.py # OFFSET_ILLEGAL 纠错分支（12 PASS/0 FAIL，2026-09-28 实测；Java `DefaultMQPushConsumerImpl:402-427`，与 C++ `rmq_live_offset_illegal`、Rust `live_offset_illegal`、.NET `offset-illegal` 同场景）：这条分支做四件事 —— 位点改用 broker 给的修正值（`setNextOffset`）→ 丢掉这条队列上已取回未消费的消息（`ProcessQueue.setDropped(true)`）→ 把修正位点**立刻**落盘（`updateAndFreezeOffset` + `persist`）→ 撤掉队列让 rebalance 按修正位点重建（`removeProcessQueue` + `rebalanceImmediately`）。离线单测（`tests/test_offset_illegal_recover.py`，13 项）只能锁住本地状态怎么清、哪个 ack 被作废，真机证两件它证不出的事：**S1 丢队列**——listener 卡住第一条（在途 1 条、缓冲里 2 条）后用 `resetOffsetByQueueId` 把位点重置到 3（下一笔 pull 被 `PullMessageProcessor:539-545` 短路成 OFFSET_RESET ⇒ 客户端 `OffsetIllegal`）：修复前缓冲里的第 1、2 条照常投递（listener 实收 3 条），修复后只剩在途的第 0 条且它的 ack 因队列已被丢（`ConsumeMessageConcurrentlyService:267`）而作废，再发第 4 条验证重建后的队列从修正位点续跑、冻结随重建解除（新消息的 ack 让 broker 位点前进到 4）；**S2 立刻落盘**——利用 `resetOffsetByQueueId` 两笔 RPC 非原子（第 1 笔 commitOffset 无区间校验先落库、第 2 笔 222 被 `resetOffsetInner` 拒绝）把 broker 已提交位点做成非法值 103，再让 `persist_consumer_offset_interval=60000` 的新消费者从 103 起拉：窗口内唯一能把 103 写回 3（maxOffset）的路径就是纠错分支自带的那次 persist，且全程零投递。⚠ 发现延迟 ~24s 是 Java 同构的长轮询语义（客户端下发 `suspendTimeoutMillis=20000`、broker `PullRequestHoldService` 每 5s 巡检，命中前那笔 pull 不会重读 resetOffsetTable），等待窗口给 45s
python verify_reset_offset_live.py # 220 重置消费位点的**客户端半边**（20 PASS/0 FAIL，2026-09-28 实测；与 C++ `rmq_live_reset_offset`、Rust `live_reset_offset`、.NET `reset-offset` 同场景）。Java 这条链路分两头：admin 侧发 222（`AdminBrokerProcessor:2255-2270`；`useServerSideResetOffset=true` 时 broker **自己**改位点、一笔 220 都不推），开关关掉才走 `Broker2Client.resetOffset:158-163` 推 220 —— body 形状由 `isC` 决定：`ResetOffsetBodyForC`（offsetTable 是 **JSON 数组**）还是 `ResetOffsetBody`（对象即键的 map，Java 自己的 `ClientRemotingProcessor.resetOffset:153` 只解这一种）。收到后进 `MQClientInstance.resetOffset:1403-1450`：命中的队列挂起 → `pq.setDropped(true); pq.clear()`（在途批次 ack 与缓冲一起作废）→ 非顺序消费时等 `RESET_OFFSET_MAX_WAIT`=10s → `updateConsumeOffset` + `removeUnnecessaryMessageQueue`（先落盘再摘队列）→ 恢复。离线 `tests/test_reset_offset_handler.py` 只锁得住「本地表怎么动」，真机要证三件它证不出的事：**220 真到了本端**（数组形状漏解时 220 被静默丢弃，客户端照常拉取、位点永不后移）、**位点当场落盘**（不是等下一个周期）、**在途/缓冲的旧批次真作废**（旧 ack 不能把位点推回去）。S1 回退重置（10 → 3）：1 队列 topic 先消费 10 条并把位点周期落盘到 10（reset 要求组在 broker 上有记录，否则目标位点取不到）→ 换一个落盘周期 60s 的步进消费者制造「1 条在途（offset 10 卡在 listener）+ 4 条留在缓冲」的窗口 → 222 应答里的目标位点就是 3 → broker 位点 **0.2s 内**变成 3（周期落盘还是 60s，快过它的只有重置路径自带的那次 persist）→ 本地表里旧位点已摘掉、队列代号 +1（队列真被重建，不是只拨游标）→ 放行后新队列第一批 offset 3 已在途时本地位点仍未越过 3（旧批次 ack 作废）→ 重投序列 `[10, 3, 4, …, 14]`（在途那条 10 也在新队列上重投）→ 窗口内只有重置那一次写 broker → 关停落盘把 ack 写回（15）→ S2 前跳（3 → maxOffset 10，`timestamp=-1` 即 Java 的 null ⇒ `getMaxOffset`）：第 4 条卡在 listener 时重置 ⇒ broker 位点 0.2s 内前跳到 10、被跳过的 4..9 **一条都不投**（在途那条的 ack 也作废）、新消息 offset 10 正常投递、关停后 11。收尾无条件把 `useServerSideResetOffset` 还原成 `true`。⚠ 重置用的时间戳必须是**墙钟**（`time.time()` 那一档）：broker 的 `getOffsetInQueueByTime` 拿它跟消息 `storeTimestamp`（broker 墙钟）比，拿「自开机以来的毫秒」（`time.monotonic()`）去比会让目标静默塌成 0、整条队列从头重投，而用例表面上是「重置成功」。⚠ 本端口有意把那个 10s 等待压到 **0.2s**（220 是 oneway、broker 不等响应；队列代号已让在途 ack 全部失效，不靠「等」避竞争），写完位点**补一次立刻 rebalance**（Java 要等 `removeUnnecessaryMessageQueue` 的延时撤销 + 下一轮 rebalance）
python verify_pull_consumer_heartbeat_live.py # 拉模式消费者（DefaultMQPullConsumer）的心跳必须把消费组注册进 broker（12 PASS/0 FAIL，2026-09-28 实测，与 C++ `rmq_live_pull_heartbeat`、Rust `live_pull_heartbeat`、.NET `pull-heartbeat` 同场景）。Java 的经典拉模式消费者把组登记进实例的 `consumerTable`（`DefaultMQPullConsumerImpl.start():746`），心跳由实例级周期任务发出；本端口的心跳循环在消费者内（与推送/轻量消费者同一做法：`start()` 里先同步打一轮、之后按 `heartbeat_interval_millis` 重发，`heartbeat_enabled=false` 可关）。离线（`tests/test_pull_consumer.py` 的 4 项）锁得住报文形状与循环开关，锁不住 **broker 真的登记了本组**：不发心跳时 `consumerConnection`(203) 与 `GET_CONSUMER_LIST_BY_GROUP`(38) 都是空的，broker 侧 `isRejectPullConsumerEnabled=true` 还会直接拒掉每一笔拉取（`PullMessageProcessor:493-505`），而客户端日志里只有"拉取正常"—— 这条坏法是**静默**的。A0 建 4 队列 topic → A1 起消费者 + 真拉一轮（拉取链路先通，后面的 203 才有意义）→ A2 主节点 203：本组在册且 `consumeType=CONSUME_ACTIVELY`、`consumeFromWhere=CONSUME_FROM_LAST_OFFSET`、`messageModel=CLUSTERING`（Java `:348-353`；四个端口一度都发 `CONSUME_PASSIVELY`，而 `ClientManageProcessor:87-92` 会**跳过**主动型心跳的订阅登记，只是靠 `PullMessageProcessor:397-412` 的补偿分支才照常拉得到）→ A2b 订阅表带 registerTopics 的 topic 且 `subString="*"`、`subVersion=0`（`subscriptions():357-385` 走 `FilterAPI.buildSubscriptionData(topic, SUB_ALL)` 并把 subVersion 归零）→ A3 38 的 clientId 列表里有本实例 → A4 从节点 203/38 同样看得到（漏扇出时指向从节点的拉取会被 `PullMessageProcessor:420-427` 回 `SUBSCRIPTION_NOT_EXIST`）→ A5 幽灵组对照：203 报错、38 空列表（判据本身有效，不是"203 恒真"）→ A6 `shutdown()` 的 35 让 203 随即查不到（不必等 ~120s 通道扫描）。⚠ 这个用例**会删掉它自己建的 topic**
python verify_publish_route_master_live.py # 发布路由跳过没有 master 的 broker（25 PASS/0 FAIL，2026-09-28 实测，与 C++ `rmq_live_publish_route_master`、Rust `live_publish_route_master`、.NET `publish-route-master` 同场景）：Java `MQClientInstance.topicRouteData2TopicPublishInfo:294-303` 组装发布信息时，brokerDatas 里没有同名 broker、或它的 brokerAddrs 没有 MASTER_ID，整条 QueueData 跳过 —— 从节点自己也注册进 namesrv 且默认配置下照样带写位（`RouteInfoManager` 只在「prime slave 且 enableActingMaster」时才抹掉 WRITE，本机 broker.conf 是 false），漏判这条生产者就会把消息发到从节点上，而从节点对发送请求一律 reject（`SendMessageProcessor` ⇒ SYSTEM_BUSY(2)，**还是可重试码**）白烧重试；消费侧是另一份口径（`topicRouteData2TopicSubscribeInfo:318-332`：读位 + readQueueNums、**不要求有 master**），停窗口内消费者仍要看得见队列、还得能从从节点拉。离线 `tests/test_publish_route_master.py` 锁的是判据（构造出的路由形状 → 队列集；**地址侧**同题：发布地址平表只认 brokerId=0、查不到先按 topic 刷一次路由再查、仍查不到报「The broker[X] not exist」（码 None）、四个 `MQAdminImpl` offset 查询同口径、退让口径 `broker_addr_of` 作负控；**订阅侧**同题：`lock_batch_mq`/`unlock_batch_mq` 拿不到主就整台跳过（不刷路由、不抛、0 条 wire）、`pop_message` 刷一次路由后仍只认主、本端报「The broker[X] not exist」（码 None）、`query_consumer_offset` 刷一次路由后**放宽**到从节点，三支各有负控），真机锁的是判据作用在**真实路由形状**上（名字服务里 broker-a 真的只剩 `{1: slave}`）：S0 控制腿（路由 {0: master, 1: slave}、发布/订阅各 4）→ S1 每队列定点预埋 1 条并等从节点 store 追上（不然 S6 无从消费）→ S2 用 `scripts/rmq_test_broker.sh stop` **只停 master**（SIGTERM ⇒ unregisterBrokerAll）等到 broker-a 只剩 {1: slave} → S3 读原始表得到 `msg_queue_list == 0`、S3b 发布信息访问器本端抛「Can not find Message Queue for topic」→ S4 订阅队列仍是 4（消费侧的 `:318-332` 不要求有 master）→ S5 不指定队列的同步发送快速失败且报错里**没有从节点地址**（旧缓存腿打的是死掉的 master），S5b 把发送实例刷成停后形状后 S5c 是**本端 10005**、无 BrokersSent（一条 broker wire 都不发）→ S5d (B) 地址侧对照：定点发到该队列（定点发送不取发布信息，地址解析是它唯一的路由来源）→ Java `findBrokerAddressInPublish:1295-1305` 只认 brokerId=0，本端报「The broker[broker-a] not exist」、一条 broker wire 都不发（实测 1ms；旧行为是打到从节点上换一个可重试的 SYSTEM_BUSY(2) —— 白烧一整轮重试，错误类型也和 Java 不一样；S5c 若漏做，不指定队列的发送就是这个下场）→ S5e (D) 订阅口径：顺序锁整台跳过（Java `RebalanceImpl#lock:153`/`lockAll:195` 走 `findBrokerAddressInSubscribe(brokerName, MASTER_ID, true)`：只认主、**不刷路由**，拿不到就整台跳过；实测 `lock_batch_mq` 空集且 0ms，S5e2 对照腿把同一份报文直接点名从节点 —— `lockOKMQSet=1`，从节点**本来**发得出锁，空集是客户端没去而不是 broker 拒绝；S5e3 解锁同样安静跳过）→ S5f (E) 订阅口径：POP 只认主（`PullAPIWrapper#popAsync:369-373`），`pop_message` 本端报「The broker[broker-a] not exist」（实测 0ms，不是从节点回的错）→ S5g (F) 位点读取（`RemoteBrokerOffsetStore#fetchConsumeOffsetFromBroker:237-241`）：只认主 → 刷一次路由 → 重查**放宽**（位点是 HA 复制的同一份数据，可以从从节点读）；冷实例（路由缓存里没这个 topic）是这条路径最纯的形状，`query_consumer_offset` 由从节点答复（QUERY_NOT_FOUND ⇒ None）不报错 —— 旧口径在此直接报「No route info of this topic」、连刷新都没有 → S6/S6a 停窗口内新起的 push 消费者仍看到 4 条队列、并从从节点把预埋的 4 条收齐 → S7 master 拉回后发布队列恢复 4、S7b 两条失败发送一条都没落库（maxOffset 仍是 1）、S7c 发送 SEND_OK。⚠ 整个停窗口由脚本自己驱动，`try/finally` 保证 master 一定被拉回来

```

其它真机脚本：`verify_acl_live.py`（需开 ACL 的集群）/ `verify_pull_live.py` /
`verify_rr_live.py` / `verify_latency_live.py` / `verify_pop_live.py` /
`verify_pop_consumer_live.py`（11 PASS / 0 FAIL） / `verify_redelivery_live.py`（30 PASS / 0 FAIL）。

`verify_consumer_heartbeat_slave_live.py`（11 PASS / 0 FAIL，**需集群里有一台从节点**）：
消费者心跳扇出到从节点的真机判别式（Java `MQClientInstance#sendHeartbeatToAllBroker`:732-750
遍历 `brokerAddrTable` 的**每个 brokerId**，只在 `consumerEmpty && id != MASTER_ID` 时跳过 ——
消费者心跳必带 ConsumerData，所以从节点不跳；生产者心跳只带 ProducerData，仍只打 master）。
从节点起法（本机集群，brokerId=1、独立 store/端口，`slaveReadEnable=true` 才服务拉取）：

```bash
cat > /tmp/rmq_rust_live/slave.conf <<'EOF'
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=1
brokerRole=SLAVE
listenPort=10931
haListenPort=10932
storePathRootDir=/tmp/rmq_rust_live/slave_store
namesrvAddr=127.0.0.1:9876
brokerIP1=127.0.0.1
slaveReadEnable=true
isolateLogEnable=true
EOF
cd <rocketmq-5.5.1 发行目录> && JAVA_OPT_EXT="-Xms2g -Xmx2g" sh bin/mqbroker -c /tmp/rmq_rust_live/slave.conf
```

场景：S0 路由里 broker-a 有 {0: master, 1: slave}，且 `get_all_broker_addrs()` 两台、
`get_route_of_all_brokers()` 只有 master（两个 helper 的分工）→ S1 起 push 消费者等首轮心跳 →
**S2 核心：从节点上 `GET_CONSUMER_LIST_BY_GROUP(38)` 查到本 clientId** →
S2b 对照：同一个从节点查一个从未心跳过的组 → 查不到（38 这条判据本身有效）→
S3 主节点上 38 也查到（组确实注册成）→ **S4 功能后果：无订阅标志的拉取（push 默认形状，
`postSubscriptionWhenPull=false`）打从节点不再回 `SUBSCRIPTION_NOT_EXIST(24)`** →
S4b 对照：同一个从节点对未注册组仍回 24（那道门真按**每台自己的** consumerTable 判）→
S5/S5b 生产者心跳仍只打 master（从节点 204 `GET_PRODUCER_CONNECTION_LIST` 查不到本组）。
把消费者的心跳改回只打 master，S2 立刻回 `no consumer for this group`、S4 立刻回 `code=24`
（2026-09-28 实测 9/11），这两条是同一处改动的正反两面。

`verify_pop_consumer_live.py` 的 S5 证明 POP 循环和 pull 循环一样把拉取统计写进了 307 状态表
（Java `DefaultMQPushConsumerImpl.popMessage` 的 `PopCallback.onSuccess:556-563`：`case FOUND:`
里先 `incPullRT`，且这一格打在**空列表判定之前**，`msgFoundList` 非空才 `incPullTPS`；
`POLLING_NOT_FOUND` 两格都不动 —— 空手而归是长轮询的常态，把挂起时间折进 RT 会把它毁掉）。
这条链坏掉是**静默**的：消息照弹照 ack、消费完全正常，只有运维看板上一片 0，而看板上"这个消费者
没在拉取"和"这个消费者压根没起来"是两种完全不同的处置。快照每 10s 采样一次、窗口取 minute 差分，
所以夹具必须**持续有流量**并跨过两个采样点（实测 52 条 / 约 26s），否则 `pullTPS` 仍是 0 —— 那是
夹具不够长，不是判据错。拉取侧与消费侧两格各自独立：只有 `consumeOKTPS` 有值而 `pullRT`/`pullTPS`
全 0，正是漏记的形状。`tests/test_pop_consumer.py`（`TestPopLoopPullStats`，3 项）跑**真实的**
`_queue_pop_loop`，离线锁死三种 status 各自记哪几格。实测 `pullRT=2006.2`、`pullTPS=1.9982`、
52 发 52 收（两格的具体取值随真机节奏浮动，判据只要求非 0）。

⚠ 这个脚本曾因**只压 `consume_thread_max` 不压 `consume_thread_min`** 起不来：默认值两侧同为
20（Java 5.x `DefaultMQPushConsumer:162/:169`），把 max 压到 4 之后 `consumeThreadMin (20) is
larger than consumeThreadMax (4)` 会直接在 `start()` 被 #66 那道闸门挡下
（`DefaultMQPushConsumerImpl:1116`）。小池子必须**两个一起压**（脚本在三处都补上了 min），
否则看到的是启动失败，而不是"并发度调小了"。

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

`CONSUME_FROM_FIRST_OFFSET` 的起点语义（Java `RebalancePushImpl:197-208` / `RebalanceLitePullImpl:114-124`，
两支同形）：位点表里没有已提交位点时起点就是**字面量 0**，**不**发 `GET_MIN_OFFSET(31)` 查询
（上游那句注释是 `//the offset will be fixed by the OFFSET_ILLEGAL process`：真要越界时 broker
会在拉取应答里用 `nextBeginOffset` 把位点纠回来）。多打这一枪不只是慢：minOffset 属
`MQAdminImpl` 口径、只认 master（见 `verify_publish_route_master_live.py`），**主掉线期间
恰好是「新起的消费者一条都拉不到」**，而离线夹具里那笔 `GET_MIN_OFFSET` 永远有应答、断言
全绿 —— 这条分歧只有真机停 master 才看得见。`tests/test_lite_pull_consumer.py` 的
`test_assign_before_start_leaves_the_start_point_to_start` 锁住 lite pull 的另一半：
先 `assign` 再 `start()` 时不许在 assign 那一刻就把起点落成 0（拉取循环只在缺值时补解析，
写进去就永久钉死），`start()` 之后真去问了 broker、交付的第一条正是已提交位点的下一条。
⚠ `DefaultLitePullConsumerImpl.seekToBegin:697-700` 是同一话题里的**例外**：`seek_to_begin()`
真会调 `minOffset`，这里照抄不改。

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
│   ├── boundary_type.py       时间戳查位点的边界语义（LOWER/UPPER，含 getType 宽松解析）
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

## 推送消费者启动期校验（`consumer.py:_check_config_ranges()`）

对齐 Java `DefaultMQPushConsumerImpl#checkConfig` 的数值段（`:1099-1209`）：13 条区间在
`start()` 里逐条检查，比较一律 `< lo or > hi`（两端闭区），文案逐字照抄 Java、只去掉
`FAQUrl.suggest_todo()` 尾巴（本移植没有 FAQ 短链服务）。要点：

- `pullThresholdForTopic` / `pullThresholdSizeForTopic` 的 `-1` 是"关闭"哨兵（Java 用
  `if (x != -1)` 包住整条检查），**其余闸门没有这层豁免**，`-1` 照拒；
- `pullInterval` 的下界是 **0**（Java 原文如此，0 = 不间隔），别照抄邻居闸门的 1；
- `pullThresholdSizeForQueue` / `pullThresholdSizeForTopic` 的单位是 **MiB**；
- `consumeThreadMin > consumeThreadMax` 是**严格大于**（相等合法，Java 允许单线程消费者），
  消息里带上两个数值；
- `popBatchNums` 跟随 Java 字面的 `<= 0`，文案仍写 `[1, 32]`；
- 校验排在所有 null 检查之后、`MQClientInstance` 建连之前 —— 坏配置必须在注册 clientId 之前
  失败，否则 broker 的 `ConsumerManager` 会留下一堆永不心跳的僵尸 clientId，把 rebalance 用的
  `cidAll` 撑歪（真机表现为队列分配不均，而客户端日志里只有启动失败那一条）。

Java `:1058` 的 `consumeTimestamp` 格式校验本端口**会真拒**（`consume_timestamp` 是可配的
`%Y%m%d%H%M%S` 字符串），这是与 .NET 的一处差异（那边没有这个可配项）。回归守卫：
`tests/test_consumer_check_config.py`（57 项，逐条锁区间两端、`-1` 哨兵、检查顺序与文案）；
真机守卫见上面 `verify_flow_control_live.py` 的 S5。C++ / Rust / .NET 用同一张闸门表、同一段
顺序、同一条文案（`cpp/tests/test_consumer_check_config.cpp`、`rust/src/client/consumer.rs` 的
`RANGE_GATES`、`dotnet/tests/RocketMQ.Client.Tests/ConsumerCheckConfigTests.cs`）。

## 定时任务周期（pollNameServerInterval / persistConsumerOffsetInterval）

`MQClientInstance` 的后台周期任务逐条对齐 Java `MQClientInstance#startScheduledTask`
（`:389-432`）。**每个循环的首跳都落在 `initialDelay` 这一刻**，不是 `initialDelay + period`
—— 对应 `scheduleAtFixedRate` 的语义（真机实测抓到过本端口按后者排的错误，见下）：

| 循环 | Java 锚点 | initialDelay | 周期 | 可配字段（默认） |
|------|-----------|--------------|------|------------------|
| 动态 name server 刷新 | `:390-398` | 10s | 2min | —（仅未配置静态地址且有地址服务器时调度） |
| 在用 topic 路由刷新 | `:400-406` | 10ms | `pollNameServerInterval` | `poll_name_server_interval`（30000ms） |
| 心跳 | `:408-415` | 1s | `heartbeatBrokerInterval` | —（见下） |
| 消费者位点落盘 | `:417-423` | 10s | `persistConsumerOffsetInterval` | `persist_consumer_offset_interval`（5000ms） |
| 线程池弹性巡检 | `:425-431` | 1min | 1min | —（inc/dec 本是空实现） |

要点：

- `poll_name_server_interval` **五个门面都有**（producer / push / pull / lite / admin，与 Java
  一样继承自 `ClientConfig`，`ClientConfig:58`），并在各自 `start()` 里透传给
  `MQClientInstance`；周期只在循环入口读一次，之后再改字段不影响已排定的任务
  （对齐 `scheduleAtFixedRate` 一次性排定）。
- `persist_consumer_offset_interval` 只管**后台周期落盘**；`shutdown()` 里的收尾落盘是
  无条件的（`DefaultMQPushConsumerImpl#shutdown` 先 `persistConsumerOffset` 再停服务），
  所以调大周期只推迟落盘时机、不丢位点。集群模式下位点落盘走**同步**
  `UPDATE_CONSUMER_OFFSET`，广播模式写本地 `~/.rocketmq_offsets/<clientId>/<group>/offsets.json`。
- 心跳有一处**已知且有意的偏差**：Python 在 `start()` 里先同步打一轮心跳，之后按 30s 周期跑；
  Java 的循环首跳在 1s。首轮同步心跳覆盖了 Java 首跳的作用（注册 clientId 后立刻可见），
  周期与 Java 默认值一致，回归守卫 `tests/test_scheduled_intervals.py` 锁的是这个口径。
- 上述四条（动态地址 / 路由 / 落盘 / 弹性）的周期与首跳次序由
  `tests/test_scheduled_intervals.py`（12 项，用 `_RecordingStop` 记录 `wait()` 实参）逐条锁死；
  真机守卫 `verify_interval_live.py` 用两条不同周期的实例对照，证明差异来自周期本身
  而不是路由缓存或 broker 行为（I1 快慢组 1.06s vs 30.20s，I2 首笔落盘 10.18s × 双组、
  第二批 0.83s vs 60s 组仍在原地）。

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

时间戳查位点带 **`boundaryType`**（`SearchOffsetRequestHeader:41`）：admin 的两个边界入口
`search_lower_boundary_offset` / `search_upper_boundary_offset`（`DefaultMQAdminExt:133/:137`）
分别固定发 `LOWER` / `UPPER`，`search_offset` 等价于 LOWER（`MQAdminImpl:189`）；MQ 级
`search_offset_by_timestamp` 默认也是显式 LOWER（`MQClientAPIImpl:1381`），传
`boundary_type=None` 则整键不写，复现已废弃的 5 参重载（`:1352`）的报文。入网文本是
`Enum.toString()` 的**大写枚举名**（`RemotingCommand.makeCustomHeaderToNet:430`），
`BoundaryType.getName()` 的小写名只喂给 `getType` 做比对、从不上报文；字段 `@CFNullable`，
broker 缺键回落 LOWER，有键但值不认识（`getType:41` 只认 `equalsIgnoreCase("upper")`）同样回落。
真机判别式（`verify_admin_live.py` 第 7.5 节）：1 队列 topic 发 3 条后对远未来时间戳查位点，
LOWER = maxOffset(3)、UPPER = maxOffset-1(2) —— 两个数不同即证明字段真的到了 broker 并被解析
（字段丢失或两边都按 LOWER 处理时两个数必然相等）。报文形状（含 `boundary_type=None` 时
**整键不写**、缺键回 `None`、未知值宽松回落 LOWER）由 `tests/test_search_offset_boundary.py`
（7 项，离线抓 `make_custom_header_to_net()` 的 extFields）锁死。

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
