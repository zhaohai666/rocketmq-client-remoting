# 单元测试：编解码往返 + Java 对齐守卫
add_executable(rmq_test_codec test_codec.cpp)
target_link_libraries(rmq_test_codec PRIVATE rocketmq::remoting)
add_test(NAME codec COMMAND rmq_test_codec)

# clientId 口径：Java `ClientConfig#buildMQClientId` 的 IP@instanceName + changeInstanceNameToPID
add_executable(rmq_test_client_id test_client_id.cpp)
target_link_libraries(rmq_test_client_id PRIVATE rocketmq::remoting)
add_test(NAME client_id COMMAND rmq_test_client_id)

add_executable(rmq_test_java_alignment test_java_alignment.cpp)
target_link_libraries(rmq_test_java_alignment PRIVATE rocketmq::remoting)
add_test(NAME java_alignment COMMAND rmq_test_java_alignment)

# 路由 / 心跳 / 订阅数据：往返 + Java 字段名守卫
add_executable(rmq_test_route_heartbeat test_route_heartbeat.cpp)
target_link_libraries(rmq_test_route_heartbeat PRIVATE rocketmq::remoting)
add_test(NAME route_heartbeat COMMAND rmq_test_route_heartbeat)

# 传输层：本机真实 TCP 收发（半包重组 / 超时 / 建连失败）
add_executable(rmq_test_transport test_transport.cpp)
target_link_libraries(rmq_test_transport PRIVATE rocketmq::remoting)
add_test(NAME transport COMMAND rmq_test_transport)

# 断连立刻失败在途请求（Java failFast / requestFail）：真 socket 对端读完就关，
# 断言"毫秒级判死 + 报 SEND_REQUEST 而不是超时 + 回调只一次 + 不牵连别人家的请求"
add_executable(rmq_test_fail_fast test_fail_fast.cpp)
target_link_libraries(rmq_test_fail_fast PRIVATE rocketmq::remoting)
add_test(NAME fail_fast COMMAND rmq_test_fail_fast)

# 压缩：zlib 往返 + 外部夹具交叉验证 + 失败必须显式抛错
add_executable(rmq_test_compression test_compression.cpp)
target_link_libraries(rmq_test_compression PRIVATE rocketmq::remoting)
add_test(NAME compression COMMAND rmq_test_compression)

# 管理端：fastjson2 非法 JSON 容忍 + 管理响应体 DTO + TopicConfig/订阅组字段名与默认值
add_executable(rmq_test_admin test_admin.cpp)
target_link_libraries(rmq_test_admin PRIVATE rocketmq::remoting)
add_test(NAME admin COMMAND rmq_test_admin)

# 客户端日志：行格式（毫秒/pid/线程名/文件:行）+ 按大小轮转 + maxIndex 上限 + 级别过滤
add_executable(rmq_test_logging test_logging.cpp)
target_link_libraries(rmq_test_logging PRIVATE rocketmq::remoting)
add_test(NAME logging COMMAND rmq_test_logging)

# ACL 鉴权：SHA1/HMAC/Base64 标准向量 + 签名内容与签名值对拍 **Java 官方实现**
add_executable(rmq_test_acl test_acl.cpp)
target_link_libraries(rmq_test_acl PRIVATE rocketmq::remoting)
add_test(NAME acl COMMAND rmq_test_acl)

# Request-Reply（5.x）：等待槽语义 + createReplyMessage 形状 + 326 回推入口（含迟到/坏包）
add_executable(rmq_test_request_reply test_request_reply.cpp)
target_link_libraries(rmq_test_request_reply PRIVATE rocketmq::remoting)
add_test(NAME request_reply COMMAND rmq_test_request_reply)

# 发送延迟故障容错：阈值表逐档映射 + 隔离/到期恢复 + 选择退化链（available→reachable→轮询）
add_executable(rmq_test_latency test_latency.cpp)
target_link_libraries(rmq_test_latency PRIVATE rocketmq::remoting)
add_test(NAME latency COMMAND rmq_test_latency)

# 发送重试链路：mock name server + broker，锁死 sendDefaultImpl 的重试分类语义
add_executable(rmq_test_send_retry test_send_retry.cpp)
target_link_libraries(rmq_test_send_retry PRIVATE rocketmq::remoting)
add_test(NAME send_retry COMMAND rmq_test_send_retry)

# 异步发送链路：AsyncSenderExecutor / NettyClientPublicExecutor 两个池 + 按异常类型分类的
# 重试链（Java sendMessageAsync/onExceptionImpl），以及有界队列的 executor rejected
add_executable(rmq_test_producer_async test_producer_async.cpp)
target_link_libraries(rmq_test_producer_async PRIVATE rocketmq::remoting)
add_test(NAME producer_async COMMAND rmq_test_producer_async)

# 异步发送背压基元：公平（只有队首能拿）+ 运行时原地改容量（在途保留、叫醒等待者）
add_executable(rmq_test_backpressure test_backpressure.cpp)
target_link_libraries(rmq_test_backpressure PRIVATE rocketmq::remoting)
add_test(NAME backpressure COMMAND rmq_test_backpressure)

# POP 模式：CK 串编解码 + 请求头字段名守卫 + POP_CK 反构
add_executable(rmq_test_pop test_pop.cpp)
target_link_libraries(rmq_test_pop PRIVATE rocketmq::remoting)
add_test(NAME pop COMMAND rmq_test_pop)

# POP **消费侧**（pop_mode 消费循环）本地单测
add_executable(rmq_test_pop_consumer test_pop_consumer.cpp)
target_link_libraries(rmq_test_pop_consumer PRIVATE rocketmq::remoting)
add_test(NAME pop_consumer COMMAND rmq_test_pop_consumer)

# POP + 顺序监听器（Java ConsumeMessagePopOrderlyService，5.5.0 未完成骨架）：
# 分派进骨架后 listener 不被调、不 ack，请求集按 (pq, mq) 去重、pq 撤销才摘
add_executable(rmq_test_pop_orderly test_pop_orderly.cpp)
target_link_libraries(rmq_test_pop_orderly PRIVATE rocketmq::remoting)
add_test(NAME pop_orderly COMMAND rmq_test_pop_orderly)

# classic 并发消费的 ackIndex 语义（Java processConsumeResult）：部分 ack 的尾巴回投、
# 回投失败钳住位点、RECONSUME_LATER 覆盖宽 ackIndex、广播丢尾巴
add_executable(rmq_test_consume_ack_index test_consume_ack_index.cpp)
target_link_libraries(rmq_test_consume_ack_index PRIVATE rocketmq::remoting)
add_test(NAME consume_ack_index COMMAND rmq_test_consume_ack_index)

# 顺序消费的重投闸门（Java ConsumeMessageOrderlyService:236-362）：-1 的两套含义、
# 本地 +1、用尽后回投那条消息的字段、回投失败仍挂起，以及 %RETRY% 属性抬进发送头
add_executable(rmq_test_orderly_reconsume test_orderly_reconsume.cpp)
target_link_libraries(rmq_test_orderly_reconsume PRIVATE rocketmq::remoting)
add_test(NAME orderly_reconsume COMMAND rmq_test_orderly_reconsume)

# 消息轨迹：Java 逐字节对拍向量 + javaSplit 语义 + 无 keys SubBefore / 坏记录隔离回归
add_executable(rmq_test_trace test_trace.cpp)
target_link_libraries(rmq_test_trace PRIVATE rocketmq::remoting)
add_test(NAME trace COMMAND rmq_test_trace)

# 钩子：CheckForbiddenHook（不吞异常）与 FilterMessageHook（异常吞掉 + 拉取/POP 差异）
# 以及订阅语义 tagsSet/codeSet 的 Java 对拍向量
add_executable(rmq_test_hook test_hook.cpp)
target_link_libraries(rmq_test_hook PRIVATE rocketmq::remoting)
add_test(NAME hook COMMAND rmq_test_hook)

# 消费线程弹性：ConsumeExecutor（core/max 两档）+ updateCorePoolSize 五条守卫 +
# adjustThreadPool 的上游 no-op 语义 + msgAccCnt
add_executable(rmq_test_consume_thread_pool test_consume_thread_pool.cpp)
target_link_libraries(rmq_test_consume_thread_pool PRIVATE rocketmq::remoting)
add_test(NAME consume_thread_pool COMMAND rmq_test_consume_thread_pool)

# 动态 name server：DefaultTopAddressing 的 URL 规则 / clearNewLine / mock 地址服务器
add_executable(rmq_test_top_addressing test_top_addressing.cpp)
target_link_libraries(rmq_test_top_addressing PRIVATE rocketmq::remoting)
add_test(NAME top_addressing COMMAND rmq_test_top_addressing)

add_executable(rmq_test_consumer_stats test_consumer_stats.cpp)
target_link_libraries(rmq_test_consumer_stats PRIVATE rocketmq::remoting)
add_test(NAME consumer_stats COMMAND rmq_test_consumer_stats)

# W3C traceparent：生成/校验/子 span/注入不覆盖/属性提取
add_executable(rmq_test_trace_context test_trace_context.cpp)
target_link_libraries(rmq_test_trace_context PRIVATE rocketmq::remoting)
add_test(NAME trace_context COMMAND rmq_test_trace_context)

# 跨语言互操作（C++ <-> Python 参考客户端）。依赖 python 侧 venv，不存在则跳过。
set(RMQ_PY_VENV "${CMAKE_CURRENT_SOURCE_DIR}/../../python/.venv/bin/python")
if(EXISTS "${RMQ_PY_VENV}")
    add_test(NAME interop
             COMMAND ${RMQ_PY_VENV} ${CMAKE_CURRENT_SOURCE_DIR}/interop_check.py
                     $<TARGET_FILE:rmq_interop>)
endif()

# 轻量拉取消费者（DefaultLitePullConsumer）：start 前纯状态行为 + 221/309 body wire 形状
add_executable(rmq_test_lite_pull test_lite_pull.cpp)
target_link_libraries(rmq_test_lite_pull PRIVATE rocketmq::remoting)
add_test(NAME lite_pull COMMAND rmq_test_lite_pull)

# 轻量拉取游标（Java updatePullOffset:982-998）：每轮成功拉取都跟随 nextBeginOffset，
# 不只在 FOUND；顺带锁死 FIRST_OFFSET 不发 GET_MIN_OFFSET（帧内假集群，非 mock）
add_executable(rmq_test_lite_pull_cursor test_lite_pull_cursor.cpp)
target_link_libraries(rmq_test_lite_pull_cursor PRIVATE rocketmq::remoting)
add_test(NAME lite_pull_cursor COMMAND rmq_test_lite_pull_cursor)

# 队列分配策略（AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM /
# MACHINE_ROOM_NEARBY）：Java 单测对拍 + 守卫 + 消费者可替换
add_executable(rmq_test_allocate_strategy test_allocate_strategy.cpp)
target_link_libraries(rmq_test_allocate_strategy PRIVATE rocketmq::remoting)
add_test(NAME allocate_strategy COMMAND rmq_test_allocate_strategy)

# 一致性哈希环（CONSISTENT_HASH 的底座）：MD5 官方向量 + Java 落点/回绕/replica 口径
add_executable(rmq_test_consistent_hash test_consistent_hash.cpp)
target_link_libraries(rmq_test_consistent_hash PRIVATE rocketmq::remoting)
add_test(NAME consistent_hash COMMAND rmq_test_consistent_hash)

# Validators fast-fail：topic/group 字符表与长度 + checkMessage 文案/顺序/响应码 +
# start() 组名守卫（非法/保留组不碰网络即失败）
add_executable(rmq_test_validators test_validators.cpp)
target_link_libraries(rmq_test_validators PRIVATE rocketmq::remoting)
add_test(NAME validators COMMAND rmq_test_validators)

# broker 反向推送 40 NOTIFY_CONSUMER_IDS_CHANGED：实例级处理器 + 整组唤醒 + 计数
#（真机用例注入不了反向请求，只能离线把分派锁死）
add_executable(rmq_test_broker_requests test_broker_requests.cpp)
target_link_libraries(rmq_test_broker_requests PRIVATE rocketmq::remoting)
add_test(NAME broker_requests COMMAND rmq_test_broker_requests)

# 定时消息撤回（370）：句柄编解码对拍 Java 向量 + bname 键名 + 本地校验顺序（不打网络）
add_executable(rmq_test_recall_message test_recall_message.cpp)
target_link_libraries(rmq_test_recall_message PRIVATE rocketmq::remoting)
add_test(NAME recall_message COMMAND rmq_test_recall_message)

# CHECK_CLIENT_CONFIG(46)：mock broker 锁死协议形状与分支语义
#（SQL92 写错时 broker 静默放行全部消息，只有这笔请求能把它变成启动错误；真机见 rmq_sql92_live）
add_executable(rmq_test_check_client_config test_check_client_config.cpp)
target_link_libraries(rmq_test_check_client_config PRIVATE rocketmq::remoting)
add_test(NAME check_client_config COMMAND rmq_test_check_client_config)

# isPullExpired 停摆自愈：锁阈值算术与逐队列时刻表（判错方向要么静默不消费、要么凭空重投）。
# 「撤掉之后真能重新消费」要起真线程，只能在真机验（examples/live_consumer.cpp 的停摆场景）。
add_executable(rmq_test_pull_expired test_pull_expired.cpp)
target_link_libraries(rmq_test_pull_expired PRIVATE rocketmq::remoting)
add_test(NAME pull_expired COMMAND rmq_test_pull_expired)

# 拉取前流控五个阈值（Java ProcessQueue）：只有条数那条会在小消息场景先命中，其余四条
# 判错方向是静默的（字节/跨度/topic 级闸门失效 ⇒ 缓冲无限堆），必须离线锁死再上真机。
add_executable(rmq_test_flow_control test_flow_control.cpp)
target_link_libraries(rmq_test_flow_control PRIVATE rocketmq::remoting)
add_test(NAME flow_control COMMAND rmq_test_flow_control)

# 退出注销 UNREGISTER_CLIENT(35)：mock broker 锁死头的形状（空白组槽位整个不上线）与
# 「每台 broker 含从节点」的扇出（broker 端判的是 group != null，真机看不出来）。
# 真机侧的证据在 examples/live_producer_unregister.cpp（抓帧 + 204 前后对比）。
add_executable(rmq_test_producer_unregister test_producer_unregister.cpp)
target_link_libraries(rmq_test_producer_unregister PRIVATE rocketmq::remoting)
add_test(NAME producer_unregister COMMAND rmq_test_producer_unregister)

# 推送消费者配置数值闸门（Java checkConfig :1099-1209）：十三道区间的边界值、越界文案、
# min/max 相对关系与"坏配置在建 MQClientInstance 之前就被拒"（地址指向没人监听的端口，
# 报的必须是配置文案而不是连接失败）。真机侧的"边界值真能启动并消费 + broker 查无此组"
# 在 examples/live_flow_control.cpp 的 S5。
add_executable(rmq_test_consumer_check_config test_consumer_check_config.cpp)
target_link_libraries(rmq_test_consumer_check_config PRIVATE rocketmq::remoting)
add_test(NAME consumer_check_config COMMAND rmq_test_consumer_check_config)

# 订阅后置 + 立即心跳（Java DefaultMQPushConsumerImpl#subscribe:1265-1287）：
# start() 之后 subscribe 不再抛 already started，且立刻进活订阅表；unsubscribe 只删表项。
# 消费者对着连不上的 name server 启动（零 broker ⇒ 心跳一台都发不出去），
# 报文层面「broker 真收到带新订阅的心跳」由 examples/live_subscribe.cpp 真机验证。
add_executable(rmq_test_subscribe_after_start test_subscribe_after_start.cpp)
target_link_libraries(rmq_test_subscribe_after_start PRIVATE rocketmq::remoting)
add_test(NAME subscribe_after_start COMMAND rmq_test_subscribe_after_start)

# 广播本地位点文件（Java LocalFileOffsetStore）：fastjson2「对象作 key」格式与 Java
# 逐字节对拍 + .bak 滚动 + 主文件缺失回退 .bak（四端互认 ~/.rocketmq_offsets 的底座）
add_executable(rmq_test_local_offsets test_local_offsets.cpp)
target_link_libraries(rmq_test_local_offsets PRIVATE rocketmq::remoting)
add_test(NAME local_offsets COMMAND rmq_test_local_offsets)

# 定时任务节奏（Java MQClientInstance#startScheduledTask :389-432）：五个门面的
# pollNameServerInterval=30000 / persistConsumerOffsetInterval=5000 默认值与透传 +
# 「首跳落在 initialDelay（10ms）而不是 initialDelay+周期」+ 周期真按传入值走。
# 假 name server 记录 GET_ROUTEINFO_BY_TOPIC 的到达时刻 —— 这两个偏差在真机上只
# 表现为「慢」，不表现为「错」，只有计时断言能抓住。
add_executable(rmq_test_scheduled_intervals test_scheduled_intervals.cpp)
target_link_libraries(rmq_test_scheduled_intervals PRIVATE rocketmq::remoting)
add_test(NAME scheduled_intervals COMMAND rmq_test_scheduled_intervals)

# postSubscriptionWhenPull（Java DefaultMQPushConsumer 默认 false）+ pullFromWhichNode
# （Java PullAPIWrapper#recalculatePullFromWhichNode / #updatePullFromWhichNode）：
# findBrokerAddressInSubscribe 的四个分支、按 brokerId 选主/从的真实 socket 报文、
# 从节点清 COMMIT_OFFSET 位（:219-221）、SUBSCRIPTION 位关闭时 subscription 键不上线、
# 以及从节点缺席时回退主节点。与 python/tests/test_pull_post_subscription.py 同题。
add_executable(rmq_test_pull_post_subscription test_pull_post_subscription.cpp)
target_link_libraries(rmq_test_pull_post_subscription PRIVATE rocketmq::remoting)
add_test(NAME pull_post_subscription COMMAND rmq_test_pull_post_subscription)

# 生产者自动攒批（Java ProduceAccumulator，526 行）：归并键四元组 / 批量 body 组装的
# ID 顺序 / 四组批级属性口径 / 全局字节闸门 / 守卫线程对空批的清理 / 拆分批量应答 /
# 同步阻塞语义 / 异步回调与额度归还（含 Java 的漏还路径）/ sender 生命周期。
# 与 python/tests/test_produce_accumulator.py、.NET ProduceAccumulatorTests.cs、
# rust/src/client/produce_accumulator/tests.rs 同题。
add_executable(rmq_test_produce_accumulator test_produce_accumulator.cpp)
target_link_libraries(rmq_test_produce_accumulator PRIVATE rocketmq::remoting)
add_test(NAME produce_accumulator COMMAND rmq_test_produce_accumulator)

# correctTagsOffset（Java DefaultMQPushConsumerImpl:713-717，调用点 :394-401）：
# 空应答（NO_NEW_MSG / NO_MATCHED_MSG）且 ProcessQueue 为空时把"已消费位点"抬到
# 应答的 nextBeginOffset（increaseOnly）；闸门 = pending 为空 **且** 在途批次为 0
#（Java msgCount 要等 listener 返回才减）。漏了它位点永久落后、闸门写松则崩溃时静默跳消息。
# 与 python/tests/test_correct_tags_offset.py、rust/src/client/consumer.rs 同名测试同题。
add_executable(rmq_test_correct_tags_offset test_correct_tags_offset.cpp)
target_link_libraries(rmq_test_correct_tags_offset PRIVATE rocketmq::remoting)
add_test(NAME correct_tags_offset COMMAND rmq_test_correct_tags_offset)

# OFFSET_ILLEGAL 纠错（Java DefaultMQPushConsumerImpl:402-427）：位点被 broker 纠正时
# 覆盖写修正值并冻结（ControllableOffset allowToUpdate=false）、撤队列（setDropped）、
# 立刻落盘（persist）、叫醒 rebalance 重建。冻结/代号写错是静默的两种极端：位点与 broker
# 来回弹跳，或崩溃后从非法位点重启、纠错白做。
# 与 python/tests/test_offset_illegal_recover.py、rust/src/client/consumer.rs 同名测试同题。
add_executable(rmq_test_offset_illegal_recover test_offset_illegal_recover.cpp)
target_link_libraries(rmq_test_offset_illegal_recover PRIVATE rocketmq::remoting)
add_test(NAME offset_illegal_recover COMMAND rmq_test_offset_illegal_recover)

# 220 RESET_CONSUMER_CLIENT_OFFSET（Java MQClientInstance.resetOffset:1403-1450）：
# map/ForC 两种请求体形状（Java 发起方恒为 JAVA ⇒ 只推 map；language=CPP 的旧 C++ SDK
# 管理端让 broker 推数组，本端口两种都认）+ 重置 = 撤队列（代号 +1 丢掉在途批次）+
# 新位点经撤销尾巴落盘（广播本地位点文件里必须是新值、且与兄弟队列 merge）。
# 只写内存表的实现在真机上是静默失效的（旧批次 ack 把位点推回原处），必须离线锁死。
# 与 python/tests/test_reset_offset_handler.py、rust/src/client/consumer.rs 的同名测试、
# dotnet/tests/RocketMQ.Client.Tests/ResetOffsetTests.cs 同题。
add_executable(rmq_test_reset_offset test_reset_offset.cpp)
target_link_libraries(rmq_test_reset_offset PRIVATE rocketmq::remoting)
add_test(NAME reset_offset COMMAND rmq_test_reset_offset)

# 拉模式消费者心跳（#98）：Java DefaultMQPullConsumerImpl 靠实例级周期任务把本组
# 注册给 broker（consumeType=CONSUME_ACTIVELY :348、consumeFromWhere=CONSUME_FROM_LAST_OFFSET
# :353、订阅集 setSubVersion(0L) :357-385）；轻量拉取同样 ACTIVELY（:1111-1112）。
# 判据取线上报文：假端点收 HEART_BEAT 的 JSON body 与 UNREGISTER_CLIENT(35) 的 extFields。
# 与 python/tests/test_pull_consumer.py、python/tests/test_lite_pull_consumer.py 同题。
add_executable(rmq_test_pull_consumer_heartbeat test_pull_consumer_heartbeat.cpp)
target_link_libraries(rmq_test_pull_consumer_heartbeat PRIVATE rocketmq::remoting)
add_test(NAME pull_consumer_heartbeat COMMAND rmq_test_pull_consumer_heartbeat)

# 发布地址「只要 master」（#100，Java MQClientInstance#findBrokerAddressInPublish:1295-1305）：
# 平表由真的 updateTopicRouteInfoFromNameServer 写入（:962-964）、查不到先刷路由再查
# （sendKernelImpl:919-924）、仍拿不到抛 "The broker[X] not exist" 且码为 -1，以及
# 管理 offset 查询同口径（MQAdminImpl:195/214/232/250）。
# 队列集的筛选（:301-303）在 test_route_heartbeat.cpp 里；这条边界是
# 「本端立刻报错」与「把写请求打到从节点上白挨一轮 SYSTEM_BUSY(2)」的分水岭。
# 与 python/tests/test_publish_route_master.py 的地址侧同题。
add_executable(rmq_test_publish_route_master test_publish_route_master.cpp)
target_link_libraries(rmq_test_publish_route_master PRIVATE rocketmq::remoting)
add_test(NAME publish_route_master COMMAND rmq_test_publish_route_master)

# cleanExpiredMsg 挂起逃生口（Java ConsumeMessageConcurrentlyService:68-88/192-200 +
# ProcessQueue.cleanExpiredMsg:75-127）：listener 卡死不返回时唯一的回收路径。离线锁
# 「只看队首 / 严格大于阈值 / 单轮 16 条 / 回投失败留在原地 / 撤走的队列不再被扫」与
# 消费路径的配合（在途登记、containsMessage 闸门、隔轮不泄漏）；
# 「回投成功 → %RETRY% 重投 → reconsumeTimes=1 的第二次投递」只能在真机验
#（examples/live_clean_expired_msg.cpp）。与 python/tests/test_clean_expired_msg.py 同题。
add_executable(rmq_test_clean_expired_msg test_clean_expired_msg.cpp)
target_link_libraries(rmq_test_clean_expired_msg PRIVATE rocketmq::remoting)
add_test(NAME clean_expired_msg COMMAND rmq_test_clean_expired_msg)
