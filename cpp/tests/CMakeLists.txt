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
