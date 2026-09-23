# 自检二进制：协议层往返自检（不依赖真实集群）
add_executable(rmq_selfcheck selfcheck.cpp)
target_link_libraries(rmq_selfcheck PRIVATE rocketmq::remoting)

# 互操作工具：与 Python 参考客户端做跨语言编解码交叉验证
add_executable(rmq_interop interop_tool.cpp)
target_link_libraries(rmq_interop PRIVATE rocketmq::remoting)

# 真实集群消息类型联调（需先起 nameServer+broker；不打进 ctest）
add_executable(rmq_live_message_types live_message_types.cpp)
target_link_libraries(rmq_live_message_types PRIVATE rocketmq::remoting)

add_executable(rmq_live_redelivery live_redelivery.cpp)
target_link_libraries(rmq_live_redelivery PRIVATE rocketmq::remoting)

# 真实集群管理端联调（需先起 nameServer+broker；不打进 ctest）
add_executable(rmq_admin_live admin_live.cpp)
target_link_libraries(rmq_admin_live PRIVATE rocketmq::remoting)

# 真实集群自动压缩联调（自产自销 + 与 Java 探针跨客户端互通；不打进 ctest）
add_executable(rmq_compression_live compression_live.cpp)
target_link_libraries(rmq_compression_live PRIVATE rocketmq::remoting)

# ACL 鉴权真机联调（需 broker 开 authenticationEnabled；不打进 ctest）
add_executable(rmq_live_acl live_acl.cpp)
target_link_libraries(rmq_live_acl PRIVATE rocketmq::remoting)

# TLS + traceparent 真机联调（需 broker 开 tls.test.mode.enable；不打进 ctest）
add_executable(rmq_live_tls live_tls.cpp)
target_link_libraries(rmq_live_tls PRIVATE rocketmq::remoting)

# 主动拉取消费者（DefaultMQPullConsumer）真机联调
add_executable(rmq_live_pull live_pull.cpp)
target_link_libraries(rmq_live_pull PRIVATE rocketmq::remoting)

# 轻量拉取消费者（DefaultLitePullConsumer）真机联调：subscribe/assign 双模式 + poll
add_executable(rmq_live_lite_pull live_lite_pull.cpp)
target_link_libraries(rmq_live_lite_pull PRIVATE rocketmq::remoting)

# Request-Reply（5.x）真机联调（请求 → 应答 → 326 推回；不打进 ctest）
add_executable(rmq_live_request_reply live_rr.cpp)
target_link_libraries(rmq_live_request_reply PRIVATE rocketmq::remoting)

# 发送延迟故障容错真机联调（默认关/开、隔离退化链、到期恢复；不打进 ctest）
add_executable(rmq_live_latency live_latency.cpp)
target_link_libraries(rmq_live_latency PRIVATE rocketmq::remoting)

# POP 模式（5.x 轻量消费）真机联调：POP/ACK/延长不可见时间/复活重投（不打进 ctest）
add_executable(rmq_live_pop live_pop.cpp)
target_link_libraries(rmq_live_pop PRIVATE rocketmq::remoting)

# POP **消费侧**真机联调（消费循环 + ack + 延长不可见时间；不打进 ctest）
add_executable(rmq_live_pop_consumer live_pop_consumer.cpp)
target_link_libraries(rmq_live_pop_consumer PRIVATE rocketmq::remoting)

# 消息轨迹真机联调（需 broker traceTopicEnable=true；不打进 ctest）
add_executable(rmq_live_trace live_trace.cpp)
target_link_libraries(rmq_live_trace PRIVATE rocketmq::remoting)

# 钩子真机联调：CheckForbiddenHook 拦截 + FilterMessageHook 拉取/POP 两条路径（不打进 ctest）
add_executable(rmq_live_hook live_hook.cpp)
target_link_libraries(rmq_live_hook PRIVATE rocketmq::remoting)


# 名字校验（Validators/TopicValidator）真机联调：本地快拒 + 合法名字照常收发 + 往返对照（不打进 ctest）
add_executable(rmq_validators_live validators_live.cpp)
target_link_libraries(rmq_validators_live PRIVATE rocketmq::remoting)

# 定时消息撤回（370）真机联调：句柄往返 + 撤回后到点不投递 + broker 开关还原（不打进 ctest）
add_executable(rmq_recall_live recall_live.cpp)
target_link_libraries(rmq_recall_live PRIVATE rocketmq::remoting)

# unitName / unitMode / enableStreamRequestType 真机联调：clientId 后缀 + broker 侧单元位（不打进 ctest）
add_executable(rmq_live_unit_config live_unit_config.cpp)
target_link_libraries(rmq_live_unit_config PRIVATE rocketmq::remoting)

# SQL92 过滤 + CHECK_CLIENT_CONFIG(46) 真机联调（需 broker enablePropertyFilter=true；不打进 ctest）
add_executable(rmq_sql92_live sql92_live.cpp)
target_link_libraries(rmq_sql92_live PRIVATE rocketmq::remoting)

# 异步发送背压（两个公平信号量）真机联调：限流文案 + broker 侧对账 + 运行时扩容（不打进 ctest）
add_executable(rmq_live_backpressure live_backpressure.cpp)
target_link_libraries(rmq_live_backpressure PRIVATE rocketmq::remoting)

# 异步发送内核（准备段/回调线程口径、并发不串台、定点/拦截/批量/关池排空）真机联调（不打进 ctest）
add_executable(rmq_live_async_send live_async_send.cpp)
target_link_libraries(rmq_live_async_send PRIVATE rocketmq::remoting)

# 发送头 c/d/n 三个字段真机联调（模板 topic 决定自动建出来的队列数；不打进 ctest）
add_executable(rmq_live_send_header live_send_header.cpp)
target_link_libraries(rmq_live_send_header PRIVATE rocketmq::remoting)
