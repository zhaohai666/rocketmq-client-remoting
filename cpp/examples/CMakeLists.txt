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

# 主动拉取消费者（DefaultMQPullConsumer）真机联调
add_executable(rmq_live_pull live_pull.cpp)
target_link_libraries(rmq_live_pull PRIVATE rocketmq::remoting)

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

