# 单元测试：编解码往返 + Java 对齐守卫
add_executable(rmq_test_codec test_codec.cpp)
target_link_libraries(rmq_test_codec PRIVATE rocketmq::remoting)
add_test(NAME codec COMMAND rmq_test_codec)

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

# POP 模式：CK 串编解码 + 请求头字段名守卫 + POP_CK 反构
add_executable(rmq_test_pop test_pop.cpp)
target_link_libraries(rmq_test_pop PRIVATE rocketmq::remoting)
add_test(NAME pop COMMAND rmq_test_pop)

# POP **消费侧**（pop_mode 消费循环）本地单测
add_executable(rmq_test_pop_consumer test_pop_consumer.cpp)
target_link_libraries(rmq_test_pop_consumer PRIVATE rocketmq::remoting)
add_test(NAME pop_consumer COMMAND rmq_test_pop_consumer)

# 消息轨迹：Java 逐字节对拍向量 + javaSplit 语义 + 无 keys SubBefore / 坏记录隔离回归
add_executable(rmq_test_trace test_trace.cpp)
target_link_libraries(rmq_test_trace PRIVATE rocketmq::remoting)
add_test(NAME trace COMMAND rmq_test_trace)

# 跨语言互操作（C++ <-> Python 参考客户端）。依赖 python 侧 venv，不存在则跳过。
set(RMQ_PY_VENV "${CMAKE_CURRENT_SOURCE_DIR}/../../python/.venv/bin/python")
if(EXISTS "${RMQ_PY_VENV}")
    add_test(NAME interop
             COMMAND ${RMQ_PY_VENV} ${CMAKE_CURRENT_SOURCE_DIR}/interop_check.py
                     $<TARGET_FILE:rmq_interop>)
endif()
