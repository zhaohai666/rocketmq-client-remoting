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

# 跨语言互操作（C++ <-> Python 参考客户端）。依赖 python 侧 venv，不存在则跳过。
set(RMQ_PY_VENV "${CMAKE_CURRENT_SOURCE_DIR}/../../python/.venv/bin/python")
if(EXISTS "${RMQ_PY_VENV}")
    add_test(NAME interop
             COMMAND ${RMQ_PY_VENV} ${CMAKE_CURRENT_SOURCE_DIR}/interop_check.py
                     $<TARGET_FILE:rmq_interop>)
endif()
