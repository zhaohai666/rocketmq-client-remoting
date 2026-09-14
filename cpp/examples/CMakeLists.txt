# 自检二进制：协议层往返自检（不依赖真实集群）
add_executable(rmq_selfcheck selfcheck.cpp)
target_link_libraries(rmq_selfcheck PRIVATE rocketmq::remoting)

# 互操作工具：与 Python 参考客户端做跨语言编解码交叉验证
add_executable(rmq_interop interop_tool.cpp)
target_link_libraries(rmq_interop PRIVATE rocketmq::remoting)

# 真实集群消息类型联调（需先起 nameServer+broker；不打进 ctest）
add_executable(rmq_live_message_types live_message_types.cpp)
target_link_libraries(rmq_live_message_types PRIVATE rocketmq::remoting)

# 真实集群管理端联调（需先起 nameServer+broker；不打进 ctest）
add_executable(rmq_admin_live admin_live.cpp)
target_link_libraries(rmq_admin_live PRIVATE rocketmq::remoting)

# 真实集群自动压缩联调（自产自销 + 与 Java 探针跨客户端互通；不打进 ctest）
add_executable(rmq_compression_live compression_live.cpp)
target_link_libraries(rmq_compression_live PRIVATE rocketmq::remoting)

