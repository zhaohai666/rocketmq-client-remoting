cmake_minimum_required(VERSION 3.15)

project(rocketmq_client_remoting LANGUAGES CXX VERSION 0.1.0)

# ---------------------------------------------------------------- 全局设置
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

if(MSVC)
    # 源码统一 UTF-8，避免 MSVC 按本地代码页解析中文注释导致 C4819/编译错误
    add_compile_options(/utf-8 /Zc:__cplusplus)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS NOMINMAX)
else()
    add_compile_options(-Wall -Wextra)
endif()

# MinGW/GCC 交叉编译到 Windows 时同样必须定义 NOMINMAX：否则 windef.h 会定义
# min/max 宏，把代码里所有 std::min/std::max 展开成非法表达式而编译失败。
# WIN32_LEAN_AND_MEAN 可减少 windows.h 的无关依赖，缩短编译时间。
# （net_compat.h 内部也会兜底定义这两个宏，双保险。）
if(WIN32)
    add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN)
endif()

include(GNUInstallDirs)

option(RMQ_BUILD_TESTS     "Build unit tests"      ON)
option(RMQ_BUILD_EXAMPLES  "Build selfcheck binary" ON)

# ---------------------------------------------------------------- 静态库
set(RMQ_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

set(RMQ_SOURCES
    src/common/util_all.cpp
    src/common/message.cpp
    src/client/latency.cpp
    src/common/message_decoder.cpp
    src/common/subscription_data.cpp
    src/common/compression.cpp
    src/common/mix_all.cpp
    src/common/top_addressing.cpp
    src/common/topic_config.cpp
    src/remoting/protocol/json.cpp
    src/remoting/protocol/serialize.cpp
    src/remoting/protocol/remoting_command.cpp
    src/remoting/protocol/headers.cpp
    src/remoting/protocol/extra_info.cpp
    src/remoting/protocol/route.cpp
    src/remoting/protocol/heartbeat.cpp
    src/remoting/protocol/subscription.cpp
    src/remoting/protocol/admin_body.cpp
    src/remoting/protocol/body.cpp
    src/remoting/remoting_client.cpp
    src/remoting/rpchook.cpp
    src/client/result.cpp
    src/client/mq_client.cpp
    src/client/consumer_stats.cpp
    src/client/trace_context.cpp
    src/client/producer.cpp
    src/client/consumer.cpp
    src/client/consume_executor.cpp
    src/client/pull_consumer.cpp
    src/client/lite_pull_consumer.cpp
    src/client/request_reply.cpp
    src/client/admin.cpp
    src/client/hook.cpp
    src/client/trace.cpp
    src/client/trace_dispatcher.cpp
    src/client/trace_hook.cpp
)

# 传输层用到 std::thread / std::mutex，需要显式链接线程库（Linux 上尤其必要）
find_package(Threads REQUIRED)

# 消息体压缩：Java 用 java.util.zip（永远存在），Python 用标准库 zlib。
# 这里同样默认要求 zlib（libz 在各平台都自带）。若要构建无 zlib 的精简版，
# 用 -DRMQ_WITH_ZLIB=OFF；此时**遇到压缩消息会抛异常**而不是静默返回压缩字节，
# 以免把数据损坏伪装成成功。
option(RMQ_WITH_ZLIB "Enable zlib message body (de)compression" ON)
if(RMQ_WITH_ZLIB)
    find_package(ZLIB REQUIRED)
endif()

# 传输层 TLS（对应 Java NettyRemotingClient 的 SslHandler）。找到 OpenSSL 就默认编入
# （test mode 信任 broker 自签证书，配合 PERMISSIVE broker 即配即通）；找不到时静默降级，
# setTlsEnable(true) 会在运行时抛出带说明的错误，其余路径零影响。
option(RMQ_ENABLE_TLS "Enable TLS transport (requires OpenSSL)" ON)
if(RMQ_ENABLE_TLS)
    find_package(OpenSSL QUIET)
    if(OpenSSL_FOUND)
        message(STATUS "RocketMQ client TLS: enabled (OpenSSL ${OPENSSL_VERSION})")
    else()
        message(STATUS "RocketMQ client TLS: OpenSSL not found, building WITHOUT TLS")
    endif()
endif()

add_library(rocketmq_remoting STATIC ${RMQ_SOURCES})
target_include_directories(rocketmq_remoting PUBLIC
    $<BUILD_INTERFACE:${RMQ_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_link_libraries(rocketmq_remoting PUBLIC Threads::Threads)

if(RMQ_WITH_ZLIB)
    target_link_libraries(rocketmq_remoting PUBLIC ZLIB::ZLIB)
    # 让 message_decoder / compression.cpp 感知 zlib 是否可用
    target_compile_definitions(rocketmq_remoting PUBLIC ROCKETMQ_HAS_ZLIB=1)
endif()

if(RMQ_ENABLE_TLS AND OpenSSL_FOUND)
    target_link_libraries(rocketmq_remoting PUBLIC OpenSSL::SSL OpenSSL::Crypto)
    target_include_directories(rocketmq_remoting PRIVATE ${OPENSSL_INCLUDE_DIR})
    # 仅 tls_session.cpp 感知（PUBLIC 与否：tests 需要自己的 mock TLS server，也用 OpenSSL）
    target_compile_definitions(rocketmq_remoting PUBLIC RMQ_HAS_TLS=1)
    target_sources(rocketmq_remoting PRIVATE src/remoting/tls_session.cpp)
endif()

# util_all 里 localIp() 用到 socket；Windows 需要链接 winsock
if(WIN32)
    target_link_libraries(rocketmq_remoting PUBLIC ws2_32)
endif()

add_library(rocketmq::remoting ALIAS rocketmq_remoting)

# ---------------------------------------------------------------- 子目录
if(RMQ_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(RMQ_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ---------------------------------------------------------------- 安装
install(TARGETS rocketmq_remoting
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
