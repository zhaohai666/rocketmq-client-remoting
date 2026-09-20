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
    src/common/topic_validator.cpp
    src/client/latency.cpp
    src/client/validators.cpp
    src/common/message_decoder.cpp
    src/common/subscription_data.cpp
    src/common/compression.cpp
    src/common/consistent_hash.cpp
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
    src/client/allocate_strategy.cpp
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

# 消息体压缩的另外两个后端：Java 用 lz4-java 的 LZ4Frame*Stream 与 zstd-jni，
# Python 用 lz4.frame / zstandard，Rust 用 lz4_flex::frame / zstd crate；这里直接链接
# 系统的 liblz4（LZ4 Frame 互操作格式，见 lz4frame.h）与 libzstd。
# 与 zlib 同一约定：**找不到就只关这一个后端，configure 照样成功**，运行期遇到该算法
# 的压缩消息一律抛异常，绝不静默把压缩字节当正文交出去（那等于伪装成成功的数据损坏）。
option(RMQ_WITH_ZSTD "Enable zstd message body (de)compression" ON)
option(RMQ_WITH_LZ4  "Enable lz4  message body (de)compression" ON)

# 两个后端都用"裸路径查找优先、cmake config 兜底"：brew 只装 dylib + 头文件（没有
# *-config.cmake），而 vcpkg / 源码安装会导出带 include 路径的 imported target。
# 搜索前缀覆盖 Intel Mac 的 /usr/local、Apple Silicon 的 /opt/homebrew 与 Linux 默认路径。
set(RMQCompression_HINTS /usr/local /opt/homebrew)

set(RMQ_ZSTD_FOUND FALSE)
if(RMQ_WITH_ZSTD)
    find_path(RMQ_ZSTD_INCLUDE_DIR NAMES zstd.h
              HINTS ${RMQCompression_HINTS} PATH_SUFFIXES include)
    find_library(RMQ_ZSTD_LIBRARY NAMES zstd libzstd
                 HINTS ${RMQCompression_HINTS} PATH_SUFFIXES lib)
    if(RMQ_ZSTD_INCLUDE_DIR AND RMQ_ZSTD_LIBRARY)
        set(RMQ_ZSTD_FOUND TRUE)
        set(RMQ_ZSTD_LIBS ${RMQ_ZSTD_LIBRARY})
        message(STATUS "RocketMQ client zstd: enabled (${RMQ_ZSTD_LIBRARY})")
    else()
        find_package(zstd CONFIG QUIET)
        foreach(RMQ_ZSTD_CANDIDATE zstd::zstd zstd::libzstd_shared zstd::libzstd_static)
            if(NOT RMQ_ZSTD_FOUND AND TARGET ${RMQ_ZSTD_CANDIDATE})
                set(RMQ_ZSTD_FOUND TRUE)
                set(RMQ_ZSTD_LIBS ${RMQ_ZSTD_CANDIDATE})
                message(STATUS "RocketMQ client zstd: enabled via cmake config (${RMQ_ZSTD_CANDIDATE})")
            endif()
        endforeach()
    endif()
    if(NOT RMQ_ZSTD_FOUND)
        set(RMQ_ZSTD_INCLUDE_DIR "")
        set(RMQ_ZSTD_LIBRARY "")
        message(STATUS "RocketMQ client zstd: libzstd not found, building WITHOUT zstd")
    endif()
endif()

set(RMQ_LZ4_FOUND FALSE)
if(RMQ_WITH_LZ4)
    # lz4frame.h 是 Frame 格式的公开头；lz4.h 一并要求（compression.cpp 两处都用）
    find_path(RMQ_LZ4_INCLUDE_DIR NAMES lz4frame.h
              HINTS ${RMQCompression_HINTS} PATH_SUFFIXES include)
    find_library(RMQ_LZ4_LIBRARY NAMES lz4 liblz4
                 HINTS ${RMQCompression_HINTS} PATH_SUFFIXES lib)
    if(RMQ_LZ4_INCLUDE_DIR AND RMQ_LZ4_LIBRARY)
        set(RMQ_LZ4_FOUND TRUE)
        set(RMQ_LZ4_LIBS ${RMQ_LZ4_LIBRARY})
        message(STATUS "RocketMQ client lz4: enabled (${RMQ_LZ4_LIBRARY})")
    else()
        find_package(lz4 CONFIG QUIET)
        foreach(RMQ_LZ4_CANDIDATE lz4::lz4 LZ4::lz4)
            if(NOT RMQ_LZ4_FOUND AND TARGET ${RMQ_LZ4_CANDIDATE})
                set(RMQ_LZ4_FOUND TRUE)
                set(RMQ_LZ4_LIBS ${RMQ_LZ4_CANDIDATE})
                message(STATUS "RocketMQ client lz4: enabled via cmake config (${RMQ_LZ4_CANDIDATE})")
            endif()
        endforeach()
    endif()
    if(NOT RMQ_LZ4_FOUND)
        set(RMQ_LZ4_INCLUDE_DIR "")
        set(RMQ_LZ4_LIBRARY "")
        message(STATUS "RocketMQ client lz4: liblz4 not found, building WITHOUT lz4")
    endif()
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

# zstd / lz4 同样是 PUBLIC：示例与单测里直接调 CompressorFactory 的代码要看到同一套宏，
# 否则"编译期关掉的后端"在测试里会被误判成 bug（见 tests/test_compression.cpp 的分支）。
if(RMQ_ZSTD_FOUND)
    target_link_libraries(rocketmq_remoting PUBLIC ${RMQ_ZSTD_LIBS})
    if(RMQ_ZSTD_INCLUDE_DIR)
        target_include_directories(rocketmq_remoting PUBLIC ${RMQ_ZSTD_INCLUDE_DIR})
    endif()
    target_compile_definitions(rocketmq_remoting PUBLIC ROCKETMQ_HAS_ZSTD=1)
endif()

if(RMQ_LZ4_FOUND)
    target_link_libraries(rocketmq_remoting PUBLIC ${RMQ_LZ4_LIBS})
    if(RMQ_LZ4_INCLUDE_DIR)
        target_include_directories(rocketmq_remoting PUBLIC ${RMQ_LZ4_INCLUDE_DIR})
    endif()
    target_compile_definitions(rocketmq_remoting PUBLIC ROCKETMQ_HAS_LZ4=1)
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
