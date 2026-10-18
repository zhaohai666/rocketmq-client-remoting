cmake_minimum_required(VERSION 3.15)

project(rocketmq_client_remoting LANGUAGES CXX VERSION 0.1.0)

# ---------------------------------------------------------------- Global settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

if(MSVC)
    # Sources are UTF-8: otherwise MSVC parses non-ASCII comments by the local code page, which gives C4819 warnings or build errors
    add_compile_options(/utf-8 /Zc:__cplusplus)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS NOMINMAX)
else()
    add_compile_options(-Wall -Wextra)
endif()

# MinGW/GCC cross builds for Windows must define NOMINMAX too: otherwise windef.h
# defines the min/max macros, expanding every std::min/std::max in the code into an
# illegal expression, so the build fails. WIN32_LEAN_AND_MEAN trims unrelated
# windows.h dependencies and cuts compile time (net_compat.h also defines both as a fallback, doubly safe).
if(WIN32)
    add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN)
endif()

include(GNUInstallDirs)

option(RMQ_BUILD_TESTS     "Build unit tests"      ON)
option(RMQ_BUILD_EXAMPLES  "Build selfcheck binary" ON)

# ---------------------------------------------------------------- Static library
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
    src/common/recall_message_handle.cpp
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
    src/remoting/socks_proxy.cpp
    src/remoting/rpchook.cpp
    src/client/result.cpp
    src/client/static_topic.cpp
    src/client/allocate_strategy.cpp
    src/client/mq_client.cpp
    src/client/consumer_stats.cpp
    src/client/trace_context.cpp
    src/client/producer.cpp
    src/client/produce_accumulator.cpp
    src/client/consumer.cpp
    src/client/consume_executor.cpp
    src/client/backpressure.cpp
    src/client/pull_consumer.cpp
    src/client/pull_schedule_service.cpp
    src/client/lite_pull_consumer.cpp
    src/client/request_reply.cpp
    src/client/admin.cpp
    src/client/hook.cpp
    src/client/trace.cpp
    src/client/trace_dispatcher.cpp
    src/client/trace_hook.cpp
)

# The transport layer uses std::thread / std::mutex, so the thread library must be linked explicitly (especially on Linux)
find_package(Threads REQUIRED)

# Message body compression: the peer implementations all use standard zlib, which
# always exists (libz ships on every platform), so this build requires it by
# default. A slim zlib-free build uses -DRMQ_WITH_ZLIB=OFF; a compressed
# message then throws instead of silently returning compressed bytes, which would disguise data corruption as success.
option(RMQ_WITH_ZLIB "Enable zlib message body (de)compression" ON)
if(RMQ_WITH_ZLIB)
    find_package(ZLIB REQUIRED)
endif()

# The other two backends: wire frames come from lz4-java's LZ4Frame*Stream and zstd-jni; this links
# the system liblz4 (LZ4 Frame interop format, see lz4frame.h) and libzstd directly. As with zlib:
# a missing backend disables only that backend and configure still succeeds. At runtime a message
# on that codec throws instead of handing out compressed bytes as the body: that disguises data corruption as success.
option(RMQ_WITH_ZSTD "Enable zstd message body (de)compression" ON)
option(RMQ_WITH_LZ4  "Enable lz4  message body (de)compression" ON)

# Both backends search plain paths first and fall back to the cmake config: brew installs only the
# dylib plus headers (no *-config.cmake), while vcpkg or a source install exports an imported target
# with include paths. Search prefixes cover /usr/local (Intel Mac), /opt/homebrew (Apple Silicon) and the Linux defaults.
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
    # lz4frame.h is the public header of the Frame format; lz4.h is required too (compression.cpp uses both)
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

# Transport TLS, the counterpart of the connection-layer SslHandler. With OpenSSL found it is
# built in by default: "test mode" trusts the broker self-signed certificate, so a PERMISSIVE
# broker works right away. Missing OpenSSL degrades silently: setTlsEnable(true) throws a descriptive error, other paths unaffected.
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
    # Lets message_decoder / compression.cpp know whether zlib is available
    target_compile_definitions(rocketmq_remoting PUBLIC ROCKETMQ_HAS_ZLIB=1)
endif()

# zstd / lz4 are PUBLIC too: examples and unit tests call CompressorFactory directly and
# must see the same macros, else a compile-time-disabled backend looks like a test bug (see the branches in tests/test_compression.cpp).
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
    # Only tls_session.cpp needs it (PUBLIC anyway: the tests build their own mock TLS server and use OpenSSL too)
    target_compile_definitions(rocketmq_remoting PUBLIC RMQ_HAS_TLS=1)
    target_sources(rocketmq_remoting PRIVATE src/remoting/tls_session.cpp)
endif()

# localIp() in util_all uses socket, so Windows needs to link winsock
if(WIN32)
    target_link_libraries(rocketmq_remoting PUBLIC ws2_32)
endif()

add_library(rocketmq::remoting ALIAS rocketmq_remoting)
# The library file stays librocketmq_remoting.a, but the EXPORTED namespace name is the
# consumer-friendly rocketmq::remoting (matching the in-tree ALIAS), so
# target_link_libraries(app PRIVATE rocketmq::remoting) works both ways.
set_target_properties(rocketmq_remoting PROPERTIES EXPORT_NAME remoting)

# ---------------------------------------------------------------- Subdirectories
if(RMQ_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(RMQ_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ---------------------------------------------------------------- Installation
# Plain `make install` used to ship only the archive plus headers: a consumer then had to
# build the target by hand AND declare every transitive dependency (XLIB/OpenSSL/zstd/lz4)
# before find_package would succeed. With the EXPORT set + config package below, a
# consumer only needs:
#
#     find_package(rocketmq_client_remoting CONFIG REQUIRED)
#     target_link_libraries(my_app PRIVATE rocketmq::remoting)
#
# and the config file replays the Threads/ZLIB/zstd/lz4/OpenSSL finds in the consumer's
# scope (see cmake/rocketmq_client_remoting-config.cmake.in).
include(CMakePackageConfigHelpers)

set(RMQ_INSTALL_CMAKEDIR ${CMAKE_INSTALL_LIBDIR}/cmake/rocketmq_client_remoting CACHE PATH
    "Install directory for the rocketmq_client_remoting CMake package config")

install(TARGETS rocketmq_remoting
        EXPORT rocketmq_client_remoting-targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

configure_package_config_file(
    cmake/rocketmq_client_remoting-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/rocketmq_client_remoting-config.cmake
    INSTALL_DESTINATION ${RMQ_INSTALL_CMAKEDIR})

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/rocketmq_client_remoting-config-version.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/rocketmq_client_remoting-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/rocketmq_client_remoting-config-version.cmake
        DESTINATION ${RMQ_INSTALL_CMAKEDIR})

install(EXPORT rocketmq_client_remoting-targets
        FILE rocketmq_client_remoting-targets.cmake
        NAMESPACE rocketmq::
        DESTINATION ${RMQ_INSTALL_CMAKEDIR})
