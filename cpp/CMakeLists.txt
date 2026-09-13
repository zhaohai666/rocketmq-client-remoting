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

include(GNUInstallDirs)

option(RMQ_BUILD_TESTS     "Build unit tests"      ON)
option(RMQ_BUILD_EXAMPLES  "Build selfcheck binary" ON)

# ---------------------------------------------------------------- 静态库
set(RMQ_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

set(RMQ_SOURCES
    src/common/util_all.cpp
    src/common/message.cpp
    src/common/message_decoder.cpp
    src/remoting/protocol/json.cpp
    src/remoting/protocol/serialize.cpp
    src/remoting/protocol/remoting_command.cpp
    src/remoting/protocol/headers.cpp
)

add_library(rocketmq_remoting STATIC ${RMQ_SOURCES})
target_include_directories(rocketmq_remoting PUBLIC
    $<BUILD_INTERFACE:${RMQ_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

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
