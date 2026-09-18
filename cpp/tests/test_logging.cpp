// 客户端日志单测：行格式（毫秒 + pid + 线程名 + 文件:行号）、按大小轮转与备份数上限、
// 级别过滤、以及"关闭文件输出后不再写盘"。
//
// 这些断言是**可验证性**的支点：Java 侧靠 logback 配置保证的能力（SizeBasedTriggeringPolicy /
// FixedWindowPolicy / maxIndex），这里必须有自己的回归守卫，否则将来改 logging.h 时
// "日志把磁盘撑爆" 或 "备份无限增长" 会无声回归。
//
// ⚠ 本文件的日志行会同时打到 stderr（logging.h 的设计：stderr + 文件双写），
//   所以 ctest 输出里出现一堆日志行是**正常的**，不是失败。
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#include "rocketmq/common/logging.h"

using namespace rocketmq;

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << "\n";                           \
        }                                                                      \
    } while (0)

namespace {

namespace fs = std::filesystem;

// 轮转参数必须**极小**，否则测试要写 64MB 才能触发一次轮转。
// 这两个值在 logging.h 里是"首次使用即缓存"，因此必须在任何日志调用之前 setenv。
constexpr std::uintmax_t kTestMaxSize = 1200;
constexpr int kTestMaxIndex = 3;
constexpr std::uintmax_t kMaxSizeEnv = 1200;

std::string readAll(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return std::string();
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

std::uintmax_t sizeOf(const std::string& path) {
    std::error_code ec;
    const auto n = fs::file_size(path, ec);
    return ec ? 0 : static_cast<std::uintmax_t>(n);
}

std::vector<std::string> splitLines(const std::string& s) {
    std::vector<std::string> out;
    size_t start = 0;
    while (start < s.size()) {
        size_t pos = s.find('\n', start);
        if (pos == std::string::npos) {
            out.push_back(s.substr(start));
            break;
        }
        out.push_back(s.substr(start, pos - start));
        start = pos + 1;
    }
    return out;
}

int countNonEmpty(const std::vector<std::string>& v) {
    int n = 0;
    for (const std::string& s : v) {
        if (!s.empty()) ++n;
    }
    return n;
}

std::string linePayload(size_t n) { return std::string(n, 'x'); }

std::uintmax_t envUmax(const char* name) {
    const char* v = std::getenv(name);
    if (v == nullptr) return 0;
    return static_cast<std::uintmax_t>(std::strtoull(v, nullptr, 10));
}

// ::setenv 是 POSIX 专有，MSVC 下不存在（只有 _putenv_s），Windows 原生构建会直接编译失败。
void setEnvVar(const char* name, const char* value) {
#if defined(_WIN32)
    _putenv_s(name, value);
#else
    ::setenv(name, value, 1);
#endif
}

}  // namespace

int main() {
    // ---- 必须在第一次日志调用之前设置：logging.h 的静态缓存取首次读到的环境变量 ----
    setEnvVar("ROCKETMQ_CPP_LOG_FILE_MAX_SIZE", "1200");
    setEnvVar("ROCKETMQ_CPP_LOG_FILE_MAX_INDEX", "3");
    setEnvVar("ROCKETMQ_CPP_LOG_LEVEL", "INFO");

    // 环境变量确实被解析到了（而不是静默回落默认值 64MB/10）
    CHECK(logFileMaxSize() == kTestMaxSize, "env max size honored");
    CHECK(logFileMaxIndex() == kTestMaxIndex, "env max index honored");
    CHECK(envUmax("ROCKETMQ_CPP_LOG_FILE_MAX_SIZE") == kTestMaxSize, "env raw value");

    const fs::path dir =
        fs::temp_directory_path() /
        ("rmq_logging_test_" + std::to_string(static_cast<unsigned long long>(ROCKETMQ_CPP_GETPID())));
    std::error_code ec;
    fs::remove_all(dir, ec);
    fs::create_directories(dir, ec);
    CHECK(fs::exists(dir), "temp dir created");

    const std::string logPath = (dir / "cpp_client.log").string();
    setLogFile(logPath);

    // ---------------------------------------------------------------- 线程名
    // 主线程应自动落 "main"（对齐 Java），而不是不可读的 tid 哈希
    CHECK(currentThreadName() == std::string("main"), "main thread named \"main\"");
    const std::string before = currentThreadName();
    setThreadName("ConsumeMessageThread_0");
    CHECK(currentThreadName() == "ConsumeMessageThread_0", "setThreadName updates log name");
    // 改名是**线程局部**的：另起线程不应继承
    std::string otherThreadName;
    std::thread t([&otherThreadName]() { otherThreadName = currentThreadName(); });
    t.join();
    CHECK(otherThreadName != "ConsumeMessageThread_0", "thread name is thread-local");
    CHECK(otherThreadName != std::string("main"), "worker thread is not main");
    setThreadName("ConsumeMessageThread_0");  // 恢复，供下面的格式断言使用

    // ---------------------------------------------------------------- 行格式
    const int expectedLine = __LINE__ + 1;
    logger_info("format-check " + before);
    const std::string body = readAll(logPath);
    const std::vector<std::string> lines = splitLines(body);
    CHECK(lines.size() == 1, "one line written");
    if (!lines.empty()) {
        // 对齐 Java 的 `%d{...SSS} %-5p [%pid] [%t] [%logger#%M:%L] - %m`
        const std::regex re(
            R"(^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} INFO  \[[0-9]+\] \[ConsumeMessageThread_0\] \[test_logging\.cpp:(\d+)\] - )");
        std::smatch m;
        CHECK(std::regex_search(lines[0], m, re), "line matches Java-like format");
        if (m.size() > 1) {
            CHECK(std::stoi(m[1].str()) == expectedLine, "file:line captures call site");
        } else {
            std::cout << "[INFO] actual line: " << lines[0] << "\n";
            ++g_fail;
        }
        CHECK(lines[0].find("format-check") != std::string::npos, "message body preserved");
        CHECK(lines[0].back() != '\r', "no stray CR");
    }
    CHECK(logBaseName("/a/b/c/file.cpp") == std::string("file.cpp"), "logBaseName strips dirs");
    CHECK(logBaseName("plain.cpp") == std::string("plain.cpp"), "logBaseName keeps bare name");
    CHECK(logBaseName(nullptr) == std::string("?"), "logBaseName handles null");

    // ---------------------------------------------------------------- 级别过滤
    const std::uintmax_t afterOne = sizeOf(logPath);
    setLogLevel(LOG_ERROR);
    logger_info("should-be-suppressed");
    logger_debug("should-be-suppressed");
    CHECK(sizeOf(logPath) == afterOne, "INFO/DEBUG suppressed at ERROR level");
    logger_error("error-level-visible");
    const std::string afterErr = readAll(logPath);
    CHECK(afterErr.find("error-level-visible") != std::string::npos, "ERROR passes filter");
    CHECK(afterErr.find("should-be-suppressed") == std::string::npos, "suppressed text absent");
    CHECK(afterErr.find("ERROR [") != std::string::npos, "ERROR level name formatted");
    setLogLevel(LOG_INFO);

    // ---------------------------------------------------------------- 轮转
    const std::uintmax_t baseBefore = sizeOf(logPath);
    CHECK(baseBefore > 0, "base log non-empty before rotation");
    CHECK(!fs::exists(logPath + ".1"), "no backup before rotation");

    // 每行约 280 字节，需 ~5 行触发一次；写 60 行足以滚满 maxIndex=3
    const std::string filler = linePayload(200);
    for (int i = 0; i < 60; ++i) {
        logger_warn("rotate-" + std::to_string(i) + " " + filler);
    }

    CHECK(fs::exists(logPath + ".1"), "backup .1 created");
    CHECK(fs::exists(logPath + ".2"), "backup .2 created");
    CHECK(fs::exists(logPath + ".3"), "backup .3 created");
    CHECK(!fs::exists(logPath + ".4"), "backup .4 never exceeds maxIndex");

    const std::uintmax_t baseAfter = sizeOf(logPath);
    CHECK(baseAfter > 0, "base log still being written after rotation");
    CHECK(baseAfter <= kMaxSizeEnv, "base log size capped by maxFileSize");

    // 备份里应是**较早**的行：.3 最旧、.1 最新（FixedWindow 语义）
    const std::string b1 = readAll(logPath + ".1");
    const std::string b3 = readAll(logPath + ".3");
    CHECK(countNonEmpty(splitLines(b1)) > 0, "backup .1 has content");
    CHECK(countNonEmpty(splitLines(b3)) > 0, "backup .3 has content");
    CHECK(b1.find("rotate-0 ") == std::string::npos,
          "oldest lines rotated out of .1 (window advanced)");
    CHECK(b3.find("rotate-") != std::string::npos, "oldest surviving lines landed in .3");
    CHECK(readAll(logPath).find("rotate-59") != std::string::npos, "newest line is in base file");

    // ---------------------------------------------------------------- 关闭文件输出
    setLogFile("");
    const std::uintmax_t frozen = sizeOf(logPath);
    logger_error("after-file-disabled");
    CHECK(sizeOf(logPath) == frozen, "no disk write after setLogFile(\"\")");
    CHECK(!fs::exists(logPath + ".4"), "disabled sink does not rotate");

    // 重新指回文件应能恢复写入
    setLogFile(logPath);
    logger_error("reopened-after-disable");
    CHECK(readAll(logPath).find("reopened-after-disable") != std::string::npos,
          "file output resumes after setLogFile(path)");
    flushLogFile();

    fs::remove_all(dir, ec);

    std::cout << "\nlogging: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
