// Client logging unit test: line format (millisecond + pid + thread name + file:line), rotation
// by size with a backup-index cap, level filtering, and "no disk write once file output is off".
//
//
// ⚠ Log lines from this file also reach stderr (logging.h writes to stderr and the file at once),
//   so a pile of log lines in the ctest output is **normal**, not a failure.
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

// The rotation parameters must stay **tiny**, or the test would have to write 64MB to rotate once.
// logging.h caches both values on first use, so setenv has to run before any logging call.
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

// ::setenv is POSIX-only and does not exist on MSVC (only _putenv_s), so a native Windows build fails to compile.
void setEnvVar(const char* name, const char* value) {
#if defined(_WIN32)
    _putenv_s(name, value);
#else
    ::setenv(name, value, 1);
#endif
}

}  // namespace

int main() {
    // ---- Must run before the first logging call: logging.h's static cache keeps the first env read ----
    setEnvVar("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", "1200");
    setEnvVar("ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX", "3");
    setEnvVar("ROCKETMQ_CLIENT_LOG_LEVEL", "INFO");

    // The environment variables really were parsed (not a silent fall back to the 64MB/10 defaults)
    CHECK(logFileMaxSize() == kTestMaxSize, "env max size honored");
    CHECK(logFileMaxIndex() == kTestMaxIndex, "env max index honored");
    CHECK(envUmax("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE") == kTestMaxSize, "env raw value");

    const fs::path dir =
        fs::temp_directory_path() /
        ("rmq_logging_test_" + std::to_string(static_cast<unsigned long long>(ROCKETMQ_CPP_GETPID())));
    std::error_code ec;
    fs::remove_all(dir, ec);
    fs::create_directories(dir, ec);
    CHECK(fs::exists(dir), "temp dir created");

    const std::string logPath = (dir / "cpp_client.log").string();
    setLogFile(logPath);

    CHECK(currentThreadName() == std::string("main"), "main thread named \"main\"");
    const std::string before = currentThreadName();
    setThreadName("ConsumeMessageThread_0");
    CHECK(currentThreadName() == "ConsumeMessageThread_0", "setThreadName updates log name");
    // Renaming is **thread-local**: another thread must not inherit it
    std::string otherThreadName;
    std::thread t([&otherThreadName]() { otherThreadName = currentThreadName(); });
    t.join();
    CHECK(otherThreadName != "ConsumeMessageThread_0", "thread name is thread-local");
    CHECK(otherThreadName != std::string("main"), "worker thread is not main");
    setThreadName("ConsumeMessageThread_0");  // restore it for the format assertions below

    // ---------------------------------------------------------------- line format
    const int expectedLine = __LINE__ + 1;
    logger_info("format-check " + before);
    const std::string body = readAll(logPath);
    const std::vector<std::string> lines = splitLines(body);
    CHECK(lines.size() == 1, "one line written");
    if (!lines.empty()) {
        const std::regex re(
            R"(^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} INFO  \[[0-9]+\] \[ConsumeMessageThread_0\] \[test_logging\.cpp:(\d+)\] - )");
        std::smatch m;
        CHECK(std::regex_search(lines[0], m, re), "line matches the recorded format");
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

    // ---------------------------------------------------------------- level filtering
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

    // ---------------------------------------------------------------- rotation
    const std::uintmax_t baseBefore = sizeOf(logPath);
    CHECK(baseBefore > 0, "base log non-empty before rotation");
    CHECK(!fs::exists(logPath + ".1"), "no backup before rotation");

    // Roughly 280 bytes per line, so ~5 lines trigger one rotation; 60 lines fill maxIndex=3
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

    // Backups must hold the **older** lines: .3 is the oldest, .1 the newest (FixedWindow semantics)
    const std::string b1 = readAll(logPath + ".1");
    const std::string b3 = readAll(logPath + ".3");
    CHECK(countNonEmpty(splitLines(b1)) > 0, "backup .1 has content");
    CHECK(countNonEmpty(splitLines(b3)) > 0, "backup .3 has content");
    CHECK(b1.find("rotate-0 ") == std::string::npos,
          "oldest lines rotated out of .1 (window advanced)");
    CHECK(b3.find("rotate-") != std::string::npos, "oldest surviving lines landed in .3");
    CHECK(readAll(logPath).find("rotate-59") != std::string::npos, "newest line is in base file");

    // ---------------------------------------------------------------- disabling file output
    setLogFile("");
    const std::uintmax_t frozen = sizeOf(logPath);
    logger_error("after-file-disabled");
    CHECK(sizeOf(logPath) == frozen, "no disk write after setLogFile(\"\")");
    CHECK(!fs::exists(logPath + ".4"), "disabled sink does not rotate");

    // Pointing the log back at a file must resume writing
    setLogFile(logPath);
    logger_error("reopened-after-disable");
    CHECK(readAll(logPath).find("reopened-after-disable") != std::string::npos,
          "file output resumes after setLogFile(path)");
    flushLogFile();

    fs::remove_all(dir, ec);

    std::cout << "\nlogging: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
