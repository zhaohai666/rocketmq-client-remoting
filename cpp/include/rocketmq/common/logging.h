// ⚠ 级别与文件路径在**首次写日志时**求值并缓存，因此必须在第一次日志输出前设置环境变量。
//   程序内可用 setLogLevel() / setLogFile() 直接改（后者会重新打开文件）。
#ifndef ROCKETMQ_COMMON_LOGGING_H
#define ROCKETMQ_COMMON_LOGGING_H

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

#if defined(_WIN32)
#include <process.h>
#define ROCKETMQ_CPP_GETPID _getpid
#else
#include <pthread.h>
#include <unistd.h>
#define ROCKETMQ_CPP_GETPID getpid
#endif

namespace rocketmq {

enum LogLevel {
    LOG_DEBUG = 0,
    LOG_INFO = 1,
    LOG_WARN = 2,
    LOG_ERROR = 3,
    LOG_OFF = 4,
};

// ---------------------------------------------------------------- 级别
inline int resolveDefaultLogLevel() {
    const char* env = std::getenv("ROCKETMQ_CLIENT_LOG_LEVEL");
    if (env == nullptr) return LOG_INFO;
    std::string s(env);
    for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if (s == "DEBUG") return LOG_DEBUG;
    if (s == "INFO") return LOG_INFO;
    if (s == "WARN" || s == "WARNING") return LOG_WARN;
    if (s == "ERROR") return LOG_ERROR;
    if (s == "OFF" || s == "NONE") return LOG_OFF;
    return LOG_INFO;
}

inline std::atomic<int>& logLevelRef() {
    static std::atomic<int> level{resolveDefaultLogLevel()};
    return level;
}

inline void setLogLevel(int level) { logLevelRef().store(level, std::memory_order_relaxed); }
inline int logLevel() { return logLevelRef().load(std::memory_order_relaxed); }

// 环境变量是否**显式**指定了级别。宿主程序若要"默认 INFO、但尊重外部显式配置"，
// 写成 `if (!logLevelSetFromEnv()) setLogLevel(LOG_INFO);`，这样真机排查时
// 直接 `ROCKETMQ_CLIENT_LOG_LEVEL=DEBUG ./app` 就能提高日志级别而不必改代码。
inline bool logLevelSetFromEnv() { return std::getenv("ROCKETMQ_CLIENT_LOG_LEVEL") != nullptr; }

inline std::uintmax_t resolveLogFileMaxSize() {
    const std::uintmax_t kDefault = 64ull * 1024 * 1024;
    const char* env = std::getenv("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE");
    if (env == nullptr) return kDefault;
    try {
        long long v = std::stoll(std::string(env));
        return v > 0 ? static_cast<std::uintmax_t>(v) : 0;
    } catch (...) {
        return kDefault;
    }
}

inline int resolveLogFileMaxIndex() {
    const char* env = std::getenv("ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX");
    if (env == nullptr) return 10;
    try {
        int v = std::stoi(std::string(env));
        return v > 0 ? v : 0;
    } catch (...) {
        return 10;
    }
}

inline std::uintmax_t logFileMaxSize() {
    static const std::uintmax_t v = resolveLogFileMaxSize();
    return v;
}

inline int logFileMaxIndex() {
    static const int v = resolveLogFileMaxIndex();
    return v;
}

// ---------------------------------------------------------------- 日志文件
struct LogFileSink {
    std::FILE* fp = nullptr;
    std::string path;          // 空 = 关闭文件输出
    std::uintmax_t size = 0;   // 当前文件已写字节数（本进程视角）
    bool explicitPath = false; // 是否由 setLogFile() 指定
    bool openFailed = false;
};

inline LogFileSink& logFileSink() {
    static LogFileSink sink;
    return sink;
}

inline std::mutex& logMutex() {
    static std::mutex m;
    return m;
}

inline std::string defaultLogFilePath() {
    const char* env = std::getenv("ROCKETMQ_CLIENT_LOG_FILE");
    if (env != nullptr) {
        std::string s(env);
        if (s.empty() || s == "OFF" || s == "NONE") return std::string();
        return s;
    }
    const char* home = std::getenv("HOME");
    if (home == nullptr || *home == '\0') home = std::getenv("USERPROFILE");
    if (home == nullptr || *home == '\0') return std::string();
    return std::string(home) + "/logs/rocketmqlogs/rocketmq_cpp_client.log";
}

inline const std::string& logFilePath() {
    static const std::string path = defaultLogFilePath();
    return path;
}

inline void closeLogSinkLocked(LogFileSink& sink) {
    if (sink.fp != nullptr) {
        std::fclose(sink.fp);
        sink.fp = nullptr;
    }
    sink.size = 0;
}

inline bool openLogSinkLocked(LogFileSink& sink) {
    if (sink.path.empty() || sink.openFailed) return false;
    std::error_code ec;
    const std::filesystem::path p(sink.path);
    if (!p.parent_path().empty()) {
        std::filesystem::create_directories(p.parent_path(), ec);
    }
    // "ab" 而不是 "a"：Windows 的文本模式会把 '\n' 改写成 "\r\n"，既让落盘行格式与
    // 其它平台不一致，又让轮转用的字节数（fprintf 返回值，翻译前）与真实文件大小脱钩。
    sink.fp = std::fopen(sink.path.c_str(), "ab");
    if (sink.fp == nullptr) {
        // 静默降级为仅 stderr（不阻断客户端），但只提示一次
        sink.openFailed = true;
        std::fprintf(stderr, "[rocketmq] client file log disabled: cannot open %s\n",
                     sink.path.c_str());
        return false;
    }
    std::error_code sizeEc;
    const auto existing = std::filesystem::file_size(p, sizeEc);
    sink.size = sizeEc ? 0 : static_cast<std::uintmax_t>(existing);
    return true;
}

// FixedWindow 滚动：<file>.N 最旧，先删；然后 .N-1 -> .N …… .1 -> .2；最后 base -> .1
inline void rollLogFileLocked(LogFileSink& sink, int maxIndex) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::remove(sink.path + "." + std::to_string(maxIndex), ec);
    for (int i = maxIndex - 1; i >= 1; --i) {
        const std::string src = sink.path + "." + std::to_string(i);
        if (fs::exists(src, ec)) {
            fs::rename(src, sink.path + "." + std::to_string(i + 1), ec);
        }
    }
    fs::rename(sink.path, sink.path + ".1", ec);
}

// 宿主程序可直接指定日志文件（空串 = 关闭文件输出）
inline void setLogFile(const std::string& path) {
    std::lock_guard<std::mutex> lk(logMutex());
    LogFileSink& sink = logFileSink();
    closeLogSinkLocked(sink);
    sink.path = path;
    sink.explicitPath = true;
    sink.openFailed = false;
}

inline void flushLogFile() {
    std::lock_guard<std::mutex> lk(logMutex());
    LogFileSink& sink = logFileSink();
    if (sink.fp != nullptr) std::fflush(sink.fp);
}

// ---------------------------------------------------------------- 线程名
// 取值优先级：本线程设置的名称（同 OS 线程名）-> "main"（进程主线程）-> OS 线程名 -> tid 短串。
// 结果按线程缓存（thread_local），线程改名请用 setThreadName()。
inline std::string& threadNameCache() {
    thread_local std::string name;
    return name;
}

namespace detail {
struct MainThreadId {
    std::thread::id id;
    MainThreadId() : id(std::this_thread::get_id()) {}
};
inline const MainThreadId kMainThreadId{};
}  // namespace detail

inline std::string computeThreadName() {
    if (std::this_thread::get_id() == detail::kMainThreadId.id) {
        return "main";
    }
#if !defined(_WIN32)
    char buf[64] = {0};
    if (pthread_getname_np(pthread_self(), buf, sizeof(buf)) == 0 && buf[0] != '\0') {
        return std::string(buf);
    }
#endif
    const size_t h = std::hash<std::thread::id>{}(std::this_thread::get_id());
    char tmp[32];
    std::snprintf(tmp, sizeof(tmp), "tid-%04x", static_cast<unsigned>(h & 0xffffu));
    return std::string(tmp);
}

inline std::string currentThreadName() {
    std::string& cached = threadNameCache();
    if (cached.empty()) cached = computeThreadName();
    return cached;
}

inline void setThreadName(const std::string& name) {
    threadNameCache() = name;
#if defined(__APPLE__)
    // macOS: int pthread_setname_np(const char*)，上限 64 字节
    if (!name.empty()) pthread_setname_np(name.substr(0, 63).c_str());
#elif defined(__linux__)
    // Linux: int pthread_setname_np(pthread_t, const char*)，上限 16 字节（含结尾 NUL）
    if (!name.empty()) pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
#endif
}

// ---------------------------------------------------------------- 输出
inline const char* logLevelName(int level) {
    switch (level) {
        case LOG_DEBUG: return "DEBUG";
        case LOG_INFO: return "INFO";
        case LOG_WARN: return "WARN";
        case LOG_ERROR: return "ERROR";
        default: return "?";
    }
}

inline const char* logBaseName(const char* path) {
    if (path == nullptr) return "?";
    const char* base = path;
    for (const char* p = path; *p != '\0'; ++p) {
        if (*p == '/' || *p == '\\') base = p + 1;
    }
    return base;
}

inline void writeLogLine(int level, const std::string& msg, const char* file = nullptr,
                         int line = 0) {
    if (level < logLevel()) return;

    const auto now = std::chrono::system_clock::now();
    const std::time_t secs = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now.time_since_epoch())
                        .count() %
                    1000;
    std::tm tmv{};
#if defined(_WIN32)
    localtime_s(&tmv, &secs);
#else
    localtime_r(&secs, &tmv);
#endif
    char ts[32];
    ts[0] = '\0';
    std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);

    char head[192];
    std::snprintf(head, sizeof(head), "%s.%03d %-5s [%d] [%s] [%s:%d] - ", ts,
                  static_cast<int>(ms), logLevelName(level),
                  static_cast<int>(ROCKETMQ_CPP_GETPID()), currentThreadName().c_str(),
                  logBaseName(file), line);

    std::string out;
    out.reserve(msg.size() + 96);
    out += head;
    out += msg;
    out += '\n';

    std::fwrite(out.data(), 1, out.size(), stderr);

    std::lock_guard<std::mutex> lk(logMutex());
    LogFileSink& sink = logFileSink();
    if (!sink.explicitPath) sink.path = logFilePath();
    if (sink.path.empty() || sink.openFailed) return;
    if (sink.fp == nullptr && !openLogSinkLocked(sink)) return;

    const std::uintmax_t maxSize = logFileMaxSize();
    const int maxIndex = logFileMaxIndex();
    if (maxSize > 0 && maxIndex > 0 && sink.size + out.size() > maxSize) {
        closeLogSinkLocked(sink);
        rollLogFileLocked(sink, maxIndex);
        if (!openLogSinkLocked(sink)) return;
    }

    std::fwrite(out.data(), 1, out.size(), sink.fp);
    std::fflush(sink.fp);  // 保证 tail -f 实时可见
    sink.size += out.size();
}

}  // namespace rocketmq

#define logger_debug(msg) \
    ::rocketmq::writeLogLine(::rocketmq::LOG_DEBUG, (msg), __FILE__, __LINE__)
#define logger_info(msg) \
    ::rocketmq::writeLogLine(::rocketmq::LOG_INFO, (msg), __FILE__, __LINE__)
#define logger_warn(msg) \
    ::rocketmq::writeLogLine(::rocketmq::LOG_WARN, (msg), __FILE__, __LINE__)
#define logger_error(msg) \
    ::rocketmq::writeLogLine(::rocketmq::LOG_ERROR, (msg), __FILE__, __LINE__)

#endif  // ROCKETMQ_COMMON_LOGGING_H
