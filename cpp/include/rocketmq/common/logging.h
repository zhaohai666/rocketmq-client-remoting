// 轻量客户端日志（对应 Java 的 rmq.client.logback.xml 与 Python 的 rocketmq/logging.py）。
//
// 设计目标：
//   - 默认级别 INFO，因此**良性事件**（如长轮询超时）走 DEBUG 会被抑制，不污染运行日志；
//   - 同时输出到 stderr 与文件，便于"日志是否干净"的可验证性；
//   - **按大小轮转**（对齐 Java logback 的 SizeBasedTriggeringPolicy + FixedWindow），
//     避免常驻进程把单个日志文件撑到无限大。
//
// 配置（环境变量）：
//   ROCKETMQ_CPP_LOG_LEVEL          = DEBUG | INFO | WARN | ERROR | OFF      （默认 INFO）
//   ROCKETMQ_CPP_LOG_FILE           = 日志文件绝对路径
//                                     （默认 $HOME/logs/rocketmqlogs/rocketmq_cpp_client.log；
//                                      设为 "OFF"/"NONE"/空串可关闭文件输出，只留 stderr）
//   ROCKETMQ_CPP_LOG_FILE_MAX_SIZE  = 单文件上限字节数（默认 67108864 = 64MB，对齐 Java
//                                     logback 的 <maxFileSize>64MB</maxFileSize>；0 = 不轮转）
//   ROCKETMQ_CPP_LOG_FILE_MAX_INDEX = 保留的备份份数（默认 10，对齐 Java
//                                     rocketmq.log.file.maxIndex；0 = 不保留备份）
//
// ⚠ 级别与文件路径在**首次写日志时**求值并缓存，因此必须在第一次日志输出前设置环境变量。
//   程序内可用 setLogLevel() / setLogFile() 直接改（后者会重新打开文件）。
//
// 日志行格式（对齐 Java 的 `%d{...SSS} %-5p [%pid] [%t] [%logger#%M:%L] - %m`）：
//   2026-09-14 16:57:21.123 INFO  [54606] [ConsumeMessageThread_0] [consumer.cpp:289] - ...
//   Java 的 %logger#%M:%L 是"日志器名#方法:行"，本实现没有日志器层级，用**文件名:行号**替代，
//   信息量等价且更直观。
//   线程名：主线程落 [main]（对齐 Java），工作线程由客户端内部命名
//   （ConsumeMessageThread_N / AsyncSenderThread_N / RemotingClientReader-<addr>），
//   未命名线程回落 OS 线程名，再回落 [tid-xxxx]。
//
// 与 Java 的已知差异（在此显式记录，避免被当成 bug）：
//   1. 备份文件**不压缩**（Java 会 gzip 成 other_days/rocketmq_client-%i.log.gz）；
//      备份名是 `<file>.1` … `<file>.N`（FixedWindow 语义），仍是滚动窗口而不是按天归档。
//   2. **同步写**（Java 走 AsyncAppender）。每行之后 fflush，所以 `tail -f` 实时可见。
//   3. 连接关闭记为 DEBUG。"对端关闭连接"在 Java 里走 Netty 的 channelInactive 会打一行
//      INFO/WARN，但正常 shutdown 也会命中同一条路径，在默认 INFO 级别下会变成退出时的
//      假异常噪声。真正的协议异常（帧长非法、解码失败）仍按 WARN 记录。
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
    const char* env = std::getenv("ROCKETMQ_CPP_LOG_LEVEL");
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
// 直接 `ROCKETMQ_CPP_LOG_LEVEL=DEBUG ./app` 就能提高日志级别而不必改代码。
inline bool logLevelSetFromEnv() { return std::getenv("ROCKETMQ_CPP_LOG_LEVEL") != nullptr; }

// ---------------------------------------------------------------- 轮转参数
// Java logback: <maxFileSize>64MB</maxFileSize> + maxIndex 默认 10
inline std::uintmax_t resolveLogFileMaxSize() {
    const std::uintmax_t kDefault = 64ull * 1024 * 1024;
    const char* env = std::getenv("ROCKETMQ_CPP_LOG_FILE_MAX_SIZE");
    if (env == nullptr) return kDefault;
    try {
        long long v = std::stoll(std::string(env));
        return v > 0 ? static_cast<std::uintmax_t>(v) : 0;
    } catch (...) {
        return kDefault;
    }
}

inline int resolveLogFileMaxIndex() {
    const char* env = std::getenv("ROCKETMQ_CPP_LOG_FILE_MAX_INDEX");
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
    const char* env = std::getenv("ROCKETMQ_CPP_LOG_FILE");
    if (env != nullptr) {
        std::string s(env);
        if (s.empty() || s == "OFF" || s == "NONE") return std::string();
        return s;
    }
    const char* home = std::getenv("HOME");
    if (home == nullptr || *home == '\0') home = std::getenv("USERPROFILE");  // Windows: Java user.home
    if (home == nullptr || *home == '\0') return std::string();
    // 文件名刻意与 Java 的 rocketmq_client.log 区分：同机同时跑 Java 客户端时
    // 两边轮转策略不同，写同一文件会互相插行、互相截断。
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
// 在**动态初始化阶段**（main 之前，此时进程仍是单线程）捕获主线程 id。
// 这样日志里的启动阶段会显示 [main]，与 Java 客户端一致，而不是不可读的 tid 哈希。
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
    // 回落到线程 id 的短哈希：稳定、可区分，但不可读（Java 侧总是有线程名）。
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

// 命名当前线程：既更新日志用名，也尽力设置 OS 线程名（便于 gdb / ps -M 观察）。
// 客户端内部的工作线程在启动时调用它，对应 Java 的 ThreadFactory 命名。
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

// file/line 是**调用点**（由 logger_* 宏传入），对应 Java 的 %M:%L。
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

// ------------------------------------------------------------------ 入口宏
// 用宏而不是函数，是为了捕获**调用点**的 __FILE__/__LINE__（对应 Java 日志里的 %M:%L）。
// 这些宏目前都只接受**一个**实参，实参内部允许出现逗号（例如字符串拼接里的逗号）。
#define logger_debug(msg) \
    ::rocketmq::writeLogLine(::rocketmq::LOG_DEBUG, (msg), __FILE__, __LINE__)
#define logger_info(msg) \
    ::rocketmq::writeLogLine(::rocketmq::LOG_INFO, (msg), __FILE__, __LINE__)
#define logger_warn(msg) \
    ::rocketmq::writeLogLine(::rocketmq::LOG_WARN, (msg), __FILE__, __LINE__)
#define logger_error(msg) \
    ::rocketmq::writeLogLine(::rocketmq::LOG_ERROR, (msg), __FILE__, __LINE__)

#endif  // ROCKETMQ_COMMON_LOGGING_H
