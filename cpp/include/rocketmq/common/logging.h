// 极简日志（对应 Python 侧 rocketmq/logging.py 的语义）。
//
// 设计目标与 Python 侧一致：
//   - 默认级别 INFO，因此**良性事件**（如长轮询超时）走 DEBUG 会被抑制，
//     不污染运行日志；
//   - 同时输出到 stderr 与文件，便于"日志是否干净"的可验证性。
//
// 级别可用环境变量覆盖：
//   ROCKETMQ_CPP_LOG_LEVEL = DEBUG | INFO | WARN | ERROR | OFF   （默认 INFO）
//   ROCKETMQ_CPP_LOG_FILE  = 日志文件绝对路径
//                            （默认 $HOME/logs/rocketmqlogs/rocketmq_cpp_client.log；
//                             设为 "OFF" 或空串可关闭文件输出）
#ifndef ROCKETMQ_COMMON_LOGGING_H
#define ROCKETMQ_COMMON_LOGGING_H

#include <atomic>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <mutex>
#include <string>

namespace rocketmq {

enum LogLevel {
    LOG_DEBUG = 0,
    LOG_INFO = 1,
    LOG_WARN = 2,
    LOG_ERROR = 3,
    LOG_OFF = 4,
};

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

inline void setLogLevel(int level) { logLevelRef().store(level); }
inline int logLevel() { return logLevelRef().load(); }

// 解析日志文件路径：返回空串表示关闭文件输出。
inline const std::string& logFilePath() {
    static const std::string path = []() -> std::string {
        const char* env = std::getenv("ROCKETMQ_CPP_LOG_FILE");
        if (env != nullptr) {
            std::string s(env);
            if (s.empty() || s == "OFF" || s == "NONE") return std::string();
            return s;
        }
        const char* home = std::getenv("HOME");
        if (home == nullptr) return std::string();
        return std::string(home) + "/logs/rocketmqlogs/rocketmq_cpp_client.log";
    }();
    return path;
}

inline std::mutex& logMutex() {
    static std::mutex m;
    return m;
}

inline const char* logLevelName(int level) {
    switch (level) {
        case LOG_DEBUG: return "DEBUG";
        case LOG_INFO: return "INFO";
        case LOG_WARN: return "WARN";
        case LOG_ERROR: return "ERROR";
        default: return "?";
    }
}

inline void writeLogLine(int level, const std::string& msg) {
    if (level < logLevel()) return;

    std::time_t secs = std::time(nullptr);
    std::tm tmv{};
#if defined(_WIN32)
    localtime_s(&tmv, &secs);
#else
    localtime_r(&secs, &tmv);
#endif
    char ts[32];
    ts[0] = '\0';
    std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);

    const char* lv = logLevelName(level);
    std::fprintf(stderr, "%s [%s] %s\n", ts, lv, msg.c_str());

    const std::string& path = logFilePath();
    if (path.empty()) return;
    std::lock_guard<std::mutex> lk(logMutex());
    // 首次写入时创建目录（失败则静默降级为仅 stderr）
    static bool dirReady = false;
    if (!dirReady) {
        dirReady = true;
        std::error_code ec;
        std::filesystem::create_directories(
            std::filesystem::path(path).parent_path(), ec);
    }
    if (std::FILE* f = std::fopen(path.c_str(), "a")) {
        std::fprintf(f, "%s [%s] %s\n", ts, lv, msg.c_str());
        std::fclose(f);
    }
}

inline void logger_debug(const std::string& msg) { writeLogLine(LOG_DEBUG, msg); }
inline void logger_info(const std::string& msg) { writeLogLine(LOG_INFO, msg); }
inline void logger_warn(const std::string& msg) { writeLogLine(LOG_WARN, msg); }
inline void logger_error(const std::string& msg) { writeLogLine(LOG_ERROR, msg); }

// const char* / 字面量重载
inline void logger_debug(const char* msg) { writeLogLine(LOG_DEBUG, std::string(msg)); }
inline void logger_info(const char* msg) { writeLogLine(LOG_INFO, std::string(msg)); }
inline void logger_warn(const char* msg) { writeLogLine(LOG_WARN, std::string(msg)); }
inline void logger_error(const char* msg) { writeLogLine(LOG_ERROR, std::string(msg)); }

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_LOGGING_H
