#include "rocketmq/client/trace_context.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <random>
#include <vector>

namespace rocketmq {

namespace {

const char* const kHexDigits = "0123456789abcdef";

std::string randomHex(size_t n) {
    static thread_local std::mt19937_64 rng(
        std::random_device{}() ^ static_cast<uint64_t>(std::chrono::steady_clock::now()
                                                           .time_since_epoch().count()));
    std::string out;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        out.push_back(kHexDigits[rng() % 16]);
    }
    return out;
}

std::string toLower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

bool allHex(const std::string& s) {
    if (s.empty()) return false;
    for (char c : s) {
        if (!std::isxdigit(static_cast<unsigned char>(c))) return false;
    }
    return true;
}

bool allZero(const std::string& s) {
    for (char c : s) {
        if (c != '0') return false;
    }
    return true;
}

}  // namespace

std::string generateTraceparent() {
    // 00-<trace-id 32hex>-<parent-id 16hex>-01（flags 01 = recorded）
    return "00-" + randomHex(32) + "-" + randomHex(16) + "-01";
}

bool isValidTraceparent(const std::string& value) {
    if (value.empty()) return false;
    // 宽松：允许首尾空白，段内按小写比对
    std::string v = value;
    // trim
    size_t b = v.find_first_not_of(" \t\r\n");
    size_t e = v.find_last_not_of(" \t\r\n");
    if (b == std::string::npos) return false;
    v = v.substr(b, e - b + 1);

    // 按 '-' 切 4 段
    std::vector<std::string> parts;
    size_t pos = 0;
    while (true) {
        size_t dash = v.find('-', pos);
        if (dash == std::string::npos) {
            parts.push_back(v.substr(pos));
            break;
        }
        parts.push_back(v.substr(pos, dash - pos));
        pos = dash + 1;
    }
    if (parts.size() != 4) return false;
    const std::string version = parts[0];
    const std::string traceId = toLower(parts[1]);
    const std::string parentId = toLower(parts[2]);
    const std::string flags = toLower(parts[3]);
    // version：00 或 2 位 hex 且不是 ff
    if (version == "ff") return false;
    if (version != "00") {
        if (version.size() != 2 || !allHex(version)) return false;
    }
    if (traceId.size() != 32 || !allHex(traceId) || allZero(traceId)) return false;
    if (parentId.size() != 16 || !allHex(parentId) || allZero(parentId)) return false;
    if (flags.size() != 2 || !allHex(flags)) return false;
    return true;
}

std::string childTraceparent(const std::string& parent) {
    if (!isValidTraceparent(parent)) return std::string();
    // 同一 trace-id，换 parent-id（对齐 W3C：子 span 复用 trace-id）
    size_t first = parent.find('-');
    size_t second = parent.find('-', first + 1);
    const std::string traceId = toLower(parent.substr(first + 1, second - first - 1));
    return "00-" + traceId + "-" + randomHex(16) + "-01";
}

std::string injectTraceContext(Message* msg) {
    if (msg == nullptr) return std::string();
    const std::string existing = msg->getProperty(kTraceContextProperty);
    if (!existing.empty()) return existing;  // 上游传播优先，不覆盖
    const std::string tp = generateTraceparent();
    msg->putProperty(kTraceContextProperty, tp);
    return tp;
}

std::string extractTraceparent(const Message& msg) {
    return msg.getProperty(kTraceContextProperty);
}

bool traceContextEnabledFromEnv() {
    const char* v = std::getenv("ROCKETMQ_TRACE_CONTEXT_ENABLE");
    if (v == nullptr) return false;
    std::string s(v);
    for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s == "1" || s == "true" || s == "yes";
}

}  // namespace rocketmq
