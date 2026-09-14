// MixAll 的实现（对应 Python common/mix_all.py 中的 properties 与分组判定部分）。
#include "rocketmq/common/mix_all.h"

#include <string>
#include <vector>

#include "rocketmq/common/util_all.h"

namespace rocketmq {

namespace {

// 对应 Java MixAll.PREDEFINE_GROUP_SET
const char* const kPredefineGroups[] = {
    MixAll::DEFAULT_CONSUMER_GROUP,
    MixAll::DEFAULT_PRODUCER_GROUP,
    MixAll::TOOLS_CONSUMER_GROUP,
    MixAll::SCHEDULE_CONSUMER_GROUP,
    MixAll::FILTERSRV_CONSUMER_GROUP,
    MixAll::MONITOR_CONSUMER_GROUP,
    MixAll::CLIENT_INNER_PRODUCER_GROUP,
    MixAll::SELF_TEST_PRODUCER_GROUP,
    MixAll::SELF_TEST_CONSUMER_GROUP,
    MixAll::ONS_HTTP_PROXY_GROUP,
    MixAll::CID_ONSAPI_PERMISSION_GROUP,
    MixAll::CID_ONSAPI_OWNER_GROUP,
    MixAll::CID_ONSAPI_PULL_GROUP,
    MixAll::CID_SYS_RMQ_TRANS,
};

bool isSpaceChar(char c) { return c == ' ' || c == '\t' || c == '\f'; }

std::string trimLeft(const std::string& s) {
    size_t i = 0;
    while (i < s.size() && isSpaceChar(s[i])) ++i;
    return s.substr(i);
}

std::string trimBoth(const std::string& s) {
    size_t b = 0;
    while (b < s.size() && isSpaceChar(s[b])) ++b;
    size_t e = s.size();
    while (e > b && isSpaceChar(s[e - 1])) --e;
    return s.substr(b, e - b);
}

// 按 \r\n / \n / \r 切行（等价于 Python str.splitlines 对本场景的行为）
std::vector<std::string> splitLines(const std::string& text) {
    std::vector<std::string> lines;
    std::string cur;
    for (size_t i = 0; i < text.size(); ++i) {
        char c = text[i];
        if (c == '\n') {
            lines.push_back(cur);
            cur.clear();
        } else if (c == '\r') {
            lines.push_back(cur);
            cur.clear();
            if (i + 1 < text.size() && text[i + 1] == '\n') ++i;
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) lines.push_back(cur);
    return lines;
}

}  // namespace

bool MixAll::isPredefinedGroup(const std::string& consumerGroup) {
    for (const char* g : kPredefineGroups) {
        if (consumerGroup == g) return true;
    }
    return false;
}

std::string MixAll::getIpStr() { return UtilAll::localIp(); }

int32_t MixAll::pid() { return UtilAll::pid(); }

std::string MixAll::properties2String(const PropertyMap& properties) {
    // Java: 每条 "key=value\n"（Properties.store 风格用 "=" 作分隔符），null 值跳过。
    // 这里 PropertyMap 的值是 std::string，没有 null 概念，故不做过滤；
    // 调用方若想跳过某键，不要放进 map 即可。
    std::string out;
    for (const auto& kv : properties) {
        out += kv.first;
        out += '=';
        out += kv.second;
        out += '\n';
    }
    return out;
}

PropertyMap MixAll::string2Properties(const std::string& text) {
    // 语义对齐 Java MixAll.string2Properties -> java.util.Properties.load：
    //   1) 行尾未转义的 '\' 表示续行（下一行前导空白被丢弃）；
    //   2) 空行、以 '#' 或 '!' 开头的行是注释；
    //   3) 键与值以**第一个** '='、':' 或**空白**分隔（空白也是合法分隔符！）；
    //   4) 分隔符前后的空白被跳过；值的**尾部**空白保留（Java 不去尾空白）。
    //
    // 注：Java 还会处理 \t \n \uXXXX 等转义，broker 配置导出里不出现，
    // 这里不实现（把反斜杠语义做错反而更不一致）。
    PropertyMap result;

    // 先把续行合并成逻辑行
    std::vector<std::string> logical;
    std::string pending;
    bool hasPending = false;
    for (const std::string& raw : splitLines(text)) {
        std::string line = raw;
        if (hasPending) {
            line = pending + trimLeft(line);
            pending.clear();
            hasPending = false;
        }
        // 行尾反斜杠个数为奇数 => 续行
        size_t trailing = 0;
        for (size_t i = line.size(); i > 0 && line[i - 1] == '\\'; --i) ++trailing;
        if (trailing % 2 == 1) {
            pending = line.substr(0, line.size() - 1);
            hasPending = true;
            continue;
        }
        logical.push_back(line);
    }
    if (hasPending) logical.push_back(pending);

    for (const std::string& line : logical) {
        std::string stripped = trimBoth(line);
        if (stripped.empty()) continue;
        if (stripped[0] == '#' || stripped[0] == '!') continue;

        size_t n = line.size();
        size_t i = 0;
        while (i < n && isSpaceChar(line[i])) ++i;
        size_t keyStart = i;
        while (i < n && line[i] != '=' && line[i] != ':' && !isSpaceChar(line[i])) ++i;
        std::string key = line.substr(keyStart, i - keyStart);
        // 跳过分隔符前的空白
        while (i < n && isSpaceChar(line[i])) ++i;
        // 可选的 '=' / ':' 及其后的空白
        if (i < n && (line[i] == '=' || line[i] == ':')) {
            ++i;
            while (i < n && isSpaceChar(line[i])) ++i;
        }
        result[key] = line.substr(i);
    }
    return result;
}

}  // namespace rocketmq
