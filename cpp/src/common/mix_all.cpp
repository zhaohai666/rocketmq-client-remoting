// MixAll implementation (the properties and group-classification parts).
#include "rocketmq/common/mix_all.h"

#include <string>
#include <vector>

#include "rocketmq/common/util_all.h"

namespace rocketmq {

namespace {

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

// Split into lines on \r\n / \n / \r
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

const std::string& MixAll::cachedIpStr() {
    // Function-local static: since C++11 its initialization is thread-safe, and the client may start on several threads at once
    static const std::string ip = UtilAll::localIp();
    return ip;
}

int32_t MixAll::pid() { return UtilAll::pid(); }

std::string MixAll::properties2String(const PropertyMap& properties) {
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
    PropertyMap result;

    // Merge continuation lines into logical lines first
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
        // An odd number of trailing backslashes means a continuation line
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
        // Skip whitespace before the separator
        while (i < n && isSpaceChar(line[i])) ++i;
        // The optional '=' / ':' and the whitespace after it
        if (i < n && (line[i] == '=' || line[i] == ':')) {
            ++i;
            while (i < n && isSpaceChar(line[i])) ++i;
        }
        result[key] = line.substr(i);
    }
    return result;
}

}  // namespace rocketmq
