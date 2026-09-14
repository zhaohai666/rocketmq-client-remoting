// Java 对齐守卫：校验 codes.h 的常量与 Java 源码一致。
//
// 两级校验：
//   1) 与内置的"权威取值表"逐项比对（防止 C++ 侧被误改）；
//   2) 若设置环境变量 ROCKETMQ_JAVA_SRC（指向 RocketMQ Java 源码树根），
//      额外读取 RequestCode.java 做同名常量的回归校验。未设置则跳过（不算失败）。
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "rocketmq/remoting/protocol/codes.h"

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

// 在 Java 文本中查找 `name = <int> ;`（允许空白），返回是否命中并输出取值
static bool parseJavaConst(const std::string& text, const std::string& name, long long& out) {
    size_t from = 0;
    while (true) {
        size_t p = text.find(name, from);
        if (p == std::string::npos) return false;
        // 名称后应当是空白或 '='
        size_t q = p + name.size();
        while (q < text.size() && (text[q] == ' ' || text[q] == '\t')) ++q;
        if (q < text.size() && text[q] == '=') {
            ++q;
            while (q < text.size() && (text[q] == ' ' || text[q] == '\t')) ++q;
            bool neg = false;
            if (q < text.size() && text[q] == '-') { neg = true; ++q; }
            if (q < text.size() && std::isdigit(static_cast<unsigned char>(text[q]))) {
                long long v = 0;
                while (q < text.size() && std::isdigit(static_cast<unsigned char>(text[q]))) {
                    v = v * 10 + (text[q] - '0');
                    ++q;
                }
                out = neg ? -v : v;
                return true;
            }
        }
        from = p + name.size();
    }
}

static void checkBuiltinTable() {
    // RequestCode
    CHECK(RequestCode::SEND_MESSAGE == 10, "SEND_MESSAGE==10");
    CHECK(RequestCode::PULL_MESSAGE == 11, "PULL_MESSAGE==11");
    CHECK(RequestCode::QUERY_MESSAGE == 12, "QUERY_MESSAGE==12");
    CHECK(RequestCode::QUERY_CONSUMER_OFFSET == 14, "QUERY_CONSUMER_OFFSET==14");
    CHECK(RequestCode::UPDATE_CONSUMER_OFFSET == 15, "UPDATE_CONSUMER_OFFSET==15");
    CHECK(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP == 29, "SEARCH_OFFSET_BY_TIMESTAMP==29");
    CHECK(RequestCode::GET_MAX_OFFSET == 30, "GET_MAX_OFFSET==30");
    CHECK(RequestCode::GET_MIN_OFFSET == 31, "GET_MIN_OFFSET==31");
    CHECK(RequestCode::HEART_BEAT == 34, "HEART_BEAT==34");
    CHECK(RequestCode::UNREGISTER_CLIENT == 35, "UNREGISTER_CLIENT==35");
    CHECK(RequestCode::CONSUMER_SEND_MSG_BACK == 36, "CONSUMER_SEND_MSG_BACK==36");
    CHECK(RequestCode::END_TRANSACTION == 37, "END_TRANSACTION==37");
    CHECK(RequestCode::GET_CONSUMER_LIST_BY_GROUP == 38, "GET_CONSUMER_LIST_BY_GROUP==38");
    CHECK(RequestCode::CHECK_TRANSACTION_STATE == 39, "CHECK_TRANSACTION_STATE==39");
    CHECK(RequestCode::NOTIFY_CONSUMER_IDS_CHANGED == 40, "NOTIFY_CONSUMER_IDS_CHANGED==40");
    CHECK(RequestCode::LOCK_BATCH_MQ == 41, "LOCK_BATCH_MQ==41");
    CHECK(RequestCode::UNLOCK_BATCH_MQ == 42, "UNLOCK_BATCH_MQ==42");
    CHECK(RequestCode::GET_ROUTEINFO_BY_TOPIC == 105, "GET_ROUTEINFO_BY_TOPIC==105");
    CHECK(RequestCode::GET_BROKER_CLUSTER_INFO == 106, "GET_BROKER_CLUSTER_INFO==106");
    CHECK(RequestCode::SEND_BATCH_MESSAGE == 320, "SEND_BATCH_MESSAGE==320");

    // ResponseCode
    CHECK(ResponseCode::SUCCESS == 0, "SUCCESS==0");
    CHECK(ResponseCode::SYSTEM_ERROR == 1, "SYSTEM_ERROR==1");
    CHECK(ResponseCode::TOPIC_NOT_EXIST == 17, "TOPIC_NOT_EXIST==17");
    CHECK(ResponseCode::PULL_NOT_FOUND == 19, "PULL_NOT_FOUND==19");
    CHECK(ResponseCode::PULL_OFFSET_MOVED == 21, "PULL_OFFSET_MOVED==21");
    CHECK(ResponseCode::QUERY_NOT_FOUND == 22, "QUERY_NOT_FOUND==22");
    CHECK(ResponseCode::TRANSACTION_SHOULD_COMMIT == 200, "TRANSACTION_SHOULD_COMMIT==200");

    // LanguageCode / SerializeType
    CHECK(LanguageCode::JAVA == 0 && LanguageCode::CPP == 1 && LanguageCode::PYTHON == 3,
          "LanguageCode JAVA/CPP/PYTHON");
    CHECK(SerializeType::JSON == 0 && SerializeType::ROCKETMQ == 1, "SerializeType JSON/ROCKETMQ");

    // valueOf 反查
    CHECK(std::string(LanguageCode::valueOf(LanguageCode::CPP)) == "CPP", "LanguageCode::valueOf");
    CHECK(LanguageCode::valueOf(999) == nullptr, "LanguageCode::valueOf unknown -> nullptr");
    CHECK(std::string(SerializeType::valueOf(SerializeType::ROCKETMQ)) == "ROCKETMQ",
          "SerializeType::valueOf");
}

static bool readFile(const std::string& path, std::string& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    std::ostringstream ss;
    ss << in.rdbuf();
    out = ss.str();
    return true;
}

static void checkAgainstJavaSource() {
    const char* src = std::getenv("ROCKETMQ_JAVA_SRC");
    if (src == nullptr || *src == '\0') {
        std::cout << "[skip] ROCKETMQ_JAVA_SRC not set; Java source regression check skipped\n";
        return;
    }
    std::vector<std::string> candidates = {
        std::string(src) + "/remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RequestCode.java",
        std::string(src) + "/common/src/main/java/org/apache/rocketmq/remoting/protocol/RequestCode.java",
        std::string(src) + "/remoting/src/main/java/org/apache/rocketmq/common/protocol/RequestCode.java",
    };
    std::string text;
    bool found = false;
    for (const auto& c : candidates) {
        if (readFile(c, text)) { found = true; break; }
    }
    if (!found) {
        std::cout << "[skip] RequestCode.java not found under ROCKETMQ_JAVA_SRC=" << src << "\n";
        return;
    }

    struct Pair { const char* name; long long expect; };
    const Pair pairs[] = {
        {"SEND_MESSAGE", RequestCode::SEND_MESSAGE},
        {"PULL_MESSAGE", RequestCode::PULL_MESSAGE},
        {"HEART_BEAT", RequestCode::HEART_BEAT},
        {"GET_MAX_OFFSET", RequestCode::GET_MAX_OFFSET},
        {"GET_ROUTEINFO_BY_TOPIC", RequestCode::GET_ROUTEINFO_BY_TOPIC},
        {"SEND_BATCH_MESSAGE", RequestCode::SEND_BATCH_MESSAGE},
    };
    for (const auto& pr : pairs) {
        long long v = 0;
        if (parseJavaConst(text, pr.name, v)) {
            CHECK(v == pr.expect, std::string("java ") + pr.name + " matches codes.h");
        } else {
            std::cout << "[warn] java const " << pr.name << " not found; skipped\n";
        }
    }
}

int main() {
    checkBuiltinTable();
    checkAgainstJavaSource();

    std::cout << "\n===== java alignment summary =====\n";
    std::cout << "  PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    if (g_fail == 0) return 0;
    return 1;
}
