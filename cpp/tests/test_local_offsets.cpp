// 广播模式本地位点文件（Java LocalFileOffsetStore）的格式对齐守卫 —— 不需要集群。
//
// Java 侧 fastjson2 序列化 Map<MessageQueue, AtomicLong> 时把 MessageQueue 对象直接
// 当 JSON key 写出（用真实 jar 跑 OffsetSerializeWrapper.toJson 实测）：
//   {"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9,...}}
// 严格 JSON 非法，但 fastjson2 自产自销能读回。四端按同一格式落盘，Java 消费者
// 与本端消费者共用同一份 ~/.rocketmq_offsets/<clientId>/<group>/offsets.json 时互认。
//
// 与 python/tests/test_local_offsets.py、rust/src/client/consumer.rs（mod tests）、
// dotnet/tests/RocketMQ.Client.Tests/LocalOffsetsTests.cs 的同名测试同题。
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>

#include "rocketmq/client/consumer.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

// 真实 fastjson2 2.0.59 + rocketmq-client 5.5.0 的输出（含 pretty 版）
const char* kJavaCompact =
    "{\"offsetTable\":{{\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"Tt\"}:9,"
    "{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7}}";

const char* kJavaPretty =
    "{\n\t\"offsetTable\":{\n\t\t{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7\n\t}\n}";

std::map<std::string, MessageQueue> mqMap() {
    std::map<std::string, MessageQueue> map;
    for (int32_t i = 0; i < 3; ++i) {
        MessageQueue mq("Tt", "broker-a", i);
        map[DefaultMQPushConsumer::offsetKey(mq)] = mq;
    }
    return map;
}

std::map<std::string, int64_t> items(
    std::initializer_list<std::pair<const std::string, int64_t>> pairs) {
    return std::map<std::string, int64_t>(pairs);
}

std::string readFile(const std::string& path) {
    std::ifstream f(path);
    return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

std::string tempDir(const char* tag) {
    static int seq = 0;
    std::string dir = (std::filesystem::temp_directory_path() /
                       ("rmq_cpp_offsets_" + std::string(tag) + "_" +
                        std::to_string(++seq) + "_" + std::to_string(::time(nullptr))))
                          .string();
    return dir;
}

void testBuildUsesJavaObjectAsKeyFormat() {
    std::string text = DefaultMQPushConsumer::buildLocalOffsetsJson(
        items({{"Ttbroker-a0", 7}, {"Ttbroker-a1", 9}}), mqMap());
    expect(text.rfind("{\"offsetTable\":{", 0) == 0, "build_format_prefix", text);
    // 字段序 brokerName/queueId/topic（fastjson2 字母序），每个 key 是内嵌对象
    expect(text.find("\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7") !=
               std::string::npos,
           "build_format_q0");
    expect(text.find("\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"Tt\"}:9") !=
               std::string::npos,
           "build_format_q1");
}

void testBuildSkipsEntriesWithoutQueueInfo() {
    std::string text = DefaultMQPushConsumer::buildLocalOffsetsJson(
        items({{"Ttbroker-a0", 7}, {"Orphan_b0", 3}}), mqMap());
    expect(text.find("Orphan") == std::string::npos, "build_orphan_skipped", text);
}

void testParseJavaFormatCompactAndPretty() {
    auto compact = DefaultMQPushConsumer::parseLocalOffsetsText(kJavaCompact);
    expect(compact.has_value() && (*compact)["Ttbroker-a1"] == 9 && (*compact)["Ttbroker-a0"] == 7,
           "parse_java_compact");
    auto pretty = DefaultMQPushConsumer::parseLocalOffsetsText(kJavaPretty);
    expect(pretty.has_value() && (*pretty)["Ttbroker-a0"] == 7, "parse_java_pretty");
}

void testParseAcceptsLegacyFlatMap() {
    auto flat = DefaultMQPushConsumer::parseLocalOffsetsText("{\"Ttbroker-a0\":5}");
    expect(flat.has_value() && (*flat)["Ttbroker-a0"] == 5, "parse_legacy_flat");
}

void testParseRejectsGarbage() {
    expect(!DefaultMQPushConsumer::parseLocalOffsetsText("{\"offsetTable\":{{{").has_value(),
           "reject_double_open");
    expect(!DefaultMQPushConsumer::parseLocalOffsetsText("{\"offsetTable\":").has_value(),
           "reject_truncated");
    expect(!DefaultMQPushConsumer::parseLocalOffsetsText("").has_value(), "reject_empty");
}

void testRoundTripBuiltText() {
    std::string text = DefaultMQPushConsumer::buildLocalOffsetsJson(
        items({{"Ttbroker-a0", 7}, {"Ttbroker-a1", 9}, {"Ttbroker-a2", 11}}), mqMap());
    auto parsed = DefaultMQPushConsumer::parseLocalOffsetsText(text);
    expect(parsed.has_value() && (*parsed)["Ttbroker-a0"] == 7 && (*parsed)["Ttbroker-a1"] == 9 &&
               (*parsed)["Ttbroker-a2"] == 11,
           "roundtrip");
}

void testSaveWritesJavaFormatAndRollsBak() {
    std::string dir = tempDir("bak");
    std::string path = dir + "/offsets.json";
    std::map<std::string, MessageQueue> map = mqMap();

    DefaultMQPushConsumer::saveLocalOffsetsAt(
        path, items({{"Ttbroker-a0", 7}, {"Ttbroker-a1", 9}, {"Ttbroker-a2", 11}}), map);
    std::string raw = readFile(path);
    expect(raw.rfind("{\"offsetTable\":{", 0) == 0, "save_java_format_prefix", raw);
    // 首次写不产生 .bak（Java string2File 只在已有旧内容时滚动）
    expect(!std::filesystem::exists(path + ".bak"), "save_first_no_bak");

    DefaultMQPushConsumer::saveLocalOffsetsAt(path, items({{"Ttbroker-a0", 99}}), map);
    // .bak = 上一代内容（Java MixAll.string2File 语义）
    expect(readFile(path + ".bak") == raw, "save_bak_is_previous");

    // 主文件在 → 读主文件
    std::map<std::string, int64_t> loaded = DefaultMQPushConsumer::loadLocalOffsetsAt(path);
    expect(loaded["Ttbroker-a0"] == 99, "load_main_wins");

    // 主文件缺失 → .bak（上一代）
    std::error_code ec;
    std::filesystem::remove(path, ec);
    loaded = DefaultMQPushConsumer::loadLocalOffsetsAt(path);
    expect(loaded["Ttbroker-a0"] == 7, "load_falls_to_bak");

    // .bak 也缺失 → 空（按首次启动处理）
    std::filesystem::remove(path + ".bak", ec);
    expect(DefaultMQPushConsumer::loadLocalOffsetsAt(path).empty(), "load_both_missing_empty");

    std::filesystem::remove_all(dir, ec);
}

void testLoadReadsJavaWrittenFile() {
    // Java 写出的文件本端要能读（跨端互认的核心）
    std::string dir = tempDir("java");
    std::string path = dir + "/offsets.json";
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    {
        std::ofstream f(path, std::ios::trunc);
        f << kJavaCompact;
    }
    std::map<std::string, int64_t> loaded = DefaultMQPushConsumer::loadLocalOffsetsAt(path);
    expect(loaded["Ttbroker-a1"] == 9 && loaded["Ttbroker-a0"] == 7, "load_java_written");

    std::filesystem::remove_all(dir, ec);
}

}  // namespace

int main() {
    testBuildUsesJavaObjectAsKeyFormat();
    testBuildSkipsEntriesWithoutQueueInfo();
    testParseJavaFormatCompactAndPretty();
    testParseAcceptsLegacyFlatMap();
    testParseRejectsGarbage();
    testRoundTripBuiltText();
    testSaveWritesJavaFormatAndRollsBak();
    testLoadReadsJavaWrittenFile();

    std::printf("%s: %d checks, %d failures\n", "local_offsets", checks, fails);
    return fails == 0 ? 0 : 1;
}
