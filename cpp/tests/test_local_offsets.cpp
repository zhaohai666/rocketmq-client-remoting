// Local offset store unit tests: object-as-key wire format, parsing and save/load rotation (offline).
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
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

// Real fastjson2 2.0.59 + rocketmq-client 5.5.0 output (compact form plus the pretty variant)
const char* kWireCompact =
    "{\"offsetTable\":{{\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"Tt\"}:9,"
    "{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7}}";

const char* kWirePretty =
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

void testBuildUsesObjectAsKeyFormat() {
    std::string text = DefaultMQPushConsumer::buildLocalOffsetsJson(
        items({{"Ttbroker-a0", 7}, {"Ttbroker-a1", 9}}), mqMap());
    expect(text.rfind("{\"offsetTable\":{", 0) == 0, "build_format_prefix", text);
    // Field order brokerName/queueId/topic (fastjson2 alphabetical), each key is an embedded object
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

void testParseCompatFormatCompactAndPretty() {
    auto compact = DefaultMQPushConsumer::parseLocalOffsetsText(kWireCompact);
    expect(compact.has_value() && (*compact)["Ttbroker-a1"] == 9 && (*compact)["Ttbroker-a0"] == 7,
           "parse_compact_fastjson2_shape");
    auto pretty = DefaultMQPushConsumer::parseLocalOffsetsText(kWirePretty);
    expect(pretty.has_value() && (*pretty)["Ttbroker-a0"] == 7, "parse_pretty_fastjson2_shape");
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

void testSaveWritesCompatFormatAndRollsBak() {
    std::string dir = tempDir("bak");
    std::string path = dir + "/offsets.json";
    std::map<std::string, MessageQueue> map = mqMap();

    DefaultMQPushConsumer::saveLocalOffsetsAt(
        path, items({{"Ttbroker-a0", 7}, {"Ttbroker-a1", 9}, {"Ttbroker-a2", 11}}), map);
    std::string raw = readFile(path);
    expect(raw.rfind("{\"offsetTable\":{", 0) == 0, "save_wire_format_prefix", raw);
    expect(!std::filesystem::exists(path + ".bak"), "save_first_no_bak");

    DefaultMQPushConsumer::saveLocalOffsetsAt(path, items({{"Ttbroker-a0", 99}}), map);
    expect(readFile(path + ".bak") == raw, "save_bak_is_previous");

    // Main file present -> read the main file
    std::map<std::string, int64_t> loaded = DefaultMQPushConsumer::loadLocalOffsetsAt(path);
    expect(loaded["Ttbroker-a0"] == 99, "load_main_wins");

    // Main file missing -> .bak (previous generation)
    std::error_code ec;
    std::filesystem::remove(path, ec);
    loaded = DefaultMQPushConsumer::loadLocalOffsetsAt(path);
    expect(loaded["Ttbroker-a0"] == 7, "load_falls_to_bak");

    // .bak also missing -> empty (treated as a first startup)
    std::filesystem::remove(path + ".bak", ec);
    expect(DefaultMQPushConsumer::loadLocalOffsetsAt(path).empty(), "load_both_missing_empty");

    std::filesystem::remove_all(dir, ec);
}

void testLoadReadsCompatWrittenFile() {
    std::string dir = tempDir("legacy");
    std::string path = dir + "/offsets.json";
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    {
        std::ofstream f(path, std::ios::trunc);
        f << kWireCompact;
    }
    std::map<std::string, int64_t> loaded = DefaultMQPushConsumer::loadLocalOffsetsAt(path);
    expect(loaded["Ttbroker-a1"] == 9 && loaded["Ttbroker-a0"] == 7, "load_wire_written");

    std::filesystem::remove_all(dir, ec);
}

}  // namespace

int main() {
    testBuildUsesObjectAsKeyFormat();
    testBuildSkipsEntriesWithoutQueueInfo();
    testParseCompatFormatCompactAndPretty();
    testParseAcceptsLegacyFlatMap();
    testParseRejectsGarbage();
    testRoundTripBuiltText();
    testSaveWritesCompatFormatAndRollsBak();
    testLoadReadsCompatWrittenFile();

    std::printf("%s: %d checks, %d failures\n", "local_offsets", checks, fails);
    return fails == 0 ? 0 : 1;
}
