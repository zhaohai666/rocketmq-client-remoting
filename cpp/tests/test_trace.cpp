// 消息轨迹单测（对齐 python/tests/test_trace.py 的 Java 对拍向量 + client.trace 语义）。
//
// 最有价值的是第一组：EXPECTED_* 的字段序列与 Java 官方
// TraceDataEncoder.encoderFromContextBean 的输出**逐字节一致**（SOH=\x01 / STX=\x02），
// 字段序列一旦改动就与 Java 客户端 / 控制台不兼容。向量的来源见
// python/tests/test_trace.py 头注释（Java 探针 /tmp/TraceParity.java）。
//
// 另外两组是**真机踩出来的回归守卫**，勿删：
//   * 无 keys 的 SubBefore（末段为空）解出来只剩 7 段，必须能解出而不是被跳过/越界；
//   * 一条坏记录不能毁掉整条 trace data（真机表现：2 条轨迹消息只解出 1 条记录）。
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/client/trace.h"
#include "rocketmq/client/trace_dispatcher.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

// 把控制字符转成可读形式，失败时才看得懂
std::string esc(const std::string& s) {
    std::string out;
    for (char c : s) {
        if (c == '\x01') out += "<SOH>";
        else if (c == '\x02') out += "<STX>";
        else out += c;
    }
    return out;
}

void expect(bool ok, const std::string& name) {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("  [FAIL] %s\n", name.c_str());
    }
}

void expectEq(const std::string& actual, const std::string& expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("  [FAIL] %s\n         actual  =%s\n         expected=%s\n", name.c_str(),
                    esc(actual).c_str(), esc(expected).c_str());
    }
}

void expectInt(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("  [FAIL] %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
}

const std::string SOH(TraceConstants::CONTENT_SPLITOR);
const std::string STX(TraceConstants::FIELD_SPLITOR);

std::string joinFields(const std::vector<std::string>& fields) {
    std::string out;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i) out += SOH;
        out += fields[i];
    }
    return out;
}

int64_t countOf(const std::string& s, const std::string& sub) {
    int64_t n = 0;
    size_t pos = 0;
    while ((pos = s.find(sub, pos)) != std::string::npos) {
        ++n;
        pos += sub.size();
    }
    return n;
}

const char* MSG_ID_1 = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6";
const char* MSG_ID_2 = "AC1400A1F0A018B4AAC2A1B2C3D4E5F7";
const char* OFFSET_MSG_ID = "AC1400A1000027100000000000000001";

// ---- Java 官方实现的编码结果（勿手改；改了就与 Java/控制台不兼容）----
std::string expectedPub() {
    return joinFields({"Pub", "1700000000000", "DefaultRegion", "GID_test", "TopicTest", MSG_ID_1,
                       "TagA", "KeyA KeyB", "127.0.0.1:10911", "42", "7", "0", OFFSET_MSG_ID,
                       "true"}) + STX;
}
std::string expectedSubBefore() {
    return joinFields({"SubBefore", "1700000000000", "DefaultRegion", "CID_test", "REQ-SUB-001",
                       MSG_ID_1, "2", "KeyA KeyB"}) + STX +
           joinFields({"SubBefore", "1700000000000", "DefaultRegion", "CID_test", "REQ-SUB-001",
                       MSG_ID_2, "0", "KeyC"}) + STX;
}
std::string expectedSubAfter() {
    return joinFields({"SubAfter", "REQ-SUB-001", MSG_ID_1, "11", "false", "KeyA KeyB", "2",
                       "1700000000000", "CID_test"}) + STX;
}
std::string expectedEndTransaction() {
    return joinFields({"EndTransaction", "1700000000000", "DefaultRegion", "GID_test",
                       "TopicTest", MSG_ID_1, "TagA", "KeyA KeyB", "127.0.0.1:10911", "0",
                       "TRAN-001", "COMMIT_MESSAGE", "false"}) + STX;
}
std::string expectedRecall() {
    return joinFields({"Recall", "1700000000000", "DefaultRegion", "GID_test", "TopicTest",
                       MSG_ID_1, "true"}) + STX;
}

TraceBean makeBean(const std::string& msgId = MSG_ID_1, const std::string& keys = "KeyA KeyB",
                   int32_t retryTimes = 2) {
    TraceBean b;
    b.topic = "TopicTest";
    b.msgId = msgId;
    b.offsetMsgId = OFFSET_MSG_ID;
    b.tags = "TagA";
    b.keys = keys;
    b.storeHost = "127.0.0.1:10911";
    b.storeTime = 1700000000123;
    b.retryTimes = retryTimes;
    b.bodyLength = 42;
    b.msgType = static_cast<int32_t>(TraceMessageType::NORMAL);
    b.transactionId = "TRAN-001";
    b.transactionState = "COMMIT_MESSAGE";
    return b;
}

std::shared_ptr<TraceContext> pubContext(const std::string& topic, const std::string& msgId,
                                        const std::string& keys) {
    auto ctx = std::make_shared<TraceContext>();
    ctx->traceType = TraceType::PUB;
    ctx->timeStamp = 1700000000000;
    ctx->regionId = "DefaultRegion";
    ctx->groupName = "GID_test";
    ctx->costTime = 7;
    ctx->traceBeans = {makeBean(msgId, keys)};
    ctx->traceBeans[0].topic = topic;
    return ctx;
}

// 捕获发送的假分发器（sendTraceMessage 是 protected 虚函数 seam，不打真实网络）
class CapturingDispatcher : public AsyncTraceDispatcher {
public:
    struct Sent {
        std::string traceTopic;
        std::string body;
        std::string keys;
    };
    std::vector<Sent> sent;

    CapturingDispatcher(const std::string& group, TraceDispatcherType type, int32_t batchNum,
                        const std::string& topic)
        : AsyncTraceDispatcher(group, type, batchNum, topic) {}

protected:
    void sendTraceMessage(const std::string& traceTopic, const std::string& body,
                          const std::string& keys) override {
        sent.push_back(Sent{traceTopic, body, keys});
    }
};

}  // namespace

int main() {
    // ================================================= 1. 编码：与 Java 逐字节一致
    {
        TraceContext ctx;
        ctx.traceType = TraceType::PUB;
        ctx.timeStamp = 1700000000000;
        ctx.regionId = "DefaultRegion";
        ctx.groupName = "GID_test";
        ctx.costTime = 7;
        ctx.isSuccess = true;
        ctx.requestId = "REQ-PUB-001";
        ctx.traceBeans = {makeBean()};
        auto tb = TraceDataEncoder::encoderFromContextBean(&ctx);
        expect(tb.has_value(), "pub: encoder returns value");
        if (tb.has_value()) {
            expectEq(tb->transData, expectedPub(), "pub: matches java vector");
            // trans_key = msgId + 按空格拆开的业务 keys
            expect(tb->transKey.size() == 3 && tb->transKey.count(MSG_ID_1) == 1 &&
                       tb->transKey.count("KeyA") == 1 && tb->transKey.count("KeyB") == 1,
                   "pub: trans_key = msgId + business keys");
        }
    }
    {
        TraceContext ctx;
        ctx.traceType = TraceType::SUB_BEFORE;
        ctx.timeStamp = 1700000000000;
        ctx.regionId = "DefaultRegion";
        ctx.groupName = "CID_test";
        ctx.requestId = "REQ-SUB-001";
        ctx.traceBeans = {makeBean(), makeBean(MSG_ID_2, "KeyC", 0)};
        auto tb = TraceDataEncoder::encoderFromContextBean(&ctx);
        expect(tb.has_value(), "sub_before: encoder returns value");
        if (tb.has_value()) {
            expectEq(tb->transData, expectedSubBefore(),
                     "sub_before: matches java vector (2 beans)");
            expectInt(static_cast<long long>(tb->transKey.size()), 5,
                      "sub_before: trans_key merges both beans");
        }
    }
    {
        TraceContext ctx;
        ctx.traceType = TraceType::SUB_AFTER;
        ctx.timeStamp = 1700000000000;
        ctx.groupName = "CID_test";
        ctx.requestId = "REQ-SUB-001";
        ctx.costTime = 11;
        ctx.isSuccess = false;
        ctx.contextCode = 2;
        ctx.accessChannel = AccessChannel::LOCAL;
        ctx.traceBeans = {makeBean()};
        auto tb = TraceDataEncoder::encoderFromContextBean(&ctx);
        expect(tb.has_value() && tb->transData == expectedSubAfter(),
               "sub_after: matches java vector");
    }
    {
        // CLOUD 通道不追加 timestamp + groupName 两段（Java TraceDataEncoder:208）
        TraceContext ctx;
        ctx.traceType = TraceType::SUB_AFTER;
        ctx.timeStamp = 1700000000000;
        ctx.groupName = "CID_test";
        ctx.requestId = "REQ-SUB-001";
        ctx.costTime = 11;
        ctx.isSuccess = false;
        ctx.contextCode = 2;
        ctx.accessChannel = AccessChannel::CLOUD;
        ctx.traceBeans = {makeBean()};
        auto tb = TraceDataEncoder::encoderFromContextBean(&ctx);
        expect(tb.has_value(), "sub_after CLOUD: encoder returns value");
        if (tb.has_value()) {
            expectEq(tb->transData,
                     joinFields({"SubAfter", "REQ-SUB-001", MSG_ID_1, "11", "false", "KeyA KeyB",
                                 "2"}) + STX,
                     "sub_after: CLOUD drops timestamp+group");
        }
    }
    {
        TraceContext ctx;
        ctx.traceType = TraceType::END_TRANSACTION;
        ctx.timeStamp = 1700000000000;
        ctx.regionId = "DefaultRegion";
        ctx.groupName = "GID_test";
        ctx.traceBeans = {makeBean()};
        auto tb = TraceDataEncoder::encoderFromContextBean(&ctx);
        expect(tb.has_value() && tb->transData == expectedEndTransaction(),
               "end_tx: matches java vector");
    }
    {
        TraceContext ctx;
        ctx.traceType = TraceType::RECALL;
        ctx.timeStamp = 1700000000000;
        ctx.regionId = "DefaultRegion";
        ctx.groupName = "GID_test";
        ctx.isSuccess = true;
        ctx.traceBeans = {makeBean()};
        auto tb = TraceDataEncoder::encoderFromContextBean(&ctx);
        expect(tb.has_value() && tb->transData == expectedRecall(), "recall: matches java vector");
    }
    expect(!TraceDataEncoder::encoderFromContextBean(nullptr).has_value(),
           "encoder(nullptr) -> nullopt");
    {
        TraceContext ctx;  // 未设置 traceType
        expect(!TraceDataEncoder::encoderFromContextBean(&ctx).has_value(),
               "encoder(no traceType) -> nullopt");
        expect(ctx.accessChannelOrLocal() == AccessChannel::LOCAL,
               "accessChannel nullopt -> LOCAL");
        expect(ctx.traceBeans.empty() && ctx.costTime == 0 && ctx.isSuccess,
               "context: defaults");
        expectInt(static_cast<long long>(ctx.requestId.size()), 32, "context: requestId length");
    }

    // ================================================= 2. javaSplit（Java String.split 语义）
    {
        auto parts = TraceDataEncoder::javaSplit("a" + STX, STX);
        expect(parts.size() == 1 && parts[0] == "a", "javaSplit: trailing empty dropped");
        parts = TraceDataEncoder::javaSplit("a" + STX + "b", STX);
        expect(parts.size() == 2 && parts[0] == "a" && parts[1] == "b", "javaSplit: inner split");
        parts = TraceDataEncoder::javaSplit(STX, STX);
        expect(parts.empty(), "javaSplit: only separators -> empty");
        parts = TraceDataEncoder::javaSplit("abc", STX);
        expect(parts.size() == 1 && parts[0] == "abc", "javaSplit: no separator");
    }

    // ================================================= 3. 解码：往返一致
    {
        auto records = TraceDataEncoder::decoderFromTraceDataString(expectedPub());
        expectInt(static_cast<long long>(records.size()), 1, "decode pub: 1 record");
        const TraceContext& c = records[0];
        expect(c.traceType == TraceType::PUB, "decode pub: type");
        expectInt(c.timeStamp, 1700000000000, "decode pub: timeStamp");
        expect(c.regionId == "DefaultRegion" && c.groupName == "GID_test",
               "decode pub: region/group");
        expectInt(c.costTime, 7, "decode pub: costTime");
        expect(c.isSuccess, "decode pub: success");
        expect(c.traceBeans[0].msgId == MSG_ID_1, "decode pub: msgId = UNIQ_KEY");
        expect(c.traceBeans[0].offsetMsgId == OFFSET_MSG_ID, "decode pub: offsetMsgId");
        expectInt(c.traceBeans[0].bodyLength, 42, "decode pub: bodyLength");
        expectInt(c.traceBeans[0].msgType, 0, "decode pub: msgType ordinal");
    }
    {
        auto records = TraceDataEncoder::decoderFromTraceDataString(expectedSubBefore());
        expectInt(static_cast<long long>(records.size()), 2, "decode sub_before: 2 records");
        expect(records[0].traceBeans[0].msgId == MSG_ID_1, "decode sub_before: bean 1 msgId");
        expect(records[1].traceBeans[0].msgId == MSG_ID_2, "decode sub_before: bean 2 msgId");
        expectInt(records[0].traceBeans[0].retryTimes, 2, "decode sub_before: retryTimes");
        expect(records[0].requestId == "REQ-SUB-001", "decode sub_before: requestId");
    }
    {
        auto records = TraceDataEncoder::decoderFromTraceDataString(expectedSubAfter());
        expectInt(static_cast<long long>(records.size()), 1, "decode sub_after: 1 record");
        expectInt(records[0].contextCode, 2, "decode sub_after: contextCode");
        expect(!records[0].isSuccess, "decode sub_after: success=false");
        expectInt(records[0].costTime, 11, "decode sub_after: costTime");
        expect(records[0].groupName == "CID_test", "decode sub_after: groupName");
    }
    {
        auto records = TraceDataEncoder::decoderFromTraceDataString(expectedEndTransaction());
        expectInt(static_cast<long long>(records.size()), 1, "decode end_tx: 1 record");
        const TraceBean& b = records[0].traceBeans[0];
        expect(b.transactionId == "TRAN-001" && b.transactionState == "COMMIT_MESSAGE",
               "decode end_tx: transaction id/state");
        expect(!b.fromTransactionCheck, "decode end_tx: fromTransactionCheck");
        // 向量里的 msgType 段来自 makeBean()（NORMAL=0）；真实客户端发 END_TRANSACTION 轨迹时
        // 钩子会填 Trans_msg_Commit(2)，但编码/解码本身只看 ordinal。
        expectInt(b.msgType, 0, "decode end_tx: msgType ordinal from vector");
    }
    {
        auto records = TraceDataEncoder::decoderFromTraceDataString(expectedRecall());
        expectInt(static_cast<long long>(records.size()), 1, "decode recall: 1 record");
        expect(records[0].isSuccess, "decode recall: success=true");
    }
    {
        // 老版本 Pub（13 段，无 offsetMsgId）
        std::string old = joinFields({"Pub", "1700000000000", "DefaultRegion", "GID_test",
                                      "TopicTest", MSG_ID_1, "TagA", "KeyA KeyB",
                                      "127.0.0.1:10911", "42", "7", "0", "true"}) + STX;
        auto records = TraceDataEncoder::decoderFromTraceDataString(old);
        expectInt(static_cast<long long>(records.size()), 1, "decode pub v13: 1 record");
        expect(records[0].isSuccess, "decode pub v13: success from segment 12");
        expect(records[0].traceBeans[0].offsetMsgId.empty(), "decode pub v13: no offsetMsgId");
    }
    {
        // 带 clientHost 的 15 段形式
        std::string v15 = joinFields({"Pub", "1700000000000", "DefaultRegion", "GID_test",
                                      "TopicTest", MSG_ID_1, "TagA", "KeyA KeyB",
                                      "127.0.0.1:10911", "42", "7", "0", OFFSET_MSG_ID, "true",
                                      "10.0.0.9"}) + STX;
        auto records = TraceDataEncoder::decoderFromTraceDataString(v15);
        expectInt(static_cast<long long>(records.size()), 1, "decode pub v15: 1 record");
        expect(records[0].traceBeans[0].clientHost == "10.0.0.9",
               "decode pub v15: clientHost from segment 14");
    }
    expect(TraceDataEncoder::decoderFromTraceDataString("").empty(), "decode empty -> no records");
    expect(TraceDataEncoder::decoderFromTraceDataString(STX + STX).empty(),
           "decode bare separators -> no records");

    // ================================================= 4. 回归守卫：无 keys 的 SubBefore
    {
        // 被消费的消息没有 keys 时，SubBefore 的末段（keys）为空 → Java String.split
        // 连末尾 CONTENT_SPLITOR 一起丢弃 → 只剩 7 段。Java 原生实现会 AIOOBE，
        // 我们必须解成 keys=""，绝不能按"段数不足"跳过或越界。
        std::string raw = joinFields({"SubBefore", "1700000000000", "DefaultRegion",
                                      "GID_trace_live", "REQ-001", MSG_ID_1, "0", ""}) + STX;
        auto records = TraceDataEncoder::decoderFromTraceDataString(raw);
        expectInt(static_cast<long long>(records.size()), 1,
                  "keyless SubBefore: decoded (not skipped)");
        expect(records[0].traceType == TraceType::SUB_BEFORE, "keyless SubBefore: type");
        expect(records[0].traceBeans[0].keys.empty(), "keyless SubBefore: keys empty");
        expect(records[0].traceBeans[0].msgId == MSG_ID_1, "keyless SubBefore: msgId kept");
        expectInt(records[0].traceBeans[0].retryTimes, 0, "keyless SubBefore: retryTimes");
    }

    // ================================================= 5. 回归守卫：坏记录隔离
    {
        std::string broken = joinFields({"SubAfter", "REQ-9", MSG_ID_2}) + STX;  // 段数不足
        std::string goodSub = joinFields({"SubBefore", "1700000000000", "R", "G", "REQ-1",
                                          MSG_ID_1, "0", "K"}) + STX;
        auto records =
            TraceDataEncoder::decoderFromTraceDataString(expectedPub() + broken + goodSub);
        expectInt(static_cast<long long>(records.size()), 2,
                  "bad record isolated: sibling records survive");
        if (records.size() == 2) {
            expect(records[0].traceType == TraceType::PUB, "bad record isolated: pub kept first");
            expect(records[1].traceType == TraceType::SUB_BEFORE,
                   "bad record isolated: sub_before kept after");
        }
        // 未知类型只跳过自己
        std::string unknown = joinFields({"SomethingNew", "1", "2"}) + STX;
        auto r2 = TraceDataEncoder::decoderFromTraceDataString(unknown + expectedRecall());
        expectInt(static_cast<long long>(r2.size()), 1, "unknown kind: only itself skipped");
        expect(r2[0].traceType == TraceType::RECALL, "unknown kind: recall survives");
    }

    // ================================================= 6. 分发器：分组 / keys / 跳过规则
    {
        CapturingDispatcher d("GID_trace_test", TraceDispatcherType::PRODUCE, 20, "");
        expect(d.getTraceTopicName() == "RMQ_SYS_TRACE_TOPIC", "dispatcher: default trace topic");
        expectInt(d.batchNum(), 20, "dispatcher: batchNum honoured");
        expect(d.isStarted() == false, "dispatcher: not started (no network)");

        auto c1 = pubContext("TopicTest", MSG_ID_1, "KeyA");
        auto c2 = pubContext("TopicTest", MSG_ID_2, "KeyB");
        std::string rec1 = TraceDataEncoder::encoderFromContextBean(c1.get())->transData;
        std::string rec2 = TraceDataEncoder::encoderFromContextBean(c2.get())->transData;
        d.append(c1);
        d.append(c2);
        d.flush();
        expectInt(static_cast<long long>(d.sent.size()), 1,
                  "dispatcher: same business topic -> 1 trace message");
        if (!d.sent.empty()) {
            expect(d.sent[0].traceTopic == "RMQ_SYS_TRACE_TOPIC",
                   "dispatcher: sends to trace topic");
            expectEq(d.sent[0].body, rec1 + rec2, "dispatcher: body concatenates records");
            expectInt(countOf(d.sent[0].body, STX), 2, "dispatcher: 2 records in body");
            // keys 里含两个 msgId 与业务 key（控制台按 keys 反查轨迹）
            const std::string& k = d.sent[0].keys;
            expect(k.find(MSG_ID_1) != std::string::npos &&
                       k.find(MSG_ID_2) != std::string::npos &&
                       k.find("KeyA") != std::string::npos &&
                       k.find("KeyB") != std::string::npos,
                   "dispatcher: keys aggregated (msgId + business keys)");
        }
        expectInt(d.discardCount(), 0, "dispatcher: nothing discarded");
    }
    {
        // 不同业务 topic -> 各自一条轨迹消息
        CapturingDispatcher d("G", TraceDispatcherType::PRODUCE, 20, "");
        d.append(pubContext("TopicA", MSG_ID_1, "K"));
        d.append(pubContext("TopicB", MSG_ID_2, "K"));
        d.flush();
        expectInt(static_cast<long long>(d.sent.size()), 2, "dispatcher: different topics -> 2 msgs");
    }
    {
        // 空 region / 空 bean 的上下文直接跳过（Java sendTraceData 的 continue）
        CapturingDispatcher d("G", TraceDispatcherType::PRODUCE, 20, "");
        auto noRegion = pubContext("T", MSG_ID_1, "K");
        noRegion->regionId.clear();
        d.append(noRegion);
        auto noBean = std::make_shared<TraceContext>();
        noBean->traceType = TraceType::PUB;
        noBean->regionId = "DefaultRegion";
        d.append(noBean);
        d.flush();
        expectInt(static_cast<long long>(d.sent.size()), 0, "dispatcher: empty region/beans skipped");
    }
    {
        // 自定义 traceTopic（对应 Java setTraceTopic）
        CapturingDispatcher d("G", TraceDispatcherType::PRODUCE, 10, "MY_TRACE_TOPIC");
        expect(d.getTraceTopicName() == "MY_TRACE_TOPIC", "dispatcher: custom trace topic name");
        d.append(pubContext("T", MSG_ID_1, "K"));
        d.flush();
        expectInt(static_cast<long long>(d.sent.size()), 1, "dispatcher: custom topic -> 1 msg");
        if (!d.sent.empty()) {
            expect(d.sent[0].traceTopic == "MY_TRACE_TOPIC", "dispatcher: custom topic used");
        }
    }
    {
        // CLOUD 通道 -> 轨迹 topic 变成 rmq_sys_TRACE_DATA_<region>（Java TRACE_TOPIC_PREFIX）
        CapturingDispatcher d("G", TraceDispatcherType::PRODUCE, 10, "");
        auto ctx = pubContext("T", MSG_ID_1, "K");
        ctx->accessChannel = AccessChannel::CLOUD;
        ctx->regionId = "RegionA";
        d.append(ctx);
        d.flush();
        expectInt(static_cast<long long>(d.sent.size()), 1, "dispatcher CLOUD: 1 msg");
        if (!d.sent.empty()) {
            expect(d.sent[0].traceTopic == "rmq_sys_TRACE_DATA_RegionA",
                   "dispatcher CLOUD: trace topic = prefix + region");
        }
    }

    // ================================================= 7. 模型默认值
    {
        TraceBean b;
        expectInt(b.msgType, 0, "bean: default msgType = Normal_Msg");
        expectInt(b.bodyLength, 0, "bean: default bodyLength");
        expectInt(b.retryTimes, 0, "bean: default retryTimes");
        expect(!b.storeHost.empty(), "bean: storeHost defaults to local address");
        expect(!b.clientHost.empty(), "bean: clientHost defaults to local address");
        TraceTransferBean tb;
        expect(tb.transData.empty() && tb.transKey.empty(), "transfer bean: empty defaults");
        expect(TraceConstants::CONTENT_SPLITOR[0] == '\x01', "constants: CONTENT_SPLITOR = SOH");
        expect(TraceConstants::FIELD_SPLITOR[0] == '\x02', "constants: FIELD_SPLITOR = STX");
        expect(std::string(TraceConstants::GROUP_NAME_PREFIX) == "_INNER_TRACE_PRODUCER",
               "constants: GROUP_NAME_PREFIX");
        expect(std::string(TraceConstants::TRACE_TOPIC_PREFIX) == "rmq_sys_TRACE_DATA_",
               "constants: TRACE_TOPIC_PREFIX");
        expect(std::string(TraceConstants::DEFAULT_TRACE_REGION_ID) == "DefaultRegion",
               "constants: DEFAULT_TRACE_REGION_ID");
        expect(std::string(traceTypeName(TraceType::SUB_BEFORE)) == "SubBefore",
               "traceTypeName: SubBefore");
        expect(traceTypeFromName("EndTransaction") == TraceType::END_TRANSACTION,
               "traceTypeFromName: EndTransaction");
    }

    std::printf("trace: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
