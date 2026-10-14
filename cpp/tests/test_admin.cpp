// Admin unit tests: tolerance of fastjson2 non-standard JSON, admin response-body DTOs,
// TopicConfig / SubscriptionGroupConfig field names and defaults, MixAll properties
// semantics, permission and group classification, and the key CreateTopic / QueryMessage header fields.
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/remoting/protocol/admin_body.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/serialize.h"
#include "rocketmq/remoting/protocol/subscription.h"

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

#define CHECK_EQ(actual, expect, msg)                                          \
    do {                                                                       \
        auto a_ = (actual);                                                    \
        auto e_ = (expect);                                                    \
        if (a_ == e_) {                                                        \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << " expect=" << e_                \
                      << " actual=" << a_ << "\n";                             \
        }                                                                      \
    } while (0)

namespace {

JsonValue parseMust(const std::string& s, const char* what) {
    JsonValue v;
    std::string err;
    if (!jsonParse(s, v, &err)) {
        ++g_fail;
        std::cout << "[FAIL] jsonParse failed for " << what << ": " << err << "\n";
    }
    return v;
}

// ---------------------------------------------------------------- 1. Non-standard JSON tolerance

void testJsonToleratesFastjson2Extensions() {
    // 1.1 the map key is an inlined object (fastjson2 rendering of a non-string key): invalid JSON
    const char* mqKeyed =
        "{\"offsetTable\":{{" 
        "\"brokerName\":\"broker-a\",\"queueId\":3,\"topic\":\"t\"}"
        ":{\"maxOffset\":2,\"minOffset\":0}}}";
    JsonValue v = parseMust(mqKeyed, "object-as-map-key");
    const JsonValue* table = v.find("offsetTable");
    CHECK(table != nullptr && table->isObject(), "object-as-key: offsetTable is an object");
    if (table != nullptr) {
        CHECK_EQ(table->objectItems().size(), static_cast<size_t>(1),
                 "object-as-key: exactly one key");
        const std::string& rawKey = table->objectItems()[0].first;
        CHECK(rawKey.find("\"brokerName\"") != std::string::npos,
              "object-as-key: the key keeps the raw JSON text");
    }

    // 1.2 unquoted numeric key
    JsonValue numeric = parseMust("{\"brokerAddrs\":{0:\"127.0.0.1:10911\"}}", "numeric-key");
    const JsonValue* addrs = numeric.find("brokerAddrs");
    CHECK(addrs != nullptr && addrs->contains("0"), "numeric-key: 0 is treated as the string key \"0\"");

    // 1.3 NaN / Infinity / -Infinity (fastjson2 writes these three non-standard literals)
    JsonValue nonFinite = parseMust("{\"a\":NaN,\"b\":Infinity,\"c\":-Infinity}", "non-finite");
    CHECK(nonFinite.get("a").isNumber(), "NaN parsed as Number");
    CHECK(nonFinite.get("b").isNumber(), "Infinity parsed as Number");
    CHECK(nonFinite.get("c").isNumber(), "-Infinity parsed as Number");
    // on dump, non-finite values fall back to null (JSON has no NaN/Inf)
    CHECK(nonFinite.dump().find("null") != std::string::npos, "NaN/Inf dump falls back to null");

    // 1.4 trailing comma
    JsonValue trailing = parseMust("{\"a\":1,\"b\":[1,2,],}", "trailing-comma");
    CHECK_EQ(trailing.get("a").intValue(), static_cast<int64_t>(1), "trailing-comma: a");
    CHECK_EQ(trailing.get("b").size(), static_cast<size_t>(2), "trailing-comma: b length 2");

    // 1.5 outright garbage must be rejected (tolerance must not become silent success)
    JsonValue bad;
    std::string err;
    CHECK(!jsonParse("{\"a\":}", bad, &err), "garbage JSON must fail to parse");
    CHECK(!jsonParse("", bad, &err), "empty string must fail to parse");
}

// ---------------------------------------------------------------- 2. MessageQueue key

void testMessageQueueKeyRoundTrip() {
    MessageQueue mq("MyTopic", "broker-a", 3);
    std::string key = messageQueueKey(mq);
    // fastjson2 orders the map-key fields alphabetically: brokerName, queueId, topic
    CHECK_EQ(key, std::string("{\"brokerName\":\"broker-a\",\"queueId\":3,\"topic\":\"MyTopic\"}"),
             "messageQueueKey output matches fastjson2");

    MessageQueue back;
    CHECK(parseMessageQueueKey(key, back), "parseMessageQueueKey succeeds");
    CHECK_EQ(back.topic, std::string("MyTopic"), "mq key round-trip topic");
    CHECK_EQ(back.brokerName, std::string("broker-a"), "mq key round-trip brokerName");
    CHECK_EQ(back.queueId, 3, "mq key round-trip queueId");

    // a plain string key must not be misread as a MessageQueue
    MessageQueue dummy;
    CHECK(!parseMessageQueueKey("0", dummy), "numeric key is not treated as a MessageQueue");
    CHECK(!parseMessageQueueKey("G1", dummy), "string key is not treated as a MessageQueue");
}

// ---------------------------------------------------------------- 3. TopicStatsTable / ConsumeStats

void testTopicStatsTableFromRealFixture() {
    const char* body =
        "{\"offsetTable\":{"
        "{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"t\"}"
        ":{\"lastUpdateTimestamp\":1789374000000,\"maxOffset\":8,\"minOffset\":0},"
        "{\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"t\"}"
        ":{\"lastUpdateTimestamp\":1789374000001,\"maxOffset\":2,\"minOffset\":0}"
        "},\"topicPutTps\":1.5}";

    TopicStatsTable t;
    CHECK(TopicStatsTable::decode(body, t), "TopicStatsTable::decode succeeds");
    CHECK_EQ(t.offsetTable.size(), static_cast<size_t>(2), "TopicStatsTable has 2 queues");
    CHECK_EQ(t.totalMaxOffset(), static_cast<int64_t>(10), "totalMaxOffset = 8+2");
    CHECK(t.offsetTable.count(MessageQueue("t", "broker-a", 1)) == 1,
          "queueId=1 is found by MessageQueue key");
    CHECK_EQ(t.offsetTable[MessageQueue("t", "broker-a", 1)].maxOffset,
             static_cast<int64_t>(2), "maxOffset of queueId=1");

    // round-trip
    Bytes encoded = t.encode();
    TopicStatsTable again;
    CHECK(TopicStatsTable::decode(encoded, again), "TopicStatsTable round-trip decode");
    CHECK_EQ(again.totalMaxOffset(), static_cast<int64_t>(10), "totalMaxOffset unchanged after round-trip");
    CHECK_EQ(again.offsetTable.size(), static_cast<size_t>(2), "queue count unchanged after round-trip");
}

void testConsumeStatsFromRealFixture() {
    const char* body =
        "{\"consumeTps\":0.0,\"offsetTable\":{"
        "{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"t\"}"
        ":{\"brokerOffset\":8,\"consumerOffset\":5,\"lastTimestamp\":1,\"pullOffset\":7}"
        "}}";
    ConsumeStats cs;
    CHECK(ConsumeStats::decode(body, cs), "ConsumeStats::decode succeeds");
    CHECK_EQ(cs.offsetTable.size(), static_cast<size_t>(1), "ConsumeStats has 1 queue");
    OffsetWrapper ow = cs.offsetTable[MessageQueue("t", "broker-a", 0)];
    CHECK_EQ(ow.lag(), static_cast<int64_t>(3), "OffsetWrapper.lag = 8-5");
    CHECK_EQ(cs.totalLag(), static_cast<int64_t>(3), "ConsumeStats.totalLag");
    CHECK_EQ(ow.pullOffset, static_cast<int64_t>(7), "pullOffset is parsed");
}

void testResetOffsetBodyUsesMessageQueueKeys() {
    // ResetOffsetBody.offsetTable is a Map<MessageQueue, Long>, not a nested map
    const char* body =
        "{\"offsetTable\":{{" 
        "\"brokerName\":\"broker-a\",\"queueId\":3,\"topic\":\"t\"}:2}}";
    ResetOffsetBody b;
    CHECK(ResetOffsetBody::decode(body, b), "ResetOffsetBody::decode succeeds");
    CHECK_EQ(b.offsetTable.size(), static_cast<size_t>(1), "ResetOffsetBody has 1 entry");
    auto it = b.offsetTable.find(MessageQueue("t", "broker-a", 3));
    CHECK(it != b.offsetTable.end(), "ResetOffsetBody is found by MessageQueue key");
    if (it != b.offsetTable.end()) {
        CHECK_EQ(it->second, static_cast<int64_t>(2), "ResetOffsetBody offset value = 2");
    }
}

// ---------------------------------------------------------------- 4. TopicConfig

void testTopicConfigDefaultsAndAttributes() {
    TopicConfig c("t");
    CHECK_EQ(c.readQueueNums, 16, "TopicConfig default readQueueNums=16");
    CHECK_EQ(c.writeQueueNums, 16, "TopicConfig default writeQueueNums=16");
    CHECK_EQ(c.perm, 6, "TopicConfig default perm=6");
    CHECK_EQ(c.topicFilterType, std::string("SINGLE_TAG"), "TopicConfig default SINGLE_TAG");
    CHECK_EQ(c.order, false, "TopicConfig default order=false");

    JsonValue v = c.toJson();
    CHECK(v.find("attributes") != nullptr, "TopicConfig serialization must carry attributes (even when empty)");
    CHECK(v.find("topicFilterType") != nullptr, "TopicConfig serialization must carry topicFilterType");

    const char* probe =
        "{\"attributes\":{},\"order\":false,\"perm\":4,\"readQueueNums\":4,"
        "\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"t\",\"topicSysFlag\":0,"
        "\"writeQueueNums\":4}";
    TopicConfig parsed;
    CHECK(TopicConfig::decode(probe, parsed), "TopicConfig::decode succeeds");
    CHECK_EQ(parsed.readQueueNums, 4, "TopicConfig parses readQueueNums=4");
    CHECK_EQ(parsed.perm, 4, "TopicConfig parses perm=4");

    // attributes content round-trip
    TopicConfig withAttrs("t");
    withAttrs.attributes["k1"] = "v1";
    TopicConfig back;
    CHECK(TopicConfig::decode(withAttrs.encode(), back), "TopicConfig round-trip with attributes");
    CHECK_EQ(back.attributes.size(), static_cast<size_t>(1), "attributes round-trip keeps 1 entry");
    CHECK_EQ(back.attributes["k1"], std::string("v1"), "attributes round-trip value");

    // permToString three-flag form
    CHECK_EQ(PermName::permToString(6), std::string("RW-"), "perm=6 -> RW-");
    CHECK_EQ(PermName::permToString(4), std::string("R--"), "perm=4 -> R--");
    CHECK_EQ(PermName::permToString(2), std::string("-W-"), "perm=2 -> -W-");
    CHECK_EQ(PermName::permToString(7), std::string("RWX"), "perm=7 -> RWX");
}

// ---------------------------------------------------------------- 5. SubscriptionGroupConfig

void testSubscriptionGroupConfigProbeFixture() {
    const char* probe =
        "{\"attributes\":{},\"brokerId\":0,\"consumeBroadcastEnable\":true,"
        "\"consumeEnable\":true,\"consumeFromMinEnable\":true,\"consumeMessageOrderly\":false,"
        "\"consumeTimeoutMinute\":15,\"groupName\":\"MyGroup\","
        "\"groupRetryPolicy\":{\"type\":\"CUSTOMIZED\"},\"groupSysFlag\":0,"
        "\"notifyConsumerIdsChangedEnable\":true,\"retryMaxTimes\":16,\"retryQueueNums\":1,"
        "\"whichBrokerWhenConsumeSlowly\":1}";

    SubscriptionGroupConfig cfg;
    CHECK(SubscriptionGroupConfig::decode(probe, cfg), "SubscriptionGroupConfig::decode succeeds");
    CHECK_EQ(cfg.groupName, std::string("MyGroup"), "groupName");
    CHECK_EQ(cfg.retryMaxTimes, 16, "retryMaxTimes=16");
    CHECK_EQ(cfg.retryQueueNums, 1, "retryQueueNums=1");
    CHECK_EQ(cfg.brokerId, 0, "brokerId=0 (MASTER_ID)");
    CHECK_EQ(cfg.whichBrokerWhenConsumeSlowly, 1, "whichBrokerWhenConsumeSlowly=1");
    CHECK_EQ(cfg.consumeTimeoutMinute, 15, "consumeTimeoutMinute=15");
    CHECK_EQ(cfg.groupRetryPolicy.type, std::string("CUSTOMIZED"), "groupRetryPolicy.type");
    CHECK_EQ(cfg.hasSubscriptionDataSet, false, "subscriptionDataSet does not appear when null");

    SubscriptionGroupConfig def("g");
    CHECK_EQ(def.retryMaxTimes, 16, "default retryMaxTimes=16");
    CHECK_EQ(def.consumeEnable, true, "default consumeEnable=true");
    CHECK_EQ(def.consumeFromMinEnable, true, "default consumeFromMinEnable=true");
    CHECK_EQ(def.consumeBroadcastEnable, true, "default consumeBroadcastEnable=true");
    CHECK_EQ(def.notifyConsumerIdsChangedEnable, true, "default notifyConsumerIdsChangedEnable=true");
    CHECK_EQ(def.consumeMessageOrderly, false, "default consumeMessageOrderly=false");

    // subscriptionDataSet must not appear in the JSON when unset (fastjson2 skips null)
    JsonValue defJson = def.toJson();
    CHECK(defJson.find("subscriptionDataSet") == nullptr,
          "subscriptionDataSet is not serialized when unset");

    // round-trip
    def.retryMaxTimes = 3;
    SubscriptionGroupConfig again;
    CHECK(SubscriptionGroupConfig::decode(def.encode(), again), "SubscriptionGroupConfig round-trip");
    CHECK_EQ(again.retryMaxTimes, 3, "retryMaxTimes=3 after round-trip");
    CHECK_EQ(again.groupName, std::string("g"), "groupName after round-trip");
}

void testSubscriptionGroupWrapperAndMerge() {
    const char* probe =
        "{\"dataVersion\":{\"counter\":1,\"timestamp\":1789374000000},"
        "\"forbiddenTable\":{},"
        "\"subscriptionGroupTable\":{\"g1\":{\"brokerId\":0,\"consumeEnable\":true,"
        "\"groupName\":\"g1\",\"groupRetryPolicy\":{\"type\":\"CUSTOMIZED\"},"
        "\"retryMaxTimes\":5}}}";
    SubscriptionGroupWrapper w;
    CHECK(SubscriptionGroupWrapper::decode(probe, w), "SubscriptionGroupWrapper::decode succeeds");
    CHECK_EQ(w.subscriptionGroupTable.size(), static_cast<size_t>(1), "wrapper has 1 group");
    CHECK_EQ(w.subscriptionGroupTable["g1"].retryMaxTimes, 5, "wrapper group retryMaxTimes=5");
    CHECK(!w.dataVersion.isNull(), "dataVersion is preserved (pagination must send it back unchanged)");

    // pagination accumulation: mergeFrom overwrites same names and appends new ones
    SubscriptionGroupWrapper page2;
    page2.subscriptionGroupTable["g2"] = SubscriptionGroupConfig("g2");
    page2.subscriptionGroupTable["g1"] = SubscriptionGroupConfig("g1");
    w.mergeFrom(page2);
    CHECK_EQ(w.subscriptionGroupTable.size(), static_cast<size_t>(2), "2 groups after merge");
    CHECK_EQ(w.subscriptionGroupTable["g1"].retryMaxTimes, 16,
             "merge overwrites the same name (g1 back to default 16)");
    CHECK(w.subscriptionGroupTable.count("g2") == 1, "merge appends g2");
}

void testTopicConfigSerializeWrapper() {
    const char* probe =
        "{\"dataVersion\":{\"counter\":0,\"timestamp\":1},"
        "\"topicConfigTable\":{\"t\":{\"attributes\":{},\"order\":false,\"perm\":6,"
        "\"readQueueNums\":16,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"t\","
        "\"topicSysFlag\":0,\"writeQueueNums\":16}}}";
    TopicConfigSerializeWrapper w;
    CHECK(TopicConfigSerializeWrapper::decode(probe, w), "TopicConfigSerializeWrapper::decode");
    CHECK_EQ(w.topicConfigTable.size(), static_cast<size_t>(1), "1 topicConfig");
    CHECK_EQ(w.topicConfigTable["t"].writeQueueNums, 16, "topicConfig writeQueueNums=16");
}

void testQueryConsumeQueueResponseBody() {
    const char* probe =
        "{\"filterData\":\"*\",\"maxQueueIndex\":88,\"minQueueIndex\":1,"
        "\"subscriptionData\":{\"classFilterMode\":false,\"expressionType\":\"TAG\","
        "\"subString\":\"*\",\"topic\":\"t\",\"version\":1}}";
    QueryConsumeQueueResponseBody b;
    CHECK(QueryConsumeQueueResponseBody::decode(probe, b), "QueryConsumeQueue decode succeeds");
    CHECK_EQ(b.minQueueIndex, static_cast<int64_t>(1), "minQueueIndex=1");
    CHECK_EQ(b.maxQueueIndex, static_cast<int64_t>(88), "maxQueueIndex=88");
    CHECK_EQ(b.filterData, std::string("*"), "filterData=*");
    CHECK_EQ(b.hasQueueData, false, "queueData does not appear when null");
    CHECK(!b.subscriptionData.isNull(), "subscriptionData is passed through");

    // the shape that carries queueData
    const char* withQ =
        "{\"maxQueueIndex\":2,\"minQueueIndex\":0,\"queueData\":["
        "{\"bitMap\":null,\"eval\":false,\"physicOffset\":0,\"physicSize\":100,\"tagsCode\":0}]}";
    QueryConsumeQueueResponseBody b2;
    CHECK(QueryConsumeQueueResponseBody::decode(withQ, b2), "QueryConsumeQueue (with queueData)");
    CHECK_EQ(b2.hasQueueData, true, "queueData is parsed");
    CHECK_EQ(b2.queueData.size(), static_cast<size_t>(1), "queueData has 1 entry");
    CHECK_EQ(b2.queueData[0].physicSize, static_cast<int64_t>(100), "physicSize=100");
    CHECK_EQ(b2.queueData[0].hasBitMap, false, "bitMap is null");
}

// ---------------------------------------------------------------- 6. remaining public bodies

void testPublicBodies() {
    // KVTable
    KVTable kv;
    CHECK(KVTable::decode(std::string("{\"table\":{\"k1\":\"v1\",\"k2\":\"v2\"}}"), kv),
          "KVTable decode");
    CHECK_EQ(kv.table.size(), static_cast<size_t>(2), "KVTable has 2 entries");
    CHECK_EQ(kv.table["k1"], std::string("v1"), "KVTable k1=v1");

    // TopicList
    TopicList tl;
    CHECK(TopicList::decode(std::string("{\"topicList\":[\"a\",\"b\"]}"), tl), "TopicList decode");
    CHECK_EQ(tl.topicList.size(), static_cast<size_t>(2), "TopicList has 2 entries");
    CHECK(tl.contains("b"), "TopicList contains b");
    CHECK(!tl.contains("c"), "TopicList does not contain c");

    // ClusterInfo: the real broker shape (brokerAddrTable[name] is a BrokerData)
    const char* clusterJson =
        "{\"brokerAddrTable\":{\"broker-a\":{\"brokerAddrs\":{\"0\":\"127.0.0.1:10911\"},"
        "\"brokerName\":\"broker-a\",\"cluster\":\"DefaultCluster\",\"enableActingMaster\":false}},"
        "\"clusterAddrTable\":{\"DefaultCluster\":[\"broker-a\"]}}";
    ClusterInfo ci;
    CHECK(ClusterInfo::decode(clusterJson, ci), "ClusterInfo decode succeeds");
    CHECK_EQ(ci.brokerAddrTable.size(), static_cast<size_t>(1), "ClusterInfo has 1 broker");
    std::vector<std::string> addrs = ci.getBrokerAddrs();
    CHECK_EQ(addrs.size(), static_cast<size_t>(1), "getBrokerAddrs has 1 entry");
    if (!addrs.empty()) CHECK_EQ(addrs[0], std::string("127.0.0.1:10911"), "broker address");
    std::vector<std::string> cl = ci.getBrokerAddrsOfCluster("DefaultCluster");
    CHECK_EQ(cl.size(), static_cast<size_t>(1), "broker addresses looked up by cluster");
    CHECK_EQ(ci.getBrokerAddrsOfCluster("NoSuchCluster").size(), static_cast<size_t>(0),
             "an unknown cluster returns empty");

    // GetConsumerListByGroupResponseBody
    GetConsumerListByGroupResponseBody g;
    CHECK(GetConsumerListByGroupResponseBody::decode(
              std::string("{\"consumerIdList\":[\"c1\",\"c2\"]}"), g),
          "GetConsumerListByGroup decode");
    CHECK_EQ(g.consumerIdList.size(), static_cast<size_t>(2), "consumerIdList has 2 entries");

    ConsumeStatsList sl;
    CHECK(ConsumeStatsList::decode(
              std::string("{\"consumeStatsList\":[{\"G_BROKER\":\"x\"}],"
                          "\"brokerAddr\":\"127.0.0.1:10911\",\"totalDiff\":7,"
                          "\"totalInflightDiff\":2}"),
              sl),
          "ConsumeStatsList decode");
    CHECK_EQ(sl.statsList.size(), static_cast<size_t>(1), "consumeStatsList has 1 row");
    CHECK(sl.hasBrokerAddr && sl.brokerAddr == "127.0.0.1:10911", "ConsumeStatsList brokerAddr");
    CHECK_EQ(sl.totalDiff, static_cast<long long>(7), "ConsumeStatsList totalDiff");
    CHECK_EQ(sl.totalInflightDiff, static_cast<long long>(2), "ConsumeStatsList totalInflightDiff");
    ConsumeStatsList wrongKey;
    CHECK(ConsumeStatsList::decode(std::string("{\"statsList\":[{\"G\":\"x\"}]}"), wrongKey),
          "legacy-key JSON still parses (no error)");
    CHECK_EQ(wrongKey.statsList.size(), static_cast<size_t>(0), "legacy-key statsList is ignored");
}

// ---------------------------------------------------------------- 7. MixAll properties

void testTopicConfigPropertiesWireSemantics() {
    // properties text fixture: whitespace trimming, comment lines, continuation and empty value at once
    const char* text = "a = b\n  c : d  \nk=v\n#comment\n!c2\nempty=\ncont=first\\\n    second\n";
    PropertyMap p = MixAll::string2Properties(text);
    CHECK_EQ(p["a"], std::string("b"), "properties: key fully trimmed, value left-trimmed");
    CHECK_EQ(p["c"], std::string("d  "), "properties: value keeps trailing whitespace");
    CHECK_EQ(p["k"], std::string("v"), "properties: plain k=v");
    CHECK_EQ(p["empty"], std::string(""), "properties: empty value");
    CHECK_EQ(p["cont"], std::string("firstsecond"), "properties: \\ continuation joined");
    CHECK(p.count("#comment") == 0, "properties: # comment is skipped");
    CHECK(p.count("!c2") == 0, "properties: ! comment is skipped");

    // whitespace is also a legal separator, matching standard Properties parsing semantics
    PropertyMap p2 = MixAll::string2Properties("a b\nonlykey\nk=v\n");
    CHECK_EQ(p2["a"], std::string("b"), "properties: whitespace as separator");
    CHECK_EQ(p2["onlykey"], std::string(""), "properties: key only");
    PropertyMap p3 = MixAll::string2Properties("a b=c=d\n");
    CHECK_EQ(p3["a"], std::string("b=c=d"), "properties: with a whitespace separator the = inside the value is kept");
    PropertyMap p4 = MixAll::string2Properties("messageDelayLevel=1s 5s 10s\n");
    CHECK_EQ(p4["messageDelayLevel"], std::string("1s 5s 10s"),
             "properties: spaces inside the value are kept (the separator is '=')");

    // properties2String: every entry becomes one "k=v" line
    PropertyMap m;
    m["brokerName"] = "broker-a";
    m["listenPort"] = "10911";
    std::string out = MixAll::properties2String(m);
    CHECK_EQ(out, std::string("brokerName=broker-a\nlistenPort=10911\n"),
             "properties2String output format");

    // round-trip: parse(serialize(x)) == x
    PropertyMap back = MixAll::string2Properties(MixAll::properties2String(m));
    CHECK_EQ(back.size(), m.size(), "properties round-trip entry count");

    // a real broker config fragment (values contain spaces, comments included)
    const char* realish =
        "# broker config\n"
        "brokerClusterName=DefaultCluster\n"
        "messageDelayLevel=1s 5s 10s 30s 1m 2m 3m\n"
        "autoCreateTopicEnable=true\n";
    PropertyMap rp = MixAll::string2Properties(realish);
    CHECK_EQ(rp.size(), static_cast<size_t>(3), "the realistic fragment parses to 3 keys");
    CHECK_EQ(rp["brokerClusterName"], std::string("DefaultCluster"), "brokerClusterName");
    CHECK_EQ(rp["messageDelayLevel"], std::string("1s 5s 10s 30s 1m 2m 3m"), "messageDelayLevel");
}

// ---------------------------------------------------------------- 8. permission / group classification

void testPermAndGroupHelpers() {
    CHECK(PermName::isValid(0), "isValid(0)");
    CHECK(PermName::isValid(7), "isValid(7)");
    CHECK(!PermName::isValid(8), "isValid(8) invalid");
    CHECK(!PermName::isValid(-1), "isValid(-1) invalid");

    // LMQ / system consumer group / predefined group
    CHECK(MixAll::isLmq("%LMQ%topic"), "isLmq hit");
    CHECK(!MixAll::isLmq("normal"), "isLmq no hit");
    CHECK(MixAll::isSysConsumerGroup("CID_RMQ_SYS_FOO"), "isSysConsumerGroup hit");
    CHECK(!MixAll::isSysConsumerGroup("NormalGroup"), "isSysConsumerGroup no hit");
    CHECK(MixAll::isPredefinedGroup(MixAll::TOOLS_CONSUMER_GROUP), "predefined group hit");
    CHECK(MixAll::isPredefinedGroup(MixAll::DEFAULT_CONSUMER_GROUP), "the default consumer group is predefined");
    CHECK(!MixAll::isPredefinedGroup("MyBizGroup"), "a business group is not predefined");
    CHECK(MixAll::isSysTopic("rmq_sys_TRACE_DATA"), "isSysTopic hit");
    CHECK(!MixAll::isSysTopic("MyTopic"), "isSysTopic no hit");

    // UNIQUE_MSG_QUERY_FLAG is the extFields KEY name, not a flag value
    CHECK_EQ(std::string(MixAll::UNIQUE_MSG_QUERY_FLAG), std::string("_UNIQUE_KEY_QUERY"),
             "UNIQUE_MSG_QUERY_FLAG is a key name");
    CHECK_EQ(QueryMsgType::ALL_MESSAGE, 0, "QueryMsgType::ALL_MESSAGE=0");
    CHECK_EQ(QueryMsgType::UNIQUE_KEY, 1, "QueryMsgType::UNIQUE_KEY=1");
    CHECK_EQ(QueryMsgType::NORMAL, 2, "QueryMsgType::NORMAL=2");
    CHECK_EQ(std::string(MessageConst::INDEX_KEY_TYPE), std::string("K"), "INDEX_KEY_TYPE=K");
    CHECK_EQ(std::string(MessageConst::INDEX_UNIQUE_TYPE), std::string("U"), "INDEX_UNIQUE_TYPE=U");
}

// ---------------------------------------------------------------- 9. request header key fields

void testCreateTopicRequestHeaderSendsTopicFilterType() {
    // the broker's CreateTopicRequestHeader.checkFields() rejects an empty topicFilterType:
    // "topicFilterType = [null] value invalid" -- that is how a real broker answers when it is left out.
    CreateTopicRequestHeader h;
    h.topic = "t";
    h.defaultTopic = "TBW102";
    h.readQueueNums = 4;
    h.writeQueueNums = 4;
    h.perm = 6;
    h.topicFilterType = TopicFilterType::SINGLE_TAG;
    h.topicSysFlag = 0;
    h.order = false;
    h.attributes = "";
    h.force = false;
    PropertyMap ext = h.toExtFields();
    CHECK_EQ(ext["topicFilterType"], std::string("SINGLE_TAG"), "createTopic must send topicFilterType");
    CHECK_EQ(ext["attributes"], std::string(""), "attributes must be an empty string, not null");
    CHECK_EQ(ext["force"], std::string("false"), "force must be lowercase false (lowercase boolean literal convention)");
    CHECK(ext.count("topic") == 1 && ext.count("perm") == 1, "topic/perm are sent together");

    CreateTopicRequestHeader minimal;
    minimal.topic = "t";
    PropertyMap ext2 = minimal.toExtFields();
    CHECK(ext2.count("topicFilterType") == 0, "topicFilterType is not written when unset");
}

void testQueryMessageRequestHeaderIndexType() {
    QueryMessageRequestHeader h;
    h.topic = "t";
    h.key = "k";
    h.maxNum = 32;
    h.beginTimestamp = 1;
    h.endTimestamp = 2;
    h.indexType = MessageConst::INDEX_UNIQUE_TYPE;
    PropertyMap ext = h.toExtFields();
    CHECK_EQ(ext["indexType"], std::string("U"), "indexType is sent");
    CHECK(ext.count("lastKey") == 0, "lastKey is not written when unset");

    // decode back
    QueryMessageRequestHeader back;
    back.fromExtFields(ext);
    CHECK_EQ(back.indexType.value_or(""), std::string("U"), "indexType read back");
    CHECK_EQ(back.key.value_or(""), std::string("k"), "key read back");
}

void testSearchOffsetRequestHeaderBoundaryType() {
    SearchOffsetRequestHeader h;
    h.topic = "t";
    h.queueId = 2;
    h.timestamp = 1700000000000LL;
    PropertyMap ext = h.toExtFields();
    CHECK(ext.count("boundaryType") == 0, "the key is not written when the boundary type is unset (@CFNullable)");

    h.boundaryType = BoundaryType::LOWER;
    CHECK_EQ(h.toExtFields()["boundaryType"], std::string("LOWER"), "LOWER goes on the wire as the uppercase enum name");
    h.boundaryType = BoundaryType::UPPER;
    PropertyMap up = h.toExtFields();
    CHECK_EQ(up["boundaryType"], std::string("UPPER"), "UPPER goes on the wire as the uppercase enum name");

    SearchOffsetRequestHeader back;
    back.fromExtFields(up);
    CHECK(back.boundaryType.value_or(BoundaryType::LOWER) == BoundaryType::UPPER,
          "boundaryType read back as UPPER");
    CHECK_EQ(boundaryTypeLowercaseName(BoundaryType::UPPER), std::string("upper"),
             "getName() lowercase name");
    // lenient parsing: only upper (case-insensitive) maps to UPPER
    for (const char* text : {"UPPER", "upper", "Upper"}) {
        CHECK(boundaryTypeFromString(text) == BoundaryType::UPPER, std::string(text) + " => UPPER");
    }
    for (const char* text : {"LOWER", "lower", "", "junk"}) {
        CHECK(boundaryTypeFromString(text) == BoundaryType::LOWER, std::string(text) + " => LOWER");
    }
    SearchOffsetRequestHeader empty;
    empty.fromExtFields(PropertyMap{});
    CHECK(!empty.boundaryType.has_value(), "a missing key yields nullopt");
}

void testBrokerVipChannel() {
    CHECK(MixAll::brokerVipChannel(false, "127.0.0.1:10911") == "127.0.0.1:10911",
          "isChange=false returns the input unchanged");
    CHECK(MixAll::brokerVipChannel(true, "127.0.0.1:10911") == "127.0.0.1:10909",
          "10911 => 10909");
    CHECK(MixAll::brokerVipChannel(true, "127.0.0.1:abc") == "127.0.0.1:abc",
          "an unparsable port returns the input unchanged instead of throwing a parse error");
    CHECK(MixAll::brokerVipChannel(true, "127.0.0.1") == "127.0.0.1", "without a colon the input is returned unchanged");
    CHECK(MixAll::brokerVipChannel(true, "10.0.0.1:8899") == "10.0.0.1:8897", "8899 => 8897");
}

void testTrackTypeNamesMatchProtocol() {
    CHECK(std::string(trackTypeName(TrackType::CONSUMED)) == "CONSUMED", "CONSUMED");
    CHECK(std::string(trackTypeName(TrackType::CONSUMED_BUT_FILTERED)) == "CONSUMED_BUT_FILTERED",
          "CONSUMED_BUT_FILTERED");
    CHECK(std::string(trackTypeName(TrackType::PULL)) == "PULL", "PULL");
    CHECK(std::string(trackTypeName(TrackType::NOT_CONSUME_YET)) == "NOT_CONSUME_YET",
          "NOT_CONSUME_YET");
    CHECK(std::string(trackTypeName(TrackType::NOT_ONLINE)) == "NOT_ONLINE", "NOT_ONLINE");
    CHECK(std::string(trackTypeName(TrackType::CONSUME_BROADCASTING)) == "CONSUME_BROADCASTING",
          "CONSUME_BROADCASTING");
    CHECK(std::string(trackTypeName(TrackType::UNKNOWN)) == "UNKNOWN", "UNKNOWN");
    MessageTrack mt;
    CHECK(mt.trackType == TrackType::UNKNOWN && mt.exceptionDesc.empty(),
          "MessageTrack defaults to UNKNOWN with no exceptionDesc");
}

void testAdminVipChannelKnobDefaultsOff() {
    DefaultMQAdminExt admin;
    CHECK(!admin.isVipChannelEnabled(), "default false");
    admin.setVipChannelEnabled(true);
    CHECK(admin.isVipChannelEnabled(), "setVipChannelEnabled(true) takes effect");
    admin.setVipChannelEnabled(false);
    CHECK(!admin.isVipChannelEnabled(), "switching it back off takes effect");
}

}  // namespace

int main() {
    testJsonToleratesFastjson2Extensions();
    testMessageQueueKeyRoundTrip();
    testTopicStatsTableFromRealFixture();
    testConsumeStatsFromRealFixture();
    testResetOffsetBodyUsesMessageQueueKeys();
    testTopicConfigDefaultsAndAttributes();
    testSubscriptionGroupConfigProbeFixture();
    testSubscriptionGroupWrapperAndMerge();
    testTopicConfigSerializeWrapper();
    testQueryConsumeQueueResponseBody();
    testPublicBodies();
    testTopicConfigPropertiesWireSemantics();
    testPermAndGroupHelpers();
    testCreateTopicRequestHeaderSendsTopicFilterType();
    testQueryMessageRequestHeaderIndexType();
    testSearchOffsetRequestHeaderBoundaryType();
    testBrokerVipChannel();
    testTrackTypeNamesMatchProtocol();
    testAdminVipChannelKnobDefaultsOff();

    std::cout << "admin: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
