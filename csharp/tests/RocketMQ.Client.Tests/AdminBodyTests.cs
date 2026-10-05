// 管理端 body 单测（镜像 C++ test_admin.cpp 的核心部分）：
// fastjson2 非法 JSON 容错（MessageQueue 内联对象键、NaN/Infinity、裸数字键）、
// properties 文本往返、DTO 默认值、offsetTable 解析。
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class AdminBodyTests
{
    // ---------------------------------------------- MessageQueue 内联对象键

    [Fact]
    public void MessageQueueKey_Fastjson2FieldOrder()
    {
        // 键按字母序：brokerName, queueId, topic
        var mq = new MessageQueue("MyTopic", "broker-a", 3);
        Assert.Equal("{\"brokerName\":\"broker-a\",\"queueId\":3,\"topic\":\"MyTopic\"}",
            MessageQueueKeys.MessageQueueKey(mq));
    }

    [Fact]
    public void TopicStatsTable_DecodeIllegalJson_InlineObjectKey()
    {
        // 真实 broker（fastjson2）输出：键是 JSON 对象字面量 → 整体是"非法 JSON"，
        // 解析器必须容忍"对象作为键"（保留原始文本作键名）。
        const string json = "{\"offsetTable\":{{\"brokerName\":\"broker-a\",\"queueId\":3,\"topic\":\"MyTopic\"}:" +
                            "{\"maxOffset\":88,\"minOffset\":0,\"lastUpdateTimestamp\":1700000000000}}," +
                            "\"topicPutTps\":0.0}";

        Assert.True(TopicStatsTable.Decode(Encoding.UTF8.GetBytes(json), out TopicStatsTable t));
        Assert.Single(t.OffsetTable);
        var mq = new MessageQueue("MyTopic", "broker-a", 3);
        Assert.True(t.OffsetTable.ContainsKey(mq));
        Assert.Equal(88, t.OffsetTable[mq].MaxOffset);
        Assert.Equal(88, t.TotalMaxOffset());
        Assert.Equal(0.0, t.TopicPutTps, 6);
    }

    [Fact]
    public void ConsumeStats_DecodeAndLag()
    {
        const string json = "{\"consumeTps\":1.5,\"offsetTable\":{" +
                            "{\"brokerName\":\"b\",\"queueId\":0,\"topic\":\"T\"}:" +
                            "{\"brokerOffset\":100,\"consumerOffset\":40,\"lastTimestamp\":0,\"pullOffset\":0}}}";

        Assert.True(ConsumeStats.Decode(Encoding.UTF8.GetBytes(json), out ConsumeStats c));
        Assert.Equal(60, c.TotalLag());
        Assert.Equal(1.5, c.ConsumeTps, 6);
    }

    [Fact]
    public void ResetOffsetBody_DecodeOffsetTable()
    {
        // ⚠ Java 字段是 Map<MessageQueue, Long>，不是嵌套 map（早期 Python 实现写错）
        const string json = "{\"offsetTable\":{{\"brokerName\":\"b\",\"queueId\":2,\"topic\":\"T\"}:123456}}";

        Assert.True(ResetOffsetBody.Decode(Encoding.UTF8.GetBytes(json), out ResetOffsetBody b));
        Assert.Single(b.OffsetTable);
        Assert.Equal(123456, b.OffsetTable[new MessageQueue("T", "b", 2)]);
    }

    [Fact]
    public void ConsumeStatsList_DecodeUsesJavaFieldName()
    {
        // ⚠ JSON 键是 Java 字段名 consumeStatsList，不是 statsList：写错键名时真机
        // 341 响应会解析成空集合，看着像「这个 broker 没有积压」
        const string json =
            "{\"consumeStatsList\":[{\"G_BROKER\":[{\"offsetTable\":{}}]}],"
            + "\"brokerAddr\":\"127.0.0.1:10911\",\"totalDiff\":7,\"totalInflightDiff\":2}";

        Assert.True(ConsumeStatsList.Decode(Encoding.UTF8.GetBytes(json), out ConsumeStatsList sl));
        Assert.Equal(1, sl.StatsList.Size());
        Assert.True(sl.HasBrokerAddr);
        Assert.Equal("127.0.0.1:10911", sl.BrokerAddr);
        Assert.Equal(7, sl.TotalDiff);
        Assert.Equal(2, sl.TotalInflightDiff);

        // 旧键名的 JSON 必须解析不出行，否则会掩盖回归
        Assert.True(ConsumeStatsList.Decode(
            Encoding.UTF8.GetBytes("{\"statsList\":[{\"G\":[{}]}]}"), out ConsumeStatsList stale));
        Assert.Equal(0, stale.StatsList.Size());
    }

    [Fact]
    public void MessageQueueKey_PlainStringKey_ReturnsFalse()
    {
        // 普通字符串键（"0"、"G1"）不是内联对象，解析必须返回 false
        Assert.False(MessageQueueKeys.ParseMessageQueueKey("0", out _));
        Assert.False(MessageQueueKeys.ParseMessageQueueKey("G1", out _));
        Assert.True(MessageQueueKeys.ParseMessageQueueKey(
            "{\"brokerName\":\"b\",\"queueId\":1,\"topic\":\"t\"}", out MessageQueue mq));
        Assert.Equal(1, mq.QueueId);
    }

    // ------------------------------------------------------------- NaN/Infinity

    [Fact]
    public void Json_ToleratesNaNAndInfinity()
    {
        // fastjson2 会输出非法 JSON 的 NaN/-Infinity（Double 值）
        Assert.True(Json.TryParse("{\"a\":NaN,\"b\":-Infinity,\"c\":Infinity}", out JsonValue v, out _));
        Assert.True(double.IsNaN(v.Get("a").DoubleValue()));
        Assert.True(double.IsNegativeInfinity(v.Get("b").DoubleValue()));
        Assert.True(double.IsPositiveInfinity(v.Get("c").DoubleValue()));
    }

    [Fact]
    public void Json_ToleratesTrailingComma()
    {
        // fastjson2 某些路径会留尾逗号
        Assert.True(Json.TryParse("{\"a\":1,}", out JsonValue v, out _));
        Assert.Equal(1, v.Get("a").IntValue());
    }

    // ------------------------------------------------------------- properties

    [Fact]
    public void MessageProperties_RoundTrip()
    {
        var props = new PropertyMap
        {
            ["TAGS"] = "TagA",
            ["KEYS"] = "K1 K2",
            ["city"] = "Hangzhou",
        };
        string s = MessageDecoder.MessagePropertiesToString(props);
        PropertyMap back = MessageDecoder.StringToMessageProperties(s);
        Assert.Equal(props, back);
    }

    // ------------------------------------------------------------- 订阅组

    [Fact]
    public void SubscriptionGroupConfig_Defaults_MatchJavaProbe()
    {
        var c = new SubscriptionGroupConfig("MyGroup");
        string json = c.ToJson().Dump();
        // Java 5.x 探针实测的字段集与默认值
        Assert.Contains("\"attributes\":{}", json);
        Assert.Contains("\"brokerId\":0", json);
        Assert.Contains("\"consumeBroadcastEnable\":true", json);
        Assert.Contains("\"consumeEnable\":true", json);
        Assert.Contains("\"consumeFromMinEnable\":true", json);
        Assert.Contains("\"consumeMessageOrderly\":false", json);
        Assert.Contains("\"consumeTimeoutMinute\":15", json);
        Assert.Contains("\"groupName\":\"MyGroup\"", json);
        Assert.Contains("\"groupRetryPolicy\":{\"type\":\"CUSTOMIZED\"}", json);
        Assert.Contains("\"retryMaxTimes\":16", json);
        Assert.Contains("\"retryQueueNums\":1", json);
        Assert.Contains("\"whichBrokerWhenConsumeSlowly\":1", json);
        Assert.DoesNotContain("subscriptionDataSet", json); // fastjson2 跳过 null 字段
    }

    // ------------------------------------------------------------- TopicConfig

    [Fact]
    public void TopicConfig_EncodeDecode()
    {
        var tc = new TopicConfig("T1");
        byte[] data = tc.Encode();
        Assert.True(TopicConfig.Decode(data, out TopicConfig back));
        Assert.Equal("T1", back.TopicName);
        Assert.Equal(16, back.ReadQueueNums);
        Assert.Equal(16, back.WriteQueueNums);
        Assert.Equal(6, back.Perm);
    }

    // ------------------------------------------------------------- ClusterInfo

    [Fact]
    public void ClusterInfo_DecodeRealShape()
    {
        // 真实 RocketMQ：brokerAddrTable[name] 是 BrokerData 对象
        const string json = "{\"brokerAddrTable\":{\"broker-a\":{\"brokerAddrs\":{\"0\":\"127.0.0.1:10911\"}," +
                            "\"brokerName\":\"broker-a\",\"cluster\":\"DefaultCluster\",\"enableActingMaster\":false,\"zoneName\":\"\"}}," +
                            "\"clusterAddrTable\":{\"DefaultCluster\":[\"broker-a\"]}}";

        Assert.True(ClusterInfo.Decode(Encoding.UTF8.GetBytes(json), out ClusterInfo ci));
        Assert.Equal(new List<string> { "127.0.0.1:10911" }, ci.GetBrokerAddrs());
        Assert.Equal(new List<string> { "127.0.0.1:10911" }, ci.GetBrokerAddrsOfCluster("DefaultCluster"));
        Assert.Empty(ci.GetBrokerAddrsOfCluster("NoSuchCluster"));
    }
}
