// 路由 / 心跳结构体单测（镜像 C++ test_route_heartbeat.cpp 的核心断言）。
//
// 重点：JSON 字段名与 fastjson2 对齐（键按字母序）、TBW102 回退裁剪、
// MessageQueue 组装只挑可写队列、ConsumerData 无 4.x 字段。
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class RouteHeartbeatTests
{
    [Fact]
    public void QueueData_JsonFieldNames()
    {
        var qd = new QueueData("broker-a", 8, 8, 6, 0);
        string json = qd.ToJson().Dump();
        Assert.Equal(
            "{\"brokerName\":\"broker-a\",\"perm\":6,\"readQueueNums\":8,\"topicSysFlag\":0,\"writeQueueNums\":8}",
            json);
    }

    [Fact]
    public void BrokerData_JsonFieldNames_AndNumericKeyRoundTrip()
    {
        var bd = new BrokerData("DefaultCluster", "broker-a",
            new SortedDictionary<long, string> { [0] = "127.0.0.1:10911", [1] = "127.0.0.1:10921" });
        string json = bd.ToJson().Dump();
        Assert.Contains("\"brokerAddrs\":{\"0\":\"127.0.0.1:10911\",\"1\":\"127.0.0.1:10921\"}", json);
        Assert.Contains("\"brokerName\":\"broker-a\"", json);
        Assert.Contains("\"cluster\":\"DefaultCluster\"", json);
        Assert.Contains("\"enableActingMaster\":false", json);

        // 解析端容忍 fastjson2 裸数字键
        Assert.True(Json.TryParse("{\"brokerAddrs\":{0:\"127.0.0.1:10911\"},\"brokerName\":\"b\",\"cluster\":\"c\",\"enableActingMaster\":false,\"zoneName\":\"\"}",
            out JsonValue v, out _));
        BrokerData back = BrokerData.FromJson(v);
        Assert.Equal("b", back.BrokerName);
        Assert.Equal("127.0.0.1:10911", back.BrokerAddrs[0]);
        Assert.True(back.BrokerAddrs.ContainsKey(1L) == false);
    }

    [Fact]
    public void BrokerData_SelectBrokerAddr_PrefersMaster()
    {
        var bd = new BrokerData("c", "b", new SortedDictionary<long, string>
        {
            [1] = "slave:10921",
            [0] = "master:10911",
        });
        Assert.Equal("master:10911", bd.SelectBrokerAddr());

        var noMaster = new BrokerData("c", "b", new SortedDictionary<long, string> { [3] = "slave:10931" });
        Assert.Equal("slave:10931", noMaster.SelectBrokerAddr());
    }

    [Fact]
    public void TopicRouteData_GetAllMessageQueue_OnlyWritable()
    {
        var route = new TopicRouteData
        {
            QueueDatas = new List<QueueData>
            {
                new("broker-a", 8, 8, 6, 0),   // 可写
                new("broker-b", 8, 8, 4, 0),   // PERM_READ only -> 不可写
                new("broker-c", 8, 8, 2, 0),   // PERM_WRITE
            },
            BrokerDatas = new List<BrokerData>
            {
                new("c", "broker-a", new SortedDictionary<long, string> { [0] = "a:10911" }),
                new("c", "broker-c", new SortedDictionary<long, string> { [0] = "c:10911" }),
                // broker-b 不在 brokerDatas 中 -> 也要跳过
            },
        };

        List<MessageQueue> mqs = route.GetAllMessageQueue("T1");
        Assert.Equal(16, mqs.Count);
        Assert.All(mqs, mq =>
        {
            Assert.Equal("T1", mq.Topic);
            Assert.NotEqual("broker-b", mq.BrokerName);
        });
    }

    [Fact]
    public void TopicRouteData_EncodeDecode_RoundTrip()
    {
        var route = new TopicRouteData
        {
            OrderTopicConf = "",
            QueueDatas = new List<QueueData> { new("b", 8, 8, 6, 0) },
            BrokerDatas = new List<BrokerData>
            {
                new("c", "b", new SortedDictionary<long, string> { [0] = "1.2.3.4:10911" }),
            },
        };

        byte[] encoded = route.Encode();
        Assert.True(TopicRouteData.Decode(encoded, out TopicRouteData back));
        Assert.Single(back.QueueDatas);
        Assert.Equal("b", back.QueueDatas[0].BrokerName);
        Assert.Equal("1.2.3.4:10911", back.BrokerDatas[0].BrokerAddrs[0]);
    }

    [Fact]
    public void TopicRouteData_Changed_SortsBeforeCompare()
    {
        var a = new TopicRouteData
        {
            QueueDatas = new List<QueueData> { new("b1", 8, 8, 6, 0), new("b2", 8, 8, 6, 0) },
        };
        var b = new TopicRouteData
        {
            QueueDatas = new List<QueueData> { new("b2", 8, 8, 6, 0), new("b1", 8, 8, 6, 0) },
        };
        Assert.False(a.TopicRouteDataChanged(b)); // 排序后等价
        var c = new TopicRouteData
        {
            QueueDatas = new List<QueueData> { new("b1", 8, 8, 6, 0), new("b3", 8, 8, 6, 0) },
        };
        Assert.True(a.TopicRouteDataChanged(c));
        Assert.True(a.TopicRouteDataChanged(null));
    }

    // ---------------------------------------------------------------- 心跳

    [Fact]
    public void HeartbeatData_JsonFieldNames_Fastjson2Order()
    {
        var hb = new HeartbeatData("cid");
        hb.AddProducerData(new ProducerData("pg1"));
        var cd = new ConsumerData("cg1", ConsumeType.ConsumePassively,
            MessageModel.Clustering, ConsumeFromWhere.ConsumeFromLastOffset);
        cd.AddSubscriptionData(FilterAPI.BuildSubscriptionData("T1", "*"));
        hb.AddConsumerData(cd);

        string json = hb.ToJson().Dump();
        // 5.x：无 consumeTimestamp/maxReconsumeTimes；有 heartbeatFingerprint/withoutSub
        Assert.Contains("\"clientID\":\"cid\"", json);
        Assert.Contains("\"heartbeatFingerprint\":0", json);
        Assert.Contains("\"withoutSub\":false", json);
        Assert.Contains("\"groupName\":\"cg1\"", json);
        // 与 Java/C++ 一致：SUB_ALL 的 tagsSet 与 codeSet **都为空**（Java setSubString("*") 后直接 return）
        Assert.Contains("\"subscriptionDataSet\":[{\"classFilterMode\":false,\"codeSet\":[],\"expressionType\":\"TAG\",\"subString\":\"*\",\"subVersion\":", json);
        Assert.DoesNotContain("consumeTimestamp", json);
        Assert.DoesNotContain("maxReconsumeTimes", json);
    }

    [Fact]
    public void HeartbeatData_Decode_ToleratesIsWithoutSub()
    {
        // 兼容 Java 字段名 isWithoutSub（bool getter 的 fastjson2 名）
        const string json = "{\"clientID\":\"c\",\"consumerDataSet\":[],\"heartbeatFingerprint\":7," +
                            "\"producerDataSet\":[{\"groupName\":\"g\"}],\"isWithoutSub\":true}";
        Assert.True(HeartbeatData.Decode(Encoding.UTF8.GetBytes(json), out HeartbeatData hb));
        Assert.Equal(7, hb.HeartbeatFingerprint);
        Assert.True(hb.WithoutSub);
        Assert.Single(hb.ProducerDataSet);
        Assert.Equal("g", hb.ProducerDataSet[0].GroupName);
    }

    [Fact]
    public void BuildSubscriptionData()
    {
        // Java 对拍向量（探针 /tmp/subprobe/{SubProbe,BlankProbe,EdgeProbe}.java 实测）：
        //   "TagA||TagB" → tagsSet={TagA,TagB}, codeSet={2598919,2598920}
        //   "*" / ""     → subString 归一为 "*"，**两个集合都空**（Java 直接 return）
        //   "   "        → 走 split 分支，集合空但 subString **原样保留**
        //   "|||"        → Java-split 只丢末尾空串 → 字面量标签 "|"（hash 124）
        //   "||"         → Java-split 结果数组长度为 0 → 抛 "subString split error"
        SubscriptionData sd = FilterAPI.BuildSubscriptionData("T", "TagA||TagB");
        Assert.Equal("T", sd.Topic);
        Assert.Equal("TagA||TagB", sd.SubString);
        Assert.Equal(new SortedSet<string> { "TagA", "TagB" }, sd.TagsSet);
        Assert.Equal(new SortedSet<long> { 2598919, 2598920 }, sd.CodeSet);

        SubscriptionData star = FilterAPI.BuildSubscriptionData("T", "");
        Assert.Equal("*", star.SubString);
        Assert.Empty(star.TagsSet);
        Assert.Empty(star.CodeSet);

        SubscriptionData wild = FilterAPI.BuildSubscriptionData("T", "*");
        Assert.Equal("*", wild.SubString);
        Assert.Empty(wild.TagsSet);
        Assert.Empty(wild.CodeSet);

        // 纯空白 ≠ 空串：Java StringUtils.isEmpty 只认 null/""，所以"   "走 split 分支，
        // 标签被 trim 成空 ⇒ 集合仍空，但 subString 原样保留。
        SubscriptionData blank = FilterAPI.BuildSubscriptionData("T", "   ");
        Assert.Equal("   ", blank.SubString);
        Assert.Empty(blank.TagsSet);
        Assert.Empty(blank.CodeSet);

        // "|||"：确认实现的是 Java-split（只丢末尾空串）而不是"丢所有空段"
        SubscriptionData pipes = FilterAPI.BuildSubscriptionData("T", "|||");
        Assert.Equal(new SortedSet<string> { "|" }, pipes.TagsSet);
        Assert.Equal(new SortedSet<long> { 124 }, pipes.CodeSet);

        // "||" / "||||"：Java 抛 new Exception("subString split error")
        Assert.Throws<ArgumentException>(() => FilterAPI.BuildSubscriptionData("T", "||"));
        Assert.Throws<ArgumentException>(() => FilterAPI.BuildSubscriptionData("T", "||||"));
    }

    [Fact]
    public void SubscriptionData_CompareTo()
    {
        var s1 = new SubscriptionData("T1", "*");
        var s2 = new SubscriptionData("T2", "*");
        Assert.Equal("T1@*", s1.CompareTo(s2) < 0 ? "T1@*" : "???");
        Assert.Equal(0, s1.CompareTo(new SubscriptionData("T1", "*")));
    }
}
