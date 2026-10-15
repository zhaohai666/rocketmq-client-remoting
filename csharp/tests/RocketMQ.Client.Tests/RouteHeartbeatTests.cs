// Unit tests for the route / heartbeat structs (the core assertions on fields and codec).
//
// The emphasis: JSON field names aligned with fastjson2 (keys in alphabetical order), the
// TBW102 fallback trimming, MessageQueue assembly picking only the writable queues, and
// ConsumerData carrying no 4.x fields.
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

        // The parsing side tolerates fastjson2's bare numeric keys
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
                new("broker-a", 8, 8, 6, 0),   // writable
                new("broker-b", 8, 8, 4, 0),   // PERM_READ only -> not writable
                new("broker-c", 8, 8, 2, 0),   // PERM_WRITE
            },
            BrokerDatas = new List<BrokerData>
            {
                new("c", "broker-a", new SortedDictionary<long, string> { [0] = "a:10911" }),
                new("c", "broker-c", new SortedDictionary<long, string> { [0] = "c:10911" }),
                // broker-b is not in brokerDatas -> it must be skipped as well
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
    public void TopicRouteData_GetAllMessageQueue_SkipsBrokerWithoutMaster()
    {
        var route = new TopicRouteData
        {
            QueueDatas = new List<QueueData>
            {
                new("broker-a", 4, 4, 6, 0),   // master down -> only brokerId=1 left
                new("broker-b", 4, 4, 6, 0),   // both master and slave present
            },
            BrokerDatas = new List<BrokerData>
            {
                new("c", "broker-a", new SortedDictionary<long, string> { [1] = "a:10931" }),
                new("c", "broker-b", new SortedDictionary<long, string>
                {
                    [0] = "b:10911",
                    [1] = "b:10931",
                }),
            },
        };

        List<MessageQueue> mqs = route.GetAllMessageQueue("T");
        Assert.Equal(4, mqs.Count);
        Assert.All(mqs, mq => Assert.Equal("broker-b", mq.BrokerName));

        // Negative control: put the master back and broker-a's 4 entries come straight back (it skips "there is no master", not the name)
        route.BrokerDatas[0].BrokerAddrs[0] = "a:10911";
        Assert.Equal(8, route.GetAllMessageQueue("T").Count);
    }

    [Fact]
    public void TopicRouteData_GetAllSubscribeMessageQueue_KeepsMasterlessBroker()
    {
        var route = new TopicRouteData
        {
            QueueDatas = new List<QueueData>
            {
                new("broker-a", 4, 4, 6, 0),   // master down -> only brokerId=1 left
                new("broker-ro", 3, 1, 4, 0),  // a read-only topic (perm=4)
            },
            BrokerDatas = new List<BrokerData>
            {
                new("c", "broker-a", new SortedDictionary<long, string> { [1] = "a:10931" }),
            },
        };

        // Publish side: perm=6 plus a master gets in; broker-a has no master => empty
        Assert.Empty(route.GetAllMessageQueue("T"));

        // Subscribe side: broker-a's 4 entries + broker-ro's 3 (even though broker-ro is not in BrokerDatas)
        List<MessageQueue> sub = route.GetAllSubscribeMessageQueue("T");
        Assert.Equal(7, sub.Count);
        Assert.Equal("broker-a", sub[0].BrokerName);
        Assert.Equal(3, sub[3].QueueId);
        Assert.Equal("broker-ro", sub[4].BrokerName);
        Assert.Equal(2, sub[6].QueueId);

        // Negative control: clearing the read bit from perm (2 = write only) makes broker-ro disappear and leaves broker-a unaffected.
        route.QueueDatas[1].Perm = 2;
        Assert.Equal(4, route.GetAllSubscribeMessageQueue("T").Count);
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
        Assert.False(a.TopicRouteDataChanged(b)); // equivalent after sorting
        var c = new TopicRouteData
        {
            QueueDatas = new List<QueueData> { new("b1", 8, 8, 6, 0), new("b3", 8, 8, 6, 0) },
        };
        Assert.True(a.TopicRouteDataChanged(c));
        Assert.True(a.TopicRouteDataChanged(null));
    }

    // ---------------------------------------------------------------- Heartbeat

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
        // 5.x: no consumeTimestamp/maxReconsumeTimes; there is heartbeatFingerprint/withoutSub
        Assert.Contains("\"clientID\":\"cid\"", json);
        Assert.Contains("\"heartbeatFingerprint\":0", json);
        Assert.Contains("\"withoutSub\":false", json);
        Assert.Contains("\"groupName\":\"cg1\"", json);
        Assert.Contains("\"subscriptionDataSet\":[{\"classFilterMode\":false,\"codeSet\":[],\"expressionType\":\"TAG\",\"subString\":\"*\",\"subVersion\":", json);
        Assert.DoesNotContain("consumeTimestamp", json);
        Assert.DoesNotContain("maxReconsumeTimes", json);
    }

    [Fact]
    public void HeartbeatData_Decode_ToleratesIsWithoutSub()
    {
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

        SubscriptionData blank = FilterAPI.BuildSubscriptionData("T", "   ");
        Assert.Equal("   ", blank.SubString);
        Assert.Empty(blank.TagsSet);
        Assert.Empty(blank.CodeSet);

        SubscriptionData pipes = FilterAPI.BuildSubscriptionData("T", "|||");
        Assert.Equal(new SortedSet<string> { "|" }, pipes.TagsSet);
        Assert.Equal(new SortedSet<long> { 124 }, pipes.CodeSet);

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
