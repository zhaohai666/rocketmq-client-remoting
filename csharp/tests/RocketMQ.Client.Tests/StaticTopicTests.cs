// 静态主题路由测试（覆盖五类用例）：
// mock broker 名推导、类型化映射解析缺省值、端点推导（未承载队列哨兵 / scope
// 不合并 / epoch 围栏）、以及 GetBrokerNameFromMessageQueue 的重写/透传。
using System.Globalization;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class StaticTopicTests
{
    private static JsonValue RawMapping(string scope, int totalQueues, string bname,
        long epoch, JsonValue currIdMap)
    {
        var v = JsonValue.MakeObject();
        v.Set("scope", JsonValue.MakeString(scope));
        v.Set("totalQueues", JsonValue.MakeInt(totalQueues));
        v.Set("bname", JsonValue.MakeString(bname));
        v.Set("epoch", JsonValue.MakeInt(epoch));
        v.Set("dirty", JsonValue.MakeBool(false));
        v.Set("currIdMap", currIdMap);
        return v;
    }

    private static JsonValue CurrIdMap(params (string key, int value)[] pairs)
    {
        var v = JsonValue.MakeObject();
        foreach ((string key, int value) in pairs)
        {
            v.Set(key, JsonValue.MakeInt(value));
        }

        return v;
    }

    private static TopicRouteData MappingRoute(params (string broker, JsonValue info)[] entries)
    {
        var route = new TopicRouteData();
        var mapping = JsonValue.MakeObject();
        foreach ((string broker, JsonValue info) in entries)
        {
            mapping.Set(broker, info);
        }

        route.TopicQueueMappingByBroker = mapping;
        return route;
    }

    [Fact]
    public void MockBrokerNameStripsGlobalUnderscores()
    {
        Assert.Equal("__syslo__global__", StaticTopic.GetMockBrokerName("__global__"));
        Assert.Equal("__syslo__zone-a", StaticTopic.GetMockBrokerName("zone-a"));
        Assert.True(StaticTopic.IsLogicalQueueMockBrokerName("__syslo__global__"));
        Assert.False(StaticTopic.IsLogicalQueueMockBrokerName("broker-a"));
    }

    [Fact]
    public void ParseDefaultsScopeToGlobal()
    {
        var raw = JsonValue.MakeObject();
        raw.Set("totalQueues", JsonValue.MakeInt(2));
        raw.Set("bname", JsonValue.MakeString("b1"));
        raw.Set("epoch", JsonValue.MakeInt(3));
        Assert.True(StaticTopic.ParseTopicQueueMappingInfo(raw, out StaticTopic.TopicQueueMappingInfo? info));
        Assert.NotNull(info);
        Assert.Equal("__global__", info!.Scope);
        Assert.Equal(2, info.TotalQueues);
        Assert.Equal("b1", info.Bname);
        Assert.Equal(3, info.Epoch);
        Assert.False(StaticTopic.ParseTopicQueueMappingInfo(JsonValue.MakeString("x"), out _));
    }

    [Fact]
    public void EndpointsSingleScopeWithUnhostedQueue()
    {
        TopicRouteData route = MappingRoute(
            ("broker-a", RawMapping("__global__", 4, "broker-a", 0,
                CurrIdMap(("0", 0), ("1", 1), ("2", 2)))));
        var endpoints = new Dictionary<MessageQueue, string>();
        Assert.True(StaticTopic.TopicRouteData2EndpointsForStaticTopic("StaticTopic", route, endpoints));
        Assert.Equal(4, endpoints.Count); // hosted + unhosted
        for (int id = 0; id < 3; id++)
        {
            Assert.Equal("broker-a",
                endpoints[new MessageQueue("StaticTopic", "__syslo__global__", id)]);
        }

        Assert.Equal("__syslo__none__",
            endpoints[new MessageQueue("StaticTopic", "__syslo__global__", 3)]);
        // 空映射（非静态主题）返回 false。
        Assert.False(StaticTopic.TopicRouteData2EndpointsForStaticTopic("T", new TopicRouteData(),
            new Dictionary<MessageQueue, string>()));
    }

    [Fact]
    public void ScopesStayApartAndEpochFences()
    {
        TopicRouteData route = MappingRoute(
            ("broker-old", RawMapping("__global__", 1, "broker-old", 3, CurrIdMap(("0", 0)))),
            ("broker-new", RawMapping("__global__", 1, "broker-new", 7, CurrIdMap(("0", 0)))),
            ("broker-b", RawMapping("zone-x", 1, "broker-b", 9, CurrIdMap(("0", 0)))));
        var endpoints = new Dictionary<MessageQueue, string>();
        Assert.True(StaticTopic.TopicRouteData2EndpointsForStaticTopic("T", route, endpoints));
        Assert.Equal("broker-new", endpoints[new MessageQueue("T", "__syslo__global__", 0)]);
        Assert.Equal("broker-b", endpoints[new MessageQueue("T", "__syslo__zone-x", 0)]);
        Assert.Equal(2, endpoints.Count); // scopes do not merge
    }

    [Fact]
    public void InstanceRewritesMockBrokerNameViaEndPointsTable()
    {
        var inst = new MQClientInstance("static-test@DEFAULT",
            new List<string> { "127.0.0.1:9876" });
        inst.SetTopicEndPointsForTest("StaticTopic", new Dictionary<MessageQueue, string>
        {
            [new MessageQueue("StaticTopic", "__syslo__global__", 0)] = "broker-a",
            [new MessageQueue("StaticTopic", "__syslo__global__", 1)] = "broker-b",
        });

        Assert.Equal("broker-a", inst.GetBrokerNameFromMessageQueue(
            new MessageQueue("StaticTopic", "__syslo__global__", 0)));
        Assert.Equal("broker-b", inst.GetBrokerNameFromMessageQueue(
            new MessageQueue("StaticTopic", "__syslo__global__", 1)));
        // 表里没有的队列 / 非静态主题原样透传自己的 broker 名。
        Assert.Equal("__syslo__global__", inst.GetBrokerNameFromMessageQueue(
            new MessageQueue("StaticTopic", "__syslo__global__", 99)));
        Assert.Equal("broker-a", inst.GetBrokerNameFromMessageQueue(
            new MessageQueue("Plain", "broker-a", 0)));
    }
}
