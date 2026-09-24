// SEARCH_OFFSET_BY_TIMESTAMP(29) 的 boundaryType 字段（离线，无真实集群）。
//
// 镜像 python/tests/test_search_offset_boundary.py 与 C++ test_admin.cpp 的
// testSearchOffsetRequestHeaderBoundaryType。Java 锚点（5.5.1 逐条核对）：
//   * SearchOffsetRequestHeader:41 private BoundaryType boundaryType —— @CFNullable，
//     不 set 就为 null；getBoundaryType():85 在 null 时回落 LOWER。
//   * RemotingCommand.makeCustomHeaderToNet:430 fieldsMap.put(name, value.toString())
//     ⇒ 枚举入网走 Enum.toString() = **大写枚举名** LOWER/UPPER（BoundaryType.getName()
//     那个小写名 :33 只喂给 getType，不上报文）；值为 null 的字段**整键不写**。
//   * MQClientAPIImpl#searchOffset(addr, mq, ts, timeout):1377 直接转 ...LOWER...:1381
//     —— MQ 级入口一定带 LOWER；真正不带字段的只有已废弃的 5 参重载 :1352。
//   * DefaultMQAdminExt:133/:137 对外给的是 searchLowerBoundaryOffset /
//     searchUpperBoundaryOffset 两个方法名。
//   * broker 侧解析：只有 equalsIgnoreCase("upper") 才是 UPPER，其余一律 LOWER。
using System.Collections.Generic;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class SearchOffsetBoundaryTests
{
    private const long Ts = 1700000000000L;
    private static readonly MessageQueue Mq = new("T_Boundary", "broker-0", 2);

    private static SearchOffsetRequestHeader Header(BoundaryType? boundary) => new()
    {
        Topic = Mq.Topic,
        QueueId = Mq.QueueId,
        Timestamp = Ts,
        BoundaryType = boundary,
    };

    private static DefaultMQAdminExt Started(MockCluster cluster)
    {
        var admin = new DefaultMQAdminExt("ADMIN_boundary");
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        admin.Start();
        return admin;
    }

    // ---------------------------------------------------------------- 报文形状

    /// <summary>
    /// 入网文本必须是 Enum.toString() 的大写枚举名。Java 的 BoundaryType 有
    /// getName()（"lower"/"upper"）与枚举名两套写法，报文里出现哪一套 broker 都认
    /// （getType 大小写不敏感），但对拍 Java 客户端时必须是大写名 —— 这条断言就是
    /// 防止有人"顺手"写成小写。
    /// </summary>
    [Fact]
    public void WireValueIsTheJavaEnumNameNotTheLowercaseName()
    {
        foreach ((BoundaryType boundary, string expected) in
                 new[] { (BoundaryType.Lower, "LOWER"), (BoundaryType.Upper, "UPPER") })
        {
            PropertyMap ext = Header(boundary).ToExtFields();
            Assert.Equal(expected, ext["boundaryType"]);
            Assert.Equal(expected.ToLowerInvariant(), BoundaryTypeNames.LowercaseName(boundary));
        }

        // 两套写法确实不同：getName() 拿到的小写名永远不进报文
        Assert.NotEqual(BoundaryTypeNames.Name(BoundaryType.Upper),
            BoundaryTypeNames.LowercaseName(BoundaryType.Upper));
    }

    /// <summary>@CFNullable：没设就整键不写，不能发 boundaryType=""。</summary>
    [Fact]
    public void UnsetBoundaryOmitsTheField()
    {
        PropertyMap ext = Header(null).ToExtFields();
        Assert.False(ext.ContainsKey("boundaryType"));
        Assert.Equal(new[] { "queueId", "timestamp", "topic" }, new List<string>(ext.Keys));

        var roundTrip = new SearchOffsetRequestHeader();
        roundTrip.FromExtFields(new PropertyMap());
        Assert.Null(roundTrip.BoundaryType);
    }

    /// <summary>BoundaryType.getType：只有 upper（大小写不敏感）才是 UPPER。</summary>
    [Fact]
    public void DecodeIsLenientLikeJavaGetType()
    {
        var header = new SearchOffsetRequestHeader();
        foreach ((string text, BoundaryType expected) in new[]
                 {
                     ("UPPER", BoundaryType.Upper), ("upper", BoundaryType.Upper),
                     ("Upper", BoundaryType.Upper), ("LOWER", BoundaryType.Lower),
                     ("lower", BoundaryType.Lower), ("", BoundaryType.Lower),
                     ("junk", BoundaryType.Lower),
                 })
        {
            header.FromExtFields(new PropertyMap { ["boundaryType"] = text });
            Assert.Equal(expected, header.BoundaryType);
        }

        Assert.Equal(BoundaryType.Lower, BoundaryTypeNames.GetType(null));
        Assert.Equal(BoundaryType.Lower, BoundaryTypeNames.GetType("123"));
    }

    /// <summary>缺键 ⇒ null（读取端自行回落 LOWER），与 @CFNullable 的语义一致。</summary>
    [Fact]
    public void MissingKeyDecodesToNullNotLower()
    {
        var header = new SearchOffsetRequestHeader();
        header.FromExtFields(new PropertyMap { ["topic"] = Mq.Topic, ["queueId"] = "2" });
        Assert.Null(header.BoundaryType);
    }

    // ---------------------------------------------------------------- 调用链（真连接，mock broker）

    [Fact]
    public void ClientAlwaysSendsLowerByDefault()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster);
        try
        {
            admin.SearchOffset(Mq, Ts);

            List<WireRecord> hits = cluster.Records().FindAll(
                r => r.Code == RequestCode.SearchOffsetByTimestamp);
            WireRecord hit = Assert.Single(hits);
            Assert.Equal("LOWER", hit.Ext["boundaryType"]);
            Assert.Equal(Mq.Topic, hit.Ext["topic"]);
            Assert.Equal("2", hit.Ext["queueId"]);
            Assert.Equal(Ts.ToString(), hit.Ext["timestamp"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ClientCanAskForUpper()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster);
        try
        {
            admin.SearchUpperBoundaryOffset(Mq, Ts);
            Assert.Equal(1, cluster.CountRequestsWith(RequestCode.SearchOffsetByTimestamp,
                "boundaryType", "UPPER"));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>Java 已废弃的 5 参重载 :1352 是唯一"不写字段"的路径，保留以对齐报文。</summary>
    [Fact]
    public void NullBoundaryKeepsTheFieldOffTheWire()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster);
        try
        {
            admin.GetMQClientInstance().SearchOffsetByBoundary(Mq, 1, null);

            Assert.False(cluster.AnyRequestHas(RequestCode.SearchOffsetByTimestamp,
                "boundaryType"));
            Assert.Equal(1, cluster.CountRequests(RequestCode.SearchOffsetByTimestamp));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>Java DefaultMQAdminExt:133/:137 的两个方法各自固定 LOWER / UPPER。</summary>
    [Fact]
    public void AdminExposesJavaBoundaryMethodNames()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster);
        try
        {
            admin.SearchOffset(Mq, Ts);
            admin.SearchLowerBoundaryOffset(Mq, Ts);
            admin.SearchUpperBoundaryOffset(Mq, Ts);

            List<WireRecord> hits = cluster.Records().FindAll(
                r => r.Code == RequestCode.SearchOffsetByTimestamp);
            Assert.Equal(3, hits.Count);
            Assert.Equal("LOWER", hits[0].Ext["boundaryType"]);
            Assert.Equal("LOWER", hits[1].Ext["boundaryType"]);
            Assert.Equal("UPPER", hits[2].Ext["boundaryType"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }
}
