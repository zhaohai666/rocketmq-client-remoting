// SEARCH_OFFSET_BY_TIMESTAMP(29) 的 boundaryType 字段（离线，无真实集群）。
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

    [Fact]
    public void WireValueIsTheLegacyEnumNameNotTheLowercaseName()
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
    public void DecodeIsLenientLikeLegacyGetType()
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

    [Fact]
    public void AdminExposesLegacyBoundaryMethodNames()
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
