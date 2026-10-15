// The boundaryType field of SEARCH_OFFSET_BY_TIMESTAMP(29) (offline, no real cluster).
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

    // ---------------------------------------------------------------- Wire shape

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

        // the two spellings really differ: the lowercase name from getName() never enters the message
        Assert.NotEqual(BoundaryTypeNames.Name(BoundaryType.Upper),
            BoundaryTypeNames.LowercaseName(BoundaryType.Upper));
    }

    /// <summary>@CFNullable: when it is not set the whole key is not written, and
    /// boundaryType="" must not be sent.</summary>
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

    /// <summary>BoundaryType.getType: only upper (case-insensitive) is UPPER.</summary>
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

    /// <summary>A missing key => null (the reading side falls back to LOWER itself),
    /// consistent with the @CFNullable semantics.</summary>
    [Fact]
    public void MissingKeyDecodesToNullNotLower()
    {
        var header = new SearchOffsetRequestHeader();
        header.FromExtFields(new PropertyMap { ["topic"] = Mq.Topic, ["queueId"] = "2" });
        Assert.Null(header.BoundaryType);
    }

    // ---------------------------------------------------------------- The call chain (a real connection, mock broker)

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
