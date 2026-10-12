using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class MessageTrackTests
{
    [Fact]
    public void TrackType_Members_MatchLegacyEnumSet()
    {
        Assert.Equal(new[]
        {
            "Consumed", "ConsumedButFiltered", "Pull", "NotConsumeYet",
            "NotOnline", "ConsumeBroadcasting", "Unknown",
        }, Enum.GetNames<TrackType>());
    }

    [Fact]
    public void MessageTrack_DefaultsToUnknown()
    {
        var mt = new MessageTrack { ConsumerGroup = "G1" };
        Assert.Equal(TrackType.Unknown, mt.TrackType);
        Assert.Null(mt.ExceptionDesc);
        Assert.Contains("G1", mt.ToString());
    }

    [Fact]
    public void VipChannel_TranslatesPortMinus2_LikeLegacyBrokerVIPChannel()
    {
        Assert.Equal("127.0.0.1:10909", MixAll.BrokerVipChannel(true, "127.0.0.1:10911"));
        Assert.Equal("127.0.0.1:10911", MixAll.BrokerVipChannel(false, "127.0.0.1:10911"));
        Assert.Equal("127.0.0.1:notaport", MixAll.BrokerVipChannel(true, "127.0.0.1:notaport"));
        Assert.Equal("no-colon", MixAll.BrokerVipChannel(true, "no-colon"));
    }

    [Fact]
    public void AdminVipChannel_KnobDefaultsOff_AndToggles()
    {
        var admin = new DefaultMQAdminExt();
        Assert.False(admin.VipChannelEnabled);
        admin.VipChannelEnabled = true;
        Assert.True(admin.VipChannelEnabled);
        admin.VipChannelEnabled = false;
        Assert.False(admin.VipChannelEnabled);
    }
}
