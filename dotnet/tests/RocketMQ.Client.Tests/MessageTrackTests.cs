// 消息轨迹（Java DefaultMQAdminExtImpl.messageTrackDetail）与 VIP 通道开关的
// 离线单测：TrackType 字符串值与 Java 枚举名逐字对拍、VIP 端口换算取 Java
// MixAll.brokerVIPChannel 官方语义、admin 开关默认 false（5.x ClientConfig 默认值）。
// 「真集群上逐组判 CONSUMED/FILTERED/…」需要真 broker，归真机用例收口（四端同一约定）。
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class MessageTrackTests
{
    [Fact]
    public void TrackType_Members_MatchJavaEnumSet()
    {
        // Java TrackType 枚举（org.apache.rocketmq.tools.admin.api.TrackType）七个值，
        // C# 成员名按同一顺序一一对应（Java 名即成员名的 SCREAMING_SNAKE 形式）
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
    public void VipChannel_TranslatesPortMinus2_LikeJavaBrokerVIPChannel()
    {
        Assert.Equal("127.0.0.1:10909", MixAll.BrokerVipChannel(true, "127.0.0.1:10911"));
        Assert.Equal("127.0.0.1:10911", MixAll.BrokerVipChannel(false, "127.0.0.1:10911"));
        // 端口不可解析：Java 抛 NumberFormatException，这里原样返回
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
