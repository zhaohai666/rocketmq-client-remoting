// The capability gaps closed against the Java 5.x reference client.
//
// Each block pins one Java facade to this port and pairs the new accessor with the behaviour it
// steers, so a refactor that keeps the setter but drops the wiring goes red here:
//
//  - ConsumeFromWhere: all six values of the Java enum (the three "deprecated" ones are not dead:
//    Java folds them into the CONSUME_FROM_LAST_OFFSET branch of the offset-init switch,
//    RebalancePushImpl#computePullFromWhereWithException lines 171-173).
//  - LitePull connectBrokerByUser / defaultBrokerId: the only way to read from a slave
//    (PullAPIWrapper#recalculatePullFromWhichNode). Off must keep the historic "no broker id on
//    the request" shape.
//  - Admin adminExtGroup: default "admin_ext_group" plus the Java accessor pair.
//
// Nothing here dials: the pin is read through LiteBrokerIdForTest, the broker id a round would
// carry, and the group is a value on the facade.
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class CapabilityCloseoutTests
{
    // The authoritative list is Java common/.../consumer/ConsumeFromWhere.java.
    private static readonly string[] JavaConsumeFromWhere =
    {
        "CONSUME_FROM_LAST_OFFSET",
        "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
        "CONSUME_FROM_MIN_OFFSET",
        "CONSUME_FROM_MAX_OFFSET",
        "CONSUME_FROM_FIRST_OFFSET",
        "CONSUME_FROM_TIMESTAMP",
    };

    [Fact]
    public void ConsumeFromWhereCarriesAllSixJavaValues()
    {
        string[] ours =
        {
            ConsumeFromWhere.ConsumeFromLastOffset,
            ConsumeFromWhere.ConsumeFromLastOffsetAndFromMinWhenBootFirst,
            ConsumeFromWhere.ConsumeFromMinOffset,
            ConsumeFromWhere.ConsumeFromMaxOffset,
            ConsumeFromWhere.ConsumeFromFirstOffset,
            ConsumeFromWhere.ConsumeFromTimestamp,
        };
        Assert.Equal(JavaConsumeFromWhere, ours);
    }

    [Fact]
    public void LitePullBrokerPinDefaultsMatchJava()
    {
        DefaultLitePullConsumer c = new("GID_CloseoutPin");
        Assert.False(c.IsConnectBrokerByUser);
        Assert.Equal(MixAll.MasterId, c.DefaultBrokerId);
    }

    [Fact]
    public void LitePullBrokerPinSteersTheRequestBrokerId()
    {
        DefaultLitePullConsumer c = new("GID_CloseoutPinResolve");
        MessageQueue mq = new("CloseoutTopic", "broker-a", 0);

        // Off means "no broker id on the request": MQClientInstance.PullMessage then resolves the
        // address from the route, which is what every lite round did before this switch existed.
        Assert.Null(c.LiteBrokerIdForTest(mq));

        c.SetConnectBrokerByUser(true);
        c.SetDefaultBrokerId(1);
        Assert.True(c.IsConnectBrokerByUser);
        Assert.Equal(1, c.DefaultBrokerId);
        Assert.Equal(1, c.LiteBrokerIdForTest(mq));

        c.SetConnectBrokerByUser(false);
        Assert.Null(c.LiteBrokerIdForTest(mq));
        Assert.Equal(1, c.DefaultBrokerId); // unpinning keeps the id; the switch decides its use
    }

    [Fact]
    public void AdminExtGroupMatchesJava()
    {
        Assert.Equal("admin_ext_group", DefaultMQAdminExt.DefaultAdminExtGroup);
        DefaultMQAdminExt admin = new();
        Assert.Equal("admin_ext_group", admin.GetAdminExtGroup());
        admin.SetAdminExtGroup("custom-admin-group");
        Assert.Equal("custom-admin-group", admin.GetAdminExtGroup());
        Assert.Equal("custom-admin-group", admin.AdminExtGroup);
    }

    [Fact]
    public void AwaitTerminationMillisWhenShutdownRoundTrips()
    {
        // Six of the seven ports default this to Java's 0; the C# port has always had the knob.
        DefaultMQPushConsumer c = new("GID_CloseoutAwait");
        Assert.Equal(0, c.AwaitTerminationMillisWhenShutdown);
        c.SetAwaitTerminationMillisWhenShutdown(1500);
        Assert.Equal(1500, c.AwaitTerminationMillisWhenShutdown);
    }
}
