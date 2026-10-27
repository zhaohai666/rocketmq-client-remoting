// Offline unit tests for the pull loop's backoff decision when the broker answers a pull
// with an error response code: FLOW_CONTROL (215) is the broker being busy, not the
// client's own cache threshold, and the two must not share one backoff number.
//
// The generic arm is no longer a constant baked into the decision: it is the consumer's
// configured PullExceptionBackoffMillis, which the loop captures when it is set up and
// hands to the helper. The tests therefore take the SAME consumer's value as the loop
// would, so a leg pins the rule and the knob in one assertion.
//
// The decision is asserted as a pure recorded value (what the loop would wait), never by
// racing a timer.
using RocketMQ.Client;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class BrokerFlowControlBackoffTests
{
    private const string Group = "GID_BrokerFlowControlBackoff";

    [Fact]
    public void BrokerFlowControl_RetriesEarly_WithTwentyMillis()
    {
        // The wire contract first: 215 is the code the broker answers a flow-controlled pull with.
        Assert.Equal(215, ResponseCode.FlowControl);
        Assert.Equal(20, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.FlowControl, 1000));
        Assert.Equal(20, DefaultMQPushConsumer.BrokerFlowControlBackoffMillis);
    }

    [Fact]
    public void OtherBrokerErrors_TakeTheConfiguredGenericBackoff()
    {
        // Control for the test above: only 215 gets the early retry. Reverting the
        // FLOW_CONTROL branch makes both facts identical here, but flips the positive
        // assertion; hardcoding "always 20" fails this one.
        var c = new DefaultMQPushConsumer(Group);
        int knob = c.PullExceptionBackoffMillis;
        Assert.Equal(1000, knob);
        foreach (int code in new[]
        {
            ResponseCode.SystemError,
            ResponseCode.SystemBusy,
            ResponseCode.TopicNotExist,
            ResponseCode.SubscriptionParseFailed,
            ResponseCode.BrokerNotExist,
        })
        {
            Assert.Equal(knob,
                DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(code, knob));
        }

        // The 215 arm ignores the knob entirely: a busy broker is not a capacity verdict,
        // so moving the generic wait must not move the early retry.
        c.PullExceptionBackoffMillis = 2500;
        Assert.Equal(20, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.FlowControl, c.PullExceptionBackoffMillis));
    }

    [Fact]
    public void FlowControlAndCacheThresholds_AreDifferentNumbers()
    {
        // The broker-side early retry must be strictly shorter than the generic
        // backoff, otherwise the branch carries no behaviour.
        Assert.True(DefaultMQPushConsumer.BrokerFlowControlBackoffMillis
            < new DefaultMQPushConsumer(Group).PullExceptionBackoffMillis);
        // And the three arms are three different numbers.
        Assert.NotEqual(DefaultMQPushConsumer.CacheFlowControlBackoffMillis,
            DefaultMQPushConsumer.BrokerFlowControlBackoffMillis);
    }
}
