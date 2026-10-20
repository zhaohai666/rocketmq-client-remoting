// Offline unit tests for the pull loop's backoff decision when the broker answers a pull
// with an error response code: FLOW_CONTROL (215) is the broker being busy, not the
// client's own cache threshold, and the two must not share one backoff number.
//
// The decision is asserted as a pure recorded value (what the loop would wait), never by
// racing a timer.
using RocketMQ.Client;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class BrokerFlowControlBackoffTests
{
    [Fact]
    public void BrokerFlowControl_RetriesEarly_WithTwentyMillis()
    {
        // The wire contract first: 215 is the code the broker answers a flow-controlled pull with.
        Assert.Equal(215, ResponseCode.FlowControl);
        Assert.Equal(20, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.FlowControl));
        Assert.Equal(20, DefaultMQPushConsumer.BrokerFlowControlBackoffMillis);
    }

    [Fact]
    public void OtherBrokerErrors_KeepTheGeneralExceptionBackoff()
    {
        // Control for the test above: only 215 gets the early retry. Reverting the
        // FLOW_CONTROL branch makes both facts identical here, but flips the positive
        // assertion; hardcoding "always 20" fails this one.
        Assert.Equal(500, DefaultMQPushConsumer.PullExceptionBackoffMillis);
        foreach (int code in new[]
        {
            ResponseCode.SystemError,
            ResponseCode.SystemBusy,
            ResponseCode.TopicNotExist,
            ResponseCode.SubscriptionParseFailed,
            ResponseCode.BrokerNotExist,
        })
        {
            Assert.Equal(DefaultMQPushConsumer.PullExceptionBackoffMillis,
                DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(code));
        }
    }

    [Fact]
    public void FlowControlAndCacheThresholds_AreDifferentNumbers()
    {
        // The broker-side early retry must be strictly shorter than the general
        // exception backoff, otherwise the branch carries no behaviour.
        Assert.True(DefaultMQPushConsumer.BrokerFlowControlBackoffMillis
            < DefaultMQPushConsumer.PullExceptionBackoffMillis);
    }
}
