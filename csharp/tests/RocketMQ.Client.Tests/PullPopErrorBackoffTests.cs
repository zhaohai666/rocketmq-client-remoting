// Offline unit tests for the ONE generic error backoff of the push consumer's two
// consume loops: the wait before re-issuing a pull (or pop) request after a round that
// failed for any reason other than the broker's own flow control.
//
// It used to be two constants that disagreed (500 ms on the pull path, 3000 ms on the
// POP re-schedule's POLLING_FULL leg). Now it is one configurable knob
// (DefaultMQPushConsumer.PullExceptionBackoffMillis, default 1000 ms) which each loop
// captures when it is set up and hands to the pure decision helpers, so every leg here
// asks the helper for the value the running loop would park on.
//
// The three arms that deliberately do NOT move with the knob are pinned too: a broker
// FLOW_CONTROL(215) retries in 20 ms, this client's own cache-threshold gate parks for
// 50 ms, and a paused consumer keeps polling every 1000 ms.
using RocketMQ.Client;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class PullPopErrorBackoffTests
{
    private const string Group = "GID_PullPopErrorBackoff";

    /// <summary>Leg 1: the knob is a real property with a getter and a setter, and its
    /// default is 1000 ms for a consumer nobody touched.</summary>
    [Fact]
    public void Knob_DefaultsToOneThousandMillis()
    {
        var c = new DefaultMQPushConsumer(Group);
        Assert.Equal(1000, c.PullExceptionBackoffMillis);
        Assert.Equal(1000, new DefaultMQPushConsumer(Group).PullExceptionBackoffMillis);
        c.PullExceptionBackoffMillis = 1234;
        Assert.Equal(1234, c.PullExceptionBackoffMillis);
    }

    /// <summary>Leg 2: the 215 answer is not a capacity verdict on this client, so it
    /// keeps its own early retry -- and moving the knob does not touch it.</summary>
    [Fact]
    public void BrokerFlowControl215_KeepsItsTwentyMillis_EverythingElseTakesTheKnob()
    {
        Assert.Equal(20, DefaultMQPushConsumer.BrokerFlowControlBackoffMillis);
        foreach (int knob in new[] { 1000, 1750, 5000 })
        {
            Assert.Equal(20, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
                ResponseCode.FlowControl, knob));
            Assert.Equal(knob, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
                ResponseCode.SystemError, knob));
        }

        // The two arms are different numbers on the same consumer: without the 215
        // branch these two calls answer identically, which is what makes the split real.
        var c = new DefaultMQPushConsumer(Group);
        Assert.NotEqual(
            DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
                ResponseCode.FlowControl, c.PullExceptionBackoffMillis),
            DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
                ResponseCode.SystemBusy, c.PullExceptionBackoffMillis));
    }

    /// <summary>Leg 3: one knob drives both paths -- the pull path's broker-error arm and
    /// the POP path's failed-round arm both read the value the caller captured.</summary>
    [Fact]
    public void SettingTheKnob_MovesTheWaitOnBothPaths()
    {
        var c = new DefaultMQPushConsumer(Group) { PullExceptionBackoffMillis = 1750 };
        int knob = c.PullExceptionBackoffMillis;

        // Pull path.
        Assert.Equal(1750, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.SystemError, knob));
        // POP path: a POLLING_FULL verdict the re-schedule cannot act on.
        Assert.Equal(1750, DefaultMQPushConsumer.PopNextRoundDelayMillis(
            PopStatus.PollingFull, 0, 50, knob));
        // POP path: a status the decision does not know at all.
        Assert.Equal(1750, DefaultMQPushConsumer.PopNextRoundDelayMillis(
            (PopStatus)42, 0, 0, knob));

        // Control on an untouched consumer: the same two calls answer the default, so
        // the pair above really measures the knob and not a constant.
        int fallback = new DefaultMQPushConsumer(Group).PullExceptionBackoffMillis;
        Assert.Equal(1000, fallback);
        Assert.Equal(1000, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.SystemError, fallback));
        Assert.Equal(1000, DefaultMQPushConsumer.PopNextRoundDelayMillis(
            PopStatus.PollingFull, 0, 50, fallback));
    }

    /// <summary>Leg 4: the failed arms -- a non-215 broker error on the pull path and a
    /// POLLING_FULL round on the POP path -- answer the SAME number, while a round that
    /// simply found nothing and the client's own cache gate keep their own.</summary>
    [Fact]
    public void NonFlowControlErrorAndPollingFull_AreTheSameArm()
    {
        var c = new DefaultMQPushConsumer(Group);
        int knob = c.PullExceptionBackoffMillis;

        int pullArm = DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.SystemError, knob);
        int popArm = DefaultMQPushConsumer.PopNextRoundDelayMillis(
            PopStatus.PollingFull, 0, 50, knob);
        Assert.Equal(pullArm, popArm);
        Assert.Equal(knob, pullArm);

        // A POLLING_FULL round stays distinguishable from a round that found nothing:
        // the empty answer goes straight to the next round.
        Assert.NotEqual(pullArm, DefaultMQPushConsumer.PopNextRoundDelayMillis(
            PopStatus.PollingNotFound, 0, 50, knob));
        Assert.Equal(0, DefaultMQPushConsumer.PopNextRoundDelayMillis(
            PopStatus.NoNewMsg, 0, 50, knob));
        // The failed arms sit above both the 20 ms early retry and this client's own
        // 50 ms cache park, and the two unmoved arms stay two different numbers.
        Assert.True(pullArm > DefaultMQPushConsumer.BrokerFlowControlBackoffMillis);
        Assert.True(pullArm > DefaultMQPushConsumer.CacheFlowControlBackoffMillis);
        Assert.Equal(50, DefaultMQPushConsumer.CacheFlowControlBackoffMillis);
    }

    /// <summary>The knob's one input rule: it is a wait, so a negative value means
    /// "re-issue at once" rather than parking the loop forever.</summary>
    [Fact]
    public void NegativeKnob_IsClampedToZero()
    {
        var c = new DefaultMQPushConsumer(Group) { PullExceptionBackoffMillis = -5000 };
        Assert.Equal(0, c.PullExceptionBackoffMillis);
        Assert.Equal(0, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.SystemError, c.PullExceptionBackoffMillis));
        // The 215 arm is still the early retry, so the two arms do not collapse.
        Assert.Equal(20, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.FlowControl, c.PullExceptionBackoffMillis));
    }
}
