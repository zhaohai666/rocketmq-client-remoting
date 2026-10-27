// Offline unit tests for the PULL loop's round cadence: how long a round waits before
// re-issuing the next pull, keyed on what actually survives the pre-delivery filter.
//
// The rule mirrors the POP re-schedule decision PopNextRoundDelayMillis in this same
// class: only a round that DELIVERED rows is paced by PullIntervalMillis. A round that
// came back empty, was emptied by the tag pass, or answered NO_NEW_MSG / NO_MATCHED_MSG
// goes straight to the next round -- the cadence paces deliveries, so a local wait on
// an idle round would only delay the next fetch. An OFFSET_ILLEGAL round exits into its own
// recovery and never reaches a cadence wait, and a FAILED round backs off before the
// cadence is ever consulted, so the cadence replaces neither the 215 early retry nor the
// generic error backoff.
//
// The decision is asserted as a pure recorded value (what the loop would wait), never by
// racing a timer.
using RocketMQ.Client;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class PullRoundCadenceTests
{
    private const string Group = "GID_PullRoundCadence";

    /// <summary>A pace value distinct from every wait the loop already owns (the 20 ms
    /// early retry, the 50 ms cache park, the 1000 ms generic backoff), so a leg that
    /// returns it can never be mistaken for one of those arms.</summary>
    private const int Interval = 50;

    [Fact]
    public void PullNextRound_FoundWithDeliveredRows_PacesByIntervalOrGoesStraightOn()
    {
        // Interval 0: no pace to wait, next round now.
        Assert.Equal(0, DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.Found, 2, 0));
        // Interval > 0: the configured pace applies to a real delivery.
        Assert.Equal(Interval,
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.Found, 2, Interval));
    }

    [Fact]
    public void PullNextRound_FoundEmptiedByTheFilter_GoesStraightOn_EvenWhenIntervalIsSet()
    {
        // The tag pass removed every row, so the surviving list PULL_TPS counts is
        // empty: this round delivered nothing and the interval must not fire. Reverting
        // the wait to the raw pulled count -- or dropping the deliveredCount gate --
        // would return the interval here and go red.
        Assert.Equal(0,
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.Found, 0, Interval));
    }

    [Fact]
    public void PullNextRound_EmptyAnswers_GoStraightOn()
    {
        // A round that simply found nothing is NOT a delivery and NOT a failure: the
        // cadence paces rounds that delivered, whatever the interval is set to.
        Assert.Equal(0,
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.NoNewMsg, 0, Interval));
        Assert.Equal(0,
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.NoMatchedMsg, 0, Interval));
    }

    [Fact]
    public void PullNextRound_OffsetIllegal_NeverPaces_TheRecoveryExitsTheRound()
    {
        // Control for the cadence: an offset-illegal answer goes to its own
        // freeze/persist/revoke/recover path and exits the round; it never sits on the
        // interval. The gate is the status, not the (always-empty here) count -- so even
        // a spurious non-empty count cannot make it pace.
        Assert.Equal(0,
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.OffsetIllegal, 0, Interval));
        Assert.Equal(0,
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.OffsetIllegal, 5, Interval));
    }

    /// <summary>The cadence must not have swallowed the two failed-round waits. This
    /// consumer's generic backoff is the same knob the loop captures at set-up, so the
    /// leg reads the real default (1000 ms) rather than a hand-picked number.</summary>
    [Fact]
    public void Cadence_DoesNotReplace_TheEarlyRetry_OrTheGenericBackoff()
    {
        var c = new DefaultMQPushConsumer(Group);
        int knob = c.PullExceptionBackoffMillis;
        Assert.Equal(1000, knob);

        // A broker FLOW_CONTROL(215) still retries early, independent of the interval.
        Assert.Equal(20, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.FlowControl, knob));
        Assert.Equal(DefaultMQPushConsumer.BrokerFlowControlBackoffMillis,
            DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
                ResponseCode.FlowControl, knob));
        // Any other broker failure still parks on the captured generic backoff.
        Assert.Equal(knob, DefaultMQPushConsumer.PullBackoffMillisForBrokerResponse(
            ResponseCode.SystemError, knob));

        // The three arms are three different numbers: the cadence neither collapses into
        // the early retry nor masquerades as the generic backoff for an idle round.
        Assert.NotEqual(Interval, DefaultMQPushConsumer.BrokerFlowControlBackoffMillis);
        Assert.NotEqual(Interval, knob);
        Assert.Equal(0,
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.NoNewMsg, 0, knob));
    }
}
