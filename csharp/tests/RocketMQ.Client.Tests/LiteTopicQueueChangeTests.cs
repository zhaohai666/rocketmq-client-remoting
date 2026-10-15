// The topic queue set change listener of the LitePull consumer
// (registerTopicMessageQueueChangeListener).
//
// The contract of this capability (pinned item by item, do not change it from memory):
//  1. Argument guards: a null topic or listener -> MQClientException("Topic or listener is null");
//  2. Registering the same topic again -> warn once and overwrite the old listener; the
//     old listener never receives another callback afterwards;
//  3. Snapshot timing: only a registration **while running** immediately records a
//     version of the current queue set; registering before startup has no snapshot, so
//     the first comparison always fires a callback ("the queues as they are right now"
//     must be recognised as a change -- that is the contract, not a bug);
//  4. Set equality: a differing count or an element not in the old set both count as a
//     change; having no snapshot is equivalent to not equal;
//  5. Only a change fires the callback, and the snapshot advances after the callback, so
//     an unmoving queue must not keep pestering the listener;
//  6. Failing to get the queues of a single topic only skips that topic; the remaining
//     topics of that round keep being compared (a deliberate deviation: the catch is
//     pulled into the loop body so a topic that never has a route cannot permanently
//     starve the listeners queued behind it);
//  7. The queues are asked from the nameserver afresh each round, and an empty queue set
//     is treated as "not found" and throws -- otherwise a scale-up would only become
//     visible after a full route poll interval, and nameserver jitter would be misreported
//     as a fake scale-down to 0 queues;
//  8. The key of the listener table is the **namespace-wrapped** full name, and that is
//     what the callback receives too; neither re-registering nor fetching the queues
//     prepends the prefix a second time (WrapNamespace is idempotent);
//  9. The check interval defaults to 30s with a lower bound of 1s (0 would make the
//     timer task fail to start at all), and the background thread really does compare
//     on that period.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class LiteTopicQueueChangeTests
{
    private const string Topic = "LiteQueueChangeTopic";
    private const string Ghost = "LiteQueueChangeGhost";

    private static void Eq(long expected, long actual, string because) =>
        Assert.True(actual == expected, $"expected {expected}, actual {actual}, because {because}");

    /// <summary>Only records callbacks, does not change state; the background thread
    /// calls it too, so it brings its own lock.</summary>
    private sealed class Recorder : ITopicMessageQueueChangeListener
    {
        private readonly object _gate = new();
        private readonly List<(string Topic, List<int> QueueIds)> _events = new();

        public void OnChanged(string topic, IReadOnlyList<MessageQueue> messageQueues)
        {
            List<int> ids = messageQueues.Select(mq => mq.QueueId).OrderBy(id => id).ToList();
            lock (_gate)
            {
                _events.Add((topic, ids));
            }
        }

        public List<(string Topic, List<int> QueueIds)> Events()
        {
            lock (_gate)
            {
                return new List<(string, List<int>)>(_events);
            }
        }

        public int Count => Events().Count;
    }

    /// <summary>A fake cluster whose readable queue count can change: the route is
    /// computed on the fly from <paramref name="queueNums" />, so a test can scale the
    /// topic up/down after the consumer has started.</summary>
    private sealed class Fixture : IDisposable
    {
        public readonly MockCluster Cluster;
        private readonly string _broker;
        private int _queueNums;

        public Fixture(int queueNums = 2)
        {
            Cluster = MockCluster.Start(1);
            _broker = MockCluster.BrokerName(0);
            _queueNums = queueNums;
            Cluster.SetRouteBody(req =>
            {
                string asked = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
                int queueNums = _queueNums;
                var route = new TopicRouteData();
                if (!asked.EndsWith(Ghost, StringComparison.Ordinal))
                {
                    route.QueueDatas.Add(new QueueData(_broker, queueNums, queueNums,
                        PermName.PermRead | PermName.PermWrite, 0));
                }
                // A Ghost that gives not a single queue: equivalent to "no queues found for this topic".
                route.BrokerDatas.Add(new BrokerData("MockCluster", _broker,
                    new SortedDictionary<long, string> { { MixAll.MasterId, Cluster.BrokerAddrs[0] } }));
                return route.Encode();
            });
        }

        public void Scale(int queueNums) => _queueNums = queueNums;

        public DefaultLitePullConsumer Started(string instance, string group, string ns = "")
        {
            var consumer = new DefaultLitePullConsumer(group) { Namespace = ns };
            consumer.SetNamesrvAddr(Cluster.NamesrvAddr);
            consumer.SetInstanceName(instance);
            consumer.Subscribe(Topic, "*");
            consumer.Start();
            return consumer;
        }

        public void Dispose() => Cluster.Dispose();
    }

    // ------------------------------------------------------------ 1. Argument guards and the interval

    [Fact]
    public void NullTopicOrNullListener_IsRejected()
    {
        var consumer = new DefaultLitePullConsumer("GID_lite_qc_guard");
        MQClientException ex = Assert.Throws<MQClientException>(
            () => consumer.RegisterTopicMessageQueueChangeListener(null!, new Recorder()));
        Assert.Contains("Topic or listener is null", ex.Message);

        ex = Assert.Throws<MQClientException>(
            () => consumer.RegisterTopicMessageQueueChangeListener("", new Recorder()));
        Assert.Contains("Topic or listener is null", ex.Message);

        ex = Assert.Throws<MQClientException>(
            () => consumer.RegisterTopicMessageQueueChangeListener(Topic, null!));
        Assert.Contains("Topic or listener is null", ex.Message);
    }

    [Fact]
    public void CheckInterval_DefaultsTo30s_AndFloorsToOneSecond()
    {
        var consumer = new DefaultLitePullConsumer("GID_lite_qc_interval");
        Eq(30000, consumer.TopicMetadataCheckIntervalMillis, "the default interval");
        consumer.SetTopicMetadataCheckIntervalMillis(0);
        Eq(1000, consumer.TopicMetadataCheckIntervalMillis, "0 is rejected by the timer task and must be clamped to 1s");
        consumer.SetTopicMetadataCheckIntervalMillis(-5);
        Eq(1000, consumer.TopicMetadataCheckIntervalMillis, "a negative value is clamped to 1s too");
        consumer.SetTopicMetadataCheckIntervalMillis(5000);
        Eq(5000, consumer.TopicMetadataCheckIntervalMillis, "a legal value is kept as is");
    }

    // ---------------------------------------------- 2. Registering while not started: the first round always fires once

    [Fact]
    public void RegisterBeforeStart_HasNoSnapshot_FirstRoundFiresOnce()
    {
        using var f = new Fixture(2);
        var consumer = new DefaultLitePullConsumer("GID_lite_qc_presnapshot")
        {
            Namespace = "",
        };
        consumer.SetNamesrvAddr(f.Cluster.NamesrvAddr);
        consumer.SetInstanceName("qc-presnapshot");
        consumer.Subscribe(Topic, "*");
        var rec = new Recorder();
        consumer.RegisterTopicMessageQueueChangeListener(Topic, rec);
        Eq(0, rec.Count, "registering while not started must not fire immediately");

        consumer.Start();
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "no snapshot => the first round must fire once");
        List<(string Topic, List<int> QueueIds)> events = rec.Events();
        Eq(1, events.Count, "fired once");
        Assert.Equal(new[] { 0, 1 }, events[0].QueueIds.ToArray());
        Assert.Equal(Topic, events[0].Topic);

        Eq(0, consumer.FetchTopicMessageQueuesAndCompare(), "the queues did not move so it must not pester again");
        Eq(1, rec.Count, "no new callback in the second round");
        consumer.Shutdown();
    }

    // ------------------------------------ 3. Registering while running: the snapshot absorbs "the queues as they are right now"

    [Fact]
    public void RegisterWhileRunning_SnapshotsAndReportsOnlyRealChanges()
    {
        using var f = new Fixture(2);
        DefaultLitePullConsumer consumer = f.Started("qc-running", "GID_lite_qc_running");
        var rec = new Recorder();
        consumer.RegisterTopicMessageQueueChangeListener(Topic, rec);

        Eq(0, consumer.FetchTopicMessageQueuesAndCompare(), "registering while running already has a snapshot => the first round does not fire");
        Eq(0, rec.Count, "the current queues must not be treated as a change");

        f.Scale(4);
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "a scale-up fires once");
        Assert.Equal(new[] { 0, 1, 2, 3 }, rec.Events()[0].QueueIds.ToArray());

        Eq(0, consumer.FetchTopicMessageQueuesAndCompare(), "an identical set does not fire again");
        Eq(1, rec.Count, "still only one");

        f.Scale(1);
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "a scale-down must fire as well");
        Assert.Equal(new[] { 0 }, rec.Events()[1].QueueIds.ToArray());
        consumer.Shutdown();
    }

    // ------------------------------------------- 4. Re-registering overwrites the old listener

    [Fact]
    public void ReRegisterOverwritesTheOldListener()
    {
        using var f = new Fixture(2);
        DefaultLitePullConsumer consumer = f.Started("qc-overwrite", "GID_lite_qc_overwrite");
        var first = new Recorder();
        var second = new Recorder();
        consumer.RegisterTopicMessageQueueChangeListener(Topic, first);
        consumer.RegisterTopicMessageQueueChangeListener(Topic, second);

        f.Scale(3);
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "after overwriting only one listener is left to fire");
        Eq(1, second.Count, "the new listener got the callback");
        Eq(0, first.Count, "the old listener is no longer notified");
        consumer.Shutdown();
    }

    // ------------------------------- 5. One topic with no queues does not interrupt the rest of the round

    [Fact]
    public void UnroutableTopic_DoesNotAbortTheRound()
    {
        using var f = new Fixture(2);
        DefaultLitePullConsumer consumer = f.Started("qc-isolate", "GID_lite_qc_isolate");
        var ghost = new Recorder();
        var healthy = new Recorder();
        consumer.RegisterTopicMessageQueueChangeListener(Ghost, ghost);
        consumer.RegisterTopicMessageQueueChangeListener(Topic, healthy);

        f.Scale(3);
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "a bad topic must not take the whole round down");
        Eq(0, ghost.Count, "a topic whose queues cannot be found does not fire (nor does it fire with an empty set)");
        Assert.Equal(new[] { 0, 1, 2 }, healthy.Events()[0].QueueIds.ToArray());
        consumer.Shutdown();
    }

    // ------------------------ 6. Asking the nameserver afresh each round: no reading of the 30s poll cache, and no second prefix

    [Fact]
    public void EachRoundRequeriesTheNameServer_WithTheFullTopicName()
    {
        using var f = new Fixture(2);
        DefaultLitePullConsumer consumer = f.Started("qc-refresh", "GID_lite_qc_refresh");
        consumer.RegisterTopicMessageQueueChangeListener(Topic, new Recorder());

        f.Cluster.ClearRequests();
        consumer.FetchTopicMessageQueuesAndCompare();
        int afterFirst = f.Cluster.CountRequests(RequestCode.GetRouteinfoByTopic);
        consumer.FetchTopicMessageQueuesAndCompare();
        int afterSecond = f.Cluster.CountRequests(RequestCode.GetRouteinfoByTopic);
        Assert.True(afterSecond > afterFirst,
            "the route must be queried afresh each round, otherwise a scale-up only becomes visible after a full route poll interval");

        consumer.Shutdown();
    }

    [Fact]
    public void ListenerKeyIsNamespaced_AndIsNotWrappedTwice()
    {
        using var f = new Fixture(2);
        DefaultLitePullConsumer consumer = f.Started("qc-ns", "GID_lite_qc_ns", "ns1");
        var rec = new Recorder();
        consumer.RegisterTopicMessageQueueChangeListener(Topic, rec);

        f.Scale(3);
        consumer.FetchTopicMessageQueuesAndCompare();
        List<(string Topic, List<int> QueueIds)> events = rec.Events();
        Eq(1, events.Count, "it must fire even with a namespace configured");
        Assert.Equal("ns1%" + Topic, events[0].Topic);

        // The key in the table is already the full name; fetching the queues must not wrap it again (ns1%ns1%Topic is a route that cannot be found).
        Assert.DoesNotContain(f.Cluster.Records()
            .Where(r => r.Code == RequestCode.GetRouteinfoByTopic)
            .Select(r => r.Ext.TryGetValue("topic", out string? t) ? t : ""),
            asked => asked!.StartsWith("ns1%ns1%", StringComparison.Ordinal));
        consumer.Shutdown();
    }

    // ------------------------------------- 7. The background thread really compares on the interval

    [Fact]
    public void MetadataLoop_ActuallyComparesOnItsPeriod()
    {
        using var f = new Fixture(2);
        DefaultLitePullConsumer consumer = f.Started("qc-loop", "GID_lite_qc_loop");
        var rec = new Recorder();
        consumer.RegisterTopicMessageQueueChangeListener(Topic, rec);
        f.Scale(3);

        // The first-lookup delay is shortened via the parameter (10s by default), the interval is 50ms.
        var loop = new Thread(() => consumer.MetadataLoop(20, 50))
        {
            IsBackground = true,
        };
        loop.Start();
        DateTime deadline = DateTime.UtcNow.AddSeconds(3);
        while (rec.Count == 0 && DateTime.UtcNow < deadline)
        {
            Thread.Sleep(20);
        }

        loop.Join(2000);
        Assert.True(rec.Count >= 1, "the background loop is not comparing the queue sets");
        Assert.Equal(new[] { 0, 1, 2 }, rec.Events()[0].QueueIds.ToArray());
        consumer.Shutdown();
    }
}
