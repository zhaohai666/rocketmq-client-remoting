// LitePull 消费者的 topic 队列集合变更监听（registerTopicMessageQueueChangeListener）。
//
// 这条能力的契约（逐条钉住，勿凭印象改）：
//  1. 入参守卫：topic 为空或 listener 为 null → MQClientException("Topic or listener is null")；
//  2. 同一 topic 重复注册 → warn 一条并覆盖旧监听器，旧监听器此后不再收到任何回调；
//  3. 快照时机：只有**运行中**注册才立刻记一版当前队列集合；未启动时注册没有快照，
//     于是首轮比对一定回调一次（"当下这套队列"要被认成变化，这是契约不是 bug）；
//  4. 集合相等判定：数量不等或元素不属于旧集都算变化；没快照 ≡ 不相等；
//  5. 变化才回调、回调后快照跟着推进，队列不动时不得反复打扰监听器；
//  6. 单个 topic 取不到队列只跳过它，本轮其余 topic 继续比对（有意偏差：把 catch 收进
//     循环体，免得一个长期无路由的 topic 永久饿死排在它后面的监听器）；
//  7. 取队列每轮现问 nameserver，且空队列集按"查不到"抛错 —— 否则扩容要等满一次路由
//     轮询才看得见，而 nameserver 抖动会被误报成"这个 topic 缩到 0 队列"的假缩容；
//  8. 监听器表的键是**套好命名空间**的全名，回调收到的也是它；重注册/再取队列都不会
//     二次拼接前缀（WrapNamespace 幂等）；
//  9. 检查周期默认 30s、下限 1s（0 会让定时任务直接起不来），后台线程真的在按周期比对。
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

    /// <summary>只记回调，不改状态；后台线程也会调它，所以自带锁。</summary>
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

    /// <summary>一个可读队列数可变的 fake 集群：路由由 <paramref name="queueNums" /> 现算，
    /// 所以测试能在消费者跑起来之后把 topic 扩容/缩容。</summary>
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
                // Ghost 一个队列都不给：等价于「这个 topic 查不到队列」。
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

    // ------------------------------------------------------------ 1. 入参守卫与周期

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
        Eq(30000, consumer.TopicMetadataCheckIntervalMillis, "默认周期");
        consumer.SetTopicMetadataCheckIntervalMillis(0);
        Eq(1000, consumer.TopicMetadataCheckIntervalMillis, "0 会被定时任务拒掉，必须夹到 1s");
        consumer.SetTopicMetadataCheckIntervalMillis(-5);
        Eq(1000, consumer.TopicMetadataCheckIntervalMillis, "负数同样夹到 1s");
        consumer.SetTopicMetadataCheckIntervalMillis(5000);
        Eq(5000, consumer.TopicMetadataCheckIntervalMillis, "合法值原样保留");
    }

    // ---------------------------------------------- 2. 未启动注册：首轮一定回调一次

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
        Eq(0, rec.Count, "未启动时注册不该立刻回调");

        consumer.Start();
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "没有快照 ⇒ 首轮必须回调一次");
        List<(string Topic, List<int> QueueIds)> events = rec.Events();
        Eq(1, events.Count, "回调一次");
        Assert.Equal(new[] { 0, 1 }, events[0].QueueIds.ToArray());
        Assert.Equal(Topic, events[0].Topic);

        Eq(0, consumer.FetchTopicMessageQueuesAndCompare(), "队列没动就不该再打扰");
        Eq(1, rec.Count, "第二轮没有新回调");
        consumer.Shutdown();
    }

    // ------------------------------------ 3. 运行中注册：快照吃掉"当下这套队列"

    [Fact]
    public void RegisterWhileRunning_SnapshotsAndReportsOnlyRealChanges()
    {
        using var f = new Fixture(2);
        DefaultLitePullConsumer consumer = f.Started("qc-running", "GID_lite_qc_running");
        var rec = new Recorder();
        consumer.RegisterTopicMessageQueueChangeListener(Topic, rec);

        Eq(0, consumer.FetchTopicMessageQueuesAndCompare(), "运行中注册已有快照 ⇒ 首轮不回调");
        Eq(0, rec.Count, "不该把当下队列当成变化");

        f.Scale(4);
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "扩容回调一次");
        Assert.Equal(new[] { 0, 1, 2, 3 }, rec.Events()[0].QueueIds.ToArray());

        Eq(0, consumer.FetchTopicMessageQueuesAndCompare(), "同一集合不重复回调");
        Eq(1, rec.Count, "仍然只有一条");

        f.Scale(1);
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "缩容同样要回调");
        Assert.Equal(new[] { 0 }, rec.Events()[1].QueueIds.ToArray());
        consumer.Shutdown();
    }

    // ------------------------------------------- 4. 重复注册覆盖旧监听器

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
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "覆盖后只剩一个监听器要回调");
        Eq(1, second.Count, "新监听器收到回调");
        Eq(0, first.Count, "旧监听器不再被通知");
        consumer.Shutdown();
    }

    // ------------------------------- 5. 一个 topic 查不到队列不打断本轮其余 topic

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
        Eq(1, consumer.FetchTopicMessageQueuesAndCompare(), "坏 topic 不能带走整轮");
        Eq(0, ghost.Count, "查不到队列的 topic 不回调（也不能回调一个空集）");
        Assert.Equal(new[] { 0, 1, 2 }, healthy.Events()[0].QueueIds.ToArray());
        consumer.Shutdown();
    }

    // ------------------------ 6. 每轮现问 nameserver：不读 30s 轮询的缓存，也不二次套前缀

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
            "每轮都要现查路由，否则扩容要等满一次路由轮询才看得见");

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
        Eq(1, events.Count, "配了命名空间也要回调");
        Assert.Equal("ns1%" + Topic, events[0].Topic);

        // 表里的键已是全名，取队列时不能被再套一次（ns1%ns1%Topic 是查不到的路由）。
        Assert.DoesNotContain(f.Cluster.Records()
            .Where(r => r.Code == RequestCode.GetRouteinfoByTopic)
            .Select(r => r.Ext.TryGetValue("topic", out string? t) ? t : ""),
            asked => asked!.StartsWith("ns1%ns1%", StringComparison.Ordinal));
        consumer.Shutdown();
    }

    // ------------------------------------- 7. 后台线程按周期真的在比对

    [Fact]
    public void MetadataLoop_ActuallyComparesOnItsPeriod()
    {
        using var f = new Fixture(2);
        DefaultLitePullConsumer consumer = f.Started("qc-loop", "GID_lite_qc_loop");
        var rec = new Recorder();
        consumer.RegisterTopicMessageQueueChangeListener(Topic, rec);
        f.Scale(3);

        // 首查延迟用参数缩短（默认仍是 10s），周期给 50ms。
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
        Assert.True(rec.Count >= 1, "后台循环没有在比对队列集合");
        Assert.Equal(new[] { 0, 1, 2 }, rec.Events()[0].QueueIds.ToArray());
        consumer.Shutdown();
    }
}
