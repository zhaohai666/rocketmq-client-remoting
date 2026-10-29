// Offline unit tests for reading/writing the local offset table (OffsetStore): the golden
// vectors come from real fastjson2 serialisation output.
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class LocalOffsetsTests
{
    // the output of real fastjson2 2.0.59 + rocketmq-client 5.5.0 (including the pretty version)
    private const string LegacyCompact =
        "{\"offsetTable\":{{\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"Tt\"}:9,"
        + "{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7}}";

    private const string LegacyPretty =
        "{\n\t\"offsetTable\":{\n\t\t{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7\n\t}\n}";

    private static Dictionary<string, MessageQueue> MqMap()
    {
        var map = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        for (int i = 0; i < 3; i++)
        {
            var mq = new MessageQueue("Tt", "broker-a", i);
            map[mq.Topic + mq.BrokerName + mq.QueueId] = mq;
        }

        return map;
    }

    private static Dictionary<string, long> Items(params (string Key, long Off)[] pairs)
    {
        var dict = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach ((string key, long off) in pairs)
        {
            dict[key] = off;
        }

        return dict;
    }

    [Fact]
    public void Build_UsesLegacyObjectAsKeyFormat()
    {
        string text = DefaultMQPushConsumer.BuildLocalOffsetsJson(
            Items(("Ttbroker-a0", 7), ("Ttbroker-a1", 9)), MqMap());
        Assert.StartsWith("{\"offsetTable\":{", text);
        // field order brokerName/queueId/topic (fastjson2 alphabetical), each key is an embedded object
        Assert.Contains("\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7", text);
        Assert.Contains("\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"Tt\"}:9", text);
    }

    [Fact]
    public void Build_SkipsEntriesWithoutQueueInfo()
    {
        string text = DefaultMQPushConsumer.BuildLocalOffsetsJson(
            Items(("Ttbroker-a0", 7), ("Orphan_b0", 3)), MqMap());
        Assert.DoesNotContain("Orphan", text);
    }

    [Fact]
    public void Parse_LegacyFormatCompactAndPretty()
    {
        Dictionary<string, long>? compact = DefaultMQPushConsumer.ParseLocalOffsetsText(LegacyCompact);
        Assert.NotNull(compact);
        Assert.Equal(9L, compact!["Ttbroker-a1"]);
        Assert.Equal(7L, compact["Ttbroker-a0"]);

        Dictionary<string, long>? pretty = DefaultMQPushConsumer.ParseLocalOffsetsText(LegacyPretty);
        Assert.NotNull(pretty);
        Assert.Equal(7L, pretty!["Ttbroker-a0"]);
    }

    [Fact]
    public void Parse_AcceptsLegacyFlatMap()
    {
        Dictionary<string, long>? flat =
            DefaultMQPushConsumer.ParseLocalOffsetsText("{\"Ttbroker-a0\":5}");
        Assert.NotNull(flat);
        Assert.Equal(5L, flat!["Ttbroker-a0"]);
    }

    [Fact]
    public void Parse_RejectsGarbage()
    {
        Assert.Null(DefaultMQPushConsumer.ParseLocalOffsetsText("{\"offsetTable\":{{{"));
        Assert.Null(DefaultMQPushConsumer.ParseLocalOffsetsText("{\"offsetTable\":"));
        Assert.Null(DefaultMQPushConsumer.ParseLocalOffsetsText(""));
    }

    [Fact]
    public void RoundTrip_BuiltText()
    {
        string text = DefaultMQPushConsumer.BuildLocalOffsetsJson(
            Items(("Ttbroker-a0", 7), ("Ttbroker-a1", 9), ("Ttbroker-a2", 11)), MqMap());
        Dictionary<string, long>? parsed = DefaultMQPushConsumer.ParseLocalOffsetsText(text);
        Assert.NotNull(parsed);
        Assert.Equal(7L, parsed!["Ttbroker-a0"]);
        Assert.Equal(9L, parsed["Ttbroker-a1"]);
        Assert.Equal(11L, parsed["Ttbroker-a2"]);
    }

    [Fact]
    public void Save_WritesLegacyFormatAndRollsBak()
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_offsets_" + Guid.NewGuid().ToString("N"));
        string path = Path.Combine(dir, "offsets.json");
        try
        {
            var items = Items(("Ttbroker-a0", 7), ("Ttbroker-a1", 9), ("Ttbroker-a2", 11));
            Dictionary<string, MessageQueue> mqMap = MqMap();

            DefaultMQPushConsumer.SaveLocalOffsetsAt(path, items, mqMap);
            string raw = File.ReadAllText(path);
            Assert.StartsWith("{\"offsetTable\":{", raw);
            Assert.False(File.Exists(path + ".bak"));

            DefaultMQPushConsumer.SaveLocalOffsetsAt(path, Items(("Ttbroker-a0", 99)), mqMap);
            Assert.Equal(raw, File.ReadAllText(path + ".bak"));

            // the main file is there -> read the main file
            Dictionary<string, long> loaded = DefaultMQPushConsumer.LoadLocalOffsetsAt(path);
            Assert.Equal(99L, loaded["Ttbroker-a0"]);

            // the main file is missing -> .bak (the previous generation)
            File.Delete(path);
            loaded = DefaultMQPushConsumer.LoadLocalOffsetsAt(path);
            Assert.Equal(7L, loaded["Ttbroker-a0"]);

            // .bak is missing too -> empty (treated as a first start)
            File.Delete(path + ".bak");
            Assert.Empty(DefaultMQPushConsumer.LoadLocalOffsetsAt(path));
        }
        finally
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, true);
            }
        }
    }

    [Fact]
    public void Load_ReadsLegacyWrittenFile()
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_offsets_wire_" + Guid.NewGuid().ToString("N"));
        string path = Path.Combine(dir, "offsets.json");
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(path, LegacyCompact);
            Dictionary<string, long> loaded = DefaultMQPushConsumer.LoadLocalOffsetsAt(path);
            Assert.Equal(9L, loaded["Ttbroker-a1"]);
            Assert.Equal(7L, loaded["Ttbroker-a0"]);
        }
        finally
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, true);
            }
        }
    }

    // ------------------------------------------------- message-model offset store

    private static string NewOffsetPath(string tag)
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_offsets_" + tag + "_" + Guid.NewGuid().ToString("N"));
        return Path.Combine(dir, "offsets.json");
    }

    private static void DeleteOffsetFile(string path)
    {
        string? dir = Path.GetDirectoryName(path);
        if (dir is not null && Directory.Exists(dir))
        {
            Directory.Delete(dir, true);
        }
    }

    private static bool WaitFor(Func<bool> cond, int timeoutMs = 15000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    [Fact]
    public void PullConsumer_Broadcasting_RoutesOffsetsToTheLocalFile()
    {
        string path = NewOffsetPath("pull_bc");
        using var cluster = MockCluster.Start(1);
        var mq = new MessageQueue("PullModelTopic", MockCluster.BrokerName(0), 0);
        var c = new DefaultMQPullConsumer("GID_PullModelBroadcast");
        c.SetMessageModel(MessageModel.Broadcasting);
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("pull-model-bc");
        c.LocalOffsetPathForTest = path;
        try
        {
            c.Start();
            c.UpdateConsumeOffset(mq, 11);

            Assert.True(c.FetchConsumeOffset(mq, out long fetched));
            Assert.Equal(11L, fetched);
            Assert.Equal(11L, DefaultMQPushConsumer.LoadLocalOffsetsAt(path)[
                DefaultMQPushConsumer.LocalOffsetKey(mq)]);
            EqCount(0, cluster.CountRequests(RequestCode.UpdateConsumerOffset),
                "broadcasting must not commit the offset to a broker");
            EqCount(0, cluster.CountRequests(RequestCode.QueryConsumerOffset),
                "broadcasting must not read the offset from a broker");
        }
        finally
        {
            c.Shutdown();
            DeleteOffsetFile(path);
        }
    }

    [Fact]
    public void PullConsumer_Clustering_UsesTheBrokerOffsetStore()
    {
        string path = NewOffsetPath("pull_cl");
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyExt(RequestCode.QueryConsumerOffset,
            _ => new Dictionary<string, string> { ["offset"] = "5" });
        var mq = new MessageQueue("PullModelTopic", MockCluster.BrokerName(0), 0);
        var c = new DefaultMQPullConsumer("GID_PullModelClustering");
        c.SetMessageModel(MessageModel.Clustering);
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("pull-model-cl");
        c.RegisterMessageQueueListener(mq.Topic, null);
        c.LocalOffsetPathForTest = path;
        try
        {
            c.Start();
            c.UpdateConsumeOffset(mq, 9);

            Assert.True(c.FetchConsumeOffset(mq, out long fetched));
            Assert.Equal(5L, fetched);
            EqCount(1, cluster.CountRequests(RequestCode.UpdateConsumerOffset),
                "clustering commits the offset to the broker");
            EqCount(1, cluster.CountRequests(RequestCode.QueryConsumerOffset),
                "clustering reads the offset from the broker");
            Assert.False(File.Exists(path));
        }
        finally
        {
            c.Shutdown();
            DeleteOffsetFile(path);
        }
    }

    [Fact]
    public void LitePull_Broadcasting_CommitKeepsExistingEntriesAndSkipsTheBroker()
    {
        string path = NewOffsetPath("lite_bc");
        var mq = new MessageQueue("LiteModelTopic", MockCluster.BrokerName(0), 0);
        var previous = new MessageQueue("LitePreviousTopic", MockCluster.BrokerName(0), 3);
        DefaultMQPushConsumer.SaveLocalOffsetsAt(path,
            new Dictionary<string, long> { [DefaultMQPushConsumer.LocalOffsetKey(previous)] = 5 },
            new Dictionary<string, MessageQueue>
            {
                [DefaultMQPushConsumer.LocalOffsetKey(previous)] = previous,
            });

        using var cluster = MockCluster.Start(1);
        var c = new DefaultLitePullConsumer("GID_LiteModelBroadcast");
        c.SetMessageModel(MessageModel.Broadcasting);
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-model-bc");
        c.SetAutoCommit(false);
        c.LocalOffsetPathForTest = path;
        c.Assign(new[] { mq });
        try
        {
            c.Start();

            // Not in the local file yet: the broker would answer the default 0 here.
            Assert.Equal(-1L, c.Committed(mq));
            EqCount(0, cluster.CountRequests(RequestCode.QueryConsumerOffset),
                "broadcasting reads the commit from the local file");

            c.Commit(new Dictionary<MessageQueue, long> { [mq] = 9 });

            Dictionary<string, long> table = DefaultMQPushConsumer.LoadLocalOffsetsAt(path);
            Assert.Equal(9L, table[DefaultMQPushConsumer.LocalOffsetKey(mq)]);
            Assert.Equal(5L, table[DefaultMQPushConsumer.LocalOffsetKey(previous)]);
            EqCount(0, cluster.CountRequests(RequestCode.UpdateConsumerOffset),
                "broadcasting never commits to a broker");
        }
        finally
        {
            c.Shutdown();
            DeleteOffsetFile(path);
        }
    }

    [Fact]
    public void LitePull_Broadcasting_RevokePersistLandsInTheLocalFile()
    {
        string path = NewOffsetPath("lite_revoke");
        using var cluster = MockCluster.Start(1);
        string topic = "LiteRevokeModelTopic";
        var mq = new MessageQueue(topic, MockCluster.BrokerName(0), 0);
        var c = new DefaultLitePullConsumer("GID_LiteModelRevoke");
        c.SetMessageModel(MessageModel.Broadcasting);
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-model-revoke");
        c.SetAutoCommit(false);
        c.PollNameServerIntervalMillis = 200;
        c.LocalOffsetPathForTest = path;
        c.Subscribe(topic, "*");
        try
        {
            c.Start();
            Assert.True(WaitFor(() => c.PullCursorOf(mq) >= 0), "the first rebalance assigns the queue");
            c.Commit(new Dictionary<MessageQueue, long> { [mq] = 7 }, persist: false);

            cluster.SetRouteBody(_ => new TopicRouteData().Encode());
            Assert.True(WaitFor(() => c.PendingCommitOf(mq) == -1),
                "the rebalance revokes the queue once its route disappears");

            Assert.Equal(7L, DefaultMQPushConsumer.LoadLocalOffsetsAt(path)[
                DefaultMQPushConsumer.LocalOffsetKey(mq)]);
            EqCount(0, cluster.CountRequests(RequestCode.UpdateConsumerOffset),
                "the revoke-path persist is local in broadcasting");
        }
        finally
        {
            c.Shutdown();
            DeleteOffsetFile(path);
        }
    }

    private static void EqCount(int expected, int actual, string because) =>
        Assert.True(actual == expected, $"expected {expected}, actual {actual}, because {because}");
}
