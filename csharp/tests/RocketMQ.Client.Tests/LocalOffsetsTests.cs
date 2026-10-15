// Offline unit tests for reading/writing the local offset table (OffsetStore): the golden
// vectors come from real fastjson2 serialisation output.
using RocketMQ.Client;
using RocketMQ.Common;
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
}
