// 本地位点文件格式对齐 Java LocalFileOffsetStore 的回归守卫。
//
// Java 侧 fastjson2 序列化 Map<MessageQueue, AtomicLong> 时把 MessageQueue 对象直接
// 当 JSON key 写出（用真实 jar 跑 OffsetSerializeWrapper.toJson 实测）：
//   {"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9,...}}
// 严格 JSON 非法，但 fastjson2 自产自销能读回。四端按同一格式落盘，Java 消费者
// 与本端消费者共用同一份 ~/.rocketmq_offsets/<clientId>/<group>/offsets.json 时互认。
//
// 与 python/tests/test_local_offsets.py、rust/src/client/consumer.rs（mod tests）
// 的同名测试同题。
using RocketMQ.Client;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class LocalOffsetsTests
{
    // 真实 fastjson2 2.0.59 + rocketmq-client 5.5.0 的输出（含 pretty 版）
    private const string JavaCompact =
        "{\"offsetTable\":{{\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"Tt\"}:9,"
        + "{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7}}";

    private const string JavaPretty =
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
    public void Build_UsesJavaObjectAsKeyFormat()
    {
        string text = DefaultMQPushConsumer.BuildLocalOffsetsJson(
            Items(("Ttbroker-a0", 7), ("Ttbroker-a1", 9)), MqMap());
        Assert.StartsWith("{\"offsetTable\":{", text);
        // 字段序 brokerName/queueId/topic（fastjson2 字母序），每个 key 是内嵌对象
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
    public void Parse_JavaFormatCompactAndPretty()
    {
        Dictionary<string, long>? compact = DefaultMQPushConsumer.ParseLocalOffsetsText(JavaCompact);
        Assert.NotNull(compact);
        Assert.Equal(9L, compact!["Ttbroker-a1"]);
        Assert.Equal(7L, compact["Ttbroker-a0"]);

        Dictionary<string, long>? pretty = DefaultMQPushConsumer.ParseLocalOffsetsText(JavaPretty);
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
    public void Save_WritesJavaFormatAndRollsBak()
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
            // 首次写不产生 .bak（Java string2File 只在已有旧内容时滚动）
            Assert.False(File.Exists(path + ".bak"));

            DefaultMQPushConsumer.SaveLocalOffsetsAt(path, Items(("Ttbroker-a0", 99)), mqMap);
            // .bak = 上一代内容（Java MixAll.string2File 语义）
            Assert.Equal(raw, File.ReadAllText(path + ".bak"));

            // 主文件在 → 读主文件
            Dictionary<string, long> loaded = DefaultMQPushConsumer.LoadLocalOffsetsAt(path);
            Assert.Equal(99L, loaded["Ttbroker-a0"]);

            // 主文件缺失 → .bak（上一代）
            File.Delete(path);
            loaded = DefaultMQPushConsumer.LoadLocalOffsetsAt(path);
            Assert.Equal(7L, loaded["Ttbroker-a0"]);

            // .bak 也缺失 → 空（按首次启动处理）
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
    public void Load_ReadsJavaWrittenFile()
    {
        // Java 写出的文件本端要能读（跨端互认的核心）
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_offsets_java_" + Guid.NewGuid().ToString("N"));
        string path = Path.Combine(dir, "offsets.json");
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(path, JavaCompact);
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
