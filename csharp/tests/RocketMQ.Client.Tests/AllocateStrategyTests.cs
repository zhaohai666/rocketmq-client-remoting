// 队列分配策略单测（对应 Java client/test 的 AllocateMessageQueue*Test）。
//
// 与 rust/src/client/allocate_strategy.rs 的 AVERAGELY_CASES / CIRCLE_CASES、
// cpp/tests/test_allocate_strategy.cpp、python/tests/test_allocate_strategy.py 同一张表，
// 四语言逐格对拍。
using System;
using System.Collections.Generic;
using System.Linq;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class AllocateStrategyTests
{
    // Java 测试里的 createMessageQueueList(size)：new MessageQueue("topic", "brokerName", i)
    private static List<MessageQueue> QueuesOf(int size) =>
        Enumerable.Range(0, size).Select(i => new MessageQueue("topic", "brokerName", i)).ToList();

    // Java 测试里的 createConsumerIdList(size)："CID_PREFIX" + i
    private static List<string> CidsOf(int size) =>
        Enumerable.Range(0, size).Select(i => "CID_PREFIX" + i).ToList();

    private static List<int> Ids(List<MessageQueue> mqs) => mqs.Select(m => m.QueueId).ToList();

    public static IEnumerable<object[]> AveragelyCases()
    {
        // (队列数, 消费者数, 逐个消费者期望的 queueId；与 Java 10×4 → {3,3,2,2} 同表)
        yield return new object[] { 10, 4, new[] { new[] { 0, 1, 2 }, new[] { 3, 4, 5 }, new[] { 6, 7 }, new[] { 8, 9 } } };
        yield return new object[] { 8, 3, new[] { new[] { 0, 1, 2 }, new[] { 3, 4, 5 }, new[] { 6, 7 } } };
        yield return new object[] { 9, 3, new[] { new[] { 0, 1, 2 }, new[] { 3, 4, 5 }, new[] { 6, 7, 8 } } };
        yield return new object[] { 4, 4, new[] { new[] { 0 }, new[] { 1 }, new[] { 2 }, new[] { 3 } } };
        // 队列比消费者少：只有前 modulo 个各 1 条，其余空
        yield return new object[] { 2, 4, new[] { new[] { 0 }, new[] { 1 }, Array.Empty<int>(), Array.Empty<int>() } };
        yield return new object[] { 1, 3, new[] { new[] { 0 }, Array.Empty<int>(), Array.Empty<int>() } };
        yield return new object[] { 3, 1, new[] { new[] { 0, 1, 2 } } };
        // 空 mqAll（守卫）
        yield return new object[] { 0, 2, new[] { Array.Empty<int>(), Array.Empty<int>() } };
    }

    public static IEnumerable<object[]> CircleCases()
    {
        // Java AllocateMessageQueueAveragelyByCircleTest：10 / 4 → {0,4,8} {1,5,9} {2,6} {3,7}
        yield return new object[] { 10, 4, new[] { new[] { 0, 4, 8 }, new[] { 1, 5, 9 }, new[] { 2, 6 }, new[] { 3, 7 } } };
        yield return new object[] { 8, 3, new[] { new[] { 0, 3, 6 }, new[] { 1, 4, 7 }, new[] { 2, 5 } } };
        yield return new object[] { 9, 3, new[] { new[] { 0, 3, 6 }, new[] { 1, 4, 7 }, new[] { 2, 5, 8 } } };
        yield return new object[] { 4, 4, new[] { new[] { 0 }, new[] { 1 }, new[] { 2 }, new[] { 3 } } };
        yield return new object[] { 2, 4, new[] { new[] { 0 }, new[] { 1 }, Array.Empty<int>(), Array.Empty<int>() } };
        yield return new object[] { 1, 3, new[] { new[] { 0 }, Array.Empty<int>(), Array.Empty<int>() } };
        yield return new object[] { 3, 1, new[] { new[] { 0, 1, 2 } } };
        yield return new object[] { 0, 2, new[] { Array.Empty<int>(), Array.Empty<int>() } };
    }

    [Theory]
    [MemberData(nameof(AveragelyCases))]
    public void Averagely_partitions_into_contiguous_ranges(int mqSize, int cidSize, int[][] expect)
    {
        RunTable(new AllocateMessageQueueAveragely(), mqSize, cidSize, expect);
    }

    [Theory]
    [MemberData(nameof(CircleCases))]
    public void Circle_partitions_by_stride(int mqSize, int cidSize, int[][] expect)
    {
        RunTable(new AllocateMessageQueueAveragelyByCircle(), mqSize, cidSize, expect);
    }

    private static void RunTable(IAllocateMessageQueueStrategy strategy, int mqSize, int cidSize,
        int[][] expect)
    {
        List<MessageQueue> mqAll = QueuesOf(mqSize);
        List<string> cidAll = CidsOf(cidSize);
        Assert.Equal(cidSize, expect.Length);
        for (int i = 0; i < cidSize; i++)
        {
            List<int> got = Ids(strategy.Allocate("ConsumerGroupTest", cidAll[i], mqAll, cidAll));
            Assert.True(expect[i].SequenceEqual(got),
                $"{strategy.GetName()} mq={mqSize} cid={cidSize} index={i} → [{string.Join(",", got)}]");
        }
    }

    /// Java AllocateMessageQueueAveragelyTest 原样对拍：只断言 size。
    [Fact]
    public void Averagely_matches_java_unit_test_sizes()
    {
        List<MessageQueue> mqAll = QueuesOf(10);
        List<string> cidAll = CidsOf(4);
        var strategy = new AllocateMessageQueueAveragely();
        Assert.Equal(new[] { 3, 3, 2, 2 },
            cidAll.Select(cid => strategy.Allocate("G", cid, mqAll, cidAll).Count).ToArray());
    }

    /// Java AllocateMessageQueueAveragelyByCircleTest 第一段断言：currentCID 不在 cidAll → 空。
    [Fact]
    public void Circle_returns_nothing_when_cid_not_in_list()
    {
        Assert.Empty(new AllocateMessageQueueAveragelyByCircle()
            .Allocate("G", "CID_PREFIX", QueuesOf(10), CidsOf(4)));
    }

    /// 守卫：与 Python/Rust 一致返回空列表，而不是像 Java check() 那样抛 IllegalArgumentException。
    [Theory]
    [InlineData("", 4, 2)]              // 空 currentCid
    [InlineData("CID_PREFIX0", 0, 2)]   // 空 mqAll
    [InlineData("CID_PREFIX0", 4, 0)]   // 空 cidAll
    [InlineData("CID_NOT_IN_LIST", 4, 2)]
    public void Guards_return_empty_result(string currentCid, int mqSize, int cidSize)
    {
        List<MessageQueue> mqAll = QueuesOf(mqSize);
        List<string> cidAll = CidsOf(cidSize);
        Assert.Empty(new AllocateMessageQueueAveragely().Allocate("G", currentCid, mqAll, cidAll));
        Assert.Empty(new AllocateMessageQueueAveragelyByCircle().Allocate("G", currentCid, mqAll, cidAll));
    }

    /// Java AllocateMessageQueueByConfigTest：配 4 个队列，2 个消费者都拿到 [0,1,2,3]。
    [Fact]
    public void ByConfig_matches_java_unit_test()
    {
        List<MessageQueue> mqAll = QueuesOf(4);
        List<string> cidAll = CidsOf(2);
        var strategy = new AllocateMessageQueueByConfig();
        strategy.SetMessageQueueList(mqAll);
        foreach (string cid in cidAll)
        {
            Assert.Equal(new[] { 0, 1, 2, 3 }, Ids(strategy.Allocate("G", cid, mqAll, cidAll)));
        }

        Assert.Equal(new[] { 0, 1, 2, 3 }, Ids(strategy.GetMessageQueueList()));
    }

    /// Java / Python 的 ByConfig.allocate 都不调 check：守卫场景照样返回配置值。
    [Fact]
    public void ByConfig_ignores_all_guards()
    {
        var strategy = new AllocateMessageQueueByConfig(QueuesOf(2));
        var empty = new List<string>();
        Assert.Equal(new[] { 0, 1 }, Ids(strategy.Allocate("G", "", QueuesOf(0), empty)));
        Assert.Equal(new[] { 0, 1 }, Ids(strategy.Allocate("G", "anyCID", QueuesOf(5), empty)));
    }

    /// 未配置 = 空列表（Python/Rust 口径），且 allocate 给的是副本。
    [Fact]
    public void ByConfig_defaults_empty_and_returns_copy()
    {
        var strategy = new AllocateMessageQueueByConfig();
        Assert.Empty(strategy.GetMessageQueueList());
        List<MessageQueue> before = strategy.Allocate("G", "CID0", new List<MessageQueue>(), new List<string>());
        strategy.SetMessageQueueList(QueuesOf(3));
        Assert.Empty(before);
        Assert.Equal(new[] { 0, 1, 2 },
            Ids(strategy.Allocate("G", "CID0", new List<MessageQueue>(), new List<string>())));
    }

    /// Java getName() 的字面值（六个策略齐全）。
    [Fact]
    public void Names_match_java()
    {
        IAllocateMessageQueueStrategy[] strategies =
        {
            new AllocateMessageQueueAveragely(),
            new AllocateMessageQueueAveragelyByCircle(),
            new AllocateMessageQueueByConfig(),
            new AllocateMessageQueueConsistentHash(),
            new AllocateMessageQueueByMachineRoom(),
            new AllocateMachineRoomNearby(new AllocateMessageQueueConsistentHash(), new DashRoom()),
        };
        Assert.Equal(new[] { "AVG", "AVG_BY_CIRCLE", "CONFIG", "CONSISTENT_HASH", "MACHINE_ROOM",
                             "MACHINE_ROOM_NEARBY-CONSISTENT_HASH" },
            strategies.Select(s => s.GetName()).ToArray());
    }

    /// 全覆盖 + 不重叠：任意 (队列数, 消费者数) 组合下，所有消费者的并集恰为 mqAll。
    [Fact]
    public void Partition_covers_every_queue_exactly_once()
    {
        IAllocateMessageQueueStrategy[] strategies =
        {
            new AllocateMessageQueueAveragely(),
            new AllocateMessageQueueAveragelyByCircle(),
        };
        for (int mqSize = 0; mqSize <= 7; mqSize++)
        {
            for (int cidSize = 1; cidSize <= 5; cidSize++)
            {
                List<MessageQueue> mqAll = QueuesOf(mqSize);
                List<string> cidAll = CidsOf(cidSize);
                foreach (IAllocateMessageQueueStrategy s in strategies)
                {
                    List<int> all = cidAll
                        .SelectMany(cid => Ids(s.Allocate("G", cid, mqAll, cidAll)))
                        .ToList();
                    Assert.Equal(mqSize, all.Count);
                    Assert.Equal(mqSize, all.Distinct().Count());
                }
            }
        }
    }

    /// 逐值对拍 Python `AllocateMessageQueueAveragely.allocate`（consumer.py:188-214）与
    /// `AllocateMessageQueueAveragelyByCircle.allocate`（consumer.py:223-236）的算式。
    [Fact]
    public void Matches_python_reference_arithmetic()
    {
        for (int mqSize = 0; mqSize <= 12; mqSize++)
        {
            for (int cidSize = 0; cidSize <= 5; cidSize++)
            {
                List<MessageQueue> mqAll = QueuesOf(mqSize);
                List<string> cidAll = CidsOf(cidSize);
                List<int> ids = Enumerable.Range(0, mqSize).ToList();
                List<string> probes = new List<string>(cidAll) { "CID_NOT_IN_LIST", "" };
                foreach (string cid in probes)
                {
                    Assert.Equal(PythonAveragely(ids, cidAll, cid),
                        Ids(new AllocateMessageQueueAveragely().Allocate("G", cid, mqAll, cidAll)));
                    Assert.Equal(PythonCircle(ids, cidAll, cid),
                        Ids(new AllocateMessageQueueAveragelyByCircle().Allocate("G", cid, mqAll, cidAll)));
                }
            }
        }
    }

    // 直译 consumer.py:188-214，不参与实现，只用于差分测试
    private static List<int> PythonAveragely(List<int> mqAll, List<string> cidAll, string currentCid)
    {
        if (mqAll.Count == 0) return new List<int>();
        int index = cidAll.FindIndex(cid => string.Equals(cid, currentCid, StringComparison.Ordinal));
        if (cidAll.Count == 0 || index < 0) return new List<int>();
        int mod = mqAll.Count % cidAll.Count;
        int averageSize = mqAll.Count / cidAll.Count;
        if (averageSize == 0) return index < mqAll.Count ? new List<int> { mqAll[index] } : new List<int>();
        int startIndex, endIndex;
        if (mod > 0 && index < mod)
        {
            startIndex = index * (averageSize + 1);
            endIndex = startIndex + averageSize + 1;
        }
        else
        {
            startIndex = mod * (averageSize + 1) + (index - mod) * averageSize;
            endIndex = startIndex + averageSize;
        }

        endIndex = Math.Min(endIndex, mqAll.Count);
        if (startIndex >= endIndex) return new List<int>();
        return mqAll.GetRange(startIndex, endIndex - startIndex);
    }

    // 直译 consumer.py:223-236
    private static List<int> PythonCircle(List<int> mqAll, List<string> cidAll, string currentCid)
    {
        var result = new List<int>();
        if (mqAll.Count == 0) return result;
        int index = cidAll.FindIndex(cid => string.Equals(cid, currentCid, StringComparison.Ordinal));
        if (cidAll.Count == 0 || index < 0) return result;
        for (int i = index; i < mqAll.Count; i += cidAll.Count) result.Add(mqAll[i]);
        return result;
    }

    /// 真实队列（同 topic 挂多个 brokerName）也一律按下标切分。
    [Fact]
    public void Allocate_real_queues_from_multiple_brokers()
    {
        (string Broker, int QueueId)[] layout =
        {
            ("broker-a", 0), ("broker-a", 1), ("broker-b", 0), ("broker-b", 1), ("broker-c", 0),
        };
        List<MessageQueue> mqAll = layout.Select(q => new MessageQueue("TopicTest", q.Broker, q.QueueId)).ToList();
        List<string> cidAll = CidsOf(2);
        var strategy = new AllocateMessageQueueAveragely();
        List<MessageQueue> first = strategy.Allocate("G", cidAll[0], mqAll, cidAll);
        List<MessageQueue> second = strategy.Allocate("G", cidAll[1], mqAll, cidAll);
        Assert.Equal(3, first.Count);
        Assert.Equal(2, second.Count);
        Assert.Equal("broker-b", first[2].BrokerName);
        Assert.Equal(0, first[2].QueueId);
        Assert.Equal("broker-b", second[0].BrokerName);
        Assert.Equal(1, second[0].QueueId);
    }

    /// 消费者默认策略 = AVG，可替换；置 null 由 Start() 拒绝（Java checkConfig）。
    [Fact]
    public void Consumers_expose_strategy()
    {
        var push = new DefaultMQPushConsumer("G_test");
        Assert.Equal("AVG", push.AllocateMessageQueueStrategy!.GetName());
        push.AllocateMessageQueueStrategy = new AllocateMessageQueueAveragelyByCircle();
        Assert.Equal("AVG_BY_CIRCLE", push.AllocateMessageQueueStrategy.GetName());
        push.AllocateMessageQueueStrategy = null;
        Assert.Null(push.AllocateMessageQueueStrategy);
        // 地址给一个不可达的假值即可：checkConfig 全部是本地校验，排在建连之前。
        push.SetNamesrvAddr("127.0.0.1:1");
        MQClientException pushException = Assert.Throws<MQClientException>(() => push.Start());
        Assert.Contains("allocateMessageQueueStrategy is null", pushException.Message);

        var lite = new DefaultLitePullConsumer("G_test");
        Assert.Equal("AVG", lite.AllocateMessageQueueStrategy!.GetName());
        var byConfig = new AllocateMessageQueueByConfig(QueuesOf(2));
        lite.SetAllocateMessageQueueStrategy(byConfig);
        Assert.Equal("CONFIG", lite.AllocateMessageQueueStrategy.GetName());
        Assert.Equal(new[] { 0, 1 }, Ids(lite.AllocateMessageQueueStrategy.Allocate("G", "whoever",
            QueuesOf(9), CidsOf(3))));
        lite.SetAllocateMessageQueueStrategy(null);
        lite.SetNamesrvAddr("127.0.0.1:1");
        lite.Subscribe("TopicTest", "*");
        MQClientException e = Assert.Throws<MQClientException>(() => lite.Start());
        Assert.Contains("allocateMessageQueueStrategy is null", e.Message);

        // 拉模式消费者同样带这份配置（Java DefaultMQPullConsumer:89 字段 + :196-202 读写口，
        // checkConfig:803 拒绝 null）；本端口拉模式不做 rebalance，所以它是配置面。
        var pull = new DefaultMQPullConsumer("G_test");
        Assert.Equal("AVG", pull.AllocateMessageQueueStrategy!.GetName());
        pull.SetAllocateMessageQueueStrategy(new AllocateMessageQueueByConfig(QueuesOf(2)));
        Assert.Equal("CONFIG", pull.AllocateMessageQueueStrategy.GetName());
        pull.SetAllocateMessageQueueStrategy(null);
        Assert.Null(pull.AllocateMessageQueueStrategy);
        pull.SetNamesrvAddr("127.0.0.1:1");
        MQClientException pullException = Assert.Throws<MQClientException>(() => pull.Start());
        Assert.Contains("allocateMessageQueueStrategy is null", pullException.Message);
    }

    // ============================================================ CONSISTENT_HASH

    // Java `AllocateMessageQueueConsitentHashTest` 的 createConsumerIdList：`CID-i`。
    // 与 CidsOf() 的 "CID_PREFIX i" **刻意不同名** —— clientId 参与哈希，改一个字符整张表就废了。
    private static List<string> ChCids(int size) =>
        Enumerable.Range(0, size).Select(i => "CID-" + i).ToList();

    private static List<int> ChAllocateIds(IAllocateMessageQueueStrategy strategy, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll) =>
        Ids(strategy.Allocate("testConsumerGroup", currentCid, mqAll, cidAll));

    public static IEnumerable<object[]> ConsistentHashCases()
    {
        // 落点表（virtualNodeCnt = 3）。期望值来自**真实 Java 5.5.1 客户端**，与 Rust /
        // Python / C++ 的表逐值相同：四语言 + Java 必须算出同一个环，否则混跑时全体队列换主
        // （重复 / 漏消费），而不是"分得稍有不均"。
        yield return new object[] { 6, 2, new[] { new[] { 2 }, new[] { 0, 1, 3, 4, 5 } } };
        yield return new object[] { 6, 3, new[] { new[] { 2 }, new[] { 1, 5 }, new[] { 0, 3, 4 } } };
        yield return new object[]
        {
            10, 4,
            new[] { new[] { 2 }, Array.Empty<int>(), new[] { 0, 3, 4, 8 }, new[] { 1, 5, 6, 7, 9 } }
        };
        yield return new object[]
        {
            20, 10,
            new[]
            {
                new[] { 2, 14, 15 }, new[] { 17 }, new[] { 8, 11 }, Array.Empty<int>(),
                Array.Empty<int>(), new[] { 1, 5, 9, 10 }, new[] { 6, 7, 12 },
                new[] { 13, 18 }, new[] { 16 }, new[] { 0, 3, 4, 19 }
            }
        };
    }

    [Theory]
    [MemberData(nameof(ConsistentHashCases))]
    public void Consistent_hash_matches_java_ring_table(int mqSize, int cidSize, int[][] expect)
    {
        RunConsistentHashTable(new AllocateMessageQueueConsistentHash(3), mqSize, cidSize, expect);
    }

    public static IEnumerable<object[]> ConsistentHashDefaultVcCases()
    {
        // 同一算法、默认 virtualNodeCnt = 10（Java 无参构造）
        yield return new object[] { 4, 2, new[] { new[] { 0, 2 }, new[] { 1, 3 } } };
        yield return new object[] { 8, 3, new[] { new[] { 0, 2, 4 }, new[] { 3, 5, 6, 7 }, new[] { 1 } } };
    }

    [Theory]
    [MemberData(nameof(ConsistentHashDefaultVcCases))]
    public void Consistent_hash_with_default_virtual_node_count_matches_java(int mqSize, int cidSize,
        int[][] expect)
    {
        RunConsistentHashTable(new AllocateMessageQueueConsistentHash(), mqSize, cidSize, expect);
    }

    private static void RunConsistentHashTable(IAllocateMessageQueueStrategy strategy, int mqSize,
        int cidSize, int[][] expect)
    {
        List<MessageQueue> mqAll = QueuesOf(mqSize);  // ("topic","brokerName",i)，toString 参与哈希
        List<string> cidAll = ChCids(cidSize);
        Assert.Equal(cidSize, expect.Length);
        for (int i = 0; i < cidSize; i++)
        {
            List<int> got = ChAllocateIds(strategy, cidAll[i], mqAll, cidAll);
            Assert.True(expect[i].SequenceEqual(got),
                $"{strategy.GetName()} mq={mqSize} cid={cidSize} index={i} → [{string.Join(",", got)}]");
        }
    }

    [Fact]
    public void Consistent_hash_exposes_virtual_node_count()
    {
        Assert.Equal(3, new AllocateMessageQueueConsistentHash(3).VirtualNodeCnt);
        Assert.Equal(10, new AllocateMessageQueueConsistentHash().VirtualNodeCnt);
    }

    /// Java verifyAllocateAll：任意规模下每条队列恰好分给一个消费者（不重不漏）。
    [Fact]
    public void Consistent_hash_covers_every_queue_once()
    {
        var strategy = new AllocateMessageQueueConsistentHash(3);
        for (int mqSize = 1; mqSize <= 11; mqSize++)
        {
            for (int cidSize = 1; cidSize <= 7; cidSize++)
            {
                List<MessageQueue> mqAll = QueuesOf(mqSize);
                List<string> cidAll = ChCids(cidSize);
                List<int> flat = cidAll
                    .SelectMany(cid => ChAllocateIds(strategy, cid, mqAll, cidAll)).ToList();
                Assert.Equal(Enumerable.Range(0, mqSize).ToList(), flat.OrderBy(x => x).ToList());
            }
        }
    }

    /// 一致性哈希的全部意义：成员变化只动涉及的那段弧，其它消费者的队列不换主。
    [Fact]
    public void Consistent_hash_is_stable_when_membership_changes()
    {
        var strategy = new AllocateMessageQueueConsistentHash(3);
        List<MessageQueue> mqAll = QueuesOf(9);
        List<string> cidAll = ChCids(4);

        string[] OwnerOf(List<string> cids)
        {
            var owner = new string[9];
            foreach (string cid in cids)
            {
                foreach (MessageQueue mq in strategy.Allocate("g", cid, mqAll, cids))
                {
                    owner[mq.QueueId] = cid;
                }
            }

            return owner;
        }

        string[] before = OwnerOf(cidAll);
        Assert.All(before, o => Assert.NotNull(o));

        // 摘掉 CID-0：它原来的队列会被别人接走（那是必须发生的），
        // 但**原本就在别人手里**的队列必须还在那个人手里。
        List<string> remaining = cidAll.GetRange(1, cidAll.Count - 1);
        string[] afterRemove = OwnerOf(remaining);
        for (int qid = 0; qid < before.Length; qid++)
        {
            if (before[qid] != "CID-0")
            {
                Assert.Equal(before[qid], afterRemove[qid]);  // 摘队时不该换主
            }
        }

        // 加一个新消费者：同理，只有分给 CID-NEW 的队列是新增的。
        List<string> joined = new(remaining) { "CID-NEW" };
        string[] afterAdd = OwnerOf(joined);
        for (int qid = 0; qid < before.Length; qid++)
        {
            if (afterAdd[qid] != "CID-NEW")
            {
                Assert.Equal(before[qid], afterAdd[qid]);  // 加人时不该换主
            }
        }
    }

    /// Java 构造函数就抛 IllegalArgumentException("illegal virtualNodeCnt :")；
    /// 0 合法（Java 只挡 <0）⇒ 环上没有虚拟节点 ⇒ 一条都分不到。
    [Fact]
    public void Consistent_hash_rejects_negative_virtual_node_count()
    {
        MQClientException e = Assert.Throws<MQClientException>(
            () => new AllocateMessageQueueConsistentHash(-1));
        Assert.Contains("illegal virtualNodeCnt :-1", e.Message);
        Assert.Empty(ChAllocateIds(new AllocateMessageQueueConsistentHash(0), "CID-0",
            QueuesOf(4), ChCids(2)));
    }

    /// Java 探针场景：两个 cid 的虚拟节点 key（"CID-0-0"…）首字符都是 'C' ⇒ 哈希相同
    /// ⇒ Java TreeMap.put 后者覆盖前者，环上只剩 CID-1 ⇒ 队列全归它。
    private sealed class FirstCharHash : IHashFunction
    {
        public int Calls { get; private set; }

        public long Hash(string key)
        {
            Calls++;
            return key.Length == 0 ? 0 : key[0];
        }
    }

    [Fact]
    public void Consistent_hash_uses_the_injected_hash_function()
    {
        var injected = new FirstCharHash();
        var strategy = new AllocateMessageQueueConsistentHash(2, injected);
        List<MessageQueue> mqAll = QueuesOf(4);
        List<string> cidAll = ChCids(2);
        Assert.Empty(ChAllocateIds(strategy, "CID-0", mqAll, cidAll));
        Assert.Equal(new[] { 0, 1, 2, 3 }, ChAllocateIds(strategy, "CID-1", mqAll, cidAll));
        Assert.True(injected.Calls > 0, "自定义哈希没被调用");
    }

    [Fact]
    public void Consistent_hash_guards_return_empty()
    {
        var strategy = new AllocateMessageQueueConsistentHash();
        List<MessageQueue> mqAll = QueuesOf(4);
        List<string> cidAll = ChCids(2);
        Assert.Empty(ChAllocateIds(strategy, "CID-NOT-HERE", mqAll, cidAll));
        Assert.Empty(ChAllocateIds(strategy, "", mqAll, cidAll));
        Assert.Empty(ChAllocateIds(strategy, "CID-0", new List<MessageQueue>(), cidAll));
        Assert.Empty(ChAllocateIds(strategy, "CID-0", mqAll, new List<string>()));
    }

    // =============================================================== MACHINE_ROOM

    private static List<MessageQueue> RoomQueuesOf(string brokerName, int from, int to) =>
        Enumerable.Range(from, to - from).Select(i => new MessageQueue("topic", brokerName, i)).ToList();

    /// Java AllocateMessageQueueByMachineRoomTest：10 队列（0..4 在 room1）+ 白名单 {room1}
    /// + 2 消费者 → [0,1,4] / [2,3]（真实 Java 客户端复核）。
    /// 余数队列给**前 rem 个**消费者（rem > currentIndex），与 AVG 的切法不同。
    [Fact]
    public void ByMachineRoom_matches_java_unit_test()
    {
        List<MessageQueue> mqAll = Enumerable.Range(0, 10)
            .Select(i => new MessageQueue("topic", i < 5 ? "room1@broker-a" : "room2@broker-b", i))
            .ToList();
        List<string> cidAll = CidsOf(2);
        var strategy = new AllocateMessageQueueByMachineRoom(new[] { "room1" });
        Assert.Equal(new[] { 0, 1, 4 }, Ids(strategy.Allocate("G", cidAll[0], mqAll, cidAll)));
        Assert.Equal(new[] { 2, 3 }, Ids(strategy.Allocate("G", cidAll[1], mqAll, cidAll)));
        Assert.Single(strategy.GetConsumeridcs());
    }

    public static IEnumerable<object[]> JavaSplitCases()
    {
        // (broker 名, Java `String#split("@")` 的真实段数（JDK 17 实测）, 是否参与分配)
        yield return new object[] { "room1@broker-a", 2, true };
        yield return new object[] { "room1@", 1, false };      // 尾空段被 Java 丢掉
        yield return new object[] { "room1@b@", 2, true };     // 裁尾后仍是 2 段
        yield return new object[] { "@room1", 2, false };       // 2 段，但机房是空串、不在白名单
        yield return new object[] { "room1@broker@a", 3, false };
        yield return new object[] { "@", 0, false };
        yield return new object[] { "broker-a", 1, false };
        yield return new object[] { "", 1, false };
    }

    /// broker 名必须是 `机房@名字`（且切分按 Java 的裁尾口径）。
    [Theory]
    [MemberData(nameof(JavaSplitCases))]
    public void ByMachineRoom_uses_java_split_on_the_broker_name(string broker, int javaParts,
        bool allocated)
    {
        Assert.Equal(javaParts, AllocateMessageQueueStrategyUtil.JavaSplit(broker, '@').Length);
        List<string> cidAll = CidsOf(1);
        var strategy = new AllocateMessageQueueByMachineRoom(new[] { "room1" });
        Assert.Equal(allocated,
            strategy.Allocate("G", cidAll[0], RoomQueuesOf(broker, 0, 1), cidAll).Count > 0);
    }

    [Fact]
    public void ByMachineRoom_whitelist_is_swappable_and_defaults_to_nothing()
    {
        List<string> cidAll = CidsOf(1);
        var unset = new AllocateMessageQueueByMachineRoom();
        // 没配机房 = 一条都不分（Java 此处是 NPE，本端口按"守卫返回空"口径）
        Assert.Empty(unset.Allocate("G", cidAll[0], RoomQueuesOf("room1@b", 0, 1), cidAll));

        var strategy = new AllocateMessageQueueByMachineRoom(new[] { "room1" });
        Assert.NotEmpty(strategy.Allocate("G", cidAll[0], RoomQueuesOf("room1@broker-a", 0, 1), cidAll));
        strategy.SetConsumeridcs(new[] { "room2" });  // Java setter：换完立刻对后续 rebalance 生效
        Assert.Contains("room2", strategy.GetConsumeridcs());
        Assert.Empty(strategy.Allocate("G", cidAll[0], RoomQueuesOf("room1@broker-a", 0, 1), cidAll));
    }

    [Theory]
    [InlineData("", 4, 2)]
    [InlineData("CID_PREFIX0", 0, 2)]
    [InlineData("CID_PREFIX0", 4, 0)]
    [InlineData("CID_NOT_IN_LIST", 4, 2)]
    public void ByMachineRoom_guards_return_empty(string currentCid, int mqSize, int cidSize)
    {
        var strategy = new AllocateMessageQueueByMachineRoom(new[] { "room1" });
        Assert.Empty(strategy.Allocate("G", currentCid, RoomQueuesOf("room1@broker-a", 0, mqSize),
            CidsOf(cidSize)));
    }

    // ========================================================= MACHINE_ROOM_NEARBY

    /// Java 测试同款 resolver：broker `IDCx-brokerName` / 消费者 `IDCx-CID-i` 取 '-' 前段。
    private sealed class DashRoom : IMachineRoomResolver
    {
        public string BrokerDeployIn(MessageQueue messageQueue) =>
            messageQueue.BrokerName.Split('-')[0];

        public string ConsumerDeployIn(string clientId) => clientId.Split('-')[0];
    }

    /// 拿一个机房前缀，用于断言"队列有没有外流到别的机房"。
    private static string RoomOf(string nameOrId) => nameOrId.Split('-')[0];

    private static List<MessageQueue> NearbyMq(int idcSize, int queueSize) =>
        Enumerable.Range(1, idcSize)
            .SelectMany(i => Enumerable.Range(0, queueSize)
                .Select(q => new MessageQueue("topic", "IDC" + i + "-brokerName", q)))
            .ToList();

    private static List<string> NearbyCids(int idcSize, int consumerSize) =>
        Enumerable.Range(1, idcSize)
            .SelectMany(i => Enumerable.Range(0, consumerSize).Select(q => "IDC" + i + "-CID-" + q))
            .ToList();

    /// Java testWhenIDCSizeEquals：机房数相等时每个消费者只拿到**同机房**的队列，
    /// 且全员并集恰好是全集（不重不漏）。四组规模与 Java 参数化用例一致。
    [Theory]
    [InlineData(10)]
    [InlineData(20)]
    [InlineData(30)]
    [InlineData(1)]
    public void Nearby_allocates_same_room_only_and_covers_everything(int consumerSize)
    {
        var strategy = new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), new DashRoom());
        const int idcSize = 5;
        List<MessageQueue> mqAll = NearbyMq(idcSize, 20);
        List<string> cidAll = NearbyCids(idcSize, consumerSize);
        List<string> flat = new();
        foreach (string cid in cidAll)
        {
            foreach (MessageQueue mq in strategy.Allocate("Test-C-G", cid, mqAll, cidAll))
            {
                Assert.Equal(RoomOf(cid), RoomOf(mq.BrokerName));  // 不该拿到别的机房的队列
                flat.Add(mq.BrokerName + "#" + mq.QueueId);
            }
        }

        List<string> expected = mqAll.Select(mq => mq.BrokerName + "#" + mq.QueueId).ToList();
        Assert.Equal(expected.OrderBy(x => x, StringComparer.Ordinal).ToList(),
            flat.OrderBy(x => x, StringComparer.Ordinal).ToList());
    }

    /// Java testWhenConsumerIDCIsLess：broker 机房多于消费者机房时，**没有活消费者**的机房
    /// 要交给全部消费者共享（否则没人消费），有消费者的机房仍然只给自己的消费者。
    [Fact]
    public void Nearby_shares_rooms_that_have_no_consumer()
    {
        var strategy = new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), new DashRoom());
        // 真实 Java 客户端：mqs = IDC2×4 + IDC1×2，cids = IDC1 的两个消费者
        List<MessageQueue> mqAll = RoomQueuesOf("IDC2-brokerName", 0, 4)
            .Concat(RoomQueuesOf("IDC1-brokerName", 0, 2)).ToList();
        List<string> cidAll = new() { "IDC1-CID-0", "IDC1-CID-1" };

        List<string> Describe(List<MessageQueue> mqs) =>
            mqs.Select(mq => mq.BrokerName + "#" + mq.QueueId).ToList();

        Assert.Equal(new[] { 0, 0, 1 }, Ids(strategy.Allocate("G", "IDC1-CID-0", mqAll, cidAll)));
        // 顺序也是 Java 的口径：先收同机房队列，再补空机房的共享队列
        Assert.Equal(new[] { "IDC1-brokerName#0", "IDC2-brokerName#0", "IDC2-brokerName#1" },
            Describe(strategy.Allocate("G", "IDC1-CID-0", mqAll, cidAll)));
        Assert.Equal(new[] { "IDC1-brokerName#1", "IDC2-brokerName#2", "IDC2-brokerName#3" },
            Describe(strategy.Allocate("G", "IDC1-CID-1", mqAll, cidAll)));

        // 5 个机房、只有前 2 个有消费者：每条队列都得有人消费，健康机房不外流。
        List<MessageQueue> manyMq = NearbyMq(5, 4);
        List<string> manyCids = NearbyCids(2, 3);
        int claimed = 0;
        foreach (string cid in manyCids)
        {
            foreach (MessageQueue mq in strategy.Allocate("Test-C-G", cid, manyMq, manyCids))
            {
                string room = RoomOf(mq.BrokerName);
                if (room is "IDC1" or "IDC2")
                {
                    Assert.Equal(room, RoomOf(cid));  // 有消费者的机房队列不该外流
                }

                claimed++;
            }
        }

        Assert.Equal(manyMq.Count, claimed);
    }

    /// `getName()` = `MACHINE_ROOM_NEARBY-<内层策略名>`（Java 复核）。
    [Fact]
    public void Nearby_name_exposes_the_inner_strategy()
    {
        var resolver = new DashRoom();
        Assert.Equal("MACHINE_ROOM_NEARBY-AVG",
            new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), resolver).GetName());
        Assert.Equal("MACHINE_ROOM_NEARBY-AVG_BY_CIRCLE",
            new AllocateMachineRoomNearby(new AllocateMessageQueueAveragelyByCircle(), resolver)
                .GetName());
        Assert.Equal("MACHINE_ROOM_NEARBY-CONFIG",
            new AllocateMachineRoomNearby(new AllocateMessageQueueByConfig(QueuesOf(1)), resolver)
                .GetName());
    }

    private sealed class BlankBrokerRoom : IMachineRoomResolver
    {
        public string BrokerDeployIn(MessageQueue messageQueue) => string.Empty;
        public string ConsumerDeployIn(string clientId) => "IDC1";
    }

    private sealed class BlankConsumerRoom : IMachineRoomResolver
    {
        public string BrokerDeployIn(MessageQueue messageQueue) => "IDC1";
        public string ConsumerDeployIn(string clientId) => string.Empty;
    }

    /// resolver 给出空机房 ⇒ Java 抛 IllegalArgumentException。这里照抛而不返回空：
    /// 静默返回空等于把整个 topic 的队列撤走，而 rebalance 抓住错误会保住现有分配。
    [Fact]
    public void Nearby_throws_when_a_room_is_unknown()
    {
        List<MessageQueue> mqAll = QueuesOf(2);
        List<string> cidAll = ChCids(1);
        MQClientException e = Assert.Throws<MQClientException>(
            () => new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), new BlankBrokerRoom())
                .Allocate("G", "CID-0", mqAll, cidAll));
        // 文案带 Java 的 MessageQueue#toString
        Assert.Contains("Machine room is null for mq MessageQueue [topic=topic, brokerName=brokerName, " +
                        "queueId=0]", e.Message);

        e = Assert.Throws<MQClientException>(
            () => new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), new BlankConsumerRoom())
                .Allocate("G", "CID-0", mqAll, cidAll));
        Assert.Contains("Machine room is null for consumer id CID-0", e.Message);

        // 守卫仍然优先：cidAll 为空时先返回空，不碰 resolver
        Assert.Empty(new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), new BlankBrokerRoom())
            .Allocate("G", "CID-0", mqAll, new List<string>()));
    }

    /// Java 构造器对 null 参数抛 NullPointerException，文案照抄。
    [Fact]
    public void Nearby_rejects_null_arguments()
    {
        var strategy = Assert.Throws<MQClientException>(
            () => new AllocateMachineRoomNearby(null, new DashRoom()));
        Assert.Contains("allocateMessageQueueStrategy is null", strategy.Message);
        MQClientException resolver = Assert.Throws<MQClientException>(
            () => new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), null));
        Assert.Contains("machineRoomResolver is null", resolver.Message);
    }

    /// javaSplit 的早返回分支（没命中分隔符时不裁尾）单独钉一遍。
    [Theory]
    [InlineData("", 1)]
    [InlineData("@", 0)]
    [InlineData("room1@", 1)]
    [InlineData("room1@b@", 2)]
    [InlineData("@room1", 2)]
    [InlineData("room1@@b", 3)]
    [InlineData("broker-a", 1)]
    public void Java_split_matches_jdk(string text, int parts)
    {
        Assert.Equal(parts, AllocateMessageQueueStrategyUtil.JavaSplit(text, '@').Length);
    }

    /// 三个新策略在 push / lite / pull 消费者上都能换上并读回（暴露面与旧策略一致）。
    [Fact]
    public void New_strategies_plug_into_the_consumers()
    {
        var cidAll = new List<string> { "CID-0" };

        var push = new DefaultMQPushConsumer("G_test");
        push.AllocateMessageQueueStrategy = new AllocateMessageQueueConsistentHash(3);
        Assert.Equal("CONSISTENT_HASH", push.AllocateMessageQueueStrategy.GetName());
        // 单消费者 + 哈希环：4 条队列全落在自己身上
        Assert.Equal(new[] { 0, 1, 2, 3 }, Ids(push.AllocateMessageQueueStrategy
            .Allocate("g", "CID-0", QueuesOf(4), cidAll)));

        var lite = new DefaultLitePullConsumer("G_lite");
        lite.SetAllocateMessageQueueStrategy(
            new AllocateMessageQueueByMachineRoom(new[] { "room1" }));
        Assert.Equal("MACHINE_ROOM", lite.AllocateMessageQueueStrategy!.GetName());
        Assert.Equal(new[] { 0, 1, 2 }, Ids(lite.AllocateMessageQueueStrategy.Allocate("g", "CID-0",
            RoomQueuesOf("room1@broker-a", 0, 3), cidAll)));

        lite.SetAllocateMessageQueueStrategy(
            new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), new DashRoom()));
        Assert.Equal("MACHINE_ROOM_NEARBY-AVG", lite.AllocateMessageQueueStrategy.GetName());

        var pull = new DefaultMQPullConsumer("G_test");
        pull.SetAllocateMessageQueueStrategy(new AllocateMessageQueueConsistentHash());
        Assert.Equal("CONSISTENT_HASH", pull.AllocateMessageQueueStrategy!.GetName());
    }
}
