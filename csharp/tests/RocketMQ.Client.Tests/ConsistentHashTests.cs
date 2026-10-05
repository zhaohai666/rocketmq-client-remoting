// 一致性哈希环单测：MD5 摘要 → 前 4 字节大端拼接、含端点的 tailMap、回绕、replica 续编号。
//
// 期望值与 rust/src/common/consistent_hash.rs、cpp/tests/test_consistent_hash.cpp 逐值一致
// （四语言对拍）；策略侧的落点表见 AllocateStrategyTests 的 CONSISTENT_HASH 用例。
using System;
using System.Collections.Generic;
using System.Linq;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ConsistentHashTests
{
    private static List<INode> NodesOf(params string[] keys) =>
        keys.Select(k => (INode)new ClientNode(k)).ToList();

    private static string RoutedKey(ConsistentHashRouter router, string objectKey) =>
        router.RouteNode(objectKey)?.GetKey() ?? "<null>";

    /// Java 只取摘要前 4 字节大端拼接（不是完整 128 bit）。
    /// 参考值：MD5("") = d41d8cd9…、MD5("abc") = 90015098 3cd24fb0…（RFC 1321 附录 A）。
    [Theory]
    [InlineData("", 0xD41D8CD9)]
    [InlineData("abc", 0x90015098)]
    [InlineData("message digest", 0xF96B697D)]
    public void Md5_hash_takes_only_the_first_four_bytes(string key, long expected)
    {
        Assert.Equal(expected, new Md5Hash().Hash(key));
    }

    /// 结果必须落在 32 位内：Java 的 long 只装了 4 个字节，负数永远不会出现。
    [Fact]
    public void Md5_hash_stays_in_the_low_32_bits()
    {
        var hash = new Md5Hash();
        foreach (string key in new[] { "", "abc", "CID-0-0", "MessageQueue [topic=t, brokerName=b, queueId=0]" })
        {
            long h = hash.Hash(key);
            Assert.InRange(h, 0L, (long)uint.MaxValue);
        }
    }

    /// 同一个 key 永远落在同一个节点上（策略稳定性的前提）+ 两个节点都要被路由到。
    [Fact]
    public void Ring_routes_to_the_clockwise_neighbour_and_wraps_around()
    {
        var router = new ConsistentHashRouter(NodesOf("n0", "n1"), 5);
        foreach (string key in new[]
                 {
                     "MessageQueue [topic=t, brokerName=b, queueId=0]", "k", "另一个"
                 })
        {
            string first = RoutedKey(router, key);
            for (int i = 0; i < 3; i++)
            {
                Assert.Equal(first, RoutedKey(router, key));
            }
        }

        List<string> seen = Enumerable.Range(0, 64)
            .Select(i => RoutedKey(router, "queue-" + i)).Distinct().ToList();
        Assert.Equal(2, seen.Count);  // 两个物理节点都得被路由到
    }

    [Fact]
    public void Empty_ring_routes_to_nothing()
    {
        var router = new ConsistentHashRouter(new List<INode>(), 3);
        Assert.Null(router.RouteNode("anything"));
        Assert.Empty(router.RingHashes());
    }

    /// Java 的构造器只是遍历 pNodes 调 addNode ⇒ 空集合时不会抛；检查在 addNode 里。
    [Fact]
    public void Negative_virtual_node_count_is_rejected_by_add_node()
    {
        var router = new ConsistentHashRouter(new List<INode>(), 0);
        Assert.Empty(router.RingHashes());  // 空节点集 + 负数也不触发检查
        ArgumentException e = Assert.Throws<ArgumentException>(() => router.AddNode(new ClientNode("a"), -1));
        Assert.Contains("illegal virtual node counts :-1", e.Message);
    }

    /// Java 的 `i + existingReplicas`：同一物理节点二次 addNode 得到 2×v 个**不同**落点，
    /// 而不是全撞在同一个 hash 上（TreeMap.put 会覆盖 ⇒ 实际少一半节点）。
    [Fact]
    public void Re_adding_a_node_keeps_virtual_nodes_distinct()
    {
        var dup = new ClientNode("dup");
        var router = new ConsistentHashRouter(new List<INode>(), 0);
        router.AddNode(dup, 3);
        Assert.Equal(3, router.GetExistingReplicas(dup));
        router.AddNode(dup, 2);
        Assert.Equal(5, router.GetExistingReplicas(dup));
        Assert.Equal(5, router.RingHashes().Count);
    }

    /// removeNode 只摘该物理节点的虚拟节点（按 key 判定，Java 亦如此）。
    [Fact]
    public void Remove_node_drops_only_that_physical_node()
    {
        var a = new ClientNode("a");
        var b = new ClientNode("b");
        var router = new ConsistentHashRouter(NodesOf("a", "b"), 4);
        router.RemoveNode(a);
        Assert.Equal(0, router.GetExistingReplicas(a));
        Assert.Equal(4, router.GetExistingReplicas(b));
        Assert.Equal("b", RoutedKey(router, "whatever"));
    }

    /// 注入的 HashFunction 必须参与每一次查找（Java 允许注入）。
    private sealed class FirstByte : IHashFunction
    {
        public int Calls { get; private set; }

        public long Hash(string key)
        {
            Calls++;
            return key.Length == 0 ? 0 : key[0];
        }
    }

    [Fact]
    public void Custom_hash_function_is_used_for_every_lookup()
    {
        var injected = new FirstByte();
        var router = new ConsistentHashRouter(NodesOf("aaa", "bbb"), 1, injected);
        // "a"(97) 与 "b"(98) 各占一个位置；"c"(99) 越过末尾 → 回绕到最小 hash 的节点
        Assert.Equal("aaa", RoutedKey(router, "a!"));
        Assert.Equal("bbb", RoutedKey(router, "b!"));
        Assert.Equal("aaa", RoutedKey(router, "c!"));
        Assert.True(injected.Calls > 0, "自定义哈希没被调用");
    }

    /// null 哈希函数 = 默认 MD5Hash（Java 此处抛 NPE，本端口「空即默认」）。
    [Fact]
    public void Null_hash_function_falls_back_to_md5()
    {
        var withNull = new ConsistentHashRouter(NodesOf("CID-0", "CID-1"), 3, null);
        var withMd5 = new ConsistentHashRouter(NodesOf("CID-0", "CID-1"), 3);
        Assert.Equal(withMd5.RingHashes(), withNull.RingHashes());
    }

    /// 环上的 hash 升序 = Java TreeMap 的遍历序（ceiling/回绕口径一致的前提）。
    [Fact]
    public void Ring_hashes_are_ascending()
    {
        List<long> hashes = new ConsistentHashRouter(NodesOf("CID-0", "CID-1", "CID-2"), 3).RingHashes();
        Assert.Equal(9, hashes.Count);  // 3 节点 × 3 虚拟，且没有互相覆盖
        Assert.Equal(hashes.OrderBy(x => x).ToList(), hashes);
    }
}
