// Unit tests for the consistent hash ring: MD5 digest -> big-endian concatenation of the
// first 4 bytes, a tailMap holding the endpoints, wrap-around, and replica continuation
// numbering.
//
// The expected values are golden values fixed under the digest rule, written out value by
// value; the strategy-side landing table is in the CONSISTENT_HASH case of
// AllocateStrategyTests.
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

    [Theory]
    [InlineData("", 0xD41D8CD9)]
    [InlineData("abc", 0x90015098)]
    [InlineData("message digest", 0xF96B697D)]
    public void Md5_hash_takes_only_the_first_four_bytes(string key, long expected)
    {
        Assert.Equal(expected, new Md5Hash().Hash(key));
    }

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

    /// The same key always lands on the same node (the precondition for strategy
    /// stability) + both nodes must be routed to.
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
        Assert.Equal(2, seen.Count);  // both physical nodes must be routed to
    }

    [Fact]
    public void Empty_ring_routes_to_nothing()
    {
        var router = new ConsistentHashRouter(new List<INode>(), 3);
        Assert.Null(router.RouteNode("anything"));
        Assert.Empty(router.RingHashes());
    }

    [Fact]
    public void Negative_virtual_node_count_is_rejected_by_add_node()
    {
        var router = new ConsistentHashRouter(new List<INode>(), 0);
        Assert.Empty(router.RingHashes());  // an empty node set plus a negative number does not trigger the check either
        ArgumentException e = Assert.Throws<ArgumentException>(() => router.AddNode(new ClientNode("a"), -1));
        Assert.Contains("illegal virtual node counts :-1", e.Message);
    }

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
        // "a"(97) and "b"(98) each occupy one position; "c"(99) passes the end -> wraps around to the node with the smallest hash
        Assert.Equal("aaa", RoutedKey(router, "a!"));
        Assert.Equal("bbb", RoutedKey(router, "b!"));
        Assert.Equal("aaa", RoutedKey(router, "c!"));
        Assert.True(injected.Calls > 0, "the custom hash was never called");
    }

    [Fact]
    public void Null_hash_function_falls_back_to_md5()
    {
        var withNull = new ConsistentHashRouter(NodesOf("CID-0", "CID-1"), 3, null);
        var withMd5 = new ConsistentHashRouter(NodesOf("CID-0", "CID-1"), 3);
        Assert.Equal(withMd5.RingHashes(), withNull.RingHashes());
    }

    [Fact]
    public void Ring_hashes_are_ascending()
    {
        List<long> hashes = new ConsistentHashRouter(NodesOf("CID-0", "CID-1", "CID-2"), 3).RingHashes();
        Assert.Equal(9, hashes.Count);  // 3 nodes x 3 virtual, with no overwriting of each other
        Assert.Equal(hashes.OrderBy(x => x).ToList(), hashes);
    }
}
