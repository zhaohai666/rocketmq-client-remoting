// 一致性哈希环（对应 Java `org.apache.rocketmq.common.consistenthash` 包）。
//
// 移植 `ConsistentHashRouter` / `Node` / `VirtualNode` / `HashFunction` 四个类型加
// `ConsistentHashRouter.MD5Hash` 这个私有实现；目前唯一的用户是队列分配策略
// `AllocateMessageQueueConsistentHash`。
//
// ## 为什么整套照搬而不是"等价改写"
//
// 环上的落点由三处细节共同决定，任何一处不同都会让**全体**队列换主，与 Java 客户端
// 混跑时表现为重复消费 / 漏消费，而不是"分得稍有不均"：
//
// 1. 哈希函数只取 MD5 摘要的**前 4 个字节**按大端拼成整数（Java
//    `for (int i = 0; i < 4; i++) { h <<= 8; h |= digest[i] & 0xFF; }`），不是完整 128 bit；
// 2. 虚拟节点的 key 是 `物理节点key + "-" + 副本序号`，序号从 `existingReplicas` 起算；
// 3. 查找用 `TreeMap#tailMap(hashVal)`，它**含端点**，所以取的是第一个 ≥ hashVal 的落点
//    （相等的 hash 归自己），越过环末尾时回绕到 `firstKey()`。
//
// ## 与 Java 的有意差异
//
// 1. `hashFunction == null` 在 Java 抛 `NullPointerException`；这里与 C++ 端口一样按
//    「null 即默认 MD5Hash」处理，因为策略侧的 customHashFunction 本来就是可选项。
// 2. 环用 `SortedDictionary` + 顺序枚举做 ceiling 查找（Java TreeMap 是 O(log n)，
//    这里是 O(n)）。环的规模是 消费者数 × 虚拟节点数（默认 10），量级很小，
//    换来的是不引任何第三方有序字典。
// 3. MD5 直接用 BCL 的 `System.Security.Cryptography.MD5`（= Java 的
//    `MessageDigest.getInstance("MD5")`，同一个 RFC 1321 实现），**不引 NuGet**。
//    只用于算环上的落点，不当密码学原语用。
// 4. `addNode` 的负数虚拟节点数：Java 抛 `IllegalArgumentException`，这里抛它的 .NET
//    同名对应物 `ArgumentException`（文案一致）。本文件在 `Common` 命名空间，
//    不该反向依赖 `Client` 里的 `MQClientException`；rebalance 侧 catch 的是 `Exception`，
//    所以两种抛法在消费端行为相同。策略构造器仍在 `Client` 侧抛 `MQClientException`。
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace RocketMQ.Common;

/// <summary>
/// 对应 Java <c>Node#getKey</c>：能被映到环上的东西，物理节点和虚拟节点都算。
/// </summary>
public interface INode
{
    string GetKey();
}

/// <summary>
/// 对应 Java <c>AllocateMessageQueueConsistentHash.ClientNode</c>：key 就是 clientId。
/// </summary>
/// <remarks>
/// Java 里它是策略的 private 静态内部类；这里与 <c>common</c> 的其它节点类型放在一起
/// （Python / Rust / C++ 端口同样把它暴露在哈希模块里）。
/// </remarks>
public sealed class ClientNode : INode
{
    public string ClientId { get; }

    public ClientNode(string clientId) => ClientId = clientId;

    public string GetKey() => ClientId;
}

/// <summary>对应 Java <c>HashFunction#hash(String)</c>：把字符串映射到环上的位置。</summary>
public interface IHashFunction
{
    long Hash(string key);
}

/// <summary>
/// 对应 Java <c>ConsistentHashRouter.MD5Hash</c>（默认哈希函数）。
/// </summary>
/// <remarks>⚠ 只取摘要前 4 字节（不是完整 128 bit）；换一种写法就不是 Java 的那个环。</remarks>
public sealed class Md5Hash : IHashFunction
{
    public long Hash(string key)
    {
        // Java：MessageDigest(MD5).digest(utf8(key))，再 `h <<= 8; h |= digest[i] & 0xFF`（i<4）
        // —— 那个掩码是必须的，因为 Java 的 byte 有符号；C# 的 byte 无符号，升位即同值。
        byte[] digest = MD5.HashData(Encoding.UTF8.GetBytes(key));
        long h = 0;
        for (int i = 0; i < 4; i++)
        {
            h = (h << 8) | digest[i];
        }

        return h;
    }
}

/// <summary>
/// 对应 Java <c>ConsistentHashRouter</c>：虚拟节点环 + "顺时针找最近物理节点"。
/// </summary>
public sealed class ConsistentHashRouter
{
    // Java 是 TreeMap<Long, VirtualNode<T>>；SortedDictionary 按键升序枚举，
    // 顺序枚举到第一个 ≥ hashVal 的项即含端点的 tailMap().firstKey()。
    private readonly SortedDictionary<long, VirtualNode> _ring = new();
    private readonly IHashFunction _hashFunction;

    /// <summary>对应 Java <c>ConsistentHashRouter(Collection&lt;T&gt;, int)</c>：用默认 MD5Hash。</summary>
    public ConsistentHashRouter(IEnumerable<INode> pNodes, int vNodeCount)
        : this(pNodes, vNodeCount, null)
    {
    }

    /// <summary>对应 Java <c>ConsistentHashRouter(Collection&lt;T&gt;, int, HashFunction)</c>。</summary>
    public ConsistentHashRouter(IEnumerable<INode> pNodes, int vNodeCount, IHashFunction? hashFunction)
    {
        // Java 此处对 null 抛 NPE；本端口「null 即默认」（文件头差异 1）。
        _hashFunction = hashFunction ?? new Md5Hash();
        foreach (INode pNode in pNodes)
        {
            AddNode(pNode, vNodeCount);
        }
    }

    /// <summary>
    /// 对应 Java <c>#addNode</c>。<paramref name="vNodeCount"/> 为负 → Java 抛
    /// IllegalArgumentException，这里抛 <see cref="ArgumentException"/> 同文案。
    /// </summary>
    /// <remarks>
    /// `i + existingReplicas` 那段不是冗余：同一个物理节点分两次 addNode 时，Java 靠已有
    /// 副本数把虚拟节点编号继续往后排；从 0 重编会让两批虚拟节点撞在同一个 hash 上
    /// （Java 是 TreeMap.put，后者覆盖前者 ⇒ 实际少一半节点）。
    /// </remarks>
    public void AddNode(INode pNode, int vNodeCount)
    {
        if (vNodeCount < 0)
        {
            throw new ArgumentException("illegal virtual node counts :" +
                                        vNodeCount.ToString(CultureInfo.InvariantCulture));
        }

        int existingReplicas = GetExistingReplicas(pNode);
        for (int i = 0; i < vNodeCount; i++)
        {
            var vNode = new VirtualNode(pNode, i + existingReplicas);
            // Java 是 TreeMap.put：同 hash 时后来者覆盖，位置不变。
            _ring[_hashFunction.Hash(vNode.GetKey())] = vNode;
        }
    }

    /// <summary>对应 Java <c>#removeNode</c>：摘掉该物理节点的全部虚拟节点（按 key 判定，Java 亦如此）。</summary>
    public void RemoveNode(INode pNode)
    {
        var doomed = new List<long>();
        foreach (KeyValuePair<long, VirtualNode> entry in _ring)
        {
            if (entry.Value.IsVirtualNodeOf(pNode))
            {
                doomed.Add(entry.Key);
            }
        }

        foreach (long hash in doomed)
        {
            _ring.Remove(hash);
        }
    }

    /// <summary>
    /// 对应 Java <c>#routeNode</c>：环空返回 null，否则返回顺时针第一个（含同 hash）
    /// 虚拟节点所属的物理节点。
    /// </summary>
    public INode? RouteNode(string objectKey)
    {
        if (_ring.Count == 0)
        {
            return null;
        }

        long hashVal = _hashFunction.Hash(objectKey);
        foreach (KeyValuePair<long, VirtualNode> entry in _ring)
        {
            if (entry.Key >= hashVal)
            {
                return entry.Value.PhysicalNode;
            }
        }

        // tailMap 为空 ⇒ Java 回绕到 ring.firstKey()（上面已保证环非空）
        return _ring.First().Value.PhysicalNode;
    }

    /// <summary>对应 Java <c>#getExistingReplicas</c>。</summary>
    public int GetExistingReplicas(INode pNode)
    {
        int replicas = 0;
        foreach (KeyValuePair<long, VirtualNode> entry in _ring)
        {
            if (entry.Value.IsVirtualNodeOf(pNode))
            {
                replicas++;
            }
        }

        return replicas;
    }

    /// <summary>环上的 hash 集合（调试与单测用；顺序即 Java TreeMap 的升序）。</summary>
    public List<long> RingHashes()
    {
        var hashes = new List<long>(_ring.Count);
        foreach (KeyValuePair<long, VirtualNode> entry in _ring)
        {
            hashes.Add(entry.Key);
        }

        return hashes;
    }

    /// <summary>对应 Java <c>VirtualNode</c>：物理节点 + 副本序号。</summary>
    private sealed class VirtualNode
    {
        public INode PhysicalNode { get; }
        public int ReplicaIndex { get; }

        public VirtualNode(INode physicalNode, int replicaIndex)
        {
            PhysicalNode = physicalNode;
            ReplicaIndex = replicaIndex;
        }

        /// <summary>对应 Java <c>VirtualNode#getKey</c>：物理节点 key + "-" + 序号。</summary>
        public string GetKey() =>
            PhysicalNode.GetKey() + "-" + ReplicaIndex.ToString(CultureInfo.InvariantCulture);

        /// <summary>对应 Java <c>VirtualNode#isVirtualNodeOf</c>：Java 只比 key，从不比对象身份。</summary>
        public bool IsVirtualNodeOf(INode pNode) =>
            string.Equals(PhysicalNode.GetKey(), pNode.GetKey(), StringComparison.Ordinal);
    }
}
