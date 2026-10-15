// Ported `ConsistentHashRouter` / `Node` / `VirtualNode` / `HashFunction` four types plus
// the private `ConsistentHashRouter.MD5Hash` implementation; the only current user is the queue allocation strategy
// `AllocateMessageQueueConsistentHash`.
//
// ## Why port the whole thing verbatim instead of "equivalent rewriting"
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace RocketMQ.Common;

public interface INode
{
    string GetKey();
}

public sealed class ClientNode : INode
{
    public string ClientId { get; }

    public ClientNode(string clientId) => ClientId = clientId;

    public string GetKey() => ClientId;
}

public interface IHashFunction
{
    long Hash(string key);
}

public sealed class Md5Hash : IHashFunction
{
    public long Hash(string key)
    {
        byte[] digest = MD5.HashData(Encoding.UTF8.GetBytes(key));
        long h = 0;
        for (int i = 0; i < 4; i++)
        {
            h = (h << 8) | digest[i];
        }

        return h;
    }
}

public sealed class ConsistentHashRouter
{
    private readonly SortedDictionary<long, VirtualNode> _ring = new();
    private readonly IHashFunction _hashFunction;

    public ConsistentHashRouter(IEnumerable<INode> pNodes, int vNodeCount)
        : this(pNodes, vNodeCount, null)
    {
    }

    public ConsistentHashRouter(IEnumerable<INode> pNodes, int vNodeCount, IHashFunction? hashFunction)
    {
        _hashFunction = hashFunction ?? new Md5Hash();
        foreach (INode pNode in pNodes)
        {
            AddNode(pNode, vNodeCount);
        }
    }

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
            _ring[_hashFunction.Hash(vNode.GetKey())] = vNode;
        }
    }

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

        return _ring.First().Value.PhysicalNode;
    }

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

    public List<long> RingHashes()
    {
        var hashes = new List<long>(_ring.Count);
        foreach (KeyValuePair<long, VirtualNode> entry in _ring)
        {
            hashes.Add(entry.Key);
        }

        return hashes;
    }

    private sealed class VirtualNode
    {
        public INode PhysicalNode { get; }
        public int ReplicaIndex { get; }

        public VirtualNode(INode physicalNode, int replicaIndex)
        {
            PhysicalNode = physicalNode;
            ReplicaIndex = replicaIndex;
        }

        public string GetKey() =>
            PhysicalNode.GetKey() + "-" + ReplicaIndex.ToString(CultureInfo.InvariantCulture);

        public bool IsVirtualNodeOf(INode pNode) =>
            string.Equals(PhysicalNode.GetKey(), pNode.GetKey(), StringComparison.Ordinal);
    }
}
