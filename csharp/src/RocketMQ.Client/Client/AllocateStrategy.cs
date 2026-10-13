// 队列分配策略（rebalance 时把某个 topic 的队列分给同一消费组里的各个消费者）。
//
//
// ## 移植范围
using System;
using System.Collections.Generic;
using System.Globalization;

using RocketMQ.Common;

namespace RocketMQ.Client;

public interface IAllocateMessageQueueStrategy
{
    List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll);

    string GetName();
}

public static class AllocateMessageQueueStrategyUtil
{
    public static string[] Split(string text, char sep)
    {
        string[] parts = text.Split(sep);
        if (parts.Length == 1)
        {
            return parts;
        }

        int end = parts.Length;
        while (end > 0 && parts[end - 1].Length == 0)
        {
            end--;
        }

        Array.Resize(ref parts, end);
        return parts;
    }

    public static int Check(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll)
    {
        if (string.IsNullOrEmpty(currentCid))
        {
            return -1;
        }

        if (mqAll.Count == 0)
        {
            return -1;
        }

        if (cidAll.Count == 0)
        {
            return -1;
        }

        int index = cidAll.FindIndex(cid => string.Equals(cid, currentCid, StringComparison.Ordinal));
        if (index < 0)
        {
            ClientLog.Info("[BUG] ConsumerGroup: " + consumerGroup + " The consumerId: "
                + currentCid + " not in cidAll: " + string.Join(", ", cidAll));
            return -1;
        }

        return index;
    }
}

public sealed class AllocateMessageQueueAveragely : IAllocateMessageQueueStrategy
{
    public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll)
    {
        int index = AllocateMessageQueueStrategyUtil.Check(consumerGroup, currentCid, mqAll, cidAll);
        if (index < 0)
        {
            return new List<MessageQueue>();
        }

        int mod = mqAll.Count % cidAll.Count;
        int averageSize = mqAll.Count <= cidAll.Count
            ? 1
            : (mod > 0 && index < mod ? mqAll.Count / cidAll.Count + 1 : mqAll.Count / cidAll.Count);
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.Min(averageSize, mqAll.Count - startIndex);
        if (range <= 0 || startIndex < 0 || startIndex >= mqAll.Count)
        {
            return new List<MessageQueue>();
        }

        return mqAll.GetRange(startIndex, range);
    }

    public string GetName() => "AVG";
}

public sealed class AllocateMessageQueueAveragelyByCircle : IAllocateMessageQueueStrategy
{
    public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll)
    {
        int index = AllocateMessageQueueStrategyUtil.Check(consumerGroup, currentCid, mqAll, cidAll);
        if (index < 0)
        {
            return new List<MessageQueue>();
        }

        var result = new List<MessageQueue>();
        for (int i = index; i < mqAll.Count; i += cidAll.Count)
        {
            result.Add(mqAll[i]);
        }

        return result;
    }

    public string GetName() => "AVG_BY_CIRCLE";
}

public sealed class AllocateMessageQueueByConfig : IAllocateMessageQueueStrategy
{
    private readonly object _lock = new();
    private List<MessageQueue> _messageQueueList = new();

    public AllocateMessageQueueByConfig()
    {
    }

    public AllocateMessageQueueByConfig(List<MessageQueue> messageQueueList)
    {
        _messageQueueList = new List<MessageQueue>(messageQueueList);
    }

    public void SetMessageQueueList(List<MessageQueue> messageQueueList)
    {
        lock (_lock)
        {
            _messageQueueList = new List<MessageQueue>(messageQueueList);
        }
    }

    public List<MessageQueue> GetMessageQueueList()
    {
        lock (_lock)
        {
            return new List<MessageQueue>(_messageQueueList);
        }
    }

    public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll) => GetMessageQueueList();

    public string GetName() => "CONFIG";
}

public sealed class AllocateMessageQueueConsistentHash : IAllocateMessageQueueStrategy
{
    private readonly int _virtualNodeCnt;
    private readonly IHashFunction? _customHashFunction;

    public AllocateMessageQueueConsistentHash() : this(10, null)
    {
    }

    public AllocateMessageQueueConsistentHash(int virtualNodeCnt) : this(virtualNodeCnt, null)
    {
    }

    public AllocateMessageQueueConsistentHash(int virtualNodeCnt, IHashFunction? customHashFunction)
    {
        if (virtualNodeCnt < 0)
        {
            throw new MQClientException("illegal virtualNodeCnt :" +
                                        virtualNodeCnt.ToString(CultureInfo.InvariantCulture));
        }

        _virtualNodeCnt = virtualNodeCnt;
        _customHashFunction = customHashFunction;
    }

    public int VirtualNodeCnt => _virtualNodeCnt;

    public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll)
    {
        var result = new List<MessageQueue>();
        if (AllocateMessageQueueStrategyUtil.Check(consumerGroup, currentCid, mqAll, cidAll) < 0)
        {
            return result;
        }

        var cidNodes = new List<INode>(cidAll.Count);
        foreach (string cid in cidAll)
        {
            cidNodes.Add(new ClientNode(cid));
        }

        var router = new ConsistentHashRouter(cidNodes, _virtualNodeCnt, _customHashFunction);
        foreach (MessageQueue mq in mqAll)
        {
            INode? node = router.RouteNode(mq.ToString());
            if (node != null && string.Equals(node.GetKey(), currentCid, StringComparison.Ordinal))
            {
                result.Add(mq);
            }
        }

        return result;
    }

    public string GetName() => "CONSISTENT_HASH";
}

public sealed class AllocateMessageQueueByMachineRoom : IAllocateMessageQueueStrategy
{
    private readonly object _lock = new();
    private HashSet<string> _consumeridcs = new();

    public AllocateMessageQueueByMachineRoom()
    {
    }

    /// <summary>按机房集合（consumeridcs）构造：只在这些机房内分配队列。</summary>
    public AllocateMessageQueueByMachineRoom(IEnumerable<string> consumeridcs)
    {
        _consumeridcs = new HashSet<string>(consumeridcs, StringComparer.Ordinal);
    }

    public void SetConsumeridcs(IEnumerable<string> consumeridcs)
    {
        lock (_lock)
        {
            _consumeridcs = new HashSet<string>(consumeridcs, StringComparer.Ordinal);
        }
    }

    public HashSet<string> GetConsumeridcs()
    {
        lock (_lock)
        {
            return new HashSet<string>(_consumeridcs, StringComparer.Ordinal);
        }
    }

    public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll)
    {
        int currentIndex = AllocateMessageQueueStrategyUtil.Check(consumerGroup, currentCid, mqAll, cidAll);
        var result = new List<MessageQueue>();
        if (currentIndex < 0)
        {
            return result;
        }

        HashSet<string> rooms = GetConsumeridcs();
        var premqAll = new List<MessageQueue>();
        foreach (MessageQueue mq in mqAll)
        {
            string[] temp = AllocateMessageQueueStrategyUtil.Split(mq.BrokerName, '@');
            if (temp.Length == 2 && rooms.Contains(temp[0]))
            {
                premqAll.Add(mq);
            }
        }

        int cidSize = cidAll.Count;  // 守卫保证 >= 1
        int mod = premqAll.Count / cidSize;
        int rem = premqAll.Count % cidSize;
        int startIndex = mod * currentIndex;
        int endIndex = startIndex + mod;
        for (int i = startIndex; i < endIndex; i++)
        {
            result.Add(premqAll[i]);
        }

        if (rem > currentIndex)
        {
            // 越界不可能：`currentIndex < rem` 且 `rem < cidSize`
            // ⇒ `currentIndex + mod*cidSize < rem + mod*cidSize == premqAll.Count`。
            result.Add(premqAll[currentIndex + (mod * cidSize)]);
        }

        return result;
    }

    public string GetName() => "MACHINE_ROOM";
}

public interface IMachineRoomResolver
{
    string BrokerDeployIn(MessageQueue messageQueue);

    string ConsumerDeployIn(string clientId);
}

public sealed class AllocateMachineRoomNearby : IAllocateMessageQueueStrategy
{
    private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStrategy;
    private readonly IMachineRoomResolver _machineRoomResolver;
    private readonly string _name;

    public AllocateMachineRoomNearby(IAllocateMessageQueueStrategy? allocateMessageQueueStrategy,
        IMachineRoomResolver? machineRoomResolver)
    {
        if (allocateMessageQueueStrategy == null)
        {
            throw new MQClientException("allocateMessageQueueStrategy is null");
        }

        if (machineRoomResolver == null)
        {
            throw new MQClientException("machineRoomResolver is null");
        }

        _allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        _machineRoomResolver = machineRoomResolver;
        _name = "MACHINE_ROOM_NEARBY" + "-" + allocateMessageQueueStrategy.GetName();
    }

    public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll)
    {
        if (AllocateMessageQueueStrategyUtil.Check(consumerGroup, currentCid, mqAll, cidAll) < 0)
        {
            return new List<MessageQueue>();
        }

        var mr2Mq = new SortedDictionary<string, List<MessageQueue>>(StringComparer.Ordinal);
        foreach (MessageQueue mq in mqAll)
        {
            string brokerMachineRoom = _machineRoomResolver.BrokerDeployIn(mq);
            if (string.IsNullOrEmpty(brokerMachineRoom))
            {
                throw new MQClientException("Machine room is null for mq " + mq);
            }

            if (!mr2Mq.TryGetValue(brokerMachineRoom, out List<MessageQueue>? roomMqs))
            {
                roomMqs = new List<MessageQueue>();
                mr2Mq[brokerMachineRoom] = roomMqs;
            }

            roomMqs.Add(mq);
        }

        var mr2c = new SortedDictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (string cid in cidAll)
        {
            string consumerMachineRoom = _machineRoomResolver.ConsumerDeployIn(cid);
            if (string.IsNullOrEmpty(consumerMachineRoom))
            {
                throw new MQClientException("Machine room is null for consumer id " + cid);
            }

            if (!mr2c.TryGetValue(consumerMachineRoom, out List<string>? roomCids))
            {
                roomCids = new List<string>();
                mr2c[consumerMachineRoom] = roomCids;
            }

            roomCids.Add(cid);
        }

        var allocateResults = new List<MessageQueue>();

        string currentMachineRoom = _machineRoomResolver.ConsumerDeployIn(currentCid);
        bool hasRoomMq = mr2Mq.TryGetValue(currentMachineRoom, out List<MessageQueue>? mqInThisMachineRoom);
        if (hasRoomMq)
        {
            mr2Mq.Remove(currentMachineRoom);
        }

        mr2c.TryGetValue(currentMachineRoom, out List<string>? consumerInThisMachineRoom);
        consumerInThisMachineRoom ??= new List<string>();
        if (mqInThisMachineRoom != null && mqInThisMachineRoom.Count > 0)
        {
            allocateResults.AddRange(_allocateMessageQueueStrategy.Allocate(consumerGroup, currentCid,
                mqInThisMachineRoom, consumerInThisMachineRoom));
        }

        // 2. 没有任何存活消费者的机房，其队列由全部消费者共享。
        foreach (KeyValuePair<string, List<MessageQueue>> machineRoomEntry in mr2Mq)
        {
            if (mr2c.ContainsKey(machineRoomEntry.Key))
            {
                continue;  // 该机房有活消费者 ⇒ 队列留给它自己
            }

            allocateResults.AddRange(_allocateMessageQueueStrategy.Allocate(consumerGroup, currentCid,
                machineRoomEntry.Value, cidAll));
        }

        return allocateResults;
    }

    public string GetName() => _name;
}
