// 队列分配策略（rebalance 时把某个 topic 的队列分给同一消费组里的各个消费者）。
//
// 对应 Java `org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy` 与实现类
// `org.apache.rocketmq.client.consumer.rebalance.{AbstractAllocateMessageQueueStrategy,
// AllocateMessageQueueAveragely, AllocateMessageQueueAveragelyByCircle,
// AllocateMessageQueueByConfig, AllocateMessageQueueConsistentHash,
// AllocateMessageQueueByMachineRoom, AllocateMachineRoomNearby}`
// （#allocate / #getName / #check）。
//
// ## 移植范围
//
// Java 快照里的 6 个策略全部移植：AVG / AVG_BY_CIRCLE / CONFIG /
// `CONSISTENT_HASH`（要 `Common/ConsistentHash.cs` 的哈希环）/ `MACHINE_ROOM` /
// `MACHINE_ROOM_NEARBY-<内层>`（代理模式，见 `AllocateMachineRoomNearby`）。
//
// ## 与 Java 的有意差异（跟随 Python/Rust/C++ 口径）
//
// 1. **守卫不抛异常**：Java `AbstractAllocateMessageQueueStrategy#check` 在 currentCID 为空串、
//    mqAll 为空、cidAll 为空时抛 IllegalArgumentException；这里非法入参一律返回空列表 ——
//    rebalance 是后台周期任务，一条脏入参不该把消费者打挂。
//    唯一的例外是 `AllocateMachineRoomNearby`：resolver 给出空机房时 Java 抛
//    IllegalArgumentException，其它语言侧与本文件一律**照抛**（`MQClientException`），
//    因为静默返回空列表等于把整个 topic 的队列撤走，而 rebalance 抓住异常时反而会保住现有分配。
// 2. **AllocateMessageQueueByConfig / ByMachineRoom 未配置态**：Java 直接返回字段
//    （没配过 = null，`consumeridcs.contains` 还会 NPE）；这里与 Python/Rust/C++ 一样规整成
//    空集合，且 Allocate 返回副本。
//    同理 Java 的 `AllocateMachineRoomNearby` 构造器对 null 参数抛 NPE，本端口用
//    `MQClientException` 表达（与 `Consumer.cs` 里 "allocateMessageQueueStrategy is null" 同族）。
// 3. 策略字段用 `IAllocateMessageQueueStrategy?`，消费者属性 set 时把 null 视为「不改」
//    （Java 里 null 会在调用处抛 NPE 并被 rebalance 的 catch 记 error，这里直接挡在门口）。
// 4. **broker 名的 `@` 切分**：Java `AllocateMessageQueueByMachineRoom` 用 `String#split("@")`，
//    它会**丢掉末尾空段**（`"room1@"` → 1 段、`"room1@b@"` → 2 段），而 C# `String#Split` 不会。
//    本文件用 `JavaSplit` 复刻 Java 口径，否则一条队列参不参与分配会与 Java 客户端不一致。
using System;
using System.Collections.Generic;
using System.Globalization;

using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>
/// 队列分配策略接口（对应 Java <c>AllocateMessageQueueStrategy</c> 的 #allocate + #getName）。
/// </summary>
/// <remarks>
/// 刻意保持简单：两个方法都只读入参、不改状态，因此实例可以在 rebalance 线程与各消费线程间共享。
/// <paramref name="mqAll"/> 与 <paramref name="cidAll"/> 都要求**调用方先排序**
/// （Java RebalanceImpl#rebalanceByTopic 对两者都 Collections.sort 过），否则同组不同实例
/// 会算出互相冲突的分配。策略本身不排序、不去重，只保证「输出是输入的一个有序子序列」。
/// </remarks>
public interface IAllocateMessageQueueStrategy
{
    /// <summary>对应 Java <c>AllocateMessageQueueStrategy#allocate(consumerGroup, currentCID, mqAll, cidAll)</c>。</summary>
    List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll);

    /// <summary>对应 Java <c>AllocateMessageQueueStrategy#getName</c>：算法名（AVG / AVG_BY_CIRCLE /
    /// CONFIG / CONSISTENT_HASH / MACHINE_ROOM / MACHINE_ROOM_NEARBY-&lt;内层&gt;）。</summary>
    string GetName();
}

/// <summary>
/// 分配守卫，对应 Java <c>AbstractAllocateMessageQueueStrategy#check</c>
/// （Python 把同样的守卫内联在每个 allocate 开头）。
/// </summary>
/// <returns>currentCid 在 cidAll 中的下标；守卫不通过返回 -1（Java 是抛 IllegalArgumentException）。</returns>
public static class AllocateMessageQueueStrategyUtil
{
    /// <summary>
    /// 对应 Java <c>String#split(String)</c>（limit = 0）：**丢掉末尾的空段**。
    /// </summary>
    /// <remarks>
    /// 与 C# <c>String#Split</c> 的唯一区别就在尾上空段：<c>"room1@"</c> Java 得 <c>{"room1"}</c>
    /// （1 段），C# 得 <c>{"room1", ""}</c>（2 段）。但**没命中分隔符时** Java 走的是
    /// <c>Pattern#split</c> 里 "If no match was found, return this" 那条早返回，整串原样给出
    /// （哪怕它本身就是空串），所以不能无条件裁尾。JDK 17 实测：
    /// <c>""</c> → 1 段、<c>"@"</c> → 0 段、<c>"room1@"</c> → 1 段、<c>"room1@b@"</c> → 2 段。
    /// </remarks>
    public static string[] JavaSplit(string text, char sep)
    {
        string[] parts = text.Split(sep);
        if (parts.Length == 1)
        {
            return parts;  // 没命中分隔符：Java 直接返回整串
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

        // Java 的 List.indexOf 用 String.equals（逐字节），这里等价写成 Ordinal
        int index = cidAll.FindIndex(cid => string.Equals(cid, currentCid, StringComparison.Ordinal));
        if (index < 0)
        {
            // Java AbstractAllocateMessageQueueStrategy#check 的同名日志
            ClientLog.Info("[BUG] ConsumerGroup: " + consumerGroup + " The consumerId: "
                + currentCid + " not in cidAll: " + string.Join(", ", cidAll));
            return -1;
        }

        return index;
    }
}

/// <summary>
/// 平均分配：把 mqAll 切成**连续区间**，余数依次分给前 mod 个消费者（AVG）。
/// 对应 Java <c>AllocateMessageQueueAveragely</c>。
/// </summary>
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

        // 对齐 Java AllocateMessageQueueAveragely#allocate
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

/// <summary>
/// 环形平均分配：第 index 个消费者取走下标 ≡ index (mod cidAll.Count) 的队列（AVG_BY_CIRCLE）。
/// 对应 Java <c>AllocateMessageQueueAveragelyByCircle</c>。
/// </summary>
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

        // 对齐 Java：for (i = index; i < mqAll.size(); i++) if (i % cidAll.size() == index) add
        // 守卫已保证 cidAll.Count >= 1
        var result = new List<MessageQueue>();
        for (int i = index; i < mqAll.Count; i += cidAll.Count)
        {
            result.Add(mqAll[i]);
        }

        return result;
    }

    public string GetName() => "AVG_BY_CIRCLE";
}

/// <summary>
/// 按显式配置分配：完全无视 mqAll / cidAll，把配置进去的队列原样发给**每一个**消费者（CONFIG）。
/// 对应 Java <c>AllocateMessageQueueByConfig</c>（通常用于广播式手工绑队列）。
/// </summary>
/// <remarks>Java 与 Python 的 allocate 都**不调 check**，所以空 group / 空 cidAll 也照样返回配置值。</remarks>
public sealed class AllocateMessageQueueByConfig : IAllocateMessageQueueStrategy
{
    private readonly object _lock = new();
    private List<MessageQueue> _messageQueueList = new();

    public AllocateMessageQueueByConfig()
    {
    }

    /// <summary>对应 Python <c>AllocateMessageQueueByConfig(message_queue_list=[...])</c>（Java 只有无参构造 + setter）。</summary>
    public AllocateMessageQueueByConfig(List<MessageQueue> messageQueueList)
    {
        _messageQueueList = new List<MessageQueue>(messageQueueList);
    }

    /// <summary>对应 Java <c>#setMessageQueueList(List)</c>：注册到消费者之后仍可改，对所有后续 rebalance 生效。</summary>
    public void SetMessageQueueList(List<MessageQueue> messageQueueList)
    {
        lock (_lock)
        {
            _messageQueueList = new List<MessageQueue>(messageQueueList);
        }
    }

    /// <summary>对应 Java <c>#getMessageQueueList()</c>；返回副本，不交出共享可变引用。</summary>
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

/// <summary>
/// 一致性哈希分配：把每个 clientId 按比例铺成虚拟节点组成哈希环，再看每条队列
/// （以 <see cref="MessageQueue.ToString"/> 为 key）落进哪个区间。
/// 对应 Java <c>AllocateMessageQueueConsistentHash</c>（CONSISTENT_HASH），环本体在
/// <c>Common/ConsistentHash.cs</c>。
/// </summary>
/// <remarks>
/// 与 AVG 的关键差别**不是"均不均"**，而是**稳定性**：队列数或消费者数变化时，只有落在
/// 新增/移除节点之间弧段上的队列会换主（Java 单测 <c>AllocateMessageQueueConsitentHashTest</c>
/// 正断言这点），AVG 会把所有人的分界整体挪掉。
///
/// ⚠ 哈希的 key 必须是 <c>MessageQueue#toString()</c> 的 Java 形状
/// （"MessageQueue [topic=…, brokerName=…, queueId=…]"）：换一种写法就是另一个环，
/// 与 Java 客户端混跑时表现为全体队列换主。
///
/// 环每次 Allocate 现场重建（Java 同），策略对象本身无可变状态 ⇒ 天然线程安全。
/// </remarks>
public sealed class AllocateMessageQueueConsistentHash : IAllocateMessageQueueStrategy
{
    private readonly int _virtualNodeCnt;
    private readonly IHashFunction? _customHashFunction;

    /// <summary>对应 Java <c>#AllocateMessageQueueConsistentHash()</c>：默认 10 个虚拟节点。</summary>
    public AllocateMessageQueueConsistentHash() : this(10, null)
    {
    }

    public AllocateMessageQueueConsistentHash(int virtualNodeCnt) : this(virtualNodeCnt, null)
    {
    }

    /// <summary>
    /// 对应 Java <c>#AllocateMessageQueueConsistentHash(int, HashFunction)</c>。
    /// <paramref name="customHashFunction"/> 为 null 即 Java 的 null（环自带 MD5Hash）。
    /// 负数 <paramref name="virtualNodeCnt"/> → Java 抛 IllegalArgumentException，这里同抛
    /// <see cref="MQClientException"/>（构造期错误，不在 rebalance 后台路径上，
    /// 文件头差异 1 的「守卫返回空」口径不适用）。
    /// </summary>
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

    /// <summary>对应 Java 的 <c>virtualNodeCnt</c> 字段（只有 getter 语义，改它要重建策略）。</summary>
    public int VirtualNodeCnt => _virtualNodeCnt;

    public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll)
    {
        var result = new List<MessageQueue>();
        if (AllocateMessageQueueStrategyUtil.Check(consumerGroup, currentCid, mqAll, cidAll) < 0)
        {
            return result;
        }

        // 对齐 Java：cidAll 逐个包成 ClientNode 建环；customHashFunction 为 null 时用 MD5Hash。
        var cidNodes = new List<INode>(cidAll.Count);
        foreach (string cid in cidAll)
        {
            cidNodes.Add(new ClientNode(cid));
        }

        var router = new ConsistentHashRouter(cidNodes, _virtualNodeCnt, _customHashFunction);
        foreach (MessageQueue mq in mqAll)
        {
            // Java：`routeNode(mq.toString())` 再比 `currentCID.equals(clientNode.getKey())`。
            // null 分支真的会走到：cidAll 非空但虚拟节点数可以是 0 ⇒ 环空 ⇒ 谁都拿不到队列。
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

/// <summary>
/// 按机房分配：broker 名约定为 `<机房>@<brokerName>`，只有前缀在 <c>Consumeridcs</c> 白名单里的
/// 队列参与分配，然后在这批队列内部再切一次"平均"。
/// 对应 Java <c>AllocateMessageQueueByMachineRoom</c>（MACHINE_ROOM，类注释里的场景是
/// "Computer room Hashing queue algorithm, such as Alipay logic room"）。
/// </summary>
/// <remarks>
/// ⚠ broker 名用 <see cref="AllocateMessageQueueStrategyUtil.JavaSplit"/> 切，判据是 Java 的
/// 「裁掉尾空段后正好 2 段」。
/// ⚠ 分片算式与 <see cref="AllocateMessageQueueAveragely"/> **看着像但不同**：余数队列按
/// <c>rem &gt; currentIndex</c> 分给**前 rem 个**消费者，且每人区间长度恒为 mod。
/// </remarks>
public sealed class AllocateMessageQueueByMachineRoom : IAllocateMessageQueueStrategy
{
    private readonly object _lock = new();
    private HashSet<string> _consumeridcs = new();

    public AllocateMessageQueueByMachineRoom()
    {
    }

    /// <summary>对应 Python <c>AllocateMessageQueueByMachineRoom(consumeridcs={...})</c>。</summary>
    public AllocateMessageQueueByMachineRoom(IEnumerable<string> consumeridcs)
    {
        _consumeridcs = new HashSet<string>(consumeridcs, StringComparer.Ordinal);
    }

    /// <summary>对应 Java <c>#setConsumeridcs(Set)</c>：注册后仍可改，对后续 rebalance 生效。</summary>
    public void SetConsumeridcs(IEnumerable<string> consumeridcs)
    {
        lock (_lock)
        {
            _consumeridcs = new HashSet<string>(consumeridcs, StringComparer.Ordinal);
        }
    }

    /// <summary>对应 Java <c>#getConsumeridcs()</c>；返回副本，不交出共享可变引用。</summary>
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
        // Java: `String[] temp = mq.getBrokerName().split("@");`
        //       `if (temp.length == 2 && consumeridcs.contains(temp[0])) premqAll.add(mq);`
        var premqAll = new List<MessageQueue>();
        foreach (MessageQueue mq in mqAll)
        {
            string[] temp = AllocateMessageQueueStrategyUtil.JavaSplit(mq.BrokerName, '@');
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

/// <summary>
/// 对应 Java <c>AllocateMachineRoomNearby.MachineRoomResolver</c>：告诉策略「某个队列 / 某个
/// 客户端在哪个机房」。Java 接口注释明确写着返回值**不能为 null**；返回空串在这里等同，
/// 会抛 <see cref="MQClientException"/>（文件头差异 1 的例外条款）。
/// </summary>
public interface IMachineRoomResolver
{
    /// <summary>对应 Java <c>#brokerDeployIn(MessageQueue)</c>。</summary>
    string BrokerDeployIn(MessageQueue messageQueue);

    /// <summary>对应 Java <c>#consumerDeployIn(String clientID)</c>。</summary>
    string ConsumerDeployIn(string clientId);
}

/// <summary>
/// 机房就近代理策略：先把队列和消费者按机房分组，
///  1. 本消费者所在机房的队列只分给同机房的消费者（用内层策略算）；
///  2. 没有任何存活消费者的机房，其队列由所有机房的消费者按 cidAll 一起分（共享），
///     否则它们就没人消费了。
/// 因此**本消费者会拿到别机房的队列**，且同机房的队列排在结果前面（Java 就是这个拼接顺序）。
/// 对应 Java <c>AllocateMachineRoomNearby</c>（MACHINE_ROOM_NEARBY-&lt;内层名&gt;）。
/// </summary>
public sealed class AllocateMachineRoomNearby : IAllocateMessageQueueStrategy
{
    private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStrategy;
    private readonly IMachineRoomResolver _machineRoomResolver;
    private readonly string _name;

    /// <summary>
    /// 对应 Java 构造器：参数为 null 时 Java 抛 NullPointerException，这里抛
    /// <see cref="MQClientException"/> 且文案完全一致（文件头差异 2）。
    /// </summary>
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
        // Java 的 getName() 是现拼的；这里构造期算好（策略对象是不可变的）。
        _name = "MACHINE_ROOM_NEARBY" + "-" + allocateMessageQueueStrategy.GetName();
    }

    public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll)
    {
        if (AllocateMessageQueueStrategyUtil.Check(consumerGroup, currentCid, mqAll, cidAll) < 0)
        {
            return new List<MessageQueue>();
        }

        // 按机房分组。Java 用两个 TreeMap ⇒ 机房按**字典序**遍历，SortedDictionary 同序。
        var mr2Mq = new SortedDictionary<string, List<MessageQueue>>(StringComparer.Ordinal);
        foreach (MessageQueue mq in mqAll)
        {
            string brokerMachineRoom = _machineRoomResolver.BrokerDeployIn(mq);
            // Java `StringUtils.isNoneEmpty`：null 或空串都算「机房未知」⇒ 抛异常。
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

        // 1. 与本消费者同机房的队列只分给同机房的消费者。
        //    Java 是 `mr2Mq.remove(currentMachineRoom)` ⇒ 顺手把该机房从"待共享"集合里摘掉。
        string currentMachineRoom = _machineRoomResolver.ConsumerDeployIn(currentCid);
        bool hasRoomMq = mr2Mq.TryGetValue(currentMachineRoom, out List<MessageQueue>? mqInThisMachineRoom);
        if (hasRoomMq)
        {
            mr2Mq.Remove(currentMachineRoom);
        }

        mr2c.TryGetValue(currentMachineRoom, out List<string>? consumerInThisMachineRoom);
        // 守卫保证 currentCid ∈ cidAll，而 cidAll 已全部归进 mr2c ⇒ 必然命中；
        // 真为 null 时等价于 Java 传 null 给内层策略（内置策略都吃 null 列表里的空集合）。
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
