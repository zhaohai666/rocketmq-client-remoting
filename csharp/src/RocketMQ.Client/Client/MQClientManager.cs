// MQClientManager：同进程内按 clientId 复用 MQClientInstance（对应
// org.apache.rocketmq.client.impl.MQClientManager）。
//
// Java 的口径：producer/consumer/admin 各自 start() 时向 manager 要实例
// （getOrCreateMQClientInstance：clientId 命中即复用，未命中现建并放进 factoryTable），
// shutdown() 时归还；MQClientInstance#shutdown 只在 consumerTable/adminExtTable 为空、
// producerTable 不再有人用时才真正关停 —— 等价于这里的引用计数归零。复用的是
// **整个实例**：连接、路由缓存、路由/动态取址周期任务、消费统计采样全部共享，
// 同 clientId 的两个门面只有一份后台任务。
//
// 引用计数的判定必须原子（「减到 0 → 摘表 → 决定关停」不允许与并发的
// getOrCreate 交错：否则一边刚拿到实例、另一边已经把它关了），而 register/unregister
// 都是低频生命周期调用，所以直接用一把锁包住整张表，而不是
// ConcurrentDictionary + 分散的 Interlocked —— 后者把「判 0」和「摘表」拆开后
// 反而要再造第二把锁把两者缝回去。
//
// 与 Java 的差异（有意为之）：Java 的判据是三张注册表（producer/consumer/adminExt）
// 是否为空，靠门面各自 register/unregister 自己的组；C# 各门面没有可对齐的
// 「组 → 实例」注册表（一个门面对象就是一个注册项），故直接用引用计数表达同一条
// 语义。归还时的 UNREGISTER_CLIENT(35) 仍由各门面自己先发（对齐 Java 的
// unregisterProducer/unregisterConsumer → 私有 unregisterClient 的次序），这里只管
// 实例的生死。
using RocketMQ.Common;

namespace RocketMQ.Client;

public sealed class MQClientManager
{
    private static readonly MQClientManager Singleton = new();

    /// <summary>进程内单例（Java MQClientManager.getInstance()）。</summary>
    public static MQClientManager Instance => Singleton;

    private readonly object _gate = new();
    private readonly Dictionary<string, Entry> _factoryTable = new(StringComparer.Ordinal);

    private sealed class Entry(MQClientInstance instance)
    {
        internal MQClientInstance Instance { get; } = instance;
        internal int RefCount;
    }

    private MQClientManager()
    {
    }

    /// <summary>
    /// 对应 Java <c>getOrCreateMQClientInstance</c>：<paramref name="clientId"/> 命中缓存即
    /// 复用并引用计数 +1；未命中用 <paramref name="factory"/> 现建。创建参数取**第一个**
    /// 到达者的配置（Java 同：实例由首个 clientConfig 构造，后到者的超时/TLS 等实例级
    /// 参数不再生效），RPC 钩子的 first-wins 语义由
    /// <see cref="MQClientInstance.RegisterRpcHook"/> 单独保证。
    ///
    /// 返回后调用方须自行 <see cref="MQClientInstance.Start"/>（已启动的共享实例再调一次
    /// Start 是幂等的：周期任务都有「已在跑就不再起」的守卫），并在 Shutdown 时用
    /// <see cref="UnregisterMQClient"/> 归还。
    /// </summary>
    public MQClientInstance GetOrCreateMQClientInstance(string clientId, Func<MQClientInstance> factory)
    {
        if (string.IsNullOrEmpty(clientId))
        {
            throw new MQClientException("clientId is empty");
        }

        lock (_gate)
        {
            if (!_factoryTable.TryGetValue(clientId, out Entry? entry))
            {
                MQClientInstance created = factory()
                    ?? throw new MQClientException("MQClientInstance factory returned null");
                entry = new Entry(created);
                _factoryTable[clientId] = entry;
                ClientLog.Info("Created new MQClientInstance for clientId:[" + clientId + "]");
            }

            entry.RefCount++;
            return entry.Instance;
        }
    }

    /// <summary>
    /// 归还实例（对应 Java 门面 shutdown 末尾的 <c>mQClientFactory.shutdown()</c> +
    /// <c>removeClientFactory(clientId, this)</c>）：引用计数 -1，**归零才真正
    /// <see cref="MQClientInstance.Shutdown"/>** —— 先shutdown后摘表的次序反了会让并发
    /// 复用者拿到一个正在退场的实例。identity 不符（clientId 撞了旧表项）或表里已没有
    /// 该 clientId 时返回 false 且不动任何状态。
    ///
    /// 返回 true 表示本次调用真的触发了（或未来会触发）实例收尾：计数未归零时实例继续
    /// 存活，false 不代表实例被关闭。
    /// </summary>
    public bool UnregisterMQClient(string clientId, MQClientInstance? instance)
    {
        if (instance is null)
        {
            return false;
        }

        bool last;
        lock (_gate)
        {
            if (!_factoryTable.TryGetValue(clientId, out Entry? entry)
                || !ReferenceEquals(entry.Instance, instance))
            {
                return false;
            }

            last = --entry.RefCount <= 0;
            if (last)
            {
                _factoryTable.Remove(clientId);
            }
        }

        if (last)
        {
            // 在锁外关停：Shutdown 要 join 周期线程（各 2s 预算），抱着表锁会卡住
            // 并发的 getOrCreate。此刻表项已摘，后到者只会拿到全新实例。
            ClientLog.Info("the client factory [" + clientId + "] shutdown OK, refcount=0");
            instance.Shutdown();
        }

        return true;
    }

    // ---------------- 诊断 / 测试观测面 ----------------

    /// <summary>当前存活的实例数（引用计数归零的已摘除）。</summary>
    public int InstanceCount
    {
        get
        {
            lock (_gate)
            {
                return _factoryTable.Count;
            }
        }
    }

    /// <summary>某 clientId 当前被几个门面在用；不在表中返回 0。</summary>
    public int RefCountOf(string clientId)
    {
        lock (_gate)
        {
            return _factoryTable.TryGetValue(clientId, out Entry? entry) ? entry.RefCount : 0;
        }
    }

    /// <summary>取当前表项里的实例（诊断用；没人在用时返回 false）。</summary>
    public bool TryGetMQClientInstance(string clientId, out MQClientInstance? instance)
    {
        lock (_gate)
        {
            if (_factoryTable.TryGetValue(clientId, out Entry? entry))
            {
                instance = entry.Instance;
                return true;
            }
        }

        instance = null;
        return false;
    }
}
