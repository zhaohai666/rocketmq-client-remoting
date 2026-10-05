using System.Collections.Generic;
using System.Threading;
using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>
/// 发送延迟故障容错（对应 org.apache.rocketmq.client.latency.* 与 Python client/latency.py）。
///
/// 实现 MQFaultStrategy + LatencyFaultToleranceImpl（带 FaultItem）：追踪每个 broker 的
/// 发送延迟，延迟过高或发生异常时<b>隔离</b>一段时间（不分配给新消息），默认关闭。
///
/// 与 Java 关键点逐条对齐：
///   * latencyMax / notAvailableDuration 两套阈值表（逐字一致）；
///   * UpdateFaultItem 的 notAvailableDuration 取 ComputeNotAvailableDuration，
///     隔离（异常）场景固定按 10000ms 算档位；
///   * FaultItem.IsAvailable() = now &gt;= startTimestamp（隔离期未过则不可用）；
///   * LatencyFaultToleranceImpl.IsAvailable/IsReachable 在没有记录时返回 true，
///     即"从未出过问题的 broker 默认可用/可达"。
///
/// 有意差异（与 Python 参考实现一致）：省略 Java 的"后台可达性探测线程"（startDetector），
/// reachable 由 UpdateFaultItem 的调用方写入。默认 sendLatencyFaultEnable=false，开启后才记录与生效。
/// </summary>
public class FaultItem
{
    public FaultItem(string name)
    {
        Name = name;
    }

    public string Name { get; }

    public double CurrentLatency { get; set; }

    public long StartTimestamp { get; private set; }

    /// <summary>Java：only when now + dur &gt; startTimestamp 才更新（保持最长隔离期）。</summary>
    public void UpdateNotAvailableDuration(long notAvailableDurationMillis)
    {
        long now = UtilAll.CurrentTimeMillis();
        if (notAvailableDurationMillis > 0 && now + notAvailableDurationMillis > StartTimestamp)
        {
            StartTimestamp = now + notAvailableDurationMillis;
        }
    }

    public bool IsAvailable() => UtilAll.CurrentTimeMillis() >= StartTimestamp;

    public bool IsReachable() { return _reachableFlag; }

    public void SetReachable(bool reachable) { _reachableFlag = reachable; }

    private bool _reachableFlag = true;
}

/// <summary>
/// 对应 Java client.latency.LatencyFaultToleranceImpl（纯内存版，无探测线程）。
/// 表用锁保护；FaultItem 字段由表锁串行化访问。
/// </summary>
public class LatencyFaultToleranceImpl
{
    private readonly object _lock = new();
    private readonly Dictionary<string, FaultItem> _faultItemTable = new();

    public void UpdateFaultItem(string name, double currentLatency,
                                long notAvailableDurationMillis, bool reachable)
    {
        lock (_lock)
        {
            if (!_faultItemTable.TryGetValue(name, out FaultItem? item))
            {
                item = new FaultItem(name);
                _faultItemTable[name] = item;
            }
            item.CurrentLatency = currentLatency;
            item.UpdateNotAvailableDuration(notAvailableDurationMillis);
            item.SetReachable(reachable);
        }
    }

    public bool IsAvailable(string name)
    {
        FaultItem? item;
        lock (_lock)
        {
            _faultItemTable.TryGetValue(name, out item);
        }

        // 没有记录 = 从未出过问题，默认可用（与 Java/Python 一致）
        return item == null || item.IsAvailable();
    }

    public bool IsReachable(string name)
    {
        FaultItem? item;
        lock (_lock)
        {
            _faultItemTable.TryGetValue(name, out item);
        }

        return item == null || item.IsReachable();
    }

    public void Remove(string name)
    {
        lock (_lock)
        {
            _faultItemTable.Remove(name);
        }
    }

    /// <summary>供测试/诊断读取（不存在返回 null）。</summary>
    public FaultItem? GetFaultItem(string name)
    {
        lock (_lock)
        {
            return _faultItemTable.TryGetValue(name, out FaultItem? item) ? item : null;
        }
    }
}

/// <summary>
/// 对应 Java client.latency.MQFaultStrategy。
///
/// 仅当 SendLatencyFaultEnable 为 true 时，发送选队列阶段会：
///   1) 优先选 available（隔离期已过）的 broker；
///   2) 否则选 reachable 的 broker；
///   3) 否则退化为普通轮询。
/// 发送结果/异常会回调 UpdateFaultItem 写延迟与隔离信息。
/// </summary>
public class MQFaultStrategy
{
    // 两套阈值表（Java DefaultMQProducer 的默认值，逐字一致）
    public static readonly int[] LatencyMax = { 50, 100, 550, 1800, 3000, 5000, 15000 };
    public static readonly int[] NotAvailableDuration = { 0, 0, 2000, 5000, 6000, 10000, 30000 };

    private bool _sendLatencyFaultEnable;

    public MQFaultStrategy(bool sendLatencyFaultEnable = false)
    {
        _sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public bool IsSendLatencyFaultEnable() => _sendLatencyFaultEnable;

    public void SetSendLatencyFaultEnable(bool enable) { _sendLatencyFaultEnable = enable; }

    public LatencyFaultToleranceImpl LatencyFaultTolerance { get; } = new();

    /// <summary>
    /// 队列选择（对应 Python select_one_message_queue / Java 同名方法）。
    /// lastBrokerName 非空时尽量避开该 broker；enable=false 时退化为普通轮询。
    /// </summary>
    public MessageQueue SelectOneMessageQueue(TopicPublishInfo tpInfo,
                                              string? lastBrokerName,
                                              bool resetIndex = false)
    {
        if (_sendLatencyFaultEnable)
        {
            if (resetIndex)
            {
                tpInfo.ResetIndex();
            }

            // 1) available：隔离期已过的 broker
            MessageQueue? mq = tpInfo.SelectOneMessageQueue(
                q => LatencyFaultTolerance.IsAvailable(q.BrokerName),
                q => lastBrokerName == null || q.BrokerName != lastBrokerName);
            if (mq is not null)
            {
                return mq;
            }

            // 2) reachable：隔离中但通道仍可达的 broker
            mq = tpInfo.SelectOneMessageQueue(
                q => LatencyFaultTolerance.IsReachable(q.BrokerName),
                q => lastBrokerName == null || q.BrokerName != lastBrokerName);
            if (mq is not null)
            {
                return mq;
            }

            // 3) 全部被隔离且不可达：退化为普通轮询
            return tpInfo.SelectOneMessageQueue();
        }

        // 关闭时退化为普通轮询（lastBrokerName 非空时避开它；全部同名时该重载内部已兜底）
        return tpInfo.SelectOneMessageQueue(lastBrokerName ?? "");
    }

    /// <summary>
    /// 故障记录：isolation=true 时 latency 固定按 10000ms 算档位（→ 隔离 10000ms）。
    /// 未开启时直接忽略（与 Java/Python 一致）。
    /// </summary>
    public void UpdateFaultItem(string brokerName, double currentLatency,
                                bool isolation, bool reachable)
    {
        if (!_sendLatencyFaultEnable)
        {
            return;
        }

        double latency = isolation ? 10000.0 : currentLatency;
        long duration = ComputeNotAvailableDuration(latency);
        LatencyFaultTolerance.UpdateFaultItem(brokerName, currentLatency, duration, reachable);
    }

    /// <summary>从表尾向首找第一个 latency &gt;= LatencyMax[i] 的档位；都不满足返回 0。</summary>
    public long ComputeNotAvailableDuration(double currentLatency)
    {
        for (int i = LatencyMax.Length - 1; i >= 0; --i)
        {
            if (currentLatency >= LatencyMax[i])
            {
                return NotAvailableDuration[i];
            }
        }
        return 0;
    }
}
