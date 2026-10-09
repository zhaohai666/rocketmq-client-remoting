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
///     即"从未出过问题的 broker 默认可用/可达"；
///   * startDetector 可达性探测（<see cref="LatencyFaultToleranceImpl.StartDetector"/>）：
///     Java 把它跑在单线程 ScheduledExecutor（3s 首跳、3s 周期），每轮对容错表里的每个
///     FaultItem 按 checkStamp（detectInterval=2000ms 一档）用 ServiceDetector 探测，
///     探活成功即把 reachableFlag 置回 true。本端口的探测器是 TCP 连接（见
///     <see cref="TcpServiceDetector"/>），detectTimeout=200ms / detectInterval=2000ms
///     与 Java ClientConfig:82-83 的默认值逐字一致；开关 StartDetectorEnable 默认 false，
///     开启后隔离（RemotingException 路径）才把 reachable 置 false、由探测线程翻回来。
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

    /// <summary>
    /// Java FaultItem.checkStamp：下一轮探测的到期时刻（ms）。初始 0 ⇒ 首轮必探测；
    /// 每次探测完 +detectInterval。
    /// </summary>
    public long CheckStamp { get; set; }

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

/// <summary>对应 Java client.latency.Resolver：broker 名 → 地址（查不到返回 null）。</summary>
public delegate string? BrokerAddrResolver(string brokerName);

/// <summary>对应 Java client.latency.ServiceDetector：探测远端服务是否恢复。</summary>
public interface IServiceDetector
{
    bool Detect(string endpoint, long timeoutMillis);
}

/// <summary>
/// TCP 连接探测器：在 timeoutMillis 内完成一次建连即视为可达，探完立刻断开。
/// Java 内置的 ServiceDetector 是一发 getMaxOffset RPC（隐含"请求-响应全链路通"），
/// 本端口按任务口径用裸 TCP 建连——对「机器活着、端口开着」这一判定等价，且不依赖
/// 具体请求码。任何异常（含超时）都算不可达。
/// </summary>
public sealed class TcpServiceDetector : IServiceDetector
{
    public bool Detect(string endpoint, long timeoutMillis)
    {
        int colon = endpoint.LastIndexOf(':');
        if (colon <= 0 || colon == endpoint.Length - 1)
        {
            return false;
        }

        if (!int.TryParse(endpoint[(colon + 1)..], System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out int port) || port is <= 0 or > 65535)
        {
            return false;
        }

        try
        {
            using var probe = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            var task = probe.ConnectAsync(endpoint[..colon], port);
            if (!task.Wait(TimeSpan.FromMilliseconds(Math.Max(1, timeoutMillis))))
            {
                return false;
            }

            return probe.Connected;
        }
        catch (Exception)
        {
            return false;
        }
    }
}

/// <summary>
/// 对应 Java client.latency.LatencyFaultToleranceImpl。表用锁保护；FaultItem 字段由表锁
/// 串行化访问（CheckStamp 例外：只由探测线程写、阈值判断读，竞态无害）。
/// </summary>
public class LatencyFaultToleranceImpl
{
    private readonly object _lock = new();
    private readonly Dictionary<string, FaultItem> _faultItemTable = new();

    // Java ClientConfig:82-83 的默认值：detectTimeout=200、detectInterval=2000
    private int _detectTimeout = 200;
    private int _detectInterval = 2000;
    private volatile bool _startDetectorEnable;
    private BrokerAddrResolver? _resolver;
    private IServiceDetector? _serviceDetector;

    private Thread? _detectorThread;
    private readonly ManualResetEventSlim _detectorStop = new(false);
    private bool _detectorRunning;

    public LatencyFaultToleranceImpl(BrokerAddrResolver? resolver = null,
        IServiceDetector? serviceDetector = null)
    {
        _resolver = resolver;
        _serviceDetector = serviceDetector;
    }

    public int DetectTimeout
    {
        get => _detectTimeout;
        set => _detectTimeout = value;
    }

    public int DetectInterval
    {
        get => _detectInterval;
        set => _detectInterval = value;
    }

    public void SetResolver(BrokerAddrResolver? resolver) => _resolver = resolver;

    public void SetServiceDetector(IServiceDetector? detector) => _serviceDetector = detector;

    public bool IsStartDetectorEnable() => _startDetectorEnable;

    public void SetStartDetectorEnable(bool enable) => _startDetectorEnable = enable;

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

    // ---------------- 可达性探测（Java LatencyFaultToleranceImpl:60-103）----------------

    /// <summary>
    /// 一轮探测（Java detectByOneRound）：到期（now - checkStamp &gt;= 0）的 FaultItem 逐个
    /// 探测——resolver 拿不到地址就把条目摘掉（broker 已下线）；探测器探活成功且当前
    /// 标记不可达时把 reachableFlag 翻回 true（Java 同处打 info 日志）。
    /// checkStamp 在探测**之前**就 +detectInterval：探测挂死也不会把周期拖短。
    /// </summary>
    public void DetectByOneRound()
    {
        List<KeyValuePair<string, FaultItem>> snapshot;
        lock (_lock)
        {
            snapshot = new List<KeyValuePair<string, FaultItem>>(_faultItemTable);
        }

        long now = UtilAll.CurrentTimeMillis();
        foreach (KeyValuePair<string, FaultItem> kv in snapshot)
        {
            FaultItem brokerItem = kv.Value;
            if (now - brokerItem.CheckStamp < 0)
            {
                continue;
            }

            brokerItem.CheckStamp = UtilAll.CurrentTimeMillis() + _detectInterval;
            string? brokerAddr = _resolver?.Invoke(brokerItem.Name);
            if (brokerAddr is null)
            {
                lock (_lock)
                {
                    _faultItemTable.Remove(kv.Key);
                }

                continue;
            }

            if (_serviceDetector is null)
            {
                continue;
            }

            bool serviceOk = _serviceDetector.Detect(brokerAddr, _detectTimeout);
            if (serviceOk && !brokerItem.IsReachable())
            {
                ClientLog.Info(brokerItem.Name + " is reachable now, then it can be used.");
                brokerItem.SetReachable(true);
            }
        }
    }

    /// <summary>
    /// 对应 Java startDetector：单线程 3s 首跳 + 3s 固定周期，每轮只在 startDetectorEnable
    /// 为真时探测。重复调用幂等（Java 靠 executor 单例；这里靠 _detectorRunning 标记）。
    /// </summary>
    public void StartDetector()
    {
        lock (_lock)
        {
            if (_detectorRunning)
            {
                return;
            }

            _detectorRunning = true;
        }

        _detectorStop.Reset();
        _detectorThread = new Thread(DetectorLoop)
        {
            IsBackground = true,
            Name = "LatencyFaultToleranceScheduledThread",
        };
        _detectorThread.Start();
    }

    public void ShutdownDetector()
    {
        lock (_lock)
        {
            if (!_detectorRunning)
            {
                return;
            }

            _detectorRunning = false;
        }

        _detectorStop.Set();
        _detectorThread?.Join(2000);
        _detectorThread = null;
    }

    private void DetectorLoop()
    {
        // Java scheduleAtFixedRate(run, 3, 3, SECONDS)：首跳 3s、固定 3s。
        // 计划时刻锚定（Schedules.WaitUntil 的第二形态无停止事件重载，用 100ms 分片版）。
        long next = UtilAll.CurrentTimeMillis() + 3000;
        while (!_detectorStop.IsSet)
        {
            Schedules.WaitUntil(() => !_detectorStop.IsSet, next);
            if (_detectorStop.IsSet)
            {
                return;
            }

            try
            {
                if (_startDetectorEnable)
                {
                    DetectByOneRound();
                }
            }
            catch (Exception e)
            {
                ClientLog.Warn("unexpected exception raised while detecting service reachability: "
                               + e.Message);
            }

            next += 3000;
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
    private bool _startDetectorEnable;

    public MQFaultStrategy(bool sendLatencyFaultEnable = false)
    {
        _sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public bool IsSendLatencyFaultEnable() => _sendLatencyFaultEnable;

    public void SetSendLatencyFaultEnable(bool enable) { _sendLatencyFaultEnable = enable; }

    public bool IsStartDetectorEnable() => _startDetectorEnable;

    /// <summary>同步把开关传导给容错表（Java setStartDetectorEnable 同款）。</summary>
    public void SetStartDetectorEnable(bool enable)
    {
        _startDetectorEnable = enable;
        LatencyFaultTolerance.SetStartDetectorEnable(enable);
    }

    /// <summary>起探测线程（Java MQFaultStrategy.startDetector → LatencyFaultToleranceImpl）。
    /// 每轮只在 <see cref="IsStartDetectorEnable"/> 为真时探测，开关运行时可随时翻转。</summary>
    public void StartDetector() => LatencyFaultTolerance.StartDetector();

    public void Shutdown() => LatencyFaultTolerance.ShutdownDetector();

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
