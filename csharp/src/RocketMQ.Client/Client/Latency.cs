using System.Collections.Generic;
using System.Threading;
using RocketMQ.Common;

namespace RocketMQ.Client;

/// Implements MQFaultStrategy + LatencyFaultToleranceImpl (with FaultItem): tracks each broker's
/// send latency; when latency is too high or an exception occurs the broker is <b>isolated</b>
/// for a while (not assigned to new messages). Disabled by default.
public class FaultItem
{
    public FaultItem(string name)
    {
        Name = name;
    }

    public string Name { get; }

    public double CurrentLatency { get; set; }

    public long StartTimestamp { get; private set; }

    public long CheckStamp { get; set; }

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

public delegate string? BrokerAddrResolver(string brokerName);

public interface IServiceDetector
{
    bool Detect(string endpoint, long timeoutMillis);
}

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

public class LatencyFaultToleranceImpl
{
    private readonly object _lock = new();
    private readonly Dictionary<string, FaultItem> _faultItemTable = new();

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

    /// <summary>Read for tests/diagnostics (returns null when absent).</summary>
    public FaultItem? GetFaultItem(string name)
    {
        lock (_lock)
        {
            return _faultItemTable.TryGetValue(name, out FaultItem? item) ? item : null;
        }
    }


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

/// Only when SendLatencyFaultEnable is true does the send queue-selection phase:
///   1) prefer an available broker (isolation period expired);
///   2) otherwise pick a reachable broker;
///   3) otherwise fall back to plain round-robin.
/// Send results/exceptions call back into UpdateFaultItem to record latency and isolation info.
/// </summary>
public class MQFaultStrategy
{
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

    public void SetStartDetectorEnable(bool enable)
    {
        _startDetectorEnable = enable;
        LatencyFaultTolerance.SetStartDetectorEnable(enable);
    }

    public void StartDetector() => LatencyFaultTolerance.StartDetector();

    public void Shutdown() => LatencyFaultTolerance.ShutdownDetector();

    public LatencyFaultToleranceImpl LatencyFaultTolerance { get; } = new();

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

            // 1) available: brokers whose isolation period has expired
            MessageQueue? mq = tpInfo.SelectOneMessageQueue(
                q => LatencyFaultTolerance.IsAvailable(q.BrokerName),
                q => lastBrokerName == null || q.BrokerName != lastBrokerName);
            if (mq is not null)
            {
                return mq;
            }

            // 2) reachable: brokers that are isolated but whose channel is still reachable
            mq = tpInfo.SelectOneMessageQueue(
                q => LatencyFaultTolerance.IsReachable(q.BrokerName),
                q => lastBrokerName == null || q.BrokerName != lastBrokerName);
            if (mq is not null)
            {
                return mq;
            }

            // 3) all isolated and unreachable: fall back to plain round-robin
            return tpInfo.SelectOneMessageQueue();
        }

        // When disabled, falls back to plain round-robin (avoiding lastBrokerName
        // when non-null; that overload already handles the all-same-name case)
        return tpInfo.SelectOneMessageQueue(lastBrokerName ?? "");
    }

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

    /// <summary>Finds, scanning from the tail toward the head, the first tier with latency &gt;= LatencyMax[i]; returns 0 if none matches.</summary>
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
