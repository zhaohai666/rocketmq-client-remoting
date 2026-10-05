// 消费侧统计（对应 org.apache.rocketmq.client.stat.ConsumerStatsManager 与
// org.apache.rocketmq.common.stats.{StatsItem,StatsItemSet,StatsSnapshot}）。
//
// Java 真实模型（5.5.1 源码逐条核对，Python 参考实现有完整注释）：
// StatsItem 持有**累计值** value/times（只增不减），加两级采样快照链
// （每 10s 采分钟点、每 10 分钟采小时点）。快照计算 computeStatsData（StatsItem.java:53-79）：
//   sum   = last.value - first.value
//   tps   = sum * 1000.0 / (last.ts - first.ts)     // 每秒
//   avgpt = timesDiff > 0 ? sum / timesDiff : 0     // RT 项即平均耗时
//
// 实现差异（语义不变）：Java 给每个 StatsItem 单独排采样任务；这里由 manager 的
// **一个**采样线程统一巡采（10s 一轮，60 轮做一次小时级）——精度相同，线程省。
using System;
using System.Collections.Generic;
using System.Threading;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>对应 Java StatsSnapshot：sum / tps / avgpt / times。</summary>
public sealed class StatsSnapshot
{
    public long Sum { get; set; }
    public double Tps { get; set; }
    public double Avgpt { get; set; }
    public long Times { get; set; }
}

/// <summary>
/// Java StatsItem.computeStatsData（StatsItem.java:53-79）逐条照抄。
/// 采样点 = (timestampMs, 累计 value, 累计 times)。
/// </summary>
public static class StatsItems
{
    // 采样参数（Java StatsItem.init 的 scheduleAtFixedRate 参数）
    public const double SamplingIntervalSeconds = 10.0;
    public const int MinuteListMax = 60;   // ≈ 10 分钟窗口
    public const int HourListMax = 60;

    public static StatsSnapshot Compute(IReadOnlyList<(long ts, long value, long times)> csList)
    {
        var ss = new StatsSnapshot();
        if (csList.Count == 0)
        {
            return ss;
        }

        (long firstTs, long firstValue, long firstTimes) = csList[0];
        (long lastTs, long lastValue, long lastTimes) = csList[csList.Count - 1];
        ss.Sum = lastValue - firstValue;
        long spanMs = lastTs - firstTs;
        if (spanMs > 0)
        {
            ss.Tps = ss.Sum * 1000.0 / spanMs;
        }

        long timesDiff = lastTimes - firstTimes;
        ss.Times = timesDiff;
        if (timesDiff > 0)
        {
            ss.Avgpt = ss.Sum * 1.0 / timesDiff;
        }

        return ss;
    }
}

/// <summary>单项统计：累计 value/times + 分钟/小时两级采样链。</summary>
public sealed class StatsItem
{
    private readonly object _lock = new();
    private readonly List<(long ts, long value, long times)> _minute = new();
    private readonly List<(long ts, long value, long times)> _hour = new();
    private long _value;
    private long _times;

    internal StatsItem(string statsName, string statsKey)
    {
        StatsName = statsName;
        StatsKey = statsKey;
    }

    public string StatsName { get; }
    public string StatsKey { get; }

    public void AddValue(long incValue, long incTimes)
    {
        lock (_lock)
        {
            _value += incValue;
            _times += incTimes;
        }
    }

    public void Sample()
    {
        lock (_lock)
        {
            AppendMinute(UtilAll.CurrentTimeMillis(), _value, _times);
        }
    }

    public void SampleHour()
    {
        lock (_lock)
        {
            AppendHour(UtilAll.CurrentTimeMillis(), _value, _times);
        }
    }

    public StatsSnapshot GetStatsDataInMinute()
    {
        lock (_lock)
        {
            return StatsItems.Compute(_minute);
        }
    }

    public StatsSnapshot GetStatsDataInHour()
    {
        lock (_lock)
        {
            return StatsItems.Compute(_hour);
        }
    }

    public (long value, long times) Snapshot()
    {
        lock (_lock)
        {
            return (_value, _times);
        }
    }

    // 仅供测试：注入自定义时间戳的采样点（真实时钟两次 Sample 间隔≈0，tps 无从测起）。
    public void AppendSampleForTest(long tsMs, long value, long times)
    {
        lock (_lock)
        {
            AppendMinute(tsMs, value, times);
            AppendHour(tsMs, value, times);
        }
    }

    private void AppendMinute(long ts, long v, long t)
    {
        _minute.Add((ts, v, t));
        if (_minute.Count > StatsItems.MinuteListMax)
        {
            _minute.RemoveAt(0);
        }
    }

    private void AppendHour(long ts, long v, long t)
    {
        _hour.Add((ts, v, t));
        if (_hour.Count > StatsItems.HourListMax)
        {
            _hour.RemoveAt(0);
        }
    }
}

/// <summary>key -&gt; StatsItem（对应 Java StatsItemSet；key = topic@group）。</summary>
public sealed class StatsItemSet
{
    private readonly object _lock = new();
    private readonly Dictionary<string, StatsItem> _items = new(StringComparer.Ordinal);

    public StatsItemSet(string statsName)
    {
        StatsName = statsName;
    }

    public string StatsName { get; }

    public StatsItem GetAndCreate(string key)
    {
        lock (_lock)
        {
            if (!_items.TryGetValue(key, out StatsItem? item))
            {
                item = new StatsItem(StatsName, key);
                _items[key] = item;
            }

            return item;
        }
    }

    public StatsItem? Find(string key)
    {
        lock (_lock)
        {
            return _items.TryGetValue(key, out StatsItem? item) ? item : null;
        }
    }

    public void AddValue(string key, long incValue, long incTimes)
    {
        GetAndCreate(key).AddValue(incValue, incTimes);
    }

    public List<string> Keys()
    {
        lock (_lock)
        {
            return new List<string>(_items.Keys);
        }
    }

    public void SampleAll()
    {
        foreach (string key in Keys())
        {
            Find(key)?.Sample();
        }
    }

    public void SampleHourAll()
    {
        foreach (string key in Keys())
        {
            Find(key)?.SampleHour();
        }
    }
}

/// <summary>
/// 消费统计管理器（Java ConsumerStatsManager）。五个 StatsItemSet，key 一律 topic@group：
/// PULL_RT / PULL_TPS / CONSUME_RT / CONSUME_OK_TPS / CONSUME_FAILED_TPS。
/// Start() 起统一采样线程（10s 分钟级 + 每 60 轮即 10 分钟小时级）。
/// </summary>
public sealed class ConsumerStatsManager : IDisposable
{
    private readonly object _lock = new();
    private readonly StatsItemSet[] _sets;
    private Thread? _sampleThread;
    private volatile bool _stop;
    private bool _started;

    public ConsumerStatsManager()
    {
        TopicAndGroupPullRT = new StatsItemSet("PULL_RT");
        TopicAndGroupPullTPS = new StatsItemSet("PULL_TPS");
        TopicAndGroupConsumeRT = new StatsItemSet("CONSUME_RT");
        TopicAndGroupConsumeOKTPS = new StatsItemSet("CONSUME_OK_TPS");
        TopicAndGroupConsumeFailedTPS = new StatsItemSet("CONSUME_FAILED_TPS");
        _sets = new[]
        {
            TopicAndGroupPullRT, TopicAndGroupPullTPS, TopicAndGroupConsumeRT,
            TopicAndGroupConsumeOKTPS, TopicAndGroupConsumeFailedTPS
        };
    }

    public StatsItemSet TopicAndGroupPullRT { get; }
    public StatsItemSet TopicAndGroupPullTPS { get; }
    public StatsItemSet TopicAndGroupConsumeRT { get; }
    public StatsItemSet TopicAndGroupConsumeOKTPS { get; }
    public StatsItemSet TopicAndGroupConsumeFailedTPS { get; }

    public void Start()
    {
        // Java 的 Start() 是空实现（采样挂在每个 StatsItem 的调度器上）；这里收敛为
        // 一个统一采样线程，精度不变（10s）。
        lock (_lock)
        {
            if (_started)
            {
                return;
            }

            _started = true;
        }

        _stop = false;
        _sampleThread = new Thread(SampleLoop)
        {
            IsBackground = true,
            Name = "rmq-consumer-stats-sampler"
        };
        _sampleThread.Start();
    }

    public void Shutdown()
    {
        lock (_lock)
        {
            if (!_started)
            {
                return;
            }

            _started = false;
        }

        _stop = true;
        lock (_lock)
        {
            Monitor.PulseAll(_lock);   // 唤醒采样线程（不等完一个 10s 周期）
        }

        _sampleThread?.Join(3000);
        _sampleThread = null;
    }

    public void Dispose() => Shutdown();

    private void SampleLoop()
    {
        long rounds = 0;
        var interval = TimeSpan.FromSeconds(StatsItems.SamplingIntervalSeconds);
        while (true)
        {
            bool pulsed;
            lock (_lock)
            {
                // 到点或被 Shutdown 的 Pulse 唤醒；返回 true = 唤醒（去检查 _stop）
                pulsed = Monitor.Wait(_lock, interval);
            }

            if ((pulsed && _stop) || _stop)
            {
                return;
            }

            rounds++;
            foreach (StatsItemSet s in _sets)
            {
                s.SampleAll();
            }

            if (rounds % 60 == 0)   // 60 × 10s = 10 分钟
            {
                foreach (StatsItemSet s in _sets)
                {
                    s.SampleHourAll();
                }
            }
        }
    }

    // ---------------- 记数（Java ConsumerStatsManager 同名方法，参数顺序一致）----------------
    private static string Key(string topic, string group) => topic + "@" + group;

    public void IncPullRT(string group, string topic, long rt)
    {
        TopicAndGroupPullRT.AddValue(Key(topic, group), rt, 1);
    }

    public void IncPullTPS(string group, string topic, long msgs)
    {
        TopicAndGroupPullTPS.AddValue(Key(topic, group), msgs, 1);
    }

    public void IncConsumeRT(string group, string topic, long rt)
    {
        TopicAndGroupConsumeRT.AddValue(Key(topic, group), rt, 1);
    }

    public void IncConsumeOKTPS(string group, string topic, long msgs)
    {
        TopicAndGroupConsumeOKTPS.AddValue(Key(topic, group), msgs, 1);
    }

    public void IncConsumeFailedTPS(string group, string topic, long msgs)
    {
        TopicAndGroupConsumeFailedTPS.AddValue(Key(topic, group), msgs, 1);
    }

    // ---------------- 查询 ----------------
    /// <summary>
    /// Java ConsumerStatsManager.consumeStatus：全部取 minute 快照；
    /// consumeFailedMsgs 取 failed 的 **hour** 窗口 sum（Java 特意跨窗口，照抄）。
    /// </summary>
    public ConsumeStatus ConsumeStatus(string group, string topic)
    {
        var cs = new ConsumeStatus();
        string key = Key(topic, group);
        StatsItem? item = TopicAndGroupPullRT.Find(key);
        if (item != null)
        {
            cs.PullRT = item.GetStatsDataInMinute().Avgpt;
        }

        item = TopicAndGroupPullTPS.Find(key);
        if (item != null)
        {
            cs.PullTPS = item.GetStatsDataInMinute().Tps;
        }

        item = TopicAndGroupConsumeRT.Find(key);
        if (item != null)
        {
            cs.ConsumeRT = item.GetStatsDataInMinute().Avgpt;
        }

        item = TopicAndGroupConsumeOKTPS.Find(key);
        if (item != null)
        {
            cs.ConsumeOKTPS = item.GetStatsDataInMinute().Tps;
        }

        item = TopicAndGroupConsumeFailedTPS.Find(key);
        if (item != null)
        {
            cs.ConsumeFailedTPS = item.GetStatsDataInMinute().Tps;
            cs.ConsumeFailedMsgs = item.GetStatsDataInHour().Sum;
        }

        return cs;
    }
}
