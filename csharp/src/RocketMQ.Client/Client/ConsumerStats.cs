using System;
using System.Collections.Generic;
using System.Threading;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

public sealed class StatsSnapshot
{
    public long Sum { get; set; }
    public double Tps { get; set; }
    public double Avgpt { get; set; }
    public long Times { get; set; }
}

public static class StatsItems
{
    public const double SamplingIntervalSeconds = 10.0;
    public const int MinuteSampleIntervalMs = 10_000;
    public const int HourSampleIntervalMs = 600_000;
    public const int DaySampleIntervalMs = 3_600_000;
    public const int MinuteListMax = 7;
    public const int HourListMax = 7;
    public const int DayListMax = 25;

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

/// <summary>Single stats item: cumulative value/times plus minute/hour/day sampling chains.</summary>
public sealed class StatsItem
{
    private readonly object _lock = new();
    private readonly List<(long ts, long value, long times)> _minute = new();
    private readonly List<(long ts, long value, long times)> _hour = new();
    private readonly List<(long ts, long value, long times)> _day = new();
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

    public void SampleDay()
    {
        lock (_lock)
        {
            AppendDay(UtilAll.CurrentTimeMillis(), _value, _times);
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

    public StatsSnapshot GetStatsDataInDay()
    {
        lock (_lock)
        {
            return StatsItems.Compute(_day);
        }
    }

    public (long value, long times) Snapshot()
    {
        lock (_lock)
        {
            return (_value, _times);
        }
    }

    // Test-only: sample points with an injected timestamp (two real-clock Sample
    // calls are ~0 apart, so tps cannot be measured).
    public void AppendSampleForTest(long tsMs, long value, long times)
    {
        lock (_lock)
        {
            AppendMinute(tsMs, value, times);
            AppendHour(tsMs, value, times);
            AppendDay(tsMs, value, times);
        }
    }

    // Test-only: sample points with an injected timestamp.
    public void AppendDaySampleForTest(long tsMs, long value, long times)
    {
        lock (_lock)
        {
            AppendDay(tsMs, value, times);
        }
    }

    // The first point is a zero baseline one period back, so a fresh window's sum is the
    // cumulative delta rather than zero.
    private void AppendMinute(long ts, long v, long t)
    {
        if (_minute.Count == 0)
        {
            _minute.Add((ts - StatsItems.MinuteSampleIntervalMs, 0, 0));
        }

        _minute.Add((ts, v, t));
        if (_minute.Count > StatsItems.MinuteListMax)
        {
            _minute.RemoveAt(0);
        }
    }

    private void AppendHour(long ts, long v, long t)
    {
        if (_hour.Count == 0)
        {
            _hour.Add((ts - StatsItems.HourSampleIntervalMs, 0, 0));
        }

        _hour.Add((ts, v, t));
        if (_hour.Count > StatsItems.HourListMax)
        {
            _hour.RemoveAt(0);
        }
    }

    private void AppendDay(long ts, long v, long t)
    {
        if (_day.Count == 0)
        {
            _day.Add((ts - StatsItems.DaySampleIntervalMs, 0, 0));
        }

        _day.Add((ts, v, t));
        if (_day.Count > StatsItems.DayListMax)
        {
            _day.RemoveAt(0);
        }
    }
}

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

    public void SampleDayAll()
    {
        foreach (string key in Keys())
        {
            Find(key)?.SampleDay();
        }
    }
}

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
            Monitor.PulseAll(_lock);   // wake the sampler (don't wait for a full 10s period)
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
                // Woken when due or by Shutdown's Pulse; true = woken (go check _stop)
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

            if (rounds % 60 == 0)   // 60 x 10s = 10 minutes
            {
                foreach (StatsItemSet s in _sets)
                {
                    s.SampleHourAll();
                }

                if (rounds % 360 == 0)   // 6 x 10min = 1 hour
                {
                    foreach (StatsItemSet s in _sets)
                    {
                        s.SampleDayAll();
                    }
                }
            }
        }
    }

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
