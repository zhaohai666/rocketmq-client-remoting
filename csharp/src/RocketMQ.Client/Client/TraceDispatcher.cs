// Responsibility: hooks drop a TraceContext into an in-memory queue (Append); a background thread flushes when either
// "enough batchNum records accumulated OR more than 5s since last send" is met, then sends the encoded text
// with a **dedicated internal producer** to the trace topic (default RMQ_SYS_TRACE_TOPIC).
//
//
// **Recursion guard**: the internal producer's own EnableTrace must be false, and
// SendMessageTraceHook skips messages whose topic starts with the trace topic prefix — both safeguards are required,
// otherwise the trace would self-replicate without bound.
using System.Collections.Concurrent;
using System.Text;
using System.Threading;

using RocketMQ.Common;
using RocketMQ.Remoting;

namespace RocketMQ.Client;

public enum TraceDispatcherType
{
    Produce = 0,
    Consume = 1,
}

/// <summary>Async trace dispatcher (corresponds to AsyncTraceDispatcher).</summary>
public sealed class AsyncTraceDispatcher
{
    private static int _counter = 1;
    private static int _instanceNum;

    public const int WaitForShutdown = 5000;
    public const int FlushTraceInterval = 5000;
    private const int QueueCapacity = 2048;
    private const int MaxBatch = 20;

    private readonly int _batchNum;
    private readonly int _maxMsgSize = 128000;
    private readonly int _traceInstanceId = Interlocked.Increment(ref _instanceNum);
    private readonly string _group;
    private readonly TraceDispatcherType _type;
    private readonly BlockingCollection<TraceContext> _queue = new(QueueCapacity);
    private readonly object _lock = new();
    private readonly DefaultMQProducer _traceProducer;

    private string _traceTopicName;
    private object? _hostProducer;
    private object? _hostConsumer;
    private Thread? _worker;
    private volatile bool _stopped;
    private bool _started;
    private AccessChannel _accessChannel = AccessChannel.Local;
    private long _lastFlushTime = UtilAll.CurrentTimeMillis();

    public AsyncTraceDispatcher(string group, TraceDispatcherType type, int batchNum = 10,
        string? traceTopicName = null, IRpcHook? rpcHook = null)
    {
        _batchNum = Math.Min(batchNum, MaxBatch);
        _group = group;
        _type = type;
        _traceTopicName = traceTopicName ?? MixAll.TraceTopic;
        _traceProducer = CreateTraceProducer(rpcHook);
    }

    // ---------------- Internal producer ----------------
    private DefaultMQProducer CreateTraceProducer(IRpcHook? rpcHook)
    {
        string groupName = TraceConstants.GroupNamePrefix + "-" + _group + "-" + _type + "-"
                            + Interlocked.Increment(ref _counter);
        var producer = new DefaultMQProducer(groupName);
        // ACL hook passthrough: trace messages must also be able to pass through authenticated clusters (null means the host did not enable ACL)
        if (rpcHook is not null)
        {
            producer.SetRpcHook(rpcHook);
        }

        producer.SendMsgTimeout = 5000;
        producer.MaxMessageSize = _maxMsgSize;
        // WARNING: must turn off its own tracing, otherwise trace messages would be traced again -> infinite recursion
        producer.EnableTrace = false;
        return producer;
    }

    public string GetTraceTopicName() => _traceTopicName;

    public string NamespaceV2 { get; set; } = string.Empty;

    /// <summary>Number of trace records pending to be sent (the backlog of the trace queue).
    /// When the queue is full, Append returns false and drops the record; this is the only observation point.</summary>
    public int QueueSize => _queue.Count;

    public void SetHostProducer(object host) => _hostProducer = host;

    public void SetHostConsumer(object host) => _hostConsumer = host;

    /// <summary>The host client's clientId (used by the EndTransaction trace's clientHost).</summary>
    public string ClientId()
    {
        if (_hostProducer is DefaultMQProducer producer)
        {
            return producer.ClientId;
        }

        if (_hostConsumer is DefaultMQPushConsumer consumer)
        {
            return consumer.ClientId;
        }

        return string.Empty;
    }

    // ---------------- Lifecycle ----------------
    public void Start(List<string> nameSrvAddr, AccessChannel accessChannel)
    {
        lock (_lock)
        {
            if (!_started)
            {
                _traceProducer.NamesrvAddr = string.Join(";", nameSrvAddr);
                _traceProducer.InstanceName = TraceConstants.TraceInstanceName + "_"
                                              + string.Join(";", nameSrvAddr);
                _traceProducer.NamespaceV2 = NamespaceV2;
                _traceProducer.EnableTrace = false;
                _traceProducer.Start();
                _started = true;
            }
        }

        _accessChannel = accessChannel;
        if (_worker is null)
        {
            _stopped = false;
            _worker = new Thread(AsyncRun)
            {
                IsBackground = true,
                Name = "MQ-AsyncArrayDispatcher-Thread" + _traceInstanceId.ToString(
                    System.Globalization.CultureInfo.InvariantCulture),
            };
            _worker.Start();
        }
    }

    public void Shutdown()
    {
        try
        {
            Flush();
        }
        catch (Exception e)
        {
            ClientLog.Warn("trace dispatcher flush before shutdown failed: " + e.Message);
        }

        _stopped = true;
        if (_started)
        {
            try
            {
                _traceProducer.Shutdown();
            }
            catch (Exception e)
            {
                ClientLog.Warn("trace producer shutdown failed: " + e.Message);
            }
        }
    }

    // ---------------- Enqueue / flush ----------------
    public bool Append(TraceContext ctx)
    {
        return _queue.TryAdd(ctx);
    }

    public void Flush()
    {
        while (_queue.Count > 0)
        {
            try
            {
                FlushTraceContext(true);
            }
            catch (Exception e)
            {
                ClientLog.Warn("flushTraceContext error: " + e.Message);
            }
        }
    }

    private void AsyncRun()
    {
        while (!_stopped)
        {
            try
            {
                FlushTraceContext(false);
            }
            catch (Exception e)
            {
                ClientLog.Warn("flushTraceContext error: " + e.Message);
            }
        }
    }

    private void FlushTraceContext(bool forceFlush)
    {
        int size = _queue.Count;
        if (size != 0)
        {
            long now = UtilAll.CurrentTimeMillis();
            if (forceFlush || size >= _batchNum || (now - _lastFlushTime) > FlushTraceInterval)
            {
                var contextList = new List<TraceContext>(_batchNum);
                for (int i = 0; i < _batchNum; ++i)
                {
                    if (_queue.TryTake(out TraceContext? ctx, System.TimeSpan.Zero) && ctx is not null)
                    {
                        contextList.Add(ctx);
                    }
                    else
                    {
                        break;
                    }
                }

                AsyncSendTraceMessage(contextList);
                return;
            }
        }

        Thread.Sleep(5);
    }

    private void AsyncSendTraceMessage(List<TraceContext> contextList)
    {
        if (contextList.Count == 0)
        {
            return;
        }

        _lastFlushTime = UtilAll.CurrentTimeMillis();
        ThreadPool.QueueUserWorkItem(_ =>
        {
            try
            {
                SendTraceData(contextList);
            }
            catch (Exception e)
            {
                ClientLog.Warn("sendTraceData error: " + e.Message);
            }
        });
    }

    // ---------------- Send ----------------
    private void SendTraceData(List<TraceContext> contextList)
    {
        var beanMap = new Dictionary<string, List<TraceTransferBean>>();
        foreach (TraceContext context in contextList)
        {
            AccessChannel ac = context.AccessChannel ?? _accessChannel;
            string regionId = context.RegionId;
            if (string.IsNullOrEmpty(regionId) || context.TraceBeans.Count == 0)
            {
                continue;
            }

            string traceTopic = ac == AccessChannel.Cloud
                ? TraceConstants.TraceTopicPrefix + regionId
                : _traceTopicName;
            string topic = context.TraceBeans[0].Topic;
            string key = topic + TraceConstants.ContentSplitor + traceTopic;
            if (!beanMap.TryGetValue(key, out List<TraceTransferBean>? list))
            {
                list = new List<TraceTransferBean>();
                beanMap[key] = list;
            }

            TraceTransferBean? tb = TraceDataEncoder.EncoderFromContextBean(context);
            if (tb is not null)
            {
                list.Add(tb);
            }
        }

        foreach (KeyValuePair<string, List<TraceTransferBean>> kv in beanMap)
        {
            string[] parts = kv.Key.Split(new[] { TraceConstants.ContentSplitor }, StringSplitOptions.None);
            string topic = parts[0];
            string traceTopic = parts.Length > 1 ? parts[1] : _traceTopicName;
            FlushData(kv.Value, topic, traceTopic);
        }
    }

    private void FlushData(List<TraceTransferBean> transBeanList, string topic, string traceTopic)
    {
        if (transBeanList.Count == 0)
        {
            return;
        }

        var sb = new StringBuilder();
        var keySet = new HashSet<string>(StringComparer.Ordinal);
        int count = 0;
        foreach (TraceTransferBean bean in transBeanList)
        {
            foreach (string k in bean.TransKey)
            {
                keySet.Add(k);
            }

            sb.Append(bean.TransData);
            ++count;
            if (sb.Length >= _maxMsgSize)
            {
                SendTraceDataByMq(keySet, sb.ToString(), traceTopic);
                sb.Clear();
                keySet.Clear();
                count = 0;
            }
        }

        if (count > 0)
        {
            SendTraceDataByMq(keySet, sb.ToString(), traceTopic);
        }

        transBeanList.Clear();
    }

    private void SendTraceDataByMq(HashSet<string> keySet, string data, string traceTopic)
    {
        var msg = new Message(traceTopic, Encoding.UTF8.GetBytes(data));
        // keys holds the **original message's msgId** (not offsetMsgId); the console uses it to look up the trace
        var keys = new List<string>(keySet);
        keys.Sort(StringComparer.Ordinal);
        msg.Keys = string.Join(MessageConst.KeySeparator, keys);
        try
        {
            _traceProducer.Send(msg);
        }
        catch (Exception e)
        {
            ClientLog.Warn("send trace data failed: " + e.Message);
        }
    }
}
