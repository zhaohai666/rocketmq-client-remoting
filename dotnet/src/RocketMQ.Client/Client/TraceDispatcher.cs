// 异步轨迹分发器（对应 org.apache.rocketmq.client.trace.AsyncTraceDispatcher）。
//
// 职责：钩子把 TraceContext 丢进内存队列（Append），后台线程按
// 「攒够 batchNum 条 或 距上次发送超过 5s」两个条件触发刷写，再把编码后的文本
// 用**独立的内部生产者**发到轨迹 topic（默认 RMQ_SYS_TRACE_TOPIC）。
//
// 与 Java / Python 的对应关系：
//   * 队列          ← ArrayBlockingQueue<TraceContext>(2048)
//   * _flush        ← flushTraceContext（含 force_flush 语义）
//   * 分组发送       ← sendTraceData（按 topic + '\u0001' + traceTopic 分组）
//   * 切块           ← flushData（按 maxMessageSize 128K 切）
//   * 内部生产者组名  ← "_INNER_TRACE_PRODUCER-<group>-<PRODUCE|CONSUME>-<N>"
//
// **防递归**：内部生产者自身的 EnableTrace 必须为 false，且
// SendMessageTraceHook 会跳过 topic 以轨迹 topic 开头的消息 —— 两道保险都要有，
// 否则轨迹会自我复制到无限。
using System.Collections.Concurrent;
using System.Text;
using System.Threading;

using RocketMQ.Common;
using RocketMQ.Remoting;

namespace RocketMQ.Client;

/// <summary>对应 org.apache.rocketmq.client.trace.TraceDispatcher.Type。</summary>
public enum TraceDispatcherType
{
    Produce = 0,
    Consume = 1,
}

/// <summary>轨迹异步分发器（对应 AsyncTraceDispatcher）。</summary>
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
        _batchNum = Math.Min(batchNum, MaxBatch); // Java 注释明说最大 20
        _group = group;
        _type = type;
        _traceTopicName = traceTopicName ?? MixAll.TraceTopic;
        _traceProducer = CreateTraceProducer(rpcHook);
    }

    // ---------------- 内部生产者 ----------------
    private DefaultMQProducer CreateTraceProducer(IRpcHook? rpcHook)
    {
        string groupName = TraceConstants.GroupNamePrefix + "-" + _group + "-" + _type + "-"
                            + Interlocked.Increment(ref _counter);
        var producer = new DefaultMQProducer(groupName);
        // ACL 钩子透传：轨迹消息同样要能通过认证集群（null 表示宿主未开 ACL）
        if (rpcHook is not null)
        {
            producer.SetRpcHook(rpcHook);
        }

        producer.SendMsgTimeout = 5000;
        producer.MaxMessageSize = _maxMsgSize;
        // ⚠ 必须关闭自身的轨迹，否则轨迹消息会被再次追踪 → 无限递归
        producer.EnableTrace = false;
        return producer;
    }

    public string GetTraceTopicName() => _traceTopicName;

    /// <summary>当前待发送的轨迹条数（对应 Python 的 trace_context_queue.qsize()）。
    /// 队列满时 Append 会返回 false 并丢弃，这里是唯一的观测点。</summary>
    public int QueueSize => _queue.Count;

    public void SetHostProducer(object host) => _hostProducer = host;

    public void SetHostConsumer(object host) => _hostConsumer = host;

    /// <summary>宿主客户端的 clientId（EndTransaction 轨迹的 clientHost 用它）。</summary>
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

    // ---------------- 生命周期 ----------------
    public void Start(List<string> nameSrvAddr, AccessChannel accessChannel)
    {
        lock (_lock)
        {
            if (!_started)
            {
                _traceProducer.NamesrvAddr = string.Join(";", nameSrvAddr);
                _traceProducer.InstanceName = TraceConstants.TraceInstanceName + "_"
                                              + string.Join(";", nameSrvAddr);
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

    // ---------------- 入队 / 刷写 ----------------
    public bool Append(TraceContext ctx)
    {
        // 队列满时直接丢弃（与 Java 一致，不阻塞业务）；返回 false 让调用方记计数。
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

        // 防止忙等（Java Thread.sleep(5)）
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

    // ---------------- 发送 ----------------
    private void SendTraceData(List<TraceContext> contextList)
    {
        // 按 (业务 topic, 轨迹 topic) 分组后逐组发送（对应 Java sendTraceData）。
        var beanMap = new Dictionary<string, List<TraceTransferBean>>();
        foreach (TraceContext context in contextList)
        {
            AccessChannel ac = context.AccessChannel ?? _accessChannel;
            string regionId = context.RegionId;
            // region 为空或 bean 为空则跳过（与 Java 一致）
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
        // keys 里放的是**原始消息的 msgId**（不是 offsetMsgId），控制台按它反查轨迹
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
