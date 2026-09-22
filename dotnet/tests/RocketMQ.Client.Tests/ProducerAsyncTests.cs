// 异步发送内核（Java DefaultMQProducerImpl.sendDefaultImpl 的 ASYNC 分支 +
// MQClientAPIImpl.sendMessageAsync/onExceptionImpl）的离线对拍。
//
// 对端实现：python/tests/test_producer_async.py（38 项）、
// cpp/tests/test_producer_async.cpp、rust/src/client/producer/send_retry_tests.rs。
// 这里按 .NET 能观察到的证据取样：假集群（MockCluster.cs）能看见**上线的报文**，
// 日志能看见**重试链**，两者合起来足够钉死「调用方立刻返回 / 回调恰好一次 /
// 异步不看 retryResponseCodes / 预算共享 / 闸门与队满怎么拒绝」这几条不变式。
//
// ⚠ .NET 的传输层照 Java/Netty 的口径实现：对端关连接**不**提前惊动在途请求，
// 只能等自己的响应超时。而一笔「等到超时」的尝试必然把共享预算吃光，于是
// 「同一台 broker 上换 opaque 重试」在进程内造不出来（C++/Rust 的端口能在
// 连接断掉时立刻失败，所以它们有那条断言）。这里改成断言**重试换了哪些 broker**
// （日志取证）与**并发发送彼此不串台**（opaque 取证）。
using System.Diagnostics;
using System.Globalization;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ProducerAsyncTests : IDisposable
{
    private const string Topic = "T1";

    private readonly string _dir;

    public ProducerAsyncTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rmq_dotnet_async_"
                                           + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        // ClientLog 全程持有写句柄，先把日志指向别处再删临时目录
        ClientLog.SetLogFile(Path.Combine(_dir, "detached.log"));
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (Exception)
        {
            // 清理失败不影响测试
        }
    }

    // ---------------------------------------------------------------- 夹具

    /// <summary>
    /// 起一个已启动的生产者。<paramref name="senderPool"/> 必须在 <c>Start()</c> **之前**挂上
    /// （池在 Start 里定，对应 Java setAsyncSenderExecutor 的用法），所以这里一并传入。
    /// </summary>
    private static DefaultMQProducer Started(MockCluster cluster, string group,
        ConsumeExecutor? senderPool = null)
    {
        var producer = new DefaultMQProducer(group)
        {
            NamesrvAddr = cluster.NamesrvAddr,
            InstanceName = group,
            AsyncSenderExecutor = senderPool,
        };
        producer.Start();
        return producer;
    }

    private static Message Msg(int bodyLen = 10) =>
        new(Topic, new byte[Math.Max(0, bodyLen)]);

    /// <summary>轮询等待条件成立（回调是跨线程的，不能假设它已经跑完）。</summary>
    private static bool WaitUntil(Func<bool> cond, int millis)
    {
        var watch = Stopwatch.StartNew();
        while (watch.ElapsedMilliseconds < millis)
        {
            if (cond())
            {
                return true;
            }

            Thread.Sleep(5);
        }

        return cond();
    }

    /// <summary>记录每一笔终态回调（次数、结果/异常文本、跑在哪根线程上）。</summary>
    private sealed class Recorder : ISendCallback
    {
        private readonly object _gate = new();
        private readonly List<SendResult?> _results = new();
        private readonly List<string> _errors = new();
        private readonly List<int> _threads = new();
        private readonly List<string> _threadNames = new();

        public int Count
        {
            get
            {
                lock (_gate)
                {
                    return _results.Count + _errors.Count;
                }
            }
        }

        public bool WaitDone(int n, int millis) => WaitUntil(() => Count >= n, millis);

        public List<SendResult?> Results()
        {
            lock (_gate)
            {
                return new List<SendResult?>(_results);
            }
        }

        public List<string> Errors()
        {
            lock (_gate)
            {
                return new List<string>(_errors);
            }
        }

        public (int ThreadId, string ThreadName) LastThread()
        {
            lock (_gate)
            {
                return _threads.Count == 0
                    ? (-1, string.Empty)
                    : (_threads[^1], _threadNames[^1]);
            }
        }

        public void OnSuccess(SendResult sendResult)
        {
            lock (_gate)
            {
                _results.Add(sendResult);
                _threads.Add(Environment.CurrentManagedThreadId);
                _threadNames.Add(Thread.CurrentThread.Name ?? string.Empty);
            }
        }

        public void OnException(string error)
        {
            lock (_gate)
            {
                _errors.Add(error);
                _threads.Add(Environment.CurrentManagedThreadId);
                _threadNames.Add(Thread.CurrentThread.Name ?? string.Empty);
            }
        }
    }

    /// <summary>数 before/after 各跑了几次，并记下 after 有没有看到结果/异常。
    /// 可选「在 before 里park 一段时间」，用来把池的 worker 占住（真集群造不出这段时长）。</summary>
    private sealed class CountingSendHook : ISendMessageHook
    {
        private readonly int _parkMillis;
        private int _before;
        private int _after;
        private int _sawResult;
        private int _sawException;
        private string _beforeThreadName = string.Empty;

        public CountingSendHook(int parkMillis = 0) => _parkMillis = parkMillis;

        public int Before() => Volatile.Read(ref _before);

        public int After() => Volatile.Read(ref _after);

        public string BeforeThreadName() => Volatile.Read(ref _beforeThreadName);

        public bool SawResult() => Volatile.Read(ref _sawResult) > 0;

        public bool SawException() => Volatile.Read(ref _sawException) > 0;

        public string HookName() => "counting";

        public void SendMessageBefore(SendMessageContext context)
        {
            Interlocked.Increment(ref _before);
            _beforeThreadName = Thread.CurrentThread.Name ?? string.Empty;
            if (_parkMillis > 0)
            {
                Thread.Sleep(_parkMillis);
            }
        }

        public void SendMessageAfter(SendMessageContext context)
        {
            Interlocked.Increment(ref _after);
            if (context.SendResult is not null)
            {
                Interlocked.Increment(ref _sawResult);
            }

            if (context.Exception is not null)
            {
                Interlocked.Increment(ref _sawException);
            }
        }
    }

    /// <summary>拦截钩子：记录它看到的 CommunicationMode，可顺带睡一段时间（把准备段的
    /// 预算花光，用来验 sendKernelImpl 自己的那道闸），也可以直接拒绝某个标签。</summary>
    private sealed class ForbiddenHook : ICheckForbiddenHook
    {
        private readonly int _sleepMillis;
        private int _calls;
        private volatile CommunicationMode _mode = CommunicationMode.Sync;

        public ForbiddenHook(int sleepMillis = 0) => _sleepMillis = sleepMillis;

        public int Calls() => Volatile.Read(ref _calls);

        public CommunicationMode Mode() => _mode;

        public string HookName() => "forbidden";

        public void CheckForbidden(CheckForbiddenContext context)
        {
            Interlocked.Increment(ref _calls);
            _mode = context.CommunicationMode;
            if (_sleepMillis > 0)
            {
                // 钩子接口是同步的（Java 也是），睡的就是预算用的那把挂钟
                Thread.Sleep(_sleepMillis);
            }

            string tags = context.Message?.Tags ?? string.Empty;
            if (tags == "Forbidden")
            {
                throw new MQClientException("offline test: tag Forbidden is not allowed");
            }
        }
    }

    /// <summary>取本轮写进日志文件里的「异步重试」行数（Java 的那条 warn 是重试链唯一
    /// 能被外部看到的证据：它带着第几次、换到了哪台 broker）。</summary>
    private string CaptureLog(string tag)
    {
        string path = Path.Combine(_dir, tag + ".log");
        ClientLog.SetLogFile(path);
        ClientLog.FlushLogFile();
        return path;
    }

    /// <summary>ClientLog 握着写句柄，必须像 tail -f 那样以 ReadWrite 共享打开。</summary>
    private static List<string> RetryLines(string path, string topic)
    {
        ClientLog.FlushLogFile();
        var lines = new List<string>();
        using (FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        using (StreamReader sr = new(fs))
        {
            string? line;
            while ((line = sr.ReadLine()) is not null)
            {
                if (line.Contains("async send msg by retry", StringComparison.Ordinal)
                    && line.Contains("topic=" + topic, StringComparison.Ordinal))
                {
                    lines.Add(line);
                }
            }
        }

        return lines;
    }

    /// <summary>数日志里含某个片段的行数（ClientLog 握着写句柄，按 ReadWrite 共享打开）。</summary>
    private static int CountLines(string path, string needle)
    {
        ClientLog.FlushLogFile();
        int n = 0;
        using (FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        using (StreamReader sr = new(fs))
        {
            string? line;
            while ((line = sr.ReadLine()) is not null)
            {
                if (line.Contains(needle, StringComparison.Ordinal)) n++;
            }
        }

        return n;
    }

    /// <summary>从重试日志行里抠出 brokerName=（判「换没换 broker」）。</summary>
    private static string FieldOf(string line, string marker)
    {
        int from = line.IndexOf(marker, StringComparison.Ordinal);
        if (from < 0)
        {
            return string.Empty;
        }

        int start = from + marker.Length;
        int end = line.IndexOfAny(new[] { ',', ':', ' ' }, start);
        return end < 0 ? line[start..] : line[start..end];
    }

    // ---------------------------------------------------------------- 调用方不被阻塞

    /// <summary>
    /// 异步发送的**第一不变式**：调用方立刻返回，准备段跑在 AsyncSenderExecutor_N 上，
    /// 用户回调跑在 NettyClientPublicExecutor_N 上，且**恰好一次**。
    /// </summary>
    [Fact]
    public void SendAsync_ReturnsBeforeTheRequestIsIssued()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        var hooks = new CountingSendHook(parkMillis: 600);
        DefaultMQProducer producer = Started(cluster, "GID_async_nonblocking");
        producer.RegisterSendMessageHook(hooks);
        int callerThread = Environment.CurrentManagedThreadId;

        var cb = new Recorder();
        var watch = Stopwatch.StartNew();
        producer.SendAsync(Msg(), cb, 5000);
        watch.Stop();
        // 钩子还要睡 600ms：调用方若被拖住，这里就不会远小于它
        Assert.True(watch.ElapsedMilliseconds < 500,
            "调用方必须立刻返回，elapsed=" + watch.ElapsedMilliseconds);
        Assert.Equal(0, cb.Count);
        Assert.Equal(0, cluster.Requests(0));
        Assert.True(WaitUntil(() => hooks.Before() >= 1, 3000), "准备段要跑起来");
        Assert.True(hooks.BeforeThreadName().StartsWith("AsyncSenderExecutor_", StringComparison.Ordinal),
            "准备段要在自己的池里跑，实际线程名=" + hooks.BeforeThreadName());
        Assert.True(cb.WaitDone(1, 5000));
        Assert.Equal(1, cb.Count);
        Assert.Empty(cb.Errors());
        Assert.Equal(SendStatus.SendOk, cb.Results()[0]!.SendStatus);
        (int threadId, string threadName) = cb.LastThread();
        Assert.NotEqual(callerThread, threadId);
        Assert.True(threadName.StartsWith("NettyClientPublicExecutor_", StringComparison.Ordinal),
            "用户回调不能在发送池或读线程上跑（Java executeInvokeCallback），实际=" + threadName);
        Assert.Equal(1, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>每笔异步发送只跑**一次** before/after，且 after 看得到 SendResult。</summary>
    [Fact]
    public void AsyncSuccess_RunsEachHookOnce_AndSeesTheResult()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        var hooks = new CountingSendHook();
        DefaultMQProducer producer = Started(cluster, "GID_async_hooks");
        producer.RegisterSendMessageHook(hooks);

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 3000);
        Assert.True(cb.WaitDone(1, 3000));
        Assert.Single(cb.Results());
        SendResult result = cb.Results()[0]!;
        // MsgId 取客户端自己补的 UNIQ_KEY（32 位十六进制），OffsetMsgId 才是 broker 回的那份
        Assert.Equal(32, result.MsgId.Length);
        Assert.NotEqual(result.MsgId, result.OffsetMsgId);
        Assert.Equal("MOCK-0-1", result.OffsetMsgId);
        Assert.Equal(1, hooks.Before());
        Assert.Equal(1, hooks.After());
        Assert.True(hooks.SawResult(), "after 钩子要看到 sendResult");
        Assert.False(hooks.SawException());
        producer.Shutdown();
    }

    /// <summary>timeout=0：排队还没结束预算就没了 —— 一笔都不许上线。</summary>
    [Fact]
    public void ZeroTimeout_FailsWithoutSendingAnything()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_async_zero_timeout");

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 0);
        Assert.True(cb.WaitDone(1, 3000));
        Assert.Single(cb.Errors());
        Assert.Contains("DEFAULT ASYNC send call timeout", cb.Errors()[0]);
        Assert.Equal(0, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>
    /// 未 Start / 已 Shutdown：同步抛给调用方（Java 走回调，那边问题更难查），回调一次都不跑。
    /// </summary>
    [Fact]
    public void OutsideTheProducerLifecycle_ThrowsAtTheCaller()
    {
        using var cluster = MockCluster.Start(1);
        var unstarted = new DefaultMQProducer("GID_async_unstarted")
        {
            NamesrvAddr = cluster.NamesrvAddr,
            InstanceName = "GID_async_unstarted",
        };
        var cb = new Recorder();
        MQClientException before = Assert.Throws<MQClientException>(
            () => unstarted.SendAsync(Msg(), cb, 1000));
        Assert.Contains("producer not started", before.Message);

        DefaultMQProducer producer = Started(cluster, "GID_async_after_shutdown");
        producer.SendAsync(Msg(), cb, 3000);
        Assert.True(cb.WaitDone(1, 3000));
        producer.Shutdown();
        MQClientException after = Assert.Throws<MQClientException>(
            () => producer.SendAsync(Msg(), cb, 1000));
        // 关池之后分得清「没启动」和「已经关掉」（Java 的 CREATE_JUST / SHUTDOWN_ALREADY）
        Assert.Contains("producer already shutdown", after.Message);
        Assert.Equal(1, cb.Count);
    }

    /// <summary>用户回调自己抛异常不能带走回调池的 worker（Java 两处都 catch Throwable）。</summary>
    [Fact]
    public void UserCallbackException_KeepsThePoolAlive()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_async_callback_throws");

        var boom = new ThrowingCallback();
        producer.SendAsync(Msg(), boom, 3000);
        Assert.True(WaitUntil(() => Volatile.Read(ref boom.Calls) >= 1, 3000));

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 3000);
        Assert.True(cb.WaitDone(1, 3000), "池子还活着才能收到第二笔的回调");
        Assert.Single(cb.Results());
        producer.Shutdown();
    }

    private sealed class ThrowingCallback : ISendCallback
    {
        public int Calls;

        public void OnSuccess(SendResult sendResult)
        {
            Interlocked.Increment(ref Calls);
            throw new InvalidOperationException("callback body blew up");
        }

        public void OnException(string error)
        {
            Interlocked.Increment(ref Calls);
            throw new InvalidOperationException("callback body blew up");
        }
    }

    // ---------------------------------------------------------------- 重试链

    /// <summary>
    /// **异步发送不看 RetryResponseCodes**：broker 明确回了错就原样交给回调，一次尝试结束
    /// （同一个码在同步路径里会重试满 3 次，见 SendRetryTests）。
    /// </summary>
    [Fact]
    public void BrokerRejectedCode_EndsTheChainAfterOneAttempt()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.SystemError, 0));
        cluster.Script(1, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_async_broker_code");
        producer.SendLatencyFaultEnable = true;
        Assert.True(producer.IsRetryResponseCode(ResponseCode.SystemError),
            "这个码在同步路径里是可重试的");

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 3000);
        Assert.True(cb.WaitDone(1, 3000));
        Assert.Single(cb.Errors());
        Assert.Contains("CODE:", cb.Errors()[0]);
        Assert.Equal(1, cluster.Requests(0));
        Assert.True(cluster.Requests(1) == 0, "异步链不换 broker（Java needRetry=false）");
        FaultItem? item = producer.MqFaultStrategy.LatencyFaultTolerance
            .GetFaultItem(MockCluster.BrokerName(0));
        Assert.NotNull(item);
        Assert.False(item!.IsAvailable(), "回了错误的 broker 要被隔离");
        // 响应是 broker 给的 ⇒ 链路是通的，只隔离不判不可达（Java 异步传 reachable=true）
        Assert.True(item.IsReachable());
        producer.Shutdown();
    }

    /// <summary>
    /// 超时预算是**所有尝试共享**的：一笔等到超时就吃光了剩余时间，链必须停在这里，
    /// 不许再换 broker 试第二台（Java 往下传的是 timeoutMillis - costTime）。
    /// </summary>
    [Fact]
    public void Timeout_SharesOneBudgetAcrossAttempts()
    {
        using var cluster = MockCluster.Start(2);
        cluster.MakeSilent(0);
        cluster.Script(1, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_async_shared_budget");
        Assert.Equal(2, producer.RetryTimesWhenSendAsyncFailed);

        var watch = Stopwatch.StartNew();
        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 800);
        Assert.True(cb.WaitDone(1, 5000));
        long elapsed = watch.ElapsedMilliseconds;
        Assert.Single(cb.Errors());
        Assert.Contains("wait response timeout, cost=", cb.Errors()[0]);
        Assert.Equal(1, cluster.Requests(0));
        Assert.True(cluster.Requests(1) == 0, "预算没了就不该有第二次尝试");
        // 超时的判分在清理线程上（Java 的 scanResponseTable 每轮 1s），只卡下界
        Assert.True(elapsed >= 700, "等满了一次请求超时，elapsed=" + elapsed);
        producer.Shutdown();
    }

    /// <summary>
    /// 建连失败（Java 的 RemotingConnectException）算「没收到响应」：快、可重试，
    /// 按 RetryTimesWhenSendAsyncFailed 试满，且每次**避开刚失败的那台**。
    /// </summary>
    [Fact]
    public void ConnectFailure_RetriesOntoAnotherBroker_AndAvoidsTheFailedOne()
    {
        string topic = "AsyncConnectRetry";
        using var cluster = MockCluster.StartAt(
            new List<string?> { MockCluster.DeadAddr(), MockCluster.DeadAddr() });
        string log = CaptureLog("connect_retry");
        DefaultMQProducer producer = Started(cluster, "GID_async_connect_retry");

        var watch = Stopwatch.StartNew();
        var cb = new Recorder();
        producer.SendAsync(new Message(topic, new byte[10]), cb, 8000);
        Assert.True(cb.WaitDone(1, 5000));
        long elapsed = watch.ElapsedMilliseconds;
        Assert.Single(cb.Errors());
        // 外层 catch 的口径：建连失败**原样**抛出，不包装成 "unknown reason"
        Assert.Contains("connect failed to", cb.Errors()[0]);
        Assert.DoesNotContain("last error", cb.Errors()[0]);
        // 三次建连各在毫秒级失败：慢下来就是在等超时，说明重试判据写错了
        Assert.True(elapsed < 2000, "建连级失败不该吃掉预算，elapsed=" + elapsed);

        List<string> lines = RetryLines(log, topic);
        Assert.Equal(2, lines.Count);
        Assert.Equal("1", FieldOf(lines[0], "by retry "));
        Assert.Equal("2", FieldOf(lines[1], "by retry "));
        string first = FieldOf(lines[0], "brokerName=");
        string second = FieldOf(lines[1], "brokerName=");
        Assert.NotEqual(first, second);
        Assert.Contains(MockCluster.BrokerName(1), first + second);
        producer.Shutdown();
    }

    /// <summary>换到的那台是真 broker：链路恢复，回调拿到 SEND_OK，只重试了一次。</summary>
    [Fact]
    public void ConnectFailure_RecoversOnAnotherBroker()
    {
        string topic = "AsyncRecover";
        using var cluster = MockCluster.StartAt(
            new List<string?> { MockCluster.DeadAddr(), null });
        cluster.Script(1, new List<(int, int)>(), (ResponseCode.Success, 0));
        string log = CaptureLog("recover");
        DefaultMQProducer producer = Started(cluster, "GID_async_recover");
        producer.SendLatencyFaultEnable = true;

        var cb = new Recorder();
        producer.SendAsync(new Message(topic, new byte[10]), cb, 8000);
        Assert.True(cb.WaitDone(1, 5000));
        Assert.Single(cb.Results());
        Assert.Equal(SendStatus.SendOk, cb.Results()[0]!.SendStatus);
        Assert.True(cluster.Requests(1) == 1, "只有活着的那台收到了消息");
        Assert.Single(RetryLines(log, topic));
        FaultItem? dead = producer.MqFaultStrategy.LatencyFaultTolerance
            .GetFaultItem(MockCluster.BrokerName(0));
        Assert.NotNull(dead);
        Assert.False(dead!.IsAvailable());
        Assert.False(dead.IsReachable(), "建连失败既隔离也不可达");
        producer.Shutdown();
    }

    /// <summary>RetryTimesWhenSendAsyncFailed=0 ⇒ 一笔都不重试。</summary>
    [Fact]
    public void RetryCapZeroMeansOneAttempt()
    {
        string topic = "AsyncNoRetry";
        using var cluster = MockCluster.StartAt(
            new List<string?> { MockCluster.DeadAddr(), MockCluster.DeadAddr() });
        string log = CaptureLog("no_retry");
        DefaultMQProducer producer = Started(cluster, "GID_async_no_retry");
        producer.RetryTimesWhenSendAsyncFailed = 0;

        var cb = new Recorder();
        producer.SendAsync(new Message(topic, new byte[10]), cb, 8000);
        Assert.True(cb.WaitDone(1, 5000));
        Assert.Single(cb.Errors());
        Assert.Empty(RetryLines(log, topic));
        producer.Shutdown();
    }

    /// <summary>
    /// 定点发送（调用方给了 mq）永不换 broker：Java 传下去的 topicPublishInfo 是 null，
    /// 重试只能在**同一台**上原地换 opaque。
    /// </summary>
    [Fact]
    public void PinnedQueue_NeverSwitchesBroker()
    {
        string topic = "AsyncPinned";
        using var cluster = MockCluster.StartAt(
            new List<string?> { MockCluster.DeadAddr(), null });
        cluster.Script(1, new List<(int, int)>(), (ResponseCode.Success, 0));
        string log = CaptureLog("pinned");
        DefaultMQProducer producer = Started(cluster, "GID_async_pinned");

        var pinned = new MessageQueue(topic, MockCluster.BrokerName(0), 0);
        var cb = new Recorder();
        producer.SendAsync(new Message(topic, new byte[10]), cb, 8000, pinned);
        Assert.True(cb.WaitDone(1, 5000));
        Assert.Single(cb.Errors());
        Assert.True(cluster.Requests(1) == 0, "定点发送不许挪到另一台 broker");
        List<string> lines = RetryLines(log, topic);
        Assert.Equal(2, lines.Count);
        Assert.All(lines, line => Assert.Equal(pinned.BrokerName, FieldOf(line, "brokerName=")));
        producer.Shutdown();
    }

    /// <summary>
    /// 定点发送的唯一路由来源是 sendKernelImpl 里的那一次刷路由（Java :919-924）：
    /// 调用方没走 sendDefaultImpl 取发布信息，不刷路由就解析不出地址。
    /// </summary>
    [Fact]
    public void PinnedQueue_RefreshesTheRouteToResolveTheAddress()
    {
        string topic = "AsyncPinnedRoute";
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_async_pinned_route");

        var pinned = new MessageQueue(topic, MockCluster.BrokerName(0), 0);
        var cb = new Recorder();
        producer.SendAsync(new Message(topic, new byte[10]), cb, 3000, pinned);
        Assert.True(cb.WaitDone(1, 3000), string.Join(" / ", cb.Errors()));
        Assert.Single(cb.Results());
        Assert.True(cluster.CountRequests(RequestCode.GetRouteinfoByTopic) > 0,
            "定点发送要自己刷一次路由才解析得出地址");
        Assert.Equal(1, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>地址表里查不到的 broker：就地报「The broker[x] not exist」，回调不绕道回调池。</summary>
    [Fact]
    public void UnresolvableBroker_ReportsNotExistThroughTheCallback()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQProducer producer = Started(cluster, "GID_async_no_broker");
        int callerThread = Environment.CurrentManagedThreadId;

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 3000,
            new MessageQueue(Topic, "no-such-broker", 0));
        Assert.True(cb.WaitDone(1, 3000));
        Assert.Single(cb.Errors());
        Assert.Contains("The broker[no-such-broker] not exist", cb.Errors()[0]);
        Assert.Equal(0, cluster.Requests(0));
        // 请求还没建就失败了 ⇒ 没进传输层 ⇒ 不绕道回调池，在准备段那根线程上就地转交
        //（Python _complete 同一条）
        (int threadId, string threadName) = cb.LastThread();
        Assert.NotEqual(callerThread, threadId);
        Assert.StartsWith("AsyncSenderExecutor_", threadName);
        producer.Shutdown();
    }

    /// <summary>路由完全取不到：定性成「找不到 topic」并交给回调，一笔都不发。</summary>
    [Fact]
    public void MissingRoute_ReportsNotFoundTopicThroughTheCallback()
    {
        using var cluster = MockCluster.Start(1, routeOk: false);
        DefaultMQProducer producer = Started(cluster, "GID_async_no_route");

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 3000);
        Assert.True(cb.WaitDone(1, 3000));
        Assert.Single(cb.Errors());
        Assert.Equal(0, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>并发的一批异步发送彼此不串台：每笔一个终态回调，且上线的 opaque 互不相同。</summary>
    [Fact]
    public void ConcurrentAsyncSends_NeverCrossTalk()
    {
        const int sends = 24;
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_async_concurrent");

        var cb = new Recorder();
        for (int i = 0; i < sends; ++i)
        {
            producer.SendAsync(Msg(), cb, 5000);
        }

        Assert.True(cb.WaitDone(sends, 10000), "每笔都要拿到恰好一次终态");
        Assert.True(WaitUntil(() => cluster.Requests(0) >= sends, 3000));
        Assert.Equal(sends, cb.Count);
        Assert.Equal(sends, cluster.Requests(0));
        var opaques = new HashSet<int>();
        for (int i = 0; i < sends; ++i)
        {
            int opaque = cluster.SendOpaque(0, i);
            Assert.NotEqual(int.MinValue, opaque);
            Assert.True(opaques.Add(opaque), "在途请求的 opaque 必须互不相同");
        }

        producer.Shutdown();
    }

    // ---------------------------------------------------------------- 准备段的闸门与钩子

    /// <summary>拦截钩子拒绝：异常原样到回调，且**一次请求都不上线**；钩子看到的是 ASYNC。</summary>
    [Fact]
    public void CheckForbiddenRejection_ReachesTheCallback_AndRunsInAsyncMode()
    {
        using var cluster = MockCluster.Start(1);
        var forbidden = new ForbiddenHook();
        DefaultMQProducer producer = Started(cluster, "GID_async_forbidden");
        producer.RegisterCheckForbiddenHook(forbidden);
        Message rejected = Msg();
        rejected.Tags = "Forbidden";

        var cb = new Recorder();
        producer.SendAsync(rejected, cb, 3000);
        Assert.True(cb.WaitDone(1, 3000));
        Assert.Single(cb.Errors());
        Assert.Contains("tag Forbidden is not allowed", cb.Errors()[0]);
        Assert.Equal(0, cluster.Requests(0));
        Assert.Equal(1, forbidden.Calls());
        Assert.Equal(CommunicationMode.Async, forbidden.Mode());

        // 同一个生产者换个标签照常发出去（拒绝不是把池子弄坏了）
        producer.SendAsync(Msg(), cb, 3000);
        Assert.True(cb.WaitDone(2, 3000));
        Assert.Equal(1, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>
    /// Java sendKernelImpl:1043-1046 —— ASYNC 分支**自己**还有一道预算闸：准备工作
    /// （这里用拦截钩子的耗时代表）把预算花光就不再建请求，但 after 钩子照跑一次。
    /// </summary>
    [Fact]
    public void PrepStageBeyondBudget_ReportsSendKernelImplTimeout()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        var hooks = new CountingSendHook();
        DefaultMQProducer producer = Started(cluster, "GID_async_kernel_timeout");
        producer.RegisterCheckForbiddenHook(new ForbiddenHook(sleepMillis: 400));
        producer.RegisterSendMessageHook(hooks);

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 200);
        Assert.True(cb.WaitDone(1, 3000));
        Assert.Single(cb.Errors());
        Assert.Contains("sendKernelImpl call timeout", cb.Errors()[0]);
        Assert.True(cluster.Requests(0) == 0, "预算没了就不该建请求");
        Assert.Equal(1, hooks.Before());
        Assert.True(hooks.After() == 1, "RemotingTooMuchRequest 也跑 after（Java :1088）");
        Assert.True(hooks.SawException());
        producer.Shutdown();
    }

    /// <summary>批量没有异步内核（本端口只有同步批量内核）：在池线程里发一批，回调照样转交。</summary>
    [Fact]
    public void BatchAsync_FallsBackToTheSyncBatchKernel()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_async_batch");

        MessageBatch batch = MessageBatch.GenerateFromList(new List<Message> { Msg(), Msg() });
        var cb = new Recorder();
        producer.SendAsync(batch, cb, 3000);
        Assert.True(cb.WaitDone(1, 3000), string.Join(" / ", cb.Errors()));
        Assert.Single(cb.Results());
        Assert.Equal(SendStatus.SendOk, cb.Results()[0]!.SendStatus);
        WireRecord? sent = cluster.SendRequestAt(0);
        Assert.NotNull(sent);
        Assert.Equal(RequestCode.SendBatchMessage, sent!.Code);
        Assert.Equal("true", sent.Ext["m"]);
        producer.Shutdown();
    }

    // ---------------------------------------------------------------- 背压闸门

    /// <summary>默认配置与 Java 一致，而且闸门默认**不拦路**。</summary>
    [Fact]
    public void BackpressureDefaultsMatchJava()
    {
        var producer = new DefaultMQProducer("GID_async_bp_default");
        Assert.False(producer.EnableBackpressureForAsyncMode);
        Assert.Equal(1024, producer.GetBackPressureForAsyncSendNum());
        Assert.Equal(100 * 1024 * 1024, producer.GetBackPressureForAsyncSendSize());
        Assert.Equal(1024, producer.SemaphoreAsyncSendNumAvailablePermits);
        Assert.Equal(100L * 1024 * 1024, producer.SemaphoreAsyncSendSizeAvailablePermits);
        Assert.Equal(2, producer.RetryTimesWhenSendAsyncFailed);
        Assert.Equal(50000, producer.AsyncSenderQueueCapacity);
    }

    /// <summary>条数闸地板值 10、字节闸地板值 1M（Java DefaultMQProducerImpl:141-153）。</summary>
    [Fact]
    public void BackpressureFloorsAreJavaFloors()
    {
        var producer = new DefaultMQProducer("GID_async_bp_floor")
        {
            BackPressureForAsyncSendNum = 1,
            BackPressureForAsyncSendSize = 1,
        };
        Assert.Equal(10, producer.GetBackPressureForAsyncSendNum());
        Assert.Equal(1024 * 1024, producer.GetBackPressureForAsyncSendSize());

        producer.BackPressureForAsyncSendNum = 20;
        producer.BackPressureForAsyncSendSize = 4 * 1024 * 1024;
        Assert.Equal(20, producer.GetBackPressureForAsyncSendNum());
        Assert.Equal(4 * 1024 * 1024, producer.GetBackPressureForAsyncSendSize());
        Assert.Equal(20, producer.SemaphoreAsyncSendNumAvailablePermits);
    }

    /// <summary>
    /// 条数闸打满：第 11 笔**在调用方线程上**等到预算耗尽，报 Java 的原文案，
    /// 且一笔都不许上线（闸在 submit 之前）。
    /// </summary>
    [Fact]
    public void NumGate_RejectsWithJavaMessageAndSendsNothing()
    {
        using var cluster = MockCluster.Start(1);
        cluster.MakeSilent(0);
        DefaultMQProducer producer = Started(cluster, "GID_async_num_gate");
        producer.EnableBackpressureForAsyncMode = true;
        producer.BackPressureForAsyncSendNum = 10;
        int callerThread = Environment.CurrentManagedThreadId;

        var held = new Recorder();
        for (int i = 0; i < 10; ++i)
        {
            producer.SendAsync(Msg(), held, 2000);
        }

        Assert.Equal(0, producer.SemaphoreAsyncSendNumAvailablePermits);

        var rejected = new Recorder();
        producer.SendAsync(Msg(), rejected, 200);
        Assert.True(rejected.WaitDone(1, 5000));
        Assert.Single(rejected.Errors());
        Assert.Contains("send message tryAcquire semaphoreAsyncNum timeout", rejected.Errors()[0]);
        // 扣不到许可就连池子都没进 ⇒ 回调就地转交（Python _complete）
        Assert.Equal(callerThread, rejected.LastThread().ThreadId);
        Assert.True(WaitUntil(() => cluster.Requests(0) == 10, 3000),
            "前 10 笔都要上线，被拒的第 11 笔一笔都不许发");
        Assert.True(held.WaitDone(10, 8000), "前 10 笔要在自己的超时里收尾");
        Assert.True(WaitUntil(
            () => producer.SemaphoreAsyncSendNumAvailablePermits == 10, 5000), "许可必须还得回来");
        producer.Shutdown();
    }

    /// <summary>字节闸打满：报自己的文案，并且**把已经拿到的条数许可还回去**。</summary>
    [Fact]
    public void SizeGate_RejectsAndGivesTheNumPermitBack()
    {
        const int big = 600 * 1024;
        using var cluster = MockCluster.Start(1);
        cluster.MakeSilent(0);
        DefaultMQProducer producer = Started(cluster, "GID_async_size_gate");
        producer.EnableBackpressureForAsyncMode = true;
        producer.BackPressureForAsyncSendNum = 10;
        producer.BackPressureForAsyncSendSize = 1024 * 1024;

        var held = new Recorder();
        producer.SendAsync(Msg(big), held, 2000);
        Assert.True(WaitUntil(() => producer.SemaphoreAsyncSendNumAvailablePermits == 9, 1000));
        Assert.Equal(1024 * 1024 - big, producer.SemaphoreAsyncSendSizeAvailablePermits);

        var rejected = new Recorder();
        producer.SendAsync(Msg(big), rejected, 300);
        Assert.True(rejected.WaitDone(1, 5000));
        Assert.Single(rejected.Errors());
        Assert.Contains("send message tryAcquire semaphoreAsyncSize timeout", rejected.Errors()[0]);
        // 条数许可是这笔自己拿的，失败必须还回去 —— 只剩第一笔占着的那一格
        Assert.Equal(9, producer.SemaphoreAsyncSendNumAvailablePermits);
        Assert.Equal(1024 * 1024 - big, producer.SemaphoreAsyncSendSizeAvailablePermits);

        Assert.True(held.WaitDone(1, 8000));
        Assert.True(WaitUntil(
            () => producer.SemaphoreAsyncSendSizeAvailablePermits == 1024 * 1024, 5000));
        Assert.Equal(10, producer.SemaphoreAsyncSendNumAvailablePermits);
        producer.Shutdown();
    }

    /// <summary>成功与失败都要归还许可：链路上每条终点都只归还一次。</summary>
    [Fact]
    public void Permits_AreGivenBackOnBothOutcomes()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        cluster.Script(1, new List<(int, int)>(), (ResponseCode.SystemError, 0));
        DefaultMQProducer producer = Started(cluster, "GID_async_permits");
        producer.EnableBackpressureForAsyncMode = true;
        producer.BackPressureForAsyncSendNum = 10;
        producer.BackPressureForAsyncSendSize = 1024 * 1024;

        var ok = new Recorder();
        var failed = new Recorder();
        for (int i = 0; i < 3; ++i)
        {
            producer.SendAsync(Msg(), ok, 3000,
                new MessageQueue(Topic, MockCluster.BrokerName(0), 0));
        }

        for (int i = 0; i < 2; ++i)
        {
            producer.SendAsync(Msg(), failed, 3000,
                new MessageQueue(Topic, MockCluster.BrokerName(1), 0));
        }

        Assert.True(ok.WaitDone(3, 5000));
        Assert.True(failed.WaitDone(2, 5000));
        Assert.True(WaitUntil(
            () => producer.SemaphoreAsyncSendNumAvailablePermits == 10
                  && producer.SemaphoreAsyncSendSizeAvailablePermits == 1024 * 1024, 5000),
            "两笔终点都得把许可还得干净");
        producer.Shutdown();
    }

    // ---------------------------------------------------------------- 有界发送队列

    /// <summary>
    /// 队列满了（Java 的 LinkedBlockingQueue(50000) 投不进去）：把这一笔**退回调用方**，
    /// 不排队、不走回调；等池子空出来之后，前一笔照常发出去。
    /// </summary>
    [Fact]
    public void QueueFull_RejectsTheCallerWithoutSending()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        var parked = new CountingSendHook(parkMillis: 1200);
        // 1 根 worker + 1 格队列：第一笔占住 worker，第二笔排上队，第三笔就被退回
        using var pool = new ConsumeExecutor(1, 1, 60.0, "AsyncSenderExecutor",
            maxQueueSize: 1, threadNameSep: "_", threadIndexFrom: 1);
        DefaultMQProducer producer = Started(cluster, "GID_async_queue_full", pool);
        producer.RegisterSendMessageHook(parked);

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 5000);
        // 等第一笔真的被 worker 取走（钩子已开跑）再投后两笔，否则「队列已满」会提前
        // 落在第二笔上，测的就不是同一格位置了。
        Assert.True(WaitUntil(() => parked.Before() >= 1, 3000), "准备段要跑起来");
        producer.SendAsync(Msg(), cb, 5000);
        MQClientException rejected = Assert.Throws<MQClientException>(
            () => producer.SendAsync(Msg(), cb, 5000));
        Assert.Contains("executor rejected", rejected.Message);
        Assert.True(cb.Count == 0, "被拒的一笔不该有回调");
        Assert.True(cluster.Requests(0) == 0, "被拒之前一次请求都不该发出去");

        Assert.True(cb.WaitDone(2, 10000), "排队的两笔要在池子空出来之后跑完");
        Assert.Equal(2, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>
    /// 队列满 + **开了背压**（Java :675-681）：许可已经扣了，就地跑完这一笔（阻塞调用方），
    /// 好让回调把许可还回来 —— 不退给调用方，也不白等一次超时。
    /// </summary>
    [Fact]
    public void QueueFull_RunsInlineWhenBackpressureIsOn()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        var parked = new CountingSendHook(parkMillis: 900);
        using var pool = new ConsumeExecutor(1, 1, 60.0, "AsyncSenderExecutor",
            maxQueueSize: 1, threadNameSep: "_", threadIndexFrom: 1);
        DefaultMQProducer producer = Started(cluster, "GID_async_queue_full_bp", pool);
        producer.RegisterSendMessageHook(parked);
        producer.EnableBackpressureForAsyncMode = true;
        producer.BackPressureForAsyncSendNum = 10;
        producer.BackPressureForAsyncSendSize = 1024 * 1024;

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 8000);
        Assert.True(WaitUntil(() => parked.Before() >= 1, 3000), "准备段要跑起来");
        producer.SendAsync(Msg(), cb, 8000);
        var watch = Stopwatch.StartNew();
        producer.SendAsync(Msg(), cb, 8000);
        watch.Stop();
        // 就地跑：这一笔的耗时落在调用方线程上（等前面排队的跑完 + 自己发完）
        Assert.True(watch.ElapsedMilliseconds >= 300,
            "开了背压时队满要就地跑完，elapsed=" + watch.ElapsedMilliseconds);
        Assert.True(cb.WaitDone(3, 12000), string.Join(" / ", cb.Errors()));
        Assert.Equal(3, cb.Results().Count(r => r is not null));
        Assert.Equal(3, cluster.Requests(0));
        Assert.Equal(10, producer.SemaphoreAsyncSendNumAvailablePermits);
        producer.Shutdown();
    }

    /// <summary>
    /// 排队把预算吃光：出队之后才算真实耗时，超了就直接回调
    /// 「DEFAULT ASYNC send call timeout」，不再发请求（Python _run / Java :555）。
    /// </summary>
    [Fact]
    public void QueueWaitBeyondBudget_ReportsAsyncSendCallTimeout()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        var parked = new CountingSendHook(parkMillis: 900);
        using var pool = new ConsumeExecutor(1, 1, 60.0, "AsyncSenderExecutor",
            maxQueueSize: 5, threadNameSep: "_", threadIndexFrom: 1);
        DefaultMQProducer producer = Started(cluster, "GID_async_stale_budget", pool);
        producer.RegisterSendMessageHook(parked);

        var first = new Recorder();
        var stale = new Recorder();
        producer.SendAsync(Msg(), first, 8000);
        // 先确认真的只有一根 worker 被占住，再投后面三笔：否则某笔可能在第一笔之前
        // 就被取走，它的 80ms 预算还够，测试意图就没了。
        Assert.True(WaitUntil(() => parked.Before() >= 1, 3000), "准备段要跑起来");
        for (int i = 0; i < 3; ++i)
        {
            // 排在第一笔后面：等轮到自己时，80ms 的预算早就被排队花光了
            producer.SendAsync(Msg(), stale, 80);
        }

        Assert.True(stale.WaitDone(3, 12000), string.Join(" / ", stale.Errors()));
        Assert.All(stale.Errors(), e => Assert.Contains("DEFAULT ASYNC send call timeout", e));
        Assert.True(first.WaitDone(1, 12000));
        Assert.True(cluster.Requests(0) == 1, "预算没了的三笔不该上线");
        producer.Shutdown();
    }

    /// <summary>
    /// 关池要把手上的活干完（Java/Python 用的是不等待的 <c>shutdown()</c>，队列里还没跑到的
    /// 准备段会连同任务一起被丢掉）：交进来的每一笔准备段都必须**跑到**。
    ///
    /// ⚠ 「上线多少笔」断不到精确值：准备段把报文写进 socket 之后，紧接着就是关客户端，
    /// broker 还没读走的**尾部**几帧会随连接一起丢掉（跑整套用例时实测 32/36 上线、单跑
    /// 稳定 36/36），那几笔既不上线也没回调 —— 与 Java 同一条：客户端一关就停超时清理并清空
    /// 在途表，没回来的那笔就此没有终态。所以这里只断言「至少一半上线」：防的回归（把队列
    /// 里的任务丢掉）只能让 cores 笔上线，即总数的 1/3，差得够远。
    /// </summary>
    [Fact]
    public void Shutdown_DrainsTheHandedOffSends()
    {
        // 比池子的核数多一截：Shutdown 那一刻一定还有任务排在队列里
        int sends = Math.Max(1, Environment.ProcessorCount) * 3;
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        var parked = new CountingSendHook(parkMillis: 80);
        // 日志要在动作**之前**切到新文件，否则「池任务异常」那几行落在上一轮的日志里
        string drainLog = CaptureLog("drain");
        DefaultMQProducer producer = Started(cluster, "GID_async_shutdown_drain");
        producer.RegisterSendMessageHook(parked);
        var each = new Recorder[sends];
        for (int i = 0; i < sends; ++i)
        {
            each[i] = new Recorder();
            producer.SendAsync(Msg(), each[i], 5000);
        }

        producer.Shutdown();
        // 排空的**定义**：队列里的准备段每一个都跑过（不是随池子一起丢掉）
        Assert.True(parked.Before() == sends,
            "每个准备段都要跑到，实际=" + parked.Before() + " 发送=" + sends);
        // 跑完准备段的一笔就把报文交给了传输层；关掉客户端时被打断的只是 broker 还没
        // 读走的尾部（见方法注释），所以这里只要求「至少一半上线」
        Assert.True(WaitUntil(() => cluster.Requests(0) * 2 >= sends, 5000),
            "至少一半要上线，上线=" + cluster.Requests(0)
            + " 发送=" + sends
            + " 报错=" + each.Count(r => r.Errors().Count > 0)
            + " 成功=" + each.Count(r => r.Results().Count > 0)
            + " after钩子=" + parked.After()
            + " 池任务异常=" + CountLines(drainLog, "consume executor task raised"));
        Thread.Sleep(500);
        int terminal = 0;
        foreach (Recorder r in each)
        {
            Assert.True(r.Count <= 1, "一笔发送最多一个终态回调，实际=" + r.Count);
            terminal += r.Count;
            Assert.All(r.Results(), s => Assert.Equal(SendStatus.SendOk, s!.SendStatus));
        }

        // 至少有一笔在关客户端之前就走完了整条链（否则这个断言就只是在证「回调都没跑」）
        Assert.True(terminal >= 1, "排空之后总该有几笔拿到了终态");
    }

    /// <summary>调用方自带的池（Java setAsyncSenderExecutor）由它自己关，生产者不接手。</summary>
    [Fact]
    public void CallerOwnedSenderPool_SurvivesProducerShutdown()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        using var pool = new ConsumeExecutor(2, 2, 60.0, "AsyncSenderExecutor",
            maxQueueSize: 10, threadNameSep: "_", threadIndexFrom: 1);
        DefaultMQProducer producer = Started(cluster, "GID_async_custom_pool", pool);
        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 5000);
        Assert.True(cb.WaitDone(1, 5000));
        producer.Shutdown();

        // 池没被生产者关掉：换个生产者接着用同一个池
        DefaultMQProducer next = Started(cluster, "GID_async_custom_pool_next", pool);
        var again = new Recorder();
        next.SendAsync(Msg(), again, 5000);
        Assert.True(again.WaitDone(1, 5000), string.Join(" / ", again.Errors()));
        next.Shutdown();
        Assert.True(WaitUntil(() => pool.WorkerCount() >= 1, 1000), "池子还归调用方");
    }
}
