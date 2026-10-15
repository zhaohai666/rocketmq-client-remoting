// Sampling here is based on the evidence C# can observe: the fake cluster
// (MockCluster.cs) sees the **request that goes on the wire**, and the log sees the
// **retry chain**; together they are enough to pin down the invariants "the caller
// returns immediately / the callback fires exactly once / async ignores
// retryResponseCodes / the budget is shared / how the gates and a full queue reject".
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
        // ClientLog holds the write handle throughout, so redirect the log elsewhere before deleting the temp directory
        ClientLog.SetLogFile(Path.Combine(_dir, "detached.log"));
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (Exception)
        {
            // A cleanup failure does not affect the test
        }
    }

    // ---------------------------------------------------------------- Fixtures

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

    /// <summary>Polls until the condition holds (the callback is cross-thread, so it cannot be assumed to have run yet).</summary>
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

    private sealed class Recorder : ISendCallback
    {
        private readonly object _gate = new();
        private readonly List<SendResult?> _results = new();
        private readonly List<Exception> _errors = new();
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
                return _errors.Select(e => e.Message).ToList();
            }
        }

        public List<Exception> Exceptions()
        {
            lock (_gate)
            {
                return new List<Exception>(_errors);
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

        public void OnException(Exception error)
        {
            lock (_gate)
            {
                _errors.Add(error);
                _threads.Add(Environment.CurrentManagedThreadId);
                _threadNames.Add(Thread.CurrentThread.Name ?? string.Empty);
            }
        }
    }

    /// <summary>Counts how often before/after each ran, and records whether after saw a
    /// result or an exception. Optionally "parks for a while inside before" to occupy a
    /// pool worker (a real cluster cannot produce a stretch that long).</summary>
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

    /// <summary>An interceptor hook: records the CommunicationMode it saw, may optionally
    /// sleep for a while (spending the prologue's budget, to verify sendKernelImpl's own
    /// gate), and may outright reject a given tag.</summary>
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
                Thread.Sleep(_sleepMillis);
            }

            string tags = context.Message?.Tags ?? string.Empty;
            if (tags == "Forbidden")
            {
                throw new MQClientException("offline test: tag Forbidden is not allowed");
            }
        }
    }

    private string CaptureLog(string tag)
    {
        string path = Path.Combine(_dir, tag + ".log");
        ClientLog.SetLogFile(path);
        ClientLog.FlushLogFile();
        return path;
    }

    /// <summary>ClientLog holds the write handle, so it must be opened shared for
    /// ReadWrite, the way <c>tail -f</c> does it.</summary>
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

    /// <summary>Counts the log lines containing a given fragment (ClientLog holds the
    /// write handle, so it is opened shared for ReadWrite).</summary>
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

    /// <summary>Extracts brokerName= from a retry log line (decides "did it switch
    /// broker").</summary>
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

    // ---------------------------------------------------------------- The caller is not blocked

    /// <summary>
    /// The **first invariant of async send**: the caller returns immediately, the
    /// prologue runs on AsyncSenderExecutor_N, the user callback runs on
    /// NettyClientPublicExecutor_N, and it fires **exactly once**.
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
        // The hook also sleeps 600ms: if the caller were held up, this would not be far smaller than it
        Assert.True(watch.ElapsedMilliseconds < 500,
            "the caller must return immediately, elapsed=" + watch.ElapsedMilliseconds);
        Assert.Equal(0, cb.Count);
        Assert.Equal(0, cluster.Requests(0));
        Assert.True(WaitUntil(() => hooks.Before() >= 1, 3000), "the prologue must run");
        Assert.True(hooks.BeforeThreadName().StartsWith("AsyncSenderExecutor_", StringComparison.Ordinal),
            "the prologue must run in its own pool, actual thread name=" + hooks.BeforeThreadName());
        Assert.True(cb.WaitDone(1, 5000));
        Assert.Equal(1, cb.Count);
        Assert.Empty(cb.Errors());
        Assert.Equal(SendStatus.SendOk, cb.Results()[0]!.SendStatus);
        (int threadId, string threadName) = cb.LastThread();
        Assert.NotEqual(callerThread, threadId);
        Assert.True(threadName.StartsWith("NettyClientPublicExecutor_", StringComparison.Ordinal),
            "the user callback must not run on the send pool or the reader thread, actual=" + threadName);
        Assert.Equal(1, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>Each async send runs before/after **once** only, and after sees the
    /// SendResult.</summary>
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
        // MsgId is the UNIQ_KEY the client filled in itself (32 hex digits); OffsetMsgId is the one the broker returned
        Assert.Equal(32, result.MsgId.Length);
        Assert.NotEqual(result.MsgId, result.OffsetMsgId);
        Assert.Equal("MOCK-0-1", result.OffsetMsgId);
        Assert.Equal(1, hooks.Before());
        Assert.Equal(1, hooks.After());
        Assert.True(hooks.SawResult(), "the after hook must see the sendResult");
        Assert.False(hooks.SawException());
        producer.Shutdown();
    }

    /// <summary>timeout=0: the budget is gone before queueing even finishes -- not one
    /// message may go on the wire.</summary>
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
        Assert.Contains("producer already shutdown", after.Message);
        Assert.Equal(1, cb.Count);
    }

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
        Assert.True(cb.WaitDone(1, 3000), "the pool must still be alive to receive the second callback");
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

        public void OnException(Exception error)
        {
            Interlocked.Increment(ref Calls);
            throw new InvalidOperationException("callback body blew up");
        }
    }

    // ---------------------------------------------------------------- Retry chain

    /// <summary>
    /// **Async send does not consult RetryResponseCodes**: an explicit broker error is
    /// handed to the callback as is and the attempt ends there (the same code would be
    /// retried three times on the synchronous path, see SendRetryTests).
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
            "this code is retryable on the synchronous path");

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 3000);
        Assert.True(cb.WaitDone(1, 3000));
        Assert.Single(cb.Errors());
        Assert.Contains("CODE:", cb.Errors()[0]);
        Assert.Equal(1, cluster.Requests(0));
        Assert.True(cluster.Requests(1) == 0, "the async chain does not switch broker (the needRetry=false semantics)");
        FaultItem? item = producer.MqFaultStrategy.LatencyFaultTolerance
            .GetFaultItem(MockCluster.BrokerName(0));
        Assert.NotNull(item);
        Assert.False(item!.IsAvailable(), "the broker that returned an error must be isolated");
        Assert.True(item.IsReachable());
        producer.Shutdown();
    }

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
        Assert.True(cluster.Requests(1) == 0, "with the budget gone there must be no second attempt");
        Assert.True(elapsed >= 700, "a full request timeout was waited out, elapsed=" + elapsed);
        producer.Shutdown();
    }

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
        // A local loopback rejection of a "just closed port" can be stretched to ~2s (plus
        // the 1s-granularity connection poll, i.e. ~3s each time): the default 8s budget
        // cannot cover 3 retries and would be judged callTimeout. Relax it so the retry chain
        // can run to completion naturally.
        producer.SendAsync(new Message(topic, new byte[10]), cb, 30000);
        Assert.True(cb.WaitDone(1, 40000));
        long elapsed = watch.ElapsedMilliseconds;
        Assert.Single(cb.Errors());
        // The outer catch's rule: a connect failure is thrown **as is**, not wrapped as "unknown reason"
        Assert.Contains("connect failed to", cb.Errors()[0]);
        Assert.DoesNotContain("last error", cb.Errors()[0]);
        // After three connect failures the chain wraps up naturally (the connect failed to
        // above already rules out a callTimeout cut short): elapsed is milliseconds on a
        // machine where the rejection is immediate, around 9s on one where it takes ~2s
        Assert.True(elapsed < 30000, "a connect-level failure must not eat the budget, elapsed=" + elapsed);

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

    /// <summary>The one it switched to is a real broker: the chain recovers, the callback
    /// gets SEND_OK, and only one retry happened.</summary>
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
        Assert.True(cluster.Requests(1) == 1, "only the one that is alive received the message");
        Assert.Single(RetryLines(log, topic));
        FaultItem? dead = producer.MqFaultStrategy.LatencyFaultTolerance
            .GetFaultItem(MockCluster.BrokerName(0));
        Assert.NotNull(dead);
        Assert.False(dead!.IsAvailable());
        Assert.False(dead.IsReachable(), "a connect failure both isolates and is unreachable");
        producer.Shutdown();
    }

    /// <summary>RetryTimesWhenSendAsyncFailed=0 => not a single retry.</summary>
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
        // Same as ConnectFailure_RetriesOntoAnotherBroker: a ~2s connect rejection needs a 30s budget
        producer.SendAsync(new Message(topic, new byte[10]), cb, 30000, pinned);
        Assert.True(cb.WaitDone(1, 40000));
        Assert.Single(cb.Errors());
        Assert.True(cluster.Requests(1) == 0, "a pinned send must not move to another broker");
        List<string> lines = RetryLines(log, topic);
        Assert.Equal(2, lines.Count);
        Assert.All(lines, line => Assert.Equal(pinned.BrokerName, FieldOf(line, "brokerName=")));
        producer.Shutdown();
    }

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
            "a pinned send has to refresh the route once itself before it can resolve the address");
        Assert.Equal(1, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>A broker that is not in the address table: reports "The broker[x] not
    /// exist" right here, and the callback is not routed via the callback pool.</summary>
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
        // It failed before the request was even built => it never entered the transport layer
        // => it is not routed via the callback pool but handed over in place on the
        // prologue's own thread
        (int threadId, string threadName) = cb.LastThread();
        Assert.NotEqual(callerThread, threadId);
        Assert.StartsWith("AsyncSenderExecutor_", threadName);
        producer.Shutdown();
    }

    /// <summary>The route cannot be obtained at all: classified as "topic not found" and
    /// handed to the callback, with nothing sent.</summary>
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

    [Fact]
    public void NoNameServerAddress_Reports10004ThroughTheCallback()
    {
        using var cluster = MockCluster.Start(1, routeOk: false);
        DefaultMQProducer producer = Started(cluster, "GID_async_no_namesrv");
        ((List<string>)producer.Client().NameServerAddrs).Clear();

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 3000);
        Assert.True(cb.WaitDone(1, 3000));
        Assert.Equal("No name server address, please set it.", cb.Errors()[0]);
        Assert.Equal(0, cluster.Requests(0));
        Assert.Equal(ClientErrorCode.NoNameServerException,
            ((MQClientException)cb.Exceptions()[0]).ResponseCode);
        producer.Shutdown();
    }

    /// <summary>A concurrent batch of async sends does not cross wires: one terminal
    /// callback each, and the opaques that go on the wire are all different.</summary>
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

        Assert.True(cb.WaitDone(sends, 10000), "every send must get exactly one terminal callback");
        Assert.True(WaitUntil(() => cluster.Requests(0) >= sends, 3000));
        Assert.Equal(sends, cb.Count);
        Assert.Equal(sends, cluster.Requests(0));
        var opaques = new HashSet<int>();
        for (int i = 0; i < sends; ++i)
        {
            int opaque = cluster.SendOpaque(0, i);
            Assert.NotEqual(int.MinValue, opaque);
            Assert.True(opaques.Add(opaque), "the opaques of in-flight requests must all differ");
        }

        producer.Shutdown();
    }

    // ---------------------------------------------------------------- Prologue gates and hooks

    /// <summary>An interceptor hook rejects: the exception reaches the callback as is
    /// and **not one request goes on the wire**; the hook sees ASYNC.</summary>
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

        // The same producer still sends another tag normally (a rejection does not break the pool)
        producer.SendAsync(Msg(), cb, 3000);
        Assert.True(cb.WaitDone(2, 3000));
        Assert.Equal(1, cluster.Requests(0));
        producer.Shutdown();
    }

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
        Assert.True(cluster.Requests(0) == 0, "with the budget gone no request must be built");
        Assert.Equal(1, hooks.Before());
        Assert.True(hooks.After() == 1, "RemotingTooMuchRequest runs the after hook too");
        Assert.True(hooks.SawException());
        producer.Shutdown();
    }

    /// <summary>Batch has no async kernel (this port only has the synchronous batch
    /// kernel): a batch is sent on a pool thread and the callback is still handed
    /// over.</summary>
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

    // ---------------------------------------------------------------- Backpressure gates

    [Fact]
    public void BackpressureDefaultsMatchLegacy()
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

    [Fact]
    public void BackpressureFloorsAreLegacyFloors()
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

    [Fact]
    public void NumGate_RejectsWithLegacyMessageAndSendsNothing()
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
        // With no permit obtainable it never even entered the pool => the callback is handed over in place
        Assert.Equal(callerThread, rejected.LastThread().ThreadId);
        Assert.True(WaitUntil(() => cluster.Requests(0) == 10, 3000),
            "the first 10 must go on the wire, the rejected 11th must not send anything");
        Assert.True(held.WaitDone(10, 8000), "the first 10 must finish within their own timeouts");
        Assert.True(WaitUntil(
            () => producer.SemaphoreAsyncSendNumAvailablePermits == 10, 5000), "the permits must come back");
        producer.Shutdown();
    }

    /// <summary>The byte gate is saturated: reports its own message and **returns the
    /// count permit it already took</summary>.
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
        // The count permit was taken by this send itself, so a failure must return it --
        // only the single slot held by the first send remains
        Assert.Equal(9, producer.SemaphoreAsyncSendNumAvailablePermits);
        Assert.Equal(1024 * 1024 - big, producer.SemaphoreAsyncSendSizeAvailablePermits);

        Assert.True(held.WaitDone(1, 8000));
        Assert.True(WaitUntil(
            () => producer.SemaphoreAsyncSendSizeAvailablePermits == 1024 * 1024, 5000));
        Assert.Equal(10, producer.SemaphoreAsyncSendNumAvailablePermits);
        producer.Shutdown();
    }

    /// <summary>Both success and failure must return the permits: every terminus on the
    /// chain returns them exactly once.</summary>
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
            "both sends must return the permits cleanly");
        producer.Shutdown();
    }

    // ---------------------------------------------------------------- Bounded send queue

    [Fact]
    public void QueueFull_RejectsTheCallerWithoutSending()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        var parked = new CountingSendHook(parkMillis: 1200);
        // 1 worker + a queue of 1: the first send occupies the worker, the second queues
        // up and the third is rejected
        using var pool = new ConsumeExecutor(1, 1, 60.0, "AsyncSenderExecutor",
            maxQueueSize: 1, threadNameSep: "_", threadIndexFrom: 1);
        DefaultMQProducer producer = Started(cluster, "GID_async_queue_full", pool);
        producer.RegisterSendMessageHook(parked);

        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 5000);
        // Wait until the first send has really been taken by the worker (the hook has
        // started) before submitting the other two, otherwise "the queue is full" would
        // land on the second send early and the test would no longer be about the same
        // queue position.
        Assert.True(WaitUntil(() => parked.Before() >= 1, 3000), "the prologue must run");
        producer.SendAsync(Msg(), cb, 5000);
        MQClientException rejected = Assert.Throws<MQClientException>(
            () => producer.SendAsync(Msg(), cb, 5000));
        Assert.Contains("executor rejected", rejected.Message);
        Assert.True(cb.Count == 0, "a rejected send must not get a callback");
        Assert.True(cluster.Requests(0) == 0, "no request at all may go out before a rejection");

        Assert.True(cb.WaitDone(2, 10000), "the two queued sends must finish once the pool frees up");
        Assert.Equal(2, cluster.Requests(0));
        producer.Shutdown();
    }

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
        Assert.True(WaitUntil(() => parked.Before() >= 1, 3000), "the prologue must run");
        producer.SendAsync(Msg(), cb, 8000);
        var watch = Stopwatch.StartNew();
        producer.SendAsync(Msg(), cb, 8000);
        watch.Stop();
        // Runs in place: this send's elapsed time lands on the caller's thread (waiting for the queued ones plus its own send)
        Assert.True(watch.ElapsedMilliseconds >= 300,
            "with backpressure on, a full queue must run in place, elapsed=" + watch.ElapsedMilliseconds);
        Assert.True(cb.WaitDone(3, 12000), string.Join(" / ", cb.Errors()));
        Assert.Equal(3, cb.Results().Count(r => r is not null));
        Assert.Equal(3, cluster.Requests(0));
        Assert.Equal(10, producer.SemaphoreAsyncSendNumAvailablePermits);
        producer.Shutdown();
    }

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
        // First confirm that really only one worker is occupied, then submit the next
        // three: otherwise one of them might be taken before the first send, its 80ms
        // budget would still suffice and the intent of the test would be lost.
        Assert.True(WaitUntil(() => parked.Before() >= 1, 3000), "the prologue must run");
        for (int i = 0; i < 3; ++i)
        {
            // Queued behind the first send: by the time it is its turn the 80ms budget has long since been eaten by queueing
            producer.SendAsync(Msg(), stale, 80);
        }

        Assert.True(stale.WaitDone(3, 12000), string.Join(" / ", stale.Errors()));
        Assert.All(stale.Errors(), e => Assert.Contains("DEFAULT ASYNC send call timeout", e));
        Assert.True(first.WaitDone(1, 12000));
        Assert.True(cluster.Requests(0) == 1, "the three sends without budget must not go on the wire");
        producer.Shutdown();
    }

    [Fact]
    public void Shutdown_DrainsTheHandedOffSends()
    {
        // Well over the pool's core count: at Shutdown there are certainly still tasks queued
        int sends = Math.Max(1, Environment.ProcessorCount) * 3;
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        var parked = new CountingSendHook(parkMillis: 80);
        // The log must be switched to the new file **before** the action, otherwise the "pool task raised" lines land in the previous round's log
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
        // Definition of **drained**: every prologue in the queue has run (not discarded together with the pool)
        Assert.True(parked.Before() == sends,
            "every prologue must run, actual=" + parked.Before() + " sends=" + sends);
        // A send whose prologue finished has already handed the request to the transport
        // layer; what the client shutdown interrupts is only the tail the broker has not
        // read yet (see the method comment), so "at least half on the wire" is all that
        // is required here
        Assert.True(WaitUntil(() => cluster.Requests(0) * 2 >= sends, 5000),
            "at least half must go on the wire, on wire=" + cluster.Requests(0)
            + " sends=" + sends
            + " errors=" + each.Count(r => r.Errors().Count > 0)
            + " successes=" + each.Count(r => r.Results().Count > 0)
            + " after hooks=" + parked.After()
            + " pool task exceptions=" + CountLines(drainLog, "consume executor task raised"));
        Thread.Sleep(500);
        int terminal = 0;
        foreach (Recorder r in each)
        {
            Assert.True(r.Count <= 1, "a send gets at most one terminal callback, actual=" + r.Count);
            terminal += r.Count;
            Assert.All(r.Results(), s => Assert.Equal(SendStatus.SendOk, s!.SendStatus));
        }

        // At least one send completed the whole chain before the client was closed
        // (otherwise this assertion would only be proving "no callback ran at all")
        Assert.True(terminal >= 1, "after draining, some sends must have reached a terminal state");
    }

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

        // The pool was not closed by the producer: another producer keeps using the same pool
        DefaultMQProducer next = Started(cluster, "GID_async_custom_pool_next", pool);
        var again = new Recorder();
        next.SendAsync(Msg(), again, 5000);
        Assert.True(again.WaitDone(1, 5000), string.Join(" / ", again.Errors()));
        next.Shutdown();
        Assert.True(WaitUntil(() => pool.WorkerCount() >= 1, 1000), "the pool still belongs to the caller");
    }

    /// The second half locks down "every sub-message must pass
    /// Validators.CheckMessage": handing the MessageBatch straight to SendAsync skips
    /// that step, which means the batch path bypasses local validation; and such
    /// failures must go <b>into the callback</b>, not back onto the caller's stack.
    /// </summary>
    [Fact]
    public void SendBatchAsync_SendsOneRequestPerBatch()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_batch_async");
        producer.MaxMessageSize = 4096;
        var cb = new Recorder();
        producer.SendBatchAsync(new List<Message> { Msg(10), Msg(10), Msg(10) }, cb, 5000);

        Assert.True(cb.WaitDone(1, 5000), string.Join(" / ", cb.Errors()));
        Assert.Equal(1, cb.Count);
        Assert.Equal(SendStatus.SendOk, cb.Results()[0]!.SendStatus);
        Assert.Equal(1, cluster.CountRequests(RequestCode.SendBatchMessage));
        Assert.Equal(0, cluster.CountRequests(RequestCode.SendMessageV2));
        WireRecord? wire = cluster.FirstSendRequest();
        Assert.NotNull(wire);
        Assert.True(wire!.HasBody);
        // A batch of three: the encoded body must be far bigger than a single message, otherwise only one of them was sent
        Assert.True(wire.Body.Length > 3 * 10, "batch body carries all sub-messages: " + wire.Body.Length);

        // An oversized sub-message: the whole batch is rejected in place, nothing goes on the wire, and the failure goes to the callback
        var bad = new Recorder();
        producer.SendBatchAsync(new List<Message> { Msg(10), Msg(4096) }, bad, 5000);
        Assert.True(bad.WaitDone(1, 5000), "a validation failure must also get a terminal callback");
        Assert.Contains("over max value", string.Join(" / ", bad.Errors()));
        Assert.Equal(1, cluster.CountRequests(RequestCode.SendBatchMessage));

        // An empty list: the same message text as the synchronous kernel, likewise into the callback
        var empty = new Recorder();
        producer.SendBatchAsync(new List<Message>(), empty, 5000);
        Assert.True(empty.WaitDone(1, 5000), "an empty batch must also get a terminal callback");
        Assert.Contains("message list is empty", string.Join(" / ", empty.Errors()));
        Assert.Equal(1, cluster.CountRequests(RequestCode.SendBatchMessage));
        producer.Shutdown();
    }

    [Fact]
    public void PinnedAsyncSendUsesTheAsyncWording_AndStopsBeforeTheWire()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_pinned_guard_async");
        var pinned = new MessageQueue("T2", MockCluster.BrokerName(0), 0);

        // (1) A single async send with a mismatched topic: the async message text, zero requests
        var cb = new Recorder();
        producer.SendAsync(Msg(), cb, 5000, pinned);
        Assert.True(cb.WaitDone(1, 5000), "a rejection must also get a terminal callback");
        Assert.Contains("Topic of the message does not match its target message queue",
            string.Join(" / ", cb.Errors()));
        Assert.Null(cluster.FirstSendRequest());

        // (2) The same topic still sends normally (the guard must not be so wide it hits valid sends)
        var ok = new Recorder();
        producer.SendAsync(Msg(), ok, 5000, new MessageQueue(Topic, MockCluster.BrokerName(0), 0));
        Assert.True(ok.WaitDone(1, 5000), string.Join(" / ", ok.Errors()));
        Assert.Equal(SendStatus.SendOk, ok.Results()[0]!.SendStatus);
        Assert.Equal(1, cluster.CountRequests(RequestCode.SendMessageV2));

        // (3) Batch async shares the same guard and the same message text
        var batch = new Recorder();
        producer.SendBatchAsync(new List<Message> { Msg(), Msg() }, batch, 5000, pinned);
        Assert.True(batch.WaitDone(1, 5000), "a rejected batch must also get a terminal callback");
        Assert.Contains("Topic of the message does not match its target message queue",
            string.Join(" / ", batch.Errors()));
        Assert.Equal(1, cluster.CountRequests(RequestCode.SendMessageV2));

        var good = new Recorder();
        producer.SendBatchAsync(new List<Message> { Msg(), Msg() }, good, 5000,
            new MessageQueue(Topic, MockCluster.BrokerName(0), 0));
        Assert.True(good.WaitDone(1, 5000), string.Join(" / ", good.Errors()));
        Assert.Equal(1, cluster.CountRequests(RequestCode.SendBatchMessage));
        producer.Shutdown();
    }

    [Fact]
    public void SendAsyncHeaderCarriesBrokerNameAndTopicKeys()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_async_header");
        producer.CreateTopicKey = "AsyncTopicKey";
        producer.DefaultTopicQueueNums = 11;

        var cb = new Recorder();
        producer.SendAsync(Msg(10), cb, 5000);
        Assert.True(cb.WaitDone(1, 5000), string.Join(" / ", cb.Errors()));
        Assert.Equal(SendStatus.SendOk, cb.Results()[0]!.SendStatus);

        WireRecord? wire = cluster.FirstSendRequest();
        Assert.NotNull(wire);
        Assert.Equal(RequestCode.SendMessageV2, wire!.Code);
        Assert.Equal("broker-0", wire.Ext["n"]);
        Assert.Equal("AsyncTopicKey", wire.Ext["c"]);
        Assert.Equal("11", wire.Ext["d"]);
        producer.Shutdown();
    }

    [Fact]
    public void BatchSends_CarryPerSubClientIds_AndReportTheBatchId()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_batch_client_id");

        SendResult sync = producer.SendBatch(new List<Message> { Msg(), Msg(), Msg() });
        var cb = new Recorder();
        producer.SendBatchAsync(new List<Message> { Msg(), Msg(), Msg() }, cb, 5000);
        Assert.True(cb.WaitDone(1, 5000), string.Join(" / ", cb.Errors()));
        SendResult asyncResult = cb.Results()[0]!;

        foreach (int n in new[] { 0, 1 })
        {
            WireRecord? wire = cluster.SendRequestAt(n);
            Assert.NotNull(wire);
            SendResult r = n == 0 ? sync : asyncResult;

            // (1) Every sub-message in the body carries a distinct 32 hex digit client ID
            List<Message> subs = MessageDecoder.DecodeBatchMessages(wire!.Body);
            Assert.Equal(3, subs.Count);
            var seen = new HashSet<string>();
            foreach (Message m in subs)
            {
                string id = MessageClientIDSetter.GetUniqId(m);
                Assert.Equal(32, id.Length);
                Assert.True(seen.Add(id), "the UNIQ_KEY of every sub-message must be distinct");
                // The batch's own ID must not collide with a sub-message's, otherwise the two were not distinguished at all
                Assert.NotEqual(r.MsgId, id);
            }

            // (2) MsgId = the client's own ID for the batch (it is in the request properties); OffsetMsgId is the broker's one
            Assert.Equal(32, r.MsgId.Length);
            Assert.DoesNotContain(",", r.MsgId);
            Assert.Contains(r.MsgId, wire.Ext["i"]);
            Assert.StartsWith("MOCK-", r.OffsetMsgId);
            Assert.NotEqual(r.MsgId, r.OffsetMsgId);
        }

        producer.Shutdown();
    }

    /// <summary>
    /// When a batch passes the byte gate it is charged for the <b>whole batch</b> (the
    /// batch length accumulates per message, an empty body counts as 1). Writing "one"
    /// makes no send fail, it just lets a batch of 100 occupy a single slot of capacity --
    /// the byte gate becomes useless for batches, and it only shows up once a long run
    /// blows up the queue.
    /// </summary>
    [Fact]
    public void SendBatchAsync_ChargesWholeBatchToSizeGate()
    {
        using var cluster = MockCluster.Start(1);
        cluster.MakeSilent(0);
        DefaultMQProducer producer = Started(cluster, "GID_batch_async_bp");
        producer.EnableBackpressureForAsyncMode = true;
        producer.BackPressureForAsyncSendNum = 10;
        producer.BackPressureForAsyncSendSize = 1024 * 1024;
        const int each = 300 * 1024;
        List<Message> batch = new List<Message> { Msg(each), Msg(each) };
        var held = new Recorder();
        producer.SendBatchAsync(batch, held, 2000);

        long whole = 2L * each;
        Assert.True(WaitUntil(
            () => producer.SemaphoreAsyncSendSizeAvailablePermits == 1024 * 1024 - whole, 2000),
            "the byte gate must charge the whole batch, free=" + producer.SemaphoreAsyncSendSizeAvailablePermits);
        Assert.Equal(9, producer.SemaphoreAsyncSendNumAvailablePermits);

        var rejected = new Recorder();
        producer.SendBatchAsync(batch, rejected, 150);
        Assert.True(rejected.WaitDone(1, 5000));
        Assert.Equal("send message tryAcquire semaphoreAsyncSize timeout", rejected.Errors()[0]);
        Assert.Equal(9, producer.SemaphoreAsyncSendNumAvailablePermits);
        Assert.Equal(1024 * 1024 - whole, producer.SemaphoreAsyncSendSizeAvailablePermits);

        Assert.True(held.WaitDone(1, 8000), "the first send must still get a terminal state");
        Assert.Equal(1, cluster.CountRequests(RequestCode.SendBatchMessage));
        Assert.True(WaitUntil(
            () => producer.SemaphoreAsyncSendSizeAvailablePermits == 1024 * 1024, 5000));
        Assert.Equal(10, producer.SemaphoreAsyncSendNumAvailablePermits);
        producer.Shutdown();
    }

    /// <summary>When not started, batch async follows the same rule as single async:
    /// throws in place and not a single callback is delivered.</summary>
    [Fact]
    public void SendBatchAsync_BeforeStartThrowsSynchronously()
    {
        var producer = new DefaultMQProducer("GID_batch_async_nostart")
        {
            NamesrvAddr = "127.0.0.1:9876",
        };
        var cb = new Recorder();
        MQClientException e = Assert.Throws<MQClientException>(
            () => producer.SendBatchAsync(new List<Message> { Msg() }, cb, 1000));
        Assert.Contains("producer", e.Message);
        Assert.Equal(0, cb.Count);
    }
}
