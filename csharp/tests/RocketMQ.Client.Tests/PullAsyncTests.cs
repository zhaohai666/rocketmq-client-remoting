using System.Diagnostics;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class PullAsyncTests
{
    private const string Topic = "PullAsyncTopic";
    private const string GroupPrefix = "GID_PullAsync";

    private static MessageQueue Queue0() => new(Topic, MockCluster.BrokerName(0), 0);

    private static int Pulls(MockCluster cluster) => cluster.CountRequests(RequestCode.PullMessage);

    /// <summary>Waits for a callback to be accounted (either success or exception) and
    /// returns whether it arrived within the deadline.</summary>
    private static bool WaitForRec(Recorder rec, int timeoutMs = 8000) => rec.WaitForFirst(timeoutMs);

    /// <summary>A started pull-mode consumer, with a distinct instanceName per case (the
    /// clientId isolates the MqClient).</summary>
    private static DefaultMQPullConsumer Started(MockCluster cluster, string instance)
    {
        var c = new DefaultMQPullConsumer(GroupPrefix + "_" + instance);
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName(instance);
        c.Start();
        return c;
    }

    /// <summary>An IPullCallback stand-in that accounts exactly once; optionally "throws
    /// again inside the callback" to create escaping pressure.</summary>
    private sealed class Recorder : IPullCallback
    {
        private readonly ManualResetEventSlim _first = new(false);
        public int Successes;
        public int Exceptions;
        public PullResult? LastResult;
        public Exception? LastError;
        public long CallbackThreadId;
        public bool ThrowFromCallback;
        public bool Threw;

        public void OnSuccess(PullResult pullResult)
        {
            Interlocked.Increment(ref Successes);
            LastResult = pullResult;
            CallbackThreadId = Thread.CurrentThread.ManagedThreadId;
            _first.Set();
            if (ThrowFromCallback)
            {
                Threw = true;
                throw new InvalidOperationException("callback boom (must not escape)");
            }
        }

        public void OnException(Exception error)
        {
            Interlocked.Increment(ref Exceptions);
            LastError = error;
            CallbackThreadId = Thread.CurrentThread.ManagedThreadId;
            _first.Set();
            if (ThrowFromCallback)
            {
                Threw = true;
                throw new InvalidOperationException("callback boom (must not escape)");
            }
        }

        public bool WaitForFirst(int ms) => _first.Wait(ms);
    }

    private static Dictionary<string, string> PullResp(long next, long min = 0, long max = 9) => new()
    {
        ["nextBeginOffset"] = next.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ["minOffset"] = min.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ["maxOffset"] = max.ToString(System.Globalization.CultureInfo.InvariantCulture),
    };

    /// <summary>A message body in the 17-field storage format (the same encoding path as
    /// in LitePullCursorTests).</summary>
    private static byte[] BodyWithOneMessage(long queueOffset)
    {
        var m = new MessageExt
        {
            Topic = Topic,
            BrokerName = MockCluster.BrokerName(0),
            QueueId = 0,
            QueueOffset = queueOffset,
            SysFlag = 0,
            BornTimestamp = 1700000000000L,
            StoreTimestamp = 1700000000001L,
            BornHost = "127.0.0.1",
            BornHostPort = 10911,
            StoreHost = "127.0.0.1",
            StoreHostPort = 10911,
            CommitLogOffset = 4096,
            Body = Encoding.UTF8.GetBytes("hello-pull-async"),
        };
        return MessageDecoder.EncodeMessageExt(m, false);
    }

    // ---------------------------------------------------------------- 1. Success path

    [Fact]
    public void PullAsync_NoNewMsg_OnSuccessExactlyOnce_OffCallerThread()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQPullConsumer c = Started(cluster, "pa_basic");
        var rec = new Recorder();
        long callerThread = Thread.CurrentThread.ManagedThreadId;
        try
        {
            // No script = the faithful shape of an empty broker: PULL_NOT_FOUND + nextBeginOffset echoing the request offset
            c.PullAsync(Queue0(), "*", 100, 32, rec);
            Assert.True(WaitForRec(rec), "the callback must be accounted");
            Assert.Equal(1, rec.Successes);
            Assert.Equal(0, rec.Exceptions);
            Assert.NotNull(rec.LastResult);
            Assert.True(rec.LastResult!.IsNoNewMsg); // PullNotFound → NO_NEW_MSG
            Assert.Equal(100L, rec.LastResult!.NextBeginOffset);
            // The callback does not run on the calling thread (the minimum requirement of async semantics)
            Assert.NotEqual(callerThread, rec.CallbackThreadId);

            Thread.Sleep(150); // exactly once: a second, late arrival counts as a violation
            Assert.Equal(1, rec.Successes);
            Assert.Equal(1, Pulls(cluster)); // one call = one PULL_MESSAGE(11)
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void PullAsync_Found_DecodesMessagesAndFillsBrokerName()
    {
        using var cluster = MockCluster.Start(1);
        cluster.ScriptPull(ResponseCode.Success, PullResp(next: 1), BodyWithOneMessage(0));
        DefaultMQPullConsumer c = Started(cluster, "pa_found");
        var rec = new Recorder();
        try
        {
            c.PullAsync(Queue0(), "*", 0, 32, rec);
            Assert.True(WaitForRec(rec));
            Assert.Equal(1, rec.Successes);
            PullResult r = rec.LastResult!;
            Assert.True(r.IsFound);
            Assert.Equal(1L, r.NextBeginOffset);
            Assert.Single(r.MsgFoundList);
            MessageExt m = r.MsgFoundList[0];
            Assert.Equal(Topic, m.Topic);
            Assert.Equal(0L, m.QueueOffset);
            Assert.Equal("hello-pull-async", Encoding.UTF8.GetString(m.Body));
            // MqClient.PullMessage:1541 fills in brokerName/queueId from the request queue -- the async chain must not lose them
            Assert.Equal(MockCluster.BrokerName(0), m.BrokerName);
            Assert.Equal(0, m.QueueId);
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void PullAsync_OverloadByBrokerNameQueueId_SameSemantics()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQPullConsumer c = Started(cluster, "pa_overload");
        var rec = new Recorder();
        try
        {
            // The (topic, brokerName, queueId) locating version: no MessageQueue assembly, landing the same PULL_MESSAGE
            c.PullAsync(Topic, "*", MockCluster.BrokerName(0), 0, 7, 16, rec);
            Assert.True(WaitForRec(rec));
            Assert.Equal(1, rec.Successes);
            Assert.Equal(7L, rec.LastResult!.NextBeginOffset);
            WireRecord sent = cluster.Records().Last(r => r.Code == RequestCode.PullMessage);
            Assert.Equal("7", sent.Ext["queueOffset"]);
            Assert.Equal("16", sent.Ext["maxMsgNums"]);
        }
        finally
        {
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 2. Exception path

    [Fact]
    public void PullAsync_BrokerErrorCode_GoesToOnException_AsBrokerException()
    {
        using var cluster = MockCluster.Start(1);
        // SystemError(1) is not in the pull status code mapping table => the kernel throws MQBrokerException (MqClient:1521)
        cluster.ScriptPull(ResponseCode.SystemError);
        DefaultMQPullConsumer c = Started(cluster, "pa_brokererr");
        var rec = new Recorder();
        try
        {
            c.PullAsync(Queue0(), "*", 0, 32, rec);
            Assert.True(WaitForRec(rec));
            Assert.Equal(0, rec.Successes);
            Assert.Equal(1, rec.Exceptions);
            MQBrokerException ex = Assert.IsType<MQBrokerException>(rec.LastError);
            Assert.Equal(ResponseCode.SystemError, ex.ResponseCode);
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void PullAsync_SilentBrokerWithGate_CallerReturnsFast_TimeoutToOnException()
    {
        using var gate = new ManualResetEventSlim(false);
        using var cluster = MockCluster.Start(1);
        // The gate never answers: it turns the "the request reached the broker, the response
        // is forever on its way" window into a deterministic one
        // (the Fake is controllable, no dead port needed -- a Windows loopback refused
        // connection brings its own ~2s connect delay, which would pollute the timing
        // assertions).
        cluster.ScriptPull(ResponseCode.PullNotFound, gate: gate);
        DefaultMQPullConsumer c = Started(cluster, "pa_timeout");
        var rec = new Recorder();
        try
        {
            var sw = Stopwatch.StartNew();
            c.PullAsync(Queue0(), "*", 0, 32, rec, timeoutMillis: 800);
            long callElapsed = sw.ElapsedMilliseconds;
            // The calling thread returns immediately: it does not wait out the 800ms pull timeout
            Assert.True(callElapsed < 500, "PullAsync must not block the calling thread, elapsed=" + callElapsed + "ms");

            Assert.True(WaitForRec(rec), "a timeout must also go to onException");
            Assert.Equal(0, rec.Successes);
            Assert.Equal(1, rec.Exceptions);
            Assert.IsType<RemotingTimeoutException>(rec.LastError);
            sw.Stop();
            // The timeout uses the explicit timeoutMillis (800ms), not the default consumerPullTimeoutMillis (10s)
            Assert.True(sw.ElapsedMilliseconds < 6000,
                "the explicit 800ms timeout is the backstop, actual total=" + sw.ElapsedMilliseconds + "ms");
        }
        finally
        {
            gate.Set(); // let the broker's connection thread wind up (no teardown blocking beyond the Fake's 5s backstop)
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. Synchronous guards on the call stack

    [Fact]
    public void PullAsync_NotStarted_ThrowsSynchronously_AndCallbackUntouched()
    {
        using var cluster = MockCluster.Start(1);
        var c = new DefaultMQPullConsumer(GroupPrefix + "NotStarted");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        var rec = new Recorder();
        // A deliberate difference (the rule carried over from SendAsync): a configuration error throws on the call stack, it is not silently turned into onException
        MQClientException ex = Assert.Throws<MQClientException>(
            () => c.PullAsync(Queue0(), "*", 0, 32, rec));
        Assert.Contains("not started", ex.Message);
        Assert.Equal(0, rec.Successes + rec.Exceptions);

        // The overload is guarded too
        Assert.Throws<MQClientException>(
            () => c.PullAsync(Topic, "*", MockCluster.BrokerName(0), 0, 0, 32, rec));

        // A null callback: ThrowIfNull comes before getting the client -- so even when not started the argument is reported first
        Assert.Throws<ArgumentNullException>(
            () => c.PullAsync(Queue0(), "*", 0, 32, null!));
    }

    // ---------------------------------------------------------------- 4. A callback that throws again must not escape

    [Fact]
    public void PullAsync_CallbackThrows_IsSwallowed_AndConsumerKeepsWorking()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQPullConsumer c = Started(cluster, "pa_cbthrow");
        var angry = new Recorder { ThrowFromCallback = true };
        var calm = new Recorder();
        try
        {
            c.PullAsync(Queue0(), "*", 0, 32, angry);
            Assert.True(WaitForRec(angry));
            Assert.True(angry.Threw, "the callback really went through the throw branch");

            // The callback exception is only logged (PullConsumer.cs:786-793): the consumer is unaffected and the next pull succeeds as usual
            c.PullAsync(Queue0(), "*", 3, 32, calm);
            Assert.True(WaitForRec(calm));
            Assert.Equal(1, calm.Successes);
            Assert.Equal(3L, calm.LastResult!.NextBeginOffset);
        }
        finally
        {
            c.Shutdown();
        }
    }
}
