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

    /// <summary>等一笔回调落账（success 或 exception 任一），返回是否在期限内等到。</summary>
    private static bool WaitForRec(Recorder rec, int timeoutMs = 8000) => rec.WaitForFirst(timeoutMs);

    /// <summary>已启动的拉模式消费者，每用例独立 instanceName（clientId 隔离 MqClient）。</summary>
    private static DefaultMQPullConsumer Started(MockCluster cluster, string instance)
    {
        var c = new DefaultMQPullConsumer(GroupPrefix + "_" + instance);
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName(instance);
        c.Start();
        return c;
    }

    /// <summary>恰好一次记账的 IPullCallback 替身；可选「回调内再抛」制造逃逸压力。</summary>
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

    /// <summary>一篇 17 段存储格式的消息体（与 LitePullCursorTests 同一条编码路径）。</summary>
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

    // ---------------------------------------------------------------- 1. 成功路径

    [Fact]
    public void PullAsync_NoNewMsg_OnSuccessExactlyOnce_OffCallerThread()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQPullConsumer c = Started(cluster, "pa_basic");
        var rec = new Recorder();
        long callerThread = Thread.CurrentThread.ManagedThreadId;
        try
        {
            // 无脚本 = 空 broker 的忠实形态：PULL_NOT_FOUND + nextBeginOffset 回显请求位点
            c.PullAsync(Queue0(), "*", 100, 32, rec);
            Assert.True(WaitForRec(rec), "回调要落账");
            Assert.Equal(1, rec.Successes);
            Assert.Equal(0, rec.Exceptions);
            Assert.NotNull(rec.LastResult);
            Assert.True(rec.LastResult!.IsNoNewMsg); // PullNotFound → NO_NEW_MSG
            Assert.Equal(100L, rec.LastResult!.NextBeginOffset);
            // 回调不在调用线程上跑（异步语义的最低要求）
            Assert.NotEqual(callerThread, rec.CallbackThreadId);

            Thread.Sleep(150); // 恰好一次：迟到的第二笔算违规
            Assert.Equal(1, rec.Successes);
            Assert.Equal(1, Pulls(cluster)); // 一次调用 = 一笔 PULL_MESSAGE(11)
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
            // MqClient.PullMessage:1541 用请求队列补写 brokerName/queueId —— 异步链不许丢
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
            // (topic, brokerName, queueId) 定位版：免拼 MessageQueue，落同一笔 PULL_MESSAGE
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

    // ---------------------------------------------------------------- 2. 异常路径

    [Fact]
    public void PullAsync_BrokerErrorCode_GoesToOnException_AsBrokerException()
    {
        using var cluster = MockCluster.Start(1);
        // SystemError(1) 不在 pull 状态码映射表里 → 内核抛 MQBrokerException（MqClient:1521）
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
        // 挂闸不应答：把「请求到了 broker、应答永远在路上」的窗口拉成确定性的
        // （Fake 可控，不用死端口 —— Windows 回环拒连自带 ~2s 建连延迟，会污染计时断言。）
        cluster.ScriptPull(ResponseCode.PullNotFound, gate: gate);
        DefaultMQPullConsumer c = Started(cluster, "pa_timeout");
        var rec = new Recorder();
        try
        {
            var sw = Stopwatch.StartNew();
            c.PullAsync(Queue0(), "*", 0, 32, rec, timeoutMillis: 800);
            long callElapsed = sw.ElapsedMilliseconds;
            // 调用线程立即返回：不等满 800ms 的拉取超时
            Assert.True(callElapsed < 500, "PullAsync 不许阻塞调用线程，elapsed=" + callElapsed + "ms");

            Assert.True(WaitForRec(rec), "超时也要走 onException");
            Assert.Equal(0, rec.Successes);
            Assert.Equal(1, rec.Exceptions);
            Assert.IsType<RemotingTimeoutException>(rec.LastError);
            sw.Stop();
            // 超时用的是显式 timeoutMillis（800ms），不是默认 consumerPullTimeoutMillis（10s）
            Assert.True(sw.ElapsedMilliseconds < 6000,
                "800ms 显式超时兜底，actual total=" + sw.ElapsedMilliseconds + "ms");
        }
        finally
        {
            gate.Set(); // 放 broker 连接线程收工（Fake 侧 5s 兜底之外不卡 teardown）
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. 调用栈上的同步守卫

    [Fact]
    public void PullAsync_NotStarted_ThrowsSynchronously_AndCallbackUntouched()
    {
        using var cluster = MockCluster.Start(1);
        var c = new DefaultMQPullConsumer(GroupPrefix + "NotStarted");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        var rec = new Recorder();
        // 有意差异（口径从 SendAsync 沿用）：配置错误在调用栈上抛，不静默转 onException
        MQClientException ex = Assert.Throws<MQClientException>(
            () => c.PullAsync(Queue0(), "*", 0, 32, rec));
        Assert.Contains("not started", ex.Message);
        Assert.Equal(0, rec.Successes + rec.Exceptions);

        // 重载版同样守卫
        Assert.Throws<MQClientException>(
            () => c.PullAsync(Topic, "*", MockCluster.BrokerName(0), 0, 0, 32, rec));

        // null 回调：ThrowIfNull 排在取 client 之前 —— 未启动也先报参数
        Assert.Throws<ArgumentNullException>(
            () => c.PullAsync(Queue0(), "*", 0, 32, null!));
    }

    // ---------------------------------------------------------------- 4. 回调再抛不许逃逸

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
            Assert.True(angry.Threw, "回调确实走过 throw 分支");

            // 回调异常只留日志（PullConsumer.cs:786-793）：消费者不受牵连，下一笔照常成功
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
