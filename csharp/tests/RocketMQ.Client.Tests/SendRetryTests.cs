// A real cluster cannot produce SYSTEM_BUSY, and it cannot produce "a slow broker eats
// the whole budget" either, yet those two are the entire difficulty of this kernel, so
// an in-process fake cluster is used here (MockCluster.cs: 1 namesrv + N brokers, speaking
// only the remoting protocol), scripting each broker's response code and response delay.
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class SendRetryTests
{
    private const string Topic = "T1";

    // ---------------------------------------------------------------- Cases

    private static DefaultMQProducer Started(MockCluster cluster, string group)
    {
        var producer = new DefaultMQProducer(group)
        {
            NamesrvAddr = cluster.NamesrvAddr,
            InstanceName = group,
        };
        producer.Start();
        return producer;
    }

    private static Message Msg() => new(Topic, Encoding.UTF8.GetBytes("hello"));

    [Fact]
    public void DefaultRetryResponseCodes_MatchLegacy()
    {
        var producer = new DefaultMQProducer("GID_RetryCodes");
        Assert.Equal(8, producer.RetryResponseCodes.Count);
        foreach (int code in new[]
                 {
                     ResponseCode.SystemError, ResponseCode.SystemBusy,
                     ResponseCode.ServiceNotAvailable, ResponseCode.NoPermission,
                     ResponseCode.TopicNotExist, ResponseCode.NoBuyerId,
                     ResponseCode.NotInCurrentUnit, ResponseCode.GoAway,
                 })
        {
            Assert.True(producer.IsRetryResponseCode(code), "code=" + code);
        }

        // Never getting a response code (the connection was not even established) counts as non-retryable; a code not in the set is non-retryable too
        Assert.False(producer.IsRetryResponseCode(null));
        Assert.False(producer.IsRetryResponseCode(ResponseCode.MessageIllegal));

        producer.AddRetryResponseCode(ResponseCode.MessageIllegal);
        Assert.True(producer.IsRetryResponseCode(ResponseCode.MessageIllegal));
        // RetryResponseCodes hands out a copy, so mutating it does not affect the decision
        producer.RetryResponseCodes.Clear();
        Assert.True(producer.IsRetryResponseCode(ResponseCode.MessageIllegal));
    }

    /// <summary>A retryable broker response code: resend to another broker and finally
    /// land on that healthy broker.</summary>
    [Fact]
    public void RetryableBrokerCode_SwitchesToAnotherBroker()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.ServiceNotAvailable, 0));
        cluster.Script(1, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryOk");

        SendResult result = producer.Send(Msg());
        Assert.Equal(SendStatus.SendOk, result.SendStatus);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(1, cluster.Requests(1));
        producer.Shutdown();
    }

    /// <summary>A non-retryable response code: not one retry, the exception is thrown to
    /// the caller as is.</summary>
    [Fact]
    public void NonRetryableBrokerCode_ThrowsAtOnce()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.MessageIllegal, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryNoRetry");

        MQBrokerException e = Assert.Throws<MQBrokerException>(() => producer.Send(Msg()));
        Assert.Equal(ResponseCode.MessageIllegal, e.ResponseCode);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(0, cluster.Requests(1));
        producer.Shutdown();
    }

    /// <summary>After adding a RetryResponseCode, a code that was originally
    /// non-retryable must also switch broker.</summary>
    [Fact]
    public void AddedRetryResponseCode_MakesCodeRetryable()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.MessageIllegal, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryAdded");
        producer.AddRetryResponseCode(ResponseCode.MessageIllegal);

        SendResult result = producer.Send(Msg());
        Assert.Equal(SendStatus.SendOk, result.SendStatus);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(1, cluster.Requests(1));
        producer.Shutdown();
    }

    /// <summary>Retries exhausted: the error code is taken from the last failing broker's
    /// response code, and the message lists the brokers it dealt with.</summary>
    [Fact]
    public void ExhaustedRetries_ReportBrokersSentAndCode()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.ServiceNotAvailable, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryExhaust");
        producer.RetryTimesWhenSendFailed = 2;

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ResponseCode.ServiceNotAvailable, e.ResponseCode);
        Assert.Equal(3, cluster.Requests(0));
        Assert.Contains("Send [3] times, still failed", e.Message);
        Assert.Contains("Topic: " + Topic, e.Message);
        Assert.Contains("BrokersSent: [broker-0, broker-0, broker-0]", e.Message);
        producer.Shutdown();
    }

    /// <summary>The route cannot be obtained at all: it is classified as NOT_FOUND_TOPIC
    /// outside the loop, without burning the retry count.</summary>
    [Fact]
    public void MissingRoute_FailsFastWithNotFoundTopicCode()
    {
        using var cluster = MockCluster.Start(1, routeOk: false);
        DefaultMQProducer producer = Started(cluster, "GID_RetryNoRoute");

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ClientErrorCode.NotFoundTopicException, e.ResponseCode);
        // Not a single broker was contacted -- the failure happened before queue selection
        Assert.Equal(0, cluster.Requests(0));
        producer.Shutdown();
    }

    /// Without this step, whoever misconfigured the address server would go check whether
    /// the topic exists, which is the completely wrong direction. Here the instance's
    /// address list is emptied after Start to simulate "the addresses vanished while
    /// running".
    /// </summary>
    [Fact]
    public void NoNameServerAddress_Reports10004NotMissingRoute()
    {
        using var cluster = MockCluster.Start(1, routeOk: false);
        DefaultMQProducer producer = Started(cluster, "GID_NoNamesrv");
        // NameServerAddrs exposes the instance's internal List itself, so Clear mutates it
        ((List<string>)producer.Client().NameServerAddrs).Clear();

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ClientErrorCode.NoNameServerException, e.ResponseCode);
        Assert.Equal("No name server address, please set it.", e.Message);
        Assert.Equal(0, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>
    /// The contrasting branch: the addresses are there, only this topic's route cannot be
    /// fetched -> 10004 must not be masked by 10005. This is the other side of the same
    /// check as <see cref="MissingRoute_FailsFastWithNotFoundTopicCode"/>.
    /// </summary>
    [Fact]
    public void NameServerConfigured_KeepsThe10005NoRouteCode()
    {
        using var cluster = MockCluster.Start(1, routeOk: false);
        DefaultMQProducer producer = Started(cluster, "GID_NamesrvOk");

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ClientErrorCode.NotFoundTopicException, e.ResponseCode);
        producer.Shutdown();
    }

    /// <summary>
    /// The ClientErrorCode table has exactly seven constants, none may be missing and
    /// none may have its value changed. 10001~10005 classify send retries; 10006/10007
    /// each have their own throw site (request-reply timeout, failure to build the reply
    /// message) -- the table used to lack these two, so a site had to throw with the
    /// default code 1.
    /// </summary>
    [Fact]
    public void ClientErrorCodeTable_MatchesLegacy()
    {
        var expected = new Dictionary<string, int>
        {
            ["ConnectBrokerException"] = 10001,
            ["AccessBrokerTimeout"] = 10002,
            ["BrokerNotExistException"] = 10003,
            ["NoNameServerException"] = 10004,
            ["NotFoundTopicException"] = 10005,
            ["RequestTimeoutException"] = 10006,
            ["CreateReplyMessageException"] = 10007,
        };
        var got = new Dictionary<string, int>();
        foreach (FieldInfo f in typeof(ClientErrorCode).GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            if (f.IsLiteral)
            {
                got[f.Name] = (int)f.GetRawConstantValue()!;
            }
        }

        Assert.Equal(expected, got);
    }

    /// <summary>Cannot connect to the broker: classified as CONNECT_BROKER_EXCEPTION
    /// (10001) instead of bubbling up as is.</summary>
    [Fact]
    public void ConnectFailure_IsQualifiedWith10001()
    {
        // Borrows a just-released local port as a "dead broker": the route is valid but
        // all three attempts fail to connect
        Socket probe = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        string deadAddr = ((IPEndPoint)probe.LocalEndPoint!).ToString();
        probe.Dispose();

        using var cluster = MockCluster.WithAddrs(new List<string> { deadAddr });
        DefaultMQProducer producer = Started(cluster, "GID_RetryConnect");
        producer.RetryTimesWhenSendFailed = 2;

        // A local loopback rejection of a "just released port" can be stretched to ~2s by
        // the firewall: the default 3s budget cannot cover 3 attempts and would be judged
        // callTimeout and throw a timeout; relax it so the retries run out and land in 10001.
        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg(), 30_000));
        Assert.Equal(ClientErrorCode.ConnectBrokerException, e.ResponseCode);
        Assert.Contains("Send [3] times, still failed", e.Message);
        producer.Shutdown();
    }

    /// <summary>The per-attempt timeout cap: a slow broker must not eat the whole budget,
    /// the remaining brokers still get a chance.</summary>
    [Fact]
    public void PerRequestTimeout_CapsSlowBrokerAndRetries()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)> { (ResponseCode.Success, 700) },
            (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryPerReq");
        producer.SendMsgMaxTimeoutPerRequest = 150;
        producer.SendMsgTimeout = 3000;

        var watch = Stopwatch.StartNew();
        SendResult result = producer.Send(Msg());
        watch.Stop();

        Assert.Equal(SendStatus.SendOk, result.SendStatus);
        // Without the cap the first attempt would have to wait the full 700ms to return; here it gives up on that slow broker after 150ms
        Assert.True(watch.ElapsedMilliseconds < 500,
            "elapsed=" + watch.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture));
        Assert.Equal(1, cluster.Requests(1));
        producer.Shutdown();
    }

    /// <summary>
    /// The whole budget is used up: throws RemotingTooMuchRequestException rather than
    /// "retries exhausted", and the second send must not go on the wire.
    [Fact]
    public void ExhaustedBudget_ReportsCallTimeout()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.SystemError, 0));
        var producer = new DefaultMQProducer("GID_RetryBudget")
        {
            NamesrvAddr = cluster.NamesrvAddr,
            InstanceName = "GID_RetryBudget",
            SendMsgTimeout = 100,
            RetryTimesWhenSendFailed = 2,
        };
        producer.RegisterSendMessageHook(new SleepBeforeHook(120));
        producer.Start();

        Assert.Throws<RemotingTooMuchRequestException>(() => producer.Send(Msg()));
        Assert.Equal(1, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>Sleeps fixed milliseconds inside SendMessageBefore: adds a deterministic
    /// amount of time to the send path itself.</summary>
    private sealed class SleepBeforeHook : ISendMessageHook
    {
        private readonly int _millis;

        public SleepBeforeHook(int millis) => _millis = millis;

        public string HookName() => "sleep-before";

        public void SendMessageBefore(SendMessageContext context) => Thread.Sleep(_millis);

        public void SendMessageAfter(SendMessageContext context)
        {
        }
    }

    /// <summary>"Stored but not stored well": returned as is by default, only switches
    /// broker when the switch is on.</summary>
    [Fact]
    public void NotStoreOk_OnlySwitchesBrokerWhenConfigured()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.FlushDiskTimeout, 0));

        DefaultMQProducer off = Started(cluster, "GID_NotStoreOff");
        SendResult kept = off.Send(Msg());
        Assert.Equal(SendStatus.FlushDiskTimeout, kept.SendStatus);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(0, cluster.Requests(1));
        off.Shutdown();

        cluster.Script(0, new List<(int, int)>(), (ResponseCode.FlushDiskTimeout, 0));
        DefaultMQProducer on = Started(cluster, "GID_NotStoreOn");
        on.RetryAnotherBrokerWhenNotStoreOk = true;
        SendResult switched = on.Send(Msg());
        Assert.Equal(SendStatus.SendOk, switched.SendStatus);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(1, cluster.Requests(1));
        on.Shutdown();
    }

    /// <summary>With fault avoidance on: the failing broker must be isolated (not
    /// selectable) and the successful one must record its real latency.</summary>
    [Fact]
    public void FailedBroker_IsIsolated_AndLatencyIsRecorded()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)> { (ResponseCode.ServiceNotAvailable, 30) },
            (ResponseCode.ServiceNotAvailable, 30));
        DefaultMQProducer producer = Started(cluster, "GID_RetryFault");
        producer.SendLatencyFaultEnable = true;

        SendResult result = producer.Send(Msg());
        Assert.Equal(SendStatus.SendOk, result.SendStatus);

        LatencyFaultToleranceImpl tolerance = producer.MqFaultStrategy.LatencyFaultTolerance;
        Assert.False(tolerance.IsAvailable("broker-0"), "the broker that returned an error must be isolated");
        FaultItem? failed = tolerance.GetFaultItem("broker-0");
        Assert.NotNull(failed);
        // What is recorded is still the real latency (~30ms), not the isolation tier (10000ms)
        Assert.InRange(failed!.CurrentLatency, 10.0, 600.0);
        Assert.False(failed.IsReachable());
        FaultItem? ok = tolerance.GetFaultItem("broker-1");
        Assert.NotNull(ok);
        Assert.True(ok!.IsAvailable());
        Assert.True(ok.IsReachable());
        producer.Shutdown();
    }

    // ------------------------------------------------------- The request hook really reaches the socket

    private const string WireSk = "SK_wire_12345678";

    [Fact]
    public void RequestHooksReachTheWire()
    {
        // ---- (1) ACL only: the signature fields are present, ReqT is not ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultMQProducer producer = StartedWithHook(cluster, "GID_HookAclOnly", false);
            Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);

            WireRecord? rec = cluster.FirstSendRequest();
            Assert.NotNull(rec);
            Assert.Equal("AK_wire", rec!.Ext[SessionCredentials.AccessKeyField]);
            Assert.True(rec.Ext.ContainsKey(SessionCredentials.SignatureField));
            Assert.False(rec.Ext.ContainsKey(MixAll.ReqT), "ReqT must not be sent without stream enabled");
            producer.Shutdown();
        }

        // ---- (2) ACL + stream: ReqT is written **before** signing, so it counts towards the signature ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultMQProducer producer = StartedWithHook(cluster, "GID_HookStreamAcl", true);
            Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);

            WireRecord rec = cluster.FirstSendRequest()!;
            Assert.Equal("0", rec.Ext[MixAll.ReqT]);
            // Recomputed on the broker side: recompute the signature once over the captured
            // request (including ReqT, excluding Signature)
            Assert.Equal(rec.Ext[SessionCredentials.SignatureField],
                AclClientRPCHook.CalcSignature(WireSk, rec.ToCommand()));
            producer.Shutdown();
        }

        // ---- (3) a lite consumer enables stream by default: both the route and the heartbeat carry the flag ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultLitePullConsumer consumer = StartedLite(cluster, "GID_HookLite", stream: null);
            Assert.True(cluster.CountRequestsWith(RequestCode.GetRouteinfoByTopic,
                MixAll.ReqT, "0") > 0, "a route request must carry ReqT");
            Assert.Equal(cluster.CountRequests(RequestCode.GetRouteinfoByTopic),
                cluster.CountRequestsWith(RequestCode.GetRouteinfoByTopic, MixAll.ReqT, "0"));
            Assert.True(cluster.CountRequestsWith(RequestCode.HeartBeat, MixAll.ReqT, "0") > 0,
                "a heartbeat must carry ReqT");
            consumer.Shutdown();
        }

        // ---- (4) explicitly disabled: not one request carries it, but the request really went out ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultLitePullConsumer consumer = StartedLite(cluster, "GID_HookLiteOff", stream: false);
            Assert.True(cluster.CountRequests(RequestCode.GetRouteinfoByTopic) > 0);
            Assert.False(cluster.AnyRequestHas(RequestCode.GetRouteinfoByTopic, MixAll.ReqT));
            Assert.False(cluster.AnyRequestHas(RequestCode.HeartBeat, MixAll.ReqT));
            consumer.Shutdown();
        }
    }

    /// Note: the request code and the single-letter key <c>m</c> (batch) of the V2 header
    /// are two different things: the broker uses <c>m</c> to choose between
    /// sendBatchMessage and a single write (SendMessageProcessor:117 reads
    /// requestHeader.isBatch()), while the code only affects how the server categorises
    /// by code (proxy AbstractRemotingActivity:69 and auth
    /// DefaultAuthorizationContextBuilder:230-240 both list 310/320 in the same case).
    /// So both are evidenced: matching the code but not <c>m</c> makes the batch body get
    /// parsed as a single message.
    /// </summary>
    [Fact]
    public void SendRequestCodeFollowsLegacy()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQProducer producer = Started(cluster, "GID_SendCodeBranch");

        cluster.ClearRequests();
        Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);
        WireRecord single = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendMessageV2, single.Code);
        Assert.Equal("false", single.Ext["m"]);

        cluster.ClearRequests();
        Assert.Equal(SendStatus.SendOk,
            producer.SendBatch(new List<Message> { Msg(), Msg() }).SendStatus);
        WireRecord batch = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendBatchMessage, batch.Code);
        Assert.Equal("true", batch.Ext["m"]);

        // reply is judged before batch: a batch carrying MSG_TYPE=reply still goes to 325.
        cluster.ClearRequests();
        Message replyBatch = Msg();
        replyBatch.IsBatch = true;
        replyBatch.Properties[MessageConst.PropertyMessageType] = MixAll.REPLY_MESSAGE_FLAG;
        Assert.Equal(SendStatus.SendOk, producer.Send(replyBatch).SendStatus);
        WireRecord reply = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendReplyMessageV2, reply.Code);
        Assert.Equal("true", reply.Ext["m"]);

        producer.Shutdown();
    }

    /// <c>c</c>/<c>d</c> are a functional matter: when the broker auto-creates a topic it
    /// uses these two values to decide the queue count
    /// (<c>AbstractSendMessageProcessor.createTopicInSendMessageMethod</c>), so hard-coding
    /// TBW102/4 turns <c>CreateTopicKey</c> / <c>DefaultTopicQueueNums</c> into fake setters.
    /// <c>n</c> is the on-the-wire peer of those: a classic broker addresses by connection
    /// address and never reads it, while the proxy and the audit/trace side do.
    ///
    /// All three entry points (synchronous / batch 320 / oneway) must carry the same
    /// values, and <c>d=0</c> must go on the wire verbatim -- 0 means "the caller explicitly
    /// said 0", not "not configured". The async entry point's copy is covered by
    /// ProducerAsyncTests.SendAsyncHeaderCarriesBrokerNameAndTopicKeys.
    /// </summary>
    [Fact]
    public void SendHeaderCarriesBrokerNameAndTopicKeys()
    {
        using var cluster = MockCluster.Start(2);
        DefaultMQProducer producer = Started(cluster, "GID_SendHeaderFields");

        cluster.ClearRequests();
        Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);
        WireRecord first = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendMessageV2, first.Code);
        Assert.Equal("broker-0", first.Ext["n"]);
        Assert.Equal(MixAll.DefaultTopic, first.Ext["c"]);
        Assert.Equal("4", first.Ext["d"]);

        // ---- (2) c/d follow the producer config; when round-robin moves to broker-1, n moves with it
        cluster.ClearRequests();
        producer.CreateTopicKey = "CreatedTopicKey";
        producer.DefaultTopicQueueNums = 9;
        Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);
        WireRecord second = cluster.FirstSendRequest()!;
        Assert.Equal("broker-1", second.Ext["n"]); // n is the broker this send selected, not the first one in the route
        Assert.Equal("CreatedTopicKey", second.Ext["c"]);
        Assert.Equal("9", second.Ext["d"]);

        // ---- (3) the batch goes through the same header-building function (the code 320 and m=true are covered by the previous section)
        cluster.ClearRequests();
        Assert.Equal(SendStatus.SendOk,
            producer.SendBatch(new List<Message> { Msg(), Msg() }).SendStatus);
        WireRecord batch = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendBatchMessage, batch.Code);
        Assert.Equal("CreatedTopicKey", batch.Ext["c"]);
        Assert.Equal("9", batch.Ext["d"]);
        Assert.True(batch.Ext.ContainsKey("n"), "a batch must carry brokerName too");

        // ---- (4) oneway: d=0 must go on the wire verbatim, it must not be overridden by the default 4
        cluster.ClearRequests();
        producer.DefaultTopicQueueNums = 0;
        producer.SendOneway(Msg());
        Assert.True(WaitUntil(() => cluster.FirstSendRequest() != null, 3000), "the oneway request must be captured");
        WireRecord oneway = cluster.FirstSendRequest()!;
        // "0" is the caller explicitly saying 0, it must not be overridden by the default 4
        Assert.Equal("0", oneway.Ext["d"]);
        Assert.Equal("CreatedTopicKey", oneway.Ext["c"]);
        Assert.True(oneway.Ext.ContainsKey("n"), "oneway must carry brokerName too");

        producer.Shutdown();
    }

    [Fact]
    public void PinnedSendBatch_LandsOnTheGivenQueue()
    {
        using var cluster = MockCluster.Start(2);
        DefaultMQProducer producer = Started(cluster, "GID_BatchPinned");

        cluster.ClearRequests();
        var pinned = new MessageQueue(Topic, MockCluster.BrokerName(1), 1);
        Assert.Equal(SendStatus.SendOk,
            producer.SendBatch(new List<Message> { Msg(), Msg() }, pinned).SendStatus);
        WireRecord batch = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendBatchMessage, batch.Code);
        Assert.Equal("true", batch.Ext["m"]);
        Assert.Equal("1", batch.Ext["e"]);
        Assert.Equal(MockCluster.BrokerName(1), batch.Ext["n"]);
        producer.Shutdown();
    }

    [Fact]
    public void PinnedSendRefusesAMismatchedTopic_BeforeAnyRequest()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQProducer producer = Started(cluster, "GID_PinnedGuard");
        cluster.ClearRequests();

        MQClientException e = Assert.Throws<MQClientException>(() =>
            producer.Send(Msg(), new MessageQueue("T2", MockCluster.BrokerName(0), 0)));
        Assert.Equal("message's topic not equal mq's topic", e.Message);
        Assert.Equal(0, cluster.Requests(0));
        Assert.Null(cluster.FirstSendRequest());

        // The same topic still sends normally (the guard must not be so wide it hits valid sends)
        Assert.Equal(SendStatus.SendOk, producer
            .Send(Msg(), new MessageQueue(Topic, MockCluster.BrokerName(0), 0)).SendStatus);
        Assert.Equal(Topic, cluster.FirstSendRequest()!.Ext["b"]);

        // A batch shares the same guard: a mismatch is rejected with zero on the wire; a batch with the same topic still sends
        int sent = cluster.Records().Count(r => IsSend(r.Code));
        Assert.Throws<MQClientException>(() => producer.SendBatch(
            new List<Message> { Msg(), Msg() }, new MessageQueue("T2", MockCluster.BrokerName(0), 0)));
        Assert.Equal(sent, cluster.Records().Count(r => IsSend(r.Code)));
        Assert.Equal(SendStatus.SendOk, producer.SendBatch(
            new List<Message> { Msg(), Msg() },
            new MessageQueue(Topic, MockCluster.BrokerName(0), 0)).SendStatus);
        Assert.Equal(RequestCode.SendBatchMessage, cluster.SendRequestAt(sent)!.Code);
        producer.Shutdown();
    }

    private static bool IsSend(int code) =>
        code is RequestCode.SendMessage or RequestCode.SendMessageV2
            or RequestCode.SendBatchMessage or RequestCode.SendReplyMessageV2;

    [Fact]
    public void PinnedOnewayHasNoTopicGuard_AndTakesOnlyTheQueueIdFromTheMq()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQProducer producer = Started(cluster, "GID_OnewayPinned");
        cluster.ClearRequests();

        producer.SendOneway(Msg(), new MessageQueue("T2", MockCluster.BrokerName(0), 3));
        Assert.True(WaitUntil(() => cluster.FirstSendRequest() != null, 3000), "the oneway request must be captured");
        WireRecord oneway = cluster.FirstSendRequest()!;
        // If a guard existed this send would throw; it went out and b is the msg's own topic (not the queue's T2)
        Assert.Equal(Topic, oneway.Ext["b"]);
        Assert.Equal("3", oneway.Ext["e"]);
        Assert.Equal(MockCluster.BrokerName(0), oneway.Ext["n"]);
        producer.Shutdown();
    }

    /// <summary>
    /// Under a namespace the pinned guard compares the resource names **after each side
    /// is wrapped**: a queue topic that already carries the prefix (exactly what a real
    /// cluster's route returns) must not be rejected by mistake, a bare name is let
    /// through too, and a same-named topic under a different namespace is still a
    /// different resource.
    /// </summary>
    [Fact]
    public void PinnedSendComparesTopicsAfterNamespaceWrapping()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQProducer producer = Started(cluster, "GID_PinnedGuardNs");
        producer.Namespace = "ns1";
        cluster.ClearRequests();

        // (1) The queue topic already carries the prefix
        Assert.Equal(SendStatus.SendOk, producer
            .Send(Msg(), new MessageQueue("ns1%T1", MockCluster.BrokerName(0), 0)).SendStatus);
        Assert.Equal("ns1%T1", cluster.FirstSendRequest()!.Ext["b"]);

        // (2) The queue topic is a bare name: equal after wrapping on both sides, so it is let through too
        Assert.Equal(SendStatus.SendOk, producer
            .Send(Msg(), new MessageQueue(Topic, MockCluster.BrokerName(0), 0)).SendStatus);
        Assert.Equal("ns1%T1", cluster.SendRequestAt(1)!.Ext["b"]);

        // (3) A same-named topic under another namespace: must be rejected, with not one extra send on the wire
        MQClientException e = Assert.Throws<MQClientException>(() => producer
            .Send(Msg(), new MessageQueue("ns2%T1", MockCluster.BrokerName(0), 0)));
        Assert.Equal("message's topic not equal mq's topic", e.Message);
        Assert.Null(cluster.SendRequestAt(2));
        producer.Shutdown();
    }

    [Fact]
    public void EmptyBrokerNameStaysOutOfTheSendHeader()
    {
        var inst = new MQClientInstance(
            "SendHeaderOff_" + Environment.CurrentManagedThreadId.ToString(CultureInfo.InvariantCulture),
            new List<string> { "127.0.0.1:9876" });
        var msg = new Message(Topic, Encoding.UTF8.GetBytes("hello"));

        RemotingCommand withName = inst.BuildSendRequest("PG_Header", msg,
            new MessageQueue(Topic, "broker-a", 0));
        withName.MakeCustomHeaderToNet();
        Assert.Equal("broker-a", withName.ExtFields["n"]);

        var noName = new Message(Topic, Encoding.UTF8.GetBytes("hello"));
        RemotingCommand without = inst.BuildSendRequest("PG_Header", noName,
            new MessageQueue(Topic, "", 0));
        without.MakeCustomHeaderToNet();
        Assert.False(without.ExtFields.ContainsKey("n"), "an empty brokerName must not go on the wire");
        Assert.Equal(MixAll.DefaultTopic, without.ExtFields["c"]);
        Assert.Equal("4", without.ExtFields["d"]);
    }

    /// <summary>Polls until the condition holds (a oneway send has no response to wait
    /// for).</summary>
    private static bool WaitUntil(Func<bool> cond, int millis)
    {
        Stopwatch watch = Stopwatch.StartNew();
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

    private static DefaultMQProducer StartedWithHook(MockCluster cluster, string group, bool stream)
    {
        var producer = new DefaultMQProducer(group)
        {
            NamesrvAddr = cluster.NamesrvAddr,
            InstanceName = group,
            EnableStreamRequestType = stream,
        };
        producer.SetRpcHook(new AclClientRPCHook(new SessionCredentials("AK_wire", WireSk)));
        producer.Start();
        return producer;
    }

    private static DefaultLitePullConsumer StartedLite(MockCluster cluster, string group,
        bool? stream)
    {
        var consumer = new DefaultLitePullConsumer(group);
        consumer.SetInstanceName(group);
        if (stream.HasValue)
        {
            consumer.EnableStreamRequestType = stream.Value;
        }

        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.Subscribe(Topic, "*");
        consumer.Start();
        return consumer;
    }
}
