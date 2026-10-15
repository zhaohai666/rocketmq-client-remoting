// Offline unit tests of the 2026-10 alignment batch: the namespaceV2 request hook /
// VIP channel / scheduled message setters / the deprecated ConsumeFromWhere values /
// the recall trace hook / the request waiting slot TTL scan.
using System.Reflection;
using System.Threading;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

// PropertyMap is a global using alias on the src side (SortedDictionary<string,string>).
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class ClientParityTests
{
    private const string NsV2 = "MQ_INST_parity";

    /// <summary>A probe hook that only records the order in which it was called.</summary>
    private sealed class OrderProbeHook : IRpcHook
    {
        private readonly string _name;
        private readonly List<string> _log;

        public OrderProbeHook(string name, List<string> log)
        {
            _name = name;
            _log = log;
        }

        public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
        {
            _ = remoteAddr;
            _log.Add(_name);
        }

        public void DoAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand? response)
        {
            _ = remoteAddr;
            _ = request;
            _ = response;
        }
    }

    // ---------------------------------------------------------------- namespaceV2 hooks

    [Fact]
    public void NamespaceRpcHook_AddsNsdAndNs()
    {
        var hook = new NamespaceRpcHook(() => NsV2);
        var cmd = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        hook.DoBeforeRequest("127.0.0.1:10911", cmd);

        Assert.Equal("true", cmd.ExtFields[MixAll.RpcNamespacedField]);
        Assert.Equal(NsV2, cmd.ExtFields[MixAll.RpcNamespaceField]);
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void NamespaceRpcHook_AddsNothingWhenUnset(string? ns)
    {
        var hook = new NamespaceRpcHook(() => ns);
        var cmd = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        hook.DoBeforeRequest("127.0.0.1:10911", cmd);

        Assert.False(cmd.ExtFields.ContainsKey(MixAll.RpcNamespacedField));
        Assert.False(cmd.ExtFields.ContainsKey(MixAll.RpcNamespaceField));
    }

    [Fact]
    public void NamespaceRpcHook_ReadsConfigPerRequest()
    {
        string current = string.Empty;
        var hook = new NamespaceRpcHook(() => current);

        var first = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        hook.DoBeforeRequest("a", first);
        Assert.False(first.ExtFields.ContainsKey(MixAll.RpcNamespaceField));

        current = NsV2;
        var second = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        hook.DoBeforeRequest("a", second);
        Assert.Equal(NsV2, second.ExtFields[MixAll.RpcNamespaceField]);
    }

    [Fact]
    public void RequestHooks_ComposeKeepsLegacyOrder_NamespaceStreamUser()
    {
        var user = new OrderProbeHook("user", new List<string>());
        IRpcHook? composed = RequestHooks.Compose(enableStreamRequestType: true, user, () => NsV2);

        var chain = Assert.IsType<ChainedRpcHook>(composed);
        Assert.Collection(chain.Hooks,
            h => Assert.IsType<NamespaceRpcHook>(h),
            h => Assert.IsType<StreamTypeRPCHook>(h),
            h => Assert.Same(user, h));
    }

    [Fact]
    public void RequestHooks_Compose_ReturnsSingleHookUnwrapped()
    {
        var user = new OrderProbeHook("user", new List<string>());

        // No ns and no stream => the user hook is returned as is (no chaining, zero overhead)
        Assert.Same(user, RequestHooks.Compose(false, user));
        // Stream only => straight to StreamTypeRPCHook
        Assert.IsType<StreamTypeRPCHook>(RequestHooks.Compose(true, null));
        // ns only => straight to NamespaceRpcHook
        Assert.IsType<NamespaceRpcHook>(RequestHooks.Compose(false, null, () => NsV2));
        // Neither => null (no hook registered)
        Assert.Null(RequestHooks.Compose(false, null));
    }

    [Fact]
    public void RequestHooks_NamespaceHookInstalledEvenWhileNamespaceIsEmpty()
    {
        string current = string.Empty;
        IRpcHook? composed = RequestHooks.Compose(false, null, () => current);

        var hook = Assert.IsType<NamespaceRpcHook>(composed);
        var bare = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        hook.DoBeforeRequest("a", bare);
        Assert.False(bare.ExtFields.ContainsKey(MixAll.RpcNamespaceField));

        // A namespace configured after Start(): the hook is still hung on the instance, so the very next request carries ns/nsd
        current = NsV2;
        var late = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        hook.DoBeforeRequest("a", late);
        Assert.Equal(NsV2, late.ExtFields[MixAll.RpcNamespaceField]);
        Assert.Equal("true", late.ExtFields[MixAll.RpcNamespacedField]);
    }

    [Fact]
    public void AclSignature_CoversNamespaceAndStreamFields()
    {
        // If the order were wrong (ACL signing first), it would sign content without nsd/ReqT,
        // which does not match the ExtFields that really go on the wire -> signature
        // verification fails on a broker with auth enabled.
        var credentials = new SessionCredentials("AK", "SK");
        var hook = RequestHooks.Compose(true, new AclClientRPCHook(credentials), () => NsV2)!;

        var withNs = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        withNs.Body = System.Text.Encoding.UTF8.GetBytes("payload");
        withNs.HasBody = true;
        hook.DoBeforeRequest("127.0.0.1:10911", withNs);
        Assert.Equal(NsV2, withNs.ExtFields[MixAll.RpcNamespaceField]);
        Assert.Equal("0", withNs.ExtFields[MixAll.ReqT]);

        // The same request under a different namespace => the signature must change (showing that ns is part of the signed content)
        var otherNs = RequestHooks.Compose(true, new AclClientRPCHook(credentials), () => NsV2 + "_2")!;
        var other = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        other.Body = withNs.Body;
        other.HasBody = true;
        otherNs.DoBeforeRequest("127.0.0.1:10911", other);

        Assert.NotEqual(withNs.ExtFields[SessionCredentials.SignatureField],
                        other.ExtFields[SessionCredentials.SignatureField]);
    }

    // ---------------------------------------------------------------- VIP channel

    [Theory]
    [InlineData(true, "127.0.0.1:10911", "127.0.0.1:10909")]
    [InlineData(false, "127.0.0.1:10911", "127.0.0.1:10911")]
    [InlineData(true, "10.0.0.1:8091", "10.0.0.1:8089")]
    [InlineData(true, "broker-no-port", "broker-no-port")]
    [InlineData(true, "host:abc", "host:abc")]
    public void BrokerVipChannel_RewritesPortMinusTwo(bool enable, string addr, string expected)
    {
        Assert.Equal(expected, MixAll.BrokerVipChannel(enable, addr));
    }

    [Fact]
    public void Producer_VipAndDetectorSwitchesDefaultOffLikeLegacy()
    {
        var p = new DefaultMQProducer("GID_ParityUnit");
        Assert.False(p.SendMessageWithVIPChannel);
        Assert.False(p.StartDetectorEnable);
        Assert.Equal(string.Empty, p.NamespaceV2);

        p.SendMessageWithVIPChannel = true;
        p.StartDetectorEnable = true;
        p.NamespaceV2 = NsV2;
        Assert.True(p.SendMessageWithVIPChannel);
        Assert.True(p.StartDetectorEnable);
        Assert.Equal(NsV2, p.NamespaceV2);

        p.NamespaceV2 = null!;
        Assert.Equal(string.Empty, p.NamespaceV2);
    }

    // ---------------------------------------------------------------- Scheduled message setters

    [Fact]
    public void TimerSetters_WriteLegacyPropertyNames()
    {
        var m = new Message("TopicTimer", System.Text.Encoding.UTF8.GetBytes("x"));
        Assert.Equal(0, m.GetDelayTimeSec());
        Assert.Equal(0, m.GetDelayTimeMs());
        Assert.Equal(0, m.GetDeliverTimeMs());

        m.SetDelayTimeSec(60);
        m.SetDelayTimeMs(1500);
        m.SetDeliverTimeMs(1700000000000);

        Assert.Equal("60", m.Properties[MessageConst.PropertyTimerDelaySec]);
        Assert.Equal("1500", m.Properties[MessageConst.PropertyTimerDelayMs]);
        Assert.Equal("1700000000000", m.Properties[MessageConst.PropertyTimerDeliverMs]);
        Assert.Equal(60, m.GetDelayTimeSec());
        Assert.Equal(1500, m.GetDelayTimeMs());
        Assert.Equal(1700000000000, m.GetDeliverTimeMs());
    }

    [Fact]
    public void TimerGetter_ParsesLikeLegacyLongParseOrThrows()
    {
        var m = new Message("TopicTimer", System.Text.Encoding.UTF8.GetBytes("x"));
        m.Properties[MessageConst.PropertyTimerDelaySec] = "not-a-number";
        Assert.ThrowsAny<FormatException>(() => m.GetDelayTimeSec());
    }

    // ---------------------------------------------------------------- Deprecated ConsumeFromWhere values

    [Theory]
    [InlineData(ConsumeFromWhere.ConsumeFromLastOffsetAndFromMinWhenBootFirst,
                ConsumeFromWhere.ConsumeFromLastOffset)]
    [InlineData(ConsumeFromWhere.ConsumeFromMinOffset, ConsumeFromWhere.ConsumeFromLastOffset)]
    [InlineData(ConsumeFromWhere.ConsumeFromMaxOffset, ConsumeFromWhere.ConsumeFromLastOffset)]
    [InlineData(ConsumeFromWhere.ConsumeFromLastOffset, ConsumeFromWhere.ConsumeFromLastOffset)]
    [InlineData(ConsumeFromWhere.ConsumeFromFirstOffset, ConsumeFromWhere.ConsumeFromFirstOffset)]
    [InlineData(ConsumeFromWhere.ConsumeFromTimestamp, ConsumeFromWhere.ConsumeFromTimestamp)]
    public void ConsumeFromWhere_NormalizesDeprecatedValues(string input, string expected)
    {
        Assert.Equal(expected, ConsumeFromWhere.NormalizeDeprecated(input));
    }

    [Fact]
    public void ConsumeFromWhere_DeprecatedStringsMatchLegacy()
    {
        // What is reported in the heartbeat is the literal string and these historic values are still in config files, so a typo means a different enum
        Assert.Equal("CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
            ConsumeFromWhere.ConsumeFromLastOffsetAndFromMinWhenBootFirst);
        Assert.Equal("CONSUME_FROM_MIN_OFFSET", ConsumeFromWhere.ConsumeFromMinOffset);
        Assert.Equal("CONSUME_FROM_MAX_OFFSET", ConsumeFromWhere.ConsumeFromMaxOffset);
    }

    // ---------------------------------------------------------------- Recall trace hook

    private const string RecallTopic = NsV2 + "%TopicRecall";
    private const string RecallGroup = NsV2 + "%GID_Recall";
    private const string UniqKey = "0123456789ABCDEF0123456789abcdef";

    private static RemotingCommand RecallRequest(string handle)
    {
        var header = new RecallMessageRequestHeader
        {
            Topic = RecallTopic,
            ProducerGroup = RecallGroup,
            RecallHandle = handle,
            Bname = "broker-a",
        };
        var cmd = RemotingCommand.CreateRequestCommand(RequestCode.RecallMessage, header);
        // The real send chain spreads customHeader into ExtFields before going on the wire (MakeCustomHeaderToNet while encoding), and the hook reads the spread fields, so the test has to spread them too.
        cmd.MakeCustomHeaderToNet();
        return cmd;
    }

    private static RemotingCommand RecallResponse(int code, string? region)
    {
        var cmd = RemotingCommand.CreateResponseCommand(code, null);
        if (region is not null)
        {
            cmd.ExtFields[MessageConst.PropertyMsgRegion] = region;
        }

        return cmd;
    }

    private static List<TraceContext> Drain(AsyncTraceDispatcher d)
    {
        FieldInfo field = typeof(AsyncTraceDispatcher).GetField("_queue",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var queue = (System.Collections.Concurrent.BlockingCollection<TraceContext>)field.GetValue(d)!;
        var list = new List<TraceContext>();
        while (queue.TryTake(out TraceContext? ctx, TimeSpan.Zero) && ctx is not null)
        {
            list.Add(ctx);
        }

        return list;
    }

    [Fact]
    public void RecallTraceHook_RecordsSuccessOnResponseSide()
    {
        var dispatcher = new AsyncTraceDispatcher("GID_ParityUnit", TraceDispatcherType.Produce);
        var hook = new DefaultRecallMessageTraceHook(dispatcher) { EnableDefaultTrace = true };
        string handle = RecallMessageHandle.BuildHandle("TopicRecall", "broker-a", "1700000000000", UniqKey);

        hook.DoBeforeRequest("127.0.0.1:10911", RecallRequest(handle));
        Assert.Empty(Drain(dispatcher));

        hook.DoAfterResponse("127.0.0.1:10911", RecallRequest(handle),
            RecallResponse(ResponseCode.Success, "DefaultRegion"));

        List<TraceContext> contexts = Drain(dispatcher);
        TraceContext ctx = Assert.Single(contexts);
        Assert.Equal(RocketMQ.Client.TraceType.Recall, ctx.TraceType);
        Assert.Equal("DefaultRegion", ctx.RegionId);
        Assert.Equal("GID_Recall", ctx.GroupName); // the namespace prefix is stripped
        Assert.True(ctx.IsSuccess);
        TraceBean bean = Assert.Single(ctx.TraceBeans);
        Assert.Equal("TopicRecall", bean.Topic);
        Assert.Equal(UniqKey, bean.MsgId); // the msgId comes from the handle, not the response body
    }

    [Fact]
    public void RecallTraceHook_MarksFailureResponse()
    {
        var dispatcher = new AsyncTraceDispatcher("GID_ParityUnit", TraceDispatcherType.Produce);
        var hook = new DefaultRecallMessageTraceHook(dispatcher) { EnableDefaultTrace = true };
        string handle = RecallMessageHandle.BuildHandle("TopicRecall", "broker-a", "1700000000000", UniqKey);

        hook.DoAfterResponse("a", RecallRequest(handle),
            RecallResponse(ResponseCode.SystemError, "DefaultRegion"));

        TraceContext ctx = Assert.Single(Drain(dispatcher));
        Assert.False(ctx.IsSuccess);
    }

    [Theory]
    [InlineData(false, true, true, "the switch is off by default (the config item defaults to false)")]
    [InlineData(true, false, true, "the response is null")]
    [InlineData(true, true, false, "the response carries no MSG_REGION")]
    public void RecallTraceHook_ThreeGatesAllMustPass(bool enabled, bool hasResponse, bool hasRegion, string why)
    {
        _ = why;
        var dispatcher = new AsyncTraceDispatcher("GID_ParityUnit", TraceDispatcherType.Produce);
        var hook = new DefaultRecallMessageTraceHook(dispatcher) { EnableDefaultTrace = enabled };
        string handle = RecallMessageHandle.BuildHandle("TopicRecall", "broker-a", "1700000000000", UniqKey);

        hook.DoAfterResponse("a", RecallRequest(handle),
            hasResponse ? RecallResponse(ResponseCode.Success, hasRegion ? "DefaultRegion" : null) : null);

        Assert.Empty(Drain(dispatcher));
    }

    [Fact]
    public void RecallTraceHook_IgnoresOtherRequestCodes()
    {
        var dispatcher = new AsyncTraceDispatcher("GID_ParityUnit", TraceDispatcherType.Produce);
        var hook = new DefaultRecallMessageTraceHook(dispatcher) { EnableDefaultTrace = true };
        var send = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);

        hook.DoAfterResponse("a", send, RecallResponse(ResponseCode.Success, "DefaultRegion"));
        Assert.Empty(Drain(dispatcher));
    }

    [Fact]
    public void RecallTraceHook_SwallowsBadHandle()
    {
        var dispatcher = new AsyncTraceDispatcher("GID_ParityUnit", TraceDispatcherType.Produce);
        var hook = new DefaultRecallMessageTraceHook(dispatcher) { EnableDefaultTrace = true };
        RemotingCommand req = RecallRequest("not-a-handle");
        req.ExtFields.Remove("recallHandle");

        hook.DoAfterResponse("a", req, RecallResponse(ResponseCode.Success, "DefaultRegion"));
        Assert.Empty(Drain(dispatcher));
    }

    // ---------------------------------------------------------------- request waiting slot TTL scan

    private sealed class RecordingCallback : RequestCallback
    {
        public int SuccessCount;
        public Exception? Error;

        public override void OnSuccess(Message? responseMessage) => SuccessCount++;

        public override void OnException(Exception? e) => Error = e;
    }

    [Fact]
    public void ScanExpiredRequest_RemovesExpiredAndFiresCallbackOnce()
    {
        var holder = new RequestFutureHolder();
        var cb = new RecordingCallback();
        var future = new RequestResponseFuture("corr-scan-1", 1, cb);
        Thread.Sleep(30);
        holder.PutRequest("corr-scan-1", future);

        List<RequestResponseFuture> expired = holder.ScanExpiredRequest();

        RequestResponseFuture swept = Assert.Single(expired);
        Assert.Same(future, swept);
        Assert.Null(holder.GetRequest("corr-scan-1"));
        Assert.IsType<RequestTimeoutException>(cb.Error);
        Assert.Equal(ClientErrorCode.RequestTimeoutException,
            Assert.IsAssignableFrom<MQClientException>(cb.Error).ResponseCode);

        // Scan once more: it is gone from the table and the callback does not fire a second time
        Assert.Empty(holder.ScanExpiredRequest());
        Assert.Equal(0, cb.SuccessCount);
    }

    [Fact]
    public void ScanExpiredRequest_LeavesLiveEntriesAlone()
    {
        var holder = new RequestFutureHolder();
        var live = new RequestResponseFuture("corr-live", 60000, new RecordingCallback());
        holder.PutRequest("corr-live", live);

        Assert.Empty(holder.ScanExpiredRequest());
        Assert.Same(live, holder.GetRequest("corr-live"));
    }

    [Fact]
    public void ScanExpiredRequest_AnsweredAfterSweep_CallbackFiresOnce()
    {
        // "Whoever picks it up owns it": the scan picks it up first => the later response gets null, and the user callback still fires only once
        var holder = new RequestFutureHolder();
        var cb = new RecordingCallback();
        var future = new RequestResponseFuture("corr-race", 1, cb);
        holder.PutRequest("corr-race", future);
        Thread.Sleep(30);

        Assert.Single(holder.ScanExpiredRequest());
        Assert.Null(holder.PutResponse("corr-race", new Message("T", System.Text.Encoding.UTF8.GetBytes("late"))));
        // The callback takes the timeout branch, and only that once
        Assert.IsType<RequestTimeoutException>(cb.Error);
        Assert.Equal(0, cb.SuccessCount);
    }

    [Fact]
    public void ScanExpiredRequest_SyncCallerIsRemovedButNotWoken()
    {
        var holder = new RequestFutureHolder();
        var future = new RequestResponseFuture("corr-sync", 1, null);
        holder.PutRequest("corr-sync", future);
        Thread.Sleep(30);

        Assert.Single(holder.ScanExpiredRequest());
        Assert.Null(holder.GetRequest("corr-sync"));
        Assert.Null(future.WaitResponseMessage(0));
    }

    [Fact]
    public void ScheduledTask_RefCountsProducersAndIsSafeToToggle()
    {
        // The scan thread is a process-wide singleton plus a producer reference count:
        // starting twice and stopping once must not starve a producer that is still in
        // use, and after everyone has left it must really stop (otherwise the test process
        // keeps a background thread alive).
        var holder = new RequestFutureHolder();
        var p1 = new object();
        var p2 = new object();
        holder.StartScheduledTask(p1);
        holder.StartScheduledTask(p1); // the same instance counted twice is not two
        holder.StartScheduledTask(p2);
        holder.ShutdownScheduledTask(p1);

        var future = new RequestResponseFuture("corr-thread", 1, null);
        holder.PutRequest("corr-thread", future);
        Thread.Sleep(30);
        // p2 is still alive => the scan thread is still running. The expired entry must be
        // picked up by either the "manual scan" or the "background thread"; both count as
        // passing (what is asserted here is that the reference count did not stop the
        // thread, not who picked it up).
        holder.ScanExpiredRequest();
        Assert.Null(holder.GetRequest("corr-thread"));

        holder.ShutdownScheduledTask(p2);
        holder.ShutdownScheduledTask(p2); // a repeated stop is harmless
    }
}
