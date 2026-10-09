// 2026-10 对齐批次的离线单测：namespaceV2 请求钩子 / VIP channel / 定时消息 setter /
// ConsumeFromWhere 弃用值 / recall 轨迹钩子 / request 等待槽 TTL 扫描。
//
// 这些能力都只有「写错就静默」的失败模式，真机矩阵很难稳定命中，所以逐条离线锁死：
//   * nsd/ns 必须在 ACL 签名**之前**写入 ExtFields，否则开鉴权的 broker 验签必失败
//     （Java MQClientAPIImpl:329-332 的注册顺序就是为此）；
//   * VIP 口是 port-2 的纯算术改写，改错就是把消息发到另一个进程；
//   * 定时消息三个键的名字与类型必须与 Java Message 一致（拼错=定时器不生效，broker
//     照收不误）；
//   * 扫描线程的「谁摘到谁回调」如果写反，超时和应答到达两条路径会各回调一次。
using System.Reflection;
using System.Threading;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

// PropertyMap 是 src 侧的 global using 别名（SortedDictionary<string,string>）。
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class ClientParityTests
{
    private const string NsV2 = "MQ_INST_parity";

    /// <summary>只记录自己被调用顺序的探针钩子。</summary>
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

    // ---------------------------------------------------------------- namespaceV2 钩子

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
        // Java 每笔请求现读 clientConfig.getNamespaceV2()：配置改了要跟着走，
        // 所以钩子持的是取值函数而不是字符串快照。
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
    public void RequestHooks_ComposeKeepsJavaOrder_NamespaceStreamUser()
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

        // 无 ns 无 stream ⇒ 用户钩子原样返回（不套链，零开销）
        Assert.Same(user, RequestHooks.Compose(false, user));
        // 只有 stream ⇒ 直接给 StreamTypeRPCHook
        Assert.IsType<StreamTypeRPCHook>(RequestHooks.Compose(true, null));
        // 只有 ns ⇒ 直接给 NamespaceRpcHook
        Assert.IsType<NamespaceRpcHook>(RequestHooks.Compose(false, null, () => NsV2));
        // 都没有 ⇒ null（不注册钩子）
        Assert.Null(RequestHooks.Compose(false, null));
    }

    /// <summary>
    /// Java 的 MQClientAPIImpl **无条件**注册 NamespaceRpcHook，命名空间为空时由钩子在
    /// doBeforeRequest 里自己直接返回。所以装不装钩子取决于「facade 有没有给取值函数」，
    /// 而不是「Start() 那一刻命名空间是不是空的」—— 后者会让
    /// <c>consumer.NamespaceV2 = "ns"</c> 在 start() 之后静默失效。
    /// </summary>
    [Fact]
    public void RequestHooks_NamespaceHookInstalledEvenWhileNamespaceIsEmpty()
    {
        string current = string.Empty;
        IRpcHook? composed = RequestHooks.Compose(false, null, () => current);

        var hook = Assert.IsType<NamespaceRpcHook>(composed);
        var bare = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        hook.DoBeforeRequest("a", bare);
        Assert.False(bare.ExtFields.ContainsKey(MixAll.RpcNamespaceField));

        // Start() 之后才配命名空间：钩子还挂在实例上，下一笔请求就带上 ns/nsd
        current = NsV2;
        var late = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        hook.DoBeforeRequest("a", late);
        Assert.Equal(NsV2, late.ExtFields[MixAll.RpcNamespaceField]);
        Assert.Equal("true", late.ExtFields[MixAll.RpcNamespacedField]);
    }

    [Fact]
    public void AclSignature_CoversNamespaceAndStreamFields()
    {
        // 顺序错了（ACL 先算签名）就会签出一份「不含 nsd/ReqT」的内容，
        // 与真正上线的 ExtFields 不一致 → 开鉴权的 broker 验签失败。
        var credentials = new SessionCredentials("AK", "SK");
        var hook = RequestHooks.Compose(true, new AclClientRPCHook(credentials), () => NsV2)!;

        var withNs = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, null);
        withNs.Body = System.Text.Encoding.UTF8.GetBytes("payload");
        withNs.HasBody = true;
        hook.DoBeforeRequest("127.0.0.1:10911", withNs);
        Assert.Equal(NsV2, withNs.ExtFields[MixAll.RpcNamespaceField]);
        Assert.Equal("0", withNs.ExtFields[MixAll.ReqT]);

        // 同一个请求换个命名空间 ⇒ 签名必须变（说明 ns 进了签名内容）
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
    // 没有冒号 / 端口非数字：与 Java 一样原样返回（不抛）
    [InlineData(true, "broker-no-port", "broker-no-port")]
    [InlineData(true, "host:abc", "host:abc")]
    public void BrokerVipChannel_RewritesPortMinusTwo(bool enable, string addr, string expected)
    {
        Assert.Equal(expected, MixAll.BrokerVipChannel(enable, addr));
    }

    [Fact]
    public void Producer_VipAndDetectorSwitchesDefaultOffLikeJava()
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

        // null 视同空（Java 的 setNamespaceV2 不做拼接，本端口归一成空串防 NRE）
        p.NamespaceV2 = null!;
        Assert.Equal(string.Empty, p.NamespaceV2);
    }

    // ---------------------------------------------------------------- 定时消息 setter

    [Fact]
    public void TimerSetters_WriteJavaPropertyNames()
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
    public void TimerGetter_ParsesLikeJavaLongParseOrThrows()
    {
        // Java 的 getter 就是 Long.parseLong：坏值直接抛，不能静默当 0
        var m = new Message("TopicTimer", System.Text.Encoding.UTF8.GetBytes("x"));
        m.Properties[MessageConst.PropertyTimerDelaySec] = "not-a-number";
        Assert.ThrowsAny<FormatException>(() => m.GetDelayTimeSec());
    }

    // ---------------------------------------------------------------- ConsumeFromWhere 弃用值

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
    public void ConsumeFromWhere_DeprecatedStringsMatchJava()
    {
        // 心跳里上报的是字面字符串，配置文件里还留着这些历史值，拼错就是换了一个枚举
        Assert.Equal("CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
            ConsumeFromWhere.ConsumeFromLastOffsetAndFromMinWhenBootFirst);
        Assert.Equal("CONSUME_FROM_MIN_OFFSET", ConsumeFromWhere.ConsumeFromMinOffset);
        Assert.Equal("CONSUME_FROM_MAX_OFFSET", ConsumeFromWhere.ConsumeFromMaxOffset);
    }

    // ---------------------------------------------------------------- recall 轨迹钩子

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
        // 真实发送链在上线前会把 customHeader 摊进 ExtFields（编码时 MakeCustomHeaderToNet），
        // 钩子读的是摊平后的字段，所以测试同样要先摊平。
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
        Assert.Empty(Drain(dispatcher)); // Java 同款：轨迹只记在响应侧

        hook.DoAfterResponse("127.0.0.1:10911", RecallRequest(handle),
            RecallResponse(ResponseCode.Success, "DefaultRegion"));

        List<TraceContext> contexts = Drain(dispatcher);
        TraceContext ctx = Assert.Single(contexts);
        Assert.Equal(RocketMQ.Client.TraceType.Recall, ctx.TraceType);
        Assert.Equal("DefaultRegion", ctx.RegionId);
        Assert.Equal("GID_Recall", ctx.GroupName); // 命名空间前缀剥掉
        Assert.True(ctx.IsSuccess);
        TraceBean bean = Assert.Single(ctx.TraceBeans);
        Assert.Equal("TopicRecall", bean.Topic);
        Assert.Equal(UniqKey, bean.MsgId); // msgId 来自句柄，不是响应体
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
    [InlineData(false, true, true, "开关默认关（Java 的系统属性默认 false）")]
    [InlineData(true, false, true, "响应为 null")]
    [InlineData(true, true, false, "响应没带 MSG_REGION")]
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
        // 轨迹是旁路观测：句柄格式坏了不能让撤回 RPC 本身抛（Java 同处 catch Exception 空处理）
        var dispatcher = new AsyncTraceDispatcher("GID_ParityUnit", TraceDispatcherType.Produce);
        var hook = new DefaultRecallMessageTraceHook(dispatcher) { EnableDefaultTrace = true };
        RemotingCommand req = RecallRequest("not-a-handle");
        req.ExtFields.Remove("recallHandle");

        hook.DoAfterResponse("a", req, RecallResponse(ResponseCode.Success, "DefaultRegion"));
        Assert.Empty(Drain(dispatcher));
    }

    // ---------------------------------------------------------------- request 等待槽 TTL 扫描

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
        // isTimeout 是**严格**大于（Java 同口径：timeout - elapsed < 0），所以留一点真实流逝
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

        // 再扫一次：表里已经没有了，回调也不会第二次触发
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
        // 「谁摘到谁负责」：扫描先摘走 ⇒ 后到的应答拿到 null，用户回调仍然只有一次
        var holder = new RequestFutureHolder();
        var cb = new RecordingCallback();
        var future = new RequestResponseFuture("corr-race", 1, cb);
        holder.PutRequest("corr-race", future);
        Thread.Sleep(30);

        Assert.Single(holder.ScanExpiredRequest());
        Assert.Null(holder.PutResponse("corr-race", new Message("T", System.Text.Encoding.UTF8.GetBytes("late"))));
        // 回调走的是超时分支，且只有这一次
        Assert.IsType<RequestTimeoutException>(cb.Error);
        Assert.Equal(0, cb.SuccessCount);
    }

    [Fact]
    public void ScanExpiredRequest_SyncCallerIsRemovedButNotWoken()
    {
        // 同步调用（无回调）自己在 WaitResponseMessage 的 latch 上超时，与 Java 一致：
        // 扫描只负责摘表，不能替它 set latch，否则同步方拿到 null 应答却分不清是超时还是空应答。
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
        // 扫描线程是进程级单例 + producer 引用计数：起两次停一次不能把还在用的 producer 饿死，
        // 全部退场后必须真的停（否则测试进程挂着后台线程）。
        var holder = new RequestFutureHolder();
        var p1 = new object();
        var p2 = new object();
        holder.StartScheduledTask(p1);
        holder.StartScheduledTask(p1); // 同一实例重复计入不算两个
        holder.StartScheduledTask(p2);
        holder.ShutdownScheduledTask(p1);

        var future = new RequestResponseFuture("corr-thread", 1, null);
        holder.PutRequest("corr-thread", future);
        Thread.Sleep(30);
        // p2 仍存活 ⇒ 扫描线程还在跑。到期条目必须由「手动扫」或「后台线程」之一摘掉，
        // 两者都算通过（这里断言的是引用计数没把线程停掉，而不是谁摘的）。
        holder.ScanExpiredRequest();
        Assert.Null(holder.GetRequest("corr-thread"));

        holder.ShutdownScheduledTask(p2);
        holder.ShutdownScheduledTask(p2); // 重复停无害
    }
}
