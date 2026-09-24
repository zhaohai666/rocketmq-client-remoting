// 发送重试内核（Java DefaultMQProducerImpl#sendDefaultImpl）的离线对拍。
//
// 真集群造不出 SYSTEM_BUSY，也造不出「慢 broker 把总预算吃光」，而这两条恰好是这段
// 内核的全部难点，所以这里用进程内假集群（MockCluster.cs：1 个 namesrv + N 个 broker，
// 只说 remoting 协议），把每个 broker 的应答码和应答延迟脚本化。
//
// 与 python/tests/test_send_retry.py、cpp/tests/test_send_retry.cpp、
// rust/src/client/producer/send_retry_tests.rs 同题。
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

    // ---------------------------------------------------------------- 用例

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

    /// <summary>默认集合与 Java DefaultMQProducer#retryResponseCodes 逐项一致（8 个码）。</summary>
    [Fact]
    public void DefaultRetryResponseCodes_MatchJava()
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

        // 没等到响应码（连接就没建立）等于不可重试；不在集合里的码也不可重试
        Assert.False(producer.IsRetryResponseCode(null));
        Assert.False(producer.IsRetryResponseCode(ResponseCode.MessageIllegal));

        producer.AddRetryResponseCode(ResponseCode.MessageIllegal);
        Assert.True(producer.IsRetryResponseCode(ResponseCode.MessageIllegal));
        // RetryResponseCodes 给的是副本，改它不影响判定
        producer.RetryResponseCodes.Clear();
        Assert.True(producer.IsRetryResponseCode(ResponseCode.MessageIllegal));
    }

    /// <summary>可重试的 broker 响应码：换一台重发，最终落在那台健康的 broker 上。</summary>
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

    /// <summary>不可重试的响应码：一次都不该重试，异常原样抛给调用方。</summary>
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

    /// <summary>加了RetryResponseCode 之后，原本不可重试的码也要换 broker。</summary>
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

    /// <summary>重试耗尽：错误码取最后一次失败的 broker 响应码，文案带上打过交道的 broker 列表。</summary>
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

    /// <summary>路由完全取不到：循环外就按 NOT_FOUND_TOPIC 定性，不把重试次数空转掉。</summary>
    [Fact]
    public void MissingRoute_FailsFastWithNotFoundTopicCode()
    {
        using var cluster = MockCluster.Start(1, routeOk: false);
        DefaultMQProducer producer = Started(cluster, "GID_RetryNoRoute");

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ClientErrorCode.NotFoundTopicException, e.ResponseCode);
        // 一次 broker 都没联系过 —— 失败发生在选队列之前
        Assert.Equal(0, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>
    /// Java <c>validateNameServerSetting</c>（DefaultMQProducerImpl:729）：一个 name server 地址
    /// 都没有时报 10004，而不是把寻址故障说成"这个 topic 没路由"（10005）。
    ///
    /// 少了这一步，配错地址服务器的运维会去查 topic 存在不存在，方向完全错。
    /// 这里在 Start 之后把实例的地址列表清空，模拟"跑着跑着地址没了"。
    /// </summary>
    [Fact]
    public void NoNameServerAddress_Reports10004NotMissingRoute()
    {
        using var cluster = MockCluster.Start(1, routeOk: false);
        DefaultMQProducer producer = Started(cluster, "GID_NoNamesrv");
        // NameServerAddrs 暴露的是实例内部那个 List 本身，所以 Clear 就是改它
        ((List<string>)producer.Client().NameServerAddrs).Clear();

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ClientErrorCode.NoNameServerException, e.ResponseCode);
        Assert.Equal("No name server address, please set it.", e.Message);
        Assert.Equal(0, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>
    /// 对照分支：地址在、只是这个 topic 拉不到路由 → 10004 不能把 10005 顶掉。
    /// 与 <see cref="MissingRoute_FailsFastWithNotFoundTopicCode"/> 同一条检查的两面。
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
    /// client/src/main/java/org/apache/rocketmq/client/common/ClientErrorCode.java 一共七个常量，
    /// 一个都不能少、一个都不能改值。10001~10005 是发送重试的定性；10006/10007 各有各的抛出点
    /// （request-reply 超时、造应答消息失败），以前表里缺这两个，站点只能拿默认码 1 抛出去。
    /// </summary>
    [Fact]
    public void ClientErrorCodeTable_MatchesJava()
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

    /// <summary>连不上 broker：定性成 CONNECT_BROKER_EXCEPTION(10001)，而不是原样冒泡。</summary>
    [Fact]
    public void ConnectFailure_IsQualifiedWith10001()
    {
        // 借一个刚释放的本地端口当"死 broker"：路由有效，但三次尝试都连不上
        Socket probe = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        string deadAddr = ((IPEndPoint)probe.LocalEndPoint!).ToString();
        probe.Dispose();

        using var cluster = MockCluster.WithAddrs(new List<string> { deadAddr });
        DefaultMQProducer producer = Started(cluster, "GID_RetryConnect");
        producer.RetryTimesWhenSendFailed = 2;

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ClientErrorCode.ConnectBrokerException, e.ResponseCode);
        Assert.Contains("Send [3] times, still failed", e.Message);
        producer.Shutdown();
    }

    /// <summary>单次超时上限：慢 broker 不能把整个预算吃光，剩下的 broker 还有机会。</summary>
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
        // 没有上限的话第一次就要等满 700ms 才返回；这里 150ms 就放弃了那台慢 broker
        Assert.True(watch.ElapsedMilliseconds < 500,
            "elapsed=" + watch.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture));
        Assert.Equal(1, cluster.Requests(1));
        producer.Shutdown();
    }

    /// <summary>
    /// 总预算用完：抛 RemotingTooMuchRequestException 而不是「重试耗尽」，且第二笔不许上线。
    ///
    /// ⚠ 预算是让**发送钩子睡 120ms** 吃掉的，不靠 broker 的应答延迟：后者的耗时恰好等于
    /// 本次调用的预算（100ms 的等待在 100.0~100.9ms 之间返回完全正常），而判定是
    /// <c>timeout &lt; costTime</c> 的整数毫秒比较（Java 用 currentTimeMillis，同样如此），
    /// 卡在边界上就会偶发地多跑一笔 0ms 预算的尝试、最后抛成「重试耗尽」——实测整跑测试集时
    /// 会随机红一次。python 同题用例（test_send_retry.py
    /// ::test_call_timeout_stops_retrying_and_raises_too_much_request）用的是同一招：
    /// 往发送路径注入 first_send_sleep_ms=60 &gt; timeout=30。
    /// </summary>
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

    /// <summary>在 SendMessageBefore 里睡 fixed 毫秒：把耗时确定地加进发送路径本身。</summary>
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

    /// <summary>「存了但没存好」：默认原样返回，开了开关才换 broker。</summary>
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

    /// <summary>故障规避打开时：失败的 broker 要被隔离（不可选），成功的那台要记下真实延迟。</summary>
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
        Assert.False(tolerance.IsAvailable("broker-0"), "回错误的 broker 必须被隔离");
        FaultItem? failed = tolerance.GetFaultItem("broker-0");
        Assert.NotNull(failed);
        // 记的仍是真实延迟（~30ms），而不是隔离档位（10000ms）
        Assert.InRange(failed!.CurrentLatency, 10.0, 600.0);
        Assert.False(failed.IsReachable());
        FaultItem? ok = tolerance.GetFaultItem("broker-1");
        Assert.NotNull(ok);
        Assert.True(ok!.IsAvailable());
        Assert.True(ok.IsReachable());
        producer.Shutdown();
    }

    // ------------------------------------------------------- 请求钩子真的写到 socket 上

    private const string WireSk = "SK_wire_12345678";

    /// <summary>
    /// 钩子的四段取证，全部只看**抓下来的报文**：
    /// ① 只注册 ACL → 有 AccessKey/Signature、没有 ReqT；
    /// ② ACL + stream → 有 ReqT="0"，且把抓到的报文按 broker 的口径复算 HMAC 能对上
    ///    （证明 ReqT 落在签名内容里，而不是签完之后又改了几个字段）；
    /// ③ lite 消费者（Java 默认开 stream）→ 路由与心跳都带 ReqT；
    /// ④ 关掉开关 → 一笔都不带，但请求照发（不是"没打出去"造成的假绿）。
    /// 顺序在 RequestHooks.Compose 里，绑定位置在各 facade 的 Start() 里，两处都得线上验证。
    /// </summary>
    [Fact]
    public void RequestHooksReachTheWire()
    {
        // ---- ① 只有 ACL：签名字段在，ReqT 不在 ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultMQProducer producer = StartedWithHook(cluster, "GID_HookAclOnly", false);
            Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);

            WireRecord? rec = cluster.FirstSendRequest();
            Assert.NotNull(rec);
            Assert.Equal("AK_wire", rec!.Ext[SessionCredentials.AccessKeyField]);
            Assert.True(rec.Ext.ContainsKey(SessionCredentials.SignatureField));
            Assert.False(rec.Ext.ContainsKey(MixAll.ReqT), "没开 stream 不该打 ReqT");
            producer.Shutdown();
        }

        // ---- ② ACL + stream：ReqT 在签**之前**写入，所以算进签名 ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultMQProducer producer = StartedWithHook(cluster, "GID_HookStreamAcl", true);
            Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);

            WireRecord rec = cluster.FirstSendRequest()!;
            Assert.Equal("0", rec.Ext[MixAll.ReqT]); // Java 写 code 的字符串形式，不是枚举名
            // broker 侧复算：拿抓到的报文（含 ReqT、排除 Signature）重算一遍签名
            Assert.Equal(rec.Ext[SessionCredentials.SignatureField],
                AclClientRPCHook.CalcSignature(WireSk, rec.ToCommand()));
            producer.Shutdown();
        }

        // ---- ③ lite 消费者默认开 stream：路由 + 心跳都带标 ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultLitePullConsumer consumer = StartedLite(cluster, "GID_HookLite", stream: null);
            Assert.True(cluster.CountRequestsWith(RequestCode.GetRouteinfoByTopic,
                MixAll.ReqT, "0") > 0, "路由请求要带 ReqT");
            Assert.Equal(cluster.CountRequests(RequestCode.GetRouteinfoByTopic),
                cluster.CountRequestsWith(RequestCode.GetRouteinfoByTopic, MixAll.ReqT, "0"));
            Assert.True(cluster.CountRequestsWith(RequestCode.HeartBeat, MixAll.ReqT, "0") > 0,
                "心跳要带 ReqT");
            consumer.Shutdown();
        }

        // ---- ④ 显式关掉：一笔都不带，但请求确实发出去了 ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultLitePullConsumer consumer = StartedLite(cluster, "GID_HookLiteOff", stream: false);
            Assert.True(cluster.CountRequests(RequestCode.GetRouteinfoByTopic) > 0);
            Assert.False(cluster.AnyRequestHas(RequestCode.GetRouteinfoByTopic, MixAll.ReqT));
            Assert.False(cluster.AnyRequestHas(RequestCode.HeartBeat, MixAll.ReqT));
            consumer.Shutdown();
        }
    }

    /// <summary>
    /// 发送请求码的三级判据（Java MQClientAPIImpl#sendMessage:550-563）：先判 isReply
    /// ⇒ 325，再判「这条消息是不是批量」⇒ SendBatchMessage(320)，否则 310。
    ///
    /// ⚠ 请求码与 V2 头的单字母键 <c>m</c>（batch）是两件事：broker 按 <c>m</c> 选
    /// sendBatchMessage 还是单条写入（SendMessageProcessor:117 读 requestHeader.isBatch()），
    /// 码只影响服务端按码归类（proxy AbstractRemotingActivity:69 与 auth
    /// DefaultAuthorizationContextBuilder:230-240 都把 310/320 列在同一个 case 里）。
    /// 所以两个都取证：只对码不对 <c>m</c>，批量 body 会被按单条解析。
    /// </summary>
    [Fact]
    public void SendRequestCodeFollowsJava()
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

        // reply 判在 batch 之前：带 MSG_TYPE=reply 的批量仍然走 325。
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

    /// <summary>
    /// Java <c>sendKernelImpl</c> 在同一个发送头上还写了三个值（本端口此前全部漏掉）：
    /// <c>:996 setDefaultTopic(producer.getCreateTopicKey())</c> → V2 键 <c>c</c>、
    /// <c>:997 setDefaultTopicQueueNums(...)</c> → V2 键 <c>d</c>、
    /// <c>:1007 setBrokerName(brokerName)</c> → V2 键 <c>n</c>
    /// （SendMessageRequestHeaderV2.java:69，<c>@CFNullable</c> 所以空值整条不上线）。
    ///
    /// <c>c</c>/<c>d</c> 是功能问题：broker 侧自动建 topic 时按这两个值决定队列数
    /// （<c>AbstractSendMessageProcessor.createTopicInSendMessageMethod</c>），写死
    /// TBW102/4 等于把 <c>CreateTopicKey</c> / <c>DefaultTopicQueueNums</c> 变成假 setter。
    /// <c>n</c> 是线上报文对等：经典 broker 按连接地址寻址、不读它，proxy 与审计/轨迹侧读。
    ///
    /// 三种入口（同步 / 批量 320 / 单向）都要带同一份值，且 <c>d=0</c> 要原样上线 ——
    /// 0 是「调用方明说的 0」，不是「没配」。异步入口的那一份见
    /// ProducerAsyncTests.SendAsyncHeaderCarriesBrokerNameAndTopicKeys。
    /// </summary>
    [Fact]
    public void SendHeaderCarriesBrokerNameAndTopicKeys()
    {
        using var cluster = MockCluster.Start(2);
        DefaultMQProducer producer = Started(cluster, "GID_SendHeaderFields");

        // ---- ① 不配置时就是 Java 的那两个常量；n 是这一笔选中的那台 broker
        cluster.ClearRequests();
        Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);
        WireRecord first = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendMessageV2, first.Code);
        Assert.Equal("broker-0", first.Ext["n"]);
        Assert.Equal(MixAll.DefaultTopic, first.Ext["c"]);
        Assert.Equal("4", first.Ext["d"]);

        // ---- ② c/d 跟着 producer 配置走；轮询换到 broker-1 时 n 也跟着换
        cluster.ClearRequests();
        producer.CreateTopicKey = "CreatedTopicKey";
        producer.DefaultTopicQueueNums = 9;
        Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);
        WireRecord second = cluster.FirstSendRequest()!;
        Assert.Equal("broker-1", second.Ext["n"]); // n 是这一笔选中的 broker，不是路由里的第一台
        Assert.Equal("CreatedTopicKey", second.Ext["c"]);
        Assert.Equal("9", second.Ext["d"]);

        // ---- ③ 批量走同一个建头函数（码 320 与 m=true 由上一节负责）
        cluster.ClearRequests();
        Assert.Equal(SendStatus.SendOk,
            producer.SendBatch(new List<Message> { Msg(), Msg() }).SendStatus);
        WireRecord batch = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendBatchMessage, batch.Code);
        Assert.Equal("CreatedTopicKey", batch.Ext["c"]);
        Assert.Equal("9", batch.Ext["d"]);
        Assert.True(batch.Ext.ContainsKey("n"), "批量也要带 brokerName");

        // ---- ④ 单向：d=0 必须原样上线，不能被默认值 4 顶掉
        cluster.ClearRequests();
        producer.DefaultTopicQueueNums = 0;
        producer.SendOneway(Msg());
        Assert.True(WaitUntil(() => cluster.FirstSendRequest() != null, 3000), "单向请求要被抓到");
        WireRecord oneway = cluster.FirstSendRequest()!;
        // "0" 是调用方明说的 0，不能被默认值 4 顶掉
        Assert.Equal("0", oneway.Ext["d"]);
        Assert.Equal("CreatedTopicKey", oneway.Ext["c"]);
        Assert.True(oneway.Ext.ContainsKey("n"), "单向也要带 brokerName");

        producer.Shutdown();
    }

    /// <summary>
    /// 队列没有 broker 名（手工指定的 MessageQueue）时，<c>n</c> 整条不上线，而不是写
    /// 一个空串 —— Java 那个字段是 <c>@CFNullable</c>，<c>writeIfNotNull</c> 会跳过 null。
    /// 纯离线：只建请求、不发出去（Encode 前要把头展开成 extFields，同 broker 侧口径）。
    /// </summary>
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
        Assert.False(without.ExtFields.ContainsKey("n"), "空 brokerName 不该上线");
        // c/d 仍然按 Java 默认值上线
        Assert.Equal(MixAll.DefaultTopic, without.ExtFields["c"]);
        Assert.Equal("4", without.ExtFields["d"]);
    }

    /// <summary>轮询等待条件成立（单向发送没有应答可等）。</summary>
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

    /// <summary>
    /// 带 ACL 钩子的生产者。钩子必须排在 Start() **之前**：Java 的 rpcHook 随
    /// <c>MQClientAPIImpl</c> 构造传入，本端口各 facade 也在 <c>Start()</c> 里把它绑到
    /// 传输层 —— 启动后再 SetRpcHook 已经来不及，报文会裸着出去（这正是 ① ② 要抓的东西）。
    /// </summary>
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

    /// <summary>
    /// 起一个 lite 消费者：stream 传 null 表示**不动默认值**（Java 的
    /// DefaultLitePullConsumer 在构造函数里就置 true，③ 段要验的正是这个默认）。
    /// </summary>
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
