// No cutting corners at the message level: in-process fake namesrv/broker answer routes,
// "no committed offset" and pull responses over **real sockets**, and "was this RPC
// actually sent" is evidenced off the socket throughout. The negative-control leg (the
// LAST_OFFSET branch must send GET_MAX_OFFSET) proves this request log is not mute.
//
// This pins the invariant: the lite initial offset starts at 0 and sends no
// GET_MIN_OFFSET; the push leg adds one more case here (subscription mode goes through
// Consumer.cs's ResolveInitialOffset).
using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;
// PropertyMap is a global using alias on the src side (SortedDictionary<string,string>), so the test project has to declare it explicitly.
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class InitialOffsetTests
{
    private const string Topic = "InitialOffsetTopic";
    private const string Broker = "broker-a";
    private const int MaxFrame = 20 * 1024 * 1024;

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    // ---------------------------------------------------------------- Fake endpoints

    /// <summary>An arriving request: the code plus an extFields snapshot.</summary>
    private sealed record WireReq(int Code, PropertyMap Ext);

    /// <summary>
    /// A fake namesrv plus a fake broker (the same shell): answers routes, answers "this
    /// offset was never committed" and answers pull requests, and records every request
    /// in arrival order. GET_MIN_OFFSET / GET_MAX_OFFSET are **answered as usual** --
    /// only after they are answered can one talk about "the client chose not to send it",
    /// otherwise what is being measured is "it sent it and nobody cared".
    /// </summary>
    private sealed class OffsetEndpoint : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<WireReq> _reqs = new();
        private readonly Socket _listener;
        /// <summary>The clientID recognised from the heartbeat (34) body. Rebalance needs
        /// it to place itself in cidAll, otherwise this consumer gets not a single queue,
        /// never pulls, and the pull leg would be testing nothing.</summary>
        private string _clientId = string.Empty;

        public string Addr { get; }

        private OffsetEndpoint(Socket listener)
        {
            _listener = listener;
            Addr = ((IPEndPoint)listener.LocalEndPoint!).ToString();
            new Thread(AcceptLoop) { IsBackground = true }.Start();
        }

        public static OffsetEndpoint Start()
        {
            var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            server.Listen(16);
            return new OffsetEndpoint(server);
        }

        public List<WireReq> Requests(int code)
        {
            lock (_gate)
            {
                return _reqs.Where(r => r.Code == code).ToList();
            }
        }

        /// <summary>All request codes in arrival order -- carried in the failure message so
        /// there is no need to spin up another debug session for a fake endpoint.</summary>
        public List<int> Codes()
        {
            lock (_gate)
            {
                return _reqs.Select(r => r.Code).ToList();
            }
        }

        public void Dispose()
        {
            try
            {
                _listener.Dispose();
            }
            catch (Exception)
            {
                // Closing the listening port needs no exception handling
            }
        }

        private void AcceptLoop()
        {
            while (true)
            {
                Socket client;
                try
                {
                    client = _listener.Accept();
                }
                catch (Exception)
                {
                    return;  // after Dispose, Accept necessarily throws, this is a normal shutdown
                }

                new Thread(() =>
                {
                    using (client)
                    {
                        try
                        {
                            ServeFrames(client);
                        }
                        catch (Exception)
                        {
                            // A mid-stream disconnect / decode failure: just drop this connection
                        }
                    }
                })
                { IsBackground = true }.Start();
            }
        }

        private void ServeFrames(Socket client)
        {
            var stream = new NetworkStream(client, ownsSocket: false);
            var lenBuf = new byte[4];
            while (true)
            {
                if (!ReadFully(stream, lenBuf, 0, 4))
                {
                    return;
                }

                int total = BinaryPrimitives.ReadInt32BigEndian(lenBuf);
                if (total <= 0 || total > MaxFrame)
                {
                    return;
                }

                var frame = new byte[4 + total];
                Buffer.BlockCopy(lenBuf, 0, frame, 0, 4);
                if (!ReadFully(stream, frame, 4, total))
                {
                    return;
                }

                if (!RemotingCommand.TryDecode(frame, out RemotingCommand req, out _))
                {
                    return;
                }

                RemotingCommand? resp = Respond(req);
                if (resp is null)
                {
                    continue;  // oneway has no response to write
                }

                byte[] wire = resp.Encode();
                stream.Write(wire, 0, wire.Length);
            }
        }

        private RemotingCommand? Respond(RemotingCommand req)
        {
            lock (_gate)
            {
                _reqs.Add(new WireReq(req.Code, new PropertyMap(req.ExtFields)));
            }

            if (req.Code == RequestCode.HeartBeat && req.Body.Length > 0)
            {
                // The clientID in the heartbeat body is this consumer's identity: rebalance uses it to line up with cidAll
                if (HeartbeatData.Decode(req.Body, out HeartbeatData hb) && hb.ClientId.Length > 0)
                {
                    lock (_gate)
                    {
                        _clientId = hb.ClientId;
                    }
                }
            }

            if (req.Code == RequestCode.GetRouteinfoByTopic)
            {
                string topic = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
                // The auto-subscribed %RETRY% topic needs a route too: without it the
                // retry queue is never assigned and the retry-start rules go untested.
                if (topic != Topic && !topic.StartsWith(MixAll.RetryGroupTopicPrefix, StringComparison.Ordinal))
                {
                    return Echo(req, ResponseCode.TopicNotExist, "mock: no route");
                }

                RemotingCommand routeResp = Echo(req, ResponseCode.Success, null);
                routeResp.Body = BuildRoute(Addr);
                routeResp.HasBody = true;
                return routeResp;
            }

            if (req.Code == RequestCode.GetConsumerListByGroup && !req.IsOnewayRpc())
            {
                // The first step of rebalance: this consumer must be in the list, otherwise it gets no queues and never pulls
                string clientId;
                lock (_gate)
                {
                    clientId = _clientId;
                }

                if (clientId.Length == 0)
                {
                    return Echo(req, ResponseCode.Success, null);
                }

                var body = new GetConsumerListByGroupResponseBody();
                body.ConsumerIdList.Add(clientId);
                RemotingCommand listResp = Echo(req, ResponseCode.Success, null);
                listResp.Body = body.Encode();
                listResp.HasBody = true;
                return listResp;
            }

            if (req.Code == RequestCode.QueryConsumerOffset && !req.IsOnewayRpc())
            {
                // A new group: no committed offset on the broker -- exactly the situation the FIRST_OFFSET branch handles
                return Echo(req, ResponseCode.QueryNotFound, "mock: no committed offset");
            }

            if (req.Code is RequestCode.GetMaxOffset or RequestCode.GetMinOffset
                && !req.IsOnewayRpc())
            {
                RemotingCommand offsetResp = Echo(req, ResponseCode.Success, null);
                offsetResp.AddExtField("offset", "5");
                return offsetResp;
            }

            // A lite consumer pulls with 361 and a classic pull with 11 (#107); the
            // assertion only picks 11, so both codes must be answered here, otherwise the
            // lite cursor cannot advance at all.
            if (req.Code is RequestCode.PullMessage or RequestCode.LitePullMessage
                && !req.IsOnewayRpc())
            {
                // Immediately answers "no new messages this round": the assertion only looks at queueOffset in the request header, not at the response semantics.
                RemotingCommand pullResp = Echo(req, ResponseCode.PullNotFound, null);
                pullResp.AddExtField("nextBeginOffset", "0");
                pullResp.AddExtField("minOffset", "0");
                pullResp.AddExtField("maxOffset", "0");
                return pullResp;
            }

            // Heartbeats / offset commits and the like always succeed, do not let the background thread get stuck on an error
            return req.IsOnewayRpc() ? null : Echo(req, ResponseCode.Success, null);
        }

        private static byte[] BuildRoute(string brokerAddr)
        {
            var route = new TopicRouteData();
            route.QueueDatas.Add(new QueueData(Broker, 1, 1, PermName.PermRead | PermName.PermWrite, 0));
            route.BrokerDatas.Add(new BrokerData("DefaultCluster", Broker,
                new SortedDictionary<long, string> { { MixAll.MasterId, brokerAddr } }));
            return route.Encode();
        }

        private static RemotingCommand Echo(RemotingCommand req, int code, string? remark)
        {
            var resp = RemotingCommand.CreateResponseCommand(code, remark);
            // The client pairs by opaque, a cross-wired response is dropped as noise
            resp.Opaque = req.Opaque;
            resp.SerializeTypeCurrentRpc = req.SerializeTypeCurrentRpc;
            return resp;
        }

        private static bool ReadFully(Stream stream, byte[] buffer, int offset, int count)
        {
            int read = 0;
            while (read < count)
            {
                int n;
                try
                {
                    n = stream.Read(buffer, offset + read, count - read);
                }
                catch (Exception)
                {
                    return false;
                }

                if (n <= 0)
                {
                    return false;
                }

                read += n;
            }

            return true;
        }
    }

    private static bool WaitFor(Func<bool> cond, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    private sealed class NullListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
                                                       ConsumeConcurrentlyContext context)
            => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    // ---------------------------------------------------------------- Cases

    /// <summary>
    /// Lite pull: the FIRST_OFFSET start is the literal 0, the very first pull goes on
    /// the wire already carrying queueOffset=0, and not a single GET_MIN_OFFSET is sent
    /// the whole way.
    ///
    /// The negative-control leg is "LAST_OFFSET must send GET_MAX_OFFSET": without it,
    /// deleting that whole branch (making the start always 0 and querying no offset at
    /// all) would still turn the main assertion green.
    /// </summary>
    [Fact]
    public void LitePullFirstOffsetStartsAtZeroWithoutAMinOffsetRpc()
    {
        using var ep = OffsetEndpoint.Start();

        var first = new DefaultLitePullConsumer("LitePG_InitFirst");
        first.SetNamesrvAddr(ep.Addr);
        first.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        first.Assign(new[] { Queue0() });
        first.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.LitePullMessage).Count > 0, TimeSpan.FromSeconds(5)),
                "the FIRST_OFFSET lite-pull must really send the first pull (LITE_PULL_MESSAGE=361)");
            List<WireReq> pulls = ep.Requests(RequestCode.LitePullMessage);
            Assert.Equal("0", pulls[0].Ext["queueOffset"]);
            Assert.Empty(ep.Requests(RequestCode.GetMinOffset));
        }
        finally
        {
            first.Shutdown();
        }

        // Negative control: the default LAST_OFFSET goes through maxOffset -- proving this request log is not mute
        var last = new DefaultLitePullConsumer("LitePG_InitLast");
        last.SetNamesrvAddr(ep.Addr);
        last.Assign(new[] { Queue0() });
        last.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.GetMaxOffset).Count > 0, TimeSpan.FromSeconds(5)),
                "the LAST_OFFSET lite-pull must send GET_MAX_OFFSET");
        }
        finally
        {
            last.Shutdown();
        }

        Assert.Empty(ep.Requests(RequestCode.GetMinOffset));
    }

    /// <summary>
    /// The same scenario for a push consumer (subscription mode goes through
    /// Consumer.cs's ResolveInitialOffset): when a new group has no committed offset,
    /// the start 0 goes on the wire directly and no GET_MIN_OFFSET is sent.
    /// </summary>
    [Fact]
    public void PushConsumerFirstOffsetStartsAtZeroWithoutAMinOffsetRpc()
    {
        using var ep = OffsetEndpoint.Start();

        var first = new DefaultMQPushConsumer("CG_InitFirst")
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        first.SetNamesrvAddr(ep.Addr);
        first.Subscribe(Topic, "*");
        first.SetMessageListener(new NullListener());
        first.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.PullMessage).Count > 0, TimeSpan.FromSeconds(10)),
                "the FIRST_OFFSET push consumer must really send the first pull; request codes that arrived:"
                + string.Join(",", ep.Codes()));
            List<WireReq> pulls = ep.Requests(RequestCode.PullMessage);
            Assert.Equal("0", pulls[0].Ext["queueOffset"]);
            Assert.Empty(ep.Requests(RequestCode.GetMinOffset));
        }
        finally
        {
            first.Shutdown();
        }

        // Negative control: the LAST_OFFSET branch should send maxOffset
        var last = new DefaultMQPushConsumer("CG_InitLast");
        last.SetNamesrvAddr(ep.Addr);
        last.Subscribe(Topic, "*");
        last.SetMessageListener(new NullListener());
        last.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.GetMaxOffset).Count > 0, TimeSpan.FromSeconds(10)),
                "the LAST_OFFSET push consumer must send GET_MAX_OFFSET");
        }
        finally
        {
            last.Shutdown();
        }

        Assert.Empty(ep.Requests(RequestCode.GetMinOffset));
    }

    /// <summary>
    /// The start offset never depends on the subscription type: a SQL92 selector with
    /// CONSUME_FROM_FIRST_OFFSET must still start at the literal 0. An earlier draft
    /// short-circuited every SQL92 subscription to GET_MAX_OFFSET, which silently
    /// dropped the whole backlog produced before the consumer came up -- so the pin is
    /// "offset 0 on the wire" AND "no max-offset RPC at all".
    /// </summary>
    [Fact]
    public void Sql92SubscriptionStillStartsAtThePolicyOffset()
    {
        using var ep = OffsetEndpoint.Start();

        var sql = new DefaultMQPushConsumer("CG_InitSql")
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        sql.SetNamesrvAddr(ep.Addr);
        sql.Subscribe(Topic, MessageSelector.BySql("a > 0"));
        sql.SetMessageListener(new NullListener());
        sql.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.PullMessage).Count > 0, TimeSpan.FromSeconds(10)),
                "a SQL92 FIRST_OFFSET consumer must really pull; request codes that arrived:"
                + string.Join(",", ep.Codes()));
            Assert.Equal("0", ep.Requests(RequestCode.PullMessage)[0].Ext["queueOffset"]);
            Assert.Empty(ep.Requests(RequestCode.GetMaxOffset));
            Assert.Empty(ep.Requests(RequestCode.GetMinOffset));
        }
        finally
        {
            sql.Shutdown();
        }
    }

    /// <summary>CONSUME_FROM_TIMESTAMP searches at the configured time, not at an
    /// offset computed from boot time: the start instant is what an operator sets, so a
    /// 2020 timestamp must reach the broker as 2020.</summary>
    [Fact]
    public void TimestampStartUsesTheConfiguredInstant()
    {
        const string Configured = "20200101000000";
        Assert.True(DateTimeOffset.TryParseExact(Configured, "yyyyMMddHHmmss",
            CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTimeOffset parsed));
        long expected = parsed.ToUnixTimeMilliseconds();

        using var ep = OffsetEndpoint.Start();

        var consumer = new DefaultMQPushConsumer("CG_InitTs")
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromTimestamp,
            ConsumeTimestamp = Configured,
        };
        consumer.SetNamesrvAddr(ep.Addr);
        consumer.Subscribe(Topic, "*");
        consumer.SetMessageListener(new NullListener());
        consumer.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.SearchOffsetByTimestamp).Count > 0,
                    TimeSpan.FromSeconds(10)),
                "the TIMESTAMP policy must send SEARCH_OFFSET_BY_TIMESTAMP; request codes that arrived:"
                + string.Join(",", ep.Codes()));
            WireReq search = ep.Requests(RequestCode.SearchOffsetByTimestamp)[0];
            Assert.Equal(expected.ToString(CultureInfo.InvariantCulture), search.Ext["timestamp"]);
            Assert.Equal(Topic, search.Ext["topic"]);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    /// <summary>A malformed start time is rejected where it is set, before any thread or
    /// registration exists -- not discovered later as a pull failure.</summary>
    [Fact]
    public void MalformedConsumeTimestampFailsAtStart()
    {
        using var ep = OffsetEndpoint.Start();

        var consumer = new DefaultMQPushConsumer("CG_InitBadTs")
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromTimestamp,
        };
        consumer.ConsumeTimestamp = "2020-01-01 00:00:00";
        consumer.SetNamesrvAddr(ep.Addr);
        consumer.Subscribe(Topic, "*");
        consumer.SetMessageListener(new NullListener());

        MQClientException e = Assert.Throws<MQClientException>(consumer.Start);
        Assert.Contains("consumeTimestamp is invalid", e.Message);
        Assert.Contains("2020-01-01 00:00:00", e.Message);
    }

    /// <summary>
    /// A %RETRY% queue with nothing stored starts at 0 under LAST_OFFSET (every queued
    /// retry has to be delivered) while the business queue of the same consumer still
    /// goes to the queue tail -- and the retry start needs no max-offset RPC of its own.
    /// The business-topic leg is the control: it proves the same code path does send
    /// GET_MAX_OFFSET when the policy asks for the tail.
    /// </summary>
    [Fact]
    public void RetryTopicStartsAtZeroWhereTheBusinessTopicStartsAtTheTail()
    {
        using var ep = OffsetEndpoint.Start();
        const string Group = "CG_InitRetry";

        var consumer = new DefaultMQPushConsumer(Group);
        consumer.SetNamesrvAddr(ep.Addr);
        consumer.Subscribe(Topic, "*");
        consumer.SetMessageListener(new NullListener());
        consumer.Start();
        try
        {
            string retryTopic = MixAll.GetRetryTopic(Group);
            Assert.True(WaitFor(() => ep.Requests(RequestCode.PullMessage)
                    .Exists(r => r.Ext.TryGetValue("topic", out string? t) && t == retryTopic),
                TimeSpan.FromSeconds(15)),
                "the auto-subscribed retry topic must be pulled; request codes that arrived:"
                + string.Join(",", ep.Codes()));

            List<WireReq> retryPulls = ep.Requests(RequestCode.PullMessage)
                .FindAll(r => r.Ext.TryGetValue("topic", out string? t) && t == retryTopic);
            Assert.Equal("0", retryPulls[0].Ext["queueOffset"]);

            // The control: the business topic of the same consumer does ask for the tail.
            Assert.True(WaitFor(() => ep.Requests(RequestCode.GetMaxOffset)
                    .Exists(r => r.Ext.TryGetValue("topic", out string? t) && t == Topic),
                TimeSpan.FromSeconds(10)),
                "the LAST_OFFSET business-topic start must send GET_MAX_OFFSET for that topic");
            Assert.False(ep.Requests(RequestCode.GetMaxOffset)
                .Exists(r => r.Ext.TryGetValue("topic", out string? t) && t == retryTopic),
                "a retry topic with no stored offset must not be resolved by a max-offset query");
        }
        finally
        {
            consumer.Shutdown();
        }
    }
}
