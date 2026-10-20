// Offline unit tests for selector-flavoured subscriptions on the pull path: the
// expression type must travel subscription -> heartbeat -> pull request, while a TAG
// selector keeps producing exactly the wire shape the plain string path produces today.
//
// Every assertion reads a request captured off the socket (recorded state), never a
// clock window.
using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

using Xunit;

namespace RocketMQ.Client.Tests;

public class PullSelectorTests
{
    private const string SqlTopic = "SelPullSqlTopic";
    private const string TagTopic = "SelPullTagTopic";
    private const string Group = "GID_sel_pull_wire";
    private const string Broker = "broker-sel";
    private const string Sql = "price > 10 AND region IN ('a','b')";
    private const int MaxFrame = 20 * 1024 * 1024;

    // ---------------------------------------------------------------- pure builder

    [Fact]
    public void Build_TagSelector_IsExactlyWhatTheStringPathBuilds()
    {
        SubscriptionData viaSelector = SubscriptionSelectors.Build(TagTopic,
            MessageSelector.ByTag("TagA||TagB"));
        SubscriptionData viaString = FilterAPI.BuildSubscriptionData(TagTopic, "TagA||TagB");

        Assert.Equal(ExpressionType.TAG, viaSelector.ExpressionType);
        Assert.Equal("TagA||TagB", viaSelector.SubString);
        Assert.True(viaSelector.TagsSet.SetEquals(viaString.TagsSet));
        Assert.True(viaSelector.CodeSet.SetEquals(viaString.CodeSet));
        // Positive guard on the sets themselves: an empty set would match vacuously.
        Assert.Equal(2, viaSelector.TagsSet.Count);
        Assert.False(viaSelector.CodeSet.Count == 0, "a TAG selector must fill codeSet");
        Assert.False(viaSelector.ClassFilterMode);
    }

    [Fact]
    public void Build_Sql92Selector_CarriesExpressionVerbatim()
    {
        SubscriptionData sub = SubscriptionSelectors.Build(SqlTopic, MessageSelector.BySql(Sql));
        Assert.Equal(ExpressionType.Sql92, sub.ExpressionType);
        Assert.Equal(Sql, sub.SubString);
        // Control against the TAG leg: an SQL92 expression must NOT be hashed as tags.
        Assert.Empty(sub.TagsSet);
        Assert.Empty(sub.CodeSet);
        Assert.False(sub.ClassFilterMode);
    }

    [Fact]
    public void Build_ClassFilterSelector_SetsClassFilterMode()
    {
        SubscriptionData sub = SubscriptionSelectors.Build(SqlTopic,
            new MessageSelector { Type = ExpressionType.ClassFilter, Expression = "MyFilter" });
        Assert.Equal(ExpressionType.ClassFilter, sub.ExpressionType);
        Assert.True(sub.ClassFilterMode);
        Assert.Equal("MyFilter", sub.SubString);
    }

    [Fact]
    public void Build_BlankSelector_DefaultsToAllMessagesTagType()
    {
        SubscriptionData sub = SubscriptionSelectors.Build(TagTopic, new MessageSelector
        {
            Type = string.Empty,
            Expression = string.Empty,
        });
        Assert.Equal(ExpressionType.TAG, sub.ExpressionType);
        Assert.Equal("*", sub.SubString);
    }

    // ---------------------------------------------------------------- wire: pull consumer

    [Fact]
    public void PullConsumer_SelectorCarriesExpressionTypeOntoTheWire()
    {
        using SelectorEndpoint ep = SelectorEndpoint.Start();
        var consumer = new DefaultMQPullConsumer(Group);
        consumer.SetNamesrvAddr(ep.Addr);
        consumer.SetInstanceName("sel-pull-wire");
        consumer.RegisterMessageQueueListener(SqlTopic, null);
        consumer.RegisterMessageQueueListener(TagTopic, null);
        consumer.Start();
        try
        {
            // The pre-pull heartbeat still has the legacy shape for both topics.
            Assert.Equal(1, consumer.SendHeartbeatToAllBroker());
            SubscriptionData before = FindSubscription(LastHeartbeat(ep), SqlTopic);
            Assert.Equal("*", before.SubString);
            Assert.Equal(ExpressionType.TAG, before.ExpressionType);

            var sqlMq = new MessageQueue(SqlTopic, Broker, 0);
            consumer.Pull(sqlMq, MessageSelector.BySql(Sql), 0, 32);
            PropertyMap sqlPull = LastRequest(ep, RequestCode.PullMessage);
            Assert.Equal(ExpressionType.Sql92, sqlPull["expressionType"]);
            Assert.Equal(Sql, sqlPull["subscription"]);
            Assert.True(sqlPull["subVersion"] != "0",
                "a typed expression sends its own version, unlike the TAG path");

            var tagMq = new MessageQueue(TagTopic, Broker, 0);
            consumer.Pull(tagMq, MessageSelector.ByTag("TagA||TagB"), 0, 32);
            PropertyMap tagPull = LastRequest(ep, RequestCode.PullMessage);
            Assert.Equal(ExpressionType.TAG, tagPull["expressionType"]);
            Assert.Equal("TagA||TagB", tagPull["subscription"]);
            Assert.True(tagPull["subVersion"] == "0",
                "a TAG selector keeps the zeroed subVersion the string path sends");

            // Control: the string overload sends the identical TAG triple.
            consumer.Pull(tagMq, "TagA||TagB", 0, 32);
            PropertyMap stringPull = LastRequest(ep, RequestCode.PullMessage);
            Assert.Equal(tagPull["expressionType"], stringPull["expressionType"]);
            Assert.Equal(tagPull["subscription"], stringPull["subscription"]);
            Assert.Equal(tagPull["subVersion"], stringPull["subVersion"]);

            // Subscription -> heartbeat: the SQL92 topic now registers SQL92, the TAG
            // selector must not have changed the legacy heartbeat shape.
            Assert.Equal(1, consumer.SendHeartbeatToAllBroker());
            List<SubscriptionData> subs = DecodeSubscriptions(ep);
            SubscriptionData sqlSub = subs.First(s => s.Topic == SqlTopic);
            Assert.Equal(ExpressionType.Sql92, sqlSub.ExpressionType);
            Assert.Equal(Sql, sqlSub.SubString);
            SubscriptionData tagSub = subs.First(s => s.Topic == TagTopic);
            Assert.Equal("*", tagSub.SubString);
            Assert.Equal(ExpressionType.TAG, tagSub.ExpressionType);
            Assert.Equal(0L, tagSub.SubVersion);

            // The long-poll flavour carries the type too (and the suspend bit).
            consumer.PullBlockIfNotFound(sqlMq, MessageSelector.BySql(Sql), 0, 32);
            PropertyMap blockedPull = LastRequest(ep, RequestCode.PullMessage);
            Assert.Equal(ExpressionType.Sql92, blockedPull["expressionType"]);
            int flag = int.Parse(blockedPull["sysFlag"], CultureInfo.InvariantCulture);
            Assert.True(PullSysFlag.HasSuspendFlag(flag));
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- wire: lite pull consumer

    [Fact]
    public void LitePull_SelectorSubscribeCarriesExpressionTypeIntoPullAndHeartbeat()
    {
        using SelectorEndpoint ep = SelectorEndpoint.Start();
        var consumer = new DefaultLitePullConsumer(Group + "_lite");
        consumer.SetNamesrvAddr(ep.Addr);
        consumer.SetInstanceName("sel-lite-sql");
        consumer.Subscribe(SqlTopic, MessageSelector.BySql(Sql));
        consumer.Start();
        try
        {
            Assert.True(WaitFor(() => AnyRequest(ep, RequestCode.LitePullMessage,
                "expressionType", ExpressionType.Sql92), TimeSpan.FromSeconds(10)),
                "the lite pull request carries SQL92");

            PropertyMap lite = LastRequest(ep, RequestCode.LitePullMessage);
            Assert.Equal(Sql, lite["subscription"]);
            Assert.True(lite["subVersion"] != "0", "SQL92 carries its own subVersion");

            // Heartbeat side of the same subscription table.
            List<SubscriptionData> subs = DecodeSubscriptions(ep);
            SubscriptionData sqlSub = subs.First(s => s.Topic == SqlTopic);
            Assert.Equal(ExpressionType.Sql92, sqlSub.ExpressionType);
            Assert.Equal(Sql, sqlSub.SubString);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void LitePull_StringSubscribe_StillSendsTagZeroVersion()
    {
        // Control for the test above: without a selector nothing on the lite wire changes.
        using SelectorEndpoint ep = SelectorEndpoint.Start();
        var consumer = new DefaultLitePullConsumer(Group + "_lite_tag");
        consumer.SetNamesrvAddr(ep.Addr);
        consumer.SetInstanceName("sel-lite-tag");
        consumer.Subscribe(TagTopic, "TagA");
        consumer.Start();
        try
        {
            Assert.True(WaitFor(() => AnyRequest(ep, RequestCode.LitePullMessage,
                "expressionType", ExpressionType.TAG), TimeSpan.FromSeconds(10)),
                "the plain TAG subscription still pulls with TAG");
            PropertyMap lite = LastRequest(ep, RequestCode.LitePullMessage);
            Assert.Equal("0", lite["subVersion"]);
            Assert.Equal("TagA", lite["subscription"]);

            List<SubscriptionData> subs = DecodeSubscriptions(ep);
            SubscriptionData tagSub = subs.First(s => s.Topic == TagTopic);
            Assert.Equal(ExpressionType.TAG, tagSub.ExpressionType);
            Assert.Contains("TagA", tagSub.TagsSet);
        }
        finally
        {
        consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- fake endpoint

    private sealed record WireReq(int Code, PropertyMap Ext, byte[] Body);

    /// <summary>One loopback socket that plays both the namesrv (route for every topic)
    /// and the broker (offset queries, pulls, heartbeats), recording every request.</summary>
    private sealed class SelectorEndpoint : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<WireReq> _reqs = new();
        private readonly Socket _listener;
        private string _clientId = string.Empty;

        public string Addr { get; }

        private SelectorEndpoint(Socket listener)
        {
            _listener = listener;
            Addr = ((IPEndPoint)listener.LocalEndPoint!).ToString();
            new Thread(AcceptLoop) { IsBackground = true }.Start();
        }

        public static SelectorEndpoint Start()
        {
            var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            server.Listen(16);
            return new SelectorEndpoint(server);
        }

        public List<WireReq> Snapshot()
        {
            lock (_gate)
            {
                return new List<WireReq>(_reqs);
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
                    return;
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
                    continue;
                }

                byte[] wire = resp.Encode();
                stream.Write(wire, 0, wire.Length);
            }
        }

        private RemotingCommand? Respond(RemotingCommand req)
        {
            lock (_gate)
            {
                _reqs.Add(new WireReq(req.Code, new PropertyMap(req.ExtFields), req.Body));
            }

            if (req.Code == RequestCode.HeartBeat && req.Body.Length > 0)
            {
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
                RemotingCommand routeResp = Echo(req, ResponseCode.Success, null);
                routeResp.Body = BuildRoute(Addr);
                routeResp.HasBody = true;
                return routeResp;
            }

            if (req.Code == RequestCode.GetConsumerListByGroup && !req.IsOnewayRpc())
            {
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
                return Echo(req, ResponseCode.QueryNotFound, "mock: no committed offset");
            }

            if (req.Code is RequestCode.GetMaxOffset or RequestCode.GetMinOffset && !req.IsOnewayRpc())
            {
                RemotingCommand offsetResp = Echo(req, ResponseCode.Success, null);
                offsetResp.AddExtField("offset", "5");
                return offsetResp;
            }

            if (req.Code is RequestCode.PullMessage or RequestCode.LitePullMessage && !req.IsOnewayRpc())
            {
                RemotingCommand pullResp = Echo(req, ResponseCode.PullNotFound, null);
                pullResp.AddExtField("nextBeginOffset", "0");
                pullResp.AddExtField("minOffset", "0");
                pullResp.AddExtField("maxOffset", "0");
                return pullResp;
            }

            return req.IsOnewayRpc() ? null : Echo(req, ResponseCode.Success, null);
        }

        private static byte[] BuildRoute(string brokerAddr)
        {
            var route = new TopicRouteData();
            route.QueueDatas.Add(new QueueData(Broker, 1, 1,
                PermName.PermRead | PermName.PermWrite, 0));
            route.BrokerDatas.Add(new BrokerData("DefaultCluster", Broker,
                new SortedDictionary<long, string> { { MixAll.MasterId, brokerAddr } }));
            return route.Encode();
        }

        private static RemotingCommand Echo(RemotingCommand req, int code, string? remark)
        {
            var resp = RemotingCommand.CreateResponseCommand(code, remark);
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

    // ---------------------------------------------------------------- evidence helpers

    private static PropertyMap LastRequest(SelectorEndpoint ep, int code)
    {
        WireReq req = ep.Snapshot().LastOrDefault(r => r.Code == code)
            ?? throw new Xunit.Sdk.XunitException("no request with code " + code);
        return req.Ext;
    }

    private static bool AnyRequest(SelectorEndpoint ep, int code, string key, string value) =>
        ep.Snapshot().Any(r => r.Code == code
            && r.Ext.TryGetValue(key, out string? v) && v == value);

    private static byte[] LastHeartbeat(SelectorEndpoint ep) =>
        ep.Snapshot().Last(r => r.Code == RequestCode.HeartBeat).Body;

    private static List<SubscriptionData> DecodeSubscriptions(SelectorEndpoint ep)
    {
        byte[] body = LastHeartbeat(ep);
        Assert.True(HeartbeatData.Decode(body, out HeartbeatData hb), "heartbeat body must decode");
        Assert.NotEmpty(hb.ConsumerDataSet);
        return new List<SubscriptionData>(hb.ConsumerDataSet[0].SubscriptionDataSet);
    }

    private static SubscriptionData FindSubscription(byte[] heartbeatBody, string topic)
    {
        Assert.True(HeartbeatData.Decode(heartbeatBody, out HeartbeatData hb));
        return hb.ConsumerDataSet[0].SubscriptionDataSet.First(s => s.Topic == topic);
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
}
