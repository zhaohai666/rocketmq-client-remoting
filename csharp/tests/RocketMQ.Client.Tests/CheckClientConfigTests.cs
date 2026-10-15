// Offline wire unit tests for CHECK_CLIENT_CONFIG(46): the shape of the request and the
// response is pinned down item by item.
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;
// PropertyMap is a global using alias on the src side (SortedDictionary<string,string>), so the test project has to declare it explicitly.
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class CheckClientConfigTests
{
    private const string Group = "GID_CheckCfg";
    private const string Topic = "Sql92Topic";
    private const string ClientId = "127.0.0.1@1234#5678";
    private const int MaxFrame = 20 * 1024 * 1024;

    private static SubscriptionData Sql92Sub(string expression = "a > 10") =>
        new(Topic, expression) { ExpressionType = RocketMQ.Common.ExpressionType.Sql92 };

    private static SubscriptionData TagSub(string type) =>
        new(Topic, "tagA || tagB") { ExpressionType = type };

    // ---------------------------------------------------------------- mock broker

    /// <summary>The evidence of one 46 request: the extFields and the raw body.</summary>
    private sealed class CheckRecord
    {
        public PropertyMap Ext { get; init; } = new();
        public byte[] Body { get; init; } = Array.Empty<byte>();
    }

    /// <summary>
    /// An in-process fake endpoint: it plays the namesrv (returning routes) as well as the
    /// broker (replying with the scripted response code for 46). It only recognises those
    /// two requests and answers SUCCESS to everything else (heartbeats and the like must
    /// not bother the test).
    /// </summary>
    private sealed class MockBroker : IDisposable
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, byte[]> _routes = new();
        private readonly Queue<(int Code, string Remark)> _checkSteps = new();
        private readonly List<CheckRecord> _checks = new();
        private int _checkCount;
        private (int Code, string Remark) _checkTail = (ResponseCode.Success, "");

        public string Addr { get; }

        private readonly Socket _listener;

        private MockBroker(Socket listener)
        {
            _listener = listener;
            Addr = ((IPEndPoint)listener.LocalEndPoint!).ToString();
            new Thread(AcceptLoop) { IsBackground = true }.Start();
        }

        public static MockBroker Start()
        {
            var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            server.Listen(16);
            return new MockBroker(server);
        }

        public void AddRoute(string topic, TopicRouteData route)
        {
            lock (_gate)
            {
                _routes[topic] = route.Encode();
            }
        }

        /// <summary>Scripted response codes for the first N requests of 46; once
        /// exhausted it keeps using tail.</summary>
        public void ScriptChecks((int Code, string Remark)[] steps,
            (int Code, string Remark)? tail = null)
        {
            lock (_gate)
            {
                _checkSteps.Clear();
                foreach ((int, string) s in steps)
                {
                    _checkSteps.Enqueue(s);
                }

                _checkTail = tail ?? (ResponseCode.Success, "");
                _checkCount = 0;
                _checks.Clear();
            }
        }

        public int CheckCount
        {
            get
            {
                lock (_gate)
                {
                    return _checkCount;
                }
            }
        }

        public CheckRecord RecordAt(int i)
        {
            lock (_gate)
            {
                return _checks[i];
            }
        }

        public JsonValue BodyJson(int i)
        {
            CheckRecord r = RecordAt(i);
            Assert.True(RemotingSerializable.Decode(r.Body, out JsonValue v), "the body of 46 is JSON");
            return v;
        }

        public void Dispose()
        {
            try
            {
                _listener.Dispose();
            }
            catch (Exception)
            {
                // Closing the listening port does not need any exception handling
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
                    continue;  // a oneway request has no response to write
                }

                byte[] wire = resp.Encode();
                stream.Write(wire, 0, wire.Length);
            }
        }

        private RemotingCommand? Respond(RemotingCommand req)
        {
            if (req.Code == RequestCode.GetRouteinfoByTopic)
            {
                string topic = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
                byte[] body;
                lock (_gate)
                {
                    if (!_routes.TryGetValue(topic, out byte[]? found))
                    {
                        return Echo(req, ResponseCode.TopicNotExist, "mock: no route");
                    }

                    body = found;
                }

                RemotingCommand routeResp = Echo(req, ResponseCode.Success, null);
                routeResp.Body = body;
                routeResp.HasBody = true;
                return routeResp;
            }

            if (req.Code == RequestCode.CheckClientConfig)
            {
                int code;
                string remark;
                lock (_gate)
                {
                    (code, remark) = _checkSteps.Count > 0 ? _checkSteps.Dequeue() : _checkTail;
                    _checks.Add(new CheckRecord
                    {
                        Ext = new PropertyMap(req.ExtFields),
                        Body = req.Body,
                    });
                    _checkCount++;
                }

                return Echo(req, code, remark);
            }

            // Non-46 requests such as heartbeats are always answered successfully, do not let the background thread get stuck on an error
            return req.IsOnewayRpc() ? null : Echo(req, ResponseCode.Success, null);
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

    /// <summary>An already-closed loopback port that is guaranteed to refuse connections
    /// (more reliable than a fixed port).</summary>
    private static string DeadAddr()
    {
        var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        s.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        s.Listen(1);
        string addr = ((IPEndPoint)s.LocalEndPoint!).ToString();
        s.Dispose();
        return addr;
    }

    /// <summary>Starts an instance and stuffs the Topic route into the cache (the broker
    /// points at the mock itself).</summary>
    private static MQClientInstance Seeded(MockBroker mock, string? brokerAddr = null) =>
        SeededWithRoute(mock, route => route.BrokerDatas.Add(new BrokerData("DefaultCluster",
            "broker-a", new SortedDictionary<long, string>
            {
                { MixAll.MasterId, brokerAddr ?? mock.Addr },
            })));

    /// <summary>The version with a custom route shape: the caller stuffs brokers into the
    /// route.</summary>
    private static MQClientInstance SeededWithRoute(MockBroker mock, Action<TopicRouteData> fill)
    {
        var instance = new MQClientInstance(ClientId, new List<string> { mock.Addr });
        var route = new TopicRouteData();
        fill(route);
        mock.AddRoute(Topic, route);
        Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic), "the route must really enter the cache");
        return instance;
    }

    /// <summary>Runs a piece of logic and recycles the instance (a timer thread that is
    /// not shut down would bleed into the next case).</summary>
    private static void Using(MockBroker mock, string? brokerAddr, Action<MQClientInstance> body)
    {
        MQClientInstance instance = Seeded(mock, brokerAddr);
        try
        {
            body(instance);
        }
        finally
        {
            instance.Shutdown();
        }
    }

    private static void Using(MockBroker mock, Action<MQClientInstance> body) =>
        Using(mock, null, body);

    // ---------------------------------------------------------------- Cases

    [Fact]
    public void TagOnlySubscriptionsSendNothing()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(Array.Empty<(int, string)>());
        Using(mock, instance => instance.CheckSubscriptionsInBroker(Group, new List<SubscriptionData>
        {
            TagSub(RocketMQ.Common.ExpressionType.TAG),
            TagSub(""),
        }));
        Assert.Equal(0, mock.CheckCount);
    }

    /// <summary>The shape of the SQL92 request: no extFields, a three-part body and a
    /// SubscriptionData with only 7 keys.</summary>
    [Fact]
    public void Sql92RequestShapeMatchesLegacy()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(Array.Empty<(int, string)>());
        Using(mock, instance =>
            instance.CheckSubscriptionsInBroker(Group, new List<SubscriptionData> { Sql92Sub() }));
        Assert.Equal(1, mock.CheckCount);

        CheckRecord record = mock.RecordAt(0);
        // A null request header => no extFields on the wire
        Assert.Empty(record.Ext);

        JsonValue body = mock.BodyJson(0);
        Assert.Equal(ClientId, body.Get("clientId").StringValue());
        Assert.Equal(Group, body.Get("group").StringValue());
        JsonValue? sd = body.Find("subscriptionData");
        Assert.NotNull(sd);
        Assert.Equal(Topic, sd!.Get("topic").StringValue());
        Assert.Equal("a > 10", sd.Get("subString").StringValue());
        Assert.Equal(RocketMQ.Common.ExpressionType.Sql92, sd.Get("expressionType").StringValue());
        string[] keys = sd.ObjectItems().Select(kv => kv.Key).OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(new[]
        {
            "classFilterMode", "codeSet", "expressionType", "subString", "subVersion", "tagsSet",
            "topic",
        }, keys);
        Assert.Null(sd.Find("filterClassSource"));
    }

    /// <summary>SUBSCRIPTION_PARSE_FAILED(23) carries the response code as is -- that is
    /// the criterion for a startup failure.</summary>
    [Fact]
    public void BrokerRejectCodeBecomesClientException()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(new[]
        {
            (ResponseCode.SubscriptionParseFailed, "bad sql92"),
        });
        MQClientException e = Assert.Throws<MQClientException>(() => Using(mock, instance =>
            instance.CheckSubscriptionsInBroker(Group,
                new List<SubscriptionData> { Sql92Sub("a >") })));
        Assert.Equal(ResponseCode.SubscriptionParseFailed, e.ResponseCode);
        Assert.Contains("bad sql92", e.Message);
    }

    [Fact]
    public void BrokerWithoutPropertyFilterReportsSystemError()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(new[]
        {
            (ResponseCode.SystemError, "property filter not enable"),
        });
        MQClientException e = Assert.Throws<MQClientException>(() => Using(mock, instance =>
            instance.CheckSubscriptionsInBroker(Group, new List<SubscriptionData> { Sql92Sub() })));
        Assert.Equal(ResponseCode.SystemError, e.ResponseCode);
    }

    [Fact]
    public void NoRouteSkipsTheSubscription()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(Array.Empty<(int, string)>());
        // Deliberately no route seeded: cache only, skip it when it cannot be obtained
        var instance = new MQClientInstance(ClientId, new List<string> { mock.Addr });
        try
        {
            instance.CheckSubscriptionsInBroker(Group, new List<SubscriptionData> { Sql92Sub() });
            Assert.Equal(0, mock.CheckCount);
            // cache only: a route that did not enter the table must not be pulled in as a side effect
            Assert.Null(instance.FindBrokerAddrByTopic(Topic));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    [Fact]
    public void TransportErrorIsWrappedWithLegacyMessage()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(Array.Empty<(int, string)>());
        string dead = DeadAddr();
        MQClientException e = Assert.Throws<MQClientException>(() => Using(mock, dead, instance =>
            instance.CheckSubscriptionsInBroker(Group, new List<SubscriptionData> { Sql92Sub() })));
        Assert.Contains("SQL92", e.Message);
        Assert.Contains("server has not been upgraded to support", e.Message);
        Assert.NotNull(e.InnerException);
        Assert.Equal(0, mock.CheckCount);
    }

    /// <summary>An empty subscription set sends nothing at all.</summary>
    [Fact]
    public void EmptySubscriptionsSendNothing()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(Array.Empty<(int, string)>());
        Using(mock, instance =>
            instance.CheckSubscriptionsInBroker(Group, new List<SubscriptionData>()));
        Assert.Equal(0, mock.CheckCount);
    }

    /// <summary>Master first: a master plus a slave that refuses connections, all five
    /// requests land on the master.</summary>
    [Fact]
    public void MasterBrokerIsPreferred()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(Array.Empty<(int, string)>());
        string dead = DeadAddr();
        MQClientInstance instance = SeededWithRoute(mock, route => route.BrokerDatas.Add(
            new BrokerData("DefaultCluster", "broker-a", new SortedDictionary<long, string>
            {
                { MixAll.MasterId, mock.Addr },
                { 1, dead },
            })));
        try
        {
            for (int i = 0; i < 5; ++i)
            {
                instance.CheckSubscriptionsInBroker(Group,
                    new List<SubscriptionData> { Sql92Sub() });
            }
        }
        finally
        {
            instance.Shutdown();
        }

        Assert.Equal(5, mock.CheckCount);
    }

    /// <summary>A CheckClientRequestBody round trip; the namespace only takes part in
    /// serialisation when it is set.</summary>
    [Fact]
    public void BodyRoundTripAndNamespace()
    {
        var input = new CheckClientRequestBody
        {
            ClientId = "cid",
            Group = "gid",
            SubscriptionData = Sql92Sub("color = 'red'"),
        };
        Assert.Null(input.ToJson().Find("namespace"));
        Assert.True(CheckClientRequestBody.Decode(input.Encode(), out CheckClientRequestBody back));
        Assert.Equal("cid", back.ClientId);
        Assert.Equal("gid", back.Group);
        Assert.Equal("color = 'red'", back.SubscriptionData?.SubString);
        Assert.Equal(RocketMQ.Common.ExpressionType.Sql92, back.SubscriptionData?.ExpressionType);

        input.Namespace = "nsA";
        Assert.True(CheckClientRequestBody.Decode(input.Encode(), out CheckClientRequestBody back2));
        Assert.Equal("nsA", back2.Namespace);
    }

    [Fact]
    public void MqClientApiTimeoutMatchesLegacy()
    {
        Assert.Equal(3000, MQClientInstance.MqClientApiTimeoutMillis);
    }
}
