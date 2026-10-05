// CHECK_CLIENT_CONFIG(46) 的离线单测（对应 Java MQClientInstance#checkClientInBroker）。
//
// 为什么要有这一步：SQL92 表达式写错时 broker **不会**报错 ——
// ExpressionMessageFilter#isMatched 在 ConsumeQueue 阶段拿不到编译好的过滤数据就直接
// return true（静默放行全部消息），消费者启动照常成功。Java 因此在
// DefaultMQPushConsumerImpl.start:1014 主动发一笔 46 号请求，把「写错的表达式」变成
// 启动期错误。本端口此前只有 RequestCode 常量、没有调用方，所以这里锁死协议形状与
// 分支语义（真机行为见 examples/RocketMQ.Examples/LiveSql92.cs）。
//
// 与 python/tests/test_check_client_config.py、cpp/tests/test_check_client_config.cpp、
// rust/src/client/mq_client.rs 的同一组断言对拍。
//
// 对齐基准（Java 5.5.1，逐行读过）：
//   * MQClientAPIImpl#checkClientInBroker:3256：请求头是 **null**，body 是
//     CheckClientRequestBody 的 JSON（clientId / group / subscriptionData）；
//     响应码非 SUCCESS 时抛 MQClientException(响应码, remark)。
//   * MQClientInstance#checkClientInBroker:534：只查非 TAG 订阅
//     （ExpressionType.isTagType：null / "" / TAG 都算 TAG）；broker 地址来自
//     findBrokerAddrByTopic（**只读缓存**路由、随机一个 broker、优先 master），
//     取不到就跳过；网络类异常包装成固定文案的 MQClientException（"…server has not
//     been upgraded to support…"）。
//   * ClientConfig#mqClientApiTimeout 默认 **3000ms**，这笔请求用的就是它。
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;
// PropertyMap 是 src 侧的 global using 别名（SortedDictionary<string,string>），测试项目要显式声明。
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

    /// <summary>一笔 46 请求的取证：extFields 与原始 body。</summary>
    private sealed class CheckRecord
    {
        public PropertyMap Ext { get; init; } = new();
        public byte[] Body { get; init; } = Array.Empty<byte>();
    }

    /// <summary>
    /// 进程内假端点：同时扮演 namesrv（回路由）与 broker（按脚本回 46 的响应码）。
    /// 只认这两种请求，其余一律应答 SUCCESS（心跳之类别来烦测试）。
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

        /// <summary>脚本化前 N 笔 46 的应答码；耗尽后一直用 tail。</summary>
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
            Assert.True(RemotingSerializable.Decode(r.Body, out JsonValue v), "46 的 body 是 JSON");
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
                // 关掉监听端口不需要处理任何异常
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
                    return;  // Dispose 之后 Accept 必然抛，正常收工
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
                            // 半路断连、解码失败：丢掉这条连接即可
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
                    continue;  // oneway 请求没有应答可写
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

            // 心跳等非 46 请求一律应答成功，别让后台线程卡在错误上
            return req.IsOnewayRpc() ? null : Echo(req, ResponseCode.Success, null);
        }

        private static RemotingCommand Echo(RemotingCommand req, int code, string? remark)
        {
            var resp = RemotingCommand.CreateResponseCommand(code, remark);
            // 客户端按 opaque 配对，串了的响应会被当噪声丢掉
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

    /// <summary>一个已经关掉、必然拒连的回环端口（比固定端口可靠）。</summary>
    private static string DeadAddr()
    {
        var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        s.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        s.Listen(1);
        string addr = ((IPEndPoint)s.LocalEndPoint!).ToString();
        s.Dispose();
        return addr;
    }

    /// <summary>起一个实例并把 Topic 的路由塞进缓存（broker 指向 mock 自己）。</summary>
    private static MQClientInstance Seeded(MockBroker mock, string? brokerAddr = null) =>
        SeededWithRoute(mock, route => route.BrokerDatas.Add(new BrokerData("DefaultCluster",
            "broker-a", new SortedDictionary<long, string>
            {
                { MixAll.MasterId, brokerAddr ?? mock.Addr },
            })));

    /// <summary>自定义路由形状的版本：由调用方往 route 里塞 broker。</summary>
    private static MQClientInstance SeededWithRoute(MockBroker mock, Action<TopicRouteData> fill)
    {
        var instance = new MQClientInstance(ClientId, new List<string> { mock.Addr });
        var route = new TopicRouteData();
        fill(route);
        mock.AddRoute(Topic, route);
        Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic), "路由要真的进缓存");
        return instance;
    }

    /// <summary>跑一段逻辑并回收实例（定时器线程不关会串到下一个用例）。</summary>
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

    // ---------------------------------------------------------------- 用例

    /// <summary>TAG（含 null / 空串）一律不发 46：Java ExpressionType.isTagType 短路。</summary>
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

    /// <summary>SQL92 那一笔的形状：无 extFields、body 三段、SubscriptionData 只有 7 个键。</summary>
    [Fact]
    public void Sql92RequestShapeMatchesJava()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(Array.Empty<(int, string)>());
        Using(mock, instance =>
            instance.CheckSubscriptionsInBroker(Group, new List<SubscriptionData> { Sql92Sub() }));
        Assert.Equal(1, mock.CheckCount);

        CheckRecord record = mock.RecordAt(0);
        // 请求头 null ⇒ 线上没有 extFields
        Assert.Empty(record.Ext);

        JsonValue body = mock.BodyJson(0);
        Assert.Equal(ClientId, body.Get("clientId").StringValue());
        Assert.Equal(Group, body.Get("group").StringValue());
        JsonValue? sd = body.Find("subscriptionData");
        Assert.NotNull(sd);
        Assert.Equal(Topic, sd!.Get("topic").StringValue());
        Assert.Equal("a > 10", sd.Get("subString").StringValue());
        Assert.Equal(RocketMQ.Common.ExpressionType.Sql92, sd.Get("expressionType").StringValue());
        // Java SubscriptionData 的序列化字段名（filterClassSource 是 @JSONField(serialize=false)）
        string[] keys = sd.ObjectItems().Select(kv => kv.Key).OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(new[]
        {
            "classFilterMode", "codeSet", "expressionType", "subString", "subVersion", "tagsSet",
            "topic",
        }, keys);
        Assert.Null(sd.Find("filterClassSource"));
    }

    /// <summary>SUBSCRIPTION_PARSE_FAILED(23) 原样带上响应码 —— 这是启动失败的判据。</summary>
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

    /// <summary>broker 未开 enablePropertyFilter 时回 SYSTEM_ERROR(1)（Java 同码）。</summary>
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

    /// <summary>查不到路由 → 跳过（Java findBrokerAddrByTopic 返回 null 即 continue）。</summary>
    [Fact]
    public void NoRouteSkipsTheSubscription()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(Array.Empty<(int, string)>());
        // 刻意不塞路由：只读缓存，拿不到就跳过
        var instance = new MQClientInstance(ClientId, new List<string> { mock.Addr });
        try
        {
            instance.CheckSubscriptionsInBroker(Group, new List<SubscriptionData> { Sql92Sub() });
            Assert.Equal(0, mock.CheckCount);
            // 只读缓存：路由没进表就不会被顺手补拉
            Assert.Null(instance.FindBrokerAddrByTopic(Topic));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    /// <summary>连不上 broker 时 Java 不吞异常，而是换成一段固定文案再抛。</summary>
    [Fact]
    public void TransportErrorIsWrappedWithJavaMessage()
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

    /// <summary>空订阅集合什么都不发。</summary>
    [Fact]
    public void EmptySubscriptionsSendNothing()
    {
        using MockBroker mock = MockBroker.Start();
        mock.ScriptChecks(Array.Empty<(int, string)>());
        Using(mock, instance =>
            instance.CheckSubscriptionsInBroker(Group, new List<SubscriptionData>()));
        Assert.Equal(0, mock.CheckCount);
    }

    /// <summary>master 优先：master + 一个拒连从节点，五笔全部落在 master。</summary>
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

    /// <summary>CheckClientRequestBody 往返；namespace 只在置位时参与序列化。</summary>
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

    /// <summary>MQClientApiTimeoutMillis 就是 Java ClientConfig.mqClientApiTimeout 的默认值。</summary>
    [Fact]
    public void MqClientApiTimeoutMatchesJava()
    {
        Assert.Equal(3000, MQClientInstance.MqClientApiTimeoutMillis);
    }
}
