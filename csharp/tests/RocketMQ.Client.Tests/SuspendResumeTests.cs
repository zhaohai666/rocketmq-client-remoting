// Offline unit tests for push consumer suspend()/resume() -- an in-process fake cluster, no real cluster needed.
//
//
// Why the evidence must come off the socket: suspend() neither throws nor returns
// anything, and the only observable fact is "does PULL_MESSAGE(11) still go out". Here it
// suspends for 2.5s and counts the requests, then counts once more after resuming.
using System.Buffers.Binary;
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

public class SuspendResumeTests
{
    private const string Topic = "SuspendResumeTopic";
    private const string Broker = "broker-a";
    private const int MaxFrame = 20 * 1024 * 1024;

    /// <summary>An arriving request: the code plus the extFields exactly as they went on
    /// the wire.</summary>
    private sealed record WireReq(int Code, PropertyMap Ext);

    /// <summary>
    /// A fake namesrv + fake broker: answers routes, recognises the clientID from the
    /// heartbeat so the rebalance really can get queues, answers "no committed offset"
    /// and "no new messages this round", and records every request.
    /// </summary>
    private sealed class SuspendEndpoint : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<WireReq> _reqs = new();
        private readonly Socket _listener;
        private string _clientId = string.Empty;

        public string Addr { get; }

        private SuspendEndpoint(Socket listener)
        {
            _listener = listener;
            Addr = ((IPEndPoint)listener.LocalEndPoint!).ToString();
            new Thread(AcceptLoop) { IsBackground = true }.Start();
        }

        public static SuspendEndpoint Start()
        {
            var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            server.Listen(16);
            return new SuspendEndpoint(server);
        }

        public int Count(int code)
        {
            lock (_gate)
            {
                return _reqs.Count(r => r.Code == code);
            }
        }

        /// <summary>The queues seen in pull requests (consumer group|queueId), used to
        /// verify that suspending did not tear down the assignment.</summary>
        public SortedSet<string> PulledQueues()
        {
            lock (_gate)
            {
                return new SortedSet<string>(_reqs
                    .Where(r => r.Code == RequestCode.PullMessage)
                    .Select(r => QueueKey(r.Ext)));
            }
        }

        private static string QueueKey(PropertyMap ext)
        {
            string group = ext.TryGetValue("consumerGroup", out string? g) ? g : "";
            string queueId = ext.TryGetValue("queueId", out string? q) ? q : "";
            return group + "|" + queueId;
        }

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

            if (req.Code == RequestCode.HeartBeat && req.Body.Length > 0
                && HeartbeatData.Decode(req.Body, out HeartbeatData hb) && hb.ClientId.Length > 0)
            {
                lock (_gate)
                {
                    _clientId = hb.ClientId;
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
                offsetResp.AddExtField("offset", "0");
                return offsetResp;
            }

            if (req.Code == RequestCode.PullMessage && !req.IsOnewayRpc())
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
            route.QueueDatas.Add(new QueueData(Broker, 1, 1, PermName.PermRead | PermName.PermWrite, 0));
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

    [Fact]
    public void SuspendStopsPullsAndResumeRestartsThem()
    {
        using SuspendEndpoint endpoint = SuspendEndpoint.Start();

        var consumer = new DefaultMQPushConsumer("CG_parity_suspend")
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        consumer.SetNamesrvAddr(endpoint.Addr);
        consumer.Subscribe(Topic, "*");
        consumer.SetMessageListener(new NullListener());
        consumer.Start();
        try
        {
            string key = DefaultMQPushConsumer.OffsetKeyForTest(new MessageQueue(Topic, Broker, 0));

            Assert.True(WaitFor(() => endpoint.Count(RequestCode.PullMessage) >= 2, TimeSpan.FromSeconds(10)),
                "the pull loop must really be running before the suspend; request codes that arrived:" + string.Join(",", endpoint.Codes()));

            consumer.Suspend();
            Assert.True(consumer.IsPaused, "isPause() must reflect the suspend truthfully");

            // The one already in flight before the suspend lands first, and only then is the
            // baseline taken, otherwise a legitimate request counts as a leak; the 300ms
            // also gives the loop time to "stamp + stop at the suspend gate".
            Thread.Sleep(600);
            int baseline = endpoint.Count(RequestCode.PullMessage);
            long stampBefore = consumer.LastPullAt(key);
            Thread.Sleep(2500);

            int pausedCount = endpoint.Count(RequestCode.PullMessage);
            Assert.True(pausedCount == baseline,
                "no new PULL_MESSAGE may go on the wire while suspended:"
                + $"baseline {baseline}, after 2.5s suspended {pausedCount}");

            // An ordering invariant: the stamping happens **before** the suspend decision,
            // so a suspended loop still stamps on time and the 120s stalled criterion
            // never judges a merely suspended consumer to be a dead loop.
            Assert.True(consumer.LastPullAt(key) > stampBefore,
                "lastPullTimestamp must keep advancing while suspended, otherwise isPullExpired would wrongly tear down this loop");
            Assert.False(consumer.PullStalledForTest(key, UtilAll.CurrentTimeMillis()),
                "suspended is not stalled: the self-healing criterion must stay quiet during this window");

            SortedSet<string> queuesBefore = endpoint.PulledQueues();
            consumer.Resume();
            Assert.False(consumer.IsPaused);

            Assert.True(WaitFor(() => endpoint.Count(RequestCode.PullMessage) > baseline, TimeSpan.FromSeconds(5)),
                "pulling must resume after Resume()");
            Assert.True(queuesBefore.SetEquals(endpoint.PulledQueues()),
                "suspending/resuming only pauses sending requests, the assignment set must be kept in place");
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void SuspendAndResumeAreIdempotentAndSafeBeforeStart()
    {
        var consumer = new DefaultMQPushConsumer("CG_parity_suspend_api");
        Assert.False(consumer.IsPaused);

        consumer.Suspend();
        consumer.Suspend();
        Assert.True(consumer.IsPaused);

        consumer.Resume();
        consumer.Resume();
        Assert.False(consumer.IsPaused);
    }
}
