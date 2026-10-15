// Real-machine verification of UNREGISTER_CLIENT(35) on producer exit (C#).
// Usage: rmq unreg-live [namesrv]
//
// Scenario sections: U1~U6.
//
// Note on the strength of the criteria: in C# each producer holds its own
// `MQClientInstance` and its own TCP connection, and the connection is closed on exit too,
// so the broker's channel scan would remove the group anyway -- looking at U5 alone you
// cannot tell whether the 35 or the disconnect deserves the credit, so a hook has to
// capture the frame (U3) and prove directly that "this one really went on the wire".
// Also: "every 35 gets a SUCCESS" is **not observable** in this port -- the transport layer
// deliberately does not call `IRpcHook#DoAfterResponse` (see the notes in
// Remoting/RemotingClient.cs), and U5's broker-side effect is its substitute criterion. The
// header shape, the fan-out to the slave and the swallowed single-machine failure are pinned
// offline in `tests/RocketMQ.Client.Tests/ProducerUnregisterTests.cs`.
//
// Prerequisite: a NameServer + Broker are up, with ``autoCreateTopicEnable=true``.
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class ProducerUnregisterLive
{
    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            _pass++;
            Console.WriteLine("  [PASS] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
        else
        {
            _fail++;
            Console.WriteLine("  [FAIL] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
    }

    private static string Num(int v) => v.ToString(CultureInfo.InvariantCulture);

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 200)
    {
        long deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeoutMs;
        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    private static string Join(IEnumerable<string> v) => string.Join(",", v);

    private sealed class UnregisterProbe : IRpcHook
    {
        private readonly object _lk = new();
        private readonly List<Frame> _frames = new();
        private int _count;

        public sealed class Frame
        {
            public int Seq { get; init; }
            public int Code { get; init; }
            public string Addr { get; init; } = string.Empty;
            public PropertyMap Ext { get; init; } = new();
        }

        public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
        {
            PropertyMap ext = request.CustomHeader is not null
                ? new PropertyMap(request.CustomHeader.ToExtFields())
                : new PropertyMap(request.ExtFields);
            lock (_lk)
            {
                _frames.Add(new Frame
                {
                    Seq = ++_count,
                    Code = request.Code,
                    Addr = remoteAddr,
                    Ext = ext,
                });
            }
        }

        /// <summary>This port's transport layer deliberately never calls this (see the
        /// file header notes), so it is never triggered.</summary>
        public void DoAfterResponse(string remoteAddr, RemotingCommand request,
            RemotingCommand? response)
        {
            _ = remoteAddr;
            _ = request;
            _ = response;
        }

        public List<Frame> Frames()
        {
            lock (_lk) return new List<Frame>(_frames);
        }

        public List<Frame> Of(int code) => Frames().Where(f => f.Code == code).ToList();

        /// <summary>The sequence number of the last business send (used to decide "the
        /// unregister comes after the send").</summary>
        public int LastOf(params int[] codes)
        {
            int seq = -1;
            foreach (Frame f in Frames())
            {
                if (codes.Contains(f.Code)) seq = f.Seq;
            }

            return seq;
        }
    }

    private static string ExtField(UnregisterProbe.Frame f, string key) =>
        f.Ext.TryGetValue(key, out string? v) ? v : string.Empty;

    private static string ExtText(PropertyMap ext) =>
        "{" + string.Join(", ", ext.Select(kv => kv.Key + "=" + kv.Value)) + "}";

    private static List<string> ConnectionClientIds(DefaultMQAdminExt admin, string addr,
        string group)
    {
        try
        {
            ProducerConnection pc = admin.ExamineProducerConnectionInfo(group, addr);
            return pc.ConnectionSet.Select(c => c.ClientId).ToList();
        }
        catch (Exception e)
        {
            string msg = e.Message;
            if (msg.Contains("not exist", StringComparison.Ordinal)
                || msg.Contains("not online", StringComparison.Ordinal))
            {
                return new List<string>();
            }

            Console.WriteLine("  [diag] ExamineProducerConnectionInfo(" + group + ") threw: " + msg);
            return new List<string>();
        }
    }

    private sealed class ScopedProducer : IDisposable
    {
        public DefaultMQProducer Producer { get; }

        public ScopedProducer(string group, string namesrv, string kind, string stamp,
            IRpcHook? hook)
        {
            Producer = new DefaultMQProducer(group)
            {
                NamesrvAddr = namesrv,
                InstanceName = kind + "-" + stamp,
                SendMsgTimeout = 5000,
            };
            if (hook is not null) Producer.SetRpcHook(hook);
            Producer.Start();
        }

        public void Dispose()
        {
            try
            {
                Producer.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] producer shutdown failed: " + e.Message);
            }
        }
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        var admin = new DefaultMQAdminExt("UNREGADMIN");
        admin.SetNamesrvAddr(namesrv);
        admin.SetTimeoutMillis(10000);
        admin.Start();

        try
        {
            ClusterInfo cluster = new();
            for (int i = 0; i < 40; ++i)
            {
                try
                {
                    cluster = admin.FetchBrokerClusterInfo();
                    if (cluster.BrokerAddrTable.Count > 0) break;
                }
                catch (Exception e)
                {
                    if (i == 39) Console.WriteLine("  [diag] cluster probe failed: " + e.Message);
                }

                Thread.Sleep(1000);
            }

            if (cluster.BrokerAddrTable.Count == 0)
            {
                Check("the cluster liveness probe", false, "the nameServer has no broker registered");
                return 1;
            }

            List<string> brokers = cluster.GetBrokerAddrs();
            Check("the cluster liveness probe", true, "brokers=" + Join(brokers));
            RunChecks(namesrv, brokers, admin);
            Console.WriteLine();
            Console.WriteLine("== result: " + Num(_pass) + "/" + Num(_pass + _fail) + " passed ==");
            return _fail == 0 ? 0 : 1;
        }
        finally
        {
            try
            {
                admin.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] admin shutdown failed: " + e.Message);
            }
        }
    }

    private static void RunChecks(string namesrv, List<string> brokers, DefaultMQAdminExt admin)
    {
        string brokerAddr = brokers[0];
        string topic = "Unreg_" + Stamp;
        string group = "PID_unreg_" + Stamp;
        string peerGroup = "PID_unreg_peer_" + Stamp;

        var probe = new UnregisterProbe();
        using var p = new ScopedProducer(group, namesrv, "unreg", Stamp, probe);
        using var peer = new ScopedProducer(peerGroup, namesrv, "unreg-peer", Stamp, null);

        SendResult r = p.Producer.Send(new Message(topic, Encoding.UTF8.GetBytes("unreg-probe")),
            5000);
        Check("U1 the producer sent successfully", r.SendStatus == SendStatus.SendOk, "msgId=" + r.MsgId);
        peer.Producer.Send(new Message(topic, Encoding.UTF8.GetBytes("peer")), 5000);

        string clientId = p.Producer.ClientId;
        List<string> seen = new();
        bool registered = WaitUntil(() =>
        {
            seen = ConnectionClientIds(admin, brokerAddr, group);
            return seen.Contains(clientId);
        }, 70000);
        Check("U2 after the heartbeat a 204 sees this clientId", registered,
            "clientId=" + clientId + " currently=" + Join(seen));
        List<string> peerSeen = ConnectionClientIds(admin, brokerAddr, peerGroup);
        Check("U2b the control group's registration is visible (this 204 criterion is itself valid)",
            peerSeen.Contains(peer.Producer.ClientId), "currently=" + Join(peerSeen));

        Check("U3 precondition: there must be no 35 before exiting", probe.Of(RequestCode.UnregisterClient).Count == 0,
            "count=" + Num(probe.Of(RequestCode.UnregisterClient).Count));

        // The unregister goes over the long connection that is **still open**: C#'s Shutdown
        // order is "35 first, then close the client", and only the broker side can verify
        // that order (U5).
        p.Producer.Shutdown();

        List<UnregisterProbe.Frame> unregs = probe.Of(RequestCode.UnregisterClient);
        Check("U3 Shutdown sent one 35 to each known broker",
            unregs.Count == brokers.Count,
            "count=" + Num(unregs.Count) + " brokers=" + Num(brokers.Count));
        bool shapeOk = unregs.Count > 0;
        foreach (UnregisterProbe.Frame f in unregs)
        {
            if (ExtField(f, "clientID") != clientId) shapeOk = false;
            if (ExtField(f, "producerGroup") != group) shapeOk = false;
            if (f.Ext.ContainsKey("consumerGroup")) shapeOk = false;
            if (!brokers.Contains(f.Addr)) shapeOk = false;
        }

        Check("U3b the 35 header is clientID + producerGroup, consumerGroup does not go on the wire (that field is passed as null)",
            shapeOk, unregs.Count == 0
                ? "no frame was captured"
                : ("addr=" + unregs[0].Addr + " ext=" + ExtText(unregs[0].Ext)));
        int lastSend = probe.LastOf(RequestCode.SendMessage, RequestCode.SendMessageV2);
        int firstUnreg = unregs.Count == 0 ? -1 : unregs[0].Seq;
        Check("U3c the 35 comes after the business send", lastSend >= 0 && firstUnreg > lastSend,
            "last_send=" + Num(lastSend) + " first_unreg=" + Num(firstUnreg));

        bool gone = WaitUntil(() => ConnectionClientIds(admin, brokerAddr, group).Count == 0, 10000);
        Check("U5 after exiting, a 204 no longer finds this group (the broker replies not exist)", gone,
            "currently=" + Join(ConnectionClientIds(admin, brokerAddr, group)));

        List<string> peerAfter = ConnectionClientIds(admin, brokerAddr, peerGroup);
        Check("U6 the control group is still there (ruling out a false positive of the broker having cleared everything)",
            peerAfter.Contains(peer.Producer.ClientId), "currently=" + Join(peerAfter));
    }
}
