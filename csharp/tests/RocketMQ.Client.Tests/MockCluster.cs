// An in-process fake cluster (1 namesrv + N brokers, speaking only the remoting protocol).
//
// A real cluster cannot produce SYSTEM_BUSY, and it cannot produce "a slow broker eats
// the whole budget" either, yet those two are the entire difficulty of the send kernel,
// so each broker's response code, response delay and "simply never answers" is scripted here.
//
// Extracted out of SendRetryTests to be shared by two kinds of cases: the synchronous
// retry kernel (SendRetryTests) and the async send kernel (ProducerAsyncTests).
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
// PropertyMap is a global using alias on the src side (SortedDictionary<string,string>), so the test project has to declare it explicitly.
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

/// <summary>
/// The evidence of one request that went on the wire: the request code
/// (send/route/heartbeat...), opaque, extFields and the raw body. The client talking
/// about itself does not count as evidence -- only fields captured off the socket prove
/// that a hook really took effect.
/// </summary>
internal sealed class WireRecord
{
    public int Code { get; init; }

    /// <summary>The opaque at the time it went on the wire (the retry chain requires a
    /// new value each round, otherwise the responses of two attempts cross wires).</summary>
    public int Opaque { get; init; }

    public PropertyMap Ext { get; init; } = new();
    public byte[] Body { get; init; } = Array.Empty<byte>();
    public bool HasBody { get; init; }

    public byte Language { get; init; }

    /// <summary>The arrival instant (ms, relative to <see cref="MockCluster.ClockOrigin" />).
    /// The rhythm assertions of timed tasks depend on it alone: the request content is
    /// perfectly normal and only the arrival instant proves the period is right.</summary>
    public long ArrivalMs { get; init; }

    public static WireRecord Of(RemotingCommand req, long arrivalMs = 0) => new()
    {
        Code = req.Code,
        Opaque = req.Opaque,
        Ext = new PropertyMap(req.ExtFields),
        Body = req.Body,
        HasBody = req.HasBody,
        Language = req.Language,
        ArrivalMs = arrivalMs,
    };

    /// <summary>Restored into a raw request, used to recompute the signature broker
    /// side.</summary>
    public RemotingCommand ToCommand()
    {
        var cmd = RemotingCommand.CreateRequestCommand(Code, null);
        foreach ((string key, string value) in Ext)
        {
            cmd.AddExtField(key, value);
        }

        cmd.Body = Body;
        cmd.HasBody = HasBody;
        return cmd;
    }
}

/// <summary>
/// An in-process fake cluster. Broker names are fixed as broker-0..N and the queue order
/// in the route matches, so the first send necessarily lands on broker-0 (queue selection
/// is round-robin and the cursor starts at 0).
/// </summary>
internal sealed class MockCluster : IDisposable
{
    /// <summary>The sentinel response code in the script meaning "receive but do not
    /// answer": it forces the client into a timeout.</summary>
    public const int NoReply = -1;

    /// <summary>Cap on the number of evidence records: background threads keep sending,
    /// a few hundred is enough to tell "was it tagged or not".</summary>
    private const int RequestLogCap = 500;

    /// <summary>Cap on a single frame's length; beyond it the connection is dropped
    /// directly (the same criterion the client uses).</summary>
    private const int MaxFrame = 20 * 1024 * 1024;

    private readonly object _gate = new();
    private readonly List<BrokerScript> _brokers = new();
    private readonly List<string> _brokerAddrs = new();
    private readonly List<Socket> _sockets = new();
    private readonly List<WireRecord> _requests = new();
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private bool _routeOk = true;

    public string NamesrvAddr { get; private init; } = string.Empty;

    /// <summary>The address of broker-N in the route (in the same order as the index);
    /// nobody is listening on the <see cref="DeadAddr" /> ones.</summary>
    public IReadOnlyList<string> BrokerAddrs
    {
        get
        {
            lock (_gate)
            {
                return new List<string>(_brokerAddrs);
            }
        }
    }

    /// <summary>A single broker's script: pops (response code, sleep ms before
    /// responding) in order, then keeps using Tail forever once exhausted.</summary>
    private sealed class BrokerScript
    {
        public Queue<(int Code, int DelayMs)> Steps { get; } = new();
        public (int Code, int DelayMs) Tail { get; set; } = (ResponseCode.Success, 0);
        public int Requests;

        /// <summary>Every request code that landed on this broker, including heartbeats
        /// and admin calls (the send counter above only covers send codes).</summary>
        public readonly List<int> RequestCodes = new();

        public readonly List<int> SendOpaques = new();
    }

    /// <summary>Starts brokerCount really-listening brokers plus one namesrv.</summary>
    public static MockCluster Start(int brokerCount, bool routeOk = true) =>
        StartAt(Enumerable.Repeat<string?>(null, brokerCount).ToList(), routeOk);

    /// <summary>
    /// Starts the cluster with the given address table: <b>null = start a really-listening
    /// mock broker</b>, non-null is written into the route verbatim (putting
    /// <see cref="DeadAddr" /> there produces "connecting to this broker always
    /// fails"). Broker names are still broker-0..N, so the index of
    /// <see cref="Requests" /> matches the order here.
    /// </summary>
    public static MockCluster StartAt(List<string?> addrs, bool routeOk = true)
    {
        var listeners = new List<(int Index, Socket Socket)>();
        var final = new List<string>();
        for (int i = 0; i < addrs.Count; ++i)
        {
            if (addrs[i] is not null)
            {
                final.Add(addrs[i]!);
                continue;
            }

            Socket l = BindLoopback();
            listeners.Add((i, l));
            final.Add(EndPointOf(l));
        }

        MockCluster cluster = WithAddrs(final, routeOk);
        foreach ((int index, Socket socket) in listeners)
        {
            int broker = index;
            Serve(socket, req => cluster.BrokerRespond(broker, req));
        }

        return cluster;
    }

    /// <summary>Starts only the namesrv, with the route pointing at the given address (an
    /// already-closed port can be used to produce a connect failure).</summary>
    public static MockCluster WithAddrs(List<string> brokerAddrs, bool routeOk = true)
    {
        Socket namesrv = BindLoopback();
        var cluster = new MockCluster
        {
            NamesrvAddr = EndPointOf(namesrv),
            _routeOk = routeOk,
        };
        lock (cluster._gate)
        {
            cluster._brokerAddrs.AddRange(brokerAddrs);
            for (int i = 0; i < brokerAddrs.Count; ++i)
            {
                cluster._brokers.Add(new BrokerScript());
            }
        }

        Serve(namesrv, cluster.NamesrvRespond);
        return cluster;
    }

    /// <summary>Scripts the index-th broker: replies following steps first, then uses
    /// tail forever.</summary>
    public void Script(int index, List<(int Code, int DelayMs)> steps,
        (int Code, int DelayMs) tail)
    {
        lock (_gate)
        {
            BrokerScript broker = _brokers[index];
            broker.Steps.Clear();
            foreach ((int, int) step in steps)
            {
                broker.Steps.Enqueue(step);
            }

            broker.Tail = tail;
            broker.Requests = 0;
            broker.SendOpaques.Clear();
        }
    }

    /// <summary>Makes the index-th broker **receive but never answer** from now on (forcing
    /// the client into a response timeout).</summary>
    public void MakeSilent(int index) =>
        Script(index, new List<(int, int)>(), (NoReply, 0));

    /// <summary>
    /// Attaches a response body to a request code (per broker). Used for those admin
    /// calls where "no body equals failure" -- e.g. the caller of 222 requires at least
    /// one broker to return a non-empty ResetOffsetBody.
    /// </summary>
    public void SetReplyBody(int code, Func<RemotingCommand, byte[]> bodyFor)
    {
        lock (_gate)
        {
            _replyBodies[code] = bodyFor;
        }
    }

    private readonly Dictionary<int, Func<RemotingCommand, byte[]>> _replyBodies = new();

    private readonly Dictionary<int, Func<RemotingCommand, Dictionary<string, string>>> _replyExts = new();

    private readonly Dictionary<int, int> _replyCodes = new();

    /// <summary>
    /// Attaches response extFields to a request code (per request, so a paged endpoint can
    /// answer totalTopicNum / totalGroupNum according to the incoming seq).
    /// </summary>
    public void SetReplyExt(int code, Func<RemotingCommand, Dictionary<string, string>> extFor)
    {
        lock (_gate)
        {
            _replyExts[code] = extFor;
        }
    }

    /// <summary>
    /// Scripts the response code of one request code (every broker answers it that way).
    /// For primitives whose contract is "the caller sees the broker code verbatim": the
    /// generic SUCCESS answer cannot prove a non-success is neither swallowed nor thrown.
    /// </summary>
    public void SetReplyCode(int code, int replyCode, string? remark = null)
    {
        lock (_gate)
        {
            _replyCodes[code] = replyCode;
            if (remark is not null)
            {
                _replyRemarks[code] = remark;
            }
            else
            {
                _replyRemarks.Remove(code);
            }
        }
    }

    private readonly Dictionary<int, string> _replyRemarks = new();

    private Func<RemotingCommand, byte[]>? _routeBodyFor;

    /// <summary>
    /// Overrides the name server's route response body (computed per request). By default
    /// that fixed route has only one queue per broker, and cases like "the topic scales
    /// up/down after the consumer has started" must be able to change the read queue
    /// count.
    /// </summary>
    public void SetRouteBody(Func<RemotingCommand, byte[]> bodyFor)
    {
        lock (_gate)
        {
            _routeBodyFor = bodyFor;
        }
    }

    /// <summary>
    /// A scripted PULL_MESSAGE response (FIFO hit, replied once per request). When
    /// <see cref="Gate" /> is non-null the response first waits on it -- used to turn the
    /// "the request has reached the broker, the response is still on its way" window
    /// into a deterministic one.
    /// </summary>
    internal sealed class ScriptedPull
    {
        public int Code { get; init; } = ResponseCode.PullNotFound;
        public Dictionary<string, string> Ext { get; init; } = new();
        public byte[]? Body { get; init; }
        public ManualResetEventSlim? Gate { get; init; }
    }

    private readonly Queue<ScriptedPull> _pullScripts = new();

    public void ScriptPull(int code, Dictionary<string, string>? ext = null, byte[]? body = null,
        ManualResetEventSlim? gate = null)
    {
        lock (_gate)
        {
            _pullScripts.Enqueue(new ScriptedPull
            {
                Code = code,
                Ext = ext ?? new Dictionary<string, string>(),
                Body = body,
                Gate = gate,
            });
        }
    }

    private RemotingCommand PullRespond(RemotingCommand req)
    {
        ScriptedPull? script;
        lock (_gate)
        {
            script = _pullScripts.Count > 0 ? _pullScripts.Dequeue() : null;
        }

        // A 5s backstop: even when a case fails it must not block a connection thread forever (Dispose waits on it).
        script?.Gate?.Wait(TimeSpan.FromSeconds(5));

        if (script is null)
        {
            string offset = req.ExtFields.TryGetValue("queueOffset", out string? q) ? q : "0";
            RemotingCommand empty = Respond(req, ResponseCode.PullNotFound, null);
            empty.AddExtField("nextBeginOffset", offset);
            empty.AddExtField("minOffset", "0");
            empty.AddExtField("maxOffset", "0");
            return empty;
        }

        RemotingCommand resp = Respond(req, script.Code,
            script.Code == ResponseCode.Success ? null : "mock pull");
        foreach (KeyValuePair<string, string> kv in script.Ext)
        {
            resp.AddExtField(kv.Key, kv.Value);
        }

        if (script.Body is { Length: > 0 })
        {
            resp.Body = script.Body;
            resp.HasBody = true;
        }

        return resp;
    }

    /// <summary>The number of SEND requests the index-th broker received.</summary>
    public int Requests(int index)
    {
        lock (_gate)
        {
            return _brokers[index].Requests;
        }
    }

    /// <summary>How many requests of this code landed on the index-th broker. Unlike
    /// <see cref="Requests" /> this also covers heartbeats and admin calls, which is what
    /// the per-broker fan-out assertions need.</summary>
    public int RequestsAt(int index, int code)
    {
        lock (_gate)
        {
            return _brokers[index].RequestCodes.Count(c => c == code);
        }
    }

    /// <summary>
    /// The opaque of the n-th (from 0) **send** request on the index-th broker; it
    /// returns <see cref="int.MinValue" /> when that many have not arrived yet. The async
    /// retry chain requires a new opaque each round and concurrent sends require them to
    /// differ pairwise.
    /// </summary>
    public int SendOpaque(int index, int n)
    {
        lock (_gate)
        {
            List<int> opaques = _brokers[index].SendOpaques;
            return n >= 0 && n < opaques.Count ? opaques[n] : int.MinValue;
        }
    }

    /// <summary>Records every request that goes on the wire (both namesrv and broker), for
    /// hook assertions to look back at.</summary>
    private void Record(RemotingCommand req)
    {
        lock (_gate)
        {
            if (_requests.Count < RequestLogCap)
            {
                _requests.Add(WireRecord.Of(req, _clock.ElapsedMilliseconds));
            }
        }
    }

    /// <summary>The monotonic clock at the fake cluster's start point (the time origin of
    /// <see cref="WireRecord.ArrivalMs" />).</summary>
    public long ClockOrigin => _clock.ElapsedMilliseconds;

    public void ClearRequests()
    {
        lock (_gate)
        {
            _requests.Clear();
        }
    }

    /// <summary>
    /// Takes out all the evidence snapshots in arrival order. The only landing point for
    /// deciding "who came first" -- for example an unregister(35) must come after the
    /// business send, and over the long connection that is not closed yet.
    /// </summary>
    public List<WireRecord> Records()
    {
        lock (_gate)
        {
            return new List<WireRecord>(_requests);
        }
    }

    /// <summary>How many requests were received on the given request code.</summary>
    public int CountRequests(int code)
    {
        lock (_gate)
        {
            return _requests.Count(r => r.Code == code);
        }
    }

    /// <summary>On the given request code, how many requests had key=value in
    /// extFields.</summary>
    public int CountRequestsWith(int code, string key, string value)
    {
        lock (_gate)
        {
            return _requests.Count(r =>
                r.Code == code && r.Ext.TryGetValue(key, out string? v) && v == value);
        }
    }

    public WireRecord? FirstSendRequest()
    {
        lock (_gate)
        {
            return _requests.FirstOrDefault(r => IsSendCode(r.Code));
        }
    }

    /// <summary>The evidence of the n-th (from 0) send request, across brokers in
    /// arrival order.</summary>
    public WireRecord? SendRequestAt(int n)
    {
        lock (_gate)
        {
            return _requests.Where(r => IsSendCode(r.Code)).ElementAtOrDefault(n);
        }
    }

    /// <summary>Whether any request on the given request code carried this
    /// extField.</summary>
    public bool AnyRequestHas(int code, string key)
    {
        lock (_gate)
        {
            return _requests.Any(r => r.Code == code && r.Ext.ContainsKey(key));
        }
    }

    public void Dispose()
    {
        foreach (Socket s in _sockets)
        {
            try
            {
                s.Dispose();
            }
            catch (Exception)
            {
                // Closing the listening port does not need any exception handling
            }
        }
    }

    private RemotingCommand? NamesrvRespond(RemotingCommand req)
    {
        Record(req);
        if (req.Code == RequestCode.GetRouteinfoByTopic)
        {
            List<string> addrs;
            bool ok;
            Func<RemotingCommand, byte[]>? routeBodyFor;
            lock (_gate)
            {
                addrs = new List<string>(_brokerAddrs);
                ok = _routeOk;
                routeBodyFor = _routeBodyFor;
            }

            if (!ok)
            {
                return Respond(req, ResponseCode.TopicNotExist, "mock: no route");
            }

            RemotingCommand resp = Respond(req, ResponseCode.Success, null);
            resp.Body = routeBodyFor?.Invoke(req) ?? BuildRoute(addrs);
            resp.HasBody = true;
            return resp;
        }

        Func<RemotingCommand, byte[]>? namesrvBodyFor;
        Func<RemotingCommand, Dictionary<string, string>>? namesrvExtFor;
        lock (_gate)
        {
            _replyBodies.TryGetValue(req.Code, out namesrvBodyFor);
            _replyExts.TryGetValue(req.Code, out namesrvExtFor);
        }

        if (namesrvBodyFor is not null)
        {
            RemotingCommand withBody = Respond(req, ResponseCode.Success, null);
            withBody.Body = namesrvBodyFor(req);
            withBody.HasBody = true;
            if (namesrvExtFor is not null)
            {
                foreach (KeyValuePair<string, string> kv in namesrvExtFor(req))
                {
                    withBody.AddExtField(kv.Key, kv.Value);
                }
            }

            return withBody;
        }

        RemotingCommand plain = Respond(req, ResponseCode.Success, null);
        if (namesrvExtFor is not null)
        {
            foreach (KeyValuePair<string, string> kv in namesrvExtFor(req))
            {
                plain.AddExtField(kv.Key, kv.Value);
            }
        }

        return plain;
    }

    private RemotingCommand? BrokerRespond(int index, RemotingCommand req)
    {
        Record(req);
        lock (_gate)
        {
            _brokers[index].RequestCodes.Add(req.Code);
        }
        // Both 361 and 11 are answered as pull responses: whether the client switches
        // code is asserted separately by offline cases (#107); this only covers "the code
        // has to work once it is switched too".
        if (req.Code == RequestCode.PullMessage || req.Code == RequestCode.LitePullMessage)
        {
            return req.IsOnewayRpc() ? null : PullRespond(req);
        }

        if (!IsSendCode(req.Code))
        {
            Func<RemotingCommand, byte[]>? bodyFor;
            Func<RemotingCommand, Dictionary<string, string>>? extFor;
            int scriptedCode;
            string? scriptedRemark;
            bool silent;
            lock (_gate)
            {
                // Silence is a whole-broker state: a heartbeat or admin call must run into
                // the same wait the send path produces, not a canned SUCCESS.
                silent = _brokers[index].Tail.Code == NoReply;
                _replyBodies.TryGetValue(req.Code, out bodyFor);
                _replyExts.TryGetValue(req.Code, out extFor);
                scriptedCode = _replyCodes.TryGetValue(req.Code, out int c) ? c : ResponseCode.Success;
                scriptedRemark = _replyRemarks.TryGetValue(req.Code, out string? m) ? m : null;
            }

            if (silent && !req.IsOnewayRpc())
            {
                return null;
            }

            if (bodyFor is not null && !req.IsOnewayRpc())
            {
                RemotingCommand withBody = Respond(req, ResponseCode.Success, null);
                withBody.Body = bodyFor(req);
                withBody.HasBody = true;
                if (extFor is not null)
                {
                    foreach (KeyValuePair<string, string> kv in extFor(req))
                    {
                        withBody.AddExtField(kv.Key, kv.Value);
                    }
                }

                return withBody;
            }

            if (req.IsOnewayRpc())
            {
                return null;
            }

            if (scriptedCode != ResponseCode.Success)
            {
                return Respond(req, scriptedCode, scriptedRemark ?? "mock failure");
            }

            // Heartbeats and other non-send requests are always answered successfully, do not let the background thread get stuck on an error
            RemotingCommand defaultReply = Respond(req, ResponseCode.Success, scriptedRemark);
            if (extFor is not null)
            {
                foreach (KeyValuePair<string, string> kv in extFor(req))
                {
                    defaultReply.AddExtField(kv.Key, kv.Value);
                }
            }

            return defaultReply;
        }

        int code;
        int delay;
        int seq;
        lock (_gate)
        {
            BrokerScript broker = _brokers[index];
            (int, int) step = broker.Steps.Count > 0 ? broker.Steps.Dequeue() : broker.Tail;
            (code, delay) = step;
            broker.Requests++;
            seq = broker.Requests;
            broker.SendOpaques.Add(req.Opaque);
        }

        if (delay > 0)
        {
            Thread.Sleep(delay);
        }

        if (req.IsOnewayRpc() || code == NoReply)
        {
            return null;
        }

        RemotingCommand resp = Respond(req, code,
            code == ResponseCode.Success ? null : "mock failure");
        if (IsStoreStatus(code))
        {
            // Only when the broker really took the message is there a msgId/queueOffset to reply with
            resp.AddExtField("msgId", "MOCK-" + index.ToString(CultureInfo.InvariantCulture)
                                         + "-" + seq.ToString(CultureInfo.InvariantCulture));
            resp.AddExtField("queueId", "0");
            resp.AddExtField("queueOffset", seq.ToString(CultureInfo.InvariantCulture));
        }

        return resp;
    }

    private static bool IsStoreStatus(int code) =>
        code is ResponseCode.Success or ResponseCode.FlushDiskTimeout
            or ResponseCode.SlaveNotAvailable or ResponseCode.FlushSlaveTimeout;

    private static bool IsSendCode(int code) =>
        code is RequestCode.SendMessage or RequestCode.SendMessageV2
            or RequestCode.SendBatchMessage or RequestCode.SendReplyMessage
            or RequestCode.SendReplyMessageV2;

    /// <summary>The queue order in the route matches the broker names, and each has only
    /// one queue.</summary>
    private static byte[] BuildRoute(List<string> brokerAddrs)
    {
        var route = new TopicRouteData();
        for (int i = 0; i < brokerAddrs.Count; ++i)
        {
            string name = BrokerName(i);
            route.QueueDatas.Add(new QueueData(name, 1, 1,
                PermName.PermRead | PermName.PermWrite, 0));
            route.BrokerDatas.Add(new BrokerData("MockCluster", name,
                new SortedDictionary<long, string> { { MixAll.MasterId, brokerAddrs[i] } }));
        }

        return route.Encode();
    }

    internal static string BrokerName(int index) =>
        "broker-" + index.ToString(CultureInfo.InvariantCulture);

    private static RemotingCommand Respond(RemotingCommand req, int code, string? remark)
    {
        var resp = RemotingCommand.CreateResponseCommand(code, remark);
        // The client pairs requests by opaque, a cross-wired response is dropped as noise
        resp.Opaque = req.Opaque;
        resp.SerializeTypeCurrentRpc = req.SerializeTypeCurrentRpc;
        return resp;
    }

    private static Socket BindLoopback()
    {
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        server.Listen(16);
        return server;
    }

    private static string EndPointOf(Socket server) =>
        ((IPEndPoint)server.LocalEndPoint!).ToString();

    internal static string DeadAddr()
    {
        Socket probe = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        string addr = ((IPEndPoint)probe.LocalEndPoint!).ToString();
        probe.Dispose();
        return addr;
    }

    /// <summary>
    /// The accept loop: one thread per connection. A **dedicated thread** is used here
    /// rather than a Task on purpose -- reading a frame and the scripted delay are both
    /// blocking calls, and a few hundred work items stuck in Read would starve the thread
    /// pool; measured, that drags the next accept out to seconds and turns the timing
    /// assertions into false failures.
    /// </summary>
    private static void Serve(Socket listener, Func<RemotingCommand, RemotingCommand?> respond)
    {
        new Thread(() =>
        {
            while (true)
            {
                Socket client;
                try
                {
                    client = listener.Accept();
                }
                catch (Exception)
                {
                    return; // after Dispose, Accept necessarily throws, this is a normal shutdown
                }

                new Thread(() =>
                {
                    using (client)
                    {
                        try
                        {
                            ServeFrames(client, respond);
                        }
                        catch (Exception)
                        {
                            // A mid-stream disconnect or a decode failure: just drop this connection
                        }
                    }
                })
                { IsBackground = true }.Start();
            }
        })
        { IsBackground = true }.Start();
    }

    private static void ServeFrames(Socket client, Func<RemotingCommand, RemotingCommand?> respond)
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

            RemotingCommand? resp = respond(req);
            if (resp is null)
            {
                continue;
            }

            byte[] wire = resp.Encode();
            stream.Write(wire, 0, wire.Length);
        }
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
