// Remoting transport connection-lifecycle contract, offline against loopback fake peers.
//
// Five behaviours, each measured on a real socket this process owns, and each asserted on two
// things at once: the connection table afterwards, and the log line that has to explain it.
// "The frame vanished and nothing was said" is the defect under test here, so a leg that only
// checks the table proves half of it.
//
//   T1 a frame that does not decode -- and a length prefix outside the accepted range, which is
//      the same event one level earlier -- is an ERROR naming the peer, and the connection goes
//      the way a peer-disconnected one goes; the peer's next valid request is served on a fresh
//      connection, and a *different* connection is untouched.
//   T2 a response whose opaque no request is waiting on is dropped with a WARNING naming the
//      opaque and the address, and the connection stays up.
//   T3 the writable question is about the outbound side, not about having connected: no
//      connection => true, a connection with room => true, a connection whose send buffer is
//      full => false.
//   T4 a connection idle past the limit is closed by the sweep while one still carrying traffic
//      survives; a non-positive configured limit restores the default instead of switching the
//      sweep off, because the sweep is always installed.
//   T5/T6 a synchronous wait that runs out closes the connection it went out on by default and
//      keeps it when that is turned off; an expired in-flight entry logs one line per entry.
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class TransportContractTests
{
    private const int ConnectTimeoutMillis = 1000;
    private const int InvokeTimeoutMillis = 3000;
    private const int StrayOpaque = 987654;

    private enum PeerMode
    {
        Echo,        // answer every request with a matching SUCCESS
        Silent,      // read requests, never answer, never close
        BadHeader,   // answer the first request with an undecodable frame, echo later ones
        BadLength,   // answer the first request with an illegal length prefix, echo later ones
        StrayFirst,  // answer the first request with an orphan response, then the real one
        Sink,        // accept and never read at all (builds send-buffer backpressure)
    }

    /// <summary>
    /// A loopback peer that keeps accepting until disposed, so a client that dropped a connection
    /// can reconnect to the same address. Counters are what the legs assert on: a close this peer
    /// observes is a close the **client** initiated, because no mode here ever closes a connection
    /// itself.
    /// </summary>
    private sealed class Peer : IDisposable
    {
        private readonly Socket _server;
        private readonly PeerMode _mode;
        private readonly ManualResetEventSlim _drain = new(false);
        private volatile bool _disposed;
        private int _connections;
        private int _requests;
        private int _clientCloses;

        public Peer(PeerMode mode)
        {
            _mode = mode;
            _server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _server.Listen(8);
            Address = ((IPEndPoint)_server.LocalEndPoint!).ToString();
            _ = Task.Run(AcceptLoop);
        }

        public string Address { get; }

        public int Requests => Volatile.Read(ref _requests);

        public int Connections => Volatile.Read(ref _connections);

        /// <summary>Closes this peer observed coming from the client side.</summary>
        public int ClientCloses => Volatile.Read(ref _clientCloses);

        /// <summary>Switch a Sink peer from "never reads" to "drains forever", which is what frees
        /// a send buffer the client parked in.</summary>
        public void StartDraining() => _drain.Set();

        private async Task AcceptLoop()
        {
            while (!_disposed)
            {
                Socket client;
                try
                {
                    client = await _server.AcceptAsync();
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                Interlocked.Increment(ref _connections);
                _ = Task.Run(() => Serve(client));
            }
        }

        private async Task Serve(Socket client)
        {
            try
            {
                if (_mode == PeerMode.Sink)
                {
                    // Never read: the peer's receive buffer, then the client's send buffer, fill
                    // up. Only disposal ends the connection, and draining is a leg-controlled
                    // switch.
                    while (!_disposed)
                    {
                        if (!_drain.IsSet)
                        {
                            await Task.Delay(20);
                            continue;
                        }

                        byte[] scratch = new byte[65536];
                        if (await client.ReceiveAsync(scratch) <= 0)
                        {
                            Interlocked.Increment(ref _clientCloses);
                            return;
                        }
                    }

                    return;
                }

                while (!_disposed)
                {
                    byte[]? frame = await NextFrame(client);
                    if (frame is null)
                    {
                        // The client hung up: this is the only close this peer can observe.
                        Interlocked.Increment(ref _clientCloses);
                        return;
                    }

                    RemotingCommand req = RemotingCommand.Decode(frame);
                    int seen = Interlocked.Increment(ref _requests);
                    if (_mode == PeerMode.Silent)
                    {
                        continue; // read, keep the connection, say nothing
                    }

                    if (_mode == PeerMode.BadHeader && seen == 1)
                    {
                        await client.SendAsync(UndecodableFrame());
                        continue;
                    }

                    if (_mode == PeerMode.BadLength && seen == 1)
                    {
                        await client.SendAsync(IllegalLengthFrame());
                        continue;
                    }

                    if (_mode == PeerMode.StrayFirst && seen == 1)
                    {
                        await client.SendAsync(ResponseFrame(StrayOpaque));
                    }

                    await client.SendAsync(ResponseFrame(req.Opaque));
                }
            }
            catch
            {
                Interlocked.Increment(ref _clientCloses);
            }
            finally
            {
                try
                {
                    client.Shutdown(SocketShutdown.Both);
                }
                catch
                {
                    // already disconnected
                }

                client.Dispose();
            }
        }

        /// <summary>One complete frame off the stream, or null once the peer closed it.</summary>
        private async Task<byte[]?> NextFrame(Socket client)
        {
            byte[] lenBuf = new byte[4];
            if (!await ReceiveExact(client, lenBuf))
            {
                return null;
            }

            int total = BinaryPrimitives.ReadInt32BigEndian(lenBuf);
            if (total <= 0 || total > RemotingClient.MaxFrameLength)
            {
                return null;
            }

            byte[] frame = new byte[4 + total];
            Buffer.BlockCopy(lenBuf, 0, frame, 0, 4);
            return await ReceiveExact(client, frame.AsMemory(4)) ? frame : null;
        }

        private async Task<bool> ReceiveExact(Socket client, Memory<byte> buffer)
        {
            int off = 0;
            while (off < buffer.Length && !_disposed)
            {
                int n = await client.ReceiveAsync(buffer[off..]);
                if (n <= 0)
                {
                    return false;
                }

                off += n;
            }

            return off == buffer.Length;
        }

        private static byte[] ResponseFrame(int opaque)
        {
            RemotingCommand resp = RemotingCommand.CreateResponseCommand(ResponseCode.Success, null);
            resp.Opaque = opaque;
            return resp.Encode();
        }

        /// <summary>A frame inside a legal length prefix whose own header length does not fit
        /// inside it: TryDecode rejects it, which is exactly the case the contract says is fatal to
        /// the socket.</summary>
        private static byte[] UndecodableFrame()
        {
            byte[] f = new byte[12];
            BinaryPrimitives.WriteInt32BigEndian(f.AsSpan(0, 4), 8);
            BinaryPrimitives.WriteInt32BigEndian(f.AsSpan(4, 4), 0x00FFFFFF);
            Encoding.ASCII.GetBytes("junk").CopyTo(f, 8);
            return f;
        }

        /// <summary>A length prefix of -1: the reader cannot even tell how many bytes belong to the
        /// frame.</summary>
        private static byte[] IllegalLengthFrame()
        {
            byte[] f = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(f.AsSpan(0, 4), -1);
            return f;
        }

        public void Dispose()
        {
            _disposed = true;
            _server.Dispose();
        }
    }

    private static RemotingCommand HeartBeat()
    {
        return RemotingCommand.CreateRequestCommand(RequestCode.HeartBeat);
    }

    private static void WaitUntil(Func<bool> condition, int timeoutMillis = 8000)
    {
        Stopwatch sw = Stopwatch.StartNew();
        while (!condition() && sw.ElapsedMilliseconds < timeoutMillis)
        {
            Thread.Sleep(10);
        }
    }

    /// <summary>
    /// The transport reports these events through its own logger, so each leg points the log file
    /// at a private temp path, then reads the lines back. The level is part of the rendered line
    /// ("yyyy-MM-dd HH:mm:ss.fff ERROR [pid] [thread] [file:line] - message"), which is what lets a
    /// leg insist on ERROR rather than a mere WARN.
    /// </summary>
    private sealed class LogCapture : IDisposable
    {
        private readonly string _dir;
        private readonly string _path;
        private readonly LogLevel _levelBefore;

        public LogCapture(string tag)
        {
            _dir = Path.Combine(Path.GetTempPath(),
                "rmq_dotnet_contract_" + tag + "_" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(_dir);
            _path = Path.Combine(_dir, "transport.log");
            _levelBefore = ClientLog.LogLevel;
            ClientLog.SetLogFile(_path);
            ClientLog.SetLogLevel(LogLevel.Debug);
        }

        /// <summary>Lines carrying every one of the given fragments (case-sensitive substring).</summary>
        public List<string> LinesWith(params string[] needles)
        {
            ClientLog.FlushLogFile();
            var outLines = new List<string>();
            foreach (string line in ReadAllLinesShared(_path))
            {
                if (needles.All(line.Contains))
                {
                    outLines.Add(line);
                }
            }

            return outLines;
        }

        public void Dispose()
        {
            ClientLog.FlushLogFile();
            ClientLog.SetLogFile(string.Empty);
            ClientLog.FlushLogFile();
            ClientLog.SetLogLevel(_levelBefore);
            try
            {
                Directory.Delete(_dir, recursive: true);
            }
            catch
            {
                // A cleanup failure does not affect the test
            }
        }

        // ClientLog holds the write handle, and the file must be read shared for ReadWrite (the way
        // tail -f does it) or the open is judged to conflict with it.
        private static string[] ReadAllLinesShared(string path)
        {
            using FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using StreamReader sr = new(fs);
            var lines = new List<string>();
            string? line;
            while ((line = sr.ReadLine()) is not null)
            {
                lines.Add(line);
            }

            return lines.ToArray();
        }
    }

    // ------------------------------------------------------------------ T1

    [Fact]
    public void UndecodableFrameLogsErrorAndClosesThatConnection()
    {
        using var echo = new Peer(PeerMode.Echo);
        using var bad = new Peer(PeerMode.BadHeader);
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);

        // A second, healthy connection that must survive the whole leg.
        RemotingCommand probe = HeartBeat();
        Assert.Equal(ResponseCode.Success, client.InvokeSync(echo.Address, probe).Code);

        bool threw = false;
        List<string> errors;
        List<string> decodeLines;
        using (var log = new LogCapture("t1_decode"))
        {
            RemotingCommand req = HeartBeat();
            try
            {
                client.InvokeSync(bad.Address, req, 2000);
            }
            catch (Exception)
            {
                threw = true;
            }

            WaitUntil(() => bad.ClientCloses >= 1);
            // The level and the message are captured here, while this leg still owns the sink.
            errors = log.LinesWith(" ERROR [", "cannot decode frame");
            decodeLines = log.LinesWith("cannot decode frame");
        }

        Assert.True(threw, "the request that got the undecodable frame does not return a response");
        Assert.True(bad.ClientCloses >= 1, "the client takes the connection down, not just the frame");
        Assert.Equal(1, client.ConnectionCount); // only the broken connection left the table

        Assert.Single(errors);
        Assert.Contains(bad.Address, errors[0]);
        Assert.Contains("closing the connection", errors[0]);
        // Reported once, and at ERROR: a second, lower-level line would mean the same event is
        // counted twice by whatever reads the log.
        Assert.Single(decodeLines);

        // The point of closing: the same peer's next request is served on a fresh connection.
        RemotingCommand retry = HeartBeat();
        Assert.Equal(ResponseCode.Success, client.InvokeSync(bad.Address, retry, 3000).Code);
        Assert.Equal(2, bad.Connections);

        RemotingCommand probe2 = HeartBeat();
        Assert.Equal(ResponseCode.Success, client.InvokeSync(echo.Address, probe2).Code);
        Assert.Equal(0, echo.ClientCloses);
    }

    [Fact]
    public void IllegalFrameLengthTakesTheConnectionDown()
    {
        using var echo = new Peer(PeerMode.Echo);
        using var bad = new Peer(PeerMode.BadLength);
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);
        RemotingCommand probe = HeartBeat();
        client.InvokeSync(echo.Address, probe);

        bool threw = false;
        List<string> errors;
        List<string> decodeLines;
        using (var log = new LogCapture("t1_length"))
        {
            RemotingCommand req = HeartBeat();
            try
            {
                client.InvokeSync(bad.Address, req, 2000);
            }
            catch (Exception)
            {
                threw = true;
            }

            WaitUntil(() => bad.ClientCloses >= 1);
            errors = log.LinesWith(" ERROR [", "illegal frame length");
            decodeLines = log.LinesWith("cannot decode frame");
        }

        Assert.True(threw, "the request does not return a response");
        Assert.True(bad.ClientCloses >= 1, "an out-of-range length prefix closes the connection");
        Assert.Single(errors);
        Assert.Contains(bad.Address, errors[0]);
        // A broken length prefix is the same event one level earlier: the reader never got a frame
        // to decode, so it must not also be reported as a decode failure.
        Assert.Empty(decodeLines);

        RemotingCommand probe2 = HeartBeat();
        Assert.Equal(ResponseCode.Success, client.InvokeSync(echo.Address, probe2).Code);
    }

    // ------------------------------------------------------------------ T2

    [Fact]
    public void UnmatchedResponseWarnsAndKeepsTheConnection()
    {
        using var peer = new Peer(PeerMode.StrayFirst);
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);

        int code = -1;
        List<string> warns;
        using (var log = new LogCapture("t2"))
        {
            RemotingCommand req = HeartBeat();
            try
            {
                code = client.InvokeSync(peer.Address, req, 3000).Code;
            }
            catch (Exception)
            {
                code = -1;
            }

            Thread.Sleep(150);
            warns = log.LinesWith(" WARN ", "does not match any in-flight request");
        }

        Assert.Equal(ResponseCode.Success, code); // the real answer still reached the caller
        Assert.Single(warns);
        Assert.Contains(StrayOpaque.ToString(CultureInfo.InvariantCulture), warns[0]);
        Assert.Contains(peer.Address, warns[0]);
        Assert.Equal(0, peer.ClientCloses);
        Assert.Equal(1, client.ConnectionCount);

        // A follow-up on the same connection proves it is really usable, not merely unreaped.
        RemotingCommand again = HeartBeat();
        Assert.Equal(ResponseCode.Success, client.InvokeSync(peer.Address, again, 3000).Code);
        Assert.Equal(1, peer.Connections);
    }

    // ------------------------------------------------------------------ T3

    [Fact]
    public async Task WritableAnswersAboutTheSendBufferNotAboutHavingConnected()
    {
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);

        // An address with no connection is writable -- and answering must not create one.
        Assert.True(client.IsChannelWritable("127.0.0.1:59998"));
        Assert.Equal(0, client.ConnectionCount);

        using var drained = new Peer(PeerMode.Silent);
        RemotingCommand first = HeartBeat();
        client.InvokeOneway(drained.Address, first);
        Assert.True(client.IsChannelWritable(drained.Address));

        // The one unwritable answer: a peer that accepts and never reads, so the kernel buffers
        // fill and the client's own send parks inside SendAll. A writer thread holds that park
        // while the leg asks the question.
        using var sink = new Peer(PeerMode.Sink);
        using var sinkClient = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);
        RemotingCommand init = HeartBeat();
        sinkClient.InvokeOneway(sink.Address, init); // establishes the pooled socket

        var stopSender = false;
        bool sawFalse = false;
        var writer = Task.Run(() =>
        {
            RemotingCommand big = HeartBeat();
            big.Body = new byte[256 * 1024];
            for (int i = 0; i < 4096 && !Volatile.Read(ref stopSender); ++i)
            {
                try
                {
                    sinkClient.InvokeOneway(sink.Address, big);
                }
                catch (Exception)
                {
                    break; // the connection went away; the leg has what it needs
                }
            }
        });

        WaitUntil(() =>
        {
            if (sinkClient.IsChannelWritable(sink.Address))
            {
                return false;
            }

            sawFalse = true;
            return true;
        }, 8000);
        Assert.True(sawFalse, "the peer that never reads really filled the send buffer");
        Assert.False(sinkClient.IsChannelWritable(sink.Address));

        sink.StartDraining();
        bool drainedAgain = false;
        WaitUntil(() => drainedAgain = sinkClient.IsChannelWritable(sink.Address), 8000);
        Assert.True(drainedAgain, "once there is room again the same connection is writable");

        Volatile.Write(ref stopSender, true);
        // Closing the socket is what wakes a send parked in the kernel, so the writer can only be
        // joined after shutdown.
        sinkClient.Shutdown();
        Assert.True(await Task.WhenAny(writer, Task.Delay(TimeSpan.FromSeconds(10))) == writer,
            "the parked send returned once the socket closed");
    }

    [Fact]
    public void ClosedAddressIsWritableAgain()
    {
        using var peer = new Peer(PeerMode.Silent);
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);
        RemotingCommand req = HeartBeat();
        client.InvokeOneway(peer.Address, req);
        Assert.True(client.IsChannelWritable(peer.Address));

        RemotingCommand closeChannel = HeartBeat();
        client.CloseChannel(peer.Address);
        Assert.Equal(0, client.ConnectionCount);
        // "No connection" is not "cannot write": the next send simply connects.
        Assert.True(client.IsChannelWritable(peer.Address));
    }

    // ------------------------------------------------------------------ T4

    [Fact]
    public async Task IdleConnectionIsSweptWhileABusyOneSurvives()
    {
        using var quiet = new Peer(PeerMode.Silent);
        using var busy = new Peer(PeerMode.Silent);
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);
        client.ChannelIdleTimeoutMillis = 400;
        Assert.Equal(400, client.ChannelIdleTimeoutMillis);

        RemotingCommand q = HeartBeat();
        RemotingCommand b = HeartBeat();
        client.InvokeOneway(quiet.Address, q);
        client.InvokeOneway(busy.Address, b);

        var stopBeat = false;
        var beat = Task.Run(() =>
        {
            while (!Volatile.Read(ref stopBeat))
            {
                try
                {
                    RemotingCommand poke = HeartBeat();
                    client.InvokeOneway(busy.Address, poke);
                }
                catch (Exception)
                {
                    break;
                }

                Thread.Sleep(100); // well inside the 400 ms idle limit
            }
        });

        List<string> warns;
        using (var log = new LogCapture("t4"))
        {
            WaitUntil(() => quiet.ClientCloses >= 1, 5000);
            warns = log.LinesWith(" WARN ", "idle for", "closing it");
        }

        Assert.True(quiet.ClientCloses >= 1, "the connection that stopped carrying traffic was closed");
        Assert.Single(warns);
        Assert.Contains(quiet.Address, warns[0]);
        Volatile.Write(ref stopBeat, true);
        Assert.True(await Task.WhenAny(beat, Task.Delay(TimeSpan.FromSeconds(5))) == beat,
            "the writer loop stopped once the flag was raised");

        // Only the idle connection left the table; the one written to every 100 ms is still there.
        Assert.Equal(1, client.ConnectionCount);
        Assert.True(client.IsChannelWritable(busy.Address));
        Assert.Equal(0, busy.ClientCloses);
        Assert.Equal(0, client.SweepIdleConnections());
    }

    [Fact]
    public void IdleKnobCannotDisableTheSweepAndTheTransportDrivesIt()
    {
        using (var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis))
        {
            Assert.Equal(RemotingClient.DefaultChannelIdleTimeoutMillis, client.ChannelIdleTimeoutMillis);
            foreach (long bad in new long[] { 0, -1, -120 })
            {
                client.ChannelIdleTimeoutMillis = bad;
                // A non-positive value restores the default instead of disabling the sweep: the
                // sweep is always installed, so an unsettable deadline would mean "reap now".
                Assert.Equal(RemotingClient.DefaultChannelIdleTimeoutMillis, client.ChannelIdleTimeoutMillis);
            }

            client.ChannelIdleTimeoutMillis = 30000;
            Assert.Equal(30000, client.ChannelIdleTimeoutMillis);
        }

        // A quiet connection is reaped by the transport's own pass, with no caller-side tick.
        using var quiet = new Peer(PeerMode.Silent);
        using var auto = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);
        auto.ChannelIdleTimeoutMillis = 300;
        RemotingCommand req = HeartBeat();
        auto.InvokeOneway(quiet.Address, req);
        using (var log = new LogCapture("t4_auto"))
        {
            WaitUntil(() => quiet.ClientCloses >= 1, 5000);
            Assert.NotEmpty(log.LinesWith(" WARN ", "idle for", "closing it"));
        }
    }

    // ------------------------------------------------------------------ T5

    [Fact]
    public void ExpiredEntriesWarnOneEachAndKeepTheConnection()
    {
        using var peer = new Peer(PeerMode.Silent);
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);

        int timeouts = 0;
        int others = 0;
        List<string> warns;
        using (var log = new LogCapture("t5"))
        {
            var a = new ManualResetEventSlim(false);
            var b = new ManualResetEventSlim(false);
            RemotingCommand r1 = HeartBeat();
            RemotingCommand r2 = HeartBeat();
            client.InvokeAsync(peer.Address, r1, (_, error) =>
            {
                if (error is RemotingTimeoutException) Interlocked.Increment(ref timeouts);
                else if (error is null) Interlocked.Increment(ref others);
                a.Set();
            }, 200);
            client.InvokeAsync(peer.Address, r2, (_, error) =>
            {
                if (error is RemotingTimeoutException) Interlocked.Increment(ref timeouts);
                else if (error is null) Interlocked.Increment(ref others);
                b.Set();
            }, 200);

            // The sweep reclaims an entry one grace period after its deadline, so this waits on
            // time rather than on a connection event.
            WaitUntil(() => Volatile.Read(ref timeouts) == 2, 6000);
            Thread.Sleep(150);
            warns = log.LinesWith(" WARN ", "expired", "failed with a timeout");
            Assert.True(a.Wait(1000) && b.Wait(1000));
        }

        Assert.Equal(2, timeouts);
        Assert.Equal(0, others);
        Assert.Equal(2, warns.Count); // one line per reclaimed entry
        Assert.All(warns, l => Assert.Contains(peer.Address, l));
        Assert.Equal(0, client.PendingRequestCount);
        Assert.Equal(0, peer.ClientCloses); // reclaiming a request is not a connection event
    }

    // ------------------------------------------------------------------ T6

    private sealed class SyncTimeoutRun
    {
        public bool Threw;
        public bool TimedOut;
        public bool Alive = true;
        public int CloseWarns;
        public int TimeoutWarns;
    }

    private static SyncTimeoutRun RunSyncTimeout(bool closeOnTimeout, int timeoutMillis)
    {
        using var peer = new Peer(PeerMode.Silent);
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);
        client.CloseSocketOnSyncTimeout = closeOnTimeout;
        // Bring the connection up first, so "kept" and "closed" are statements about a socket that
        // really exists.
        RemotingCommand init = HeartBeat();
        client.InvokeOneway(peer.Address, init);
        Assert.Equal(closeOnTimeout, client.CloseSocketOnSyncTimeout);

        var run = new SyncTimeoutRun();
        List<string> closeWarns;
        List<string> timeoutWarns;
        using (var log = new LogCapture("t6"))
        {
            RemotingCommand req = HeartBeat();
            try
            {
                client.InvokeSync(peer.Address, req, timeoutMillis);
            }
            catch (RemotingTimeoutException)
            {
                run.Threw = true;
                run.TimedOut = true;
            }
            catch (Exception)
            {
                run.Threw = true;
            }

            Thread.Sleep(300); // let a close reach the peer before the counters are read
            closeWarns = log.LinesWith("closing the connection");
            timeoutWarns = log.LinesWith("wait response from", "timeout");
        }

        run.Alive = peer.ClientCloses == 0;
        run.CloseWarns = closeWarns.Count;
        run.TimeoutWarns = timeoutWarns.Count;
        return run;
    }

    [Fact]
    public void SyncTimeoutClosesTheConnectionByDefault()
    {
        SyncTimeoutRun on = RunSyncTimeout(true, 400);
        Assert.True(on.Threw && on.TimedOut, "the caller still gets the timeout it asked for");
        Assert.False(on.Alive, "by default the connection the wait ran out on is closed");
        Assert.Equal(1, on.CloseWarns);
        Assert.Equal(1, on.TimeoutWarns);

        SyncTimeoutRun off = RunSyncTimeout(false, 400);
        Assert.True(off.Threw && off.TimedOut, "the timeout is still reported to the caller");
        Assert.True(off.Alive, "with the knob off the socket is kept -- the timeout is caller-side");
        Assert.Equal(0, off.CloseWarns);
        Assert.Equal(1, off.TimeoutWarns);
    }

    /// <summary>
    /// The two floors of the close decision, on the only budget this port can hand the wait.
    ///
    /// This transport does not subtract the connect cost, so the time still available when the wait
    /// began is the whole timeout. The floors then leave exactly one shape that keeps the socket: a
    /// budget of zero, which no wait can ever be answered on.
    /// </summary>
    [Fact]
    public void SyncTimeoutCloseRuleHasOneFloorShape()
    {
        SyncTimeoutRun zero = RunSyncTimeout(true, 0);
        Assert.True(zero.Threw, "a zero budget still reports the timeout to the caller");
        Assert.True(zero.Alive, "a zero budget is at both floors, so the connection is kept");
        Assert.Equal(0, zero.CloseWarns);

        SyncTimeoutRun justAbove = RunSyncTimeout(true, 101);
        Assert.False(justAbove.Alive, "one millisecond above the floor is a close, not a keep");
        Assert.Equal(1, justAbove.CloseWarns);
    }

    // ------------------------------------------------------------------ channel listeners

    /// <summary>
    /// A channel listener is a "this connection is new" event, not a "this address is used"
    /// event: reusing a live connection must not fire it again, and a reconnect must. That
    /// distinction is what keeps a re-registration reaction to one heartbeat per reconnect
    /// instead of one per request.
    /// </summary>
    [Fact]
    public void ChannelListenerFiresOncePerNewConnectionAndAgainAfterAReconnect()
    {
        using var peer = new Peer(PeerMode.Echo);
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);
        var seen = new List<string>();
        client.RegisterChannelListener(addr =>
        {
            lock (seen)
            {
                seen.Add(addr);
            }
        });

        Assert.Equal(ResponseCode.Success, client.InvokeSync(peer.Address, HeartBeat()).Code);
        WaitUntil(() =>
        {
            lock (seen)
            {
                return seen.Count == 1;
            }
        });
        lock (seen)
        {
            Assert.Equal(new[] { peer.Address }, seen.ToArray());
        }

        // The same live connection: the second request reuses it and fires nothing.
        Assert.Equal(ResponseCode.Success, client.InvokeSync(peer.Address, HeartBeat()).Code);
        Assert.Equal(1, peer.Connections);
        Assert.Single(seen);

        // A closed connection forces a reconnect, and the listener fires for it.
        client.CloseChannel(peer.Address);
        Assert.Equal(ResponseCode.Success, client.InvokeSync(peer.Address, HeartBeat()).Code);
        WaitUntil(() =>
        {
            lock (seen)
            {
                return seen.Count == 2;
            }
        });
        lock (seen)
        {
            Assert.Equal(2, seen.Count);
        }

        Assert.Equal(2, peer.Connections);
    }

    /// <summary>A listener is an observer: its failure is logged and the invoke that opened the
    /// connection still succeeds.</summary>
    [Fact]
    public void AThrowingChannelListenerDoesNotBreakTheInvoke()
    {
        using var peer = new Peer(PeerMode.Echo);
        using var client = new RemotingClient(ConnectTimeoutMillis, InvokeTimeoutMillis);
        client.RegisterChannelListener(addr =>
            throw new InvalidOperationException("listener blew up for " + addr));

        using var log = new LogCapture("channel_listener");
        Assert.Equal(ResponseCode.Success, client.InvokeSync(peer.Address, HeartBeat()).Code);
        WaitUntil(() => log.LinesWith("channel listener", peer.Address).Count == 1);

        List<string> lines = log.LinesWith("channel listener", peer.Address);
        Assert.Single(lines);
        Assert.Contains("listener blew up", lines[0]);
    }
}
