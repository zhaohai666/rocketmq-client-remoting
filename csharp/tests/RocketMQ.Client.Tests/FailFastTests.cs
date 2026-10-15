// Real sockets, not MockCluster: what is being tested is precisely "what the reader thread
// does after it sees EOF".
using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class FailFastTests
{
    /// <summary>
    /// The timeout is deliberately set an order of magnitude longer than the test itself:
    /// any implementation that only "fails once the timeout is reached" gets caught by the
    /// duration bound in the assertion, while the correct failFast path is milliseconds.
    /// </summary>
    private const int LongTimeoutMillis = 30000;

    /// <summary>
    /// A local fake peer: reads a whole request frame by totalLength, then decides what to
    /// do based on the connection sequence number.
    /// The first <c>dropConns</c> connections close right after reading a frame (creating
    /// EOF, and **never replying**); later connections reply SUCCESS, and when
    /// <c>gated</c> they wait for <see cref="Release"/> before replying -- used to stop a
    /// request precisely in the "already written, still in flight" state.
    /// </summary>
    private sealed class Peer : IDisposable
    {
        private readonly Socket _server;
        private readonly int _dropConns;
        private readonly bool _holdForever;
        private readonly ManualResetEventSlim _gate = new(true);
        private int _connections;
        private int _frames;
        private volatile bool _disposed;

        public Peer(int dropConns = 1, bool holdForever = false, bool gatedReplies = false)
        {
            _dropConns = dropConns;
            _holdForever = holdForever;
            if (gatedReplies)
            {
                _gate.Reset();
            }

            _server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _server.Listen(8);
            Address = ((IPEndPoint)_server.LocalEndPoint!).ToString();
            _ = Task.Run(AcceptLoop);
        }

        public string Address { get; }

        /// <summary>The number of request frames fully received: the test uses it to wait
        /// for "the peer really received it".</summary>
        public int Frames => Volatile.Read(ref _frames);

        public int Connections => Volatile.Read(ref _connections);

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

                int index = Interlocked.Increment(ref _connections) - 1;
                _ = Task.Run(() => Serve(client, index));
            }
        }

        /// <summary>Releases the replies held at the gate: the request is by now in the
        /// client's in-flight table.</summary>
        public void Release() => _gate.Set();

        private async Task Serve(Socket client, int connIndex)
        {
            bool drop = !_holdForever && connIndex < _dropConns;
            try
            {
                while (true)
                {
                    byte[] lenBuf = new byte[4];
                    if (!await ReceiveExactAsync(client, lenBuf))
                    {
                        return;
                    }

                    int totalLen = BinaryPrimitives.ReadInt32BigEndian(lenBuf);
                    byte[] frame = new byte[4 + totalLen];
                    Buffer.BlockCopy(lenBuf, 0, frame, 0, 4);
                    if (!await ReceiveExactAsync(client, frame.AsMemory(4)) || _disposed)
                    {
                        return;
                    }

                    Interlocked.Increment(ref _frames);

                    if (drop)
                    {
                        // Closes right after reading, never replies: the reader thread can only conclude from EOF that this request will never get a heartbeat.
                        return;
                    }

                    if (_holdForever)
                    {
                        // Neither replies nor closes: this request can only be finalised by the caller itself (timeout cleanup or Shutdown).
                        while (!_disposed)
                        {
                            await Task.Delay(20);
                        }

                        return;
                    }

                    RemotingCommand req = RemotingCommand.Decode(frame);
                    RemotingCommand resp = RemotingCommand.CreateResponseCommand(ResponseCode.Success, null);
                    resp.Opaque = req.Opaque;
                    _gate.Wait();
                    byte[] wire = resp.Encode();
                    await client.SendAsync(wire);
                }
            }
            catch
            {
                // The peer disconnected first, which is exactly the shape being tested
            }
            finally
            {
                Close(client);
            }
        }

        private static void Close(Socket client)
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

        private static async Task<bool> ReceiveExactAsync(Socket client, Memory<byte> buffer)
        {
            int off = 0;
            while (off < buffer.Length)
            {
                int n = await client.ReceiveAsync(buffer[off..]);
                if (n <= 0)
                {
                    return false;
                }

                off += n;
            }

            return true;
        }

        public void Dispose()
        {
            _disposed = true;
            _server.Dispose();
        }
    }

    private static RemotingCommand MakeRequest()
    {
        return RemotingCommand.CreateRequestCommand(RequestCode.HeartBeat);
    }

    private static void WaitUntil(Func<bool> condition, int timeoutMillis = 5000)
    {
        var sw = Stopwatch.StartNew();
        while (!condition() && sw.ElapsedMilliseconds < timeoutMillis)
        {
            Thread.Sleep(10);
        }
    }

    /// <summary>A synchronous call: the connection drops before the response, so it must
    /// **immediately** throw a send failure instead of waiting out the 30s timeout.</summary>
    [Fact]
    public void InvokeSync_FailsFast_InsteadOfWaitingForTimeout()
    {
        using var peer = new Peer();
        using var client = new RemotingClient();

        var sw = Stopwatch.StartNew();
        Exception? raw = Record.Exception(() => client.InvokeSync(peer.Address, MakeRequest(), LongTimeoutMillis));
        sw.Stop();

        Assert.NotNull(raw);
        Assert.IsType<RemotingSendRequestException>(raw);
        Assert.DoesNotContain("timeout", raw!.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("connection closed", raw.Message);
        Assert.True(sw.ElapsedMilliseconds < 5000,
            "failFast must not wait for the invoke timeout, took " + sw.ElapsedMilliseconds + "ms");
    }

    /// <summary>An async callback: likewise gets the send failure immediately, not a
    /// timeout.</summary>
    [Fact]
    public void InvokeAsync_ReportsSendFailure_Immediately()
    {
        using var peer = new Peer();
        using var client = new RemotingClient();

        int fired = 0;
        Exception? error = null;
        var sw = Stopwatch.StartNew();
        client.InvokeAsync(peer.Address, MakeRequest(), (_, err) =>
        {
            Volatile.Write(ref error, err);
            Interlocked.Increment(ref fired);
        }, LongTimeoutMillis);

        WaitUntil(() => Volatile.Read(ref fired) > 0);
        sw.Stop();

        Assert.Equal(1, Volatile.Read(ref fired));
        Assert.IsType<RemotingSendRequestException>(error);
        Assert.Contains("connection closed", error!.Message);
        Assert.True(sw.ElapsedMilliseconds < 5000,
            "failFast callback must not wait for the sweep, took " + sw.ElapsedMilliseconds + "ms");
    }

    /// <summary>
    /// failFast and the cleanup thread race for the same opaque: the callback may only be
    /// delivered **once**. The timeout is deliberately set to 200ms so that the two paths
    /// really collide.
    /// </summary>
    [Fact]
    public void FailFast_CallbackFires_ExactlyOnce()
    {
        using var peer = new Peer();
        using var client = new RemotingClient();

        int fired = 0;
        client.InvokeAsync(peer.Address, MakeRequest(), (_, _) => Interlocked.Increment(ref fired), 200);

        // Even after the timeout plus 1s of grace, there is still exactly one delivery.
        Thread.Sleep(2500);
        Assert.Equal(1, Volatile.Read(ref fired));
    }

    /// <summary>
    /// Killing one connection must not drag down another party's in-flight requests: a
    /// not-yet-answered request on the other connection must keep waiting and eventually
    /// receive its own success response.
    /// </summary>
    [Fact]
    public void FailFast_DoesNotTouch_OtherConnections()
    {
        // Address A: takes the request but holds the reply; address B: closes right after reading. The two connections are independent.
        using var alive = new Peer(dropConns: 0, gatedReplies: true);
        using var doomed = new Peer();
        using var client = new RemotingClient();

        int aliveFired = 0;
        Exception? aliveError = null;
        RemotingCommand? aliveResponse = null;
        client.InvokeAsync(alive.Address, MakeRequest(), (resp, err) =>
        {
            Volatile.Write(ref aliveError, err);
            Volatile.Write(ref aliveResponse, resp);
            Interlocked.Increment(ref aliveFired);
        }, LongTimeoutMillis);
        WaitForFrame(alive, 1);

        int doomedFired = 0;
        client.InvokeAsync(doomed.Address, MakeRequest(), (_, _) => Interlocked.Increment(ref doomedFired),
            LongTimeoutMillis);
        WaitUntil(() => Volatile.Read(ref doomedFired) > 0);
        Assert.Equal(1, Volatile.Read(ref doomedFired));

        // At the moment the dead connection is finalised, the request on the live connection must still be sitting in the in-flight table.
        Assert.Equal(0, Volatile.Read(ref aliveFired));
        Assert.Null(aliveError);

        alive.Release();
        WaitUntil(() => Volatile.Read(ref aliveFired) > 0);
        Assert.Equal(1, Volatile.Read(ref aliveFired));
        Assert.Null(aliveError);
        Assert.NotNull(aliveResponse);
        Assert.Equal(ResponseCode.Success, aliveResponse!.Code);
    }

    /// <summary>
    /// After being killed, the **same address** can still open a new connection and finish
    /// a new request: the connection table must already have the dead one removed, and the
    /// old reader thread's cleanup must not hurt the new connection's in-flight requests
    /// (which is the point of claiming by Connection object reference).
    /// </summary>
    [Fact]
    public void AfterFailFast_SameAddress_KeepsWorking()
    {
        // The first connection closes right after reading, the second onwards replies normally -- both requests hit the same address.
        using var peer = new Peer(dropConns: 1);
        using var client = new RemotingClient();

        int firstFired = 0;
        client.InvokeAsync(peer.Address, MakeRequest(), (_, _) => Interlocked.Increment(ref firstFired),
            LongTimeoutMillis);
        WaitUntil(() => Volatile.Read(ref firstFired) > 0);
        Assert.Equal(1, Volatile.Read(ref firstFired));

        int secondFired = 0;
        Exception? secondError = null;
        RemotingCommand? secondResponse = null;
        client.InvokeAsync(peer.Address, MakeRequest(), (resp, err) =>
        {
            Volatile.Write(ref secondError, err);
            Volatile.Write(ref secondResponse, resp);
            Interlocked.Increment(ref secondFired);
        }, LongTimeoutMillis);

        WaitUntil(() => Volatile.Read(ref secondFired) > 0);
        Assert.Equal(1, Volatile.Read(ref secondFired));
        Assert.Null(secondError);
        Assert.NotNull(secondResponse);
        Assert.Equal(ResponseCode.Success, secondResponse!.Code);
        Assert.Equal(2, peer.Connections);
    }

    /// <summary>
    /// Shutdown finalises the in-flight requests: the callbacks are delivered before the
    /// reader thread exits, so the caller is not left with a callback that never settles.
    /// </summary>
    [Fact]
    public void Shutdown_Drains_InFlightRequests()
    {
        using var peer = new Peer(holdForever: true);
        var client = new RemotingClient();

        int fired = 0;
        Exception? error = null;
        client.InvokeAsync(peer.Address, MakeRequest(), (_, err) =>
        {
            Volatile.Write(ref error, err);
            Interlocked.Increment(ref fired);
        }, LongTimeoutMillis);
        WaitForFrame(peer, 1);
        Assert.Equal(0, Volatile.Read(ref fired));

        var sw = Stopwatch.StartNew();
        client.Shutdown();
        sw.Stop();

        Assert.Equal(1, Volatile.Read(ref fired));
        Assert.IsType<RemotingSendRequestException>(error);
        Assert.True(sw.ElapsedMilliseconds < 5000,
            "shutdown must settle in-flight callbacks, took " + sw.ElapsedMilliseconds + "ms");

        // Shutdown can be called repeatedly, it does not deliver a second time.
        client.Shutdown();
        Assert.Equal(1, Volatile.Read(ref fired));
        client.Dispose();
    }

    private static void WaitForFrame(Peer peer, int expected)
    {
        WaitUntil(() => peer.Frames >= expected);
        Assert.True(peer.Frames >= expected, "peer never received request " + expected);
    }
}
