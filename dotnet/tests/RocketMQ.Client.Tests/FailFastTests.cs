// 对端断开时立刻判死在途请求（Java NettyRemotingHandler#close -> NettyRemotingAbstract#failFast
// -> requestFail），镜像 Python tests/test_fail_fast.py 与 C++ test_fail_fast.cpp。
//
// 缺这一步会错两件事，都不只是"慢一点"：
//   - 时机：调用方要等满 invoke 超时（或清理线程的 timeout+1s 宽限）才发现对端早就断了，
//     broker 重启 / 主备切换 / 网络抖动时这段空等直接压在发送链路上。
//   - 语义：报成 RemotingTimeoutException，Java 报 RemotingSendRequestException。
//     异步发送的重试分类按异常**类型**分流，错类型等于错决策。
//
// 用真 socket，不用 MockCluster：要被测的就是"读线程看见 EOF 之后做了什么"。
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
    /// 故意把超时设得比测试本身长一个量级：任何"等到超时才算失败"的实现都会被
    /// 断言里的耗时上限抓住，而正确的 failFast 路径是毫秒级。
    /// </summary>
    private const int LongTimeoutMillis = 30000;

    /// <summary>
    /// 本机假对端：按 totalLength 收完整请求帧，然后按连接序号决定处置方式。
    /// 前 <c>dropConns</c> 条连接读完一帧就关掉（制造 EOF，且**永不回复**）；之后的连接
    /// 回 SUCCESS，<c>gated</c> 时等到 <see cref="Release"/> 才回——用于把一条请求
    /// 精确地停在"已写出、尚在途"的状态。
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

        /// <summary>已完整收到的请求帧数：测试用它等"对端确实收到了"。</summary>
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

        /// <summary>放行 gate 上攒着的回复：请求此刻已经在客户端在途表里。</summary>
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
                        // 读完就断，绝不回复：读线程只能靠 EOF 判定这条请求永远不会有心跳。
                        return;
                    }

                    if (_holdForever)
                    {
                        // 不回复也不关：这条请求只能等调用方自己收口（超时清理或 Shutdown）。
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
                // 对端先断了，正是要测的形状
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
                // 已经断了
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

    /// <summary>同步调用：连接在响应之前断开，必须**立刻**抛发送失败，而不是等满 30s 超时。</summary>
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
        // 等满超时要 30s；毫秒级判死才是 Java failFast 的口径。
        Assert.True(sw.ElapsedMilliseconds < 5000,
            "failFast must not wait for the invoke timeout, took " + sw.ElapsedMilliseconds + "ms");
    }

    /// <summary>异步回调：同样立刻拿到发送失败，而不是超时。</summary>
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
    /// failFast 与清理线程抢同一条 opaque：回调只能投递**一次**。
    /// 这里刻意把超时设成 200ms，让两条路径真的撞上。
    /// </summary>
    [Fact]
    public void FailFast_CallbackFires_ExactlyOnce()
    {
        using var peer = new Peer();
        using var client = new RemotingClient();

        int fired = 0;
        client.InvokeAsync(peer.Address, MakeRequest(), (_, _) => Interlocked.Increment(ref fired), 200);

        // 超时 + 1s 宽限都过去之后，仍然只有一次投递。
        Thread.Sleep(2500);
        Assert.Equal(1, Volatile.Read(ref fired));
    }

    /// <summary>
    /// 判死一条连接不能牵连别人家的在途请求：另一条连接上尚未应答的请求必须继续等，
    /// 并且最终拿到它自己的成功响应。
    /// </summary>
    [Fact]
    public void FailFast_DoesNotTouch_OtherConnections()
    {
        // 地址 A：收下请求但按住不回复；地址 B：读完就关。两条连接互不相干。
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

        // 死连接收口的那一刻，活连接上那条请求必须还好好待在途表里。
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
    /// 判死之后**同一个地址**还能建新连接、跑完新请求：连接表必须已经把死连接摘干净，
    /// 旧读线程的收尾也不能误伤新连接上的在途请求（按 Connection 对象引用认领的意义）。
    /// </summary>
    [Fact]
    public void AfterFailFast_SameAddress_KeepsWorking()
    {
        // 第一条连接读完就关，第二条起正常回复 —— 两次请求打到同一个地址。
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
    /// Shutdown 时把在途请求收口：读线程退出前投递回调，调用方不能留下永不结算的回调。
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

        // Shutdown 可重复调用，不会补投第二次。
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
