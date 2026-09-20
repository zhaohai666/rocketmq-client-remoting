// GO_AWAY(1500) 的传输层处理（镜像 Rust / C++ / Python 的 go_away 用例）。
//
// 对应 Java NettyRemotingClient#invokeImpl:828-873：broker / proxy 优雅下线时会给
// 在途请求回 GO_AWAY，语义是"这条连接别再用了"。客户端必须换连接重发一次，且**只**
// 重发一次；第二次还是 GO_AWAY 就报发送失败。enableReconnectForGoAway 关掉时不重连。
//
// 修之前这条码被当成普通业务响应交给上层：同步调用拿到 GO_AWAY 就当"发送成功"，
// 消息其实没落到 broker。
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class GoAwayTests
{
    /// <summary>前 goAwayConns 条连接一律回 GO_AWAY，之后的连接回 SUCCESS。</summary>
    private sealed class GoAwayBroker : IDisposable
    {
        private readonly Socket _server;
        private readonly int _goAwayConns;
        private int _connections;

        public ConcurrentQueue<int> Opaques { get; } = new();

        public GoAwayBroker(int goAwayConns)
        {
            _goAwayConns = goAwayConns;
            _server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _server.Listen(8);
            EndPoint = (IPEndPoint)_server.LocalEndPoint!;
            _ = Task.Run(AcceptLoop);
        }

        public IPEndPoint EndPoint { get; }


        public int Connections => Volatile.Read(ref _connections);

        private async Task AcceptLoop()
        {
            while (true)
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
                _ = Task.Run(() => Serve(client, index < _goAwayConns));
            }
        }

        private async Task Serve(Socket client, bool goAway)
        {
            var buf = new MemoryStream();
            byte[] chunk = new byte[4096];
            try
            {
                while (true)
                {
                    int n = await client.ReceiveAsync(chunk);
                    if (n <= 0)
                    {
                        return;
                    }

                    buf.Write(chunk, 0, n);
                    byte[] data = buf.GetBuffer();
                    int consumed = 0;
                    while (buf.Length - consumed >= 4)
                    {
                        int totalLen = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(consumed, 4));
                        if (buf.Length - consumed < 4 + totalLen)
                        {
                            break;
                        }

                        byte[] frame = new byte[4 + totalLen];
                        Buffer.BlockCopy(data, consumed, frame, 0, 4 + totalLen);
                        consumed += 4 + totalLen;

                        RemotingCommand req = RemotingCommand.Decode(frame);
                        Opaques.Enqueue(req.Opaque);
                        RemotingCommand resp = RemotingCommand.CreateResponseCommand(
                            goAway ? ResponseCode.GoAway : ResponseCode.Success, "");
                        resp.Opaque = req.Opaque;
                        await client.SendAsync(resp.Encode());
                    }

                    buf.SetLength(buf.Length - consumed);
                }
            }
            catch
            {
                // 客户端重连时会先关掉旧连接，这里静默退出即可
            }
            finally
            {
                try
                {
                    client.Dispose();
                }
                catch
                {
                    // 忽略
                }
            }
        }

        public void Dispose() => _server.Dispose();
    }

    private static RemotingCommand MakeRequest() =>
        RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, new SendMessageRequestHeader
        {
            ProducerGroup = "pg",
            Topic = "T",
        });

    [Fact]
    public void GoAway_ReconnectsAndRetriesOnce()
    {
        using var broker = new GoAwayBroker(1);
        using var client = new RemotingClient();
        RemotingCommand req = MakeRequest();
        int firstOpaque = req.Opaque;

        RemotingCommand resp = client.InvokeSync(broker.EndPoint.ToString()!, req, 5000);

        Assert.Equal(ResponseCode.Success, resp.Code);
        Assert.Equal(2, broker.Connections);
        int[] seen = broker.Opaques.ToArray();
        Assert.Equal(2, seen.Length);
        Assert.Equal(firstOpaque, seen[0]);
        Assert.True(seen[1] != firstOpaque, "重发必须用新 opaque，否则响应会错配");
    }

    [Fact]
    public void GoAway_TwiceFailsInsteadOfLooping()
    {
        using var broker = new GoAwayBroker(int.MaxValue);
        using var client = new RemotingClient();

        RemotingSendRequestException err = Assert.Throws<RemotingSendRequestException>(
            () => client.InvokeSync(broker.EndPoint.ToString()!, MakeRequest(), 5000));

        // 文案对齐 Java：RemotingSendRequestException("Receive GO_AWAY twice ...")
        Assert.Contains("GO_AWAY twice", err.Message);
        Assert.Equal(2, broker.Connections);
    }

    [Fact]
    public void GoAway_WithoutReconnectFlagSurfacesError()
    {
        using var broker = new GoAwayBroker(int.MaxValue);
        using var client = new RemotingClient { EnableReconnectForGoAway = false };

        RemotingSendRequestException err = Assert.Throws<RemotingSendRequestException>(
            () => client.InvokeSync(broker.EndPoint.ToString()!, MakeRequest(), 5000));

        Assert.Contains("Receive GO_AWAY from channel", err.Message);
        Assert.Equal(1, broker.Connections);
    }

    [Fact]
    public async Task GoAway_AsyncPathRetriesToo()
    {
        using var broker = new GoAwayBroker(1);
        using var client = new RemotingClient();
        var done = new TaskCompletionSource<(RemotingCommand? Resp, Exception? Err)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        client.InvokeAsync(broker.EndPoint.ToString()!, MakeRequest(),
            (resp, err) => done.TrySetResult((resp, err)));

        await Task.WhenAny(done.Task, Task.Delay(8000));
        Assert.True(done.Task.IsCompleted, "async GO_AWAY 重发后回调必须触发");
        (RemotingCommand? Resp, Exception? Err) result = await done.Task;
        Assert.Null(result.Err);
        Assert.Equal(ResponseCode.Success, result.Resp!.Code);
        Assert.Equal(2, broker.Connections);
    }

    [Fact]
    public async Task GoAway_AsyncTwiceReportsError()
    {
        using var broker = new GoAwayBroker(int.MaxValue);
        using var client = new RemotingClient();
        var done = new TaskCompletionSource<(RemotingCommand? Resp, Exception? Err)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        client.InvokeAsync(broker.EndPoint.ToString()!, MakeRequest(),
            (resp, err) => done.TrySetResult((resp, err)));

        await Task.WhenAny(done.Task, Task.Delay(8000));
        Assert.True(done.Task.IsCompleted, "async GO_AWAY 两次后回调必须触发");
        (RemotingCommand? Resp, Exception? Err) result = await done.Task;
        Assert.Null(result.Resp);
        Assert.Contains("GO_AWAY twice", result.Err!.Message);
        Assert.Equal(2, broker.Connections);
    }
}
