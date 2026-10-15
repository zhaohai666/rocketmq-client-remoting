// Transport layer handling of GO_AWAY(1500).
//
//
// Before the fix this code was handed up as an ordinary business response: a synchronous
// call that got GO_AWAY treated it as "the send succeeded", while the message had in fact
// never reached the broker.
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
    /// <summary>The first goAwayConns connections always reply GO_AWAY, the ones after
    /// them reply SUCCESS.</summary>
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
                // the client closes the old connection when it reconnects, exiting silently here is enough
            }
            finally
            {
                try
                {
                    client.Dispose();
                }
                catch
                {
                    // ignored
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
        Assert.True(seen[1] != firstOpaque, "a resend must use a new opaque, otherwise the responses get mismatched");
    }

    [Fact]
    public void GoAway_TwiceFailsInsteadOfLooping()
    {
        using var broker = new GoAwayBroker(int.MaxValue);
        using var client = new RemotingClient();

        RemotingSendRequestException err = Assert.Throws<RemotingSendRequestException>(
            () => client.InvokeSync(broker.EndPoint.ToString()!, MakeRequest(), 5000));

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
        Assert.True(done.Task.IsCompleted, "after an async GO_AWAY resend the callback must fire");
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
        Assert.True(done.Task.IsCompleted, "after two async GO_AWAYs the callback must fire");
        (RemotingCommand? Resp, Exception? Err) result = await done.Task;
        Assert.Null(result.Resp);
        Assert.Contains("GO_AWAY twice", result.Err!.Message);
        Assert.Equal(2, broker.Connections);
    }
}
