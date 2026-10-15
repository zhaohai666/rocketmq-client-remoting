// Transport layer unit tests:
// real local TCP -- an echo broker for the invokeSync round trip, sticky/partial packet
// reassembly, a timeout throwing RemotingTimeoutException, a refused connection throwing
// RemotingConnectException, and the first request with opaque==0 not being rewritten.
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class TransportTests
{
    /// <summary>
    /// A local fake broker: accepts connections, reads a whole request frame by
    /// totalLength, and writes back a response encoding (code, opaque) as is. Supports
    /// handling frame by frame.
    /// </summary>
    private static Task<(Socket Server, IPEndPoint EndPoint, Func<Task> RunLoop)> StartEchoBrokerAsync()
    {
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        server.Listen(8);

        var endPoint = (IPEndPoint)server.LocalEndPoint!;

        async Task RunLoop()
        {
            while (true)
            {
                Socket client;
                try
                {
                    client = await server.AcceptAsync();
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                _ = Task.Run(async () =>
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
                                break;
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
                                RemotingCommand resp = RemotingCommand.CreateResponseCommand(
                                    ResponseCode.Success, "");
                                resp.Opaque = req.Opaque;
                                resp.Body = Encoding.UTF8.GetBytes("ack-" + req.Code);
                                byte[] wire = resp.Encode();
                                // simulating a partial packet: sent in two segments
                                await client.SendAsync(wire[..10]);
                                await Task.Delay(5);
                                await client.SendAsync(wire[10..]);
                            }

                            if (consumed > 0)
                            {
                                int rest = (int)buf.Length - consumed;
                                if (rest > 0)
                                {
                                    Buffer.BlockCopy(data, consumed, data, 0, rest);
                                }

                                buf.SetLength(rest);
                            }
                        }
                    }
                    catch
                    {
                        // exits as soon as the connection drops
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
                });
            }
        }

        Func<Task> loop = RunLoop;
        return Task.FromResult<(Socket Server, IPEndPoint EndPoint, Func<Task> RunLoop)>(
            (server, endPoint, loop));
    }

    private static RemotingCommand MakeRequest(int opaque = -1)
    {
        var hdr = new SendMessageRequestHeader
        {
            ProducerGroup = "pg",
            Topic = "T",
        };
        RemotingCommand cmd = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, hdr);
        if (opaque >= 0)
        {
            cmd.Opaque = opaque;
        }

        return cmd;
    }

    [Fact]
    public async Task InvokeSync_EchoRoundTrip()
    {
        (Socket server, IPEndPoint ep, Func<Task> loop) = await StartEchoBrokerAsync();
        var loopTask = Task.Run(loop);
        try
        {
            using var client = new RemotingClient();
            RemotingCommand resp = client.InvokeSync(ep.ToString(), MakeRequest(), 3000);
            Assert.Equal(ResponseCode.Success, resp.Code);
            Assert.Equal("ack-10", Encoding.UTF8.GetString(resp.Body)); // SendMessage=10
        }
        finally
        {
            server.Dispose();
            await Task.WhenAny(loopTask, Task.Delay(100));
        }
    }

    [Fact]
    public async Task InvokeSync_OpaqueZero_FirstRequestPreserved()
    {
        // the opaque counter starts at 0: the first request's opaque is 0 and the transport layer must not treat it as "unset" and rewrite it.
        // The fake broker echoes the opaque back, so the mere arrival of the response proves 0 was not rewritten.
        (Socket server, IPEndPoint ep, Func<Task> loop) = await StartEchoBrokerAsync();
        var loopTask = Task.Run(loop);
        try
        {
            using var client = new RemotingClient();
            var req = new RemotingCommand();
            req.Code = RequestCode.HeartBeat;
            req.Opaque = 0;
            RemotingCommand resp = client.InvokeSync(ep.ToString(), req, 3000);
            Assert.Equal(ResponseCode.Success, resp.Code);
            Assert.Equal(0, req.Opaque);
        }
        finally
        {
            server.Dispose();
            await Task.WhenAny(loopTask, Task.Delay(100));
        }
    }

    [Fact]
    public void InvokeSync_Timeout_Throws()
    {
        // a bare listener that never starts echoing: the connection is established but nothing ever comes back -> timeout
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        server.Listen(2);
        var ep = (IPEndPoint)server.LocalEndPoint!;
        try
        {
            using var client = new RemotingClient();
            Assert.ThrowsAny<RemotingTimeoutException>(
                () => client.InvokeSync(ep.ToString(), MakeRequest(), 300));
        }
        finally
        {
            server.Dispose();
        }
    }

    [Fact]
    public void InvokeSync_ConnectRefused_Throws()
    {
        // occupy a port and close it immediately: the connection is most likely refused
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var ep = (IPEndPoint)server.LocalEndPoint!;
        server.Dispose();
        using var client = new RemotingClient();
        Assert.ThrowsAny<RemotingException>(
            () => client.InvokeSync(ep.ToString(), MakeRequest(), 1000));
    }

    [Fact]
    public async Task InvokeOneway_DoesNotThrowOnEcho()
    {
        (Socket server, IPEndPoint ep, Func<Task> loop) = await StartEchoBrokerAsync();
        var loopTask = Task.Run(loop);
        try
        {
            using var client = new RemotingClient();
            var req = MakeRequest();
            req.MarkOnewayRpc();
            client.InvokeOneway(ep.ToString(), req);
            await Task.Delay(100); // give it time to send
        }
        finally
        {
            server.Dispose();
            await Task.WhenAny(loopTask, Task.Delay(100));
        }
    }

    [Fact]
    public async Task InvokeAsync_EchoRoundTrip_FiresOnceWithResponse()
    {
        (Socket server, IPEndPoint ep, Func<Task> loop) = await StartEchoBrokerAsync();
        var loopTask = Task.Run(loop);
        try
        {
            using var client = new RemotingClient();
            RemotingCommand req = MakeRequest();
            int fired = 0;
            RemotingCommand? seen = null;
            Exception? err = null;
            client.InvokeAsync(ep.ToString(), req, (response, error) =>
            {
                Interlocked.Increment(ref fired);
                seen = response;
                err = error;
            });

            for (int i = 0; i < 100 && Volatile.Read(ref fired) == 0; i++)
            {
                await Task.Delay(20);
            }

            Assert.Equal(1, fired);
            Assert.Null(err);
            Assert.NotNull(seen);
            Assert.Equal(ResponseCode.Success, seen!.Code);
            // the response must echo back the same opaque, otherwise the in-flight table is linked wrong
            Assert.Equal(req.Opaque, seen.Opaque);
        }
        finally
        {
            server.Dispose();
            await Task.WhenAny(loopTask, Task.Delay(100));
        }
    }

    [Fact]
    public async Task InvokeAsync_SilentServer_TimeoutFiresOnceWithError()
    {
        // accept only and never reply: timeoutMillis must take effect, and the callback must still fire **exactly once** with a timeout exception.
        // Before the fix that argument was simply dropped and both the in-flight entry and the callback hung forever.
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        server.Listen(2);
        var ep = (IPEndPoint)server.LocalEndPoint!;
        Task acceptTask = Task.Run(async () =>
        {
            try
            {
                Socket conn = await server.AcceptAsync();
                await Task.Delay(5000); // it takes it and never replies
                conn.Dispose();
            }
            catch
            {
                // the server side is released
            }
        });
        try
        {
            using var client = new RemotingClient();
            int fired = 0;
            Exception? err = null;
            client.InvokeAsync(ep.ToString(), MakeRequest(), (response, error) =>
            {
                Interlocked.Increment(ref fired);
                err = error;
            }, 300);

            for (int i = 0; i < 300 && Volatile.Read(ref fired) == 0; i++)
            {
                await Task.Delay(20);
            }

            Assert.Equal(1, fired);
            Assert.IsAssignableFrom<RemotingTimeoutException>(err);
            await Task.Delay(300);
            Assert.Equal(1, fired); // it is not delivered twice
        }
        finally
        {
            server.Dispose();
            await Task.WhenAny(acceptTask, Task.Delay(100));
        }
    }
}
