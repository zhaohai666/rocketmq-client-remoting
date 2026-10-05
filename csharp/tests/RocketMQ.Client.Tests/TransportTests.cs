// 传输层单测（镜像 C++ test_transport.cpp 的核心断言）：
// 真实本机 TCP —— echo broker 测 invokeSync 往返、粘包/半包重组、
// 超时抛 RemotingTimeoutException、连接拒绝抛 RemotingConnectException、
// opaque==0 的首个请求不被改写。
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
    /// 本机假 broker：接受连接，按 totalLength 收完整请求帧，
    /// 把 (code, opaque) 原样编码为响应回写。支持逐帧处理。
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
                                // 半包模拟：分两段发
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
                        // 连接断开即退出
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
        // opaque 计数器从 0 起算：第一个请求 opaque 就是 0，传输层不能当"未设置"改写。
        // 假 broker 按 opaque 回填响应，因此只要响应到达就证明 0 没被改。
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
        // 不启动 echo 的裸 listener：连接建立但永不回包 -> 超时
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
        // 占一个端口后立即关闭：连接大概率被拒
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
            await Task.Delay(100); // 给发送时间
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
            // 响应必须回填同一个 opaque，否则说明在途表关联错了
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
        // 只 accept、永不回包：timeoutMillis 必须生效，回调仍要**恰好一次**带上超时异常。
        // 修复前该参数被直接丢弃，在途条目和回调都会永久悬挂。
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        server.Listen(2);
        var ep = (IPEndPoint)server.LocalEndPoint!;
        Task acceptTask = Task.Run(async () =>
        {
            try
            {
                Socket conn = await server.AcceptAsync();
                await Task.Delay(5000); // 收下就不回
                conn.Dispose();
            }
            catch
            {
                // 服务端已释放
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

            // 清理线程与 Java scanResponseTable 同式：deadline + 1s 宽限才判超时
            for (int i = 0; i < 300 && Volatile.Read(ref fired) == 0; i++)
            {
                await Task.Delay(20);
            }

            Assert.Equal(1, fired);
            Assert.IsAssignableFrom<RemotingTimeoutException>(err);
            await Task.Delay(300);
            Assert.Equal(1, fired); // 不会被重复投递
        }
        finally
        {
            server.Dispose();
            await Task.WhenAny(acceptTask, Task.Delay(100));
        }
    }
}
