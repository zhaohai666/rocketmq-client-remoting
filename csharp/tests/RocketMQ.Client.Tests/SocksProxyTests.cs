// SOCKS 代理传输层单测：本地手写假 SOCKS5 / SOCKS4 服务器（最小握手 + CONNECT +
// 双向转发到本机 echo broker），断言 RemotingClient 配上 SocksProxyConfig 后经代理
// 完成一次完整 invokeSync；再覆盖用户名密码子协商的成功与拒绝、方法拒绝、CONNECT
// 被拒、死代理四条失败路径。
//
// 本机 loopback 怪癖：刚关闭的端口约 2s 内还可能"连上"再被拒——所以「连接失败类」
// 断言一律给 ≥30s 的超时预算（connect 30s + invoke 60s），而不是用默认 3s。
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class SocksProxyTests
{
    // ---------------------------------------------------------------- 测试基础设施

    /// <summary>通用 TCP 测试服务器：Bind(0) 起监听，每条连接交给 handler 异步处理。</summary>
    private sealed class TcpTestServer : IDisposable
    {
        private readonly Socket _listener;
        private readonly Func<Socket, Task> _handler;

        public TcpTestServer(Func<Socket, Task> handler)
        {
            _handler = handler;
            _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _listener.Listen(16);
        }

        public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndPoint!;

        public string Addr => EndPoint.ToString();

        public async Task Loop()
        {
            while (true)
            {
                Socket client;
                try
                {
                    client = await _listener.AcceptAsync();
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                _ = Task.Run(() => _handler(client));
            }
        }

        public void Dispose()
        {
            _listener.Dispose();
        }
    }

    /// <summary>echo broker：按 totalLength 收完整请求帧，把 code 原样编码为响应回写。</summary>
    private static async Task HandleEcho(Socket client)
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
                    if (totalLen <= 0 || totalLen > RemotingClient.MaxFrameLength)
                    {
                        break;
                    }

                    if (buf.Length - consumed < 4 + totalLen)
                    {
                        break;
                    }

                    byte[] frame = new byte[4 + totalLen];
                    Buffer.BlockCopy(data, consumed, frame, 0, 4 + totalLen);
                    consumed += 4 + totalLen;

                    RemotingCommand req = RemotingCommand.Decode(frame);
                    RemotingCommand resp = RemotingCommand.CreateResponseCommand(ResponseCode.Success, "");
                    resp.Opaque = req.Opaque;
                    resp.Body = Encoding.UTF8.GetBytes("ack-" + req.Code);
                    await client.SendAsync(resp.Encode());
                }

                if (consumed > 0)
                {
                    int rest = (int)buf.Length - consumed;
                    Buffer.BlockCopy(data, consumed, data, 0, rest);
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
            try { client.Dispose(); } catch { /* 忽略 */ }
        }
    }

    /// <summary>
    /// 假 SOCKS5 服务器：最小握手（方法选择 + 可选 RFC 1929 用户名密码子协商）+
    /// CONNECT 到真实目标 + 双向转发。Username 非空 = 要求用户名密码认证。
    /// </summary>
    private sealed class FakeSocks5Server : IDisposable
    {
        private readonly TcpTestServer _server;

        public string? Username;
        public string? Password;
        public bool RejectConnect;       // CONNECT 回 REP=1（一般性失败）
        public int AuthRejections;       // 用户名密码被拒次数
        public int ConnectCount;         // 成功 CONNECT 次数

        public FakeSocks5Server()
        {
            _server = new TcpTestServer(HandleClient);
        }

        public string Addr => _server.Addr;
        public Task Loop() => _server.Loop();

        private async Task HandleClient(Socket client)
        {
            try
            {
                byte[] head = await ReadExactAsync(client, 2);
                if (head[0] != 5)
                {
                    return; // 不是 SOCKS5，直接断
                }

                byte[] methods = await ReadExactAsync(client, head[1]);
                if (Username is not null)
                {
                    // 要求用户名密码认证：客户端必须提供 0x02 方法
                    if (Array.IndexOf(methods, (byte)0x02) < 0)
                    {
                        await client.SendAsync(new byte[] { 5, 0xFF });
                        return;
                    }

                    await client.SendAsync(new byte[] { 5, 0x02 });
                    await ReadExactAsync(client, 1); // VER（RFC 1929 固定为 1，校验略）
                    byte[] ulenByte = await ReadExactAsync(client, 1);
                    byte[] user = await ReadExactAsync(client, ulenByte[0]);
                    byte[] plenByte = await ReadExactAsync(client, 1);
                    byte[] pass = await ReadExactAsync(client, plenByte[0]);
                    if (Encoding.UTF8.GetString(user) != Username
                        || Encoding.UTF8.GetString(pass) != (Password ?? string.Empty))
                    {
                        Interlocked.Increment(ref AuthRejections);
                        await client.SendAsync(new byte[] { 1, 1 }); // status != 0 = 拒绝
                        return;
                    }

                    await client.SendAsync(new byte[] { 1, 0 });
                }
                else
                {
                    await client.SendAsync(new byte[] { 5, 0x00 });
                }

                // CONNECT：VER CMD RSV ATYP ADDR PORT
                byte[] req = await ReadExactAsync(client, 4);
                byte[] addr;
                if (req[3] == 1)
                {
                    addr = await ReadExactAsync(client, 4);
                }
                else if (req[3] == 3)
                {
                    byte[] lenByte = await ReadExactAsync(client, 1);
                    addr = await ReadExactAsync(client, lenByte[0]);
                }
                else if (req[3] == 4)
                {
                    addr = await ReadExactAsync(client, 16);
                }
                else
                {
                    return;
                }

                byte[] portBytes = await ReadExactAsync(client, 2);
                if (RejectConnect)
                {
                    await client.SendAsync(new byte[] { 5, 1, 0, 1, 0, 0, 0, 0, 0, 0 });
                    return;
                }

                int port = BinaryPrimitives.ReadUInt16BigEndian(portBytes);
                var target = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await target.ConnectAsync(new IPEndPoint(new IPAddress(addr), port));
                Interlocked.Increment(ref ConnectCount);
                await client.SendAsync(new byte[] { 5, 0, 0, 1, 0, 0, 0, 0, 0, 0 });

                // 双向转发：SOCKS 隧道里跑的就是 echo broker 的 RocketMQ 帧
                Task a = Relay(client, target);
                Task b = Relay(target, client);
                await Task.WhenAll(a, b);
            }
            catch
            {
                // 握手失败 / 断开即结束
            }
            finally
            {
                try { client.Dispose(); } catch { /* 忽略 */ }
            }
        }

        public void Dispose()
        {
            _server.Dispose();
        }
    }

    /// <summary>假 SOCKS4/4a 服务器：8 字节头 + USERID(NUL 结尾) [+ DOMAIN(NUL 结尾)]，
    /// 回 CD=0x5A 后双向转发。</summary>
    private sealed class FakeSocks4Server : IDisposable
    {
        private readonly TcpTestServer _server;
        public int ConnectCount;

        public FakeSocks4Server()
        {
            _server = new TcpTestServer(HandleClient);
        }

        public string Addr => _server.Addr;
        public Task Loop() => _server.Loop();

        private async Task HandleClient(Socket client)
        {
            try
            {
                byte[] head = await ReadExactAsync(client, 8);
                if (head[0] != 4)
                {
                    return;
                }

                // USERID：读到 NUL
                while ((await ReadExactAsync(client, 1))[0] != 0)
                {
                }

                // SOCKS4a：DSTIP = 0.0.0.x 时其后跟 NUL 结尾的域名
                if (head[4] == 0 && head[5] == 0 && head[6] == 0)
                {
                    while ((await ReadExactAsync(client, 1))[0] != 0)
                    {
                    }
                }

                int port = BinaryPrimitives.ReadUInt16BigEndian(head.AsSpan(2));
                var target = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await target.ConnectAsync(new IPEndPoint(new IPAddress(head.AsSpan(4, 4)), port));
                Interlocked.Increment(ref ConnectCount);
                // VN CD DSTPORT(2) DSTIP(4)，CD = 0x5A 表示同意
                await client.SendAsync(new byte[] { 0, 0x5A, head[2], head[3], 0, 0, 0, 0 });

                Task a = Relay(client, target);
                Task b = Relay(target, client);
                await Task.WhenAll(a, b);
            }
            catch
            {
                // 握手失败 / 断开即结束
            }
            finally
            {
                try { client.Dispose(); } catch { /* 忽略 */ }
            }
        }

        public void Dispose()
        {
            _server.Dispose();
        }
    }

    private static async Task Relay(Socket from, Socket to)
    {
        byte[] buf = new byte[8192];
        try
        {
            while (true)
            {
                int n = await from.ReceiveAsync(buf);
                if (n <= 0)
                {
                    break;
                }

                await to.SendAsync(buf.AsMemory(0, n));
            }
        }
        catch
        {
            // 单向断开即退出，另一向会随之见到 EOF/异常
        }
        finally
        {
            try { to.Dispose(); } catch { /* 忽略 */ }
        }
    }

    private static async Task<byte[]> ReadExactAsync(Socket sock, int count)
    {
        var buf = new byte[count];
        int got = 0;
        while (got < count)
        {
            int n = await sock.ReceiveAsync(buf.AsMemory(got));
            if (n <= 0)
            {
                throw new IOException("unexpected EOF from client");
            }

            got += n;
        }

        return buf;
    }

    private static RemotingCommand MakeRequest()
    {
        var hdr = new SendMessageRequestHeader
        {
            ProducerGroup = "pg",
            Topic = "T",
        };
        return RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, hdr);
    }

    // ---------------------------------------------------------------- 用例

    [Fact]
    public async Task InvokeSync_ThroughSocks5_NoAuth_Succeeds()
    {
        using var broker = new TcpTestServer(HandleEcho);
        using var proxy = new FakeSocks5Server();
        Task proxyLoop = Task.Run(proxy.Loop);
        Task brokerLoop = Task.Run(broker.Loop);
        try
        {
            using var client = new RemotingClient(connectTimeoutMillis: 10000, invokeTimeoutMillis: 15000,
                socksProxy: new SocksProxyConfig { Addr = proxy.Addr });
            RemotingCommand resp = client.InvokeSync(broker.Addr, MakeRequest());
            Assert.Equal(ResponseCode.Success, resp.Code);
            Assert.Equal("ack-10", Encoding.UTF8.GetString(resp.Body)); // SendMessage=10
            Assert.Equal(1, proxy.ConnectCount);
        }
        finally
        {
            await Task.WhenAny(brokerLoop, Task.Delay(100));
            await Task.WhenAny(proxyLoop, Task.Delay(100));
        }
    }

    [Fact]
    public async Task InvokeSync_ThroughSocks5_UserPassAuth_Succeeds()
    {
        using var broker = new TcpTestServer(HandleEcho);
        using var proxy = new FakeSocks5Server { Username = "rmq", Password = "secret" };
        Task proxyLoop = Task.Run(proxy.Loop);
        Task brokerLoop = Task.Run(broker.Loop);
        try
        {
            using var client = new RemotingClient(connectTimeoutMillis: 10000, invokeTimeoutMillis: 15000,
                socksProxy: new SocksProxyConfig { Addr = proxy.Addr, Username = "rmq", Password = "secret" });
            RemotingCommand resp = client.InvokeSync(broker.Addr, MakeRequest());
            Assert.Equal(ResponseCode.Success, resp.Code);
            Assert.Equal(1, proxy.ConnectCount);
            Assert.Equal(0, proxy.AuthRejections);
        }
        finally
        {
            await Task.WhenAny(brokerLoop, Task.Delay(100));
            await Task.WhenAny(proxyLoop, Task.Delay(100));
        }
    }

    [Fact]
    public async Task InvokeSync_ThroughSocks5_WrongPassword_ThrowsConnectException()
    {
        using var broker = new TcpTestServer(HandleEcho);
        using var proxy = new FakeSocks5Server { Username = "rmq", Password = "secret" };
        Task proxyLoop = Task.Run(proxy.Loop);
        Task brokerLoop = Task.Run(broker.Loop);
        try
        {
            // 连接失败类断言：给 ≥30s 预算（本机 loopback 怪癖 + 子协商往返余量）
            using var client = new RemotingClient(connectTimeoutMillis: 30000, invokeTimeoutMillis: 60000,
                socksProxy: new SocksProxyConfig { Addr = proxy.Addr, Username = "rmq", Password = "wrong" });
            RemotingConnectException ex = Assert.Throws<RemotingConnectException>(
                () => client.InvokeSync(broker.Addr, MakeRequest()));
            Assert.Contains("socks", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1, proxy.AuthRejections);
            Assert.Equal(0, proxy.ConnectCount);
        }
        finally
        {
            await Task.WhenAny(brokerLoop, Task.Delay(100));
            await Task.WhenAny(proxyLoop, Task.Delay(100));
        }
    }

    [Fact]
    public async Task InvokeSync_Socks5_DemandsAuthButNoCredentials_ThrowsConnectException()
    {
        using var broker = new TcpTestServer(HandleEcho);
        using var proxy = new FakeSocks5Server { Username = "rmq", Password = "secret" };
        Task proxyLoop = Task.Run(proxy.Loop);
        Task brokerLoop = Task.Run(broker.Loop);
        try
        {
            // 客户端没配凭证 → 只报 0x00 方法 → 代理回 0xFF
            using var client = new RemotingClient(connectTimeoutMillis: 30000, invokeTimeoutMillis: 60000,
                socksProxy: new SocksProxyConfig { Addr = proxy.Addr });
            RemotingConnectException ex = Assert.Throws<RemotingConnectException>(
                () => client.InvokeSync(broker.Addr, MakeRequest()));
            Assert.Contains("rejected all offered auth methods", ex.Message);
            Assert.Equal(0, proxy.ConnectCount);
        }
        finally
        {
            await Task.WhenAny(brokerLoop, Task.Delay(100));
            await Task.WhenAny(proxyLoop, Task.Delay(100));
        }
    }

    [Fact]
    public async Task InvokeSync_Socks5_ConnectRejected_ThrowsConnectException()
    {
        using var broker = new TcpTestServer(HandleEcho);
        using var proxy = new FakeSocks5Server { RejectConnect = true };
        Task proxyLoop = Task.Run(proxy.Loop);
        Task brokerLoop = Task.Run(broker.Loop);
        try
        {
            using var client = new RemotingClient(connectTimeoutMillis: 30000, invokeTimeoutMillis: 60000,
                socksProxy: new SocksProxyConfig { Addr = proxy.Addr });
            RemotingConnectException ex = Assert.Throws<RemotingConnectException>(
                () => client.InvokeSync(broker.Addr, MakeRequest()));
            Assert.Contains("CONNECT", ex.Message);
            Assert.Equal(0, proxy.ConnectCount);
        }
        finally
        {
            await Task.WhenAny(brokerLoop, Task.Delay(100));
            await Task.WhenAny(proxyLoop, Task.Delay(100));
        }
    }

    [Fact]
    public async Task InvokeSync_ThroughSocks4_Succeeds()
    {
        using var broker = new TcpTestServer(HandleEcho);
        using var proxy = new FakeSocks4Server();
        Task proxyLoop = Task.Run(proxy.Loop);
        Task brokerLoop = Task.Run(broker.Loop);
        try
        {
            // 目标是 127.0.0.1（IPv4 字面量）→ 原生 SOCKS4（非 4a），USERID 取 Username
            using var client = new RemotingClient(connectTimeoutMillis: 10000, invokeTimeoutMillis: 15000,
                socksProxy: new SocksProxyConfig
                {
                    Addr = proxy.Addr,
                    Version = SocksProxyVersion.V4,
                    Username = "user4",
                });
            RemotingCommand resp = client.InvokeSync(broker.Addr, MakeRequest());
            Assert.Equal(ResponseCode.Success, resp.Code);
            Assert.Equal(1, proxy.ConnectCount);
        }
        finally
        {
            await Task.WhenAny(brokerLoop, Task.Delay(100));
            await Task.WhenAny(proxyLoop, Task.Delay(100));
        }
    }

    [Fact]
    public void InvokeSync_DeadProxy_ThrowsConnectException()
    {
        // 预留一个没人监听的 loopback 端口当"死代理"
        var reserved = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        reserved.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        string deadAddr = ((IPEndPoint)reserved.LocalEndPoint!).ToString();
        reserved.Dispose();

        // 本机 loopback 怪癖：刚关闭的端口约 2s 内还可能"连上"再被拒，
        // 所以 connect/invoke 都给足 ≥30s 预算，断言只看异常类型。
        using var client = new RemotingClient(connectTimeoutMillis: 30000, invokeTimeoutMillis: 60000,
            socksProxy: new SocksProxyConfig { Addr = deadAddr });
        Assert.Throws<RemotingConnectException>(
            () => client.InvokeSync("127.0.0.1:9", MakeRequest()));
    }

    [Fact]
    public void SocksProxyConfig_ToString_MasksPassword()
    {
        var cfg = new SocksProxyConfig { Addr = "10.0.0.1:1080", Username = "u", Password = "p" };
        string s = cfg.ToString();
        Assert.Contains("10.0.0.1:1080", s);
        Assert.Contains("username: u", s);
        Assert.DoesNotContain("password: p", s);
        Assert.Contains("***", s);
    }
}
