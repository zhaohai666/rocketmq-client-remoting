// SOCKS proxy transport layer unit tests: a hand-written local fake SOCKS5 / SOCKS4
// server (minimal handshake + CONNECT + bidirectional forwarding to a local echo
// broker) asserts that RemotingClient, once configured with SocksProxyConfig, completes a
// full invokeSync through the proxy; then the four failure paths are covered: username
// and password sub-negotiation success and rejection, method rejection, and a rejected
// CONNECT, plus a dead proxy.
//
// A local loopback quirk: a just-closed port may still "connect" and then be rejected for
// about 2s -- so every "connection failure" assertion is given a timeout budget of >=30s
// (connect 30s + invoke 60s) instead of the default 3s.
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
    // ---------------------------------------------------------------- Test infrastructure

    /// <summary>A general purpose TCP test server: Bind(0) to start listening, each
    /// connection handled asynchronously by the handler.</summary>
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

    /// <summary>An echo broker: reads a whole request frame by totalLength, then writes
    /// back a response encoding the code verbatim.</summary>
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
            // Exits as soon as the connection drops
        }
        finally
        {
            try { client.Dispose(); } catch { /* ignore */ }
        }
    }

    /// <summary>
    /// A fake SOCKS5 server: minimal handshake (method selection + optional RFC 1929
    /// username/password sub-negotiation) + CONNECT to the real target + bidirectional
    /// forwarding. A non-empty Username means username/password auth is required.
    /// </summary>
    private sealed class FakeSocks5Server : IDisposable
    {
        private readonly TcpTestServer _server;

        public string? Username;
        public string? Password;
        public bool RejectConnect;       // CONNECT replies REP=1 (general failure)
        public int AuthRejections;       // how many times the username/password was rejected
        public int ConnectCount;         // how many CONNECTs succeeded

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
                    return; // not SOCKS5, drop right away
                }

                byte[] methods = await ReadExactAsync(client, head[1]);
                if (Username is not null)
                {
                    // Username/password auth required: the client must offer method 0x02
                    if (Array.IndexOf(methods, (byte)0x02) < 0)
                    {
                        await client.SendAsync(new byte[] { 5, 0xFF });
                        return;
                    }

                    await client.SendAsync(new byte[] { 5, 0x02 });
                    await ReadExactAsync(client, 1); // VER (fixed at 1 in RFC 1929, the check is skipped)
                    byte[] ulenByte = await ReadExactAsync(client, 1);
                    byte[] user = await ReadExactAsync(client, ulenByte[0]);
                    byte[] plenByte = await ReadExactAsync(client, 1);
                    byte[] pass = await ReadExactAsync(client, plenByte[0]);
                    if (Encoding.UTF8.GetString(user) != Username
                        || Encoding.UTF8.GetString(pass) != (Password ?? string.Empty))
                    {
                        Interlocked.Increment(ref AuthRejections);
                        await client.SendAsync(new byte[] { 1, 1 }); // status != 0 means rejected
                        return;
                    }

                    await client.SendAsync(new byte[] { 1, 0 });
                }
                else
                {
                    await client.SendAsync(new byte[] { 5, 0x00 });
                }

                // CONNECT: VER CMD RSV ATYP ADDR PORT
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

                // Bidirectional forwarding: what runs inside the SOCKS tunnel are the echo broker's RocketMQ frames
                Task a = Relay(client, target);
                Task b = Relay(target, client);
                await Task.WhenAll(a, b);
            }
            catch
            {
                // A failed handshake / a disconnect ends it
            }
            finally
            {
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        public void Dispose()
        {
            _server.Dispose();
        }
    }

    /// <summary>A fake SOCKS4/4a server: an 8-byte header + USERID (NUL terminated)
    /// [+ DOMAIN (NUL terminated)], replying CD=0x5A and then forwarding
    /// bidirectionally.</summary>
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

                // USERID: read up to NUL
                while ((await ReadExactAsync(client, 1))[0] != 0)
                {
                }

                // SOCKS4a: when DSTIP = 0.0.0.x a NUL terminated domain name follows
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
                // VN CD DSTPORT(2) DSTIP(4), CD = 0x5A means granted
                await client.SendAsync(new byte[] { 0, 0x5A, head[2], head[3], 0, 0, 0, 0 });

                Task a = Relay(client, target);
                Task b = Relay(target, client);
                await Task.WhenAll(a, b);
            }
            catch
            {
                // A failed handshake / a disconnect ends it
            }
            finally
            {
                try { client.Dispose(); } catch { /* ignore */ }
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
            // Exits as soon as one direction drops, the other then sees an EOF/an exception
        }
        finally
        {
            try { to.Dispose(); } catch { /* ignore */ }
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

    // ---------------------------------------------------------------- Cases

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
            // "Connection failure" assertions: give a >=30s budget (local loopback quirk + sub-negotiation round-trip slack)
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
            // The client has no credentials configured -> it only offers method 0x00 -> the proxy replies 0xFF
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
            // The target is 127.0.0.1 (an IPv4 literal) -> native SOCKS4 (not 4a), with USERID taken from Username
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
        // Reserve a loopback port nobody listens on as the "dead proxy"
        var reserved = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        reserved.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        string deadAddr = ((IPEndPoint)reserved.LocalEndPoint!).ToString();
        reserved.Dispose();

        // A local loopback quirk: a just-closed port may still "connect" and then be
        // rejected for about 2s, so connect/invoke both get a full >=30s budget and the
        // assertion only looks at the exception type.
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
