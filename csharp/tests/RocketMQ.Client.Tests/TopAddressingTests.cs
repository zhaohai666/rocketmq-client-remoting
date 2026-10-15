// Unit tests for the dynamic name server (DefaultTopAddressing) -- a local HttpListener
// mock, no dependency on the public internet.
using System;
using System.Collections.Generic;
using System.Threading;

using Xunit;

namespace RocketMQ.Client.Tests;

public class TopAddressingTests : IDisposable
{
    private readonly HttpListenerMock _server = new();

    private DefaultTopAddressing Top() => new(
        domain: "127.0.0.1:" + _server.Port);

    public TopAddressingTests()
    {
        // MQClientInstance builds a TopAddressing internally (it reads env at construction), so the mock address server is injected via an environment variable
        Environment.SetEnvironmentVariable("ROCKETMQ_NAMESRV_DOMAIN", "127.0.0.1:" + _server.Port);
    }

    public void Dispose()
    {
        _server.Dispose();
        Environment.SetEnvironmentVariable("ROCKETMQ_NAMESRV_DOMAIN", null);
    }

    // ------------------------------------------------ URL construction

    [Fact]
    public void GetWsAddrAppendsDefaultPort()
        => Assert.Equal("http://jmenv.tbsite.net:8080/rocketmq/nsaddr",
            DefaultTopAddressing.GetWsAddr("jmenv.tbsite.net"));

    [Fact]
    public void GetWsAddrSkipsDefaultPortWhenDomainHasOne()
        => Assert.Equal("http://host:12345/rocketmq/nsaddr",
            DefaultTopAddressing.GetWsAddr("host:12345"));

    [Fact]
    public void BuildUrlWithUnitName()
    {
        var ta = new DefaultTopAddressing("http://h:8080/rocketmq/nsaddr", unitName: "unitA");
        Assert.Equal("http://h:8080/rocketmq/nsaddr-unitA?nofix=1", ta.BuildUrl());
    }

    [Fact]
    public void BuildUrlWithUnitNameAndPara()
    {
        var ta = new DefaultTopAddressing("http://h:8080/rocketmq/nsaddr",
            unitName: "u", para: new Dictionary<string, string> { ["k"] = "v" });
        Assert.Equal("http://h:8080/rocketmq/nsaddr-u?nofix=1&k=v", ta.BuildUrl());
    }

    [Fact]
    public void BuildUrlIgnoresBlankUnitName()
    {
        var ta = new DefaultTopAddressing("http://h:8080/rocketmq/nsaddr", unitName: "   ");
        Assert.Equal("http://h:8080/rocketmq/nsaddr", ta.BuildUrl());
    }

    // ------------------------------------------------ ClearNewLine

    [Fact]
    public void ClearNewLineCutsAtCr()
        => Assert.Equal("1.2.3.4:9876", DefaultTopAddressing.ClearNewLine("  1.2.3.4:9876\r\nrest"));

    [Fact]
    public void ClearNewLineCutsAtLf()
        => Assert.Equal("a:9876", DefaultTopAddressing.ClearNewLine("a:9876\nb:9877"));

    [Fact]
    public void ClearNewLineTrimsOnly()
        => Assert.Equal("a:9876", DefaultTopAddressing.ClearNewLine("  a:9876  "));

    // ------------------------------------------------ Address fetching behaviour

    [Fact]
    public void FetchReturnsClearedBodyOn200()
    {
        _server.Status = 200;
        _server.Body = "10.0.0.1:9876;10.0.0.2:9876\nextra";
        string? got = Top().FetchNsAddr(verbose: false);
        Assert.Equal("10.0.0.1:9876;10.0.0.2:9876", got);
        Assert.StartsWith("/rocketmq/nsaddr", _server.LastPath);
    }

    [Fact]
    public void FetchReturnsNullOnNon200()
    {
        _server.Status = 500;
        Assert.Null(Top().FetchNsAddr(verbose: false));
    }

    [Fact]
    public void FetchReturnsNullOnConnectionError()
    {
        var ta = new DefaultTopAddressing(domain: "127.0.0.1:1", timeoutMillis: 300);
        Assert.Null(ta.FetchNsAddr(verbose: false));
    }

    [Fact]
    public void NoDomainMeansDisabled()
    {
        // explicitly passing an empty string domain ("?? env" only falls back on null) -> dynamic fetching is off
        var ta = new DefaultTopAddressing(domain: string.Empty);
        Assert.Equal(string.Empty, ta.WsAddr);
        Assert.Null(ta.FetchNsAddr());
    }

    [Fact]
    public void ApplyChangeOnly()
    {
        _server.Status = 200;
        _server.Body = "127.0.0.1:9876";
        var ta = Top();
        Assert.Equal("127.0.0.1:9876", ta.FetchAndApply());   // first time: changed -> applied
        Assert.Null(ta.FetchAndApply());                      // same -> not applied
        _server.Body = "10.0.0.9:9876";
        Assert.Equal("10.0.0.9:9876", ta.FetchAndApply());    // changed -> applied
    }

    // ------------------------------------------------ Instance integration

    [Fact]
    public void StartWithStaticAddrsNeverFetches()
    {
        _server.Status = 200;
        _server.Body = "127.0.0.1:9876";
        using var mqc = new MQClientInstance("c@dotnetdyn", new[] { "127.0.0.1:9876" });
        mqc.TopAddressing.GetType();   // a no-op reference
        mqc.Start();
        mqc.Shutdown();
        Assert.Equal(0, _server.HitCount);   // with a static address configured it must not ask the address server
    }

    [Fact]
    public void StartWithEmptyAddrsFetchesOnce()
    {
        _server.Status = 200;
        _server.Body = "127.0.0.1:9876";
        using var mqc = new MQClientInstance("c@dotnetdyn2", Array.Empty<string>());
        mqc.Start();
        Assert.Equal(new[] { "127.0.0.1:9876" }, mqc.NameServerAddrs);
        Assert.Equal(1, _server.HitCount);
        mqc.Shutdown();
    }

    [Fact]
    public void StartFailsWhenAddressServerReturnsNone()
    {
        _server.Status = 500;
        using var mqc = new MQClientInstance("c@dotnetdyn3", Array.Empty<string>());
        Assert.Throws<MQClientException>(() => mqc.Start());
    }

    [Fact]
    public void PeriodicRefreshPicksUpChange()
    {
        _server.Status = 200;
        _server.Body = "127.0.0.1:9876";
        using var mqc = new MQClientInstance("c@dotnetdyn4", Array.Empty<string>());
        mqc.Start();
        _server.Body = "10.0.0.9:9876";
        mqc.FetchNameServerAddr();
        Assert.Equal(new[] { "10.0.0.9:9876" }, mqc.NameServerAddrs);
        mqc.Shutdown();
    }

    // ------------------------------------------------ mock server

    /// <summary>A minimal address server: a hand-written HTTP/1.0 response over
    /// TcpListener. (HttpListener needs ~10s to start and stop on macOS, which is
    /// unusable in a unit test.)</summary>
    private sealed class HttpListenerMock : IDisposable
    {
        private readonly System.Net.Sockets.TcpListener _listener;
        private readonly Thread _thread;
        private int _status = 200;
        private string _body = "127.0.0.1:9876";
        private int _hits;
        private string _lastPath = string.Empty;
        private volatile bool _running = true;

        public HttpListenerMock()
        {
            _listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((System.Net.IPEndPoint)_listener.LocalEndpoint).Port;
            _thread = new Thread(Loop) { IsBackground = true };
            _thread.Start();
        }

        public int Port { get; }
        public int Status { set => _status = value; }
        public string Body { set => _body = value; }
        public int HitCount => _hits;
        public string LastPath => _lastPath;

        private void Loop()
        {
            while (_running)
            {
                System.Net.Sockets.TcpClient client;
                try
                {
                    client = _listener.AcceptTcpClient();
                }
                catch
                {
                    return;   // the listener is already closed
                }
                using (client)
                using (var stream = client.GetStream())
                {
                    // read the request headers (up to the blank line), take the path from the request line
                    var buf = new byte[4096];
                    int total = 0, n;
                    while ((n = stream.Read(buf, total, buf.Length - total)) > 0)
                    {
                        total += n;
                        string got = System.Text.Encoding.ASCII.GetString(buf, 0, total);
                        if (got.Contains("\r\n\r\n") || total >= buf.Length) break;
                    }
                    string request = System.Text.Encoding.ASCII.GetString(buf, 0, total);
                    int sp1 = request.IndexOf(' ');
                    int sp2 = sp1 >= 0 ? request.IndexOf(' ', sp1 + 1) : -1;
                    if (sp1 >= 0 && sp2 > sp1) _lastPath = request.Substring(sp1 + 1, sp2 - sp1 - 1);
                    Interlocked.Increment(ref _hits);
                    byte[] head = System.Text.Encoding.ASCII.GetBytes(
                        "HTTP/1.0 " + _status + (_status == 200 ? " OK" : " ERR") + "\r\n"
                        + "Content-Length: " + System.Text.Encoding.UTF8.GetByteCount(_body)
                        + "\r\nConnection: close\r\n\r\n");
                    byte[] body = System.Text.Encoding.UTF8.GetBytes(_body);
                    stream.Write(head, 0, head.Length);
                    stream.Write(body, 0, body.Length);
                    stream.Flush();
                }
            }
        }

        public void Dispose()
        {
            _running = false;
            try { _listener.Stop(); } catch { /* ignore */ }
        }
    }
}
