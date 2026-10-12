// 提供：连接管理（惰性建连 + 复用 + TCP_NODELAY）、invokeSync / invokeAsync /
// invokeOneway、opaque 到响应回调的分发、超时控制、连接状态探活。
//
// 线程模型（与 Python/C++ 参考实现一致，读线程每连接一个）：
//   - 调用线程：encode -> 锁写 -> Socket.Send 全量；随后在 invokeSync 里等事件
//   - 读线程：阻塞 Receive -> 按 totalLength(4) 分帧 -> decode ->
//             按 opaque 从响应表取出 future -> 唤醒调用线程 / 触发回调
//   - C# 的阻塞 Receive 在 socket 被另一线程 Close 时立即抛异常退出，
//     因此不需要 C++ 那套 select(300ms) 轮询，关连接即可及时回收读线程。
//
// 线格式：totalLength(4) | headerLength(4) | header | body
//   totalLength = 4 + headerLength + bodyLength（即首 4 字节之后的所有字节数）
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Remoting;

/// <summary>带超时控制的 TCP 传输客户端。</summary>
public sealed class RemotingClient : IDisposable
{
    public delegate void InvokeCallback(RemotingCommand? response, Exception? error);

    public delegate RemotingCommand? RequestProcessor(RemotingCommand request, string addr);

    public const int MaxFrameLength = 16 * 1024 * 1024;

    private sealed class Connection
    {
        public required string Addr;
        public Socket Sock = null!;
        // 写侧互斥。⚠ 它的存在不只是"别把两个请求的字节 interleleave"，还钉住了
        // SslStream 的并发口径：运行时的 SslStream 用 _nestedRead / _nestedWrite 两个
        // **互相独立**的标记位放行 I/O（见 System.Net.Security/SslStream.IO.cs），
        // 也就是只支持"一路读 + 一路写"，同侧重入直接抛 invalidnestedcall。
        // 本类每条连接只有一个读线程，加上这把写锁，正好落在被支持的形状里 ——
        // 去掉它或再起第二个读线程，就会把 TLS 连接推到未定义行为上。
        public readonly object WriteLock = new();
        public volatile bool ReaderDone;
        // TLS 会话（TLS 启用时非空；读写走 SslStream）
        public SslStream? Tls;
    }

    private sealed class Future
    {
        public readonly ManualResetEventSlim Done = new(false);
        public RemotingCommand Response = new();

        public bool HasResponse;
        public InvokeCallback? Callback;

        public string Addr = string.Empty;
        public long DeadlineMs;
        public long TimeoutMs;
        public bool CallbackFired;

        public Connection? Conn;

        public Exception? Cause;

        public static long MonoNowMs() => Environment.TickCount64;
    }

    private volatile bool _running = true;
    private readonly int _connectTimeoutMillis;
    private readonly int _invokeTimeoutMillis;
    private readonly bool _tlsEnable;
    private readonly TlsOptions? _tlsOptions;
    private readonly SocksProxyConfig? _socksProxy;

    private readonly object _connMutex = new();
    private readonly Dictionary<string, Connection> _conns = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<int, Future> _respTable = new();

    private readonly object _sweepLock = new();
    private Timer? _sweeper;

    // broker 主动请求处理器表：requestCode -> handler。仅用于事务回查
    // (CHECK_TRANSACTION_STATE=39) 这类「服务端反过来找我」的命令。
    private readonly Dictionary<int, RequestProcessor> _processors = new();
    private readonly object _procLock = new();

    // 读线程账本：读线程自行结束后置 ReaderDone，由 PruneThreads 回收，避免句柄无限堆积。
    private readonly object _threadMutex = new();
    private readonly List<(Connection conn, Thread thread)> _threads = new();

    // RPC 钩子（ACL 等）。注册发生在 start 阶段；发送路径只在锁内取一次引用，
    // **绝不持锁调用钩子**（钩子内部要做签名计算）。
    private readonly object _hookLock = new();
    private IRpcHook? _rpcHook;

    public bool RegisterRpcHook(IRpcHook hook)
    {
        lock (_hookLock)
        {
            if (_rpcHook is not null)
            {
                return false;
            }

            _rpcHook = hook;
            return true;
        }
    }

    public void UnregisterRpcHook()
    {
        lock (_hookLock)
        {
            _rpcHook = null;
        }
    }

    /// <summary>当前已注册的请求钩子（未注册返回 null）。供 trace 启动这类「往已有链上
    /// 追加一段」的场景读取 —— 单槽传输层换钩子要拿着旧值才能组合（先 Unregister 再
    /// Register 链，中间不留空窗的责任在调用方）。</summary>
    public IRpcHook? CurrentHook() => CurrentRpcHook();

    private IRpcHook? CurrentRpcHook()
    {
        lock (_hookLock)
        {
            return _rpcHook;
        }
    }

    public RemotingClient(int connectTimeoutMillis = 3000, int invokeTimeoutMillis = 15000,
        bool? tlsEnable = null, TlsOptions? tlsOptions = null, SocksProxyConfig? socksProxy = null)
    {
        _connectTimeoutMillis = connectTimeoutMillis;
        _invokeTimeoutMillis = invokeTimeoutMillis;
        _tlsEnable = tlsEnable ?? EnvTlsEnabled();
        _tlsOptions = tlsOptions;
        _socksProxy = socksProxy;
    }

    internal static bool EnvTlsEnabled()
    {
        string? v = Environment.GetEnvironmentVariable("ROCKETMQ_TLS_ENABLE");
        return v is not null && (v.Trim().ToLowerInvariant() is "1" or "true" or "yes");
    }

    /// <summary>
    /// 严格校验的主机名/SAN 匹配：host 是 IP 时比对 IP SAN，否则比对 DNS SAN
    /// （精确或 <c>*.example</c> 单层通配）。CustomRootTrust 链不查名字，所以
    /// RemoteCertificateValidationCallback 里必须自己补这一步。
    /// </summary>
    internal static bool CertMatchesHostname(X509Certificate2 cert, string hostname)
    {
        if (string.IsNullOrEmpty(hostname)) return false;
        bool hostIsIp = IPAddress.TryParse(hostname, out IPAddress? hostIp);
        foreach (var ext in cert.Extensions)
        {
            if (ext is not X509SubjectAlternativeNameExtension san) continue;
            if (hostIsIp)
            {
                // EnumerateIPAddresses() 返回 IEnumerable<IPAddress>（.NET 10）。
                // 归一比较：顺带处理 ::ffff:127.0.0.1 这类 IPv4-mapped 形态。
                foreach (IPAddress ip in san.EnumerateIPAddresses())
                {
                    if (ip.Equals(hostIp)) return true;
                    if (ip.IsIPv4MappedToIPv6 && ip.MapToIPv4().Equals(hostIp)) return true;
                }
            }
            else
            {
                foreach (var dns in san.EnumerateDnsNames())
                {
                    if (dns.Equals(hostname, StringComparison.OrdinalIgnoreCase)) return true;
                    // 单层通配：*.example.com 匹配 a.example.com，不匹配 a.b.example.com
                    if (dns.StartsWith("*.", StringComparison.Ordinal))
                    {
                        var suffix = dns[1..]; // ".example.com"
                        var idx = hostname.IndexOf('.', StringComparison.Ordinal);
                        if (idx > 0 && hostname[idx..].Equals(suffix, StringComparison.OrdinalIgnoreCase)
                            && !hostname[(idx + 1)..].Contains('.'))
                        {
                            return true;
                        }
                    }
                }
            }
        }
        // 兜底：无 SAN 段时退回 CN（RFC 6125 已废弃，但对老证书仍有用）
        if (!cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Any())
        {
            var cn = cert.GetNameInfo(X509NameType.DnsName, false);
            return cn is not null && cn.Equals(hostname, StringComparison.OrdinalIgnoreCase);
        }
        return false;
    }

    internal static bool CertHasSan(X509Certificate2 cert)
    {
        return cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Any();
    }

    public bool EnableReconnectForGoAway { get; set; } = true;

    public int ConnectTimeoutMillis => _connectTimeoutMillis;

    public int InvokeTimeoutMillis => _invokeTimeoutMillis;

    /// <summary>当前活跃连接数（测试/诊断用）。</summary>
    public int ConnectionCount
    {
        get
        {
            lock (_connMutex)
            {
                return _conns.Count;
            }
        }
    }

    public int PendingRequestCount => _respTable.Count;

    // ---------------------------------------------------------------- 建连

    private static Socket ConnectWithTimeout(string host, int port, int timeoutMillis, out string error)
    {
        error = string.Empty;
        // IPv6 字面量直接交给 IPAddress.Parse；主机名走 DNS
        IPAddress[] addresses;
        try
        {
            addresses = IPAddress.TryParse(host, out IPAddress? literal)
                ? new[] { literal }
                : Dns.GetHostAddresses(host);
        }
        catch (Exception e)
        {
            error = "cannot resolve host " + host + ": " + e.Message;
            return null!;
        }

        foreach (IPAddress ip in addresses)
        {
            if (ip.AddressFamily != AddressFamily.InterNetwork && ip.AddressFamily != AddressFamily.InterNetworkV6)
            {
                continue;
            }

            var sock = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                // ConnectAsync + 超时：等价于 C++ 的非阻塞 connect + select
                using var cts = new CancellationTokenSource(timeoutMillis);
                Task connectTask = sock.ConnectAsync(ip, port, cts.Token).AsTask();
                try
                {
                    connectTask.Wait();
                }
                catch (AggregateException) when (cts.IsCancellationRequested)
                {
                    // 连接超时
                    try
                    {
                        sock.Dispose();
                    }
                    catch
                    {
                        // dispose 失败忽略
                    }

                    continue;
                }

                TuneSocket(sock);
                return sock;
            }
            catch (Exception)
            {
                try
                {
                    sock.Dispose();
                }
                catch
                {
                    // dispose 失败忽略
                }
            }
        }

        error = "connect failed to " + host + ":" + port.ToString(CultureInfo.InvariantCulture);
        return null!;
    }

    private static void TuneSocket(Socket sock)
    {
        try
        {
            sock.NoDelay = true; // TCP_NODELAY：请求需低延迟
        }
        catch
        {
            // 个别平台不支持，忽略
        }
    }

    private Socket ConnectViaSocks(string addr, string destHost, int destPort)
    {
        SocksProxyConfig proxy = _socksProxy!;
        ParseAddress(proxy.Addr, out string proxyHost, out string proxyPortStr);
        int proxyPort = int.TryParse(proxyPortStr, NumberStyles.Integer, CultureInfo.InvariantCulture,
            out int pp) ? pp : 0;
        if (proxyHost.Length == 0 || proxyPort is < 1 or > 65535)
        {
            throw new RemotingConnectException("invalid socks proxy addr: " + proxy.Addr);
        }

        Socket sock = ConnectWithTimeout(proxyHost, proxyPort, _connectTimeoutMillis, out string error);
        if (sock is null)
        {
            throw new RemotingConnectException("socks proxy " + proxy.Addr + " connect failed: " + error);
        }

        try
        {
            SocksProxy.Handshake(sock, proxy, destHost, destPort, _connectTimeoutMillis);
            return sock;
        }
        catch (Exception e)
        {
            try { sock.Dispose(); } catch { /* ignore */ }
            throw new RemotingConnectException(addr + " (socks handshake via " + proxy.Addr
                + ": " + e.Message + ")");
        }
    }

    /// <summary>"host:port" 拆分，支持 IPv6 的 [::1]:10911 形式。</summary>
    public static void ParseAddress(string addr, out string host, out string port)
    {
        host = string.Empty;
        port = string.Empty;
        if (addr.Length > 0 && addr[0] == '[')
        {
            int end = addr.IndexOf(']');
            if (end > 0)
            {
                host = addr[1..end];
                int colon = addr.IndexOf(':', end);
                port = colon < 0 ? string.Empty : addr[(colon + 1)..];
                return;
            }
        }

        // IPv4 / 主机名：最后一个 ':' 之后是端口
        int lastColon = addr.LastIndexOf(':');
        if (lastColon < 0)
        {
            host = addr;
            return;
        }

        host = addr[..lastColon];
        port = addr[(lastColon + 1)..];
    }

    private Connection GetOrCreateConnection(string addr)
    {
        // shutdown 之后不再建连：读线程退出时会投递回调（FailFast / 正常响应），回调里
        // 若重进传输层，就会在 Shutdown 正持有 _threadMutex 等 Join 的当口往账本里塞新线程。
        if (!_running)
        {
            throw new RemotingConnectException("remoting client is shut down, " + addr);
        }

        lock (_connMutex)
        {
            if (_conns.TryGetValue(addr, out Connection? existing) && existing.Sock is { Connected: true })
            {
                return existing;
            }
        }

        // 建连放到锁外，避免慢 connect 阻塞其它地址
        ParseAddress(addr, out string host, out string portStr);
        int port = int.TryParse(portStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out int p) ? p : 0;

        Socket sock;
        if (_socksProxy is not null)
        {
            sock = ConnectViaSocks(addr, host, port);
        }
        else
        {
            sock = ConnectWithTimeout(host, port, _connectTimeoutMillis, out string error);
            if (sock is null)
            {
                throw new RemotingConnectException(error);
            }
        }

        var conn = new Connection { Addr = addr, Sock = sock };

        if (_tlsEnable)
        {
            try
            {
                var ns = new NetworkStream(sock, ownsSocket: false);
                var tlsOpts = _tlsOptions;
                bool strict = tlsOpts?.CaCert is not null;
                SslStream ssl;
                if (strict)
                {
                    var caCert = X509CertificateLoader.LoadCertificateFromFile(tlsOpts!.CaCert!);
                    var clientCerts = new X509CertificateCollection();
                    if (tlsOpts!.ClientCert is not null)
                    {
                        // PEM(证书+私钥) → X509Certificate2。⚠ macOS 的 SslStream 不认
                        // CreateFromPemFile 产出的**临时**私钥，必须先导出成 PFX 再用
                        // X509CertificateLoader 加载（PKCS#12 容器），否则客户端证书无法出示。
                        var withKey = string.IsNullOrEmpty(tlsOpts.ClientKey)
                            ? X509CertificateLoader.LoadCertificateFromFile(tlsOpts.ClientCert)
                            : X509Certificate2.CreateFromPemFile(tlsOpts.ClientCert, tlsOpts.ClientKey);
                        var pfx = withKey.Export(X509ContentType.Pkcs12);
                        clientCerts.Add(X509CertificateLoader.LoadPkcs12(pfx, null));
                    }
                    var chainPolicy = new X509ChainPolicy
                    {
                        TrustMode = X509ChainTrustMode.CustomRootTrust,
                        RevocationMode = X509RevocationMode.NoCheck,
                    };
                    chainPolicy.CustomTrustStore.Add(caCert);
                    var targetName = string.IsNullOrEmpty(tlsOpts.ServerName) ? host : tlsOpts.ServerName;
                    var auth = new SslClientAuthenticationOptions
                    {
                        TargetHost = host,
                        ClientCertificates = clientCerts,
                        RemoteCertificateValidationCallback = (sender, cert, chain, errors) =>
                        {
                            if (cert is null) return false;
                            var peer = (X509Certificate2)cert;
                            var custom = new X509Chain
                            {
                                ChainPolicy = chainPolicy,
                            };
                            if (!custom.Build(peer))
                            {
                                // 失败原因落到日志：链状态逐条打出来，否则只看到一句
                                // "rejected by callback" 无法区分链败还是名字败。
                                var statuses = string.Join(",", custom.ChainStatus.Select(s => s.Status));
                                ClientLog.Warn("tls strict: chain build failed for " + peer.Subject
                                               + " (SAN=" + CertHasSan(peer) + "), status=[" + statuses + "]");
                                return false;
                            }
                            // 主机名/SAN 校验（CustomRootTrust 链不含名字检查）：
                            // host 是 IP 时比 IP SAN，否则比 DNS SAN（支持 *.example 通配）。
                            if (!CertMatchesHostname(peer, targetName))
                            {
                                ClientLog.Warn("tls strict: hostname mismatch, target=" + targetName
                                               + " subject=" + peer.Subject + " (SAN=" + CertHasSan(peer) + ")");
                                return false;
                            }
                            return true;
                        },
                    };
                    ssl = new SslStream(ns, false);
                    ssl.AuthenticateAsClient(auth);
                }
                else
                {
                    ssl = new SslStream(ns, false,
                        (sender, cert, chain, errors) => true);   // test mode：信任一切
                    ssl.AuthenticateAsClient(host);
                }
                conn.Tls = ssl;
            }
            catch (Exception e)
            {
                try { sock.Dispose(); } catch { /* ignore */ }
                throw new RemotingConnectException(addr + " (tls handshake: " + e.Message + ")");
            }
        }

        PruneThreads();

        lock (_connMutex)
        {
            // 并发建连：若已有别人先建成，放弃自己的 socket
            if (_conns.TryGetValue(addr, out Connection? winner) && winner.Sock is { Connected: true })
            {
                try
                {
                    sock.Dispose();
                }
                catch
                {
                    // dispose 失败忽略
                }

                return winner;
            }

            _conns[addr] = conn;
        }

        var reader = new Thread(() =>
        {
            ClientLog.SetThreadName("RemotingClientReader-" + addr);
            ReadLoop(conn);
        })
        {
            IsBackground = true,
        };
        reader.Start();
        lock (_threadMutex)
        {
            _threads.Add((conn, reader));
        }

        return conn;
    }

    // ---------------------------------------------------------------- 读循环

    private void ReadLoop(Connection conn)
    {
        var buf = new MemoryStream();
        byte[] chunk = new byte[65536];
        Socket sock = conn.Sock;
        while (_running)
        {
            int n;
            try
            {
                n = conn.Tls is not null
                    ? conn.Tls.Read(chunk, 0, chunk.Length)
                    : sock.Receive(chunk, chunk.Length, SocketFlags.None);
            }
            catch (Exception)
            {
                // shutdown()/closeChannel() 关闭 socket 会让 Receive 立即抛异常退出，
                // 属正常退出路径，记 DEBUG（默认 INFO 下不可见，不会出现"退出时的假异常"噪声）。
                if (_running)
                {
                    ClientLog.Debug("remoting reader: connection " + conn.Addr + " closed, reader exiting");
                }

                break;
            }

            if (n <= 0)
            {
                if (_running)
                {
                    ClientLog.Debug("remoting reader: connection " + conn.Addr + " closed by peer");
                }

                break;
            }

            buf.Write(chunk, 0, n);
            buf.Flush();

            // 按 totalLength 前缀切帧；粘包/半包都由这里处理
            byte[] data = buf.GetBuffer();
            int consumed = 0;
            while (true)
            {
                int available = (int)buf.Length - consumed;
                if (available < 4)
                {
                    break;
                }

                int totalLen = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(consumed, 4));
                if (totalLen <= 0 || totalLen > MaxFrameLength)
                {
                    ClientLog.Warn("remoting reader: illegal frame length "
                        + totalLen.ToString(CultureInfo.InvariantCulture)
                        + " from " + conn.Addr + ", dropping "
                        + available.ToString(CultureInfo.InvariantCulture) + " buffered bytes");
                    consumed = (int)buf.Length;
                    break;
                }

                if (available < 4 + totalLen)
                {
                    break; // 半包，继续收
                }

                byte[] frame = new byte[4 + totalLen];
                Buffer.BlockCopy(data, consumed, frame, 0, 4 + totalLen);
                consumed += 4 + totalLen;
                Dispatch(frame, conn.Addr);
            }

            // 把已消费的字节移出缓冲
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

        lock (_connMutex)
        {
            if (_conns.TryGetValue(conn.Addr, out Connection? found) && ReferenceEquals(found, conn))
            {
                _conns.Remove(conn.Addr);
            }
        }

        CloseQuietly(conn);
        FailFast(conn);
        conn.ReaderDone = true;
    }

    private int FailFast(Connection conn)
    {
        List<(Future Future, int Opaque)>? doomed = null;
        foreach (KeyValuePair<int, Future> kv in _respTable)
        {
            if (!ReferenceEquals(kv.Value.Conn, conn))
            {
                continue;
            }

            // TryRemove 决定归属：与读线程/清理线程抢同一 opaque 时只有一方拿到非空值
            if (!_respTable.TryRemove(kv.Key, out Future? removed) || removed is null)
            {
                continue;
            }

            doomed ??= new List<(Future, int)>();
            doomed.Add((removed, kv.Key));
        }

        if (doomed is null)
        {
            return 0;
        }

        foreach ((Future f, int opaque) in doomed)
        {
            var error = new RemotingSendRequestException(f.Addr
                + " connection closed before response, opaque="
                + opaque.ToString(CultureInfo.InvariantCulture));
            InvokeCallback? cb = null;
            lock (f.Done)
            {
                f.Cause = error;
                f.Done.Set();
                if (!f.CallbackFired)
                {
                    f.CallbackFired = true;
                    cb = f.Callback;
                }
            }

            // 与 SweepExpired 同理：回调必须脱离在途表执行
            cb?.Invoke(null, error);
        }

        ClientLog.Warn("remoting reader: connection " + conn.Addr + " closed, "
            + doomed.Count.ToString(CultureInfo.InvariantCulture)
            + " in-flight request(s) failed fast");
        return doomed.Count;
    }

    private void Dispatch(byte[] frame, string from)
    {
        if (!RemotingCommand.TryDecode(frame, out RemotingCommand cmd, out _))
        {
            ClientLog.Warn("remoting reader: drop undecodable frame ("
                + frame.Length.ToString(CultureInfo.InvariantCulture) + " bytes) from " + from);
            return; // 解不出的帧直接丢，不影响其它请求
        }

        if (!_respTable.TryRemove(cmd.Opaque, out Future? future))
        {
            // 在途表里查不到：要么是迟到的响应，要么是 **broker 主动发来的请求**。
            // 后者交给已注册的处理器（典型：事务回查 CHECK_TRANSACTION_STATE=39）。
            if (!cmd.IsResponseType())
            {
                RequestProcessor? proc = null;
                lock (_procLock)
                {
                    _processors.TryGetValue(cmd.Code, out proc);
                }

                if (proc is not null)
                {
                    // 处理器在读线程里执行：异常必须兜住，否则读线程会死掉，
                    // 该连接上其余响应会全部丢失（比丢一条回查严重得多）。
                    try
                    {
                        RemotingCommand? resp = proc(cmd, from);
                        // 处理器产出了响应（典型 326 必须回 SUCCESS/SYSTEM_ERROR，broker 侧是
                        // invokeSync，不回响应它那边会超时）；按请求 opaque 原样回填并标记响应位。
                        if (resp is not null)
                        {
                            resp.Opaque = cmd.Opaque;
                            resp.MarkResponseType();
                            try
                            {
                                SendResponse(from, resp);
                            }
                            catch (Exception se)
                            {
                                ClientLog.Warn("remoting: send processor response for code "
                                    + cmd.Code.ToString(CultureInfo.InvariantCulture) + " failed: "
                                    + se.Message);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        ClientLog.Warn("remoting: processor for request code "
                            + cmd.Code.ToString(CultureInfo.InvariantCulture) + " threw: " + e.Message);
                    }
                }
                else
                {
                    // 未开启事务时 broker 不会发这类请求，属于预期情况，不用 Warn
                    ClientLog.Debug("remoting: no processor for broker request code "
                        + cmd.Code.ToString(CultureInfo.InvariantCulture) + " from " + from);
                }
            }

            return; // 迟到的响应 / 已处理完的 broker 请求
        }

        InvokeCallback? cb = null;
        lock (future.Done)
        {
            future.Response = cmd;
            future.HasResponse = true;
            if (!future.CallbackFired)
            {
                future.CallbackFired = true;
                cb = future.Callback;
            }
        }

        future.Done.Set();
        cb?.Invoke(cmd, null);
    }

    public void RegisterProcessor(int requestCode, RequestProcessor handler)
    {
        lock (_procLock)
        {
            _processors[requestCode] = handler;
        }
    }

    /// <summary>注销按 requestCode 索引的请求处理器。</summary>
    public void UnregisterProcessor(int requestCode)
    {
        lock (_procLock)
        {
            _processors.Remove(requestCode);
        }
    }

    // ---------------------------------------------------------------- 发送

    // 把请求写出去，返回**实际使用的那条连接**：调用方把它记在在途表项上，
    // 连接断开时靠对象引用判断该失败哪些请求（见 FailFast）。
    private Connection SendRequest(string addr, RemotingCommand request)
    {
        // RPC 钩子必须在 Encode() **之前**执行：ACL 钩子把 AccessKey/Signature 写进
        // ExtFields，而签名覆盖的正是「即将上线的这份 ExtFields + Body」。
        CurrentRpcHook()?.DoBeforeRequest(addr, request);
        Connection conn = GetOrCreateConnection(addr);
        byte[] data = request.Encode();
        lock (conn.WriteLock)
        {
            try
            {
                SendAll(conn, data);
            }
            catch (Exception)
            {
                CloseQuietly(conn);
                lock (_connMutex)
                {
                    if (_conns.TryGetValue(addr, out Connection? found) && ReferenceEquals(found, conn))
                    {
                        _conns.Remove(addr);
                    }
                }

                throw new RemotingSendRequestException(addr);
            }
        }

        return conn;
    }

    private static void SendAll(Connection conn, byte[] data)
    {
        if (conn.Tls is not null)
        {
            // SslStream.Write 语义 = 写完全部字节
            conn.Tls.Write(data, 0, data.Length);
            return;
        }

        Socket sock = conn.Sock;
        int sent = 0;
        while (sent < data.Length)
        {
            int n = sock.Send(data, sent, data.Length - sent, SocketFlags.None);
            if (n <= 0)
            {
                throw new SocketException((int)SocketError.ConnectionAborted);
            }

            sent += n;
        }
    }

    private void SendResponse(string addr, RemotingCommand response)
    {
        Connection conn = GetOrCreateConnection(addr);
        byte[] data = response.Encode();
        lock (conn.WriteLock)
        {
            try
            {
                SendAll(conn, data);
            }
            catch (Exception)
            {
                CloseQuietly(conn);
                lock (_connMutex)
                {
                    if (_conns.TryGetValue(addr, out Connection? found) && ReferenceEquals(found, conn))
                    {
                        _conns.Remove(addr);
                    }
                }

                throw new RemotingSendRequestException(addr);
            }
        }
    }

    // ---------------------------------------------------------------- opaque 登记

    // 取得一个**在途请求中唯一**的 opaque，并在同一把锁内登记 future。
    private Future RegisterFutureAcquiringOpaque(RemotingCommand request, InvokeCallback? cb,
        string addr = "", long timeoutMillis = 0)
    {
        var future = new Future
        {
            Callback = cb,
            Addr = addr,
            TimeoutMs = timeoutMillis,
            // timeoutMillis == 0 表示不交给清理线程（同步路径自己等、自己摘）
            DeadlineMs = timeoutMillis > 0 ? Future.MonoNowMs() + timeoutMillis : 0,
        };
        lock (_respTable)
        {
            if (_respTable.ContainsKey(request.Opaque))
            {
                int candidate;
                do
                {
                    candidate = RemotingCommand.NextOpaque();
                }
                while (candidate == request.Opaque || _respTable.ContainsKey(candidate));
                request.Opaque = candidate;
            }

            _respTable[request.Opaque] = future;
        }

        return future;
    }

    private void UnregisterFuture(int opaque) => _respTable.TryRemove(opaque, out _);

    /// <summary>启动超时清理线程（幂等；只有异步调用才需要）。</summary>
    private void EnsureSweeper()
    {
        lock (_sweepLock)
        {
            if (_sweeper is not null)
            {
                return;
            }

            _sweeper = new Timer(_ => SweepExpired(), null, 100, 100);
        }
    }

    private void StopSweeper()
    {
        Timer? timer;
        lock (_sweepLock)
        {
            timer = _sweeper;
            _sweeper = null;
        }

        if (timer is null)
        {
            return;
        }

        // Dispose(waitHandle) 等到正在执行的那一轮结束，等价于 cpp 版 join 清理线程。
        using var done = new ManualResetEvent(false);
        timer.Dispose(done);
        try
        {
            done.WaitOne(TimeSpan.FromSeconds(5));
        }
        catch (ObjectDisposedException)
        {
            // 已完成，忽略
        }
    }

    private void SweepExpired()
    {
        const long GraceMs = 1000;
        long now = Future.MonoNowMs();
        List<(Future Future, int Opaque)>? expired = null;
        foreach (KeyValuePair<int, Future> kv in _respTable)
        {
            Future f = kv.Value;
            if (f.DeadlineMs == 0 || now < f.DeadlineMs + GraceMs)
            {
                continue;
            }

            // TryRemove 决定归属：与读线程同时摘同一 opaque 时只有一方拿到非空值
            if (!_respTable.TryRemove(kv.Key, out Future? removed) || removed is null)
            {
                continue;
            }

            if (expired is null)
            {
                expired = new List<(Future, int)>();
            }

            expired.Add((removed, kv.Key));
        }

        if (expired is null)
        {
            return;
        }

        foreach ((Future f, int opaque) in expired)
        {
            InvokeCallback? cb = null;
            lock (f.Done)
            {
                f.Done.Set();
                if (f.CallbackFired)
                {
                    continue;  // 读线程已经抢先投递（响应正好赶在宽限期内到）
                }

                f.CallbackFired = true;
                cb = f.Callback;
            }

            // 回调必须**脱离在途表**执行：回调里常常还要回到传输层或业务层，
            // 持表回调会和 shutdown 抢同一把锁，也可能自锁。
            cb?.Invoke(null, new RemotingTimeoutException(f.Addr + " async invoke timeout "
                + f.TimeoutMs.ToString(CultureInfo.InvariantCulture) + " ms, opaque="
                + opaque.ToString(CultureInfo.InvariantCulture)));
        }
    }

    // ---------------------------------------------------------------- 公开调用

    public RemotingCommand InvokeSync(string addr, RemotingCommand request, int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _invokeTimeoutMillis;
        long deadline = Future.MonoNowMs() + timeout;
        RemotingCommand response = InvokeOnce(addr, request, timeout);
        if (response.Code != ResponseCode.GoAway)
        {
            return response;
        }

        return RetryAfterGoAway(addr, request, deadline);
    }

    /// <summary>
    /// 单次请求-应答：登记 opaque、写出、等响应或超时。不含 GO_AWAY 判定。
    /// 对端在响应之前断开连接时立刻抛 RemotingSendRequestException（读线程的 FailFast
    /// 把等待方叫醒），不会等满 timeout 再报一个语义错误的超时。
    /// </summary>
    private RemotingCommand InvokeOnce(string addr, RemotingCommand request, int timeout)
    {
        // timeoutMillis 传 0：同步路径自己等、自己摘，不交给清理线程重复判超时。
        Future future = RegisterFutureAcquiringOpaque(request, null, addr, 0);
        int opaque = request.Opaque;
        try
        {
            future.Conn = SendRequest(addr, request);
        }
        catch (Exception)
        {
            UnregisterFuture(opaque);
            throw;
        }

        if (!future.Done.Wait(timeout))
        {
            UnregisterFuture(opaque);
            throw new RemotingTimeoutException(addr + " wait response timeout "
                + timeout.ToString(CultureInfo.InvariantCulture) + " ms, opaque="
                + opaque.ToString(CultureInfo.InvariantCulture));
        }

        if (!future.HasResponse)
        {
            UnregisterFuture(opaque);
            throw future.Cause is not null
                ? new RemotingSendRequestException(future.Cause.Message)
                : new RemotingSendRequestException(addr + " connection closed before response, opaque="
                    + opaque.ToString(CultureInfo.InvariantCulture));
        }

        try
        {
            CurrentRpcHook()?.DoAfterResponse(addr, request, future.Response);
        }
        catch (Exception e)
        {
            ClientLog.Warn("rpc hook doAfterResponse failed: " + e.Message);
        }

        return future.Response;
    }

    private RemotingCommand RetryAfterGoAway(string addr, RemotingCommand request, long deadlineMs)
    {
        if (!EnableReconnectForGoAway)
        {
            throw new RemotingSendRequestException("Receive GO_AWAY from channel " + addr);
        }

        ClientLog.Info("remoting: receive GO_AWAY from " + addr + ", reconnect and retry once");
        CloseChannel(addr);
        long remaining = deadlineMs - Future.MonoNowMs();
        if (remaining <= 0)
        {
            throw new RemotingTimeoutException(addr + " GO_AWAY retry budget exhausted, opaque="
                + request.Opaque.ToString(CultureInfo.InvariantCulture));
        }

        RemotingCommand retry = CopyForRetry(request);
        RemotingCommand response = InvokeOnce(addr, retry,
            (int)Math.Min(remaining, int.MaxValue));
        if (response.Code == ResponseCode.GoAway)
        {
            throw new RemotingSendRequestException(
                "Receive GO_AWAY twice in request from channel " + addr);
        }

        return response;
    }

    private static RemotingCommand CopyForRetry(RemotingCommand request)
    {
        return new RemotingCommand
        {
            Code = request.Code,
            Language = request.Language,
            Version = request.Version,
            Opaque = RemotingCommand.NextOpaque(),
            Flag = request.Flag,
            Remark = request.Remark,
            HasRemark = request.HasRemark,
            ExtFields = new PropertyMap(request.ExtFields),
            CustomHeader = request.CustomHeader,
            Body = request.Body,
            HasBody = request.HasBody,
            SerializeTypeCurrentRpc = request.SerializeTypeCurrentRpc,
        };
    }

    public void InvokeAsync(string addr, RemotingCommand request, InvokeCallback callback, int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _invokeTimeoutMillis;
        EnsureSweeper();
        long deadline = Future.MonoNowMs() + timeout;
        RemotingCommand original = CopyForRetry(request);
        original.Opaque = request.Opaque;
        Future future = RegisterFutureAcquiringOpaque(request, (response, error) =>
        {
            if (response is null || error is not null || response.Code != ResponseCode.GoAway)
            {
                callback(response, error);
                return;
            }

            System.Threading.Tasks.Task.Run(() =>
            {
                try
                {
                    callback(RetryAfterGoAway(addr, original, deadline), null);
                }
                catch (Exception retryError)
                {
                    // 回调契约：失败也只能通过 error 表达，不能把异常抛回读线程
                    callback(null, retryError);
                }
            });
        }, addr, timeout);
        int opaque = request.Opaque;
        try
        {
            future.Conn = SendRequest(addr, request);
        }
        catch (Exception)
        {
            UnregisterFuture(opaque);
            throw;
        }
    }

    public void InvokeOneway(string addr, RemotingCommand request)
    {
        request.MarkOnewayRpc();
        SendRequest(addr, request);
    }

    /// <summary>连接是否存活（探活）。</summary>
    public bool IsChannelWritable(string addr)
    {
        lock (_connMutex)
        {
            return _conns.TryGetValue(addr, out Connection? c) && c.Sock is { Connected: true };
        }
    }

    private static void CloseQuietly(Connection conn)
    {
        // WriteLock 同时也是 socket 生命周期的锁：写路径失败时持锁关掉它，读线程退出时也走
        // 这里，于是既不会重复 Dispose（SslStream/Socket 二次 dispose 只是没意义，把已归还
        // 的句柄再关一次才真危险），也不会出现"一路在 Send、另一路把 socket 抽走"的窗口。
        lock (conn.WriteLock)
        {
            if (conn.Sock is null)
            {
                return;
            }

            try
            {
                conn.Tls?.Dispose();
            }
            catch
            {
                // dispose 失败忽略
            }

            try
            {
                conn.Sock.Dispose();
            }
            catch
            {
                // dispose 失败忽略
            }

            conn.Sock = null!;
        }
    }

    /// <summary>主动关闭单条连接；读线程会自行退出并清理。</summary>
    public void CloseChannel(string addr)
    {
        Connection? conn;
        lock (_connMutex)
        {
            if (!_conns.Remove(addr, out conn))
            {
                return;
            }
        }

        // 关闭 socket 会让读线程的 Receive 立即抛异常并自行退出
        CloseQuietly(conn);
    }

    /// <summary>预留：NameServer 地址列表由上层（MQClientInstance）维护。</summary>
    public void UpdateNameServerAddressList(IReadOnlyList<string> addrs)
    {
        // 由上层 MQClientInstance 维护 NameServer 列表；传输层不持有
    }

    private void PruneThreads()
    {
        lock (_threadMutex)
        {
            for (int i = _threads.Count - 1; i >= 0; --i)
            {
                if (_threads[i].conn.ReaderDone)
                {
                    _threads.RemoveAt(i);
                }
            }
        }
    }

    /// <summary>关闭全部连接并回收读线程；可重复调用。</summary>
    public void Shutdown()
    {
        if (!_running)
        {
            return; // 已关闭
        }

        _running = false;
        // 先停清理线程：它会在别的线程上触发回调，必须早于关连接退出，
        // 否则 shutdown 之后还可能冒出一个超时回调。
        StopSweeper();
        List<Connection> all;
        lock (_connMutex)
        {
            all = new List<Connection>(_conns.Values);
            _conns.Clear();
        }

        // 关闭 socket 让阻塞中的 reader 立刻退出
        foreach (Connection c in all)
        {
            CloseQuietly(c);
        }

        lock (_threadMutex)
        {
            foreach ((Connection _, Thread t) in _threads)
            {
                if (t.IsAlive)
                {
                    t.Join(2000);
                }
            }

            _threads.Clear();
        }

        _respTable.Clear();
    }

    public void Dispose() => Shutdown();
}
