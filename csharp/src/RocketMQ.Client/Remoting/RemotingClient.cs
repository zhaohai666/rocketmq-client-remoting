// Provides: connection management (lazy connect + reuse + TCP_NODELAY), invokeSync /
// invokeAsync / invokeOneway, opaque-to-response callback dispatch, timeout control, and
// connection liveness probing.
//
// Threading model (one reader thread per connection):
//   - Calling thread: encode -> lock the write -> Socket.Send everything; then waits on an
//     event inside invokeSync
//   - Reader thread: blocking Receive -> frame by totalLength(4) -> decode ->
//     look up the future in the response table by opaque -> wake the calling thread /
//     fire the callback
//   - A blocking Receive throws and exits immediately when the socket is closed by another
//     thread, so no select(300ms)-style polling is needed; closing the connection reaps the
//     reader thread promptly.
//
// Wire format: totalLength(4) | headerLength(4) | header | body
//   totalLength = 4 + headerLength + bodyLength (i.e. all bytes after the first 4 bytes)
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

/// <summary>TCP transport client with timeout control.</summary>
public sealed class RemotingClient : IDisposable
{
    public delegate void InvokeCallback(RemotingCommand? response, Exception? error);

    public delegate RemotingCommand? RequestProcessor(RemotingCommand request, string addr);

    public const int MaxFrameLength = 16 * 1024 * 1024;

    /// <summary>Default idle deadline for a connection that carries no traffic at all.</summary>
    public const long DefaultChannelIdleTimeoutMillis = 120 * 1000;

    private sealed class Connection
    {
        public required string Addr;
        public Socket Sock = null!;
        // Write-side mutex. WARNING: its purpose is not only preventing the bytes of two
        // requests from interleaving; it also pins down the concurrency contract of
        // SslStream: the runtime's SslStream uses the _nestedRead / _nestedWrite flags, which
        // are **mutually independent**, to admit I/O (see System.Net.Security/SslStream.IO.cs),
        // i.e. it only supports one read lane plus one write lane; same-lane reentrancy throws
        // invalidnestedcall directly.
        // This class has only one reader thread per connection, and with this write lock it
        // lands exactly in the supported shape — removing it or starting a second reader
        // thread would push the TLS connection into undefined behavior.
        public readonly object WriteLock = new();
        public volatile bool ReaderDone;
        // When this connection last carried bytes in either direction, on the same monotonic
        // clock as Future.MonoNowMs. Zero until the connection is registered: an entry still
        // being established is not an idle one.
        public long LastActivityMs;
        // TLS session (non-null when TLS is enabled; reads/writes go through SslStream)
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
    private long _channelIdleTimeoutMs = DefaultChannelIdleTimeoutMillis;

    // Broker-initiated request processor table: requestCode -> handler. Only used for
    // commands like transaction check-back (CHECK_TRANSACTION_STATE=39), i.e. "the server
    // comes back to me".
    private readonly Dictionary<int, RequestProcessor> _processors = new();
    private readonly object _procLock = new();

    // Reader-thread ledger: reader threads set ReaderDone on exit and are reclaimed by
    // PruneThreads, avoiding unbounded handle accumulation.
    private readonly object _threadMutex = new();
    private readonly List<(Connection conn, Thread thread)> _threads = new();

    // RPC hook (ACL etc.). Registration happens at the start phase; the send path only
    // takes one reference under the lock and **never calls the hook while holding the
    // lock** (the hook needs to do signature computation).
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

    /// <summary>The currently registered request hook (null when none is registered). For
    /// scenarios like trace startup that need to read it in order to "append a segment to an
    /// existing chain" — swapping hooks on the single-slot transport requires the old value to
    /// compose (the caller is responsible for leaving no gap when unregistering then
    /// registering the chain).</summary>
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
    /// Strictly validated hostname/SAN matching: when host is an IP, compare against IP SANs,
    /// otherwise compare against DNS SANs (exact or the single-level wildcard
    /// <c>*.example</c>). The CustomRootTrust chain does not check names, so this step must be
    /// added inside RemoteCertificateValidationCallback.
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
                // EnumerateIPAddresses() returns IEnumerable<IPAddress> (.NET 10).
                // Normalized comparison: also handles IPv4-mapped forms like ::ffff:127.0.0.1.
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
                    // single-level wildcard: *.example.com matches a.example.com but not
                    // a.b.example.com
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
        // Fallback: when there is no SAN extension, fall back to CN (deprecated by RFC 6125
        // but still useful for old certificates)
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

    /// <summary>
    /// How long a connection may carry no traffic at all before the transport closes it. A value
    /// that is not positive restores the default rather than switching the sweep off: the sweep
    /// is always installed, so an unsettable deadline would otherwise mean "reap immediately".
    /// </summary>
    public long ChannelIdleTimeoutMillis
    {
        get => _channelIdleTimeoutMs;
        set => _channelIdleTimeoutMs = value > 0 ? value : DefaultChannelIdleTimeoutMillis;
    }

    /// <summary>
    /// Whether a synchronous wait that runs out with the response still missing also takes the
    /// connection down. On by default: nothing else will ever deliver that response, so the stream
    /// is not trustworthy for the requests behind it.
    /// </summary>
    public bool CloseSocketOnSyncTimeout { get; set; } = true;

    /// <summary>
    /// Close every connection idle strictly longer than ChannelIdleTimeoutMillis and return how
    /// many went. The transport's own sweeper calls this; the method is public so a test can drive
    /// one pass.
    /// </summary>
    public int SweepIdleConnections()
    {
        long limit = _channelIdleTimeoutMs;
        long now = Future.MonoNowMs();
        List<Connection>? idle = null;
        lock (_connMutex)
        {
            foreach ((string addr, Connection conn) in _conns)
            {
                long last = conn.LastActivityMs;
                if (last == 0 || now - last <= limit)
                {
                    continue;
                }

                idle ??= new List<Connection>();
                idle.Add(conn);
            }

            if (idle is not null)
            {
                foreach (Connection conn in idle)
                {
                    _conns.Remove(conn.Addr);
                }
            }
        }

        if (idle is null)
        {
            return 0;
        }

        foreach (Connection conn in idle)
        {
            ClientLog.Warn("remoting: connection to " + conn.Addr + " idle for "
                + (now - conn.LastActivityMs).ToString(CultureInfo.InvariantCulture)
                + " ms (limit " + limit.ToString(CultureInfo.InvariantCulture) + " ms), closing it");
            // The same route a peer disconnect takes: the reader's Receive throws, it exits,
            // drops the table entry and failFasts this connection's in-flight requests.
            CloseFromOutsideWriteLane(conn);
        }

        return idle.Count;
    }

    public int ConnectTimeoutMillis => _connectTimeoutMillis;

    public int InvokeTimeoutMillis => _invokeTimeoutMillis;

    /// <summary>Current number of active connections (for tests/diagnostics).</summary>
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

    // ---------------------------------------------------------------- connecting

    private static Socket ConnectWithTimeout(string host, int port, int timeoutMillis, out string error)
    {
        error = string.Empty;
        // IPv6 literals go straight to IPAddress.Parse; hostnames go through DNS
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
                // ConnectAsync + timeout: non-blocking connect, take the result within a limit
                using var cts = new CancellationTokenSource(timeoutMillis);
                Task connectTask = sock.ConnectAsync(ip, port, cts.Token).AsTask();
                try
                {
                    connectTask.Wait();
                }
                catch (AggregateException) when (cts.IsCancellationRequested)
                {
                    // connection timeout
                    try
                    {
                        sock.Dispose();
                    }
                    catch
                    {
                        // dispose failure ignored
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
                    // dispose failure ignored
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
            sock.NoDelay = true; // TCP_NODELAY: requests need low latency
        }
        catch
        {
            // not supported on some platforms; ignore
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

    /// <summary>Splits "host:port", supporting the IPv6 [::1]:10911 form.</summary>
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

        // IPv4 / hostname: the port is after the last ':'
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
        // No more connections after shutdown: the reader thread fires callbacks when it exits,
        // and if a callback re-enters the transport layer it would add new threads to the
        // ledger while Shutdown holds _threadMutex waiting to Join.
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

        // Connecting is done outside the lock so a slow connect does not block other addresses
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
                        // PEM(cert+key) -> X509Certificate2. WARNING: macOS's SslStream does
                        // not recognize the **ephemeral** private key produced by
                        // CreateFromPemFile; it must first be exported to PFX and loaded with
                        // X509CertificateLoader (a PKCS#12 container), otherwise the client
                        // certificate cannot be presented.
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
                                // Failure reasons go to the log: chain statuses are printed one
                                // by one, otherwise all you see is a single
                                // "rejected by callback" and cannot tell a chain failure from
                                // a name failure.
                                var statuses = string.Join(",", custom.ChainStatus.Select(s => s.Status));
                                ClientLog.Warn("tls strict: chain build failed for " + peer.Subject
                                               + " (SAN=" + CertHasSan(peer) + "), status=[" + statuses + "]");
                                return false;
                            }
                            // Hostname/SAN validation (the CustomRootTrust chain includes no
                            // name check): when host is an IP, compare IP SANs, otherwise
                            // compare DNS SANs (supports the *.example wildcard).
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
                        (sender, cert, chain, errors) => true);   // test mode: trust everything
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
            // Concurrent connects: if someone else already won, discard our own socket
            if (_conns.TryGetValue(addr, out Connection? winner) && winner.Sock is { Connected: true })
            {
                try
                {
                    sock.Dispose();
                }
                catch
                {
                    // dispose failure ignored
                }

                return winner;
            }

            _conns[addr] = conn;
        }

        // The connection is registered: the idle clock starts here, and the sweeper that reads
        // that clock has to be running whatever call shape created this connection.
        conn.LastActivityMs = Future.MonoNowMs();
        EnsureSweeper();

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

    // ---------------------------------------------------------------- read loop

    private void ReadLoop(Connection conn)
    {
        var buf = new MemoryStream();
        byte[] chunk = new byte[65536];
        Socket sock = conn.Sock;
        // Set when the byte stream stopped lining up with the frame boundaries. Nothing that
        // arrives after that can be trusted for any later request, so the reader exits and the
        // normal teardown below takes the connection down -- the same route a peer disconnect
        // takes.
        bool streamUntrustworthy = false;
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
                // shutdown()/closeChannel() closing the socket makes Receive throw and exit
                // immediately; this is a normal exit path, so log it at DEBUG (invisible under
                // the default INFO level, avoiding "spurious exception on exit" noise).
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

            // Bytes arrived: the connection is not the idle one, whatever happens to them next.
            conn.LastActivityMs = Future.MonoNowMs();
            buf.Write(chunk, 0, n);
            buf.Flush();

            // Split frames by the totalLength prefix; sticky packets / partial packets are
            // both handled here
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
                    // The length prefix itself is out of range, so the bytes on the stream no
                    // longer line up with the frame boundaries. This is the same event as an
                    // undecodable frame one level earlier, and it is reported once, here: what
                    // follows cannot be attributed to any request, so the connection goes.
                    ClientLog.Error("remoting reader: illegal frame length "
                        + totalLen.ToString(CultureInfo.InvariantCulture)
                        + " from " + conn.Addr + ", closing the connection");
                    streamUntrustworthy = true;
                    break;
                }

                if (available < 4 + totalLen)
                {
                    break; // partial packet, keep receiving
                }

                byte[] frame = new byte[4 + totalLen];
                Buffer.BlockCopy(data, consumed, frame, 0, 4 + totalLen);
                consumed += 4 + totalLen;
                if (!Dispatch(frame, conn.Addr))
                {
                    streamUntrustworthy = true;
                    break;
                }
            }

            // move the consumed bytes out of the buffer
            if (consumed > 0)
            {
                int rest = (int)buf.Length - consumed;
                if (rest > 0)
                {
                    Buffer.BlockCopy(data, consumed, data, 0, rest);
                }

                buf.SetLength(rest);
            }

            if (streamUntrustworthy)
            {
                break;
            }
        }

        lock (_connMutex)
        {
            if (_conns.TryGetValue(conn.Addr, out Connection? found) && ReferenceEquals(found, conn))
            {
                _conns.Remove(conn.Addr);
            }
        }

        CloseFromOutsideWriteLane(conn);
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

            // TryRemove decides ownership: when racing the reader/sweeper thread for the
            // same opaque, only one side gets a non-null value
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

            // Same as SweepExpired: callbacks must run outside the in-flight table
            cb?.Invoke(null, error);
        }

        ClientLog.Warn("remoting reader: connection " + conn.Addr + " closed, "
            + doomed.Count.ToString(CultureInfo.InvariantCulture)
            + " in-flight request(s) failed fast");
        return doomed.Count;
    }

    /// <summary>
    /// Hand one complete frame to its owner. Returns whether the connection is still usable, i.e.
    /// false when this frame took the connection down and the reader must stop reading from it.
    /// </summary>
    private bool Dispatch(byte[] frame, string from)
    {
        if (!RemotingCommand.TryDecode(frame, out RemotingCommand cmd, out _))
        {
            // The bytes no longer line up with the frame boundaries, so nothing that arrives after
            // this frame on this stream can be trusted for any later request: report the peer and
            // take the connection down through the same route a peer disconnect takes.
            ClientLog.Error("remoting reader: cannot decode frame from " + from + " ("
                + frame.Length.ToString(CultureInfo.InvariantCulture) + " bytes), closing the connection");
            return false;
        }

        if (!_respTable.TryRemove(cmd.Opaque, out Future? future))
        {
            if (cmd.IsResponseType())
            {
                // A response whose opaque is no longer in the in-flight table: the request was
                // already answered, timed out or failed fast. Dropping it is right, saying so is
                // what makes a double-answering peer visible; the frame itself was well-formed, so
                // the connection stays up for the requests still on it.
                ClientLog.Warn("remoting reader: response from " + from
                    + " does not match any in-flight request (opaque="
                    + cmd.Opaque.ToString(CultureInfo.InvariantCulture) + "), dropped");
                return true;
            }

            // Missing from the in-flight table and not a response: a **broker-initiated request**.
            // The latter is taken over by the registered processor (typically the
            // transaction check-back CHECK_TRANSACTION_STATE=39).
            RequestProcessor? proc = null;
            lock (_procLock)
            {
                _processors.TryGetValue(cmd.Code, out proc);
            }

            if (proc is not null)
            {
                // The processor runs in the reader thread: exceptions must be caught,
                // otherwise the reader thread dies and all other responses on that
                // connection are lost (far worse than losing one check-back).
                try
                {
                    RemotingCommand? resp = proc(cmd, from);
                    // The processor produced a response (typically 326 must reply
                    // SUCCESS/SYSTEM_ERROR; on the broker side it is invokeSync, and it
                    // times out if no response is sent); echo back the request's opaque
                    // verbatim and set the response bit.
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
                // When transactions are not enabled the broker never sends such requests,
                // so this is expected; no Warn needed
                ClientLog.Debug("remoting: no processor for broker request code "
                    + cmd.Code.ToString(CultureInfo.InvariantCulture) + " from " + from);
            }

            return true; // late response / already-handled broker request
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
        return true;
    }

    public void RegisterProcessor(int requestCode, RequestProcessor handler)
    {
        lock (_procLock)
        {
            _processors[requestCode] = handler;
        }
    }

    /// <summary>Unregisters the request processor indexed by requestCode.</summary>
    public void UnregisterProcessor(int requestCode)
    {
        lock (_procLock)
        {
            _processors.Remove(requestCode);
        }
    }

    // ---------------------------------------------------------------- sending

    // Writes the request out, returning **the connection actually used**: the caller records
    // it on the in-flight table entry, and when the connection drops that object reference
    // determines which requests should fail (see FailFast).
    private Connection SendRequest(string addr, RemotingCommand request)
    {
        // The RPC hook must run **before** Encode(): the ACL hook writes AccessKey/Signature
        // into ExtFields, and the signature covers exactly "this ExtFields + Body about to go
        // on the wire".
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

        // The bytes went out: the connection just did real work, so its idle clock restarts here.
        conn.LastActivityMs = Future.MonoNowMs();
        return conn;
    }

    private static void SendAll(Connection conn, byte[] data)
    {
        if (conn.Tls is not null)
        {
            // SslStream.Write semantics = write all bytes
            conn.Tls.Write(data, 0, data.Length);
            return;
        }

        Socket? sock = conn.Sock;
        if (sock is null)
        {
            // A closer outside the write lane unpublished this connection between the lookup and
            // the lock: the table entry is gone, so this send has no socket to write to.
            throw new SocketException((int)SocketError.NotConnected);
        }

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

        conn.LastActivityMs = Future.MonoNowMs();
    }

    // ---------------------------------------------------------------- opaque registration

    // Acquires an opaque that is **unique among in-flight requests** and registers the future
    // under the same lock.
    private Future RegisterFutureAcquiringOpaque(RemotingCommand request, InvokeCallback? cb,
        string addr = "", long timeoutMillis = 0)
    {
        var future = new Future
        {
            Callback = cb,
            Addr = addr,
            TimeoutMs = timeoutMillis,
            // timeoutMillis == 0 means "do not hand to the sweeper" (the sync path waits and
            // removes it itself)
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

    /// <summary>
    /// Starts the sweeper thread (idempotent). It reclaims expired in-flight requests, which only
    /// async calls need, and sweeps idle connections, which every call shape needs -- a client
    /// that only ever uses InvokeSync / InvokeOneway still has to see its quiet connections
    /// closed, so the thread is installed when the first connection is registered.
    /// </summary>
    private void EnsureSweeper()
    {
        lock (_sweepLock)
        {
            if (_sweeper is not null)
            {
                return;
            }

            _sweeper = new Timer(_ =>
            {
                SweepExpired();
                SweepIdleConnections();
            }, null, 100, 100);
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

        // Dispose(waitHandle) waits for the in-flight round to finish, equivalent to joining
        // the sweeper thread.
        using var done = new ManualResetEvent(false);
        timer.Dispose(done);
        try
        {
            done.WaitOne(TimeSpan.FromSeconds(5));
        }
        catch (ObjectDisposedException)
        {
            // already finished; ignore
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

            // TryRemove decides ownership: when removing the same opaque concurrently with the
        // reader thread, only one side gets a non-null value
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
                    continue;  // the reader thread already delivered first (the response
                              // arrived just within the grace period)
                }

                f.CallbackFired = true;
                cb = f.Callback;
            }

            // Callbacks must run **outside the in-flight table**: callbacks often go back to
            // the transport or business layer, and invoking while holding the table would
            // race with shutdown for the same lock and may self-deadlock.
            //
            // One event, one log line: without this a swept request is invisible, because whether
            // the business callback logs anything is out of the transport's hands.
            ClientLog.Warn("remoting: in-flight request to " + f.Addr + " expired (opaque="
                + opaque.ToString(CultureInfo.InvariantCulture) + ", timeout="
                + f.TimeoutMs.ToString(CultureInfo.InvariantCulture) + " ms), failed with a timeout");
            cb?.Invoke(null, new RemotingTimeoutException(f.Addr + " async invoke timeout "
                + f.TimeoutMs.ToString(CultureInfo.InvariantCulture) + " ms, opaque="
                + opaque.ToString(CultureInfo.InvariantCulture)));
        }
    }

    // ---------------------------------------------------------------- public calls

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
    /// Single request-answer: register opaque, write out, wait for a response or timeout. No
    /// GO_AWAY judgment. When the peer closes the connection before responding, it throws
    /// RemotingSendRequestException immediately (the reader thread's FailFast wakes the
    /// waiter); it does not wait for the full timeout to report a semantically wrong timeout.
    /// </summary>
    private RemotingCommand InvokeOnce(string addr, RemotingCommand request, int timeout)
    {
        // timeoutMillis of 0: the sync path waits and removes the future itself, not handing
        // duplicate timeout judgment to the sweeper.
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
            // A sync wait that ran out with the response still missing has no evidence the peer is
            // readable at all: the frame may be lost on the way, and nothing else will ever deliver
            // it. Both floors below measure the budget the wait was handed when it began -- this
            // port does not subtract the connect and write cost, so that budget IS the timeout and
            // the second floor is out of reach for any positive value. A positive timeout therefore
            // always closes; zero is the one shape that keeps the socket, and such a request was
            // never going to be answered anyway.
            long leftMillis = timeout;
            if (CloseSocketOnSyncTimeout && (leftMillis > 100 || leftMillis > timeout / 4))
            {
                ClientLog.Warn("remoting: sync wait to " + addr + " timed out (opaque="
                    + opaque.ToString(CultureInfo.InvariantCulture) + ", timeout="
                    + timeout.ToString(CultureInfo.InvariantCulture) + " ms), closing the connection");
                CloseChannel(addr);
            }

            ClientLog.Warn("remoting: wait response from " + addr + " timeout, opaque="
                + opaque.ToString(CultureInfo.InvariantCulture) + ", "
                + timeout.ToString(CultureInfo.InvariantCulture) + " ms");
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
                    // Callback contract: failures may only be expressed through error;
                    // exceptions must not be thrown back into the reader thread
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

    /// <summary>
    /// Whether this address can take a write right now. The question is about the outbound side,
    /// not about having connected: an address with no connection is writable (the transport
    /// connects on demand), and only a live connection whose send buffer is full is not.
    /// </summary>
    public bool IsChannelWritable(string addr)
    {
        Socket? sock;
        lock (_connMutex)
        {
            sock = _conns.TryGetValue(addr, out Connection? c) ? c.Sock : null;
        }

        if (sock is null)
        {
            // "Nothing is open yet" is not "this cannot take data": the next send simply creates
            // the connection. Same for a table entry whose socket is already gone -- the next send
            // replaces it.
            return true;
        }

        // A live socket: ask the kernel whether it would take a write right now. This is a snapshot
        // of the send buffer, not a queue-depth watermark, so a live connection that is merely slow
        // but currently drained still reports writable -- as does one whose peer just died (the
        // read path is what notices that and takes the channel down). On a TLS connection the probe
        // sees the underlying socket only, not the crypto layer's own buffers.
        try
        {
            return sock.Poll(0, SelectMode.SelectWrite);
        }
        catch (SocketException)
        {
            // The probe itself failed: report writable and let the write path (send failure ->
            // close the connection) decide, rather than refusing the send.
            return true;
        }
        catch (ObjectDisposedException)
        {
            return true;
        }
    }

    private static void CloseQuietly(Connection conn)
    {
        // WriteLock is also the lock for the socket lifetime: the write path closes it under
        // the lock on failure, so there is neither a duplicate Dispose (a second dispose of
        // SslStream/Socket is merely pointless; closing an already-returned handle again is the
        // truly dangerous part) nor a window where "one lane is Sending while another yanks the
        // socket away". Only the write lane, which already holds the lock, comes through here --
        // everything outside it uses CloseFromOutsideWriteLane.
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
                // dispose failure ignored
            }

            try
            {
                conn.Sock.Dispose();
            }
            catch
            {
                // dispose failure ignored
            }

            conn.Sock = null!;
        }
    }

    /// <summary>
    /// Close a connection from a thread that does **not** own the write lane: shutdown and the idle
    /// sweep both come from outside, where taking WriteLock would mean waiting for a thread parked
    /// inside a blocking Send -- an unbounded hang, since the peer that stopped reading is the very
    /// reason the send is parked. So unpublish the socket first (the write path then sees no socket
    /// and the reader sees an invalid one), then wake whatever is blocked on it: the shutdown is
    /// what turns a parked send or receive into an exception, and the in-flight requests on this
    /// connection are failFasted by the reader thread's exit path as usual.
    /// </summary>
    private static void CloseFromOutsideWriteLane(Connection conn)
    {
        Socket? sock = Interlocked.Exchange(ref conn.Sock, null!);
        if (sock is null)
        {
            return; // already unpublished
        }

        try
        {
            sock.Shutdown(SocketShutdown.Both);
        }
        catch
        {
            // already disconnected
        }

        try
        {
            conn.Tls?.Dispose();
        }
        catch
        {
            // dispose failure ignored
        }

        try
        {
            sock.Dispose();
        }
        catch
        {
            // dispose failure ignored
        }
    }

    /// <summary>Actively closes a single connection; the reader thread exits and cleans up on its own.</summary>
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

        // Closing the socket makes the reader thread's Receive throw immediately and exit on its own
        CloseFromOutsideWriteLane(conn);
    }

    /// <summary>Reserved: the NameServer address list is maintained by the upper layer (MQClientInstance).</summary>
    public void UpdateNameServerAddressList(IReadOnlyList<string> addrs)
    {
        // The NameServer list is maintained by the upper-layer MQClientInstance; the transport
        // layer does not hold it
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

    /// <summary>Closes all connections and reaps reader threads; safe to call repeatedly.</summary>
    public void Shutdown()
    {
        if (!_running)
        {
            return; // already closed
        }

        _running = false;
        // Stop the sweeper first: it fires callbacks on other threads and must exit before
        // closing connections, otherwise a timeout callback could still pop up after shutdown.
        StopSweeper();
        List<Connection> all;
        lock (_connMutex)
        {
            all = new List<Connection>(_conns.Values);
            _conns.Clear();
        }

        // Closing the socket makes blocked readers exit immediately
        foreach (Connection c in all)
        {
            CloseFromOutsideWriteLane(c);
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
