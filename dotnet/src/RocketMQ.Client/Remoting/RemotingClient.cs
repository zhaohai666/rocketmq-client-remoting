// Socket 长连接客户端（对应 org.apache.rocketmq.remoting.netty.NettyRemotingClient 的核心能力）。
//
// 提供：连接管理（惰性建连 + 复用 + TCP_NODELAY）、invokeSync / invokeAsync /
// invokeOneway、opaque 到响应回调的分发、超时控制、连接状态探活。
//
// 线程模型（与 Python/C++ 参考实现一致，读线程每连接一个）：
//   - 调用线程：encode -> 锁写 -> Socket.Send 全量；随后在 invokeSync 里等事件
//   - 读线程：阻塞 Receive -> 按 totalLength(4) 分帧 -> decode ->
//             按 opaque 从响应表取出 future -> 唤醒调用线程 / 触发回调
//   - .NET 的阻塞 Receive 在 socket 被另一线程 Close 时立即抛异常退出，
//     因此不需要 C++ 那套 select(300ms) 轮询，关连接即可及时回收读线程。
//
// 线格式：totalLength(4) | headerLength(4) | header | body
//   totalLength = 4 + headerLength + bodyLength（即首 4 字节之后的所有字节数）
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Remoting;

/// <summary>带超时控制的 TCP 传输客户端。</summary>
public sealed class RemotingClient : IDisposable
{
    /// <summary>响应到达时在读线程中触发的回调。实现需自行保证线程安全。</summary>
    public delegate void InvokeCallback(RemotingCommand response);

    /// <summary>单帧上限（与 Java NettyRemotingClient 的 16MB 限制一致）。</summary>
    public const int MaxFrameLength = 16 * 1024 * 1024;

    private sealed class Connection
    {
        public required string Addr;
        public Socket Sock = null!;
        public readonly object WriteLock = new();
        public volatile bool ReaderDone;
    }

    private sealed class Future
    {
        public readonly ManualResetEventSlim Done = new(false);
        public RemotingCommand Response = new();
        public InvokeCallback? Callback;
    }

    private volatile bool _running = true;
    private readonly int _connectTimeoutMillis;
    private readonly int _invokeTimeoutMillis;

    private readonly object _connMutex = new();
    private readonly Dictionary<string, Connection> _conns = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<int, Future> _respTable = new();

    // 读线程账本：读线程自行结束后置 ReaderDone，由 PruneThreads 回收，避免句柄无限堆积。
    private readonly object _threadMutex = new();
    private readonly List<(Connection conn, Thread thread)> _threads = new();

    public RemotingClient(int connectTimeoutMillis = 3000, int invokeTimeoutMillis = 15000)
    {
        _connectTimeoutMillis = connectTimeoutMillis;
        _invokeTimeoutMillis = invokeTimeoutMillis;
    }

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
        Socket sock = ConnectWithTimeout(host, port, _connectTimeoutMillis, out string error);
        if (sock is null)
        {
            throw new RemotingConnectException(error);
        }

        var conn = new Connection { Addr = addr, Sock = sock };

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

        // 每连接一个读线程，命名后日志里能直接看出是哪条链路（对应 Java 的
        // NettyClientWorkerThread；Java 用线程池复用，这里是一连接一线程，故带上地址）。
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
                n = sock.Receive(chunk, chunk.Length, SocketFlags.None);
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
                // n == 0 对端正常关闭。Java 侧 Netty 的 channelInactive 会打一行，
                // 但正常 shutdown 也会走到这里，为免默认 INFO 下变成噪声同样降到 DEBUG。
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
                    // 真正的协议异常（对应 Java NettyRemotingAbstract 的 "decode message length error"）：
                    // 必须可见，否则会表现为"请求莫名超时"而没人知道原因。
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

        conn.ReaderDone = true;
        // 把自己从连接表摘掉（避免留下失效条目）
        lock (_connMutex)
        {
            if (_conns.TryGetValue(conn.Addr, out Connection? found) && ReferenceEquals(found, conn))
            {
                _conns.Remove(conn.Addr);
            }
        }
    }

    private void Dispatch(byte[] frame, string from)
    {
        if (!RemotingCommand.TryDecode(frame, out RemotingCommand cmd, out _))
        {
            // Java 侧这里同样是 warn（解码失败意味着这条响应永久丢失，调用方只会看到超时）。
            ClientLog.Warn("remoting reader: drop undecodable frame ("
                + frame.Length.ToString(CultureInfo.InvariantCulture) + " bytes) from " + from);
            return; // 解不出的帧直接丢，不影响其它请求
        }

        if (!_respTable.TryRemove(cmd.Opaque, out Future? future))
        {
            return; // 已超时的请求（响应来晚了）
        }

        InvokeCallback? cb;
        lock (future.Done)
        {
            future.Response = cmd;
            cb = future.Callback;
        }

        future.Done.Set();
        cb?.Invoke(cmd);
    }

    // ---------------------------------------------------------------- 发送

    private void SendRequest(string addr, RemotingCommand request)
    {
        Connection conn = GetOrCreateConnection(addr);
        byte[] data = request.Encode();
        lock (conn.WriteLock)
        {
            try
            {
                SendAll(conn.Sock, data);
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

    private static void SendAll(Socket sock, byte[] data)
    {
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

    // ---------------------------------------------------------------- opaque 登记

    // 取得一个**在途请求中唯一**的 opaque，并在同一把锁内登记 future。
    //
    // 为什么不能简单用 "opaque == 0 就重分配"：RemotingCommand 的 opaque 计数器从 0
    // 开始，所以 createRequestCommand() 产出的**第一个**请求 opaque 就是 0，把 0 当
    // "未设置"会把它改掉，导致调用方与服务端回填的 opaque 不一致（调用方拿不到响应）。
    // 这里只在**真的与在途请求冲突**时才重分配，其余情况原样保留（含 0），与 Java
    // NettyRemotingClient 从不改写调用方 opaque 的行为一致。
    private Future RegisterFutureAcquiringOpaque(RemotingCommand request, InvokeCallback? cb)
    {
        var future = new Future { Callback = cb };
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

    // ---------------------------------------------------------------- 公开调用

    /// <summary>
    /// 同步调用：等到响应或超时。超时抛 RemotingTimeoutException；
    /// 建连失败抛 RemotingConnectException；发送失败抛 RemotingSendRequestException。
    /// timeoutMillis &lt; 0 表示使用 invokeTimeoutMillis。
    /// </summary>
    public RemotingCommand InvokeSync(string addr, RemotingCommand request, int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _invokeTimeoutMillis;
        Future future = RegisterFutureAcquiringOpaque(request, null);
        int opaque = request.Opaque;
        try
        {
            SendRequest(addr, request);
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

        return future.Response;
    }

    /// <summary>
    /// 异步调用：发送后立即返回，响应到达时在读线程里触发 callback。
    /// 注意 callback 在**读线程**中执行，实现需自行保证线程安全。
    /// </summary>
    public void InvokeAsync(string addr, RemotingCommand request, InvokeCallback callback, int timeoutMillis = -1)
    {
        Future future = RegisterFutureAcquiringOpaque(request, callback);
        int opaque = request.Opaque;
        try
        {
            SendRequest(addr, request);
        }
        catch (Exception)
        {
            UnregisterFuture(opaque);
            throw;
        }
    }

    /// <summary>单向调用：置 oneway 标志后发送，不等响应（对应 Java invokeOneway）。</summary>
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
