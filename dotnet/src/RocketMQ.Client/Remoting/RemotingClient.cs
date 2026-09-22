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
//
// 与 Java 的一处口径差异（刻意如此）：NettyRemotingClient#scanChannelTablesOfNameServer
// （channelNotActiveInterval=60s）在 Java 客户端里**从未被调度** —— client + remoting 全树
// grep 不到调用点，属于死代码，所以这里不做空闲连接回收；对端真断开时读线程立刻见到 EOF，
// 惰性清理已覆盖真实场景。异步请求的超时清理（scanResponseTable）则有实现。
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Remoting;

/// <summary>带超时控制的 TCP 传输客户端。</summary>
public sealed class RemotingClient : IDisposable
{
    /// <summary>
    /// 异步调用回调（对应 Java InvokeCallback 的 operationSucceed / operationFail 二分）：
    /// 成功时 response 非空、error 为空；超时或发送失败时 response 为空、error 非空。
    /// 在读线程或超时清理线程中触发，实现需自行保证线程安全。
    /// </summary>
    public delegate void InvokeCallback(RemotingCommand? response, Exception? error);

    /// <summary>
    /// broker 主动发来的**请求**（而非响应）的处理器：handler(请求命令, 对端地址)。
    /// 对应 Java NettyRemotingAbstract 的 processor 表。返回非 null 的 RemotingCommand
    /// 表示「要回一个响应」——典型是 PUSH_REPLY_MESSAGE_TO_CLIENT(326)，broker 侧是
    /// invokeSync，不回响应它那边会超时；返回 null 表示「不回响应」，与 Java
    /// ClientRemotingProcessor.checkTransactionState 返回 null 的语义一致（broker 用
    /// invokeOneway 发的回查本来也不期待响应）。
    /// </summary>
    public delegate RemotingCommand? RequestProcessor(RemotingCommand request, string addr);

    /// <summary>单帧上限（与 Java NettyRemotingClient 的 16MB 限制一致）。</summary>
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
        public InvokeCallback? Callback;

        // 异步请求的超时账目（对应 Java ResponseFuture 的 timeoutMillis + beginTimestamp）。
        // Addr/DeadlineMs/TimeoutMs 只在登记时写入；CallbackFired 由读线程与清理线程争抢。
        // DeadlineMs == 0 表示不由清理线程负责（同步路径自己等、自己摘除）。
        public string Addr = string.Empty;
        public long DeadlineMs;
        public long TimeoutMs;
        public bool CallbackFired;

        // 单调时钟：不受系统时间调整影响（Java 用 currentTimeMillis，这里刻意取更稳的口径）
        public static long MonoNowMs() => Environment.TickCount64;
    }

    private volatile bool _running = true;
    private readonly int _connectTimeoutMillis;
    private readonly int _invokeTimeoutMillis;
    private readonly bool _tlsEnable;

    private readonly object _connMutex = new();
    private readonly Dictionary<string, Connection> _conns = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<int, Future> _respTable = new();

    // 在途请求超时清理（对应 Java NettyRemotingAbstract.scanResponseTable 及其定时线程）。
    // 异步请求只有被这张表按时摘掉并回调一次，调用方才知道失败；缺了它回调会永久悬挂、
    // 条目永久泄漏。首次 InvokeAsync 时才创建，纯同步用法不留线程。
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

    /// <summary>
    /// 安装 RPC 钩子（对应 Java NettyRemotingClient#registerRPCHook）。钩子在每次请求
    /// **编码之前**于发送路径上被调用，从而能把 AccessKey/Signature 写进 ExtFields。
    /// </summary>
    /// <remarks>
    /// <b>first-wins</b>：已有钩子时返回 false 且不覆盖，与 Java 在构造 MQClientInstance 时
    /// 绑定钩子的行为一致（同一 clientId 复用实例）。因此钩子必须在 Start() 之前设置。
    /// <para>
    /// 注意（与 Java 的有意差异）：Java 还在响应完成回调里调用 DoAfterResponse，本实现
    /// 没有该调用——响应在读线程里分发，此处不持有请求对象，为了不把每个请求的 Body
    /// 都拷一份挂在在途表上，故省略。AclClientRPCHook.DoAfterResponse 本身是空实现，
    /// 因此无功能影响。
    /// </para>
    /// </remarks>
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

    private IRpcHook? CurrentRpcHook()
    {
        lock (_hookLock)
        {
            return _rpcHook;
        }
    }

    public RemotingClient(int connectTimeoutMillis = 3000, int invokeTimeoutMillis = 15000,
        bool? tlsEnable = null)
    {
        _connectTimeoutMillis = connectTimeoutMillis;
        _invokeTimeoutMillis = invokeTimeoutMillis;
        // TLS（对应 Java NettyRemotingClient 的 isUseTLS / tls.enable）。显式参数优先，
        // 否则读 ROCKETMQ_TLS_ENABLE（Java 是 JVM 系统属性 -Dtls.enable，这里等价为 env）。
        _tlsEnable = tlsEnable ?? EnvTlsEnabled();
    }

    internal static bool EnvTlsEnabled()
    {
        string? v = Environment.GetEnvironmentVariable("ROCKETMQ_TLS_ENABLE");
        return v is not null && (v.Trim().ToLowerInvariant() is "1" or "true" or "yes");
    }

    /// <summary>
    /// 对应 Java NettyClientConfig.enableReconnectForGoAway（默认 <c>true</c>）：
    /// broker / proxy 优雅下线时给在途请求回 <c>GO_AWAY(1500)</c>，语义是「这条连接别再用了」。
    /// 开启时换一条连接重发一次（只一次），第二次仍是 GO_AWAY 就报发送失败；关掉直接失败。
    /// 必须在首条连接建立前设置。
    /// </summary>
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

        // TLS：在任何 RocketMQ 帧之前完成握手（对应 Java pipeline.addFirst(SslHandler)）。
        // test mode 信任 broker 自签证书（Java tls.test.mode.enable 默认 true 的等价语义）。
        if (_tlsEnable)
        {
            try
            {
                var ns = new NetworkStream(sock, ownsSocket: false);
                var ssl = new SslStream(ns, false,
                    (sender, cert, chain, errors) => true);   // test mode：信任一切
                ssl.AuthenticateAsClient(host);
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
            // 与超时清理线程抢同一个回调：谁先置位谁投递，另一个只能放弃
            // （Java 用 ResponseFuture.executeCallbackOnlyOnce 表达同一约束）。
            if (!future.CallbackFired)
            {
                future.CallbackFired = true;
                cb = future.Callback;
            }
        }

        future.Done.Set();
        cb?.Invoke(cmd, null);
    }

    /// <summary>
    /// 注册 broker 主动请求的处理器（对应 Java NettyRemotingAbstract 的 processor 表）。
    /// 只有「请求类型且不在本地在途响应表里」的命令才会派发到这里，不会影响现有的
    /// InvokeSync / InvokeAsync 响应分发。处理器在读线程里执行，需自保证线程安全。
    /// </summary>
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

    private void SendRequest(string addr, RemotingCommand request)
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

    /// <summary>
    /// 把**响应**写回对端（对应 Java processRequestCommand 里给处理器产出响应走
    /// ctx.writeAndFlush）。不走 RPC 钩子（响应不需要重签名），且调用方已设好 Opaque
    /// 与响应位。仅复用了连接写锁与分帧发送。
    /// </summary>
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
    //
    // 为什么不能简单用 "opaque == 0 就重分配"：RemotingCommand 的 opaque 计数器从 0
    // 开始，所以 createRequestCommand() 产出的**第一个**请求 opaque 就是 0，把 0 当
    // "未设置"会把它改掉，导致调用方与服务端回填的 opaque 不一致（调用方拿不到响应）。
    // 这里只在**真的与在途请求冲突**时才重分配，其余情况原样保留（含 0），与 Java
    // NettyRemotingClient 从不改写调用方 opaque 的行为一致。
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

    /// <summary>
    /// 摘除已超时的在途请求并回调一次（对应 Java scanResponseTable）。
    /// </summary>
    private void SweepExpired()
    {
        // 与 Java 同式：beginTimestamp + timeoutMillis + 1000 <= now。这 1s 宽限是给
        // "响应已经在路上"留的余量——超时后 1s 内到达的响应仍按成功投递。
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

    /// <summary>
    /// 同步调用：等到响应或超时，含 GO_AWAY 换连接重发（对应 Java
    /// <c>NettyRemotingClient#invokeImpl:828-873</c>）。超时抛 RemotingTimeoutException；
    /// 建连失败抛 RemotingConnectException；发送失败抛 RemotingSendRequestException。
    /// timeoutMillis &lt; 0 表示使用 invokeTimeoutMillis。
    /// </summary>
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

    /// <summary>单次请求-应答：登记 opaque、写出、等响应或超时。不含 GO_AWAY 判定。</summary>
    private RemotingCommand InvokeOnce(string addr, RemotingCommand request, int timeout)
    {
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
    /// GO_AWAY 的收口：开关关掉直接报错；否则换连接重发一次，第二次还是 GO_AWAY 就抛。
    /// <paramref name="deadlineMs"/> 是整次调用（含首发）的绝对截止时间，重发只花剩余预算
    /// ——Java 用同一个 Stopwatch 扣减 timeoutMillis。
    /// </summary>
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

    /// <summary>
    /// GO_AWAY 重发用的请求副本（对应 Java 的 createRequestCommand + setBody + setExtFields）。
    /// 必须是**新** opaque：旧请求已经拿到应答，复用会让下一次响应错配。
    /// ExtFields 按值复制——ACL 签名（AccessKey/Signature）就在里面，重发要带原签名。
    /// </summary>
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

    /// <summary>
    /// 异步调用：发送后立即返回。回调**恰好触发一次**——响应到达时在读线程里带 response，
    /// 超时或无响应时由清理线程带 error（对应 Java scanResponseTable → operationFail）。
    /// timeoutMillis &lt; 0 表示使用 invokeTimeoutMillis。
    ///
    /// GO_AWAY 与同步路径同一套语义（Java 里两条路共用 invokeImpl）：重发要建新连接并等
    /// 它的读线程投递响应，绝不能压在回调所在的读线程上，所以交给线程池。
    /// </summary>
    public void InvokeAsync(string addr, RemotingCommand request, InvokeCallback callback, int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _invokeTimeoutMillis;
        EnsureSweeper();
        long deadline = Future.MonoNowMs() + timeout;
        RemotingCommand original = CopyForRetry(request);
        original.Opaque = request.Opaque;
        RegisterFutureAcquiringOpaque(request, (response, error) =>
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
