// A one-frame fast-channel listener for the VIP rerouting cases. It binds a free
// (port, port-2) pair, answers the first frame that lands on port-2 with a refusal, and
// hands out the plain address. Binding the fast port inside the test (instead of assuming
// nothing else occupies it) is what makes "the frame really used the fast channel" a
// deterministic claim while the suite runs its cases in parallel.
using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client.Tests;

internal sealed class FastChannelReply : IDisposable
{
    private const int MaxFrame = 20 * 1024 * 1024;

    private readonly Socket _plainListener;
    private readonly Socket _fastListener;

    private FastChannelReply(Socket plainListener, Socket fastListener, string plainAddr)
    {
        _plainListener = plainListener;
        _fastListener = fastListener;
        PlainAddr = plainAddr;
    }

    /// <summary>The plain route address; the caller's request must land two ports below it.</summary>
    public string PlainAddr { get; }

    /// <summary>True once a frame arrived on the fast port and the refusal went back.</summary>
    public volatile bool FrameArrived;

    /// <summary>Binds a free (p, p-2) pair whose fast port refuses the next frame; null when
    /// no pair could be bound.</summary>
    public static FastChannelReply? Start(int replyCode, string remark)
    {
        for (int attempt = 0; attempt < 64; ++attempt)
        {
            int port = FreeEphemeralPort();
            Socket? fast = TryBind(port - 2);
            if (fast is null)
            {
                continue;
            }

            Socket? plain = TryBind(port);
            if (plain is null)
            {
                fast.Dispose();
                continue;
            }

            var probe = new FastChannelReply(plain, fast, "127.0.0.1:"
                + port.ToString(CultureInfo.InvariantCulture));
            ServeOneFrame(fast, replyCode, remark, probe);
            ServeSilently(plain);
            return probe;
        }

        return null;
    }

    public void Dispose()
    {
        _fastListener.Dispose();
        _plainListener.Dispose();
    }

    private static int FreeEphemeralPort()
    {
        using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)probe.LocalEndPoint!).Port;
    }

    private static Socket? TryBind(int port)
    {
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            server.Bind(new IPEndPoint(IPAddress.Loopback, port));
            server.Listen(16);
            return server;
        }
        catch (SocketException)
        {
            server.Dispose();
            return null;
        }
    }

    private static void ServeOneFrame(Socket listener, int replyCode, string remark,
        FastChannelReply owner)
    {
        new Thread(() =>
        {
            Socket client;
            try
            {
                client = listener.Accept();
            }
            catch (Exception)
            {
                return;
            }

            using (client)
            {
                try
                {
                    var stream = new NetworkStream(client, ownsSocket: false);
                    var lenBuf = new byte[4];
                    if (!ReadFully(stream, lenBuf, 0, 4))
                    {
                        return;
                    }

                    int total = BinaryPrimitives.ReadInt32BigEndian(lenBuf);
                    if (total <= 0 || total > MaxFrame)
                    {
                        return;
                    }

                    var frame = new byte[4 + total];
                    Buffer.BlockCopy(lenBuf, 0, frame, 0, 4);
                    RemotingCommand? req = null;
                    string? decErr = null;
                    bool got = ReadFully(stream, frame, 4, total)
                        && RemotingCommand.TryDecode(frame, out req, out decErr);
                    if (!got || req is null)
                    {
                        return;
                    }

                    RemotingCommand resp = RemotingCommand.CreateResponseCommand(replyCode, remark);
                    resp.Opaque = req!.Opaque;
                    resp.SerializeTypeCurrentRpc = req.SerializeTypeCurrentRpc;
                    byte[] wire = resp.Encode();
                    stream.Write(wire, 0, wire.Length);
                    owner.FrameArrived = true;
                    // Hold the connection until the client closes it: shutting the socket
                    // right after the write races the client's read and the refusal can be
                    // lost to a reset.
                    var drain = new byte[4096];
                    while (stream.Read(drain, 0, drain.Length) > 0)
                    {
                    }
                }
                catch (Exception)
                {
                    // a broken handshake is still a fast-channel landing, just not a provable one
                }
            }
        })
        { IsBackground = true }.Start();
    }

    private static void ServeSilently(Socket listener)
    {
        new Thread(() =>
        {
            while (true)
            {
                Socket client;
                try
                {
                    client = listener.Accept();
                }
                catch (Exception)
                {
                    return;
                }

                new Thread(() =>
                {
                    try { client.Close(); }
                    catch (Exception) { }
                })
                { IsBackground = true }.Start();
            }
        })
        { IsBackground = true }.Start();
    }

    private static bool ReadFully(Stream stream, byte[] buffer, int offset, int count)
    {
        int read = 0;
        while (read < count)
        {
            int n = stream.Read(buffer, offset + read, count - read);
            if (n <= 0)
            {
                return false;
            }

            read += n;
        }

        return true;
    }
}
