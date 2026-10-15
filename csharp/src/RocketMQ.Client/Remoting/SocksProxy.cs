// Zero-dependency hand-rolled handshake: SOCKS5 follows RFC 1928 (no auth 0x00 / username
// password 0x02, sub-negotiation RFC 1929), SOCKS4 follows the original protocol, and
// domain-name targets use the SOCKS4a extension (DSTIP = 0.0.0.x).
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace RocketMQ.Remoting;

/// <summary>SOCKS protocol version.</summary>
public enum SocksProxyVersion
{
    /// <summary>SOCKS4; when the target is a hostname, automatically uses the SOCKS4a extension, with Username as USERID.</summary>
    V4,

    /// <summary>SOCKS5 (default). Supports both no-auth and username/password (RFC 1929) sub-negotiations.</summary>
    V5,
}

public sealed class SocksProxyConfig
{
    /// <summary>Proxy address "host:port" (IPv6 literals use the "[::1]:1080" form).</summary>
    public string Addr { get; set; } = string.Empty;

    /// <summary>
    /// Auth username: SOCKS5's username/password sub-negotiation / SOCKS4's USERID.
    /// Empty means no auth (the SOCKS5 handshake only offers the 0x00 method).
    /// </summary>
    public string? Username { get; set; }

    /// <summary>Auth password (only used by SOCKS5's username/password sub-negotiation; SOCKS4 has no password concept).</summary>
    public string? Password { get; set; }

    public SocksProxyVersion Version { get; set; } = SocksProxyVersion.V5;

    public override string ToString()
    {
        return "SocksProxy address: " + Addr + ", username: " + Username + ", password: "
            + (string.IsNullOrEmpty(Password) ? (Password ?? "null") : "***");
    }
}

/// <summary>
/// SOCKS handshake (blocking, runs on the caller's connect thread; the timeout is deducted
/// uniformly per <c>deadline</c>, covering greeting / sub-negotiation / CONNECT). Failures
/// always throw, and the caller translates them into
/// <see cref="RemotingConnectException"/>.
/// </summary>
internal static class SocksProxy
{
    private const byte Ver5 = 5;
    private const byte Ver4 = 4;
    private const byte VerUserPass = 1;
    private const byte CmdConnect = 1;
    private const byte MethodNoAuth = 0x00;
    private const byte MethodUserPass = 0x02;
    private const byte MethodNoAcceptable = 0xFF;
    private const byte RepSucceeded = 0x00;
    private const byte AddrTypeIpv4 = 0x01;
    private const byte AddrTypeDomain = 0x03;
    private const byte AddrTypeIpv6 = 0x04;

    public static void Handshake(Socket sock, SocksProxyConfig proxy, string destHost, int destPort,
        int timeoutMillis)
    {
        if (string.IsNullOrEmpty(proxy.Addr))
        {
            throw new IOException("socks proxy addr is empty: " + proxy);
        }

        long deadline = Environment.TickCount64 + Math.Max(1, timeoutMillis);
        if (proxy.Version == SocksProxyVersion.V5)
        {
            Handshake5(sock, proxy, destHost, destPort, deadline);
        }
        else
        {
            Handshake4(sock, proxy, destHost, destPort, deadline);
        }
    }

    // ---------------------------------------------------------------- SOCKS5

    private static void Handshake5(Socket sock, SocksProxyConfig proxy, string destHost, int destPort,
        long deadline)
    {
        bool withAuth = !string.IsNullOrEmpty(proxy.Username);
        // Method selection: with credentials, offer both 0x00 and 0x02 and let the proxy
        // choose (matches Netty Socks5ProxyHandler's method set when credentials are present)
        byte[] greeting = withAuth
            ? new byte[] { Ver5, 2, MethodNoAuth, MethodUserPass }
            : new byte[] { Ver5, 1, MethodNoAuth };
        SendAll(sock, greeting, deadline);

        var selected = new byte[2];
        ReadExact(sock, selected, 0, 2, deadline);
        if (selected[0] != Ver5)
        {
            throw new IOException("socks5: bad greeting reply version "
                + selected[0].ToString(CultureInfo.InvariantCulture));
        }

        if (selected[1] == MethodNoAcceptable)
        {
            throw new IOException("socks5: proxy rejected all offered auth methods");
        }

        if (selected[1] == MethodUserPass)
        {
            if (!withAuth)
            {
                throw new IOException("socks5: proxy demands username/password auth but no credentials configured");
            }

            SubNegotiateUserPass(sock, proxy, deadline);
        }
        else if (selected[1] != MethodNoAuth)
        {
            throw new IOException("socks5: proxy chose unsupported method 0x"
                + selected[1].ToString("X2", CultureInfo.InvariantCulture));
        }

        SendAll(sock, BuildConnect5(destHost, destPort), deadline);

        var head = new byte[4];
        ReadExact(sock, head, 0, 4, deadline);
        if (head[0] != Ver5)
        {
            throw new IOException("socks5: bad reply version "
                + head[0].ToString(CultureInfo.InvariantCulture));
        }

        if (head[1] != RepSucceeded)
        {
            throw new IOException("socks5: CONNECT to " + destHost + ":" + destPort
                .ToString(CultureInfo.InvariantCulture) + " failed, reply code "
                + head[1].ToString(CultureInfo.InvariantCulture));
        }

        // BND.ADDR length varies with ATYP: 1=4-byte IPv4; 3=1-byte length-prefixed domain;
        // 4=16-byte IPv6. There is also a 2-byte BND.PORT after it; read it all out, otherwise
        // leftover bytes pollute the subsequent RocketMQ frames.
        int addrLen = head[3] switch
        {
            AddrTypeIpv4 => 4,
            AddrTypeIpv6 => 16,
            AddrTypeDomain => -1,
            _ => throw new IOException("socks5: bad reply address type "
                + head[3].ToString(CultureInfo.InvariantCulture)),
        };
        if (addrLen < 0)
        {
            var lenByte = new byte[1];
            ReadExact(sock, lenByte, 0, 1, deadline);
            addrLen = lenByte[0];
        }

        var rest = new byte[addrLen + 2]; // BND.ADDR + BND.PORT
        ReadExact(sock, rest, 0, rest.Length, deadline);
    }

    private static void SubNegotiateUserPass(Socket sock, SocksProxyConfig proxy, long deadline)
    {
        byte[] uname = Encoding.UTF8.GetBytes(proxy.Username!);
        byte[] passwd = Encoding.UTF8.GetBytes(proxy.Password ?? string.Empty);
        if (uname.Length > 255)
        {
            throw new IOException("socks5: username longer than 255 bytes");
        }

        if (passwd.Length > 255)
        {
            throw new IOException("socks5: password longer than 255 bytes");
        }

        // RFC 1929: VER(1) ULEN(1) UNAME PLEN(1) PASSWD
        var req = new byte[3 + uname.Length + passwd.Length];
        req[0] = VerUserPass;
        req[1] = (byte)uname.Length;
        Buffer.BlockCopy(uname, 0, req, 2, uname.Length);
        req[2 + uname.Length] = (byte)passwd.Length;
        Buffer.BlockCopy(passwd, 0, req, 3 + uname.Length, passwd.Length);
        SendAll(sock, req, deadline);

        var rep = new byte[2];
        ReadExact(sock, rep, 0, 2, deadline);
        if (rep[0] != VerUserPass)
        {
            throw new IOException("socks5: bad auth reply version "
                + rep[0].ToString(CultureInfo.InvariantCulture));
        }

        if (rep[1] != 0)
        {
            throw new IOException("socks5: username/password auth rejected, status "
                + rep[1].ToString(CultureInfo.InvariantCulture));
        }
    }

    private static byte[] BuildConnect5(string destHost, int destPort)
    {
        if (destPort is < 1 or > 65535)
        {
            throw new IOException("socks5: invalid destination port "
                + destPort.ToString(CultureInfo.InvariantCulture));
        }

        byte addrType;
        byte[] addr;
        if (IPAddress.TryParse(destHost, out IPAddress? ip))
        {
            byte[] raw = ip.GetAddressBytes();
            addrType = raw.Length == 4 ? AddrTypeIpv4 : AddrTypeIpv6;
            addr = raw;
        }
        else
        {
            addrType = AddrTypeDomain;
            addr = Encoding.UTF8.GetBytes(destHost);
            if (addr.Length is < 1 or > 255)
            {
                throw new IOException("socks5: destination host name length out of range: " + destHost);
            }
        }

        // VER(1) CMD(1) RSV(1) ATYP(1) ADDR PORT(2)
        var req = new byte[6 + addr.Length];
        req[0] = Ver5;
        req[1] = CmdConnect;
        req[3] = addrType;
        Buffer.BlockCopy(addr, 0, req, 4, addr.Length);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(req.AsSpan(4 + addr.Length),
            (ushort)destPort);
        return req;
    }

    // ------------------------------------------------------------- SOCKS4/4a

    private static void Handshake4(Socket sock, SocksProxyConfig proxy, string destHost, int destPort,
        long deadline)
    {
        if (destPort is < 1 or > 65535)
        {
            throw new IOException("socks4: invalid destination port "
                + destPort.ToString(CultureInfo.InvariantCulture));
        }

        byte[] userid = Encoding.UTF8.GetBytes(proxy.Username ?? string.Empty);
        if (userid.Length > 255)
        {
            throw new IOException("socks4: USERID longer than 255 bytes");
        }

        // Target is an IPv4 literal -> native SOCKS4; otherwise the 4a extension: DSTIP =
        // 0.0.0.x (x non-zero), with a NUL-terminated domain name after USERID, which the
        // proxy resolves.
        byte[] dstIp = { 0, 0, 0, 1 };
        byte[]? domain = null;
        if (IPAddress.TryParse(destHost, out IPAddress? ip))
        {
            byte[] raw = ip.GetAddressBytes();
            if (raw.Length == 4)
            {
                dstIp = raw;
            }
            else
            {
                throw new IOException("socks4: IPv6 destination not supported, target=" + destHost);
            }
        }
        else
        {
            domain = Encoding.UTF8.GetBytes(destHost);
            if (domain.Length is < 1 or > 255)
            {
                throw new IOException("socks4a: destination host name length out of range: " + destHost);
            }
        }

        // VER(1) CMD(1) DSTPORT(2) DSTIP(4) USERID NUL [HOSTNAME NUL]
        int total = 8 + userid.Length + 1;
        if (domain is not null)
        {
            total += domain.Length + 1;
        }

        var req = new byte[total];
        req[0] = Ver4;
        req[1] = CmdConnect;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(req.AsSpan(2), (ushort)destPort);
        Buffer.BlockCopy(dstIp, 0, req, 4, 4);
        Buffer.BlockCopy(userid, 0, req, 8, userid.Length);
        req[8 + userid.Length] = 0;
        if (domain is not null)
        {
            Buffer.BlockCopy(domain, 0, req, 9 + userid.Length, domain.Length);
            req[9 + userid.Length + domain.Length] = 0;
        }

        SendAll(sock, req, deadline);

        // Fixed 8-byte response: VN(1) CD(1) DSTPORT(2) DSTIP(4); CD = 0x5A(90) means granted
        var rep = new byte[8];
        ReadExact(sock, rep, 0, 8, deadline);
        if (rep[1] != 0x5A)
        {
            throw new IOException("socks4: CONNECT to " + destHost + ":" + destPort
                .ToString(CultureInfo.InvariantCulture) + " failed, reply code "
                + rep[1].ToString(CultureInfo.InvariantCulture));
        }
    }

    // ---------------------------------------------------------------- I/O

    private static int RemainingMs(long deadline)
    {
        long left = deadline - Environment.TickCount64;
        // Always give the Socket's timeout field a positive value (0 = wait forever, which
        // degenerates into a hang)
        return (int)Math.Max(1, left);
    }

    private static void SendAll(Socket sock, byte[] data, long deadline)
    {
        sock.SendTimeout = RemainingMs(deadline);
        int sent = 0;
        while (sent < data.Length)
        {
            int n = sock.Send(data, sent, data.Length - sent, SocketFlags.None);
            if (n <= 0)
            {
                throw new IOException("socks: proxy connection closed while sending");
            }

            sent += n;
        }
    }

    private static void ReadExact(Socket sock, byte[] buf, int offset, int count, long deadline)
    {
        sock.ReceiveTimeout = RemainingMs(deadline);
        int got = 0;
        while (got < count)
        {
            int n = sock.Receive(buf, offset + got, count - got, SocketFlags.None);
            if (n <= 0)
            {
                throw new IOException("socks: proxy closed connection during handshake");
            }

            got += n;
        }
    }
}
