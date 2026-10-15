using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace RocketMQ.Common;

public static class UtilAll
{
    public const string YyyyMmDdHhMmSs = "%Y-%m-%d %H:%M:%S";
    public const string YyyyMmDdHhMmSsSss = "%Y-%m-%d %H:%M:%S.%03d";

    public const string HexArray = "0123456789ABCDEF";

    public static long CurrentTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public static long CurrentTimeSeconds() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    public static string Offset2FileName(long offset) =>
        offset.ToString("D20", CultureInfo.InvariantCulture);

    public static long ComputeElapseTimeMillis(long lastTime) => CurrentTimeMillis() - lastTime;

    /// <summary>
    /// Monotonic high-precision clock origin (based on Stopwatch's monotonic time base, unaffected by wall-clock stepping back).
    ///
    /// Only used to measure **elapsed time**: the wall clock (`CurrentTimeMillis`) has millisecond granularity and can be stepped back by NTP,
    /// so a local-loopback sub-millisecond round trip would measure as 0, and the send-latency fault-tolerance threshold would never take effect.
    /// </summary>
    public static double MonotonicMillis() =>
        Stopwatch.GetElapsedTime(_stopwatchStart).TotalMilliseconds;

    private static readonly long _stopwatchStart = Stopwatch.GetTimestamp();

    public static string TimeToHumanString(long ts, string pattern = YyyyMmDdHhMmSs)
    {
        if (ts <= 0)
        {
            return "-";
        }

        // First extract the millisecond placeholder ("%03d" in the pattern is specially treated as milliseconds)
        string suffix = string.Empty;
        bool hasMillis = false;
        int msPos = pattern.IndexOf("%03d", StringComparison.Ordinal);
        if (msPos >= 0)
        {
            hasMillis = true;
            suffix = pattern[(msPos + 4)..];
            pattern = pattern[..msPos];
        }

        var local = DateTimeOffset.FromUnixTimeMilliseconds(ts).LocalDateTime;
        string dotnet = StrftimeToDotNet(pattern);
        string head = local.ToString(dotnet, CultureInfo.InvariantCulture);
        if (!hasMillis)
        {
            return head;
        }

        long ms = ts % 1000;
        if (ms < 0)
        {
            ms = 0;
        }

        return head + ms.ToString("D3", CultureInfo.InvariantCulture) + suffix;
    }

    /// <summary>Maps the used strftime specifiers to C# custom format strings (processed character by character to avoid %m/%M confusion).</summary>
    private static string StrftimeToDotNet(string pattern)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < pattern.Length; i++)
        {
            char c = pattern[i];
            if (c != '%')
            {
                sb.Append(c);
                continue;
            }

            if (i + 1 >= pattern.Length)
            {
                throw new FormatException($"bad strftime pattern: {pattern}");
            }

            char spec = pattern[++i];
            switch (spec)
            {
                case 'Y': sb.Append("yyyy"); break;
                case 'y': sb.Append("yy"); break;
                case 'm': sb.Append("MM"); break;
                case 'd': sb.Append("dd"); break;
                case 'H': sb.Append("HH"); break;
                case 'M': sb.Append("mm"); break;
                case 'S': sb.Append("ss"); break;
                default:
                    throw new FormatException(
                        $"unsupported strftime specifier %{spec} in pattern: {pattern}");
            }
        }

        return sb.ToString();
    }

    public static bool IsBlank(string? s)
    {
        if (s is null)
        {
            return true;
        }

        foreach (char c in s)
        {
            if (!char.IsWhiteSpace(c))
            {
                return false;
            }
        }

        return true;
    }

    public static bool IsNotBlank(string? s) => !IsBlank(s);

    public static int Pid() => Environment.ProcessId;

    /// <summary>Strict IPv4 validation. WARNING: do not rely only on IPAddress.TryParse—it would parse "1" as 0.0.0.1.</summary>
    public static bool IsIpv4(string addr) =>
        IpParse.TryParseStrict(addr, IpParse.IpFamily.V4, out _);

    /// <summary>IP string -> 4B(v4) / 16B(v6) network-order bytes.</summary>
    public static bool IpToBytes(string ip, bool v6, out byte[] outBuf)
    {
        outBuf = Array.Empty<byte>();
        if (!IpParse.TryParseStrict(ip, v6 ? IpParse.IpFamily.V6 : IpParse.IpFamily.V4, out IPAddress? parsed))
        {
            return false;
        }

        outBuf = parsed!.GetAddressBytes();
        return outBuf.Length == (v6 ? 16 : 4);
    }

    /// <summary>Network-order bytes -> IP string (v4: 4B, v6: 16B).</summary>
    public static bool BytesToIp(byte[] raw, out string ip)
    {
        ip = string.Empty;
        if (raw.Length != 4 && raw.Length != 16)
        {
            return false;
        }

        ip = new IPAddress(raw).ToString();
        return true;
    }

    public static uint Crc32(byte[] data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (byte b in data)
        {
            crc = Crc32Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        }

        return crc ^ 0xFFFFFFFFu;
    }

    private static readonly uint[] Crc32Table = BuildCrc32Table();

    private static uint[] BuildCrc32Table()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++)
            {
                c = (c & 1) != 0 ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }

            table[i] = c;
        }

        return table;
    }

    public static int CharToByte(char c)
    {
        int idx = HexArray.IndexOf(c);
        if (idx >= 0)
        {
            return idx;
        }

        // Accept lowercase input
        if (c >= 'a' && c <= 'f')
        {
            return c - 'a' + 10;
        }

        if (c >= 'A' && c <= 'F')
        {
            return c - 'A' + 10;
        }

        if (c >= '0' && c <= '9')
        {
            return c - '0';
        }

        return -1;
    }

    /// <summary>Convert byte-by-byte to **uppercase** hex (msgId depends on case).</summary>
    public static string Bytes2String(byte[] bs)
    {
        var sb = new StringBuilder(bs.Length * 2);
        foreach (byte b in bs)
        {
            sb.Append(HexArray[(b >> 4) & 0x0F]);
            sb.Append(HexArray[b & 0x0F]);
        }

        return sb.ToString();
    }

    /// <summary>Hex string -> byte string; returns empty array for illegal characters/odd length.</summary>
    public static byte[] String2Bytes(string hexString)
    {
        if (string.IsNullOrEmpty(hexString))
        {
            return Array.Empty<byte>();
        }

        int len = hexString.Length;
        if (len % 2 != 0)
        {
            return Array.Empty<byte>(); // illegal length
        }

        var outBuf = new byte[len / 2];
        for (int i = 0; i < len / 2; i++)
        {
            int hi = CharToByte(hexString[i * 2]);
            int lo = CharToByte(hexString[(i * 2) + 1]);
            if (hi < 0 || lo < 0)
            {
                return Array.Empty<byte>(); // illegal character -> empty array
            }

            outBuf[i] = (byte)((hi << 4) | lo);
        }

        return outBuf;
    }

    public static string LocalIp()
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            socket.Connect(IPAddress.Parse("8.8.8.8"), 80);
            if (socket.LocalEndPoint is IPEndPoint ep)
            {
                return ep.Address.ToString();
            }
        }
        catch (Exception)
        {
            // fall back to the fallback path when the network is unavailable
        }

        try
        {
            string host = Dns.GetHostName();
            if (!string.IsNullOrEmpty(host))
            {
                return host;
            }
        }
        catch (Exception)
        {
            // ignore
        }

        return "127.0.0.1";
    }

    public static string NextMillisString() =>
        CurrentTimeMillis().ToString(CultureInfo.InvariantCulture);

    public static string UserHome()
    {
        string home = Environment.GetEnvironmentVariable("HOME") ?? string.Empty;
        if (home.Length == 0)
        {
            home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        }
        return home;
    }

    public static string TimeMillisToHumanString3(long ts) =>
        DateTimeOffset.FromUnixTimeMilliseconds(ts).LocalDateTime
            .ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);

    public static string CreateUniqId() => InnerIdGenerator.CreateUniqId();
}

/// <summary>IP parsing helper: provides stricter family validation than IPAddress.TryParse.</summary>
internal static class IpParse
{
    internal enum IpFamily
    {
        V4,
        V6,
    }

    /// <summary>
    /// Strict parsing: beyond TryParse, also **round-trip compare** the normalized string.
    /// Otherwise "1" would be accepted by TryParse as 0.0.0.1, but the strict criterion must reject such abbreviated forms.
    /// </summary>
    internal static bool TryParseStrict(string text, IpFamily family, out IPAddress? parsed) =>
        TryParseStrict(text, family, out parsed, out _);

    internal static bool TryParseStrict(
        string text, IpFamily family, out IPAddress? parsed, out string canonical)
    {
        parsed = null;
        canonical = string.Empty;
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        if (!System.Net.IPAddress.TryParse(text, out var candidate))
        {
            return false;
        }

        AddressFamily want = family == IpFamily.V6
            ? AddressFamily.InterNetworkV6
            : AddressFamily.InterNetwork;
        if (candidate.AddressFamily != want)
        {
            return false;
        }

        canonical = candidate.ToString();
        parsed = candidate;
        // Round-trip check: reject non-canonical forms like "01.02.03.04" / "1"
        return string.Equals(canonical, text, StringComparison.Ordinal);
    }
}

/// <summary>
/// The C# counterpart of MessageClientIDSetter:
/// IP(4|16B) + PID(2B) + hash(4B) + millis-of-day(4B) + auto-increment(2B).
/// </summary>
public static class InnerIdGenerator
{
    private static int _counter;

    public static string CreateUniqId()
    {
        uint c = unchecked((uint)Interlocked.Increment(ref _counter));

        byte[] result;
        string ip = UtilAll.LocalIp();
        if (UtilAll.IsIpv4(ip) && UtilAll.IpToBytes(ip, false, out byte[] ipBytes))
        {
            result = ipBytes;
        }
        else if (UtilAll.IpToBytes(ip, true, out byte[] ipBytes6))
        {
            result = ipBytes6;
        }
        else
        {
            result = new byte[] { 0x7F, 0x00, 0x00, 0x01 };
        }

        var writer = new ByteWriter(16);
        writer.WriteBytes(result);

        int pidv = UtilAll.Pid();
        writer.WriteInt16(unchecked((short)(pidv & 0xFFFF)));

        int classHash = LegacyHash.StringHash("RocketMQClient");
        if (classHash < 0)
        {
            classHash = -classHash;
        }

        writer.WriteUInt32(unchecked((uint)classHash));

        var now = DateTimeOffset.Now;
        uint dayMs = unchecked(
            (uint)((((now.Hour * 60) + now.Minute) * 60 + now.Second) * 1000));
        writer.WriteUInt32(dayMs);

        writer.WriteInt16(unchecked((short)(c & 0xFFFF)));

        return UtilAll.Bytes2String(writer.ToArray());
    }
}
