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
    /// 单调高精度时钟起点（基于 Stopwatch 的单调时基，不受墙钟回拨影响）。
    ///
    /// 只用于量**耗时**：墙钟（`CurrentTimeMillis`）粒度是毫秒且会被 NTP 往回拨，
    /// 本地环回亚毫秒往返会量成 0，发送延迟故障容错的阈值就永远不会生效。
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

        // 先拆出毫秒占位符（模式里的 "%03d" 特判为毫秒）
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

    /// <summary>把用到的 strftime 说明符映射成 C# 自定义格式串（逐字符处理，避免 %m/%M 混淆）。</summary>
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

    /// <summary>严格 IPv4 校验。⚠ 不能只用 IPAddress.TryParse——它会把 "1" 解析成 0.0.0.1。</summary>
    public static bool IsIpv4(string addr) =>
        IpParse.TryParseStrict(addr, IpParse.IpFamily.V4, out _);

    /// <summary>IP 字符串 -> 4B(v4) / 16B(v6) 网络序字节。</summary>
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

    /// <summary>网络序字节 -> IP 字符串（v4: 4B, v6: 16B）。</summary>
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

        // 兼容小写输入
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

    /// <summary>逐字节转**大写**十六进制（msgId 依赖大小写）。</summary>
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

    /// <summary>十六进制字符串 -> 字节串；非法字符/奇数长度返回空数组。</summary>
    public static byte[] String2Bytes(string hexString)
    {
        if (string.IsNullOrEmpty(hexString))
        {
            return Array.Empty<byte>();
        }

        int len = hexString.Length;
        if (len % 2 != 0)
        {
            return Array.Empty<byte>(); // 非法长度
        }

        var outBuf = new byte[len / 2];
        for (int i = 0; i < len / 2; i++)
        {
            int hi = CharToByte(hexString[i * 2]);
            int lo = CharToByte(hexString[(i * 2) + 1]);
            if (hi < 0 || lo < 0)
            {
                return Array.Empty<byte>(); // 非法字符 -> 空数组
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
            // 网络不可用时走回落路径
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

/// <summary>IP 解析辅助：提供比 IPAddress.TryParse 更严格的族校验。</summary>
internal static class IpParse
{
    internal enum IpFamily
    {
        V4,
        V6,
    }

    /// <summary>
    /// 严格解析：TryParse 之外还要**回环比对**规范化字符串。
    /// 否则 "1" 会被 TryParse 接受为 0.0.0.1，严格口径必须拒绝这类缩写形式。
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
        // 回环校验：拒绝 "01.02.03.04" / "1" 这类非常规写法
        return string.Equals(canonical, text, StringComparison.Ordinal);
    }
}

/// <summary>
/// MessageClientIDSetter 的 C# 对应：
/// IP(4|16B) + PID(2B) + hash(4B) + 当日毫秒(4B) + 自增(2B)。
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
