// 协议序列化：JSON（RemotingSerializable）与 RocketMQ 私有二进制（RocketMQSerializable）。
//
// 对应 org.apache.rocketmq.remoting.protocol.RemotingSerializable / RocketMQSerializable。
//
// RocketMQ 二进制 header 线格式：
//   code(2) | language(1) | version(2) | opaque(4) | flag(4)
//   | remark(int + utf8) | extFields(int + [key(short + utf8) value(int + utf8)]...)
//
// ⚠ C++/Python 里字符串就是 UTF-8 字节串；C# 的 string 是 UTF-16，所以
//   writeStr/readStr 必须**显式 UTF-8 编解码**，且长度字段是 **UTF-8 字节数**（不是字符数）——
//   含中文的 remark 若用字符数算长度，broker 侧必然解析错位。
using System.Globalization;
using System.Text;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

/// <summary>RocketMQ 二进制 header 的字段集合。刻意不依赖 RemotingCommand，避免循环依赖。</summary>
public sealed class ProtocolHeaderFields
{
    public int Code { get; set; }
    public byte Language { get; set; }
    public int Version { get; set; }
    public int Opaque { get; set; }
    public int Flag { get; set; }
    public string Remark { get; set; } = string.Empty;
    public bool HasRemark { get; set; }
    public PropertyMap ExtFields { get; set; } = new();
}

/// <summary>org.apache.rocketmq.remoting.protocol.RemotingSerializable。</summary>
public static class RemotingSerializable
{
    /// <summary>JsonValue -> UTF-8 字节串。</summary>
    public static byte[] Encode(JsonValue v) => Encoding.UTF8.GetBytes(v.Dump());

    public static string Dump(JsonValue v) => v.Dump();

    /// <summary>UTF-8 字节串 -> JsonValue；空串或解析失败返回 false。</summary>
    public static bool Decode(byte[] data, out JsonValue value) =>
        Json.TryParse(Encoding.UTF8.GetString(data), out value, out _);
}

/// <summary>org.apache.rocketmq.remoting.protocol.RocketMQSerializable。</summary>
public static class RocketMQSerializable
{
    private static readonly Encoding Utf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

    /// <summary>十进制 ASCII 写入：先占位 int 长度，写完回补（对应 Java writeDecimalLong）。</summary>
    public static void WriteDecimalLong(ByteWriter buf, long value)
    {
        // 先占位 4 字节长度
        int lenPos = buf.Length;
        buf.WriteInt32(0);
        int start = buf.Length;

        if (value == 0)
        {
            buf.WriteUInt8((byte)'0');
        }
        else
        {
            if (value < 0)
            {
                buf.WriteUInt8((byte)'-');
                if (value == long.MinValue)
                {
                    // -(INT64_MIN) 溢出，直接写字符串常量
                    byte[] digits = Utf8.GetBytes("9223372036854775808");
                    buf.WriteBytes(digits);
                    PatchLength(buf, lenPos, start);
                    return;
                }

                value = -value;
            }

            buf.WriteBytes(Utf8.GetBytes(
                value.ToString(CultureInfo.InvariantCulture)));
        }

        PatchLength(buf, lenPos, start);
    }

    private static void PatchLength(ByteWriter buf, int lenPos, int start)
    {
        int n = buf.Length - start;
        // 直接改写占位的那 4 个字节（大端）
        buf.PatchInt32At(lenPos, n);
    }

    public static void WriteDecimalInt(ByteWriter buf, int value) => WriteDecimalLong(buf, value);

    /// <summary>
    /// useShortLength=true 用 2 字节长度（extFields 的 key），false 用 4 字节（value / remark）。
    /// </summary>
    public static void WriteStr(ByteWriter buf, bool useShortLength, string s)
    {
        byte[] bytes = Utf8.GetBytes(s);
        int n = bytes.Length;
        if (useShortLength)
        {
            buf.WriteUInt16(unchecked((ushort)n));
        }
        else
        {
            buf.WriteUInt32(unchecked((uint)n));
        }

        buf.WriteBytes(bytes);
    }

    /// <summary>读取返回 false 表示越界（由上层按「解码失败」处理）。</summary>
    public static bool ReadStr(
        byte[] buf, int offset, bool useShortLength, out string value, out int newOffset)
    {
        value = string.Empty;
        newOffset = offset;
        int n;
        if (useShortLength)
        {
            if (offset + 2 > buf.Length)
            {
                return false;
            }

            n = (buf[offset] << 8) | buf[offset + 1];
            offset += 2;
        }
        else
        {
            if (offset + 4 > buf.Length)
            {
                return false;
            }

            uint v = 0;
            for (int i = 0; i < 4; i++)
            {
                v = (v << 8) | buf[offset + i];
            }

            n = unchecked((int)v);
            offset += 4;
        }

        if (offset + n > buf.Length)
        {
            return false;
        }

        value = Utf8.GetString(buf, offset, n);
        offset += n;
        newOffset = offset;
        return true;
    }

    public static byte[] MapSerialize(PropertyMap mapData)
    {
        if (mapData.Count == 0)
        {
            return Array.Empty<byte>();
        }

        var buf = new ByteWriter(64);
        foreach (var kv in mapData)
        {
            WriteStr(buf, true, kv.Key);
            WriteStr(buf, false, kv.Value);
        }

        return buf.ToArray();
    }

    public static int CalTotalLen(string remark, bool hasRemark, int extLen)
    {
        if (!hasRemark)
        {
            return 2 + 1 + 2 + 4 + 4 + 4 + 0 + 4 + extLen;
        }

        int remarkLen = remark.Length == 0 ? 0 : Utf8.GetByteCount(remark);
        return 2 + 1 + 2 + 4 + 4 + 4 + remarkLen + 4 + extLen;
    }

    public static byte[] RocketMqProtocolEncode(ProtocolHeaderFields header)
    {
        byte[] remarkBytes = Array.Empty<byte>();
        bool hasRemark = header.HasRemark && header.Remark.Length > 0;
        if (hasRemark)
        {
            remarkBytes = Utf8.GetBytes(header.Remark);
        }

        byte[] extFieldsBytes = MapSerialize(header.ExtFields);
        bool hasExt = extFieldsBytes.Length > 0;

        var buf = new ByteWriter(64);
        buf.WriteInt16(unchecked((short)(header.Code & 0xFFFF)));
        buf.WriteUInt8(header.Language);
        buf.WriteInt16(unchecked((short)(header.Version & 0xFFFF)));
        buf.WriteInt32(header.Opaque);
        buf.WriteInt32(header.Flag);

        if (hasRemark)
        {
            buf.WriteInt32(remarkBytes.Length);
            buf.WriteBytes(remarkBytes);
        }
        else
        {
            buf.WriteInt32(0);
        }

        if (hasExt)
        {
            buf.WriteInt32(extFieldsBytes.Length);
            buf.WriteBytes(extFieldsBytes);
        }
        else
        {
            buf.WriteInt32(0);
        }

        return buf.ToArray();
    }

    public static bool RocketMqProtocolDecode(byte[] headerBytes, out ProtocolHeaderFields outFields)
    {
        outFields = new ProtocolHeaderFields();
        int p = 0;

        bool Need(int n) => p + n <= headerBytes.Length;

        if (!Need(2))
        {
            return false;
        }

        int code = (headerBytes[p] << 8) | headerBytes[p + 1];
        p += 2;

        if (!Need(1))
        {
            return false;
        }

        byte language = headerBytes[p];
        p += 1;

        if (!Need(2))
        {
            return false;
        }

        int version = (headerBytes[p] << 8) | headerBytes[p + 1];
        p += 2;

        if (!Need(4))
        {
            return false;
        }

        uint opaque = 0;
        for (int i = 0; i < 4; i++)
        {
            opaque = (opaque << 8) | headerBytes[p + i];
        }

        p += 4;

        if (!Need(4))
        {
            return false;
        }

        uint flag = 0;
        for (int i = 0; i < 4; i++)
        {
            flag = (flag << 8) | headerBytes[p + i];
        }

        p += 4;

        if (!ReadStr(headerBytes, p, false, out string remark, out p))
        {
            return false;
        }

        if (!Need(4))
        {
            return false;
        }

        uint extLen = 0;
        for (int i = 0; i < 4; i++)
        {
            extLen = (extLen << 8) | headerBytes[p + i];
        }

        p += 4;

        var extFields = new PropertyMap();
        if (extLen > 0)
        {
            int end = p + unchecked((int)extLen);
            if (end > headerBytes.Length)
            {
                return false;
            }

            while (p < end)
            {
                if (!ReadStr(headerBytes, p, true, out string k, out p))
                {
                    return false;
                }

                if (!ReadStr(headerBytes, p, false, out string v, out p))
                {
                    return false;
                }

                extFields[k] = v;
            }
        }

        outFields.Code = code & 0xFFFF;
        outFields.Language = language;
        outFields.Version = version & 0xFFFF;
        outFields.Opaque = unchecked((int)opaque);
        outFields.Flag = unchecked((int)flag);
        outFields.Remark = remark;
        outFields.HasRemark = remark.Length > 0;
        outFields.ExtFields = extFields;
        return true;
    }
}
