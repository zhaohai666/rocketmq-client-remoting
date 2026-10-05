// RocketMQ 二进制读写工具：全部大端（network byte order），与 Java ByteBuffer 默认序一致。
//
// C++ 侧是「往 Bytes(std::string) 里 push」的函数式 API；C# 用 ByteWriter/ByteReader 两个类，
// 语义一一对应（方法名保持 Java DataOutputStream 的 writeInt/Long/Short/Byte 风格）。
using System.Buffers.Binary;
using System.Globalization;

namespace RocketMQ.Common;

/// <summary>越界/截断异常：与 C++ 的 std::out_of_range 对应，上层按「解码失败」处理。</summary>
public sealed class ByteBufferException : Exception
{
    public ByteBufferException(string message) : base(message)
    {
    }
}

/// <summary>
/// 可增长的二进制写出器（对应 C++ 的 <c>putInt*(Bytes&amp;, v)</c> 系列自由函数）。
/// 全部大端序。
/// </summary>
public sealed class ByteWriter
{
    private byte[] _buf;
    private int _len;

    public ByteWriter(int capacity = 64)
    {
        _buf = new byte[capacity < 16 ? 16 : capacity];
    }

    public int Length => _len;

    public void WriteUInt8(byte v)
    {
        Ensure(1);
        _buf[_len++] = v;
    }

    /// <summary>写一个有符号字节（Java writeByte）。C# 没有 signed byte 的独立存储，按位写。</summary>
    public void WriteInt8(sbyte v) => WriteUInt8(unchecked((byte)v));

    public void WriteInt16(short v)
    {
        Ensure(2);
        BinaryPrimitives.WriteInt16BigEndian(_buf.AsSpan(_len, 2), v);
        _len += 2;
    }

    /// <summary>写 16 位无符号（Java writeShort 的位模式用途，如 int16 的「无符号」读法）。</summary>
    public void WriteUInt16(ushort v)
    {
        Ensure(2);
        BinaryPrimitives.WriteUInt16BigEndian(_buf.AsSpan(_len, 2), v);
        _len += 2;
    }

    public void WriteInt32(int v)
    {
        Ensure(4);
        BinaryPrimitives.WriteInt32BigEndian(_buf.AsSpan(_len, 4), v);
        _len += 4;
    }

    public void WriteUInt32(uint v)
    {
        Ensure(4);
        BinaryPrimitives.WriteUInt32BigEndian(_buf.AsSpan(_len, 4), v);
        _len += 4;
    }

    public void WriteInt64(long v)
    {
        Ensure(8);
        BinaryPrimitives.WriteInt64BigEndian(_buf.AsSpan(_len, 8), v);
        _len += 8;
    }

    public void WriteBytes(byte[] data) => WriteBytes(data.AsSpan());

    public void WriteBytes(ReadOnlySpan<byte> data)
    {
        Ensure(data.Length);
        data.CopyTo(_buf.AsSpan(_len));
        _len += data.Length;
    }

    /// <summary>取走当前内容（拷贝）。</summary>
    public byte[] ToArray() => _buf.AsSpan(0, _len).ToArray();

    /// <summary>当前内容的只读视图（不拷贝，随后追加会使旧视图失效）。</summary>
    public ReadOnlySpan<byte> AsSpan() => _buf.AsSpan(0, _len);

    /// <summary>回写已写区域内某个位置的 int32（用于「先占位、写完回补长度」的编码手法）。</summary>
    public void PatchInt32At(int pos, int v)
    {
        if (pos < 0 || pos + 4 > _len)
        {
            throw new ByteBufferException("PatchInt32At out of range");
        }

        BinaryPrimitives.WriteInt32BigEndian(_buf.AsSpan(pos, 4), v);
    }

    private void Ensure(int n)
    {
        if (_len + n > _buf.Length)
        {
            int newSize = _buf.Length * 2;
            while (newSize < _len + n)
            {
                newSize *= 2;
            }

            Array.Resize(ref _buf, newSize);
        }
    }
}

/// <summary>只读游标。越界抛 <see cref="ByteBufferException"/>，由上层按「解码失败」处理。</summary>
public sealed class ByteReader
{
    private readonly byte[] _data;
    private int _pos;

    public ByteReader(byte[] data, int offset = 0)
    {
        _data = data ?? throw new ArgumentNullException(nameof(data));
        if (offset < 0 || offset > data.Length)
        {
            throw new ByteBufferException("ByteBuffer offset out of range");
        }

        _pos = offset;
    }

    public byte[] Data => _data;
    public int Pos => _pos;
    public int Remaining => _data.Length - _pos;

    public void Seek(int p)
    {
        if (p < 0 || p > _data.Length)
        {
            throw new ByteBufferException("ByteBuffer seek out of range");
        }

        _pos = p;
    }

    public void Skip(int n)
    {
        Require(n);
        _pos += n;
    }

    public void Require(int n)
    {
        if (Remaining < n)
        {
            throw new ByteBufferException("ByteBuffer underflow");
        }
    }

    /// <summary>不移动游标的读取（对应 Java ByteBuffer.get(index) 系列）。</summary>
    public int PeekInt32(int index) => GetInt32At(_data, index);

    public long PeekInt64(int index) => GetInt64At(_data, index);

    public sbyte ReadInt8()
    {
        Require(1);
        return unchecked((sbyte)_data[_pos++]);
    }

    /// <summary>读一个无符号字节，按 int 返回（Java readUnsignedByte）。</summary>
    public int ReadUnsignedInt8()
    {
        Require(1);
        return _data[_pos++];
    }

    /// <summary>读 16 位，按**无符号**语义返回 int（Java readUnsignedShort）。</summary>
    public int ReadUnsignedInt16()
    {
        Require(2);
        int v = (_data[_pos] << 8) | _data[_pos + 1];
        _pos += 2;
        return v;
    }

    public short ReadInt16()
    {
        Require(2);
        short v = BinaryPrimitives.ReadInt16BigEndian(_data.AsSpan(_pos, 2));
        _pos += 2;
        return v;
    }

    public int ReadInt32()
    {
        Require(4);
        int v = GetInt32At(_data, _pos);
        _pos += 4;
        return v;
    }

    public uint ReadUnsignedInt32() => unchecked((uint)ReadInt32());

    public long ReadInt64()
    {
        Require(8);
        long v = GetInt64At(_data, _pos);
        _pos += 8;
        return v;
    }

    public byte[] ReadBytes(int n)
    {
        Require(n);
        var outBuf = new byte[n];
        Array.Copy(_data, _pos, outBuf, 0, n);
        _pos += n;
        return outBuf;
    }

    /// <summary>静态版本：从任意偏移读取，不持有游标。</summary>
    public static int GetInt32At(byte[] b, int offset)
    {
        if (b.Length < offset + 4)
        {
            throw new ByteBufferException("ByteBuffer underflow (int32)");
        }

        return BinaryPrimitives.ReadInt32BigEndian(b.AsSpan(offset, 4));
    }

    public static long GetInt64At(byte[] b, int offset)
    {
        if (b.Length < offset + 8)
        {
            throw new ByteBufferException("ByteBuffer underflow (int64)");
        }

        return BinaryPrimitives.ReadInt64BigEndian(b.AsSpan(offset, 8));
    }
}

public static class JavaHash
{
    /// <summary>
    /// Java String.hashCode：逐 char（UTF-16 单元）累加，32 位有符号回绕。
    /// C# 的 <see cref="string"/> 本来就是 UTF-16，所以直接迭代 char 即与 Java 逐位一致
    /// （C++ 侧要先从 UTF-8 解码再拆代理对，这里天然不需要）。
    /// unchecked 保证溢出回绕而不是抛异常。
    /// </summary>
    public static int JavaStringHash(string s)
    {
        unchecked
        {
            int h = 0;
            foreach (char c in s)
            {
                h = (h * 31) + c;
            }

            return h;
        }
    }
}

/// <summary>Java 的 int/long 溢出语义：结果按 32/64 位有符号回绕（对应 C++ 的 toInt32）。</summary>
public static class JavaNumber
{
    public static int ToInt32(long v) => unchecked((int)(v & 0xFFFFFFFFL));

    public static string ToStringInvariant(long v) => v.ToString(CultureInfo.InvariantCulture);

    public static string ToStringInvariant(int v) => v.ToString(CultureInfo.InvariantCulture);
}
