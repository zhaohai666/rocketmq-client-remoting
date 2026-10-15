using System.Buffers.Binary;
using System.Globalization;

namespace RocketMQ.Common;

/// <summary>Out-of-bounds/truncation exception: the upper layer uniformly treats it as "decode failure".</summary>
public sealed class ByteBufferException : Exception
{
    public ByteBufferException(string message) : base(message)
    {
    }
}

/// <summary>
/// A growable binary writer (PutInt*/Write* family of APIs).
/// All big-endian.
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

    public void WriteInt8(sbyte v) => WriteUInt8(unchecked((byte)v));

    public void WriteInt16(short v)
    {
        Ensure(2);
        BinaryPrimitives.WriteInt16BigEndian(_buf.AsSpan(_len, 2), v);
        _len += 2;
    }

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

    /// <summary>Take the current content (copy).</summary>
    public byte[] ToArray() => _buf.AsSpan(0, _len).ToArray();

    /// <summary>A read-only view of the current content (no copy; subsequent appends invalidate the old view).</summary>
    public ReadOnlySpan<byte> AsSpan() => _buf.AsSpan(0, _len);

    /// <summary>Write back an int32 at a position already in the written region (for the "reserve first, backfill length after writing" encoding technique).</summary>
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

/// <summary>Read-only cursor. Out-of-bounds throws <see cref="ByteBufferException"/>, treated by the upper layer as "decode failure".</summary>
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

    public int PeekInt32(int index) => GetInt32At(_data, index);

    public long PeekInt64(int index) => GetInt64At(_data, index);

    public sbyte ReadInt8()
    {
        Require(1);
        return unchecked((sbyte)_data[_pos++]);
    }

    public int ReadUnsignedInt8()
    {
        Require(1);
        return _data[_pos++];
    }

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

    /// <summary>Static version: reads from an arbitrary offset without holding a cursor.</summary>
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

public static class LegacyHash
{
    public static int StringHash(string s)
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

public static class LegacyNumber
{
    public static int ToInt32(long v) => unchecked((int)(v & 0xFFFFFFFFL));

    public static string ToStringInvariant(long v) => v.ToString(CultureInfo.InvariantCulture);

    public static string ToStringInvariant(int v) => v.ToString(CultureInfo.InvariantCulture);
}
