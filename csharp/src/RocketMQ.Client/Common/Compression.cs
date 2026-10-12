using System.Buffers.Binary;
using System.Globalization;
using System.IO.Compression;

namespace RocketMQ.Common;

public static class CompressionType
{
    public const int LZ4 = 1;
    public const int ZSTD = 2;
    public const int ZLIB = 3;

    public static int FindByValue(int value)
    {
        switch (value)
        {
            case 1:
                return LZ4;
            case 2:
                return ZSTD;
            case 0: // 兼容老版本：没有类型位的压缩消息按 ZLIB 处理
            case 3:
                return ZLIB;
            default:
                throw new InvalidOperationException(
                    "unknown compress type value: " + value.ToString(CultureInfo.InvariantCulture));
        }
    }

    public static int GetCompressionFlag(int value)
    {
        switch (value)
        {
            case 1:
                return 0x1 << 8; // COMPRESSION_LZ4_TYPE
            case 2:
                return 0x2 << 8; // COMPRESSION_ZSTD_TYPE
            case 3:
                return 0x3 << 8; // COMPRESSION_ZLIB_TYPE
            default:
                throw new InvalidOperationException(
                    "unsupported compress type flag: " + value.ToString(CultureInfo.InvariantCulture));
        }
    }
}

public static class CompressorFactory
{
    // 当前构建是否编入了 zlib 支持（C# 内置 ZLibStream，恒为 true）。
    public static bool HasZlibSupport() => true;

    /// <summary>
    /// 当前进程能否用 LZ4（取决于能否加载到带 LZ4F 导出的 liblz4）。
    /// 探测结果会缓存，调用很便宜。
    /// </summary>
    public static bool HasLz4Support()
    {
        NativeCompressionLibrary.EnsureProbed();
        return NativeLz4Codec.IsAvailable;
    }

    /// <summary>当前进程能否用 ZSTD（取决于能否加载到 libzstd）。探测结果缓存。</summary>
    public static bool HasZstdSupport()
    {
        NativeCompressionLibrary.EnsureProbed();
        return NativeZstdCodec.IsAvailable;
    }

    public static byte[] Compress(byte[] src, int compressionType, int level = 5)
    {
        int type = CompressionType.FindByValue(compressionType);
        switch (type)
        {
            case CompressionType.ZLIB:
                return ZlibDeflate(src, level);

            case CompressionType.LZ4:
                return NativeLz4Codec.IsAvailable
                    ? NativeLz4Codec.Compress(src)
                    : throw NativeCompressionLibrary.Unavailable("LZ4");

            case CompressionType.ZSTD:
                return NativeZstdCodec.IsAvailable
                    ? NativeZstdCodec.Compress(src, level)
                    : throw NativeCompressionLibrary.Unavailable("ZSTD");

            default:
                throw Unsupported("compress", type);
        }
    }

    public static byte[] Decompress(byte[] src, int compressionType)
    {
        int type = CompressionType.FindByValue(compressionType);
        switch (type)
        {
            case CompressionType.ZLIB:
                return ZlibInflate(src);

            case CompressionType.LZ4:
                return NativeLz4Codec.IsAvailable
                    ? NativeLz4Codec.Decompress(src)
                    : throw NativeCompressionLibrary.Unavailable("LZ4");

            case CompressionType.ZSTD:
                return NativeZstdCodec.IsAvailable
                    ? NativeZstdCodec.Decompress(src)
                    : throw NativeCompressionLibrary.Unavailable("ZSTD");

            default:
                throw Unsupported("decompress", type);
        }
    }

    // 关键：不能原样返回。返回压缩字节会被上层当作正文，属静默数据损坏。
    private static InvalidOperationException Unsupported(string what, int type) =>
        new("unsupported compression type for " + what + ": " + type.ToString(CultureInfo.InvariantCulture));

    private static byte[] ZlibDeflate(byte[] src, int level)
    {
        if (level < 0 || level > 9)
        {
            level = 5;
        }

        if (src.Length == 0)
        {
            return Array.Empty<byte>();
        }

        var ms = new MemoryStream();
        using (var zs = new ZLibStream(ms, ToCompressionLevel(level), true))
        {
            zs.Write(src, 0, src.Length);
        }

        return ms.ToArray();
    }

    private static byte[] ZlibInflate(byte[] src)
    {
        if (src.Length == 0)
        {
            return Array.Empty<byte>();
        }

        using var ms = new MemoryStream(src);
        using var zs = new ZLibStream(ms, CompressionMode.Decompress);
        using var outMs = new MemoryStream();
        zs.CopyTo(outMs);
        return outMs.ToArray();
    }

    private static CompressionLevel ToCompressionLevel(int level)
    {
        if (level <= 0)
        {
            return CompressionLevel.NoCompression;
        }

        if (level <= 3)
        {
            return CompressionLevel.Fastest;
        }

        return CompressionLevel.Optimal;
    }
}
