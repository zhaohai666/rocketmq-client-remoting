// 消息体压缩/解压（对应 org.apache.rocketmq.common.compression.*）。
//
// 与 Java 的对齐要点：
//   1. 类型解析走 CompressionType.findByValue：**0 与 3 都映射到 ZLIB**
//      （类型位为 0 是老版本客户端的压缩消息，必须按 ZLIB 解，否则静默损坏）。
//   2. 压缩格式是 **zlib 流格式**（RFC1950，带 2 字节头与 adler32 校验），不是
//      raw deflate：Java 用 Deflater/Inflater（nowrap=false），Python 用
//      zlib.compress/decompress，C# 这里用 System.IO.Compression.ZLibStream（同 RFC1950）。
//   3. **失败一律抛异常，绝不静默原样返回**。静默透传会把压缩字节流当作正文交给
//      上层——不报错、不抛异常，属不可察觉的数据损坏（Java 抛 IOException /
//      RuntimeException，这里的语义与其一致）。
using System.Buffers.Binary;
using System.Globalization;
using System.IO.Compression;

namespace RocketMQ.Common;

// 对应 org.apache.rocketmq.common.compression.CompressionType
public static class CompressionType
{
    public const int LZ4 = 1;
    public const int ZSTD = 2;
    public const int ZLIB = 3;

    // Java CompressionType.findByValue：
    //   case 1 -> LZ4; case 2 -> ZSTD; case 0 / case 3 -> ZLIB; default -> 抛错
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

    // Java CompressionType.getCompressionFlag：类型 -> sysFlag 的 bit8~10
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

// 对应 org.apache.rocketmq.common.compression.CompressorFactory。
public static class CompressorFactory
{
    // 当前构建是否编入了 zlib 支持（.NET 内置 ZLibStream，恒为 true）。
    public static bool HasZlibSupport() => true;

    // level 仅在 ZLIB 下有意义（Java 默认 5，也是 Python 侧使用的值）。
    public static byte[] Compress(byte[] src, int compressionType, int level = 5)
    {
        int type = CompressionType.FindByValue(compressionType);
        if (type == CompressionType.ZLIB)
        {
            return ZlibDeflate(src, level);
        }

        // LZ4 / ZSTD 当前未编入（Java 侧由 lz4-java / zstd-jni 提供）。
        // 同样选择抛错而非静默透传，让问题立刻暴露。
        throw new InvalidOperationException(
            "unsupported compression type for compress: " + type.ToString(CultureInfo.InvariantCulture));
    }

    public static byte[] Decompress(byte[] src, int compressionType)
    {
        int type = CompressionType.FindByValue(compressionType);
        if (type == CompressionType.ZLIB)
        {
            return ZlibInflate(src);
        }

        // 关键：不能原样返回。返回压缩字节会被上层当作正文，属静默数据损坏。
        throw new InvalidOperationException(
            "unsupported compression type for decompress: " + type.ToString(CultureInfo.InvariantCulture));
    }

    private static byte[] ZlibDeflate(byte[] src, int level)
    {
        if (level < 0 || level > 9)
        {
            level = 5; // Java/Python 的默认级别
        }

        if (src.Length == 0)
        {
            return Array.Empty<byte>();
        }

        // ZLibStream 默认按 zlib 流格式（RFC1950）写入，与 Java Deflater 一致。
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

        // ZLibStream 默认按 zlib 流格式（RFC1950）解析，与 Java InflaterInputStream 一致。
        // 数据损坏（截断 / 非 zlib 流）会抛 InvalidDataException，由上层按「解码失败」处理，
        // 绝不会把压缩字节原样交出去。
        using var ms = new MemoryStream(src);
        using var zs = new ZLibStream(ms, CompressionMode.Decompress);
        using var outMs = new MemoryStream();
        zs.CopyTo(outMs);
        return outMs.ToArray();
    }

    // .NET 的 ZLibStream 只接受 CompressionLevel 枚举，没有 0~9 数字级别；
    // 这里把数字级别映射到枚举，仅影响压缩率，不影响与 Java 的解压互操作。
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
