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
//   4. LZ4 / ZSTD 由系统原生库提供（见 Common/NativeCompression.cs）：
//      ZSTD 走 libzstd 的 `ZSTD_compress`/`ZSTD_decompress`，产出/接受**标准 zstd 帧**
//      （magic 0x28 B5 2F FD），对应 Java zstd-jni 的 `ZstdOutputStream`；
//      LZ4 走 liblz4 的 **LZ4F** 帧接口，对应 Java lz4-java 的 `LZ4FrameOutputStream`。
//      ⚠ 不能用 `LZ4_compress_default`——那是 raw block，没有帧头，Java 端读不了。
//      原生库确实加载不到时同样**抛错**（带 "unsupported compression type" 字样），
//      由调用方（Producer 发送路径）按 Java `tryToCompressMessage` 的方式降级为不压缩。
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

    // level 仅在 ZLIB 下有意义（Java 默认 5，也是 Python 侧使用的值）。
    // 例外：Java 的 ZstdCompressor 会把 level 透传给 ZstdOutputStream，这里跟随 Java；
    // LZ4 两边都忽略 level（lz4-java 用 LZ4FrameOutputStream 默认参数）。
    public static byte[] Compress(byte[] src, int compressionType, int level = 5)
    {
        int type = CompressionType.FindByValue(compressionType);
        switch (type)
        {
            case CompressionType.ZLIB:
                return ZlibDeflate(src, level);

            case CompressionType.LZ4:
                // Java Lz4Compressor.compress -> new LZ4FrameOutputStream(...)
                return NativeLz4Codec.IsAvailable
                    ? NativeLz4Codec.Compress(src)
                    : throw NativeCompressionLibrary.Unavailable("LZ4");

            case CompressionType.ZSTD:
                // Java ZstdCompressor.compress -> new ZstdOutputStream(out, level)
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
                // Java Lz4Compressor.decompress -> new LZ4FrameInputStream(...)
                return NativeLz4Codec.IsAvailable
                    ? NativeLz4Codec.Decompress(src)
                    : throw NativeCompressionLibrary.Unavailable("LZ4");

            case CompressionType.ZSTD:
                // Java ZstdCompressor.decompress -> new ZstdInputStream(...)
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
