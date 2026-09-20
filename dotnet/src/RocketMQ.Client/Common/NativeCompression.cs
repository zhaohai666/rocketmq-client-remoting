// 系统原生压缩库的 P/Invoke 绑定（libzstd / liblz4）。
//
// 为什么走 P/Invoke 而不是托管实现：
//   Java 客户端的 LZ4 / ZSTD 也是 JNI 调 native（lz4-java 内嵌 native、zstd-jni 内嵌
//   libzstd），语义上就是「同一份上游 C 实现」。这里直接 P/Invoke 系统的
//   libzstd.dylib / liblz4.dylib，保证四个语言端在线上产出**同一种帧格式**：
//     - ZSTD：zstd 标准帧（magic 0x28 B5 2F FD）。Java `ZstdCompressor` 用
//       `ZstdOutputStream`/`ZstdInputStream`，对应本文件的 `ZSTD_compress`/`ZSTD_decompress`。
//     - LZ4：**LZ4 Frame** 格式（magic 0x18 4D 22 04）。Java `Lz4Compressor` 用
//       `LZ4FrameOutputStream`/`LZ4FrameInputStream`，对应本文件的 **LZ4F** 流式 API。
//       ⚠ 不能用 `LZ4_compress_default`：它产出 raw block，没有帧头/块描述符/结束标记，
//       Java `LZ4FrameInputStream` 读不了。
//
// 约束（与 Common/Compression.cs 的文件头规则一致）：**绝不静默透传**。
// 原生库加载失败、导出符号缺失、native 返回错误码，一律抛异常；调用方（Producer 发送路径）
// 可以像 Java `DefaultMQProducerImpl#tryToCompressMessage` 那样降级为「不压缩且不打压缩位」，
// 但这里不会把压缩字节当成正文交出去。
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;

namespace RocketMQ.Common;

/// <summary>
/// 原生库句柄解析与可用性探测。
///
/// macOS 上 <c>NativeLibrary.TryLoad("zstd")</c> 与 <c>"libzstd"</c> 都会失败，
/// 只有 <c>"libzstd.dylib"</c> 能被 dyld 的 fallback library path 命中，
/// 因此必须自带候选名列表 + <see cref="NativeLibrary.SetDllImportResolver"/>；
/// 这也顺带覆盖 Homebrew 的两种前缀（/usr/local 与 Apple Silicon 的 /opt/homebrew）
/// 以及 Linux 的 .so/.so.1 命名。
/// </summary>
internal static class NativeCompressionLibrary
{
    /// <summary>DllImport 里写的逻辑名；由 <see cref="Resolve"/> 翻译成真实库。</summary>
    internal const string Zstd = "zstd";

    internal const string Lz4 = "lz4";

    // 候选名按「最可能且最具体」排序；NativeLibrary 的默认探测（DllImportSearchPath）
    // 在 macOS 上不会命中这些绝对路径，所以全部显式列出。
    private static readonly string[] ZstdCandidates =
    {
        "libzstd.dylib",
        "libzstd.1.dylib",
        "/usr/local/lib/libzstd.dylib",
        "/opt/homebrew/lib/libzstd.dylib",
        "libzstd.so.1",
        "libzstd.so",
        "zstd",
    };

    private static readonly string[] Lz4Candidates =
    {
        "liblz4.dylib",
        "liblz4.1.dylib",
        "/usr/local/lib/liblz4.dylib",
        "/opt/homebrew/lib/liblz4.dylib",
        "liblz4.so.1",
        "liblz4.so",
        "lz4",
    };

    private static readonly Lazy<IntPtr> ZstdHandle = new(() => LoadFirst(ZstdCandidates));
    private static readonly Lazy<IntPtr> Lz4Handle = new(() => LoadFirst(Lz4Candidates));

    // 符号级校验：库能加载但少了 LZ4F_* 导出（例如被换成了裁剪过的静态库）时，
    // 必须在探测阶段就判定不可用，而不是等 DllImport 抛 EntryPointNotFoundException。
    private static readonly Lazy<bool> ZstdUsable = new(() => HasAll(ZstdHandle.Value,
        "ZSTD_compress", "ZSTD_decompress", "ZSTD_compressBound", "ZSTD_decompressBound",
        "ZSTD_findFrameCompressedSize", "ZSTD_isError", "ZSTD_getErrorName"));

    private static readonly Lazy<bool> Lz4Usable = new(() => HasAll(Lz4Handle.Value,
        "LZ4F_createCompressionContext", "LZ4F_compressBegin", "LZ4F_compressUpdate",
        "LZ4F_compressEnd", "LZ4F_compressBound", "LZ4F_freeCompressionContext",
        "LZ4F_createDecompressionContext", "LZ4F_decompress", "LZ4F_freeDecompressionContext",
        "LZ4F_isError", "LZ4F_getErrorName"));

    internal static bool ZstdAvailable => ZstdUsable.Value;

    internal static bool Lz4Available => Lz4Usable.Value;

    static NativeCompressionLibrary()
    {
        // 解析器只对「本程序集发起的 DllImport」生效，且是全局一次性注册。
        NativeLibrary.SetDllImportResolver(typeof(NativeCompressionLibrary).Assembly, Resolve);
    }

    /// <summary>
    /// 显式触发一次探测（让静态构造函数跑起来）。压缩路径首次用到时调用。
    /// </summary>
    internal static void EnsureProbed()
    {
        _ = ZstdAvailable;
        _ = Lz4Available;
    }

    private static IntPtr Resolve(string libraryName, Assembly owner, DllImportSearchPath? searchPath)
    {
        // 静态构造函数里才注册解析器，而解析器可能在静态构造函数完成前被 DllImport
        // 调到；这里只读 Lazy 字段，不会再触发注册，无死锁风险。
        if (string.Equals(libraryName, Zstd, StringComparison.Ordinal))
        {
            return ZstdHandle.Value;
        }

        if (string.Equals(libraryName, Lz4, StringComparison.Ordinal))
        {
            return Lz4Handle.Value;
        }

        return IntPtr.Zero; // 交回运行时默认解析
    }

    private static IntPtr LoadFirst(string[] candidates)
    {
        foreach (string name in candidates)
        {
            if (NativeLibrary.TryLoad(name, typeof(NativeCompressionLibrary).Assembly, null, out IntPtr handle))
            {
                return handle;
            }
        }

        return IntPtr.Zero;
    }

    private static bool HasAll(IntPtr handle, params string[] symbols)
    {
        if (handle == IntPtr.Zero)
        {
            return false;
        }

        foreach (string s in symbols)
        {
            if (!NativeLibrary.TryGetExport(handle, s, out _))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>原生库版本，仅用于日志（zstd 10507 -&gt; "1.5.7"）。</summary>
    internal static string FormatVersion(uint packed)
    {
        // zstd/lz4 的版本号打包方式一致：major*10000 + minor*100 + patch。
        uint major = packed / 10000;
        uint minor = packed / 100 % 100;
        uint patch = packed % 100;
        return major.ToString(CultureInfo.InvariantCulture) + "." +
               minor.ToString(CultureInfo.InvariantCulture) + "." +
               patch.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>不可用时的统一错误文案：与 Java 侧「没有该 codec 就抛」的语义一致。</summary>
    internal static InvalidOperationException Unavailable(string algorithm) =>
        new("unsupported compression type for " + algorithm + ": native " + algorithm +
            " library is not loadable (need libzstd / liblz4 with LZ4F exports)");
}

/// <summary>
/// 把 byte[] 钉住并暴露 IntPtr，供 native 侧按偏移读写。
/// 用 GCHandle 而不是 unsafe + fixed，是为了不在类库里打开 AllowUnsafeBlocks。
/// </summary>
internal readonly struct PinnedBytes : IDisposable
{
    // 空数组不钉自己：GCHandle 对 byte[0] 的 AddrOfPinnedObject 语义未定义（可能是 0），
    // 而 native 即便 srcSize==0 也可能要求指针非空。统一指向这个 1 字节替身，
    // 调用点就不需要为「空消息体」特判（zstd/lz4 都允许 srcSize==0 + 合法指针）。
    private static readonly byte[] EmptyStandIn = new byte[1];
    private static readonly GCHandle EmptyStandInHandle =
        GCHandle.Alloc(EmptyStandIn, GCHandleType.Pinned);

    private readonly GCHandle _handle;

    public PinnedBytes(byte[] data)
    {
        if (data.Length == 0)
        {
            _handle = default;
            Ptr = EmptyStandInHandle.AddrOfPinnedObject();
            return;
        }

        _handle = GCHandle.Alloc(data, GCHandleType.Pinned);
        Ptr = _handle.AddrOfPinnedObject();
    }

    /// <summary>首字节地址（空数组时指向 1 字节替身，保证非 null）。</summary>
    public IntPtr Ptr { get; }

    /// <summary>第 offset 个字节的地址（native 流式接口要按已消费偏移续读）。</summary>
    public IntPtr At(int offset) => Ptr + offset;

    public void Dispose()
    {
        if (_handle.IsAllocated)
        {
            _handle.Free();
        }
    }
}

/// <summary>
/// zstd 标准帧编解码。对应 Java <c>org.apache.rocketmq.common.compression.ZstdCompressor</c>
/// （<c>ZstdOutputStream</c> / <c>ZstdInputStream</c>，zstd-jni）。
/// </summary>
internal static class NativeZstdCodec
{
    private const string Lib = NativeCompressionLibrary.Zstd;

    internal static bool IsAvailable => NativeCompressionLibrary.ZstdAvailable;

    [DllImport(Lib, EntryPoint = "ZSTD_compress")]
    private static extern nuint Compress(IntPtr dst, nuint dstCapacity, IntPtr src, nuint srcSize, int level);

    [DllImport(Lib, EntryPoint = "ZSTD_compressBound")]
    private static extern nuint CompressBound(nuint srcSize);

    [DllImport(Lib, EntryPoint = "ZSTD_decompress")]
    private static extern nuint Decompress(IntPtr dst, nuint dstCapacity, IntPtr src, nuint compressedSize);

    [DllImport(Lib, EntryPoint = "ZSTD_decompressBound")]
    private static extern ulong DecompressBound(IntPtr src, nuint srcSize);

    [DllImport(Lib, EntryPoint = "ZSTD_findFrameCompressedSize")]
    private static extern nuint FindFrameCompressedSize(IntPtr src, nuint srcSize);

    [DllImport(Lib, EntryPoint = "ZSTD_isError")]
    private static extern uint IsError(nuint code);

    [DllImport(Lib, EntryPoint = "ZSTD_getErrorName")]
    private static extern IntPtr GetErrorName(nuint code);

    [DllImport(Lib, EntryPoint = "ZSTD_maxCLevel")]
    private static extern int MaxCLevel();

    [DllImport(Lib, EntryPoint = "ZSTD_versionNumber")]
    private static extern uint VersionNumber();

    // zstd.h: #define ZSTD_CONTENTSIZE_UNKNOWN (0ULL - 1)、ZSTD_CONTENTSIZE_ERROR (0ULL - 2)
    private const ulong ContentSizeUnknown = ulong.MaxValue;
    private const ulong ContentSizeError = ulong.MaxValue - 1;

    // 帧头里的 content-size 是**不可信输入**（可以被伪造得极大），照 LZ4 侧同一上限拦住，
    // 避免一条坏消息把进程撑爆。
    private const long MaxDecompressed = 512L * 1024 * 1024;

    /// <summary>zstd-jni 的 Zstd.compress(byte[]) 就是单帧 one-shot，这里同一形状。</summary>
    internal static byte[] Compress(byte[] src, int level)
    {
        nuint bound = CompressBound((nuint)src.Length);
        FailIfError(bound, "ZSTD_compressBound");
        var dst = new byte[CheckedLength((long)bound, "compressed body")];

        nuint written;
        using (var pin = new PinnedBytes(src))
        using (var pinDst = new PinnedBytes(dst))
        {
            written = Compress(pinDst.Ptr, (nuint)dst.Length, pin.Ptr, (nuint)src.Length, ClampLevel(level));
        }

        FailIfError(written, "ZSTD_compress");
        if ((long)written > dst.Length)
        {
            // compressBound 是最坏情况上界，写超说明结果不可信——宁可抛错也不交半截数据。
            throw new InvalidOperationException("zstd compress overflowed its compressBound-sized buffer");
        }

        return Shrink(dst, (int)written);
    }

    /// <summary>
    /// 解一个或多个**串接的** zstd 帧（Java <c>ZstdInputStream</c> 可读多帧，
    /// 例如 <c>cat a.zst b.zst</c>；<c>ZSTD_decompress</c> 本身也支持多帧，
    /// 但它要求 dstCapacity 是**所有帧**明文总量）。
    /// </summary>
    internal static byte[] Decompress(byte[] src)
    {
        using var pin = new PinnedBytes(src);

        // 关键：不能用「第一帧的 content-size」定缓冲——多帧串接时那只够第一帧，
        // 而 ZSTD_decompress 容量不足是**直接报错**（"Destination buffer is too small"），
        // 不是写满返回，所以按写满扩容重试的写法解不了串接帧。
        // ZSTD_decompressBound 给的正是「所有连续帧明文总量」的上界：
        // 每帧都带 content-size 时它是精确值，否则按 #blocks * min(128KB, window) 估算。
        ulong bound = DecompressBound(pin.Ptr, (nuint)src.Length);
        if (bound == ContentSizeError || bound == ContentSizeUnknown)
        {
            // 帧本身有问题（非法 magic / 截断）。用 findFrameCompressedSize 复述原生错误名，
            // 保持「坏输入必抛」而不是透传。
            FailIfError(FindFrameCompressedSize(pin.Ptr, (nuint)src.Length), "ZSTD_decompressBound");
            throw new InvalidOperationException("zstd decompress failed: cannot bound decompressed size");
        }

        if (bound > MaxDecompressed)
        {
            throw new InvalidOperationException("zstd decompressed body exceeds " +
                                                MaxDecompressed.ToString(CultureInfo.InvariantCulture) + " bytes");
        }

        var dst = new byte[CheckedLength((long)bound, "decompressed body")];
        nuint written;
        using (var pinDst = new PinnedBytes(dst))
        {
            written = Decompress(pinDst.Ptr, (nuint)dst.Length, pin.Ptr, (nuint)src.Length);
        }

        // 上界充足的前提下若仍报 too small，说明帧头在撒谎：抛错，绝不返回半截明文。
        FailIfError(written, "ZSTD_decompress");
        return Shrink(dst, (int)written);
    }

    internal static string VersionText() => NativeCompressionLibrary.FormatVersion(VersionNumber());

    private static int ClampLevel(int level)
    {
        // Java 把 compressLevel 透传给 ZstdOutputStream；越界会被 zstd 判为错误，
        // 这里按 zstd 的语义钳到 [1, maxCLevel]，0 表示「用库默认级别 3」。
        if (level <= 0)
        {
            return 0;
        }

        int max = MaxCLevel();
        return level > max ? max : level;
    }

    private static byte[] Shrink(byte[] src, int length)
    {
        if (length == src.Length)
        {
            return src;
        }

        var result = new byte[length];
        Buffer.BlockCopy(src, 0, result, 0, length);
        return result;
    }

    private static int CheckedLength(long size, string what)
    {
        if (size < 0 || size > int.MaxValue - 1)
        {
            throw new InvalidOperationException(what + " is too large for a .NET array: " +
                                                size.ToString(CultureInfo.InvariantCulture));
        }

        return (int)size;
    }

    private static void FailIfError(nuint code, string function)
    {
        if (IsError(code) != 0)
        {
            throw new InvalidOperationException(function + " failed: " + ErrorName(code));
        }
    }

    private static string ErrorName(nuint code)
    {
        IntPtr p = GetErrorName(code);
        return p == IntPtr.Zero ? code.ToString(CultureInfo.InvariantCulture) : (Marshal.PtrToStringUTF8(p) ?? "unknown");
    }
}

/// <summary>
/// LZ4 Frame 编解码。对应 Java <c>org.apache.rocketmq.common.compression.Lz4Compressor</c>
/// （<c>LZ4FrameOutputStream</c> / <c>LZ4FrameInputStream</c>，lz4-java）。
///
/// 走官方 LZ4F 流式 API，帧头与 lz4-java 默认写法对齐：
/// 独立块（FLG bit5=1）、max64KB 块（BD=0x40）、不写 content-size（流式时长度未知）、
/// 不带 content/block checksum ⇒ FLG=0x60，头 7 字节 = magic(4)+FLG+BD+HC。
/// </summary>
internal static class NativeLz4Codec
{
    private const string Lib = NativeCompressionLibrary.Lz4;

    /// <summary>lz4frame.h 的 LZ4F_VERSION，创建上下文时必须原样传入。</summary>
    private const uint Lz4fVersion = 100;

    /// <summary>lz4frame.h 的 LZ4F_HEADER_SIZE_MAX：头最大 19 字节（magic+FLG+BD+size8+dictID4+HC）。</summary>
    private const int HeaderSizeMax = 19;

    /// <summary>LZ4F_max64KB，也是 lz4-java LZ4FrameOutputStream 的默认块大小。</summary>
    private const uint BlockMax64KB = 4;

    /// <summary>LZ4F_blockIndependent：lz4-java 默认写独立块（解码端因此无需回溯窗口）。</summary>
    private const uint BlockIndependent = 1;

    // 解压时的目的缓冲；一个满块最多 64KB 明文。
    private const int DecompressChunk = 64 * 1024;

    // 防「垃圾输入把内存吃光」：单条消息体上限远小于此。
    private const long MaxDecompressed = 512L * 1024 * 1024;

    internal static bool IsAvailable => NativeCompressionLibrary.Lz4Available;

    /// <summary>
    /// LZ4F_preferences_t 的托管镜像。字段偏移**不是**靠顺序推导出来的：
    /// 用本地编译器对 /usr/local/include/lz4frame.h 打印 offsetof 实测得到
    /// frameInfo{0,4,8,12,16(8B),24,28} + level{32,36,40} + reserved{44,48,52}、总大小 56，
    /// 所以用 LayoutKind.Explicit 把偏移钉死，避免不同运行时/平台的重排风险。
    /// C 侧枚举字段都是 4 字节 int/unsigned，contentSize 是 unsigned long long。
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 56)]
    private struct Preferences
    {
        [FieldOffset(0)] public uint BlockSizeID;
        [FieldOffset(4)] public uint BlockMode;
        [FieldOffset(8)] public uint ContentChecksumFlag;
        [FieldOffset(12)] public uint FrameType;
        [FieldOffset(16)] public ulong ContentSize;
        [FieldOffset(24)] public uint DictID;
        [FieldOffset(28)] public uint BlockChecksumFlag;
        [FieldOffset(32)] public int CompressionLevel;
        [FieldOffset(36)] public uint AutoFlush;
        [FieldOffset(40)] public uint FavorDecSpeed;
        [FieldOffset(44)] public uint Reserved0;
        [FieldOffset(48)] public uint Reserved1;
        [FieldOffset(52)] public uint Reserved2;

        /// <summary>等价于 C 的 LZ4F_INIT_PREFERENCES 后把 blockMode 改成 independent。</summary>
        internal static Preferences Lz4JavaLike() => new()
        {
            BlockSizeID = BlockMax64KB,
            BlockMode = BlockIndependent,
            ContentChecksumFlag = 0,
            FrameType = 0,
            ContentSize = 0, // 0 == unknown：流式写法，头里不留 content-size 字段
            DictID = 0,
            BlockChecksumFlag = 0,
            CompressionLevel = 0, // 0 = 库默认（fast mode）；lz4-java 也忽略上层的 level
            AutoFlush = 0,
            FavorDecSpeed = 0,
        };
    }

    [DllImport(Lib, EntryPoint = "LZ4F_createCompressionContext")]
    private static extern nuint CreateCompressionContext(out IntPtr cctx, uint version);

    [DllImport(Lib, EntryPoint = "LZ4F_freeCompressionContext")]
    private static extern nuint FreeCompressionContext(IntPtr cctx);

    [DllImport(Lib, EntryPoint = "LZ4F_compressBegin")]
    private static extern nuint CompressBegin(IntPtr cctx, IntPtr dst, nuint dstCapacity, ref Preferences prefs);

    [DllImport(Lib, EntryPoint = "LZ4F_compressBound")]
    private static extern nuint CompressBound(nuint srcSize, ref Preferences prefs);

    [DllImport(Lib, EntryPoint = "LZ4F_compressUpdate")]
    private static extern nuint CompressUpdate(IntPtr cctx, IntPtr dst, nuint dstCapacity,
        IntPtr src, nuint srcSize, IntPtr options);

    [DllImport(Lib, EntryPoint = "LZ4F_compressEnd")]
    private static extern nuint CompressEnd(IntPtr cctx, IntPtr dst, nuint dstCapacity, IntPtr options);

    [DllImport(Lib, EntryPoint = "LZ4F_createDecompressionContext")]
    private static extern nuint CreateDecompressionContext(out IntPtr dctx, uint version);

    [DllImport(Lib, EntryPoint = "LZ4F_freeDecompressionContext")]
    private static extern nuint FreeDecompressionContext(IntPtr dctx);

    [DllImport(Lib, EntryPoint = "LZ4F_decompress")]
    private static extern nuint Decompress(IntPtr dctx, IntPtr dst, ref nuint dstSize,
        IntPtr src, ref nuint srcSize, IntPtr options);

    [DllImport(Lib, EntryPoint = "LZ4F_isError")]
    private static extern uint IsError(nuint code);

    [DllImport(Lib, EntryPoint = "LZ4F_getErrorName")]
    private static extern IntPtr GetErrorName(nuint code);

    [DllImport(Lib, EntryPoint = "LZ4_versionNumber")]
    private static extern uint VersionNumber();

    /// <summary>
    /// 产出 LZ4 Frame。level 被忽略——Java <c>Lz4Compressor#compress(src, level)</c>
    /// 也不看 level（<c>new LZ4FrameOutputStream(baos)</c> 用的是默认参数）。
    /// </summary>
    internal static byte[] Compress(byte[] src)
    {
        Preferences prefs = Preferences.Lz4JavaLike();
        nuint contextError = CreateCompressionContext(out IntPtr cctx, Lz4fVersion);
        FailIfError(contextError, "LZ4F_createCompressionContext");

        try
        {
            var header = new byte[HeaderSizeMax];
            // 单块最坏增量 + 帧尾（end mark 4B，可能带 content checksum）
            nuint bodyCapacity = CompressBound((nuint)src.Length, ref prefs);
            FailIfError(bodyCapacity, "LZ4F_compressBound");
            nuint tailCapacity = CompressBound(0, ref prefs);
            FailIfError(tailCapacity, "LZ4F_compressBound");

            int headerLength;
            using (var pinHeader = new PinnedBytes(header))
            {
                nuint n = CompressBegin(cctx, pinHeader.Ptr, (nuint)header.Length, ref prefs);
                FailIfError(n, "LZ4F_compressBegin");
                headerLength = (int)n;
            }

            var body = new byte[CheckedLength((long)bodyCapacity, "lz4 compressed body")];
            int bodyLength = 0;
            if (src.Length > 0)
            {
                using var pinSrc = new PinnedBytes(src);
                using var pinBody = new PinnedBytes(body);
                nuint n = CompressUpdate(cctx, pinBody.Ptr, (nuint)body.Length, pinSrc.Ptr, (nuint)src.Length, IntPtr.Zero);
                FailIfError(n, "LZ4F_compressUpdate");
                bodyLength = (int)n;
            }

            var tail = new byte[CheckedLength((long)tailCapacity, "lz4 frame footer")];
            int tailLength;
            using (var pinTail = new PinnedBytes(tail))
            {
                nuint n = CompressEnd(cctx, pinTail.Ptr, (nuint)tail.Length, IntPtr.Zero);
                FailIfError(n, "LZ4F_compressEnd");
                tailLength = (int)n;
            }

            var result = new byte[headerLength + bodyLength + tailLength];
            Buffer.BlockCopy(header, 0, result, 0, headerLength);
            Buffer.BlockCopy(body, 0, result, headerLength, bodyLength);
            Buffer.BlockCopy(tail, 0, result, headerLength + bodyLength, tailLength);
            return result;
        }
        finally
        {
            _ = FreeCompressionContext(cctx); // 文档：总是成功，返回值可忽略
        }
    }

    /// <summary>
    /// 解 LZ4 Frame（可含多个串接帧）。语义对齐 Java <c>LZ4FrameInputStream</c>：
    /// 截断/坏块一律抛错，绝不返回半截明文。
    /// </summary>
    internal static byte[] Decompress(byte[] src)
    {
        if (src.Length == 0)
        {
            throw new InvalidOperationException("lz4 decompress failed: input is empty, not an LZ4 frame");
        }

        nuint contextError = CreateDecompressionContext(out IntPtr dctx, Lz4fVersion);
        FailIfError(contextError, "LZ4F_createDecompressionContext");

        var chunk = new byte[DecompressChunk];
        var outMs = new MemoryStream();

        try
        {
            using var pinSrc = new PinnedBytes(src);
            using var pinChunk = new PinnedBytes(chunk);

            int srcPos = 0;
            while (srcPos < src.Length)
            {
                bool frameDone = false;
                while (!frameDone)
                {
                    nuint dstAvail = (nuint)chunk.Length;
                    nuint srcAvail = (nuint)(src.Length - srcPos);
                    nuint hint = Decompress(dctx, pinChunk.Ptr, ref dstAvail,
                        pinSrc.At(srcPos), ref srcAvail, IntPtr.Zero);

                    if (IsError(hint) != 0)
                    {
                        throw new InvalidOperationException("LZ4F_decompress failed: " + ErrorName(hint));
                    }

                    if (dstAvail > 0)
                    {
                        if (outMs.Length + (long)dstAvail > MaxDecompressed)
                        {
                            throw new InvalidOperationException("lz4 decompressed body exceeds " +
                                                                MaxDecompressed + " bytes");
                        }

                        outMs.Write(chunk, 0, (int)dstAvail);
                    }

                    srcPos += (int)srcAvail;
                    frameDone = hint == 0;

                    // 没有任何推进：输入耗尽但帧还没结束 == 截断帧。
                    if (!frameDone && srcAvail == 0 && dstAvail == 0)
                    {
                        throw new InvalidOperationException(
                            "LZ4F_decompress failed: truncated LZ4 frame (input exhausted before end mark)");
                    }
                }
            }
        }
        finally
        {
            _ = FreeDecompressionContext(dctx);
        }

        return outMs.ToArray();
    }

    internal static string VersionText() => NativeCompressionLibrary.FormatVersion(VersionNumber());

    private static int CheckedLength(long size, string what)
    {
        if (size < 0 || size > int.MaxValue - 1)
        {
            throw new InvalidOperationException(what + " is too large for a .NET array: " +
                                                size.ToString(CultureInfo.InvariantCulture));
        }

        return (int)size;
    }

    private static void FailIfError(nuint code, string function)
    {
        if (IsError(code) != 0)
        {
            throw new InvalidOperationException(function + " failed: " + ErrorName(code));
        }
    }

    private static string ErrorName(nuint code)
    {
        IntPtr p = GetErrorName(code);
        return p == IntPtr.Zero ? code.ToString(CultureInfo.InvariantCulture) : (Marshal.PtrToStringUTF8(p) ?? "unknown");
    }
}
