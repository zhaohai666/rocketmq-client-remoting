// P/Invoke bindings for the system native compression libraries (libzstd / liblz4).
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;

namespace RocketMQ.Common;

/// <summary>
/// Native library handle resolution and availability probing.
///
/// On macOS, <c>NativeLibrary.TryLoad("zstd")</c> and <c>"libzstd"</c> both fail,
/// only <c>"libzstd.dylib"</c> is picked up by dyld's fallback library path,
/// so we must supply our own candidate-name list + <see cref="NativeLibrary.SetDllImportResolver"/>;
/// this also conveniently covers Homebrew's two prefixes (/usr/local and Apple Silicon's /opt/homebrew)
/// as well as Linux's .so/.so.1 naming.
/// </summary>
internal static class NativeCompressionLibrary
{
    /// <summary>The logical name written in DllImport; translated to the real library by <see cref="Resolve"/>.</summary>
    internal const string Zstd = "zstd";

    internal const string Lz4 = "lz4";

    // Candidate names are ordered by "most likely and most specific"; NativeLibrary's default probing (DllImportSearchPath)
    // does not hit these absolute paths on macOS, so they are all listed explicitly.
    // Windows DLL names are not in this table: they are covered by the RMQ_NATIVE_COMPRESSION_DIR
    // probe (see below) in front of LoadFirst, plus the runtime's default resolution.
    private static readonly string[] ZstdCandidates =
    {
        "libzstd.dylib",
        "libzstd.1.dylib",
        "/usr/local/lib/libzstd.dylib",
        "/opt/homebrew/lib/libzstd.dylib",
        "libzstd.so.1",
        "libzstd.so",
        "libzstd.dll",
        "zstd.dll",
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
        "liblz4.dll",
        "lz4.dll",
        "lz4",
    };

    private static readonly Lazy<IntPtr> ZstdHandle = new(() => LoadFirst(WithEnvDir(ZstdCandidates)));

    private static readonly Lazy<IntPtr> Lz4Handle = new(() => LoadFirst(WithEnvDir(Lz4Candidates)));

    // Symbol-level check: if the library loads but is missing the LZ4F_* exports (e.g. replaced by a trimmed static lib),
    // it must be judged unavailable during probing, rather than waiting for DllImport to throw EntryPointNotFoundException.
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
        // The resolver only applies to DllImports initiated by this assembly, and is registered globally once.
        NativeLibrary.SetDllImportResolver(typeof(NativeCompressionLibrary).Assembly, Resolve);
    }

    /// <summary>
    /// Explicitly trigger one probe (so the static constructor runs). Called when the compression path is first used.
    /// </summary>
    internal static void EnsureProbed()
    {
        _ = ZstdAvailable;
        _ = Lz4Available;
    }

    private static IntPtr Resolve(string libraryName, Assembly owner, DllImportSearchPath? searchPath)
    {
        // The resolver is registered inside the static constructor, but may be called by DllImport
        // before the static constructor finishes; here we only read the Lazy fields, which will not
        // trigger registration again, so there is no deadlock risk.
        if (string.Equals(libraryName, Zstd, StringComparison.Ordinal))
        {
            return ZstdHandle.Value;
        }

        if (string.Equals(libraryName, Lz4, StringComparison.Ordinal))
        {
            return Lz4Handle.Value;
        }

        return IntPtr.Zero; // hand back to the runtime's default resolution
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

    /// <summary>
    /// RMQ_NATIVE_COMPRESSION_DIR points to the directory holding liblz4/libzstd (same convention as zlib's
    /// CMAKE_PREFIX_PATH); the candidate names under it are tried before the system's default resolution.
    /// </summary>
    private static string[] WithEnvDir(string[] candidates)
    {
        string? dir = Environment.GetEnvironmentVariable("RMQ_NATIVE_COMPRESSION_DIR");
        if (string.IsNullOrEmpty(dir))
        {
            return candidates;
        }

        var all = new List<string>(candidates.Length * 2);
        foreach (string name in candidates)
        {
            all.Add(Path.Combine(dir, name));
        }

        all.AddRange(candidates);
        return all.ToArray();
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

    /// <summary>Native library version, used only for logging (zstd 10507 -&gt; "1.5.7").</summary>
    internal static string FormatVersion(uint packed)
    {
        // zstd/lz4 pack the version number the same way: major*10000 + minor*100 + patch.
        uint major = packed / 10000;
        uint minor = packed / 100 % 100;
        uint patch = packed % 100;
        return major.ToString(CultureInfo.InvariantCulture) + "." +
               minor.ToString(CultureInfo.InvariantCulture) + "." +
               patch.ToString(CultureInfo.InvariantCulture);
    }

    internal static InvalidOperationException Unavailable(string algorithm) =>
        new("unsupported compression type for " + algorithm + ": native " + algorithm +
            " library is not loadable (need libzstd / liblz4 with LZ4F exports)");
}

/// <summary>
/// Pins a byte[] and exposes an IntPtr, for the native side to read/write by offset.
/// Uses GCHandle instead of unsafe + fixed, to avoid enabling AllowUnsafeBlocks in the library.
/// </summary>
internal readonly struct PinnedBytes : IDisposable
{
    // Empty arrays are not pinned themselves: GCHandle.AddrOfPinnedObject on byte[0] has undefined semantics (may be 0),
    // yet native may still require a non-null pointer even when srcSize==0. Point uniformly at this 1-byte stand-in,
    // so call sites need no special handling for "empty message body" (both zstd/lz4 allow srcSize==0 with a valid pointer).
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

    /// <summary>Address of the first byte (points at the 1-byte stand-in for empty arrays, guaranteeing non-null).</summary>
    public IntPtr Ptr { get; }

    /// <summary>Address of the byte at offset (native streaming interfaces read on from the already-consumed offset).</summary>
    public IntPtr At(int offset) => Ptr + offset;

    public void Dispose()
    {
        if (_handle.IsAllocated)
        {
            _handle.Free();
        }
    }
}

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

    // zstd.h: #define ZSTD_CONTENTSIZE_UNKNOWN (0ULL - 1), ZSTD_CONTENTSIZE_ERROR (0ULL - 2)
    private const ulong ContentSizeUnknown = ulong.MaxValue;
    private const ulong ContentSizeError = ulong.MaxValue - 1;

    // The content-size in the frame header is **untrusted input** (can be forged to be huge); cap it at the same
    // limit as the LZ4 side to avoid a single bad message blowing up the process.
    private const long MaxDecompressed = 512L * 1024 * 1024;

    /// <summary>zstd-jni's Zstd.compress(byte[]) is a single-frame one-shot, same shape here.</summary>
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
            // compressBound is the worst-case upper bound; writing past it means the result is untrustworthy — throw rather than hand back half data.
            throw new InvalidOperationException("zstd compress overflowed its compressBound-sized buffer");
        }

        return Shrink(dst, (int)written);
    }

    internal static byte[] Decompress(byte[] src)
    {
        using var pin = new PinnedBytes(src);

        // Key: do NOT size the buffer from "the first frame's content-size" — when multiple frames are concatenated that
        // is only enough for the first frame, and ZSTD_decompress on insufficient capacity **fails outright**
        // ("Destination buffer is too small"), it does not fill-and-return, so a rewrite that grows-and-retries on fill
        // cannot decode concatenated frames.
        // ZSTD_decompressBound gives exactly the upper bound on the total plaintext of all consecutive frames:
        // it is exact when every frame carries a content-size, otherwise estimated as #blocks * min(128KB, window).
        ulong bound = DecompressBound(pin.Ptr, (nuint)src.Length);
        if (bound == ContentSizeError || bound == ContentSizeUnknown)
        {
            // The frame itself is malformed (illegal magic / truncated). Replay the native error name via findFrameCompressedSize,
            // keeping "bad input must throw" rather than passing through.
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

        // If it still reports too small despite an adequate upper bound, the frame header is lying: throw, never return half plaintext.
        FailIfError(written, "ZSTD_decompress");
        return Shrink(dst, (int)written);
    }

    internal static string VersionText() => NativeCompressionLibrary.FormatVersion(VersionNumber());

    private static int ClampLevel(int level)
    {
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
            throw new InvalidOperationException(what + " is too large for a C# array: " +
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

/// Uses the official LZ4F streaming API; the frame header is aligned with lz4-java's default writing:
/// independent blocks (FLG bit5=1), max64KB blocks (BD=0x40), no content-size written (length unknown in streaming),
/// no content/block checksum => FLG=0x60, header 7 bytes = magic(4)+FLG+BD+HC.
/// </summary>
internal static class NativeLz4Codec
{
    private const string Lib = NativeCompressionLibrary.Lz4;

    /// <summary>lz4frame.h's LZ4F_VERSION, must be passed verbatim when creating a context.</summary>
    private const uint Lz4fVersion = 100;

    /// <summary>lz4frame.h's LZ4F_HEADER_SIZE_MAX: header is at most 19 bytes (magic+FLG+BD+size8+dictID4+HC).</summary>
    private const int HeaderSizeMax = 19;

    /// <summary>LZ4F_max64KB, also the default block size of lz4-java's LZ4FrameOutputStream.</summary>
    private const uint BlockMax64KB = 4;

    /// <summary>LZ4F_blockIndependent: lz4-java default writes independent blocks (so the decoder needs no history window).</summary>
    private const uint BlockIndependent = 1;

    // Destination buffer for decompression; one full block is at most 64KB of plaintext.
    private const int DecompressChunk = 64 * 1024;

    // Guard against "garbage input eating all memory": per-message body limit is far below this.
    private const long MaxDecompressed = 512L * 1024 * 1024;

    internal static bool IsAvailable => NativeCompressionLibrary.Lz4Available;

    /// <summary>
    /// Managed mirror of LZ4F_preferences_t. The field offsets are **not** derived from order:
    /// they were obtained by having the local compiler print offsetof against /usr/local/include/lz4frame.h,
    /// frameInfo{0,4,8,12,16(8B),24,28} + level{32,36,40} + reserved{44,48,52}, total size 56,
    /// so LayoutKind.Explicit pins the offsets to avoid reordering risk across runtimes/platforms.
    /// The C-side enum fields are all 4-byte int/unsigned, contentSize is unsigned long long.
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

        /// <summary>Equivalent to C's LZ4F_INIT_PREFERENCES with blockMode changed to independent.</summary>
        internal static Preferences Lz4LegacyLike() => new()
        {
            BlockSizeID = BlockMax64KB,
            BlockMode = BlockIndependent,
            ContentChecksumFlag = 0,
            FrameType = 0,
            ContentSize = 0, // 0 == unknown: streaming write, no content-size field in the header
            DictID = 0,
            BlockChecksumFlag = 0,
            CompressionLevel = 0, // 0 = library default (fast mode); lz4-java also ignores the caller's level
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

    internal static byte[] Compress(byte[] src)
    {
        Preferences prefs = Preferences.Lz4LegacyLike();
        nuint contextError = CreateCompressionContext(out IntPtr cctx, Lz4fVersion);
        FailIfError(contextError, "LZ4F_createCompressionContext");

        try
        {
            var header = new byte[HeaderSizeMax];
            // worst-case single-block increment + frame footer (end mark 4B, possibly with content checksum)
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
            _ = FreeCompressionContext(cctx); // docs: always succeeds, return value can be ignored
        }
    }

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

                    // No progress at all: input exhausted but frame not yet ended == truncated frame.
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
            throw new InvalidOperationException(what + " is too large for a C# array: " +
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
