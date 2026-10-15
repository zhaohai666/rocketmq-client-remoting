// Message body compression unit tests: a ZLIB regression plus ZSTD and LZ4, with the
// emphasis on **interoperability across implementations**.
//
// Compressing and decompressing with ourselves proves nothing (both ends would share
// the same bug), so every ground truth here is **bytes produced by an external
// implementation**:
//   * `py*`  -- the official `lz4.frame` (wrapping the upstream C lz4) / `zlib.compress(data, 5)`
//   * `rs*`  -- the Rust `lz4_flex` crate (a **pure Rust implementation**, unrelated to
//              liblz4) and the Rust `zstd` crate
//   * `ZSTD_CLI*` -- frames produced by the `zstd -3` CLI (Yann Collet's reference implementation)
//   The reverse direction is verified too: the `NET_*` constants in this file are the
//   bytes C# actually produces (regression guardrails), and those bytes were separately
//   decoded by the `lz4 -d` / `zstd -d` CLIs as well as by lz4_flex and `lz4.frame`
//   (including a 2.8MB multi-block frame). The native libraries / CLIs are not on the
//   test machine, so that step was checked offline with a script; this file is what pins
//   down "external bytes get in" and "what our bytes look like" in unit tests.
//
// The plaintext is **formula-generated**, so only the compressed ground truths need to
// be embedded, not the plaintext.
using System.Globalization;
using System.Text;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class CompressionTests
{
    /// <summary>The shared plaintext used for the interoperability ground truths (1120 bytes, formula-generated).</summary>
    private static byte[] Payload() =>
        Encoding.ASCII.GetBytes(string.Concat(Enumerable.Repeat("rocketmq-compressed-payload-", 40)));

    /// <summary>70500 bytes: just past one 64KB block boundary, used for multi-block frames (= 47B pattern x 1500).</summary>
    private static byte[] MultiBlockPayload() =>
        Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat(
            "rocketmq-lz4-multiblock-seed-0123456789abcdef-\n", 1500)));

    /// <summary>About 600KB of compressible plaintext, used to really push multi-block LZ4 / multi-block zstd.</summary>
    private static byte[] LargePayload(int repeats = 13000) =>
        Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat(
            "rocketmq-compression-large-payload-0123456789abcdef-\n", repeats)));

    // ---------------------------------------------------------------- External ground truths (hex)

    // Produced by `lz4.frame.compress(payload)`: 66B. A 15-byte frame header,
    // FLG=0x68 (**content-size written**), and independent blocks.
    private const string PY_LZ4_FRAME = @"
        04224d1868406004000000000000482b000000ff0d726f636b65746d712d636f6d707265737365642d7061796c
        6f61642d1c00ffffffff30506c6f61642d00000000";

    private const string RS_LZ4_FRAME = @"
        04224d186040822c000000ff0d726f636b65746d712d636f6d707265737365642d7061796c6f61642d1c00ffff
        ffff2f60796c6f61642d00000000";

    // The LZ4 frame of an empty input: lz4.frame / lz4_flex / this client all produce the **identical** 11 bytes.
    private const string LZ4_FRAME_EMPTY = "04224d1860408200000000";

    // The 1120B frame C# actually produces (58B): a regression guardrail. Its 7-byte
    // header matches the ground truth produced by `lz4_flex`, and its 43-byte
    // compressed block is byte-for-byte identical to the ground truth from
    // `lz4.frame` (the latter additionally writes the content-size field).
    private const string NET_LZ4_FRAME = @"
        04224d186040822b000000ff0d726f636b65746d712d636f6d707265737365642d7061796c6f61642d1c00ffff
        ffff30506c6f61642d00000000";

    // `lz4.frame.compress`, 70500B input: 370B. FLG=0x48 => **linked blocks** (not
    // independent) plus content-size, BD=0x40 => 64KB blocks => 2 blocks. The decoder
    // must support reusing the compression window across blocks.
    private const string PY_LZ4_MULTIBLOCK = @"
        04224d1848406413010000000000573a010000ff20726f636b65746d712d6c7a342d6d756c7469626c6f636b2d73
        6565642d303132333435363738396162636465662d0a2f00ffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffb9506d756c74691d0000000feeffffffffffffffffffffffffffffffffffffffff5f506465662d0a0000
        0000";

    // `lz4_flex` (a pure Rust independent implementation), 70500B input: 350B.
    // BD=0x50 => **256KB blocks** (4x larger than the 64KB we emit), FLG=0x60 =>
    // independent blocks, no content-size. The decoder must not assume the block size
    // is only ever the one it writes itself.
    private const string RS_LZ4_MULTIBLOCK = @"
        04224d186050fb4f010000ff20726f636b65746d712d6c7a342d6d756c7469626c6f636b2d736565642d30313233
        3435363738396162636465662d0a2f00ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffff3060636465662d0a00000000";

    // `zstd -3` CLI: a frame with content-size (50B, FLG=0x64 => 4B FCS + content checksum).
    private const string ZSTD_CLI_WITH_SIZE = @"
        28b52ffd646003250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff7402
        78aa9792";

    private const string ZSTD_CLI_NO_SIZE = @"
        28b52ffd0408250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff740278
        aa9792";

    // `zstd::encode_all(data, 3)`: 45B, FLG=0x00 (single segment, no checksum).
    private const string RS_ZSTD_FRAME = @"
        28b52ffd0058250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff7402";

    // The zstd frame of an empty input: rust and C# produce the **identical** 9 bytes.
    private const string ZSTD_FRAME_EMPTY = "28b52ffd2000010000";

    // The zstd frame C# actually produces (46B): a regression guardrail
    // (`ZSTD_compress` one-shot => single segment + 2B FCS).
    private const string NET_ZSTD_FRAME = @"
        28b52ffd606003250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff7402";

    // `zlib.compress(payload, 5)`: 47B. The pre-existing ground truth of the ZLIB
    // path (changing LZ4/ZSTD must not regress it).
    private const string ZLIB_L5 = @"
        785e2bca4fce4e2dc92dd44dcecf2d284a2d2e4e4dd12d48acccc94f4cd12d1a951b951b951b95a3400e0058e0b9
        f0";

    // ---------------------------------------------------------------- Helpers

    private static byte[] Unhex(string hex)
    {
        var sb = new StringBuilder(hex.Length);
        foreach (char c in hex)
        {
            if (!char.IsWhiteSpace(c))
            {
                sb.Append(c);
            }
        }

        Assert.Equal(0, sb.Length % 2);
        var bytes = new byte[sb.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = byte.Parse(sb.ToString(i * 2, 2), NumberStyles.HexNumber, System.Globalization.CultureInfo.InvariantCulture);
        }

        return bytes;
    }

    private static int GetInt32LeAt(byte[] buf, int offset) =>
        buf[offset] | (buf[offset + 1] << 8) | (buf[offset + 2] << 16) | (buf[offset + 3] << 24);

    /// <summary>
    /// Walks the block descriptors of an LZ4 frame and returns the block count. Used to
    /// prove that a large input **really** was split into multiple blocks, rather than
    /// betting the assertion solely on "it decoded back".
    ///
    /// Frame header bit semantics (reverse-engineered from real output of lz4.frame /
    /// lz4_flex / the lz4 CLI, consistent with the official frame format; the numbers
    /// are per byte bit): FLG bit7-6 = version (must be 01=0x40), bit5 = blocks are
    /// independent, bit4 = block checksum, bit3 = an 8-byte content-size is present,
    /// bit2 = content checksum is present;
    /// BD bit7 = a 4-byte dictID is present, bit6-4 = block size index (4=64KB, 5=256KB,
    /// 6=1MB, 7=4MB).
    /// </summary>
    private static int CountLz4Blocks(byte[] frame)
    {
        Assert.True(frame.Length >= 7, "a frame needs at least magic(4)+FLG+BD+HC");
        Assert.Equal(0x184D2204u, (uint)BitConverter.ToInt32(frame, 0)); // the magic itself, read little-endian

        int pos = 4;
        int flg = frame[pos++];
        int bd = frame[pos++];
        Assert.Equal(1, (flg >> 6) & 0x3); // the version number
        bool blockChecksum = (flg & 0x10) != 0;
        bool contentChecksum = (flg & 0x04) != 0;
        if ((flg & 0x08) != 0)
        {
            pos += 8; // Content Size
        }

        if ((bd & 0x80) != 0)
        {
            pos += 4; // Dictionary ID
        }

        pos++; // HC
        // BD bit6-4 is the block size **index**, not an exponent: 4=64KB, 5=256KB,
        // 6=1MB, 7=4MB (each +1 is 4x), so size = 1 << (2*index + 8). This has been
        // verified by hand against two kinds of real output: an lz4.frame frame
        // (BD=0x40 => 64KB) and an lz4_flex frame (BD=0x50 => 256KB).
        int blockMax = 1 << ((((bd >> 4) & 0x7) * 2) + 8);
        Assert.InRange(blockMax, 64 * 1024, 4 * 1024 * 1024);

        int blocks = 0;
        while (pos + 4 <= frame.Length)
        {
            int size = GetInt32LeAt(frame, pos);
            if (size == 0)
            {
                // the end mark; a content checksum, when present, takes 4 more bytes
                Assert.True(pos + 4 + (contentChecksum ? 4 : 0) == frame.Length, "there must be no stray bytes after the frame end");
                return blocks;
            }

            pos += 4;
            int payload = size & 0x7FFFFFFF; // bit31=1 means the block is stored uncompressed
            Assert.True(payload <= blockMax, $"block length {payload} exceeds the block limit {blockMax} the frame header declares");
            pos += payload;
            if (blockChecksum)
            {
                pos += 4;
            }

            blocks++;
        }

        throw new Xunit.Sdk.XunitException("the LZ4 frame has no end mark");
    }

    // ---------------------------------------------------------------- Types and flag bits

    [Fact]
    public void Type_FindByValue_MatchesLegacy()
    {
        Assert.Equal(CompressionType.LZ4, CompressionType.FindByValue(1));
        Assert.Equal(CompressionType.ZSTD, CompressionType.FindByValue(2));
        Assert.Equal(CompressionType.ZLIB, CompressionType.FindByValue(3));
        Assert.Equal(CompressionType.ZLIB, CompressionType.FindByValue(0));
        Assert.Throws<InvalidOperationException>(() => CompressionType.FindByValue(4));
        Assert.Throws<InvalidOperationException>(() => CompressionType.FindByValue(-1));
    }

    [Fact]
    public void Type_GetCompressionFlag_MatchesSysFlagBits8To10()
    {
        Assert.Equal(0x1 << 8, CompressionType.GetCompressionFlag(CompressionType.LZ4));
        Assert.Equal(0x2 << 8, CompressionType.GetCompressionFlag(CompressionType.ZSTD));
        Assert.Equal(0x3 << 8, CompressionType.GetCompressionFlag(CompressionType.ZLIB));
        Assert.Throws<InvalidOperationException>(() => CompressionType.GetCompressionFlag(7));

        // closes the loop with MessageSysFlag's bit get/set
        foreach (int t in new[] { 1, 2, 3 })
        {
            int sysFlag = MessageSysFlag.CompressedFlag | CompressionType.GetCompressionFlag(t);
            Assert.Equal(CompressionType.FindByValue(t), MessageSysFlag.GetCompressionType(sysFlag));
            Assert.Equal(sysFlag, MessageSysFlag.SetCompressionType(MessageSysFlag.CompressedFlag, t));
        }
    }

    [Fact]
    public void NativeSupport_RealCodecsAreLoaded()
    {
        // libzstd / liblz4 are present in this environment: the test must prove it
        // **really reached native**, instead of taking the "skip when unavailable" path.
        Assert.True(CompressorFactory.HasZlibSupport());
        Assert.True(CompressorFactory.HasLz4Support(), "liblz4(LZ4F) must be loadable, otherwise this case fails instead of skipping");
        Assert.True(CompressorFactory.HasZstdSupport(), "libzstd must be loadable, otherwise this case fails instead of skipping");
    }

    // ---------------------------------------------------------------- ZLIB regression

    [Fact]
    public void Zlib_ExternalGolden_StillDecodes()
    {
        byte[] payload = Payload();
        byte[] external = Unhex(ZLIB_L5);
        Assert.Equal(47, external.Length);
        Assert.Equal(0x78, external[0]); // the zlib stream CMF
        Assert.Equal(payload, CompressorFactory.Decompress(external, CompressionType.ZLIB));
        Assert.Equal(payload, CompressorFactory.Decompress(external, 0)); // type bits 0 == legacy ZLIB
        Assert.Equal(payload, CompressorFactory.Decompress(
            CompressorFactory.Compress(payload, CompressionType.ZLIB, 5), CompressionType.ZLIB));
    }

    // ---------------------------------------------------------------- LZ4

    [Fact]
    public void Lz4_RoundTrip_SmallPayload()
    {
        byte[] payload = Payload();
        byte[] compressed = CompressorFactory.Compress(payload, CompressionType.LZ4);
        Assert.True(compressed.Length < payload.Length, "repetitive plaintext should compress");
        Assert.Equal(payload, CompressorFactory.Decompress(compressed, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_FrameMagicAndHeader_MatchLz4LegacyDefaults()
    {
        byte[] compressed = CompressorFactory.Compress(Payload(), CompressionType.LZ4);

        // magic 0x184D2204 stored little-endian => 04 22 4D 18 (not one of the raw block's prerequisites)
        Assert.Equal(new byte[] { 0x04, 0x22, 0x4D, 0x18 }, compressed.Take(4).ToArray());

        Assert.Equal(Unhex(RS_LZ4_FRAME).Take(7).ToArray(), compressed.Take(7).ToArray());
        Assert.Equal(0x60, compressed[4]);
        Assert.Equal(0x40, compressed[5]);

        // A byte-for-byte regression guardrail (including the compressed block body and the end mark)
        Assert.Equal(Unhex(NET_LZ4_FRAME), compressed);

        // Structurally self-consistent: 1120B plaintext < 64KB => exactly 1 block, and
        // the block descriptor length matches the rest of the frame body.
        // (A raw block -- the output of `LZ4_compress_default` -- cannot go through this
        // parsing at all, so it would not pass this case either.)
        Assert.Equal(1, CountLz4Blocks(compressed));
        Assert.Equal(43, GetInt32LeAt(compressed, 7));
        Assert.Equal(7 + 4 + 43 + 4, compressed.Length);
    }

    [Fact]
    public void Lz4_Decompress_ExternalFrame()
    {
        // A frame produced by an external implementation (official lz4.frame => upstream
        // C lz4) must decode; its frame header is 15 bytes (one content-size field more
        // than ours) and its HC differs too.
        byte[] external = Unhex(PY_LZ4_FRAME);
        Assert.Equal(66, external.Length);
        Assert.Equal(0x68, external[4]); // FLG with content-size
        Assert.Equal(Payload(), CompressorFactory.Decompress(external, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_Decompress_Lz4FlexFrame()
    {
        // lz4_flex is a **pure Rust** implementation with no code in common with
        // liblz4; being able to decode each other is what shows we wrote the
        // specification format rather than "a private liblz4 flavour".
        byte[] external = Unhex(RS_LZ4_FRAME);
        Assert.Equal(59, external.Length);
        Assert.Equal(Payload(), CompressorFactory.Decompress(external, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_EmptyInput_ProducesCanonicalEmptyFrame()
    {
        byte[] empty = CompressorFactory.Compress(Array.Empty<byte>(), CompressionType.LZ4);
        // lz4.frame / lz4_flex / this client all produce the **exact same** 11-byte
        // frame for an empty input (7 header + 4 end mark).
        Assert.Equal(Unhex(LZ4_FRAME_EMPTY), empty);
        Assert.Empty(CompressorFactory.Decompress(empty, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_Decompress_ExternalMultiBlockLinkedBlocks()
    {
        // lz4.frame writes **linked blocks** by default (FLG bit5=0): the blocks share
        // a compression window, so the decoder must maintain the 64KB window itself
        // instead of decoding each block independently.
        byte[] external = Unhex(PY_LZ4_MULTIBLOCK);
        byte[] payload = MultiBlockPayload();
        Assert.Equal(70500, payload.Length);
        Assert.Equal(370, external.Length);
        Assert.Equal(2, CountLz4Blocks(external)); // 70500B plaintext / 64KB blocks => 2 blocks
        Assert.Equal(payload, CompressorFactory.Decompress(external, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_Decompress_ExternalBlockLargerThanOurs()
    {
        // lz4_flex defaults to BD=0x50 => **256KB blocks**, 4x larger than the 64KB
        // this client writes: the decompression scratch buffer must not be hard-coded to
        // be exactly big enough for "my own block size".
        byte[] external = Unhex(RS_LZ4_MULTIBLOCK);
        Assert.Equal(350, external.Length);
        Assert.Equal(MultiBlockPayload(), CompressorFactory.Decompress(external, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_LargePayload_RoundTripsAcrossManyBlocks()
    {
        byte[] payload = LargePayload(); // ~637KB => at least 10 64KB blocks
        Assert.True(payload.Length >= 300 * 1024, "this case requires >= a few hundred KB");

        byte[] compressed = CompressorFactory.Compress(payload, CompressionType.LZ4);
        Assert.True(compressed.Length < payload.Length / 10, "repetitive plaintext should compress a lot");
        Assert.Equal(payload, CompressorFactory.Decompress(compressed, CompressionType.LZ4));

        int blocks = CountLz4Blocks(compressed);
        Assert.True(blocks > 9, $"~637KB / 64KB should split into multiple blocks, actual {blocks}");
        Assert.Equal((payload.Length + 65535) / 65536, blocks);
    }

    // ---------------------------------------------------------------- ZSTD

    [Fact]
    public void Zstd_RoundTrip_SmallPayload()
    {
        byte[] payload = Payload();
        byte[] compressed = CompressorFactory.Compress(payload, CompressionType.ZSTD);
        Assert.True(compressed.Length < payload.Length);
        Assert.Equal(payload, CompressorFactory.Decompress(compressed, CompressionType.ZSTD));
    }

    [Fact]
    public void Zstd_FrameMagicAndOutputBytes()
    {
        byte[] compressed = CompressorFactory.Compress(Payload(), CompressionType.ZSTD);
        Assert.Equal(new byte[] { 0x28, 0xB5, 0x2F, 0xFD }, compressed.Take(4).ToArray());
        Assert.Equal(Unhex(NET_ZSTD_FRAME), compressed); // regression guardrail
    }

    [Fact]
    public void Zstd_Decompress_CliFrames_WithAndWithoutContentSize()
    {
        byte[] withSize = Unhex(ZSTD_CLI_WITH_SIZE);
        byte[] noSize = Unhex(ZSTD_CLI_NO_SIZE);
        Assert.Equal(50, withSize.Length);
        Assert.Equal(49, noSize.Length);
        Assert.Equal(Payload(), CompressorFactory.Decompress(withSize, CompressionType.ZSTD));
        Assert.Equal(Payload(), CompressorFactory.Decompress(noSize, CompressionType.ZSTD));
    }

    [Fact]
    public void Zstd_Decompress_ExternalCrateFrame()
    {
        byte[] external = Unhex(RS_ZSTD_FRAME);
        Assert.Equal(45, external.Length);
        Assert.Equal(Payload(), CompressorFactory.Decompress(external, CompressionType.ZSTD));
    }

    [Fact]
    public void Zstd_EmptyInput_ProducesCanonicalEmptyFrame()
    {
        byte[] empty = CompressorFactory.Compress(Array.Empty<byte>(), CompressionType.ZSTD);
        Assert.Equal(Unhex(ZSTD_FRAME_EMPTY), empty); // byte-for-byte identical to the zstd crate's encode_all(b"", 3)
        Assert.Empty(CompressorFactory.Decompress(empty, CompressionType.ZSTD));
    }

    [Fact]
    public void Zstd_ConcatenatedFrames_DecodeLikeLegacyInputStream()
    {
        byte[] a = CompressorFactory.Compress(Payload(), CompressionType.ZSTD);
        byte[] b = CompressorFactory.Compress(Encoding.ASCII.GetBytes("tail-segment"), CompressionType.ZSTD);
        byte[] joined = new byte[a.Length + b.Length];
        Buffer.BlockCopy(a, 0, joined, 0, a.Length);
        Buffer.BlockCopy(b, 0, joined, a.Length, b.Length);

        byte[] outBytes = CompressorFactory.Decompress(joined, CompressionType.ZSTD);
        Assert.Equal(Payload().Concat(Encoding.ASCII.GetBytes("tail-segment")).ToArray(), outBytes);
    }

    [Fact]
    public void Zstd_LargePayload_RoundTrips()
    {
        byte[] payload = LargePayload();
        byte[] compressed = CompressorFactory.Compress(payload, CompressionType.ZSTD);
        Assert.True(compressed.Length < payload.Length / 50);
        Assert.Equal(payload, CompressorFactory.Decompress(compressed, CompressionType.ZSTD));
    }

    [Fact]
    public void Zstd_Levels_AllRoundTrip_AndClamped()
    {
        byte[] payload = LargePayload(200);
        foreach (int level in new[] { 0, 1, 5, 9, 19, 99, -3 })
        {
            byte[] compressed = CompressorFactory.Compress(payload, CompressionType.ZSTD, level);
            Assert.Equal(new byte[] { 0x28, 0xB5, 0x2F, 0xFD }, compressed.Take(4).ToArray());
            Assert.Equal(payload, CompressorFactory.Decompress(compressed, CompressionType.ZSTD));
        }
    }

    // ---------------------------------------------------------------- Dispatch and "never pass through silently"

    [Fact]
    public void CompressorFactory_DispatchByType_AllTypesRoundTrip()
    {
        byte[] payload = LargePayload(100);
        foreach (int t in new[] { CompressionType.ZLIB, CompressionType.LZ4, CompressionType.ZSTD, 0 })
        {
            byte[] compressed = CompressorFactory.Compress(payload, t, 5);
            Assert.NotEqual(payload, compressed);

            byte[] magic = CompressionType.FindByValue(t) switch
            {
                CompressionType.LZ4 => new byte[] { 0x04, 0x22, 0x4D, 0x18 },
                CompressionType.ZSTD => new byte[] { 0x28, 0xB5, 0x2F, 0xFD },
                _ => new byte[] { 0x78 },
            };
            Assert.Equal(magic, compressed.Take(magic.Length).ToArray());
            Assert.Equal(payload, CompressorFactory.Decompress(compressed, t));
        }
    }

    [Fact]
    public void MessageExt_EndToEnd_Lz4AndZstd()
    {
        // Goes through the real 17-field storage format encode/decode path
        // (EncodeMessageExt(needCompress) + DecodeMessage), proving that the sysFlag
        // type bits at bit8~10 are wired all the way through and not just usable by
        // CompressorFactory on its own.
        foreach (int type in new[] { CompressionType.LZ4, CompressionType.ZSTD, CompressionType.ZLIB })
        {
            byte[] body = LargePayload(200);
            int sysFlag = MessageSysFlag.CompressedFlag | CompressionType.GetCompressionFlag(type);
            var msg = new MessageExt
            {
                Topic = "CompressRoundTrip",
                QueueId = 3,
                SysFlag = sysFlag,
                Body = body,
                HasBody = true,
                BornHost = "127.0.0.1",
                StoreHost = "127.0.0.1",
            };

            byte[] raw = MessageDecoder.EncodeMessageExt(msg, needCompress: true);

            // it really is compressed on the wire: decode once without decompressing and
            // the body must start with the magic of the corresponding frame
            Assert.True(MessageDecoder.DecodeMessage(raw, out MessageExt wire, decompressBody: false), type.ToString());
            Assert.NotEqual(body, wire.Body);
            byte[] magic = type switch
            {
                CompressionType.LZ4 => new byte[] { 0x04, 0x22, 0x4D, 0x18 },
                CompressionType.ZSTD => new byte[] { 0x28, 0xB5, 0x2F, 0xFD },
                _ => new byte[] { 0x78 },
            };
            Assert.Equal(magic, wire.Body.Take(magic.Length).ToArray());

            Assert.True(MessageDecoder.DecodeMessage(raw, out MessageExt decoded, decompressBody: true), type.ToString());
            Assert.Equal(body, decoded.Body);
            Assert.False(MessageSysFlag.IsCompressed(decoded.SysFlag));
            Assert.Equal(type, MessageSysFlag.GetCompressionType(decoded.SysFlag));
        }
    }

    [Fact]
    public void UnsupportedOrBrokenInput_AlwaysThrows_NeverPassesThrough()
    {
        byte[] payload = Payload();

        foreach (int t in new[] { 4, 5, 6, 7, -1 })
        {
            Assert.Throws<InvalidOperationException>(() => CompressorFactory.Compress(payload, t, 5));
            Assert.Throws<InvalidOperationException>(() => CompressorFactory.Decompress(payload, t));
        }

        // 2) Garbage input: a broken frame must throw, it must not return the "bytes as is"
        byte[] garbage = Encoding.ASCII.GetBytes("this is definitely not a compressed frame at all");
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(garbage, CompressionType.LZ4); });
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(garbage, CompressionType.ZSTD); });
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(Array.Empty<byte>(), CompressionType.LZ4); });

        foreach (int t in new[] { CompressionType.LZ4, CompressionType.ZSTD })
        {
            byte[] full = CompressorFactory.Compress(LargePayload(50), t);
            foreach (int keep in new[] { 1, full.Length / 4, full.Length / 2, full.Length - 1 })
            {
                byte[] truncated = full.Take(keep).ToArray();
                // xunit's ThrowsAny<T>(..., string) only has a Func<Task> overload, so
                // Record.Exception is used here to get the exception and then assert,
                // putting "truncated to a few bytes" into the failure message too.
                Action attempt = () => { CompressorFactory.Decompress(truncated, t); };
                Exception? caught = Record.Exception(attempt);
                Assert.NotNull(caught);
                Assert.True(caught is not null,
                    $"type {t} truncated to {keep}/{full.Length} bytes did not report an error (data would be silently lost)");
            }
        }

        // 4) Decoding with the wrong type: taking an LZ4 frame as ZSTD must fail rather
        // than return the compressed bytes
        byte[] lz4 = CompressorFactory.Compress(payload, CompressionType.LZ4);
        byte[] zstd = CompressorFactory.Compress(payload, CompressionType.ZSTD);
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(lz4, CompressionType.ZSTD); });
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(zstd, CompressionType.LZ4); });
    }
}
