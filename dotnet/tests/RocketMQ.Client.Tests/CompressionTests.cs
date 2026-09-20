// 消息体压缩单测：ZLIB 回归 + ZSTD + LZ4，重点是**跨实现互通**。
//
// 自压自解证明不了任何事（两端会是同一个 bug），所以这里的真值全部是**外部实现产出的字节**：
//   * `py*`  —— Python 官方 `lz4.frame`（包装上游 C lz4）/ `zlib.compress(data, 5)`
//   * `rs*`  —— Rust `lz4_flex`（**纯 Rust 实现**，与 liblz4 无关）与 Rust `zstd` crate
//   * `ZSTD_CLI*` —— `zstd -3` CLI（Yann Collet 参考实现）产出的帧
//   反向也验：本文件里 `NET_*` 常量是 .NET 实际产出的字节（回归护栏），
//   这些字节另外用 `lz4 -d` / `zstd -d` CLI 与 lz4_flex、python `lz4.frame` 全部解通过
//   （含 2.8MB 的多块帧）。原生库/CLI 不在测试机上，所以那一步用脚本离线核对，
//   本文件负责把「外部字节进得来」和「我们的字节长什么样」钉在单测里。
//
// 明文都是**公式生成**的（和 rust `src/common/compression.rs` 的测试同一配方），
// 所以只需要嵌压缩后的真值，不用嵌明文。
using System.Globalization;
using System.Text;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class CompressionTests
{
    /// <summary>与 rust/C++/python 端口真值测试同一段明文（1120 字节）。</summary>
    private static byte[] Payload() =>
        Encoding.ASCII.GetBytes(string.Concat(Enumerable.Repeat("rocketmq-compressed-payload-", 40)));

    /// <summary>70500 字节：刚好跨过一个 64KB 块边界，用于多块帧（= 47B 模式 x 1500）。</summary>
    private static byte[] MultiBlockPayload() =>
        Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat(
            "rocketmq-lz4-multiblock-seed-0123456789abcdef-\n", 1500)));

    /// <summary>约 600KB 可压缩明文，用来真正压过多块 LZ4 / 多块 zstd。</summary>
    private static byte[] LargePayload(int repeats = 13000) =>
        Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat(
            "rocketmq-compression-large-payload-0123456789abcdef-\n", repeats)));

    // ---------------------------------------------------------------- 外部真值（hex）

    // Python `lz4.frame.compress(payload)`：66B。帧头 15 字节、FLG=0x68（**写了 content-size**）、
    // 块是独立块。等价于 rust 测试里的 LZ4_FRAME_HEX。
    private const string PY_LZ4_FRAME = @"
        04224d1868406004000000000000482b000000ff0d726f636b65746d712d636f6d707265737365642d7061796c
        6f61642d1c00ffffffff30506c6f61642d00000000";

    // Rust `lz4_flex::frame::FrameEncoder`（纯 Rust）：59B。帧头 7 字节、FLG=0x60
    // （不写 content-size）——与 Java LZ4FrameOutputStream 默认写法同形。
    private const string RS_LZ4_FRAME = @"
        04224d186040822c000000ff0d726f636b65746d712d636f6d707265737365642d7061796c6f61642d1c00ffff
        ffff2f60796c6f61642d00000000";

    // 空输入的 LZ4 帧：python / rust / .NET 三边产出**完全相同**的 11 字节。
    private const string LZ4_FRAME_EMPTY = "04224d1860408200000000";

    // .NET 实际产出的 1120B 帧（58B）：回归护栏。头部 7 字节与 rust 一致，
    // 43 字节压缩块与 python 逐字节一致（python 只是多写了 content-size 字段）。
    private const string NET_LZ4_FRAME = @"
        04224d186040822b000000ff0d726f636b65746d712d636f6d707265737365642d7061796c6f61642d1c00ffff
        ffff30506c6f61642d00000000";

    // Python `lz4.frame.compress`，输入 70500B：370B。FLG=0x48 ⇒ **link 块**（非独立）+
    // content-size，BD=0x40 ⇒ 64KB 块 ⇒ 2 个块。解码端必须支持跨块复用压缩窗口。
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

    // Rust `lz4_flex`，输入 70500B：350B。BD=0x50 ⇒ **256KB 块**（比我们自己发的 64KB 大 4 倍），
    // FLG=0x60 ⇒ 独立块、无 content-size。解码端不能假设块大小只有自己写的那种。
    private const string RS_LZ4_MULTIBLOCK = @"
        04224d186050fb4f010000ff20726f636b65746d712d6c7a342d6d756c7469626c6f636b2d736565642d30313233
        3435363738396162636465662d0a2f00ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ffffffffffffffffffffffffffffffff3060636465662d0a00000000";

    // `zstd -3` CLI：带 content-size 的帧（50B，FLG=0x64 ⇒ 4B FCS + content checksum）。
    private const string ZSTD_CLI_WITH_SIZE = @"
        28b52ffd646003250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff7402
        78aa9792";

    // `zstd -3 --no-content-size` CLI：不带 content-size 的帧（49B）——
    // Java zstd-jni `ZstdOutputStream` 流式写的就是这个形状，必须能解。
    private const string ZSTD_CLI_NO_SIZE = @"
        28b52ffd0408250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff740278
        aa9792";

    // Rust `zstd::encode_all(data, 3)`：45B，FLG=0x00（单段、无 checksum）。
    private const string RS_ZSTD_FRAME = @"
        28b52ffd0058250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff7402";

    // 空输入的 zstd 帧：rust 与 .NET 产出**完全相同**的 9 字节。
    private const string ZSTD_FRAME_EMPTY = "28b52ffd2000010000";

    // .NET 实际产出的 zstd 帧（46B）：回归护栏（`ZSTD_compress` one-shot ⇒ 单段 + 2B FCS）。
    private const string NET_ZSTD_FRAME = @"
        28b52ffd606003250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff7402";

    // Python `zlib.compress(payload, 5)`：47B。ZLIB 路径的既有真值（改动 LZ4/ZSTD 时不许回退）。
    private const string ZLIB_L5 = @"
        785e2bca4fce4e2dc92dd44dcecf2d284a2d2e4e4dd12d48acccc94f4cd12d1a951b951b951b95a3400e0058e0b9
        f0";

    // ---------------------------------------------------------------- 辅助

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
    /// 走一遍 LZ4 Frame 的块描述符，返回块数。用来证明大输入**确实**被拆成多块，
    /// 而不是只把断言押在「能解回来」上。
    ///
    /// 帧头位义（用 python / rust / lz4 CLI 三种产出实测反推，与官方 frame format 一致，
    /// 数值按字节位算）：FLG bit7-6 = 版本(必须 01=0x40)、bit5 = 块独立、bit4 = 块校验、
    /// bit3 = 有 8 字节 content-size、bit2 = 有 content checksum；
    /// BD bit7 = 有 4 字节 dictID，bit6-4 = 块大小指数（4=64KB、5=256KB、6=1MB、7=4MB）。
    /// </summary>
    private static int CountLz4Blocks(byte[] frame)
    {
        Assert.True(frame.Length >= 7, "帧至少要有 magic(4)+FLG+BD+HC");
        Assert.Equal(0x184D2204u, (uint)BitConverter.ToInt32(frame, 0)); // 小端读出即魔数本体

        int pos = 4;
        int flg = frame[pos++];
        int bd = frame[pos++];
        Assert.Equal(1, (flg >> 6) & 0x3); // 版本号
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
        // BD bit6-4 是块大小的**编号**而不是指数：4=64KB、5=256KB、6=1MB、7=4MB（每 +1 翻 4 倍），
        // 所以 size = 1 << (2*编号 + 8)。用 python 帧(BD=0x40⇒64KB) 与 rust 帧(BD=0x50⇒256KB)
        // 两种真实产出各自验算过。
        int blockMax = 1 << ((((bd >> 4) & 0x7) * 2) + 8);
        Assert.InRange(blockMax, 64 * 1024, 4 * 1024 * 1024);

        int blocks = 0;
        while (pos + 4 <= frame.Length)
        {
            int size = GetInt32LeAt(frame, pos);
            if (size == 0)
            {
                // 结束标记；content checksum 若有则再占 4 字节
                Assert.True(pos + 4 + (contentChecksum ? 4 : 0) == frame.Length, "帧尾之后不该有杂字节");
                return blocks;
            }

            pos += 4;
            int payload = size & 0x7FFFFFFF; // bit31=1 表示该块未压缩存放
            Assert.True(payload <= blockMax, $"块长度 {payload} 超过帧头声明的块上限 {blockMax}");
            pos += payload;
            if (blockChecksum)
            {
                pos += 4;
            }

            blocks++;
        }

        throw new Xunit.Sdk.XunitException("LZ4 帧没有结束标记");
    }

    // ---------------------------------------------------------------- 类型与标志位

    [Fact]
    public void Type_FindByValue_MatchesJava()
    {
        // Java CompressionType.findByValue：0 与 3 都必须是 ZLIB（老客户端没有类型位）
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
        // Java CompressionType.getCompressionFlag：sysFlag 的 bit8~10
        Assert.Equal(0x1 << 8, CompressionType.GetCompressionFlag(CompressionType.LZ4));
        Assert.Equal(0x2 << 8, CompressionType.GetCompressionFlag(CompressionType.ZSTD));
        Assert.Equal(0x3 << 8, CompressionType.GetCompressionFlag(CompressionType.ZLIB));
        Assert.Throws<InvalidOperationException>(() => CompressionType.GetCompressionFlag(7));

        // 与 MessageSysFlag 的取位/放位闭环
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
        // 这个环境里 libzstd / liblz4 是存在的：测试必须证明**真的跑到了 native**，
        // 而不是走「不可用就跳过」的路径。
        Assert.True(CompressorFactory.HasZlibSupport());
        Assert.True(CompressorFactory.HasLz4Support(), "liblz4(LZ4F) 必须可加载，否则本用例失败而不是跳过");
        Assert.True(CompressorFactory.HasZstdSupport(), "libzstd 必须可加载，否则本用例失败而不是跳过");
    }

    // ---------------------------------------------------------------- ZLIB 回归

    [Fact]
    public void Zlib_ExternalGolden_StillDecodes()
    {
        byte[] payload = Payload();
        byte[] external = Unhex(ZLIB_L5);
        Assert.Equal(47, external.Length);
        Assert.Equal(0x78, external[0]); // zlib 流 CMF
        Assert.Equal(payload, CompressorFactory.Decompress(external, CompressionType.ZLIB));
        Assert.Equal(payload, CompressorFactory.Decompress(external, 0)); // 类型位 0 == 老版本 ZLIB
        Assert.Equal(payload, CompressorFactory.Decompress(
            CompressorFactory.Compress(payload, CompressionType.ZLIB, 5), CompressionType.ZLIB));
    }

    // ---------------------------------------------------------------- LZ4

    [Fact]
    public void Lz4_RoundTrip_SmallPayload()
    {
        byte[] payload = Payload();
        byte[] compressed = CompressorFactory.Compress(payload, CompressionType.LZ4);
        Assert.True(compressed.Length < payload.Length, "重复明文应被压缩");
        Assert.Equal(payload, CompressorFactory.Decompress(compressed, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_FrameMagicAndHeader_MatchLz4JavaDefaults()
    {
        byte[] compressed = CompressorFactory.Compress(Payload(), CompressionType.LZ4);

        // magic 0x184D2204 按小端落盘 => 04 22 4D 18（不是 raw block 的必要条件之一）
        Assert.Equal(new byte[] { 0x04, 0x22, 0x4D, 0x18 }, compressed.Take(4).ToArray());

        // 与 rust `lz4_flex` 的帧头**逐字节相同**：7 字节、FLG=0x60（独立块、不写 content-size）、
        // BD=0x40（max64KB）。这正是 Java `LZ4FrameOutputStream` 的默认写法。
        Assert.Equal(Unhex(RS_LZ4_FRAME).Take(7).ToArray(), compressed.Take(7).ToArray());
        Assert.Equal(0x60, compressed[4]);
        Assert.Equal(0x40, compressed[5]);

        // 逐字节回归护栏（含压缩块本体 + 结束标记）
        Assert.Equal(Unhex(NET_LZ4_FRAME), compressed);

        // 结构自洽：1120B 明文 < 64KB ⇒ 恰好 1 个块，块描述符长度与帧体剩余量吻合。
        // （raw block——`LZ4_compress_default` 的产物——根本走不通这个解析，也就过不了本用例。）
        Assert.Equal(1, CountLz4Blocks(compressed));
        Assert.Equal(43, GetInt32LeAt(compressed, 7));
        Assert.Equal(7 + 4 + 43 + 4, compressed.Length);
    }

    [Fact]
    public void Lz4_Decompress_PythonFrame()
    {
        // 外部实现（python lz4.frame ⇒ 上游 C lz4）产出的帧必须能解，
        // 它的帧头 15 字节（比我们的多一个 content-size 字段），HC 也不同。
        byte[] external = Unhex(PY_LZ4_FRAME);
        Assert.Equal(66, external.Length);
        Assert.Equal(0x68, external[4]); // 带 content-size 的 FLG
        Assert.Equal(Payload(), CompressorFactory.Decompress(external, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_Decompress_RustLz4FlexFrame()
    {
        // lz4_flex 是**纯 Rust** 实现，和 liblz4 没有任何代码关系，
        // 能互解才说明我们写的是规范格式而不是「liblz4 私有口味」。
        byte[] external = Unhex(RS_LZ4_FRAME);
        Assert.Equal(59, external.Length);
        Assert.Equal(Payload(), CompressorFactory.Decompress(external, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_EmptyInput_ProducesCanonicalEmptyFrame()
    {
        byte[] empty = CompressorFactory.Compress(Array.Empty<byte>(), CompressionType.LZ4);
        // python / rust / .NET 三边对空输入产出**一模一样**的 11 字节帧（头 7 + 结束标记 4）。
        Assert.Equal(Unhex(LZ4_FRAME_EMPTY), empty);
        Assert.Empty(CompressorFactory.Decompress(empty, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_Decompress_ExternalMultiBlockLinkedBlocks()
    {
        // python 默认写 **link 块**（FLG bit5=0）：块之间共享压缩窗口，
        // 解码端必须自己维护 64KB 窗口，不能每块独立解。
        byte[] external = Unhex(PY_LZ4_MULTIBLOCK);
        byte[] payload = MultiBlockPayload();
        Assert.Equal(70500, payload.Length);
        Assert.Equal(370, external.Length);
        Assert.Equal(2, CountLz4Blocks(external)); // 70500B 明文 / 64KB 块 => 2 块
        Assert.Equal(payload, CompressorFactory.Decompress(external, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_Decompress_ExternalBlockLargerThanOurs()
    {
        // rust 默认 BD=0x50 ⇒ **256KB 块**，比本端口写出的 64KB 大 4 倍：
        // 解压的 scratch 缓冲不能按「自己的块大小」写死成刚好够用。
        byte[] external = Unhex(RS_LZ4_MULTIBLOCK);
        Assert.Equal(350, external.Length);
        Assert.Equal(MultiBlockPayload(), CompressorFactory.Decompress(external, CompressionType.LZ4));
    }

    [Fact]
    public void Lz4_LargePayload_RoundTripsAcrossManyBlocks()
    {
        byte[] payload = LargePayload(); // ~637KB ⇒ 至少 10 个 64KB 块
        Assert.True(payload.Length >= 300 * 1024, "用例要求 >= 几百 KB");

        byte[] compressed = CompressorFactory.Compress(payload, CompressionType.LZ4);
        Assert.True(compressed.Length < payload.Length / 10, "重复明文应被压得很小");
        Assert.Equal(payload, CompressorFactory.Decompress(compressed, CompressionType.LZ4));

        int blocks = CountLz4Blocks(compressed);
        Assert.True(blocks > 9, $"~637KB / 64KB 应该拆出多个块，实际 {blocks}");
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
        // Java `Zstd.compress(byte[])` 产出的是**标准 zstd 帧**（magic 0x28 B5 2F FD），不是 raw block
        Assert.Equal(new byte[] { 0x28, 0xB5, 0x2F, 0xFD }, compressed.Take(4).ToArray());
        Assert.Equal(Unhex(NET_ZSTD_FRAME), compressed); // 回归护栏
    }

    [Fact]
    public void Zstd_Decompress_CliFrames_WithAndWithoutContentSize()
    {
        // 带 content-size（FLG 的 FCS 字段非 0）与不带（Java ZstdOutputStream 流式写法）两种都要能解：
        // 不带时 ZSTD_getFrameContentSize 返回 UNKNOWN，得靠扩容缓冲而不是按声明长度一次开到位。
        byte[] withSize = Unhex(ZSTD_CLI_WITH_SIZE);
        byte[] noSize = Unhex(ZSTD_CLI_NO_SIZE);
        Assert.Equal(50, withSize.Length);
        Assert.Equal(49, noSize.Length);
        Assert.Equal(Payload(), CompressorFactory.Decompress(withSize, CompressionType.ZSTD));
        Assert.Equal(Payload(), CompressorFactory.Decompress(noSize, CompressionType.ZSTD));
    }

    [Fact]
    public void Zstd_Decompress_RustCrateFrame()
    {
        byte[] external = Unhex(RS_ZSTD_FRAME);
        Assert.Equal(45, external.Length);
        Assert.Equal(Payload(), CompressorFactory.Decompress(external, CompressionType.ZSTD));
    }

    [Fact]
    public void Zstd_EmptyInput_ProducesCanonicalEmptyFrame()
    {
        byte[] empty = CompressorFactory.Compress(Array.Empty<byte>(), CompressionType.ZSTD);
        Assert.Equal(Unhex(ZSTD_FRAME_EMPTY), empty); // 与 rust zstd::encode_all(b"", 3) 逐字节一致
        Assert.Empty(CompressorFactory.Decompress(empty, CompressionType.ZSTD));
    }

    [Fact]
    public void Zstd_ConcatenatedFrames_DecodeLikeJavaInputStream()
    {
        // Java ZstdInputStream 会读串接的多帧（Zstd_decompress 文档同样要求 compressedSize 是整帧总长）：
        // 把两个帧拼起来必须解成两段明文的首尾相接。
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
        // Java 把 compressLevel 透传给 ZstdOutputStream（Python 则忽略），这里跟随 Java。
        byte[] payload = LargePayload(200);
        foreach (int level in new[] { 0, 1, 5, 9, 19, 99, -3 })
        {
            byte[] compressed = CompressorFactory.Compress(payload, CompressionType.ZSTD, level);
            Assert.Equal(new byte[] { 0x28, 0xB5, 0x2F, 0xFD }, compressed.Take(4).ToArray());
            Assert.Equal(payload, CompressorFactory.Decompress(compressed, CompressionType.ZSTD));
        }
    }

    // ---------------------------------------------------------------- 分发与「绝不静默透传」

    [Fact]
    public void CompressorFactory_DispatchByType_AllTypesRoundTrip()
    {
        byte[] payload = LargePayload(100);
        foreach (int t in new[] { CompressionType.ZLIB, CompressionType.LZ4, CompressionType.ZSTD, 0 })
        {
            byte[] compressed = CompressorFactory.Compress(payload, t, 5);
            Assert.NotEqual(payload, compressed);

            // 帧头必须对得上：t==0 走的是 ZLIB（Java findByValue 的兼容映射）
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
        // 走真实的 17 段存储格式编解码路径（EncodeMessageExt(needCompress) + DecodeMessage），
        // 证明 sysFlag 的 bit8~10 类型位一路打通，而不只是 CompressorFactory 自己能用。
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

            // 线上确实是压缩过的：不解压地解一次，正文应以对应帧的魔数开头
            Assert.True(MessageDecoder.DecodeMessage(raw, out MessageExt wire, decompressBody: false), type.ToString());
            Assert.NotEqual(body, wire.Body);
            byte[] magic = type switch
            {
                CompressionType.LZ4 => new byte[] { 0x04, 0x22, 0x4D, 0x18 },
                CompressionType.ZSTD => new byte[] { 0x28, 0xB5, 0x2F, 0xFD },
                _ => new byte[] { 0x78 },
            };
            Assert.Equal(magic, wire.Body.Take(magic.Length).ToArray());

            // 正常解码：拿到明文，并且 COMPRESSED_FLAG 被清掉（类型位保留），与 Java 一致
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

        // 1) 未知类型（SNAPPY=4 及 5/6/7）：Java findByValue 抛 RuntimeException
        foreach (int t in new[] { 4, 5, 6, 7, -1 })
        {
            Assert.Throws<InvalidOperationException>(() => CompressorFactory.Compress(payload, t, 5));
            Assert.Throws<InvalidOperationException>(() => CompressorFactory.Decompress(payload, t));
        }

        // 2) 垃圾输入：坏帧必须抛，不能返回「原样字节」
        byte[] garbage = Encoding.ASCII.GetBytes("this is definitely not a compressed frame at all");
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(garbage, CompressionType.LZ4); });
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(garbage, CompressionType.ZSTD); });
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(Array.Empty<byte>(), CompressionType.LZ4); });

        // 3) 截断帧：宁可抛错也不能交半截明文（Java ZstdInputStream/LZ4FrameInputStream 抛 IOException）
        foreach (int t in new[] { CompressionType.LZ4, CompressionType.ZSTD })
        {
            byte[] full = CompressorFactory.Compress(LargePayload(50), t);
            foreach (int keep in new[] { 1, full.Length / 4, full.Length / 2, full.Length - 1 })
            {
                byte[] truncated = full.Take(keep).ToArray();
                // xunit 的 ThrowsAny<T>(…, string) 只有 Func<Task> 重载，这里用 Record.Exception
                // 拿到异常再断言，顺带把「截断到几个字节」写进失败信息里。
                Action attempt = () => { CompressorFactory.Decompress(truncated, t); };
                Exception? caught = Record.Exception(attempt);
                Assert.NotNull(caught);
                Assert.True(caught is not null,
                    $"type {t} 截断到 {keep}/{full.Length} 字节却没能报错（会静默丢数据）");
            }
        }

        // 4) 用错类型解：拿 LZ4 帧当 ZSTD 解必须失败，而不是返回压缩字节
        byte[] lz4 = CompressorFactory.Compress(payload, CompressionType.LZ4);
        byte[] zstd = CompressorFactory.Compress(payload, CompressionType.ZSTD);
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(lz4, CompressionType.ZSTD); });
        Assert.ThrowsAny<Exception>(() => { CompressorFactory.Decompress(zstd, CompressionType.LZ4); });
    }
}
