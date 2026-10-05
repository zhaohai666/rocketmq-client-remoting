// 自动压缩的**真实集群 + 跨客户端**联调工具（对应 cpp/examples/compression_live.cpp）。
//
// 为什么需要它：单元测试只能证明「zlib 往返正确」，证明不了
//   1. producer 真的在超过 compressMsgBodyOverHowmuch(4096) 时压缩并置 COMPRESSED_FLAG；
//   2. broker 存的是压缩体（storeSize 远小于原文）；
//   3. 消费端真的解压并把 COMPRESSED_FLAG 清掉（对齐 Java MessageDecoder）；
//   4. **别的客户端（Java）产生的压缩消息，我们能正确解压** —— 这是真机才暴露的静默数据损坏点。
//
// 载荷是**确定性**的（重复一行固定文本后截断），与 Java 探针 CompressProbe 完全相同，
// 因此两端各自本地重建后比较 CRC32 即可，无需交换文件。
//
// ⚠ CRC32 显示值会不同，这不是 bug：Java `UtilAll.crc32` 返回 `(int)(value & 0x7FFFFFFF)`，
// 砍掉了最高位；本工具用标准 CRC-32。所以 Java 打印 1785582993 对应本工具打印 3933066641
// （差正好 2^31）。判定互通要看各自的 `match=` 字段，不要直接比两边打印的 CRC 数字。
//
// 用法（由 Program 以 "compression-live <mode> ..." 形式调用）：
//   rmq compression-live selftest <namesrv>
//   rmq compression-live send     <namesrv> <topic> <group> <size> [codec]
//   rmq compression-live recv     <namesrv> <topic> <group> <size>
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>压缩真实集群 + 跨客户端联调（与 Java CompressProbe 对齐）。</summary>
internal static class CompressionLive
{
    private const string KLine = "rocketmq-compress-interop-payload-line-0123456789\n";

    public static int Run(string[] args)
    {
        if (args.Length < 1)
        {
            Usage();
            return 2;
        }

        string mode = args[0];
        // 分发器已剥掉 "compression-live"，此处 args = [mode, namesrv, topic, group, size, [codec]]
        if (mode == "send" && args.Length >= 5)
        {
            if (!TryParseCodec(args.Length >= 6 ? args[5] : null, out int codec))
            {
                Console.Error.WriteLine("unknown codec: " + args[5]);
                Usage();
                return 2;
            }

            return Send(args[1], args[2], args[3], int.Parse(args[4], CultureInfo.InvariantCulture),
                codec);
        }

        if (mode == "recv" && args.Length >= 5)
        {
            return Recv(args[1], args[2], args[3], int.Parse(args[4], CultureInfo.InvariantCulture));
        }

        if (mode == "selftest" && args.Length >= 2)
        {
            return SelfTest(args[1]);
        }

        Usage();
        return 2;
    }

    private static void Usage()
    {
        Console.Error.WriteLine(
            "usage: rmq compression-live selftest <namesrv>\n" +
            "       rmq compression-live send <namesrv> <topic> <group> <size> [codec]\n" +
            "       rmq compression-live recv <namesrv> <topic> <group> <size>\n" +
            "  codec = zlib（默认）| lz4 | zstd，只影响发送端；接收端按 sysFlag 类型位自动解压");
    }

    /// <summary>矩阵用的算法名解析：拼错 codec 直接退出，别让它看起来像"互通失败"。</summary>
    private static bool TryParseCodec(string? name, out int codec)
    {
        switch (name)
        {
            case null:
            case "":
            case "zlib":
                codec = CompressionType.ZLIB;
                return true;
            case "lz4":
                codec = CompressionType.LZ4;
                return true;
            case "zstd":
                codec = CompressionType.ZSTD;
                return true;
            default:
                codec = CompressionType.ZLIB;
                return false;
        }
    }

    private static string CodecName(int codec) => codec switch
    {
        CompressionType.LZ4 => "lz4",
        CompressionType.ZSTD => "zstd",
        _ => "zlib",
    };

    // ---------------- 确定性载荷 ----------------

    private static byte[] BuildPayload(int size)
    {
        var s = new List<byte>(size + 64);
        byte[] line = Encoding.UTF8.GetBytes(KLine);
        while (s.Count < size)
        {
            s.AddRange(line);
        }

        s.RemoveRange(size, s.Count - size);
        return s.ToArray();
    }

    // ---------------- 消费者：收 1 条即停 ----------------

    private sealed class FirstMsgListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private MessageExt? _first;
        private int _count;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                if (_first is null && msgs.Count > 0) _first = msgs[0];
                _count += msgs.Count;
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public bool Have()
        {
            lock (_lk) return _first is not null;
        }

        public MessageExt First()
        {
            lock (_lk) return _first!;
        }

        public int Count()
        {
            lock (_lk) return _count;
        }
    }

    private static bool RecvOne(string namesrv, string topic, string group, int timeoutSec, out MessageExt outMsg)
    {
        outMsg = new MessageExt();
        var listener = new FirstMsgListener();
        var cons = new DefaultMQPushConsumer(group)
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
            PullTimeoutMillis = 3000,
            PullSuspendTimeoutMillis = 1000,
        };
        cons.SetNamesrvAddr(namesrv);
        cons.Subscribe(topic, "*");
        cons.SetMessageListener(listener);
        cons.Start();
        try
        {
            for (int i = 0; i < timeoutSec * 4 && !listener.Have(); ++i)
            {
                Thread.Sleep(250);
            }

            if (listener.Have())
            {
                outMsg = listener.First();
                return true;
            }

            return false;
        }
        finally
        {
            cons.Shutdown();
        }
    }

    // ---------------- send ----------------

    private static int Send(string namesrv, string topic, string group, int size, int codec)
    {
        byte[] payload = BuildPayload(size);
        var prod = new DefaultMQProducer(group)
        {
            NamesrvAddr = namesrv,
            SendMsgTimeout = 10000,
            // 阈值以下不会压缩，矩阵要的是「真的压过」，所以调用方给够载荷尺寸（默认阈值 4 KiB）。
            CompressType = codec,
        };
        prod.Start();
        try
        {
            SendResult sr = prod.Send(new Message(topic, payload));
            uint crc = UtilAll.Crc32(payload);
            Console.WriteLine("SEND_OK codec=" + CodecName(codec)
                              + " len=" + payload.Length.ToString(CultureInfo.InvariantCulture)
                              + " crc32=" + crc.ToString(CultureInfo.InvariantCulture)
                              + " msgId=" + sr.MsgId);
            return sr.SendStatus == SendStatus.SendOk ? 0 : 1;
        }
        finally
        {
            prod.Shutdown();
        }
    }

    // ---------------- recv ----------------

    private static int Recv(string namesrv, string topic, string group, int size)
    {
        byte[] payload = BuildPayload(size);
        if (!RecvOne(namesrv, topic, group, 30, out MessageExt m))
        {
            Console.WriteLine("RECV_TIMEOUT");
            return 3;
        }

        bool lenOk = m.Body.Length == size;
        bool crcOk = m.Body.Length > 0 && UtilAll.Crc32(m.Body) == UtilAll.Crc32(payload);
        Console.WriteLine("RECV_OK len=" + m.Body.Length.ToString(CultureInfo.InvariantCulture)
                          + " crc32=" + (m.Body.Length > 0
                              ? UtilAll.Crc32(m.Body).ToString(CultureInfo.InvariantCulture)
                              : "0")
                          + " storeSize=" + m.StoreSize.ToString(CultureInfo.InvariantCulture)
                          + " match=" + ((lenOk && crcOk) ? 1 : 0).ToString(CultureInfo.InvariantCulture));
        return (lenOk && crcOk) ? 0 : 1;
    }

    // ---------------- selftest：自产自销 + 压缩真实性校验 ----------------

    private static int SelfTest(string namesrv)
    {
        int pass = 0;
        int fail = 0;
        void Check(string name, bool ok, string detail)
        {
            if (ok) pass++;
            else fail++;
            Console.WriteLine("[" + (ok ? "PASS" : "FAIL") + "] " + name + "  " + detail);
        }

        string stamp = UtilAll.CurrentTimeMillis().ToString(CultureInfo.InvariantCulture);
        string topic = "CompressLiveDotnet_" + stamp;
        string group = "CompressLiveDotnetGroup_" + stamp;
        const int size = 8192; // 远超 compressMsgBodyOverHowmuch(4096)
        byte[] payload = BuildPayload(size);
        uint payloadCrc = UtilAll.Crc32(payload);
        Console.WriteLine("payload len=" + size.ToString(CultureInfo.InvariantCulture)
                          + " crc32=" + payloadCrc.ToString(CultureInfo.InvariantCulture)
                          + "（确定性载荷，Java 探针同算法）");

        // 1) 发出
        {
            var prod = new DefaultMQProducer("CompressLiveDotnetProducer_" + stamp)
            {
                NamesrvAddr = namesrv,
                SendMsgTimeout = 10000,
            };
            prod.Start();
            SendResult sr = prod.Send(new Message(topic, payload));
            bool ok = sr.SendStatus == SendStatus.SendOk;
            Check("发送 " + size.ToString(CultureInfo.InvariantCulture) + "B 消息（触发自动压缩）", ok,
                "msgId=" + sr.MsgId);
            prod.Shutdown();
            if (!ok) return 1;
        }

        // 2) 收回来，正文必须与原文逐字节一致（证明"压缩-存储-解压"闭环）
        if (!RecvOne(namesrv, topic, group, 30, out MessageExt m))
        {
            Check("消费回压缩消息", false, "30s 超时");
            return 1;
        }

        Check("消费回压缩消息", true,
            "len=" + m.Body.Length.ToString(CultureInfo.InvariantCulture)
            + " storeSize=" + m.StoreSize.ToString(CultureInfo.InvariantCulture));
        Check("解压后正文与原文一致（len + CRC32）",
            m.Body.Length == payload.Length && UtilAll.Crc32(m.Body) == payloadCrc,
            "期望 len=" + payload.Length.ToString(CultureInfo.InvariantCulture)
            + " crc=" + payloadCrc.ToString(CultureInfo.InvariantCulture)
            + " 实际 len=" + m.Body.Length.ToString(CultureInfo.InvariantCulture)
            + " crc=" + UtilAll.Crc32(m.Body).ToString(CultureInfo.InvariantCulture));

        // 3) 证明 broker 里存的**确实是压缩体**：storeSize 应该远小于原文长度。
        Check("broker 侧存储为压缩体（storeSize 远小于原文）",
            m.StoreSize > 0 && m.StoreSize < size / 2,
            "storeSize=" + m.StoreSize.ToString(CultureInfo.InvariantCulture)
            + " 原文=" + size.ToString(CultureInfo.InvariantCulture)
            + " 压缩比=" + (size / Math.Max(1, m.StoreSize)).ToString(CultureInfo.InvariantCulture) + ":1");

        // 4) 解压后 COMPRESSED_FLAG 必须被清掉（对齐 Java MessageDecoder），否则上层会误以为 body 还是压缩的。
        Check("解压后 COMPRESSED_FLAG 已清除",
            !MessageSysFlag.IsCompressed(m.SysFlag),
            "sysFlag=" + m.SysFlag.ToString(CultureInfo.InvariantCulture));

        // 5) 小消息**不应**被压缩（阈值语义）
        {
            string smallTopic = "CompressLiveSmall_" + stamp;
            string smallGroup = "CompressLiveSmallGroup_" + stamp;
            const string small = "tiny-payload-under-threshold";
            var prod = new DefaultMQProducer("CompressLiveSmallProducer_" + stamp)
            {
                NamesrvAddr = namesrv,
            };
            prod.Start();
            prod.Send(new Message(smallTopic, Encoding.UTF8.GetBytes(small)));
            prod.Shutdown();
            if (RecvOne(namesrv, smallTopic, smallGroup, 20, out MessageExt sm))
            {
                Check("小于阈值(4096)的消息不压缩",
                    sm.Body.Length == small.Length && !MessageSysFlag.IsCompressed(sm.SysFlag),
                    "len=" + sm.Body.Length.ToString(CultureInfo.InvariantCulture)
                    + " storeSize=" + sm.StoreSize.ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                Check("小于阈值(4096)的消息不压缩", false, "20s 未消费到");
            }
        }

        // 6) 复现"线上带压缩标志"的编解码路径，确认标志位语义闭环：
        //    置 COMPRESSED_FLAG|ZLIB 的 MessageExt，用 needCompress=true 编码 -> 存储体变小；
        //    解压解码 -> 还原原文；不解压解码 -> 拿到的就是压缩字节。
        {
            var probe = new MessageExt
            {
                Topic = topic,
                Body = payload,
                SysFlag = MessageSysFlag.CompressedFlag
                          | CompressionType.GetCompressionFlag(CompressionType.ZLIB),
            };

            byte[] storedCompressed = MessageDecoder.EncodeMessageExt(probe, /*needCompress=*/true);
            MessageDecoder.DecodeMessage(storedCompressed, out MessageExt raw, true, /*decompressBody=*/false);
            MessageDecoder.DecodeMessage(storedCompressed, out MessageExt restored, true, /*decompressBody=*/true);

            Check("带压缩标志编码后存储体变小",
                storedCompressed.Length < payload.Length / 2,
                "stored=" + storedCompressed.Length.ToString(CultureInfo.InvariantCulture)
                + " 原文=" + payload.Length.ToString(CultureInfo.InvariantCulture));
            Check("不解压解码拿到压缩字节、解压解码还原原文",
                raw.Body.Length < payload.Length
                && restored.Body.Length == payload.Length
                && UtilAll.Crc32(restored.Body) == payloadCrc
                && !MessageSysFlag.IsCompressed(restored.SysFlag),
                "rawLen=" + raw.Body.Length.ToString(CultureInfo.InvariantCulture)
                + " restoredLen=" + restored.Body.Length.ToString(CultureInfo.InvariantCulture));
        }

        // 7) 另外两个后端（LZ4 Frame / ZSTD）各过一遍真机闭环：生产端压缩 → broker 存压缩体
        //    → 消费端解压还原。单测里有用 Java 那套库（lz4-java / zstd-jni）生成的硬编码夹具
        //    证明**线上格式**互通，这里证明的是「自己的发送路径真的用上了这个后端」。
        //    后端走系统库（P/Invoke 到 liblz4 / libzstd），装不上时打 SKIP，不算失败。
        foreach (var (name, type, available) in new (string, int, bool)[]
                 {
                     ("lz4", CompressionType.LZ4, CompressorFactory.HasLz4Support()),
                     ("zstd", CompressionType.ZSTD, CompressorFactory.HasZstdSupport()),
                 })
        {
            string tag = "后端 " + name + " 真机往返";
            if (!available)
            {
                Console.WriteLine("[SKIP] " + tag + "  本机没有可用的 lib" + name);
                continue;
            }

            string suffix = name + "_" + stamp;
            string btopic = "CompressLiveDotnet_" + suffix;
            string bgroup = "CompressLiveDotnetGroup_" + suffix;
            var bprod = new DefaultMQProducer("CompressLiveDotnetProducer_" + suffix)
            {
                NamesrvAddr = namesrv,
                SendMsgTimeout = 10000,
                CompressType = type,
            };
            bprod.Start();
            SendResult bsr = bprod.Send(new Message(btopic, payload));
            bprod.Shutdown();
            if (bsr.SendStatus != SendStatus.SendOk)
            {
                Check(tag, false, "send status=" + bsr.SendStatus);
                continue;
            }

            if (!RecvOne(namesrv, btopic, bgroup, 30, out MessageExt bm))
            {
                Check(tag, false, "30s 未消费到");
                continue;
            }

            Check(tag,
                bm.Body.Length == payload.Length
                && UtilAll.Crc32(bm.Body) == payloadCrc
                && bm.StoreSize > 0 && bm.StoreSize < size / 2
                && !MessageSysFlag.IsCompressed(bm.SysFlag),
                "len=" + bm.Body.Length.ToString(CultureInfo.InvariantCulture)
                + " storeSize=" + bm.StoreSize.ToString(CultureInfo.InvariantCulture)
                + " sysFlag=" + bm.SysFlag.ToString(CultureInfo.InvariantCulture));
        }

        Console.WriteLine();
        Console.WriteLine("==== compression live summary ====");
        Console.WriteLine((fail == 0 ? "ALL PASS" : "FAILED") + " (fail=" + fail.ToString(CultureInfo.InvariantCulture) + ")");
        return fail == 0 ? 0 : 1;
    }
}
