// A **real cluster + cross-client** integration tool for automatic compression.
//
//
//
//
// Usage (called by Program in the form "compression-live <mode> ..."):
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
        // The dispatcher has already stripped "compression-live", so here args = [mode, namesrv, topic, group, size, [codec]]
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
        "  codec = zlib (default) | lz4 | zstd, affects the sending side only; the receiving side decompresses automatically by the sysFlag type bits");
    }

    /// <summary>Resolving the algorithm name for the matrix: a mistyped codec exits right
    /// away rather than letting it look like "interoperability failed".</summary>
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

    // ---------------- the deterministic payload ----------------

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

    // ---------------- the consumer: stops after receiving 1 ----------------

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
     // Below the threshold nothing is compressed, and what the matrix wants is "really
    // compressed", so the caller has to give a payload big enough (the default threshold is 4 KiB).
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

    // ---------------- selftest: produce and consume + verifying the compression really happened ----------------

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
   const int size = 8192; // far above compressMsgBodyOverHowmuch(4096)
        byte[] payload = BuildPayload(size);
        uint payloadCrc = UtilAll.Crc32(payload);
        Console.WriteLine("payload len=" + size.ToString(CultureInfo.InvariantCulture)
                          + " crc32=" + payloadCrc.ToString(CultureInfo.InvariantCulture)
    + " (a deterministic payload, the interop probe uses the same algorithm)");

        // 1) send it out
        {
            var prod = new DefaultMQProducer("CompressLiveDotnetProducer_" + stamp)
            {
                NamesrvAddr = namesrv,
                SendMsgTimeout = 10000,
            };
            prod.Start();
            SendResult sr = prod.Send(new Message(topic, payload));
            bool ok = sr.SendStatus == SendStatus.SendOk;
  Check("sending a " + size.ToString(CultureInfo.InvariantCulture) + "B message (triggering automatic compression)", ok,
                "msgId=" + sr.MsgId);
            prod.Shutdown();
            if (!ok) return 1;
        }

        // 2) Receive it back; the body must be byte-for-byte identical to the original
        // (proving the "compress-store-decompress" loop)
        if (!RecvOne(namesrv, topic, group, 30, out MessageExt m))
        {
      Check("consuming the compressed message back", false, "30s timeout");
            return 1;
        }

  Check("consuming the compressed message back", true,
            "len=" + m.Body.Length.ToString(CultureInfo.InvariantCulture)
            + " storeSize=" + m.StoreSize.ToString(CultureInfo.InvariantCulture));
        Check("the decompressed body matches the original (len + CRC32)",
            m.Body.Length == payload.Length && UtilAll.Crc32(m.Body) == payloadCrc,
            "expected len=" + payload.Length.ToString(CultureInfo.InvariantCulture)
            + " crc=" + payloadCrc.ToString(CultureInfo.InvariantCulture)
          + " actual len=" + m.Body.Length.ToString(CultureInfo.InvariantCulture)
            + " crc=" + UtilAll.Crc32(m.Body).ToString(CultureInfo.InvariantCulture));

      // 3) Prove that what the broker stored **really is compressed**: storeSize should be
    // far smaller than the original length.
      Check("the broker side stored a compressed body (storeSize far below the original)",
            m.StoreSize > 0 && m.StoreSize < size / 2,
            "storeSize=" + m.StoreSize.ToString(CultureInfo.InvariantCulture)
     + " original=" + size.ToString(CultureInfo.InvariantCulture)
            + " ratio=" + (size / Math.Max(1, m.StoreSize)).ToString(CultureInfo.InvariantCulture) + ":1");

        Check("COMPRESSED_FLAG is cleared after decompressing",
            !MessageSysFlag.IsCompressed(m.SysFlag),
            "sysFlag=" + m.SysFlag.ToString(CultureInfo.InvariantCulture));

        // 5) A small message must **not** be compressed (the threshold semantics)
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
  Check("a message below the threshold (4096) is not compressed",
                    sm.Body.Length == small.Length && !MessageSysFlag.IsCompressed(sm.SysFlag),
                    "len=" + sm.Body.Length.ToString(CultureInfo.InvariantCulture)
                    + " storeSize=" + sm.StoreSize.ToString(CultureInfo.InvariantCulture));
            }
            else
            {
    Check("a message below the threshold (4096) is not compressed", false, "not consumed within 20s");
            }
        }

        // 6) Reproduce the codec path that carries the compressed flag on the wire, confirming
   // the flag semantics close the loop:
        //    a MessageExt with COMPRESSED_FLAG|ZLIB is encoded with needCompress=true -> the
  //    stored body gets smaller; decode with decompression -> the original is restored;
        //    decode without decompression -> what you get are the compressed bytes.
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

         Check("the stored body gets smaller after encoding with the compressed flag",
                storedCompressed.Length < payload.Length / 2,
                "stored=" + storedCompressed.Length.ToString(CultureInfo.InvariantCulture)
        + " original=" + payload.Length.ToString(CultureInfo.InvariantCulture));
            Check("decoding without decompression yields the compressed bytes and with decompression restores the original",
                raw.Body.Length < payload.Length
                && restored.Body.Length == payload.Length
                && UtilAll.Crc32(restored.Body) == payloadCrc
                && !MessageSysFlag.IsCompressed(restored.SysFlag),
                "rawLen=" + raw.Body.Length.ToString(CultureInfo.InvariantCulture)
                + " restoredLen=" + restored.Body.Length.ToString(CultureInfo.InvariantCulture));
        }

        foreach (var (name, type, available) in new (string, int, bool)[]
                 {
                     ("lz4", CompressionType.LZ4, CompressorFactory.HasLz4Support()),
                     ("zstd", CompressionType.ZSTD, CompressorFactory.HasZstdSupport()),
                 })
        {
     string tag = name + " real-machine round trip";
            if (!available)
            {
        Console.WriteLine("[SKIP] " + tag + "  there is no usable lib" + name + " on this machine");
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
            Check(tag, false, "not consumed within 30s");
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
