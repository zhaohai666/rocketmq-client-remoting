// 跨语言互操作工具：与 Python 参考客户端交叉验证协议字节。
// 对应 cpp/examples/interop_tool.cpp（172 行）。
//
// 用法（由 Program 以 "interop <mode> [args...]" 形式调用）：
//   rmq interop --emit                 打印规范命令的 JSON / ROCKETMQ 两种编码（十六进制）
//   rmq interop --decode <hex>         解码给定十六进制帧并打印字段（供 Python -> C# 验证）
//   rmq interop --emit-route           打印规范 TopicRouteData 的 JSON
//   rmq interop --emit-heartbeat       打印规范 HeartbeatData 的 JSON
//   rmq interop --decode-route <hex>   按 TopicRouteData 解码 hex(body)，回吐重序列化 JSON
//   rmq interop --decode-heartbeat <hex>  按 HeartbeatData 解码 hex(body)，回吐重序列化 JSON
//
// "解码后重新序列化再交给 Python 用 json.loads 比对"这种形式，
// 能把"字段名不一致"暴露成缺失键，而不是靠字符串相等这种脆弱断言。
using System;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>跨语言协议字节互操作（与 Python 参考客户端对齐）。</summary>
internal static class InteropTool
{
    // 规范命令（两侧必须一致）：
    //   code=10, language=1(CPP), opaque=7, version=0, flag=0,
    //   remark="hello", extFields{topic=T_TEST, queueId=2}, body="payload"
    private const int CanonicalCode = 10;

    public static int Run(string[] args)
    {
        if (args.Length < 1)
        {
            Console.Error.WriteLine("usage: rmq interop --emit | --decode <hex> | --emit-route | " +
                                    "--emit-heartbeat | --decode-route <hex> | --decode-heartbeat <hex>");
            return 1;
        }

        string mode = args[0];
        switch (mode)
        {
            case "--emit":
                return Emit();
            case "--emit-route":
                Console.WriteLine(Json.Dump(CanonicalRoute().ToJson()));
                return 0;
            case "--emit-heartbeat":
                Console.WriteLine(Json.Dump(CanonicalHeartbeat().ToJson()));
                return 0;
            case "--decode" when args.Length >= 2:
                return DecodeFrame(args[1]);
            case "--decode-route" when args.Length >= 2:
                return DecodeRoute(args[1]);
            case "--decode-heartbeat" when args.Length >= 2:
                return DecodeHeartbeat(args[1]);
            default:
                Console.Error.WriteLine("unknown mode: " + mode);
                return 1;
        }
    }

    // ---------------- --emit ----------------

    private static int Emit()
    {
        RemotingCommand jsonCmd = BuildCanonicalCommand(SerializeType.Json);
        RemotingCommand rmqCmd = BuildCanonicalCommand(SerializeType.Rocketmq);
        Console.WriteLine("JSON " + UtilAll.Bytes2String(jsonCmd.Encode()));
        Console.WriteLine("ROCKETMQ " + UtilAll.Bytes2String(rmqCmd.Encode()));
        return 0;
    }

    private static RemotingCommand BuildCanonicalCommand(byte serializeType)
    {
        var cmd = new RemotingCommand
        {
            Code = CanonicalCode,
            Language = LanguageCode.Cpp,
            Version = 0,
            Opaque = 7,
            Flag = 0,
            Remark = "hello",
            HasRemark = true,
            SerializeTypeCurrentRpc = serializeType,
        };
        cmd.AddExtField("topic", "T_TEST");
        cmd.AddExtField("queueId", "2");
        cmd.Body = Encoding.UTF8.GetBytes("payload");
        cmd.HasBody = true;
        return cmd;
    }

    // ---------------- --emit-* 规范数据 ----------------

    private static TopicRouteData CanonicalRoute()
    {
        var route = new TopicRouteData { OrderTopicConf = string.Empty };
        route.QueueDatas.Add(new QueueData("broker-a", 4, 4, PermName.PermRead | PermName.PermWrite, 0));
        route.QueueDatas.Add(new QueueData("broker-b", 2, 2, PermName.PermRead, 0));
        var addrsA = new SortedDictionary<long, string>
        {
            [0] = "127.0.0.1:10911",
            [1] = "127.0.0.1:10912",
        };
        var addrsB = new SortedDictionary<long, string>
        {
            [0] = "127.0.0.1:10913",
        };
        route.BrokerDatas.Add(new BrokerData("DefaultCluster", "broker-a", addrsA, string.Empty, false));
        route.BrokerDatas.Add(new BrokerData("DefaultCluster", "broker-b", addrsB, string.Empty, false));
        return route;
    }

    private static HeartbeatData CanonicalHeartbeat()
    {
        var hb = new HeartbeatData("10.0.0.1@12345");
        hb.AddProducerData(new ProducerData("pg_probe"));
        var cd = new ConsumerData("cg_probe", ConsumeType.ConsumePassively, MessageModel.Clustering,
            ConsumeFromWhere.ConsumeFromLastOffset)
        {
            UnitMode = false,
        };
        SubscriptionData sd = FilterAPI.BuildSubscriptionData("TopicProbe", "TagA||TagB");
        sd.SubVersion = 1700000000000L;
        cd.AddSubscriptionData(sd);
        hb.AddConsumerData(cd);
        return hb;
    }

    // ---------------- --decode ----------------

    private static int DecodeFrame(string hex)
    {
        byte[] raw = UtilAll.String2Bytes(hex);
        if (raw.Length == 0)
        {
            Console.Error.WriteLine("empty/invalid hex");
            return 2;
        }

        RemotingCommand cmd;
        try
        {
            cmd = RemotingCommand.Decode(raw);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("decode failed: " + e.Message);
            return 3;
        }

        var sb = new StringBuilder();
        sb.Append("code=").Append(cmd.Code.ToString(CultureInfo.InvariantCulture));
        sb.Append(";language=").Append(cmd.Language.ToString(CultureInfo.InvariantCulture));
        sb.Append(";opaque=").Append(cmd.Opaque.ToString(CultureInfo.InvariantCulture));
        sb.Append(";version=").Append(cmd.Version.ToString(CultureInfo.InvariantCulture));
        sb.Append(";remark=").Append(cmd.Remark);
        sb.Append(";body=").Append(cmd.HasBody ? Encoding.UTF8.GetString(cmd.Body) : string.Empty);
        foreach (var kv in cmd.ExtFields)
        {
            sb.Append(";ext.").Append(kv.Key).Append('=').Append(kv.Value);
        }

        Console.WriteLine(sb.ToString());
        return 0;
    }

    private static int DecodeRoute(string hex)
    {
        byte[] raw = UtilAll.String2Bytes(hex);
        if (raw.Length == 0)
        {
            Console.Error.WriteLine("empty/invalid hex");
            return 2;
        }

        if (!TopicRouteData.Decode(raw, out TopicRouteData route))
        {
            Console.Error.WriteLine("decode failed");
            return 3;
        }

        Console.WriteLine(Json.Dump(route.ToJson()));
        return 0;
    }

    private static int DecodeHeartbeat(string hex)
    {
        byte[] raw = UtilAll.String2Bytes(hex);
        if (raw.Length == 0)
        {
            Console.Error.WriteLine("empty/invalid hex");
            return 2;
        }

        if (!HeartbeatData.Decode(raw, out HeartbeatData hb))
        {
            Console.Error.WriteLine("decode failed");
            return 3;
        }

        Console.WriteLine(Json.Dump(hb.ToJson()));
        return 0;
    }
}
