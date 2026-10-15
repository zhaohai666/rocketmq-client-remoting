// Cross-language interop tool: exchanges frame bytes with the Python-side process and cross-verifies them byte by byte.
//
// Usage (called by Program in the form "interop <mode> [args...]"):
//   rmq interop --emit    prints the JSON / ROCKETMQ encodings of a canonical command (hex)
//   rmq interop --decode <hex>  decodes a given hex frame and prints the fields (for Python -> C# verification)
//   rmq interop --emit-route    prints the JSON of a canonical TopicRouteData
//   rmq interop --emit-heartbeat  prints the JSON of a canonical HeartbeatData
//   rmq interop --decode-route <hex> decodes hex(body) as TopicRouteData and prints the re-serialised JSON
//   rmq interop --decode-heartbeat <hex> decodes hex(body) as HeartbeatData and prints the re-serialised JSON
//
// The form "decode, re-serialise and hand it to Python to compare with json.loads" turns
// "a field name that does not match" into a missing key, instead of relying on string
// equality which is a brittle assertion.
using System;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>Cross-language protocol byte interop (exchanging canonical frame bytes with the Python-side process).</summary>
internal static class InteropTool
{
    // The canonical command (both sides must agree):
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

    // ---------------- the --emit-* canonical data ----------------

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
