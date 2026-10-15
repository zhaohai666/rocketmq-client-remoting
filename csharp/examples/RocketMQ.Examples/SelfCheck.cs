// Protocol codec self-check: no cluster dependency, verifying the "encode -> decode" loop and the key constants.
using System;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>A local self-check: a three-way codec loop over the HEART_BEAT / GET_MAX_OFFSET
/// command headers plus the MessageExt storage format.</summary>
internal static class SelfCheck
{
    public static int Run()
    {
        int pass = 0;
        int fail = 0;

        void Check(string name, bool ok)
        {
            if (ok) pass++;
            else fail++;
            Console.WriteLine("[" + (ok ? "PASS" : "FAIL") + "] " + name);
        }

        // 1) HEART_BEAT command header (JSON serialisation) encode -> decode loop
        RemotingCommand hb = RemotingCommand.CreateRequestCommand(RequestCode.HeartBeat);
        hb.AddExtField("clientID", "127.0.0.1@10911");
        byte[] wire = hb.Encode();
        RemotingCommand dec = RemotingCommand.Decode(wire);
        bool ok1 = dec.Code == RequestCode.HeartBeat
                   && dec.GetExtField("clientID") == "127.0.0.1@10911";
        Console.WriteLine("request  : " + hb.ToString());
        Console.WriteLine("wire     : " + wire.Length.ToString(CultureInfo.InvariantCulture) + " bytes");
        Console.WriteLine("decoded  : " + dec.ToString());
        Check("HEART_BEAT json round-trip", ok1);

        // 2) GET_MAX_OFFSET command header (ROCKETMQ binary serialisation) encode -> decode loop
        RemotingCommand req2 = RemotingCommand.CreateRequestCommand(RequestCode.GetMaxOffset);
        req2.SerializeTypeCurrentRpc = SerializeType.Rocketmq;
        req2.AddExtField("topic", "TBW102");
        req2.AddExtField("queueId", "0");
        RemotingCommand dec2 = RemotingCommand.Decode(req2.Encode());
        bool ok2 = dec2.SerializeTypeCurrentRpc == SerializeType.Rocketmq
                   && dec2.GetExtField("topic") == "TBW102";
        Check("GET_MAX_OFFSET rocketmq-binary round-trip", ok2);

        // 3) The 17-field MessageExt storage format: encode -> decode loop
        var m = new MessageExt
        {
            Topic = "SELF_CHECK",
            Body = Encoding.UTF8.GetBytes("hello-rocketmq-csharp"),
            BornHost = "127.0.0.1",
            BornHostPort = 10911,
            StoreHost = "127.0.0.1",
            StoreHostPort = 10911,
        };
        m.Tags = "SelfCheck";
        byte[] raw = MessageDecoder.EncodeMessageExt(m, false);
        MessageDecoder.DecodeMessage(raw, out MessageExt dm, true, true);
        bool ok3 = Encoding.UTF8.GetString(dm.Body) == "hello-rocketmq-csharp"
                   && dm.Tags == "SelfCheck";
        Check("message encode/decode round-trip", ok3);

        Console.WriteLine();
        bool allOk = fail == 0;
        Console.WriteLine("selfcheck: " + (allOk ? "ALL PASS" : "FAILED")
                          + " (PASS=" + pass.ToString(CultureInfo.InvariantCulture)
                          + " FAIL=" + fail.ToString(CultureInfo.InvariantCulture) + ")");
        return allOk ? 0 : 1;
    }
}
