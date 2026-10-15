// Real-machine verification of POP mode (5.x lightweight consumption).
// Usage: rmq pop [namesrv]
//
// The semantic differences between POP and pull are the centre of the assertions:
//   - **no offset is committed**, confirmation comes via ack; a message that is not acked is
//     revived and redelivered after invisibleTime;
//   - the broker does **not** write POP_CK on a plain topic message, so the client has to
//     reconstruct the 8-segment CK string itself.
//
// Scenarios:
//   S1 create the topic (8 queues) + send 10 messages
//   S2 multi-queue POP (queueId=-1, initMode=0) -> FOUND with messages
//   S3 every message is stamped with an **8-segment** POP_CK whose brokerName/queueId match the message
//   S4 ack one -> SUCCESS
//   S5 ChangeInvisibleTime -> SUCCESS and it returns a **new** 8-segment extraInfo
//   S6 verification really works: an illegal queueId -> MESSAGE_ILLEGAL; an out-of-range offset -> NO_MESSAGE
//   S7 single-queue POP (a new consumer group + queueId=0) -> FOUND and every message has queueId 0
//   S8 not acking revives: a POP with a small invisibleTime that is not acked can POP the very
//      same message again after waiting, and its POP_CK has retryFlag=1 (from
//      %RETRY%<group>_<topic>) -- at-least-once semantics
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LivePop
{
    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private static readonly string Topic = "PopLiveNet_" + Stamp;
    private static readonly string TopicRevive = "PopLiveReviveNet_" + Stamp;
    private static readonly string Group = "GID_PopLiveNet_" + Stamp;
    private static readonly string GroupSingle = "GID_PopLiveNetSingle_" + Stamp;
    private static readonly string GroupRevive = "GID_PopLiveNetRevive_" + Stamp;

    private const int QueueNum = 8;
    private const int NMsg = 10;
    private const int ReviveQueueNum = 4;
    private const int ReviveNMsg = 3;
    private const long InvisibleShort = 5000;
    private const int ReviveWaitSec = 20;

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            Interlocked.Increment(ref _pass);
            Console.WriteLine("  [PASS] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
        else
        {
            Interlocked.Increment(ref _fail);
            Console.WriteLine("  [FAIL] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
    }

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string UniqKeyOf(MessageExt m) =>
        m.Properties.TryGetValue(MessageConst.PropertyUniqClientMessageIdKeyidx, out string? v)
            ? v
            : m.MsgId;

    private static string PopCkOf(MessageExt m) =>
        m.Properties.TryGetValue(MessageConst.PropertyPopCk, out string? v) ? v : string.Empty;

    private static int I(int v) => v;

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("POP live (C#): namesrv=" + namesrv + " topic=" + Topic
            + " group=" + Group);
        Console.WriteLine(new string('=', 70));

        var producer = new DefaultMQProducer("PG_PopLiveNet_" + Stamp);
        producer.NamesrvAddr = namesrv;
        producer.Start();
        MQClientInstance client = producer.Client();

        try
        {
            // ------------------------------------------------ S1 create the topic + send messages
            Console.WriteLine("=== S1 preparing the topic and the messages ===");
            producer.CreateTopic("TBW102", Topic, QueueNum);
            producer.CreateTopic("TBW102", TopicRevive, ReviveQueueNum);
            for (int i = 0; i < NMsg; ++i)
            {
                var msg = new Message(Topic, Str2Bytes("pop-live-net-" + i.ToString(CultureInfo.InvariantCulture)));
                msg.PutProperty("KEYS", "pk" + i.ToString(CultureInfo.InvariantCulture));
                producer.Send(msg);
            }

            Console.WriteLine("  sent " + NMsg.ToString(CultureInfo.InvariantCulture)
                + " msgs to " + Topic);

            TopicRouteData? route = client.GetTopicRouteData(Topic);
            Check("S1 the topic route is usable", route is not null && route.BrokerDatas.Count > 0);
            if (route is null || route.BrokerDatas.Count == 0)
            {
                return Finish(producer);
            }

            string brokerName = route.BrokerDatas[0].BrokerName;
            string addr = MQClientInstance.FindBrokerAddrInRoute(route, brokerName);
            Console.WriteLine("  brokerName=" + brokerName + " addr=" + addr);

            // ------------------------------------------------ S2 multi-queue POP
            Console.WriteLine("=== S2 multi-queue POP (the addr goes through the auto-resolution path) ===");
            PopResult res = client.PopMessage(Group, Topic, -1, 32, 60000, 0, 0);
            Check("S2 the POP got messages", res.IsFound && res.MsgFoundList.Count > 0,
                "status=" + PopStatusNames.Name(res.Status)
                    + " count=" + res.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture)
                    + " startOffsetInfo=" + res.StartOffsetInfo);
            if (res.MsgFoundList.Count == 0)
            {
                return Finish(producer);
            }

            // ------------------------------------------------ S3 POP_CK reconstruction
            Console.WriteLine("=== S3 POP_CK reconstruction (8 segments) ===");
            bool all8 = true;
            bool match = true;
            bool hasFirstPopTime = true;
            string sample = string.Empty;
            foreach (MessageExt m in res.MsgFoundList)
            {
                string ck = PopCkOf(m);
                if (ck.Length == 0)
                {
                    all8 = false;
                    break;
                }

                string[] seg = ExtraInfoUtil.Split(ck);
                if (seg.Length != 8)
                {
                    all8 = false;
                    break;
                }

                if (ExtraInfoUtil.GetBrokerName(seg) != brokerName
                    || ExtraInfoUtil.GetQueueId(seg) != m.QueueId)
                {
                    match = false;
                }

                if (!m.Properties.ContainsKey(MessageConst.PropertyFirstPopTime))
                {
                    hasFirstPopTime = false;
                }

                if (sample.Length == 0)
                {
                    sample = ck;
                }
            }

            Check("S3a every message has an 8-segment POP_CK", all8, "sample='" + sample + "'");
            Check("S3b the CK's brokerName/queueId match the message", match);
            Check("S3c 1ST_POP_TIME has been filled in", hasFirstPopTime);

            // ------------------------------------------------ S4 ack
            Console.WriteLine("=== S4 ACK ===");
            MessageExt first = res.MsgFoundList[0];
            string ck1 = PopCkOf(first);
            string[] seg1 = ExtraInfoUtil.Split(ck1);
            int qid1 = ExtraInfoUtil.GetQueueId(seg1);
            long off1 = ExtraInfoUtil.GetQueueOffset(seg1);
            int ackCode = client.AckMessage(Group, Topic, qid1, ck1, off1, 3000, brokerName, addr);
            Check("S4 ack returns SUCCESS", ackCode == ResponseCode.Success,
                "code=" + ackCode.ToString(CultureInfo.InvariantCulture)
                    + " queueId=" + qid1.ToString(CultureInfo.InvariantCulture)
                    + " offset=" + off1.ToString(CultureInfo.InvariantCulture));

            // ------------------------------------------------ S5 ChangeInvisibleTime
            Console.WriteLine("=== S5 CHANGE_MESSAGE_INVISIBLETIME ===");
            MessageExt second = res.MsgFoundList[1];
            string ck2 = PopCkOf(second);
            string[] seg2 = ExtraInfoUtil.Split(ck2);
            ChangeInvisibleTimeResult res5 = client.ChangeInvisibleTime(Group, Topic,
                ExtraInfoUtil.GetQueueId(seg2), ck2, ExtraInfoUtil.GetQueueOffset(seg2), 30000,
                3000, brokerName, addr);
            Check("S5a extending the invisible time succeeded", res5.Success,
                "code=" + res5.Code.ToString(CultureInfo.InvariantCulture)
                    + " popTime=" + res5.PopTime.ToString(CultureInfo.InvariantCulture)
                    + " invisibleTime=" + res5.InvisibleTime.ToString(CultureInfo.InvariantCulture));
            string[] newSeg = res5.ExtraInfo.Length == 0
                ? Array.Empty<string>()
                : ExtraInfoUtil.Split(res5.ExtraInfo);
            Check("S5b a new 8-segment extraInfo came back and the new values are used",
                newSeg.Length == 8
                    && ExtraInfoUtil.GetInvisibleTime(newSeg) == res5.InvisibleTime
                    && ExtraInfoUtil.GetPopTime(newSeg) == res5.PopTime,
                "new='" + res5.ExtraInfo + "'");

            // ------------------------------------------------ S6 verification really works
            Console.WriteLine("=== S6 illegal parameters must be rejected ===");
            int badQueue = client.AckMessage(Group, Topic, QueueNum + 90, ck1, off1, 3000,
                brokerName, addr);
            Check("S6a an illegal queueId is rejected (MESSAGE_ILLEGAL)",
                badQueue == ResponseCode.MessageIllegal,
                "code=" + badQueue.ToString(CultureInfo.InvariantCulture));
            int badOffset = client.AckMessage(Group, Topic, qid1, ck1, 1L << 40, 3000,
                brokerName, addr);
            Check("S6b an out-of-range offset is rejected (NO_MESSAGE)", badOffset == ResponseCode.NoMessage,
                "code=" + badOffset.ToString(CultureInfo.InvariantCulture));

            // ------------------------------------------------ S7 single-queue POP
            Console.WriteLine("=== S7 single-queue POP ===");
            // A new consumer group is used: the old group already has a pop offset on this
            // topic and repeating the POP would get nothing
            PopResult res7 = client.PopMessage(GroupSingle, Topic, 0, 32, 60000, 0, 0,
                "*", "TAG", false, 10000, brokerName, addr);
            bool onlyQ0 = true;
            foreach (MessageExt m in res7.MsgFoundList)
            {
                if (m.QueueId != I(0))
                {
                    onlyQ0 = false;
                }
            }

            Check("S7 the single-queue POP got the messages of queue 0",
                res7.IsFound && res7.MsgFoundList.Count > 0 && onlyQ0,
                "status=" + PopStatusNames.Name(res7.Status)
                    + " count=" + res7.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));

            // ------------------------------------------------ S8 not acking revives
            Console.WriteLine("=== S8 not acking -> revived redelivery (at-least-once semantics) ===");
            for (int i = 0; i < ReviveNMsg; ++i)
            {
                var msg = new Message(TopicRevive,
                    Str2Bytes("pop-revive-net-" + i.ToString(CultureInfo.InvariantCulture)));
                msg.PutProperty("KEYS", "rv" + i.ToString(CultureInfo.InvariantCulture));
                producer.Send(msg);
            }

            TopicRouteData? routeRv = client.GetTopicRouteData(TopicRevive);
            string addrRv = string.Empty;
            if (routeRv is not null && routeRv.BrokerDatas.Count > 0)
            {
                addrRv = MQClientInstance.FindBrokerAddrInRoute(routeRv,
                    routeRv.BrokerDatas[0].BrokerName);
            }

            PopResult res8a = client.PopMessage(GroupRevive, TopicRevive, -1, 32, InvisibleShort,
                0, 0, "*", "TAG", false, 10000, string.Empty, addrRv);
            var firstKeys = new HashSet<string>();
            foreach (MessageExt m in res8a.MsgFoundList)
            {
                firstKeys.Add(UniqKeyOf(m));
            }

            Check("S8a the first POP got messages (not acked)",
                res8a.IsFound && res8a.MsgFoundList.Count > 0,
                "count=" + res8a.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));

            Console.WriteLine("  waiting " + ReviveWaitSec.ToString(CultureInfo.InvariantCulture)
                + " s for the broker to revive...");
            Thread.Sleep(ReviveWaitSec * 1000);

            PopResult res8b = client.PopMessage(GroupRevive, TopicRevive, -1, 32, 60000, 0, 0,
                "*", "TAG", false, 10000, string.Empty, addrRv);
            int revived = 0;
            var retryFlags = new HashSet<string>();
            foreach (MessageExt m in res8b.MsgFoundList)
            {
                if (!firstKeys.Contains(UniqKeyOf(m)))
                {
                    continue;
                }

                ++revived;
                string ck = PopCkOf(m);
                if (ck.Length > 0)
                {
                    retryFlags.Add(ExtraInfoUtil.GetRetry(ExtraInfoUtil.Split(ck)));
                }
            }

            Check("S8b the un-acked message was revived and redelivered", revived > 0,
                "revived=" + revived.ToString(CultureInfo.InvariantCulture)
                    + " total=" + res8b.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));
            Check("S8c the revived message's POP_CK has retryFlag=1 (from %RETRY%<group>_<topic>)",
                retryFlags.Contains("1"),
                "retryFlagsSize=" + retryFlags.Count.ToString(CultureInfo.InvariantCulture));

            return Finish(producer);
        }
        catch (Exception ex)
        {
            Console.WriteLine("[FATAL] " + ex);
            ++_fail;
            return Finish(producer);
        }
    }

    private static int Finish(DefaultMQProducer producer)
    {
        try
        {
            producer.Shutdown();
        }
        catch (Exception)
        {
            // a cleanup failure does not affect the conclusion of the assertions
        }

        Console.WriteLine("############ PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture) + " ############");
        return _fail == 0 ? 0 : 1;
    }
}
