// POP 模式（5.x 轻量消费）真机验证。
// 用法：rmq pop [namesrv]
//
// 与 cpp/examples/live_pop.cpp、python/verify_pop_live.py 完全同场景
// （三语言对拍用同一套断言）。
//
// POP 与 pull 的语义差异是断言的重心：
//   - **不提交位点**，靠 ack 确认；不 ack 的消息在 invisibleTime 后被复活重投；
//   - broker 在普通 topic 消息上**不写** POP_CK，客户端必须自己反构 8 段 CK 串。
//
// 场景：
//   S1 建 topic(8 队列) + 发 10 条
//   S2 多队列 POP（queueId=-1, initMode=0）→ FOUND 且拿到消息
//   S3 每条消息都被盖上 **8 段** POP_CK，且 brokerName/queueId 与消息实际一致
//   S4 ack 一条 → SUCCESS
//   S5 ChangeInvisibleTime → SUCCESS 且返回**新的** 8 段 extraInfo
//   S6 校验真的生效：非法 queueId → MESSAGE_ILLEGAL；越界 offset → NO_MESSAGE
//   S7 单队列 POP（新消费组 + queueId=0）→ FOUND 且消息 queueId 全为 0
//   S8 不 ack 会复活：小 invisibleTime POP 后不 ack，等待后重 POP 能拿到同一条消息，
//      且它的 POP_CK 是 retryFlag=1（来自 %RETRY%<group>_<topic>）—— 至少一次语义
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
        Console.WriteLine("POP live (.NET): namesrv=" + namesrv + " topic=" + Topic
            + " group=" + Group);
        Console.WriteLine(new string('=', 70));

        var producer = new DefaultMQProducer("PG_PopLiveNet_" + Stamp);
        producer.NamesrvAddr = namesrv;
        producer.Start();
        MQClientInstance client = producer.Client();

        try
        {
            // ------------------------------------------------ S1 建 topic + 发消息
            Console.WriteLine("=== S1 准备 topic 与消息 ===");
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
            Check("S1 topic 路由可用", route is not null && route.BrokerDatas.Count > 0);
            if (route is null || route.BrokerDatas.Count == 0)
            {
                return Finish(producer);
            }

            string brokerName = route.BrokerDatas[0].BrokerName;
            string addr = MQClientInstance.FindBrokerAddrInRoute(route, brokerName);
            Console.WriteLine("  brokerName=" + brokerName + " addr=" + addr);

            // ------------------------------------------------ S2 多队列 POP
            Console.WriteLine("=== S2 多队列 POP（addr 走自动解析路径）===");
            PopResult res = client.PopMessage(Group, Topic, -1, 32, 60000, 0, 0);
            Check("S2 POP 取到消息", res.IsFound && res.MsgFoundList.Count > 0,
                "status=" + PopStatusNames.Name(res.Status)
                    + " count=" + res.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture)
                    + " startOffsetInfo=" + res.StartOffsetInfo);
            if (res.MsgFoundList.Count == 0)
            {
                return Finish(producer);
            }

            // ------------------------------------------------ S3 POP_CK 反构
            Console.WriteLine("=== S3 POP_CK 反构（8 段）===");
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

            Check("S3a 每条消息都有 8 段 POP_CK", all8, "sample='" + sample + "'");
            Check("S3b CK 的 brokerName/queueId 与消息一致", match);
            Check("S3c 1ST_POP_TIME 已补", hasFirstPopTime);

            // ------------------------------------------------ S4 ack
            Console.WriteLine("=== S4 ACK ===");
            MessageExt first = res.MsgFoundList[0];
            string ck1 = PopCkOf(first);
            string[] seg1 = ExtraInfoUtil.Split(ck1);
            int qid1 = ExtraInfoUtil.GetQueueId(seg1);
            long off1 = ExtraInfoUtil.GetQueueOffset(seg1);
            int ackCode = client.AckMessage(Group, Topic, qid1, ck1, off1, 3000, brokerName, addr);
            Check("S4 ack 返回 SUCCESS", ackCode == ResponseCode.Success,
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
            Check("S5a 延长不可见时间成功", res5.Success,
                "code=" + res5.Code.ToString(CultureInfo.InvariantCulture)
                    + " popTime=" + res5.PopTime.ToString(CultureInfo.InvariantCulture)
                    + " invisibleTime=" + res5.InvisibleTime.ToString(CultureInfo.InvariantCulture));
            string[] newSeg = res5.ExtraInfo.Length == 0
                ? Array.Empty<string>()
                : ExtraInfoUtil.Split(res5.ExtraInfo);
            Check("S5b 返回新的 8 段 extraInfo 且用新值",
                newSeg.Length == 8
                    && ExtraInfoUtil.GetInvisibleTime(newSeg) == res5.InvisibleTime
                    && ExtraInfoUtil.GetPopTime(newSeg) == res5.PopTime,
                "new='" + res5.ExtraInfo + "'");

            // ------------------------------------------------ S6 校验真的生效
            Console.WriteLine("=== S6 非法参数必须被拒绝 ===");
            int badQueue = client.AckMessage(Group, Topic, QueueNum + 90, ck1, off1, 3000,
                brokerName, addr);
            Check("S6a 非法 queueId 被拒（MESSAGE_ILLEGAL）",
                badQueue == ResponseCode.MessageIllegal,
                "code=" + badQueue.ToString(CultureInfo.InvariantCulture));
            int badOffset = client.AckMessage(Group, Topic, qid1, ck1, 1L << 40, 3000,
                brokerName, addr);
            Check("S6b 越界 offset 被拒（NO_MESSAGE）", badOffset == ResponseCode.NoMessage,
                "code=" + badOffset.ToString(CultureInfo.InvariantCulture));

            // ------------------------------------------------ S7 单队列 POP
            Console.WriteLine("=== S7 单队列 POP ===");
            // 用新消费组：老组在同一 topic 上已有 pop 位点，重复 POP 拿不到东西
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

            Check("S7 单队列 POP 取到队列 0 的消息",
                res7.IsFound && res7.MsgFoundList.Count > 0 && onlyQ0,
                "status=" + PopStatusNames.Name(res7.Status)
                    + " count=" + res7.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));

            // ------------------------------------------------ S8 不 ack 会复活
            Console.WriteLine("=== S8 不 ack → 复活重投（至少一次语义）===");
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

            Check("S8a 首轮 POP 取到消息（不 ack）",
                res8a.IsFound && res8a.MsgFoundList.Count > 0,
                "count=" + res8a.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));

            Console.WriteLine("  等待 " + ReviveWaitSec.ToString(CultureInfo.InvariantCulture)
                + " s 让 broker 复活...");
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

            Check("S8b 未 ack 的消息被复活重投", revived > 0,
                "revived=" + revived.ToString(CultureInfo.InvariantCulture)
                    + " total=" + res8b.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));
            Check("S8c 复活消息的 POP_CK retryFlag=1（来自 %RETRY%<group>_<topic>）",
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
            // 收尾失败不影响断言结论
        }

        Console.WriteLine("############ PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture) + " ############");
        return _fail == 0 ? 0 : 1;
    }
}
