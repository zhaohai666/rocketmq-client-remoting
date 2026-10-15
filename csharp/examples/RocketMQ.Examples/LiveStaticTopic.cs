// Real-machine integration of the C# client's static topics (513 createStaticTopic +
// endpoint rewriting) (scenario sections S1-S5).
//
// S1 createStaticTopic(513): a single broker, totalQueues=8, 4 physical queues.
// S2 route read back: TopicQueueMappingByBroker is non-empty.
// S3 topicRouteData2EndpointsForStaticTopic: 8 endpoints (0-3 -> the broker,
//    4-7 -> __syslo__none__).
// S4 instance rewriting: GetBrokerNameFromMessageQueue resolves a mock queue name to the
//    real broker; ids outside the table and non-static topics pass through.
// S5 end to end: a normal producer sends 4 messages, a PUSH consumer receives all 4.
//
// Usage: rmq static-topic-live [namesrv]
using System.Globalization;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveStaticTopic
{
    private const int PhysicalQueues = 4;
    private const int TotalQueues = 8;

    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private static string _nsAddr = "127.0.0.1:9876";
    private static string _topic = "StaticLive_" + Stamp;
    private static string _group = "StaticPG_" + Stamp;
    private static string _brokerName = string.Empty;

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            _pass++;
        }
        else
        {
            _fail++;
        }

        Console.WriteLine("  " + (ok ? "[PASS]" : "[FAIL]") + " " + name
                          + (detail.Length == 0 ? "" : "  " + detail));
    }

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 500)
    {
        long deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeoutMs;
        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    private sealed class Recorder : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        public readonly List<string> Bodies = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs)
                {
                    Bodies.Add(System.Text.Encoding.UTF8.GetString(m.Body));
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public int Count()
        {
            lock (_lk)
            {
                return Bodies.Count;
            }
        }
    }

    /// <summary>
    /// The TopicQueueMappingDetail document for a single broker (the 513 body, the
    /// broker-side fastjson2 field names of TopicQueueMappingDetail, matching the protocol
    /// definition field by field).
    /// </summary>
    private static JsonValue MappingDetail(string brokerName)
    {
        var currIdMap = JsonValue.MakeObject();
        var hosted = JsonValue.MakeObject();
        for (int gid = 0; gid < PhysicalQueues; ++gid)
        {
            currIdMap.Set(gid.ToString(CultureInfo.InvariantCulture), JsonValue.MakeInt(gid));

            var entry = JsonValue.MakeObject();
            entry.Set("gen", JsonValue.MakeInt(0));
            entry.Set("queueId", JsonValue.MakeInt(gid));
            entry.Set("bname", JsonValue.MakeString(brokerName));
            entry.Set("logicOffset", JsonValue.MakeInt(0));
            entry.Set("startOffset", JsonValue.MakeInt(0));
            entry.Set("endOffset", JsonValue.MakeInt(-1));
            entry.Set("timeOfStart", JsonValue.MakeInt(-1));
            entry.Set("timeOfEnd", JsonValue.MakeInt(-1));

            var list = JsonValue.MakeArray();
            list.PushArray(entry);
            hosted.Set(gid.ToString(CultureInfo.InvariantCulture), list);
        }

        var doc = JsonValue.MakeObject();
        doc.Set("topic", JsonValue.MakeString(_topic));
        doc.Set("scope", JsonValue.MakeString(StaticTopic.MetadataScopeGlobal));
        doc.Set("totalQueues", JsonValue.MakeInt(TotalQueues));
        doc.Set("bname", JsonValue.MakeString(brokerName));
        doc.Set("epoch", JsonValue.MakeInt(0));
        doc.Set("dirty", JsonValue.MakeBool(false));
        doc.Set("currIdMap", currIdMap);
        doc.Set("hostedQueues", hosted);
        return doc;
    }

    private static bool MappingByBrokerNonEmpty(TopicRouteData route)
    {
        return !route.TopicQueueMappingByBroker.IsNull
               && route.TopicQueueMappingByBroker.IsObject
               && route.TopicQueueMappingByBroker.ObjectItems().Count > 0;
    }

    public static int Run(string[] args)
    {
        if (args.Length > 0)
        {
            _nsAddr = args[0];
        }

        Console.WriteLine("namesrv=" + _nsAddr + " topic=" + _topic);

        // ---------------- S1 createStaticTopic(513) ----------------
        Console.WriteLine("\nS1 starting the admin side, CreateStaticTopic(513)");
        var admin = new DefaultMQAdminExt("StaticLiveAdmin_" + Stamp);
        admin.SetNamesrvAddr(_nsAddr);
        admin.Start();
        string brokerAddr;
        try
        {
            ClusterInfo info = admin.FetchBrokerClusterInfo();
            _brokerName = info.BrokerAddrTable.Keys.First();
            brokerAddr = info.BrokerAddrTable[_brokerName].SelectBrokerAddr();
            Check("S1 the broker address resolves", brokerAddr.Length > 0,
                _brokerName + "(" + brokerAddr + ")");

            var config = new TopicConfig(_topic)
            {
                ReadQueueNums = PhysicalQueues,
                WriteQueueNums = PhysicalQueues,
            };
            admin.CreateStaticTopic(brokerAddr, MixAll.DefaultTopic, config,
                MappingDetail(_brokerName), true);
            Check("S1 CreateStaticTopic(513) was accepted", true,
                "totalQueues=" + TotalQueues);
        }
        finally
        {
            admin.Shutdown();
        }

        // ---------------- S2/S3 route read back + endpoint set derivation ----------------
        Console.WriteLine("\nS2/S3 route read back + endpoint set derivation");
        TopicRouteData? route = null;
        WaitUntil(() =>
        {
            var probe = new DefaultMQAdminExt("StaticLiveProbe_" + Stamp);
            probe.SetNamesrvAddr(_nsAddr);
            probe.Start();
            try
            {
                TopicRouteData r = probe.ExamineTopicRoute(_topic);
                if (MappingByBrokerNonEmpty(r))
                {
                    route = r;
                    return true;
                }
            }
            catch (Exception)
            {
                // the route does not carry the mapping information yet, keep waiting
            }
            finally
            {
                probe.Shutdown();
            }

            return false;
        }, 30000, 2000);
        Check("S2 the route carries TopicQueueMappingByBroker", route is not null);
        if (route is null)
        {
            return Finish();
        }

        var endpoints = new Dictionary<MessageQueue, string>();
        bool derived = StaticTopic.TopicRouteData2EndpointsForStaticTopic(
            _topic, route, endpoints);
        Check("S3 the endpoint set is non-empty", derived && endpoints.Count > 0);
        if (!derived)
        {
            return Finish();
        }

        string mockName = StaticTopic.GetMockBrokerName(StaticTopic.MetadataScopeGlobal);
        Check("S3 the total endpoint count = totalQueues", endpoints.Count == TotalQueues,
            "got " + endpoints.Count + " want " + TotalQueues);

        bool hostedOk = true;
        for (int gid = 0; gid < PhysicalQueues; ++gid)
        {
            var mq = new MessageQueue(_topic, mockName, gid);
            endpoints.TryGetValue(mq, out string? bname);
            hostedOk = hostedOk && bname == _brokerName;
        }

        Check("S3 the hosted ids 0-" + (PhysicalQueues - 1) + " -> the broker", hostedOk);

        bool unhostedOk = true;
        for (int gid = PhysicalQueues; gid < TotalQueues; ++gid)
        {
            var mq = new MessageQueue(_topic, mockName, gid);
            endpoints.TryGetValue(mq, out string? bname);
            unhostedOk = unhostedOk && bname == StaticTopic.LogicalQueueMockNotExist;
        }

        Check("S3 the unhosted ids " + PhysicalQueues + "-" + (TotalQueues - 1)
              + " → __syslo__none__", unhostedOk);

        // ---------------- S4/S5 instance rewriting + end-to-end send/receive ----------------
        Console.WriteLine("\nS4/S5 instance rewriting + end-to-end send/receive");
        var rec = new Recorder();
        var consumer = new DefaultMQPushConsumer(_group);
        consumer.SetNamesrvAddr(_nsAddr);
        consumer.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
        consumer.Subscribe(_topic, "*");
        consumer.SetMessageListener(rec);
        consumer.Start();

        var inst = consumer.Client();
        bool rewriteOk = WaitUntil(() =>
        {
            for (int gid = 0; gid < PhysicalQueues; ++gid)
            {
                if (inst.GetBrokerNameFromMessageQueue(
                        new MessageQueue(_topic, mockName, gid)) != _brokerName)
                {
                    return false;
                }
            }

            return true;
        }, 20000);
        Check("S4 the instance rewrites the mock queue to the real broker", rewriteOk);

        // id TotalQueues-1 is inside the endpoint table and its value is __syslo__none__ (the
        // protocol semantics: return the endpoint value); the pass-through case has to use an
        // id outside the table (99 here).
        string unhostedRewrite = inst.GetBrokerNameFromMessageQueue(
            new MessageQueue(_topic, mockName, TotalQueues - 1));
        Check("S4 an unhosted id returns the sentinel value",
            unhostedRewrite == StaticTopic.LogicalQueueMockNotExist, unhostedRewrite);

        string outsideRewrite = inst.GetBrokerNameFromMessageQueue(
            new MessageQueue(_topic, mockName, 99));
        Check("S4 an id outside the table passes the mock name through", outsideRewrite == mockName, outsideRewrite);

        string plainRewrite = inst.GetBrokerNameFromMessageQueue(
            new MessageQueue("Plain_" + Stamp, _brokerName, 0));
        Check("S4 a non-static topic passes through as is", plainRewrite == _brokerName, plainRewrite);

        var prod = new DefaultMQProducer("StaticProd_" + Stamp)
        {
            NamesrvAddr = _nsAddr,
            SendMsgTimeout = 5000,
        };
        prod.Start();
        int sent = 0;
        try
        {
            for (int i = 0; i < PhysicalQueues; ++i)
            {
                var msg = new Message(_topic,
                    System.Text.Encoding.UTF8.GetBytes("static-" + i));
                SendResult r = prod.Send(msg, 5000);
                if (r.SendStatus == SendStatus.SendOk)
                {
                    sent++;
                }
            }
        }
        finally
        {
            prod.Shutdown();
        }

        Check("S5 sent " + PhysicalQueues + " messages", sent == PhysicalQueues,
            "sent=" + sent);

        bool consumed = WaitUntil(() => rec.Count() >= PhysicalQueues, 40000, 1000);
        Check("S5 consumption received all " + PhysicalQueues, consumed,
            "got " + rec.Count());
        consumer.Shutdown();

        return Finish();
    }

    private static int Finish()
    {
        Console.WriteLine("\n=== RESULT pass=" + _pass + " fail=" + _fail + " ===");
        return _fail == 0 ? 0 : 1;
    }
}
