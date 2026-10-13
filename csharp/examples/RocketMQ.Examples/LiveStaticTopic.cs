// C# 客户端静态主题（513 createStaticTopic + 端点重写）真机联调（场景段 S1-S5）。
//
// S1 createStaticTopic(513)：单 broker，totalQueues=8，物理 4 队列。
// S2 路由回读：TopicQueueMappingByBroker 非空。
// S3 topicRouteData2EndpointsForStaticTopic：8 条端点（0-3 → broker，
//    4-7 → __syslo__none__）。
// S4 实例重写：GetBrokerNameFromMessageQueue 对 mock 队列名解析到真实
//    broker；表外 id 与非静态主题透传。
// S5 端到端：普通生产者发 4 条、PUSH 消费者收齐 4 条。
//
// 用法：rmq static-topic-live [namesrv]
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
    /// 单 broker 的 TopicQueueMappingDetail 文档（513 body，broker 侧 TopicQueueMappingDetail
    /// 的 fastjson2 字段名，与协议定义逐字段一致）。
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
        Console.WriteLine("\nS1 起管理端，CreateStaticTopic(513)");
        var admin = new DefaultMQAdminExt("StaticLiveAdmin_" + Stamp);
        admin.SetNamesrvAddr(_nsAddr);
        admin.Start();
        string brokerAddr;
        try
        {
            ClusterInfo info = admin.FetchBrokerClusterInfo();
            _brokerName = info.BrokerAddrTable.Keys.First();
            brokerAddr = info.BrokerAddrTable[_brokerName].SelectBrokerAddr();
            Check("S1 broker 地址解析", brokerAddr.Length > 0,
                _brokerName + "(" + brokerAddr + ")");

            var config = new TopicConfig(_topic)
            {
                ReadQueueNums = PhysicalQueues,
                WriteQueueNums = PhysicalQueues,
            };
            admin.CreateStaticTopic(brokerAddr, MixAll.DefaultTopic, config,
                MappingDetail(_brokerName), true);
            Check("S1 CreateStaticTopic(513) 被接受", true,
                "totalQueues=" + TotalQueues);
        }
        finally
        {
            admin.Shutdown();
        }

        // ---------------- S2/S3 路由回读 + 端点集推导 ----------------
        Console.WriteLine("\nS2/S3 路由回读 + 端点集推导");
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
                // 路由还没带上 mapping 信息，继续等
            }
            finally
            {
                probe.Shutdown();
            }

            return false;
        }, 30000, 2000);
        Check("S2 路由携带 TopicQueueMappingByBroker", route is not null);
        if (route is null)
        {
            return Finish();
        }

        var endpoints = new Dictionary<MessageQueue, string>();
        bool derived = StaticTopic.TopicRouteData2EndpointsForStaticTopic(
            _topic, route, endpoints);
        Check("S3 端点集非空", derived && endpoints.Count > 0);
        if (!derived)
        {
            return Finish();
        }

        string mockName = StaticTopic.GetMockBrokerName(StaticTopic.MetadataScopeGlobal);
        Check("S3 端点总数 = totalQueues", endpoints.Count == TotalQueues,
            "got " + endpoints.Count + " want " + TotalQueues);

        bool hostedOk = true;
        for (int gid = 0; gid < PhysicalQueues; ++gid)
        {
            var mq = new MessageQueue(_topic, mockName, gid);
            endpoints.TryGetValue(mq, out string? bname);
            hostedOk = hostedOk && bname == _brokerName;
        }

        Check("S3 承载 id 0-" + (PhysicalQueues - 1) + " → broker", hostedOk);

        bool unhostedOk = true;
        for (int gid = PhysicalQueues; gid < TotalQueues; ++gid)
        {
            var mq = new MessageQueue(_topic, mockName, gid);
            endpoints.TryGetValue(mq, out string? bname);
            unhostedOk = unhostedOk && bname == StaticTopic.LogicalQueueMockNotExist;
        }

        Check("S3 无承载 id " + PhysicalQueues + "-" + (TotalQueues - 1)
              + " → __syslo__none__", unhostedOk);

        // ---------------- S4/S5 实例重写 + 端到端收发 ----------------
        Console.WriteLine("\nS4/S5 实例重写 + 端到端收发");
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
        Check("S4 实例把 mock 队列重写到真实 broker", rewriteOk);

        // id TotalQueues-1 在端点表内，值就是 __syslo__none__（协议语义：返回端点值）；
        // 透传用例要用表外 id（这里取 99）。
        string unhostedRewrite = inst.GetBrokerNameFromMessageQueue(
            new MessageQueue(_topic, mockName, TotalQueues - 1));
        Check("S4 无承载 id 返回哨兵值",
            unhostedRewrite == StaticTopic.LogicalQueueMockNotExist, unhostedRewrite);

        string outsideRewrite = inst.GetBrokerNameFromMessageQueue(
            new MessageQueue(_topic, mockName, 99));
        Check("S4 表外 id 透传 mock 名", outsideRewrite == mockName, outsideRewrite);

        string plainRewrite = inst.GetBrokerNameFromMessageQueue(
            new MessageQueue("Plain_" + Stamp, _brokerName, 0));
        Check("S4 非静态主题原样透传", plainRewrite == _brokerName, plainRewrite);

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

        Check("S5 发送 " + PhysicalQueues + " 条", sent == PhysicalQueues,
            "sent=" + sent);

        bool consumed = WaitUntil(() => rec.Count() >= PhysicalQueues, 40000, 1000);
        Check("S5 消费收齐 " + PhysicalQueues + " 条", consumed,
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
