// C# admin client (DefaultMQAdminExt) **real-cluster** integration (every admin-side RPC exercised).
//
//
// This program does not start a cluster itself; the caller must start nameServer(9876) + broker(10911) with
// autoCreateTopicEnable=true. Usage (invoked by Program as "admin-live [namesrv]").
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>Full-chain admin integration against a real cluster.</summary>
internal static class AdminLive
{
    private static string _gNamesrv = "127.0.0.1:9876";
    private static int _gPass;
    private static int _gFail;
    private static int _gSkip;
    private static readonly List<(string Name, bool Ok)> Results = new();

    private static void Check(string name, bool ok, string detail = "")
    {
        Results.Add((name, ok));
        if (ok) ++_gPass;
        else ++_gFail;
        Console.WriteLine("[" + (ok ? "PASS" : "FAIL") + "] " + name + (detail.Length > 0 ? "  " + detail : string.Empty));
    }

    // Records items that "depend on broker-side config differences and do not hold on this machine"; not counted as failures.
    private static void Skip(string name, string detail)
    {
        ++_gSkip;
        Console.WriteLine("[SKIP] " + name + "  " + detail);
    }

    private static string Bytes2Str(byte[] b) => Encoding.UTF8.GetString(b);

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    // ---------------- Collect the first batch of consumed messages (sendMessageBack needs the original MessageExt) ----------------

    private sealed class CollectingListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private MessageExt _first = new();
        private long _count;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                if (_first.Topic.Length == 0 && msgs.Count > 0) _first = msgs[0];
                _count += msgs.Count;
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public MessageExt First()
        {
            lock (_lk) return _first;
        }

        public long Count()
        {
            lock (_lk) return _count;
        }
    }

    private static int Report()
    {
        Console.WriteLine("\n================ Summary ================");
        foreach ((string Name, bool Ok) r in Results)
        {
            Console.WriteLine("  [" + (r.Ok ? "PASS" : "FAIL") + "] " + r.Name);
        }

        Console.WriteLine("======================================");
        Console.WriteLine("Total " + (_gPass + _gFail).ToString(CultureInfo.InvariantCulture)
                          + " items, " + _gFail.ToString(CultureInfo.InvariantCulture)
                          + " failed, " + _gSkip.ToString(CultureInfo.InvariantCulture) + " skipped");
        return _gFail == 0 ? 0 : 1;
    }

    // ---------------- Entry point ----------------

    public static int Run(string[] args)
    {
        if (args.Length > 0) _gNamesrv = args[0];

        string stamp = UtilAll.CurrentTimeMillis().ToString(CultureInfo.InvariantCulture);
        string topic = "AdminLiveDotnet_" + stamp;
        string group = "AdminLiveDotnetGroup_" + stamp;
        string kvNs = "AdminLiveDotnetKv_" + stamp;
        const int nMsg = 8;

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_gNamesrv);
        admin.SetTimeoutMillis(10000);
        try
        {
            admin.Start();
        }
        catch (Exception e)
        {
            Console.WriteLine("[FATAL] admin start failed: " + e.Message);
            return 1;
        }

        // ---------- 1. Cluster liveness (broker registration race: port open != registered) ----------
        bool clusterOk = false;
        ClusterInfo? cluster = null;
        string lastErr = string.Empty;
        for (int i = 0; i < 40; ++i)
        {
            try
            {
                ClusterInfo ci = admin.FetchBrokerClusterInfo();
                if (ci.BrokerAddrTable.Count > 0)
                {
                    cluster = ci;
                    clusterOk = true;
                    break;
                }
            }
            catch (Exception e)
            {
                lastErr = e.Message;
            }

            Thread.Sleep(1000);
        }

        if (!clusterOk || cluster is null)
        {
            Check("cluster liveness", false, "nameServer has no registered brokers last_err=" + lastErr);
            admin.Shutdown();
            return Report();
        }

        List<string> addrs = cluster.GetBrokerAddrs();
        Check("fetchBrokerClusterInfo", true,
            "brokers=" + cluster.BrokerAddrTable.Count.ToString(CultureInfo.InvariantCulture)
            + " addrs=" + addrs.Count.ToString(CultureInfo.InvariantCulture));
        string brokerAddr = addrs[0];

        // ---------- 2. Topic management ----------
        try
        {
            admin.CreateTopic(MixAll.DefaultTopic, topic, 4);
            Check("createTopic(" + topic + ")", true, "queueNum=4");
        }
        catch (Exception e)
        {
            Check("createTopic(" + topic + ")", false, e.Message);
        }

        Thread.Sleep(1000);

        try
        {
            TopicList all = admin.FetchAllTopicList();
            Check("fetchAllTopicList", true, "topicCount=" + all.Topics.Count.ToString(CultureInfo.InvariantCulture));
            Check("new topic appears in topicList", all.Contains(topic), topic);
        }
        catch (Exception e)
        {
            Check("fetchAllTopicList", false, e.Message);
        }

        try
        {
            TopicRouteData route = admin.ExamineTopicRoute(topic);
            Check("examineTopicRoute", true,
                "brokers=" + route.BrokerDatas.Count.ToString(CultureInfo.InvariantCulture)
                + " queues=" + route.QueueDatas.Count.ToString(CultureInfo.InvariantCulture));
            bool allFour = route.QueueDatas.Count > 0;
            foreach (QueueData qd in route.QueueDatas)
            {
                if (qd.ReadQueueNums != 4) allFour = false;
            }

            Check("route readQueueNums==4", allFour, "ok");
        }
        catch (Exception e)
        {
            Check("examineTopicRoute", false, e.Message);
        }

        try
        {
            HashSet<string> clusters = admin.GetClusterList(topic);
            Check("getClusterList", clusters.Count > 0, "clusters=" + clusters.Count.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("getClusterList", false, e.Message);
        }

        try
        {
            TopicConfig cfg = admin.ExamineTopicConfig(brokerAddr, topic);
            Check("examineTopicConfig", true,
                "read=" + cfg.ReadQueueNums.ToString(CultureInfo.InvariantCulture)
                + " write=" + cfg.WriteQueueNums.ToString(CultureInfo.InvariantCulture)
                + " perm=" + cfg.Perm.ToString(CultureInfo.InvariantCulture)
                + " filter=" + cfg.TopicFilterType);
            Check("TopicConfig queue counts match creation", cfg.ReadQueueNums == 4 && cfg.WriteQueueNums == 4,
                "read=" + cfg.ReadQueueNums.ToString(CultureInfo.InvariantCulture)
                + " write=" + cfg.WriteQueueNums.ToString(CultureInfo.InvariantCulture));
            Check("TopicConfig default perm=6", cfg.Perm == 6, "perm=" + cfg.Perm.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("examineTopicConfig", false, e.Message);
        }

        try
        {
            TopicConfigSerializeWrapper w = admin.GetAllTopicConfig(brokerAddr);
            Check("getAllTopicConfig", w.TopicConfigTable.Count > 0,
                "topicConfigs=" + w.TopicConfigTable.Count.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("getAllTopicConfig", false, e.Message);
        }

        // ---------- 3. Broker config: properties text (historical bug spot) ----------
        PropertyMap brokerCfg = new();
        try
        {
            brokerCfg = admin.GetBrokerConfig(brokerAddr);
            Check("getBrokerConfig (properties text)", brokerCfg.Count > 0,
                "keys=" + brokerCfg.Count.ToString(CultureInfo.InvariantCulture));
            bool hasBn = brokerCfg.TryGetValue("brokerName", out var bn) && (bn?.Length ?? 0) > 0;
            Check("getBrokerConfig parsed brokerName", hasBn, "brokerName=" + (hasBn ? bn : string.Empty));
        }
        catch (Exception e)
        {
            Check("getBrokerConfig (properties text)", false, e.Message);
        }

        if (brokerCfg.Count > 0)
        {
            // Reversible change: write a harmless config then restore it
            bool foundO = brokerCfg.TryGetValue("sendMessageThreadPoolNums", out var o);
            string original = foundO && o != null ? o : string.Empty;
            const string probe = "11";
            try
            {
                var upd = new PropertyMap { ["sendMessageThreadPoolNums"] = probe };
                admin.UpdateBrokerConfig(brokerAddr, upd);
                Check("updateBrokerConfig(sendMessageThreadPoolNums=11)", true, "OK");
                Thread.Sleep(1000);
                PropertyMap after = admin.GetBrokerConfig(brokerAddr);
                bool ok = after.TryGetValue("sendMessageThreadPoolNums", out var v) && v == probe;
                Check("updateBrokerConfig took effect", ok, "expected " + probe + " got " + v);
                if (original.Length > 0)
                {
                    var back = new PropertyMap { ["sendMessageThreadPoolNums"] = original };
                    admin.UpdateBrokerConfig(brokerAddr, back);
                }
            }
            catch (Exception e)
            {
                Check("updateBrokerConfig", false, e.Message);
            }
        }

        // ---------- 4. NameServer KV config ----------
        try
        {
            admin.CreateAndUpdateKvConfig(kvNs, "k1", "v1");
            Check("createAndUpdateKvConfig", true, "OK");
            bool found = admin.GetKvConfig(kvNs, "k1", out string value);
            Check("getKVConfig", found, "value=" + value);
            Check("KV value round trip", found && value == "v1", "expected v1 got " + value);
            KvTable t = admin.GetKvListByNamespace(kvNs);
            Check("getKVListByNamespace contains k1", t.Table.ContainsKey("k1"),
                "tableSize=" + t.Table.Count.ToString(CultureInfo.InvariantCulture));
            admin.DeleteKvConfig(kvNs, "k1");
            Check("deleteKVConfig", true, "OK");
            bool stillThere = admin.GetKvConfig(kvNs, "k1", out _);
            Check("KV gone after delete", !stillThere, "found=" + (stillThere ? "1" : "0"));
        }
        catch (Exception e)
        {
            Check("NameServer KV chain", false, e.Message);
        }

        // ---------- 5. Subscription group management ----------
        {
            var sgc = new SubscriptionGroupConfig(group) { ConsumeEnable = true, RetryMaxTimes = 5 };
            try
            {
                admin.CreateAndUpdateSubscriptionGroupConfig(brokerAddr, sgc);
                Check("createAndUpdateSubscriptionGroupConfig", true, "group=" + group);

                bool ok = admin.GetSubscriptionGroupConfig(brokerAddr, group, out SubscriptionGroupConfig single);
                Check("getSubscriptionGroupConfig (single query)", ok, "found=" + (ok ? "1" : "0"));
                if (ok)
                {
                    Check("subscription group retryMaxTimes round trip", single.RetryMaxTimes == 5,
                        "got " + single.RetryMaxTimes.ToString(CultureInfo.InvariantCulture));
                }

                SubscriptionGroupWrapper wrapper = admin.GetAllSubscriptionGroup(brokerAddr);
                Check("getAllSubscriptionGroup (paged)", wrapper.SubscriptionGroupTable.Count > 0,
                    "groups=" + wrapper.SubscriptionGroupTable.Count.ToString(CultureInfo.InvariantCulture));
                Check("paged result contains the new subscription group", wrapper.SubscriptionGroupTable.ContainsKey(group),
                    "count=" + wrapper.SubscriptionGroupTable.Count.ToString(CultureInfo.InvariantCulture));

                bool exOk = admin.ExamineSubscriptionGroupConfig(brokerAddr, group, out SubscriptionGroupConfig ex);
                Check("examineSubscriptionGroupConfig", exOk, "group=" + ex.GroupName);
            }
            catch (Exception e)
            {
                Check("subscription group management chain", false, e.Message);
            }
        }

        // ---------- 6. Produce + stats + query ----------
        var sent = new List<(byte[] Body, string MsgId, string OffsetMsgId)>();
        MessageQueue firstMq = new();
        {
            var prod = new DefaultMQProducer("AdminLiveDotnetProducer_" + stamp)
            {
                NamesrvAddr = _gNamesrv,
                SendMsgTimeout = 5000,
            };
            prod.Start();
            for (int i = 0; i < nMsg; ++i)
            {
                string payload = "admin-live-" + i.ToString(CultureInfo.InvariantCulture);
                try
                {
                    SendResult sr = prod.Send(new Message(topic, Str2Bytes(payload)));
                    if (sr.SendStatus == SendStatus.SendOk)
                    {
                        sent.Add((Str2Bytes(payload), sr.MsgId, sr.OffsetMsgId));
                        if (firstMq.Topic.Length == 0) firstMq = sr.MessageQueue;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("send " + i.ToString(CultureInfo.InvariantCulture) + " error: " + e.Message);
                }
            }

            Check("sync send " + nMsg.ToString(CultureInfo.InvariantCulture) + " messages",
                sent.Count == nMsg, "ok=" + sent.Count.ToString(CultureInfo.InvariantCulture)
                + "/" + nMsg.ToString(CultureInfo.InvariantCulture));
            prod.Shutdown();
        }

        if (sent.Count == 0)
        {
            admin.Shutdown();
            return Report();
        }

        Thread.Sleep(2000);

        long maxOff = 0;
        long minOff = 0;
        MessageQueue mq = firstMq.Topic.Length == 0 ? new MessageQueue(topic, "broker-a", 0) : firstMq;
        try
        {
            TopicStatsTable stats = admin.ExamineTopicStats(topic);
            Check("examineTopicStats", stats.OffsetTable.Count > 0,
                "queues=" + stats.OffsetTable.Count.ToString(CultureInfo.InvariantCulture));
            Check("TopicStatsTable maxOffset sum >= sent count", stats.TotalMaxOffset() >= nMsg,
                "maxOffsetSum=" + stats.TotalMaxOffset().ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("examineTopicStats", false, e.Message);
        }

        try
        {
            ConsumeStats cs = admin.ExamineConsumeStats(brokerAddr, group, topic);
            Check("examineConsumeStats", true,
                "queues=" + cs.OffsetTable.Count.ToString(CultureInfo.InvariantCulture)
                + " lag=" + cs.TotalLag().ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("examineConsumeStats", false, e.Message);
        }

        try
        {
            QueryConsumeQueueResponseBody q = admin.QueryConsumeQueue(brokerAddr, topic, 0, 0, 10, group);
            Check("queryConsumeQueue", true,
                "min=" + q.MinQueueIndex.ToString(CultureInfo.InvariantCulture)
                + " max=" + q.MaxQueueIndex.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("queryConsumeQueue", false, e.Message);
        }

        // queryMessage: only assert "request reachable, normal reply, correct return type".
        // Do not assert "must find it": msgId is a uniqKey and the broker-side uniqKey inverted index is only
        // supported by the RocksDB index implementation; with this machine's default file index and no KEYS set,
        // finding nothing is a **broker config difference**, not a client bug.
        {
            string msgId0 = sent[0].MsgId;
            try
            {
                List<MessageExt> byKey = admin.QueryMessage(topic, msgId0, 32, 0, UtilAll.CurrentTimeMillis() + 60000);
                List<MessageExt> byKey2 = admin.QueryMessageByKey(topic, msgId0, 32);
                bool uniqFound = admin.QueryMessageByUniqKey(topic, msgId0, out _);
                Check("queryMessage request reachable with normal reply", true,
                    "key=" + byKey.Count.ToString(CultureInfo.InvariantCulture)
                    + " normal=" + byKey2.Count.ToString(CultureInfo.InvariantCulture)
                    + " uniq=" + (uniqFound ? "1" : "0"));
                Skip("queryMessage hit result (needs broker RocksDB/KEYS index)",
                    "this machine uses the default file index and the message has no KEYS; an empty uniqKey query result is expected");
            }
            catch (Exception e)
            {
                Check("queryMessage request reachable with normal reply", false, e.Message);
            }
        }

        // viewMessage(byOffsetMsgId): only a broker-assigned offset msgId can decode the commitLog offset
        try
        {
            MessageExt vm = admin.ViewMessage(topic, sent[0].OffsetMsgId);
            string bodyStr = Bytes2Str(vm.Body);
            string head = bodyStr.Length > 32 ? bodyStr.Substring(0, 32) : bodyStr;
            Check("viewMessage(byOffsetMsgId)", true,
                "topic=" + vm.Topic + " offset=" + vm.QueueOffset.ToString(CultureInfo.InvariantCulture)
                + " body=" + head);
            bool bodyEq = vm.Body.Length == sent[0].Body.Length && vm.Body.AsSpan().SequenceEqual(sent[0].Body.AsSpan());
            Check("viewMessage body matches what was sent", bodyEq,
                "expected " + Bytes2Str(sent[0].Body) + " got " + Bytes2Str(vm.Body));
        }
        catch (Exception e)
        {
            Check("viewMessage(byOffsetMsgId)", false, e.Message);
        }

        // viewMessage(uniqKey): the 5.x msgId cannot decode an offset; must fall back to uniqKey with a clean exception,
        // instead of connecting with a garbage port (previously showed "connect failed to 192.168.0.105:371196628")
        try
        {
            admin.ViewMessage(topic, sent[0].MsgId);
            Check("viewMessage(uniqKey) falls back with a clean exception", false, "no exception was thrown");
        }
        catch (MQClientException e)
        {
            Check("viewMessage(uniqKey) falls back with a clean exception", true, "code="
                + e.ResponseCode.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("viewMessage(uniqKey) falls back with a clean exception", false,
                e.GetType().Name + ": " + e.Message);
        }

        // ---------- 7. Offset read-only queries ----------
        try
        {
            maxOff = admin.MaxOffset(mq);
            minOff = admin.MinOffset(mq);
            Check("maxOffset/minOffset", maxOff >= minOff,
                "min=" + minOff.ToString(CultureInfo.InvariantCulture) + " max=" + maxOff.ToString(CultureInfo.InvariantCulture));
            Check("searchOffset(now) >= minOffset", admin.SearchOffset(mq, UtilAll.CurrentTimeMillis()) >= minOff, "ok");
            Check("earliestMsgStoreTime > 0", admin.EarliestMsgStoreTime(mq) > 0, "ok");
            bool offOk = admin.ExamineConsumerOffset(group, mq, out long off);
            Check("examineConsumerOffset callable", true, "offset=" + off.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("Offset read-only queries", false, e.Message);
        }

        string bndTopic = "AdminBoundaryTopicDotnet_" + stamp;
        try
        {
            admin.CreateTopic(MixAll.DefaultTopic, bndTopic, 1);
            Check("createTopic(" + bndTopic + ", 1 queue)", true, "queueNum=1");
        }
        catch (Exception e)
        {
            Check("createTopic(" + bndTopic + ", 1 queue)", false, e.Message);
        }

        try
        {
            var bprod = new DefaultMQProducer("AdminLiveDotnetBoundaryProducer_" + stamp)
            {
                NamesrvAddr = _gNamesrv,
                SendMsgTimeout = 5000,
            };
            bprod.Start();
            for (int i = 0; i < 3; ++i)
            {
                bprod.Send(new Message(bndTopic,
                    Str2Bytes("boundary-" + i.ToString(CultureInfo.InvariantCulture))));
            }

            Check("boundary topic sent 3 messages", true, "ok");
            bprod.Shutdown();
        }
        catch (Exception e)
        {
            Check("boundary topic sent 3 messages", false, e.Message);
        }

        Thread.Sleep(2000);
        try
        {
            var bmq = new MessageQueue(bndTopic, firstMq.BrokerName, 0);
            long future = UtilAll.CurrentTimeMillis() + 600000;
            long bMax = admin.MaxOffset(bmq);
            long lo = admin.SearchLowerBoundaryOffset(bmq, future);
            long up = admin.SearchUpperBoundaryOffset(bmq, future);
            long loPast = admin.SearchLowerBoundaryOffset(bmq, 1);
            long upPast = admin.SearchUpperBoundaryOffset(bmq, 1);
            Check("boundary topic maxOffset == 3", bMax == 3,
                "max=" + bMax.ToString(CultureInfo.InvariantCulture));
            Check("LOWER boundary = maxOffset (the next offset after the queue tail)", lo == bMax,
                "lower=" + lo.ToString(CultureInfo.InvariantCulture)
                + " maxOffset=" + bMax.ToString(CultureInfo.InvariantCulture));
            Check("UPPER boundary = maxOffset-1 (the last message's own offset)", up == bMax - 1,
                "upper=" + up.ToString(CultureInfo.InvariantCulture)
                + " maxOffset-1=" + (bMax - 1).ToString(CultureInfo.InvariantCulture));
            Check("the two boundaries really differ (proves boundaryType works)", lo != up,
                "lower=" + lo.ToString(CultureInfo.InvariantCulture)
                + " upper=" + up.ToString(CultureInfo.InvariantCulture));
            Check("when the timestamp precedes all messages both LOWER/UPPER collapse to minOffset",
                loPast == 0 && upPast == 0,
                "lower=" + loPast.ToString(CultureInfo.InvariantCulture)
                + " upper=" + upPast.ToString(CultureInfo.InvariantCulture));
            Check("searchOffset default boundary matches explicit LOWER",
                admin.SearchOffset(bmq, future) == lo,
                "lower=" + lo.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("searchOffset boundaryType", false, e.Message);
        }

        // ---------- 8. Consume + sendMessageBack redelivery ----------
        // Order matters: consume FIRST, then redeliver. If the offset is reset to max beforehand the consumer can
        // never pull the historical messages and the sendMessageBack path never gets to run.
        // This group is brand new with no committed offsets, so FIRST_OFFSET really starts from the minimum offset.
        string retryTopic = MixAll.GetRetryTopic(group);
        bool backsent = false;
        {
            var listener = new CollectingListener();
            var cons = new DefaultMQPushConsumer(group)
            {
                ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
                PullSuspendTimeoutMillis = 3000,
            };
            cons.SetNamesrvAddr(_gNamesrv);
            cons.Subscribe(topic, "*");
            cons.SetMessageListener(listener);
            cons.Start();

            for (int i = 0; i < 60 && listener.Count() < 1; ++i)
            {
                Thread.Sleep(500);
            }

            Check("consumed messages (prerequisite for sendMessageBack)", listener.Count() >= 1,
                "consumed=" + listener.Count().ToString(CultureInfo.InvariantCulture));

            if (listener.Count() >= 1)
            {
                MessageExt first = listener.First();
                try
                {
                    bool ok = cons.SendMessageBack(first, 0);
                    backsent = true;
                    Check("consumer.sendMessageBack (redelivered to " + retryTopic + ")", ok,
                        "origin_topic=" + first.Topic + " commitLogOffset=" + first.CommitLogOffset.ToString(CultureInfo.InvariantCulture));
                }
                catch (Exception e)
                {
                    Check("consumer.sendMessageBack", false, e.Message);
                }
            }

            cons.Shutdown();
        }

        // Verifying it shows up in %RETRY% requires polling, not an immediate assertion.
        // The broker's SendMessageProcessor.consumerSendMsgBack rewrites delayLevel == 0 into
        // `3 + reconsumeTimes` (default 3 -> 10s); the message first enters SCHEDULE_TOPIC_XXXX,
        // and only reaches %RETRY%<group> when the delay expires.
        if (backsent)
        {
            long retryMax = -1;
            int waited = 0;
            for (int i = 0; i < 30; ++i)
            {
                try
                {
                    TopicStatsTable st = admin.ExamineTopicStats(retryTopic);
                    retryMax = st.TotalMaxOffset();
                    if (retryMax > 0) break;
                }
                catch (Exception)
                {
                    retryMax = -1;
                }

                Thread.Sleep(1000);
                ++waited;
            }

            Check("sendMessageBack landed in " + retryTopic + " (maxOffsetSum>0)", retryMax > 0,
                "maxOffsetSum=" + retryMax.ToString(CultureInfo.InvariantCulture)
                + " (polled " + waited.ToString(CultureInfo.InvariantCulture)
                + "s; the broker rewrites delayLevel=0 into 3, visible in about 10s)");
        }

        {
            int rows = 0;
            bool groupWithOffsets = false;
            int waited = 0;
            for (int i = 0; i < 20 && !groupWithOffsets; ++i)
            {
                try
                {
                    ConsumeStatsList sl = admin.FetchConsumeStatsInBroker(brokerAddr, false);
                    rows = sl.StatsList.Size();
                    for (int r = 0; r < rows && !groupWithOffsets; ++r)
                    {
                        JsonValue stats = sl.StatsList.At(r).Get(group);
                        for (int s = 0; s < stats.Size(); ++s)
                        {
                            if (stats.At(s).Get("offsetTable").ObjectItems().Count > 0)
                            {
                                groupWithOffsets = true;
                                break;
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    rows = 0;
                }

                if (!groupWithOffsets)
                {
                    Thread.Sleep(1000);
                    ++waited;
                }
            }

            Check("fetchConsumeStatsInBroker one row per subscription group", rows > 0,
                "groups=" + rows.ToString(CultureInfo.InvariantCulture));
            Check("fetchConsumeStatsInBroker group stats carry an offsetTable", groupWithOffsets,
                "polled " + waited.ToString(CultureInfo.InvariantCulture) + "s");
        }

        // ---------- 9. Real broker-side offset reset (last, so it does not disturb the consumption above) ----------
        {
            long ts = UtilAll.CurrentTimeMillis() + 60000; // future time -> offsets should be pushed to max
            try
            {
                SortedDictionary<MessageQueue, long> reset = admin.ResetOffsetByTimestamp(topic, group, ts, true);
                Check("resetOffsetByTimestamp(INVOKE_BROKER_TO_RESET_OFFSET)", reset.Count > 0,
                    "queues=" + reset.Count.ToString(CultureInfo.InvariantCulture));
                Thread.Sleep(1000);
                bool foundOff = admin.ExamineConsumerOffset(group, mq, out long off);
                long maxNow = admin.MaxOffset(mq);
                Check("after reset the consumer offset was pushed to maxOffset", foundOff && off >= maxNow - 1,
                    "consumerOffset=" + off.ToString(CultureInfo.InvariantCulture)
                    + " maxOffset=" + maxNow.ToString(CultureInfo.InvariantCulture));
            }
            catch (Exception e)
            {
                Check("resetOffsetByTimestamp", false, e.Message);
            }
        }

        {
            int queueId = mq.QueueId;
            long minReset;
            try
            {
                minReset = admin.MinOffset(mq);
            }
            catch (Exception e)
            {
                Check("minOffset (reset precondition)", false, e.Message);
                minReset = -1;
            }
            if (minReset >= 0)
            {
                try
                {
                    SortedDictionary<MessageQueue, long> table =
                        admin.ResetOffsetByQueueId(brokerAddr, group, topic, queueId, minReset);
                    Check("resetOffsetByQueueId (back to minOffset)",
                        table.Count == 0 || table.Keys.First().QueueId == queueId,
                        "returnedTable=" + table.Count.ToString(CultureInfo.InvariantCulture)
                        + " (the protocol semantics are void, may be empty)");
                    bool found = admin.ExamineConsumerOffset(group, mq, out long off);
                    Check("offset == target offset after resetOffsetByQueueId", found && off == minReset,
                        "consumerOffset=" + off.ToString(CultureInfo.InvariantCulture)
                        + " target=" + minReset.ToString(CultureInfo.InvariantCulture));
                }
                catch (Exception e)
                {
                    Check("resetOffsetByQueueId (back to minOffset)", false, e.Message);
                }

                try
                {
                    var pull = new DefaultMQPullConsumer(group);
                    pull.SetNamesrvAddr(_gNamesrv);
                    pull.Start();
                    PullResult first = pull.Pull(mq, "*", minReset, 16, 5000);
                    Check("the first pull after reset is short-circuited by the broker into PULL_OFFSET_MOVED",
                        first.Status == PullStatus.OffsetIllegal && first.NextBeginOffset == minReset,
                        "status=" + first.Status + " nextBeginOffset="
                        + first.NextBeginOffset.ToString(CultureInfo.InvariantCulture));
                    PullResult second = pull.Pull(mq, "*", first.NextBeginOffset, 16, 5000);
                    Check("historical messages are pullable again after the reset", second.MsgFoundList.Count > 0,
                        "status=" + second.Status + " found="
                        + second.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));
                    pull.Shutdown();
                }
                catch (Exception e)
                {
                    Check("re-pull after reset", false, e.Message);
                }

                try
                {
                    long badTarget = admin.MaxOffset(mq) + 100;
                    bool rejected = false;
                    string remark = "";
                    try
                    {
                        admin.ResetOffsetByQueueId(brokerAddr, group, topic, queueId, badTarget);
                    }
                    catch (Exception e)
                    {
                        rejected = true;
                        remark = e.Message;
                    }
                    Check("resetOffsetByQueueId out-of-range target rejected by the broker", rejected,
                        "badTarget=" + badTarget.ToString(CultureInfo.InvariantCulture)
                        + " " + remark.Substring(0, Math.Min(120, remark.Length)));
                    bool found = admin.ExamineConsumerOffset(group, mq, out long off);
                    Check("the out-of-range reset stays at the first written illegal offset (the two RPCs are not atomic)",
                        found && off == badTarget,
                        "consumerOffset=" + off.ToString(CultureInfo.InvariantCulture));
                }
                catch (Exception e)
                {
                    Check("resetOffsetByQueueId out-of-range target rejected by the broker", false, e.Message);
                }
            }

            // 343 reads the broker's offsetTable (whichTopicByConsumer); the group only shows up after committing offsets.
            try
            {
                TopicList byBroker = admin.QueryTopicsByConsumerToBroker(brokerAddr, group);
                Check("queryTopicsByConsumerToBroker reads the topics this group consumed",
                    byBroker.Topics.Contains(topic),
                    "topics=" + byBroker.Topics.Count.ToString(CultureInfo.InvariantCulture));
                TopicList byGroup = admin.QueryTopicsByConsumer(group);
                Check("queryTopicsByConsumer(group) fans out over the %RETRY% route and merges",
                    byGroup.Topics.Contains(topic),
                    "topics=" + byGroup.Topics.Count.ToString(CultureInfo.InvariantCulture));
            }
            catch (Exception e)
            {
                Check("queryTopicsByConsumer", false, e.Message);
            }
        }

        // ---------- 10. Cleanup ----------
        try
        {
            admin.DeleteSubscriptionGroup(brokerAddr, group, true);
            Check("deleteSubscriptionGroup", true, "OK");
        }
        catch (Exception e)
        {
            Check("deleteSubscriptionGroup", false, e.Message);
        }

        try
        {
            admin.DeleteTopic(topic);
            Check("deleteTopic", true, "OK");
            Thread.Sleep(1000);
            TopicList after = admin.FetchAllTopicList();
            Check("topic gone after deleteTopic", !after.Contains(topic), topic);
        }
        catch (Exception e)
        {
            Check("deleteTopic", false, e.Message);
        }

        try
        {
            admin.DeleteTopic(bndTopic);
            Check("deleteTopic(boundary)", true, "OK");
        }
        catch (Exception e)
        {
            Check("deleteTopic(boundary)", false, e.Message);
        }

        admin.Shutdown();
        return Report();
    }
}
