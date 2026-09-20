// C# 管理客户端（DefaultMQAdminExt）的**真实集群**联调（对应 cpp/examples/admin_live.cpp）。
//
// 全部打真实 nameServer + broker，无 mock。覆盖：
//   1. 集群探活 / fetchBrokerClusterInfo / getClusterList
//   2. createTopic（队列数校验）→ fetchAllTopicList → examineTopicRoute
//   3. examineTopicConfig / getAllTopicConfig
//   4. getBrokerConfig（**properties 文本**，验证没被当 KVTable JSON 解析）
//   5. updateBrokerConfig → 回读确认生效（可逆，测完还原）
//   6. NameServer KV：createAndUpdateKvConfig → getKvConfig → getKvListByNamespace → delete
//   7. 订阅组：create/update → 单查 → 分页全量 → examine → delete
//   8. 生产 N 条 → examineTopicStats / examineConsumeStats / queryConsumeQueue /
//      queryMessage（可达性）/ viewMessage(msgId)
//   9. maxOffset / minOffset / searchOffset / earliestMsgStoreTime / examineConsumerOffset
//  10. sendMessageBack：消费 1 条后重投 → 轮询 %RETRY%<group> 出现该消息
//  11. resetOffsetByTimestamp（真实 INVOKE_BROKER_TO_RESET_OFFSET，language=CPP）
//  12. 清理：deleteSubscriptionGroup / deleteTopic
//
// 本程序自身不启动集群；调用方需先启动 nameServer(9876) + broker(10911) 且
// autoCreateTopicEnable=true。用法（由 Program 以 "admin-live [namesrv]" 形式调用）。
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>真实集群管理端全链路联调（与 cpp/examples/admin_live.cpp 对齐）。</summary>
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

    // 记录"依赖 broker 侧配置差异、本机不成立"的项，不计入失败。
    private static void Skip(string name, string detail)
    {
        ++_gSkip;
        Console.WriteLine("[SKIP] " + name + "  " + detail);
    }

    private static string Bytes2Str(byte[] b) => Encoding.UTF8.GetString(b);

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    // ---------------- 收集第一批消费到的消息（sendMessageBack 需要原始 MessageExt）----------------

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
        Console.WriteLine("\n================ 汇总 ================");
        foreach ((string Name, bool Ok) r in Results)
        {
            Console.WriteLine("  [" + (r.Ok ? "PASS" : "FAIL") + "] " + r.Name);
        }

        Console.WriteLine("======================================");
        Console.WriteLine("总计 " + (_gPass + _gFail).ToString(CultureInfo.InvariantCulture)
                          + " 项，失败 " + _gFail.ToString(CultureInfo.InvariantCulture)
                          + " 项，跳过 " + _gSkip.ToString(CultureInfo.InvariantCulture) + " 项");
        return _gFail == 0 ? 0 : 1;
    }

    // ---------------- 入口 ----------------

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

        // ---------- 1. 集群探活（broker 注册竞态：端口开 != 已注册）----------
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
            Check("集群探活", false, "nameServer 无 broker 注册 last_err=" + lastErr);
            admin.Shutdown();
            return Report();
        }

        List<string> addrs = cluster.GetBrokerAddrs();
        Check("fetchBrokerClusterInfo", true,
            "brokers=" + cluster.BrokerAddrTable.Count.ToString(CultureInfo.InvariantCulture)
            + " addrs=" + addrs.Count.ToString(CultureInfo.InvariantCulture));
        string brokerAddr = addrs[0];

        // ---------- 2. Topic 管理 ----------
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
            Check("新 topic 出现在 topicList", all.Contains(topic), topic);
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

            Check("路由 readQueueNums==4", allFour, "ok");
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
            Check("TopicConfig 队列数与创建一致", cfg.ReadQueueNums == 4 && cfg.WriteQueueNums == 4,
                "read=" + cfg.ReadQueueNums.ToString(CultureInfo.InvariantCulture)
                + " write=" + cfg.WriteQueueNums.ToString(CultureInfo.InvariantCulture));
            Check("TopicConfig 默认 perm=6", cfg.Perm == 6, "perm=" + cfg.Perm.ToString(CultureInfo.InvariantCulture));
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

        // ---------- 3. Broker 配置：properties 文本（历史 bug 点）----------
        PropertyMap brokerCfg = new();
        try
        {
            brokerCfg = admin.GetBrokerConfig(brokerAddr);
            Check("getBrokerConfig(properties 文本)", brokerCfg.Count > 0,
                "keys=" + brokerCfg.Count.ToString(CultureInfo.InvariantCulture));
            bool hasBn = brokerCfg.TryGetValue("brokerName", out var bn) && (bn?.Length ?? 0) > 0;
            Check("getBrokerConfig 解析出 brokerName", hasBn, "brokerName=" + (hasBn ? bn : string.Empty));
        }
        catch (Exception e)
        {
            Check("getBrokerConfig(properties 文本)", false, e.Message);
        }

        if (brokerCfg.Count > 0)
        {
            // 可逆修改：写一个无害配置再还原
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
                Check("updateBrokerConfig 生效", ok, "期望 " + probe + " 实际 " + v);
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

        // ---------- 4. NameServer KV 配置 ----------
        try
        {
            admin.CreateAndUpdateKvConfig(kvNs, "k1", "v1");
            Check("createAndUpdateKvConfig", true, "OK");
            bool found = admin.GetKvConfig(kvNs, "k1", out string value);
            Check("getKVConfig", found, "value=" + value);
            Check("KV 值往返一致", found && value == "v1", "期望 v1 实际 " + value);
            KvTable t = admin.GetKvListByNamespace(kvNs);
            Check("getKVListByNamespace 含 k1", t.Table.ContainsKey("k1"),
                "tableSize=" + t.Table.Count.ToString(CultureInfo.InvariantCulture));
            admin.DeleteKvConfig(kvNs, "k1");
            Check("deleteKVConfig", true, "OK");
            bool stillThere = admin.GetKvConfig(kvNs, "k1", out _);
            Check("KV 删除后不再存在", !stillThere, "found=" + (stillThere ? "1" : "0"));
        }
        catch (Exception e)
        {
            Check("NameServer KV 链路", false, e.Message);
        }

        // ---------- 5. 订阅组管理 ----------
        {
            var sgc = new SubscriptionGroupConfig(group) { ConsumeEnable = true, RetryMaxTimes = 5 };
            try
            {
                admin.CreateAndUpdateSubscriptionGroupConfig(brokerAddr, sgc);
                Check("createAndUpdateSubscriptionGroupConfig", true, "group=" + group);

                bool ok = admin.GetSubscriptionGroupConfig(brokerAddr, group, out SubscriptionGroupConfig single);
                Check("getSubscriptionGroupConfig(单查)", ok, "found=" + (ok ? "1" : "0"));
                if (ok)
                {
                    Check("订阅组 retryMaxTimes 往返一致", single.RetryMaxTimes == 5,
                        "实际 " + single.RetryMaxTimes.ToString(CultureInfo.InvariantCulture));
                }

                SubscriptionGroupWrapper wrapper = admin.GetAllSubscriptionGroup(brokerAddr);
                Check("getAllSubscriptionGroup(分页)", wrapper.SubscriptionGroupTable.Count > 0,
                    "groups=" + wrapper.SubscriptionGroupTable.Count.ToString(CultureInfo.InvariantCulture));
                Check("分页结果含新订阅组", wrapper.SubscriptionGroupTable.ContainsKey(group),
                    "count=" + wrapper.SubscriptionGroupTable.Count.ToString(CultureInfo.InvariantCulture));

                bool exOk = admin.ExamineSubscriptionGroupConfig(brokerAddr, group, out SubscriptionGroupConfig ex);
                Check("examineSubscriptionGroupConfig", exOk, "group=" + ex.GroupName);
            }
            catch (Exception e)
            {
                Check("订阅组管理链路", false, e.Message);
            }
        }

        // ---------- 6. 生产 + 统计 + 查询 ----------
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

            Check("同步发送 " + nMsg.ToString(CultureInfo.InvariantCulture) + " 条",
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
            Check("TopicStatsTable maxOffset 总和 >= 发送数", stats.TotalMaxOffset() >= nMsg,
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

        // queryMessage：只断言"请求可达、正常应答、返回类型正确"。
        // 不断言"一定查得到"：msgId 属于 uniqKey，broker 侧 uniqKey 倒排索引只有
        // RocksDB 索引实现才支持；本机默认文件索引 + 消息未设 KEYS，查不到是
        // **broker 配置差异**，不是客户端 bug。
        {
            string msgId0 = sent[0].MsgId;
            try
            {
                List<MessageExt> byKey = admin.QueryMessage(topic, msgId0, 32, 0, UtilAll.CurrentTimeMillis() + 60000);
                List<MessageExt> byKey2 = admin.QueryMessageByKey(topic, msgId0, 32);
                bool uniqFound = admin.QueryMessageByUniqKey(topic, msgId0, out _);
                Check("queryMessage 请求可达且正常应答", true,
                    "key=" + byKey.Count.ToString(CultureInfo.InvariantCulture)
                    + " normal=" + byKey2.Count.ToString(CultureInfo.InvariantCulture)
                    + " uniq=" + (uniqFound ? "1" : "0"));
                Skip("queryMessage 命中结果（需 broker 开 RocksDB/KEYS 索引）",
                    "本机为默认文件索引且消息未设 KEYS，uniqKey 查询返回空属预期");
            }
            catch (Exception e)
            {
                Check("queryMessage 请求可达且正常应答", false, e.Message);
            }
        }

        // viewMessage(byOffsetMsgId)：只有 broker 赋值的 offset msgId 能解出 commitLog 偏移
        try
        {
            MessageExt vm = admin.ViewMessage(topic, sent[0].OffsetMsgId);
            string bodyStr = Bytes2Str(vm.Body);
            string head = bodyStr.Length > 32 ? bodyStr.Substring(0, 32) : bodyStr;
            Check("viewMessage(byOffsetMsgId)", true,
                "topic=" + vm.Topic + " offset=" + vm.QueueOffset.ToString(CultureInfo.InvariantCulture)
                + " body=" + head);
            bool bodyEq = vm.Body.Length == sent[0].Body.Length && vm.Body.AsSpan().SequenceEqual(sent[0].Body.AsSpan());
            Check("viewMessage body 与发送一致", bodyEq,
                "期望 " + Bytes2Str(sent[0].Body) + " 实际 " + Bytes2Str(vm.Body));
        }
        catch (Exception e)
        {
            Check("viewMessage(byOffsetMsgId)", false, e.Message);
        }

        // viewMessage(uniqKey)：5.x 的 msgId 解不出偏移，必须走 uniqKey 兜底并给出干净异常，
        // 而不是拿垃圾端口去 connect（曾表现为 "connect failed to 192.168.0.105:371196628"）
        try
        {
            admin.ViewMessage(topic, sent[0].MsgId);
            Check("viewMessage(uniqKey) 走兜底并给出干净异常", false, "没有抛异常");
        }
        catch (MQClientException e)
        {
            Check("viewMessage(uniqKey) 走兜底并给出干净异常", true, "code="
                + e.ResponseCode.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("viewMessage(uniqKey) 走兜底并给出干净异常", false,
                e.GetType().Name + ": " + e.Message);
        }

        // ---------- 7. Offset 只读查询 ----------
        try
        {
            maxOff = admin.MaxOffset(mq);
            minOff = admin.MinOffset(mq);
            Check("maxOffset/minOffset", maxOff >= minOff,
                "min=" + minOff.ToString(CultureInfo.InvariantCulture) + " max=" + maxOff.ToString(CultureInfo.InvariantCulture));
            Check("searchOffset(now) >= minOffset", admin.SearchOffset(mq, UtilAll.CurrentTimeMillis()) >= minOff, "ok");
            Check("earliestMsgStoreTime > 0", admin.EarliestMsgStoreTime(mq) > 0, "ok");
            bool offOk = admin.ExamineConsumerOffset(group, mq, out long off);
            Check("examineConsumerOffset 可调用", true, "offset=" + off.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("Offset 只读查询", false, e.Message);
        }

        // ---------- 8. 消费 + sendMessageBack 重投 ----------
        // 顺序很关键：必须**先消费**再重投。若先把位点重置到 max，消费者就再也拉不到
        // 历史消息，sendMessageBack 路径根本没机会执行。
        // 本组是新建的、无已提交位点，故 FIRST_OFFSET 会真正从最小位点开始读。
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

            Check("消费到消息（sendMessageBack 前置）", listener.Count() >= 1,
                "consumed=" + listener.Count().ToString(CultureInfo.InvariantCulture));

            if (listener.Count() >= 1)
            {
                MessageExt first = listener.First();
                // 消费者仍在线时重投（对齐 Java：在消费线程内调用 sendMessageBack）
                try
                {
                    bool ok = cons.SendMessageBack(first, 0);
                    backsent = true;
                    Check("consumer.sendMessageBack(重投到 " + retryTopic + ")", ok,
                        "origin_topic=" + first.Topic + " commitLogOffset=" + first.CommitLogOffset.ToString(CultureInfo.InvariantCulture));
                }
                catch (Exception e)
                {
                    Check("consumer.sendMessageBack", false, e.Message);
                }
            }

            cons.Shutdown();
        }

        // 校验 %RETRY% 里出现该消息：必须轮询而不是立即断言。
        // broker 的 SendMessageProcessor.consumerSendMsgBack 在 delayLevel == 0 时会改写
        // 成 `3 + reconsumeTimes`（默认 3 -> 10s），消息先进 SCHEDULE_TOPIC_XXXX，
        // 到点才投递到 %RETRY%<group>。
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

            Check("sendMessageBack 落到 " + retryTopic + "（maxOffsetSum>0）", retryMax > 0,
                "maxOffsetSum=" + retryMax.ToString(CultureInfo.InvariantCulture)
                + "（轮询 " + waited.ToString(CultureInfo.InvariantCulture)
                + "s；broker 把 delayLevel=0 改写为 3，约 10s 后可见）");
        }

        // ---------- 8.5 FetchConsumeStatsInBroker（341）：按订阅组出行，本组带 offsetTable ----------
        // Java `AdminBrokerProcessor#fetchAllConsumeStatsInBroker` 对
        // `subscriptionGroupTable.keySet()` 的每个组建一行 {group: [ConsumeStats]}，内层 topic
        // 才来自位点表 `whichTopicByConsumer`（所以要等消费者刷过位点）。
        // ⚠ 响应 JSON 键是 Java 字段名 consumeStatsList，不是 statsList：写错键名时真机响应
        // 会解析成空集合，看着像「这个 broker 没有积压」—— 这条断言就是回归护栏。
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

            Check("fetchConsumeStatsInBroker 按订阅组出行", rows > 0,
                "groups=" + rows.ToString(CultureInfo.InvariantCulture));
            Check("fetchConsumeStatsInBroker 本组统计带 offsetTable", groupWithOffsets,
                "轮询 " + waited.ToString(CultureInfo.InvariantCulture) + "s");
        }

        // ---------- 9. 真实 broker 端位点重置（放最后，避免干扰上面的消费）----------
        {
            long ts = UtilAll.CurrentTimeMillis() + 60000; // 未来时间 -> 位点应被推到 max
            try
            {
                SortedDictionary<MessageQueue, long> reset = admin.ResetOffsetByTimestamp(topic, group, ts, true);
                Check("resetOffsetByTimestamp(INVOKE_BROKER_TO_RESET_OFFSET)", reset.Count > 0,
                    "queues=" + reset.Count.ToString(CultureInfo.InvariantCulture));
                Thread.Sleep(1000);
                bool foundOff = admin.ExamineConsumerOffset(group, mq, out long off);
                long maxNow = admin.MaxOffset(mq);
                Check("reset 后消费者位点被推到 maxOffset", foundOff && off >= maxNow - 1,
                    "consumerOffset=" + off.ToString(CultureInfo.InvariantCulture)
                    + " maxOffset=" + maxNow.ToString(CultureInfo.InvariantCulture));
            }
            catch (Exception e)
            {
                Check("resetOffsetByTimestamp", false, e.Message);
            }
        }

        // ---------- 10. 清理 ----------
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
            Check("deleteTopic 后 topic 消失", !after.Contains(topic), topic);
        }
        catch (Exception e)
        {
            Check("deleteTopic", false, e.Message);
        }

        admin.Shutdown();
        return Report();
    }
}
