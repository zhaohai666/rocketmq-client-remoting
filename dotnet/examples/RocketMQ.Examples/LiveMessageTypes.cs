// C# 客户端的**真实集群**消息类型联调（对应 cpp/examples/live_message_types.cpp）。
//
// 覆盖 8 类消息能力，全部打真实 nameServer + broker：
//   1. 异步发送（sendAsync + SendCallback）
//   2. 顺序消息（sendBySelector 同 key 落同队列 + 顺序消费保序）
//   3. 带 Tag 消息 + 服务端 Tag 过滤
//   4. 用户属性透传
//   5. 延迟消息（setDelayTimeLevel 并校验 store_ts - born_ts >= 3000ms）
//   6. 带 Key 消息 + 按 Key 服务端查询（QUERY_MESSAGE）
//   7. 事务消息（两阶段的提交路径；完整链路见 RunTransaction）+ 落库可消费
//   8. 批量消息（SendBatch → SEND_BATCH_MESSAGE(320)，broker 按 N 条独立消息投递、offset 连续）
//   附：消费者心跳注册（HEART_BEAT）
//
// 本程序自身不启动集群；调用方需先启动 nameServer(9876) + broker(10911) 且
// autoCreateTopicEnable=true。用法（由 Program 以 "message-types [namesrv]" 形式调用）。
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>真实集群 7 类消息能力联调（与 cpp/examples/live_message_types.cpp 对齐）。</summary>
internal static class LiveMessageTypes
{
    private static string _gNamesrv = "127.0.0.1:9876";
    private static string _gPrefix = string.Empty;
    private static readonly List<(string Name, bool Ok)> Results = new();
    private static int _gPass;
    private static int _gFail;

    private static void Check(string name, bool ok, string detail = "")
    {
        Results.Add((name, ok));
        if (ok) ++_gPass;
        else ++_gFail;
        Console.WriteLine("[" + (ok ? "PASS" : "FAIL") + "] " + name + (detail.Length > 0 ? "  " + detail : string.Empty));
    }

    private static string Bytes2Str(byte[] b) => Encoding.UTF8.GetString(b);

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    // ---------------- 监听器 ----------------

    private sealed class CollectingListenerConcurrently : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<MessageExt> _msgs = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext ctx)
        {
            lock (_lk) _msgs.AddRange(msgs);
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<MessageExt> Snapshot()
        {
            lock (_lk) return new List<MessageExt>(_msgs);
        }
    }

    private sealed class CollectingListenerOrderly : IMessageListenerOrderly
    {
        private readonly object _lk = new();
        private readonly List<MessageExt> _msgs = new();

        public bool Orderly() => true;

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext ctx)
        {
            lock (_lk) _msgs.AddRange(msgs);
            return ConsumeOrderlyStatus.Success;
        }

        public List<MessageExt> Snapshot()
        {
            lock (_lk) return new List<MessageExt>(_msgs);
        }
    }

    private sealed class CollectingCallback : ISendCallback
    {
        private int _ok;
        private int _err;
        private readonly object _lk = new();
        private readonly List<SendResult> _results = new();
        private readonly List<string> _errors = new();

        public void OnSuccess(SendResult result)
        {
            Interlocked.Increment(ref _ok);
            lock (_lk) _results.Add(result);
        }

        public void OnException(string error)
        {
            Interlocked.Increment(ref _err);
            lock (_lk) _errors.Add(error);
        }

        public int Ok() => _ok;
        public int Err() => _err;

        public List<SendResult> Results()
        {
            lock (_lk) return new List<SendResult>(_results);
        }

        public List<string> Errors()
        {
            lock (_lk) return new List<string>(_errors);
        }
    }

    private sealed class CommitTxListener : ITransactionListener
    {
        public LocalTransactionState ExecuteLocalTransaction(Message msg, string arg) =>
            LocalTransactionState.CommitMessage;

        public LocalTransactionState CheckLocalTransaction(MessageExt msg) =>
            LocalTransactionState.CommitMessage;
    }

    /// <summary>本地事务回滚：broker 不应把半消息投递出来。</summary>
    private sealed class RollbackTxListener : ITransactionListener
    {
        public LocalTransactionState ExecuteLocalTransaction(Message msg, string arg) =>
            LocalTransactionState.RollbackMessage;

        public LocalTransactionState CheckLocalTransaction(MessageExt msg) =>
            LocalTransactionState.RollbackMessage;
    }

    /// <summary>
    /// 本地事务返回 Unknow，等 broker 回查时才判 Commit。
    /// CheckCalls 用于证明 **broker 确实回调过**（否则"最终收到"可能只是普通消息路径）。
    /// </summary>
    private sealed class UnknownThenCommitTxListener : ITransactionListener
    {
        private int _checkCalls;

        public int CheckCalls => Volatile.Read(ref _checkCalls);

        public LocalTransactionState ExecuteLocalTransaction(Message msg, string arg) =>
            LocalTransactionState.Unknow;

        public LocalTransactionState CheckLocalTransaction(MessageExt msg)
        {
            Interlocked.Increment(ref _checkCalls);
            return LocalTransactionState.CommitMessage;
        }
    }

    // ---------------- 消费辅助 ----------------

    // 活跃等待直至收到 expect 条（或最多 durationSec 秒），再关闭消费者，返回已收到的消息。
    private static List<MessageExt> RunConsumer(string topic, string subExpr, int durationSec,
        bool orderly, string groupSuffix, int expect = 0, int pullTimeout = 3000, int pullSuspend = 1000)
    {
        var conc = new CollectingListenerConcurrently();
        var ord = new CollectingListenerOrderly();
        IMessageListener listener = orderly ? ord : conc;

        var consumer = new DefaultMQPushConsumer(_gPrefix + "_" + groupSuffix)
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
            PullTimeoutMillis = pullTimeout,
            PullSuspendTimeoutMillis = pullSuspend,
        };
        consumer.SetNamesrvAddr(_gNamesrv);
        consumer.Subscribe(topic, subExpr);
        consumer.SetMessageListener(listener);
        consumer.Start();

        var deadline = DateTime.UtcNow.AddSeconds(durationSec);
        while (DateTime.UtcNow < deadline)
        {
            int got = orderly ? ord.Snapshot().Count : conc.Snapshot().Count;
            if (expect > 0 && got >= expect) break;
            Thread.Sleep(100);
        }

        List<MessageExt> msgs = orderly ? ord.Snapshot() : conc.Snapshot();
        consumer.Shutdown();
        return msgs;
    }

    // 等待 broker 在 nameServer 注册完成（避免端口刚开、注册未落地的竞态）
    private static List<string> WaitBroker(DefaultMQProducer prod)
    {
        var addrs = new List<string>();
        for (int i = 0; i < 40; ++i)
        {
            try
            {
                List<MessageQueue> mqs = prod.FetchPublishMessageQueues(MixAll.DefaultTopic);
                foreach (MessageQueue mq in mqs)
                {
                    string a = prod.Client().BrokerAddrOf(mq.BrokerName);
                    if (a.Length > 0 && !addrs.Contains(a)) addrs.Add(a);
                }

                if (addrs.Count > 0) return addrs;
            }
            catch (Exception)
            {
                // 路由还没就绪，继续等
            }

            Thread.Sleep(1000);
        }

        return addrs;
    }

    // ---------------- 入口 ----------------

    public static int Run(string[] args)
    {
        if (args.Length >= 1) _gNamesrv = args[0];
        long stamp = UtilAll.CurrentTimeSeconds();
        _gPrefix = "MTDotnet_" + stamp;

        Console.WriteLine("=== C# 客户端消息类型联调（真实集群）===");
        Console.WriteLine("namesrv = " + _gNamesrv + "  prefix = " + _gPrefix);

        var prod = new DefaultMQProducer(_gPrefix + "_producer")
        {
            NamesrvAddr = _gNamesrv,
            SendMsgTimeout = 5000,
        };
        try
        {
            prod.Start();
        }
        catch (Exception e)
        {
            Check("生产者启动", false, e.Message);
            return 1;
        }

        List<string> brokers = WaitBroker(prod);
        if (brokers.Count == 0)
        {
            Check("集群探活", false, "nameServer 无 broker 注册");
            prod.Shutdown();
            return 1;
        }

        Check("集群探活", true, "brokers=" + string.Join(",", brokers));

        // 预建本用例全部 topic：新 topic 只能等 broker 每 30s 一轮的
        // registerNameServerPeriod 才进路由，期间消费者的订阅是空转的。
        {
            var admin = new DefaultMQAdminExt();
            admin.SetNamesrvAddr(_gNamesrv);
            admin.SetTimeoutMillis(10000);
            admin.Start();
            try
            {
                foreach (string suffix in new[]
                         { "Async", "Order", "Tag", "Prop", "Delay", "Key", "Tx",
                           "TxRollback", "TxCheck", "Hb" })
                {
                    admin.CreateTopic(MixAll.DefaultTopic, _gPrefix + "_" + suffix, 4);
                }
                Thread.Sleep(1000);
            }
            catch (Exception e)
            {
                Console.WriteLine("  预建 topic 失败（改用自动创建）: " + e.Message);
            }
            admin.Shutdown();
        }

        long t0 = UtilAll.CurrentTimeMillis();

        // ---------- 1. 异步发送 ----------
        {
            string topic = _gPrefix + "_Async";
            var cb = new CollectingCallback();
            Message msg = new(topic, Str2Bytes("async-hello"));
            try
            {
                prod.SendAsync(msg, cb);
            }
            catch (Exception e)
            {
                Check("异步发送 sendAsync", false, "throw: " + e.Message);
            }

            for (int i = 0; i < 200 && cb.Ok() + cb.Err() == 0; ++i)
            {
                Thread.Sleep(25);
            }

            List<SendResult> rs = cb.Results();
            bool ok = cb.Ok() == 1 && cb.Err() == 0 && rs.Count > 0 && rs[0].SendStatus == SendStatus.SendOk;
            Check("异步发送 sendAsync", ok, "ok=" + cb.Ok().ToString(CultureInfo.InvariantCulture)
                  + " err=" + cb.Err().ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 2. 顺序消息：同 key 落同队列 + 顺序消费保序 ----------
        List<byte[]> bodiesOrder = new();
        {
            string topic = _gPrefix + "_Order";
            var selector = new SelectMessageQueueByHash();
            var qids = new List<int>();
            for (int i = 0; i < 10; ++i)
            {
                byte[] body = Str2Bytes(string.Format(CultureInfo.InvariantCulture, "ord-{0:00}", i));
                bodiesOrder.Add(body);
                SendResult sr = prod.SendBySelector(new Message(topic, body), selector, "shard-A");
                if (!qids.Contains(sr.MessageQueue.QueueId)) qids.Add(sr.MessageQueue.QueueId);
            }

            Check("顺序发送: 同 key 路由到同一队列", qids.Count == 1,
                "distinct_queue_ids=" + (qids.Count == 0 ? "?" : string.Join(",", qids)));

            List<MessageExt> run = RunConsumer(topic, "*", 12, /*orderly=*/true, "order", /*expect=*/10);
            List<string> recv = run.Select(m => Bytes2Str(m.Body)).ToList();
            List<string> expect = bodiesOrder.Select(Bytes2Str).ToList();
            bool seqOk = recv.SequenceEqual(expect);
            bool offsetOk = true;
            for (int i = 1; i < run.Count; ++i)
            {
                if (run[i].QueueOffset <= run[i - 1].QueueOffset) offsetOk = false;
            }

            string orderDetail = "received=" + run.Count.ToString(CultureInfo.InvariantCulture) + "/10 seq_ok="
                                 + (seqOk ? "1" : "0") + " offset_monotonic=" + (offsetOk ? "1" : "0");
            if (run.Count > 0)
            {
                orderDetail += " first=" + recv[0] + " last=" + recv[run.Count - 1];
            }

            Check("顺序消费保序", run.Count == 10 && seqOk && offsetOk, orderDetail);
        }

        // ---------- 3. 带 Tag 消息 + 服务端 Tag 过滤 ----------
        {
            string topic = _gPrefix + "_Tag";
            for (int i = 0; i < 3; ++i)
            {
                Message m = new(topic, Str2Bytes("tagA-" + i)) { Tags = "TagA" };
                prod.Send(m);
            }

            for (int i = 0; i < 3; ++i)
            {
                Message m = new(topic, Str2Bytes("tagB-" + i)) { Tags = "TagB" };
                prod.Send(m);
            }

            List<MessageExt> run = RunConsumer(topic, "TagA", 12, false, "tag");
            bool allA = run.All(m => m.Tags == "TagA");
            Check("Tag 过滤消费（仅收到 TagA）", run.Count == 3 && allA,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 4. 用户属性透传 ----------
        {
            string topic = _gPrefix + "_Prop";
            for (int i = 0; i < 3; ++i)
            {
                Message m = new(topic, Str2Bytes("prop-" + i)) { Tags = "P" };
                m.SetUserProperty("city", "Hangzhou");
                m.SetUserProperty("env", "prod");
                prod.Send(m);
            }

            List<MessageExt> run = RunConsumer(topic, "*", 12, false, "prop");
            bool okCity = run.All(m => m.GetUserProperty("city") == "Hangzhou");
            bool okEnv = run.All(m => m.GetUserProperty("env") == "prod");
            Check("用户属性透传(city=Hangzhou, env=prod)", okCity && okEnv && run.Count == 3,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 5. 延迟消息 ----------
        {
            string topic = _gPrefix + "_Delay";
            prod.Send(new Message(topic, Str2Bytes("normal-now")));
            Message dm = new(topic, Str2Bytes("delayed-5s")) { DelayTimeLevel = 2 }; // level2 = 5s
            prod.Send(dm);

            List<MessageExt> run = RunConsumer(topic, "*", 20, false, "delay", /*expect=*/2);
            var delayed = new List<MessageExt>();
            var normal = new List<MessageExt>();
            foreach (MessageExt m in run)
            {
                if (Bytes2Str(m.Body) == "delayed-5s") delayed.Add(m);
                else if (Bytes2Str(m.Body) == "normal-now") normal.Add(m);
            }

            Check("延迟消息最终投递", delayed.Count > 0,
                "delayed received=" + delayed.Count.ToString(CultureInfo.InvariantCulture)
                + " normal=" + normal.Count.ToString(CultureInfo.InvariantCulture));
            if (delayed.Count > 0)
            {
                long drift = delayed[0].StoreTimestamp - delayed[0].BornTimestamp;
                Check("延迟生效(store_ts-born_ts>=3000ms)", drift >= 3000,
                    "drift=" + drift.ToString(CultureInfo.InvariantCulture) + "ms");
            }
            else
            {
                Check("延迟生效(store_ts-born_ts>=3000ms)", false, "无延迟消息可校验");
            }
        }

        // ---------- 6. 带 Key 消息 + 按 Key 查询 ----------
        {
            string topic = _gPrefix + "_Key";
            string key = "MTKEY_" + stamp.ToString(CultureInfo.InvariantCulture);
            Message m = new(topic, Str2Bytes("key-msg-payload")) { Keys = key };
            prod.Send(m);
            Thread.Sleep(1000);

            long begin = t0 - 120000;
            long end = UtilAll.CurrentTimeMillis() + 120000;
            List<MessageExt> found = prod.QueryMessage(topic, key, 10, begin, end);
            bool hit = found.Any(x => Bytes2Str(x.Body) == "key-msg-payload");
            Check("按 Key 查询(query_message)", hit, "returned=" + found.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 7. 事务消息（对齐 Java 的两阶段：半消息 + END_TRANSACTION + 回查）----------
        // ---------- 7.1 COMMIT ----------
        {
            string topic = _gPrefix + "_Tx";
            var listener = new CommitTxListener();
            string stateStr;
            bool txOk = false;
            try
            {
                TransactionSendResult tsr = prod.SendMessageInTransaction(
                    new Message(topic, Str2Bytes("tx-commit")), listener);
                stateStr = LocalTransactionStateNames.Name(tsr.LocalTransactionState);
                txOk = tsr.SendStatus == SendStatus.SendOk
                    && tsr.LocalTransactionState == LocalTransactionState.CommitMessage;
            }
            catch (Exception e)
            {
                stateStr = "throw: " + e.Message;
            }

            Check("事务-COMMIT 发送状态", txOk, "state=" + stateStr);

            List<MessageExt> run = RunConsumer(topic, "*", 10, false, "tx");
            bool consumed = run.Any(m => Bytes2Str(m.Body) == "tx-commit");
            Check("事务-COMMIT 落库可被消费", consumed,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 7.2 ROLLBACK ----------
        {
            string topic = _gPrefix + "_TxRollback";
            var listener = new RollbackTxListener();
            string stateStr;
            bool txOk = false;
            try
            {
                TransactionSendResult tsr = prod.SendMessageInTransaction(
                    new Message(topic, Str2Bytes("tx-rollback")), listener);
                stateStr = LocalTransactionStateNames.Name(tsr.LocalTransactionState);
                txOk = tsr.SendStatus == SendStatus.SendOk
                    && tsr.LocalTransactionState == LocalTransactionState.RollbackMessage;
            }
            catch (Exception e)
            {
                stateStr = "throw: " + e.Message;
            }

            Check("事务-ROLLBACK 发送状态", txOk, "state=" + stateStr);

            List<MessageExt> run = RunConsumer(topic, "*", 10, false, "txrollback");
            bool consumed = run.Any(m => Bytes2Str(m.Body) == "tx-rollback");
            Check("事务-ROLLBACK 不被投递", !consumed,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 7.3 UNKNOW + broker 回查 ----------
        {
            string topic = _gPrefix + "_TxCheck";
            var listener = new UnknownThenCommitTxListener();
            string stateStr;
            bool txOk = false;
            try
            {
                TransactionSendResult tsr = prod.SendMessageInTransaction(
                    new Message(topic, Str2Bytes("tx-check")), listener);
                stateStr = LocalTransactionStateNames.Name(tsr.LocalTransactionState);
                txOk = tsr.SendStatus == SendStatus.SendOk
                    && tsr.LocalTransactionState == LocalTransactionState.Unknow;
            }
            catch (Exception e)
            {
                stateStr = "throw: " + e.Message;
            }

            Check("事务-UNKNOW 发送状态", txOk, "state=" + stateStr);

            // broker 巡检半消息的周期是 Java BrokerConfig#transactionCheckInterval，
            // **默认 30s**（联调 broker.conf 没覆写），所以半消息最快也要等一轮巡检才回查。
            // 固定 sleep(25) 会稳定地早于回查 → "最终投递"必然失败。这里先等"回查发生"
            // 这个事件（最多 90s，兼容 3s/30s 两种配置），再观察 10s 让 COMMIT 后的消息真正投递。
            long checkDeadline = UtilAll.CurrentTimeMillis() + 90_000;
            while (listener.CheckCalls == 0 && UtilAll.CurrentTimeMillis() < checkDeadline)
            {
                Thread.Sleep(1000);
            }
            List<MessageExt> run = RunConsumer(topic, "*", 10, false, "txcheck");
            bool consumed = run.Any(m => Bytes2Str(m.Body) == "tx-check");
            int checks = listener.CheckCalls;
            Check("事务-UNKNOW 触发 broker 回查", checks > 0,
                "checkLocalTransaction_calls=" + checks.ToString(CultureInfo.InvariantCulture));
            Check("事务-UNKNOW 回查后最终投递", consumed,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 8. 批量消息（SEND_BATCH_MESSAGE 320）----------
        // 请求码从 310 换成 320 之后，要在真 broker 上重新证明它确实按「批量写入」处理：
        // 判据是消费侧收到 N 条**各自完整**的消息且 queueOffset 连续。若 broker 把批量
        // body 当单条存（header.batch 没生效），这里只会收到 1 条拼接字节的消息。
        {
            string topic = _gPrefix + "_Batch";
            bool batchOk;
            string detail;
            try
            {
                var group = new List<Message>();
                for (int i = 0; i < 3; ++i)
                {
                    group.Add(new Message(topic, Str2Bytes("batch-" + i.ToString(CultureInfo.InvariantCulture))));
                }

                SendResult sr = prod.SendBatch(group, 5000);
                batchOk = sr.SendStatus == SendStatus.SendOk;
                detail = "status=" + sr.SendStatus;
            }
            catch (Exception e)
            {
                batchOk = false;
                detail = "throw: " + e.Message;
            }

            Check("批量发送 SendBatch(3 条)", batchOk, detail);

            List<MessageExt> msgs = RunConsumer(topic, "*", 15, false, "batch", expect: 3);
            var bodies = new HashSet<string>();
            var offsets = new List<long>();
            foreach (MessageExt m in msgs)
            {
                bodies.Add(Bytes2Str(m.Body));
                offsets.Add(m.QueueOffset);
            }

            offsets.Sort();
            bool contiguous = offsets.Count == 3;
            for (int i = 1; i < offsets.Count; ++i)
            {
                if (offsets[i] != offsets[i - 1] + 1) contiguous = false;
            }

            bool exact = true;
            for (int i = 0; i < 3; ++i)
            {
                if (!bodies.Contains("batch-" + i.ToString(CultureInfo.InvariantCulture))) exact = false;
            }

            Check("批量消息被 broker 按 3 条独立消息投递", exact && contiguous,
                "received=" + msgs.Count.ToString(CultureInfo.InvariantCulture)
                + " offsets=" + string.Join(",", offsets));
        }

        // ---------- 附：心跳注册 ----------
        {
            string topic = _gPrefix + "_Hb";
            prod.Send(new Message(topic, Str2Bytes("hb-probe")));
            var listener = new CollectingListenerConcurrently();
            var cons = new DefaultMQPushConsumer(_gPrefix + "_hb")
            {
                ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
                PullTimeoutMillis = 3000,
                PullSuspendTimeoutMillis = 1000,
            };
            cons.SetNamesrvAddr(_gNamesrv);
            cons.Subscribe(topic, "*");
            cons.SetMessageListener(listener);
            cons.Start();
            for (int i = 0; i < 40 && listener.Snapshot().Count < 1; ++i)
            {
                Thread.Sleep(250);
            }

            long hb = cons.HeartbeatCount;
            cons.Shutdown();
            Check("消费者心跳注册(HEART_BEAT)", hb > 0,
                "heartbeat_ok=" + hb.ToString(CultureInfo.InvariantCulture)
                + " consumed=" + listener.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
        }

        prod.Shutdown();

        // ---------- 汇总 ----------
        Console.WriteLine();
        Console.WriteLine("================ C# 消息类型联调汇总 ================");
        foreach ((string Name, bool Ok) r in Results)
        {
            Console.WriteLine("  [" + (r.Ok ? "PASS" : "FAIL") + "] " + r.Name);
        }

        Console.WriteLine("=====================================================");
        Console.WriteLine("  PASS=" + _gPass.ToString(CultureInfo.InvariantCulture)
                          + " FAIL=" + _gFail.ToString(CultureInfo.InvariantCulture));
        if (_gFail > 0)
        {
            Console.WriteLine("结果: " + _gFail.ToString(CultureInfo.InvariantCulture) + " 项失败");
            return 1;
        }

        Console.WriteLine("结果: 全部通过（C# 客户端对真实集群完成全部消息类型收发）");
        return 0;
    }
}
