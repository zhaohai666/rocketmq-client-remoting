// C# 客户端的**真实集群**消息类型联调。
//
// 覆盖 11 类消息能力，全部打真实 nameServer + broker：
//   1. 同步普通消息（Send → SEND_MESSAGE_V2，落库读回）
//   2. 异步发送（sendAsync + SendCallback）
//   3. 单向发送（SendOneway，只发不等响应；只能靠读回计数佐证）
//   4. 顺序消息（sendBySelector 同 key 落同队列 + 顺序消费保序）
//   5. 带 Tag 消息 + 服务端 Tag 过滤
//   6. 用户属性透传
//   7. 延迟消息（setDelayTimeLevel 并校验 store_ts - born_ts >= 3000ms）
//   8. 定时消息（TIMER_DELIVER_MS 属性，broker 定时仓储二级存储后投递，同样校验投递延迟）
//   9. 带 Key 消息 + 按 Key 服务端查询（QUERY_MESSAGE）
//  10. 事务消息（两阶段的提交路径；完整链路见 RunTransaction）+ 落库可消费
//  11. 批量消息（SendBatch → SEND_BATCH_MESSAGE(320)，broker 按 N 条独立消息投递、offset 连续）
//  附：消费者心跳注册（HEART_BEAT）
//
// 本程序自身不启动集群；调用方需先启动 nameServer(9876) + broker(10911) 且
// autoCreateTopicEnable=true。用法（由 Program 以
// "message-types [namesrv] [ak] [sk]" 形式调用）。
//
// 传了 accessKey/secretKey 时（broker 开 authenticationEnabled 的集群）：
//   - 本程序引导的**每个**客户端（生产者 / push 消费者 / admin）都挂上 ACL 凭据；
//   - 先跑一个不带凭据的探测生产者，断言其发送被拒（NO_PERMISSION=16）——
//     没有这条反向断言，在未开鉴权的集群上全绿只是假通过。
// 不带凭据时行为与旧版完全一致（纯文本集群的 harness 照常可用）。
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>真实集群 11 类消息能力联调（可选 ACL 凭据 + 无凭据反向断言）。</summary>
internal static class LiveMessageTypes
{
    private static string _gNamesrv = "127.0.0.1:9876";
    private static string _gPrefix = string.Empty;
    // ACL 凭据：两者都非空才生效；为空时所有客户端不挂钩子，行为与旧版一致。
    private static string _gAk = string.Empty;
    private static string _gSk = string.Empty;
    private static bool WithAcl => _gAk.Length > 0 && _gSk.Length > 0;
    private static readonly List<(string Name, bool Ok)> Results = new();
    private static int _gPass;
    private static int _gFail;

    // broker 侧鉴权失败的统一响应码（详见本目录 acl 用例）。
    private const int NoPermission = 16;

    // 给本程序引导的每类客户端装 ACL 凭据。三者的 SetCredentials 语义一致：
    // Start() 前绑定，Start() 时经 RequestHooks.Compose 注册进传输层单槽钩子。
    private static void ApplyCreds(DefaultMQProducer prod)
    {
        if (WithAcl) prod.SetCredentials(_gAk, _gSk);
    }

    private static void ApplyCreds(DefaultMQPushConsumer consumer)
    {
        if (WithAcl) consumer.SetCredentials(_gAk, _gSk);
    }

    private static void ApplyCreds(DefaultMQAdminExt admin)
    {
        if (WithAcl) admin.SetCredentials(_gAk, _gSk);
    }

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
        private readonly List<Exception> _errors = new();

        public void OnSuccess(SendResult result)
        {
            Interlocked.Increment(ref _ok);
            lock (_lk) _results.Add(result);
        }

        public void OnException(Exception error)
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
            lock (_lk) return _errors.Select(e => e.Message).ToList();
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
        ApplyCreds(consumer);
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
        if (args.Length >= 3)
        {
            _gAk = args[1];
            _gSk = args[2];
        }

        long stamp = UtilAll.CurrentTimeSeconds();
        _gPrefix = "MTDotnet_" + stamp;

        Console.WriteLine("=== C# 客户端消息类型联调（真实集群）===");
        Console.WriteLine("namesrv = " + _gNamesrv + "  prefix = " + _gPrefix +
                          "  acl = " + (WithAcl ? "on(ak=" + _gAk + ")" : "off"));

        // ---------- 0. 反向断言：不带凭据的发送必须被拒（NO_PERMISSION=16） ----------
        // 只有传了凭据才跑：这一半断言用来戳穿「集群其实没开鉴权」的假通过。
        // 探测发送打到 TBW102（每个 broker 启动即注册默认 topic，路由必然可解析；
        // 若打全新 topic，无路由会在本地先炸，根本走不到 broker 的鉴权环节）。
        // 探测生产者放在**第一个**启动：本端口的客户端 Start() 时默认 instanceName 会被
        // 改写成 <pid>#<nanoTime>，每个客户端各自独占一个 MQClientInstance 与传输层
        // 钩子槽，本不存在「先到先得」的串扰；仍按先起先关的顺序跑，两种实例语义下都安全。
        if (WithAcl)
        {
            var probe = new DefaultMQProducer(_gPrefix + "_probe")
            {
                NamesrvAddr = _gNamesrv,
                SendMsgTimeout = 5000,
            };
            bool rejected = false;
            string probeDetail;
            try
            {
                probe.Start();
                try
                {
                    probe.Send(new Message(MixAll.DefaultTopic, Str2Bytes("no-cred-probe")));
                    probeDetail = "unexpectedly succeeded（集群可能没开鉴权）";
                }
                catch (MQBrokerException e)
                {
                    probeDetail = "code=" + e.ResponseCode.ToString(CultureInfo.InvariantCulture)
                                  + " " + e.ResponseMessage;
                    rejected = e.ResponseCode == NoPermission;
                }
                catch (Exception e)
                {
                    // 部分管理路径会把底层异常拼进消息文本，按码值文本兜底识别。
                    probeDetail = e.Message;
                    rejected = probeDetail.Contains("CODE: "
                                 + NoPermission.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
                }
            }
            catch (Exception e)
            {
                probeDetail = "probe start throw: " + e.Message;
            }
            finally
            {
                probe.Shutdown();
            }

            Check("反向: 无凭据发送被拒(NO_PERMISSION=16)", rejected, probeDetail);
            // 关停后再等一拍，确保探测实例的资源彻底归还，不影响后续正式客户端。
            Thread.Sleep(1000);
        }

        var prod = new DefaultMQProducer(_gPrefix + "_producer")
        {
            NamesrvAddr = _gNamesrv,
            SendMsgTimeout = 5000,
        };
        ApplyCreds(prod);
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
            ApplyCreds(admin);
            admin.SetTimeoutMillis(10000);
            admin.Start();
            try
            {
                foreach (string suffix in new[]
                         { "Normal", "Async", "Oneway", "Order", "Tag", "Prop", "Delay", "Timer", "Key", "Tx",
                           "TxRollback", "TxCheck", "Batch", "Hb" })
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

        // ---------- 1. 同步普通消息 ----------
        {
            string topic = _gPrefix + "_Normal";
            var bodies = new List<string>();
            bool allOk = true;
            for (int i = 0; i < 3; ++i)
            {
                string body = "normal-" + i.ToString(CultureInfo.InvariantCulture);
                bodies.Add(body);
                try
                {
                    SendResult sr = prod.Send(new Message(topic, Str2Bytes(body)));
                    if (sr.SendStatus != SendStatus.SendOk || string.IsNullOrEmpty(sr.MsgId)) allOk = false;
                }
                catch (Exception e)
                {
                    allOk = false;
                    Console.WriteLine("  sync send throw: " + e.Message);
                }
            }

            List<MessageExt> run = RunConsumer(topic, "*", 12, false, "normal", expect: 3);
            var got = run.Select(m => Bytes2Str(m.Body)).OrderBy(x => x, StringComparer.Ordinal).ToList();
            bool readBack = got.SequenceEqual(bodies.OrderBy(x => x, StringComparer.Ordinal));
            Check("同步普通发送(SendOk)", allOk);
            Check("同步普通消息落库读回 3/3", readBack,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 2. 异步发送 ----------
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

        // ---------- 3. 单向发送（SendOneway）----------
        // 单向不等 broker 响应，本端只能证明「发出去且 broker 真落了库」：
        // 判据取消费侧读回条数（这是全程序里唯一允许只靠读回计数的类型）。
        {
            string topic = _gPrefix + "_Oneway";
            bool threw = false;
            try
            {
                for (int i = 0; i < 3; ++i)
                {
                    prod.SendOneway(new Message(topic, Str2Bytes(
                        "oneway-" + i.ToString(CultureInfo.InvariantCulture))));
                }
            }
            catch (Exception e)
            {
                threw = true;
                Console.WriteLine("  oneway send throw: " + e.Message);
            }

            List<MessageExt> run = RunConsumer(topic, "*", 15, false, "oneway", expect: 3);
            Check("单向发送 SendOneway(3 条)落库读回", !threw && run.Count == 3,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture) + "/3");
        }

        // ---------- 4. 顺序消息：同 key 落同队列 + 顺序消费保序 ----------
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

        // ---------- 5. 带 Tag 消息 + 服务端 Tag 过滤 ----------
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

        // ---------- 6. 用户属性透传 ----------
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

        // ---------- 7. 延迟消息（delay level）----------
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

        // ---------- 8. 定时消息（TIMER_DELIVER_MS）----------
        // 与 delay level 走不同机制：broker 把带 TIMER_DELIVER_MS 的消息收进定时
        // 仓储（SCHEDULE_TOPIC_XXXX 之外的二级存储），到点再投递回目标队列。
        // 客户端侧只需把属性写进 user property（SetDeliverTimeMs 即该属性的便捷入口），
        // 判据仍是读回消息的 store_ts - born_ts：定时未生效的话 drift 只有毫秒级。
        {
            string topic = _gPrefix + "_Timer";
            long deliverAt = UtilAll.CurrentTimeMillis() + 5000;
            Message tm = new(topic, Str2Bytes("timer-5s"));
            tm.SetDeliverTimeMs(deliverAt);
            prod.Send(tm);

            List<MessageExt> run = RunConsumer(topic, "*", 25, false, "timer", expect: 1);
            var hit = run.Where(m => Bytes2Str(m.Body) == "timer-5s").ToList();
            Check("定时消息(TIMER_DELIVER_MS)最终投递", hit.Count > 0,
                "received=" + hit.Count.ToString(CultureInfo.InvariantCulture) + "/1");
            if (hit.Count > 0)
            {
                long drift = hit[0].StoreTimestamp - hit[0].BornTimestamp;
                Check("定时生效(store_ts-born_ts>=4000ms)", drift >= 4000,
                    "drift=" + drift.ToString(CultureInfo.InvariantCulture) + "ms");
            }
            else
            {
                Check("定时生效(store_ts-born_ts>=4000ms)", false, "无定时消息可校验");
            }
        }

        // ---------- 9. 带 Key 消息 + 按 Key 查询 ----------
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

        // ---------- 10. 事务消息：COMMIT（半消息 → 本地事务提交 → 落库可消费）----------
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

        // ---------- 10.2 ROLLBACK ----------
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

        // ---------- 10.3 UNKNOW + broker 回查 ----------
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

        // ---------- 11. 批量消息（SEND_BATCH_MESSAGE 320）----------
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
            ApplyCreds(cons);
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
