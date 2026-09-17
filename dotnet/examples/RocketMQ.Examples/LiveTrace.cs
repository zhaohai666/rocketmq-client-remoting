// 消息轨迹真机验证（对齐 Java client.trace，与 cpp/examples/live_trace.cpp、
// python/verify_trace_live.py 同套场景）。
// 用法：rmq trace [namesrv]
//
// 前置：NameServer + Broker 已起，且 broker 配置 **traceTopicEnable=true**
//（否则 RMQ_SYS_TRACE_TOPIC 不会被预建，也用 admin 建不出来 —— 它是系统 topic，
// validateSystemTopicWhenUpdateTopic 默认 true 会拒绝创建）。
//
// 场景 S1–S17 与 Check() 一一对应，全部命中才返回 0。
//
// ⚠ 两条真机踩出来的硬约定（改脚本时勿回退）：
//   1. 业务消费者必须 ConsumeFromLastOffset：S1 的预热消息**没有 keys**，用
//      ConsumeFromFirstOffset 会被一起吃掉，既脏了计数断言，又会多出一条 keys 为空的 SubBefore；
//   2. 消费侧 MessageExt.MsgId 是 **offset 基 ID**（Java MessageDecoder:557-561 先 setMsgId
//      再 setOffsetMsgId，同值），所以它能对齐的是 SendResult.OffsetMsgId，
//      **不是** SendResult.MsgId（UNIQ_KEY）。
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveTrace
{
    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            _pass++;
            Console.WriteLine("  [PASS] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
        else
        {
            _fail++;
            Console.WriteLine("  [FAIL] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
    }

    private static bool WaitUntil(Func<bool> pred, int timeoutMs)
    {
        long deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeoutMs;
        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(200);
        }

        return pred();
    }

    private static long CountOf(string s, string sub)
    {
        long n = 0;
        int pos = 0;
        while ((pos = s.IndexOf(sub, pos, StringComparison.Ordinal)) >= 0)
        {
            n++;
            pos += sub.Length;
        }

        return n;
    }

    private static string BodyText(MessageExt m) => Encoding.UTF8.GetString(m.Body);

    // ---------------- 轨迹读取者：消费 RMQ_SYS_TRACE_TOPIC 并解码 ----------------
    private sealed class TraceCollector : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<TraceContext> _records = new();
        private readonly List<(string Keys, string Topic, string Text)> _raw = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt msg in msgs)
                {
                    string text = Encoding.UTF8.GetString(msg.Body);
                    string keys = msg.Keys ?? string.Empty;
                    _raw.Add((keys, msg.Topic, text));
                    try
                    {
                        foreach (TraceContext c in TraceDataEncoder.DecoderFromTraceDataString(text))
                        {
                            _records.Add(c);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("  [warn] decode trace failed: " + e.Message);
                    }
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<TraceContext> Records()
        {
            lock (_lk) return new List<TraceContext>(_records);
        }

        public List<(string Keys, string Topic, string Text)> Raw()
        {
            lock (_lk) return new List<(string, string, string)>(_raw);
        }

        public bool HasPub(string msgId)
        {
            lock (_lk)
            {
                foreach (TraceContext c in _records)
                {
                    if (c.TraceType == TraceType.Pub && c.TraceBeans.Count > 0
                        && c.TraceBeans[0].MsgId == msgId)
                    {
                        return true;
                    }
                }

                return false;
            }
        }
    }

    // ---------------- 业务消息收集者 ----------------
    private sealed class MsgCollector : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<MessageExt> _msgs = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                _msgs.AddRange(msgs);
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<MessageExt> Snapshot()
        {
            lock (_lk) return new List<MessageExt>(_msgs);
        }

        public int Count()
        {
            lock (_lk) return _msgs.Count;
        }
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        // topic 与业务消费组都带 Stamp：`run_trace_live.sh all` 会让三语言**依次**跑在同一个 broker 上，
        // 固定名会让第二个语言继承上一轮的位点与残留消息，使断言结果依赖运行顺序。全新 topic + 全新组
        // 才能让每次运行的起点一致（真正的鲁棒性另有保障，见 S6 的 body 过滤）。
        string topic = "TraceTopicLive_" + Stamp;
        string noTraceTopic = "TraceNoTraceTopic_" + Stamp;
        string producerGroup = "GID_trace_producer_live";
        string consumerGroup = "GID_trace_live_" + Stamp;
        string traceReaderGroup = "GID_trace_reader_live_" + Stamp;

        Console.WriteLine("=".PadRight(70, '='));
        Console.WriteLine("RocketMQ message trace live verify (.NET): namesrv=" + namesrv
            + " topic=" + topic);
        Console.WriteLine("=".PadRight(70, '='));

        // ---------------- 轨迹读取者（必须在被测动作之前起来）----------------
        var collector = new TraceCollector();
        var traceReader = new DefaultMQPushConsumer(traceReaderGroup);
        traceReader.SetNamesrvAddr(namesrv);
        traceReader.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        traceReader.Subscribe(MixAll.TraceTopic, "*");
        traceReader.SetMessageListener(collector);
        traceReader.Start();
        Console.WriteLine("trace reader started (group=" + traceReaderGroup + ", topic="
            + MixAll.TraceTopic + ")");

        // ---------------- S1 预热建 topic ----------------
        var warm = new DefaultMQProducer(producerGroup + "_warm");
        warm.NamesrvAddr = namesrv;
        warm.Start();
        try
        {
            warm.Send(new Message(topic, Encoding.UTF8.GetBytes("warm-up")), 5000);
            Check("S1 预热消息发送成功（触发 broker 自动建 topic）", true);
        }
        catch (Exception e)
        {
            Check("S1 预热消息发送成功（触发 broker 自动建 topic）", false, e.Message);
            return 1;
        }

        // 预热后等一下再起消费者：降低"位点解析"与"broker consumequeue 异步分发"的竞态。
        // 这一步只是让时序更宽裕，**不是**正确性保障 —— 预热消息本来就可能被投递，见 S6 的 body 过滤。
        Thread.Sleep(1500);

        // ---------------- S2 开轨迹的业务消费者 ----------------
        var msgCollector = new MsgCollector();
        var consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.SetNamesrvAddr(namesrv);
        // 见文件头 ⚠ 1：必须从队尾起算，否则会吃掉无 keys 的预热消息
        consumer.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
        consumer.Subscribe(topic, "*");
        consumer.SetMessageListener(msgCollector);
        consumer.EnableTrace = true;                       // ← 消费侧轨迹
        consumer.Start();
        bool assigned = WaitUntil(() => consumer.AssignedQueueKeys().Count > 0, 30000);
        Check("S2 消费者已分配到队列（先起消费者再发消息）", assigned,
            "assigned=" + consumer.AssignedQueueKeys().Count.ToString(CultureInfo.InvariantCulture));

        var producer = new DefaultMQProducer(producerGroup);
        producer.NamesrvAddr = namesrv;
        producer.EnableTrace = true;                       // ← 发送侧轨迹
        producer.Start();

        string body = "trace-live-" + Stamp;
        string keys = "KeyA KeyB " + Stamp;
        var outMsg = new Message(topic, Encoding.UTF8.GetBytes(body)) { Tags = "TagA", Keys = keys };
        SendResult result = producer.Send(outMsg, 5000);

        Check("S3 SendResult.traceOn == true（broker 默认 traceOn=true）", result.TraceOn,
            "traceOn=" + (result.TraceOn ? "true" : "false"));
        Check("S4 SendResult.msgId 是客户端 UNIQ_KEY，且与 offsetMsgId 不同",
            result.MsgId.Length > 0 && result.OffsetMsgId.Length > 0
            && result.MsgId != result.OffsetMsgId,
            "msgId=" + result.MsgId + " offsetMsgId=" + result.OffsetMsgId);
        Check("S5 SendResult.regionId 已解析（缺省 DefaultRegion）",
            result.RegionId == MixAll.DefaultTraceRegionId, "region=" + result.RegionId);

        // ---------------- S6/S7 消费侧 ----------------
        // ⚠ 只统计**本次发送的**消息：topic 上必然还留着 S1 的预热消息，且消费者的起始位点在
        // "组首次消费"时取的是当时的 maxOffset —— broker 的 consumequeue 是异步分发的，位点可能
        // 落在预热消息之前，于是预热消息也会被投递过来（实测三语言都踩过）。断言的本意是"本次
        // 业务消息被消费到"，所以按 body 过滤，而不是赌 topic 上只有一条消息。
        static bool IsPrimary(MessageExt m) => BodyText(m).StartsWith("trace-live-", StringComparison.Ordinal);

        bool got = WaitUntil(() => msgCollector.Snapshot().Exists(IsPrimary), 25000);
        List<MessageExt> msgs = msgCollector.Snapshot().FindAll(IsPrimary);
        Check("S6 业务消费者收到本次发送的那 1 条消息（按 body 过滤预热消息）", got && msgs.Count == 1,
            "count=" + msgs.Count.ToString(CultureInfo.InvariantCulture));
        string recvMsgId = msgs.Count == 0 ? string.Empty : msgs[0].MsgId;
        string recvUniq = msgs.Count == 0
            ? string.Empty
            : msgs[0].GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx);
        Check("S7 消费侧 msg_id == SendResult.offset_msg_id，且 UNIQ_KEY 经属性带到消费侧",
            msgs.Count > 0 && recvMsgId == result.OffsetMsgId && recvUniq == result.MsgId,
            "recv=" + recvMsgId + " offset=" + result.OffsetMsgId + " uniq=" + recvUniq);

        // ---------------- 无 keys 的消息（供 S17 用真机数据锁解码器健壮性）----------------
        SendResult keyless = producer.Send(new Message(topic, Encoding.UTF8.GetBytes("no-keys-here")),
            5000);
        static bool IsKeyless(MessageExt m) => BodyText(m) == "no-keys-here";
        bool keylessOk = WaitUntil(() => msgCollector.Snapshot().Exists(IsKeyless), 25000);
        string keylessId = keyless.OffsetMsgId;

        // 关掉消费者 → 触发消费侧轨迹分发器 flush，保证 SubBefore/SubAfter 落盘
        consumer.Shutdown();
        Thread.Sleep(1000);

        // ---------------- S8 等 Pub 轨迹落地 ----------------
        bool pubOk = WaitUntil(() => collector.HasPub(result.MsgId), 35000);
        producer.Shutdown();                               // flush 发送侧轨迹
        Thread.Sleep(500);
        pubOk = WaitUntil(() => collector.HasPub(result.MsgId), 10000) || pubOk;

        List<TraceContext> records = collector.Records();
        List<(string Keys, string Topic, string Text)> raw = collector.Raw();
        Console.WriteLine("  [info] 收到 " + raw.Count.ToString(CultureInfo.InvariantCulture)
            + " 条轨迹消息 / 解出 " + records.Count.ToString(CultureInfo.InvariantCulture)
            + " 条轨迹记录");
        foreach (TraceContext r in records)
        {
            Console.WriteLine("         - " + (r.TraceType?.ToString() ?? "-")
                + " topic=" + (r.TraceBeans.Count == 0 ? "-" : r.TraceBeans[0].Topic)
                + " msgId=" + (r.TraceBeans.Count == 0 ? "-" : r.TraceBeans[0].MsgId)
                + " group=" + r.GroupName
                + " success=" + (r.IsSuccess ? "true" : "false")
                + " code=" + r.ContextCode.ToString(CultureInfo.InvariantCulture));
        }

        int pubCount = 0;
        TraceContext? pub = null;
        foreach (TraceContext r in records)
        {
            if (r.TraceType == TraceType.Pub && r.TraceBeans.Count > 0
                && r.TraceBeans[0].MsgId == result.MsgId)
            {
                pubCount++;
                pub ??= r;
            }
        }

        Check("S8 轨迹里出现 Pub 记录且 msgId 与 SendResult.msg_id 一致", pubOk && pubCount >= 1,
            "count=" + pubCount.ToString(CultureInfo.InvariantCulture));
        Check("S9 Pub 轨迹的 topic / groupName 正确",
            pub is not null && pub.TraceBeans[0].Topic == topic && pub.GroupName == producerGroup,
            pub is null
                ? "no pub record"
                : "topic=" + pub.TraceBeans[0].Topic + " group=" + pub.GroupName);
        {
            bool keyHit = false;
            foreach ((string k, string _, string _) in raw)
            {
                if (k.Contains(result.MsgId, StringComparison.Ordinal)) keyHit = true;
            }

            Check("S10 承载 Pub 轨迹的轨迹消息 keys 里含该 msgId（控制台按 keys 反查）", keyHit,
                "msgId=" + result.MsgId);
        }

        int subBeforeCount = 0;
        int subAfterCount = 0;
        TraceContext? subBefore = null;
        TraceContext? subAfter = null;
        foreach (TraceContext r in records)
        {
            if (r.TraceBeans.Count == 0 || r.TraceBeans[0].MsgId != recvMsgId) continue;
            if (r.TraceType == TraceType.SubBefore)
            {
                subBeforeCount++;
                subBefore ??= r;
            }
            else if (r.TraceType == TraceType.SubAfter)
            {
                subAfterCount++;
                subAfter ??= r;
            }
        }

        Check("S11 轨迹里出现 SubBefore 且 msgId 与消费侧一致", subBeforeCount >= 1,
            "count=" + subBeforeCount.ToString(CultureInfo.InvariantCulture));
        Check("S12 SubBefore/SubAfter 配对且 requestId 一致、success=true、contextCode=0",
            subBefore is not null && subAfter is not null
            && subAfter.RequestId == subBefore.RequestId && subAfter.IsSuccess
            && subAfter.ContextCode == 0,
            subAfter is null
                ? "no sub_after"
                : "req=" + subAfter.RequestId + " success=" + (subAfter.IsSuccess ? "true" : "false")
                  + " code=" + subAfter.ContextCode.ToString(CultureInfo.InvariantCulture));
        Check("S13 SubBefore 的 retryTimes 与被消费消息一致",
            subBefore is not null && msgs.Count > 0
            && subBefore.TraceBeans[0].RetryTimes == msgs[0].ReconsumeTimes,
            subBefore is null
                ? "no sub traces"
                : "trace=" + subBefore.TraceBeans[0].RetryTimes.ToString(CultureInfo.InvariantCulture)
                  + " msg=" + (msgs.Count == 0
                      ? "-1"
                      : msgs[0].ReconsumeTimes.ToString(CultureInfo.InvariantCulture)));

        // ---------------- S14 防递归 ----------------
        {
            bool leak = false;
            foreach (TraceContext r in records)
            {
                if (r.TraceBeans.Count > 0 && r.TraceBeans[0].Topic == MixAll.TraceTopic) leak = true;
            }

            Check("S14 没有任何轨迹记录的 topic 是轨迹 topic 本身（防递归）", !leak);
        }

        // ---------------- S15 关闭轨迹就不产轨迹 ----------------
        var quiet = new DefaultMQProducer(producerGroup + "_quiet");
        quiet.NamesrvAddr = namesrv;
        quiet.EnableTrace = false;                         // ← 显式关闭
        quiet.Start();
        quiet.Send(new Message(noTraceTopic, Encoding.UTF8.GetBytes("no-trace")), 5000);
        quiet.Shutdown();
        Thread.Sleep(6500);
        {
            int leaked = 0;
            foreach (TraceContext r in collector.Records())
            {
                if (r.TraceType == TraceType.Pub && r.TraceBeans.Count > 0
                    && r.TraceBeans[0].Topic == noTraceTopic)
                {
                    leaked++;
                }
            }

            Check("S15 enable_trace=false 的生产者不产生 Pub 轨迹", leaked == 0,
                "leaked=" + leaked.ToString(CultureInfo.InvariantCulture));
        }

        // ---------------- S16 编码结构自检 ----------------
        {
            // 每条记录都以 FIELD_SPLITOR 结尾 → 记录数就是 STX 出现次数。
            // 这条断言的价值：解码器只要丢过任何一条记录（真机踩过），这里立刻红。
            long segments = 0;
            foreach ((string _, string t, string text) in raw)
            {
                if (t == MixAll.TraceTopic) segments += CountOf(text, TraceConstants.FieldSplitor);
            }

            Check("S16 轨迹文本记录数（STX 计数）== 解码出的记录数（Java split 语义）",
                segments == records.Count && records.Count > 0,
                "segments=" + segments.ToString(CultureInfo.InvariantCulture)
                + " decoded=" + records.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------------- S17 无 keys 消息的轨迹也要解得出来 ----------------
        {
            int before = 0;
            string keylessKeys = string.Empty;
            foreach (TraceContext r in records)
            {
                if (r.TraceType == TraceType.SubBefore && r.TraceBeans.Count > 0
                    && r.TraceBeans[0].MsgId == keylessId)
                {
                    before++;
                    keylessKeys = r.TraceBeans[0].Keys;
                }
            }

            Check("S17 无 keys 的消息也能解出 SubBefore 轨迹（空 keys 段不崩）",
                keylessOk && before >= 1 && keylessKeys.Length == 0,
                "consumed=" + (keylessOk ? "true" : "false")
                + " before=" + before.ToString(CultureInfo.InvariantCulture));
        }

        traceReader.Shutdown();
        warm.Shutdown();

        Console.WriteLine("########################################");
        Console.WriteLine("PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }
}
