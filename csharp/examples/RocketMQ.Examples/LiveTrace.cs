// Prerequisite: a NameServer + Broker are up and the broker is configured with
// **traceTopicEnable=true** (otherwise RMQ_SYS_TRACE_TOPIC is not pre-created and cannot be
// created with the admin API either -- it is a system topic and
// validateSystemTopicWhenUpdateTopic defaults to true, which refuses the creation).
//
// Scenarios S1-S17 map one to one onto Check(); it returns 0 only when everything hits.
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

    // ---------------- the trace reader: consumes RMQ_SYS_TRACE_TOPIC and decodes ----------------
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

    // ---------------- the business message collector ----------------
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
        // Both the topic and the business consumer group carry a Stamp: several rounds of
        // real-machine integration run **one after another** against the same broker, and a
        // fixed name would inherit the previous round's offsets and leftover messages, making
        // the assertions depend on the run order. A brand new topic + a brand new group is
        // what makes each run start from the same point (the real robustness is guaranteed
    // elsewhere, see the body filtering in S6).
        string topic = "TraceTopicLive_" + Stamp;
        string noTraceTopic = "TraceNoTraceTopic_" + Stamp;
        string producerGroup = "GID_trace_producer_live";
        string consumerGroup = "GID_trace_live_" + Stamp;
        string traceReaderGroup = "GID_trace_reader_live_" + Stamp;

        Console.WriteLine("=".PadRight(70, '='));
        Console.WriteLine("RocketMQ message trace live verify (C#): namesrv=" + namesrv
            + " topic=" + topic);
        Console.WriteLine("=".PadRight(70, '='));

        // ---------------- the trace reader (it has to be up before the action under test)----------------
        var collector = new TraceCollector();
        var traceReader = new DefaultMQPushConsumer(traceReaderGroup);
        traceReader.SetNamesrvAddr(namesrv);
        traceReader.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        traceReader.Subscribe(MixAll.TraceTopic, "*");
        traceReader.SetMessageListener(collector);
        traceReader.Start();
        Console.WriteLine("trace reader started (group=" + traceReaderGroup + ", topic="
            + MixAll.TraceTopic + ")");

        // ---------------- S1 warm-up creates the topic ----------------
        var warm = new DefaultMQProducer(producerGroup + "_warm");
        warm.NamesrvAddr = namesrv;
        warm.Start();
        try
        {
            warm.Send(new Message(topic, Encoding.UTF8.GetBytes("warm-up")), 5000);
       Check("S1 the warm-up message was sent successfully (triggering the broker to auto-create the topic)", true);
        }
        catch (Exception e)
        {
       Check("S1 the warm-up message was sent successfully (triggering the broker to auto-create the topic)", false, e.Message);
            return 1;
        }

        // Wait a bit after the warm-up before starting the consumer, to reduce the race
    // between "offset resolution" and the broker's asynchronous consumequeue dispatch.
        // This only gives the timing more slack, it is **not** a correctness guarantee -- the
        // warm-up message may well be delivered, see the body filtering in S6.
        Thread.Sleep(1500);

        // ---------------- S2 the business consumer with tracing on ----------------
        var msgCollector = new MsgCollector();
        var consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.SetNamesrvAddr(namesrv);
   // See the file header note (1): it must count from the tail, otherwise it would swallow
        // the warm-up message that has no keys
        consumer.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
        consumer.Subscribe(topic, "*");
        consumer.SetMessageListener(msgCollector);
        consumer.EnableTrace = true;                       // <- consumer-side tracing
        consumer.Start();
        bool assigned = WaitUntil(() => consumer.AssignedQueueKeys().Count > 0, 30000);
    Check("S2 the consumer was assigned queues (start the consumer before sending)", assigned,
            "assigned=" + consumer.AssignedQueueKeys().Count.ToString(CultureInfo.InvariantCulture));

        var producer = new DefaultMQProducer(producerGroup);
        producer.NamesrvAddr = namesrv;
   producer.EnableTrace = true;                       // <- producer-side tracing
        producer.Start();

        string body = "trace-live-" + Stamp;
        string keys = "KeyA KeyB " + Stamp;
        var outMsg = new Message(topic, Encoding.UTF8.GetBytes(body)) { Tags = "TagA", Keys = keys };
        SendResult result = producer.Send(outMsg, 5000);

     Check("S3 SendResult.traceOn == true (the broker defaults traceOn=true)", result.TraceOn,
            "traceOn=" + (result.TraceOn ? "true" : "false"));
   Check("S4 SendResult.msgId is the client's UNIQ_KEY and differs from offsetMsgId",
            result.MsgId.Length > 0 && result.OffsetMsgId.Length > 0
            && result.MsgId != result.OffsetMsgId,
            "msgId=" + result.MsgId + " offsetMsgId=" + result.OffsetMsgId);
     Check("S5 SendResult.regionId is resolved (the default is DefaultRegion)",
            result.RegionId == MixAll.DefaultTraceRegionId, "region=" + result.RegionId);

        // ---------------- S6/S7 the consume side ----------------
  // Only the messages from **this send** are counted: the topic necessarily still holds S1's
        // warm-up message, and a consumer's starting offset on "the group's first consumption"
  // is the maxOffset at that moment -- the broker's consumequeue is dispatched
  // asynchronously, so the offset can land before the warm-up message and the warm-up
    // message can then be delivered too (observed on a real machine). What the assertion
 // means is "this business message was consumed", so it filters by body instead of
  // betting on the topic holding exactly one message.
        static bool IsPrimary(MessageExt m) => BodyText(m).StartsWith("trace-live-", StringComparison.Ordinal);

        bool got = WaitUntil(() => msgCollector.Snapshot().Exists(IsPrimary), 25000);
        List<MessageExt> msgs = msgCollector.Snapshot().FindAll(IsPrimary);
    Check("S6 the business consumer received the 1 message from this send (the warm-up message is filtered out by body)", got && msgs.Count == 1,
            "count=" + msgs.Count.ToString(CultureInfo.InvariantCulture));
        string recvMsgId = msgs.Count == 0 ? string.Empty : msgs[0].MsgId;
        string recvUniq = msgs.Count == 0
            ? string.Empty
            : msgs[0].GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx);
  Check("S7 the consume side msg_id == SendResult.offset_msg_id and the UNIQ_KEY travelled there via the properties",
            msgs.Count > 0 && recvMsgId == result.OffsetMsgId && recvUniq == result.MsgId,
            "recv=" + recvMsgId + " offset=" + result.OffsetMsgId + " uniq=" + recvUniq);

        // ---------------- a message with no keys (S17 uses real machine data to lock decoder robustness)----------------
        SendResult keyless = producer.Send(new Message(topic, Encoding.UTF8.GetBytes("no-keys-here")),
            5000);
        static bool IsKeyless(MessageExt m) => BodyText(m) == "no-keys-here";
        bool keylessOk = WaitUntil(() => msgCollector.Snapshot().Exists(IsKeyless), 25000);
        string keylessId = keyless.OffsetMsgId;

        // close the consumer -> triggers the consume-side trace dispatcher to flush, ensuring SubBefore/SubAfter land
        consumer.Shutdown();
        Thread.Sleep(1000);

        // ---------------- S8 wait for the Pub trace to land ----------------
        bool pubOk = WaitUntil(() => collector.HasPub(result.MsgId), 35000);
  producer.Shutdown();   // flush the producer-side trace
        Thread.Sleep(500);
        pubOk = WaitUntil(() => collector.HasPub(result.MsgId), 10000) || pubOk;

        List<TraceContext> records = collector.Records();
        List<(string Keys, string Topic, string Text)> raw = collector.Raw();
   Console.WriteLine("  [info] received " + raw.Count.ToString(CultureInfo.InvariantCulture)
            + " trace messages / decoded " + records.Count.ToString(CultureInfo.InvariantCulture)
            + " trace records");
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

   Check("S8 a Pub record appears in the trace and its msgId matches SendResult.msg_id", pubOk && pubCount >= 1,
            "count=" + pubCount.ToString(CultureInfo.InvariantCulture));
        Check("S9 the Pub trace's topic / groupName are correct",
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

      Check("S10 the keys of the trace message carrying the Pub trace contain that msgId (the console looks it up by keys)", keyHit,
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

  Check("S11 a SubBefore appears in the trace and its msgId matches the consume side", subBeforeCount >= 1,
            "count=" + subBeforeCount.ToString(CultureInfo.InvariantCulture));
        Check("S12 SubBefore/SubAfter are paired with the same requestId, success=true, contextCode=0",
            subBefore is not null && subAfter is not null
            && subAfter.RequestId == subBefore.RequestId && subAfter.IsSuccess
            && subAfter.ContextCode == 0,
            subAfter is null
                ? "no sub_after"
                : "req=" + subAfter.RequestId + " success=" + (subAfter.IsSuccess ? "true" : "false")
                  + " code=" + subAfter.ContextCode.ToString(CultureInfo.InvariantCulture));
    Check("S13 SubBefore's retryTimes matches the consumed message",
            subBefore is not null && msgs.Count > 0
            && subBefore.TraceBeans[0].RetryTimes == msgs[0].ReconsumeTimes,
            subBefore is null
                ? "no sub traces"
                : "trace=" + subBefore.TraceBeans[0].RetryTimes.ToString(CultureInfo.InvariantCulture)
                  + " msg=" + (msgs.Count == 0
                      ? "-1"
                      : msgs[0].ReconsumeTimes.ToString(CultureInfo.InvariantCulture)));

   // ---------------- S14 the recursion guard ----------------
        {
            bool leak = false;
            foreach (TraceContext r in records)
            {
                if (r.TraceBeans.Count > 0 && r.TraceBeans[0].Topic == MixAll.TraceTopic) leak = true;
            }

      Check("S14 no trace record has the trace topic itself as its topic (the recursion guard)", !leak);
        }

        // ---------------- S15 turning tracing off produces no trace ----------------
        var quiet = new DefaultMQProducer(producerGroup + "_quiet");
        quiet.NamesrvAddr = namesrv;
        quiet.EnableTrace = false;             // <- explicitly off
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

       Check("S15 a producer with enable_trace=false produces no Pub trace", leaked == 0,
                "leaked=" + leaked.ToString(CultureInfo.InvariantCulture));
        }

        // ---------------- S16 the encoding structure self-check ----------------
        {
      // Every record ends with FIELD_SPLITOR -> the record count is the number of STX
        // occurrences. The value of this assertion: if the decoder ever dropped a record
  // (which happened on a real machine) it goes red right here.
            long segments = 0;
            foreach ((string _, string t, string text) in raw)
            {
                if (t == MixAll.TraceTopic) segments += CountOf(text, TraceConstants.FieldSplitor);
            }

            Check("S16 the number of records in the trace text (STX count) == the number of decoded records",
                segments == records.Count && records.Count > 0,
                "segments=" + segments.ToString(CultureInfo.InvariantCulture)
                + " decoded=" + records.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------------- S17 a trace for a message with no keys must also decode ----------------
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

       Check("S17 a message with no keys still yields a decodable SubBefore trace (the empty keys segment does not crash it)",
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
