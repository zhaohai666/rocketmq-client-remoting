// Request-Reply（5.x）真机验证（对应 python/verify_request_reply_live.py、cpp/examples/live_rr.cpp）。
// 用法：rmq rr [namesrv]
//
// 链路（与 Python 参考客户端同一套场景、同一套断言）：
//   请求方 producer.Request(msg, timeout)            应答方 push consumer
//   ──────────────────────────────                  ─────────────────────
//   CORRELATION_ID = uuid
//   REPLY_TO_CLIENT = clientId ──────────►          收到请求（broker 已写 CLUSTER）
//   TTL = timeout                                  create_reply_message(req, body)
//                                                    → topic = <CLUSTER>_REPLY_TOPIC
//                                                    → MSG_TYPE = "reply"
//   ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ────        producer.send(reply) → 325
//        broker 按 REPLY_TO_CLIENT 找到请求方连接推回
//
// 场景（与 Python 逐条对应）：
//   S1 只建请求 topic；断言客户端 **不能** 建 <cluster>_REPLY_TOPIC（broker 系统 topic），
//      并确认请求方已心跳注册（REPLY_TO_CLIENT 要靠它反查 channel）
//   S2 请求消息确实带 CORRELATION_ID / REPLY_TO_CLIENT / TTL
//   S3 Request() 拿回应答：body 正确、topic 是 <cluster>_REPLY_TOPIC、带 REPLY_MESSAGE_ARRIVE_TIME
//   S4 应答确实走了 SEND_REPLY_MESSAGE_V2(325)：第三个客户端订阅 <cluster>_REPLY_TOPIC 能看到
//   S5 无应答方时 Request() 在 timeout 附近抛 RequestTimeoutException（不卡死/不静默返回），
//      且异常带 10006 REQUEST_TIMEOUT_EXCEPTION（Java 抛的就是带码的那一个构造）
//   S6 并发 3 个 request：每个拿到的都是自己的应答（CORRELATION_ID 不串台）
//   S7 应答方（消费者）本身是普通 push 消费者：Reply 流量不影响后续普通消费
//   S8 CLUSTER 属性由 broker 在存储时写入：投递到的那份有、客户端手里那份没有 ——
//      这正是 MessageUtil.createReplyMessage 存在的意思；拿本地那份造应答必须撞上
//      带 10007 CREATE_REPLY_MESSAGE_EXCEPTION 的 MQClientException，而不是别的错
//
// ⚠ 两个真机必踩点（与 Python 文档一致）：
//   1) <cluster>_REPLY_TOPIC 是 broker 启动时注册的**系统 topic**，客户端 createTopic 会被
//      INVALID_PARAMETER(29)「conflict with system topic」拒绝，且 broker 侧不留日志，只能从
//      客户端异常看出来 —— 所以顺手把"错误传播是准的"也验了。
//   2) 消费者一律用 CONSUME_FROM_FIRST_OFFSET（项目 MEMORY 第 8 条硬性约定）。
using System.Text;
using System.Threading;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveRR
{
    private static readonly long WallNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static readonly string Stamp = WallNowMs.ToString(CultureInfo.InvariantCulture);
    private static readonly string Topic = "RequestReplyNet_" + Stamp;
    private static readonly string RequestGroup = "PG_RRReqNet_" + Stamp;
    private static readonly string ConsumerGroup = "CG_RRReplyNet_" + Stamp;
    private static readonly string WatchGroup = "CG_RRWatchNet_" + Stamp;

    private const int QueueNum = 4;
    // broker.conf 里写的是 brokerClusterName=DefaultCluster（见 /tmp/run_*_live.sh）
    private const string Cluster = "DefaultCluster";
    private static readonly string ReplyTopic = MixAll.GetReplyTopic(Cluster);
    private const int RequestTimeout = 8000;

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

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string BodyOf(MessageExt m) => Encoding.UTF8.GetString(m.Body);

    private static string BodyOf(Message m) => Encoding.UTF8.GetString(m.Body);

    // 顺着 inner 链找出 broker 的原始异常（CreateTopic 把 MQBrokerException 包进 MQClientException）。
    private static MQBrokerException? FindBrokerException(Exception? e)
    {
        while (e is not null)
        {
            if (e is MQBrokerException be)
            {
                return be;
            }

            e = e.InnerException;
        }

        return null;
    }

    private static bool WaitFor(Func<bool> pred, long timeoutMs)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred())
            {
                return true;
            }

            Thread.Sleep(200);
        }

        return false;
    }

    // ---------------- 应答方监听器（普通 push 消费者 + 发应答的生产者） ----------------
    private sealed class ReplierListener : IMessageListenerConcurrently
    {
        private readonly DefaultMQProducer _producer;
        private readonly object _lock = new();
        private readonly List<string> _received = new();
        private readonly List<string> _replied = new();
        private readonly List<string> _replyErrors = new();
        private bool _gotFirst;
        private string? _firstCorr;
        private string? _firstReplyTo;
        private string? _firstTtl;
        private MessageExt? _deliveredPing;

        /// <summary>投递到的那条 "ping-1" 请求原文（S8 要用它验 broker 写的 CLUSTER）。</summary>
        public MessageExt? DeliveredPing
        {
            get
            {
                lock (_lock)
                {
                    return _deliveredPing;
                }
            }
        }

        public ReplierListener(DefaultMQProducer producer) => _producer = producer;

        public DefaultMQProducer Producer => _producer;

        public int ReceivedCount
        {
            get
            {
                lock (_lock)
                {
                    return _received.Count;
                }
            }
        }

        public int RepliedCount
        {
            get
            {
                lock (_lock)
                {
                    return _replied.Count;
                }
            }
        }

        public string ReplyErrors
        {
            get
            {
                lock (_lock)
                {
                    return string.Join("; ", _replyErrors);
                }
            }
        }

        public (string? Corr, string? ReplyTo, string? Ttl) FirstRequest
        {
            get
            {
                lock (_lock)
                {
                    return (_firstCorr, _firstReplyTo, _firstTtl);
                }
            }
        }

        public bool ReceivedBody(string body)
        {
            lock (_lock)
            {
                return _received.Contains(body);
            }
        }

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            foreach (MessageExt m in msgs)
            {
                string body = BodyOf(m);
                lock (_lock)
                {
                    _received.Add(body);
                    if (body == "ping-1")
                    {
                        // S8 要用**投递到的**那条原始消息验 CLUSTER 是 broker 补的
                        _deliveredPing = m;
                    }

                    if (!_gotFirst)
                    {
                        _gotFirst = true;
                        _firstCorr = m.GetProperty(MessageConst.PropertyCorrelationId);
                        _firstReplyTo = m.GetProperty(MessageConst.PropertyReplyToClient);
                        _firstTtl = m.GetProperty(MessageConst.PropertyMessageTTL);
                    }
                }

                try
                {
                    Message reply = RequestReply.CreateReplyMessage(m, Str2Bytes("reply:" + body));
                    _producer.Send(reply);
                    lock (_lock)
                    {
                        _replied.Add(body);
                    }
                }
                catch (Exception e)
                {
                    lock (_lock)
                    {
                        _replyErrors.Add(e.GetType().Name + ": " + e.Message);
                    }
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    // ---------------- 独立订阅 <cluster>_REPLY_TOPIC 的观察者（S4 的硬证据） ----------------
    private sealed class CollectListener : IMessageListenerConcurrently
    {
        private readonly List<string> _got = new();
        private readonly object _lock = new();

        public IReadOnlyList<string> Got
        {
            get
            {
                lock (_lock)
                {
                    return _got.ToArray();
                }
            }
        }

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            lock (_lock)
            {
                foreach (MessageExt m in msgs)
                {
                    _got.Add(BodyOf(m));
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("Request-Reply live (.NET): namesrv=" + namesrv + " topic=" + Topic);
        Console.WriteLine("   应答 topic = " + ReplyTopic + "  cluster=" + Cluster);
        Console.WriteLine(new string('=', 70));

        // ---------------- S1 建请求 topic；应答 topic 由 broker 预注册（客户端不能建） ----------------
        Console.WriteLine();
        Console.WriteLine("S1 建请求 topic；应答 topic 由 broker 预注册（客户端不能建）");
        {
            var prep = new DefaultMQProducer("PG_RRPrepNet_" + Stamp)
            {
                NamesrvAddr = namesrv,
            };
            prep.Start();
            try
            {
                prep.CreateTopic("TBW102", Topic, QueueNum);
                try
                {
                    prep.CreateTopic("TBW102", ReplyTopic, QueueNum);
                    Check("S1 客户端 CreateTopic(应答 topic) 被 broker 拒绝", false, "竟然建成功了");
                }
                catch (Exception e)
                {
                    MQBrokerException? be = FindBrokerException(e);
                    int code = be?.ResponseCode ?? -1;
                    string desc = be?.ResponseMessage ?? string.Empty;
                    // CreateTopicInRoute 把 MQBrokerException 的 message 文本嵌进外层
                    // MQClientException（没把原异常作为 InnerException 透传），故也直接看文本。
                    // broker 拒绝系统 topic 的原文固定为「CODE: 29 ... conflict with system topic」。
                    string full = e.Message;
                    bool ok = (code == ResponseCode.InvalidParameter
                                  && desc.Contains("system topic", StringComparison.Ordinal))
                              || (full.Contains("CODE: 29", StringComparison.Ordinal)
                                  && full.Contains("system topic", StringComparison.Ordinal));
                    Check("S1 客户端 CreateTopic(应答 topic) 被拒（broker 系统 topic）",
                        ok, "code=" + (code >= 0 ? code.ToString(CultureInfo.InvariantCulture) : "?")
                        + " msg=" + full);
                }
            }
            finally
            {
                prep.Shutdown();
            }
        }

        var requester = new DefaultMQProducer(RequestGroup)
        {
            NamesrvAddr = namesrv,
            InstanceName = "RRReqNet",
        };
        requester.Start();
        // 请求方必须先被 broker 登记（心跳）才能被 REPLY_TO_CLIENT 反查到 channel，
        // 否则 broker 的 ReplyMessageProcessor 找不到 channel，回给应答方 SYSTEM_ERROR。
        // .NET 在 Request() 内部会补一次心跳（SendHeartbeatToAllBroker），
        // 这里仅确认实例已经起来、clientId 已生成（真正的注册证明是 S3 能拿到应答）。
        Check("S1 请求方已启动、clientId 已生成",
            requester.IsStarted && requester.ClientId.Length > 0, "clientId=" + requester.ClientId);

        var replierProducer = new DefaultMQProducer("PG_RRReplierNet_" + Stamp)
        {
            NamesrvAddr = namesrv,
            InstanceName = "RRReplierNet",
        };
        replierProducer.Start();
        var replier = new ReplierListener(replierProducer);
        var replierConsumer = new DefaultMQPushConsumer(ConsumerGroup)
        {
            InstanceName = "RRConsumerNet",
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        replierConsumer.SetNamesrvAddr(namesrv);
        replierConsumer.Subscribe(Topic, "*");
        replierConsumer.SetMessageListener(replier);
        replierConsumer.Start();

        // 观察者：独立订阅 <cluster>_REPLY_TOPIC，是 S4 的硬证据
        var watcher = new CollectListener();
        var watchConsumer = new DefaultMQPushConsumer(WatchGroup)
        {
            InstanceName = "RRWatchNet",
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        watchConsumer.SetNamesrvAddr(namesrv);
        watchConsumer.Subscribe(ReplyTopic, "*");
        watchConsumer.SetMessageListener(watcher);
        watchConsumer.Start();
        Check("S1 应答消费者 / 观察者已启动",
            replierConsumer.IsStarted && watchConsumer.IsStarted, "");

        try
        {
            // ---------------- S2/S3 一次完整往返 ----------------
            Console.WriteLine();
            Console.WriteLine("S2/S3 一次完整 request → reply 往返");
            var msg = new Message(Topic, Str2Bytes("ping-1"))
            {
                Keys = "rr",
            };
            Message reply = requester.Request(msg, RequestTimeout);

            string want = "reply:ping-1";
            Check("S3 Request() 拿到应答且 body 正确", BodyOf(reply) == want,
                "got=" + BodyOf(reply) + " want=" + want);
            Check("S3 应答 topic 是 <cluster>_REPLY_TOPIC", reply.Topic == ReplyTopic,
                "topic=" + reply.Topic);
            string? arrive = reply.GetProperty(MessageConst.PropertyReplyMessageArriveTime);
            Check("S3 应答带 REPLY_MESSAGE_ARRIVE_TIME（客户端收到时打的戳）",
                !string.IsNullOrEmpty(arrive) && arrive.All(char.IsDigit), "value=" + arrive);

            bool gotReq = WaitFor(() => replier.ReceivedCount >= 1, 15000);
            Check("S2 应答方收到请求消息", gotReq, "received=" + replier.ReceivedCount);
            if (gotReq)
            {
                (string? corr, string? replyTo, string? ttl) = replier.FirstRequest;
                Check("S2 请求消息带 CORRELATION_ID（uuid 字符串）",
                    !string.IsNullOrEmpty(corr) && corr.Length == 36, "corr=" + corr);
                Check("S2 请求消息带 REPLY_TO_CLIENT = 请求方 clientId",
                    replyTo == requester.ClientId, "replyTo=" + replyTo
                    + " clientId=" + requester.ClientId);
                Check("S2 请求消息带 TTL = timeout", ttl == RequestTimeout.ToString(CultureInfo.InvariantCulture),
                    "ttl=" + ttl);
            }

            // ---------------- S4 应答确实走了 325 且落到了 REPLY_TOPIC ----------------
            Console.WriteLine();
            Console.WriteLine("S4 应答走 SEND_REPLY_MESSAGE_V2(325) 且落到 <cluster>_REPLY_TOPIC");
            bool replied = WaitFor(() => replier.RepliedCount >= 1, 15000);
            Check("S4 应答方成功发出了应答消息", replied,
                "replied=" + replier.RepliedCount + " errors=" + replier.ReplyErrors);
            bool seen = WaitFor(() => watcher.Got.Contains(want), 15000);
            Check("S4 订阅 " + ReplyTopic + " 的独立消费者能看到该应答",
                seen, "watched=[" + string.Join(" ", watcher.Got) + "]");

            // ---------------- S5 无应答方 → RequestTimeoutException ----------------
            Console.WriteLine();
            Console.WriteLine("S5 无应答方时 Request() 抛 RequestTimeoutException");
            string silentTopic = Topic + "_NoReplier";
            {
                var prep2 = new DefaultMQProducer("PG_RRPrep2Net_" + Stamp)
                {
                    NamesrvAddr = namesrv,
                    InstanceName = "RRPrep2Net",
                };
                try
                {
                    prep2.Start();
                    prep2.CreateTopic("TBW102", silentTopic, QueueNum);
                }
                finally
                {
                    prep2.Shutdown();
                }
            }

            long t0 = NowMs();
            Exception? raised = null;
            try
            {
                var silent = new Message(silentTopic, Str2Bytes("nobody-home"))
                {
                    Keys = "rr-silent",
                };
                requester.Request(silent, 3000);
            }
            catch (Exception e)
            {
                raised = e;
            }

            long elapsed = NowMs() - t0;
            Check("S5 抛的是 RequestTimeoutException（不是静默返回/别的异常）",
                raised is RequestTimeoutException,
                "raised=" + (raised?.GetType().Name ?? "null") + ": " + raised?.Message);
            // Java 抛的是 RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, ...)：
            // 光有类型不够，调用方按 ResponseCode 分流时要知道"请求已经投出去了，只是没等到应答"
            // （对方可能只是慢），这跟发送本身失败是两类处置。
            Check("S5 异常带 10006 REQUEST_TIMEOUT_EXCEPTION",
                raised is MQClientException mce
                && mce.ResponseCode == ClientErrorCode.RequestTimeoutException,
                "code=" + (raised as MQClientException)?.ResponseCode);
            Check("S5 超时时长接近设定值（2s ~ 12s，说明真的等了而不是立即失败）",
                2000 <= elapsed && elapsed <= 12000, "elapsed=" + elapsed.ToString(CultureInfo.InvariantCulture) + "ms");

            // ---------------- S6 并发请求不串台 ----------------
            Console.WriteLine();
            Console.WriteLine("S6 并发 3 个 request，各自拿到自己的应答");
            var results = new System.Collections.Concurrent.ConcurrentDictionary<int, string>();
            var errors = new List<string>();
            var tasks = new List<Task>();
            for (int i = 0; i < 3; ++i)
            {
                int local = i;
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        var m = new Message(Topic, Str2Bytes("ping-conc-" + local.ToString(CultureInfo.InvariantCulture)))
                        {
                            Keys = "rr-conc",
                        };
                        Message r = requester.Request(m, RequestTimeout);
                        results[local] = BodyOf(r);
                    }
                    catch (Exception e)
                    {
                        lock (errors)
                        {
                            errors.Add("i=" + local.ToString(CultureInfo.InvariantCulture)
                                + " " + e.GetType().Name + ": " + e.Message);
                        }
                    }
                }));
            }

            Task.WaitAll(tasks.ToArray(), RequestTimeout + 5000);
            bool s6Ok = results.Count == 3 && errors.Count == 0;
            for (int i = 0; i < 3; ++i)
            {
                string expect = "reply:ping-conc-" + i.ToString(CultureInfo.InvariantCulture);
                if (!results.TryGetValue(i, out string? got) || got != expect)
                {
                    s6Ok = false;
                }
            }

            Check("S6 3 个并发 request 全部拿到应答且一一对应",
                s6Ok, "results=" + string.Join(",", results.OrderBy(k => k.Key)
                    .Select(k => k.Key.ToString(CultureInfo.InvariantCulture) + "=>" + k.Value))
                + " errors=" + string.Join("; ", errors));

            // ---------------- S7 消费者未被 Reply 流量破坏 ----------------
            Console.WriteLine();
            Console.WriteLine("S7 应答方的消费者仍然完好（普通 push 消费不受 Reply 影响）");
            var plain = new Message(Topic, Str2Bytes("plain-after-replies"))
            {
                Keys = "rr-plain",
            };
            requester.Send(plain);
            bool plainOk = WaitFor(() => replier.ReceivedBody("plain-after-replies"), 15000);
            Check("S7 后续普通消息仍被消费", plainOk,
                "received=" + replier.ReceivedCount);

            // ---------------- S8 CLUSTER 属性来自 broker；造不出应答时报 10007 ----------------
            // Java MessageUtil.createReplyMessage（:46/49）抛的是
            // MQClientException(ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION=10007, ...)。
            // 这条既验"错误码带上了"，也验它**为什么**存在：CLUSTER 是 broker 存储时补的
            // （SendMessageProcessor:318/614），客户端手里那份永远没有 ⇒ 应答必须建立在
            // **投递到的**那条消息上，用错对象就撞上 10007。
            Console.WriteLine();
            Console.WriteLine("S8 CLUSTER 由 broker 写入；造不出应答时报 10007 而不是别的错");
            MessageExt? delivered = replier.DeliveredPing;
            Check("S8 投递到的请求消息带 broker 写入的 CLUSTER=" + Cluster,
                delivered?.GetProperty(MessageConst.PropertyCluster) == Cluster,
                "cluster=" + delivered?.GetProperty(MessageConst.PropertyCluster));
            // GetProperty 对不存在的键返回空串（不是 null），所以这里判"空"而不是"null"；
            // 下面那条 10007 断言才是"这份确实没有 CLUSTER"的硬证据。
            Check("S8 客户端手里那份请求消息**没有** CLUSTER（属性确实是 broker 补的）",
                msg.GetProperty(MessageConst.PropertyCluster).Length == 0,
                "value=\"" + msg.GetProperty(MessageConst.PropertyCluster) + "\"");

            Exception? raised8 = null;
            try
            {
                RequestReply.CreateReplyMessage(msg, Str2Bytes("pong"));
            }
            catch (Exception e)
            {
                raised8 = e;
            }

            Check("S8 拿本地那份请求消息造应答 → MQClientException 带 10007",
                raised8 is MQClientException c8
                && c8.ResponseCode == ClientErrorCode.CreateReplyMessageException,
                "raised=" + (raised8?.GetType().Name ?? "null") + " code="
                + (raised8 as MQClientException)?.ResponseCode);
            Check("S8 10007 的文案点到缺失的 CLUSTER 属性（Java 原文）",
                raised8?.Message.Contains("property[" + MessageConst.PropertyCluster
                                          + "] is null.") == true,
                "msg=" + raised8?.Message);

            Exception? raised8b = null;
            try
            {
                RequestReply.CreateReplyMessage(null!, Str2Bytes("pong"));
            }
            catch (Exception e)
            {
                raised8b = e;
            }

            Check("S8 请求消息为 null → 同样是 10007（不是裸 NullReferenceException）",
                raised8b is MQClientException c8b
                && c8b.ResponseCode == ClientErrorCode.CreateReplyMessageException,
                "raised=" + (raised8b?.GetType().Name ?? "null") + " code="
                + (raised8b as MQClientException)?.ResponseCode);
        }
        finally
        {
            requester.Shutdown();
            replierConsumer.Shutdown();
            watchConsumer.Shutdown();
        }

        Console.WriteLine();
        Console.WriteLine("Request-Reply: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }
}
