// 发送头三个字段（defaultTopic / defaultTopicQueueNums / brokerName）真机联调
//（对应 python/verify_send_header_live.py、cpp/examples/live_send_header.cpp、
//  rust/examples/live_send_header.rs 的 H0–H5）。
//
// 离线单测（tests/SendRetryTests.cs）锁的是**上线形状**：V2 头的单字母键 c/d/n 有没有
// 真的写进 extFields、五种发送入口是不是带同一份值。但「字段上了线」和「broker 真拿它做
// 了决定」是两件事，后者只有真集群能证：
//   H1  默认值：不带任何配置发到新 topic，broker 按 min(d=4, TBW102.writeQueueNums) 建出
//       队列（TopicConfigManager.java:289），与 Java 客户端默认行为一致。
//   H2  DefaultTopicQueueNums=2 真的生效：建出来的 topic 只有 2 条队列。修之前这里写死 4，
//       这条必然变 4 —— 那是那个假 setter 唯一可观测的后果。
//   H3  CreateTopicKey=src 真的生效：先建一个带 PERM_INHERIT、3 条队列的模板 topic，再以
//       它为 c 发送 → 新 topic 继承模板的 3 条队列，而不是 TBW102 的 8 条。
//   H4  补上这三个字段之后，五种入口（同步 / 定点 / 单向 / 批量 320 / 异步）在真 broker
//       上仍逐条落地，条数一条不差。
//   H5  n（brokerName）：落点就是路由选中的那台 broker 名。⚠ 经典 broker 的发送链路里
//       没有 requestHeader.getBrokerName() 的读者（5.5.1 源码 grep 过），所以 n 在线上的
//       存在只能由离线抓帧证明，这里不假装能观测到它。
//
// 用法：rmq send-header [namesrv]（需本地 5.5.1 集群，autoCreateTopicEnable=true）
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveSendHeader
{
    private static string _namesrv = "127.0.0.1:9876";
    private static string _stamp = string.Empty;
    private static DefaultMQAdminExt _admin = null!;
    private static string _brokerAddr = string.Empty;
    private static string _brokerName = string.Empty;

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

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static void Sleep(int ms) => Thread.Sleep(ms);

    private static bool WaitUntil(Func<bool> pred, int seconds)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(seconds);
        for (;;)
        {
            if (pred()) return true;
            if (DateTime.UtcNow >= deadline) return false;
            Sleep(100);
        }
    }

    /// <summary>按 stamp 隔离的一组 topic；跑完 Cleanup 扫干净。</summary>
    private static readonly List<string> Topics = new();

    private static string Topic(string kind)
    {
        string t = "Hdr" + kind + "_" + _stamp;
        Topics.Add(t);
        return t;
    }

    /// <summary>一次异步发送的终态。</summary>
    private sealed class Latch : ISendCallback
    {
        private readonly object _lk = new();
        private readonly List<SendResult> _oks = new();
        private readonly List<Exception> _errors = new();

        public void OnSuccess(SendResult sendResult)
        {
            lock (_lk) _oks.Add(sendResult);
        }

        public void OnException(Exception error)
        {
            lock (_lk) _errors.Add(error);
        }

        public bool WaitDone(int ms) => WaitUntil(() =>
        {
            lock (_lk) return _oks.Count + _errors.Count > 0;
        }, ms / 1000);

        public bool FirstOk()
        {
            lock (_lk) return _oks.Count == 1 && _errors.Count == 0
                && _oks[0].SendStatus == SendStatus.SendOk;
        }

        public string Errors()
        {
            lock (_lk) return string.Join(";", _errors.Select(e => e.Message));
        }
    }

    /// <summary>
    /// 配好并启动一个生产者。instanceName 显式给定，clientId 才可预测；c/d 两个旋钮
    /// 传 null 表示保持默认（H1 就是拿默认值当基线的）。
    /// </summary>
    private sealed class ScopedProducer : IDisposable
    {
        private readonly DefaultMQProducer _producer;

        public ScopedProducer(string kind, string? createTopicKey = null,
            int? defaultTopicQueueNums = null)
        {
            _producer = new DefaultMQProducer("PID_send_header_" + _stamp)
            {
                NamesrvAddr = _namesrv,
                InstanceName = "hdr-" + kind + "-" + _stamp,
                SendMsgTimeout = 5000,
            };
            if (createTopicKey is not null) _producer.CreateTopicKey = createTopicKey;
            if (defaultTopicQueueNums is not null)
            {
                _producer.DefaultTopicQueueNums = defaultTopicQueueNums.Value;
            }
            _producer.Start();
        }

        public DefaultMQProducer Producer => _producer;

        public SendResult Send(string topic, string body) =>
            _producer.Send(new Message(topic, Bytes(body)), 5000);

        public void Dispose()
        {
            try
            {
                _producer.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] producer shutdown failed: " + e.Message);
            }
        }
    }

    /// <summary>读回 broker 上该 topic 的 (read, write) 队列数；还不存在返回 null。</summary>
    private static (int Read, int Write)? QueueNums(string topic)
    {
        try
        {
            TopicConfig cfg = _admin.ExamineTopicConfig(_brokerAddr, topic);
            return (cfg.ReadQueueNums, cfg.WriteQueueNums);
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>等 topic 在 broker 上出现（自动建 topic 在发送链路里同步做，落地要一点时间）。</summary>
    private static (int Read, int Write) WaitForTopic(string topic)
    {
        WaitUntil(() => QueueNums(topic) is not null, 10);
        return QueueNums(topic) ?? (-1, -1);
    }

    /// <summary>该 topic 全 broker 的落库条数（新 topic 的 minOffset 恒为 0）；读不到返回 -1。</summary>
    private static long TotalMessages(string topic)
    {
        try
        {
            TopicStatsTable stats = _admin.ExamineTopicStats(topic);
            long total = 0;
            foreach (KeyValuePair<MessageQueue, TopicOffset> kv in stats.OffsetTable)
            {
                total += kv.Value.MaxOffset - kv.Value.MinOffset;
            }
            return total;
        }
        catch (Exception)
        {
            return -1;
        }
    }

    // ------------------------------------------------- H0/H1/H2/H3 队列数
    private static void H0ToH3QueueNums()
    {
        // H0：TBW102 是这个 broker 上真正的「模板 topic」，H2/H3 的判据都依赖它的队列数
        (int Read, int Write)? tbw = QueueNums(MixAll.DefaultTopic);
        if (tbw is null || tbw.Value.Write <= 0)
        {
            Check("H0 TBW102 模板可读", false, "examineTopicConfig(TBW102) 读不到");
            return;
        }
        Check("H0 TBW102 模板可读", true, "read=" + tbw.Value.Read + " write=" + tbw.Value.Write);

        // H1：什么都不设，d 走 MixAll.DefaultTopicQueueNums(4)
        string t1 = Topic("Def");
        using (ScopedProducer p1 = new("h1"))
        {
            SendResult r1 = p1.Send(t1, "h1");
            Check("H1 默认配置发送成功", r1.SendStatus == SendStatus.SendOk,
                "msgId=" + r1.MsgId + " broker=" + r1.MessageQueue.BrokerName);
        }
        (int Read, int Write) n1 = WaitForTopic(t1);
        int want1 = Math.Min(MixAll.DefaultTopicQueueNums, tbw.Value.Write);
        Check("H1 默认 d=4 建的 topic 队列数=min(4, TBW102)",
            n1.Write == want1 && n1.Read == n1.Write,
            "read=" + n1.Read + " write=" + n1.Write + " (TBW102=" + tbw.Value.Write + ")");

        // H2：d=2 必须把队列数带下去——修之前这里写死 4
        string t2 = Topic("Nums");
        using (ScopedProducer p2 = new("h2", null, 2))
        {
            p2.Send(t2, "h2");
        }
        (int Read, int Write) n2 = WaitForTopic(t2);
        Check("H2 DefaultTopicQueueNums=2 建的 topic 只有 2 条队列",
            n2.Write == Math.Min(2, tbw.Value.Write) && n2.Read == n2.Write,
            "read=" + n2.Read + " write=" + n2.Write + "（写死 4 的旧行为会是 " + n1.Write + "）");

        // H3：c 指向自己的模板 topic。模板必须带 PERM_INHERIT，否则 TopicConfigManager:286
        // 的 isInherited 不通过，broker 直接拒绝自动建 topic
        string src = Topic("Src");
        try
        {
            _admin.CreateTopicInBroker(_brokerAddr, src, 3, 3,
                PermName.PermRead | PermName.PermWrite | PermName.PermInherit);
        }
        catch (Exception e)
        {
            Check("H3 模板 topic 创建", false, e.Message);
            return;
        }
        (int Read, int Write) nsrc = WaitForTopic(src);
        Check("H3 模板 topic 建好（3 条队列、带 INHERIT）",
            nsrc.Write == 3 && !(nsrc.Read == tbw.Value.Read && nsrc.Write == tbw.Value.Write),
            "src=(" + nsrc.Read + "," + nsrc.Write + ") tbw102=(" + tbw.Value.Read + "," +
            tbw.Value.Write + ")");

        string t3 = Topic("Inherit");
        using (ScopedProducer p3 = new("h3", src, 8))
        {
            p3.Send(t3, "h3");
        }
        (int Read, int Write) n3 = WaitForTopic(t3);
        Check("H3 createTopicKey=模板 topic 时被继承（min(8,3)=3 而不是 TBW102 的 " +
            tbw.Value.Write + "）", n3.Write == 3 && n3.Read == 3,
            "read=" + n3.Read + " write=" + n3.Write);
    }

    // ------------------------------------------------------------- H4 五种入口
    private static void H4SendEntries()
    {
        string t4 = Topic("Entries");
        Dictionary<string, string> landed = new();
        Latch latch = new();
        using (ScopedProducer p = new("h4"))
        {
            SendResult rs = p.Send(t4, "h4-sync");
            landed["sync"] = rs.SendStatus == SendStatus.SendOk ? "" : "status=" + rs.SendStatus;

            // 定点发送：显式给 mq，落在另一条调用链上（broker 名由调用方给）
            MessageQueue mq = rs.MessageQueue;
            SendResult rb = p.Producer.Send(new Message(t4, Bytes("h4-pinned")), mq, 5000);
            landed["pinned"] = rb.SendStatus == SendStatus.SendOk
                && rb.MessageQueue.BrokerName == mq.BrokerName
                ? "" : "status=" + rb.SendStatus + " broker=" + rb.MessageQueue.BrokerName;

            try
            {
                p.Producer.SendOneway(new Message(t4, Bytes("h4-oneway")));
                landed["oneway"] = "";  // 单向没有应答，只能靠后面的总数兜底
            }
            catch (Exception e)
            {
                landed["oneway"] = e.Message;
            }

            List<Message> batch = new();
            for (int i = 0; i < 3; i++) batch.Add(new Message(t4, Bytes("h4-batch-" + i)));
            SendResult rr = p.Producer.SendBatch(batch, 5000);
            landed["batch"] = rr.SendStatus == SendStatus.SendOk ? "" : "status=" + rr.SendStatus;

            p.Producer.SendAsync(new Message(t4, Bytes("h4-async")), latch, 5000);
            landed["async"] = latch.WaitDone(10000) && latch.FirstOk()
                ? "" : "回调没拿到唯一的 SEND_OK：" + latch.Errors();
        }

        foreach (string entry in new[] { "sync", "pinned", "oneway", "batch", "async" })
        {
            string why = landed.TryGetValue(entry, out string? v) ? v : "该入口没有记录";
            Check("H4 " + entry + " 入口发送成功", why.Length == 0, why);
        }
        // 1 同步 + 1 定点 + 1 单向 + 3 批量 + 1 异步 = 7 条
        bool got = WaitUntil(() => TotalMessages(t4) >= 7, 20);
        Check("H4 七条消息逐条落库（批量按子消息计）", got && TotalMessages(t4) == 7,
            "total=" + TotalMessages(t4));
    }

    // ------------------------------------------------------------- H5 brokerName
    private static void H5BrokerNameMatchesRoute()
    {
        string t5 = Topic("BrokerName");
        SendResult r5;
        using (ScopedProducer p = new("h5"))
        {
            r5 = p.Send(t5, "h5");
        }
        Check("H5 落点 broker 名与路由一致（n 就是它）",
            r5.SendStatus == SendStatus.SendOk && r5.MessageQueue.BrokerName == _brokerName,
            "落点=" + r5.MessageQueue.BrokerName + " 路由=" + _brokerName);
    }

    private static void Cleanup()
    {
        foreach (string t in Topics)
        {
            try
            {
                _admin.DeleteTopic(t);
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] DeleteTopic(" + t + ") failed: " + e.Message);
            }
        }
    }

    public static int Run(string[] args)
    {
        _namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        _stamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        Console.WriteLine("namesrv=" + _namesrv + " stamp=" + _stamp);

        _admin = new DefaultMQAdminExt("HDRDOTNETADMIN");
        _admin.SetNamesrvAddr(_namesrv);
        _admin.SetTimeoutMillis(10000);
        _admin.Start();

        try
        {
            // 集群探活：拿 broker 地址，并反查它的 brokerName（H5 的判据）
            ClusterInfo? cluster = null;
            WaitUntil(() =>
            {
                try
                {
                    cluster = _admin.FetchBrokerClusterInfo();
                    return cluster.BrokerAddrTable.Count > 0;
                }
                catch (Exception)
                {
                    return false;
                }
            }, 30);
            if (cluster is null || cluster.BrokerAddrTable.Count == 0)
            {
                Check("集群探活", false, "nameServer 无 broker 注册");
            }
            else
            {
                foreach (KeyValuePair<string, BrokerData> kv in cluster.BrokerAddrTable)
                {
                    _brokerAddr = kv.Value.SelectBrokerAddr();
                    _brokerName = kv.Key;
                    break;
                }
                Check("集群探活", _brokerAddr.Length > 0,
                    "broker=" + _brokerAddr + " name=" + _brokerName);

                H0ToH3QueueNums();
                H4SendEntries();
                H5BrokerNameMatchesRoute();
            }
        }
        catch (Exception e)
        {
            Check("联调异常", false, e.Message);
        }

        Cleanup();
        _admin.Shutdown();
        Console.WriteLine();
        Console.WriteLine("PASS=" + Volatile.Read(ref _pass) + " FAIL=" + Volatile.Read(ref _fail));
        return Volatile.Read(ref _fail) == 0 ? 0 : 1;
    }
}
