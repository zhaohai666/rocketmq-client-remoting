// TLS + traceparent 真机联调（对应 cpp live_tls.cpp / run_tls_live.sh）。
//
// 验证点（单进程闭环）：
//   0. 传输层：逐轮新建 TLS 连接的首包必须落地；一条 TLS 连接上并发请求必须全部拿到应答
//      并且 opaque 逐笔对上（.NET 的读线程走 SslStream.Read、调用方走 SslStream.Write，
//      两者必然在同一条会话上交叠）；
//   1. TLS 生产者/消费者（test-mode，信任 broker 自签证书）真连真发真收；
//   2. 生产侧注入的 traceparent 属性随消息走完整链路，消费侧可提取且合法。
//
// 用法：dotnet run ... -- tls <namesrv> <topic> <group>
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

internal static class LiveTls
{
    // ---- S0 传输层压力的口径 ----
    // nameServer 对不存在的 topic 回 TOPIC_NOT_EXIST(17) 也算"首包落地"；等满超时才算丢。
    private const int FirstPacketRounds = 30;
    private const long FirstPacketBudgetMs = 1500;
    private const int ConcurrentThreads = 16;
    private const int ConcurrentPerThread = 20;
    // 320 笔并发在 loopback 上实测几百毫秒；预算给 10s，真被会话串行化卡到就是几十秒量级。
    private const long ConcurrentBudgetMs = 10000;

    private static RemotingCommand RouteRequest(string topic) =>
        RemotingCommand.CreateRequestCommand(RequestCode.GetRouteinfoByTopic,
            new GetRouteInfoRequestHeader { Topic = topic });

    /// <summary>
    /// 直接用 RemotingClient 压 TLS 会话的读写线程口径：S0a 每轮新建连接打首包，
    /// S0b 一条连接上多线程并发（读线程与写线程彻底交叠）并要求 opaque 逐笔对上。
    /// </summary>
    private static bool RunTransportStress(string namesrv)
    {
        bool ok = true;

        int lost = 0;
        long worstMs = 0;
        for (int i = 0; i < FirstPacketRounds; i++)
        {
            var client = new RemotingClient(tlsEnable: true);
            Stopwatch sw = Stopwatch.StartNew();
            try
            {
                client.InvokeSync(namesrv, RouteRequest("TlsLiveFirstPacket_" + i), 5000);
                long ms = sw.ElapsedMilliseconds;
                if (ms > worstMs) worstMs = ms;
                if (ms > FirstPacketBudgetMs)
                {
                    lost++;
                    Console.WriteLine("  round " + i + " 首包慢到 " + ms + "ms");
                }
            }
            catch (Exception e)
            {
                lost++;
                Console.WriteLine("  round " + i + " 首包失败 (" + sw.ElapsedMilliseconds + "ms): "
                    + e.Message);
            }
            finally
            {
                client.Dispose();
            }
        }

        Console.WriteLine("  [" + (lost == 0 ? "PASS" : "FAIL") + "] S0a " + FirstPacketRounds
            + " 轮新建 TLS 连接首包全部落地  lost=" + lost + " worst=" + worstMs + "ms");
        ok &= lost == 0;

        var shared = new RemotingClient(tlsEnable: true);
        int failures = 0;
        Stopwatch all = Stopwatch.StartNew();
        Task[] workers = new Task[ConcurrentThreads];
        for (int t = 0; t < ConcurrentThreads; t++)
        {
            int thread = t;
            workers[t] = Task.Run(() =>
            {
                for (int i = 0; i < ConcurrentPerThread; i++)
                {
                    RemotingCommand req =
                        RouteRequest("TlsLiveConcurrent_" + thread + "_" + i);
                    try
                    {
                        RemotingCommand resp = shared.InvokeSync(namesrv, req, 5000);
                        // opaque 错号 = 响应串台到别的请求，和丢一样严重
                        if (resp.Opaque != req.Opaque)
                        {
                            Interlocked.Increment(ref failures);
                            Console.WriteLine("  thread " + thread + " req " + i
                                + " 响应串台: sent=" + req.Opaque + " got=" + resp.Opaque);
                        }
                    }
                    catch (Exception e)
                    {
                        Interlocked.Increment(ref failures);
                        Console.WriteLine("  thread " + thread + " req " + i + " 失败: " + e.Message);
                    }
                }
            });
        }

        Task.WaitAll(workers);
        long concurrentMs = all.ElapsedMilliseconds;
        shared.Dispose();
        bool concurrentOk = failures == 0 && concurrentMs < ConcurrentBudgetMs;
        Console.WriteLine("  [" + (concurrentOk ? "PASS" : "FAIL") + "] S0b 单条 TLS 连接 "
            + ConcurrentThreads + " 线程并发 " + ConcurrentThreads * ConcurrentPerThread
            + " 笔全部对上号  fail=" + failures + " elapsed=" + concurrentMs + "ms");
        return ok && concurrentOk;
    }

    public static int Run(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("usage: tls <namesrv> <topic> <group>");
            return 2;
        }

        string namesrv = args[0];
        string topic = args[1];
        string group = args[2];

        // 0) 传输层压力放在最前：只要一个能建 TLS 连接的 nameServer 地址，不依赖 topic
        Console.WriteLine("=== S0 TLS 传输层 ===");
        bool transportOk = RunTransportStress(namesrv);

        // 1) 明文生产者预建 topic（约定：先建 topic 再起消费者）
        var prep = new DefaultMQProducer("GID_TLS_PREP");
        prep.NamesrvAddr = namesrv;
        prep.Start();
        try
        {
            prep.CreateTopic("init", topic, 4);
        }
        catch (Exception e)
        {
            Console.WriteLine("  预建 topic 失败（改用自动创建）: " + e.Message);
        }

        Thread.Sleep(3000);

        // 2) TLS 消费者先起
        var listener = new CountListener();
        var consumer = new DefaultMQPushConsumer(group);
        consumer.SetNamesrvAddr(namesrv);
        consumer.TlsEnable = true;
        consumer.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();

        // 3) TLS 生产者发送 3 条（trace 注入开启）
        var producer = new DefaultMQProducer("GID_TLS_PROD");
        producer.NamesrvAddr = namesrv;
        producer.TlsEnable = true;
        producer.EnableTraceContext = true;
        producer.Start();

        int sent = 0;
        for (int i = 0; i < 3; i++)
        {
            var result = producer.Send(new Message(topic, System.Text.Encoding.UTF8.GetBytes("tls-live-" + i)));
            if (result.SendStatus == SendStatus.SendOk)
            {
                sent++;
            }
        }

        // 4) 等收齐
        for (int i = 0; i < 80 && listener.Count() < sent; i++)
        {
            Thread.Sleep(250);
        }

        producer.Shutdown();

        long got = listener.Count();
        string tp = listener.LastTraceparent();
        bool traceOk = !string.IsNullOrEmpty(tp) && TraceParentContext.IsValid(tp);

        consumer.Shutdown();
        prep.Shutdown();

        bool ok = sent == 3 && got == 3 && traceOk && transportOk;
        Console.WriteLine("sent=" + sent + " consumed=" + got
            + " traceparent=" + (traceOk ? tp : "<missing/invalid>"));
        Console.WriteLine("  props=" + listener.LastProps());
        Console.WriteLine(ok ? "  [PASS]" : "  [FAIL]");
        return ok ? 0 : 1;
    }

    private sealed class CountListener : IMessageListenerConcurrently
    {
        private readonly object _lock = new();
        private long _count;
        private string _lastTraceparent = string.Empty;
        private string _lastProps = string.Empty;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext ctx)
        {
            foreach (MessageExt m in msgs)
            {
                string tp = m.GetProperty(TraceParentContext.TraceContextProperty);
                lock (_lock)
                {
                    _count++;
                    _lastTraceparent = tp;
                    if (_lastProps.Length == 0 && m.Properties != null)
                    {
                        foreach (var kv in m.Properties)
                        {
                            _lastProps += kv.Key + "=" + kv.Value + ";";
                        }
                    }
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public long Count()
        {
            lock (_lock) return _count;
        }

        public string LastTraceparent()
        {
            lock (_lock) return _lastTraceparent;
        }

        public string LastProps()
        {
            lock (_lock) return _lastProps;
        }
    }
}
