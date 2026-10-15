// Real-machine integration of TLS + traceparent.
//
// What is verified (a single-process loop):
//   0. Transport layer: the first packet of every freshly created TLS connection must land;
//      concurrent requests on one TLS connection must all get responses with their opaque
//      matched one by one (C#'s reader thread goes through SslStream.Read and the callers
//      through SslStream.Write, so the two inevitably interleave on the same session);
//   1. A TLS producer/consumer (test-mode, trusting the broker's self-signed certificate)
//      really connects, sends and receives;
//   2. The traceparent property injected on the producer side travels the whole chain and
//      can be extracted and is legal on the consumer side.
//
// Usage: dotnet run ... -- tls <namesrv> <topic> <group>
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
    // ---- the rule of thumb for S0 transport stress ----
    // The nameServer replying TOPIC_NOT_EXIST(17) for a non-existent topic also counts as
    // "the first packet landed"; only waiting out the timeout counts as a loss.
    private const int FirstPacketRounds = 30;
    private const long FirstPacketBudgetMs = 1500;
    private const int ConcurrentThreads = 16;
    private const int ConcurrentPerThread = 20;
    // 320 concurrent requests measured in a few hundred milliseconds on loopback; the
    // budget is 10s, and if the session really got serialised it would be tens of seconds.
    private const long ConcurrentBudgetMs = 10000;

    private static RemotingCommand RouteRequest(string topic) =>
        RemotingCommand.CreateRequestCommand(RequestCode.GetRouteinfoByTopic,
            new GetRouteInfoRequestHeader { Topic = topic });

    /// <summary>
    /// Drives the TLS session's reader/writer thread rule directly with RemotingClient:
    /// S0a creates a connection each round and sends a first packet, S0b runs many threads
    /// concurrently on one connection (the reader and writer threads interleave completely)
    /// and requires the opaque of every request to match.
    /// </summary>
    private static bool RunTransportStress(string namesrv, TlsOptions? tlsOptions)
    {
        bool ok = true;

        int lost = 0;
        long worstMs = 0;
        for (int i = 0; i < FirstPacketRounds; i++)
        {
            var client = new RemotingClient(tlsEnable: true, tlsOptions: tlsOptions);
            Stopwatch sw = Stopwatch.StartNew();
            try
            {
                client.InvokeSync(namesrv, RouteRequest("TlsLiveFirstPacket_" + i), 5000);
                long ms = sw.ElapsedMilliseconds;
                if (ms > worstMs) worstMs = ms;
                if (ms > FirstPacketBudgetMs)
                {
                    lost++;
                    Console.WriteLine("  round " + i + " first packet took " + ms + "ms");
                }
            }
            catch (Exception e)
            {
                lost++;
                Console.WriteLine("  round " + i + " first packet failed (" + sw.ElapsedMilliseconds + "ms): "
                    + e.Message);
            }
            finally
            {
                client.Dispose();
            }
        }

        Console.WriteLine("  [" + (lost == 0 ? "PASS" : "FAIL") + "] S0a " + FirstPacketRounds
            + " rounds of first packets on fresh TLS connections all landed  lost=" + lost + " worst=" + worstMs + "ms");
        ok &= lost == 0;

        var shared = new RemotingClient(tlsEnable: true, tlsOptions: tlsOptions);
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
                        // a mismatched opaque = the response crossed wires onto another request, as bad as a loss
                        if (resp.Opaque != req.Opaque)
                        {
                            Interlocked.Increment(ref failures);
                            Console.WriteLine("  thread " + thread + " req " + i
                                + " a response crossed wires: sent=" + req.Opaque + " got=" + resp.Opaque);
                        }
                    }
                    catch (Exception e)
                    {
                        Interlocked.Increment(ref failures);
                        Console.WriteLine("  thread " + thread + " req " + i + " failed: " + e.Message);
                    }
                }
            });
        }

        Task.WaitAll(workers);
        long concurrentMs = all.ElapsedMilliseconds;
        shared.Dispose();
        bool concurrentOk = failures == 0 && concurrentMs < ConcurrentBudgetMs;
        Console.WriteLine("  [" + (concurrentOk ? "PASS" : "FAIL") + "] S0b one TLS connection, "
            + ConcurrentThreads + " threads concurrently, all " + ConcurrentThreads * ConcurrentPerThread
            + " requests matched  fail=" + failures + " elapsed=" + concurrentMs + "ms");
        return ok && concurrentOk;
    }

    public static int Run(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("usage: tls <namesrv> <topic> <group> [leg] [caCert] [clientCert] [clientKey]");
            return 2;
        }

        string namesrv = args[0];
        string topic = args[1];
        string group = args[2];
        // three legs:
        //   plain     tlsEnable=True, trusting the self-signed certificate (test-mode)
        //   ca_verify + CaCert -- strict certificate chain + hostname SAN verification
        //   mtls      ca_verify + a client certificate (broker -Dtls.server.authClient=true)
        string leg = args.Length > 3 ? args[3] : "plain";
        TlsOptions? tlsOptions = null;
        if (leg is "ca_verify" or "mtls")
        {
            tlsOptions = new TlsOptions
            {
                CaCert = args.Length > 5 ? args[5] : "",
                ServerName = "127.0.0.1",
            };
            if (leg == "mtls")
            {
                tlsOptions.ClientCert = args.Length > 6 ? args[6] : "";
                tlsOptions.ClientKey = args.Length > 7 ? args[7] : "";
            }
        }
        Console.WriteLine("=== C# TLS live: leg=" + leg + " topic=" + topic + " ===");

        // 0) transport stress goes first: it only needs a nameServer address that can
        //    establish a TLS connection, it does not depend on the topic
        Console.WriteLine("=== S0 TLS transport layer ===");
        bool transportOk = RunTransportStress(namesrv, tlsOptions);

        // 1) the plaintext producer pre-creates the topic (the convention: create the topic
        //    before starting the consumer). On a strictly verified cluster both the namesrv
        //    and the broker are TLS, so this prep producer goes over TLS too.
        var prep = new DefaultMQProducer("GID_TLS_PREP")
        {
            NamesrvAddr = namesrv,
            TlsEnable = true,
            TlsOptions = tlsOptions,
        };
        prep.Start();
        try
        {
            prep.CreateTopic("init", topic, 4);
        }
        catch (Exception e)
        {
            Console.WriteLine("  pre-creating the topic failed (falling back to auto-creation): " + e.Message);
        }

        Thread.Sleep(3000);

        // 2) the TLS consumer starts first
        var listener = new CountListener();
        var consumer = new DefaultMQPushConsumer(group);
        consumer.SetNamesrvAddr(namesrv);
        consumer.TlsEnable = true;
        consumer.TlsOptions = tlsOptions;
        consumer.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();

        // 3) the TLS producer sends 3 messages (trace injection on)
        var producer = new DefaultMQProducer("GID_TLS_PROD");
        producer.NamesrvAddr = namesrv;
        producer.TlsEnable = true;
        producer.TlsOptions = tlsOptions;
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

        // 4) wait until everything is received
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
