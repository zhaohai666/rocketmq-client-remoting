// Real-machine verification of send latency fault tolerance (sendLatencyFaultEnable)
// (S1-S5 verify the fault tolerance chain, S6 verifies how the send retry kernel classifies).
// Usage: rmq latency [namesrv]
using System.Diagnostics;
using System.Globalization;
using System.Net.Sockets;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveLatency
{
    private static readonly long WallNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private static readonly string Stamp = WallNowMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
    private static readonly string Topic = "LatencyNet_" + Stamp;
    private const string Broker = "broker-a";

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            _pass++;
            Console.WriteLine("  [PASS] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
        else
        {
            _fail++;
            Console.WriteLine("  [FAIL] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
    }

    /// <summary>
    /// Borrows a just-released local port as a "dead namesrv": it binds to get the port and
    /// then disposes at once, so connecting to it is refused (a real TCP stack, not a stub).
    /// </summary>
    private static string DeadLoopbackAddress()
    {
        var probe = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        string addr = ((System.Net.IPEndPoint)probe.LocalEndPoint!).ToString();
        probe.Dispose();
        return addr;
    }

    private static int SendOk(DefaultMQProducer producer, int count, string prefix)
    {
        int ok = 0;
        for (int i = 0; i < count; ++i)
        {
            try
            {
                producer.Send(new Message(Topic, System.Text.Encoding.UTF8.GetBytes(prefix + "-" + i)));
                ok++;
            }
            catch (Exception e)
            {
                Console.WriteLine("    send " + i + " failed: " + e.Message);
            }
        }
        return ok;
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("LatencyFaultTolerance live (C#): namesrv=" + namesrv + " topic=" + Topic);
        Console.WriteLine(new string('=', 70));

        var prep = new DefaultMQProducer("PG_LatPrepNet_" + Stamp) { NamesrvAddr = namesrv };
        prep.Start();
        try
        {
            prep.CreateTopic("TBW102", Topic, 4);
        }
        catch (Exception e)
        {
            Console.WriteLine("  pre-creating the topic failed (falling back to auto-creation): " + e.Message);
        }
        Thread.Sleep(3000);
        prep.Shutdown();

        // ---------------- S1 off by default ----------------
        Console.WriteLine();
        Console.WriteLine("S1 off by default: behaviour unchanged, no fault tolerance table recorded");
        var p1 = new DefaultMQProducer("PG_LatOffNet_" + Stamp) { NamesrvAddr = namesrv };
        Check("S1 SendLatencyFaultEnable=false by default", !p1.SendLatencyFaultEnable);
        p1.Start();
        Check("S1 with it off all 10 sends succeed", SendOk(p1, 10, "off") == 10);
        Check("S1 with it off no fault tolerance table is recorded",
              p1.MqFaultStrategy.LatencyFaultTolerance.GetFaultItem(Broker) == null);
        p1.Shutdown();

        // ---------------- S2/S3 the healthy path once enabled ----------------
        Console.WriteLine();
        Console.WriteLine("S2/S3 fault avoidance enabled: a healthy broker sends normally and the latency is recorded");
        var p2 = new DefaultMQProducer("PG_LatOnNet_" + Stamp) { NamesrvAddr = namesrv };
        p2.SendLatencyFaultEnable = true;
        Check("S2 the switch takes effect", p2.SendLatencyFaultEnable);
        p2.Start();
        Check("S2 with it on all 20 sends succeed", SendOk(p2, 20, "on") == 20);
        FaultItem? item = p2.MqFaultStrategy.LatencyFaultTolerance.GetFaultItem(Broker);
        Check("S3 the fault tolerance table recorded broker-a", item != null);
        if (item != null)
        {
            Check("S3 the recorded measured latency is > 0", item.CurrentLatency > 0.0,
                  "latency=" + item.CurrentLatency.ToString("F1", System.Globalization.CultureInfo.InvariantCulture) + "ms");
            Check("S3 the healthy broker is still available", item.IsAvailable() && item.IsReachable());
            Check("S3 the latency is below the first tier threshold (no isolation triggered)", item.StartTimestamp == 0);
        }

        // ---------------- S4 inject an isolation -> the degradation chain ----------------
        Console.WriteLine();
        Console.WriteLine("S4 inject an isolation: the available->reachable->plain round-robin degradation chain still sends");
        p2.MqFaultStrategy.UpdateFaultItem(Broker, 99999.0, isolation: true, reachable: false);
        Check("S4 broker-a is unavailable after the injection",
              !p2.MqFaultStrategy.LatencyFaultTolerance.IsAvailable(Broker));
        Check("S4 with a single isolated broker, degraded round-robin still sends 5", SendOk(p2, 5, "iso") == 5);

        // ---------------- S5 recovery when the isolation expires ----------------
        Console.WriteLine();
        Console.WriteLine("S5 recovery when the isolation expires (remove then inject 2000ms)");
        p2.MqFaultStrategy.LatencyFaultTolerance.Remove(Broker);
        p2.MqFaultStrategy.LatencyFaultTolerance.UpdateFaultItem(Broker, 1.0, 2000, true);
        Check("S5 unavailable during the isolation", !p2.MqFaultStrategy.LatencyFaultTolerance.IsAvailable(Broker));
        Thread.Sleep(2300);
        Check("S5 available again once it expires", p2.MqFaultStrategy.LatencyFaultTolerance.IsAvailable(Broker));
        Check("S5 sending works again after the recovery", SendOk(p2, 3, "after") == 3);
        p2.Shutdown();

        // ---------------- S6 the send retry kernel ----------------
        Console.WriteLine();
        Console.WriteLine("S6 the send retry kernel (sendDefaultImpl): the config surface + the classification when there is no route");
        var p3 = new DefaultMQProducer("PG_LatRetryNet_" + Stamp) { NamesrvAddr = namesrv };
        p3.Start();
        Check("S6 the default retryable response codes are exactly the 8 agreed by the protocol",
            p3.IsRetryResponseCode(ResponseCode.SystemError)
            && p3.IsRetryResponseCode(ResponseCode.SystemBusy)
            && p3.IsRetryResponseCode(ResponseCode.ServiceNotAvailable)
            && p3.IsRetryResponseCode(ResponseCode.NoPermission)
            && p3.IsRetryResponseCode(ResponseCode.TopicNotExist)
            && p3.IsRetryResponseCode(ResponseCode.NoBuyerId)
            && p3.IsRetryResponseCode(ResponseCode.NotInCurrentUnit)
            && p3.IsRetryResponseCode(ResponseCode.GoAway)
            && p3.RetryResponseCodes.Count == 8
            // never getting a response code (the connection was never made) counts as non-retryable; a code not in the set is non-retryable too
            && !p3.IsRetryResponseCode(null)
            && !p3.IsRetryResponseCode(ResponseCode.MessageIllegal));

        // Both the per-attempt timeout cap and "switch broker on non-SEND_OK" are enabled:
        // a healthy broker still gives SEND_OK, showing that the cap and the switch did not
        // hurt the normal path (the switch-broker branch itself only runs when the broker
        // replies FLUSH_DISK_TIMEOUT, which a real cluster cannot produce, so it stays in
        // SendRetryTests' in-process fake cluster for cross-checking).
        p3.AddRetryResponseCode(ResponseCode.MessageIllegal);
        p3.SendMsgMaxTimeoutPerRequest = 2000;
        p3.RetryAnotherBrokerWhenNotStoreOk = true;
        Check("S6 the switch reads back", p3.RetryAnotherBrokerWhenNotStoreOk
                              && p3.SendMsgMaxTimeoutPerRequest == 2000
                              && p3.IsRetryResponseCode(ResponseCode.MessageIllegal));
        int capped = 0;
        for (int i = 0; i < 10; ++i)
        {
            try
            {
                if (p3.Send(new Message(Topic, System.Text.Encoding.UTF8.GetBytes("retry-" + i)))
                        .SendStatus == SendStatus.SendOk)
                {
                    capped++;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("    send " + i + " failed: " + e.Message);
            }
        }

        Check("S6 with the cap and the non-SEND_OK switch all 10 are still SEND_OK", capped == 10,
              "ok=" + capped.ToString(CultureInfo.InvariantCulture) + "/10");
        p3.Shutdown();

        // The route cannot be fetched at all: our own route refresh folds the transport
        // failure into "cannot get the route", so it is classified as NOT_FOUND_TOPIC(10005),
        // and not a single broker was contacted and the retry count was not burned for nothing.
        string deadAddr = DeadLoopbackAddress();
        var orphan = new DefaultMQProducer("PG_LatOrphanNet_" + Stamp) { NamesrvAddr = deadAddr };
        orphan.Start();
        var watch = Stopwatch.StartNew();
        MQClientException? routeErr = null;
        try
        {
            orphan.Send(new Message("LatencyNoRoute_" + Stamp, System.Text.Encoding.UTF8.GetBytes("x")),
                2000);
        }
        catch (MQClientException e)
        {
            routeErr = e;
        }
        finally
        {
            watch.Stop();
        }

        Check("S6 with no route it is classified as NOT_FOUND_TOPIC without burning retries",
            routeErr is not null
            && routeErr.ResponseCode == ClientErrorCode.NotFoundTopicException
            && !routeErr.Message.Contains("Send [", StringComparison.Ordinal)
            && watch.ElapsedMilliseconds < 5000,
            "elapsed=" + watch.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture)
            + "ms err=" + (routeErr?.Message ?? "<none>"));
        orphan.Shutdown();

        Console.WriteLine();
        Console.WriteLine("== result: PASS=" + _pass + " FAIL=" + _fail + " ==");
        return _fail == 0 ? 0 : 1;
    }
}
