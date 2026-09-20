// 发送延迟故障容错（sendLatencyFaultEnable）真机验证
// （对应 python/verify_latency_live.py、cpp/examples/live_latency.cpp，同一套 S1–S5；
//  S6 是发送重试内核的检查，另两种语言由各自的 live 用例锁定）。
// 用法：rmq latency [namesrv]
//
// 场景（单 broker broker-a / DefaultCluster）：
//   S1 默认关闭：发 10 条全 OK（行为不变，容错表不记录）
//   S2 开启故障规避：发 20 条全 OK（broker 健康，不触发隔离）
//   S3 成功发送后容错表有记录：broker-a 的 FaultItem.CurrentLatency > 0 且可用/可达
//   S4 手工注入隔离（broker-a，隔离档位 10000ms）→ 再发送：单 broker 下走
//      available→reachable→普通轮询 的退化链仍全部发出
//   S5 隔离到期恢复：remove 后注入 2000ms 隔离，立即不可用，≈2s 后恢复可用
//   S6 发送重试内核：可重试码集合与 Java 对齐、单次超时上限与「非 SEND_OK 换 broker」
//      开关不影响真集群上的正常发送、彻底拿不到路由时按 NOT_FOUND_TOPIC 定性而非空转重试
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
    /// 借一个刚释放的本地端口当"死 namesrv"：bind 拿到端口后立刻 Dispose，
    /// 再连它就是连接被拒（真 TCP 栈，不是打桩）。
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
        Console.WriteLine("LatencyFaultTolerance live (.NET): namesrv=" + namesrv + " topic=" + Topic);
        Console.WriteLine(new string('=', 70));

        var prep = new DefaultMQProducer("PG_LatPrepNet_" + Stamp) { NamesrvAddr = namesrv };
        prep.Start();
        try
        {
            prep.CreateTopic("TBW102", Topic, 4);
        }
        catch (Exception e)
        {
            Console.WriteLine("  预建 topic 失败（改用自动创建）: " + e.Message);
        }
        Thread.Sleep(3000);
        prep.Shutdown();

        // ---------------- S1 默认关闭 ----------------
        Console.WriteLine();
        Console.WriteLine("S1 默认关闭：行为不变，不记录容错表");
        var p1 = new DefaultMQProducer("PG_LatOffNet_" + Stamp) { NamesrvAddr = namesrv };
        Check("S1 默认 SendLatencyFaultEnable=false", !p1.SendLatencyFaultEnable);
        p1.Start();
        Check("S1 关闭时发 10 条全部成功", SendOk(p1, 10, "off") == 10);
        Check("S1 关闭时不记录容错表",
              p1.MqFaultStrategy.LatencyFaultTolerance.GetFaultItem(Broker) == null);
        p1.Shutdown();

        // ---------------- S2/S3 开启后健康路径 ----------------
        Console.WriteLine();
        Console.WriteLine("S2/S3 开启故障规避：健康 broker 正常发送并记录延迟");
        var p2 = new DefaultMQProducer("PG_LatOnNet_" + Stamp) { NamesrvAddr = namesrv };
        p2.SendLatencyFaultEnable = true;
        Check("S2 开关生效", p2.SendLatencyFaultEnable);
        p2.Start();
        Check("S2 开启后发 20 条全部成功", SendOk(p2, 20, "on") == 20);
        FaultItem? item = p2.MqFaultStrategy.LatencyFaultTolerance.GetFaultItem(Broker);
        Check("S3 容错表已记录 broker-a", item != null);
        if (item != null)
        {
            Check("S3 记录的实测延迟 > 0", item.CurrentLatency > 0.0,
                  "latency=" + item.CurrentLatency.ToString("F1", System.Globalization.CultureInfo.InvariantCulture) + "ms");
            Check("S3 健康 broker 仍可用", item.IsAvailable() && item.IsReachable());
            Check("S3 延迟低于第一档阈值（未触发隔离）", item.StartTimestamp == 0);
        }

        // ---------------- S4 注入隔离 → 退化链 ----------------
        Console.WriteLine();
        Console.WriteLine("S4 注入隔离：available→reachable→普通轮询 退化链仍能发出");
        p2.MqFaultStrategy.UpdateFaultItem(Broker, 99999.0, isolation: true, reachable: false);
        Check("S4 注入后 broker-a 不可用",
              !p2.MqFaultStrategy.LatencyFaultTolerance.IsAvailable(Broker));
        Check("S4 隔离中单 broker 退化轮询仍发出 5 条", SendOk(p2, 5, "iso") == 5);

        // ---------------- S5 隔离到期恢复 ----------------
        Console.WriteLine();
        Console.WriteLine("S5 隔离到期恢复（remove 后注入 2000ms）");
        // 注意：S4 注入的是隔离档位 10000ms，而 UpdateNotAvailableDuration **只延长不缩短**
        // （Java 语义），必须先 Remove 才能注入更短的 2000ms 档。
        p2.MqFaultStrategy.LatencyFaultTolerance.Remove(Broker);
        p2.MqFaultStrategy.LatencyFaultTolerance.UpdateFaultItem(Broker, 1.0, 2000, true);
        Check("S5 隔离期内不可用", !p2.MqFaultStrategy.LatencyFaultTolerance.IsAvailable(Broker));
        Thread.Sleep(2300);
        Check("S5 到期后恢复可用", p2.MqFaultStrategy.LatencyFaultTolerance.IsAvailable(Broker));
        Check("S5 恢复后发送正常", SendOk(p2, 3, "after") == 3);
        p2.Shutdown();

        // ---------------- S6 发送重试内核 ----------------
        Console.WriteLine();
        Console.WriteLine("S6 发送重试内核（sendDefaultImpl）：配置面 + 没有路由时的定性");
        var p3 = new DefaultMQProducer("PG_LatRetryNet_" + Stamp) { NamesrvAddr = namesrv };
        p3.Start();
        Check("S6 默认可重试响应码就是 Java 那 8 个",
            p3.IsRetryResponseCode(ResponseCode.SystemError)
            && p3.IsRetryResponseCode(ResponseCode.SystemBusy)
            && p3.IsRetryResponseCode(ResponseCode.ServiceNotAvailable)
            && p3.IsRetryResponseCode(ResponseCode.NoPermission)
            && p3.IsRetryResponseCode(ResponseCode.TopicNotExist)
            && p3.IsRetryResponseCode(ResponseCode.NoBuyerId)
            && p3.IsRetryResponseCode(ResponseCode.NotInCurrentUnit)
            && p3.IsRetryResponseCode(ResponseCode.GoAway)
            && p3.RetryResponseCodes.Count == 8
            // 没等到响应码（压根没连上）等于不可重试；不在集合里的码也不可重试
            && !p3.IsRetryResponseCode(null)
            && !p3.IsRetryResponseCode(ResponseCode.MessageIllegal));

        // 单次超时上限 + 「非 SEND_OK 换 broker」都打开：健康的 broker 上照常 SEND_OK，
        // 说明钳位与开关没有误伤正常路径（换 broker 分支本身要 broker 回 FLUSH_DISK_TIMEOUT
        // 才走得到，真集群造不出来，留在 SendRetryTests 的进程内假集群里对拍）。
        p3.AddRetryResponseCode(ResponseCode.MessageIllegal);
        p3.SendMsgMaxTimeoutPerRequest = 2000;
        p3.RetryAnotherBrokerWhenNotStoreOk = true;
        Check("S6 开关可读回", p3.RetryAnotherBrokerWhenNotStoreOk
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

        Check("S6 上限与非 SEND_OK 开关下 10 条仍然全 SEND_OK", capped == 10,
              "ok=" + capped.ToString(CultureInfo.InvariantCulture) + "/10");
        p3.Shutdown();

        // 路由彻底拉不到：本项目的路由刷新把传输层失败并入「拿不到路由」，所以定性为
        // NOT_FOUND_TOPIC(10005)，而且一次 broker 都没联系、重试次数没被空转掉。
        // （Python/Rust 那条链把连接失败原样抛出，语义不同，各自的 live 用例分别锁定。）
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

        Check("S6 没有路由时按 NOT_FOUND_TOPIC 定性且不空转重试",
            routeErr is not null
            && routeErr.ResponseCode == ClientErrorCode.NotFoundTopicException
            && !routeErr.Message.Contains("Send [", StringComparison.Ordinal)
            && watch.ElapsedMilliseconds < 5000,
            "elapsed=" + watch.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture)
            + "ms err=" + (routeErr?.Message ?? "<none>"));
        orphan.Shutdown();

        Console.WriteLine();
        Console.WriteLine("== 结果: PASS=" + _pass + " FAIL=" + _fail + " ==");
        return _fail == 0 ? 0 : 1;
    }
}
