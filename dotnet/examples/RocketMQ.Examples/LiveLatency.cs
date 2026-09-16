// 发送延迟故障容错（sendLatencyFaultEnable）真机验证
// （对应 python/verify_latency_live.py、cpp/examples/live_latency.cpp，同一套 S1–S5）。
// 用法：rmq latency [namesrv]
//
// 场景（单 broker broker-a / DefaultCluster）：
//   S1 默认关闭：发 10 条全 OK（行为不变，容错表不记录）
//   S2 开启故障规避：发 20 条全 OK（broker 健康，不触发隔离）
//   S3 成功发送后容错表有记录：broker-a 的 FaultItem.CurrentLatency > 0 且可用/可达
//   S4 手工注入隔离（broker-a，隔离档位 10000ms）→ 再发送：单 broker 下走
//      available→reachable→普通轮询 的退化链仍全部发出
//   S5 隔离到期恢复：remove 后注入 2000ms 隔离，立即不可用，≈2s 后恢复可用
using RocketMQ.Client;
using RocketMQ.Common;

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

        Console.WriteLine();
        Console.WriteLine("== 结果: PASS=" + _pass + " FAIL=" + _fail + " ==");
        return _fail == 0 ? 0 : 1;
    }
}
