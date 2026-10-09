// startDetector 可达性探测线程的离线单测（对应 Java client.latency.LatencyFaultToleranceImpl
// 的 detectByOneRound / startDetector 与 ClientConfig 的 detectTimeout/detectInterval）。
//
// 为什么必须单测而不是只靠真机：探测线程只在「broker 被隔离 + 之后恢复」这条罕见路径上生效，
// 真机矩阵里几乎不会命中；而它的失败模式是静默的——探测永远失败就是把坏 broker 一直隔离
// （发送延迟飙升但没有报错），探测永远成功就是把不可达的 broker 放回轮询。
//
// 三条关键判据逐条锁死：
//   1. 默认值 200ms/2000ms/startDetectorEnable=false 与 Java ClientConfig:82-83 一致
//      （开关默认关是 Java 的行为，不能自作主张打开）；
//   2. checkStamp 门控：一轮探测后把下一跳推到 now+detectInterval，未到期不重复探测
//      （Java 同口径；写反了就是每 3s 周期里对每台 broker 疯狂建连）；
//   3. resolver 查不到地址 = broker 已从路由里消失，条目直接摘掉（Java 同），
//      而不是留在表里被永久隔离。
using System.Net;
using System.Net.Sockets;
using RocketMQ.Client;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class LatencyDetectorTests
{
    /// <summary>可编程的假探测器：记录每次探测的地址/超时，并按脚本返回可达与否。</summary>
    private sealed class FakeDetector : IServiceDetector
    {
        private readonly Queue<bool> _script = new();

        public List<string> Probed { get; } = new();

        public List<long> Timeouts { get; } = new();

        public void Enqueue(bool ok) => _script.Enqueue(ok);

        public bool Detect(string endpoint, long timeoutMillis)
        {
            Probed.Add(endpoint);
            Timeouts.Add(timeoutMillis);
            return _script.Count > 0 ? _script.Dequeue() : true;
        }
    }

    private static LatencyFaultToleranceImpl MakeTolerance(FakeDetector detector,
        BrokerAddrResolver? resolver = null, int detectInterval = 2000)
    {
        var tol = new LatencyFaultToleranceImpl(resolver, detector)
        {
            DetectInterval = detectInterval,
        };
        return tol;
    }

    // ---------------------------------------------------------------- 默认值（Java 对拍）

    [Fact]
    public void Detector_DefaultsMatchJavaClientConfig()
    {
        var tol = new LatencyFaultToleranceImpl();
        Assert.Equal(200, tol.DetectTimeout);
        Assert.Equal(2000, tol.DetectInterval);
        Assert.False(tol.IsStartDetectorEnable());

        var strategy = new MQFaultStrategy();
        Assert.False(strategy.IsStartDetectorEnable());

        var producer = new DefaultMQProducer("GID_DetectorUnit");
        Assert.False(producer.StartDetectorEnable);
        Assert.False(producer.SendLatencyFaultEnable);
    }

    [Fact]
    public void SetStartDetectorEnable_PropagatesToToleranceTable()
    {
        var strategy = new MQFaultStrategy(true);
        strategy.SetStartDetectorEnable(true);
        Assert.True(strategy.IsStartDetectorEnable());
        Assert.True(strategy.LatencyFaultTolerance.IsStartDetectorEnable());

        strategy.SetStartDetectorEnable(false);
        Assert.False(strategy.LatencyFaultTolerance.IsStartDetectorEnable());
    }

    // ---------------------------------------------------------------- DetectByOneRound

    [Fact]
    public void DetectByOneRound_ExploresExpiredItemAndFlipsReachable()
    {
        var detector = new FakeDetector();
        detector.Enqueue(true);
        LatencyFaultToleranceImpl tol = MakeTolerance(detector, name => "127.0.0.1:10911");

        // 隔离 + 标记不可达（Java 的 RemotingException 路径）
        tol.UpdateFaultItem("broker-a", 12.0, 10000, reachable: false);
        Assert.False(tol.IsReachable("broker-a"));

        tol.DetectByOneRound();

        Assert.Equal(new[] { "127.0.0.1:10911" }, detector.Probed);
        // 探测用的超时就是 detectTimeout（200ms），不是发送超时
        Assert.Equal(new[] { 200L }, detector.Timeouts);
        Assert.True(tol.IsReachable("broker-a"));

        // checkStamp 已推到未来：本轮之后不该再探
        FaultItem item = tol.GetFaultItem("broker-a")!;
        Assert.True(item.CheckStamp >= UtilAll.CurrentTimeMillis());
    }

    [Fact]
    public void DetectByOneRound_SkipsItemsNotDueYet()
    {
        var detector = new FakeDetector();
        LatencyFaultToleranceImpl tol = MakeTolerance(detector, _ => "127.0.0.1:10911");
        tol.UpdateFaultItem("broker-a", 1.0, 10000, false);

        // 手动把下一跳推到未来（等价于上一轮刚探过）
        tol.GetFaultItem("broker-a")!.CheckStamp = UtilAll.CurrentTimeMillis() + 5000;
        tol.DetectByOneRound();

        Assert.Empty(detector.Probed);
        // 未到期就不该被翻回可达
        Assert.False(tol.IsReachable("broker-a"));
    }

    [Fact]
    public void DetectByOneRound_RemovesItemWhenResolverHasNoAddress()
    {
        var detector = new FakeDetector();
        detector.Enqueue(true);
        LatencyFaultToleranceImpl tol = MakeTolerance(detector, _ => null);
        tol.UpdateFaultItem("broker-gone", 1.0, 10000, false);

        tol.DetectByOneRound();

        // broker 已经不在路由里：条目摘掉，探测器一次都不该被调用
        Assert.Null(tol.GetFaultItem("broker-gone"));
        Assert.True(tol.IsAvailable("broker-gone"));
        Assert.Empty(detector.Probed);
    }

    [Fact]
    public void DetectByOneRound_KeepsUnreachableWhenProbeFails()
    {
        var detector = new FakeDetector();
        detector.Enqueue(false);
        LatencyFaultToleranceImpl tol = MakeTolerance(detector, _ => "127.0.0.1:10911");
        tol.UpdateFaultItem("broker-a", 1.0, 60000, false);

        tol.DetectByOneRound();

        Assert.False(tol.IsReachable("broker-a"));
        Assert.Single(detector.Probed);
    }

    [Fact]
    public void DetectByOneRound_WithoutDetectorOnlyResolvesAddresses()
    {
        // resolver 有地址但探测器未配置：既不崩溃也不该改可达性
        LatencyFaultToleranceImpl tol = new(new BrokerAddrResolver(_ => "127.0.0.1:10911"), null);
        tol.UpdateFaultItem("broker-a", 1.0, 10000, false);
        tol.DetectByOneRound();

        Assert.NotNull(tol.GetFaultItem("broker-a"));
        Assert.False(tol.IsReachable("broker-a"));
    }

    [Fact]
    public void DetectByOneRound_AdvancesCheckStampByInterval()
    {
        var detector = new FakeDetector();
        LatencyFaultToleranceImpl tol = MakeTolerance(detector, _ => "127.0.0.1:1", detectInterval: 7000);
        tol.UpdateFaultItem("broker-a", 1.0, 10000, false);
        tol.GetFaultItem("broker-a")!.CheckStamp = 0; // 初始 0 ⇒ 首轮必探

        long before = UtilAll.CurrentTimeMillis();
        tol.DetectByOneRound();
        long stamp = tol.GetFaultItem("broker-a")!.CheckStamp;

        // 到期判定用的是「探测之前」就写好的下一跳，所以落在 [before+7000, now+7000] 之间
        Assert.InRange(stamp, before + 7000, UtilAll.CurrentTimeMillis() + 7000);
    }

    // ---------------------------------------------------------------- 探测线程生命周期

    [Fact]
    public void StartDetector_IsIdempotentAndShutdownStopsIt()
    {
        var tol = new LatencyFaultToleranceImpl();
        tol.StartDetector();
        tol.StartDetector(); // 重复调用不报错（Java 靠单例 executor，本端口靠运行标记）
        tol.ShutdownDetector();
        tol.ShutdownDetector(); // 已停再停也应无害
    }

    // ---------------------------------------------------------------- TcpServiceDetector

    [Theory]
    [InlineData("127.0.0.1", 200)]          // 没有端口分隔符
    [InlineData("127.0.0.1:", 200)]          // 端口为空
    [InlineData("127.0.0.1:abc", 200)]       // 端口非数字
    [InlineData("127.0.0.1:0", 200)]         // 端口越界（下）
    [InlineData("127.0.0.1:70000", 200)]     // 端口越界（上）
    public void TcpServiceDetector_RejectsMalformedEndpoint(string endpoint, int timeout)
    {
        // 坏地址一律判不可达，且不能抛（探测线程里抛出来就是整轮其他 broker 都不探了）
        Assert.False(new TcpServiceDetector().Detect(endpoint, timeout));
    }

    [Fact]
    public void TcpServiceDetector_AcceptsListeningPortAndRejectsClosedOne()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        try
        {
            Assert.True(new TcpServiceDetector().Detect($"127.0.0.1:{port}", 1000));
        }
        finally
        {
            listener.Stop();
        }

        // 端口已关闭：立刻判不可达（超时兜底之外还要能吃到 connection refused）
        Assert.False(new TcpServiceDetector().Detect($"127.0.0.1:{port}", 1000));
    }
}
