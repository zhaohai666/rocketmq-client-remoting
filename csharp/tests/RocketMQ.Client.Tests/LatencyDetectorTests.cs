// Why this needs unit tests rather than relying on a real machine only: the detection
// thread only takes effect on the rare path "the broker was isolated and later recovered",
// which a real-machine matrix will almost never hit; and its failure mode is silent --
// detection always failing means a bad broker stays isolated forever (send latency
// skyrockets with no error), detection always succeeding means an unreachable broker goes
// back into rotation.
using System.Net;
using System.Net.Sockets;
using RocketMQ.Client;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class LatencyDetectorTests
{
    /// <summary>A programmable fake detector: records the address/timeout of every probe
    /// and returns reachable or not following a script.</summary>
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


    [Fact]
    public void Detector_DefaultsMatchLegacyClientConfig()
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

        tol.UpdateFaultItem("broker-a", 12.0, 10000, reachable: false);
        Assert.False(tol.IsReachable("broker-a"));

        tol.DetectByOneRound();

        Assert.Equal(new[] { "127.0.0.1:10911" }, detector.Probed);
        // the timeout used for probing is detectTimeout (200ms), not the send timeout
        Assert.Equal(new[] { 200L }, detector.Timeouts);
        Assert.True(tol.IsReachable("broker-a"));

        // checkStamp has already been pushed into the future: it must not be probed again this round
        FaultItem item = tol.GetFaultItem("broker-a")!;
        Assert.True(item.CheckStamp >= UtilAll.CurrentTimeMillis());
    }

    [Fact]
    public void DetectByOneRound_SkipsItemsNotDueYet()
    {
        var detector = new FakeDetector();
        LatencyFaultToleranceImpl tol = MakeTolerance(detector, _ => "127.0.0.1:10911");
        tol.UpdateFaultItem("broker-a", 1.0, 10000, false);

        // manually push the next hop into the future (equivalent to having just probed)
        tol.GetFaultItem("broker-a")!.CheckStamp = UtilAll.CurrentTimeMillis() + 5000;
        tol.DetectByOneRound();

        Assert.Empty(detector.Probed);
        // it must not be flipped back to reachable before it is due
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

        // the broker is no longer in the route: the entry is removed and the detector must not be called even once
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
        // the resolver has an address but no detector is configured: it must neither crash nor change reachability
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
        tol.GetFaultItem("broker-a")!.CheckStamp = 0; // initially 0 => the first round must probe

        long before = UtilAll.CurrentTimeMillis();
        tol.DetectByOneRound();
        long stamp = tol.GetFaultItem("broker-a")!.CheckStamp;

        // the due decision uses the next hop written **before** the probe, so it lands between [before+7000, now+7000]
        Assert.InRange(stamp, before + 7000, UtilAll.CurrentTimeMillis() + 7000);
    }

    // ---------------------------------------------------------------- The detection thread lifecycle

    [Fact]
    public void StartDetector_IsIdempotentAndShutdownStopsIt()
    {
        var tol = new LatencyFaultToleranceImpl();
        tol.StartDetector();
        tol.StartDetector();
        tol.ShutdownDetector();
        tol.ShutdownDetector(); // stopping an already stopped one must be harmless too
    }

    // ---------------------------------------------------------------- TcpServiceDetector

    [Theory]
    [InlineData("127.0.0.1", 200)]          // no port separator
    [InlineData("127.0.0.1:", 200)]          // the port is empty
    [InlineData("127.0.0.1:abc", 200)]       // the port is not a number
    [InlineData("127.0.0.1:0", 200)]         // the port is out of range (below)
    [InlineData("127.0.0.1:70000", 200)]     // the port is out of range (above)
    public void TcpServiceDetector_RejectsMalformedEndpoint(string endpoint, int timeout)
    {
        // a bad address is judged unreachable in every case, and it must not throw (throwing inside the detection thread means none of the other brokers get probed that round either)
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

        // the port is closed: judged unreachable immediately (beyond the timeout backstop it must also be able to get connection refused)
        Assert.False(new TcpServiceDetector().Detect($"127.0.0.1:{port}", 1000));
    }
}
