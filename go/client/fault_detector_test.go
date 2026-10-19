// Fault-avoidance reachability detector tests (fault_detector.go). Everything
// runs against the in-process mock cluster from instance_test.go: a mock
// nameserver serving the route and a mock broker whose GET_MAX_OFFSET answer
// is phase-controlled, so outage and recovery need no real cluster.
//
// Failure assertions avoid the loopback connect-refusal quirk: the "down"
// broker rejects at the PROTOCOL level (SYSTEM_ERROR reply) or by protocol
// silence on an ESTABLISHED connection (the probe's InvokeSync timeout path),
// never by dialing a dead port.
package client

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const detectTestTopic = "DetectTopic"

// startDetectorFixture wires one broker + one nameserver and starts a producer
// with the fault strategy and the detector enabled at test speed (50ms
// scheduler tick, 100ms per-broker detect interval). probeAnswer tells the
// broker how to answer GET_MAX_OFFSET(30): >0 = that response code, 0 =
// SUCCESS, <0 = protocol silence (connection stays up, no reply).
func startDetectorFixture(t *testing.T, probeAnswer func() int32) (*DefaultMQProducer, *mockServer) {
	t.Helper()
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqGetMaxOffset {
			if code := probeAnswer(); code < 0 {
				return nil
			} else if code > 0 {
				return remoting.CreateResponseCommand(code, "injected outage")
			}
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	})
	ns := startMockServer(t, nameserverOnReq(routes(map[string]string{
		detectTestTopic: routeBodyFor(broker.addr, ""),
	})))
	p := MustNewDefaultMQProducer("GID_detector")
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	p.SetSendLatencyFaultEnable(true)
	p.SetStartDetectorEnable(true)
	p.SetDetectTimeout(500)
	p.SetDetectInterval(100)
	if err := p.Start(); err != nil {
		t.Fatalf("producer start: %v", err)
	}
	t.Cleanup(p.Shutdown)
	// Warm the route so the Resolver (FindBrokerAddressInPublish) and
	// detectTopic both have something to work with.
	if _, err := p.topicPublishInfo(detectTestTopic); err != nil {
		t.Fatalf("warm route: %v", err)
	}
	p.faultStrategy.latencyFaultTolerance.tickMillis.Store(50)
	return p, broker
}

func TestFaultDetectorProbesPeriodicallyAndRestoresReachability(t *testing.T) {
	healthy := atomic.Bool{}
	healthy.Store(false)
	p, broker := startDetectorFixture(t, func() int32 {
		if healthy.Load() {
			return 0 // SUCCESS: the probe round-trips
		}
		return remoting.RespSystemError // protocol-level rejection
	})
	tolerance := p.faultStrategy.latencyFaultTolerance

	tolerance.updateFaultItem("b1", 0, 0, false)
	if tolerance.isReachable("b1") {
		t.Fatal("precondition: b1 must start unreachable")
	}

	// Outage: probes keep failing at the protocol level, so several rounds
	// must run WITHOUT restoring reachability.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if tolerance.isReachable("b1") {
			t.Fatal("a failed probe must not restore reachability")
		}
		time.Sleep(25 * time.Millisecond)
	}
	probes := broker.requests(remoting.ReqGetMaxOffset)
	if len(probes) < 2 {
		t.Fatalf("detector must probe periodically during the outage, got %d probes", len(probes))
	}
	var header remoting.GetMaxOffsetRequestHeader
	header.FromExtFields(probes[0].ExtFields())
	if header.Topic == nil || *header.Topic != detectTestTopic {
		t.Errorf("probe topic = %v, want %s", header.Topic, detectTestTopic)
	}

	// Recovery: the broker starts answering; the next round must flip b1 back
	// to reachable so the send path's reachableFilter admits it again.
	healthy.Store(true)
	waitFor(t, "detector restoring b1 reachability", func() bool {
		return tolerance.isReachable("b1")
	})
}

func TestFaultDetectorTimeoutKeepsBrokerUnreachable(t *testing.T) {
	// -1 = protocol silence: the connection is established but the broker
	// never answers, so the probe exercises the InvokeSync timeout path
	// (detectTimeout) instead of a loopback connect refusal.
	p, broker := startDetectorFixture(t, func() int32 { return -1 })
	tolerance := p.faultStrategy.latencyFaultTolerance
	tolerance.updateFaultItem("b1", 0, 0, false)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tolerance.isReachable("b1") {
			t.Fatal("a timed-out probe must not restore reachability")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if got := len(broker.requests(remoting.ReqGetMaxOffset)); got < 2 {
		t.Fatalf("a silent broker must still be probed repeatedly, got %d probes", got)
	}
}

func TestFaultDetectorDisabledNeverProbes(t *testing.T) {
	p, broker := startDetectorFixture(t, func() int32 { return 0 })
	p.SetStartDetectorEnable(false)
	tolerance := p.faultStrategy.latencyFaultTolerance
	tolerance.updateFaultItem("b1", 0, 0, false)

	time.Sleep(300 * time.Millisecond)
	if got := len(broker.requests(remoting.ReqGetMaxOffset)); got != 0 {
		t.Fatalf("a disabled detector must not probe, got %d probes", got)
	}
	if tolerance.isReachable("b1") {
		t.Fatal("without the detector b1 must stay unreachable")
	}
}

// TestDetectByOneRoundCoversRouteGoneAndSelection covers the two behaviours the
// integration fixture cannot show deterministically: a broker the route no
// longer knows loses its fault record, and a restored broker is admitted by the
// reachableFilter again.
func TestDetectByOneRoundCoversRouteGoneAndSelection(t *testing.T) {
	tol := newLatencyFaultToleranceImpl()
	tol.setDetectInterval(0) // every round probes everything
	tol.setStartDetectorEnable(true)
	var probes []string
	tol.setDetector(
		func(name string) (string, bool) {
			if name == "gone" {
				return "", false
			}
			return "127.0.0.1:1", true
		},
		func(addr string, timeoutMillis int64) bool {
			if timeoutMillis != int64(defaultDetectTimeout) {
				t.Errorf("detector timeout = %d, want %v", timeoutMillis, defaultDetectTimeout)
			}
			probes = append(probes, addr)
			return true
		},
	)
	tol.updateFaultItem("gone", 0, 0, false)
	tol.updateFaultItem("live", 0, 0, false)
	tol.detectByOneRound()

	if tol.faultItem("gone") != nil {
		t.Fatal("a broker the route no longer knows must lose its fault record")
	}
	if len(probes) != 1 || probes[0] != "127.0.0.1:1" {
		t.Fatalf("probes = %v, want exactly the live broker once", probes)
	}
	if !tol.isReachable("live") {
		t.Fatal("a successful probe must restore reachability")
	}

	// Selection: b1 is isolated AND unreachable, b2 isolated but reachable —
	// the available pass finds nothing and the reachable pass must admit b2,
	// never the dead b1. Once the detector restores b1 (done above for "live",
	// replayed here), the reachable pass may pick it again.
	strategy := newMQFaultStrategy(true)
	info := NewTopicPublishInfo()
	info.UpdateFromRoute(mustRoute(t, twoBrokerRouteBody("1.1.1.1:10911", "2.2.2.2:10911")), "T")
	strategy.latencyFaultTolerance.updateFaultItem("b1", 0, 60000, false)
	strategy.latencyFaultTolerance.updateFaultItem("b2", 0, 60000, true)
	for i := 0; i < 10; i++ {
		mq, ok, err := strategy.selectOneMessageQueue(info, "", false)
		if err != nil || !ok {
			t.Fatalf("select %d: ok=%v err=%v", i, ok, err)
		}
		if mq.BrokerName == "b1" {
			t.Fatalf("an unreachable broker must not be picked, got %v", mq)
		}
	}
	strategy.latencyFaultTolerance.updateFaultItem("b1", 0, 60000, true) // detector restored it
	sawB1 := false
	for i := 0; i < 20 && !sawB1; i++ {
		mq, ok, err := strategy.selectOneMessageQueue(info, "", false)
		if err != nil || !ok {
			t.Fatalf("select after recovery %d: ok=%v err=%v", i, ok, err)
		}
		sawB1 = mq.BrokerName == "b1"
	}
	if !sawB1 {
		t.Fatal("after recovery b1 must be admitted by the reachable pass again")
	}
}

func TestDetectorConfigDefaultsMatchGoldenConfig(t *testing.T) {
	strategy := newMQFaultStrategy(false)
	tol := strategy.latencyFaultTolerance
	if got := tol.detectTimeout; got != 200 {
		t.Errorf("default detectTimeout = %v, want 200", got)
	}
	if got := tol.detectInterval; got != 2000 {
		t.Errorf("default detectInterval = %v, want 2000", got)
	}
	if tol.isStartDetectorEnable() {
		t.Error("startDetectorEnable must default to false")
	}
	if got := tol.tickMillis.Load(); got != 3000 {
		t.Errorf("detector tick = %dms, want 3000ms", got)
	}

	p := MustNewDefaultMQProducer("GID_detector_defaults")
	if p.IsStartDetectorEnable() {
		t.Error("producer detector switch must default to false")
	}
	p.SetStartDetectorEnable(true)
	p.SetDetectTimeout(300)
	p.SetDetectInterval(4000)
	if !p.IsStartDetectorEnable() || !p.faultStrategy.latencyFaultTolerance.isStartDetectorEnable() {
		t.Error("SetStartDetectorEnable must reach both the strategy and the table")
	}
	if got := p.faultStrategy.latencyFaultTolerance.detectTimeout; got != 300 {
		t.Errorf("detectTimeout = %v, want 300", got)
	}
	if got := p.faultStrategy.latencyFaultTolerance.detectInterval; got != 4000 {
		t.Errorf("detectInterval = %v, want 4000", got)
	}
}
