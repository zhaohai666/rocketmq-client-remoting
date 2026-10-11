// The fault-avoidance REACHABILITY DETECTOR (Java
// LatencyFaultToleranceImpl.startDetector + the anonymous ServiceDetector in
// DefaultMQProducerImpl's constructor).
//
// Java shape, kept here one-to-one:
//   - DefaultMQProducerImpl.start():272 calls mqFaultStrategy.startDetector(),
//     which schedules detectByOneRound at a FIXED 3s rate; each round is a
//     no-op unless startDetectorEnable is on. Here that is detectLoop, a
//     goroutine started by producer Start and stopped by Shutdown.
//   - Each round walks the fault table and probes every item whose checkStamp
//     (next-due time) has passed; the per-broker period is detectInterval
//     (Java ClientConfig, default 2000ms), NOT the 3s tick.
//   - The ServiceDetector is one GET_MAX_OFFSET(30) against the broker's
//     master publish address — the cheapest RPC that proves the whole chain
//     (transport, request routing, store) answers. A success flips an
//     unreachable broker back to reachable so the send path's reachableFilter
//     admits it again; a failure leaves the flag alone.
//
// The detect* setters mirror ClientConfig's startDetectorEnable/detectTimeout/
// detectInterval; they may be flipped at runtime like any other Java knob.
package client

import (
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// DefaultDetectTimeoutMillis / DefaultDetectIntervalMillis are Java
// ClientConfig's detector defaults (200ms per probe, 2000ms per broker).
const (
	DefaultDetectTimeoutMillis  = int64(200)
	DefaultDetectIntervalMillis = int64(2 * 1000)
)

// ---------------- configuration (Java ClientConfig) ----------------

// SetStartDetectorEnable toggles the background reachability detector (Java
// ClientConfig#setStartDetectorEnable, system property
// com.rocketmq.startDetectorEnable). Off by default, exactly like Java. The
// switch is read every scheduler tick, so it may be flipped while running.
func (p *DefaultMQProducer) SetStartDetectorEnable(enable bool) {
	p.faultStrategy.SetStartDetectorEnable(enable)
}

// IsStartDetectorEnable reports the detector switch.
func (p *DefaultMQProducer) IsStartDetectorEnable() bool {
	return p.faultStrategy.StartDetectorEnable()
}

// SetDetectTimeout sets how long one probe may take (Java
// ClientConfig#setDetectTimeout, default DefaultDetectTimeoutMillis).
func (p *DefaultMQProducer) SetDetectTimeout(v int64) {
	p.faultStrategy.setDetectTimeout(v)
}

// SetDetectInterval sets how often EACH broker in the fault table is probed
// (Java ClientConfig#setDetectInterval, default DefaultDetectIntervalMillis).
func (p *DefaultMQProducer) SetDetectInterval(v int64) {
	p.faultStrategy.setDetectInterval(v)
}

// ---------------- lifecycle hooks (called by Start/Shutdown) ----------------

// startDetector is Java DefaultMQProducerImpl.start():272. The goroutine runs
// unconditionally once the producer is up (like Java's scheduled executor) and
// consults startDetectorEnable every tick. The Resolver + ServiceDetector need
// the client instance, so they are installed here too.
func (p *DefaultMQProducer) startDetector() {
	p.mu.Lock()
	inst := p.mqClient
	p.mu.Unlock()
	if inst == nil {
		return
	}
	// Java Resolver = DefaultMQProducerImpl::findBrokerAddressInPublish:
	// master publish address only, no route refresh.
	p.faultStrategy.latencyFaultTolerance.setDetector(
		func(name string) (string, bool) {
			return inst.FindBrokerAddressInPublish(name)
		},
		p.detectByMaxOffset,
	)
	p.detectorRunning.Store(true)
	go p.detectLoop()
}

// stopDetector is Java LatencyFaultToleranceImpl.shutdown, called from
// producer Shutdown.
func (p *DefaultMQProducer) stopDetector() {
	p.detectorRunning.Store(false)
}

// detectLoop is Java startDetector's scheduleAtFixedRate(3, 3, SECONDS)
// runnable: every tick, one round of probing if startDetectorEnable is on.
// The tick is re-read each 50ms slice, so a runtime retune (or a test
// shrinking it) takes effect on the next slice, and Shutdown stays prompt
// (the heartbeatLoop pattern).
func (p *DefaultMQProducer) detectLoop() {
	tolerance := p.faultStrategy.latencyFaultTolerance
	lastRound := time.Now()
	for p.detectorRunning.Load() {
		if time.Since(lastRound) >= time.Duration(tolerance.tickMillis.Load())*time.Millisecond {
			tolerance.detectByOneRoundIfEnabled()
			lastRound = time.Now()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ---------------- ServiceDetector ----------------

// detectByMaxOffset is the ServiceDetector Java builds in
// DefaultMQProducerImpl's constructor (:155-169): pick a topic, send
// GET_MAX_OFFSET(30) for its queue 0 to the endpoint, and report
// "service back to normal" only when the RPC round-trips cleanly. Every
// failure mode — transport error, broker rejection, timeout — counts as still
// down, and (per Java detectByOneRound) leaves the reachable flag untouched.
func (p *DefaultMQProducer) detectByMaxOffset(addr string, timeoutMillis int64) bool {
	if addr == "" {
		return false
	}
	inst, err := p.requireClient()
	if err != nil {
		return false
	}
	topic := p.detectTopic(inst)
	if topic == "" {
		// Java pickTopic: an empty topic table yields Optional.empty and the
		// detector reports false.
		return false
	}
	header := &remoting.GetMaxOffsetRequestHeader{
		Topic:   remoting.StrPtr(topic),
		QueueID: remoting.I32Ptr(0),
	}
	request := remoting.CreateRequestCommand(remoting.ReqGetMaxOffset, header)
	response, err := inst.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		common.LogDebugf("detector: GET_MAX_OFFSET probe to %s failed: %v", addr, err)
		return false
	}
	return inst.checkResponse(response) == nil
}

// detectTopic picks the probe topic (Java DefaultMQProducerImpl.pickTopic: the
// first key of topicPublishInfoTable). The instance's topicsInUse registry is
// the same set here — topicPublishInfo() registers every topic it looks up.
func (p *DefaultMQProducer) detectTopic(inst *Instance) string {
	topics := inst.topicsInUseSnapshot()
	if len(topics) == 0 {
		return ""
	}
	return topics[0]
}
